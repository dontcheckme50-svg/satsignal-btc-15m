import asyncio, math, time, logging
from typing import Callable, List, Optional
from scipy.stats import norm
from app.config import settings
from app.database import PredictionLedger
from app.engines import EarlyPredictionEngine, EvidenceEngine, FinalDecisionEngine, RegimeEngine, WeaknessEngine
from app.execution import ExecutionManager
from app.feeds import MarketDataHub, iso_now, safe_age_seconds
from app.models import ContractInfo, DashboardSnapshot, EngineStatusSummary, PredictionRecord, Signal

logger = logging.getLogger(__name__)

class PredictorOrchestrator:
    def __init__(self, hub: MarketDataHub, ledger: PredictionLedger):
        self.hub = hub
        self.ledger = ledger
        self.executor = ExecutionManager(hub, ledger)
        self.evidence_engine = EvidenceEngine()
        self.weakness_engine = WeaknessEngine()
        self.regime_engine = RegimeEngine()
        self.early_engine = EarlyPredictionEngine()
        self.final_engine = FinalDecisionEngine()
        self.active_contract: Optional[ContractInfo] = None
        self.last_logged_ts: float = 0.0
        self.last_multi_exchange_logged_ts: float = 0.0
        self.latest_snapshot: Optional[DashboardSnapshot] = None
        self.latest_baseline_z_prob: float = 0.5
        self._listeners: List[Callable[[DashboardSnapshot], None]] = []
        self._running = False
        # Runtime health telemetry: failures are isolated and the loop remains alive.
        self.last_cycle_started_ts: Optional[float] = None
        self.last_cycle_success_ts: Optional[float] = None
        self.last_cycle_duration_sec: Optional[float] = None
        self.last_cycle_error: Optional[str] = None
        self.consecutive_cycle_errors: int = 0
        self.total_cycle_errors: int = 0
        self.last_execution_error: Optional[str] = None
        self.last_settlement_error: Optional[str] = None

    def register_listener(self, cb: Callable[[DashboardSnapshot], None]) -> None:
        self._listeners.append(cb)

    def _resolve_active_contract(self, now: float, live_price: float) -> ContractInfo:
        kc = self.hub.kalshi_contract
        if (
            kc is not None
            and kc.is_official_kalshi
            and kc.strike_source == "KALSHI_OFFICIAL"
            and kc.open_timestamp <= now < kc.close_timestamp
            and self.hub.last_kalshi_ts > 0
            and now - self.hub.last_kalshi_ts <= 12.0
        ):
            rem = max(0.0, kc.close_timestamp - now)
            cp_rem = rem % settings.checkpoint_interval_sec
            if cp_rem < 1.0:
                cp_rem = float(settings.checkpoint_interval_sec)
            return ContractInfo(ticker=kc.ticker, strike_price=kc.strike_price, strike_source="KALSHI_OFFICIAL", open_timestamp=kc.open_timestamp, close_timestamp=kc.close_timestamp, seconds_remaining=rem, checkpoint_seconds_remaining=cp_rem, is_official_kalshi=True, yes_bid_dollars=kc.yes_bid_dollars, yes_ask_dollars=kc.yes_ask_dollars, no_bid_dollars=kc.no_bid_dollars, no_ask_dollars=kc.no_ask_dollars)
        window_open = math.floor(now / 900.0) * 900.0
        window_close = window_open + 900.0
        ticker = f"SYNTH-BTC15M-{int(window_close)}"
        rem = max(0.0, window_close - now)
        strike = self.active_contract.strike_price if (self.active_contract and self.active_contract.ticker == ticker) else live_price
        cp_rem = rem % settings.checkpoint_interval_sec
        if cp_rem < 1.0:
            cp_rem = float(settings.checkpoint_interval_sec)
        return ContractInfo(ticker=ticker, strike_price=strike, strike_source="SYNTHETIC_OPEN_PROXY", open_timestamp=window_open, close_timestamp=window_close, seconds_remaining=rem, checkpoint_seconds_remaining=cp_rem, is_official_kalshi=False)

    async def reconcile_unsettled_contracts(self, current_ticker: str, fallback_price: float) -> None:
        for old_ticker in self.ledger.get_unsettled_contract_tickers():
            if old_ticker == current_ticker: continue
            if old_ticker.startswith("KXBTC15M"):
                verified = await self.hub.fetch_verified_kalshi_settlement(old_ticker)
                if verified is not None:
                    outcome, exp_val = verified
                    self.ledger.settle_contract(old_ticker, settlement_price=(exp_val if exp_val is not None else fallback_price), official_result=outcome, settlement_source="KALSHI_VERIFIED")
            else:
                # Synthetic contracts have no official settlement source and no
                # captured close-price snapshot. Never grade them with a later
                # contract's fallback price; leave them explicitly unsettled.
                continue

    def evaluate_once(self, now: Optional[float] = None, record_to_db: bool = True) -> Optional[DashboardSnapshot]:
        ts = now if now is not None else time.time()
        live_price = self.hub.live_price
        if live_price is None or live_price <= 0: return None
        new_contract = self._resolve_active_contract(ts, live_price)
        if self.active_contract is not None and new_contract.ticker != self.active_contract.ticker:
            # Do not settle a synthetic contract using a price from the next
            # contract window. Synthetic contracts lack an official outcome.
            self.last_logged_ts = 0.0
        self.active_contract = new_contract
        spot_age = safe_age_seconds(self.hub.last_spot_ts, ts)
        multi_reference = self.hub.get_multi_exchange_snapshot()
        multi_reference["strike_price"] = new_contract.strike_price
        reference_price = multi_reference.get("reference_price")
        multi_reference["reference_vs_strike_bps"] = (
            round((float(reference_price) - float(new_contract.strike_price)) / float(new_contract.strike_price) * 10000.0, 4)
            if reference_price is not None and new_contract.strike_price > 0 else None
        )
        if record_to_db and ts - self.last_multi_exchange_logged_ts >= 5.0:
            try:
                self.ledger.record_multi_exchange_reference(
                    epoch_ts=ts,
                    contract_ticker=new_contract.ticker,
                    strike_price=new_contract.strike_price,
                    primary_spot_price=multi_reference.get("primary_spot_price"),
                    official_brti_price=multi_reference.get("official_brti_price"),
                    snapshot=multi_reference,
                )
                self.last_multi_exchange_logged_ts = ts
            except Exception:
                logger.exception("Failed to persist multi-exchange shadow observation")
        observed_bars = [bar for bar in self.hub.bars_1s if bar[0] <= ts]
        ev = self.evidence_engine.analyze(ts, live_price, new_contract, list(self.hub.trades), observed_bars, list(self.hub.bids), list(self.hub.asks))
        weakness = self.weakness_engine.analyze(ev, spot_age)
        regime = self.regime_engine.analyze(ev, weakness, observed_bars)
        early = self.early_engine.analyze(ev, weakness, new_contract)
        final = self.final_engine.decide(ev, weakness, regime, early, new_contract, self.ledger.get_engine_weights())
        self.latest_baseline_z_prob = float(norm.cdf(EvidenceEngine._strike_z_score(live_price, new_contract.strike_price, new_contract.seconds_remaining, ev.volatility_annualized)))
        if record_to_db and new_contract.is_official_kalshi and (self.last_logged_ts == 0.0 or ts - self.last_logged_ts >= 30.0) and new_contract.seconds_remaining >= settings.final_lockout_seconds:
            self.ledger.record_prediction(PredictionRecord(contract_ticker=new_contract.ticker, timestamp_iso=iso_now(ts), epoch_ts=ts, strike_price=new_contract.strike_price, btc_price_at_pred=live_price, seconds_remaining=round(new_contract.seconds_remaining, 1), main_signal=ev.signal, early_signal=early.signal, final_signal=final.signal, confidence=final.confidence, up_prob=final.up_probability, down_prob=final.down_probability, regime=regime.regime))
            self.last_logged_ts = ts
        engines_summary = EngineStatusSummary(
            engine_a_evidence={"status": "ACTIVE", "signal": ev.signal.value, "raw_score": ev.raw_score, "structure": ev.structure_bias},
            engine_b_weakness={"status": "DEGRADED_FEED" if weakness.feed_degraded else "ACTIVE", "weakness_score": weakness.weakness_score, "flags": weakness.flags},
            engine_c_regime={"status": "ACTIVE", "regime": regime.regime.value, "min_confidence_required": regime.min_confidence_required, "chop_index": regime.chop_index},
            engine_d_early={"status": "ACTIVE_WINDOW" if early.active_window else "MONITORING", "signal": early.signal.value, "confidence": early.confidence},
            engine_e_final={"status": "ACTIVE", "signal": final.signal.value, "confidence": final.confidence, "agreement": final.agreement_status},
        )
        self.latest_snapshot = DashboardSnapshot(server_time_iso=iso_now(ts), contract=new_contract, feeds=self.hub.get_feed_statuses(), evidence=ev, weakness=weakness, regime=regime, early=early, final=final, engines=engines_summary, scoreboard=self.ledger.get_scoreboard(), recent_predictions=self.ledger.get_recent_predictions(limit=20), recent_trades=self.ledger.get_recent_trades(limit=15))
        return self.latest_snapshot

    def get_runtime_health(self) -> dict:
        now = time.time()
        cycle_age = None if self.last_cycle_success_ts is None else round(now - self.last_cycle_success_ts, 2)
        if not self._running:
            health = "STOPPED"
        elif self.last_cycle_success_ts is None:
            health = "DEGRADED" if self.consecutive_cycle_errors else "WARMING_UP"
        elif (
            self.consecutive_cycle_errors
            or self.last_execution_error
            or self.last_settlement_error
            or cycle_age is None
            or cycle_age > 5.0
        ):
            health = "DEGRADED"
        else:
            health = "HEALTHY"
        return {
            "running": self._running,
            "has_snapshot": self.latest_snapshot is not None,
            "last_cycle_started_ts": self.last_cycle_started_ts,
            "last_cycle_success_ts": self.last_cycle_success_ts,
            "last_cycle_age_seconds": cycle_age,
            "last_cycle_duration_seconds": self.last_cycle_duration_sec,
            "last_cycle_error": self.last_cycle_error,
            "consecutive_cycle_errors": self.consecutive_cycle_errors,
            "total_cycle_errors": self.total_cycle_errors,
            "last_execution_error": self.last_execution_error,
            "last_settlement_error": self.last_settlement_error,
            "health": health,
        }

    async def run(self) -> None:
        self._running = True
        reconcile_counter = 0
        while self._running:
            cycle_started = time.time()
            self.last_cycle_started_ts = cycle_started
            snap = None
            try:
                # The prediction cycle is isolated: one exception must not terminate
                # the long-lived background worker.
                snap = self.evaluate_once(now=cycle_started)
                cycle_finished = time.time()
                self.last_cycle_duration_sec = round(cycle_finished - cycle_started, 4)
                # A cycle without usable live market data is not a successful
                # prediction snapshot. Keep the last true success/error state so
                # health can accurately transition to WARMING_UP or DEGRADED.
                if snap is not None:
                    self.last_cycle_success_ts = cycle_finished
                    self.last_cycle_error = None
                    self.consecutive_cycle_errors = 0
            except Exception as exc:
                self.last_cycle_error = f"{type(exc).__name__}: {exc}"
                self.consecutive_cycle_errors += 1
                self.total_cycle_errors += 1
                self.last_cycle_duration_sec = round(time.time() - cycle_started, 4)
                logger.exception("SATSignal prediction cycle failed; worker will continue")
            if snap is not None:
                try:
                    await self.executor.evaluate_and_execute(
                        cycle_started, snap.contract, snap.final,
                        baseline_z_prob=self.latest_baseline_z_prob,
                    )
                    self.last_execution_error = None
                except Exception as exc:
                    self.last_execution_error = f"{type(exc).__name__}: {exc}"
                    logger.exception("SATSignal execution evaluation failed; prediction worker continues")
                reconcile_counter += 1
                if reconcile_counter >= 10:
                    reconcile_counter = 0
                    try:
                        await self.reconcile_unsettled_contracts(snap.contract.ticker, snap.evidence.live_price)
                        self.last_settlement_error = None
                    except Exception as exc:
                        self.last_settlement_error = f"{type(exc).__name__}: {exc}"
                        logger.exception("Settlement reconciliation failed; prediction worker continues")
                try:
                    snap.scoreboard = self.ledger.get_scoreboard()
                    snap.recent_predictions = self.ledger.get_recent_predictions(limit=20)
                    snap.recent_trades = self.ledger.get_recent_trades(limit=15)
                except Exception:
                    logger.exception("Could not refresh scoreboard/history for snapshot")
                for cb in list(self._listeners):
                    try:
                        cb(snap)
                    except Exception:
                        logger.exception("Snapshot listener failed; other listeners will still be notified")
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        self._running = False
