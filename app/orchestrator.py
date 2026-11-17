import asyncio, math, time
from typing import Callable, List, Optional
from scipy.stats import norm
from app.config import settings
from app.database import PredictionLedger
from app.engines import EarlyPredictionEngine, EvidenceEngine, FinalDecisionEngine, RegimeEngine, WeaknessEngine
from app.execution import ExecutionManager
from app.feeds import MarketDataHub, iso_now
from app.models import ContractInfo, DashboardSnapshot, EngineStatusSummary, PredictionRecord, Signal

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
        self.latest_snapshot: Optional[DashboardSnapshot] = None
        self.latest_baseline_z_prob: float = 0.5
        self._listeners: List[Callable[[DashboardSnapshot], None]] = []
        self._running = False

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
                self.ledger.settle_contract(old_ticker, settlement_price=fallback_price, settlement_source="SPOT_CLOSE_PROXY")

    def evaluate_once(self, now: Optional[float] = None, record_to_db: bool = True) -> Optional[DashboardSnapshot]:
        ts = now if now is not None else time.time()
        live_price = self.hub.live_price
        if live_price is None or live_price <= 0: return None
        new_contract = self._resolve_active_contract(ts, live_price)
        if self.active_contract is not None and new_contract.ticker != self.active_contract.ticker:
            if not self.active_contract.ticker.startswith("KXBTC15M"):
                self.ledger.settle_contract(self.active_contract.ticker, settlement_price=live_price, settlement_source="SPOT_CLOSE_PROXY")
            self.last_logged_ts = 0.0
        self.active_contract = new_contract
        spot_age = round(ts - self.hub.last_spot_ts, 2) if self.hub.last_spot_ts else None
        ev = self.evidence_engine.analyze(ts, live_price, new_contract, list(self.hub.trades), list(self.hub.bars_1s), list(self.hub.bids), list(self.hub.asks))
        weakness = self.weakness_engine.analyze(ev, spot_age)
        regime = self.regime_engine.analyze(ev, weakness, list(self.hub.bars_1s))
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

    async def run(self) -> None:
        self._running = True
        reconcile_counter = 0
        while self._running:
            ts = time.time()
            snap = self.evaluate_once(now=ts)
            if snap is not None:
                await self.executor.evaluate_and_execute(ts, snap.contract, snap.final, baseline_z_prob=self.latest_baseline_z_prob)
                reconcile_counter += 1
                if reconcile_counter >= 10:
                    reconcile_counter = 0
                    await self.reconcile_unsettled_contracts(snap.contract.ticker, snap.evidence.live_price)
                snap.scoreboard = self.ledger.get_scoreboard()
                snap.recent_predictions = self.ledger.get_recent_predictions(limit=20)
                snap.recent_trades = self.ledger.get_recent_trades(limit=15)
                for cb in list(self._listeners):
                    try: cb(snap)
                    except Exception: pass
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        self._running = False
