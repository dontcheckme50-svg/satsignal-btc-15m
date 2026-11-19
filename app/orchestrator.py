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
        # A directional lock requires two consecutive one-second decisions
        # for the same official contract and direction.
        self._directional_lock_candidate_ticker: Optional[str] = None
        self._directional_lock_candidate: Signal = Signal.WAIT
        self._directional_lock_candidate_count: int = 0
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
        self._settlement_task: Optional[asyncio.Task] = None
        # Rotate bounded settlement batches so an old unresolved contract
        # cannot permanently starve newer contracts later in the backlog.
        self._settlement_cursor_ticker: Optional[str] = None
        self._reconcile_cycle_counter: int = 0

    def _reset_directional_lock_candidate(self) -> None:
        self._directional_lock_candidate_ticker = None
        self._directional_lock_candidate = Signal.WAIT
        self._directional_lock_candidate_count = 0

    def register_listener(self, cb: Callable[[DashboardSnapshot], None]) -> None:
        self._listeners.append(cb)

    def _resolve_active_contract(self, now: float, live_price: float) -> ContractInfo:
        kc = self.hub.kalshi_contract
        kalshi_age = safe_age_seconds(self.hub.last_kalshi_ts, now)
        if (
            kc is not None
            and kc.is_official_kalshi
            and kc.strike_source == "KALSHI_OFFICIAL"
            and kc.open_timestamp <= now < kc.close_timestamp
            and kalshi_age is not None
            and kalshi_age <= 12.0
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

    async def reconcile_unsettled_contracts(self, current_ticker: str, fallback_price: Optional[float] = None) -> None:
        # fallback_price is retained for call compatibility only. A later live spot
        # price must never be attributed to an already-closed contract.
        # Keep reconciliation bounded so a historical backlog cannot stall the
        # prediction loop for dozens of sequential HTTP timeouts.
        pending = [
            ticker for ticker in self.ledger.get_unsettled_contract_tickers()
            if ticker != current_ticker
        ]
        if not pending:
            self._settlement_cursor_ticker = None
            return
        start_index = 0
        if self._settlement_cursor_ticker in pending:
            start_index = (pending.index(self._settlement_cursor_ticker) + 1) % len(pending)
        batch = [
            pending[(start_index + offset) % len(pending)]
            for offset in range(min(5, len(pending)))
        ]
        self._settlement_cursor_ticker = batch[-1]
        semaphore = asyncio.Semaphore(3)

        async def verify(ticker: str):
            async with semaphore:
                try:
                    return ticker, await self.hub.fetch_verified_kalshi_settlement(ticker)
                except Exception as exc:
                    logger.warning("Settlement verification failed for %s: %s", ticker, exc)
                    return ticker, None

        results = await asyncio.gather(*(verify(ticker) for ticker in batch))
        for old_ticker, verified in results:
            if verified is not None:
                outcome, exp_val = verified
                self.ledger.settle_contract(
                    old_ticker,
                    settlement_price=exp_val,
                    official_result=outcome,
                    settlement_source="KALSHI_VERIFIED",
                )

    def evaluate_once(self, now: Optional[float] = None, record_to_db: bool = True) -> Optional[DashboardSnapshot]:
        ts = now if now is not None else time.time()
        live_price = self.hub.live_price
        if live_price is None or live_price <= 0: return None
        new_contract = self._resolve_active_contract(ts, live_price)
        if self.active_contract is not None and new_contract.ticker != self.active_contract.ticker:
            # Do not settle a synthetic contract using a price from the next
            # contract window. Synthetic contracts lack an official outcome.
            self.last_logged_ts = 0.0
            # Start a fresh shadow-history cadence for each official contract.
            self.last_multi_exchange_logged_ts = 0.0
            self._reset_directional_lock_candidate()
        self.active_contract = new_contract
        spot_age = safe_age_seconds(self.hub.last_spot_ts, ts)
        book_age = safe_age_seconds(self.hub.last_book_ts, ts)
        book_source_matches = bool(
            self.hub.active_spot_source
            and self.hub.last_book_source == self.hub.active_spot_source
        )
        book_fresh = (
            book_age is not None
            and book_age <= settings.stale_feed_threshold_sec
            and book_source_matches
        )
        kalshi_age = safe_age_seconds(self.hub.last_kalshi_ts, ts)
        official_contract_fresh = bool(
            new_contract.is_official_kalshi
            and kalshi_age is not None
            and kalshi_age <= 12.0
        )
        # This is the single actionability gate for directional locks and
        # dashboard engine status. A patched/failed downstream engine must
        # never be able to turn stale cached data into an official direction.
        engine_inputs_fresh = bool(
            spot_age is not None
            and spot_age <= settings.stale_feed_threshold_sec
            and book_fresh
            and official_contract_fresh
        )
        evidence_bids = list(self.hub.bids) if book_fresh else []
        evidence_asks = list(self.hub.asks) if book_fresh else []
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
        # Bound bar history by age as well as timestamp; old candles must not
        # leak into momentum, volatility, or regime calculations after feed gaps.
        observed_bars = [bar for bar in self.hub.bars_1s if new_contract.open_timestamp <= bar[0] and ts - 300.0 <= bar[0] <= ts]
        brti_age = safe_age_seconds(self.hub.last_brti_ts, ts)
        brti_is_fresh = (
            self.hub.brti_status == "LIVE"
            and brti_age is not None
            and brti_age <= settings.stale_feed_threshold_sec
        )
        official_brti_price = self.hub.official_brti_price if brti_is_fresh else None
        ev = self.evidence_engine.analyze(
            ts, live_price, new_contract, list(self.hub.trades),
            observed_bars, evidence_bids, evidence_asks,
            official_brti_price=official_brti_price,
            official_brti_age_sec=brti_age if brti_is_fresh else None,
            reference_price=reference_price,
            reference_age_sec=multi_reference.get("reference_age_seconds"),
        )
        weakness = self.weakness_engine.analyze(
            ev, spot_age, book_age, book_source_matches=book_source_matches
        )
        regime = self.regime_engine.analyze(ev, weakness, observed_bars)
        early = self.early_engine.analyze(ev, weakness, new_contract)
        final = self.final_engine.decide(ev, weakness, regime, early, new_contract, self.ledger.get_engine_weights())
        self.latest_baseline_z_prob = float(norm.cdf(EvidenceEngine._strike_z_score(live_price, new_contract.strike_price, new_contract.seconds_remaining, ev.volatility_annualized)))
        if record_to_db and new_contract.is_official_kalshi and (self.last_logged_ts == 0.0 or ts - self.last_logged_ts >= 30.0):
            # Save changing engine state as an observation, not a scored prediction.
            observation_payload = {
                "contract": new_contract.model_dump(),
                "evidence": ev.model_dump(),
                "weakness": weakness.model_dump(),
                "regime": regime.model_dump(),
                "early": early.model_dump(),
                "final": final.model_dump(),
                "feeds": [feed.model_dump() for feed in self.hub.get_feed_statuses()],
            }
            self.ledger.record_prediction_observation(
                contract_ticker=new_contract.ticker,
                epoch_ts=ts,
                seconds_remaining=round(new_contract.seconds_remaining, 1),
                payload=observation_payload,
            )
            self.last_logged_ts = ts

        spot_side_at_lock = "UP" if live_price > new_contract.strike_price else "DOWN" if live_price < new_contract.strike_price else "AT_STRIKE"
        brti_side_at_lock = (
            "UP" if official_brti_price > new_contract.strike_price
            else "DOWN" if official_brti_price < new_contract.strike_price
            else "AT_STRIKE"
        ) if official_brti_price is not None else None
        if brti_side_at_lock is None:
            brti_cohort = "BRTI_UNAVAILABLE"
        elif "AT_STRIKE" in (spot_side_at_lock, brti_side_at_lock):
            brti_cohort = "NEUTRAL"
        elif spot_side_at_lock == brti_side_at_lock:
            brti_cohort = f"AGREE_{spot_side_at_lock}"
        else:
            brti_cohort = "DISAGREE"

        # Official directional locks require two consecutive evaluations
        # agreeing on the same direction. WAIT, a reversal, or rollover resets
        # confirmation. At the final-entry cutoff, an unconfirmed direction
        # becomes a single immutable WAIT lock.
        if record_to_db:
            if not new_contract.is_official_kalshi or self.ledger.has_official_prediction_lock(new_contract.ticker):
                self._reset_directional_lock_candidate()
            elif final.signal in (Signal.UP, Signal.DOWN) and engine_inputs_fresh:
                if (
                    self._directional_lock_candidate_ticker == new_contract.ticker
                    and self._directional_lock_candidate == final.signal
                ):
                    self._directional_lock_candidate_count += 1
                else:
                    self._directional_lock_candidate_ticker = new_contract.ticker
                    self._directional_lock_candidate = final.signal
                    self._directional_lock_candidate_count = 1
            else:
                self._reset_directional_lock_candidate()

            confirmed_direction = (
                engine_inputs_fresh
                and new_contract.is_official_kalshi
                and self._directional_lock_candidate_ticker == new_contract.ticker
                and self._directional_lock_candidate in (Signal.UP, Signal.DOWN)
                and self._directional_lock_candidate_count >= 2
                and final.signal == self._directional_lock_candidate
            )
            at_final_lockout = new_contract.seconds_remaining <= settings.final_lockout_seconds
            lock_signal = (
                final.signal if confirmed_direction
                else Signal.WAIT if at_final_lockout
                else None
            )
            if (
                new_contract.is_official_kalshi
                and lock_signal is not None
                and not self.ledger.has_official_prediction_lock(new_contract.ticker)
            ):
                self.ledger.record_prediction(PredictionRecord(
                    contract_ticker=new_contract.ticker,
                    timestamp_iso=iso_now(ts),
                    epoch_ts=ts,
                    strike_price=new_contract.strike_price,
                    btc_price_at_pred=live_price,
                    seconds_remaining=round(new_contract.seconds_remaining, 1),
                    main_signal=ev.signal,
                    early_signal=early.signal,
                    final_signal=lock_signal,
                    confidence=final.confidence if lock_signal != Signal.WAIT else 0.0,
                    up_prob=final.up_probability if lock_signal != Signal.WAIT else 0.5,
                    down_prob=final.down_probability if lock_signal != Signal.WAIT else 0.5,
                    regime=regime.regime,
                    brti_cohort=brti_cohort,
                    brti_side_at_lock=brti_side_at_lock,
                    spot_side_at_lock=spot_side_at_lock,
                ))
                self._reset_directional_lock_candidate()
        # Engine outputs are recomputed from this cycle, but their dashboard
        # status must also disclose whether the underlying live inputs are usable.
        # In particular, live_price remains cached during a spot-feed outage;
        # a recent snapshot timestamp must not make that cached price look fresh.
        engine_input_health = {
            "contract_ticker": new_contract.ticker,
            "evaluated_at_ts": ts,
            "spot_age_seconds": None if spot_age is None else round(spot_age, 3),
            "book_age_seconds": None if book_age is None else round(book_age, 3),
            "book_source_matches_spot": book_source_matches,
            "kalshi_age_seconds": None if kalshi_age is None else round(kalshi_age, 3),
            "official_contract_fresh": official_contract_fresh,
            "fresh": engine_inputs_fresh,
        }
        engine_block_status = "ACTIVE" if engine_inputs_fresh else "BLOCKED_STALE_INPUT"
        engines_summary = EngineStatusSummary(
            engine_a_evidence={"status": engine_block_status, "signal": ev.signal.value, "raw_score": ev.raw_score, "structure": ev.structure_bias, "input_health": engine_input_health},
            engine_b_weakness={"status": "DEGRADED_FEED" if weakness.feed_degraded else "ACTIVE", "weakness_score": weakness.weakness_score, "flags": weakness.flags, "input_health": engine_input_health},
            engine_c_regime={"status": engine_block_status, "regime": regime.regime.value, "min_confidence_required": regime.min_confidence_required, "chop_index": regime.chop_index, "input_health": engine_input_health},
            engine_d_early={"status": "BLOCKED_STALE_INPUT" if not engine_inputs_fresh else ("ACTIVE_WINDOW" if early.active_window else "MONITORING"), "signal": early.signal.value, "confidence": early.confidence, "input_health": engine_input_health},
            engine_e_final={"status": "BLOCKED_STALE_INPUT" if not engine_inputs_fresh else "ACTIVE", "signal": final.signal.value, "confidence": final.confidence, "agreement": final.agreement_status, "input_health": engine_input_health},
        )
        self.latest_snapshot = DashboardSnapshot(server_time_iso=iso_now(ts), contract=new_contract, feeds=self.hub.get_feed_statuses(), evidence=ev, weakness=weakness, regime=regime, early=early, final=final, engines=engines_summary, scoreboard=self.ledger.get_scoreboard(), recent_predictions=self.ledger.get_recent_predictions(limit=20), recent_trades=self.ledger.get_recent_trades(limit=15))
        return self.latest_snapshot

    def _schedule_settlement_reconciliation(self, current_ticker: str) -> None:
        # Settlement is independent of spot-feed health. Count every worker
        # loop, including loops that cannot produce a prediction snapshot.
        self._reconcile_cycle_counter += 1
        if self._reconcile_cycle_counter < 10:
            return
        self._reconcile_cycle_counter = 0
        if self._settlement_task is None or self._settlement_task.done():
            self._settlement_task = asyncio.create_task(
                self._settlement_worker(current_ticker, self.hub.live_price or 0.0),
                name="settlement-reconciliation",
            )

    async def _settlement_worker(self, current_ticker: str, fallback_price: float) -> None:
        try:
            await self.reconcile_unsettled_contracts(current_ticker, fallback_price)
            self.last_settlement_error = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_settlement_error = f"{type(exc).__name__}: {exc}"
            logger.exception("Background settlement reconciliation failed")

    async def shutdown_background_tasks(self) -> None:
        task = self._settlement_task
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        self._settlement_task = None

    def get_runtime_health(self) -> dict:
        now = time.time()
        cycle_age = None if self.last_cycle_success_ts is None else round(now - self.last_cycle_success_ts, 2)
        # Worker liveness and market-data readiness are separate: a healthy
        # loop can still be unable to make an actionable official-contract call.
        spot_age = safe_age_seconds(self.hub.last_spot_ts, now)
        book_age = safe_age_seconds(self.hub.last_book_ts, now)
        kalshi_age = safe_age_seconds(self.hub.last_kalshi_ts, now)
        spot_fresh = spot_age is not None and spot_age <= settings.stale_feed_threshold_sec
        book_source_matches = bool(
            self.hub.active_spot_source
            and self.hub.last_book_source == self.hub.active_spot_source
        )
        book_fresh = (
            book_age is not None
            and book_age <= settings.stale_feed_threshold_sec
            and book_source_matches
        )
        kalshi_fresh = kalshi_age is not None and kalshi_age <= 12.0
        official_contract_active = bool(
            self.active_contract is not None
            and self.active_contract.is_official_kalshi
            and self.active_contract.open_timestamp <= now < self.active_contract.close_timestamp
        )
        market_data_ready = bool(spot_fresh and book_fresh and kalshi_fresh and official_contract_active)
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
            "settlement_reconciliation_running": bool(getattr(self, "_settlement_task", None) is not None and not getattr(self, "_settlement_task").done()),
            "market_data_health": "READY" if market_data_ready else "DEGRADED",
            "market_data_ready": market_data_ready,
            "spot_age_seconds": None if spot_age is None else round(spot_age, 2),
            "order_book_age_seconds": None if book_age is None else round(book_age, 2),
            "order_book_source_matches_spot": book_source_matches,
            "kalshi_age_seconds": None if kalshi_age is None else round(kalshi_age, 2),
            "official_contract_active": official_contract_active,
            "official_brti_status": self.hub.brti_status,
            "health": health,
        }

    async def run(self) -> None:
        self._running = True
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
            market = self.hub.kalshi_contract
            now = time.time()
            current_ticker = (
                market.ticker
                if market is not None
                and market.is_official_kalshi
                and market.open_timestamp <= now < market.close_timestamp
                else ""
            )
            self._schedule_settlement_reconciliation(current_ticker)
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        self._running = False
        if self._settlement_task is not None and not self._settlement_task.done():
            self._settlement_task.cancel()
