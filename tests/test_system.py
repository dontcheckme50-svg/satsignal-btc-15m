import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import pytest
from pathlib import Path

from app.config import settings
from app.database import PredictionLedger
from app.execution import ExecutionManager
from app.engines import EarlyPredictionEngine, EvidenceEngine, WeaknessEngine
from app.feeds import MarketDataHub
from fastapi.testclient import TestClient
from app.main import app
from app.models import (
    ContractInfo, FinalDecisionReport, MarketRegime, PredictionRecord,
    RegimeReport, Signal, TradeRecord,
)


def _contract(**overrides):
    values = dict(
        ticker="KXBTC15M-TEST",
        strike_price=90000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=time.time() - 100,
        close_timestamp=time.time() + 800,
        seconds_remaining=800.0,
        checkpoint_seconds_remaining=80.0,
        is_official_kalshi=True,
        yes_ask_dollars=0.20,
        no_ask_dollars=0.80,
    )
    values.update(overrides)
    return ContractInfo(**values)


def _decision(signal=Signal.UP, up=0.95, down=0.05):
    return FinalDecisionReport(
        signal=signal,
        confidence=95.0,
        up_probability=up,
        down_probability=down,
        agreement_status="STRONG_CONSENSUS",
        agreement_details=[],
        rationale="test",
    )


def _fresh_execution_hub(contract):
    hub = MarketDataHub()
    now = time.time()
    hub.live_price = 90010.0
    hub.last_spot_ts = now
    hub.active_spot_source = "TEST_LIVE"
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)], source="TEST_LIVE")
    hub.last_kalshi_ts = now
    hub.kalshi_contract = contract
    return hub


def test_paper_mode_is_forced():
    assert settings.trading_mode == "paper"


def test_database_round_trip_and_scoreboard(tmp_path):
    ledger = PredictionLedger()
    rec = PredictionRecord(
        contract_ticker="KXBTC15M-TEST",
        timestamp_iso="2026-11-17 00:00:00 UTC",
        epoch_ts=1794873600.0,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=500.0,
        main_signal=Signal.UP,
        early_signal=Signal.UP,
        final_signal=Signal.UP,
        confidence=70.0,
        up_prob=0.70,
        down_prob=0.30,
        regime=MarketRegime.STABLE,
    )
    ledger.record_prediction(rec)
    assert len(ledger.get_recent_predictions()) == 1
    assert ledger.get_scoreboard().waits_issued == 0


def test_execution_refuses_synthetic_contract(tmp_path):
    ledger = PredictionLedger()
    hub = MarketDataHub()
    manager = ExecutionManager(hub, ledger)
    synthetic = _contract(
        ticker="SYNTH-BTC15M-TEST",
        strike_source="SYNTHETIC_OPEN_PROXY",
        is_official_kalshi=False,
    )
    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), synthetic, _decision()))
    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_requires_real_kalshi_ask(tmp_path):
    ledger = PredictionLedger()
    no_quote = _contract(yes_ask_dollars=None)
    hub = _fresh_execution_hub(no_quote)
    manager = ExecutionManager(hub, ledger)
    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), no_quote, _decision()))
    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_places_at_most_one_paper_trade_per_contract(tmp_path):
    ledger = PredictionLedger()
    contract = _contract()
    hub = _fresh_execution_hub(contract)
    manager = ExecutionManager(hub, ledger)
    import asyncio
    first = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))
    second = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))
    assert first is not None
    assert first.mode == "paper"
    assert first.quote_source == "KALSHI_LIVE_BOOK"
    assert second is None
    assert len(ledger.get_recent_trades()) == 1


def test_kalshi_market_time_and_quote_helpers():
    assert MarketDataHub._quote_dollars("0.42") == 0.42
    assert MarketDataHub._quote_dollars("42") is None
    assert MarketDataHub._quote_dollars(None) is None
    assert MarketDataHub._parse_kalshi_time("2026-11-17T00:00:00Z") > 0


def test_http_dashboard_and_health_endpoints():
    client = TestClient(app)
    assert client.get("/").status_code == 200
    dashboard = client.get("/").text
    assert "SATSIGNAL BTC 15M" in dashboard
    assert 'id="system-health"' in dashboard
    assert 'id="market-health"' in dashboard
    assert 'fetchOrchestratorHealth' in dashboard
    assert client.get("/docs").status_code == 200
    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.json()["trading_mode"] == "paper"


def test_synthetic_contracts_are_not_scored(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    hub = MarketDataHub()
    now = time.time()
    for i in range(40):
        hub.ingest_trade(now - 39 + i, 90000.0 + i, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90040.0, 4.0)], [(90041.0, 3.0)])
    orchestrator = PredictorOrchestrator(hub, ledger)
    snapshot = orchestrator.evaluate_once(now=now, record_to_db=True)
    assert snapshot is not None
    assert snapshot.contract.is_official_kalshi is False
    assert snapshot.final.signal == Signal.WAIT
    assert "official Kalshi contract" in snapshot.final.rationale
    assert ledger.get_recent_predictions() == []
    shadow_rows = ledger.get_multi_exchange_reference_history(limit=5)
    assert len(shadow_rows) == 1
    assert shadow_rows[0]["contract_ticker"] == snapshot.contract.ticker
    assert shadow_rows[0]["reference_status"] == "UNAVAILABLE"


def test_trade_table_has_settlement_timestamp_column(tmp_path):
    ledger = PredictionLedger()
    with ledger._connection() as conn:
        columns = {row["name"] for row in conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ?", ("trades",)).fetchall()}
    assert "settled_at" in columns


def _prediction_record(ticker, final=Signal.UP, main=Signal.UP, early=Signal.DOWN, ts=1794873600.0):
    return PredictionRecord(
        contract_ticker=ticker,
        timestamp_iso="2026-11-17 00:00:00 UTC",
        epoch_ts=ts,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=500.0,
        main_signal=main,
        early_signal=early,
        final_signal=final,
        confidence=70.0,
        up_prob=0.70,
        down_prob=0.30,
        regime=MarketRegime.STABLE,
    )


def test_scoreboard_tracks_pending_and_each_engine_accuracy(tmp_path):
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("KXBTC15M-A", final=Signal.UP, main=Signal.UP, early=Signal.DOWN))
    ledger.record_prediction(_prediction_record("KXBTC15M-B", final=Signal.UP, main=Signal.WAIT, early=Signal.DOWN, ts=1794873630.0))
    ledger.record_prediction(_prediction_record("KXBTC15M-C", final=Signal.WAIT, main=Signal.UP, early=Signal.UP, ts=1794873660.0))

    # Verified settlement: outcome UP. WAIT signals are excluded from directional accuracy.
    ledger.settle_contract("KXBTC15M-A", 90020.0, official_result="UP", settlement_source="KALSHI_VERIFIED")
    ledger.settle_contract("KXBTC15M-B", 89980.0, official_result="DOWN", settlement_source="KALSHI_VERIFIED")

    sb = ledger.get_scoreboard()
    assert sb.total_predictions == 3
    assert sb.pending_predictions == 1
    assert sb.final_scored == 2
    assert sb.final_wins == 1
    assert sb.final_losses == 1
    assert sb.final_accuracy_pct == 50.0
    assert sb.main_scored == 1  # WAIT is not scored
    assert sb.main_wins == 1
    assert sb.early_scored == 2
    assert sb.early_wins == 1
    assert sb.early_losses == 1
    assert sb.waits_issued == 1


def test_synthetic_contract_window_is_exactly_15_minutes():
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    hub = MarketDataHub()
    orchestrator = PredictorOrchestrator(hub, ledger)
    now = 1794873675.25
    contract = orchestrator._resolve_active_contract(now, 90000.0)
    assert contract.close_timestamp - contract.open_timestamp == 900.0
    assert contract.seconds_remaining == contract.close_timestamp - now
    assert 0.0 < contract.seconds_remaining <= 900.0


def test_dashboard_countdown_uses_monotonic_server_clock_sync():
    from app.main import DASHBOARD_HTML

    assert 'contractCloseEpochMs = Number(d.contract.close_timestamp) * 1000' in DASHBOARD_HTML
    assert 'setInterval(tickTimers, 250)' in DASHBOARD_HTML
    assert 'serverClockAnchorEpochMs + (performance.now() - serverClockAnchorPerfMs)' in DASHBOARD_HTML
    assert 'fetch("/api/time", {cache:"no-store"})' in DASHBOARD_HTML
    assert "serverClockOffsetMs" not in DASHBOARD_HTML
    assert 'document.getElementById("forecast-badge")' in DASHBOARD_HTML
    assert 'Lock Engine F Forecast' in DASHBOARD_HTML


@pytest.mark.asyncio
async def test_orchestrator_survives_prediction_cycle_exception(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator
    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(MarketDataHub(), ledger)
    calls = {"evaluate": 0}

    def broken_evaluate_once(now=None, record_to_db=True):
        calls["evaluate"] += 1
        raise RuntimeError("injected cycle failure")

    async def stop_after_cycle(_seconds):
        orchestrator.stop()

    orchestrator.evaluate_once = broken_evaluate_once
    monkeypatch.setattr("app.orchestrator.asyncio.sleep", stop_after_cycle)
    await orchestrator.run()

    health = orchestrator.get_runtime_health()
    assert calls["evaluate"] == 1
    assert health["running"] is False
    assert health["total_cycle_errors"] == 1
    assert health["consecutive_cycle_errors"] == 1
    assert "injected cycle failure" in health["last_cycle_error"]
    assert health["health"] == "STOPPED"



def test_fresh_official_brti_contributes_independent_strike_evidence():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    evidence = EvidenceEngine().analyze(
        now, 90010.0, contract, [], [], [], [],
        official_brti_price=90020.0, official_brti_age_sec=0.5,
    )

    assert evidence.official_brti_price == 90020.0
    assert evidence.official_brti_age_seconds == 0.5
    assert evidence.brti_distance_from_strike == 20.0
    assert evidence.brti_spot_spread_bps is not None
    assert evidence.brti_conflict is False


@pytest.mark.parametrize("brti_mode", ["fresh", "stale", "unavailable"])
def test_orchestrator_only_passes_fresh_official_brti_to_evidence(monkeypatch, tmp_path, brti_mode):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    hub.kalshi_contract = _contract(
        open_timestamp=now - 10.0, close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub.last_kalshi_ts = now
    if brti_mode != "unavailable":
        hub.brti_status = "LIVE"
        hub.official_brti_price = 90020.0
        hub.last_brti_ts = (
            now if brti_mode == "fresh"
            else now - settings.stale_feed_threshold_sec - 1.0
        )

    orchestrator = PredictorOrchestrator(
        hub, PredictionLedger()
    )
    captured = {}
    original_analyze = orchestrator.evidence_engine.analyze

    def capture_brti(*args, **kwargs):
        captured["price"] = kwargs.get("official_brti_price")
        captured["age"] = kwargs.get("official_brti_age_sec")
        return original_analyze(*args, **kwargs)

    monkeypatch.setattr(orchestrator.evidence_engine, "analyze", capture_brti)
    snapshot = orchestrator.evaluate_once(now=now, record_to_db=False)

    assert snapshot is not None
    if brti_mode == "fresh":
        assert captured["price"] == 90020.0
        assert captured["age"] == pytest.approx(0.0)
        assert snapshot.evidence.official_brti_price == 90020.0
    else:
        assert captured["price"] is None
        assert captured["age"] is None
        assert snapshot.evidence.official_brti_price is None


def test_final_decision_uses_blended_official_brti_strike_evidence():
    from scipy.stats import norm
    from app.engines import FinalDecisionEngine
    from app.models import EarlyPredictionReport, WeaknessReport

    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    evidence = EvidenceEngine().analyze(
        now, 90010.0, contract, [], [], [], [],
        official_brti_price=90030.0, official_brti_age_sec=0.5,
    )
    spot_z = EvidenceEngine._strike_z_score(
        90010.0, contract.strike_price,
        contract.seconds_remaining, evidence.volatility_annualized,
    )
    brti_z = EvidenceEngine._strike_z_score(
        90030.0, contract.strike_price,
        contract.seconds_remaining, evidence.volatility_annualized,
    )
    expected_blended_z = 0.65 * spot_z + 0.35 * brti_z
    assert evidence.strike_z_score == pytest.approx(expected_blended_z, abs=1e-6)

    weakness = WeaknessReport(weakness_score=0.0)
    regime = RegimeReport(
        regime=MarketRegime.STABLE,
        volatility_percentile=0.0,
        chop_index=0.0,
        min_confidence_required=0.0,
        explanation="test",
    )
    early = EarlyPredictionReport(
        active_window=False, signal=Signal.WAIT, confidence=50.0,
        up_prob=0.5, down_prob=0.5, rationale="test",
    )
    final = FinalDecisionEngine().decide(
        evidence, weakness, regime, early, contract,
        {"evidence": 1.0, "early": 1.0},
    )
    micro_drift_z = (evidence.raw_score / 100.0) * 0.40
    expected_probability = round(float(norm.cdf(expected_blended_z + micro_drift_z)), 4)

    assert final.up_probability == expected_probability


def test_early_prediction_is_independent_of_blended_brti_strike_probability():
    from app.models import WeaknessReport

    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    evidence = EvidenceEngine().analyze(
        now, 90010.0, contract, [], [], [], [],
        official_brti_price=89980.0, official_brti_age_sec=0.5,
    )
    weakness = WeaknessReport(weakness_score=0.0)
    bullish_strike = evidence.model_copy(update={"strike_z_score": 2.5})
    bearish_strike = evidence.model_copy(update={"strike_z_score": -2.5})

    early_bullish = EarlyPredictionEngine().analyze(bullish_strike, weakness, contract)
    early_bearish = EarlyPredictionEngine().analyze(bearish_strike, weakness, contract)

    # Strike/BRTI location belongs to the final probability model; it must not
    # be counted again by the independent early tape/flow model.
    assert early_bullish.up_prob == early_bearish.up_prob
    assert early_bullish.down_prob == early_bearish.down_prob


def test_brti_spot_strike_conflict_is_flagged_as_weakness():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    evidence = EvidenceEngine().analyze(
        now, 90020.0, contract, [], [], [], [],
        official_brti_price=89980.0, official_brti_age_sec=0.5,
    )
    weakness = WeaknessEngine().analyze(evidence, spot_age_sec=0.0, book_age_sec=0.0)

    assert evidence.brti_conflict is True
    assert weakness.weakness_score >= 20.0
    assert any("BRTI and spot" in flag for flag in weakness.flags)


def test_stale_official_brti_is_excluded_without_becoming_a_spot_fallback():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    without_brti = EvidenceEngine().analyze(now, 90020.0, contract, [], [], [], [])
    with_stale_brti = EvidenceEngine().analyze(
        now, 90020.0, contract, [], [], [], [],
        official_brti_price=89980.0, official_brti_age_sec=10.0,
    )

    assert with_stale_brti.official_brti_price is None
    assert with_stale_brti.brti_distance_from_strike is None
    assert with_stale_brti.raw_score == without_brti.raw_score


def test_evidence_engine_excludes_stale_bars_from_short_horizon_signals():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    stale_bars = [
        (int(now - 900 + i), 80000.0 + i * 100.0, 1.0)
        for i in range(120)
    ]
    baseline = EvidenceEngine().analyze(now, 90010.0, contract, [], [], [], [])
    with_stale_history = EvidenceEngine().analyze(
        now, 90010.0, contract, [], stale_bars, [], []
    )

    assert with_stale_history.momentum_score == baseline.momentum_score
    assert with_stale_history.rsi_14 == baseline.rsi_14
    assert with_stale_history.volatility_annualized == baseline.volatility_annualized
    assert with_stale_history.structure_bias == baseline.structure_bias


def test_sparse_trade_bars_are_resampled_by_elapsed_time_not_observation_count():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 150.0, seconds_remaining=750.0)

    def price_at(second):
        step = int((second - (now - 120.0)) // 10)
        return 90000.0 + step * 2.0

    sparse_bars = [
        (int(second), price_at(second), 1.0)
        for second in range(int(now - 120), int(now), 10)
    ]
    dense_bars = [
        (int(second), price_at(second), 1.0)
        for second in range(int(now - 120), int(now))
    ]

    engine = EvidenceEngine()
    sparse = engine.analyze(now, 90024.0, contract, [], sparse_bars, [], [])
    dense = engine.analyze(now, 90024.0, contract, [], dense_bars, [], [])

    assert sparse.momentum_score == dense.momentum_score
    assert sparse.rsi_14 == dense.rsi_14
    assert sparse.volatility_annualized == dense.volatility_annualized
    assert sparse.structure_bias == dense.structure_bias


def test_evidence_engine_excludes_future_and_pre_contract_trades_and_bars():
    now = 1_000_000.0
    contract = _contract(open_timestamp=now - 100.0, seconds_remaining=800.0)
    trades = [
        (now - 5.0, 90010.0, 0.3, False),  # buy within contract
        (now - 3.0, 90009.0, 0.1, True),   # sell within contract
        (now - 150.0, 89990.0, 4.0, False), # before this contract opened
        (now + 1.0, 90020.0, 99.0, False), # future timestamp
    ]
    bars = [
        (int(now - 20), 90000.0, 0.1),
        (int(now + 20), 999999.0, 100.0),  # future bar must be ignored
    ]

    evidence = EvidenceEngine().analyze(
        now, 90010.0, contract, trades, bars, [], []
    )

    assert evidence.trade_delta_60s == 0.2
    assert evidence.cvd_contract == 0.2
    assert evidence.buying_pressure_pct == 75.0



def test_feed_status_marks_future_timestamps_invalid():    hub = MarketDataHub()
    future = time.time() + 5.0
    hub.last_spot_ts = future
    hub.active_spot_source = "BINANCE_SPOT"
    hub.last_book_ts = future
    hub.last_brti_ts = future
    hub.brti_status = "LIVE"
    hub.official_brti_price = 90000.0

    statuses = {item.name: item.status for item in hub.get_feed_statuses()}

    assert statuses["Primary Spot Feed"] == "FUTURE_TIMESTAMP"
    assert statuses["Order Book Depth"] == "FUTURE_TIMESTAMP"
    assert statuses["CF Benchmarks BRTI"] == "FUTURE_TIMESTAMP"


def test_brti_without_source_timestamp_never_displays_as_live():
    hub = MarketDataHub()
    hub.brti_status = "LIVE"
    hub.official_brti_price = 90000.0
    hub.last_brti_ts = 0.0

    statuses = {item.name: item.status for item in hub.get_feed_statuses()}

    assert statuses["CF Benchmarks BRTI"] == "UNAVAILABLE"


def test_ingest_trade_rejects_invalid_and_future_market_data():
    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90000.0, 0.25, False, "TEST")
    baseline = (hub.live_price, hub.last_spot_ts, len(hub.trades), hub._current_sec_vol)

    hub.ingest_trade(now + 5.0, 999999.0, 10.0, False, "TEST")
    hub.ingest_trade(now + 0.1, float("nan"), 1.0, False, "TEST")
    hub.ingest_trade(now + 0.1, 90001.0, float("inf"), False, "TEST")
    hub.ingest_trade(now + 0.1, 90001.0, -1.0, False, "TEST")
    hub.ingest_trade(now + 0.1, 90001.0, 1.0, "not-bool", "TEST")
    hub.ingest_trade(now - 1.0, 80000.0, 1.0, False, "TEST")

    assert (hub.live_price, hub.last_spot_ts, len(hub.trades), hub._current_sec_vol) == baseline


def test_ingest_book_rejects_crossed_invalid_and_future_books():
    hub = MarketDataHub()
    now = time.time()
    hub.ingest_book(now, [(89999.0, 2.0)], [(90001.0, 3.0)])
    baseline = (list(hub.bids), list(hub.asks), hub.last_book_ts)

    hub.ingest_book(now + 5.0, [(90010.0, 1.0)], [(90011.0, 1.0)])
    hub.ingest_book(now + 0.1, [(90002.0, 1.0)], [(90001.0, 1.0)])
    hub.ingest_book(now + 0.1, [(float("nan"), 1.0)], [(90001.0, 1.0)])
    hub.ingest_book(now - 1.0, [(90002.0, 1.0)], [(90003.0, 1.0)])

    assert (hub.bids, hub.asks, hub.last_book_ts) == baseline


@pytest.mark.asyncio
async def test_synthetic_contracts_are_not_settled_with_later_fallback_price(tmp_path):
    from app.orchestrator import PredictorOrchestrator
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("SYNTH-BTC15M-OLD"))
    orchestrator = PredictorOrchestrator(MarketDataHub(), ledger)

    await orchestrator.reconcile_unsettled_contracts("SYNTH-BTC15M-NEW", fallback_price=99999.0)

    pending = ledger.get_recent_predictions(limit=10)
    assert pending[0].settled is False
    assert pending[0].settlement_source is None
    assert ledger.get_scoreboard().pending_predictions == 1



def test_one_second_bars_use_previous_second_last_trade_price():
    hub = MarketDataHub()
    sec = int(time.time()) - 3
    hub.ingest_trade(sec + 0.1, 90000.0, 0.2, False, "TEST")
    hub.ingest_trade(sec + 0.4, 90005.0, 0.3, False, "TEST")
    hub.ingest_trade(sec + 1.1, 90010.0, 0.1, False, "TEST")

    assert hub.bars_1s[-1][0] == sec
    assert hub.bars_1s[-1][1] == 90005.0
    assert hub.bars_1s[-1][2] == pytest.approx(0.5)



def test_volume_ratio_is_neutral_when_history_is_shorter_than_one_minute():
    now = time.time()
    contract = _contract(open_timestamp=now - 20.0, seconds_remaining=800.0)
    trades = [
        (now - 5.0, 90010.0, 0.3, False),
        (now - 3.0, 90009.0, 0.1, True),
    ]
    evidence = EvidenceEngine().analyze(now, 90010.0, contract, trades, [], [], [])

    assert evidence.volume_ratio == 1.0



def test_primary_binance_feed_is_not_mirrored_into_independent_shadow_venue():
    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90000.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(89999.0, 2.0)], [(90001.0, 2.0)])

    snapshot = hub.get_multi_exchange_snapshot()
    shadow_binance = next(v for v in snapshot["venues"] if v["venue"] == "BINANCE")

    assert shadow_binance["status"] == "UNAVAILABLE"
    assert shadow_binance["mid"] is None



def test_coinbase_fallback_ticker_never_invents_liquidity_sizes():
    hub = MarketDataHub()

    assert hub._coinbase_ticker_book({"best_bid": "90000", "best_ask": "90001"}) is None
    assert hub._coinbase_ticker_book({
        "best_bid": "90000", "best_ask": "90001",
        "best_bid_size": "2", "best_ask_size": "3",
    }) == ([(90000.0, 2.0)], [(90001.0, 3.0)])
    assert hub._coinbase_ticker_book({
        "best_bid": "90002", "best_ask": "90001",
        "best_bid_size": "2", "best_ask_size": "3",
    }) is None


def test_kalshi_feed_future_timestamp_is_not_live():
    hub = MarketDataHub()
    hub.last_kalshi_ts = time.time() + 5.0
    hub.kalshi_contract = _contract(open_timestamp=time.time() - 10.0, close_timestamp=time.time() + 500.0)

    statuses = {item.name: item.status for item in hub.get_feed_statuses()}

    assert statuses["Kalshi KXBTC15M"] == "FUTURE_TIMESTAMP"



def test_brti_parser_rejects_nonfinite_and_nonpositive_values():
    hub = MarketDataHub()

    assert hub._parse_brti_value("90000.25") == 90000.25
    assert hub._parse_brti_value(float("nan")) is None
    assert hub._parse_brti_value(float("inf")) is None
    assert hub._parse_brti_value("-1") is None
    assert hub._parse_brti_value("invalid") is None


def test_brti_payload_parses_documented_array_and_uses_source_timestamp():
    hub = MarketDataHub()
    now = 1_800_000_000.0
    parsed = hub._parse_brti_payload({
        "serverTime": "2027-01-15T08:00:00Z",
        "payload": [
            {"value": "89999.50", "time": int((now - 2.0) * 1000)},
            {"value": "90001.25", "time": int((now - 0.5) * 1000)},
        ],
    }, now=now)

    assert parsed == (90001.25, now - 0.5)


def test_brti_payload_accepts_direct_pass_through_envelope():
    now = 1_800_000_000.0
    parsed = MarketDataHub._parse_brti_payload({
        "data": {"payload": [{"value": "90000.5", "time": int(now * 1000)}]}
    }, now=now)

    assert parsed == (90000.5, now)


@pytest.mark.parametrize("item", [
    {"value": "90000", "time": 1_799_999_000_000},
    {"value": "90000", "time": 1_800_000_010_000},
    {"value": "90000", "time": 1_800_000_000_000, "repeatOfPreviousValue": True},
    {"value": "nan", "time": 1_800_000_000_000},
    {"value": "90000", "time": None},
])
def test_brti_payload_rejects_stale_future_repeated_or_invalid_values(item):
    assert MarketDataHub._parse_brti_payload({"payload": [item]}, now=1_800_000_000.0) is None



def test_orchestrator_filters_future_bars_before_regime_engine(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator

    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90000.0, 0.2, False, "TEST")
    hub.kalshi_contract = _contract(
        open_timestamp=now - 10.0,
        close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub.last_kalshi_ts = now
    hub.bars_1s.extend([
        (int(now - 5), 89999.0, 0.1),
        (int(now + 100), 999999.0, 100.0),
    ])
    orchestrator = PredictorOrchestrator(hub, PredictionLedger())
    captured = {}
    original = orchestrator.regime_engine.analyze

    def spy(evidence, weakness, bars):
        captured["bars"] = list(bars)
        return original(evidence, weakness, bars)

    monkeypatch.setattr(orchestrator.regime_engine, "analyze", spy)
    orchestrator.evaluate_once(now=now, record_to_db=False)

    assert captured["bars"]
    assert all(bar[0] <= now for bar in captured["bars"])



def test_future_kalshi_timestamp_cannot_authorize_official_contract():
    from app.orchestrator import PredictorOrchestrator

    hub = MarketDataHub()
    now = time.time()
    hub.kalshi_contract = _contract(open_timestamp=now - 100.0, close_timestamp=now + 800.0)
    hub.last_kalshi_ts = now + 5.0
    orchestrator = PredictorOrchestrator(hub, PredictionLedger())

    resolved = orchestrator._resolve_active_contract(now, 90000.0)

    assert resolved.is_official_kalshi is False
    assert resolved.ticker.startswith("SYNTH-BTC15M-")



@pytest.mark.asyncio
async def test_primary_spot_stream_clean_close_is_backed_off(monkeypatch):
    hub = MarketDataHub()
    hub._running = True
    calls = {"worker": 0, "sleep": 0}

    async def clean_close():
        calls["worker"] += 1

    async def stop_after_backoff(_seconds):
        calls["sleep"] += 1
        hub._running = False

    monkeypatch.setattr(hub, "_run_binance_ws", clean_close)
    monkeypatch.setattr(hub, "_run_coinbase_ws", clean_close)
    monkeypatch.setattr("app.feeds.asyncio.sleep", stop_after_backoff)

    await hub._spot_stream_loop()
    assert calls == {"worker": 1, "sleep": 1}



def test_unsettled_settlement_queue_contains_only_official_contracts_and_is_ordered(tmp_path):
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("SYNTH-BTC15M-A", ts=1794873600.0))
    ledger.record_prediction(_prediction_record("KXBTC15M-NEW", ts=1794873700.0))
    ledger.record_prediction(_prediction_record("KXBTC15M-OLD", ts=1794873500.0))

    assert ledger.get_unsettled_contract_tickers() == ["KXBTC15M-OLD", "KXBTC15M-NEW"]


@pytest.mark.asyncio
async def test_settlement_reconciliation_is_bounded_to_five_and_three_concurrent(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    for i in range(7):
        ledger.record_prediction(_prediction_record(f"KXBTC15M-{i}", ts=1794873600.0 + i))

    hub = MarketDataHub()
    orchestrator = PredictorOrchestrator(hub, ledger)
    calls = []
    active = 0
    max_active = 0

    async def fake_verify(ticker):
        nonlocal active, max_active
        calls.append(ticker)
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0)
        active -= 1
        return None

    monkeypatch.setattr(hub, "fetch_verified_kalshi_settlement", fake_verify)
    await orchestrator.reconcile_unsettled_contracts("KXBTC15M-CURRENT", fallback_price=90000.0)

    assert len(calls) == 5
    assert max_active <= 3


@pytest.mark.asyncio
async def test_settlement_reconciliation_rotates_past_unsettled_oldest_batch(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    tickers = [f"KXBTC15M-ROTATE-{i}" for i in range(7)]
    for i, ticker in enumerate(tickers):
        ledger.record_prediction(_prediction_record(ticker, ts=1794873600.0 + i))

    hub = MarketDataHub()
    orchestrator = PredictorOrchestrator(hub, ledger)
    calls = []

    async def fake_verify(ticker):
        calls.append(ticker)
        # The first five are not settled yet; the last two have official outcomes.
        return ("UP", None) if ticker in tickers[5:] else None

    monkeypatch.setattr(hub, "fetch_verified_kalshi_settlement", fake_verify)

    await orchestrator.reconcile_unsettled_contracts("KXBTC15M-CURRENT")
    assert calls == tickers[:5]
    assert set(ledger.get_unsettled_contract_tickers()) == set(tickers)

    calls.clear()
    await orchestrator.reconcile_unsettled_contracts("KXBTC15M-CURRENT")

    assert calls == tickers[5:] + tickers[:3]
    assert set(ledger.get_unsettled_contract_tickers()) == set(tickers[:5])


@pytest.mark.asyncio
async def test_settlement_scheduler_runs_even_without_prediction_snapshots(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    orchestrator = PredictorOrchestrator(
        MarketDataHub(), PredictionLedger()
    )
    scheduled = []

    async def fake_worker(current_ticker, fallback_price):
        scheduled.append((current_ticker, fallback_price))

    monkeypatch.setattr(orchestrator, "_settlement_worker", fake_worker)

    for _ in range(9):
        orchestrator._schedule_settlement_reconciliation("")
    assert orchestrator._settlement_task is None

    # The tenth loop schedules reconciliation even though there was no
    # market snapshot and therefore no prediction/execution cycle.
    orchestrator._schedule_settlement_reconciliation("")
    await orchestrator._settlement_task

    assert scheduled == [("", 0.0)]


def test_exchange_timestamp_parser_handles_milliseconds_and_iso():
    hub = MarketDataHub()
    epoch = 1_800_000_000.0

    assert hub._parse_exchange_timestamp(epoch * 1000) == epoch
    assert hub._parse_exchange_timestamp("2027-01-15T08:00:00Z") == datetime(
        2027, 1, 15, 8, 0, 0, tzinfo=timezone.utc
    ).timestamp()
    assert hub._parse_exchange_timestamp("bad-time", fallback=123.0) == 123.0



@pytest.mark.asyncio
async def test_spot_stream_retries_binance_after_coinbase_fallback(monkeypatch):
    hub = MarketDataHub()
    hub._running = True
    clock = 0.0
    attempts = []

    async def fake_binance():
        attempts.append("binance")
        if attempts.count("binance") == 1:
            raise RuntimeError("temporary primary feed outage")
        hub._running = False

    async def fake_coinbase():
        attempts.append("coinbase")

    async def fake_sleep(seconds):
        nonlocal clock
        clock += seconds

    monkeypatch.setattr(time, "monotonic", lambda: clock)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(hub, "_run_binance_ws", fake_binance)
    monkeypatch.setattr(hub, "_run_coinbase_ws", fake_coinbase)

    await hub._spot_stream_loop()

    assert attempts[0] == "binance"
    assert "coinbase" in attempts
    assert attempts[-1] == "binance"
    assert attempts.count("binance") == 2


def test_fallback_source_can_recover_when_exchange_clock_is_behind():
    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90000.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_trade(now - 0.2, 90001.0, 0.1, False, "COINBASE_FALLBACK")

    assert hub.active_spot_source == "COINBASE_FALLBACK"
    assert hub.live_price == 90001.0
    assert hub.last_spot_ts == pytest.approx(now - 0.2)
    assert hub.get_feed_statuses()[0].age_seconds >= 0.0


def test_order_book_source_switch_allows_timestamp_offset():
    hub = MarketDataHub()
    now = time.time()
    hub.ingest_book(now, [(89999.0, 2.0)], [(90001.0, 2.0)], source="BINANCE_SPOT")
    hub.ingest_book(now - 0.2, [(90000.0, 2.0)], [(90002.0, 2.0)], source="COINBASE_FALLBACK")

    assert hub.bids[0][0] == 90000.0
    assert hub.last_book_source == "COINBASE_FALLBACK"
    assert hub.last_book_ts == pytest.approx(now - 0.2)



def test_execution_rechecks_actual_contract_expiration_not_snapshot_countdown(tmp_path):
    ledger = PredictionLedger()
    now = time.time()
    stale_countdown = _contract(
        open_timestamp=now - 100.0,
        close_timestamp=now + 20.0,
        seconds_remaining=800.0,
    )
    hub = _fresh_execution_hub(stale_countdown)
    manager = ExecutionManager(hub, ledger)

    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(now, stale_countdown, _decision()))

    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_rejects_nonfinite_model_probability(tmp_path):
    ledger = PredictionLedger()
    contract = _contract()
    manager = ExecutionManager(_fresh_execution_hub(contract), ledger)
    bad_probability = _decision(up=float("nan"), down=0.05)

    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), _contract(), bad_probability))

    assert result is None
    assert ledger.get_recent_trades() == []



def test_execution_does_not_reenter_contract_after_paper_trade_settlement(tmp_path):
    ledger = PredictionLedger()
    contract = _contract()
    manager = ExecutionManager(_fresh_execution_hub(contract), ledger)

    import asyncio
    first = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))
    assert first is not None
    ledger.settle_contract(contract.ticker, settlement_price=90020.0, official_result="UP")

    second = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))

    assert second is None
    assert len(ledger.get_recent_trades()) == 1
    assert ledger.get_recent_trades()[0].status == "SETTLED"



@pytest.mark.parametrize("stale_feed", ["spot", "kalshi"])
def test_execution_refuses_stale_feed_independent_of_prediction(tmp_path, stale_feed):
    ledger = PredictionLedger()
    contract = _contract()
    hub = _fresh_execution_hub(contract)
    if stale_feed == "spot":
        hub.last_spot_ts = time.time() - 30.0
    else:
        hub.last_kalshi_ts = time.time() - 20.0
    manager = ExecutionManager(hub, ledger)

    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))

    assert result is None
    assert ledger.get_recent_trades() == []



@pytest.mark.asyncio
async def test_prediction_loop_does_not_wait_for_settlement_worker(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(MarketDataHub(), ledger)
    original_sleep = asyncio.sleep
    cycles = {"count": 0}
    settlement = {"started": False, "completed": False}

    fake_snapshot = SimpleNamespace(        contract=SimpleNamespace(ticker="KXBTC15M-CURRENT"),
        evidence=SimpleNamespace(live_price=90000.0),
        final=SimpleNamespace(signal=Signal.WAIT),
        scoreboard=None,
        recent_predictions=[],
        recent_trades=[],
    )

    def fake_evaluate_once(now=None, record_to_db=True):
        cycles["count"] += 1
        return fake_snapshot

    async def fake_execute(*args, **kwargs):
        return None

    async def slow_reconcile(*args, **kwargs):
        settlement["started"] = True
        await original_sleep(0.2)
        settlement["completed"] = True

    async def run_loop_sleep(_seconds):
        await original_sleep(0)
        if cycles["count"] >= 10:
            orchestrator.stop()

    orchestrator.evaluate_once = fake_evaluate_once
    orchestrator.reconcile_unsettled_contracts = slow_reconcile
    monkeypatch.setattr(orchestrator.executor, "evaluate_and_execute", fake_execute)
    monkeypatch.setattr("app.orchestrator.asyncio.sleep", run_loop_sleep)

    await orchestrator.run()

    assert cycles["count"] >= 10
    assert settlement["started"] is True
    assert settlement["completed"] is False
    await orchestrator.shutdown_background_tasks()



def test_stale_spot_feed_degrades_prediction_and_recovers_on_fresh_trade(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    hub = MarketDataHub()
    hub.ingest_trade(now - settings.stale_feed_threshold_sec - 2.0, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    hub.kalshi_contract = _contract(
        open_timestamp=now - 10.0, close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub.last_kalshi_ts = now
    orchestrator = PredictorOrchestrator(
        hub, PredictionLedger()
    )

    degraded = orchestrator.evaluate_once(now=now, record_to_db=False)
    assert degraded is not None
    assert degraded.weakness.feed_degraded is True
    assert any("spot feed" in flag.lower() for flag in degraded.weakness.flags)
    assert degraded.final.signal == Signal.WAIT

    recovered_at = time.time()
    hub.ingest_trade(recovered_at, 90012.0, 0.2, False, "BINANCE_SPOT")
    recovered = orchestrator.evaluate_once(now=recovered_at, record_to_db=False)

    assert recovered is not None
    assert recovered.weakness.feed_degraded is False
    assert not any("spot feed" in flag.lower() for flag in recovered.weakness.flags)


def test_stale_order_book_is_excluded_and_degrades_prediction(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.bids = [(90009.0, 100.0)]
    hub.asks = [(90011.0, 1.0)]
    hub.last_book_source = "BINANCE_SPOT"
    hub.last_book_ts = now - 30.0
    orchestrator = PredictorOrchestrator(hub, PredictionLedger())

    snapshot = orchestrator.evaluate_once(now=now, record_to_db=False)

    assert snapshot.weakness.feed_degraded is True
    assert any("order-book feed" in flag for flag in snapshot.weakness.flags)
    assert snapshot.evidence.orderbook_imbalance == 0.0
    assert snapshot.final.signal == Signal.WAIT



def test_coinbase_primary_trade_side_flag_is_not_coerced():
    hub = MarketDataHub()

    assert hub._coinbase_is_buyer_maker("buy") is True
    assert hub._coinbase_is_buyer_maker("sell") is False
    assert hub._coinbase_is_buyer_maker(None) is None
    assert hub._coinbase_is_buyer_maker("unknown") is None

def test_official_prediction_lock_is_idempotent_per_contract(tmp_path):
    ledger = PredictionLedger()
    first = ledger.record_prediction(_prediction_record("KXBTC15M-LOCK-ONCE", final=Signal.UP))
    duplicate = ledger.record_prediction(_prediction_record("KXBTC15M-LOCK-ONCE", final=Signal.DOWN, ts=1794873660.0))

    assert first > 0
    assert duplicate == first
    rows = ledger.get_recent_predictions(limit=10)
    assert len(rows) == 1
    assert rows[0].final_signal == Signal.UP
    assert ledger.has_official_prediction_lock("KXBTC15M-LOCK-ONCE")


def test_official_settlement_is_idempotent_and_does_not_regrade_lock(tmp_path):
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("KXBTC15M-SETTLE-ONCE", final=Signal.UP))

    first_update = ledger.settle_contract(
        "KXBTC15M-SETTLE-ONCE",
        settlement_price=90020.0,
        official_result="UP",
        settlement_source="KALSHI_VERIFIED",
    )
    duplicate_update = ledger.settle_contract(
        "KXBTC15M-SETTLE-ONCE",
        settlement_price=89980.0,
        official_result="DOWN",
        settlement_source="LATE_CONFLICTING_RETRY",
    )

    assert first_update == 1
    assert duplicate_update == 0
    row = ledger.get_recent_predictions(limit=1)[0]
    assert row.settled is True
    assert row.actual_outcome == "UP"
    assert row.final_won is True
    assert row.settlement_source == "KALSHI_VERIFIED"

def test_contract_rollover_clears_previous_directional_confirmation_candidate(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    new_contract = _contract(
        ticker="KXBTC15M-ROLLOVER-NEW",
        open_timestamp=now - 10.0,
        close_timestamp=now + 800.0,
    )
    hub.kalshi_contract = new_contract
    hub.last_kalshi_ts = now

    orchestrator = PredictorOrchestrator(
        hub, PredictionLedger()
    )
    orchestrator.active_contract = _contract(
        ticker="KXBTC15M-ROLLOVER-OLD",
        open_timestamp=now - 910.0,
        close_timestamp=now - 10.0,
    )
    orchestrator._directional_lock_candidate_ticker = "KXBTC15M-ROLLOVER-OLD"
    orchestrator._directional_lock_candidate = Signal.UP
    orchestrator._directional_lock_candidate_count = 1
    orchestrator.last_logged_ts = now - 5.0

    snapshot = orchestrator.evaluate_once(now=now, record_to_db=False)

    assert snapshot is not None
    assert snapshot.contract.ticker == "KXBTC15M-ROLLOVER-NEW"
    assert orchestrator._directional_lock_candidate_ticker is None
    assert orchestrator._directional_lock_candidate == Signal.WAIT
    assert orchestrator._directional_lock_candidate_count == 0
    assert orchestrator.last_logged_ts == 0.0

def test_directional_lock_requires_two_consecutive_matching_votes(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    contract = _contract(open_timestamp=now - 10.0, close_timestamp=now + 800.0)
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(hub, ledger)
    monkeypatch.setattr(orchestrator.final_engine, "decide", lambda *a, **k: _decision(Signal.UP))

    first = orchestrator.evaluate_once(now=now, record_to_db=True)
    assert first is not None
    assert ledger.get_recent_predictions() == []
    assert orchestrator._directional_lock_candidate_count == 1

    second = orchestrator.evaluate_once(now=now + 1.0, record_to_db=True)
    assert second is not None
    rows = ledger.get_recent_predictions()
    assert len(rows) == 1
    assert rows[0].contract_ticker == contract.ticker
    assert rows[0].final_signal == Signal.UP
    assert ledger.has_official_prediction_lock(contract.ticker)


@pytest.mark.parametrize(
    "votes",
    [
        [Signal.UP, Signal.WAIT, Signal.UP],
        [Signal.UP, Signal.DOWN, Signal.UP],
    ],
)
def test_wait_or_reversal_resets_directional_confirmation(monkeypatch, tmp_path, votes):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    contract = _contract(open_timestamp=now - 10.0, close_timestamp=now + 800.0)
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(hub, ledger)
    sequence = iter(votes)
    monkeypatch.setattr(
        orchestrator.final_engine, "decide",
        lambda *a, **k: _decision(next(sequence), 0.95 if votes[0] != Signal.DOWN else 0.05,
                                  0.05 if votes[0] != Signal.DOWN else 0.95),
    )

    for offset in range(len(votes)):
        orchestrator.evaluate_once(now=now + offset, record_to_db=True)

    assert ledger.get_recent_predictions() == []
    assert orchestrator._directional_lock_candidate == Signal.UP
    assert orchestrator._directional_lock_candidate_count == 1

def test_brti_cohort_is_preserved_and_performance_uses_settled_directional_locks(tmp_path):
    ledger = PredictionLedger()
    base = dict(
        timestamp_iso="2026-11-18 00:00:00 UTC",
        epoch_ts=1795046400.0,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=500.0,
        main_signal=Signal.UP,
        early_signal=Signal.UP,
        final_signal=Signal.UP,
        confidence=75.0,
        up_prob=0.75,
        down_prob=0.25,
        regime=MarketRegime.STABLE,    )
    records = [
        PredictionRecord(contract_ticker="KXBTC15M-COHORT-A", brti_cohort="AGREE_UP",
                         brti_side_at_lock="UP", spot_side_at_lock="UP", **base),
        PredictionRecord(contract_ticker="KXBTC15M-COHORT-B", brti_cohort="DISAGREE",
                         brti_side_at_lock="DOWN", spot_side_at_lock="UP", **base),
        PredictionRecord(contract_ticker="KXBTC15M-COHORT-C", brti_cohort="BRTI_UNAVAILABLE",
                         brti_side_at_lock=None, spot_side_at_lock="UP", **base),
    ]
    for record in records:
        ledger.record_prediction(record)
    ledger.settle_contract("KXBTC15M-COHORT-A", 90020.0, "UP", "KALSHI_OFFICIAL")
    ledger.settle_contract("KXBTC15M-COHORT-B", 89980.0, "DOWN", "KALSHI_OFFICIAL")
    # Leave the third contract pending: pending predictions must not affect accuracy.
    saved = {p.contract_ticker: p for p in ledger.get_recent_predictions()}
    assert saved["KXBTC15M-COHORT-A"].brti_cohort == "AGREE_UP"
    assert saved["KXBTC15M-COHORT-B"].brti_side_at_lock == "DOWN"
    assert saved["KXBTC15M-COHORT-C"].settled is False
    stats = {row["cohort"]: row for row in ledger.get_brti_cohort_performance()}
    assert stats["AGREE_UP"] == {
        "cohort": "AGREE_UP", "settled": 1, "wins": 1, "losses": 0, "accuracy_pct": 100.0
    }
    assert stats["DISAGREE"] == {
        "cohort": "DISAGREE", "settled": 1, "wins": 0, "losses": 1, "accuracy_pct": 0.0
    }
    assert "BRTI_UNAVAILABLE" not in stats
    report = ledger.get_brti_cohort_report()
    assert report["baseline"] == {
        "settled": 2, "wins": 1, "losses": 1, "accuracy_pct": 50.0
    }
    report_rows = {row["cohort"]: row for row in report["cohorts"]}
    assert report_rows["AGREE_UP"]["vs_baseline_pct_points"] == 50.0
    assert report_rows["DISAGREE"]["vs_baseline_pct_points"] == -50.0
    assert report_rows["AGREE_UP"]["sample_status"] == "LIMITED_SAMPLE"
    assert report_rows["DISAGREE"]["minimum_descriptive_sample"] == 30
    assert report["brti_vs_spot"]["sample_status"] == "LIMITED_SAMPLE"
    assert report["brti_vs_spot"]["minimum_descriptive_sample"] == 30
    assert report["brti_vs_spot"] == {
        "sample_size": 2,
        "sample_status": "LIMITED_SAMPLE",
        "minimum_descriptive_sample": 30,
        "brti": {"correct": 2, "incorrect": 0, "accuracy_pct": 100.0},
        "spot": {"correct": 1, "incorrect": 1, "accuracy_pct": 50.0},
        "brti_minus_spot_pct_points": 50.0,
        "note": "Paired settled locks only; descriptive comparison, not proof of predictive lift.",
    }
    assert "not statistical significance or proof of predictive lift" in report["interpretation"]

@pytest.mark.parametrize(
    "brti_mode,expected_cohort",
    [
        ("agree", "AGREE_UP"),
        ("disagree", "DISAGREE"),
        ("unavailable", "BRTI_UNAVAILABLE"),
    ],
)
def test_official_lock_freezes_brtI_alignment_at_prediction_time(
    monkeypatch, tmp_path, brti_mode, expected_cohort
):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    contract = _contract(open_timestamp=now - 10.0, close_timestamp=now + 800.0)
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)])
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    if brti_mode != "unavailable":
        hub.brti_status = "LIVE"
        hub.official_brti_price = 90020.0 if brti_mode == "agree" else 89980.0
        hub.last_brti_ts = now
    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(hub, ledger)
    monkeypatch.setattr(
        orchestrator.final_engine, "decide",
        lambda *a, **k: _decision(Signal.UP),
    )

    orchestrator.evaluate_once(now=now, record_to_db=True)
    orchestrator.evaluate_once(now=now + 1.0, record_to_db=True)

    rows = ledger.get_recent_predictions()
    assert len(rows) == 1
    assert rows[0].brti_cohort == expected_cohort
    assert rows[0].spot_side_at_lock == "UP"
    assert rows[0].brti_side_at_lock == (
        "UP" if brti_mode == "agree"
        else "DOWN" if brti_mode == "disagree"
        else None
    )


def test_brti_cohort_api_returns_settled_performance(monkeypatch):
    import app.main as main_module

    expected = [
        {"cohort": "AGREE_UP", "settled": 8, "wins": 6, "losses": 2, "accuracy_pct": 75.0}
    ]
    expected_report = {
        "baseline": {"settled": 20, "wins": 12, "losses": 8, "accuracy_pct": 60.0},
        "cohorts": expected,
        "brti_vs_spot": {
            "sample_size": 0,
            "brti": {"correct": 0, "incorrect": 0, "accuracy_pct": None},
            "spot": {"correct": 0, "incorrect": 0, "accuracy_pct": None},
            "brti_minus_spot_pct_points": None,
            "note": "Paired settled locks only; descriptive comparison, not proof of predictive lift.",
        },
        "interpretation": "Descriptive only; cohort differences are not proof of predictive lift.",
    }
    monkeypatch.setattr(
        main_module.ledger, "get_brti_cohort_report", lambda: expected_report
    )
    response = TestClient(main_module.app).get("/api/brti-cohorts")
    assert response.status_code == 200
    assert response.json() == expected_report


@pytest.mark.asyncio
async def test_official_settlement_without_expiration_value_never_uses_current_spot(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("KXBTC15M-NO-EXPIRATION-VALUE", final=Signal.UP))
    hub = MarketDataHub()
    async def verified_without_price(_ticker):
        return "UP", None

    monkeypatch.setattr(hub, "fetch_verified_kalshi_settlement", verified_without_price)
    orchestrator = PredictorOrchestrator(hub, ledger)

    await orchestrator.reconcile_unsettled_contracts(
        "KXBTC15M-CURRENT", fallback_price=99999.0
    )

    row = ledger.get_recent_predictions(limit=1)[0]
    assert row.settled is True
    assert row.actual_outcome == "UP"
    assert row.final_won is True
    assert row.settlement_price is None
    assert row.settlement_source == "KALSHI_VERIFIED"


def test_settlement_without_official_result_requires_a_valid_contract_price(tmp_path):
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("KXBTC15M-UNVERIFIED-SETTLEMENT"))

    assert ledger.settle_contract(
        "KXBTC15M-UNVERIFIED-SETTLEMENT",
        settlement_price=None,
        official_result=None,
        settlement_source="SPOT_CLOSE_PROXY",
    ) == 0

    row = ledger.get_recent_predictions(limit=1)[0]
    assert row.settled is False
    assert row.actual_outcome is None


@pytest.mark.parametrize("side,expected_pnl", [("YES", 0.79), ("NO", -0.21)])
def test_verified_outcome_settles_paper_trade_without_prediction_lock(
    tmp_path, side, expected_pnl
):
    ledger = PredictionLedger()
    ledger.record_trade(TradeRecord(
        mode="paper",
        contract_ticker="KXBTC15M-TRADE-WITHOUT-LOCK",
        timestamp_iso="2026-11-18 00:00:00 UTC",
        side=side,
        contracts=1,
        entry_price=0.20,
        quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.01,
        fair_prob_at_entry=0.70,
        net_edge=0.49,
        status="OPEN",
        notes="Trade may be opened before the two-vote official forecast lock.",
    ))

    # The official Kalshi result remains authoritative even when expiration_value
    # is missing and no scored prediction row exists for this contract.
    ledger.settle_contract(
        "KXBTC15M-TRADE-WITHOUT-LOCK",
        settlement_price=None,
        official_result="UP",
        settlement_source="KALSHI_VERIFIED",
    )

    trades = ledger.get_recent_trades(limit=1)
    assert len(trades) == 1
    assert trades[0].status == "SETTLED"
    assert trades[0].pnl_usd == pytest.approx(expected_pnl)

@pytest.mark.asyncio
async def test_settlement_reconciler_discovers_open_trade_without_prediction_lock(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    ticker = "KXBTC15M-TRADE-ONLY-RECONCILE"
    ledger = PredictionLedger()
    ledger.record_trade(TradeRecord(
        mode="paper",
        contract_ticker=ticker,
        timestamp_iso="2026-11-18 00:00:00 UTC",
        side="YES",
        contracts=1,
        entry_price=0.20,
        quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.01,
        fair_prob_at_entry=0.70,
        net_edge=0.49,
        status="OPEN",
        notes="No forecast lock exists yet.",
    ))
    hub = MarketDataHub()

    async def verified_outcome(_ticker):
        assert _ticker == ticker
        return "UP", None

    monkeypatch.setattr(hub, "fetch_verified_kalshi_settlement", verified_outcome)
    orchestrator = PredictorOrchestrator(hub, ledger)

    assert ledger.get_unsettled_contract_tickers() == [ticker]
    await orchestrator.reconcile_unsettled_contracts("KXBTC15M-CURRENT")

    trade = ledger.get_recent_trades(limit=1)[0]
    assert trade.status == "SETTLED"
    assert trade.pnl_usd == pytest.approx(0.79)

def test_order_book_source_mismatch_degrades_and_recovers_prediction(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(
        now, [(90009.0, 2.0)], [(90011.0, 2.0)], source="BINANCE_SPOT"
    )
    hub.kalshi_contract = _contract(
        open_timestamp=now - 10.0,
        close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub.last_kalshi_ts = now

    # Spot switches to Coinbase while the last fresh book still belongs to Binance.
    hub.ingest_trade(now, 90012.0, 0.2, False, "COINBASE_FALLBACK")
    statuses = {item.name: item.status for item in hub.get_feed_statuses()}
    assert statuses["Order Book Depth"] == "SOURCE_MISMATCH"
    orchestrator = PredictorOrchestrator(
        hub, PredictionLedger()
    )
    degraded = orchestrator.evaluate_once(now=now, record_to_db=False)
    assert degraded.weakness.feed_degraded is True
    assert any("source does not match" in flag.lower() for flag in degraded.weakness.flags)
    assert degraded.final.signal == Signal.WAIT

    # A fresh book from the active Coinbase source restores usable evidence.
    hub.ingest_book(
        now, [(90011.0, 2.0)], [(90013.0, 2.0)], source="COINBASE_FALLBACK"
    )
    recovered = orchestrator.evaluate_once(now=now, record_to_db=False)
    statuses = {item.name: item.status for item in hub.get_feed_statuses()}
    assert statuses["Order Book Depth"] == "LIVE"
    assert recovered.weakness.feed_degraded is False


@pytest.mark.asyncio
async def test_supervised_feed_worker_restarts_after_unexpected_crash(monkeypatch):
    hub = MarketDataHub()
    hub._running = True
    attempts = []

    async def flaky_worker():
        attempts.append(len(attempts) + 1)
        if len(attempts) == 1:
            raise RuntimeError("simulated websocket task crash")
        hub._running = False

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_wait)

    await hub._run_supervised_feed("Spot Feed Worker", flaky_worker, retry_delay=0.01)

    runtime = hub._feed_runtime["Spot Feed Worker"]
    assert attempts == [1, 2]
    assert runtime["restart_count"] == 1
    assert runtime["last_error"] == "RuntimeError: simulated websocket task crash"
    assert runtime["state"] == "STOPPED"


def test_feed_statuses_expose_supervisor_restart_diagnostics():
    hub = MarketDataHub()
    runtime = hub._feed_runtime["Kalshi Feed Worker"]
    runtime.update(
        state="RESTARTING",
        restart_count=2,
        last_error="TimeoutError: simulated timeout",
        started_ts=time.time() - 1.0,
    )

    statuses = {status.name: status for status in hub.get_feed_statuses()}

    assert statuses["Kalshi Feed Worker"].status == "RESTARTING"
    assert "restarts=2" in statuses["Kalshi Feed Worker"].detail
    assert "simulated timeout" in statuses["Kalshi Feed Worker"].detail


def test_all_prediction_engines_disclose_stale_cached_spot_and_recover(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)], source="BINANCE_SPOT")
    hub.kalshi_contract = _contract(
        open_timestamp=now - 10.0,
        close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub.last_kalshi_ts = now
    # The cached price remains populated, but its source timestamp is stale.
    hub.last_spot_ts = now - settings.stale_feed_threshold_sec - 1.0

    ledger = PredictionLedger()
    orchestrator = PredictorOrchestrator(hub, ledger)
    stale = orchestrator.evaluate_once(now=now, record_to_db=True)

    assert stale is not None
    assert stale.final.signal == Signal.WAIT
    assert "stale or degraded" in stale.final.rationale.lower()
    assert stale.engines.engine_a_evidence["status"] == "BLOCKED_STALE_INPUT"
    assert stale.engines.engine_b_weakness["status"] == "DEGRADED_FEED"
    assert stale.engines.engine_c_regime["status"] == "BLOCKED_STALE_INPUT"
    assert stale.engines.engine_d_early["status"] == "BLOCKED_STALE_INPUT"
    assert stale.engines.engine_e_final["status"] == "BLOCKED_STALE_INPUT"
    assert stale.engines.engine_a_evidence["input_health"]["fresh"] is False
    assert stale.engines.engine_a_evidence["input_health"]["contract_ticker"] == stale.contract.ticker
    assert ledger.has_official_prediction_lock(stale.contract.ticker) is False

    # Once both live spot and its matching book recover, engine health recovers
    # on the next evaluation without restarting the orchestrator or engines.
    recovered_at = time.time()
    hub.ingest_trade(recovered_at, 90012.0, 0.15, False, "BINANCE_SPOT")
    hub.ingest_book(
        recovered_at, [(90011.0, 2.0)], [(90013.0, 2.0)],
        source="BINANCE_SPOT",
    )
    hub.last_kalshi_ts = recovered_at
    recovered = orchestrator.evaluate_once(now=recovered_at, record_to_db=False)

    assert recovered is not None
    assert recovered.engines.engine_a_evidence["status"] == "ACTIVE"
    assert recovered.engines.engine_c_regime["status"] == "ACTIVE"
    assert recovered.engines.engine_d_early["status"] in {"ACTIVE_WINDOW", "MONITORING"}
    assert recovered.engines.engine_e_final["status"] == "ACTIVE"
    assert recovered.engines.engine_a_evidence["input_health"]["fresh"] is True


def test_evidence_engine_excludes_pre_contract_trades_and_bars_from_flow():
    now = time.time()
    contract = _contract(
        open_timestamp=now - 2.0,
        close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    previous_contract_sell = (now - 5.0, 89900.0, 50.0, True)
    current_contract_buy = (now - 1.0, 90010.0, 0.2, False)
    old_bar = (int(now - 5.0), 80000.0, 100.0)
    current_bar = (int(now - 1.0), 90010.0, 0.1)

    engine = EvidenceEngine()
    with_previous_contract_data = engine.analyze(
        now, 90010.0, contract,
        [previous_contract_sell, current_contract_buy],
        [old_bar, current_bar], [], [],
    )
    current_contract_only = engine.analyze(
        now, 90010.0, contract,
        [current_contract_buy], [current_bar], [], [],
    )

    assert with_previous_contract_data.buying_pressure_pct == 100.0
    assert with_previous_contract_data.selling_pressure_pct == 0.0
    assert with_previous_contract_data.trade_delta_60s == pytest.approx(0.2)
    assert with_previous_contract_data.cvd_contract == pytest.approx(0.2)
    assert with_previous_contract_data.momentum_score == current_contract_only.momentum_score
    assert with_previous_contract_data.rsi_14 == current_contract_only.rsi_14
    assert with_previous_contract_data.volatility_annualized == current_contract_only.volatility_annualized


def test_regime_engine_receives_only_active_contract_bars(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator

    now = time.time()
    contract = _contract(
        open_timestamp=now - 10.0,
        close_timestamp=now + 800.0,
        seconds_remaining=800.0,
    )
    hub = MarketDataHub()
    hub.ingest_trade(now, 90010.0, 0.2, False, "BINANCE_SPOT")
    hub.ingest_book(now, [(90009.0, 2.0)], [(90011.0, 2.0)], source="BINANCE_SPOT")
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    hub.bars_1s.extend([
        (int(now - 20.0), 80000.0, 100.0),  # prior contract
        (int(now - 5.0), 90005.0, 0.1),    # active contract
    ])

    orchestrator = PredictorOrchestrator(
        hub, PredictionLedger()
    )
    captured = {}
    original = orchestrator.regime_engine.analyze

    def capture_bars(evidence, weakness, bars):
        captured["bars"] = list(bars)
        return original(evidence, weakness, bars)

    monkeypatch.setattr(orchestrator.regime_engine, "analyze", capture_bars)
    snapshot = orchestrator.evaluate_once(now=now, record_to_db=False)

    assert snapshot is not None
    assert captured["bars"]
    assert all(bar[0] >= contract.open_timestamp for bar in captured["bars"])


@pytest.mark.parametrize("book_failure", ["missing", "stale", "source_mismatch"])
def test_execution_refuses_missing_stale_or_misaligned_spot_book(tmp_path, book_failure):
    ledger = PredictionLedger()
    contract = _contract()
    hub = _fresh_execution_hub(contract)
    if book_failure == "missing":
        hub.bids, hub.asks = [], []
        hub.last_book_ts = 0.0
    elif book_failure == "stale":
        hub.last_book_ts = time.time() - settings.stale_feed_threshold_sec - 1.0
    else:
        hub.last_book_source = "OLD_SPOT_SOURCE"
    manager = ExecutionManager(hub, ledger)

    result = asyncio.run(manager.evaluate_and_execute(time.time(), contract, _decision()))

    assert result is None
    assert ledger.get_recent_trades() == []



def test_execution_rejects_delayed_snapshot_after_contract_rollover(tmp_path):
    ledger = PredictionLedger()
    old_contract = _contract(ticker="KXBTC15M-OLD")
    new_contract = _contract(
        ticker="KXBTC15M-NEW",
        open_timestamp=time.time() - 5.0,
        close_timestamp=time.time() + 895.0,
    )
    hub = _fresh_execution_hub(new_contract)
    manager = ExecutionManager(hub, ledger)

    result = asyncio.run(
        manager.evaluate_and_execute(time.time(), old_contract, _decision())
    )

    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_uses_latest_live_kalshi_ask_not_snapshot_quote(tmp_path):
    ledger = PredictionLedger()
    snapshot_contract = _contract(yes_ask_dollars=0.80)
    live_contract = _contract(
        yes_ask_dollars=0.20,
        open_timestamp=snapshot_contract.open_timestamp,
        close_timestamp=snapshot_contract.close_timestamp,
    )
    hub = _fresh_execution_hub(live_contract)
    manager = ExecutionManager(hub, ledger)

    result = asyncio.run(
        manager.evaluate_and_execute(time.time(), snapshot_contract, _decision())
    )

    assert result is not None
    assert result.entry_price == pytest.approx(0.20)
    assert result.quote_source == "KALSHI_LIVE_BOOK"


def test_database_reopen_preserves_predictions_trades_and_scoreboard(tmp_path):
    db_path = str(tmp_path / "restart-persistence.db")
    first = PredictionLedger()
    ticker = "KXBTC15M-RESTART-PERSISTENCE"
    first.record_prediction(_prediction_record(ticker, final=Signal.UP))
    first.record_trade(TradeRecord(
        mode="paper", contract_ticker=ticker,
        timestamp_iso="2026-11-18 00:00:00 UTC", side="YES",        contracts=2, entry_price=0.20, quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.02, fair_prob_at_entry=0.70, net_edge=0.49,
        status="OPEN", notes="Restart persistence verification",
    ))
    first.settle_contract(
        ticker, settlement_price=90020.0,
        official_result="UP", settlement_source="KALSHI_VERIFIED",
    )

    reopened = PredictionLedger()
    scoreboard = reopened.get_scoreboard()
    predictions = reopened.get_recent_predictions(limit=5)
    trades = reopened.get_recent_trades(limit=5)

    assert scoreboard.total_predictions == 1
    assert scoreboard.total_settled == 1
    assert scoreboard.final_wins == 1
    assert scoreboard.final_accuracy_pct == 100.0
    assert len(predictions) == 1
    assert predictions[0].contract_ticker == ticker
    assert predictions[0].settled is True
    assert len(trades) == 1
    assert trades[0].status == "SETTLED"
    assert trades[0].pnl_usd == pytest.approx(1.58)


def test_concurrent_prediction_writes_keep_one_official_lock(tmp_path):
    db_path = str(tmp_path / "concurrent-locks.db")
    ledger = PredictionLedger()
    record = _prediction_record("KXBTC15M-CONCURRENT-LOCK")

    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(lambda _index: ledger.record_prediction(record), range(24)))

    assert len(set(ids)) == 1
    assert len(ledger.get_recent_predictions(limit=10)) == 1
    assert ledger.get_scoreboard().total_predictions == 1


def test_concurrent_settlement_is_idempotent_and_does_not_double_pnl(tmp_path):
    db_path = str(tmp_path / "concurrent-settlement.db")
    ticker = "KXBTC15M-CONCURRENT-SETTLEMENT"
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record(ticker, final=Signal.UP))
    ledger.record_trade(TradeRecord(
        mode="paper", contract_ticker=ticker,
        timestamp_iso="2026-11-18 00:00:00 UTC", side="YES",
        contracts=1, entry_price=0.20, quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.01, fair_prob_at_entry=0.70, net_edge=0.49,
        status="OPEN", notes="Concurrent settlement verification",
    ))

    def settle(_index):
        return ledger.settle_contract(
            ticker, settlement_price=None, official_result="UP",
            settlement_source="KALSHI_VERIFIED",
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(settle, range(12)))

    reopened = PredictionLedger()
    assert reopened.get_scoreboard().total_settled == 1
    trades = reopened.get_recent_trades(limit=5)
    assert len(trades) == 1
    assert trades[0].status == "SETTLED"
    assert trades[0].pnl_usd == pytest.approx(0.79)


def test_postgres_schema_initialization_is_idempotent_and_preserves_records():
    ledger = PredictionLedger()
    ticker = "KXBTC15M-SCHEMA-IDEMPOTENT"
    ledger.record_prediction(_prediction_record(ticker))
    before = ledger.get_recent_predictions(limit=5)

    ledger._init_db()

    after = ledger.get_recent_predictions(limit=5)
    assert len(before) == len(after) == 1
    assert after[0].contract_ticker == ticker


def test_database_storage_is_supabase_postgres_only():
    ledger = PredictionLedger()
    health = ledger.get_storage_health()

    assert ledger.database_url
    assert not hasattr(ledger, "db_path")
    assert health["status"] == "HEALTHY"
    assert health["storage_type"] == "POSTGRESQL"
    assert health["provider"] == "SUPABASE"


def test_storage_health_reports_saved_postgres_record_counts():
    ledger = PredictionLedger()
    ledger.record_prediction(_prediction_record("KXBTC15M-STORAGE-HEALTH"))

    health = ledger.get_storage_health()

    assert health["status"] == "HEALTHY"
    assert health["storage_type"] == "POSTGRESQL"
    assert health["connection_check"] == "ok"
    assert health["integrity_check"] == "not_applicable"
    assert health["record_counts"]["predictions"] == 1
    assert health["record_counts"]["trades"] == 0


def test_storage_health_reports_unavailable_postgres_without_crashing():
    ledger = PredictionLedger()
    ledger.database_url = "postgresql://invalid.invalid:1/no_such_database?connect_timeout=1&sslmode=require"

    health = ledger.get_storage_health()

    assert health["status"] == "UNAVAILABLE"
    assert health["storage_type"] == "POSTGRESQL"
    assert health["integrity_check"] == "ERROR"
    assert health["connection_check"] == "failed"
    assert health["error_type"] == "OperationalError"


@pytest.mark.parametrize(
    ("storage_status", "integrity", "error_type"),
    [
        ("HEALTHY", "not_applicable", None),
        ("UNAVAILABLE", "ERROR", "OperationalError"),
    ],
)
def test_process_health_exposes_storage_diagnostics_without_failing_liveness(
    monkeypatch, storage_status, integrity, error_type
):
    import importlib

    main_module = importlib.import_module("app.main")
    storage = {
        "status": storage_status,
        "storage_type": "POSTGRESQL",
        "integrity_check": integrity,
    }
    if error_type:
        storage["error_type"] = error_type
    monkeypatch.setattr(main_module.ledger, "get_storage_health", lambda: storage)

    health = asyncio.run(main_module.get_health())

    assert health["status"] == "ok"
    assert health["storage_status"] == storage_status
    assert health["storage_integrity_check"] == integrity
    assert health["storage"] == storage


def test_feed_status_reports_worker_uptime_and_cached_brti_age():
    now = time.time()
    hub = MarketDataHub()
    hub._feed_runtime["BRTI Feed Worker"].update({
        "state": "RESTARTING",
        "restart_count": 2,
        "started_ts": now - 45.0,
        "last_error": "test disconnect",
    })
    hub.brti_status = "LIVE"
    hub.official_brti_price = 90123.45
    hub.last_brti_ts = now - 37.0
    hub.brti_detail = "BRTI stream reconnecting: test disconnect"

    statuses = {item.name: item for item in hub.get_feed_statuses()}
    worker = statuses["BRTI Feed Worker"]
    brti = statuses["CF Benchmarks BRTI"]

    assert worker.last_updated_iso is None
    assert worker.age_seconds is None
    assert "worker uptime=45." in worker.detail
    assert "cached official BRTI value=90123.45" in brti.detail
    assert "last tick age=37." in brti.detail
    assert "(not a new tick)" in brti.detail
