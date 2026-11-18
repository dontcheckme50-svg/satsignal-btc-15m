import asyncio
import time
from datetime import datetime, timezone
import pytest
from pathlib import Path

from app.config import settings
from app.database import PredictionLedger
from app.execution import ExecutionManager
from app.engines import EvidenceEngine
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
    hub.last_kalshi_ts = now
    hub.kalshi_contract = contract
    return hub


def test_paper_mode_is_forced():
    assert settings.trading_mode == "paper"


def test_database_round_trip_and_scoreboard(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
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
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
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
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
    no_quote = _contract(yes_ask_dollars=None)
    hub = _fresh_execution_hub(no_quote)
    manager = ExecutionManager(hub, ledger)
    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), no_quote, _decision()))
    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_places_at_most_one_paper_trade_per_contract(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
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
    assert "SATSIGNAL BTC 15M" in client.get("/").text
    assert client.get("/docs").status_code == 200
    health = client.get("/api/health")
    assert health.status_code == 200
    assert health.json()["trading_mode"] == "paper"


def test_synthetic_contracts_are_not_scored(tmp_path):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
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
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
    with ledger._connection() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
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
    ledger = PredictionLedger(str(tmp_path / "scoreboard.db"))
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

    ledger = PredictionLedger(":memory:")
    hub = MarketDataHub()
    orchestrator = PredictorOrchestrator(hub, ledger)
    now = 1794873675.25
    contract = orchestrator._resolve_active_contract(now, 90000.0)
    assert contract.close_timestamp - contract.open_timestamp == 900.0
    assert contract.seconds_remaining == contract.close_timestamp - now
    assert 0.0 < contract.seconds_remaining <= 900.0


def test_dashboard_countdown_uses_absolute_contract_timestamps():
    from app.main import DASHBOARD_HTML

    assert 'contractCloseEpochMs = Number(d.contract.close_timestamp) * 1000' in DASHBOARD_HTML
    assert 'setInterval(tickTimers, 250)' in DASHBOARD_HTML
    assert 'Date.now() + serverClockOffsetMs' in DASHBOARD_HTML


@pytest.mark.asyncio
async def test_orchestrator_survives_prediction_cycle_exception(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger(str(tmp_path / "worker-health.db"))
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



def test_feed_status_marks_future_timestamps_invalid():
    hub = MarketDataHub()
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
    ledger = PredictionLedger(str(tmp_path / "synthetic-unsettled.db"))
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



def test_orchestrator_filters_future_bars_before_regime_engine(monkeypatch, tmp_path):
    from app.orchestrator import PredictorOrchestrator

    hub = MarketDataHub()
    now = time.time()
    hub.ingest_trade(now, 90000.0, 0.2, False, "TEST")
    hub.bars_1s.extend([
        (int(now - 5), 89999.0, 0.1),
        (int(now + 100), 999999.0, 100.0),
    ])
    orchestrator = PredictorOrchestrator(hub, PredictionLedger(str(tmp_path / "regime-bars.db")))
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
    orchestrator = PredictorOrchestrator(hub, PredictionLedger(":memory:"))

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
    ledger = PredictionLedger(str(tmp_path / "settlement-queue.db"))
    ledger.record_prediction(_prediction_record("SYNTH-BTC15M-A", ts=1794873600.0))
    ledger.record_prediction(_prediction_record("KXBTC15M-NEW", ts=1794873700.0))
    ledger.record_prediction(_prediction_record("KXBTC15M-OLD", ts=1794873500.0))

    assert ledger.get_unsettled_contract_tickers() == ["KXBTC15M-OLD", "KXBTC15M-NEW"]


@pytest.mark.asyncio
async def test_settlement_reconciliation_is_bounded_to_five_and_three_concurrent(tmp_path, monkeypatch):
    from app.orchestrator import PredictorOrchestrator

    ledger = PredictionLedger(str(tmp_path / "settlement-batch.db"))
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



def test_exchange_timestamp_parser_handles_milliseconds_and_iso():
    hub = MarketDataHub()
    epoch = 1_800_000_000.0

    assert hub._parse_exchange_timestamp(epoch * 1000) == epoch
    assert hub._parse_exchange_timestamp("2027-01-15T08:00:00Z") == datetime(
        2027, 1, 15, 8, 0, 0, tzinfo=timezone.utc
    ).timestamp()
    assert hub._parse_exchange_timestamp("bad-time", fallback=123.0) == 123.0



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
    ledger = PredictionLedger(str(tmp_path / "execution-expiry.db"))
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
    ledger = PredictionLedger(str(tmp_path / "execution-probability.db"))
    contract = _contract()
    manager = ExecutionManager(_fresh_execution_hub(contract), ledger)
    bad_probability = _decision(up=float("nan"), down=0.05)

    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), _contract(), bad_probability))

    assert result is None
    assert ledger.get_recent_trades() == []



def test_execution_does_not_reenter_contract_after_paper_trade_settlement(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "one-entry-per-contract.db"))
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
