import time
import pytest
from pathlib import Path

from app.config import settings
from app.database import PredictionLedger
from app.execution import ExecutionManager
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
    hub = MarketDataHub()
    manager = ExecutionManager(hub, ledger)
    no_quote = _contract(yes_ask_dollars=None)
    import asyncio
    result = asyncio.run(manager.evaluate_and_execute(time.time(), no_quote, _decision()))
    assert result is None
    assert ledger.get_recent_trades() == []


def test_execution_places_at_most_one_paper_trade_per_contract(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "ledger.db"))
    hub = MarketDataHub()
    manager = ExecutionManager(hub, ledger)
    contract = _contract()
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
    assert health["health"] == "DEGRADED"
