import concurrent.futures
import time
from types import SimpleNamespace

import pytest

from app.config import settings
from app.database import PredictionLedger
from app.execution import ExecutionManager
from app.feeds import MarketDataHub
from app.models import (
    ContractInfo,
    FinalDecisionReport,
    MarketRegime,
    PredictionRecord,
    Signal,
    TradeRecord,
)


def _contract(now: float, ticker: str = "KXBTC15M-EXECUTION"):
    return ContractInfo(
        ticker=ticker,
        strike_price=90000.0,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 20.0,
        close_timestamp=now + 600.0,
        seconds_remaining=600.0,
        checkpoint_seconds_remaining=60.0,
        is_official_kalshi=True,
        yes_bid_dollars=0.20,
        yes_ask_dollars=0.21,
        no_bid_dollars=0.78,
        no_ask_dollars=0.80,
    )


def _ready_hub(contract: ContractInfo, now: float):
    hub = MarketDataHub()
    hub.kalshi_contract = contract
    hub.live_price = 90010.0
    hub.last_spot_ts = now
    hub.last_kalshi_ts = now
    hub.last_book_ts = now
    hub.active_spot_source = "TEST_LIVE"
    hub.last_book_source = "TEST_LIVE"
    hub.bids = [(90009.0, 2.0)]
    hub.asks = [(90011.0, 2.0)]
    return hub


def _decision(signal: Signal):
    return FinalDecisionReport(
        signal=signal,
        confidence=80.0 if signal != Signal.WAIT else 0.0,
        up_probability=0.90 if signal == Signal.UP else 0.10 if signal == Signal.DOWN else 0.50,
        down_probability=0.10 if signal == Signal.UP else 0.90 if signal == Signal.DOWN else 0.50,
        agreement_status="TEST",
        rationale="Execution lifecycle test",
    )


def _record_lock(ticker: str, signal: Signal):
    return PredictionRecord(
        contract_ticker=ticker,
        timestamp_iso="2027-01-01 00:00:00 UTC",
        epoch_ts=1800000000.0,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=500.0,
        main_signal=signal,
        early_signal=signal,
        final_signal=signal,
        confidence=80.0 if signal != Signal.WAIT else 0.0,
        up_prob=0.90 if signal == Signal.UP else 0.10 if signal == Signal.DOWN else 0.50,
        down_prob=0.10 if signal == Signal.UP else 0.90 if signal == Signal.DOWN else 0.50,
        regime=MarketRegime.STABLE,
    )


@pytest.mark.asyncio
async def test_transient_direction_without_official_lock_never_opens_paper_trade(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr("app.execution.time.time", lambda: now)
    ledger = PredictionLedger(str(tmp_path / "transient-no-entry.db"))
    contract = _contract(now)
    manager = ExecutionManager(_ready_hub(contract, now), ledger)

    result = await manager.evaluate_and_execute(now, contract, _decision(Signal.UP))

    assert result is None
    assert ledger.get_recent_trades() == []


@pytest.mark.asyncio
async def test_matching_official_directional_lock_creates_only_one_paper_trade(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr("app.execution.time.time", lambda: now)
    ledger = PredictionLedger(str(tmp_path / "one-paper-entry.db"))
    contract = _contract(now)
    ledger.record_prediction(_record_lock(contract.ticker, Signal.UP))
    manager = ExecutionManager(_ready_hub(contract, now), ledger)

    first = await manager.evaluate_and_execute(now, contract, _decision(Signal.UP))
    second = await manager.evaluate_and_execute(now + 1.0, contract, _decision(Signal.UP))

    trades = ledger.get_recent_trades()
    assert first is not None
    assert first.mode == "paper"
    assert first.contract_ticker == contract.ticker
    assert second is None
    assert len(trades) == 1
    assert trades[0].side == "YES"


@pytest.mark.asyncio
async def test_wait_lock_and_opposite_signal_never_execute(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr("app.execution.time.time", lambda: now)
    ledger = PredictionLedger(str(tmp_path / "wait-and-reversal.db"))
    contract = _contract(now)
    ledger.record_prediction(_record_lock(contract.ticker, Signal.WAIT))
    manager = ExecutionManager(_ready_hub(contract, now), ledger)

    wait_result = await manager.evaluate_and_execute(now, contract, _decision(Signal.WAIT))
    opposite_result = await manager.evaluate_and_execute(now, contract, _decision(Signal.DOWN))

    assert wait_result is None
    assert opposite_result is None
    assert ledger.get_recent_trades() == []


@pytest.mark.asyncio
async def test_live_mode_is_fail_closed_and_never_creates_trade(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr("app.execution.time.time", lambda: now)
    monkeypatch.setattr("app.execution.settings", SimpleNamespace(trading_mode="live"))
    ledger = PredictionLedger(str(tmp_path / "live-mode-blocked.db"))
    contract = _contract(now)
    ledger.record_prediction(_record_lock(contract.ticker, Signal.UP))
    manager = ExecutionManager(_ready_hub(contract, now), ledger)

    result = await manager.evaluate_and_execute(now, contract, _decision(Signal.UP))

    assert result is None
    assert ledger.get_recent_trades() == []


def test_atomic_trade_insert_allows_only_one_concurrent_contract_entry(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "atomic-trade-entry.db"))
    trade = TradeRecord(
        mode="paper",
        contract_ticker="KXBTC15M-ATOMIC",
        timestamp_iso="2027-01-01 00:00:00 UTC",
        side="YES",
        contracts=1,
        entry_price=0.21,
        quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.02,
        fair_prob_at_entry=0.90,
        net_edge=0.67,
        status="OPEN",
        notes="Atomic insert concurrency test",
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _i: ledger.record_trade_once_per_contract(trade), range(8)))

    assert sum(result is not None for result in results) == 1
    assert len(ledger.get_recent_trades(limit=20)) == 1
