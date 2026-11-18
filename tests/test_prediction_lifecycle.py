import time

from app.database import PredictionLedger
from app.feeds import MarketDataHub
from app.models import ContractInfo
from app.orchestrator import PredictorOrchestrator


def _official_contract(ticker: str, now: float, remaining: float = 800.0, strike: float = 90000.0):
    return ContractInfo(
        ticker=ticker,
        strike_price=strike,
        strike_source="KALSHI_OFFICIAL",
        open_timestamp=now - 10.0,
        close_timestamp=now + remaining,
        seconds_remaining=remaining,
        checkpoint_seconds_remaining=remaining % 180 or 180.0,
        is_official_kalshi=True,
        yes_bid_dollars=0.20,
        yes_ask_dollars=0.21,
        no_bid_dollars=0.79,
        no_ask_dollars=0.80,
    )


def _ready_hub(contract: ContractInfo, now: float, price: float = 90010.0):
    hub = MarketDataHub()
    hub.live_price = price
    hub.last_spot_ts = now
    hub.active_spot_source = "TEST_LIVE"
    hub.kalshi_contract = contract
    hub.last_kalshi_ts = now
    return hub


def test_official_contract_rollover_resets_prediction_recording_throttle(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "rollover-recording.db"))
    now = 1_800_000_000.0
    first = _official_contract("KXBTC15M-FIRST", now)
    hub = _ready_hub(first, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    first_snapshot = orchestrator.evaluate_once(now=now)
    assert first_snapshot.contract.ticker == first.ticker
    assert len(ledger.get_recent_predictions()) == 1

    # A new contract arrives only one second later in this deterministic test.
    # The per-contract transition must reset the previous contract's throttle.
    next_now = now + 1.0
    second = _official_contract("KXBTC15M-SECOND", next_now, strike=90100.0)
    hub.kalshi_contract = second
    hub.last_kalshi_ts = next_now
    hub.live_price = 90110.0
    hub.last_spot_ts = next_now

    second_snapshot = orchestrator.evaluate_once(now=next_now)

    records = ledger.get_recent_predictions(limit=10)
    assert second_snapshot.contract.ticker == second.ticker
    assert {record.contract_ticker for record in records} == {first.ticker, second.ticker}
    assert len(records) == 2
    assert orchestrator.last_logged_ts == next_now


def test_prediction_recording_respects_final_lockout_window(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "final-lockout-recording.db"))
    now = 1_800_000_000.0
    contract = _official_contract("KXBTC15M-LOCKOUT", now, remaining=64.0)
    hub = _ready_hub(contract, now)
    orchestrator = PredictorOrchestrator(hub, ledger)

    snapshot = orchestrator.evaluate_once(now=now)

    assert snapshot.contract.seconds_remaining == 64.0
    assert ledger.get_recent_predictions() == []
    assert orchestrator.last_logged_ts == 0.0


def test_synthetic_contract_never_creates_official_prediction_record(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "synthetic-recording.db"))
    now = 1_800_000_000.0
    hub = MarketDataHub()
    hub.live_price = 90010.0
    hub.last_spot_ts = now
    hub.active_spot_source = "TEST_LIVE"
    orchestrator = PredictorOrchestrator(hub, ledger)

    snapshot = orchestrator.evaluate_once(now=now)

    assert snapshot.contract.is_official_kalshi is False
    assert snapshot.contract.strike_source == "SYNTHETIC_OPEN_PROXY"
    assert ledger.get_recent_predictions() == []
