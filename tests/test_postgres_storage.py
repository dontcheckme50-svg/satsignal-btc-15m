import os
import uuid

import pytest

from app.database import PredictionLedger
from app.models import MarketRegime, PredictionRecord, Signal, TradeRecord


pytestmark = pytest.mark.skipif(
    not os.getenv("DATABASE_URL"),
    reason="PostgreSQL integration runs in the dedicated CI service job",
)


def test_postgres_ledger_lock_settlement_and_scoreboard():
    ticker = "KXBTC15M-PG-" + uuid.uuid4().hex[:12]
    ledger = PredictionLedger(":memory:")
    prediction = PredictionRecord(
        contract_ticker=ticker,
        timestamp_iso="2026-11-20 12:00:00 UTC",
        epoch_ts=1795176000.0,
        strike_price=90000.0,
        btc_price_at_pred=90010.0,
        seconds_remaining=300.0,
        main_signal=Signal.UP,
        early_signal=Signal.UP,
        final_signal=Signal.UP,
        confidence=75.0,
        up_prob=0.75,
        down_prob=0.25,
        regime=MarketRegime.STABLE,
        brti_cohort="ALIGNED",
        brti_side_at_lock="UP",
        spot_side_at_lock="UP",
    )
    first_id = ledger.record_prediction(prediction)
    assert first_id > 0
    assert ledger.record_prediction(prediction) == first_id
    assert ledger.has_official_prediction_lock(ticker)

    ledger.record_trade(TradeRecord(
        mode="paper",
        contract_ticker=ticker,
        timestamp_iso="2026-11-20 12:00:00 UTC",
        side="YES",
        contracts=1,
        entry_price=0.20,
        quote_source="KALSHI_LIVE_BOOK",
        fee_paid=0.01,
        fair_prob_at_entry=0.75,
        net_edge=0.54,
        status="OPEN",
        notes="PostgreSQL integration test",
    ))
    assert ledger.settle_contract(
        ticker, settlement_price=90020.0,
        official_result="UP", settlement_source="KALSHI_VERIFIED",
    ) == 1
    assert ledger.settle_contract(
        ticker, settlement_price=90020.0,
        official_result="UP", settlement_source="KALSHI_VERIFIED",
    ) == 0

    scoreboard = ledger.get_scoreboard()
    assert scoreboard.total_predictions >= 1
    assert scoreboard.total_settled >= 1
    assert scoreboard.final_wins >= 1
    assert any(item.contract_ticker == ticker for item in ledger.get_recent_predictions(100))
    assert any(item.contract_ticker == ticker for item in ledger.get_recent_trades(100))
    assert ledger.get_storage_health()["storage_type"] == "POSTGRESQL"
