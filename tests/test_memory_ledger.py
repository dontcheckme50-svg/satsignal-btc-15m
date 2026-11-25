
import threading
from dataclasses import replace

import pytest

from app.config import settings
import app.database as database
from app.database import PredictionLedger


def test_ledger_fails_closed_without_supabase_database_url(monkeypatch):
    monkeypatch.setattr(database, "settings", replace(settings, database_url=None))

    with pytest.raises(RuntimeError, match="DATABASE_URL is required"):
        PredictionLedger()


def test_supabase_ledger_persists_observation_and_reports_health():
    ledger = PredictionLedger()
    row_id = ledger.record_prediction_observation(
        "KXBTC15M-POSTGRES-TEST", 1234.0, 300.0, {"signal": "UP"}
    )

    rows = ledger.get_recent_prediction_observations(
        contract_ticker="KXBTC15M-POSTGRES-TEST"
    )
    health = ledger.get_storage_health()

    assert row_id > 0
    assert len(rows) == 1
    assert rows[0]["payload"] == {"signal": "UP"}
    assert health["status"] == "HEALTHY"
    assert health["storage_type"] == "POSTGRESQL"
    assert health["provider"] == "SUPABASE"


def test_supabase_ledger_serializes_concurrent_observation_writes():
    ledger = PredictionLedger()
    failures = []

    def write(index):
        try:
            ledger.record_prediction_observation(
                "KXBTC15M-CONCURRENT", float(index), 100.0, {"i": index}
            )
        except Exception as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(24)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    rows = ledger.get_recent_prediction_observations(
        limit=100, contract_ticker="KXBTC15M-CONCURRENT"
    )
    assert not failures
    assert len(rows) == 24


def test_supabase_history_is_not_pruned_by_memory_retention_policy():
    ledger = PredictionLedger()
    for index in range(3):
        ledger.record_prediction_observation(
            "KXBTC15M-HISTORY", float(index), 100.0, {"i": index}
        )
    rows = ledger.get_recent_prediction_observations(
        limit=10, contract_ticker="KXBTC15M-HISTORY"
    )
    assert len(rows) == 3

def test_diagnostic_history_retention_bounds_only_diagnostic_tables(monkeypatch):
    monkeypatch.setattr(PredictionLedger, "MEMORY_OBSERVATION_RETENTION", 2)
    monkeypatch.setattr(PredictionLedger, "MEMORY_REFERENCE_RETENTION", 2)
    monkeypatch.setattr(PredictionLedger, "MEMORY_PRUNE_EVERY_WRITES", 1)
    ledger = PredictionLedger()
    ticker = "KXBTC15M-RETENTION-TEST"

    for index in range(3):
        ledger.record_prediction_observation(
            ticker, float(index), 100.0, {"i": index}
        )
        ledger.record_multi_exchange_reference(
            float(index), ticker, 100.0, 101.0, 102.0,
            {"status": "LIVE", "reference_price": 101.0},
        )

    observations = ledger.get_recent_prediction_observations(
        limit=10, contract_ticker=ticker
    )
    with ledger._connection() as conn:
        reference_count = conn.execute(
            "SELECT COUNT(*) FROM multi_exchange_reference WHERE contract_ticker = ?",
            (ticker,),
        ).fetchone()[0]

    assert len(observations) == 2
    assert int(reference_count) == 2

