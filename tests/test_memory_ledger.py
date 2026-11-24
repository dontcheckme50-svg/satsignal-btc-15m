
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
