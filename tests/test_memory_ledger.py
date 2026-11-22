import threading
from dataclasses import replace

from app.config import settings
import app.database as database
from app.database import PredictionLedger


def test_memory_ledger_keeps_schema_and_records_across_operations(monkeypatch):
    monkeypatch.setattr(database, "settings", replace(settings, database_url=None))
    ledger = PredictionLedger(":memory:")

    row_id = ledger.record_prediction_observation(
        "KXBTC15M-TEST", 1234.0, 300.0, {"signal": "UP"}
    )

    rows = ledger.get_recent_prediction_observations(contract_ticker="KXBTC15M-TEST")
    health = ledger.get_storage_health()

    assert row_id == 1
    assert len(rows) == 1
    assert rows[0]["payload"] == {"signal": "UP"}
    assert health["status"] == "HEALTHY"
    assert health["storage_type"] == "MEMORY"
    assert health["record_counts"]["prediction_observations"] == 1


def test_memory_ledger_serializes_concurrent_observation_writes(monkeypatch):
    monkeypatch.setattr(database, "settings", replace(settings, database_url=None))
    ledger = PredictionLedger(":memory:")
    failures = []

    def write(index):
        try:
            ledger.record_prediction_observation(
                "KXBTC15M-CONCURRENT", float(index), 100.0, {"i": index}
            )
        except Exception as exc:  # pragma: no cover - assertion reports details
            failures.append(exc)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(40)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    rows = ledger.get_recent_prediction_observations(limit=100, contract_ticker="KXBTC15M-CONCURRENT")
    assert not failures
    assert len(rows) == 40
