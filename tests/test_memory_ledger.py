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

def test_memory_history_retention_bounds_only_diagnostic_tables(monkeypatch):
    monkeypatch.setattr(database, "settings", replace(settings, database_url=None))
    monkeypatch.setattr(PredictionLedger, "MEMORY_OBSERVATION_RETENTION", 2)
    monkeypatch.setattr(PredictionLedger, "MEMORY_REFERENCE_RETENTION", 2)
    monkeypatch.setattr(PredictionLedger, "MEMORY_PRUNE_EVERY_WRITES", 1)
    ledger = PredictionLedger(":memory:")

    for index in range(3):
        ledger.record_prediction_observation(
            "KXBTC15M-RETENTION", float(index), 100.0, {"i": index}
        )
        ledger.record_multi_exchange_reference(
            float(index), "KXBTC15M-RETENTION", 100.0, 101.0, 102.0,
            {"status": "LIVE", "reference_price": 101.0},
        )

    observations = ledger.get_recent_prediction_observations(
        limit=10, contract_ticker="KXBTC15M-RETENTION"
    )
    references = ledger.get_multi_exchange_reference_history(limit=10)
    assert len(observations) == 2
    assert len(references) == 2

