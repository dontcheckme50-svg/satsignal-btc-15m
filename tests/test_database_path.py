from pathlib import Path

from app.database import PredictionLedger


def test_ledger_creates_missing_database_parent_directory(tmp_path):
    database_path = tmp_path / "nested" / "state" / "satsignal.db"

    ledger = PredictionLedger(str(database_path))

    assert database_path.is_file()
    health = ledger.get_storage_health()
    assert health["status"] == "HEALTHY"
    assert health["record_counts"] == {
        "predictions": 0,
        "prediction_observations": 0,
        "trades": 0,
        "multi_exchange_reference": 0,
    }


def test_ledger_does_not_create_parent_for_memory_database(tmp_path, monkeypatch):
    # In-memory SQLite remains supported and must not create a filesystem path.
    monkeypatch.chdir(tmp_path)
    ledger = PredictionLedger(":memory:")
    assert ledger.get_storage_health()["storage_type"] == "MEMORY"
    assert list(Path(tmp_path).iterdir()) == []
