from app.database import PredictionLedger


def test_ledger_uses_supabase_postgres_without_local_database_path():
    ledger = PredictionLedger()

    assert ledger.database_url.startswith(("postgres://", "postgresql://"))
    assert not hasattr(ledger, "db_path")
    health = ledger.get_storage_health()
    assert health["status"] == "HEALTHY"
    assert health["storage_type"] == "POSTGRESQL"
    assert health["provider"] == "SUPABASE"
    assert health["record_counts"] == {
        "predictions": 0,
        "prediction_observations": 0,
        "forecast_locks": 0,
        "trades": 0,
        "multi_exchange_reference": 0,
    }
