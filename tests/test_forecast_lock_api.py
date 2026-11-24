from types import SimpleNamespace

from fastapi.testclient import TestClient

import app.main as main_module
from app.models import ForecastReport


def test_manual_forecast_lock_endpoint_returns_locked_forecast(monkeypatch):
    lock = {
        "contract_ticker": "KXBTC15M-TEST",
        "signal": "UP",
        "confidence": 72.0,
    }
    monkeypatch.setattr(main_module.orchestrator, "lock_forecast", lambda: lock)
    monkeypatch.setattr(
        main_module.orchestrator,
        "latest_snapshot",
        SimpleNamespace(forecast=ForecastReport(signal="UP", confidence=72.0)),
    )

    response = TestClient(main_module.app).post("/api/forecast/lock")

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["lock"] == lock
    assert body["forecast"]["signal"] == "UP"
    assert body["forecast"]["confidence"] == 72.0


def test_manual_forecast_lock_endpoint_returns_conflict_when_rejected(monkeypatch):
    def reject_lock():
        raise ValueError("No actionable forecast is available")

    monkeypatch.setattr(main_module.orchestrator, "lock_forecast", reject_lock)

    response = TestClient(main_module.app).post("/api/forecast/lock")

    assert response.status_code == 409
    assert response.json()["detail"] == "No actionable forecast is available"
