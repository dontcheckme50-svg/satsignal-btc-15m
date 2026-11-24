from types import SimpleNamespace

from fastapi.testclient import TestClient

import app.main as main_module
from app.models import ForecastReport


def test_server_time_endpoint_returns_millisecond_epoch():
    response = TestClient(main_module.app).get("/api/time")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload["server_time_epoch_ms"], int)
    assert payload["server_time_epoch_ms"] > 0


def test_forecast_endpoint_exposes_independent_engine_f_state(monkeypatch):
    snapshot = SimpleNamespace(
        contract=SimpleNamespace(ticker="KXBTC15M-TEST"),
        forecast=ForecastReport(signal="UP", confidence=72.0, up_probability=0.72, down_probability=0.28),
        engines=SimpleNamespace(engine_f_forecast={"status": "ACTIVE", "independent": True}),
    )
    monkeypatch.setattr(main_module.orchestrator, "latest_snapshot", snapshot)

    response = TestClient(main_module.app).get("/api/forecast")

    assert response.status_code == 200
    body = response.json()
    assert body["contract_ticker"] == "KXBTC15M-TEST"
    assert body["forecast"]["signal"] == "UP"
    assert body["engine_status"]["independent"] is True


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


def test_manual_forecast_lock_endpoint_returns_conflict_when_rejected(monkeypatch):
    def reject_lock():
        raise ValueError("No actionable forecast is available")

    monkeypatch.setattr(main_module.orchestrator, "lock_forecast", reject_lock)

    response = TestClient(main_module.app).post("/api/forecast/lock")

    assert response.status_code == 409
    assert response.json()["detail"] == "No actionable forecast is available"
