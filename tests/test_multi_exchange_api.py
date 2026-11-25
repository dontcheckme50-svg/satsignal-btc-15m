from fastapi.testclient import TestClient
from app.main import app


def test_multi_exchange_shadow_endpoint():
    client = TestClient(app)
    response = client.get("/api/multi-exchange")
    assert response.status_code == 200
    payload = response.json()
    assert "reference_price" in payload
    assert payload["benchmark_type"] == "SYNTHETIC_MULTI_EXCHANGE"
    assert "quality" in payload and "eligible" in payload["quality"]
    assert "venues" in payload
    assert all("status" in venue and "connected" in venue and "age_seconds" in venue for venue in payload["venues"])
    assert "reference_vs_primary_bps" in payload
    assert "reference_vs_brti_bps" in payload
    assert "reference_vs_strike_bps" in payload
    assert payload["venue_count_required"] == 2
    assert "trade_flow" in payload
    assert "15" in payload["trade_flow"]["windows"]
    assert "60" in payload["trade_flow"]["windows"]
    assert "180" in payload["trade_flow"]["windows"]
    assert "not CF Benchmarks BRTI" in payload["method"]



def test_history_api_limits_are_bounded():
    client = TestClient(app)

    assert client.get("/api/multi-exchange/history?limit=1001").status_code == 422
    assert client.get("/api/history?limit=501").status_code == 422
    assert client.get("/api/trades?limit=201").status_code == 422



def test_dashboard_exposes_individual_venue_health():
    from app.main import DASHBOARD_HTML

    assert 'id="mx-venue-health"' in DASHBOARD_HTML
    assert 'v.status' in DASHBOARD_HTML
    assert 'v.age_seconds' in DASHBOARD_HTML
