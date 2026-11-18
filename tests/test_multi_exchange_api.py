from fastapi.testclient import TestClient
from app.main import app


def test_multi_exchange_shadow_endpoint():
    client = TestClient(app)
    response = client.get("/api/multi-exchange")
    assert response.status_code == 200
    payload = response.json()
    assert "reference_price" in payload
    assert "venues" in payload
    assert "reference_vs_primary_bps" in payload
    assert "reference_vs_brti_bps" in payload
    assert "reference_vs_strike_bps" in payload
    assert payload["venue_count_required"] == 2
    assert "not CF Benchmarks BRTI" in payload["method"]
