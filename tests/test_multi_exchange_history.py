import tempfile
from pathlib import Path

from app.database import PredictionLedger


def test_multi_exchange_reference_history_persists_and_calculates_basis(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "shadow.db"))
    snapshot = {
        "status": "LIVE",
        "reference_price": 90000.0,
        "usd_venue_count_live": 3,
        "included_venues": ["COINBASE", "KRAKEN", "BITSTAMP"],
        "outlier_venues": [],
        "venues": [
            {"venue": "COINBASE", "quote": "USD", "status": "LIVE", "mid": 90000.0, "spread_bps": 1.0, "imbalance": 0.1, "bid_depth_25bps": 1000.0, "ask_depth_25bps": 800.0, "age_seconds": 0.2}
        ],
        "consolidated_bid_depth_usd_within_25bps": 1000.0,
        "consolidated_ask_depth_usd_within_25bps": 800.0,
        "consolidated_imbalance": 0.1111,
        "primary_spot_age_seconds": 0.3,
        "official_brti_age_seconds": 0.4,
    }
    ledger.record_multi_exchange_reference(
        epoch_ts=1000.0,
        contract_ticker="KXBTC15M-TEST",
        strike_price=90010.0,
        primary_spot_price=89990.0,
        official_brti_price=89980.0,
        snapshot=snapshot,
    )

    history = ledger.get_multi_exchange_reference_history(limit=10)

    assert len(history) == 1
    row = history[0]
    assert row["reference_status"] == "LIVE"
    assert row["reference_price"] == 90000.0
    assert row["primary_spot_age_seconds"] == 0.3
    assert row["official_brti_age_seconds"] == 0.4
    assert row["ref_vs_primary_bps"] == 1.1112
    assert row["ref_vs_brti_bps"] == 2.2227
    assert row["reference_vs_strike_bps"] == -1.111
    assert row["included_venues"] == ["COINBASE", "KRAKEN", "BITSTAMP"]
    assert row["venues"][0]["venue"] == "COINBASE"

def test_multi_exchange_history_api_returns_observations():
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app)
    response = client.get("/api/multi-exchange/history?limit=10")
    assert response.status_code == 200
    assert "observations" in response.json()

def test_stale_primary_and_brtI_are_not_used_in_reference_comparisons():
    import time
    from app.feeds import MarketDataHub

    hub = MarketDataHub()
    hub.live_price = 90000.0
    hub.active_spot_source = "BINANCE_SPOT"
    hub.last_spot_ts = time.time() - 12.0
    hub.bids = [(89990.0, 1.0)]
    hub.asks = [(90010.0, 1.0)]
    hub.official_brti_price = 90005.0
    hub.last_brti_ts = time.time() - 12.0
    hub.brti_status = "LIVE"

    snapshot = hub.get_multi_exchange_snapshot()

    assert snapshot["primary_spot_price"] is None
    assert snapshot["official_brti_price"] is None
    assert snapshot["primary_spot_age_seconds"] > 5.0
    assert snapshot["official_brti_age_seconds"] > 5.0
    assert snapshot["reference_vs_primary_bps"] is None
    assert snapshot["reference_vs_brti_bps"] is None
