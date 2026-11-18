from app.database import PredictionLedger


def test_multi_exchange_history_preserves_per_venue_trade_flow(tmp_path):
    ledger = PredictionLedger(str(tmp_path / "shadow-flow.sqlite3"))
    snapshot = {
        "reference_price": 100.0,
        "status": "LIVE",
        "usd_venue_count_live": 2,
        "included_venues": ["COINBASE", "KRAKEN"],
        "outlier_venues": [],
        "primary_spot_age_seconds": 0.2,
        "official_brti_age_seconds": None,
        "consolidated_bid_depth_usd_within_25bps": 10_000.0,
        "consolidated_ask_depth_usd_within_25bps": 9_000.0,
        "consolidated_imbalance": 0.0526,
        "venues": [
            {
                "venue": "COINBASE",
                "quote": "USD",
                "status": "LIVE",
                "mid": 100.0,
                "spread_bps": 1.0,
                "imbalance": 0.1,
                "bid_depth_25bps": 5_000.0,
                "ask_depth_25bps": 4_000.0,
                "age_seconds": 0.2,
                "trade_flow": {
                    "latest_trade_age_seconds": 0.1,
                    "windows": {"60": {"buy_volume_btc": 0.4, "sell_volume_btc": 0.1, "delta_btc": 0.3}},
                },
            }
        ],
    }

    ledger.record_multi_exchange_reference(
        epoch_ts=1_000.0,
        contract_ticker="TEST-CONTRACT",
        strike_price=99.0,
        primary_spot_price=100.0,
        official_brti_price=None,
        snapshot=snapshot,
    )

    history = ledger.get_multi_exchange_reference_history(limit=1)
    saved_flow = history[0]["venues"][0]["trade_flow"]

    assert saved_flow["windows"]["60"]["buy_volume_btc"] == 0.4
    assert saved_flow["windows"]["60"]["sell_volume_btc"] == 0.1
    assert saved_flow["windows"]["60"]["delta_btc"] == 0.3
