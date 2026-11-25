import time

from app.feeds import MarketDataHub
from app.multi_exchange import MultiExchangeReference


def test_brti_live_status_uses_timestamp_age_not_price_equality():
    hub = MarketDataHub()
    now = time.time()
    hub.brti_status = "LIVE"
    hub.official_brti_price = 90000.0
    hub.last_brti_ts = now
    # Coincidental equality between a source timestamp and a price is valid.
    hub._last_brti_source_ts = 90000.0
    hub._last_brti_value = 90000.0

    statuses = {item.name: item.status for item in hub.get_feed_statuses()}

    assert statuses["CF Benchmarks BRTI"] == "LIVE"


def test_coinbase_shadow_feed_requires_confirmation_of_both_requested_channels():
    confirmed = {
        "type": "subscriptions",
        "channels": [
            {"name": "level2", "product_ids": ["BTC-USD"]},
            {"name": "matches", "product_ids": ["BTC-USD"]},
        ],
    }
    incomplete = {
        "type": "subscriptions",
        "channels": [{"name": "level2", "product_ids": ["BTC-USD"]}],
    }
    wrong_product = {
        "type": "subscriptions",
        "channels": [
            {"name": "level2", "product_ids": ["ETH-USD"]},
            {"name": "matches", "product_ids": ["ETH-USD"]},
        ],
    }

    assert MultiExchangeReference._coinbase_subscription_confirmed(confirmed)
    assert not MultiExchangeReference._coinbase_subscription_confirmed(incomplete)
    assert not MultiExchangeReference._coinbase_subscription_confirmed(wrong_product)
    assert not MultiExchangeReference._coinbase_subscription_confirmed({"type": "error"})
