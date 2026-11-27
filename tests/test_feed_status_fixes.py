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


def test_brti_exception_diagnostic_includes_empty_timeout_exception_type():
    detail = MarketDataHub._brti_exception_detail(TimeoutError())

    assert detail == "BRTI check error: TimeoutError: TimeoutError()"


def test_brti_payload_diagnostic_reports_shape_without_exposing_prices():
    payload = {
        "data": {
            "serverTime": "2026-11-27T00:00:00Z",
            "payload": [{"value": "12345.67", "time": 1795747200000}],
        }
    }

    detail = MarketDataHub._brti_payload_shape_detail(payload)

    assert "payload_type=list" in detail
    assert "rows=1" in detail
    assert "first_row_fields=['time', 'value']" in detail
    assert "12345.67" not in detail

def test_brti_request_uses_kalshi_supported_compatibility_host_on_fallback():
    assert MarketDataHub._brti_request_base_urls(
        "https://external-api.kalshi.com/trade-api/v2"
    ) == [
        "https://external-api.kalshi.com/trade-api/v2",
        "https://api.elections.kalshi.com/trade-api/v2",
    ]


def test_brti_request_fallback_order_is_reversible_for_legacy_host():
    assert MarketDataHub._brti_request_base_urls(
        "https://api.elections.kalshi.com/trade-api/v2/"
    ) == [
        "https://api.elections.kalshi.com/trade-api/v2",
        "https://external-api.kalshi.com/trade-api/v2",
    ]


def test_brti_request_does_not_override_custom_host():
    assert MarketDataHub._brti_request_base_urls(
        "https://custom.example/trade-api/v2"
    ) == ["https://custom.example/trade-api/v2"]