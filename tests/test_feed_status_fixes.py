import json
import time

import pytest

from datetime import datetime, timezone

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
    detail = MarketDataHub._kalshi_request_exception_detail(TimeoutError(), "BRTI")

    assert detail == (
        "BRTI overall request timeout or unclassified timeout error "
        "(TimeoutError): TimeoutError()"
    )


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
    assert MarketDataHub._kalshi_rest_base_urls(
        "https://external-api.kalshi.com/trade-api/v2"
    ) == [
        "https://external-api.kalshi.com/trade-api/v2",
        "https://api.elections.kalshi.com/trade-api/v2",
    ]


def test_brti_request_fallback_order_is_reversible_for_legacy_host():
    assert MarketDataHub._kalshi_rest_base_urls(
        "https://api.elections.kalshi.com/trade-api/v2/"
    ) == [
        "https://api.elections.kalshi.com/trade-api/v2",
        "https://external-api.kalshi.com/trade-api/v2",
    ]


def test_brti_request_does_not_override_custom_host():
    assert MarketDataHub._kalshi_rest_base_urls(
        "https://custom.example/trade-api/v2"
    ) == ["https://custom.example/trade-api/v2"]

def test_parses_fresh_official_brti_5hz_message():
    now = 1_800_000_000.25
    event = {
        "type": "cfbenchmarks_value_5hz",
        "msg": {
            "index_id": "BRTI",
            "value_usd": "90123.45670000",
            "source_ts_ms": 1_800_000_000_200,
            "received_at": 1_800_000_000_220,
        },
    }

    parsed = MarketDataHub._parse_brti_5hz_message(event, now=now)

    assert parsed == (90123.4567, 1_800_000_000.2)


def test_rejects_non_brti_or_stale_5hz_events():
    now = 1_800_000_000.0
    wrong_index = {
        "type": "cfbenchmarks_value_5hz",
        "msg": {"index_id": "ETHUSD_RTI", "value_usd": "3000", "source_ts_ms": 1_800_000_000_000},
    }
    stale = {
        "type": "cfbenchmarks_value_5hz",
        "msg": {"index_id": "BRTI", "value_usd": "90000", "source_ts_ms": 1_799_999_980_000},
    }

    assert MarketDataHub._parse_brti_5hz_message(wrong_index, now=now) is None
    assert MarketDataHub._parse_brti_5hz_message(stale, now=now) is None


def test_brti_5hz_indexlist_must_explicitly_include_brti():
    valid = {
        "type": "cfbenchmarks_value_5hz_indexlist",
        "msg": {"index_ids": ["BRTI", "ETHUSD_RTI"]},
    }
    missing = {
        "type": "cfbenchmarks_value_5hz_indexlist",
        "msg": {"index_ids": ["ETHUSD_RTI"]},
    }

    assert MarketDataHub._brti_ws_indexlist_has_brti(valid, "cfbenchmarks_value_5hz") is True
    assert MarketDataHub._brti_ws_indexlist_has_brti(missing, "cfbenchmarks_value_5hz") is False
    assert MarketDataHub._brti_ws_indexlist_has_brti({"type": "other"}, "cfbenchmarks_value_5hz") is None


def test_parses_official_standard_one_second_brti_websocket_tick():
    now = 1_800_000_000.25
    event = {
        "type": "cfbenchmarks_value",
        "msg": {
            "index_id": "BRTI",
            "received_at": 1_800_000_000_220,
            "data": json.dumps({
                "type": "value",
                "id": "BRTI",
                "time": 1_800_000_000_200,
                "value": "90123.4567",
            }),
        },
    }

    assert MarketDataHub._parse_brti_ws_message(
        event, "cfbenchmarks_value", now=now
    ) == (90123.4567, 1_800_000_000.2)


@pytest.mark.asyncio
async def test_brti_websocket_verifies_indexlist_and_fails_over(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    import app.feeds as feeds

    monkeypatch.setattr(
        feeds,
        "settings",
        SimpleNamespace(
            kalshi_key_id="test-key-id",
            kalshi_private_key=None,
            kalshi_private_key_path=None,
        ),
    )
    hub = MarketDataHub()
    hub._private_key = feeds.ed25519.Ed25519PrivateKey.generate()
    hub._running = True
    calls = []
    sent_messages = []
    source_ts_ms = int(time.time() * 1000)

    class FakeFrame:
        type = feeds.aiohttp.WSMsgType.TEXT

        def __init__(self, event):
            self.data = json.dumps(event)

    class FakeWebSocket:
        def __init__(self):
            self.frames = iter([
                FakeFrame({
                    "id": 1, "type": "subscribed",
                    "msg": {"channel": "cfbenchmarks_value_5hz", "sid": 7},
                }),
                FakeFrame({
                    "id": 2, "type": "cfbenchmarks_value_5hz_indexlist",
                    "msg": {"index_ids": ["BRTI", "ETHUSD_RTI"]},
                }),
                FakeFrame({
                    "type": "cfbenchmarks_value_5hz",
                    "msg": {
                        "index_id": "BRTI",
                        "value_usd": "90123.45",
                        "source_ts_ms": source_ts_ms,
                        "received_at": source_ts_ms + 20,
                    },
                }),
            ])

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def send_json(self, value):
            sent_messages.append(value)

        async def receive(self, timeout=None):
            frame = next(self.frames)
            event = json.loads(frame.data)
            if event.get("type") == "cfbenchmarks_value_5hz":
                hub._running = False
            return frame

    class FakeConnect:
        def __init__(self, should_timeout):
            self.should_timeout = should_timeout

        async def __aenter__(self):
            if self.should_timeout:
                raise asyncio.TimeoutError()
            return FakeWebSocket()

        async def __aexit__(self, *_args):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def ws_connect(self, url, **kwargs):
            calls.append((url, kwargs))
            return FakeConnect(should_timeout=len(calls) == 1)

    monkeypatch.setattr(feeds.aiohttp, "ClientSession", FakeSession)

    await hub._brti_websocket_loop()

    assert [item[0] for item in calls] == [
        "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        "wss://api.elections.kalshi.com/trade-api/ws/v2",
    ]
    assert calls[1][1]["headers"]["KALSHI-ACCESS-KEY"] == "test-key-id"
    assert calls[1][1]["headers"]["KALSHI-ACCESS-SIGNATURE"]
    assert sent_messages == [
        {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["cfbenchmarks_value_5hz"],
                "index_ids": ["BRTI"],
            },
        },
        {
            "id": 2,
            "cmd": "update_subscription",
            "params": {"sid": 7, "action": "indexlist"},
        },
    ]
    assert hub.brti_status == "LIVE"
    assert hub.official_brti_price == 90123.45
    assert hub.last_brti_ts == pytest.approx(source_ts_ms / 1000.0)
    assert "5Hz" in hub.brti_detail
    assert "api.elections.kalshi.com" in hub.brti_detail

@pytest.mark.asyncio
async def test_kalshi_market_poller_fails_over_to_supported_host_after_timeout(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    import app.feeds as feeds

    monkeypatch.setattr(
        feeds,
        "settings",
        SimpleNamespace(
            kalshi_public_base_url="https://api.elections.kalshi.com/trade-api/v2",
            checkpoint_interval_sec=180,
            kalshi_key_id=None,
            kalshi_private_key=None,
            kalshi_private_key_path=None,
        ),
    )
    hub = MarketDataHub()
    hub._running = True
    now = time.time()
    calls = []
    market = {
        "ticker": "KXBTC15M-TEST-FAILOVER",
        "open_time": datetime.fromtimestamp(now - 60, timezone.utc).isoformat(),
        "close_time": datetime.fromtimestamp(now + 600, timezone.utc).isoformat(),
        "floor_strike": 90000,
        "yes_bid_dollars": 0.45,
        "yes_ask_dollars": 0.46,
        "no_bid_dollars": 0.54,
        "no_ask_dollars": 0.55,
    }

    class FakeResponse:
        status = 200

        async def json(self):
            hub._running = False
            return {"markets": [market]}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class FakeRequest:
        def __init__(self, should_timeout):
            self.should_timeout = should_timeout

        async def __aenter__(self):
            if self.should_timeout:
                raise asyncio.TimeoutError()
            return FakeResponse()

        async def __aexit__(self, *_args):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def get(self, url, **_kwargs):
            calls.append(url)
            return FakeRequest(should_timeout=len(calls) == 1)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(feeds.aiohttp, "ClientSession", FakeSession)
    monkeypatch.setattr(feeds.asyncio, "sleep", no_sleep)

    await hub._kalshi_market_loop()

    assert calls == [
        "https://api.elections.kalshi.com/trade-api/v2/markets",
        "https://external-api.kalshi.com/trade-api/v2/markets",
    ]
    assert hub.kalshi_contract is not None
    assert hub.kalshi_contract.ticker == "KXBTC15M-TEST-FAILOVER"
    assert hub.kalshi_contract.is_official_kalshi is True
    assert "via external-api.kalshi.com" in hub.kalshi_status_detail