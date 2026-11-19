import time
import pytest

import app.multi_exchange as multi_exchange_module
from app.multi_exchange import MultiExchangeReference, VenueBook


def _book(venue, bid, ask, qty=2.0):
    venue.bids = {float(bid): qty, float(bid) - 1: qty}
    venue.asks = {float(ask): qty, float(ask) + 1: qty}
    venue.last_book_ts = time.time()
    venue.connected = True


def test_venue_snapshot_calculates_mid_spread_and_imbalance():
    venue = VenueBook("TEST", "USD")
    now = time.time()
    _book(venue, 100.0, 102.0, 3.0)
    snapshot = venue.snapshot(now)

    assert snapshot["status"] == "LIVE"
    assert snapshot["mid"] == 101.0
    assert snapshot["spread"] == 2.0
    assert snapshot["spread_bps"] > 0
    assert snapshot["imbalance"] == 0.0


def test_multi_exchange_reference_requires_two_live_venues():
    reference = MultiExchangeReference()
    _book(reference.coinbase, 90000, 90010, 5.0)
    _book(reference.binance, 92000, 92010, 8.0)
    snapshot = reference.get_snapshot()

    assert snapshot["status"] == "PARTIAL"
    assert snapshot["venue_count_live"] == 2
    assert snapshot["usd_venue_count_live"] == 1
    assert snapshot["reference_price"] is None


def test_multi_exchange_reference_aggregates_and_filters_outlier():
    reference = MultiExchangeReference()
    _book(reference.coinbase, 90000, 90010, 4.0)
    _book(reference.kraken, 90002, 90012, 3.0)
    _book(reference.bitstamp, 92000, 92010, 8.0)
    _book(reference.binance, 92000, 92010, 8.0)

    snapshot = reference.get_snapshot()

    assert snapshot["status"] == "LIVE"
    assert snapshot["venue_count_live"] == 4
    assert snapshot["reference_price"] is not None
    assert set(snapshot["included_venues"]) == {"COINBASE", "KRAKEN"}
    assert "BITSTAMP" in snapshot["outlier_venues"]
    assert "BINANCE" not in snapshot["included_venues" ]
    assert snapshot["consolidated_imbalance"] is not None
    assert "not CF Benchmarks BRTI" in snapshot["method"]


def test_stale_book_is_not_counted_as_live():
    venue = VenueBook("TEST", "USD")
    venue.bids = {100.0: 1.0}
    venue.asks = {101.0: 1.0}
    venue.last_book_ts = time.time() - 8.0

    snapshot = venue.snapshot()

    assert snapshot["status"] == "STALE"
    assert snapshot["mid"] is None


def test_reference_uses_only_usd_venues_for_composite():
    reference = MultiExchangeReference()
    _book(reference.coinbase, 90000, 90010, 5.0)
    _book(reference.kraken, 90002, 90012, 3.0)
    _book(reference.bitstamp, 90003, 90013, 2.0)
    _book(reference.binance, 92000, 92010, 10.0)

    snapshot = reference.get_snapshot()

    assert snapshot["status"] == "LIVE"
    assert snapshot["usd_venue_count_live"] == 3
    assert snapshot["venue_count_live"] == 4
    assert set(snapshot["included_venues"]) == {"COINBASE", "KRAKEN", "BITSTAMP"}
    assert snapshot["reference_age_seconds"] is not None
    assert snapshot["reference_timestamp"] is not None

def test_near_market_depth_imbalance_uses_25bps_window():
    venue = VenueBook("TEST", "USD")
    venue.bids = {100.0: 2.0, 99.0: 10.0}
    venue.asks = {100.2: 1.0, 101.2: 10.0}
    venue.last_book_ts = time.time()

    snapshot = venue.snapshot()

    assert snapshot["bid_depth_25bps"] == 200.0
    assert snapshot["ask_depth_25bps"] == 100.2
    assert snapshot["imbalance"] > 0.0


def test_bitstamp_polling_dependency_is_imported():
    # Catch startup-time NameErrors before the Bitstamp background task is launched.
    assert multi_exchange_module.aiohttp.ClientSession is not None



def test_trade_flow_uses_rolling_windows_and_aggressor_delta():
    venue = VenueBook("TEST", "USD")
    now = 1000.0
    venue.trades.extend([
        (now - 5, 100.0, 0.8, "buy"),
        (now - 3, 100.0, 0.3, "sell"),
        (now - 20, 100.0, 0.5, "sell"),
        (now - 70, 100.0, 0.1, "buy"),
        (now - 200, 100.0, 1.0, "buy"),
    ])
    venue.last_trade_ts = now - 3

    flow = venue.trade_flow_snapshot(now)

    assert flow["windows"]["15"]["buy_volume_btc"] == 0.8
    assert flow["windows"]["15"]["sell_volume_btc"] == 0.3
    assert flow["windows"]["15"]["delta_btc"] == 0.5
    assert flow["windows"]["60"]["delta_btc"] == 0.0
    assert flow["windows"]["180"]["buy_volume_btc"] == 0.9
    assert flow["windows"]["15"]["status"] == "ACTIVE"


def test_unknown_trade_side_is_not_misclassified_as_buy_or_sell():
    venue = VenueBook("TEST", "USD")
    venue.trades.append((100.0, 100.0, 0.25, "unknown"))

    flow = venue.trade_flow_snapshot(100.0)

    assert flow["windows"]["15"]["unknown_volume_btc"] == 0.25
    assert flow["windows"]["15"]["buy_volume_btc"] == 0.0
    assert flow["windows"]["15"]["sell_volume_btc"] == 0.0
    assert flow["windows"]["15"]["delta_ratio"] is None
    assert flow["windows"]["15"]["status"] == "UNCLASSIFIED"


def test_aggregate_cross_venue_flow_sums_btc_delta():
    reference = MultiExchangeReference()
    reference.coinbase.trades.append((100.0, 100.0, 0.4, "buy"))
    reference.kraken.trades.append((100.0, 100.0, 0.2, "sell"))

    snapshot = reference.get_snapshot(now=100.0)

    assert snapshot["trade_flow"]["windows"]["15"]["buy_volume_btc"] == 0.4
    assert snapshot["trade_flow"]["windows"]["15"]["sell_volume_btc"] == 0.2
    assert snapshot["trade_flow"]["windows"]["15"]["delta_btc"] == 0.2
    assert snapshot["trade_flow"]["windows"]["15"]["delta_ratio"] == 0.333333
    assert snapshot["trade_flow"]["windows"]["15"]["source_venues"] == ["COINBASE", "KRAKEN"]



def test_coinbase_maker_side_is_inverted_to_aggressor_side():
    reference = MultiExchangeReference()

    assert reference._coinbase_aggressor_side("buy") == "sell"
    assert reference._coinbase_aggressor_side("sell") == "buy"
    assert reference._coinbase_aggressor_side(None) == "unknown"



def test_binance_aggressor_side_from_aggtrade_maker_flag():
    reference = MultiExchangeReference()

    assert reference._binance_aggressor_side(True) == "sell"
    assert reference._binance_aggressor_side(False) == "buy"
    assert reference._binance_aggressor_side(None) == "unknown"


@pytest.mark.asyncio
async def test_binance_is_started_as_independent_shadow_worker(monkeypatch):
    reference = MultiExchangeReference()
    created = []

    async def fake_worker():
        return None

    monkeypatch.setattr(reference, "_binance_loop", fake_worker)
    monkeypatch.setattr(reference, "_coinbase_loop", fake_worker)
    monkeypatch.setattr(reference, "_kraken_loop", fake_worker)
    monkeypatch.setattr(reference, "_bitstamp_poll_loop", fake_worker)

    await reference.start()
    names = {task.get_name() for task in reference._tasks}
    assert "multi-binance" in names
    assert len(reference._tasks) == 4
    await reference.stop()


def test_crossed_book_is_not_reported_live():
    venue = VenueBook("TEST", "USD")
    venue.bids = {102.0: 1.0}
    venue.asks = {101.0: 1.0}
    venue.last_book_ts = time.time()

    snapshot = venue.snapshot()

    assert snapshot["status"] == "INVALID"
    assert snapshot["mid"] is None


def test_two_venues_with_excessive_pairwise_disagreement_do_not_form_live_composite():
    reference = MultiExchangeReference()
    _book(reference.coinbase, 90000, 90010, 3.0)
    _book(reference.kraken, 90600, 90610, 3.0)

    snapshot = reference.get_snapshot()

    assert snapshot["status"] == "PARTIAL"
    assert snapshot["reference_price"] is None
    assert snapshot["included_venues"] == []
    assert set(snapshot["outlier_venues"]) == {"COINBASE", "KRAKEN"}


def test_non_finite_order_book_levels_are_ignored():
    venue = VenueBook("TEST", "USD")
    venue.update_level("bid", float("nan"), 1.0)
    venue.update_level("bid", 100.0, float("inf"))
    venue.update_level("bid", 100.0, 2.0)
    venue.update_level("ask", 101.0, 2.0)
    venue.last_book_ts = time.time()

    snapshot = venue.snapshot()

    assert snapshot["status"] == "LIVE"
    assert snapshot["bid"] == 100.0
    assert snapshot["bid_depth"] == 200.0



def test_future_book_timestamp_is_not_treated_as_fresh():
    venue = VenueBook("TEST", "USD")
    venue.bids = {100.0: 1.0}
    venue.asks = {101.0: 1.0}
    venue.last_book_ts = 1005.0

    snapshot = venue.snapshot(now=1000.0)

    assert snapshot["status"] == "FUTURE_TIMESTAMP"
    assert snapshot["mid"] is None
    assert snapshot["age_seconds"] is None


def test_future_trades_are_excluded_from_rolling_flow():
    venue = VenueBook("TEST", "USD")
    venue.trades.extend([
        (995.0, 100.0, 0.5, "buy"),
        (1001.0, 100.0, 10.0, "sell"),
    ])

    flow = venue.trade_flow_snapshot(now=1000.0)

    assert flow["windows"]["15"]["buy_volume_btc"] == 0.5
    assert flow["windows"]["15"]["sell_volume_btc"] == 0.0
    assert flow["windows"]["15"]["trade_count"] == 1


def test_bitstamp_ticker_poll_does_not_claim_trade_freshness():
    venue = VenueBook("BITSTAMP", "USD")
    venue.last_trade_price = 100.0
    venue.last_ticker_ts = 995.0

    snapshot = venue.snapshot(now=1000.0)

    assert snapshot["last_trade_age_seconds"] is None
    assert snapshot["last_ticker_age_seconds"] == 5.0



def test_invalid_order_book_side_is_ignored():
    venue = VenueBook("TEST", "USD")
    venue.update_level("mystery", 100.0, 1.0)

    assert venue.bids == {}
    assert venue.asks == {}


def test_future_trade_timestamp_is_not_reported_as_zero_age():
    venue = VenueBook("TEST", "USD")
    venue.last_trade_ts = 1005.0

    flow = venue.trade_flow_snapshot(now=1000.0)

    assert flow["latest_trade_age_seconds"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("worker_name,venue_name", [
    ("_binance_loop", "binance"),
    ("_coinbase_loop", "coinbase"),
    ("_kraken_loop", "kraken"),
])
async def test_clean_websocket_close_marks_venue_disconnected_and_backoffs(monkeypatch, worker_name, venue_name):
    reference = MultiExchangeReference()
    reference._running = True

    class FakeWebSocket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return None
        async def send(self, _message):
            return None
        def __aiter__(self):
            async def empty():
                if False:
                    yield None
            return empty()

    monkeypatch.setattr(multi_exchange_module.websockets, "connect", lambda *args, **kwargs: FakeWebSocket())

    async def stop_on_backoff(_seconds):
        reference._running = False

    monkeypatch.setattr(multi_exchange_module.asyncio, "sleep", stop_on_backoff)
    await getattr(reference, worker_name)()

    venue = getattr(reference, venue_name)
    assert venue.connected is False
    assert venue.reconnects == 1
    assert "closed cleanly" in venue.last_error



def test_shadow_trade_ingestion_rejects_nonfinite_and_invalid_values():
    venue = VenueBook("TEST", "USD")
    now = time.time()

    assert venue.ingest_trade(now, 100.0, 0.5, "buy") is True
    baseline = (len(venue.trades), venue.last_trade_price, venue.last_trade_ts)
    assert venue.ingest_trade(now, float("inf"), 0.5, "buy") is False
    assert venue.ingest_trade(now, 100.0, float("nan"), "sell") is False
    assert venue.ingest_trade(now + 5.0, 100.0, 1.0, "buy") is False
    assert venue.ingest_trade(now, 0.0, 1.0, "buy") is False

    assert (len(venue.trades), venue.last_trade_price, venue.last_trade_ts) == baseline


def test_trade_flow_snapshot_ignores_malformed_records():
    venue = VenueBook("TEST", "USD")
    now = 1000.0
    venue.trades.extend([
        (995.0, 100.0, 0.5, "buy"),
        (996.0, 100.0, float("inf"), "sell"),
        ("bad", 100.0, 1.0, "buy"),
        (1001.0, 100.0, 10.0, "sell"),
    ])

    flow = venue.trade_flow_snapshot(now)

    assert flow["windows"]["15"]["buy_volume_btc"] == 0.5
    assert flow["windows"]["15"]["sell_volume_btc"] == 0.0
    assert flow["windows"]["15"]["trade_count"] == 1

 
def test_ticker_bbo_bootstraps_venue_without_stale_depth():
    reference = MultiExchangeReference()
    reference.coinbase.bids = {99.0: 100.0}
    reference.coinbase.asks = {102.0: 100.0}
    now = time.time()

    assert reference._ingest_top_of_book(reference.coinbase, 100.0, 101.0, 2.0, 3.0, now)
    snapshot = reference.coinbase.snapshot(now)

    assert snapshot["status"] == "LIVE"
    assert snapshot["mid"] == 100.5
    assert snapshot["levels"] == 1
    assert snapshot["bid_depth"] == 200.0
    assert snapshot["ask_depth"] == 303.0


@pytest.mark.parametrize("values", [
    (101.0, 100.0, 1.0, 1.0),
    (100.0, 101.0, 0.0, 1.0),
    (100.0, 101.0, 1.0, float("nan")),
])
def test_ticker_bbo_rejects_crossed_or_invalid_values(values):
    reference = MultiExchangeReference()
    now = time.time()

    assert not reference._ingest_top_of_book(reference.kraken, *values, now)
    assert reference.kraken.bids == {}
    assert reference.kraken.asks == {}


def test_two_usd_venues_create_reference_while_usdt_binance_remains_excluded():
    reference = MultiExchangeReference()
    _book(reference.coinbase, 90000, 90010, 5.0)
    _book(reference.kraken, 90002, 90012, 3.0)
    _book(reference.binance, 92000, 92010, 8.0)

    snapshot = reference.get_snapshot()

    assert snapshot["status"] == "LIVE"
    assert set(snapshot["included_venues"]) == {"COINBASE", "KRAKEN"}
    assert "BINANCE" not in snapshot["included_venues"]
