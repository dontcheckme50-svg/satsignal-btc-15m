import time

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
