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
