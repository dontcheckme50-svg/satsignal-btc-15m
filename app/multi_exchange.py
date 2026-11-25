import asyncio
import aiohttp
import json
import math
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

import websockets


class VenueBook:
    def __init__(self, name: str, quote: str):
        self.name = name
        self.quote = quote
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_book_ts: float = 0.0
        self.last_trade_ts: float = 0.0
        self.last_ticker_ts: float = 0.0
        self.last_trade_price: Optional[float] = None
        self.trades = deque(maxlen=50000)
        self.connected = False
        self.last_error: Optional[str] = None
        self.reconnects = 0

    def update_level(self, side: str, price: float, qty: float) -> None:
        price, qty = float(price), float(qty)
        if not math.isfinite(price) or price <= 0 or not math.isfinite(qty):
            return
        normalized_side = str(side or "").lower()
        if normalized_side not in ("buy", "bid", "b", "sell", "ask", "a"):
            return
        book = self.bids if normalized_side in ("buy", "bid", "b") else self.asks
        if qty <= 0:
            book.pop(price, None)
        else:
            book[price] = qty

    def ingest_trade(self, ts: float, price: float, qty: float, side: str) -> bool:
        try:
            ts, price, qty = float(ts), float(price), float(qty)
        except (TypeError, ValueError):
            return False
        side = str(side or "unknown").lower()
        now = time.time()
        if not all(math.isfinite(v) for v in (ts, price, qty)) or ts <= 0 or price <= 0 or qty <= 0 or ts > now + 0.5:
            return False
        if side not in ("buy", "sell", "unknown"):
            side = "unknown"
        self.trades.append((ts, price, qty, side))
        self.last_trade_price, self.last_trade_ts = price, ts
        return True

    def trade_flow_snapshot(self, now: Optional[float] = None, windows=(15, 60, 180)) -> dict:
        now = time.time() if now is None else now
        latest_trade_age = (now - self.last_trade_ts) if self.last_trade_ts and now >= self.last_trade_ts else None
        result = {
            "latest_trade_age_seconds": round(latest_trade_age, 3) if latest_trade_age is not None else None,
            "windows": {},
        }
        max_window = max(windows) if windows else 0
        # Exclude future-dated records as well as trades outside the rolling window.
        recent_max = []
        for trade in self.trades:
            try:
                ts, price, qty, side = trade
                ts, price, qty = float(ts), float(price), float(qty)
            except (TypeError, ValueError):
                continue
            if not all(math.isfinite(v) for v in (ts, price, qty)) or price <= 0 or qty <= 0 or ts > now or ts < now - max_window:
                continue
            recent_max.append((ts, price, qty, side))
        for window in windows:
            recent = [trade for trade in recent_max if now - window <= trade[0] <= now]
            buy_volume = sum(qty for _, _, qty, side in recent if side == "buy")
            sell_volume = sum(qty for _, _, qty, side in recent if side == "sell")
            unknown_volume = sum(qty for _, _, qty, side in recent if side not in ("buy", "sell"))
            classified_volume = buy_volume + sell_volume
            delta = buy_volume - sell_volume
            result["windows"][str(window)] = {
                "trade_count": len(recent),
                "buy_volume_btc": round(buy_volume, 8),
                "sell_volume_btc": round(sell_volume, 8),
                "unknown_volume_btc": round(unknown_volume, 8),
                "delta_btc": round(delta, 8),
                "delta_ratio": round(delta / classified_volume, 6) if classified_volume else None,
                "classified_volume_btc": round(classified_volume, 8),
                "status": "ACTIVE" if classified_volume > 0 else ("UNCLASSIFIED" if unknown_volume > 0 else "QUIET"),
            }
        return result

    def snapshot(self, now: Optional[float] = None, max_levels: int = 20) -> dict:
        now = time.time() if now is None else now
        bids = sorted(
            ((p, q) for p, q in self.bids.items() if math.isfinite(p) and p > 0 and math.isfinite(q) and q > 0),
            reverse=True,
        )[:max_levels]
        asks = sorted(
            ((p, q) for p, q in self.asks.items() if math.isfinite(p) and p > 0 and math.isfinite(q) and q > 0),
        )[:max_levels]
        raw_age = (now - self.last_book_ts) if self.last_book_ts else None
        age = max(0.0, raw_age) if raw_age is not None and raw_age >= -0.5 else None
        book_valid = bool(bids and asks and bids[0][0] > 0 and asks[0][0] > 0 and bids[0][1] > 0 and asks[0][1] > 0 and bids[0][0] < asks[0][0])
        future_timestamp = raw_age is not None and raw_age < -0.5
        if not book_valid or age is None or age > 5.0:
            invalid_status = "FUTURE_TIMESTAMP" if future_timestamp else ("INVALID" if book_valid is False and bids and asks and age is not None and age <= 5.0 else ("STALE" if age is not None else "UNAVAILABLE"))
            return {
                "venue": self.name, "quote": self.quote, "status": invalid_status,
                "age_seconds": round(age, 3) if age is not None else None, "last_trade_price": self.last_trade_price,
                "last_trade_age_seconds": round(now - self.last_trade_ts, 3) if self.last_trade_ts and now >= self.last_trade_ts else None,
                "last_ticker_age_seconds": round(now - self.last_ticker_ts, 3) if self.last_ticker_ts and now >= self.last_ticker_ts else None,
                "connected": self.connected,
                "bid": None, "ask": None, "mid": None, "spread": None, "spread_bps": None,
                "bid_depth": 0.0, "ask_depth": 0.0, "bid_depth_25bps": 0.0, "ask_depth_25bps": 0.0, "imbalance": None, "levels": 0,
                "last_error": self.last_error, "reconnects": self.reconnects,
                "trade_flow": self.trade_flow_snapshot(now),
            }
        bid, bid_qty = bids[0]
        ask, ask_qty = asks[0]
        mid = (bid + ask) / 2.0
        bid_depth = sum(p * q for p, q in bids)
        ask_depth = sum(p * q for p, q in asks)
        bid_depth_25bps = sum(p * q for p, q in bids if p >= mid * 0.9975)
        ask_depth_25bps = sum(p * q for p, q in asks if p <= mid * 1.0025)
        imbalance = (bid_depth_25bps - ask_depth_25bps) / (bid_depth_25bps + ask_depth_25bps) if bid_depth_25bps + ask_depth_25bps else 0.0
        return {
            "venue": self.name, "quote": self.quote, "status": "LIVE", "age_seconds": round(age, 3),
            "last_trade_price": self.last_trade_price,
            "last_trade_age_seconds": round(now - self.last_trade_ts, 3) if self.last_trade_ts and now >= self.last_trade_ts else None,
            "last_ticker_age_seconds": round(now - self.last_ticker_ts, 3) if self.last_ticker_ts and now >= self.last_ticker_ts else None,
            "connected": self.connected,
            "bid": bid, "ask": ask, "mid": mid, "spread": ask - bid,
            "spread_bps": ((ask - bid) / mid * 10000.0) if mid else None,
            "bid_depth": bid_depth, "ask_depth": ask_depth, "bid_depth_25bps": bid_depth_25bps, "ask_depth_25bps": ask_depth_25bps, "imbalance": imbalance,
            "top_bid_qty": bid_qty, "top_ask_qty": ask_qty, "levels": min(len(bids), len(asks)),
            "last_error": self.last_error, "reconnects": self.reconnects,
            "trade_flow": self.trade_flow_snapshot(now),
        }


class MultiExchangeReference:
    """Read-only shadow reference from independent public USD venues."""

    def __init__(self):
        self.coinbase = VenueBook("COINBASE", "USD")
        self.kraken = VenueBook("KRAKEN", "USD")
        self.bitstamp = VenueBook("BITSTAMP", "USD")
        self.binance = VenueBook("BINANCE", "USDT")
        self._running = False
        self._tasks = []

    @staticmethod
    def _coinbase_subscription_confirmed(message: dict) -> bool:
        """Return true only after Coinbase confirms both requested BTC-USD channels."""
        if not isinstance(message, dict) or message.get("type") != "subscriptions":
            return False
        channels = message.get("channels")
        if not isinstance(channels, list):
            return False
        confirmed = set()
        for channel in channels:
            product_ids = channel.get("product_ids") if isinstance(channel, dict) else None
            if not isinstance(product_ids, list) or "BTC-USD" not in product_ids:
                continue
            confirmed.add(channel.get("name"))
        return {"level2", "matches"}.issubset(confirmed)

    @staticmethod
    def _coinbase_aggressor_side(maker_side: str) -> str:
        maker_side = str(maker_side or "").lower()
        if maker_side == "buy":
            return "sell"
        if maker_side == "sell":
            return "buy"
        return "unknown"

    async def start(self):
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._coinbase_loop(), name="multi-coinbase"),
            asyncio.create_task(self._kraken_loop(), name="multi-kraken"),
            asyncio.create_task(self._bitstamp_poll_loop(), name="multi-bitstamp"),
            asyncio.create_task(self._binance_loop(), name="multi-binance"),
        ]

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def ingest_binance(self, price: Optional[float], bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], ts: float):
        if price and price > 0:
            self.binance.last_trade_price = float(price)
            self.binance.last_trade_ts = ts
        if bids and asks:
            self.binance.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
            self.binance.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
            self.binance.last_book_ts = ts
            self.binance.connected = True

    @staticmethod
    def _binance_aggressor_side(buyer_is_maker) -> str:
        # Binance aggTrade m=true means buyer was maker; aggressive taker sold.
        if buyer_is_maker is True:
            return "sell"
        if buyer_is_maker is False:
            return "buy"
        return "unknown"

    async def _binance_loop(self):
        url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth20@100ms/btcusdt@aggTrade"
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, open_timeout=10, close_timeout=5) as ws:
                    self.binance.connected = True
                    self.binance.last_error = None
                    async for raw in ws:
                        if not self._running:
                            break
                        envelope = json.loads(raw)
                        msg = envelope.get("data", envelope)
                        stream = str(envelope.get("stream", ""))
                        now = time.time()
                        if "depth20" in stream or ("bids" in msg and "asks" in msg):
                            bids = [(float(p), float(q)) for p, q in msg.get("bids", []) if float(p) > 0 and float(q) > 0]
                            asks = [(float(p), float(q)) for p, q in msg.get("asks", []) if float(p) > 0 and float(q) > 0]
                            if bids and asks and max(p for p, _ in bids) < min(p for p, _ in asks):
                                self.binance.bids = dict(bids)
                                self.binance.asks = dict(asks)
                                self.binance.last_book_ts = now
                        elif "aggTrade" in stream or msg.get("e") == "aggTrade":
                            price, qty = float(msg["p"]), float(msg["q"])
                            if price > 0 and qty > 0:
                                side = self._binance_aggressor_side(msg.get("m"))
                                self.binance.ingest_trade(now, price, qty, side)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.binance.connected = False
                self.binance.last_error = f"{type(exc).__name__}: {exc}"
                self.binance.reconnects += 1
                await asyncio.sleep(1.5)
            else:
                if self._running:
                    self.binance.connected = False
                    self.binance.last_error = "WebSocket closed cleanly; reconnecting"
                    self.binance.reconnects += 1
                    await asyncio.sleep(1.5)

    async def _coinbase_loop(self):
        url = "wss://ws-feed.exchange.coinbase.com"
        subscribe = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2", "matches"]}
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, open_timeout=10, close_timeout=5) as ws:
                    await ws.send(json.dumps(subscribe))
                    self.coinbase.connected = False
                    self.coinbase.last_error = "Awaiting Coinbase subscription confirmation"
                    async for raw in ws:
                        if not self._running:
                            break
                        msg = json.loads(raw)
                        now = time.time()
                        typ = msg.get("type")
                        if typ == "subscriptions":
                            if not self._coinbase_subscription_confirmed(msg):
                                raise RuntimeError("Coinbase subscription confirmation missing BTC-USD level2/matches")
                            self.coinbase.connected = True
                            self.coinbase.last_error = None
                        elif typ == "error":
                            raise RuntimeError(f"Coinbase protocol error: {msg.get('message') or msg}")
                        elif typ == "snapshot" and msg.get("product_id") == "BTC-USD":
                            self.coinbase.bids.clear()
                            self.coinbase.asks.clear()
                            valid_levels = 0
                            for price, qty in msg.get("bids", []):
                                before = len(self.coinbase.bids)
                                self.coinbase.update_level("bid", float(price), float(qty))
                                valid_levels += int(len(self.coinbase.bids) > before)
                            for price, qty in msg.get("asks", []):
                                before = len(self.coinbase.asks)
                                self.coinbase.update_level("ask", float(price), float(qty))
                                valid_levels += int(len(self.coinbase.asks) > before)
                            if valid_levels:
                                self.coinbase.last_book_ts = now
                        elif typ == "l2update" and msg.get("product_id") == "BTC-USD":
                            valid_changes = 0
                            for side, price, qty in msg.get("changes", []):
                                try:
                                    price, qty = float(price), float(qty)
                                except (TypeError, ValueError):
                                    continue
                                if math.isfinite(price) and price > 0 and math.isfinite(qty):
                                    self.coinbase.update_level(side, price, qty)
                                    valid_changes += 1
                            if valid_changes:
                                self.coinbase.last_book_ts = now
                        elif typ == "match" and msg.get("product_id") == "BTC-USD":
                            p, q = float(msg["price"]), float(msg["size"])
                            # Coinbase match side is maker-side; invert to aggressor side.
                            aggressor_side = self._coinbase_aggressor_side(msg.get("side"))
                            self.coinbase.ingest_trade(now, p, q, aggressor_side)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.coinbase.connected = False
                self.coinbase.last_error = f"{type(exc).__name__}: {exc}"
                self.coinbase.reconnects += 1
                await asyncio.sleep(1.5)
            else:
                if self._running:
                    self.coinbase.connected = False
                    self.coinbase.last_error = "WebSocket closed cleanly; reconnecting"
                    self.coinbase.reconnects += 1
                    await asyncio.sleep(1.5)

    async def _kraken_loop(self):
        url = "wss://ws.kraken.com/v2"
        subscribe_book = {"method": "subscribe", "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10}}
        subscribe_trades = {"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD"]}}
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, open_timeout=10, close_timeout=5) as ws:
                    await ws.send(json.dumps(subscribe_book))
                    await ws.send(json.dumps(subscribe_trades))
                    self.kraken.connected = True
                    self.kraken.last_error = None
                    async for raw in ws:
                        if not self._running:
                            break
                        msg = json.loads(raw)
                        channel, data_rows = msg.get("channel"), msg.get("data", [])
                        now = time.time()
                        if channel == "book":
                            valid_updates = 0
                            for row in data_rows:
                                for side_key, side in (("bids", "bid"), ("asks", "ask")):
                                    target = self.kraken.bids if side == "bid" else self.kraken.asks
                                    if msg.get("type") == "snapshot":
                                        target.clear()
                                    for level in row.get(side_key, []):
                                        try:
                                            price, qty = float(level["price"]), float(level["qty"])
                                        except (KeyError, TypeError, ValueError):
                                            continue
                                        if not math.isfinite(price) or price <= 0 or not math.isfinite(qty):
                                            continue
                                        self.kraken.update_level(side, price, qty)
                                        valid_updates += 1
                            if valid_updates:
                                self.kraken.last_book_ts = now
                        elif channel == "trade":
                            for trade in data_rows:
                                p, q = float(trade["price"]), float(trade["qty"])
                                self.kraken.ingest_trade(now, p, q, str(trade.get("side", "")).lower())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.kraken.connected = False
                self.kraken.last_error = f"{type(exc).__name__}: {exc}"
                self.kraken.reconnects += 1
                await asyncio.sleep(1.5)
            else:
                if self._running:
                    self.kraken.connected = False
                    self.kraken.last_error = "WebSocket closed cleanly; reconnecting"
                    self.kraken.reconnects += 1
                    await asyncio.sleep(1.5)

    async def _bitstamp_poll_loop(self):
        book_url = "https://www.bitstamp.net/api/v2/order_book/btcusd/?group=1"
        ticker_url = "https://www.bitstamp.net/api/v2/ticker/btcusd/"
        while self._running:
            try:
                timeout = aiohttp.ClientTimeout(total=4)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    while self._running:
                        try:
                            async def get_json(url):
                                async with session.get(url) as response:
                                    response.raise_for_status()
                                    return await response.json()

                            book_data, ticker_data = await asyncio.gather(
                                get_json(book_url), get_json(ticker_url)
                            )
                            now = time.time()
                            bids = [(float(p), float(q)) for p, q in book_data.get("bids", []) if float(q) > 0]
                            asks = [(float(p), float(q)) for p, q in book_data.get("asks", []) if float(q) > 0]
                            if bids and asks:
                                self.bitstamp.bids = dict(bids)
                                self.bitstamp.asks = dict(asks)
                                self.bitstamp.last_book_ts = now
                                self.bitstamp.connected = True
                                self.bitstamp.last_error = None
                            last = ticker_data.get("last")
                            if last:
                                # REST ticker polling is not a trade stream. Track ticker freshness
                                # separately and do not pretend it proves a recent execution.
                                self.bitstamp.last_trade_price = float(last)
                                self.bitstamp.last_ticker_ts = now
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            self.bitstamp.connected = False
                            self.bitstamp.last_error = f"{type(exc).__name__}: {exc}"
                            self.bitstamp.reconnects += 1
                        await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Session creation/teardown errors are also recoverable; keep the
                # venue worker alive instead of silently losing Bitstamp forever.
                self.bitstamp.connected = False
                self.bitstamp.last_error = f"{type(exc).__name__}: {exc}"
                self.bitstamp.reconnects += 1
                await asyncio.sleep(1.5)

    @staticmethod
    def _aggregate_trade_flow(venues: List[dict], windows=(15, 60, 180)) -> dict:
        aggregate = {"windows": {}}
        for window in windows:
            buy = sum(v.get("trade_flow", {}).get("windows", {}).get(str(window), {}).get("buy_volume_btc", 0.0) for v in venues)
            sell = sum(v.get("trade_flow", {}).get("windows", {}).get(str(window), {}).get("sell_volume_btc", 0.0) for v in venues)
            unknown = sum(v.get("trade_flow", {}).get("windows", {}).get(str(window), {}).get("unknown_volume_btc", 0.0) for v in venues)
            count = sum(v.get("trade_flow", {}).get("windows", {}).get(str(window), {}).get("trade_count", 0) for v in venues)
            classified = buy + sell
            delta = buy - sell
            source_venues = sorted(
                v.get("venue", "UNKNOWN")
                for v in venues
                if v.get("trade_flow", {}).get("windows", {}).get(str(window), {}).get("classified_volume_btc", 0.0) > 0
            )
            aggregate["windows"][str(window)] = {
                "trade_count": count,
                "source_venues": source_venues,
                "buy_volume_btc": round(buy, 8),
                "sell_volume_btc": round(sell, 8),
                "unknown_volume_btc": round(unknown, 8),
                "delta_btc": round(delta, 8),
                "delta_ratio": round(delta / classified, 6) if classified else None,
                "classified_volume_btc": round(classified, 8),
                "status": "ACTIVE" if classified > 0 else ("UNCLASSIFIED" if unknown > 0 else "QUIET"),
            }
        return aggregate

    def get_snapshot(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else now
        venues = [self.coinbase.snapshot(now), self.kraken.snapshot(now), self.bitstamp.snapshot(now), self.binance.snapshot(now)]
        live = [v for v in venues if v["status"] == "LIVE" and v["mid"] is not None]
        usd_live = [v for v in live if v["quote"] == "USD"]
        prices = [v["mid"] for v in usd_live]
        composite = None
        excluded = []
        accepted = []
        if prices:
            sorted_prices = sorted(prices)
            median = sorted_prices[len(sorted_prices) // 2] if len(sorted_prices) % 2 else sum(sorted_prices[len(sorted_prices)//2-1:len(sorted_prices)//2+1]) / 2
            accepted = [v for v in usd_live if abs(v["mid"] - median) / median <= 0.0035]
            # Prevent two venues from both passing a median filter while still
            # disagreeing with each other beyond the intended consensus tolerance.
            if len(accepted) >= 2:
                accepted_prices = [v["mid"] for v in accepted]
                pairwise_spread = (max(accepted_prices) - min(accepted_prices)) / median
                if pairwise_spread > 0.0035:
                    accepted = []
            excluded = [v["venue"] for v in usd_live if v not in accepted]
            weights = [max(1.0, min(v["bid_depth_25bps"] + v["ask_depth_25bps"], 2_000_000.0)) for v in accepted]
            if len(accepted) >= 2:
                composite = sum(v["mid"] * w for v, w in zip(accepted, weights)) / sum(weights)
        total_bid_depth = sum(v["bid_depth_25bps"] for v in accepted)
        total_ask_depth = sum(v["ask_depth_25bps"] for v in accepted)
        imbalance = (total_bid_depth - total_ask_depth) / (total_bid_depth + total_ask_depth) if total_bid_depth + total_ask_depth else None
        return {
            "timestamp": now,
            "reference_age_seconds": max((v["age_seconds"] for v in accepted), default=None),
            "reference_timestamp": now - max((v["age_seconds"] for v in accepted), default=0.0) if accepted else None,
            "status": "LIVE" if len(accepted) >= 2 and composite is not None else ("PARTIAL" if usd_live else "UNAVAILABLE"),
            "reference_price": composite,
            "venue_count_live": len(live),
            "usd_venue_count_live": len(usd_live),
            "venue_count_required": 2,
            "included_venues": [v["venue"] for v in accepted],
            "outlier_venues": excluded,
            "venues": venues,
            "trade_flow": self._aggregate_trade_flow(venues),
            "consolidated_bid_depth_usd_within_25bps": total_bid_depth,
            "consolidated_ask_depth_usd_within_25bps": total_ask_depth,
            "consolidated_imbalance": imbalance,
            "quality": {
                "eligible": len(accepted) >= 2 and composite is not None,
                "minimum_venues_met": len(accepted) >= 2,
                "freshness_max_seconds": 5.0,
                "agreement_tolerance_pct": 0.35,
                "accepted_venue_count": len(accepted),
                "accepted_venue_spread_bps": round((max(v["mid"] for v in accepted) - min(v["mid"] for v in accepted)) / median * 10000.0, 4) if len(accepted) >= 2 and median > 0 else None,
                "reason": "PASS" if len(accepted) >= 2 and composite is not None else ("INSUFFICIENT_AGREEMENT" if usd_live else "NO_FRESH_USD_VENUES"),
            },
            "benchmark_type": "SYNTHETIC_MULTI_EXCHANGE",
            "method": "Synthetic USD-venue reference: median outlier filter (0.35%), then capped depth-weighted mid-price; never official CF Benchmarks BRTI. Cross-venue trade-flow volumes are summed observations and may duplicate activity across venues; they are not unique global volume.",
        }
