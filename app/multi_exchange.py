import asyncio
import json
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
        self.last_trade_price: Optional[float] = None
        self.trades = deque(maxlen=5000)
        self.connected = False
        self.last_error: Optional[str] = None
        self.reconnects = 0

    def update_level(self, side: str, price: float, qty: float) -> None:
        book = self.bids if side.lower() in ("buy", "bid", "b") else self.asks
        if qty <= 0:
            book.pop(price, None)
        else:
            book[price] = qty

    def snapshot(self, now: Optional[float] = None, max_levels: int = 20) -> dict:
        now = now or time.time()
        bids = sorted(self.bids.items(), reverse=True)[:max_levels]
        asks = sorted(self.asks.items())[:max_levels]
        age = now - self.last_book_ts if self.last_book_ts else None
        if not bids or not asks or age is None or age > 5.0:
            return {
                "venue": self.name, "quote": self.quote, "status": "STALE" if age is not None else "UNAVAILABLE",
                "age_seconds": round(age, 3) if age is not None else None, "last_trade_price": self.last_trade_price,
                "last_trade_age_seconds": round(now - self.last_trade_ts, 3) if self.last_trade_ts else None,
                "bid": None, "ask": None, "mid": None, "spread": None, "spread_bps": None,
                "bid_depth": 0.0, "ask_depth": 0.0, "bid_depth_25bps": 0.0, "ask_depth_25bps": 0.0, "imbalance": None, "levels": 0,
                "last_error": self.last_error, "reconnects": self.reconnects,
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
            "last_trade_age_seconds": round(now - self.last_trade_ts, 3) if self.last_trade_ts else None,
            "bid": bid, "ask": ask, "mid": mid, "spread": ask - bid,
            "spread_bps": ((ask - bid) / mid * 10000.0) if mid else None,
            "bid_depth": bid_depth, "ask_depth": ask_depth, "bid_depth_25bps": bid_depth_25bps, "ask_depth_25bps": ask_depth_25bps, "imbalance": imbalance,
            "top_bid_qty": bid_qty, "top_ask_qty": ask_qty, "levels": min(len(bids), len(asks)),
            "last_error": self.last_error, "reconnects": self.reconnects,
        }


class MultiExchangeReference:
    """Read-only, shadow-mode USD reference from independent public exchange feeds."""

    def __init__(self):
        self.coinbase = VenueBook("COINBASE", "USD")
        self.kraken = VenueBook("KRAKEN", "USD")
        self.binance = VenueBook("BINANCE", "USDT")
        self._running = False
        self._tasks = []

    async def start(self):
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._coinbase_loop(), name="multi-coinbase"),
            asyncio.create_task(self._kraken_loop(), name="multi-kraken"),
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

    async def _coinbase_loop(self):
        url = "wss://ws-feed.exchange.coinbase.com"
        subscribe = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2", "matches"]}
        while self._running:
            try:
                async with websockets.connect(url, ping_interval=20, open_timeout=10, close_timeout=5) as ws:
                    await ws.send(json.dumps(subscribe))
                    self.coinbase.connected = True
                    self.coinbase.last_error = None
                    async for raw in ws:
                        if not self._running:
                            break
                        msg = json.loads(raw)
                        now = time.time()
                        typ = msg.get("type")
                        if typ == "snapshot" and msg.get("product_id") == "BTC-USD":
                            self.coinbase.bids = {float(p): float(q) for p, q in msg.get("bids", []) if float(q) > 0}
                            self.coinbase.asks = {float(p): float(q) for p, q in msg.get("asks", []) if float(q) > 0}
                            self.coinbase.last_book_ts = now
                        elif typ == "l2update" and msg.get("product_id") == "BTC-USD":
                            for side, price, qty in msg.get("changes", []):
                                self.coinbase.update_level(side, float(price), float(qty))
                            self.coinbase.last_book_ts = now
                        elif typ == "match" and msg.get("product_id") == "BTC-USD":
                            p, q = float(msg["price"]), float(msg["size"])
                            self.coinbase.last_trade_price, self.coinbase.last_trade_ts = p, now
                            # Coinbase Exchange "side" is maker-side; retain raw side only.
                            self.coinbase.trades.append((now, p, q, str(msg.get("side", "")).lower()))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.coinbase.connected = False
                self.coinbase.last_error = f"{type(exc).__name__}: {exc}"
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
                        channel = msg.get("channel")
                        data_rows = msg.get("data", [])
                        now = time.time()
                        if channel == "book":
                            for row in data_rows:
                                for side_key, side in (("bids", "bid"), ("asks", "ask")):
                                    levels = row.get(side_key, [])
                                    target = self.kraken.bids if side == "bid" else self.kraken.asks
                                    if msg.get("type") == "snapshot":
                                        target.clear()
                                    for level in levels:
                                        price, qty = float(level["price"]), float(level["qty"])
                                        if qty <= 0:
                                            target.pop(price, None)
                                        else:
                                            target[price] = qty
                            self.kraken.last_book_ts = now
                        elif channel == "trade":
                            for trade in data_rows:
                                p, q = float(trade["price"]), float(trade["qty"])
                                self.kraken.last_trade_price, self.kraken.last_trade_ts = p, now
                                self.kraken.trades.append((now, p, q, str(trade.get("side", "")).lower()))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.kraken.connected = False
                self.kraken.last_error = f"{type(exc).__name__}: {exc}"
                self.kraken.reconnects += 1
                await asyncio.sleep(1.5)

    def get_snapshot(self, now: Optional[float] = None) -> dict:
        now = now or time.time()
        venues = [self.coinbase.snapshot(now), self.kraken.snapshot(now), self.binance.snapshot(now)]
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
            excluded = [v["venue"] for v in usd_live if v not in accepted]
            # Weight accepted USD venues by near-market quote depth, capped to prevent one venue dominating.
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
            "consolidated_bid_depth_usd_within_25bps": total_bid_depth,
            "consolidated_ask_depth_usd_within_25bps": total_ask_depth,
            "consolidated_imbalance": imbalance,
            "method": "USD venues only: median outlier filter (0.35%), then capped depth-weighted mid-price; shadow-mode estimate, not CF Benchmarks BRTI.",
        }
