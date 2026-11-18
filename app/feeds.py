import asyncio, base64, json, math, time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional, Tuple
import aiohttp, websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding
from app.config import settings
from app.models import ContractInfo, FeedStatus
from app.multi_exchange import MultiExchangeReference

def iso_now(ts: Optional[float] = None) -> str:
    t = ts if ts is not None else time.time()
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

class MarketDataHub:
    def __init__(self):
        self.live_price: Optional[float] = None
        self.active_spot_source: str = "NONE"
        self.last_spot_ts: float = 0.0
        self.trades: Deque[Tuple[float, float, float, bool]] = deque(maxlen=50000)
        self.bars_1s: Deque[Tuple[int, float, float]] = deque(maxlen=3600)
        self._current_sec: int = 0
        self._current_sec_vol: float = 0.0
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.last_book_ts: float = 0.0
        self.kalshi_contract: Optional[ContractInfo] = None
        self.last_kalshi_ts: float = 0.0
        self.kalshi_status_detail: str = "Initializing..."
        self.official_brti_price: Optional[float] = None
        self.last_brti_ts: float = 0.0
        self.brti_status: str = "UNAVAILABLE"
        self.brti_detail: str = "No Kalshi/CFB BRTI entitlement configured"
        self._running = False
        self._tasks = []
        self.multi_exchange = MultiExchangeReference()
        self._private_key = self._load_kalshi_key()

    def _load_kalshi_key(self):
        if not settings.kalshi_key_id or not settings.kalshi_private_key_path:
            return None
        try:
            with open(settings.kalshi_private_key_path, "rb") as f:
                return serialization.load_pem_private_key(f.read(), password=None)
        except Exception as e:
            self.brti_detail = f"Key load error: {e}"
            return None

    def sign_kalshi(self, ts_ms: str, method: str, path: str) -> str:
        msg = f"{ts_ms}{method.upper()}{path.split('?')[0]}".encode("utf-8")
        if isinstance(self._private_key, ed25519.Ed25519PrivateKey):
            sig = self._private_key.sign(msg)
        else:
            sig = self._private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
        return base64.b64encode(sig).decode("utf-8")

    def ingest_trade(self, ts: float, price: float, qty: float, is_buyer_maker: bool, source: str) -> None:
        self.live_price = price
        self.last_spot_ts = ts
        self.active_spot_source = source
        self.trades.append((ts, price, qty, is_buyer_maker))
        sec = int(ts)
        if self._current_sec == 0:
            self._current_sec = sec
        if sec > self._current_sec:
            self.bars_1s.append((self._current_sec, price, self._current_sec_vol))
            self._current_sec = sec
            self._current_sec_vol = qty
        else:
            self._current_sec_vol += qty

    def ingest_book(self, ts: float, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> None:
        self.bids = sorted(bids, key=lambda x: x[0], reverse=True)[:20]
        self.asks = sorted(asks, key=lambda x: x[0])[:20]
        self.last_book_ts = ts

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.multi_exchange.start()
        self._tasks = [
            asyncio.create_task(self._spot_stream_loop(), name="spot-feed"),
            asyncio.create_task(self._kalshi_market_loop(), name="kalshi-feed"),
        ]
        if self._private_key and settings.kalshi_key_id:
            self._tasks.append(asyncio.create_task(self._brti_poll_loop(), name="brti-feed"))

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        await self.multi_exchange.stop()

    def get_multi_exchange_snapshot(self) -> dict:
        if self.active_spot_source == "BINANCE_SPOT":
            self.multi_exchange.ingest_binance(self.live_price, list(self.bids), list(self.asks), self.last_spot_ts)
        snapshot = self.multi_exchange.get_snapshot()
        now = time.time()
        primary_age = round(now - self.last_spot_ts, 3) if self.last_spot_ts else None
        brti_age = round(now - self.last_brti_ts, 3) if self.last_brti_ts else None
        primary_fresh = primary_age is not None and primary_age <= settings.stale_feed_threshold_sec
        brti_fresh = self.brti_status == "LIVE" and brti_age is not None and brti_age <= settings.stale_feed_threshold_sec
        primary_price = self.live_price if primary_fresh else None
        brti_price = self.official_brti_price if brti_fresh else None
        snapshot["primary_spot_price"] = primary_price
        snapshot["primary_spot_age_seconds"] = primary_age
        snapshot["official_brti_price"] = brti_price
        snapshot["official_brti_age_seconds"] = brti_age
        ref_price = snapshot.get("reference_price")
        def diff_bps(value, benchmark):
            return round((float(value) - float(benchmark)) / float(benchmark) * 10000.0, 4) if value is not None and benchmark is not None and float(benchmark) > 0 else None
        snapshot["reference_vs_primary_bps"] = diff_bps(ref_price, primary_price)
        snapshot["reference_vs_brti_bps"] = diff_bps(ref_price, brti_price)
        return snapshot

    async def fetch_verified_kalshi_settlement(self, ticker: str) -> Optional[Tuple[str, Optional[float]]]:
        if not ticker.startswith("KXBTC15M"):
            return None
        url = f"{settings.kalshi_public_base_url}/markets/{ticker}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=6) as resp:
                    if resp.status != 200: return None
                    m = (await resp.json()).get("market", {})
                    if m.get("status") == "settled" and m.get("result") in ("yes", "no"):
                        return ("UP" if m["result"] == "yes" else "DOWN"), (float(m["expiration_value"]) if m.get("expiration_value") is not None else None)
        except Exception:
            return None
        return None

    async def _spot_stream_loop(self) -> None:
        use_coinbase = False
        while self._running:
            try:
                if not use_coinbase: await self._run_binance_ws()
                else: await self._run_coinbase_ws()
            except Exception as exc:
                use_coinbase = True if ("451" in str(exc) or "403" in str(exc)) else (not use_coinbase)
                await asyncio.sleep(1.5)

    async def _run_binance_ws(self) -> None:
        async with websockets.connect(settings.binance_ws_url, ping_interval=20, open_timeout=8) as ws:
            async for raw in ws:
                if not self._running: break
                payload = json.loads(raw)
                stream, data, now = payload.get("stream", ""), payload.get("data", {}), time.time()
                if "aggTrade" in stream:
                    self.ingest_trade(now, float(data["p"]), float(data["q"]), bool(data["m"]), "BINANCE_SPOT")
                elif "depth20" in stream:
                    self.ingest_book(now, [(float(p), float(q)) for p, q in data.get("bids", [])], [(float(p), float(q)) for p, q in data.get("asks", [])])

    async def _run_coinbase_ws(self) -> None:
        sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "ticker"]}
        async with websockets.connect(settings.coinbase_ws_url, ping_interval=20, open_timeout=8) as ws:
            await ws.send(json.dumps(sub))
            async for raw in ws:
                if not self._running: break
                msg, now = json.loads(raw), time.time()
                if msg.get("type") == "match":
                    self.ingest_trade(now, float(msg["price"]), float(msg["size"]), msg.get("side") == "buy", "COINBASE_FALLBACK")
                elif msg.get("type") == "ticker" and "best_bid" in msg and "best_ask" in msg:
                    self.ingest_book(now, [(float(msg["best_bid"]), float(msg.get("best_bid_size", 1.0)))], [(float(msg["best_ask"]), float(msg.get("best_ask_size", 1.0)))])

    @staticmethod
    def _parse_kalshi_time(value: str) -> float:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    @staticmethod
    def _quote_dollars(value):
        if value in (None, ""):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if 0.0 <= number <= 1.0 else None

    async def _kalshi_market_loop(self) -> None:
        url = f"{settings.kalshi_public_base_url}/markets"
        params = {"series_ticker": "KXBTC15M", "status": "open", "limit": 100}
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    async with session.get(url, params=params, timeout=6) as resp:
                        if resp.status == 200:
                            markets = (await resp.json()).get("markets", [])
                            now = time.time()
                            candidates = []
                            for market in markets:
                                try:
                                    open_ts = self._parse_kalshi_time(market["open_time"])
                                    close_ts = self._parse_kalshi_time(market["close_time"])
                                    raw_strike = market.get("floor_strike")
                                    strike = float(raw_strike)
                                except (KeyError, TypeError, ValueError):
                                    continue

                                # A market is usable only while its official contract window is active.
                                if not (open_ts <= now < close_ts):
                                    continue
                                if not math.isfinite(strike) or strike <= 0 or not str(market.get("ticker", "")).startswith("KXBTC15M"):
                                    continue
                                candidates.append((close_ts, open_ts, strike, market))

                            self.last_kalshi_ts = now
                            if candidates:
                                close_ts, open_ts, strike, market = min(candidates, key=lambda item: item[0])
                                rem = close_ts - now
                                self.kalshi_contract = ContractInfo(
                                    ticker=market["ticker"],
                                    strike_price=strike,
                                    strike_source="KALSHI_OFFICIAL",
                                    open_timestamp=open_ts,
                                    close_timestamp=close_ts,
                                    seconds_remaining=rem,
                                    checkpoint_seconds_remaining=rem % settings.checkpoint_interval_sec,
                                    is_official_kalshi=True,
                                    yes_bid_dollars=self._quote_dollars(market.get("yes_bid_dollars")),
                                    yes_ask_dollars=self._quote_dollars(market.get("yes_ask_dollars")),
                                    no_bid_dollars=self._quote_dollars(market.get("no_bid_dollars")),
                                    no_ask_dollars=self._quote_dollars(market.get("no_ask_dollars")),
                                )
                                self.kalshi_status_detail = f"Active official contract: {market['ticker']}"
                            else:
                                self.kalshi_contract = None
                                self.kalshi_status_detail = "No currently active KXBTC15M contract with an official strike"
                        else:
                            self.kalshi_status_detail = f"HTTP {resp.status}"
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.kalshi_status_detail = f"Poller error: {e}"
                await asyncio.sleep(3.0)

    async def _brti_poll_loop(self) -> None:
        endpoint = "/trade-api/v2/cfbenchmarks/values"
        url = f"{settings.kalshi_prod_base_url}/cfbenchmarks/values?id=BRTI"
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    ts_ms = str(int(time.time() * 1000))
                    headers = {"KALSHI-ACCESS-KEY": settings.kalshi_key_id or "", "KALSHI-ACCESS-SIGNATURE": self.sign_kalshi(ts_ms, "GET", endpoint), "KALSHI-ACCESS-TIMESTAMP": ts_ms}
                    async with session.get(url, headers=headers, timeout=5) as resp:
                        if resp.status == 200:
                            val = (await resp.json()).get("data", {}).get("payload", {}).get("value")
                            if val is not None:
                                self.official_brti_price, self.last_brti_ts, self.brti_status, self.brti_detail = float(val), time.time(), "LIVE", "Official CF Benchmarks BRTI"
                        elif resp.status in (401, 403):
                            self.brti_status, self.brti_detail = "UNAVAILABLE", "Account lacks CF Benchmarks BRTI entitlement"
                            break
                except Exception as e:
                    self.brti_status, self.brti_detail = "UNAVAILABLE", f"BRTI check error: {e}"
                await asyncio.sleep(2.0)

    def get_feed_statuses(self) -> List[FeedStatus]:
        now = time.time()
        spot_age = round(now - self.last_spot_ts, 2) if self.last_spot_ts else None
        spot_status = "UNAVAILABLE" if spot_age is None else ("STALE" if spot_age > settings.stale_feed_threshold_sec else ("LIVE" if self.active_spot_source == "BINANCE_SPOT" else "FALLBACK"))
        book_age = round(now - self.last_book_ts, 2) if self.last_book_ts else None
        book_status = "LIVE" if (book_age is not None and book_age <= settings.stale_feed_threshold_sec) else ("STALE" if book_age is not None else "UNAVAILABLE")
        kalshi_age = round(now - self.last_kalshi_ts, 2) if self.last_kalshi_ts else None
        kalshi_status = (
            "LIVE" if (self.kalshi_contract is not None and self.kalshi_contract.close_timestamp > now and kalshi_age is not None and kalshi_age <= 12.0)
            else ("STALE" if self.kalshi_contract is not None and kalshi_age is not None and kalshi_age > 12.0 else "UNAVAILABLE")
        )
        brti_age = round(now - self.last_brti_ts, 2) if self.last_brti_ts else None
        brti_display_status = "STALE" if brti_age is not None and brti_age > settings.stale_feed_threshold_sec else self.brti_status
        multi = self.get_multi_exchange_snapshot()
        venue_statuses = []
        for venue in multi.get("venues", []):
            venue_statuses.append(FeedStatus(
                name=f"Multi-Exchange {venue['venue']}",
                status=venue["status"],
                age_seconds=venue.get("age_seconds"),
                detail=f"Quote={venue['quote']} | mid={venue.get('mid')} | imbalance={venue.get('imbalance')}",
            ))
        venue_statuses.append(FeedStatus(
            name="Multi-Exchange Reference",
            status=multi["status"],
            last_updated_iso=iso_now(multi["reference_timestamp"]) if multi.get("reference_timestamp") else None,
            age_seconds=multi.get("reference_age_seconds"),
            detail=f"Reference={multi.get('reference_price')} | live venues={multi['venue_count_live']} | outliers={multi['outlier_venues']}",
        ))
        return [
            FeedStatus(name="Primary Spot Feed", status=spot_status, last_updated_iso=iso_now(self.last_spot_ts) if self.last_spot_ts else None, age_seconds=spot_age, detail=self.active_spot_source),
            FeedStatus(name="Order Book Depth", status=book_status, last_updated_iso=iso_now(self.last_book_ts) if self.last_book_ts else None, age_seconds=book_age, detail=f"{len(self.bids)} bid / {len(self.asks)} ask levels"),
            FeedStatus(name="Kalshi KXBTC15M", status=kalshi_status, last_updated_iso=iso_now(self.last_kalshi_ts) if self.last_kalshi_ts else None, age_seconds=kalshi_age, detail=self.kalshi_status_detail),
            FeedStatus(name="CF Benchmarks BRTI", status=brti_display_status, last_updated_iso=iso_now(self.last_brti_ts) if self.last_brti_ts else None, age_seconds=brti_age, detail=self.brti_detail),
        ] + venue_statuses
