import asyncio, base64, json, math, time, logging
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional, Tuple
import aiohttp, websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding
from app.config import settings
from app.models import ContractInfo, FeedStatus
from app.multi_exchange import MultiExchangeReference

logger = logging.getLogger(__name__)

def safe_age_seconds(ts: Optional[float], now: Optional[float] = None) -> Optional[float]:
    if not ts:
        return None
    now = time.time() if now is None else now
    raw_age = now - float(ts)
    if raw_age < -0.5:
        return None
    return max(0.0, raw_age)


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
        self._current_sec_last_price: Optional[float] = None
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.last_book_ts: float = 0.0
        self.last_book_source: str = "NONE"
        self.kalshi_contract: Optional[ContractInfo] = None
        self.last_kalshi_ts: float = 0.0
        self.kalshi_status_detail: str = "Initializing..."
        self.official_brti_price: Optional[float] = None
        self.last_brti_ts: float = 0.0
        self.brti_status: str = "UNAVAILABLE"
        self.brti_detail: str = "No Kalshi/CFB BRTI entitlement configured"
        self._running = False
        self._tasks = []
        self._feed_runtime = {
            "Spot Feed Worker": {"state": "STOPPED", "restart_count": 0, "last_error": None, "started_ts": None},
            "Kalshi Feed Worker": {"state": "STOPPED", "restart_count": 0, "last_error": None, "started_ts": None},
        }
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
        # Reject malformed, non-finite, future-dated, and materially out-of-order
        # events before they can alter price, volume, bars, or prediction evidence.
        try:
            ts, price, qty = float(ts), float(price), float(qty)
        except (TypeError, ValueError):
            return
        now = time.time()
        if not all(math.isfinite(v) for v in (ts, price, qty)) or ts <= 0 or price <= 0 or qty <= 0:
            return
        if ts > now + 0.5 or (source == self.active_spot_source and ts < self.last_spot_ts):
            return
        if not isinstance(is_buyer_maker, bool) or not isinstance(source, str) or not source.strip():
            return
        self.live_price = price
        self.last_spot_ts = ts
        self.active_spot_source = source
        self.trades.append((ts, price, qty, is_buyer_maker))
        sec = int(ts)
        if self._current_sec == 0:
            self._current_sec = sec
            self._current_sec_vol = qty
            self._current_sec_last_price = price
        elif sec > self._current_sec:
            # Store the completed second's actual last trade price, not the
            # first price from the following second.
            self.bars_1s.append((self._current_sec, self._current_sec_last_price or price, self._current_sec_vol))
            self._current_sec = sec
            self._current_sec_vol = qty
            self._current_sec_last_price = price
        elif sec == self._current_sec:
            self._current_sec_vol += qty
            self._current_sec_last_price = price
        # Timestamp ordering is validated above, so closed seconds never receive
        # late updates. Gaps remain gaps rather than fabricated candles.

    def ingest_book(self, ts: float, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]], source: Optional[str] = None) -> None:
        try:
            ts = float(ts)
            clean_bids = [(float(p), float(q)) for p, q in bids if math.isfinite(float(p)) and math.isfinite(float(q)) and float(p) > 0 and float(q) > 0]
            clean_asks = [(float(p), float(q)) for p, q in asks if math.isfinite(float(p)) and math.isfinite(float(q)) and float(p) > 0 and float(q) > 0]
        except (TypeError, ValueError, IndexError):
            return
        book_source = source or self.active_spot_source or "UNKNOWN"
        if not math.isfinite(ts) or ts <= 0 or ts > time.time() + 0.5 or (book_source == self.last_book_source and ts < self.last_book_ts):
            return
        clean_bids = sorted(clean_bids, key=lambda x: x[0], reverse=True)[:20]
        clean_asks = sorted(clean_asks, key=lambda x: x[0])[:20]
        if not clean_bids or not clean_asks or clean_bids[0][0] >= clean_asks[0][0]:
            return
        self.bids, self.asks = clean_bids, clean_asks
        self.last_book_ts = ts
        self.last_book_source = book_source

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        await self.multi_exchange.start()
        self._tasks = [
            asyncio.create_task(
                self._run_supervised_feed("Spot Feed Worker", self._spot_stream_loop),
                name="spot-feed-supervisor",
            ),
            asyncio.create_task(
                self._run_supervised_feed("Kalshi Feed Worker", self._kalshi_market_loop),
                name="kalshi-feed-supervisor",
            ),
        ]
        if self._private_key and settings.kalshi_key_id:
            self._tasks.append(asyncio.create_task(self._brti_poll_loop(), name="brti-feed"))

    async def _run_supervised_feed(self, worker_name: str, runner, retry_delay: float = 2.0) -> None:
        """Keep critical feed workers alive if an unexpected exception escapes a feed loop."""
        runtime = self._feed_runtime[worker_name]
        while self._running:
            runtime["state"] = "RUNNING"
            runtime["started_ts"] = time.time()
            try:
                await runner()
                if not self._running:
                    break
                runtime["last_error"] = "Worker exited unexpectedly"
                logger.error("%s exited while feed hub was running; restarting", worker_name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                runtime["last_error"] = f"{type(exc).__name__}: {exc}"
                logger.exception("%s crashed; restarting", worker_name)
            if self._running:
                runtime["restart_count"] += 1
                runtime["state"] = "RESTARTING"
                await asyncio.sleep(retry_delay)
        runtime["state"] = "STOPPED"

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for runtime in self._feed_runtime.values():
            runtime["state"] = "STOPPED"
        await self.multi_exchange.stop()

    def get_multi_exchange_snapshot(self) -> dict:
        snapshot = self.multi_exchange.get_snapshot()
        now = time.time()
        primary_age = safe_age_seconds(self.last_spot_ts, now)
        brti_age = safe_age_seconds(self.last_brti_ts, now)
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
        next_binance_probe = 0.0
        while self._running:
            # Coinbase is a fallback, not a permanent one-way switch. Probe
            # Binance again periodically so a recovered primary feed can resume.
            attempt_coinbase = use_coinbase and time.monotonic() < next_binance_probe
            try:
                if attempt_coinbase:
                    await self._run_coinbase_ws()
                else:
                    await self._run_binance_ws()
                    use_coinbase = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not attempt_coinbase:
                    use_coinbase = True
                    next_binance_probe = time.monotonic() + (
                        60.0 if ("451" in str(exc) or "403" in str(exc)) else 15.0
                    )
                # Keep fallback retries alive, but avoid tight reconnect loops.
                await asyncio.sleep(1.5)
            else:
                # A clean server-side close is still a disconnect. If the
                # primary closed, fall back and schedule a later primary probe.
                if self._running:
                    if not attempt_coinbase:
                        use_coinbase = True
                        next_binance_probe = time.monotonic() + 15.0
                    await asyncio.sleep(1.5)

    @staticmethod
    def _parse_exchange_timestamp(value, fallback: Optional[float] = None) -> float:
        try:
            if isinstance(value, (int, float)):
                parsed = float(value)
                if parsed > 1e12:
                    parsed /= 1000.0
            elif isinstance(value, str) and value.strip():
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            else:
                parsed = fallback if fallback is not None else time.time()
        except (TypeError, ValueError, OverflowError):
            parsed = fallback if fallback is not None else time.time()
        return parsed if math.isfinite(parsed) and parsed > 0 else (fallback if fallback is not None else time.time())

    async def _run_binance_ws(self) -> None:
        async with websockets.connect(settings.binance_ws_url, ping_interval=20, open_timeout=8) as ws:
            async for raw in ws:
                if not self._running: break
                payload = json.loads(raw)
                stream, data, received_at = payload.get("stream", ""), payload.get("data", {}), time.time()
                if "aggTrade" in stream:
                    event_ts = self._parse_exchange_timestamp(data.get("T"), received_at)
                    # Binance maker flag must be a JSON boolean; never coerce a
                    # malformed string/null into a BUY/SELL classification.
                    self.ingest_trade(event_ts, float(data["p"]), float(data["q"]), data.get("m"), "BINANCE_SPOT")
                elif "depth20" in stream:
                    event_ts = self._parse_exchange_timestamp(data.get("E"), received_at)
                    self.ingest_book(event_ts, [(float(p), float(q)) for p, q in data.get("bids", [])], [(float(p), float(q)) for p, q in data.get("asks", [])], source="BINANCE_SPOT")

    @staticmethod
    def _coinbase_is_buyer_maker(side) -> Optional[bool]:
        normalized = str(side or "").lower()
        if normalized == "buy":
            return True
        if normalized == "sell":
            return False
        return None

    @staticmethod
    def _coinbase_ticker_book(msg: dict):
        required = ("best_bid", "best_ask", "best_bid_size", "best_ask_size")
        if not all(key in msg for key in required):
            return None
        try:
            bid, ask = float(msg["best_bid"]), float(msg["best_ask"])
            bid_qty, ask_qty = float(msg["best_bid_size"]), float(msg["best_ask_size"])
        except (TypeError, ValueError):
            return None
        if not all(math.isfinite(v) and v > 0 for v in (bid, ask, bid_qty, ask_qty)) or bid >= ask:
            return None
        return ([(bid, bid_qty)], [(ask, ask_qty)])

    async def _run_coinbase_ws(self) -> None:
        sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "ticker"]}
        async with websockets.connect(settings.coinbase_ws_url, ping_interval=20, open_timeout=8) as ws:
            await ws.send(json.dumps(sub))
            async for raw in ws:
                if not self._running: break
                msg, received_at = json.loads(raw), time.time()
                event_ts = self._parse_exchange_timestamp(msg.get("time"), received_at)
                if msg.get("type") == "match":
                    maker_flag = self._coinbase_is_buyer_maker(msg.get("side"))
                    if maker_flag is not None:
                        self.ingest_trade(event_ts, float(msg["price"]), float(msg["size"]), maker_flag, "COINBASE_FALLBACK")
                elif msg.get("type") == "ticker":
                    parsed_book = self._coinbase_ticker_book(msg)
                    if parsed_book is not None:
                        self.ingest_book(event_ts, parsed_book[0], parsed_book[1], source="COINBASE_FALLBACK")

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

    @staticmethod
    def _parse_brti_value(value) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed > 0 else None

    @classmethod
    def _parse_brti_payload(cls, response, now: Optional[float] = None) -> Optional[Tuple[float, float]]:
        """Parse the documented CF Benchmarks payload and retain its source timestamp."""
        if not isinstance(response, dict):
            return None
        payload = response.get("payload")
        if payload is None and isinstance(response.get("data"), dict):
            payload = response["data"].get("payload")
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            return None
        current = time.time() if now is None else float(now)
        candidates = []
        for item in payload:
            if not isinstance(item, dict) or item.get("repeatOfPreviousValue") is True:
                continue
            value = cls._parse_brti_value(item.get("value"))
            try:
                source_ts = float(item.get("time")) / 1000.0
            except (TypeError, ValueError, OverflowError):
                continue
            if value is None or not math.isfinite(source_ts) or source_ts <= 0:
                continue
            age = current - source_ts
            if age < -0.5 or age > settings.stale_feed_threshold_sec:
                continue
            candidates.append((source_ts, value))
        if not candidates:
            return None
        source_ts, value = max(candidates, key=lambda item: item[0])
        return value, source_ts

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
                            parsed = self._parse_brti_payload(await resp.json())
                            if parsed is not None:
                                self.official_brti_price, self.last_brti_ts = parsed
                                self.brti_status, self.brti_detail = "LIVE", "Official CF Benchmarks BRTI (source timestamp validated)"
                            else:
                                self.brti_status, self.brti_detail = "INVALID_DATA", "No valid, fresh BRTI value with a source timestamp was returned"
                        elif resp.status in (401, 403):
                            self.brti_status, self.brti_detail = "UNAVAILABLE", "Account lacks CF Benchmarks BRTI entitlement"
                            break
                        else:
                            self.brti_status, self.brti_detail = "UNAVAILABLE", f"Kalshi BRTI HTTP {resp.status}"
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.brti_status, self.brti_detail = "UNAVAILABLE", f"BRTI check error: {e}"
                await asyncio.sleep(2.0)

    def get_feed_statuses(self) -> List[FeedStatus]:
        now = time.time()
        spot_age = safe_age_seconds(self.last_spot_ts, now)
        spot_status = "FUTURE_TIMESTAMP" if self.last_spot_ts and spot_age is None else ("UNAVAILABLE" if spot_age is None else ("STALE" if spot_age > settings.stale_feed_threshold_sec else ("LIVE" if self.active_spot_source == "BINANCE_SPOT" else "FALLBACK")))
        book_age = safe_age_seconds(self.last_book_ts, now)
        book_source_matches = bool(
            self.active_spot_source
            and self.last_book_source == self.active_spot_source
        )
        book_status = (
            "FUTURE_TIMESTAMP" if self.last_book_ts and book_age is None
            else ("SOURCE_MISMATCH" if book_age is not None and book_age <= settings.stale_feed_threshold_sec and not book_source_matches
            else ("LIVE" if (book_age is not None and book_age <= settings.stale_feed_threshold_sec)
            else ("STALE" if book_age is not None else "UNAVAILABLE")))
        )
        kalshi_age = safe_age_seconds(self.last_kalshi_ts, now)
        kalshi_status = (
            "FUTURE_TIMESTAMP" if self.last_kalshi_ts and kalshi_age is None
            else ("LIVE" if (self.kalshi_contract is not None and self.kalshi_contract.close_timestamp > now and kalshi_age is not None and kalshi_age <= 12.0)
            else ("STALE" if self.kalshi_contract is not None and kalshi_age is not None and kalshi_age > 12.0 else "UNAVAILABLE"))
        )
        brti_age = safe_age_seconds(self.last_brti_ts, now)
        if self.last_brti_ts and brti_age is None:
            brti_display_status = "FUTURE_TIMESTAMP"
        elif brti_age is None:
            # A LIVE label without a source timestamp is not evidence of a live feed.
            brti_display_status = "UNAVAILABLE" if self.brti_status == "LIVE" else self.brti_status
        elif brti_age > settings.stale_feed_threshold_sec:
            brti_display_status = "STALE"
        else:
            brti_display_status = self.brti_status
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
        worker_statuses = []
        for name, runtime in self._feed_runtime.items():
            started_ts = runtime.get("started_ts")
            worker_age = safe_age_seconds(started_ts, now)
            detail = f"restarts={runtime['restart_count']}"
            if runtime.get("last_error"):
                detail += f" | last error={runtime['last_error']}"
            worker_statuses.append(FeedStatus(
                name=name,
                status=runtime["state"],
                last_updated_iso=iso_now(started_ts) if started_ts else None,
                age_seconds=worker_age,
                detail=detail,
            ))
        return [
            FeedStatus(name="Primary Spot Feed", status=spot_status, last_updated_iso=iso_now(self.last_spot_ts) if self.last_spot_ts else None, age_seconds=spot_age, detail=self.active_spot_source),
            FeedStatus(name="Order Book Depth", status=book_status, last_updated_iso=iso_now(self.last_book_ts) if self.last_book_ts else None, age_seconds=book_age, detail=f"{len(self.bids)} bid / {len(self.asks)} ask levels | source={self.last_book_source} | active spot={self.active_spot_source}"),
            FeedStatus(name="Kalshi KXBTC15M", status=kalshi_status, last_updated_iso=iso_now(self.last_kalshi_ts) if self.last_kalshi_ts else None, age_seconds=kalshi_age, detail=self.kalshi_status_detail),
            FeedStatus(name="CF Benchmarks BRTI", status=brti_display_status, last_updated_iso=iso_now(self.last_brti_ts) if self.last_brti_ts else None, age_seconds=brti_age, detail=self.brti_detail),
        ] + worker_statuses + venue_statuses
