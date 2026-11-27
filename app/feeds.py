import asyncio, base64, json, math, re, time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional, Tuple
import aiohttp, websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa
from app.config import settings
from app.models import ContractInfo, FeedStatus
from app.multi_exchange import MultiExchangeReference

def normalize_kalshi_private_key_text(value: str) -> str:
    """Normalize common environment-secret encodings without logging key material."""
    text = value.lstrip("\ufeff").strip()

    # Secret managers may serialize newline escapes more than once.
    for _ in range(4):
        previous = text
        text = (
            text.replace(r"\\r\\n", r"\r\n")
                .replace(r"\\n", r"\n")
                .replace(r"\\r", r"\r")
                .replace(r"\u000d\u000a", r"\r\n")
                .replace(r"\u000a", r"\n")
                .replace(r"\u000d", r"\r")
        )
        if text == previous:
            break

    # Remove repeated matching quote wrappers left by JSON/secret UIs.
    for _ in range(3):
        text = text.strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
            text = text[1:-1].strip()
        else:
            break

    text = (
        text.replace(r"\r\n", "\n")
            .replace(r"\n", "\n")
            .replace(r"\r", "\n")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
    ).strip()

    # Some secret stores expose the complete PEM as a base64 string.
    if "-----BEGIN " not in text:
        try:
            decoded = base64.b64decode(text, validate=True).decode("utf-8")
        except Exception:
            decoded = ""
        if "-----BEGIN " in decoded:
            text = decoded.strip()

    # Rebuild PEM line wrapping if a host flattened all line breaks.
    # Preserve traditional encrypted-PEM metadata blocks untouched.
    match = re.fullmatch(
        r"\s*-----BEGIN ([A-Z0-9 ]*PRIVATE KEY)-----\s*(.*?)\s*-----END \1-----\s*",
        text,
        flags=re.DOTALL,
    )
    if match:
        label, body = match.groups()
        if not body.lstrip().startswith(("Proc-Type:", "DEK-Info:")):
            compact_body = re.sub(r"\s+", "", body)
            if (
                compact_body
                and len(compact_body) % 4 == 0
                and re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", compact_body)
            ):
                wrapped = "\n".join(
                    compact_body[index:index + 64]
                    for index in range(0, len(compact_body), 64)
                )
                text = f"-----BEGIN {label}-----\n{wrapped}\n-----END {label}-----\n"

    return text


def _safe_key_input_summary(source: str, raw: str, normalized: str) -> str:
    """Return key-format diagnostics only; never include secret content."""
    raw_header = bool(re.search(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", raw))
    raw_footer = bool(re.search(r"-----END [A-Z0-9 ]*PRIVATE KEY-----", raw))
    norm_header = bool(re.search(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----", normalized))
    norm_footer = bool(re.search(r"-----END [A-Z0-9 ]*PRIVATE KEY-----", normalized))
    raw_newlines = raw.count(chr(10)) + raw.count(chr(13))
    escaped_newlines = raw.count("\\n") + raw.count("\\r")
    return (
        f"{source}: chars={len(raw)}, raw_header={raw_header}, raw_footer={raw_footer}, "
        f"normalized_header={norm_header}, normalized_footer={norm_footer}, "
        f"raw_newlines={raw_newlines}, escaped_newlines={escaped_newlines}, "
        f"normalized_lines={len(normalized.splitlines())}"
    )


def iso_now(ts: Optional[float] = None) -> str:
    t = ts if ts is not None else time.time()
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_age_seconds(timestamp: Optional[float], now: Optional[float] = None) -> Optional[float]:
    """Return a non-negative age in seconds, or None when no valid timestamp exists."""
    if timestamp is None:
        return None
    try:
        ts = float(timestamp)
        current = time.time() if now is None else float(now)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(ts) or ts <= 0 or not math.isfinite(current) or ts > current + 0.5:
        return None
    return max(0.0, current - ts)

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
        # Track which venue supplied the latest order book so the orchestrator
        # can reject book data that does not match the active spot feed.
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
        self._private_key = self._load_kalshi_key()
        self.multi_exchange = MultiExchangeReference()
        self._feed_runtime = {
            name: {"state": "STOPPED", "restart_count": 0, "last_error": None, "started_ts": None}
            for name in ("Spot Feed Worker", "Kalshi Feed Worker", "BRTI Feed Worker", "Multi-Exchange Worker")
        }
        self._last_brti_value = None
        self._last_brti_source_ts = None

    def _load_kalshi_key(self):
        """Load a Kalshi RSA or Ed25519 key from either supported secret source.

        Prefer KALSHI_PRIVATE_KEY when valid, but fall back to
        KALSHI_PRIVATE_KEY_PATH if the inline value is absent or malformed.
        This is important when a stale/placeholder inline secret coexists with
        a valid mounted PEM file.
        """
        if not settings.kalshi_key_id:
            self.brti_detail = "KALSHI_KEY_ID is not configured"
            return None

        candidates = []
        direct_value = settings.kalshi_private_key
        if isinstance(direct_value, str) and direct_value.strip():
            normalized = normalize_kalshi_private_key_text(direct_value)
            candidates.append(("KALSHI_PRIVATE_KEY", normalized.encode("utf-8")))

        key_path = settings.kalshi_private_key_path
        if isinstance(key_path, str) and key_path.strip():
            try:
                with open(key_path.strip(), "rb") as key_file:
                    candidates.append(("KALSHI_PRIVATE_KEY_PATH", key_file.read()))
            except Exception as exc:
                candidates.append((
                    "KALSHI_PRIVATE_KEY_PATH",
                    None,
                ))
                path_error = f"{type(exc).__name__}: {exc}"
            else:
                path_error = None
        else:
            path_error = None

        if not candidates:
            self.brti_detail = (
                "Kalshi private key is not configured; set KALSHI_PRIVATE_KEY "
                "or KALSHI_PRIVATE_KEY_PATH"
            )
            return None

        errors = []
        if path_error:
            errors.append(f"KALSHI_PRIVATE_KEY_PATH: {path_error}")

        for source, pem_data in candidates:
            if pem_data is None:
                continue
            try:
                if b"-----BEGIN OPENSSH PRIVATE KEY-----" in pem_data:
                    key = serialization.load_ssh_private_key(pem_data, password=None)
                else:
                    key = serialization.load_pem_private_key(pem_data, password=None)
                if not isinstance(key, (ed25519.Ed25519PrivateKey, rsa.RSAPrivateKey)):
                    raise TypeError("unsupported key type; expected Ed25519 or RSA")
                self.brti_detail = f"Kalshi private key loaded from {source}"
                return key
            except Exception as exc:
                # Do not include key material in diagnostics. Source and parser
                # error are sufficient to diagnose format/algorithm failures.
                raw_value = direct_value if source == "KALSHI_PRIVATE_KEY" else ""
                summary = (
                    _safe_key_input_summary(source, raw_value, normalized)
                    if raw_value else source
                )
                # Parser diagnostics disclose only structure, never key bytes.
                errors.append(f"{summary}: {type(exc).__name__}: {str(exc)[:180]}")

        self.brti_detail = "Kalshi private key could not be loaded: " + " | ".join(errors)
        return None

    def sign_kalshi(self, ts_ms: str, method: str, path: str) -> str:
        msg = f"{ts_ms}{method.upper()}{path.split('?')[0]}".encode("utf-8")
        if isinstance(self._private_key, ed25519.Ed25519PrivateKey):
            sig = self._private_key.sign(msg)
        else:
            sig = self._private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return base64.b64encode(sig).decode("utf-8")

    def ingest_trade(self, ts: float, price: float, qty: float, is_buyer_maker: bool, source: str) -> bool:
        """Accept only finite, positive, non-future, non-regressing market trades."""
        try:
            ts, price, qty = float(ts), float(price), float(qty)
        except (TypeError, ValueError, OverflowError):
            return False
        now = time.time()
        if (not all(math.isfinite(v) for v in (ts, price, qty)) or ts <= 0 or price <= 0 or qty <= 0
                or ts > now + 0.5 or not isinstance(is_buyer_maker, bool)):
            return False
        source = str(source or "UNKNOWN")
        # Permit a small exchange-clock offset when switching providers, but do
        # not let an older packet from the same source roll the live state back.
        if source == self.active_spot_source and self.last_spot_ts and ts < self.last_spot_ts:
            return False
        sec = int(ts)
        if sec < self._current_sec and source == self.active_spot_source:
            return False
        self.live_price = price
        self.last_spot_ts = ts
        self.active_spot_source = source
        self.trades.append((ts, price, qty, is_buyer_maker))
        if self._current_sec == 0:
            self._current_sec = sec
            self._current_sec_last_price = price
            self._current_sec_vol = qty
        elif sec > self._current_sec:
            # Close the prior second with its own final trade, not the first
            # trade from the new second.
            self.bars_1s.append((self._current_sec, self._current_sec_last_price, self._current_sec_vol))
            self._current_sec = sec
            self._current_sec_last_price = price
            self._current_sec_vol = qty
        elif sec == self._current_sec:
            self._current_sec_last_price = price
            self._current_sec_vol += qty
        else:
            # A newly selected fallback can have a slightly behind exchange
            # clock. Keep the valid trade/live source, but don't rewrite a bar
            # already closed by the prior source.
            return True
        return True

    def ingest_book(
        self,
        ts: float,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        source: Optional[str] = None,
    ) -> bool:
        try:
            ts = float(ts)
            parsed_bids = [(float(p), float(q)) for p, q in bids]
            parsed_asks = [(float(p), float(q)) for p, q in asks]
        except (TypeError, ValueError, OverflowError):
            return False
        now = time.time()
        if not math.isfinite(ts) or ts <= 0 or ts > now + 0.5 or not parsed_bids or not parsed_asks:
            return False
        if any(not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q <= 0 for p, q in parsed_bids + parsed_asks):
            return False
        bids_sorted = sorted(parsed_bids, key=lambda x: x[0], reverse=True)[:20]
        asks_sorted = sorted(parsed_asks, key=lambda x: x[0])[:20]
        if bids_sorted[0][0] >= asks_sorted[0][0]:
            return False
        # Preserve the older caller contract: when a book source is omitted,
        # associate it with the currently active spot venue.
        source = str(source or self.active_spot_source or "UNKNOWN")
        if source == self.last_book_source and self.last_book_ts and ts < self.last_book_ts:
            return False
        self.bids = bids_sorted
        self.asks = asks_sorted
        self.last_book_ts = ts
        self.last_book_source = source
        return True

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        workers = [
            ("Spot Feed Worker", self._spot_stream_loop),
            ("Kalshi Feed Worker", self._kalshi_market_loop),
        ]
        if self._private_key and settings.kalshi_key_id:
            workers.append(("BRTI Feed Worker", self._brti_poll_loop))
        workers.append(("Multi-Exchange Worker", self._run_multi_exchange_worker))
        self._tasks = [
            asyncio.create_task(self._run_supervised_feed(name, worker), name=name)
            for name, worker in workers
        ]

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        try:
            await self.multi_exchange.stop()
        except Exception:
            pass

    async def _run_multi_exchange_worker(self) -> None:
        await self.multi_exchange.start()
        try:
            while self._running:
                await asyncio.sleep(1.0)
        finally:
            await self.multi_exchange.stop()

    async def _run_supervised_feed(self, name: str, worker, retry_delay: float = 1.5) -> None:
        runtime = self._feed_runtime.setdefault(name, {"state": "STOPPED", "restart_count": 0, "last_error": None, "started_ts": None})
        while self._running:
            runtime["state"] = "RUNNING"
            runtime["started_ts"] = time.time()
            try:
                await worker()
                if self._running:
                    runtime["state"] = "RESTARTING"
                    runtime["last_error"] = "Worker returned unexpectedly"
                    runtime["restart_count"] += 1
                    await asyncio.sleep(retry_delay)
                else:
                    runtime["state"] = "STOPPED"
                    return
            except asyncio.CancelledError:
                runtime["state"] = "STOPPED"
                raise
            except Exception as exc:
                runtime["last_error"] = f"{type(exc).__name__}: {exc}"
                runtime["restart_count"] += 1
                runtime["state"] = "RESTARTING" if self._running else "STOPPED"
                if self._running:
                    await asyncio.sleep(retry_delay)
        runtime["state"] = "STOPPED"

    @staticmethod
    def _parse_exchange_timestamp(value, fallback: Optional[float] = None) -> Optional[float]:
        if value is None or value == "":
            return fallback
        try:
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().replace(".", "", 1).isdigit()):
                number = float(value)
                if not math.isfinite(number) or number <= 0:
                    return fallback
                return number / 1000.0 if number > 100_000_000_000 else number
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            result = parsed.timestamp()
            return result if math.isfinite(result) and result > 0 else fallback
        except (TypeError, ValueError, OverflowError):
            return fallback

    @staticmethod
    def _coinbase_is_buyer_maker(side):
        normalized = str(side or "").strip().lower()
        if normalized == "buy":
            return True
        if normalized == "sell":
            return False
        return None

    @staticmethod
    def _coinbase_ticker_book(msg):
        try:
            bid, ask = float(msg["best_bid"]), float(msg["best_ask"])
            bid_size, ask_size = float(msg["best_bid_size"]), float(msg["best_ask_size"])
            values = (bid, ask, bid_size, ask_size)
            if not all(math.isfinite(v) and v > 0 for v in values) or bid >= ask:
                return None
            return ([(bid, bid_size)], [(ask, ask_size)])
        except (KeyError, TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _parse_brti_value(value):
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) and number > 0 else None

    @classmethod
    def _parse_brti_payload(cls, payload, now: Optional[float] = None):
        now = time.time() if now is None else float(now)
        if not isinstance(payload, dict):
            return None
        data = payload.get("data", payload)
        if isinstance(data, dict) and "payload" in data:
            data = data["payload"]
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for item in reversed(rows):
            if not isinstance(item, dict) or item.get("repeatOfPreviousValue") is True:
                continue
            value = cls._parse_brti_value(item.get("value"))
            ts = cls._parse_exchange_timestamp(item.get("time", item.get("timestamp")))
            if value is None or ts is None or ts > now + 0.5 or now - ts > 10.0:
                continue
            return value, ts
        return None

    def get_multi_exchange_snapshot(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else float(now)
        snapshot = self.multi_exchange.get_snapshot(now)
        spot_age = safe_age_seconds(self.last_spot_ts, now)
        brti_age = safe_age_seconds(self.last_brti_ts, now)
        spot_fresh = spot_age is not None and spot_age <= settings.stale_feed_threshold_sec
        brti_fresh = (
            self.brti_status == "LIVE"
            and brti_age is not None
            and brti_age <= settings.stale_feed_threshold_sec
        )
        snapshot["primary_spot_price"] = self.live_price if spot_fresh else None
        snapshot["primary_spot_source"] = self.active_spot_source
        snapshot["primary_spot_age_seconds"] = round(spot_age, 3) if spot_age is not None else None
        snapshot["official_brti_price"] = self.official_brti_price if brti_fresh else None
        snapshot["official_brti_age_seconds"] = round(brti_age, 3) if brti_age is not None else None
        reference_price = snapshot.get("reference_price")
        primary_price = snapshot["primary_spot_price"]
        brti_price = snapshot["official_brti_price"]
        snapshot["reference_vs_primary_bps"] = (
            round((float(reference_price) - float(primary_price)) / float(primary_price) * 10000.0, 4)
            if reference_price is not None and primary_price is not None and primary_price > 0 else None
        )
        snapshot["reference_vs_brti_bps"] = (
            round((float(reference_price) - float(brti_price)) / float(brti_price) * 10000.0, 4)
            if reference_price is not None and brti_price is not None and brti_price > 0 else None
        )
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
                    # Never attribute a settlement response for a different
                    # market to the requested contract, even if the API returns
                    # a valid settled result.
                    if m.get("ticker") != ticker:
                        return None
                    if m.get("status") == "settled" and m.get("result") in ("yes", "no"):
                        return ("UP" if m["result"] == "yes" else "DOWN"), (float(m["expiration_value"]) if m.get("expiration_value") is not None else None)
        except Exception:
            return None
        return None

    async def _spot_stream_loop(self) -> None:
        use_coinbase = False
        next_primary_retry = 0.0
        while self._running:
            try:
                if not use_coinbase:
                    await self._run_binance_ws()
                    if self._running:
                        raise RuntimeError("Binance websocket closed")
                else:
                    await self._run_coinbase_ws()
                    if self._running:
                        raise RuntimeError("Coinbase websocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = str(exc)
                now = time.monotonic()
                if not use_coinbase:
                    # Any primary failure triggers fallback; once the fallback
                    # session ends, immediately retry primary on the next cycle.
                    use_coinbase = True
                    next_primary_retry = now
                else:
                    use_coinbase = False
                await asyncio.sleep(1.5)

    async def _run_binance_ws(self) -> None:
        async with websockets.connect(settings.binance_ws_url, ping_interval=20, open_timeout=8) as ws:
            async for raw in ws:
                if not self._running: break
                payload = json.loads(raw)
                stream, data, now = payload.get("stream", ""), payload.get("data", {}), time.time()
                if "aggTrade" in stream:
                    self.ingest_trade(self._parse_exchange_timestamp(data.get("T", data.get("E")), now), float(data["p"]), float(data["q"]), data.get("m") is True, "BINANCE_SPOT")
                elif "depth20" in stream:
                    self.ingest_book(self._parse_exchange_timestamp(data.get("E"), now), [(float(p), float(q)) for p, q in data.get("bids", [])], [(float(p), float(q)) for p, q in data.get("asks", [])], "BINANCE_SPOT")

    async def _run_coinbase_ws(self) -> None:
        sub = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "ticker"]}
        async with websockets.connect(settings.coinbase_ws_url, ping_interval=20, open_timeout=8) as ws:
            await ws.send(json.dumps(sub))
            async for raw in ws:
                if not self._running: break
                msg, now = json.loads(raw), time.time()
                if msg.get("type") == "match":
                    self.ingest_trade(self._parse_exchange_timestamp(msg.get("time"), now), float(msg["price"]), float(msg["size"]), self._coinbase_is_buyer_maker(msg.get("side")) is True, "COINBASE_FALLBACK")
                elif msg.get("type") == "ticker":
                    parsed_book = self._coinbase_ticker_book(msg)
                    if parsed_book:
                        self.ingest_book(self._parse_exchange_timestamp(msg.get("time"), now), parsed_book[0], parsed_book[1], "COINBASE_FALLBACK")

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
        params = {"series_ticker": "KXBTC15M", "status": "open", "limit": 100}
        request_timeout = aiohttp.ClientTimeout(
            total=8.0, connect=3.0, sock_connect=3.0, sock_read=5.0
        )
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    bases = self._kalshi_rest_base_urls(settings.kalshi_public_base_url)
                    attempt_details = []
                    response_received = False
                    for base_index, base_url in enumerate(bases):
                        url = f"{base_url}/markets"
                        host = base_url.split("://", 1)[-1].split("/", 1)[0]
                        started = asyncio.get_running_loop().time()
                        try:
                            async with session.get(
                                url, params=params, timeout=request_timeout
                            ) as resp:
                                elapsed = asyncio.get_running_loop().time() - started
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
                                        if not (open_ts <= now < close_ts):
                                            continue
                                        if (
                                            not math.isfinite(strike)
                                            or strike <= 0
                                            or not str(market.get("ticker", "")).startswith("KXBTC15M")
                                        ):
                                            continue
                                        candidates.append((close_ts, open_ts, strike, market))

                                    self.last_kalshi_ts = now
                                    if candidates:
                                        close_ts, open_ts, strike, market = min(
                                            candidates, key=lambda item: item[0]
                                        )
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
                                        self.kalshi_status_detail = (
                                            f"Active official contract: {market['ticker']} via {host}"
                                        )
                                    else:
                                        self.kalshi_contract = None
                                        self.kalshi_status_detail = (
                                            "No currently active KXBTC15M contract with an official strike"
                                        )
                                    response_received = True
                                    break

                                try:
                                    body = (await resp.text())[:200]
                                except Exception:
                                    body = ""
                                suffix = f": {body}" if body else ""
                                attempt_details.append(
                                    f"{host} HTTP {resp.status} after {elapsed:.2f}s{suffix}"
                                )
                                # Public market-data errors may be host-specific. Try
                                # the supported alternate host before marking it stale.
                                retryable = resp.status in (
                                    401, 403, 408, 429, 500, 502, 503, 504
                                )
                                if not retryable or base_index == len(bases) - 1:
                                    self.kalshi_status_detail = (
                                        "Kalshi market request failed: " + " | ".join(attempt_details)
                                    )
                                    response_received = True
                                    break
                        except asyncio.CancelledError:
                            raise
                        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                            elapsed = asyncio.get_running_loop().time() - started
                            detail = self._kalshi_request_exception_detail(exc, "Kalshi market")
                            attempt_details.append(
                                f"{host} after {elapsed:.2f}s: {detail}"
                            )
                            if base_index == len(bases) - 1:
                                self.kalshi_status_detail = (
                                    "Kalshi market request failed on all supported hosts: "
                                    + " | ".join(attempt_details)
                                )
                        except Exception as exc:
                            elapsed = asyncio.get_running_loop().time() - started
                            self.kalshi_status_detail = (
                                f"{host} after {elapsed:.2f}s: {self._kalshi_request_exception_detail(exc, "Kalshi market")}"
                            )
                            response_received = True
                            break
                    if not response_received and not attempt_details:
                        self.kalshi_status_detail = "Kalshi market request did not receive a response"
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.kalshi_status_detail = self._kalshi_request_exception_detail(exc, "Kalshi market")
                await asyncio.sleep(3.0)
    @staticmethod
    def _kalshi_rest_base_urls(configured_base_url: str) -> List[str]:
        """Return the configured production host plus Kalshi's supported compatibility host."""
        primary = str(configured_base_url or "").strip().rstrip("/")
        if not primary:
            primary = "https://external-api.kalshi.com/trade-api/v2"
        host = primary.split("://", 1)[-1].split("/", 1)[0].lower()
        alternates = {
            "external-api.kalshi.com": "https://api.elections.kalshi.com/trade-api/v2",
            "api.elections.kalshi.com": "https://external-api.kalshi.com/trade-api/v2",
        }
        alternate = alternates.get(host)
        return [primary, alternate] if alternate and alternate != primary else [primary]
    @staticmethod
    def _kalshi_request_exception_detail(exc: Exception, source: str) -> str:
        """Preserve timeout phase and exception type without exposing credentials."""
        exception_name = type(exc).__name__
        if exception_name == "ConnectionTimeoutError":
            phase = "connection establishment"
        elif exception_name == "SocketTimeoutError":
            phase = "socket read"
        elif isinstance(exc, asyncio.TimeoutError):
            phase = "overall request timeout or unclassified timeout"
        elif isinstance(exc, aiohttp.ClientConnectorError):
            phase = "DNS/TCP/TLS connection"
        else:
            phase = "request"
        message = str(exc).strip() or repr(exc)
        return f"{source} {phase} error ({exception_name}): {message[:240]}"

    @staticmethod
    def _brti_payload_shape_detail(payload) -> str:
        """Summarize response structure without logging index values or credentials."""
        if not isinstance(payload, dict):
            return f"BRTI HTTP 200 but unexpected response type: {type(payload).__name__}"
        data = payload.get("data", payload)
        inner = data.get("payload", data) if isinstance(data, dict) else data
        rows = inner if isinstance(inner, list) else ([inner] if isinstance(inner, dict) else [])
        first_keys = sorted(str(key) for key in rows[0].keys()) if rows and isinstance(rows[0], dict) else []
        outer_keys = sorted(str(key) for key in payload.keys())
        data_keys = sorted(str(key) for key in data.keys()) if isinstance(data, dict) else []
        return (
            "BRTI HTTP 200 but no fresh valid value; "
            f"outer_keys={outer_keys}, data_keys={data_keys}, "
            f"payload_type={type(inner).__name__}, rows={len(rows)}, "
            f"first_row_fields={first_keys}"
        )
    async def _brti_poll_loop(self) -> None:
        endpoint = "/trade-api/v2/cfbenchmarks/values"
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    ts_ms = str(int(time.time() * 1000))
                    headers = {
                        "KALSHI-ACCESS-KEY": settings.kalshi_key_id or "",
                        "KALSHI-ACCESS-SIGNATURE": self.sign_kalshi(ts_ms, "GET", endpoint),
                        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
                    }
                    bases = self._kalshi_rest_base_urls(settings.kalshi_prod_base_url)
                    request_timeout = aiohttp.ClientTimeout(
                        total=6.0, connect=2.5, sock_connect=2.5, sock_read=4.5
                    )
                    attempt_details = []
                    for base_index, base_url in enumerate(bases):
                        url = f"{base_url}/cfbenchmarks/values?id=BRTI"
                        host = base_url.split("://", 1)[-1].split("/", 1)[0]
                        try:
                            async with session.get(
                                url, headers=headers, timeout=request_timeout
                            ) as resp:
                                if resp.status == 200:
                                    payload = await resp.json()
                                    parsed = self._parse_brti_payload(payload)
                                    if parsed:
                                        value, source_ts = parsed
                                        if source_ts != self._last_brti_source_ts or value != self._last_brti_value:
                                            self.official_brti_price = value
                                            self.last_brti_ts = source_ts
                                            self.brti_status = "LIVE"
                                            self.brti_detail = f"Official CF Benchmarks BRTI via {host}"
                                            self._last_brti_source_ts, self._last_brti_value = source_ts, value
                                        else:
                                            self.brti_status = "STALE"
                                            self.brti_detail = "BRTI source value/timestamp has not advanced"
                                    else:
                                        self.brti_status = "UNAVAILABLE"
                                        self.brti_detail = self._brti_payload_shape_detail(payload)
                                    break
                                try:
                                    body = (await resp.text())[:240]
                                except Exception:
                                    body = ""
                                suffix = f": {body}" if body else ""
                                attempt_details.append(f"{host} HTTP {resp.status}{suffix}")
                                if resp.status in (401, 403):
                                    self.brti_status = "UNAVAILABLE"
                                    self.brti_detail = f"Kalshi CF Benchmarks HTTP {resp.status}: authentication or entitlement denied{suffix}"
                                    return
                                # A 503 can represent a CF upstream timeout; retry once on
                                # Kalshi's officially supported alternate production host.
                                if resp.status not in (408, 429, 500, 502, 503, 504) or base_index == len(bases) - 1:
                                    self.brti_status = "UNAVAILABLE"
                                    self.brti_detail = f"Kalshi CF Benchmarks HTTP {resp.status}{suffix}"
                                    break
                        except asyncio.CancelledError:
                            raise
                        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                            detail = self._kalshi_request_exception_detail(exc, "Kalshi market")
                            attempt_details.append(f"{host}: {detail}")
                            if base_index == len(bases) - 1:
                                self.brti_status = "UNAVAILABLE"
                                self.brti_detail = "BRTI request failed on all supported Kalshi hosts: " + " | ".join(attempt_details)
                        except Exception as exc:
                            self.brti_status = "UNAVAILABLE"
                            self.brti_detail = f"{host}: {self._kalshi_request_exception_detail(exc, "Kalshi market")}"
                            break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.brti_status = "UNAVAILABLE"
                    self.brti_detail = self._kalshi_request_exception_detail(exc, "Kalshi market")
                await asyncio.sleep(2.0)
    def get_feed_statuses(self) -> List[FeedStatus]:
        now = time.time()
        def status_for(ts):
            if not ts:
                return "UNAVAILABLE", None
            age = now - ts
            if age < -0.5:
                return "FUTURE_TIMESTAMP", age
            if age > settings.stale_feed_threshold_sec:
                return "STALE", age
            return "LIVE", age

        spot_status, spot_age = status_for(self.last_spot_ts)
        if spot_status == "LIVE" and self.active_spot_source != "BINANCE_SPOT":
            spot_status = "FALLBACK"
        book_status, book_age = status_for(self.last_book_ts)
        if book_status in ("LIVE", "FALLBACK") and self.active_spot_source and self.last_book_source != self.active_spot_source:
            book_status = "SOURCE_MISMATCH"
        kalshi_status, kalshi_age = status_for(self.last_kalshi_ts)
        if kalshi_status == "LIVE" and (self.kalshi_contract is None or self.kalshi_contract.close_timestamp <= now):
            kalshi_status = "UNAVAILABLE"
        brti_status, brti_age = status_for(self.last_brti_ts)
        if self.brti_status == "UNAVAILABLE" or self.official_brti_price is None or not self.last_brti_ts:
            brti_status = "UNAVAILABLE"
        elif brti_status == "LIVE" and self.brti_status != "LIVE":
            brti_status = self.brti_status

        statuses = [
            FeedStatus(name="Primary Spot Feed", status=spot_status, last_updated_iso=iso_now(self.last_spot_ts) if self.last_spot_ts > 0 else None, age_seconds=round(spot_age, 2) if spot_age is not None else None, detail=self.active_spot_source),
            FeedStatus(name="Order Book Depth", status=book_status, last_updated_iso=iso_now(self.last_book_ts) if self.last_book_ts > 0 else None, age_seconds=round(book_age, 2) if book_age is not None else None, detail=f"{len(self.bids)} bid / {len(self.asks)} ask levels; source={self.last_book_source}"),
            FeedStatus(name="Kalshi KXBTC15M", status=kalshi_status, last_updated_iso=iso_now(self.last_kalshi_ts) if self.last_kalshi_ts > 0 else None, age_seconds=round(kalshi_age, 2) if kalshi_age is not None else None, detail=self.kalshi_status_detail),
            FeedStatus(name="CF Benchmarks BRTI", status=brti_status, last_updated_iso=iso_now(self.last_brti_ts) if self.last_brti_ts > 0 else None, age_seconds=round(brti_age, 2) if brti_age is not None else None, detail=self.brti_detail),
        ]
        for name, runtime in self._feed_runtime.items():
            if runtime.get("restart_count", 0) or runtime.get("state") not in (None, "STOPPED"):
                detail = f"state={runtime.get('state')}; restarts={runtime.get('restart_count', 0)}"
                if runtime.get("last_error"):
                    detail += f"; last_error={runtime['last_error']}"
                age = safe_age_seconds(runtime.get("started_ts"), now)
                statuses.append(FeedStatus(name=name, status=runtime.get("state", "STOPPED"), last_updated_iso=iso_now(runtime["started_ts"]) if runtime.get("started_ts") else None, age_seconds=round(age, 2) if age is not None else None, detail=detail))
        return statuses
