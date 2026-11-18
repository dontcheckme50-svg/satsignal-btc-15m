import sqlite3, json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from app.config import settings
from app.models import MarketRegime, PredictionRecord, Scoreboard, Signal, TradeRecord

class PredictionLedger:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    @contextmanager
    def _connection(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_ticker TEXT NOT NULL,
                    timestamp_iso TEXT NOT NULL,
                    epoch_ts REAL NOT NULL,
                    strike_price REAL NOT NULL,
                    btc_price_at_pred REAL NOT NULL,
                    seconds_remaining REAL NOT NULL,
                    main_signal TEXT NOT NULL,
                    early_signal TEXT NOT NULL,
                    final_signal TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    up_prob REAL NOT NULL,
                    down_prob REAL NOT NULL,
                    regime TEXT NOT NULL,
                    settled INTEGER NOT NULL DEFAULT 0,
                    settlement_source TEXT,
                    settlement_price REAL,
                    actual_outcome TEXT,
                    final_won INTEGER,
                    main_won INTEGER,
                    early_won INTEGER,
                    is_official_lock INTEGER NOT NULL DEFAULT 0,
                    brti_cohort TEXT NOT NULL DEFAULT 'UNKNOWN',
                    brti_side_at_lock TEXT,
                    spot_side_at_lock TEXT
                );
                CREATE TABLE IF NOT EXISTS prediction_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_ticker TEXT NOT NULL,
                    epoch_ts REAL NOT NULL,
                    seconds_remaining REAL NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT NOT NULL,
                    contract_ticker TEXT NOT NULL,
                    timestamp_iso TEXT NOT NULL,
                    side TEXT NOT NULL,
                    contracts INTEGER NOT NULL,
                    entry_price REAL NOT NULL,
                    quote_source TEXT NOT NULL DEFAULT 'KALSHI_LIVE_BOOK',
                    fee_paid REAL NOT NULL,
                    fair_prob_at_entry REAL NOT NULL,
                    net_edge REAL NOT NULL,
                    status TEXT NOT NULL,
                    pnl_usd REAL NOT NULL DEFAULT 0.0,
                    notes TEXT NOT NULL DEFAULT '',
                    settled_at TEXT
                );
                CREATE TABLE IF NOT EXISTS multi_exchange_reference (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoch_ts REAL NOT NULL,
                    contract_ticker TEXT NOT NULL,
                    strike_price REAL,
                    primary_spot_price REAL,
                    primary_spot_age_seconds REAL,
                    official_brti_price REAL,
                    official_brti_age_seconds REAL,
                    reference_price REAL,
                    reference_status TEXT NOT NULL,
                    usd_venue_count INTEGER NOT NULL DEFAULT 0,
                    included_venues_json TEXT NOT NULL DEFAULT '[]',
                    outlier_venues_json TEXT NOT NULL DEFAULT '[]',
                    venues_json TEXT NOT NULL DEFAULT '[]',
                    bid_depth_25bps REAL,
                    ask_depth_25bps REAL,
                    imbalance REAL,
                    ref_vs_primary_bps REAL,
                    ref_vs_brti_bps REAL,
                    primary_vs_brti_bps REAL,
                    reference_vs_strike_bps REAL
                );
                CREATE INDEX IF NOT EXISTS idx_pred_ticker ON predictions(contract_ticker);
                CREATE INDEX IF NOT EXISTS idx_pred_settled ON predictions(settled);
                CREATE INDEX IF NOT EXISTS idx_observation_contract_epoch ON prediction_observations(contract_ticker, epoch_ts);
                CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(contract_ticker);
                CREATE INDEX IF NOT EXISTS idx_multi_ref_epoch ON multi_exchange_reference(epoch_ts);
                CREATE INDEX IF NOT EXISTS idx_multi_ref_contract ON multi_exchange_reference(contract_ticker);
            """)
            prediction_columns = {row["name"] for row in conn.execute("PRAGMA table_info(predictions)").fetchall()}
            if "brti_cohort" not in prediction_columns:
                conn.execute("ALTER TABLE predictions ADD COLUMN brti_cohort TEXT NOT NULL DEFAULT 'UNKNOWN'")
            if "brti_side_at_lock" not in prediction_columns:
                conn.execute("ALTER TABLE predictions ADD COLUMN brti_side_at_lock TEXT")
            if "spot_side_at_lock" not in prediction_columns:
                conn.execute("ALTER TABLE predictions ADD COLUMN spot_side_at_lock TEXT")
            if "is_official_lock" not in prediction_columns:
                conn.execute("ALTER TABLE predictions ADD COLUMN is_official_lock INTEGER NOT NULL DEFAULT 0")
                # Keep all historical snapshots, but designate only the latest
                # existing record for each contract as the legacy scoreboard representative.
                conn.execute("""
                    UPDATE predictions SET is_official_lock = 1
                    WHERE id IN (
                        SELECT MAX(id) FROM predictions GROUP BY contract_ticker
                    )
                """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_one_official_prediction_per_contract
                ON predictions(contract_ticker) WHERE is_official_lock = 1
            """)
            trade_columns = {row["name"] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
            if "settled_at" not in trade_columns:
                conn.execute("ALTER TABLE trades ADD COLUMN settled_at TEXT")
            reference_columns = {row["name"] for row in conn.execute("PRAGMA table_info(multi_exchange_reference)").fetchall()}
            if "primary_spot_age_seconds" not in reference_columns:
                conn.execute("ALTER TABLE multi_exchange_reference ADD COLUMN primary_spot_age_seconds REAL")
            if "official_brti_age_seconds" not in reference_columns:
                conn.execute("ALTER TABLE multi_exchange_reference ADD COLUMN official_brti_age_seconds REAL")
            if "reference_vs_strike_bps" not in reference_columns:
                conn.execute("ALTER TABLE multi_exchange_reference ADD COLUMN reference_vs_strike_bps REAL")


    def record_multi_exchange_reference(self, epoch_ts: float, contract_ticker: str, strike_price: Optional[float], primary_spot_price: Optional[float], official_brti_price: Optional[float], snapshot: dict) -> int:
        reference_price = snapshot.get("reference_price")
        def bps(a, b):
            return round((float(a) - float(b)) / float(b) * 10000.0, 4) if a is not None and b is not None and float(b) > 0 else None
        venue_details = [
            {
                "venue": v.get("venue"), "quote": v.get("quote"), "status": v.get("status"),
                "mid": v.get("mid"), "spread_bps": v.get("spread_bps"),
                "imbalance": v.get("imbalance"), "bid_depth_25bps": v.get("bid_depth_25bps"),
                "ask_depth_25bps": v.get("ask_depth_25bps"), "age_seconds": v.get("age_seconds"),
                "trade_flow": v.get("trade_flow"),
            }
            for v in snapshot.get("venues", [])
        ]
        with self._connection() as conn:
            cur = conn.execute(
                """INSERT INTO multi_exchange_reference (
                    epoch_ts, contract_ticker, strike_price, primary_spot_price, primary_spot_age_seconds,
                    official_brti_price, official_brti_age_seconds, reference_price, reference_status, usd_venue_count, included_venues_json,
                    outlier_venues_json, venues_json, bid_depth_25bps, ask_depth_25bps, imbalance,
                    ref_vs_primary_bps, ref_vs_brti_bps, primary_vs_brti_bps, reference_vs_strike_bps
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    epoch_ts, contract_ticker, strike_price, primary_spot_price, snapshot.get("primary_spot_age_seconds"),
                    official_brti_price, snapshot.get("official_brti_age_seconds"), reference_price,
                    snapshot.get("status", "UNAVAILABLE"), int(snapshot.get("usd_venue_count_live", 0)),
                    json.dumps(snapshot.get("included_venues", [])), json.dumps(snapshot.get("outlier_venues", [])),
                    json.dumps(venue_details), snapshot.get("consolidated_bid_depth_usd_within_25bps"),
                    snapshot.get("consolidated_ask_depth_usd_within_25bps"), snapshot.get("consolidated_imbalance"),
                    bps(reference_price, primary_spot_price), bps(reference_price, official_brti_price),
                    bps(primary_spot_price, official_brti_price), bps(reference_price, strike_price),
                ),
            )
            return int(cur.lastrowid)

    def get_multi_exchange_reference_history(self, limit: int = 200) -> List[dict]:
        limit = max(1, min(int(limit), 5000))
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM multi_exchange_reference ORDER BY epoch_ts DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["included_venues"] = json.loads(item.pop("included_venues_json") or "[]")
            item["outlier_venues"] = json.loads(item.pop("outlier_venues_json") or "[]")
            item["venues"] = json.loads(item.pop("venues_json") or "[]")
            result.append(item)
        return result

    def record_prediction(self, rec: PredictionRecord) -> int:
        """Persist the one official scored prediction for a contract, idempotently."""
        with self._connection() as conn:
            cur = conn.execute(
                """INSERT OR IGNORE INTO predictions (
                    contract_ticker, timestamp_iso, epoch_ts, strike_price,
                    btc_price_at_pred, seconds_remaining, main_signal, early_signal,
                    final_signal, confidence, up_prob, down_prob, regime, is_official_lock,
                    brti_cohort, brti_side_at_lock, spot_side_at_lock
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (rec.contract_ticker, rec.timestamp_iso, rec.epoch_ts, rec.strike_price,
                 rec.btc_price_at_pred, rec.seconds_remaining, rec.main_signal.value,
                 rec.early_signal.value, rec.final_signal.value, rec.confidence,
                 rec.up_prob, rec.down_prob, rec.regime.value, rec.brti_cohort,
                 rec.brti_side_at_lock, rec.spot_side_at_lock),
            )
            if cur.rowcount:
                return int(cur.lastrowid)
            existing = conn.execute(
                "SELECT id FROM predictions WHERE contract_ticker = ? AND is_official_lock = 1",
                (rec.contract_ticker,),
            ).fetchone()
            return int(existing["id"]) if existing else 0


    def get_brti_cohort_performance(self) -> List[dict]:
        """Return settled directional lock accuracy grouped by BRTI/spot alignment at lock time."""
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT brti_cohort, COUNT(*) AS settled,
                   SUM(CASE WHEN final_won = 1 THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN final_won = 0 THEN 1 ELSE 0 END) AS losses
                   FROM predictions
                   WHERE is_official_lock = 1 AND settled = 1
                     AND final_signal IN ('UP', 'DOWN') AND final_won IS NOT NULL
                   GROUP BY brti_cohort ORDER BY brti_cohort"""
            ).fetchall()
        return [
            {"cohort": r["brti_cohort"], "settled": int(r["settled"]),
             "wins": int(r["wins"] or 0), "losses": int(r["losses"] or 0),
             "accuracy_pct": round((int(r["wins"] or 0) / int(r["settled"])) * 100.0, 2)
                 if int(r["settled"]) else None}
            for r in rows
        ]

    def has_official_prediction_lock(self, contract_ticker: str) -> bool:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM predictions WHERE contract_ticker = ? AND is_official_lock = 1 LIMIT 1",
                (contract_ticker,),
            ).fetchone()
            return row is not None

    def record_prediction_observation(self, contract_ticker: str, epoch_ts: float, seconds_remaining: float, payload: dict) -> int:
        with self._connection() as conn:
            cur = conn.execute(
                "INSERT INTO prediction_observations (contract_ticker, epoch_ts, seconds_remaining, payload_json) VALUES (?, ?, ?, ?)",
                (contract_ticker, epoch_ts, seconds_remaining, json.dumps(payload, default=lambda value: getattr(value, "value", str(value)))),
            )
            return int(cur.lastrowid)

    def get_recent_prediction_observations(self, limit: int = 100, contract_ticker: Optional[str] = None) -> List[dict]:
        with self._connection() as conn:
            if contract_ticker:
                rows = conn.execute(
                    "SELECT * FROM prediction_observations WHERE contract_ticker = ? ORDER BY epoch_ts DESC LIMIT ?",
                    (contract_ticker, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM prediction_observations ORDER BY epoch_ts DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json") or "{}")
                result.append(item)
            return result

    def record_trade(self, tr: TradeRecord) -> int:
        with self._connection() as conn:
            cur = conn.execute(
                """INSERT INTO trades (
                    mode, contract_ticker, timestamp_iso, side, contracts,
                    entry_price, quote_source, fee_paid, fair_prob_at_entry,
                    net_edge, status, pnl_usd, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (tr.mode, tr.contract_ticker, tr.timestamp_iso, tr.side, tr.contracts,
                 tr.entry_price, tr.quote_source, tr.fee_paid, tr.fair_prob_at_entry,
                 tr.net_edge, tr.status, tr.pnl_usd, tr.notes),
            )
            return int(cur.lastrowid)

    def get_unsettled_contract_tickers(self) -> List[str]:
        # Only official contracts have an authoritative settlement source.
        # Synthetic proxy contracts must never enter the official settlement queue.
        with self._connection() as conn:
            rows = conn.execute(
                """SELECT contract_ticker, MIN(epoch_ts) AS first_prediction_ts
                   FROM predictions
                   WHERE settled = 0 AND is_official_lock = 1 AND contract_ticker LIKE 'KXBTC15M%'
                   GROUP BY contract_ticker
                   ORDER BY first_prediction_ts ASC"""
            ).fetchall()
            return [r["contract_ticker"] for r in rows]

    def count_trades_for_ticker(self, contract_ticker: str) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM trades WHERE contract_ticker = ?", (contract_ticker,)).fetchone()
            return int(row["c"])

    def count_open_contracts_for_ticker(self, contract_ticker: str) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COALESCE(SUM(contracts), 0) AS c FROM trades WHERE contract_ticker = ? AND status = 'OPEN'", (contract_ticker,)).fetchone()
            return int(row["c"])

    def settle_contract(self, contract_ticker: str, settlement_price: float, official_result: Optional[str] = None, settlement_source: str = "SPOT_CLOSE_PROXY") -> int:
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM predictions WHERE contract_ticker = ? AND is_official_lock = 1 AND settled = 0", (contract_ticker,)).fetchall()
            updated = 0
            for r in rows:
                strike = float(r["strike_price"])
                outcome = official_result if official_result in ("UP", "DOWN") else ("UP" if settlement_price >= strike else "DOWN")
                def grade(sig: str) -> Optional[int]:
                    return None if sig == "WAIT" else (1 if sig == outcome else 0)
                conn.execute(
                    """UPDATE predictions SET settled = 1, settlement_source = ?, settlement_price = ?,
                       actual_outcome = ?, final_won = ?, main_won = ?, early_won = ? WHERE id = ? AND settled = 0""",
                    (settlement_source, settlement_price, outcome, grade(r["final_signal"]), grade(r["main_signal"]), grade(r["early_signal"]), r["id"]),
                )
                updated += 1

            open_trades = conn.execute("SELECT * FROM trades WHERE contract_ticker = ? AND status = 'OPEN'", (contract_ticker,)).fetchall()
            for tr in open_trades:
                pred_row = conn.execute("SELECT strike_price, actual_outcome FROM predictions WHERE contract_ticker = ? AND is_official_lock = 1 LIMIT 1", (contract_ticker,)).fetchone()
                if pred_row and pred_row["actual_outcome"] in ("UP", "DOWN"):
                    yes_won = pred_row["actual_outcome"] == "UP"
                elif official_result in ("UP", "DOWN"):
                    yes_won = official_result == "UP"
                else:
                    strike = float(pred_row["strike_price"]) if pred_row else settlement_price
                    yes_won = settlement_price >= strike
                won = (tr["side"] == "YES" and yes_won) or (tr["side"] == "NO" and not yes_won)
                payout = float(tr["contracts"]) * 1.0 if won else 0.0
                cost = (float(tr["contracts"]) * float(tr["entry_price"])) + float(tr["fee_paid"])
                settled_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                conn.execute("UPDATE trades SET status = 'SETTLED', pnl_usd = ?, settled_at = ? WHERE id = ?", (round(payout - cost, 4), settled_at, tr["id"]))
            return updated

    def get_today_realized_pnl(self) -> float:
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with self._connection() as conn:
            row = conn.execute("SELECT COALESCE(SUM(pnl_usd), 0.0) AS p FROM trades WHERE status = 'SETTLED' AND settled_at LIKE ?", (f"{today_prefix}%",)).fetchone()
            return float(row["p"])

    def get_recent_predictions(self, limit: int = 25) -> List[PredictionRecord]:
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM predictions WHERE is_official_lock = 1 ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [
                PredictionRecord(
                    id=r["id"], is_official_lock=bool(r["is_official_lock"]), contract_ticker=r["contract_ticker"], timestamp_iso=r["timestamp_iso"],
                    epoch_ts=r["epoch_ts"], strike_price=r["strike_price"], btc_price_at_pred=r["btc_price_at_pred"],
                    seconds_remaining=r["seconds_remaining"], main_signal=Signal(r["main_signal"]),
                    early_signal=Signal(r["early_signal"]), final_signal=Signal(r["final_signal"]),
                    confidence=r["confidence"], up_prob=r["up_prob"], down_prob=r["down_prob"],
                    regime=MarketRegime(r["regime"]), brti_cohort=r["brti_cohort"],
                    brti_side_at_lock=r["brti_side_at_lock"], spot_side_at_lock=r["spot_side_at_lock"],
                    settled=bool(r["settled"]), settlement_source=r["settlement_source"],
                    settlement_price=r["settlement_price"], actual_outcome=r["actual_outcome"],
                    final_won=None if r["final_won"] is None else bool(r["final_won"]),
                    main_won=None if r["main_won"] is None else bool(r["main_won"]),
                    early_won=None if r["early_won"] is None else bool(r["early_won"]),
                ) for r in rows
            ]

    def get_recent_trades(self, limit: int = 20) -> List[TradeRecord]:
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [
                TradeRecord(
                    id=r["id"], mode=r["mode"], contract_ticker=r["contract_ticker"], timestamp_iso=r["timestamp_iso"],
                    side=r["side"], contracts=r["contracts"], entry_price=r["entry_price"], quote_source=r["quote_source"],
                    fee_paid=r["fee_paid"], fair_prob_at_entry=r["fair_prob_at_entry"], net_edge=r["net_edge"],
                    status=r["status"], pnl_usd=r["pnl_usd"], notes=r["notes"],
                ) for r in rows
            ]

    def get_scoreboard(self) -> Scoreboard:
        with self._connection() as conn:
            totals = conn.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN settled = 0 THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN final_signal = 'WAIT' THEN 1 ELSE 0 END) AS waits FROM predictions WHERE is_official_lock = 1").fetchone()
            settled_rows = conn.execute("SELECT up_prob, actual_outcome, final_won, main_won, early_won FROM predictions WHERE settled = 1 AND is_official_lock = 1").fetchall()
            def calc_acc(col: str) -> tuple[int, int, int, float]:
                # WAIT signals are deliberately excluded from directional accuracy.
                vals = [r[col] for r in settled_rows if r[col] is not None]
                if not vals: return 0, 0, 0, 0.0
                wins = sum(int(v) for v in vals)
                losses = len(vals) - wins
                return len(vals), wins, losses, round((wins / len(vals)) * 100.0, 2)
            f_n, f_wins, f_losses, f_acc = calc_acc("final_won")
            m_n, m_wins, m_losses, m_acc = calc_acc("main_won")
            e_n, e_wins, e_losses, e_acc = calc_acc("early_won")
            brier_vals = [(float(r["up_prob"]) - (1.0 if r["actual_outcome"] == "UP" else 0.0)) ** 2 for r in settled_rows if r["actual_outcome"] in ("UP", "DOWN")]
            brier = round(sum(brier_vals) / len(brier_vals), 4) if brier_vals else None
            trade_stats = conn.execute("SELECT COUNT(*) AS cnt, COALESCE(SUM(pnl_usd), 0.0) AS pnl FROM trades WHERE status != 'REJECTED'").fetchone()
            realized_pnl = round(float(trade_stats["pnl"]), 2)
            today_pnl = self.get_today_realized_pnl()
            return Scoreboard(
                total_predictions=int(totals["total"] or 0), pending_predictions=int(totals["pending"] or 0),
                total_settled=f_n, final_scored=f_n, final_wins=f_wins, final_losses=f_losses,
                final_accuracy_pct=f_acc, main_scored=m_n, main_wins=m_wins, main_losses=m_losses,
                main_accuracy_pct=m_acc, early_scored=e_n, early_wins=e_wins, early_losses=e_losses,
                early_accuracy_pct=e_acc,
                brier_score=brier, waits_issued=int(totals["waits"] or 0), trading_mode=settings.trading_mode,
                bankroll_usd=round(settings.paper_bankroll_usd + realized_pnl, 2),
                realized_pnl_usd=realized_pnl, total_trades=int(trade_stats["cnt"]),
                daily_loss_locked=(today_pnl <= -abs(settings.max_daily_loss_usd)),
            )

    def get_engine_weights(self) -> Dict[str, float]:
        sb = self.get_scoreboard()
        if sb.total_settled < 15:
            return {"evidence": 1.0, "early": 1.0}
        return {"evidence": round(max(0.6, min(1.4, sb.main_accuracy_pct / 52.0)), 3),
                "early": round(max(0.6, min(1.4, sb.early_accuracy_pct / 52.0)), 3)}
