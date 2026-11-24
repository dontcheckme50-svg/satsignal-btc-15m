from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from app.config import settings
from app.models import MarketRegime, PredictionRecord, Scoreboard, Signal, TradeRecord

class PredictionLedger:
    """Persistent ledger backed exclusively by the configured Supabase PostgreSQL database."""

    def __init__(self, db_path: str | None = None):
        self.database_url = settings.database_url
        if not self.database_url:
            raise RuntimeError(
                "DATABASE_URL must point to the Supabase PostgreSQL database; "
                "SATSignal will not fall back to local or in-memory storage."
            )
        self.db_path = None
        self._init_db()

    def _connect(self):
        from app.postgres_compat import connect
        return connect(self.database_url)

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
                    epoch_ts DOUBLE PRECISION NOT NULL,
                    strike_price DOUBLE PRECISION NOT NULL,
                    btc_price_at_pred DOUBLE PRECISION NOT NULL,
                    seconds_remaining DOUBLE PRECISION NOT NULL,
                    main_signal TEXT NOT NULL,
                    early_signal TEXT NOT NULL,
                    final_signal TEXT NOT NULL,
                    confidence DOUBLE PRECISION NOT NULL,
                    up_prob DOUBLE PRECISION NOT NULL,
                    down_prob DOUBLE PRECISION NOT NULL,
                    regime TEXT NOT NULL,
                    settled INTEGER NOT NULL DEFAULT 0,
                    settlement_source TEXT,
                    settlement_price DOUBLE PRECISION,
                    actual_outcome TEXT,
                    final_won INTEGER,
                    main_won INTEGER,
                    early_won INTEGER
                );
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode TEXT NOT NULL,
                    contract_ticker TEXT NOT NULL,
                    timestamp_iso TEXT NOT NULL,
                    side TEXT NOT NULL,
                    contracts INTEGER NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    quote_source TEXT NOT NULL DEFAULT 'KALSHI_LIVE_BOOK',
                    fee_paid DOUBLE PRECISION NOT NULL,
                    fair_prob_at_entry DOUBLE PRECISION NOT NULL,
                    net_edge DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    pnl_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    notes TEXT NOT NULL DEFAULT '',
                    settled_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_pred_ticker ON predictions(contract_ticker);
                CREATE INDEX IF NOT EXISTS idx_pred_settled ON predictions(settled);
                CREATE INDEX IF NOT EXISTS idx_trades_ticker ON trades(contract_ticker);
            """)
            trade_columns = {row["name"] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
            if "settled_at" not in trade_columns:
                conn.execute("ALTER TABLE trades ADD COLUMN settled_at TEXT")

    def record_prediction(self, rec: PredictionRecord) -> int:
        with self._connection() as conn:
            cur = conn.execute(
                """INSERT INTO predictions (
                    contract_ticker, timestamp_iso, epoch_ts, strike_price,
                    btc_price_at_pred, seconds_remaining, main_signal, early_signal,
                    final_signal, confidence, up_prob, down_prob, regime
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (rec.contract_ticker, rec.timestamp_iso, rec.epoch_ts, rec.strike_price,
                 rec.btc_price_at_pred, rec.seconds_remaining, rec.main_signal.value,
                 rec.early_signal.value, rec.final_signal.value, rec.confidence,
                 rec.up_prob, rec.down_prob, rec.regime.value),
            )
            return int(cur.lastrowid)

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
        with self._connection() as conn:
            rows = conn.execute("SELECT DISTINCT contract_ticker FROM predictions WHERE settled = 0").fetchall()
            return [r["contract_ticker"] for r in rows]

    def count_open_contracts_for_ticker(self, contract_ticker: str) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COALESCE(SUM(contracts), 0) AS c FROM trades WHERE contract_ticker = ? AND status = 'OPEN'", (contract_ticker,)).fetchone()
            return int(row["c"])

    def settle_contract(self, contract_ticker: str, settlement_price: float, official_result: Optional[str] = None, settlement_source: str = "SPOT_CLOSE_PROXY") -> int:
        with self._connection() as conn:
            rows = conn.execute("SELECT * FROM predictions WHERE contract_ticker = ? AND settled = 0", (contract_ticker,)).fetchall()
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
                pred_row = conn.execute("SELECT strike_price, actual_outcome FROM predictions WHERE contract_ticker = ? LIMIT 1", (contract_ticker,)).fetchone()
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
            rows = conn.execute("SELECT * FROM predictions ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [
                PredictionRecord(
                    id=r["id"], contract_ticker=r["contract_ticker"], timestamp_iso=r["timestamp_iso"],
                    epoch_ts=r["epoch_ts"], strike_price=r["strike_price"], btc_price_at_pred=r["btc_price_at_pred"],
                    seconds_remaining=r["seconds_remaining"], main_signal=Signal(r["main_signal"]),
                    early_signal=Signal(r["early_signal"]), final_signal=Signal(r["final_signal"]),
                    confidence=r["confidence"], up_prob=r["up_prob"], down_prob=r["down_prob"],
                    regime=MarketRegime(r["regime"]), settled=bool(r["settled"]), settlement_source=r["settlement_source"],
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
            totals = conn.execute("SELECT COUNT(*) AS total, SUM(CASE WHEN settled = 0 THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN final_signal = 'WAIT' THEN 1 ELSE 0 END) AS waits FROM predictions").fetchone()
            settled_rows = conn.execute("SELECT up_prob, actual_outcome, final_won, main_won, early_won FROM predictions WHERE settled = 1").fetchall()
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
