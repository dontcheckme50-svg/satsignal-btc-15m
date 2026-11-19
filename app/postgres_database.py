"""Opt-in PostgreSQL backend for the existing SATSIGNAL prediction ledger.

This backend is selected explicitly with SATSIGNAL_STORAGE_BACKEND=postgres.
The SQLite PredictionLedger remains the default and is not modified.
"""
from __future__ import annotations

import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.database import PredictionLedger
from app.postgres_schema import apply_postgres_schema


class _CursorCompat:
    def __init__(self, cursor):
        self._cursor = cursor
        self.lastrowid = None

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is not None and "id" in row:
            self.lastrowid = row["id"]
        return row

    def fetchall(self):
        return self._cursor.fetchall()


class _ConnectionCompat:
    """Small sqlite-style surface for the inherited, parameterized ledger SQL."""
    def __init__(self, connection):
        self._connection = connection

    @staticmethod
    def _sql(sql: str) -> tuple[str, bool]:
        statement = sql.strip()
        ignored_insert = bool(re.match(r"INSERT\s+OR\s+IGNORE\s+INTO", statement, re.I))
        if ignored_insert:
            statement = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", statement, count=1, flags=re.I)
            statement = statement.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING RETURNING id"
        elif re.match(r"INSERT\s+INTO", statement, re.I) and "RETURNING" not in statement.upper():
            statement = statement.rstrip().rstrip(";") + " RETURNING id"
        statement = statement.replace("?", "%s")
        return statement, ignored_insert

    def execute(self, sql: str, params=()):
        statement, _ = self._sql(sql)
        cur = self._connection.execute(statement, params or ())
        wrapped = _CursorCompat(cur)
        if "RETURNING id" in statement.upper():
            row = cur.fetchone()
            wrapped.lastrowid = row["id"] if row else None
            # Preserve rowcount and expose the consumed RETURNING row as fetchone().
            wrapped._first = row
        else:
            wrapped._first = None
        return wrapped

    def cursor(self):
        return self._connection.cursor()

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()


class PostgresPredictionLedger(PredictionLedger):
    """PostgreSQL implementation with the same public ledger API."""

    def __init__(self, database_url: str):
        if not database_url:
            raise ValueError("SUPABASE_DATABASE_URL is required for PostgreSQL storage")
        self.database_url = database_url
        self.db_path = "postgresql"
        self._init_db()

    def _connect(self):
        return _ConnectionCompat(psycopg.connect(
            self.database_url,
            connect_timeout=10,
            sslmode="require",
            row_factory=dict_row,
            application_name="satsignal-ledger",
        ))

    @contextmanager
    def _connection(self, immediate: bool = False):
        conn = self._connect()
        try:
            if immediate:
                conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        # DDL is transactional and only runs when this backend is explicitly selected.
        with self._connection() as conn:
            apply_postgres_schema(conn._connection)

    def get_storage_health(self) -> dict:
        try:
            with self._connection() as conn:
                row = conn.execute("SELECT 1 AS ok").fetchone()
                counts = {}
                for table in ("predictions", "prediction_observations", "trades", "multi_exchange_reference"):
                    result = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                    counts[table] = int(result["count"])
                size = conn.execute("SELECT pg_database_size(current_database()) AS bytes").fetchone()
            return {
                "status": "HEALTHY" if row and row["ok"] == 1 else "DEGRADED",
                "storage_type": "POSTGRESQL",
                "database_size_bytes": int(size["bytes"]),
                "record_counts": counts,
            }
        except Exception as exc:
            return {"status": "UNAVAILABLE", "storage_type": "POSTGRESQL", "error_type": type(exc).__name__}

    def get_unsettled_contract_tickers(self) -> list[str]:
        # Timestamp strings are stored in the existing UTC text format.
        with self._connection() as conn:
            rows = conn.execute(
                """WITH pending AS (
                       SELECT contract_ticker, MIN(epoch_ts) AS first_seen
                       FROM predictions
                       WHERE settled = 0 AND is_official_lock = 1
                         AND contract_ticker LIKE 'KXBTC15M%'
                       GROUP BY contract_ticker
                       UNION ALL
                       SELECT contract_ticker,
                              COALESCE(EXTRACT(EPOCH FROM MIN(to_timestamp(
                                  NULLIF(substring(timestamp_iso from 1 for 19), '') ,
                                  'YYYY-MM-DD HH24:MI:SS'
                              ))), 0.0) AS first_seen
                       FROM trades
                       WHERE status = 'OPEN' AND contract_ticker LIKE 'KXBTC15M%'
                       GROUP BY contract_ticker
                   )
                   SELECT contract_ticker, MIN(first_seen) AS first_seen
                   FROM pending GROUP BY contract_ticker ORDER BY first_seen ASC"""
            ).fetchall()
            return [r["contract_ticker"] for r in rows]

