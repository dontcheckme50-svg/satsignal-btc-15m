"""Narrow PostgreSQL adapter for the SQL surface used by PredictionLedger."""
from __future__ import annotations
import re
import psycopg
from psycopg.rows import dict_row


class RowAdapter(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class CursorAdapter:
    def __init__(self, cursor, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid
        self.rowcount = cursor.rowcount

    def fetchone(self):
        row = self._cursor.fetchone()
        return RowAdapter(row) if row is not None else None

    def fetchall(self):
        return [RowAdapter(row) for row in self._cursor.fetchall()]


class ConnectionAdapter:
    def __init__(self, connection):
        self._connection = connection

    def execute(self, sql, params=()):
        raw = sql.strip()
        upper = raw.upper()
        if upper.startswith("PRAGMA TABLE_INFO("):
            match = re.search(r"PRAGMA\s+table_info\(([^)]+)\)", raw, re.I)
            table = match.group(1).strip("'\"")
            cursor = self._connection.execute(
                """SELECT column_name AS name FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = %s
                   ORDER BY ordinal_position""", (table,)
            )
            return CursorAdapter(cursor)
        if upper.startswith("PRAGMA JOURNAL_MODE"):
            return CursorAdapter(self._connection.execute("SELECT 'POSTGRESQL' AS journal_mode"))
        if upper.startswith("PRAGMA QUICK_CHECK"):
            return CursorAdapter(self._connection.execute("SELECT 'not_applicable' AS integrity"))
        if upper.startswith("BEGIN IMMEDIATE"):
            raw = "SELECT pg_advisory_xact_lock(hashtext('satsignal-settlement'))"
        raw = raw.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        # psycopg treats literal percent signs (including SQL LIKE wildcards)
        # as placeholder syntax. Escape them before translating ? parameters.
        raw = raw.replace("%", "%%")
        raw = raw.replace("?", "%s")
        if raw.upper().startswith("INSERT OR IGNORE INTO"):
            raw = re.sub(r"^INSERT OR IGNORE INTO", "INSERT INTO", raw, flags=re.I)
            raw += " ON CONFLICT DO NOTHING"
        is_insert = raw.upper().startswith("INSERT INTO")
        if is_insert and " RETURNING " not in raw.upper():
            raw += " RETURNING id"
        cursor = self._connection.execute(raw, params or ())
        inserted_id = None
        if is_insert and cursor.rowcount:
            returned = cursor.fetchone()
            if returned:
                inserted_id = returned["id"]
        return CursorAdapter(cursor, inserted_id)

    def executescript(self, script):
        for statement in script.split(";"):
            if statement.strip():
                self.execute(statement)

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()


def connect(database_url: str) -> ConnectionAdapter:
    if "sslmode=" not in database_url:
        separator = "&" if "?" in database_url else "?"
        database_url += separator + "sslmode=require"
    connection = psycopg.connect(
        database_url, row_factory=dict_row, connect_timeout=10
    )
    return ConnectionAdapter(connection)
