"""Small PostgreSQL connection adapter for the prediction ledger."""
from __future__ import annotations

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
    """Translate the ledger's qmark bind markers and expose insert IDs explicitly."""

    def __init__(self, connection):
        self._connection = connection

    def execute(self, sql, params=()):
        raw = sql.strip()
        # psycopg reserves percent signs for placeholders. Escape literal LIKE
        # wildcards before translating the ledger's qmark parameters.
        raw = raw.replace("%", "%%").replace("?", "%s")
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
    """Connect to Supabase PostgreSQL with TLS required by default."""
    if "sslmode=" not in database_url:
        separator = "&" if "?" in database_url else "?"
        database_url += separator + "sslmode=require"
    connection = psycopg.connect(
        database_url, row_factory=dict_row, connect_timeout=10
    )
    return ConnectionAdapter(connection)
