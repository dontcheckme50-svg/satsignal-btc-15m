"""Safely copy SATSIGNAL SQLite history into the configured PostgreSQL database.

Usage:
  DATABASE_URL=... python -m scripts.migrate_sqlite_to_postgres /path/to/satsignal.db
  DATABASE_URL=... python -m scripts.migrate_sqlite_to_postgres /path/to/satsignal.db --dry-run

The destination schema is initialized by PredictionLedger first. The import is
transactional and idempotent: identical existing rows are accepted; conflicting
rows stop the migration without overwriting destination data.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

import psycopg
from psycopg import sql

TABLES = (
    "predictions",
    "prediction_observations",
    "trades",
    "multi_exchange_reference",
)


def migrate(source_path: str, database_url: str, dry_run: bool = False) -> dict:
    if not os.path.isfile(source_path):
        raise FileNotFoundError(f"SQLite source file not found: {source_path}")

    # Ensure the destination tables and indexes exist using the application's
    # canonical schema initializer, not a second hand-maintained schema.
    from app.database import PredictionLedger
    PredictionLedger(":memory:")

    source = sqlite3.connect(f"file:{os.path.abspath(source_path)}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    copied = {}
    try:
        with psycopg.connect(database_url, connect_timeout=10) as destination:
            with destination.cursor() as cursor:
                for table in TABLES:
                    source_columns = [
                        row["name"]
                        for row in source.execute(f"PRAGMA table_info({table})").fetchall()
                    ]
                    if not source_columns:
                        raise RuntimeError(f"Source table is missing or has no columns: {table}")
                    destination_columns = cursor.execute(
                        """SELECT column_name FROM information_schema.columns
                           WHERE table_schema = current_schema() AND table_name = %s
                           ORDER BY ordinal_position""",
                        (table,),
                    ).fetchall()
                    destination_names = [row[0] for row in destination_columns]
                    if source_columns != destination_names:
                        raise RuntimeError(
                            f"Schema mismatch for {table}: SQLite={source_columns}, "
                            f"PostgreSQL={destination_names}"
                        )
                    rows = source.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
                    copied[table] = {"source_rows": len(rows), "inserted": 0, "already_present": 0}
                    if dry_run:
                        continue

                    columns = sql.SQL(", ").join(map(sql.Identifier, source_columns))
                    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in source_columns)
                    insert = sql.SQL(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING RETURNING id"
                    ).format(sql.Identifier(table), columns, placeholders)
                    select_existing = sql.SQL("SELECT {} FROM {} WHERE id = %s").format(
                        columns, sql.Identifier(table)
                    )
                    for row in rows:
                        values = tuple(row[column] for column in source_columns)
                        inserted = cursor.execute(insert, values).fetchone()
                        if inserted:
                            copied[table]["inserted"] += 1
                            continue
                        existing = cursor.execute(select_existing, (row["id"],)).fetchone()
                        if existing is None or tuple(existing) != values:
                            raise RuntimeError(
                                f"Conflicting destination data detected in {table}, id={row['id']}; "
                                "no existing destination rows were overwritten"
                            )
                        copied[table]["already_present"] += 1

                    # Keep future generated IDs above every imported ID.
                    cursor.execute(
                        sql.SQL(
                            "SELECT setval(pg_get_serial_sequence({}, 'id'), "
                            "GREATEST(COALESCE(MAX(id), 1), 1), MAX(id) IS NOT NULL) FROM {}"
                        ).format(sql.Literal(table), sql.Identifier(table))
                    )
            if dry_run:
                destination.rollback()
            else:
                destination.commit()
    finally:
        source.close()
    return copied


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite_path", help="Path to the exported SQLite database file")
    parser.add_argument("--dry-run", action="store_true", help="Validate schemas and count rows only")
    args = parser.parse_args()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL must be set in the environment; do not put credentials in source code")
    try:
        result = migrate(args.sqlite_path, database_url, dry_run=args.dry_run)
    except Exception as exc:
        print(f"Migration stopped: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print("DRY RUN - no rows written" if args.dry_run else "Migration committed")
    for table, counts in result.items():
        print(
            f"{table}: source={counts['source_rows']}, "
            f"inserted={counts['inserted']}, already_present={counts['already_present']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
