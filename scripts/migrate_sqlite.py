"""Safely copy a live SQLite ledger into a new persistent-volume location.

Uses SQLite's online backup API so committed WAL data is included consistently.
Never overwrites an existing destination.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

TABLES_TO_VERIFY = (
    "predictions",
    "prediction_observations",
    "trades",
    "multi_exchange_reference",
)


def _integrity_check(conn: sqlite3.Connection) -> str:
    rows = conn.execute("PRAGMA integrity_check").fetchall()
    return str(rows[0][0]) if len(rows) == 1 else repr(rows)


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    counts: dict[str, int] = {}
    for table in TABLES_TO_VERIFY:
        if table in existing:
            # Table names are selected only from the fixed allowlist above.
            counts[table] = int(
                conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            )
    return counts


def migrate_database(source_path: str, destination_path: str) -> dict[str, Any]:
    source = Path(source_path).expanduser().resolve()
    destination = Path(destination_path).expanduser().resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Source database does not exist: {source}")
    if source == destination:
        raise ValueError("Source and destination must be different files")
    if not destination.parent.is_dir():
        raise FileNotFoundError(
            f"Destination directory does not exist: {destination.parent}"
        )
    if destination.exists():
        raise FileExistsError(
            f"Destination already exists; refusing to overwrite: {destination}"
        )

    source_uri = f"{source.as_uri()}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True, timeout=30.0)) as source_conn:
        source_conn.execute("PRAGMA busy_timeout=30000")
        source_integrity = _integrity_check(source_conn)
        if source_integrity != "ok":
            raise sqlite3.DatabaseError(
                f"Source database integrity check failed: {source_integrity}"
            )
        source_counts = _table_counts(source_conn)
        if not source_counts:
            raise sqlite3.DatabaseError(
                "Source database has none of the expected SATSIGNAL ledger tables"
            )

        with closing(sqlite3.connect(str(destination), timeout=30.0)) as destination_conn:
            source_conn.backup(destination_conn, pages=256, sleep=0.1)
            destination_conn.execute("PRAGMA journal_mode=WAL")
            destination_conn.commit()
            destination_integrity = _integrity_check(destination_conn)
            destination_counts = _table_counts(destination_conn)

    if destination_integrity != "ok":
        raise sqlite3.DatabaseError(
            f"Destination database integrity check failed: {destination_integrity}"
        )
    if destination_counts != source_counts:
        raise sqlite3.DatabaseError(
            "Migration verification failed: source and destination row counts differ "
            f"(source={source_counts}, destination={destination_counts})"
        )

    return {
        "status": "verified",
        "source": str(source),
        "destination": str(destination),
        "integrity_check": destination_integrity,
        "row_counts": destination_counts,
        "source_preserved": source.is_file(),
        "destination_created": destination.is_file(),
        "overwrote_existing_destination": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy SATSIGNAL SQLite data to a new persistent-volume path."
    )
    parser.add_argument("--source", required=True, help="Existing SQLite database file")
    parser.add_argument(
        "--destination",
        required=True,
        help="New database path on the mounted persistent volume; must not exist",
    )
    args = parser.parse_args()
    result = migrate_database(args.source, args.destination)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
