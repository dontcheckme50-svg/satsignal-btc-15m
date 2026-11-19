"""Read-only inspection of the dedicated Supabase schema before any DDL.

This script never creates, alters, or deletes database objects.
"""
from __future__ import annotations

import os
import sys

TABLE_NAMES = (
    "predictions",
    "prediction_observations",
    "trades",
    "multi_exchange_reference",
)
INDEX_NAMES = (
    "idx_pred_ticker",
    "idx_pred_settled",
    "idx_observation_contract_epoch",
    "idx_trades_ticker",
    "idx_multi_ref_epoch",
    "idx_multi_ref_contract",
    "idx_one_official_prediction_per_contract",
)


def main() -> int:
    dsn = os.environ.get("SUPABASE_DATABASE_URL")
    if not dsn:
        print("SUPABASE_DATABASE_URL is not set; no inspection attempted.")
        return 2

    try:
        import psycopg

        with psycopg.connect(
            dsn,
            connect_timeout=10,
            sslmode="require",
            application_name="satsignal-supabase-schema-inspection",
            autocommit=True,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_schema()")
                database, schema = cur.fetchone()
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_type = 'BASE TABLE'
                      AND table_name = ANY(%s)
                    ORDER BY table_name
                    """,
                    (list(TABLE_NAMES),),
                )
                tables = [row[0] for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = current_schema()
                      AND indexname = ANY(%s)
                    ORDER BY indexname
                    """,
                    (list(INDEX_NAMES),),
                )
                indexes = [row[0] for row in cur.fetchall()]

        print("READ-ONLY SCHEMA INSPECTION: SUCCESS")
        print(f"DATABASE: {database}")
        print(f"SCHEMA: {schema}")
        print("EXISTING TARGET TABLES: " + (", ".join(tables) if tables else "none"))
        print("EXISTING TARGET INDEXES: " + (", ".join(indexes) if indexes else "none"))
        print("NO DATABASE OBJECTS WERE CHANGED.")
        return 0
    except Exception as exc:
        # Do not print exception text: it can contain connection details.
        print(f"READ-ONLY SCHEMA INSPECTION: FAILED ({type(exc).__name__})")
        return 1


if __name__ == "__main__":
    sys.exit(main())
