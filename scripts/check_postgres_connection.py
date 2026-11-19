"""Read-only connectivity check for the SATSIGNAL Supabase PostgreSQL database.

Set SUPABASE_DATABASE_URL in the test environment. This script performs only
a SELECT 1 query; it does not create, alter, or migrate any database objects.
Connection details and exception messages are deliberately never printed.
"""
from __future__ import annotations

import os
import sys


def main() -> int:
    dsn = os.environ.get("SUPABASE_DATABASE_URL", "").strip()
    if not dsn:
        print("SUPABASE_DATABASE_URL is not set; no connection attempted.")
        return 2

    try:
        import psycopg
    except ImportError:
        print("psycopg is not installed; install project requirements first.")
        return 3

    try:
        with psycopg.connect(
            dsn,
            connect_timeout=10,
            sslmode="require",
            application_name="satsignal-supabase-connectivity-probe",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                if row != (1,):
                    print("Database connection opened, but SELECT 1 returned an unexpected result.")
                    return 4
        print("POSTGRES CONNECTION: SUCCESS")
        print("READ-ONLY QUERY: SELECT 1 PASSED")
        return 0
    except Exception as exc:
        # Avoid printing exception text because drivers may include connection
        # metadata. The class name is enough to guide safe diagnostics.
        print(f"POSTGRES CONNECTION: FAILED ({type(exc).__name__})")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
