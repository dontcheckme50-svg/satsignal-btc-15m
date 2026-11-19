"""Read-only connectivity check for the SATSIGNAL Supabase PostgreSQL database.

Set SUPABASE_DATABASE_URL in the test environment. This script performs only
a SELECT 1 query; it does not create, alter, or migrate any database objects.
Connection details and exception messages are deliberately never printed.
"""
from __future__ import annotations

import os
import sys


def _safe_error_category(exc: Exception) -> str:
    """Return a coarse diagnostic without exposing connection details."""
    message = str(exc).lower()
    if "password authentication failed" in message or "authentication failed" in message:
        return "AUTHENTICATION_REJECTED"
    if "could not translate host name" in message or "name or service not known" in message or "getaddrinfo" in message:
        return "DNS_LOOKUP_FAILED"
    if "timeout" in message or "timed out" in message:
        return "CONNECTION_TIMEOUT"
    if "ssl" in message or "tls" in message or "certificate" in message:
        return "TLS_NEGOTIATION_FAILED"
    if "connection refused" in message:
        return "CONNECTION_REFUSED"
    if "no pg_hba.conf entry" in message:
        return "SERVER_ACCESS_POLICY_REJECTED"
    return "UNCLASSIFIED_OPERATIONAL_ERROR"


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
        print(f"SAFE DIAGNOSTIC: {_safe_error_category(exc)}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
