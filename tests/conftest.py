import os
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    raise RuntimeError(
        "TEST_DATABASE_URL is required for tests. Use a dedicated disposable PostgreSQL/Supabase test database."
    )
os.environ["DATABASE_URL"] = TEST_DATABASE_URL


@pytest.fixture(autouse=True)
def clear_test_ledger():
    """Every test gets an empty ledger in the dedicated test database only."""
    from app.postgres_compat import connect

    conn = connect(TEST_DATABASE_URL)
    try:
        conn.execute(
            "TRUNCATE TABLE predictions, prediction_observations, trades, "
            "multi_exchange_reference RESTART IDENTITY CASCADE"
        )
        conn.commit()
    finally:
        conn.close()
