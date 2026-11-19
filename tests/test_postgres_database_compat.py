from app.postgres_database import _ConnectionCompat


def test_sqlite_placeholders_are_translated():
    sql, ignored = _ConnectionCompat._sql(
        "SELECT * FROM predictions WHERE contract_ticker = ? AND settled = ?"
    )
    assert sql == "SELECT * FROM predictions WHERE contract_ticker = %s AND settled = %s"
    assert ignored is False


def test_insert_or_ignore_becomes_idempotent_postgres_insert():
    sql, ignored = _ConnectionCompat._sql(
        "INSERT OR IGNORE INTO predictions (contract_ticker) VALUES (?)"
    )
    assert ignored is True
    assert sql == (
        "INSERT INTO predictions (contract_ticker) VALUES (%s) "
        "ON CONFLICT DO NOTHING RETURNING id"
    )


def test_regular_insert_returns_generated_id():
    sql, ignored = _ConnectionCompat._sql(
        "INSERT INTO trades (contract_ticker) VALUES (?)"
    )
    assert ignored is False
    assert sql == "INSERT INTO trades (contract_ticker) VALUES (%s) RETURNING id"


def test_existing_returning_clause_is_not_duplicated():
    sql, ignored = _ConnectionCompat._sql(
        "INSERT INTO predictions (contract_ticker) VALUES (?) RETURNING id"
    )
    assert ignored is False
    assert sql == "INSERT INTO predictions (contract_ticker) VALUES (%s) RETURNING id"
