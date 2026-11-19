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

def test_settlement_reads_receive_row_locks():
    prediction_sql, _ = _ConnectionCompat._sql(
        "SELECT * FROM predictions WHERE contract_ticker = ? AND is_official_lock = 1 AND settled = 0"
    )
    trade_sql, _ = _ConnectionCompat._sql(
        "SELECT * FROM trades WHERE contract_ticker = ? AND status = 'OPEN'"
    )
    assert prediction_sql.endswith("FOR UPDATE")
    assert trade_sql.endswith("FOR UPDATE")


class _FakeCursor:
    def __init__(self, row, rowcount):
        self._row = row
        self.rowcount = rowcount
    def fetchone(self):
        row, self._row = self._row, None
        return row
    def fetchall(self):
        return []


class _FakeConnection:
    def __init__(self, row, rowcount):
        self.cursor = _FakeCursor(row, rowcount)
    def execute(self, statement, params=()):
        return self.cursor


def test_ignored_insert_without_returned_id_has_zero_rowcount():
    from app.postgres_database import _ConnectionCompat
    conn = _ConnectionCompat(_FakeConnection(None, 1))
    cur = conn.execute("INSERT OR IGNORE INTO predictions (contract_ticker) VALUES (?)", ("T",))
    assert cur.lastrowid is None
    assert cur.rowcount == 0


def test_insert_with_returned_id_preserves_id_and_row():
    from app.postgres_database import _ConnectionCompat
    conn = _ConnectionCompat(_FakeConnection({"id": 42}, 1))
    cur = conn.execute("INSERT INTO trades (contract_ticker) VALUES (?)", ("T",))
    assert cur.lastrowid == 42
    assert cur.rowcount == 1
    assert cur.fetchone() == {"id": 42}
