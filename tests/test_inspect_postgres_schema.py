import sys

from scripts import inspect_postgres_schema


def test_inspection_fails_closed_without_database_url(monkeypatch, capsys):
    monkeypatch.delenv("SUPABASE_DATABASE_URL", raising=False)
    assert inspect_postgres_schema.main() == 2
    assert "no inspection attempted" in capsys.readouterr().out


def test_inspection_uses_only_catalog_selects(monkeypatch, capsys):
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=None):
            calls.append((sql, params))

        def fetchone(self):
            return ("postgres", "public")

        def fetchall(self):
            return [("predictions",)] if "information_schema.tables" in calls[-1][0] else [("idx_pred_ticker",)]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def cursor(self):
            return Cursor()

    class FakePsycopg:
        @staticmethod
        def connect(dsn, **kwargs):
            assert dsn == "postgresql://redacted"
            assert kwargs["sslmode"] == "require"
            assert kwargs["autocommit"] is True
            return Connection()

    monkeypatch.setenv("SUPABASE_DATABASE_URL", "postgresql://redacted")
    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)

    assert inspect_postgres_schema.main() == 0
    output = capsys.readouterr().out
    assert "READ-ONLY SCHEMA INSPECTION: SUCCESS" in output
    assert "NO DATABASE OBJECTS WERE CHANGED." in output
    assert all(sql.lstrip().upper().startswith("SELECT") for sql, _ in calls)
