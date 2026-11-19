import sys
from contextlib import contextmanager

from scripts import check_postgres_connection


def test_probe_fails_closed_without_database_url(monkeypatch, capsys):
    monkeypatch.delenv("SUPABASE_DATABASE_URL", raising=False)

    assert check_postgres_connection.main() == 2
    output = capsys.readouterr().out
    assert "not set" in output
    assert "SUCCESS" not in output


def test_probe_uses_tls_and_runs_select_one_only(monkeypatch, capsys):
    calls = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql):
            calls["sql"] = sql

        def fetchone(self):
            return (1,)

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
            calls["dsn"] = dsn
            calls["kwargs"] = kwargs
            return Connection()

    monkeypatch.setenv("SUPABASE_DATABASE_URL", "postgresql://redacted")
    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)

    assert check_postgres_connection.main() == 0
    assert calls["sql"] == "SELECT 1"
    assert calls["kwargs"]["sslmode"] == "require"
    assert calls["kwargs"]["connect_timeout"] == 10
    assert "redacted" not in capsys.readouterr().out
