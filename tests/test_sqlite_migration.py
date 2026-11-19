import sqlite3

import pytest

from scripts.migrate_sqlite import migrate_database


def _make_source(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE predictions (id INTEGER PRIMARY KEY, contract_ticker TEXT);
            CREATE TABLE prediction_observations (id INTEGER PRIMARY KEY, payload_json TEXT);
            CREATE TABLE trades (id INTEGER PRIMARY KEY, contract_ticker TEXT);
            CREATE TABLE multi_exchange_reference (id INTEGER PRIMARY KEY, contract_ticker TEXT);
            INSERT INTO predictions VALUES (1, 'KXBTC15M-ONE');
            INSERT INTO predictions VALUES (2, 'KXBTC15M-TWO');
            INSERT INTO prediction_observations VALUES (1, '{"signal":"UP"}');
            INSERT INTO trades VALUES (1, 'KXBTC15M-ONE');
            INSERT INTO multi_exchange_reference VALUES (1, 'KXBTC15M-ONE');
            """
        )


def test_migration_copies_all_ledger_tables_and_verifies_counts(tmp_path):
    source = tmp_path / "legacy.db"
    destination_dir = tmp_path / "persistent"
    destination_dir.mkdir()
    destination = destination_dir / "satsignal_btc15m.db"
    _make_source(source)

    result = migrate_database(str(source), str(destination))

    assert result["status"] == "verified"
    assert result["integrity_check"] == "ok"
    assert result["source_preserved"] is True
    assert result["overwrote_existing_destination"] is False
    assert result["row_counts"] == {
        "predictions": 2,
        "prediction_observations": 1,
        "trades": 1,
        "multi_exchange_reference": 1,
    }
    with sqlite3.connect(destination) as conn:
        assert conn.execute("SELECT contract_ticker FROM predictions ORDER BY id").fetchall() == [
            ("KXBTC15M-ONE",),
            ("KXBTC15M-TWO",),
        ]


def test_migration_refuses_to_overwrite_existing_destination(tmp_path):
    source = tmp_path / "legacy.db"
    destination = tmp_path / "existing.db"
    _make_source(source)
    destination.write_bytes(b"keep this file unchanged")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        migrate_database(str(source), str(destination))

    assert destination.read_bytes() == b"keep this file unchanged"
    assert source.is_file()


def test_migration_rejects_invalid_source_without_creating_destination(tmp_path):
    source = tmp_path / "not-a-database.db"
    destination = tmp_path / "persistent.db"
    source.write_bytes(b"not sqlite")

    with pytest.raises(sqlite3.DatabaseError):
        migrate_database(str(source), str(destination))

    assert not destination.exists()


def test_migration_requires_destination_directory_to_exist(tmp_path):
    source = tmp_path / "legacy.db"
    _make_source(source)
    destination = tmp_path / "missing" / "persistent.db"

    with pytest.raises(FileNotFoundError, match="Destination directory"):
        migrate_database(str(source), str(destination))

    assert source.is_file()
    assert not destination.exists()
