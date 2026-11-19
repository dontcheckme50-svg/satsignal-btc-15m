import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from app.storage_guard import enforce_persistent_storage, inspect_storage_mount


def test_storage_is_confirmed_only_when_mount_is_present_writable_and_contains_db(tmp_path):
    mount = tmp_path / "data"
    mount.mkdir()
    db = mount / "satsignal.db"

    status = inspect_storage_mount(
        str(db), str(mount), mount_checker=lambda _path: True
    )

    assert status["persistent_mount_confirmed"] is True
    assert status["status"] == "PERSISTENT_MOUNT_CONFIRMED"
    assert status["database_inside_mount"] is True
    assert status["mount_writable"] is True


def test_required_storage_fails_closed_when_mount_is_not_present(tmp_path):
    mount = tmp_path / "data"
    mount.mkdir()
    with pytest.raises(RuntimeError, match="startup blocked"):
        enforce_persistent_storage(
            str(mount / "satsignal.db"), str(mount),
            required=True, mount_checker=lambda _path: False,
        )


def test_required_storage_rejects_database_outside_mount(tmp_path):
    mount = tmp_path / "data"
    mount.mkdir()
    outside = tmp_path / "satsignal.db"
    with pytest.raises(RuntimeError, match="startup blocked"):
        enforce_persistent_storage(
            str(outside), str(mount),
            required=True, mount_checker=lambda _path: True,
        )


def test_non_required_storage_reports_unverified_without_blocking(tmp_path):
    mount = tmp_path / "data"
    mount.mkdir()
    status = enforce_persistent_storage(
        str(mount / "satsignal.db"), str(mount),
        required=False, mount_checker=lambda _path: False,
    )
    assert status["status"] == "PERSISTENCE_UNVERIFIED"
    assert status["persistent_mount_confirmed"] is False


def test_sqlite_records_survive_multiple_real_process_restarts_on_same_mount(tmp_path):
    """Exercise separate Python processes against the same mounted-directory path."""
    mount = tmp_path / "persistent-volume"
    mount.mkdir()
    db = mount / "satsignal.db"
    write_script = (
        "import sqlite3,sys; "
        "c=sqlite3.connect(sys.argv[1]); "
        "c.execute('create table if not exists restart_probe (value text)'); "
        "c.execute('insert into restart_probe values (?)', ('survives',)); "
        "c.commit(); c.close()"
    )
    read_script = (
        "import sqlite3,sys; "
        "c=sqlite3.connect(sys.argv[1]); "
        "rows=c.execute('select value from restart_probe').fetchall(); "
        "print(len(rows)); print(rows[-1][0]); c.close()"
    )

    for _ in range(3):
        subprocess.run([sys.executable, "-c", write_script, str(db)], check=True)

    result = subprocess.run(
        [sys.executable, "-c", read_script, str(db)],
        check=True, capture_output=True, text=True,
    )
    assert result.stdout.splitlines() == ["3", "survives"]
