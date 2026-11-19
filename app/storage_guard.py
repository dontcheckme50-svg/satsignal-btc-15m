"""Fail-closed checks for deployments that require durable SQLite storage.

A SQLite file survives process/container restarts only when its parent directory
is backed by storage that the host preserves. This module verifies the expected
mount at startup; it cannot manufacture persistence on an ephemeral filesystem.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Callable


def inspect_storage_mount(
    db_path: str,
    mount_path: str = "/data",
    *,
    mount_checker: Callable[[str], bool] = os.path.ismount,
) -> dict:
    mount = Path(mount_path).resolve()
    database = Path(db_path).resolve()
    try:
        inside_mount = os.path.commonpath((str(mount), str(database))) == str(mount)
    except ValueError:
        inside_mount = False

    mounted = mount_checker(str(mount))
    writable = False
    write_error = None
    if mounted and inside_mount and mount.is_dir():
        try:
            with tempfile.NamedTemporaryFile(
                prefix=".satsignal-storage-check-", dir=str(mount), delete=True
            ):
                pass
            writable = True
        except OSError as exc:
            write_error = type(exc).__name__

    confirmed = bool(mounted and inside_mount and writable)
    return {
        "status": "PERSISTENT_MOUNT_CONFIRMED" if confirmed else "PERSISTENCE_UNVERIFIED",
        "persistent_mount_confirmed": confirmed,
        "mount_path": str(mount),
        "database_path": str(database),
        "database_inside_mount": inside_mount,
        "mount_detected": bool(mounted),
        "mount_writable": writable,
        "write_error_type": write_error,
    }


def enforce_persistent_storage(
    db_path: str,
    mount_path: str = "/data",
    *,
    required: bool = False,
    mount_checker: Callable[[str], bool] = os.path.ismount,
) -> dict:
    result = inspect_storage_mount(
        db_path, mount_path, mount_checker=mount_checker
    )
    if required and not result["persistent_mount_confirmed"]:
        raise RuntimeError(
            "SATSignal startup blocked: durable storage is required, but "
            f"{mount_path} is not a verified writable mount containing "
            f"the configured SQLite database path ({db_path}). Attach a "
            "persistent Hostless volume at the configured mount path; "
            "do not run with an ephemeral database."
        )
    return result
