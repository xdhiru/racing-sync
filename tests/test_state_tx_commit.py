"""Regression tests for StateStore._write_tx transaction unwinding (audit B3).

A failed COMMIT or a BaseException escaping the transaction body must not
leave the sqlite connection inside an open transaction: the next
BEGIN IMMEDIATE would fail with "cannot start a transaction within a
transaction" and wedge every write until restart.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from racing_sync.state import StateStore, TorrentState, UpsertResult


class _CommitFailingConn:
    """Delegating proxy around sqlite3.Connection whose COMMIT raises.

    sqlite3.Connection methods are C-level, so patching the instance is not
    reliable; a proxy that only intercepts execute() is.
    """

    def __init__(self, real: sqlite3.Connection, exc: BaseException):
        self._real = real
        self._exc = exc

    def execute(self, sql, *args, **kwargs):
        if str(sql).strip().upper().startswith("COMMIT"):
            raise self._exc
        return self._real.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_commit_failure_rolls_back_and_next_write_succeeds(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    real = store._conn
    try:
        ts = TorrentState(source_infohash="a" * 40, source_name="A")
        store._conn = _CommitFailingConn(real, sqlite3.OperationalError("disk I/O error"))
        with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
            store.upsert(ts)
        # The failed COMMIT must not strand an open transaction.
        assert store._conn.in_transaction is False
        assert real.in_transaction is False

        store._conn = real
        # Before the fix this raised "cannot start a transaction within a
        # transaction"; now the next write begins and commits normally.
        assert store.upsert(ts) is UpsertResult.WRITTEN
        assert store.get(ts.source_infohash) is not None
    finally:
        store._conn = real
        store.close()


def test_base_exception_from_body_rolls_back(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    real = store._conn
    try:

        def _boom():
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            with store._write_tx():
                store._conn.execute("INSERT INTO meta (key, value) VALUES ('k', 'v')")
                _boom()

        assert real.in_transaction is False
        assert store.get_meta("k") is None
        # Connection still usable for a fresh transaction.
        with store._write_tx():
            store.set_meta("k2", "v2")
        assert store.get_meta("k2") == "v2"
    finally:
        store.close()
