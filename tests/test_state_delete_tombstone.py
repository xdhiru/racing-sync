"""Regression (audit B7): delete() must never erase an existing tombstone."""
from __future__ import annotations

import sqlite3

from racing_sync.state import State, StateStore, TorrentState

H = "a" * 40


def _raw_deleted_at(db) -> str | None:
    conn = sqlite3.connect(db)
    try:
        row = conn.execute(
            "SELECT deleted_at FROM torrent_state WHERE source_infohash = ?",
            (H,),
        ).fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()


def test_delete_keeps_an_existing_tombstone(tmp_path):
    db = tmp_path / "state.db"
    store = StateStore(db)
    try:
        store.upsert(TorrentState(
            source_infohash=H, source_name="Gone", state=State.DONE))
        assert store.tombstone(H) is True
        stamp = _raw_deleted_at(db)
        assert stamp

        # tombstone() returns False for an already-stamped row and refreshes
        # the stamp; delete() must not fall through to a hard DELETE.
        store.delete(H)

        assert _raw_deleted_at(db)
        assert store.get(H) is None
    finally:
        store.close()


def test_delete_absent_row_is_a_noop(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.delete(H)
        assert store.get(H) is None
    finally:
        store.close()
