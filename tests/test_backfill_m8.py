"""M8 backfill: narrow gaps across phases (state/gc, forget pairs, margin)."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_gc_tombstones_ttl(tmp_path: Path):
    """Only tombstones older than the TTL are hard-deleted."""
    import datetime as dt

    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="a" * 40, source_name="Old", state=State.FAILED))
        store.upsert(TorrentState(source_infohash="b" * 40, source_name="New", state=State.FAILED))
        store.delete("a" * 40)
        store.delete("b" * 40)
        old = (dt.datetime.now(dt.UTC) - dt.timedelta(hours=49)).isoformat()
        store._conn.execute(
            "UPDATE torrent_state SET deleted_at = ? WHERE source_infohash = ?",
            (old, "a" * 40),
        )
        assert store.gc_tombstones(ttl_seconds=24 * 3600) == 1
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None  # tombstoned, not live
        # Fresh tombstone survives; ttl=0 takes it.
        assert store.gc_tombstones(ttl_seconds=24 * 3600) == 0
        assert store.gc_tombstones(ttl_seconds=0) == 1
    finally:
        store.close()


def test_null_deleted_at_normalized_on_open(tmp_path: Path):
    """Legacy NULL deleted_at rows become visible live rows, not ghosts."""
    import datetime as dt
    import sqlite3

    from racing_sync.state import (
        SCHEMA_TABLES,
        State,
        StateStore,
    )

    db = tmp_path / "legacy.db"
    now = dt.datetime.now(dt.UTC).isoformat()
    legacy_schema = "\n".join(
        "deleted_at TEXT" if ln.strip().startswith("deleted_at ") else ln
        for ln in SCHEMA_TABLES.splitlines()
    )
    assert legacy_schema != SCHEMA_TABLES
    conn = sqlite3.connect(str(db), isolation_level=None)
    try:
        conn.executescript(legacy_schema)
        conn.execute(
            "INSERT INTO torrent_state (source_infohash, source_name, "
            "state, created_at, updated_at, deleted_at) "
            "VALUES (?, ?, ?, ?, ?, NULL)",
            ("a" * 40, "Legacy", State.NEW.value, now, now),
        )
    finally:
        conn.close()
    store = StateStore(db)
    try:
        got = store.get("a" * 40)
        assert got is not None and got.state == State.NEW
        raw = store._conn.execute(
            "SELECT deleted_at FROM torrent_state WHERE source_infohash = ?", ("a" * 40,)
        ).fetchone()[0]
        assert raw == ""
    finally:
        store.close()


def test_find_by_name_empty_string(tmp_path: Path):
    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="a" * 40, source_name="A", state=State.NEW))
        assert store.find_by_name("") == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_forget_hard_deletes_pairs_without_zombies(tmp_path: Path):
    """forget(hard) on an owner hard-deletes waiters too: re-drop works."""
    from test_forget import FakeDest, _cfg, _watch_pair_store

    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.forget import forget_torrent
    from racing_sync.state import State, TorrentState

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store, _owner, _waiters, _bystanders = _watch_pair_store(tmp_path)
    dest = FakeDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Shared/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd),
        dest=dest,
        store=store,
        target="a" * 40,
        apply=True,
        delete_files=True,
        ignore=False,
        hard=True,
    )

    assert store.get("a" * 40) is None
    assert store.get("b" * 40) is None
    assert store.get("c" * 40) is None
    assert {p["source_infohash"] for p in result["paired_cancelled"]} == {"b" * 40, "c" * 40}
    # No tombstones, no ignore entries: a later re-drop re-ingests clean.
    assert store.is_ignored("a" * 40) is False
    assert store.is_ignored("b" * 40) is False
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Re", state=State.NEW))
    assert store.get("b" * 40) is not None


def test_flood_margin_math():
    """Parsed waits stretch 1.2x + 1s (never the exact ban window)."""
    from racing_sync.telegram_protocol import _with_flood_margin

    assert _with_flood_margin(5) == 7
    assert _with_flood_margin(39) == 47
    assert _with_flood_margin(0) == 1
    assert _with_flood_margin(None) == 1
    assert _with_flood_margin("nope") == 1
