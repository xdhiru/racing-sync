from __future__ import annotations

from pathlib import Path
import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState, check_transition


def test_done_can_only_transition_to_re_adding():
    check_transition(State.DONE, State.RE_ADDING)
    # Fresh-DB self-heal: falsely adopted DONE (SSD bytes never moved) demotes
    # back to MOVING so the rclone move runs before any fuse injection.
    check_transition(State.DONE, State.MOVING)
    for s in [State.NEW, State.QUEUED, State.DOWNLOADING,
              State.WAITING_DISK, State.QUERYING, State.FAILED]:
        with pytest.raises(ValueError):
            check_transition(State.DONE, s)


def test_failed_can_retry_to_queued_and_new():
    check_transition(State.FAILED, State.QUEUED)
    check_transition(State.FAILED, State.NEW)


def test_new_can_fast_track_to_done_for_manual_fuse():
    # Manual fuse adoption: same infohash already seeding from fuse with
    # verified bytes needs no SSD work (NEW/WAITING_INDEXER -> DONE).
    check_transition(State.NEW, State.DONE)
    check_transition(State.WAITING_INDEXER, State.DONE)
    check_transition(State.QUERYING, State.DONE)
    check_transition(State.WAITING_DISK, State.DONE)


def test_new_can_go_to_any_inflight():
    for s in [State.QUERYING, State.WAITING_DISK, State.QUEUED,
              State.DOWNLOADING, State.MOVING, State.RE_ADDING, State.DONE]:
        check_transition(State.NEW, s)


def test_downloading_to_moving_is_ok():
    check_transition(State.DOWNLOADING, State.MOVING)


def test_downloading_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.DOWNLOADING, State.DONE)


def test_moving_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.MOVING, State.DONE)


def test_indexer_park_and_retry():
    check_transition(State.NEW, State.WAITING_INDEXER)
    check_transition(State.QUERYING, State.WAITING_INDEXER)
    check_transition(State.WAITING_INDEXER, State.QUERYING)
    check_transition(State.WAITING_INDEXER, State.QUEUED)
    check_transition(State.WAITING_INDEXER, State.FAILED)


def test_force_direct_defaults_zero_and_roundtrips(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert TorrentState(source_infohash="a" * 40).force_direct == 0
        ts = TorrentState(source_infohash="b" * 40, state=State.WAITING_INDEXER,
                          force_direct=1)
        store.upsert(ts)
        assert store.get("b" * 40).force_direct == 1
        # list/get paths without blob carry it too.
        assert store.list_by_state(State.WAITING_INDEXER)[0].force_direct == 1
    finally:
        store.close()


def test_force_direct_migrates_1_0_database(tmp_path: Path):
    """A 1.0.0 DB (full schema minus force_direct) gains it on open."""
    import sqlite3

    from racing_sync.state import SCHEMA_TABLES

    old_schema = "\n".join(
        ln for ln in SCHEMA_TABLES.splitlines()
        if "force_direct" not in ln
    )
    db = tmp_path / "old.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(old_schema)
    conn.execute(
        "INSERT INTO torrent_state (source_infohash, state, created_at, updated_at)"
        " VALUES (?, ?, ?, ?)",
        ("c" * 40, "waiting_indexer", "2026-01-01T00:00:00+00:00",
         "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    store = StateStore(db)
    try:
        cols = [r[1] for r in store._conn.execute(
            "PRAGMA table_info(torrent_state)").fetchall()]
        assert "force_direct" in cols
        assert store.get("c" * 40).force_direct == 0
        row = store.get("c" * 40)
        row.force_direct = 1
        store.upsert(row)
        assert store.get("c" * 40).force_direct == 1
    finally:
        store.close()


def test_indexer_no_self_transition():
    # Re-parking bumps fields but should not go through transition().
    # The state machine still treats WAITING_INDEXER -> WAITING_INDEXER
    # as illegal; callers must upsert() instead.
    with pytest.raises(ValueError):
        check_transition(State.WAITING_INDEXER, State.WAITING_INDEXER)


def test_queued_to_done_is_allowed():
    # When a torrent is already complete/seeding on VPS2, QUEUED fast-tracks to DONE.
    check_transition(State.QUEUED, State.DONE)


def test_queued_to_moving_and_waiting_disk_is_allowed():
    # Single file already on remote skips SSD download (QUEUED->MOVING);
    # SSD-full park after add goes back to WAITING_DISK (bug 1).
    check_transition(State.QUEUED, State.MOVING)
    check_transition(State.QUEUED, State.WAITING_DISK)


def test_find_by_name_extension_matching(tmp_path):
    from racing_sync.state import StateStore, TorrentState

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    # Stored without .mkv (as a download-target indexer or VPS2 client might report)
    ts = TorrentState(
        source_infohash="hash123",
        source_name="Harbor.Lights.S01E06.1080p-Raccoon",
        state=State.DONE,
    )
    store.upsert(ts)

    # Query with .mkv (as Alpha reports)
    matches = store.find_by_name("Harbor.Lights.S01E06.1080p-Raccoon.mkv")
    assert len(matches) == 1
    assert matches[0].source_infohash == "hash123"

    # Query exact without .mkv
    matches2 = store.find_by_name("Harbor.Lights.S01E06.1080p-Raccoon")
    assert len(matches2) == 1


def test_upsert_preserves_created_at(tmp_path):
    import datetime as dt
    import time
    from racing_sync.state import StateStore, TorrentState

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    orig_time = dt.datetime(2025, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    ts = TorrentState(
        source_infohash="hash456",
        source_name="Test.Release",
        state=State.QUEUED,
        created_at=orig_time,
    )
    store.upsert(ts)

    loaded = store.get("hash456")
    assert loaded is not None
    assert loaded.created_at == orig_time

    # Subsequent upsert with different created_at in memory must NOT overwrite DB created_at
    newer_time = dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    ts2 = TorrentState(
        source_infohash="hash456",
        source_name="Test.Release",
        state=State.DOWNLOADING,
        created_at=newer_time,
    )
    store.upsert(ts2)

    reloaded = store.get("hash456")
    assert reloaded is not None
    assert reloaded.state == State.DOWNLOADING
    assert reloaded.created_at == orig_time


def test_iter_logs_returns_list_and_handles_pruning(tmp_path):
    from racing_sync.state import StateStore

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    for i in range(10):
        store.append_log("INFO", f"msg {i}", source_infohash="hash1")

    logs = store.iter_logs(limit=5)
    assert isinstance(logs, list)
    assert len(logs) == 5
    assert logs[0]["message"] == "msg 9"

    store.prune_logs(max_records=3)
    remaining = store.iter_logs(limit=10)
    assert len(remaining) == 3


def test_row_to_state_handles_string_and_none_blob(tmp_path):
    import datetime as dt
    from racing_sync.state import StateStore, TorrentState, State

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    ts = TorrentState(
        source_infohash="hash789",
        source_name="Test.Blob",
        state=State.QUEUED,
    )
    store.upsert(ts)

    # Manually update cross_seed_blob in SQLite to empty string TEXT
    store._conn.execute(
        "UPDATE torrent_state SET cross_seed_blob = '' WHERE source_infohash = 'hash789'"
    )

    loaded = store.get("hash789")
    assert loaded is not None
    assert isinstance(loaded.cross_seed_blob, bytes)
    assert loaded.cross_seed_blob == b""

    # All active query returns blob-less state without error
    active = store.all_active()
    assert len(active) == 1
    assert active[0].cross_seed_blob == b""


def test_state_store_blob_lazy_load_and_preservation(tmp_path: Path):
    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    test_blob = b"d8:announce25:http://tracker.example.com4:infod4:name4:testeee"
    ts = TorrentState(
        source_infohash="hash_blob_1",
        source_name="Test.Blob.Release",
        cross_seed_blob=test_blob,
        state=State.QUEUED,
    )
    store.upsert(ts)

    # get_blob returns the stored blob
    assert store.get_blob("hash_blob_1") == test_blob
    assert store.get_blob("non_existent") == b""

    # all() defaults to include_blob=False (prevents OOM)
    all_default = store.all()
    assert len(all_default) == 1
    assert all_default[0].cross_seed_blob == b""

    # all(include_blob=True) returns blob
    all_with_blob = store.all(include_blob=True)
    assert len(all_with_blob) == 1
    assert all_with_blob[0].cross_seed_blob == test_blob

    # Upserting a row loaded without blob does NOT overwrite or wipe the DB blob
    ts_loaded_no_blob = all_default[0]
    ts_loaded_no_blob.state = State.DOWNLOADING
    store.upsert(ts_loaded_no_blob)

    # DB blob is preserved
    assert store.get_blob("hash_blob_1") == test_blob
    loaded_again = store.get("hash_blob_1")
    assert loaded_again is not None
    assert loaded_again.cross_seed_blob == test_blob
    assert loaded_again.state == State.DOWNLOADING

    # Upserting with a new non-empty blob DOES overwrite
    new_blob = b"new_blob_content"
    ts_loaded_no_blob.cross_seed_blob = new_blob
    store.upsert(ts_loaded_no_blob)
    assert store.get_blob("hash_blob_1") == new_blob


@pytest.mark.anyio
async def test_coordinator_lazy_loads_blob_on_queued_and_re_adding(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.config import AppConfig

    cfg = AppConfig.from_toml(Path(__file__).parent.parent / "config.example.toml")
    store = StateStore(tmp_path / "test.db")

    test_blob = b"d8:announce25:http://tracker.example.com4:infod4:name4:testeee"
    ts = TorrentState(
        source_infohash="hash_resumed_1",
        source_name="Test.Resumed",
        cross_seed_blob=test_blob,
        state=State.QUEUED,
    )
    store.upsert(ts)

    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents.return_value = []
    coord.dest_client.add_torrent.return_value = MagicMock(accepted=True, hash="hash_resumed_1")
    coord._await_hash_for_name = AsyncMock(return_value="hash_resumed_1")
    coord.transition = MagicMock(side_effect=lambda t, s, **kwargs: setattr(t, "state", s))

    # Simulate resuming from DB without blob (as returned by all_active())
    ts_resumed = store.all_active()[0]
    assert ts_resumed.cross_seed_blob == b""
    assert ts_resumed._blob == b""

    # _do_queued should lazily fetch blob from store instead of failing with 'no blob'
    await coord._do_queued(ts_resumed)
    assert ts_resumed.cross_seed_blob == test_blob
    coord.dest_client.add_torrent.assert_awaited_once()

    # Now test RE_ADDING lazy load
    coord.dest_client.add_torrent.reset_mock()
    ts_readding = TorrentState(
        source_infohash="hash_readd_1",
        source_name="Test.Readd",
        cross_seed_blob=test_blob,
        state=State.RE_ADDING,
    )
    store.upsert(ts_readding)

    active_rows = [r for r in store.all_active() if r.source_infohash == "hash_readd_1"]
    assert len(active_rows) == 1
    ts_readd_resumed = active_rows[0]
    assert ts_readd_resumed.cross_seed_blob == b""
    assert ts_readd_resumed._blob == b""

    coord._target_mount_for = MagicMock(return_value=Path("/mnt/remote"))
    coord.dest_client.get_torrent = AsyncMock(return_value=MagicMock(save_path="/mnt/remote"))
    await coord._re_add_cross_seed_torrent(ts_readd_resumed)
    assert ts_readd_resumed.cross_seed_blob == test_blob
    coord.dest_client.add_torrent.assert_awaited_once()


def test_transition_clears_last_error(tmp_path: Path):
    store = StateStore(tmp_path / "test_err.db")
    try:
        ts = TorrentState(source_infohash="1" * 40, state=State.QUEUED)
        store.upsert(ts)
        store.transition(ts, State.FAILED, error="some failure")
        assert ts.last_error == "some failure"
        store.transition(ts, State.QUEUED)
        assert ts.last_error == ""
    finally:
        store.close()


def test_transition_resets_retry_timers(tmp_path: Path):
    import datetime as dt
    store = StateStore(tmp_path / "test_timers.db")
    try:
        past = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        ts = TorrentState(
            source_infohash="2" * 40,
            state=State.FAILED,
            indexer_next_retry_at=past,
            indexer_attempts=5,
        )
        store.upsert(ts)
        store.transition(ts, State.NEW)
        assert ts.indexer_next_retry_at is None
        assert ts.indexer_attempts == 0
    finally:
        store.close()


def test_transition_to_done_sets_completed_at(tmp_path: Path):
    import datetime as dt
    store = StateStore(tmp_path / "test_done_at.db")
    try:
        ts = TorrentState(source_infohash="4" * 40, state=State.QUEUED)
        assert ts.completed_at is None
        store.upsert(ts)
        before = dt.datetime.now(dt.timezone.utc)
        store.transition(ts, State.DONE)
        assert ts.completed_at is not None
        assert ts.completed_at >= before
        reloaded = store.get("4" * 40)
        assert reloaded is not None
        assert reloaded.completed_at is not None
        assert reloaded.vps1_last_activity_at is None
        # Activity timestamp round-trips too.
        stamp = dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
        reloaded.vps1_last_activity_at = stamp
        store.upsert(reloaded)
        assert store.get("4" * 40).vps1_last_activity_at == stamp
    finally:
        store.close()


def test_state_store_meta(tmp_path: Path):
    db_path = tmp_path / "meta_test.db"
    store = StateStore(db_path)
    try:
        assert store.get_meta("nonexistent") is None
        assert store.get_meta("nonexistent", default="def") == "def"

        store.set_meta("key1", "val1")
        assert store.get_meta("key1") == "val1"

        # Overwrite key
        store.set_meta("key1", "val2")
        assert store.get_meta("key1") == "val2"
    finally:
        store.close()


def test_tombstone_hides_and_refuses_writes(tmp_path: Path):
    """Forget tombstones: invisible to reads, refusing writes, GC'd by age."""
    from racing_sync.coordinator_errors import AbandonedError

    store = StateStore(tmp_path / "s.db")
    try:
        ts = TorrentState(source_infohash="t" * 40, source_name="Tomb",
                          state=State.MOVING)
        store.upsert(ts)
        assert store.tombstone("t" * 40) is True
        assert store.tombstone("t" * 40) is False  # already stamped
        assert store.get("t" * 40) is None
        assert store.get_blob("t" * 40) == b""
        assert store.all() == []
        assert store.all_active() == []
        assert store.find_by_name("Tomb") == []
        assert store.list_by_state(State.MOVING) == []
        # Every write path refuses instead of resurrecting.
        store.upsert(ts)
        assert store.get("t" * 40) is None
        with pytest.raises(AbandonedError):
            store.transition(ts, State.RE_ADDING)
        # Fresh tombstones survive GC; expired ones are hard-deleted.
        assert store.gc_tombstones() == 0
        store._conn.execute(
            "UPDATE torrent_state SET deleted_at = '2000-01-01T00:00:00+00:00' "
            "WHERE source_infohash = ?", ("t" * 40,))
        assert store.gc_tombstones() == 1
    finally:
        store.close()


def test_tombstone_case_insensitive_and_restamp_refreshes(tmp_path: Path):
    """Uppercase forget stamps the canonical row; re-stamp refreshes TTL."""
    from racing_sync.state import _TOMBSTONE_TTL_SECONDS

    store = StateStore(tmp_path / "s.db")
    try:
        ts = TorrentState(source_infohash="ab" * 20, source_name="Case",
                          state=State.NEW)
        store.upsert(ts)
        assert store.tombstone("AB" * 20) is True
        assert store.get("ab" * 20) is None
        # Re-stamp reports False (no live row) but refreshes the stamp.
        store._conn.execute(
            "UPDATE torrent_state SET deleted_at = '2000-01-01T00:00:00+00:00' "
            "WHERE source_infohash = ?", ("ab" * 20,))
        assert store.tombstone("ab" * 20) is False
        assert store.gc_tombstones(ttl_seconds=_TOMBSTONE_TTL_SECONDS) == 0
        # Lowercase clear lifts an uppercase-stamped tombstone.
        assert store.clear_tombstone("AB" * 20) is True
        # None names never crash the lookup.
        assert store.find_by_name(None) == []
    finally:
        store.close()


def test_transition_refuses_concurrent_move(tmp_path: Path):
    """A stale snapshot cannot clobber a state that moved underneath it."""
    from racing_sync.coordinator_errors import AbandonedError

    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="m" * 40, source_name="M",
                                  state=State.NEW))
        stale = store.get("m" * 40)
        fresh = store.get("m" * 40)
        assert stale.version == fresh.version == 0
        store.transition(fresh, State.QUERYING)
        assert store.get("m" * 40).version == 1
        # Stale copy still thinks NEW: its transition must fail loudly,
        # and the DB keeps the winner's state.
        with pytest.raises(AbandonedError):
            store.transition(stale, State.QUEUED)
        assert store.get("m" * 40).state == State.QUERYING
        # In-memory loser is untouched (snapshot restored).
        assert stale.state == State.NEW
    finally:
        store.close()


def test_upsert_bumps_version_and_preserves_it_on_read(tmp_path: Path):
    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="v" * 40, source_name="V",
                                  state=State.NEW))
        assert store.get("v" * 40).version == 0
        ts = store.get("v" * 40)
        ts.source_name = "V2"
        store.upsert(ts)
        assert store.get("v" * 40).version == 1
    finally:
        store.close()


def test_set_vps1_activity_is_narrow(tmp_path: Path):
    """Activity stamp must not clobber concurrent worker fields."""
    import datetime as dt

    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="w" * 40, source_name="W",
                                  state=State.DONE, last_error="boom",
                                  total_bytes=1234))
        when = dt.datetime.now(dt.timezone.utc)
        assert store.set_vps1_activity("w" * 40, when) is True
        row = store.get("w" * 40)
        assert row.vps1_last_activity_at is not None
        assert row.last_error == "boom"
        assert row.total_bytes == 1234
        assert row.state == State.DONE
        assert store.set_vps1_activity("  ") is False
    finally:
        store.close()