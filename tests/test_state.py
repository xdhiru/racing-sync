from __future__ import annotations

from pathlib import Path
import pytest

from racing_sync.state import State, StateStore, TorrentState, check_transition


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_done_can_only_transition_to_re_adding():
    check_transition(State.DONE, State.RE_ADDING)
    for s in [State.NEW, State.QUEUED, State.DOWNLOADING, State.MOVING,
              State.WAITING_DISK, State.QUERYING, State.FAILED]:
        with pytest.raises(ValueError):
            check_transition(State.DONE, s)


def test_failed_can_retry_to_queued_and_new():
    check_transition(State.FAILED, State.QUEUED)
    check_transition(State.FAILED, State.NEW)


def test_new_cannot_go_directly_to_done():
    with pytest.raises(ValueError):
        check_transition(State.NEW, State.DONE)


def test_new_can_go_to_any_inflight():
    for s in [State.QUERYING, State.WAITING_DISK, State.QUEUED,
              State.DOWNLOADING, State.MOVING, State.RE_ADDING]:
        check_transition(State.NEW, s)


def test_downloading_to_moving_is_ok():
    check_transition(State.DOWNLOADING, State.MOVING)


def test_downloading_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.DOWNLOADING, State.DONE)


def test_moving_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.MOVING, State.DONE)


def test_seedpool_park_and_retry():
    check_transition(State.NEW, State.WAITING_SEEDPOOL)
    check_transition(State.QUERYING, State.WAITING_SEEDPOOL)
    check_transition(State.WAITING_SEEDPOOL, State.QUERYING)
    check_transition(State.WAITING_SEEDPOOL, State.QUEUED)
    check_transition(State.WAITING_SEEDPOOL, State.FAILED)


def test_seedpool_no_self_transition():
    # Re-parking bumps fields but should not go through transition().
    # The state machine still treats WAITING_SEEDPOOL -> WAITING_SEEDPOOL
    # as illegal; callers must upsert() instead.
    with pytest.raises(ValueError):
        check_transition(State.WAITING_SEEDPOOL, State.WAITING_SEEDPOOL)


def test_queued_to_done_is_allowed():
    # When a torrent is already complete/seeding on VPS2, QUEUED fast-tracks to DONE.
    check_transition(State.QUEUED, State.DONE)


def test_find_by_name_extension_matching(tmp_path):
    from racing_sync.state import StateStore, TorrentState

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    # Stored without .mkv (as Seedpool or VPS2 client might report)
    ts = TorrentState(
        source_infohash="hash123",
        source_name="Game.Day.Murders.S01E06.1080p-Kitsune",
        state=State.DONE,
    )
    store.upsert(ts)

    # Query with .mkv (as Aither reports)
    matches = store.find_by_name("Game.Day.Murders.S01E06.1080p-Kitsune.mkv")
    assert len(matches) == 1
    assert matches[0].source_infohash == "hash123"

    # Query exact without .mkv
    matches2 = store.find_by_name("Game.Day.Murders.S01E06.1080p-Kitsune")
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
    from racing_sync.coordinator import Coordinator
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

    coord = object.__new__(Coordinator)
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
            seedpool_next_retry_at=past,
            seedpool_attempts=5,
        )
        store.upsert(ts)
        store.transition(ts, State.NEW)
        assert ts.seedpool_next_retry_at is None
        assert ts.seedpool_attempts == 0
    finally:
        store.close()


def test_migrate_handles_old_schema_without_telegram_or_seedpool(tmp_path: Path):
    import sqlite3
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE torrent_state (
            source_infohash TEXT PRIMARY KEY,
            dest_infohash TEXT NOT NULL DEFAULT '',
            source_name TEXT NOT NULL DEFAULT '',
            source_tracker TEXT NOT NULL DEFAULT '',
            source_announce_url TEXT NOT NULL DEFAULT '',
            classification_kind TEXT NOT NULL DEFAULT 'unknown',
            total_bytes INTEGER NOT NULL DEFAULT 0,
            save_path TEXT NOT NULL DEFAULT '',
            cross_seed_infohash TEXT NOT NULL DEFAULT '',
            cross_seed_source TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT 'new',
            batch_index INTEGER NOT NULL DEFAULT 0,
            batches_total INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        INSERT INTO torrent_state (source_infohash, created_at, updated_at)
        VALUES ('3333333333333333333333333333333333333333', '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')
    """)
    conn.commit()
    conn.close()

    # Opening StateStore on existing legacy DB runs _migrate()
    store = StateStore(db_path)
    try:
        row = store.get("3333333333333333333333333333333333333333")
        assert row is not None
        assert row.telegram_message_id == 0
        assert row.seedpool_attempts == 0
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