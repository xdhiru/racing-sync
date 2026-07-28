from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from racing_sync.clients.abstract import AddResult, Torrent
from racing_sync.clients.http_base import HTTPClientBase, HTTPClientConfig
from racing_sync.coordinator import Coordinator, WebUIUnresponsiveError
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.telegram_bot import render_active, render_detail


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_fetch_retries_sftp_timeout_once():
    """A single SFTP stall is retried; a clean miss is not."""
    coord = object.__new__(Coordinator)
    blob = _single_file_torrent_bytes("Retry.Show.mkv", 50)
    coord.sftp = MagicMock()
    coord.sftp.fetch_torrent = MagicMock(side_effect=[TimeoutError(), blob])
    coord.source_client = MagicMock()
    coord.source_client.export_torrent = AsyncMock(return_value=None)

    out = await coord._fetch_racing_torrent_bytes("a" * 40)
    assert out == blob
    assert coord.sftp.fetch_torrent.call_count == 2
    coord.source_client.export_torrent.assert_not_called()

    coord.sftp.fetch_torrent = MagicMock(return_value=None)
    out = await coord._fetch_racing_torrent_bytes("b" * 40)
    assert out is None
    assert coord.sftp.fetch_torrent.call_count == 1


@pytest.mark.anyio
async def test_fetch_deluge_export_failure_stays_debug(caplog):
    """Deluge has no torrent-file RPC; the failed fallback must not warn."""
    import logging
    from racing_sync.clients.deluge import DelugeClient

    coord = object.__new__(Coordinator)
    coord.sftp = MagicMock()
    coord.sftp.fetch_torrent = MagicMock(return_value=None)
    coord.source_client = MagicMock(spec=DelugeClient)
    coord.source_client.export_torrent = AsyncMock(
        side_effect=Exception("deluge rpc core.get_torrent_file error: {'message': 'Unknown method'}")
    )

    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        out = await coord._fetch_racing_torrent_bytes("c" * 40)
    assert out is None
    assert not [r for r in caplog.records
                if r.levelno >= logging.WARNING and "export failed" in r.message]


@pytest.mark.anyio
async def test_late_fetch_failure_uses_backoff(tmp_path: Path):
    """Unfetchable late seeds back off instead of hammering SFTP every tick."""
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord._target_mount_for = MagicMock(return_value=tmp_path)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=None, accepted=True, detail="Ok.")
    )
    coord.dest_client.export_torrent = AsyncMock(return_value=None)
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=None)
    coord.store = MagicMock()
    coord._failed_late_cross_seeds = {}

    ts = TorrentState(
        source_infohash="d" * 40,
        source_name="Gone.Show",
        dest_infohash="d" * 40,
        injected_private_hashes="",
        state=State.DONE,
    )
    group = [
        Torrent(hash="d" * 40, name="Gone.Show", category="", save_path="",
                size_bytes=10, state="seeding", progress=1.0),
        Torrent(hash="e" * 40, name="Gone.Show", category="", save_path="",
                size_bytes=10, state="seeding", progress=1.0),
    ]

    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert "e" * 40 in coord._failed_late_cross_seeds
    coord.dest_client.add_torrent.assert_not_called()

    # Second tick within backoff: fetch must not even be attempted.
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=None)
    await coord._check_and_inject_late_cross_seeds(ts, group)
    coord._fetch_racing_torrent_bytes.assert_not_called()


def test_state_store_readd_fields_roundtrip(tmp_path: Path):
    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    now = dt.datetime.now(dt.timezone.utc)
    next_retry = now + dt.timedelta(seconds=1800)

    ts = TorrentState(
        source_infohash="readd_hash_1",
        source_name="Test.Release.2026",
        state=State.RE_ADDING,
        readd_first_attempted_at=now,
        readd_next_retry_at=next_retry,
        readd_attempts=3,
    )
    store.upsert(ts)

    loaded = store.get("readd_hash_1")
    assert loaded is not None
    assert loaded.source_infohash == "readd_hash_1"
    assert loaded.state == State.RE_ADDING
    assert loaded.readd_attempts == 3
    assert loaded.readd_first_attempted_at is not None
    assert abs((loaded.readd_first_attempted_at - now).total_seconds()) < 1
    assert loaded.readd_next_retry_at is not None
    assert abs((loaded.readd_next_retry_at - next_retry).total_seconds()) < 1


def test_state_store_migration_adds_readd_columns(tmp_path: Path):
    import sqlite3
    db_path = tmp_path / "legacy.db"
    # Create legacy table without readd columns
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
            injected_private_hashes TEXT NOT NULL DEFAULT '',
            indexer_first_queried_at TEXT NOT NULL DEFAULT '',
            indexer_next_retry_at TEXT NOT NULL DEFAULT '',
            indexer_attempts INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL,
            batch_index INTEGER NOT NULL DEFAULT 0,
            batches_total INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            telegram_message_id INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.close()

    # Opening with StateStore should run _migrate() and add the columns
    store = StateStore(db_path)
    ts = TorrentState(
        source_infohash="legacy_hash",
        source_name="Legacy.Release",
        state=State.RE_ADDING,
        readd_attempts=1,
    )
    store.upsert(ts)
    loaded = store.get("legacy_hash")
    assert loaded is not None
    assert loaded.readd_attempts == 1
    store.close()


@pytest.mark.anyio
async def test_reinject_immediate_retry_succeeds():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 0.001
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._stop = False
    coord.dest_client = AsyncMock()
    coord.store = MagicMock()
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))

    # Attempt 1: TimeoutError; Attempt 2: Success
    coord.dest_client.add_torrent.side_effect = [
        asyncio.TimeoutError("WebUI connection timed out"),
        AddResult(hash=None, accepted=True, detail="Ok."),
    ]

    ts = TorrentState(
        source_infohash="retry_success_hash",
        source_name="My.Movie.2026",
        state=State.RE_ADDING,
        cross_seed_blob=b"torrent_bytes",
    )
    coord.transition = lambda t, s, error="": setattr(t, "state", s)

    await coord._do_re_add(ts)

    assert coord.dest_client.add_torrent.call_count == 2
    assert ts.state == State.DONE
    assert ts.readd_attempts == 2
    assert ts.readd_next_retry_at is None


@pytest.mark.anyio
async def test_reinject_cycle_fails_triggers_30m_backoff():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 0.001
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._stop = False
    coord.dest_client = AsyncMock()
    coord.store = MagicMock()
    coord._schedule_telegram_update = MagicMock()
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))

    # Both attempts fail with TimeoutError
    coord.dest_client.add_torrent.side_effect = [
        asyncio.TimeoutError("WebUI timeout 1"),
        asyncio.TimeoutError("WebUI timeout 2"),
    ]

    ts = TorrentState(
        source_infohash="backoff_hash",
        source_name="My.Series.S01",
        state=State.RE_ADDING,
        cross_seed_blob=b"torrent_bytes",
    )
    transition_called = False
    def mock_transition(t, s, error=""):
        nonlocal transition_called
        transition_called = True
        setattr(t, "state", s)
    coord.transition = mock_transition

    await coord._do_re_add(ts)

    assert coord.dest_client.add_torrent.call_count == 2
    # Must NOT fail! Must remain RE_ADDING!
    assert not transition_called
    assert ts.state == State.RE_ADDING
    assert ts.readd_attempts == 2
    assert ts.readd_next_retry_at is not None
    now_utc = dt.datetime.now(dt.timezone.utc)
    remaining_sec = (ts.readd_next_retry_at - now_utc).total_seconds()
    assert 1700 <= remaining_sec <= 1810
    coord._schedule_telegram_update.assert_called_once_with(ts)


@pytest.mark.anyio
async def test_reinject_24h_hard_deadline_fails():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.dest_client = AsyncMock()
    coord.store = MagicMock()

    now = dt.datetime.now(dt.timezone.utc)
    ts = TorrentState(
        source_infohash="deadline_hash",
        source_name="Old.Dead.Torrent",
        state=State.RE_ADDING,
        readd_first_attempted_at=now - dt.timedelta(seconds=86405),  # > 24h
    )

    failed_error = ""
    def mock_transition(t, s, error=""):
        nonlocal failed_error
        failed_error = error
        setattr(t, "state", s)
    coord.transition = mock_transition

    await coord._do_re_add(ts)

    assert ts.state == State.FAILED
    assert "re-injection timed out after" in failed_error
    assert ">24h limit" in failed_error
    assert coord.dest_client.add_torrent.call_count == 0


@pytest.mark.anyio
async def test_reinject_tick_skips_backed_off_torrent():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord._tasks = set()
    coord._running_infohashes = set()
    coord.watch = None
    coord.store = MagicMock()
    coord.store.list_indexer_ready.return_value = []
    coord._check_and_inject_late_cross_seeds = AsyncMock()
    coord._live = {}

    now = dt.datetime.now(dt.timezone.utc)

    # Torrent 1: In RE_ADDING with backoff timer in the future (skip!)
    ts_backed_off = TorrentState(
        source_infohash="backed_off",
        source_name="Backed.Off.Torrent",
        state=State.RE_ADDING,
        readd_next_retry_at=now + dt.timedelta(seconds=1200),
    )

    # Torrent 2: In RE_ADDING with timer elapsed (ready!)
    ts_ready = TorrentState(
        source_infohash="ready_to_retry",
        source_name="Ready.Torrent",
        state=State.RE_ADDING,
        readd_next_retry_at=now - dt.timedelta(seconds=10),
    )

    coord.store.all_active.return_value = [ts_backed_off, ts_ready]

    scheduled: list[str] = []
    async def fake_process(ts: TorrentState):
        scheduled.append(ts.source_infohash)
    coord._process_torrent = fake_process

    # Call _tick
    # Mock _list_source_torrents to return empty list
    coord._list_source_torrents = AsyncMock(return_value=[])

    await coord._tick()

    # Give created tasks a micro-turn to start
    await asyncio.sleep(0.01)

    assert "backed_off" not in scheduled
    assert "ready_to_retry" in scheduled


def test_telegram_messages_show_reinject_retry_countdown():
    now = dt.datetime.now(dt.timezone.utc)
    ts = TorrentState(
        source_infohash="tg_hash",
        source_name="Cyberpunk.Edgerunners.S01",
        state=State.RE_ADDING,
        readd_next_retry_at=now + dt.timedelta(minutes=25),
    )

    # 1. Detail message formatting
    detail_msg = render_detail(ts)
    assert "Re-adding on fuse mount (WebUI busy, retrying in 25m)" in detail_msg

    # 2. Active tasks status message formatting
    status_msg, _, _ = render_active(
        active=[(ts, None)],
        page=0,
        page_size=5,
    )
    assert "🔄 Re-adding (retry in 25m)" in status_msg


@pytest.mark.anyio
async def test_http_base_request_retries_on_timeout_and_connector_error():
    cfg = HTTPClientConfig(host="http://127.0.0.1:8080")
    client = HTTPClientBase(cfg, label="test-client")
    client._authed = True

    mock_session = MagicMock()
    client._session = mock_session

    call_count = 0
    async def mock_request(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise asyncio.TimeoutError("Simulated timeout")
        if call_count == 2:
            raise aiohttp.ClientConnectorError(
                connection_key=MagicMock(), os_error=OSError("Simulated connector error")
            )
        # Succeeded on attempt 3
        resp = MagicMock()
        resp.status = 200
        return resp

    mock_session.request = mock_request

    with patch("asyncio.sleep", AsyncMock()):
        resp = await client.request("GET", "/api/v2/test")
        assert resp.status == 200
        assert call_count == 3


@pytest.mark.anyio
async def test_await_hash_for_name_normalizes_to_lower():
    coord = object.__new__(Coordinator)
    coord.dest_client = MagicMock()
    t = Torrent(
        hash="ABCD1234EF",
        name="Test.Movie.2026",
        category="",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
    )
    coord.dest_client.list_torrents = AsyncMock(return_value=[t])
    h = await coord._await_hash_for_name("Test.Movie.2026")
    assert h == "abcd1234ef"


@pytest.mark.anyio
async def test_re_inject_racing_torrents_case_insensitive():
    coord = object.__new__(Coordinator)
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse/Test.Movie.2026"))
    coord.dest_client = MagicMock()
    coord.dest_client.add_torrent = AsyncMock()
    coord._fetch_racing_torrent_bytes = AsyncMock()

    # ts already has lowercase injected hash
    ts = TorrentState(
        source_infohash="src_1",
        source_name="Test.Movie.2026",
        injected_private_hashes="abcd1234ef",
    )

    # racing client returns uppercase hash for the same release
    t_upper = Torrent(
        hash="ABCD1234EF",
        name="Test.Movie.2026",
        category="",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
    )
    coord._list_source_torrents = AsyncMock(return_value=[t_upper])

    await coord._re_inject_racing_torrents(ts)

    # Should recognize ABCD1234EF is already in abcd1234ef and skip re-injection
    coord.dest_client.add_torrent.assert_not_called()
    coord._fetch_racing_torrent_bytes.assert_not_called()


@pytest.mark.anyio
async def test_check_and_inject_late_cross_seeds_normalizes_hash(tmp_path: Path):
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    fname = "Test.Movie.2026.mkv"
    fsize = 1000
    (fuse_dir / fname).write_bytes(b"m" * fsize)
    blob = _single_file_torrent_bytes(fname, fsize)
    coord = object.__new__(Coordinator)
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = MagicMock()
    coord.dest_client.add_torrent = AsyncMock(return_value=AddResult(hash="new_h", accepted=True))
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=blob)
    coord.store = MagicMock()

    ts = TorrentState(
        source_infohash="src_1",
        source_name="Test.Movie.2026",
        injected_private_hashes="",
    )

    t_upper = Torrent(
        hash="LATE1234EF",
        name="Test.Movie.2026",
        category="",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
    )

    await coord._check_and_inject_late_cross_seeds(ts, [t_upper])

    # Injected hash should be stored in lowercase
    assert ts.injected_private_hashes == "late1234ef"
    coord.store.upsert.assert_called_once_with(ts)


@pytest.mark.anyio
async def test_do_moving_parks_when_pause_fails(tmp_path: Path):
    from racing_sync.clients.abstract import TorrentFile
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files.return_value = [
        TorrentFile(name="Movie.mkv", size_bytes=1000, progress=1.0)
    ]
    coord.dest_client.pause.side_effect = RuntimeError("qB webui error")
    coord._rclone_move = AsyncMock()

    ts = TorrentState(
        source_infohash="hash_pause_fail",
        source_name="Movie",
        save_path=str(tmp_path),
        state=State.MOVING,
    )

    # Transient pause failure must NOT raise/FAILED (would waste SSD bytes);
    # it parks in MOVING for the next tick to retry.
    await coord._do_moving(ts)
    assert ts.state == State.MOVING
    # Rclone move must NOT proceed while the client is still writing.
    coord._rclone_move.assert_not_called()
    coord.store.upsert.assert_called()


@pytest.mark.anyio
async def test_do_re_add_timer_delay_parks_without_sleeping():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 30  # > 5s -> timer pattern
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()

    ts = TorrentState(
        source_infohash="timer_test_hash",
        source_name="Timer.Show.2026",
        state=State.RE_ADDING,
        readd_attempts=0,
        readd_next_retry_at=None,
    )

    await coord._do_re_add(ts)

    # Must set readd_next_retry_at into the future and return without adding torrents
    assert ts.readd_next_retry_at is not None
    coord.dest_client.add_torrent.assert_not_called()
    coord.store.upsert.assert_called()


@pytest.mark.anyio
async def test_re_add_cross_seed_missing_blob_fails():
    coord = object.__new__(Coordinator)
    coord.store = MagicMock()
    coord.store.get_blob.return_value = None  # No blob
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda ts, s, error="": setattr(ts, "state", s))

    ts = TorrentState(
        source_infohash="missing_blob_hash",
        source_name="No.Blob.Show",
        cross_seed_blob=b"",
        state=State.RE_ADDING,
    )

    await coord._re_add_cross_seed_torrent(ts)

    # Must transition to FAILED, NOT silently succeed
    assert ts.state == State.FAILED
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.FAILED


def _single_file_torrent_bytes(name: str, length: int) -> bytes:
    from racing_sync.watchdir import _bencode
    return _bencode({
        b"announce": b"http://tracker.example/announce",
        b"info": {
            b"name": name.encode("utf-8"),
            b"length": length,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })


@pytest.mark.anyio
async def test_do_re_add_parks_when_fuse_content_missing(tmp_path: Path):
    """SSD-complete data that never reached the fuse mount must NOT be injected."""
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 120
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord._stop = False
    coord.dest_client = AsyncMock()
    coord.store = MagicMock()
    fuse_dir = tmp_path / "fuse-empty"
    fuse_dir.mkdir()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="gate_park_hash",
        source_name="Gated.Movie.2026",
        state=State.RE_ADDING,
        cross_seed_blob=_single_file_torrent_bytes("Gated.Movie.2026.mkv", 100),
    )

    await coord._do_re_add(ts)

    # No blind injection, no DONE — parked for retry with a timer.
    coord.dest_client.add_torrent.assert_not_called()
    assert ts.state == State.RE_ADDING
    assert ts.readd_next_retry_at is not None
    coord.store.upsert.assert_called()


@pytest.mark.anyio
async def test_do_re_add_proceeds_when_fuse_content_present(tmp_path: Path):
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 120
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._stop = False
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent.return_value = AddResult(hash=None, accepted=True, detail="Ok.")
    coord.store = MagicMock()
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Gated.Movie.2026.mkv").write_bytes(b"x" * 100)
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="gate_pass_hash",
        source_name="Gated.Movie.2026",
        state=State.RE_ADDING,
        cross_seed_blob=_single_file_torrent_bytes("Gated.Movie.2026.mkv", 100),
    )

    await coord._do_re_add(ts)

    assert coord.dest_client.add_torrent.call_count == 1
    assert ts.state == State.DONE


@pytest.mark.anyio
async def test_do_moving_persists_blob_for_adopted_rows_without_one(tmp_path: Path):
    """Fresh-DB adoption (recovery) creates MOVING rows with no .torrent blob.

    _do_moving deletes the SSD client entry before RE_ADDING, so it must
    persist the torrent bytes first — otherwise _re_add_cross_seed_torrent
    fails and the fuse gate has nothing to verify against.
    """
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.watchdir import _bencode

    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()
    (ssd_dir / "Adopted.Movie.2026.mkv").write_bytes(b"y" * 64)
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    blob = _bencode({
        b"announce": b"http://tracker.example/announce",
        b"info": {
            b"name": b"Adopted.Movie.2026.mkv",
            b"length": 64,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd_dir
    coord.cfg.ssd.path = ssd_dir
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.store = store
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Adopted.Movie.2026.mkv", size_bytes=64, progress=1.0),
    ])
    coord.dest_client.export_torrent = AsyncMock(return_value=blob)
    coord._rclone_move = AsyncMock(
        return_value=MagicMock(ok=True, returncode=0, stdout="", stderr="")
    )
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    # Adopted-style row: hashes known, but no blob anywhere (fresh DB).
    ts = TorrentState(
        source_infohash="d" * 40,
        source_name="Adopted.Movie.2026",
        dest_infohash="d" * 40,
        save_path=str(ssd_dir),
        total_bytes=64,
        state=State.MOVING,
    )
    store.upsert(ts)
    assert not store.get_blob("d" * 40)

    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        await coord._do_moving(ts)

    assert ts.state == State.RE_ADDING
    assert store.get_blob("d" * 40) == blob


@pytest.mark.anyio
async def test_do_queued_fuse_fast_track_persists_classification(tmp_path: Path):
    """A season pack fast-tracked QUEUED->DONE must remember kind=season.

    Without this the row keeps kind="unknown" (-> unsorted mount) while its
    bytes live at the default mount, so every later RE_ADDING/late-seed fuse
    gate checks the wrong directory and parks forever (Chad/Harbor.Lights case).
    """
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    (fuse_dir / "Pack.S01").mkdir(parents=True)
    (fuse_dir / "Pack.S01" / "Pack.S01E01.mkv").write_bytes(b"a" * 100)
    (fuse_dir / "Pack.S01" / "Pack.S01E02.mkv").write_bytes(b"b" * 100)
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    season_files = [
        TorrentFile(name="Pack.S01/Pack.S01E01.mkv", size_bytes=100, progress=1.0),
        TorrentFile(name="Pack.S01/Pack.S01E02.mkv", size_bytes=100, progress=1.0),
    ]

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd_dir
    coord.cfg.ssd.path = ssd_dir
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = fuse_dir / "unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    ext = Torrent(
        hash="b" * 40,
        name="Pack.S01",
        category="racing",
        save_path=str(fuse_dir),
        size_bytes=200,
        state="seeding",
        progress=1.0,
    )
    coord.dest_client.list_torrents = AsyncMock(return_value=[ext])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=season_files)
    coord._list_source_torrents = AsyncMock(return_value=[])
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="b" * 40,
        source_name="Pack.S01",
        save_path=str(ssd_dir),
        state=State.QUEUED,
    )
    assert ts.classification_kind == "unknown"

    await coord._do_queued(ts)

    assert ts.state == State.DONE
    assert ts.classification_kind == "season"
    assert coord._target_mount_for(ts) == fuse_dir


@pytest.mark.anyio
async def test_do_queued_retries_files_listing_after_add(tmp_path: Path):
    """A 404 right after add (loaded client registration lag) must not FAILED."""
    from unittest.mock import patch
    from racing_sync.clients.abstract import AddResult, TorrentFile

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = str(tmp_path / "fuse")
    coord.cfg.rclone.fuse.mount_unsorted = str(tmp_path / "fuse-unsorted")
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="ab" * 20, accepted=True, detail="Ok.")
    )
    movie = [TorrentFile(name="Queued.Movie.2026.mkv", size_bytes=100, progress=0.0)]
    coord.dest_client.get_torrent_files = AsyncMock(
        side_effect=[RuntimeError("404 Not Found"), RuntimeError("404 Not Found"), movie]
    )
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="c" * 40, source_name="Queued.Movie.2026",
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await coord._do_queued(ts)

    assert coord.dest_client.get_torrent_files.call_count == 3
    assert ts.state == State.DOWNLOADING
    assert ts.dest_infohash == "ab" * 20


@pytest.mark.anyio
async def test_do_queued_persistent_files_failure_stays_queued(tmp_path: Path):
    """If the files endpoint never recovers, park in QUEUED (not FAILED).

    dest_infohash must be persisted so the next tick re-enters through the
    existing-torrent check instead of re-adding a duplicate.
    """
    from unittest.mock import patch
    from racing_sync.clients.abstract import AddResult

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="ab" * 20, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent_files = AsyncMock(side_effect=RuntimeError("boom"))
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="c" * 40, source_name="Queued.Movie.2026",
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        await coord._do_queued(ts)

    assert ts.state == State.QUEUED
    assert ts.dest_infohash == "ab" * 20
    coord.store.upsert.assert_called()
    failed = [c for c in coord.transition.call_args_list if c[0][1] == State.FAILED]
    assert failed == []


@pytest.mark.anyio
async def test_do_queued_existing_resume_failure_stays_queued(tmp_path: Path):
    from racing_sync.clients.abstract import Torrent

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.fuse.mount = str(tmp_path / "fuse")
    coord.cfg.rclone.fuse.mount_unsorted = str(tmp_path / "fuse-unsorted")
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    ext = Torrent(
        hash="d" * 40, name="Existing.Show", category="racing",
        save_path=str(tmp_path), size_bytes=100, state="downloading",
        progress=0.5,
    )
    coord.dest_client.list_torrents = AsyncMock(return_value=[ext])
    coord.dest_client.resume = AsyncMock(side_effect=RuntimeError("qB busy"))
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="d" * 40, source_name="Existing.Show",
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    await coord._do_queued(ts)

    assert ts.state == State.QUEUED
    assert ts.dest_infohash == "d" * 40
    coord.store.upsert.assert_called()


@pytest.mark.anyio
async def test_do_queued_parks_to_readding_when_fuse_files_missing(tmp_path: Path):
    """A fuse-complete entry with missing bytes must NOT mark DONE — nor FAILED.

    FAILED would trigger re-downloads when the mount is merely warming; the
    row goes to RE_ADDING where the fuse gate + backoff machinery handles it.
    """
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd_dir
    coord.cfg.ssd.path = ssd_dir
    coord.cfg.rclone.fuse.mount = str(fuse_dir)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse_dir / "unsorted")
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    ext = Torrent(
        hash="e" * 40,
        name="Ghost.Show.S01",
        category="racing",
        save_path=str(fuse_dir),
        size_bytes=100,
        state="seeding",
        progress=1.0,
    )
    coord.dest_client.list_torrents = AsyncMock(return_value=[ext])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Ghost.Show.S01/S01E01.mkv", size_bytes=100, progress=1.0),
    ])
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="f" * 40,
        source_name="Ghost.Show.S01",
        cross_seed_blob=b"fake-blob",
        save_path=str(ssd_dir),
        state=State.QUEUED,
    )

    await coord._do_queued(ts)

    assert ts.state == State.RE_ADDING
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.RE_ADDING
    # No blind re-injection of matches.
    coord.dest_client.add_torrent.assert_not_called()


@pytest.mark.anyio
async def test_do_queued_resumes_ssd_flow_when_fuse_entry_missing_but_ssd_has_files(tmp_path: Path):
    """Fuse-pointing entry + bytes on SSD = never-moved data: drive SSD flow."""
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()
    (ssd_dir / "Ghost.Movie.2026.mkv").write_bytes(b"g" * 100)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd_dir
    coord.cfg.ssd.path = ssd_dir
    coord.cfg.rclone.fuse.mount = str(fuse_dir)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse_dir / "unsorted")
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    ext = Torrent(
        hash="e" * 40,
        name="Ghost.Movie.2026",
        category="racing",
        save_path=str(fuse_dir),
        size_bytes=100,
        state="seeding",
        progress=1.0,
    )
    coord.dest_client.list_torrents = AsyncMock(return_value=[ext])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Ghost.Movie.2026.mkv", size_bytes=100, progress=1.0),
    ])
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="f" * 40,
        source_name="Ghost.Movie.2026",
        cross_seed_blob=b"fake-blob",
        save_path=str(fuse_dir),
        state=State.QUEUED,
    )

    await coord._do_queued(ts)

    # SSD flow resumes (DOWNLOADING re-polls, then MOVING moves SSD bytes).
    assert ts.state == State.DOWNLOADING
    assert ts.save_path == str(ssd_dir)
    assert ts.dest_infohash == "e" * 40
    coord.dest_client.add_torrent.assert_not_called()


@pytest.mark.anyio
async def test_late_cross_seeds_defer_when_fuse_content_missing(tmp_path: Path):
    """Regression: DONE row + unknown VPS1 matches + empty fuse => defer, not inject.

    Mirrors the reported incident ("detected late cross-seed ..." followed by
    blind "auto-injected ... onto fuse" while bytes were still on SSD).
    """
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=None, accepted=True, detail="Ok.")
    )
    coord.store = MagicMock()
    coord._failed_late_cross_seeds = {}

    priv_blobs = [
        _single_file_torrent_bytes("Late.Show.S01E01.mkv", 700 + i)
        for i in range(3)
    ]
    coord._fetch_racing_torrent_bytes = AsyncMock(side_effect=list(priv_blobs))

    ts = TorrentState(
        source_infohash="a" * 40,
        source_name="Late.Show.S01E01",
        dest_infohash="a" * 40,
        cross_seed_infohash="a" * 40,
        injected_private_hashes="",
        state=State.DONE,
    )
    from racing_sync.watchdir import _bencoded_info_hash
    group = [
        Torrent(hash="a" * 40, name="Late.Show.S01E01", category="",
                save_path="", size_bytes=700, state="seeding", progress=1.0),
    ]
    for blob in priv_blobs:
        h = _bencoded_info_hash(blob)[0].lower()
        group.append(Torrent(hash=h, name="Late.Show.S01E01", category="",
                             save_path="", size_bytes=700, state="seeding", progress=1.0))

    await coord._check_and_inject_late_cross_seeds(ts, group)

    # Nothing injected; all three deferred with backoff (no per-tick storm).
    coord.dest_client.add_torrent.assert_not_called()
    assert ts.injected_private_hashes == ""
    assert len(coord._failed_late_cross_seeds) == 3
    coord.store.upsert.assert_not_called()


@pytest.mark.anyio
async def test_re_inject_racing_torrents_replaces_stale_ssd_entry(tmp_path: Path):
    """A duplicate hash pointing at SSD (not fuse) must be replaced, not marked.

    This is the fresh-DB trap: the SSD torrent still exists when RE_ADDING
    runs, so qB reports "Fails." — marking it injected would leave a
    fuse-claimed torrent seeding from SSD (or broken), with no fuse entry.
    """
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()
    (fuse_dir / "Stale.Movie.2026.mkv").write_bytes(b"z" * 80)

    blob = _single_file_torrent_bytes("Stale.Movie.2026.mkv", 80)
    from racing_sync.watchdir import _bencoded_info_hash
    real_hash = _bencoded_info_hash(blob)[0].lower()

    coord = object.__new__(Coordinator)
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = AsyncMock()
    # First add fails (duplicate); second add after replace succeeds.
    coord.dest_client.add_torrent = AsyncMock(side_effect=[
        AddResult(hash=None, accepted=False, detail="Fails."),
        AddResult(hash=None, accepted=True, detail="Ok."),
    ])
    ssd_entry = Torrent(
        hash=real_hash, name="Stale.Movie.2026", category="racing",
        save_path=str(ssd_dir), size_bytes=80, state="seeding", progress=1.0,
    )
    fuse_entry = Torrent(
        hash=real_hash, name="Stale.Movie.2026", category="racing",
        save_path=str(fuse_dir), size_bytes=80, state="seeding", progress=1.0,
    )
    coord.dest_client.get_torrent = AsyncMock(side_effect=[ssd_entry, fuse_entry])
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=blob)

    ts = TorrentState(
        source_infohash="src_stale_1",
        source_name="Stale.Movie.2026",
        injected_private_hashes="",
    )
    t_match = Torrent(
        hash=real_hash, name="Stale.Movie.2026", category="",
        save_path="", size_bytes=80, state="racing", progress=1.0,
    )
    coord._list_source_torrents = AsyncMock(return_value=[t_match])

    await coord._re_inject_racing_torrents(ts)

    # Stale SSD entry deleted (files kept), fuse entry added + recorded.
    coord.dest_client.delete.assert_awaited_once_with(real_hash, delete_files=False)
    assert coord.dest_client.add_torrent.await_count == 2
    assert ts.injected_private_hashes == real_hash


@pytest.mark.anyio
async def test_re_inject_racing_torrents_skips_missing_fuse_content(tmp_path: Path):
    coord = object.__new__(Coordinator)
    fuse_dir = tmp_path / "fuse-empty"
    fuse_dir.mkdir()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = MagicMock()
    coord.dest_client.add_torrent = AsyncMock()
    coord._fetch_racing_torrent_bytes = AsyncMock(
        return_value=_single_file_torrent_bytes("Late.Ep.mkv", 50)
    )

    ts = TorrentState(
        source_infohash="src_gate_1",
        source_name="Late.Show.S01",
        injected_private_hashes="",
    )

    t_match = Torrent(
        hash="a" * 40,
        name="Late.Show.S01",
        category="",
        save_path="",
        size_bytes=50,
        state="racing",
        progress=1.0,
    )
    coord._list_source_torrents = AsyncMock(return_value=[t_match])

    await coord._re_inject_racing_torrents(ts)

    # Content absent at fuse target -> no blind injection, hash not recorded.
    coord.dest_client.add_torrent.assert_not_called()
    assert ts.injected_private_hashes == ""


@pytest.mark.anyio
async def test_do_queued_batches_non_episodic_bundle_in_file_groups(tmp_path: Path):
    """Multi-file movie bundles (games, complete packs) stream in file groups.

    Total size must NOT disqualify content: only an individual file bigger
    than the cap refuses the torrent.
    """
    from racing_sync.clients.abstract import AddResult, TorrentFile

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 10_000
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = str(tmp_path / "fuse")
    coord.cfg.rclone.fuse.mount_unsorted = str(tmp_path / "fuse-unsorted")
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="dd" * 20, accepted=True, detail="Ok.")
    )
    files = [
        TorrentFile(name="Pack/A.Level1.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/B.Level2.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/C.Level3.bin", size_bytes=4000, progress=0.0),
    ]
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="e" * 40, source_name="Pack", total_bytes=12000,
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    await coord._do_queued(ts)

    assert ts.state == State.DOWNLOADING
    assert ts.classification_kind == "movie"
    assert ts.batches_total == 2
    prio_map = coord.dest_client.set_file_priorities.call_args[0][1]
    assert prio_map["Pack/A.Level1.bin"] == 1
    assert prio_map["Pack/B.Level2.bin"] == 1
    assert prio_map["Pack/C.Level3.bin"] == 0


@pytest.mark.anyio
async def test_do_queued_fails_single_oversize_member_not_total(tmp_path: Path):
    """One member bigger than the cap fails the torrent, however small the rest."""
    from racing_sync.clients.abstract import AddResult, TorrentFile

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100_000_000_000
    coord.cfg.ssd.skip_movie_larger_than_bytes = 10_000
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="dd" * 20, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Pack/ok.bin", size_bytes=1000, progress=0.0),
        TorrentFile(name="Pack/huge.bin", size_bytes=50_000, progress=0.0),
    ])
    coord.dest_client.delete = AsyncMock()
    def _set_state(t, s, error="", **kwargs):
        t.state = s
        t.last_error = error
    coord.transition = MagicMock(side_effect=_set_state)

    ts = TorrentState(
        source_infohash="e" * 40, source_name="Pack", total_bytes=51000,
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    await coord._do_queued(ts)

    assert ts.state == State.FAILED
    assert "skip threshold" in ts.last_error
    assert "Pack/huge.bin" in ts.last_error
    coord.dest_client.delete.assert_awaited_once_with("dd" * 20, delete_files=True)


@pytest.mark.anyio
async def test_ensure_fuse_entry_retries_delayed_visibility_after_replace(tmp_path: Path):
    """A re-add accepted by qB but invisible to immediate lookups (10k-torrent
    registration lag) must be confirmed via retry, not reported as rejected."""
    from unittest.mock import patch
    from racing_sync.watchdir import _bencoded_info_hash

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    blob = _single_file_torrent_bytes("Lag.Movie.2026.mkv", 80)
    real_hash = _bencoded_info_hash(blob)[0].lower()

    coord = object.__new__(Coordinator)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent = AsyncMock(side_effect=[
        AddResult(hash=None, accepted=False, detail="Fails."),
        AddResult(hash=None, accepted=True, detail="Ok."),
    ])
    ssd_entry = Torrent(
        hash=real_hash, name="Lag.Movie.2026", category="racing",
        save_path=str(ssd_dir), size_bytes=80, state="seeding", progress=1.0,
    )
    fuse_entry = Torrent(
        hash=real_hash, name="Lag.Movie.2026", category="racing",
        save_path=str(fuse_dir), size_bytes=80, state="seeding", progress=1.0,
    )
    # First lookup: stale SSD entry. Post-replace lookups: invisible twice
    # (registration lag), then the fuse entry.
    coord.dest_client.get_torrent = AsyncMock(
        side_effect=[ssd_entry, None, None, fuse_entry]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        ok, _ = await coord._ensure_fuse_entry(
            blob=blob, infohash=real_hash, target_mount=fuse_dir,
            label="cross-seed torrent",
        )

    assert ok is True
    coord.dest_client.delete.assert_awaited_once_with(real_hash, delete_files=False)


@pytest.mark.anyio
async def test_ensure_fuse_entry_unconfirmed_replace_is_retryable(tmp_path: Path):
    """A replace accepted but never visible must NOT read as a rejection.

    Returns the sentinel detail so callers park/retry instead of FAILED —
    the pre-fix code returned (False, "Ok."), which failed the row with the
    absurd message "fuse re-add rejected: Ok."
    """
    from unittest.mock import patch
    from racing_sync.coordinator import _NOT_VISIBLE_DETAIL
    from racing_sync.watchdir import _bencoded_info_hash

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    blob = _single_file_torrent_bytes("Ghost.Movie.2026.mkv", 80)
    real_hash = _bencoded_info_hash(blob)[0].lower()

    coord = object.__new__(Coordinator)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent = AsyncMock(side_effect=[
        AddResult(hash=None, accepted=False, detail="Fails."),
        AddResult(hash=None, accepted=True, detail="Ok."),
    ])
    ssd_entry = Torrent(
        hash=real_hash, name="Ghost.Movie.2026", category="racing",
        save_path=str(ssd_dir), size_bytes=80, state="seeding", progress=1.0,
    )
    coord.dest_client.get_torrent = AsyncMock(
        side_effect=[ssd_entry, None, None, None, None]
    )

    with patch("asyncio.sleep", new_callable=AsyncMock):
        ok, detail = await coord._ensure_fuse_entry(
            blob=blob, infohash=real_hash, target_mount=fuse_dir,
            label="cross-seed torrent",
        )

    assert ok is False
    assert detail == _NOT_VISIBLE_DETAIL


@pytest.mark.anyio
async def test_re_add_cross_seed_unconfirmed_replace_retries_not_fails(tmp_path: Path):
    """The S36E174 incident: accepted-but-invisible re-add must raise the
    retryable error (RE_ADDING parks) instead of FAILED."""
    from racing_sync.coordinator import WebUIUnresponsiveError

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = tmp_path / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-unsorted"
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    blob = _single_file_torrent_bytes("Retry.Movie.2026.mkv", 80)
    from racing_sync.watchdir import _bencoded_info_hash
    real_hash = _bencoded_info_hash(blob)[0].lower()

    ts = TorrentState(
        source_infohash="src_retry_1", source_name="Retry.Movie.2026",
        dest_infohash=real_hash, cross_seed_infohash=real_hash,
        cross_seed_blob=blob, state=State.RE_ADDING,
    )
    coord._ensure_fuse_entry = AsyncMock(return_value=(False, "added but entry not yet visible on dest client"))

    with pytest.raises(WebUIUnresponsiveError):
        await coord._re_add_cross_seed_torrent(ts)
    assert ts.state == State.RE_ADDING
    coord.transition.assert_not_called()

