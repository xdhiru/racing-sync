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
