"""Tests for the seedpool retry policy math.

We don't run the full coordinator here (it needs live qBittorrent +
Prowlarr). Instead we verify the timing rules that _park_for_seedpool_retry
applies.
"""

from __future__ import annotations

import datetime as dt
import pytest

from racing_sync.config import AppConfig
from racing_sync.state import State, TorrentState


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _cfg() -> AppConfig:
    return AppConfig.from_toml(
        __file__.replace("\\", "/").rsplit("/", 1)[0] + "/../config.example.toml"
    )


def test_first_attempt_sets_first_queried_at():
    cfg = _cfg()
    ts = TorrentState(source_infohash="a" * 40, state=State.NEW)
    assert ts.seedpool_first_queried_at is None
    assert ts.seedpool_attempts == 0

    now = dt.datetime.now(dt.timezone.utc)
    interval = cfg.cross_seed.seedpool_retry_interval_seconds
    ts.seedpool_first_queried_at = now
    ts.seedpool_attempts = 1
    ts.seedpool_next_retry_at = now + dt.timedelta(seconds=interval)

    assert ts.seedpool_first_queried_at == now
    assert ts.seedpool_next_retry_at is not None
    diff = (ts.seedpool_next_retry_at - now).total_seconds()
    assert abs(diff - interval) < 1


def test_retry_window_is_24_hours():
    cfg = _cfg()
    assert cfg.cross_seed.seedpool_max_age_seconds == 86400
    assert cfg.cross_seed.seedpool_retry_interval_seconds == 1800


def test_expired_max_age_marks_failed():
    """If the first attempt was > 24 h ago, the next park should escalate
    to FAILED. We simulate by backdating seedpool_first_queried_at."""
    cfg = _cfg()
    ts = TorrentState(
        source_infohash="a" * 40,
        state=State.WAITING_SEEDPOOL,
        seedpool_first_queried_at=dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=cfg.cross_seed.seedpool_max_age_seconds + 1),
        seedpool_next_retry_at=dt.datetime.now(dt.timezone.utc),
        seedpool_attempts=10,
    )
    now = dt.datetime.now(dt.timezone.utc)
    elapsed = now - ts.seedpool_first_queried_at
    assert elapsed > dt.timedelta(seconds=cfg.cross_seed.seedpool_max_age_seconds)


@pytest.mark.anyio
async def test_process_torrent_inner_dispatches_querying_state():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord._do_waiting_seedpool = AsyncMock()

    ts = TorrentState("hash1", state=State.QUERYING)
    await coord._process_torrent_inner(ts)

    # Must dispatch to _do_waiting_seedpool when in QUERYING state
    coord._do_waiting_seedpool.assert_awaited_once_with(ts)


@pytest.mark.anyio
async def test_list_source_torrents_caches_within_ttl():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.clients.abstract import Torrent

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.source.category = "racing"
    coord.cfg.source.min_age_seconds = 0
    coord._source_torrents_cache = []
    coord._source_torrents_cached_at = 0.0

    t1 = Torrent(hash="h1", name="Show.A", category="racing", save_path="", size_bytes=100, state="", progress=1.0)
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents.return_value = [t1]

    # First call: fetches from client
    res1 = await coord._list_source_torrents()
    assert len(res1) == 1
    assert coord.source_client.list_torrents.await_count == 1

    # Second call right after: returns cached result without RPC call
    res2 = await coord._list_source_torrents()
    assert len(res2) == 1
    assert coord.source_client.list_torrents.await_count == 1

    # Force refresh: bypasses cache and calls RPC again
    res3 = await coord._list_source_torrents(force_refresh=True)
    assert len(res3) == 1
    assert coord.source_client.list_torrents.await_count == 2


@pytest.mark.anyio
async def test_do_new_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_12345", state=State.NEW)
    await coord._do_new(ts)

    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: vanished_h"
    )


@pytest.mark.anyio
async def test_do_waiting_seedpool_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_67890", state=State.QUERYING)
    await coord._do_waiting_seedpool(ts)

    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: vanished_h"
    )


@pytest.mark.anyio
async def test_do_re_add_skips_when_already_injected_in_step_1():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="abc123456789", state=State.RE_ADDING)
    ts.injected_private_hashes = "abc123456789"
    ts.cross_seed_infohash = "abc123456789"
    ts.cross_seed_blob = b"dummy"

    await coord._do_re_add(ts)

    # Should not call add_torrent because it was already injected
    coord.dest_client.add_torrent.assert_not_called()
    coord.transition.assert_called_once_with(ts, State.DONE)


@pytest.mark.anyio
async def test_do_re_add_accepts_when_dest_client_returns_fails_but_already_exists():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.clients.abstract import AddResult, Torrent

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock()
    coord._target_mount_for = MagicMock(return_value="/mnt/fuse")

    ts = TorrentState(source_infohash="abc123456789", state=State.RE_ADDING)
    ts.cross_seed_infohash = "abc123456789"
    ts.cross_seed_blob = b"dummy"

    coord.dest_client.add_torrent.return_value = AddResult(hash=None, accepted=False, detail="Fails.")
    coord.dest_client.get_torrent.return_value = Torrent(
        hash="abc123456789", name="Test", category="racing", save_path="/mnt/fuse", size_bytes=100, state="", progress=1.0
    )

    await coord._do_re_add(ts)

    coord.dest_client.add_torrent.assert_awaited_once()
    coord.dest_client.get_torrent.assert_awaited_once_with("abc123456789")
    coord.transition.assert_called_once_with(ts, State.DONE)


def test_should_notify_telegram_policy():
    from racing_sync.coordinator import _should_notify_telegram

    # Same-state transition should not notify
    assert not _should_notify_telegram(State.NEW, State.NEW)
    assert not _should_notify_telegram(State.DOWNLOADING, State.DOWNLOADING)

    # Transitioning to NEW should not notify
    assert not _should_notify_telegram(State.QUEUED, State.NEW)

    # Active and terminal states should notify
    assert _should_notify_telegram(State.NEW, State.QUEUED)
    assert _should_notify_telegram(State.QUEUED, State.DOWNLOADING)
    assert _should_notify_telegram(State.DOWNLOADING, State.MOVING)
    assert _should_notify_telegram(State.MOVING, State.RE_ADDING)
    assert _should_notify_telegram(State.RE_ADDING, State.DONE)
    assert _should_notify_telegram(State.RE_ADDING, State.FAILED)
    assert _should_notify_telegram(State.NEW, State.WAITING_SEEDPOOL)


@pytest.mark.anyio
async def test_process_torrent_inner_does_not_fallthrough_to_waiting_seedpool_from_new():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord._do_waiting_seedpool = AsyncMock()

    async def fake_do_new(ts: TorrentState) -> None:
        ts.state = State.WAITING_SEEDPOOL

    coord._do_new = AsyncMock(side_effect=fake_do_new)

    ts = TorrentState("hash_new", state=State.NEW)
    await coord._process_torrent_inner(ts)

    coord._do_new.assert_awaited_once_with(ts)
    coord._do_waiting_seedpool.assert_not_called()


@pytest.mark.anyio
async def test_tick_skips_waiting_seedpool_in_step_4():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord._tasks = set()
    coord._running_infohashes = set()
    coord._live = {}
    coord._check_and_inject_late_cross_seeds = AsyncMock()
    coord.watch = None
    coord.store = MagicMock()
    coord.store.list_seedpool_ready.return_value = []
    coord._list_source_torrents = AsyncMock(return_value=[])

    ts_waiting = TorrentState(
        source_infohash="waiting_seedpool",
        source_name="Waiting.Seedpool.Release",
        state=State.WAITING_SEEDPOOL,
    )
    ts_queued = TorrentState(
        source_infohash="queued_ready",
        source_name="Queued.Release",
        state=State.QUEUED,
    )
    coord.store.all_active.return_value = [ts_waiting, ts_queued]

    scheduled: list[str] = []

    async def fake_process(ts: TorrentState) -> None:
        scheduled.append(ts.source_infohash)

    coord._process_torrent = fake_process

    await coord._tick()
    await asyncio.sleep(0.01)

    assert "waiting_seedpool" not in scheduled
    assert "queued_ready" in scheduled


@pytest.mark.anyio
async def test_coordinator_run_stops_immediately_when_stop_requested():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord.start = AsyncMock()
    coord.shutdown = AsyncMock()
    coord.cfg = MagicMock()
    # Large sleep interval: if _stop is ignored, this test would hang/timeout
    coord.cfg.general.source_poll_interval = 9999

    tick_called = False

    async def fake_tick():
        nonlocal tick_called
        tick_called = True
        coord.request_stop()

    coord._tick = fake_tick

    exit_code = await coord.run()
    assert exit_code == 0
    assert tick_called is True
    coord.shutdown.assert_awaited_once()
