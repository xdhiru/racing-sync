"""Tests for the indexer retry policy math.

We don't run the full coordinator here (it needs live qBittorrent +
Prowlarr). Instead we verify the timing rules that _park_for_indexer_retry
applies.
"""

from __future__ import annotations

import datetime as dt
import pytest

from racing_sync.config import AppConfig
from racing_sync.state import State, TorrentState


def _cfg() -> AppConfig:
    return AppConfig.from_toml(
        __file__.replace("\\", "/").rsplit("/", 1)[0] + "/../config.example.toml"
    )


def test_first_attempt_sets_first_queried_at():
    cfg = _cfg()
    ts = TorrentState(source_infohash="a" * 40, state=State.NEW)
    assert ts.indexer_first_queried_at is None
    assert ts.indexer_attempts == 0

    now = dt.datetime.now(dt.timezone.utc)
    interval = cfg.cross_seed.indexer_retry_interval_seconds
    ts.indexer_first_queried_at = now
    ts.indexer_attempts = 1
    ts.indexer_next_retry_at = now + dt.timedelta(seconds=interval)

    assert ts.indexer_first_queried_at == now
    assert ts.indexer_next_retry_at is not None
    diff = (ts.indexer_next_retry_at - now).total_seconds()
    assert abs(diff - interval) < 1


def test_retry_window_is_24_hours():
    cfg = _cfg()
    assert cfg.cross_seed.indexer_max_age_seconds == 86400
    assert cfg.cross_seed.indexer_retry_interval_seconds == 1800


def test_expired_max_age_marks_failed():
    """If the first attempt was > 24 h ago, the next park should escalate
    to FAILED. We simulate by backdating indexer_first_queried_at."""
    cfg = _cfg()
    ts = TorrentState(
        source_infohash="a" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=cfg.cross_seed.indexer_max_age_seconds + 1),
        indexer_next_retry_at=dt.datetime.now(dt.timezone.utc),
        indexer_attempts=10,
    )
    now = dt.datetime.now(dt.timezone.utc)
    elapsed = now - ts.indexer_first_queried_at
    assert elapsed > dt.timedelta(seconds=cfg.cross_seed.indexer_max_age_seconds)


@pytest.mark.anyio
async def test_process_torrent_inner_dispatches_querying_state():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord._do_waiting_indexer = AsyncMock()

    ts = TorrentState("hash1", state=State.QUERYING)
    await coord._process_torrent_inner(ts)

    # Must dispatch to _do_waiting_indexer when in QUERYING state
    coord._do_waiting_indexer.assert_awaited_once_with(ts)


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
async def test_do_waiting_indexer_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_67890", state=State.QUERYING)
    await coord._do_waiting_indexer(ts)

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
    assert _should_notify_telegram(State.NEW, State.WAITING_INDEXER)