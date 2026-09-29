"""Janitor delete staggering: spread remove_torrent load, don't burst it."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState


def _stagger_pair(tmp_path: Path, db_name: str = "s.db", **cleanup_over):
    """Two eligible DONE rows with matching source members."""
    store = StateStore(tmp_path / db_name)
    now = dt.datetime.now(dt.timezone.utc)
    rows = []
    members = []
    for h, name, size in (("a" * 40, "ShowA.mkv", 700),
                          ("b" * 40, "ShowB.mkv", 701)):
        ts = TorrentState(
            source_infohash=h, source_name=name, dest_infohash=h,
            total_bytes=size, state=State.DONE,
            completed_at=now - dt.timedelta(hours=100),
            vps1_last_activity_at=now - dt.timedelta(hours=10),
        )
        store.upsert(ts)
        rows.append(ts)
        members.append(Torrent(
            hash=h, name=name, category="", save_path="/vps1/data",
            size_bytes=size, state="seeding", progress=1.0, ratio=2.0,
            trackers=[], upspeed_bps=0, num_leechers=0, added_on=0))
    ssd = tmp_path

    class _FakeSource:
        def __init__(self, ms):
            self.members = ms

        async def list_torrents(self, *, category=None, hashes=None):
            return list(self.members)

    cfg = MagicMock()
    cfg.source.category = ""
    cfg.dest.save_path = ssd
    base = dict(enabled=True, dry_run=False)
    base.update(cleanup_over)
    cfg.cleanup = CleanupConfig(**base)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = _FakeSource(members)
    coord.dest_client = MagicMock()
    coord.sftp = None
    coord._stop = False
    return coord, store


@pytest.mark.anyio
async def test_stagger_sleeps_between_group_deletes(tmp_path: Path):
    """Two groups deleted: one settle pause between them, none after last."""
    coord, store = _stagger_pair(tmp_path, delete_stagger_seconds=25.0)
    try:
        coord._cleanup_verdict = AsyncMock(side_effect=[
            (0.0, 700.0, store.get("a" * 40), ["g1"]),
            (1.0, 701.0, store.get("b" * 40), ["g2"]),
        ])
        coord._delete_source_group = AsyncMock(return_value=True)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await coord._maybe_cleanup_source()

        assert coord._delete_source_group.await_count == 2
        mock_sleep.assert_awaited_once_with(25.0)
    finally:
        store.close()


@pytest.mark.anyio
async def test_stagger_skipped_on_dry_run(tmp_path: Path):
    """Dry-run rehearsal: no pauses (nothing real happens)."""
    coord, store = _stagger_pair(tmp_path, delete_stagger_seconds=25.0,
                                 dry_run=True)
    try:
        coord._cleanup_verdict = AsyncMock(side_effect=[
            (0.0, 700.0, store.get("a" * 40), ["g1"]),
            (1.0, 701.0, store.get("b" * 40), ["g2"]),
        ])
        coord._delete_source_group = AsyncMock(return_value=True)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await coord._maybe_cleanup_source()

        assert coord._delete_source_group.await_count == 2
        mock_sleep.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_stagger_zero_disables_pause(tmp_path: Path):
    """stagger=0 restores back-to-back deletes."""
    coord, store = _stagger_pair(tmp_path, delete_stagger_seconds=0.0)
    try:
        coord._cleanup_verdict = AsyncMock(side_effect=[
            (0.0, 700.0, store.get("a" * 40), ["g1"]),
            (1.0, 701.0, store.get("b" * 40), ["g2"]),
        ])
        coord._delete_source_group = AsyncMock(return_value=True)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await coord._maybe_cleanup_source()

        assert coord._delete_source_group.await_count == 2
        mock_sleep.assert_not_called()
    finally:
        store.close()
