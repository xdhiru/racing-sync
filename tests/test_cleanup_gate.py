"""Janitor gate: persisted cadence, startup delay, calm-and-roomy skip."""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024 ** 3


def _gate_coord(tmp_path: Path, db_name: str = "s.db", **cleanup_over):
    store = StateStore(tmp_path / db_name)
    cfg = MagicMock()
    cfg.source.category = ""
    base = dict(enabled=True, dry_run=True,
                janitor_startup_delay_seconds=0.0)
    base.update(cleanup_over)
    cfg.cleanup = CleanupConfig(**base)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.sftp = None
    coord._stop = False
    return coord, store


@pytest.mark.anyio
async def test_persisted_clock_survives_restart(tmp_path: Path):
    """A reboot resumes the hourly cadence instead of running immediately."""
    coord, store = _gate_coord(tmp_path)
    try:
        coord._run_source_cleanup = AsyncMock()
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 1
        assert store.get_meta("cleanup_last_run") is not None

        # Same process, immediate second call: skipped.
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 1

        # Simulated restart: fresh coordinator, same DB → still skipped.
        coord2, _ = _gate_coord(tmp_path)
        coord2.store = store
        coord2._run_source_cleanup = AsyncMock()
        await coord2._maybe_cleanup_source()
        assert coord2._run_source_cleanup.await_count == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_startup_delay_holds_first_run(tmp_path: Path):
    """No deletes in the settle window after boot; 0 disables."""
    coord, store = _gate_coord(
        tmp_path, janitor_startup_delay_seconds=3600.0)
    try:
        coord._run_source_cleanup = AsyncMock()
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 0

        coord.cfg.cleanup.janitor_startup_delay_seconds = 0.0
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_calm_and_roomy_skips_scan(tmp_path: Path):
    """Plenty of space + quiet intake: no scan, no deletes."""
    import datetime as dt

    coord, store = _gate_coord(tmp_path)
    try:
        now = dt.datetime.now(dt.timezone.utc)
        store.upsert(TorrentState(
            source_infohash="a" * 40, source_name="Old Show",
            dest_infohash="a" * 40, total_bytes=700, state=State.DONE,
            completed_at=now - dt.timedelta(hours=200)))
        # Roomy (above the 40 GiB high watermark) + calm.
        coord._source_free_bytes = AsyncMock(return_value=90 * GiB)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        await coord._maybe_cleanup_source()

        coord.source_client.list_torrents.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_pressure_still_runs(tmp_path: Path):
    """Tight disk: the run proceeds to the source list as before."""
    import datetime as dt

    coord, store = _gate_coord(tmp_path)
    try:
        now = dt.datetime.now(dt.timezone.utc)
        store.upsert(TorrentState(
            source_infohash="a" * 40, source_name="Old Show",
            dest_infohash="a" * 40, total_bytes=700, state=State.DONE,
            completed_at=now - dt.timedelta(hours=200)))
        coord._source_free_bytes = AsyncMock(return_value=1 * GiB)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        await coord._maybe_cleanup_source()

        coord.source_client.list_torrents.assert_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_free_space_runs_fail_closed(tmp_path: Path):
    """Unmeasurable disk never counts as roomy."""
    import datetime as dt

    coord, store = _gate_coord(tmp_path)
    try:
        now = dt.datetime.now(dt.timezone.utc)
        store.upsert(TorrentState(
            source_infohash="a" * 40, source_name="Old Show",
            dest_infohash="a" * 40, total_bytes=700, state=State.DONE,
            completed_at=now - dt.timedelta(hours=200)))
        coord._source_free_bytes = AsyncMock(return_value=None)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        await coord._maybe_cleanup_source()

        coord.source_client.list_torrents.assert_awaited()
    finally:
        store.close()
