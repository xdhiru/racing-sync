"""Janitor gate: persisted cadence, startup delay, capacity trigger, blind disk."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024**3


def _gate_coord(tmp_path: Path, db_name: str = "s.db", **cleanup_over):
    store = StateStore(tmp_path / db_name)
    cfg = MagicMock()
    cfg.source.category = ""
    base = dict(enabled=True, dry_run=True, janitor_startup_delay_seconds=0.0)
    base.update(cleanup_over)
    cfg.cleanup = CleanupConfig(**base)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.sftp = None
    # Measured and below the 15 GiB trigger by default: an automatic pass skips
    # a blind disk by design, and these cadence tests are about the run clock,
    # not about blindness. Cases that want another disk state override this.
    coord._source_free_bytes = AsyncMock(return_value=5 * GiB)
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
    coord, store = _gate_coord(tmp_path, janitor_startup_delay_seconds=3600.0)
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
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Old Show",
                dest_infohash="a" * 40,
                total_bytes=700,
                state=State.DONE,
                completed_at=now - dt.timedelta(hours=200),
            )
        )
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
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Old Show",
                dest_infohash="a" * 40,
                total_bytes=700,
                state=State.DONE,
                completed_at=now - dt.timedelta(hours=200),
            )
        )
        # Measured and below the 15 GiB trigger: the pass has a real claim that
        # space is needed, so it scans (a blind disk would be skipped instead).
        coord._source_free_bytes = AsyncMock(return_value=1 * GiB)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        await coord._maybe_cleanup_source()

        coord.source_client.list_torrents.assert_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_free_space_skips_the_automatic_pass(tmp_path: Path):
    """Unmeasurable disk never counts as roomy — and never as needing space.

    Original intent kept: no reading must not degrade into "roomy, nothing to
    do", which would silence the janitor on a claim nobody measured. The
    capacity rule adds the other half of the same honesty: with no measurement
    the trigger cannot be evaluated either, so an AUTOMATIC pass deletes
    nothing. An operator can still ask for the pass (see
    `test_forced_pass_runs_proof_gated_on_a_blind_disk`).
    """
    import datetime as dt

    coord, store = _gate_coord(tmp_path)
    try:
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Old Show",
                dest_infohash="a" * 40,
                total_bytes=700,
                state=State.DONE,
                completed_at=now - dt.timedelta(hours=200),
            )
        )
        coord._source_free_bytes = AsyncMock(return_value=None)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        summary = await coord._maybe_cleanup_source()

        # Never treated as roomy: the skip names the missing reading, not the
        # headroom, and is never reported as "no space needed".
        assert summary is not None
        assert "VPS1 free space unknown" in summary["reason"]
        assert "not reclaiming automatically" in summary["reason"]
        assert "no space needed" not in summary["reason"]
        # Never treated as needing space: the scan (and every delete it could
        # lead to) does not happen on a disk the pass cannot measure.
        coord.source_client.list_torrents.assert_not_called()
        assert summary["deleted"] == 0
        assert summary["candidates"] == 0
        # And no number is invented for a disk that gave none.
        assert summary["free"] is None
        assert summary["free_after"] is None
        assert summary["target"] is None
        assert summary["shortfall_bytes"] == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_forced_pass_runs_proof_gated_on_a_blind_disk(tmp_path: Path, caplog):
    """`/cleanup` (force) DOES run on a blind disk — and only on proof.

    The operator explicitly asked for a pass, so the missing reading no longer
    stops the scan — but nothing is invented in its place: the row still has to
    prove its content is secured on VPS2, and the report says free is unknown
    with no target. (The per-run cap that bounds such a pass is pinned in
    tests/test_cleanup_headroom.py.)
    """
    import datetime as dt

    coord, store = _gate_coord(tmp_path)
    try:
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Old Show",
                dest_infohash="a" * 40,
                total_bytes=700,
                state=State.DONE,
                completed_at=now - dt.timedelta(hours=200),
            )
        )
        # A real VPS1 member for that row, so the pass reaches the proof gate.
        coord.source_client.list_torrents = AsyncMock(
            return_value=[
                Torrent(
                    hash="a" * 40,
                    name="Old Show",
                    category="",
                    save_path="/vps1/data",
                    size_bytes=700,
                    state="seeding",
                    progress=1.0,
                    ratio=2.0,
                    trackers=[],
                    upspeed_bps=0,
                    num_leechers=0,
                    added_on=0,
                )
            ]
        )
        # The content is NOT proven secured: a forced blind pass still keeps it.
        coord._cleanup_fuse_healthy = AsyncMock(return_value=False)
        coord._delete_source_group = AsyncMock(return_value=True)
        coord._source_free_bytes = AsyncMock(return_value=None)
        coord._cleanup_arrivals_per_hour = MagicMock(return_value=0.0)

        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            summary = await coord._maybe_cleanup_source(force=True)

        assert summary is not None
        # The scan ran: the operator asked, so the blind disk is not a veto...
        coord.source_client.list_torrents.assert_awaited()
        # ...but every row still passes the proof gate before it can go.
        coord._cleanup_fuse_healthy.assert_awaited_once()
        coord._delete_source_group.assert_not_awaited()
        assert summary["deleted"] == 0
        assert summary["candidates"] == 0
        assert "verification 1" in summary["reason"]
        # ...and no free-space number was invented.
        assert summary["free"] is None
        assert summary["free_after"] is None
        assert summary["target"] is None
        assert summary["shortfall_bytes"] == 0
        # The report says so out loud.
        assert "VPS1 free space unknown" in caplog.text
    finally:
        store.close()
