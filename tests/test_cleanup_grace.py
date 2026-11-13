"""Grace anchoring for DONE rows that lack completed_at (cleanup E10).

Recovery adopts fuse seeds as DONE without setting ``completed_at``. The
janitor must age those rows from ``created_at`` (the adoption instant) so
they still serve the adaptive grace instead of aging from zero.

Entry point is ``_cleanup_verdict``: the janitor loop, grouping, fuse
health and deletes are covered by tests/test_cleanup.py, so fuse health is
stubbed here to isolate the grace decision.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.coordinator import Coordinator
from racing_sync.coordinator_content import cleanup_grace_seconds
from racing_sync.state import State, TorrentState

H = "a" * 40
FNAME = "Show.S01E01.mkv"
FSIZE = 700


def _cleanup_cfg() -> CleanupConfig:
    """Cleanup config with the grace pinned to 2h (min == max)."""
    return CleanupConfig(enabled=True, dry_run=True,
                         min_grace_hours=2.0, max_grace_hours=2.0)


def _coord(cfg: CleanupConfig) -> Coordinator:
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.cleanup = cfg
    # Fuse-entry/byte verification is not part of the grace decision.
    coord._cleanup_fuse_healthy = AsyncMock(return_value=True)
    return coord


def _row(
    *, created_at: dt.datetime | None = None,
    completed_at: dt.datetime | None = None,
    activity_at: dt.datetime | None = None,
) -> TorrentState:
    row = TorrentState(
        source_infohash=H, source_name=FNAME, dest_infohash=H,
        total_bytes=FSIZE, state=State.DONE,
        completed_at=completed_at, vps1_last_activity_at=activity_at,
    )
    if created_at is None:
        row.created_at = None  # type: ignore[assignment]
    else:
        row.created_at = created_at
    return row


def _member(added_on: int) -> Torrent:
    """Quiet, known-stats VPS1 member (passes the pre-grace gates)."""
    return Torrent(hash=H, name=FNAME, category="", save_path="/vps1/data",
                   size_bytes=FSIZE, state="seeding", progress=1.0,
                   ratio=2.0, trackers=[], upspeed_bps=0, num_leechers=0,
                   added_on=added_on)


async def _verdict(coord: Coordinator, row: TorrentState, group: list[Torrent],
                   now: dt.datetime, cfg: CleanupConfig):
    grace_s = cleanup_grace_seconds(cfg, None, 0.0)
    assert grace_s == 2 * 3600
    return await coord._cleanup_verdict(row, group, grace_s, now,
                                        dry_run=True, free_bytes=None)


@pytest.mark.anyio
async def test_adopted_done_inside_grace_is_kept_despite_old_added_on():
    """A freshly adopted row ages from created_at, not from zero.

    Its VPS1 torrent was added days ago, so the idle fallback alone would
    confirm the race as over; the grace measured from adoption still holds
    while the row was recently observed active.
    """
    now = dt.datetime.now(dt.UTC)
    cfg = _cleanup_cfg()
    coord = _coord(cfg)
    row = _row(created_at=now - dt.timedelta(minutes=30),
               completed_at=None,
               activity_at=now - dt.timedelta(minutes=10))
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    assert await _verdict(coord, row, [_member(old_added_on)], now, cfg) is None


@pytest.mark.anyio
async def test_adopted_done_without_activity_still_waits_grace():
    """No completion stamp and no activity stamp: old added_on is not proof.

    Recovery adopts a fuse seed at created_at; the VPS1 member may have
    existed for days, so the 45-minute idle fallback must not cut the
    grace period short.
    """
    now = dt.datetime.now(dt.UTC)
    cfg = _cleanup_cfg()
    coord = _coord(cfg)
    row = _row(created_at=now - dt.timedelta(minutes=30),
               completed_at=None, activity_at=None)
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    assert await _verdict(coord, row, [_member(old_added_on)], now, cfg) is None


@pytest.mark.anyio
async def test_done_past_grace_still_follows_the_eligible_path():
    """A known completed_at older than grace keeps the existing behavior."""
    now = dt.datetime.now(dt.UTC)
    cfg = _cleanup_cfg()
    coord = _coord(cfg)
    row = _row(created_at=now - dt.timedelta(hours=6),
               completed_at=now - dt.timedelta(hours=5),
               activity_at=now - dt.timedelta(minutes=10))
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got = await _verdict(coord, row, [_member(old_added_on)], now, cfg)

    assert got is not None
    # Sort key stays anchored on the completion instant.
    assert got[0] == row.completed_at.timestamp()


@pytest.mark.anyio
async def test_adopted_done_past_grace_ages_from_created_at():
    """No completed_at: created_at decides grace and the sort key.

    Without the fallback the row would age from zero (age_s = 0), never
    reach grace and stay pinned behind the idle-confirm wait.
    """
    now = dt.datetime.now(dt.UTC)
    cfg = _cleanup_cfg()
    coord = _coord(cfg)
    row = _row(created_at=now - dt.timedelta(hours=5),
               completed_at=None,
               activity_at=now - dt.timedelta(minutes=10))
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got = await _verdict(coord, row, [_member(old_added_on)], now, cfg)

    assert got is not None
    assert got[0] == row.created_at.timestamp()


@pytest.mark.anyio
async def test_missing_completed_and_created_at_keeps_zero_age_fallback():
    """Created_at None (defensive): age stays 0, so grace still applies."""
    now = dt.datetime.now(dt.UTC)
    cfg = _cleanup_cfg()
    coord = _coord(cfg)
    row = _row(created_at=None, completed_at=None,
               activity_at=now - dt.timedelta(minutes=10))
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    assert await _verdict(coord, row, [_member(old_added_on)], now, cfg) is None
