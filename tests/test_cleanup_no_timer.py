"""No clock anywhere in VPS1 cleanup: the scenarios the grace timer used to cover.

This file replaces `tests/test_cleanup_grace.py`. The rule changed — elapsed
time left the policy when VPS1 started reclaiming to a free-space band
(`[cleanup].low_watermark_free_bytes` = trigger, `high_watermark_free_bytes` =
target; see tests/test_cleanup_headroom.py). Every scenario the grace suite
covered is still covered here, under the new model:

  - an adopted DONE row (no `completed_at`) whose VPS1 torrent is days old;
  - an adopted DONE row with no activity stamp at all;
  - a DONE row aged far past the old 2 h/72 h grace bounds;
  - aging/ordering anchored on `created_at` when `completed_at` is missing;
  - a row with neither stamp (the defensive zero-age fallback).

What each one now proves is the equivalent safety property instead of the
timer: a row is kept until verification proves the content is secured — and
once it is a candidate, the pressure order (biggest-first, then oldest) still
comes from a real anchor, never from a guessed epoch.

Entry point is `_cleanup_verdict`: the janitor loop, grouping, fuse health and
deletes have their own suites, so fuse health is stubbed here to isolate the
eligibility decision. Release names are invented.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.coordinator import Coordinator
from racing_sync.state import State, TorrentState

H = "a" * 40
FNAME = "Harbor.Lights.S01E01.mkv"
FSIZE = 700
GiB = 1024**3
# Below the 15 GiB trigger: the space is needed, so the pass asks its question.
NEEDED = 5 * GiB


def _cleanup_cfg() -> CleanupConfig:
    """Dry-run config on the live defaults (the timer keys no longer exist)."""
    return CleanupConfig(enabled=True, dry_run=True)


def _coord(*, healthy: bool = True) -> Coordinator:
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.cleanup = _cleanup_cfg()
    # Fuse-entry/byte verification is not the subject of these tests.
    coord._cleanup_fuse_healthy = AsyncMock(return_value=healthy)
    return coord


def _row(
    *,
    created_at: dt.datetime | None = None,
    completed_at: dt.datetime | None = None,
    activity_at: dt.datetime | None = None,
    size: int = FSIZE,
) -> TorrentState:
    row = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        dest_infohash=H,
        total_bytes=size,
        state=State.DONE,
        completed_at=completed_at,
        vps1_last_activity_at=activity_at,
    )
    row.created_at = created_at  # type: ignore[assignment]
    return row


def _member(added_on: int, h: str = H) -> Torrent:
    """Quiet, known-stats VPS1 member (passes the pre-verification gates)."""
    return Torrent(
        hash=h,
        name=FNAME,
        category="",
        save_path="/vps1/data",
        size_bytes=FSIZE,
        state="seeding",
        progress=1.0,
        ratio=2.0,
        trackers=[],
        upspeed_bps=0,
        num_leechers=0,
        added_on=added_on,
    )


async def _verdict(
    coord: Coordinator, row: TorrentState, group: list[Torrent], now: dt.datetime, **over
):
    blocked: dict[str, int] = {}
    got = await coord._cleanup_verdict(
        row,
        group,
        now,
        dry_run=True,
        free_bytes=NEEDED,
        blocked=blocked,
        **over,
    )
    return got, blocked


@pytest.mark.anyio
async def test_adopted_done_with_old_added_on_needs_no_clock():
    """A row adopted days after its VPS1 torrent appeared is eligible at once.

    Old rule: `created_at` 30 minutes ago put it inside the 2 h grace, so the
    five-day-old `added_on` could not fast-lane it. New rule: nothing ages —
    the fuse proof is the whole gate, so the row becomes a candidate the moment
    it verifies, and its place in the pressure order comes from the adoption
    instant (`created_at`), not from a countdown.
    """
    now = dt.datetime.now(dt.UTC)
    coord = _coord()
    row = _row(
        created_at=now - dt.timedelta(minutes=30),
        completed_at=None,
        activity_at=now - dt.timedelta(minutes=10),
    )
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got, blocked = await _verdict(coord, row, [_member(old_added_on)], now)

    assert got is not None, "verification, not elapsed time, opens the gate"
    assert blocked == {}
    assert got[0] == row.created_at.timestamp()


@pytest.mark.anyio
async def test_adopted_done_without_stamps_is_kept_until_it_verifies():
    """No completion stamp, no activity stamp: proof still decides, never age.

    The old suite kept this row through the grace window because `added_on`
    alone is not evidence. The surviving reason is stronger and honest: until
    the fuse copy verifies, a starving disk does not touch it — and the keep is
    counted as `verification`, not as a wait.
    """
    now = dt.datetime.now(dt.UTC)
    coord = _coord(healthy=False)
    row = _row(created_at=now - dt.timedelta(minutes=30), completed_at=None, activity_at=None)
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got, blocked = await _verdict(coord, row, [_member(old_added_on)], now)

    assert got is None
    assert blocked == {"verification": 1}
    # The report vocabulary lost the wait and kept the surviving MOVING lane.
    assert "grace" not in Coordinator._CLEANUP_BLOCK_ORDER
    assert Coordinator._cleanup_blocked_text({"idle": 1}) == "idle 1"


@pytest.mark.anyio
async def test_done_aged_past_the_old_grace_and_just_secured_get_the_same_verdict():
    """Age buys nothing and costs nothing: identical rows, identical verdicts."""
    now = dt.datetime.now(dt.UTC)
    coord = _coord()
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    aged = _row(created_at=now - dt.timedelta(days=400), completed_at=now - dt.timedelta(days=380))
    fresh = _row(created_at=now, completed_at=now - dt.timedelta(seconds=60))

    got_aged, blocked_aged = await _verdict(coord, aged, [_member(old_added_on)], now)
    got_fresh, blocked_fresh = await _verdict(coord, fresh, [_member(old_added_on)], now)

    assert got_aged is not None and got_fresh is not None
    assert blocked_aged == blocked_fresh == {}
    # Each stays anchored on its own completion instant, so the order is still
    # deterministic — but neither needed to wait for anything.
    assert got_aged[0] == aged.completed_at.timestamp()
    assert got_fresh[0] == fresh.completed_at.timestamp()
    # Same size, same state: only the anchor differs, oldest goes first.
    ordered = Coordinator._cleanup_sort_candidates(
        [(got_aged[0], got_aged[1], aged, [object()]), (got_fresh[0], got_fresh[1], fresh, [])],
        True,
    )
    assert [c[2].created_at for c in ordered] == [aged.created_at, fresh.created_at]


@pytest.mark.anyio
async def test_missing_completed_at_keeps_created_at_as_the_pressure_anchor():
    """`created_at` still orders the sweep; it just no longer gates it.

    Without the fallback the row would anchor on "now" and outrank genuinely
    older rows in the biggest-first-then-oldest order.
    """
    now = dt.datetime.now(dt.UTC)
    coord = _coord()
    row = _row(
        created_at=now - dt.timedelta(hours=5),
        completed_at=None,
        activity_at=now - dt.timedelta(minutes=10),
    )
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got, _blocked = await _verdict(coord, row, [_member(old_added_on)], now)

    assert got is not None
    assert got[0] == row.created_at.timestamp()


@pytest.mark.anyio
async def test_no_stamps_at_all_falls_back_to_now_and_orders_last():
    """Defensive: neither stamp present → the anchor is "now", never epoch 0.

    An epoch anchor would sort an unanchored row to the FRONT of the pressure
    order (oldest-first) and delete it before content with a real history. The
    zero-age fallback survives as "youngest", so it is the last thing a pass
    reaches.
    """
    now = dt.datetime.now(dt.UTC)
    coord = _coord()
    row = _row(created_at=None, completed_at=None, activity_at=now - dt.timedelta(minutes=10))
    old_added_on = int((now - dt.timedelta(days=5)).timestamp())

    got, _blocked = await _verdict(coord, row, [_member(old_added_on)], now)

    assert got is not None
    assert got[0] == pytest.approx(now.timestamp(), abs=2.0)

    anchored = _row(
        created_at=now - dt.timedelta(hours=5), completed_at=now - dt.timedelta(hours=5)
    )
    ordered = Coordinator._cleanup_sort_candidates(
        [
            (got[0], got[1], row, []),
            (anchored.created_at.timestamp(), anchored.total_bytes, anchored, []),
        ],
        True,
    )
    assert [c[2].source_infohash for c in ordered] == [H, H]
    assert [c[0] for c in ordered] == sorted([c[0] for c in ordered])
    assert ordered[0][2] is anchored  # the anchored row goes first, the stampless one last


def test_no_grace_helper_is_left_to_call():
    """The timer is gone from the code, not just from the config."""
    import racing_sync.coordinator as coordinator_mod
    from racing_sync import config_disposition, coordinator_cleanup

    assert not hasattr(coordinator_mod, "cleanup_grace_seconds")
    assert not hasattr(coordinator_cleanup, "cleanup_grace_seconds")
    assert not hasattr(coordinator_cleanup, "_lerp")
    for gone in ("min_grace_hours", "max_grace_hours", "idle_confirm_minutes"):
        # No field to drift into, and a fail-loud entry in the removed registry.
        assert gone not in CleanupConfig.model_fields, gone
        assert f"cleanup.{gone}" in config_disposition.REMOVED, gone
