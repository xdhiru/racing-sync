"""Burst-mode janitor: arrival bytes, pressure decision, cadence/cap, urgency.

The janitor adapts to intake rate and projected fill: it records arrival
byte sizes next to the arrival timestamps, derives (arrivals/h, bytes/h,
projected hours-to-fill), and switches to the burst cadence/cap when any
pressure signal trips. An arrival that does not fit at all marks the next
pass urgent, which also opens the early MOVING path.
"""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator
from pydantic import ValidationError

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024**3
LOW = 5 * GiB  # below the 15 GiB low watermark
MID = 20 * GiB  # above low, below the 40 GiB high watermark
ROOMY = 90 * GiB  # above the 40 GiB high watermark
H = "a" * 40
FNAME = "Show.S01E01.mkv"
FSIZE = 700


def _cfg(**over) -> CleanupConfig:
    base = dict(enabled=True, dry_run=False, janitor_startup_delay_seconds=0.0)
    base.update(over)
    return CleanupConfig(**base)


def _member(h: str, name: str, size: int) -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=size,
        state="seeding",
        progress=1.0,
        ratio=2.0,
        trackers=[],
        upspeed_bps=0,
        num_leechers=0,
        added_on=0,
    )


class _FakeSource:
    def __init__(self, members: list[Torrent]):
        self.members = members
        self.delete = AsyncMock()

    async def list_torrents(self, *, category=None, hashes=None):
        return list(self.members)


def _harness(tmp_path: Path, count: int = 4, **cleanup_over):
    """Store rows + matching VPS1 members, wired into a bare coordinator."""
    store = StateStore(tmp_path / "s.db")
    now = dt.datetime.now(dt.UTC)
    members: list[Torrent] = []
    rows: list[TorrentState] = []
    for i in range(count):
        h = str(i + 1) * 40
        name = f"Show.{i}.mkv"
        size = FSIZE + i
        row = TorrentState(
            source_infohash=h,
            source_name=name,
            dest_infohash=h,
            total_bytes=size,
            state=State.DONE,
            completed_at=now - dt.timedelta(hours=100),
            vps1_last_activity_at=now - dt.timedelta(hours=10),
            # Marked injected so the pre-delete blob guard is a no-op.
            injected_private_hashes=h,
        )
        store.upsert(row)
        rows.append(row)
        members.append(_member(h, name, size))
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.cleanup = _cfg(**cleanup_over)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = _FakeSource(members)
    coord.dest_client = MagicMock()
    coord.sftp = None
    coord._stop = False
    return coord, store, members


# ---------------------------------------------------------------- config


def test_config_defaults_and_bounds():
    cfg = CleanupConfig()
    assert cfg.burst_janitor_interval_seconds == 300
    assert cfg.burst_per_run_cap == 25
    assert cfg.fill_lookahead_hours == 2.0
    # Calm defaults untouched.
    assert cfg.janitor_interval_seconds == 3600
    assert cfg.per_run_cap == 10

    with pytest.raises(ValidationError, match="burst_janitor_interval_seconds"):
        CleanupConfig(janitor_interval_seconds=300, burst_janitor_interval_seconds=600)
    with pytest.raises(ValidationError, match="burst_per_run_cap"):
        CleanupConfig(per_run_cap=10, burst_per_run_cap=9)
    # Equal values are allowed (bursting never worse than calm).
    CleanupConfig(janitor_interval_seconds=300, burst_janitor_interval_seconds=300)
    CleanupConfig(per_run_cap=10, burst_per_run_cap=10)


# ----------------------------------------------------------- arrival rates


def test_arrival_rates_sum_bytes_and_prune_both_windows():
    coord = make_coordinator()
    now = time.monotonic()
    coord._arrival_times = [now - 10, now - 20, now - 4000]
    coord._arrival_bytes = [(now - 10, 100), (now - 20, 250), (now - 4000, 999)]

    arrivals, bytes_rate = coord._cleanup_arrival_rates(now)

    assert arrivals == 2.0
    assert bytes_rate == 350.0
    # Same 1 h cutoff for both lists: stale entries dropped from each.
    assert len(coord._arrival_times) == 2
    assert len(coord._arrival_bytes) == 2
    # Thin wrapper keeps the historic count-only API.
    assert coord._cleanup_arrivals_per_hour(now) == 2.0


# -------------------------------------------------------------- pressure


def test_pressure_decision():
    cfg = _cfg()
    coord = make_coordinator()

    # Calm and roomy: nothing trips.
    arrivals, bytes_rate, projected, urgent, bursting = coord._cleanup_pressure(cfg, ROOMY)
    assert (arrivals, bytes_rate, projected, urgent, bursting) == (0.0, 0.0, None, False, False)

    # Free space under the low watermark.
    *_, bursting = coord._cleanup_pressure(cfg, LOW)
    assert bursting is True

    # Arrivals at/above the burst rate, roomy disk.
    coord._arrival_times = [time.monotonic()] * int(cfg.burst_arrivals_per_hour)
    arrivals, _, projected, _, bursting = coord._cleanup_pressure(cfg, ROOMY)
    assert arrivals >= cfg.burst_arrivals_per_hour
    assert projected is None
    assert bursting is True

    # Recent bytes/hour project a full disk inside the lookahead.
    coord._arrival_times = [time.monotonic()]
    coord._arrival_bytes = [(time.monotonic(), 50 * GiB)]
    arrivals, bytes_rate, projected, _, bursting = coord._cleanup_pressure(cfg, ROOMY)
    assert bytes_rate == float(50 * GiB)
    assert projected == pytest.approx(90 / 50 * 3600)
    assert projected < cfg.fill_lookahead_hours * 3600
    assert bursting is True

    # 0 disables only the projected-fill trigger.
    off = _cfg(fill_lookahead_hours=0.0)
    *_, bursting = coord._cleanup_pressure(off, ROOMY)
    assert bursting is False

    # Urgent arrival (a release that did not fit) bursts even when roomy.
    coord._arrival_bytes = []
    coord._arrival_times = []
    coord._cleanup_urgent = True
    _, _, _, urgent, bursting = coord._cleanup_pressure(cfg, ROOMY)
    assert (urgent, bursting) == (True, True)


# ---------------------------------------------------------- cadence gate


@pytest.mark.anyio
async def test_burst_cadence_selected_from_free_space(tmp_path: Path):
    coord, store, _ = _harness(
        tmp_path, count=0, janitor_interval_seconds=3600, burst_janitor_interval_seconds=120
    )
    try:
        coord._run_source_cleanup = AsyncMock()
        coord._source_free_bytes = AsyncMock(return_value=LOW)
        # 200 s since the last run: calm would skip, burst must run.
        coord._cleanup_last_run_wall_ts = time.time() - 200
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 1
        kwargs = coord._run_source_cleanup.await_args.kwargs
        assert kwargs["bursting"] is True
        assert kwargs["free_bytes"] == LOW
        assert kwargs["arrivals"] == 0.0

        # Calm and roomy: the same elapsed time is below the hourly gate.
        coord._run_source_cleanup.reset_mock()
        coord._source_free_bytes = AsyncMock(return_value=ROOMY)
        coord._cleanup_last_run_wall_ts = time.time() - 200
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 0

        # ... and once the normal interval elapsed, a calm run happens.
        coord._cleanup_last_run_wall_ts = time.time() - 3601
        await coord._maybe_cleanup_source()
        assert coord._run_source_cleanup.await_count == 1
        assert coord._run_source_cleanup.await_args.kwargs["bursting"] is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_urgent_arrival_runs_early_and_is_consumed(tmp_path: Path):
    coord, store, _ = _harness(
        tmp_path, count=0, janitor_interval_seconds=3600, burst_janitor_interval_seconds=120
    )
    try:
        coord._run_source_cleanup = AsyncMock()
        coord._source_free_bytes = AsyncMock(return_value=ROOMY)
        coord._cleanup_urgent = True
        coord._cleanup_last_run_wall_ts = time.time() - 200

        await coord._maybe_cleanup_source()

        assert coord._run_source_cleanup.await_count == 1
        kwargs = coord._run_source_cleanup.await_args.kwargs
        assert kwargs["urgent"] is True and kwargs["bursting"] is True
        # The run consumed the flag.
        assert coord._cleanup_urgent is False
    finally:
        store.close()


# --------------------------------------------------------------- cap


@pytest.mark.anyio
async def test_burst_cap_used_when_bursting(tmp_path: Path):
    coord, store, members = _harness(
        tmp_path,
        count=4,
        per_run_cap=2,
        burst_per_run_cap=4,
        delete_stagger_seconds=0,
    )
    try:
        cfg = coord.cfg.cleanup

        def _verdicts():
            return [
                (float(i), float(m.size_bytes), store.get(m.infohash), [m])
                for i, m in enumerate(members)
            ]

        coord._cleanup_verdict = AsyncMock(side_effect=_verdicts())
        coord._delete_source_group = AsyncMock(return_value=True)
        await coord._run_source_cleanup(
            cfg,
            free_bytes=LOW,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=True,
            urgent=False,
        )
        assert coord._delete_source_group.await_count == 4

        # Calm run (mid disk, so no calm-and-roomy short-circuit) falls
        # back to per_run_cap.
        coord._delete_source_group.reset_mock()
        coord._cleanup_verdict = AsyncMock(side_effect=_verdicts())
        await coord._run_source_cleanup(
            cfg,
            free_bytes=MID,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=False,
            urgent=False,
        )
        assert coord._delete_source_group.await_count == 2
    finally:
        store.close()


# ----------------------------------------------------------- ordering


def test_sort_done_before_moving_under_pressure_else_oldest():
    coord = make_coordinator()
    now = dt.datetime.now(dt.UTC)

    def _row(state: State) -> TorrentState:
        return TorrentState(
            source_infohash="x" * 40, source_name="x", total_bytes=1, state=state, created_at=now
        )

    done_old = _row(State.DONE)
    done_big = _row(State.DONE)
    moving = _row(State.MOVING)
    candidates = [
        (10.0, 100.0, done_old, []),
        (20.0, 500.0, done_big, []),
        (30.0, 900.0, moving, []),
    ]

    pressured = coord._cleanup_sort_candidates(candidates, True)
    assert [c[2] for c in pressured] == [done_big, done_old, moving]
    assert [c[1] for c in pressured] == [500.0, 100.0, 900.0]

    calm = coord._cleanup_sort_candidates(candidates, False)
    assert [c[2] for c in calm] == [done_old, done_big, moving]


# ------------------------------------------------------------ urgency


def test_note_cleanup_arrival_sets_urgency_and_warns(caplog):
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.general.disk_safety_margin_bytes = 0
    coord._last_source_free_bytes = 1000

    with caplog.at_level("WARNING", logger="racing_sync.coordinator"):
        coord._note_cleanup_arrival(2000)
    assert coord._cleanup_urgent is True
    assert "exceeds VPS1 free" in caplog.text
    assert "prioritise cleanup" in caplog.text

    # A release that fits is not urgent.
    coord._cleanup_urgent = False
    caplog.clear()
    with caplog.at_level("WARNING", logger="racing_sync.coordinator"):
        coord._note_cleanup_arrival(500)
    assert coord._cleanup_urgent is False
    assert caplog.text == ""

    # Safety margin narrows the fit test: 700 > 1000 - 400.
    coord.cfg.general.disk_safety_margin_bytes = 400
    with caplog.at_level("WARNING", logger="racing_sync.coordinator"):
        coord._note_cleanup_arrival(700)
    assert coord._cleanup_urgent is True

    # Unknown free space never guesses (and never raises).
    coord._cleanup_urgent = False
    coord._last_source_free_bytes = None
    coord._note_cleanup_arrival(10**12)
    assert coord._cleanup_urgent is False


# --------------------------------------------------- MOVING urgent path


@pytest.mark.anyio
async def test_moving_verdict_allowed_under_urgent_above_low_watermark():
    cfg = _cfg()
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.cleanup = cfg
    coord._cleanup_idle_confirmed = MagicMock(return_value=True)
    coord._cleanup_ssd_bytes_present = AsyncMock(return_value=True)
    now = dt.datetime.now(dt.UTC)
    row = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        dest_infohash=H,
        total_bytes=FSIZE,
        state=State.MOVING,
        completed_at=now - dt.timedelta(hours=100),
    )
    group = [_member(H, FNAME, FSIZE)]

    # Roomy disk: the early path needs the urgent signal.
    assert (
        await coord._cleanup_verdict(
            row, group, 0.0, now, dry_run=True, free_bytes=ROOMY, urgent=False
        )
        is None
    )
    assert (
        await coord._cleanup_verdict(
            row, group, 0.0, now, dry_run=True, free_bytes=ROOMY, urgent=True
        )
        is not None
    )
    # The live flag is honoured when the caller does not pass one.
    coord._cleanup_urgent = True
    assert (
        await coord._cleanup_verdict(row, group, 0.0, now, dry_run=True, free_bytes=ROOMY)
        is not None
    )


# ---------------------------------------------------------- dry run


@pytest.mark.anyio
async def test_dry_run_burst_logs_but_deletes_nothing(tmp_path: Path, caplog):
    coord, store, members = _harness(
        tmp_path, count=2, dry_run=True, per_run_cap=2, burst_per_run_cap=4
    )
    try:
        coord._cleanup_verdict = AsyncMock(
            side_effect=[
                (float(i), float(m.size_bytes), store.get(m.infohash), [m])
                for i, m in enumerate(members)
            ]
        )
        with caplog.at_level("INFO", logger="racing_sync.coordinator"):
            await coord._run_source_cleanup(
                coord.cfg.cleanup,
                free_bytes=LOW,
                arrivals=0.0,
                bytes_rate=0.0,
                projected=None,
                bursting=True,
                urgent=False,
            )
        assert "burst mode" in caplog.text
        assert "would delete 2 content group(s)" in caplog.text
        coord.source_client.delete.assert_not_awaited()
    finally:
        store.close()
