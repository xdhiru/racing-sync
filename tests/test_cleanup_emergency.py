"""Cleanup EMERGENCY: a starving VPS1 acts now and skips the politeness delay.

A 13.5 h busy day landed 9-13 releases/hour on a small VPS1 scratch disk while
cleanup reclaimed less than it took: 19 rows were fuse-VERIFIED (safe to delete
by the project's own rule) and blocked purely by the 2 h grace floor, at 16 GiB
free, and the janitor only woke on its burst cadence. An emergency — free space
at/below `critical_watermark_free_bytes`, or a newly discovered release that
cannot fit — therefore (1) lets a fuse-VERIFIED DONE row become eligible without
waiting out grace or the idle fast lane, and (2) runs the next pass promptly
instead of up to a burst interval late, bounded so it never becomes a hot loop.

Both change TIMING only. Verification, protection, unknown stats, H&R minimums,
`.torrent` preservation, the `completed_at is None` adoption keep, the MOVING
early path and the activity gate (still owned by
`ignore_activity_under_pressure`) behave exactly as before.

Release names here are invented; no real log or config file is read.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig, CleanupConfig
from racing_sync.coordinator_cleanup import _EMERGENCY_WAKE_MIN_FLOOR_S
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024**3
FNAME = "Midnight.Harbor.S02E04.mkv"
FSIZE = 700
# CleanupConfig defaults: low 15 GiB, high 40 GiB, critical 8 GiB.
CRITICAL = 5 * GiB  # at/below the critical watermark -> emergency
ABOVE_CRIT = 10 * GiB  # below the low watermark, above critical: no emergency
NO_PRESSURE = 20 * GiB  # above the low watermark (only `urgent` can emergency)
H = "a" * 40


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(
        enabled=True,
        dry_run=False,
        janitor_startup_delay_seconds=0.0,
        delete_stagger_seconds=0.0,
        min_grace_hours=2.0,
    )
    base.update(over)
    return CleanupConfig(**base)


def _base_cfg(tmp_path: Path, **cleanup_over) -> MagicMock:
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.dest.save_path = tmp_path
    cfg.ssd.path = tmp_path / "ssd"
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_u"
    cfg.cleanup = _cleanup_cfg(**cleanup_over)
    return cfg


def _blob(name: str = FNAME, size: int = FSIZE) -> bytes:
    """Minimal single-file .torrent bytes the fuse-health check can decode."""
    nb = name.encode()
    info = (
        b"d6:lengthi"
        + str(size).encode()
        + b"e4:name"
        + str(len(nb)).encode()
        + b":"
        + nb
        + b"12:piece lengthi16384e6:pieces20:"
        + b"\x00" * 20
        + b"e"
    )
    return b"d8:announce4:test4:info" + info + b"e"


def _mounts(tmp_path: Path, *, with_file: bool = True, name: str = FNAME) -> None:
    """Fuse dirs the byte check can stat; `with_file` is the verification lever."""
    for folder in ("fuse", "fuse_u"):
        d = tmp_path / folder
        d.mkdir(exist_ok=True)
        if with_file:
            (d / name).write_bytes(b"\x00" * FSIZE)


def _member(
    h: str,
    *,
    name: str = FNAME,
    upspeed: int = 0,
    leechers: int = 0,
    ratio: float = 2.0,
    uploaded: int = 1024,
    seed_s: float = 3600.0,
) -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=FSIZE,
        state="seeding",
        progress=1.0,
        ratio=ratio,
        seeding_time_seconds=seed_s,
        total_uploaded_bytes=uploaded,
        trackers=[],
        upspeed_bps=upspeed,
        num_leechers=leechers,
        added_on=0,
    )


def _row(
    store: StateStore,
    h: str,
    *,
    state: State = State.DONE,
    completed_min_ago: float | None = 10.0,
    activity_min_ago: float | None = 1.0,
    name: str = FNAME,
) -> TorrentState:
    """Default shape: DONE 10 minutes ago, swarm seen active 1 minute ago.

    Both timing lanes are closed on purpose: grace is 2 h (`min_grace_hours`)
    and `idle_confirm_minutes = 45` cannot confirm a swarm observed a minute
    ago — so without an emergency this row is kept.
    """
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        dest_infohash=h,
        total_bytes=FSIZE,
        injected_private_hashes="",
        state=state,
        completed_at=(now - dt.timedelta(minutes=completed_min_ago))
        if completed_min_ago is not None
        else None,
        vps1_last_activity_at=(
            now - dt.timedelta(minutes=activity_min_ago) if activity_min_ago is not None else None
        ),
    )
    store.upsert(ts)
    return ts


class _Source:
    def __init__(self, members: list[Torrent]):
        self.members = members
        self.deleted: list[tuple[str, bool]] = []

    async def list_torrents(self, *, category=None, hashes=None):
        return list(self.members)

    async def delete(self, h: str, *, delete_files: bool = False):
        self.deleted.append((h.lower(), delete_files))
        self.members = [m for m in self.members if m.hash.lower() != h.lower()]


class _Dest:
    """Every hash it is handed answers as a fuse-seeded entry."""

    def __init__(self, hashes: list[str]):
        self.hashes = {h.lower() for h in hashes}

    async def list_torrents(self, *, category=None, hashes=None):
        want = {h.lower() for h in hashes} if hashes is not None else self.hashes
        return [
            Torrent(
                hash=h,
                name="x",
                category="racing",
                save_path="/mnt/fuse",
                size_bytes=1,
                state="seeding",
                progress=1.0,
            )
            for h in (want & self.hashes)
        ]


def _coord(store: StateStore, src, dest, cfg) -> object:
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = dest
    coord.sftp = None
    coord._stop = False
    return coord


async def _scan(coord, cfg, *, free: int | None, urgent: bool = False) -> dict:
    """One janitor run on the pressure signal the tick already computed."""
    return await coord._run_source_cleanup(
        cfg.cleanup,
        free_bytes=free,
        arrivals=0.0,
        bytes_rate=0.0,
        projected=None,
        bursting=True,
        urgent=urgent,
    )


def _setup(
    tmp_path: Path,
    *,
    h: str = H,
    name: str = FNAME,
    state: State = State.DONE,
    completed_min_ago: float | None = 10.0,
    activity_min_ago: float | None = 1.0,
    verified: bool = True,
    **cleanup_over,
):
    """One row wired into a coordinator with fuse entries and (optionally) bytes."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, **cleanup_over)
    _mounts(tmp_path, with_file=verified, name=name)
    _row(
        store,
        h,
        state=state,
        completed_min_ago=completed_min_ago,
        activity_min_ago=activity_min_ago,
        name=name,
    )
    # Blob storage is row-scoped: the row has to exist before its bytes land.
    assert store.set_blob(h, _blob(name)) is True
    src = _Source([_member(h, name=name)])
    return store, cfg, src, _Dest([h]), h


# ------------------------------------------------------- the predicate itself


def test_emergency_predicate_triggers_and_boundaries():
    cfg = _cleanup_cfg()
    trigger = make_coordinator()._cleanup_emergency_trigger
    assert trigger(cfg, CRITICAL, False) == "critical"
    assert trigger(cfg, int(CRITICAL), False) == "critical"  # at, not only below
    assert trigger(cfg, ABOVE_CRIT, False) is None
    # Only the arrival trigger, above every free-space watermark.
    assert trigger(cfg, NO_PRESSURE, True) == "arrival-did-not-fit"
    # The disk state outranks the arrival in the naming.
    assert trigger(cfg, CRITICAL, True) == "critical"
    # Unknown free space never guesses; a disabled watermark stays silent.
    assert trigger(cfg, None, False) is None
    assert trigger(_cleanup_cfg(critical_watermark_free_bytes=0), CRITICAL, False) is None
    # An unreadable config section is no emergency (fail closed).
    assert trigger(object(), CRITICAL, False) is None


def test_emergency_is_not_bursting_and_not_pressured():
    """Three honest names for three different signals (no overloading)."""
    coord = make_coordinator()
    coord.cfg = MagicMock()
    cfg = _cleanup_cfg()
    coord._arrival_times = []
    coord._arrival_bytes = []
    # Spam intake alone: bursting, but nothing is starving -> no emergency.
    busy = time.monotonic()
    coord._arrival_times = [busy] * 9
    _, _, _, urgent, bursting = coord._cleanup_pressure(cfg, NO_PRESSURE)
    assert (urgent, bursting) == (False, True)
    assert coord._cleanup_emergency_trigger(cfg, NO_PRESSURE, urgent) is None
    # A critical disk: bursting AND emergency, with its own name and reason.
    _, _, _, urgent2, bursting2 = coord._cleanup_pressure(cfg, CRITICAL)
    assert (urgent2, bursting2) == (False, True)
    assert coord._cleanup_emergency_trigger(cfg, CRITICAL, urgent2) == "critical"


# ------------------------------------------------------- grace bypass (CHANGE 1)


@pytest.mark.anyio
async def test_critical_watermark_rescues_a_verified_row(tmp_path: Path, caplog):
    """The 13.5 h report: verified rows held by a 2 h politeness delay."""
    store, cfg, src, dest, h = _setup(tmp_path)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert [d[0] for d in src.deleted] == [h]
        assert summary["deleted"] == 1
        text = caplog.text
        assert f"emergency: deleting {FNAME} " in text
        assert "after 600s of a 7200s grace" in text
        assert f"VPS1 free={CRITICAL} B" in text
        assert "trigger=critical" in text
        assert "fuse-verified, grace and idle-confirm skipped" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_above_critical_without_urgency_still_waits_out_grace(tmp_path: Path, caplog):
    """The same row, the same second, no emergency: today's keep, unchanged."""
    store, cfg, src, dest, _h = _setup(tmp_path)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=ABOVE_CRIT)
        assert src.deleted == []
        assert summary["deleted"] == 0
        assert "grace 1" in summary["reason"]
        assert "emergency" not in summary["reason"]
        assert "emergency: deleting" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_arrival_that_did_not_fit_rescues_a_verified_row(tmp_path: Path, caplog):
    """The `_cleanup_urgent` trigger, above every free-space watermark."""
    store, cfg, src, dest, h = _setup(tmp_path)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=NO_PRESSURE, urgent=True)
        assert [d[0] for d in src.deleted] == [h]
        assert summary["deleted"] == 1
        assert "trigger=arrival-did-not-fit" in caplog.text
        assert f"VPS1 free={NO_PRESSURE} B" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_calm_run_above_the_watermarks_is_unchanged(tmp_path: Path):
    """No emergency and no burst: the row waits, exactly as before."""
    store, cfg, src, dest, _h = _setup(tmp_path)
    coord = _coord(store, src, dest, cfg)
    try:
        summary = await coord._run_source_cleanup(
            cfg.cleanup,
            free_bytes=NO_PRESSURE,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=False,
            urgent=False,
        )
        assert src.deleted == []
        assert "grace 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_never_rescues_an_unverified_row(tmp_path: Path, caplog):
    """Ordering: verification runs first and still keeps on any doubt."""
    store, cfg, src, dest, _h = _setup(tmp_path, verified=False)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert src.deleted == []
        assert "verification 1" in summary["reason"]
        assert "emergency: deleting" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_keeps_an_adopted_row_without_completed_at(tmp_path: Path, caplog):
    """Recovery-adopted DONE rows: no proof VPS2 ever served them."""
    store, cfg, src, dest, _h = _setup(tmp_path, completed_min_ago=None)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert src.deleted == []
        assert "grace 1" in summary["reason"]
        assert "emergency: deleting" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_does_not_bypass_the_activity_keep(tmp_path: Path, caplog):
    """Switch off: peers attached still keep the VPS1 copy under an emergency."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path)
    _mounts(tmp_path)
    _row(store, H)
    assert store.set_blob(H, _blob()) is True
    src = _Source([_member(H, leechers=3, upspeed=200_000)])
    coord = _coord(store, src, _Dest([H]), cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert src.deleted == []
        assert "activity 1" in summary["reason"]
        assert "emergency on" in summary["reason"]
        assert "emergency: deleting" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_activity_keep_lifts_only_with_the_operators_switch(tmp_path: Path):
    """Same row, `ignore_activity_under_pressure = true`: emergency deletes it."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, ignore_activity_under_pressure=True)
    _mounts(tmp_path)
    _row(store, H)
    assert store.set_blob(H, _blob()) is True
    src = _Source([_member(H, leechers=3, upspeed=200_000)])
    coord = _coord(store, src, _Dest([H]), cfg)
    try:
        summary = await _scan(coord, cfg, free=CRITICAL)
        assert [d[0] for d in src.deleted] == [H]
        assert summary["deleted"] == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_stats_and_protection_still_keep_under_emergency(tmp_path: Path):
    """Neither gate is a timing gate: an emergency does not reach them."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, protected_patterns=["midnight.harbor"])
    _mounts(tmp_path)
    blind = "a" * 40
    guarded = "b" * 40
    _row(store, blind, name="Quiet.Shore.S01E01.mkv")
    _row(store, guarded, name=FNAME)
    assert store.set_blob(blind, _blob()) is True
    assert store.set_blob(guarded, _blob()) is True
    src = _Source(
        [
            _member(blind, name="Quiet.Shore.S01E01.mkv", ratio=0.0, uploaded=0, seed_s=0.0),
            _member(guarded, name=FNAME),
        ]
    )
    coord = _coord(store, src, _Dest([blind, guarded]), cfg)
    try:
        summary = await _scan(coord, cfg, free=CRITICAL)
        assert src.deleted == []
        assert "unknown stats 1" in summary["reason"]
        assert "protection 1" in summary["reason"]
        assert "emergency on" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_logs_exactly_one_line_per_row(tmp_path: Path, caplog):
    """Bounded output: three rescued rows, three INFO lines, no per-scan spam."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path)
    _mounts(tmp_path)
    members, hashes = [], []
    for i in range(3):
        h = str(i + 1) * 40
        name = f"Midnight.Harbor.S02E0{i}.mkv"
        _row(store, h, name=name)
        assert store.set_blob(h, _blob(name)) is True
        for folder in (tmp_path / "fuse", tmp_path / "fuse_u"):
            (folder / name).write_bytes(b"\x00" * FSIZE)
        members.append(_member(h, name=name))
        hashes.append(h)
    src = _Source(members)
    coord = _coord(store, src, _Dest(hashes), cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert summary["deleted"] == 3
        rescue_lines = [r for r in caplog.records if "emergency: deleting" in r.getMessage()]
        assert len(rescue_lines) == 3
        assert all("\n" not in r.getMessage() for r in rescue_lines)
    finally:
        store.close()


# --------------------------------------------------- MOVING stays as it was


@pytest.mark.anyio
async def test_moving_row_still_needs_the_idle_confirm_under_emergency(tmp_path: Path):
    """Emergency skips the DONE grace wait; the MOVING early path is untouched."""
    store, cfg, src, dest, _h = _setup(tmp_path, state=State.MOVING)
    coord = _coord(store, src, dest, cfg)
    try:
        summary = await _scan(coord, cfg, free=CRITICAL)
        assert src.deleted == []
        # Idle-confirm failed (swarm seen a minute ago): kept by grace, as before.
        assert "grace 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_row_without_pressure_is_still_kept(tmp_path: Path):
    """No pressure, no urgency: MOVING keeps its own `not pressured` keep."""
    store, cfg, src, dest, _h = _setup(
        tmp_path, state=State.MOVING, activity_min_ago=None, completed_min_ago=None
    )
    coord = _coord(store, src, dest, cfg)
    try:
        summary = await coord._run_source_cleanup(
            cfg.cleanup,
            free_bytes=NO_PRESSURE,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=False,
            urgent=False,
        )
        assert src.deleted == []
        assert "not pressured 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_early_path_still_works_under_urgency(tmp_path: Path):
    """The pre-existing urgent MOVING path is unchanged by the emergency work."""
    store, cfg, src, dest, _h = _setup(
        tmp_path, state=State.MOVING, activity_min_ago=None, completed_min_ago=None
    )
    coord = _coord(store, src, dest, cfg)
    coord._cleanup_idle_confirmed = MagicMock(return_value=True)
    coord._cleanup_ssd_bytes_present = AsyncMock(return_value=True)
    try:
        summary = await coord._run_source_cleanup(
            cfg.cleanup,
            free_bytes=NO_PRESSURE,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=True,
            urgent=True,
        )
        assert summary["deleted"] == 1
    finally:
        store.close()


# ------------------------------------------------------ report-line contract


def test_blocked_reason_appends_emergency_state():
    coord = make_coordinator()
    blocked = {"grace": 19}
    assert coord._cleanup_blocked_reason(blocked) == "nothing eligible (grace 19)"
    line = coord._cleanup_blocked_reason(blocked, emergency=True)
    assert line == "nothing eligible (grace 19; emergency on)"
    assert "\n" not in line
    # No keeps at all: still one honest line.
    assert coord._cleanup_blocked_reason({}, emergency=True) == "nothing eligible (emergency on)"
    assert coord._cleanup_blocked_reason({}, emergency=False) == "nothing eligible"
    # A wide breakdown stays one bounded line with the suffix on.
    wide = {"grace": 19, "activity": 17, "verification": 9, "x" * 300: 4}
    capped = coord._cleanup_blocked_reason(wide, emergency=True)
    assert capped.startswith("nothing eligible (") and capped.endswith("; emergency on)")
    assert len(capped) <= 160 + len("nothing eligible ()")
    assert "\n" not in capped


@pytest.mark.anyio
async def test_report_line_names_the_starving_state(tmp_path: Path, caplog):
    """The exact starvation line, now self-explanatory in the operator log."""
    store, cfg, src, dest, _h = _setup(tmp_path, verified=False)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=CRITICAL)
        assert summary["reason"] == "nothing eligible (verification 1; emergency on)"
        assert "cleanup janitor: nothing eligible (verification 1; emergency on)" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_critical_disk_warning_states_the_bypass(tmp_path: Path, caplog):
    """The CRITICAL line promises what the emergency actually does."""
    store, cfg, src, dest, _h = _setup(tmp_path)
    coord = _coord(store, src, dest, cfg)
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            await _scan(coord, cfg, free=CRITICAL)
        assert "VPS1 disk CRITICAL" in caplog.text
        assert "fuse-verified DONE rows skip the grace wait" in caplog.text
    finally:
        store.close()


# ------------------------------------------------- act now (CHANGE 2): the gate


def _tick_setup(tmp_path: Path, **cleanup_over):
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, **cleanup_over)
    coord = _coord(store, _Source([]), _Dest([]), cfg)
    coord._run_source_cleanup = AsyncMock(return_value={"deleted": 0})
    return store, cfg, coord


@pytest.mark.anyio
async def test_emergency_bypasses_the_wall_clock_gate(tmp_path: Path, caplog):
    """100 s into a 300 s burst cadence: an emergency runs instead of waiting."""
    store, cfg, coord = _tick_setup(tmp_path)
    coord._source_free_bytes = AsyncMock(return_value=CRITICAL)
    coord._cleanup_last_run_wall_ts = time.time() - 100
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            await coord._maybe_cleanup_source()
        coord._run_source_cleanup.assert_awaited_once()
        assert "emergency (critical) — running" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_arrival_urgency_also_bypasses_the_gate(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    coord._source_free_bytes = AsyncMock(return_value=NO_PRESSURE)
    coord._cleanup_urgent = True
    coord._cleanup_emergency_wake = True
    coord._cleanup_last_run_wall_ts = time.time() - 100
    try:
        await coord._maybe_cleanup_source()
        coord._run_source_cleanup.assert_awaited_once()
        assert coord._run_source_cleanup.await_args.kwargs["urgent"] is True
        # Both emergency signals are consumed by the pass they triggered.
        assert coord._cleanup_urgent is False
        assert coord._cleanup_emergency_wake is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_emergency_keeps_the_cadence_gate(tmp_path: Path):
    """Without an emergency the self-gate is byte-for-byte today's behaviour."""
    store, cfg, coord = _tick_setup(tmp_path)
    coord._source_free_bytes = AsyncMock(return_value=ABOVE_CRIT)
    coord._cleanup_last_run_wall_ts = time.time() - 100
    try:
        assert await coord._maybe_cleanup_source() is None
        coord._run_source_cleanup.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_gate_is_bounded_by_the_spacing(tmp_path: Path):
    """No hot loop: an emergency pass never runs closer than burst / 5."""
    store, cfg, coord = _tick_setup(tmp_path, burst_janitor_interval_seconds=300)
    coord._source_free_bytes = AsyncMock(return_value=CRITICAL)
    spacing = coord._cleanup_emergency_spacing_seconds(cfg.cleanup)
    assert spacing == 60.0  # 300 / 5
    coord._cleanup_last_run_wall_ts = time.time() - (spacing - 30)
    try:
        assert await coord._maybe_cleanup_source() is None
        coord._run_source_cleanup.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_spacing_is_derived_and_floored(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    try:
        coord.cfg.cleanup = _cleanup_cfg(burst_janitor_interval_seconds=100)
        # burst/5 = 20 s: above the floor, derived from the existing cadence.
        assert coord._cleanup_emergency_spacing_seconds(coord.cfg.cleanup) == 20.0
        coord.cfg.cleanup = _cleanup_cfg(burst_janitor_interval_seconds=60)
        # The smallest legal burst interval: burst/5 = 12 s, so the floor wins.
        assert (
            coord._cleanup_emergency_spacing_seconds(coord.cfg.cleanup)
            == _EMERGENCY_WAKE_MIN_FLOOR_S
        )
    finally:
        store.close()


@pytest.mark.anyio
async def test_arrival_trigger_sets_a_flag_and_spawns_nothing():
    """Discovery signals the janitor with a flag — never a second pass."""
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.general.disk_safety_margin_bytes = 0
    coord._last_source_free_bytes = 1000
    before = len(asyncio.all_tasks())
    coord._note_cleanup_arrival(2000)
    assert coord._cleanup_urgent is True
    assert coord._cleanup_emergency_wake is True
    assert len(asyncio.all_tasks()) == before


# ------------------------------------------------- act now (CHANGE 2): the wake


def test_emergency_wake_seconds_only_shorten_under_emergency(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    coord._last_source_free_bytes = NO_PRESSURE
    try:
        assert coord._cleanup_emergency_wake_seconds(cfg.cleanup, 300.0) == 300.0
        # Critical watermark: shortened to burst / 5 (default 300 / 5 = 60).
        coord._last_source_free_bytes = CRITICAL
        assert coord._cleanup_emergency_wake_seconds(cfg.cleanup, 300.0) == 60.0
        # Arrival trigger alone, above every watermark.
        coord._last_source_free_bytes = NO_PRESSURE
        coord._cleanup_urgent = True
        assert coord._cleanup_emergency_wake_seconds(cfg.cleanup, 300.0) == 60.0
        # Never longer than the cadence the loop already chose.
        coord._last_source_free_bytes = CRITICAL
        assert coord._cleanup_emergency_wake_seconds(cfg.cleanup, 45.0) == 45.0
        # No fresh probe on the hot path: a probe failure changes nothing.
        coord._source_free_bytes = AsyncMock(side_effect=AssertionError("probed again"))
        assert coord._cleanup_emergency_wake_seconds(cfg.cleanup, 300.0) == 60.0
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_wait_consumes_the_flag_and_returns_promptly(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    coord._cleanup_emergency_wake = True
    try:
        started = time.monotonic()
        await asyncio.wait_for(coord._cleanup_emergency_wait(600.0), timeout=5.0)
        assert time.monotonic() - started < 1.0
        assert coord._cleanup_emergency_wake is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_wait_sleeps_the_cadence_without_a_trigger(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    try:
        started = time.monotonic()
        await coord._cleanup_emergency_wait(1.2)
        elapsed = time.monotonic() - started
        assert 1.0 <= elapsed < 2.5
        assert getattr(coord, "_cleanup_emergency_wake", False) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_wait_honours_a_stop_request(tmp_path: Path):
    store, cfg, coord = _tick_setup(tmp_path)
    coord._stop = True
    coord._cleanup_emergency_wake = True
    try:
        await asyncio.wait_for(coord._cleanup_emergency_wait(600.0), timeout=2.0)
        # Stopping: nothing slept, and nothing was consumed.
        assert coord._cleanup_emergency_wake is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_janitor_loop_wakes_on_the_flag_under_its_lock(tmp_path: Path):
    """The real loop: prompt wake, one pass at a time, no second janitor task."""
    store, cfg, coord = _tick_setup(tmp_path)
    coord._stop = False
    seen: list[bool] = []
    overlap = {"live": 0, "max": 0}

    async def _step():
        overlap["live"] += 1
        overlap["max"] = max(overlap["max"], overlap["live"])
        seen.append(coord._janitor_lock.locked())
        overlap["live"] -= 1
        if len(seen) >= 2:
            coord._stop = True

    coord._janitor_step = _step
    # Cleanup cadence long enough that only the wake can produce pass two.
    cfg.cleanup = _cleanup_cfg(janitor_interval_seconds=3600, burst_janitor_interval_seconds=600)
    tasks_before = len(asyncio.all_tasks())
    started = time.monotonic()
    try:

        async def _nudge():
            await asyncio.sleep(0.05)
            coord._cleanup_emergency_wake = True

        nudge = asyncio.create_task(_nudge())
        await asyncio.wait_for(coord._janitor_loop(), timeout=10.0)
        await nudge
        assert time.monotonic() - started < 5.0  # a 600 s cadence never would
        assert seen == [True, True]  # every pass ran under the janitor lock
        assert overlap["max"] == 1  # and never two at once
        # The loop consumed the wake, so it cannot re-fire forever.
        assert coord._cleanup_emergency_wake is False
        assert len(asyncio.all_tasks()) <= tasks_before + 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_emergency_passes_stay_spaced_when_nothing_is_eligible(tmp_path: Path):
    """The bound that matters: an emergency with nothing to reclaim does not spin.

    Two back-to-back emergency ticks: the first runs, the second is refused by
    the spacing gate, and the wake flag is consumed either way so the loop's
    sliced sleep cannot return instantly forever.
    """
    store, cfg, coord = _tick_setup(tmp_path)
    coord._source_free_bytes = AsyncMock(return_value=CRITICAL)
    coord._cleanup_last_run_wall_ts = time.time() - 400
    coord._cleanup_emergency_wake = True
    try:
        await coord._maybe_cleanup_source()
        coord._run_source_cleanup.assert_awaited_once()
        first_stamp = coord._cleanup_last_run_wall_ts
        coord._cleanup_emergency_wake = True
        assert await coord._maybe_cleanup_source() is None
        coord._run_source_cleanup.assert_awaited_once()  # still exactly one pass
        assert coord._cleanup_emergency_wake is False  # consumed, not left armed
        assert coord._cleanup_last_run_wall_ts == first_stamp
    finally:
        store.close()


# --------------------------------------------------------- config stays honest


def test_no_new_config_keys():
    """Emergency reuses the watermark that already existed."""
    assert "emergency_watermark_free_bytes" not in AppConfig.model_fields
    assert "emergency_janitor_interval_seconds" not in CleanupConfig.model_fields
    cfg = CleanupConfig()
    assert 0 < cfg.critical_watermark_free_bytes < cfg.low_watermark_free_bytes
