"""Relocation: what the reclaim path does when the eligible inventory runs out.

The hole this closes: VPS1 is most likely to be full of exactly the releases
the janitor may NOT delete (no cross-seed found yet, or still mid-pipeline), so
a space-driven pass can delete every secured row it is allowed to, stop short of
its free-space target, and leave dozens of releases squatting the disk. The
answer is not deletion — it is RELOCATION: ask the push lane to copy those bytes
onto the very remote VPS2 seeds from, which turns them into deletable content
for a later pass.

Pinned here:

* the pass asks only when it is SHORT and its deletions are done (never when it
  reached its target, never under the per-run cap, never in dry-run, never with
  `[cleanup].enabled` off, never on a disk it cannot measure);
* ACUTE pressure (a rescue, or an emergency pass) relocates the SMALLEST
  releases first — the fastest conversion into a deletable row and the fewest
  half-finished copies; ORDINARY pressure (a target top-up) relocates the
  LARGEST first — the most bytes per push job;
* one request per row, bounded by `max_concurrent_pushes` minus the live pushes
  AND by a quarter of the pass's own per-run cap (floor 1);
* dedupe reuses the push lane's OWN memos — a live checkpoint, the per-hash
  refusal backoff and the launch-failure streak — never a parallel one;
* a watch-origin drop and an operator-held row are never chosen;
* the lane's refusal reason reaches the operator's WARNING verbatim, and
  nothing here ever deletes, forgets or abandons an unsecured release;
* `rescue_vps1_room` now has three outcomes: room made (resume), relocating
  (no resume), nothing possible.

Release names here are invented; no real log or config file is read.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

# Byte-sized test rows: one secured group frees ROW_BYTES, so the band
# arithmetic stays readable (trigger 1000, target 2500, free 0).
ROW_BYTES = 1000
MARGIN = 100
TRIGGER = 1000
TARGET = 2500
FREE = 0
PUSH_OK = "pushing 1 file(s) from VPS1 to seedremote:unsorted"
PUSH_NO = "push refused: VPS1's copy is incomplete and no file of it is complete yet"


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(
        enabled=True,
        dry_run=False,
        janitor_startup_delay_seconds=0.0,
        delete_stagger_seconds=0.0,
        low_watermark_free_bytes=TRIGGER,
        high_watermark_free_bytes=TARGET,
        critical_watermark_free_bytes=0,
        # Both caps at 12 so the relocation ceiling (12 // 4 = 3) is the same
        # whether or not this tick is bursting.
        per_run_cap=12,
        burst_per_run_cap=12,
    )
    base.update(over)
    return CleanupConfig(**base)


def _base_cfg(tmp_path: Path, *, push_on: bool = True, **cleanup_over) -> MagicMock:
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.dest.save_path = tmp_path
    cfg.ssd.path = tmp_path / "ssd"
    cfg.ssd.safety_margin_bytes = 0
    cfg.general.disk_safety_margin_bytes = MARGIN
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_u"
    cfg.cleanup = _cleanup_cfg(**cleanup_over)
    # The push lane's own consent, read exactly (`is True`) by the runtime view.
    cfg.source.rclone_push.enabled = True if push_on else False
    cfg.source.rclone_push.push_on_janitor_pressure = True if push_on else False
    cfg.source.rclone_push.max_concurrent_pushes = 8
    cfg.source.rclone_push.poll_seconds = 120
    cfg.source.rclone_push.timeout_seconds = 21600
    return cfg


def _blob(name: str, size: int = ROW_BYTES) -> bytes:
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


def _mounts(tmp_path: Path, names: list[str]) -> None:
    """Fuse stand-ins the byte check can stat: only listed names exist."""
    for folder in ("fuse", "fuse_u"):
        d = tmp_path / folder
        d.mkdir(exist_ok=True)
        for name in names:
            if not (d / name).exists():
                (d / name).write_bytes(b"\x00" * ROW_BYTES)


def _member(h: str, name: str, size: int = ROW_BYTES, *, state: str = "seeding") -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=size,
        state=state,
        progress=1.0 if state == "seeding" else 0.0,
        ratio=2.0,
        seeding_time_seconds=3600.0,
        total_uploaded_bytes=1024,
        trackers=[],
        upspeed_bps=0,
        dlspeed_bps=0,
        num_leechers=0,
        added_on=0,
    )


class _Source:
    """VPS1 racing client: the live listing, deletes and resumes (with logs)."""

    def __init__(self, members: list[Torrent]):
        self.members = members
        self.deleted: list[tuple[str, bool]] = []
        self.resumed: list[str] = []

    async def list_torrents(self, *, category=None, hashes=None):
        if hashes is not None:
            want = {h.lower() for h in hashes}
            return [m for m in self.members if m.hash.lower() in want]
        return list(self.members)

    async def delete(self, h: str, *, delete_files: bool = False):
        self.deleted.append((h.lower(), delete_files))
        self.members = [m for m in self.members if m.hash.lower() != h.lower()]

    async def resume(self, h: str) -> None:
        self.resumed.append(h.lower())


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


def _hash(i: int) -> str:
    return f"{i:040x}"


def _secured_name(i: int) -> str:
    return f"Example.Show.S01E{20 + i}.1080p.WEB-DL-GROUP.mkv"


def _unsecured_name(i: int) -> str:
    return f"Example.Show.S02E{30 + i}.1080p.WEB-DL-GROUP-GROUP2.mkv"


def _secured_row(store: StateStore, h: str, name: str) -> None:
    now = dt.datetime.now(dt.UTC)
    store.upsert(
        TorrentState(
            source_infohash=h,
            source_name=name,
            dest_infohash=h,
            total_bytes=ROW_BYTES,
            injected_private_hashes="",
            state=State.DONE,
            completed_at=now - dt.timedelta(seconds=60),
            vps1_last_activity_at=now - dt.timedelta(seconds=60),
            fuse_verified=1,
        )
    )


def _unsecured_row(
    store: StateStore,
    h: str,
    name: str,
    size: int,
    *,
    state: State = State.WAITING_INDEXER,
    **fields,
) -> TorrentState:
    """A release whose bytes are on VPS1 and which VPS2 has NOT secured."""
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        total_bytes=size,
        state=state,
        created_at=now - dt.timedelta(seconds=60),
        **fields,
    )
    store.upsert(ts)
    return ts


def _harness(
    tmp_path: Path,
    *,
    secured: int = 2,
    unsecured: list[tuple[str, int]] | None = None,
    free: int | None = FREE,
    push_on: bool = True,
    unsecured_state: State = State.WAITING_INDEXER,
    **cleanup_over,
):
    """`secured` proven-DONE rows + unsecured squatting rows, wired end to end.

    The push lane is stubbed at its ENTRY point (`start_source_push`), so these
    tests prove who is chosen, in which order, how often and with what report —
    the lane's own checks stay the lane's (see `tests/test_coordinator_push.py`).

    Returns (coord, store, src, cfg, sec_hashes, uns_hashes, asked).
    """
    unsecured = list(unsecured or [])
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, push_on=push_on, **cleanup_over)
    sec_hashes = [_hash(i + 1) for i in range(secured)]
    sec_names = [_secured_name(i) for i in range(secured)]
    _mounts(tmp_path, sec_names)
    members = []
    for h, name in zip(sec_hashes, sec_names, strict=True):
        _secured_row(store, h, name)
        assert store.set_blob(h, _blob(name)) is True
        members.append(_member(h, name))
    uns_hashes = []
    for j, (name, size) in enumerate(unsecured):
        h = _hash(100 + j)
        uns_hashes.append(h)
        _unsecured_row(store, h, name, size, state=unsecured_state)
        members.append(_member(h, name, size, state="downloading"))
    src = _Source(members)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = _Dest(sec_hashes + uns_hashes)
    coord.sftp = None
    coord._stop = False
    coord._last_source_free_bytes = free
    coord._source_free_bytes = AsyncMock(return_value=free)
    coord._is_watch_row = MagicMock(
        side_effect=lambda ts: (getattr(ts, "cross_seed_source", "") or "") == "watch-dir"
    )
    asked: list[tuple[str, int, str]] = []

    async def _ask(ts, *, reason="manual"):
        asked.append(
            (
                (getattr(ts, "source_infohash", "") or "").lower(),
                int(getattr(ts, "total_bytes", 0) or 0),
                reason,
            )
        )
        return True, PUSH_OK

    coord.start_source_push = AsyncMock(side_effect=_ask)
    coord.start_source_push.return_value = (True, PUSH_OK)
    return coord, store, src, cfg, sec_hashes, uns_hashes, asked


async def _pass(coord, cfg, *, free: int | None = FREE, urgent: bool = False, **over) -> dict:
    """One janitor run on the pressure signal the tick already computed."""
    return await coord._run_source_cleanup(
        cfg.cleanup,
        free_bytes=free,
        arrivals=0.0,
        bytes_rate=0.0,
        projected=None,
        bursting=True,
        urgent=urgent,
        **over,
    )


class _InfoCapture:
    """INFO+ capture limited to this module's logger."""

    def __init__(self, level: int = logging.INFO):
        self._level = level

    def __enter__(self):
        self.records: list[logging.LogRecord] = []
        self._handler = logging.Handler()
        self._handler.emit = self.records.append
        self._log = logging.getLogger("racing_sync.coordinator")
        self._log.addHandler(self._handler)
        self._prev = self._log.level
        self._log.setLevel(self._level)
        return self.records

    def __exit__(self, *exc):
        self._log.removeHandler(self._handler)
        self._log.setLevel(self._prev)
        return False


def _lines(records, needle: str) -> list[str]:
    return [r.getMessage() for r in records if needle in r.getMessage()]


# --------------------------------------------------------------------------- #
# the pass asks when its deletions ran out
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_short_pass_after_deleting_everything_eligible_requests_relocation(tmp_path):
    """Deletions done, target still out of reach → the hatch is asked, and said."""
    coord, store, src, cfg, sec, uns, asked = _harness(
        tmp_path, secured=2, unsecured=[(_unsecured_name(0), 5000)]
    )
    try:
        with _InfoCapture() as records:
            summary = await _pass(coord, cfg)
        # Both secured groups went (free 0 -> 2000), and 500 is still missing.
        assert len(src.deleted) == 2
        assert summary["shortfall_bytes"] == 500
        assert summary["relocate_requested"] == 1
        assert [h for h, _s, _r in asked] == [uns[0]]
        assert summary["relocate_note"]
        info = _lines(records, "relocation requested")
        assert len(info) == 1
        assert "1/1" in info[0] and "at stake" in info[0]
        # No WARNING: relocation is in flight, so the gap is being closed.
        assert not _lines(records, "relocation unavailable")
        assert _lines(records, "still unsecured") == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_short_pass_with_nothing_deletable_at_all_still_asks(tmp_path):
    """The headline failure: zero secured rows, a disk full of unsecured ones."""
    coord, store, src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 4000) for i in range(3)]
    )
    try:
        summary = await _pass(coord, cfg)
        assert src.deleted == []
        assert "nothing eligible" in summary["reason"]
        assert summary["relocate_requested"] == 3
        assert {h for h, _s, _r in asked} == set(uns)
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_relocation_when_the_pass_reached_its_target(tmp_path):
    """Room restored by deletion alone: the hatch is never consulted."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=6, unsecured=[(_unsecured_name(i), 4000) for i in range(3)]
    )
    try:
        summary = await _pass(coord, cfg)
        assert summary["target_reached"] is True
        assert summary["relocate_requested"] == 0
        assert asked == []
        assert summary["relocate_note"] == ""
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_cap_stopped_pass_does_not_ask(tmp_path):
    """Its own deletions are not finished yet: relocation would compete."""
    coord, store, src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=8,
        unsecured=[(_unsecured_name(0), 9000)],
        per_run_cap=1,
        burst_per_run_cap=1,
    )
    try:
        summary = await _pass(coord, cfg)
        assert len(src.deleted) == 1
        assert summary["eligible_remaining"] > 0
        assert summary["relocate_requested"] == 0
        assert asked == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the runway decides the order: acute = smallest first, ordinary = largest
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_acute_pressure_relocates_the_smallest_first(tmp_path):
    """An emergency pass wants rows converted FAST: smallest-first."""
    sizes = [7000, 1000, 4000]
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), s) for i, s in enumerate(sizes)]
    )
    try:
        # `_cleanup_urgent` is the arrival-did-not-fit emergency, so this pass
        # is ACUTE (`emergency == "arrival-did-not-fit"`).
        summary = await _pass(coord, cfg, urgent=True)
        assert summary["relocate_requested"] == 3
        assert [s for _h, s, _r in asked] == [1000, 4000, 7000]
    finally:
        store.close()


@pytest.mark.anyio
async def test_critical_watermark_is_acute_too(tmp_path):
    """Free at/below `critical_watermark_free_bytes`: same smallest-first rule."""
    sizes = [6000, 2000, 5000]
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=0,
        unsecured=[(_unsecured_name(i), s) for i, s in enumerate(sizes)],
        critical_watermark_free_bytes=100,
    )
    try:
        summary = await _pass(coord, cfg, free=50)
        assert summary["relocate_requested"] == 3
        assert [s for _h, s, _r in asked] == [2000, 5000, 6000]
    finally:
        store.close()


@pytest.mark.anyio
async def test_ordinary_pressure_relocates_the_largest_first(tmp_path):
    """A target top-up (no emergency, no arrival waiting) wants the big ones."""
    sizes = [1000, 7000, 4000]
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), s) for i, s in enumerate(sizes)]
    )
    try:
        # Free below the trigger, so the pass does reclaim, but no emergency and
        # no rescue `target_override`: ORDINARY pressure, so biggest-first.
        summary = await _pass(coord, cfg, free=500)
        assert summary["relocate_requested"] == 3
        assert [s for _h, s, _r in asked] == [7000, 4000, 1000]
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_lane_counts_as_acute_and_prefers_the_smallest(tmp_path):
    """The rescue's `target_override` is acute by definition (an arrival waits)."""
    sizes = [8000, 1200, 3000]
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), s) for i, s in enumerate(sizes)]
    )
    try:
        summary = await _pass(coord, cfg, free=0, target_override=9000, reclaim_override=True)
        assert summary["relocate_requested"] == 3
        assert [s for _h, s, _r in asked] == [1200, 3000, 8000]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# dedupe: existing memos only, never a parallel one
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_a_row_with_a_live_push_is_not_requested(tmp_path):
    """The checkpoint table is the single answer to "is this already going?"."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(3)]
    )
    try:
        store.claim_push(
            uns[1],
            src_dir="/vps1/data",
            target_remote="seedremote:unsorted",
            deadline_at=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=30),
            files_total=1,
        )
        coord._live_push_cache = None
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 2
        assert {h for h, _s, _r in asked} == {uns[0], uns[2]}
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_recently_refused_hash_is_skipped(tmp_path):
    """The lane's own refusal backoff defers the hash; no new memo exists."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(2)]
    )
    try:
        coord._push_pressure_backoff = {uns[0]: time.monotonic() + 600.0}
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
        assert {h for h, _s, _r in asked} == {uns[1]}
        # A refusal is written back into that SAME memo (`_note_pressure_refusal`
        # from the push lane), so the next pass leaves the release alone instead
        # of re-asking it every tick.
        coord.start_source_push = AsyncMock(return_value=(False, PUSH_NO))
        coord._push_pressure_backoff = {}
        coord._live_push_cache = None
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        assert coord.start_source_push.await_count == 2
        assert set(coord._push_pressure_backoff) >= set(uns)
        coord.start_source_push = AsyncMock(return_value=(True, PUSH_OK))
        coord._live_push_cache = None
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        assert coord.start_source_push.await_count == 0, "the deferred hashes are not re-asked"
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_launch_throttled_hash_is_skipped(tmp_path):
    """A VPS1 lane that died twice for this group is left alone (existing memo)."""
    coord, store, _src, cfg, _sec, uns, _asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(2)]
    )
    try:
        coord._push_launch_failures = {uns[0]: (2, time.monotonic() + 600.0, time.monotonic(), "x")}
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the bounds
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_a_two_hundred_row_backlog_cannot_fire_two_hundred_pushes(tmp_path):
    """The per-pass ceiling is a quarter of the pass's own per-run cap (floor 1)."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=0,
        unsecured=[(_unsecured_name(i), 1000 + i) for i in range(200)],
        burst_per_run_cap=12,
    )
    try:
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 3  # 12 // 4
        assert len(asked) == 3
    finally:
        store.close()


@pytest.mark.anyio
async def test_the_ceiling_follows_the_burst_cap(tmp_path):
    """A burst pass may relocate as much as it may delete: 25 // 4 = 6."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=0,
        unsecured=[(_unsecured_name(i), 1000 + i) for i in range(40)],
        burst_per_run_cap=25,
    )
    try:
        summary = await _pass(coord, cfg, urgent=True)
        assert summary["relocate_requested"] == 6
        assert len(asked) == 6
    finally:
        store.close()


@pytest.mark.anyio
async def test_the_lane_concurrency_budget_bounds_the_requests(tmp_path):
    """Never more than `max_concurrent_pushes` minus what is already live."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 1000) for i in range(10)]
    )
    try:
        cfg.source.rclone_push.max_concurrent_pushes = 2
        # One slot is already taken by a live checkpoint.
        store.claim_push(
            uns[0],
            src_dir="/vps1/data",
            target_remote="seedremote:unsorted",
            deadline_at=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=30),
            files_total=1,
        )
        coord._live_push_cache = None
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
        assert len(asked) == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_one_request_per_row_even_when_repeated(tmp_path):
    """`seen` dedupes by hash: a row is asked about at most once per pass."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(0), 3000)]
    )
    try:
        # A second row claiming the SAME VPS1 hash (a sibling entry).
        _unsecured_row(store, uns[0], _unsecured_name(1), 3000)
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
        assert len(asked) == 1
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the gates
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_dry_run_requests_nothing(tmp_path):
    """A rehearsal may not start real copies, however short the disk is."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(3)], dry_run=True
    )
    try:
        summary = await _pass(coord, cfg)
        assert summary["dry_run"] is True
        assert summary["relocate_requested"] == 0
        assert asked == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_disabled_cleanup_requests_nothing(tmp_path):
    """The app may not touch space (or bytes) it is not allowed to."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=0,
        unsecured=[(_unsecured_name(i), 3000) for i in range(3)],
        enabled=False,
    )
    try:
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        assert asked == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_unmeasurable_disk_requests_nothing(tmp_path):
    """No reading, no claim — not even a relocation request (automatic pass)."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(3)], free=None
    )
    try:
        summary = await _pass(coord, cfg, free=None)
        assert summary["relocate_requested"] == 0
        assert asked == []
        # The operator-forced pass on the same blind disk still asks for nothing.
        summary = await _pass(coord, cfg, free=None, force=True)
        assert summary["relocate_requested"] == 0
        assert asked == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_lane_off_leaves_the_whole_path_inert(tmp_path):
    """With `[source.rclone_push]` off: no request, and no warning either."""
    coord, store, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path,
        secured=0,
        unsecured=[(_unsecured_name(i), 3000) for i in range(3)],
        push_on=False,
    )
    try:
        with _InfoCapture(logging.WARNING) as records:
            summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        assert asked == []
        assert _lines(records, "relocation") == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# never the operator's rows, never a watch drop, never a row VPS2 already has
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_a_watch_origin_row_is_never_chosen(tmp_path):
    """Watch-dir drops answer to their own election, not to the janitor."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(2)]
    )
    try:
        ts = store.get(uns[1], include_blob=False)
        ts.cross_seed_source = "watch-dir"
        store.upsert(ts)
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
        assert {h for h, _s, _r in asked} == {uns[0]}
    finally:
        store.close()


@pytest.mark.anyio
async def test_operator_held_and_already_working_rows_are_left_alone(tmp_path):
    """`skipped` is a hold; DOWNLOADING/MOVING/DONE/FAILED are not the lane's."""
    coord, store, _src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(5)]
    )
    try:
        held = store.get(uns[0], include_blob=False)
        held.skipped = 1
        store.upsert(held)
        for i, state in enumerate(
            (State.DOWNLOADING, State.MOVING, State.DONE, State.FAILED), start=1
        ):
            ts = store.get(uns[i], include_blob=False)
            ts.state = state
            store.upsert(ts)
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        assert asked == []
        # Nothing was deleted or dropped for trying: the rows all still exist.
        assert all(store.get(h, include_blob=False) is not None for h in uns)
    finally:
        store.close()


@pytest.mark.anyio
async def test_only_rows_the_vps1_listing_still_holds_are_offered(tmp_path):
    """Presence is evidence: a row VPS1 no longer lists is not a candidate."""
    coord, store, src, cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3000) for i in range(2)]
    )
    try:
        src.members = [m for m in src.members if m.hash.lower() != uns[1]]
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 1
        assert {h for h, _s, _r in asked} == {uns[0]}
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_row_without_a_vps1_hash_is_not_offered(tmp_path):
    """The push keys a copy on the 40-char VPS1 infohash; without one, no ask."""
    store = StateStore(tmp_path / "s.db")
    coord, _s2, _src, cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(0), 3000)]
    )
    try:
        bad = _hash(500)[:39]
        _unsecured_row(store, bad, _unsecured_name(9), 3000)
        coord.source_client.members.append(_member(bad, _unsecured_name(9), 3000))
        await _pass(coord, cfg)
        assert all(len(h) == 40 for h, _s, _r in asked)
        assert bad not in {h for h, _s, _r in asked}
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the operator's line: the lane's own words, and nothing more
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_refusal_reason_reaches_the_warning_verbatim(tmp_path):
    """One WARNING per pass, quoting `start_source_push` exactly, no titles."""
    mib = 1024**2
    coord, store, _src, cfg, _sec, _uns, _asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 3 * mib) for i in range(2)]
    )
    try:
        coord.start_source_push = AsyncMock(return_value=(False, PUSH_NO))
        with _InfoCapture() as records:
            summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 0
        warns = _lines(records, "relocation unavailable")
        assert len(warns) == 1, "exactly one warning line per pass"
        assert warns[0].endswith(PUSH_NO), "the lane's reason, verbatim"
        assert "6.0 MiB still unsecured on VPS1" in warns[0]
        for record in records:
            assert _unsecured_name(0) not in record.getMessage()
            assert _unsecured_name(1) not in record.getMessage()
        # The refusal also lands in the summary the rescue lane reads.
        assert PUSH_NO in summary["relocate_note"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_warning_when_nothing_is_squatting_the_disk(tmp_path):
    """Short but with no unsecured rows: the existing INFO reason is enough."""
    coord, store, _src, cfg, _sec, _uns, _asked = _harness(tmp_path, secured=0, unsecured=[])
    try:
        with _InfoCapture(logging.WARNING) as records:
            await _pass(coord, cfg)
        assert _lines(records, "relocation") == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_the_relocation_info_line_names_no_release(tmp_path):
    """Counts and bytes only, matching the rescue/push logging discipline."""
    names = [_unsecured_name(i) for i in range(2)]
    coord, store, _src, cfg, _sec, _uns, _asked = _harness(
        tmp_path, secured=0, unsecured=[(n, 4000) for n in names]
    )
    try:
        with _InfoCapture() as records:
            await _pass(coord, cfg)
        lines = _lines(records, "relocation requested")
        assert len(lines) == 1
        assert all(n not in lines[0] for n in names)
        assert "800" not in lines[0] or "at stake" in lines[0]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the safety statement: nothing here deletes or abandons an unsecured release
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_an_unsecured_release_is_never_deleted_or_forgotten(tmp_path):
    """The hatch only asks the lane; the sweep still deletes secured rows only."""
    coord, store, src, cfg, sec, uns, _asked = _harness(
        tmp_path,
        secured=2,
        unsecured=[(_unsecured_name(i), 5000) for i in range(2)],
    )
    deletes: list[str] = []
    row_deletes: list[str] = []
    real_delete_group = coord._delete_source_group

    async def _spy_group(ts, group, cfg_, dry_run):
        deletes.append((ts.source_infohash or "").lower())
        return await real_delete_group(ts, group, cfg_, dry_run)

    real_store_delete = store.delete

    def _spy_store_delete(h):
        row_deletes.append((h or "").lower())
        return real_store_delete(h)

    try:
        coord._delete_source_group = _spy_group
        store.delete = _spy_store_delete
        summary = await _pass(coord, cfg)
        assert summary["relocate_requested"] == 2
        # Only the secured groups were ever offered to the delete path...
        assert set(deletes) <= {h.lower() for h in sec}
        assert set(deletes).isdisjoint(set(uns))
        # ...nothing was removed from the client or the state for them...
        assert set(h for h, _d in src.deleted).isdisjoint(set(uns))
        assert row_deletes == []
        # ...and both unsecured rows are exactly where they were.
        for h in uns:
            ts = store.get(h, include_blob=False)
            assert ts is not None
            assert ts.state == State.WAITING_INDEXER
            assert any(m.hash.lower() == h for m in src.members)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# rescue_vps1_room: three outcomes
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_rescue_outcome_one_room_made_resumes(tmp_path):
    coord, store, src, _cfg, _sec, _uns, asked = _harness(tmp_path, secured=4)
    starved = _unsecured_row(store, "a" * 40, "Example.Show.S03E01.1080p.WEB-DL-GROUP.mkv", 1500)
    try:
        made, freed, reason = await coord.rescue_vps1_room(1500, why="test")
        assert made is True
        assert freed == 2 * ROW_BYTES
        assert "freed" in reason
        done = await coord.rescue_starved_release(starved, 1500, why="test", entries=[])
        assert done is True
        assert src.resumed == ["a" * 40]
        assert asked == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_outcome_two_relocating_requests_and_does_not_resume(tmp_path):
    """Nothing secured to delete, unsecured rows to move: relocate, do NOT resume."""
    coord, store, src, _cfg, _sec, uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 5000) for i in range(2)]
    )
    starved = _unsecured_row(store, "a" * 40, "Example.Show.S03E02.1080p.WEB-DL-GROUP.mkv", 1500)
    try:
        made, freed, reason = await coord.rescue_vps1_room(1500, why="test")
        assert (made, freed) == (False, 0)
        assert reason.startswith("relocating, will retry")
        assert "2 unsecured VPS1 release(s)" in reason
        assert {h for h, _s, _r in asked} == set(uns)
        done = await coord.rescue_starved_release(starved, 1500, why="test", entries=[])
        assert done is False
        assert src.resumed == [], "no room yet, so the arrival is not resumed"
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_outcome_two_reports_progress_not_a_failure(tmp_path):
    """A relocation in flight is an INFO, not a WARNING (nothing was lost)."""
    coord, store, _src, _cfg, _sec, _uns, _asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 5000) for i in range(2)]
    )
    starved = _unsecured_row(store, "a" * 40, "Example.Show.S03E03.1080p.WEB-DL-GROUP.mkv", 1500)
    try:
        with _InfoCapture() as records:
            await coord.rescue_starved_release(starved, 1500, why="test", entries=[])
        assert _lines(records, "waiting on relocation")
        assert not _lines(records, "no room made")
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_outcome_two_when_room_was_made_but_is_not_enough(tmp_path):
    coord, store, src, _cfg, _sec, uns, asked = _harness(
        tmp_path, secured=1, unsecured=[(_unsecured_name(i), 5000) for i in range(2)]
    )
    try:
        made, freed, reason = await coord.rescue_vps1_room(9000, why="test")
        assert made is False
        assert freed == ROW_BYTES
        assert reason.startswith("relocating, will retry")
        assert "still" in reason and "short" in reason
        assert len(asked) == 2
        assert {h for h, _s, _r in asked} == set(uns)
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_outcome_three_nothing_possible_is_unchanged(tmp_path):
    """With the lane off there is no relocation: today's refusal, word for word."""
    coord, store, src, _cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=1, unsecured=[(_unsecured_name(0), 5000)], push_on=False
    )
    try:
        made, freed, reason = await coord.rescue_vps1_room(9000, why="test")
        assert made is False
        assert freed == ROW_BYTES
        assert not reason.startswith("relocating")
        assert "still" in reason and "short" in reason
        assert asked == []
        assert len(src.deleted) == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_still_refuses_an_unmeasured_disk_without_relocating(tmp_path):
    coord, store, _src, _cfg, _sec, _uns, asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(0), 5000)], free=None
    )
    try:
        made, freed, reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert (made, freed) == (False, 0)
        assert "unmeasured" in reason
        assert asked == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the chooser, called directly (the documented signature)
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_chooser_returns_counts_and_a_one_line_summary(tmp_path):
    coord, store, _src, cfg, _sec, uns, _asked = _harness(
        tmp_path, secured=0, unsecured=[(_unsecured_name(i), 2500) for i in range(4)]
    )
    try:
        rows = coord._reclaim_rows_on_vps1(await offload_rows(store), set(by_hash(coord)))
        started, line = await coord._reclaim_request_pushes(
            rows, 6000, why="unit", acute=True, pass_cap=12
        )
        assert started == 3  # ceiling 12 // 4
        assert "3/4" in line and "unit" in line
        assert "10000 B at stake" in line
        # An empty need means the pass already has its room.
        started, line = await coord._reclaim_request_pushes(
            rows, 0, why="unit", acute=False, pass_cap=12
        )
        assert (started, line) == (0, "")
        assert uns[0]  # sanity: the rows really were the unsecured ones
    finally:
        store.close()


def by_hash(coord) -> set[str]:
    return {m.hash.lower() for m in coord.source_client.members}


async def offload_rows(store) -> list[TorrentState]:
    return list(store.all())
