"""VPS1 rescue: an arrival that cannot fit must be made room for, and resumed.

The capacity model this pins down: VPS1 is fed directly by autobrr, so "there
was no room" is not something the app can prevent — it is a race that is about
to error. Two independent triggers live in the discovery poll (arithmetic: a
newly seen release bigger than the LAST MEASURED free space minus its safety
margin; stalled: a tracked release that has not advanced across two
consecutive polls at zero download rate while the measured free space sits
inside the margin), and both hand over to `rescue_vps1_room`, which reclaims
ONLY content already proven secured on VPS2, biggest-first, stopping the
moment the arrival fits, under the same per-run cap. An unmeasurable disk is
never a claim: no reading, no rescue. Nothing here relaxes a gate — the rescue
skips only the waiting.

Release names here are invented; no real log or config file is read.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

# Byte-sized test rows: one group frees ROW_BYTES, so the arithmetic below is
# readable (`need = size + MARGIN`).
ROW_BYTES = 1000
MARGIN = 100
FREE = 0
TRIGGER = 1000
TARGET = 2500
FNAME = "Example.Show.S01E10.1080p.WEB-DL-GROUP.mkv"
STARVED_NAME = "Example.Show.S01E99.1080p.WEB-DL-GROUP.mkv"


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(
        enabled=True,
        dry_run=False,
        janitor_startup_delay_seconds=0.0,
        delete_stagger_seconds=0.0,
        low_watermark_free_bytes=TRIGGER,
        high_watermark_free_bytes=TARGET,
        critical_watermark_free_bytes=0,
    )
    base.update(over)
    return CleanupConfig(**base)


def _base_cfg(tmp_path: Path, **cleanup_over) -> MagicMock:
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.dest.save_path = tmp_path
    cfg.ssd.path = tmp_path / "ssd"
    cfg.ssd.safety_margin_bytes = 0
    cfg.general.disk_safety_margin_bytes = MARGIN
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_u"
    cfg.cleanup = _cleanup_cfg(**cleanup_over)
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


def _member(
    h: str,
    name: str = FNAME,
    size: int = ROW_BYTES,
    *,
    state: str = "seeding",
    progress: float = 1.0,
    dlspeed: int = 0,
) -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=size,
        state=state,
        progress=progress,
        ratio=2.0,
        seeding_time_seconds=3600.0,
        total_uploaded_bytes=1024,
        trackers=[],
        upspeed_bps=0,
        dlspeed_bps=dlspeed,
        num_leechers=0,
        added_on=0,
    )


class _Source:
    """VPS1 racing client: lists, deletes and resumes (with call logs)."""

    def __init__(self, members: list[Torrent], *, fail_resume: bool = False):
        self.members = members
        self.deleted: list[tuple[str, bool]] = []
        self.resumed: list[str] = []
        self.fail_resume = fail_resume

    async def list_torrents(self, *, category=None, hashes=None):
        if hashes is not None:
            want = {h.lower() for h in hashes}
            return [m for m in self.members if m.hash.lower() in want]
        return list(self.members)

    async def delete(self, h: str, *, delete_files: bool = False):
        self.deleted.append((h.lower(), delete_files))
        self.members = [m for m in self.members if m.hash.lower() != h.lower()]

    async def resume(self, h: str) -> None:
        if self.fail_resume:
            raise RuntimeError("resume refused by the daemon")
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


def _done_row(
    store: StateStore,
    h: str,
    *,
    name: str,
    verified: bool = True,
    size: int = ROW_BYTES,
) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        dest_infohash=h,
        total_bytes=size,
        injected_private_hashes="",
        state=State.DONE,
        completed_at=now - dt.timedelta(seconds=60),
        vps1_last_activity_at=now - dt.timedelta(seconds=60),
        fuse_verified=1 if verified else 0,
    )
    store.upsert(ts)
    return ts


def _new_row(store: StateStore, h: str, *, name: str, size: int) -> TorrentState:
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        total_bytes=size,
        state=State.NEW,
    )
    store.upsert(ts)
    return ts


def _harness(
    tmp_path: Path,
    count: int = 4,
    *,
    unverified: int = 0,
    free: int | None = FREE,
    fail_resume: bool = False,
    **cleanup_over,
):
    """`count` DONE rows (distinct names) + matching VPS1 members and mount bytes.

    `unverified` marks that many of the LAST rows as never-fuse-verified (no
    mount bytes either), which is the lever for "a rescue frees only secured
    content".
    """
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, **cleanup_over)
    names = [f"Example.Show.S01E{10 + i}.1080p.WEB-DL-GROUP.mkv" for i in range(count)]
    hashes = [_hash(i + 1) for i in range(count)]
    secured_names = names[: count - unverified]
    _mounts(tmp_path, secured_names)
    members: list[Torrent] = []
    for i, (h, name) in enumerate(zip(hashes, names, strict=True)):
        _done_row(store, h, name=name, verified=i < count - unverified)
        assert store.set_blob(h, _blob(name)) is True
        members.append(_member(h, name))
    src = _Source(members, fail_resume=fail_resume)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = _Dest(hashes)
    coord.sftp = None
    coord._stop = False
    coord._last_source_free_bytes = free
    coord._source_free_bytes = AsyncMock(return_value=free)
    return coord, store, src, cfg, hashes


# ------------------------------------------------------- rescue_vps1_room


@pytest.mark.anyio
async def test_rescue_stops_at_size_plus_margin_with_rows_left(tmp_path: Path):
    """Biggest-first toward `size + margin`, never past the arrival's need."""
    # need = 1500 + 100 = 1600: two 1000-byte groups, three stay seeding.
    coord, store, src, _cfg, hashes = _harness(tmp_path, count=5)
    try:
        made, freed, reason = await coord.rescue_vps1_room(1500, why="test")
        assert made is True
        assert freed == 2 * ROW_BYTES
        assert len(src.deleted) == 2
        assert "1.6 KiB" in reason or "need" in reason
        # The untouched rows are still on VPS1 and still tracked.
        assert {d[0] for d in src.deleted}.isdisjoint(set(hashes[2:]))
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_frees_only_secured_rows(tmp_path: Path):
    """An unverified row survives a rescue however badly the disk needs room."""
    coord, store, src, _cfg, hashes = _harness(tmp_path, count=3, unverified=1)
    try:
        # need = 1000 + 100: both secured groups go (1000 then 2000 >= 1100),
        # the unverified one is never even offered to the sweep.
        made, freed, _reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert made is True
        assert freed == 2 * ROW_BYTES
        assert len(src.deleted) == 2
        assert hashes[2] not in {d[0] for d in src.deleted}
        assert store.get(hashes[2], include_blob=False).state == State.DONE
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_is_bounded_by_the_per_run_cap(tmp_path: Path):
    """One rescue call cannot wipe the box: the cap stops it, room or not."""
    coord, store, src, _cfg, _hashes = _harness(
        tmp_path, count=8, per_run_cap=1, burst_per_run_cap=1
    )
    try:
        made, freed, reason = await coord.rescue_vps1_room(8000, why="test")
        assert made is False
        assert len(src.deleted) == 1
        assert freed == ROW_BYTES
        assert "short" in reason and "cap" in reason
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_refuses_when_janitor_disabled(tmp_path: Path):
    """The app cannot free space it is not allowed to touch — and says which switch."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=2, enabled=False)
    try:
        made, freed, reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert (made, freed) == (False, 0)
        assert "[cleanup].enabled" in reason
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_refuses_an_unmeasured_disk(tmp_path: Path):
    """No reading, no claim: a blind VPS1 is not a reason to delete anything."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=3, free=None)
    try:
        made, freed, reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert (made, freed) == (False, 0)
        assert "unmeasured" in reason
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_dry_run_frees_nothing_and_is_reported(tmp_path: Path):
    """A rehearsal cannot claim room: nothing is deleted, so nothing resumes."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=3, dry_run=True)
    try:
        made, freed, reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert (made, freed) == (False, 0)
        assert "dry_run" in reason
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_reports_nothing_eligible_without_deleting(tmp_path: Path):
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=1, unverified=1)
    try:
        made, freed, reason = await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert (made, freed) == (False, 0)
        assert "nothing could be reclaimed" in reason
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_does_not_touch_cadence_or_urgency(tmp_path: Path):
    """A rescue pass leaves the janitor's own clock and flag alone."""
    coord, store, _src, cfg, _hashes = _harness(tmp_path, count=3)
    coord._cleanup_urgent = True
    try:
        await coord.rescue_vps1_room(ROW_BYTES, why="test")
        assert coord._cleanup_urgent is True
        assert coord._cleanup_last_run_wall() == 0.0
        assert cfg.cleanup.per_run_cap == _cleanup_cfg().per_run_cap
    finally:
        store.close()


# -------------------------------------------------------- resume behaviour


@pytest.mark.anyio
async def test_resume_called_once_after_a_rescue_that_made_room(tmp_path: Path):
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        done = await coord.rescue_starved_release(
            starved,
            1500,
            why="new arrival does not fit VPS1",
            entries=[_member("a" * 40, STARVED_NAME, 1500, state="downloading", progress=0.0)],
        )
        assert done is True
        assert src.resumed == ["a" * 40]
    finally:
        store.close()


@pytest.mark.anyio
async def test_resume_not_called_when_nothing_was_freed(tmp_path: Path):
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=2, unverified=2)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        done = await coord.rescue_starved_release(
            starved, 1500, why="new arrival does not fit VPS1", entries=[]
        )
        assert done is False
        assert src.resumed == []
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_resume_refused_for_an_operator_held_row(tmp_path: Path):
    """`skipped` (the /hold_ flag) is the operator's decision: never overridden."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    starved.skipped = 1
    store.upsert(starved)
    try:
        done = await coord.rescue_starved_release(
            starved, 1500, why="new arrival does not fit VPS1", entries=[]
        )
        assert done is False
        # Room WAS made (that part is not the operator's call); only the
        # resume is refused, because the hold is theirs.
        assert len(src.deleted) == 2
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_resume_refused_for_a_paused_client_entry(tmp_path: Path):
    """A torrent the operator paused in the racing client stays paused."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        done = await coord.rescue_starved_release(
            starved,
            1500,
            why="new arrival does not fit VPS1",
            entries=[_member("a" * 40, STARVED_NAME, 1500, state="paused", progress=0.0)],
        )
        assert done is False
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_failing_resume_is_logged_not_propagated(tmp_path: Path, caplog):
    """A resume that raises must never travel back into the discovery poll."""
    coord, store, _src, _cfg, _hashes = _harness(tmp_path, count=4, fail_resume=True)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            done = await coord.rescue_starved_release(
                starved, 1500, why="new arrival does not fit VPS1", entries=[]
            )
        assert done is False
        assert any("resume failed" in r.getMessage() for r in caplog.records)
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_window_bounds_attempts_per_release(tmp_path: Path):
    """One attempt per hash per window: a doomed release is not swept every poll."""
    coord, store, src, _cfg, _hashes = _harness(
        tmp_path, count=4, per_run_cap=1, burst_per_run_cap=1
    )
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=8000)
    try:
        first = await coord.rescue_starved_release(
            starved, 8000, why="new arrival does not fit VPS1", entries=[]
        )
        deleted_after_first = len(src.deleted)
        assert first is False
        second = await coord.rescue_starved_release(
            starved, 8000, why="new arrival does not fit VPS1", entries=[]
        )
        assert second is False
        assert len(src.deleted) == deleted_after_first
        # Outside the window the release is eligible again.
        coord._rescue_attempts["a" * 40] -= coord._rescue_window_seconds(_cleanup_cfg()) * 2
        third = await coord.rescue_starved_release(
            starved, 8000, why="new arrival does not fit VPS1", entries=[]
        )
        assert third is False
        assert len(src.deleted) > deleted_after_first
    finally:
        store.close()


@pytest.mark.anyio
async def test_stall_memo_is_bounded_and_pruned(tmp_path: Path):
    """The per-hash memos never grow with the number of releases ever seen."""
    coord, store, _src, _cfg, _hashes = _harness(tmp_path, count=1)
    try:
        for i in range(400):
            coord._note_racing_stall(_hash(i), 0.5, 0)
        assert len(coord._rescue_stalls) <= coord._RESCUE_MEMO_CAP
        for i in range(400):
            coord._note_rescue_attempt(_hash(i))
        assert len(coord._rescue_attempts) <= coord._RESCUE_MEMO_CAP
        # Expired entries are dropped on insert, not kept forever.
        for h in list(coord._rescue_stalls):
            prog, seen, streak = coord._rescue_stalls[h]
            coord._rescue_stalls[h] = (prog, seen - 10_000.0, streak)
        coord._note_racing_stall("f" * 40, 0.5, 0)
        assert "f" * 40 in coord._rescue_stalls
        assert len(coord._rescue_stalls) == 1
    finally:
        store.close()


# ------------------------------------------------------ detection at poll


@pytest.mark.anyio
async def test_arithmetic_trigger_rescues_a_big_first_seen_release(tmp_path: Path):
    """A newly discovered release that cannot fit is made room for immediately."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4)
    big = _member(
        "b" * 40,
        "Example.Movie.2026.2160p.WEB-DL-GROUP.mkv",
        1500,
        state="downloading",
        progress=0.0,
    )
    try:
        created = await coord._process_source_group([big])
        assert created is not None
        assert src.resumed == ["b" * 40]
        assert len(src.deleted) == 2  # 1500 + 100 = 1600 -> two 1000-byte groups
        assert store.get("b" * 40, include_blob=False).state == State.NEW
    finally:
        store.close()


@pytest.mark.anyio
async def test_arithmetic_trigger_is_silent_without_a_reading(tmp_path: Path):
    """Free space unknown: no rescue, no guess, no delete (the capacity rule)."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=None)
    big = _member(
        "b" * 40,
        "Example.Movie.2026.2160p.WEB-DL-GROUP.mkv",
        1500,
        state="downloading",
        progress=0.0,
    )
    try:
        created = await coord._process_source_group([big])
        assert created is not None
        assert src.resumed == []
        assert src.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_arithmetic_trigger_does_not_fire_when_it_fits(tmp_path: Path):
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=900)
    small = _member(
        "b" * 40,
        "Example.Movie.2026.1080p.WEB-DL-GROUP.mkv",
        800,
        state="downloading",
        progress=0.0,
    )
    try:
        await coord._process_source_group([small])
        assert src.deleted == []
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_stalled_trigger_needs_two_non_advancing_polls(tmp_path: Path):
    """Same progress + zero download rate twice, inside the margin: rescue."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=50)
    starved = _new_row(store, "c" * 40, name=STARVED_NAME, size=2000)
    group = [_member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.2, dlspeed=0)]
    try:
        await coord._process_source_group(group)
        assert starved.state == State.NEW
        assert src.deleted == [], "one stalled poll is not yet an emergency"
        assert src.resumed == []
        await coord._process_source_group(group)
        assert len(src.deleted) == 2  # remaining 1600 + margin 100 -> two groups
        assert src.resumed == ["c" * 40]
    finally:
        store.close()


@pytest.mark.anyio
async def test_stalled_trigger_ignores_a_moving_race(tmp_path: Path):
    """Progress advanced between polls: the release is alive, nothing is freed."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=50)
    _new_row(store, "c" * 40, name=STARVED_NAME, size=2000)
    try:
        await coord._process_source_group(
            [_member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.2, dlspeed=0)]
        )
        await coord._process_source_group(
            [_member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.35, dlspeed=0)]
        )
        await coord._process_source_group(
            [_member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.36, dlspeed=0)]
        )
        assert src.deleted == []
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_stalled_trigger_ignores_a_downloading_race(tmp_path: Path):
    """A non-zero download rate resets the streak, whatever progress says."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=50)
    _new_row(store, "c" * 40, name=STARVED_NAME, size=2000)
    member = _member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.2, dlspeed=0)
    try:
        # Poll 1: first sighting of the streak, nothing is claimed yet.
        await coord._process_source_group([member])
        assert src.deleted == []
        # Poll 2 would arm it (two non-advancing polls) — but the client is
        # writing again, so the streak resets and nothing is freed.
        member.dlspeed_bps = 500_000
        await coord._process_source_group([member])
        member.dlspeed_bps = 0
        await coord._process_source_group([member])
        assert src.deleted == [], "the reset must not leave a stale streak"
        assert src.resumed == []
        # Two genuinely non-advancing polls later it is an emergency again.
        await coord._process_source_group([member])
        assert len(src.deleted) == 2
        assert src.resumed == ["c" * 40]
    finally:
        store.close()


@pytest.mark.anyio
async def test_stalled_trigger_needs_the_disk_to_be_inside_the_margin(tmp_path: Path):
    """Stalled with room above the margin is a dead swarm, not a full disk."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=MARGIN + 5_000)
    _new_row(store, "c" * 40, name=STARVED_NAME, size=2000)
    group = [_member("c" * 40, STARVED_NAME, 2000, state="downloading", progress=0.2, dlspeed=0)]
    try:
        await coord._process_source_group(group)
        await coord._process_source_group(group)
        await coord._process_source_group(group)
        assert src.deleted == []
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_stalled_trigger_skips_a_done_row(tmp_path: Path):
    """A completed race is not starved: its progress will never move again."""
    coord, store, src, _cfg, hashes = _harness(tmp_path, count=3)
    group = [_member(hashes[0], FNAME, ROW_BYTES, state="seeding", progress=1.0, dlspeed=0)]
    try:
        for _ in range(4):
            await coord._process_source_group(group)
        assert src.resumed == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_whole_poll_rescues_the_release_that_cannot_fit(tmp_path: Path):
    """The rescue rides the normal discovery tick: one call, groups keep flowing."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4, free=900)
    big = _member(
        "b" * 40,
        "Example.Movie.2026.2160p.WEB-DL-GROUP.mkv",
        1500,
        state="downloading",
        progress=0.0,
    )
    other = _member(
        "d" * 40,
        "Example.Series.2026.1080p.WEB-DL-GROUP.mkv",
        200,
        state="downloading",
        progress=0.0,
    )
    try:
        fresh = await coord._poll_source_racing([big, other])
        assert {t.source_infohash for t in fresh} == {"b" * 40, "d" * 40}
        assert src.resumed == ["b" * 40], "only the release that could not fit"
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_rescue_that_raises_never_reaches_the_poll(tmp_path: Path):
    """A exploding rescue is a WARNING: the tick and its rows survive."""
    coord, store, _src, _cfg, _hashes = _harness(tmp_path, count=4)
    coord.rescue_starved_release = AsyncMock(side_effect=RuntimeError("rescue exploded"))
    big = _member(
        "b" * 40,
        "Example.Movie.2026.2160p.WEB-DL-GROUP.mkv",
        1500,
        state="downloading",
        progress=0.0,
    )
    try:
        fresh = await coord._poll_source_racing([big])
        assert len(fresh) == 1
        assert coord.rescue_starved_release.await_count == 1
    finally:
        store.close()


# ---------------------------------------------------------------- logging


@pytest.mark.anyio
async def test_rescue_success_line_is_one_info_and_names_no_release(tmp_path: Path):
    """One INFO on a rescue that made room and resumed; no title in the log."""
    coord, store, src, _cfg, _hashes = _harness(tmp_path, count=4)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        with caplog_at_info() as records:
            await coord.rescue_starved_release(
                starved, 1500, why="new arrival does not fit VPS1", entries=[]
            )
        assert src.resumed == ["a" * 40]
        rescue_lines = [r.getMessage() for r in records if "rescue:" in r.getMessage()]
        assert len(rescue_lines) == 1
        assert "freed" in rescue_lines[0] and "resumed" in rescue_lines[0]
        assert STARVED_NAME not in rescue_lines[0]
        # The lane's own lines never print a release title either; the janitor
        # lines it delegates to keep their existing (name-carrying) format.
        assert all(STARVED_NAME not in m for m in rescue_lines)
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_room_made_is_one_warning(tmp_path: Path, caplog):
    coord, store, _src, _cfg, _hashes = _harness(tmp_path, count=1, unverified=1)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            await coord.rescue_starved_release(
                starved, 1500, why="new arrival does not fit VPS1", entries=[]
            )
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("no room made" in m for m in warnings)
        assert all(STARVED_NAME not in m for m in warnings)
    finally:
        store.close()


class caplog_at_info:
    """INFO-level capture limited to this module's logger."""

    def __enter__(self):
        self._handler = logging.Handler()
        self.records: list[logging.LogRecord] = []
        self._handler.emit = self.records.append
        self._log = logging.getLogger("racing_sync.coordinator")
        self._log.addHandler(self._handler)
        self._level = self._log.level
        self._log.setLevel(logging.INFO)
        return self.records

    def __exit__(self, *exc):
        self._log.removeHandler(self._handler)
        self._log.setLevel(self._level)
        return False


# ------------------------------------------------- relocation (rescue outcome 2)
#
# A rescue whose deletions cannot help has a second move: hand the unsecured
# releases to the push lane, which copies them onto the seed remote and makes
# them deletable later. That is outcome 2 of the three `rescue_vps1_room` can
# report — room made (resume), relocating (do NOT resume, the room is not there
# yet), nothing possible (today's refusal). The push lane's own consent keys are
# the only thing that has to be switched on; no new config exists for this.


def _open_push_lane(cfg: MagicMock) -> None:
    """The existing `[source.rclone_push]` consent, read exactly by the runtime."""
    cfg.source.rclone_push.enabled = True
    cfg.source.rclone_push.push_on_janitor_pressure = True
    cfg.source.rclone_push.max_concurrent_pushes = 4


def _squatting(store: StateStore, src, h: str, *, name: str, size: int) -> TorrentState:
    """An unsecured release whose bytes are still on VPS1 (row + client member)."""
    ts = _new_row(store, h, name=name, size=size)
    src.members.append(_member(h, name, size, state="downloading", progress=0.0))
    return ts


@pytest.mark.anyio
async def test_rescue_that_only_relocated_does_not_resume(tmp_path: Path):
    """Outcome 2: nothing secured, so the bytes are moved instead — no resume."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=1, unverified=1)
    _open_push_lane(cfg)
    asked: list[tuple[str, str]] = []

    async def _ask(ts, *, reason="manual"):
        asked.append(((getattr(ts, "source_infohash", "") or "").lower(), reason))
        return True, "pushing 1 file(s) from VPS1 to seedremote:unsorted"

    squat_name = "Example.Show.S01E30.1080p.WEB-DL-GROUP.mkv"
    coord.start_source_push = AsyncMock(side_effect=_ask)
    _squatting(store, src, "e" * 40, name=squat_name, size=4000)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        made, freed, reason = await coord.rescue_vps1_room(1500, why="test")
        assert (made, freed) == (False, 0)
        assert reason.startswith("relocating, will retry")
        assert "1 unsecured VPS1 release(s)" in reason
        # The lane is asked with its own reason word, so its checks still apply.
        assert asked == [("e" * 40, "reclaim")]
        done = await coord.rescue_starved_release(
            starved, 1500, why="new arrival does not fit VPS1", entries=[]
        )
        assert done is False
        assert src.resumed == [], "the room is on its way, not here: nothing resumes"
        assert src.deleted == [], "an unsecured release was not deleted for being in the way"
        assert store.get("e" * 40, include_blob=False).state == State.NEW
    finally:
        store.close()


@pytest.mark.anyio
async def test_rescue_with_the_lane_open_still_resumes_when_it_made_room(tmp_path: Path):
    """Outcome 1 is unchanged: room made by deletion resumes, hatch not consulted."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=4)
    _open_push_lane(cfg)
    coord.start_source_push = AsyncMock(return_value=(True, "pushing"))
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        done = await coord.rescue_starved_release(
            starved, 1500, why="new arrival does not fit VPS1", entries=[]
        )
        assert done is True
        assert src.resumed == ["a" * 40]
        assert len(src.deleted) == 2
        assert coord.start_source_push.await_count == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_cap_stopped_rescue_reports_its_shortfall_not_a_relocation(tmp_path: Path):
    """Outcome 3 stands while the sweep's own deletes are still owed."""
    coord, store, src, cfg, _hashes = _harness(
        tmp_path, count=8, per_run_cap=1, burst_per_run_cap=1
    )
    _open_push_lane(cfg)
    coord.start_source_push = AsyncMock(return_value=(True, "pushing"))
    _squatting(store, src, "e" * 40, name="Example.Show.S01E31.1080p.WEB-DL-GROUP.mkv", size=4000)
    try:
        made, freed, reason = await coord.rescue_vps1_room(8000, why="test")
        assert made is False
        assert freed == ROW_BYTES
        assert not reason.startswith("relocating")
        assert "short" in reason and "cap" in reason
        assert coord.start_source_push.await_count == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_relocating_rescue_is_reported_as_progress_not_as_a_failure(tmp_path, caplog):
    """No WARNING for a relocation in flight: the release is being handled."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=1, unverified=1)
    _open_push_lane(cfg)
    coord.start_source_push = AsyncMock(return_value=(True, "pushing"))
    _squatting(store, src, "e" * 40, name="Example.Show.S01E32.1080p.WEB-DL-GROUP.mkv", size=4000)
    starved = _new_row(store, "a" * 40, name=STARVED_NAME, size=1500)
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            done = await coord.rescue_starved_release(
                starved, 1500, why="new arrival does not fit VPS1", entries=[]
            )
        assert done is False
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("no room made" in m for m in warnings)
        assert all(STARVED_NAME not in m for m in warnings)
    finally:
        store.close()
