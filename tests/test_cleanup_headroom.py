"""VPS1 cleanup headroom: reclaim to a free-space TARGET, never on a timer.

The operator rule this pins down: keep a race seeding on VPS1 until the space
is actually needed for a newly arrived torrent; once the content is secured
(fuse entries present and the bytes verified on the mount) it can go without
worry. Elapsed time therefore left the policy: `[cleanup].low_watermark_free_bytes`
is the TRIGGER and `high_watermark_free_bytes` the TARGET, `per_run_cap` stays
the safety valve, and every safety gate (verification, protection, unknown
stats, `.torrent` preservation, dry-run purity, the state+version re-read)
behaves exactly as before. A disk that cannot be measured is never called
roomy — and never called tight either: an automatic pass skips it, and only a
pass the operator asked for runs there, proof-gated, cap-bounded and inventing
no free-space number.

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
from racing_sync.config import AppConfig, CleanupConfig
from racing_sync.config_alias import normalize_config
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024**3
# A band small enough for byte-sized test rows to cross it: trigger 1000 B,
# target 2500 B, so three 1000-byte groups restore the headroom.
TRIGGER = 1000
TARGET = 2500
FREE = 0  # "deep below the trigger": space is unmistakably needed
ROW_BYTES = 1000
FNAME = "Harbor.Lights.S03E05.mkv"


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
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_u"
    cfg.cleanup = _cleanup_cfg(**cleanup_over)
    return cfg


def _blob(name: str = FNAME, size: int = ROW_BYTES) -> bytes:
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


def _mounts(tmp_path: Path, names: list[str], *, with_file: bool = True) -> None:
    """Fuse stand-ins the byte check can stat; `with_file` is the proof lever."""
    for folder in ("fuse", "fuse_u"):
        d = tmp_path / folder
        d.mkdir(exist_ok=True)
        if with_file:
            for name in names:
                (d / name).write_bytes(b"\x00" * ROW_BYTES)


def _member(h: str, name: str = FNAME, size: int = ROW_BYTES) -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=size,
        state="seeding",
        progress=1.0,
        ratio=2.0,
        seeding_time_seconds=3600.0,
        total_uploaded_bytes=1024,
        trackers=[],
        upspeed_bps=0,
        num_leechers=0,
        added_on=0,
    )


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


def _hash(i: int) -> str:
    return f"{i:040x}"


def _row(
    store: StateStore,
    h: str,
    *,
    name: str = FNAME,
    completed_sec_ago: float = 60.0,
    verified: bool = False,
) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        dest_infohash=h,
        total_bytes=ROW_BYTES,
        injected_private_hashes="",
        state=State.DONE,
        # 60 seconds after DONE: deep below the 2 h floor this replaced.
        completed_at=now - dt.timedelta(seconds=completed_sec_ago),
        vps1_last_activity_at=now - dt.timedelta(seconds=completed_sec_ago),
        fuse_verified=1 if verified else 0,
    )
    store.upsert(ts)
    return ts


def _harness(
    tmp_path: Path,
    count: int = 3,
    *,
    verified: bool = True,
    free: int | None = FREE,
    completed_sec_ago: float = 60.0,
    **cleanup_over,
):
    """`count` DONE rows (distinct names) + matching VPS1 members and mount bytes."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, **cleanup_over)
    names = [f"Harbor.Lights.S03E{10 + i}.mkv" for i in range(count)]
    hashes = [_hash(i + 1) for i in range(count)]
    _mounts(tmp_path, names, with_file=verified)
    members: list[Torrent] = []
    for h, name in zip(hashes, names, strict=True):
        _row(store, h, name=name, completed_sec_ago=completed_sec_ago)
        assert store.set_blob(h, _blob(name)) is True
        members.append(_member(h, name))
    src = _Source(members)
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = _Dest(hashes)
    coord.sftp = None
    coord._stop = False
    coord._source_free_bytes = AsyncMock(return_value=free)
    return coord, store, src, cfg, hashes


async def _run(coord, cfg, *, free: int | None = FREE, **over):
    return await coord._run_source_cleanup(
        cfg.cleanup,
        free_bytes=free,
        arrivals=0.0,
        bytes_rate=0.0,
        projected=None,
        bursting=False,
        urgent=False,
        **over,
    )


# ------------------------------------------------------------- eligibility


@pytest.mark.anyio
async def test_secured_row_is_eligible_sixty_seconds_after_done(tmp_path: Path):
    """The old 2 h floor is gone: verification, not age, opens the gate."""
    coord, store, src, cfg, hashes = _harness(tmp_path, count=1)
    try:
        summary = await _run(coord, cfg)
        assert [d[0] for d in src.deleted] == [hashes[0]]
        assert summary["deleted"] == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_roomy_disk_deletes_nothing_however_old(tmp_path: Path):
    """The idle-days guarantee: at/above the trigger the janitor never deletes.

    Half the rows are secured 60 s ago, half are 400 days past the 72 h ceiling
    the old timer used to expire at: under the band they are the same row, and
    both keep seeding.
    """
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=3)
    try:
        summary = await _run(coord, cfg, free=TARGET * 10)
        assert src.deleted == []
        assert summary["deleted"] == 0
        assert "no space needed" in summary["reason"]
        # And the scan never even happens: no candidate list, no deletes.
        assert summary["candidates"] == 0

        # Aged far past every bound the policy used to carry: same answer.
        store2 = StateStore(tmp_path / "aged.db")
        try:
            aged = _hash(9)
            name = "Harbor.Lights.S03E99.mkv"
            _mounts(tmp_path, [name])
            _row(store2, aged, name=name, completed_sec_ago=400 * 24 * 3600, verified=True)
            assert store2.set_blob(aged, _blob(name)) is True
            src2 = _Source([_member(aged, name)])
            coord.store = store2
            coord.source_client = src2
            coord.dest_client = _Dest([aged])
            summary = await _run(coord, cfg, free=TARGET * 10)
            assert src2.deleted == []
            assert summary["deleted"] == 0
            assert "waiting for space 1" in summary["reason"]
        finally:
            store2.close()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_free_space_skips_the_automatic_pass(tmp_path: Path, caplog):
    """Blind disk, no operator: the automatic pass deletes NOTHING.

    The rule has two halves and this is one of them. An unmeasurable VPS1 (no
    `get_free_space`, dead statvfs probe) is never read as "roomy, nothing to
    do" — that would be a headroom claim nobody measured — and it is just as
    little read as "space needed": reclaiming on that claim would spend real
    seed time to satisfy a trigger the pass cannot evaluate. So no reading means
    no automatic deletes, and the report says `unknown` wherever a byte figure
    would be a guess. An operator who wants the blind pass asks for it
    (`/cleanup` → force), see the forced tests below.
    """
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=3)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _run(coord, cfg, free=None)
        # Not read as needing space: nothing was reclaimed, on three rows that
        # are fully secured and would otherwise go right now.
        assert src.deleted == []
        assert summary["deleted"] == 0
        assert summary["candidates"] == 0
        # Not read as roomy either: the skip names the missing reading, never a
        # headroom claim.
        assert "VPS1 free space unknown" in summary["reason"]
        assert "not reclaiming automatically" in summary["reason"]
        assert "no space needed" not in summary["reason"]
        assert "VPS1 free space unknown" in caplog.text
        # Nothing is guessed: no reading, so no target and no projection.
        assert summary["free"] is None
        assert summary["free_after"] is None
        assert summary["target"] is None
        assert summary["shortfall_bytes"] == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_forced_pass_reclaims_on_proof_and_invents_no_number(tmp_path: Path, caplog):
    """`force` (operator `/cleanup`) DOES run on a blind disk: proof-gated, capped.

    The other half of the rule. Asked explicitly, the pass does not need a
    measurement to know the operator wants a sweep, so it reclaims the rows
    whose content verifies — bounded by the per-run cap — while inventing
    nothing: no target, no projected free space, and the report says the free
    space is unknown.
    """
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=3)
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            summary = await _run(coord, cfg, free=None, force=True)
        # The scan ran and reclaimed on proof alone, so the cap owns the sweep
        # instead of a target that cannot be evaluated.
        assert summary["candidates"] == 3
        assert summary["deleted"] == 3
        assert len(src.deleted) == 3
        # Nothing is guessed: no reading, so no target and no projection.
        assert summary["free"] is None
        assert summary["free_after"] is None
        assert summary["target"] is None
        assert summary["shortfall_bytes"] == 0
        # And the run is never reported as a roomy skip.
        assert "no space needed" not in summary["reason"]
        assert "VPS1 free space unknown" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_forced_blind_pass_is_still_bounded_by_the_cap(tmp_path: Path):
    """A blind pass is opt-in, never a blank cheque: `per_run_cap` still stops it."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=3, per_run_cap=2)
    try:
        summary = await _run(coord, cfg, free=None, force=True)
        assert summary["deleted"] == 2
        assert len(src.deleted) == 2
        assert summary["cap"] == 2
        # The rows it did not reach stay put, and are reported as still eligible
        # instead of being papered over with an invented target.
        assert summary["eligible_remaining"] == 1
        assert summary["target"] is None
        assert summary["free"] is None
        assert summary["free_after"] is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_free_space_never_rescues_an_unproven_row(tmp_path: Path):
    """Blind disk, same gates: content verification cannot be guessed either."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=3, verified=False)
    try:
        # Forced, so the pass really scans: on a blind automatic pass nothing
        # would run at all and the gate would never be asked.
        summary = await _run(coord, cfg, free=None, force=True)
        assert src.deleted == []
        assert summary["deleted"] == 0
        assert "verification 3" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_not_verifiable_row_is_never_rescued_by_headroom(tmp_path: Path):
    """A starving disk does not lower a safety gate: unproven bytes stay."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=2, verified=False)
    try:
        summary = await _run(coord, cfg)
        assert src.deleted == []
        assert "verification 2" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_protected_rows_are_never_deleted(tmp_path: Path):
    coord, store, src, cfg, _hashes = _harness(
        tmp_path, count=2, protected_patterns=["harbor.lights"]
    )
    try:
        summary = await _run(coord, cfg)
        assert src.deleted == []
        assert "protection 2" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_dry_run_deletes_nothing_and_mutates_nothing(tmp_path: Path):
    coord, store, src, cfg, hashes = _harness(tmp_path, count=2, dry_run=True)
    try:
        before = store.get(hashes[0], include_blob=False)
        summary = await _run(coord, cfg)
        assert src.deleted == []
        assert summary["dry_run"] is True
        assert summary["deleted"] == 2  # the rehearsal count, nothing more
        after = store.get(hashes[0], include_blob=False)
        assert after.state == before.state == State.DONE
        assert after.version == before.version
        assert after.fuse_verified == 0  # no memo written by a rehearsal
        assert store.get_blob(hashes[0]) == _blob("Harbor.Lights.S03E10.mkv")
    finally:
        store.close()


# ------------------------------------------------------- reclaim toward target


@pytest.mark.anyio
async def test_pass_stops_at_the_target_not_at_the_first_candidate(tmp_path: Path):
    """Five eligible rows, room for three: the band, not the count, decides."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=5, per_run_cap=10)
    try:
        summary = await _run(coord, cfg)
        # 0 -> 1000 -> 2000 -> 3000 >= target 2500: three groups, then stop.
        assert summary["deleted"] == 3
        assert len(src.deleted) == 3
        assert summary["target"] == TARGET
        assert summary["free_after"] >= TARGET
        assert summary["target_reached"] is True
        assert summary["shortfall_bytes"] == 0
        assert summary["eligible_remaining"] == 2
    finally:
        store.close()


@pytest.mark.anyio
async def test_biggest_first_reaches_the_target_soonest(tmp_path: Path):
    """The pressure order is the headroom order: the largest secured row goes."""
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, per_run_cap=10)
    small, big = _hash(1), _hash(2)
    _mounts(tmp_path, ["Small.Ep.mkv", "Big.Pack.mkv"])
    for h, name, size in ((small, "Small.Ep.mkv", 400), (big, "Big.Pack.mkv", 3000)):
        ts = _row(store, h, name=name)
        ts.total_bytes = size
        store.upsert(ts)
        assert store.set_blob(h, _blob(name, size)) is True
        (tmp_path / "fuse" / name).write_bytes(b"\x00" * size)
        (tmp_path / "fuse_u" / name).write_bytes(b"\x00" * size)
    src = _Source([_member(small, "Small.Ep.mkv", 400), _member(big, "Big.Pack.mkv", 3000)])
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = _Dest([small, big])
    coord.sftp = None
    coord._stop = False
    coord._source_free_bytes = AsyncMock(return_value=FREE)
    try:
        summary = await _run(coord, cfg)
        assert [d[0] for d in src.deleted] == [big]  # one pass restores the band
        assert summary["deleted"] == 1
        assert summary["target_reached"] is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_cap_stops_short_and_reports_the_shortfall(tmp_path: Path, caplog):
    """The safety valve wins over the target: the gap is stated, not hidden."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=5, per_run_cap=2)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _run(coord, cfg)
        assert summary["deleted"] == 2
        assert summary["cap"] == 2
        assert summary["target_reached"] is False
        assert summary["shortfall_bytes"] == TARGET - 2 * ROW_BYTES
        assert summary["eligible_remaining"] == 3
        line = next(
            r.getMessage() for r in caplog.records if "stopped the pass short" in r.getMessage()
        )
        # Every number the operator needs, in one line.
        for token in ("cap 2", "-> ", f"target {TARGET} B", "3 eligible row(s)"):
            assert token in line
        assert "per-run cap" in line
        text = coord._cleanup_summary_text(summary)
        assert "short of the" in text and "cap 2" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_eligible_inventory_running_out_is_not_called_a_cap_stop(tmp_path: Path, caplog):
    """Draining everything eligible below the target still says what is left."""
    coord, store, src, cfg, _hashes = _harness(tmp_path, count=1, per_run_cap=10)
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _run(coord, cfg)
        assert summary["deleted"] == 1
        assert summary["eligible_remaining"] == 0
        assert summary["target_reached"] is False
        assert summary["shortfall_bytes"] == TARGET - ROW_BYTES
        assert "stopped the pass short" not in caplog.text
    finally:
        store.close()


# ------------------------------------------------------------- band plumbing


def test_band_and_reclaim_are_read_from_the_two_watermarks():
    coord = make_coordinator()
    cfg = _cleanup_cfg()
    assert coord._cleanup_band(cfg) == (TRIGGER, TARGET)
    # Below the trigger: needed. At/above it: not needed, whatever the age.
    assert coord._cleanup_reclaim_needed(cfg, TRIGGER - 1, False, None) is True
    assert coord._cleanup_reclaim_needed(cfg, TRIGGER, False, None) is False
    assert coord._cleanup_reclaim_needed(cfg, TARGET * 10, False, None) is False
    # A release that cannot fit is "space needed", even above the trigger.
    assert coord._cleanup_reclaim_needed(cfg, TARGET * 10, True, None) is True
    assert coord._cleanup_reclaim_needed(cfg, TARGET * 10, False, "critical") is True
    # Unknown free space is not an answer: the predicate can say neither
    # "needed" nor "roomy", so the automatic pass deletes nothing
    # (test_unknown_free_space_skips_the_automatic_pass) and only an
    # operator-forced pass reclaims on proof alone
    # (test_forced_pass_reclaims_on_proof_and_invents_no_number).
    assert coord._cleanup_reclaim_needed(cfg, None, True, "critical") is False
    # A disabled trigger reclaims nothing rather than everything.
    assert (
        coord._cleanup_reclaim_needed(_cleanup_cfg(low_watermark_free_bytes=0), 1, False, None)
        is False
    )


def test_cleanup_band_reads_an_unreadable_section_as_no_band():
    coord = make_coordinator()
    assert coord._cleanup_band(object()) == (0, 0)
    assert coord._cleanup_reclaim_needed(object(), 10, False, None) is False


# ---------------------------------------------------------- deprecated config


@pytest.mark.parametrize(
    "key",
    ["min_grace_hours", "max_grace_hours", "idle_confirm_minutes"],
)
def test_removed_cleanup_timers_refuse_to_load(key: str):
    """FAIL-LOUD: the dead knobs name their replacement instead of vanishing."""
    for spelling in (
        # internal table layout
        {"source": {"type": "deluge"}, "cleanup": {key: 2.0}},
        # capability-chain layout: `[cleanup]` keeps its name in both
        {"client": {"type": "deluge"}, "cleanup": {key: 2.0}},
    ):
        with pytest.raises(ValueError) as exc:
            normalize_config(spelling)
        message = str(exc.value)
        assert f"`cleanup.{key}` is no longer supported" in message
        assert "free-space target, not a timer" in message


def test_removed_timers_refuse_through_a_real_file_load(tmp_path: Path):
    base = (Path(__file__).parent / "fixtures" / "config" / "minimal.toml").read_text(
        encoding="utf-8"
    )
    dead = tmp_path / "config.toml"
    dead.write_text(f"{base}\n[cleanup]\nmin_grace_hours = 2\n", encoding="utf-8")
    with pytest.raises(Exception) as exc:
        AppConfig.from_toml(dead)
    assert "free-space target, not a timer" in str(exc.value)


def test_cleanup_config_ignores_nothing_silently():
    """The live model has no field for the removed timers (so none can drift)."""
    assert not {"min_grace_hours", "max_grace_hours", "idle_confirm_minutes"} & set(
        CleanupConfig.model_fields
    )


# --------------------------------------------------------------- the 🗄 hint


def _hint_coord(free: int | None, *, verified: bool) -> tuple[object, TorrentState]:
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.cleanup = _cleanup_cfg()
    coord._last_source_free_bytes = free
    now = dt.datetime.now(dt.UTC)
    row = TorrentState(
        source_infohash="a" * 40,
        source_name=FNAME,
        dest_infohash="a" * 40,
        total_bytes=ROW_BYTES,
        state=State.DONE,
        completed_at=now - dt.timedelta(seconds=60),
        fuse_verified=1 if verified else 0,
    )
    return coord, row


def test_hint_reports_headroom_state_and_never_a_countdown():
    space_needed, row = _hint_coord(TRIGGER - 1, verified=True)
    assert space_needed._cleanup_hint(row) == "secured · reclaiming"

    roomy, row2 = _hint_coord(TARGET * 10, verified=True)
    assert roomy._cleanup_hint(row2) == "secured · waiting for space"

    # The row the janitor will not touch is never shown as "clean ready".
    unverifiable, row3 = _hint_coord(TRIGGER - 1, verified=False)
    assert unverifiable._cleanup_hint(row3) == "not verifiable yet"

    for hint in (
        space_needed._cleanup_hint(row),
        roomy._cleanup_hint(row2),
        unverifiable._cleanup_hint(row3),
    ):
        assert "~" not in hint
        assert "clean ready" not in hint
        assert not any(ch.isdigit() for ch in hint)


def test_hint_drops_the_countdown_helpers_and_ignores_age():
    """No remaining-seconds maths is left to compute a countdown from."""
    from racing_sync.coordinator_cleanup import CleanupMixin

    assert not hasattr(CleanupMixin, "_cleanup_remaining_seconds")
    assert not hasattr(CleanupMixin, "_cleanup_eta_compact")
    coord_a, row = _hint_coord(TARGET * 10, verified=True)
    now = dt.datetime.now(dt.UTC)
    row.completed_at = now - dt.timedelta(seconds=60)
    coord_b, row_b = _hint_coord(TARGET * 10, verified=True)
    row_b.completed_at = now - dt.timedelta(days=400)
    # Same status at 1 minute and at 400 days: the hint cannot count down.
    assert coord_a._cleanup_hint(row) == coord_b._cleanup_hint(row_b)
