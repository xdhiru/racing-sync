"""`[cleanup].ignore_activity_under_pressure`: disk pressure vs. the activity keep.

A near-full VPS1 held 17 rows for 13.5 h on `still active on VPS1 (1/1
racing)` while intake kept landing. The opt-in override lifts that keep and
nothing else, so these tests pin both halves: the row deletes when the run
reads real pressure, and it is STILL kept when verification, unknown stats,
protection or a quiet disk says so. Release names here are invented.
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
from racing_sync.coordinator_cleanup import CleanupMixin
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024**3
FNAME = "Sample.Show.S01E01.mkv"
FSIZE = 700
# Below the 15 GiB low watermark: the janitor's own pressure signal.
TIGHT = 5 * GiB
# Between the low (15 GiB) and high (40 GiB) watermarks: roomy, but the
# calm-and-roomy short-circuit does not fire, so the scan really runs.
ROOMY = 20 * GiB


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(
        enabled=True,
        dry_run=False,
        janitor_startup_delay_seconds=0.0,
        delete_stagger_seconds=0.0,
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


def _mounts(tmp_path: Path, *, with_file: bool = True) -> None:
    """Fuse dirs the byte check can stat; `with_file` is the verification lever."""
    for name in ("fuse", "fuse_u"):
        d = tmp_path / name
        d.mkdir(exist_ok=True)
        if with_file:
            (d / FNAME).write_bytes(b"\x00" * FSIZE)


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
    completed_h_ago: float | None = 100.0,
    activity_h_ago: float | None = 10.0,
    name: str = FNAME,
) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=h,
        source_name=name,
        dest_infohash=h,
        total_bytes=FSIZE,
        injected_private_hashes="",
        state=state,
        completed_at=(now - dt.timedelta(hours=completed_h_ago))
        if completed_h_ago is not None
        else None,
        vps1_last_activity_at=(
            now - dt.timedelta(hours=activity_h_ago) if activity_h_ago is not None else None
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


async def _scan(coord, cfg, *, free: int | None, bursting: bool) -> dict:
    """One janitor run with the pressure signal the tick already computed."""
    return await coord._run_source_cleanup(
        cfg.cleanup,
        free_bytes=free,
        arrivals=0.0,
        bytes_rate=0.0,
        projected=None,
        bursting=bursting,
        urgent=False,
    )


def _leeching_setup(tmp_path: Path, **cleanup_over):
    """DONE row, fuse-proven on VPS2, past grace, one leecher pulling from VPS1."""
    h = "a" * 40
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, **cleanup_over)
    _mounts(tmp_path)
    _row(store, h)
    # Blob storage is row-scoped: the row has to exist before its bytes land.
    assert store.set_blob(h, _blob()) is True
    src = _Source([_member(h, leechers=2, upspeed=200_000)])
    dest = _Dest([h])
    return store, cfg, src, dest, h


# ------------------------------------------------------------------ janitor


@pytest.mark.anyio
async def test_override_off_keeps_the_leeching_row(tmp_path: Path):
    """Today's behaviour, unchanged: a leecher on VPS1 is an absolute block."""
    store, cfg, src, dest, h = _leeching_setup(tmp_path)
    try:
        coord = _coord(store, src, dest, cfg)
        summary = await _scan(coord, cfg, free=TIGHT, bursting=True)
        assert src.deleted == []
        assert summary["deleted"] == 0
        assert "activity" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_override_on_under_pressure_deletes_the_leeching_row(tmp_path: Path, caplog):
    store, cfg, src, dest, h = _leeching_setup(tmp_path, ignore_activity_under_pressure=True)
    try:
        coord = _coord(store, src, dest, cfg)
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=TIGHT, bursting=True)
        assert [d[0] for d in src.deleted] == [h]
        assert summary["deleted"] == 1
        assert "pressure: deleting" in caplog.text
        assert "despite 2 leecher(s)/upspeed" in caplog.text
        assert f"VPS1 free={TIGHT} B" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_override_on_without_pressure_still_keeps(tmp_path: Path):
    """The switch alone is not permission: a roomy disk keeps the old rule."""
    store, cfg, src, dest, h = _leeching_setup(tmp_path, ignore_activity_under_pressure=True)
    try:
        coord = _coord(store, src, dest, cfg)
        summary = await _scan(coord, cfg, free=ROOMY, bursting=False)
        assert src.deleted == []
        assert "activity 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_override_on_but_unverified_fuse_bytes_still_keeps(tmp_path: Path, caplog):
    """The safety line: pressure never deletes content VPS2 cannot be proven to serve."""
    h = "a" * 40
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path, ignore_activity_under_pressure=True)
    _mounts(tmp_path, with_file=False)  # entries exist, bytes do not
    src = _Source([_member(h, leechers=2)])
    coord = _coord(store, src, _Dest([h]), cfg)
    _row(store, h)
    assert store.set_blob(h, _blob()) is True
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            summary = await _scan(coord, cfg, free=TIGHT, bursting=True)
        assert src.deleted == []
        assert "verification 1" in summary["reason"]
        # The rescue line is only earned by a row that really becomes a
        # candidate: a verification keep must not leave a "deleting" claim.
        assert "pressure: deleting" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_override_on_but_unknown_stats_still_keeps(tmp_path: Path):
    """An unreported swarm is unproven, not quiet — pressure does not read it as activity."""
    store, cfg, src, dest, h = _leeching_setup(tmp_path, ignore_activity_under_pressure=True)
    src.members = [
        _member(
            h,
            upspeed=0,
            leechers=3,
            ratio=0.0,
            uploaded=0,
            seed_s=0.0,
        )
    ]
    try:
        coord = _coord(store, src, dest, cfg)
        summary = await _scan(coord, cfg, free=TIGHT, bursting=True)
        assert src.deleted == []
        assert "unknown stats 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_override_on_but_protected_row_still_keeps(tmp_path: Path):
    store, cfg, src, dest, h = _leeching_setup(
        tmp_path, ignore_activity_under_pressure=True, protected_patterns=["sample.show"]
    )
    try:
        coord = _coord(store, src, dest, cfg)
        summary = await _scan(coord, cfg, free=TIGHT, bursting=True)
        assert src.deleted == []
        assert "protection 1" in summary["reason"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_janitor_tick_passes_its_pressure_signal_down(tmp_path: Path):
    """End to end through `_maybe_cleanup_source`: the low watermark IS the pressure."""
    store, cfg, src, dest, h = _leeching_setup(tmp_path, ignore_activity_under_pressure=True)
    coord = _coord(store, src, dest, cfg)
    coord._source_free_bytes = AsyncMock(return_value=TIGHT)
    try:
        await coord._maybe_cleanup_source()
        assert [d[0] for d in src.deleted] == [h]
    finally:
        store.close()


# ------------------------------------------------------------- manual /clean


def _manual_setup(tmp_path: Path, **cleanup_over):
    """One DONE row for `/clean_<hash>`; fuse + blob checks stubbed like the command tests."""
    h = "b" * 40
    store = StateStore(tmp_path / "s.db")
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.cleanup = _cleanup_cfg(**cleanup_over)
    src = _Source([_member(h, leechers=1, upspeed=500_000)])
    coord = _coord(store, src, _Dest([h]), cfg)
    _row(store, h)
    coord._cleanup_fuse_healthy = AsyncMock(return_value=True)
    coord._cleanup_preserve_racing_blobs = AsyncMock(return_value=True)
    coord._delete_source_group = AsyncMock(return_value=True)
    return store, cfg, src, coord, h


@pytest.mark.anyio
async def test_manual_clean_keeps_an_active_row_without_the_switch(tmp_path: Path):
    store, _cfg, _src, coord, h = _manual_setup(tmp_path)
    coord._last_source_free_bytes = TIGHT
    try:
        text = await coord._clean_task_command(h)
        assert "still shows race activity" in text
        coord._delete_source_group.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_manual_clean_honours_the_override_under_pressure(tmp_path: Path, caplog):
    store, _cfg, _src, coord, h = _manual_setup(tmp_path, ignore_activity_under_pressure=True)
    coord._last_source_free_bytes = TIGHT
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            text = await coord._clean_task_command(h)
        assert text.startswith("Deleted 1 VPS1 torrent(s)")
        coord._delete_source_group.assert_awaited_once()
        assert "pressure: deleting" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_manual_clean_without_pressure_still_keeps(tmp_path: Path):
    store, _cfg, _src, coord, h = _manual_setup(tmp_path, ignore_activity_under_pressure=True)
    coord._last_source_free_bytes = ROOMY
    try:
        text = await coord._clean_task_command(h)
        assert "still shows race activity" in text
        coord._delete_source_group.assert_not_awaited()
    finally:
        store.close()


# ------------------------------------------------------- readable refusals


@pytest.mark.anyio
async def test_nothing_eligible_breakdown_separates_activity_from_grace(tmp_path: Path, caplog):
    """ "activity 1, grace 1": the two keeps are distinguishable without a report."""
    held, waiting = "a" * 40, "d" * 40
    other = "Sample.Show.S01E02.mkv"
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path)
    _mounts(tmp_path)
    for d in (tmp_path / "fuse", tmp_path / "fuse_u"):
        (d / other).write_bytes(b"\x00" * FSIZE)
    src = _Source([_member(held, leechers=2), _member(waiting, name=other)])
    coord = _coord(store, src, _Dest([held, waiting]), cfg)
    _row(store, held)
    # `held` finished long ago (grace served); `waiting` finished an hour ago
    # (grace floor is 2 h) and was seen active a minute ago, so neither the
    # age nor the idle fast lane lets it go.
    _row(store, waiting, name=other, completed_h_ago=1.0, activity_h_ago=1 / 60)
    assert store.set_blob(held, _blob()) is True
    assert store.set_blob(waiting, _blob(other)) is True
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            # 10 GiB: below the 15 GiB low watermark, ABOVE the 8 GiB critical
            # one. At `TIGHT` the same scan is an emergency, where a verified
            # row skips the grace wait (see tests/test_cleanup_emergency.py);
            # this test measures the two keeps, so it stays out of emergency.
            summary = await _scan(coord, cfg, free=10 * GiB, bursting=False)
        assert summary["deleted"] == 0
        assert summary["reason"].startswith("nothing eligible (")
        assert "activity 1" in summary["reason"]
        assert "grace 1" in summary["reason"]
        assert "emergency" not in summary["reason"]
        assert "cleanup janitor: nothing eligible" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_missing_fuse_file_warning_names_the_file(tmp_path: Path, caplog):
    """The keep that repeated 80 times must say WHICH file never arrived."""
    h = "a" * 40
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path)
    _mounts(tmp_path, with_file=False)
    coord = _coord(store, _Source([_member(h)]), _Dest([h]), cfg)
    _row(store, h)
    assert store.set_blob(h, _blob()) is True
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            await _scan(coord, cfg, free=ROOMY, bursting=False)
        text = caplog.text
        assert "fuse file(s) missing" in text
        assert FNAME in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_missing_fuse_entries_warning_lists_hashes(tmp_path: Path, caplog):
    h = "a" * 40
    injected = "c" * 40
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(tmp_path)
    _mounts(tmp_path)
    src = _Source([_member(h), _member(injected)])
    coord = _coord(store, src, _Dest([h]), cfg)
    row = _row(store, h)
    row.injected_private_hashes = injected
    store.upsert(row)
    assert store.set_blob(h, _blob()) is True
    # `injected` has no dest entry at all: the list has to name it.
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            await _scan(coord, cfg, free=ROOMY, bursting=False)
        assert "fuse entry missing" in caplog.text
        assert injected[:10] in caplog.text
    finally:
        store.close()


def test_blocked_breakdown_is_bounded_and_one_line():
    """A diagnosis aid, not a report: capped, single line, ordered."""
    coord = make_coordinator()
    blocked = {
        "activity": 17,
        "grace": 3,
        "verification": 1,
        "x" * 400: 9,
    }
    text = coord._cleanup_blocked_text(blocked)
    assert text.startswith("grace 3, activity 17, verification 1")
    assert "\n" not in text
    assert len(text) <= 160
    assert coord._cleanup_blocked_reason({}) == "nothing eligible"


def test_summary_text_keeps_its_contract():
    """`reason` stays one plain string; the `/cleanup` reply only grows a tail."""
    line = CleanupMixin._cleanup_summary_text(
        {
            "deleted": 0,
            "candidates": 0,
            "dry_run": False,
            "grace_s": 7200.0,
            "free": TIGHT,
            "bursting": True,
            "reason": "nothing eligible (activity 17, grace 3)",
        }
    )
    assert line == "🧹 Cleanup: nothing deleted — nothing eligible (activity 17, grace 3)."


# ----------------------------------------------------------------- defaults


def test_pressure_override_defaults_off():
    assert CleanupConfig().ignore_activity_under_pressure is False


def test_throughput_defaults_raised_the_cap_and_cut_the_stagger():
    """12 deletes at 60 s serialise past the 300 s burst cadence; 24 at 25 s do not."""
    cfg = CleanupConfig()
    assert cfg.delete_stagger_seconds == 25.0
    example = AppConfig.from_toml(Path(__file__).parent.parent / "config.example.toml")
    assert example.cleanup.delete_stagger_seconds == 25.0
    assert example.cleanup.burst_per_run_cap == 24
    assert example.cleanup.per_run_cap <= example.cleanup.burst_per_run_cap
    # The cross-check validator still accepts the shipped pairing and the
    # documented floor/ceiling spellings.
    CleanupConfig(per_run_cap=10, burst_per_run_cap=24, janitor_interval_seconds=600)
    with pytest.raises(ValueError, match="burst_per_run_cap"):
        CleanupConfig(per_run_cap=25, burst_per_run_cap=24)


def test_model_stagger_matches_the_janitor_fallback():
    """One truth: the code's own fallback (25) is now the model default too."""
    import inspect

    from racing_sync.coordinator_cleanup import CleanupMixin

    src = inspect.getsource(CleanupMixin._run_source_cleanup)
    assert '"delete_stagger_seconds", 25' in src
    assert CleanupConfig().delete_stagger_seconds == 25.0
