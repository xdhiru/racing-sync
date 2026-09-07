"""VPS1 cleanup janitor ([cleanup]) tests.

Covers grace math, eligibility gates (DONE normal path, MOVING early
pressure path), dry-run purity, atomic group deletes, and the hourly gate.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.config import CleanupConfig
from conftest import make_coordinator
from racing_sync.coordinator import Coordinator, cleanup_grace_seconds
from racing_sync.state import State, StateStore, TorrentState

GiB = 1024 ** 3


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(enabled=True, dry_run=False)
    base.update(over)
    return CleanupConfig(**base)


def _make_coord(ssd: Path, store: StateStore, src, dest, cfg) -> Coordinator:
    coord = make_coordinator()
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = dest
    coord.sftp = None
    coord._stop = False
    return coord


def _base_cfg(ssd: Path) -> MagicMock:
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.dest.save_path = ssd
    cfg.cleanup = _cleanup_cfg()
    return cfg


FNAME = "Show.S01E01.mkv"
FSIZE = 700


def _member(h: str, upspeed: int = 0, leechers: int = 0, ratio: float = 2.0,
            added_on: int = 0) -> Torrent:
    return Torrent(
        hash=h, name=FNAME, category="", save_path="/vps1/data",
        size_bytes=FSIZE, state="seeding", progress=1.0, ratio=ratio,
        trackers=[], upspeed_bps=upspeed, num_leechers=leechers,
        added_on=added_on,
    )


def _row(store: StateStore, h: str, state: State, completed_h_ago: float | None,
         activity_h_ago: float | None, injected: str, save_path: str = "") -> TorrentState:
    now = dt.datetime.now(dt.timezone.utc)
    ts = TorrentState(
        source_infohash=h, source_name=FNAME, dest_infohash=h,
        total_bytes=FSIZE, save_path=save_path,
        injected_private_hashes=injected, state=state,
        completed_at=(now - dt.timedelta(hours=completed_h_ago)) if completed_h_ago is not None else None,
        vps1_last_activity_at=(
            (now - dt.timedelta(hours=activity_h_ago)) if activity_h_ago is not None else None
        ),
    )
    store.upsert(ts)
    return ts


class _FakeSource:
    def __init__(self, members: list[Torrent]):
        self.members = members
        self.deleted: list[tuple[str, bool]] = []

    async def list_torrents(self, *, category=None, hashes=None):
        return list(self.members)

    async def delete(self, h: str, *, delete_files: bool = False):
        self.deleted.append((h.lower(), delete_files))
        self.members = [m for m in self.members if m.hash.lower() != h.lower()]


class _FakeDest:
    def __init__(self, hashes: list[str], files: list[TorrentFile] | None = None):
        self.hashes = {h.lower() for h in hashes}
        self.files = files or []

    async def list_torrents(self, *, category=None, hashes=None):
        want = {h.lower() for h in hashes} if hashes is not None else self.hashes
        return [
            Torrent(hash=h, name="x", category="racing", save_path="/mnt/fuse",
                    size_bytes=1, state="seeding", progress=1.0)
            for h in (want & self.hashes)
        ]

    async def get_torrent_files(self, h: str):
        return list(self.files)


# ---- grace math ----

def test_grace_calm_and_roomy_is_max():
    cfg = _cleanup_cfg()
    assert cleanup_grace_seconds(cfg, 100 * GiB, 0.0) == 72 * 3600


def test_grace_critical_and_burst_is_min():
    cfg = _cleanup_cfg()
    assert cleanup_grace_seconds(cfg, 1 * GiB, 50.0) == 2 * 3600


def test_grace_interpolates_and_unknown_free_uses_velocity():
    cfg = _cleanup_cfg()
    mid_free = (15 + 40) / 2 * GiB
    mid = cleanup_grace_seconds(cfg, mid_free, 0.0)
    assert 2 * 3600 < mid < 72 * 3600
    # Unknown disk never degrades below the velocity curve.
    assert cleanup_grace_seconds(cfg, None, 0.0) == 72 * 3600
    assert cleanup_grace_seconds(cfg, None, 50.0) == 2 * 3600


def test_cleanup_config_rejects_bad_bounds():
    import pydantic
    with pytest.raises(pydantic.ValidationError):
        CleanupConfig(min_grace_hours=10, max_grace_hours=2)
    with pytest.raises(pydantic.ValidationError):
        CleanupConfig(low_watermark_free_bytes=99, high_watermark_free_bytes=1)
    with pytest.raises(pydantic.ValidationError):
        CleanupConfig(burst_arrivals_per_hour=1, calm_arrivals_per_hour=2)


# ---- janitor gating ----

@pytest.mark.anyio
async def test_janitor_disabled_is_noop(tmp_path: Path):
    store = StateStore(tmp_path / "s.db")
    src = _FakeSource([])
    dest = _FakeDest([])
    cfg = _base_cfg(tmp_path)
    cfg.cleanup = _cleanup_cfg(enabled=False)
    coord = _make_coord(tmp_path, store, src, dest, cfg)
    src.list_torrents = AsyncMock(wraps=src.list_torrents)
    await coord._maybe_cleanup_source()
    src.list_torrents.assert_not_called()


@pytest.mark.anyio
async def test_janitor_respects_interval(tmp_path: Path):
    store = StateStore(tmp_path / "s.db")
    src = _FakeSource([])
    dest = _FakeDest([])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))
    import time
    coord._cleanup_last_run = time.monotonic()
    src.list_torrents = AsyncMock(wraps=src.list_torrents)
    await coord._maybe_cleanup_source()
    src.list_torrents.assert_not_called()


# ---- DONE normal path ----

@pytest.mark.anyio
async def test_done_idle_and_old_gets_deleted(tmp_path: Path):
    pub, p1, p2 = "a" * 40, "b" * 40, "c" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=100.0, activity_h_ago=10.0,
         injected=f"{p1},{p2}")
    src = _FakeSource([_member(pub), _member(p1), _member(p2)])
    dest = _FakeDest([pub, p1, p2])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert len(src.deleted) == 3
    first, rest = src.deleted[0], src.deleted[1:]
    assert first[1] is True  # files deleted exactly once
    assert all(r[1] is False for r in rest)
    assert {h for h, _ in src.deleted} == {pub, p1, p2}


@pytest.mark.anyio
async def test_done_recent_and_quiet_is_kept(tmp_path: Path):
    pub = "a" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=1.0, activity_h_ago=None,
         injected="")
    src = _FakeSource([_member(pub)])
    dest = _FakeDest([pub])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert src.deleted == []
    # No activity stamp invented by a read-only evaluation.
    assert store.get(pub).vps1_last_activity_at is None


@pytest.mark.anyio
async def test_done_active_swarm_bumps_activity_and_keeps(tmp_path: Path):
    pub = "a" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=100.0, activity_h_ago=50.0,
         injected="")
    src = _FakeSource([_member(pub, upspeed=500_000, leechers=3)])
    dest = _FakeDest([pub])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert src.deleted == []
    assert store.get(pub).vps1_last_activity_at is not None


@pytest.mark.anyio
async def test_done_missing_fuse_entry_is_kept(tmp_path: Path):
    pub, gone = "a" * 40, "b" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=100.0, activity_h_ago=10.0,
         injected=gone)
    src = _FakeSource([_member(pub)])
    dest = _FakeDest([pub])  # `gone` vanished from dest
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert src.deleted == []


@pytest.mark.anyio
async def test_done_protected_pattern_is_kept(tmp_path: Path):
    pub = "a" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=100.0, activity_h_ago=10.0,
         injected="")
    src = _FakeSource([_member(pub)])
    dest = _FakeDest([pub])
    cfg = _base_cfg(tmp_path)
    cfg.cleanup = _cleanup_cfg(protected_patterns=["show.s01"])
    coord = _make_coord(tmp_path, store, src, dest, cfg)

    await coord._maybe_cleanup_source()

    assert src.deleted == []


@pytest.mark.anyio
async def test_dry_run_deletes_nothing(tmp_path: Path):
    pub = "a" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=100.0, activity_h_ago=10.0,
         injected="")
    src = _FakeSource([_member(pub)])
    dest = _FakeDest([pub])
    cfg = _base_cfg(tmp_path)
    cfg.cleanup = _cleanup_cfg(dry_run=True)
    coord = _make_coord(tmp_path, store, src, dest, cfg)

    await coord._maybe_cleanup_source()

    assert src.deleted == []
    assert store.get(pub).state == State.DONE


# ---- MOVING early pressure path ----

@pytest.mark.anyio
async def test_done_old_quiet_no_stamp_uses_added_on_fallback(tmp_path: Path):
    """Pre-existing idle backlog clears on the first run, no grace wait.

    No activity stamp was ever recorded, but every member was added days
    ago and the swarm is quiet now: the race is verifiably over.
    """
    import time
    pub = "a" * 40
    added = int(time.time()) - 5 * 24 * 3600
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=None, activity_h_ago=None,
         injected="")
    src = _FakeSource([_member(pub, added_on=added)])
    dest = _FakeDest([pub])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert [h for h, _ in src.deleted] == [pub]


@pytest.mark.anyio
async def test_done_fresh_torrent_no_stamp_waits_grace(tmp_path: Path):
    """A torrent added minutes ago with no stamp can't prove idleness yet."""
    import time
    pub = "a" * 40
    added = int(time.time()) - 120
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.DONE, completed_h_ago=None, activity_h_ago=None,
         injected="")
    src = _FakeSource([_member(pub, added_on=added)])
    dest = _FakeDest([pub])
    coord = _make_coord(tmp_path, store, src, dest, _base_cfg(tmp_path))

    await coord._maybe_cleanup_source()

    assert src.deleted == []


@pytest.mark.anyio
async def test_moving_early_deletes_only_under_pressure(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    (ssd / FNAME).write_bytes(b"D" * FSIZE)
    files = [TorrentFile(name=FNAME, size_bytes=FSIZE, progress=1.0)]

    async def run_case(free_bytes: int | None) -> list:
        pub = "a" * 40
        store = StateStore(tmp_path / f"{free_bytes}.db")
        _row(store, pub, State.MOVING, completed_h_ago=None, activity_h_ago=10.0,
             injected="", save_path=str(ssd))
        src = _FakeSource([_member(pub)])
        dest = _FakeDest([pub], files=list(files))
        coord = _make_coord(ssd, store, src, dest, _base_cfg(ssd))
        coord._source_free_bytes = AsyncMock(return_value=free_bytes)
        await coord._maybe_cleanup_source()
        return src.deleted

    assert await run_case(5 * GiB) != []   # pressured: early delete
    assert await run_case(60 * GiB) == []  # roomy: wait for DONE+grace


@pytest.mark.anyio
async def test_delete_source_group_partitions_by_size(tmp_path: Path):
    """Same dir + same size shares files (deleted once); different sizes
    are different files (each partition deletes its own)."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "s.db")
    cfg = _base_cfg(ssd)
    coord = _make_coord(ssd, store, _FakeSource([]), _FakeDest([]), cfg)

    def _mem(h, size):
        return Torrent(hash=h, name=FNAME, category="", save_path="/vps1/data",
                       size_bytes=size, state="seeding", progress=1.0)
    ts = TorrentState(source_infohash="a" * 40, source_name=FNAME)

    # Same size: one shared copy, files deleted exactly once.
    src = _FakeSource([_mem("a" * 40, 700), _mem("b" * 40, 700)])
    coord.source_client = src
    await coord._delete_source_group(
        ts, [_mem("a" * 40, 700), _mem("b" * 40, 700)], cfg.cleanup, False)
    assert src.deleted == [("a" * 40, True), ("b" * 40, False)]

    # Different sizes: different files, each partition deletes its own.
    src2 = _FakeSource([_mem("a" * 40, 700), _mem("c" * 40, 500)])
    coord.source_client = src2
    await coord._delete_source_group(
        ts, [_mem("a" * 40, 700), _mem("c" * 40, 500)], cfg.cleanup, False)
    assert src2.deleted == [("a" * 40, True), ("c" * 40, True)]


@pytest.mark.anyio
async def test_moving_early_skipped_when_ssd_bytes_gone(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    # NOTE: file deliberately absent from SSD.
    files = [TorrentFile(name=FNAME, size_bytes=FSIZE, progress=1.0)]
    pub = "a" * 40
    store = StateStore(tmp_path / "s.db")
    _row(store, pub, State.MOVING, completed_h_ago=None, activity_h_ago=10.0,
         injected="", save_path=str(ssd))
    src = _FakeSource([_member(pub)])
    dest = _FakeDest([pub], files=list(files))
    coord = _make_coord(ssd, store, src, dest, _base_cfg(ssd))
    coord._source_free_bytes = AsyncMock(return_value=1 * GiB)

    await coord._maybe_cleanup_source()

    assert src.deleted == []
