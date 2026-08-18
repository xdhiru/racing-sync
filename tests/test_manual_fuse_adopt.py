"""Manual fuse adoption: same infohash already seeding from fuse on VPS2.

Covers the reported incident: operator manually moves files to the rclone
remote and adds the VPS1 torrent on VPS2 pointing at the fuse mount with no
category/tags. The row was stuck in WAITING_INDEXER waiting for a prowlarr
hit that would never come, because:

  - recovery only lists category="racing" (manual add invisible), and
  - NEW/WAITING_INDEXER workers never checked dest before querying prowlarr.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.state import State, StateStore, TorrentState


def _fuse_cfg(tmp_path: Path, coordinator):
    fuse = tmp_path / "fuse"
    fuse.mkdir(exist_ok=True)
    (fuse / "unsorted").mkdir(exist_ok=True)
    coordinator.cfg.rclone.fuse.mount = fuse
    coordinator.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coordinator.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    return fuse


def _seeded_fuse_torrent(fuse: Path, h: str, name: str, *, category: str = "") -> Torrent:
    # No category by default: reproduces the manual add without settings.
    return Torrent(
        hash=h,
        name=name,
        category=category,
        save_path=str(fuse),
        size_bytes=100,
        state="seeding",
        progress=1.0,
    )


@pytest.mark.anyio
async def test_new_adopts_manual_fuse_without_category(tmp_path: Path):
    """_do_new fast-tracks to DONE without touching prowlarr."""
    store = StateStore(tmp_path / "state.db")
    h = "a" * 40
    ts = TorrentState(source_infohash=h, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"x" * 100)

    coord.source_client = AsyncMock()
    coord.source_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash=h, name="Pack.2026", category="", save_path="",
            size_bytes=100, state="seeding", progress=1.0,
            trackers=["https://tracker.animebytes.tv/announce"],
        )
    )
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.source_client.export_torrent = AsyncMock(return_value=b"blob")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Pack.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100, progress=1.0)]
    )
    # Must not be reached when manual fuse is present.
    coord.prowlarr = AsyncMock()
    coord.prowlarr.best_match = AsyncMock(
        side_effect=AssertionError("prowlarr must not be queried for manual fuse")
    )
    coord.sftp = None

    await coord._do_new(ts)

    reloaded = store.get(h)
    assert reloaded is not None
    assert reloaded.state == State.DONE
    assert reloaded.dest_infohash == h
    assert reloaded.save_path == str(fuse)
    coord.prowlarr.best_match.assert_not_called()
    # Category-agnostic: hash lookup, never a category filter.
    _, kwargs = coord.dest_client.list_torrents.call_args
    assert kwargs.get("hashes") == [h]
    assert "category" not in kwargs


@pytest.mark.anyio
async def test_waiting_indexer_adopts_manual_fuse(tmp_path: Path):
    """Parked rows adopt on retry instead of another prowlarr miss."""
    store = StateStore(tmp_path / "state.db")
    h = "b" * 40
    ts = TorrentState(source_infohash=h, source_name="Pack.2026", state=State.WAITING_INDEXER)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"y" * 100)

    coord.source_client = AsyncMock()
    coord.source_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash=h, name="Pack.2026", category="", save_path="",
            size_bytes=100, state="seeding", progress=1.0,
            trackers=["https://beta.me/announce"],
        )
    )
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Pack.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100, progress=1.0)]
    )
    coord.prowlarr = AsyncMock()
    coord.prowlarr.best_match = AsyncMock(
        side_effect=AssertionError("prowlarr must not be queried for manual fuse")
    )
    coord.sftp = None

    await coord._do_waiting_indexer(ts)

    assert store.get(h).state == State.DONE


@pytest.mark.anyio
async def test_manual_fuse_missing_bytes_does_not_adopt(tmp_path: Path):
    """skip_check ghost (complete but bytes absent, nothing on SSD): keep flow."""
    store = StateStore(tmp_path / "state.db")
    h = "c" * 40
    ts = TorrentState(source_infohash=h, source_name="Ghost.2026", state=State.NEW)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd

    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Ghost.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Ghost.2026.mkv", size_bytes=100, progress=1.0)]
    )

    adopted = await coord._adopt_manual_fuse_if_present(ts)

    assert adopted is False
    assert store.get(h).state == State.NEW


@pytest.mark.anyio
async def test_manual_fuse_incomplete_or_off_fuse_does_not_adopt(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"z" * 100)

    # Incomplete (still downloading) on fuse.
    h1 = "d" * 40
    ts1 = TorrentState(source_infohash=h1, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts1)
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h1, name="Pack.2026", category="", save_path=str(fuse),
                size_bytes=100, state="downloading", progress=0.5,
            )
        ]
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    assert await coord._adopt_manual_fuse_if_present(ts1) is False
    assert store.get(h1).state == State.NEW

    # Complete but on SSD, not fuse.
    h2 = "e" * 40
    ts2 = TorrentState(source_infohash=h2, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts2)
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h2, name="Pack.2026", category="racing",
                save_path="/local/ssd", size_bytes=100,
                state="seeding", progress=1.0,
            )
        ]
    )
    assert await coord._adopt_manual_fuse_if_present(ts2) is False
    assert store.get(h2).state == State.NEW


@pytest.mark.anyio
async def test_tick_sweep_adopts_parked_rows_in_one_lookup(tmp_path: Path):
    """_sweep_manual_fuse_adoptions batch-adopts without per-row RPC storm."""
    from racing_sync.coordinator import Coordinator  # noqa: F401  (ensures import ok)

    store = StateStore(tmp_path / "state.db")
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir(exist_ok=True)
    (fuse / "A.mkv").write_bytes(b"a" * 50)
    (fuse / "B.mkv").write_bytes(b"b" * 60)

    ha, hb = "a" * 40, "b" * 40
    store.upsert(TorrentState(source_infohash=ha, source_name="A", state=State.WAITING_INDEXER))
    store.upsert(TorrentState(source_infohash=hb, source_name="B", state=State.NEW))

    coord = make_coordinator()
    coord.store = store
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._running_infohashes = set()
    coord.dest_client = AsyncMock()

    async def _list_torrents(*, category=None, hashes=None):
        assert category is None  # category-agnostic
        assert hashes is not None
        out = []
        if ha in hashes:
            out.append(_seeded_fuse_torrent(fuse, ha, "A"))
        if hb in hashes:
            out.append(_seeded_fuse_torrent(fuse, hb, "B"))
        return out

    coord.dest_client.list_torrents = AsyncMock(side_effect=_list_torrents)

    async def _files(h):
        if h == ha:
            return [TorrentFile(name="A.mkv", size_bytes=50, progress=1.0)]
        return [TorrentFile(name="B.mkv", size_bytes=60, progress=1.0)]

    coord.dest_client.get_torrent_files = AsyncMock(side_effect=_files)

    await coord._sweep_manual_fuse_adoptions()

    assert coord.dest_client.list_torrents.await_count == 1
    assert store.get(ha).state == State.DONE
    assert store.get(hb).state == State.DONE


@pytest.mark.anyio
async def test_tick_sweep_skips_live_workers(tmp_path: Path):
    """Rows with a live worker are left to their own _do_* check (no race)."""
    store = StateStore(tmp_path / "state.db")
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir(exist_ok=True)
    (fuse / "A.mkv").write_bytes(b"a" * 10)

    ha = "a" * 40
    store.upsert(TorrentState(source_infohash=ha, source_name="A", state=State.NEW))

    coord = make_coordinator()
    coord.store = store
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._running_infohashes = {ha}
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])

    await coord._sweep_manual_fuse_adoptions()

    # Skipped before any dest RPC.
    coord.dest_client.list_torrents.assert_not_called()
    assert store.get(ha).state == State.NEW
