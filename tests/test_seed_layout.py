"""Top-folder matching between the VPS2 download and VPS1 racing seeds.

The destination torrent and a racing torrent for the same release can
disagree on the wrapper folder: one is a single flat file, the other is a
multi-file torrent whose names all sit under ``Top/``. The physical remote
layout is fixed by the download; every racing blob is re-added with a
save_path that makes its own relative names resolve there.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.torrent_layout import blob_common_top, blob_is_wrapped, is_flat
from racing_sync.watchdir import _bencode

H = "d" * 40
SIBLING = "e" * 40
NAME = "Show.S01E01.mkv"
TOP = "Show.S01E01"
SIZE = 1000


def _flat_blob(name: str = NAME, size: int = SIZE) -> bytes:
    return _bencode(
        {
            b"announce": b"http://tracker.example/announce",
            b"info": {
                b"name": name.encode(),
                b"length": size,
                b"piece length": 16384,
                b"pieces": b"0" * 20,
            },
        }
    )


def _wrapped_blob(top: str = TOP, name: str = NAME, size: int = SIZE) -> bytes:
    return _bencode(
        {
            b"announce": b"http://tracker.example/announce",
            b"info": {
                b"name": top.encode(),
                b"files": [{b"length": size, b"path": [name.encode()]}],
                b"piece length": 16384,
                b"pieces": b"0" * 20,
            },
        }
    )


def _racing_torrent(h: str, name: str) -> Torrent:
    return Torrent(
        hash=h,
        name=name,
        category="",
        save_path="/vps1/data",
        size_bytes=SIZE,
        state="seeding",
        progress=1.0,
    )


def _moving_coord(tmp_path: Path, files: list[TorrentFile], racing: list[Torrent]):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    for f in files:
        p = ssd / f.name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * f.size_bytes)

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=list(files))
    coord._list_source_torrents = AsyncMock(return_value=list(racing))

    async def _move(local, remote, ts, files_from=None, **kw):
        if files_from is not None:
            for n in files_from:
                p = Path(local, *str(n).split("/"))
                if p.is_file():
                    p.unlink()
        elif isinstance(local, Path) and local.is_file():
            local.unlink()

    coord._rclone_move = AsyncMock(side_effect=_move)
    ts = TorrentState(
        source_infohash=H,
        source_name=NAME,
        dest_infohash=H,
        save_path=str(ssd),
        classification_kind="episode",
        total_bytes=SIZE,
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_flat_target_wraps_for_folder_shaped_racing_sibling(tmp_path: Path):
    """Target flat + racing sibling under Top: move into Top and record it."""
    coord, store = _moving_coord(
        tmp_path,
        [TorrentFile(name=NAME, size_bytes=SIZE, progress=1.0, priority=1)],
        [_racing_torrent(H, NAME), _racing_torrent(SIBLING, TOP)],
    )
    try:
        await coord._do_moving(store.get(H))

        assert store.get(H).seed_subdir == TOP
        # Bare single move + (no siblings) -> one call into the wrapper.
        first = coord._rclone_move.await_args_list[0]
        assert first.args[1] == "remote:unsorted/" + TOP
    finally:
        store.close()


@pytest.mark.anyio
async def test_flat_target_stays_flat_without_folder_shaped_racing(tmp_path: Path):
    coord, store = _moving_coord(
        tmp_path,
        [TorrentFile(name=NAME, size_bytes=SIZE, progress=1.0, priority=1)],
        [_racing_torrent(H, NAME)],
    )
    try:
        await coord._do_moving(store.get(H))

        assert store.get(H).seed_subdir == ""
        assert coord._rclone_move.await_args_list[0].args[1] == "remote:unsorted"
    finally:
        store.close()


@pytest.mark.anyio
async def test_wrapped_target_keeps_its_own_top(tmp_path: Path):
    """A target that already carries Top moves unchanged and records Top."""
    coord, store = _moving_coord(
        tmp_path,
        [TorrentFile(name=f"{TOP}/{NAME}", size_bytes=SIZE, progress=1.0, priority=1)],
        [_racing_torrent(H, NAME)],
    )
    try:
        await coord._do_moving(store.get(H))

        assert store.get(H).seed_subdir == TOP
        assert all(call.args[1] == "remote:unsorted" for call in coord._rclone_move.await_args_list)
    finally:
        store.close()


@pytest.mark.anyio
async def test_movie_route_uses_the_default_remote(tmp_path: Path):
    """The same wrapping applies to the sorted (movie) remote."""
    coord, store = _moving_coord(
        tmp_path,
        [TorrentFile(name="Movie.2026.mkv", size_bytes=SIZE, progress=1.0, priority=1)],
        [_racing_torrent(H, "Movie.2026")],
    )
    try:
        ts = store.get(H)
        ts.source_name = "Movie.2026.mkv"
        ts.classification_kind = "movie"
        store.upsert(ts)

        await coord._do_moving(store.get(H))

        assert store.get(H).seed_subdir == "Movie.2026"
        assert coord._rclone_move.await_args_list[0].args[1] == "remote:media/Movie.2026"
    finally:
        store.close()


def test_blob_topology_helpers():
    assert blob_is_wrapped(_wrapped_blob()) is True
    assert blob_common_top(_wrapped_blob()) == TOP
    assert blob_is_wrapped(_flat_blob()) is False
    assert blob_common_top(_flat_blob()) is None
    assert is_flat([f"Top/{NAME}"]) is False


def _layout_coord(tmp_path: Path, *, subdir: str = "", blob: bytes | None = None):
    coord = make_coordinator()
    ts = TorrentState(
        source_infohash=H,
        source_name=NAME,
        seed_subdir=subdir,
        cross_seed_blob=blob or b"",
    )
    return coord, ts


def test_flat_blob_seeds_one_level_inside_the_physical_top(tmp_path: Path):
    coord, ts = _layout_coord(tmp_path, subdir=TOP)
    base = tmp_path / "fuse-unsorted"

    assert coord._seed_root_for(ts, _flat_blob(), base) == base / TOP


def test_seed_layout_preserves_the_exact_folder_case(tmp_path: Path):
    """POSIX fuse paths are case-sensitive: never fold the recorded top."""
    coord, ts = _layout_coord(tmp_path, subdir="Show.S01E01")
    base = tmp_path / "fuse-unsorted"

    assert coord._seed_subdir_for(ts) == "Show.S01E01"
    assert coord._seed_root_for(ts, _flat_blob(), base) == base / "Show.S01E01"


@pytest.mark.skipif(os.name == "nt", reason="Windows path comparisons fold case")
def test_case_only_fuse_save_path_is_not_the_same_target():
    """On POSIX a case-only save_path is a different directory, not our entry."""
    coord = make_coordinator()

    assert coord._save_path_points_at_target("/mnt/fuse/show", Path("/mnt/fuse/Show")) is False


@pytest.mark.anyio
async def test_case_only_top_variants_warn_and_keep_the_source_case(tmp_path: Path, caplog):
    """Two racing folders differing only by case: one physical copy wins."""
    import logging

    coord = make_coordinator()
    coord._list_source_torrents = AsyncMock(
        return_value=[
            _racing_torrent(H, "Show.S01E01"),
            _racing_torrent(SIBLING, "show.s01e01"),
        ]
    )
    ts = TorrentState(
        source_infohash=H,
        source_name="Show.S01E01.mkv",
        total_bytes=SIZE,
    )

    with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
        chosen = await coord._racing_wrapper_folder(ts)

    assert chosen == "Show.S01E01"
    assert "multiple racing top folders" in caplog.text
    assert "case-only variants cannot share a case-sensitive fuse mount" in caplog.text


def test_wrapped_blob_seeds_at_the_mount_root(tmp_path: Path):
    coord, ts = _layout_coord(tmp_path, subdir=TOP)

    assert coord._seed_root_for(ts, _wrapped_blob(), tmp_path) == tmp_path


def test_old_rows_derive_subdir_from_the_download_blob(tmp_path: Path):
    coord, ts = _layout_coord(tmp_path, subdir="", blob=_wrapped_blob())

    assert coord._seed_subdir_for(ts) == TOP


@pytest.mark.anyio
async def test_reinject_points_a_flat_racing_blob_inside_the_target_top(tmp_path: Path):
    """End-to-end: wrapped target on disk, flat racing blob -> base/Top."""
    fuse_unsorted = tmp_path / "fuse-unsorted"
    (fuse_unsorted / TOP).mkdir(parents=True)
    (fuse_unsorted / TOP / NAME).write_bytes(b"x" * SIZE)

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.store = StateStore(tmp_path / "state.db")
    coord._list_source_torrents = AsyncMock(return_value=[_racing_torrent(H, NAME)])
    coord._load_racing_torrent_bytes = AsyncMock(return_value=_flat_blob())
    coord._target_mount_for = MagicMock(return_value=fuse_unsorted)
    coord._target_mount_for_blob = MagicMock(return_value=fuse_unsorted)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, ""))
    ts = TorrentState(
        source_infohash=H,
        source_name=NAME,
        dest_infohash=H,
        total_bytes=SIZE,
        seed_subdir=TOP,
        state=State.RE_ADDING,
    )
    coord.store.upsert(ts)
    try:
        await coord._re_inject_racing_torrents(coord.store.get(H))

        kwargs = coord._ensure_fuse_entry.await_args.kwargs
        assert Path(kwargs["target_mount"]) == fuse_unsorted / TOP
        assert kwargs["infohash"] == H
    finally:
        coord.store.close()
