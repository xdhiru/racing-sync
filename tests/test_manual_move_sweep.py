"""Manual moves to the remote are detected without waiting for the indexer.

Two parts: the content probe looks one level deep (content moved into a
release folder), and a bounded sweep probes waiting rows every poll so a
manual move is adopted within one poll interval instead of one indexer
retry window.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode, _bencoded_info_hash

NAME = "Example.Show.S01E01.mkv"
FOLDER = "Example.Show.S01E01"
SIZE = 700


def _blob(name: str = NAME, size: int = SIZE) -> bytes:
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


def _hash_of(blob: bytes) -> str:
    return _bencoded_info_hash(blob)[0].lower()


def _row(store: StateStore, blob: bytes) -> TorrentState:
    ts = TorrentState(
        source_infohash=_hash_of(blob),
        source_name=NAME,
        dest_infohash=_hash_of(blob),
        total_bytes=SIZE,
        state=State.WAITING_INDEXER,
    )
    ts.cross_seed_blob = blob
    ts._blob = blob
    store.upsert(ts)
    return ts


def _coord(store: StateStore, mount: Path):
    coord = make_coordinator(store=store)
    coord.cfg = MagicMock()
    coord._target_mount_for = MagicMock(return_value=mount)
    coord._admit_fuse_existing = AsyncMock()
    return coord


@pytest.mark.anyio
async def test_probe_finds_content_moved_into_a_release_folder(tmp_path: Path):
    """One level deep: the torrent's file sits inside a release folder."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _blob()
        ts = _row(store, blob)
        mount = tmp_path / "fuse"
        (mount / FOLDER).mkdir(parents=True)
        (mount / FOLDER / NAME).write_bytes(b"x" * SIZE)

        coord = _coord(store, mount)
        status, found, present, total = await coord._fuse_content_evidence(ts)

        assert status == "present"
        assert found == blob
        assert (present, total) == (1, 1)
        # The physical top folder is recorded so the later checks look
        # where the operator's copy actually sits.
        assert ts.seed_subdir == FOLDER
        assert store.get(ts.source_infohash).seed_subdir == FOLDER
    finally:
        store.close()


@pytest.mark.anyio
async def test_probe_ignores_content_that_is_not_there(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _blob()
        ts = _row(store, blob)
        mount = tmp_path / "fuse"
        (mount / "Other.Release.S01E01").mkdir(parents=True)
        (mount / "Other.Release.S01E01" / "Other.mkv").write_bytes(b"x" * SIZE)

        coord = _coord(store, mount)
        status, _found, _present, _total = await coord._fuse_content_evidence(ts)

        assert status == "absent"
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_adopts_a_waiting_row_with_moved_content(tmp_path: Path):
    """A waiting row is adopted on the sweep, not on its retry timer."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _blob()
        ts = _row(store, blob)
        mount = tmp_path / "fuse"
        mount.mkdir(parents=True)
        (mount / NAME).write_bytes(b"x" * SIZE)

        coord = _coord(store, mount)
        await coord._sweep_manual_moved_content()

        coord._admit_fuse_existing.assert_awaited_once()
        args = coord._admit_fuse_existing.call_args[0]
        assert args[0].source_infohash == ts.source_infohash
        assert args[1] == blob
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_leaves_rows_without_content_alone(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _blob()
        _row(store, blob)
        mount = tmp_path / "fuse"
        mount.mkdir(parents=True)

        coord = _coord(store, mount)
        await coord._sweep_manual_moved_content()

        coord._admit_fuse_existing.assert_not_awaited()
    finally:
        store.close()


def test_release_folder_matching(tmp_path: Path):
    """The folder and the file name differ by extension and group suffix."""
    coord = make_coordinator()

    assert coord._child_matches_release(FOLDER, NAME) is True
    assert coord._child_matches_release("Example.Show.S01E01.1080p.WEB", NAME) is True
    assert coord._child_matches_release("Other.Release.S01E01", NAME) is False
    assert coord._child_matches_release("", NAME) is False
