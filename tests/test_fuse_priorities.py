"""_select_all_fuse_files: fuse seeds must want every file.

A fuse entry with deselected (priority 0) files reports complete via
skip_check while seeding nothing. The re-inject paths reset priorities
to all-wanted; transient client trouble parks (False) instead of
marking DONE.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import AddResult, Torrent, TorrentFile

H = "d" * 40


def _coord(tmp_path):
    fuse = tmp_path / "fuse"
    fuse.mkdir(exist_ok=True)
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()
    return coord, fuse


def _files():
    return [
        TorrentFile(name="Top/a.mkv", size_bytes=100, priority=1),
        TorrentFile(name="Top/b.mkv", size_bytes=200, priority=0),
    ]


@pytest.mark.anyio
async def test_all_wanted_passthrough(tmp_path):
    coord, _ = _coord(tmp_path)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Top/a.mkv", size_bytes=100, priority=1),
        TorrentFile(name="Top/b.mkv", size_bytes=200, priority=6),
    ])
    assert await coord._select_all_fuse_files(H, label="t") is True
    coord.dest_client.set_file_priorities.assert_not_called()


@pytest.mark.anyio
async def test_deselected_reset_to_wanted(tmp_path):
    coord, _ = _coord(tmp_path)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=_files())
    assert await coord._select_all_fuse_files(H, label="t") is True
    coord.dest_client.set_file_priorities.assert_awaited_once_with(
        H, {"Top/a.mkv": 1, "Top/b.mkv": 1})


@pytest.mark.anyio
async def test_list_failure_parks(tmp_path):
    coord, _ = _coord(tmp_path)
    coord.dest_client.get_torrent_files = AsyncMock(
        side_effect=TimeoutError("webui busy"))
    assert await coord._select_all_fuse_files(H, label="t") is False
    coord.dest_client.set_file_priorities.assert_not_called()


@pytest.mark.anyio
async def test_reset_failure_parks(tmp_path):
    coord, _ = _coord(tmp_path)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=_files())
    coord.dest_client.set_file_priorities = AsyncMock(
        side_effect=RuntimeError("rejected"))
    assert await coord._select_all_fuse_files(H, label="t") is False


@pytest.mark.anyio
async def test_non_list_fail_open(tmp_path):
    """Test doubles with plain MagicMock file lists: entry already verified."""
    coord, _ = _coord(tmp_path)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=MagicMock())
    assert await coord._select_all_fuse_files(H, label="t") is True


@pytest.mark.anyio
async def test_already_on_fuse_resets_priorities(tmp_path):
    """already-on-fuse exit selects all files before reporting injected."""
    coord, fuse = _coord(tmp_path)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=H, accepted=False, detail="already added"))
    coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
        hash=H, name="Top", category="racing", save_path=str(fuse),
        size_bytes=300, state="seeding", progress=1.0))
    coord.dest_client.get_torrent_files = AsyncMock(return_value=_files())
    ok, detail = await coord._ensure_fuse_entry(
        blob=b"fake", infohash=H, target_mount=Path(fuse), label="t")
    assert ok is True
    assert detail == "already added"
    coord.dest_client.set_file_priorities.assert_awaited_once_with(
        H, {"Top/a.mkv": 1, "Top/b.mkv": 1})


@pytest.mark.anyio
async def test_already_on_fuse_park_on_reset_failure(tmp_path):
    coord, fuse = _coord(tmp_path)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=H, accepted=False, detail="already added"))
    coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
        hash=H, name="Top", category="racing", save_path=str(fuse),
        size_bytes=300, state="seeding", progress=1.0))
    coord.dest_client.get_torrent_files = AsyncMock(return_value=_files())
    coord.dest_client.set_file_priorities = AsyncMock(
        side_effect=RuntimeError("rejected"))
    ok, _detail = await coord._ensure_fuse_entry(
        blob=b"fake", infohash=H, target_mount=Path(fuse), label="t")
    assert ok is False
