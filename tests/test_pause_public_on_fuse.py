"""pause_public_torrents_on_fuse: public fuse entries land paused, not seeding.

Public torrents injected onto the fuse mount (fresh add, already-seeding
duplicate, or replaced stale entry) are paused when
`cross_seed.pause_public_torrents_on_fuse` is true (default). Private
torrents always seed as before, and undecodable blobs fail open toward
seeding so a private is never paused by mistake.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import AddResult, Torrent
from racing_sync.config import CrossSeedConfig

PUBLIC_ANNOUNCE = "http://nyaa.tracker.wf/announce"
PRIVATE_ANNOUNCE = "https://beta.me/announce"


def _blob(announce: str, name: str = "Show.S01E01.mkv", size: int = 100) -> bytes:
    from racing_sync.watchdir import _bencode

    return _bencode(
        {
            b"announce": announce.encode(),
            b"info": {
                b"name": name.encode(),
                b"length": size,
                b"piece length": 16384,
                b"pieces": b"12345678901234567890",
            },
        }
    )


def _coord(pause_flag: bool, fuse: Path):
    coord = make_coordinator()
    coord.cfg.cross_seed.pause_public_torrents_on_fuse = pause_flag
    coord._save_path_points_at_target = lambda _sp, _tm: True  # noqa: E731
    coord.dest_client = AsyncMock()
    coord.dest_client.pause = AsyncMock()
    return coord


def _fuse_entry(h: str, fuse: Path) -> Torrent:
    return Torrent(
        hash=h, name="Show", category="racing", save_path=str(fuse),
        size_bytes=100, state="uploading", progress=1.0,
    )


def test_config_defaults_to_pause_public():
    assert CrossSeedConfig().pause_public_torrents_on_fuse is True


@pytest.mark.anyio
async def test_public_fresh_add_lands_paused(tmp_path: Path):
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "a" * 40
    coord = _coord(True, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=h, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))

    ok, _ = await coord._ensure_fuse_entry(
        blob=_blob(PUBLIC_ANNOUNCE), infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
    _, kwargs = coord.dest_client.add_torrent.call_args
    assert kwargs["paused"] is True
    assert kwargs["skip_check"] is True
    coord.dest_client.pause.assert_awaited_once_with(h)


@pytest.mark.anyio
async def test_public_add_disabled_seeds(tmp_path: Path):
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "b" * 40
    coord = _coord(False, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=h, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))

    ok, _ = await coord._ensure_fuse_entry(
        blob=_blob(PUBLIC_ANNOUNCE), infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
    _, kwargs = coord.dest_client.add_torrent.call_args
    assert kwargs["paused"] is False
    coord.dest_client.pause.assert_not_called()


@pytest.mark.anyio
async def test_private_never_pauses_even_when_enabled(tmp_path: Path):
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "c" * 40
    coord = _coord(True, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=h, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))

    ok, _ = await coord._ensure_fuse_entry(
        blob=_blob(PRIVATE_ANNOUNCE), infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
    _, kwargs = coord.dest_client.add_torrent.call_args
    assert kwargs["paused"] is False
    coord.dest_client.pause.assert_not_called()


@pytest.mark.anyio
async def test_public_already_seeding_gets_paused_in_place(tmp_path: Path):
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "d" * 40
    coord = _coord(True, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=None, accepted=False, detail="Fails.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))

    ok, detail = await coord._ensure_fuse_entry(
        blob=_blob(PUBLIC_ANNOUNCE), infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
    assert detail == "already added"
    coord.dest_client.pause.assert_awaited_once_with(h)


@pytest.mark.anyio
async def test_undecodable_blob_fails_open_to_seeding(tmp_path: Path):
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "e" * 40
    coord = _coord(True, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=h, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))

    ok, _ = await coord._ensure_fuse_entry(
        blob=b"not-a-torrent", infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
    _, kwargs = coord.dest_client.add_torrent.call_args
    assert kwargs["paused"] is False
    coord.dest_client.pause.assert_not_called()


@pytest.mark.anyio
async def test_pause_failure_keeps_success(tmp_path: Path):
    """A failed pause must not fail the row — entry is correctly placed."""
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    h = "f" * 40
    coord = _coord(True, fuse)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=h, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=_fuse_entry(h, fuse))
    coord.dest_client.pause = AsyncMock(side_effect=RuntimeError("webui down"))

    ok, _ = await coord._ensure_fuse_entry(
        blob=_blob(PUBLIC_ANNOUNCE), infohash=h, target_mount=fuse, label="test",
    )

    assert ok is True
