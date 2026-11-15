"""Audit A3: /injectfuse preempt must only delete files at proven SSD paths.

The preempt drops VPS2 dest entries so the row can go RE_ADDING. Only an
entry whose save_path sits under a configured SSD root owns local bytes;
fuse entries and foreign/unknown paths (other disk, network share, empty
save_path) are removed file-less — `on_fuse == False` is not ownership.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode

H_SOURCE = "a" * 40
H_SSD = "d" * 40
H_FUSE = "e" * 40
H_FOREIGN = "f" * 40
NAME = "Show.S01E01.1080p"
TRACKER = "https://tracker.example/announce?passkey=AAA"


def _entry(h, save_path):
    return Torrent(
        hash=h,
        name=NAME,
        category="",
        save_path=save_path,
        size_bytes=500,
        state="downloading",
        progress=0.5,
        trackers=[TRACKER],
        files=[],
    )


def _blob(fname="Solo.mkv", size=500):
    return _bencode(
        {
            b"info": {b"name": fname.encode(), b"length": size},
        }
    )


@pytest.mark.anyio
async def test_preempt_deletes_files_only_under_ssd_root(tmp_path):
    """SSD-root entry goes with files; fuse and foreign entries do not."""
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd.mkdir()
    fuse.mkdir()
    fuse_u.mkdir()
    # The fuse gate runs before the preempt; make verification pass.
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse_u
    coord.cfg.classifier._episode_re = None
    coord.store = StateStore(tmp_path / "state.db")
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[_entry(H_SOURCE, "/vps1")])
    coord.source_client.export_torrent = AsyncMock(return_value=_blob())
    coord.sftp = None
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            _entry(H_SSD, str(ssd)),
            _entry(H_FUSE, str(fuse)),
            _entry(H_FOREIGN, "/mnt/other"),
        ]
    )
    coord.dest_client.delete = AsyncMock()
    coord._live = {}
    try:
        coord.store.upsert(
            TorrentState(
                source_infohash=H_SOURCE,
                source_name=NAME,
                total_bytes=500,
                state=State.DOWNLOADING,
                dest_infohash=H_SSD,
                cross_seed_infohash=H_FUSE,
                injected_private_hashes=H_FOREIGN,
            )
        )
        out = await coord.injectfuse_confirmed([H_SOURCE])
        assert out.startswith("injecting")
        # Deletes run concurrently: map hash -> delete_files, not order.
        got = {
            call.args[0]: call.kwargs.get("delete_files")
            for call in coord.dest_client.delete.await_args_list
        }
        assert got == {H_SSD: True, H_FUSE: False, H_FOREIGN: False}
        assert coord.store.get(H_SOURCE).state == State.RE_ADDING
    finally:
        coord.store.close()
