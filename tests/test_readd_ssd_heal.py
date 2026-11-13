"""RE_ADDING healer: the SSD-presence check is size-aware.

A same-name zero-byte/truncated SSD leftover (interrupted earlier move)
must not read as "bytes are still local": that suppressed healing forever
even though the full file exists remotely, so the row only hit the 24h
FAIL. Only an exact expected-size match may block healing; a stat error
stays unverifiable ("" -> caller parks, never heals on doubt).
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import AddResult, Torrent, TorrentFile
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode, _bencoded_info_hash

TOP = "Batch"
PRESENT = f"{TOP}/ep01.mkv"
MISSING = f"{TOP}/ep04v2.mkv"
PRESENT_SIZE = 100
MISSING_SIZE = 200
KEY = "c" * 40


def _blob() -> bytes:
    return _bencode({
        b"info": {
            b"name": TOP.encode(),
            b"files": [
                {b"length": PRESENT_SIZE, b"path": [b"ep01.mkv"]},
                {b"length": MISSING_SIZE, b"path": [b"ep04v2.mkv"]},
            ],
        },
    })


def _scaffold(tmp_path):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd.mkdir(exist_ok=True)
    fuse_u.mkdir(parents=True, exist_ok=True)
    (fuse_u / TOP).mkdir(parents=True, exist_ok=True)
    # ep01 present at full size on the fuse target; ep04v2 nowhere.
    (fuse_u / TOP / "ep01.mkv").write_bytes(b"x" * PRESENT_SIZE)

    blob = _blob()
    blob_hash, _, _, _ = _bencoded_info_hash(blob)

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse_u
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 120
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.fuse_readd_heal_after_seconds = 7200
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord._rclone_move = AsyncMock()
    coord.transition = lambda t, s, error="": setattr(t, "state", s)
    coord._schedule_telegram_update = MagicMock()

    ts = TorrentState(
        source_infohash=KEY, source_name="Show.Batch",
        dest_infohash=blob_hash, save_path=str(ssd),
        classification_kind="movie",
        cross_seed_blob=blob,
        state=State.RE_ADDING,
        readd_attempts=1,
    )
    coord.store.upsert(ts)
    # Sustained window already elapsed for the missing file.
    coord._readd_heal_track = {
        KEY: [time.monotonic() - 10800, (MISSING,), 0],
    }
    return coord, coord.store, blob_hash, ssd


def _wire_download(coord, ssd, blob_hash):
    """Mock dest client so a heal can complete end-to-end."""

    async def _files(h):
        if not hasattr(_files, "n"):
            _files.n = 0
        _files.n += 1
        if _files.n == 1:
            return [
                TorrentFile(name=PRESENT, size_bytes=PRESENT_SIZE, progress=0.0),
                TorrentFile(name=MISSING, size_bytes=MISSING_SIZE, progress=0.0),
            ]
        (ssd / TOP).mkdir(exist_ok=True)
        (ssd / TOP / "ep04v2.mkv").write_bytes(b"y" * MISSING_SIZE)
        return [
            TorrentFile(name=PRESENT, size_bytes=PRESENT_SIZE, progress=1.0),
            TorrentFile(name=MISSING, size_bytes=MISSING_SIZE, progress=1.0),
        ]

    coord.dest_client.get_torrent_files = AsyncMock(side_effect=_files)
    coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
        hash=blob_hash, name="Show.Batch", size_bytes=300, progress=1.0,
        state="downloading", category="racing", save_path=str(ssd)))
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=blob_hash, accepted=True, detail="Ok."))

    async def _move(local, remote, ts, files_from=None, **kw):
        for n in files_from or []:
            p = Path(local, *n.split("/"))
            if p.is_file():  # noqa: ASYNC240 - tiny mock-move side effect
                p.unlink()  # noqa: ASYNC240 - tiny mock-move side effect

    coord._rclone_move = AsyncMock(side_effect=_move)


@pytest.mark.anyio
@pytest.mark.parametrize("ssd_size", [0, MISSING_SIZE - 50])
async def test_same_name_leftover_does_not_block_heal(tmp_path, ssd_size):
    """Zero-byte/truncated SSD leftover is not "still local": heal fires."""
    coord, store, blob_hash, ssd = _scaffold(tmp_path)
    try:
        _wire_download(coord, ssd, blob_hash)
        (ssd / TOP).mkdir(exist_ok=True)
        (ssd / TOP / "ep04v2.mkv").write_bytes(b"z" * ssd_size)

        await coord._do_re_add(store.get(KEY))

        coord.dest_client.add_torrent.assert_awaited_once()
        coord.dest_client.resume.assert_awaited()
        coord._rclone_move.assert_awaited_once()
        assert coord.dest_client.set_file_priorities.call_args[0][1] == {
            PRESENT: 0, MISSING: 1}
        # The stale stub was overwritten by the re-download, then moved.
        assert coord._rclone_move.call_args.kwargs["files_from"] == [MISSING]
        assert not (ssd / TOP / "ep04v2.mkv").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_exact_size_ssd_file_still_blocks_heal(tmp_path):
    """Full-size same-name SSD file = fuse lag, not loss: no heal."""
    coord, store, blob_hash, ssd = _scaffold(tmp_path)
    try:
        _wire_download(coord, ssd, blob_hash)
        (ssd / TOP).mkdir(exist_ok=True)
        (ssd / TOP / "ep04v2.mkv").write_bytes(b"y" * MISSING_SIZE)

        await coord._do_re_add(store.get(KEY))

        assert store.get(KEY).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        coord._rclone_move.assert_not_called()
        # Clock/attempts untouched: the incident is still parked.
        assert coord._readd_heal_track[KEY][2] == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_ssd_stat_error_is_unverifiable_no_heal(tmp_path, monkeypatch):
    """Stat/OSError on the SSD candidate: "" path, never heal on doubt."""
    coord, store, blob_hash, ssd = _scaffold(tmp_path)
    try:
        _wire_download(coord, ssd, blob_hash)
        (ssd / TOP).mkdir(exist_ok=True)
        (ssd / TOP / "ep04v2.mkv").write_bytes(b"y" * MISSING_SIZE)

        orig_stat = Path.stat

        def _flaky_stat(self, *a, **kw):
            if self.name == "ep04v2.mkv" and str(self).startswith(str(ssd)):
                raise OSError("simulated SSD stat failure")
            return orig_stat(self, *a, **kw)

        monkeypatch.setattr(Path, "stat", _flaky_stat)

        await coord._do_re_add(store.get(KEY))

        assert store.get(KEY).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        coord._rclone_move.assert_not_called()
        assert coord._readd_heal_track[KEY][2] == 0
    finally:
        store.close()
