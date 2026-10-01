"""RE_ADDING sustained-missing healer: selective re-download + move.

The fuse gate parks on ANY absence, but fresh absences are usually
fuse/rclone index lag. The healer fires only after the SAME missing set
is observed continuously for fuse_readd_heal_after_seconds (2h default),
with a fresh uncached re-stat, a healthy mount, SSD bytes also absent,
and capped attempts — otherwise it parks exactly as before.
"""
from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

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


def _scaffold(tmp_path, *, heal_after=7200):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd.mkdir(exist_ok=True)
    (fuse_u / TOP).mkdir(parents=True, exist_ok=True)
    fuse_u.mkdir(parents=True, exist_ok=True)
    # ep01 present at full size on the fuse target; ep04v2 nowhere.
    # (Target mount comes from the blob kind -> unsorted for this layout.)
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
    coord.cfg.fuse_readd_heal_after_seconds = heal_after
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord._rclone_move = AsyncMock()
    coord.transition = lambda t, s, error="": setattr(t, "state", s)
    coord._schedule_telegram_update = MagicMock()

    ts = TorrentState(
        source_infohash="c" * 40, source_name="Show.Batch",
        dest_infohash=blob_hash, save_path=str(ssd),
        classification_kind="movie",
        cross_seed_blob=blob,
        state=State.RE_ADDING,
        readd_attempts=1,
    )
    coord.store.upsert(ts)
    return coord, coord.store, blob, blob_hash, ssd, fuse


def _track(coord, key, age_s, missing, attempts=0):
    coord._readd_heal_track = {
        key: [time.monotonic() - age_s, tuple(sorted(missing)), attempts]
    }


@pytest.mark.anyio
async def test_heal_not_before_threshold(tmp_path):
    """Fresh absence: clock starts, no re-download, parks as today."""
    coord, store, blob, _, _, _ = _scaffold(tmp_path)
    try:
        await coord._do_re_add(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        assert row.readd_next_retry_at is not None
        coord.dest_client.add_torrent.assert_not_called()
        coord._rclone_move.assert_not_called()
        track = coord._readd_heal_track
        assert "c" * 40 in track
        assert track["c" * 40][1] == (MISSING,)
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_disabled_zero(tmp_path):
    """Threshold 0: no clock, no heal, plain park."""
    coord, store, blob, _, _, _ = _scaffold(tmp_path, heal_after=0)
    try:
        await coord._do_re_add(store.get("c" * 40))
        assert store.get("c" * 40).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        assert getattr(coord, "_readd_heal_track", {}) in ({}, None) or \
            "c" * 40 not in coord._readd_heal_track
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_mount_outage_never_heals(tmp_path):
    """`<mount unavailable>` proves nothing: no clock, no heal."""
    coord, store, blob, _, _, fuse = _scaffold(tmp_path)
    try:
        # Kill the mount: every fuse stat reads absent-with-marker.
        import shutil
        shutil.rmtree(tmp_path / "fuse-unsorted")
        _track(coord, "c" * 40, 10800, (MISSING,))
        await coord._do_re_add(store.get("c" * 40))
        assert store.get("c" * 40).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        # Outage time must not count: clock dropped.
        assert "c" * 40 not in coord._readd_heal_track
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_set_change_restarts_clock(tmp_path):
    """A changed missing set restarts the window instead of healing."""
    coord, store, blob, _, _, fuse = _scaffold(tmp_path)
    try:
        # ep01 vanishes too now: set differs from the tracked one.
        (tmp_path / "fuse-unsorted" / TOP / "ep01.mkv").unlink()
        _track(coord, "c" * 40, 10800, (MISSING,))
        await coord._do_re_add(store.get("c" * 40))
        assert store.get("c" * 40).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        rec = coord._readd_heal_track["c" * 40]
        assert rec[1] == (f"{TOP}/ep01.mkv", MISSING)
        assert time.monotonic() - rec[0] < 60
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_ssd_bytes_present_no_redownload(tmp_path):
    """Bytes still on SSD = fuse lag, not loss: never re-download."""
    coord, store, blob, _, _, _ = _scaffold(tmp_path)
    try:
        ssd_top = tmp_path / "ssd" / TOP
        ssd_top.mkdir(exist_ok=True)
        (ssd_top / "ep04v2.mkv").write_bytes(b"y" * MISSING_SIZE)
        _track(coord, "c" * 40, 10800, (MISSING,))
        await coord._do_re_add(store.get("c" * 40))
        assert store.get("c" * 40).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        coord._rclone_move.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_attempts_capped(tmp_path):
    """Three strikes: fall through to max-age FAIL, no re-download loop."""
    coord, store, blob, _, _, _ = _scaffold(tmp_path)
    try:
        _track(coord, "c" * 40, 10800, (MISSING,), attempts=3)
        await coord._do_re_add(store.get("c" * 40))
        assert store.get("c" * 40).state == State.RE_ADDING
        coord.dest_client.add_torrent.assert_not_called()
        coord._rclone_move.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_happy_path_selective(tmp_path):
    """Sustained window elapsed: only the missing file downloads + moves."""
    coord, store, blob, blob_hash, ssd, fuse = _scaffold(tmp_path)
    try:
        _track(coord, "c" * 40, 10800, (MISSING,))

        async def _files(h):
            if not hasattr(_files, "n"):
                _files.n = 0
            _files.n += 1
            if _files.n == 1:
                # Priority-listing call: nothing complete yet.
                return [
                    TorrentFile(name=PRESENT, size_bytes=PRESENT_SIZE, progress=0.0),
                    TorrentFile(name=MISSING, size_bytes=MISSING_SIZE, progress=0.0),
                ]
            # Completion poll: the mock "download" lands the bytes.
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
            assert files_from == [MISSING]
            for n in files_from or []:
                p = Path(local, *n.split("/"))
                if p.is_file():
                    p.unlink()

        coord._rclone_move = AsyncMock(side_effect=_move)

        await coord._do_re_add(store.get("c" * 40))

        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        # Selective priorities: missing downloads, present skipped.
        prio_map = coord.dest_client.set_file_priorities.call_args[0][1]
        assert prio_map == {PRESENT: 0, MISSING: 1}
        coord.dest_client.resume.assert_awaited()
        coord.dest_client.delete.assert_awaited_once_with(
            blob_hash, delete_files=False)
        # Attempts capped so the fresh move cannot trigger a re-download
        # while fuse indexes it.
        assert coord._readd_heal_track["c" * 40][2] >= 3
    finally:
        store.close()
