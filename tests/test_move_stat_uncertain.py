"""Regression (audit A1): a stat() failure on the move path must never
classify content as an incomplete partial and unlink it.

Step 2 of ``_do_moving`` classifies every file; step 3 unlinks every member
of ``incomplete_files``. A transient ``Path.stat()`` error (EACCES, ESTALE,
fuse blip, AV lock) on a client-complete file used to be read as
``_on_disk = -1`` ("short on SSD"), so a fully downloaded file landed in
``incomplete_files`` and was destroyed while still in MOVING. Unverifiable
bytes must instead be treated like any other unverified file: never moved,
never individually deleted, and selected ones block the wipe (step 5b).
"""
from __future__ import annotations

import errno
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import TorrentFile
from racing_sync.state import State, StateStore, TorrentState

E1 = "Unc.Season/Unc.Season.S01E01.mkv"
TARGET = "Unc.Season/Unc.Season.S01E02.mkv"


def _raise_on_classification_stat(target: Path):
    """Raise OSError on the *second* stat of ``target`` inside the move loop.

    The loop stats each file twice before deciding: once via
    ``Path.exists()`` and once in the classification block. Raising on the
    second call puts a transient stat error exactly where the bug read it
    as "short on disk", while leaving later ``exists()``/``is_file()``
    checks working — so the old code's step-3 unlink still runs and the
    regression is observable.
    """
    real_stat = Path.stat
    key = os.path.normcase(str(target))
    calls = {"n": 0}
    fired: list[bool] = []

    def _flaky(self, *args, **kwargs):
        if os.path.normcase(str(self)) == key:
            calls["n"] += 1
            if calls["n"] == 2:
                fired.append(True)
                raise OSError(errno.EACCES, "Permission denied", str(self))
        return real_stat(self, *args, **kwargs)

    return _flaky, fired


def _moving_season_row(tmp_path, cls_files):
    """MOVING season row with E1+TARGET both on disk at full metadata size."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    top = ssd / "Unc.Season"
    top.mkdir()
    for name in (E1, TARGET):
        (ssd / name).write_bytes(b"x" * 1000)

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(
        side_effect=lambda t, s, error="": setattr(t, "state", s))
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
    coord.dest_client.get_torrent_files = AsyncMock(return_value=cls_files)
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")

    async def _fake_move(local, remote, ts, *, include=None, files_from=None,
                         extra=None):
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="u" * 40,
        source_name="Unc.Season",
        dest_infohash="u" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store.get("u" * 40)


@pytest.mark.anyio
async def test_complete_file_stat_error_is_not_unlinked(tmp_path):
    """Client-complete file whose stat() fails: never classified incomplete."""
    target = tmp_path / "ssd" / TARGET
    cls_files = [
        TorrentFile(name=E1, size_bytes=1000, progress=1.0, priority=1),
        # Client-verified complete AND full on disk; only stat() hiccups.
        TorrentFile(name=TARGET, size_bytes=1000, progress=1.0, priority=1),
    ]
    coord, row = _moving_season_row(tmp_path, cls_files)
    store = coord.store
    try:
        flaky_stat, fired = _raise_on_classification_stat(target)
        with patch.object(Path, "stat", flaky_stat):
            with patch("racing_sync.coordinator.wipe_local_tree",
                       new_callable=AsyncMock) as mock_wipe:
                await coord._do_moving(row)

        assert fired, "expected the classification stat() to raise"
        # (a) the fully downloaded file was not unlinked.
        assert target.exists()
        assert target.stat().st_size == 1000
        # (b) no destructive client delete was issued.
        assert not [c for c in coord.dest_client.delete.await_args_list
                    if c.kwargs.get("delete_files") is True]
        # (c) the row never advanced past MOVING into the wipe path.
        assert row.state in (State.MOVING, State.DOWNLOADING)
        assert row.state not in (State.RE_ADDING, State.DONE, State.FAILED)
        mock_wipe.assert_not_awaited()
        # The stat failure hands back / parks instead of deleting.
        coord.dest_client.resume.assert_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_incomplete_file_stat_error_is_not_left_to_wipe(tmp_path):
    """Non-complete file whose stat() fails: unverified, not silently skipped."""
    target = tmp_path / "ssd" / TARGET
    cls_files = [
        TorrentFile(name=E1, size_bytes=1000, progress=1.0, priority=1),
        # Selected, genuinely partial: stat() failure must not let the
        # folder wipe take its real bytes unverified (old code `continue`d).
        TorrentFile(name=TARGET, size_bytes=1000, progress=0.5, priority=1),
    ]
    coord, row = _moving_season_row(tmp_path, cls_files)
    store = coord.store
    try:
        flaky_stat, fired = _raise_on_classification_stat(target)
        with patch.object(Path, "stat", flaky_stat):
            await coord._do_moving(row)

        assert fired, "expected the classification stat() to raise"
        assert target.exists()
        assert target.stat().st_size == 1000
        assert not [c for c in coord.dest_client.delete.await_args_list
                    if c.kwargs.get("delete_files") is True]
        assert row.state in (State.MOVING, State.DOWNLOADING)
        assert row.state not in (State.RE_ADDING, State.DONE, State.FAILED)
        coord.dest_client.resume.assert_awaited()
    finally:
        store.close()
