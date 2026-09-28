"""Tests for download settle handling: lag recheck and park-escalation throttle."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.clients.abstract import TorrentFile


def _moving_single(tmp_path, *, progress=0.0):
    """Single-file MOVING row, full bytes on disk, lagging client progress."""
    from racing_sync.clients.abstract import Torrent

    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    (ssd / "Solo.mkv").write_bytes(b"x" * 1000)
    files = [TorrentFile(name="Solo.mkv", size_bytes=1000,
                         progress=progress, priority=1)]

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
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord._rclone_move = AsyncMock()
    ts = TorrentState(
        source_infohash="s" * 40, source_name="Solo",
        dest_infohash="s" * 40, save_path=str(ssd),
        classification_kind="movie",
        batches_total=1, batch_index=0, state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_settle_recheck_fires_once_then_parks(tmp_path):
    """Full bytes + lagging progress: one force recheck, then park (no wipe)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    row = store.get("s" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree",
               new_callable=AsyncMock) as mock_wipe:
        await coord._do_moving(row)
        assert row.state == State.MOVING
        coord.dest_client.recheck.assert_awaited_once_with("s" * 40)
        mock_wipe.assert_not_awaited()
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
        # Second tick: no second recheck (once per row), still parked.
        await coord._do_moving(store.get("s" * 40))
        assert coord.dest_client.recheck.await_count == 1
        assert store.get("s" * 40).state == State.MOVING
    store.close()


@pytest.mark.anyio
async def test_no_recheck_when_file_short(tmp_path):
    """Genuinely short file: pre-existing missing-file path, no recheck.

    Short files are unlinked as incomplete partials before the branch, so
    the move cannot find them (FileNotFoundError, pre-existing behavior).
    The settle recheck must not fire there — rechecking cannot create
    bytes, and the wipe below is never reached either.
    """
    coord, store = _moving_single(tmp_path, progress=0.0)
    (tmp_path / "ssd" / "Solo.mkv").write_bytes(b"x" * 100)  # short
    row = store.get("s" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree",
               new_callable=AsyncMock) as mock_wipe:
        with pytest.raises(FileNotFoundError):
            await coord._do_moving(row)
        coord.dest_client.recheck.assert_not_called()
        mock_wipe.assert_not_awaited()
    store.close()


def test_park_escalation_throttles_after_first_error(caplog):
    """_park_moving: ERROR at 5, then debug until every 20th park."""
    import logging

    coord = make_coordinator()
    coord.store = MagicMock()
    ts = TorrentState(source_infohash="p" * 40, source_name="Parked",
                      state=State.MOVING)
    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        for _ in range(30):
            coord._park_moving(ts, "waiting")
    errors = [r for r in caplog.records
              if r.levelno == logging.ERROR
              and "MOVING stalled" in r.getMessage()]
    # Parks 5 and 25 only.
    assert len(errors) == 2
    assert "(parked 5x" in errors[0].getMessage()
    assert "(parked 25x" in errors[1].getMessage()

