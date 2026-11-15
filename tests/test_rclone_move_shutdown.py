"""Shutdown must never start an rclone move after stop is requested."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.state import State, TorrentState


class _TrackingSem:
    """Semaphore stand-in that records whether it was entered."""

    def __init__(self) -> None:
        self.entered = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        return False


def _coord():
    coord = make_coordinator()
    coord._stop = False
    coord._move_sem = _TrackingSem()
    return coord


def _ts() -> TorrentState:
    return TorrentState(source_infohash="r" * 40, source_name="R", state=State.MOVING)


@pytest.mark.anyio
async def test_rclone_move_raises_when_stop_already_set(tmp_path):
    coord = _coord()
    coord._stop = True
    with patch("racing_sync.coordinator_move.move_local_to_remote", AsyncMock()) as child:
        with pytest.raises(asyncio.CancelledError):
            await coord._rclone_move(tmp_path, "remote:x", _ts())
    child.assert_not_awaited()
    assert coord._move_sem.entered is False


@pytest.mark.anyio
async def test_rclone_move_raises_when_stop_set_while_queued(tmp_path):
    coord = _coord()
    wake = asyncio.Event()

    def _queued_then_stop(ts, lane):
        assert lane == "move"
        coord._stop = True
        wake.set()
        return True, wake

    coord.check_running_tasks = MagicMock(side_effect=_queued_then_stop)
    with patch("racing_sync.coordinator_move.move_local_to_remote", AsyncMock()) as child:
        with pytest.raises(asyncio.CancelledError):
            await coord._rclone_move(tmp_path, "remote:x", _ts())
    coord.check_running_tasks.assert_called_once()
    child.assert_not_awaited()
    assert coord._move_sem.entered is False
