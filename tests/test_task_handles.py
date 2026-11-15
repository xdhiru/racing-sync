"""P2: TaskHandle cancel/registry, stop_workers_for via handles, statuses."""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.anyio
async def test_handle_cancel_task_lifecycle():
    from racing_sync.task import TaskHandle

    task = asyncio.ensure_future(asyncio.sleep(60))
    try:
        hd = TaskHandle(infohash="a" * 40, gid="a" * 12, kind="downloading", task=task)
        assert hd.cancel() is True
        assert hd.cancelled is True
        assert hd.event.is_set()
        await asyncio.sleep(0)
        assert task.cancelled() or task.done()
        assert hd.cancel() is False
    finally:
        if not task.done():
            task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.anyio
async def test_handle_cancel_aborts_owned_tree():
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync import rclone_ops
    from racing_sync.task import TaskHandle

    proc = MagicMock()
    proc.returncode = None
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.wait = AsyncMock(return_value=0)
    try:
        rclone_ops._ACTIVE_MOVES["tk"] = proc
        hd = TaskHandle(infohash="a" * 40, gid="a" * 12, kind="move", tree_key="tk")
        assert hd.cancel() is True
        proc.terminate.assert_called_once()
    finally:
        rclone_ops._ACTIVE_MOVES.clear()
        rclone_ops._RCLONE_PROGRESS.clear()


def test_abort_move_tree_edge_cases():
    from unittest.mock import MagicMock

    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import abort_move_tree

    try:
        assert abort_move_tree(None) is False
        assert abort_move_tree("") is False
        assert abort_move_tree("missing") is False
        done = MagicMock()
        done.returncode = 0
        rclone_ops._ACTIVE_MOVES["done"] = done
        assert abort_move_tree("done") is False
    finally:
        rclone_ops._ACTIVE_MOVES.clear()


def test_registry_register_and_unregister():
    from types import SimpleNamespace

    from racing_sync.task import (
        TaskHandle,
        handles_for,
        register_handle,
        unregister_handle,
    )

    coord = SimpleNamespace()
    ha = TaskHandle(infohash="A" * 40, gid="a" * 12, kind="dl")
    hb = TaskHandle(infohash="a" * 40, gid="a" * 12, kind="move")
    register_handle(coord, ha)
    register_handle(coord, hb)
    assert len(handles_for(coord, "a" * 40)) == 2
    unregister_handle(coord, ha)
    assert handles_for(coord, "a" * 40) == [hb]
    unregister_handle(coord, hb)
    assert handles_for(coord, "a" * 40) == []
    assert handles_for(coord, "") == []
    assert handles_for(object(), "a" * 40) == []


@pytest.mark.anyio
async def test_stop_workers_for_cancels_via_handles():
    from unittest.mock import AsyncMock, MagicMock

    from conftest import make_coordinator

    from racing_sync.task import handles_for

    coord = make_coordinator(MagicMock())

    async def _work(_ts):
        await asyncio.sleep(60)

    coord._process_torrent = AsyncMock(side_effect=_work)
    from racing_sync.state import State, TorrentState

    ts = TorrentState(source_infohash="a" * 40, source_name="Show", state=State.DOWNLOADING)
    coord._spawn_worker(ts)
    await asyncio.sleep(0.05)
    hd = handles_for(coord, "a" * 40)[0]
    assert await coord.stop_workers_for("a" * 40) is True
    assert hd.cancelled is True
    for _ in range(100):
        await asyncio.sleep(0)
        if not handles_for(coord, "a" * 40):
            break
    assert handles_for(coord, "a" * 40) == []


def test_status_for_lanes():
    from unittest.mock import MagicMock

    from racing_sync.state import State, TorrentState
    from racing_sync.task_status import (
        DownloadStatus,
        MoveStatus,
        QueueStatus,
        ReaddStatus,
        status_counts,
        status_for,
    )

    coord = MagicMock()
    coord.live_progress_map.return_value = {"a" * 40: 0.5}
    coord.live_speed_map.return_value = {"a" * 40: 1000}
    coord.rclone_move_progress_map.return_value = {}
    dl = status_for(
        coord, TorrentState(source_infohash="a" * 40, source_name="D", state=State.DOWNLOADING)
    )
    assert isinstance(dl, DownloadStatus)
    assert dl.progress == 0.5 and dl.speed_bps == 1000
    mv = status_for(
        coord, TorrentState(source_infohash="b" * 40, source_name="M", state=State.MOVING)
    )
    assert isinstance(mv, MoveStatus) and mv.progress is None
    assert isinstance(
        status_for(
            coord, TorrentState(source_infohash="c" * 40, source_name="R", state=State.RE_ADDING)
        ),
        ReaddStatus,
    )
    q = status_for(
        coord, TorrentState(source_infohash="d" * 40, source_name="Q", state=State.QUEUED)
    )
    assert isinstance(q, QueueStatus) and q.label == "Queued"
    # Doubles never crash the projection.
    assert status_for(None, MagicMock()).label != ""
    rows = [
        TorrentState(source_infohash="a" * 40, source_name="D", state=State.DOWNLOADING),
        TorrentState(source_infohash="d" * 40, source_name="Q", state=State.QUEUED),
    ]
    assert status_counts(coord, rows) == {"Downloading": 1, "Queued": 1}


def test_status_for_move_reads_rclone_map():
    from types import SimpleNamespace

    from racing_sync.state import State, TorrentState
    from racing_sync.task_status import status_for

    prog = SimpleNamespace(percent=25.0, speed_bps=2000, eta_seconds=60)
    coord = SimpleNamespace(
        live_progress_map=lambda: {},
        live_speed_map=lambda: {},
        rclone_move_progress_map=lambda: {"b" * 40: prog},
    )
    mv = status_for(
        coord, TorrentState(source_infohash="b" * 40, source_name="M", state=State.MOVING)
    )
    assert mv.progress == 0.25
    assert mv.speed_bps == 2000
    assert mv.eta_seconds == 60
