"""P3: central qb-listener verdicts + worker snapshot fast path."""
from __future__ import annotations

import asyncio
import time

import pytest


def _live_item(src="s" * 40):
    from types import SimpleNamespace
    return SimpleNamespace(progress=0.5, size_mb=1.0, dlspeed_bps=0,
                           source_infohash=src)


def _torrent(h, progress=0.5, state="downloading"):
    from racing_sync.clients.abstract import Torrent
    return Torrent(hash=h, name="Show", category="racing", save_path="",
                   size_bytes=1000, state=state, progress=progress,
                   dlspeed_bps=10)


@pytest.mark.anyio
async def test_listener_pass_records_verdicts_and_wakes():
    from unittest.mock import AsyncMock, MagicMock
    from conftest import make_coordinator
    from racing_sync.task import TaskHandle, handles_for, register_handle

    coord = make_coordinator(MagicMock())
    coord._stop = False
    coord.dest_client = MagicMock()
    coord._live = {"d" * 40: _live_item("s" * 40),
                   "e" * 40: _live_item("t" * 40)}
    coord.dest_client.list_torrents = AsyncMock(return_value=[
        _torrent("d" * 40, progress=1.0, state="seeding"),
        _torrent("e" * 40, progress=0.5, state="missingFiles"),
    ])
    hd = TaskHandle(infohash="s" * 40, gid="s" * 12, kind="downloading")
    register_handle(coord, hd)
    await coord._qb_listener_pass()
    assert coord._live_verdicts == {"d" * 40: "complete",
                                    "e" * 40: "missingFiles"}
    assert hd.event.is_set()
    # Snapshot stashed for workers.
    assert coord._live_snapshot_for("d" * 40).progress == 1.0


@pytest.mark.anyio
async def test_listener_pass_detects_error_and_stall():
    from unittest.mock import AsyncMock, MagicMock
    from conftest import make_coordinator

    coord = make_coordinator(MagicMock())
    coord._stop = False
    coord.cfg.general.download_stall_timeout_seconds = 10
    coord.dest_client = MagicMock()
    coord._live = {"d" * 40: _live_item("s" * 40),
                   "f" * 40: _live_item("u" * 40)}
    coord.dest_client.list_torrents = AsyncMock(return_value=[
        _torrent("d" * 40, progress=0.5, state="error"),
        _torrent("f" * 40, progress=0.5, state="downloading"),
    ])
    coord._live_still = {"f" * 40: [0.5, time.monotonic() - 30.0]}
    await coord._qb_listener_pass()
    assert coord._live_verdicts["d" * 40] == "error"
    assert coord._live_verdicts["f" * 40] == "stalled"


@pytest.mark.anyio
async def test_worker_completes_without_per_row_rpc():
    """Fresh snapshot + complete verdict: no get_torrent call."""
    from unittest.mock import AsyncMock, MagicMock
    from conftest import make_coordinator
    from racing_sync.state import State, TorrentState

    coord = make_coordinator(MagicMock())
    coord._stop = False
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock()
    coord.dest_client.get_tracker_messages = AsyncMock(return_value=[])
    h, s = "d" * 40, "s" * 40
    coord._live = {h: _live_item(s)}
    snap = _torrent(h, progress=1.0, state="seeding")
    coord._live_last = {h: snap}
    coord._live_last_at = time.monotonic()
    coord._live_verdicts = {h: "complete"}
    ts = TorrentState(source_infohash=s, source_name="Show",
                      state=State.DOWNLOADING, dest_infohash=h)
    await coord._wait_for_completion(ts)
    coord.dest_client.get_torrent.assert_not_awaited()
    assert coord._live_verdicts == {}


@pytest.mark.anyio
async def test_worker_falls_back_to_direct_fetch_when_stale():
    """No listener snapshot: today's direct get_torrent path."""
    from unittest.mock import AsyncMock, MagicMock
    from conftest import make_coordinator
    from racing_sync.state import State, TorrentState

    coord = make_coordinator(MagicMock())
    coord._stop = False
    coord.dest_client = MagicMock()
    coord.dest_client.get_tracker_messages = AsyncMock(return_value=[])
    h, s = "d" * 40, "s" * 40
    coord._live = {}
    coord._live_last = {}
    coord._live_last_at = 0.0
    coord.dest_client.get_torrent = AsyncMock(
        return_value=_torrent(h, progress=1.0, state="seeding"))
    ts = TorrentState(source_infohash=s, source_name="Show",
                      state=State.DOWNLOADING, dest_infohash=h)
    await coord._wait_for_completion(ts)
    coord.dest_client.get_torrent.assert_awaited()


@pytest.mark.anyio
async def test_listener_loop_idle_without_client():
    from unittest.mock import MagicMock
    from conftest import make_coordinator
    import racing_sync.coordinator_live as _live_mod

    coord = make_coordinator(MagicMock())
    coord._stop = False
    coord.dest_client = None
    coord._live = {}
    task = asyncio.ensure_future(coord._qb_listener_loop())
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
