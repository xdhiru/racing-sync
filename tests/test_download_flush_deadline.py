"""Completion-flush deadline in the single-flow download wait.

At torrent level, progress >= 0.999 skips the progress stall watchdog, so a
client that keeps reporting ``downloading`` at 100% (final completion flush
never lands) used to loop forever and pin a worker + slot. The single-flow
branch now applies ``download_stall_timeout_seconds`` as a hard flush
deadline: frozen 100% raises DownloadStalledError (the caller parks and
eventually fails the row via the normal no-progress escalation), while a
flush that lands — or any genuine progress — clears the deadline.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync import coordinator_download as download_mod
from racing_sync.clients.abstract import Torrent
from racing_sync.coordinator_errors import DownloadStalledError
from racing_sync.state import TorrentState


class _FakeClock:
    """Monotonic clock advancing a fixed step per read."""

    def __init__(self, step: float) -> None:
        self._now = 1000.0
        self._step = step

    def monotonic(self) -> float:
        self._now += self._step
        return self._now


def _torrent(state: str, progress: float = 1.0) -> Torrent:
    return Torrent(
        hash="h1", name="Show.S01", size_bytes=2000, progress=progress,
        state=state, category="racing", save_path="/tmp",
    )


def _make_coord(tmp_path, *, state: str, stall_timeout: float) -> object:
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.general.dest_poll_interval = 0.01
    coord.cfg.general.download_stall_timeout_seconds = stall_timeout
    coord.cfg.dest.save_path = tmp_path
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(return_value=_torrent(state))
    # Polling hooks a bare coordinator does not provide.
    coord._check_dest_tracker_health = AsyncMock()
    coord._wait_worker_pause = AsyncMock()
    return coord


def _ts() -> TorrentState:
    return TorrentState(
        source_infohash="h1", source_name="Show.S01", save_path="/tmp")


@pytest.mark.anyio
async def test_complete_but_downloading_raises_after_flush_deadline(
        tmp_path, monkeypatch):
    """100% with a stuck ``downloading`` state fails once flush time exceeds
    the stall window, naming the row and the stuck client state."""
    clock = _FakeClock(step=0.5)
    monkeypatch.setattr(download_mod, "time", clock)
    coord = _make_coord(tmp_path, state="downloading", stall_timeout=1)
    ts = _ts()

    with pytest.raises(DownloadStalledError) as exc:
        await coord._wait_for_completion(ts)

    msg = str(exc.value)
    assert "Show.S01" in msg
    assert "downloading" in msg
    assert "stuck at 100%" in msg
    assert "completion flush" in msg
    assert exc.value.progress == 1.0
    # Parked each poll (first poll at +0.5s sets the deadline, +1.5s trips).
    assert coord._wait_worker_pause.await_count == 3


@pytest.mark.anyio
async def test_flush_landing_returns_without_raise(tmp_path, monkeypatch):
    """A state flip out of ``downloading`` returns normally even though far
    more than the stall window of wall time has passed."""
    clock = _FakeClock(step=100.0)
    monkeypatch.setattr(download_mod, "time", clock)
    coord = _make_coord(tmp_path, state="downloading", stall_timeout=1)
    calls = 0

    async def _get_torrent(h):
        nonlocal calls
        calls += 1
        return _torrent("seeding" if calls > 1 else "downloading")

    coord.dest_client.get_torrent = AsyncMock(side_effect=_get_torrent)
    ts = _ts()

    await coord._wait_for_completion(ts)
    assert coord._wait_worker_pause.await_count == 1


@pytest.mark.anyio
async def test_progress_advance_resets_flush_deadline(tmp_path, monkeypatch):
    """A download that advances (0.999 -> 1.0) mid-flush restarts the
    deadline: the raise only fires after a full window at the new progress."""
    clock = _FakeClock(step=1.0)
    monkeypatch.setattr(download_mod, "time", clock)
    coord = _make_coord(tmp_path, state="downloading", stall_timeout=1)
    seq = [0.999, 1.0, 1.0, 1.0, 1.0]
    coord.dest_client.get_torrent = AsyncMock(
        side_effect=lambda h: _torrent("downloading", seq.pop(0)))
    ts = _ts()

    with pytest.raises(DownloadStalledError) as exc:
        await coord._wait_for_completion(ts)
    assert exc.value.progress == 1.0
    # Without the reset, the deadline set at 0.999 would trip one poll
    # earlier (await_count 2); the advance restarts the window at 1.0.
    assert coord._wait_worker_pause.await_count == 3
