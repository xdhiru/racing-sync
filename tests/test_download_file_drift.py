"""File-list drift during batch wait: transient parks, persistent fails.

A batch's frozen ``expected_files`` can briefly miss the client's live
file list (metadata re-resolve, deselect, rename, registration lag).
The outer worker re-resolves batches next tick, so the first two
drifting/empty ticks must park (TimeoutError) and only the third
consecutive one fails the row (RuntimeError).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.state import TorrentState


def _torrent() -> Torrent:
    return Torrent(
        hash="h1", name="Show.S01", size_bytes=2000, progress=0.5,
        state="downloading", category="racing", save_path="/tmp",
    )


def _make_coord(tmp_path, files):
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.general.dest_poll_interval = 0.01
    coord.cfg.general.download_stall_timeout_seconds = 0
    coord.cfg.dest.save_path = tmp_path
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(return_value=_torrent())
    coord.dest_client.get_torrent_files = AsyncMock(return_value=list(files))
    # Polling hooks a bare coordinator does not provide.
    coord._check_dest_tracker_health = AsyncMock()
    coord._wait_worker_pause = AsyncMock()
    coord._batch_bytes_on_disk = AsyncMock(return_value=True)
    coord._fail_downloading_row = AsyncMock()
    return coord


def _ts() -> TorrentState:
    return TorrentState(
        source_infohash="h1", source_name="Show.S01", save_path="/tmp")


@pytest.mark.anyio
async def test_drift_parks_twice_then_fails(tmp_path):
    """Two drifting ticks park the row; the third fails it."""
    coord = _make_coord(tmp_path, [
        TorrentFile(name="Show.S01E01.mkv", size_bytes=1000, progress=1.0)])
    ts = _ts()
    key = "h1"

    with pytest.raises(TimeoutError, match=r"Show\.S01.*\(1/3\).*re-planning"):
        await coord._wait_for_completion(
            ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])
    assert coord._dl_file_drift[key] == 1
    assert coord.store.transition.call_count == 0
    coord._fail_downloading_row.assert_not_awaited()

    with pytest.raises(TimeoutError, match=r"Show\.S01.*\(2/3\).*re-planning"):
        await coord._wait_for_completion(
            ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])
    assert coord._dl_file_drift[key] == 2
    assert coord.store.transition.call_count == 0
    coord._fail_downloading_row.assert_not_awaited()

    # Persistent mismatch: the existing hard failure now fires.
    with pytest.raises(RuntimeError,
                       match="expected batch files missing from torrent"):
        await coord._wait_for_completion(
            ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])
    assert coord._dl_file_drift[key] == 3


@pytest.mark.anyio
async def test_clean_poll_resets_drift_counter(tmp_path):
    """A matching file list clears the row's consecutive-drift count."""
    coord = _make_coord(tmp_path, [
        TorrentFile(name="Show.S01E01.mkv", size_bytes=1000, progress=1.0)])
    ts = _ts()
    with pytest.raises(TimeoutError):
        await coord._wait_for_completion(
            ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])
    assert coord._dl_file_drift["h1"] == 1

    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=1000, progress=1.0),
        TorrentFile(name="Show.S01E02.mkv", size_bytes=1000, progress=1.0),
    ])
    await coord._wait_for_completion(
        ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])
    assert "h1" not in coord._dl_file_drift


@pytest.mark.anyio
async def test_empty_file_list_uses_same_escalation(tmp_path):
    """An empty live list parks twice, then fails as a mismatch."""
    coord = _make_coord(tmp_path, [])
    ts = _ts()
    expected = ["Show.S01E01.mkv"]

    for n in (1, 2):
        with pytest.raises(
                TimeoutError,
                match=rf"file list empty for Show\.S01 \({n}/3\)"):
            await coord._wait_for_completion(ts, expected_files=expected)
        assert coord._dl_file_drift["h1"] == n
        coord._fail_downloading_row.assert_not_awaited()

    with pytest.raises(RuntimeError,
                       match="expected batch files missing from torrent"):
        await coord._wait_for_completion(ts, expected_files=expected)
