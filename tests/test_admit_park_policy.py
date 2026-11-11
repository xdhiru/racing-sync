"""Regression: SSD-budget park in the multi-file movie branch keeps files.

Audit A2: `_setup_queued_download` parks the multi-file non-episodic
(movie/unknown) branch when an SSD room re-check fails. That park must
drop only the client entry (`delete_files=False`), matching the
season/full-torrent and post-classify grow-fail parks: a metadata-driven
wipe can delete shared-save_path sibling data belonging to another
torrent, and keeping the paused entry lets the retry adopt it.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import TorrentFile
from racing_sync.state import State, TorrentState


@pytest.mark.anyio
async def test_setup_movie_batch_budget_park_keeps_files(tmp_path):
    """Budget park drops the entry without files and waits on disk."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()

    coord = make_coordinator()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Pack/part1.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/part2.bin", size_bytes=4000, progress=0.0),
    ])
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(
        side_effect=lambda t, s, error="": setattr(t, "state", s)
    )
    # No usable batch cap: the branch re-checks SSD room before batching.
    coord._frozen_batch_cap = MagicMock(return_value=0)

    ts = TorrentState(
        source_infohash="a" * 40,
        source_name="Pack",
        total_bytes=8000,
        dest_infohash="b" * 40,
        save_path=str(ssd),
        state=State.QUEUED,
    )

    with patch("racing_sync.coordinator.ssd_has_room", return_value=False):
        await coord._setup_queued_download(ts, b"blob")

    # Multi-file, non-episodic -> the movie/file-group branch parked.
    assert ts.classification_kind == "movie"
    # Keep the paused entry for the retry; never wipe shared save_path data.
    coord.dest_client.delete.assert_awaited_once_with(
        "b" * 40, delete_files=False
    )
    assert ts.state == State.WAITING_DISK
    coord.dest_client.resume.assert_not_called()
