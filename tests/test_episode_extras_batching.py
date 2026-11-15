"""Single-episode torrents with extras are batched when over the batch cap.

Audit D4: an episode-classified row with multiple files (episode + sample/
subs/nfo) matched neither the season nor the movie/unknown branch, so its
footprint fell back to sum(files). When extras pushed that over the cap
while each file fit, the refine-to-real grow failed every cycle and the
row looped in WAITING_DISK forever.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from racing_sync.clients.abstract import TorrentFile
from racing_sync.state import State, TorrentState

from test_ssd_ledger import _coord_with_cap


@pytest.mark.anyio
async def test_episode_with_extras_over_cap_is_batched(tmp_path):
    cap = 10_000
    coord = _coord_with_cap(tmp_path, cap)
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[
            TorrentFile(name="Show.S01E01.mkv", size_bytes=9_000, progress=0.0),
            TorrentFile(name="sample.mkv", size_bytes=3_000, progress=0.0),
        ]
    )
    coord.dest_client.set_file_priorities = AsyncMock()

    ts = TorrentState(
        source_infohash="e" * 40,
        dest_infohash="e" * 40,
        source_name="Show.S01E01",
        total_bytes=12_000,
        state=State.QUEUED,
    )

    class _Cls:
        kind = "episode"
        episodes: list = []

    with (
        patch("racing_sync.coordinator.classify", return_value=_Cls()),
        patch.object(coord, "_frozen_batch_cap", return_value=cap),
        patch.object(coord, "_fuse_skipped", AsyncMock(return_value=set())),
        patch.object(coord, "_ssd_adjust", AsyncMock(return_value=True)),
        patch.object(coord, "_park_queued_for_download_slot", AsyncMock(return_value=False)),
    ):
        await coord._setup_queued_download(ts, blob=b"")

    # 12_000 > 10_000 cap but each file fits: batched, not full-torrent.
    assert ts.batches_total == 2
    assert ts.batch_index == 0
    coord.dest_client.set_file_priorities.assert_awaited_once()
