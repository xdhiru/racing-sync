"""Legacy MOVING rows with no checkpoint and no dest entry: bounded parks.

A stage-less MOVING row whose dest entry is not listed (``_pre`` is an
empty list) used to park forever — the entry was likely deleted before
the durable-checkpoint era, so re-listing can never bring it back. The
first ``_LEGACY_MOVE_FAIL_PARKS - 1`` ticks park with a visible count;
the Nth consecutive no-checkpoint tick fails the row for operator
attention with ``delete_files=False`` (complete SSD bytes must survive).
A successful later listing resets the per-row counter so a stale count
can never fail a healthy tick.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import TorrentFile
from racing_sync.coordinator_move import _LEGACY_MOVE_FAIL_PARKS
from racing_sync.state import State, StateStore, TorrentState


def _legacy_row(tmp_path):
    """Stage-less MOVING row, complete bytes on SSD, entry not listed."""
    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    (ssd / "Solo.mkv").write_bytes(b"x" * 1000)
    files = [TorrentFile(name="Solo.mkv", size_bytes=1000,
                         progress=1.0, priority=1)]

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
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.delete = AsyncMock()
    coord._rclone_move = AsyncMock()
    coord._ssd_release = AsyncMock()
    coord._notify_telegram = AsyncMock()
    ts = TorrentState(
        source_infohash="l" * 40, source_name="Solo",
        dest_infohash="l" * 40, save_path=str(ssd),
        classification_kind="movie",
        batches_total=1, batch_index=0, state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_legacy_no_checkpoint_parks_then_fails(tmp_path):
    """First N-1 ticks park with a count; the Nth fails with bytes kept."""
    coord, store = _legacy_row(tmp_path)
    try:
        for n in range(1, _LEGACY_MOVE_FAIL_PARKS):
            with patch("racing_sync.coordinator.wipe_local_tree",
                       new_callable=AsyncMock) as mock_wipe:
                await coord._do_moving(store.get("l" * 40))
            row = store.get("l" * 40)
            assert row.state == State.MOVING
            assert "legacy MOVING row has no checkpoint" in (
                row.last_error or "")
            assert f"({n}/{_LEGACY_MOVE_FAIL_PARKS})" in (
                row.last_error or "")
            coord.dest_client.delete.assert_not_called()
            mock_wipe.assert_not_awaited()
        assert coord._legacy_move_parks["l" * 40] == (
            _LEGACY_MOVE_FAIL_PARKS - 1)

        # Nth consecutive no-checkpoint tick: terminal fail, entry dropped,
        # complete SSD bytes survive (delete_files=False).
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("l" * 40))
        row = store.get("l" * 40)
        assert row.state == State.FAILED
        assert "no checkpoint and no dest entry" in (row.last_error or "")
        coord.dest_client.delete.assert_awaited_once_with(
            "l" * 40, delete_files=False)
        coord._ssd_release.assert_awaited_once_with("l" * 40)
        mock_wipe.assert_not_awaited()
        assert "l" * 40 not in coord._legacy_move_parks
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_successful_listing_resets_legacy_counter(tmp_path):
    """A listed entry heals the counter; a later gap restarts at 1."""
    coord, store = _legacy_row(tmp_path)
    try:
        # Accrue under the fail threshold.
        for _ in range(_LEGACY_MOVE_FAIL_PARKS - 1):
            await coord._do_moving(store.get("l" * 40))
        assert coord._legacy_move_parks["l" * 40] == (
            _LEGACY_MOVE_FAIL_PARKS - 1)

        # The entry reappears: the successful listing clears the counter
        # instead of letting the stale count fail a healthy tick.
        coord.dest_client.list_torrents = AsyncMock(return_value=[
            SimpleNamespace(state="checkingDL", progress=0.5)])
        await coord._do_moving(store.get("l" * 40))
        assert store.get("l" * 40).state == State.MOVING
        assert coord._legacy_move_parks.get("l" * 40) is None
        coord.dest_client.delete.assert_not_called()

        # Gone again: counting restarts from 1 — no immediate fail.
        coord.dest_client.list_torrents = AsyncMock(return_value=[])
        await coord._do_moving(store.get("l" * 40))
        row = store.get("l" * 40)
        assert row.state == State.MOVING
        assert f"(1/{_LEGACY_MOVE_FAIL_PARKS})" in (row.last_error or "")
        assert coord._legacy_move_parks["l" * 40] == 1
        coord.dest_client.delete.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_legacy_counter_bounded_at_5000(tmp_path):
    """The per-row counter dict evicts instead of growing unbounded."""
    coord, store = _legacy_row(tmp_path)
    try:
        coord._legacy_move_parks = {f"{i:040d}": 1 for i in range(5000)}
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock):
            await coord._do_moving(store.get("l" * 40))
        assert len(coord._legacy_move_parks) <= 5000
        assert "l" * 40 in coord._legacy_move_parks
    finally:
        store.close()
