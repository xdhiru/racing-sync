"""P8: admit-time stop-duplicate with recorded deferrals."""

from __future__ import annotations

import pytest


def _coord_with_store(tmp_path, **kw):
    from unittest.mock import AsyncMock, MagicMock

    from conftest import make_coordinator

    from racing_sync.state import StateStore

    coord = make_coordinator(StateStore(tmp_path / "s.db"))
    coord.cfg = MagicMock()
    coord.cfg.source.min_age_seconds = 0
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    for k, v in kw.items():
        setattr(coord, k, v)
    return coord


def _mkrow(h, name, state, size=1000):
    from racing_sync.state import TorrentState

    return TorrentState(source_infohash=h, source_name=name, state=state, total_bytes=size)


@pytest.mark.anyio
async def test_do_new_defers_to_inflight_sibling(tmp_path):
    """NEW row with a QUEUED same-content copy parks — no second search."""
    from unittest.mock import AsyncMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State

    coord = _coord_with_store(tmp_path)
    coord.store.upsert(_mkrow("o" * 40, "Twin.Show.S01E01", State.QUEUED))
    waiter = _mkrow("w" * 40, "Twin.Show.S01E01", State.NEW)
    coord.store.upsert(waiter)
    snap = Torrent(
        hash="w" * 40,
        name="Twin.Show.S01E01",
        category="racing",
        save_path="/vps1",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord._source_torrents_cache = [snap]
    coord.source_client.get_torrent = AsyncMock(return_value=snap)
    coord._pick_and_admit = AsyncMock()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)

    await coord._do_new(waiter)

    coord._pick_and_admit.assert_not_called()
    assert coord.store.get("w" * 40).state == State.WAITING_INDEXER
    assert coord._deferred_owner_of("w" * 40) == "o" * 40


@pytest.mark.anyio
async def test_do_new_picks_without_sibling(tmp_path):
    from unittest.mock import AsyncMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State

    coord = _coord_with_store(tmp_path)
    waiter = _mkrow("w" * 40, "Solo.Show.S01E01", State.NEW)
    coord.store.upsert(waiter)
    snap = Torrent(
        hash="w" * 40,
        name="Solo.Show.S01E01",
        category="racing",
        save_path="/vps1",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord._source_torrents_cache = [snap]
    coord.source_client.get_torrent = AsyncMock(return_value=snap)
    coord._pick_and_admit = AsyncMock()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)

    await coord._do_new(waiter)

    coord._pick_and_admit.assert_awaited_once()
    assert coord._deferred_owner_of("w" * 40) is None


@pytest.mark.anyio
async def test_waiting_indexer_fast_path_skips_prowlarr(tmp_path):
    """Recorded + live owner: no VPS1 fetch, no scan, timer pushed."""
    from unittest.mock import AsyncMock

    from racing_sync.state import State

    coord = _coord_with_store(tmp_path)
    coord.store.upsert(_mkrow("o" * 40, "Twin.Show.S01E01", State.QUEUED))
    waiter = _mkrow("w" * 40, "Twin.Show.S01E01", State.WAITING_INDEXER)
    coord.store.upsert(waiter)
    coord._deferred_on = {"w" * 40: "o" * 40}
    coord._pick_and_admit = AsyncMock()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)

    await coord._do_waiting_indexer(waiter)

    coord.source_client.get_torrent.assert_not_called()
    coord._pick_and_admit.assert_not_called()
    assert coord.store.get("w" * 40).indexer_next_retry_at is not None


@pytest.mark.anyio
async def test_waiting_indexer_owner_gone_clears_and_picks(tmp_path):
    from unittest.mock import AsyncMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State

    coord = _coord_with_store(tmp_path)
    # Owner DONE: no longer in-flight — the waiter proceeds alone.
    coord.store.upsert(_mkrow("o" * 40, "Twin.Show.S01E01", State.DONE))
    waiter = _mkrow("w" * 40, "Twin.Show.S01E01", State.WAITING_INDEXER)
    coord.store.upsert(waiter)
    coord._deferred_on = {"w" * 40: "o" * 40}
    snap = Torrent(
        hash="w" * 40,
        name="Twin.Show.S01E01",
        category="racing",
        save_path="/vps1",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord._source_torrents_cache = [snap]
    coord.source_client.get_torrent = AsyncMock(return_value=snap)
    coord._pick_and_admit = AsyncMock()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)

    await coord._do_waiting_indexer(waiter)

    assert coord._deferred_owner_of("w" * 40) is None
    coord._pick_and_admit.assert_awaited_once()


def test_deferral_record_helpers():
    from unittest.mock import MagicMock

    from conftest import make_coordinator

    coord = make_coordinator(MagicMock())
    assert coord._deferred_owner_of("w" * 40) is None
    coord._record_deferred_on("w" * 40, "O" * 40)
    assert coord._deferred_owner_of("w" * 40) == "o" * 40
    coord._clear_deferred_on("w" * 40)
    assert coord._deferred_owner_of("w" * 40) is None
    # Unknown owner never reads live.
    assert coord._deferred_owner_live("z" * 40) is False
    assert coord._deferred_owner_live("") is False
