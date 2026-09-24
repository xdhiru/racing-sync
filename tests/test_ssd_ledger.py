"""Global SSD reservation ledger: budget shared across concurrent torrents.

Covers the 32GB+16GB-over-40GB regression: admission reserves an estimate
(min(total, configured_cap)) and blocks when the global sum would exceed the
budget — even when each torrent alone fits current free space.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conftest import make_coordinator
from racing_sync.state import State, StateStore, TorrentState


def _coord_with_cap(tmp_path, cap: int):
    coord = make_coordinator()
    coord._stop = False
    # Unknown test-double rows must not prune real reservations (see _ssd_prune_stale).
    coord.store.get = MagicMock(side_effect=lambda h: TorrentState(
        source_infohash=h, source_name="x", state=State.DOWNLOADING))
    coord._ssd_reserved = {}
    coord._ssd_lock = None
    coord._waiting_disk_next_check = {}
    coord.cfg = MagicMock()
    coord.cfg.ssd.max_inflight_bytes = cap
    coord.cfg.ssd.path = tmp_path  # real dir → real free (huge), global cap binds
    coord.cfg.general.disk_safety_margin_bytes = 0
    coord.cfg.dest.save_path = tmp_path
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    return coord


@pytest.mark.anyio
async def test_global_budget_blocks_second_torrent(tmp_path):
    """32GB admitted, 16GB parked: reserved sum never exceeds 40GB."""
    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)

    ts1 = TorrentState(source_infohash="a" * 40, source_name="Big32",
                       total_bytes=32_000, state=State.NEW)
    ts2 = TorrentState(source_infohash="b" * 40, source_name="Mid16",
                       total_bytes=16_000, state=State.NEW)

    assert await coord._ssd_try_reserve(ts1.source_infohash, coord._ssd_estimate_for_new(32_000)) is True
    assert coord._ssd_reserved_total() == 32_000
    # 32k+16k=48k > 40k → park even though 16k alone fits free space.
    assert await coord._ssd_try_reserve(ts2.source_infohash, coord._ssd_estimate_for_new(16_000)) is False
    assert coord._ssd_reserved_total() == 32_000

    # First finishes (MOVING→RE_ADDING releases via transition hook path).
    await coord._ssd_release(ts1.source_infohash)
    assert await coord._ssd_try_reserve(ts2.source_infohash, coord._ssd_estimate_for_new(16_000)) is True
    assert coord._ssd_reserved_total() == 16_000


@pytest.mark.anyio
async def test_unknown_size_reserves_full_cap(tmp_path):
    """Unknown-size rows must not reserve 0 (admits on a full disk)."""
    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)

    assert coord._ssd_estimate_for_new(0) == cap
    assert await coord._ssd_try_reserve("u" * 40, coord._ssd_estimate_for_new(0)) is True
    assert coord._ssd_reserved_total() == cap


@pytest.mark.anyio
async def test_varying_batch_footprint_refines_down(tmp_path):
    """Season with uneven episodes + game pack refine to max batch, freeing budget."""
    cap = 10_000
    coord = _coord_with_cap(tmp_path, cap)

    # Admit a 16GB season optimistically as one batch-cap estimate.
    est = coord._ssd_estimate_for_new(16_000)
    assert est == 10_000
    assert await coord._ssd_try_reserve("s" * 40, est) is True
    # Post-classify the real max batch is 5GB (varying sizes) → shrink frees 5GB.
    assert await coord._ssd_adjust("s" * 40, 5_000) is True
    assert coord._ssd_reserved_total() == 5_000
    # A 4GB game pack now fits in the freed budget.
    assert await coord._ssd_try_reserve("g" * 40, 4_000) is True
    assert coord._ssd_reserved_total() == 9_000


@pytest.mark.anyio
async def test_single_topup_beyond_budget_fails(tmp_path):
    """Single 32GB file admitted as estimate must top up to total or roll back."""
    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)
    # Another torrent holds 30GB.
    assert await coord._ssd_try_reserve("x" * 40, 30_000) is True
    # New single estimated min(32k,40k)=32k → 30k+32k=62k > cap → blocked at admission.
    assert await coord._ssd_try_reserve("y" * 40, coord._ssd_estimate_for_new(32_000)) is False
    # Even if admitted (e.g. pre-ledger row), growing 10k→32k fails when over budget.
    coord._ssd_reserved["y" * 40] = 10_000
    assert await coord._ssd_adjust("y" * 40, 32_000) is False
    assert coord._ssd_reserved["y" * 40] == 10_000


@pytest.mark.anyio
async def test_wait_disk_promotes_after_release(tmp_path):
    """WAITING_DISK row waits quietly, then promotes once budget frees."""
    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)
    coord._waiting_disk_next_check = {}
    assert await coord._ssd_try_reserve("a" * 40, 32_000) is True

    ts = TorrentState(source_infohash="b" * 40, source_name="Mid16",
                      total_bytes=16_000, state=State.WAITING_DISK)
    await coord._wait_disk_then_queue(ts)
    assert ts.state == State.WAITING_DISK  # parked, deadline set
    assert ts.source_infohash in coord._waiting_disk_next_check

    await coord._ssd_release("a" * 40)
    coord._waiting_disk_next_check.clear()  # simulate 60s elapse
    await coord._wait_disk_then_queue(ts)
    assert ts.state == State.QUEUED


@pytest.mark.anyio
async def test_rebuild_after_abrupt_stop(tmp_path):
    """Kill -9 mid-download: new coordinator rebuilds ledger from DB rows."""
    from pathlib import Path

    db = tmp_path / "state.db"
    store = StateStore(Path(db))
    try:
        rows = [
            TorrentState(source_infohash="q" * 40, source_name="Q",
                         total_bytes=10_000, state=State.QUEUED),
            TorrentState(source_infohash="d" * 40, source_name="D",
                         total_bytes=12_000, state=State.DOWNLOADING,
                         batches_total=4, batch_index=1),
            TorrentState(source_infohash="m" * 40, source_name="M",
                         total_bytes=8_000, state=State.MOVING),
            TorrentState(source_infohash="w" * 40, source_name="W",
                         total_bytes=50_000, state=State.WAITING_DISK),
            TorrentState(source_infohash="f" * 40, source_name="F",
                         total_bytes=9_000, state=State.DONE),
        ]
        for r in rows:
            store.upsert(r)

        coord = make_coordinator()
        coord.store = store
        coord.cfg = MagicMock()
        coord.cfg.ssd.max_inflight_bytes = 40_000

        await coord._ssd_rebuild_from_db()

        reserved = dict(coord._ssd_reserved)
        # Active rows reserved, parked/terminal rows hold nothing.
        assert set(reserved) == {"q" * 40, "d" * 40, "m" * 40}
        assert reserved["q" * 40] == 10_000
        assert reserved["m" * 40] == 8_000
        # Batched DOWNLOADING row reserves one batch upper bound, not total.
        assert reserved["d" * 40] == min(12_000, 40_000)
        assert coord._ssd_reserved_total() == 10_000 + min(12_000, 40_000) + 8_000

        # Abrupt-stop replay: second coordinator on the SAME db file (no
        # shutdown cleanup) rebuilds identically — no leak, no double-spend.
        coord2 = make_coordinator()
        coord2.store = StateStore(Path(db))
        try:
            coord2.cfg = MagicMock()
            coord2.cfg.ssd.max_inflight_bytes = 40_000
            await coord2._ssd_rebuild_from_db()
            assert dict(coord2._ssd_reserved) == reserved
        finally:
            coord2.store.close()
    finally:
        store.close()


@pytest.mark.anyio
async def test_stale_reservation_pruned_after_forget(tmp_path):
    """Row deleted (forget/CLI without coordinator) is pruned on next admission."""
    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)
    coord._ssd_reserved["gone" + "0" * 36] = 30_000
    # store.get returns None → stale.
    coord.store.get = MagicMock(return_value=None)
    # New 16GB torrent: prune frees 30k first, then 16k fits.
    assert await coord._ssd_try_reserve("n" * 40, 16_000) is True
    assert "gone" + "0" * 36 not in coord._ssd_reserved

@pytest.mark.anyio
async def test_waiting_retry_uses_remaining_batches_not_total(tmp_path):
    """A partially-moved season retries WAITING_DISK on its remainder.

    Live case: full 32 GB estimate never fits the 37 GB budget alongside
    other rows, while the one unmoved batch (~5 GB) would. The retry must
    reserve the remainder, not the total.
    """
    from unittest.mock import AsyncMock
    from racing_sync.clients.abstract import TorrentFile

    cap = 40_000
    coord = _coord_with_cap(tmp_path, cap)
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=27_000, progress=1.0),
        TorrentFile(name="Show.S01E02.mkv", size_bytes=5_000, progress=0.0),
    ])
    coord._batch_cap_cache = {("w" * 40): 30_000}

    assert await coord._ssd_try_reserve("x" * 40, 32_000) is True
    ts = TorrentState(source_infohash="w" * 40, source_name="Season32",
                      dest_infohash="w" * 40, total_bytes=32_000,
                      classification_kind="season", batches_total=2,
                      batch_index=1, state=State.WAITING_DISK)
    await coord._wait_disk_then_queue(ts)

    assert ts.state == State.QUEUED
    # Remainder (~5k), not the full 32k estimate.
    assert coord._ssd_reserved["w" * 40] == 5_000
    assert coord._ssd_reserved_total() == 37_000


@pytest.mark.anyio
async def test_grow_beyond_physical_disk_fails(tmp_path):
    """A grow inside the global budget still fails when the disk is full.

    Admission checks physical free for the (smaller) estimate only; the
    refine-to-real step must re-check the delta or concurrent grows
    overcommit past ENOSPC.
    """
    cap = 1_000_000
    coord = _coord_with_cap(tmp_path, cap)
    assert await coord._ssd_try_reserve("p" * 40, 10_000) is True
    cramped = MagicMock(total=1_000_000, used=995_000, free=5_000)
    with patch("shutil.disk_usage", return_value=cramped):
        assert await coord._ssd_adjust("p" * 40, 100_000) is False
    assert coord._ssd_reserved["p" * 40] == 10_000
    # A grow that fits the live disk still succeeds.
    assert await coord._ssd_adjust("p" * 40, 12_000) is True
    assert coord._ssd_reserved["p" * 40] == 12_000


@pytest.mark.anyio
async def test_prune_stale_reaps_forgotten_wait_and_park_keys(tmp_path):
    """Forget bypasses transition pops: the prune reaps orphaned hints."""
    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(TorrentState(source_infohash="q" * 40, source_name="X",
                                  state=State.WAITING_DISK))
        coord = make_coordinator(store)
        coord.cfg = MagicMock()
        coord._waiting_disk_next_check = {"q" * 40: 1.0, "z" * 40: 2.0}
        coord._moving_parks = {"q" * 40: 3}
        coord._ssd_reserved = {}
        await coord._ssd_prune_stale()
        # Live WAITING_DISK row keeps its hint; gone rows and wrong-state
        # counters are reaped.
        assert coord._waiting_disk_next_check == {"q" * 40: 1.0}
        assert coord._moving_parks == {}
    finally:
        store.close()
