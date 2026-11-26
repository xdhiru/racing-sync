"""I11: one per-state worker dwell budget.

A wedged state handler used to pin its worker slot (and any semaphore it
held) until the 24h max-age backstop. Each handler call now runs under the
state's budget, is cancelled on expiry, and retried next tick; a row that
keeps overrunning fails for inspection.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import make_coordinator


def _ts(state=None, **kw):
    from racing_sync.state import State, TorrentState

    base = dict(
        source_infohash="b" * 40,
        source_name="Show",
        state=State.NEW if state is None else state,
    )
    base.update(kw)
    return TorrentState(**base)


@pytest.mark.anyio
async def test_over_budget_pass_is_cancelled_and_retried(tmp_path, monkeypatch):
    from racing_sync import coordinator as coord_mod
    from racing_sync.state import State, StateStore

    store = StateStore(tmp_path / "budget.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        store.upsert(ts)
        monkeypatch.setitem(coord_mod._STATE_WORK_BUDGET_S, State.NEW, 0.05)
        cancelled = asyncio.Event()

        async def _wedged(_row):
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        coord._do_new = _wedged
        await asyncio.wait_for(coord._process_torrent_inner(ts), timeout=2)

        assert cancelled.is_set(), "the wedged handler must be cancelled"
        fresh = store.get(ts.source_infohash, include_blob=False)
        assert fresh.state == State.NEW, "a budget hit parks, never transitions"
        assert "budget" in (fresh.last_error or "")
        assert coord._state_budget_hits[ts.source_infohash] == (State.NEW, 1)

        # A healthy pass clears both the counter and the advisory text.
        async def _fine(_row):
            return None

        coord._do_new = _fine
        fresh = store.get(ts.source_infohash, include_blob=False)
        await coord._process_torrent_inner(fresh)
        assert ts.source_infohash not in coord._state_budget_hits
        assert not (store.get(ts.source_infohash, include_blob=False).last_error or "")
    finally:
        store.close()


@pytest.mark.anyio
async def test_three_over_budget_passes_fail_the_row(tmp_path, monkeypatch):
    from racing_sync import coordinator as coord_mod
    from racing_sync.state import State, StateStore

    store = StateStore(tmp_path / "budget.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        store.upsert(ts)
        monkeypatch.setitem(coord_mod._STATE_WORK_BUDGET_S, State.NEW, 0.05)

        async def _wedged(_row):
            await asyncio.sleep(60)

        coord._do_new = _wedged
        for _ in range(3):
            row = store.get(ts.source_infohash, include_blob=False)
            await asyncio.wait_for(coord._process_torrent_inner(row), timeout=2)

        fresh = store.get(ts.source_infohash, include_blob=False)
        assert fresh.state == State.FAILED
        assert "budget" in (fresh.last_error or "")
        assert ts.source_infohash not in coord._state_budget_hits
    finally:
        store.close()


@pytest.mark.anyio
async def test_handler_timeout_error_is_not_mistaken_for_budget(tmp_path, monkeypatch):
    """File-drift/stall handlers raise TimeoutError as a park signal."""
    from racing_sync import coordinator as coord_mod
    from racing_sync.state import State, StateStore

    store = StateStore(tmp_path / "budget.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        store.upsert(ts)
        monkeypatch.setitem(coord_mod._STATE_WORK_BUDGET_S, State.NEW, 60.0)

        async def _drift(_row):
            raise TimeoutError("file list empty (1/3); re-planning next tick")

        coord._do_new = _drift
        with pytest.raises(TimeoutError, match="re-planning"):
            await coord._process_torrent_inner(ts)
        assert not getattr(coord, "_state_budget_hits", {})
    finally:
        store.close()


@pytest.mark.anyio
async def test_queue_budget_does_not_cover_the_download_it_promotes(tmp_path, monkeypatch):
    """Each handler call is budgeted separately, not the whole worker pass."""
    from racing_sync import coordinator as coord_mod
    from racing_sync.state import State, StateStore

    store = StateStore(tmp_path / "budget.db")
    try:
        coord = make_coordinator(store)
        coord._download_sem = asyncio.Semaphore(1)
        ts = _ts(state=State.QUEUED)
        store.upsert(ts)
        monkeypatch.setitem(coord_mod._STATE_WORK_BUDGET_S, State.QUEUED, 0.05)
        finished = asyncio.Event()

        async def _queued(row, *, _holds_download_slot=False):
            row.state = State.DOWNLOADING

        async def _downloading(_row):
            await asyncio.sleep(0.2)
            finished.set()

        coord._do_queued = _queued
        coord._do_downloading = _downloading
        await asyncio.wait_for(coord._process_torrent_inner(ts), timeout=2)

        assert finished.is_set(), "the download step must not inherit the queue budget"
        assert not getattr(coord, "_state_budget_hits", {})
    finally:
        store.close()
