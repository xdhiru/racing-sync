"""Regression: failing a row that is forgotten mid-flight must not resurrect it.

Audit F1: `_fail_downloading_row`'s force-write fallback upserted the stale
in-memory row when the guarded transition raised, re-inserting a hard-deleted
row as a zombie FAILED row (which then pages Telegram). The fallback must
never run for an abandoned row, and must re-read the row for `ValueError`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState

_H = "a" * 40


def _row(h: str = _H) -> TorrentState:
    return TorrentState(
        source_infohash=h,
        source_name="Zombie.Release.1080p",
        dest_infohash="",
        state=State.DOWNLOADING,
    )


def _coord(store: StateStore):
    coord = make_coordinator(store)
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()
    coord._notify_telegram = AsyncMock()
    return coord


def _spy_upserts(store: StateStore) -> list:
    calls: list = []
    real_upsert = store.upsert

    def _upsert(row):
        calls.append(row)
        return real_upsert(row)

    store.upsert = _upsert  # type: ignore[method-assign]
    return calls


@pytest.mark.anyio
async def test_abandoned_mid_fail_does_not_resurrect(tmp_path):
    """Forget landing between the pre-check and the write leaves no zombie."""
    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(_row())
        coord = _coord(store)
        ts = store.get(_H)
        assert ts is not None
        real_transition = coord.transition

        def _forget_then_transition(row, dst, **kw):
            # Hard delete after the `_abandoned` pre-check passed: the real
            # transition now raises AbandonedError, hitting the fallback.
            assert store.hard_delete(_H) is True
            return real_transition(row, dst, **kw)

        coord.transition = _forget_then_transition
        upserts = _spy_upserts(store)

        await coord._fail_downloading_row(ts, _H, "boom")

        assert store.get(_H) is None
        assert upserts == []
        coord._notify_telegram.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_value_error_after_delete_does_not_resurrect(tmp_path):
    """A ValueError fallback also re-reads first: deleted rows stay deleted."""
    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(_row())
        coord = _coord(store)
        ts = store.get(_H)
        assert ts is not None

        def _forget_then_value_error(row, dst, **kw):
            assert store.hard_delete(_H) is True
            raise ValueError("simulated concurrent-move refusal")

        coord.transition = _forget_then_value_error
        upserts = _spy_upserts(store)

        await coord._fail_downloading_row(ts, _H, "boom")

        assert store.get(_H) is None
        assert upserts == []
        coord._notify_telegram.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_tombstoned_row_stays_invisible(tmp_path):
    """A row tombstoned mid-fail is not force-written either."""
    store = StateStore(tmp_path / "s.db")
    try:
        store.upsert(_row())
        coord = _coord(store)
        ts = store.get(_H)
        assert ts is not None
        real_transition = coord.transition

        def _tombstone_then_transition(row, dst, **kw):
            assert store.tombstone(_H) is True
            return real_transition(row, dst, **kw)

        coord.transition = _tombstone_then_transition
        upserts = _spy_upserts(store)

        await coord._fail_downloading_row(ts, _H, "boom")

        # Tombstoned rows are invisible to reads; no force-write happened.
        assert store.get(_H) is None
        assert store.get(_H, include_blob=False) is None
        assert upserts == []
        coord._notify_telegram.assert_not_called()
    finally:
        store.close()
