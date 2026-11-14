"""SSD ledger rebuild aggregate clamp (audit D3).

After a crash with many in-flight multi-batch rows, each row re-reserves
min(total, cap) and the raw sum can reach N×cap — which blocks every
admission until each row refines. The rebuild must clamp the aggregate to
the global cap, oldest ``created_at`` first, so the earliest-admitted work
wins. With no configured cap the legacy per-row behavior is unchanged.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState

CAP = 10_000


def _row(infohash: str, total: int, state: State, *,
         created_at: datetime, batches_total: int = 4) -> TorrentState:
    return TorrentState(
        source_infohash=infohash, source_name=infohash,
        total_bytes=total, state=state, batches_total=batches_total,
        created_at=created_at)


def _coord_over(store: StateStore, cap: int | None) -> object:
    coord = make_coordinator()
    coord.store = store
    coord.cfg = MagicMock()
    # 0 → _ssd_global_cap() returns None (unlimited, legacy behavior).
    coord.cfg.ssd.max_inflight_bytes = 0 if cap is None else cap
    return coord


@pytest.mark.anyio
async def test_rebuild_clamps_aggregate_oldest_first(tmp_path, caplog):
    """Per-row estimates sum past the cap: oldest rows keep the budget,
    a younger row is skipped, and an even younger small row still fits."""
    store = StateStore(tmp_path / "state.db")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        rows = [
            _row("a" * 40, 4_000, State.DOWNLOADING, created_at=t0),
            _row("b" * 40, 4_000, State.QUEUED,
                 created_at=t0 + timedelta(seconds=1)),
            _row("c" * 40, 7_000, State.MOVING,
                 created_at=t0 + timedelta(seconds=2)),
            _row("d" * 40, 2_000, State.DOWNLOADING,
                 created_at=t0 + timedelta(seconds=3)),
            # Parked row holds nothing and must not count as a candidate.
            _row("w" * 40, 9_000, State.WAITING_DISK, created_at=t0),
        ]
        for r in rows:
            store.upsert(r)

        coord = _coord_over(store, CAP)
        with caplog.at_level(logging.WARNING,
                             logger="racing_sync.coordinator"):
            await coord._ssd_rebuild_from_db()

        reserved = dict(coord._ssd_reserved)
        # a(4000)+b(4000)=8000; c(7000) would push past 10000 → skipped;
        # d(2000) still fits afterwards. Aggregate never exceeds the cap.
        assert set(reserved) == {"a" * 40, "b" * 40, "d" * 40}
        assert coord._ssd_reserved_total() == CAP
        assert "clamped 1 of 4" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_rebuild_unlimited_cap_keeps_legacy_behavior(tmp_path, caplog):
    """_ssd_global_cap() None → no aggregate clamp; every active row
    reserves its per-row amount (multi-batch rows reserve their total)."""
    store = StateStore(tmp_path / "state.db")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        rows = [
            _row("a" * 40, 6_000, State.DOWNLOADING, created_at=t0),
            _row("b" * 40, 7_000, State.QUEUED,
                 created_at=t0 + timedelta(seconds=1)),
            _row("s" * 40, 9_000, State.MOVING,
                 created_at=t0 + timedelta(seconds=2), batches_total=1),
        ]
        for r in rows:
            store.upsert(r)

        coord = _coord_over(store, None)
        with caplog.at_level(logging.WARNING,
                             logger="racing_sync.coordinator"):
            await coord._ssd_rebuild_from_db()

        assert dict(coord._ssd_reserved) == {
            "a" * 40: 6_000, "b" * 40: 7_000, "s" * 40: 9_000}
        assert coord._ssd_reserved_total() == 22_000
        assert "clamped" not in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_rebuild_sorts_missing_created_at_last(tmp_path, caplog):
    """A corrupt/legacy row without created_at sorts last, so a younger
    row with a valid timestamp wins the clamped budget."""
    store = StateStore(tmp_path / "state.db")
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    try:
        rows = [
            _row("a" * 40, 6_000, State.DOWNLOADING, created_at=t0),
            _row("b" * 40, 6_000, State.QUEUED,
                 created_at=t0 + timedelta(seconds=1)),
        ]
        for r in rows:
            store.upsert(r)
        # Wipe the OLDEST row's timestamp (legacy/corrupt cell): it must
        # sort last, so b wins the budget and a is clamped.
        store._conn.execute(
            "UPDATE torrent_state SET created_at = '' "
            "WHERE source_infohash = ?",
            ("a" * 40,))

        coord = _coord_over(store, CAP)
        with caplog.at_level(logging.WARNING,
                             logger="racing_sync.coordinator"):
            await coord._ssd_rebuild_from_db()

        reserved = dict(coord._ssd_reserved)
        assert set(reserved) == {"b" * 40}
        assert coord._ssd_reserved_total() == 6_000
        assert "clamped 1 of 2" in caplog.text
    finally:
        store.close()
