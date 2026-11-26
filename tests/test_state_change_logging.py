"""I13: state-changing bookkeeping failures must be loud, never `pass`.

These paths are intentionally best-effort (the main operation proceeds),
but a swallowed failure leaks a slot/budget or loses a repair write, so
each one logs a stable reason plus the row hash.
"""

from __future__ import annotations

import datetime as dt
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator


class _RaisingDiscardSet(set):
    def discard(self, _item):
        raise RuntimeError("bookkeeping exploded")


def test_transition_logs_admission_release_failure(tmp_path, caplog):
    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "state.db")
    try:
        ts = TorrentState(source_infohash="a" * 40, source_name="Show", state=State.QUEUED)
        store.upsert(ts)
        coord = make_coordinator(store)
        coord._ssd_reserved = {}
        coord._download_admissions = _RaisingDiscardSet({"a" * 40})

        with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
            coord.transition(ts, State.WAITING_DISK)

        assert "download-admission release failed" in caplog.text
        assert "aaaaaaaaaa" in caplog.text
        # The bookkeeping failure never blocks the transition itself.
        assert store.get("a" * 40).state == State.WAITING_DISK
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_logs_failed_activity_stamp(caplog):
    """A lost VPS1-activity stamp makes the row look quieter next run."""
    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord.cfg.cleanup = MagicMock()
    coord._cleanup_member_active = lambda _m, _c: True
    coord.store.set_vps1_activity = MagicMock(side_effect=RuntimeError("db locked"))
    ts = TorrentState(source_infohash="c" * 40, source_name="Show", state=State.DONE)

    with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
        verdict = await coord._cleanup_verdict(
            ts, [MagicMock()], 0.0, dt.datetime.now(dt.UTC), False, None
        )

    assert verdict is None, "a lively swarm still vetoes deletion this run"
    assert "could not stamp VPS1 activity" in caplog.text
    assert "cccccccccc" in caplog.text


@pytest.mark.anyio
async def test_cleanup_logs_failed_healed_blob_persist(caplog):
    """An unpersisted healing export keeps VPS1 and warns for the rerun."""
    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.dest_client = MagicMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"d4:infod1:xe")
    coord.store.set_blob = MagicMock(side_effect=RuntimeError("disk full"))
    ts = TorrentState(source_infohash="d" * 40, source_name="Show", state=State.DONE)

    with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
        healed = await coord._cleanup_heal_blob(ts, {"d" * 40}, False)

    assert healed is False
    assert "could not persist healed blob" in caplog.text
    assert "dddddddddd" in caplog.text
