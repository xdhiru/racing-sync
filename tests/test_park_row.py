"""I1: one park primitive (`_park_row`) for worker early returns."""

from __future__ import annotations

import pytest
from conftest import make_coordinator

from racing_sync.coordinator_errors import AbandonedError
from racing_sync.state import State, StateStore, TorrentState


def _ts(**kw):
    base = dict(source_infohash="a" * 40, source_name="Show", state=State.DOWNLOADING)
    base.update(kw)
    return TorrentState(**base)


def test_park_row_records_reason_and_releases_live(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        store.upsert(ts)
        coord._live = {"a" * 40: object()}

        assert coord._park_row(ts, "waiting for the flush", live_key="a" * 40) is True

        row = store.get("a" * 40, include_blob=False)
        assert row.state == State.DOWNLOADING, "a park never changes state"
        assert (row.last_error or "").startswith("parked: waiting for the flush")
        assert coord._live == {}
    finally:
        store.close()


def test_park_row_does_not_clobber_a_row_that_moved_on(tmp_path):
    import copy

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        store.upsert(ts)
        snap = copy.copy(ts)
        # Another actor moves the row after this worker's snapshot.
        store.transition(ts, State.MOVING)

        assert coord._park_row(snap, "stale worker park") is False

        row = store.get("a" * 40, include_blob=False)
        assert row.state == State.MOVING
        assert row.last_error == "", "the stale park must not write anything"
    finally:
        store.close()


def test_park_row_raises_abandoned_for_a_forgotten_row(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        ts = _ts()
        with pytest.raises(AbandonedError):
            coord._park_row(ts, "row is gone")
    finally:
        store.close()


def test_park_row_falls_back_to_a_guarded_upsert():
    """A store without update_columns still gets a state-guarded write."""

    class _Store:
        def __init__(self):
            self.calls = []

        def get(self, _hash, include_blob=False):
            return object()  # row present

        def upsert(self, ts, **kw):
            self.calls.append((ts, kw))
            return "written"

    store = _Store()
    coord = make_coordinator()
    coord.store = store
    ts = _ts()

    assert coord._park_row(ts, "legacy store") is True
    assert len(store.calls) == 1
    assert store.calls[0][1] == {"expected_state": State.DOWNLOADING}
    assert (store.calls[0][0].last_error or "").startswith("parked: legacy store")


def test_park_readd_sets_the_timer_and_reason(tmp_path):
    import datetime as dt

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        ts = TorrentState(source_infohash="a" * 40, source_name="Show", state=State.RE_ADDING)
        store.upsert(ts)
        before = dt.datetime.now(dt.UTC)

        assert coord._park_readd(ts, "fuse lag", delay_s=120) is True

        row = store.get("a" * 40, include_blob=False)
        assert row.state == State.RE_ADDING, "a park never changes state"
        assert (row.last_error or "").startswith("parked: fuse lag")
        assert row.readd_next_retry_at is not None
        assert (row.readd_next_retry_at - before).total_seconds() >= 110
    finally:
        store.close()


def test_park_readd_clears_the_timer_when_healed(tmp_path):
    import datetime as dt

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        ts = TorrentState(
            source_infohash="b" * 40,
            source_name="Show",
            state=State.RE_ADDING,
            readd_next_retry_at=dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
        )
        store.upsert(ts)

        assert coord._park_readd(ts, "healed bytes moved", delay_s=None) is True

        row = store.get("b" * 40, include_blob=False)
        assert row.readd_next_retry_at is None
    finally:
        store.close()
