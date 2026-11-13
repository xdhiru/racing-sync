from __future__ import annotations

from racing_sync.state import State, StateStore, TorrentState, UpsertResult

H = "a" * 40


def _row(**kw) -> TorrentState:
    base = dict(
        source_infohash=H,
        source_name="Guarded Row",
        state=State.DOWNLOADING,
    )
    base.update(kw)
    return TorrentState(**base)


def test_guarded_upsert_writes_when_snapshot_matches(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row()
        assert store.upsert(ts) is UpsertResult.WRITTEN

        ts.classification_kind = "movie"
        assert store.upsert(
            ts, expected_version=ts.version, expected_state=State.DOWNLOADING,
        ) is UpsertResult.WRITTEN
        assert store.get(H).classification_kind == "movie"
    finally:
        store.close()


def test_stale_version_upsert_conflicts_without_clobbering(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        stale = _row()
        assert store.upsert(stale) is UpsertResult.WRITTEN

        # Another writer advances the row after our snapshot was taken.
        mover = store.get(H)
        store.transition(mover, State.MOVING)
        before = store.get(H)

        stale.source_name = "Stale Name"
        assert store.upsert(stale, expected_version=stale.version) is UpsertResult.CONFLICT

        after = store.get(H)
        assert after.state == State.MOVING
        assert after.source_name == "Guarded Row"
        assert after.version == before.version
    finally:
        store.close()


def test_expected_state_mismatch_conflicts(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row()
        assert store.upsert(ts) is UpsertResult.WRITTEN

        ts.source_name = "Renamed"
        assert store.upsert(
            ts, expected_state=State.MOVING,
        ) is UpsertResult.CONFLICT
        assert store.get(H).source_name == "Guarded Row"
    finally:
        store.close()


def test_guarded_upsert_never_inserts(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row()
        assert store.upsert(ts, expected_version=0) is UpsertResult.CONFLICT
        assert store.get(H) is None
    finally:
        store.close()


def test_guarded_upsert_still_respects_tombstones(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row()
        assert store.upsert(ts) is UpsertResult.WRITTEN
        store.tombstone(H)

        ts.source_name = "Resurrected"
        assert store.upsert(
            ts, expected_version=ts.version,
        ) is UpsertResult.TOMBSTONED
    finally:
        store.close()
