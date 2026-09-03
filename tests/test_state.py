from __future__ import annotations

import pytest

from racing_sync.state import State, check_transition


def test_terminal_done_has_no_outgoing():
    for s in [State.NEW, State.QUEUED, State.DOWNLOADING, State.MOVING,
              State.RE_ADDING, State.WAITING_DISK, State.QUERYING,
              State.FAILED]:
        with pytest.raises(ValueError):
            check_transition(State.DONE, s)


def test_failed_can_retry_to_queued():
    check_transition(State.FAILED, State.QUEUED)


def test_new_can_go_to_any_inflight():
    for s in [State.QUERYING, State.WAITING_DISK, State.QUEUED,
              State.DOWNLOADING, State.MOVING, State.RE_ADDING]:
        check_transition(State.NEW, s)


def test_downloading_to_moving_is_ok():
    check_transition(State.DOWNLOADING, State.MOVING)


def test_downloading_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.DOWNLOADING, State.DONE)


def test_moving_to_done_is_illegal():
    with pytest.raises(ValueError):
        check_transition(State.MOVING, State.DONE)


def test_seedpool_park_and_retry():
    check_transition(State.NEW, State.WAITING_SEEDPOOL)
    check_transition(State.QUERYING, State.WAITING_SEEDPOOL)
    check_transition(State.WAITING_SEEDPOOL, State.QUERYING)
    check_transition(State.WAITING_SEEDPOOL, State.QUEUED)
    check_transition(State.WAITING_SEEDPOOL, State.FAILED)


def test_seedpool_no_self_transition():
    # Re-parking bumps fields but should not go through transition().
    # The state machine still treats WAITING_SEEDPOOL -> WAITING_SEEDPOOL
    # as illegal; callers must upsert() instead.
    with pytest.raises(ValueError):
        check_transition(State.WAITING_SEEDPOOL, State.WAITING_SEEDPOOL)


def test_queued_to_done_is_allowed():
    # When a torrent is already complete/seeding on VPS2, QUEUED fast-tracks to DONE.
    check_transition(State.QUEUED, State.DONE)


def test_find_by_name_extension_matching(tmp_path):
    from racing_sync.state import StateStore, TorrentState

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    # Stored without .mkv (as Seedpool or VPS2 client might report)
    ts = TorrentState(
        source_infohash="hash123",
        source_name="Game.Day.Murders.S01E06.1080p-Kitsune",
        state=State.DONE,
    )
    store.upsert(ts)

    # Query with .mkv (as Aither reports)
    matches = store.find_by_name("Game.Day.Murders.S01E06.1080p-Kitsune.mkv")
    assert len(matches) == 1
    assert matches[0].source_infohash == "hash123"

    # Query exact without .mkv
    matches2 = store.find_by_name("Game.Day.Murders.S01E06.1080p-Kitsune")
    assert len(matches2) == 1