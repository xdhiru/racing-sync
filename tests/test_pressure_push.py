"""The janitor-pressure trigger of the "push from VPS1" lane.

Fictional names only ("Example.Show.S01E01…-ExampleGroup.mkv", "seedremote"),
as everywhere in this repo.

The feature pinned here is ONE switch (`push_on_janitor_pressure`) consuming
ONE existing verdict (`CleanupMixin._cleanup_pressure`). Pinned:

* inert by default — with the switch off the sweep lists no rows, probes no
  disk and asks the janitor nothing at all;
* "pressured" is literally `bursting or urgent` (each arm on its own), never
  a re-derived threshold;
* unknown free space fails closed: no guess, no push;
* eligibility: only rows parked on an INDEXER miss — a public-export park, a
  watch-dir drop, an operator hold and a row that already owns a live push
  are all left alone;
* ordering biggest-first, bounded by `max_concurrent_pushes`;
* a refusal is backed off per hash instead of hot-looping every poll;
* `reason="pressure"` reaches `start_source_push`.

The fixture style (blobs whose own bytes carry the infohash they claim) is
the one `tests/test_coordinator_push.py` establishes, imported here so the
two files cannot drift.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator
from test_coordinator_push import (
    EPISODE_NAME,
    EPISODE_TOP,
    MOVIE_NAME,
    NEXT_EPISODE,
    _hash_of,
    _live_push,
    _push_cfg,
    _push_coordinator,
    _row,
    _single_blob,
    _vps1_torrent,
)

from racing_sync.config import AppConfig
from racing_sync.state import State, StateStore
from racing_sync.state_schema import LIVE_PUSH_STATES
from racing_sync.vps1_push import PushCheck

SMALL = 1_000_000
MEDIUM = 3_000_000
LARGE = 5_000_000


def _pressure_cfg(*, pressure: bool = True, cap: int = 1) -> AppConfig:
    """The push section with the one new switch set the way the test needs."""
    cfg = _push_cfg(cap=cap)
    cfg.source.rclone_push.push_on_janitor_pressure = pressure
    return cfg


def _parked(store: StateStore, name: str, size: int, **kw) -> tuple[bytes, str, object]:
    """A row parked as an indexer miss, plus the VPS1 torrent it can push.

    Returns (blob, infohash, live torrent): the row gets the same park a
    cross-seed miss leaves behind (indexer timers armed, no public schedule),
    unless the caller overrides those columns to park it some other way.
    """
    blob = _single_blob(name, size)
    h = _hash_of(blob)
    park = {
        "indexer_first_queried_at": dt.datetime.now(dt.UTC) - dt.timedelta(hours=1),
        "indexer_next_retry_at": dt.datetime.now(dt.UTC) + dt.timedelta(minutes=30),
    }
    park.update(kw)
    _ts = _row(
        store,
        blob,
        name=name,
        size=size,
        kind="episode",
        state=State.WAITING_INDEXER,
        **park,
    )
    return blob, h, _vps1_torrent(blob, name=EPISODE_TOP, size=size)


def _arm(coord, *, free: int | None, urgent: bool = False, bursting: bool = False) -> None:
    """Hand the sweep the janitor's verdict, with the probe cached as usual.

    `_cleanup_pressure` is mocked (the arms under test are its OUTPUT flags),
    but the call arguments are what pin "consumes the janitor's decision":
    the `[cleanup]` section plus the free bytes, nothing re-derived here.
    """
    coord._last_source_free_bytes = free
    coord._source_free_bytes = AsyncMock(return_value=free)
    coord._cleanup_pressure = MagicMock(return_value=(0.0, 0.0, None, urgent, bursting))


def _pressure_coordinator(store: StateStore, cfg: AppConfig, live: list) -> MagicMock:
    coord = _push_coordinator(store, cfg, live=live, files=[])
    coord._is_watch_row = MagicMock(
        side_effect=lambda ts: (getattr(ts, "cross_seed_source", "") or "") == "watch-dir"
    )
    return coord


@pytest.fixture
def transport(monkeypatch):
    """Preflight / launch / progress mocked, with the transport's own types."""
    from racing_sync import coordinator_push

    def _ok(request, **_kwargs):
        return PushCheck(
            True, "", list_path="/tmp/x.lst", log_path="/tmp/x.log", files_total=len(request.files)
        )

    holder = {
        "preflight": AsyncMock(side_effect=_ok),
        "start_copy": AsyncMock(side_effect=_ok),
        "remote_progress": AsyncMock(return_value=(0, 1)),
    }
    monkeypatch.setattr(coordinator_push, "preflight", holder["preflight"])
    monkeypatch.setattr(coordinator_push, "start_copy", holder["start_copy"])
    monkeypatch.setattr(coordinator_push, "remote_progress", holder["remote_progress"])
    return holder


# --------------------------------------------------------------------------- #
# the switch, and how pressure is obtained
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_switch_off_probes_nothing_and_pushes_nothing(tmp_path):
    """Inert by default: not even a query, so today's tick is unchanged."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(pressure=False), [torrent])
        coord._last_source_free_bytes = None
        coord._source_free_bytes = AsyncMock(return_value=1024)
        coord._cleanup_pressure = MagicMock(return_value=(0.0, 0.0, None, True, True))
        store.list_by_state = MagicMock(return_value=[])

        assert await coord.pressure_push_sweep() == 0

        coord._source_free_bytes.assert_not_awaited()
        coord._cleanup_pressure.assert_not_called()
        store.list_by_state.assert_not_called()
        assert store.list_pushes() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_master_switch_off_is_inert_even_with_the_pressure_switch_on(tmp_path):
    """`push_on_janitor_pressure` implies nothing about `[source].enabled`."""
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _pressure_cfg(pressure=True)
        cfg.source.rclone_push.enabled = False
        coord = _pressure_coordinator(store, cfg, [])
        assert coord._push_pressure_on() is False
        assert await coord.pressure_push_sweep() == 0
        assert store.list_pushes() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_not_pressured_pushes_nothing(tmp_path, transport):
    """The calm verdict is a no-op: the row keeps waiting out its window."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=99 * 1024**3)

        assert await coord.pressure_push_sweep() == 0

        transport["start_copy"].assert_not_awaited()
        assert store.list_pushes() == []
        # The janitor's own function answers, with its section and the free bytes.
        coord._cleanup_pressure.assert_called_once_with(coord.cfg.cleanup, 99 * 1024**3)
    finally:
        store.close()


@pytest.mark.anyio
async def test_bursting_arm_alone_starts_the_push(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=99 * 1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 1

        assert store.push_is_live(h) is True
        transport["start_copy"].assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_urgent_arm_alone_starts_the_push(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        # urgent without bursting is exactly what the janitor can report.
        _arm(coord, free=99 * 1024**3, urgent=True)

        assert await coord.pressure_push_sweep() == 1

        assert store.push_is_live(h) is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_uses_the_cached_probe_and_probes_only_once(tmp_path, transport):
    """The janitor's cached reading is preferred; a missing one costs one probe."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, h, torrent = _parked(store, EPISODE_NAME, SMALL)
        assert store.get(h).indexer_next_retry_at is not None
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 1
        coord._source_free_bytes.assert_not_awaited()

        # No cached reading (a boot before the janitor's first pass): the
        # sweep probes once itself and keeps the answer for the next tick.
        store.clear_push(h)
        coord._live_push_cache = None
        coord._last_source_free_bytes = None
        _blob2, _h2, torrent2 = _parked(store, MOVIE_NAME, MEDIUM)
        coord._list_source_torrents = AsyncMock(return_value=[torrent, torrent2])
        assert await coord.pressure_push_sweep() == 1
        coord._source_free_bytes.assert_awaited_once()
        assert coord._last_source_free_bytes == 1024**3
    finally:
        store.close()


@pytest.mark.anyio
async def test_unknown_free_space_fails_closed(tmp_path, transport):
    """Undetectable disk is never "pressured": no guess, no traffic."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=None, bursting=True)

        assert await coord.pressure_push_sweep() == 0

        transport["start_copy"].assert_not_awaited()
        coord._cleanup_pressure.assert_not_called()
        assert store.list_pushes() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_probe_that_raises_is_just_unknown(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        coord._last_source_free_bytes = None
        coord._source_free_bytes = AsyncMock(side_effect=RuntimeError("ssh is wedged"))
        coord._cleanup_pressure = MagicMock(return_value=(0.0, 0.0, None, True, True))

        assert await coord.pressure_push_sweep() == 0

        coord._cleanup_pressure.assert_not_called()
        assert store.list_pushes() == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# which rows qualify, in what order, how many at once
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_biggest_free_up_first(tmp_path, transport):
    """The janitor's biggest-first instinct, applied to the scarce slots."""
    store = StateStore(tmp_path / "state.db")
    try:
        _b1, small, t1 = _parked(store, EPISODE_NAME, SMALL)
        _b2, medium, t2 = _parked(store, MOVIE_NAME, MEDIUM)
        _b3, large, t3 = _parked(store, NEXT_EPISODE, LARGE)
        coord = _pressure_coordinator(store, _pressure_cfg(cap=3), [t1, t2, t3])
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 3

        order = [call.args[0].infohash for call in transport["start_copy"].await_args_list]
        assert order == [large, medium, small]
    finally:
        store.close()


@pytest.mark.anyio
async def test_never_exceeds_max_concurrent_pushes(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        _b1, _h1, t1 = _parked(store, EPISODE_NAME, SMALL)
        _b2, _h2, t2 = _parked(store, MOVIE_NAME, MEDIUM)
        _b3, _h3, t3 = _parked(store, NEXT_EPISODE, LARGE)
        coord = _pressure_coordinator(store, _pressure_cfg(cap=1), [t1, t2, t3])
        _arm(coord, free=1024**3, urgent=True)

        assert await coord.pressure_push_sweep() == 1

        assert transport["start_copy"].await_count == 1
        assert len(store.list_pushes(states=LIVE_PUSH_STATES)) == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_slots_already_taken_by_live_pushes_are_not_asked_for(tmp_path, transport):
    """The budget is the cap MINUS what is already running."""
    store = StateStore(tmp_path / "state.db")
    try:
        _b1, busy_h, t1 = _parked(store, EPISODE_NAME, MEDIUM)
        _b2, _h2, t2 = _parked(store, MOVIE_NAME, LARGE)
        coord = _pressure_coordinator(store, _pressure_cfg(cap=2), [t1, t2])
        _live_push(store, busy_h, expires_in=dt.timedelta(minutes=30))
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 1

        assert [c.args[0].infohash for c in transport["start_copy"].await_args_list] != [busy_h]
        assert transport["start_copy"].await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_row_that_already_has_a_live_push_is_skipped(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, busy_h, t1 = _parked(store, EPISODE_NAME, LARGE)
        _b2, other_h, t2 = _parked(store, MOVIE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(cap=3), [t1, t2])
        _live_push(store, busy_h, expires_in=dt.timedelta(minutes=30), files_total=7)
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 1

        # The existing checkpoint is untouched (its own counters survive, no
        # second copy launched); the row without a push is the one that ran.
        assert store.get_push(busy_h).files_total == 7
        assert [c.args[0].infohash for c in transport["start_copy"].await_args_list] == [other_h]
        assert len(store.list_pushes(states=LIVE_PUSH_STATES)) == 2
    finally:
        store.close()


@pytest.mark.anyio
async def test_only_indexer_miss_parks_qualify(tmp_path, transport):
    """Public-export parks, watch drops and held rows are nobody's pressure push."""
    store = StateStore(tmp_path / "state.db")
    try:
        _b1, miss_h, t1 = _parked(store, EPISODE_NAME, LARGE)
        # A public-export miss parks on its own indefinite schedule.
        _b2, public_h, t2 = _parked(
            store,
            MOVIE_NAME,
            MEDIUM,
            indexer_next_retry_at=None,
            indexer_first_queried_at=None,
            public_export_first_attempted_at=dt.datetime.now(dt.UTC),
            public_export_next_retry_at=dt.datetime.now(dt.UTC) + dt.timedelta(minutes=5),
        )
        assert public_h != miss_h
        # A watch-dir drop answers to its own election, not to the indexer.
        _b3, watch_h, t3 = _parked(store, NEXT_EPISODE, SMALL, cross_seed_source="watch-dir")
        # An operator-held row gets no automatic work at all.
        _b4, held_h, t4 = _parked(
            store, "Example.Held.1080p.x264-ExampleGroup.mkv", 4_000_000, skipped=1
        )
        coord = _pressure_coordinator(store, _pressure_cfg(cap=4), [t1, t2, t3, t4])
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 1

        live = {p.infohash for p in store.list_pushes(states=LIVE_PUSH_STATES)}
        assert live == {miss_h}
        for other in (public_h, watch_h, held_h):
            assert store.get_push(other) is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_row_vps2_is_already_working_on_is_not_offered(tmp_path, transport):
    """DOWNLOADING is untouched by a push; the sweep must not even ask."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, SMALL)
        ts = _row(
            store, blob, name=EPISODE_TOP, size=SMALL, kind="episode", state=State.DOWNLOADING
        )
        coord = _pressure_coordinator(
            store, _pressure_cfg(cap=4), [_vps1_torrent(blob, name=EPISODE_TOP, size=SMALL)]
        )
        _arm(coord, free=1024**3, bursting=True)

        assert await coord.pressure_push_sweep() == 0

        assert ts.state == State.DOWNLOADING
        transport["start_copy"].assert_not_awaited()
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# refusals back off; the reason travels
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_refused_push_is_backed_off_not_retried_every_tick(tmp_path, transport):
    """A push that cannot run must not cost a preflight per poll."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=1024**3, bursting=True)
        transport["preflight"].side_effect = lambda *_a, **_k: PushCheck(
            False, "push refused: rclone is not there"
        )

        assert await coord.pressure_push_sweep() == 0
        assert h in coord._push_pressure_backoff  # type: ignore[attr-defined]
        first_calls = transport["preflight"].await_count

        # Next tick: still pressured, still the same parked row — deferred.
        assert await coord.pressure_push_sweep() == 0
        assert transport["preflight"].await_count == first_calls

        # Once the defer expires the row is eligible again.
        coord._push_pressure_backoff[h] = 0.0  # type: ignore[attr-defined]
        assert await coord.pressure_push_sweep() == 0
        assert transport["preflight"].await_count > first_calls
    finally:
        store.close()


@pytest.mark.anyio
async def test_the_backoff_map_stays_bounded(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _pressure_coordinator(store, _pressure_cfg(), [])
        _arm(coord, free=1024**3, bursting=True)
        for i in range(500):
            coord._note_pressure_refusal(f"{i:040x}", "push refused: no bytes")
        memo = coord._push_pressure_backoff  # type: ignore[attr-defined]
        assert len(memo) <= 200
    finally:
        store.close()


@pytest.mark.anyio
async def test_pressure_reason_reaches_start_source_push(tmp_path):
    """The only storage the reason needs is the one the entry point logs."""
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=1024**3, urgent=True)
        coord.start_source_push = AsyncMock(return_value=(True, "pushing 1 file(s) from VPS1"))

        assert await coord.pressure_push_sweep() == 1

        coord.start_source_push.assert_awaited_once()
        called = coord.start_source_push.await_args
        assert called.kwargs == {"reason": "pressure"}
        assert called.args[0].source_infohash == _hash_of(_single_blob(EPISODE_NAME, SMALL))
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_logs_one_line_only_when_it_starts_pushes(tmp_path, transport, caplog):
    store = StateStore(tmp_path / "state.db")
    try:
        _blob, _h, torrent = _parked(store, EPISODE_NAME, SMALL)
        coord = _pressure_coordinator(store, _pressure_cfg(), [torrent])
        _arm(coord, free=123456789)
        coord._cleanup_pressure = MagicMock(
            return_value=(0.0, 0.0, None, False, False),
        )
        with caplog.at_level("INFO", logger="racing_sync.coordinator"):
            assert await coord.pressure_push_sweep() == 0
        assert not [r for r in caplog.records if "pressure push:" in r.getMessage()]

        # Pressured, but every push is refused: still nothing at INFO.
        coord._cleanup_pressure = MagicMock(return_value=(0.0, 0.0, None, True, False))
        transport["preflight"].side_effect = lambda *_a, **_k: PushCheck(
            False, "push refused: rclone is not there"
        )
        with caplog.at_level("INFO", logger="racing_sync.coordinator"):
            assert await coord.pressure_push_sweep() == 0
        assert not [r for r in caplog.records if "pressure push:" in r.getMessage()]

        transport["preflight"].side_effect = lambda *_a, **_k: PushCheck(True, "", files_total=1)
        coord._push_pressure_backoff.clear()  # type: ignore[attr-defined]
        with caplog.at_level("INFO", logger="racing_sync.coordinator"):
            assert await coord.pressure_push_sweep() == 1
        lines = [r.getMessage() for r in caplog.records if "pressure push:" in r.getMessage()]
        assert len(lines) == 1
        assert "urgent" in lines[0]
        assert "123456789" in lines[0]
        # No release names in the summary line (the push's own start log has them).
        assert EPISODE_NAME not in lines[0]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the poller tick it rides
# --------------------------------------------------------------------------- #


def _tick_coordinator() -> MagicMock:
    coord = make_coordinator()
    coord.scan_watch = AsyncMock(return_value=[])
    coord._list_source_torrents = AsyncMock(return_value=[])
    coord._poll_source_racing = AsyncMock(return_value=[])
    coord._sweep_manual_fuse_adoptions = AsyncMock()
    coord._sweep_manual_moved_content = AsyncMock()
    coord._drain_intake = AsyncMock(return_value=0)
    coord.push_sweep = AsyncMock()
    coord.pressure_push_sweep = AsyncMock(return_value=0)
    return coord


@pytest.mark.anyio
async def test_pressure_sweep_runs_right_after_the_push_sweep():
    """One line in the poll step, next to the sweep it follows."""
    coord = _tick_coordinator()
    await coord._poll_source_step()
    coord.pressure_push_sweep.assert_awaited_once()


@pytest.mark.anyio
async def test_a_pressure_sweep_failure_does_not_break_the_tick():
    coord = _tick_coordinator()
    coord.pressure_push_sweep = AsyncMock(side_effect=RuntimeError("boom"))
    await coord._poll_source_step()
    coord._drain_intake.assert_awaited_once()


@pytest.mark.anyio
async def test_cancellation_still_escapes_the_tick():
    coord = _tick_coordinator()
    coord.pressure_push_sweep = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await coord._poll_source_step()
