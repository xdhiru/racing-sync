"""The indexer wait window COLLAPSES under VPS1 disk pressure.

Fictional names only ("Example.Show.S01E01.1080p.WEB-DL-GROUP"), as
everywhere in this repo.

Pinned behaviour of `RetryAdmitMixin._indexer_effective_wait_seconds` and the
TWO readers that must never disagree about it
(`_prowlarr_timed_out` = the racing-torrent/push trigger,
`_park_for_indexer_retry` = the FAILED escalation):

    effective = max(floor, min(indexer_wait_seconds, seconds_to_fill))
    floor     = max(_INDEXER_WAIT_COLLAPSE_FLOOR_S,
                    2 * prowlarr_retry_interval_seconds)

* the numbers come from the janitor's OWN `_cleanup_pressure` fed with its
  CACHED free-space probe — no second threshold, no new VPS1 RPC;
* calm box ⇒ the configured window, byte-identical to before (the FAILED
  wording included);
* unknown free space / no projection (bytes_rate 0) ⇒ no collapse: never
  guess at the disk;
* a projection below the floor is clamped up, never to zero and never under
  the retry interval;
* a live VPS1 push still re-arms instead of failing, however short the
  collapsed window is;
* public-export-schedule rows (no wait window at all) are untouched;
* one INFO line, only when the collapse is what caused the escalation, and a
  FAILED `last_error` that names the pressure instead of the plain timeout.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.config import AppConfig
from racing_sync.coordinator_admit_retry import _INDEXER_WAIT_COLLAPSE_FLOOR_S
from racing_sync.state import State, StateStore, TorrentState

ROOT = Path(__file__).resolve().parents[1]
GIB = 1024**3

#: The shipped defaults this feature is documented against.
WAIT = 28800
RETRY_GAP = 900

SHOW = "Example.Show.S01E01.1080p.WEB-DL-GROUP"
MOVIE = "Example.Movie.2026.1080p.WEB-DL-GROUP"


def _cfg(
    *,
    wait: int = WAIT,
    retry_gap: int = RETRY_GAP,
    low_watermark: int = 0,
    lookahead_hours: float = 0.0,
    burst_arrivals: float = 1e12,
) -> AppConfig:
    """The shipped example config with the wait keys pinned.

    Every pressure arm of `_cleanup_pressure` is OFF by default here (no
    watermark, no lookahead, an unreachable burst rate), so a test opts into
    pressure explicitly instead of inheriting a tuned default.
    """
    cfg = AppConfig.from_toml(ROOT / "config.example.toml")
    cfg.cross_seed.indexer_wait_seconds = wait
    cfg.cross_seed.prowlarr_retry_interval_seconds = retry_gap
    cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = True
    cfg.cleanup.low_watermark_free_bytes = low_watermark
    cfg.cleanup.fill_lookahead_hours = lookahead_hours
    cfg.cleanup.burst_arrivals_per_hour = burst_arrivals
    return cfg


def _coord(store: StateStore, cfg: AppConfig, *, free: int | None, arrivals: int, size_each: int):
    """A coordinator whose janitor state is the one a pressured box would have.

    `_cleanup_pressure` is NOT mocked: the arrival window and the cached
    probe are the real inputs it reads, so the collapse is exercised against
    the production arithmetic. `_source_free_bytes` explodes if called — the
    collapse must ride the cached reading, never a new VPS1 RPC.
    """
    coord = make_coordinator(store)
    coord.cfg = cfg
    now = time.monotonic()
    coord._last_source_free_bytes = free
    coord._arrival_times = [now] * arrivals
    coord._arrival_bytes = [(now, int(size_each))] * arrivals
    coord._cleanup_urgent = False
    coord._source_free_bytes = MagicMock(side_effect=AssertionError("collapse must not probe VPS1"))
    coord._push_is_live = MagicMock(return_value=False)
    coord._push_poll_seconds = MagicMock(return_value=120.0)
    return coord


def _projected_seconds(coord) -> float | None:
    """The janitor's own projection, read straight from `_cleanup_pressure`."""
    return coord._cleanup_pressure(coord.rt.cleanup.config, coord._last_source_free_bytes)[2]


def _row(store: StateStore, h: str, elapsed_s: float, *, name: str = SHOW) -> TorrentState:
    """A WAITING_INDEXER row parked `elapsed_s` ago (indexer schedule only)."""
    now = dt.datetime.now(dt.UTC)
    store.upsert(
        TorrentState(
            source_infohash=h,
            source_name=name,
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=now - dt.timedelta(seconds=elapsed_s),
            indexer_attempts=max(1, int(elapsed_s // RETRY_GAP)),
            indexer_next_retry_at=now,
        )
    )
    return store.get(h)


def test_collapse_lands_on_the_janitors_projection(tmp_path):
    """5 GiB free against 10 GiB/h of intake = 1800 s to fill ⇒ the 8 h wait
    collapses to that projection, and the reason carries both numbers."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB, lookahead_hours=2.0, burst_arrivals=8.0),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        assert _projected_seconds(coord) == pytest.approx(1800.0)
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "a" * 40, 600))
        assert wait_s == pytest.approx(1800.0)
        assert "collapsed" in reason and "1800s" in reason and "28800s" in reason
        assert "5368709120 B" in reason
    finally:
        store.close()


def test_trigger_and_escalation_agree_at_the_same_instant(tmp_path):
    """THE no-drift test: one row, two readers, one verdict.

    A row 2000 s old is past the collapsed 1800 s window for BOTH the
    racing-torrent trigger and the FAILED escalation; a 1000 s-old row is
    inside it for both. Mocked pressure is used here only to keep the two
    reads on the same instant.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _cfg(low_watermark=15 * GIB, lookahead_hours=2.0, burst_arrivals=8.0)
        coord = _coord(store, cfg, free=5 * GIB, arrivals=10, size_each=GIB)
        coord._cleanup_pressure = MagicMock(return_value=(10.0, 10 * GIB, 1800.0, False, True))

        stale = _row(store, "b" * 40, 2000)
        assert coord._prowlarr_timed_out(stale) is True
        coord._park_for_indexer_retry(stale, reason="indexer miss")
        assert store.get("b" * 40).state == State.FAILED

        fresh = _row(store, "c" * 40, 1000)
        assert coord._prowlarr_timed_out(fresh) is False
        coord._park_for_indexer_retry(fresh, reason="indexer miss")
        row = store.get("c" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_next_retry_at > dt.datetime.now(dt.UTC)
    finally:
        store.close()


def test_calm_box_keeps_the_full_window(tmp_path):
    """Not bursting ⇒ today's behaviour, even with a 1800 s projection that
    is far shorter than the configured wait."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), free=5 * GIB, arrivals=10, size_each=GIB)
        assert _projected_seconds(coord) == pytest.approx(1800.0)
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "d" * 40, 600))
        assert (wait_s, reason) == (float(WAIT), "")
        coord._park_for_indexer_retry(_row(store, "e" * 40, 2000), reason="indexer miss")
        assert store.get("e" * 40).state == State.WAITING_INDEXER
    finally:
        store.close()


def test_unknown_free_space_never_collapses(tmp_path):
    """No cached probe ⇒ no guess, and no new VPS1 RPC either."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=None,
            arrivals=10,
            size_each=GIB,
        )
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "f" * 40, 600))
        assert (wait_s, reason) == (float(WAIT), "")
        coord._park_for_indexer_retry(_row(store, "g" * 40, 2000), reason="indexer miss")
        assert store.get("g" * 40).state == State.WAITING_INDEXER
        coord._source_free_bytes.assert_not_called()
    finally:
        store.close()


def test_no_bytes_rate_means_no_projection(tmp_path):
    """A pressured box with an empty intake window has nothing to project:
    `_cleanup_pressure` returns None and the wait stays configured."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(low_watermark=15 * GIB), free=5 * GIB, arrivals=0, size_each=0)
        assert _projected_seconds(coord) is None
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "h" * 40, 600))
        assert (wait_s, reason) == (float(WAIT), "")
        coord._park_for_indexer_retry(_row(store, "i" * 40, 2000), reason="indexer miss")
        assert store.get("i" * 40).state == State.WAITING_INDEXER
    finally:
        store.close()


def test_projection_below_the_floor_is_clamped(tmp_path):
    """Never to zero, never under the retry interval: the floor is
    `max(_INDEXER_WAIT_COLLAPSE_FLOOR_S, 2 * prowlarr_retry_interval_seconds)`
    — built from a key that already exists."""
    store = StateStore(tmp_path / "state.db")
    try:
        # 1 GiB free against 3600 GiB/h -> a 1 s projection.
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=GIB,
            arrivals=1,
            size_each=3600 * GIB,
        )
        assert _projected_seconds(coord) == pytest.approx(1.0)
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "j" * 40, 600))
        assert wait_s == max(_INDEXER_WAIT_COLLAPSE_FLOOR_S, 2 * RETRY_GAP) == 1800.0
        assert wait_s >= RETRY_GAP > 0
        assert "1800s" in reason

        # A short retry interval cannot drop the floor under 600 s.
        tight = _coord(
            store,
            _cfg(retry_gap=120, low_watermark=15 * GIB),
            free=GIB,
            arrivals=1,
            size_each=3600 * GIB,
        )
        assert tight._indexer_effective_wait_seconds(_row(store, "k" * 40, 600))[0] == 600.0
    finally:
        store.close()


def test_urgent_alone_collapses_like_bursting(tmp_path):
    """ "Pressured" is the push valve's verdict — `bursting or urgent` — so an
    arrival that did not fit collapses the window on its own."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(lookahead_hours=2.0, burst_arrivals=8.0),
            free=40 * GIB,
            arrivals=1,
            size_each=20 * GIB,
        )
        # One arrival at 20 GiB/h over 40 GiB free projects 7200 s: exactly at
        # the lookahead edge, under the burst rate and above no watermark —
        # so nothing bursts, and the wait stays configured.
        assert _projected_seconds(coord) == pytest.approx(7200.0)
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "l" * 40, 600))
        assert (wait_s, reason) == (float(WAIT), "")
        # The urgency flag alone (an arrival that did not fit) is pressure.
        coord._cleanup_urgent = True
        assert coord._cleanup_pressure(coord.rt.cleanup.config, coord._last_source_free_bytes)[3]
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "l" * 40, 600))
        assert wait_s == pytest.approx(7200.0)
        assert "urgent" in reason
    finally:
        store.close()


def test_live_push_still_wins_over_a_collapsed_window(tmp_path):
    """The re-arm branch is checked before the escalation: while VPS1 is
    copying these bytes the row is never FAILED, however short the wait."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        coord._push_is_live = MagicMock(return_value=True)
        row = _row(store, "m" * 40, 2000)
        coord._park_for_indexer_retry(row, reason="indexer miss")
        after = store.get("m" * 40)
        assert after.state == State.WAITING_INDEXER
        assert after.indexer_next_retry_at > dt.datetime.now(dt.UTC)
        # Re-armed on the push cadence, and the clock was NOT reset.
        assert after.indexer_first_queried_at < dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1900)
    finally:
        store.close()


def test_vanished_source_still_gets_its_fresh_window_under_pressure(tmp_path):
    """The collapse shortens the clock, it does not remove the one fresh
    window a source that vanished from VPS1 is granted."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        before = dt.datetime.now(dt.UTC)
        row = _row(store, "n" * 40, 2000)
        coord._park_for_indexer_retry(row, reason="source vanished from VPS1")
        after = store.get("n" * 40)
        assert after.state == State.WAITING_INDEXER
        assert after.indexer_first_queried_at >= before
    finally:
        store.close()


def test_public_schedule_rows_are_never_touched(tmp_path):
    """`public_export_retry_interval_seconds` rows have no wait window: even a
    collapsing box must not FAILED them or arm an indexer timer."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="o" * 40,
                source_name=MOVIE,
                state=State.WAITING_INDEXER,
                # Far past both the configured and any collapsed window.
                indexer_first_queried_at=now - dt.timedelta(seconds=WAIT + 600),
                indexer_attempts=99,
                public_export_first_attempted_at=now - dt.timedelta(days=3),
                public_export_attempts=120,
            )
        )
        coord._park_for_indexer_retry(store.get("o" * 40), reason="source export miss")
        row = store.get("o" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_next_retry_at is None
        assert row.public_export_next_retry_at is not None
        assert row.public_export_next_retry_at > now
    finally:
        store.close()


def test_collapse_escalation_logs_once_and_names_the_pressure(tmp_path, caplog):
    """One INFO line with both numbers plus the free-space signal, and a
    FAILED `last_error` that says what really ended the wait."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        row = _row(store, "p" * 40, 2000)
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            coord._park_for_indexer_retry(row, reason="indexer miss")
        collapse = [r for r in caplog.records if "configured window alone" in r.getMessage()]
        assert len(collapse) == 1
        assert collapse[0].levelno == logging.INFO
        msg = collapse[0].getMessage()
        assert "effective 1800s" in msg and "configured 28800s" in msg
        assert "VPS1 free 5368709120 B" in msg
        failed = store.get("p" * 40)
        assert failed.state == State.FAILED
        assert "disk pressure" in failed.last_error
        assert "2000s" in failed.last_error
    finally:
        store.close()


def test_no_collapse_credit_when_the_full_window_was_spent(tmp_path, caplog):
    """Past the configured 8 h the row fails anyway: the INFO line is reserved
    for escalations the collapse actually caused."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(
            store,
            _cfg(low_watermark=15 * GIB),
            free=5 * GIB,
            arrivals=10,
            size_each=GIB,
        )
        row = _row(store, "q" * 40, WAIT + 600)
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            coord._park_for_indexer_retry(row, reason="indexer miss")
        assert [r for r in caplog.records if "configured window alone" in r.getMessage()] == []
        assert store.get("q" * 40).state == State.FAILED
    finally:
        store.close()


def test_calm_failed_message_text_is_unchanged(tmp_path):
    """The plain-timeout wording survives untouched when nothing collapsed —
    what an operator sees on a quiet box is exactly what they saw before."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), free=100 * GIB, arrivals=1, size_each=GIB)
        coord._park_for_indexer_retry(_row(store, "r" * 40, WAIT + 600), reason="indexer miss")
        row = store.get("r" * 40)
        assert row.state == State.FAILED
        assert row.last_error == f"Prowlarr cross-seed not found within {WAIT}s"
    finally:
        store.close()
