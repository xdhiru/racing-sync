"""P1 wiring: MOVING cards show live rclone lines; progress resets parks."""

from __future__ import annotations

import time

from racing_sync.rclone_ops import RcloneProgress
from racing_sync.state import State, TorrentState


def _moving_row(h="a" * 40):
    return TorrentState(
        source_infohash=h,
        source_name="Big.Show.S01",
        state=State.MOVING,
        total_bytes=10_000_000_000,
        source_announce_url="https://alpha.cc/announce/xyz",
    )


def _fresh(**kw):
    args = dict(
        transferred_bytes=4_200_000_000,
        total_bytes=10_000_000_000,
        percent=42.0,
        speed_bps=80_000_000,
        eta_seconds=72,
        updated_at=time.monotonic(),
    )
    args.update(kw)
    return RcloneProgress(**args)


def test_moving_card_shows_live_line():
    from racing_sync.telegram_views import render_active

    h = "a" * 40
    text, _, _ = render_active(
        [(_moving_row(h), None)], page=0, page_size=5, move_progress={h: _fresh()}
    )
    assert "📦 Moving · 42%" in text
    assert "▓" in text or "█" in text
    assert "76.3M/s" in text
    assert "ETA 72s" in text


def test_moving_card_plain_without_fresh_progress():
    from racing_sync.telegram_views import render_active

    h = "a" * 40
    # Stale entry: plain badge, no fossilized percentage.
    text, _, _ = render_active(
        [(_moving_row(h), None)],
        page=0,
        page_size=5,
        move_progress={h: _fresh(updated_at=time.monotonic() - 600.0)},
    )
    assert "📦 Moving" in text
    assert "42%" not in text
    # No entry at all: same plain badge.
    text2, _, _ = render_active([(_moving_row(h), None)])
    assert "📦 Moving" in text2 and "%" not in text2.split("📦 Moving")[1].split("\n")[0]


def test_park_counter_resets_on_transferred_growth():
    """A pumping move restarts the stall clock instead of escalating."""
    from unittest.mock import MagicMock

    from conftest import make_coordinator

    from racing_sync import rclone_ops

    h = "a" * 40
    store = MagicMock()
    store.update_columns.return_value = 2
    coord = make_coordinator(store)
    coord._moving_parks = {h: 4}
    coord._moving_tree = {h: "tree-key"}
    coord._moving_progress_bytes = {h: 100}
    try:
        rclone_ops._RCLONE_PROGRESS["tree-key"] = _fresh(transferred_bytes=200)
        coord._park_moving(_moving_row(h), "busy", quiet=True)
        assert coord._moving_parks[h] == 1
        # Same bytes next tick: no advance, counter climbs again.
        coord._park_moving(_moving_row(h), "busy", quiet=True)
        assert coord._moving_parks[h] == 2
    finally:
        rclone_ops._RCLONE_PROGRESS.clear()


def test_move_progress_map_joins_row_to_tree():
    from unittest.mock import MagicMock

    from conftest import make_coordinator

    from racing_sync import rclone_ops

    coord = make_coordinator(MagicMock())
    coord._moving_tree = {"a" * 40: "tree-key", "b" * 40: "gone-tree"}
    try:
        prog = _fresh()
        rclone_ops._RCLONE_PROGRESS["tree-key"] = prog
        got = coord.rclone_move_progress_map()
        assert got == {"a" * 40: prog}
    finally:
        rclone_ops._RCLONE_PROGRESS.clear()


def test_transition_clears_progress_join_maps():
    from unittest.mock import MagicMock

    from conftest import make_coordinator

    coord = make_coordinator(MagicMock())
    h = "b" * 40
    ts = TorrentState(source_infohash=h, source_name="S", state=State.MOVING)
    coord._moving_parks = {h: 3}
    coord._moving_tree = {h: "t"}
    coord._moving_progress_bytes = {h: 9}
    coord.transition(ts, State.RE_ADDING, error="")
    assert h not in coord._moving_parks
    assert h not in coord._moving_tree
    assert h not in coord._moving_progress_bytes
