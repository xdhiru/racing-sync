"""Shortened chains: the engine takes the derived edge and drains stragglers.

A download-only or move-only deployment must never enter a stage that does
not exist. The natural handoffs ask `rt.chain`; startup drains rows left in
a link the new config no longer runs (before the SSD ledger is rebuilt).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.config import AppConfig
from racing_sync.state import State, StateStore, TorrentState

_DOWNLOAD_ONLY = """
[general]

[client]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8080"

[storage]
path = "/downloads"
save_path = "/downloads"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[transfer]
enabled = false

[seed]
enabled = false

[cleanup]
enabled = true
"""

H = "a" * 40


def _download_only(tmp_path: Path) -> AppConfig:
    path = tmp_path / "config.toml"
    path.write_text(_DOWNLOAD_ONLY, encoding="utf-8")
    return AppConfig.from_toml(path)


def _full() -> AppConfig:
    return AppConfig.from_toml(Path(__file__).parent / "fixtures" / "config" / "minimal.toml")


def _coord(store, cfg):
    coord = make_coordinator(store, cfg=cfg)
    coord._schedule_telegram_update = MagicMock()
    coord.transition = lambda ts, dst, error="", batch_index=None: store.transition(
        ts, dst, error=error, batch_index=batch_index
    )
    return coord


def _row(store, state: State, **kw) -> TorrentState:
    ts = TorrentState(
        source_infohash=kw.pop("hash", H), source_name="Short.Chain", state=state, **kw
    )
    store.upsert(ts)
    return ts


@pytest.mark.anyio
async def test_drain_finishes_re_adding_when_seed_is_off(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, State.RE_ADDING)
        coord = _coord(store, _download_only(tmp_path))

        await coord._drain_disabled_links()

        row = store.get(H)
        assert row.state == State.DONE
        assert row.last_error == ""
    finally:
        store.close()


@pytest.mark.anyio
async def test_drain_finishes_an_unmoved_moving_row_when_transfer_is_off(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, State.MOVING)
        coord = _coord(store, _download_only(tmp_path))

        await coord._drain_disabled_links()

        assert store.get(H).state == State.DONE
    finally:
        store.close()


@pytest.mark.anyio
async def test_drain_fails_a_partially_moved_row_instead_of_claiming_done(tmp_path):
    """A checkpoint means bytes may sit on both sides: fail for the operator."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, State.MOVING, move_stage="moved")
        coord = _coord(store, _download_only(tmp_path))

        await coord._drain_disabled_links()

        row = store.get(H)
        assert row.state == State.FAILED
        assert "transfer link disabled" in row.last_error
    finally:
        store.close()


@pytest.mark.anyio
async def test_full_chain_drains_nothing(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, State.RE_ADDING)
        coord = _coord(store, _full())

        await coord._drain_disabled_links()

        assert store.get(H).state == State.RE_ADDING
    finally:
        store.close()


@pytest.mark.anyio
async def test_move_completion_asks_the_chain(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row(store, State.MOVING, move_stage="wiped")
        coord = _coord(store, _download_only(tmp_path))
        coord._finish_moving_tracking = MagicMock()

        await coord._replay_move_checkpoint(ts, "wiped")

        assert store.get(H).state == State.DONE
    finally:
        store.close()


@pytest.mark.anyio
async def test_move_completion_re_adds_when_seed_is_on(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row(store, State.MOVING, move_stage="wiped")
        coord = _coord(store, _full())
        coord._finish_moving_tracking = MagicMock()

        await coord._replay_move_checkpoint(ts, "wiped")

        assert store.get(H).state == State.RE_ADDING
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_is_skipped_without_a_seed_link(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _download_only(tmp_path))
        coord.source_client = AsyncMock()

        await coord._maybe_cleanup_source()

        coord.source_client.list_torrents.assert_not_called()
        assert coord._cleanup_seed_skip_logged is True
    finally:
        store.close()
