"""Local acquisition: one box, the racing copy is the staged copy.

No indexer query and no second download. A complete entry on the client is
adopted as the SSD source; a missing or unfinished one parks on the retry
clock instead of falling through to the pick.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.chain import Chain
from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig
from racing_sync.config_summary import summarize
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode

H = "e" * 40
TOP = "Local.Copy.Movie.2024"
SIZE = 500


def _blob() -> bytes:
    return _bencode({b"info": {b"name": TOP.encode(), b"length": SIZE}})


def _config(tmp_path: Path, *, source_host="http://box:8080", dest_host="http://box:8080"):
    text = f"""
[general]

[client]
type = "qbittorrent"
host = "{source_host}"

[dest]
host = "{dest_host}"

[storage]
path = "/ssd"
save_path = "/ssd"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[acquire]
provider = "local"

[remote]
path = "remote:movies/"
unsorted = "remote:unsorted/"

[seed]
path = "/mnt/fuse"
"""
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def _entry(progress: float = 1.0) -> Torrent:
    return Torrent(
        hash=H,
        name=TOP,
        category="racing",
        save_path="/ssd",
        size_bytes=SIZE,
        state="seeding" if progress >= 1.0 else "downloading",
        progress=progress,
        trackers=[],
    )


def _scaffold(tmp_path: Path, *, entries, adopt=True, state=State.NEW):
    cfg_path = _config(tmp_path)
    cfg = AppConfig.from_toml(cfg_path)
    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store, cfg=cfg)
    coord._schedule_telegram_update = MagicMock()
    coord.transition = lambda ts, dst, error="", batch_index=None: store.transition(
        ts, dst, error=error, batch_index=batch_index
    )
    snap = _entry(1.0)
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.source_client.get_torrent = AsyncMock(return_value=snap)
    coord._source_torrents_cache = [snap]
    coord.dest_client = AsyncMock()
    coord._dest_entries_for = AsyncMock(return_value=list(entries))
    coord._adopt_ssd_entry = AsyncMock(return_value=adopt)
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()

    ts = TorrentState(
        source_infohash=H,
        source_name=TOP,
        classification_kind="movie",
        total_bytes=SIZE,
        cross_seed_blob=_blob(),
        state=state,
    )
    store.upsert(ts)
    return coord, store, cfg_path


def test_local_chain_adopts_the_client_copy(tmp_path: Path):
    cfg = AppConfig.from_toml(_config(tmp_path))
    chain = Chain.from_config(cfg)

    assert chain.acquire == "local"
    assert chain.adopts_local is True
    assert chain.stage is True
    assert chain.downloads is False


def test_local_needs_one_client(tmp_path: Path):
    with pytest.raises(ValueError, match="same client"):
        AppConfig.from_toml(_config(tmp_path, dest_host="http://other:8080"))


def test_local_summary_names_the_client_copy(tmp_path: Path):
    path = _config(tmp_path)
    shape = summarize(AppConfig.from_toml(path), path)["shape"]

    assert shape["acquire"] == ["local_client"]


@pytest.mark.anyio
async def test_complete_racing_copy_is_adopted(tmp_path):
    coord, store, _ = _scaffold(tmp_path, entries=[_entry(1.0)])
    try:
        await coord._do_new(store.get(H))

        coord._adopt_ssd_entry.assert_awaited_once()
        coord._pick_and_admit.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_missing_copy_parks_without_picking(tmp_path):
    coord, store, _ = _scaffold(tmp_path, entries=[])
    try:
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_next_retry_at is not None
    finally:
        store.close()


@pytest.mark.anyio
async def test_unfinished_copy_waits_instead_of_duplicating(tmp_path):
    coord, store, _ = _scaffold(tmp_path, entries=[_entry(0.4)])
    try:
        await coord._do_new(store.get(H))

        coord._adopt_ssd_entry.assert_not_called()
        coord._pick_and_admit.assert_not_called()
        assert store.get(H).state == State.WAITING_INDEXER
    finally:
        store.close()


@pytest.mark.anyio
async def test_wake_keeps_waiting_for_the_racing_copy(tmp_path):
    coord, store, _ = _scaffold(tmp_path, entries=[_entry(0.4)], state=State.WAITING_INDEXER)
    try:
        await coord._do_waiting_indexer(store.get(H))

        coord._pick_and_admit.assert_not_called()
        coord._adopt_ssd_entry.assert_not_called()
        assert store.get(H).state == State.WAITING_INDEXER
    finally:
        store.close()
