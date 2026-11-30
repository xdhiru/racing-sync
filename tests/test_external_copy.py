"""External acquisition: the operator copies the bytes; the daemon verifies.

No indexer search and no SSD download are allowed in this shape. A copy
that is absent keeps the row on the manual-move timer; when the window
expires the row fails with where the copy was expected instead of falling
back to a download it was never configured to run.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode

H = "b" * 40
TOP = "External.Copy.Movie.2024"
EP1 = f"{TOP}/part1.mkv"
EP2 = f"{TOP}/part2.mkv"
SIZE1 = 120
SIZE2 = 240


def _blob() -> bytes:
    return _bencode(
        {
            b"info": {
                b"name": TOP.encode(),
                b"files": [
                    {b"length": SIZE1, b"path": [b"part1.mkv"]},
                    {b"length": SIZE2, b"path": [b"part2.mkv"]},
                ],
            },
        }
    )


def _config(tmp_path: Path, *, timeout: int = 43200) -> AppConfig:
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd = tmp_path / "ssd"
    for d in (fuse, fuse_u, ssd):
        d.mkdir(parents=True, exist_ok=True)
    text = f"""
[general]
client_rpc_timeout_seconds = 5

[client]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8080"

[storage]
path = "{ssd.as_posix()}"
save_path = "{ssd.as_posix()}"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[acquire]
provider = "external"

[transfer]
provider = "external"

[seed]
path = "{fuse.as_posix()}"
unsorted_path = "{fuse_u.as_posix()}"
manual_move_check_seconds = 1800
manual_move_timeout_seconds = {timeout}
"""
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return AppConfig.from_toml(path)


def _scaffold(tmp_path: Path, *, state=State.NEW, fuse_files=(), stamp=None, timeout=43200):
    (tmp_path / "fuse").mkdir(exist_ok=True)
    cfg = _config(tmp_path, timeout=timeout)
    for rel, size in fuse_files:
        target = Path(cfg.rclone.fuse.mount) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x" * size)

    blob = _blob()
    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store, cfg=cfg)
    coord._schedule_telegram_update = MagicMock()
    coord.transition = lambda ts, dst, error="", batch_index=None: store.transition(
        ts, dst, error=error, batch_index=batch_index
    )
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.source_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash=H,
            name=TOP,
            category="racing",
            save_path="/vps1",
            size_bytes=SIZE1 + SIZE2,
            state="seeding",
            progress=1.0,
            trackers=[],
        )
    )
    coord._source_torrents_cache = [coord.source_client.get_torrent.return_value]
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()

    ts = TorrentState(
        source_infohash=H,
        source_name=TOP,
        classification_kind="movie",
        total_bytes=SIZE1 + SIZE2,
        cross_seed_blob=blob,
        state=state,
        fuse_wait_first_at=stamp,
    )
    store.upsert(ts)
    return coord, store


@pytest.mark.anyio
async def test_absent_copy_waits_and_never_picks(tmp_path):
    coord, store = _scaffold(tmp_path)
    try:
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.WAITING_INDEXER
        assert row.fuse_wait_first_at is not None
        assert row.indexer_next_retry_at is not None
        assert "waiting for the manual move" in row.last_error
    finally:
        store.close()


@pytest.mark.anyio
async def test_present_copy_is_admitted_without_a_download(tmp_path):
    coord, store = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1), (EP2, SIZE2)])
    try:
        coord._admit_resolved = AsyncMock()
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_not_called()
        coord._admit_resolved.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_expired_window_fails_instead_of_downloading(tmp_path):
    start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=13)
    coord, store = _scaffold(tmp_path, state=State.WAITING_INDEXER, stamp=start, timeout=3600)
    try:
        await coord._do_waiting_indexer(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.FAILED
        assert row.last_error.startswith("external copy unavailable")
        assert "never appeared" in row.last_error
    finally:
        store.close()


@pytest.mark.anyio
async def test_expired_incomplete_copy_fails_with_the_detail(tmp_path):
    start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=13)
    coord, store = _scaffold(
        tmp_path,
        state=State.WAITING_INDEXER,
        fuse_files=[(EP1, SIZE1)],
        stamp=start,
        timeout=3600,
    )
    try:
        await coord._do_waiting_indexer(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.FAILED
        assert "incomplete" in row.last_error
    finally:
        store.close()
