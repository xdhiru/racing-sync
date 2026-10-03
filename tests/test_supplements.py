"""tracker_map supplementary fuse cross-seeds for racing rows.

VPS1 is not the whole world: tracker_map indexers may hold the same
release under a different infohash. The supplement harvest injects
those to fuse (per-blob gate, skip_check only onto verified bytes),
memoized per row. Empty map / no prowlarr = no-op.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.prowlarr import TorrentHit
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode, _bencoded_info_hash

NAME = "Show.S01E01.1080p"
SIZE = 500


def _blob(fname=NAME, size=SIZE):
    return _bencode({
        b"info": {b"name": fname.encode(), b"length": size},
    })


def _hit(title=NAME, size=SIZE, indexer="PrivIdx"):
    return TorrentHit(
        title=title, guid="g", indexer=indexer, indexer_id=7,
        size_bytes=size, download_url="http://x/y.torrent",
        magnet_url="", info_url="", publish_date="",
    )


def _idx(name="PrivIdx"):
    return SimpleNamespace(name=name, enable=True)


def _coord(tmp_path, *, entries=None, skip_title=False):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd.mkdir(exist_ok=True)
    fuse.mkdir(exist_ok=True)
    fuse_u.mkdir(exist_ok=True)
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse_u
    coord.cfg.classifier._episode_re = None
    coord.cfg.source.category = ""
    coord.cfg.source.min_age_seconds = 0
    coord.cfg.prowlarr.tracker_map.entries = (
        {"tk": "PrivIdx"} if entries is None else dict(entries))
    coord.cfg.prowlarr.should_skip_title = MagicMock(return_value=skip_title)
    # NOTE: NAME matches the episode regex, so blob kind is "episode" and
    # the fuse target is mount_unsorted (same routing the app uses).
    coord.store = StateStore(tmp_path / "state.db")
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.sftp = None
    coord.dest_client = AsyncMock()
    coord.prowlarr = AsyncMock()
    coord.prowlarr.get_indexer_by_name = MagicMock(
        side_effect=lambda n: _idx(n) if n == "PrivIdx" else None)
    coord._live = {}
    return coord, ssd, fuse, fuse_u


def _row(**kw):
    base = dict(
        source_infohash="e" * 40, source_name=NAME, total_bytes=SIZE,
        save_path="", classification_kind="episode",
        injected_private_hashes="", state=State.RE_ADDING,
    )
    base.update(kw)
    return TorrentState(**base)


@pytest.mark.anyio
async def test_no_prowlarr_noop(tmp_path):
    coord, _, _, _ = _coord(tmp_path)
    try:
        coord.prowlarr = None
        ts = _row()
        coord.store.upsert(ts)
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        assert coord.store.get("e" * 40).injected_private_hashes == ""
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_empty_map_noop(tmp_path):
    coord, _, _, _ = _coord(tmp_path, entries={})
    try:
        ts = _row()
        coord.store.upsert(ts)
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        coord.prowlarr.search_indexers_parallel.assert_not_called()
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_skip_title_noop(tmp_path):
    coord, _, _, _ = _coord(tmp_path, skip_title=True)
    try:
        ts = _row()
        coord.store.upsert(ts)
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        coord.prowlarr.search_indexers_parallel.assert_not_called()
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_hit_injected_and_recorded(tmp_path):
    coord, _, _, fuse_u = _coord(tmp_path)
    blob = _blob()
    blob_hash, _, _, _ = _bencoded_info_hash(blob)
    coord.prowlarr.search_indexers_parallel = AsyncMock(
        return_value={"PrivIdx": [_hit()]})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    (fuse_u / NAME).write_bytes(b"x" * SIZE)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "ok"))
    try:
        coord.store.upsert(_row())
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        assert blob_hash in coord.store.get("e" * 40).injected_private_hashes
        coord._ensure_fuse_entry.assert_awaited_once()
        _kw = coord._ensure_fuse_entry.await_args[1]
        assert _kw["infohash"] == blob_hash
        assert _kw["label"] == "tracker_map supplement"
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_non_matching_hit_skipped(tmp_path):
    coord, _, _, fuse_u = _coord(tmp_path)
    blob = _blob()
    coord.prowlarr.search_indexers_parallel = AsyncMock(
        return_value={"PrivIdx": [_hit(title="Other.Show.S01E01")]})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "ok"))
    try:
        coord.store.upsert(_row())
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        coord._ensure_fuse_entry.assert_not_called()
        coord.prowlarr.download_torrent.assert_not_called()
        assert coord.store.get("e" * 40).injected_private_hashes == ""
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_fuse_missing_skipped_real_gate(tmp_path):
    coord, _, _, _ = _coord(tmp_path)
    blob = _blob()
    coord.prowlarr.search_indexers_parallel = AsyncMock(
        return_value={"PrivIdx": [_hit()]})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "ok"))
    try:
        coord.store.upsert(_row())
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        # Bytes never reached the remote: no blind inject.
        coord._ensure_fuse_entry.assert_not_called()
        assert coord.store.get("e" * 40).injected_private_hashes == ""
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_known_hash_skipped(tmp_path):
    coord, _, _, fuse_u = _coord(tmp_path)
    blob = _blob()
    blob_hash, _, _, _ = _bencoded_info_hash(blob)
    coord.prowlarr.search_indexers_parallel = AsyncMock(
        return_value={"PrivIdx": [_hit()]})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    (fuse_u / NAME).write_bytes(b"x" * SIZE)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "ok"))
    try:
        coord.store.upsert(_row(cross_seed_infohash=blob_hash))
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        coord._ensure_fuse_entry.assert_not_called()
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_memo_throttles_repeat_search(tmp_path):
    coord, _, _, fuse_u = _coord(tmp_path)
    blob = _blob()
    coord.prowlarr.search_indexers_parallel = AsyncMock(return_value={})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    (fuse_u / NAME).write_bytes(b"x" * SIZE)
    try:
        coord.store.upsert(_row())
        ts = coord.store.get("e" * 40)
        await coord._re_inject_tracker_map_supplements(ts)
        await coord._re_inject_tracker_map_supplements(
            coord.store.get("e" * 40))
        assert coord.prowlarr.search_indexers_parallel.await_count == 1
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_rides_re_inject_racing_torrents(tmp_path):
    """Supplements fire as part of the normal re-inject step (all 3 sites)."""
    coord, _, _, fuse_u = _coord(tmp_path)
    blob = _blob()
    blob_hash, _, _, _ = _bencoded_info_hash(blob)
    coord.prowlarr.search_indexers_parallel = AsyncMock(
        return_value={"PrivIdx": [_hit()]})
    coord.prowlarr.download_torrent = AsyncMock(return_value=blob)
    (fuse_u / NAME).write_bytes(b"x" * SIZE)
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "ok"))
    coord.transition = lambda t, s, error="": setattr(t, "state", s)
    try:
        coord.store.upsert(_row())
        await coord._re_inject_racing_torrents(coord.store.get("e" * 40))
        assert blob_hash in coord.store.get("e" * 40).injected_private_hashes
    finally:
        coord.store.close()



