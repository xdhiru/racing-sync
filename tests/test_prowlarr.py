from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from racing_sync.config import ProwlarrConfig, AppConfig
from racing_sync.prowlarr import ProwlarrClient, Indexer, TorrentHit
from racing_sync.coordinator import pick_ssd_source_for_racing, Coordinator
from racing_sync.state import StateStore, TorrentState, State
from racing_sync.clients.abstract import Torrent
from racing_sync.watchdir import _bencode, _bencoded_info_hash


def test_prowlarr_config_should_skip_title():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
        skip_query_substrings=["subsplease", "erasergroups"],
    )
    assert cfg.should_skip_title("[SubsPlease] One Piece - 1100 (1080p)") is True
    assert cfg.should_skip_title("Some.Show.S01E01.EraserGroups.720p") is True
    assert cfg.should_skip_title("SUBSPLEASE.movie") is True
    assert cfg.should_skip_title("The.Matrix.1999.1080p") is False
    assert cfg.should_skip_title("") is False


@pytest.mark.anyio
async def test_prowlarr_client_skips_search():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
        skip_query_substrings=["subsplease"],
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Seedpool (API)", "torrent", True, [])

    # search_indexer
    hits = await client.search_indexer(idx, "[SubsPlease] Naruto - 01")
    assert hits == []

    # best_match
    match = await client.best_match("[SubsPlease] Bleach - 01")
    assert match is None

    # search_indexers_parallel
    results = await client.search_indexers_parallel([idx], "[SubsPlease] Gintama - 01")
    assert results == {}


@pytest.mark.anyio
async def test_pick_ssd_source_for_racing_skips_prowlarr():
    cfg = MagicMock()
    cfg.prowlarr.enabled = True
    cfg.prowlarr.should_skip_title = lambda title: "subsplease" in title.lower()
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.refetch_public_via_prowlarr = True
    cfg.cross_seed.allow_ssh_export = True

    prowlarr = AsyncMock()
    source_client = AsyncMock()
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=b"sftp-torrent-bytes")

    torrent = Torrent(
        hash="hash123",
        name="[SubsPlease] Frieren - 28 (1080p)",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="",
        progress=1.0,
        trackers=["http://privatetracker.org/announce"],
    )

    decision = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=torrent,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=sftp,
        source_client=source_client,
        attempt_prowlarr=True,
    )

    # Prowlarr should NOT be called at all
    prowlarr.best_match.assert_not_called()
    assert decision is None


@pytest.mark.anyio
async def test_do_new_watch_dir_skips_prowlarr(tmp_path: Path):
    raw_data = _bencode({
        b"announce": b"http://privatetracker.org/announce",
        b"info": {
            b"name": b"[SubsPlease] Attack on Titan - 01",
            b"length": 5000,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.should_skip_title = lambda title: "subsplease" in title.lower()
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.prowlarr = AsyncMock()
    coord.store = store
    coord.transition = lambda t, s: setattr(t, "state", s)

    ts = TorrentState(
        source_infohash=infohash,
        source_name=name,
        total_bytes=total,
        source_announce_url=announce,
        cross_seed_blob=raw_data,
        cross_seed_source="watch-dir",
        state=State.NEW,
    )

    await coord._do_new_watch_dir(ts)

    # Prowlarr search should NOT be attempted
    coord.prowlarr.search_indexers_parallel.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.state == State.QUEUED
