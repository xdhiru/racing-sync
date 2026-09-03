from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from racing_sync.config import AppConfig, ProwlarrConfig, WatchDirConfig, GeneralConfig, DestConfig, SSDConfig, RcloneConfig
from racing_sync.watchdir import WatchDirScanner, WatchItem, _bencode, _bencoded_info_hash, parse_torrent_file
from racing_sync.coordinator import Coordinator
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.prowlarr import TorrentHit, Indexer
from racing_sync.clients.abstract import AddResult


def _create_sample_torrent_data(name: str = "Test.Movie.1080p", length: int = 1000, announce: str = "http://seedpool.net/announce", piece_length: int = 16384) -> bytes:
    torrent_dict = {
        b"announce": announce.encode("utf-8"),
        b"info": {
            b"name": name.encode("utf-8"),
            b"length": length,
            b"piece length": piece_length,
            b"pieces": b"12345678901234567890",
        },
    }
    return _bencode(torrent_dict)


def test_bencoded_info_hash_and_announce():
    data = _create_sample_torrent_data("Ubuntu.iso", 2000, "https://torrent.ubuntu.com/announce")
    infohash, name, total, announce = _bencoded_info_hash(data)
    assert name == "Ubuntu.iso"
    assert total == 2000
    assert announce == "https://torrent.ubuntu.com/announce"
    assert len(infohash) == 40


def test_prowlarr_config_is_download_indexer():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
        download_indexer_substrings=["seedpool", "publicbt"],
    )
    assert cfg.is_download_indexer("https://tracker.seedpool.org/announce/1234") is True
    assert cfg.is_download_indexer("http://publicbt.com/announce") is True
    assert cfg.is_download_indexer("https://aither.cc/announce/1234") is False
    assert cfg.is_download_indexer("") is False


@pytest.mark.anyio
async def test_watchdir_scanner_scan_once(tmp_path: Path):
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    tfile = watch_dir / "test.torrent"
    raw_data = _create_sample_torrent_data("Sample.Release", 5000, "http://tracker.example.com/announce")
    tfile.write_bytes(raw_data)

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)

    items = await scanner.scan_once()
    assert len(items) == 1
    item = items[0]
    assert item.name == "Sample.Release"
    assert item.size_bytes == 5000
    assert item.announce_url == "http://tracker.example.com/announce"
    assert item.torrent_bytes == raw_data

    # Second scan returns nothing since it's already seen
    items2 = await scanner.scan_once()
    assert len(items2) == 0


@pytest.mark.anyio
async def test_watchdir_pickup_in_tick(tmp_path: Path):
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    tfile = watch_dir / "sample.torrent"
    raw_data = _create_sample_torrent_data("My.Release", 10000, "http://seedpool.org/announce")
    tfile.write_bytes(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord.cfg.general.source_poll_interval = 30
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.cfg.watch_dir = WatchDirConfig(path=watch_dir, delete_after_pickup=True)
    coord.store = store
    coord._running_infohashes = set()
    coord._tasks = set()
    coord._live = {}
    coord.watch = WatchDirScanner(coord.cfg.watch_dir, prowlarr=None)
    coord._list_source_torrents = AsyncMock(return_value=[])
    coord.store.list_seedpool_ready = MagicMock(return_value=[])
    coord.store.all_active = MagicMock(return_value=[])

    # Run one tick
    await coord._tick()

    # The torrent should be in state store as NEW and watch-dir
    rows = store.all()
    assert len(rows) == 1
    ts = rows[0]
    assert ts.source_name == "My.Release"
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data
    assert ts.source_announce_url == "http://seedpool.org/announce"

    # File should have been deleted after pickup
    assert not tfile.exists()


@pytest.mark.anyio
async def test_do_new_watch_dir_already_download_tracker(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Seedpool.Content", 5000, "http://seedpool.net/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: "seedpool" in url
    coord.prowlarr = MagicMock()
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

    # Should NOT search for a download indexer because it already comes from seedpool
    coord.prowlarr.get_download_indexer.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.state == State.QUEUED


@pytest.mark.anyio
async def test_do_new_watch_dir_already_download_tracker_still_searches_other_cross_seeds(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Seedpool.Movie.1080p", 5000, "http://seedpool.net/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: "seedpool" in url
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s: setattr(t, "state", s)

    bhd_idx = Indexer(2, "BeyondHD", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexer = MagicMock()
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "BeyondHD" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    bhd_hit = TorrentHit(
        title="Seedpool.Movie.1080p",
        guid="2",
        indexer="BeyondHD",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "beyondhd": [bhd_hit],
    }

    bhd_torrent_bytes = _create_sample_torrent_data("Seedpool.Movie.1080p", 5000, "http://beyond-hd.me/announce", piece_length=32768)
    coord.prowlarr.download_torrent.return_value = bhd_torrent_bytes

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

    # download_indexer should NOT be queried
    coord.prowlarr.get_download_indexer.assert_not_called()
    # But BeyondHD was searched!
    coord.prowlarr.search_indexers_parallel.assert_called_once()
    # Dropped torrent used for SSD
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data

    # BeyondHD was saved as a cross-seed for FUSE
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 2  # dropped seedpool torrent + BeyondHD cross-seed


@pytest.mark.anyio
async def test_do_new_watch_dir_public_torrent_skips_sacrificial_copy(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Public.Movie.1080p", 5000, "http://tracker.opentrackr.org:1337/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: "seedpool" in url
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s: setattr(t, "state", s)

    bhd_idx = Indexer(2, "BeyondHD", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexer = MagicMock()
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "BeyondHD" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    bhd_hit = TorrentHit(
        title="Public.Movie.1080p",
        guid="2",
        indexer="BeyondHD",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "beyondhd": [bhd_hit],
    }

    bhd_torrent_bytes = _create_sample_torrent_data("Public.Movie.1080p", 5000, "http://beyond-hd.me/announce", piece_length=32768)
    coord.prowlarr.download_torrent.return_value = bhd_torrent_bytes

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

    # download_indexer should NOT be queried for a sacrificial copy
    coord.prowlarr.get_download_indexer.assert_not_called()
    # But BeyondHD was searched for cross-seeds!
    coord.prowlarr.search_indexers_parallel.assert_called_once()
    # Dropped public torrent used directly for SSD
    assert ts.cross_seed_source == "public-watch-dir"
    assert ts.cross_seed_blob == raw_data

    # BeyondHD cross-seed was saved for FUSE injection
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 2  # dropped public torrent + BeyondHD cross-seed


@pytest.mark.anyio
async def test_do_new_watch_dir_with_prowlarr_search_and_cross_seeds(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://aither.cc/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: "seedpool" in url
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s: setattr(t, "state", s)

    dl_idx = Indexer(1, "Seedpool (API)", "torrent", True, [])
    bhd_idx = Indexer(2, "BeyondHD", "torrent", True, [])

    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexer = MagicMock(return_value=dl_idx)
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "BeyondHD" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    # Mock Prowlarr search results
    dl_hit = TorrentHit(
        title="Private.Movie.1080p",
        guid="1",
        indexer="Seedpool (API)",
        indexer_id=1,
        size_bytes=5000,
        download_url="http://prowlarr/dl/1",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    bhd_hit = TorrentHit(
        title="Private.Movie.1080p",
        guid="2",
        indexer="BeyondHD",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    coord.prowlarr.search_indexers_parallel.return_value = {
        "seedpool (api)": [dl_hit],
        "beyondhd": [bhd_hit],
    }

    # Mock downloads from Prowlarr
    dl_torrent_bytes = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://seedpool.net/announce")
    bhd_torrent_bytes = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://beyond-hd.me/announce", piece_length=32768)

    async def mock_dl(hit):
        if hit.indexer_id == 1:
            return dl_torrent_bytes
        return bhd_torrent_bytes

    coord.prowlarr.download_torrent.side_effect = mock_dl

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

    # Chosen download torrent should be Seedpool
    assert ts.cross_seed_source == "public-prowlarr"
    assert ts.cross_seed_blob == dl_torrent_bytes
    assert ts.state == State.QUEUED

    # Cross seed directory should have original dropped torrent AND BeyondHD torrent
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    assert watch_cross_dir.exists()
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 2  # Original dropped + BeyondHD


@pytest.mark.anyio
async def test_re_inject_watch_dir_torrents(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.general.state_db = db_path
    coord.store = store
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent.return_value = AddResult(hash="h1", accepted=True)

    infohash = "1111222233334444555566667777888899990000"
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    watch_cross_dir.mkdir(parents=True)

    # Add two torrents to the cross seed directory
    t1 = _create_sample_torrent_data("Movie.Part1", 2000, "http://tracker1/announce")
    t2 = _create_sample_torrent_data("Movie.Part2", 2000, "http://tracker2/announce")
    (watch_cross_dir / "t1.torrent").write_bytes(t1)
    (watch_cross_dir / "t2.torrent").write_bytes(t2)

    ts = TorrentState(
        source_infohash=infohash,
        source_name="Movie",
        cross_seed_source="watch-dir",
        state=State.RE_ADDING,
    )

    await coord._re_inject_watch_dir_torrents(ts)

    # Both torrents must have been added to dest_client
    assert coord.dest_client.add_torrent.await_count == 2
    injected = ts.injected_private_hashes.split(",")
    assert len(injected) == 2
