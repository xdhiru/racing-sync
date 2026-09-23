from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from racing_sync.config import AppConfig, DownloadIndexerConfig, ProwlarrConfig, WatchDirConfig, GeneralConfig, DestConfig, SSDConfig, RcloneConfig
from racing_sync.watchdir import WatchDirScanner, WatchItem, _bencode, _bencoded_info_hash, parse_torrent_file
from conftest import make_coordinator
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.prowlarr import TorrentHit, Indexer
from racing_sync.clients.abstract import AddResult


def _create_sample_torrent_data(name: str = "Test.Movie.1080p", length: int = 1000, announce: str = "http://dl-indexer.example.net/announce", piece_length: int = 16384) -> bytes:
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
    data = _create_sample_torrent_data("Ubuntu.iso", 2000, "https://torrents\.example\.org/announce")
    infohash, name, total, announce = _bencoded_info_hash(data)
    assert name == "Ubuntu.iso"
    assert total == 2000
    assert announce == "https://torrents\.example\.org/announce"
    assert len(infohash) == 40


def test_prowlarr_config_is_download_indexer():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexers=[
            DownloadIndexerConfig(
                name="Test Indexer (API)",
                announce_substrings=["test-indexer", "publicbt"],
            ),
            DownloadIndexerConfig(
                name="Second Indexer",
                announce_substrings=["second"],
            ),
        ],
    )
    assert cfg.download_indexer_names == ["Test Indexer (API)", "Second Indexer"]
    assert cfg.is_download_indexer("https://tracker.test-indexer.example/announce/1234") is True
    assert cfg.is_download_indexer("http://publicbt.com/announce") is True
    assert cfg.is_download_indexer("https://second.example/announce") is True
    assert cfg.is_download_indexer("https://alpha.cc/announce/1234") is False
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
    raw_data = _create_sample_torrent_data("My.Release", 10000, "http://dl-indexer.example.org/announce")
    tfile.write_bytes(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord.cfg.general.source_poll_interval = 30
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.cfg.watch_dir = WatchDirConfig(path=watch_dir, delete_after_pickup=True)
    coord.store = store
    coord.watch = WatchDirScanner(coord.cfg.watch_dir, prowlarr=None)
    coord._list_source_torrents = AsyncMock(return_value=[])
    coord.store.list_indexer_ready = MagicMock(return_value=[])
    coord.store.all_active = MagicMock(return_value=[])

    # Run one tick
    await coord._tick()

    # The torrent should be in state store as NEW and watch-dir
    rows = store.all(include_blob=True)
    assert len(rows) == 1
    ts = rows[0]
    assert ts.source_name == "My.Release"
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data
    assert ts.source_announce_url == "http://dl-indexer.example.org/announce"

    # File should have been deleted after pickup
    assert not tfile.exists()


@pytest.mark.anyio
async def test_do_new_watch_dir_already_download_tracker(tmp_path: Path):
    raw_data = _create_sample_torrent_data("DLIndexer.Content", 5000, "http://dl-indexer.example.net/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in url
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.prowlarr = MagicMock()
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

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

    # Should NOT search the download-target indexers because the drop already comes from one
    coord.prowlarr.get_download_indexers.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.state == State.QUEUED


@pytest.mark.anyio
async def test_do_new_watch_dir_already_download_tracker_still_searches_other_cross_seeds(tmp_path: Path):
    raw_data = _create_sample_torrent_data("DLIndexer.Movie.1080p", 5000, "http://dl-indexer.example.net/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in url
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beta": "Beta"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    bhd_idx = Indexer(2, "Beta", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[])
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "Beta" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    bhd_hit = TorrentHit(
        title="DLIndexer.Movie.1080p",
        guid="2",
        indexer="Beta",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "beta": [bhd_hit],
    }

    bhd_torrent_bytes = _create_sample_torrent_data("DLIndexer.Movie.1080p", 5000, "http://beta.me/announce", piece_length=32768)
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

    # download-target indexers should NOT be queried
    coord.prowlarr.get_download_indexers.assert_not_called()
    # But Beta was searched!
    coord.prowlarr.search_indexers_parallel.assert_called_once()
    # Dropped torrent used for SSD
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data

    # Beta was saved as a cross-seed for FUSE
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 2  # dropped download-indexer torrent + Beta cross-seed


@pytest.mark.anyio
async def test_do_new_watch_dir_public_torrent_skips_sacrificial_copy(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Public.Movie.1080p", 5000, "http://tracker.opentrackr.org:1337/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in url
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beta": "Beta"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    bhd_idx = Indexer(2, "Beta", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[])
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "Beta" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    bhd_hit = TorrentHit(
        title="Public.Movie.1080p",
        guid="2",
        indexer="Beta",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "beta": [bhd_hit],
    }

    bhd_torrent_bytes = _create_sample_torrent_data("Public.Movie.1080p", 5000, "http://beta.me/announce", piece_length=32768)
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

    # download-target indexers should NOT be queried for a sacrificial copy
    coord.prowlarr.get_download_indexers.assert_not_called()
    # But Beta was searched for cross-seeds!
    coord.prowlarr.search_indexers_parallel.assert_called_once()
    # Dropped public torrent used directly for SSD
    assert ts.cross_seed_source == "public-watch-dir"
    assert ts.cross_seed_blob == raw_data

    # Beta cross-seed was saved for FUSE injection
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 2  # dropped public torrent + Beta cross-seed


@pytest.mark.anyio
async def test_do_new_watch_dir_with_prowlarr_search_and_cross_seeds(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://alpha.cc/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in url
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beta": "Beta"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    dl_idx = Indexer(1, "Test Indexer (API)", "torrent", True, [])
    bhd_idx = Indexer(2, "Beta", "torrent", True, [])

    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[dl_idx])
    coord.prowlarr.get_indexer_by_name = MagicMock(side_effect=lambda n: bhd_idx if n == "Beta" else None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    # Mock Prowlarr search results
    dl_hit = TorrentHit(
        title="Private.Movie.1080p",
        guid="1",
        indexer="Test Indexer (API)",
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
        indexer="Beta",
        indexer_id=2,
        size_bytes=5000,
        download_url="http://prowlarr/dl/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    coord.prowlarr.search_indexers_parallel.return_value = {
        "test indexer (api)": [dl_hit],
        "beta": [bhd_hit],
    }

    # Mock downloads from Prowlarr (distinct piece lengths → distinct
    # infohashes for the same files, like real cross-tracker variants)
    dl_torrent_bytes = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://dl-indexer.example.net/announce", piece_length=32768)
    bhd_torrent_bytes = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://beta.me/announce", piece_length=65536)

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

    # Chosen download torrent should be the download-target indexer hit
    assert ts.cross_seed_source == "public-prowlarr"
    assert ts.cross_seed_blob == dl_torrent_bytes
    assert ts.state == State.QUEUED

    # Cross seed directory should have original dropped torrent, the
    # sacrificial download-torrent copy AND Beta torrent
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    assert watch_cross_dir.exists()
    saved_files = list(watch_cross_dir.glob("*.torrent"))
    assert len(saved_files) == 3  # Original dropped + sacrificial + Beta


@pytest.mark.anyio
async def test_re_inject_watch_dir_torrents(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.general.state_db = db_path
    coord.store = store
    # Fuse target holds the moved content (single-file torrents land by name).
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Movie.Part1").write_bytes(b"x" * 2000)
    (fuse_dir / "Movie.Part2").write_bytes(b"x" * 2000)
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent.return_value = AddResult(hash="h1", accepted=True)
    coord.dest_client.get_torrent = AsyncMock(return_value=MagicMock(save_path=str(fuse_dir)))

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


@pytest.mark.anyio
async def test_re_inject_watch_dir_torrents_skips_missing_fuse_content(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.general.state_db = db_path
    coord.store = store
    # Fuse target is empty: content was never moved -> no blind injection.
    fuse_dir = tmp_path / "fuse-empty"
    fuse_dir.mkdir()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord.dest_client = AsyncMock()
    coord.dest_client.add_torrent.return_value = AddResult(hash="h1", accepted=True)

    infohash = "2222333344445555666677778888999900001111"
    watch_cross_dir = tmp_path / "watch_cross_seeds" / infohash
    watch_cross_dir.mkdir(parents=True)

    t1 = _create_sample_torrent_data("Movie.Part1", 2000, "http://tracker1/announce")
    (watch_cross_dir / "t1.torrent").write_bytes(t1)

    ts = TorrentState(
        source_infohash=infohash,
        source_name="Movie",
        cross_seed_source="watch-dir",
        state=State.RE_ADDING,
    )

    await coord._re_inject_watch_dir_torrents(ts)

    coord.dest_client.add_torrent.assert_not_called()
    assert ts.injected_private_hashes == ""


@pytest.mark.anyio
async def test_do_queued_fuse_fast_path_injects_dropped_watch_blob(tmp_path: Path):
    """QUEUED→DONE fast path must seed the dropped watch copy, not skip it.

    Regression: the fast path only re-injected VPS1 racing torrents, so a
    manually dropped torrent whose content already seeded from fuse was
    marked DONE without ever being injected.
    """
    from racing_sync.clients.abstract import AddResult, Torrent, TorrentFile

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Fast.Movie.1080p.mkv").write_bytes(b"m" * 2000)

    dropped = _create_sample_torrent_data(
        "Fast.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    drop_hash, _, _, _ = _bencoded_info_hash(dropped)
    cross = _create_sample_torrent_data(
        "Fast.Movie.1080p.mkv", 2000, "http://dl-indexer.example.net/announce",
        piece_length=32768)
    cross_hash, _, _, _ = _bencoded_info_hash(cross)
    assert cross_hash != drop_hash
    watch_cross_dir = tmp_path / "watch_cross_seeds" / drop_hash
    watch_cross_dir.mkdir(parents=True)
    (watch_cross_dir / f"{drop_hash}.torrent").write_bytes(dropped)

    from racing_sync.config import ClassifierConfig

    coord = make_coordinator()
    coord.cfg.general.state_db = db_path
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    existing = Torrent(
        hash=cross_hash, name="Fast.Movie.1080p.mkv", category="racing",
        save_path=str(fuse_dir), size_bytes=2000, state="seeding", progress=1.0,
    )
    injected_entry = Torrent(
        hash=drop_hash, name="Fast.Movie.1080p.mkv", category="racing",
        save_path=str(fuse_dir), size_bytes=2000, state="seeding", progress=1.0,
    )
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[existing])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Fast.Movie.1080p.mkv", size_bytes=2000, progress=1.0),
    ])
    coord.dest_client.get_torrent = AsyncMock(return_value=injected_entry)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=drop_hash, accepted=True, detail=""))

    ts = TorrentState(
        source_infohash=drop_hash,
        source_name="Fast.Movie.1080p.mkv",
        total_bytes=2000,
        cross_seed_infohash=cross_hash,
        # Sacrificial flavour: the SSD leg used the Prowlarr copy, but the
        # row is still watch-origin — the dropped copy must be injected.
        cross_seed_source="public-prowlarr",
        cross_seed_blob=cross,
        classification_kind="movie",
        state=State.QUEUED,
    )
    ts._blob = cross
    try:
        assert coord._is_watch_row(ts) is True
        await coord._do_queued(ts)
        assert ts.state == State.DONE
        # The dropped copy was injected to fuse (skip_check seeding).
        assert coord.dest_client.add_torrent.await_count == 1
        sent = coord.dest_client.add_torrent.call_args.kwargs["torrent_files"]
        assert sent == [dropped]
        assert drop_hash in ts.injected_private_hashes.split(",")
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_queued_fuse_fast_path_stays_queued_on_transient_webui(tmp_path: Path):
    """Transient dest errors during fast-path watch injection park QUEUED."""
    from racing_sync.clients.abstract import Torrent, TorrentFile
    from racing_sync.coordinator_errors import WebUIUnresponsiveError

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Slow.Movie.1080p.mkv").write_bytes(b"m" * 2000)

    dropped = _create_sample_torrent_data(
        "Slow.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    drop_hash, _, _, _ = _bencoded_info_hash(dropped)
    watch_cross_dir = tmp_path / "watch_cross_seeds" / drop_hash
    watch_cross_dir.mkdir(parents=True)
    (watch_cross_dir / f"{drop_hash}.torrent").write_bytes(dropped)

    from racing_sync.config import ClassifierConfig

    coord = make_coordinator()
    coord.cfg.general.state_db = db_path
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    ts = TorrentState(
        source_infohash=drop_hash,
        source_name="Slow.Movie.1080p.mkv",
        total_bytes=2000,
        cross_seed_source="watch-dir",
        cross_seed_blob=dropped,
        classification_kind="movie",
        state=State.QUEUED,
    )
    ts._blob = dropped

    existing = Torrent(
        hash="e" * 40, name="Slow.Movie.1080p.mkv", category="racing",
        save_path=str(fuse_dir), size_bytes=2000, state="seeding", progress=1.0,
    )
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[existing])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Slow.Movie.1080p.mkv", size_bytes=2000, progress=1.0),
    ])
    coord.dest_client.add_torrent = AsyncMock(
        side_effect=WebUIUnresponsiveError("busy"))

    try:
        await coord._do_queued(ts)
        assert ts.state == State.QUEUED
    finally:
        store.close()


@pytest.mark.anyio
async def test_watchdir_scanner_caches_by_mtime_and_size(tmp_path: Path):
    from unittest.mock import patch
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    tfile = watch_dir / "sample.torrent"
    raw_data = _create_sample_torrent_data("Cached.Release", 4000)
    tfile.write_bytes(raw_data)

    empty_file = watch_dir / "empty.torrent"
    empty_file.write_bytes(b"")

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)

    # First scan: picks up sample.torrent, skips 0-byte empty.torrent
    items = await scanner.scan_once()
    assert len(items) == 1
    assert items[0].name == "Cached.Release"
    assert tfile in scanner._file_cache

    # Second scan: file_cache should be used without calling parse_torrent_file again
    with patch("racing_sync.watchdir.parse_torrent_file") as mock_parse:
        items2 = await scanner.scan_once()
        assert len(items2) == 0  # already seen
        mock_parse.assert_not_called()


def test_bencoded_info_hash_preserves_raw_unsorted_info_bytes():
    import hashlib
    # Construct an info dict with keys intentionally NOT sorted according to bencode standard:
    # 'zeta' before 'alpha'
    raw_info = b"d4:zetai100e5:alphai200ee"
    raw_torrent = b"d8:announce16:http://tracker/a4:info" + raw_info + b"e"

    expected_hash = hashlib.sha1(raw_info).hexdigest().lower()
    infohash, name, total, announce = _bencoded_info_hash(raw_torrent)
    assert infohash == expected_hash
    assert announce == "http://tracker/a"


def test_bdecode_security_caps():
    import pytest
    from racing_sync.watchdir import _bdecode, MAX_BENCODE_DEPTH

    # String length pointing past EOF
    bad_str = b"50:short"
    with pytest.raises(ValueError, match="extends past EOF"):
        _bdecode(bad_str, 0)

    # Negative string length
    bad_neg = b"-5:hello"
    with pytest.raises(ValueError):
        _bdecode(bad_neg, 0)

    # Depth recursion cap
    nested = b"l" * (MAX_BENCODE_DEPTH + 5) + b"i1e" + b"e" * (MAX_BENCODE_DEPTH + 5)
    with pytest.raises(ValueError, match="recursion depth"):
        _bdecode(nested, 0)

    # Empty torrent / oversize torrent
    with pytest.raises(ValueError, match="empty torrent"):
        _bencoded_info_hash(b"")

    oversize = b"d4:info" + (b"0" * (21 * 1024 * 1024)) + b"e"
    with pytest.raises(ValueError, match="exceeds maximum allowed size"):
        _bencoded_info_hash(oversize)


@pytest.mark.anyio
async def test_watchdir_bad_file_caching_and_half_write(tmp_path: Path):
    import time
    import os
    from unittest.mock import patch

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    corrupt_file = watch_dir / "corrupt.torrent"
    corrupt_file.write_bytes(b"d4:infonot_valid_bencode")

    # If mtime is right now, it should be treated as half-write and not added to bad_files immediately
    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)

    items = await scanner.scan_once()
    assert len(items) == 0
    # Because mtime is recent, bad_files doesn't cache yet (allowing write to finish)
    assert corrupt_file not in scanner._bad_files

    # Set mtime back by 5 seconds
    old_time = time.time() - 5.0
    os.utime(corrupt_file, (old_time, old_time))

    items2 = await scanner.scan_once()
    assert len(items2) == 0
    # Now it is recorded as bad file
    assert corrupt_file in scanner._bad_files

    # Scan again; corrupt file should be skipped from bad_files cache without re-parsing
    with patch("racing_sync.watchdir.parse_torrent_file") as mock_parse:
        items3 = await scanner.scan_once()
        assert len(items3) == 0
        mock_parse.assert_not_called()


@pytest.mark.anyio
async def test_watchdir_prunes_seen_on_delete(tmp_path: Path):
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    tfile = watch_dir / "test.torrent"
    raw_data = _create_sample_torrent_data("Release1", 1000)
    tfile.write_bytes(raw_data)

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=True)
    scanner = WatchDirScanner(cfg, prowlarr=None)

    items = await scanner.scan_once()
    assert len(items) == 1
    infohash = items[0].infohash
    assert infohash in scanner._seen

    # Delete the picked-up item
    await scanner.delete_picked_up(items[0])
    assert not tfile.exists()
    assert infohash not in scanner._seen


def test_matches_release_helper():
    from racing_sync.coordinator import _matches_release

    # Matching titles
    assert _matches_release("Show.Name.S01E01.1080p", 1000, "Show.Name.S01E01.1080p", 1000)
    assert _matches_release("Show.Name.S01E01.1080p.mkv", 1000, "Show.Name.S01E01.1080p", 1000)
    assert _matches_release("Show.Name.S01E01.1080p [A1B2C3D4]", 1000, "Show.Name.S01E01.1080p", 1000)

    # Size tolerance: within 2% or 50MB
    assert _matches_release("Show.Name", 100000000, "Show.Name", 100500000)
    # Size difference too large
    assert not _matches_release("Show.Name", 100000000, "Show.Name", 200000000)

    # CRITICAL: Title mismatch MUST return False even if size is exactly identical
    assert not _matches_release("Completely.Different.Movie", 1000, "Show.Name.S01E01.1080p", 1000)


def test_extract_torrent_files_from_bencoded():
    from racing_sync.watchdir import extract_torrent_files_from_bencoded

    # Single file
    data_single = _create_sample_torrent_data("Movie.1080p.mkv", 25000)
    files = extract_torrent_files_from_bencoded(data_single)
    assert len(files) == 1
    assert files[0].name == "Movie.1080p.mkv"
    assert files[0].size_bytes == 25000

    # Multi file
    torrent_dict = {
        b"info": {
            b"name": b"Show.S01",
            b"files": [
                {b"length": 1000, b"path": [b"Show.S01E01.mkv"]},
                {b"length": 2000, b"path": [b"Season 1", b"Show.S01E02.mkv"]},
            ],
        }
    }
    data_multi = _bencode(torrent_dict)
    files_multi = extract_torrent_files_from_bencoded(data_multi)
    assert len(files_multi) == 2
    assert files_multi[0].name == "Show.S01/Show.S01E01.mkv"
    assert files_multi[0].size_bytes == 1000
    assert files_multi[1].name == "Show.S01/Season 1/Show.S01E02.mkv"
    assert files_multi[1].size_bytes == 2000


@pytest.mark.anyio
async def test_do_new_watch_dir_rejects_wrong_title_matching_size(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://alpha.cc/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beta": "Beta"}
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch", query_prowlarr=True, prefer_prowlarr_result=True)
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    dl_idx = Indexer(1, "Test Indexer (API)", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[dl_idx])
    coord.prowlarr.get_indexer_by_name = MagicMock(return_value=None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    # Hit has identical size (5000) but WRONG title
    wrong_hit = TorrentHit(
        title="Completely.Unrelated.Release",
        guid="1",
        indexer="Test Indexer (API)",
        indexer_id=1,
        size_bytes=5000,
        download_url="http://prowlarr/dl/1",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "test indexer (api)": [wrong_hit],
    }

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

    # download_torrent should NEVER have been called for the unrelated release
    coord.prowlarr.download_torrent.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data


@pytest.mark.anyio
async def test_do_new_watch_dir_query_prowlarr_disabled(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://alpha.cc/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch", query_prowlarr=False)
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    coord.prowlarr = MagicMock()
    coord.prowlarr.search_indexers_parallel = AsyncMock()

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

    # Prowlarr search should NOT be called when query_prowlarr is False
    coord.prowlarr.search_indexers_parallel.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.state == State.QUEUED


@pytest.mark.anyio
async def test_do_new_watch_dir_prefer_prowlarr_result_disabled(tmp_path: Path):
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://alpha.cc/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {}
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch", query_prowlarr=True, prefer_prowlarr_result=False)
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    dl_idx = Indexer(1, "Test Indexer (API)", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[dl_idx])
    coord.prowlarr.search_indexers_parallel = AsyncMock()

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

    # When prefer_prowlarr_result is False, download-target indexers are not even queried for a sacrificial copy
    coord.prowlarr.get_download_indexers.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data


@pytest.mark.anyio
async def test_do_new_watch_dir_classifies_and_skips_oversize_movie(tmp_path: Path):
    from racing_sync.config import ClassifierConfig
    raw_data = _create_sample_torrent_data("Massive.Movie.2024.1080p", 2 * 1024 * 1024 * 1024)
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 10000000000
    coord.cfg.ssd.skip_movie_larger_than_bytes = 1024 * 1024 * 1024  # 1 GB max, torrent is 2 GB
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = False
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch")
    coord.store = store
    def _tr(t, s, error=""):
        t.state = s
        t.last_error = error
    coord.transition = _tr

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

    # Oversize single file must be rejected upfront
    assert ts.classification_kind == "movie"
    assert ts.state == State.FAILED
    assert "single file larger than skip threshold" in ts.last_error
    assert "Massive.Movie.2024.1080p" in ts.last_error


@pytest.mark.anyio
async def test_do_new_watch_dir_classifies_season(tmp_path: Path):
    from racing_sync.config import ClassifierConfig
    torrent_dict = {
        b"announce": b"http://dl-indexer.example.net/announce",
        b"info": {
            b"name": b"Show.S01",
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
            b"files": [
                {b"length": 1000, b"path": [b"Show.S01E01.mkv"]},
                {b"length": 1000, b"path": [b"Show.S01E02.mkv"]},
            ],
        },
    }
    raw_data = _bencode(torrent_dict)
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 10000000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = False
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch")
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

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

    # Season must be classified as "season" and queued
    assert ts.classification_kind == "season"
    assert ts.state == State.QUEUED


@pytest.mark.anyio
async def test_do_new_watch_dir_sacrificial_prefers_first_download_indexer(tmp_path: Path):
    """With two download-target indexers both holding the exact release,
    the sacrificial SSD copy comes from the highest-priority one."""
    raw_data = _create_sample_torrent_data("Private.Movie.1080p", 5000, "http://alpha.cc/announce")
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    first_idx = Indexer(1, "First (API)", "torrent", True, [])
    second_idx = Indexer(2, "Second (API)", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexers = MagicMock(return_value=[first_idx, second_idx])
    coord.prowlarr.get_indexer_by_name = MagicMock(return_value=None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    first_hit = TorrentHit(
        title="Private.Movie.1080p", guid="1", indexer="First (API)",
        indexer_id=1, size_bytes=5000, download_url="http://prowlarr/dl/1",
        magnet_url="", info_url="", publish_date="",
    )
    second_hit = TorrentHit(
        title="Private.Movie.1080p", guid="2", indexer="Second (API)",
        indexer_id=2, size_bytes=5000, download_url="http://prowlarr/dl/2",
        magnet_url="", info_url="", publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "first (api)": [first_hit],
        "second (api)": [second_hit],
    }
    first_bytes = _create_sample_torrent_data(
        "Private.Movie.1080p", 5000, "http://first.example/announce")
    second_bytes = _create_sample_torrent_data(
        "Private.Movie.1080p", 5000, "http://second.example/announce")

    async def mock_dl(hit):
        return first_bytes if hit.indexer_id == 1 else second_bytes

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

    assert ts.cross_seed_source == "public-prowlarr"
    assert ts.cross_seed_blob == first_bytes
    assert ts.state == State.QUEUED


def test_watchdir_pickup_log_shows_domain_not_passkey(tmp_path: Path, caplog):
    """The scanner log must never contain the announce passkey."""
    import logging

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data(
        "Secret.Show.S01E01", 5000,
        "https://dl-indexer.example.net/announce/08f3b6d5c2a7419e0b5d8f3a6c4e29715")
    (watch_dir / "secret.torrent").write_bytes(raw)

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    import asyncio

    with caplog.at_level(logging.INFO, logger="racing_sync.watchdir"):
        items = asyncio.run(scanner.scan_once())

    assert len(items) == 1
    assert any("announce=dl-indexer.example.net" in r.message for r in caplog.records)
    assert not any("08f3b6d5c2a7419e0b5d8f3a6c4e29715" in r.message
                   for r in caplog.records)


# ---- same-content election across watch drops ----

_PUB_ANNOUNCE = "http://tracker.opentrackr.org:1337/announce"
_DL_ANNOUNCE = "http://dl-indexer.example.net/announce"
_PRIV_ANNOUNCE = "https://alpha.cc/announce/xyz"


def _election_coord(tmp_path: Path, store: StateStore):
    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = tmp_path / "state.db"
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in (url or "")
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)
    return coord


def _watch_drop(store: StateStore, name: str, size: int, announce: str,
                piece_length: int, state: State = State.NEW) -> TorrentState:
    """Same content (name+size), distinct infohash via piece_length."""
    raw = _create_sample_torrent_data(name, size, announce, piece_length=piece_length)
    infohash, nm, total, ann = _bencoded_info_hash(raw)
    ts = TorrentState(
        source_infohash=infohash,
        source_name=nm,
        total_bytes=total,
        source_announce_url=ann,
        source_tracker=ann,
        cross_seed_blob=raw,
        cross_seed_source="watch-dir",
        state=state,
    )
    ts._blob = raw
    store.upsert(ts)
    return ts


def test_watch_rank_public_first(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    pub = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 16384)
    dl = _watch_drop(store, "Shared.Release.1080p", 5000, _DL_ANNOUNCE, 32768)
    priv = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 65536)
    assert coord._watch_rank(pub) == 0
    assert coord._watch_rank(dl) == 1
    assert coord._watch_rank(priv) == 2


@pytest.mark.anyio
async def test_scan_watch_ingest_log_shows_domain_not_passkey(tmp_path: Path, caplog):
    """Coordinator ingest log must never contain the announce passkey."""
    import logging

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data(
        "Secret.Show.S01E02", 5000,
        "https://dl-indexer.example.net/announce/aaaabbbbccccddddeeeeffff0000111122223333")
    (watch_dir / "secret2.torrent").write_bytes(raw)

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.store = store
    coord.watch = WatchDirScanner(
        WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False),
        prowlarr=None,
    )
    coord.cfg.watch_dir = coord.watch._cfg
    try:
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            await coord.scan_watch()
        assert any("announce=dl-indexer.example.net" in r.message for r in caplog.records)
        assert not any("aaaabbbbccccddddeeeeffff0000111122223333" in r.message
                       for r in caplog.records)
    finally:
        store.close()


@pytest.mark.anyio
async def test_scan_watch_deletes_done_duplicate_drop(tmp_path: Path):
    """Re-dropped file for a DONE row is clutter: removed like a fresh ingest."""
    from racing_sync.state import StateStore

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data("Done.Show.S01E01", 5000,
                                      "https://alpha.cc/announce/xyz")
    tfile = watch_dir / "done.torrent"
    tfile.write_bytes(raw)
    infohash, _, _, _ = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash=infohash, source_name="Done.Show.S01E01",
                              total_bytes=5000, cross_seed_source="watch-dir",
                              state=State.DONE))
    coord = make_coordinator()
    coord.store = store
    wcfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=True)
    coord.watch = WatchDirScanner(wcfg, prowlarr=None)
    coord.cfg.watch_dir = wcfg
    try:
        await coord.scan_watch()
        assert not tfile.exists()
        assert store.get(infohash).state == State.DONE
    finally:
        store.close()


@pytest.mark.anyio
async def test_scan_watch_keeps_duplicate_drop_while_inflight(tmp_path: Path):
    """Same setup but row still working: the file must survive."""
    from racing_sync.state import StateStore

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data("Busy.Show.S01E01", 5000,
                                      "https://alpha.cc/announce/xyz")
    tfile = watch_dir / "busy.torrent"
    tfile.write_bytes(raw)
    infohash, _, _, _ = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash=infohash, source_name="Busy.Show.S01E01",
                              total_bytes=5000, cross_seed_source="watch-dir",
                              state=State.DOWNLOADING))
    coord = make_coordinator()
    coord.store = store
    wcfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=True)
    coord.watch = WatchDirScanner(wcfg, prowlarr=None)
    coord.cfg.watch_dir = wcfg
    try:
        await coord.scan_watch()
        assert tfile.exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_scan_watch_keeps_done_duplicate_when_pickup_disabled(tmp_path: Path):
    from racing_sync.state import StateStore

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data("Kept.Show.S01E01", 5000,
                                      "https://alpha.cc/announce/xyz")
    tfile = watch_dir / "kept.torrent"
    tfile.write_bytes(raw)
    infohash, _, _, _ = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash=infohash, source_name="Kept.Show.S01E01",
                              total_bytes=5000, cross_seed_source="watch-dir",
                              state=State.DONE))
    coord = make_coordinator()
    coord.store = store
    wcfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    coord.watch = WatchDirScanner(wcfg, prowlarr=None)
    coord.cfg.watch_dir = wcfg
    try:
        await coord.scan_watch()
        assert tfile.exists()
    finally:
        store.close()


def test_is_watch_row_labels_and_blob_dir(tmp_path: Path):
    from racing_sync.state import StateStore

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    coord = make_coordinator()
    coord.cfg.general.state_db = db_path
    coord.store = store
    try:
        # All three watch SSD flavours identify by label alone.
        for label in ("watch-dir", "public-watch-dir", "public-prowlarr"):
            ts = TorrentState(source_infohash="a" * 40, cross_seed_source=label)
            assert coord._is_watch_row(ts) is True
        # VPS1 flavours and empty labels do not — without a blob dir.
        for label in ("", "public-racing", "public-dl-indexer-fallback",
                  "dl-indexer-cross-seed", "private-sftp-fallback",
                      "private-export-fallback"):
            ts = TorrentState(source_infohash="b" * 40, cross_seed_source=label)
            assert coord._is_watch_row(ts) is False
        # The persisted blob dir is the durable signal either way.
        d = tmp_path / "watch_cross_seeds" / ("c" * 40)
        d.mkdir(parents=True)
        (d / "x.torrent").write_bytes(b"not-a-torrent")
        assert coord._is_watch_row(
            TorrentState(source_infohash="c" * 40, cross_seed_source="")) is True
        assert coord._is_watch_row(
            TorrentState(source_infohash="d" * 40, cross_seed_source="")) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_new_watch_dir_all_remote_skips_budget(tmp_path: Path):
    """Already-remote drops never queue behind SSD budget they won't use."""
    from unittest.mock import AsyncMock

    from racing_sync.config import ClassifierConfig

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Budget.Movie.1080p.mkv").write_bytes(b"m" * 2000)
    raw = _create_sample_torrent_data(
        "Budget.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    infohash, name, total, announce = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 1  # budget exhausted for everything
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord.cfg.general.state_db = tmp_path / "state.db"
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = False
    coord.prowlarr = None
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)
    coord._ssd_try_reserve = AsyncMock()

    ts = TorrentState(
        source_infohash=infohash, source_name=name, total_bytes=total,
        source_announce_url=announce, cross_seed_blob=raw,
        cross_seed_source="watch-dir", state=State.NEW,
    )
    ts._blob = raw
    try:
        await coord._do_new_watch_dir(ts)
        assert ts.state == State.QUEUED
        coord._ssd_try_reserve.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_new_watch_dir_partial_remote_still_parks(tmp_path: Path):
    """One missing byte on fuse: normal budget queue, no over-skip."""
    from unittest.mock import AsyncMock

    from racing_sync.config import ClassifierConfig

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    raw = _create_sample_torrent_data(
        "Missing.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    infohash, name, total, announce = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 1
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord.cfg.general.state_db = tmp_path / "state.db"
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = False
    coord.prowlarr = None
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)
    coord._ssd_try_reserve = AsyncMock(return_value=False)

    ts = TorrentState(
        source_infohash=infohash, source_name=name, total_bytes=total,
        source_announce_url=announce, cross_seed_blob=raw,
        cross_seed_source="watch-dir", state=State.NEW,
    )
    ts._blob = raw
    try:
        await coord._do_new_watch_dir(ts)
        assert ts.state == State.WAITING_DISK
        coord._ssd_try_reserve.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_scan_watch_skips_ignored_drop_and_keeps_file(tmp_path: Path):
    """A cancelled (ignored) hash re-dropped is left alone, not re-ingested."""
    from racing_sync.state import StateStore

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    raw = _create_sample_torrent_data(
        "Ignored.Show.S01E01", 5000, "https://alpha.cc/announce/xyz")
    tfile = watch_dir / "ignored.torrent"
    tfile.write_bytes(raw)
    infohash, _, _, _ = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    store.ignore_torrent(infohash, "Ignored.Show.S01E01")
    coord = make_coordinator()
    coord.store = store
    wcfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=True)
    coord.watch = WatchDirScanner(wcfg, prowlarr=None)
    coord.cfg.watch_dir = wcfg
    try:
        await coord.scan_watch()
        assert store.get(infohash) is None
        assert tfile.exists()
    finally:
        store.close()


def test_watch_election_prefers_public(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    pub = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 16384)
    priv = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    ok_pub, owner_pub = coord._watch_election(pub)
    ok_priv, owner = coord._watch_election(priv)
    assert ok_pub is True
    assert owner_pub is None
    assert ok_priv is False
    assert owner is not None and owner.source_infohash == pub.source_infohash


def test_watch_election_prefers_download_tracker_over_sacrificial(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    dl = _watch_drop(store, "Shared.Release.1080p", 5000, _DL_ANNOUNCE, 16384)
    priv = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    ok_dl, owner_dl = coord._watch_election(dl)
    ok_priv, owner = coord._watch_election(priv)
    assert ok_dl is True
    assert owner_dl is None
    assert ok_priv is False
    assert owner is not None and owner.source_infohash == dl.source_infohash


def test_watch_election_defers_to_inflight_owner(tmp_path: Path):
    """First-come lock: a public NEW debut defers to a private DOWNLOADING row."""
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    owner = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 16384,
                        state=State.DOWNLOADING)
    pub = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 32768)
    ok_pub, winner = coord._watch_election(pub)
    assert ok_pub is False
    assert winner is not None and winner.source_infohash == owner.source_infohash
    # The owner itself is unblocked.
    ok_owner, owner_owner = coord._watch_election(owner)
    assert ok_owner is True
    assert owner_owner is None


def test_watch_election_sees_proceeded_flavours_as_peers(tmp_path: Path):
    """A public row that already proceeded still serializes later arrivals.

    Regression: the peer filter only matched the pristine "watch-dir"
    label, so a private drop arriving after the public copy left NEW
    downloaded the same bytes a second time.
    """
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    owner = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 16384,
                        state=State.DOWNLOADING)
    owner.cross_seed_source = "public-watch-dir"
    store.upsert(owner)
    late = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    ok_late, winner = coord._watch_election(late)
    assert ok_late is False
    assert winner is not None and winner.source_infohash == owner.source_infohash
    # Same for a sacrificial row mid-flight.
    owner.cross_seed_source = "public-prowlarr"
    store.upsert(owner)
    ok_late2, winner2 = coord._watch_election(late)
    assert ok_late2 is False
    assert winner2 is not None and winner2.source_infohash == owner.source_infohash


def test_watch_wait_note_names_winning_tracker(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    _watch_drop(store, "Shared.Release.1080p", 5000,
                "https://dl-indexer.example.net/announce/xyz", 16384,
                state=State.DOWNLOADING)
    waiter = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    assert coord._watch_wait_note(waiter) == "Waiting turn · dl-indexer.example.net copy first"
    # Proceeding rows and non-watch rows get no note.
    assert coord._watch_wait_note(
        _watch_drop(store, "Other.Release.1080p", 5000, _PRIV_ANNOUNCE, 16384)) == ""


def test_watch_election_releases_on_done_and_failed(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    owner = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 16384,
                        state=State.DOWNLOADING)
    waiter = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 32768)
    assert coord._watch_election(waiter)[0] is False
    owner.state = State.DONE
    store.upsert(owner)
    assert coord._watch_election(waiter)[0] is True
    owner.state = State.FAILED
    store.upsert(owner)
    assert coord._watch_election(waiter)[0] is True


def test_watch_election_ignores_other_content_and_non_watch_rows(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    # Different size: different content, no election.
    other = _watch_drop(store, "Shared.Release.1080p", 6000, _PRIV_ANNOUNCE, 16384,
                        state=State.DOWNLOADING)
    solo = _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    assert coord._watch_election(solo)[0] is True
    # VPS1-derived row with same content: watch election does not gate it.
    racing = TorrentState(
        source_infohash="r" * 40,
        source_name="Shared.Release.1080p",
        total_bytes=5000,
        state=State.NEW,
    )
    store.upsert(racing)
    assert coord._watch_election(racing)[0] is True
    # ... and a racing row never blocks a watch row either.
    racing.state = State.DOWNLOADING
    store.upsert(racing)
    assert coord._watch_election(solo)[0] is True
    assert other.state == State.DOWNLOADING  # untouched


@pytest.mark.anyio
async def test_do_new_watch_dir_defers_when_peer_owns_content(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    coord.prowlarr = MagicMock()
    _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 16384,
                state=State.DOWNLOADING)
    pub = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 32768)

    await coord._do_new_watch_dir(pub)

    assert pub.state == State.NEW
    coord.prowlarr.get_download_indexers.assert_not_called()


@pytest.mark.anyio
async def test_wait_disk_then_queue_defers_for_watch_election(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = _election_coord(tmp_path, store)
    _watch_drop(store, "Shared.Release.1080p", 5000, _PRIV_ANNOUNCE, 16384,
                state=State.DOWNLOADING)
    waiter = _watch_drop(store, "Shared.Release.1080p", 5000, _PUB_ANNOUNCE, 32768,
                         state=State.WAITING_DISK)
    coord._ssd_try_reserve = AsyncMock(return_value=True)

    await coord._wait_disk_then_queue(waiter)

    assert waiter.state == State.WAITING_DISK
    coord._ssd_try_reserve.assert_not_called()


@pytest.mark.anyio
async def test_watchdir_same_size_swap_reparsed(tmp_path: Path):
    """An mtime+size cache hit must still prove byte-identity.

    Same-size replacement with a preserved mtime (syncthing-style) must
    not serve the stale infohash forever.
    """
    import os

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    tfile = watch_dir / "swap.torrent"
    data_a = _create_sample_torrent_data("AAAA.Release", 5000, "http://tracker.example.com/announce")
    data_b = _create_sample_torrent_data("BBBB.Release", 5000, "http://tracker.example.com/announce")
    assert len(data_a) == len(data_b)
    tfile.write_bytes(data_a)

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    items = await scanner.scan_once()
    assert len(items) == 1 and items[0].name == "AAAA.Release"

    st = tfile.stat()
    tfile.write_bytes(data_b)
    os.utime(tfile, (st.st_atime, st.st_mtime))
    scanner._seen.clear()
    items2 = await scanner.scan_once()
    assert len(items2) == 1 and items2[0].name == "BBBB.Release"


@pytest.mark.anyio
async def test_watchdir_picks_up_uppercase_suffix(tmp_path: Path):
    """Show.TORRENT must not be silently ignored (Linux glob is exact-case)."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "Show.TORRENT").write_bytes(
        _create_sample_torrent_data("Upper.Release", 5000, "http://tracker.example.com/announce"))

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    items = await scanner.scan_once()
    assert len(items) == 1 and items[0].name == "Upper.Release"


def _grace_coord(tmp_path: Path, store: StateStore):
    """Watch scaffold with an explicit preferred-copy grace window."""
    coord = make_coordinator()
    coord.store = store
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.general.state_db = tmp_path / "state.db"
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.tracker_map.entries = {}
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in (url or "")
    coord.cfg.prowlarr.should_skip_title = MagicMock(return_value=False)
    coord.cfg.cross_seed.prowlarr_retry_interval_seconds = 1800
    coord.prowlarr = MagicMock()
    coord.dest_client = MagicMock()
    from unittest.mock import AsyncMock

    coord.dest_client.delete = AsyncMock()
    transitioned = []
    coord.transition = lambda t, s, **k: transitioned.append(s)
    coord.transitioned = transitioned
    return coord


@pytest.mark.anyio
async def test_watch_sacrificial_holds_preferred_grace(tmp_path: Path):
    """A rank-2 NEW drop holds (stays NEW) instead of locking immediately."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        ts = _watch_drop(store, "Grace.Hold.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        await coord._do_new_watch_dir(ts)
        assert coord.transitioned == []
        assert store.get(ts.source_infohash, include_blob=False).state == State.NEW
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_sacrificial_proceeds_after_grace(tmp_path: Path):
    """An aged rank-2 drop proceeds (no indefinite hold)."""
    import datetime as dt

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        ts = _watch_drop(store, "Grace.Aged.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        ts.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)
        store.upsert(ts)
        await coord._do_new_watch_dir(ts)
        assert coord.transitioned == [State.WAITING_DISK]
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_rank1_proceeds_despite_grace(tmp_path: Path):
    """A fresh download-indexer drop never waits (already preferred)."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        ts = _watch_drop(store, "Grace.Rank1.1080p", 5000,
                         "https://dl-indexer.example.net/announce/xyz", 16384)
        await coord._do_new_watch_dir(ts)
        assert coord.transitioned == [State.WAITING_DISK]
    finally:
        store.close()


def _racing_inflight(store: StateStore, name: str, size: int = 5000):
    """Same-content racing row past admission (invisible to watch election)."""
    ts = TorrentState(
        source_infohash="f" * 39 + "1", source_name=name, total_bytes=size,
        source_announce_url="https://ops.example/announce/xyz",
        source_tracker="https://ops.example/announce/xyz",
        cross_seed_source="prowlarr", state=State.QUEUED)
    store.upsert(ts)
    return ts


@pytest.mark.anyio
async def test_watch_new_defers_to_inflight_racing_copy(tmp_path: Path):
    """Grace hold yields: an in-flight racing copy owns the content."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        _racing_inflight(store, "Grace.Flyer.1080p")
        ts = _watch_drop(store, "Grace.Flyer.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        await coord._do_new_watch_dir(ts)
        # Deferred as a waiter, not grace-held and not admitted.
        assert coord.transitioned == []
        assert store.get(ts.source_infohash, include_blob=False).state == State.NEW
        note = coord._watch_wait_note(ts)
        assert note.startswith("Waiting turn")
        assert "ops.example" in note
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_prefer_override_beats_inflight_deferral(tmp_path: Path):
    """Operator /prefer_ still wins over the cross-path deferral."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        _racing_inflight(store, "Grace.Flyer.1080p")
        ts = _watch_drop(store, "Grace.Flyer.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        coord._grace_exempt = {ts.source_infohash: 1.0}
        await coord._do_new_watch_dir(ts)
        # Exemption consumed and the row admitted past the deferral.
        assert coord._grace_exempt == {}
        assert coord.transitioned != []
    finally:
        store.close()


def test_watch_wait_note_preferred_grace(tmp_path: Path):
    """Grace-held rows get a card note instead of looking stuck."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _election_coord(tmp_path, store)
        coord.cfg.general.preferred_copy_grace_seconds = 300
        waiter = _watch_drop(store, "Grace.Note.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        assert coord._watch_wait_note(waiter).startswith("Waiting for preferred copy")
    finally:
        store.close()


def _grace_pair(store: StateStore):
    """Two same-content NEW priv drops (distinct hashes), both fresh.

    The first is backdated seconds so ordering is deterministic while
    both stay inside the grace window.
    """
    import datetime as dt

    a = _watch_drop(store, "Skip.Grace.1080p", 5000, _PRIV_ANNOUNCE, 16384)
    b = _watch_drop(store, "Skip.Grace.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    assert a.source_infohash != b.source_infohash
    a.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
    store.upsert(a)
    return a, b


def _prefer_pair(store: StateStore):
    """Two same-content NEW watch drops (priv tracker) with distinct hashes.

    The first is backdated seconds so it deterministically wins ties
    (both stay inside the grace window).
    """
    import datetime as dt

    a = _watch_drop(store, "Prefer.Pair.1080p", 5000, _PRIV_ANNOUNCE, 16384)
    b = _watch_drop(store, "Prefer.Pair.1080p", 5000, _PRIV_ANNOUNCE, 32768)
    assert a.source_infohash != b.source_infohash
    a.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=10)
    store.upsert(a)
    return a, b


def test_watch_election_skips_grace_held_peers(tmp_path: Path):
    """No phantom owners: a grace-held peer blocks nobody.

    Two fresh rank-2 drops evaluate independently (both proceed to hold);
    with grace disabled the later still defers to the earlier.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        a, b = _grace_pair(store)
        ok, owner = coord._watch_election(b)
        assert ok is True and owner is None
        coord.cfg.general.preferred_copy_grace_seconds = 0
        ok2, owner2 = coord._watch_election(b)
        assert ok2 is False and owner2 is not None
        assert owner2.source_infohash == a.source_infohash
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_sacrificial_hit_skips_hold(tmp_path: Path):
    """A download-indexer hit found by the search releases the hold at once."""
    from racing_sync.prowlarr import Indexer, TorrentHit

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        ts = _watch_drop(store, "Grace.Hit.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        idx = Indexer(1, "Preferred (API)", "torrent", True, [])
        coord.prowlarr.get_download_indexers = MagicMock(return_value=[idx])
        raw = _create_sample_torrent_data("Grace.Hit.1080p", 5000,
                                          "http://preferred.example.net/announce")
        coord.prowlarr.search_indexers_parallel = AsyncMock(
            return_value={"preferred (api)": [
                TorrentHit(title="Grace.Hit.1080p", guid="g", indexer="Preferred (API)",
                           indexer_id=1, size_bytes=5000,
                           download_url="http://preferred.example.net/dl",
                           magnet_url="", info_url="", publish_date="")]})
        coord.prowlarr.download_torrent = AsyncMock(return_value=raw)
        await coord._do_new_watch_dir(ts)
        assert coord.transitioned == [State.WAITING_DISK]
        assert ts.cross_seed_source == "public-prowlarr"
    finally:
        store.close()


def test_prefer_grace_row_starts_held_row(tmp_path: Path):
    """Operator pick exempts a grace-held row; siblings counted."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        a, _b = _prefer_pair(store)
        row, msg = coord.prefer_grace_row(a.source_infohash)
        assert row is not None and row.source_infohash == a.source_infohash
        assert msg.startswith("Preferred")
        assert "1 waiting sibling(s)" in msg
        assert (a.source_infohash.lower() in (coord._grace_exempt or {})) is True
    finally:
        store.close()


def test_prefer_grace_row_unknown_and_ambiguous(tmp_path: Path):
    """Unknown hashes and ambiguous prefixes raise LookupError."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        with pytest.raises(LookupError):
            coord.prefer_grace_row("d" * 40)
        with pytest.raises(LookupError, match="not a torrent hash"):
            coord.prefer_grace_row("zzz")
        # Two rows sharing a first hex char -> ambiguous prefix.
        seen: dict[str, object] = {}
        pair = None
        for pl in range(16384, 16424):
            r = _watch_drop(store, "Prefer.Ambi.1080p", 5000, _PRIV_ANNOUNCE, pl)
            c = (r.source_infohash or "")[:1]
            if c in seen:
                pair = c
                break
            seen[c] = r
        assert pair is not None
        with pytest.raises(LookupError, match="matches 2"):
            coord.prefer_grace_row(pair)
    finally:
        store.close()


def test_prefer_grace_row_refusals(tmp_path: Path):
    """Rank-1, owned, and non-NEW rows get explanations, not exemptions."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        first = _watch_drop(store, "Prefer.Rank1.1080p", 5000,
                            "https://dl-indexer.example.net/announce/xyz", 16384)
        row, msg = coord.prefer_grace_row(first.source_infohash)
        assert row is None and "already first" in msg
        owner = _watch_drop(store, "Prefer.Owned.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        waiter = _watch_drop(store, "Prefer.Owned.1080p", 5000, _PRIV_ANNOUNCE, 32768)
        owner.state = State.DOWNLOADING
        store.upsert(owner)
        row2, msg2 = coord.prefer_grace_row(waiter.source_infohash)
        assert row2 is None and "waits on" in msg2
        locked = _watch_drop(store, "Prefer.Locked.1080p", 5000, _PRIV_ANNOUNCE, 16385)
        locked.state = State.QUEUED
        store.upsert(locked)
        row3, msg3 = coord.prefer_grace_row(locked.source_infohash)
        assert row3 is None and "is queued" in msg3
        assert getattr(coord, "_grace_exempt", {}) == {}
    finally:
        store.close()


@pytest.mark.anyio
async def test_prefer_exemption_consumed_by_worker(tmp_path: Path):
    """An exempted row proceeds past grace on its next run."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        a, _b = _prefer_pair(store)
        row, _msg = coord.prefer_grace_row(a.source_infohash)
        assert row is not None
        await coord._do_new_watch_dir(a)
        assert coord.transitioned == [State.WAITING_DISK]
        assert (a.source_infohash.lower() in (coord._grace_exempt or {})) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_grace_search_throttled_while_holding(tmp_path: Path):
    """Repeat evaluations inside the window don't re-query per tick."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        from racing_sync.prowlarr import Indexer

        coord.prowlarr.get_download_indexers = MagicMock(
            return_value=[Indexer(1, "Preferred (API)", "torrent", True, [])])
        coord.prowlarr.search_indexers_parallel = AsyncMock(return_value={})
        ts = _watch_drop(store, "Grace.Throttle.1080p", 5000, _PRIV_ANNOUNCE, 16384)
        await coord._do_new_watch_dir(ts)
        await coord._do_new_watch_dir(ts)
        assert coord.prowlarr.search_indexers_parallel.await_count == 1
        assert coord.transitioned == []
    finally:
        store.close()


def test_prefer_exemption_pruned_with_row(tmp_path: Path):
    """Exemptions for gone/non-NEW rows are reaped by the prune."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        coord.cfg = MagicMock()
        coord._grace_exempt = {"z" * 40: 1.0}
        coord._ssd_reserved = {}
        coord._waiting_disk_next_check = {}
        coord._moving_parks = {}
        coord._ssd_prune_stale()
        assert coord._grace_exempt == {}
    finally:
        store.close()


def test_prefer_succeeds_despite_grace_held_peer(tmp_path: Path):
    """Preferring the later drop while the earlier grace-held peer exists
    must exempt, not refuse with 'waits on'."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        _a, b = _grace_pair(store)
        row, msg = coord.prefer_grace_row(b.source_infohash)
        assert row is not None and row.source_infohash == b.source_infohash
        assert msg.startswith("Preferred")
        assert "1 waiting sibling(s)" in msg
    finally:
        store.close()


def test_exempt_peer_blocks_election(tmp_path: Path):
    """A /prefer_'d row leads: fellow waiters defer to it automatically."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_coord(tmp_path, store)
        a, b = _grace_pair(store)
        row, _msg = coord.prefer_grace_row(a.source_infohash)
        assert row is not None
        ok, owner = coord._watch_election(b)
        assert ok is False
        assert owner is not None and owner.source_infohash == a.source_infohash
    finally:
        store.close()


def _on_ssd_coord(tmp_path: Path, store, *, complete: bool):
    """Watch harness with a dest entry for the dropped hash (manual seed)."""
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.config import ClassifierConfig

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 1  # budget exhausted for everything
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.rclone.fuse.mount = tmp_path / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord.cfg.general.state_db = tmp_path / "state.db"
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = False
    coord.prowlarr = None
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)
    return coord


@pytest.mark.anyio
async def test_do_new_watch_dir_on_ssd_complete_skips_budget(tmp_path: Path):
    """Same hash already complete on dest: QUEUED with no SSD reservation."""
    from unittest.mock import AsyncMock, MagicMock

    raw = _create_sample_torrent_data(
        "Seeded.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    infohash, name, total, announce = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    coord = _on_ssd_coord(tmp_path, store, complete=True)
    entry = MagicMock(hash=infohash, save_path=str(tmp_path / "downloads"))
    entry.is_complete = MagicMock(return_value=True)
    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[entry])
    coord._ssd_try_reserve = AsyncMock(return_value=False)

    ts = TorrentState(
        source_infohash=infohash, source_name=name, total_bytes=total,
        source_announce_url=announce, cross_seed_blob=raw,
        cross_seed_source="watch-dir", state=State.NEW,
    )
    ts._blob = raw
    try:
        await coord._do_new_watch_dir(ts)
        assert ts.state == State.QUEUED
        coord._ssd_try_reserve.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_new_watch_dir_on_ssd_partial_still_parks(tmp_path: Path):
    """Same hash present but partial: normal budget queue, no over-skip."""
    from unittest.mock import AsyncMock, MagicMock

    raw = _create_sample_torrent_data(
        "Partial.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    infohash, name, total, announce = _bencoded_info_hash(raw)

    store = StateStore(tmp_path / "state.db")
    coord = _on_ssd_coord(tmp_path, store, complete=False)
    entry = MagicMock(hash=infohash, save_path=str(tmp_path / "downloads"))
    entry.is_complete = MagicMock(return_value=False)
    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[entry])
    coord._ssd_try_reserve = AsyncMock(return_value=False)

    ts = TorrentState(
        source_infohash=infohash, source_name=name, total_bytes=total,
        source_announce_url=announce, cross_seed_blob=raw,
        cross_seed_source="watch-dir", state=State.NEW,
    )
    ts._blob = raw
    try:
        await coord._do_new_watch_dir(ts)
        assert ts.state == State.WAITING_DISK
        coord._ssd_try_reserve.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_wait_disk_promotes_when_complete_lands_on_dest(tmp_path: Path):
    """PARKED row whose bytes finish on dest skips the budget on re-check."""
    from unittest.mock import AsyncMock, MagicMock

    store = StateStore(tmp_path / "state.db")
    coord = _on_ssd_coord(tmp_path, store, complete=True)
    coord._stop = False
    coord._waiting_disk_next_check = {}
    coord._watch_election = MagicMock(return_value=(True, None))
    entry = MagicMock(hash="c" * 40, save_path=str(tmp_path / "downloads"))
    entry.is_complete = MagicMock(return_value=True)
    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[entry])
    coord._ssd_try_reserve = AsyncMock(return_value=False)

    ts = TorrentState(source_infohash="c" * 40, source_name="LateComplete",
                      total_bytes=16_000, state=State.WAITING_DISK)
    try:
        await coord._wait_disk_then_queue(ts)
        assert ts.state == State.QUEUED
        coord._ssd_try_reserve.assert_not_called()
    finally:
        store.close()
