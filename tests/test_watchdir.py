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
    rows = store.all(include_blob=True)
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
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

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
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

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
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

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
    # Fuse target holds the moved content (single-file torrents land by name).
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Movie.Part1").write_bytes(b"x" * 2000)
    (fuse_dir / "Movie.Part2").write_bytes(b"x" * 2000)
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
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


@pytest.mark.anyio
async def test_re_inject_watch_dir_torrents_skips_missing_fuse_content(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
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
    assert _matches_release("Show.Name.S01E01.1080p [Seedpool]", 1000, "Show.Name.S01E01.1080p", 1000)

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
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://aither.cc/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {"beyond-hd": "BeyondHD"}
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch", query_prowlarr=True, prefer_prowlarr_result=True)
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    dl_idx = Indexer(1, "Seedpool (API)", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexer = MagicMock(return_value=dl_idx)
    coord.prowlarr.get_indexer_by_name = MagicMock(return_value=None)
    coord.prowlarr.search_indexers_parallel = AsyncMock()
    coord.prowlarr.download_torrent = AsyncMock()

    # Hit has identical size (5000) but WRONG title
    wrong_hit = TorrentHit(
        title="Completely.Unrelated.Release",
        guid="1",
        indexer="Seedpool (API)",
        indexer_id=1,
        size_bytes=5000,
        download_url="http://prowlarr/dl/1",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    coord.prowlarr.search_indexers_parallel.return_value = {
        "seedpool (api)": [wrong_hit],
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
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://aither.cc/announce")
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
    raw_data = _create_sample_torrent_data("Target.Release.1080p", 5000, "http://aither.cc/announce")
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
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.cfg.prowlarr.should_skip_title.return_value = False
    coord.cfg.prowlarr.tracker_map.entries = {}
    coord.cfg.watch_dir = WatchDirConfig(path=tmp_path / "watch", query_prowlarr=True, prefer_prowlarr_result=False)
    coord.store = store
    coord.transition = lambda t, s, **kwargs: setattr(t, "state", s)

    dl_idx = Indexer(1, "Seedpool (API)", "torrent", True, [])
    coord.prowlarr = MagicMock()
    coord.prowlarr.get_download_indexer = MagicMock(return_value=dl_idx)
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

    # When prefer_prowlarr_result is False, download indexer should not even be queried for sacrificial copy
    coord.prowlarr.get_download_indexer.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.cross_seed_blob == raw_data


@pytest.mark.anyio
async def test_do_new_watch_dir_classifies_and_skips_oversize_movie(tmp_path: Path):
    from racing_sync.config import ClassifierConfig
    raw_data = _create_sample_torrent_data("Massive.Movie.2024.1080p", 2 * 1024 * 1024 * 1024)
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
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
        b"announce": b"http://seedpool.net/announce",
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

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
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



