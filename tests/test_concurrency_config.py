from __future__ import annotations

from pathlib import Path
from racing_sync.config import AppConfig


def test_concurrency_defaults():
    # Minimal config to validate defaults
    data = """
    [general]
    source_poll_interval = 30
    dest_poll_interval = 15

    [source]
    type = "qbittorrent"
    host = "http://127.0.0.1:8080"

    [dest]
    host = "http://127.0.0.1:8081"
    save_path = "/downloads"

    [ssd]
    path = "/downloads"
    max_inflight_bytes = 1000000000
    skip_movie_larger_than_bytes = 1000000000

    [rclone.remote]
    default = "remote:movies/"
    unsorted = "remote:unsorted/"

    [rclone.fuse]
    mount = "/mnt/fuse"
    mount_unsorted = "/mnt/fuse/unsorted"
    """
    import tomllib
    cfg = AppConfig.model_validate(tomllib.loads(data))
    assert cfg.rclone.binary == Path("/usr/local/bin/rclone")
    assert cfg.dest.max_active_downloads == 3
    assert cfg.rclone.max_concurrent_moves == 3
    assert cfg.max_active_downloads == 3
    assert cfg.max_concurrent_moves == 3
    assert cfg.rclone.fuse.reinject_delay_seconds == 30
    assert cfg.fuse_reinject_delay_seconds == 30


def test_concurrency_custom_overrides():
    data = """
    [general]
    source_poll_interval = 30
    dest_poll_interval = 15

    [source]
    type = "qbittorrent"
    host = "http://127.0.0.1:8080"

    [dest]
    host = "http://127.0.0.1:8081"
    save_path = "/downloads"
    max_active_downloads = 5

    [ssd]
    path = "/downloads"
    max_inflight_bytes = 1000000000
    skip_movie_larger_than_bytes = 1000000000

    [rclone]
    binary = "/usr/bin/rclone"
    max_concurrent_moves = 2

    [rclone.remote]
    default = "remote:movies/"
    unsorted = "remote:unsorted/"

    [rclone.fuse]
    mount = "/mnt/fuse"
    mount_unsorted = "/mnt/fuse/unsorted"
    reinject_delay_seconds = 15
    """
    import tomllib
    cfg = AppConfig.model_validate(tomllib.loads(data))
    assert cfg.dest.max_active_downloads == 5
    assert cfg.rclone.max_concurrent_moves == 2
    assert cfg.max_active_downloads == 5
    assert cfg.max_concurrent_moves == 2
    assert cfg.fuse_reinject_delay_seconds == 15


def test_app_config_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test_config.toml"
    toml_file.write_text(
        """
        [general]
        source_poll_interval = 30
        dest_poll_interval = 15

        [source]
        type = "qbittorrent"
        host = "http://127.0.0.1:8080"

        [dest]
        host = "http://127.0.0.1:8081"
        save_path = "/downloads"

        [ssd]
        path = "/downloads"
        max_inflight_bytes = 1000000000
        skip_movie_larger_than_bytes = 1000000000

        [rclone.remote]
        default = "remote:movies/"
        unsorted = "remote:unsorted/"

        [rclone.fuse]
        mount = "/mnt/fuse"
        mount_unsorted = "/mnt/fuse/unsorted"
        """,
        encoding="utf-8",
    )
    cfg = AppConfig.from_toml(toml_file)
    assert cfg.source.type == "qbittorrent"
    assert cfg.dest.save_path == Path("/downloads")


def test_app_config_from_toml_falls_back_to_tomli(tmp_path: Path, monkeypatch):
    import builtins
    from unittest.mock import MagicMock

    toml_file = tmp_path / "test_config.toml"
    toml_file.write_text(
        """
        [general]
        source_poll_interval = 30
        dest_poll_interval = 15

        [source]
        type = "qbittorrent"
        host = "http://127.0.0.1:8080"

        [dest]
        host = "http://127.0.0.1:8081"
        save_path = "/downloads"

        [ssd]
        path = "/downloads"
        max_inflight_bytes = 1000000000
        skip_movie_larger_than_bytes = 1000000000

        [rclone.remote]
        default = "remote:movies/"
        unsorted = "remote:unsorted/"

        [rclone.fuse]
        mount = "/mnt/fuse"
        mount_unsorted = "/mnt/fuse/unsorted"
        """,
        encoding="utf-8",
    )

    orig_import = builtins.__import__
    mock_tomli = MagicMock()
    mock_tomli.load.return_value = {
        "general": {"source_poll_interval": 30, "dest_poll_interval": 15},
        "source": {"type": "qbittorrent", "host": "http://127.0.0.1:8080"},
        "dest": {"host": "http://127.0.0.1:8081", "save_path": "/downloads"},
        "ssd": {
            "path": "/downloads",
            "max_inflight_bytes": 1000000000,
            "skip_movie_larger_than_bytes": 1000000000,
        },
        "rclone": {
            "remote": {"default": "remote:movies/", "unsorted": "remote:unsorted/"},
            "fuse": {"mount": "/mnt/fuse", "mount_unsorted": "/mnt/fuse/unsorted"},
        },
    }

    def fake_import(name, *args, **kwargs):
        if name == "tomllib":
            raise ModuleNotFoundError("No module named 'tomllib'")
        if name == "tomli":
            return mock_tomli
        return orig_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    cfg = AppConfig.from_toml(toml_file)
    mock_tomli.load.assert_called_once()
    assert cfg.source.type == "qbittorrent"


def test_secret_str_masking_in_repr_and_string_equality():
    from racing_sync.config import SecretStr, SourceConfig

    sec = SecretStr("mypassword")
    assert repr(sec) == "SecretStr('**********')"
    assert sec == "mypassword"
    assert sec.get_secret_value() == "mypassword"
    assert f"user:{sec}" == "user:mypassword"

    cfg = SourceConfig(type="qbittorrent", host="localhost", password="mypassword")
    assert "mypassword" not in repr(cfg)
    assert "SecretStr('**********')" in repr(cfg)
    assert cfg.password == "mypassword"


import pytest
from unittest.mock import AsyncMock, MagicMock
from racing_sync.coordinator import Coordinator
from racing_sync.state import TorrentState, State
from racing_sync.clients.abstract import AddResult, TorrentFile


@pytest.mark.anyio
async def test_coordinator_wait_disk_stops_on_stop():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord._stop = True
    ts = TorrentState(source_infohash="abc", source_name="test", total_bytes=9999999999)
    # Should exit immediately without hanging
    await coord._wait_disk_then_queue(ts)
    assert ts.state != State.QUEUED


@pytest.mark.anyio
async def test_coordinator_do_queued_extracts_infohash_from_blob():
    from racing_sync.watchdir import _bencode
    from racing_sync.config import ClassifierConfig
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = "/downloads"
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.ssd.skip_movie_larger_than_bytes = 0
    coord.cfg.rclone.fuse.mount = "/fuse"
    coord.cfg.rclone.fuse.mount_unsorted = "/fuse/unsorted"
    coord.transition = MagicMock()

    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    # add_torrent returns accepted with no hash in result
    coord.dest_client.add_torrent = AsyncMock(return_value=AddResult(hash=None, accepted=True, detail="ok"))
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[TorrentFile(name="ep1.mkv", size_bytes=1000, priority=1, progress=0.0)])
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord._await_hash_for_name = AsyncMock()

    # Bencoded sample torrent
    info_dict = {b"name": b"Test.Episode", b"piece length": 16384, b"pieces": b"", b"length": 1000}
    torrent_dict = {b"info": info_dict, b"announce": b"http://tracker.com/announce"}
    sample_blob = _bencode(torrent_dict)

    import hashlib
    expected_hash = hashlib.sha1(_bencode(info_dict)).hexdigest().lower()

    ts = TorrentState(
        source_infohash="some_other_hash",
        source_name="Test.Episode",
        state=State.QUEUED,
        save_path="/downloads",
        cross_seed_blob=sample_blob,
    )

    await coord._do_queued(ts)

    assert ts.dest_infohash == expected_hash
    # Ensure _await_hash_for_name was never called because it extracted infohash directly from blob
    coord._await_hash_for_name.assert_not_called()


@pytest.mark.anyio
async def test_late_cross_seeds_handles_none_detail_and_expires_failures():
    from racing_sync.clients.abstract import Torrent
    import datetime as dt

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = "/fuse"
    coord._target_mount_for = MagicMock(return_value=Path("/fuse"))
    coord.dest_client = MagicMock()
    coord.store = MagicMock()
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"d8:announce7:http...e")
    coord._failed_late_cross_seeds = {}

    # add_torrent returns accepted=False and detail=None
    coord.dest_client.add_torrent = AsyncMock(return_value=AddResult(hash=None, accepted=False, detail=None))
    coord.dest_client.get_torrent = AsyncMock(return_value=None)

    ts = TorrentState(
        source_infohash="src1",
        source_name="Movie.Title",
        injected_private_hashes="",
    )
    group = [Torrent(hash="late1", name="Movie.Title", category="", save_path="", size_bytes=1000, state="seeding", progress=1.0)]

    # Run injection - must not raise AttributeError: 'NoneType' object has no attribute 'lower'
    await coord._check_and_inject_late_cross_seeds(ts, group)

    assert "late1" in coord._failed_late_cross_seeds

    # Simulate 31 minutes passing
    coord._failed_late_cross_seeds["late1"] = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=31)

    # Now add_torrent succeeds with detail=None
    coord.dest_client.add_torrent = AsyncMock(return_value=AddResult(hash="late1", accepted=True, detail=None))
    await coord._check_and_inject_late_cross_seeds(ts, group)

    assert "late1" in ts.injected_private_hashes
    assert "late1" not in coord._failed_late_cross_seeds




