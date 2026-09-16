from __future__ import annotations

from pathlib import Path
import pytest
from racing_sync.config import AppConfig


@pytest.fixture
def anyio_backend():
    return "asyncio"


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
    assert str(sec) == "**********"
    assert f"user:{sec}" == "user:**********"
    assert sec.get_secret_value() == "mypassword"

    cfg = SourceConfig(type="qbittorrent", host="localhost", password="mypassword")
    assert "mypassword" not in repr(cfg)
    assert "mypassword" not in str(cfg)
    assert "SecretStr('**********')" in repr(cfg)
    assert "mypassword" not in cfg.model_dump_json()
    assert cfg.password.get_secret_value() == "mypassword"


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


@pytest.mark.anyio
async def test_process_torrent_handles_illegal_transition_to_failed():
    coord = object.__new__(Coordinator)
    coord.store = MagicMock()
    coord.store.transition.side_effect = ValueError("illegal transition: done -> failed")
    coord._notify_telegram = AsyncMock()

    # ts is in DONE state, which has no legal transition to FAILED
    ts = TorrentState(source_infohash="h1", source_name="DoneItem", state=State.DONE)

    async def raise_boom(_):
        raise RuntimeError("boom!")

    coord._process_torrent_inner = raise_boom

    # Must catch ValueError from check_transition(DONE -> FAILED) and not crash
    await coord._process_torrent(ts)

    coord.store.upsert.assert_called_once_with(ts)
    assert ts.last_error == "boom!"
    coord.store.append_log.assert_called_once_with("ERROR", "boom!", "h1")
    coord._notify_telegram.assert_awaited_once_with(ts)


@pytest.mark.anyio
async def test_wait_for_completion_stall_timeout():
    from racing_sync.clients.abstract import Torrent

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.general.download_stall_timeout_seconds = 0.05
    coord.cfg.general.dest_poll_interval = 0.01
    coord._stop = False
    coord._live = {"h1": MagicMock()}

    ts = TorrentState(source_infohash="h1", source_name="StalledItem", state=State.DOWNLOADING)

    # Torrent stays at 50%
    stalled_torrent = Torrent(
        hash="h1", name="StalledItem", category="", save_path="", size_bytes=1000, state="downloading", progress=0.5
    )
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(return_value=stalled_torrent)

    with pytest.raises(TimeoutError, match="stalled"):
        await coord._wait_for_completion(ts)


def test_config_strict_validations():
    from pydantic import ValidationError
    from racing_sync.config import (
        NginxAuthConfig,
        DelugeSFTPConfig,
        ProwlarrConfig,
        TelegramConfig,
        LoggingSinkConfig,
        APIConfig,
    )

    # 1. Nginx auth mode defaults to "off"
    nginx = NginxAuthConfig()
    assert nginx.mode == "off"

    # 2. DelugeSFTP port bounds
    with pytest.raises(ValidationError):
        DelugeSFTPConfig(state_dir=Path("/srv/deluge"), ssh_port=0)
    with pytest.raises(ValidationError):
        DelugeSFTPConfig(state_dir=Path("/srv/deluge"), ssh_port=70000)
    assert DelugeSFTPConfig(state_dir=Path("/srv/deluge"), ssh_port=2222).ssh_port == 2222

    # 3. Prowlarr bounds and placeholders
    with pytest.raises(ValidationError):
        ProwlarrConfig(timeout_seconds=0.5)
    with pytest.raises(ValidationError):
        ProwlarrConfig(max_results=0)
    with pytest.raises(ValidationError, match="placeholder"):
        ProwlarrConfig(
            enabled=True,
            base_url="http://localhost:9696",
            api_key="CHANGE_ME",
            download_indexer="Index",
        )

    # 4. Telegram placeholder and missing check
    with pytest.raises(ValidationError, match="placeholder"):
        TelegramConfig(enabled=True, bot_token="CHANGE_ME", chat_id="12345")
    with pytest.raises(ValidationError, match="placeholder"):
        TelegramConfig(enabled=True, bot_token="12345:ABCDE", chat_id="CHANGE_ME")

    # 5. LoggingSink url validation
    with pytest.raises(ValidationError, match="http"):
        LoggingSinkConfig(enabled=True, url="ftp://invalid")
    with pytest.raises(ValidationError, match="http"):
        LoggingSinkConfig(enabled=True, url="")

    # 6. APIConfig token validation
    with pytest.raises(ValidationError, match="placeholder"):
        APIConfig(enabled=True, api_token="CHANGE_ME")
    with pytest.raises(ValidationError, match="api.api_token is required"):
        APIConfig(enabled=True, api_token="")



@pytest.mark.anyio
async def test_pick_ssd_source_public_and_private_paths():
    from unittest.mock import patch
    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import pick_ssd_source_for_racing

    source_client = AsyncMock()
    source_client.export_torrent.return_value = b"d8:announce...e"
    prowlarr = AsyncMock()

    # Public path: public tracker present -> direct export from source_client, no Prowlarr
    t_public = Torrent(
        hash="hpub",
        name="Public.Movie",
        category="",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
        trackers=["http://tracker.openbittorrent.com/announce"],
    )
    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = False
    cfg.cross_seed.refetch_public_via_prowlarr = False

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_public,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=source_client,
    )
    assert dec is not None
    assert dec.source_label == "public-racing"
    assert dec.torrent_bytes == b"d8:announce...e"
    prowlarr.best_match.assert_not_called()

    # Private path: only private tracker present -> query Prowlarr
    t_priv = Torrent(
        hash="hpriv",
        name="Priv.Movie",
        category="",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
        trackers=["https://aither.cc/announce/passkey"],
    )
    cfg_priv = MagicMock()
    cfg_priv.prowlarr.enabled = True
    cfg_priv.prowlarr.should_skip_title.return_value = False
    cfg_priv.prowlarr.download_indexer = "Seedpool (API)"
    cfg_priv.prowlarr.tracker_map = {"aither.cc": "Aither (API)"}
    cfg_priv.prowlarr.get_download_indexer.return_value = MagicMock()
    cfg_priv.cross_seed.allow_prowlarr_cross_seed = True

    hit = MagicMock(title="Priv.Movie", size_bytes=1000, download_url="http://seedpool/1", guid="0123456789012345678901234567890123456789")
    prowlarr.best_match.return_value = hit
    prowlarr.download_torrent.return_value = b"prowlarr_blob"

    dec_priv = await pick_ssd_source_for_racing(
        cfg=cfg_priv,
        source_torrent=t_priv,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=source_client,
    )
    assert dec_priv is not None
    assert dec_priv.source_label == "seedpool-cross-seed"
    assert dec_priv.torrent_bytes == b"prowlarr_blob"
    assert dec_priv.infohash == ""


@pytest.mark.anyio
async def test_wait_disk_then_queue_false_branch():
    from unittest.mock import patch

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord._stop = False
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="h1", state=State.WAITING_DISK, total_bytes=1000)

    with patch("racing_sync.coordinator.ssd_has_room", return_value=False), \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # Should check room once, not sleep, and return immediately to release worker slot
        await coord._wait_disk_then_queue(ts)
        mock_sleep.assert_not_called()
        coord.transition.assert_not_called()


@pytest.mark.anyio
async def test_pick_ssd_source_private_primary_with_public_dupe():
    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import pick_ssd_source_for_racing

    t_priv = Torrent(
        hash="priv_hash_1111",
        name="Movie.2024.1080p",
        category="",
        save_path="",
        size_bytes=2000,
        state="racing",
        progress=1.0,
        trackers=["https://aither.cc/announce/passkey"],
    )
    t_pub = Torrent(
        hash="pub_hash_2222",
        name="Movie.2024.1080p",
        category="",
        save_path="",
        size_bytes=2000,
        state="racing",
        progress=1.0,
        trackers=["udp://tracker.opentrackr.org:1337/announce"],
    )

    source_client = AsyncMock()
    source_client.export_torrent.return_value = b"pub_torrent_bytes"
    sftp = MagicMock()
    sftp.fetch_torrent.return_value = b"sftp_pub_bytes"

    # 1. SFTP export path: must export the public torrent hash, NOT the private one
    cfg_sftp = MagicMock()
    cfg_sftp.cross_seed.allow_ssh_export = True
    cfg_sftp.cross_seed.refetch_public_via_prowlarr = False

    dec_sftp = await pick_ssd_source_for_racing(
        cfg=cfg_sftp,
        source_torrent=t_priv,
        other_source_torrents=[t_pub],
        prowlarr=None,
        sftp=sftp,
        source_client=source_client,
    )
    assert dec_sftp is not None
    assert dec_sftp.source_label == "public-racing"
    assert dec_sftp.infohash == "pub_hash_2222"
    assert dec_sftp.torrent_bytes == b"sftp_pub_bytes"
    sftp.fetch_torrent.assert_called_once_with("pub_hash_2222")

    # 2. WebUI export path: must export the public torrent hash via client
    cfg_webui = MagicMock()
    cfg_webui.cross_seed.allow_ssh_export = False
    cfg_webui.cross_seed.refetch_public_via_prowlarr = False

    dec_webui = await pick_ssd_source_for_racing(
        cfg=cfg_webui,
        source_torrent=t_priv,
        other_source_torrents=[t_pub],
        prowlarr=None,
        sftp=None,
        source_client=source_client,
    )
    assert dec_webui is not None
    assert dec_webui.source_label == "public-racing"
    assert dec_webui.infohash == "pub_hash_2222"
    assert dec_webui.torrent_bytes == b"pub_torrent_bytes"
    source_client.export_torrent.assert_awaited_once_with("pub_hash_2222")


@pytest.mark.anyio
async def test_wait_disk_then_queue_does_not_block_worker():
    from unittest.mock import patch

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord._stop = False
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="h1", state=State.WAITING_DISK, total_bytes=1000)

    # When SSD has no room, _process_torrent_inner finishes immediately
    with patch("racing_sync.coordinator.ssd_has_room", return_value=False):
        await coord._process_torrent_inner(ts)
    coord.transition.assert_not_called()


@pytest.mark.anyio
async def test_do_queued_happy_path():
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = "/mnt/fuse"
    coord.cfg.rclone.fuse.mount_unsorted = "/mnt/fuse/unsorted"
    coord.cfg.classifier._episode_re = None
    coord.cfg.ssd.skip_movie_larger_than_bytes = 10_000_000_000

    ts = TorrentState(source_infohash="h1", source_name="Movie.2024", state=State.QUEUED, save_path="/downloads")
    ts._blob = b"d8:announce..."

    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(return_value=AddResult(hash="dest_h1", accepted=True))
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[TorrentFile("Movie.2024.mkv", 5_000_000_000)])
    coord.dest_client.resume = AsyncMock()

    coord.transition = MagicMock()

    await coord._do_queued(ts)

    coord.dest_client.add_torrent.assert_awaited_once()
    assert ts.dest_infohash == "dest_h1"
    coord.transition.assert_called_once_with(ts, State.DOWNLOADING)
