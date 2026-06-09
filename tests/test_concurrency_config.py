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


