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

    [rclone]
    binary = "/usr/bin/rclone"

    [rclone.remote]
    default = "remote:movies/"
    unsorted = "remote:unsorted/"

    [rclone.fuse]
    mount = "/mnt/fuse"
    mount_unsorted = "/mnt/fuse/unsorted"
    """
    import tomllib
    cfg = AppConfig.model_validate(tomllib.loads(data))
    assert cfg.dest.max_active_downloads == 3
    assert cfg.rclone.max_concurrent_moves == 3
    assert cfg.max_active_downloads == 3
    assert cfg.max_concurrent_moves == 3


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
    """
    import tomllib
    cfg = AppConfig.model_validate(tomllib.loads(data))
    assert cfg.dest.max_active_downloads == 5
    assert cfg.rclone.max_concurrent_moves == 2
    assert cfg.max_active_downloads == 5
    assert cfg.max_concurrent_moves == 2
