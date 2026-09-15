from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from racing_sync.__main__ import main

MINIMAL_CONFIG = """
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


def test_main_check_config(tmp_path: Path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    rc = main(["check-config", "--config", str(cfg_file)])
    assert rc == 0


def test_signal_handler_fallback_on_not_implemented(tmp_path: Path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)

    with patch("racing_sync.__main__.Coordinator") as mock_coord_cls, \
         patch("racing_sync.__main__.setup_logging"):
        mock_coord = MagicMock()

        async def fake_run():
            return 0

        async def fake_shutdown():
            pass

        mock_coord.run.side_effect = fake_run
        mock_coord.shutdown.side_effect = fake_shutdown
        mock_coord_cls.return_value = mock_coord

        rc = main(["run", "--config", str(cfg_file)])
        assert rc == 0
