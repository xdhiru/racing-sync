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


def test_main_run_startup_failure_is_clean(tmp_path: Path, capsys):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)

    with patch("racing_sync.__main__.Coordinator") as mock_coord_cls, \
         patch("racing_sync.__main__.setup_logging"):
        mock_coord = MagicMock()

        async def fake_run():
            raise Exception("Server '[h]:1' not found in known_hosts")

        async def fake_shutdown():
            pass

        mock_coord.run.side_effect = fake_run
        mock_coord.shutdown.side_effect = fake_shutdown
        mock_coord_cls.return_value = mock_coord

        rc = main(["run", "--config", str(cfg_file)])
        assert rc == 2
    err = capsys.readouterr().err
    assert "Failed to start" in err
    assert "known_hosts" in err


def test_main_run_reset_clears_state_db_and_logs(tmp_path: Path):
    from racing_sync.config import AppConfig

    state_db = tmp_path / "state.db"
    state_db.write_bytes(b"old-db")
    (tmp_path / "state.db-wal").write_bytes(b"wal")
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "racing-sync.log").write_text("old logs")
    (log_dir / "old-run").mkdir()
    (log_dir / "old-run" / "x.log").write_text("x")

    # MINIMAL_CONFIG already has [general]; overlay tmp paths on the object.
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    cfg = AppConfig.from_toml(cfg_file)
    cfg.general.state_db = state_db
    cfg.general.log_dir = log_dir

    with patch("racing_sync.__main__.AppConfig") as mock_cfg_cls, \
         patch("racing_sync.__main__.Coordinator") as mock_coord_cls, \
         patch("racing_sync.__main__.setup_logging"):
        mock_cfg_cls.from_toml.return_value = cfg
        mock_coord = MagicMock()

        async def fake_run():
            return 0

        async def fake_shutdown():
            pass

        mock_coord.run.side_effect = fake_run
        mock_coord.shutdown.side_effect = fake_shutdown
        mock_coord_cls.return_value = mock_coord

        rc = main(["run", "--config", str(cfg_file), "--reset"])
        assert rc == 0

    assert not state_db.exists()
    assert not (tmp_path / "state.db-wal").exists()
    assert log_dir.is_dir()
    assert list(log_dir.iterdir()) == []


def test_do_reset_refuses_system_log_dir(tmp_path: Path):
    from racing_sync.__main__ import _do_reset
    from racing_sync.config import AppConfig

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    cfg = AppConfig.from_toml(cfg_file)
    cfg.general.state_db = tmp_path / "state.db"
    for unsafe in (Path("/var/log"), Path("/"), Path.home()):
        cfg.general.log_dir = unsafe
        lines = _do_reset(cfg)
        assert any("refusing" in ln for ln in lines), unsafe


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
