from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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
    rclone_bin = tmp_path / "rclone"
    rclone_bin.write_bytes(b"fake")
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    fuse = tmp_path / "fuse"
    (fuse / "unsorted").mkdir(parents=True)
    logs = tmp_path / "logs"
    cfg_text = f"""
[general]
source_poll_interval = 30
dest_poll_interval = 15
state_db = "{(tmp_path / "state.db").as_posix()}"
log_dir = "{logs.as_posix()}"

[source]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8081"
save_path = "{downloads.as_posix()}"

[ssd]
path = "{downloads.as_posix()}"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[rclone]
binary = "{rclone_bin.as_posix()}"

[rclone.remote]
default = "remote:movies/"
unsorted = "remote:unsorted/"

[rclone.fuse]
mount = "{fuse.as_posix()}"
mount_unsorted = "{(fuse / "unsorted").as_posix()}"
"""
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(cfg_text)
    rc = main(["check-config", "--config", str(cfg_file)])
    assert rc == 0


def test_main_check_config_reports_missing_binary(tmp_path: Path, capsys):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG + '\n[rclone]\nbinary = "/no/such/rclone"\n')
    rc = main(["check-config", "--config", str(cfg_file)])
    assert rc == 2
    assert "rclone.binary" in capsys.readouterr().err


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

        rc = main(["run", "--config", str(cfg_file), "--reset", "--yes"])
        assert rc == 0

    assert not state_db.exists()
    assert not (tmp_path / "state.db-wal").exists()
    assert log_dir.is_dir()
    assert list(log_dir.iterdir()) == []


def test_run_reset_requires_yes(tmp_path: Path):
    """--reset without --yes refuses (destructive bookkeeping wipe)."""
    from racing_sync.__main__ import main

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    rc = main(["run", "--config", str(cfg_file), "--reset"])
    assert rc == 2


def test_do_reset_refuses_fuse_overlap_log_dir(tmp_path: Path):
    """A log dir on/under a fuse mount is never cleared."""
    from racing_sync.__main__ import _do_reset
    from racing_sync.config import AppConfig

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    cfg = AppConfig.from_toml(cfg_file)
    cfg.general.state_db = tmp_path / "state.db"
    fuse = tmp_path / "mnt-remote"
    fuse.mkdir()
    (fuse / "l.log").write_text("x")
    cfg.general.log_dir = fuse
    cfg.rclone.fuse.mount = fuse
    lines = _do_reset(cfg)
    assert any("overlaps fuse mount" in ln for ln in lines)
    assert (fuse / "l.log").exists()


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


def test_do_reset_refuses_unsafe_state_db(tmp_path: Path):
    """--reset never unlinks a state.db outside racing-sync data."""
    from racing_sync.__main__ import _do_reset
    from racing_sync.config import AppConfig

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    cfg = AppConfig.from_toml(cfg_file)
    cfg.general.log_dir = tmp_path / "logs"
    # A checkout-looking parent must refuse even a real file.
    proj = tmp_path / "proj"
    proj.mkdir()
    (proj / "pyproject.toml").write_text("")
    victim = proj / "state.db"
    victim.write_bytes(b"precious")
    cfg.general.state_db = victim
    lines = _do_reset(cfg)
    assert victim.exists()
    assert any("refusing" in ln for ln in lines)


def test_app_config_rejects_ssd_fuse_overlap(tmp_path: Path):
    """SSD nested in a fuse mount is rejected at load, not warned at runtime."""
    import pydantic

    from racing_sync.config import AppConfig

    bad = MINIMAL_CONFIG.replace('mount = "/mnt/fuse"',
                                 'mount = "/downloads"')
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(bad)
    with pytest.raises(pydantic.ValidationError, match="overlap"):
        AppConfig.from_toml(cfg_file)


def test_check_config_flags_same_remote_and_cap(tmp_path: Path):
    """default==unsorted and cap>disk surface as check-config problems."""
    from racing_sync.__main__ import _check_config_env
    from racing_sync.config import AppConfig

    same_remote = MINIMAL_CONFIG.replace('unsorted = "remote:unsorted/"',
                                         'unsorted = "remote:movies/"')
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(same_remote)
    cfg = AppConfig.from_toml(cfg_file)
    problems = _check_config_env(cfg)
    assert any("unsorted" in p for p in problems)


def test_general_config_preferred_grace_default():
    """Preferred-copy grace defaults to 1 hour (0 disables)."""
    from racing_sync.config import GeneralConfig

    assert GeneralConfig().preferred_copy_grace_seconds == 3600


def test_main_full_requires_yes(tmp_path: Path, capsys):
    """Bare --full refuses before touching anything; --yes proceeds."""
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)

    with patch("racing_sync.__main__.Coordinator"), \
         patch("racing_sync.__main__.setup_logging"):
        rc = main(["run", "--config", str(cfg_file), "--full"])
        assert rc == 2
        out = capsys.readouterr()
        assert "--yes" in out.err

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
        rc = main(["run", "--config", str(cfg_file), "--full", "--yes"])
        assert rc == 0


def test_check_config_warns_not_fails_on_down_fuse_mount(tmp_path: Path):
    """A down fuse mount warns (runtime parks) instead of failing the check."""
    from racing_sync.__main__ import _check_config_env, _check_config_warnings
    from racing_sync.config import AppConfig

    text = MINIMAL_CONFIG.replace('mount = "/mnt/fuse"',
                                  f'mount = "{(tmp_path / "no-such-mount").as_posix()}"')
    text = text.replace('mount_unsorted = "/mnt/fuse/unsorted"',
                        f'mount_unsorted = "{(tmp_path / "no-such-mount-unsorted").as_posix()}"')
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text)
    cfg = AppConfig.from_toml(cfg_file)
    assert _check_config_env(cfg) == [] or all(
        "fuse" not in p for p in _check_config_env(cfg))
    warnings = _check_config_warnings(cfg)
    assert len(warnings) == 2
    assert all("parks" in w for w in warnings)
