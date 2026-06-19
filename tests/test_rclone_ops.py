from __future__ import annotations

from pathlib import Path
import pytest
from unittest.mock import MagicMock

from racing_sync.rclone_ops import (
    build_move_cmd,
    redact_rclone_cmd,
    validate_safe_delete_path,
    wipe_local_tree,
    wipe_local_files,
)
from racing_sync.config import AppConfig


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_validate_safe_delete_path_refuses_root(tmp_path: Path):
    root = Path(tmp_path.resolve().anchor)
    with pytest.raises(ValueError, match="refusing to delete filesystem root"):
        validate_safe_delete_path(root)


def test_validate_safe_delete_path_refuses_base_dir(tmp_path: Path):
    base = tmp_path / "ssd"
    base.mkdir()
    with pytest.raises(ValueError, match="refusing to delete base directory"):
        validate_safe_delete_path(base, base_dir=base)


def test_validate_safe_delete_path_refuses_outside_base(tmp_path: Path):
    base = tmp_path / "ssd"
    base.mkdir()
    outside = tmp_path / "other"
    outside.mkdir()
    with pytest.raises(ValueError, match="not within allowed base directories"):
        validate_safe_delete_path(outside, base_dir=base)


def test_validate_safe_delete_path_allows_child(tmp_path: Path):
    base = tmp_path / "ssd"
    child = base / "Show.S01"
    child.mkdir(parents=True)
    # Should not raise
    validate_safe_delete_path(child, base_dir=base)


@pytest.mark.anyio
async def test_wipe_local_tree_safely(tmp_path: Path):
    base = tmp_path / "ssd"
    child = base / "Show.S01"
    child.mkdir(parents=True)
    f = child / "ep1.mkv"
    f.write_text("dummy")

    await wipe_local_tree(child, base_dir=base)
    assert not child.exists()
    assert base.exists()


@pytest.mark.anyio
async def test_wipe_local_files_safely(tmp_path: Path):
    base = tmp_path / "ssd"
    base.mkdir(parents=True)
    f1 = base / "ep1.parts"
    f2 = base / "ep2.parts"
    f1.write_text("part1")
    f2.write_text("part2")

    await wipe_local_files([f1, f2], base_dir=base)
    assert not f1.exists()
    assert not f2.exists()
    assert base.exists()


def test_redact_rclone_cmd():
    cmd = [
        "rclone",
        "move",
        "/local/path",
        "remote:path",
        "--password",
        "supersecret",
        "--rc-pass=secret123",
        "--s3-secret-access-key",
        "keyval",
        "--normal-flag",
        "normalval",
    ]
    sanitized = redact_rclone_cmd(cmd)
    assert "supersecret" not in sanitized
    assert "secret123" not in sanitized
    assert "keyval" not in sanitized
    assert "--password ******" in sanitized
    assert "--rc-pass=******" in sanitized
    assert "--s3-secret-access-key ******" in sanitized
    assert "--normal-flag normalval" in sanitized


def test_build_move_cmd(tmp_path: Path):
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = Path("/etc/rclone.conf")
    cfg.rclone.extra_move_flags = ["--transfers=4", "--checkers=8"]

    cmd = build_move_cmd(
        cfg,
        tmp_path / "src",
        "remote:dest",
        include=["--include=*.mkv"],
        extra=["--dry-run"],
    )
    assert cmd[0] == str(Path("/usr/bin/rclone"))
    assert cmd[1:4] == ["move", str(tmp_path / "src"), "remote:dest"]
    assert "--config" in cmd
    assert str(Path("/etc/rclone.conf")) in cmd
    assert "--transfers=4" in cmd
    assert "--include=*.mkv" in cmd
    assert "--dry-run" in cmd


@pytest.mark.anyio
async def test_run_rclone_timeout_redacts_command(monkeypatch):
    from unittest.mock import AsyncMock
    from racing_sync.rclone_ops import run_rclone, RcloneError

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.env = {}

    mock_proc = AsyncMock()
    mock_proc.communicate.side_effect = TimeoutError()
    mock_proc.kill = MagicMock()
    mock_proc.wait = AsyncMock()

    with monkeypatch.context() as m:
        m.setattr("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc))
        cmd = ["rclone", "move", "/src", "remote:dst", "--password", "supersecret123"]
        with pytest.raises(RcloneError) as exc_info:
            await run_rclone(cfg, cmd, timeout=0.01)
        err_msg = str(exc_info.value)
        assert "supersecret123" not in err_msg
        assert "--password ******" in err_msg


