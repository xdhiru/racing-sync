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
    assert cmd[1] == "move"
    # Flags before `--`, positionals last — rclone treats everything after
    # `--` as positionals, so flags there break with rc=2 (see prod log).
    assert cmd[-3:] == ["--", str(tmp_path / "src"), "remote:dest"]
    dash_idx = cmd.index("--")
    assert "--config" in cmd[:dash_idx]
    assert str(Path("/etc/rclone.conf")) in cmd[:dash_idx]
    assert "--transfers=4" in cmd[:dash_idx]
    assert "--include=*.mkv" in cmd[:dash_idx]
    assert "--dry-run" in cmd[:dash_idx]


def test_build_move_cmd_files_from_raw(tmp_path: Path):
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    cmd = build_move_cmd(
        cfg, tmp_path / "src", "remote:dest",
        files_from="/tmp/list.lst",
    )
    dash_idx = cmd.index("--")
    assert "--files-from-raw" in cmd[:dash_idx]
    assert "/tmp/list.lst" in cmd[:dash_idx]
    assert cmd[-3:] == ["--", str(tmp_path / "src"), "remote:dest"]


def test_build_move_cmd_rejects_include_plus_files_from(tmp_path: Path):
    import pytest as _pytest

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    with _pytest.raises(Exception):
        build_move_cmd(
            cfg, tmp_path / "src", "remote:dest",
            include=["--include=*.mkv"], files_from="/tmp/list.lst",
        )


@pytest.mark.anyio
async def test_move_local_to_remote_files_from_writes_and_cleans_list(tmp_path: Path):
    from unittest.mock import AsyncMock, patch
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import move_local_to_remote

    src = tmp_path / "src"
    src.mkdir()
    (src / "Top").mkdir()

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    seen: dict = {}

    async def _fake_run(cfg_, cmd, **kwargs):
        idx = cmd.index("--files-from-raw")
        with open(cmd[idx + 1], encoding="utf-8") as fh:
            seen["content"] = fh.read()
        seen["list_path"] = cmd[idx + 1]
        from racing_sync.rclone_ops import RcloneResult
        return RcloneResult(returncode=0, stdout="", stderr="", duration=0.1)

    with patch.object(rclone_ops, "run_rclone", AsyncMock(side_effect=_fake_run)):
        await move_local_to_remote(
            cfg, src, "remote:dest",
            files_from=["Top/a.mkv", "Top/Sub/b.mkv"],
        )

    assert seen["content"] == "Top/a.mkv\nTop/Sub/b.mkv\n"
    assert not Path(seen["list_path"]).exists()


@pytest.mark.anyio
async def test_move_local_to_remote_rejects_empty_files_from(tmp_path: Path):
    import pytest as _pytest
    from racing_sync.rclone_ops import move_local_to_remote

    src = tmp_path / "src"
    src.mkdir()

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()

    with _pytest.raises(ValueError):
        await move_local_to_remote(cfg, src, "remote:dest", files_from=[])


@pytest.mark.anyio
async def test_run_rclone_timeout_redacts_command(monkeypatch):
    from unittest.mock import AsyncMock
    from racing_sync.rclone_ops import run_rclone, RcloneError

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.env = {}
    cfg.rclone.binary = Path("/usr/bin/rclone")

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


@pytest.mark.anyio
async def test_run_rclone_timeout_raises_timeout_error(monkeypatch):
    """A hung remote must raise RcloneTimeoutError (park), not plain RcloneError."""
    from unittest.mock import AsyncMock
    from racing_sync.rclone_ops import run_rclone, RcloneError, RcloneTimeoutError

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")

    mock_proc = AsyncMock()
    mock_proc.communicate.side_effect = TimeoutError()
    mock_proc.terminate = MagicMock()
    mock_proc.wait = AsyncMock()

    with monkeypatch.context() as m:
        m.setattr("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc))
        with pytest.raises(RcloneTimeoutError) as exc_info:
            await run_rclone(cfg, ["rclone", "move", "/src", "remote:dst"], timeout=0.01)
        assert isinstance(exc_info.value, RcloneError)  # still catchable as RcloneError
        assert "source intact" in str(exc_info.value)
        mock_proc.terminate.assert_called()


@pytest.mark.anyio
async def test_run_rclone_cancel_terminates_child(monkeypatch):
    """Shutdown mid-move must kill the child, not orphan an uploader."""
    import asyncio as _asyncio
    from unittest.mock import AsyncMock
    from racing_sync.rclone_ops import run_rclone

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")

    mock_proc = AsyncMock()
    mock_proc.communicate.side_effect = _asyncio.CancelledError()
    mock_proc.terminate = MagicMock()
    mock_proc.wait = AsyncMock()

    with monkeypatch.context() as m:
        m.setattr("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc))
        with pytest.raises(_asyncio.CancelledError):
            await run_rclone(cfg, ["rclone", "move", "/src", "remote:dst"], timeout=60)
        mock_proc.terminate.assert_called()


def test_move_timeout_seconds_falls_back_for_test_doubles():
    """MagicMock configs must not poison the wait_for timeout."""
    from racing_sync.rclone_ops import _move_timeout_seconds

    assert _move_timeout_seconds(MagicMock()) == 6 * 3600
    cfg = MagicMock()
    cfg.rclone.move_timeout_seconds = 1800
    assert _move_timeout_seconds(cfg) == 1800
    cfg.rclone.move_timeout_seconds = 5  # below the 60s floor
    assert _move_timeout_seconds(cfg) == 60.0
