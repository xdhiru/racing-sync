from __future__ import annotations

import asyncio
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
    (src / "Top" / "a.mkv").write_bytes(b"a")
    (src / "Top" / "Sub").mkdir(parents=True)
    (src / "Top" / "Sub" / "b.mkv").write_bytes(b"b")

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


@pytest.mark.anyio
async def test_run_rclone_kills_child_on_communicate_error(monkeypatch):
    """Non-timeout communicate() failures must not orphan a running move."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.rclone_ops import run_rclone

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")

    mock_proc = AsyncMock()
    mock_proc.returncode = None
    mock_proc.communicate.side_effect = OSError("broken pipe")
    mock_proc.terminate = MagicMock()
    mock_proc.kill = MagicMock()
    mock_proc.wait = AsyncMock()

    with monkeypatch.context() as m:
        m.setattr("asyncio.create_subprocess_exec", AsyncMock(return_value=mock_proc))
        with pytest.raises(OSError, match="broken pipe"):
            await run_rclone(cfg, ["rclone", "move", "/src", "remote:dst"], timeout=60)
        mock_proc.terminate.assert_called_once()


def test_reject_hijack_flags_normalizes_variants():
    from racing_sync.rclone_ops import _reject_hijack_flags, RcloneError

    for bad in ("--config", "-config", "---config", "--password_command",
                "-password-command=x", "--ask-password"):
        with pytest.raises(RcloneError):
            _reject_hijack_flags([bad], "test")
    # Credential families and the remote-control surface are refused too.
    for bad in ("--password", "--rc", "--rc-addr", "--rc-user",
                "--drive-token", "--s3-session-token", "--b2-key=x",
                "--dropbox-client-secret", "--gcs-client-id",
                "--custom-token"):
        with pytest.raises(RcloneError):
            _reject_hijack_flags([bad], "test")
    # Legit tuning flags pass through untouched.
    _reject_hijack_flags(["--transfers=4", "-v", "--bwlimit=0",
                          "--s3-chunk-size=64M"], "test")


def test_move_timeout_seconds_falls_back_for_test_doubles():
    """MagicMock configs must not poison the wait_for timeout."""
    from racing_sync.rclone_ops import _move_timeout_seconds

    assert _move_timeout_seconds(MagicMock()) == 6 * 3600
    cfg = MagicMock()
    cfg.rclone.move_timeout_seconds = 1800
    assert _move_timeout_seconds(cfg) == 1800
    cfg.rclone.move_timeout_seconds = 5  # below the 60s floor
    assert _move_timeout_seconds(cfg) == 60.0


def _orphan_proc():
    from unittest.mock import AsyncMock, MagicMock

    proc = AsyncMock()
    proc.communicate.side_effect = TimeoutError()
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.wait = AsyncMock(side_effect=asyncio.TimeoutError())
    proc.returncode = None
    proc.pid = 4242
    return proc


def _rclone_cfg():
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    return cfg


@pytest.mark.anyio
async def test_unreaped_orphan_blocks_duplicate_tree(monkeypatch, tmp_path):
    """Timeout + unkillable child keeps the tree guard: retry parks, never races."""
    import asyncio as _asyncio
    from unittest.mock import AsyncMock
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import (
        RcloneBusyError, RcloneTimeoutError, run_rclone,
    )

    tree = tmp_path / "Top"
    tree.mkdir()
    proc = _orphan_proc()
    spawn = AsyncMock(return_value=proc)
    monkeypatch.setattr(_asyncio, "create_subprocess_exec", spawn)
    try:
        with pytest.raises(RcloneTimeoutError):
            await run_rclone(_rclone_cfg(), ["rclone", "move", "x", "y"],
                             timeout=0.01, tree=tree)
        # Orphan unreaped (returncode None): guard held.
        assert rclone_ops._ACTIVE_MOVES, "orphan must keep its tree guard"
        with pytest.raises(RcloneBusyError) as exc_info:
            await run_rclone(_rclone_cfg(), ["rclone", "move", "x", "y"],
                             timeout=0.01, tree=tree)
        assert isinstance(exc_info.value, RcloneTimeoutError)  # parks like timeout
        assert "4242" in str(exc_info.value)
        assert spawn.await_count == 1, "no duplicate spawn while orphan lives"
    finally:
        rclone_ops._ACTIVE_MOVES.clear()


@pytest.mark.anyio
async def test_reaped_orphan_releases_tree(monkeypatch, tmp_path):
    """Once the orphan exits, the next move for the tree proceeds."""
    import asyncio as _asyncio
    from unittest.mock import AsyncMock
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import run_rclone

    tree = tmp_path / "Top"
    tree.mkdir()
    proc = _orphan_proc()
    proc2 = AsyncMock()
    proc2.communicate = AsyncMock(return_value=(b"", b""))
    proc2.returncode = 0
    spawn = AsyncMock(side_effect=[proc, proc2])
    monkeypatch.setattr(_asyncio, "create_subprocess_exec", spawn)
    try:
        with pytest.raises(Exception):
            await run_rclone(_rclone_cfg(), ["rclone", "move", "x", "y"],
                             timeout=0.01, tree=tree)
        assert spawn.await_count == 1
        proc.returncode = 0  # loop reaps the orphan meanwhile
        res = await run_rclone(_rclone_cfg(), ["rclone", "move", "x", "y"],
                               timeout=60, tree=tree)
        assert res.ok
        assert spawn.await_count == 2
        assert not rclone_ops._ACTIVE_MOVES
    finally:
        rclone_ops._ACTIVE_MOVES.clear()


@pytest.mark.anyio
async def test_success_drops_tree_guard(monkeypatch, tmp_path):
    import asyncio as _asyncio
    from unittest.mock import AsyncMock
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import run_rclone

    tree = tmp_path / "Top"
    tree.mkdir()
    proc = AsyncMock()
    proc.communicate = AsyncMock(return_value=(b"", b""))
    proc.returncode = 0
    monkeypatch.setattr(_asyncio, "create_subprocess_exec",
                        AsyncMock(return_value=proc))
    try:
        res = await run_rclone(_rclone_cfg(), ["rclone", "move", "x", "y"],
                               timeout=60, tree=tree)
        assert res.ok
        assert not rclone_ops._ACTIVE_MOVES
    finally:
        rclone_ops._ACTIVE_MOVES.clear()


def test_tree_key_resolves_dotdot():
    """Two spellings of one tree share one guard entry."""
    from racing_sync.rclone_ops import _tree_key

    assert _tree_key("a/b/../c") == _tree_key("a/c")
    assert _tree_key("a/./c") == _tree_key("a/c")


def test_cap_eviction_keeps_live_entries():
    """Over-cap trim drops reaped guards, never running moves."""
    from racing_sync import rclone_ops

    live_holder = type("P", (), {"returncode": None})()
    reaped_holder = type("P", (), {"returncode": 0})()
    try:
        for i in range(rclone_ops._ACTIVE_MOVES_CAP + 5):
            key = f"/tree/{i}"
            rclone_ops._ACTIVE_MOVES[key] = (
                reaped_holder if i % 2 else live_holder)
        kept_live = [k for k, v in rclone_ops._ACTIVE_MOVES.items()
                     if v is live_holder]
        assert kept_live, "live guards must survive cap pressure"
    finally:
        rclone_ops._ACTIVE_MOVES.clear()


def test_build_move_cmd_rejects_empty_remote(tmp_path: Path):
    import pytest as _pytest

    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    with _pytest.raises(Exception, match="empty dest_remote"):
        build_move_cmd(cfg, tmp_path / "src", "   ")


@pytest.mark.anyio
async def test_empty_source_with_remote_bytes_skips_move(tmp_path: Path):
    """Already-moved bytes: verified skip, no move spawned."""
    from unittest.mock import AsyncMock, patch
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import move_local_to_remote

    src = tmp_path / "src"
    src.mkdir()
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    async def _fake_run(cfg_, cmd, **kwargs):
        assert cmd[0] == "lsf"
        from racing_sync.rclone_ops import RcloneResult
        return RcloneResult(returncode=0, stdout="Top/a.mkv\n", stderr="",
                            duration=0.1)

    with patch.object(rclone_ops, "run_rclone", AsyncMock(side_effect=_fake_run)) as _m:
        res = await move_local_to_remote(
            cfg, src, "remote:dest", files_from=["Top/a.mkv"])
    assert res.ok
    assert _m.await_count == 1  # lsf only; no move spawned


@pytest.mark.anyio
async def test_empty_source_without_remote_bytes_fails_loud(tmp_path: Path):
    """Source gone + remote empty: RcloneError, never silent success."""
    import pytest as _pytest
    from unittest.mock import AsyncMock, patch
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import RcloneError, move_local_to_remote

    src = tmp_path / "src"
    src.mkdir()
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    async def _fake_run(cfg_, cmd, **kwargs):
        from racing_sync.rclone_ops import RcloneResult
        return RcloneResult(returncode=0, stdout="", stderr="", duration=0.1)

    with patch.object(rclone_ops, "run_rclone", AsyncMock(side_effect=_fake_run)):
        with _pytest.raises(RcloneError, match="refusing silent no-op"):
            await move_local_to_remote(
                cfg, src, "remote:dest", files_from=["Top/a.mkv"])


@pytest.mark.anyio
async def test_move_cmd_carries_error_on_no_transfer(tmp_path: Path):
    """Non-empty moves fail loudly when nothing transfers."""
    from unittest.mock import AsyncMock, patch
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import move_local_to_remote

    src = tmp_path / "src"
    src.mkdir()
    (src / "a.mkv").write_bytes(b"a")
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    cfg.rclone.binary = Path("/usr/bin/rclone")
    cfg.rclone.config_path = None
    cfg.rclone.extra_move_flags = []

    seen = {}

    async def _fake_run(cfg_, cmd, **kwargs):
        seen["cmd"] = list(cmd)
        from racing_sync.rclone_ops import RcloneResult
        return RcloneResult(returncode=0, stdout="", stderr="", duration=0.1)

    with patch.object(rclone_ops, "run_rclone", AsyncMock(side_effect=_fake_run)):
        res = await move_local_to_remote(cfg, src, "remote:dest")
    assert res.ok
    assert "--error-on-no-transfer" in seen["cmd"]


@pytest.mark.anyio
async def test_wipe_reports_partial_failures(tmp_path: Path):

    from racing_sync.rclone_ops import wipe_local_files

    base = tmp_path / "base"
    base.mkdir()
    gone = base / "gone.bin"
    gone.write_bytes(b"x")
    missing = base / "missing.bin"
    assert await wipe_local_files([gone, missing], base_dir=base) == []
    assert not gone.exists()
    outsider = tmp_path / "outside.bin"
    outsider.write_bytes(b"x")
    assert await wipe_local_files([outsider], base_dir=base) == [outsider]
    assert outsider.exists()


def test_parse_rclone_size_and_eta():
    from racing_sync.rclone_ops import _parse_rclone_size, _parse_rclone_eta

    assert _parse_rclone_size("80.5M") == int(80.5 * 1024 ** 2)
    assert _parse_rclone_size("1.2G") == int(1.2 * 1024 ** 3)
    assert _parse_rclone_size("0 B") == 0
    assert _parse_rclone_size("nope") is None
    assert _parse_rclone_eta("3m12s") == 192
    assert _parse_rclone_eta("45s") == 45
    assert _parse_rclone_eta("2h") == 7200
    assert _parse_rclone_eta("-") is None
    assert _parse_rclone_eta("") is None


def test_ingest_progress_line_updates_map():
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import _ingest_progress_line

    try:
        _ingest_progress_line(
            "k",
            b"Transferred: 80.500M / 200.000M, 40%, 10.000M/s, ETA 12s\n")
        prog = rclone_ops._RCLONE_PROGRESS["k"]
        assert prog.percent == 40.0
        assert prog.transferred_bytes == int(80.5 * 1024 ** 2)
        assert prog.total_bytes == int(200.0 * 1024 ** 2)
        assert prog.speed_bps == int(10.0 * 1024 ** 2)
        assert prog.eta_seconds == 12
        # Noise lines never create entries.
        _ingest_progress_line("j", b"2026/01/01 NOTICE: Config file\n")
        assert "j" not in rclone_ops._RCLONE_PROGRESS
    finally:
        rclone_ops._RCLONE_PROGRESS.clear()


def test_stats_flags_inserted_before_positionals(tmp_path: Path):
    from racing_sync.rclone_ops import _with_stats_flags

    cmd = ["rclone", "move", "--stats=9s", "--", "src", "remote:dest"]
    assert _with_stats_flags(cmd) == [
        "rclone", "move", "--stats=9s",
        "--stats=5s", "--stats-one-line", "--stats-log-level", "NOTICE",
        "--", "src", "remote:dest",
    ]


@pytest.mark.anyio
async def test_progress_run_pumps_stats_and_evicts(monkeypatch, tmp_path):
    """A progress move ingests stats lines, keeps stderr, evicts after."""
    import asyncio as _asyncio
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import (
        _tree_key, rclone_counters, run_rclone,
    )

    tree = tmp_path / "Top"
    tree.mkdir()
    proc = MagicMock()
    proc.returncode = 0
    proc.stderr.at_eof.return_value = False
    proc.stderr.readline = AsyncMock(side_effect=[
        b"Transferred: 10.000M / 20.000M, 50%, 5.000M/s, ETA 10s\n",
        b"",
    ])
    proc.stderr.read = AsyncMock(return_value=b"tail\n")
    proc.stdout.read = AsyncMock(return_value=b"")
    proc.wait = AsyncMock(return_value=0)
    monkeypatch.setattr(_asyncio, "create_subprocess_exec",
                        AsyncMock(return_value=proc))
    before = rclone_counters()["moves"]
    try:
        res = await run_rclone(
            _rclone_cfg(), ["rclone", "move", "--", "x", "y"],
            timeout=60, tree=tree, progress=True)
    finally:
        rclone_ops._ACTIVE_MOVES.clear()
        rclone_ops._RCLONE_PROGRESS.clear()
    assert res.ok
    assert res.stderr.endswith("tail\n")
    # Entry evicted on completion (no stale percentages linger).
    assert _tree_key(tree) not in rclone_ops._RCLONE_PROGRESS
    assert rclone_counters()["moves"] == before


@pytest.mark.anyio
async def test_single_file_source_counts_as_having_files(tmp_path: Path):
    """A single-file move source is never 'empty' (regression: scandir
    on a file raised NotADirectoryError, misdiagnosing present files
    as empty and failing healthy single-file moves as silent no-ops)."""
    from racing_sync.rclone_ops import _move_source_has_files

    single = tmp_path / "Show.S01E01.mkv"
    single.write_bytes(b"x" * 16)
    assert await _move_source_has_files(single, None) is True
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert await _move_source_has_files(empty_dir, None) is False
    full_dir = tmp_path / "full"
    full_dir.mkdir()
    (full_dir / "a.mkv").write_bytes(b"x")
    assert await _move_source_has_files(full_dir, None) is True
    assert await _move_source_has_files(tmp_path / "missing", None) is False


@pytest.mark.anyio
async def test_single_file_move_reaches_spawn(tmp_path: Path):
    """End to end: an existing single file proceeds to the move call."""
    from unittest.mock import AsyncMock, patch
    from racing_sync import rclone_ops
    from racing_sync.rclone_ops import RcloneResult, move_local_to_remote

    single = tmp_path / "Show.S01E01.mkv"
    single.write_bytes(b"x" * 16)

    async def _fake_run(cfg_, cmd, **kwargs):
        return RcloneResult(returncode=0, stdout="", stderr="", duration=0.1)

    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(side_effect=_fake_run)) as _m:
        res = await move_local_to_remote(_rclone_cfg(), single,
                                         "remote:dest")
    assert res.ok
    assert _m.await_count == 1  # the move spawned; no refusal, no lsf
