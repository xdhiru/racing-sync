"""Regression tests for the recursive remote verify (audit finding C1).

`_remote_has_bytes` must list the remote recursively and require every
wanted file, otherwise a replayed move with nested torrent paths reports
data loss while the bytes sit safe on the remote.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from racing_sync import rclone_ops
from racing_sync.config import AppConfig
from racing_sync.rclone_ops import _remote_has_bytes


def _cfg() -> MagicMock:
    cfg = MagicMock(spec=AppConfig)
    cfg.rclone = MagicMock()
    return cfg


def _lsf_result(stdout: str, returncode: int = 0) -> SimpleNamespace:
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr="",
                           duration=0.1)


@pytest.mark.anyio
async def test_nested_path_present_and_recursive_flag():
    """Nested paths are visible because lsf runs with -R; all wanted exist."""
    seen = {}

    async def _fake_run(cfg_, cmd, **kwargs):
        seen["cmd"] = list(cmd)
        return _lsf_result("Show/Season/ep.mkv\n")

    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(side_effect=_fake_run)):
        ok = await _remote_has_bytes(
            _cfg(), "remote:dest", files_from=["Show/Season/ep.mkv"])
    assert ok is True
    assert "-R" in seen["cmd"]


@pytest.mark.anyio
async def test_partial_coverage_is_not_verified():
    """One of two wanted files on the remote must not count as complete."""
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(return_value=_lsf_result(
                          "Show/Season/ep1.mkv\n"))):
        ok = await _remote_has_bytes(
            _cfg(), "remote:dest",
            files_from=["Show/Season/ep1.mkv", "Show/Season/ep2.mkv"])
    assert ok is False


@pytest.mark.anyio
async def test_backslash_entries_are_normalized():
    """Windows-style files_from entries match forward-slash listings."""
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(return_value=_lsf_result(
                          "./Show/Season/ep.mkv\n"))):
        ok = await _remote_has_bytes(
            _cfg(), "remote:dest", files_from=["Show\\Season\\ep.mkv"])
    assert ok is True


@pytest.mark.anyio
async def test_no_file_list_non_empty_listing_means_bytes():
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(return_value=_lsf_result("ep.mkv\n"))):
        assert await _remote_has_bytes(_cfg(), "remote:dest", None) is True


@pytest.mark.anyio
async def test_empty_or_failed_listing_is_not_verified():
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(return_value=_lsf_result(""))):
        assert await _remote_has_bytes(_cfg(), "remote:dest", None) is False
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(return_value=_lsf_result(
                          "Show/Season/ep.mkv\n", returncode=3))):
        assert await _remote_has_bytes(
            _cfg(), "remote:dest", ["Show/Season/ep.mkv"]) is False
    with patch.object(rclone_ops, "run_rclone",
                      AsyncMock(side_effect=RuntimeError("boom"))):
        assert await _remote_has_bytes(
            _cfg(), "remote:dest", ["Show/Season/ep.mkv"]) is False
