from __future__ import annotations

from pathlib import Path
import pytest

from racing_sync.rclone_ops import (
    build_move_cmd,
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
