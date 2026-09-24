"""Central destructive-path guards: one rule, every caller."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from racing_sync.safety import (
    DaemonLock,
    backup_db,
    clear_dir_children,
    daemon_lock_path,
    fuse_roots,
    is_safe_dir_to_clear,
    overlaps_fuse,
    state_db_parent_refusal,
    validate_safe_delete_path,
)


def _cfg(**over):
    cfg = SimpleNamespace(
        general=SimpleNamespace(state_db=str(over.pop("state_db", "/data/app/state.db")),
                                log_dir="/data/app/logs"),
        rclone=SimpleNamespace(fuse=SimpleNamespace(
            mount=over.pop("mount", None), mount_unsorted=over.pop("mount_unsorted", None))),
        ssd=SimpleNamespace(path="/data/ssd"),
        dest=SimpleNamespace(save_path="/data/ssd"),
    )
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def test_validate_refuses_root_and_base_and_outside(tmp_path: Path):
    base = tmp_path / "base"
    base.mkdir()
    with pytest.raises(ValueError, match="filesystem root"):
        validate_safe_delete_path(Path("/"))
    with pytest.raises(ValueError, match="base directory"):
        validate_safe_delete_path(base, base_dir=base)
    with pytest.raises(ValueError, match="not within"):
        validate_safe_delete_path(tmp_path / "elsewhere", base_dir=base)
    validate_safe_delete_path(base / "child", base_dir=base)


def test_is_safe_dir_to_clear_refuses_system_and_cwd(tmp_path: Path):
    assert is_safe_dir_to_clear(Path("/var/log"), "log dir") is not None
    assert is_safe_dir_to_clear(Path("/"), "log dir") is not None
    assert is_safe_dir_to_clear(Path.cwd(), "log dir") is not None
    ok = tmp_path / "app" / "logs"
    ok.mkdir(parents=True)
    assert is_safe_dir_to_clear(ok, "log dir") is None


def test_state_db_parent_refusal_covers_root_and_checkout(tmp_path: Path):
    assert state_db_parent_refusal(_cfg(state_db="/state.db")) is not None
    # Exact system path as the db file itself.
    assert state_db_parent_refusal(_cfg(state_db="/var/log")) is not None
    checkout = tmp_path / "proj"
    checkout.mkdir()
    (checkout / ".git").mkdir()
    assert state_db_parent_refusal(
        _cfg(state_db=str(checkout / "state.db"))) is not None
    good = tmp_path / "data" / "app"
    good.mkdir(parents=True)
    assert state_db_parent_refusal(
        _cfg(state_db=str(good / "state.db"))) is None


def test_overlaps_fuse_both_directions(tmp_path: Path):
    mnt = tmp_path / "mnt"
    mnt.mkdir()
    assert overlaps_fuse(mnt / "sub", [mnt]) is True
    assert overlaps_fuse(tmp_path, [mnt]) is True  # fuse inside dir
    assert overlaps_fuse(tmp_path / "other", [mnt]) is False
    assert overlaps_fuse(mnt, []) is False


def test_fuse_roots_skips_blanks():
    cfg = _cfg(mount="  ", mount_unsorted="/mnt/remote")
    assert fuse_roots(cfg) == [Path("/mnt/remote")]


def test_clear_dir_children_never_root_itself(tmp_path: Path):
    root = tmp_path / "logs"
    root.mkdir()
    (root / "a.log").write_text("x")
    sub = root / "sub"
    sub.mkdir()
    (sub / "b.log").write_text("y")
    lines = clear_dir_children(root, base_desc="log")
    assert root.is_dir()
    assert list(root.iterdir()) == []
    assert any("deleted log entry" in ln for ln in lines)


def test_daemon_lock_exclusive_and_released(tmp_path: Path):
    from racing_sync.state import StateStore

    cfg = _cfg(state_db=str(tmp_path / "state.db"))
    lock_path = daemon_lock_path(cfg)
    assert lock_path is not None and lock_path.parent == tmp_path

    first, second = DaemonLock(lock_path), DaemonLock(lock_path)
    assert first.acquire() is True
    assert first.acquire() is True  # idempotent
    # Second holder (a CLI in another process) is refused while held.
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    second.release()
    # Releasing twice is safe.
    second.release()


def test_backup_db_snapshot_and_prune(tmp_path: Path):
    from racing_sync.state import StateStore

    db = tmp_path / "state.db"
    store = StateStore(db)
    try:
        store.set_meta("k", "v")
    finally:
        store.close()
    cfg = _cfg(state_db=str(db))

    first = backup_db(cfg, tag="pre-reset")
    assert first is not None and Path(first).is_file()
    # A snapshot taken through WAL reads back the data.
    from racing_sync.state import StateStore as S2
    probe = S2(Path(first))
    try:
        assert probe.get_meta("k") == "v"
    finally:
        probe.close()
    # Missing DB → None (nothing to back up).
    assert backup_db(_cfg(state_db=str(tmp_path / "nope.db"))) is None
    # Prune keeps the newest 7.
    for i in range(9):
        p = backup_db(cfg, tag=f"t{i}")
        assert p is not None
    survivors = sorted(db.parent.glob("state.db.*.bak"))
    assert len(survivors) == 7
