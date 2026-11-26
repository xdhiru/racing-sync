"""I7: one save-path ownership rule for every delete/adoption guard."""

from __future__ import annotations

from unittest.mock import MagicMock

from conftest import make_coordinator

from racing_sync.path_ownership import PathOwner, classify_save_path


def _cfg(tmp_path):
    cfg = MagicMock()
    cfg.rclone.fuse.mount = str(tmp_path / "fuse")
    cfg.rclone.fuse.mount_unsorted = str(tmp_path / "fuse" / "unsorted")
    cfg.dest.save_path = str(tmp_path / "ssd")
    cfg.ssd.path = str(tmp_path / "ssd")
    return cfg


def test_classifies_fuse_ssd_and_foreign(tmp_path):
    cfg = _cfg(tmp_path)
    assert classify_save_path(cfg, str(tmp_path / "fuse")) is PathOwner.FUSE
    assert classify_save_path(cfg, str(tmp_path / "fuse" / "Show")) is PathOwner.FUSE
    assert classify_save_path(cfg, str(tmp_path / "fuse" / "unsorted" / "Show")) is PathOwner.FUSE
    assert classify_save_path(cfg, str(tmp_path / "ssd")) is PathOwner.SSD
    assert classify_save_path(cfg, str(tmp_path / "ssd" / "Show")) is PathOwner.SSD
    assert classify_save_path(cfg, str(tmp_path / "elsewhere")) is PathOwner.FOREIGN


def test_normalizes_separators_and_trailing_slashes(tmp_path):
    cfg = _cfg(tmp_path)
    assert classify_save_path(cfg, str(tmp_path / "fuse") + "/") is PathOwner.FUSE
    assert classify_save_path(cfg, str(tmp_path / "fuse") + "\\\\") is PathOwner.FUSE


def test_prefix_match_requires_a_path_boundary(tmp_path):
    cfg = _cfg(tmp_path)
    assert classify_save_path(cfg, str(tmp_path / "fuse-extra")) is PathOwner.FOREIGN
    assert classify_save_path(cfg, str(tmp_path / "ssd-old")) is PathOwner.FOREIGN


def test_unknown_when_cfg_has_no_usable_roots():
    cfg = MagicMock()  # MagicMock attributes are not str/Path
    assert classify_save_path(cfg, "/mnt/fuse/x") is PathOwner.UNKNOWN
    assert classify_save_path(cfg, "") is PathOwner.UNKNOWN
    assert classify_save_path(cfg, None) is PathOwner.UNKNOWN
    assert classify_save_path(cfg, 123) is PathOwner.UNKNOWN


def test_helpers_delegate_to_the_one_rule(tmp_path):
    from racing_sync.forget import _is_fuse_save_path
    from racing_sync.recovery import _save_path_on_ssd

    cfg = _cfg(tmp_path)
    on_fuse = str(tmp_path / "fuse" / "Show")
    on_ssd = str(tmp_path / "ssd" / "Show")

    assert _save_path_on_ssd(cfg, on_ssd)
    assert not _save_path_on_ssd(cfg, on_fuse)

    coord = make_coordinator(cfg=cfg)
    assert coord._save_path_is_on_fuse(on_fuse)
    assert not coord._save_path_is_on_fuse(on_ssd)

    assert _is_fuse_save_path(cfg, on_fuse)
    assert not _is_fuse_save_path(cfg, on_ssd)
