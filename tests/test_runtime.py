"""Read-through capability views (`Runtime`) over a loaded AppConfig.

The views exist so call sites ask a capability instead of walking the
section tree. These tests pin the delegation for the smallest and the
fullest fixture, the late-edit behaviour (views must not snapshot), the
precedence rules they absorb, and the route helpers.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import make_coordinator

from racing_sync.config import AppConfig, DelugeSFTPConfig, WatchDirConfig
from racing_sync.runtime import Runtime

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _load(name: str) -> AppConfig:
    return AppConfig.from_toml(FIXTURES / f"{name}.toml")


def _minimal_with(
    tmp_path: Path, *, general: str = "", dest: str = "", rclone: str = "", fuse: str = ""
) -> Path:
    """minimal.toml with extra keys spliced into the given sections."""
    text = (FIXTURES / "minimal.toml").read_text(encoding="utf-8")
    text = text.replace("[general]\n", f"[general]\n{general}\n", 1)
    text = text.replace("[dest]\n", f"[dest]\n{dest}\n", 1)
    text = text.replace("[rclone.remote]\n", f"[rclone]\n{rclone}\n\n[rclone.remote]\n", 1)
    text = text.replace("[rclone.fuse]\n", f"[rclone.fuse]\n{fuse}\n", 1)
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


_COMMON = {
    "intake.min_age_seconds": 0,
    "storage.safety_margin_bytes": 0,
    "storage.disk_safety_margin_bytes": 0,
    "transfer.provider": "rclone",
    "transfer.binary": Path("/usr/local/bin/rclone"),
    "transfer.config_path": None,
    "transfer.extra_flags": [],
    "transfer.batch_extra_flags": [],
    "transfer.max_concurrent": 3,
    "transfer.timeout_seconds": 21600,
    "seed.provider": "path",
    "seed.reinject_delay_seconds": 30,
    "seed.reinject_retry_gap_seconds": 120,
    "seed.reinject_backoff_seconds": 1800,
    "seed.reinject_max_age_seconds": 86400,
    "seed.readd_heal_after_seconds": 7200,
    "seed.manual_move_check_seconds": 1800,
    "seed.manual_move_timeout_seconds": 43200,
    "acquire.provider": "auto",
    "acquire.allow_prowlarr_cross_seed": True,
    "acquire.allow_ssh_export": True,
    "acquire.refetch_public_via_prowlarr": False,
    "acquire.fallback_to_racing_torrent": False,
    "acquire.inject_racing_torrents_to_fuse": True,
    "acquire.pause_public_torrents_on_fuse": True,
    "acquire.prowlarr_max_age_seconds": 86400,
    "acquire.public_export_retry_interval_seconds": 1800,
    "acquire.preferred_copy_grace_seconds": 3600,
    "limits.max_active_downloads": 3,
    "limits.max_concurrent_moves": 3,
    "limits.rpc_timeout_seconds": 30,
    "limits.download_stall_timeout_seconds": 7200,
    "limits.download_max_stall_parks": 3,
    "limits.download_max_check_wait_seconds": 43200,
    "limits.download_check_stall_seconds": 3600,
    "recovery.run_on_startup": True,
    "recovery.auto_retry_failed": True,
    "recovery.max_failed_retries": 3,
    "recovery.batch_size": 50,
    "paths.state_db": Path("/var/lib/racing-sync/state.db"),
    "paths.log_dir": Path("/var/log/racing-sync"),
    "paths.log_retention_days": 14,
}

_MINIMAL = {
    **_COMMON,
    "intake.client_type": "qbittorrent",
    "intake.category": "",
    "storage.path": Path("/downloads"),
    "storage.save_path": Path("/downloads"),
    "storage.max_inflight_bytes": 1000000000,
    "storage.skip_single_file_bytes": 1000000000,
    "transfer.remote_default": "remote:movies/",
    "transfer.remote_unsorted": "remote:unsorted/",
    "seed.mount": Path("/mnt/fuse"),
    "seed.mount_unsorted": Path("/mnt/fuse/unsorted"),
    "indexers.enabled": False,
    "indexers.download_indexer_names": [],
    "indexers.tracker_map_entries": [],
    "acquire.prowlarr_retry_interval_seconds": 1800,
    "limits.source_poll_interval": 120,
    "limits.dest_poll_interval": 30,
    "control.telegram": False,
    "control.api": False,
    "cleanup.enabled": False,
}

_SPLIT = {
    **_COMMON,
    "intake.client_type": "deluge",
    "intake.category": "racing",
    "storage.path": Path("/srv/qbittorrent/data"),
    "storage.save_path": Path("/srv/qbittorrent/data"),
    "storage.max_inflight_bytes": 42949672960,
    "storage.skip_single_file_bytes": 42949672960,
    "transfer.remote_default": "remote:qbittorrent/",
    "transfer.remote_unsorted": "remote:qbittorrent/unsorted/",
    "seed.mount": Path("/mnt/remote/qbittorrent"),
    "seed.mount_unsorted": Path("/mnt/remote/qbittorrent/unsorted"),
    "indexers.enabled": True,
    "indexers.download_indexer_names": ["Seedpool (API)", "TorrentLeech"],
    "indexers.tracker_map_entries": ["Alpha (API)", "Beta"],
    "acquire.prowlarr_retry_interval_seconds": 900,
    "limits.source_poll_interval": 75,
    "limits.dest_poll_interval": 60,
    "control.telegram": True,
    "control.api": True,
    "cleanup.enabled": True,
}


@pytest.mark.parametrize(("name", "expected"), [("minimal", _MINIMAL), ("split", _SPLIT)])
def test_views_delegate_every_attribute(name: str, expected: dict[str, object]):
    rt = Runtime(_load(name))

    for path, value in expected.items():
        view, attr = path.split(".", 1)
        assert getattr(getattr(rt, view), attr) == value, path


def test_views_read_through_after_config_mutation():
    cfg = _load("minimal")
    rt = Runtime(cfg)
    storage = rt.storage
    seed = rt.seed
    acquire = rt.acquire
    limits = rt.limits

    cfg.ssd.path = Path("/mutated/ssd")
    cfg.rclone.fuse.mount = Path("/mutated/fuse")
    cfg.cross_seed.allow_ssh_export = False
    cfg.general.max_active_downloads = 11
    cfg.watch_dir = WatchDirConfig(path=Path("/mutated/watch"))

    assert storage.path == Path("/mutated/ssd")
    assert seed.mount == Path("/mutated/fuse")
    assert acquire.allow_ssh_export is False
    assert limits.max_active_downloads == 11
    assert rt.intake.watch is cfg.watch_dir


def test_views_refuse_writes_and_leave_the_config_untouched():
    cfg = _load("minimal")
    rt = Runtime(cfg)
    before = cfg.model_dump()

    with pytest.raises(AttributeError):
        rt.storage.path = Path("/x")
    with pytest.raises(AttributeError):
        rt.limits.max_active_downloads = 99

    for view in (
        "intake",
        "storage",
        "transfer",
        "seed",
        "indexers",
        "acquire",
        "limits",
        "recovery",
        "control",
        "paths",
        "cleanup",
    ):
        getattr(rt, view)

    assert cfg.ssd.path == Path("/downloads")
    assert cfg.max_active_downloads == 3
    assert cfg.model_dump() == before


def test_cleanup_view_exposes_the_section():
    cfg = _load("split")

    assert Runtime(cfg).cleanup.config is cfg.cleanup


def test_general_limits_override_the_section_values(tmp_path: Path):
    cfg_file = _minimal_with(
        tmp_path,
        general="max_active_downloads = 7\nmax_concurrent_moves = 9",
        dest="max_active_downloads = 4",
        rclone="max_concurrent_moves = 5",
    )

    rt = Runtime(AppConfig.from_toml(cfg_file))

    assert rt.limits.max_active_downloads == 7
    assert rt.limits.max_concurrent_moves == 9


def test_section_limits_apply_without_general_overrides(tmp_path: Path):
    cfg_file = _minimal_with(
        tmp_path, dest="max_active_downloads = 4", rclone="max_concurrent_moves = 5"
    )

    rt = Runtime(AppConfig.from_toml(cfg_file))

    assert rt.limits.max_active_downloads == 4
    assert rt.limits.max_concurrent_moves == 5


@pytest.mark.parametrize(("rclone_delay", "expected"), [(None, 30), (15, 15)])
def test_seed_reinject_delay_prefers_the_rclone_spelling(
    tmp_path: Path, rclone_delay: int | None, expected: int
):
    rclone = "" if rclone_delay is None else f"reinject_delay_seconds = {rclone_delay}"
    cfg_file = _minimal_with(tmp_path, rclone=rclone, fuse="reinject_delay_seconds = 30")

    rt = Runtime(AppConfig.from_toml(cfg_file))

    assert rt.seed.reinject_delay_seconds == expected


def test_transfer_and_seed_route_helpers_on_split():
    rt = Runtime(_load("split"))

    assert rt.transfer.remote_for(episode=True) == "remote:qbittorrent/unsorted/"
    assert rt.transfer.remote_for(episode=False) == "remote:qbittorrent/"
    assert rt.seed.path_for(episode=True) == Path("/mnt/remote/qbittorrent/unsorted")
    assert rt.seed.path_for(episode=False) == Path("/mnt/remote/qbittorrent")
    assert rt.seed.mounts() == [
        Path("/mnt/remote/qbittorrent"),
        Path("/mnt/remote/qbittorrent/unsorted"),
    ]


def test_indexer_helpers_on_split():
    rt = Runtime(_load("split"))

    assert rt.indexers.is_download_indexer("http://tracker.example/seedpool/announce")
    assert rt.indexers.is_download_indexer("http://tracker.example/torrentleech/announce")
    assert not rt.indexers.is_download_indexer("http://tracker.example/other/announce")
    assert not rt.indexers.should_skip_title("Seedpool.Release.1080p")
    assert rt.indexers.download_indexer_names == ["Seedpool (API)", "TorrentLeech"]
    assert rt.indexers.tracker_map_entries == ["Alpha (API)", "Beta"]


def test_intake_optional_intakes_follow_the_source_model():
    minimal = Runtime(_load("minimal"))
    assert minimal.intake.watch is None
    assert minimal.intake.blob_source is None
    assert minimal.intake.sftp_state_dir is None

    split_cfg = _load("split")
    split = Runtime(split_cfg)
    assert split.intake.watch is split_cfg.watch_dir
    assert isinstance(split.intake.blob_source, DelugeSFTPConfig)
    assert split.intake.blob_source is split_cfg.source.deluge_sftp
    assert split.intake.sftp_state_dir == Path("/home/deluge/.config/deluge/state")


def test_coordinator_rt_follows_a_swapped_cfg():
    coord = make_coordinator()
    coord.cfg = _load("minimal")
    assert coord.rt.storage.path == Path("/downloads")

    coord.cfg = _load("split")
    assert coord.rt.storage.path == Path("/srv/qbittorrent/data")
    assert coord.rt.intake.client_type == "deluge"
