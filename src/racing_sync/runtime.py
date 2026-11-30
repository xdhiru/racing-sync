"""Typed, read-through views of one loaded config (composition root).

Consumers ask a capability for what it needs — `rt.storage.path`,
`rt.transfer.remote_for(episode=True)` — instead of walking a section tree.
Views hold the config and read through on every access, so nothing is
copied and a config swapped late (tests, operators editing before start)
is always the one that answers. None of these views writes.

Precedence rules that used to live in call sites (`[general]` overrides,
the two spellings of the reinject delay) live here once.
"""

from __future__ import annotations

from pathlib import Path

from .chain import Chain
from .config import AppConfig, CleanupConfig, DelugeSFTPConfig, WatchDirConfig


class Runtime:
    """Capability views over one `AppConfig`."""

    __slots__ = ("_cfg",)

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def intake(self) -> IntakeSettings:
        return IntakeSettings(self._cfg)

    @property
    def storage(self) -> StorageSettings:
        return StorageSettings(self._cfg)

    @property
    def transfer(self) -> TransferSettings:
        return TransferSettings(self._cfg)

    @property
    def seed(self) -> SeedSettings:
        return SeedSettings(self._cfg)

    @property
    def indexers(self) -> IndexerSettings:
        return IndexerSettings(self._cfg)

    @property
    def acquire(self) -> AcquireSettings:
        return AcquireSettings(self._cfg)

    @property
    def limits(self) -> LimitsSettings:
        return LimitsSettings(self._cfg)

    @property
    def recovery(self) -> RecoverySettings:
        return RecoverySettings(self._cfg)

    @property
    def control(self) -> ControlSettings:
        return ControlSettings(self._cfg)

    @property
    def paths(self) -> PathsSettings:
        return PathsSettings(self._cfg)

    @property
    def cleanup(self) -> CleanupSettings:
        return CleanupSettings(self._cfg)

    @property
    def chain(self) -> Chain:
        """Which links run today and where each stage hands off."""
        return Chain.from_config(self._cfg)


class IntakeSettings:
    """How racing content is observed."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def client_type(self) -> str:
        return self._cfg.source.type

    @property
    def category(self) -> str:
        return self._cfg.source.category

    @property
    def min_age_seconds(self) -> int:
        return self._cfg.source.min_age_seconds

    @property
    def watch(self) -> WatchDirConfig | None:
        """The watch-dir config, or None when that intake is not configured."""
        return self._cfg.watch_dir

    @property
    def blob_source(self) -> DelugeSFTPConfig | None:
        """Deluge SFTP config when it can export .torrent files, else None."""
        sftp = self._cfg.source.deluge_sftp
        if self._cfg.source.type == "deluge" and sftp is not None and sftp.enabled:
            return sftp
        return None

    @property
    def sftp_state_dir(self) -> Path | None:
        sftp = self.blob_source
        return sftp.state_dir if sftp is not None else None


class StorageSettings:
    """SSD staging: where downloads land and how much may be in flight."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def path(self) -> Path:
        return self._cfg.ssd.path

    @property
    def save_path(self) -> Path:
        """The destination client's staging directory (on `path`)."""
        return self._cfg.dest.save_path

    @property
    def max_inflight_bytes(self) -> int:
        return self._cfg.ssd.max_inflight_bytes

    @property
    def skip_single_file_bytes(self) -> int:
        """A single file larger than this cannot be staged (`0` = no limit)."""
        return self._cfg.ssd.skip_movie_larger_than_bytes

    @property
    def safety_margin_bytes(self) -> int:
        return self._cfg.ssd.safety_margin_bytes

    @property
    def disk_safety_margin_bytes(self) -> int:
        return self._cfg.general.disk_safety_margin_bytes


class TransferSettings:
    """How bytes leave staging (`rclone` today)."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        pipeline = getattr(self._cfg, "pipeline", None)
        on = getattr(pipeline, "transfer", True)
        return bool(on) and getattr(self._cfg.rclone, "remote", None) is not None

    @property
    def provider(self) -> str:
        return "rclone" if self.enabled else "none"

    @property
    def binary(self) -> Path:
        return self._cfg.rclone.binary

    @property
    def config_path(self) -> Path | None:
        return self._cfg.rclone.config_path

    @property
    def remote_default(self) -> str:
        remote = self._cfg.rclone.remote
        return remote.default if remote is not None else ""

    @property
    def remote_unsorted(self) -> str:
        remote = self._cfg.rclone.remote
        return remote.unsorted if remote is not None else ""

    def remote_for(self, *, episode: bool) -> str:
        return self.remote_unsorted if episode else self.remote_default

    @property
    def extra_flags(self) -> list[str]:
        return self._cfg.rclone.extra_move_flags

    @property
    def batch_extra_flags(self) -> list[str]:
        return self._cfg.rclone.batch_move_extra_flags

    @property
    def max_concurrent(self) -> int:
        return self._cfg.max_concurrent_moves

    @property
    def timeout_seconds(self) -> float:
        return self._cfg.rclone.move_timeout_seconds


class SeedSettings:
    """Where finished content is seeded from (a fuse mount or plain dir)."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        pipeline = getattr(self._cfg, "pipeline", None)
        on = getattr(pipeline, "seed", True)
        return bool(on) and getattr(self._cfg.rclone, "fuse", None) is not None

    @property
    def provider(self) -> str:
        return "path" if self.enabled else "none"

    @property
    def mount(self) -> Path | None:
        fuse = self._cfg.rclone.fuse
        return fuse.mount if fuse is not None else None

    @property
    def mount_unsorted(self) -> Path | None:
        fuse = self._cfg.rclone.fuse
        return fuse.mount_unsorted if fuse is not None else None

    def path_for(self, *, episode: bool) -> Path | None:
        return self.mount_unsorted if episode else self.mount

    def mounts(self) -> list[Path]:
        return [path for path in (self.mount, self.mount_unsorted) if path is not None]

    @property
    def reinject_delay_seconds(self) -> int:
        return self._cfg.fuse_reinject_delay_seconds

    @property
    def reinject_retry_gap_seconds(self) -> int:
        return self._cfg.fuse_reinject_retry_gap_seconds

    @property
    def reinject_backoff_seconds(self) -> int:
        return self._cfg.fuse_reinject_backoff_seconds

    @property
    def reinject_max_age_seconds(self) -> int:
        return self._cfg.fuse_reinject_max_age_seconds

    @property
    def readd_heal_after_seconds(self) -> int:
        return self._cfg.fuse_readd_heal_after_seconds

    @property
    def manual_move_check_seconds(self) -> int:
        return self._cfg.rclone.fuse.manual_move_check_seconds

    @property
    def manual_move_timeout_seconds(self) -> int:
        return self._cfg.rclone.fuse.manual_move_timeout_seconds


class IndexerSettings:
    """Which indexers exist and how a release maps onto them."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        return self._cfg.prowlarr.enabled

    @property
    def download_indexer_names(self) -> list[str]:
        return self._cfg.prowlarr.download_indexer_names

    @property
    def tracker_map_entries(self) -> list[str]:
        return list(self._cfg.prowlarr.tracker_map.entries.values())

    def is_download_indexer(self, announce_url: str) -> bool:
        return self._cfg.prowlarr.is_download_indexer(announce_url)

    def should_skip_title(self, title: str) -> bool:
        return self._cfg.prowlarr.should_skip_title(title)


class AcquireSettings:
    """How bytes are acquired for the SSD (indexers, exports, ratio rules)."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        pipeline = getattr(self._cfg, "pipeline", None)
        return bool(getattr(pipeline, "acquire", True))

    @property
    def provider(self) -> str:
        return "auto" if self.enabled else "none"

    @property
    def allow_prowlarr_cross_seed(self) -> bool:
        return self._cfg.cross_seed.allow_prowlarr_cross_seed

    @property
    def allow_ssh_export(self) -> bool:
        return self._cfg.cross_seed.allow_ssh_export

    @property
    def refetch_public_via_prowlarr(self) -> bool:
        return self._cfg.cross_seed.refetch_public_via_prowlarr

    @property
    def fallback_to_racing_torrent(self) -> bool:
        return self._cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout

    @property
    def inject_racing_torrents_to_fuse(self) -> bool:
        return self._cfg.cross_seed.inject_racing_torrents_to_fuse

    @property
    def pause_public_torrents_on_fuse(self) -> bool:
        return self._cfg.cross_seed.pause_public_torrents_on_fuse

    @property
    def prowlarr_retry_interval_seconds(self) -> int:
        return self._cfg.cross_seed.prowlarr_retry_interval_seconds

    @property
    def prowlarr_max_age_seconds(self) -> int:
        return self._cfg.cross_seed.prowlarr_max_age_seconds

    @property
    def public_export_retry_interval_seconds(self) -> int:
        return self._cfg.cross_seed.public_export_retry_interval_seconds

    @property
    def preferred_copy_grace_seconds(self) -> int:
        return self._cfg.general.preferred_copy_grace_seconds


class LimitsSettings:
    """Concurrency, polling and stall budgets."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def max_active_downloads(self) -> int:
        return self._cfg.max_active_downloads

    @property
    def max_concurrent_moves(self) -> int:
        return self._cfg.max_concurrent_moves

    @property
    def source_poll_interval(self) -> int:
        return self._cfg.general.source_poll_interval

    @property
    def dest_poll_interval(self) -> int:
        return self._cfg.general.dest_poll_interval

    @property
    def rpc_timeout_seconds(self) -> int:
        return self._cfg.general.client_rpc_timeout_seconds

    @property
    def download_stall_timeout_seconds(self) -> int:
        return self._cfg.general.download_stall_timeout_seconds

    @property
    def download_max_stall_parks(self) -> int:
        return self._cfg.general.download_max_stall_parks

    @property
    def download_max_check_wait_seconds(self) -> int:
        return self._cfg.general.download_max_check_wait_seconds

    @property
    def download_check_stall_seconds(self) -> int:
        return self._cfg.general.download_check_stall_seconds


class RecoverySettings:
    """Startup reconciliation policy."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def run_on_startup(self) -> bool:
        return self._cfg.recovery.run_on_startup

    @property
    def auto_retry_failed(self) -> bool:
        return self._cfg.recovery.auto_retry_failed

    @property
    def max_failed_retries(self) -> int:
        return self._cfg.recovery.max_failed_retries

    @property
    def batch_size(self) -> int:
        return self._cfg.recovery.batch_size


class ControlSettings:
    """Optional control planes."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def telegram(self) -> bool:
        return self._cfg.telegram.enabled

    @property
    def api(self) -> bool:
        return self._cfg.api.enabled


class PathsSettings:
    """Bookkeeping locations."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def state_db(self) -> Path:
        return self._cfg.general.state_db

    @property
    def log_dir(self) -> Path:
        return self._cfg.general.log_dir

    @property
    def log_retention_days(self) -> int:
        return self._cfg.general.log_retention_days


class CleanupSettings:
    """Source-side cleanup (destructive; off unless enabled)."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    @property
    def enabled(self) -> bool:
        return self._cfg.cleanup.enabled

    @property
    def config(self) -> CleanupConfig:
        return self._cfg.cleanup


__all__ = [
    "AcquireSettings",
    "CleanupSettings",
    "ControlSettings",
    "IndexerSettings",
    "IntakeSettings",
    "LimitsSettings",
    "PathsSettings",
    "RecoverySettings",
    "Runtime",
    "SeedSettings",
    "StorageSettings",
    "TransferSettings",
]
