"""Composition root: build the concrete services one config implies.

Exactly one place knows which client class, indexer client, blob source
and intake scanner a config produces. Nothing here performs I/O — the
coordinator still starts each service in its historical order with its
own backoff — so composing cannot change startup behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass

from .capabilities import BlobSource, WatchIntake
from .clients.abstract import TorrentClient
from .clients.deluge import DelugeClient
from .clients.qbittorrent import QBittorrentClient
from .config import AppConfig
from .prowlarr import ProwlarrClient
from .runtime import Runtime
from .sftp_source import SFTPExporter
from .watchdir import WatchDirScanner


@dataclass(frozen=True)
class Composition:
    runtime: Runtime
    source: TorrentClient
    dest: TorrentClient
    indexers: ProwlarrClient | None
    blobs: BlobSource | None
    watch: WatchIntake | None


def build_source_client(cfg: AppConfig) -> TorrentClient:
    if cfg.source.type == "qbittorrent":
        return QBittorrentClient(cfg.source, label="source-qb")
    return DelugeClient(cfg.source)


def build_dest_client(cfg: AppConfig) -> TorrentClient:
    return QBittorrentClient(cfg.dest, label="dest-qb")


def build_indexer_service(cfg: AppConfig) -> ProwlarrClient | None:
    if not cfg.prowlarr.enabled:
        return None
    return ProwlarrClient(cfg.prowlarr)


def build_blob_source(cfg: AppConfig) -> SFTPExporter | None:
    sftp = cfg.source.deluge_sftp
    if cfg.source.type != "deluge" or sftp is None or not sftp.enabled:
        # qBittorrent sources export through their own WebUI endpoint.
        return None
    return SFTPExporter(sftp)


def build_watch_intake(cfg: AppConfig, indexers: ProwlarrClient | None) -> WatchDirScanner | None:
    if cfg.watch_dir is None:
        return None
    return WatchDirScanner(cfg.watch_dir, indexers)


def compose(cfg: AppConfig) -> Composition:
    """Resolve the service graph for one config (no I/O, nothing started)."""
    indexers = build_indexer_service(cfg)
    return Composition(
        runtime=Runtime(cfg),
        source=build_source_client(cfg),
        dest=build_dest_client(cfg),
        indexers=indexers,
        blobs=build_blob_source(cfg),
        watch=build_watch_intake(cfg, indexers),
    )


__all__ = [
    "Composition",
    "build_blob_source",
    "build_dest_client",
    "build_indexer_service",
    "build_source_client",
    "build_watch_intake",
    "compose",
]
