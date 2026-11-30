"""Capability Protocols the orchestrator depends on.

The coordinator asks for what it needs, not for a concrete class, so a
deployment can swap an implementation without touching pipeline code
(phase 3/4 add the remaining providers). Concrete implementations today:
`clients/*` (source/dest), `sftp_source.SFTPExporter`, `watchdir`.
Indexer *policy* is covered here; its search surface stays concrete until
the acquisition provider lands.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BlobSource(Protocol):
    """Fetch the .torrent bytes for an infohash (SFTP export, API export)."""

    def connect(self) -> None: ...

    def close(self) -> None: ...

    def fetch_torrent(self, infohash: str) -> bytes | None: ...


@runtime_checkable
class WatchIntake(Protocol):
    """Scan a drop directory and retire consumed files."""

    async def scan_once(self) -> list[Any]: ...

    async def delete_picked_up(self, item: Any) -> None: ...


@runtime_checkable
class IndexerPolicy(Protocol):
    """Which indexers exist and how releases map onto them."""

    @property
    def download_indexer_names(self) -> list[str]: ...

    def is_download_indexer(self, announce_url: str) -> bool: ...

    def should_skip_title(self, title: str) -> bool: ...


__all__ = ["BlobSource", "IndexerPolicy", "WatchIntake"]
