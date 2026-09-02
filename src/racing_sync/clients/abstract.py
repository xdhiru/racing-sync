"""Abstract TorrentClient interface used by the coordinator."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterable


@dataclass(slots=True)
class TorrentFile:
    name: str                # path inside the torrent (relative)
    size_bytes: int
    priority: int = 1        # qBittorrent: 0=do not download, 1=normal, 6=high, 7=max


@dataclass(slots=True)
class Torrent:
    hash: str
    name: str
    category: str
    save_path: str
    size_bytes: int
    state: str
    progress: float
    ratio: float = 0.0
    trackers: list[str] = field(default_factory=list)
    files: list[TorrentFile] = field(default_factory=list)
    # Unix timestamp when the torrent was added to the racing client.
    # Used by the coordinator to honour [source].min_age_seconds.
    # 0 means "unknown" (skips the age check).
    added_on: int = 0

    @property
    def infohash(self) -> str:
        return self.hash

    def is_complete(self) -> bool:
        return self.progress >= 0.999


@dataclass(slots=True)
class AddResult:
    hash: str | None
    accepted: bool
    detail: str = ""


class TorrentClient(ABC):
    """Abstract interface for torrent client operations.

    Note: lifecycle methods `start()` and `close()` are intentionally NOT
    declared here. They are provided by the concrete client (e.g.
    `HTTPClientBase`) and abstracting them just creates an MRO conflict
    with multiple-inheritance clients (qB / Deluge) where the concrete
    base defines them.
    """

    @abstractmethod
    async def list_torrents(
        self,
        *,
        category: str | None = None,
        hashes: Iterable[str] | None = None,
    ) -> list[Torrent]: ...

    @abstractmethod
    async def get_torrent(self, torrent_hash: str) -> Torrent | None: ...

    @abstractmethod
    async def get_torrent_files(self, torrent_hash: str) -> list[TorrentFile]: ...

    @abstractmethod
    async def get_trackers(self, torrent_hash: str) -> list[str]: ...

    @abstractmethod
    async def add_torrent(
        self,
        *,
        urls: list[str] | None = None,
        torrent_files: list[bytes] | None = None,
        save_path: str,
        category: str = "",
        paused: bool = True,
        skip_check: bool = False,
        content_layout: str | None = None,
        tags: list[str] | None = None,
    ) -> AddResult: ...

    @abstractmethod
    async def set_file_priorities(
        self, torrent_hash: str, priorities: dict[str, int]
    ) -> None: ...

    @abstractmethod
    async def pause(self, torrent_hash: str) -> None: ...

    @abstractmethod
    async def resume(self, torrent_hash: str) -> None: ...

    @abstractmethod
    async def delete(
        self, torrent_hash: str, *, delete_files: bool = False
    ) -> None: ...

    @abstractmethod
    async def recheck(self, torrent_hash: str) -> None: ...

    @abstractmethod
    async def export_torrent(self, torrent_hash: str) -> bytes: ...