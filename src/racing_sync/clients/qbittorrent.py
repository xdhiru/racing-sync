"""qBittorrent WebUI v2 wrapper."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Iterable
from urllib.parse import urlencode

import aiohttp

from ..config import DestConfig, SourceConfig
from .abstract import AddResult, Torrent, TorrentClient, TorrentFile
from .http_base import HTTPClientBase, AuthError

log = logging.getLogger(__name__)


class QBittorrentClient(TorrentClient, HTTPClientBase):
    """Speaks the qBittorrent WebUI v2 API."""

    def __init__(self, cfg: SourceConfig | DestConfig, label: str):
        # Build the HTTPClientConfig from the union
        from ..config import DestConfig, HTTPClientConfig, SourceConfig
        if isinstance(cfg, SourceConfig):
            http_cfg = HTTPClientConfig.from_source(cfg)
        elif isinstance(cfg, DestConfig):
            http_cfg = HTTPClientConfig.from_dest(cfg)
        else:
            http_cfg = cfg  # already an HTTPClientConfig
        HTTPClientBase.__init__(self, http_cfg, label=label)
        self._add_lock = asyncio.Lock()

    async def _do_client_auth(self) -> None:
        async with self.session.post(
            "api/v2/auth/login",
            data={
                "username": self._cfg.username,
                "password": self._cfg.password,
            },
        ) as r:
            if r.status != 200:
                body = await r.text()
                raise AuthError(
                    f"qB login at {self._cfg.host} returned HTTP {r.status}: "
                    f"{body[:200]!r}. Check [dest].username / password and "
                    f"that the WebUI is running."
                )
            text = (await r.text()).strip()
            if text == "Ok." or text == "":
                return
            # qB returns "Fails." on bad credentials.
            raise AuthError(
                f"qB login at {self._cfg.host} rejected credentials "
                f"(user={self._cfg.username!r}): {text!r}. "
                f"Check [dest].username / [dest].password in config.toml. "
                f"If the WebUI has 'Bypass authentication for clients on "
                f"localhost' enabled, this might still fail from non-loopback "
                f"addresses."
            )

    # ---- introspection ----

    async def list_torrents(
        self,
        *,
        category: str | None = None,
        hashes: Iterable[str] | None = None,
    ) -> list[Torrent]:
        params: dict[str, str] = {}
        if category:
            params["category"] = category
            params["filter"] = "all"
        if hashes:
            params["hashes"] = "|".join(hashes)
        async with await self.request("GET", "/api/v2/torrents/info", params=params) as r:
            data = await r.json()
        return [_torrent_from_qb(t) for t in data]

    async def get_torrent(self, torrent_hash: str) -> Torrent | None:
        rows = await self.list_torrents(hashes=[torrent_hash])
        if not rows:
            return None
        t = rows[0]
        # Fill files and trackers for parity
        t.files = await self.get_torrent_files(torrent_hash)
        t.trackers = await self.get_trackers(torrent_hash)
        return t

    async def get_torrent_files(self, torrent_hash: str) -> list[TorrentFile]:
        async with await self.request(
            "GET", "/api/v2/torrents/files", params={"hash": torrent_hash}
        ) as r:
            data = await r.json()
        return [
            TorrentFile(name=row["name"], size_bytes=row["size"], priority=row["priority"])
            for row in data
        ]

    async def get_trackers(self, torrent_hash: str) -> list[str]:
        async with await self.request(
            "GET", "/api/v2/torrents/trackers", params={"hash": torrent_hash}
        ) as r:
            data = await r.json()
        urls: list[str] = []
        for row in data:
            url = row.get("url", "")
            if url and url != "** [DHT] **" and "** [PeX] **" not in url:
                urls.append(url)
        return urls

    # ---- mutation ----

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
    ) -> AddResult:
        if not urls and not torrent_files:
            raise ValueError("add_torrent requires urls or torrent_files")

        fields: dict[str, str] = {
            "savepath": save_path,
            "paused": "true" if paused else "false",
            "skip_checking": "true" if skip_check else "false",
            "autoTMM": "false",
        }
        if category:
            fields["category"] = category
        if content_layout:
            fields["contentLayout"] = content_layout
        if tags:
            fields["tags"] = ",".join(tags)

        data = aiohttp.FormData()
        for k, v in fields.items():
            data.add_field(k, v)
        if urls:
            for u in urls:
                data.add_field("urls", u)
        if torrent_files:
            for idx, blob in enumerate(torrent_files):
                fname = f"torrent_{idx}.torrent"
                data.add_field(
                    "torrents",
                    blob,
                    filename=fname,
                    content_type="application/x-bittorrent",
                )

        async with self._add_lock:
            async with await self.request(
                "POST", "/api/v2/torrents/add", data=data
            ) as r:
                text = (await r.text()).strip()
        if text == "Ok." or text == "":
            return AddResult(hash=None, accepted=True, detail=text)
        if text == "Fails.":
            return AddResult(hash=None, accepted=False, detail=text)
        # Some versions return the new torrent hash on success
        return AddResult(hash=text if len(text) == 40 else None, accepted=True, detail=text)

    async def set_file_priorities(
        self, torrent_hash: str, priorities: dict[str, int]
    ) -> None:
        """Set per-file priorities.

        `priorities` is a {file_name: priority_int}. Internally qB uses file
        indexes, so we look up the index for each file first.
        """
        files = await self.get_torrent_files(torrent_hash)
        index_map = {f.name: i for i, f in enumerate(files)}
        for name, prio in priorities.items():
            idx = index_map.get(name)
            if idx is None:
                log.warning("set_file_priorities: file %r not in torrent", name)
                continue
            data = aiohttp.FormData()
            data.add_field("hash", torrent_hash)
            data.add_field("id", str(idx))
            data.add_field("priority", str(prio))
            async with await self.request(
                "POST", "/api/v2/torrents/filePrio", data=data
            ) as r:
                await r.read()

    async def pause(self, torrent_hash: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        try:
            # qBittorrent v5.0+ renamed pause/resume to stop/start
            async with await self.request("POST", "/api/v2/torrents/stop", data=data) as r:
                await r.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                # Legacy qBittorrent (< v5.0) fallback
                data2 = aiohttp.FormData()
                data2.add_field("hashes", torrent_hash)
                async with await self.request("POST", "/api/v2/torrents/pause", data=data2) as r:
                    await r.read()
            else:
                raise

    async def resume(self, torrent_hash: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        try:
            # qBittorrent v5.0+ renamed pause/resume to stop/start
            async with await self.request("POST", "/api/v2/torrents/start", data=data) as r:
                await r.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                # Legacy qBittorrent (< v5.0) fallback
                data2 = aiohttp.FormData()
                data2.add_field("hashes", torrent_hash)
                async with await self.request("POST", "/api/v2/torrents/resume", data=data2) as r:
                    await r.read()
            else:
                raise

    async def delete(self, torrent_hash: str, *, delete_files: bool = False) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        data.add_field("deleteFiles", "true" if delete_files else "false")
        async with await self.request(
            "POST", "/api/v2/torrents/delete", data=data
        ) as r:
            await r.read()

    async def recheck(self, torrent_hash: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        async with await self.request(
            "POST", "/api/v2/torrents/recheck", data=data
        ) as r:
            await r.read()

    # ---- qB-specific helpers used by the coordinator ----

    async def set_save_path(self, torrent_hash: str, save_path: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hash", torrent_hash)
        data.add_field("location", save_path)
        async with await self.request(
            "POST", "/api/v2/torrents/setLocation", data=data
        ) as r:
            await r.read()

    async def export_torrent(self, torrent_hash: str) -> bytes:
        """Return the .torrent file bytes from qB's own state.

        This is the file we can re-add elsewhere without re-downloading
        metadata.
        """
        async with await self.request(
            "GET", "/api/v2/torrents/export", params={"hash": torrent_hash}
        ) as r:
            return await r.read()

    async def get_properties(self, torrent_hash: str) -> dict[str, Any]:
        async with await self.request(
            "GET", "/api/v2/torrents/properties", params={"hash": torrent_hash}
        ) as r:
            return await r.json()

    async def piece_state(self, torrent_hash: str) -> list[int]:
        async with await self.request(
            "GET", "/api/v2/torrents/pieceStates",
            params={"hash": torrent_hash},
        ) as r:
            return await r.json()


def _torrent_from_qb(d: dict[str, Any]) -> Torrent:
    state = (
        d.get("state")
        or ("completed" if d.get("progress", 0) >= 1.0 else "downloading")
    )
    return Torrent(
        hash=d["hash"],
        name=d["name"],
        category=d.get("category", ""),
        save_path=d.get("save_path") or d.get("content_path") or "",
        size_bytes=int(d.get("size", d.get("total_size", 0)) or 0),
        state=str(state),
        progress=float(d.get("progress", 0.0)),
        ratio=float(d.get("ratio", 0.0)),
        trackers=[],
        files=[],
        added_on=int(d.get("added_on") or 0),
    )


def build_qbtorrent_from_source(cfg: SourceConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="source-qb")


def build_qbtorrent_from_dest(cfg: DestConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="dest-qb")