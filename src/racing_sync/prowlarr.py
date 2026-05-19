"""Prowlarr client wrapper.

Supports:
  - Indexer lookup by announce URL substring  (via /indexer + cache)
  - Torrent search across an indexer          (via /indexer/{id}/newznab)
  - Torrent download (returns the .torrent bytes for qBittorrent to add)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus

import aiohttp

from .config import ProwlarrConfig

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Indexer:
    id: int
    name: str
    protocol: str          # "torrent" / "usenet"
    enable: bool
    capabilities: list[str]


@dataclass(slots=True)
class TorrentHit:
    title: str
    guid: str
    indexer: str
    indexer_id: int
    size_bytes: int
    download_url: str       # absolute URL — the .torrent file behind it
    magnet_url: str
    info_url: str
    publish_date: str


class ProwlarrError(RuntimeError):
    pass


class ProwlarrClient:
    """Async client for the Prowlarr HTTP API."""

    def __init__(self, cfg: ProwlarrConfig):
        self._cfg = cfg
        self._session: aiohttp.ClientSession | None = None
        self._indexers_by_name: dict[str, Indexer] = {}
        self._indexers_by_id: dict[int, Indexer] = {}
        self._refresh_lock = asyncio.Lock()

    # ---------- lifecycle ----------

    async def __aenter__(self) -> "ProwlarrClient":
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def start(self) -> None:
        if not self._cfg.enabled:
            raise ProwlarrError("prowlarr disabled in config")
        import socket
        headers = {"X-Api-Key": self._cfg.api_key}
        self._session = aiohttp.ClientSession(
            base_url=self._cfg.base_url.rstrip("/") + "/",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=self._cfg.timeout_seconds),
            connector=aiohttp.TCPConnector(family=socket.AF_INET),
        )
        await self._refresh_indexers()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------- indexers ----------

    async def _refresh_indexers(self) -> None:
        async with self._refresh_lock:
            if not self._session:
                raise ProwlarrError("not started")
            async with self._session.get("/api/v1/indexer") as r:
                r.raise_for_status()
                data = await r.json()
            self._indexers_by_name.clear()
            self._indexers_by_id.clear()
            for raw in data:
                idx = Indexer(
                    id=raw["id"],
                    name=raw["name"],
                    protocol=raw.get("protocol", "torrent"),
                    enable=raw.get("enable", True),
                    capabilities=list(raw.get("caps", {}).get("categories", {}).keys()),
                )
                self._indexers_by_name[idx.name.lower()] = idx
                self._indexers_by_id[idx.id] = idx
            log.debug("prowlarr: loaded %d indexers", len(self._indexers_by_id))

    def get_indexer_by_name(self, name: str) -> Indexer | None:
        return self._indexers_by_name.get(name.lower())

    def get_download_indexer(self) -> Indexer:
        idx = self.get_indexer_by_name(self._cfg.download_indexer)
        if idx is None:
            raise ProwlarrError(
                f"download_indexer {self._cfg.download_indexer!r} not found. "
                f"Known: {sorted(self._indexers_by_name)}"
            )
        if not idx.enable:
            raise ProwlarrError(f"download_indexer {idx.name!r} is disabled in prowlarr")
        return idx

    def resolve_indexer_for_announce(
        self, announce_url: str, tracker_map
    ) -> Indexer | None:
        """Map a torrent's announce URL to a prowlarr indexer."""
        name = tracker_map.resolve(announce_url)
        if not name:
            return None
        return self.get_indexer_by_name(name)

    # ---------- search ----------

    async def search_indexer(
        self,
        indexer: Indexer,
        query: str,
        *,
        limit: int | None = None,
    ) -> list[TorrentHit]:
        """Run a Newznab-style search against a single indexer."""
        if not self._session:
            raise ProwlarrError("not started")
        limit = limit or self._cfg.max_results
        params = {
            "t": "search",
            "q": query,
            "limit": limit,
            "offset": 0,
            "cat": "5000",  # standard "movies/TV/etc" category; prowlarr maps
        }
        path = f"/api/v1/indexer/{indexer.id}/newznab"
        async with self._session.get(path, params=params) as r:
            r.raise_for_status()
            text = await r.text()
        return _parse_newznab(text, indexer)

    async def search_download_indexer(self, query: str) -> list[TorrentHit]:
        idx = self.get_download_indexer()
        return await self.search_indexer(idx, query)

    async def download_torrent(self, hit: TorrentHit) -> bytes:
        """Fetch the .torrent bytes for a hit (qBittorrent can accept this directly)."""
        if not self._session:
            raise ProwlarrError("not started")
        async with self._session.get(hit.download_url) as r:
            r.raise_for_status()
            data = await r.read()
        if not data.startswith(b"d"):
            raise ProwlarrError(
                f"download from {hit.download_url} did not return a bencoded torrent "
                f"(first bytes: {data[:8]!r})"
            )
        return data

    # ---------- convenience: query by query string built from a torrent ----------

    async def best_match(
        self,
        query: str,
        *,
        prefer_indexer: Indexer | None = None,
    ) -> TorrentHit | None:
        """Search the configured download indexer and return the most-relevant hit.

        Relevance heuristic (no fancy ML): exact title match wins, then
        largest size wins. Good enough for cross-seed matching where the
        query is the original torrent name.
        """
        idx = prefer_indexer or self.get_download_indexer()
        hits = await self.search_indexer(idx, query)
        if not hits:
            return None
        ql = query.lower()
        hits.sort(
            key=lambda h: (
                h.title.lower() != ql,             # exact match first
                -h.size_bytes,                    # larger first
            )
        )
        return hits[0]

    async def search_indexers_parallel(
        self,
        indexers: list[Indexer],
        query: str,
    ) -> dict[str, list[TorrentHit]]:
        """Search multiple indexers concurrently and return hits keyed by indexer name (lowercase)."""
        async def _search_one(idx: Indexer) -> tuple[str, list[TorrentHit]]:
            try:
                hits = await self.search_indexer(idx, query)
                return idx.name.lower(), hits
            except Exception as e:  # noqa: BLE001
                log.warning("prowlarr search on indexer %r failed: %s", idx.name, e)
                return idx.name.lower(), []

        tasks = [_search_one(idx) for idx in indexers]
        results = await asyncio.gather(*tasks)
        return dict(results)


# ---------- helpers ----------

def _parse_newznab(xml_text: str, indexer: Indexer) -> list[TorrentHit]:
    """Tiny newznab XML parser. Avoids extra deps; prowlarr responses are simple."""
    import xml.etree.ElementTree as ET

    hits: list[TorrentHit] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ProwlarrError(f"invalid newznab response: {e}") from e

    channel = root.find("channel")
    if channel is None:
        return hits

    for item in channel.findall("item"):
        enclosure = item.find("enclosure")
        attrs = enclosure.attrib if enclosure is not None else {}
        size = int(float(attrs.get("length", "0")))
        hits.append(
            TorrentHit(
                title=(item.findtext("title") or "").strip(),
                guid=(item.findtext("guid") or "").strip(),
                indexer=indexer.name,
                indexer_id=indexer.id,
                size_bytes=size,
                download_url=attrs.get("url", ""),
                magnet_url=_first_attr(item, "torznab:attr", name="magneturl"),
                info_url=item.findtext("comments") or "",
                publish_date=item.findtext("pubDate") or "",
            )
        )
    return hits


def _first_attr(item, tag: str, *, name: str) -> str:
    for child in item.findall(tag):
        if child.attrib.get("name") == name:
            return child.attrib.get("value", "")
    return ""


def url_quote(s: str) -> str:
    return quote_plus(s)