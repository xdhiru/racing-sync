"""Prowlarr client wrapper.

Supports:
  - Indexer lookup by announce URL substring  (via /indexer + cache)
  - Torrent search across an indexer          (via /indexer/{id}/newznab)
  - Torrent download (returns the .torrent bytes for qBittorrent to add)
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote_plus, urlsplit, urlunsplit

import aiohttp

from .config import ProwlarrConfig


def _is_fetchable_http_url(url: str) -> bool:
    """True iff `url` is an http(s) URL safe to fetch an enclosure from.

    Prowlarr itself commonly lives on localhost/LAN (enclosure downloads
    are served by Prowlarr), so loopback and private hosts are explicitly
    allowed. Refused: multicast/unspecified/link-local literals (cloud
    metadata endpoints like 169.254.169.254 live there) and known metadata
    hostnames. No DNS resolution, so no rebinding protection —
    indexers themselves remain trusted infrastructure; this only narrows
    what a compromised/malicious feed entry can make us fetch.
    """
    try:
        from ipaddress import ip_address as _ip

        parts = urlsplit(url or "")
        if parts.scheme not in ("http", "https"):
            return False
        host = (parts.hostname or "").strip().lower().strip("[]")
        if not host:
            return False
        if host in ("metadata.google.internal", "metadata.google",
                    "instance-data", "instance-data-compute"):
            return False
        try:
            ip = _ip(host)
        except ValueError:
            return True
        if ip.is_multicast or ip.is_unspecified or ip.is_link_local:
            return False
        return True
    except Exception:
        return False


def _scrub_url(url: str) -> str:
    """Scrub query parameters from URL to prevent leaking API keys or passkeys in logs."""
    try:
        parts = urlsplit(url)
        if not parts.scheme and not parts.netloc:
            return url
        # Strip userinfo (user:pass@) — never log credentials.
        netloc = parts.hostname or ""
        try:
            if parts.port:
                netloc += f":{parts.port}"
        except ValueError:
            pass
        return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
    except Exception:
        return "<scrubbed_url>"

log = logging.getLogger(__name__)

# Upper bound for one Newznab feed payload (10 MiB): a bloated or
# malicious indexer must not OOM the daemon via an uncapped r.text().
_MAX_FEED_BYTES = 10 * 1024 * 1024


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
        if self._session is not None and not self._session.closed:
            return
        import socket
        try:
            use_v6 = bool(getattr(self._cfg, "use_ipv6", False))
        except Exception:
            use_v6 = False
        # IPv4-only by default (intentional — tracker/DNS on seedboxes is
        # overwhelmingly v4); bounded pool + cached DNS either way.
        self._session = aiohttp.ClientSession(
            base_url=self._cfg.base_url.rstrip("/") + "/",
            timeout=aiohttp.ClientTimeout(total=self._cfg.timeout_seconds),
            connector=aiohttp.TCPConnector(
                family=socket.AF_UNSPEC if use_v6 else socket.AF_INET,
                limit=100,
                limit_per_host=20,
                ttl_dns_cache=300,
            ),
        )
        try:
            await self._refresh_indexers()
        except Exception:
            try:
                await self.close()
            except Exception:
                pass
            raise

    @property
    def _auth_headers(self) -> dict[str, str]:
        key = self._cfg.api_key.get_secret_value() if hasattr(self._cfg.api_key, "get_secret_value") else str(self._cfg.api_key)
        return {"X-Api-Key": key}

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # ---------- indexers ----------

    async def _refresh_indexers(self) -> None:
        async with self._refresh_lock:
            if not self._session:
                raise ProwlarrError("not started")
            last_exc: Exception | None = None
            data: Any = None
            for attempt in range(3):
                try:
                    async with self._session.get("api/v1/indexer", headers=self._auth_headers) as r:
                        r.raise_for_status()
                        data = await r.json()
                    break
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                    last_exc = e
                    if attempt == 2:
                        raise ProwlarrError(f"prowlarr indexer refresh failed: {e}") from e
                    await asyncio.sleep(0.5 * (2 ** attempt))
                except aiohttp.ClientResponseError as e:
                    raise ProwlarrError(f"prowlarr indexer refresh failed: HTTP {e.status}") from e
            if data is None:
                if last_exc:
                    raise ProwlarrError(f"prowlarr indexer refresh failed: {last_exc}") from last_exc
                raise ProwlarrError("prowlarr indexer refresh returned no data")
            if not isinstance(data, list):
                raise ProwlarrError(f"prowlarr indexer refresh returned unexpected shape: {type(data).__name__}")
            # Build temp dicts then swap atomically so readers never see empty.
            # One malformed indexer entry must not abort the whole refresh.
            by_name: dict[str, Indexer] = {}
            by_id: dict[int, Indexer] = {}
            for raw in data:
                try:
                    if not isinstance(raw, dict):
                        continue
                    idx_id = raw["id"]
                    idx_name = raw["name"]
                    if not isinstance(idx_id, int) or not isinstance(idx_name, str):
                        continue
                    caps = raw.get("caps") or {}
                    cats = (caps.get("categories") if isinstance(caps, dict) else None) or {}
                    cats = cats.keys() if isinstance(cats, dict) else []
                    idx = Indexer(
                        id=idx_id,
                        name=idx_name,
                        protocol=raw.get("protocol", "torrent"),
                        enable=raw.get("enable", True),
                        capabilities=list(cats),
                    )
                except (KeyError, TypeError):
                    continue
                by_name[idx.name.lower()] = idx
                by_id[idx.id] = idx
            self._indexers_by_name = by_name
            self._indexers_by_id = by_id
            log.debug("prowlarr: loaded %d indexers", len(self._indexers_by_id))

    def get_indexer_by_name(self, name: str) -> Indexer | None:
        return self._indexers_by_name.get(name.lower())

    def get_download_indexers(self) -> list[Indexer]:
        """All usable download-target indexers, in configured priority order.

        Entries whose name is unknown to Prowlarr or disabled there are
        skipped with a warning; raises only when NOTHING configured is
        usable (fail fast instead of parking every row until max-age).
        """
        wanted = self._cfg.download_indexer_names
        usable: list[Indexer] = []
        missing: list[str] = []
        disabled: list[str] = []
        for name in wanted:
            idx = self.get_indexer_by_name(name)
            if idx is None:
                missing.append(name)
            elif not idx.enable:
                disabled.append(name)
            else:
                usable.append(idx)
        for name in missing:
            log.warning(
                "download-target indexer %r not found in prowlarr; skipping. "
                "Known: %s", name, sorted(self._indexers_by_name),
            )
        for name in disabled:
            log.warning(
                "download-target indexer %r is disabled in prowlarr; skipping",
                name,
            )
        if not usable:
            raise ProwlarrError(
                f"none of the configured download-target indexers {wanted!r} "
                f"is usable (missing={missing}, disabled={disabled}). "
                f"Known: {sorted(self._indexers_by_name)}"
            )
        return usable

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
        if self._cfg.should_skip_title(query):
            log.info("prowlarr: skipping search on %s for %r (matches skip_query_substrings)", indexer.name, query)
            return []
        if not self._session:
            raise ProwlarrError("not started")
        limit = limit or self._cfg.max_results
        params: dict[str, str | int] = {
            "t": "search",
            "q": query,
            "limit": limit,
            "offset": 0,
            "cat": "2000,5000",  # standard movies (2000) and TV (5000) categories
        }
        path = f"api/v1/indexer/{indexer.id}/newznab"
        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                async with self._session.get(path, params=params, headers=self._auth_headers) as r:
                    r.raise_for_status()
                    # Size-cap the feed before decoding: a bloated or
                    # malicious indexer must not OOM the daemon. Non-byte
                    # yields (test doubles) fall back to text().
                    try:
                        raw = await r.content.read(_MAX_FEED_BYTES + 1)
                    except Exception:
                        raw = None
                    if isinstance(raw, (bytes, bytearray)):
                        if len(raw) > _MAX_FEED_BYTES:
                            raise ProwlarrError(
                                f"prowlarr feed from {indexer.name!r} exceeds "
                                f"{_MAX_FEED_BYTES} bytes; refusing")
                        try:
                            text = bytes(raw).decode("utf-8", errors="replace")
                        except Exception as e:
                            raise ProwlarrError(
                                f"prowlarr feed from {indexer.name!r} undecodable: {e}"
                            ) from e
                    else:
                        text = await r.text()
                break
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                last_exc = e
            except aiohttp.ClientResponseError as e:
                # Rate-limit / gateway hiccups are retryable; anything else
                # fails fast so bad queries don't burn 3 attempts.
                if e.status not in (408, 425, 429, 502, 503, 504):
                    raise ProwlarrError(f"prowlarr search on {indexer.name!r} failed: HTTP {e.status}") from e
                last_exc = e
            if attempt == 2:
                raise ProwlarrError(f"prowlarr search on {indexer.name!r} failed: {last_exc}") from last_exc
            await asyncio.sleep(0.5 * (2 ** attempt))
        return _parse_newznab(text, indexer)

    async def download_torrent(self, hit: TorrentHit) -> bytes:
        """Fetch the .torrent bytes for a hit (qBittorrent can accept this directly)."""
        if not self._session:
            raise ProwlarrError("not started")
        safe_url = _scrub_url(hit.download_url)
        parsed = urlsplit(hit.download_url)
        if parsed.scheme not in ("http", "https"):
            raise ProwlarrError(f"invalid or unsafe download_url scheme: {safe_url!r}")
        # Never attach the Prowlarr X-Api-Key to third-party enclosure hosts
        # (it would leak); instead refuse non-routable targets outright.
        # Exception: Prowlarr serving the enclosure itself (same netloc as
        # base_url) requires the key — without it same-host downloads 401.
        if not _is_fetchable_http_url(hit.download_url):
            raise ProwlarrError(f"refusing non-routable download_url: {safe_url!r}")
        headers = None
        try:
            if parsed.netloc.lower() == urlsplit(self._cfg.base_url).netloc.lower():
                headers = self._auth_headers
        except Exception:
            headers = None

        last_exc: Exception | None = None
        data = b""
        for attempt in range(3):
            try:
                async with self._session.get(hit.download_url, headers=headers) as r:
                    r.raise_for_status()
                    content_length = r.headers.get("Content-Length")
                    max_bytes = 20 * 1024 * 1024
                    if content_length:
                        cl = content_length.replace(",", "").strip()
                        if cl.isdigit() and int(cl) > max_bytes:
                            raise ProwlarrError(
                                f"torrent download from {safe_url} exceeds max size: {content_length} bytes"
                            )
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in r.content.iter_chunked(64 * 1024):
                        total += len(chunk)
                        if total > max_bytes:
                            raise ProwlarrError(
                                f"torrent download from {safe_url} exceeded max size of {max_bytes} bytes"
                            )
                        chunks.append(chunk)
                    data = b"".join(chunks)
                break
            except ProwlarrError:
                # Size-cap violations are local verdicts, not retryable.
                raise
            except aiohttp.ClientResponseError as e:
                # Rate-limit / gateway hiccups mirror the search retry set;
                # anything else fails fast.
                if e.status not in (408, 425, 429, 502, 503, 504):
                    raise ProwlarrError(
                        f"torrent download from {safe_url} failed with HTTP status {e.status}"
                    ) from e
                last_exc = e
            except (TimeoutError, asyncio.TimeoutError, aiohttp.ClientError) as e:
                last_exc = e
            if attempt == 2:
                raise ProwlarrError(
                    f"torrent download from {safe_url} failed: {last_exc}"
                ) from last_exc
            delay = 0.5 * (2 ** attempt)
            try:
                retry_after = None
                if isinstance(last_exc, aiohttp.ClientResponseError):
                    retry_after = last_exc.headers.get("Retry-After") if last_exc.headers else None
                if retry_after:
                    delay = min(5.0, float(retry_after))
            except (TypeError, ValueError):
                pass
            await asyncio.sleep(delay)

        if not data.startswith(b"d"):
            raise ProwlarrError(
                f"download from {safe_url} did not return a bencoded torrent "
                f"(first bytes: {data[:8]!r})"
            )
        return data

    # ---------- convenience: query by query string built from a torrent ----------

    async def best_match(
        self,
        query: str,
        *,
        prefer_indexer: Indexer | None = None,
        indexers: list[Indexer] | None = None,
        target_size: int = 0,
    ) -> TorrentHit | None:
        """Search the download-target indexers for the EXACT release.

        Indexers are tried in priority order (an explicit `prefer_indexer`
        first, else the given `indexers`, else all configured usable
        indexers); the first indexer with an exact match wins.

        Cross-seed correctness demands byte-identical content: a same-episode
        different-group release (e.g. `...H.264-Raccoon` vs `...H.264-WebRip`)
        must never be accepted, or the racing torrents would later be pointed
        at foreign bytes. So unlike a similarity ranking, this returns None
        unless a hit matches exactly:

          - normalized title equality (trailing indexer tags and `.torrent`
            / media extensions stripped, case-folded; `.`/`_`/` `/`-runs
            unified so punctuation variants still match, while release-group
            and tag tokens keep discriminating), AND
          - when `target_size > 0`, total size within min(50 MiB, 2%) —
            same rule as `coordinator._matches_release` (kept in sync
            manually; the two modules cannot import each other).

        Among exact matches on one indexer the largest wins (deterministic).
        Returns None when no indexer has the exact release — callers treat
        that as "no cross-seed yet" and park/retry instead of downloading
        the wrong release.
        """
        if self._cfg.should_skip_title(query):
            log.info("prowlarr: skipping best_match for %r (matches skip_query_substrings)", query)
            return None
        if prefer_indexer is not None:
            ordered = [prefer_indexer]
        elif indexers is not None:
            ordered = list(indexers)
        else:
            ordered = self.get_download_indexers()
        for idx in ordered:
            hits = await self.search_indexer(idx, query)
            if not hits:
                continue
            exact = [
                h for h in hits
                if h.download_url
                and _is_exact_release_match(h.title, h.size_bytes, query, target_size)
            ]
            if not exact:
                log.info(
                    "prowlarr: %d hit(s) on %r for %r but none is the exact release; ignoring",
                    len(hits), idx.name, query,
                )
                continue
            exact.sort(key=lambda h: -h.size_bytes)
            return exact[0]
        log.info(
            "prowlarr: no exact release for %r on any of %d download-target indexer(s); ignoring",
            query, len(ordered),
        )
        return None

    async def search_indexers_parallel(
        self,
        indexers: list[Indexer],
        query: str,
    ) -> dict[str, list[TorrentHit]]:
        """Search multiple indexers concurrently and return hits keyed by indexer name (lowercase)."""
        if self._cfg.should_skip_title(query):
            log.info("prowlarr: skipping parallel search for %r (matches skip_query_substrings)", query)
            return {}

        sem = asyncio.Semaphore(4)

        async def _search_one(idx: Indexer) -> tuple[str, list[TorrentHit]]:
            async with sem:
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

def _norm_title_for_match(name: str) -> str:
    """Normalize a release title for exact-release comparison.

    Mirrors `coordinator.normalize_content_name` (trailing `[...]` indexer
    tags and `.torrent`/media extensions stripped, case-folded) and additionally
    unifies `.`/`_`/`-`/space runs so punctuation variants of the same release
    still match. Release-group suffixes (`-Raccoon` vs `-WebRip`) and other
    tokens survive as-is and keep discriminating.
    """
    s = (name or "").strip()
    s = re.sub(r"\.torrent$", "", s, flags=re.IGNORECASE).strip()
    for ext in (".mkv", ".mp4", ".avi", ".ts", ".m4v"):
        if s.lower().endswith(ext):
            s = s[: -len(ext)].strip()
            break
    # Strip AFTER extensions: indexer tags trail the filename
    # ("... [A1B2C3D4].mkv"), so tag stripping must see the bare name.
    # (Deliberately local: coordinator.normalize_content_name keeps the
    # opposite order so differently-tagged releases stay separate rows.)
    s = re.sub(r"\s*\[[^\]]+\]\s*$", "", s).strip()
    s = re.sub(r"[._\- ]+", " ", s).strip()
    return s.lower()


def _is_exact_release_match(hit_title: str, hit_size: int, target_name: str, target_size: int) -> bool:
    """True iff a Prowlarr hit is the same release (not just similar)."""
    return release_title_matches(hit_title, hit_size, target_name, target_size)


def release_title_matches(hit_title: str, hit_size: int, target_name: str, target_size: int) -> bool:
    """Public exact-release predicate shared with the coordinator.

    Same rules as the `best_match` selection gate (normalized title equality
    with unified separators, plus min(50 MiB, 2%) size agreement), so a
    downloaded `.torrent` whose *internal* name/size drifted from its index
    listing is still rejected before its bytes reach any client.
    """
    if _norm_title_for_match(hit_title) != _norm_title_for_match(target_name):
        return False
    from .coordinator_content import size_within_tolerance
    return size_within_tolerance(hit_size, target_size)


def _findtext_ns(item: Any, name: str) -> str:
    """Namespace-blind findtext for title/guid/comments/pubDate.

    channel/item detection above is namespace-blind, but ElementTree
    findtext("title") is namespace-sensitive — default-ns feeds would yield
    empty titles that all compare equal in best_match. Match by local tag.
    """
    try:
        direct = item.findtext(name)
    except Exception:
        direct = None
    if direct:
        return direct
    target = name.split(":")[-1].lower()
    for child in item:
        tag = getattr(child, "tag", None)
        if not isinstance(tag, str):
            continue
        if tag.rpartition("}")[2].split(":")[-1].lower() == target:
            try:
                return (child.text or "")
            except Exception:
                return ""
    return ""


def _parse_newznab(xml_text: str, indexer: Indexer) -> list[TorrentHit]:
    """Tiny newznab XML parser (defusedxml when available, stdlib fallback)."""
    try:
        from defusedxml.ElementTree import fromstring as _fromstring
    except ImportError:
        import xml.etree.ElementTree as _ET

        _fromstring = _ET.fromstring

    # Reject DTD/entity declarations before parsing: stdlib ElementTree
    # expands internal entities without limits (billion-laughs), and the
    # old substring check was case/whitespace-fragile. defusedxml raises
    # EntitiesForbidden instead when one slips through.
    try:
        import re as _re
        if _re.search(r"<!\s*(DOCTYPE|ENTITY)", xml_text, _re.IGNORECASE):
            raise ProwlarrError("untrusted XML contains DTD or entity declaration")
    except ProwlarrError:
        raise
    except Exception:
        pass

    hits: list[TorrentHit] = []
    try:
        root = _fromstring(xml_text)
    except Exception as e:  # noqa: BLE001 (ParseError or EntitiesForbidden)
        raise ProwlarrError(f"invalid newznab response: {e}") from e

    channel = root.find("channel")
    if channel is None:
        # Fall back to namespace-blind search (feeds using a default ns).
        for child in root:
            tag = child.tag if isinstance(child.tag, str) else ""
            if tag.rpartition("}")[2].split(":")[-1] == "channel":
                channel = child
                break
    if channel is None:
        return hits

    for item in list(channel):
        item_tag = item.tag if isinstance(item.tag, str) else ""
        if item_tag.rpartition("}")[2].split(":")[-1] != "item":
            continue
        try:
            enclosure = None
            for child in item:
                ctag = child.tag if isinstance(child.tag, str) else ""
                if ctag.rpartition("}")[2].split(":")[-1] == "enclosure":
                    enclosure = child
                    break
            attrs = enclosure.attrib if enclosure is not None else {}
            try:
                size = int(float((attrs.get("length") or "0").replace(",", "").strip() or 0))
            except (ValueError, TypeError):
                size = 0
            raw_url = (attrs.get("url") or "").strip()
            magnet_url = _first_attr(item, "torznab:attr", name="magneturl")
            if not magnet_url:
                # Spec/indexers vary case: magnetUrl vs magneturl.
                magnet_url = _first_attr(item, "torznab:attr", name="magnetUrl")
            download_url = ""
            scheme = urlsplit(raw_url).scheme if raw_url else ""
            if raw_url and scheme in ("http", "https"):
                download_url = raw_url
            elif raw_url and scheme == "magnet" and not magnet_url:
                magnet_url = raw_url
            if not download_url and not magnet_url:
                # Unusable for our flows (download_torrent needs http(s));
                # skip instead of emitting a hit that hard-fails later.
                continue
            hits.append(
                TorrentHit(
                    title=(_findtext_ns(item, "title") or "").strip(),
                    guid=(_findtext_ns(item, "guid") or "").strip(),
                    indexer=indexer.name,
                    indexer_id=indexer.id,
                    size_bytes=size,
                    download_url=download_url,
                    magnet_url=magnet_url,
                    info_url=_findtext_ns(item, "comments") or "",
                    publish_date=_findtext_ns(item, "pubDate") or "",
                )
            )
        except Exception as e:
            log.warning("prowlarr: skipping malformed feed item: %s", e)
            continue
    return hits


def _first_attr(item: Any, tag: str, *, name: str) -> str:
    target_tag = tag.rpartition("}")[2].split(":")[-1].lower()
    target_name = name.lower()
    for child in item:
        child_tag = getattr(child, "tag", None)
        if not isinstance(child_tag, str):
            continue
        if child_tag.rpartition("}")[2].split(":")[-1].lower() != target_tag:
            continue
        attrib = child.attrib or {}
        for k, v in attrib.items():
            if k.lower() == "name" and str(v).lower() == target_name:
                return attrib.get("value", "") or attrib.get("Value", "")
    return ""


def url_quote(s: str) -> str:
    return quote_plus(s)