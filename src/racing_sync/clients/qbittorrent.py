"""qBittorrent WebUI v2 wrapper."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import re
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
                "password": self._cfg.password.get_secret_value() if hasattr(self._cfg.password, "get_secret_value") else str(self._cfg.password),
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
                f"Check [{self._label}].username / [{self._label}].password in config.toml. "
                f"If the WebUI has 'Bypass authentication for clients on "
                f"localhost' enabled, this might still fail from non-loopback "
                f"addresses."
            )

    _LOGIN_PAGE_MARKERS = (
        "<html", "<!doctype", "<form", "login", "password", "username",
        "sign in", "unauthorized",
    )

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        """Request + JSON-decode with re-auth on login-page responses.

        An expired session can surface as HTTP 200 + login HTML (proxies,
        older WebUI builds) instead of 401/403: r.json() then raises a
        decode error with _authed still True, and every later call fails
        identically until restart. Re-login (single-flight, shared with
        concurrent expiries) and retry once — but ONLY when the payload
        looks like a login page. Genuine corrupt JSON re-raises
        immediately without a wasteful login.
        """
        try:
            async with await self.request(method, path, **kwargs) as r:
                return await r.json()
        except (aiohttp.ContentTypeError, ValueError) as e:
            try:
                _body = await r.text()
            except Exception:
                _body = ""
            _probe = f"{_body or ''} {getattr(e, 'doc', '') or ''}".lower()
            if not any(m in _probe for m in self._LOGIN_PAGE_MARKERS):
                raise
            log.warning(
                "[%s] %s %s returned login page (expired session?); re-authenticating",
                self._label, method, path,
            )
        try:
            self._authed = False
        except Exception:
            pass
        if not await self._login_singleflight(force=True):
            raise AuthError(
                f"[{self._label}] re-auth failed for {path}")
        async with await self.request(method, path, **kwargs) as r:
            return await r.json()

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
        hash_list = list(hashes) if hashes is not None else None
        if hash_list:
            # Chunk large hash filters to avoid 414 URL-too-long on
            # 10k-torrent instances (qB takes pipe-separated hashes).
            out: list[Torrent] = []
            for i in range(0, len(hash_list), 200):
                chunk = hash_list[i : i + 200]
                chunk_params = dict(params)
                chunk_params["hashes"] = "|".join(chunk)
                data = await self._request_json(
                    "GET", "/api/v2/torrents/info", params=chunk_params)
                for row in data:
                    try:
                        out.append(_torrent_from_qb(row))
                    except Exception as e:  # noqa: BLE001
                        log.warning("qB list_torrents skipping bad row: %s", e)
            return out
        elif hashes is not None:
            # Explicit empty filter: return nothing instead of everything.
            return []
        data = await self._request_json(
            "GET", "/api/v2/torrents/info", params=params)
        torrents: list[Torrent] = []
        for row in data:
            try:
                torrents.append(_torrent_from_qb(row))
            except Exception as e:  # noqa: BLE001
                log.warning("qB list_torrents skipping bad row: %s", e)
        return torrents

    async def get_torrent(self, torrent_hash: str) -> Torrent | None:
        rows = await self.list_torrents(hashes=[torrent_hash])
        if not rows:
            return None
        t = rows[0]
        # Concurrently fetch files and trackers to avoid serial RTT latency.
        # Trackers are best-effort: a 404 there must not fail the lookup.
        files, trackers = await asyncio.gather(
            self.get_torrent_files(torrent_hash),
            self.get_trackers(torrent_hash),
            return_exceptions=True,
        )
        if isinstance(files, BaseException):
            raise files
        t.files = files if isinstance(files, list) else []
        t.trackers = trackers if isinstance(trackers, list) else []
        return t

    async def get_torrent_files(self, torrent_hash: str) -> list[TorrentFile]:
        data = await self._request_json(
            "GET", "/api/v2/torrents/files", params={"hash": torrent_hash})
        out: list[TorrentFile] = []
        for row in data:
            try:
                name = row.get("name", "")
                try:
                    size = int(float(row.get("size", 0) or 0))
                except (TypeError, ValueError):
                    size = 0
                try:
                    prog = float(row.get("progress", 0.0) or 0.0)
                except (TypeError, ValueError):
                    prog = 0.0
                out.append(
                    TorrentFile(
                        name=name,
                        size_bytes=size,
                        priority=row.get("priority", 1),
                        progress=prog,
                    )
                )
            except Exception:
                continue
        return out

    async def get_trackers(self, torrent_hash: str) -> list[str]:
        data = await self._request_json(
            "GET", "/api/v2/torrents/trackers", params={"hash": torrent_hash})
        urls: list[str] = []
        for row in data:
            url = (row.get("url", "") or "").strip()
            if url and not url.startswith("**") and url not in urls:
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
            # qB expects a single `urls` field, newline-separated.
            # Sending multiple `urls` parts keeps only the last on some versions.
            data.add_field("urls", "\n".join(urls))
        if torrent_files:
            for idx, blob in enumerate(torrent_files):
                fname = f"torrent_{idx}.torrent"
                data.add_field(
                    "torrents",
                    blob,
                    filename=fname,
                    content_type="application/x-bittorrent",
                )

        # _add_lock covers ONLY the POST: qB serializes adds server-side
        # anyway, and the "Fails." duplicate check below (get_torrent) must
        # stay outside so concurrent adds don't queue behind lookups.
        async with self._add_lock:
            async with await self.request(
                "POST", "/api/v2/torrents/add", data=data
            ) as r:
                text = (await r.text()).strip()
        is_hex40 = len(text) == 40 and all(c in "0123456789abcdefABCDEF" for c in text)
        if is_hex40:
            return AddResult(hash=text.lower(), accepted=True, detail=text)
        if len(text) == 64 and all(c in "0123456789abcdefABCDEF" for c in text):
            # v2 (sha256) hash echo: accept, but leave hash empty — callers
            # resolve the v1 lookup hash from the .torrent blob instead.
            return AddResult(hash=None, accepted=True, detail=text)
        if text == "Ok." or text == "":
            return AddResult(hash=None, accepted=True, detail=text)
        if text == "Fails.":
            # qBittorrent returns "Fails." for both invalid torrents and duplicate torrents.
            # Check if the torrent already exists in qBittorrent.
            candidate_hash: str | None = None
            if torrent_files:
                for blob in torrent_files:
                    try:
                        from ..watchdir import _bencoded_info_hash
                        candidate_hash, _, _, _ = _bencoded_info_hash(blob)
                        break
                    except Exception:
                        candidate_hash = None
                        continue
            elif urls:
                for u in urls:
                    m = re.search(
                        r"xt=urn:btih:([A-Za-z0-9]{32,64})", u
                    )
                    if m:
                        raw = m.group(1)
                        if len(raw) == 32:
                            # 32 chars: base32 (v1) — but 32-char hex also
                            # matches; try b32decode first, fall back to hex.
                            try:
                                import base64
                                candidate_hash = base64.b32decode(raw.upper()).hex()
                            except Exception:
                                candidate_hash = raw.lower()
                        elif len(raw) in (40, 64):
                            # v1 hex (40) or v2 hex (64)
                            if all(c in "0123456789abcdefABCDEF" for c in raw):
                                candidate_hash = raw.lower()
                            else:
                                continue
                        else:
                            # 52-char base32 (v1+v2 hybrid) or other lengths
                            try:
                                import base64
                                padded = raw.upper() + "=" * (-len(raw) % 8)
                                candidate_hash = base64.b32decode(padded).hex()
                            except Exception:
                                continue
                        break

            if candidate_hash:
                try:
                    existing = await self.get_torrent(candidate_hash.lower())
                    if existing is not None:
                        log.info(
                            "qB add_torrent returned 'Fails.' but torrent %s already exists",
                            candidate_hash[:10],
                        )
                        return AddResult(hash=candidate_hash.lower(), accepted=True, detail="already added")
                except Exception as e:
                    log.debug("could not check if torrent %s exists after Fails: %s", candidate_hash[:10], e)

            return AddResult(hash=None, accepted=False, detail=text)

        # Unknown response text (e.g. HTML login page, "Torrent is not valid"):
        # never treat as success — caller must see accepted=False.
        log.warning("qB add_torrent unexpected response: %r", text[:200])
        return AddResult(hash=None, accepted=False, detail=text)

    async def set_file_priorities(
        self, torrent_hash: str, priorities: dict[str, int]
    ) -> None:
        """Set per-file priorities.

        `priorities` is a {file_name: priority_int}. Internally qB uses file
        indexes, so we look up the index for each file first, then batch file
        indices by priority using qBittorrent's piped id format ('0|1|2').
        Valid qB priorities are 0 (skip), 1 (normal), 6 (high), 7 (max).
        """
        files = await self.get_torrent_files(torrent_hash)
        index_map = {f.name: i for i, f in enumerate(files)}
        prio_to_ids: dict[int, list[str]] = {}
        for name, prio in priorities.items():
            idx = index_map.get(name)
            if idx is None:
                log.warning("set_file_priorities: file %r not in torrent", name)
                continue
            if prio not in (0, 1, 6, 7):
                log.warning(
                    "set_file_priorities: invalid qB priority %r for %r; "
                    "expected one of 0,1,6,7",
                    prio, name,
                )
                continue
            prio_to_ids.setdefault(prio, []).append(str(idx))

        if not prio_to_ids:
            log.warning("set_file_priorities: no valid files to update for %s", torrent_hash)
            return

        for prio, ids in prio_to_ids.items():
            data = aiohttp.FormData()
            data.add_field("hash", torrent_hash)
            data.add_field("id", "|".join(ids))
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
        try:
            async with await self.request(
                "POST", "/api/v2/torrents/delete", data=data
            ) as r:
                await r.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                # Already gone: delete is idempotent, not an error.
                return
            raise

    async def recheck(self, torrent_hash: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        try:
            async with await self.request(
                "POST", "/api/v2/torrents/recheck", data=data
            ) as r:
                await r.read()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                # Already gone: nothing to recheck.
                return
            raise

    # ---- qB-specific helpers used by the coordinator ----

    async def set_save_path(self, torrent_hash: str, save_path: str) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        data.add_field("location", save_path)
        async with await self.request(
            "POST", "/api/v2/torrents/setLocation", data=data
        ) as r:
            await r.read()

    async def export_torrent(self, torrent_hash: str) -> bytes:
        """Return the .torrent file bytes from qB's own state.

        This is the file we can re-add elsewhere without re-downloading
        metadata. Stream-capped like the Prowlarr/Deluge download paths so
        a rogue endpoint cannot OOM the daemon with one response.
        """
        from ..watchdir import MAX_TORRENT_BYTES

        async with await self.request(
            "GET", "/api/v2/torrents/export", params={"hash": torrent_hash}
        ) as r:
            chunks: list[bytes] = []
            total = 0
            async for chunk in r.content.iter_chunked(64 * 1024):
                total += len(chunk)
                if total > MAX_TORRENT_BYTES:
                    raise ValueError(
                        f"qB .torrent export for {torrent_hash} exceeds "
                        f"{MAX_TORRENT_BYTES} bytes"
                    )
                chunks.append(chunk)
            return b"".join(chunks)

    async def get_properties(self, torrent_hash: str) -> dict[str, Any]:
        return await self._request_json(
            "GET", "/api/v2/torrents/properties", params={"hash": torrent_hash})

    async def piece_state(self, torrent_hash: str) -> list[int]:
        return await self._request_json(
            "GET", "/api/v2/torrents/pieceStates",
            params={"hash": torrent_hash})


def _split_path(name: str) -> list[str]:
    """Split on both POSIX and Windows separators for cross-platform save_path."""
    return [p for p in re.split(r"[\\/]+", name) if p]


def _torrent_from_qb(d: dict[str, Any]) -> Torrent:
    state = (
        d.get("state")
        or ("completed" if (d.get("progress", 0) or 0) >= 1.0 else "downloading")
    )
    # qB `save_path` is always a directory — never truncate it on suffix
    # alone (that corrupts dotted dirs like `/data/My.Show.S01`). The only
    # exception is the single-file case where save_path itself points at
    # the file (basename == torrent name == content_path basename).
    sp = str(d.get("save_path") or "").strip()
    torrent_name = str(d.get("name") or "").strip()
    cp_raw = str(d.get("content_path") or "").strip()
    if not sp and cp_raw:
        cp_parts = _split_path(cp_raw)
        leading = "/" if cp_raw.startswith("/") else ("\\" if cp_raw.startswith("\\") else "")
        if torrent_name and cp_parts and cp_parts[-1] == torrent_name:
            parent = "/".join(cp_parts[:-1])
            sp = (leading + parent) if parent else (leading or cp_raw)
        else:
            sp = cp_raw
    elif sp and torrent_name and cp_raw:
        if _split_path(sp)[-1:] == [torrent_name] and _split_path(cp_raw)[-1:] == [torrent_name]:
            parent_parts = _split_path(sp)[:-1]
            leading = "/" if sp.startswith("/") else ("\\" if sp.startswith("\\") else "")
            joined = "/".join(parent_parts)
            sp = (leading + joined) if joined else sp
    infohash = str(d.get("hash") or "").strip().lower()
    if not infohash:
        raise ValueError(f"qB torrent row missing infohash: {d!r}")
    def _num(value: object, default: int = 0) -> int:
        try:
            return int(float(value or 0))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default

    def _fnum(value: object, default: float = 0.0) -> float:
        try:
            return float(value or 0.0)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return default

    return Torrent(
        hash=infohash,
        name=torrent_name or infohash,
        category=d.get("category", "") or "",
        save_path=sp,
        size_bytes=_num(d.get("size", d.get("total_size", 0))),
        state=str(state),
        progress=_fnum(d.get("progress", 0.0)),
        ratio=_fnum(d.get("ratio", 0.0)),
        trackers=[],
        files=[],
        added_on=_num(d.get("added_on")),
        upspeed_bps=_num(d.get("upspeed")),
        num_leechers=_num(d.get("num_leechs")),
        total_uploaded_bytes=_num(d.get("uploaded")),
        seeding_time_seconds=_num(d.get("seeding_time")),
    )


def build_qbtorrent_from_source(cfg: SourceConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="source-qb")


def build_qbtorrent_from_dest(cfg: DestConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="dest-qb")