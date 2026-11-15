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


def _scrub_add_url(u: object) -> str:
    """Host-only rendering of an add URL for errors/logs.

    URLs can carry passkeys (tracker `tr=` params) and magnet xt hashes;
    error paths must never echo them.
    """
    try:
        from urllib.parse import urlsplit as _us

        _p = _us(str(u or ""))
        if _p.scheme == "magnet":
            return "magnet:?xt=..."
        if _p.hostname:
            return f"{_p.scheme}://{_p.hostname}/..."
        return f"{(_p.scheme or 'url')}:..."
    except Exception:
        return "<unparsable>"


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
        # qBittorrent v5.0+ renamed pause/resume to stop/start. The 404
        # fallback probes the version once per client instance (source and
        # dest may run different versions): legacy servers must not pay a
        # failed RPC plus retry on every call.
        self._legacy_pause_api: bool | None = None

    async def _do_client_auth(self) -> None:
        async with self.session.post(
            "api/v2/auth/login",
            data={
                "username": self._cfg.username,
                "password": self._cfg.password.get_secret_value()
                if hasattr(self._cfg.password, "get_secret_value")
                else str(self._cfg.password),
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
        "<html",
        "<!doctype",
        "<form",
        "login",
        "password",
        "username",
        "sign in",
        "unauthorized",
    )
    # Bare words ("login", "password", "username") also appear in torrent
    # names/JSON — they only count with an HTML hint in the probe.
    _LOGIN_PAGE_STRONG_MARKERS = (
        "<html",
        "<!doctype",
        "<form",
        "sign in",
        "unauthorized",
    )
    _LOGIN_PAGE_WEAK_MARKERS = ("login", "password", "username")

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
        _need_reauth = False
        try:
            async with await self.request(method, path, **kwargs) as r:
                try:
                    return await r.json()
                except (aiohttp.ContentTypeError, ValueError) as e:
                    # Read the body INSIDE the context: after exit the
                    # response is released and text() goes empty (login
                    # detection miss) or raises.
                    try:
                        _body = await r.text()
                    except Exception:
                        _body = ""
                    _probe = f"{_body or ''} {getattr(e, 'doc', '') or ''}".lower()
                    if not any(m in _probe for m in self._LOGIN_PAGE_STRONG_MARKERS):
                        if "<" not in _probe or not any(
                            m in _probe for m in self._LOGIN_PAGE_WEAK_MARKERS
                        ):
                            raise
                    log.warning(
                        "[%s] %s %s returned login page (expired session?); re-authenticating",
                        self._label,
                        method,
                        path,
                    )
                    _need_reauth = True
        except AuthError:
            raise
        except (aiohttp.ContentTypeError, ValueError):
            # Genuine corrupt JSON (not a login page): re-raised inside.
            raise
        if not _need_reauth:
            # Transport errors from request() propagate untouched — only a
            # login-page payload earns a re-login + retry.
            raise RuntimeError("unreachable: request raised without decode path")
        try:
            self._authed = False
        except Exception:
            pass
        if not await self._login_singleflight(force=True):
            raise AuthError(f"[{self._label}] re-auth failed for {path}")
        async with await self.request(method, path, **kwargs) as r:
            return await r.json()

    # ---- introspection ----

    @staticmethod
    def _hashes_from_blobs(torrent_files: list[bytes] | None) -> list[str]:
        """Infohashes derivable from payload bytes (all decodable blobs).

        Every blob is tried (not just the first): a multi-blob add whose
        later blob is the duplicate must still resolve. Never raises.
        """
        out: list[str] = []
        if not torrent_files:
            return out
        try:
            from ..watchdir import _bencoded_info_hash
        except Exception:
            return out
        for blob in torrent_files:
            try:
                if not isinstance(blob, (bytes, bytearray)) or not blob:
                    continue
                h, _, _, _ = _bencoded_info_hash(bytes(blob))
                if h and h.lower() not in out:
                    out.append(h.lower())
            except Exception:
                continue
        return out

    async def _exists_on_qb(self, infohash: str) -> bool:
        """Single-RPC existence check (no files/trackers fan-out).

        The duplicate-add path only needs to know the entry is there;
        get_torrent() would pay info + files + trackers per candidate.
        """
        rows = await self.list_torrents(hashes=[infohash])
        return bool(rows)

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
            # Chunk hash filters (qB takes pipe-separated hashes; URL
            # limits bite past ~100) and fetch chunks concurrently
            # (4-wide): 10k-hash verify sweeps no longer pay serial
            # round-trips per chunk.
            _sem = asyncio.Semaphore(4)

            async def _one(chunk: list[str]) -> list[Torrent]:
                async with _sem:
                    chunk_params = dict(params)
                    chunk_params["hashes"] = "|".join(chunk)
                    data = await self._request_json(
                        "GET", "/api/v2/torrents/info", params=chunk_params
                    )
                    if isinstance(data, dict):
                        # Error envelope, not a list: no rows to yield.
                        log.warning("qB list_torrents unexpected dict payload; skipping chunk")
                        return []
                    rows: list[Torrent] = []
                    for row in data:
                        try:
                            rows.append(_torrent_from_qb(row))
                        except Exception as e:  # noqa: BLE001
                            log.warning("qB list_torrents skipping bad row: %s", e)
                    return rows

            out: list[Torrent] = []
            for part in await asyncio.gather(
                *(_one(hash_list[i : i + 100]) for i in range(0, len(hash_list), 100))
            ):
                out.extend(part)
            return out
        elif hashes is not None:
            # Explicit empty filter: return nothing instead of everything.
            return []
        data = await self._request_json("GET", "/api/v2/torrents/info", params=params)
        torrents: list[Torrent] = []
        if isinstance(data, dict):
            log.warning("qB list_torrents unexpected dict payload; returning empty")
            return []
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
            "GET", "/api/v2/torrents/files", params={"hash": torrent_hash}
        )
        if not isinstance(data, list):
            # Dict error envelope (or garbage): no files to yield, and
            # iterating a dict would silently produce [] via row.get misses.
            log.warning("qB get_torrent_files unexpected non-list payload; returning empty")
            return []
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
            "GET", "/api/v2/torrents/trackers", params={"hash": torrent_hash}
        )
        urls: list[str] = []
        for row in data if isinstance(data, list) else []:
            try:
                if not isinstance(row, dict):
                    continue
                url = (row.get("url", "") or "").strip()
            except Exception:
                continue
            if url and not url.startswith("**") and url not in urls:
                urls.append(url)
        return urls

    async def get_tracker_messages(self, torrent_hash: str) -> list[str]:
        """Non-empty per-tracker `msg` strings (unregistered/passkey signals)."""
        try:
            data = await self._request_json(
                "GET", "/api/v2/torrents/trackers", params={"hash": torrent_hash}
            )
        except AuthError:
            # Auth expiry must surface for re-login, never read as quiet.
            raise
        except Exception:
            return []
        msgs: list[str] = []
        try:
            rows = data if isinstance(data, list) else []
        except Exception:
            return []
        for row in rows:
            try:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("url", "") or "")
                if url.startswith("**"):
                    # DHT/PeX/LSD pseudo-rows: no tracker verdict here.
                    continue
                msg = str(row.get("msg", "") or "").strip()
                if msg and msg not in msgs:
                    msgs.append(msg)
            except Exception:
                continue
        return msgs

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
        if urls and torrent_files:
            raise ValueError(
                "add_torrent takes urls or torrent_files, not both "
                "(mixed multipart keeps only the last urls part on some "
                "qB versions — split into two calls)"
            )

        if urls:
            # Never hand daemon-local paths or hostless URLs to the
            # qBittorrent daemon: it fetches `urls` server-side (SSRF +
            # file read). Same gate as the Deluge client.
            from urllib.parse import urlsplit as _us

            for u in urls:
                try:
                    _p = _us(u)
                except Exception:
                    raise ValueError(
                        f"qbittorrent add_torrent refusing unparsable URL: {_scrub_add_url(u)}"
                    )
                if _p.scheme not in ("http", "https", "magnet"):
                    raise ValueError(
                        f"qbittorrent add_torrent refusing scheme "
                        f"{_p.scheme!r} (file:// etc. would hand daemon-local "
                        f"paths to the VPS2 daemon)"
                    )
                if _p.scheme in ("http", "https") and not _p.hostname:
                    raise ValueError(
                        f"qbittorrent add_torrent refusing URL without host: {_scrub_add_url(u)}"
                    )

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
            from ..watchdir import MAX_TORRENT_BYTES

            for idx, blob in enumerate(torrent_files):
                if not isinstance(blob, (bytes, bytearray)):
                    raise ValueError(
                        f"torrent payload #{idx} is not bytes ({type(blob).__name__}); refusing"
                    )
                if len(blob) > MAX_TORRENT_BYTES:
                    raise ValueError(f"torrent payload exceeds {MAX_TORRENT_BYTES} bytes; refusing")
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
            async with await self.request("POST", "/api/v2/torrents/add", data=data) as r:
                text = (await r.text()).strip()
        is_hex40 = len(text) == 40 and all(c in "0123456789abcdefABCDEF" for c in text)
        if is_hex40:
            return AddResult(hash=text.lower(), accepted=True, detail=text)
        if len(text) == 64 and all(c in "0123456789abcdefABCDEF" for c in text):
            # v2 (sha256) hash echo: resolve the v1 lookup hash from the
            # .torrent blob when callers supplied one (they usually did —
            # the add carried the bytes), else leave empty as before.
            _blob_hashes = self._hashes_from_blobs(torrent_files)
            return AddResult(
                hash=_blob_hashes[0] if _blob_hashes else None, accepted=True, detail=text
            )
        if text == "Ok." or text == "":
            # Same blob-hash resolution: an accepted add of known bytes
            # reports its hash so callers skip a lookup round-trip.
            _blob_hashes = self._hashes_from_blobs(torrent_files)
            return AddResult(
                hash=_blob_hashes[0] if _blob_hashes else None, accepted=True, detail=text
            )
        if text == "Fails.":
            # qBittorrent returns "Fails." for both invalid torrents and duplicate torrents.
            # Check if the torrent already exists in qBittorrent.
            candidates: list[str] = self._hashes_from_blobs(torrent_files)
            if urls and not candidates:
                for u in urls:
                    m = re.search(r"xt=urn:btih:([A-Za-z0-9]{32,64})", u)
                    if not m:
                        continue
                    raw = m.group(1)
                    if len(raw) == 32:
                        # Ambiguous: 32-char all-hex could be either.
                        # A genuine base32 v1 hash is ~never all-hex
                        # ((16/32)^32), so try hex first — a base32
                        # mis-decode yields a confidently-wrong hash.
                        # (Deliberate, test-locked: never reject here.)
                        if all(c in "0123456789abcdefABCDEF" for c in raw):
                            parsed = raw.lower()
                        else:
                            try:
                                import base64

                                parsed = base64.b32decode(raw.upper()).hex()
                            except Exception:
                                continue
                    elif len(raw) in (40, 64):
                        # v1 hex (40) or v2 hex (64)
                        if all(c in "0123456789abcdefABCDEF" for c in raw):
                            parsed = raw.lower()
                        else:
                            continue
                    else:
                        # 52-char base32 (v1+v2 hybrid) or other lengths
                        try:
                            import base64

                            padded = raw.upper() + "=" * (-len(raw) % 8)
                            parsed = base64.b32decode(padded).hex()
                        except Exception:
                            continue
                    if parsed and parsed not in candidates:
                        candidates.append(parsed)

            for candidate_hash in candidates:
                try:
                    if await self._exists_on_qb(candidate_hash.lower()):
                        log.info(
                            "qB add_torrent returned 'Fails.' but torrent %s already exists",
                            candidate_hash[:10],
                        )
                        return AddResult(
                            hash=candidate_hash.lower(), accepted=True, detail="already added"
                        )
                    if len(candidate_hash) == 64:
                        # v2/hybrid hash qB may index under the v1 prefix:
                        # fall back to the first 40 hex chars (20 bytes).
                        if await self._exists_on_qb(candidate_hash.lower()[:40]):
                            log.info(
                                "qB add_torrent returned 'Fails.' but torrent %s already exists",
                                candidate_hash[:10],
                            )
                            return AddResult(
                                hash=candidate_hash.lower()[:40],
                                accepted=True,
                                detail="already added",
                            )
                except Exception as e:
                    log.debug(
                        "could not check if torrent %s exists after Fails: %s",
                        candidate_hash[:10],
                        e,
                    )

            return AddResult(hash=None, accepted=False, detail=text)

        # Unknown response text (e.g. HTML login page, "Torrent is not valid"):
        # never treat as success — caller must see accepted=False.
        log.warning("qB add_torrent unexpected response: %r", text[:200])
        return AddResult(hash=None, accepted=False, detail=text)

    async def set_file_priorities(self, torrent_hash: str, priorities: dict[str, int]) -> None:
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
                    "set_file_priorities: invalid qB priority %r for %r; expected one of 0,1,6,7",
                    prio,
                    name,
                )
                continue
            prio_to_ids.setdefault(prio, []).append(str(idx))

        if not prio_to_ids:
            raise ValueError(
                f"set_file_priorities: no valid files to update for {torrent_hash} "
                "(caller would otherwise download unfiltered and blow the SSD budget)"
            )

        applied: list[tuple[int, list[str]]] = []
        try:
            for prio, ids in prio_to_ids.items():
                data = aiohttp.FormData()
                data.add_field("hash", torrent_hash)
                data.add_field("id", "|".join(ids))
                data.add_field("priority", str(prio))
                async with await self.request("POST", "/api/v2/torrents/filePrio", data=data) as r:
                    await r.read()
                applied.append((prio, ids))
        except aiohttp.ClientResponseError as e:
            # 404 = entry vanished mid-set (half-applied priorities stay);
            # anything else aborts too. Either way the caller must know
            # exactly what landed: log the rollback picture, then raise.
            log.warning(
                "qB set_file_priorities for %s failed after %d/%d batches "
                "(HTTP %s; applied=%s): caller must re-verify",
                torrent_hash,
                len(applied),
                len(prio_to_ids),
                getattr(e, "status", "?"),
                applied,
            )
            raise

    async def _pause_resume(self, torrent_hash: str, *, pause: bool) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        if self._legacy_pause_api is False:
            async with await self.request(
                "POST", f"/api/v2/torrents/{'stop' if pause else 'start'}", data=data
            ) as r:
                await r.read()
            return
        if self._legacy_pause_api is True:
            async with await self.request(
                "POST", f"/api/v2/torrents/{'pause' if pause else 'resume'}", data=data
            ) as r:
                await r.read()
            return
        try:
            # qBittorrent v5.0+ renamed pause/resume to stop/start
            async with await self.request(
                "POST", f"/api/v2/torrents/{'stop' if pause else 'start'}", data=data
            ) as r:
                await r.read()
            self._legacy_pause_api = False
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                # Legacy qBittorrent (< v5.0) fallback, remembered.
                self._legacy_pause_api = True
                data2 = aiohttp.FormData()
                data2.add_field("hashes", torrent_hash)
                async with await self.request(
                    "POST", f"/api/v2/torrents/{'pause' if pause else 'resume'}", data=data2
                ) as r:
                    await r.read()
            else:
                raise

    async def pause(self, torrent_hash: str) -> None:
        await self._pause_resume(torrent_hash, pause=True)

    async def resume(self, torrent_hash: str) -> None:
        await self._pause_resume(torrent_hash, pause=False)

    async def delete(self, torrent_hash: str, *, delete_files: bool = False) -> None:
        data = aiohttp.FormData()
        data.add_field("hashes", torrent_hash)
        data.add_field("deleteFiles", "true" if delete_files else "false")
        try:
            async with await self.request("POST", "/api/v2/torrents/delete", data=data) as r:
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
            async with await self.request("POST", "/api/v2/torrents/recheck", data=data) as r:
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
        async with await self.request("POST", "/api/v2/torrents/setLocation", data=data) as r:
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
                        f"qB .torrent export for {torrent_hash} exceeds {MAX_TORRENT_BYTES} bytes"
                    )
                chunks.append(chunk)
            data = b"".join(chunks)
            # A bencoded torrent is always a dict: refuse empty/error
            # pages before they travel as "bytes" (callers treat b""
            # as a clean miss, but HTML garbage must fail loudly).
            if not data.startswith(b"d"):
                raise ValueError(f"qB .torrent export for {torrent_hash} is not bencoded data")
            return data

    async def get_properties(self, torrent_hash: str) -> dict[str, Any]:
        return await self._request_json(
            "GET", "/api/v2/torrents/properties", params={"hash": torrent_hash}
        )

    async def piece_state(self, torrent_hash: str) -> list[int]:
        return await self._request_json(
            "GET", "/api/v2/torrents/pieceStates", params={"hash": torrent_hash}
        )


def _split_path(name: str) -> list[str]:
    """Split on both POSIX and Windows separators for cross-platform save_path."""
    return [p for p in re.split(r"[\\/]+", name) if p]


def _torrent_from_qb(d: dict[str, Any]) -> Torrent:
    state = d.get("state") or ("completed" if (d.get("progress", 0) or 0) >= 1.0 else "downloading")
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
        # Fall back to the magnet URI's xt hash when the row carries none
        # (some proxies strip `hash` but keep `magnet_uri`).
        try:
            _mag = str(d.get("magnet_uri") or "")
            _m = re.search(r"xt=urn:btih:([A-Za-z0-9]{32,64})", _mag)
            if _m:
                _raw = _m.group(1)
                if len(_raw) == 40 and all(c in "0123456789abcdefABCDEF" for c in _raw):
                    infohash = _raw.lower()
                elif len(_raw) == 32 and all(c in "0123456789abcdefABCDEF" for c in _raw):
                    pass  # ambiguous MD5-shaped ID: refuse, don't fabricate
                else:
                    try:
                        import base64

                        _padded = _raw.upper() + "=" * (-len(_raw) % 8)
                        infohash = base64.b32decode(_padded).hex()
                    except Exception:
                        infohash = ""
        except Exception:
            infohash = ""
    if not infohash:
        raise ValueError(f"qB torrent row missing infohash: {d!r}")

    def _num(value: object, default: int = 0) -> int:
        try:
            return int(float(value or 0))  # type: ignore[arg-type]
        except (TypeError, ValueError, OverflowError):
            return default

    def _fnum(value: object, default: float = 0.0) -> float:
        try:
            return float(value or 0.0)  # type: ignore[arg-type]
        except (TypeError, ValueError, OverflowError):
            return default

    def _fprogress(value: object) -> float:
        # Torrent progress is a 0..1 fraction; clamp the daemon's value
        # (rogue builds report >1) so completion math can't exceed 100%.
        try:
            return min(1.0, max(0.0, float(value or 0.0)))  # type: ignore[arg-type]
        except (TypeError, ValueError, OverflowError):
            return 0.0

    # qB /torrents/info carries the first tracker URL in `tracker`.
    # Populate it so snapshot list rows are usable without a follow-up
    # get_trackers RPC (the coordinator's source snapshot serves rows
    # with trackers directly). Multi-tracker torrents report only the
    # first here — get_torrent() still fetches the full set, so detail
    # paths are unaffected.
    _first_tracker = d.get("tracker", "") or ""
    try:
        _first_tracker = str(_first_tracker).strip()
    except Exception:
        _first_tracker = ""
    return Torrent(
        hash=infohash,
        name=torrent_name or infohash,
        category=d.get("category", "") or "",
        save_path=sp,
        size_bytes=_num(d.get("size") or d.get("total_size") or 0),
        state=str(state),
        progress=_fprogress(d.get("progress", 0.0)),
        ratio=_fnum(d.get("ratio", 0.0)),
        trackers=[_first_tracker] if _first_tracker else [],
        files=[],
        added_on=_num(d.get("added_on")),
        upspeed_bps=_num(d.get("upspeed")),
        dlspeed_bps=_num(d.get("dlspeed")),
        num_leechers=_num(d.get("num_leechs")),
        num_seeds=_num(d.get("num_seeds")),
        total_uploaded_bytes=_num(d.get("uploaded")),
        seeding_time_seconds=_num(d.get("seeding_time")),
    )


def build_qbtorrent_from_source(cfg: SourceConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="source-qb")


def build_qbtorrent_from_dest(cfg: DestConfig) -> QBittorrentClient:
    return QBittorrentClient(cfg, label="dest-qb")
