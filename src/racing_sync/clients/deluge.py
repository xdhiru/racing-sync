"""Deluge JSON-RPC client wrapper.

Deluge's WebUI exposes a thin JSON-RPC interface at /json. Authentication
requires:
  1) connect to the daemon (POST system.listMethods once for keepalive, etc.)
  2) call auth.login with the daemon password

Real-world Deluge setups differ: some expose the daemon directly, others
require a host:port. We assume the WebUI is reachable on the same host
configured in [source] for racing client.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from typing import Any

from ..config import SourceConfig
from .abstract import AddResult, Torrent, TorrentClient, TorrentFile
from .http_base import HTTPClientBase

log = logging.getLogger(__name__)


def _extract_tracker_urls(raw: list) -> list[str]:
    """Deluge's daemon returns each torrent's trackers as a list of
    `[{url, tier}, ...]` dicts, NOT a list of URL strings. We only
    care about the URLs.
    """
    out: list[str] = []
    for t in raw or []:
        if isinstance(t, dict):
            url = t.get("url") or ""
            if url and url not in out:
                out.append(url)
        elif isinstance(t, str) and t and t not in out:
            out.append(t)
    return out


class DelugeClient(TorrentClient, HTTPClientBase):
    """JSON-RPC client for Deluge."""

    def __init__(self, cfg: SourceConfig):
        from ..config import HTTPClientConfig
        HTTPClientBase.__init__(self, HTTPClientConfig.from_source(cfg),
                                label="source-deluge")
        self._req_id = 0
        self._daemon_password: str | None = getattr(cfg, "deluge_password", None)
        # If you need to set a daemon password, extend SourceConfig.
        # Keep a reference to the SFTP config so we can fall back to
        # reading .torrent files when the daemon RPC is unavailable.
        self._sftp_cfg = (
            cfg.deluge_sftp if cfg.deluge_sftp and cfg.deluge_sftp.enabled
            else None
        )

    async def _do_client_auth(self) -> None:
        # Deluge's WebUI uses the same login endpoint as the daemon. We
        # call auth.login via JSON-RPC directly on self.session.
        # Note: We must NOT call self._rpc here because self.request()
        # would attempt to re-acquire self._auth_lock (deadlock!).
        from .http_base import AuthError
        payload = {
            "method": "auth.login",
            "params": [self._cfg.password.get_secret_value() if hasattr(self._cfg.password, "get_secret_value") else str(self._cfg.password)],
            "id": 1,
        }
        async with self.session.post("json", json=payload) as r:
            if r.status >= 400:
                body = await r.text()
                raise AuthError(
                    f"deluge login HTTP {r.status} at {self._cfg.host}: {body[:200]}"
                )
            data = await r.json()
            if data.get("error"):
                raise AuthError(f"deluge auth error: {data['error']}")
            if data.get("result") is False:
                raise AuthError(
                    f"deluge auth.login at {self._cfg.host} failed (returned False). "
                    f"Check [source].username / [source].password in config.toml."
                )

        # Check if WebUI is connected to a daemon; auto-connect if disconnected
        try:
            check_payload = {"method": "web.connected", "params": [], "id": 2}
            async with self.session.post("json", json=check_payload) as r:
                if r.status == 200:
                    check_data = await r.json()
                    if not check_data.get("result"):
                        hosts_payload = {"method": "web.get_hosts", "params": [], "id": 3}
                        async with self.session.post("json", json=hosts_payload) as hr:
                            if hr.status == 200:
                                hdata = await hr.json()
                                hosts = hdata.get("result") or []
                                if hosts:
                                    host_id = hosts[0][0]
                                    connect_payload = {
                                        "method": "web.connect",
                                        "params": [host_id],
                                        "id": 4,
                                    }
                                    async with self.session.post("json", json=connect_payload) as cr:
                                        await cr.read()
        except Exception as e:
            log.warning("deluge web.connect check failed: %s", e)

    # ---- JSON-RPC plumbing ----

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        self._req_id += 1
        payload = {
            "method": method,
            "params": params,
            "id": self._req_id,
        }
        async with await self.request(
            "POST", "json", json_body=payload
        ) as r:
            data = await r.json()
        if "error" in data and data["error"]:
            raise RuntimeError(f"deluge rpc {method} error: {data['error']}")
        return data.get("result")

    # ---- introspection ----

    async def list_torrents(
        self,
        *,
        category: str | None = None,
        hashes: Iterable[str] | None = None,
    ) -> list[Torrent]:
        # Deluge uses filter_dict: {"label": "racing"} for category
        filt: dict[str, Any] = {}
        if category:
            filt["label"] = category
        if hashes:
            filt["hash"] = list(hashes)
        status_keys = [
            "name",
            "total_size",
            "label",
            "save_path",
            "state",
            "progress",
            "ratio",
            "trackers",
            "time_added",
        ]
        info = await self._rpc("core.get_torrents_status", [filt, status_keys])
        rows = info or {}
        out: list[Torrent] = []
        for h, status in rows.items():
            out.append(
                Torrent(
                    hash=h,
                    name=status.get("name", ""),
                    category=status.get("label", "") or "",
                    save_path=status.get("save_path", "") or "",
                    size_bytes=int(status.get("total_size", 0) or 0),
                    state=status.get("state", ""),
                    progress=min(1.0, max(0.0, float(status.get("progress", 0.0) or 0.0) / 100.0)),
                    ratio=float(status.get("ratio", 0.0)),
                    trackers=_extract_tracker_urls(
                        status.get("trackers", []) or []
                    ),
                    files=[],
                    added_on=int(status.get("time_added", 0) or 0),
                )
            )
        if hashes:
            hash_set = {h.lower() for h in hashes}
            out = [t for t in out if t.hash.lower() in hash_set]
        return out

    async def get_torrent(self, torrent_hash: str) -> Torrent | None:
        rows = await self.list_torrents(hashes=[torrent_hash])
        for t in rows:
            if t.hash.lower() == torrent_hash.lower():
                t.files = await self.get_torrent_files(torrent_hash)
                return t
        return None

    async def get_torrent_files(self, torrent_hash: str) -> list[TorrentFile]:
        """Fetch files for `torrent_hash`.
        
        Tries Deluge's native `core.get_torrent_status(..., ['files', 'file_priorities'])`.
        Falls back to decoding the .torrent file from SFTP if unavailable.
        """
        try:
            status = await self._rpc(
                "core.get_torrent_status",
                [torrent_hash, ["files", "file_priorities", "file_progress"]],
            )
            if status and "files" in status:
                prios = status.get("file_priorities", []) or []
                progs = status.get("file_progress", []) or []
                out: list[TorrentFile] = []
                for item in status["files"]:
                    idx = item.get("index", len(out))
                    prio = prios[idx] if idx < len(prios) else item.get("priority", 1)
                    prog = progs[idx] if idx < len(progs) else 0.0
                    out.append(
                        TorrentFile(
                            name=item.get("path", ""),
                            size_bytes=int(item.get("size", 0)),
                            priority=int(prio),
                            progress=float(prog),
                        )
                    )
                if out:
                    return out
        except Exception as e:
            log.debug("deluge get_torrent_status files failed: %s; falling back to .torrent file", e)
        return await self._files_from_torrent_file(torrent_hash)

    async def _files_from_torrent_file(self, torrent_hash: str) -> list[TorrentFile]:
        """Decode the .torrent file from SFTP and extract its file list.

        Deluge stores .torrent files in `<state_dir>/<hash>.torrent`. We
        already have SFTP access; reuse it. This is also more reliable
        than the daemon's RPC since the .torrent file is immutable and
        the daemon version is irrelevant.
        """
        from ..sftp_source import SFTPExporter
        if not self._sftp_cfg:
            return []
        sftp_cfg = self._sftp_cfg

        def _fetch() -> bytes | None:
            with SFTPExporter(sftp_cfg) as sftp:
                return sftp.fetch_torrent(torrent_hash)

        blob = await asyncio.to_thread(_fetch)
        if not blob:
            return []
        from ..watchdir import _bdecode
        try:
            _, root = _bdecode(blob, 0)
        except Exception as e:  # noqa: BLE001
            log.warning("deluge: failed to bencode .torrent for %s: %s",
                        torrent_hash[:10], e)
            return []
        info = root.get(b"info") if isinstance(root, dict) else None
        if not info:
            return []
        files = info.get(b"files")
        if files:
            # Multi-file mode
            out: list[TorrentFile] = []
            for f in files:
                path = b"/".join(f.get(b"path", [])).decode("utf-8", "replace")
                out.append(TorrentFile(
                    name=path,
                    size_bytes=int(f.get(b"length", 0)),
                    priority=1,
                    progress=0.0,
                ))
            return out
        # Single-file mode
        name = info.get(b"name", b"").decode("utf-8", "replace")
        return [TorrentFile(
            name=name,
            size_bytes=int(info.get(b"length", 0)),
            priority=1,
            progress=0.0,
        )]

    async def get_trackers(self, torrent_hash: str) -> list[str]:
        status = await self._rpc("core.get_torrent_status", [torrent_hash, ["trackers"]])
        if not status:
            return []
        return _extract_tracker_urls(status.get("trackers", []))

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
        opts: dict[str, Any] = {
            "download_location": save_path,
            "add_paused": paused,
            "seed_mode": skip_check,  # "skip hash check on completion"
        }
        if category:
            opts["label"] = category

        results: list[Any] = []
        if torrent_files:
            # Deluge has add_torrent_file (string of base64 or .torrent path).
            # We pass the bytes directly via base64.
            import base64
            for blob in torrent_files:
                encoded = base64.b64encode(blob).decode()
                res = await self._rpc(
                    "core.add_torrent_file", [f"{blob[:6].hex()}.torrent", encoded, opts]
                )
                results.append(res)
        elif urls:
            for u in urls:
                res = await self._rpc("core.add_torrent_url", [u, opts])
                results.append(res)
        else:
            raise ValueError("add_torrent requires urls or torrent_files")

        return AddResult(
            hash=str(results[0]) if results and results[0] else None,
            accepted=bool(results),
            detail=json.dumps([str(r) for r in results]),
        )

    async def set_file_priorities(
        self, torrent_hash: str, priorities: dict[str, int]
    ) -> None:
        try:
            status = await self._rpc(
                "core.get_torrent_status",
                [torrent_hash, ["files", "file_priorities"]],
            )
            if status and "files" in status:
                files = status.get("files", [])
                curr_prios = list(status.get("file_priorities", []))
                if len(curr_prios) < len(files):
                    curr_prios = [1] * len(files)
                name_to_idx = {
                    item.get("path", ""): item.get("index", i)
                    for i, item in enumerate(files)
                }
                for name, prio in priorities.items():
                    if name in name_to_idx:
                        idx = name_to_idx[name]
                        if 0 <= idx < len(curr_prios):
                            curr_prios[idx] = int(prio)
                await self._rpc("core.set_torrent_file_priorities", [torrent_hash, curr_prios])
        except Exception as e:
            log.warning("deluge set_torrent_file_priorities failed for %s: %s", torrent_hash, e)

    async def pause(self, torrent_hash: str) -> None:
        await self._rpc("core.pause_torrent", [torrent_hash])

    async def resume(self, torrent_hash: str) -> None:
        await self._rpc("core.resume_torrent", [torrent_hash])

    async def delete(self, torrent_hash: str, *, delete_files: bool = False) -> None:
        await self._rpc(
            "core.remove_torrent",
            [torrent_hash, bool(delete_files)],
        )

    async def recheck(self, torrent_hash: str) -> None:
        await self._rpc("core.force_recheck", [torrent_hash])

    # ---- deluge-specific ----

    async def export_torrent(self, torrent_hash: str) -> bytes:
        """Ask Deluge to read its .torrent state and return the bytes."""
        import base64

        encoded = await self._rpc("core.get_torrent_file", [torrent_hash])
        if not encoded:
            raise FileNotFoundError(f"deluge has no .torrent for {torrent_hash}")
        return base64.b64decode(encoded)


def build_deluge(cfg: SourceConfig) -> DelugeClient:
    return DelugeClient(cfg)