"""Optional FastAPI control plane.

Endpoints:
  GET  /api/state            -> all rows from state DB
  GET  /api/active           -> only in-flight rows
  GET  /api/logs?limit=N     -> recent run_log rows
  POST /api/recover          -> trigger reconciler now
  POST /api/retry/{hash}     -> FAILED -> QUEUED
  POST /api/forget/{hash}    -> abandon a torrent (row + dest entries + SSD data)
  GET  /api/ssd              -> free bytes on the configured SSD path
  POST /api/scan-watch       -> force a watch-dir scan
"""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from pydantic import BaseModel

try:
    from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    Depends = Header = HTTPException = Query = Request = object  # type: ignore[assignment,misc]
    FastAPI = Any  # type: ignore[assignment,misc]

from .coordinator import Coordinator
from .forget import forget_torrent
from .rclone_ops import ssd_free_bytes
from .recovery import reconcile
from .state import State

log = logging.getLogger(__name__)


_INFOHASH_RE = re.compile(r"[0-9a-f]{40}")


def _host_is_trusted(client_host: str, trusted_proxies: set[str]) -> bool:
    """Normalize both sides before comparing.

    `request.client.host` is an IP literal (never the string "localhost")
    and may arrive as IPv6-mapped IPv4 (`::ffff:127.0.0.1`) behind dual-stack
    servers; "localhost" in config means loopback (127.0.0.1/::1).
    """
    host = (client_host or "").strip().lower()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if host.startswith("::ffff:"):
        host = host[7:]
    expanded: set[str] = set()
    for entry in trusted_proxies:
        e = (entry or "").strip().lower().strip("[]")
        if e == "localhost":
            expanded.update({"127.0.0.1", "::1"})
        elif e:
            expanded.add(e)
    return host in expanded


@asynccontextmanager
async def _hold_ops_lock(coord: Coordinator) -> AsyncIterator[None]:
    """Serialize API-triggered ops against the coordinator tick.

    Tolerates hand-built test doubles without a real lock.
    """
    lock = getattr(coord, "_ops_lock", None)
    if lock is None or not hasattr(lock, "__aenter__"):
        yield
    else:
        async with lock:
            yield


class RetryResult(BaseModel):
    source_infohash: str
    new_state: str


class ForgetResult(BaseModel):
    source_infohash: str
    source_name: str
    applied: bool
    dest_entries: list[str]
    local_paths: list[str]
    skipped_paths: list[str]
    errors: list[str]


def build_app(coord: Coordinator) -> FastAPI:
    if not HAS_FASTAPI:
        raise RuntimeError(
            "FastAPI is required to run the control plane API. "
            "Install it via 'pip install racing-sync[api]'."
        )
    cfg = coord.cfg
    app = FastAPI(title="racing-sync", version="0.1.0")

    def auth(
        request: Request,
        x_authenticated_user: str | None = Header(default=None),
        x_api_token: str | None = Header(default=None),
    ) -> str:
        if not cfg.api.enabled:
            raise HTTPException(403, "api disabled")
        client_host = request.client.host if request.client else ""
        trusted_proxies = set(getattr(cfg.api, "trusted_proxies", ["127.0.0.1", "::1", "localhost"]))
        if cfg.api.trust_nginx_header and x_authenticated_user:
            if not _host_is_trusted(client_host, trusted_proxies):
                raise HTTPException(403, "untrusted proxy for nginx auth header")
            return x_authenticated_user
        token_str = (
            cfg.api.api_token.get_secret_value()
            if hasattr(cfg.api.api_token, "get_secret_value")
            else str(cfg.api.api_token)
        )
        if token_str and x_api_token and secrets.compare_digest(x_api_token, token_str):
            return "token"
        raise HTTPException(401, "auth required")

    @app.get("/api/state", dependencies=[Depends(auth)])
    async def state(limit: int = Query(default=5000, ge=1, le=50000)) -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(coord.store.all)
        return [_ts_to_dict(t) for t in rows[:limit]]

    @app.get("/api/active", dependencies=[Depends(auth)])
    async def active() -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(coord.store.all_active)
        return [_ts_to_dict(t) for t in rows]

    @app.get("/api/logs", dependencies=[Depends(auth)])
    async def logs(
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(lambda: list(coord.store.iter_logs(limit=limit)))
        return [dict(r) for r in rows]

    @app.get("/api/ssd", dependencies=[Depends(auth)])
    async def ssd() -> dict[str, Any]:
        try:
            free_bytes = await asyncio.to_thread(ssd_free_bytes, cfg)
        except OSError as e:
            raise HTTPException(503, f"ssd path unavailable: {e}") from e
        return {"free_bytes": free_bytes, "path": str(cfg.ssd.path)}

    @app.post("/api/recover", dependencies=[Depends(auth)])
    async def recover() -> dict[str, Any]:
        async with _hold_ops_lock(coord):
            rpt = await reconcile(cfg, dest=coord.dest_client, store=coord.store)
        return {
            "summary": rpt.summary(),
            "kept": rpt.kept,
            "resumed": rpt.resumed,
            "re_added": rpt.re_added,
            "orphans": rpt.orphans,
            "unknowns": rpt.unknowns,
            "adopted": rpt.adopted,
        }

    @app.post("/api/scan-watch", dependencies=[Depends(auth)])
    async def scan_watch() -> dict[str, Any]:
        if coord.watch is None:
            return {"items": 0, "note": "watch_dir not configured"}
        async with _hold_ops_lock(coord):
            items = await coord.scan_watch()
        return {"items": len(items)}

    @app.post("/api/retry/{source_infohash}", dependencies=[Depends(auth)])
    async def retry(source_infohash: str) -> RetryResult:
        normalized = (source_infohash or "").strip().lower()
        if not _INFOHASH_RE.fullmatch(normalized):
            raise HTTPException(422, "must be 40-char hex infohash")

        def _do_retry() -> str:
            ts = coord.store.get(normalized)
            if ts is None:
                raise HTTPException(404, "unknown hash")
            if ts.state != State.FAILED:
                raise HTTPException(409, f"state is {ts.state.value}")
            ts.failed_retries = 0
            try:
                coord.store.transition(ts, State.QUEUED, error="")
            except ValueError as e:
                # Row moved concurrently (e.g. tick rescheduled it).
                raise HTTPException(409, f"state changed concurrently: {e}") from e
            return ts.state.value

        async with _hold_ops_lock(coord):
            new_state = await asyncio.to_thread(_do_retry)
        return RetryResult(source_infohash=normalized, new_state=new_state)

    @app.post("/api/forget/{source_infohash}", dependencies=[Depends(auth)])
    async def forget(
        source_infohash: str,
        delete_files: bool = Query(default=True),
    ) -> ForgetResult:
        normalized = (source_infohash or "").strip().lower()
        if not _INFOHASH_RE.fullmatch(normalized):
            raise HTTPException(422, "must be 40-char hex infohash")
        async with _hold_ops_lock(coord):
            try:
                result = await forget_torrent(
                    cfg, dest=coord.dest_client, store=coord.store,
                    target=normalized, apply=True, delete_files=delete_files,
                )
            except LookupError as e:
                raise HTTPException(404, str(e)) from e
        return ForgetResult(
            source_infohash=result["source_infohash"],
            source_name=result["source_name"],
            applied=result["applied"],
            dest_entries=result["dest_entries"],
            local_paths=result["local_paths"],
            skipped_paths=result["skipped_paths"],
            errors=result["errors"],
        )

    return app


def _ts_to_dict(t) -> dict[str, Any]:
    return {
        "source_infohash": t.source_infohash,
        "dest_infohash": t.dest_infohash,
        "source_name": t.source_name,
        "classification_kind": t.classification_kind,
        "state": t.state.value,
        "total_bytes": t.total_bytes,
        "batch_index": t.batch_index,
        "batches_total": t.batches_total,
        "last_error": t.last_error,
        "created_at": t.created_at.isoformat(),
        "updated_at": t.updated_at.isoformat(),
    }


async def serve(coord: Coordinator) -> None:
    import uvicorn
    cfg = coord.cfg.api
    app = build_app(coord)
    config = uvicorn.Config(
        app, host=cfg.host, port=cfg.port, log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()