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
import time
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

# Brute-force throttle: per-IP auth-failure timestamps (monotonic).
_AUTH_FAILURES: dict[str, list[float]] = {}
_AUTH_WINDOW_S = 60.0
_AUTH_MAX_FAILURES = 10


def _note_auth_failure(ip: str) -> bool:
    """Record an auth failure; True when the IP is now throttled (429)."""
    now = time.monotonic()
    try:
        fails = _AUTH_FAILURES.get(ip)
        if not isinstance(fails, list):
            fails = []
            _AUTH_FAILURES[ip] = fails
        cutoff = now - _AUTH_WINDOW_S
        while fails and fails[0] < cutoff:
            fails.pop(0)
        fails.append(now)
        if len(_AUTH_FAILURES) > 1000:
            for k in list(_AUTH_FAILURES.keys())[:500]:
                _AUTH_FAILURES.pop(k, None)
        return len(fails) > _AUTH_MAX_FAILURES
    except Exception:
        return False


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
    ignored: bool = False
    paired_cancelled: list[str] = []


def build_app(coord: Coordinator) -> FastAPI:
    if not HAS_FASTAPI:
        raise RuntimeError(
            "FastAPI is required to run the control plane API. "
            "Install it via 'pip install racing-sync[api]'."
        )
    cfg = coord.cfg
    # No unauthenticated docs/openapi endpoints: the control plane sits
    # behind token/nginx auth, its schema surface should too.
    app = FastAPI(title="racing-sync", version="0.1.0",
                  docs_url=None, openapi_url=None, redoc_url=None)

    def auth(
        request: Request,
        x_authenticated_user: str | None = Header(default=None),
        x_api_token: str | None = Header(default=None),
    ) -> str:
        if not cfg.api.enabled:
            raise HTTPException(403, "api disabled")
        client_host = request.client.host if request.client else ""
        trusted_proxies = set(getattr(cfg.api, "trusted_proxies", ["127.0.0.1", "::1", "localhost"]))
        if cfg.api.trust_nginx_header and x_authenticated_user and x_authenticated_user.strip():
            if not _host_is_trusted(client_host, trusted_proxies):
                log.warning("api auth rejected for untrusted proxy %r", client_host)
                raise HTTPException(403, "untrusted proxy for nginx auth header")
            return x_authenticated_user.strip()
        try:
            token_str = (
                cfg.api.api_token.get_secret_value()
                if hasattr(cfg.api.api_token, "get_secret_value")
                else str(cfg.api.api_token)
            )
        except Exception:
            token_str = ""
        # Compare stripped UTF-8 bytes: config validation strips for its
        # checks, so compare the same way (a trailing-space token
        # validates but could never authenticate), and non-ASCII tokens
        # must 401, not 500 out of compare_digest.
        try:
            want = (token_str or "").strip().encode("utf-8")
            got = (x_api_token or "").strip().encode("utf-8")
            if want and got and secrets.compare_digest(got, want):
                return "token"
        except Exception:
            pass
        _fail_ip = (client_host or "unknown").strip() or "unknown"
        if _note_auth_failure(_fail_ip):
            log.warning("api auth throttled for %s (too many failures)", _fail_ip)
            raise HTTPException(429, "too many auth failures; backing off")
        log.warning("api auth failure from %s", _fail_ip)
        raise HTTPException(401, "auth required")

    @app.get("/api/state", dependencies=[Depends(auth)])
    async def state(
        limit: int = Query(default=500, ge=1, le=5000),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        # SQL-level pagination: never materialize the whole table for a
        # 50k-row request.
        rows = await asyncio.to_thread(coord.store.all, False, limit, offset)
        return [_ts_to_dict(t) for t in rows]

    @app.get("/api/active", dependencies=[Depends(auth)])
    async def active(
        limit: int = Query(default=500, ge=1, le=5000),
    ) -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(coord.store.all_active, False, limit)
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
        except HTTPException:
            raise
        except Exception as e:  # noqa: BLE001
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
        ignore: bool = Query(default=False),
    ) -> ForgetResult:
        normalized = (source_infohash or "").strip().lower()
        if not _INFOHASH_RE.fullmatch(normalized):
            raise HTTPException(422, "must be 40-char hex infohash")
        async with _hold_ops_lock(coord):
            try:
                result = await forget_torrent(
                    cfg, dest=coord.dest_client, store=coord.store,
                    target=normalized, apply=True, delete_files=delete_files,
                    ignore=ignore,
                )
            except LookupError as e:
                raise HTTPException(404, str(e)) from e
            # Free the SSD budget immediately (forget bypasses transition hooks).
            try:
                await coord._ssd_release(result.get("source_infohash") or normalized)
            except Exception:
                pass
            # Same for the quiet-wait / MOVING-park maps (transition pops
            # bypassed); the admission set reconciles itself via
            # _running_infohashes, and the prune reaps the rest.
            try:
                _gone = (result.get("source_infohash") or normalized).lower()
                _wd = getattr(coord, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop(_gone, None)
                _mp = getattr(coord, "_moving_parks", None)
                if isinstance(_mp, dict):
                    _mp.pop(_gone, None)
            except Exception:
                pass
        paired_hashes: list[str] = []
        try:
            for pair in result.get("paired_cancelled") or []:
                _ph = (pair.get("source_infohash") or "").strip().lower()
                if _ph:
                    paired_hashes.append(_ph)
        except Exception:
            paired_hashes = []
        try:
            for _ph in paired_hashes:
                await coord._ssd_release(_ph)
        except Exception:
            pass
        return ForgetResult(
            source_infohash=result["source_infohash"],
            source_name=result["source_name"],
            applied=result["applied"],
            dest_entries=result["dest_entries"],
            local_paths=result["local_paths"],
            skipped_paths=result["skipped_paths"],
            errors=result["errors"],
            ignored=bool(result.get("ignored", False)),
            paired_cancelled=paired_hashes,
        )

    return app


def _ts_to_dict(t) -> dict[str, Any]:
    try:
        from .logging_setup import sanitize_log_text
        safe_error = sanitize_log_text(str(t.last_error or ""))[:500]
    except Exception:
        safe_error = str(t.last_error or "")[:500]
    try:
        created = t.created_at.isoformat() if t.created_at is not None else ""
    except Exception:
        created = ""
    try:
        updated = t.updated_at.isoformat() if t.updated_at is not None else ""
    except Exception:
        updated = ""
    try:
        state_val = t.state.value
    except Exception:
        state_val = str(getattr(t, "state", "unknown"))
    try:
        name_val = str(t.source_name or "")[:200]
    except Exception:
        name_val = ""
    return {
        "source_infohash": t.source_infohash,
        "dest_infohash": t.dest_infohash,
        "source_name": name_val,
        "classification_kind": t.classification_kind,
        "state": state_val,
        "total_bytes": t.total_bytes,
        "batch_index": t.batch_index,
        "batches_total": t.batches_total,
        "last_error": safe_error,
        "created_at": created,
        "updated_at": updated,
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