"""Optional FastAPI control plane.

Endpoints:
  GET  /api/state            -> all rows from state DB
  GET  /api/active           -> only in-flight rows
  GET  /api/logs?limit=N     -> recent run_log rows
  POST /api/recover          -> trigger reconciler now
  POST /api/retry/{hash}     -> FAILED -> QUEUED
  GET  /api/ssd              -> free bytes on the configured SSD path
  POST /api/scan-watch       -> force a watch-dir scan
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from .coordinator import Coordinator
from .recovery import reconcile
from .rclone_ops import ssd_free_bytes
from .state import State, StateStore
from .watchdir import WatchDirScanner

log = logging.getLogger(__name__)


def build_app(coord: Coordinator) -> FastAPI:
    cfg = coord.cfg
    app = FastAPI(title="racing-sync", version="0.1.0")

    def auth(
        x_authenticated_user: str | None = Header(default=None),
        x_api_token: str | None = Header(default=None),
    ) -> str:
        if not cfg.api.enabled:
            raise HTTPException(403, "api disabled")
        if cfg.api.trust_nginx_header and x_authenticated_user:
            return x_authenticated_user
        if cfg.api.api_token and x_api_token == cfg.api.api_token:
            return "token"
        raise HTTPException(401, "auth required")

    @app.get("/api/state", dependencies=[Depends(auth)])
    def state() -> list[dict[str, Any]]:
        return [_ts_to_dict(t) for t in coord.store.all()]

    @app.get("/api/active", dependencies=[Depends(auth)])
    def active() -> list[dict[str, Any]]:
        return [_ts_to_dict(t) for t in coord.store.all_active()]

    @app.get("/api/logs", dependencies=[Depends(auth)])
    def logs(limit: int = 200) -> list[dict[str, Any]]:
        rows = list(coord.store.iter_logs(limit=limit))
        return [dict(r) for r in rows]

    @app.get("/api/ssd", dependencies=[Depends(auth)])
    def ssd() -> dict[str, Any]:
        return {"free_bytes": ssd_free_bytes(cfg), "path": str(cfg.ssd.path)}

    @app.post("/api/recover", dependencies=[Depends(auth)])
    async def recover() -> dict[str, Any]:
        rpt = await reconcile(cfg, dest=coord.dest_client, store=coord.store)
        return rpt.summary()

    @app.post("/api/scan-watch", dependencies=[Depends(auth)])
    async def scan_watch() -> dict[str, Any]:
        if coord.watch is None:
            return {"items": 0, "note": "watch_dir not configured"}
        items = await coord.watch.scan_once()
        return {"items": len(items)}

    class RetryResult(BaseModel):
        source_infohash: str
        new_state: str

    @app.post("/api/retry/{source_infohash}", dependencies=[Depends(auth)])
    async def retry(source_infohash: str) -> RetryResult:
        ts = coord.store.get(source_infohash)
        if ts is None:
            raise HTTPException(404, "unknown hash")
        if ts.state != State.FAILED:
            raise HTTPException(409, f"state is {ts.state.value}")
        coord.store.transition(ts, State.QUEUED, error="")
        return RetryResult(source_infohash=source_infohash, new_state=ts.state.value)

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