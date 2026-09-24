"""Uniform bounds for loop-blocking I/O: timeouts, threads, chunking.

Every client RPC, DB read on the hot path, and filesystem walk goes
through here so a wedged peer (hung qB, dead fuse, stalled disk) degrades
one operation instead of wedging a worker, the tick, or the API:

- :func:`bounded` — `asyncio.wait_for` with a labeled timeout.
  Timeouts surface as builtin `TimeoutError`, which is already in the
  retryable contract (park-and-retry, never silent).
- :func:`offload` — `asyncio.to_thread` for sync SQLite / FS calls that
  would otherwise stall the event loop on a slow disk or WAL lock.
- :func:`chunked` — bound `hashes=` list sizes (nginx 414 + qB URL
  limits bite past a few hundred hashes).
- :func:`rpc_timeout_seconds` — the configured per-RPC budget, with a
  safe fallback for test doubles and partial configs.

Cancellation discipline: `bounded` never swallows `CancelledError`
(`wait_for` only converts its own timeout into `TimeoutError`; an outer
cancel still arrives as `CancelledError`, which callers must not catch
as `Exception` — and `except Exception` never does).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Iterable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_RPC_TIMEOUT_SECONDS = 30.0
MIN_RPC_TIMEOUT_SECONDS = 5.0
MAX_RPC_TIMEOUT_SECONDS = 300.0
DEFAULT_HASH_CHUNK = 100


def rpc_timeout_seconds(cfg: object | None) -> float:
    """Per-RPC timeout budget from config (clamped, fail-safe default)."""
    try:
        raw = getattr(getattr(cfg, "general", None),
                      "client_rpc_timeout_seconds", None)
        if raw is None:
            return DEFAULT_RPC_TIMEOUT_SECONDS
        return max(MIN_RPC_TIMEOUT_SECONDS,
                   min(MAX_RPC_TIMEOUT_SECONDS, float(raw)))
    except (TypeError, ValueError):
        return DEFAULT_RPC_TIMEOUT_SECONDS


async def bounded(awaitable: Awaitable[T], *, timeout: float | None,
                  label: str = "rpc") -> T:
    """Await with a deadline; TimeoutError propagates (retryable)."""
    try:
        seconds = float(timeout if timeout is not None
                        else DEFAULT_RPC_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        seconds = DEFAULT_RPC_TIMEOUT_SECONDS
    if seconds <= 0:
        seconds = DEFAULT_RPC_TIMEOUT_SECONDS
    try:
        return await asyncio.wait_for(awaitable, timeout=seconds)
    except asyncio.TimeoutError as e:
        log.warning("%s timed out after %.0fs", label, seconds)
        raise TimeoutError(f"{label} timed out after {seconds:.0f}s") from e


async def offload(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Run a blocking sync call in a worker thread."""
    return await asyncio.to_thread(fn, *args, **kwargs)


def chunked(items: Iterable[T], size: int = DEFAULT_HASH_CHUNK) -> list[list[T]]:
    """Split into bounded chunks (hash-list URL limits)."""
    try:
        n = max(1, int(size or DEFAULT_HASH_CHUNK))
    except (TypeError, ValueError):
        n = DEFAULT_HASH_CHUNK
    items = list(items or [])
    return [items[i:i + n] for i in range(0, len(items), n)]


__all__ = [
    "DEFAULT_RPC_TIMEOUT_SECONDS",
    "DEFAULT_HASH_CHUNK",
    "rpc_timeout_seconds",
    "bounded",
    "offload",
    "chunked",
]
