"""Bounded read-retry policy for worker-side client reads.

Transport retries already live in the clients (http_base retries network
errors + transient statuses for idempotent reads). This module is the
worker-side policy on top: ``retryable(3, (1, 2, 5))`` around a single
list/get/files/trackers/export read so a transient blip doesn't park a
row (or burn a tick) when one more attempt seconds later succeeds.

Reads only — never add/delete/move/resume (a retried write can double
apply; the clients already gate those to single attempts). Deterministic
errors (auth, validation, lookup) propagate immediately; cancellation is
never swallowed.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

#: Statuses worth one more attempt when they arrive as
#: aiohttp.ClientResponseError (registration lag 404s right after add,
#: plus the transient gateway set). Anything else fails fast.
_RETRYABLE_HTTP_STATUSES = frozenset({404, 408, 425, 429, 500, 502, 503, 504})


def is_retryable_read_error(exc: BaseException) -> bool:
    """True when a failed read deserves another attempt. Never raises."""
    try:
        if isinstance(exc, TimeoutError):
            return True
        if isinstance(exc, OSError):
            # One errno policy with the worker phases: a full or unwritable
            # disk (ENOSPC/EACCES/ENOENT/...) will not heal in two seconds,
            # so retrying only delays the loud failure. Socket-backed
            # errnos (reset, refused, interrupted) stay retryable.
            from .coordinator_errors import is_fatal_os_error

            return not is_fatal_os_error(exc)
        try:
            import aiohttp

            if isinstance(exc, aiohttp.ClientResponseError):
                try:
                    return int(getattr(exc, "status", 0) or 0) in (_RETRYABLE_HTTP_STATUSES)
                except (TypeError, ValueError):
                    return False
            if isinstance(exc, aiohttp.ClientConnectionError):
                return True
        except ImportError:
            pass
    except Exception:
        pass
    return False


def retryable(
    retries: int = 3,
    delays: tuple[float, ...] = (1.0, 2.0, 5.0),
    *,
    label: str = "read",
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorate an async read with bounded retries (default 3 retries, 1/2/5s).

    One initial attempt plus `retries` more: the default is four tries
    spaced 1s, 2s and 5s — the tolerance the hand-rolled loops had before
    this policy replaced them. Counting retries rather than attempts keeps
    every delay reachable.

    Retries only :func:`is_retryable_read_error` failures; every other
    exception (auth, validation, lookup, programming errors) propagates
    on the first attempt. CancelledError is never swallowed. The last
    error re-raises when the retries exhaust.
    """
    try:
        _n = max(0, int(retries))
    except (TypeError, ValueError):
        _n = 3
    try:
        _delays = tuple(max(0.0, float(d)) for d in (delays or ()))
        if not _delays:
            _delays = (1.0, 2.0, 5.0)
    except (TypeError, ValueError):
        _delays = (1.0, 2.0, 5.0)

    def _decorate(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(fn)
        async def _wrapper(*args: Any, **kwargs: Any) -> T:
            _last: BaseException | None = None
            for _i in range(_n + 1):
                try:
                    return await fn(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    if not is_retryable_read_error(e):
                        raise
                    _last = e
                    if _i >= _n:
                        break
                    _wait = _delays[min(_i, len(_delays) - 1)]
                    try:
                        log.debug(
                            "%s transient (%s); retrying in %.0fs (attempt %d/%d)",
                            label,
                            e,
                            _wait,
                            _i + 2,
                            _n + 1,
                        )
                    except Exception:
                        pass
                    try:
                        await asyncio.sleep(_wait)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
            assert _last is not None
            raise _last

        return _wrapper

    return _decorate


__all__ = ["retryable", "is_retryable_read_error"]
