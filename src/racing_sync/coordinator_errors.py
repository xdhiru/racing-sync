"""Shared coordinator errors / retry contract.

Split out of the coordinator god-file so every phase module (download,
reinject, late-seed) shares one definition of what is retryable. Re-exported
from ``racing_sync.coordinator`` for backwards compatibility.
"""

from __future__ import annotations

import aiohttp
import errno as _errno


class WebUIUnresponsiveError(RuntimeError):
    """Raised when destination WebUI times out, disconnects, or rejects re-injection under load."""


_WEBUI_RETRY_ERRORS = (
    TimeoutError,
    aiohttp.ClientError,
    ConnectionError,
    OSError,
    WebUIUnresponsiveError,
)

# OSError errnos that are always fatal: parking them as "transient" would
# wedge a row forever (disk full never heals by retrying) or silently
# desert data. These fail fast and loudly instead.
_FATAL_OS_ERRNOS = frozenset({
    _errno.ENOSPC,
    _errno.EACCES,
    _errno.EPERM,
    _errno.EROFS,
    _errno.EFBIG,
    getattr(_errno, "EDQUOT", 122),
})

# OSError errnos that are genuinely transient on socket-backed clients.
# Anything else OSError-shaped (errno None, exotic codes) preserves the
# historical behavior: park and retry. Only the fatal set above fails.
_TRANSIENT_OS_ERRNOS = frozenset({
    _errno.EPIPE,
    _errno.ECONNRESET,
    _errno.ECONNABORTED,
    _errno.ECONNREFUSED,
    _errno.ETIMEDOUT,
    _errno.ENETRESET,
    _errno.ENETDOWN,
    _errno.ENETUNREACH,
    _errno.EHOSTDOWN,
    _errno.EHOSTUNREACH,
    _errno.EAGAIN,
    getattr(_errno, "EWOULDBLOCK", 11),
    _errno.EINTR,
    getattr(_errno, "ESHUTDOWN", 108),
})


def is_fatal_os_error(exc: BaseException) -> bool:
    """True for disk/permission OSErrors that must never park-and-retry."""
    if not isinstance(exc, OSError):
        return False
    try:
        return exc.errno in _FATAL_OS_ERRNOS
    except Exception:
        return False


def is_retryable_client_error(exc: BaseException) -> bool:
    """True when a phase may park-and-retry instead of failing the row.

    Timeouts, client transport errors, connection resets and the
    WebUI-unresponsive marker are transient. Fatal disk/permission
    errnos are not (fail fast). Unknown OSError shapes (errno None,
    exotic codes) keep the historical park-and-retry behavior.
    """
    if isinstance(exc, (TimeoutError, WebUIUnresponsiveError)):
        return True
    if isinstance(exc, aiohttp.ClientError):
        return True
    if isinstance(exc, ConnectionError):
        return True
    if isinstance(exc, OSError):
        return not is_fatal_os_error(exc)
    return False

# Indeterminate fuse-entry outcome from _ensure_fuse_entry: the re-add was
# accepted but the entry is not yet visible to lookups (client registration
# lag). Callers must park/retry, never fail the row over it.
_NOT_VISIBLE_DETAIL = "added but entry not yet visible on dest client"


class BatchMoveIncompleteError(RuntimeError):
    """A batch rclone move exited 0 but left batch files on local disk.

    rclone reports success when its filters match nothing, so a 0-transfer
    must never advance the batch (the old unconditional local cleanup would
    then destroy not-yet-uploaded data). Callers retry the same batch.
    """


class AbandonedError(RuntimeError):
    """A worker noticed its DB row is gone (forget/cancel removed it).

    Must unwind the worker WITHOUT failing anything: the generic worker
    wrapper transitions unhandled exceptions to FAILED, and that upsert
    would resurrect the deliberately deleted row as a zombie FAILED row.
    """


__all__ = [
    "AbandonedError",
    "BatchMoveIncompleteError",
    "WebUIUnresponsiveError",
    "_WEBUI_RETRY_ERRORS",
    "is_fatal_os_error",
    "is_retryable_client_error",
]
