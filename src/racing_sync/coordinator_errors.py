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


class RcloneTransientError(RuntimeError):
    """An rclone move failed with a transient-looking remote error.

    Timeouts already surface as RcloneTimeoutError; this covers rc!=0 with
    transport/remote markers (reset, refused, unreachable, 5xx, handshake).
    Callers park the row for retry like a timeout — failing would loop
    re-download/stall/fail on every remote blip. Persistent misconfig
    (bad remote, auth) parks loudly with the rc+stderr in last_error and
    escalates via the moving-park counter instead of failing silently.
    """


# Lowercase markers matched against rclone rc!=0 stderr to classify the
# failure as transient (park) vs persistent (fail). Conservative: unknown
# text fails like before.
_RCLONE_TRANSIENT_MARKERS = (
    "timeout", "timed out", "connection reset", "connection refused",
    "connection aborted", "network unreachable", "network is unreachable",
    "host unreachable", "no route to host", "broken pipe",
    "temporary failure", "try again", "service unavailable",
    "bad gateway", "gateway timeout", "internal error",
    "tls handshake", "handshake failure", "connection closed",
    "too many requests", "slow down", "socket", "eof",
)


def is_transient_rclone_stderr(stderr: str) -> bool:
    """True when rclone stderr looks like a transient remote/transport blip."""
    try:
        low = (stderr or "").lower()
    except Exception:
        return False
    return any(m in low for m in _RCLONE_TRANSIENT_MARKERS)


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
    "RcloneTransientError",
    "WebUIUnresponsiveError",
    "_WEBUI_RETRY_ERRORS",
    "is_fatal_os_error",
    "is_retryable_client_error",
    "is_transient_rclone_stderr",
]
