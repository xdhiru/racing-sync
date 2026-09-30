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


class UnregisteredTorrentError(RuntimeError):
    """The tracker reports the torrent as deleted/unregistered.

    Downloading further is pointless (no seeds will ever come): callers
    fail the row terminally instead of parking it. Never auto-retried —
    a deleted release stays deleted.
    """


class DownloadStalledError(TimeoutError):
    """A download made no progress for a full stall window.

    Carries the progress fraction at timeout so callers can tell a
    repeatedly-stalled (dead) row apart from one that advances between
    windows: only consecutive no-progress parks escalate to FAILED.
    """

    def __init__(self, message: str, *, progress: float = 0.0):
        super().__init__(message)
        try:
            self.progress = float(progress)
        except (TypeError, ValueError):
            self.progress = 0.0


class TorrentCheckingError(Exception):
    """The client reports the torrent is hash-checking (checking* state).

    A checking torrent is neither downloading nor paused-verifiable: moves
    must wait it out (moving mid-check risks shipping bytes the check then
    fails), pauses must not fight it, and rechecks must not pile on. The
    client state rides along for log context, plus the check-fraction
    progress (qB reports verification progress in the progress field) so
    the watchdog can tell a crawling check from a wedged one.
    """

    def __init__(self, message: str, *, client_state: str = "",
                 progress: float = 0.0):
        super().__init__(message)
        try:
            self.client_state = str(client_state or "")
        except Exception:
            self.client_state = ""
        try:
            self.progress = float(progress or 0.0)
        except (TypeError, ValueError):
            self.progress = 0.0


# Lowercase substrings of qB tracker `msg` values that mean "this release
# is gone from the tracker — stop downloading". Matched loosely on purpose
# (trackers word it many ways); passkey/auth failures are classified
# separately below so the operator gets the right repair hint.
_TRACKER_UNREGISTERED_MARKERS = (
    "unregister",
    "not register",
    "unknown torrent",
    "torrent not found",
    "torrent unknown",
    "no such torrent",
    "deleted",
    "removed",
    "not exist",
    "does not exist",
    "invalid infohash",
    "unknown infohash",
)

_TRACKER_AUTH_MARKERS = (
    "passkey",
    "not authorized",
    "not authorised",
    "unauthorized",
    "unauthorised",
    "invalid account",
    "banned",
)

# Marker prefix stamped into last_error for terminal tracker failures.
# auto_retry_failed() refuses rows carrying the unregistered marker.
TRACKER_UNREGISTERED_MARKER = "tracker unregistered:"
TRACKER_AUTH_MARKER = "tracker auth:"


def classify_tracker_message(msg: object) -> str | None:
    """'unregistered' / 'auth' / None for one tracker msg string."""
    try:
        low = str(msg or "").lower()
    except Exception:
        return None
    if not low.strip():
        return None
    for m in _TRACKER_UNREGISTERED_MARKERS:
        if m in low:
            return "unregistered"
    for m in _TRACKER_AUTH_MARKERS:
        if m in low:
            return "auth"
    return None


__all__ = [
    "AbandonedError",
    "BatchMoveIncompleteError",
    "DownloadStalledError",
    "RcloneTransientError",
    "TRACKER_AUTH_MARKER",
    "TRACKER_UNREGISTERED_MARKER",
    "TorrentCheckingError",
    "UnregisteredTorrentError",
    "WebUIUnresponsiveError",
    "_WEBUI_RETRY_ERRORS",
    "classify_tracker_message",
    "is_fatal_os_error",
    "is_retryable_client_error",
    "is_transient_rclone_stderr",
]
