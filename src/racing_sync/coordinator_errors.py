"""Shared coordinator errors / retry contract.

Split out of the coordinator god-file so every phase module (download,
reinject, late-seed) shares one definition of what is retryable. Re-exported
from ``racing_sync.coordinator`` for backwards compatibility.
"""

from __future__ import annotations

import aiohttp


class WebUIUnresponsiveError(RuntimeError):
    """Raised when destination WebUI times out, disconnects, or rejects re-injection under load."""


_WEBUI_RETRY_ERRORS = (
    TimeoutError,
    aiohttp.ClientError,
    ConnectionError,
    OSError,
    WebUIUnresponsiveError,
)

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
    "_NOT_VISIBLE_DETAIL",
    "_WEBUI_RETRY_ERRORS",
]
