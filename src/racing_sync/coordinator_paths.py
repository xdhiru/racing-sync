"""Untrusted torrent-relative path joining.

Split out of the coordinator god-file. Used by the download/move/delete
paths and re-exported from ``racing_sync.coordinator`` for compatibility.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

_INFOHASH_DIR_RE = re.compile(r"[0-9a-f]{40}")


def _safe_ssd_join(base: Path, name: str) -> Path | None:
    """Join untrusted client/torrent file `name` under `base`, or None if unsafe.

    Rejects absolute paths, ``..`` segments, and control chars instead of
    silently relativizing them. Callers must skip (never delete/move) None.
    """
    if not name or not isinstance(name, str):
        return None
    if "\n" in name or "\r" in name or "\0" in name:
        log.warning("refusing file name with control chars: %r", name[:100])
        return None
    norm = name.replace("\\", "/")
    if norm.startswith("/") or (len(norm) >= 2 and norm[1] == ":" and norm[0].isalpha()):
        log.warning("refusing absolute file name: %r", name[:100])
        return None
    parts = [p for p in norm.strip("/").split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        log.warning("refusing traversal file name: %r", name[:100])
        return None
    return base / "/".join(parts)


def _watch_cross_seed_dir(state_db: Path | str, infohash: str) -> Path | None:
    """Per-row blob dir under the state.db sibling, or None when unsafe.

    The infohash flows into a filesystem path here: only a 40-char hex
    is accepted, so a hostile value (e.g. '../../x') can never escape the
    blob root via join/mkdir. Empty hashes also refuse (never the root).
    """
    h = (infohash or "").strip().lower()
    if _INFOHASH_DIR_RE.fullmatch(h) is None:
        return None
    try:
        return Path(state_db).parent / "watch_cross_seeds" / h
    except Exception:
        return None


__all__ = ["_safe_ssd_join", "_watch_cross_seed_dir"]
