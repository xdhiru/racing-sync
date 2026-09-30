"""Untrusted torrent-relative path joining.

Split out of the coordinator god-file. Used by the download/move/delete
paths and re-exported from ``racing_sync.coordinator`` for compatibility.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
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
        _parent = Path(state_db).parent
    except Exception:
        return None
    # ":memory:" (or any bare filename) has parent "." — never litter
    # the checkout/CWD with a blob dir; in-memory DBs keep no blobs.
    try:
        _name = getattr(Path(state_db), "name", "") or ""
    except Exception:
        _name = ""
    if str(_parent) in (".", "") or _name == ":memory:":
        return None
    try:
        return _parent / "watch_cross_seeds" / h
    except Exception:
        return None


#: TTL (seconds) for the shared fuse stat cache below.
FUSE_STAT_TTL_S = 60.0
#: Upper bound on cached paths (oldest-first eviction past it).
_FUSE_STAT_CAP = 20000
_fuse_stat_cache: dict[str, tuple[float, bool, int]] = {}
_fuse_stat_lock = threading.Lock()


def fuse_stat_cached(path: Path | str) -> tuple[bool, int]:
    """(exists, size_bytes) for `path`, cached 60s across callers/threads.

    The RE_ADDING gate, batch skip checks and watch election stat the same
    fuse files every tick per row; against a high-latency mount (teldrive)
    each stat is a round-trip, and the storm slows the mount for everyone
    (including qB's own reads). One cached stat per path per minute
    collapses it. Fail-closed like the callers: unstatable reads absent.

    Staleness note: the mount's own dir-cache (rclone default 5m) is
    already coarser than this TTL, so cached answers are never staler
    than what a fresh stat could return anyway; absence still parks for
    retry, and callers re-verify before anything destructive.
    """
    try:
        key = os.path.normpath(os.path.abspath(str(path)))
    except Exception:
        return False, -1
    if not key:
        return False, -1
    now = time.monotonic()
    try:
        with _fuse_stat_lock:
            hit = _fuse_stat_cache.get(key)
            if hit is not None and now - hit[0] < FUSE_STAT_TTL_S:
                return hit[1], hit[2]
    except Exception:
        pass
    try:
        st = os.stat(key)
        import stat as _stat_mod
        if _stat_mod.S_ISREG(st.st_mode):
            # Regular file: existence + size. Anything else (dirs, the
            # mount itself): existence only (size -1 never satisfies a
            # want>0 comparison downstream).
            found: tuple[bool, int] = (True, int(st.st_size))
        else:
            found = (True, -1)
    except OSError:
        found = (False, -1)
    except Exception:
        return False, -1
    try:
        with _fuse_stat_lock:
            _fuse_stat_cache[key] = (now, found[0], found[1])
            if len(_fuse_stat_cache) > _FUSE_STAT_CAP:
                cutoff = now - FUSE_STAT_TTL_S
                for k in [k for k, v in _fuse_stat_cache.items()
                          if v[0] < cutoff]:
                    _fuse_stat_cache.pop(k, None)
                while len(_fuse_stat_cache) > _FUSE_STAT_CAP:
                    _fuse_stat_cache.pop(next(iter(_fuse_stat_cache)), None)
    except Exception:
        pass
    return found


def fuse_stat_invalidate(path: Path | str) -> None:
    """Drop one cached path (call after mutating it). Best-effort."""
    try:
        key = os.path.normpath(os.path.abspath(str(path)))
    except Exception:
        return
    try:
        with _fuse_stat_lock:
            _fuse_stat_cache.pop(key, None)
    except Exception:
        pass


__all__ = ["_safe_ssd_join", "_watch_cross_seed_dir", "fuse_stat_cached",
           "fuse_stat_invalidate", "FUSE_STAT_TTL_S"]
