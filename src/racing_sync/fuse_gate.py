"""Single fuse verification gate (refactor).

Previously the same "blob -> expected files -> stat under mount"
check lived in 6+ places with slight drift:
  _verify_and_adopt_manual_fuse, _sweep_manual, _do_queued fast-path,
  _do_re_add gate, _ensure_fuse_entry, _blob_fully_remote,
  _fuse_skipped, _missing_fuse_files, _expected_fuse_files.

This module is the one implementation. Coordinator methods delegate
to it for backwards compat (tests patch coordinator methods).
"""

from __future__ import annotations

import asyncio
import enum
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

#: Mount-point probes are meaningful on POSIX only; tests flip this to
#: exercise the POSIX path on any host (patching os.name would also
#: change how pathlib picks its flavour).
_POSIX = os.name == "posix"


def mount_is_mounted(mount: Path | str) -> bool:
    """True when `mount` is really mounted (POSIX) or unknowable (elsewhere).

    An unmounted mountpoint is an ordinary empty directory: its stat
    succeeds, so every expected file reads "missing" — the row records a
    content miss and the healer can re-download bytes that are present on
    the remote. A directory that holds entries is a working tree even when
    the kernel does not call it a mountpoint (bind layouts, alternate
    mount tooling), so only the empty non-mountpoint directory is treated
    as unavailable. Windows has no ismount semantics for this setup
    (rclone uses drive letters, and a dead drive fails the stat), so it
    keeps the stat-only path.
    """
    if not _POSIX:
        return True
    try:
        if os.path.ismount(str(mount)):
            return True
        with os.scandir(str(mount)) as _it:
            return next(_it, None) is not None
    except OSError:
        return False


class FuseVerdict(str, enum.Enum):  # noqa: UP042  (StrEnum changes str(Enum); .value is the contract)
    PRESENT = "present"
    MISSING = "missing"
    UNVERIFIABLE = "unverifiable"


def expected_files_from_blob(blob: bytes | None) -> list[tuple[str, int]] | None:
    """Decode (torrent-relative name, size) pairs, None when unverifiable."""
    if not blob or not isinstance(blob, (bytes, bytearray)):
        return None
    try:
        from .watchdir import extract_torrent_files_from_bencoded

        pairs = [
            (f.name, f.size_bytes)
            for f in extract_torrent_files_from_bencoded(bytes(blob))
            if getattr(f, "name", "")
        ]
        return pairs or None
    except Exception:
        return None


def _join(mount: Path, name: str):
    try:
        from .coordinator_paths import _safe_ssd_join

        return _safe_ssd_join(mount, name or "")
    except Exception:
        return None


async def missing_under(
    mount: Path | str, expected: list[tuple[str, int]], *, fresh: bool = False
) -> list[str] | None:
    """Files absent/size-mismatched under mount, None when unstatable.

    None = fail-closed unknowable (dead mount, stat error). Callers must
    park, never mark DONE on None. Empty list = fully present. With
    fresh=True the cached stats for the mount + targets are invalidated
    first, so a stale cached absence (rclone dir-cache lag resolving the
    other way) is re-probed instead of trusted.
    """
    try:
        from .coordinator_paths import fuse_stat_cached, fuse_stat_invalidate

        m = Path(mount)

        def _check() -> list[str] | None:
            try:
                if fresh:
                    try:
                        fuse_stat_invalidate(m)
                    except Exception:
                        pass
                    for _name, _want in expected or []:
                        _t = _join(m, _name or "")
                        if _t is not None:
                            try:
                                fuse_stat_invalidate(_t)
                            except Exception:
                                pass
                mount_ok, _ = fuse_stat_cached(m)
            except Exception:
                return None
            if not mount_ok or not mount_is_mounted(m):
                return [f"<mount unavailable: {m}>"]
            missing: list[str] = []
            for name, want in expected or []:
                target = _join(m, name or "")
                if target is None:
                    missing.append(name)
                    continue
                try:
                    exists, actual = fuse_stat_cached(target)
                except Exception:
                    return None
                if not exists:
                    missing.append(name)
                    continue
                if want and actual != want:
                    missing.append(f"{name} (size {actual}!={want})")
            return missing

        try:
            return await asyncio.to_thread(_check)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse_gate: availability check failed for %s: %s", mount, e)
            return None
    except Exception as e:  # noqa: BLE001
        log.warning("fuse_gate: check failed for %s: %s", mount, e)
        return None


async def skipped_present(mount: Path | str, items: list[tuple[str, int]]) -> set[str]:
    """Subset of names already size-verified on mount (fail-open empty).

    Strict equality (actual == want), matching legacy _fuse_skipped:
    unknown-size (want=0) only skips actual 0-byte files. This differs
    deliberately from missing_under (which treats want=0 as present
    for gate purposes) — batch skip must not skip real bytes.
    """
    if not items:
        return set()
    try:
        from .coordinator_paths import fuse_stat_cached

        m = Path(mount)

        def _check() -> set[str]:
            skipped: set[str] = set()
            for name, size in items or []:
                target = _join(m, name or "")
                if target is None:
                    continue
                try:
                    exists, actual = fuse_stat_cached(target)
                except Exception:
                    continue
                if not exists:
                    continue
                try:
                    want = int(size or 0)
                except (TypeError, ValueError):
                    want = 0
                try:
                    got = int(actual)
                except (TypeError, ValueError):
                    continue
                if got == want:
                    skipped.add(name)
            return skipped

        try:
            return await asyncio.to_thread(_check)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse_gate: skip check failed, downloading everything: %s", e)
            return set()
    except Exception as e:  # noqa: BLE001
        log.warning("fuse_gate: skip check failed: %s", e)
        return set()


__all__ = [
    "FuseVerdict",
    "expected_files_from_blob",
    "missing_under",
    "skipped_present",
]
