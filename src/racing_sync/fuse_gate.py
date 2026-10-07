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
from pathlib import Path

log = logging.getLogger(__name__)


class FuseVerdict(str, enum.Enum):
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
    mount: Path | str, expected: list[tuple[str, int]]
) -> list[str] | None:
    """Files absent/size-mismatched under mount, None when unstatable.

    None = fail-closed unknowable (dead mount, stat error). Callers must
    park, never mark DONE on None. Empty list = fully present.
    """
    try:
        from .coordinator_paths import fuse_stat_cached

        m = Path(mount)

        def _check() -> list[str] | None:
            try:
                mount_ok, _ = fuse_stat_cached(m)
            except Exception:
                return None
            if not mount_ok:
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


async def verify_blob_on_mount(
    blob: bytes | None, mount: Path | str
) -> tuple[FuseVerdict, list[str], list[tuple[str, int]]]:
    """One gate for all callers: decode blob, stat, return verdict.

    - (PRESENT, [], expected) — every byte verified.
    - (MISSING, [...], expected) — mount readable, files absent.
    - (UNVERIFIABLE, [...], []) — no blob / undecodable / mount unreadable.
    """
    expected = expected_files_from_blob(blob)
    if not expected:
        return FuseVerdict.UNVERIFIABLE, ["<undecodable blob>"], []
    missing = await missing_under(mount, expected)
    if missing is None:
        return FuseVerdict.UNVERIFIABLE, ["<mount unstatable>"], expected or []
    if missing:
        return FuseVerdict.MISSING, missing, expected or []
    return FuseVerdict.PRESENT, [], expected or []


async def skipped_present(
    mount: Path | str, items: list[tuple[str, int]]
) -> set[str]:
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
    "verify_blob_on_mount",
    "skipped_present",
]
