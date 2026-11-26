"""One save-path ownership rule (audit I7).

Every destructive guard asks the same question: does this client
save_path live under a fuse mount (remote bytes — never delete through
the mount), under an SSD staging root (local bytes — safe to delete),
somewhere else, or is ownership unknowable (test double / unconfigured)?

Previously three near-copies answered it (`recovery._save_path_on_ssd`,
`FuseMixin._save_path_is_on_fuse`, `forget._is_fuse_save_path`), with
different normalization and symlink handling. This module is the one
rule; the old helpers delegate so their callers keep working.
"""

from __future__ import annotations

import enum
from pathlib import Path

from .coordinator_content import fold_path_case


class PathOwner(str, enum.Enum):  # noqa: UP042  (str(Enum) is the contract here)
    """Where a client save_path points, as far as the config can tell."""

    FUSE = "fuse"
    SSD = "ssd"
    FOREIGN = "foreign"
    UNKNOWN = "unknown"


def _norm(value: object) -> str | None:
    """Normalized posix-style path string, or None when unusable."""
    if not isinstance(value, (str, Path)):
        return None
    try:
        out = str(value).rstrip("/\\").replace("\\", "/")
    except Exception:
        return None
    return out or None


def _configured_roots(cfg: object) -> tuple[list[str], list[str]]:
    """(fuse mounts, SSD roots) from cfg; [] for anything unusable."""
    fuse: list[str] = []
    ssd: list[str] = []
    try:
        fcfg = getattr(getattr(cfg, "rclone", None), "fuse", None)
        for raw in (getattr(fcfg, "mount", None), getattr(fcfg, "mount_unsorted", None)):
            norm = _norm(raw)
            if norm and norm not in fuse:
                fuse.append(norm)
    except Exception:
        pass
    try:
        for raw in (getattr(cfg.dest, "save_path", None), getattr(cfg.ssd, "path", None)):
            norm = _norm(raw)
            if norm and norm not in ssd:
                ssd.append(norm)
    except Exception:
        pass
    return fuse, ssd


def _resolved(value: str) -> str | None:
    """Real path for `value`, or None when it cannot be resolved."""
    try:
        return str(Path(value).resolve()).replace("\\", "/").rstrip("/\\") or None
    except OSError:
        return None
    except Exception:
        return None


def classify_save_path(
    cfg: object,
    save_path: object,
    *,
    resolve_links: bool = False,
) -> PathOwner:
    """Classify `save_path` against the configured fuse/SSD roots.

    Never raises. `resolve_links=True` also compares real paths (a
    symlink pointing into the fuse mount must classify as FUSE or a
    symlinked save_path could delete remote data through the mount); it
    is opt-in because resolving stats the path, and hot paths must not
    add fuse reads.

    FUSE is checked first: config validation already rejects SSD/fuse
    overlap, and the destructive direction is "never delete through the
    mount".
    """
    sp = _norm(save_path)
    if sp is None:
        return PathOwner.UNKNOWN
    fuse_roots, ssd_roots = _configured_roots(cfg)
    if not fuse_roots and not ssd_roots:
        # Test doubles / unconfigured cfg: ownership is unknowable.
        return PathOwner.UNKNOWN

    candidates = {sp}
    if resolve_links:
        resolved = _resolved(sp)
        if resolved:
            candidates.add(resolved)

    def _under(roots: list[str]) -> bool:
        for base in roots:
            bases = {base}
            if resolve_links:
                resolved = _resolved(base)
                if resolved:
                    bases.add(resolved)
            for cand in candidates:
                folded = fold_path_case(cand)
                for b in bases:
                    folded_base = fold_path_case(b)
                    if folded_base and (
                        folded == folded_base or folded.startswith(folded_base + "/")
                    ):
                        return True
        return False

    if _under(fuse_roots):
        return PathOwner.FUSE
    if _under(ssd_roots):
        return PathOwner.SSD
    return PathOwner.FOREIGN
