"""Content-key dedup helper (refactor).

Single place for "same release?" logic previously duplicated across:
- VPS1 source grouping (name match)
- watch-dir election (normalized name + size)
- in-flight same-content deferral (normalized name + size + active states)
- late cross-seed grouping

A content key is (normalized_name, size_or_0). Size 0 means "unknown"
and matches only on name, preserving the old fail-open behaviour where
unknown sizes never block each other incorrectly... actually to keep
exact legacy semantics:
- _same_content_torrents: name-only (no size check, infohash differs)
- watch election / inflight: name + size equality when both known.

Helpers below keep those two flavours explicit instead of one fuzzy rule.
"""

from __future__ import annotations


def _norm(name: str) -> str:
    try:
        from .coordinator_content import normalize_content_name

        return normalize_content_name(name or "")
    except Exception:
        try:
            return (name or "").strip().lower()
        except Exception:
            return ""


def content_key(name: str, size_bytes: int = 0) -> tuple[str, int]:
    """Stable grouping key for same-content detection."""
    try:
        size = int(size_bytes or 0)
    except (TypeError, ValueError):
        size = 0
    if size < 0:
        size = 0
    return (_norm(name), size)


def same_release_name(a_name: str, b_name: str) -> bool:
    """VPS1 grouping rule: same normalized name (size checked by caller)."""
    try:
        return _norm(a_name) == _norm(b_name)
    except Exception:
        return False


def same_content(
    a_name: str,
    a_size: int,
    b_name: str,
    b_size: int,
    *,
    require_size: bool = False,
    allow_unknown: bool = True,
) -> bool:
    """Same normalized name, then the size mode decides.

    - both sizes known: they must be equal.
    - any side unknown: rejected when `allow_unknown` is False (the
      destructive mode — name-only evidence is not enough to delete);
      otherwise a one-sided unknown matches, and two unsized releases
      match only when `require_size` is False (distinct unsized releases
      would otherwise mutually defer forever).

    Callers pick the mode explicitly: deferral/election use
    `require_size=True`, destructive grouping uses `allow_unknown=False`,
    and display/dedup keep the permissive default. Legacy inline rules
    mapped to:
      name + equal sizes when both known:
          same_content(..., allow_unknown=False)
      name + no cross-size check (VPS1 grouping):
          same_release_name
    """
    try:
        if _norm(a_name) != _norm(b_name):
            return False
    except Exception:
        return False
    try:
        a_s = int(a_size or 0)
    except (TypeError, ValueError):
        a_s = 0
    try:
        b_s = int(b_size or 0)
    except (TypeError, ValueError):
        b_s = 0
    if not a_s or not b_s:
        if not allow_unknown:
            return False
        if not a_s and not b_s:
            return not require_size
        return True
    return a_s == b_s


__all__ = ["content_key", "same_release_name", "same_content"]
