"""Fuse-seeding surface mixin: late cross-seeds + entry management.

Split out of the coordinator god-file. Late cross-seeds arriving on
VPS1 after a release completed are injected onto the fuse mount (with
skip_check=True, fail-closed onto verified bytes only); the fuse-entry
helpers (mount resolution, blob classification, availability checks,
entry ensure/replace, public-pause policy) back the re-add, inject,
and late-seed flows alike.

The halves live in ``coordinator_fuse_late`` (``LateSeedMixin``) and
``coordinator_fuse_entries`` (``FuseEntryMixin``); this module composes
them and re-exports the helpers other modules import from here.
"""

from __future__ import annotations

from .classifier import classify
from .coordinator_fuse_entries import (
    _BLOB_KIND_CACHE,
    FuseEntryMixin,
    _blob_cache_key,
    _blob_cache_put,
    _is_duplicate_add_detail as _is_duplicate_add_detail,
)
from .coordinator_fuse_late import LateSeedMixin, _record_injected_hash as _record_injected_hash


class FuseMixin(LateSeedMixin, FuseEntryMixin):
    """Late cross-seeds + fuse-entry management (duck-typed ``self``)."""

    # Kept on the composed mixin so the ``classify`` lookup resolves in
    # this module's namespace, where callers patch it.
    def _classify_blob_kind(self, blob: bytes | None) -> str:
        """Classify .torrent bytes; "unknown" when undecodable or cfg broken.

        Decodes are cached per blob sha1 (bounded): the late tick derives
        the mount from the blob per row per poll, and re-bdecoding the
        same bytes every tick is pure waste. A single fresh bdecode stays
        on-loop (microseconds for torrent metainfo) — no thread hop for
        a cache miss.
        """
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return "unknown"
            _key = _blob_cache_key(blob)
            if _key:
                try:
                    hit = _BLOB_KIND_CACHE.get(_key)
                except Exception:
                    hit = None
                if hit in ("movie", "season", "episode", "mixed", "unknown"):
                    return hit
            from .watchdir import extract_torrent_files_from_bencoded

            files = extract_torrent_files_from_bencoded(bytes(blob))
            if not files:
                return "unknown"
            kind = classify(files, self.cfg).kind
            kind = kind if kind in ("movie", "season", "episode", "mixed", "unknown") else "unknown"
            _blob_cache_put(_BLOB_KIND_CACHE, _key, kind)
            return kind
        except Exception:
            return "unknown"
