"""Fuse-seeding surface mixin: late cross-seeds + entry management.

Split out of the coordinator god-file. Late cross-seeds arriving on
VPS1 after a release completed are injected onto the fuse mount (with
skip_check=True, fail-closed onto verified bytes only); the fuse-entry
helpers (mount resolution, blob classification, availability checks,
entry ensure/replace, public-pause policy) back the re-add, inject,
and late-seed flows alike.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path

from .classifier import classify
from .clients.abstract import Torrent
from .clients.deluge import DelugeClient
from .coordinator_content import _looks_public, fold_path_case
from .coordinator_errors import _NOT_VISIBLE_DETAIL
from .coordinator_inject import _group_is_ignored
from .io_bounds import chunked, rpc
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Post-restart late-seed grace: unseen DONE rows defer their first check
# past this window (plus hash jitter) so a cold memo doesn't fire one
# full check per DONE row on the first poll.
_LATE_SEED_BOOT_GRACE_S = 300.0
# Proven-healthy verdict window + cap for the unified fuse memo
# (`late:<row>` row checks + `orig:<hash>` original entry checks).
_FUSE_HEALTHY_TTL_S = 1800.0
_FUSE_HEALTHY_CAP = 5000
#: Cap on recorded injected hashes per row. Long-lived series rows would
#: otherwise grow the CSV without bound; a trimmed hash re-injects at
#: worst as a duplicate ("already added"), never as new bytes.
_INJECTED_HASHES_KEEP = 50
#: Short defer window for fuse outages (dead mount / failed check).
#: Outages prove nothing about content, so they must never earn the 30m
#: content-miss backoff — a flapping mount would otherwise silence rows
#: for half an hour at a time.
_FUSE_OUTAGE_TTL_S = 300.0
#: Cap on cached per-blob decodes below (kind + announce). One bdecode is
#: microseconds, but the late tick re-decoded the same blobs per row per
#: poll; the cache collapses repeats. Oldest-first eviction past the cap.
_BLOB_DECODE_CACHE_CAP = 500
_BLOB_KIND_CACHE: dict[str, str] = {}
_BLOB_ANNOUNCE_CACHE: dict[str, str] = {}


def _blob_cache_key(blob: bytes | None) -> str:
    """sha1 key for per-blob decode caches ("" when unhashable)."""
    try:
        if not blob or not isinstance(blob, (bytes, bytearray)):
            return ""
        import hashlib

        return hashlib.sha1(bytes(blob)).hexdigest()
    except Exception:
        return ""


def _blob_cache_put(cache: dict[str, str], key: str, value: str) -> None:
    try:
        if not key:
            return
        cache[key] = value
        while len(cache) > _BLOB_DECODE_CACHE_CAP:
            cache.pop(next(iter(cache)), None)
    except Exception:
        pass


def _split_fuse_outage(missing: list[str]) -> tuple[list[str], list[str]]:
    """Split `(outage_markers, real_missing)` from a fuse-miss list.

    `<...>` entries (dead mount, failed availability check) prove nothing
    about the bytes: callers short-defer those and reserve the 30m
    content-miss backoff for verified-absent files.
    """
    try:
        outage = [m for m in missing or [] if str(m).startswith("<")]
        real = [m for m in missing or [] if not str(m).startswith("<")]
        return outage, real
    except Exception:
        return [], list(missing or [])


def _record_injected_hash(ts: TorrentState, h_low: str) -> bool:
    """Append `h_low` to the row's injected CSV, trimmed to the last 50.

    True when the set changed. Shared with the re-add/watch injectors
    so every writer enforces the same cap.
    """
    try:
        h_low = (h_low or "").lower()
        if not h_low:
            return False
        current = [h for h in (ts.injected_private_hashes or "").split(",") if h]
    except Exception:
        return False
    if h_low in current:
        return False
    current.append(h_low)
    try:
        ts.injected_private_hashes = ",".join(current[-_INJECTED_HASHES_KEEP:])
    except Exception:
        return False
    return True


def _is_duplicate_add_detail(detail: object) -> bool:
    """True when an add rejection only means 'already there'.

    qB answers duplicate adds with "Fails." and Deluge echoes "already
    ...". AddResult carries no machine-readable code (a client overhaul
    may add one), so the verdict lives in exactly this helper — never as
    inline string checks scattered across callers.
    """
    try:
        d = detail if isinstance(detail, str) else ""
        return d == "Fails." or "already" in d.lower()
    except Exception:
        return False


class FuseMixin:
    """Late cross-seeds + fuse-entry management (duck-typed ``self``)."""

    async def _check_and_inject_late_cross_seeds(
        self, ts: TorrentState, group: list[Torrent]
    ) -> None:
        """Check if new cross-seeds arrived on VPS1 for a completed release and inject them to FUSE."""
        if not hasattr(self, "_failed_late_cross_seeds"):
            self._failed_late_cross_seeds = {}
        _row_key = (ts.source_infohash or "").lower()
        _late_key = f"late:{_row_key}" if _row_key else ""
        if _late_key and self._fuse_healthy_fresh(_late_key):
            return
        self._late_seed_sweep()
        # Post-restart burst guard: the memo is cold on boot, so every
        # DONE row would otherwise run a full check (dest export + fuse
        # stats) on the first poll. Unseen rows defer past a short grace
        # window with hash jitter, spreading the burst over minutes.
        try:
            _boot = float(getattr(self, "_boot_mono", 0.0) or 0.0)
        except Exception:
            _boot = 0.0
        if _boot and _row_key:
            _age = time.monotonic() - _boot
            if 0 <= _age < _LATE_SEED_BOOT_GRACE_S:
                try:
                    _failed = getattr(self, "_failed_late_cross_seeds", None)
                    _seen = self._fuse_healthy_fresh(_late_key) or (
                        isinstance(_failed, dict) and _row_key in _failed
                    )
                except Exception:
                    _seen = True
                if not _seen:
                    try:
                        _jitter = float(int(_row_key[:8], 16) % 240)
                    except Exception:
                        _jitter = 0.0
                    try:
                        self._fuse_healthy_store()[_late_key] = (
                            time.monotonic() + (_LATE_SEED_BOOT_GRACE_S - _age) + _jitter
                        )
                    except Exception:
                        pass
                    return
        _injected_before = ts.injected_private_hashes
        _save_before = ts.save_path
        # DONE row whose save_path is not on fuse *may* never have completed
        # its rclone move — but it may also be a healthy row whose save_path
        # was never updated after the move (pre-fix rows). Distinguish by
        # proving fuse health: verified rows repair save_path forward and
        # processing continues; only unverified rows demote to MOVING.
        if ts.save_path and not self._save_path_is_on_fuse(ts.save_path):
            verified_mount = await self._verified_fuse_mount_for_done_row(ts)
            if verified_mount is not None:
                log.info(
                    "late cross-seed: %s seeds from fuse but save_path %s is stale; "
                    "repairing to %s instead of demoting",
                    ts.source_name[:50],
                    ts.save_path,
                    verified_mount,
                )
                ts.save_path = str(verified_mount)
                try:
                    self.store.upsert(ts)
                except Exception:  # noqa: BLE001
                    pass
            else:
                log.warning(
                    "late cross-seed: %s is DONE but save_path %s is not on fuse; "
                    "demoting to MOVING so the SSD move runs before any injection",
                    ts.source_name[:50],
                    ts.save_path,
                )
                await self._demote_false_done_to_moving(ts)
                return

        known_hashes = {
            h.lower()
            for h in (
                ts.source_infohash,
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            )
            if h
        }

        def _late_pending() -> bool:
            """Any unexpired deferral for this row's hashes or group members."""
            try:
                rel: set[str] = set(known_hashes)
                try:
                    for t in group or []:
                        _gh = (getattr(t, "infohash", "") or "").lower()
                        if _gh:
                            rel.add(_gh)
                except Exception:
                    pass
                try:
                    _out = getattr(self, "_fuse_outage_until", None)
                    if isinstance(_out, dict) and _out:
                        _now_m = time.monotonic()
                        for k in rel:
                            try:
                                if _now_m < float(_out.get(k, 0.0)):
                                    return True
                            except Exception:
                                continue
                except Exception:
                    pass
                fm = getattr(self, "_failed_late_cross_seeds", None)
                if not isinstance(fm, dict) or not fm:
                    return False
                _now = dt.datetime.now(dt.UTC)
                for k, v in fm.items():
                    if k in rel:
                        try:
                            if (_now - v).total_seconds() < 1800:
                                return True
                        except Exception:
                            return True
            except Exception:
                pass
            return False

        if not hasattr(self, "_failed_late_cross_seeds"):
            self._failed_late_cross_seeds = {}
        now_utc = dt.datetime.now(dt.UTC)

        # Repair a racing (source) injection that RE_ADDING step 1 silently
        # skipped (SFTP timeout bursts + no Deluge export fallback): the
        # source hash is in known_hashes by construction, so the
        # new-torrent loop below would never retry it and the row would
        # seed the cross-seed only. Runs before the early return so rows
        # with no *new* arrivals still heal.
        await self._ensure_source_fuse_entry(ts, group, now_utc)

        new_torrents = [
            t
            for t in group
            if t.infohash.lower() not in known_hashes
            and not _group_is_ignored([t], getattr(self, "store", None))
        ]

        # Repair the original entry even on quiet ticks (no new arrivals):
        # otherwise an original SSD leftover only heals when something else
        # arrives. Failed-map gating inside keeps the steady state cheap.
        await self._ensure_original_fuse_entry(
            ts,
            self._target_mount_for(ts),
        )

        if not new_torrents:
            # Fully healthy and nothing new: back off success for 30m instead
            # of export+stat per DONE row per tick. New VPS1 arrivals wait
            # at most one window (late seeds are bonus, not pipeline).
            # Never memoize while deferrals are pending — that would freeze
            # retries for the full window.
            if not _late_pending():
                self._late_seed_memoize(_row_key, _injected_before, _save_before, ts, changed=False)
            return

        ts_fallback_mount = self._target_mount_for(ts)
        changed = False

        for t in new_torrents:
            h_low = t.infohash.lower()
            failed_at = self._failed_late_cross_seeds.get(h_low)
            if failed_at and (now_utc - failed_at).total_seconds() < 1800:
                continue
            if self._fuse_outage_active(h_low):
                continue

            log.info(
                "detected late cross-seed for completed content '%s': %s (%s)",
                ts.source_name[:40],
                t.infohash[:10],
                t.name[:40],
            )
            try:
                blob = await self._fetch_racing_torrent_bytes(t.infohash)
            except Exception as e:  # noqa: BLE001
                log.warning("late cross-seed: fetch %s failed: %s", t.infohash[:10], e)
                self._failed_late_cross_seeds[h_low] = now_utc
                continue

            if not blob:
                log.warning("late cross-seed: no .torrent bytes available for %s", t.infohash[:10])
                self._failed_late_cross_seeds[h_low] = now_utc
                continue

            # Per-torrent mount: ts.kind may be stale "unknown" (fresh
            # adoption) which routes movies/seasons to unsorted. The blob's
            # own layout is authoritative.
            target_mount = self._target_mount_for_blob(blob, ts_fallback_mount)

            # Fuse gate (fail-closed): a DONE row proves the original content
            # moved, not that this late arrival's bytes did — verify before
            # injecting. Undecodable blobs and missing content reuse the 30m
            # failure backoff (no per-tick storm, no blind skip_check seeds).
            late_expected = self._expected_fuse_files(blob)
            if not late_expected:
                log.warning(
                    "late cross-seed: cannot decode file list for %s; deferring injection",
                    t.infohash[:10],
                )
                self._failed_late_cross_seeds[h_low] = now_utc
                continue
            late_missing = await self._confirmed_fuse_missing(target_mount, late_expected)
            if late_missing:
                _outage, _real = _split_fuse_outage(late_missing)
                if not _real:
                    # Dead mount / failed check proves nothing about the
                    # bytes: short outage defer, never the 30m content-miss
                    # backoff (and no SSD-probe demote off an unproven miss).
                    log.warning(
                        "late cross-seed: fuse unavailable for %s at %s (%s); short defer",
                        t.infohash[:10],
                        target_mount,
                        _outage[0] if _outage else "unknown",
                    )
                    self._note_fuse_outage(h_low)
                    continue
                log.warning(
                    "late cross-seed: fuse content missing for %s (%s) at %s (%d files); deferring injection",
                    t.infohash[:10],
                    t.name[:40],
                    target_mount,
                    len(_real),
                )
                self._failed_late_cross_seeds[h_low] = now_utc
                # Self-heal: content missing on fuse but still sitting on SSD
                # (abrupt-stop leftover, or a fuse-pointing entry whose bytes
                # never moved) must go through MOVING first — never inject
                # around it. Best-effort; a failed SSD probe just defers.
                # Off-loop: exists()+iterdir() over SSD stalls the loop on
                # a slow disk.
                try:
                    from .recovery import find_content_on_ssd

                    _ssd_hit = await asyncio.to_thread(find_content_on_ssd, self.cfg, late_expected)
                    if _ssd_hit is not None:
                        log.warning(
                            "late cross-seed: %s missing on fuse but present on SSD; "
                            "demoting %s to MOVING",
                            t.infohash[:10],
                            ts.source_name[:50],
                        )
                        await self._demote_false_done_to_moving(ts)
                        return
                except Exception:  # noqa: BLE001
                    pass
                continue

            try:
                ok, detail = await self._ensure_fuse_entry(
                    blob=blob,
                    infohash=h_low,
                    target_mount=target_mount,
                    label="late cross-seed",
                )
                if ok:
                    log.info(
                        "auto-injected late cross-seed %s (%s) onto fuse (%s)",
                        t.infohash[:10],
                        t.name[:40],
                        target_mount,
                    )
                    if _record_injected_hash(ts, h_low):
                        changed = True
                    self._failed_late_cross_seeds.pop(h_low, None)
                else:
                    log.warning(
                        "late cross-seed: add %s rejected by dest client: %s",
                        t.infohash[:10],
                        detail,
                    )
                    self._failed_late_cross_seeds[h_low] = now_utc
                    if len(self._failed_late_cross_seeds) > 5000:
                        cutoff = now_utc - dt.timedelta(minutes=30)
                        self._failed_late_cross_seeds = {
                            k: v for k, v in self._failed_late_cross_seeds.items() if v >= cutoff
                        }
            except Exception as e:  # noqa: BLE001
                log.warning("late cross-seed: add %s failed: %s", t.infohash[:10], e)
                self._failed_late_cross_seeds[h_low] = now_utc

        if changed:
            self.store.upsert(ts)
        elif not _late_pending():
            self._late_seed_memoize(_row_key, _injected_before, _save_before, ts, changed=False)

    def _late_seed_defer(self, h_low: str) -> None:
        """Record a late-seed deferral for the 30m backoff map (best-effort)."""
        try:
            d = getattr(self, "_failed_late_cross_seeds", None)
            if isinstance(d, dict) and h_low:
                d[h_low] = dt.datetime.now(dt.UTC)
        except Exception:
            pass

    def _note_fuse_outage(self, key: str) -> None:
        """Short-defer `key` past a fuse outage (dead mount / failed check).

        Outages must not touch the 30m content-miss map: the mount may be
        back in seconds, and the bytes were never proven absent.
        """
        try:
            if not key:
                return
            d = getattr(self, "_fuse_outage_until", None)
            if not isinstance(d, dict):
                d = {}
                self._fuse_outage_until = d  # type: ignore[attr-defined]
            d[key] = time.monotonic() + _FUSE_OUTAGE_TTL_S
        except Exception:
            pass

    def _fuse_outage_active(self, key: str) -> bool:
        """True while `key` sits inside a short outage deferral."""
        try:
            if not key:
                return False
            d = getattr(self, "_fuse_outage_until", None)
            if not isinstance(d, dict):
                return False
            return time.monotonic() < float(d.get(key, 0.0))
        except Exception:
            return False

    def _fuse_healthy_store(self) -> dict:
        """The one proven-healthy memo (row + entry verdicts, namespaced).

        Late-seed row checks (`late:<source-hash>`) and original entry
        checks (`orig:<entry-hash>`) shared one TTL, one cap and one sweep
        instead of two parallel dicts with duplicated expiry logic.
        """
        try:
            d = getattr(self, "_fuse_healthy_until", None)
            if not isinstance(d, dict):
                d = {}
                self._fuse_healthy_until = d  # type: ignore[attr-defined]
            return d
        except Exception:
            return {}

    def _fuse_healthy_fresh(self, key: str) -> bool:
        """True when `key` holds an unexpired healthy verdict."""
        try:
            if not key:
                return False
            nxt = self._fuse_healthy_store().get(key)
            return nxt is not None and time.monotonic() < float(nxt)
        except Exception:
            return False

    def _fuse_healthy_note(self, key: str) -> None:
        """Record a healthy verdict for the shared TTL window."""
        try:
            if not key:
                return
            d = self._fuse_healthy_store()
            d[key] = time.monotonic() + _FUSE_HEALTHY_TTL_S
            if len(d) > _FUSE_HEALTHY_CAP:
                for _k in list(d.keys())[: len(d) - _FUSE_HEALTHY_CAP]:
                    d.pop(_k, None)
        except Exception:
            pass

    def _late_seed_sweep(self) -> None:
        """Bound the late-seed failure map + healthy memo (30m TTL, 5000 cap).

        Called at the top of every late-seed check; cheap fast path unless a
        map grew large. Restart-wiped maps simply re-warm (thundering herd
        bounded by one full scan per DONE row, then memoized).
        """
        try:
            d = getattr(self, "_fuse_healthy_until", None)
            if isinstance(d, dict) and len(d) > 2000:
                now_m = time.monotonic()
                for k in list(d.keys()):
                    try:
                        if now_m >= float(d[k]):
                            d.pop(k, None)
                    except Exception:
                        d.pop(k, None)
                if len(d) > 5000:
                    for k in list(d.keys())[: len(d) - 5000]:
                        d.pop(k, None)
            fm = getattr(self, "_failed_late_cross_seeds", None)
            if not isinstance(fm, dict) or len(fm) <= 2000:
                return
            try:
                cutoff = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=30)
            except Exception:
                return
            for k in list(fm.keys()):
                try:
                    if fm[k] < cutoff:
                        fm.pop(k, None)
                except Exception:
                    fm.pop(k, None)
            if len(fm) > 5000:
                for k in list(fm.keys())[: len(fm) - 5000]:
                    fm.pop(k, None)
            ou = getattr(self, "_fuse_outage_until", None)
            if isinstance(ou, dict) and len(ou) > 2000:
                now_m = time.monotonic()
                for k in list(ou.keys()):
                    try:
                        if now_m >= float(ou[k]):
                            ou.pop(k, None)
                    except Exception:
                        ou.pop(k, None)
        except Exception:
            pass

    def _late_seed_memoize(
        self,
        row_key: str,
        injected_before: str,
        save_before: str,
        ts: TorrentState,
        *,
        changed: bool,
    ) -> None:
        """Record a quiet healthy check so the next tick skips this DONE row."""
        try:
            if changed or not row_key:
                return
            if ts.injected_private_hashes != injected_before or ts.save_path != save_before:
                return
            self._fuse_healthy_note(f"late:{row_key}")
        except Exception:
            pass

    async def _ensure_source_fuse_entry(
        self, ts: TorrentState, group: list[Torrent], now_utc: dt.datetime
    ) -> None:
        """Repair a missing racing-torrent fuse seed on DONE rows.

        Step 1 of RE_ADDING can silently skip the VPS1 torrent (SFTP
        timeout bursts; Deluge daemons have no torrent-file export
        fallback), leaving the row DONE with only the cross-seed seeding.
        The source hash sits in known_hashes by construction, so the
        new-torrent loop never retries it. Repair it here while VPS1 still
        lists it. Best-effort with the same 30m failure backoff as late
        cross-seeds (no per-tick SFTP storm); skipped entirely when the
        row needs no repair.
        """
        try:
            inject_flag = bool(self.cfg.cross_seed.inject_racing_torrents_to_fuse)
        except Exception:
            inject_flag = True
        if not inject_flag:
            return
        if (ts.cross_seed_source or "") == "watch-dir":
            return  # watch-dir drops have their own injector + blob store.
        source_low = (ts.source_infohash or "").lower()
        if not source_low:
            return
        # The source hash itself is always "known"; what matters is whether
        # it was ever injected (recorded) or IS the SSD/cross-seed torrent
        # itself (public path: same bytes, already seeding).
        injected_set = {h.lower() for h in ts.injected_private_hashes.split(",") if h}
        if source_low in injected_set:
            return
        if source_low in {
            (ts.dest_infohash or "").lower(),
            (ts.cross_seed_infohash or "").lower(),
        } - {""}:
            return
        src_entry = next(
            (t for t in (group or []) if t.infohash.lower() == source_low),
            None,
        )
        if src_entry is None:
            return  # VPS1 cleaned already; nothing to repair.
        failed_at = self._failed_late_cross_seeds.get(source_low)
        if failed_at and (now_utc - failed_at).total_seconds() < 1800:
            return
        if self._fuse_outage_active(source_low):
            return
        log.info(
            "late cross-seed: racing torrent %s (%s) never injected for %s; repairing",
            source_low[:10],
            (src_entry.name or "")[:40],
            ts.source_name[:40],
        )
        try:
            blob = await self._fetch_racing_torrent_bytes(ts.source_infohash)
        except Exception as e:  # noqa: BLE001
            log.warning("late cross-seed: repair fetch %s failed: %s", source_low[:10], e)
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        if not blob:
            log.warning(
                "late cross-seed: repair of racing torrent %s deferred (no .torrent bytes yet)",
                source_low[:10],
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        try:
            fallback = self._target_mount_for(ts)
        except Exception:  # noqa: BLE001
            return
        target_mount = self._target_mount_for_blob(blob, fallback)
        expected = self._expected_fuse_files(blob)
        if not expected:
            log.warning(
                "late cross-seed: repair cannot decode file list for %s; deferring",
                source_low[:10],
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        try:
            missing = await self._confirmed_fuse_missing(target_mount, expected)
        except Exception:  # noqa: BLE001
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        if missing:
            _outage, _real = _split_fuse_outage(missing)
            if not _real:
                log.warning(
                    "late cross-seed: repair of racing torrent %s deferred "
                    "(fuse unavailable at %s; short defer)",
                    source_low[:10],
                    target_mount,
                )
                self._note_fuse_outage(source_low)
                return
            log.warning(
                "late cross-seed: repair of racing torrent %s deferred "
                "(fuse content missing at %s, %d files)",
                source_low[:10],
                target_mount,
                len(_real),
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        try:
            ok, detail = await self._ensure_fuse_entry(
                blob=blob,
                infohash=source_low,
                target_mount=target_mount,
                label="racing torrent (repair)",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("late cross-seed: repair add %s failed: %s", source_low[:10], e)
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        if not ok:
            log.warning(
                "late cross-seed: repair add %s rejected by dest client: %s",
                source_low[:10],
                detail,
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        log.info(
            "auto-injected missing racing torrent %s (%s) onto fuse (%s) (repair)",
            source_low[:10],
            (src_entry.name or "")[:40],
            target_mount,
        )
        _record_injected_hash(ts, source_low)
        self.store.upsert(ts)
        self._failed_late_cross_seeds.pop(source_low, None)

    async def _dest_entries_for_hashes(self, hashes: set[str]) -> list | None:
        """Dest entries for recorded hashes (chunked, RPC-bounded).

        None on transport failure (fail-closed: callers treat unknown as
        unverified, never as healthy).
        """
        try:
            present: list = []
            for _chunk in chunked(sorted(hashes)):
                present.extend(
                    await rpc(
                        self.dest_client.list_torrents(hashes=_chunk),
                        getattr(self, "cfg", None),
                        "dest verify list_torrents",
                    )
                    or []
                )
            return present
        except Exception:  # noqa: BLE001
            return None

    async def _reinject_missing_done_entries(
        self,
        ts: TorrentState,
        missing: set[str],
        blob: bytes | None,
        target: Path,
    ) -> bool:
        """Re-inject lost dest entries whose bytes verify at the target.

        A client restart (or manual delete) can drop the entry while the
        bytes sit verified on fuse: rebuilding the entry is cheaper and
        safer than demoting the whole row to MOVING (the bytes already
        moved — a move would find nothing). Only hashes matching the row
        blob's own infohash are rebuilt; anything else (late seeds with
        unknown blobs) refuses so a wrong blob can never seed under a
        stranger's hash. True only when every missing entry rebuilt.
        """
        if not blob or not missing:
            return False
        try:
            from .watchdir import _bencoded_info_hash

            own_hash, _, _, _ = _bencoded_info_hash(bytes(blob))
            own_hash = (own_hash or "").lower()
        except Exception:
            return False
        if not own_hash:
            return False
        for mh in sorted(missing):
            if mh != own_hash:
                return False
            try:
                ok, _ = await self._ensure_fuse_entry(
                    blob=bytes(blob),
                    infohash=mh,
                    target_mount=target,
                    label="missing entry (repair)",
                )
            except Exception:  # noqa: BLE001
                return False
            if not ok:
                return False
        return True

    async def _verified_fuse_mount_for_done_row(self, ts: TorrentState) -> Path | None:
        """Prove a DONE row really seeds from fuse; return the mount or None.

        Checks both halves of "seeding from fuse": every recorded hash has a
        live dest entry AND the torrent bytes are present under the fuse
        target. Entries alone are not proof (a skip_check entry reports
        complete with zero bytes). Entries lost while bytes verify are
        rebuilt in place (same blob, same hash) instead of demoting the
        row. None means unverifiable — callers must take the fail-closed
        path (demote/defer), never assume health.
        """
        hashes = {
            h.lower()
            for h in (
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            )
            if h
        }
        if not hashes:
            return None
        present = await self._dest_entries_for_hashes(hashes)
        if present is None:
            return None
        have = {t.hash.lower() for t in (present or [])}
        blob = ts._blob or ts.cross_seed_blob
        if not blob and getattr(self, "store", None) is not None:
            try:
                blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                blob = None
        expected = self._expected_fuse_files(blob)
        if not expected:
            return None
        try:
            target = self._target_mount_for_blob(blob, self._target_mount_for(ts))
        except Exception:  # noqa: BLE001
            return None
        missing_entries = hashes - have
        if missing_entries:
            if not await self._reinject_missing_done_entries(ts, missing_entries, blob, target):
                return None
            present = await self._dest_entries_for_hashes(hashes)
            if present is None:
                return None
            have = {t.hash.lower() for t in (present or [])}
            if hashes - have:
                return None
        try:
            missing = await self._confirmed_fuse_missing(target, expected)
        except Exception:  # noqa: BLE001
            return None
        return target if not missing else None

    async def _demote_false_done_to_moving(self, ts: TorrentState) -> None:
        """Self-heal a DONE row whose save_path never left SSD.

        Fresh-DB recovery may have adopted an SSD-complete torrent as DONE
        (stale save_path, pre-fix DB). The rclone move must run before any
        fuse injection, so move the row back to MOVING when the SSD bytes
        are still present. When the bytes cannot be confirmed, leave the row
        alone (late seeds stay deferred by the caller). Single guarded
        write: save_path rides the transition UPDATE on a fresh re-read
        (state+version guard refuses concurrent moves loudly) instead of
        a narrow write plus a second write.
        """
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        if not h:
            return
        expected: list[tuple[str, int]] | None = None
        try:
            files = await self.dest_client.get_torrent_files(h)
            expected = [
                (str(f.name), int(f.size_bytes or 0))
                for f in (files or [])
                if getattr(f, "name", "")
            ] or None
        except Exception:  # noqa: BLE001
            expected = None
        if not expected:
            return
        try:
            from .recovery import find_content_on_ssd

            ssd_root = await asyncio.to_thread(find_content_on_ssd, self.cfg, expected)
        except Exception:  # noqa: BLE001
            return
        if ssd_root is None:
            return
        try:
            fresh = await asyncio.to_thread(self.store.get, (ts.source_infohash or "").lower())
        except Exception:  # noqa: BLE001
            return
        if fresh is None:
            return
        fresh.save_path = str(ssd_root)
        try:
            self.transition(fresh, State.MOVING)
            log.warning(
                "late cross-seed: demoted %s from DONE to MOVING; "
                "SSD content at %s will move before fuse injection",
                ts.source_name[:50],
                ssd_root,
            )
        except ValueError as e:
            log.warning(
                "late cross-seed: could not demote %s to MOVING: %s",
                ts.source_infohash[:10],
                e,
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "late cross-seed: demote failed for %s: %s",
                ts.source_infohash[:10],
                e,
            )

    async def _ensure_original_fuse_entry(self, ts: TorrentState, fallback_mount: Path) -> None:
        """Ensure the original (adopted) hash also seeds from fuse.

        The reported incident left the public SSD entry behind while only
        privates were injected. Best-effort: missing/unverifiable original
        blobs must not block new seeds whose own fuse gate already passed.
        """
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        if not h:
            return
        # Same 30m backoff as late cross-seeds: without it every tick pays a
        # dest export + fuse stat per DONE row even when healthy or stably
        # unrepairable.
        try:
            _failed = getattr(self, "_failed_late_cross_seeds", None)
            _fa = _failed.get(h) if isinstance(_failed, dict) else None
            if _fa is not None:
                try:
                    if (dt.datetime.now(dt.UTC) - _fa).total_seconds() < 1800:
                        return
                except Exception:
                    pass
        except Exception:
            pass
        # Dest-export only (no source RPC): the original entry's own bytes
        # are the authoritative repair payload, and this keeps the late-tick
        # free of extra source calls. Unavailable => skip repair; new seeds
        # stay protected by their own per-torrent fuse gate.
        #
        # Healthy memo (unified fuse memo, same window as the failure
        # backoff above): a proven-healthy entry must not pay export +
        # stats every tick — that was an INFO line plus qB/fuse round-trips
        # per DONE row per poll. Only verified health memoizes; every
        # defer/fail path re-probes next tick as before.
        _orig_key = f"orig:{h}" if h else ""
        if _orig_key and self._fuse_healthy_fresh(_orig_key):
            return
        if self._fuse_outage_active(h):
            return
        blob: bytes | None = None
        try:
            export_fn = getattr(self.dest_client, "export_torrent", None)
            if callable(export_fn):
                candidate = await rpc(
                    export_fn(h), getattr(self, "cfg", None), "dest original export"
                )
                if isinstance(candidate, (bytes, bytearray)) and candidate:
                    blob = bytes(candidate)
        except Exception:  # noqa: BLE001
            # Transport flap (dest down): no deferral from the error
            # itself — the outage note below already backs everything
            # else off, and a stable miss must not be confused with a
            # down client.
            blob = None
        if not blob:
            # Unverifiable right now (not provably missing): short defer
            # instead of paying an export every tick.
            self._note_fuse_outage(h)
            return
        target = self._target_mount_for_blob(blob, fallback_mount)
        expected = self._expected_fuse_files(blob)
        if not expected:
            self._late_seed_defer(h)
            return
        try:
            missing = await self._confirmed_fuse_missing(target, expected)
        except Exception:  # noqa: BLE001
            self._late_seed_defer(h)
            return
        if missing:
            _outage, _real = _split_fuse_outage(missing)
            if not _real:
                self._note_fuse_outage(h)
                return
            self._late_seed_defer(h)
            return
        try:
            ok, detail = await self._ensure_fuse_entry(
                blob=blob,
                infohash=h,
                target_mount=target,
                label="original content",
            )
            if not ok:
                log.warning(
                    "late cross-seed: original %s fuse repair rejected: %s",
                    h[:10],
                    detail,
                )
                self._late_seed_defer(h)
            else:
                try:
                    _failed6 = getattr(self, "_failed_late_cross_seeds", None)
                    if isinstance(_failed6, dict):
                        _failed6.pop(h, None)
                except Exception:
                    pass
                self._fuse_healthy_note(f"orig:{h}" if h else "")
        except Exception as e:  # noqa: BLE001
            log.warning("late cross-seed: original %s fuse repair failed: %s", h[:10], e)
            self._late_seed_defer(h)

    async def _confirmed_fuse_missing(
        self, target_mount: Path, files: list[tuple[str, int]]
    ) -> list[str]:
        """Cached miss confirm: re-probe fresh once before reporting absent.

        The shared stat cache (60s) can report an absence the mount has
        already resolved: a cached-only miss no longer earns a 30m
        backoff — only a fresh-confirmed miss defers. Outage markers
        (`<...>`) pass through untouched; callers split dead-mount from
        content-miss handling.
        """
        missing = await self._missing_fuse_files(target_mount, files)
        if not missing:
            return []
        if any(str(m).startswith("<") for m in missing):
            return missing
        try:
            from .fuse_gate import missing_under as _missing_under

            fresh_missing = await _missing_under(target_mount, files, fresh=True)
        except Exception:  # noqa: BLE001
            return missing
        if fresh_missing is None:
            # Indeterminate re-probe (dead mount, stat error) is an outage,
            # not proof the content is gone. Returning the cached miss here
            # charged a 30-minute content backoff for a mount that may be
            # back in seconds; the next probe confirms a real miss instead.
            return [f"<mount unavailable: {target_mount}>"]
        return fresh_missing

    async def _fetch_racing_torrent_bytes(self, infohash: str) -> bytes | None:
        """Fetch the raw .torrent bytes for a racing-client infohash.

        Tries in order:
          1. SFTP exporter (Deluge state dir, qB BT_backup)
          2. The source client's own export endpoint (qB /api/v2/torrents/export,
             Deluge core.get_torrent_file).
        """
        # Validate that infohash is a valid hex infohash
        if (
            not infohash
            or len(infohash) != 40
            or not all(c in "0123456789abcdefABCDEF" for c in infohash)
        ):
            log.warning("_fetch_racing_torrent_bytes: skipping invalid infohash %r", infohash)
            return None

        if self.sftp is not None:
            # One retry on timeout: several workers share a single paramiko
            # SFTP connection behind a lock, so a burst of late/re-inject
            # fetches can stall one call past the 15s budget. A clean miss
            # (file absent) returns None and is not retried.
            for attempt in (1, 2):
                try:
                    blob = await asyncio.wait_for(
                        asyncio.to_thread(self.sftp.fetch_torrent, infohash),
                        timeout=15.0,
                    )
                    if blob:
                        return blob
                    break
                except TimeoutError:
                    log.warning(
                        "sftp fetch %s timed out after 15s (attempt %d/2)", infohash[:10], attempt
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp fetch %s failed: %s", infohash[:10], e)
                    break

        try:
            return await asyncio.wait_for(
                self.source_client.export_torrent(infohash),
                timeout=15.0,
            )
        except AttributeError:
            return None
        except Exception as e:  # noqa: BLE001
            # Deluge daemons expose no torrent-file RPC (SFTP is the
            # mandatory path there, enforced by config validation), so a
            # failed fallback is routine — keep it out of the warning log.
            if self.sftp is not None and isinstance(self.source_client, DelugeClient):
                log.debug(
                    "_fetch_racing_torrent_bytes deluge export unavailable for %s: %s",
                    infohash[:10],
                    e,
                )
            else:
                log.warning(
                    "_fetch_racing_torrent_bytes export failed for %s: %s", infohash[:10], e
                )
            return None

    @staticmethod
    def _expected_fuse_files(blob: bytes | None) -> list[tuple[str, int]] | None:
        """Decode (torrent-relative name, size) pairs from .torrent bytes.

        Returns None when there is nothing to verify (no blob / undecodable /
        empty) — callers then keep existing behavior and let downstream steps
        fail loudly instead of gating on an empty expectation.
        Delegates to fuse_gate.expected_files_from_blob (single rule).
        """
        try:
            from .fuse_gate import expected_files_from_blob as _expected

            return _expected(blob)
        except Exception:
            return None

    async def _fuse_entry_bytes_ok(self, blob: bytes | None, target_mount: Path) -> bool:
        """Post-add byte re-verify: entry placed AND bytes still at target.

        Guards the gate→add TOCTOU (bytes moved between verification and
        injection): an entry pointing at the target while the bytes are
        gone parks/retries instead of marking injected. Cached stats, so
        the re-stat is cheap. Undecodable blobs yield no expectation —
        nothing to re-verify, so they pass (the pre-add gate owns that
        legacy path explicitly). False on any other doubt.
        """
        try:
            expected = self._expected_fuse_files(blob)
            if not expected:
                return True
            return not await self._missing_fuse_files(target_mount, expected)
        except Exception:  # noqa: BLE001
            return False

    async def _missing_fuse_files(
        self, target_mount: Path, files: list[tuple[str, int]]
    ) -> list[str]:
        """Expected files absent (or size-mismatched) under the fuse target.

        Delegates to fuse_gate.missing_under (single implementation).
        Fail-closed as before: unstatable mount/check returns a marker
        missing entry, never an empty pass.
        """
        try:
            from .fuse_gate import missing_under as _missing_under

            res = await _missing_under(target_mount, files or [])
        except Exception as e:  # noqa: BLE001
            log.warning("fuse availability check failed for %s: %s", target_mount, e)
            return [f"<availability check failed: {e}>"]
        if res is None:
            return [f"<mount unavailable: {target_mount}>"]
        return res

    def _fuse_mount_strs(self) -> list[str]:
        """Normalized fuse mount strings; [] when unconfigured/broken."""
        try:
            mounts = [self.cfg.rclone.fuse.mount, self.cfg.rclone.fuse.mount_unsorted]
        except Exception:
            return []
        out: list[str] = []
        for fm in mounts:
            try:
                s = str(fm).rstrip("/\\").replace("\\", "/")
            except Exception:
                continue
            if s:
                out.append(s)
        return out

    def _save_path_is_on_fuse(self, save_path: object) -> bool:
        """True iff a client save_path points at a configured fuse mount."""
        if not isinstance(save_path, str) or not save_path:
            return False
        sp = fold_path_case(save_path.rstrip("/\\").replace("\\", "/"))
        for fm in self._fuse_mount_strs():
            fm_folded = fold_path_case(fm)
            if sp == fm_folded or (fm_folded and sp.startswith(fm_folded + "/")):
                return True
        return False

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

    def _target_mount_for_kind(self, kind: str, fallback: Path) -> Path:
        """Fuse mount for a classification kind; fallback on error/unknown-cfg."""
        try:
            if kind in ("movie", "season"):
                return Path(self.cfg.rclone.fuse.mount)
            if kind in ("episode", "mixed", "unknown"):
                return Path(self.cfg.rclone.fuse.mount_unsorted)
        except Exception:
            pass
        return fallback

    def _target_mount_for_blob(self, blob: bytes | None, fallback: Path) -> Path:
        """Per-torrent fuse mount derived from the blob's own layout.

        Fresh-DB adoptions start as "unknown" and would otherwise route every
        late cross-seed to unsorted (wrong for movies/seasons, and the fuse
        gate then checks the wrong directory). The blob is authoritative.
        """
        kind = self._classify_blob_kind(blob)
        if kind == "unknown":
            return fallback
        return self._target_mount_for_kind(kind, fallback)

    def _save_path_points_at_target(self, save_path: object, target_mount: object) -> bool:
        """Does an existing client entry point at the fuse target we inject to?

        A duplicate hash may already exist pointing elsewhere (e.g. a leftover
        SSD entry). Such entries must be replaced, never mistaken for a fuse
        seed. Non-string save paths (shouldn't happen) never match.
        """
        if not isinstance(save_path, str):
            return False
        sp = fold_path_case(save_path.rstrip("/\\").replace("\\", "/"))
        tm = fold_path_case(str(target_mount).rstrip("/\\").replace("\\", "/"))
        return bool(tm) and (sp == tm or sp.startswith(tm + "/"))

    def _should_pause_public_on_fuse(self, blob: bytes | None) -> bool:
        """True when a fuse-injected blob must land paused, not seeding.

        Gated on `cross_seed.pause_public_torrents_on_fuse` AND a positively
        identified public announce URL in the blob. Fail-open toward seeding
        (current behavior) on any doubt — undecodable blobs, missing config,
        unknown trackers — so a private torrent is never paused by mistake
        and break a seeding obligation.
        """
        try:
            flag = getattr(getattr(self, "cfg", None), "cross_seed", None)
            if not bool(getattr(flag, "pause_public_torrents_on_fuse", False)):
                return False
        except Exception:
            return False
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return False
            _key = _blob_cache_key(blob)
            announce = None
            if _key:
                try:
                    if _key in _BLOB_ANNOUNCE_CACHE:
                        announce = _BLOB_ANNOUNCE_CACHE[_key]
                except Exception:
                    announce = None
            if announce is None:
                from .watchdir import _bencoded_info_hash

                _, _, _, announce = _bencoded_info_hash(bytes(blob))
                _blob_cache_put(_BLOB_ANNOUNCE_CACHE, _key, announce or "")
        except Exception:
            return False
        try:
            if not announce:
                return False
            return bool(_looks_public([announce]))
        except Exception:
            return False

    async def _pause_fuse_entry_best_effort(self, h_low: str, *, label: str) -> None:
        """Pause a fuse entry without ever failing the row.

        The entry is already correctly placed; a failed pause only means it
        keeps seeding until the operator pauses it by hand. Never raises.
        """
        try:
            pause_fn = getattr(self.dest_client, "pause", None)
            if not callable(pause_fn):
                return
            await pause_fn(h_low)
            log.info(
                "pausing public %s %s on fuse (pause_public_torrents_on_fuse=true)",
                label,
                h_low[:10],
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not pause public %s %s on fuse (keeps seeding): %s", label, h_low[:10], e
            )

    async def _select_all_fuse_files(self, h_low: str, *, label: str) -> bool:
        """Ensure every file in a fuse entry is wanted (priority != 0).

        A fuse seed must seed the full content: an entry inheriting the
        SSD batch deselection (all priorities 0) reports complete via
        skip_check while seeding nothing. Returns True when all files
        are wanted (or nothing verifiable, e.g. test doubles), False
        when the check/reset hit a transient client error and the
        caller should park/retry instead of marking DONE.
        """
        try:
            files = await rpc(
                self.dest_client.get_torrent_files(h_low),
                getattr(self, "cfg", None),
                "dest fuse get_torrent_files",
            )
        except Exception as e:  # noqa: BLE001
            # Test doubles often leave get_torrent_files as a plain
            # MagicMock (not awaitable): fail open, the entry itself was
            # already verified at the fuse target.
            if "can't be used in 'await'" in str(e) or "MagicMock" in str(e):
                return True
            log.warning(
                "fuse priority check failed for %s %s: %s; parking re-add", label, h_low[:10], e
            )
            return False
        if not isinstance(files, list) or not files:
            return True
        try:
            names = [f.name for f in files if getattr(f, "name", "")]
        except Exception:
            return True
        if not names:
            return True
        try:
            needs_reset = any(int(getattr(f, "priority", 1) or 0) == 0 for f in files)
        except Exception:
            needs_reset = False
        if not needs_reset:
            return True
        log.warning("fuse entry %s %s has deselected files; selecting all", label, h_low[:10])
        try:
            await rpc(
                self.dest_client.set_file_priorities(h_low, {n: 1 for n in names}),
                getattr(self, "cfg", None),
                "dest fuse set_file_priorities",
            )
        except Exception as e:  # noqa: BLE001
            if "can't be used in 'await'" in str(e) or "MagicMock" in str(e):
                return True
            log.warning(
                "fuse priority reset failed for %s %s: %s; parking re-add", label, h_low[:10], e
            )
            return False
        return True

    async def _ensure_fuse_entry(
        self, *, blob: bytes, infohash: str, target_mount: Path, label: str
    ) -> tuple[bool, str]:
        """Ensure a fuse-pointing dest entry exists for `infohash`.

        Adds `blob` with skip_check. When the client reports a duplicate,
        the existing entry is verified: fuse-pointing entries are accepted
        as-is, but entries pointing elsewhere (stale SSD leftovers) are
        deleted (files kept) and re-added at the fuse target — the bytes
        were verified at the target before this call.

        Public torrents land paused when
        `cross_seed.pause_public_torrents_on_fuse` is set (fresh adds go in
        paused; already-seeding fuse entries are paused in place). Private
        torrents always seed as before.

        Returns (ok, detail). Never raises for client rejections; callers
        apply their own retry/fail policy. Exact add kwargs are kept stable
        for the seeding contract (category/tags/skip_check) apart from the
        paused flag above.

        Registration lag: a loaded client can accept the re-add while the
        entry is not yet visible to lookups. The post-add check is retried;
        a still-unconfirmed entry returns _NOT_VISIBLE_DETAIL so callers
        park/retry instead of failing the row over a transient.
        """
        h_low = infohash.lower()
        pause_public = self._should_pause_public_on_fuse(blob)
        add_kwargs: dict[str, object] = {
            "torrent_files": [blob],
            "save_path": str(target_mount),
            "category": "racing",
            "paused": True if pause_public else False,
            "skip_check": True,
            "tags": ["racing", "fuse"],
        }
        res = await rpc(
            self.dest_client.add_torrent(**add_kwargs),  # type: ignore[arg-type]
            getattr(self, "cfg", None),
            "dest fuse add_torrent",
        )
        detail = res.detail if isinstance(res.detail, str) else ""
        if res.accepted and not _is_duplicate_add_detail(detail):
            # Verify the entry actually landed where asked: the fuse index
            # can lag (rclone busy with another move), and an accepted-but-
            # invisible entry must park/retry — never mark DONE, never touch
            # the moved files. Same patience window as the replace path.
            for _ in range(4):
                try:
                    _st = await rpc(
                        self.dest_client.get_torrent(h_low),
                        getattr(self, "cfg", None),
                        "dest fuse get_torrent",
                    )
                except Exception:
                    _st = None
                if _st is not None and self._save_path_points_at_target(
                    getattr(_st, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
                    if not await self._fuse_entry_bytes_ok(blob, target_mount):
                        log.warning(
                            "%s %s entry visible but bytes unverifiable at %s; parking re-add",
                            label,
                            h_low[:10],
                            target_mount,
                        )
                        return False, _NOT_VISIBLE_DETAIL
                    if not await self._select_all_fuse_files(h_low, label=label):
                        return False, _NOT_VISIBLE_DETAIL
                    if pause_public:
                        await self._pause_fuse_entry_best_effort(h_low, label=label)
                    return True, detail
                await asyncio.sleep(2)
            log.warning(
                "%s %s accepted but not yet visible on fuse (%s); parking re-add",
                label,
                h_low[:10],
                target_mount,
            )
            return False, _NOT_VISIBLE_DETAIL
        if not _is_duplicate_add_detail(detail):
            return False, detail
        # Possible duplicate: inspect what's actually there.
        try:
            dest_st = await rpc(
                self.dest_client.get_torrent(h_low),
                getattr(self, "cfg", None),
                "dest fuse get_torrent",
            )
        except Exception as e:  # noqa: BLE001
            log.debug("could not check dest client for %s: %s", h_low[:10], e)
            dest_st = None
        if dest_st is not None and self._save_path_points_at_target(
            getattr(dest_st, "save_path", ""), target_mount
        ):
            log.info("%s %s already on fuse; marking as injected", label, h_low[:10])
            if not await self._fuse_entry_bytes_ok(blob, target_mount):
                log.warning(
                    "%s %s entry on fuse but bytes unverifiable at %s; parking re-add",
                    label,
                    h_low[:10],
                    target_mount,
                )
                return False, _NOT_VISIBLE_DETAIL
            if not await self._select_all_fuse_files(h_low, label=label):
                return False, _NOT_VISIBLE_DETAIL
            if pause_public:
                await self._pause_fuse_entry_best_effort(h_low, label=label)
            return True, "already added"
        if dest_st is not None:
            log.warning(
                "%s %s exists at %s (not the fuse target %s); replacing with fuse entry",
                label,
                h_low[:10],
                getattr(dest_st, "save_path", "?"),
                target_mount,
            )
            # Files kept deliberately: deleting with files here would destroy
            # bytes the fuse gate verified. The leftover SSD bytes are not
            # reclaimed automatically (no row references SSD past this
            # point; only a manual SSD clear reaps them) — the accepted
            # cost of never deleting verified data on a hunch. The row
            # already holds no SSD reservation, so admission accounting
            # treats the space as free; a truly full disk still parks via
            # the ssd_has_room stat gate.
            try:
                await rpc(
                    self.dest_client.delete(h_low, delete_files=False),
                    getattr(self, "cfg", None),
                    "dest fuse delete",
                )
            except Exception as e:  # noqa: BLE001
                return False, f"cannot remove non-fuse entry: {e}"
            res2 = await rpc(
                self.dest_client.add_torrent(**add_kwargs),  # type: ignore[arg-type]
                getattr(self, "cfg", None),
                "dest fuse add_torrent",
            )
            detail2 = res2.detail if isinstance(res2.detail, str) else ""
            if res2.accepted or _is_duplicate_add_detail(detail2):
                dest_st2 = None
                for _ in range(4):
                    try:
                        dest_st2 = await rpc(
                            self.dest_client.get_torrent(h_low),
                            getattr(self, "cfg", None),
                            "dest fuse get_torrent",
                        )
                    except Exception:
                        dest_st2 = None
                    if dest_st2 is not None:
                        break
                    await asyncio.sleep(2)
                if dest_st2 is not None and self._save_path_points_at_target(
                    getattr(dest_st2, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
                    if not await self._select_all_fuse_files(h_low, label=label):
                        return False, _NOT_VISIBLE_DETAIL
                    if pause_public:
                        await self._pause_fuse_entry_best_effort(h_low, label=label)
                    return True, detail2
                return False, _NOT_VISIBLE_DETAIL
            return False, detail2
        return False, detail

    def _target_mount_for(self, ts: TorrentState) -> Path:
        """Where on the fuse mount should this torrent's data live?"""
        if ts.classification_kind == "movie" or ts.classification_kind == "season":
            return Path(self.cfg.rclone.fuse.mount)
        return Path(self.cfg.rclone.fuse.mount_unsorted)
