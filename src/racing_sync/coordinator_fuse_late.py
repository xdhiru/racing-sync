"""Late cross-seed seeding surface: inject VPS1 arrivals onto the fuse mount.

Late cross-seeds arriving after a release completed are injected onto the
fuse mount (with skip_check=True, fail-closed onto verified bytes only);
the outage/healthy memos, the boot grace and the DONE-row repair/demote
paths live here. Cross-half helpers are reached through ``self`` (the
FuseMixin composes both halves); this module never imports
``coordinator_fuse``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path

from .clients.abstract import Torrent
from .coordinator_errors import AbandonedError
from .coordinator_inject import _group_is_ignored
from .io_bounds import rpc
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


class LateSeedMixin:
    """Late cross-seeds + the DONE-row fuse repair/demote paths."""

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
                except Exception as e:  # noqa: BLE001
                    # The repair is applied in memory either way, so this
                    # pass still injects; the write is retried next tick.
                    log.warning(
                        "late cross-seed: could not persist repaired save_path for %s: %s",
                        _row_key[:10],
                        e,
                    )
            else:
                log.warning(
                    "late cross-seed: %s is DONE but save_path %s is not on fuse; "
                    "demoting to MOVING so the SSD move runs before any injection",
                    ts.source_name[:50],
                    ts.save_path,
                )
                await self._demote_false_done_to_moving(ts)
                return
        elif ts.save_path:
            # save_path already points at the mount, but a recorded entry
            # can still be lost (client restart, manual delete). The
            # new-torrent loop below skips recorded hashes and the source
            # repair only covers the original, so nothing would ever
            # re-add an injected private. Verifying here re-injects what
            # is missing; unverifiable is not a reason to demote (the
            # save_path is correct) — the janitor keeps VPS1 meanwhile.
            try:
                _hs = {(ts.dest_infohash or "").lower(), _row_key}
                _hs.update(
                    h.strip().lower()
                    for h in (ts.injected_private_hashes or "").split(",")
                    if h.strip()
                )
                _outaged = any(self._fuse_outage_active(h) for h in _hs if h)
            except Exception:
                _outaged = False
            if not _outaged:
                # An active outage means the mount is unreachable, not that
                # entries are missing: defer like every other fuse check.
                try:
                    await self._verified_fuse_mount_for_done_row(ts)
                except AbandonedError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.debug("late cross-seed: fuse verify failed for %s: %s", _row_key[:10], e)

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
                # DB first: the janitor may already have deleted VPS1.
                blob = await self._load_racing_torrent_bytes(t.infohash)
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
                except AbandonedError:
                    raise
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
        """Bound the late-seed failure map + healthy/outage memos.

        TTL + cap hygiene now lives in one shared sweep
        (`Coordinator._sweep_runtime_maps`); this keeps the call-site name
        the late-seed path uses. Restart-wiped maps simply re-warm
        (thundering herd bounded by one full scan per DONE row, then
        memoized).
        """
        sweep = getattr(self, "_sweep_runtime_maps", None)
        if callable(sweep):
            sweep()

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
            inject_flag = bool(self.rt.acquire.inject_racing_torrents_to_fuse)
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
            # DB first: VPS1 may already be cleaned for this row.
            blob = await self._load_racing_torrent_bytes(ts.source_infohash)
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
            self._note_seed_uninjectable(ts, source_low)
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
        try:
            self._late_seed_misses.pop(source_low, None)
        except Exception:
            pass

    def _note_seed_uninjectable(self, ts: TorrentState, source_low: str) -> None:
        """Escalate a seed that has had no .torrent bytes for ~12 hours.

        The late-seed repair retries every 30 minutes forever, which is
        right for a transient SFTP blip but wrong for a state file that is
        empty/truncated: nothing inside the app can ever fetch it. One
        ERROR per hash tells the operator to re-add the .torrent by hand
        (or restore the file) instead of retrying silently.
        """
        try:
            counts = getattr(self, "_late_seed_misses", None)
            if not isinstance(counts, dict):
                counts = {}
                self._late_seed_misses = counts
            n = int(counts.get(source_low, 0)) + 1
            counts[source_low] = n
            if len(counts) > 5000:
                for key in list(counts)[: len(counts) - 5000]:
                    counts.pop(key, None)
        except Exception:
            return
        if n == 24:
            log.error(
                "late cross-seed: racing torrent %s (%s) has had no .torrent bytes "
                "for ~12h (state file unreadable and the daemon has no export); "
                "re-add its .torrent to VPS2 by hand or restore the VPS1 state file",
                source_low[:10],
                (ts.source_name or "")[:50],
            )

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
        # An entry carrying the hash but seeding from the SSD is not a fuse
        # seed: it would pass the entry gate while nothing serves from the
        # mount. Only entries off the SSD count as alive here.
        have, missing_entries = self._fuse_entry_split(present, hashes)
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
            target = self._verify_mount_for(ts, blob, self._target_mount_for(ts))
        except Exception:  # noqa: BLE001
            return None
        if missing_entries:
            if not await self._reinject_missing_done_entries(ts, missing_entries, blob, target):
                return None
            present = await self._dest_entries_for_hashes(hashes)
            if present is None:
                return None
            _, still_missing = self._fuse_entry_split(present, hashes)
            if still_missing:
                return None
        try:
            from .fuse_gate import verify_blob_on_mount as _verify

            check = await _verify(target, blob, expected=expected, confirm=True)
        except Exception:  # noqa: BLE001
            return None
        return target if check.verified else None

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
        except AbandonedError:
            # Row forgotten mid-flight: the caller must unwind instead of
            # logging "demote failed" and carrying on as if it ran.
            raise
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
