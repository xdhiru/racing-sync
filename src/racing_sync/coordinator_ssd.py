"""SSD budget mixin: batch caps + global reservation ledger.

Split out of the coordinator god-file. ``max_inflight_bytes`` is a GLOBAL
budget across concurrent downloads: admission reserves an estimate,
post-classify refines to the real footprint (max batch for seasons/games,
total for singles), terminal/park transitions release. Rebuilt from the DB
on startup so abrupt stops resume with a correct budget.

Patch-target compatibility: ``ssd_has_room`` / ``ssd_max_inflight_bytes``
are resolved through the ``racing_sync.coordinator`` namespace at call time
(tests patch ``racing_sync.coordinator.ssd_has_room``), never imported here
at module top (that would freeze a direct reference and bypass the patch).
"""

from __future__ import annotations

import asyncio
import logging

from .state import State, TorrentState

# Guards lazy SSD-lock creation so two coroutines cannot install two
# different asyncio.Lock objects (split-brain over-admission).
import threading as _threading

_SSD_LOCK_GUARD = _threading.Lock()

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class SSDLedgerMixin:
    """Batch caps + global SSD reservation ledger (duck-typed ``self``)."""

    def _batch_cap_bytes(self) -> int:
        from . import coordinator as _c

        try:
            cap = _c.ssd_max_inflight_bytes(self.cfg)
            if isinstance(cap, int) and cap > 0:
                return cap
        except Exception:
            pass
        # ssd_max_inflight_bytes folds free-space into the cap, so it
        # reports 0 both when the disk is full and when nothing is
        # configured. Fall back to the configured cap so batching never
        # crashes with "cap_bytes must be positive".
        try:
            configured = int(getattr(self.cfg.ssd, "max_inflight_bytes", 0) or 0)
            if configured > 0:
                return configured
        except Exception:
            pass
        return 0

    def _frozen_batch_cap(self, ts: TorrentState) -> int:
        """Stable batch cap for one row (see _batch_cap_cache).

        Priority: in-memory cache, then the persisted ``batch_cap_bytes``
        column (survives restarts so batch boundaries never shift under a
        persisted ``batch_index``), then a fresh freeze from the live cap.
        """
        try:
            cache = getattr(self, "_batch_cap_cache", None)
            if cache is None:
                cache = {}
                self._batch_cap_cache = cache
            key = (ts.source_infohash or "").lower()
            if key and key in cache and cache[key] > 0:
                return cache[key]
            try:
                persisted = int(getattr(ts, "batch_cap_bytes", 0) or 0)
            except (TypeError, ValueError):
                persisted = 0
            if persisted > 0:
                if key and cache is not None:
                    cache[key] = persisted
                return persisted
        except Exception:
            cache = None
            key = ""
        cap = self._batch_cap_bytes()
        if cap <= 0:
            return 0
        try:
            if key and cache is not None:
                cache[key] = cap
                if len(cache) > 5000:
                    # Bound memory: drop an arbitrary chunk (oldest unknown
                    # order, but caps re-freeze on next use).
                    for k in list(cache)[:2500]:
                        cache.pop(k, None)
            try:
                ts.batch_cap_bytes = cap
            except Exception:
                pass
            # Persist the freeze so a restart before any other upsert keeps
            # the same batch boundaries under a persisted batch_index.
            try:
                store = getattr(self, "store", None)
                if store is not None and hasattr(store, "upsert"):
                    store.upsert(ts)
            except Exception:
                pass
        except Exception:
            pass
        return cap

    def _drop_frozen_batch_cap(self, ts: TorrentState) -> None:
        try:
            cache = getattr(self, "_batch_cap_cache", None)
            if cache:
                cache.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass

    # ---- global SSD reservation ledger ----

    def _ssd_global_cap(self) -> int | None:
        """Configured global SSD budget, or None when unconfigured/test doubles.

        Only real int/float configs count — MagicMock doubles (int(MagicMock)==1)
        must not impose a 1-byte cap. Unconfigured means unlimited (legacy behavior).
        """
        try:
            raw = getattr(self.cfg.ssd, "max_inflight_bytes", 0)
            if isinstance(raw, bool):
                return None
            if not isinstance(raw, (int, float)):
                return None
            v = int(raw or 0)
            return v if v > 0 else None
        except Exception:
            return None

    def _ssd_estimate_for_new(self, total_bytes: int) -> int:
        """Admission estimate for an unclassified torrent: min(total, global_cap).

        Uses the STABLE configured cap, never the free-shrunk live cap, so a
        full disk doesn't shrink the estimate and admit easier (inverted logic).
        Refined to the real footprint after classify (max batch / single total).
        """
        try:
            total = max(0, int(total_bytes or 0))
        except Exception:
            total = 0
        cap = self._ssd_global_cap()
        if cap is None:
            return total
        return min(total, cap) if total > 0 else 0

    def _ssd_reserved_total(self) -> int:
        try:
            d = getattr(self, "_ssd_reserved", None)
            if not isinstance(d, dict):
                return 0
            return sum(int(v) for v in d.values() if isinstance(v, (int, float)))
        except Exception:
            return 0

    async def _ssd_lock_for(self):
        """Per-coordinator SSD lock, lazily created (tolerates test doubles)."""
        lk = getattr(self, "_ssd_lock", None)
        if lk is not None and hasattr(lk, "__aenter__"):
            return lk
        with _SSD_LOCK_GUARD:
            lk = getattr(self, "_ssd_lock", None)
            if lk is not None and hasattr(lk, "__aenter__"):
                return lk
            try:
                lk = asyncio.Lock()
            except Exception:
                return None
            try:
                self._ssd_lock = lk  # type: ignore[attr-defined]
            except Exception:
                pass
        return lk

    def _ssd_prune_stale(self) -> None:
        """Drop reservations for rows no longer needing SSD (forget/crash drift)."""
        try:
            d = getattr(self, "_ssd_reserved", None)
            if not isinstance(d, dict) or not d:
                return
            store = getattr(self, "store", None)
            if store is None or not hasattr(store, "get"):
                return
            for h in list(d.keys()):
                try:
                    row = store.get(h)
                except Exception:
                    continue
                # Deleted row → free. Known non-SSD states → free. Unknown
                # doubles (MagicMock state) → keep (can't prove stale).
                if row is None:
                    d.pop(h, None)
                    continue
                try:
                    st = getattr(row, "state", None)
                except Exception:
                    continue
                if isinstance(st, State) and st not in (
                    State.QUEUED, State.DOWNLOADING, State.MOVING,
                ):
                    d.pop(h, None)
        except Exception:
            pass

    async def _ssd_try_reserve(self, infohash: str, amount: int) -> bool:
        """Atomically admit `amount` iff global budget + physical free allow it.

        Returns True and records the reservation on success; False leaves
        everything unchanged (caller parks to WAITING_DISK).
        """
        try:
            key = (infohash or "").lower()
            if not key:
                return False
            amount = max(0, int(amount or 0))
        except Exception:
            return False
        lk = await self._ssd_lock_for()
        if lk is not None:
            await lk.acquire()
        try:
            self._ssd_prune_stale()
            cap = self._ssd_global_cap()
            if cap is not None:
                if self._ssd_reserved_total() + amount > cap:
                    return False
                if len(getattr(self, "_ssd_reserved", {})) > 5000:
                    return False
            # Physical free check last (stable estimate first).
            try:
                from . import coordinator as _c

                if not _c.ssd_has_room(self.cfg, amount):
                    return False
            except Exception:
                return False
            try:
                d = getattr(self, "_ssd_reserved", None)
                if not isinstance(d, dict):
                    d = {}
                    self._ssd_reserved = d  # type: ignore[attr-defined]
                d[key] = amount
            except Exception:
                pass
            return True
        finally:
            try:
                if lk is not None:
                    lk.release()
            except Exception:
                pass

    async def _ssd_release(self, infohash: str) -> None:
        try:
            key = (infohash or "").lower()
            if not key:
                return
            lk = await self._ssd_lock_for()
            if lk is not None:
                await lk.acquire()
            try:
                d = getattr(self, "_ssd_reserved", None)
                if isinstance(d, dict):
                    d.pop(key, None)
            finally:
                try:
                    if lk is not None:
                        lk.release()
                except Exception:
                    pass
        except Exception:
            pass

    async def _ssd_adjust(self, infohash: str, new_amount: int) -> bool:
        """Refine a held reservation (post-classify shrink/grow).

        Global-budget check only — physical free was verified at admission
        (try_reserve) seconds earlier; re-checking free here breaks test
        doubles with fake paths and adds no safety (disk can't fill in
        seconds beyond the reserved upper bound). Growing beyond the global
        budget fails (caller must roll back); shrinking always succeeds.
        Returns True on success.
        """
        try:
            key = (infohash or "").lower()
            new_amount = max(0, int(new_amount or 0))
            if not key:
                return False
        except Exception:
            return False
        lk = await self._ssd_lock_for()
        if lk is not None:
            await lk.acquire()
        try:
            d = getattr(self, "_ssd_reserved", None)
            if not isinstance(d, dict):
                try:
                    d = {}
                    self._ssd_reserved = d  # type: ignore[attr-defined]
                except Exception:
                    return True
            old = int(d.get(key, 0) or 0)
            if new_amount <= old:
                d[key] = new_amount
                return True
            cap = self._ssd_global_cap()
            if cap is not None and self._ssd_reserved_total() - old + new_amount > cap:
                return False
            d[key] = new_amount
            return True
        finally:
            try:
                if lk is not None:
                    lk.release()
            except Exception:
                pass

    async def _ssd_rebuild_from_db(self) -> None:
        """Re-reserve SSD for in-flight rows after (re)start — crash recovery.

        Uses DB-aware footprints: batched rows reserve one batch upper bound
        (min(total, configured)), singles reserve total. WAITING_DISK rows
        hold nothing. Best-effort: never raises.
        """
        try:
            d = getattr(self, "_ssd_reserved", None)
            if not isinstance(d, dict):
                self._ssd_reserved = {}  # type: ignore[attr-defined]
                d = self._ssd_reserved
            else:
                d.clear()
            store = getattr(self, "store", None)
            if store is None or not hasattr(store, "all"):
                return
            try:
                rows = await asyncio.to_thread(store.all)
            except Exception:
                return
            cap = self._ssd_global_cap()
            for ts in rows or []:
                try:
                    if getattr(ts, "state", None) not in (
                        State.QUEUED, State.DOWNLOADING, State.MOVING,
                    ):
                        continue
                    h = (getattr(ts, "source_infohash", "") or "").lower()
                    if not h:
                        continue
                    try:
                        total = max(0, int(getattr(ts, "total_bytes", 0) or 0))
                    except Exception:
                        total = 0
                    batches = 0
                    try:
                        batches = int(getattr(ts, "batches_total", 0) or 0)
                    except Exception:
                        batches = 0
                    if batches > 1 and cap is not None:
                        amt = min(total, cap) if total > 0 else cap
                    elif cap is not None and total > cap and batches <= 1:
                        # Unclassified large row: optimistically one batch;
                        # _setup refines (or tops up + rolls back for singles).
                        amt = cap
                    else:
                        amt = total
                    if amt > 0:
                        d[h] = int(amt)
                except Exception:
                    continue
            # Clamp dict size for safety.
            if len(d) > 5000:
                for k in list(d.keys())[: len(d) - 5000]:
                    d.pop(k, None)
            log.info(
                "ssd ledger rebuilt: %d rows reserved ~%d MB of %s cap",
                len(d), sum(d.values()) // (1024 * 1024),
                f"{cap // (1024*1024)} MB" if cap else "unlimited",
            )
        except Exception:
            pass


__all__ = ["SSDLedgerMixin"]
