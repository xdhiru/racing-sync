"""Queued/download admission: lane scheduling and slot admission.

Split from coordinator_admit_queue.py; `AdmitMixin` composes this mixin
next to `QueueAdmitMixin` (they share `self`)."""

import asyncio
import logging
import time
from pathlib import Path

from .coordinator_errors import AbandonedError
from .coordinator_paths import _safe_ssd_join
from .io_bounds import offload
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class LaneAdmitMixin:
    # How long a still-full WAITING_DISK row stays quiet before its next
    # SSD re-check. Batches drain via MOVING in the meantime.
    WAITING_DISK_RECHECK_SECONDS = 60.0

    def _note_waiting_disk_check(self, ts: TorrentState) -> None:
        """Schedule the next quiet re-check for a parked row (bounded map).

        Parked rows that are forgotten or renamed never reach the pop on
        promotion, so the map is capped like the other per-hash notes.
        """
        try:
            _wd = getattr(self, "_waiting_disk_next_check", None)
            if not isinstance(_wd, dict):
                _wd = {}
                self._waiting_disk_next_check = _wd  # type: ignore[attr-defined]
            _wd[(ts.source_infohash or "").lower()] = (
                time.monotonic() + self.WAITING_DISK_RECHECK_SECONDS
            )
            if len(_wd) > 5000:
                for _k in list(_wd.keys())[: len(_wd) - 5000]:
                    _wd.pop(_k, None)
        except Exception:
            pass

    async def _wait_disk_then_queue(self, ts: TorrentState) -> None:
        # Global ledger re-check (quiet 60s cadence): reserves before QUEUED
        # so a new arrival mid-batch can't over-commit the budget that batch
        # completion is about to need.
        if self._stop:
            return
        if self._is_watch_row(ts):
            # Same-content election also gates WAITING_DISK promotion: no
            # client entry exists yet, so deferring here is as cheap as NEW.
            try:
                _proceed, _owner = self._watch_election(ts)
            except Exception:
                _proceed, _owner = True, None
            if not _proceed:
                _owner_desc = (
                    f"{(_owner.source_infohash or '')[:10]} ({_owner.state.value})"
                    if _owner is not None
                    else "unknown owner"
                )
                log.debug(
                    "watch-dir: %s stays waiting_disk — same content owned by %s",
                    ts.source_name[:60],
                    _owner_desc,
                )
                # Refresh the quiet-wait window like the SSD-full path so a
                # deferred row re-checks election at most once per interval.
                self._note_waiting_disk_check(ts)
                return
        # Parked-row on-SSD shortcut (mirrors the admit-time one above):
        # a complete dest entry may have appeared — or been added
        # manually — while parked. It needs no budget, so skip straight
        # to QUEUED instead of waiting out a full ledger for bytes that
        # will never download. Fail-open: any doubt reserves normally.
        try:
            _pre = await self._dest_complete_entry(
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)]
            )
        except Exception:
            _pre = None
        if _pre is not None:
            log.info(
                "watch-dir: %s already complete on dest; skipping SSD budget", ts.source_name[:60]
            )
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop((ts.source_infohash or "").lower(), None)
            except Exception:
                pass
            self.transition(ts, State.QUEUED)
            return
        # Cap before reserve: a promotion that can't download yet must not
        # churn the ledger (reserve → find full → release) every quiet
        # window. Live-worker count, off-loop. Non-int configs skip.
        try:
            _pre_max_raw = getattr(self.cfg, "max_active_downloads", 0)
            _pre_max = (
                int(_pre_max_raw or 0)
                if isinstance(_pre_max_raw, (int, float)) and not isinstance(_pre_max_raw, bool)
                else 0
            )
        except (TypeError, ValueError):
            _pre_max = 0
        if _pre_max > 0 and await self._live_download_count() >= _pre_max:
            self._note_waiting_disk_check(ts)
            log.debug(
                "downloads full (%d/%d); %s stays waiting_disk (no reserve churn)",
                _pre_max,
                _pre_max,
                ts.source_name[:60],
            )
            return
        needed = self._ssd_estimate_for_new(ts.total_bytes)
        admitted = await self._ssd_try_reserve(ts.source_infohash, needed)
        if not admitted:
            # Full total didn't fit, but a partially-moved row may need far
            # less than its total — retry against remaining batches before
            # parking another 60s.
            try:
                rem = await self._estimate_remaining_bytes(ts)
            except Exception:
                rem = None
            if rem is not None and rem < needed:
                log.info(
                    "retrying %s against remaining ~%d MB (full ~%d MB did not fit)",
                    ts.source_name[:60],
                    rem // (1024 * 1024),
                    needed // (1024 * 1024),
                )
                admitted = await self._ssd_try_reserve(ts.source_infohash, rem)
                if admitted:
                    needed = rem
        if not admitted:
            # Still full: an unclassified row (never reached batch setup,
            # so no cursors for the retry above) may still be batchable —
            # size one batch from its persisted .torrent bytes instead of
            # demanding the whole cap. Blob fetch is off-loop and runs
            # only on this otherwise-park path.
            try:
                _blob = getattr(ts, "_blob", None) or None
                if not _blob:
                    try:
                        _blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                    except Exception:
                        _blob = None
                try:
                    _foot = self._blob_admit_footprint(ts, _blob, ts.total_bytes or 0)
                except Exception:
                    _foot = None
            except Exception:
                _foot = None
            if isinstance(_foot, int) and not isinstance(_foot, bool) and 0 < _foot < needed:
                log.info(
                    "retrying %s against ~%d MB batch footprint (full ~%d MB did not fit)",
                    ts.source_name[:60],
                    _foot // (1024 * 1024),
                    needed // (1024 * 1024),
                )
                admitted = await self._ssd_try_reserve(ts.source_infohash, _foot)
                if admitted:
                    needed = _foot
        if admitted:
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop((ts.source_infohash or "").lower(), None)
            except Exception:
                pass
            # A WAITING_DISK promotion inside a worker bypasses the tick's
            # max_active_downloads gate (checked at schedule time) — re-check
            # here so parked bursts can't overshoot concurrent downloads.
            # The SSD reservation is already held; release it if we stay parked
            # or if the promotion itself is refused (abandoned row).
            # Non-int configs (test doubles) skip the count check.
            _max_dl_raw = getattr(self.cfg, "max_active_downloads", 0)
            if isinstance(_max_dl_raw, bool) or not isinstance(_max_dl_raw, (int, float)):
                _max_dl = 0
            else:
                _max_dl = int(_max_dl_raw or 0)
            if _max_dl > 0:
                # Live workers only: parked DB rows with no worker must
                # not pin the cap (they already survived the pre-reserve
                # check above; this re-check closes the reserve race).
                _active_dl = await self._live_download_count()
                if _active_dl >= _max_dl:
                    # Downloads full: release the just-made reservation and
                    # stay parked (never FAILED — space, not an error).
                    await self._ssd_release(ts.source_infohash)
                    self._note_waiting_disk_check(ts)
                    log.debug(
                        "downloads full (%d/%d); %s stays waiting_disk",
                        _active_dl,
                        _max_dl,
                        ts.source_name[:60],
                    )
                    return
            try:
                self.transition(ts, State.QUEUED)
            except (AbandonedError, ValueError) as e:
                # Row forgotten/failed concurrently after we reserved SSD:
                # release the reservation instead of leaking it until prune.
                try:
                    await self._ssd_release(ts.source_infohash)
                except Exception:
                    pass
                log.info(
                    "waiting_disk admit aborted for %s (%s)", (ts.source_infohash or "")[:10], e
                )
            return
        # Still full: stay parked quietly until the next interval instead of
        # hot-looping every tick while batch moves drain.
        self._note_waiting_disk_check(ts)
        log.debug(
            "ssd still full; %s stays waiting_disk (need ~%d MB, re-check in %ds)",
            ts.source_name[:60],
            needed // (1024 * 1024),
            int(self.WAITING_DISK_RECHECK_SECONDS),
        )

    async def _live_download_count(self, *, exclude: str = "") -> int:
        """Live DOWNLOADING workers (DB DOWNLOADING ∩ running set).

        One offloaded round-trip plus an in-memory intersect: parked
        rows with no worker (stalled, forgotten mid-flight) never pin
        the cap. `exclude` (a row judging its own admission) is not
        counted — a row must never pin its own slot. Never raises.
        """
        try:
            running = getattr(self, "_running_infohashes", None)
            if not isinstance(running, set) or not running:
                return 0
            _ex = (exclude or "").lower()
            rows = await offload(self.store.list_by_state, State.DOWNLOADING)
            return sum(
                1
                for t in rows or ()
                if (t.source_infohash or "").lower() in running
                and (t.source_infohash or "").lower() != _ex
            )
        except Exception:
            return 0

    async def _try_admit_download(self, ts: TorrentState) -> bool:
        """Admit one QUEUED row into downloading; False parks it (stay QUEUED).

        The tick loop only gates rows that are QUEUED at snapshot time, so a
        fresh-start burst of NEW workers can sail past it into DOWNLOADING
        (live incident: 5 concurrent against max 3). This edge check closes
        it.         Admission is atomic in the event loop (sync check+record, no
        await between), so same-tick workers racing through slow RPCs can't
        all read a stale count; the live-worker count itself is refreshed
        off-loop just before. Rows already DOWNLOADING are grandfathered
        (they drain; only new admissions wait). The client entry (if any)
        is left paused as-is and the SSD reservation stays held — the next
        tick retries through the same path with no delete/re-add churn and
        no double reservation. Stale admissions (dead workers) are dropped
        via the live `_running_infohashes` set, so a cancelled worker can
        only over-park transiently, never deadlock. Non-int configs (test
        doubles) always admit. Never raises.
        """
        key = (ts.source_infohash or "").lower()
        try:
            # Force-start bypass (operator override): skip both the cap
            # and the admission record (forced rows are invisible to the
            # gate — over-admission is the intent, and it disarms on
            # leaving the lane).
            if key and self._is_forced(key):
                return True
            raw = getattr(self.cfg, "max_active_downloads", 0)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return True
            max_dl = int(raw or 0)
            if max_dl <= 0:
                return True
            adm = getattr(self, "_download_admissions", None)
            if not isinstance(adm, set):
                adm = set()
                self._download_admissions = adm
            running = getattr(self, "_running_infohashes", None)
            if isinstance(running, set):
                # Drop admissions whose worker is gone (cancelled/shutdown):
                # only live workers pin slots.
                adm.intersection_update(running)
            n_down = await self._live_download_count(exclude=key)
            others = len(adm - {key}) if key else len(adm)
            if n_down + others >= max_dl:
                # Internal gate: the waiter path logs the one operator-
                # facing "downloads full" line. Keep the counts at debug so
                # a count disagreement cannot flood the log every second.
                log.debug(
                    "downloads full (%d/%d); %s stays queued",
                    n_down + others,
                    max_dl,
                    ts.source_name[:60],
                )
                if self._abandoned(ts):
                    # Row forgotten after the snapshot: stay silent instead
                    # of upsert-resurrecting it as QUEUED.
                    return False
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
                return False
            if key:
                adm.add(key)
            return True
        except Exception:
            return True

    _LANE_KINDS = {"dl": ("queued", "downloading"), "move": ("moving",)}

    def _lane_cap(self, lane: str) -> int | None:
        """Lane slot cap, or None when unconfigured (fail-open admit)."""
        try:
            raw = getattr(
                self.cfg, "max_active_downloads" if lane == "dl" else "max_concurrent_moves", 0
            )
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return None
            val = int(raw or 0)
            return val if val > 0 else None
        except Exception:
            return None

    def _lane_live_count(self, lane: str) -> int:
        """Running, non-parked lane usage (handles; no handles = 0)."""
        try:
            from .task import handles_for  # noqa: F401 (re-export guard)
        except Exception:
            return 0
        try:
            _reg = getattr(self, "_task_handles", None)
            if not isinstance(_reg, dict):
                return 0
            _kinds = self._LANE_KINDS.get(lane, ())
            n = 0
            for _h, _set in list(_reg.items()):
                if not isinstance(_set, set):
                    continue
                for _x in list(_set):
                    try:
                        if (
                            getattr(_x, "kind", "") in _kinds
                            and not getattr(_x, "queued", False)
                            and not getattr(_x, "cancelled", False)
                        ):
                            n += 1
                            break
                    except Exception:
                        continue
            return n
        except Exception:
            return 0

    def _is_forced(self, key: str) -> bool:
        """True when /force armed this hash (in-memory bypass)."""
        try:
            _f = getattr(self, "_forced", None)
            return isinstance(_f, set) and (key or "").lower() in _f
        except Exception:
            return False

    def _queue_lane_event(self, lane: str, key: str):
        """Register (or fetch) the lane-wait event for a hash."""
        try:
            _lanes = getattr(self, "_queued_lane", None)
            if not isinstance(_lanes, dict):
                _lanes = {}
                self._queued_lane = _lanes  # type: ignore[attr-defined]
            _q = _lanes.get(lane)
            if not isinstance(_q, dict):
                _q = {}
                _lanes[lane] = _q
            if key not in _q:
                try:
                    import time as _time

                    _q[key] = _time.monotonic()
                except Exception:
                    _q[key] = 0.0
                if len(_q) > 500:
                    try:
                        _oldest = sorted(_q.items(), key=lambda kv: kv[1])[: len(_q) - 500]
                        for _k, _ in _oldest:
                            _q.pop(_k, None)
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            from .task import handles_for

            for _hd in handles_for(self, key):
                try:
                    return _hd.event
                except Exception:
                    continue
        except Exception:
            pass
        return None

    def check_running_tasks(self, ts, lane: str = "dl"):
        """(queued, event): can this row start `lane` now?

        Force-armed rows never queue. Otherwise compares live lane usage
        (running, non-parked handles) against the cap; when full the row
        registers in the lane queue and gets its handle event (or None
        when handle-less — caller treats that as admittable, today's
        edge behavior). Never raises.
        """
        try:
            key = (getattr(ts, "source_infohash", "") or "").strip().lower()
        except Exception:
            return False, None
        if not key:
            return False, None
        try:
            if self._is_forced(key):
                return False, None
            cap = self._lane_cap(lane)
            if cap is None:
                return False, None
            if self._lane_live_count(lane) < cap:
                try:
                    _lanes = getattr(self, "_queued_lane", None)
                    if isinstance(_lanes, dict):
                        _q = _lanes.get(lane)
                        if isinstance(_q, dict):
                            _q.pop(key, None)
                except Exception:
                    pass
                return False, None
            # Full lane: only worker-backed rows park (their handle event
            # is the wake primitive). Handle-less callers stay admittable
            # — today's edge behavior, no phantom queue entries.
            try:
                from .task import handles_for

                _hs = handles_for(self, key)
            except Exception:
                _hs = []
            if not _hs:
                return False, None
            self._queue_lane_event(lane, key)
            try:
                return True, _hs[0].event
            except Exception:
                return True, None
        except Exception:
            return False, None

    async def start_from_queued(self) -> int:
        """Wake queued lane-waiters that now fit, by priority.

        Runs each scheduler tick while the queue is non-empty: fetches
        queued rows (bounded), drops gone/unforceable ones, and sets the
        events of the highest-priority rows up to the free slots. Woken
        workers re-check admission authoritatively (an event is a hint,
        not a grant). Returns woken count. Never raises.
        """
        total = 0
        try:
            try:
                from .coordinator import _sched_priority
            except Exception:
                _sched_priority = None  # type: ignore[assignment]
            _lanes = getattr(self, "_queued_lane", None)
            if not isinstance(_lanes, dict) or not _lanes:
                return 0
            for lane in ("dl", "move"):
                try:
                    _q = _lanes.get(lane)
                    if not isinstance(_q, dict) or not _q:
                        continue
                    cap = self._lane_cap(lane)
                    if cap is None:
                        # Unconfigured lane: wake everyone (admit is open).
                        _free = len(_q)
                    else:
                        _free = cap - self._lane_live_count(lane)
                    if _free <= 0:
                        continue
                    _hashes = list(_q.keys())[:50]
                    _rows: list = []
                    for _h in _hashes:
                        try:
                            _r = self.store.get(_h, include_blob=False)
                        except Exception:
                            _r = None
                        if _r is None:
                            try:
                                _q.pop(_h, None)
                            except Exception:
                                pass
                            continue
                        _rows.append(_r)
                    if _sched_priority is not None:
                        try:
                            _rows.sort(key=_sched_priority)
                        except Exception:
                            pass
                    for _r in _rows:
                        if total >= _free and cap is not None:
                            break
                        try:
                            _rk = (getattr(_r, "source_infohash", "") or "").lower()
                        except Exception:
                            continue
                        if not _rk or _rk not in _q:
                            continue
                        try:
                            _st = getattr(getattr(_r, "state", None), "value", "")
                        except Exception:
                            _st = ""
                        _want = ("queued", "downloading") if lane == "dl" else ("moving",)
                        if _st not in _want and not self._is_forced(_rk):
                            try:
                                _q.pop(_rk, None)
                            except Exception:
                                pass
                            continue
                        if getattr(_r, "skipped", 0):
                            continue
                        try:
                            from .task import handles_for

                            _hs = handles_for(self, _rk)
                        except Exception:
                            _hs = []
                        if not _hs:
                            try:
                                _q.pop(_rk, None)
                            except Exception:
                                pass
                            continue
                        try:
                            _hs[0].event.set()
                            _q.pop(_rk, None)
                            total += 1
                        except Exception:
                            continue
                except Exception:
                    continue
        except Exception:
            pass
        return total

    async def _park_queued_for_download_slot(
        self,
        ts,
        *,
        holds_slot: bool = False,
    ) -> bool:
        """Event-park when the download lane is full; True = exit worker.

        Replaces the old park-to-QUEUED upsert: the worker idles on its
        handle event (download_sem released while waiting, re-acquired
        after) instead of exiting + respawning + re-running intake and
        upserting QUEUED every tick. Each wake re-checks admission
        authoritatively; row-gone/shutdown exits True (caller returns).
        """
        try:
            key = (getattr(ts, "source_infohash", "") or "").strip().lower()
        except Exception:
            return False
        # Waiter identity: only worker-backed rows (spawned → handle with
        # an event) can event-park. Handle-less callers (tests, exotic
        # paths) keep the classic park-exit below instead of spinning.
        # No row-existence check here: _try_admit_download already stays
        # silent for forgotten rows, and forget/API paths cancel the
        # worker task itself (cancellation unwinds the wait).
        _ever_queued = False
        while not getattr(self, "_stop", False):
            if key and self._is_forced(key):
                try:
                    from .task import handles_for

                    for _hd in handles_for(self, key):
                        try:
                            _hd.queued = False
                        except Exception:
                            pass
                except Exception:
                    pass
                return False
            queued, ev = self.check_running_tasks(ts, "dl")
            if not queued:
                try:
                    from .task import handles_for

                    for _hd in handles_for(self, key):
                        try:
                            _hd.queued = False
                        except Exception:
                            pass
                except Exception:
                    pass
                # Second gate (DB-truth edge): the handle count won the
                # race but the live count says full. Join the lane queue and
                # event-park; re-checking every second only re-logged the
                # same "downloads full" line for every waiting row.
                try:
                    admitted = await self._try_admit_download(ts)
                except Exception:
                    admitted = False
                if admitted:
                    return False
                try:
                    if self._abandoned(ts):
                        return True
                except Exception:
                    pass
                ev = self._queue_lane_event("dl", key)
                if ev is None:
                    # No waiter identity: classic park-exit (row stays
                    # QUEUED; the next tick respawns).
                    return True
            if not _ever_queued:
                log.info(
                    "downloads full; %s waits for a slot (no churn)",
                    (getattr(ts, "source_name", "") or key)[:60],
                )
            _ever_queued = True
            try:
                from .task import handles_for

                for _hd in handles_for(self, key):
                    try:
                        _hd.queued = True
                    except Exception:
                        pass
            except Exception:
                pass
            if ev is None:
                return True
            try:

                async def _wait_ev(ev=ev) -> None:
                    # Bind `ev` explicitly: the closure would otherwise
                    # capture the loop variable late (next tick's event).
                    try:
                        await asyncio.wait_for(ev.wait(), 60.0)
                    except TimeoutError:
                        pass
                    try:
                        ev.clear()
                    except Exception:
                        pass

                await self._run_outside_download_slot(_wait_ev(), holds_slot)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        return True

    async def _run_outside_download_slot(self, coro, holds: bool):
        """Run `coro` with the caller's download_sem slot released.

        `_do_queued` runs under download_sem (QUEUED admission), but fuse
        re-injects are minutes of SFTP/Prowlarr/RPCs that download nothing:
        holding the slot starves real QUEUED setups behind them. Balanced
        release/re-acquire around the inject only; anything else (and
        callers holding no slot) runs inline. Never raises by itself —
        coro errors propagate to the caller.
        """
        if not holds:
            return await coro
        try:
            _sem = self.download_sem
        except Exception:
            return await coro
        if _sem is None:
            return await coro
        _sem.release()
        try:
            return await coro
        finally:
            await _sem.acquire()

    async def _drop_failed_entry(self, h: str, ts, files, why: str) -> None:
        """Drop a FAILED row's just-added entry without sibling damage.

        The entry never downloaded, but the client preallocates shells at
        add time: drop the entry (delete_files=False) and unlink exactly
        the fetched file list, each contained to the row's save dir (files
        only — never dirs, never through symlinks). A metadata-driven
        wipe could take shared-dir sibling data (same normalized top,
        different hash). Best-effort: failures only log.
        """
        try:
            if h:
                await self.dest_client.delete(h, delete_files=False)
        except Exception as e:  # noqa: BLE001
            log.warning("failed to drop entry %s: %s", (h or "")[:10], e)
        try:
            _base = Path(ts.save_path or "")
        except Exception:
            return
        for _f in files or []:
            try:
                _nm = getattr(_f, "name", "") or ""
                _p = _safe_ssd_join(_base, _nm)
                if _p is None or _p.is_symlink():
                    continue
                if _p.is_file():
                    _p.unlink()
                    log.info("removed preallocated shell %s (%s)", _nm, why)
            except OSError:
                continue
            except Exception:
                continue
