"""Queued/download admission: lane scheduling, batching and setup.

Extracted from coordinator_admit.py; `AdmitMixin` composes this mixin."""

import asyncio
import logging
import time
from pathlib import Path

from .batcher import Batch, make_batches, make_file_batches
from .classifier import oversize_single_file
from .clients.abstract import TorrentFile
from .coordinator_content import normalize_content_name, route_kind_is_sorted
from .coordinator_errors import (
    AbandonedError,
    BatchMoveIncompleteError,
    TorrentCheckingError,
    is_retryable_client_error,
)
from .coordinator_paths import _left_on_disk, _safe_ssd_join
from .io_bounds import offload, rpc
from .recovery import _save_path_on_ssd, _ssd_roots_known, find_content_on_ssd
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


# Blob-parse memo TTL: same infohash = same bytes, so expiry only bounds
# memory across huge intakes, never correctness.
_BLOB_PARSE_TTL_S = 300.0

#: Consecutive empty file lists after add before the row fails. The client
#: parses metadata in seconds, so an empty list that survives several
#: admission retries is a broken torrent, not a slow one.
_SETUP_EMPTY_FILES_FAIL_TRIES = 5


class QueueAdmitMixin:
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

    # ---- state: QUEUED ----

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
                log.info(
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
                # race but the live count says full — back to the queue
                # instead of over-admitting (the M2 burst incident).
                try:
                    if await self._try_admit_download(ts):
                        return False
                except Exception:
                    return False
                if not _ever_queued:
                    # No waiter identity: classic park-exit (row stays
                    # QUEUED; the next tick respawns) — never spin.
                    return True
                # Briefly breather before re-queueing (disagreement
                # between handle/live counts must not tight-loop).
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
                continue
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
            log.info(
                "downloads full; %s waits for a slot (no churn)",
                (getattr(ts, "source_name", "") or key)[:60],
            )
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

    async def _do_queued(self, ts: TorrentState, *, _holds_download_slot: bool = False) -> None:
        blob: bytes = ts._blob or ts.cross_seed_blob
        if not blob:
            blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            if blob:
                ts.cross_seed_blob = blob
                ts._blob = blob
        if not blob:
            log.error("missing _blob for %s; cannot add", ts.source_infohash[:10])
            self.transition(ts, State.FAILED, error="no blob")
            return

        # Re-check that the torrent isn't already present on VPS2.
        check_hashes = [
            h for h in (ts.source_infohash, ts.cross_seed_infohash, ts.dest_infohash) if h
        ]
        existing = await rpc(
            self.dest_client.list_torrents(hashes=check_hashes),
            getattr(self, "cfg", None),
            "dest queued list_torrents",
        )
        if existing:
            fuse_mounts = [
                str(path).rstrip("/\\").replace("\\", "/")
                for path in (self.rt.seed.mount, self.rt.seed.mount_unsorted)
                if path is not None
            ]

            # Prefer fuse-complete > SSD-complete > first-listed: adopting
            # an SSD partial while a fuse-complete copy exists strands the
            # seed. Completeness here is client-side; the fuse branch
            # re-verifies bytes before DONE.
            def _rank(t):
                try:
                    _sp = (getattr(t, "save_path", "") or "").rstrip("/\\").replace("\\", "/")
                    _on_fuse = any(
                        _sp == fm or _sp.startswith(fm + "/") for fm in fuse_mounts if fm
                    )
                except Exception:
                    _on_fuse = False
                try:
                    _complete = bool(t.is_complete())
                except Exception:
                    _complete = False
                return 0 if (_on_fuse and _complete) else (1 if _complete else 2)

            try:
                ext = min(existing, key=_rank)
            except Exception:
                ext = existing[0]
            save_path = ext.save_path.rstrip("/\\").replace("\\", "/")
            # Single on-fuse implementation (case-folded, mount-aware):
            # an SSD-pointing entry must never have its bytes statted
            # as fuse — data never seeded would verify as present.
            try:
                on_fuse = bool(self._save_path_is_on_fuse(getattr(ext, "save_path", "")))
            except Exception:
                on_fuse = False
            try:
                complete = bool(ext.is_complete())
            except Exception:
                complete = False
            if on_fuse and not complete:
                # An incomplete entry on the fuse mount must never be
                # adopted as a local SSD download: resuming it would write
                # bytes through the fuse mount, and the later move stage
                # would treat that path as its SSD source. Route the row to
                # the normal fuse-gated re-add flow instead and leave
                # ts.save_path untouched (it must stay an SSD path).
                _ext_hash = getattr(ext, "hash", "")
                if isinstance(_ext_hash, str) and _ext_hash.strip():
                    ts.dest_infohash = _ext_hash.lower()
                log.warning(
                    "torrent %s already exists incomplete on fuse at %s; "
                    "not adopting as an SSD download — routing to RE_ADDING",
                    (ts.source_infohash or "")[:10],
                    save_path,
                )
                self.transition(ts, State.RE_ADDING)
                return
            if on_fuse and complete:
                # Verify the bytes are really behind the fuse path: a torrent
                # added with skip_check=True reports complete even when its
                # files were never moved. Missing bytes must never mark DONE —
                # and must never FAILED either (a warming/dead mount also
                # shows nothing, and FAILED would trigger re-downloads).
                try:
                    fuse_files = await rpc(
                        self.dest_client.get_torrent_files(ext.hash),
                        getattr(self, "cfg", None),
                        "dest fuse get_torrent_files",
                    )
                    fuse_list_failed = False
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not list fuse torrent files for %s: %s",
                        ts.source_infohash[:10],
                        e,
                    )
                    fuse_files = []
                    fuse_list_failed = True
                fuse_expected = [
                    (f.name, f.size_bytes) for f in fuse_files if getattr(f, "name", "")
                ]
                if fuse_list_failed or not fuse_expected:
                    # Fail closed: an unlistable or empty file list must never
                    # mark DONE (would strand unseeded bytes as complete).
                    # Stay QUEUED so the next tick retries via this same check.
                    log.warning(
                        "fuse verification inconclusive for %s (list_failed=%s, files=%d); "
                        "staying queued",
                        ts.source_infohash[:10],
                        fuse_list_failed,
                        len(fuse_expected),
                    )
                    try:
                        self.store.upsert(ts)
                    except Exception:
                        pass
                    return
                fuse_missing = await self._missing_fuse_files(Path(save_path), fuse_expected)
                if fuse_missing:
                    ssd_root = find_content_on_ssd(self.cfg, fuse_expected)
                    if ssd_root is not None:
                        # The bytes sit on SSD (never-moved data behind a
                        # fuse-pointing entry): drive the normal SSD flow so
                        # they get moved properly. No resume — the entry
                        # points at fuse; DOWNLOADING re-polls then MOVING
                        # moves the SSD bytes and replaces the entry.
                        log.warning(
                            "torrent %s reports complete on fuse %s but %d/%d files missing; "
                            "content found on SSD at %s — resuming SSD flow instead of DONE",
                            ts.source_infohash[:10],
                            save_path,
                            len(fuse_missing),
                            len(fuse_expected),
                            ssd_root,
                        )
                        ts.dest_infohash = ext.hash.lower()
                        ts.save_path = str(ssd_root)
                        if await self._park_queued_for_download_slot(
                            ts, holds_slot=_holds_download_slot
                        ):
                            return
                        self.transition(ts, State.DOWNLOADING)
                        return
                    log.warning(
                        "torrent %s reports complete on fuse %s but %d/%d files missing "
                        "(e.g. %s); routing to RE_ADDING for gated retry instead of DONE",
                        ts.source_infohash[:10],
                        save_path,
                        len(fuse_missing),
                        len(fuse_expected),
                        fuse_missing[0],
                    )
                    self.transition(ts, State.RE_ADDING)
                    return
                log.info(
                    "torrent %s is already completed on VPS2 fuse mount; marking DONE",
                    ts.source_infohash[:10],
                )
                ts.dest_infohash = ext.hash.lower()
                ts.save_path = ext.save_path
                # Classify the verified fuse files so _target_mount_for()
                # routes later RE_ADDING/late-seed gates at the right mount.
                # Without this a season pack fast-tracked here keeps
                # kind="unknown" (→ unsorted) while its bytes live at the
                # default mount, and every later gate checks the wrong dir.
                try:
                    # Same patch-target treatment as above (classify).
                    from . import coordinator as _coordinator

                    fast_kind = _coordinator.classify(fuse_files, self.cfg).kind
                except Exception:  # noqa: BLE001
                    fast_kind = "unknown"
                if fast_kind and fast_kind != ts.classification_kind:
                    ts.classification_kind = fast_kind
                    try:
                        self.store.upsert(ts)
                    except Exception:  # noqa: BLE001
                        pass
                if self.rt.acquire.inject_racing_torrents_to_fuse:
                    if self._is_watch_row(ts):
                        # Mirror RE_ADDING step 1: inject every persisted
                        # watch blob (dropped + sacrificial + discovered),
                        # each fuse-gated on its own — the fast path must
                        # not mark DONE while a dropped copy is unseeded. A
                        # transient WebUI hiccup stays QUEUED for retry
                        # instead of failing the row (any escape FAILEDs it
                        # in the worker wrapper). Origin (not the SSD
                        # flavour label, which sacrificial/public rows
                        # overwrite) decides the injection set.
                        try:
                            await self._run_outside_download_slot(
                                self._re_inject_watch_dir_torrents(ts), _holds_download_slot
                            )
                        except Exception as e:  # noqa: BLE001
                            if not is_retryable_client_error(e):
                                # Fatal disk/permission errors (ENOSPC,
                                # EACCES, …) must fail loudly, never park.
                                raise
                            log.warning(
                                "watch re-inject hit transient dest error for %s (%s); staying queued",
                                ts.source_name[:60],
                                e,
                            )
                            try:
                                self.store.upsert(ts)
                            except Exception:
                                pass
                            return
                    else:
                        await self._run_outside_download_slot(
                            self._re_inject_racing_torrents(ts), _holds_download_slot
                        )
                self.transition(ts, State.DONE)
                return

            # Placement gate: only adopt entries under a known SSD/dest
            # root. A foreign save_path (stale path, other mount) would
            # later be moved as if it were our SSD source; fail closed
            # instead of adopting arbitrary directories. Unresolvable roots
            # (MagicMock cfg in unit tests) keep legacy adopt behavior.
            _foreign_known = False
            if save_path:
                try:
                    _roots_known = _ssd_roots_known(self.cfg)
                    _on_ssd = _save_path_on_ssd(self.cfg, save_path)
                except Exception:
                    _roots_known = False
                    _on_ssd = False
                _foreign_known = bool(_roots_known and not _on_ssd)
            if _foreign_known:
                log.error(
                    "torrent %s already on VPS2 but at foreign path %s "
                    "(not under an SSD/dest root); refusing to adopt/resume it",
                    (ts.source_infohash or "")[:10],
                    save_path,
                )
                self.transition(
                    ts,
                    State.FAILED,
                    error=(
                        f"existing dest entry at foreign path {save_path!r} "
                        "(not SSD/dest or fuse); refusing to adopt it as a "
                        "download source"
                    ),
                )
                return
            log.info(
                "torrent already on VPS2: %s (complete=%s, on_fuse=%s)",
                ts.source_infohash[:10],
                complete,
                on_fuse,
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = ext.save_path
            if await self._park_queued_for_download_slot(ts, holds_slot=_holds_download_slot):
                return
            # Batch-priority re-verify before resume: this entry may predate
            # its priorities (setup-transient park, restart between add and
            # deselect, recovery re-add). Resuming full-wanted downloads the
            # whole season past the SSD cap — and qB pre-allocates every file
            # at add time even while paused, so a late deselect never reclaims
            # the bytes. Singles resolve to no batches and resume untouched.
            try:
                await self._prepare_next_batch(ts, strict=True)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "batch priorities not verified for %s (%s); staying queued",
                    ts.source_infohash[:10],
                    e,
                )
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
                return
            try:
                await self.dest_client.resume(ext.hash)
            except Exception as e:  # noqa: BLE001
                # Transient qB hiccup (registration lag, timeout): stay
                # QUEUED so the next tick retries via this same check.
                log.warning(
                    "resume %s hit transient dest error: %s; staying queued", ext.hash[:10], e
                )
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
                return
            self.transition(ts, State.DOWNLOADING)
            return

        # Fully-remote shortcut FIRST: every byte already sits verified on
        # fuse, so adding an SSD entry just to deselect all its files
        # (0-file corpse) and delete/replace it in RE_ADDING is pure churn.
        # It also must precede the slot-park below — a remote row consumes
        # no download slot, and parking it first would record a bogus
        # admission that pins the cap for rows that really download. Skip
        # the SSD add entirely and go fuse-gated RE_ADDING, which
        # re-verifies before injecting. Fail-open: any doubt adds normally.
        try:
            _fully_remote = await self._blob_fully_remote(blob, cache_key=ts.source_infohash)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info(
                "content for %s already fully on remote; skipping SSD add",
                ts.source_name[:60],
            )
            self.transition(ts, State.RE_ADDING)
            return
        # Brand-new entry: gate before adding so a fresh-start burst of NEW
        # workers can't sail past the tick's snapshot-QUEUED-only cap into
        # concurrent DOWNLOADING (live incident: 5 at once against max 3).
        if await self._park_queued_for_download_slot(ts, holds_slot=_holds_download_slot):
            return
        result = await self.dest_client.add_torrent(
            torrent_files=[blob],
            save_path=ts.save_path,
            category="racing",
            paused=True,
            skip_check=False,
        )
        if not result.accepted:
            self.transition(
                ts,
                State.FAILED,
                error=f"add rejected: {result.detail}",
            )
            return

        # Derive infohash: prefer add_torrent result, then blob hash, fallback to polling
        new_hash = result.hash.lower() if result.hash else None
        if not new_hash and blob:
            try:
                from .watchdir import _bencoded_info_hash

                parsed_hash, _, _, _ = _bencoded_info_hash(blob)
                new_hash = parsed_hash.lower()
            except Exception:
                new_hash = None

        if not new_hash:
            new_hash = await self._await_hash_for_name(
                ts.source_name, size_bytes=int(ts.total_bytes or 0)
            )

        if new_hash:
            ts.dest_infohash = new_hash.lower()

        # Everything below runs against a just-added entry: a loaded client
        # (10k torrents) can 404/time out individual calls while it registers
        # the torrent. Any unexpected failure here parks the row back in
        # QUEUED (dest_infohash persisted) instead of FAILED — the next tick
        # re-enters through the existing-torrent check above and continues.
        # Deliberate terminal transitions (WAITING_DISK park, oversize-movie
        # FAILED) return normally inside the block and are unaffected.
        try:
            await self._setup_queued_download(ts, blob, _holds_download_slot=_holds_download_slot)
        except ValueError as e:
            # Deterministic poison (bad batching/classify, no valid files
            # for priorities): retrying the same bytes parks QUEUED forever.
            # Fail loudly so the operator sees it instead.
            # (BatchMoveIncompleteError stays on the retry path below —
            # its contract is "same batch retries".)
            log.error("queued setup poisoned for %s: %s; failing", ts.source_infohash[:10], e)
            try:
                self.transition(ts, State.FAILED, error=str(e)[:500])
            except (AbandonedError, ValueError):
                try:
                    ts.state = State.FAILED
                    ts.last_error = str(e)[:500]
                    self.store.upsert(ts)
                except Exception:
                    pass
            return
        except Exception as e:  # noqa: BLE001
            log.warning(
                "queued setup hit transient dest error for %s: %s; staying queued",
                ts.source_infohash[:10],
                e,
            )
            try:
                self.store.upsert(ts)
            except Exception:
                pass
            return

    async def _remaining_batch_footprint(self, batches, kind: str) -> int | None:
        """Max SSD bytes still needed across `batches` (remote-skipped excluded).

        Only bytes NOT already on the fuse remote can occupy SSD: batches a
        previous run moved were wiped locally, and never-selected files never
        hit disk on boxes without qB preallocation. One `_fuse_skipped` round
        covers every batch at once. Returns None when unknowable (caller
        keeps today's full-batch figure); 0 when everything is already
        remote (the row will fast-path through downloading holding nothing).
        The physical free-space check in `_ssd_try_reserve` stays the
        backstop, so a preallocating box can at worst admit slightly early,
        never overfill.
        """
        try:
            blist = list(batches or [])
            if not blist:
                return None
            all_eps = [
                (e.file_name, e.size_bytes)
                for b in blist
                for e in (getattr(b, "episodes", None) or [])
                if getattr(e, "file_name", "")
            ]
            if not all_eps:
                return 0
            skipped = await self._fuse_skipped(all_eps, kind or "unknown")
            skipped = skipped or set()
            best = 0
            for b in blist:
                rem = sum(
                    int(getattr(e, "size_bytes", 0) or 0)
                    for e in (getattr(b, "episodes", None) or [])
                    if getattr(e, "file_name", "") not in skipped
                )
                best = max(best, rem)
            return best
        except Exception:
            return None

    async def _dest_entries_for(self, hashes: list[str] | None) -> list:
        """Dest-client entries for any of `hashes` (complete or not), else [].

        Raw material for the SSD shortcuts: callers decide what a hit
        means (adopt a complete one, quiet-wait on a downloading one).
        Never raises — [] means "no usable information".
        """
        try:
            want = {(h or "").strip().lower() for h in (hashes or []) if (h or "").strip()}
            if not want:
                return []
            dest = getattr(self, "dest_client", None)
            if dest is None:
                return []
            found = await rpc(
                dest.list_torrents(hashes=sorted(want)),
                getattr(self, "cfg", None),
                "dest entries lookup",
            )
            out = []
            for t in found or []:
                try:
                    if (getattr(t, "hash", "") or "").lower() in want:
                        out.append(t)
                except Exception:
                    continue
            return out
        except Exception:
            return []

    async def _dest_complete_entry(self, hashes: list[str] | None):
        """Existing complete dest-client entry for one of `hashes`, else None.

        Admit-time shortcut (mirrors `_blob_fully_remote`): content
        already complete on the VPS2 client needs no SSD budget even
        when its rclone move is still ahead — `_do_queued` adopts and
        resumes it instead of adding a second entry. Three-valued like
        its sibling: an entry object = skip the budget; None = reserve
        normally (no client, RPC error, or only partials present).
        Fail-open by construction: a wrong None just parks in
        WAITING_DISK as today; callers re-verify downstream.
        """
        try:
            for t in await self._dest_entries_for(hashes):
                try:
                    if bool(t.is_complete()):
                        return t
                except Exception:
                    continue
            return None
        except Exception:
            return None

    def _cached_blob_files(self, blob: bytes | None, key: str):
        """extract_torrent_files_from_bencoded memoized per infohash.

        One admit parses the same bytes up to 3× (remote check, batch
        footprint, setup classify). Same infohash = same bytes by
        construction, so the key is safe. Per-instance: tests reuse
        fake hashes with different bytes across cases. Entries expire
        after _BLOB_PARSE_TTL_S (memory bound, not correctness).
        """
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return None
            from .watchdir import extract_torrent_files_from_bencoded

            _k = (key or "").lower()
            if not _k:
                # Unkeyed callers (tests, one-shots): parse, don't cache.
                try:
                    return extract_torrent_files_from_bencoded(bytes(blob))
                except Exception:
                    return None
            _now = time.monotonic()
            _cache = getattr(self, "_blob_parse_cache", None)
            if not isinstance(_cache, dict):
                _cache = {}
                self._blob_parse_cache = _cache  # type: ignore[attr-defined]
            _hit = _cache.get(_k)
            if (
                isinstance(_hit, tuple)
                and len(_hit) == 2
                and _now - float(_hit[0]) < _BLOB_PARSE_TTL_S
            ):
                return _hit[1]
            try:
                parsed = extract_torrent_files_from_bencoded(bytes(blob))
            except Exception:
                parsed = None
            try:
                _cache[_k] = (_now, parsed)
                if len(_cache) > 500:
                    for _old in list(_cache.keys())[: len(_cache) - 500]:
                        _cache.pop(_old, None)
            except Exception:
                pass
            return parsed
        except Exception:
            return None

    async def _blob_fully_remote(self, blob: bytes | None, *, cache_key: str = "") -> bool | None:
        """True iff every file in the .torrent bytes already sits verified on fuse.

        Admit-time shortcut so already-remote content never queues behind
        SSD budget it will never use (a fully-remote season otherwise parks
        in WAITING_DISK for hours, then stalls past QUEUED). Three-valued:
        None = unknowable (no blob, undecodable, empty, stat failure) and
        the caller keeps today's flow; False = real SSD work remains. Only
        size-verified files count, and both downstream gates (QUEUED setup
        shortcut, RE_ADDING fuse gate) re-verify, so a wrong True degrades
        to a parked re-add, never a blind seed.
        """
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return None
            parsed = self._cached_blob_files(blob, cache_key)
            if parsed is None:
                return None
            names = [(f.name, f.size_bytes) for f in (parsed or []) if getattr(f, "name", "")]
            if not names:
                return None
            try:
                # Patched in tests as racing_sync.coordinator.classify.
                from . import coordinator as _coordinator

                kind = _coordinator.classify(parsed, self.cfg).kind
            except Exception:
                kind = "unknown"
            skipped = await self._fuse_skipped(names, kind or "unknown")
            if not skipped:
                return False
            return all(n in skipped for n, _ in names)
        except Exception:
            return None

    async def _fuse_skipped(self, items: list[tuple[str, int]], kind: str) -> set[str]:
        """Subset of torrent-relative names already complete on the fuse target.

        Lets reprocessing skip re-downloading batches a previous run already
        moved: only size-verified files are skipped, stats run off the event
        loop, and anything unstatable is downloaded (safe direction — the
        final fuse gate re-verifies everything before injection).
        Returns the input names (exact strings) to skip.
        Delegates to fuse_gate.skipped_present (single implementation).
        """
        if not items:
            return set()
        try:
            mount = self._target_mount_for_kind(kind, Path(self.rt.storage.save_path))
        except Exception:
            return set()
        try:
            from .fuse_gate import skipped_present as _skipped_present

            return await _skipped_present(mount, items)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse skip check failed, downloading everything: %s", e)
            return set()

    async def _setup_queued_download(
        self, ts: TorrentState, blob: bytes, *, _holds_download_slot: bool = True
    ) -> None:
        """Classify, prioritize, and resume a just-added SSD torrent."""
        # Classify (with a short retry: the files endpoint can 404 for a few
        # seconds right after add while qB registers the torrent). Central
        # read-retry policy (1/2/5s, 404-tolerant) replaces the hand-rolled
        # 4x loop: same tolerance, one policy, deterministic errors fast.
        from .retry import retryable

        async def _fetch_setup_files() -> list[TorrentFile]:
            return await rpc(
                self.dest_client.get_torrent_files(ts.dest_infohash or ts.source_infohash),
                getattr(self, "cfg", None),
                "dest setup files",
            )

        files: list[TorrentFile] = await retryable(label="dest setup files")(_fetch_setup_files)()
        _setup_h = (ts.dest_infohash or ts.source_infohash or "").lower()
        try:
            _empty_seen = getattr(self, "_setup_files_empty", None)
            if not isinstance(_empty_seen, dict):
                _empty_seen = {}
                self._setup_files_empty = _empty_seen  # type: ignore[attr-defined]
        except Exception:
            _empty_seen = {}
        if not files:
            # An empty list is a valid retryable answer (metadata still
            # parsing right after add), so it gets here instead of raising.
            # Falling through would run the single-flow path: reserve the
            # whole torrent and resume it with no priorities, so a season
            # whose list lands mid-download floods the SSD. Park and retry;
            # a list that never fills is a broken torrent, not a slow one.
            try:
                _n = int(_empty_seen.get(_setup_h, 0) or 0) + 1
                _empty_seen[_setup_h] = _n
                if len(_empty_seen) > 5000:
                    for _k in list(_empty_seen.keys())[: len(_empty_seen) - 5000]:
                        _empty_seen.pop(_k, None)
            except Exception:
                _n = 1
            if _setup_h:
                try:
                    await self.dest_client.delete(_setup_h, delete_files=False)
                except Exception:
                    pass
            if _n < _SETUP_EMPTY_FILES_FAIL_TRIES:
                log.info(
                    "file list empty for %s after add (%d/%d); parking to retry",
                    ts.source_name,
                    _n,
                    _SETUP_EMPTY_FILES_FAIL_TRIES,
                )
                self.transition(
                    ts,
                    State.WAITING_DISK,
                    error=f"file list empty after add ({_n}/{_SETUP_EMPTY_FILES_FAIL_TRIES})",
                )
                return
            log.warning(
                "file list still empty for %s after %d attempts; failing",
                ts.source_name,
                _n,
            )
            self.transition(
                ts,
                State.FAILED,
                error=f"file list still empty after {_n} attempts; is the torrent broken?",
            )
            return
        try:
            _empty_seen.pop(_setup_h, None)
        except Exception:
            pass
        from . import coordinator as _coordinator

        cls = _coordinator.classify(files, self.cfg)
        ts.classification_kind = cls.kind

        if len(files) == 1:
            # A previous run may already have moved this exact file: no SSD
            # need, no oversize failure — straight to MOVING (its branch
            # tolerates already-remote singles) for the fuse-gated finish.
            only = files[0]
            if await self._fuse_skipped([(only.name, only.size_bytes)], cls.kind):
                log.info("single file %s already on remote; skipping SSD download", only.name)
                # No SSD footprint — free the admission estimate for waiters.
                try:
                    await self._ssd_adjust(ts.source_infohash, 0)
                except Exception:
                    pass
                self.transition(ts, State.MOVING)
                return

        # Feasibility is per individual file: anything may flow (batched as
        # needed) unless one file alone exceeds the SSD cap. Total size never
        # disqualifies content anymore.
        too_big = oversize_single_file(files, self.cfg)
        if too_big is not None:
            # It can never fit on the SSD, so a hand-copied remote version is
            # the only path: wait for it instead of failing at once. The
            # entry is dropped either way (it could never download).
            _h = ts.dest_infohash or ts.source_infohash
            if self._park_fuse_content_wait(ts, present=0, total=len(files or [])):
                log.warning(
                    "single file %s exceeds the SSD cap; dropping the entry and "
                    "waiting for the remote copy before failing %s",
                    too_big,
                    ts.source_name,
                )
                if _h:
                    await self._drop_failed_entry(_h, ts, files, "oversize")
                try:
                    # QUEUED -> WAITING_INDEXER is not in transition()'s
                    # release set; the row no longer occupies SSD.
                    await self._ssd_release(ts.source_infohash)
                except Exception:
                    pass
                return
            log.warning(
                "single file %s exceeds skip threshold; failing %s", too_big, ts.source_name
            )
            if _h:
                await self._drop_failed_entry(_h, ts, files, "oversize")
            self.transition(
                ts,
                State.FAILED,
                error=f"single file larger than skip threshold: {too_big}",
            )
            return

        # Batch-cap feasibility: a single file bigger than the batch cap
        # can never download — admission reserves at most the cap, and
        # the post-classify refine to the real footprint would fail every
        # cycle, looping add/delete + WAITING_DISK forever without ever
        # failing (e.g. skip threshold set above max_inflight_bytes).
        # Fail loudly here instead, with the same terminal handling.
        try:
            _fit_cap = self._frozen_batch_cap(ts)
        except Exception:
            _fit_cap = 0
        if _fit_cap > 0:
            _too_big_for_cap: str | None = None
            try:
                for _f in files or []:
                    try:
                        _nm = getattr(_f, "name", "") or ""
                        _sz = int(getattr(_f, "size_bytes", 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    if _nm and _sz > _fit_cap:
                        _too_big_for_cap = _nm
                        break
            except Exception:
                _too_big_for_cap = None
            if _too_big_for_cap is not None:
                log.warning(
                    "single file %s exceeds batch cap (%d B); failing %s",
                    _too_big_for_cap,
                    _fit_cap,
                    ts.source_name,
                )
                # Remove the paused torrent from VPS2 so it does not leak as an orphan
                h = ts.dest_infohash or ts.source_infohash
                if h:
                    await self._drop_failed_entry(h, ts, files, "over-cap")
                self.transition(
                    ts,
                    State.FAILED,
                    error=f"single file larger than batch cap ({_fit_cap} B): {_too_big_for_cap}",
                )
                return

        # Apply batch file priorities for seasons
        first_need: set[str] | None = None
        _real_footprint: int | None = None
        if cls.kind in ("season", "mixed") and cls.episodes:
            episodes = [e for e in cls.episodes]
            cap = self._frozen_batch_cap(ts)
            if cap <= 0:
                # No usable batch cap: re-check SSD room (disk may have
                # filled since scheduling). Park if full, else one batch.
                total_ep = sum(e.size_bytes for e in episodes)
                # Patched in tests as racing_sync.coordinator.ssd_has_room.
                from . import coordinator as _coordinator

                if not _coordinator.ssd_has_room(
                    self.cfg, min(total_ep, ts.total_bytes or total_ep)
                ):
                    log.info("ssd cap in use; parking %s", ts.source_name)
                    # Keep the paused entry for the retry (existing-entry
                    # adopt path) — see the grow-fail park below.
                    try:
                        _h = ts.dest_infohash or ts.source_infohash
                        if _h:
                            await self.dest_client.delete(_h, delete_files=False)
                    except Exception:
                        pass
                    self.transition(ts, State.WAITING_DISK)
                    return
                cap = total_ep or ts.total_bytes or 1
            batches = make_batches(episodes, cap_bytes=cap)
            ts.batches_total = len(batches)
            ts.batch_index = 0
            # Refine global reservation to the real footprint (max REMAINING
            # batch — covers varying episode sizes, in-place batches hold one
            # at a time; bytes already on the remote hold no SSD).
            _rem = await self._remaining_batch_footprint(batches, cls.kind)
            if _rem is None:
                try:
                    _real_footprint = max((b.size_bytes for b in batches), default=0) or cap
                except Exception:
                    _real_footprint = cap
            else:
                _real_footprint = _rem
            if batches:
                # First batch only: priority 1; rest: 0. Files a previous run
                # already moved stay deselected (re-downloaded never).
                first = batches[0]
                skip0 = await self._fuse_skipped(
                    [(e.file_name, e.size_bytes) for e in first.episodes],
                    cls.kind,
                )
                prio_map = {f.name: 0 for f in files}
                first_need = {e.file_name for e in first.episodes} - skip0
                for name in first_need:
                    prio_map[name] = 1
                await self.dest_client.set_file_priorities(
                    ts.dest_infohash or ts.source_infohash,
                    prio_map,
                )
                await self._warn_if_preallocated(ts, files, first_need)
        elif cls.kind in ("movie", "unknown") and len(files) > 1:
            # Type-agnostic file-group batching for multi-file content that
            # is not episodic (games, disc images, complete packs with plain
            # numbering): stream name-sorted groups through the SSD instead
            # of needing the whole torrent on disk at once. Single episodes
            # stay on the full-torrent path even when extras are present.
            cap = self._frozen_batch_cap(ts)
            if cap <= 0:
                total_files = sum(f.size_bytes for f in files)
                from . import coordinator as _coordinator

                if not _coordinator.ssd_has_room(
                    self.cfg, min(total_files, ts.total_bytes or total_files)
                ):
                    log.info("ssd cap in use; parking %s", ts.source_name)
                    # Keep the paused entry for the retry (existing-entry
                    # adopt path) — see the grow-fail park below. A
                    # metadata-driven wipe could take shared-save_path
                    # sibling data.
                    try:
                        _h = ts.dest_infohash or ts.source_infohash
                        if _h:
                            await self.dest_client.delete(_h, delete_files=False)
                    except Exception:
                        pass
                    self.transition(ts, State.WAITING_DISK)
                    return
                cap = total_files or ts.total_bytes or 1
            batches = make_file_batches(files, cap_bytes=cap)
            ts.batches_total = len(batches)
            ts.batch_index = 0
            _rem = await self._remaining_batch_footprint(batches, cls.kind)
            if _rem is None:
                try:
                    _real_footprint = max((b.size_bytes for b in batches), default=0) or cap
                except Exception:
                    _real_footprint = cap
            else:
                _real_footprint = _rem
            if batches:
                first = batches[0]
                wanted = {ep.file_name for ep in first.episodes}
                skip0 = await self._fuse_skipped(
                    [(e.file_name, e.size_bytes) for e in first.episodes],
                    cls.kind,
                )
                first_need = wanted - skip0
                prio_map = {f.name: (1 if f.name in first_need else 0) for f in files}
                await self.dest_client.set_file_priorities(
                    ts.dest_infohash or ts.source_infohash,
                    prio_map,
                )
                await self._warn_if_preallocated(ts, files, first_need)
        elif cls.kind == "episode" and len(files) > 1:
            cap = self._frozen_batch_cap(ts)
            if cap > 0 and sum(f.size_bytes for f in files) > cap:
                # A single episode whose extras (sample/subs/nfo) push the total
                # past the batch cap can never fit the full-torrent footprint:
                # admission reserves at most the cap, and the refine-to-real grow
                # would fail every cycle (WAITING_DISK loop). Batch the file
                # groups instead; the episode lands in its own group and the
                # extras stream through after it.
                batches = make_file_batches(files, cap_bytes=cap)
                ts.batches_total = len(batches)
                ts.batch_index = 0
                _rem = await self._remaining_batch_footprint(batches, cls.kind)
                if _rem is None:
                    try:
                        _real_footprint = max((b.size_bytes for b in batches), default=0) or cap
                    except Exception:
                        _real_footprint = cap
                else:
                    _real_footprint = _rem
                if batches:
                    first = batches[0]
                    wanted = {ep.file_name for ep in first.episodes}
                    skip0 = await self._fuse_skipped(
                        [(e.file_name, e.size_bytes) for e in first.episodes],
                        cls.kind,
                    )
                    first_need = wanted - skip0
                    prio_map = {f.name: (1 if f.name in first_need else 0) for f in files}
                    await self.dest_client.set_file_priorities(
                        ts.dest_infohash or ts.source_infohash,
                        prio_map,
                    )
                    await self._warn_if_preallocated(ts, files, first_need)

        # All-remote fast path: every batch already sits verified on the
        # remote (remaining footprint zero) — nothing will ever hit SSD, so
        # skip the download + MOVING and go fuse-gated RE_ADDING, which
        # verifies presence before injecting. Without this, a single-flow
        # row waits on torrent-level progress for a paused, fully-deselected
        # entry that can never complete (stuck DOWNLOADING), or wipes its
        # way into a FileNotFoundError. QUEUED -> RE_ADDING is legal and
        # frees the admission slot + SSD reservation via transition().
        if ts.batches_total > 0 and _real_footprint == 0:
            log.info(
                "all %d batche(s) for %s already on remote; skipping SSD download",
                ts.batches_total,
                ts.source_name,
            )
            # Safety net for the admit-race (bytes landed on fuse between
            # the _do_queued shortcut check and this classification): the
            # SSD entry was just added with all files deselected (0-file
            # corpse). Drop the entry WITHOUT files: it never downloaded,
            # so any bytes at those paths belong to someone else (shared
            # save_path) — a metadata-driven wipe could take sibling data.
            try:
                _h = ts.dest_infohash or ts.source_infohash
                if _h:
                    await self.dest_client.delete(_h, delete_files=False)
            except Exception as e:  # noqa: BLE001
                log.debug(
                    "could not delete fully-remote SSD corpse %s: %s",
                    (ts.dest_infohash or ts.source_infohash or "")[:10],
                    e,
                )
            self.transition(ts, State.RE_ADDING)
            return

        # Refine the admission estimate to the real SSD footprint now that
        # classification/batches are known (remaining batch bytes for
        # seasons/games; singles/full-torrent → total). Zero is a valid
        # refinement (everything already remote — hold nothing). Shrinking
        # always succeeds and frees budget for waiters; growing (single
        # bigger than estimate) can fail globally — roll back the just-added
        # torrent and park.
        try:
            if _real_footprint is None:
                try:
                    _real_footprint = sum(f.size_bytes for f in files) or ts.total_bytes or 0
                except Exception:
                    _real_footprint = ts.total_bytes or 0
            _ok = await self._ssd_adjust(ts.source_infohash, int(_real_footprint or 0))
        except Exception:
            _ok = True
        if not _ok:
            log.info(
                "ssd budget in use after classify (need ~%d MB, reserved ~%d MB); parking %s",
                int(_real_footprint or 0) // (1024 * 1024),
                self._ssd_reserved_total() // (1024 * 1024),
                ts.source_name,
            )
            # Keep the paused entry (delete_files=False): the retry adopts
            # it via the existing-entry path instead of re-adding, and a
            # metadata-driven wipe could take shared-dir sibling data.
            try:
                _h = ts.dest_infohash or ts.source_infohash
                if _h:
                    await self.dest_client.delete(_h, delete_files=False)
            except Exception:
                pass
            self.transition(ts, State.WAITING_DISK)
            return

        # Resume (skipped when the first batch needs nothing locally yet:
        # a client with zero selected files may refuse; the download loop
        # resumes on reaching the first batch with work). Gate first: setup
        # (add + prioritize) already ran, so park with the paused entry
        # intact — the next tick resumes through the existing-entry path.
        if await self._park_queued_for_download_slot(ts, holds_slot=_holds_download_slot):
            return
        if first_need is None or first_need:
            await self.dest_client.resume(ts.dest_infohash or ts.source_infohash)
        self.transition(ts, State.DOWNLOADING)

    async def _await_hash_for_name(
        self, name: str, *, size_bytes: int | None = None, timeout_s: float = 60
    ) -> str | None:
        deadline = time.monotonic() + timeout_s
        _gap = 2.0
        try:
            _want_size = int(size_bytes or 0)
        except (TypeError, ValueError):
            _want_size = 0
        # Normalized fallback: the client may report a differently-cased
        # or punctuated name for the just-added torrent — exact match
        # first, normalized second, per poll.
        try:
            _want_norm = normalize_content_name(name or "")
        except Exception:
            _want_norm = ""
        while time.monotonic() < deadline:
            if getattr(self, "_stop", False):
                return None
            # Dest entries are always added with category="racing": filter
            # server-side instead of pulling 10k long-term seeds per poll.
            # Bounded + backoff: a wedged WebUI must degrade this lookup
            # instead of wedging the worker on a raw 60s poll loop.
            try:
                rows = await rpc(
                    self.dest_client.list_torrents(category="racing"),
                    getattr(self, "cfg", None),
                    "dest await-hash list",
                )
            except (asyncio.CancelledError, AbandonedError):
                raise
            except Exception as e:  # noqa: BLE001
                log.debug("await-hash poll failed for %s: %s", name[:40], e)
                rows = []
            for t in rows or []:
                try:
                    if t.name != name:
                        continue
                    try:
                        _got = int(getattr(t, "size_bytes", 0) or 0)
                    except (TypeError, ValueError):
                        _got = 0
                    if _want_size and _got and _got != _want_size:
                        continue
                    return t.hash.lower()
                except Exception:
                    continue
            if _want_norm:
                for t in rows or []:
                    try:
                        if normalize_content_name(getattr(t, "name", "") or "") != _want_norm:
                            continue
                        # The name alone is not identity: another release can
                        # share it, and this hash is later used to delete
                        # files on failure. Require the size we added to
                        # agree whenever both sides know it.
                        try:
                            _got = int(getattr(t, "size_bytes", 0) or 0)
                        except (TypeError, ValueError):
                            _got = 0
                        if _want_size and _got and _got != _want_size:
                            continue
                        return t.hash.lower()
                    except Exception:
                        continue
            await asyncio.sleep(_gap)
            _gap = min(10.0, _gap * 1.5)
        return None

    # ---- state: DOWNLOADING ----

    def _resolve_batches(
        self, files: list[TorrentFile], kind: str, cap_bytes: int, episodes=None
    ) -> list[Batch]:
        """Episode batches for seasons, file-group batches otherwise.

        Single shared rule used at QUEUED time and re-resolved during the
        download loop, so batch counts stay consistent: seasons split by
        episode, movie/unknown multi-file content splits name-sorted file
        groups. Single files, single episodes and empty lists yield no
        batches (full-torrent flow).

        `episodes` (when the caller already classified) skips the internal
        re-classify: a mid-flight sidecar must not re-derive a different
        episode list — or flip the kind — under a pinned classification.
        """
        if kind in ("season", "mixed"):
            try:
                if episodes is None:
                    # Same patch-target treatment as above (classify).
                    from . import coordinator as _coordinator

                    episodes = _coordinator.classify(files, self.cfg).episodes
                eps = list(episodes or [])
            except Exception:
                return []
            if not eps:
                return []
            return make_batches(eps, cap_bytes=cap_bytes)
        if kind in ("movie", "unknown") and len(files) > 1:
            return make_file_batches(files, cap_bytes=cap_bytes)
        return []

    async def _get_batches_for_torrent(self, ts: TorrentState) -> list[Batch] | None:
        """Batch list, [] when genuinely single-flow, None on transient RPC failure.

        Callers must not confuse the two: None means "unknown, retry next
        tick", never "full-torrent flow".
        """
        h = ts.dest_infohash or ts.source_infohash
        try:
            files = await rpc(
                self.dest_client.get_torrent_files(h),
                getattr(self, "cfg", None),
                "dest batches files",
            )
        except Exception as e:
            log.warning("could not get torrent files for batches: %s", e)
            return None
        # Same rule as _do_queued so batch counts stay consistent across
        # the download loop. Pinned kind wins; a fresh classify only fills
        # in unknown — and its episodes are reused so one parse serves both.
        kind = ts.classification_kind
        _eps = None
        if not kind or kind == "unknown":
            try:
                from . import coordinator as _coordinator

                _cls = _coordinator.classify(files, self.cfg)
                kind = _cls.kind
                _eps = _cls.episodes
            except Exception as e:
                log.warning("could not classify files for batches: %s", e)
                return None
        cap = self._frozen_batch_cap(ts)
        if cap <= 0:
            cap = sum(f.size_bytes for f in files) or 1
        try:
            return self._resolve_batches(files, kind, cap, episodes=_eps)
        except Exception as e:
            log.warning("could not make batches for %s: %s", ts.source_name, e)
            return None

    async def _move_and_clean_batch(
        self, ts: TorrentState, batch: Batch, skip: set[str] | None = None
    ) -> None:
        if batch is None:
            return
        save_path = ts.save_path or str(self.rt.storage.save_path)
        src_dir = Path(save_path).resolve()
        remote = (
            self.rt.transfer.remote_default
            if route_kind_is_sorted(ts.classification_kind)
            else self.rt.transfer.remote_unsorted
        )
        log.info(
            "moving completed batch %d/%d for %s",
            ts.batch_index + 1,
            ts.batches_total,
            ts.source_name,
        )
        extra_flags = self.rt.transfer.batch_extra_flags
        skip_norm = {(n or "").replace("\\", "/").strip("/") for n in (skip or set())}
        names = [n for n in batch.file_names() if n not in skip_norm]
        unsafe = [n for n in names if _safe_ssd_join(src_dir, n or "") is None]
        if unsafe:
            # Every other move path traversal-guards its file list; a
            # hostile `../` name here would upload and delete outside the
            # SSD tree. Park loud (never advance, never move) for the
            # operator instead of failing the row over client metadata.
            raise BatchMoveIncompleteError(
                f"refusing to move {len(unsafe)} path-traversal file(s) for "
                f"{ts.source_name} (e.g. {unsafe[0]!r}); keeping batch"
            )
        if not names:
            log.info(
                "batch %d/%d for %s already on remote; skipping move",
                ts.batch_index + 1,
                ts.batches_total,
                ts.source_name,
            )
            return
        await self._rclone_move(
            src_dir,
            remote,
            ts,
            files_from=names,
            extra=extra_flags,
        )
        # rclone exits 0 even when it transferred nothing: batch files moved
        # by rclone are already gone, so anything still present never reached
        # the remote. Raising (instead of deleting) keeps the batch retryable
        # and the data intact.
        stragglers = [n for n in names if _left_on_disk(src_dir, n)]
        if stragglers:
            raise BatchMoveIncompleteError(
                f"rclone move reported ok but {len(stragglers)} batch file(s) "
                f"never reached {remote} (e.g. {stragglers[0]}); not advancing batch"
            )

    @staticmethod
    def _client_reports_paused(state_str: object) -> bool:
        """True when a client torrent state reads as paused/stopped.

        Covers qBittorrent v5 (`stoppedDL/stoppedUP`) and legacy (`pausedDL/
        pausedUP`) spellings. A vanished entry (None) or non-string state
        (test doubles) counts as paused — verification best-effort only.
        """
        if state_str is None:
            return True
        if not isinstance(state_str, str):
            return True
        s = state_str.lower()
        return "paus" in s or "stop" in s

    async def _pause_verified(self, h: str) -> tuple[bool, Exception | None]:
        """Pause and verify the client actually stopped IO (3 attempts).

        Re-reads the entry after each pause RPC: without verification a
        resume in the gap (auto-manage, operator) leaves qB writing while
        rclone moves. A failed verification read fails CLOSED (park and
        retry next tick) — the pause may not have landed, and moving
        under a writing client corrupts the remote. A hash-checking entry
        short-circuits immediately (no pause storm against an in-progress
        check): callers wait it out via TorrentCheckingError instead of
        fighting it.
        """
        err: Exception | None = None
        for attempt in range(3):
            try:
                await rpc(self.dest_client.pause(h), getattr(self, "cfg", None), "dest pause")
                try:
                    cur = await rpc(
                        self.dest_client.get_torrent(h),
                        getattr(self, "cfg", None),
                        "dest get_torrent",
                    )
                    state = getattr(cur, "state", "") if cur is not None else ""
                    if cur is None or self._client_reports_paused(state):
                        return True, None
                    try:
                        _slow = str(state or "").lower().startswith("check")
                    except Exception:
                        _slow = False
                    if _slow:
                        # Hash check in flight (e.g. our settle recheck on
                        # slow storage): pausing further can't help and the
                        # move must wait for it — signal, don't retry.
                        # Check-fraction rides along for the watchdog.
                        try:
                            _prog = float(getattr(cur, "progress", 0.0) or 0.0)
                        except (TypeError, ValueError):
                            _prog = 0.0
                        return False, TorrentCheckingError(
                            f"client is hash-checking ({state}); waiting it out",
                            client_state=str(state),
                            progress=_prog,
                        )
                    err = RuntimeError(f"client still reports state={state!r} after pause")
                except Exception as e:
                    # Verification read failed: the pause may not have
                    # landed — fail closed (retry the pause, then park)
                    # instead of moving under a writing client.
                    err = RuntimeError(f"could not verify pause for {h[:10]}: {e}")
            except Exception as e:  # noqa: BLE001
                err = e
            if attempt < 2:
                await asyncio.sleep(1)
        return False, err

    async def _batch_bytes_on_disk(self, ts: TorrentState, batch_files: list) -> bool:
        """True iff every expected batch file is present on SSD at full size.

        Offloaded to a thread (fuse-adjacent stat can block). A truncated or
        preallocated-but-incomplete file reads as missing — the caller keeps
        polling so the client re-fetches instead of moving air. Zero-size
        files skip the disk gate: they carry no bytes, so client-complete
        is completion for them (a not-yet-created empty file must not poll
        forever here — the move path verifies presence before moving).
        """
        try:
            base = Path(ts.save_path) if ts.save_path else Path(self.rt.storage.save_path)
        except Exception:
            return False

        def _check() -> bool:
            for f in batch_files:
                try:
                    name = getattr(f, "name", "") or ""
                    want = getattr(f, "size_bytes", 0) or 0
                    if not want:
                        continue
                    p = _safe_ssd_join(base, name)
                    if p is None or not p.is_file():
                        return False
                    if p.stat().st_size < want:
                        return False
                except OSError:
                    return False
            return True

        try:
            return await asyncio.to_thread(_check)
        except Exception:
            return False

    async def _warn_if_preallocated(
        self,
        ts: TorrentState,
        files: list,
        wanted: set[str] | None,
    ) -> None:
        """Loud one-time hint when deselected files already occupy full size.

        qB creates every file at add time even while paused; with
        pre-allocation enabled the deselected episodes sit at full size and a
        season bigger than the SSD fills the disk no matter how correct the
        batch priorities are (deselect-after-add never reclaims the bytes).
        Nothing here can free them (no client API removes the bytes short of
        a full delete+re-add, which would pre-allocate again) — the fix is
        sparse allocation on the VPS2 client. Probe once per row; never
        raises so setup can never fail over a hint.
        """
        try:
            if wanted is None:
                return
            key = (ts.source_infohash or "").lower()
            if not key:
                return
            try:
                _warned = getattr(self, "_prealloc_warned", None)
                if not isinstance(_warned, set):
                    _warned = set()
                    self._prealloc_warned = _warned  # type: ignore[attr-defined]
                if key in _warned:
                    return
            except Exception:
                _warned = set()
            try:
                base = Path(ts.save_path) if ts.save_path else Path(self.rt.storage.save_path)
            except Exception:
                return
            others = [
                f for f in (files or []) if (getattr(f, "name", "") or "") not in (wanted or set())
            ]

            def _check() -> tuple[int, int]:
                n = 0
                total = 0
                for f in others:
                    try:
                        name = getattr(f, "name", "") or ""
                        want = int(getattr(f, "size_bytes", 0) or 0)
                        if not name or want <= 0:
                            continue
                        try:
                            prog = float(getattr(f, "progress", 0.0) or 0.0)
                        except (TypeError, ValueError):
                            prog = 0.0
                        if prog >= 0.01:
                            continue
                        p = _safe_ssd_join(base, name)
                        if p is None:
                            continue
                        try:
                            if not p.is_file():
                                continue
                            if p.stat().st_size >= int(want * 0.9):
                                n += 1
                                total += want
                        except OSError:
                            continue
                    except Exception:
                        continue
                return n, total

            try:
                n, total = await asyncio.to_thread(_check)
            except Exception:
                return
            if n <= 0:
                return
            try:
                _warned.add(key)
                if len(_warned) > 500:
                    for k in list(_warned)[:250]:
                        _warned.discard(k)
            except Exception:
                pass
            log.warning(
                "qB pre-allocated %d deselected file(s) (~%d MB) for %s: "
                "deselect-after-add cannot reclaim them — disable pre-allocation "
                "(sparse allocation) on the VPS2 client or seasons bigger than "
                "the SSD will keep filling the disk",
                n,
                total // (1024 * 1024),
                ts.source_name[:60],
            )
        except Exception:
            pass
