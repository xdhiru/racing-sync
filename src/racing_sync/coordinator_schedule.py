"""Coordinator schedule mixin: tick orchestration and worker spawning.

Split out of the coordinator god-file. This half owns "when work runs":
the tick (poll -> schedule -> janitor), the per-step passes, worker
spawning/stopping with in-flight bookkeeping, and the shared in-memory
map sweep that keeps long-running daemons bounded.

The mixin is duck-typed against ``Coordinator`` (the dataclass fields
live there) and must never import ``racing_sync.coordinator`` (circular).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time

from .io_bounds import offload
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


_SCHED_STATE_PRIORITY = {
    # Bounded pipeline work first: timer-expired re-adds, admissions,
    # in-flight downloads/moves and indexer wakeups.
    State.RE_ADDING: 0,
    State.QUEUED: 0,
    State.MOVING: 0,
    State.DOWNLOADING: 0,
    # Unbounded discovery after that: endless NEW rows must not crowd out
    # the rows above.
    State.NEW: 1,
    # Parked rows last (pre-existing rule, kept).
    State.WAITING_DISK: 2,
    State.WAITING_INDEXER: 2,
    State.DONE: 3,
    State.FAILED: 3,
}

# Misc worker budget: NEW intake, WAITING_DISK checks, RE_ADDING and
# DONE/FAILED drains share this (downloads/moves have their own caps).
# Bounds intake bursts without letting long downloads/moves starve them.
_MISC_WORKER_BUDGET = 12

# Worker-stop join budget: cancel() cannot interrupt a to_thread body.
_STOP_WORKERS_TIMEOUT_S = 30.0

# In-memory bookkeeping hygiene (I15): one shared sweep prunes expired TTL
# entries and caps hash-keyed maps. Throttled because the scheduler calls
# it every pass; maps are small in normal operation, so the walk is cheap.
_MAP_SWEEP_THROTTLE_S = 60.0
_MAP_ENTRY_CAP = 5000
#: Late-seed failure deferrals older than this are dead weight.
_LATE_SEED_FAILURE_WINDOW = dt.timedelta(minutes=30)


def _sched_priority(ts) -> tuple[int, dt.datetime]:
    """Worker-scheduling order key (lower first, then oldest first).

    all_active() ordering is DB-dependent; without an explicit priority,
    bulk NEW rows fill every worker slot each tick and starve the bounded
    pipeline states behind them. Compares real timestamps: rows with no
    (or unparsable) updated_at sort LAST, never first — broken rows must
    not permanently prioritize themselves.
    """
    _FAR = dt.datetime.max.replace(tzinfo=dt.UTC)
    try:
        prio = _SCHED_STATE_PRIORITY.get(ts.state, 1)
    except Exception:
        prio = 1
    try:
        updated = getattr(ts, "updated_at", None)
        if updated is None:
            return (prio, _FAR)
        if isinstance(updated, str):
            if not updated:
                return (prio, _FAR)
            updated = dt.datetime.fromisoformat(updated)
        if isinstance(updated, dt.datetime):
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=dt.UTC)
            return (prio, updated)
    except Exception:
        pass
    return (prio, _FAR)


class ScheduleMixin:
    def _spawn_worker(self, ts: TorrentState) -> None:
        """Launch a _process_torrent worker with in-flight bookkeeping.

        No-op if the row already has a live worker (both call sites
        pre-check too; the re-check here closes the gap after awaits).
        A TaskHandle rides alongside the raw task: uniform cancel(),
        plus the event/force slots P3/P4 park on. The raw `_worker_tasks`
        registry stays (callers poke it); handles only add behavior.
        """
        h = (ts.source_infohash or "").lower()
        if h in self._running_infohashes:
            return
        self._running_infohashes.add(h)
        try:
            task = asyncio.create_task(self._process_torrent(ts))
        except RuntimeError:
            # Loop closed/shutting down: release the claim so the
            # scheduler never skips this hash forever.
            self._running_infohashes.discard(h)
            raise
        self._tasks.add(task)
        try:
            _wt = getattr(self, "_worker_tasks", None)
            if not isinstance(_wt, dict):
                _wt = {}
                self._worker_tasks = _wt  # type: ignore[attr-defined]
            _wt.setdefault(h, set()).add(task)
        except Exception:
            pass
        try:
            from .task import TaskHandle, register_handle, short_gid

            try:
                _kind = (
                    str(getattr(getattr(ts, "state", None), "value", "") or "").lower() or "worker"
                )
            except Exception:
                _kind = "worker"
            _handle = TaskHandle(infohash=h, gid=short_gid(h), kind=_kind, task=task)
            try:
                _trees = getattr(self, "_moving_tree", None)
                if isinstance(_trees, dict) and h in _trees:
                    _handle.tree_key = _trees[h]
            except Exception:
                pass
            register_handle(self, _handle)
        except Exception:
            _handle = None  # type: ignore[assignment]

        def _done_cb(t: asyncio.Task, infohash: str = h) -> None:
            self._tasks.discard(t)
            self._running_infohashes.discard(infohash)
            try:
                _wt2 = getattr(self, "_worker_tasks", None)
                if isinstance(_wt2, dict):
                    _s = _wt2.get(infohash)
                    if isinstance(_s, set):
                        _s.discard(t)
                        if not _s:
                            _wt2.pop(infohash, None)
            except Exception:
                pass
            try:
                if _handle is not None:
                    from .task import unregister_handle

                    unregister_handle(self, _handle)
            except Exception:
                pass
            # Retrieve the result: an unretrieved exception (AbandonedError,
            # a real crash) would otherwise surface only as "Task exception
            # was never retrieved" at GC. Never re-raise here.
            if not t.cancelled():
                try:
                    exc = t.exception()
                except BaseException:
                    exc = None
                if exc is not None:
                    log.debug("worker for %s ended with %r", (infohash or "")[:10], exc)

        task.add_done_callback(_done_cb)

    async def stop_workers_for(self, infohash: str) -> bool:
        """Cancel live workers for one hash; True when any were stopped.

        Used by forget-without-remember (never-seen semantics): the row
        is hard-deleted right after, so a stale in-flight upsert must
        not resurrect it as a zombie. Cancellation is clean — the worker
        wrapper unwinds through AbandonedError/quiet paths and its
        finally drops the live map. Best-effort and idempotent; unknown
        hashes (and bare test doubles) report False.

        Cancels via TaskHandle (flag + wake + task-cancel + owned rclone
        abort), falling back to raw task cancels for handles-less
        entries. Bounded by _STOP_WORKERS_TIMEOUT_S: cancel() cannot
        interrupt a worker wedged in a to_thread body (fuse/SFTP
        syscalls), so the join must not hang the forget/API call behind
        a leaked thread.
        """
        try:
            h = (infohash or "").strip().lower()
            if not h:
                return False
            try:
                from .task import handles_for

                _handles = handles_for(self, h)
            except Exception:
                _handles = []
            _live_tasks: list = []
            _stopped = False
            for _hd in _handles:
                try:
                    if _hd.cancel():
                        _stopped = True
                except Exception:
                    pass
                try:
                    _t = getattr(_hd, "task", None)
                    if _t is not None and not _t.done():
                        _live_tasks.append(_t)
                except Exception:
                    pass
            try:
                _wt = getattr(self, "_worker_tasks", None)
                _tasks = list(_wt.get(h, ())) if isinstance(_wt, dict) else []
            except Exception:
                return _stopped
            for t in _tasks:
                try:
                    if t.done():
                        continue
                except Exception:
                    continue
                if t not in _live_tasks:
                    _live_tasks.append(t)
                if t not in [getattr(_x, "task", None) for _x in _handles]:
                    try:
                        t.cancel()
                    except Exception:
                        pass
                    _stopped = True
            if not _live_tasks and not _stopped:
                return False
            try:
                await asyncio.wait_for(
                    asyncio.gather(*_live_tasks, return_exceptions=True),
                    timeout=_STOP_WORKERS_TIMEOUT_S,
                )
            except TimeoutError:
                log.warning(
                    "stop_workers_for %s timed out; %d worker(s) linger", h[:10], len(_live_tasks)
                )
            except Exception:
                pass
            return True if (_stopped or _live_tasks) else False
        except Exception:
            return False

    async def _tick(self) -> None:
        """One iteration: poll sources, schedule work, run the janitor.

        Each step takes its production loop's lock inside `_tick_inner`, so
        this pass serialises against the live loops step by step. (It used
        to hold `_ops_lock` across the whole pass, a lock the scheduler and
        janitor loops never take — so it never actually excluded them.)
        """
        _t0 = time.monotonic()
        await self._tick_inner()
        # Tick health for /readyz + /metrics (strict numerics; a wedged
        # loop shows as ever-growing age while the process stays up).
        try:
            _t1 = time.monotonic()
            self._last_tick_at = _t1
            self._last_tick_duration_s = _t1 - _t0
        except Exception:
            pass

    def _sweep_runtime_maps(self) -> None:
        """Prune expired TTL memos and cap hash-keyed maps. Never raises.

        One sweep for every in-memory bookkeeping map: TTL maps drop
        expired entries even while small (a stale memo is never useful
        again, and a forgotten/cancelled row never gets to pop its own
        key), while hash-keyed maps shed their oldest entries past the cap
        (dict insertion order). Restart-wiped maps simply re-warm.
        """
        try:
            _now_m = time.monotonic()
            _last = float(getattr(self, "_map_sweep_at", 0.0) or 0.0)
            if _now_m - _last < _MAP_SWEEP_THROTTLE_S:
                return
            self._map_sweep_at = _now_m
        except Exception:
            pass

        def _prune_expired(attr: str) -> None:
            try:
                m = getattr(self, attr, None)
                if not isinstance(m, dict) or not m:
                    return
                for _k in list(m.keys()):
                    try:
                        if _now_m >= float(m[_k]):
                            m.pop(_k, None)
                    except Exception:
                        m.pop(_k, None)
            except Exception:
                pass

        for _attr in (
            "_fuse_outage_until",
            "_fuse_healthy_until",
            "_waiting_disk_next_check",
            "_supplement_ok_at",
        ):
            _prune_expired(_attr)

        try:
            _fm = getattr(self, "_failed_late_cross_seeds", None)
            if isinstance(_fm, dict) and _fm:
                _cutoff = dt.datetime.now(dt.UTC) - _LATE_SEED_FAILURE_WINDOW
                for _k in list(_fm.keys()):
                    try:
                        if _fm[_k] < _cutoff:
                            _fm.pop(_k, None)
                    except Exception:
                        _fm.pop(_k, None)
        except Exception:
            pass

        for _attr in (
            "_fuse_outage_until",
            "_fuse_healthy_until",
            "_waiting_disk_next_check",
            "_supplement_ok_at",
            "_failed_late_cross_seeds",
            "_moving_parks",
            "_moving_tree",
            "_moving_progress_bytes",
            "_readd_heal_track",
            "_state_budget_hits",
            "_dl_stall_parks",
            "_dl_file_drift",
            "_grace_exempt",
            "_last_grace_search",
        ):
            try:
                _m = getattr(self, _attr, None)
                if isinstance(_m, dict) and len(_m) > _MAP_ENTRY_CAP:
                    for _k in list(_m.keys())[: len(_m) - _MAP_ENTRY_CAP]:
                        _m.pop(_k, None)
            except Exception:
                continue

    async def _schedule_step(self) -> None:
        """Scheduler pass: indexer wakeups, worker scheduling, live refresh.

        Runs on the scheduler loop under `_schedule_lock`, and inside
        `_tick_inner` for single-pass callers. Snapshots are re-validated
        at worker start (re-get guards), so interleaving with the poller
        loop or API ops cannot double-schedule or clobber rows.
        """
        log.debug("scheduler: enter")
        # One place bounds every in-memory bookkeeping map (I15): the
        # scheduler pass is the natural heartbeat, and the sweep throttles
        # itself so the walk stays cheap.
        try:
            self._sweep_runtime_maps()
        except Exception:
            pass
        # A scheduling pass decides which same-content row owns the bytes.
        # Deciding from a 30s-old snapshot would miss rows created since
        # (and transitions), letting two rows admit together — drop it so
        # the pass elects against the live table.
        try:
            self._inflight_snapshot = None
        except Exception:
            pass
        # 3. Wake up WAITING_INDEXER rows whose retry timer has elapsed —
        # both the download-target indexer schedule and the independent
        # public-export schedule. Indexer wakeups still respect worker
        # capacity: an unbounded timer burst must not spawn unbounded
        # workers.
        try:
            _max_workers = max(
                12,
                self.rt.limits.max_active_downloads * 2 + self.rt.limits.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError, AttributeError):
            # Broken config must fail closed to a bounded burst, never to
            # unbounded worker spawn (0 is falsy = cap disabled below).
            _max_workers = 12
        try:
            ready_waiting = await offload(self.store.list_waiting_ready)
        except Exception:
            try:
                ready_waiting = await offload(self.store.list_indexer_ready)
            except Exception:
                ready_waiting = []
        for ts in list(ready_waiting or []):
            if _max_workers and max(0, _max_workers - len(self._tasks)) <= 0:
                break
            try:
                self._wakeup_indexer_row(ts)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "indexer wakeup for %s failed; continuing: %s",
                    (getattr(ts, "source_infohash", "") or "")[:10],
                    e,
                )
                continue

        # 4. Schedule workers for active states that have no live task.
        # Bounded pipeline work sorts before unbounded discovery: NEW rows
        # are unlimited (bulk watch drops re-evaluate every tick) and would
        # otherwise fill every worker slot each tick, starving timer-expired
        # RE_ADDING rows indefinitely (live incident: 135s-delayed re-adds
        # never re-ran while 40+ NEW rows cycled). WAITING_DISK still sorts
        # last so parked rows never starve real work either.
        try:
            _all_active = await offload(self.store.all_active)
        except Exception:
            _all_active = []
        active = sorted(
            _all_active or [],
            key=_sched_priority,
        )
        scheduled = 0
        scheduled_waiting_disk = 0
        try:
            max_concurrent_workers = max(
                12,
                self.rt.limits.max_active_downloads * 2 + self.rt.limits.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError, AttributeError):
            max_concurrent_workers = 12

        # Event-parked lane waiters (P4): they hold a worker claim (no
        # dupe spawns) but must not count as live lane usage — freed
        # slots wake them via start_from_queued().
        try:
            _reg = getattr(self, "_task_handles", None)
            _parked: set[str] = set()
            if isinstance(_reg, dict):
                for _hk, _hs in list(_reg.items()):
                    try:
                        if isinstance(_hs, set) and any(
                            getattr(_x, "queued", False) for _x in list(_hs)
                        ):
                            _parked.add(_hk)
                    except Exception:
                        continue
        except Exception:
            _parked = set()

        active_downloads = sum(
            1
            for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state in (State.QUEUED, State.DOWNLOADING)
            and (t.source_infohash or "").lower() not in _parked
        )
        active_moves = sum(
            1
            for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state == State.MOVING
            and (t.source_infohash or "").lower() not in _parked
        )
        # Per-phase worker budgets: a handful of multi-hour DOWNLOADING /
        # MOVING workers must not starve every other state via the global
        # slot pool. Downloads/moves gate on their live counts above;
        # intake, checks and re-adds share a bounded misc budget; the
        # global worker count stays only as a backstop.
        live_misc = sum(
            1
            for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state not in (State.QUEUED, State.DOWNLOADING, State.MOVING)
            and (t.source_infohash or "").lower() not in _parked
        )

        for ts in active:
            if len(self._tasks) >= max_concurrent_workers:
                break
            _tkey = (ts.source_infohash or "").lower()
            if _tkey in self._running_infohashes:
                continue

            # Operator hold (/hold_): no workers until resumed — no
            # Prowlarr queries, no downloads, no moves. State is kept.
            if getattr(ts, "skipped", 0):
                continue

            # Operator intake pause (/pause): NEW rows wait until
            # resumed; every other state keeps draining normally.
            if getattr(ts, "state", None) == State.NEW and getattr(self, "_intake_paused", False):
                continue

            # Skip WAITING_INDEXER rows: they are parked and woken up exclusively
            # by Step 3 when their indexer_next_retry_at timer elapses.
            if ts.state == State.WAITING_INDEXER:
                continue

            # Quiet-wait for SSD-full parking: re-check at most once per
            # 60s instead of every tick. Batches drain via MOVING in the
            # meantime; the next check promotes to QUEUED automatically.
            if ts.state == State.WAITING_DISK:
                try:
                    _wd = getattr(self, "_waiting_disk_next_check", None)
                    nxt = _wd.get(_tkey) if isinstance(_wd, dict) else None
                except Exception:
                    nxt = None
                if nxt is not None and time.monotonic() < nxt:
                    continue

            # Skip RE_ADDING rows whose backoff timer has not yet elapsed
            if ts.state == State.RE_ADDING and ts.readd_next_retry_at:
                now_utc = dt.datetime.now(dt.UTC)
                if ts.readd_next_retry_at > now_utc:
                    continue

            # Limit concurrent active qBittorrent additions / downloads on SSD.
            # Both QUEUED and workerless DOWNLOADING rows consume a slot:
            # spawning over cap just parks inside _try_admit after burning
            # a worker plus a dest list_torrents storm. Force-start bypasses
            # (operator override; the worker still honors admission).
            if (
                ts.state in (State.QUEUED, State.DOWNLOADING)
                and active_downloads >= self.rt.limits.max_active_downloads
                and not self._is_forced(_tkey)
            ):
                continue

            # Limit concurrent active rclone move commands
            if (
                ts.state == State.MOVING
                and active_moves >= self.rt.limits.max_concurrent_moves
                and not self._is_forced(_tkey)
            ):
                continue

            # All other states (NEW intake, WAITING_DISK checks, RE_ADDING,
            # DONE/FAILED drains) share the misc budget: unbounded intake
            # bursts must not flood the loop with Prowlarr picks either.
            if (
                ts.state not in (State.QUEUED, State.DOWNLOADING, State.MOVING)
                and live_misc >= _MISC_WORKER_BUDGET
            ):
                continue

            try:
                self._spawn_worker(ts)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("scheduling worker for %s failed; continuing: %s", _tkey[:10], e)
                continue

            if ts.state in (State.QUEUED, State.DOWNLOADING):
                active_downloads += 1
            elif ts.state == State.MOVING:
                active_moves += 1
            else:
                live_misc += 1

            scheduled += 1
            if ts.state == State.WAITING_DISK:
                scheduled_waiting_disk += 1
        # Wake queued lane-waiters that now fit (priority-ordered):
        # freed slots go to parked workers before new spawns churn.
        try:
            await self.start_from_queued()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("start_from_queued failed; continuing: %s", e)
        if scheduled:
            # Quiet-wait: a tick that only re-checked parked WAITING_DISK
            # rows is routine while batches drain — debug, not info, so a
            # full disk doesn't fill the log disk with its own status lines.
            _log = log.debug if scheduled == scheduled_waiting_disk else log.info
            _log(
                "scheduled %d worker(s) (active downloads=%d/%d, moves=%d/%d)",
                scheduled,
                active_downloads,
                self.rt.limits.max_active_downloads,
                active_moves,
                self.rt.limits.max_concurrent_moves,
            )

        # 4. Refresh live status (used by the Telegram bot). Isolated: a
        # failing client must not skip the janitor below.
        try:
            await self._refresh_live_status()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("live status refresh failed; continuing tick: %s", e)

    async def _janitor_step(self) -> None:
        """Janitor pass: VPS1 cleanup + tombstone GC.

        Runs on the janitor loop under `_janitor_lock` (hourly cadence,
        self-gated), and inside `_tick_inner` for single-pass callers.
        Isolating it means a slow/dead fuse or qB can no longer stall
        source discovery, scheduling, or API ops behind one lock.
        """
        log.debug("janitor: enter")
        # 5. VPS1 cleanup janitor (hourly no-op unless [cleanup].enabled).
        # Must never break the tick: all failures are caught and logged.
        try:
            await self._maybe_cleanup_source()
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup janitor failed: %s", e)

        # 5b. Tombstone GC (hourly): hard-delete forgets older than the TTL.
        # Must never break the tick either.
        try:
            _now = time.monotonic()
            _last_gc = getattr(self, "_tombstone_last_gc", 0.0)
            try:
                _last_gc = float(_last_gc or 0.0)
            except (TypeError, ValueError):
                _last_gc = 0.0
            if _now - _last_gc >= 3600:
                self._tombstone_last_gc = _now
                _n = await asyncio.to_thread(self.store.gc_tombstones)
                if _n:
                    log.info("tombstone GC removed %d expired forget row(s)", _n)
        except Exception as e:  # noqa: BLE001
            log.warning("tombstone GC failed: %s", e)

    async def _tick_inner(self) -> None:
        """One full pass: poll sources, schedule work, run janitor.

        Compatibility path for single-pass callers (tests, manual /tick):
        the production `run()` loop instead runs the three steps on
        independent loops with separate locks and clocks (see
        `_poller_loop` / `_scheduler_loop` / `_janitor_loop`).
        Each step is isolated: one step throwing must not starve the
        others (a source outage skips scheduling/janitor otherwise).
        Each step also takes its production loop's lock, so a manual tick
        cannot overlap the same step on a live loop — or another manual
        tick.
        """
        for _step, _lock_name in (
            (self._poll_source_step, "_ops_lock"),
            (self._schedule_step, "_schedule_lock"),
            (self._janitor_step, "_janitor_lock"),
        ):
            try:
                lock = self._loop_lock(_lock_name)
                if lock is not None:
                    async with lock:
                        await _step()
                else:
                    await _step()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "tick step %s failed; continuing: %s", getattr(_step, "__name__", "?"), e
                )
