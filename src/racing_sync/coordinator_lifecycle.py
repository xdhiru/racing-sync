"""Coordinator lifecycle mixin: boot, loops, shutdown, lane health.

Split out of the coordinator god-file. Start-up composes the clients,
recovers the DB and rebuilds the SSD ledger; `run()` then supervises
four independent loops (poller / scheduler / janitor / qb listener)
whose per-lane health the API reads. Shutdown cancels every task under
a bounded budget and closes the clients.

The mixin is duck-typed against ``Coordinator`` (the dataclass fields
live there) and must never import ``racing_sync.coordinator`` (circular).
"""

from __future__ import annotations

import asyncio
import logging
import time

from .clients.http_base import AuthError
from .composition import compose
from .coordinator_errors import (
    TRACKER_UNREGISTERED_MARKER,
    AbandonedError,
)
from .io_bounds import offload
from .state import State

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


# Boot-step retry budget: a 30s WebUI/SSH blip during deploy must not
# kill the daemon into a supervisor restart storm. Auth errors fail fast
# (bad credentials never heal, and hammering risks lockout); everything
# else backs off and the boot continues.
_STARTUP_ATTEMPTS = 5
_STARTUP_BACKOFF_S = (2.0, 4.0, 8.0, 16.0)


def _strict_lane_number(value: object) -> float | None:
    """Strict timestamp/duration coercion: bools, NaN, inf, doubles -> None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


async def _start_with_backoff(
    label: str,
    factory,
    *,
    attempts: int = _STARTUP_ATTEMPTS,
    delays: tuple[float, ...] = _STARTUP_BACKOFF_S,
):
    """Run an I/O-doing boot step, retrying transient failures.

    `factory` is a no-arg async callable or a blocking sync callable
    (ran in a worker thread so a dial stall never wedges the loop).
    AuthError always fails fast; CancelledError propagates for shutdown.
    """
    try:
        tries = max(1, int(attempts))
    except (TypeError, ValueError):
        tries = _STARTUP_ATTEMPTS
    last: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            if asyncio.iscoroutinefunction(factory):
                return await factory()
            return await asyncio.to_thread(factory)
        except AuthError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            last = e
            if attempt >= tries:
                break
            try:
                delay = float(delays[min(attempt - 1, len(delays) - 1)])
            except (TypeError, ValueError, IndexError):
                delay = 2.0
            log.warning(
                "startup: %s failed (%s); retrying in %.0fs (%d/%d)",
                label,
                e,
                delay,
                attempt,
                tries,
            )
            await asyncio.sleep(max(0.0, delay))
    assert last is not None
    raise last


def _log_api_task_done(task: asyncio.Task) -> None:
    """Done-callback for the API serve task: a dead API must be loud.

    Without this, `OSError: port in use` (or a missing uvicorn) kills the
    task silently and the daemon runs headless believing the API is up.
    """
    try:
        if task.cancelled():
            return
        exc = task.exception()
    except Exception:
        return
    if exc is not None:
        log.error("API server task died: %s", exc)


class LifecycleMixin:
    """Boot/run/shutdown plus the production loop lanes (duck-typed ``self``)."""

    # ---- lifecycle ----

    def _warn_if_storage_paths_overlap(self) -> None:
        """Warn when SSD dirs overlap fuse mounts (config footgun).

        on_fuse detection, move sources, and wipe guards all assume disjoint
        trees. A nested layout (e.g. SSD scratch inside the fuse mount path)
        silently misclassifies every torrent. Advisory only — never fatal.
        """
        try:
            ssd_dirs = [str(self.rt.storage.save_path), str(self.rt.storage.path)]
            fuse = [str(path) for path in self.rt.seed.mounts()]
        except Exception:
            return

        def _norm(p: str) -> str:
            return p.rstrip("/\\").replace("\\", "/")

        for s in ssd_dirs:
            for fm in fuse:
                sn, fn = _norm(s or ""), _norm(fm or "")
                if sn and fn and (sn == fn or sn.startswith(fn + "/") or fn.startswith(sn + "/")):
                    log.warning(
                        "storage overlap: SSD path %s overlaps fuse mount %s — "
                        "on_fuse detection and moves will misbehave; use disjoint paths",
                        s,
                        fm,
                    )

    def _warn_if_log_dir_on_data_mount(self) -> None:
        """Warn when log_dir shares a filesystem with SSD/state (ENOSPC feedback loop).

        TimedRotatingFileHandler has no size cap: per-tick INFO lines fill the
        log disk, and once ENOSPC hits every log call emits a traceback that
        fills it faster while SQLite upserts start failing with SQLITE_FULL.
        Advisory only — never fatal.
        """
        try:
            from pathlib import Path as _Path

            log_dir = _Path(str(self.rt.paths.log_dir))
            data_paths = [
                _Path(str(self.rt.paths.state_db)).parent,
                _Path(str(self.rt.storage.path)),
                _Path(str(self.rt.storage.save_path)),
            ]
            try:
                log_dev = log_dir.stat().st_dev if log_dir.exists() else None
            except OSError:
                log_dev = None
            if log_dev is None:
                return
            for dp in data_paths:
                try:
                    if not dp.exists():
                        continue
                    if dp.stat().st_dev == log_dev:
                        log.warning(
                            "log dir %s shares a filesystem with data path %s — "
                            "a full SSD will take down logging and state.db; "
                            "put logs on a separate mount with retention",
                            log_dir,
                            dp,
                        )
                        break
                except OSError:
                    continue
        except Exception:
            return

    async def _drain_disabled_links(self) -> None:
        """Move rows stranded in a link this chain no longer runs.

        Runs before the SSD ledger rebuild so a drained row never reserves
        budget. RE_ADDING rows are safe to finish when seeding is off: their
        bytes were verified on the seed target before the state was entered.
        A MOVING row with a checkpoint may be split between SSD and remote,
        so it fails loudly for the operator instead of claiming DONE.
        """
        chain = self.rt.chain
        if chain.adds_seed_entry and chain.moves:
            return
        try:
            rows = await offload(self.store.all)
        except Exception as e:  # noqa: BLE001
            log.warning("stranded-row drain could not list rows: %s", e)
            return
        for ts in rows or []:
            name = (ts.source_name or ts.source_infohash or "")[:60]
            try:
                if ts.state == State.RE_ADDING and not chain.adds_seed_entry:
                    log.warning("draining %s: RE_ADDING -> DONE (seed link disabled)", name)
                    self.transition(ts, State.DONE)
                elif ts.state == State.MOVING and not chain.moves:
                    if str(getattr(ts, "move_stage", "") or "").strip():
                        self.transition(
                            ts,
                            State.FAILED,
                            error="transfer link disabled while a move is in progress — "
                            "re-enable transfer and retry the row",
                        )
                    else:
                        log.warning("draining %s: MOVING -> DONE (transfer link disabled)", name)
                        self.transition(ts, State.DONE)
            except Exception as e:  # noqa: BLE001
                log.warning("stranded-row drain failed for %s: %s", name, e)

    async def start(self) -> None:
        log.info("coordinator starting")
        self._boot_mono = time.monotonic()
        # Build marker: proves which ordering-guarantee build a log file ran.
        # Bump when the fresh-DB / late-seed ordering rules change.
        log.info("build ordering-guard v3 active (move-before-inject, DONE->MOVING demotion)")
        try:
            if self.rt.chain.single_box:
                log.info(
                    "single-box topology: source and dest are the same client; "
                    "entries seeding from the seed path are excluded from intake "
                    "and never deleted"
                )
        except Exception:
            pass
        self._warn_if_storage_paths_overlap()
        self._warn_if_log_dir_on_data_mount()
        self._download_sem = asyncio.Semaphore(self.rt.limits.max_active_downloads)
        self._move_sem = asyncio.Semaphore(self.rt.limits.max_concurrent_moves)
        if self._ssd_lock is None:
            # Eager-init so concurrent admissions cannot race lazy creation
            # and install two different locks (split-brain budget).
            self._ssd_lock = asyncio.Lock()
        log.info(
            "concurrency limits: max_active_downloads=%d, max_concurrent_moves=%d",
            self.rt.limits.max_active_downloads,
            self.rt.limits.max_concurrent_moves,
        )
        try:
            services = compose(self.cfg)
            self.source_client = services.source
            self.dest_client = services.dest
            self.prowlarr = services.indexers
            self.sftp = services.blobs
            self.watch = services.watch

            await self.source_client.start()
            await self.dest_client.start()

            if self.prowlarr is not None:
                await _start_with_backoff("prowlarr", self.prowlarr.start)

            if self.sftp is not None:
                await _start_with_backoff("sftp", self.sftp.connect)
                # Reuse the shared connection for Deluge .torrent fallback
                # instead of a fresh handshake per get_torrent_files call.
                wire = getattr(self.source_client, "set_sftp_exporter", None)
                if callable(wire):
                    try:
                        wire(self.sftp)
                    except Exception:
                        pass
            # NOTE: qBittorrent sources export through the source client's
            # own endpoint instead; self.sftp stays None for them.

            if self.rt.recovery.run_on_startup:
                from .recovery import _reconcile_snapshot

                # Refactor (lazy recovery): fast snapshot only (no per-row
                # RPC verification), daemon serves in seconds. Byte
                # verification, classification, blob export and orphan
                # fixes heal lazily on the next worker tick via the
                # existing fail-closed gates (fuse gate re-verifies,
                # _do_queued classifies, blob exports on demand).
                # Unverified rows stay fail-closed (janitor requires
                # fuse_verified). reconcile_verify() remains for the
                # API /recover endpoint and tests, but no longer runs
                # as a background task (no shutdown race, no 4-wide
                # per-row storm at boot).
                await _reconcile_snapshot(
                    self.cfg,
                    dest=self.dest_client,
                    store=self.store,
                    verify=False,
                )
                self._recovery_task = None

            # Auto-retry FAILED rows so a previous run's hard failures
            # (e.g. Deluge RPC unavailable) get another chance with the
            # new code. This is the common path after the user upgrades
            # and re-runs.
            if self.rt.recovery.auto_retry_failed:
                try:
                    _all_rows = await offload(self.store.all)
                except Exception:
                    _all_rows = []
                failed_rows = [ts for ts in (_all_rows or []) if ts.state == State.FAILED]
                max_retries = self.rt.recovery.max_failed_retries
                for ts in failed_rows:
                    if ts.failed_retries >= max_retries:
                        log.warning(
                            "skipping auto-retry for %s: reached max retries (%d/%d)",
                            ts.source_name,
                            ts.failed_retries,
                            max_retries,
                        )
                        continue
                    try:
                        _err = str(getattr(ts, "last_error", "") or "")
                    except Exception:
                        _err = ""
                    if _err.startswith(TRACKER_UNREGISTERED_MARKER):
                        # A tracker-deleted release stays deleted: reviving
                        # it would burn SSD + ratio re-downloading a swarm
                        # with no seeds, then fail again. Operator-only.
                        log.info(
                            "skipping auto-retry for %s: tracker deleted "
                            "the release (never auto-retried)",
                            ts.source_name,
                        )
                        continue
                    ts.failed_retries += 1
                    # A manual copy may have started while the row was failed:
                    # give the revived row a fresh manual-move window.
                    ts.fuse_wait_first_at = None
                    try:
                        self.store.upsert(ts)
                    except AbandonedError:
                        log.info(
                            "auto-retry: row gone for %s; skipping", (ts.source_infohash or "")[:10]
                        )
                        continue
                    except Exception:
                        continue
                    try:
                        self.transition(ts, State.NEW)
                    except (AbandonedError, ValueError) as e:
                        log.info(
                            "auto-retry: cannot revive %s (%s); skipping",
                            (ts.source_infohash or "")[:10],
                            e,
                        )
                        continue

            # A disabled link must not strand rows in a stage that will
            # never run; drain them before the ledger rebuild below.
            try:
                await self._drain_disabled_links()
            except Exception as e:  # noqa: BLE001
                log.warning("stranded-row drain failed: %s", e)

            # Optional Telegram bot (lazy import: python-telegram-bot is
            # only required when enabled).
            # Rebuild the SSD ledger from DB AFTER recovery/auto-retry so an
            # abrupt stop (kill -9, power loss) resumes with correct budget
            # instead of double-spending freed space. Pre-seed quiet-wait
            # deadlines for already-parked rows so tick one doesn't herd them.
            try:
                await self._ssd_rebuild_from_db()
            except Exception:
                pass
            try:
                _interval = float(getattr(self, "WAITING_DISK_RECHECK_SECONDS", 60.0))
            except (TypeError, ValueError):
                _interval = 60.0
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if not isinstance(_wd, dict):
                    _wd = {}
                    self._waiting_disk_next_check = _wd  # type: ignore[attr-defined]
                _now_m = time.monotonic()
                for _row in self.store.all_active():
                    if getattr(_row, "state", None) == State.WAITING_DISK:
                        _wd.setdefault(
                            (getattr(_row, "source_infohash", "") or "").lower(),
                            _now_m + _interval,
                        )
            except Exception:
                pass
            self._tg = None
            if self.rt.control.telegram:
                from .telegram_bot import TelegramBot

                self._tg = TelegramBot(self.cfg.telegram, self, self.store)
                await self._tg.start()

            # Optional FastAPI control plane
            from .api import serve

            self._api_task: asyncio.Task | None = None
            if self.rt.control.api:
                self._api_task = asyncio.create_task(serve(self), name="rs-api")
                try:
                    self._api_task.add_done_callback(_log_api_task_done)
                except Exception:
                    pass

            self._coordinator_started = True
            try:
                self._started_at = time.monotonic()
            except Exception:
                pass
        except BaseException:
            # Roll back any sessions that were already opened, so we
            # don't leak aiohttp "Unclosed client session" warnings.
            await self._rollback_partial_start()
            raise

    async def _rollback_partial_start(self) -> None:
        """Close any clients that were successfully started before a
        later step failed. Idempotent.
        """
        if getattr(self, "source_client", None) is not None:
            try:
                await self.source_client.close()
            except Exception:  # noqa: BLE001
                pass
            self.source_client = None  # type: ignore[assignment]
        if getattr(self, "dest_client", None) is not None:
            try:
                await self.dest_client.close()
            except Exception:  # noqa: BLE001
                pass
            self.dest_client = None  # type: ignore[assignment]
        prowlarr = getattr(self, "prowlarr", None)
        if prowlarr is not None:
            try:
                await prowlarr.close()
            except Exception:  # noqa: BLE001
                pass
            self.prowlarr = None
        sftp = getattr(self, "sftp", None)
        if sftp is not None:
            try:
                sftp.close()
            except Exception:  # noqa: BLE001
                pass
            self.sftp = None

    def request_stop(self) -> None:
        log.info("stop requested")
        self._stop = True

    def set_intake_paused(self, paused: bool) -> bool:
        """Pause/resume intake of NEW work; returns the new state.

        Pause stops workers from starting on NEW rows (poller intake
        handoff and scheduler); rows already in flight drain normally
        and discovery keeps recording arrivals for the resume.
        """
        self._intake_paused = bool(paused)
        log.warning("intake %s by operator", "paused" if self._intake_paused else "resumed")
        return self._intake_paused

    async def shutdown(self) -> None:
        # Idempotent: __main__ and run() both call us, so guard against
        # the second invocation producing duplicate log lines.
        # Budget table (worst case, sequential): workers 15 + tg tasks 10
        # + tg.stop 10 + api drain 10 + prowlarr 5 + source 5 + dest 5 =
        # 60s, matching the __main__ runner budget — no stage may grow
        # without raising that budget, or the tail (client/store close)
        # gets abandoned on timeout.
        if self._shutdown_done:
            return
        self._shutdown_done = True
        log.info("coordinator shutting down")
        try:
            _rt = getattr(self, "_recovery_task", None)
            if _rt is not None and not _rt.done():
                _rt.cancel()
                try:
                    await _rt
                except (asyncio.CancelledError, Exception):
                    pass
        except Exception:
            pass
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            # Bounded: a wedged worker (stuck fuse stat, dead SFTP) must
            # not wedge SIGTERM forever.
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=15.0,
                )
            except (TimeoutError, asyncio.CancelledError):
                pass
        for t in list(getattr(self, "_tg_tasks", set())):
            t.cancel()
        if getattr(self, "_tg_tasks", None):
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._tg_tasks), return_exceptions=True),
                    timeout=10.0,
                )
            except (TimeoutError, asyncio.CancelledError):
                pass
        tg = getattr(self, "_tg", None)
        if tg is not None:
            try:
                await asyncio.wait_for(tg.stop(), timeout=10.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass
        api_task = getattr(self, "_api_task", None)
        if api_task is not None:
            # Graceful first: let in-flight requests drain via should_exit
            # (a bare cancel kills them and lingers sockets).
            try:
                _srv = getattr(self, "_api_server", None)
                if _srv is not None:
                    _srv.should_exit = True
            except Exception:
                pass
            try:
                await asyncio.wait_for(api_task, timeout=10.0)
            except (TimeoutError, asyncio.CancelledError, Exception):
                try:
                    if not api_task.done():
                        api_task.cancel()
                except Exception:
                    pass
                try:
                    await api_task
                except (asyncio.CancelledError, Exception):
                    pass
        if self.prowlarr is not None:
            try:
                await asyncio.wait_for(self.prowlarr.close(), timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass
        if self.sftp is not None:
            try:
                self.sftp.close()
            except Exception:  # noqa: BLE001
                pass
        if self._coordinator_started:
            try:
                await asyncio.wait_for(self.source_client.close(), timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass
            try:
                await asyncio.wait_for(self.dest_client.close(), timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass
        try:
            self.store.close()
        except Exception:  # noqa: BLE001
            pass

    # ---- main loop ----

    def _api_task_failed(self) -> bool:
        """True when the enabled API server died with an error.

        Failing fast (stop) beats running headless: without this, a
        `port in use` death is only logged and the daemon serves
        forever with no control plane. Clean exits and cancellations
        never trigger (a quiet API shutdown is not a failure).
        """
        try:
            _api_task = getattr(self, "_api_task", None)
            _api_cfg = getattr(getattr(self, "cfg", None), "api", None)
            if not bool(_api_cfg and getattr(_api_cfg, "enabled", False)):
                return False
            if _api_task is None or not _api_task.done():
                return False
            if _api_task.cancelled():
                return False
            try:
                _api_err = _api_task.exception()
            except Exception:
                return False
            if _api_err is None:
                return False
            log.error("API server died (%s); stopping instead of running headless", _api_err)
            return True
        except Exception:
            return False

    async def run(self) -> int:
        """Supervise the poller, scheduler, janitor and qb-listener loops.

        Four independent loops with separate locks and clocks (a wedged
        source poll, wedged scheduler, slow janitor, or dead dest poll
        degrades only its own lane). Each loop guards its own iterations
        and never raises; the supervisor just waits for stop and cancels
        them on shutdown. `_tick()` remains the single-pass equivalent
        for tests.
        """
        await self.start()
        try:
            loop_tasks = getattr(self, "_loop_tasks", None)
            if not isinstance(loop_tasks, set):
                loop_tasks = set()
                try:
                    self._loop_tasks = loop_tasks
                except Exception:
                    pass
            factories = (
                self._poller_loop,
                self._scheduler_loop,
                self._janitor_loop,
                self._qb_listener_loop,
            )
            owned: dict[asyncio.Task, object] = {}
            for factory in factories:
                try:
                    task = asyncio.create_task(factory())
                except RuntimeError:
                    break
                try:
                    loop_tasks.add(task)
                    owned[task] = factory
                except Exception:
                    pass
            # Telegram loops under the same supervisor: adopt the tasks
            # bot.start() already spawned (never spawn a second poll —
            # double polling self-conflicts); a crashed detail worker /
            # refresh loop / callback poll respawns loudly like any other
            # lane. Conflict-fatal polls never respawn (another session
            # holds the token — respawning would re-conflict).
            try:
                _tg = getattr(self, "_tg", None)
                if _tg is not None and not bool(getattr(_tg, "_poll_conflict_fatal", False)):
                    try:
                        _tg_by_name = {
                            "rs-telegram": _tg._loop,
                            "rs-telegram-callbacks": _tg._callback_loop,
                            "rs-telegram-detail": _tg._detail_worker_loop,
                        }
                    except Exception:
                        _tg_by_name = {}
                    for _attr in ("_task", "_callback_task", "_detail_worker"):
                        try:
                            _t = getattr(_tg, _attr, None)
                        except Exception:
                            continue
                        if not isinstance(_t, asyncio.Task) or _t.done():
                            continue
                        try:
                            _fac = (_tg_by_name or {}).get(_t.get_name())
                        except Exception:
                            _fac = None
                        if not callable(_fac):
                            continue
                        try:
                            loop_tasks.add(_t)
                            owned[_t] = _fac
                        except Exception:
                            pass
            except Exception:
                pass
            while not self._stop:
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    break
                # A dead control plane must fail fast, not run headless:
                # a bound port (or dead uvicorn) that only gets logged
                # leaves the daemon believing the API is up.
                try:
                    if self._api_task_failed():
                        self.request_stop()
                        break
                except Exception:
                    pass
                # A dead loop is a bug: respawn it loudly instead of
                # silently losing a lane (or crashing the coordinator).
                try:
                    for task in list(loop_tasks):
                        if task.done() and not task.cancelled():
                            factory = owned.pop(task, None)
                            try:
                                loop_tasks.discard(task)
                            except Exception:
                                pass
                            try:
                                task.result()
                            except asyncio.CancelledError:
                                pass
                            except Exception as e:  # noqa: BLE001
                                log.error("loop task died; respawning: %s", e, exc_info=True)
                            if factory is not None and not self._stop:
                                try:
                                    _fself = getattr(factory, "__self__", None)
                                    _fconflict = (
                                        bool(getattr(_fself, "_poll_conflict_fatal", False))
                                        if _fself is not None
                                        else False
                                    )
                                except Exception:
                                    _fconflict = False
                                if _fconflict:
                                    log.error(
                                        "telegram poll conflict-fatal; not "
                                        "respawning (another session holds "
                                        "the token)"
                                    )
                                    continue
                                try:
                                    new_task = asyncio.create_task(factory())
                                    loop_tasks.add(new_task)
                                    owned[new_task] = factory
                                except Exception:
                                    pass
                except Exception:
                    pass
        finally:
            try:
                for task in list(getattr(self, "_loop_tasks", None) or []):
                    try:
                        task.cancel()
                    except Exception:
                        pass
                try:
                    _lt = list(getattr(self, "_loop_tasks", None) or [])
                    if _lt:
                        await asyncio.wait_for(
                            asyncio.gather(*_lt, return_exceptions=True),
                            timeout=10.0,
                        )
                except (TimeoutError, asyncio.CancelledError):
                    pass
            except (TimeoutError, asyncio.CancelledError):
                pass
            except Exception:
                pass
            await self.shutdown()
        return 0

    # ---- per-lane health (production loop heartbeats) ----

    def _lane_health_map(self) -> dict:
        """Lane-health map, lazily created.

        Bare test doubles are constructed with ``object.__new__`` (no
        dataclass field defaults), so this attribute cannot be a field.
        """
        try:
            lanes = getattr(self, "_lane_health", None)
        except Exception:
            lanes = None
        if not isinstance(lanes, dict):
            lanes = {}
            try:
                self._lane_health = lanes
            except Exception:
                pass
        return lanes

    def _lane_entry(self, name: str) -> dict:
        lanes = self._lane_health_map()
        entry = lanes.get(name)
        if not isinstance(entry, dict):
            entry = {}
            lanes[name] = entry
        return entry

    def _lane_note_attempt(self, name: str) -> float:
        """Stamp a lane attempt (before the lock/step); returns start time."""
        now = time.monotonic()
        try:
            self._lane_entry(name)["last_attempt_at"] = now
        except Exception:
            pass
        return now

    def _lane_note_success(self, name: str, started_at) -> None:
        """Stamp a lane success + duration; a pass clears failure state."""
        try:
            now = time.monotonic()
            entry = self._lane_entry(name)
            entry["last_success_at"] = now
            _start = _strict_lane_number(started_at)
            entry["last_duration_s"] = max(0.0, now - _start) if _start is not None else 0.0
            entry["consecutive_failures"] = 0
            entry["last_error"] = ""
        except Exception:
            pass

    def _lane_note_failure(self, name: str, error) -> None:
        """Stamp a lane failure; CancelledError must never reach here."""
        try:
            entry = self._lane_entry(name)
            try:
                failures = int(entry.get("consecutive_failures", 0) or 0)
            except (TypeError, ValueError):
                failures = 0
            entry["consecutive_failures"] = max(0, failures) + 1
            entry["last_failure_at"] = time.monotonic()
            try:
                text = str(error) or error.__class__.__name__
            except Exception:
                text = error.__class__.__name__
            entry["last_error"] = text[:300]
        except Exception:
            pass

    def lane_health_snapshot(self) -> dict:
        """Defensive, strict-typed copy of per-lane health (never raises)."""
        out: dict[str, dict] = {}
        try:
            items = list(self._lane_health_map().items())
        except Exception:
            items = []
        for name, raw in items:
            try:
                if not isinstance(name, str) or not isinstance(raw, dict):
                    continue
                duration = _strict_lane_number(raw.get("last_duration_s"))
                failures = raw.get("consecutive_failures", 0)
                if isinstance(failures, bool) or not isinstance(failures, int):
                    failures = 0
                error = raw.get("last_error", "")
                if not isinstance(error, str):
                    error = ""
                out[name] = {
                    "last_attempt_at": _strict_lane_number(raw.get("last_attempt_at")),
                    "last_success_at": _strict_lane_number(raw.get("last_success_at")),
                    "last_failure_at": _strict_lane_number(raw.get("last_failure_at")),
                    "last_duration_s": (max(0.0, duration) if duration is not None else None),
                    "consecutive_failures": max(0, failures),
                    "last_error": error[:300],
                }
            except Exception:
                continue
        return out

    def _loop_lock(self, name: str) -> asyncio.Lock:
        """Per-loop lock, lazily created (tolerates bare test doubles)."""
        try:
            lock = getattr(self, name, None)
            if lock is not None and hasattr(lock, "__aenter__"):
                return lock
        except Exception:
            pass
        try:
            lock = asyncio.Lock()
        except Exception:
            lock = None
        try:
            setattr(self, name, lock)
        except Exception:
            pass
        return lock

    # ---- production loops ----

    async def _poller_loop(self) -> None:
        """Source-poller loop: watch + source ingest + fuse sweep."""
        while not self._stop:
            _lane_started = self._lane_note_attempt("poller")
            try:
                lock = self._loop_lock("_ops_lock")
                if lock is not None:
                    async with lock:
                        await self._poll_source_step()
                else:
                    await self._poll_source_step()
            except asyncio.CancelledError:
                break
            except AuthError as e:
                self._lane_note_failure("poller", e)
                log.error("poller: auth still failing; backing off: %s", e)
            except TimeoutError as e:
                self._lane_note_failure("poller", e)
                log.warning("poller: timed out; backing off: %s", e)
            except Exception as e:  # noqa: BLE001
                self._lane_note_failure("poller", e)
                log.error("poller: unexpected error; backing off: %s", e, exc_info=True)
            else:
                self._lane_note_success("poller", _lane_started)
            if self._stop:
                break
            try:
                await asyncio.sleep(max(1, int(float(self.rt.limits.source_poll_interval or 30))))
            except (TypeError, ValueError):
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def _scheduler_loop(self) -> None:
        """Worker-scheduler loop: wakeups, admissions, live refresh."""
        while not self._stop:
            _lane_started = self._lane_note_attempt("scheduler")
            try:
                lock = self._loop_lock("_schedule_lock")
                if lock is not None:
                    async with lock:
                        await self._schedule_step()
                else:
                    await self._schedule_step()
            except asyncio.CancelledError:
                break
            except AuthError as e:
                self._lane_note_failure("scheduler", e)
                log.error("scheduler: auth still failing; backing off: %s", e)
            except TimeoutError as e:
                self._lane_note_failure("scheduler", e)
                log.warning("scheduler: timed out; backing off: %s", e)
            except Exception as e:  # noqa: BLE001
                self._lane_note_failure("scheduler", e)
                log.error("scheduler: unexpected error; backing off: %s", e, exc_info=True)
            else:
                self._lane_note_success("scheduler", _lane_started)
            if self._stop:
                break
            try:
                await asyncio.sleep(max(1, int(float(self.rt.limits.dest_poll_interval or 30))))
            except (TypeError, ValueError):
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def _janitor_loop(self) -> None:
        """Janitor loop: VPS1 cleanup + tombstone GC (self-gated cadence)."""
        # The step self-gates on janitor_interval_seconds, so the loop
        # wakes often enough to stay responsive without busy work.
        while not self._stop:
            _lane_started = self._lane_note_attempt("janitor")
            try:
                lock = self._loop_lock("_janitor_lock")
                if lock is not None:
                    async with lock:
                        await self._janitor_step()
                else:
                    await self._janitor_step()
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                self._lane_note_failure("janitor", e)
                log.error("janitor: unexpected error; backing off: %s", e, exc_info=True)
            else:
                self._lane_note_success("janitor", _lane_started)
            if self._stop:
                break
            try:
                _cleanup = getattr(self.cfg, "cleanup", None)
                _normal = int(getattr(_cleanup, "janitor_interval_seconds", 3600) or 3600)
                _burst = int(getattr(_cleanup, "burst_janitor_interval_seconds", 300) or 300)
                # Wake on the burst cadence: the step self-gates (calm vs
                # burst) and returns immediately when cleanup is disabled,
                # so a tight disk is noticed within burst_janitor_interval.
                _wake = max(60, min(_burst, _normal))
            except (TypeError, ValueError):
                _wake = 300
            try:
                await asyncio.sleep(_wake)
            except asyncio.CancelledError:
                break
