"""Main coordinator loop.

This is the only place that orchestrates state transitions, qBittorrent
calls, rclone invocations, and prowlarr lookups. The flow per torrent:

  1. Detect on VPS1 racing client (category=racing)
  2. Pick the SSD-source torrent:
       - if VPS1 has multiple, prefer public (req #1) via prowlarr or SFTP
       - if VPS1 has only private, query prowlarr by tracker map (req #2)
       - if from watch_dir, prefer prowlarr hit on a download-target indexer (req #3)
  3. Add to VPS2 qBittorrent at SSD save_path, paused, skip_check=False
  4. Resume; poll until complete (with batched file priorities for seasons)
  5. rclone move SSD -> remote (with --include for seasons)
  6. After move: re-add private torrents to VPS2 pointing at fuse, skip_check=True
  7. Mark DONE
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import logging
import shutil  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
import stat  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
import time
from dataclasses import dataclass, field

from .capabilities import BlobSource, WatchIntake
from .classifier import (
    classify,  # noqa: F401  (runtime use via coordinator_admit lazy lookup + compat)
)
from .clients.abstract import Torrent, TorrentClient
from .clients.http_base import AuthError
from .composition import compose  # noqa: F401  (kept for module-surface compat)
from .config import AppConfig
from .coordinator_admit import AdmitMixin
from .coordinator_cleanup import CleanupMixin
from .coordinator_content import (
    _TELEGRAM_NOTIFY_STATES as _TELEGRAM_NOTIFY_STATES,  # noqa: F401  (re-exported API)
    PUBLIC_TRACKER_HOSTS as PUBLIC_TRACKER_HOSTS,  # noqa: F401  (re-exported API)
    SourceDecision,
    _looks_public,
    _matches_release,
    _should_notify_telegram,
    _verified_cross_seed_blob,
    cleanup_grace_seconds,
    normalize_content_name,
)
from .coordinator_download import DownloadMixin, LiveItem
from .coordinator_errors import (
    _NOT_VISIBLE_DETAIL,
    TRACKER_UNREGISTERED_MARKER,  # noqa: F401  (kept for module-surface compat)
    AbandonedError,
    BatchMoveIncompleteError,
    DownloadStalledError,
    RcloneTransientError,
    TorrentCheckingError,
    UnregisteredTorrentError,
    WebUIUnresponsiveError,
    is_fatal_os_error,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
)
from .coordinator_fuse import FuseMixin
from .coordinator_inject import (
    InjectMixin,
    _group_is_ignored,  # noqa: F401  (kept for module-surface compat)
)
from .coordinator_lifecycle import (
    _STARTUP_ATTEMPTS as _STARTUP_ATTEMPTS,  # noqa: F401  (re-exported API)
    _STARTUP_BACKOFF_S as _STARTUP_BACKOFF_S,  # noqa: F401  (re-exported API)
    LifecycleMixin,
    _log_api_task_done as _log_api_task_done,  # noqa: F401  (re-exported API)
    _start_with_backoff as _start_with_backoff,  # noqa: F401  (re-exported API)
    _strict_lane_number as _strict_lane_number,  # noqa: F401  (re-exported API)
)
from .coordinator_live import LiveStatusMixin
from .coordinator_move import MoveMixin
from .coordinator_ops import (
    _SOURCE_GONE as _SOURCE_GONE,  # noqa: F401  (re-exported API)
    _SOURCE_STALE_EXPIRY_S as _SOURCE_STALE_EXPIRY_S,  # noqa: F401  (re-exported API)
    OpsMixin,
    _elect_primary_torrent as _elect_primary_torrent,  # noqa: F401  (re-exported API)
)
from .coordinator_picker import pick_ssd_source_for_racing
from .coordinator_readd import ReaddMixin
from .coordinator_schedule import (
    _LATE_SEED_FAILURE_WINDOW as _LATE_SEED_FAILURE_WINDOW,  # noqa: F401  (re-exported API)
    _MAP_ENTRY_CAP as _MAP_ENTRY_CAP,  # noqa: F401  (re-exported API)
    _MAP_SWEEP_THROTTLE_S as _MAP_SWEEP_THROTTLE_S,  # noqa: F401  (re-exported API)
    _MISC_WORKER_BUDGET as _MISC_WORKER_BUDGET,  # noqa: F401  (re-exported API)
    _SCHED_STATE_PRIORITY as _SCHED_STATE_PRIORITY,  # noqa: F401  (re-exported API)
    _STOP_WORKERS_TIMEOUT_S as _STOP_WORKERS_TIMEOUT_S,  # noqa: F401  (re-exported API)
    ScheduleMixin,
    _sched_priority as _sched_priority,  # noqa: F401  (re-exported API)
)
from .coordinator_ssd import SSDLedgerMixin
from .coordinator_watchdog import WatchdogMixin
from .prowlarr import ProwlarrClient
from .rclone_ops import (
    RcloneTimeoutError,
    move_local_to_remote,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
    ssd_has_room,  # noqa: F401  (runtime use via coordinator_admit lazy lookup + compat)
    ssd_max_inflight_bytes,  # noqa: F401  (runtime use via coordinator_ssd lazy lookup + compat)
    wipe_local_tree,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
)
from .runtime import Runtime
from .state import State, StateStore, TorrentState

log = logging.getLogger(__name__)


__all__ = [
    "AbandonedError",
    "BatchMoveIncompleteError",
    "Coordinator",
    "DownloadStalledError",
    "LiveItem",
    "RcloneTransientError",
    "SourceDecision",
    "TorrentCheckingError",
    "UnregisteredTorrentError",
    "WebUIUnresponsiveError",
    "_NOT_VISIBLE_DETAIL",
    "_looks_public",
    "_matches_release",
    "_should_notify_telegram",
    "_verified_cross_seed_blob",
    "cleanup_grace_seconds",
    "normalize_content_name",
    "pick_ssd_source_for_racing",
]

# (stateless helpers live in coordinator_content.py; re-exported above)


# (moved to coordinator_content.py; re-exported above)


# (moved to coordinator_content.py; re-exported above)


# (_lerp / cleanup_grace_seconds moved to coordinator_content.py; re-exported above)


# (cross-seed picker lives in coordinator_picker.py; path guard in
# coordinator_paths.py — both re-exported above)


# (pick_ssd_source_for_racing moved to coordinator_picker.py — chunk 1/3 removed)
# (chunk 2/3 removed: public-export + prowlarr query branches)
# (chunk 3/3 removed: direct-export fallback)


# Per-state worker-pass budget (seconds): how long ONE handler invocation
# may run before it is cancelled and retried on the next tick. Without it a
# wedged pass pins its worker slot (and any semaphore it holds) until the
# 24h max-age backstop; cancelling releases both. Values stay generous so
# slow-but-alive work is never interrupted: the picker alone caps a NEW
# pass at 5 min, and every RPC underneath is timeout-bounded. DOWNLOADING
# and MOVING are exempt because their handlers own per-poll deadlines
# (stall/flush/check windows, rclone move_timeout_seconds) and a healthy
# download or move legitimately runs for hours.
_STATE_WORK_BUDGET_S: dict[State, float | None] = {
    State.NEW: 3600.0,
    State.WAITING_INDEXER: 3600.0,
    State.WAITING_DISK: 1800.0,
    State.QUEUED: 3600.0,
    State.RE_ADDING: 3600.0,
    State.DOWNLOADING: None,
    State.MOVING: None,
}

# Consecutive over-budget passes (same state, same row) before the row is
# failed for inspection instead of retried forever.
_MAX_STATE_BUDGET_HITS = 3

# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #


@dataclass
class Coordinator(
    SSDLedgerMixin,
    CleanupMixin,
    WatchdogMixin,
    LiveStatusMixin,
    InjectMixin,
    ReaddMixin,
    FuseMixin,
    DownloadMixin,
    MoveMixin,
    AdmitMixin,
    OpsMixin,
    ScheduleMixin,
    LifecycleMixin,
):
    cfg: AppConfig
    store: StateStore = field(init=False)
    source_client: TorrentClient = field(init=False)
    dest_client: TorrentClient = field(init=False)
    prowlarr: ProwlarrClient | None = field(init=False, default=None)
    sftp: BlobSource | None = field(init=False, default=None)
    watch: WatchIntake | None = field(init=False, default=None)
    _stop: bool = field(default=False, init=False)
    # Operator intake pause (/pause): no NEW work starts until
    # resumed; in-flight rows drain normally. In-memory on purpose — a
    # restart resumes normal intake, so nothing can wedge paused.
    _intake_paused: bool = field(default=False, init=False)
    _live: dict[str, LiveItem] = field(default_factory=dict, init=False)
    _tasks: set[asyncio.Task] = field(default_factory=set, init=False)
    _running_infohashes: set[str] = field(default_factory=set, init=False)
    _download_sem: asyncio.Semaphore | None = field(default=None, init=False)
    _move_sem: asyncio.Semaphore | None = field(default=None, init=False)
    _heal_move_sem: asyncio.Semaphore | None = field(default=None, init=False)
    _coordinator_started: bool = field(default=False, init=False)
    _shutdown_done: bool = field(default=False, init=False)
    _source_torrents_cache: list[Torrent] = field(default_factory=list, init=False)
    _source_torrents_cached_at: float = field(default=0.0, init=False)
    _failed_late_cross_seeds: dict[str, dt.datetime] = field(default_factory=dict, init=False)
    # Proven-healthy fuse verdicts (entry-hash -> next re-probe
    # monotonic): skips the per-tick export + stats for healthy DONE rows.
    _fuse_healthy_until: dict[str, float] = field(default_factory=dict, init=False)
    # VPS1 cleanup janitor ([cleanup]): monotonic timestamp of the last run
    # plus one monotonic timestamp per newly discovered racing release
    # (rolling intake-velocity signal for spam-burst grace shortening).
    _cleanup_last_run: float = field(default=0.0, init=False)
    _arrival_times: list = field(default_factory=list, init=False)
    # Serializes the periodic tick against API-triggered ops (recover /
    # scan-watch / retry) so they can't double-schedule or clobber rows.
    _ops_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    # Worker-scheduler loop lock: the scheduler runs concurrently with the
    # source poller, so it serializes only with itself. Scheduler/worker
    # races are closed by re-get guards, not by sharing the poller lock —
    # a wedged poll must never stall admissions, and vice versa.
    _schedule_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    # Janitor loop lock: hourly VPS1 cleanup + tombstone GC must never
    # stall source discovery, scheduling, or API ops behind one lock.
    _janitor_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    # Supervised loop tasks (poller/scheduler/janitor), cancelled on shutdown.
    _loop_tasks: set[asyncio.Task] = field(default_factory=set, init=False)
    # Background recovery verification (post-startup), cancelled on shutdown.
    _recovery_task: asyncio.Task | None = field(default=None, init=False)
    # Frozen per-torrent batch cap (bug 5): batch boundaries must not shift
    # when free space changes mid-download, or batch_index points at
    # different episodes (skip/repeat). Frozen at first use, reused for the
    # row's lifetime; cleared on terminal states to bound memory.
    _batch_cap_cache: dict[str, int] = field(default_factory=dict, init=False)
    # Quiet-wait for SSD-full parking: WAITING_DISK rows are re-checked at
    # most once per interval instead of every tick, so a full disk doesn't
    # spam "worker start/scheduled" INFO lines (which themselves fill the
    # log disk faster once ENOSPC starts).
    _waiting_disk_next_check: dict[str, float] = field(default_factory=dict, init=False)
    # Global SSD reservation ledger: infohash.lower() -> bytes reserved.
    # max_inflight_bytes is a GLOBAL budget across concurrent downloads, not
    # a per-torrent batch cap. Admission reserves an estimate; _setup refines
    # to the real footprint (max batch for seasons/games, total for singles)
    # so varying sizes share the budget safely. Released on RE_ADDING/DONE/
    # FAILED/WAITING_DISK/forget. Rebuilt from DB on startup for crash recovery.
    _ssd_reserved: dict[str, int] = field(default_factory=dict, init=False)
    _ssd_lock: asyncio.Lock | None = field(default=None, init=False)
    # Download admission set: hashes admitted at the QUEUED edge with a live
    # worker, bounding concurrent DOWNLOADING admissions to the configured
    # cap across same-tick racers (sync check+record; cleared on leaving
    # QUEUED, reconciled against _running_infohashes on each check).
    _download_admissions: set[str] = field(default_factory=set, init=False)
    # Consecutive MOVING-park counter: infohash.lower() -> parks in a row.
    # _do_moving parks (pause unverified, 0-transfer rclone, short bytes…)
    # with only a warning, so a gate that never passes idles as plain
    # "MOVING" forever. The counter (cleared on leaving MOVING) escalates to
    # ERROR so the live log names the stuck gate instead of whispering it.
    _moving_parks: dict[str, int] = field(default_factory=dict, init=False)
    # RE_ADDING heal tracker: infohash.lower() -> [first_missing_mono,
    # missing_key_tuple, heal_attempts]. The fuse gate parks on ANY absence,
    # but rclone dir-cache (~5m) + app stat cache (60s) + mount indexing lag
    # make fresh absences unreliable — only a *stable* missing set older
    # than fuse_readd_heal_after_seconds (2h default) fires a selective
    # re-download + move of just those files. Cleared when the gate passes
    # (incident over); attempts are capped so a remote that keeps losing
    # files falls through to the 24h max-age FAIL instead of looping.
    _readd_heal_track: dict[str, list] = field(default_factory=dict, init=False)
    # Prowlarr supplement sweep: row-hash -> next-allowed monotonic time.
    # tracker_map cross-seeds are opportunistic (VPS1 late arrivals are
    # still checked every tick); one Prowlarr round per row per window.
    _supplement_ok_at: dict[str, float] = field(default_factory=dict, init=False)
    # Worker-pass budget hits: infohash.lower() -> (state, consecutive hits).
    # A pass that overruns its state's budget is cancelled and retried; a
    # row that keeps doing so fails for inspection instead of pinning a slot.
    _state_budget_hits: dict[str, tuple[State, int]] = field(default_factory=dict, init=False)

    @property
    def rt(self) -> Runtime:
        """Typed capability views over the loaded config."""
        return Runtime(self.cfg)

    @property
    def download_sem(self) -> asyncio.Semaphore:
        if self._download_sem is None:
            self._download_sem = asyncio.Semaphore(self.rt.limits.max_active_downloads)
        return self._download_sem

    @property
    def move_sem(self) -> asyncio.Semaphore:
        if self._move_sem is None:
            self._move_sem = asyncio.Semaphore(self.rt.limits.max_concurrent_moves)
        return self._move_sem

    @property
    def heal_move_sem(self) -> asyncio.Semaphore:
        # Reserved single slot for re-add heals (seconds-long, operator-
        # visible repairs): they must not queue for hours behind bulk
        # multi-GB season moves sharing move_sem.
        if self._heal_move_sem is None:
            self._heal_move_sem = asyncio.Semaphore(1)
        return self._heal_move_sem

    # ---- lifecycle ----

    def __post_init__(self) -> None:
        self.store = StateStore(self.rt.paths.state_db)

    def _park_row(
        self,
        ts: TorrentState,
        reason: str,
        *,
        live_key: str | None = None,
        level: int | None = logging.WARNING,
        extra: dict | None = None,
    ) -> bool:
        """Stay in the current state, record WHY, and release the live entry.

        One park primitive for worker early returns. The reason lands in
        last_error (plus any `extra` columns, e.g. a retry timer) and the
        write is narrow (`update_columns` with expected_state), so a
        concurrent transition survives — a whole-row upsert after awaits
        could regress it (B2). A row that moved on is left alone (False);
        a forgotten row raises AbandonedError instead of being resurrected.
        Stores without update_columns fall back to a guarded upsert. Never
        raises for write failures — the worker is ending either way and the
        next tick re-derives the row.
        """
        try:
            ts.last_error = f"parked: {reason}"[:500]
        except Exception:
            pass
        if live_key is not None:
            try:
                self._live.pop(live_key.lower(), None)
            except Exception:
                pass
        try:
            if level is not None:
                log.log(level, "%s parked: %s", (ts.source_name or "")[:60], reason)
        except Exception:
            pass
        key = ts.source_infohash or ""
        mapping: dict = {"last_error": ts.last_error}
        if isinstance(extra, dict):
            mapping.update(extra)
        try:
            _up = getattr(self.store, "update_columns", None)
            if callable(_up):
                try:
                    _nv = _up(key, mapping, expected_state=ts.state)
                except TypeError:
                    # Test double without the expected_state kwarg.
                    _nv = _up(key, mapping)
                if isinstance(_nv, int) and _nv > 0:
                    try:
                        ts.version = _nv
                    except Exception:
                        pass
                    return True
                if self._abandoned(ts):
                    raise AbandonedError(f"row gone (forgotten?) for {key[:10]}; not parking")
                # Row moved on since the snapshot: never clobber it.
                return False
            try:
                self.store.upsert(ts, expected_state=ts.state)
            except TypeError:
                self.store.upsert(ts)
            return True
        except AbandonedError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("could not persist park for %s: %s", key[:10], e)
            return False

    # ---- per-torrent worker ----

    async def _process_torrent(self, ts: TorrentState) -> None:
        try:
            # Fresh-state guard: API retry / forget / recovery may have moved
            # this row after the tick snapshot. Never let a stale worker
            # object overwrite the new state via a later upsert.
            try:
                _fresh = None
                _fresh_known = False
                if getattr(self, "store", None) is not None and hasattr(self.store, "get"):
                    _fresh = self.store.get(ts.source_infohash, include_blob=False)
                    _fresh_known = True
            except Exception:
                _fresh = None
                _fresh_known = False
            if _fresh_known and _fresh is None:
                # Row deleted after the tick snapshot (forget/cancel):
                # never let the worker resurrect it via a later write.
                log.info(
                    "worker: row gone for %s (forgotten?); not starting",
                    ts.source_infohash[:10],
                )
                return
            if isinstance(_fresh, TorrentState):
                if _fresh.state != ts.state:
                    log.info(
                        "worker: %s moved %s -> %s by another actor; skipping stale worker",
                        ts.source_infohash[:10],
                        ts.state.value,
                        _fresh.state.value,
                    )
                    return
                try:
                    _fresh_v = int(getattr(_fresh, "version", 0) or 0)
                    _snap_v = int(getattr(ts, "version", 0) or 0)
                except (TypeError, ValueError, AttributeError):
                    _fresh_v = _snap_v = 0
                if _fresh_v != _snap_v:
                    # A concurrent narrow write (timer/display park) landed
                    # after the tick snapshot: our snapshot would clobber
                    # it via a later whole-row upsert. Skip; the next tick
                    # respawns with a fresh snapshot.
                    log.info(
                        "worker: %s version %d -> %d under us; skipping stale worker",
                        ts.source_infohash[:10],
                        _snap_v,
                        _fresh_v,
                    )
                    return
            try:
                await self._process_torrent_inner(ts)
            finally:
                self._drop_live_for(ts)
        except AbandonedError as e:
            # The DB row is gone (forget/cancel removed it mid-flight):
            # unwind quietly. Failing here would upsert-resurrect the
            # deliberately deleted row as a zombie FAILED row.
            log.info("worker: %s abandoned (%s); stopping", ts.source_infohash[:10], e)
            return
        except (RcloneTimeoutError, RcloneTransientError) as e:
            # Last-resort net: a timed-out or transiently-failed move from
            # any path parks the row (source bytes intact) instead of
            # failing it. The two expected sites handle this directly with
            # better context; this only fires for future call paths.
            log.error(
                "rclone move issue for %s: %s (parking, not failing)", ts.source_infohash[:10], e
            )
            try:
                if ts.state == State.MOVING:
                    self._park_moving(ts, f"rclone move timed out: {e}")
                else:
                    try:
                        _gone = (
                            getattr(self, "store", None) is not None
                            and hasattr(self.store, "get")
                            and self.store.get(ts.source_infohash, include_blob=False) is None
                        )
                    except Exception:
                        _gone = False
                    if _gone:
                        log.info(
                            "worker: row gone for %s; dropping timeout (%s)",
                            ts.source_infohash[:10],
                            e,
                        )
                        return
                    try:
                        ts.last_error = f"rclone timed out: {e}"[:500]
                    except Exception:
                        pass
                    self.store.upsert(ts)
            except Exception:  # noqa: BLE001
                pass
        except AuthError as e:
            # Source/dest credentials died mid-flight: never fail the row
            # over an ops incident (bad creds never heal by failing rows,
            # and hammering risks lockout). Leave the row untouched for
            # the next tick; the poller/scheduler loops page loudly via
            # their own AuthError arms.
            log.error(
                "worker: auth failure for %s (%s); leaving row for next tick",
                ts.source_infohash[:10],
                e,
            )
            return
        except Exception as e:  # noqa: BLE001
            # Backstop for the same abandonment race on every other path:
            # if the row is gone, any failure string would resurrect it.
            try:
                _row_gone = (
                    getattr(self, "store", None) is not None
                    and hasattr(self.store, "get")
                    and self.store.get(ts.source_infohash, include_blob=False) is None
                )
            except Exception:
                _row_gone = False
            if _row_gone:
                log.info("worker: row gone for %s; dropping error (%s)", ts.source_infohash[:10], e)
                return
            log.exception("worker failed for %s", ts.source_infohash[:10])
            if ts.state == State.DONE:
                # DONE is terminal: a late exception (e.g. after transition)
                # must never corrupt the row with a failure string.
                log.warning(
                    "late failure after DONE for %s (%s); keeping DONE",
                    ts.source_infohash[:10],
                    e,
                )
            elif ts.state != State.FAILED:
                try:
                    self.transition(ts, State.FAILED, error=str(e)[:500])
                except ValueError as ve:
                    log.warning(
                        "could not transition %s (%s) to FAILED: %s",
                        ts.source_infohash[:10],
                        ts.state.value,
                        ve,
                    )
                    ts.last_error = str(e)[:500]
                    self.store.upsert(ts)
            try:
                self.store.append_log("ERROR", str(e), ts.source_infohash)
            except Exception:
                pass
            await self._notify_telegram(ts)

    async def _notify_telegram(self, ts: TorrentState, progress: float | None = None) -> None:
        """Push a state-update to the per-torrent Telegram message."""
        tg = getattr(self, "_tg", None)
        if tg is None:
            return
        try:
            if progress is None:
                progress = self.live_progress_map().get((ts.source_infohash or "").lower())
            await tg.ensure_detail_message(ts, progress=progress)
        except Exception as e:  # noqa: BLE001
            log.warning("telegram notify failed for %s: %s", ts.source_infohash[:10], e)

    def _abandoned(self, ts: TorrentState) -> bool:
        """True when the DB row was deleted after the tick snapshot.

        Forget/cancel removes the row while a worker may still hold the
        object; any later transition/upsert would resurrect it as a zombie
        (store.upsert is INSERT ... ON CONFLICT). Confirmed-gone only: a
        store error fails OPEN so a transient DB blip never abandons live
        work.
        """
        try:
            store = getattr(self, "store", None)
            if store is None or not hasattr(store, "get"):
                return False
            return store.get(ts.source_infohash, include_blob=False) is None
        except Exception:
            return False

    def _clear_force_direct(self, ts: TorrentState, dst: State) -> None:
        """Drop the sticky "skip Prowlarr" flag once a row is terminal (D8).

        `force_direct` (set by Telegram `/start_` or a prowlarr-timeout
        fallback) must not outlive the attempt it was meant for: a row
        revived by the auto-retry path would otherwise keep bypassing
        Prowlarr forever. The operator can re-issue `/start_` if the direct
        source is still wanted.
        """
        try:
            if not getattr(ts, "force_direct", 0):
                return
            ts.force_direct = 0
            _up = getattr(self.store, "update_columns", None)
            if callable(_up):
                _nv = _up(ts.source_infohash, {"force_direct": 0}, expected_state=dst)
                if isinstance(_nv, int) and _nv > 0:
                    ts.version = _nv
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not clear force_direct for %s: %s", (ts.source_infohash or "")[:10], e
            )

    def transition(
        self, ts: TorrentState, dst: State, *, error: str = "", batch_index: int | None = None
    ) -> None:
        """Wrap store.transition + log + queue a Telegram update.

        Local log file gets one concise line per transition; Telegram
        gets a per-torrent detail message edited in place — but only
        for state changes that the operator actually cares about. We
        skip the chatty passive states (e.g. NEW when we just inserted
        a row from the source poll) so a fresh install with hundreds
        of pre-existing racing torrents doesn't spam the channel.
        """
        if self._abandoned(ts):
            # Forget/cancel deleted this row mid-flight: transitioning
            # would upsert-resurrect it. _process_torrent unwinds quietly
            # on AbandonedError.
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                f"refusing {ts.state.value} -> {dst.value}"
            )
        prev = ts.state
        self.store.transition(ts, dst, error=error, batch_index=batch_index)
        if dst in (State.DONE, State.FAILED):
            self._drop_frozen_batch_cap(ts)
            self._clear_force_direct(ts, dst)
        if dst != State.WAITING_DISK:
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop((ts.source_infohash or "").lower(), None)
            except Exception:
                pass
        else:
            # Fresh parks start their quiet-wait window immediately so the
            # next tick doesn't thundering-herd every parked row at once.
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if _wd is None:
                    _wd = {}
                    self._waiting_disk_next_check = _wd  # type: ignore[attr-defined]
                if isinstance(_wd, dict):
                    _interval = getattr(self, "WAITING_DISK_RECHECK_SECONDS", 60.0)
                    try:
                        _interval = float(_interval)
                    except (TypeError, ValueError):
                        _interval = 60.0
                    _wd[(ts.source_infohash or "").lower()] = time.monotonic() + _interval
                    if len(_wd) > 5000:
                        # Bound quiet-wait hints (forgotten rows bypass
                        # transition pops; the prune reaps them, this caps).
                        for _k in list(_wd.keys())[: len(_wd) - 5000]:
                            _wd.pop(_k, None)
            except Exception:
                pass
        # SSD ledger: reservation held only while the row can occupy SSD
        # (QUEUED/DOWNLOADING/MOVING). Leaving for park/terminal/fuse states
        # frees the budget for waiting torrents. Sync pop (no lock needed —
        # idempotent release, races resolved by locked try_reserve/adjust).
        if dst in (State.WAITING_DISK, State.RE_ADDING, State.DONE, State.FAILED):
            try:
                _rsv = getattr(self, "_ssd_reserved", None)
                if isinstance(_rsv, dict):
                    _rsv.pop((ts.source_infohash or "").lower(), None)
                elif _rsv is not None:
                    log.warning(
                        "transition: SSD reservation map is %s, not a dict; "
                        "reservation for %s may leak",
                        type(_rsv).__name__,
                        (ts.source_infohash or "")[:10],
                    )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "transition: SSD reservation release failed for %s (budget stays held): %s",
                    (ts.source_infohash or "")[:10],
                    e,
                )
        # Download admission slot: leaving QUEUED frees it (admission is
        # recorded at the QUEUED edge; every exit — DOWNLOADING, FAILED,
        # WAITING_DISK — must release, or parked rows starve).
        if prev == State.QUEUED and dst != State.QUEUED:
            try:
                _adm = getattr(self, "_download_admissions", None)
                if isinstance(_adm, set):
                    _adm.discard((ts.source_infohash or "").lower())
                elif _adm is not None:
                    log.warning(
                        "transition: download-admission set is %s, not a set; "
                        "slot for %s may stay pinned",
                        type(_adm).__name__,
                        (ts.source_infohash or "")[:10],
                    )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "transition: download-admission release failed for %s (slot stays pinned): %s",
                    (ts.source_infohash or "")[:10],
                    e,
                )
        # MOVING stall counter: leaving MOVING resets consecutive parks
        # (a later re-entry starts fresh). transition() also clears the
        # parked last_error via error="".
        if prev == State.MOVING and dst != State.MOVING:
            try:
                _mp = getattr(self, "_moving_parks", None)
                if isinstance(_mp, dict):
                    _mp.pop((ts.source_infohash or "").lower(), None)
            except Exception:
                pass
            # Same lifecycle for the move-progress join maps (row tree +
            # transferred watermark): a later re-entry starts fresh.
            for _attr in ("_moving_tree", "_moving_progress_bytes"):
                try:
                    _m = getattr(self, _attr, None)
                    if isinstance(_m, dict):
                        _m.pop((ts.source_infohash or "").lower(), None)
                except Exception:
                    pass
        # TaskHandle kind follows the row (lane accounting reads kinds):
        # a worker spawned for NEW that reaches QUEUED counts as queued.
        try:
            from .task import handles_for

            try:
                _dk = str(getattr(dst, "value", "") or "").lower()
            except Exception:
                _dk = ""
            if _dk:
                for _hd in handles_for(self, ts.source_infohash or ""):
                    try:
                        _hd.kind = _dk
                    except Exception:
                        pass
        except Exception:
            pass
        # Force-start is lane-scoped: leaving the forceable states
        # (QUEUED/DOWNLOADING/MOVING) disarms it (in-memory only, so a
        # restart can never wedge a bypass).
        if dst not in (State.QUEUED, State.DOWNLOADING, State.MOVING):
            try:
                _f = getattr(self, "_forced", None)
                if isinstance(_f, set):
                    _f.discard((ts.source_infohash or "").lower())
            except Exception:
                pass
        log.info(
            "%s %s -> %s (batch %s/%s)",
            ts.source_name[:60],
            prev.value,
            dst.value,
            ts.batch_index,
            ts.batches_total,
        )
        if _should_notify_telegram(prev, dst):
            self._schedule_telegram_update(ts)

    def _schedule_telegram_update(self, ts: TorrentState) -> None:
        tg = getattr(self, "_tg", None)
        if tg is None or self._stop:
            return
        try:
            # Snapshot state + progress now: the queued task may run after
            # later transitions mutated this object (chain NEW→…→DONE in one
            # worker), which would duplicate final-state edits and drop
            # intermediates.
            snap = copy.copy(ts)
            progress = self.live_progress_map().get((ts.source_infohash or "").lower())
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._notify_telegram(snap, progress))
            # Track fire-and-forget notifies so shutdown can await them
            # instead of leaking "Task was destroyed" warnings.
            pending = getattr(self, "_tg_tasks", None)
            if pending is None:
                pending = set()
                self._tg_tasks = pending  # type: ignore[attr-defined]
            pending.add(task)
            task.add_done_callback(pending.discard)
        except RuntimeError:
            # No running loop (e.g. during shutdown). Skip.
            pass

    async def _process_torrent_inner(self, ts: TorrentState) -> None:
        # Operator hold (/hold_): the row keeps its state but gets no
        # work until resumed. Belt-and-braces alongside the scheduler
        # guards (direct spawns must also honor the hold).
        if getattr(ts, "skipped", 0):
            log.debug("worker skip: %s is held (skipped)", ts.source_name)
            return
        # Quiet-wait: parked WAITING_DISK re-checks are routine while batch
        # moves drain — debug, not info, to avoid filling the log disk.
        if ts.state == State.WAITING_DISK:
            log.debug("worker start: %s state=%s", ts.source_name, ts.state.value)
        else:
            log.info("worker start: %s state=%s", ts.source_name, ts.state.value)
        try:
            _entry_state = ts.state
        except Exception:
            _entry_state = None
        if ts.state == State.NEW:
            await self._run_state_step(ts, self._do_new, ts)
        if ts.state == State.WAITING_INDEXER and _entry_state in (State.WAITING_INDEXER,):
            # Refactor: timer-elapsed WAITING_INDEXER rows work directly.
            # The retired QUERYING hop is gone (see state.py migration);
            # the scheduler wakes due rows and the worker picks them up
            # without an intermediate transition. Entry-state guard
            # prevents same-tick fallthrough (NEW that just parked must
            # wait out its timer).
            await self._run_state_step(ts, self._do_waiting_indexer, ts)
        if ts.state == State.WAITING_DISK and _entry_state == State.WAITING_DISK:
            # Entry-state guard like WAITING_INDEXER: a row that parked
            # itself in this pass (NEW -> WAITING_DISK) must wait out its
            # quiet timer instead of re-checking the ledger immediately.
            await self._run_state_step(ts, self._wait_disk_then_queue, ts)
        if ts.state == State.QUEUED:
            # download_sem guards QUEUED admission only (_do_queued is
            # short: add + prioritize). The multi-hour _do_downloading
            # below runs outside the semaphore so slots are not pinned.
            # _do_queued releases the slot itself around fuse re-injects
            # (minutes of non-download RPCs) via _holds_download_slot.
            async with self.download_sem:
                await self._run_state_step(ts, self._do_queued, ts, _holds_download_slot=True)
            if ts.state == State.DOWNLOADING:
                await self._run_state_step(ts, self._do_downloading, ts)
        elif ts.state == State.DOWNLOADING:
            await self._run_state_step(ts, self._do_downloading, ts)
        if ts.state == State.MOVING:
            try:
                await self._run_state_step(ts, self._do_moving, ts)
            except RcloneTimeoutError as e:
                # Hung remote (flood-wait pileup, stalled uplink): bytes are
                # intact on SSD, slots freed — park for next tick, never fail.
                self._park_moving(ts, f"rclone move timed out: {e}")
                return
        if ts.state == State.RE_ADDING:
            await self._run_state_step(ts, self._do_re_add, ts)

    async def _run_state_step(self, ts: TorrentState, fn, *args, **kwargs) -> None:
        """Run one state handler under the per-state dwell budget.

        The budget is captured from the row's state at entry, and every
        handler call is wrapped separately so a QUEUED pass that promotes
        itself to DOWNLOADING is not charged the queue budget while it
        waits hours for bytes. On expiry the handler is cancelled — the
        same unwinding path shutdown/forget already use — so its worker
        slot and any semaphore are released and the next tick retries with
        a fresh snapshot. A row that keeps overrunning fails loudly for
        inspection (see `_note_state_budget_hit`) instead of idling until
        the 24h max-age backstop. Re-raises a handler-raised TimeoutError
        (file-drift/stall re-plan) unchanged: that is a park, not a wedge.
        """
        try:
            state = ts.state
        except Exception:
            state = None
        budget = _STATE_WORK_BUDGET_S.get(state)
        if budget is None:
            await fn(*args, **kwargs)
            return
        deadline = time.monotonic() + budget
        try:
            async with asyncio.timeout_at(deadline):
                await fn(*args, **kwargs)
        except TimeoutError:
            if time.monotonic() < deadline:
                raise
            self._note_state_budget_hit(ts, state, budget)
            return
        try:
            key = (ts.source_infohash or "").lower()
            if self._state_budget_hits.pop(key, None) is None:
                return
        except Exception:
            return
        # A later pass cleared the wedge: drop the advisory text stamped by
        # _note_state_budget_hit so the row does not keep advertising it.
        try:
            if str(getattr(ts, "last_error", "") or "").startswith("worker exceeded its"):
                new_rev = self.store.update_columns(
                    ts.source_infohash, {"last_error": ""}, expected_state=state
                )
                if new_rev:
                    ts.version = new_rev
                    ts.last_error = ""
        except Exception:
            pass

    def _note_state_budget_hit(self, ts: TorrentState, state: State, budget: float) -> None:
        """Log/persist one over-budget pass; fail the row after N in a row."""
        key = (ts.source_infohash or "").lower()
        try:
            hits_map = self._state_budget_hits
        except AttributeError:
            hits_map = {}
            self._state_budget_hits = hits_map  # type: ignore[attr-defined]
        try:
            prev = hits_map.get(key)
        except Exception:
            prev = None
        hits = prev[1] + 1 if isinstance(prev, tuple) and prev and prev[0] == state else 1
        try:
            if len(hits_map) > 5000:
                for _k in list(hits_map.keys())[: len(hits_map) - 5000]:
                    hits_map.pop(_k, None)
            hits_map[key] = (state, hits)
        except Exception:
            pass
        msg = (
            f"worker exceeded its {int(budget)}s budget in {state.value} "
            f"({hits}/{_MAX_STATE_BUDGET_HITS})"
        )
        name = (ts.source_name or "")[:60]
        if hits >= _MAX_STATE_BUDGET_HITS:
            try:
                hits_map.pop(key, None)
            except Exception:
                pass
            err = f"{msg} — wedged condition, needs inspection"
            log.error("%s for %s", msg, name)
            try:
                self.transition(ts, State.FAILED, error=err[:500])
            except AbandonedError:
                # The row moved/was forgotten while we were wedged: nothing
                # to fail, and upserting would resurrect a deleted row.
                log.info("worker budget: %s no longer current; not failing", key[:10])
            except Exception as e:  # noqa: BLE001
                log.warning("worker budget: could not fail %s: %s", key[:10], e)
            return
        log.warning("%s for %s; releasing the slot and retrying next tick", msg, name)
        try:
            new_rev = self.store.update_columns(
                ts.source_infohash,
                {"last_error": f"{msg}; retrying next tick"},
                expected_state=state,
            )
            if new_rev:
                ts.version = new_rev
        except Exception:
            pass

    # ---- state: NEW ----

    # (batch caps + SSD ledger live in coordinator_ssd.SSDLedgerMixin)

    def revive_failed_row(
        self,
        *,
        infohash: str,
        name: str = "",
        size: int = 0,
        announce: str = "",
        blob: bytes | None = None,
        source_label: str = "",
    ) -> TorrentState:
        """Re-queue a FAILED row from freshly provided .torrent bytes.

        Operator intent (Telegram /add, watch-dir re-drop) explicitly
        re-runs the pipeline: FAILED (any cause, including
        tracker-deleted) → NEW with the error, retry counters, timers,
        batch cursors and force-direct flag cleared, and metadata/blob
        refreshed from the new bytes. Anything else raises LookupError
        (caller keeps its existing reply/behavior). Cancelled
        (ignored) hashes are never revived — unignore first. The row's
        stale dest entry (if any survived the failure cleanup) is left
        alone: QUEUED setup re-adds or adopts it normally.
        """
        norm = (infohash or "").strip().lower()
        if len(norm) != 40 or not all(c in "0123456789abcdef" for c in norm):
            raise LookupError("need a full 40-char infohash to revive")
        store = getattr(self, "store", None)
        if store is None or not hasattr(store, "get"):
            raise LookupError("state store unavailable")
        try:
            if callable(getattr(store, "is_ignored", None)) and store.is_ignored(norm) is True:
                raise LookupError(
                    "Ignored (previously cancelled): unignore it first, then add again"
                )
        except LookupError:
            raise
        except Exception:
            pass
        try:
            ts = store.get(norm)
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"cannot look up {norm[:10]}: {e}") from e
        if ts is None:
            raise LookupError("no tracked torrent — ingest it as new")
        try:
            state = getattr(ts, "state", None)
        except Exception:
            state = None
        if state != State.FAILED:
            try:
                label = state.value
            except Exception:
                label = "tracked"
            raise LookupError(f"Already tracked: {(ts.source_name or norm[:10])[:50]} ({label})")
        if name:
            ts.source_name = name
        if size:
            try:
                ts.total_bytes = max(0, int(size))
            except (TypeError, ValueError):
                pass
        if announce:
            ts.source_announce_url = announce
            ts.source_tracker = announce
        if blob:
            try:
                ts.cross_seed_blob = bytes(blob)
            except Exception:
                pass
            try:
                ts._blob = bytes(blob)
            except Exception:
                pass
        if source_label:
            try:
                ts.cross_seed_source = source_label
            except Exception:
                pass
        ts.last_error = ""
        ts.failed_retries = 0
        # A stale dest pointer must not be adopted: the new run re-adds
        # or adopts the live entry itself (a leftover hash here would
        # attach the row to a stranger's entry).
        ts.dest_infohash = ""
        ts.indexer_first_queried_at = None
        ts.indexer_next_retry_at = None
        ts.indexer_attempts = 0
        ts.public_export_first_attempted_at = None
        ts.public_export_next_retry_at = None
        ts.public_export_attempts = 0
        ts.readd_first_attempted_at = None
        ts.readd_next_retry_at = None
        ts.readd_attempts = 0
        ts.readd_cycles = 0
        ts.force_direct = 0
        ts.fuse_wait_first_at = None
        ts.batch_index = 0
        ts.batches_total = 0
        self.transition(ts, State.NEW)
        log.info(
            "revived failed row %s (%s) -> NEW via operator re-add",
            (ts.source_name or norm[:10])[:60],
            norm[:10],
        )
        return ts
