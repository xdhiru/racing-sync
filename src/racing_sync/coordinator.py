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

from .classifier import classify  # noqa: F401  (runtime use via coordinator_admit lazy lookup + compat)
from .clients.abstract import Torrent, TorrentClient
from .clients.deluge import DelugeClient
from .clients.http_base import AuthError
from .clients.qbittorrent import QBittorrentClient
from .config import AppConfig
from .coordinator_admit import AdmitMixin
from .coordinator_cleanup import CleanupMixin
from .coordinator_content import (
    _TELEGRAM_NOTIFY_STATES as _TELEGRAM_NOTIFY_STATES,  # noqa: F401  (re-exported API)
)
from .coordinator_content import (
    PUBLIC_TRACKER_HOSTS as PUBLIC_TRACKER_HOSTS,  # noqa: F401  (re-exported API)
)
from .coordinator_content import (
    SourceDecision,
    _looks_public,
    _matches_release,
    _should_notify_telegram,
    _verified_cross_seed_blob,
    announce_domain,
    cleanup_grace_seconds,
    normalize_content_name,
)
from .coordinator_errors import (
    _NOT_VISIBLE_DETAIL,
    AbandonedError,
    BatchMoveIncompleteError,
    DownloadStalledError,
    RcloneTransientError,
    TRACKER_UNREGISTERED_MARKER,
    TorrentCheckingError,
    UnregisteredTorrentError,
    WebUIUnresponsiveError,
    is_fatal_os_error,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
)
from .coordinator_download import DownloadMixin, LiveItem
from .coordinator_fuse import FuseMixin
from .coordinator_inject import InjectMixin, _group_is_ignored
from .coordinator_live import LiveStatusMixin
from .coordinator_move import MoveMixin
from .coordinator_picker import pick_ssd_source_for_racing
from .coordinator_readd import ReaddMixin
from .coordinator_ssd import SSDLedgerMixin
from .coordinator_watchdog import WatchdogMixin
from .content_keys import same_content as _same_content_key
from .content_keys import same_release_name as _same_release_name
from .io_bounds import offload, rpc
from .prowlarr import ProwlarrClient
from .rclone_ops import (
    RcloneTimeoutError,
    move_local_to_remote,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
    ssd_has_room,  # noqa: F401  (runtime use via coordinator_admit lazy lookup + compat)
    ssd_max_inflight_bytes,  # noqa: F401  (runtime use via coordinator_ssd lazy lookup + compat)
    wipe_local_tree,  # noqa: F401  (runtime use via coordinator_move lazy lookup + compat)
)
from .sftp_source import SFTPExporter
from .state import State, StateStore, TorrentState
from .watchdir import WatchDirScanner, WatchItem

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


def _sched_priority(ts) -> tuple[int, dt.datetime]:
    """Worker-scheduling order key (lower first, then oldest first).

    all_active() ordering is DB-dependent; without an explicit priority,
    bulk NEW rows fill every worker slot each tick and starve the bounded
    pipeline states behind them. Compares real timestamps: rows with no
    (or unparsable) updated_at sort LAST, never first — broken rows must
    not permanently prioritize themselves.
    """
    _FAR = dt.datetime.max.replace(tzinfo=dt.timezone.utc)
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
                updated = updated.replace(tzinfo=dt.timezone.utc)
            return (prio, updated)
    except Exception:
        pass
    return (prio, _FAR)


def _elect_primary_torrent(group: list) -> object:
    """Elect ONE primary torrent for SSD download, deterministically.

    Client list order is arbitrary across restarts, so the group is
    sorted by (size, name, hash) first — the same election the picker
    uses — then the first public copy wins, else the first private.
    A nondeterministic primary flips source_infohash across restarts
    and can SSD-pick a different copy than the row tracks.
    """
    ordered = sorted(
        group,
        key=lambda t: ((getattr(t, "size_bytes", 0) or 0),
                       (getattr(t, "name", "") or ""),
                       (getattr(t, "infohash", "") or "").lower()),
    )
    return next((t for t in ordered if _looks_public(t.trackers)),
                ordered[0])


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #


# Boot-step retry budget: a 30s WebUI/SSH blip during deploy must not
# kill the daemon into a supervisor restart storm. Auth errors fail fast
# (bad credentials never heal, and hammering risks lockout); everything
# else backs off and the boot continues.
_STARTUP_ATTEMPTS = 5
_STARTUP_BACKOFF_S = (2.0, 4.0, 8.0, 16.0)

# Stale source-cache expiry: a wedged VPS1 serves its last-known list for
# a while (degraded but useful), then the view expires to empty so intake
# stops grouping ghosts and upserting display names for a dead source.
_SOURCE_STALE_EXPIRY_S = 300.0

# Worker-stop join budget: cancel() cannot interrupt a to_thread body.
_STOP_WORKERS_TIMEOUT_S = 30.0


async def _start_with_backoff(label: str, factory, *,
                              attempts: int = _STARTUP_ATTEMPTS,
                              delays: tuple[float, ...] = _STARTUP_BACKOFF_S):
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
            log.warning("startup: %s failed (%s); retrying in %.0fs (%d/%d)",
                        label, e, delay, attempt, tries)
            await asyncio.sleep(max(0.0, delay))
    assert last is not None
    raise last


def _log_api_task_done(task: "asyncio.Task") -> None:
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


#: Sentinel from _fetch_source_or_fail: the VPS1 torrent is gone (third
#: consecutive miss) but the row was parked — not failed — because a
#: Prowlarr cross-seed may still supply the SSD bytes. Callers build a
#: pseudo-source from persisted row metadata and pick normally. Truthy
#: and distinct from None; compare by identity, never as a Torrent.
_SOURCE_GONE = object()


@dataclass
class Coordinator(SSDLedgerMixin, CleanupMixin, WatchdogMixin, LiveStatusMixin,
                    InjectMixin, ReaddMixin, FuseMixin, DownloadMixin, MoveMixin,
                    AdmitMixin):
    cfg: AppConfig
    store: StateStore = field(init=False)
    source_client: TorrentClient = field(init=False)
    dest_client: TorrentClient = field(init=False)
    prowlarr: ProwlarrClient | None = field(init=False, default=None)
    sftp: SFTPExporter | None = field(init=False, default=None)
    watch: WatchDirScanner | None = field(init=False, default=None)
    _stop: bool = field(default=False, init=False)
    # Operator intake pause (/pause_intake): no NEW work starts until
    # resumed; in-flight rows drain normally. In-memory on purpose — a
    # restart resumes normal intake, so nothing can wedge paused.
    _intake_paused: bool = field(default=False, init=False)
    _live: dict[str, LiveItem] = field(default_factory=dict, init=False)
    _tasks: set[asyncio.Task] = field(default_factory=set, init=False)
    _running_infohashes: set[str] = field(default_factory=set, init=False)
    _download_sem: asyncio.Semaphore | None = field(default=None, init=False)
    _move_sem: asyncio.Semaphore | None = field(default=None, init=False)
    _coordinator_started: bool = field(default=False, init=False)
    _shutdown_done: bool = field(default=False, init=False)
    _source_torrents_cache: list[Torrent] = field(default_factory=list, init=False)
    _source_torrents_cached_at: float = field(default=0.0, init=False)
    _failed_late_cross_seeds: dict[str, dt.datetime] = field(default_factory=dict, init=False)
    # Proven-healthy original fuse entries (entry-hash -> next re-probe
    # monotonic): skips the per-tick export + stats for healthy DONE rows.
    _original_healthy_at: dict[str, float] = field(default_factory=dict, init=False)
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

    @property
    def download_sem(self) -> asyncio.Semaphore:
        if self._download_sem is None:
            self._download_sem = asyncio.Semaphore(self.cfg.max_active_downloads)
        return self._download_sem

    @property
    def move_sem(self) -> asyncio.Semaphore:
        if self._move_sem is None:
            self._move_sem = asyncio.Semaphore(self.cfg.max_concurrent_moves)
        return self._move_sem

    # ---- lifecycle ----

    def __post_init__(self) -> None:
        self.store = StateStore(self.cfg.general.state_db)

    def _warn_if_storage_paths_overlap(self) -> None:
        """Warn when SSD dirs overlap fuse mounts (config footgun).

        on_fuse detection, move sources, and wipe guards all assume disjoint
        trees. A nested layout (e.g. SSD scratch inside the fuse mount path)
        silently misclassifies every torrent. Advisory only — never fatal.
        """
        try:
            ssd_dirs = [str(self.cfg.dest.save_path), str(self.cfg.ssd.path)]
            fuse = [str(self.cfg.rclone.fuse.mount), str(self.cfg.rclone.fuse.mount_unsorted)]
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
                        s, fm,
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

            log_dir = _Path(str(self.cfg.general.log_dir))
            data_paths = [
                _Path(str(self.cfg.general.state_db)).parent,
                _Path(str(self.cfg.ssd.path)),
                _Path(str(self.cfg.dest.save_path)),
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
                            log_dir, dp,
                        )
                        break
                except OSError:
                    continue
        except Exception:
            return

    async def start(self) -> None:
        log.info("coordinator starting")
        self._boot_mono = time.monotonic()
        # Build marker: proves which ordering-guarantee build a log file ran.
        # Bump when the fresh-DB / late-seed ordering rules change.
        log.info("build ordering-guard v3 active (move-before-inject, DONE->MOVING demotion)")
        self._warn_if_storage_paths_overlap()
        self._warn_if_log_dir_on_data_mount()
        self._download_sem = asyncio.Semaphore(self.cfg.max_active_downloads)
        self._move_sem = asyncio.Semaphore(self.cfg.max_concurrent_moves)
        if self._ssd_lock is None:
            # Eager-init so concurrent admissions cannot race lazy creation
            # and install two different locks (split-brain budget).
            self._ssd_lock = asyncio.Lock()
        log.info(
            "concurrency limits: max_active_downloads=%d, max_concurrent_moves=%d",
            self.cfg.max_active_downloads,
            self.cfg.max_concurrent_moves,
        )
        try:
            if self.cfg.source.type == "qbittorrent":
                self.source_client = QBittorrentClient(
                    self.cfg.source, label="source-qb"
                )
            else:
                self.source_client = DelugeClient(self.cfg.source)
            await self.source_client.start()

            self.dest_client = QBittorrentClient(
                self.cfg.dest, label="dest-qb"
            )
            await self.dest_client.start()

            if self.cfg.prowlarr.enabled:
                self.prowlarr = ProwlarrClient(self.cfg.prowlarr)
                await _start_with_backoff("prowlarr", self.prowlarr.start)

            if (self.cfg.source.type == "deluge"
                    and self.cfg.source.deluge_sftp
                    and self.cfg.source.deluge_sftp.enabled):
                self.sftp = SFTPExporter(self.cfg.source.deluge_sftp)
                await _start_with_backoff("sftp", self.sftp.connect)
                # Reuse the shared connection for Deluge .torrent fallback
                # instead of a fresh handshake per get_torrent_files call.
                wire = getattr(self.source_client, "set_sftp_exporter", None)
                if callable(wire):
                    try:
                        wire(self.sftp)
                    except Exception:
                        pass
            # NOTE: qBittorrent sources have no SFTP config (SourceConfig only
            # defines deluge_sftp) — SFTP fallback for qB goes through the
            # source client's export_torrent() endpoint instead. self.sftp
            # stays None (set in __init__) for qB sources.

            if self.cfg.watch_dir is not None:
                self.watch = WatchDirScanner(self.cfg.watch_dir, self.prowlarr)

            if self.cfg.recovery.run_on_startup:
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
                    self.cfg, dest=self.dest_client, store=self.store,
                    verify=False,
                )
                self._recovery_task = None

            # Auto-retry FAILED rows so a previous run's hard failures
            # (e.g. Deluge RPC unavailable) get another chance with the
            # new code. This is the common path after the user upgrades
            # and re-runs.
            if self.cfg.recovery.auto_retry_failed:
                try:
                    _all_rows = await offload(self.store.all)
                except Exception:
                    _all_rows = []
                failed_rows = [
                    ts for ts in (_all_rows or [])
                    if ts.state == State.FAILED
                ]
                max_retries = getattr(self.cfg.recovery, "max_failed_retries", 3)
                for ts in failed_rows:
                    if ts.failed_retries >= max_retries:
                        log.warning(
                            "skipping auto-retry for %s: reached max retries (%d/%d)",
                            ts.source_name, ts.failed_retries, max_retries,
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
                    try:
                        self.store.upsert(ts)
                    except AbandonedError:
                        log.info("auto-retry: row gone for %s; skipping",
                                 (ts.source_infohash or "")[:10])
                        continue
                    except Exception:
                        continue
                    try:
                        self.transition(ts, State.NEW)
                    except (AbandonedError, ValueError) as e:
                        log.info("auto-retry: cannot revive %s (%s); skipping",
                                 (ts.source_infohash or "")[:10], e)
                        continue

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
            if self.cfg.telegram.enabled:
                from .telegram_bot import TelegramBot
                self._tg = TelegramBot(self.cfg.telegram, self, self.store)
                await self._tg.start()

            # Optional FastAPI control plane
            from .api import serve
            self._api_task: asyncio.Task | None = None
            if self.cfg.api.enabled:
                self._api_task = asyncio.create_task(
                    serve(self), name="rs-api"
                )
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

    async def _list_source_torrents(self, *, force_refresh: bool = False) -> list:
        """Fetch racing torrents from VPS1 and apply the min-age filter.

        Single source of truth for "what is syncable from the racing
        client right now". Empty `category` in the config means
        "sync everything"; non-empty means filter by that category.
        `min_age_seconds` ensures torrents have matured for at least N
        seconds before sync starts (e.g. to allow cross-seeds to be added).

        Cached for 45 seconds and single-flighted: the split poller +
        scheduler loops (plus per-row workers) would otherwise each fire
        a full Deluge `get_torrents_status` scan every 15-30s, and a scan
        slower than the RPC timeout would pile overlapping scans onto
        deluged — starving autobrr's own injections. One scan serves all
        callers; concurrent callers share the in-flight scan instead of
        starting their own.
        """
        now_mono = time.monotonic()
        if not force_refresh and (now_mono - self._source_torrents_cached_at) < 45.0:
            return list(self._source_torrents_cache)

        lock = self._loop_lock("_source_list_lock")
        if lock is not None:
            async with lock:
                # Re-check under the lock: a concurrent caller may have
                # just refreshed while we queued.
                now_mono = time.monotonic()
                if not force_refresh and (now_mono - self._source_torrents_cached_at) < 45.0:
                    return list(self._source_torrents_cache)
                return await self._list_source_torrents_locked(
                    force_refresh=force_refresh, now_mono=now_mono)
        return await self._list_source_torrents_locked(
            force_refresh=force_refresh, now_mono=now_mono)

    async def _list_source_torrents_locked(
        self, *, force_refresh: bool = False, now_mono: float = 0.0,
    ) -> list:
        """Source list body: caller holds `_source_list_lock` (or none)."""
        now_mono = now_mono or time.monotonic()

        try:
            all_torrents = await rpc(
                self.source_client.list_torrents(
                    category=self.cfg.source.category
                ),
                getattr(self, "cfg", None), "source list_torrents",
            )
        except AuthError:
            # Bad credentials never heal by retrying, and hammering risks
            # lockout: surface to the poller/scheduler loops' dedicated
            # AuthError arms (loud error + backoff) instead of serving an
            # infinite stale cache. Worker callers park via the
            # _process_torrent AuthError arm — rows must never fail over
            # an ops incident.
            raise
        except (TimeoutError, asyncio.TimeoutError, OSError,
                ValueError, RuntimeError) as e:
            # Wedged/unreachable VPS1 poll: serve the stale cache when
            # there is one, else an empty view — a source outage must
            # degrade the poll, never fail rows or abort the tick from
            # any caller. But a dead source must expire: past
            # _SOURCE_STALE_EXPIRY_S without a success, serve empty so
            # intake stops grouping ghosts and upserting display names.
            _stale = getattr(self, "_source_torrents_cache", None) or []
            try:
                _stale_age = now_mono - float(
                    getattr(self, "_source_last_success_mono", 0.0) or 0.0)
            except (TypeError, ValueError):
                _stale_age = 0.0
            if _stale and _stale_age > _SOURCE_STALE_EXPIRY_S:
                log.error(
                    "source poll failed (%s) for %ds; expiring stale "
                    "view (%d torrent(s)): %s",
                    type(e).__name__, int(_stale_age), len(_stale), e)
                return []
            log.warning("source poll failed (%s); using %s view: %s",
                        type(e).__name__,
                        "stale cache" if _stale else "empty", e)
            return list(_stale)
        min_age = self.cfg.source.min_age_seconds
        if min_age <= 0:
            filtered = all_torrents
        else:
            time_now = time.time()
            filtered = []
            for t in all_torrents:
                if not getattr(t, "added_on", 0):
                    # Unknown age admits nothing: an autobrr burst with a
                    # missing timestamp must wait for a real age, not skip
                    # the maturity gate.
                    continue
                if (time_now - t.added_on) < min_age:
                    continue
                filtered.append(t)

        self._source_torrents_cache = filtered
        self._source_torrents_cached_at = now_mono
        self._source_last_success_mono = now_mono
        return list(filtered)

    def request_stop(self) -> None:
        log.warning("stop requested")
        self._stop = True

    def set_intake_paused(self, paused: bool) -> bool:
        """Pause/resume intake of NEW work; returns the new state.

        Pause stops workers from starting on NEW rows (poller intake
        handoff and scheduler); rows already in flight drain normally
        and discovery keeps recording arrivals for the resume.
        """
        self._intake_paused = bool(paused)
        log.warning("intake %s by operator",
                    "paused" if self._intake_paused else "resumed")
        return self._intake_paused

    async def shutdown(self) -> None:
        # Idempotent: __main__ and run() both call us, so guard against
        # the second invocation producing duplicate log lines.
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
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        for t in list(getattr(self, "_tg_tasks", set())):
            t.cancel()
        if getattr(self, "_tg_tasks", None):
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._tg_tasks), return_exceptions=True),
                    timeout=10.0,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        tg = getattr(self, "_tg", None)
        if tg is not None:
            try:
                await asyncio.wait_for(tg.stop(), timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
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
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
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
            except (asyncio.TimeoutError, asyncio.CancelledError):
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
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass
            try:
                await asyncio.wait_for(self.dest_client.close(), timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
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
            log.error("API server died (%s); stopping instead of "
                      "running headless", _api_err)
            return True
        except Exception:
            return False

    async def run(self) -> int:
        """Supervise the poller, scheduler and janitor loops.

        Three independent loops with separate locks and clocks (a wedged
        source poll, wedged scheduler, or slow janitor degrades only its
        own lane). Each loop guards its own iterations and never raises;
        the supervisor just waits for stop and cancels them on shutdown.
        `_tick()` remains the single-pass equivalent for tests.
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
            factories = (self._poller_loop, self._scheduler_loop,
                         self._janitor_loop)
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
                                log.error("loop task died; respawning: %s", e,
                                          exc_info=True)
                            if factory is not None and not self._stop:
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
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception:
                pass
            await self.shutdown()
        return 0

    async def scan_watch(self) -> list[WatchItem]:
        """Scan watch directory for releases, ingest into state store, and return items."""
        if self.watch is None:
            return []
        items = await self.watch.scan_once()
        for item in items:
            try:
                item_hash = (item.infohash or "").lower()
            except Exception:
                continue
            if not item_hash:
                continue
            ingested = False
            # Sync store I/O rides a worker thread: a WAL stall must not
            # block the poller loop behind the GIL.
            _existing = await asyncio.to_thread(
                self.store.get, item_hash, include_blob=False)
            if _existing is None:
                try:
                    _ignored = await asyncio.to_thread(
                        self.store.is_ignored, item_hash) is True
                except Exception:
                    _ignored = False
                if _ignored:
                    # A cancelled drop re-appearing: respect the ignore list
                    # instead of re-ingesting. The file is left in place
                    # (never destroy what we didn't consume); unignore to
                    # reprocess it.
                    log.info("watch-dir: ignoring cancelled drop %s (%s)",
                             item.name[:60], item.infohash[:10])
                    continue
                ts = TorrentState(
                    source_infohash=item_hash,
                    source_name=item.name,
                    total_bytes=item.size_bytes,
                    source_announce_url=item.announce_url,
                    source_tracker=item.announce_url,
                    cross_seed_blob=item.torrent_bytes,
                    cross_seed_source="watch-dir",
                    state=State.NEW,
                )
                ts._blob = item.torrent_bytes
                await asyncio.to_thread(self.store.upsert, ts)
                ingested = True
                log.info(
                    "discovered watch-dir release: %s (%s, %d bytes) announce=%s",
                    item.name,
                    item.infohash[:10],
                    item.size_bytes,
                    # Domain only: announce URLs embed per-user passkeys.
                    announce_domain(item.announce_url),
                )
            elif _existing.state == State.FAILED:
                # A re-dropped file for a failed release is the operator
                # retrying it by hand: revive into a fresh pipeline run
                # (cancelled/ignored hashes stay ignored — unignore first).
                try:
                    await asyncio.to_thread(
                        self.revive_failed_row,
                        infohash=item_hash, name=item.name,
                        size=item.size_bytes,
                        announce=item.announce_url or "",
                        blob=item.torrent_bytes,
                        source_label="watch-dir")
                except LookupError as e:
                    log.info("watch-dir: re-drop of failed %s not revived: %s",
                             item.name[:60], e)
                    continue
                except Exception as e:  # noqa: BLE001
                    log.warning("watch-dir: revive failed for %s: %s",
                                item.name[:60], e)
                    continue
                ingested = True
                log.info(
                    "watch-dir: re-queued failed release %s (%s)",
                    item.name[:60], item.infohash[:10],
                )
            elif getattr(_existing, "skipped", 0):
                # A re-dropped file for a held row is the operator
                # resuming it by hand (same as /add on a skipped row):
                # clear the hold so the normal flow (fuse check, VPS1 /
                # VPS2 checks, Prowlarr cross-seed search) runs again.
                # Fail-open: a write failure just leaves it held for an
                # explicit /unskip.
                try:
                    _resumed = bool(await asyncio.to_thread(
                        self.store.update_columns, item_hash, {"skipped": 0}))
                except Exception:
                    _resumed = False
                if _resumed:
                    log.info(
                        "watch-dir: re-drop resumes held release %s (%s)",
                        item.name[:60], item.infohash[:10],
                    )
                    ingested = True
            # Delete only what this scan ingested: an already-tracked drop
            # still belongs to the user — never destroy what we didn't
            # consume on this run. Exception: a re-dropped duplicate whose
            # row is DONE is fully handled (seeding from fuse), so with
            # delete_after_pickup it is clutter — remove it like a fresh
            # ingest. Any other state keeps the file (the first drop's
            # pipeline may still need it, or the operator may inspect it).
            if self.cfg.watch_dir and self.cfg.watch_dir.delete_after_pickup:
                if ingested:
                    await self.watch.delete_picked_up(item)
                else:
                    # Reuse the loop-top snapshot: nothing in this tick
                    # mutated a non-ingested item, so a second get is a
                    # wasted round-trip (a concurrent DONE transition is
                    # picked up on the next scan).
                    existing = _existing
                    if existing is not None and existing.state == State.DONE:
                        log.info(
                            "watch-dir: %s (%s) already done; removing duplicate drop",
                            item.name[:60], item.infohash[:10],
                        )
                        await self.watch.delete_picked_up(item)
        return items

    def _spawn_worker(self, ts: TorrentState) -> None:
        """Launch a _process_torrent worker with in-flight bookkeeping.

        No-op if the row already has a live worker (both call sites
        pre-check too; the re-check here closes the gap after awaits).
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
        task.add_done_callback(_done_cb)

    async def stop_workers_for(self, infohash: str) -> bool:
        """Cancel live workers for one hash; True when any were stopped.

        Used by forget-without-remember (never-seen semantics): the row
        is hard-deleted right after, so a stale in-flight upsert must
        not resurrect it as a zombie. Cancellation is clean — the worker
        wrapper unwinds through AbandonedError/quiet paths and its
        finally drops the live map. Best-effort and idempotent; unknown
        hashes (and bare test doubles) report False.

        Bounded by _STOP_WORKERS_TIMEOUT_S: cancel() cannot interrupt a
        worker wedged in a to_thread body (fuse/SFTP syscalls), so the
        join must not hang the forget/API call behind a leaked thread.
        """
        try:
            h = (infohash or "").strip().lower()
            if not h:
                return False
            try:
                _wt = getattr(self, "_worker_tasks", None)
                _tasks = list(_wt.get(h, ())) if isinstance(_wt, dict) else []
            except Exception:
                return False
            _live = [t for t in _tasks if not t.done()]
            if not _live:
                return False
            for t in _live:
                try:
                    t.cancel()
                except Exception:
                    pass
            try:
                await asyncio.wait_for(
                    asyncio.gather(*_live, return_exceptions=True),
                    timeout=_STOP_WORKERS_TIMEOUT_S)
            except (TimeoutError, asyncio.TimeoutError):
                log.warning(
                    "stop_workers_for %s timed out; %d worker(s) linger",
                    h[:10], len(_live))
            except Exception:
                pass
            return True
        except Exception:
            return False

    async def _tick(self) -> None:
        """One iteration: poll sources, schedule work (serialized vs API ops)."""
        lock = getattr(self, "_ops_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._ops_lock = lock
        async with lock:
            await self._tick_inner()

    async def _poll_source_racing(self, src_torrents: list[Torrent]) -> list[TorrentState]:
        """Step 2: group source torrents by release and ingest/track each group.

        Returns the NEW rows created by this poll (intake events for the
        immediate handoff in _poll_source_step; the scheduler remains the
        backstop). One poisoned group (corrupt row, failing lookup, doomed
        injection) must neither skip the remaining groups nor abort the
        rest of the tick (indexer wakeups, workers, janitor): every group
        is isolated, failures are logged and skipped.
        """
        # Group source torrents by content/release name. Multiple racing
        # torrents for the same content (e.g. public release + multiple
        # private cross-seeds) only produce ONE active SSD download.
        by_name: dict[str, list[Torrent]] = {}
        for st in src_torrents:
            norm_key = normalize_content_name(st.name)
            if not norm_key:
                # Nameless entries must not collapse into a single "" group
                # (would elect one primary and drop the rest). Track solo.
                norm_key = f"__infohash__:{st.infohash.lower()}"
            by_name.setdefault(norm_key, []).append(st)

        fresh: list[TorrentState] = []
        for group in by_name.values():
            try:
                created = await self._process_source_group(group)
                if created is not None:
                    fresh.append(created)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                try:
                    _gname = group[0].name[:60] if group and group[0].name else "?"
                except Exception:
                    _gname = "?"
                log.warning("source group %s failed; continuing with next group: %s",
                            _gname, e)
        return fresh

    async def _process_source_group(self, group: list[Torrent]) -> TorrentState | None:
        """Ingest/track one release group from the source poll (step-2 body).

        Returns the NEW row when this poll created one (intake event),
        else None (ignored / already tracked).
        """
        # Cancelled releases stay cancelled while listed on VPS1.
        if _group_is_ignored(group, getattr(self, "store", None)):
            log.info(
                "ignoring cancelled release: %s (%d duplicate(s))",
                group[0].name[:60], len(group),
            )
            return None
        # Check if any torrent in this release group is already tracked in state store
        # (state.get normalizes case internally — one lookup suffices).
        # One batched round-trip (off the loop) instead of a blocking
        # get per member; first group member present wins, as before.
        existing_ts: TorrentState | None = None
        try:
            _hashes = [(_t.infohash or "").lower() for _t in group
                       if (_t.infohash or "").strip()]
            _by_hash = await asyncio.to_thread(
                self.store.get_many, _hashes) if _hashes else {}
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed dedup lookup must not ingest a possible duplicate:
            # skip the group this tick (the caller isolates groups); the
            # next poll retries.
            return None
        for t in group:
            found_ts = (_by_hash or {}).get((t.infohash or "").lower())
            if found_ts is not None:
                existing_ts = found_ts
                break
        if existing_ts is None:
            # Group members usually share one release name: dedupe it so
            # the fuzzy LIKE runs once per distinct name, off the loop.
            _seen_names: set[str] = set()
            for t in group:
                _nm = getattr(t, "name", "") or ""
                if _nm in _seen_names:
                    continue
                _seen_names.add(_nm)
                # Off the loop but still fatal to the group: a failed
                # dedup lookup must not ingest a possible duplicate
                # (the caller isolates groups; the next poll retries).
                matches = await asyncio.to_thread(
                    self.store.find_by_name, _nm)
                if matches:
                    # find_by_name is a fuzzy LIKE: require the same
                    # normalized release AND (when both known) the same
                    # size, or repacks/different seasons with common
                    # prefixes ("Show.S01" vs "Show.S01E02") would be
                    # swallowed as duplicates and never downloaded.
                    same = [
                        m for m in matches
                        if _same_content_key(
                            m.source_name or "", m.total_bytes or 0,
                            t.name, t.size_bytes or 0,
                        )
                    ]
                    if same:
                        existing_ts = same[0]
                        break

        if existing_ts is not None:
            # Content is already being managed by an existing TorrentState;
            # keep display name fresh (persisted so Telegram/DB don't show stale names).
            if group and group[0].name and group[0].name != existing_ts.source_name:
                existing_ts.source_name = group[0].name
                try:
                    await asyncio.to_thread(self.store.upsert, existing_ts)
                except Exception:  # noqa: BLE001
                    pass
            if existing_ts.state == State.DONE and self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                if not getattr(existing_ts, "skipped", 0):
                    await self._check_and_inject_late_cross_seeds(existing_ts, group)
            return None

        # Elect ONE primary torrent for SSD download:
        # 1. Prefer public torrent if available (req #1)
        # 2. Otherwise pick first private torrent to query the download-target indexers (req #2)
        primary = _elect_primary_torrent(group)
        is_pub = _looks_public(primary.trackers)

        ts = TorrentState(
            source_infohash=primary.infohash.lower(),
            source_name=primary.name,
            total_bytes=primary.size_bytes,
            source_announce_url=primary.trackers[0] if primary.trackers else "",
            state=State.NEW,
        )
        await asyncio.to_thread(self.store.upsert, ts)
        log.info(
            "discovered racing release: %s (%s) [elected %s primary from %d duplicate(s)]",
            primary.name,
            primary.infohash[:10],
            "public" if is_pub else "private",
            len(group),
        )
        # Intake-velocity signal for the VPS1 cleanup janitor: one
        # timestamp per newly discovered racing release (spam bursts
        # shorten deletion grace). Hard-capped so a disabled janitor
        # can't grow it without bound; the janitor prunes hourly.
        try:
            arrivals = getattr(self, "_arrival_times", None)
            if arrivals is None:
                arrivals = []
                self._arrival_times = arrivals
            arrivals.append(time.monotonic())
            if len(arrivals) > 5000:
                del arrivals[:2500]
        except Exception:
            pass
        return ts

    async def _poll_source_step(self) -> None:
        """Source-poller pass: watch scan, source ingest, fuse sweep.

        Runs on the poller loop under `_ops_lock` (serialized vs API
        ops), and inside `_tick_inner` for single-pass callers. Every
        sub-step is isolated: a poisoned drop must not skip the source
        poll, and a wedged source must not wedge scheduling — the other
        loops run on their own locks and clocks.
        """
        log.debug("poller: enter")
        # 1. Watch dir (req #3). Isolated like every tick step: a poisoned
        # drop must not skip the source poll, workers or janitor below.
        watch_items: list = []
        try:
            watch_items = await self.scan_watch() or []
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir scan failed; continuing tick: %s", e)

        # 2. Source racing client (req #1 / #2)
        src_torrents = await self._list_source_torrents()
        # Throttled: log source size once every 5 minutes so we can
        # see if the racing client is being polled correctly without
        # spamming the log every poll cycle.
        if not hasattr(self, "_last_source_log_ts"):
            self._last_source_log_ts = 0.0
        now = time.monotonic()
        if now - self._last_source_log_ts > 300:
            log.info(
                "source poll: %d torrent(s) matching category=%r min_age=%ds",
                len(src_torrents), self.cfg.source.category,
                self.cfg.source.min_age_seconds,
            )
            self._last_source_log_ts = now
        fresh_racing = await self._poll_source_racing(src_torrents)

        # 2b. Manual fuse sweep: same infohash already seeding from fuse on
        # VPS2 (any category) with verified bytes needs no prowlarr/SSD work.
        # Batch-adopted here so WAITING_INDEXER rows parked on their 30m retry
        # timer are picked up within one poll interval, not one retry window.
        # Never breaks the tick: all failures are caught inside.
        try:
            await self._sweep_manual_fuse_adoptions()
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse sweep failed: %s", e)

        # 2c. Intake handoff: freshly discovered NEW rows start work now
        # instead of waiting out the next scheduler tick (racing is
        # time-sensitive). Production only (the scheduler remains the
        # backstop for tests and single-pass callers).
        try:
            await self._drain_intake(fresh_racing, watch_items)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("intake handoff failed; scheduler covers it: %s", e)

    def _intake_started(self) -> bool:
        """True once the coordinator is serving (production run loop).

        Bare test doubles built via object.__new__ lack the flag and
        read False — the intake handoff stays off there and the
        scheduler backstop covers them.
        """
        try:
            return bool(getattr(self, "_coordinator_started", False))
        except Exception:
            return False

    async def _drain_intake(self, fresh_racing: list, watch_items: list) -> int:
        """Spawn workers for rows ingested by this poller pass.

        Returns the number spawned. Fresh racing rows arrive as NEW
        TorrentStates; watch drops arrive as scanner items and are
        resolved to their NEW rows (revived/resumed rows included).
        Each spawn is isolated; capacity mirrors the scheduler burst
        cap so an autobrr flood can't spawn unbounded workers.
        Double-scheduling with the scheduler loop is closed by the
        running-set + worker re-get guards.
        """
        if not self._intake_started() or getattr(self, "_stop", False):
            return 0
        if getattr(self, "_intake_paused", False):
            return 0
        try:
            candidates: list[TorrentState] = [t for t in (fresh_racing or []) if t is not None]
        except Exception:
            candidates = []
        try:
            _store = getattr(self, "store", None)
            _get = getattr(_store, "get", None) if _store is not None else None
            for item in watch_items or []:
                try:
                    _h = getattr(item, "infohash", "") or ""
                    if not _h or not callable(_get):
                        continue
                    _row = _get(_h, include_blob=False)
                    if _row is not None:
                        candidates.append(_row)
                except Exception:
                    continue
        except Exception:
            pass
        if not candidates:
            return 0
        try:
            _max_workers = max(
                12,
                self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError, AttributeError):
            _max_workers = 12
        try:
            _tasks = getattr(self, "_tasks", None)
            _inflight = max(0, _max_workers - len(_tasks)) if isinstance(_tasks, set) else _max_workers
        except Exception:
            _inflight = _max_workers
        spawned = 0
        for ts in candidates:
            if _max_workers and spawned >= max(0, _inflight):
                break
            try:
                _state = getattr(ts, "state", None)
                if _state != State.NEW:
                    continue
                if getattr(ts, "skipped", 0):
                    continue
                _key = (getattr(ts, "source_infohash", "") or "").lower()
                try:
                    _running = getattr(self, "_running_infohashes", None)
                    if isinstance(_running, set) and _key and _key in _running:
                        continue
                except Exception:
                    pass
                self._spawn_worker(ts)
                spawned += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                try:
                    _h = (getattr(ts, "source_infohash", "") or "")[:10]
                except Exception:
                    _h = "?"
                log.warning("intake handoff for %s failed; scheduler covers it: %s", _h, e)
                continue
        if spawned:
            log.info("intake handoff: started %d worker(s) for fresh arrivals", spawned)
        return spawned

    async def _schedule_step(self) -> None:
        """Scheduler pass: indexer wakeups, worker scheduling, live refresh.

        Runs on the scheduler loop under `_schedule_lock`, and inside
        `_tick_inner` for single-pass callers. Snapshots are re-validated
        at worker start (re-get guards), so interleaving with the poller
        loop or API ops cannot double-schedule or clobber rows.
        """
        log.debug("scheduler: enter")
        # 3. Wake up WAITING_INDEXER rows whose retry timer has elapsed —
        # both the download-target indexer schedule and the independent
        # public-export schedule. Indexer wakeups still respect worker
        # capacity: an unbounded timer burst must not spawn unbounded
        # workers.
        try:
            _max_workers = max(
                12,
                self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
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
                log.warning("indexer wakeup for %s failed; continuing: %s",
                            (getattr(ts, "source_infohash", "") or "")[:10], e)
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
                self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError, AttributeError):
            max_concurrent_workers = 12

        active_downloads = sum(
            1 for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state in (State.QUEUED, State.DOWNLOADING)
        )
        active_moves = sum(
            1 for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state == State.MOVING
        )
        # Per-phase worker budgets: a handful of multi-hour DOWNLOADING /
        # MOVING workers must not starve every other state via the global
        # slot pool. Downloads/moves gate on their live counts above;
        # intake, checks and re-adds share a bounded misc budget; the
        # global worker count stays only as a backstop.
        live_misc = sum(
            1 for t in active
            if (t.source_infohash or "").lower() in self._running_infohashes
            and t.state not in (State.QUEUED, State.DOWNLOADING, State.MOVING)
        )

        for ts in active:
            if len(self._tasks) >= max_concurrent_workers:
                break
            _tkey = (ts.source_infohash or "").lower()
            if _tkey in self._running_infohashes:
                continue

            # Operator hold (/skip_): no workers until resumed — no
            # Prowlarr queries, no downloads, no moves. State is kept.
            if getattr(ts, "skipped", 0):
                continue

            # Operator intake pause (/pause_intake): NEW rows wait until
            # resumed; every other state keeps draining normally.
            if getattr(ts, "state", None) == State.NEW and getattr(
                    self, "_intake_paused", False):
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
                now_utc = dt.datetime.now(dt.timezone.utc)
                if ts.readd_next_retry_at > now_utc:
                    continue

            # Limit concurrent active qBittorrent additions / downloads on SSD.
            # Both QUEUED and workerless DOWNLOADING rows consume a slot:
            # spawning over cap just parks inside _try_admit after burning
            # a worker plus a dest list_torrents storm.
            if ts.state in (State.QUEUED, State.DOWNLOADING) and active_downloads >= self.cfg.max_active_downloads:
                continue

            # Limit concurrent active rclone move commands
            if ts.state == State.MOVING and active_moves >= self.cfg.max_concurrent_moves:
                continue

            # All other states (NEW intake, WAITING_DISK checks, RE_ADDING,
            # DONE/FAILED drains) share the misc budget: unbounded intake
            # bursts must not flood the loop with Prowlarr picks either.
            if (ts.state not in (State.QUEUED, State.DOWNLOADING, State.MOVING)
                    and live_misc >= _MISC_WORKER_BUDGET):
                continue

            try:
                self._spawn_worker(ts)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("scheduling worker for %s failed; continuing: %s",
                            _tkey[:10], e)
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
        if scheduled:
            # Quiet-wait: a tick that only re-checked parked WAITING_DISK
            # rows is routine while batches drain — debug, not info, so a
            # full disk doesn't fill the log disk with its own status lines.
            _log = log.debug if scheduled == scheduled_waiting_disk else log.info
            _log(
                "scheduled %d worker(s) (active downloads=%d/%d, moves=%d/%d)",
                scheduled,
                active_downloads, self.cfg.max_active_downloads,
                active_moves, self.cfg.max_concurrent_moves,
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

        Compatibility path for single-pass callers (tests, manual ticks):
        the production `run()` loop instead runs the three steps on
        independent loops with separate locks and clocks (see
        `_poller_loop` / `_scheduler_loop` / `_janitor_loop`).
        Each step is isolated: one step throwing must not starve the
        others (a source outage skips scheduling/janitor otherwise).
        """
        for _step in (self._poll_source_step,
                      self._schedule_step,
                      self._janitor_step):
            try:
                await _step()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("tick step %s failed; continuing: %s",
                            getattr(_step, "__name__", "?"), e)

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

    async def _poller_loop(self) -> None:
        """Source-poller loop: watch + source ingest + fuse sweep."""
        while not self._stop:
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
                log.error("poller: auth still failing; backing off: %s", e)
            except TimeoutError as e:
                log.warning("poller: timed out; backing off: %s", e)
            except Exception as e:  # noqa: BLE001
                log.error("poller: unexpected error; backing off: %s",
                          e, exc_info=True)
            if self._stop:
                break
            try:
                await asyncio.sleep(
                    max(1, int(float(self.cfg.general.source_poll_interval or 30))))
            except (TypeError, ValueError):
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def _scheduler_loop(self) -> None:
        """Worker-scheduler loop: wakeups, admissions, live refresh."""
        while not self._stop:
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
                log.error("scheduler: auth still failing; backing off: %s", e)
            except TimeoutError as e:
                log.warning("scheduler: timed out; backing off: %s", e)
            except Exception as e:  # noqa: BLE001
                log.error("scheduler: unexpected error; backing off: %s",
                          e, exc_info=True)
            if self._stop:
                break
            try:
                await asyncio.sleep(
                    max(1, int(float(self.cfg.general.dest_poll_interval or 30))))
            except (TypeError, ValueError):
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break

    async def _janitor_loop(self) -> None:
        """Janitor loop: VPS1 cleanup + tombstone GC (self-gated cadence)."""
        # The step self-gates on janitor_interval_seconds, so the loop
        # wakes often enough to stay responsive without busy work.
        while not self._stop:
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
                log.error("janitor: unexpected error; backing off: %s",
                          e, exc_info=True)
            if self._stop:
                break
            try:
                await asyncio.sleep(
                    max(300, int(getattr(
                        getattr(self.cfg, "cleanup", None),
                        "janitor_interval_seconds", 3600) or 3600)))
            except (TypeError, ValueError):
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break

    # ---- VPS1 cleanup janitor ([cleanup]) ----

    # (VPS1 cleanup janitor lives in coordinator_cleanup.CleanupMixin)

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
                        ts.source_infohash[:10], ts.state.value, _fresh.state.value,
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
                        ts.source_infohash[:10], _snap_v, _fresh_v,
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
            log.info("worker: %s abandoned (%s); stopping",
                     ts.source_infohash[:10], e)
            return
        except (RcloneTimeoutError, RcloneTransientError) as e:
            # Last-resort net: a timed-out or transiently-failed move from
            # any path parks the row (source bytes intact) instead of
            # failing it. The two expected sites handle this directly with
            # better context; this only fires for future call paths.
            log.error("rclone move issue for %s: %s (parking, not failing)",
                      ts.source_infohash[:10], e)
            try:
                if ts.state == State.MOVING:
                    self._park_moving(ts, f"rclone move timed out: {e}")
                else:
                    try:
                        _gone = (
                            getattr(self, "store", None) is not None
                            and hasattr(self.store, "get")
                            and self.store.get(ts.source_infohash,
                                               include_blob=False) is None
                        )
                    except Exception:
                        _gone = False
                    if _gone:
                        log.info("worker: row gone for %s; dropping timeout (%s)",
                                 ts.source_infohash[:10], e)
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
            log.error("worker: auth failure for %s (%s); leaving row for next tick",
                      ts.source_infohash[:10], e)
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
                log.info("worker: row gone for %s; dropping error (%s)",
                         ts.source_infohash[:10], e)
                return
            log.exception("worker failed for %s", ts.source_infohash[:10])
            if ts.state == State.DONE:
                # DONE is terminal: a late exception (e.g. after transition)
                # must never corrupt the row with a failure string.
                log.warning(
                    "late failure after DONE for %s (%s); keeping DONE",
                    ts.source_infohash[:10], e,
                )
            elif ts.state != State.FAILED:
                try:
                    self.transition(ts, State.FAILED, error=str(e)[:500])
                except ValueError as ve:
                    log.warning(
                        "could not transition %s (%s) to FAILED: %s",
                        ts.source_infohash[:10], ts.state.value, ve,
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
            log.warning("telegram notify failed for %s: %s",
                        ts.source_infohash[:10], e)

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

    async def find_release(
        self, query: str, *, per_indexer: int = 3, max_indexers: int = 10,
    ) -> dict:
        """Read-only Prowlarr search for the operator (/find command).

        Fans out over the download-target indexers and returns plain
        data (no downloads, no state changes, no exactness claims —
        titles alone answer "is it on my indexers"). Raises LookupError
        with a user-facing reason when nothing can be searched.
        """
        q = (query or "").strip()
        if not q:
            raise LookupError("Send /find <release name> to search.")
        prow = getattr(self, "prowlarr", None)
        if prow is None:
            raise LookupError("Prowlarr is not configured.")
        try:
            _skip = getattr(getattr(prow, "_cfg", None),
                            "should_skip_title", None)
            if callable(_skip) and _skip(q):
                raise LookupError(
                    f"{q!r} is skipped by config (skip_query_substrings).")
        except LookupError:
            raise
        except Exception:
            pass
        try:
            indexers = list(prow.get_download_indexers() or [])
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"indexer list unavailable: {e}") from e
        if not indexers:
            raise LookupError("No download-target indexers configured.")
        indexers = indexers[:max(1, max_indexers)]
        try:
            per = max(1, int(per_indexer or 3))
        except (TypeError, ValueError):
            per = 3
        try:
            hits = await prow.search_indexers_parallel(indexers, q)
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"search failed: {e}") from e
        out = []
        for idx in indexers:
            try:
                name = str(getattr(idx, "name", "") or "")
                hs = sorted(
                    hits.get(name.lower(), []) or [],
                    key=lambda h: -(getattr(h, "size_bytes", 0) or 0),
                )
                out.append({
                    "name": name,
                    "total": len(hs),
                    "hits": [
                        {"title": str(getattr(h, "title", "") or ""),
                         "size_bytes": int(getattr(h, "size_bytes", 0) or 0)}
                        for h in hs[:per]
                    ],
                })
            except Exception:
                continue
        return {"query": q, "indexers": out}

    def transition(self, ts: TorrentState, dst: State,
                   *, error: str = "", batch_index: int | None = None) -> None:
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
            except Exception:
                pass
        # Download admission slot: leaving QUEUED frees it (admission is
        # recorded at the QUEUED edge; every exit — DOWNLOADING, FAILED,
        # WAITING_DISK — must release, or parked rows starve).
        if prev == State.QUEUED and dst != State.QUEUED:
            try:
                _adm = getattr(self, "_download_admissions", None)
                if isinstance(_adm, set):
                    _adm.discard((ts.source_infohash or "").lower())
            except Exception:
                pass
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
        log.info(
            "%s %s -> %s (batch %s/%s)",
            ts.source_name[:60],
            prev.value,
            dst.value,
            ts.batch_index, ts.batches_total,
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
        # Operator hold (/skip_): the row keeps its state but gets no
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
            await self._do_new(ts)
        if ts.state == State.WAITING_INDEXER and _entry_state in (
            State.WAITING_INDEXER,
        ):
            # Refactor: timer-elapsed WAITING_INDEXER rows work directly.
            # The retired QUERYING hop is gone (see state.py migration);
            # the scheduler wakes due rows and the worker picks them up
            # without an intermediate transition. Entry-state guard
            # prevents same-tick fallthrough (NEW that just parked must
            # wait out its timer).
            await self._do_waiting_indexer(ts)
        if ts.state == State.WAITING_DISK:
            await self._wait_disk_then_queue(ts)
        if ts.state == State.QUEUED:
            # download_sem guards QUEUED admission only (_do_queued is
            # short: add + prioritize). The multi-hour _do_downloading
            # below runs outside the semaphore so slots are not pinned.
            # _do_queued releases the slot itself around fuse re-injects
            # (minutes of non-download RPCs) via _holds_download_slot.
            async with self.download_sem:
                await self._do_queued(ts, _holds_download_slot=True)
            if ts.state == State.DOWNLOADING:
                await self._do_downloading(ts)
        elif ts.state == State.DOWNLOADING:
            await self._do_downloading(ts)
        if ts.state == State.MOVING:
            try:
                await self._do_moving(ts)
            except RcloneTimeoutError as e:
                # Hung remote (flood-wait pileup, stalled uplink): bytes are
                # intact on SSD, slots freed — park for next tick, never fail.
                self._park_moving(ts, f"rclone move timed out: {e}")
                return
        if ts.state == State.RE_ADDING:
            await self._do_re_add(ts)

    def _wakeup_indexer_row(self, ts: TorrentState) -> None:
        """Wake one timer-elapsed WAITING_INDEXER row (step-3 loop body).

        Spawns the worker directly: the worker's WAITING_INDEXER branch
        does the `_do_waiting_indexer` work with no intermediate
        transition (the retired QUERYING hop is gone).
        """
        _key = (ts.source_infohash or "").lower()
        if _key in self._running_infohashes:
            return
        # Re-read: a worker may have moved this row (e.g. QUEUED via the
        # SSD path) after the snapshot above — never demote it back.
        try:
            fresh = self.store.get(ts.source_infohash, include_blob=False)
        except Exception:
            fresh = None
        if fresh is None or fresh.state != State.WAITING_INDEXER:
            return
        if getattr(fresh, "skipped", 0):
            return
        ts = fresh
        try:
            _public_mode = bool(
                ts.public_export_next_retry_at
                and not ts.indexer_next_retry_at)
        except Exception:
            _public_mode = False
        log.info(
            "%s retry timer fired for %s (attempt #%d)",
            "public-export" if _public_mode else "download-indexer",
            ts.source_name[:40],
            ts.public_export_attempts if _public_mode else ts.indexer_attempts,
        )
        self._spawn_worker(ts)

    # ---- state: NEW ----

    # (batch caps + SSD ledger live in coordinator_ssd.SSDLedgerMixin)

    def _cached_source_torrent(self, infohash: str):
        """Source torrent from the poller snapshot (no RPC), else None.

        The poller refreshes `_source_torrents_cache` every tick, so a
        worker needing "fresh" VPS1 metadata usually finds ≤45s-old data
        right here. Using it avoids one client lookup per row per tick
        — on Deluge every `get_torrent` is a full `get_torrents_status`
        scan, and N rows × full scans every 30s is what wedges the VPS1
        WebUI for other programs. A cache miss falls back to RPC.

        Deluge list rows always carry trackers, so they are served
        unconditionally. qB list rows carry only the first tracker
        (`tracker` field); they are served when it is present, otherwise
        the live lookup runs (which fills the full tracker set via a
        separate RPC). Single-tracker racing torrents — the common
        autobrr case — then need zero per-row RPCs while the snapshot
        is fresh; multi-tracker rows keep today's behavior.
        """
        try:
            want = (infohash or "").lower()
            if not want:
                return None
            cache = getattr(self, "_source_torrents_cache", None)
            if not cache:
                return None
            # Memoized infohash index: the list object is replaced on
            # every refresh, so id() keys the index's lifetime. Per-tick
            # callers (every worker, every tick) share one O(N) build
            # instead of each paying an O(N) scan.
            try:
                _memo = getattr(self, "_source_cache_index", None)
                if (not isinstance(_memo, tuple) or len(_memo) != 2
                        or _memo[0] != id(cache)):
                    _idx: dict[str, object] = {}
                    for _st in cache:
                        try:
                            _ih = (getattr(_st, "infohash", "") or "").lower()
                        except Exception:
                            continue
                        if _ih and _ih not in _idx:
                            _idx[_ih] = _st
                    self._source_cache_index = (id(cache), _idx)
                    _memo = self._source_cache_index
                st = _memo[1].get(want)
            except Exception:
                st = None
            if st is None:
                return None
            _is_deluge = isinstance(
                getattr(self, "source_client", None), DelugeClient)
            try:
                if _is_deluge:
                    return st
                try:
                    _trs = getattr(st, "trackers", None) or []
                except Exception:
                    _trs = []
                if any(bool(u and str(u).strip()) for u in _trs):
                    return st
                return None
            except Exception:
                return None
        except Exception:
            pass
        return None

    def _prowlarr_query_possible(self, ts: TorrentState) -> bool:
        """True when a Prowlarr cross-seed search can run for this row."""
        try:
            if getattr(self, "prowlarr", None) is None:
                return False
            _cs = getattr(getattr(self, "cfg", None), "cross_seed", None)
            if not bool(getattr(_cs, "allow_prowlarr_cross_seed", False)):
                return False
            _pq = getattr(getattr(self, "cfg", None), "prowlarr", None)
            _skip = getattr(_pq, "should_skip_title", None)
            if callable(_skip) and _skip(ts.source_name or ""):
                return False
            return True
        except Exception:
            return False

    def _pseudo_source_for_vanished(self, ts: TorrentState):
        """Stand-in source built from persisted row metadata.

        The VPS1 copy is gone, but name/size/trackers were persisted at
        ingest: enough for a Prowlarr cross-seed search (name+size) and
        for same-content sibling discovery on VPS1. None when even the
        name is unknown (nothing to search for).
        """
        try:
            name = (ts.source_name or "").strip()
            if not name:
                return None
            try:
                total = max(0, int(ts.total_bytes or 0))
            except (TypeError, ValueError):
                total = 0
            trackers: list[str] = []
            try:
                for _u in (ts.source_announce_url, ts.source_tracker):
                    if _u and _u not in trackers:
                        trackers.append(_u)
            except Exception:
                pass
            return Torrent(
                hash=(ts.source_infohash or "").lower(),
                name=name,
                category="",
                save_path="",
                size_bytes=total,
                state="vanished",
                progress=0.0,
                trackers=trackers,
            )
        except Exception:
            return None

    def revive_failed_row(self, *, infohash: str, name: str = "",
                            size: int = 0, announce: str = "",
                            blob: bytes | None = None,
                            source_label: str = "") -> TorrentState:
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
            if callable(getattr(store, "is_ignored", None)) \
                    and store.is_ignored(norm) is True:
                raise LookupError(
                    "Ignored (previously cancelled): unignore it first, "
                    "then add again")
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
            raise LookupError(
                f"Already tracked: {(ts.source_name or norm[:10])[:50]} "
                f"({label})")
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
        ts.batch_index = 0
        ts.batches_total = 0
        self.transition(ts, State.NEW)
        log.info("revived failed row %s (%s) -> NEW via operator re-add",
                 (ts.source_name or norm[:10])[:60], norm[:10])
        return ts

    async def _fetch_source_or_fail(self, ts: TorrentState):
        """Fresh VPS1 metadata, a vanish sentinel, or None (parked/failed).

        Prefers the poller snapshot (no RPC); falls back to a live
        lookup that also refreshes the snapshot entry. A single poll
        miss (client restart, registration lag) must not fail the row:
        only the third consecutive miss is terminal. Misses are tracked
        in-memory per infohash (a restart resets the count, which is
        safe — worst case one extra grace window).

        Third miss (VPS1 copy really gone — operator cleared it for
        space, tracker pruned it): fail ONLY when no other SSD source
        can exist. Otherwise the row parks for a Prowlarr cross-seed
        built from its persisted metadata and this returns the
        _SOURCE_GONE sentinel — the VPS2 pipeline continues without
        VPS1. Callers must handle the sentinel (pseudo-source pick),
        never treat it as a torrent.
        """
        try:
            st = self._cached_source_torrent(ts.source_infohash)
            if st is not None:
                try:
                    _misses = getattr(self, "_source_miss_counts", None)
                    if isinstance(_misses, dict):
                        _misses.pop((ts.source_infohash or "").lower(), None)
                except Exception:
                    pass
                return st
        except Exception:
            pass
        try:
            st = await rpc(
                self.source_client.get_torrent(ts.source_infohash),
                getattr(self, "cfg", None), "source get_torrent",
            )
        except (TimeoutError, asyncio.TimeoutError) as e:
            # Hung VPS1 lookup degrades to a quiet retry next tick —
            # never a terminal FAILED over one wedged poll.
            log.warning("source lookup timed out for %s (%s); retry next tick",
                        (ts.source_infohash or "")[:10], e)
            return None
        if st is None:
            try:
                _misses = getattr(self, "_source_miss_counts", None)
                if not isinstance(_misses, dict):
                    _misses = {}
                    self._source_miss_counts = _misses  # type: ignore[attr-defined]
                _key = (ts.source_infohash or "").lower()
                _misses[_key] = int(_misses.get(_key, 0) or 0) + 1
                if len(_misses) > 5000:
                    for _k in list(_misses.keys())[: len(_misses) - 5000]:
                        _misses.pop(_k, None)
                _n = _misses[_key]
            except Exception:
                _n = 3
            if _n < 3:
                log.warning("source torrent %s unseen (miss %d/3); keeping row",
                            (ts.source_infohash or "")[:10], _n)
                return None
            try:
                _misses.pop(_key, None)
            except Exception:
                pass
            if self._prowlarr_query_possible(ts):
                # The VPS1 copy is gone but a Prowlarr cross-seed may
                # still supply the SSD bytes: park (24h max-age backstop
                # still applies) and let the caller pick from persisted
                # metadata instead of failing a recoverable row.
                log.warning("source torrent %s gone from VPS1; parking "
                            "for Prowlarr cross-seed",
                            (ts.source_infohash or "")[:10])
                self._park_for_indexer_retry(
                    ts, reason="source vanished from VPS1")
                if ts.state == State.FAILED:
                    # Max-age tripped inside the park: the row just failed.
                    # Report plain None (parked/failed) — the caller must
                    # not run a pseudo-source pick on a FAILED row.
                    return None
                return _SOURCE_GONE
            err = f"source torrent vanished from client: {ts.source_infohash[:10]}"
            log.warning(err)
            self.transition(ts, State.FAILED, error=err)
            return None
        try:
            _misses = getattr(self, "_source_miss_counts", None)
            if isinstance(_misses, dict):
                _misses.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass
        return st

    def _same_content_torrents(self, all_source: list, st) -> list:
        """Other racing-client torrents for the same content (req #1).

        qB/Deluge don't have a content-id, so heuristic: same name + same
        total size. We use name match — usually racing has 1-3 dupes.
        Delegates to content_keys.same_release_name (single rule).
        """
        return [
            t for t in all_source
            if t.infohash != st.infohash and (
                t.name == st.name or _same_release_name(t.name, st.name)
            )
        ]

    async def _operator_supplied_decision(self, ts: TorrentState):
        """SSD decision from operator-supplied same-infohash bytes, else None.

        Covers .torrent files handed over via Telegram /add for an
        already-tracked row whose own source is unavailable (e.g. a
        racing row whose VPS1 original vanished while it parks for a
        Prowlarr cross-seed that never comes). Same infohash means same
        content, so these bytes can never substitute a wrong release —
        they are only used when the normal pick yields nothing. Never
        raises: any doubt returns None and the caller parks as before.
        """
        try:
            want = (ts.source_infohash or "").strip().lower()
            if not want:
                return None
            try:
                total = int(ts.total_bytes or 0)
            except (TypeError, ValueError):
                total = 0
            if total <= 0:
                return None
            blob = getattr(ts, "_blob", None) or getattr(ts, "cross_seed_blob", None)
            if not blob and getattr(self, "store", None) is not None:
                try:
                    blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                except Exception:
                    blob = None
            if not blob:
                return None
            try:
                from .watchdir import _bencoded_info_hash
            except Exception:
                return None
            try:
                real_hash, _, _, _ = _bencoded_info_hash(bytes(blob))
            except Exception:
                return None
            if (real_hash or "").lower() != want:
                return None
            # Same non-preferred policy as the direct fallback: a private
            # non-download swarm may still be superseded by a preferred
            # copy (NEW grace hold), unless explicitly fetched.
            try:
                _urls = [u for u in (ts.source_announce_url or "").split(",") if u] or (
                    [ts.source_tracker] if ts.source_tracker else []
                )
            except Exception:
                _urls = []
            try:
                _preferred = _looks_public(_urls) or any(
                    self.cfg.prowlarr.is_download_indexer(u or "") for u in _urls
                )
            except Exception:
                _preferred = False
            log.info(
                "using operator-supplied .torrent for %s (own source unavailable)",
                (ts.source_name or "")[:60],
            )
            return SourceDecision(
                torrent_bytes=bytes(blob),
                source_label="operator-supplied",
                name=ts.source_name or "",
                size_bytes=total,
                infohash=want,
                announce_url=(_urls[0] if _urls else ""),
                preferred=bool(_preferred),
            )
        except Exception:
            return None
