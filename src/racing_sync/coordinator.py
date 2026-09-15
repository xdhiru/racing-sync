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
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path

from .batcher import Batch, files_from_names, make_batches, make_file_batches
from .classifier import classify, oversize_single_file
from .clients.abstract import Torrent, TorrentClient, TorrentFile
from .clients.deluge import DelugeClient
from .clients.http_base import AuthError
from .clients.qbittorrent import QBittorrentClient
from .config import AppConfig
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
    fold_path_case,
    is_watch_row,
    normalize_content_name,
    watch_election_winner,
    watch_rank,
)
from .coordinator_content import WATCH_ORIGIN_LABELS
from .coordinator_errors import (
    _NOT_VISIBLE_DETAIL,
    _WEBUI_RETRY_ERRORS,
    AbandonedError,
    BatchMoveIncompleteError,
    WebUIUnresponsiveError,
)
from .coordinator_paths import _safe_ssd_join, _watch_cross_seed_dir
from .coordinator_picker import pick_ssd_source_for_racing
from .coordinator_ssd import SSDLedgerMixin
from .prowlarr import ProwlarrClient, TorrentHit
from .rclone_ops import (
    RcloneTimeoutError,
    move_local_to_remote,
    ssd_has_room,
    ssd_max_inflight_bytes,  # noqa: F401  (runtime use via coordinator_ssd lazy lookup + compat)
    wipe_local_tree,
)
from .recovery import find_content_on_ssd, reconcile
from .sftp_source import SFTPExporter
from .state import _MAX_READD_CYCLES, State, StateStore, TorrentState
from .watchdir import WatchDirScanner, WatchItem

log = logging.getLogger(__name__)


__all__ = [
    "AbandonedError",
    "BatchMoveIncompleteError",
    "Coordinator",
    "LiveItem",
    "SourceDecision",
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
    State.QUERYING: 0,
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


def _sched_priority(ts) -> tuple[int, str]:
    """Worker-scheduling order key (lower first, then oldest first).

    all_active() ordering is DB-dependent; without an explicit priority,
    bulk NEW rows fill every worker slot each tick and starve the bounded
    pipeline states behind them.
    """
    try:
        prio = _SCHED_STATE_PRIORITY.get(ts.state, 1)
    except Exception:
        prio = 1
    try:
        updated = str(getattr(ts, "updated_at", "") or "")
    except Exception:
        updated = ""
    return (prio, updated)


def _left_on_disk(src_dir: Path, name: str) -> bool:
    """True iff `name` still occupies real SSD bytes (0-transfer detector).

    Goes through the traversal guard and treats symlinks/unreadables as
    leftovers: `Path.exists()` is False for broken symlinks and can lie on
    permission errors, and neither case means "reached the remote". Doubt
    fails closed (leftover → retry, never advance).
    """
    try:
        p = _safe_ssd_join(src_dir, name)
    except OSError:
        return True
    if p is None:
        return True
    try:
        if p.is_symlink():
            return True
        return p.exists()
    except OSError:
        return True


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #


def _group_is_ignored(group: list[Torrent], store) -> bool:
    """True when any member hash of a same-content group is cancelled.

    Group members are duplicate client entries for one release, so one
    cancelled hash vetoes the whole group (otherwise the next duplicate
    would resurrect it on the following tick). Requires an actual `True`
    so bare MagicMock stores never veto in unit tests.
    """
    try:
        is_ignored = getattr(store, "is_ignored", None)
        if not callable(is_ignored):
            return False
        for t in group or []:
            try:
                if t.infohash and is_ignored(t.infohash) is True:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


@dataclass
class LiveItem:
    source_infohash: str
    name: str
    state: str
    progress: float
    size_mb: float
    eta: str = ""


@dataclass
class Coordinator(SSDLedgerMixin, CleanupMixin):
    cfg: AppConfig
    store: StateStore = field(init=False)
    source_client: TorrentClient = field(init=False)
    dest_client: TorrentClient = field(init=False)
    prowlarr: ProwlarrClient | None = field(init=False, default=None)
    sftp: SFTPExporter | None = field(init=False, default=None)
    watch: WatchDirScanner | None = field(init=False, default=None)
    _stop: bool = field(default=False, init=False)
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
    # VPS1 cleanup janitor ([cleanup]): monotonic timestamp of the last run
    # plus one monotonic timestamp per newly discovered racing release
    # (rolling intake-velocity signal for spam-burst grace shortening).
    _cleanup_last_run: float = field(default=0.0, init=False)
    _arrival_times: list = field(default_factory=list, init=False)
    # Serializes the periodic tick against API-triggered ops (recover /
    # scan-watch / retry) so they can't double-schedule or clobber rows.
    _ops_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
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
                await self.prowlarr.start()

            if (self.cfg.source.type == "deluge"
                    and self.cfg.source.deluge_sftp
                    and self.cfg.source.deluge_sftp.enabled):
                self.sftp = SFTPExporter(self.cfg.source.deluge_sftp)
                self.sftp.connect()
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
                await reconcile(
                    self.cfg, dest=self.dest_client, store=self.store
                )

            # Auto-retry FAILED rows so a previous run's hard failures
            # (e.g. Deluge RPC unavailable) get another chance with the
            # new code. This is the common path after the user upgrades
            # and re-runs.
            if self.cfg.recovery.auto_retry_failed:
                failed_rows = [
                    ts for ts in self.store.all()
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
                    ts.failed_retries += 1
                    self.store.upsert(ts)
                    self.transition(ts, State.NEW)

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

            self._coordinator_started = True
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

        Cached for 10 seconds to prevent redundant RPC calls to VPS1 when
        multiple tasks (e.g. _tick, _do_new, _do_waiting_indexer) query
        the source client within the same cycle.
        """
        now_mono = time.monotonic()
        if not force_refresh and (now_mono - self._source_torrents_cached_at) < 10.0:
            return list(self._source_torrents_cache)

        all_torrents = await self.source_client.list_torrents(
            category=self.cfg.source.category
        )
        min_age = self.cfg.source.min_age_seconds
        if min_age <= 0:
            filtered = all_torrents
        else:
            time_now = time.time()
            filtered = []
            for t in all_torrents:
                if t.added_on and (time_now - t.added_on) < min_age:
                    continue
                filtered.append(t)

        self._source_torrents_cache = filtered
        self._source_torrents_cached_at = now_mono
        return list(filtered)

    def request_stop(self) -> None:
        log.warning("stop requested")
        self._stop = True

    async def shutdown(self) -> None:
        # Idempotent: __main__ and run() both call us, so guard against
        # the second invocation producing duplicate log lines.
        if self._shutdown_done:
            return
        self._shutdown_done = True
        log.info("coordinator shutting down")
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        for t in list(getattr(self, "_tg_tasks", set())):
            t.cancel()
        if getattr(self, "_tg_tasks", None):
            await asyncio.gather(*list(self._tg_tasks), return_exceptions=True)
        tg = getattr(self, "_tg", None)
        if tg is not None:
            await tg.stop()
        api_task = getattr(self, "_api_task", None)
        if api_task is not None:
            api_task.cancel()
            try:
                await api_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.prowlarr is not None:
            try:
                await self.prowlarr.close()
            except Exception:  # noqa: BLE001
                pass
        if self.sftp is not None:
            try:
                self.sftp.close()
            except Exception:  # noqa: BLE001
                pass
        if self._coordinator_started:
            try:
                await self.source_client.close()
            except Exception:  # noqa: BLE001
                pass
            try:
                await self.dest_client.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            self.store.close()
        except Exception:  # noqa: BLE001
            pass

    # ---- main loop ----

    async def run(self) -> int:
        await self.start()
        try:
            while not self._stop:
                try:
                    await self._tick()
                except AuthError as e:
                    # WebUI auth still failing after the retry loop
                    # in HTTPClientBase.request() — log loudly, back
                    # off, and let the next tick try again instead
                    # of crashing the whole coordinator.
                    log.error(
                        "auth still failing after retries; "
                        "backing off for one poll interval: %s", e,
                    )
                except TimeoutError as e:
                    log.warning(
                        "poll tick timed out (remote client connection dropped/slow); "
                        "backing off for one poll interval: %s", e,
                    )
                except Exception as e:  # noqa: BLE001
                    log.error(
                        "unexpected error in coordinator tick; "
                        "backing off for one poll interval: %s", e, exc_info=True,
                    )
                if self._stop:
                    break
                # Cancellable sleep so SIGINT/SIGTERM break out quickly.
                try:
                    await asyncio.sleep(
                        self.cfg.general.source_poll_interval
                    )
                except asyncio.CancelledError:
                    break
        finally:
            await self.shutdown()
        return 0

    async def scan_watch(self) -> list[WatchItem]:
        """Scan watch directory for releases, ingest into state store, and return items."""
        if self.watch is None:
            return []
        items = await self.watch.scan_once()
        for item in items:
            item_hash = item.infohash.lower()
            ingested = False
            if self.store.get(item_hash, include_blob=False) is None:
                try:
                    _ignored = self.store.is_ignored(item_hash) is True
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
                self.store.upsert(ts)
                ingested = True
                log.info(
                    "discovered watch-dir release: %s (%s, %d bytes) announce=%s",
                    item.name,
                    item.infohash[:10],
                    item.size_bytes,
                    # Domain only: announce URLs embed per-user passkeys.
                    announce_domain(item.announce_url),
                )
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
                    try:
                        existing = self.store.get(item_hash, include_blob=False)
                    except Exception:
                        existing = None
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
        task = asyncio.create_task(self._process_torrent(ts))
        self._tasks.add(task)
        def _done_cb(t: asyncio.Task, infohash: str = h) -> None:
            self._tasks.discard(t)
            self._running_infohashes.discard(infohash)
        task.add_done_callback(_done_cb)

    async def _tick(self) -> None:
        """One iteration: poll sources, schedule work (serialized vs API ops)."""
        lock = getattr(self, "_ops_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._ops_lock = lock
        async with lock:
            await self._tick_inner()

    async def _poll_source_racing(self, src_torrents: list[Torrent]) -> None:
        """Step 2: group source torrents by release and ingest/track each group.

        One poisoned group (corrupt row, failing lookup, doomed injection)
        must neither skip the remaining groups nor abort the rest of the
        tick (indexer wakeups, workers, janitor): every group is isolated,
        failures are logged and skipped.
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

        for group in by_name.values():
            try:
                await self._process_source_group(group)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                try:
                    _gname = group[0].name[:60] if group and group[0].name else "?"
                except Exception:
                    _gname = "?"
                log.warning("source group %s failed; continuing with next group: %s",
                            _gname, e)

    async def _process_source_group(self, group: list[Torrent]) -> None:
        """Ingest/track one release group from the source poll (step-2 body)."""
        # Cancelled releases stay cancelled while listed on VPS1.
        if _group_is_ignored(group, getattr(self, "store", None)):
            log.info(
                "ignoring cancelled release: %s (%d duplicate(s))",
                group[0].name[:60], len(group),
            )
            return
        # Check if any torrent in this release group is already tracked in state store
        existing_ts: TorrentState | None = None
        for t in group:
            found_ts = (self.store.get(t.infohash.lower(), include_blob=False)
                        or self.store.get(t.infohash, include_blob=False))
            if found_ts is not None:
                existing_ts = found_ts
                break
        if existing_ts is None:
            for t in group:
                matches = self.store.find_by_name(t.name)
                if matches:
                    # find_by_name is a fuzzy LIKE: require the same
                    # normalized release AND (when both known) the same
                    # size, or repacks/different seasons with common
                    # prefixes ("Show.S01" vs "Show.S01E02") would be
                    # swallowed as duplicates and never downloaded.
                    norm_t = normalize_content_name(t.name)
                    same = [
                        m for m in matches
                        if normalize_content_name(m.source_name or "") == norm_t
                        and (not m.total_bytes or not t.size_bytes
                             or m.total_bytes == t.size_bytes)
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
                    self.store.upsert(existing_ts)
                except Exception:  # noqa: BLE001
                    pass
            if existing_ts.state == State.DONE and self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                await self._check_and_inject_late_cross_seeds(existing_ts, group)
            return

        # Elect ONE primary torrent for SSD download:
        # 1. Prefer public torrent if available (req #1)
        # 2. Otherwise pick first private torrent to query the download-target indexers (req #2)
        primary = next((t for t in group if _looks_public(t.trackers)), group[0])
        is_pub = _looks_public(primary.trackers)

        ts = TorrentState(
            source_infohash=primary.infohash.lower(),
            source_name=primary.name,
            total_bytes=primary.size_bytes,
            source_announce_url=primary.trackers[0] if primary.trackers else "",
            state=State.NEW,
        )
        self.store.upsert(ts)
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

    async def _tick_inner(self) -> None:
        """One iteration: poll sources, schedule work."""
        log.debug("tick: enter")
        # 1. Watch dir (req #3). Isolated like every tick step: a poisoned
        # drop must not skip the source poll, workers or janitor below.
        try:
            await self.scan_watch()
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
        await self._poll_source_racing(src_torrents)

        # 2b. Manual fuse sweep: same infohash already seeding from fuse on
        # VPS2 (any category) with verified bytes needs no prowlarr/SSD work.
        # Batch-adopted here so WAITING_INDEXER rows parked on their 30m retry
        # timer are picked up within one poll interval, not one retry window.
        # Never breaks the tick: all failures are caught inside.
        try:
            await self._sweep_manual_fuse_adoptions()
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse sweep failed: %s", e)

        # 3. Wake up WAITING_INDEXER rows whose retry timer has elapsed.
        # Indexer wakeups still respect worker capacity: an unbounded timer
        # burst must not spawn unbounded workers.
        try:
            _max_workers = max(
                12,
                self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError):
            _max_workers = 0
        ready_indexer = self.store.list_indexer_ready()
        for ts in ready_indexer:
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
        active = sorted(
            self.store.all_active(),
            key=_sched_priority,
        )
        scheduled = 0
        scheduled_waiting_disk = 0
        max_concurrent_workers = max(
            12,
            self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
        )
        available_slots = max(0, max_concurrent_workers - len(self._tasks))

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

        for ts in active:
            if available_slots <= 0:
                break
            _tkey = (ts.source_infohash or "").lower()
            if _tkey in self._running_infohashes:
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

            # Limit concurrent active qBittorrent additions / downloads on SSD
            if ts.state == State.QUEUED and active_downloads >= self.cfg.max_active_downloads:
                continue

            # Limit concurrent active rclone move commands
            if ts.state == State.MOVING and active_moves >= self.cfg.max_concurrent_moves:
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

            scheduled += 1
            if ts.state == State.WAITING_DISK:
                scheduled_waiting_disk += 1
            available_slots -= 1
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

        # 5. VPS1 cleanup janitor (hourly no-op unless [cleanup].enabled).
        # Must never break the tick: all failures are caught and logged.
        try:
            await self._maybe_cleanup_source()
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup janitor failed: %s", e)

    async def _refresh_live_status(self) -> None:
        """Re-query VPS2 progress and update the live map."""
        if not self._live:
            return
        try:
            rows = await self.dest_client.list_torrents(hashes=list(self._live.keys()))
        except Exception as e:  # noqa: BLE001
            log.warning("list_torrents for live status failed: %s", e)
            return
        for t in rows:
            item = self._live.get(t.hash.lower())
            if item is None:
                continue
            item.progress = t.progress
            item.size_mb = t.size_bytes / (1024 * 1024)
            # No ETA: we don't track downspeed, and the old
            # (1-progress)*60 placeholder misled operators.

    def live_progress_map(self) -> dict[str, float]:
        """Snapshot of in-flight download progress keyed by infohash.

        Used by the Telegram bot for the active-tasks message. Entries are
        keyed by dest hash for client polling, so alias each to its source
        hash too — cross-seed rows (dest != source) would otherwise always
        report no progress.
        """
        out: dict[str, float] = {}
        for h, item in self._live.items():
            try:
                out[h.lower()] = item.progress
                _src = (getattr(item, "source_infohash", "") or "").lower()
                if _src and _src not in out:
                    out[_src] = item.progress
            except Exception:
                continue
        return out

    # ---- VPS1 cleanup janitor ([cleanup]) ----

    # (VPS1 cleanup janitor lives in coordinator_cleanup.CleanupMixin)

    # ---- per-torrent worker ----

    def _drop_live_for(self, ts: TorrentState) -> None:
        """Pop _live entries owned by this worker (throw-path backstop).

        Expected exits pop their own keys; an unexpected throw between
        insert and pop would otherwise poll a dead hash forever. Ended
        workers own nothing live by definition, so this is idempotent.
        """
        try:
            want = (ts.source_infohash or "").lower()
            live = getattr(self, "_live", None)
            if not want or not isinstance(live, dict) or not live:
                return
            for k in [k for k, v in live.items()
                      if (getattr(v, "source_infohash", "") or "").lower() == want]:
                live.pop(k, None)
        except Exception:
            pass

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
        except RcloneTimeoutError as e:
            # Last-resort net: a timed-out move from any path parks the row
            # (source bytes intact) instead of failing it. The two expected
            # sites handle this directly with better context; this only
            # fires for future call paths.
            log.error("rclone timed out for %s: %s (parking, not failing)",
                      ts.source_infohash[:10], e)
            try:
                if ts.state == State.MOVING:
                    self._park_moving(ts, f"rclone move timed out: {e}")
                else:
                    try:
                        ts.last_error = f"rclone timed out: {e}"[:500]
                    except Exception:
                        pass
                    self.store.upsert(ts)
            except Exception:  # noqa: BLE001
                pass
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
            self.store.append_log("ERROR", str(e), ts.source_infohash)
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
        # Quiet-wait: parked WAITING_DISK re-checks are routine while batch
        # moves drain — debug, not info, to avoid filling the log disk.
        if ts.state == State.WAITING_DISK:
            log.debug("worker start: %s state=%s", ts.source_name, ts.state.value)
        else:
            log.info("worker start: %s state=%s", ts.source_name, ts.state.value)
        if ts.state == State.NEW:
            await self._do_new(ts)
        if ts.state == State.QUERYING:
            await self._do_waiting_indexer(ts)
        if ts.state == State.WAITING_DISK:
            await self._wait_disk_then_queue(ts)
        if ts.state == State.QUEUED:
            # download_sem guards QUEUED admission only (_do_queued is
            # short: add + prioritize). The multi-hour _do_downloading
            # below runs outside the semaphore so slots are not pinned.
            async with self.download_sem:
                await self._do_queued(ts)
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
        """Wake one timer-elapsed WAITING_INDEXER row (step-3 loop body)."""
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
        ts = fresh
        log.info(
            "download-indexer retry timer fired for %s (attempt #%d)",
            ts.source_name[:40], ts.indexer_attempts,
        )
        self.transition(ts, State.QUERYING)
        self._spawn_worker(ts)

    # ---- state: NEW ----

    # (batch caps + SSD ledger live in coordinator_ssd.SSDLedgerMixin)

    async def _fetch_source_or_fail(self, ts: TorrentState):
        """Fresh VPS1 metadata, or None (row already moved to FAILED)."""
        st = await self.source_client.get_torrent(ts.source_infohash)
        if st is None:
            err = f"source torrent vanished from client: {ts.source_infohash[:10]}"
            log.warning(err)
            self.transition(ts, State.FAILED, error=err)
            return None
        return st

    def _same_content_torrents(self, all_source: list, st) -> list:
        """Other racing-client torrents for the same content (req #1).

        qB/Deluge don't have a content-id, so heuristic: same name + same
        total size. We use name match — usually racing has 1-3 dupes.
        """
        st_norm = normalize_content_name(st.name)
        return [
            t for t in all_source
            if t.infohash != st.infohash and (t.name == st.name or normalize_content_name(t.name) == st_norm)
        ]

    async def _pick_and_admit(self, ts: TorrentState, st, others: list,
                              *, park_reason: str | None = None,
                              attempt_prowlarr: bool = True) -> None:
        """Pick the SSD source, then park (miss) or admit (WAITING_DISK/QUEUED).

        `park_reason=None` derives source-export-miss vs indexer-miss from
        whether the group is public (a public group never queries the
        download-target indexers, so "indexer miss" would mislead).
        A sticky per-row `force_direct` (Telegram /fetch_ or a previous
        prowlarr-timeout fallback) bypasses Prowlarr for every pick.
        """
        if getattr(ts, "force_direct", 0):
            attempt_prowlarr = False
        decision = await pick_ssd_source_for_racing(
            cfg=self.cfg,
            source_torrent=st,
            other_source_torrents=others,
            prowlarr=self.prowlarr,
            sftp=self.sftp,
            source_client=self.source_client,
            attempt_prowlarr=attempt_prowlarr,
        )
        if decision is None:
            # No SSD source right now → park and retry.
            if park_reason is None:
                park_reason = (
                    "source export miss"
                    if any(_looks_public(t.trackers) for t in [st, *others])
                    else "indexer miss"
                )
            if (park_reason == "indexer miss" and not getattr(ts, "force_direct", 0)
                    and self._prowlarr_timed_out(ts)):
                # Automatic fallback (opt-in): the download-target indexers
                # never produced this release within prowlarr_max_age_seconds.
                # Use the racing client's own bytes instead of FAILED — VPS2
                # then leeches the private swarm (counts toward ratio).
                log.info(
                    "prowlarr timeout for %s with no cross-seed; falling back "
                    "to VPS1 original for SSD download",
                    ts.source_name[:60],
                )
                try:
                    # Fresh retry window for the direct phase: the prowlarr
                    # clock is spent, so a single SFTP blip at the deadline
                    # must not fail the row outright. Direct attempts run
                    # the normal interval until one full window passes.
                    ts.force_direct = 1
                    ts.indexer_first_queried_at = dt.datetime.now(dt.timezone.utc)
                    ts.indexer_attempts = 0
                    self.store.upsert(ts)
                except Exception:
                    pass
                decision = await pick_ssd_source_for_racing(
                    cfg=self.cfg,
                    source_torrent=st,
                    other_source_torrents=others,
                    prowlarr=self.prowlarr,
                    sftp=self.sftp,
                    source_client=self.source_client,
                    attempt_prowlarr=False,
                )
            if decision is None:
                self._park_for_indexer_retry(ts, reason=park_reason)
                return

        ts.cross_seed_infohash = decision.infohash.lower()
        ts.cross_seed_source = decision.source_label
        ts.save_path = str(self.cfg.dest.save_path)

        # Persist the picked .torrent bytes so that recovery after a
        # restart can re-add the cross-seed torrent. Also keep the
        # transient in-memory copy as a fast-path for the immediate
        # QUEUED stage.
        ts.cross_seed_blob = decision.torrent_bytes
        ts._blob = decision.torrent_bytes

        # Admit-time all-remote skip: the decision bytes already sit
        # verified on fuse, so no SSD budget is reserved at all — the QUEUED
        # setup shortcut carries the row fuse-gated to RE_ADDING. Fail-open:
        # any doubt reserves normally.
        try:
            _fully_remote = await self._blob_fully_remote(decision.torrent_bytes)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info("content for %s already fully on remote; skipping SSD budget",
                     st.name[:60])
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise
            return

        # Global SSD ledger: reserve before QUEUED so concurrent high-size
        # arrivals can't all pass a point-in-time free check and exceed the
        # budget as they grow. Estimate uses the stable configured cap.
        needed = self._ssd_estimate_for_new(decision.size_bytes)
        if not await self._ssd_try_reserve(ts.source_infohash, needed):
            log.info(
                "ssd budget in use (reserved ~%d MB); parking %s",
                self._ssd_reserved_total() // (1024 * 1024), st.name,
            )
            self.transition(ts, State.WAITING_DISK)
        else:
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                # Reservation without a QUEUED row would leak budget (the
                # prune only drops non-SSD states on later admissions).
                await self._ssd_release(ts.source_infohash)
                raise

    async def _verify_and_adopt_manual_fuse(self, ts: TorrentState, ext) -> bool:
        """Verify a dest entry as a manual fuse seed and adopt to DONE.

        `ext` is a dest `Torrent` for the same infohash found via a
        category-agnostic hash lookup (manual adds usually carry no
        category/tags). Adoption requires on-fuse save_path + client-complete
        + bytes stat-able at the fuse target (skip_check ghosts must never
        mark DONE). Best-effort: any unverifiable outcome returns False and
        the caller continues the normal SSD/prowlarr flow.

        Returns True when the row was transitioned to DONE (or was already
        DONE by a concurrent actor).
        """
        try:
            if ts.state == State.DONE:
                return True
            if ts.state not in (State.NEW, State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK):
                return False
            try:
                if getattr(self, "store", None) is not None and hasattr(self.store, "is_ignored"):
                    if self.store.is_ignored(ts.source_infohash) is True:
                        return False
            except Exception:
                pass
            if ext is None:
                return False
            if not self._save_path_is_on_fuse(getattr(ext, "save_path", "")):
                return False
            try:
                complete = ext.is_complete() if hasattr(ext, "is_complete") else False
                if callable(complete):
                    complete = complete()
            except Exception:
                return False
            if not complete:
                return False
            try:
                fuse_files = await self.dest_client.get_torrent_files(ext.hash)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "manual fuse check: could not list files for %s: %s",
                    ts.source_infohash[:10], e,
                )
                return False
            fuse_expected = [
                (f.name, f.size_bytes) for f in (fuse_files or [])
                if getattr(f, "name", "")
            ]
            # Empty file list: nothing to verify against — do not adopt blind.
            # Recovery keeps the same fail-closed stance; a warming mount also
            # shows nothing and must park, not DONE.
            if not fuse_expected:
                return False
            try:
                fuse_missing = await self._missing_fuse_files(
                    Path(getattr(ext, "save_path", "")), fuse_expected
                )
            except Exception:
                return False
            if fuse_missing:
                log.warning(
                    "manual fuse check: %s reports complete on fuse %s but %d/%d files missing "
                    "(e.g. %s); keeping normal flow instead of DONE",
                    ts.source_infohash[:10], getattr(ext, "save_path", "?"),
                    len(fuse_missing), len(fuse_expected), fuse_missing[0],
                )
                return False
            log.info(
                "torrent %s is already completed on VPS2 fuse mount (manual add, category-agnostic); marking DONE",
                ts.source_infohash[:10],
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = getattr(ext, "save_path", "")
            try:
                fast_kind = classify(fuse_files, self.cfg).kind
            except Exception:  # noqa: BLE001
                fast_kind = "unknown"
            if fast_kind and fast_kind != ts.classification_kind:
                ts.classification_kind = fast_kind
                try:
                    self.store.upsert(ts)
                except Exception:  # noqa: BLE001
                    pass
            try:
                if getattr(self.cfg.cross_seed, "inject_racing_torrents_to_fuse", False):
                    await self._re_inject_racing_torrents(ts)
            except Exception as e:  # noqa: BLE001
                # Manual entry already seeds; a failed re-inject must not block
                # DONE — the late-seed job repairs the racing injection.
                log.warning(
                    "manual fuse check: re-inject for %s failed (keeping DONE): %s",
                    ts.source_infohash[:10], e,
                )
            try:
                self.transition(ts, State.DONE)
            except ValueError:
                # Concurrent actor already moved the row (e.g. tick sweep beat
                # this worker to DONE). Treat as adopted, never overwrite.
                try:
                    fresh = self.store.get(ts.source_infohash, include_blob=False)
                except Exception:
                    fresh = None
                if fresh is not None and fresh.state == State.DONE:
                    try:
                        ts.state = fresh.state
                        ts.dest_infohash = fresh.dest_infohash or ts.dest_infohash
                        ts.save_path = fresh.save_path or ts.save_path
                    except Exception:
                        pass
                    return True
                log.warning(
                    "manual fuse check: could not transition %s to DONE from %s",
                    ts.source_infohash[:10], ts.state.value,
                )
                return False
            return True
        except Exception as e:  # noqa: BLE001
            log.warning(
                "manual fuse check failed for %s: %s",
                getattr(ts, "source_infohash", "?")[:10], e,
            )
            return False

    async def _adopt_manual_fuse_if_present(self, ts: TorrentState) -> bool:
        """Category-agnostic check for a manually added fuse seed (same infohash).

        Used before any prowlarr query in _do_new/_do_waiting_indexer so an
        operator-added torrent (no category/tags) fast-tracks to DONE instead
        of parking in WAITING_INDEXER. Single-hash dest lookup — cheap and
        independent of the racing-category filter recovery relies on.
        """
        try:
            dest_client = getattr(self, "dest_client", None)
            if dest_client is None:
                return False
            if ts.state not in (State.NEW, State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK):
                return False
            h = (ts.source_infohash or "").lower()
            if not h:
                return False
            try:
                existing = await dest_client.list_torrents(hashes=[h])
            except Exception as e:  # noqa: BLE001
                log.warning("manual fuse check: dest lookup failed for %s: %s", h[:10], e)
                return False
            if not existing:
                return False
            ext = next((t for t in existing if (t.hash or "").lower() == h), existing[0])
            return await self._verify_and_adopt_manual_fuse(ts, ext)
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse check failed for %s: %s", ts.source_infohash[:10], e)
            return False

    async def _sweep_manual_fuse_adoptions(self) -> None:
        """Batch-adopt pre-SSD rows whose infohash already seeds from fuse.

        Tick-level responsiveness net: WAITING_INDEXER rows only wake on their
        30m retry timer, so a manual add between retries would otherwise sit
        parked. One batched hash lookup per tick covers all pre-SSD rows at
        once (qB accepts pipe-separated hashes); rows with live workers are
        skipped — their own _do_* check adopts without racing the worker.
        """
        try:
            dest_client = getattr(self, "dest_client", None)
            store = getattr(self, "store", None)
            if dest_client is None or store is None:
                return
            try:
                pre_ssd = store.list_by_state(
                    State.NEW, State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK
                )
            except Exception:
                return
            if not pre_ssd:
                return
            running = getattr(self, "_running_infohashes", None)
            candidates: list[TorrentState] = []
            hashes: list[str] = []
            seen: set[str] = set()
            for ts in pre_ssd:
                try:
                    h = (ts.source_infohash or "").lower()
                except Exception:
                    continue
                if not h or h in seen:
                    continue
                try:
                    if isinstance(running, set) and h in running:
                        continue
                except Exception:
                    pass
                try:
                    if hasattr(store, "is_ignored") and store.is_ignored(ts.source_infohash) is True:
                        continue
                except Exception:
                    pass
                seen.add(h)
                hashes.append(h)
                candidates.append(ts)
            if not hashes:
                return
            try:
                existing = await dest_client.list_torrents(hashes=hashes)
            except Exception as e:  # noqa: BLE001
                log.warning("manual fuse sweep: dest lookup failed: %s", e)
                return
            if not existing:
                return
            by_hash = {}
            for t in existing or []:
                try:
                    by_hash[(t.hash or "").lower()] = t
                except Exception:
                    continue
            for ts in candidates:
                try:
                    h = (ts.source_infohash or "").lower()
                    ext = by_hash.get(h)
                    if ext is None:
                        continue
                    # Re-read: a worker may have moved this row after the
                    # snapshot above — never overwrite a fresh state.
                    try:
                        fresh = store.get(ts.source_infohash, include_blob=False)
                    except Exception:
                        fresh = None
                    if fresh is None or fresh.state != ts.state:
                        continue
                    if fresh.state not in (State.NEW, State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK):
                        continue
                    await self._verify_and_adopt_manual_fuse(fresh, ext)
                except Exception:
                    continue
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse sweep failed: %s", e)

    async def _do_new(self, ts: TorrentState) -> None:
        # Label-set check only (no blob-dir probe): at NEW time every watch
        # row still carries exactly one origin label, and a stale blob dir
        # must never reroute a VPS1 row into the watch flow (missing blob
        # there would FAIL it).
        if (ts.cross_seed_source or "") in WATCH_ORIGIN_LABELS:
            await self._do_new_watch_dir(ts)
            return

        # Make sure we have the source torrent metadata
        st = await self._fetch_source_or_fail(ts)
        if st is None:
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes
        ts.source_tracker = st.trackers[0] if st.trackers else ""
        ts.source_announce_url = ts.source_tracker or ts.source_announce_url

        # Manual fuse fast-track: same infohash already seeding from fuse
        # (any category) with verified bytes needs no prowlarr/SSD work.
        try:
            if await self._adopt_manual_fuse_if_present(ts):
                return
        except Exception:
            pass

        all_source = await self._list_source_torrents()
        await self._pick_and_admit(ts, st, self._same_content_torrents(all_source, st))

    def _watch_rank(self, ts: TorrentState) -> int:
        """SSD-download priority for a watch-dir row: public (0) first."""
        return watch_rank(ts, self.cfg)

    def _watch_election(self, ts: TorrentState) -> tuple[bool, TorrentState | None]:
        """Whether this watch-dir row may proceed to SSD admission.

        Drops sharing content (normalized name + size, same rule as VPS1
        grouping) elect ONE downloader; the rest defer until it is
        DONE/FAILED/gone and then ride the already-remote fast paths with
        no re-download. Rows already holding SSD/client presence
        (QUEUED/DOWNLOADING/MOVING/RE_ADDING) lock ownership first-come —
        no preemption, since same filenames share one save_path. Returns
        (True, None) when the row may proceed, else (False, winner).
        Origin is checked with `_is_watch_row` (labels change past NEW),
        never one label. Never raises: any doubt proceeds solo (today's
        behavior).
        """
        try:
            rows = self.store.all()
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir election: cannot list rows (%s); proceeding solo", e)
            return True, None
        try:
            winner = watch_election_winner(rows, ts, self.cfg)
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir election failed (%s); proceeding solo", e)
            return True, None
        if winner is None:
            return True, None
        return False, winner

    def _watch_wait_note(self, ts: TorrentState) -> str:
        """Short human reason a watch row is deferred, or "" when it may proceed.

        Names the winning copy's tracker ("Waiting turn · <domain> copy
        first") so the card explains itself instead of reading as stuck.
        Never raises.
        """
        try:
            proceed, owner = self._watch_election(ts)
            if proceed or owner is None:
                return ""
            domain = announce_domain(owner.source_announce_url) or announce_domain(
                owner.source_tracker)
            if domain:
                return f"Waiting turn · {domain} copy first"
            try:
                rank = self._watch_rank(owner)
            except Exception:
                rank = 2
            fallback = ("public copy first", "tracker copy first", "sibling copy first")
            return "Waiting turn · " + (fallback[rank] if rank < len(fallback) else fallback[2])
        except Exception:
            return ""

    def _is_watch_row(self, ts: TorrentState) -> bool:
        """True when this row originated from a watch-dir drop."""
        try:
            return bool(is_watch_row(ts, self.cfg))
        except Exception:
            return False

    async def _do_new_watch_dir(self, ts: TorrentState) -> None:
        """Process a manual torrent drop from the watch directory."""
        blob = ts._blob or ts.cross_seed_blob
        if not blob:
            blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            if blob:
                ts.cross_seed_blob = blob
                ts._blob = blob
        if not blob:
            log.error("watch-dir torrent has no bytes: %s", ts.source_name)
            self.transition(ts, State.FAILED, error="missing .torrent bytes for watch-dir drop")
            return

        # Same-content election across watch drops: one SSD downloader
        # (public > download-tracker > sacrificial); the rest stay NEW
        # until it is DONE/FAILED/gone, then ride the already-remote
        # fast paths with no re-download.
        try:
            _proceed, _owner = self._watch_election(ts)
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir election failed for %s (%s); proceeding solo",
                        ts.source_name[:60], e)
            _proceed, _owner = True, None
        if not _proceed:
            _owner_desc = (
                f"{(_owner.source_infohash or '')[:10]} ({_owner.state.value})"
                if _owner is not None else "unknown owner"
            )
            log.info("watch-dir: deferring %s — same content owned by %s",
                     ts.source_name[:60], _owner_desc)
            return

        ts.save_path = str(self.cfg.dest.save_path)
        tracker_list = [u for u in ts.source_announce_url.split(",") if u] or ([ts.source_tracker] if ts.source_tracker else [])
        is_public = _looks_public(tracker_list)
        is_download_tracker = (
            self.cfg.prowlarr.enabled
            and any(self.cfg.prowlarr.is_download_indexer(u) for u in tracker_list)
        )
        needs_sacrificial_copy = not is_download_tracker and not is_public

        chosen_blob = blob
        chosen_label = "public-watch-dir" if is_public else "watch-dir"
        chosen_infohash = ts.source_infohash
        chosen_size = ts.total_bytes

        # Prepare persistence directory for cross-seed torrents. The
        # infohash is hex-validated by the helper: untrusted values can
        # never escape the blob root via join/mkdir.
        watch_cross_dir = _watch_cross_seed_dir(
            self.cfg.general.state_db, ts.source_infohash)
        if watch_cross_dir is not None:
            await asyncio.to_thread(watch_cross_dir.mkdir, parents=True, exist_ok=True)
            # Always persist the dropped .torrent so it can be seeded on FUSE
            _safe_name = f"{ts.source_infohash.strip().lower()}.torrent"
            await asyncio.to_thread((watch_cross_dir / _safe_name).write_bytes, blob)
        else:
            log.warning("refusing blob persistence for %s: bad infohash %r",
                        ts.source_name[:60], ts.source_infohash)

        query_prowlarr = True
        prefer_prowlarr = True
        if hasattr(self.cfg, "watch_dir") and self.cfg.watch_dir is not None:
            qp = getattr(self.cfg.watch_dir, "query_prowlarr", True)
            if isinstance(qp, bool):
                query_prowlarr = qp
            pp = getattr(self.cfg.watch_dir, "prefer_prowlarr_result", True)
            if isinstance(pp, bool):
                prefer_prowlarr = pp

        # If Prowlarr is enabled and not skipped, perform single parallel search
        should_skip_prowlarr = self.cfg.prowlarr.should_skip_title(ts.source_name)
        if not query_prowlarr:
            log.info("prowlarr: skipping search for watch-dir release %r (watch_dir.query_prowlarr is False)", ts.source_name)
        elif should_skip_prowlarr:
            log.info(
                "prowlarr: skipping search for watch-dir release %r (matches skip_query_substrings)",
                ts.source_name,
            )
        elif self.cfg.prowlarr.enabled and self.prowlarr is not None:
            indexers_to_query = []
            # Download-target indexer names (lowercase), in priority order —
            # the sacrificial pick below tries them in this order.
            download_idx_names: list[str] = []
            if needs_sacrificial_copy and prefer_prowlarr:
                try:
                    for dl_idx in self.prowlarr.get_download_indexers():
                        download_idx_names.append(dl_idx.name.lower())
                        indexers_to_query.append(dl_idx)
                except Exception as e:  # noqa: BLE001
                    log.warning("could not get download-target indexers: %s", e)

            # Add all private indexers from tracker_map
            seen_names = set(download_idx_names)
            for name in self.cfg.prowlarr.tracker_map.entries.values():
                idx = self.prowlarr.get_indexer_by_name(name)
                if idx and idx.name.lower() not in seen_names and idx.enable:
                    indexers_to_query.append(idx)
                    seen_names.add(idx.name.lower())

            hits_by_indexer: dict[str, list[TorrentHit]] = {}
            if indexers_to_query:
                try:
                    hits_by_indexer = await self.prowlarr.search_indexers_parallel(
                        indexers_to_query, ts.source_name
                    )
                except Exception as e:
                    log.warning("watch-dir: prowlarr search failed for %s: %s", ts.source_name, e)
                    hits_by_indexer = {}

            # 1. Sacrificial download torrent from the download-target
            # indexers, in priority order — first exact match wins.
            if prefer_prowlarr and download_idx_names:
                for dl_name in download_idx_names:
                    dl_hits = hits_by_indexer.get(dl_name, [])
                    matching_dl = [
                        h for h in dl_hits
                        if _matches_release(h.title, h.size_bytes, ts.source_name, ts.total_bytes)
                    ]
                    matching_dl.sort(
                        key=lambda h: (
                            normalize_content_name(h.title) != normalize_content_name(ts.source_name),
                            abs(h.size_bytes - ts.total_bytes),
                        )
                    )
                    if not matching_dl:
                        continue
                    best_dl = matching_dl[0]
                    try:
                        dl_blob = await self.prowlarr.download_torrent(best_dl)
                        from .watchdir import _bencoded_info_hash
                        dl_h, _, _, _ = _bencoded_info_hash(dl_blob)
                        chosen_blob = dl_blob
                        chosen_label = "public-prowlarr"
                        chosen_size = best_dl.size_bytes
                        chosen_infohash = dl_h
                        # Persist alongside the dropped + discovered blobs:
                        # the watch blob dir is the single source of truth
                        # for fuse injection, so flows that skip RE_ADDING
                        # (e.g. the QUEUED fuse-DONE fast path) still seed
                        # every copy. Same-infohash variants (identical
                        # files, different announce) share one filename —
                        # the dropped blob already represents them.
                        if dl_h.lower() != ts.source_infohash.lower():
                            await asyncio.to_thread((watch_cross_dir / f"{dl_h}.torrent").write_bytes, dl_blob)
                        log.info(
                            "watch-dir: using sacrificial download torrent from %s (%s)",
                            best_dl.indexer, best_dl.title,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "failed to fetch download indexer torrent: %s; using dropped file", e
                        )
                    break

            # 2. Collect other private tracker cross-seeds to inject onto FUSE
            for idx_name, hits in hits_by_indexer.items():
                if idx_name in download_idx_names:
                    continue
                for hit in hits:
                    if _matches_release(hit.title, hit.size_bytes, ts.source_name, ts.total_bytes):
                        try:
                            cross_blob = await self.prowlarr.download_torrent(hit)
                            from .watchdir import _bencoded_info_hash
                            cross_h, _, _, _ = _bencoded_info_hash(cross_blob)
                            # Same info, different announce (identical files)
                            # shares one filename — first persisted wins
                            # (dropped blob, then sacrificial), so one file
                            # seeds it and variants can't clobber each other.
                            if (watch_cross_dir / f"{cross_h}.torrent").exists():
                                log.debug(
                                    "watch-dir: cross-seed %s from %s shares infohash; keeping persisted copy",
                                    cross_h[:10], hit.indexer,
                                )
                            else:
                                await asyncio.to_thread((watch_cross_dir / f"{cross_h}.torrent").write_bytes, cross_blob)
                            log.info(
                                "watch-dir: discovered cross-seed from %s: %s (%s)",
                                hit.indexer, hit.title, cross_h[:10],
                            )
                        except Exception as e:  # noqa: BLE001
                            log.warning("could not download cross-seed from %s: %s", hit.indexer, e)

        # Classify the chosen torrent metadata for downstream routing.
        parsed_files: list = []
        try:
            from .watchdir import extract_torrent_files_from_bencoded
            parsed_files = extract_torrent_files_from_bencoded(chosen_blob)
            if parsed_files:
                cls = classify(parsed_files, self.cfg)
                ts.classification_kind = cls.kind
        except Exception as e:
            log.warning("watch-dir: failed to classify %s: %s", ts.source_name, e)

        # Feasibility is per individual file: anything may flow (batched as
        # needed) unless one file alone exceeds the SSD cap.
        too_big = oversize_single_file(parsed_files, self.cfg)
        if too_big is not None:
            log.warning("watch-dir: single file %s exceeds skip threshold; failing %s",
                        too_big, ts.source_name)
            self.transition(ts, State.FAILED, error=f"single file larger than skip threshold: {too_big}")
            return

        ts.cross_seed_infohash = chosen_infohash.lower()
        ts.cross_seed_source = chosen_label
        ts.cross_seed_blob = chosen_blob
        ts._blob = chosen_blob

        # Admit-time all-remote skip: the bytes already sit verified on
        # fuse, so no SSD budget is reserved at all (even reserve(0) can
        # park on a full disk) — the QUEUED setup shortcut carries the row
        # fuse-gated to RE_ADDING. Fail-open: any doubt reserves normally.
        try:
            _fully_remote = await self._blob_fully_remote(chosen_blob)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info("watch-dir: %s already fully on remote; skipping SSD budget",
                     ts.source_name[:60])
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise
            return

        needed = self._ssd_estimate_for_new(chosen_size)
        if not await self._ssd_try_reserve(ts.source_infohash, needed):
            log.info(
                "ssd budget in use (reserved ~%d MB); parking %s",
                self._ssd_reserved_total() // (1024 * 1024), ts.source_name,
            )
            self.transition(ts, State.WAITING_DISK)
        else:
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise

    def _prowlarr_timed_out(self, ts: TorrentState) -> bool:
        """True when the opt-in racing-torrent fallback may fire.

        Requires the config flag plus an exhausted prowlarr retry window
        (first attempt older than prowlarr_max_age_seconds). Fail-closed:
        any doubt returns False and the row keeps retrying / fails as before.
        """
        try:
            if not getattr(getattr(self, "cfg", None), "cross_seed", None):
                return False
            if not self.cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout:
                return False
            first = ts.indexer_first_queried_at
            if first is None:
                return False
            max_age = dt.timedelta(
                seconds=self.cfg.cross_seed.prowlarr_max_age_seconds)
            return (dt.datetime.now(dt.timezone.utc) - first) >= max_age
        except Exception:
            return False

    def _park_for_indexer_retry(self, ts: TorrentState, *, reason: str = "indexer miss") -> None:
        """Park into WAITING_INDEXER with an escalating retry timer.

        The first attempt: retry after prowlarr_retry_interval_seconds.
        Subsequent attempts: same interval (fixed, not exponential — we
        expect the download-target indexers to catch up shortly for racing
        releases).
        Hard cap: prowlarr_max_age_seconds since the FIRST attempt. If
        that ceiling is reached, mark FAILED for manual handling.
        """
        now = dt.datetime.now(dt.timezone.utc)
        if ts.indexer_first_queried_at is None:
            ts.indexer_first_queried_at = now
        ts.indexer_attempts += 1
        next_retry = now + dt.timedelta(
            seconds=self.cfg.cross_seed.prowlarr_retry_interval_seconds
        )
        ts.indexer_next_retry_at = next_retry
        max_age = dt.timedelta(seconds=self.cfg.cross_seed.prowlarr_max_age_seconds)
        elapsed = now - ts.indexer_first_queried_at

        log.info(
            "%s #%d for %s; next retry at %s (elapsed=%ds, max=%ds)",
            reason, ts.indexer_attempts, ts.source_name,
            next_retry.isoformat(timespec="seconds"),
            int(elapsed.total_seconds()), int(max_age.total_seconds()),
        )

        if elapsed >= max_age:
            log.error(
                "download-target indexers giving up on %s after %d attempts (%ds > %ds max)",
                ts.source_name, ts.indexer_attempts,
                int(elapsed.total_seconds()), int(max_age.total_seconds()),
            )
            # From WAITING_INDEXER → FAILED is legal (see ALLOWED).
            self.transition(
                ts, State.FAILED,
                error=(f"Prowlarr cross-seed not found within "
                       f"{self.cfg.cross_seed.prowlarr_max_age_seconds}s"),
            )
            return

        # If we're being called from _do_new (state is NEW), the
        # transition is legal. If we're being re-called from
        # _do_waiting_indexer, the state is already WAITING_INDEXER
        # and we just need to bump the retry timestamp.
        if ts.state != State.WAITING_INDEXER:
            self.transition(ts, State.WAITING_INDEXER)
        else:
            if self._abandoned(ts):
                raise AbandonedError(
                    f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                    "not refreshing indexer park"
                )
            self.store.upsert(ts)
            # No transition() fired, so push the updated timer manually.
            self._schedule_telegram_update(ts)

    async def _do_waiting_indexer(self, ts: TorrentState) -> None:
        """Wake up from WAITING_INDEXER and re-pick the SSD source.

        Called by _tick when the row's indexer_next_retry_at has elapsed.
        """
        # Pull fresh data from VPS1 in case the torrent name changed.
        st = await self._fetch_source_or_fail(ts)
        if st is None:
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes

        # Manual fuse fast-track before another prowlarr query: a torrent
        # added by hand to VPS2 while parked must not wait out the retry.
        try:
            if await self._adopt_manual_fuse_if_present(ts):
                return
        except Exception:
            pass

        all_source = await self._list_source_torrents()
        await self._pick_and_admit(
            ts, st, self._same_content_torrents(all_source, st),
            park_reason="indexer miss",
        )

    # How long a still-full WAITING_DISK row stays quiet before its next
    # SSD re-check. Batches drain via MOVING in the meantime.
    WAITING_DISK_RECHECK_SECONDS = 60.0

    async def _estimate_remaining_bytes(self, ts: TorrentState) -> int | None:
        """Remaining SSD bytes for a cursor-having row; None when unknowable.

        A partially-moved season retries WAITING_DISK against its remaining
        batches, not its full total (a 32 GB pack with 20 GB already remote
        must not block a 16 GB waiter on phantom bytes). Best-effort: any
        failure returns None and the caller stays parked.
        """
        try:
            if not hasattr(self, "dest_client"):
                return None
            try:
                total_batches = int(getattr(ts, "batches_total", 0) or 0)
            except (TypeError, ValueError):
                total_batches = 0
            if total_batches <= 0:
                return None
            h = ts.dest_infohash or ts.source_infohash
            try:
                files = await self.dest_client.get_torrent_files(h)
            except Exception:
                return None
            if not files:
                return None
            cls = classify(files, self.cfg)
            kind = cls.kind or ts.classification_kind or "unknown"
            cap = self._frozen_batch_cap(ts)
            if cap <= 0:
                return None
            batches = self._resolve_batches(files, kind, cap)
            if not batches:
                return None
            try:
                idx = int(getattr(ts, "batch_index", 0) or 0)
            except (TypeError, ValueError):
                idx = 0
            idx = max(0, min(idx, len(batches)))
            if idx >= len(batches):
                return 0
            rem = await self._remaining_batch_footprint(batches[idx:], kind)
            return int(rem) if rem is not None else None
        except Exception:
            return None

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
                    if _owner is not None else "unknown owner"
                )
                log.debug("watch-dir: %s stays waiting_disk — same content owned by %s",
                          ts.source_name[:60], _owner_desc)
                # Refresh the quiet-wait window like the SSD-full path so a
                # deferred row re-checks election at most once per interval.
                try:
                    _wd = getattr(self, "_waiting_disk_next_check", None)
                    if _wd is None:
                        _wd = {}
                        self._waiting_disk_next_check = _wd  # type: ignore[attr-defined]
                    _wd[(ts.source_infohash or "").lower()] = (
                        time.monotonic() + self.WAITING_DISK_RECHECK_SECONDS
                    )
                except Exception:
                    pass
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
                log.info("retrying %s against remaining ~%d MB (full ~%d MB did not fit)",
                         ts.source_name[:60], rem // (1024 * 1024), needed // (1024 * 1024))
                admitted = await self._ssd_try_reserve(ts.source_infohash, rem)
                if admitted:
                    needed = rem
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
            # The SSD reservation is already held; release it if we stay parked.
            # Non-int configs (test doubles) skip the count check.
            _max_dl_raw = getattr(self.cfg, "max_active_downloads", 0)
            if isinstance(_max_dl_raw, bool) or not isinstance(_max_dl_raw, (int, float)):
                _max_dl = 0
            else:
                _max_dl = int(_max_dl_raw or 0)
            if _max_dl > 0:
                try:
                    _active_dl = len(self.store.list_by_state(State.QUEUED, State.DOWNLOADING))
                except Exception:
                    _active_dl = 0
                if _active_dl >= _max_dl:
                    # Downloads full: release the just-made reservation and
                    # stay parked (never FAILED — space, not an error).
                    await self._ssd_release(ts.source_infohash)
                    try:
                        _wd2 = getattr(self, "_waiting_disk_next_check", None)
                        if _wd2 is None:
                            _wd2 = {}
                            self._waiting_disk_next_check = _wd2  # type: ignore[attr-defined]
                        _wd2[(ts.source_infohash or "").lower()] = (
                            time.monotonic() + self.WAITING_DISK_RECHECK_SECONDS
                        )
                    except Exception:
                        pass
                    log.debug(
                        "downloads full (%d/%d); %s stays waiting_disk",
                        _active_dl, _max_dl, ts.source_name[:60],
                    )
                    return
            self.transition(ts, State.QUEUED)
            return
        # Still full: stay parked quietly until the next interval instead of
        # hot-looping every tick while batch moves drain.
        try:
            _wd = getattr(self, "_waiting_disk_next_check", None)
            if _wd is None:
                _wd = {}
                self._waiting_disk_next_check = _wd  # type: ignore[attr-defined]
            _wd[(ts.source_infohash or "").lower()] = (
                time.monotonic() + self.WAITING_DISK_RECHECK_SECONDS
            )
        except Exception:
            pass
        log.debug(
            "ssd still full; %s stays waiting_disk (need ~%d MB, re-check in %ds)",
            ts.source_name[:60], needed // (1024 * 1024),
            int(self.WAITING_DISK_RECHECK_SECONDS),
        )

    # ---- state: QUEUED ----

    def _try_admit_download(self, ts: TorrentState) -> bool:
        """Admit one QUEUED row into downloading; False parks it (stay QUEUED).

        The tick loop only gates rows that are QUEUED at snapshot time, so a
        fresh-start burst of NEW workers can sail past it into DOWNLOADING
        (live incident: 5 concurrent against max 3). This edge check closes
        it. Admission is atomic in the event loop (sync check+record, no
        await between), so same-tick workers racing through slow RPCs can't
        all read a stale count. Rows already DOWNLOADING are grandfathered
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
            try:
                n_down = len(self.store.list_by_state(State.DOWNLOADING))
            except Exception:
                n_down = 0
            others = len(adm - {key}) if key else len(adm)
            if n_down + others >= max_dl:
                log.info("downloads full (%d/%d); %s stays queued",
                         n_down + others, max_dl, ts.source_name[:60])
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

    def _park_queued_for_download_slot(self, ts: TorrentState) -> bool:
        """Stay QUEUED when the download cap is full; True when parked."""
        try:
            return not self._try_admit_download(ts)
        except Exception:
            return False

    async def _do_queued(self, ts: TorrentState) -> None:
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
        check_hashes = [h for h in (ts.source_infohash, ts.cross_seed_infohash, ts.dest_infohash) if h]
        existing = await self.dest_client.list_torrents(hashes=check_hashes)
        if existing:
            ext = existing[0]
            fuse_mounts = [
                str(self.cfg.rclone.fuse.mount).rstrip("/\\").replace("\\", "/"),
                str(self.cfg.rclone.fuse.mount_unsorted).rstrip("/\\").replace("\\", "/"),
            ]
            save_path = ext.save_path.rstrip("/\\").replace("\\", "/")
            on_fuse = any(save_path == fm or save_path.startswith(fm + "/") for fm in fuse_mounts if fm)
            if on_fuse and ext.is_complete():
                # Verify the bytes are really behind the fuse path: a torrent
                # added with skip_check=True reports complete even when its
                # files were never moved. Missing bytes must never mark DONE —
                # and must never FAILED either (a warming/dead mount also
                # shows nothing, and FAILED would trigger re-downloads).
                try:
                    fuse_files = await self.dest_client.get_torrent_files(ext.hash)
                    fuse_list_failed = False
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not list fuse torrent files for %s: %s",
                        ts.source_infohash[:10], e,
                    )
                    fuse_files = []
                    fuse_list_failed = True
                fuse_expected = [
                    (f.name, f.size_bytes) for f in fuse_files
                    if getattr(f, "name", "")
                ]
                if fuse_list_failed or not fuse_expected:
                    # Fail closed: an unlistable or empty file list must never
                    # mark DONE (would strand unseeded bytes as complete).
                    # Stay QUEUED so the next tick retries via this same check.
                    log.warning(
                        "fuse verification inconclusive for %s (list_failed=%s, files=%d); "
                        "staying queued",
                        ts.source_infohash[:10], fuse_list_failed, len(fuse_expected),
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
                            ts.source_infohash[:10], save_path,
                            len(fuse_missing), len(fuse_expected), ssd_root,
                        )
                        ts.dest_infohash = ext.hash.lower()
                        ts.save_path = str(ssd_root)
                        if self._park_queued_for_download_slot(ts):
                            return
                        self.transition(ts, State.DOWNLOADING)
                        return
                    log.warning(
                        "torrent %s reports complete on fuse %s but %d/%d files missing "
                        "(e.g. %s); routing to RE_ADDING for gated retry instead of DONE",
                        ts.source_infohash[:10], save_path,
                        len(fuse_missing), len(fuse_expected), fuse_missing[0],
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
                    fast_kind = classify(fuse_files, self.cfg).kind
                except Exception:  # noqa: BLE001
                    fast_kind = "unknown"
                if fast_kind and fast_kind != ts.classification_kind:
                    ts.classification_kind = fast_kind
                    try:
                        self.store.upsert(ts)
                    except Exception:  # noqa: BLE001
                        pass
                if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
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
                            await self._re_inject_watch_dir_torrents(ts)
                        except _WEBUI_RETRY_ERRORS as e:
                            log.warning(
                                "watch re-inject hit transient dest error for %s (%s); staying queued",
                                ts.source_name[:60], e,
                            )
                            try:
                                self.store.upsert(ts)
                            except Exception:
                                pass
                            return
                    else:
                        await self._re_inject_racing_torrents(ts)
                self.transition(ts, State.DONE)
                return

            log.info(
                "torrent already on VPS2: %s (complete=%s, on_fuse=False)",
                ts.source_infohash[:10], ext.is_complete(),
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = ext.save_path
            if self._park_queued_for_download_slot(ts):
                return
            try:
                await self.dest_client.resume(ext.hash)
            except Exception as e:  # noqa: BLE001
                # Transient qB hiccup (registration lag, timeout): stay
                # QUEUED so the next tick retries via this same check.
                log.warning("resume %s hit transient dest error: %s; staying queued",
                            ext.hash[:10], e)
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
                return
            self.transition(ts, State.DOWNLOADING)
            return

        # Brand-new entry: gate before adding so a fresh-start burst of NEW
        # workers can't sail past the tick's snapshot-QUEUED-only cap into
        # concurrent DOWNLOADING (live incident: 5 at once against max 3).
        if self._park_queued_for_download_slot(ts):
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
                ts, State.FAILED, error=f"add rejected: {result.detail}",
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
            new_hash = await self._await_hash_for_name(ts.source_name)

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
            await self._setup_queued_download(ts, blob)
        except Exception as e:  # noqa: BLE001
            log.warning("queued setup hit transient dest error for %s: %s; staying queued",
                        ts.source_infohash[:10], e)
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
                for b in blist for e in (getattr(b, "episodes", None) or [])
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

    async def _blob_fully_remote(self, blob: bytes | None) -> bool | None:
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
            from .watchdir import extract_torrent_files_from_bencoded
            try:
                parsed = extract_torrent_files_from_bencoded(bytes(blob))
            except Exception:
                return None
            names = [(f.name, f.size_bytes) for f in (parsed or [])
                     if getattr(f, "name", "")]
            if not names:
                return None
            try:
                kind = classify(parsed, self.cfg).kind
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
        """
        if not items:
            return set()
        try:
            mount = self._target_mount_for_kind(kind, Path(self.cfg.dest.save_path))
        except Exception:
            return set()

        def _check() -> set[str]:
            skipped: set[str] = set()
            for name, size in items:
                joined = _safe_ssd_join(mount, name or "")
                if joined is None:
                    continue
                try:
                    actual = joined.stat().st_size
                except OSError:
                    continue
                want = size or 0
                if actual == want:
                    skipped.add(name)
            return skipped

        try:
            return await asyncio.to_thread(_check)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse skip check failed, downloading everything: %s", e)
            return set()

    async def _setup_queued_download(self, ts: TorrentState, blob: bytes) -> None:
        """Classify, prioritize, and resume a just-added SSD torrent."""
        # Classify (with a short retry: the files endpoint can 404 for a few
        # seconds right after add while qB registers the torrent).
        files: list[TorrentFile] = []
        last_err: Exception | None = None
        for _ in range(4):
            try:
                files = await self.dest_client.get_torrent_files(ts.dest_infohash or ts.source_infohash)
                last_err = None
                break
            except Exception as e:  # noqa: BLE001
                last_err = e
                await asyncio.sleep(2)
        if last_err is not None:
            raise last_err
        cls = classify(files, self.cfg)
        ts.classification_kind = cls.kind

        if len(files) == 1:
            # A previous run may already have moved this exact file: no SSD
            # need, no oversize failure — straight to MOVING (its branch
            # tolerates already-remote singles) for the fuse-gated finish.
            only = files[0]
            if await self._fuse_skipped([(only.name, only.size_bytes)], cls.kind):
                log.info("single file %s already on remote; skipping SSD download",
                         only.name)
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
            log.warning("single file %s exceeds skip threshold; failing %s",
                        too_big, ts.source_name)
            # Remove the paused torrent from VPS2 so it does not leak as an orphan
            h = ts.dest_infohash or ts.source_infohash
            if h:
                try:
                    await self.dest_client.delete(h, delete_files=True)
                except Exception as e:
                    log.warning("failed to delete skipped oversize content %s: %s", h[:10], e)
            self.transition(
                ts, State.FAILED, error=f"single file larger than skip threshold: {too_big}",
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
                if not ssd_has_room(self.cfg, min(total_ep, ts.total_bytes or total_ep)):
                    log.info("ssd cap in use; parking %s", ts.source_name)
                    try:
                        _h = ts.dest_infohash or ts.source_infohash
                        if _h:
                            await self.dest_client.delete(_h, delete_files=True)
                    except Exception:
                        pass
                    self.transition(ts, State.WAITING_DISK)
                    return
                cap = total_ep or ts.total_bytes or 1
            batches = make_batches(episodes, cap_bytes=cap)
            ts.batches_total = len(batches)
            ts.batch_index = 0
            # Refine global reservation to the real footprint (max REMAINING
            # batch — covers varying episode sizes, isolated batches hold one
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
                    [(e.file_name, e.size_bytes) for e in first.episodes], cls.kind,
                )
                prio_map = {f.name: 0 for f in files}
                first_need = {e.file_name for e in first.episodes} - skip0
                for name in first_need:
                    prio_map[name] = 1
                await self.dest_client.set_file_priorities(
                    ts.dest_infohash or ts.source_infohash, prio_map,
                )
        elif cls.kind in ("movie", "unknown") and len(files) > 1:
            # Type-agnostic file-group batching for multi-file content that
            # is not episodic (games, disc images, complete packs with plain
            # numbering): stream name-sorted groups through the SSD instead
            # of needing the whole torrent on disk at once. Single episodes
            # stay on the full-torrent path even when extras are present.
            cap = self._frozen_batch_cap(ts)
            if cap <= 0:
                total_files = sum(f.size_bytes for f in files)
                if not ssd_has_room(self.cfg, min(total_files, ts.total_bytes or total_files)):
                    log.info("ssd cap in use; parking %s", ts.source_name)
                    try:
                        _h = ts.dest_infohash or ts.source_infohash
                        if _h:
                            await self.dest_client.delete(_h, delete_files=True)
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
                    [(e.file_name, e.size_bytes) for e in first.episodes], cls.kind,
                )
                first_need = wanted - skip0
                prio_map = {f.name: (1 if f.name in first_need else 0) for f in files}
                await self.dest_client.set_file_priorities(
                    ts.dest_infohash or ts.source_infohash, prio_map,
                )

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
                ts.batches_total, ts.source_name,
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
                self._ssd_reserved_total() // (1024 * 1024), ts.source_name,
            )
            try:
                _h = ts.dest_infohash or ts.source_infohash
                if _h:
                    await self.dest_client.delete(_h, delete_files=True)
            except Exception:
                pass
            self.transition(ts, State.WAITING_DISK)
            return

        # Resume (skipped when the first batch needs nothing locally yet:
        # a client with zero selected files may refuse; the download loop
        # resumes on reaching the first batch with work). Gate first: setup
        # (add + prioritize) already ran, so park with the paused entry
        # intact — the next tick resumes through the existing-entry path.
        if self._park_queued_for_download_slot(ts):
            return
        if first_need is None or first_need:
            await self.dest_client.resume(ts.dest_infohash or ts.source_infohash)
        self.transition(ts, State.DOWNLOADING)

    async def _await_hash_for_name(self, name: str, *, timeout_s: float = 60) -> str | None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if getattr(self, "_stop", False):
                return None
            # Dest entries are always added with category="racing": filter
            # server-side instead of pulling 10k long-term seeds per poll.
            rows = await self.dest_client.list_torrents(category="racing")
            for t in rows:
                if t.name == name:
                    return t.hash.lower()
            await asyncio.sleep(2)
        return None

    # ---- state: DOWNLOADING ----

    def _resolve_batches(self, files: list[TorrentFile], kind: str, cap_bytes: int) -> list[Batch]:
        """Episode batches for seasons, file-group batches otherwise.

        Single shared rule used at QUEUED time and re-resolved during the
        download loop, so batch counts stay consistent: seasons split by
        episode, movie/unknown multi-file content splits name-sorted file
        groups. Single files, single episodes and empty lists yield no
        batches (full-torrent flow).
        """
        if kind in ("season", "mixed"):
            try:
                eps = list(classify(files, self.cfg).episodes)
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
            files = await self.dest_client.get_torrent_files(h)
        except Exception as e:
            log.warning("could not get torrent files for batches: %s", e)
            return None
        # Same rule as _do_queued so batch counts stay consistent across
        # the download loop.
        kind = ts.classification_kind
        if not kind or kind == "unknown":
            try:
                kind = classify(files, self.cfg).kind
            except Exception as e:
                log.warning("could not classify files for batches: %s", e)
                return None
        cap = self._frozen_batch_cap(ts)
        if cap <= 0:
            cap = sum(f.size_bytes for f in files) or 1
        try:
            return self._resolve_batches(files, kind, cap)
        except Exception as e:
            log.warning("could not make batches for %s: %s", ts.source_name, e)
            return None

    async def _move_and_clean_batch(
        self, ts: TorrentState, batch: Batch, skip: set[str] | None = None
    ) -> None:
        if batch is None:
            return
        save_path = ts.save_path or (
            str(self.cfg.dest.save_path) if hasattr(self, "cfg") and self.cfg else "/tmp"
        )
        src_dir = Path(save_path).resolve()
        remote = (
            self.cfg.rclone.remote.default
            if hasattr(self, "cfg") and self.cfg and ts.classification_kind in ("movie", "season")
            else (self.cfg.rclone.remote.unsorted if hasattr(self, "cfg") and self.cfg else "remote:")
        )
        log.info(
            "moving completed batch %d/%d for %s",
            ts.batch_index + 1, ts.batches_total, ts.source_name,
        )
        extra_flags = (
            self.cfg.rclone.batch_move_extra_flags
            if hasattr(self, "cfg") and hasattr(self.cfg.rclone, "batch_move_extra_flags")
            else None
        )
        skip_norm = {
            (n or "").replace("\\", "/").strip("/") for n in (skip or set())
        }
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
                ts.batch_index + 1, ts.batches_total, ts.source_name,
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
        rclone moves. A failed verification read still counts the pause
        itself as success.
        """
        err: Exception | None = None
        for attempt in range(3):
            try:
                await self.dest_client.pause(h)
                try:
                    cur = await self.dest_client.get_torrent(h)
                    state = getattr(cur, "state", "") if cur is not None else ""
                    if cur is None or self._client_reports_paused(state):
                        return True, None
                    err = RuntimeError(f"client still reports state={state!r} after pause")
                except Exception:
                    return True, None
            except Exception as e:  # noqa: BLE001
                err = e
            if attempt < 2:
                await asyncio.sleep(1)
        return False, err

    async def _batch_bytes_on_disk(self, ts: TorrentState, batch_files: list) -> bool:
        """True iff every expected batch file is present on SSD at full size.

        Offloaded to a thread (fuse-adjacent stat can block). A truncated or
        preallocated-but-incomplete file reads as missing — the caller keeps
        polling so the client re-fetches instead of moving air.
        """
        try:
            base = Path(ts.save_path) if ts.save_path else Path(self.cfg.dest.save_path)
        except Exception:
            return False

        def _check() -> bool:
            for f in batch_files:
                try:
                    name = getattr(f, "name", "") or ""
                    want = getattr(f, "size_bytes", 0) or 0
                    p = _safe_ssd_join(base, name)
                    if p is None or not p.is_file():
                        return False
                    if want and p.stat().st_size < want:
                        return False
                except OSError:
                    return False
            return True

        try:
            return await asyncio.to_thread(_check)
        except Exception:
            return False

    async def _do_downloading(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        if ts.batches_total <= 0 and hasattr(self, "dest_client"):
            batches = await self._get_batches_for_torrent(ts)
            if batches is None:
                # Transient RPC failure — not "single flow". Retry next tick
                # rather than persisting batches_total=0 as truth.
                log.warning("could not resolve batches for adopted %s; retry next tick",
                            ts.source_infohash[:10])
                self.store.upsert(ts)
                return
            ts.batches_total = len(batches)
            # Clamp a stale index (e.g. file list shrank since the cursors
            # were persisted) instead of degrading to full-torrent flow.
            if ts.batch_index >= len(batches) and batches:
                log.warning("clamping stale batch_index %d to %d for %s",
                            ts.batch_index, len(batches) - 1, ts.source_name)
                ts.batch_index = len(batches) - 1
            self.store.upsert(ts)
            # Freshly adopted rows (e.g. recovery after --reset wiped the batch
            # cursors) skip QUEUED setup, so no batch was ever prioritized and
            # the client may still select a stale batch. Restart at batch 0.
            try:
                await self._prepare_next_batch(ts)
            except Exception as e:  # noqa: BLE001
                log.warning("could not prioritize first batch for %s: %s",
                            ts.source_infohash[:10], e)
            try:
                await self.dest_client.resume(h)
            except Exception as e:  # noqa: BLE001
                log.warning("could not resume adopted torrent %s: %s", h[:10], e)

        is_batched = ts.batches_total > 1
        consecutive_batch_failures = 0
        max_batch_failures_per_tick = 5

        while not self._stop:
            # Fresh-state re-guard: this loop runs for hours across batch
            # resets — an operator forget (row gone) or API retry (row left
            # DOWNLOADING) must stop the worker instead of letting later
            # upserts resurrect or clobber the row. The worker keeps its own
            # object (rebinding to a fresh row would strand the caller's
            # state dispatch); all worker mutations are upserted immediately
            # so the two stay in sync.
            try:
                _fresh_dl = None
                if getattr(self, "store", None) is not None and hasattr(self.store, "get"):
                    _fresh_dl = self.store.get(ts.source_infohash, include_blob=False)
            except Exception:
                _fresh_dl = None
            if _fresh_dl is None:
                log.info(
                    "worker: row gone for %s (forgotten?); stopping download",
                    ts.source_infohash[:10],
                )
                return
            if isinstance(_fresh_dl, TorrentState) and _fresh_dl.state != State.DOWNLOADING:
                log.info(
                    "worker: %s left DOWNLOADING while downloading (%s); stopping",
                    ts.source_infohash[:10], _fresh_dl.state.value,
                )
                return
            # Live tracking, keyed by dest hash for client polling; the
            # source hash is recorded inside for progress-map aliasing.
            self._live[h.lower()] = LiveItem(
                source_infohash=(ts.source_infohash or "").lower(),
                name=ts.source_name,
                state="downloading",
                progress=0.0,
                size_mb=ts.total_bytes / (1024 * 1024),
            )

            cur_batch: Batch | None = None
            cur_skip: set[str] = set()
            expected_files: list[str] | None = None

            if is_batched and hasattr(self, "dest_client"):
                try:
                    batches = await self._get_batches_for_torrent(ts)
                    if batches is None:
                        # Transient RPC failure (not "no batches"): park this
                        # tick rather than degrading to a full-torrent wait
                        # that would defeat the SSD cap.
                        log.warning("batch resolution unavailable for %s; retry next tick",
                                    ts.source_name)
                        self.store.upsert(ts)
                        self._live.pop(h.lower(), None)
                        return
                    if batches and len(batches) != ts.batches_total:
                        # File list / cap drifted mid-run (tracker sidecar
                        # added, cap re-frozen): heal the total instead of
                        # comparing a stale index against a stale total.
                        log.warning("batch count drifted %d -> %d for %s; healing",
                                    ts.batches_total, len(batches), ts.source_name)
                        ts.batches_total = len(batches)
                        if ts.batch_index >= len(batches):
                            ts.batch_index = max(0, len(batches) - 1)
                        self.store.upsert(ts)
                    if batches and ts.batch_index < len(batches):
                        cur_batch = batches[ts.batch_index]
                        # Already on the remote from a previous run: neither
                        # waited on, downloaded, nor moved again.
                        cur_skip = await self._fuse_skipped(
                            [(e.file_name, e.size_bytes) for e in cur_batch.episodes],
                            ts.classification_kind or "unknown",
                        )
                        expected_files = [
                            ep.file_name for ep in cur_batch.episodes
                            if ep.file_name not in cur_skip
                        ]
                except Exception as e:
                    log.warning("could not resolve batches for %s: %s", ts.source_name, e)

            try:
                await self._wait_for_completion(ts, expected_files=expected_files)
            except TimeoutError as e:
                # Stalled swarm, not a dead torrent: park in DOWNLOADING for
                # retry instead of FAILED (transient lulls heal; the stall
                # baseline restarts next tick for a fresh window).
                log.warning("download stalled for %s (%s); parking", ts.source_name, e)
                self.store.upsert(ts)
                return
            finally:
                self._live.pop(h.lower(), None)

            if self._stop:
                return

            if is_batched:
                if cur_batch is not None:
                    if hasattr(self, "dest_client"):
                        paused_ok, pause_err = await self._pause_verified(h)
                        if not paused_ok:
                            # Never move while qB is still writing — retry
                            # shortly instead of corrupting the remote.
                            # Bound per-tick retries so one wedged torrent
                            # cannot pin this worker (and its tick) forever;
                            # state stays DOWNLOADING for retry next tick.
                            consecutive_batch_failures += 1
                            log.warning(
                                "could not pause torrent %s before batch move (%s); retrying shortly (%d/%d)",
                                h[:10], pause_err,
                                consecutive_batch_failures, max_batch_failures_per_tick,
                            )
                            if consecutive_batch_failures >= max_batch_failures_per_tick:
                                log.warning(
                                    "parking %s in DOWNLOADING after %d batch-pause failures",
                                    ts.source_name, consecutive_batch_failures,
                                )
                                self.store.upsert(ts)
                                self._live.pop(h.lower(), None)
                                return
                            await asyncio.sleep(5)
                            continue
                    try:
                        await self._move_and_clean_batch(ts, cur_batch, skip=cur_skip)
                    except RcloneTimeoutError as e:
                        # Hung remote mid-batch: child terminated, bytes
                        # intact — park in DOWNLOADING for next tick at once.
                        # (Must NOT funnel into the incomplete-move retry
                        # below: each attempt would block for the full
                        # timeout again.)
                        log.error("rclone batch move timed out for %s: %s",
                                  ts.source_name[:60], e)
                        try:
                            ts.last_error = f"batch move timed out: {e}"[:500]
                        except Exception:
                            pass
                        self.store.upsert(ts)
                        self._live.pop(h.lower(), None)
                        return
                    except BatchMoveIncompleteError as e:
                        # Same-tick retry like the pause failure above: the
                        # batch is still fully on local disk, nothing advanced.
                        # Bound it for the same reason — never wedge the worker.
                        consecutive_batch_failures += 1
                        log.warning(
                            "batch move %d/%d for %s incomplete (%s); retrying shortly (%d/%d)",
                            ts.batch_index + 1, ts.batches_total, ts.source_name, e,
                            consecutive_batch_failures, max_batch_failures_per_tick,
                        )
                        if consecutive_batch_failures >= max_batch_failures_per_tick:
                            log.warning(
                                "parking %s in DOWNLOADING after %d incomplete batch moves",
                                ts.source_name, consecutive_batch_failures,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        await asyncio.sleep(5)
                        continue
                    consecutive_batch_failures = 0
                    next_index = ts.batch_index + 1
                    if next_index < ts.batches_total:
                        # Isolated batches: fresh delete(with-files) + re-add
                        # so next batch downloads from zero with no shared-piece
                        # partials. Only verified-complete files ever moved.
                        # Index advances only after reset succeeds; a transient
                        # failure replays this already-moved batch as a no-op
                        # via _fuse_skipped next tick.
                        try:
                            reset_pos = await self._reset_torrent_for_next_batch(ts, next_index)
                        except _WEBUI_RETRY_ERRORS as e:
                            log.warning(
                                "isolated batch reset hit transient dest error for %s (%s); retry next tick",
                                ts.source_name, e,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        if reset_pos is False or reset_pos is None:
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        # Position contract: int (possibly reconciled on
                        # grouping shrink), or legacy True from test doubles.
                        ts.batch_index = reset_pos if isinstance(reset_pos, int) else next_index
                        self.store.upsert(ts)
                        # dest hash may have changed on re-add; refresh live key.
                        try:
                            new_h = ts.dest_infohash or ts.source_infohash
                            if new_h.lower() != h.lower():
                                self._live.pop(h.lower(), None)
                                h = new_h
                        except Exception:
                            pass
                    else:
                        ts.batch_index = next_index
                        self.store.upsert(ts)
                    # Tighten the SSD reservation as batches land on the
                    # remote: only remaining work should block waiters (a
                    # 32 GB season with 20 GB already moved must not pin
                    # 32 GB). Skipped here on the final batch: its bytes
                    # are still on SSD until the MOVING wipe releases them.
                    if ts.batch_index < ts.batches_total:
                        try:
                            _rem = await self._remaining_batch_footprint(
                                batches, ts.classification_kind or "unknown")
                            if _rem is not None:
                                await self._ssd_adjust(
                                    ts.source_infohash, int(_rem))
                        except Exception:
                            pass
                elif hasattr(self, "_move_and_clean_batch") and hasattr(self._move_and_clean_batch, "mock_calls"):
                    # Mock in unit test (e.g. AsyncMock)
                    await self._move_and_clean_batch(ts, None)  # type: ignore[arg-type]
                    ts.batch_index += 1
                    self.store.upsert(ts)
                elif not hasattr(self, "dest_client"):
                    # Test dummy without dest_client attached
                    ts.batch_index += 1
                    self.store.upsert(ts)
                else:
                    # Reachable only when re-resolution yields genuinely no
                    # batches (kind flipped single mid-run): fall through to
                    # MOVING for a full move instead of failing after
                    # successful downloads. (RPC-transient None parks above.)
                    log.warning(
                        "current batch %d could not be resolved for %s; "
                        "falling back to full move",
                        ts.batch_index, ts.source_name,
                    )
                    break

                if ts.batch_index < ts.batches_total:
                    # Next batch was already prioritized + resumed inside the
                    # isolated reset above; loop back to wait for it.
                    continue

            break

        self._live.pop(h.lower(), None)
        if not self._stop:
            self.transition(ts, State.MOVING)

    async def _wait_for_completion(
        self, ts: TorrentState, expected_files: list[str] | None = None
    ) -> None:
        h = ts.dest_infohash or ts.source_infohash
        last_log = 0.0
        last_progress = 0.0
        last_progress_time = time.monotonic()
        last_stall_warn = 0.0
        stall_timeout = (
            getattr(self.cfg.general, "download_stall_timeout_seconds", 0)
            if hasattr(self, "cfg") and hasattr(self.cfg, "general")
            else 0
        )
        poll_interval = (
            self.cfg.general.dest_poll_interval
            if hasattr(self, "cfg") and hasattr(self.cfg, "general")
            else 2
        )

        if expected_files is not None and not expected_files:
            # Whole batch already on the remote: nothing to wait for.
            return

        while not self._stop:
            t = await self.dest_client.get_torrent(h)
            if t is None:
                # Vanished entry + vanished row = the operator abandoned
                # this torrent mid-download (forget/cancel): unwind quietly
                # so the worker wrapper doesn't resurrect it as FAILED.
                # Vanished entry + live row = real client-side loss: fail.
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
                        "worker: row gone for %s; stopping download",
                        ts.source_infohash[:10],
                    )
                    raise AbandonedError(
                        f"row forgotten while downloading: {h[:10]}")
                raise RuntimeError(f"torrent vanished mid-download: {h}")

            if expected_files is not None:
                files = await self.dest_client.get_torrent_files(h)
                f_map = {f.name: f for f in files}
                missing_files = [fn for fn in expected_files if fn not in f_map]
                if missing_files:
                    # No `and files` guard: an empty file list this far into
                    # DOWNLOADING is pathological (registration lag only
                    # applies right after add) — fail fast instead of polling
                    # a torrent whose file list can never satisfy the batch.
                    raise RuntimeError(
                        f"expected batch files missing from torrent {ts.source_name}: {missing_files}"
                    )
                batch_files = [f_map[fn] for fn in expected_files if fn in f_map]
                total_sz = sum(f.size_bytes for f in batch_files)
                done_sz = sum(f.size_bytes * f.progress for f in batch_files)
                prog = done_sz / total_sz if total_sz > 0 else 0.0
                # Zero-size files carry no bytes: presence on SSD (verified
                # below) is completion for them, not client progress.
                all_done = (
                    len(batch_files) == len(expected_files)
                    and all((f.progress or 0.0) >= 0.999 or not (f.size_bytes or 0)
                            for f in batch_files)
                )
                if all_done and not await self._batch_bytes_on_disk(ts, batch_files):
                    # Client reports complete but bytes are not on SSD
                    # (desynced piece map after a move deleted files): keep
                    # polling so the client re-fetches instead of moving air.
                    log.warning(
                        "batch %d for %s reports complete but bytes missing on SSD; waiting",
                        ts.batch_index + 1, ts.source_name,
                    )
                    all_done = False
            else:
                prog = t.progress
                all_done = t.is_complete()

            if h.lower() in self._live:
                self._live[h.lower()].progress = prog

            now = time.monotonic()
            if now - last_log > 60:
                log.info(
                    "download %s: %.1f%% (%d MB)",
                    ts.source_name,
                    prog * 100,
                    t.size_bytes // (1024 * 1024),
                )
                last_log = now

            if prog > last_progress:
                last_progress = prog
                last_progress_time = now
            elif prog < 0.999:
                stalled_for = now - last_progress_time
                if stall_timeout > 0 and stalled_for > stall_timeout:
                    raise TimeoutError(
                        f"download {ts.source_name} stalled at {prog * 100:.1f}% for {int(stalled_for)}s"
                    )
                if stalled_for > 900 and now - last_stall_warn > 900:
                    log.warning(
                        "download %s may be stalled: progress has remained at %.1f%% for %dm",
                        ts.source_name,
                        prog * 100,
                        int(stalled_for / 60),
                    )
                    last_stall_warn = now

            if all_done:
                return
            await asyncio.sleep(poll_interval)

    async def _prepare_next_batch(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        try:
            files = await self.dest_client.get_torrent_files(h)
        except Exception as e:
            log.warning("could not get torrent files for next batch: %s", e)
            return
        try:
            kind = classify(files, self.cfg).kind
        except Exception as e:
            log.warning("could not classify files for next batch: %s", e)
            return
        cap = self._frozen_batch_cap(ts)
        if cap <= 0:
            cap = sum(f.size_bytes for f in files) or 1
        try:
            batches = self._resolve_batches(files, kind, cap)
        except Exception as e:
            log.warning("could not make batches for next batch: %s", e)
            return
        if not batches:
            return
        if ts.batch_index >= len(batches):
            return
        cur = batches[ts.batch_index]
        skip = await self._fuse_skipped(
            [(e.file_name, e.size_bytes) for e in cur.episodes], kind,
        )
        prio_map = {f.name: 0 for f in files}
        for ep in cur.episodes:
            if ep.file_name not in skip:
                prio_map[ep.file_name] = 1
        await self.dest_client.set_file_priorities(h, prio_map)

    async def _reset_torrent_for_next_batch(self, ts: TorrentState, next_index: int) -> int | bool:
        """Delete + fresh re-add for isolated per-batch downloads.

        After batch N is verified moved (straggler check passed), the old
        torrent entry holds deselected partials of batch N+1 (shared pieces)
        plus preallocated files. Flipping priorities in place can leave the
        next batch's first file permanently partial. A fresh
        ``delete(delete_files=True) + add`` wipes those partials so the next
        batch downloads from zero and only verified-complete files ever reach
        ``rclone move`` — guaranteeing 100% of bytes land on the remote.

        Crash-safe: batch_index is only advanced by the caller AFTER this
        returns a position. A crash before then replays the already-moved
        batch, which is a no-op via _fuse_skipped.

        Returns the batch position the caller must record, or False on
        transient failure (stay DOWNLOADING, retry next tick). When the
        re-resolved grouping shrank (kind flip) past ``next_index``, the
        total/index are reconciled to the finished position instead of
        parking forever.
        """
        h_old = ts.dest_infohash or ts.source_infohash
        blob = getattr(ts, "_blob", None) or ts.cross_seed_blob or None
        if blob is None or (isinstance(blob, (bytes, bytearray)) and len(blob) == 0):
            try:
                fetched = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                fetched = None
            # Test doubles may return non-bytes MagicMocks — accept truthy
            # values, reject only None/empty-bytes (real missing case).
            if fetched is None or (isinstance(fetched, (bytes, bytearray)) and len(fetched) == 0):
                pass
            else:
                blob = fetched
                try:
                    ts.cross_seed_blob = blob if isinstance(blob, (bytes, bytearray)) else ts.cross_seed_blob
                    ts._blob = blob
                except Exception:
                    pass
        if blob is None or (isinstance(blob, (bytes, bytearray)) and len(blob) == 0):
            log.warning(
                "isolated batch: missing .torrent bytes for %s; keeping batch %d to retry",
                ts.source_name, ts.batch_index,
            )
            return False
        save_path_str = ts.save_path or str(self.cfg.dest.save_path)

        # 1. Remove old entry WITH files: clears deselected partials and
        # preallocated leftovers of the next batch. Already-moved batch files
        # are gone from SSD (on remote), so only unwanted partials are lost.
        try:
            await self.dest_client.delete(h_old, delete_files=True)
            log.info(
                "isolated batch: removed torrent %s with files after batch %d/%d",
                h_old[:10], ts.batch_index + 1, ts.batches_total,
            )
        except Exception as e:  # noqa: BLE001
            # Not-found or transient: continue to add — add handles
            # "already added" duplicates, next tick retries on failure.
            log.warning(
                "isolated batch: delete %s with files failed (%s); continuing to re-add",
                h_old[:10], e,
            )

        # 2. Fresh add, paused, no skip-check (clean download of next batch).
        try:
            result = await self.dest_client.add_torrent(
                torrent_files=[blob],
                save_path=save_path_str,
                category="racing",
                paused=True,
                skip_check=False,
            )
        except _WEBUI_RETRY_ERRORS as e:
            log.warning(
                "isolated batch: re-add for %s hit transient dest error (%s); retry next tick",
                ts.source_name, e,
            )
            return False
        except Exception as e:  # noqa: BLE001
            log.warning(
                "isolated batch: re-add for %s failed (%s); retry next tick",
                ts.source_name, e,
            )
            return False
        if not result.accepted:
            log.warning(
                "isolated batch: re-add rejected for %s (%s); retry next tick",
                ts.source_name, result.detail,
            )
            return False

        # 3. Resolve new hash (same torrent → usually same hash).
        # Test doubles may return non-str hashes — only accept real strings,
        # otherwise keep the old hash (re-added same torrent).
        new_hash: str | None = None
        try:
            _rh = result.hash
            if isinstance(_rh, str) and _rh:
                new_hash = _rh.lower()
        except Exception:
            new_hash = None
        if not new_hash and isinstance(blob, (bytes, bytearray)):
            try:
                from .watchdir import _bencoded_info_hash

                parsed_hash, _, _, _ = _bencoded_info_hash(bytes(blob))
                if isinstance(parsed_hash, str) and parsed_hash:
                    new_hash = parsed_hash.lower()
            except Exception:
                new_hash = None
        if not new_hash and isinstance(blob, (bytes, bytearray)):
            try:
                awaited = await self._await_hash_for_name(ts.source_name)
                if isinstance(awaited, str) and awaited:
                    new_hash = awaited.lower()
            except Exception:
                new_hash = None
        if new_hash:
            ts.dest_infohash = new_hash
            try:
                self.store.upsert(ts)
            except Exception:
                pass
        h_new = ts.dest_infohash or ts.source_infohash

        # 4. Wait for file list (registration lag) + prioritize next batch only.
        files: list[TorrentFile] = []
        for _ in range(4):
            try:
                files = await self.dest_client.get_torrent_files(h_new)
                if files:
                    break
            except Exception:
                files = []
            await asyncio.sleep(2)
        if not files:
            log.warning(
                "isolated batch: file list not ready for %s after re-add; retry next tick",
                ts.source_name,
            )
            return False
        try:
            kind = ts.classification_kind or classify(files, self.cfg).kind
        except Exception:
            kind = ts.classification_kind or "unknown"
        cap = self._frozen_batch_cap(ts)
        if cap <= 0:
            cap = sum(f.size_bytes for f in files) or 1
        try:
            batches = self._resolve_batches(files, kind, cap)
        except Exception as e:  # noqa: BLE001
            log.warning("isolated batch: could not resolve batches for %s: %s", ts.source_name, e)
            return False
        if not batches:
            log.warning(
                "isolated batch: no batches resolvable for %s; retry next tick",
                ts.source_name,
            )
            return False
        if next_index >= len(batches):
            # Grouping shrank under us (kind flip / sidecar change) and the
            # requested batch no longer exists: everything resolvable is done.
            # Reconcile cursors to finished instead of parking forever.
            log.warning(
                "isolated batch: grouping shrank to %d batches for %s; finishing",
                len(batches), ts.source_name,
            )
            ts.batches_total = len(batches)
            ts.batch_index = len(batches)
            try:
                self.store.upsert(ts)
            except Exception:
                pass
            return len(batches)
        nxt = batches[next_index]
        try:
            skip = await self._fuse_skipped(
                [(e.file_name, e.size_bytes) for e in nxt.episodes], kind,
            )
        except Exception:
            skip = set()
        wanted = {e.file_name for e in nxt.episodes} - skip
        prio_map = {f.name: (1 if f.name in wanted else 0) for f in files}
        try:
            await self.dest_client.set_file_priorities(h_new, prio_map)
        except Exception as e:  # noqa: BLE001
            log.warning("isolated batch: priority set failed for %s: %s", ts.source_name, e)
            return False
        if wanted:
            try:
                await self.dest_client.resume(h_new)
            except Exception as e:  # noqa: BLE001
                log.warning("isolated batch: resume failed for %s: %s", ts.source_name, e)
                return False
        log.info(
            "isolated batch: ready for batch %d/%d for %s (%d file(s) wanted)",
            next_index + 1, ts.batches_total, ts.source_name, len(wanted),
        )
        return next_index

    def _season_folder_for(
        self,
        files: list,
        torrent_name: str,
        *,
        base_path: str | Path | None = None,
    ) -> Path | None:
        if not files:
            return None
        base = (
            Path(base_path).resolve()
            if base_path
            else Path(self.cfg.dest.save_path).resolve()
        )
        # The top-most folder shared by all files. Derived from ALL files
        # (most common first component), not files[0] — qB file order is
        # not guaranteed and files[0] may be a root-level extra while the
        # pack lives under Top/ (returning None then falls back to a bare
        # folder move that strips the top dir on the remote).
        from collections import Counter
        tops: Counter[str] = Counter()
        for f in files:
            parts = (f.name or "").replace("\\", "/").split("/")
            if len(parts) > 1:
                top = parts[0].strip()
                if top and top not in (".", "..") and ".." not in top:
                    tops[top] += 1
        if not tops:
            return None
        top = tops.most_common(1)[0][0]
        if all(f.name.replace("\\", "/").startswith(top + "/") for f in files):
            candidate = (base / top).resolve()
            if candidate != base and candidate.is_relative_to(base):
                return candidate
        return None

    # ---- state: MOVING ----

    def _park_moving(self, ts: TorrentState, reason: str) -> None:
        """Stay in MOVING for retry next tick, recording WHY (stall diagnosis).

        Every _do_moving early return funnels here instead of a bare
        upsert: the park reason lands in last_error (cleared by transition()
        on advance, visible via API/DB meanwhile) and consecutive parks for
        the same row escalate from WARNING to ERROR so a gate that never
        passes (unverifiable pause, 0-transfer rclone, short bytes) can't
        idle silently as plain "MOVING" forever. Raises AbandonedError when
        the row was forgotten mid-flight (callers unwind quietly instead of
        resurrecting it via the park upsert).
        """
        if self._abandoned(ts):
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                "not parking in MOVING"
            )
        key = (ts.source_infohash or "").lower()
        try:
            parks = getattr(self, "_moving_parks", None)
            if not isinstance(parks, dict):
                parks = {}
                self._moving_parks = parks
            n = int(parks.get(key, 0) or 0) + 1
            parks[key] = n
            if len(parks) > 5000:
                for _k in list(parks.keys())[: len(parks) - 5000]:
                    parks.pop(_k, None)
        except Exception:
            n = 1
        try:
            ts.last_error = f"moving parked ({n}x): {reason}"[:500]
        except Exception:
            pass
        try:
            self.store.upsert(ts)
        except Exception:  # noqa: BLE001
            pass
        if n >= 5:
            log.error(
                "MOVING stalled for %s: %s (parked %dx, still retrying)",
                ts.source_name[:60], reason, n,
            )
        else:
            log.warning(
                "staying in MOVING for %s: %s",
                ts.source_name[:60], reason,
            )

    def _single_on_fuse(self, files: list, single_file: str, ts) -> bool:
        """True iff an already-remote single file needs no SSD download/move.

        Size-verified against the kind-appropriate fuse mount; anything
        unstatable answers False (safe direction: download it).
        """
        safe = _safe_ssd_join(Path("."), single_file)
        if safe is None:
            return False
        norm = str(safe)
        if not norm or norm == ".":
            return False
        match = next(
            (f for f in files
             if str(_safe_ssd_join(Path("."), getattr(f, "name", "") or "") or "") == norm),
            None,
        )
        if match is None:
            return False
        try:
            mount = self._target_mount_for(ts)
        except Exception:
            return False
        try:
            target = _safe_ssd_join(mount, norm)
            if target is None:
                return False
            return target.stat().st_size == (match.size_bytes or 0)
        except OSError:
            return False

    async def _do_moving(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        try:
            cls_files = await self.dest_client.get_torrent_files(h)
        except _WEBUI_RETRY_ERRORS as e:
            # Every other phase parks on transient client trouble; a single
            # qB timeout here must not fail a fully-downloaded row.
            self._park_moving(
                ts,
                f"could not list files for {h[:10]} before move "
                f"({e}); will retry on next tick",
            )
            return
        cls = classify(cls_files, self.cfg)
        # Pin the QUEUED-time classification: only fill when unknown. A fresh
        # file list (tracker sidecar added, metadata completed) can flip
        # episode<->mixed and would otherwise reroute the remote and branch
        # mid-flight. Layout below still follows the fresh list; routing
        # follows the pinned kind.
        pinned_kind = ts.classification_kind or "unknown"
        if (not pinned_kind or pinned_kind == "unknown") and cls.kind:
            ts.classification_kind = cls.kind
            pinned_kind = cls.kind
            try:
                self.store.upsert(ts)
            except Exception:  # noqa: BLE001
                pass
        elif cls.kind and cls.kind != pinned_kind:
            log.warning(
                "classification flipped %s -> %s for %s after queue; keeping pinned %s for routing",
                pinned_kind, cls.kind, ts.source_name, pinned_kind,
            )
        # Branch the move on the PINNED kind (falling back to the fresh one
        # when nothing was ever pinned): batches were downloaded and routed
        # under it, so a mid-flight flip (tracker sidecar added, metadata
        # completed) must not reroute the move or rewrite batch cursors via
        # the mixed branch. Fresh `cls` still supplies the file layout
        # (episodes/single_file/sizes) below.
        branch_kind = pinned_kind if pinned_kind not in ("", "unknown") else cls.kind

        # 1. Pause torrent on VPS2 client BEFORE move begins to stop active seeding from SSD
        log.info("pausing torrent %s on VPS2 client before move", h[:10])
        paused, pause_err = await self._pause_verified(h)
        if not paused:
            # Never move while the client is still writing, but don't fail
            # terminally on a transient WebUI hiccup — stay in MOVING so the
            # next tick retries (preserves downloaded bytes on SSD).
            self._park_moving(
                ts,
                f"could not pause torrent {h[:10]} before move "
                f"({pause_err}); will retry on next tick",
            )
            return

        # 2. Separate completed files from incomplete piece-boundary files
        src_dir = (
            Path(ts.save_path).resolve()
            if ts.save_path
            else Path(self.cfg.dest.save_path).resolve()
        )
        folder = self._season_folder_for(
            cls_files, ts.source_name, base_path=src_dir
        )

        completed_files: list[TorrentFile] = []
        incomplete_files: list[TorrentFile] = []
        uncertain_files: list[TorrentFile] = []

        missing_on_disk: list[str] = []
        for f in cls_files:
            file_path = _safe_ssd_join(src_dir, f.name)
            if file_path is None:
                continue
            if not file_path.exists():
                # Not in any set below: record loudly so a desynced piece map
                # (files deleted from disk, client still complete) can't slip
                # silently into wipe + RE_ADDING.
                missing_on_disk.append(f.name)
                continue
            # Client-verified complete AND present on disk at full size: the
            # ONLY set ever moved to remote.
            # NOTE: on-disk size alone must NOT mark completeness — qBittorrent
            # pre-allocates deselected files at full size, so a piece-boundary
            # partial of a deselected episode looks "full" while its progress
            # is < 1. Moving it would upload corrupt data (and could overwrite
            # an older batch's moved file). Progress is authoritative, disk
            # size is the second gate (a 1.0-progress truncated file must not
            # move either).
            if (f.progress or 0.0) >= 0.999:
                try:
                    _on_disk = file_path.stat().st_size if file_path.is_file() else -1
                except OSError:
                    _on_disk = -1
                if _on_disk == (f.size_bytes or 0):
                    completed_files.append(f)
                    continue
                if _on_disk > (f.size_bytes or 0):
                    # Bigger than the torrent metadata says: foreign bytes
                    # (shared save_path collision), never move nor delete
                    # individually — same handling as preallocated partials.
                    log.warning(
                        "file %s oversized on SSD (%d/%d B); leaving for folder wipe, never moving",
                        f.name, max(_on_disk, 0), f.size_bytes,
                    )
                    uncertain_files.append(f)
                    continue
                log.warning(
                    "file %s reports complete but is short on SSD (%d/%d B); not moving",
                    f.name, max(_on_disk, 0), f.size_bytes,
                )
                incomplete_files.append(f)
                continue
            try:
                on_disk = file_path.stat().st_size
            except OSError:
                continue
            if on_disk < (f.size_bytes or 0):
                incomplete_files.append(f)
            else:
                # Preallocated-but-incomplete (or laggy progress): move nothing
                # and delete nothing individually — left for the folder wipe.
                uncertain_files.append(f)
        if uncertain_files:
            log.info(
                "leaving %d unverified file(s) for folder wipe (not individually "
                "deleted, never moved): e.g. %s",
                len(uncertain_files), uncertain_files[0].name,
            )
        if missing_on_disk:
            log.warning(
                "move found %d file(s) missing on SSD (e.g. %s); they move nowhere "
                "and the fuse gate must still pass before re-add",
                len(missing_on_disk), missing_on_disk[0],
            )

        # 3. Clean up incomplete piece-boundary files so they are NOT moved to remote
        for f in incomplete_files:
            file_path = _safe_ssd_join(src_dir, f.name)
            if file_path is None:
                continue
            # Never delete outside src_dir (traversal guard above) and never
            # follow symlinks out of the SSD tree.
            try:
                if file_path.is_symlink():
                    log.warning("refusing to delete symlink outside SSD tree: %s", f.name)
                    continue
                resolved = file_path.resolve()
                if resolved != src_dir.resolve() and not resolved.is_relative_to(src_dir.resolve()):
                    log.warning("refusing to delete outside SSD tree: %s", f.name)
                    continue
            except OSError:
                continue
            if file_path.exists():
                log.info(
                    "removing incomplete piece-boundary file: %s (%d/%d B)",
                    f.name, file_path.stat().st_size, f.size_bytes,
                )
                try:
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path, ignore_errors=True)
                except OSError as e:
                    log.warning("failed removing incomplete file %s: %s", f.name, e)

        # Also purge any leftover temporary extension files like .!qB or .parts for this torrent
        if folder and folder.exists() and folder.resolve() != src_dir.resolve():
            for temp_file in list(folder.glob("**/*.!qB")) + list(folder.glob("**/*.parts")):
                try:
                    temp_file.unlink()
                except OSError:
                    pass
        else:
            # Scoped to torrent-owned files only to avoid deleting concurrent downloads in shared save_path
            for f in cls_files:
                base_file = _safe_ssd_join(src_dir, f.name)
                if base_file is None:
                    continue
                for ext in (".!qB", ".parts"):
                    temp_file = base_file.parent / f"{base_file.name}{ext}"
                    try:
                        resolved_tmp = temp_file.resolve()
                        if resolved_tmp != src_dir.resolve() and not resolved_tmp.is_relative_to(src_dir.resolve()):
                            continue
                    except OSError:
                        continue
                    if temp_file.exists():
                        try:
                            temp_file.unlink()
                        except OSError:
                            pass

        # 4. Decide target remote from the PINNED kind (see above): batches
        # were moved under it, so the sweep must land beside them even if the
        # fresh file list classifies differently.
        if pinned_kind in ("movie", "season"):
            remote = self.cfg.rclone.remote.default
        else:
            remote = self.cfg.rclone.remote.unsorted

        # 5. Move completed files via rclone
        # (per-file --files-from-raw lists preserve the top dir so the
        # remote layout matches torrent-relative names.)
        if ts.batches_total > 1:
            _top_join = _safe_ssd_join(src_dir, ts.source_name)
            _top_exists = _top_join is not None and _top_join.exists()
            local_folder = folder if (folder and folder.exists()) else (_top_join if _top_exists else None)
            remaining_payload: list[Path] = []
            if local_folder and local_folder.exists() and local_folder.resolve() != src_dir.resolve():
                # Bounded, symlink-safe walk: rglob follows directory symlinks
                # (escape + loop risk), so skip links, contain results to
                # the folder, and cap the listing.
                try:
                    _scan = list(local_folder.rglob("*"))[:100000]
                    _folder_real = local_folder.resolve()
                except OSError:
                    _scan = []
                    _folder_real = None
                for p in _scan:
                    try:
                        if p.is_symlink():
                            continue
                        if not p.is_file() or p.name.endswith((".!qB", ".parts")):
                            continue
                        if _folder_real is not None and p.resolve().is_relative_to(_folder_real):
                            remaining_payload.append(p)
                    except OSError:
                        continue

            if ts.batch_index < ts.batches_total or remaining_payload:
                if local_folder and local_folder.exists():
                    # Move ONLY client-verified-complete leftovers via an exact
                    # --files-from-raw list (same mechanism batch moves use,
                    # preserving torrent-relative paths). Deselected files share
                    # pieces with batch episodes and sit preallocated-but-
                    # incomplete: a bare `<top>/**` move would upload that corrupt
                    # data (and could overwrite an older batch's moved file).
                    try:
                        top_rel = local_folder.resolve().relative_to(src_dir.resolve())
                        top = top_rel.parts[0] if top_rel.parts else ""
                    except Exception:
                        top = ""
                    leftover_names: list[str] = []
                    for f in completed_files:
                        joined = _safe_ssd_join(src_dir, f.name or "")
                        if joined is None:
                            continue
                        try:
                            norm = str(joined.relative_to(src_dir)).replace("\\", "/")
                        except ValueError:
                            continue
                        if not norm:
                            continue
                        if top and not (norm == top or norm.startswith(top + "/")):
                            continue
                        if joined.is_file():
                            leftover_names.append(norm)
                    leftover_files = files_from_names(leftover_names)
                    if not leftover_files:
                        log.info(
                            "multi-batch torrent %s: no verified-complete leftovers to move "
                            "(%d remaining file(s) are incomplete boundary data); skipping remote move",
                            ts.source_name, len(remaining_payload),
                        )
                    else:
                        log.info(
                            "multi-batch torrent %s: moving %d verified-complete leftover file(s) "
                            "(batch %d/%d, %d remaining file(s) total)",
                            ts.source_name, len(leftover_files),
                            ts.batch_index, ts.batches_total, len(remaining_payload),
                        )
                        await self._rclone_move(src_dir, remote, ts, files_from=leftover_files)
                        # Same 0-transfer hazard as batch moves (rclone exits 0
                        # even when it transferred nothing): proceeding to the
                        # folder wipe below would destroy unmoved data. Stay MOVING.
                        stuck = [n for n in leftover_files if _left_on_disk(src_dir, n)]
                        if stuck:
                            self._park_moving(
                                ts,
                                f"leftover sweep moved nothing "
                                f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                                f"staying in MOVING without wiping",
                            )
                            return
                else:
                    log.info(
                        "multi-batch torrent %s: batches already moved (no remaining local content)",
                        ts.source_name,
                    )
            else:
                log.info(
                    "multi-batch torrent %s: batches were already moved during downloading stage",
                    ts.source_name,
                )
        elif branch_kind in ("movie", "episode", "season", "unknown"):
            local: Path | None
            if branch_kind in ("movie", "episode") and cls.single_file:
                local = _safe_ssd_join(src_dir, cls.single_file)
                if local is None or not local.exists():
                    if folder is not None:
                        _alt = _safe_ssd_join(folder, cls.single_file)
                        if _alt is not None and _alt.exists():
                            local = _alt
                        elif self._single_on_fuse(cls_files, cls.single_file, ts):
                            log.info("single file %s already on remote; skipping move",
                                     cls.single_file)
                            local = None
                        else:
                            raise FileNotFoundError(f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}")
                    elif self._single_on_fuse(cls_files, cls.single_file, ts):
                        log.info("single file %s already on remote; skipping move",
                                 cls.single_file)
                        local = None
                    else:
                        raise FileNotFoundError(f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}")
            elif branch_kind in ("season", "unknown") or (branch_kind == "movie" and not cls.single_file):
                _src_top = _safe_ssd_join(src_dir, ts.source_name)
                if folder and folder.exists():
                    local = folder
                elif _src_top is not None and _src_top.exists():
                    local = _src_top
                else:
                    raise FileNotFoundError(
                        f"completed {cls.kind} content not found on SSD: "
                        f"neither {folder} nor {src_dir}/{ts.source_name} exists"
                    )
                # Verified-complete per-file move (same guarantee as the sweep
                # above): a bare `<top>/**` move would also upload
                # preallocated-but-incomplete boundary files of content that
                # never needed downloading because it is already remote.
                # Torrent-relative names keep the remote/fuse layout identical.
                try:
                    top_rel = local.resolve().relative_to(src_dir.resolve())
                    top = top_rel.parts[0] if top_rel.parts else ""
                except Exception:
                    top = ""
                folder_names: list[str] = []
                for f in completed_files:
                    joined = _safe_ssd_join(src_dir, f.name or "")
                    if joined is None:
                        continue
                    try:
                        norm = str(joined.relative_to(src_dir)).replace("\\", "/")
                    except ValueError:
                        continue
                    if not norm:
                        continue
                    if top and not (norm == top or norm.startswith(top + "/")):
                        continue
                    if joined.is_file():
                        folder_names.append(norm)
                folder_names = files_from_names(folder_names)
                if folder_names:
                    await self._rclone_move(src_dir, remote, ts, files_from=folder_names)
                    # A 0-transfer folder move must not proceed to the wipe below.
                    stuck = [n for n in folder_names if _left_on_disk(src_dir, n)]
                    if stuck:
                        self._park_moving(
                            ts,
                            f"folder move left {len(stuck)} file(s) on disk "
                            f"(e.g. {stuck[0]}); staying in MOVING without wiping",
                        )
                        return
                else:
                    # Single-flow with nothing verified: either already remote
                    # (safe to finish) or an incomplete download that advanced
                    # prematurely on torrent-level progress. The latter must
                    # NOT proceed to the folder wipe below — resume so the
                    # remaining bytes can finish, same self-heal as singles.
                    if not completed_files and (incomplete_files or uncertain_files):
                        resumed = False
                        try:
                            await self.dest_client.resume(h)
                            resumed = True
                        except Exception as e:  # noqa: BLE001
                            log.warning(
                                "could not resume incomplete folder %s for %s: %s",
                                local, ts.source_name, e,
                            )
                        log.warning(
                            "folder %s for %s has no verified-complete files "
                            "(%d incomplete, %d unverified); %s staying in "
                            "MOVING without moving",
                            local, ts.source_name,
                            len(incomplete_files), len(uncertain_files),
                            "resumed to finish downloading," if resumed else "resume failed,",
                        )
                        self._park_moving(
                            ts,
                            f"folder {ts.source_name} not verified complete "
                            f"({len(incomplete_files)} incomplete, "
                            f"{len(uncertain_files)} unverified); "
                            f"{'resumed to finish, ' if resumed else ''}"
                            f"staying in MOVING without moving",
                        )
                        return
                    log.info(
                        "folder %s for %s has no verified-complete files to move "
                        "(already remote or incomplete boundary data)",
                        local, ts.source_name,
                    )
                local = None
            else:
                cand = _safe_ssd_join(src_dir, ts.source_name)
                if cand is not None and cand.exists():
                    local = cand
                else:
                    raise FileNotFoundError(f"completed content not found on SSD: {src_dir}/{ts.source_name}")
            if (
                branch_kind in ("movie", "episode")
                and cls.single_file
                and isinstance(local, Path)
                and local.is_file()
            ):
                # Single files move below via a bare `rclone move`, which —
                # unlike the folder/season branches — never consults the
                # verified-complete set above. Refuse to move
                # client-unverified bytes: a partial file on the remote is
                # worse than waiting. Park in MOVING for retry next tick
                # (nothing is wiped).
                #
                # Self-heal: DOWNLOADING gates on torrent-level progress
                # (>=0.999), so a 99.9% torrent can advance while its single
                # file is still at 99.x% or short on disk. _do_moving pauses
                # at the top, which would freeze those last pieces forever.
                # Resume here so they can finish; next tick re-pauses and
                # moves when verified.
                verified_paths: set[Path] = set()
                for f in completed_files:
                    joined = _safe_ssd_join(src_dir, f.name or "")
                    if joined is None:
                        continue
                    try:
                        verified_paths.add(joined.resolve())
                    except OSError:
                        continue
                try:
                    local_real = local.resolve()
                except OSError:
                    local_real = None
                if local_real is None or local_real not in verified_paths:
                    detail = ""
                    try:
                        for f in cls_files:
                            joined = _safe_ssd_join(src_dir, f.name or "")
                            if joined is None:
                                continue
                            try:
                                if joined.resolve() == local_real:
                                    try:
                                        on_disk = local.stat().st_size if local.is_file() else -1
                                    except OSError:
                                        on_disk = -1
                                    detail = (
                                        f" (file progress={(f.progress or 0.0) * 100:.1f}%, "
                                        f"on-disk {max(on_disk, 0)}/{f.size_bytes or 0} B)"
                                    )
                                    break
                            except OSError:
                                continue
                    except Exception:
                        detail = ""
                    resumed = False
                    try:
                        await self.dest_client.resume(h)
                        resumed = True
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "could not resume incomplete single file %s for %s: %s",
                            cls.single_file, ts.source_name, e,
                        )
                    log.warning(
                        "single file %s for %s is not client-verified complete%s; "
                        "%s staying in MOVING without moving",
                        cls.single_file, ts.source_name, detail,
                        "resumed to finish downloading," if resumed else "resume failed,",
                    )
                    self._park_moving(
                        ts,
                        f"single file {cls.single_file} not verified complete{detail}; "
                        f"{'resumed to finish, ' if resumed else ''}"
                        f"staying in MOVING without moving",
                    )
                    return
            if local is None:
                pass
            elif isinstance(local, Path) and local.is_dir():
                # Unreachable: folder layouts move per-file above by design.
                raise RuntimeError(f"refusing bare folder move of {local}")
            else:
                await self._rclone_move(local, remote, ts)
                try:
                    _left = local.is_symlink() or local.exists()
                except OSError:
                    _left = True
                if _left:
                    self._park_moving(
                        ts,
                        f"single-file move left {local} on disk; "
                        f"staying in MOVING without wiping",
                    )
                    return
        else:
            # Mixed — per-episode moves with --include (single batch).
            # Only reachable when the pinned kind is mixed (see branch_kind
            # above), so cursor rewrites here can't corrupt single-flow rows.
            cap = self._frozen_batch_cap(ts)
            episodes = cls.episodes
            if not episodes:
                raise RuntimeError(
                    f"cannot move torrent {ts.source_name}: classification is '{cls.kind}' but no episodes found"
                )
            if cap <= 0:
                # Files are already on SSD; cap only affects move grouping.
                cap = sum(e.size_bytes for e in episodes) or 1
            batches = make_batches(episodes, cap_bytes=cap)
            if not batches:
                raise RuntimeError(
                    f"cannot move torrent {ts.source_name}: batching produced 0 batches for {len(episodes)} episodes"
                )
            for i, batch in enumerate(batches):
                ts.batch_index = i
                ts.batches_total = len(batches)
                self.store.upsert(ts)
                skip = await self._fuse_skipped(
                    [(e.file_name, e.size_bytes) for e in batch.episodes], branch_kind,
                )
                skip_norm = {(s or "").replace("\\", "/").strip("/") for s in skip}
                names = [n for n in batch.file_names() if n not in skip_norm]
                if not names:
                    log.info(
                        "mixed batch %d/%d for %s already on remote; skipping move",
                        i + 1, len(batches), ts.source_name,
                    )
                    continue
                await self._rclone_move(
                    src_dir,
                    remote,
                    ts,
                    files_from=names,
                    extra=self.cfg.rclone.batch_move_extra_flags,
                )
                stuck = [n for n in names if _left_on_disk(src_dir, n)]
                if stuck:
                    self._park_moving(
                        ts,
                        f"mixed-torrent batch move moved nothing "
                        f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                        f"staying in MOVING without wiping",
                    )
                    return

        # 6. Persist the SSD torrent's bytes for RE_ADDING before the client
        # entry is deleted below. Rows adopted by recovery (fresh state.db)
        # carry no blob — without this, _re_add_cross_seed_torrent fails on
        # "missing blob" and the fuse gate has nothing to verify against.
        # Best-effort: a failed export only warns; downstream fails loudly.
        if not (ts._blob or ts.cross_seed_blob):
            try:
                stored = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                stored = None
            if isinstance(stored, (bytes, bytearray)) and stored:
                ts.cross_seed_blob = bytes(stored)
                ts._blob = bytes(stored)
        if not (ts._blob or ts.cross_seed_blob):
            export_fn = getattr(self.dest_client, "export_torrent", None)
            if callable(export_fn):
                try:
                    exported = await export_fn(h)
                    if isinstance(exported, (bytes, bytearray)) and exported:
                        ts.cross_seed_blob = bytes(exported)
                        ts._blob = bytes(exported)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not export .torrent for %s before deleting SSD entry: %s",
                        h[:10], e,
                    )
        if ts._blob or ts.cross_seed_blob:
            try:
                self.store.upsert(ts)
            except Exception as e:  # noqa: BLE001
                log.warning("could not persist cross-seed blob for %s: %s", h[:10], e)

        # 7. Delete old torrent from VPS2 client (delete_files=False) before re-adding to FUSE
        try:
            await self.dest_client.delete(h, delete_files=False)
        except Exception as e:  # noqa: BLE001
            log.warning("could not delete old torrent from client after move: %s", e)

        # 8. Delete local content folder on SSD after move
        if folder and folder.resolve() != src_dir.resolve() and folder.exists():
            log.info("deleting content folder after move: %s", folder)
            await wipe_local_tree(
                folder,
                base_dir=[self.cfg.ssd.path, Path(self.cfg.dest.save_path)],
            )

        self.transition(ts, State.RE_ADDING)

    async def _rclone_move(
        self,
        local: Path,
        remote: str,
        ts: TorrentState,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
    ) -> None:
        async with self.move_sem:
            if files_from is not None:
                preview = ", ".join(files_from[:3]) + ("…" if len(files_from) > 3 else "")
                log.info("rclone move %s -> %s (files=%d, e.g. %s)",
                         local, remote, len(files_from), preview)
            else:
                log.info("rclone move %s -> %s (include=%s, extra=%s)", local, remote, include, extra)
            # RcloneTimeoutError propagates: the child is terminated and the
            # source tree intact, so callers park the row for retry (never
            # fail — failing would loop: re-download, stall again, fail…).
            res = await move_local_to_remote(self.cfg, local, remote, include=include,
                                             files_from=files_from, extra=extra)
            if not res.ok:
                err = res.stderr.strip()
                last_err = [ln.strip() for ln in err.splitlines() if ln.strip()][-1] if err else f"rc={res.returncode}"
                raise RuntimeError(f"rclone failed (rc={res.returncode}): {last_err}")

    # ---- state: RE_ADDING ----

    async def _do_re_add(self, ts: TorrentState) -> None:
        store = getattr(self, "store", None)
        now = dt.datetime.now(dt.timezone.utc)
        try:
            _cycles = int(getattr(ts, "readd_cycles", 0) or 0)
        except (TypeError, ValueError):
            _cycles = 0
        if _cycles >= _MAX_READD_CYCLES:
            # Rapid DONE -> re-add flapping (lost fuse entry re-added, lost
            # again, ...) would otherwise reset its timer forever and never
            # trip max-age. Page the operator instead of looping silently.
            err = (
                f"re-add flap limit reached ({_cycles} rapid DONE demotions); "
                "fuse entries keep going missing — inspect the dest client"
            )
            log.error("giving up on %s: %s", ts.source_name, err)
            ts.readd_next_retry_at = None
            self.transition(ts, State.FAILED, error=err)
            return
        if ts.readd_first_attempted_at is None:
            ts.readd_first_attempted_at = now
            if store is not None:
                store.upsert(ts)

        delay = self.cfg.fuse_reinject_delay_seconds
        if delay > 0 and ts.readd_attempts == 0 and ts.readd_next_retry_at is None:
            log.info(
                "parking %s for %ds fuse mount indexing before re-injection",
                ts.source_name[:50], delay,
            )
            if delay > 5:
                ts.readd_next_retry_at = now + dt.timedelta(seconds=delay)
                if store is not None:
                    store.upsert(ts)
                return
            else:
                await asyncio.sleep(delay)

        max_age_val = getattr(self.cfg, "fuse_reinject_max_age_seconds", 86400)
        max_age_sec = max_age_val if isinstance(max_age_val, (int, float)) else 86400
        max_age = dt.timedelta(seconds=max_age_sec)

        retry_gap_val = getattr(self.cfg, "fuse_reinject_retry_gap_seconds", 120)
        retry_gap = retry_gap_val if isinstance(retry_gap_val, (int, float)) else 120

        backoff_val = getattr(self.cfg, "fuse_reinject_backoff_seconds", 1800)
        backoff_interval = backoff_val if isinstance(backoff_val, (int, float)) else 1800

        elapsed = now - ts.readd_first_attempted_at
        if elapsed >= max_age:
            err = (
                f"re-injection timed out after {int(elapsed.total_seconds())}s "
                f"(>24h limit): destination WebUI unresponsive"
            )
            log.error("giving up on %s: %s", ts.source_name, err)
            ts.readd_next_retry_at = None
            self.transition(ts, State.FAILED, error=err)
            return

        # FUSE AVAILABILITY GATE (public + private): the rclone move must
        # have landed the cross-seed's files at the fuse target BEFORE we
        # inject anything with skip_check=True. Blind injection creates
        # torrents that can never seed (e.g. SSD-complete data that was
        # never moved). Park — don't FAILED — so mount lag or a slow remote
        # can heal on retry; persistent absence trips max_age above.
        gate_blob = ts._blob or ts.cross_seed_blob
        if not gate_blob and store is not None:
            try:
                gate_blob = await asyncio.to_thread(store.get_blob, ts.source_infohash)
            except Exception:
                gate_blob = None
        gate_expected = self._expected_fuse_files(gate_blob)
        if not gate_blob:
            # Fail closed: no blob at all means we cannot verify the fuse
            # target. Park for retry instead of injecting blind with
            # skip_check=True (which creates unseedable torrents).
            log.warning(
                "fuse content unverifiable for %s (missing blob); parking re-add",
                ts.source_name[:50],
            )
            ts.readd_next_retry_at = now + dt.timedelta(seconds=retry_gap)
            if store is not None:
                store.upsert(ts)
            return
        if gate_expected is None:
            # Blob present but undecodable/empty (e.g. unit-test doubles with
            # fake bytes): keep legacy behavior and let downstream steps fail
            # loudly instead of parking forever.
            log.warning(
                "fuse gate cannot decode blob for %s; proceeding without gate",
                ts.source_name[:50],
            )
            gate_expected = []
        if gate_expected:
            gate_target = self._target_mount_for_blob(gate_blob, self._target_mount_for(ts))
            gate_missing = await self._missing_fuse_files(gate_target, gate_expected)
            if gate_missing:
                preview = ", ".join(gate_missing[:5])
                if len(gate_missing) > 5:
                    preview += f", …+{len(gate_missing) - 5} more"
                log.warning(
                    "fuse content not yet available for %s at %s "
                    "(%d/%d missing, e.g. %s); parking re-add instead of injecting blind",
                    ts.source_name[:50], gate_target,
                    len(gate_missing), len(gate_expected), preview,
                )
                ts.readd_next_retry_at = now + dt.timedelta(seconds=retry_gap)
                if store is not None:
                    store.upsert(ts)
                return

        max_cycle_attempts = 2

        for cycle_attempt in range(1, max_cycle_attempts + 1):
            ts.readd_attempts += 1
            try:
                # 1) Re-inject the racing-client torrents (private or otherwise)
                # pointing at the fuse mount with skip_check=True (req #3).
                # Watch rows (any SSD flavour label) re-inject their
                # persisted blobs instead.
                if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                    if self._is_watch_row(ts):
                        await self._re_inject_watch_dir_torrents(ts)
                    else:
                        await self._re_inject_racing_torrents(ts)

                # 2) Re-add the cross-seed torrent
                await self._re_add_cross_seed_torrent(ts)

                if ts.state != State.FAILED:
                    ts.readd_next_retry_at = None
                    # The content now seeds from fuse: record the fuse mount
                    # as the save location. Late-seed/cleanup guards read a
                    # non-fuse save_path on DONE rows as "never moved" — a
                    # stale SSD path here would demote healthy rows forever.
                    # Use the blob-derived mount (fresh adoptions start as
                    # kind="unknown" and would otherwise record unsorted).
                    try:
                        ts.save_path = str(self._target_mount_for_blob(gate_blob, self._target_mount_for(ts)))
                    except Exception:  # noqa: BLE001
                        pass
                    self.transition(ts, State.DONE)
                return

            except _WEBUI_RETRY_ERRORS as e:
                now_curr = dt.datetime.now(dt.timezone.utc)
                elapsed_curr = now_curr - ts.readd_first_attempted_at
                if elapsed_curr >= max_age:
                    err = (
                        f"re-injection timed out after {int(elapsed_curr.total_seconds())}s "
                        f"(>24h limit): {e}"
                    )
                    log.error("giving up on %s: %s", ts.source_name, err)
                    ts.readd_next_retry_at = None
                    self.transition(ts, State.FAILED, error=err)
                    return

                if cycle_attempt < max_cycle_attempts:
                    log.warning(
                        "WebUI unresponsive during re-add for %s (%s); "
                        "retrying in %ds (attempt %d/%d)",
                        ts.source_name[:50], e, retry_gap, cycle_attempt, max_cycle_attempts,
                    )
                    if retry_gap > 5:
                        ts.readd_next_retry_at = now_curr + dt.timedelta(seconds=retry_gap)
                        if store is not None:
                            store.upsert(ts)
                        return
                    else:
                        if store is not None:
                            store.upsert(ts)
                        await asyncio.sleep(retry_gap)
                        if self._stop:
                            return
                else:
                    ts.readd_next_retry_at = now_curr + dt.timedelta(seconds=backoff_interval)
                    log.warning(
                        "WebUI still unresponsive for %s after %d attempts (%s); "
                        "backing off for %ds (30m) until %s (elapsed=%ds, max=%ds)",
                        ts.source_name[:50], max_cycle_attempts, e, backoff_interval,
                        ts.readd_next_retry_at.isoformat(timespec="seconds"),
                        int(elapsed_curr.total_seconds()), int(max_age.total_seconds()),
                    )
                    if store is not None:
                        store.upsert(ts)
                    self._schedule_telegram_update(ts)
                    return

    async def _re_add_cross_seed_torrent(self, ts: TorrentState) -> None:
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        target_hash = (ts.cross_seed_infohash or h).lower()
        injected_hashes = {x.lower() for x in ts.injected_private_hashes.split(",") if x}
        if target_hash and target_hash in injected_hashes:
            log.info("cross-seed torrent %s already injected on fuse in step 1", target_hash[:10])
            return

        blob = ts._blob or ts.cross_seed_blob
        if not blob:
            blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            if blob:
                ts.cross_seed_blob = blob
                ts._blob = blob
        if not blob:
            err = f"missing blob for re-adding cross-seed torrent {ts.source_infohash[:10]}"
            log.error(err)
            self.transition(ts, State.FAILED, error=err)
            return

        blob_hash = ""
        try:
            from .watchdir import _bencoded_info_hash
            parsed_hash, _, _, _ = _bencoded_info_hash(blob)
            blob_hash = parsed_hash.lower()
        except Exception:
            pass
        target_hash = (blob_hash or ts.cross_seed_infohash or h).lower()
        if target_hash and target_hash in injected_hashes:
            log.info("cross-seed torrent %s already injected on fuse in step 1", target_hash[:10])
            return

        target_mount = self._target_mount_for_blob(blob, self._target_mount_for(ts))
        ok, detail = await self._ensure_fuse_entry(
            blob=blob, infohash=target_hash, target_mount=target_mount,
            label="cross-seed torrent",
        )

        if not ok:
            err_msg = f"fuse re-add rejected: {detail or 'client rejected torrent'}"
            log.error("re-add cross-seed torrent failed for %s: %s", ts.source_name, err_msg)
            # "already added" with no visible entry is registration lag
            # (same transient as _NOT_VISIBLE_DETAIL) — park/retry, never
            # terminal FAILED.
            if detail == "Fails." or not detail or detail == _NOT_VISIBLE_DETAIL or "already" in (detail or "").lower():
                raise WebUIUnresponsiveError(err_msg)
            self.transition(ts, State.FAILED, error=err_msg)
            return

    async def _re_inject_watch_dir_torrents(self, ts: TorrentState) -> None:
        """Re-add every watch-dir dropped torrent and discovered cross-seeds onto FUSE."""
        watch_cross_dir = _watch_cross_seed_dir(
            self.cfg.general.state_db, ts.source_infohash)
        if watch_cross_dir is None or not watch_cross_dir.exists():
            return
        ts_fallback_mount = self._target_mount_for(ts)
        injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        injected_set = set(injected)

        try:
            for p in sorted(watch_cross_dir.glob("*.torrent")):
                try:
                    blob = await asyncio.to_thread(p.read_bytes)
                    from .watchdir import _bencoded_info_hash
                    h, _, _, _ = _bencoded_info_hash(blob)
                except Exception as e:  # noqa: BLE001
                    log.warning("failed to read watch-dir torrent %s: %s", p.name, e)
                    continue

                h_low = h.lower()
                if h_low in injected_set:
                    continue

                # Fuse gate (fail-closed): never point a re-added torrent at
                # missing/unverifiable data.
                target_mount = self._target_mount_for_blob(blob, ts_fallback_mount)
                watch_expected = self._expected_fuse_files(blob)
                if not watch_expected:
                    log.warning(
                        "re-inject watch-dir torrent %s: cannot decode file list; skipping blind injection",
                        h[:10],
                    )
                    continue
                watch_missing = await self._missing_fuse_files(target_mount, watch_expected)
                if watch_missing:
                    log.warning(
                        "re-inject watch-dir torrent %s: fuse content missing at %s (%d files); skipping blind injection",
                        h[:10], target_mount, len(watch_missing),
                    )
                    continue

                ok, detail = await self._ensure_fuse_entry(
                    blob=blob, infohash=h_low, target_mount=target_mount,
                    label="watch-dir torrent",
                )

                if ok:
                    injected.append(h_low)
                    injected_set.add(h_low)
                else:
                    # Split like _re_add_cross_seed_torrent: transient client
                    # states park the row for retry; a hard rejection (e.g.
                    # invalid blob) skips just this torrent so one bad .torrent
                    # can't hold the whole row (and its moved bytes) hostage.
                    if (not detail or detail == "Fails."
                            or detail == _NOT_VISIBLE_DETAIL
                            or "already" in detail.lower()):
                        log.warning("re-inject watch-dir torrent %s rejected: %s", h[:10], detail)
                        raise WebUIUnresponsiveError(f"re-inject watch-dir torrent {h[:10]} rejected: {detail}")
                    log.warning(
                        "re-inject watch-dir torrent %s hard-rejected (%s); skipping it",
                        h[:10], detail,
                    )
                    continue
        finally:
            ts.injected_private_hashes = ",".join(dict.fromkeys(injected))

    async def _re_inject_racing_torrents(self, ts: TorrentState) -> None:
        """Re-add every racing-client torrent matching this content onto VPS2
        pointing at the fuse mount with skip_check=True.
        """
        ts_fallback_mount = self._target_mount_for(ts)
        injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        injected_set = set(injected)
        added = 0

        try:
            racing = await self._list_source_torrents()
        except Exception as e:  # noqa: BLE001
            log.warning("could not list racing torrents for re-injection: %s", e)
            return

        target_norm = normalize_content_name(ts.source_name)
        matches = [
            t for t in racing
            if t.name == ts.source_name or normalize_content_name(t.name) == target_norm
        ]

        try:
            for t in matches:
                h_low = t.infohash.lower()
                if h_low in injected_set:
                    continue
                if _group_is_ignored([t], getattr(self, "store", None)):
                    log.info("re-inject: skipping cancelled torrent %s (%s)",
                             t.infohash[:10], (t.name or "")[:40])
                    continue
                try:
                    blob = await self._fetch_racing_torrent_bytes(t.infohash)
                except Exception as e:  # noqa: BLE001
                    log.warning("re-inject: fetch %s failed: %s",
                                t.infohash[:10], e)
                    continue
                if not blob:
                    # SFTP clean-miss + no export endpoint (Deluge has no
                    # core.get_torrent_file) surface nowhere else — log
                    # loudly so a skipped racing seed is visible.
                    log.warning(
                        "re-inject: no .torrent bytes available for %s (%s); "
                        "skipping for now (late-seed job will retry)",
                        t.infohash[:10], t.name[:50],
                    )
                    continue
                # Per-torrent mount: ts.kind may be stale "unknown" (fresh
                # adoption); the blob's own layout is authoritative.
                target_mount = self._target_mount_for_blob(blob, ts_fallback_mount)
                # Per-match fuse gate (fail-closed): only inject torrents
                # whose content is actually available at the target (same
                # release name does not guarantee the bytes were moved).
                match_expected = self._expected_fuse_files(blob)
                if not match_expected:
                    log.warning(
                        "re-inject: cannot decode file list for %s; skipping blind injection",
                        t.infohash[:10],
                    )
                    continue
                match_missing = await self._missing_fuse_files(target_mount, match_expected)
                if match_missing:
                    log.warning(
                        "re-inject: fuse content missing for %s (%s) at %s (%d files); skipping blind injection",
                        t.infohash[:10], t.name[:50], target_mount, len(match_missing),
                    )
                    continue
                ok, detail = await self._ensure_fuse_entry(
                    blob=blob, infohash=h_low, target_mount=target_mount,
                    label="racing torrent",
                )
                if not ok:
                    # Split like _re_add_cross_seed_torrent: transient client
                    # states park the row for retry; a hard rejection skips
                    # just this torrent so the cross-seed still lands.
                    if (not detail or detail == "Fails."
                            or detail == _NOT_VISIBLE_DETAIL
                            or "already" in detail.lower()):
                        log.warning(
                            "re-inject: add %s rejected: %s",
                            t.infohash[:10], detail,
                        )
                        raise WebUIUnresponsiveError(f"re-inject add {t.infohash[:10]} rejected: {detail}")
                    log.warning(
                        "re-inject: add %s hard-rejected (%s); skipping it",
                        t.infohash[:10], detail,
                    )
                    continue
                injected.append(h_low)
                injected_set.add(h_low)
                added += 1
        finally:
            ts.injected_private_hashes = ",".join(dict.fromkeys(injected))

        if added == 0:
            pending = [t for t in matches if t.infohash.lower() not in injected_set]
            if pending:
                log.warning(
                    "re-inject: 0/%d racing torrent(s) injected for %s "
                    "(VPS1 still lists them); row proceeds with cross-seed "
                    "only and the late-seed job will retry the racing injection",
                    len(pending), ts.source_name[:60],
                )
            elif not matches:
                log.info(
                    "re-inject: no racing torrents left on VPS1 for %s; cross-seed only",
                    ts.source_name[:60],
                )

    async def _check_and_inject_late_cross_seeds(
        self, ts: TorrentState, group: list[Torrent]
    ) -> None:
        """Check if new cross-seeds arrived on VPS1 for a completed release and inject them to FUSE."""
        if not hasattr(self, "_failed_late_cross_seeds"):
            self._failed_late_cross_seeds = {}
        _row_key = (ts.source_infohash or "").lower()
        try:
            _ok_at = getattr(self, "_late_seed_ok_at", None)
            if isinstance(_ok_at, dict) and _row_key:
                _next_ok = _ok_at.get(_row_key)
                if _next_ok is not None and time.monotonic() < float(_next_ok):
                    return
        except Exception:
            pass
        self._late_seed_sweep()
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
                    ts.source_name[:50], ts.save_path, verified_mount,
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
                    ts.source_name[:50], ts.save_path,
                )
                await self._demote_false_done_to_moving(ts)
                return

        known_hashes = {
            h.lower() for h in (
                ts.source_infohash,
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            ) if h
        }

        def _late_pending() -> bool:
            """Any unexpired deferral for this row's hashes or group members."""
            try:
                fm = getattr(self, "_failed_late_cross_seeds", None)
                if not isinstance(fm, dict) or not fm:
                    return False
                rel: set[str] = set(known_hashes)
                try:
                    for t in group or []:
                        _gh = (getattr(t, "infohash", "") or "").lower()
                        if _gh:
                            rel.add(_gh)
                except Exception:
                    pass
                _now = dt.datetime.now(dt.timezone.utc)
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
        now_utc = dt.datetime.now(dt.timezone.utc)

        # Repair a racing (source) injection that RE_ADDING step 1 silently
        # skipped (SFTP timeout bursts + no Deluge export fallback): the
        # source hash is in known_hashes by construction, so the
        # new-torrent loop below would never retry it and the row would
        # seed the cross-seed only. Runs before the early return so rows
        # with no *new* arrivals still heal.
        await self._ensure_source_fuse_entry(ts, group, now_utc)

        new_torrents = [
            t for t in group
            if t.infohash.lower() not in known_hashes
            and not _group_is_ignored([t], getattr(self, "store", None))
        ]

        # Repair the original entry even on quiet ticks (no new arrivals):
        # otherwise an original SSD leftover only heals when something else
        # arrives. Failed-map gating inside keeps the steady state cheap.
        await self._ensure_original_fuse_entry(
            ts, self._target_mount_for(ts),
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
        current_injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        current_injected_set = set(current_injected)
        changed = False

        for t in new_torrents:
            h_low = t.infohash.lower()
            failed_at = self._failed_late_cross_seeds.get(h_low)
            if failed_at and (now_utc - failed_at).total_seconds() < 1800:
                continue

            log.info(
                "detected late cross-seed for completed content '%s': %s (%s)",
                ts.source_name[:40], t.infohash[:10], t.name[:40],
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
            late_missing = await self._missing_fuse_files(target_mount, late_expected)
            if late_missing:
                log.warning(
                    "late cross-seed: fuse content missing for %s (%s) at %s (%d files); deferring injection",
                    t.infohash[:10], t.name[:40], target_mount, len(late_missing),
                )
                self._failed_late_cross_seeds[h_low] = now_utc
                # Self-heal: content missing on fuse but still sitting on SSD
                # (abrupt-stop leftover, or a fuse-pointing entry whose bytes
                # never moved) must go through MOVING first — never inject
                # around it. Best-effort; a failed SSD probe just defers.
                try:
                    from .recovery import find_content_on_ssd

                    if find_content_on_ssd(self.cfg, late_expected) is not None:
                        log.warning(
                            "late cross-seed: %s missing on fuse but present on SSD; "
                            "demoting %s to MOVING",
                            t.infohash[:10], ts.source_name[:50],
                        )
                        await self._demote_false_done_to_moving(ts)
                        return
                except Exception:  # noqa: BLE001
                    pass
                continue

            try:
                ok, detail = await self._ensure_fuse_entry(
                    blob=blob, infohash=h_low, target_mount=target_mount,
                    label="late cross-seed",
                )
                if ok:
                    log.info(
                        "auto-injected late cross-seed %s (%s) onto fuse (%s)",
                        t.infohash[:10], t.name[:40], target_mount,
                    )
                    current_injected.append(h_low)
                    current_injected_set.add(h_low)
                    changed = True
                    self._failed_late_cross_seeds.pop(h_low, None)
                else:
                    log.warning(
                        "late cross-seed: add %s rejected by dest client: %s",
                        t.infohash[:10], detail,
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
            ts.injected_private_hashes = ",".join(dict.fromkeys(current_injected))
            self.store.upsert(ts)
        elif not _late_pending():
            self._late_seed_memoize(_row_key, _injected_before, _save_before, ts, changed=False)

    def _late_seed_defer(self, h_low: str) -> None:
        """Record a late-seed deferral for the 30m backoff map (best-effort)."""
        try:
            d = getattr(self, "_failed_late_cross_seeds", None)
            if isinstance(d, dict) and h_low:
                d[h_low] = dt.datetime.now(dt.timezone.utc)
        except Exception:
            pass

    def _late_seed_sweep(self) -> None:
        """Bound the late-seed failure/success maps (30m TTL, 5000 cap).

        Called at the top of every late-seed check; cheap fast path unless a
        map grew large. Restart-wiped maps simply re-warm (thundering herd
        bounded by one full scan per DONE row, then memoized).
        """
        try:
            for attr in ("_failed_late_cross_seeds", "_late_seed_ok_at"):
                d = getattr(self, attr, None)
                if not isinstance(d, dict) or len(d) <= 2000:
                    continue
                if attr == "_late_seed_ok_at":
                    now_m = time.monotonic()
                    for k in list(d.keys()):
                        try:
                            if now_m >= float(d[k]):
                                d.pop(k, None)
                        except Exception:
                            d.pop(k, None)
                else:
                    try:
                        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30)
                    except Exception:
                        continue
                    for k in list(d.keys()):
                        try:
                            if d[k] < cutoff:
                                d.pop(k, None)
                        except Exception:
                            d.pop(k, None)
                if len(d) > 5000:
                    for k in list(d.keys())[: len(d) - 5000]:
                        d.pop(k, None)
        except Exception:
            pass

    def _late_seed_memoize(
        self, row_key: str, injected_before: str, save_before: str,
        ts: TorrentState, *, changed: bool,
    ) -> None:
        """Record a quiet healthy check so the next tick skips this DONE row."""
        try:
            if changed or not row_key:
                return
            if ts.injected_private_hashes != injected_before or ts.save_path != save_before:
                return
            d = getattr(self, "_late_seed_ok_at", None)
            if not isinstance(d, dict):
                d = {}
                self._late_seed_ok_at = d  # type: ignore[attr-defined]
            d[row_key] = time.monotonic() + 1800.0
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
        if source_low in {(ts.dest_infohash or "").lower(), (ts.cross_seed_infohash or "").lower()} - {""}:
            return
        src_entry = next(
            (t for t in (group or []) if t.infohash.lower() == source_low), None,
        )
        if src_entry is None:
            return  # VPS1 cleaned already; nothing to repair.
        failed_at = self._failed_late_cross_seeds.get(source_low)
        if failed_at and (now_utc - failed_at).total_seconds() < 1800:
            return
        log.info(
            "late cross-seed: racing torrent %s (%s) never injected for %s; repairing",
            source_low[:10], (src_entry.name or "")[:40], ts.source_name[:40],
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
            missing = await self._missing_fuse_files(target_mount, expected)
        except Exception:  # noqa: BLE001
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        if missing:
            log.warning(
                "late cross-seed: repair of racing torrent %s deferred "
                "(fuse content missing at %s, %d files)",
                source_low[:10], target_mount, len(missing),
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        try:
            ok, detail = await self._ensure_fuse_entry(
                blob=blob, infohash=source_low, target_mount=target_mount,
                label="racing torrent (repair)",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("late cross-seed: repair add %s failed: %s", source_low[:10], e)
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        if not ok:
            log.warning(
                "late cross-seed: repair add %s rejected by dest client: %s",
                source_low[:10], detail,
            )
            self._failed_late_cross_seeds[source_low] = now_utc
            return
        log.info(
            "auto-injected missing racing torrent %s (%s) onto fuse (%s) (repair)",
            source_low[:10], (src_entry.name or "")[:40], target_mount,
        )
        current = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        if source_low not in current:
            current.append(source_low)
        ts.injected_private_hashes = ",".join(dict.fromkeys(current))
        self.store.upsert(ts)
        self._failed_late_cross_seeds.pop(source_low, None)

    async def _verified_fuse_mount_for_done_row(self, ts: TorrentState) -> Path | None:
        """Prove a DONE row really seeds from fuse; return the mount or None.

        Checks both halves of "seeding from fuse": every recorded hash has a
        live dest entry AND the torrent bytes are present under the fuse
        target. Entries alone are not proof (a skip_check entry reports
        complete with zero bytes). None means unverifiable — callers must
        take the fail-closed path (demote/defer), never assume health.
        """
        hashes = {
            h.lower() for h in (
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            ) if h
        }
        if not hashes:
            return None
        try:
            present = await self.dest_client.list_torrents(hashes=list(hashes))
        except Exception:  # noqa: BLE001
            return None
        have = {t.hash.lower() for t in (present or [])}
        if hashes - have:
            return None
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
        try:
            missing = await self._missing_fuse_files(target, expected)
        except Exception:  # noqa: BLE001
            return None
        return target if not missing else None

    async def _demote_false_done_to_moving(self, ts: TorrentState) -> None:
        """Self-heal a DONE row whose save_path never left SSD.

        Fresh-DB recovery may have adopted an SSD-complete torrent as DONE
        (stale save_path, pre-fix DB). The rclone move must run before any
        fuse injection, so move the row back to MOVING when the SSD bytes
        are still present. When the bytes cannot be confirmed, leave the row
        alone (late seeds stay deferred by the caller).
        """
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        if not h:
            return
        expected: list[tuple[str, int]] | None = None
        try:
            files = await self.dest_client.get_torrent_files(h)
            expected = [
                (str(f.name), int(f.size_bytes or 0))
                for f in (files or []) if getattr(f, "name", "")
            ] or None
        except Exception:  # noqa: BLE001
            expected = None
        if not expected:
            return
        try:
            from .recovery import find_content_on_ssd

            ssd_root = find_content_on_ssd(self.cfg, expected)
        except Exception:  # noqa: BLE001
            return
        if ssd_root is None:
            return
        try:
            ts.save_path = str(ssd_root)
            self.store.upsert(ts)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.transition(ts, State.MOVING)
            log.warning(
                "late cross-seed: demoted %s from DONE to MOVING; "
                "SSD content at %s will move before fuse injection",
                ts.source_name[:50], ssd_root,
            )
        except ValueError as e:
            log.warning(
                "late cross-seed: could not demote %s to MOVING: %s",
                ts.source_infohash[:10], e,
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "late cross-seed: demote failed for %s: %s",
                ts.source_infohash[:10], e,
            )

    async def _ensure_original_fuse_entry(
        self, ts: TorrentState, fallback_mount: Path
    ) -> None:
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
                    if (dt.datetime.now(dt.timezone.utc) - _fa).total_seconds() < 1800:
                        return
                except Exception:
                    pass
        except Exception:
            pass
        # Dest-export only (no source RPC): the original entry's own bytes
        # are the authoritative repair payload, and this keeps the late-tick
        # free of extra source calls. Unavailable => skip repair; new seeds
        # stay protected by their own per-torrent fuse gate.
        blob: bytes | None = None
        try:
            export_fn = getattr(self.dest_client, "export_torrent", None)
            if callable(export_fn):
                candidate = await export_fn(h)
                if isinstance(candidate, (bytes, bytearray)) and candidate:
                    blob = bytes(candidate)
        except Exception:  # noqa: BLE001
            # Transport flap (dest down): silent return, no deferral — the
            # outage already backs everything else off, and a stable miss
            # must not be confused with a down client.
            blob = None
        if not blob:
            return
        target = self._target_mount_for_blob(blob, fallback_mount)
        expected = self._expected_fuse_files(blob)
        if not expected:
            self._late_seed_defer(h)
            return
        try:
            missing = await self._missing_fuse_files(target, expected)
        except Exception:  # noqa: BLE001
            self._late_seed_defer(h)
            return
        if missing:
            self._late_seed_defer(h)
            return
        try:
            ok, detail = await self._ensure_fuse_entry(
                blob=blob, infohash=h, target_mount=target,
                label="original content",
            )
            if not ok:
                log.warning(
                    "late cross-seed: original %s fuse repair rejected: %s",
                    h[:10], detail,
                )
                self._late_seed_defer(h)
            else:
                try:
                    _failed6 = getattr(self, "_failed_late_cross_seeds", None)
                    if isinstance(_failed6, dict):
                        _failed6.pop(h, None)
                except Exception:
                    pass
        except Exception as e:  # noqa: BLE001
            log.warning("late cross-seed: original %s fuse repair failed: %s", h[:10], e)
            self._late_seed_defer(h)

    async def _fetch_racing_torrent_bytes(self, infohash: str) -> bytes | None:
        """Fetch the raw .torrent bytes for a racing-client infohash.

        Tries in order:
          1. SFTP exporter (Deluge state dir, qB BT_backup)
          2. The source client's own export endpoint (qB /api/v2/torrents/export,
             Deluge core.get_torrent_file).
        """
        # Validate that infohash is a valid hex infohash
        if not infohash or len(infohash) != 40 or not all(c in "0123456789abcdefABCDEF" for c in infohash):
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
                    log.warning("sftp fetch %s timed out after 15s (attempt %d/2)",
                                infohash[:10], attempt)
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
                log.debug("_fetch_racing_torrent_bytes deluge export unavailable for %s: %s",
                          infohash[:10], e)
            else:
                log.warning("_fetch_racing_torrent_bytes export failed for %s: %s", infohash[:10], e)
            return None

    @staticmethod
    def _expected_fuse_files(blob: bytes | None) -> list[tuple[str, int]] | None:
        """Decode (torrent-relative name, size) pairs from .torrent bytes.

        Returns None when there is nothing to verify (no blob / undecodable /
        empty) — callers then keep existing behavior and let downstream steps
        fail loudly instead of gating on an empty expectation.
        """
        if not blob or not isinstance(blob, (bytes, bytearray)):
            return None
        try:
            from .watchdir import extract_torrent_files_from_bencoded
            pairs = [
                (f.name, f.size_bytes)
                for f in extract_torrent_files_from_bencoded(blob)
                if f.name
            ]
            return pairs or None
        except Exception:
            return None

    async def _missing_fuse_files(
        self, target_mount: Path, files: list[tuple[str, int]]
    ) -> list[str]:
        """Expected files absent (or size-mismatched) under the fuse target.

        Blocking fuse stats are offloaded to a thread. A failed check itself
        counts as missing — never inject blind when the mount can't be read.
        A dead mount short-circuits on one mount stat instead of one failing
        stat per file per row per tick.
        """
        mount = Path(target_mount)

        def _check() -> list[str]:
            try:
                mount.stat()
            except OSError as e:
                return [f"<mount unavailable: {mount} ({e})>"]
            missing: list[str] = []
            for name, want in files:
                target = _safe_ssd_join(mount, name or "")
                if target is None:
                    missing.append(name)
                    continue
                try:
                    actual = target.stat().st_size
                except OSError:
                    missing.append(name)
                    continue
                if want and actual != want:
                    missing.append(f"{name} (size {actual}!={want})")
            return missing

        try:
            return await asyncio.to_thread(_check)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse availability check failed for %s: %s", mount, e)
            return [f"<availability check failed: {e}>"]

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
        """Classify .torrent bytes; "unknown" when undecodable or cfg broken."""
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return "unknown"
            from .watchdir import extract_torrent_files_from_bencoded

            files = extract_torrent_files_from_bencoded(bytes(blob))
            if not files:
                return "unknown"
            kind = classify(files, self.cfg).kind
            return kind if kind in ("movie", "season", "episode", "mixed", "unknown") else "unknown"
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
            from .watchdir import _bencoded_info_hash

            _, _, _, announce = _bencoded_info_hash(bytes(blob))
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
            log.info("pausing public %s %s on fuse (pause_public_torrents_on_fuse=true)",
                     label, h_low[:10])
        except Exception as e:  # noqa: BLE001
            log.warning("could not pause public %s %s on fuse (keeps seeding): %s",
                        label, h_low[:10], e)

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
        res = await self.dest_client.add_torrent(**add_kwargs)  # type: ignore[arg-type]
        detail = res.detail if isinstance(res.detail, str) else ""
        if res.accepted and "already" not in detail.lower():
            # Verify the entry actually landed where asked: the fuse index
            # can lag (rclone busy with another move), and an accepted-but-
            # invisible entry must park/retry — never mark DONE, never touch
            # the moved files. Same patience window as the replace path.
            for _ in range(4):
                try:
                    _st = await self.dest_client.get_torrent(h_low)
                except Exception:
                    _st = None
                if _st is not None and self._save_path_points_at_target(
                    getattr(_st, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
                    if pause_public:
                        await self._pause_fuse_entry_best_effort(h_low, label=label)
                    return True, detail
                await asyncio.sleep(2)
            log.warning(
                "%s %s accepted but not yet visible on fuse (%s); parking re-add",
                label, h_low[:10], target_mount,
            )
            return False, _NOT_VISIBLE_DETAIL
        if not (detail == "Fails." or "already" in detail.lower()):
            return False, detail
        # Possible duplicate: inspect what's actually there.
        try:
            dest_st = await self.dest_client.get_torrent(h_low)
        except Exception as e:  # noqa: BLE001
            log.debug("could not check dest client for %s: %s", h_low[:10], e)
            dest_st = None
        if dest_st is not None and self._save_path_points_at_target(
            getattr(dest_st, "save_path", ""), target_mount
        ):
            log.info("%s %s already on fuse; marking as injected", label, h_low[:10])
            if pause_public:
                await self._pause_fuse_entry_best_effort(h_low, label=label)
            return True, "already added"
        if dest_st is not None:
            log.warning(
                "%s %s exists at %s (not the fuse target %s); replacing with fuse entry",
                label, h_low[:10], getattr(dest_st, "save_path", "?"), target_mount,
            )
            try:
                await self.dest_client.delete(h_low, delete_files=False)
            except Exception as e:  # noqa: BLE001
                return False, f"cannot remove non-fuse entry: {e}"
            res2 = await self.dest_client.add_torrent(**add_kwargs)  # type: ignore[arg-type]
            detail2 = res2.detail if isinstance(res2.detail, str) else ""
            if res2.accepted or detail2 == "Fails." or "already" in detail2.lower():
                dest_st2 = None
                for _ in range(4):
                    try:
                        dest_st2 = await self.dest_client.get_torrent(h_low)
                    except Exception:
                        dest_st2 = None
                    if dest_st2 is not None:
                        break
                    await asyncio.sleep(2)
                if dest_st2 is not None and self._save_path_points_at_target(
                    getattr(dest_st2, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
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
