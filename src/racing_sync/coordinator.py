"""Main coordinator loop.

This is the only place that orchestrates state transitions, qBittorrent
calls, rclone invocations, and prowlarr lookups. The flow per torrent:

  1. Detect on VPS1 racing client (category=racing)
  2. Pick the SSD-source torrent:
       - if VPS1 has multiple, prefer public (req #1) via prowlarr or SFTP
       - if VPS1 has only private, query prowlarr by tracker map (req #2)
       - if from watch_dir, prefer prowlarr hit on Seedpool (req #3)
  3. Add to VPS2 qBittorrent at SSD save_path, paused, skip_check=False
  4. Resume; poll until complete (with batched file priorities for seasons)
  5. rclone move SSD -> remote (with --include for seasons)
  6. After move: re-add private torrents to VPS2 pointing at fuse, skip_check=True
  7. Mark DONE
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .batcher import Batch, make_batches
from .classifier import Classification, classify, should_skip_movie
from .clients.abstract import Torrent, TorrentClient
from .clients.deluge import DelugeClient
from .clients.http_base import AuthError
from .clients.qbittorrent import QBittorrentClient, build_qbtorrent_from_dest
from .config import AppConfig
from .logging_setup import get_ring_buffer
from .prowlarr import ProwlarrClient
from .recovery import reconcile
from .rclone_ops import (
    move_local_to_remote,
    ssd_free_bytes,
    ssd_has_room,
    ssd_max_inflight_bytes,
    wipe_local_tree,
)
from .sftp_source import SFTPExporter
from .state import State, StateStore, TorrentState
from .watchdir import WatchDirScanner

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Cross-seed picker
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class SourceDecision:
    """Where the SSD-source torrent comes from."""

    torrent_bytes: bytes
    source_label: str     # "public-prowlarr" | "public-sftp" | "private-prowlarr" | "watch-dir"
    name: str
    size_bytes: int
    infohash: str
    announce_url: str = ""


# Telegram notification filter: only certain state transitions deserve
# a chat message. Passive discoveries (NEW on first tick) don't.
_TELEGRAM_NOTIFY_STATES = frozenset({
    State.QUEUED,
    State.DOWNLOADING,
    State.MOVING,
    State.RE_ADDING,
    State.DONE,
    State.FAILED,
    State.WAITING_SEEDPOOL,
})


def _should_notify_telegram(prev: State, dst: State) -> bool:
    """Decide whether a state transition should fire a Telegram update.

    Policy:
      - First discovery of a torrent (NEW on its own) does NOT fire —
        prevents spam on first run with N pre-existing racing torrents.
      - Any transition INTO a work-active or terminal state fires
        (QUEUED, DOWNLOADING, MOVING, RE_ADDING, DONE, FAILED,
        WAITING_SEEDPOOL).
      - The bot's per-torrent message will be created on the first
        such transition; subsequent updates just edit the same message.
    """
    if dst == State.NEW and prev == State.NEW:
        # Same-state re-entry shouldn't happen, but guard anyway.
        return False
    if prev == State.NEW and dst == State.NEW:
        return False
    return dst in _TELEGRAM_NOTIFY_STATES


PUBLIC_TRACKER_HOSTS = (
    "publicbt", "tracker.public", "tracker.openbittorrent",
    "opentrackr", "academictorrents", "bt.archlinux",
    "linuxmint", "ubuntu-releases", "nyaa", "animetosho",
    "tokyotosho", "torrentgalaxy", "1337x", "piratebay",
    "coppersurfer", "leechers-paradise", "open.stealth.si",
)


def _looks_public(tracker_urls: list[str]) -> bool:
    for url in tracker_urls:
        low = url.lower()
        if any(host in low for host in PUBLIC_TRACKER_HOSTS):
            return True
    return False


async def pick_ssd_source_for_racing(
    *,
    cfg: AppConfig,
    source_torrent: Torrent,
    other_source_torrents: list[Torrent],
    prowlarr: ProwlarrClient | None,
    sftp: SFTPExporter | None,
    source_client: TorrentClient,
    attempt_prowlarr: bool = True,
) -> SourceDecision | None:
    """Decide which .torrent bytes to feed VPS2 SSD.

    Returns:
      - SourceDecision if we have a candidate right now, OR
      - None if we should park the row in WAITING_SEEDPOOL and retry later
        (only when `attempt_prowlarr=True` and we had a real Seedpool
        miss; otherwise we fall through to the SFTP fallback even if
        Seedpool returned no hit).

    Logic per req #1 / #2:
      (a) one or more *public* torrents for the file on VPS1   → use the
                                                              racing client's
                                                              own .torrent for
                                                              SSD download.
                                                              Prowlarr is
                                                              NEVER queried
                                                              in this case.
      (b) only *private* torrents from Aither / Beyond-HD /
          AnimeBytes (per tracker_map)                         → query the
                                                              configured
                                                              download_indexer
                                                              ("Seedpool (API)"
                                                              by default) for a
                                                              cross-seed copy
    """

    # If the source torrent is itself a "public" tracker, we use IT for
    # the SSD download directly. We do NOT consult Prowlarr — the racing
    # public torrent already works, fetching a Seedpool copy would be
    # redundant. The only exception is the rare case where the racing
    # client's .torrent is unreachable on VPS1 (then refetch_public_via_prowlarr
    # can fall back to Prowlarr as a last resort).
    publics = [t for t in other_source_torrents + [source_torrent]
               if _looks_public(t.trackers)]

    if publics:
        # req #1: when a public torrent exists on VPS1, use IT for the
        # SSD download directly. We do NOT consult Prowlarr by default
        # — the racing public torrent already works, fetching a Seedpool
        # copy would be redundant.
        #
        # The racing-client torrent's .torrent bytes are obtained either
        # via qBittorrent's `/api/v2/torrents/export` endpoint (handled
        # by the source_client.export_torrent() helper) or via SFTP from
        # the Deluge state directory.
        if cfg.cross_seed.allow_ssh_export and sftp is not None:
            log.info(
                "public racing torrent present; "
                "SFTP-exporting %s from VPS1 for SSD download",
                source_torrent.infohash[:10],
            )
            blob = sftp.fetch_torrent(source_torrent.infohash)
            if blob:
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label="public-racing",
                    name=source_torrent.name,
                    size_bytes=source_torrent.size_bytes,
                    infohash=source_torrent.infohash,
                    announce_url=(
                        source_torrent.trackers[0]
                        if source_torrent.trackers else ""
                    ),
                )
        # If allow_ssh_export=false (or SFTP returned nothing for the
        # source_infohash), try to use qBittorrent's WebUI
        # /torrents/export endpoint directly via the source_client.
        try:
            blob = await source_client.export_torrent(source_torrent.infohash)
        except AttributeError:
            blob = None
        except Exception as e:  # noqa: BLE001
            log.warning("qB export_torrent failed for %s: %s",
                        source_torrent.infohash[:10], e)
            blob = None
        if blob:
            log.info(
                "public racing torrent present; "
                "fetched %s via qB export endpoint for SSD download",
                source_torrent.infohash[:10],
            )
            return SourceDecision(
                torrent_bytes=blob,
                source_label="public-racing",
                name=source_torrent.name,
                size_bytes=source_torrent.size_bytes,
                infohash=source_torrent.infohash,
                announce_url=(
                    source_torrent.trackers[0]
                    if source_torrent.trackers else ""
                ),
            )

        # Last resort in the public branch: only if the user explicitly
        # asked for it, fetch a Prowlarr cross-seed copy. This is rare
        # and intended for cases where the racing client's torrent file
        # is unreachable (e.g. VPS1 crash mid-cycle).
        if (cfg.cross_seed.refetch_public_via_prowlarr
                and cfg.cross_seed.allow_prowlarr_cross_seed
                and prowlarr is not None):
            log.warning(
                "racing client's public .torrent unavailable; "
                "falling back to Prowlarr cross-seed for %s",
                source_torrent.name,
            )
            try:
                hit = await prowlarr.best_match(source_torrent.name)
            except Exception as e:  # noqa: BLE001
                log.warning("seedpool search failed for %s: %s",
                            source_torrent.name, e)
                hit = None
            if hit:
                blob = await prowlarr.download_torrent(hit)
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label="public-seedpool-fallback",
                    name=source_torrent.name,
                    size_bytes=hit.size_bytes,
                    infohash=hit.guid,
                    announce_url=hit.download_url,
                )

    # All torrents are private. We only download from VPS2 SSD using a
    # cross-seed from the configured download_indexer ("Seedpool (API)" by default).
    # Private torrents are never downloaded directly on SSD.
    if prowlarr is not None and cfg.cross_seed.allow_prowlarr_cross_seed:
        log.info(
            "private release; querying Prowlarr (%s) for cross-seed of %s",
            cfg.prowlarr.download_indexer,
            source_torrent.name,
        )
        try:
            hit = await prowlarr.best_match(source_torrent.name)
        except Exception as e:  # noqa: BLE001
            log.warning("seedpool search failed for %s: %s",
                        source_torrent.name, e)
            hit = None
        if hit:
            log.info(
                "prowlarr hit: %s (size=%d B, indexer=%s)",
                hit.title, hit.size_bytes, hit.indexer,
            )
            blob = await prowlarr.download_torrent(hit)
            return SourceDecision(
                torrent_bytes=blob,
                source_label="seedpool-cross-seed",
                name=source_torrent.name,
                size_bytes=hit.size_bytes,
                infohash=hit.guid,
                announce_url=hit.download_url,
            )
        if attempt_prowlarr:
            log.info(
                "no prowlarr cross-seed yet for %s; will park and retry",
                source_torrent.name,
            )
            return None

    log.warning(
        "no public torrent and no Seedpool cross-seed available for %s",
        source_torrent.name,
    )
    return None


# --------------------------------------------------------------------------- #
# Coordinator
# --------------------------------------------------------------------------- #


@dataclass
class LiveItem:
    source_infohash: str
    name: str
    state: str
    progress: float
    size_mb: float
    eta: str = ""


@dataclass
class Coordinator:
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

    async def start(self) -> None:
        log.info("coordinator starting")
        self._download_sem = asyncio.Semaphore(self.cfg.max_active_downloads)
        self._move_sem = asyncio.Semaphore(self.cfg.max_concurrent_moves)
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
            elif (self.cfg.cross_seed.allow_ssh_export
                  and self.cfg.source.type == "qbittorrent"):
                self.sftp = None

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
                if failed_rows:
                    log.info(
                        "auto-retrying %d previously-FAILED row(s)",
                        len(failed_rows),
                    )
                    for ts in failed_rows:
                        ts.state = State.NEW
                        ts.last_error = ""
                        self.store.upsert(ts)

            # Optional Telegram bot
            from .telegram_bot import TelegramBot
            self._tg: TelegramBot | None = None
            if self.cfg.telegram.enabled:
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
        if getattr(self, "prowlarr", None) is not None:
            try:
                await self.prowlarr.close()
            except Exception:  # noqa: BLE001
                pass
            self.prowlarr = None
        if getattr(self, "sftp", None) is not None:
            try:
                self.sftp.close()
            except Exception:  # noqa: BLE001
                pass
            self.sftp = None

    async def _list_source_torrents(self) -> list:
        """Fetch racing torrents from VPS1 and apply the min-age filter.

        Single source of truth for "what is syncable from the racing
        client right now". Empty `category` in the config means
        "sync everything"; non-empty means filter by that category.
        `min_age_seconds` further restricts to torrents added recently.
        """
        all_torrents = await self.source_client.list_torrents(
            category=self.cfg.source.category
        )
        min_age = self.cfg.source.min_age_seconds
        if min_age <= 0:
            return all_torrents
        import time as _time
        now = _time.time()
        filtered = []
        for t in all_torrents:
            if t.added_on and (now - t.added_on) < min_age:
                continue
            filtered.append(t)
        return filtered

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
        if getattr(self, "_tg", None) is not None:
            await self._tg.stop()
        if getattr(self, "_api_task", None) is not None:
            self._api_task.cancel()
            try:
                await self._api_task
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

    async def _tick(self) -> None:
        """One iteration: poll sources, schedule work."""
        log.debug("tick: enter")
        # 1. Watch dir (req #3)
        if self.watch is not None:
            for item in await self.watch.scan_once():
                if self.store.get(item.infohash) is None:
                    ts = TorrentState(
                        source_infohash=item.infohash,
                        source_name=item.name,
                        total_bytes=item.size_bytes,
                        state=State.NEW,
                    )
                    self.store.upsert(ts)
                if self.cfg.watch_dir and self.cfg.watch_dir.delete_after_pickup:
                    await self.watch.delete_picked_up(item)

        # 2. Source racing client (req #1 / #2)
        src_torrents = await self._list_source_torrents()
        # Throttled: log source size once every 5 minutes so we can
        # see if the racing client is being polled correctly without
        # spamming the log every poll cycle.
        if not hasattr(self, "_last_source_log_ts"):
            self._last_source_log_ts = 0.0
        import time as _t
        now = _t.monotonic()
        if now - self._last_source_log_ts > 300:
            log.info(
                "source poll: %d torrent(s) matching category=%r min_age=%ds",
                len(src_torrents), self.cfg.source.category,
                self.cfg.source.min_age_seconds,
            )
            self._last_source_log_ts = now
        # Group source torrents by content/release name. Multiple racing
        # torrents for the same content (e.g. public release + multiple
        # private cross-seeds) only produce ONE active SSD download.
        by_name: dict[str, list[Torrent]] = {}
        for st in src_torrents:
            by_name.setdefault(st.name, []).append(st)

        for name, group in by_name.items():
            # Check if any torrent in this release group is already tracked in state store
            existing_ts: TorrentState | None = None
            for t in group:
                ts = self.store.get(t.infohash)
                if ts is not None:
                    existing_ts = ts
                    break
            if existing_ts is None:
                matches = self.store.find_by_name(name)
                if matches:
                    existing_ts = matches[0]

            if existing_ts is not None:
                # Content is already being managed by an existing TorrentState;
                # keep display name fresh
                existing_ts.source_name = name
                continue

            # Elect ONE primary torrent for SSD download:
            # 1. Prefer public torrent if available (req #1)
            # 2. Otherwise pick first private torrent to query Seedpool (req #2)
            primary = next((t for t in group if _looks_public(t.trackers)), group[0])
            is_pub = _looks_public(primary.trackers)

            ts = TorrentState(
                source_infohash=primary.infohash,
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

        # 3. Wake up WAITING_SEEDPOOL rows whose retry timer has elapsed.
        ready_seedpool = self.store.list_seedpool_ready()
        for ts in ready_seedpool:
            if ts.source_infohash in self._running_infohashes:
                continue
            log.info(
                "seedpool retry timer fired for %s (attempt #%d)",
                ts.source_name[:40], ts.seedpool_attempts,
            )
            self.transition(ts, State.QUERYING)
            h = ts.source_infohash
            self._running_infohashes.add(h)
            task = asyncio.create_task(self._process_torrent(ts))
            self._tasks.add(task)
            def _done_cb_seedpool(t: asyncio.Task, infohash: str = h) -> None:
                self._tasks.discard(t)
                self._running_infohashes.discard(infohash)
            task.add_done_callback(_done_cb_seedpool)

        # 4. Schedule workers for active states that have no live task
        active = self.store.all_active()
        scheduled = 0
        max_concurrent_workers = max(
            12,
            self.cfg.max_active_downloads * 2 + self.cfg.max_concurrent_moves * 2,
        )
        available_slots = max(0, max_concurrent_workers - len(self._tasks))

        active_downloads = sum(
            1 for t in active
            if t.source_infohash in self._running_infohashes
            and t.state in (State.QUEUED, State.DOWNLOADING)
        )
        active_moves = sum(
            1 for t in active
            if t.source_infohash in self._running_infohashes
            and t.state == State.MOVING
        )

        for ts in active:
            if available_slots <= 0:
                break
            if ts.source_infohash in self._running_infohashes:
                continue

            # Limit concurrent active qBittorrent additions / downloads on SSD
            if ts.state == State.QUEUED and active_downloads >= self.cfg.max_active_downloads:
                continue

            # Limit concurrent active rclone move commands
            if ts.state == State.MOVING and active_moves >= self.cfg.max_concurrent_moves:
                continue

            h = ts.source_infohash
            self._running_infohashes.add(h)
            task = asyncio.create_task(self._process_torrent(ts))
            self._tasks.add(task)
            def _done_cb(t: asyncio.Task, infohash: str = h) -> None:
                self._tasks.discard(t)
                self._running_infohashes.discard(infohash)
            task.add_done_callback(_done_cb)

            if ts.state in (State.QUEUED, State.DOWNLOADING):
                active_downloads += 1
            elif ts.state == State.MOVING:
                active_moves += 1

            scheduled += 1
            available_slots -= 1
        if scheduled:
            log.info(
                "scheduled %d worker(s) (active downloads=%d/%d, moves=%d/%d)",
                scheduled,
                active_downloads, self.cfg.max_active_downloads,
                active_moves, self.cfg.max_concurrent_moves,
            )

        # 4. Refresh live status (used by the Telegram bot)
        await self._refresh_live_status()

    async def _refresh_live_status(self) -> None:
        """Re-query VPS2 progress and update the live map."""
        try:
            rows = await self.dest_client.list_torrents()
        except Exception as e:  # noqa: BLE001
            log.warning("list_torrents for live status failed: %s", e)
            return
        for t in rows:
            item = self._live.get(t.hash.lower())
            if item is None:
                continue
            item.progress = t.progress
            item.size_mb = t.size_bytes / (1024 * 1024)
            if t.progress > 0.001:
                eta_s = (1.0 - t.progress) * 60  # crude placeholder
                item.eta = f"{eta_s:.0f}m"

    def live_progress_map(self) -> dict[str, float]:
        """Snapshot of in-flight download progress keyed by infohash.

        Used by the Telegram bot for the active-tasks message.
        """
        return {
            h.lower(): item.progress
            for h, item in self._live.items()
        }

    # ---- per-torrent worker ----

    async def _process_torrent(self, ts: TorrentState) -> None:
        try:
            await self._process_torrent_inner(ts)
        except Exception as e:  # noqa: BLE001
            log.exception("worker failed for %s", ts.source_infohash[:10])
            if ts.state != State.FAILED:
                self.transition(ts, State.FAILED, error=str(e)[:500])
            self.store.append_log("ERROR", str(e), ts.source_infohash)
            await self._notify_telegram(ts)

    async def _notify_telegram(self, ts: TorrentState) -> None:
        """Push a state-update to the per-torrent Telegram message."""
        tg = getattr(self, "_tg", None)
        if tg is None:
            return
        try:
            progress = self.live_progress_map().get(ts.source_infohash.lower())
            await tg.ensure_detail_message(ts, progress=progress)
        except Exception as e:  # noqa: BLE001
            log.warning("telegram notify failed for %s: %s",
                        ts.source_infohash[:10], e)

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
        prev = ts.state
        self.store.transition(ts, dst, error=error, batch_index=batch_index)
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
            loop = asyncio.get_running_loop()
            loop.create_task(self._notify_telegram(ts))
        except RuntimeError:
            # No running loop (e.g. during shutdown). Skip.
            pass

    async def _process_torrent_inner(self, ts: TorrentState) -> None:
        log.info("worker start: %s state=%s", ts.source_name, ts.state.value)
        if ts.state == State.NEW:
            await self._do_new(ts)
        if ts.state == State.WAITING_SEEDPOOL:
            await self._do_waiting_seedpool(ts)
        if ts.state == State.WAITING_DISK:
            await self._wait_disk_then_queue(ts)
        if ts.state == State.QUEUED:
            async with self.download_sem:
                await self._do_queued(ts)
                if ts.state == State.DOWNLOADING:
                    await self._do_downloading(ts)
        elif ts.state == State.DOWNLOADING:
            async with self.download_sem:
                await self._do_downloading(ts)
        if ts.state == State.MOVING:
            await self._do_moving(ts)
        if ts.state == State.RE_ADDING:
            await self._do_re_add(ts)

    # ---- state: NEW ----

    async def _do_new(self, ts: TorrentState) -> None:
        # Make sure we have the source torrent metadata
        st = await self.source_client.get_torrent(ts.source_infohash)
        if st is None:
            log.warning("source torrent vanished: %s", ts.source_infohash[:10])
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes
        ts.source_tracker = st.trackers[0] if st.trackers else ""
        ts.source_announce_url = ts.source_tracker or ts.source_announce_url

        # Find other source torrents for the same content (req #1).
        # qB/Deluge don't have a content-id, so heuristic: same name + same
        # total size. We use name match — usually racing has 1-3 dupes.
        all_source = await self._list_source_torrents()
        others = [t for t in all_source
                  if t.infohash != st.infohash and t.name == st.name]

        decision = await pick_ssd_source_for_racing(
            cfg=self.cfg,
            source_torrent=st,
            other_source_torrents=others,
            prowlarr=self.prowlarr,
            sftp=self.sftp,
            source_client=self.source_client,
            attempt_prowlarr=True,
        )
        if decision is None:
            # Seedpool miss + private tracker recognised → park and retry.
            self._park_for_seedpool_retry(ts)
            return

        ts.cross_seed_infohash = decision.infohash
        ts.cross_seed_source = decision.source_label
        ts.save_path = str(self.cfg.dest.save_path)

        # Persist the picked .torrent bytes so that recovery after a
        # restart can re-add the cross-seed torrent. Also keep the
        # transient in-memory copy as a fast-path for the immediate
        # QUEUED stage.
        ts.cross_seed_blob = decision.torrent_bytes
        ts._blob = decision.torrent_bytes

        if not ssd_has_room(self.cfg, decision.size_bytes):
            log.info("ssd cap in use; parking %s", st.name)
            self.transition(ts, State.WAITING_DISK)
        else:
            self.transition(ts, State.QUEUED)

    def _park_for_seedpool_retry(self, ts: TorrentState) -> None:
        """Park into WAITING_SEEDPOOL with an escalating retry timer.

        The first attempt: retry after seedpool_retry_interval_seconds.
        Subsequent attempts: same interval (fixed, not exponential — we
        expect Seedpool to catch up shortly for racing releases).
        Hard cap: seedpool_max_age_seconds since the FIRST attempt. If
        that ceiling is reached, mark FAILED for manual handling.
        """
        now = dt.datetime.now(dt.timezone.utc)
        if ts.seedpool_first_queried_at is None:
            ts.seedpool_first_queried_at = now
        ts.seedpool_attempts += 1
        next_retry = now + dt.timedelta(
            seconds=self.cfg.cross_seed.seedpool_retry_interval_seconds
        )
        ts.seedpool_next_retry_at = next_retry
        max_age = dt.timedelta(seconds=self.cfg.cross_seed.prowlarr_max_age_seconds)
        elapsed = now - ts.seedpool_first_queried_at

        log.info(
            "seedpool miss #%d for %s; next retry at %s (elapsed=%ds, max=%ds)",
            ts.seedpool_attempts, ts.source_name,
            next_retry.isoformat(timespec="seconds"),
            int(elapsed.total_seconds()), int(max_age.total_seconds()),
        )

        if elapsed >= max_age:
            log.error(
                "seedpool giving up on %s after %d attempts (%ds > %ds max)",
                ts.source_name, ts.seedpool_attempts,
                int(elapsed.total_seconds()), int(max_age.total_seconds()),
            )
            # From WAITING_SEEDPOOL → FAILED is legal (see ALLOWED).
            self.transition(
                ts, State.FAILED,
                error=(f"Prowlarr cross-seed not found within "
                       f"{self.cfg.cross_seed.prowlarr_max_age_seconds}s"),
            )
            return

        # If we're being called from _do_new (state is NEW), the
        # transition is legal. If we're being re-called from
        # _do_waiting_seedpool, the state is already WAITING_SEEDPOOL
        # and we just need to bump the retry timestamp.
        if ts.state != State.WAITING_SEEDPOOL:
            self.transition(ts, State.WAITING_SEEDPOOL)
        else:
            self.store.upsert(ts)
            # No transition() fired, so push the updated timer manually.
            self._schedule_telegram_update(ts)

    async def _do_waiting_seedpool(self, ts: TorrentState) -> None:
        """Wake up from WAITING_SEEDPOOL and re-pick the SSD source.

        Called by _tick when the row's seedpool_next_retry_at has elapsed.
        """
        # Pull fresh data from VPS1 in case the torrent name changed.
        st = await self.source_client.get_torrent(ts.source_infohash)
        if st is None:
            log.warning("source torrent vanished: %s", ts.source_infohash[:10])
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes

        all_source = await self._list_source_torrents()
        others = [t for t in all_source
                  if t.infohash != st.infohash and t.name == st.name]

        decision = await pick_ssd_source_for_racing(
            cfg=self.cfg,
            source_torrent=st,
            other_source_torrents=others,
            prowlarr=self.prowlarr,
            sftp=self.sftp,
            source_client=self.source_client,
            attempt_prowlarr=True,
        )
        if decision is None:
            # Still no hit — re-park, escalating the failure to FAILED
            # when the max_age window is exceeded.
            self._park_for_seedpool_retry(ts)
            return

        ts.cross_seed_infohash = decision.infohash
        ts.cross_seed_source = decision.source_label
        ts.save_path = str(self.cfg.dest.save_path)
        ts.cross_seed_blob = decision.torrent_bytes
        ts._blob = decision.torrent_bytes

        if not ssd_has_room(self.cfg, decision.size_bytes):
            self.transition(ts, State.WAITING_DISK)
        else:
            self.transition(ts, State.QUEUED)

    async def _wait_disk_then_queue(self, ts: TorrentState) -> None:
        # The size check uses total_bytes; for seasons the real SSD footprint
        # is bounded by the batch cap. The actual add will re-check.
        while not ssd_has_room(self.cfg, ts.total_bytes):
            await asyncio.sleep(60)
        self.transition(ts, State.QUEUED)
        await self._do_queued(ts)

    # ---- state: QUEUED ----

    async def _do_queued(self, ts: TorrentState) -> None:
        blob: bytes = ts._blob or ts.cross_seed_blob
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
                str(self.cfg.rclone.fuse.mount).rstrip("/"),
                str(self.cfg.rclone.fuse.mount_unsorted).rstrip("/"),
            ]
            save_path = ext.save_path.rstrip("/")
            on_fuse = any(save_path.startswith(fm) for fm in fuse_mounts if fm)
            if on_fuse or ext.is_complete():
                log.info(
                    "torrent %s is already completed on VPS2 (fuse=%s, complete=%s); marking DONE",
                    ts.source_infohash[:10], on_fuse, ext.is_complete(),
                )
                ts.dest_infohash = ext.hash
                ts.save_path = ext.save_path
                self.transition(ts, State.DONE)
                return

            log.info("torrent already downloading on VPS2: %s", ts.source_infohash[:10])
            self.transition(ts, State.DOWNLOADING)
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

        # Wait for the torrent to be registered and learn its hash
        new_hash = await self._await_hash_for_name(ts.source_name)
        if new_hash:
            ts.dest_infohash = new_hash

        # Classify
        files = await self.dest_client.get_torrent_files(ts.dest_infohash or ts.source_infohash)
        cls = classify(files, self.cfg)
        ts.classification_kind = cls.kind

        # Apply batch file priorities for seasons
        if cls.kind in ("season", "mixed"):
            from .classifier import parse_episode  # local import to avoid cycles
            episodes = [e for e in cls.episodes]
            cap = ssd_max_inflight_bytes(self.cfg)
            batches = make_batches(episodes, cap_bytes=cap)
            ts.batches_total = len(batches)
            ts.batch_index = 0
            # First batch only: priority 1; rest: 0
            first = batches[0]
            prio_map = {f.name: 0 for f in files}
            for ep in first.episodes:
                prio_map[ep.file_name] = 1
            await self.dest_client.set_file_priorities(
                ts.dest_infohash or ts.source_infohash, prio_map,
            )

        # Skip movies that are too big (req #7)
        if should_skip_movie(cls, self.cfg):
            log.warning("skipping oversize movie: %s (%d B)",
                        ts.source_name, ts.total_bytes)
            self.transition(
                ts, State.FAILED, error="movie larger than skip threshold",
            )
            return

        # Resume
        await self.dest_client.resume(ts.dest_infohash or ts.source_infohash)
        self.transition(ts, State.DOWNLOADING)

    async def _await_hash_for_name(self, name: str, *, timeout: float = 60) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = await self.dest_client.list_torrents()
            for t in rows:
                if t.name == name:
                    return t.hash
            await asyncio.sleep(2)
        return None

    # ---- state: DOWNLOADING ----

    async def _do_downloading(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        # Live tracking
        self._live[h.lower()] = LiveItem(
            source_infohash=h.lower(),
            name=ts.source_name,
            state="downloading",
            progress=0.0,
            size_mb=ts.total_bytes / (1024 * 1024),
        )

        try:
            await self._wait_for_completion(ts)
        finally:
            self._live.pop(h.lower(), None)

        # If this was a season with batches, advance the batch pointer
        if ts.classification_kind in ("season", "mixed"):
            ts.batch_index += 1
            self.store.upsert(ts)
            if ts.batch_index < ts.batches_total:
                # Set next batch's files to priority 1, drop current to 0
                await self._prepare_next_batch(ts)
                # The torrent is now ready to download the next batch;
                # stay in DOWNLOADING state.
                await self._do_downloading(ts)
                return

        self.transition(ts, State.MOVING)

    async def _wait_for_completion(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        last_log = 0.0
        while not self._stop:
            t = await self.dest_client.get_torrent(h)
            if t is None:
                raise RuntimeError(f"torrent vanished mid-download: {h}")
            self._live[h.lower()].progress = t.progress
            now = time.monotonic()
            if now - last_log > 60:
                log.info("download %s: %.1f%% (%d MB)",
                         ts.source_name, t.progress * 100, t.size_bytes // (1024 * 1024))
                last_log = now
            if t.is_complete():
                return
            await asyncio.sleep(self.cfg.general.dest_poll_interval)

    async def _prepare_next_batch(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        files = await self.dest_client.get_torrent_files(h)
        from .classifier import parse_episode  # noqa: F401
        eps = []
        for f in files:
            se = parse_episode(f.name)
            if se:
                from .classifier import Episode
                eps.append(Episode(f.name, se[0], se[1], f.size_bytes))
        eps.sort(key=lambda e: (e.season, e.episode))
        cap = ssd_max_inflight_bytes(self.cfg)
        batches = make_batches(eps, cap_bytes=cap)
        if ts.batch_index >= len(batches):
            return
        cur = batches[ts.batch_index]
        prio_map = {f.name: 0 for f in files}
        for ep in cur.episodes:
            prio_map[ep.file_name] = 1
        await self.dest_client.set_file_priorities(h, prio_map)
        # Wipe season folder after each batch (req #8)
        season_folder = self._season_folder_for(files, ts.source_name)
        if season_folder:
            await wipe_local_tree(season_folder)

    def _season_folder_for(self, files: list, torrent_name: str) -> Path | None:
        if not files:
            return None
        # The top-most folder path shared by all files
        first = files[0].name
        parts = first.split("/")
        if len(parts) <= 1:
            return None
        top = parts[0]
        if all(f.name.startswith(top + "/") for f in files):
            return Path(self.cfg.dest.save_path) / top
        return None

    # ---- state: MOVING ----

    async def _do_moving(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        cls_files = await self.dest_client.get_torrent_files(h)
        cls = classify(cls_files, self.cfg)

        # 1. Remove torrent from VPS2 client BEFORE move begins (delete_files=False).
        # This closes file handles and stops seeding from the SSD, avoiding I/O errors.
        log.info(
            "pausing and deleting torrent %s from VPS2 client before move (delete_files=False)",
            h[:10],
        )
        try:
            await self.dest_client.pause(h)
            await self.dest_client.delete(h, delete_files=False)
        except Exception as e:  # noqa: BLE001
            log.warning("could not delete torrent from client before move: %s", e)

        # 2. Separate completed files from incomplete piece-boundary files
        src_dir = Path(self.cfg.dest.save_path)
        folder = self._season_folder_for(cls_files, ts.source_name)
        content_dir = folder if folder and folder.exists() else src_dir

        completed_files: list[TorrentFile] = []
        incomplete_files: list[TorrentFile] = []

        for f in cls_files:
            file_path = src_dir / f.name
            if not file_path.exists():
                continue
            # A file is complete if progress >= 0.999 or its size on disk matches expected size
            if f.progress >= 0.999 or file_path.stat().st_size >= f.size_bytes:
                completed_files.append(f)
            else:
                incomplete_files.append(f)

        # 3. Clean up incomplete piece-boundary files so they are NOT moved to remote
        for f in incomplete_files:
            file_path = src_dir / f.name
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

        # Also purge any leftover temporary extension files like .!qB or .parts
        if content_dir.exists():
            for temp_file in list(content_dir.glob("**/*.!qB")) + list(content_dir.glob("**/*.parts")):
                try:
                    temp_file.unlink()
                except OSError:
                    pass

        # 4. Decide target remote
        if cls.kind in ("season", "mixed") and ts.classification_kind != "season":
            # We moved per-batch; nothing left to do
            self.transition(ts, State.RE_ADDING)
            return

        if cls.kind == "movie" or cls.kind == "season":
            remote = self.cfg.rclone.remote.default
        else:
            remote = self.cfg.rclone.remote.unsorted

        # 5. Move completed files via rclone
        if cls.kind in ("movie", "season", "unknown"):
            if cls.kind == "movie" and cls.single_file:
                local = src_dir / cls.single_file
                if not local.exists():
                    if folder and (folder / cls.single_file).exists():
                        local = folder / cls.single_file
                    else:
                        raise FileNotFoundError(f"completed movie file not found on SSD: {local}")
            elif cls.kind in ("season", "unknown") and folder and folder.exists():
                local = folder
            else:
                cand = src_dir / ts.source_name
                local = cand if cand.exists() else src_dir
            await self._rclone_move(local, remote, ts)
        else:
            # Mixed — per-episode moves with --include
            cap = ssd_max_inflight_bytes(self.cfg)
            episodes = cls.episodes
            batches = make_batches(episodes, cap_bytes=cap)
            for i, batch in enumerate(batches):
                ts.batch_index = i
                ts.batches_total = len(batches)
                self.store.upsert(ts)
                await self._rclone_move(
                    src_dir, remote, ts, include=batch.include_patterns(),
                )

        # 6. Delete local content folder on SSD after move
        if folder and folder.resolve() != src_dir.resolve() and folder.exists():
            log.info("deleting content folder after move: %s", folder)
            await wipe_local_tree(folder)

        self.transition(ts, State.RE_ADDING)

    async def _rclone_move(self, local: Path, remote: str, ts: TorrentState,
                           *, include: list[str] | None = None) -> None:
        async with self.move_sem:
            log.info("rclone move %s -> %s (include=%s)", local, remote, include)
            res = await move_local_to_remote(self.cfg, local, remote, include=include)
            if not res.ok:
                err = res.stderr.strip()
                last_err = [ln.strip() for ln in err.splitlines() if ln.strip()][-1] if err else f"rc={res.returncode}"
                raise RuntimeError(f"rclone failed (rc={res.returncode}): {last_err}")

    # ---- state: RE_ADDING ----

    async def _do_re_add(self, ts: TorrentState) -> None:
        delay = self.cfg.fuse_reinject_delay_seconds
        if delay > 0:
            log.info(
                "waiting %ds for fuse mount indexing before re-injection: %s",
                delay, ts.source_name[:50],
            )
            await asyncio.sleep(delay)

        h = ts.dest_infohash or ts.source_infohash

        # 1) Re-inject the racing-client torrents (private or otherwise)
        # pointing at the fuse mount with skip_check=True (req #3).
        # We need their .torrent bytes. Pull them via SFTP if possible;
        # otherwise just skip if the racing client was deluge and SFTP
        # is not enabled.
        if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
            await self._re_inject_racing_torrents(ts)

        # 2) Re-add the cross-seed torrent (the one we used to download
        # on SSD) pointing at the fuse mount. Without re-check.
        # Prefer the in-memory copy (set during the current run);
        # fall back to the persisted copy so that recovery after a
        # restart can still re-add the cross-seed torrent.
        blob = ts._blob or ts.cross_seed_blob
        if blob:
            target_mount = self._target_mount_for(ts)
            await self.dest_client.add_torrent(
                torrent_files=[blob],
                save_path=str(target_mount),
                category="racing",
                paused=False,
                skip_check=True,
                tags=["racing", "fuse"],
            )

        self.transition(ts, State.DONE)

    async def _re_inject_racing_torrents(self, ts: TorrentState) -> None:
        """Re-add every racing-client torrent matching this content onto VPS2
        pointing at the fuse mount with skip_check=True.

        Req #1 + #2 + #3: when VPS1 has multiple racing torrents for the
        same file (e.g. one public + one or more private), we want ALL of
        them seeding from VPS2 once the SSD download + rclone move
        complete.

        Implementation notes:
          - The .torrent bytes are fetched via SFTP (Deluge state dir)
            when available. For qBittorrent we use the WebUI's
            /torrents/export endpoint directly.
          - We avoid re-adding the SSD-source torrent if its infohash
            already lives on VPS2 (the cross-seed torrent we used for
            SSD download is also re-added later as `seedpool-cross-seed`
            or `public-racing` in the calling code).
          - Errors fetching individual torrents are logged and skipped,
            so a single bad export doesn't fail the whole injection.
        """
        target_mount = self._target_mount_for(ts)
        injected: list[str] = []

        # 1. List racing torrents for this content
        try:
            racing = await self._list_source_torrents()
        except Exception as e:  # noqa: BLE001
            log.warning("could not list racing torrents for re-injection: %s", e)
            return

        # Match by display name (the racing client shows release names).
        # Note: VPS1 may have e.g. a public torrent + Aither + Beyond-HD
        # copies of the same release, all with identical display names.
        matches = [t for t in racing if t.name == ts.source_name]

        # Also fetch the SSD-source torrent itself if it isn't in `matches`
        # (e.g. because it was a cross-seed, not in the racing list).
        cross_seed_infohash = ts.cross_seed_infohash
        seen = {t.infohash.lower() for t in matches}
        if cross_seed_infohash and cross_seed_infohash.lower() not in seen:
            # Construct a synthetic Torrent just for re-injection.
            matches.append(Torrent(
                hash=cross_seed_infohash,
                name=ts.source_name,
                category="racing",
                save_path="",
                size_bytes=ts.total_bytes,
                state="",
                progress=1.0,
                trackers=[],
            ))

        for t in matches:
            try:
                blob = await self._fetch_racing_torrent_bytes(t.infohash)
            except Exception as e:  # noqa: BLE001
                log.warning("re-inject: fetch %s failed: %s",
                            t.infohash[:10], e)
                continue
            if not blob:
                continue
            try:
                await self.dest_client.add_torrent(
                    torrent_files=[blob],
                    save_path=str(target_mount),
                    category="racing",
                    paused=False,
                    skip_check=True,
                    tags=["racing", "fuse"],
                )
                injected.append(t.infohash)
                log.info(
                    "re-injected racing torrent %s (%s) on fuse",
                    t.infohash[:10], t.name[:50],
                )
            except Exception as e:  # noqa: BLE001
                log.warning("re-inject: add %s failed: %s",
                            t.infohash[:10], e)

        ts.injected_private_hashes = ",".join(injected)

    async def _fetch_racing_torrent_bytes(self, infohash: str) -> bytes | None:
        """Fetch the raw .torrent bytes for a racing-client infohash.

        Tries in order:
          1. SFTP exporter (Deluge state dir, qB BT_backup)
          2. The source client's own export endpoint (qB /api/v2/torrents/export,
             Deluge core.get_torrent_file).
        """
        if self.sftp is not None:
            blob = self.sftp.fetch_torrent(infohash)
            if blob:
                return blob
        try:
            return await self.source_client.export_torrent(infohash)
        except AttributeError:
            return None
        except Exception:  # noqa: BLE001
            return None

    def _target_mount_for(self, ts: TorrentState) -> Path:
        """Where on the fuse mount should this torrent's data live?"""
        if ts.classification_kind == "movie" or ts.classification_kind == "season":
            return Path(self.cfg.rclone.fuse.mount)
        return Path(self.cfg.rclone.fuse.mount_unsorted)
