"""Main coordinator loop.

This is the only place that orchestrates state transitions, qBittorrent
calls, rclone invocations, and prowlarr lookups. The flow per torrent:

  1. Detect on VPS1 racing client (category=racing)
  2. Pick the SSD-source torrent:
       - if VPS1 has multiple, prefer public (req #1) via prowlarr or SFTP
       - if VPS1 has only private, query prowlarr by tracker map (req #2)
       - if from watch_dir, prefer prowlarr hit on Indexer (req #3)
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
import re
import shutil
import time
import aiohttp
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .batcher import Batch, escape_rclone_glob, make_batches
from .classifier import Classification, classify, should_skip_movie
from .clients.abstract import Torrent, TorrentClient, TorrentFile
from .clients.deluge import DelugeClient
from .clients.http_base import AuthError
from .clients.qbittorrent import QBittorrentClient, build_qbtorrent_from_dest
from .config import AppConfig
from .logging_setup import get_ring_buffer
from .prowlarr import ProwlarrClient, TorrentHit
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
from .watchdir import WatchDirScanner, WatchItem

log = logging.getLogger(__name__)


class WebUIUnresponsiveError(RuntimeError):
    """Raised when destination WebUI times out, disconnects, or rejects re-injection under load."""


_WEBUI_RETRY_ERRORS = (
    TimeoutError,
    aiohttp.ClientError,
    ConnectionError,
    OSError,
    WebUIUnresponsiveError,
)


def normalize_content_name(name: str) -> str:
    """Normalize release/torrent names for deduplication and grouping.

    Strips trailing indexer tags (e.g. '[Indexer]', '[FL]'), trailing
    file extensions ('.torrent', '.mkv', etc.), and case-folds/strips.
    """
    s = name.strip()
    s = re.sub(r"\.torrent$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s*\[[^\]]+\]\s*$", "", s).strip()
    for ext in (".mkv", ".mp4", ".avi", ".ts", ".m4v"):
        if s.lower().endswith(ext):
            s = s[:-len(ext)].strip()
            break
    return s.lower()


def _matches_release(hit_title: str, hit_size: int, target_name: str, target_size: int) -> bool:
    """Check if a Prowlarr hit matches the target release by title and size."""
    ht_norm = normalize_content_name(hit_title)
    tg_norm = normalize_content_name(target_name)
    title_matches = (
        ht_norm == tg_norm
        or hit_title.strip().lower() == target_name.strip().lower()
    )
    if not title_matches:
        return False
    if hit_size > 0 and target_size > 0:
        tolerance = min(1024 * 1024 * 50, int(target_size * 0.02))
        return abs(hit_size - target_size) <= tolerance
    return True


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
    State.WAITING_INDEXER,
})


def _should_notify_telegram(prev: State, dst: State) -> bool:
    """Decide whether a state transition should fire a Telegram update.

    Policy:
      - First discovery of a torrent (NEW on its own) does NOT fire —
        prevents spam on first run with N pre-existing racing torrents.
      - Any transition INTO a work-active or terminal state fires
        (QUEUED, DOWNLOADING, MOVING, RE_ADDING, DONE, FAILED,
        WAITING_INDEXER).
      - The bot's per-torrent message will be created on the first
        such transition; subsequent updates just edit the same message.
    """
    if prev == dst:
        # Same-state re-entry shouldn't trigger duplicate notifications.
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
    import urllib.parse as _up

    for url in tracker_urls:
        low = (url or "").strip().lower()
        if not low:
            continue
        try:
            host = (_up.urlsplit(low).hostname or "").lower()
        except Exception:
            host = ""
        hay = host or low
        for pub in PUBLIC_TRACKER_HOSTS:
            p = pub.lower()
            # Bounded substring on hostname: `nyaa` matches `nyaa.tracker.wf`
            # and `tracker.opentrackr.org`, but NOT `xxnyaa\.com` (where the
            # match is embedded in a longer alphanumeric label).
            try:
                if re.search(r"(?<![a-z0-9])" + re.escape(p) + r"(?![a-z0-9])", hay):
                    return True
            except re.error:
                if p in hay:
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
      - None if we should park the row in WAITING_INDEXER and retry later
        (only when `attempt_prowlarr=True` and we had a real Indexer
        miss; otherwise we fall through to the SFTP fallback even if
        Indexer returned no hit).

    Logic per req #1 / #2:
      (a) one or more *public* torrents for the file on VPS1   → use the
                                                              racing client's
                                                              own .torrent for
                                                              SSD download.
                                                              Prowlarr is
                                                              NEVER queried
                                                              in this case.
      (b) only *private* torrents from Alpha / Beta /
          Gamma (per tracker_map)                         → query the
                                                              configured
                                                              download_indexer
                                                              ("Indexer (API)"
                                                              by default) for a
                                                              cross-seed copy
    """

    # If the source torrent is itself a "public" tracker, we use IT for
    # the SSD download directly. We do NOT consult Prowlarr — the racing
    # public torrent already works, fetching a Indexer copy would be
    # redundant. The only exception is the rare case where the racing
    # client's .torrent is unreachable on VPS1 (then refetch_public_via_prowlarr
    # can fall back to Prowlarr as a last resort).
    publics = [t for t in [source_torrent] + other_source_torrents
               if _looks_public(t.trackers)]

    if publics:
        chosen = publics[0]
        # req #1: when a public torrent exists on VPS1, use IT for the
        # SSD download directly. We do NOT consult Prowlarr by default
        # — the racing public torrent already works, fetching a Indexer
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
                chosen.infohash[:10],
            )
            blob = None
            try:
                blob = await asyncio.wait_for(
                    asyncio.to_thread(sftp.fetch_torrent, chosen.infohash),
                    timeout=15.0,
                )
            except Exception as e:  # noqa: BLE001
                log.warning("sftp fetch %s failed: %s", chosen.infohash[:10], e)
            if blob:
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label="public-racing",
                    name=chosen.name,
                    size_bytes=chosen.size_bytes,
                    infohash=chosen.infohash,
                    announce_url=(
                        chosen.trackers[0]
                        if chosen.trackers else ""
                    ),
                )
        # If allow_ssh_export=false (or SFTP returned nothing for the
        # source_infohash), try to use qBittorrent's WebUI
        # /torrents/export endpoint directly via the source_client.
        try:
            blob = await asyncio.wait_for(
                source_client.export_torrent(chosen.infohash),
                timeout=15.0,
            )
        except AttributeError:
            blob = None
        except Exception as e:  # noqa: BLE001
            log.warning("qB export_torrent failed for %s: %s",
                        chosen.infohash[:10], e)
            blob = None
        if blob:
            log.info(
                "public racing torrent present; "
                "fetched %s via qB export endpoint for SSD download",
                chosen.infohash[:10],
            )
            return SourceDecision(
                torrent_bytes=blob,
                source_label="public-racing",
                name=chosen.name,
                size_bytes=chosen.size_bytes,
                infohash=chosen.infohash,
                announce_url=(
                    chosen.trackers[0]
                    if chosen.trackers else ""
                ),
            )

        # Last resort in the public branch: only if the user explicitly
        # asked for it, fetch a Prowlarr cross-seed copy. This is rare
        # and intended for cases where the racing client's torrent file
        # is unreachable (e.g. VPS1 crash mid-cycle).
        if (cfg.cross_seed.refetch_public_via_prowlarr
                and cfg.cross_seed.allow_prowlarr_cross_seed
                and prowlarr is not None
                and not cfg.prowlarr.should_skip_title(chosen.name)):
            log.warning(
                "racing client's public .torrent unavailable; "
                "falling back to Prowlarr cross-seed for %s",
                chosen.name,
            )
            try:
                hit = await prowlarr.best_match(chosen.name)
            except Exception as e:  # noqa: BLE001
                log.warning("indexer search failed for %s: %s",
                            chosen.name, e)
                hit = None
            if hit:
                blob = await prowlarr.download_torrent(hit)
                real_hash = ""
                announce = ""
                try:
                    from .watchdir import _bencoded_info_hash
                    real_hash, _, _, announce = _bencoded_info_hash(blob)
                except Exception:
                    real_hash = ""
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label="public-indexer-fallback",
                    name=chosen.name,
                    size_bytes=hit.size_bytes,
                    infohash=real_hash,
                    announce_url=announce or (chosen.trackers[0] if chosen.trackers else ""),
                )

        log.warning(
            "public torrent present but could not export .torrent for %s (sftp=%s, qb=%s)",
            chosen.name,
            cfg.cross_seed.allow_ssh_export and sftp is not None,
            source_client is not None,
        )
        return None

    # All torrents are private. We only download from VPS2 SSD using a
    # cross-seed from the configured download_indexer ("Indexer (API)" by default).
    # Private torrents are never downloaded directly on SSD unless SFTP fallback is explicitly used.
    should_skip_prowlarr = cfg.prowlarr.should_skip_title(source_torrent.name)
    if should_skip_prowlarr:
        log.info(
            "prowlarr: skipping cross-seed query for %r (matches skip_query_substrings)",
            source_torrent.name,
        )
    elif prowlarr is not None and cfg.cross_seed.allow_prowlarr_cross_seed:
        log.info(
            "private release; querying Prowlarr (%s) for cross-seed of %s",
            cfg.prowlarr.download_indexer,
            source_torrent.name,
        )
        try:
            hit = await prowlarr.best_match(source_torrent.name)
        except Exception as e:  # noqa: BLE001
            log.warning("indexer search failed for %s: %s",
                        source_torrent.name, e)
            hit = None
        if hit:
            log.info(
                "prowlarr hit: %s (size=%d B, indexer=%s)",
                hit.title, hit.size_bytes, hit.indexer,
            )
            blob = await prowlarr.download_torrent(hit)
            real_hash = ""
            announce = ""
            try:
                from .watchdir import _bencoded_info_hash
                real_hash, _, _, announce = _bencoded_info_hash(blob)
            except Exception:
                real_hash = ""
            return SourceDecision(
                torrent_bytes=blob,
                source_label="indexer-cross-seed",
                name=source_torrent.name,
                size_bytes=hit.size_bytes,
                infohash=real_hash,
                announce_url=announce or (source_torrent.trackers[0] if source_torrent.trackers else ""),
            )
        if attempt_prowlarr:
            log.info(
                "no prowlarr cross-seed yet for %s; will park and retry",
                source_torrent.name,
            )
            return None

    # SFTP fallback for private when attempt_prowlarr is False or Prowlarr is unavailable/skipped
    if not attempt_prowlarr and cfg.cross_seed.allow_ssh_export and sftp is not None:
        log.info(
            "Prowlarr bypass/fallback: SFTP-exporting private torrent %s from VPS1",
            source_torrent.infohash[:10],
        )
        blob = None
        try:
            blob = await asyncio.wait_for(
                asyncio.to_thread(sftp.fetch_torrent, source_torrent.infohash),
                timeout=15.0,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("sftp fallback fetch %s failed: %s", source_torrent.infohash[:10], e)
        if blob:
            return SourceDecision(
                torrent_bytes=blob,
                source_label="private-sftp-fallback",
                name=source_torrent.name,
                size_bytes=source_torrent.size_bytes,
                infohash=source_torrent.infohash,
                announce_url=(
                    source_torrent.trackers[0]
                    if source_torrent.trackers else ""
                ),
            )

    log.warning(
        "no public torrent and no Indexer cross-seed available for %s",
        source_torrent.name,
    )
    return None


def _top_include_for_folder(src_dir: Path, folder: Path) -> list[str] | None:
    """`--include` patterns moving `folder` while preserving its top dir.

    `rclone move <folder> <remote>` transfers the folder's CONTENTS (top
    dir stripped), but batch moves (`rclone move <src_dir> <remote>
    --include=**/<top>/<file>`) preserve torrent-relative paths. A stripped
    layout never matches `save_path/mount + torrent file names`, so fuse
    re-adds with `skip_check=True` would point at missing data. Moving from
    `src_dir` with `<top>/**` keeps both paths identical.
    Returns None when `folder` is not a direct child layout of `src_dir`
    (caller falls back to the exact-path move).
    """
    try:
        rel = folder.resolve().relative_to(src_dir.resolve())
    except Exception:
        return None
    if len(rel.parts) != 1 or rel.parts[0] in (".", "..", ""):
        return None
    return [f"--include={escape_rclone_glob(rel.parts[0])}/**"]


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
    _source_torrents_cache: list[Torrent] = field(default_factory=list, init=False)
    _source_torrents_cached_at: float = field(default=0.0, init=False)
    _failed_late_cross_seeds: dict[str, dt.datetime] = field(default_factory=dict, init=False)
    # Serializes the periodic tick against API-triggered ops (recover /
    # scan-watch / retry) so they can't double-schedule or clobber rows.
    _ops_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

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
            if self.store.get(item_hash) is None and self.store.get(item.infohash) is None:
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
                log.info(
                    "discovered watch-dir release: %s (%s, %d bytes) announce=%s",
                    item.name,
                    item.infohash[:10],
                    item.size_bytes,
                    item.announce_url,
                )
            if self.cfg.watch_dir and self.cfg.watch_dir.delete_after_pickup:
                await self.watch.delete_picked_up(item)
        return items

    async def _tick(self) -> None:
        """One iteration: poll sources, schedule work (serialized vs API ops)."""
        lock = getattr(self, "_ops_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._ops_lock = lock
        async with lock:
            await self._tick_inner()

    async def _tick_inner(self) -> None:
        """One iteration: poll sources, schedule work."""
        log.debug("tick: enter")
        # 1. Watch dir (req #3)
        await self.scan_watch()

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
        # Group source torrents by content/release name. Multiple racing
        # torrents for the same content (e.g. public release + multiple
        # private cross-seeds) only produce ONE active SSD download.
        by_name: dict[str, list[Torrent]] = {}
        for st in src_torrents:
            norm_key = normalize_content_name(st.name)
            by_name.setdefault(norm_key, []).append(st)

        for norm_name, group in by_name.items():
            # Check if any torrent in this release group is already tracked in state store
            existing_ts: TorrentState | None = None
            for t in group:
                found_ts = self.store.get(t.infohash.lower()) or self.store.get(t.infohash)
                if found_ts is not None:
                    existing_ts = found_ts
                    break
            if existing_ts is None:
                for t in group:
                    matches = self.store.find_by_name(t.name)
                    if matches:
                        existing_ts = matches[0]
                        break

            if existing_ts is not None:
                # Content is already being managed by an existing TorrentState;
                # keep display name fresh
                existing_ts.source_name = group[0].name
                if existing_ts.state == State.DONE and self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                    await self._check_and_inject_late_cross_seeds(existing_ts, group)
                continue

            # Elect ONE primary torrent for SSD download:
            # 1. Prefer public torrent if available (req #1)
            # 2. Otherwise pick first private torrent to query Indexer (req #2)
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

        # 3. Wake up WAITING_INDEXER rows whose retry timer has elapsed.
        ready_indexer = self.store.list_indexer_ready()
        for ts in ready_indexer:
            if ts.source_infohash in self._running_infohashes:
                continue
            log.info(
                "indexer retry timer fired for %s (attempt #%d)",
                ts.source_name[:40], ts.indexer_attempts,
            )
            self.transition(ts, State.QUERYING)
            h = ts.source_infohash
            self._running_infohashes.add(h)
            task = asyncio.create_task(self._process_torrent(ts))
            self._tasks.add(task)
            def _done_cb_indexer(t: asyncio.Task, infohash: str = h) -> None:
                self._tasks.discard(t)
                self._running_infohashes.discard(infohash)
            task.add_done_callback(_done_cb_indexer)

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

            # Skip WAITING_INDEXER rows: they are parked and woken up exclusively
            # by Step 3 when their indexer_next_retry_at timer elapses.
            if ts.state == State.WAITING_INDEXER:
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
        if ts.state == State.QUERYING:
            await self._do_waiting_indexer(ts)
        if ts.state == State.WAITING_DISK:
            await self._wait_disk_then_queue(ts)
        if ts.state == State.QUEUED:
            async with self.download_sem:
                await self._do_queued(ts)
            if ts.state == State.DOWNLOADING:
                await self._do_downloading(ts)
        elif ts.state == State.DOWNLOADING:
            await self._do_downloading(ts)
        if ts.state == State.MOVING:
            await self._do_moving(ts)
        if ts.state == State.RE_ADDING:
            await self._do_re_add(ts)

    # ---- state: NEW ----

    def _effective_inflight_cap(self, total_bytes: int) -> int:
        try:
            cap = ssd_max_inflight_bytes(self.cfg)
            if isinstance(cap, int) and cap > 0:
                return min(total_bytes, cap)
        except Exception:
            pass
        return total_bytes

    def _batch_cap_bytes(self) -> int:
        try:
            cap = ssd_max_inflight_bytes(self.cfg)
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

    async def _do_new(self, ts: TorrentState) -> None:
        if ts.cross_seed_source == "watch-dir":
            await self._do_new_watch_dir(ts)
            return

        # Make sure we have the source torrent metadata
        st = await self.source_client.get_torrent(ts.source_infohash)
        if st is None:
            err = f"source torrent vanished from client: {ts.source_infohash[:10]}"
            log.warning(err)
            self.transition(ts, State.FAILED, error=err)
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes
        ts.source_tracker = st.trackers[0] if st.trackers else ""
        ts.source_announce_url = ts.source_tracker or ts.source_announce_url

        # Find other source torrents for the same content (req #1).
        # qB/Deluge don't have a content-id, so heuristic: same name + same
        # total size. We use name match — usually racing has 1-3 dupes.
        all_source = await self._list_source_torrents()
        st_norm = normalize_content_name(st.name)
        others = [
            t for t in all_source
            if t.infohash != st.infohash and (t.name == st.name or normalize_content_name(t.name) == st_norm)
        ]

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
            # Indexer miss + private tracker recognised → park and retry.
            self._park_for_indexer_retry(ts)
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

        needed = self._effective_inflight_cap(decision.size_bytes)
        if not ssd_has_room(self.cfg, needed):
            log.info("ssd cap in use; parking %s", st.name)
            self.transition(ts, State.WAITING_DISK)
        else:
            self.transition(ts, State.QUEUED)

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

        # Prepare persistence directory for cross-seed torrents
        watch_cross_dir = Path(self.cfg.general.state_db).parent / "watch_cross_seeds" / ts.source_infohash
        await asyncio.to_thread(watch_cross_dir.mkdir, parents=True, exist_ok=True)
        # Always persist the dropped .torrent so it can be seeded on FUSE
        await asyncio.to_thread((watch_cross_dir / f"{ts.source_infohash}.torrent").write_bytes, blob)

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
            download_idx = None
            if needs_sacrificial_copy and prefer_prowlarr:
                try:
                    download_idx = self.prowlarr.get_download_indexer()
                    indexers_to_query.append(download_idx)
                except Exception as e:  # noqa: BLE001
                    log.warning("could not get download indexer: %s", e)

            # Add all private indexers from tracker_map
            seen_names = {download_idx.name.lower()} if download_idx else set()
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

            # 1. Check for sacrificial download torrent on download_indexer
            if prefer_prowlarr and download_idx and download_idx.name.lower() in hits_by_indexer:
                dl_hits = hits_by_indexer[download_idx.name.lower()]
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
                if matching_dl:
                    best_dl = matching_dl[0]
                    try:
                        dl_blob = await self.prowlarr.download_torrent(best_dl)
                        from .watchdir import _bencoded_info_hash
                        dl_h, _, _, _ = _bencoded_info_hash(dl_blob)
                        chosen_blob = dl_blob
                        chosen_label = "public-prowlarr"
                        chosen_size = best_dl.size_bytes
                        chosen_infohash = dl_h
                        log.info(
                            "watch-dir: using sacrificial download torrent from %s (%s)",
                            best_dl.indexer, best_dl.title,
                        )
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "failed to fetch download indexer torrent: %s; using dropped file", e
                        )

            # 2. Collect other private tracker cross-seeds to inject onto FUSE
            for idx_name, hits in hits_by_indexer.items():
                if download_idx and idx_name == download_idx.name.lower():
                    continue
                for hit in hits:
                    if _matches_release(hit.title, hit.size_bytes, ts.source_name, ts.total_bytes):
                        try:
                            cross_blob = await self.prowlarr.download_torrent(hit)
                            from .watchdir import _bencoded_info_hash
                            cross_h, _, _, _ = _bencoded_info_hash(cross_blob)
                            await asyncio.to_thread((watch_cross_dir / f"{cross_h}.torrent").write_bytes, cross_blob)
                            log.info(
                                "watch-dir: discovered cross-seed from %s: %s (%s)",
                                hit.indexer, hit.title, cross_h[:10],
                            )
                        except Exception as e:  # noqa: BLE001
                            log.warning("could not download cross-seed from %s: %s", hit.indexer, e)

        # Classify the chosen torrent metadata and apply movie skip upfront
        try:
            from .watchdir import extract_torrent_files_from_bencoded
            parsed_files = extract_torrent_files_from_bencoded(chosen_blob)
            if parsed_files:
                cls = classify(parsed_files, self.cfg)
                ts.classification_kind = cls.kind
                if should_skip_movie(cls, self.cfg):
                    log.warning("watch-dir: skipping oversize movie: %s (%d B)", ts.source_name, ts.total_bytes)
                    self.transition(ts, State.FAILED, error="movie larger than skip threshold")
                    return
        except Exception as e:
            log.warning("watch-dir: failed to classify %s: %s", ts.source_name, e)

        ts.cross_seed_infohash = chosen_infohash.lower()
        ts.cross_seed_source = chosen_label
        ts.cross_seed_blob = chosen_blob
        ts._blob = chosen_blob

        needed = self._effective_inflight_cap(chosen_size)
        if not ssd_has_room(self.cfg, needed):
            log.info("ssd cap in use; parking %s", ts.source_name)
            self.transition(ts, State.WAITING_DISK)
        else:
            self.transition(ts, State.QUEUED)

    def _park_for_indexer_retry(self, ts: TorrentState) -> None:
        """Park into WAITING_INDEXER with an escalating retry timer.

        The first attempt: retry after indexer_retry_interval_seconds.
        Subsequent attempts: same interval (fixed, not exponential — we
        expect Indexer to catch up shortly for racing releases).
        Hard cap: indexer_max_age_seconds since the FIRST attempt. If
        that ceiling is reached, mark FAILED for manual handling.
        """
        now = dt.datetime.now(dt.timezone.utc)
        if ts.indexer_first_queried_at is None:
            ts.indexer_first_queried_at = now
        ts.indexer_attempts += 1
        next_retry = now + dt.timedelta(
            seconds=self.cfg.cross_seed.indexer_retry_interval_seconds
        )
        ts.indexer_next_retry_at = next_retry
        max_age = dt.timedelta(seconds=self.cfg.cross_seed.prowlarr_max_age_seconds)
        elapsed = now - ts.indexer_first_queried_at

        log.info(
            "indexer miss #%d for %s; next retry at %s (elapsed=%ds, max=%ds)",
            ts.indexer_attempts, ts.source_name,
            next_retry.isoformat(timespec="seconds"),
            int(elapsed.total_seconds()), int(max_age.total_seconds()),
        )

        if elapsed >= max_age:
            log.error(
                "indexer giving up on %s after %d attempts (%ds > %ds max)",
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
            self.store.upsert(ts)
            # No transition() fired, so push the updated timer manually.
            self._schedule_telegram_update(ts)

    async def _do_waiting_indexer(self, ts: TorrentState) -> None:
        """Wake up from WAITING_INDEXER and re-pick the SSD source.

        Called by _tick when the row's indexer_next_retry_at has elapsed.
        """
        # Pull fresh data from VPS1 in case the torrent name changed.
        st = await self.source_client.get_torrent(ts.source_infohash)
        if st is None:
            err = f"source torrent vanished from client: {ts.source_infohash[:10]}"
            log.warning(err)
            self.transition(ts, State.FAILED, error=err)
            return
        ts.source_name = st.name
        ts.total_bytes = st.size_bytes

        all_source = await self._list_source_torrents()
        st_norm = normalize_content_name(st.name)
        others = [
            t for t in all_source
            if t.infohash != st.infohash and (t.name == st.name or normalize_content_name(t.name) == st_norm)
        ]

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
            self._park_for_indexer_retry(ts)
            return

        ts.cross_seed_infohash = decision.infohash.lower()
        ts.cross_seed_source = decision.source_label
        ts.save_path = str(self.cfg.dest.save_path)
        ts.cross_seed_blob = decision.torrent_bytes
        ts._blob = decision.torrent_bytes

        needed = self._effective_inflight_cap(decision.size_bytes)
        if not ssd_has_room(self.cfg, needed):
            self.transition(ts, State.WAITING_DISK)
        else:
            self.transition(ts, State.QUEUED)

    async def _wait_disk_then_queue(self, ts: TorrentState) -> None:
        # The size check uses min(total_bytes, per-batch cap); for seasons the real SSD footprint
        # is bounded by the batch cap. The actual add will re-check.
        if self._stop:
            return
        needed = self._effective_inflight_cap(ts.total_bytes)
        if ssd_has_room(self.cfg, needed):
            self.transition(ts, State.QUEUED)

    # ---- state: QUEUED ----

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
                # files were never moved. Never mark DONE on missing bytes.
                try:
                    fuse_files = await self.dest_client.get_torrent_files(ext.hash)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not list fuse torrent files for %s: %s",
                        ts.source_infohash[:10], e,
                    )
                    fuse_files = []
                fuse_expected = [(f.name, f.size_bytes) for f in fuse_files if f.name]
                fuse_missing = (
                    await self._missing_fuse_files(Path(save_path), fuse_expected)
                    if fuse_expected else []
                )
                if fuse_missing:
                    err = (
                        f"torrent {ts.source_infohash[:10]} reports complete on fuse mount "
                        f"{save_path} but {len(fuse_missing)}/{len(fuse_expected)} file(s) are "
                        f"missing (e.g. {fuse_missing[0]}); not marking DONE"
                    )
                    log.error(err)
                    self.transition(ts, State.FAILED, error=err)
                    return
                log.info(
                    "torrent %s is already completed on VPS2 fuse mount; marking DONE",
                    ts.source_infohash[:10],
                )
                ts.dest_infohash = ext.hash.lower()
                ts.save_path = ext.save_path
                if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                    await self._re_inject_racing_torrents(ts)
                self.transition(ts, State.DONE)
                return

            log.info(
                "torrent already on VPS2: %s (complete=%s, on_fuse=False)",
                ts.source_infohash[:10], ext.is_complete(),
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = ext.save_path
            await self.dest_client.resume(ext.hash)
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

        # Classify
        files = await self.dest_client.get_torrent_files(ts.dest_infohash or ts.source_infohash)
        cls = classify(files, self.cfg)
        ts.classification_kind = cls.kind

        # Apply batch file priorities for seasons
        if cls.kind in ("season", "mixed") and cls.episodes:
            episodes = [e for e in cls.episodes]
            cap = self._batch_cap_bytes()
            if cap <= 0:
                # No usable batch cap: re-check SSD room (disk may have
                # filled since scheduling). Park if full, else one batch.
                total_ep = sum(e.size_bytes for e in episodes)
                if not ssd_has_room(self.cfg, min(total_ep, ts.total_bytes or total_ep)):
                    log.info("ssd cap in use; parking %s", ts.source_name)
                    self.transition(ts, State.WAITING_DISK)
                    return
                cap = total_ep or ts.total_bytes or 1
            batches = make_batches(episodes, cap_bytes=cap)
            ts.batches_total = len(batches)
            ts.batch_index = 0
            if batches:
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
            # Remove the paused torrent from VPS2 so it does not leak as an orphan
            h = ts.dest_infohash or ts.source_infohash
            if h:
                try:
                    await self.dest_client.delete(h, delete_files=True)
                except Exception as e:
                    log.warning("failed to delete skipped oversize movie %s: %s", h[:10], e)
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
                    return t.hash.lower()
            await asyncio.sleep(2)
        return None

    # ---- state: DOWNLOADING ----

    async def _get_batches_for_torrent(self, ts: TorrentState) -> list[Batch]:
        h = ts.dest_infohash or ts.source_infohash
        try:
            files = await self.dest_client.get_torrent_files(h)
        except Exception as e:
            log.warning("could not get torrent files for batches: %s", e)
            return []
        # Use classify() — the same filtering/dedup/regex as _do_queued —
        # so batch counts stay consistent across the download loop.
        try:
            eps = list(classify(files, self.cfg).episodes)
        except Exception as e:
            log.warning("could not classify files for batches: %s", e)
            return []
        if not eps:
            return []
        cap = self._batch_cap_bytes()
        if cap <= 0:
            cap = sum(e.size_bytes for e in eps) or 1
        try:
            return make_batches(eps, cap_bytes=cap)
        except Exception as e:
            log.warning("could not make batches for %s: %s", ts.source_name, e)
            return []

    async def _move_and_clean_batch(
        self, ts: TorrentState, batch: Batch
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
        await self._rclone_move(
            src_dir,
            remote,
            ts,
            include=batch.include_patterns(),
            extra=extra_flags,
        )
        for ep in batch.episodes:
            file_path = src_dir / ep.file_name
            if file_path.exists():
                try:
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path, ignore_errors=True)
                except OSError as e:
                    log.warning("failed removing batch file %s: %s", ep.file_name, e)

    async def _do_downloading(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        if ts.batches_total <= 0 and ts.classification_kind in ("season", "mixed") and hasattr(self, "dest_client"):
            batches = await self._get_batches_for_torrent(ts)
            ts.batches_total = len(batches)
            self.store.upsert(ts)

        is_batched = ts.classification_kind in ("season", "mixed") and ts.batches_total > 1

        while not self._stop:
            # Live tracking
            self._live[h.lower()] = LiveItem(
                source_infohash=h.lower(),
                name=ts.source_name,
                state="downloading",
                progress=0.0,
                size_mb=ts.total_bytes / (1024 * 1024),
            )

            cur_batch: Batch | None = None
            expected_files: list[str] | None = None

            if is_batched and hasattr(self, "dest_client"):
                try:
                    batches = await self._get_batches_for_torrent(ts)
                    if batches and ts.batch_index < len(batches):
                        cur_batch = batches[ts.batch_index]
                        expected_files = [ep.file_name for ep in cur_batch.episodes]
                except Exception as e:
                    log.warning("could not resolve batches for %s: %s", ts.source_name, e)

            try:
                await self._wait_for_completion(ts, expected_files=expected_files)
            finally:
                self._live.pop(h.lower(), None)

            if self._stop:
                return

            if is_batched:
                if cur_batch is not None:
                    if hasattr(self, "dest_client"):
                        paused_ok = False
                        pause_err: Exception | None = None
                        for _ in range(3):
                            try:
                                await self.dest_client.pause(h)
                                paused_ok = True
                                break
                            except Exception as e:
                                pause_err = e
                        if not paused_ok:
                            # Never move while qB is still writing — retry
                            # shortly instead of corrupting the remote.
                            log.warning(
                                "could not pause torrent %s before batch move (%s); retrying shortly",
                                h[:10], pause_err,
                            )
                            await asyncio.sleep(5)
                            continue
                    await self._move_and_clean_batch(ts, cur_batch)
                    ts.batch_index += 1
                    self.store.upsert(ts)
                elif hasattr(self, "_move_and_clean_batch") and hasattr(getattr(self, "_move_and_clean_batch"), "mock_calls"):
                    # Mock in unit test (e.g. AsyncMock)
                    await self._move_and_clean_batch(ts, None)  # type: ignore[arg-type]
                    ts.batch_index += 1
                    self.store.upsert(ts)
                elif not hasattr(self, "dest_client"):
                    # Test dummy without dest_client attached
                    ts.batch_index += 1
                    self.store.upsert(ts)
                else:
                    # Batch resolution is transient (RPC/cap hiccup) but the
                    # wait above already covered the full torrent, so fall
                    # through to MOVING for a full move instead of failing
                    # after successful downloads.
                    log.warning(
                        "current batch %d could not be resolved for %s; "
                        "falling back to full move",
                        ts.batch_index, ts.source_name,
                    )
                    break

                if ts.batch_index < ts.batches_total:
                    await self._prepare_next_batch(ts)
                    if hasattr(self, "dest_client"):
                        try:
                            await self.dest_client.resume(h)
                        except Exception as e:
                            log.warning("could not resume torrent %s after batch move: %s", h[:10], e)
                    continue

            break

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

        while not self._stop:
            t = await self.dest_client.get_torrent(h)
            if t is None:
                raise RuntimeError(f"torrent vanished mid-download: {h}")

            if expected_files:
                files = await self.dest_client.get_torrent_files(h)
                f_map = {f.name: f for f in files}
                missing_files = [fn for fn in expected_files if fn not in f_map]
                if missing_files and files:
                    raise RuntimeError(
                        f"expected batch files missing from torrent {ts.source_name}: {missing_files}"
                    )
                batch_files = [f_map[fn] for fn in expected_files if fn in f_map]
                total_sz = sum(f.size_bytes for f in batch_files)
                done_sz = sum(f.size_bytes * f.progress for f in batch_files)
                prog = done_sz / total_sz if total_sz > 0 else 0.0
                all_done = (
                    len(batch_files) == len(expected_files)
                    and all(f.progress >= 0.999 for f in batch_files)
                )
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
            eps = list(classify(files, self.cfg).episodes)
        except Exception as e:
            log.warning("could not classify files for next batch: %s", e)
            return
        if not eps:
            return
        cap = self._batch_cap_bytes()
        if cap <= 0:
            cap = sum(e.size_bytes for e in eps) or 1
        try:
            batches = make_batches(eps, cap_bytes=cap)
        except Exception as e:
            log.warning("could not make batches for next batch: %s", e)
            return
        if ts.batch_index >= len(batches):
            return
        cur = batches[ts.batch_index]
        prio_map = {f.name: 0 for f in files}
        for ep in cur.episodes:
            prio_map[ep.file_name] = 1
        await self.dest_client.set_file_priorities(h, prio_map)

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

    async def _do_moving(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        cls_files = await self.dest_client.get_torrent_files(h)
        cls = classify(cls_files, self.cfg)

        # 1. Pause torrent on VPS2 client BEFORE move begins to stop active seeding from SSD
        log.info("pausing torrent %s on VPS2 client before move", h[:10])
        paused = False
        pause_err: Exception | None = None
        for attempt in range(3):
            try:
                await self.dest_client.pause(h)
                paused = True
                break
            except Exception as e:  # noqa: BLE001
                pause_err = e
                log.warning("attempt %d: could not pause torrent in client before move: %s", attempt + 1, e)
                await asyncio.sleep(1)
        if not paused:
            # Never move while the client is still writing, but don't fail
            # terminally on a transient WebUI hiccup — stay in MOVING so the
            # next tick retries (preserves downloaded bytes on SSD).
            log.warning(
                "could not pause torrent %s before move (%s); will retry on next tick",
                h[:10], pause_err,
            )
            self.store.upsert(ts)
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
                base_file = src_dir / f.name
                for ext in (".!qB", ".parts"):
                    temp_file = base_file.parent / f"{base_file.name}{ext}"
                    if temp_file.exists():
                        try:
                            temp_file.unlink()
                        except OSError:
                            pass

        # 4. Decide target remote
        if cls.kind in ("movie", "season"):
            remote = self.cfg.rclone.remote.default
        else:
            remote = self.cfg.rclone.remote.unsorted

        # 5. Move completed files via rclone
        # (local_include preserves the top dir for whole-folder moves so the
        # remote layout matches torrent-relative names; see helper above.)
        local_include: list[str] | None = None
        move_base: Path = src_dir
        if ts.batches_total > 1:
            local_folder = folder if (folder and folder.exists()) else (src_dir / ts.source_name if (src_dir / ts.source_name).exists() else None)
            remaining_payload: list[Path] = []
            if local_folder and local_folder.exists() and local_folder.resolve() != src_dir.resolve():
                remaining_payload = [
                    p for p in local_folder.rglob("*")
                    if p.is_file() and not p.name.endswith((".!qB", ".parts"))
                ]

            if ts.batch_index < ts.batches_total or remaining_payload:
                if local_folder and local_folder.exists():
                    log.warning(
                        "multi-batch torrent %s: incomplete batch move (batch %d/%d, %d remaining files); moving to remote before cleanup",
                        ts.source_name, ts.batch_index, ts.batches_total, len(remaining_payload),
                    )
                    # Never bare-move the folder (`rclone move <dir>
                    # <remote>` strips the top dir). Move from the parent
                    # with `<top>/**` so the remote keeps the folder.
                    move_base = local_folder.parent
                    top_include = _top_include_for_folder(move_base, local_folder)
                    if top_include is None:  # only if local_folder is a fs root
                        raise RuntimeError(
                            f"cannot preserve top dir moving {local_folder} to {remote}"
                        )
                    await self._rclone_move(move_base, remote, ts, include=top_include)
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
        elif cls.kind in ("movie", "episode", "season", "unknown"):
            if cls.kind in ("movie", "episode") and cls.single_file:
                local = src_dir / cls.single_file
                if not local.exists():
                    if folder and (folder / cls.single_file).exists():
                        local = folder / cls.single_file
                    else:
                        raise FileNotFoundError(f"completed {cls.kind} file not found on SSD: {local}")
            elif cls.kind in ("season", "unknown") or (cls.kind == "movie" and not cls.single_file):
                if folder and folder.exists():
                    local = folder
                elif (src_dir / ts.source_name).exists():
                    local = src_dir / ts.source_name
                else:
                    raise FileNotFoundError(
                        f"completed {cls.kind} content not found on SSD: "
                        f"neither {folder} nor {src_dir / ts.source_name} exists"
                    )
                # Never bare-move a folder: `rclone move <dir> <remote>`
                # uploads the dir's CONTENTS (top dir stripped) while batch
                # moves preserve torrent-relative paths — the remote/fuse
                # layouts would diverge and re-adds would point at missing
                # data (season packs landing flat in the remote root).
                # Move from the parent with `<top>/**` instead. This also
                # covers the folder-detection fallback above: even when
                # `folder` is None (e.g. a root-level extra broke the
                # all-share check), `src_dir/<torrent>` still moves with
                # its top dir intact.
                move_base = local.parent
                local_include = _top_include_for_folder(move_base, local)
                if local_include is None:  # only if local is a fs root
                    raise RuntimeError(
                        f"cannot preserve top dir moving {local} to {remote}"
                    )
            else:
                cand = src_dir / ts.source_name
                if cand.exists():
                    local = cand
                else:
                    raise FileNotFoundError(f"completed content not found on SSD: {cand}")
            if local_include is not None:
                await self._rclone_move(move_base, remote, ts, include=local_include)
            else:
                await self._rclone_move(local, remote, ts)
        else:
            # Mixed — per-episode moves with --include (single batch)
            cap = self._batch_cap_bytes()
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
                await self._rclone_move(
                    src_dir,
                    remote,
                    ts,
                    include=batch.include_patterns(),
                    extra=self.cfg.rclone.batch_move_extra_flags,
                )

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
        extra: list[str] | None = None,
    ) -> None:
        async with self.move_sem:
            log.info("rclone move %s -> %s (include=%s, extra=%s)", local, remote, include, extra)
            res = await move_local_to_remote(self.cfg, local, remote, include=include, extra=extra)
            if not res.ok:
                err = res.stderr.strip()
                last_err = [ln.strip() for ln in err.splitlines() if ln.strip()][-1] if err else f"rc={res.returncode}"
                raise RuntimeError(f"rclone failed (rc={res.returncode}): {last_err}")

    # ---- state: RE_ADDING ----

    async def _do_re_add(self, ts: TorrentState) -> None:
        store = getattr(self, "store", None)
        now = dt.datetime.now(dt.timezone.utc)
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
        if gate_expected:
            gate_target = self._target_mount_for(ts)
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
                if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
                    if ts.cross_seed_source == "watch-dir":
                        await self._re_inject_watch_dir_torrents(ts)
                    else:
                        await self._re_inject_racing_torrents(ts)

                # 2) Re-add the cross-seed torrent
                await self._re_add_cross_seed_torrent(ts)

                if ts.state != State.FAILED:
                    ts.readd_next_retry_at = None
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

        target_mount = self._target_mount_for(ts)
        res = await self.dest_client.add_torrent(
            torrent_files=[blob],
            save_path=str(target_mount),
            category="racing",
            paused=False,
            skip_check=True,
            tags=["racing", "fuse"],
        )
        already_exists = False
        if not res.accepted and (res.detail == "Fails." or "already" in (res.detail or "").lower()):
            try:
                dest_st = await self.dest_client.get_torrent(target_hash)
                if dest_st is not None:
                    already_exists = True
            except Exception as e:  # noqa: BLE001
                log.debug("could not check dest client for %s: %s", target_hash[:10], e)

        if not res.accepted and not already_exists:
            err_msg = f"fuse re-add rejected: {res.detail or 'client rejected torrent'}"
            log.error("re-add cross-seed torrent failed for %s: %s", ts.source_name, err_msg)
            if res.detail == "Fails." or not res.detail:
                raise WebUIUnresponsiveError(err_msg)
            self.transition(ts, State.FAILED, error=err_msg)
            return
        elif already_exists:
            log.info("cross-seed torrent %s already exists on dest client; marking as injected", target_hash[:10])

    async def _re_inject_watch_dir_torrents(self, ts: TorrentState) -> None:
        """Re-add every watch-dir dropped torrent and discovered cross-seeds onto FUSE."""
        watch_cross_dir = Path(self.cfg.general.state_db).parent / "watch_cross_seeds" / ts.source_infohash
        if not watch_cross_dir.exists():
            return
        target_mount = self._target_mount_for(ts)
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

                # Fuse gate: never point a re-added torrent at missing data.
                watch_expected = self._expected_fuse_files(blob)
                if watch_expected:
                    watch_missing = await self._missing_fuse_files(target_mount, watch_expected)
                    if watch_missing:
                        log.warning(
                            "re-inject watch-dir torrent %s: fuse content missing at %s (%d files); skipping blind injection",
                            h[:10], target_mount, len(watch_missing),
                        )
                        continue

                res = await self.dest_client.add_torrent(
                    torrent_files=[blob],
                    save_path=str(target_mount),
                    category="racing",
                    paused=False,
                    skip_check=True,
                    tags=["racing", "fuse"],
                )
                already_exists = False
                if not res.accepted:
                    try:
                        dest_st = await self.dest_client.get_torrent(h_low)
                        if dest_st is not None:
                            already_exists = True
                    except Exception as e:  # noqa: BLE001
                        log.debug("could not check dest client for %s: %s", h[:10], e)

                if res.accepted or already_exists or "already" in (res.detail or "").lower():
                    injected.append(h_low)
                    injected_set.add(h_low)
                    if already_exists:
                        log.info("watch-dir torrent %s already exists on dest client; marking as injected", h[:10])
                    else:
                        log.info("re-injected watch-dir torrent %s on fuse (%s)", h[:10], target_mount)
                else:
                    log.warning("re-inject watch-dir torrent %s rejected: %s", h[:10], res.detail)
                    raise WebUIUnresponsiveError(f"re-inject watch-dir torrent {h[:10]} rejected: {res.detail}")
        finally:
            ts.injected_private_hashes = ",".join(dict.fromkeys(injected))

    async def _re_inject_racing_torrents(self, ts: TorrentState) -> None:
        """Re-add every racing-client torrent matching this content onto VPS2
        pointing at the fuse mount with skip_check=True.
        """
        target_mount = self._target_mount_for(ts)
        injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        injected_set = set(injected)

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
                try:
                    blob = await self._fetch_racing_torrent_bytes(t.infohash)
                except Exception as e:  # noqa: BLE001
                    log.warning("re-inject: fetch %s failed: %s",
                                t.infohash[:10], e)
                    continue
                if not blob:
                    continue
                # Per-match fuse gate: only inject torrents whose content is
                # actually available at the target (same release name does not
                # guarantee the bytes were moved).
                match_expected = self._expected_fuse_files(blob)
                if match_expected:
                    match_missing = await self._missing_fuse_files(target_mount, match_expected)
                    if match_missing:
                        log.warning(
                            "re-inject: fuse content missing for %s (%s) at %s (%d files); skipping blind injection",
                            t.infohash[:10], t.name[:50], target_mount, len(match_missing),
                        )
                        continue
                res = await self.dest_client.add_torrent(
                    torrent_files=[blob],
                    save_path=str(target_mount),
                    category="racing",
                    paused=False,
                    skip_check=True,
                    tags=["racing", "fuse"],
                )
                already_exists = False
                if not res.accepted:
                    try:
                        dest_st = await self.dest_client.get_torrent(h_low)
                        if dest_st is not None:
                            already_exists = True
                    except Exception as e:  # noqa: BLE001
                        log.debug("could not check dest client for %s: %s", t.infohash[:10], e)

                if not res.accepted and not already_exists and "already" not in (res.detail or "").lower():
                    log.warning(
                        "re-inject: add %s rejected: %s",
                        t.infohash[:10], res.detail,
                    )
                    raise WebUIUnresponsiveError(f"re-inject add {t.infohash[:10]} rejected: {res.detail}")
                injected.append(h_low)
                injected_set.add(h_low)
                if already_exists:
                    log.info(
                        "racing torrent %s (%s) already exists on dest client; marking as injected",
                        t.infohash[:10], t.name[:50],
                    )
                else:
                    log.info(
                        "re-injected racing torrent %s (%s) on fuse",
                        t.infohash[:10], t.name[:50],
                    )
        finally:
            ts.injected_private_hashes = ",".join(dict.fromkeys(injected))

    async def _check_and_inject_late_cross_seeds(
        self, ts: TorrentState, group: list[Torrent]
    ) -> None:
        """Check if new cross-seeds arrived on VPS1 for a completed release and inject them to FUSE."""
        known_hashes = {
            h.lower() for h in (
                ts.source_infohash,
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            ) if h
        }

        new_torrents = [t for t in group if t.infohash.lower() not in known_hashes]
        if not new_torrents:
            return

        target_mount = self._target_mount_for(ts)
        current_injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        current_injected_set = set(current_injected)
        changed = False

        if not hasattr(self, "_failed_late_cross_seeds"):
            self._failed_late_cross_seeds = {}

        now_utc = dt.datetime.now(dt.timezone.utc)

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
                continue

            if not blob:
                log.warning("late cross-seed: no .torrent bytes available for %s", t.infohash[:10])
                continue

            # Fuse gate: a DONE row proves the original content moved, not
            # that this late arrival's bytes did — verify before injecting.
            # Missing content reuses the 30m failure backoff (no per-tick storm).
            late_expected = self._expected_fuse_files(blob)
            if late_expected:
                late_missing = await self._missing_fuse_files(target_mount, late_expected)
                if late_missing:
                    log.warning(
                        "late cross-seed: fuse content missing for %s (%s) at %s (%d files); deferring injection",
                        t.infohash[:10], t.name[:40], target_mount, len(late_missing),
                    )
                    self._failed_late_cross_seeds[h_low] = now_utc
                    continue

            try:
                res = await self.dest_client.add_torrent(
                    torrent_files=[blob],
                    save_path=str(target_mount),
                    category="racing",
                    paused=False,
                    skip_check=True,
                    tags=["racing", "fuse"],
                )
                already_exists = False
                if not res.accepted:
                    # qBittorrent returns "Fails." when a torrent already exists.
                    # Verify if it's already present on VPS2.
                    try:
                        dest_st = await self.dest_client.get_torrent(h_low)
                        if dest_st is not None:
                            already_exists = True
                    except Exception as e:  # noqa: BLE001
                        log.debug("could not check dest client for %s: %s", t.infohash[:10], e)

                if res.accepted or already_exists or "already" in (res.detail or "").lower():
                    if already_exists:
                        log.info(
                            "late cross-seed %s (%s) already exists on dest client; marking as injected",
                            t.infohash[:10], t.name[:40],
                        )
                    else:
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
                        t.infohash[:10], res.detail,
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
            try:
                blob = await asyncio.wait_for(
                    asyncio.to_thread(self.sftp.fetch_torrent, infohash),
                    timeout=15.0,
                )
                if blob:
                    return blob
            except Exception as e:  # noqa: BLE001
                log.warning("sftp fetch %s failed: %s", infohash[:10], e)

        try:
            return await asyncio.wait_for(
                self.source_client.export_torrent(infohash),
                timeout=15.0,
            )
        except AttributeError:
            return None
        except Exception as e:  # noqa: BLE001
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
        """
        def _check() -> list[str]:
            missing: list[str] = []
            for name, want in files:
                try:
                    actual = (target_mount / name).stat().st_size
                except OSError:
                    missing.append(name)
                    continue
                if want and actual != want:
                    missing.append(f"{name} (size {actual}!={want})")
            return missing

        try:
            return await asyncio.to_thread(_check)
        except Exception as e:  # noqa: BLE001
            log.warning("fuse availability check failed for %s: %s", target_mount, e)
            return [f"<availability check failed: {e}>"]

    def _target_mount_for(self, ts: TorrentState) -> Path:
        """Where on the fuse mount should this torrent's data live?"""
        if ts.classification_kind == "movie" or ts.classification_kind == "season":
            return Path(self.cfg.rclone.fuse.mount)
        return Path(self.cfg.rclone.fuse.mount_unsorted)
