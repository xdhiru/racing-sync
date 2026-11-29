"""Coordinator admission (split from coordinator.py).

`AdmitMixin` carries SSD-source picking and admission — `_pick_and_admit`,
`_admit_resolved`, the manual-fuse adoption path, and `_do_new` — moved
verbatim. Test patch-targets (`racing_sync.coordinator.*`) resolve through
the coordinator namespace via deferred imports at the call sites (a
module-level `__getattr__` cannot serve implicit global lookups); everything
else is imported directly.
"""

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path

from .batcher import Batch, make_batches, make_file_batches
from .classifier import oversize_single_file
from .clients.abstract import TorrentFile
from .content_keys import same_content as _same_content_key
from .coordinator_content import (
    WATCH_ELECTION_WAITER_STATES,
    WATCH_ORIGIN_LABELS,
    _created_ts,
    _looks_public,
    _matches_release,
    announce_domain,
    is_watch_row,
    normalize_content_name,
    route_kind_is_sorted,
    watch_election_winner,
    watch_rank,
)
from .coordinator_errors import (
    AbandonedError,
    BatchMoveIncompleteError,
    TorrentCheckingError,
    is_retryable_client_error,
)
from .coordinator_paths import _left_on_disk, _safe_ssd_join, _watch_cross_seed_dir
from .io_bounds import chunked, offload, rpc
from .prowlarr import TorrentHit
from .recovery import _save_path_on_ssd, _ssd_roots_known, find_content_on_ssd
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Blob-parse memo TTL: same infohash = same bytes, so expiry only bounds
# memory across huge intakes, never correctness.
_BLOB_PARSE_TTL_S = 300.0

# Live-content snapshot sharing window: _inflight_same_content callers
# (workers + Telegram notes) share one all_active() scan per window.
_INFLIGHT_SNAPSHOT_S = 30.0

#: States that already own (or are acquiring) the SSD bytes for their content.
_INFLIGHT_OWNER_STATES = frozenset({State.QUEUED, State.DOWNLOADING, State.MOVING, State.RE_ADDING})
#: Pre-admission states that can still race into admission in one pass.
_INFLIGHT_PREADMIT_STATES = frozenset({State.NEW, State.WAITING_INDEXER, State.WAITING_DISK})

#: Consecutive empty file lists after add before the row fails. The client
#: parses metadata in seconds, so an empty list that survives several
#: admission retries is a broken torrent, not a slow one.
_SETUP_EMPTY_FILES_FAIL_TRIES = 5


class AdmitMixin:
    async def _pick_and_admit(
        self,
        ts: TorrentState,
        st,
        others: list,
        *,
        park_reason: str | None = None,
        attempt_prowlarr: bool = True,
    ) -> None:
        """Pick the SSD source, then park (miss) or admit (WAITING_DISK/QUEUED).

        `park_reason=None` derives source-export-miss vs indexer-miss from
        whether the group is public (a public group never queries the
        download-target indexers, so "indexer miss" would mislead).
        A sticky per-row `force_direct` (Telegram /start_ or a previous
        prowlarr-timeout fallback) bypasses Prowlarr for every pick.
        """
        # Grace-hold throttle: a held NEW row re-checks at most once per
        # search interval (not every tick) — the full Prowlarr pick below
        # is the most expensive call in the intake path.
        try:
            _gh = getattr(self, "_grace_hold_until", None)
            _until = _gh.get((ts.source_infohash or "").lower()) if isinstance(_gh, dict) else None
            if _until is not None and time.monotonic() < float(_until):
                return
        except Exception:
            pass
        if getattr(ts, "force_direct", 0):
            attempt_prowlarr = False
        # Patched in tests as racing_sync.coordinator.pick_ssd_source_for_racing:
        # resolve through the coordinator namespace (deferred: implicit
        # global lookups never consult a module __getattr__).
        from . import coordinator as _coordinator

        decision = await _coordinator.pick_ssd_source_for_racing(
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
                try:
                    _skip_title = self.cfg.prowlarr.should_skip_title(st.name) is True
                except Exception:
                    _skip_title = False
                # Skipped titles park on the public (max-age-free) schedule:
                # no query will ever run for them, so the indexer max-age
                # clock must not FAILED them for it.
                park_reason = (
                    "source export miss"
                    if (any(_looks_public(t.trackers) for t in [st, *others]) or _skip_title)
                    else "indexer miss"
                )
            if (
                park_reason == "indexer miss"
                and not getattr(ts, "force_direct", 0)
                and self._prowlarr_timed_out(ts)
            ):
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
                    # The public-export clock resets with it: the row now
                    # runs a direct phase, and a stale public schedule
                    # would misroute its wakeups.
                    ts.force_direct = 1
                    ts.indexer_first_queried_at = dt.datetime.now(dt.UTC)
                    ts.indexer_attempts = 0
                    ts.public_export_first_attempted_at = None
                    ts.public_export_attempts = 0
                    ts.public_export_next_retry_at = None
                    self.store.upsert(ts)
                except Exception:
                    pass
                from . import coordinator as _coordinator

                decision = await _coordinator.pick_ssd_source_for_racing(
                    cfg=self.cfg,
                    source_torrent=st,
                    other_source_torrents=others,
                    prowlarr=self.prowlarr,
                    sftp=self.sftp,
                    source_client=self.source_client,
                    attempt_prowlarr=False,
                )
            if decision is None:
                decision = await self._operator_supplied_decision(ts)
            if decision is None:
                self._park_for_indexer_retry(ts, reason=park_reason)
                return

        # Preferred-copy grace, post-decision (NEW rows only): the Prowlarr
        # query above ran as normal; hold only when it commits to
        # non-preferred direct bytes (a download-indexer copy may still
        # arrive, or a later search may hit). Cross-seed and public
        # decisions always proceed; parks never hold. Nothing is reserved
        # yet, so returning keeps the row NEW with no leakage.
        _grace_exempted = False
        try:
            _ex = getattr(self, "_grace_exempt", None)
            if isinstance(_ex, dict):
                _grace_exempted = bool(_ex.pop((ts.source_infohash or "").lower(), None))
        except Exception:
            pass
        if (
            ts.state == State.NEW
            and not getattr(ts, "force_direct", 0)
            and not _grace_exempted
            and not bool(getattr(decision, "preferred", True))
            and self._in_preferred_grace(ts)
        ):
            _hold = False
            try:
                if getattr(getattr(self.cfg, "prowlarr", None), "download_indexers", None):
                    _hold = True
            except Exception:
                _hold = False
            if _hold:
                _left = self._preferred_grace_left(ts)
                log.info(
                    "holding %s for preferred copy (non-preferred direct, %ds grace left)",
                    ts.source_name[:60],
                    _left,
                )
                # Throttle re-picks to the search cadence (watch-flow
                # parity): re-check when grace ends or the next search is
                # due, whichever comes first — not every tick.
                try:
                    _ri = float(
                        getattr(
                            getattr(self.cfg, "cross_seed", None),
                            "prowlarr_retry_interval_seconds",
                            1800,
                        )
                        or 1800
                    )
                except (TypeError, ValueError):
                    _ri = 1800.0
                try:
                    _holds = getattr(self, "_grace_hold_until", None)
                    if not isinstance(_holds, dict):
                        _holds = {}
                        self._grace_hold_until = _holds  # type: ignore[attr-defined]
                    _holds[(ts.source_infohash or "").lower()] = time.monotonic() + min(
                        max(0.0, float(_left or 0)), max(60.0, _ri)
                    )
                    if len(_holds) > 5000:
                        for _k in list(_holds.keys())[: len(_holds) - 5000]:
                            _holds.pop(_k, None)
                except Exception:
                    pass
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
        # any doubt reserves normally. Shared tail with the watch-dir flow
        # (see _admit_resolved): keep the two in sync via that helper.
        await self._admit_resolved(
            ts,
            blob=decision.torrent_bytes,
            size_bytes=decision.size_bytes,
            display_name=st.name[:60],
        )

    async def _admit_resolved(
        self,
        ts: TorrentState,
        *,
        blob: bytes | None,
        size_bytes: int,
        display_name: str,
    ) -> None:
        """Shared intake admit tail (racing + watch-dir).

        Both flows resolve SSD bytes first (Prowlarr cross-seed / SFTP
        export / sacrificial pick) and then admit identically:
          1. bytes already fully on fuse → QUEUED with no budget (the
             QUEUED setup shortcut carries the row fuse-gated to RE_ADDING);
          2. same hash already complete on the dest client → QUEUED with
             no budget (_do_queued adopts + resumes instead of adding);
          3. else reserve the estimate from the global ledger → QUEUED,
             or park WAITING_DISK when full.
        Fail-open throughout: any doubt reserves normally. Reservations
        are released when the QUEUED transition itself fails, so a
        leaked budget can never strand waiters.
        """
        try:
            _fully_remote = await self._blob_fully_remote(blob, cache_key=ts.source_infohash)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info("content for %s already fully on remote; skipping SSD budget", display_name)
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise
            return
        try:
            _pre = await self._dest_complete_entry(
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)]
            )
        except Exception:
            _pre = None
        if _pre is not None:
            log.info("content for %s already complete on dest; skipping SSD budget", display_name)
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise
            return
        # Global SSD ledger: reserve before QUEUED so concurrent high-size
        # arrivals can't all pass a point-in-time free check and exceed the
        # budget as they grow. Prefer the blob-derived batch footprint
        # (what one batch really occupies) over the total-capped estimate,
        # so a 100 GB season of small episodes admits on one batch of free
        # space instead of demanding the whole cap while unclassified.
        # Fail-open: any doubt keeps today's estimate.
        needed = self._ssd_estimate_for_new(size_bytes)
        try:
            _foot = self._blob_admit_footprint(ts, blob, size_bytes)
        except Exception:
            _foot = None
        if isinstance(_foot, int) and not isinstance(_foot, bool) and _foot > 0:
            if _foot < needed:
                log.info(
                    "admitting %s against ~%d MB batch footprint (total ~%d MB)",
                    display_name,
                    _foot // (1024 * 1024),
                    needed // (1024 * 1024),
                )
            needed = min(needed, _foot)
        if not await self._ssd_try_reserve(ts.source_infohash, needed):
            log.info(
                "ssd budget in use (reserved ~%d MB); parking %s",
                self._ssd_reserved_total() // (1024 * 1024),
                display_name,
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
            if ts.state not in (
                State.NEW,
                State.WAITING_INDEXER,
                State.WAITING_DISK,
                State.RE_ADDING,
            ):
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
                fuse_files = await rpc(
                    self.dest_client.get_torrent_files(ext.hash),
                    getattr(self, "cfg", None),
                    "dest fuse get_torrent_files",
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "manual fuse check: could not list files for %s: %s",
                    ts.source_infohash[:10],
                    e,
                )
                return False
            fuse_expected = [
                (f.name, f.size_bytes) for f in (fuse_files or []) if getattr(f, "name", "")
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
                    ts.source_infohash[:10],
                    getattr(ext, "save_path", "?"),
                    len(fuse_missing),
                    len(fuse_expected),
                    fuse_missing[0],
                )
                return False
            log.info(
                "torrent %s is already completed on VPS2 fuse mount (manual add, category-agnostic); marking DONE",
                ts.source_infohash[:10],
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = getattr(ext, "save_path", "")
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
            try:
                if getattr(self.cfg.cross_seed, "inject_racing_torrents_to_fuse", False):
                    await self._re_inject_racing_torrents(ts)
            except Exception as e:  # noqa: BLE001
                # Manual entry already seeds; a failed re-inject must not block
                # DONE — the late-seed job repairs the racing injection.
                log.warning(
                    "manual fuse check: re-inject for %s failed (keeping DONE): %s",
                    ts.source_infohash[:10],
                    e,
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
                    ts.source_infohash[:10],
                    ts.state.value,
                )
                return False
            return True
        except Exception as e:  # noqa: BLE001
            log.warning(
                "manual fuse check failed for %s: %s",
                getattr(ts, "source_infohash", "?")[:10],
                e,
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
            if ts.state not in (
                State.NEW,
                State.WAITING_INDEXER,
                State.WAITING_DISK,
                State.RE_ADDING,
            ):
                return False
            h = (ts.source_infohash or "").lower()
            if not h:
                return False
            if self._fuse_sweep_covers(ts):
                # The tick sweep already looked this workerless row up
                # (no entry then): skip the redundant per-row RPC. Rows
                # with live workers are never covered — the sweep skips
                # them, so their own check still runs.
                return False
            try:
                existing = await rpc(
                    dest_client.list_torrents(hashes=[h]),
                    getattr(self, "cfg", None),
                    "dest manual fuse lookup",
                )
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

    async def _fuse_content_evidence(
        self, ts: TorrentState, *, blob: bytes | None = None
    ) -> tuple[str, bytes | None, int, int]:
        """Is this row's content already on the fuse target? (pre-pick)

        Answers one of three, costing at most ONE stat for the common
        "not on fuse" case (a normal new torrent):

          "present" — every expected file size-verifies at the target, so
              the row admits straight from its own blob (no Prowlarr
              query, no SSD budget);
          "partial" — at least one expected file exists but not all: an
              operator's manual rclone move is in progress (or the fuse
              index lags), so the caller parks and re-checks on a timer;
          "absent"  — nothing at the target, or nothing verifiable (no
              blob, undecodable, mount outage): keep the existing
              pick -> SSD flow unchanged.

        Cheapness: probe the torrent-name root first (`missing_under`
        with want=0 is presence-only). Only when that root exists are the
        bytes decoded and every expected file verified.

        Returns (status, blob, present_count, expected_count).
        """
        # Mount resolution never stats: a broken/unconfigured fuse section
        # is simply "absent" (the pick below owns that failure loudly).
        try:
            target = self._target_mount_for(ts)
            probe_name = (ts.source_name or "").strip()
            root = _safe_ssd_join(target, probe_name)
        except Exception:
            return "absent", None, 0, 0
        if root is None:
            return "absent", None, 0, 0
        try:
            probe_name = root.relative_to(target).as_posix()
        except Exception:
            return "absent", None, 0, 0
        # A fresh racing row has no classification yet, so `_target_mount_for`
        # answers with the UNSORTED mount. Season/movie roots live under the
        # sorted mount (and the unsorted mount is usually nested inside it),
        # so probe every configured mount and keep the one that has the root.
        # The blob decides the authoritative mount only after it is decoded.
        mounts: list[Path] = [target]
        try:
            _fuse = self.cfg.rclone.fuse
            for _raw in (
                getattr(_fuse, "mount", None),
                getattr(_fuse, "mount_unsorted", None),
            ):
                if not isinstance(_raw, (str, Path)):
                    continue
                _m = Path(str(_raw))
                if _m not in mounts:
                    mounts.append(_m)
        except Exception:
            pass
        hit_mount: Path | None = None
        try:
            from .fuse_gate import missing_under

            for _m in mounts:
                probe = await missing_under(_m, [(probe_name, 0)], fresh=False)
                # None (unstatable) or a marker is not evidence: never start
                # a manual-move wait from an outage.
                if probe is None:
                    continue
                if not probe:
                    hit_mount = _m
                    break
        except Exception:
            hit_mount = None
        if hit_mount is None:
            return "absent", None, 0, 0

        try:
            if not blob:
                blob = ts._blob or ts.cross_seed_blob
            if not blob:
                try:
                    blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                except Exception:
                    blob = None
            if not blob:
                h = (ts.source_infohash or "").lower()
                if h and not self._is_watch_row(ts):
                    # Racing rows may never have persisted bytes: export
                    # them once (the same call the pick would make later)
                    # and cache them for the admit tail.
                    try:
                        blob = await self._fetch_racing_torrent_bytes(h)
                    except Exception:
                        blob = None
                    if blob:
                        try:
                            await asyncio.to_thread(self.store.set_blob, ts.source_infohash, blob)
                        except Exception:
                            pass
            if not blob:
                return "absent", None, 0, 0

            expected = self._expected_fuse_files(blob)
            if not expected:
                return "absent", None, 0, 0
            from .fuse_gate import verify_blob_on_mount

            check = await verify_blob_on_mount(hit_mount, blob, expected=expected, confirm=True)
            if check.verified:
                return "present", bytes(blob), len(expected), len(expected)
            if check.expected is None or check.unverifiable:
                # Mount outage: the existing outage paths park; do not
                # start a manual-move window on unverifiable evidence.
                return "absent", None, 0, 0
            present = len(expected) - len(check.missing)
            if present > 0:
                return "partial", bytes(blob), present, len(expected)
            return "absent", None, 0, 0
        except Exception as e:  # noqa: BLE001
            log.warning(
                "fuse content evidence check failed for %s: %s",
                (ts.source_infohash or "?")[:10],
                e,
            )
            return "absent", None, 0, 0

    async def _admit_fuse_existing(self, ts: TorrentState, blob: bytes) -> None:
        """Admit a row whose bytes are already verified on the fuse target.

        The row seeds from its own torrent blob (no cross-seed lookup);
        `_admit_resolved`'s all-remote shortcut then takes it straight to
        QUEUED with no SSD reservation. That helper owns the transition.
        """
        ts.cross_seed_infohash = (ts.source_infohash or "").lower()
        # Keep a watch-origin label: the re-add flow keys its own
        # (multi-blob) re-injection off it. Racing rows get a label that
        # says where the bytes were found.
        try:
            if (ts.cross_seed_source or "") not in WATCH_ORIGIN_LABELS:
                ts.cross_seed_source = "fuse-existing"
        except Exception:
            ts.cross_seed_source = "fuse-existing"
        ts.cross_seed_blob = bytes(blob)
        ts._blob = bytes(blob)
        ts.fuse_wait_first_at = None
        # Same pre-admission defaults the pick path sets (the row never
        # downloads, but QUEUED logic reads them).
        try:
            ts.save_path = str(self.cfg.dest.save_path)
        except Exception:
            pass
        if int(ts.total_bytes or 0) <= 0:
            try:
                ts.total_bytes = sum(
                    int(_sz or 0) for _n, _sz in (self._expected_fuse_files(blob) or [])
                )
            except Exception:
                pass
        log.info(
            "content for %s already on fuse; admitting without an SSD pick",
            (ts.source_name or "")[:60],
        )
        await self._admit_resolved(
            ts,
            blob=bytes(blob),
            size_bytes=int(ts.total_bytes or 0),
            display_name=(ts.source_name or "")[:60],
        )

    def _fuse_mounts_configured(self) -> bool:
        """True when at least one fuse mount is a real configured path."""
        try:
            fuse = self.cfg.rclone.fuse
        except Exception:
            return False
        for _attr in ("mount", "mount_unsorted"):
            try:
                raw = getattr(fuse, _attr, None)
            except Exception:
                continue
            if isinstance(raw, (str, Path)) and str(raw).strip():
                return True
        return False

    def _manual_move_wait_limits(self) -> tuple[float, float]:
        """(check_seconds, timeout_seconds); timeout 0 disables the wait."""
        try:
            _fuse_cfg = self.cfg.rclone.fuse
        except Exception:
            _fuse_cfg = None
        try:
            timeout_s = float(getattr(_fuse_cfg, "manual_move_timeout_seconds", 43200))
        except (TypeError, ValueError):
            timeout_s = 43200.0
        try:
            check_s = float(getattr(_fuse_cfg, "manual_move_check_seconds", 1800))
        except (TypeError, ValueError):
            check_s = 1800.0
        return check_s, timeout_s

    def _park_fuse_content_wait(
        self, ts: TorrentState, *, present: int = 0, total: int = 0
    ) -> bool:
        """Park awaiting a manual copy and re-check on a timer; False = give up.

        Uses WAITING_INDEXER (the scheduler already wakes those rows) with
        `indexer_next_retry_at` as the wake timer and `fuse_wait_first_at`
        as the persisted window start, so a restart keeps the deadline.
        Returns False when the wait is disabled (timeout 0, or no fuse
        mount configured) or the ceiling is reached. The stamp is kept on
        expiry as a latch: callers that would otherwise park again (an
        oversize row that cannot download at all) must fail instead of
        restarting the window forever.
        """
        if not self._fuse_mounts_configured():
            ts.fuse_wait_first_at = None
            return False
        check_s, timeout_s = self._manual_move_wait_limits()
        if timeout_s <= 0:
            ts.fuse_wait_first_at = None
            return False
        now = dt.datetime.now(dt.UTC)
        start = ts.fuse_wait_first_at
        if start is None:
            start = now
            ts.fuse_wait_first_at = now
        elapsed = (now - start).total_seconds()
        if elapsed >= timeout_s:
            log.info(
                "manual-move wait over for %s (%ds >= %ds); falling back to the SSD pick",
                (ts.source_name or "")[:60],
                int(elapsed),
                int(timeout_s),
            )
            # Keep the stamp: it latches "already gave up" so a caller that
            # cannot download anyway (oversize single file) fails instead
            # of starting a fresh window on the next wake.
            return False
        ts.indexer_next_retry_at = now + dt.timedelta(seconds=max(60.0, check_s))
        ts.public_export_next_retry_at = None
        if total > 0:
            _what = f"{present}/{total} files present"
        else:
            _what = "nothing visible yet"
        reason = (
            f"fuse content not fully there yet ({_what}); waiting for "
            f"the manual move (elapsed {int(elapsed)}s / timeout {int(timeout_s)}s)"
        )
        log.info("%s: %s", (ts.source_name or "")[:60], reason)
        if ts.state != State.WAITING_INDEXER:
            # A fresh row transitions (notifies once); an already-parked
            # row must not re-transition (illegal), so the narrow write
            # below carries the new timer instead.
            self.transition(ts, State.WAITING_INDEXER)
        self._park_row(
            ts,
            reason,
            level=None,
            extra={
                "indexer_next_retry_at": ts.indexer_next_retry_at.isoformat(),
                "public_export_next_retry_at": "",
                "fuse_wait_first_at": ts.fuse_wait_first_at.isoformat(),
            },
        )
        try:
            self._schedule_telegram_update(ts)
        except Exception:
            pass
        return True

    async def _fuse_content_precheck(self, ts: TorrentState, *, blob: bytes | None = None) -> bool:
        """Pre-pick manual-move handling; True when the caller must stop.

        Shared by `_do_new`, `_do_waiting_indexer` and `_do_new_watch_dir`
        so a wake wave re-checks fuse before querying indexers. With the
        wait disabled (timeout 0) this is a no-op: no probe, no stats.
        """
        _check_s, _timeout_s = self._manual_move_wait_limits()
        if _timeout_s <= 0:
            return False
        try:
            status, found, present, total = await self._fuse_content_evidence(ts, blob=blob)
        except Exception:
            status, found, present, total = "absent", None, 0, 0
        if status == "present" and found:
            await self._admit_fuse_existing(ts, found)
            return True
        if status == "partial":
            if self._park_fuse_content_wait(ts, present=present, total=total):
                return True
            # Ceiling reached/disabled: fall through to the normal flow.
            return False
        # Absent: a wait already running (a copy seen earlier, or an
        # oversize row parked on this path) keeps going until its ceiling
        # instead of dropping straight back to the pick.
        if ts.fuse_wait_first_at is not None:
            if self._park_fuse_content_wait(ts, present=0, total=0):
                return True
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
                pre_ssd = await offload(
                    store.list_by_state,
                    State.NEW,
                    State.WAITING_INDEXER,
                    State.WAITING_DISK,
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
                    if (
                        hasattr(store, "is_ignored")
                        and store.is_ignored(ts.source_infohash) is True
                    ):
                        continue
                except Exception:
                    pass
                seen.add(h)
                hashes.append(h)
                candidates.append(ts)
            if not hashes:
                self._note_fuse_sweep_covered()
                return
            try:
                # Chunked + RPC-bounded: >~100 hashes risks URL limits
                # downstream, and one hung lookup must not stall the tick.
                _sweep_parts = await asyncio.gather(
                    *(
                        rpc(
                            dest_client.list_torrents(hashes=_c),
                            getattr(self, "cfg", None),
                            "dest fuse sweep list",
                        )
                        for _c in chunked(hashes)
                    )
                )
                existing = [t for _part in _sweep_parts for t in (_part or [])]
            except Exception as e:  # noqa: BLE001
                log.warning("manual fuse sweep: dest lookup failed: %s", e)
                return
            if not existing:
                self._note_fuse_sweep_covered()
                return
            by_hash = {}
            for t in existing or []:
                try:
                    by_hash[(t.hash or "").lower()] = t
                except Exception:
                    continue
            # Verify candidates concurrently (4-wide): each verify is
            # files + fuse stats + maybe re-inject, and sequential
            # verification holds the poller tick for minutes at 50 rows.
            # Per-candidate isolation is preserved (fresh-state re-read
            # inside, broad catch outside).
            _verify_sem = asyncio.Semaphore(4)

            async def _verify_one(_ts):
                async with _verify_sem:
                    try:
                        h = (_ts.source_infohash or "").lower()
                        ext = by_hash.get(h)
                        if ext is None:
                            return
                        # Re-read: a worker may have moved this row after the
                        # snapshot above — never overwrite a fresh state.
                        try:
                            fresh = await asyncio.to_thread(
                                store.get, _ts.source_infohash, include_blob=False
                            )
                        except Exception:
                            fresh = None
                        if fresh is None or fresh.state != _ts.state:
                            return
                        if fresh.state not in (
                            State.NEW,
                            State.WAITING_INDEXER,
                            State.WAITING_DISK,
                        ):
                            return
                        if getattr(fresh, "skipped", 0):
                            return
                        await self._verify_and_adopt_manual_fuse(fresh, ext)
                    except Exception:
                        return

            await asyncio.gather(*(_verify_one(ts) for ts in candidates))
            self._note_fuse_sweep_covered()
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse sweep failed: %s", e)

    def _note_grace_search(self, key: str) -> None:
        """Stamp a grace-scope search so repeats wait out the interval."""
        try:
            _ls = getattr(self, "_last_grace_search", None)
            if not isinstance(_ls, dict):
                _ls = {}
                self._last_grace_search = _ls  # type: ignore[attr-defined]
            _ls[key] = time.monotonic()
            if len(_ls) > 5000:
                for _k in list(_ls.keys())[: len(_ls) - 5000]:
                    _ls.pop(_k, None)
        except Exception:
            pass

    def _note_fuse_sweep_covered(self) -> None:
        """Stamp a completed fuse sweep: per-row adopt checks covered by
        it may skip their own dest lookup until the next sweep."""
        try:
            self._last_fuse_sweep_mono = time.monotonic()
        except Exception:
            pass

    def _fuse_sweep_covers(self, ts: TorrentState) -> bool:
        """True when the last sweep already checked this row this tick.

        Only workerless rows are covered: the sweep skips hashes with
        live workers, so a row running its own _do_* check must still
        look itself up.
        """
        try:
            _at = float(getattr(self, "_last_fuse_sweep_mono", 0.0) or 0.0)
            if time.monotonic() - _at <= 0:
                return False
            try:
                _interval = float(
                    getattr(
                        getattr(getattr(self, "cfg", None), "general", None),
                        "source_poll_interval",
                        30,
                    )
                    or 30
                )
            except (TypeError, ValueError):
                _interval = 30.0
            if time.monotonic() - _at > max(30.0, _interval):
                return False
            _running = getattr(self, "_running_infohashes", None)
            if isinstance(_running, set) and ((ts.source_infohash or "").lower() in _running):
                return False
            return True
        except Exception:
            return False

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
        # Sentinel lives in coordinator (shared with remaining core):
        # resolve through the namespace to preserve identity.
        from . import coordinator as _coordinator

        if st is _coordinator._SOURCE_GONE:
            # VPS1 copy gone (space-cleared, pruned): proceed from the
            # row's persisted metadata. Siblings still on VPS1 (if any)
            # are preferred automatically by the normal pick; otherwise
            # a Prowlarr cross-seed supplies the SSD bytes.
            st = self._pseudo_source_for_vanished(ts)
            if st is None:
                return
        else:
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

        # Pre-pick manual-move wait: content the operator already pushed to
        # the remote needs no Prowlarr query or SSD budget. A partial copy
        # parks here instead of downloading behind an indexer wait.
        if await self._fuse_content_precheck(ts):
            return

        # Admit-time stop-duplicate (P8): a same-content sibling already
        # past admission owns the SSD bytes — defer to it (recorded, so
        # later windows skip the scan while it is live) instead of a
        # second Prowlarr search + duplicate download. The WAITING_INDEXER
        # retry clock still bounds the wait: owner death hands back
        # control, max-age fails eventually. Fail-open: any doubt picks.
        try:
            _new_leader = self._inflight_same_content(ts)
        except Exception:
            _new_leader = None
        if _new_leader is not None:
            try:
                self._record_deferred_on(
                    (ts.source_infohash or "").lower(), _new_leader.source_infohash or ""
                )
            except Exception:
                pass
            log.info(
                "deferring %s to in-flight same-content %s (%s)",
                (ts.source_name or "")[:60],
                (_new_leader.source_infohash or "")[:10],
                _new_leader.state.value,
            )
            self._park_for_indexer_retry(ts, reason="same-content deferred")
            return

        all_source = await self._list_source_torrents()
        await self._pick_and_admit(ts, st, self._same_content_torrents(all_source, st))

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
            max_age = dt.timedelta(seconds=self.cfg.cross_seed.prowlarr_max_age_seconds)
            return (dt.datetime.now(dt.UTC) - first) >= max_age
        except Exception:
            return False

    def _schedule_retry(
        self,
        ts: TorrentState,
        kind: str,
        delay_s: float,
        *,
        clear_other: bool = True,
    ) -> dt.datetime:
        """Set one retry timer and return the instant it fires.

        Two schedules share the WAITING_INDEXER state: "indexer" (a
        cross-seed search) and "public" (the source-export fallback).
        Writing one clears the other by default — a leftover timer on the
        inactive schedule sits in the past and makes list_waiting_ready
        wake the row every tick. The delay is clamped to >=60s so a
        corrupt config can never spin the scheduler.
        """
        try:
            delay = max(60.0, float(delay_s))
        except (TypeError, ValueError):
            delay = 1800.0
        retry_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=delay)
        if str(kind) == "public":
            ts.public_export_next_retry_at = retry_at
            if clear_other:
                ts.indexer_next_retry_at = None
        else:
            ts.indexer_next_retry_at = retry_at
            if clear_other:
                ts.public_export_next_retry_at = None
        return retry_at

    def _park_waiting_indexer(self, ts: TorrentState, *, reason: str) -> None:
        """Persist a WAITING_INDEXER park after a timer was set.

        A fresh row transitions (notifying Telegram once); an already-
        parked row takes a whole-row upsert — a transition to the state it
        already holds is illegal, and re-notifying for a timer refresh
        would spam the channel. Forgotten rows raise AbandonedError so a
        stale snapshot never upsert-resurrects them.
        """
        if ts.state != State.WAITING_INDEXER:
            self.transition(ts, State.WAITING_INDEXER)
            return
        if self._abandoned(ts):
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                f"not refreshing {reason} park"
            )
        self.store.upsert(ts)
        self._schedule_telegram_update(ts)

    def _park_for_indexer_retry(self, ts: TorrentState, *, reason: str = "indexer miss") -> None:
        """Park into WAITING_INDEXER with an escalating retry timer.

        The first attempt: retry after prowlarr_retry_interval_seconds.
        Subsequent attempts: same interval (fixed, not exponential — we
        expect the download-target indexers to catch up shortly for racing
        releases).
        Hard cap: prowlarr_max_age_seconds since the FIRST attempt. If
        that ceiling is reached, mark FAILED for manual handling.

        A "source export miss" (public racing torrent whose .torrent
        can't be exported right now) never queries the download-target
        indexers, so it parks on the separate public-export schedule
        with no max-age clock instead — see _park_for_public_retry.
        """
        if reason == "source export miss":
            self._park_for_public_retry(ts)
            return
        now = dt.datetime.now(dt.UTC)
        if ts.indexer_first_queried_at is None:
            ts.indexer_first_queried_at = now
        ts.indexer_attempts += 1
        try:
            _retry_gap = float(
                getattr(
                    getattr(self.cfg, "cross_seed", None), "prowlarr_retry_interval_seconds", 1800
                )
                or 1800
            )
        except (TypeError, ValueError):
            _retry_gap = 1800.0
        try:
            _max_age_s = float(
                getattr(getattr(self.cfg, "cross_seed", None), "prowlarr_max_age_seconds", 86400)
                or 86400
            )
        except (TypeError, ValueError):
            _max_age_s = 86400.0
        next_retry = self._schedule_retry(ts, "indexer", _retry_gap)
        max_age = dt.timedelta(seconds=max(60.0, _max_age_s))
        elapsed = now - ts.indexer_first_queried_at

        log.info(
            "%s #%d for %s; next retry at %s (elapsed=%ds, max=%ds)",
            reason,
            ts.indexer_attempts,
            ts.source_name,
            next_retry.isoformat(timespec="seconds"),
            int(elapsed.total_seconds()),
            int(max_age.total_seconds()),
        )

        if elapsed >= max_age:
            if reason == "source vanished from VPS1":
                # Disappearance starts a new cross-seed situation: the
                # earlier window may have leaned on the VPS1 copy, so an
                # exhausted clock grants one fresh window for the vanished
                # copy instead of failing instantly. Attempts keep
                # counting, so this stays bounded.
                log.warning(
                    "source vanished after an exhausted indexer window; "
                    "granting one fresh cross-seed window for %s",
                    ts.source_name[:60],
                )
                ts.indexer_first_queried_at = now
                elapsed = dt.timedelta(0)
            else:
                log.error(
                    "download-target indexers giving up on %s after %d attempts (%ds > %ds max)",
                    ts.source_name,
                    ts.indexer_attempts,
                    int(elapsed.total_seconds()),
                    int(max_age.total_seconds()),
                )
                # From WAITING_INDEXER → FAILED is legal (see ALLOWED).
                self.transition(
                    ts,
                    State.FAILED,
                    error=(f"Prowlarr cross-seed not found within {int(max_age.total_seconds())}s"),
                )
                return

        # If we're being called from _do_new (state is NEW), the park
        # transitions. If we're being re-called from _do_waiting_indexer,
        # the state is already WAITING_INDEXER and only the timer changed.
        self._park_waiting_indexer(ts, reason="indexer")

    def _park_for_public_retry(self, ts: TorrentState) -> None:
        """Park a public-export miss on its own indefinite schedule.

        Same WAITING_INDEXER state (the Telegram row keeps offering
        Fetch), but the retry runs on public_export_retry_interval_seconds
        with no max-age FAILED: a transient VPS1 export glitch heals on a
        later window, and the source torrent's continued presence is the
        liveness proof. A vanished source still fails via the consecutive
        source-miss guard in _fetch_source_or_fail, so a dead torrent
        cannot spin here forever. Attempts are counted for observability.
        """
        try:
            interval = max(60.0, float(self.cfg.cross_seed.public_export_retry_interval_seconds))
        except (TypeError, ValueError, AttributeError):
            interval = 1800.0
        now = dt.datetime.now(dt.UTC)
        if ts.public_export_first_attempted_at is None:
            ts.public_export_first_attempted_at = now
        ts.public_export_attempts += 1
        # The row runs the public schedule now: a leftover indexer timer
        # (e.g. from a misclassified retry of a public row) would make
        # list_waiting_ready wake this row every tick, inflating attempts
        # until the 24h max-age FAILED a public row.
        self._schedule_retry(ts, "public", interval)
        # Unbounded schedule by design (no max-age FAILED), so rotate
        # verbosity: first misses log loudly, the long tail at debug —
        # a wedged export must not fill the log disk with its own lines.
        _n = ts.public_export_attempts
        if _n <= 3 or _n % 50 == 0:
            log.info(
                "source export miss #%d for %s; next retry at %s",
                _n,
                ts.source_name,
                ts.public_export_next_retry_at.isoformat(timespec="seconds"),
            )
        else:
            log.debug(
                "source export miss #%d for %s; next retry at %s",
                _n,
                ts.source_name,
                ts.public_export_next_retry_at.isoformat(timespec="seconds"),
            )
        self._park_waiting_indexer(ts, reason="public-export")

    def _inflight_same_content(self, ts: TorrentState):
        """Row that owns (or is winning) this content, else None.

        Owners (QUEUED/DOWNLOADING/MOVING/RE_ADDING) hold the SSD bytes in
        any origin (watch or racing): a fresh Prowlarr search here would
        only duplicate them. DONE is excluded on purpose — a finished
        sibling no longer holds SSD work, and the normal pick (with its
        all-remote shortcut) is the right path then.

        With no owner yet, pre-admission rows are elected deterministically
        (oldest row, then hash): two NEW rows with the same content used to
        both admit in one pass, because nothing QUEUED existed to stop
        either. The losers defer and retry on the normal indexer schedule.
        Watch rows are skipped here — their election ranks by tracker
        (public first) and an age-based pick would override it.

        Fail-open None: any doubt runs the normal pick.

        Stays synchronous: the Telegram refresh loop calls this (via
        _watch_wait_note) inline, and it is one indexed SELECT — the
        heavy list scans live on offloaded paths instead.

        The active-list snapshot is shared for _INFLIGHT_SNAPSHOT_S: N
        invocations per tick (workers + Telegram notes) share one scan
        instead of each paying O(rows).
        """
        try:
            me = (ts.source_infohash or "").lower()
            rows = self._inflight_snapshot_rows()
        except Exception:
            return None
        owners: list = []
        waiters: list = []
        try:
            for r in rows or []:
                try:
                    if (r.source_infohash or "").lower() == me:
                        continue
                    if r.state in _INFLIGHT_OWNER_STATES:
                        bucket = owners
                    elif r.state in _INFLIGHT_PREADMIT_STATES:
                        bucket = waiters
                    else:
                        continue
                    # Single content-key rule (see content_keys.same_content):
                    # same normalized name + same size when both known.
                    # Unknown sizes match (fail toward dedup, like the watch
                    # election) to avoid duplicate SSD downloads — except
                    # both-unknown, which would mutually defer forever.
                    if not _same_content_key(
                        r.source_name or "",
                        r.total_bytes or 0,
                        ts.source_name or "",
                        ts.total_bytes or 0,
                        require_size=True,
                        allow_unknown=True,
                    ):
                        continue
                    bucket.append(r)
                except Exception:
                    continue
        except Exception:
            pass
        if owners:
            return owners[0]
        if not waiters:
            return None
        try:
            if self._is_watch_row(ts):
                return None
            waiters = [r for r in waiters if not self._is_watch_row(r)]
        except Exception:
            return None
        if not waiters:
            return None

        def _election_key(row) -> tuple:
            return (_created_ts(row), (row.source_infohash or "").lower())

        try:
            winner = min([*waiters, ts], key=_election_key)
        except Exception:
            return None
        return None if winner is ts else winner

    def _inflight_snapshot_rows(self):
        """all_active() snapshot shared for _INFLIGHT_SNAPSHOT_S seconds."""
        try:
            _memo = getattr(self, "_inflight_snapshot", None)
            if (
                isinstance(_memo, tuple)
                and len(_memo) == 2
                and time.monotonic() - float(_memo[0]) < _INFLIGHT_SNAPSHOT_S
            ):
                return _memo[1]
        except Exception:
            _memo = None
        rows = self.store.all_active()
        try:
            self._inflight_snapshot = (time.monotonic(), rows)
        except Exception:
            pass
        return rows

    def _record_deferred_on(self, waiter_key: str, owner_hash: str) -> None:
        """Record a same-content deferral (waiter -> live owner).

        In-memory (`_supplement_ok_at`-style map, capped): ticks skip
        Prowlarr for the waiter till the owner leaves. Entries clear on
        use (owner gone) — no transition hook needed.
        """
        try:
            _m = getattr(self, "_deferred_on", None)
            if not isinstance(_m, dict):
                _m = {}
                self._deferred_on = _m  # type: ignore[attr-defined]
            _o = (owner_hash or "").strip().lower()
            if not _o:
                return
            _m[(waiter_key or "").strip().lower()] = _o
            if len(_m) > 5000:
                for _k in list(_m.keys())[: len(_m) - 5000]:
                    if _k != (waiter_key or "").strip().lower():
                        _m.pop(_k, None)
        except Exception:
            pass

    def _deferred_owner_of(self, waiter_key: str) -> str | None:
        """Recorded deferral owner for a waiter, if any. Never raises."""
        try:
            _m = getattr(self, "_deferred_on", None)
            if not isinstance(_m, dict):
                return None
            _o = _m.get((waiter_key or "").strip().lower())
            return _o or None
        except Exception:
            return None

    def _clear_deferred_on(self, waiter_key: str) -> None:
        """Drop a deferral record. Never raises."""
        try:
            _m = getattr(self, "_deferred_on", None)
            if isinstance(_m, dict):
                _m.pop((waiter_key or "").strip().lower(), None)
        except Exception:
            pass

    def _deferred_owner_live(self, owner_hash: str) -> bool:
        """True when the recorded owner is still in-flight (shared scan).

        DONE/FAILED/forgotten owners read gone — the waiter then falls
        through to its own scan + pick. Never raises (doubt = not live
        would strand; doubt here returns False only on real absence —
        errors return False and the waiter re-scans, today's behavior).
        """
        try:
            _o = (owner_hash or "").strip().lower()
            if not _o:
                return False
            for r in self._inflight_snapshot_rows() or []:
                try:
                    if (r.source_infohash or "").lower() == _o and r.state in (
                        State.QUEUED,
                        State.DOWNLOADING,
                        State.MOVING,
                        State.RE_ADDING,
                    ):
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _repark_deferred(self, ts: TorrentState) -> None:
        """Push the deferral retry timer quietly (no Prowlarr this window).

        Same schedule the row is parked on (public-export rows keep the
        public timer, everything else the indexer timer), attempts and
        max-age untouched so a dead owner hands back control. Silent
        when the row is gone (no upsert-resurrect).
        """
        try:
            _interval = max(60.0, float(self.cfg.cross_seed.prowlarr_retry_interval_seconds))
        except (TypeError, ValueError):
            _interval = 1800.0
        try:
            _public_interval = max(
                60.0, float(self.cfg.cross_seed.public_export_retry_interval_seconds)
            )
        except (TypeError, ValueError, AttributeError):
            _public_interval = 1800.0
        try:
            if self._abandoned(ts):
                return
            _public = bool(ts.public_export_next_retry_at and not ts.indexer_next_retry_at)
            # Refresh only the schedule the row is already parked on.
            self._schedule_retry(
                ts,
                "public" if _public else "indexer",
                _public_interval if _public else _interval,
                clear_other=False,
            )
            self.store.upsert(ts)
        except Exception:
            pass

    async def _adopt_ssd_entry(self, ts: TorrentState, entry) -> bool:
        """Adopt a complete SSD-resident dest entry; True when queued.

        The bytes are already on disk, so no SSD budget is reserved —
        the row rides QUEUED into the adopt+resume path (`_do_queued`
        adopts the live entry instead of adding a second one). Needs
        the .torrent bytes for `_do_queued`'s setup: memory, then the
        DB column, then a best-effort export of the live dest entry
        itself. Anything missing/unverifiable returns False and the
        caller runs the normal pick (fail-open, never FAILED here).
        """
        try:
            want = (ts.source_infohash or "").strip().lower()
            if not want:
                return False
            blob = getattr(ts, "_blob", None) or getattr(ts, "cross_seed_blob", None)
            if not blob and getattr(self, "store", None) is not None:
                try:
                    blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                except Exception:
                    blob = None
            if not blob:
                try:
                    dest = getattr(self, "dest_client", None)
                    _export = getattr(dest, "export_torrent", None) if dest is not None else None
                    if callable(_export):
                        _h = (getattr(entry, "hash", "") or "").lower() or want
                        blob = await asyncio.wait_for(_export(_h), timeout=15.0)
                except Exception:
                    blob = None
            if not blob:
                log.info(
                    "ssd entry complete for %s but no .torrent bytes available "
                    "(supply them via /add); falling back to normal pick",
                    (ts.source_name or "")[:60],
                )
                return False
            try:
                from .watchdir import _bencoded_info_hash

                real_hash, _, _, _ = _bencoded_info_hash(bytes(blob))
            except Exception:
                return False
            if (real_hash or "").lower() != want:
                return False
            try:
                if getattr(self, "store", None) is not None:
                    await asyncio.to_thread(self.store.set_blob, ts.source_infohash, bytes(blob))
            except Exception:
                pass
            ts.cross_seed_blob = bytes(blob)
            ts._blob = bytes(blob)
            ts.cross_seed_infohash = want
            ts.cross_seed_source = "ssd-adopted"
            log.info(
                "adopting SSD-resident %s (already complete on VPS2 client); skipping SSD budget",
                (ts.source_name or "")[:60],
            )
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                return False
            return True
        except Exception:
            return False

    async def _do_waiting_indexer(self, ts: TorrentState) -> None:
        """Wake up from WAITING_INDEXER and re-pick the SSD source.

        Called by _tick when the row's indexer_next_retry_at has elapsed.
        """
        # Manual fuse fast-track before another prowlarr query: a torrent
        # added by hand to VPS2 while parked must not wait out the retry.
        try:
            if await self._adopt_manual_fuse_if_present(ts):
                return
        except Exception:
            pass

        # Pre-pick manual-move wait, re-run on every wake: a manual move
        # that finished since the last retry admits here, and a still
        # partial one re-arms its timer without touching the indexers.
        if await self._fuse_content_precheck(ts):
            return

        # SSD shortcut: the same hash already lives on the VPS2 client
        # (added by hand and seeding/downloading from SSD) — adopt it
        # instead of querying Prowlarr for a cross-seed. A complete
        # entry rides QUEUED into the adopt+resume path (no SSD budget
        # is reserved for bytes already on disk; _do_queued adopts the
        # live entry instead of adding a second one). A still-
        # downloading entry quiet-parks this window — no indexer spam —
        # and gets adopted on completion by a later window. Fail-open:
        # any doubt runs the normal pick below.
        try:
            _entries = await self._dest_entries_for(
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)]
            )
        except Exception:
            _entries = []
        _ssd_complete = None
        _ssd_any = None
        for _e in _entries or []:
            if _ssd_any is None:
                _ssd_any = _e
            try:
                if bool(_e.is_complete()):
                    _ssd_complete = _e
                    break
            except Exception:
                continue
        if _ssd_complete is not None:
            if await self._adopt_ssd_entry(ts, _ssd_complete):
                return
        elif _ssd_any is not None:
            try:
                _interval = max(60.0, float(self.cfg.cross_seed.prowlarr_retry_interval_seconds))
            except (TypeError, ValueError):
                _interval = 1800.0
            log.info(
                "dest entry %s still downloading on SSD client; waiting "
                "for it instead of querying Prowlarr",
                (ts.source_infohash or "")[:10],
            )
            try:
                if self._abandoned(ts):
                    raise AbandonedError(
                        f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}"
                    )
                # Push the schedule this row is actually parked on (mirror
                # the same-content deferral below): a public-export row
                # must not gain an indexer timer — it would wake on the
                # wrong schedule.
                if ts.public_export_next_retry_at and not ts.indexer_next_retry_at:
                    try:
                        _public_interval = max(
                            60.0, float(self.cfg.cross_seed.public_export_retry_interval_seconds)
                        )
                    except (TypeError, ValueError, AttributeError):
                        _public_interval = 1800.0
                    self._schedule_retry(ts, "public", _public_interval, clear_other=False)
                else:
                    self._schedule_retry(ts, "indexer", _interval, clear_other=False)
                self.store.upsert(ts)
            except Exception:
                pass
            return

        # Watch rows (manual drops) carry their own bytes: the racing
        # pick below (VPS1 export / Prowlarr cross-seed) is meaningless
        # for them — and its VPS1 lookup would fail a row whose torrent
        # was never on VPS1. The watch flow proceeds from the dropped
        # .torrent instead (honoring force_direct).
        try:
            _is_watch = self._is_watch_row(ts)
        except Exception:
            _is_watch = False
        if _is_watch:
            await self._do_new_watch_dir(ts)
            return

        # Admit-time record fast path (P8): a recorded owner still live
        # skips the VPS1 fetch + scan below — no Prowlarr till it leaves.
        # Owner gone: clear the record and fall through to the scan.
        try:
            _wkey = (ts.source_infohash or "").lower()
            _rec = self._deferred_owner_of(_wkey)
        except Exception:
            _wkey, _rec = "", None
        if _rec:
            try:
                _alive = self._deferred_owner_live(_rec)
            except Exception:
                _alive = False
            if _alive:
                log.info(
                    "still deferring %s to live same-content %s",
                    (ts.source_name or "")[:60],
                    _rec[:10],
                )
                self._repark_deferred(ts)
                return
            try:
                self._clear_deferred_on(_wkey)
            except Exception:
                pass

        # Pull fresh data from VPS1 in case the torrent name changed.
        # A vanished VPS1 copy (space-cleared, pruned) proceeds from the
        # persisted metadata instead of failing: Prowlarr or a surviving
        # same-content sibling can still supply the SSD bytes.
        st = await self._fetch_source_or_fail(ts)
        if st is None:
            return
        from . import coordinator as _coordinator

        if st is _coordinator._SOURCE_GONE:
            st = self._pseudo_source_for_vanished(ts)
            if st is None:
                return
        else:
            ts.source_name = st.name
            ts.total_bytes = st.size_bytes

        # Same-content in-flight deferral: another row for this release is
        # already QUEUED/DOWNLOADING/MOVING/RE_ADDING (e.g. a watch-dropped
        # copy racing the Prowlarr search) — skip this window's search and
        # stay parked instead of polling Prowlarr for duplicate bytes. The
        # existing retry timer is pushed one quiet interval (attempts and
        # the max-age clock untouched), so a stalled/failed sibling hands
        # back control on a later window. Fail-open: any doubt picks.
        try:
            _leader = self._inflight_same_content(ts)
        except Exception:
            _leader = None
        if _leader is not None:
            log.info(
                "deferring Prowlarr retry for %s — same content %s already %s",
                ts.source_name[:60],
                (_leader.source_infohash or "")[:10],
                _leader.state.value,
            )
            try:
                self._record_deferred_on(
                    (ts.source_infohash or "").lower(), _leader.source_infohash or ""
                )
            except Exception:
                pass
            self._repark_deferred(ts)
            return

        all_source = await self._list_source_torrents()
        await self._pick_and_admit(
            ts,
            st,
            self._same_content_torrents(all_source, st),
            park_reason=None,
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

        Memoized per (infohash, batch_index, batches_total) for 60s: the
        estimate only moves with the cursor, and a stale-high value fails
        safe (over-reserve → park, never over-admit).
        """
        try:
            _ekey = (
                (ts.source_infohash or "").lower(),
                int(getattr(ts, "batch_index", 0) or 0),
                int(getattr(ts, "batches_total", 0) or 0),
            )
        except (TypeError, ValueError, AttributeError):
            _ekey = None
        _ecache = None
        if _ekey is not None:
            try:
                _ecache = getattr(self, "_estimate_cache", None)
                if not isinstance(_ecache, dict):
                    _ecache = {}
                    self._estimate_cache = _ecache  # type: ignore[attr-defined]
                _hit = _ecache.get(_ekey)
                if (
                    isinstance(_hit, tuple)
                    and len(_hit) == 2
                    and time.monotonic() - float(_hit[0]) < 60.0
                ):
                    return _hit[1]
            except Exception:
                _ecache = None
        _res = await self._estimate_remaining_bytes_uncached(ts)
        if _ekey is not None and _ecache is not None and _res is not None:
            try:
                _ecache[_ekey] = (time.monotonic(), _res)
                if len(_ecache) > 500:
                    for _old in list(_ecache.keys())[: len(_ecache) - 500]:
                        _ecache.pop(_old, None)
            except Exception:
                pass
        return _res

    async def _estimate_remaining_bytes_uncached(self, ts: TorrentState) -> int | None:
        """Uncached estimate body (see _estimate_remaining_bytes)."""
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
                files = await rpc(
                    self.dest_client.get_torrent_files(h),
                    getattr(self, "cfg", None),
                    "dest remaining estimate",
                )
            except Exception:
                return None
            if not files:
                return None
            # Patched in tests as racing_sync.coordinator.classify.
            from . import coordinator as _coordinator

            cls = _coordinator.classify(files, self.cfg)
            # Pinned kind wins over a fresh re-derive: a mid-flight sidecar
            # must not flip season->mixed under the routed batches.
            _pinned = ts.classification_kind or ""
            kind = (_pinned if _pinned not in ("", "unknown") else "") or cls.kind or "unknown"
            cap = self._frozen_batch_cap(ts)
            if cap <= 0:
                return None
            batches = self._resolve_batches(files, kind, cap, episodes=cls.episodes)
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

    # Lane queues (P4): lane -> {infohash.lower(): enqueue_monotonic}.
    # Workers that lose the lane race event-park here (sem released, no
    # DB churn) instead of exiting + respawning + re-running intake every
    # tick; start_from_queued() wakes them by priority when slots free.
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
                str(self.cfg.rclone.fuse.mount).rstrip("/\\").replace("\\", "/"),
                str(self.cfg.rclone.fuse.mount_unsorted).rstrip("/\\").replace("\\", "/"),
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

    def _watch_rank(self, ts: TorrentState) -> int:
        """SSD-download priority for a watch-dir row: public (0) first."""
        return watch_rank(ts, self.cfg)

    def _preferred_grace_seconds(self) -> int:
        """Preferred-copy grace window, or 0 when disabled/unconfigured.

        Non-numeric doubles (MagicMock cfg in unit tests) count as 0 —
        grace is opt-in runtime policy, never test-double behavior.
        """
        try:
            raw = getattr(getattr(self.cfg, "general", None), "preferred_copy_grace_seconds", 0)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return 0
            return max(0, int(raw or 0))
        except Exception:
            return 0

    def _in_preferred_grace(self, ts: TorrentState) -> bool:
        """True while a non-preferred NEW row must wait for a preferred copy.

        Anchored on the persisted created_at (restart-safe, no extra
        bookkeeping): a row older than the window proceeds. Any doubt
        fails open (proceed) — grace delays work, so it must never
        trigger spuriously.
        """
        try:
            grace = self._preferred_grace_seconds()
            if grace <= 0:
                return False
            created = getattr(ts, "created_at", None)
            if created is None:
                return False
            now = dt.datetime.now(dt.UTC)
            try:
                if created.tzinfo is None:
                    created = created.replace(tzinfo=dt.UTC)
            except Exception:
                return False
            return (now - created).total_seconds() < grace
        except Exception:
            return False

    def _preferred_grace_left(self, ts: TorrentState) -> int:
        """Whole seconds of grace remaining (0 when none)."""
        try:
            grace = self._preferred_grace_seconds()
            created = getattr(ts, "created_at", None)
            if grace <= 0 or created is None:
                return 0
            now = dt.datetime.now(dt.UTC)
            try:
                if created.tzinfo is None:
                    created = created.replace(tzinfo=dt.UTC)
            except Exception:
                return 0
            return max(0, int(grace - (now - created).total_seconds()))
        except Exception:
            return 0

    def _grace_holds_peer(self, peer: TorrentState, ts: TorrentState) -> bool:
        """True when a fellow waiter blocks nobody (phantom-owner guard).

        A waiter that is itself grace-held owns nothing yet — deferring to
        it serializes behind a download that may never start. Follows the
        hold rule exactly (rank-2, prefer-possible, in-window); locked rows
        always block, /start_-exempt rows always block, and anything
        doubtful blocks (fail-safe: skipping is the exceptional path).
        """
        try:
            try:
                if (peer.source_infohash or "").lower() == (ts.source_infohash or "").lower():
                    return False
            except Exception:
                return False
            try:
                if peer.state not in WATCH_ELECTION_WAITER_STATES:
                    return False
            except Exception:
                return False
            try:
                if not self._is_watch_row(peer):
                    return False
            except Exception:
                return False
            try:
                if self._watch_rank(peer) < 2:
                    return False
            except Exception:
                return False
            try:
                prow = getattr(self.cfg, "prowlarr", None)
                if not (
                    bool(getattr(prow, "enabled", False))
                    and bool(getattr(prow, "download_indexers", None))
                ):
                    return False
            except Exception:
                return False
            try:
                if not self._in_preferred_grace(peer):
                    return False
            except Exception:
                return False
            try:
                ex = getattr(self, "_grace_exempt", None)
                if isinstance(ex, dict) and (peer.source_infohash or "").lower() in ex:
                    return False
            except Exception:
                pass
            return True
        except Exception:
            return False

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

        Grace-held peers own nothing yet (no SSD budget, no client entry),
        so they never block: they are dropped before electing, letting
        fellow waiters evaluate — and hold — independently instead of
        deferring to a phantom owner. Whoever locks first (grace expiry,
        /start_) blocks the rest via the unchanged locked rule; exempt
        (/start_'d) rows always block. Inert when grace is disabled.
        """
        try:
            # all_active() suffices: election peers/winners can only be in
            # waiter/locked states (all non-terminal), so DONE/FAILED rows
            # could never match — and skipping them shrinks the scan on
            # long-lived DBs with hundreds of settled rows.
            rows = self.store.all_active()
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir election: cannot list rows (%s); proceeding solo", e)
            return True, None
        try:
            rows = [r for r in (rows or []) if not self._grace_holds_peer(r, ts)]
        except Exception:
            pass
        try:
            # Held (skipped) waiters own nothing yet — like grace-held
            # peers they must not block fellow waiters while frozen.
            # Held LOCKED rows keep blocking: their SSD budget / client
            # presence is real and a duplicate download to the same
            # save_path must still be prevented.
            rows = [
                r
                for r in (rows or [])
                if not (getattr(r, "skipped", 0) and r.state in WATCH_ELECTION_WAITER_STATES)
            ]
        except Exception:
            pass
        try:
            winner = watch_election_winner(rows, ts, self.cfg)
        except Exception as e:  # noqa: BLE001
            log.warning("watch-dir election failed (%s); proceeding solo", e)
            return True, None
        if winner is None:
            return True, None
        return False, winner

    def _exempt_grace_hash(self, h: str) -> None:
        """Record a one-shot /start_ exemption (bounded, pruned with rows)."""
        try:
            if not h:
                return
            d = getattr(self, "_grace_exempt", None)
            if not isinstance(d, dict):
                d = {}
                self._grace_exempt = d  # type: ignore[attr-defined]
            d[h] = time.monotonic()
            if len(d) > 5000:
                for k in list(d.keys())[: len(d) - 5000]:
                    d.pop(k, None)
        except Exception:
            pass

    def prefer_grace_row(self, target: str) -> tuple[TorrentState | None, str]:
        """Operator override: start a grace-held row's SSD download now.

        Works for watch-dir drops and VPS1 racing rows held in the
        preferred-copy grace. Returns (row to wake or None, reply message).
        The caller spawns the worker (needs a running loop); the one-shot
        exemption alone already guarantees pickup on the next tick. Waiting
        watch siblings need no update: once this row locks (QUEUED+), the
        existing election defers them automatically and the wait-note names
        it as owner; racing duplicates collapse via discovery as usual.
        Shapes follow _resolve_cancel_target (hex validation, exact-first,
        ambiguity errors). Refuses when overriding is meaningless (not NEW
        / already preferred / grace expired) or would duplicate work (a
        watch owner already exists).
        """
        try:
            norm = (target or "").strip().lower()
        except Exception:
            norm = ""
        if not norm or any(c not in "0123456789abcdef" for c in norm):
            raise LookupError(f"not a torrent hash: {target!r}")
        try:
            store = getattr(self, "store", None)
            rows = store.all() if store is not None and hasattr(store, "all") else []
        except Exception:
            rows = []
        ts = None
        if len(norm) == 40:
            for r in rows or []:
                try:
                    if (r.source_infohash or "").lower() == norm:
                        ts = r
                        break
                except Exception:
                    continue
            if ts is None:
                raise LookupError(
                    f"no tracked torrent is {norm} (it may already be done/cancelled)"
                )
        else:
            cands = []
            for r in rows or []:
                try:
                    if (r.state == State.NEW) and (r.source_infohash or "").lower().startswith(
                        norm
                    ):
                        cands.append(r)
                except Exception:
                    continue
            if len(cands) > 1:
                preview = ", ".join(
                    f"{(t.source_name or '?')[:30]} ({(t.source_infohash or '')[:10]})"
                    for t in cands[:5]
                )
                raise LookupError(
                    f"/start_{norm} matches {len(cands)} waiting torrents: "
                    f"{preview}; send /start_<full 40-char hash>"
                )
            if not cands:
                raise LookupError(
                    f"no waiting NEW torrent starts with {norm!r} (it may already be running/done)"
                )
            ts = cands[0]
        name = (ts.source_name or (ts.source_infohash or "")[:10])[:50]
        if getattr(ts, "skipped", 0):
            return None, (f"{name} is skipped — /unskip it first")
        if ts.state != State.NEW:
            return None, (
                f"{name} is {ts.state.value} — nothing to prefer "
                "(only NEW rows waiting in grace can be started)"
            )
        if self._is_watch_row(ts):
            return self._prefer_watch_row(ts, name, rows)
        return self._prefer_racing_row(ts, name)

    def _prefer_watch_row(
        self, ts: TorrentState, name: str, rows: list[TorrentState]
    ) -> tuple[TorrentState | None, str]:
        """Prefer branch for a NEW watch-dir drop (rank/owner/grace checks)."""
        try:
            rank = self._watch_rank(ts)
        except Exception:
            rank = 2
        if rank < 2:
            return None, (f"{name} is already first (rank {rank}) — proceeding on its own")
        try:
            proceed, owner = self._watch_election(ts)
        except Exception:
            proceed, owner = True, None
        if not proceed or owner is not None:
            try:
                odesc = (
                    f"{(owner.source_name or '?')[:40]} "
                    f"({(owner.source_infohash or '')[:10]}, {owner.state.value})"
                    if owner is not None
                    else "another copy"
                )
            except Exception:
                odesc = "another copy"
            return None, (
                f"{name} waits on {odesc} — it goes first; forget it to force this one instead"
            )
        if not self._in_preferred_grace(ts):
            return None, (f"{name} is past its grace wait — proceeding on its own")
        h = (ts.source_infohash or "").lower()
        self._exempt_grace_hash(h)
        sibs = 0
        try:
            want = normalize_content_name(ts.source_name or "")
            if want:
                for p in rows or []:
                    try:
                        if (p.source_infohash or "").lower() == h:
                            continue
                        if p.state not in (State.NEW, State.WAITING_DISK):
                            continue
                        if not self._is_watch_row(p):
                            continue
                        if not _same_content_key(
                            p.source_name or "",
                            p.total_bytes or 0,
                            ts.source_name or "",
                            ts.total_bytes or 0,
                            allow_unknown=True,
                        ):
                            continue
                        sibs += 1
                    except Exception:
                        continue
        except Exception:
            pass
        log.info(
            "prefer: operator started %s (%s); %d waiting sibling(s)",
            ts.source_name[:60],
            h[:10],
            sibs,
        )
        return ts, (
            f"Preferred {name} — starting SSD download now; "
            f"{sibs} waiting sibling(s) will seed from fuse/remote after."
        )

    def _prefer_racing_row(self, ts: TorrentState, name: str) -> tuple[TorrentState | None, str]:
        """Prefer branch for a NEW VPS1 racing row (source-preference checks)."""
        try:
            entries = getattr(getattr(self.cfg, "prowlarr", None), "download_indexers", None)
        except Exception:
            entries = None
        if not entries:
            return None, (
                f"{name} is proceeding on its own "
                "(no download indexers configured — nothing to wait for)"
            )
        try:
            urls = [u for u in (ts.source_announce_url or "").split(",") if u] or (
                [ts.source_tracker] if ts.source_tracker else []
            )
        except Exception:
            urls = []
        try:
            if _looks_public(urls):
                return None, (f"{name} is public — proceeding on its own")
        except Exception:
            pass
        try:
            if any(self.cfg.prowlarr.is_download_indexer(u) for u in urls):
                return None, (f"{name} is already from a download indexer — proceeding on its own")
        except Exception:
            pass
        if not self._in_preferred_grace(ts):
            return None, (f"{name} is past its grace wait — proceeding on its own")
        h = (ts.source_infohash or "").lower()
        self._exempt_grace_hash(h)
        log.info("prefer: operator started racing row %s (%s)", ts.source_name[:60], h[:10])
        return ts, (f"Preferred {name} — grace skipped, starting now.")

    def _watch_wait_note(self, ts: TorrentState) -> str:
        """Short human reason a watch row is deferred, or "" when it may proceed.

        Names the winning copy's tracker ("Waiting turn · <domain> copy
        first") so the card explains itself instead of reading as stuck.
        Never raises.
        """
        try:
            proceed, owner = self._watch_election(ts)
            if proceed or owner is None:
                # Cross-path leader invisible to the watch election: defer
                # to an in-flight same-content row of any origin like an
                # election owner (grace-holding here would duplicate its
                # download post-grace).
                try:
                    _flyer = self._inflight_same_content(ts)
                except Exception:
                    _flyer = None
                if _flyer is not None:
                    owner = _flyer
                    proceed = False
            if proceed or owner is None:
                # No owner — but a sacrificial row inside its preferred-copy
                # grace is still deliberately held: say so on the card.
                try:
                    if (
                        proceed
                        and owner is None
                        and self._watch_rank(ts) >= 2
                        and self._in_preferred_grace(ts)
                    ):
                        return (
                            f"Waiting for preferred copy · {self._preferred_grace_left(ts)}s left"
                        )
                except Exception:
                    pass
                return ""
            domain = announce_domain(owner.source_announce_url) or announce_domain(
                owner.source_tracker
            )
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

        # Pre-pick manual-move wait: a watch drop whose bytes are already
        # on fuse (fully or partly) needs no election/Prowlarr search.
        # The blob is in hand — never re-export it here.
        if await self._fuse_content_precheck(ts, blob=blob):
            return

        # Same-content election across watch drops: one SSD downloader
        # (public > download-tracker > sacrificial); the rest stay NEW
        # until it is DONE/FAILED/gone, then ride the already-remote
        # fast paths with no re-download.
        try:
            _proceed, _owner = self._watch_election(ts)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "watch-dir election failed for %s (%s); proceeding solo", ts.source_name[:60], e
            )
            _proceed, _owner = True, None
        if not _proceed:
            _owner_desc = (
                f"{(_owner.source_infohash or '')[:10]} ({_owner.state.value})"
                if _owner is not None
                else "unknown owner"
            )
            log.info(
                "watch-dir: deferring %s — same content owned by %s",
                ts.source_name[:60],
                _owner_desc,
            )
            return

        # Preferred-copy grace candidacy (decided after the Prowlarr search
        # below, which runs as normal): a sacrificial (rank-2) row with no
        # preferred bytes yet holds instead of locking, so a preferred drop
        # can preempt it. A /start_ operator override exempts one row
        # (one-shot, consumed here). Public/rank-1 rows, expired graces, and
        # setups with no download indexers configured (rank-1 unreachable)
        # never hold.
        try:
            _rank_now = self._watch_rank(ts)
        except Exception:
            _rank_now = 2
        # One-shot /start_ exemption (consumed here): the operator already
        # chose this row, so the hold below must not re-arm on it.
        _exempted = False
        try:
            _ex = getattr(self, "_grace_exempt", None)
            if isinstance(_ex, dict):
                _exempted = bool(_ex.pop((ts.source_infohash or "").lower(), None))
        except Exception:
            pass
        # Telegram /fetch on a manual drop means "stop waiting for the
        # indexer Prowlarr search, use the starting .torrent now": skip
        # the Prowlarr search, the sacrificial pick and the grace hold
        # below. Sticky like racing /fetch (not consumed): it only ever
        # matters pre-admission. Election/in-flight deferrals still
        # apply — those guard live downloads, not indexer waits.
        try:
            _forced = bool(getattr(ts, "force_direct", 0))
        except Exception:
            _forced = False
        try:
            _prow = getattr(self.cfg, "prowlarr", None)
            _prefer_possible = bool(getattr(_prow, "enabled", False)) and bool(
                getattr(_prow, "download_indexers", None)
            )
        except Exception:
            _prefer_possible = False
        _hold_candidate = (
            not _exempted and _rank_now >= 2 and _prefer_possible and self._in_preferred_grace(ts)
        )
        # Set when the search below secures download-indexer bytes: a
        # preferred source found automatically releases the hold.
        _found_preferred = False
        _h = (ts.source_infohash or "").lower()

        # Cross-path in-flight deferral: a same-content row of ANY origin
        # already past admission (e.g. a racing row the watch election
        # cannot see) owns this content — stay NEW and ride the remote
        # fast paths after it finishes instead of grace-holding and then
        # downloading a duplicate. Operator /start_ still overrides
        # (exempted rows skip this gate like the hold below).
        if not _exempted:
            try:
                _flyer = self._inflight_same_content(ts)
            except Exception:
                _flyer = None
            if _flyer is not None:
                log.info(
                    "watch-dir: deferring %s — same content %s already %s",
                    ts.source_name[:60],
                    (_flyer.source_infohash or "")[:10],
                    _flyer.state.value,
                )
                return

        ts.save_path = str(self.cfg.dest.save_path)
        tracker_list = [u for u in ts.source_announce_url.split(",") if u] or (
            [ts.source_tracker] if ts.source_tracker else []
        )
        is_public = _looks_public(tracker_list)
        is_download_tracker = self.cfg.prowlarr.enabled and any(
            self.cfg.prowlarr.is_download_indexer(u) for u in tracker_list
        )
        needs_sacrificial_copy = not is_download_tracker and not is_public

        chosen_blob = blob
        chosen_label = "public-watch-dir" if is_public else "watch-dir"
        chosen_infohash = ts.source_infohash
        chosen_size = ts.total_bytes

        # Prepare persistence directory for cross-seed torrents. The
        # infohash is hex-validated by the helper: untrusted values can
        # never escape the blob root via join/mkdir.
        watch_cross_dir = _watch_cross_seed_dir(self.cfg.general.state_db, ts.source_infohash)
        if watch_cross_dir is not None:
            await asyncio.to_thread(watch_cross_dir.mkdir, parents=True, exist_ok=True)
            # Always persist the dropped .torrent so it can be seeded on FUSE.
            # Skip the rewrite when already persisted: held rows re-run this
            # worker every tick and must not churn megabytes to SSD.
            _safe_name = f"{ts.source_infohash.strip().lower()}.torrent"
            try:
                _persisted = await asyncio.to_thread((watch_cross_dir / _safe_name).exists)
            except OSError:
                _persisted = False
            if not _persisted:
                await asyncio.to_thread((watch_cross_dir / _safe_name).write_bytes, blob)
        else:
            log.warning(
                "refusing blob persistence for %s: bad infohash %r",
                ts.source_name[:60],
                ts.source_infohash,
            )

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
        # Defaults for the skip/disabled branches below (which define
        # nothing): empty search scope, empty hits.
        indexers_to_query = []
        download_idx_names: list[str] = []
        hits_by_indexer: dict[str, list[TorrentHit]] = {}
        if _forced:
            log.info(
                "prowlarr: skipping search for watch-dir release %r (/fetch override: using starting .torrent)",
                ts.source_name,
            )
        elif not query_prowlarr:
            log.info(
                "prowlarr: skipping search for watch-dir release %r (watch_dir.query_prowlarr is False)",
                ts.source_name,
            )
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
            # Prowlarr search runs as normal (it is itself a preferred-source
            # finder): a sacrificial hit below releases the hold immediately.
            # While holding, repeats wait out prowlarr_retry_interval_seconds —
            # the same cadence as the WAITING_INDEXER retry loop — instead of
            # hammering the indexers every tick. Scope assembly AND search
            # both wait when not due.
            _search_due = True
            if _hold_candidate:
                _search_due = False
                try:
                    _ri = getattr(
                        getattr(self.cfg, "cross_seed", None),
                        "prowlarr_retry_interval_seconds",
                        1800,
                    )
                    if isinstance(_ri, bool) or not isinstance(_ri, (int, float)):
                        raise TypeError
                    _interval = max(60.0, float(_ri))
                    _ls = getattr(self, "_last_grace_search", None)
                    _last = _ls.get(_h) if isinstance(_ls, dict) else None
                    _search_due = _last is None or (time.monotonic() - float(_last)) >= _interval
                except Exception:
                    _search_due = True
            if _search_due:
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
            if _search_due and indexers_to_query:
                try:
                    hits_by_indexer = await self.prowlarr.search_indexers_parallel(
                        indexers_to_query, ts.source_name
                    )
                except Exception as e:
                    log.warning("watch-dir: prowlarr search failed for %s: %s", ts.source_name, e)
                    hits_by_indexer = {}
                self._note_grace_search(_h)
            elif _search_due:
                # Empty scope: nothing to query — stamp anyway so the
                # next tick doesn't rebuild the scope from scratch.
                self._note_grace_search(_h)

            # 1. Sacrificial download torrent from the download-target
            # indexers, in priority order — first exact match wins.
            # Skipped under /fetch (force_direct): the operator chose the
            # dropped bytes, so a preferred hit must not override them.
            if prefer_prowlarr and download_idx_names and not _forced:
                for dl_name in download_idx_names:
                    dl_hits = hits_by_indexer.get(dl_name, [])
                    matching_dl = [
                        h
                        for h in dl_hits
                        if _matches_release(h.title, h.size_bytes, ts.source_name, ts.total_bytes)
                    ]
                    matching_dl.sort(
                        key=lambda h: (
                            normalize_content_name(h.title)
                            != normalize_content_name(ts.source_name),
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
                        # Persist alongside the dropped + discovered blobs:
                        # the watch blob dir is the single source of truth
                        # for fuse injection, so flows that skip RE_ADDING
                        # (e.g. the QUEUED fuse-DONE fast path) still seed
                        # every copy. Same-infohash variants (identical
                        # files, different announce) share one filename —
                        # the dropped blob already represents them.
                        if dl_h.lower() != ts.source_infohash.lower():
                            if watch_cross_dir is None:
                                # No blob dir (refused hash): say so plainly
                                # instead of masking it as a fetch failure
                                # below — the bytes were fetched fine.
                                log.warning(
                                    "watch-dir: not persisting sacrificial copy %s (no blob dir)",
                                    dl_h[:10],
                                )
                            else:
                                await asyncio.to_thread(
                                    (watch_cross_dir / f"{dl_h}.torrent").write_bytes, dl_blob
                                )
                        # Commit the choice only once every step landed: a
                        # failure above must leave the dropped file as the
                        # default and let the next indexer try.
                        chosen_blob = dl_blob
                        chosen_label = "public-prowlarr"
                        chosen_size = best_dl.size_bytes
                        chosen_infohash = dl_h
                        log.info(
                            "watch-dir: using sacrificial download torrent from %s (%s)",
                            best_dl.indexer,
                            best_dl.title,
                        )
                        # Download-indexer bytes secured: a preferred source
                        # was found automatically (the primary choice) — the
                        # grace hold below must not re-arm on this run.
                        _found_preferred = True
                    except Exception as e:  # noqa: BLE001
                        log.warning(
                            "failed to fetch download indexer torrent from %s: %s; "
                            "trying the next one",
                            best_dl.indexer,
                            e,
                        )
                        continue
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
                            if watch_cross_dir is None:
                                log.warning(
                                    "watch-dir: not persisting cross-seed %s (no blob dir)",
                                    cross_h[:10],
                                )
                            elif (watch_cross_dir / f"{cross_h}.torrent").exists():
                                log.debug(
                                    "watch-dir: cross-seed %s from %s shares infohash; keeping persisted copy",
                                    cross_h[:10],
                                    hit.indexer,
                                )
                            else:
                                await asyncio.to_thread(
                                    (watch_cross_dir / f"{cross_h}.torrent").write_bytes, cross_blob
                                )
                            log.info(
                                "watch-dir: discovered cross-seed from %s: %s (%s)",
                                hit.indexer,
                                hit.title,
                                cross_h[:10],
                            )
                        except Exception as e:  # noqa: BLE001
                            log.warning("could not download cross-seed from %s: %s", hit.indexer, e)

        # Classify the chosen torrent metadata for downstream routing.
        parsed_files: list = []
        try:
            from .watchdir import extract_torrent_files_from_bencoded

            parsed_files = extract_torrent_files_from_bencoded(chosen_blob)
            if parsed_files:
                # Patched in tests as racing_sync.coordinator.classify.
                from . import coordinator as _coordinator

                cls = _coordinator.classify(parsed_files, self.cfg)
                ts.classification_kind = cls.kind
        except Exception as e:
            log.warning("watch-dir: failed to classify %s: %s", ts.source_name, e)

        # Feasibility is per individual file: anything may flow (batched as
        # needed) unless one file alone exceeds the SSD cap.
        too_big = oversize_single_file(parsed_files, self.cfg)
        if too_big is not None:
            # Such a file can never fit on the SSD; an operator copying it
            # to the remote by hand is its only path, so wait for that copy
            # instead of failing at once.
            if self._park_fuse_content_wait(ts, present=0, total=len(parsed_files or [])):
                log.warning(
                    "watch-dir: single file %s exceeds the SSD cap; waiting for "
                    "the remote copy before failing %s",
                    too_big,
                    ts.source_name,
                )
                return
            log.warning(
                "watch-dir: single file %s exceeds skip threshold; failing %s",
                too_big,
                ts.source_name,
            )
            self.transition(
                ts, State.FAILED, error=f"single file larger than skip threshold: {too_big}"
            )
            return

        ts.cross_seed_infohash = chosen_infohash.lower()
        ts.cross_seed_source = chosen_label
        ts.cross_seed_blob = chosen_blob
        ts._blob = chosen_blob

        # Preferred-copy grace, post-search: hold only when falling back to
        # the dropped non-preferred bytes (a preferred drop may still
        # arrive, or a later search may hit). A secured sacrificial copy,
        # public/rank-1 rows, expired graces, /start_'d rows, /fetch'd
        # rows, setups without download indexers — and watch configs with
        # query_prowlarr=False (no search will ever release the hold) —
        # proceed immediately. Nothing is reserved yet, so returning keeps
        # the row NEW with no leakage.
        if (
            _hold_candidate
            and query_prowlarr
            and not _found_preferred
            and not _forced
            and self._in_preferred_grace(ts)
        ):
            log.info(
                "watch-dir: holding %s for preferred copy (rank 2, %ds grace left)",
                ts.source_name[:60],
                self._preferred_grace_left(ts),
            )
            return

        # Shared admit tail with the racing flow (all-remote skip,
        # on-SSD shortcut, ledger reserve → QUEUED/WAITING_DISK).
        await self._admit_resolved(
            ts,
            blob=chosen_blob,
            size_bytes=chosen_size,
            display_name=ts.source_name[:60],
        )

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
            mount = self._target_mount_for_kind(kind, Path(self.cfg.dest.save_path))
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
        save_path = ts.save_path or (
            str(self.cfg.dest.save_path) if hasattr(self, "cfg") and self.cfg else "/tmp"
        )
        src_dir = Path(save_path).resolve()
        remote = (
            self.cfg.rclone.remote.default
            if hasattr(self, "cfg") and self.cfg and route_kind_is_sorted(ts.classification_kind)
            else (
                self.cfg.rclone.remote.unsorted if hasattr(self, "cfg") and self.cfg else "remote:"
            )
        )
        log.info(
            "moving completed batch %d/%d for %s",
            ts.batch_index + 1,
            ts.batches_total,
            ts.source_name,
        )
        extra_flags = (
            self.cfg.rclone.batch_move_extra_flags
            if hasattr(self, "cfg") and hasattr(self.cfg.rclone, "batch_move_extra_flags")
            else None
        )
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
            base = Path(ts.save_path) if ts.save_path else Path(self.cfg.dest.save_path)
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
                base = Path(ts.save_path) if ts.save_path else Path(self.cfg.dest.save_path)
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

    # ---- fuse surface lives in coordinator_fuse.py (FuseMixin) ----
