"""Coordinator admission (split from coordinator.py).

`AdmitMixin` carries SSD-source picking and admission — `_pick_and_admit`,
`_admit_resolved`, the manual-fuse adoption path, and `_do_new` — moved
verbatim. Test patch-targets (`racing_sync.coordinator.*`) resolve through
the coordinator namespace via deferred imports at the call sites (a
module-level `__getattr__` cannot serve implicit global lookups); everything
else is imported directly.
"""

import datetime as dt
import logging
from pathlib import Path

from .coordinator_content import WATCH_ORIGIN_LABELS, _looks_public
from .io_bounds import offload
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class AdmitMixin:
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
        if (ts.state == State.NEW and not getattr(ts, "force_direct", 0)
                and not _grace_exempted and not bool(getattr(decision, "preferred", True))
                and self._in_preferred_grace(ts)):
            _hold = False
            try:
                if getattr(getattr(self.cfg, "prowlarr", None),
                           "download_indexers", None):
                    _hold = True
            except Exception:
                _hold = False
            if _hold:
                log.info(
                    "holding %s for preferred copy (non-preferred direct, "
                    "%ds grace left)",
                    ts.source_name[:60], self._preferred_grace_left(ts),
                )
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
            ts, blob=decision.torrent_bytes,
            size_bytes=decision.size_bytes, display_name=st.name[:60],
        )

    async def _admit_resolved(
        self, ts: TorrentState, *, blob: bytes | None,
        size_bytes: int, display_name: str,
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
            _fully_remote = await self._blob_fully_remote(blob)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info("content for %s already fully on remote; skipping SSD budget",
                     display_name)
            try:
                self.transition(ts, State.QUEUED)
            except Exception:
                await self._ssd_release(ts.source_infohash)
                raise
            return
        try:
            _pre = await self._dest_complete_entry(
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)])
        except Exception:
            _pre = None
        if _pre is not None:
            log.info("content for %s already complete on dest; skipping SSD budget",
                     display_name)
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
                    "admitting %s against ~%d MB batch footprint "
                    "(total ~%d MB)", display_name,
                    _foot // (1024 * 1024), needed // (1024 * 1024),
                )
            needed = min(needed, _foot)
        if not await self._ssd_try_reserve(ts.source_infohash, needed):
            log.info(
                "ssd budget in use (reserved ~%d MB); parking %s",
                self._ssd_reserved_total() // (1024 * 1024), display_name,
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
            if ts.state not in (State.NEW, State.WAITING_INDEXER, State.WAITING_DISK):
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
            if ts.state not in (State.NEW, State.WAITING_INDEXER, State.WAITING_DISK):
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
                pre_ssd = await offload(
                    store.list_by_state,
                    State.NEW, State.WAITING_INDEXER, State.WAITING_DISK,
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
                    if fresh.state not in (State.NEW, State.WAITING_INDEXER, State.WAITING_DISK):
                        continue
                    if getattr(fresh, "skipped", 0):
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

        all_source = await self._list_source_torrents()
        await self._pick_and_admit(ts, st, self._same_content_torrents(all_source, st))
