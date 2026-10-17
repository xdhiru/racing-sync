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

from .content_keys import same_content as _same_content_key
from .coordinator_content import WATCH_ORIGIN_LABELS, _looks_public
from .coordinator_errors import AbandonedError, is_retryable_client_error
from .io_bounds import offload, rpc
from .recovery import find_content_on_ssd
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

        A "source export miss" (public racing torrent whose .torrent
        can't be exported right now) never queries the download-target
        indexers, so it parks on the separate public-export schedule
        with no max-age clock instead — see _park_for_public_retry.
        """
        if reason == "source export miss":
            self._park_for_public_retry(ts)
            return
        now = dt.datetime.now(dt.timezone.utc)
        if ts.indexer_first_queried_at is None:
            ts.indexer_first_queried_at = now
        ts.indexer_attempts += 1
        try:
            _retry_gap = float(getattr(
                getattr(self.cfg, "cross_seed", None),
                "prowlarr_retry_interval_seconds", 1800) or 1800)
        except (TypeError, ValueError):
            _retry_gap = 1800.0
        try:
            _max_age_s = float(getattr(
                getattr(self.cfg, "cross_seed", None),
                "prowlarr_max_age_seconds", 86400) or 86400)
        except (TypeError, ValueError):
            _max_age_s = 86400.0
        next_retry = now + dt.timedelta(seconds=max(60.0, _retry_gap))
        ts.indexer_next_retry_at = next_retry
        max_age = dt.timedelta(seconds=max(60.0, _max_age_s))
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
                       f"{int(max_age.total_seconds())}s"),
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
            interval = max(60.0, float(
                self.cfg.cross_seed.public_export_retry_interval_seconds))
        except (TypeError, ValueError, AttributeError):
            interval = 1800.0
        now = dt.datetime.now(dt.timezone.utc)
        if ts.public_export_first_attempted_at is None:
            ts.public_export_first_attempted_at = now
        ts.public_export_attempts += 1
        ts.public_export_next_retry_at = now + dt.timedelta(seconds=interval)
        log.info(
            "source export miss #%d for %s; next retry at %s",
            ts.public_export_attempts, ts.source_name,
            ts.public_export_next_retry_at.isoformat(timespec="seconds"),
        )
        if ts.state != State.WAITING_INDEXER:
            self.transition(ts, State.WAITING_INDEXER)
        else:
            if self._abandoned(ts):
                raise AbandonedError(
                    f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                    "not refreshing public-export park"
                )
            self.store.upsert(ts)
            self._schedule_telegram_update(ts)

    def _inflight_same_content(self, ts: TorrentState):
        """Live same-content row already past admission, else None.

        Covers QUEUED/DOWNLOADING/MOVING/RE_ADDING in any origin (watch
        or racing): either way its bytes are on (or heading to) the SSD,
        so a fresh Prowlarr search here would only duplicate them. DONE
        is excluded on purpose — a finished sibling no longer holds SSD
        work, and the normal pick (with its all-remote shortcut) is the
        right path then. Fail-open None: any doubt runs the normal pick.

        Stays synchronous: the Telegram refresh loop calls this (via
        _watch_wait_note) inline, and it is one indexed SELECT — the
        heavy list scans live on offloaded paths instead.
        """
        try:
            me = (ts.source_infohash or "").lower()
            rows = self.store.all_active()
        except Exception:
            return None
        try:
            for r in rows or []:
                try:
                    if ((r.source_infohash or "").lower() == me
                            or r.state not in (State.QUEUED, State.DOWNLOADING,
                                               State.MOVING, State.RE_ADDING)):
                        continue
                    # Single content-key rule (see content_keys.same_content):
                    # same normalized name + same size when both known.
                    # Unknown sizes match (fail toward dedup, like the watch
                    # election) to avoid duplicate SSD downloads.
                    if _same_content_key(
                        r.source_name or "", r.total_bytes or 0,
                        ts.source_name or "", ts.total_bytes or 0,
                    ):
                        return r
                except Exception:
                    continue
        except Exception:
            pass
        return None

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
                "adopting SSD-resident %s (already complete on VPS2 client); "
                "skipping SSD budget",
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
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)])
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
                _interval = max(60.0, float(
                    self.cfg.cross_seed.prowlarr_retry_interval_seconds))
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
                        f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}")
                ts.indexer_next_retry_at = (
                    dt.datetime.now(dt.timezone.utc)
                    + dt.timedelta(seconds=_interval))
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
            try:
                _interval = max(60.0, float(self.cfg.cross_seed.prowlarr_retry_interval_seconds))
            except (TypeError, ValueError):
                _interval = 1800.0
            try:
                _public_interval = max(60.0, float(
                    self.cfg.cross_seed.public_export_retry_interval_seconds))
            except (TypeError, ValueError, AttributeError):
                _public_interval = 1800.0
            log.info(
                "deferring Prowlarr retry for %s — same content %s already %s",
                ts.source_name[:60], (_leader.source_infohash or "")[:10],
                _leader.state.value,
            )
            try:
                if self._abandoned(ts):
                    raise AbandonedError(
                        f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}")
                # Push the schedule this row is actually parked on: a
                # public-export row must not gain an indexer timer (it
                # would wake on the wrong schedule and confuse the
                # max-age accounting).
                if (ts.public_export_next_retry_at
                        and not ts.indexer_next_retry_at):
                    ts.public_export_next_retry_at = (
                        dt.datetime.now(dt.timezone.utc)
                        + dt.timedelta(seconds=_public_interval))
                else:
                    ts.indexer_next_retry_at = (
                        dt.datetime.now(dt.timezone.utc)
                        + dt.timedelta(seconds=_interval))
                self.store.upsert(ts)
            except Exception:
                pass
            return

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
            # Patched in tests as racing_sync.coordinator.classify.
            from . import coordinator as _coordinator

            cls = _coordinator.classify(files, self.cfg)
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
        # Parked-row on-SSD shortcut (mirrors the admit-time one above):
        # a complete dest entry may have appeared — or been added
        # manually — while parked. It needs no budget, so skip straight
        # to QUEUED instead of waiting out a full ledger for bytes that
        # will never download. Fail-open: any doubt reserves normally.
        try:
            _pre = await self._dest_complete_entry(
                [ts.source_infohash, getattr(ts, "cross_seed_infohash", None)])
        except Exception:
            _pre = None
        if _pre is not None:
            log.info("watch-dir: %s already complete on dest; skipping SSD budget",
                     ts.source_name[:60])
            try:
                _wd = getattr(self, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop((ts.source_infohash or "").lower(), None)
            except Exception:
                pass
            self.transition(ts, State.QUEUED)
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
                        _blob = await asyncio.to_thread(
                            self.store.get_blob, ts.source_infohash)
                    except Exception:
                        _blob = None
                try:
                    _foot = self._blob_admit_footprint(
                        ts, _blob, ts.total_bytes or 0)
                except Exception:
                    _foot = None
            except Exception:
                _foot = None
            if isinstance(_foot, int) and not isinstance(_foot, bool) \
                    and 0 < _foot < needed:
                log.info("retrying %s against ~%d MB batch footprint "
                         "(full ~%d MB did not fit)",
                         ts.source_name[:60], _foot // (1024 * 1024),
                         needed // (1024 * 1024))
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
                try:
                    _active_dl = len(await offload(
                        self.store.list_by_state,
                        State.QUEUED, State.DOWNLOADING))
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
            try:
                self.transition(ts, State.QUEUED)
            except (AbandonedError, ValueError) as e:
                # Row forgotten/failed concurrently after we reserved SSD:
                # release the reservation instead of leaking it until prune.
                try:
                    await self._ssd_release(ts.source_infohash)
                except Exception:
                    pass
                log.info("waiting_disk admit aborted for %s (%s)",
                         (ts.source_infohash or "")[:10], e)
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
        existing = await rpc(
            self.dest_client.list_torrents(hashes=check_hashes),
            getattr(self, "cfg", None), "dest queued list_torrents",
        )
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
                    fuse_files = await rpc(
                        self.dest_client.get_torrent_files(ext.hash),
                        getattr(self, "cfg", None), "dest fuse get_torrent_files",
                    )
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
                            await self._re_inject_watch_dir_torrents(ts)
                        except Exception as e:  # noqa: BLE001
                            if not is_retryable_client_error(e):
                                # Fatal disk/permission errors (ENOSPC,
                                # EACCES, …) must fail loudly, never park.
                                raise
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
            # Batch-priority re-verify before resume: this entry may predate
            # its priorities (setup-transient park, restart between add and
            # deselect, recovery re-add). Resuming full-wanted downloads the
            # whole season past the SSD cap — and qB pre-allocates every file
            # at add time even while paused, so a late deselect never reclaims
            # the bytes. Singles resolve to no batches and resume untouched.
            try:
                await self._prepare_next_batch(ts, strict=True)
            except Exception as e:  # noqa: BLE001
                log.warning("batch priorities not verified for %s (%s); staying queued",
                            ts.source_infohash[:10], e)
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
        # Fully-remote shortcut: every byte already sits verified on fuse,
        # so adding an SSD entry just to deselect all its files (0-file
        # corpse) and delete/replace it in RE_ADDING is pure churn. Skip
        # the SSD add entirely and go fuse-gated RE_ADDING, which
        # re-verifies before injecting. Fail-open: any doubt adds normally.
        try:
            _fully_remote = await self._blob_fully_remote(blob)
        except Exception:
            _fully_remote = None
        if _fully_remote is True:
            log.info(
                "content for %s already fully on remote; skipping SSD add",
                ts.source_name[:60],
            )
            self.transition(ts, State.RE_ADDING)
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
        except ValueError as e:
            # Deterministic poison (bad batching/classify, no valid files
            # for priorities): retrying the same bytes parks QUEUED forever.
            # Fail loudly so the operator sees it instead.
            # (BatchMoveIncompleteError stays on the retry path below —
            # its contract is "same batch retries".)
            log.error("queued setup poisoned for %s: %s; failing",
                      ts.source_infohash[:10], e)
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
            log.warning("queued setup hit transient dest error for %s: %s; staying queued",
                        ts.source_infohash[:10], e)
            try:
                self.store.upsert(ts)
            except Exception:
                pass
            return
