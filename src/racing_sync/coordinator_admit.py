"""Coordinator admission (split from coordinator.py).

`AdmitMixin` is the composite admission mixin: it keeps the NEW/WAITING_DISK
dispatch (`_do_new`, `_do_waiting_indexer`, `_pick_and_admit`,
`_admit_resolved`, `_admit_local`, `_adopt_ssd_entry`) and composes the
`FuseAdmitMixin`, `WatchAdmitMixin`, `QueueAdmitMixin`, `LaneAdmitMixin`
and `RetryAdmitMixin` seams that were split out of this module. Test patch-targets
(`racing_sync.coordinator.*`) resolve through the coordinator namespace via
deferred imports at the call sites (a module-level `__getattr__` cannot
serve implicit global lookups); everything else is imported directly.
"""

import asyncio
import datetime as dt
import logging
import time

from .coordinator_admit_fuse import FuseAdmitMixin
from .coordinator_admit_lanes import LaneAdmitMixin
from .coordinator_admit_queue import (
    _BLOB_PARSE_TTL_S,  # noqa: F401  (re-exported module constant)
    _SETUP_EMPTY_FILES_FAIL_TRIES,  # noqa: F401  (tests import from here)
    QueueAdmitMixin,
)
from .coordinator_admit_retry import (
    _INFLIGHT_OWNER_STATES,  # noqa: F401  (re-exported module constant)
    _INFLIGHT_PREADMIT_STATES,  # noqa: F401  (re-exported module constant)
    _INFLIGHT_SNAPSHOT_S,  # noqa: F401  (re-exported module constant)
    RetryAdmitMixin,
)
from .coordinator_admit_watch import WatchAdmitMixin
from .coordinator_content import WATCH_ORIGIN_LABELS, _looks_public
from .coordinator_errors import AbandonedError
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class AdmitMixin(FuseAdmitMixin, WatchAdmitMixin, QueueAdmitMixin, LaneAdmitMixin, RetryAdmitMixin):
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
                    _skip_title = self.rt.indexers.should_skip_title(st.name) is True
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
                    _ri = float(self.rt.acquire.prowlarr_retry_interval_seconds or 1800)
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
        ts.save_path = str(self.rt.storage.save_path)

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

        # Single box: the racing client already holds the bytes. Adopt its
        # own complete entry instead of searching an indexer for a copy.
        if self.rt.chain.adopts_local:
            await self._admit_local(ts)
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

    async def _admit_local(self, ts: TorrentState) -> None:
        """Single box: use the racing client's own complete copy as staging.

        No indexer query and no second download. A missing or unfinished
        copy parks on the retry clock; adoption needs the .torrent bytes,
        which `_adopt_ssd_entry` tries from memory, the DB, then the live
        client entry.
        """
        try:
            entries = await self._dest_entries_for([ts.source_infohash])
        except Exception:
            entries = []
        complete = None
        for entry in entries or []:
            try:
                if bool(entry.is_complete()):
                    complete = entry
                    break
            except Exception:
                continue
        if complete is not None and await self._adopt_ssd_entry(ts, complete):
            return
        self._park_for_indexer_retry(
            ts,
            reason=(
                "local acquisition: the racing client has no complete copy yet"
                if complete is None
                else "local acquisition: could not adopt the racing copy yet"
            ),
        )

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
                _interval = max(60.0, float(self.rt.acquire.prowlarr_retry_interval_seconds))
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
                            60.0, float(self.rt.acquire.public_export_retry_interval_seconds)
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

        # Single box: only the racing client's own complete copy counts;
        # never fall through to an indexer query it was configured to skip.
        if self.rt.chain.adopts_local:
            await self._admit_local(ts)
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

    # ---- fuse surface lives in coordinator_fuse.py (FuseMixin) ----
