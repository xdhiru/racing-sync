"""Watch-dir admission: rank, election, grace and watch intake.

Extracted from coordinator_admit.py; `AdmitMixin` composes this mixin."""

import asyncio
import datetime as dt
import logging
import time

from .classifier import oversize_single_file
from .content_keys import same_content as _same_content_key
from .coordinator_content import (
    WATCH_ELECTION_WAITER_STATES,
    _looks_public,
    _matches_release,
    announce_domain,
    is_watch_row,
    normalize_content_name,
    watch_election_winner,
    watch_rank,
)
from .coordinator_paths import _watch_cross_seed_dir
from .prowlarr import TorrentHit
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class WatchAdmitMixin:
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
            if any(self.rt.indexers.is_download_indexer(u) for u in urls):
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

        ts.save_path = str(self.rt.storage.save_path)
        tracker_list = [u for u in ts.source_announce_url.split(",") if u] or (
            [ts.source_tracker] if ts.source_tracker else []
        )
        is_public = _looks_public(tracker_list)
        is_download_tracker = self.rt.indexers.enabled and any(
            self.rt.indexers.is_download_indexer(u) for u in tracker_list
        )
        needs_sacrificial_copy = not is_download_tracker and not is_public

        chosen_blob = blob
        chosen_label = "public-watch-dir" if is_public else "watch-dir"
        chosen_infohash = ts.source_infohash
        chosen_size = ts.total_bytes

        # Prepare persistence directory for cross-seed torrents. The
        # infohash is hex-validated by the helper: untrusted values can
        # never escape the blob root via join/mkdir.
        watch_cross_dir = _watch_cross_seed_dir(self.rt.paths.state_db, ts.source_infohash)
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
        watch = self.rt.intake.watch
        if watch is not None:
            qp = watch.query_prowlarr
            if isinstance(qp, bool):
                query_prowlarr = qp
            pp = watch.prefer_prowlarr_result
            if isinstance(pp, bool):
                prefer_prowlarr = pp
        # If Prowlarr is enabled and not skipped, perform single parallel search
        should_skip_prowlarr = self.rt.indexers.should_skip_title(ts.source_name)
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
        elif self.rt.indexers.enabled and self.prowlarr is not None:
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
                    _ri = self.rt.acquire.prowlarr_retry_interval_seconds
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
                for name in self.rt.indexers.tracker_map_entries:
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
