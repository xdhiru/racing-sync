"""Coordinator ops mixin: source intake and observation.

Split out of the coordinator god-file. This half owns "what is out
there": the watch-dir scan, the racing-client source list (cached and
single-flighted), release grouping/election into NEW rows, the intake
handoff that starts workers for fresh arrivals, the Prowlarr-backed
row lookups (`/find`, source-vanish fallbacks) and the cross-seed
decision helpers the worker consumes.

The mixin is duck-typed against ``Coordinator`` (the dataclass fields
live there) and must never import ``racing_sync.coordinator`` (circular).
"""

from __future__ import annotations

import asyncio
import logging
import time

from .clients.abstract import Torrent
from .clients.deluge import DelugeClient
from .clients.http_base import AuthError
from .content_keys import same_content as _same_content_key, same_release_name as _same_release_name
from .coordinator_content import (
    SourceDecision,
    _looks_public,
    announce_domain,
    normalize_content_name,
)
from .coordinator_inject import _group_is_ignored
from .io_bounds import rpc
from .state import State, TorrentState, UpsertResult
from .watchdir import WatchItem

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


# Stale source-cache expiry: a wedged VPS1 serves its last-known list for
# a while (degraded but useful), then the view expires to empty so intake
# stops grouping ghosts and upserting display names for a dead source.
_SOURCE_STALE_EXPIRY_S = 300.0


#: Sentinel from _fetch_source_or_fail: the VPS1 torrent is gone (third
#: consecutive miss) but the row was parked — not failed — because a
#: Prowlarr cross-seed may still supply the SSD bytes. Callers build a
#: pseudo-source from persisted row metadata and pick normally. Truthy
#: and distinct from None; compare by identity, never as a Torrent.
_SOURCE_GONE = object()


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
        key=lambda t: (
            (getattr(t, "size_bytes", 0) or 0),
            (getattr(t, "name", "") or ""),
            (getattr(t, "infohash", "") or "").lower(),
        ),
    )
    return next((t for t in ordered if _looks_public(t.trackers)), ordered[0])


class OpsMixin:
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
                    force_refresh=force_refresh, now_mono=now_mono
                )
        return await self._list_source_torrents_locked(
            force_refresh=force_refresh, now_mono=now_mono
        )

    def _seeded_entry(self, torrent: object) -> bool:
        """True when a source entry's save_path lives on the seed target."""
        try:
            from .path_ownership import PathOwner, classify_save_path

            save_path = str(getattr(torrent, "save_path", "") or "").strip()
            return bool(save_path) and classify_save_path(self.cfg, save_path) is PathOwner.FUSE
        except Exception:
            return False

    async def _list_source_torrents_locked(
        self,
        *,
        force_refresh: bool = False,
        now_mono: float = 0.0,
    ) -> list:
        """Source list body: caller holds `_source_list_lock` (or none)."""
        now_mono = now_mono or time.monotonic()

        try:
            all_torrents = await rpc(
                self.source_client.list_torrents(category=self.rt.intake.category),
                getattr(self, "cfg", None),
                "source list_torrents",
            )
        except AuthError:
            # Bad credentials never heal by retrying, and hammering risks
            # lockout: surface to the poller/scheduler loops' dedicated
            # AuthError arms (loud error + backoff) instead of serving an
            # infinite stale cache. Worker callers park via the
            # _process_torrent AuthError arm — rows must never fail over
            # an ops incident.
            raise
        except (TimeoutError, OSError, ValueError, RuntimeError) as e:
            # Wedged/unreachable VPS1 poll: serve the stale cache when
            # there is one, else an empty view — a source outage must
            # degrade the poll, never fail rows or abort the tick from
            # any caller. But a dead source must expire: past
            # _SOURCE_STALE_EXPIRY_S without a success, serve empty so
            # intake stops grouping ghosts and upserting display names.
            _stale = getattr(self, "_source_torrents_cache", None) or []
            try:
                _stale_age = now_mono - float(
                    getattr(self, "_source_last_success_mono", 0.0) or 0.0
                )
            except (TypeError, ValueError):
                _stale_age = 0.0
            if _stale and _stale_age > _SOURCE_STALE_EXPIRY_S:
                log.error(
                    "source poll failed (%s) for %ds; expiring stale view (%d torrent(s)): %s",
                    type(e).__name__,
                    int(_stale_age),
                    len(_stale),
                    e,
                )
                return []
            log.warning(
                "source poll failed (%s); using %s view: %s",
                type(e).__name__,
                "stale cache" if _stale else "empty",
                e,
            )
            return list(_stale)
        min_age = self.rt.intake.min_age_seconds
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
        if self.rt.chain.single_box and self.rt.seed.enabled:
            # Same client on both sides: the entries our own re-add points
            # at the seed path are finished work, not fresh racing intake.
            kept = [t for t in filtered if not self._seeded_entry(t)]
            if len(kept) != len(filtered):
                log.debug(
                    "single-box: ignoring %d source entr(ies) seeding from the seed path",
                    len(filtered) - len(kept),
                )
            filtered = kept

        self._source_torrents_cache = filtered
        self._source_torrents_cached_at = now_mono
        self._source_last_success_mono = now_mono
        return list(filtered)

    def _unmark_watch_drop(self, item: WatchItem) -> None:
        """Let a failed handoff be retried by the next watch scan."""
        try:
            forget = getattr(self.watch, "forget", None)
            if callable(forget):
                forget(item)
        except Exception:
            pass

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
            try:
                _existing = await asyncio.to_thread(self.store.get, item_hash, include_blob=False)
            except Exception as e:  # noqa: BLE001
                # The scan already marked this hash consumed: without a
                # re-emit the drop would be invisible until restart.
                log.warning(
                    "watch-dir: cannot read %s (%s); will retry next scan",
                    item.name[:60],
                    e,
                )
                self._unmark_watch_drop(item)
                continue
            if _existing is None:
                try:
                    _ignored = await asyncio.to_thread(self.store.is_ignored, item_hash) is True
                except Exception:
                    _ignored = False
                if _ignored:
                    # A cancelled drop re-appearing: respect the ignore list
                    # instead of re-ingesting. The file is left in place
                    # (never destroy what we didn't consume); unignore to
                    # reprocess it.
                    log.info(
                        "watch-dir: ignoring cancelled drop %s (%s)",
                        item.name[:60],
                        item.infohash[:10],
                    )
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
                try:
                    _res = await asyncio.to_thread(self.store.upsert, ts)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "watch-dir: ingest of %s failed (%s); will retry next scan",
                        item.name[:60],
                        e,
                    )
                    self._unmark_watch_drop(item)
                    continue
                if _res is not UpsertResult.WRITTEN:
                    # Forget tombstone refused the write: never mark the
                    # drop consumed or delete it. The file stays for the
                    # operator to unignore and re-drop.
                    log.warning(
                        "watch-dir: release is tombstoned, leaving drop in "
                        "place: %s (%s); run unignore first",
                        item.name[:60],
                        item.infohash,
                    )
                else:
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
                        infohash=item_hash,
                        name=item.name,
                        size=item.size_bytes,
                        announce=item.announce_url or "",
                        blob=item.torrent_bytes,
                        source_label="watch-dir",
                    )
                except LookupError as e:
                    log.info("watch-dir: re-drop of failed %s not revived: %s", item.name[:60], e)
                    continue
                except Exception as e:  # noqa: BLE001
                    log.warning("watch-dir: revive failed for %s: %s", item.name[:60], e)
                    self._unmark_watch_drop(item)
                    continue
                ingested = True
                log.info(
                    "watch-dir: re-queued failed release %s (%s)",
                    item.name[:60],
                    item.infohash[:10],
                )
            elif getattr(_existing, "skipped", 0):
                # A re-dropped file for a held row is the operator
                # resuming it by hand (same as /add on a skipped row):
                # clear the hold so the normal flow (fuse check, VPS1 /
                # VPS2 checks, Prowlarr cross-seed search) runs again.
                # Fail-open: a write failure just leaves it held for an
                # explicit /unskip.
                try:
                    _resumed = bool(
                        await asyncio.to_thread(
                            self.store.update_columns, item_hash, {"skipped": 0}
                        )
                    )
                except Exception:
                    _resumed = False
                if _resumed:
                    log.info(
                        "watch-dir: re-drop resumes held release %s (%s)",
                        item.name[:60],
                        item.infohash[:10],
                    )
                    ingested = True
            # Delete only what this scan ingested: an already-tracked drop
            # still belongs to the user — never destroy what we didn't
            # consume on this run. Exception: a re-dropped duplicate whose
            # row is DONE is fully handled (seeding from fuse), so with
            # delete_after_pickup it is clutter — remove it like a fresh
            # ingest. Any other state keeps the file (the first drop's
            # pipeline may still need it, or the operator may inspect it).
            if self.rt.intake.watch and self.rt.intake.watch.delete_after_pickup:
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
                            item.name[:60],
                            item.infohash[:10],
                        )
                        await self.watch.delete_picked_up(item)
        return items

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
                log.warning("source group %s failed; continuing with next group: %s", _gname, e)
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
                group[0].name[:60],
                len(group),
            )
            return None
        # Check if any torrent in this release group is already tracked in state store
        # (state.get normalizes case internally — one lookup suffices).
        # One batched round-trip (off the loop) instead of a blocking
        # get per member; first group member present wins, as before.
        existing_ts: TorrentState | None = None
        try:
            _hashes = [(_t.infohash or "").lower() for _t in group if (_t.infohash or "").strip()]
            _by_hash = await asyncio.to_thread(self.store.get_many, _hashes) if _hashes else {}
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
                matches = await asyncio.to_thread(self.store.find_by_name, _nm)
                if matches:
                    # find_by_name is a fuzzy LIKE: require the same
                    # normalized release AND (when both known) the same
                    # size, or repacks/different seasons with common
                    # prefixes ("Show.S01" vs "Show.S01E02") would be
                    # swallowed as duplicates and never downloaded.
                    same = [
                        m
                        for m in matches
                        if _same_content_key(
                            m.source_name or "",
                            m.total_bytes or 0,
                            t.name,
                            t.size_bytes or 0,
                            allow_unknown=True,
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
            if existing_ts.state == State.DONE and self.rt.acquire.inject_racing_torrents_to_fuse:
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
                len(src_torrents),
                self.rt.intake.category,
                self.rt.intake.min_age_seconds,
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
                self.rt.limits.max_active_downloads * 2 + self.rt.limits.max_concurrent_moves * 2,
            )
        except (TypeError, ValueError, AttributeError):
            _max_workers = 12
        try:
            _tasks = getattr(self, "_tasks", None)
            _inflight = (
                max(0, _max_workers - len(_tasks)) if isinstance(_tasks, set) else _max_workers
            )
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

    async def find_release(
        self,
        query: str,
        *,
        per_indexer: int = 3,
        max_indexers: int = 10,
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
            _skip = getattr(getattr(prow, "_cfg", None), "should_skip_title", None)
            if callable(_skip) and _skip(q):
                raise LookupError(f"{q!r} is skipped by config (skip_query_substrings).")
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
        indexers = indexers[: max(1, max_indexers)]
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
                out.append(
                    {
                        "name": name,
                        "total": len(hs),
                        "hits": [
                            {
                                "title": str(getattr(h, "title", "") or ""),
                                "size_bytes": int(getattr(h, "size_bytes", 0) or 0),
                            }
                            for h in hs[:per]
                        ],
                    }
                )
            except Exception:
                continue
        return {"query": q, "indexers": out}

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
            _public_mode = bool(ts.public_export_next_retry_at and not ts.indexer_next_retry_at)
        except Exception:
            _public_mode = False
        log.info(
            "%s retry timer fired for %s (attempt #%d)",
            "public-export" if _public_mode else "download-indexer",
            ts.source_name[:40],
            ts.public_export_attempts if _public_mode else ts.indexer_attempts,
        )
        self._spawn_worker(ts)

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
                if not isinstance(_memo, tuple) or len(_memo) != 2 or _memo[0] != id(cache):
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
            _is_deluge = isinstance(getattr(self, "source_client", None), DelugeClient)
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
                getattr(self, "cfg", None),
                "source get_torrent",
            )
        except TimeoutError as e:
            # Hung VPS1 lookup degrades to a quiet retry next tick —
            # never a terminal FAILED over one wedged poll.
            log.warning(
                "source lookup timed out for %s (%s); retry next tick",
                (ts.source_infohash or "")[:10],
                e,
            )
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
                log.warning(
                    "source torrent %s unseen (miss %d/3); keeping row",
                    (ts.source_infohash or "")[:10],
                    _n,
                )
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
                log.warning(
                    "source torrent %s gone from VPS1; parking for Prowlarr cross-seed",
                    (ts.source_infohash or "")[:10],
                )
                self._park_for_indexer_retry(ts, reason="source vanished from VPS1")
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
            t
            for t in all_source
            if t.infohash != st.infohash
            and (t.name == st.name or _same_release_name(t.name, st.name))
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
                    self.rt.indexers.is_download_indexer(u or "") for u in _urls
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
