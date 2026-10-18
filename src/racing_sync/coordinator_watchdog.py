"""Dead-torrent watchdog mixin: tracker-death detection + cascade.

Split out of the coordinator god-file. A release the tracker deleted
reports 0 seeds forever and would otherwise pin a download slot until
manual intervention: the download poll asks for tracker messages every
10th poll, and a terminal verdict fails the row (with Telegram paging)
plus narrowly-scoped pre-download siblings on the same tracker.
"""

from __future__ import annotations

import datetime as dt
import logging

from .coordinator_content import announce_domain, normalize_content_name
from .coordinator_errors import (
    AbandonedError,
    TRACKER_AUTH_MARKER,
    TRACKER_UNREGISTERED_MARKER,
    UnregisteredTorrentError,
    classify_tracker_message,
)
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class WatchdogMixin:
    """Tracker-death watchdog + sibling cascade (duck-typed ``self``)."""

    async def _fail_downloading_row(self, ts: TorrentState, h: str, error: str,
                                    *, delete_files: bool = True) -> None:
        """Fail a DOWNLOADING row terminally and free its slot.

        Releases the SSD reservation (a FAILED row holds no budget),
        records the error, and pages Telegram — a tracker-deleted or
        permanently stalled release needs operator eyes, not silent
        parking. Tolerates concurrent forget (abandoned row unwinds).

        `delete_files` controls the corpse-entry cleanup: True for
        pre-move rows (the dest entry is a paused partial, SSD scratch
        only), False when the bytes are already complete (MOVING rows —
        deleting the entry would destroy the only copy).
        """
        log.error("failing download for %s: %s", ts.source_name[:60], error[:200])
        # Abandoned first: a row forgotten while the worker ran must not
        # lose its dest entry (or SSD data) to a corpse cleanup for a row
        # that no longer exists — the wrapper unwinds quietly instead.
        if self._abandoned(ts):
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                "not failing it"
            )
        try:
            _csd = getattr(self, "_checking_track", None)
            if isinstance(_csd, dict):
                _csd.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass
        try:
            _ri = getattr(self, "_recheck_in_flight", None)
            if (isinstance(_ri, (list, tuple)) and len(_ri) == 2
                    and _ri[0] == (ts.source_infohash or "").lower()):
                self._recheck_in_flight = None
        except Exception:
            pass
        try:
            await self._ssd_release(ts.source_infohash)
        except Exception:
            pass
        try:
            _dh = (ts.dest_infohash or "").lower()
        except Exception:
            _dh = ""
        if _dh:
            # Remove the corpse entry: a paused partial would otherwise sit
            # on SSD forever (nothing reaps dest entries of FAILED rows).
            # SSD scratch data only — never fuse/remote. Pre-move rows are
            # partials by construction; callers pass delete_files=False
            # when the bytes are already complete (MOVING rows).
            try:
                await self.dest_client.delete(_dh, delete_files=delete_files)
            except Exception as e:  # noqa: BLE001
                log.warning("could not delete failed dest entry %s: %s",
                            _dh[:10], e)
        try:
            self._live.pop((h or "").lower(), None)
        except Exception:
            pass
        try:
            self.transition(ts, State.FAILED, error=error[:500])
        except (AbandonedError, ValueError):
            try:
                ts.state = State.FAILED
                ts.last_error = error[:500]
                self.store.upsert(ts)
            except Exception:
                return
        try:
            await self._notify_telegram(ts)
        except Exception:
            pass

    async def _check_dest_tracker_health(self, ts: TorrentState, h: str, t_state: str) -> None:
        """Raise on tracker-side or client-side download death.

        - qB `missingFiles`: our files vanished from SSD (operator wipe) —
          waiting can never complete.
        - Tracker msg "unregistered/deleted" (or revoked auth): the release
          is gone upstream — no seeds will ever come.
        Best-effort and quiet when the client exposes no messages (Deluge):
        unknown is never death. Raises UnregisteredTorrentError (terminal)
        or RuntimeError (missing files, terminal via the generic wrapper).
        """
        if (t_state or "") == "missingFiles":
            raise RuntimeError(
                f"torrent files missing on disk for {ts.source_name}: "
                "client reports missingFiles")
        try:
            _fn = getattr(self.dest_client, "get_tracker_messages", None)
            if not callable(_fn):
                return
            msgs = await _fn(h)
            if not isinstance(msgs, (list, tuple)):
                return
        except Exception:
            return
        for msg in msgs or []:
            try:
                verdict = classify_tracker_message(msg)
            except Exception:
                continue
            if verdict == "unregistered":
                raise UnregisteredTorrentError(
                    f"{TRACKER_UNREGISTERED_MARKER} {ts.source_name} — "
                    f"tracker says: {str(msg)[:200]}")
            if verdict == "auth":
                raise UnregisteredTorrentError(
                    f"{TRACKER_AUTH_MARKER} {ts.source_name} — "
                    f"tracker says: {str(msg)[:200]}")

    async def _cascade_tracker_death(self, dead_ts: TorrentState, error: str) -> None:
        """Fail pre-download siblings condemned by the same tracker deletion.

        Scope is deliberately narrow: same normalized content name, same
        known size, AND same tracker domain. A cross-seed copy of the same
        release from a *different* tracker has a live swarm and must never
        be killed by name match alone; rows with unknown tracker or size
        are left for their own watchdog. Only pre-download states
        (NEW/QUEUED/WAITING_DISK/WAITING_INDEXER) cascade —
        MOVING/RE_ADDING rows already hold the bytes, DOWNLOADING rows
        trip the watchdog themselves, DONE/FAILED are settled.
        Capped (25) so one deletion can't page a stampede.
        """
        try:
            want_norm = normalize_content_name(dead_ts.source_name or "")
            want_size = int(dead_ts.total_bytes or 0)
            me = (dead_ts.source_infohash or "").lower()
            domain = (announce_domain(dead_ts.source_announce_url or "")
                      or announce_domain(dead_ts.source_tracker or ""))
        except Exception:
            return
        if not want_norm or want_size <= 0 or not domain or not me:
            return
        try:
            rows = self.store.all_active()
        except Exception:
            return
        _CASCADE_STATES = (State.NEW, State.QUEUED,
                           State.WAITING_DISK, State.WAITING_INDEXER)
        done = 0
        for r in rows or []:
            if done >= 25:
                break
            try:
                rh = (r.source_infohash or "").lower()
                if not rh or rh == me or r.state not in _CASCADE_STATES:
                    continue
                try:
                    r_size = int(r.total_bytes or 0)
                except (TypeError, ValueError):
                    continue
                if r_size != want_size:
                    continue
                if normalize_content_name(r.source_name or "") != want_norm:
                    continue
                r_domain = (announce_domain(r.source_announce_url or "")
                            or announce_domain(r.source_tracker or ""))
                if not r_domain or r_domain != domain:
                    continue
            except Exception:
                continue
            log.error("cascading tracker deletion to %s (%s, %s)",
                      (r.source_name or rh[:10])[:60], r.state.value, domain)
            try:
                await self._fail_downloading_row(
                    r, "",
                    f"{TRACKER_UNREGISTERED_MARKER} same {domain} release as "
                    f"{(dead_ts.source_name or '')[:60]} "
                    f"({str(error)[:200]})")
                done += 1
            except Exception:
                continue
        if done:
            log.warning("tracker deletion of %s cascaded to %d sibling row(s)",
                        dead_ts.source_name[:60], done)
        # Wake surviving same-content rows promptly — best-ranked first: a
        # sibling that was deferring to the now-dead leader (or simply
        # waiting out a retry window) has no reason to sit out the rest of
        # its timer. Rank is public (0) > download-indexer (1) > other (2),
        # earliest first — the same order the watch election and the NEW-row
        # grace use — so a preferred copy takes over before a sacrificial
        # one burns ratio. Only the best WAITING_INDEXER waiter and the
        # best WAITING_DISK waiter are woken (max 2): whoever admits first
        # owns the content and the rest defer to it via _inflight_same_content;
        # if the best can't proceed it re-parks and the others follow on
        # their own timers. Attempts and max-age clocks are untouched, so no
        # retry budget is burned either way.
        try:
            _now_utc = dt.datetime.now(dt.timezone.utc)
            _cands: list[tuple[tuple[int, str, str], object]] = []
            for r in rows or []:
                try:
                    rh = (r.source_infohash or "").lower()
                    if not rh or rh == me:
                        continue
                    if r.state not in (State.WAITING_INDEXER, State.WAITING_DISK):
                        continue
                    try:
                        r_size = int(r.total_bytes or 0)
                    except (TypeError, ValueError):
                        continue
                    if r_size != want_size:
                        continue
                    if normalize_content_name(r.source_name or "") != want_norm:
                        continue
                    try:
                        _rank = int(self._watch_rank(r))
                    except Exception:
                        _rank = 2
                    try:
                        _created = str(getattr(r, "created_at", "") or "")
                    except Exception:
                        _created = ""
                    _cands.append(((_rank, _created, rh), r))
                except Exception:
                    continue
            _cands.sort(key=lambda item: item[0])
            _woke = 0
            _woke_indexer = False
            _woke_disk = False
            for (_key, r) in _cands:
                try:
                    rh = (r.source_infohash or "").lower()
                    if r.state == State.WAITING_INDEXER and not _woke_indexer:
                        try:
                            _t = r.indexer_next_retry_at
                        except Exception:
                            _t = None
                        if _t is not None and _t > _now_utc:
                            r.indexer_next_retry_at = _now_utc
                            try:
                                self.store.upsert(r)
                            except Exception:
                                pass
                            _woke += 1
                            _woke_indexer = True
                            try:
                                log.info("woke best-ranked sibling %s (rank %d) early for %s",
                                         (r.source_name or rh[:10])[:60], _key[0],
                                         dead_ts.source_name[:60])
                            except Exception:
                                pass
                    elif r.state == State.WAITING_DISK and not _woke_disk:
                        try:
                            _wd = getattr(self, "_waiting_disk_next_check", None)
                            if isinstance(_wd, dict) and _wd.pop(rh, None) is not None:
                                _woke += 1
                                _woke_disk = True
                        except Exception:
                            pass
                    if _woke_indexer and _woke_disk:
                        break
                except Exception:
                    continue
            if _woke:
                log.info("woke %d surviving sibling row(s) of %s early",
                         _woke, dead_ts.source_name[:60])
        except Exception:
            pass
