"""Indexer/public retry scheduling, deferral and byte estimates.

Extracted from coordinator_admit.py; `AdmitMixin` composes this mixin."""

import datetime as dt
import logging
import math
import time

from .content_keys import same_content as _same_content_key
from .coordinator_content import _created_ts
from .coordinator_errors import AbandonedError
from .io_bounds import rpc
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


# Live-content snapshot sharing window: _inflight_same_content callers
# (workers + Telegram notes) share one all_active() scan per window.
_INFLIGHT_SNAPSHOT_S = 30.0

#: Absolute floor (seconds) for a disk-pressure-collapsed indexer wait.
#: The collapse may never fire before a couple of retries have happened, so
#: the effective window is clamped to `max(this, 2 *
#: prowlarr_retry_interval_seconds)` — the retry interval is already a tuned
#: key, this literal only guards a degenerate/zero interval.
_INDEXER_WAIT_COLLAPSE_FLOOR_S = 600.0

#: States that already own (or are acquiring) the SSD bytes for their content.
_INFLIGHT_OWNER_STATES = frozenset({State.QUEUED, State.DOWNLOADING, State.MOVING, State.RE_ADDING})

#: Pre-admission states that can still race into admission in one pass.
_INFLIGHT_PREADMIT_STATES = frozenset({State.NEW, State.WAITING_INDEXER, State.WAITING_DISK})


class RetryAdmitMixin:
    def _indexer_configured_wait_seconds(self) -> float:
        """`[acquire].indexer_wait_seconds` as a float (config default fallback).

        One definition shared by the collapse helper and the park's ceiling,
        so the trigger and the escalation cannot read the window differently.
        """
        try:
            return float(self.rt.acquire.indexer_wait_seconds or 28800)
        except (TypeError, ValueError):
            return 28800.0

    def _indexer_retry_gap_seconds(self) -> float:
        """`[acquire].prowlarr_retry_interval_seconds` as a float (same fallback)."""
        try:
            return float(self.rt.acquire.prowlarr_retry_interval_seconds or 1800)
        except (TypeError, ValueError):
            return 1800.0

    def _push_deadline_term(self, ts: TorrentState) -> tuple[float, float, float, int] | None:
        """`(push_seconds, margin_seconds, throughput_bps, size)` to charge, or None.

        The wait asks the push lane one question: how long would a push of
        THIS release take, measured on this link? Only the push module can
        answer it (it owns the byte-delta EMA), and its answer is None —
        meaning "charge nothing" — unless:

        * the throughput is MEASURED (two or more plausible live samples;
          the conservative fallback speed is explicitly not used here), and
        * a positive size for this row, and
        * the push poll cadence is readable (the margin is two polls of it).

        Anything missing, anything raising, or a coordinator double without
        the push lane at all returns None, and the deadline stays exactly the
        projection-only rule it is today. An unmeasured push may never shorten
        a wait — and may never lengthen one either.
        """
        probe = getattr(self, "push_term_for_deadline", None)
        if not callable(probe):
            return None
        try:
            size = int(getattr(ts, "total_bytes", 0) or 0)
        except (TypeError, ValueError):
            return None
        try:
            term = probe(size)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(term, tuple) or len(term) != 3:
            return None
        try:
            needed, margin, speed = float(term[0]), float(term[1]), float(term[2])
        except (TypeError, ValueError):
            return None
        if not all(math.isfinite(v) for v in (needed, margin, speed)):
            return None
        if needed < 0.0 or margin < 0.0 or speed <= 0.0:
            return None
        return needed, margin, speed, size

    def _indexer_effective_wait_seconds(self, ts: TorrentState) -> tuple[float, str]:
        """The indexer wait this row should honour NOW: `(seconds, reason)`.

        Calm disk ⇒ `(indexer_wait_seconds, "")` — today's behaviour, byte for
        byte. Under VPS1 disk pressure the window COLLAPSES to the janitor's
        own projection of how long until the box fills, because a parked row
        holds the only copy of those bytes on VPS1 and the janitor cannot
        touch a non-DONE row:

            effective = max(floor, min(indexer_wait_seconds, runway))
            runway    = seconds_to_fill − push_estimate − push_margin
            floor     = max(_INDEXER_WAIT_COLLAPSE_FLOOR_S,
                            2 * prowlarr_retry_interval_seconds)

        `runway` is the projection ONCE THE PUSH HAS BEEN PAID FOR. Waiting
        for a cross-seed only makes sense while a push could still land the
        bytes before the space is needed, so a measured push time for THIS
        release plus the push's own poll slack comes off the runway first.
        `push_estimate` and `push_margin` are charged only when both are known
        (see `_push_deadline_term`); otherwise `runway` is the bare projection
        and the collapse is exactly what it was before the push term existed.
        Because the clamp keeps its floor, a push term can never take the
        window under the floor or past the configured ceiling in either
        direction: it only ever shortens the runway, never lengthens it.

        No new key and no new tunable: the projection, the floor, the byte
        deltas the estimate is built from and the margin's cadence are all
        already derived, and the floor keeps the collapse from firing before
        at least a couple of retries have run.

        Pressure comes from the SAME sources as `pressure_push_sweep`: the
        janitor's cached free-space probe (`_last_source_free_bytes`, never a
        new VPS1 RPC) fed into `_cleanup_pressure` with the `[cleanup]`
        section, and "pressured" means that verdict's `bursting or urgent`.
        Unknown free space, a calm box or no projection (bytes_rate 0) all
        mean NO collapse — never guess.

        `reason` is a one-line, self-contained explanation, empty when
        nothing collapsed, and it names the push term whenever that term is
        what shortened the wait — an operator reading "why was this 40 minutes
        and not 8 hours" gets the arithmetic, not just the verdict. Both call
        sites — `_prowlarr_timed_out` (the racing-torrent/push trigger) and
        `_park_for_indexer_retry` (the FAILED escalation) — read only this
        helper, so they cannot drift.
        """
        configured = self._indexer_configured_wait_seconds()
        try:
            free = getattr(self, "_last_source_free_bytes", None)
            if free is None:
                return configured, ""
            _arrivals, _bytes_rate, projected, urgent, bursting = self._cleanup_pressure(
                getattr(self.rt.cleanup, "config", None), free
            )
            if not (bursting or urgent) or projected is None:
                return configured, ""
            floor = max(_INDEXER_WAIT_COLLAPSE_FLOOR_S, 2.0 * self._indexer_retry_gap_seconds())
            runway = float(projected)
            push_note = ""
            term = self._push_deadline_term(ts)
            if term is not None:
                needed, margin, speed, size = term
                runway -= needed + margin
                push_note = (
                    f", minus {needed:.0f}s to push {size} B at {speed:.0f} B/s "
                    f"plus {margin:.0f}s poll margin"
                )
            effective = max(floor, min(configured, runway))
            if effective >= configured:
                return configured, ""
            verdict = "+".join(
                name for name, on in (("bursting", bursting), ("urgent", urgent)) if on
            )
            return effective, (
                f"indexer wait collapsed under VPS1 disk pressure: effective {effective:.0f}s "
                f"vs configured {configured:.0f}s (VPS1 free {int(free)} B, {verdict}, "
                f"projected to fill in {float(projected):.0f}s{push_note})"
            )
        except Exception:
            # Fail closed to the configured window: a half-built double or a
            # missing janitor reading must never shorten (or lengthen) a wait.
            return configured, ""

    def _prowlarr_timed_out(self, ts: TorrentState) -> bool:
        """True when the opt-in racing-torrent fallback may fire.

        Requires the config flag plus an exhausted prowlarr retry window
        (first attempt older than the EFFECTIVE indexer wait — see
        `_indexer_effective_wait_seconds`; the same value the park escalates
        on, so the leech/push fallback and the FAILED path always agree).
        Fail-closed: any doubt returns False and the row keeps retrying /
        fails as before.
        """
        try:
            if not self.rt.acquire.fallback_to_racing_torrent:
                return False
            first = ts.indexer_first_queried_at
            if first is None:
                return False
            _wait_s, _reason = self._indexer_effective_wait_seconds(ts)
            window = dt.timedelta(seconds=_wait_s)
            return (dt.datetime.now(dt.UTC) - first) >= window
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
        Hard cap: the EFFECTIVE indexer wait (see
        `_indexer_effective_wait_seconds`: `indexer_wait_seconds`, collapsed
        under VPS1 disk pressure) since the FIRST attempt. If that ceiling is
        reached, mark FAILED for manual handling.

        A "source export miss" (public racing torrent whose .torrent
        can't be exported right now) never queries the download-target
        indexers, so it parks on the separate public-export schedule
        with no indexer-wait clock instead — see _park_for_public_retry.
        """
        if reason == "source export miss":
            self._park_for_public_retry(ts)
            return
        now = dt.datetime.now(dt.UTC)
        if ts.indexer_first_queried_at is None:
            ts.indexer_first_queried_at = now
        ts.indexer_attempts += 1
        _retry_gap = self._indexer_retry_gap_seconds()
        _wait_s, _collapse = self._indexer_effective_wait_seconds(ts)
        _configured_s = self._indexer_configured_wait_seconds()
        next_retry = self._schedule_retry(ts, "indexer", _retry_gap)
        window = dt.timedelta(seconds=max(60.0, _wait_s))
        elapsed = now - ts.indexer_first_queried_at

        log.info(
            "%s #%d for %s; next retry at %s (elapsed=%ds, max=%ds)",
            reason,
            ts.indexer_attempts,
            ts.source_name,
            next_retry.isoformat(timespec="seconds"),
            int(elapsed.total_seconds()),
            int(window.total_seconds()),
        )

        if elapsed >= window:
            if self._push_is_live(ts.source_infohash):
                # A live VPS1 push owns this content. The indexer window is
                # spent, but failing the row while bytes are still landing on
                # the seed remote throws the race away, so re-arm on the push
                # poll cadence instead of escalating. `indexer_first_queried_at`
                # stays exactly as it is: the moment the push dies (deadline
                # passed -> not live) the next park hits the normal ceiling.
                # This wins over a collapsed window too — a row whose bytes
                # are mid-copy is never failed for the disk it is freeing.
                log.info(
                    "%s: indexer window expired but the VPS1 push is running; "
                    "re-arming on the push cadence instead of failing (%ds)",
                    ts.source_name[:60],
                    int(self._push_poll_seconds()),
                )
                self._schedule_retry(ts, "indexer", self._push_poll_seconds())
                self._park_waiting_indexer(ts, reason="indexer")
                return
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
                if _collapse and elapsed < dt.timedelta(seconds=max(60.0, _configured_s)):
                    # The collapse — not the clock — ended the wait. Say so
                    # once, with both numbers and the disk signal, so the
                    # operator can see why this fell back after minutes
                    # instead of the full configured window.
                    log.info(
                        "%s: %s; escalating after %ds (the configured window alone "
                        "would not have been exhausted yet)",
                        ts.source_name[:60],
                        _collapse,
                        int(elapsed.total_seconds()),
                    )
                log.error(
                    "download-target indexers giving up on %s after %d attempts (%ds > %ds max)",
                    ts.source_name,
                    ts.indexer_attempts,
                    int(elapsed.total_seconds()),
                    int(window.total_seconds()),
                )
                # From WAITING_INDEXER → FAILED is legal (see ALLOWED).
                # Keep the plain-timeout wording when the window was NOT
                # collapsed; when it was, name the pressure that ended it.
                if _collapse:
                    _elapsed_int = int(elapsed.total_seconds())
                    _reason_text = (
                        f"Prowlarr cross-seed wait exhausted after {_elapsed_int}s "
                        f"under disk pressure: {_collapse}"
                    )
                else:
                    _wait_int = int(window.total_seconds())
                    _reason_text = f"Prowlarr cross-seed not found within {_wait_int}s"
                self.transition(ts, State.FAILED, error=_reason_text)
                return

        # If we're being called from _do_new (state is NEW), the park
        # transitions. If we're being re-called from _do_waiting_indexer,
        # the state is already WAITING_INDEXER and only the timer changed.
        self._park_waiting_indexer(ts, reason="indexer")

    def _park_for_public_retry(self, ts: TorrentState) -> None:
        """Park a public-export miss on its own indefinite schedule.

        Same WAITING_INDEXER state (the Telegram row keeps offering
        Fetch), but the retry runs on public_export_retry_interval_seconds
        with no indexer-wait FAILED: a transient VPS1 export glitch heals on a
        later window, and the source torrent's continued presence is the
        liveness proof. A vanished source still fails via the consecutive
        source-miss guard in _fetch_source_or_fail, so a dead torrent
        cannot spin here forever. Attempts are counted for observability.
        """
        try:
            interval = max(60.0, float(self.rt.acquire.public_export_retry_interval_seconds))
        except (TypeError, ValueError, AttributeError):
            interval = 1800.0
        now = dt.datetime.now(dt.UTC)
        if ts.public_export_first_attempted_at is None:
            ts.public_export_first_attempted_at = now
        ts.public_export_attempts += 1
        # The row runs the public schedule now: a leftover indexer timer
        # (e.g. from a misclassified retry of a public row) would make
        # list_waiting_ready wake this row every tick, inflating attempts
        # until the 8h indexer-wait FAILED a public row.
        self._schedule_retry(ts, "public", interval)
        # Unbounded schedule by design (no indexer-wait FAILED), so rotate
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
            # Memoized (one SELECT per snapshot window, and not even that
            # while [source.rclone_push] is off).
            live_pushes = self._live_push_hashes()
        except Exception:
            live_pushes = set()
        try:
            for r in rows or []:
                try:
                    if (r.source_infohash or "").lower() == me:
                        continue
                    if r.state in _INFLIGHT_OWNER_STATES:
                        bucket = owners
                    elif live_pushes and (r.source_infohash or "").lower() in live_pushes:
                        # A row whose hash owns a LIVE VPS1 push is an owner
                        # too: `_INFLIGHT_OWNER_STATES` only names SSD work,
                        # and a push row parks as WAITING_INDEXER, so without
                        # this a same-content sibling would start its own SSD
                        # download right behind the copy already running.
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
            # A parked WAITING_INDEXER row that owns a live VPS1 push is still
            # an owner (see _inflight_same_content): the waiter must not
            # re-scan and start a duplicate download behind the copy.
            if self._push_has_live_owner(_o):
                return True
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
        indexer-wait untouched so a dead owner hands back control. Silent
        when the row is gone (no upsert-resurrect).
        """
        try:
            _interval = max(60.0, float(self.rt.acquire.prowlarr_retry_interval_seconds))
        except (TypeError, ValueError):
            _interval = 1800.0
        try:
            _public_interval = max(
                60.0, float(self.rt.acquire.public_export_retry_interval_seconds)
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
