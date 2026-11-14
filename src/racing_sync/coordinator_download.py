"""DOWNLOADING phase mixin: batch download loop + completion wait.

Split out of the coordinator god-file. The download worker resolves
per-batch file priorities, waits for batch completion (client progress
plus SSD presence), advances batch cursors in place, and tightens the
SSD reservation as batches land. Stalls park for retry (bounded), dead
tracker verdicts fail terminally via the watchdog mixin.

``LiveItem`` lives here too (its writer is the download loop);
coordinator.py re-exports it so existing import sites keep working.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from .batcher import Batch
from .classifier import classify
from .clients.abstract import TorrentFile
from .coordinator_errors import (
    TRACKER_UNREGISTERED_MARKER,
    AbandonedError,
    BatchMoveIncompleteError,
    DownloadStalledError,
    TorrentCheckingError,
    UnregisteredTorrentError,
    is_fatal_os_error,
    is_retryable_client_error,
)
from .io_bounds import rpc
from .rclone_ops import RcloneTimeoutError
from .state import State, TorrentState

# Explicit test seam for the download loop: unit tests drive
# _do_downloading with a mocked move step (or no dest client) and
# expect the batch index to advance without any download. Production
# must never take these branches — a missing/unusable client there is
# a broken invariant (fail loudly), not a shortcut.
_TESTING = False

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Client states with an actively-downloading libtorrent (flush still in
# flight): progress can already read 100% while the last pieces are not
# yet flushed/verified, so the entry has not flipped to seeding. Returning
# on progress alone then pauses + force-rechecks a torrent mid-flush,
# which aborts the flush and wedges the single global hash-check slot
# (every later row queues as "checking resume data"). Single-flow
# completion therefore waits these states out; anything else (seeding
# family, paused, checking, unknown/exotic) keeps the old behavior.
_DOWNLOADING_ACTIVE_STATES = frozenset(
    {"downloading", "stalleddl", "forceddl"})


def _is_actively_downloading(state: object) -> bool:
    """True iff the client state is still downloading (flush may pend).

    Never raises; unknowable states answer False (fail open toward the
    historic progress-only behavior, never strand a row on an exotic
    client spelling).
    """
    try:
        s = str(state or "").lower().replace(" ", "").replace("_", "")
    except Exception:
        return False
    return s in _DOWNLOADING_ACTIVE_STATES


@dataclass
class LiveItem:
    source_infohash: str
    name: str
    state: str
    progress: float
    size_mb: float
    eta: str = ""
    # Last polled download rate (bytes/s, 0 when unknown). Feeds the
    # Telegram active-tasks speed/ETA display via live_speed_map.
    dlspeed_bps: int = 0


class DownloadMixin:
    """Batch download loop + completion wait (duck-typed ``self``)."""

    async def _do_downloading(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        _adopted = ts.batches_total <= 0
        if ts.batches_total <= 0 and hasattr(self, "dest_client"):
            batches = await self._get_batches_for_torrent(ts)
            if batches is None:
                # Transient RPC failure — not "single flow". Retry next tick
                # rather than persisting batches_total=0 as truth.
                log.warning("could not resolve batches for adopted %s; retry next tick",
                            ts.source_infohash[:10])
                self.store.upsert(ts)
                return
            ts.batches_total = len(batches)
            # Clamp a stale index (e.g. file list shrank since the cursors
            # were persisted) instead of degrading to full-torrent flow;
            # an empty resolution clears the cursor to single-flow zero.
            if not batches:
                if ts.batch_index != 0:
                    log.warning("clearing stale batch_index %d (no batches) for %s",
                                ts.batch_index, ts.source_name)
                    ts.batch_index = 0
            elif ts.batch_index >= len(batches):
                log.warning("clamping stale batch_index %d to %d for %s",
                            ts.batch_index, len(batches) - 1, ts.source_name)
                ts.batch_index = len(batches) - 1
            self.store.upsert(ts)
            # Freshly adopted rows (e.g. recovery after --reset wiped the batch
            # cursors) skip QUEUED setup, so no batch was ever prioritized and
            # the client may still select a stale batch. Restart at batch 0.
            # Strict: an unverifiable selection must park, never resume blind
            # full-wanted past the SSD cap.
            try:
                await self._prepare_next_batch(ts, strict=True)
            except Exception as e:  # noqa: BLE001
                log.warning("could not prioritize first batch for %s: %s; retry next tick",
                            ts.source_infohash[:10], e)
                self.store.upsert(ts)
                return
            try:
                await self.dest_client.resume(h)
            except Exception as e:  # noqa: BLE001
                log.warning("could not resume adopted torrent %s: %s", h[:10], e)

        if _adopted:
            # Adopted rows skip QUEUED setup (no batch was ever prioritized
            # and no admission recorded): a restart burst of them would
            # sail past max_active_downloads. Gate here — parking keeps
            # the row DOWNLOADING for the next tick, no state churn.
            try:
                if await self._park_queued_for_download_slot(ts):
                    self.store.upsert(ts)
                    self._live.pop(h.lower(), None)
                    return
            except Exception:
                pass

        is_batched = ts.batches_total > 1
        consecutive_batch_failures = 0
        max_batch_failures_per_tick = 5
        # First wait iteration reuses the pre-loop batch resolution (see
                # above); later iterations re-resolve after each flip.
        _first_wait = True

        if is_batched and hasattr(self, "dest_client"):
            # Entry re-verify (best-effort): the client's selection may have
            # drifted since setup (operator change, full-wanted recovery
            # re-add, a setup that parked between add and deselect). Waiting
            # on batch N while the client downloads everything past the SSD
            # cap must never happen — re-assert the current batch before the
            # wait loop. Idempotent when already correct. Best-effort (log +
            # continue) rather than parking: the QUEUED-entry and adopted
            # gates above are the hard guarantees; a transient RPC failure
            # here retries on the next worker tick instead of abandoning an
            # in-flight batch wait. The resolved list is reused for the
            # first wait iteration below so the file list isn't fetched
            # twice back-to-back.
            _pre_batches: list[Batch] | None = None
            try:
                _pre_batches = await self._prepare_next_batch(ts, strict=True)
            except Exception as e:  # noqa: BLE001
                # Unverifiable selection: fall back to the fresh resolve
                # below (which parks on transient failure and degrades to
                # single-flow only when genuinely unresolvable — the
                # designed full-move fallback, not a blind cap breach).
                log.warning("batch priorities not verified for %s (%s); proceeding",
                            ts.source_infohash[:10], e)
                _pre_batches = None

        while not self._stop:
            # Fresh-state re-guard: this loop runs for hours across batch
            # resets — an operator forget (row gone) or API retry (row left
            # DOWNLOADING) must stop the worker instead of letting later
            # upserts resurrect or clobber the row. The worker keeps its own
            # object (rebinding to a fresh row would strand the caller's
            # state dispatch); all worker mutations are upserted immediately
            # so the two stay in sync.
            try:
                _fresh_dl = None
                if getattr(self, "store", None) is not None and hasattr(self.store, "get"):
                    _fresh_dl = self.store.get(ts.source_infohash, include_blob=False)
            except Exception:
                _fresh_dl = None
            if _fresh_dl is None:
                log.info(
                    "worker: row gone for %s (forgotten?); stopping download",
                    ts.source_infohash[:10],
                )
                return
            if isinstance(_fresh_dl, TorrentState) and _fresh_dl.state != State.DOWNLOADING:
                log.info(
                    "worker: %s left DOWNLOADING while downloading (%s); stopping",
                    ts.source_infohash[:10], _fresh_dl.state.value,
                )
                return
            # Live tracking, keyed by dest hash for client polling; the
            # source hash is recorded inside for progress-map aliasing.
            # Preserve the poll-updated progress across loop iterations
            # instead of flickering to 0 on every batch flip.
            try:
                _prev_item = self._live.get(h.lower())
                _prev_prog = float(getattr(_prev_item, "progress", 0.0) or 0.0)
            except (TypeError, ValueError, AttributeError):
                _prev_prog = 0.0
            self._live[h.lower()] = LiveItem(
                source_infohash=(ts.source_infohash or "").lower(),
                name=ts.source_name,
                state="downloading",
                progress=_prev_prog,
                size_mb=ts.total_bytes / (1024 * 1024),
            )

            cur_batch: Batch | None = None
            cur_skip: set[str] = set()
            expected_files: list[str] | None = None

            if is_batched and hasattr(self, "dest_client"):
                # First wait iteration reuses the pre-loop resolution above
                # (same files, cap and kind — more consistent than two
                # back-to-back fetches, and one fewer file-list RPC).
                # Only a non-empty resolution is reused: an empty one means
                # "single flow / unknown" here, and the fresh resolve below
                # stays authoritative (mock doubles and drifted cursors
                # fall back too). Later iterations always re-resolve (the
                # reset replaces the client entry between batches).
                if isinstance(_pre_batches, list) and _pre_batches and _first_wait:
                    batches: list[Batch] | None = _pre_batches
                else:
                    batches = await self._get_batches_for_torrent(ts)
                _first_wait = False
                try:
                    if batches is None:
                        # Transient RPC failure (not "no batches"): park this
                        # tick rather than degrading to a full-torrent wait
                        # that would defeat the SSD cap.
                        log.warning("batch resolution unavailable for %s; retry next tick",
                                    ts.source_name)
                        self.store.upsert(ts)
                        self._live.pop(h.lower(), None)
                        return
                    if batches and len(batches) != ts.batches_total:
                        # File list / cap drifted mid-run (tracker sidecar
                        # added, cap re-frozen): heal the total instead of
                        # comparing a stale index against a stale total.
                        log.warning("batch count drifted %d -> %d for %s; healing",
                                    ts.batches_total, len(batches), ts.source_name)
                        ts.batches_total = len(batches)
                        if ts.batch_index >= len(batches):
                            ts.batch_index = max(0, len(batches) - 1)
                        self.store.upsert(ts)
                    if batches and ts.batch_index < len(batches):
                        cur_batch = batches[ts.batch_index]
                        # Already on the remote from a previous run: neither
                        # waited on, downloaded, nor moved again.
                        cur_skip = await self._fuse_skipped(
                            [(e.file_name, e.size_bytes) for e in cur_batch.episodes],
                            ts.classification_kind or "unknown",
                        )
                        expected_files = [
                            ep.file_name for ep in cur_batch.episodes
                            if ep.file_name not in cur_skip
                        ]
                except Exception as e:
                    log.warning("could not resolve batches for %s: %s", ts.source_name, e)

            try:
                await self._wait_for_completion(ts, expected_files=expected_files)
            except UnregisteredTorrentError as e:
                # Tracker deleted the release (or revoked our auth): no
                # seeds will ever come. Fail terminally — parking would pin
                # a download slot forever, and auto-retry must never revive
                # an unregistered row (see auto_retry_failed).
                await self._fail_downloading_row(ts, h, str(e))
                try:
                    _err = str(e) or ""
                except Exception:
                    _err = ""
                if _err.startswith(TRACKER_UNREGISTERED_MARKER):
                    # Same-tracker siblings waiting on this release (NEW /
                    # QUEUED / WAITING_DISK / WAITING_INDEXER) are
                    # doomed by the same deletion — fail them now instead of
                    # letting each burn a slot and trip the watchdog alone.
                    # Auth deaths don't cascade (fixable account-side).
                    try:
                        await self._cascade_tracker_death(ts, _err)
                    except Exception:
                        pass
                return
            except DownloadStalledError as e:
                # No progress for a full stall window. Park for retry, but
                # count consecutive no-progress parks: a row that never
                # advances between windows is dead weight pinning a slot —
                # fail it after download_max_stall_parks instead of parking
                # forever. Any progress resets the count.
                try:
                    _max_parks = int(getattr(
                        getattr(self.cfg, "general", None),
                        "download_max_stall_parks", 3) or 3)
                except (TypeError, ValueError):
                    _max_parks = 3
                try:
                    _parks = getattr(self, "_dl_stall_parks", None)
                    if not isinstance(_parks, dict):
                        _parks = {}
                        self._dl_stall_parks = _parks  # type: ignore[attr-defined]
                except Exception:
                    _parks = {}
                _key = (ts.source_infohash or "").lower()
                try:
                    _cur_prog = float(getattr(e, "progress", 0.0) or 0.0)
                except (TypeError, ValueError):
                    _cur_prog = 0.0
                try:
                    _rec = _parks.get(_key)
                    if isinstance(_rec, (list, tuple)) and len(_rec) == 2:
                        _old_prog = (float(_rec[0]) if _rec[0] is not None
                                     else None)
                        _old_n = int(_rec[1])
                    else:
                        _old_prog, _old_n = None, 0
                except (TypeError, ValueError):
                    _old_prog, _old_n = None, 0
                if _old_prog is None or _cur_prog > _old_prog + 1e-9:
                    # Baseline (first sighting) or real progress: (re)start
                    # the stagnant count instead of counting the first
                    # sighting as a stall.
                    _n = 0
                else:
                    _n = _old_n + 1
                try:
                    _parks[_key] = [_cur_prog, _n]
                    if len(_parks) > 5000:
                        for _k in list(_parks.keys())[: len(_parks) - 5000]:
                            _parks.pop(_k, None)
                except Exception:
                    pass
                if _n >= max(1, _max_parks):
                    await self._fail_downloading_row(
                        ts, h,
                        f"stalled with no progress for {_n} consecutive "
                        f"windows ({e})")
                    return
                log.warning("download stalled for %s (%s); parking (%d/%d)",
                            ts.source_name, e, _n, max(1, _max_parks))
                self.store.upsert(ts)
                return
            except TimeoutError as e:
                # Transport/outage park, deliberately NOT counted in the
                # stall escalation above: an outage heals on recovery (no
                # escalation needed), while genuinely dead swarms surface
                # as DownloadStalledError (counted) or via the tracker's
                # watchdog (unregistered → FAILED). Counting outages here
                # would fail rows over a client restart.
                log.warning("download stalled for %s (%s); parking", ts.source_name, e)
                self.store.upsert(ts)
                return
            finally:
                self._live.pop(h.lower(), None)

            if self._stop:
                return

            if is_batched:
                if cur_batch is not None:
                    if hasattr(self, "dest_client"):
                        paused_ok, pause_err = await self._pause_verified(h)
                        if not paused_ok:
                            if isinstance(pause_err, TorrentCheckingError):
                                # Hash check in flight: same wait-it-out as
                                # the move path, but this worker is mid-batch
                                # (DOWNLOADING) — park at once instead of
                                # burning the 5-retry budget in ~25s against
                                # a check that needs minutes.
                                log.debug(
                                    "batch move waiting out client hash check "
                                    "for %s (%s); parking in DOWNLOADING",
                                    ts.source_name[:60],
                                    pause_err.client_state,
                                )
                                self.store.upsert(ts)
                                self._live.pop(h.lower(), None)
                                return
                            # Never move while qB is still writing — park at once
                            # instead of holding the worker on sleep(5)
                            # retries; the next tick resumes. State stays
                            # DOWNLOADING.
                            log.warning(
                                "could not pause torrent %s before batch move (%s); parking",
                                h[:10], pause_err,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                    try:
                        await self._move_and_clean_batch(ts, cur_batch, skip=cur_skip)
                    except RcloneTimeoutError as e:
                        # Hung remote mid-batch: child terminated, bytes
                        # intact — park in DOWNLOADING for next tick at once.
                        # (Must NOT funnel into the incomplete-move retry
                        # below: each attempt would block for the full
                        # timeout again.)
                        log.error("rclone batch move timed out for %s: %s",
                                  ts.source_name[:60], e)
                        try:
                            ts.last_error = f"batch move timed out: {e}"[:500]
                        except Exception:
                            pass
                        self.store.upsert(ts)
                        self._live.pop(h.lower(), None)
                        return
                    except BatchMoveIncompleteError as e:
                        # Same-tick retry like the pause failure above: the
                        # batch is still fully on local disk, nothing advanced.
                        # Bound it for the same reason — never wedge the worker.
                        consecutive_batch_failures += 1
                        log.warning(
                            "batch move %d/%d for %s incomplete (%s); retrying shortly (%d/%d)",
                            ts.batch_index + 1, ts.batches_total, ts.source_name, e,
                            consecutive_batch_failures, max_batch_failures_per_tick,
                        )
                        if consecutive_batch_failures >= max_batch_failures_per_tick:
                            log.warning(
                                "parking %s in DOWNLOADING after %d incomplete batch moves",
                                ts.source_name, consecutive_batch_failures,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        await asyncio.sleep(5)
                        continue
                    consecutive_batch_failures = 0
                    next_index = ts.batch_index + 1
                    if next_index < ts.batches_total:
                        # In-place batch advance: the entry stays (same
                        # hash, same live key) and only the next batch's
                        # files are selected + resumed. Shared-piece
                        # partials of the next batch carry over instead of
                        # being wiped + re-downloaded. Index advances only
                        # after the flip succeeds; a transient failure
                        # replays this already-moved batch as a no-op via
                        # _fuse_skipped next tick. The dest hash never
                        # changes, so no live-key refresh is needed.
                        try:
                            flip_pos = await self._flip_to_next_batch(ts, next_index)
                        except Exception as e:  # noqa: BLE001
                            if not is_retryable_client_error(e):
                                raise
                            log.warning(
                                "batch flip hit transient dest error for %s (%s); retry next tick",
                                ts.source_name, e,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        if flip_pos is False or flip_pos is None:
                            # A perpetually failing flip (priority RPC down)
                            # must not park silently forever: count it like
                            # any batch failure so it surfaces in the logs.
                            consecutive_batch_failures += 1
                            log.warning(
                                "batch flip failed for %s (%d consecutive); parking",
                                ts.source_name[:60],
                                consecutive_batch_failures,
                            )
                            self.store.upsert(ts)
                            self._live.pop(h.lower(), None)
                            return
                        # Position contract: int (possibly reconciled on
                        # grouping shrink).
                        ts.batch_index = flip_pos if isinstance(flip_pos, int) else next_index
                        self.store.upsert(ts)
                    else:
                        ts.batch_index = next_index
                        self.store.upsert(ts)
                    # Tighten the SSD reservation as batches land on the
                    # remote: only remaining work should block waiters (a
                    # 32 GB season with 20 GB already moved must not pin
                    # 32 GB). Skipped here on the final batch: its bytes
                    # are still on SSD until the MOVING wipe releases them.
                    if ts.batch_index < ts.batches_total:
                        try:
                            _rem = await self._remaining_batch_footprint(
                                batches, ts.classification_kind or "unknown")
                            if _rem is not None:
                                await self._ssd_adjust(
                                    ts.source_infohash, int(_rem))
                        except Exception:
                            pass
                elif _TESTING and hasattr(self, "_move_and_clean_batch") and hasattr(self._move_and_clean_batch, "mock_calls"):
                    # Mock in unit test (e.g. AsyncMock)
                    await self._move_and_clean_batch(ts, None)  # type: ignore[arg-type]
                    ts.batch_index += 1
                    self.store.upsert(ts)
                elif _TESTING and not hasattr(self, "dest_client"):
                    # Test dummy without dest_client attached
                    ts.batch_index += 1
                    self.store.upsert(ts)
                else:
                    # Reachable only when re-resolution yields genuinely no
                    # batches (kind flipped single mid-run): fall through to
                    # MOVING for a full move instead of failing after
                    # successful downloads. (RPC-transient None parks above.)
                    log.warning(
                        "current batch %d could not be resolved for %s; "
                        "falling back to full move",
                        ts.batch_index, ts.source_name,
                    )
                    break

                if ts.batch_index < ts.batches_total:
                    # Next batch was already prioritized + resumed inside the
                    # flip above; loop back to wait for it.
                    continue

            break

        self._live.pop(h.lower(), None)
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
        poll_n = 0
        flush_since: float | None = None
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
        try:
            # A 0/negative poll (test double, corrupt config) would
            # sleep(0)-spin and hammer the client; clamp to 1s.
            poll_interval = max(1.0, float(poll_interval))
        except (TypeError, ValueError):
            poll_interval = 2.0

        if expected_files is not None and not expected_files:
            # Whole batch already on the remote: nothing to wait for.
            return

        # Central read-retry for the direct-fallback fetches below (the
        # snapshot path needs none): a transient blip retries in 1/2/5s
        # instead of failing or burning a stall window. Defined once —
        # the loop below must not rebuild decorators per poll.
        from .retry import retryable

        async def _fetch_poll():
            return await rpc(self.dest_client.get_torrent(h),
                             getattr(self, "cfg", None),
                             "dest poll get_torrent")

        async def _fetch_wait_files():
            return await rpc(self.dest_client.get_torrent_files(h),
                             getattr(self, "cfg", None), "dest wait files")

        _poll_read = retryable(label="dest poll get_torrent")(_fetch_poll)
        _wait_files_read = retryable(
            label="dest wait files")(_fetch_wait_files)

        while not self._stop:
            # Central snapshot first: the listener poll already fetched
            # this row seconds ago — a per-row get_torrent every 1-2s per
            # worker is O(workers x polls). A stale/missing snapshot means
            # the listener is down: one direct fetch (never blind). A
            # consumed `complete` verdict still goes through every gate
            # below (vanish handling, file list, byte-on-disk) — the fast
            # path only skips the redundant RPC, never a check.
            self._consume_live_verdict(h)
            t = self._live_snapshot_for(h)
            if t is None:
                t = await _poll_read()
            if t is None:
                # Vanished entry + vanished row = the operator abandoned
                # this torrent mid-download (forget/cancel): unwind quietly
                # so the worker wrapper doesn't resurrect it as FAILED.
                # Vanished entry + live row = real client-side loss: fail.
                try:
                    _gone = (
                        getattr(self, "store", None) is not None
                        and hasattr(self.store, "get")
                        and self.store.get(ts.source_infohash, include_blob=False) is None
                    )
                except Exception:
                    _gone = False
                if _gone:
                    log.info(
                        "worker: row gone for %s; stopping download",
                        ts.source_infohash[:10],
                    )
                    raise AbandonedError(
                        f"row forgotten while downloading: {h[:10]}")
                raise RuntimeError(f"torrent vanished mid-download: {h}")

            # Dead-torrent watchdog: a release the tracker deleted reports
            # 0 seeds forever and would otherwise pin a download slot until
            # manual intervention. Client state is free (in-row); tracker
            # messages cost one cheap RPC every 10th poll (~every few min).
            poll_n += 1
            try:
                _t_state = str(getattr(t, "state", "") or "")
            except Exception:
                _t_state = ""
            if _t_state == "missingFiles" or poll_n % 10 == 1:
                try:
                    await self._check_dest_tracker_health(ts, h, _t_state)
                except (UnregisteredTorrentError, RuntimeError,
                        asyncio.CancelledError):
                    # Terminal verdicts (and cancellation) propagate; anything
                    # else is watchdog-internal noise that must never kill a
                    # healthy download.
                    raise
                except Exception as e:  # noqa: BLE001
                    log.debug("tracker health check failed for %s: %s",
                              (ts.source_infohash or "")[:10], e)

            if expected_files is not None:
                files = await _wait_files_read()
                f_map = {f.name: f for f in files}
                missing_files = [fn for fn in expected_files if fn not in f_map]
                # File-list drift (metadata re-resolve, deselect, rename)
                # and registration-lag empties are usually transient: the
                # outer worker re-resolves batches next tick. Count
                # consecutive drifting ticks per row (persisted across
                # parks, like _dl_stall_parks) so only a persistent
                # mismatch fails the row.
                _drift_key = (ts.source_infohash or "").lower()
                try:
                    _drift = getattr(self, "_dl_file_drift", None)
                    if not isinstance(_drift, dict):
                        _drift = {}
                        self._dl_file_drift = _drift  # type: ignore[attr-defined]
                except Exception:
                    _drift = {}
                try:
                    _drift_n = int(_drift.get(_drift_key, 0) or 0)
                except (TypeError, ValueError):
                    _drift_n = 0
                if missing_files:
                    _drift_n += 1
                    try:
                        _drift[_drift_key] = _drift_n
                        if len(_drift) > 5000:
                            for _k in list(_drift.keys())[: len(_drift) - 5000]:
                                _drift.pop(_k, None)
                    except Exception:
                        pass
                    if _drift_n < 3:
                        _what = (
                            "file list empty" if not files
                            else "expected batch files missing")
                        raise TimeoutError(
                            f"{_what} for {ts.source_name} "
                            f"({_drift_n}/3); re-planning next tick"
                        )
                    raise RuntimeError(
                        f"expected batch files missing from torrent {ts.source_name}: {missing_files}"
                    )
                try:
                    _drift.pop(_drift_key, None)
                except Exception:
                    pass
                batch_files = [f_map[fn] for fn in expected_files if fn in f_map]
                total_sz = sum(f.size_bytes for f in batch_files)
                done_sz = sum(f.size_bytes * f.progress for f in batch_files)
                prog = done_sz / total_sz if total_sz > 0 else 0.0
                # Zero-size files carry no bytes: presence on SSD (verified
                # below) is completion for them, not client progress.
                all_done = (
                    len(batch_files) == len(expected_files)
                    and all((f.progress or 0.0) >= 0.999 or not (f.size_bytes or 0)
                            for f in batch_files)
                )
                if all_done and not await self._batch_bytes_on_disk(ts, batch_files):
                    # Client reports complete but bytes are not on SSD
                    # (desynced piece map after a move deleted files): keep
                    # polling so the client re-fetches instead of moving air.
                    log.warning(
                        "batch %d for %s reports complete but bytes missing on SSD; waiting",
                        ts.batch_index + 1, ts.source_name,
                    )
                    all_done = False
            else:
                prog = t.progress
                all_done = t.is_complete()

            if h.lower() in self._live:
                self._live[h.lower()].progress = prog
                try:
                    self._live[h.lower()].dlspeed_bps = int(
                        getattr(t, "dlspeed_bps", 0) or 0)
                except (TypeError, ValueError):
                    self._live[h.lower()].dlspeed_bps = 0

            now = time.monotonic()
            if now - last_log > 60:
                log.info(
                    "download %s: %.1f%% (%d MB)",
                    ts.source_name,
                    prog * 100,
                    t.size_bytes // (1024 * 1024),
                )
                last_log = now

            if prog > last_progress + 1e-9:
                last_progress = prog
                last_progress_time = now
                # Genuine movement clears any pending completion-flush
                # deadline so only a truly frozen 100% accumulates time.
                flush_since = None
            elif prog < 0.999:
                stalled_for = now - last_progress_time
                if stall_timeout > 0 and stalled_for > stall_timeout:
                    raise DownloadStalledError(
                        f"download {ts.source_name} stalled at {prog * 100:.1f}% "
                        f"for {int(stalled_for)}s",
                        progress=prog,
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
                # Single-flow only (expected_files is None reaches this
                # branch with torrent-level progress): 100% while still
                # downloading is libtorrent's completion flush pending,
                # stretched to minutes on starved disks — not a done
                # download. Returning now would pause + recheck mid-flush
                # (see _do_moving's settle guard). Keep polling; the
                # >=0.999 progress skips the stall trip above, so this
                # wait is bounded by the flush deadline below, never by
                # the 24h janitor.
                if expected_files is None:
                    try:
                        _done_state = str(getattr(t, "state", "") or "")
                    except Exception:
                        _done_state = ""
                    if _is_actively_downloading(_done_state):
                        log.info(
                            "download %s at 100%% but client still %s; "
                            "waiting for the completion flush before moving",
                            ts.source_name, _done_state or "downloading",
                        )
                        # Hard deadline on the flush itself: a 100% entry
                        # whose final flush never lands would otherwise pin
                        # a worker + slot indefinitely. Reuse stall_timeout
                        # as the flush window; the caller's stall handler
                        # parks and eventually fails the row via the normal
                        # consecutive-no-progress escalation.
                        if flush_since is None:
                            flush_since = now
                        elif stall_timeout > 0 and now - flush_since > stall_timeout:
                            raise DownloadStalledError(
                                f"download {ts.source_name} stuck at 100% "
                                f"(client still {_done_state or 'downloading'}) "
                                f"waiting {int(now - flush_since)}s for the "
                                f"completion flush",
                                progress=prog,
                            )
                        await self._wait_worker_pause(ts, poll_interval)
                        continue
                # Flush landed (state flipped out of the downloading
                # family) or the batched branch finished: clear the
                # deadline tracker before the normal return.
                flush_since = None
                return
            flush_since = None
            await self._wait_worker_pause(ts, poll_interval)

    async def _wait_worker_pause(self, ts, timeout_s: float) -> None:
        """Park between download polls on the task event, not sleep().

        The listener sets the handle event on every central verdict, so
        a completion wakes the worker immediately instead of sleeping
        out the full poll interval; the timeout keeps every existing
        per-row gate (stall watchdog, tracker health, byte-on-disk)
        running when nothing happens. No handles (bare doubles) sleep
        exactly like before. Never raises except CancelledError.
        """
        try:
            _to = max(0.1, float(timeout_s or 0.0))
        except (TypeError, ValueError):
            _to = 2.0
        _ev = None
        try:
            from .task import handles_for
            for _x in handles_for(
                    self, getattr(ts, "source_infohash", "") or ""):
                try:
                    _e = getattr(_x, "event", None)
                    if _e is not None:
                        _ev = _e
                        break
                except Exception:
                    continue
        except Exception:
            _ev = None
        if _ev is None:
            try:
                await asyncio.sleep(_to)
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            return
        # Edge-triggered: clear any stale set (a verdict already
        # consumed at loop top) so this wait only fires on NEW listener
        # activity or the timeout.
        try:
            _ev.clear()
        except Exception:
            pass
        try:
            await asyncio.wait_for(_ev.wait(), _to)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        try:
            _ev.clear()
        except Exception:
            pass

    async def _prepare_next_batch(
        self, ts: TorrentState, *, strict: bool = False
    ) -> list[Batch] | None:
        """Select only the current batch's files on the dest client.

        Lenient by default (log + return on unknown, preserving the
        historical callers). With ``strict=True`` any unknowable step
        raises instead: callers that would otherwise ``resume()`` a
        possibly full-wanted entry must park instead — resuming blind
        downloads the whole season past the SSD cap (and qB pre-allocates
        every file at add time even while paused, so deselect-after-add
        never reclaims the bytes). A genuinely single-flow torrent
        (no batches) still returns normally under strict.

        Returns the resolved batch list (possibly []) so callers with a
        hot loop (the download worker) can reuse it instead of
        re-fetching the file list immediately after; None means
        unknowable (lenient path only — strict raises instead).
        """
        h = ts.dest_infohash or ts.source_infohash
        try:
            files = await rpc(self.dest_client.get_torrent_files(h),
                              getattr(self, "cfg", None), "dest next-batch files")
        except Exception as e:
            log.warning("could not get torrent files for next batch: %s", e)
            if strict:
                raise RuntimeError(f"batch files unlistable for {ts.source_name}: {e}") from e
            return None
        try:
            cls = classify(files, self.cfg)
        except Exception as e:
            log.warning("could not classify files for next batch: %s", e)
            if strict:
                raise RuntimeError(f"batch classify failed for {ts.source_name}: {e}") from e
            return None
        # Pinned kind wins over a fresh re-derive: a mid-flight sidecar
        # must not flip season->mixed under the routed batches.
        _pinned = ts.classification_kind or ""
        kind = ((_pinned if _pinned not in ("", "unknown") else "")
                or cls.kind or "unknown")
        cap = self._frozen_batch_cap(ts)
        if cap <= 0:
            try:
                cap = sum(f.size_bytes for f in files) or 1
            except Exception:
                cap = 1
            if not files and strict:
                # Unknowable under strict (empty list + no cap): the
                # fallback below would silently single-flow.
                raise RuntimeError(
                    f"batch cap unknowable for {ts.source_name}")
        try:
            batches = self._resolve_batches(files, kind, cap,
                                            episodes=cls.episodes)
        except Exception as e:
            log.warning("could not make batches for next batch: %s", e)
            if strict:
                raise RuntimeError(f"batch resolve failed for {ts.source_name}: {e}") from e
            return None
        if not batches:
            return []
        if ts.batch_index >= len(batches):
            if strict:
                # Cursor drift (grouping shrank under a persisted index):
                # heal like the download loop instead of resuming blind.
                # Narrow write + revision sync: a whole-row upsert here
                # would trip the worker's own version guard below.
                try:
                    ts.batch_index = max(0, len(batches) - 1)
                    _rev = await asyncio.to_thread(
                        self.store.update_columns,
                        ts.source_infohash,
                        {"batch_index": ts.batch_index})
                    if _rev:
                        ts.version = _rev
                except Exception:
                    pass
            else:
                return batches
        cur = batches[ts.batch_index]
        skip = await self._fuse_skipped(
            [(e.file_name, e.size_bytes) for e in cur.episodes], kind,
        )
        prio_map = {f.name: 0 for f in files}
        for ep in cur.episodes:
            if ep.file_name not in skip:
                prio_map[ep.file_name] = 1
        await self.dest_client.set_file_priorities(h, prio_map)
        return batches

    async def _flip_to_next_batch(self, ts: TorrentState, next_index: int) -> int | bool:
        """Select the next batch's files in place (no delete/re-add).

        After batch N is verified moved (straggler check passed), the
        entry keeps seeding FROM the same hash: deselected leftovers of
        already-moved batches stay put (never moved, wiped with the
        folder at the end of MOVING), and the next batch's files are
        selected + resumed. Shared-piece partials of batch N+1 left over
        from the previous download are an asset, not a hazard — the next
        batch resumes from them instead of re-downloading.

        Crash-safe like the old isolated reset: batch_index is only
        advanced by the caller AFTER this returns a position. A crash
        before then replays the already-moved batch, which is a no-op
        via _fuse_skipped (empty expected-files wait).

        Returns the batch position the caller must record, or False on
        transient failure (stay DOWNLOADING, retry next tick). Fatal
        disk/permission errors raise for the worker wrapper. When the
        re-resolved grouping shrank past ``next_index``, the total/index
        are reconciled to the finished position instead of parking
        forever.
        """
        h = ts.dest_infohash or ts.source_infohash
        # Fresh file list (registration/transient lag tolerated): the
        # priority map below must name every file, or an unnamed file
        # keeps its old (selected) priority and downloads past the cap.
        # Each fetch is RPC-bounded — 4 unbounded fetches + sleeps wedge
        # the worker 8s+ per flip on a hung client.
        files: list[TorrentFile] = []
        for _ in range(4):
            try:
                files = await rpc(self.dest_client.get_torrent_files(h),
                                  getattr(self, "cfg", None), "dest flip files")
                if files:
                    break
            except Exception:
                files = []
            await asyncio.sleep(2)
        if not files:
            log.warning(
                "batch flip: file list not ready for %s; retry next tick",
                ts.source_name,
            )
            return False
        try:
            kind = ts.classification_kind or classify(files, self.cfg).kind
        except Exception:
            kind = ts.classification_kind or "unknown"
        cap = self._frozen_batch_cap(ts)
        _cap_fallback = False
        if cap <= 0:
            cap = sum(f.size_bytes for f in files) or 1
            _cap_fallback = True
        try:
            batches = self._resolve_batches(files, kind, cap)
        except Exception as e:  # noqa: BLE001
            log.warning("batch flip: could not resolve batches for %s: %s", ts.source_name, e)
            return False
        if not batches:
            log.warning(
                "batch flip: no batches resolvable for %s; retry next tick",
                ts.source_name,
            )
            return False
        if next_index >= len(batches):
            # Grouping shrank under us (kind flip / sidecar change) and the
            # requested batch no longer exists: everything resolvable is done.
            # Reconcile cursors to finished instead of parking forever.
            # Return-only: the caller persists (single writer) — this
            # function never upserts, so a crash mid-flip replays the
            # already-moved batch as a no-op via _fuse_skipped.
            log.warning(
                "batch flip: grouping shrank to %d batches for %s; finishing",
                len(batches), ts.source_name,
            )
            ts.batches_total = len(batches)
            ts.batch_index = len(batches)
            return len(batches)
        nxt = batches[next_index]
        if _cap_fallback:
            # Unbudgeted fallback cap: refuse a next batch that doesn't
            # fit live free space instead of selecting an un-downloadable
            # batch. Fail-closed like the setup path.
            try:
                _need = await self._remaining_batch_footprint([nxt], kind)
            except Exception:
                _need = None
            if _need is None:
                try:
                    _need = sum(int(getattr(e, "size_bytes", 0) or 0)
                                for e in (nxt.episodes or []))
                except (TypeError, ValueError):
                    _need = 0
            if _need > 0:
                try:
                    from . import coordinator as _coordinator

                    if not _coordinator.ssd_has_room(self.cfg, _need):
                        log.warning(
                            "batch flip: next batch ~%d MB exceeds free space for %s; retry next tick",
                            _need // (1024 * 1024), ts.source_name)
                        return False
                except Exception:
                    pass
        try:
            skip = await self._fuse_skipped(
                [(e.file_name, e.size_bytes) for e in nxt.episodes], kind,
            )
        except Exception as e:
            # Fail closed: an unknown remote set must park, never
            # re-download bytes the remote may already hold (SSD waste
            # + double reseed). Retries next tick.
            log.warning("batch flip: fuse skip check failed for %s (%s); retry next tick",
                        ts.source_name, e)
            return False
        wanted = {e.file_name for e in nxt.episodes} - skip
        prio_map = {f.name: (1 if f.name in wanted else 0) for f in files}
        try:
            await rpc(self.dest_client.set_file_priorities(h, prio_map),
                      getattr(self, "cfg", None), "dest flip priorities")
        except Exception as e:  # noqa: BLE001
            if is_fatal_os_error(e):
                raise
            log.warning("batch flip: priority set failed for %s: %s", ts.source_name, e)
            return False
        if wanted:
            # Resume-fail parks with next-batch priorities applied but the
            # index not advanced: safe only because the priorities select
            # exactly the next batch — replaying this batch is a no-op via
            # _fuse_skipped, and the index advances solely on flip success.
            try:
                await rpc(self.dest_client.resume(h),
                          getattr(self, "cfg", None), "dest flip resume")
            except Exception as e:  # noqa: BLE001
                if is_fatal_os_error(e):
                    raise
                log.warning("batch flip: resume failed for %s: %s", ts.source_name, e)
                return False
        log.info(
            "batch flip: ready for batch %d/%d for %s (%d file(s) wanted)",
            next_index + 1, ts.batches_total, ts.source_name, len(wanted),
        )
        return next_index

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
