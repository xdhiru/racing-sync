"""RE_ADDING phase mixin: fuse-gate re-injection + sustained-missing healer.

Split out of the coordinator god-file: heal clock, availability gate,
drive, cross-seed re-add, and the `_re_inject_*` supplements (watch-dir,
racing, tracker_map). RE_ADDING rows re-inject their torrents onto the
fuse mount with skip_check=True only after the availability gate proves
the moved bytes are there; a sustained-missing set past the heal window
is selectively re-downloaded and moved. Fail-closed throughout: doubt
parks, never advances blind.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from pathlib import Path

from .coordinator_errors import (
    _NOT_VISIBLE_DETAIL,
    AbandonedError,
    WebUIUnresponsiveError,
    is_retryable_client_error,
)
from .coordinator_fuse import _record_injected_hash
from .coordinator_heal import ReaddHealMixin
from .coordinator_reinject import ReinjectMixin
from .io_bounds import rpc
from .state import _MAX_READD_CYCLES, State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


def _readd_gap_seconds(raw: object, default: float) -> float:
    """Bool-safe numeric gap (config `True` is 1s of nonsense, not 1s).

    Mirrors `_readd_heal_after_seconds`: a `bool` is an `int` subclass,
    so plain `isinstance(raw, (int, float))` would accept `True` as a
    1-second max-age / gap. Reject bools and non-numerics to the default.
    """
    try:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


class ReaddMixin(ReaddHealMixin, ReinjectMixin):
    """RE_ADDING drive + heal (duck-typed ``self``)."""

    async def _adopt_existing_fuse_seed(self, ts: TorrentState) -> bool:
        """Finish RE_ADDING when the content already seeds from fuse.

        Reconcile snapshots ``category="racing"``, so an entry placed in
        another category folder (e.g. ``/mnt/teldrive/qbittorrent/SUB``)
        reads as lost on boot. The availability gate derives the mount
        root from the blob and then reports the subfolder bytes missing
        forever. Verify at the entry's own fuse save_path (the only
        authoritative location) and adopt instead of re-injecting.
        """
        try:
            hashes = {
                h.lower()
                for h in (
                    ts.dest_infohash,
                    ts.source_infohash,
                    ts.cross_seed_infohash,
                    *ts.injected_private_hashes.split(","),
                )
                if h
            }
            if not hashes:
                return False
            entries = await self._dest_entries_for_hashes(hashes)
        except (AbandonedError, asyncio.CancelledError):
            raise
        except Exception as e:  # noqa: BLE001
            log.debug(
                "re-add: existing fuse lookup failed for %s: %s", (ts.source_infohash or "")[:10], e
            )
            return False
        for ext in entries or []:
            h_low = (getattr(ext, "hash", "") or "").lower()
            save_path = str(getattr(ext, "save_path", "") or "")
            if not h_low or not self._save_path_is_on_fuse(save_path):
                continue
            try:
                if not bool(ext.is_complete()):
                    continue
                files = await rpc(
                    self.dest_client.get_torrent_files(h_low),
                    getattr(self, "cfg", None),
                    "readd existing fuse get_torrent_files",
                )
                expected = [
                    (str(f.name), int(f.size_bytes or 0))
                    for f in (files or [])
                    if getattr(f, "name", "")
                ]
                if not expected:
                    continue
                missing = await self._confirmed_fuse_missing(Path(save_path), expected)
                if missing:
                    continue
                if not await self._select_all_fuse_files(h_low, label="existing fuse seed"):
                    return False
            except (AbandonedError, asyncio.CancelledError):
                raise
            except Exception as e:  # noqa: BLE001
                log.debug("re-add: fuse-entry check failed for %s: %s", h_low[:10], e)
                continue
            ts.dest_infohash = h_low
            ts.save_path = save_path
            log.info(
                "re-add: %s already seeds from fuse at %s (%s); adopting as DONE",
                ts.source_name[:50],
                save_path,
                h_low[:10],
            )
            try:
                self.transition(ts, State.DONE)
            except ValueError as e:
                try:
                    fresh = self.store.get(ts.source_infohash, include_blob=False)
                except Exception:
                    fresh = None
                if fresh is None or fresh.state != State.DONE:
                    log.warning(
                        "re-add: could not adopt %s from %s: %s",
                        (ts.source_infohash or "")[:10],
                        ts.state.value,
                        e,
                    )
                    return False
                ts.state = fresh.state
                ts.dest_infohash = fresh.dest_infohash or ts.dest_infohash
                ts.save_path = fresh.save_path or ts.save_path
                return True
            return True
        return False

    def _park_readd(
        self,
        ts: TorrentState,
        reason: str,
        *,
        delay_s: float | None = 0.0,
        level: int | None = logging.WARNING,
    ) -> bool:
        """Park RE_ADDING: record why plus the retry instant.

        Timer-park, never sleep: `readd_next_retry_at` survives a restart.
        One primitive for every re-add early return, so the reason, the
        timer and the guarded write cannot drift apart (I12). `delay_s=None`
        clears the timer (healed bytes: re-check next tick).
        """
        retry_at: dt.datetime | None = None
        if delay_s is not None:
            try:
                retry_at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=max(0.0, float(delay_s)))
            except (TypeError, ValueError):
                retry_at = dt.datetime.now(dt.UTC)
        try:
            ts.readd_next_retry_at = retry_at
        except Exception:
            pass
        return self._park_row(
            ts,
            reason,
            level=level,
            extra={"readd_next_retry_at": retry_at or ""},
        )

    async def _do_re_add(self, ts: TorrentState) -> None:
        store = getattr(self, "store", None)
        now = dt.datetime.now(dt.UTC)
        try:
            _cycles = int(getattr(ts, "readd_cycles", 0) or 0)
        except (TypeError, ValueError):
            _cycles = 0
        if _cycles >= _MAX_READD_CYCLES:
            # Rapid DONE -> re-add flapping (lost fuse entry re-added, lost
            # again, ...) would otherwise reset its timer forever and never
            # trip max-age. Page the operator instead of looping silently.
            err = (
                f"re-add flap limit reached ({_cycles} rapid DONE demotions); "
                "fuse entries keep going missing — inspect the dest client"
            )
            log.error("giving up on %s: %s", ts.source_name, err)
            ts.readd_next_retry_at = None
            self.transition(ts, State.FAILED, error=err)
            return
        # Manual fuse adoption: if the same infohash already seeds from a fuse
        # mount on VPS2 (any category / subfolder, e.g. an operator-placed
        # entry under SUB/), adopt to DONE instead of re-checking/injecting.
        try:
            if await self._adopt_manual_fuse_if_present(ts):
                return
        except Exception:  # noqa: BLE001
            pass
        # Existing fuse seed under a non-derived path (category subfolder):
        # adopt before the delay/gate, which would otherwise park forever.
        try:
            if await self._adopt_existing_fuse_seed(ts):
                return
        except (AbandonedError, asyncio.CancelledError):
            raise
        except Exception as e:  # noqa: BLE001
            log.warning(
                "re-add: existing fuse adoption check failed for %s: %s",
                ts.source_name[:50],
                e,
            )
        if ts.readd_first_attempted_at is None:
            ts.readd_first_attempted_at = now
            if store is not None:
                store.upsert(ts)

        delay = _readd_gap_seconds(self.rt.seed.reinject_delay_seconds, 0)
        if delay > 0 and ts.readd_attempts == 0 and ts.readd_next_retry_at is None:
            # Timer-park, never sleep: the retry instant persists in the DB
            # across restarts; a sleep would lose it (and stall the worker).
            log.info(
                "parking %s for %ds fuse mount indexing before re-injection",
                ts.source_name[:50],
                delay,
            )
            self._park_readd(
                ts,
                f"fuse mount indexing delay ({int(delay)}s)",
                delay_s=delay,
                level=None,
            )
            return

        max_age = dt.timedelta(
            seconds=_readd_gap_seconds(self.rt.seed.reinject_max_age_seconds, 86400)
        )

        retry_gap = _readd_gap_seconds(self.rt.seed.reinject_retry_gap_seconds, 120)

        backoff_interval = _readd_gap_seconds(self.rt.seed.reinject_backoff_seconds, 1800)

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
        if not gate_blob:
            # Self-heal for rows driven to RE_ADDING before their blob was
            # persisted (e.g. /inject on a freshly discovered row):
            # the racing torrent is still on VPS1, so live-fetch its bytes,
            # persist them, and continue through the gate on this same tick
            # instead of parking until max_age.
            try:
                healed = await self._fetch_racing_torrent_bytes((ts.source_infohash or "").lower())
            except Exception:
                healed = None
            if healed:
                try:
                    gate_blob = bytes(healed)
                except Exception:
                    gate_blob = None
            if gate_blob and store is not None:
                try:
                    new_rev = store.update_columns(
                        ts.source_infohash, {"cross_seed_blob": bytes(gate_blob)}
                    )
                    if new_rev:
                        ts.version = new_rev
                except Exception:
                    pass
                try:
                    ts.cross_seed_blob = bytes(gate_blob)
                    ts._blob = bytes(gate_blob)
                except Exception:
                    pass
                log.info(
                    "fuse gate: healed missing blob for %s from VPS1 (%d B)",
                    ts.source_name[:50],
                    len(gate_blob or b""),
                )
        gate_expected = self._expected_fuse_files(gate_blob)
        if not gate_blob:
            # Fail closed: no blob at all means we cannot verify the fuse
            # target. Park for retry instead of injecting blind with
            # skip_check=True (which creates unseedable torrents).
            log.warning(
                "fuse content unverifiable for %s (missing blob); parking re-add",
                ts.source_name[:50],
            )
            self._park_readd(
                ts, "fuse content unverifiable (missing blob)", delay_s=retry_gap, level=None
            )
            return
        if gate_expected is None:
            # Blob present but undecodable/empty: a real-sized blob is
            # corrupt — park instead of injecting blind with skip_check=True
            # (unseedable torrent). Tiny fakes (unit-test doubles) keep
            # legacy behavior so tests don't park forever.
            try:
                _blob_len = len(gate_blob or b"")
            except Exception:
                _blob_len = 0
            if _blob_len >= 128:
                log.warning(
                    "fuse gate cannot decode %d-byte blob for %s; parking re-add",
                    _blob_len,
                    ts.source_name[:50],
                )
                self._park_readd(
                    ts,
                    f"fuse gate cannot decode {_blob_len}-byte blob",
                    delay_s=retry_gap,
                    level=None,
                )
                return
            log.warning(
                "fuse gate cannot decode blob for %s; proceeding without gate",
                ts.source_name[:50],
            )
            gate_expected = []
        if gate_expected:
            gate_target = self._target_mount_for_ts_blob(ts, gate_blob)
            from .fuse_gate import verify_blob_on_mount as _verify

            gate_missing = (await _verify(gate_target, gate_blob, expected=gate_expected)).missing
            if gate_missing:
                preview = ", ".join(gate_missing[:5])
                if len(gate_missing) > 5:
                    preview += f", …+{len(gate_missing) - 5} more"
                log.warning(
                    "fuse content not yet available for %s at %s "
                    "(%d/%d missing, e.g. %s); parking re-add instead of injecting blind",
                    ts.source_name[:50],
                    gate_target,
                    len(gate_missing),
                    len(gate_expected),
                    preview,
                )
                # Sustained-missing healer: re-downloads + moves just the
                # never-moved files after a long stable window (never on a
                # fresh absence — that is usually fuse/rclone index lag).
                # Fail-closed: any healer trouble parks exactly as today.
                try:
                    _healed = await self._maybe_heal_readd_missing(
                        ts, gate_missing, gate_target, gate_blob
                    )
                except (AbandonedError, asyncio.CancelledError):
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "re-add heal failed for %s: %s",
                        ts.source_name[:50],
                        e,
                    )
                    _healed = False
                self._park_readd(
                    ts,
                    f"fuse content not yet available ({len(gate_missing)}/"
                    f"{len(gate_expected)} missing)",
                    delay_s=None if _healed else retry_gap,
                    level=None,
                )
                return

        # Gate passed: any heal incident for this row is over — drop its
        # clock/attempts so a future incident starts fresh.
        try:
            _ht = getattr(self, "_readd_heal_track", None)
            if isinstance(_ht, dict):
                _ht.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass

        # One drive attempt per tick (no same-tick sleep-retry): a failure
        # timer-parks and the next tick retries. Escalation rides
        # readd_attempts (reset on DONE): first failing attempt parks with
        # the short retry gap, consecutive failures back off 30m.
        ts.readd_attempts += 1
        try:
            # 1) Re-inject the racing-client torrents (private or otherwise)
            # pointing at the fuse mount with skip_check=True (req #3).
            # Watch rows (any SSD flavour label) re-inject their
            # persisted blobs instead.
            if self.rt.acquire.inject_racing_torrents_to_fuse:
                if self._is_watch_row(ts):
                    await self._re_inject_watch_dir_torrents(ts)
                else:
                    await self._re_inject_racing_torrents(ts)

            # 2) Re-add the cross-seed torrent
            await self._re_add_cross_seed_torrent(ts)

            if ts.state != State.FAILED:
                ts.readd_next_retry_at = None
                # The content now seeds from fuse: record the fuse mount
                # as the save location. Late-seed/cleanup guards read a
                # non-fuse save_path on DONE rows as "never moved" — a
                # stale SSD path here would demote healthy rows forever.
                # Use the blob-derived mount (fresh adoptions start as
                # kind="unknown" and would otherwise record unsorted).
                try:
                    ts.save_path = str(
                        self._target_mount_for_blob(gate_blob, self._target_mount_for(ts))
                    )
                except Exception:  # noqa: BLE001
                    pass
                self.transition(ts, State.DONE)
            return

        except Exception as e:  # noqa: BLE001
            if not is_retryable_client_error(e):
                # Fatal disk/permission errors (and unexpected
                # failures) fail loudly via the worker wrapper
                # instead of riding the retry clock.
                raise
            now_curr = dt.datetime.now(dt.UTC)
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

            if ts.readd_attempts < 2:
                # First failing attempt: short retry-gap park. The timer
                # persists in the DB across restarts; a sleep would lose
                # it (and stall the worker). Next tick retries.
                log.warning(
                    "WebUI unresponsive during re-add for %s (%s); "
                    "retrying next tick in %ds (attempt %d)",
                    ts.source_name[:50],
                    e,
                    retry_gap,
                    ts.readd_attempts,
                )
                self._park_readd(ts, f"re-inject failed: {e}", delay_s=retry_gap, level=None)
                return
            ts.readd_next_retry_at = now_curr + dt.timedelta(seconds=backoff_interval)
            log.warning(
                "WebUI still unresponsive for %s after %d attempts (%s); "
                "backing off for %ds (30m) until %s (elapsed=%ds, max=%ds)",
                ts.source_name[:50],
                ts.readd_attempts,
                e,
                backoff_interval,
                ts.readd_next_retry_at.isoformat(timespec="seconds"),
                int(elapsed_curr.total_seconds()),
                int(max_age.total_seconds()),
            )
            self._park_readd(
                ts,
                f"re-inject still failing after {ts.readd_attempts} attempts: {e}",
                delay_s=backoff_interval,
                level=None,
            )
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

        target_mount = self._target_mount_for_ts_blob(ts, blob)
        ok, detail = await self._ensure_fuse_entry(
            blob=blob,
            infohash=target_hash,
            target_mount=target_mount,
            label="cross-seed torrent",
        )

        if not ok:
            err_msg = f"fuse re-add rejected: {detail or 'client rejected torrent'}"
            log.error("re-add cross-seed torrent failed for %s: %s", ts.source_name, err_msg)
            # "already added" with no visible entry is registration lag
            # (same transient as _NOT_VISIBLE_DETAIL) — park/retry, never
            # terminal FAILED.
            if (
                detail == "Fails."
                or not detail
                or detail == _NOT_VISIBLE_DETAIL
                or "already" in (detail or "").lower()
            ):
                raise WebUIUnresponsiveError(err_msg)
            self.transition(ts, State.FAILED, error=err_msg)
            return

        # Success: remember the injected hash so the next cycle's early
        # return guards skip the duplicate add/verify, and persist the row.
        if _record_injected_hash(ts, target_hash):
            log.info(
                "cross-seed re-add recorded injected hash %s for %s",
                target_hash[:10],
                ts.source_name[:60],
            )
            try:
                self.store.upsert(ts, expected_version=ts.version)
            except Exception:
                pass
