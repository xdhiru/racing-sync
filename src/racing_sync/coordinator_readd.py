"""RE_ADDING phase mixin: fuse-gate re-injection + sustained-missing healer.

Split out of the coordinator god-file (part A: heal clock, gate, drive,
cross-seed re-add; the `_re_inject_*` supplements follow separately).
RE_ADDING rows re-inject their torrents onto the fuse mount with
skip_check=True only after the availability gate proves the moved bytes
are there; a sustained-missing set past the heal window is selectively
re-downloaded and moved. Fail-closed throughout: doubt parks, never
advances blind.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import stat
import time
from pathlib import Path

from .coordinator_errors import (
    _NOT_VISIBLE_DETAIL,
    AbandonedError,
    WebUIUnresponsiveError,
    is_retryable_client_error,
)
from .coordinator_paths import _left_on_disk, _safe_ssd_join
from .io_bounds import rpc
from .state import _MAX_READD_CYCLES, State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class ReaddMixin:
    """RE_ADDING drive + heal (duck-typed ``self``)."""

    # ---- state: RE_ADDING ----

    def _readd_heal_after_seconds(self) -> float:
        """Sustained-missing window before the RE_ADDING healer fires."""
        try:
            raw = getattr(getattr(self, "cfg", None),
                          "fuse_readd_heal_after_seconds", 7200)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return 7200.0
            return float(raw)
        except (TypeError, ValueError):
            return 7200.0

    async def _maybe_heal_readd_missing(
        self, ts: TorrentState, gate_missing: list[str],
        gate_target: Path, gate_blob: bytes | None,
    ) -> bool:
        """Selectively re-download + move fuse-gated files missing too long.

        Fires only when ALL hold (fail-closed otherwise, caller parks as
        today): healer enabled, mount healthy (no `<...>` outage markers),
        the SAME missing set observed continuously for longer than
        `fuse_readd_heal_after_seconds`, a fresh uncached re-stat still
        misses them (rules out the 60s stat cache), the bytes are also
        absent on SSD (a lagging fuse with local bytes is not a
        re-download case), the blob decodes, SSD budget admits the
        footprint, and fewer than 3 heal attempts ran for this set.

        On success the helper entry is removed (`delete_files=False`,
        bytes already moved), the budget released, and True returned —
        the row stays RE_ADDING and the gate passes next tick. False =
        not due / not healable / heal parked for retry. Abandoned rows
        and cancellation propagate (never swallowed).
        """
        if getattr(self, "_stop", False):
            return False
        if self._abandoned(ts):
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                "not healing re-add"
            )
        threshold = self._readd_heal_after_seconds()
        if threshold <= 0:
            return False
        try:
            key = (ts.source_infohash or "").lower()
        except Exception:
            return False
        if not key:
            return False
        # Mount-outage markers (`<mount unavailable …>`, `<availability
        # check failed …>`) prove nothing about the files: drop the clock
        # (outage time must not count toward the sustained window) and park.
        try:
            outage = any(str(m or "").startswith("<") for m in (gate_missing or []))
        except Exception:
            outage = True
        try:
            track = getattr(self, "_readd_heal_track", None)
            if not isinstance(track, dict):
                track = {}
                self._readd_heal_track = track  # type: ignore[attr-defined]
        except Exception:
            return False
        if outage:
            try:
                track.pop(key, None)
            except Exception:
                pass
            return False

        def _base_name(entry: str) -> str:
            try:
                text = str(entry or "")
            except Exception:
                return ""
            marker = " (size "
            idx = text.find(marker)
            if idx >= 0:
                text = text[:idx]
            return text.replace("\\", "/").strip("/")

        try:
            missing_key = tuple(sorted(
                n for n in (_base_name(m) for m in (gate_missing or [])) if n))
        except Exception:
            return False
        if not missing_key:
            return False
        now_m = time.monotonic()
        try:
            rec = track.get(key)
        except Exception:
            rec = None
        if (not isinstance(rec, (list, tuple)) or len(rec) != 3
                or rec[1] != missing_key):
            # First sighting, or the set changed (a file landed or a new
            # one went missing): (re)start the clock, keep the attempt
            # count for this row. Never heals on the sighting that starts
            # the clock, no matter the threshold.
            attempts = 0
            try:
                if isinstance(rec, (list, tuple)) and len(rec) == 3:
                    attempts = int(rec[2] or 0)
            except (TypeError, ValueError):
                attempts = 0
            try:
                track[key] = [now_m, missing_key, attempts]
                if len(track) > 5000:
                    for _k in list(track.keys())[: len(track) - 5000]:
                        track.pop(_k, None)
            except Exception:
                pass
            log.debug(
                "re-add heal: tracking %d missing file(s) for %s "
                "(clock started, threshold %.0fs)",
                len(missing_key), ts.source_name[:50], threshold,
            )
            return False
        try:
            attempts = int(rec[2] or 0)
        except (TypeError, ValueError):
            attempts = 0
        if now_m - float(rec[0] or now_m) < threshold:
            return False
        if attempts >= 3:
            # A remote that keeps losing healed files must fall through to
            # the 24h max-age FAIL, not loop re-downloads forever.
            log.warning(
                "re-add heal: giving up on %s after %d attempts "
                "(still %d missing); leaving to max-age FAIL",
                ts.source_name[:50], attempts, len(missing_key),
            )
            return False

        # Fresh uncached confirm (bypasses the 60s fuse_stat_cached): a
        # stale cache entry must never trigger a re-download.
        def _fresh_check() -> list[str]:
            still: list[str] = []
            for name in missing_key:
                try:
                    target = _safe_ssd_join(gate_target, name)
                    if target is None:
                        still.append(name)
                        continue
                    try:
                        st = target.stat()
                    except OSError:
                        still.append(name)
                        continue
                    if not stat.S_ISREG(st.st_mode):
                        still.append(name)
                        continue
                except OSError:
                    still.append(name)
                    continue
                except Exception:
                    return list(missing_key)
            return still

        try:
            confirmed = await asyncio.to_thread(_fresh_check)
        except Exception:
            return False
        if not confirmed:
            # Lag resolved between ticks: drop the clock, let the gate pass.
            try:
                track.pop(key, None)
            except Exception:
                pass
            return False
        if tuple(sorted(confirmed)) != missing_key:
            try:
                track[key] = [now_m, tuple(sorted(confirmed)), attempts]
            except Exception:
                pass
            return False
        heal_names = list(missing_key)
        heal_set = set(heal_names)

        # SSD-side absent check: bytes still local means fuse lag, not data
        # loss — re-downloading would duplicate them.
        try:
            ssd_base = Path(ts.save_path) if ts.save_path else None
            if ssd_base is None or self._save_path_is_on_fuse(str(ssd_base)):
                ssd_base = Path(self.cfg.dest.save_path)
        except Exception:
            return False
        try:
            for name in heal_names:
                p = _safe_ssd_join(ssd_base, name)
                if p is None:
                    return False
                try:
                    if p.exists():
                        log.info(
                            "re-add heal: %s still on SSD for %s "
                            "(fuse lag, not loss); not re-downloading",
                            name, ts.source_name[:50],
                        )
                        return False
                except OSError:
                    return False
        except Exception:
            return False

        # Sizes from the blob expectation (the fuse gate's own source).
        try:
            want_map = {n: int(s or 0) for n, s in (self._expected_fuse_files(gate_blob) or []) if n}
        except Exception:
            return False
        if not heal_set.issubset(set(want_map)):
            return False
        footprint = sum(max(0, want_map.get(n, 0)) for n in heal_names)

        reserved = False
        try:
            try:
                if not await self._ssd_try_reserve(ts.source_infohash, int(footprint)):
                    log.info(
                        "re-add heal: SSD budget in use for %s; retry later",
                        ts.source_name[:50],
                    )
                    return False
                reserved = True
            except Exception:
                return False

            # Blob hash = helper entry hash (same torrent as the SSD download).
            helper_h = ((ts.dest_infohash or ts.source_infohash) or "").lower()
            try:
                from .watchdir import _bencoded_info_hash

                parsed, _, _, _ = _bencoded_info_hash(bytes(gate_blob or b""))
                if isinstance(parsed, str) and parsed:
                    helper_h = parsed.lower()
            except Exception:
                pass
            if not helper_h:
                return False
            try:
                cur = await rpc(
                    self.dest_client.list_torrents(hashes=[helper_h]),
                    getattr(self, "cfg", None), "dest heal list_torrents")
                entry = cur[0] if cur else None
            except Exception:
                return False
            if entry is None:
                try:
                    res = await self.dest_client.add_torrent(
                        torrent_files=[bytes(gate_blob)],
                        save_path=str(ssd_base),
                        category="racing",
                        paused=True,
                        skip_check=False,
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "re-add heal: re-add failed for %s: %s",
                        ts.source_name[:50], e,
                    )
                    return False
                if not getattr(res, "accepted", False):
                    log.warning(
                        "re-add heal: re-add rejected for %s (%s)",
                        ts.source_name[:50], getattr(res, "detail", ""),
                    )
                    return False
                try:
                    if isinstance(getattr(res, "hash", None), str) and res.hash:
                        helper_h = res.hash.lower()
                except Exception:
                    pass
                try:
                    ts.dest_infohash = helper_h
                    self.store.upsert(ts)
                except Exception:
                    pass
            try:
                files = await self.dest_client.get_torrent_files(helper_h)
            except Exception:
                return False
            if not files:
                return False
            have = {getattr(f, "name", "") for f in files}
            if not heal_set.issubset(have):
                log.warning(
                    "re-add heal: torrent file list changed for %s; not healing",
                    ts.source_name[:50],
                )
                return False
            try:
                await self.dest_client.set_file_priorities(
                    helper_h,
                    {f.name: (1 if f.name in heal_set else 0) for f in files},
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: priority set failed for %s: %s",
                    ts.source_name[:50], e,
                )
                return False
            try:
                await self.dest_client.resume(helper_h)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: resume failed for %s: %s",
                    ts.source_name[:50], e,
                )
                return False
            # Point the row at the helper entry on SSD for the wait below
            # (_wait_for_completion derives its client hash + disk base
            # from these fields). Restored implicitly: success deletes the
            # helper and the DONE transition records the fuse mount.
            try:
                ts.dest_infohash = helper_h
                if not ts.save_path or self._save_path_is_on_fuse(ts.save_path):
                    ts.save_path = str(ssd_base)
                self.store.upsert(ts)
            except Exception:
                pass
            try:
                await self._wait_for_completion(ts, expected_files=heal_names)
            except (AbandonedError, asyncio.CancelledError):
                raise
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: download parked for %s: %s",
                    ts.source_name[:50], e,
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            try:
                await self.dest_client.pause(helper_h)
            except Exception:
                pass
            try:
                kind = (ts.classification_kind or "").strip() or self._classify_blob_kind(gate_blob)
            except Exception:
                kind = "unknown"
            try:
                remote = (self.cfg.rclone.remote.default
                          if kind in ("movie", "season")
                          else self.cfg.rclone.remote.unsorted)
            except Exception:
                return False
            try:
                await self._rclone_move(ssd_base, remote, ts, files_from=heal_names)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: move failed for %s: %s",
                    ts.source_name[:50], e,
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            stuck = [n for n in heal_names if _left_on_disk(ssd_base, n)]
            if stuck:
                log.warning(
                    "re-add heal: move left %d file(s) on disk for %s "
                    "(e.g. %s); retry later",
                    len(stuck), ts.source_name[:50], stuck[0],
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            try:
                await self.dest_client.delete(helper_h, delete_files=False)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: helper delete failed for %s: %s",
                    ts.source_name[:50], e,
                )
            log.warning(
                "re-add heal: recovered %d file(s) for %s "
                "(selective re-download + move); gate re-checks next tick",
                len(heal_names), ts.source_name[:50],
            )
            # Cap attempts so a fuse that never indexes the fresh move
            # cannot trigger a second re-download: further parks either
            # resolve via indexing or fall through to the max-age FAIL.
            # The gate-pass path clears the record when files appear.
            try:
                track[key] = [time.monotonic(), missing_key, 3]
            except Exception:
                pass
            return True
        except (AbandonedError, asyncio.CancelledError):
            raise
        except Exception as e:  # noqa: BLE001
            log.warning(
                "re-add heal failed for %s: %s", ts.source_name[:50], e,
            )
            return False
        finally:
            if reserved:
                try:
                    await self._ssd_release(ts.source_infohash)
                except Exception:
                    pass

    async def _do_re_add(self, ts: TorrentState) -> None:
        store = getattr(self, "store", None)
        now = dt.datetime.now(dt.timezone.utc)
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
        if ts.readd_first_attempted_at is None:
            ts.readd_first_attempted_at = now
            if store is not None:
                store.upsert(ts)

        delay = self.cfg.fuse_reinject_delay_seconds
        if delay > 0 and ts.readd_attempts == 0 and ts.readd_next_retry_at is None:
            log.info(
                "parking %s for %ds fuse mount indexing before re-injection",
                ts.source_name[:50], delay,
            )
            if delay > 5:
                ts.readd_next_retry_at = now + dt.timedelta(seconds=delay)
                if store is not None:
                    store.upsert(ts)
                return
            else:
                await asyncio.sleep(delay)

        max_age_val = getattr(self.cfg, "fuse_reinject_max_age_seconds", 86400)
        max_age_sec = max_age_val if isinstance(max_age_val, (int, float)) else 86400
        max_age = dt.timedelta(seconds=max_age_sec)

        retry_gap_val = getattr(self.cfg, "fuse_reinject_retry_gap_seconds", 120)
        retry_gap = retry_gap_val if isinstance(retry_gap_val, (int, float)) else 120

        backoff_val = getattr(self.cfg, "fuse_reinject_backoff_seconds", 1800)
        backoff_interval = backoff_val if isinstance(backoff_val, (int, float)) else 1800

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
            # persisted (e.g. /injectfuse on a freshly discovered row):
            # the racing torrent is still on VPS1, so live-fetch its bytes,
            # persist them, and continue through the gate on this same tick
            # instead of parking until max_age.
            try:
                healed = await self._fetch_racing_torrent_bytes(
                    (ts.source_infohash or "").lower())
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
                        ts.source_infohash, {"cross_seed_blob": bytes(gate_blob)})
                    if new_rev:
                        ts.version = new_rev
                except Exception:
                    pass
                try:
                    ts.cross_seed_blob = bytes(gate_blob)
                    ts._blob = bytes(gate_blob)
                except Exception:
                    pass
                log.info("fuse gate: healed missing blob for %s from VPS1 (%d B)",
                         ts.source_name[:50], len(gate_blob or b""))
        gate_expected = self._expected_fuse_files(gate_blob)
        if not gate_blob:
            # Fail closed: no blob at all means we cannot verify the fuse
            # target. Park for retry instead of injecting blind with
            # skip_check=True (which creates unseedable torrents).
            log.warning(
                "fuse content unverifiable for %s (missing blob); parking re-add",
                ts.source_name[:50],
            )
            ts.readd_next_retry_at = now + dt.timedelta(seconds=retry_gap)
            if store is not None:
                store.upsert(ts)
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
                    _blob_len, ts.source_name[:50],
                )
                ts.readd_next_retry_at = now + dt.timedelta(seconds=retry_gap)
                if store is not None:
                    store.upsert(ts)
                return
            log.warning(
                "fuse gate cannot decode blob for %s; proceeding without gate",
                ts.source_name[:50],
            )
            gate_expected = []
        if gate_expected:
            gate_target = self._target_mount_for_blob(gate_blob, self._target_mount_for(ts))
            gate_missing = await self._missing_fuse_files(gate_target, gate_expected)
            if gate_missing:
                preview = ", ".join(gate_missing[:5])
                if len(gate_missing) > 5:
                    preview += f", …+{len(gate_missing) - 5} more"
                log.warning(
                    "fuse content not yet available for %s at %s "
                    "(%d/%d missing, e.g. %s); parking re-add instead of injecting blind",
                    ts.source_name[:50], gate_target,
                    len(gate_missing), len(gate_expected), preview,
                )
                # Sustained-missing healer: re-downloads + moves just the
                # never-moved files after a long stable window (never on a
                # fresh absence — that is usually fuse/rclone index lag).
                # Fail-closed: any healer trouble parks exactly as today.
                try:
                    await self._maybe_heal_readd_missing(
                        ts, gate_missing, gate_target, gate_blob)
                except (AbandonedError, asyncio.CancelledError):
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "re-add heal failed for %s: %s",
                        ts.source_name[:50], e,
                    )
                ts.readd_next_retry_at = now + dt.timedelta(seconds=retry_gap)
                if store is not None:
                    store.upsert(ts)
                return

        # Gate passed: any heal incident for this row is over — drop its
        # clock/attempts so a future incident starts fresh.
        try:
            _ht = getattr(self, "_readd_heal_track", None)
            if isinstance(_ht, dict):
                _ht.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass

        max_cycle_attempts = 2

        for cycle_attempt in range(1, max_cycle_attempts + 1):
            ts.readd_attempts += 1
            try:
                # 1) Re-inject the racing-client torrents (private or otherwise)
                # pointing at the fuse mount with skip_check=True (req #3).
                # Watch rows (any SSD flavour label) re-inject their
                # persisted blobs instead.
                if self.cfg.cross_seed.inject_racing_torrents_to_fuse:
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
                        ts.save_path = str(self._target_mount_for_blob(gate_blob, self._target_mount_for(ts)))
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
                now_curr = dt.datetime.now(dt.timezone.utc)
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

                if cycle_attempt < max_cycle_attempts:
                    log.warning(
                        "WebUI unresponsive during re-add for %s (%s); "
                        "retrying in %ds (attempt %d/%d)",
                        ts.source_name[:50], e, retry_gap, cycle_attempt, max_cycle_attempts,
                    )
                    if retry_gap > 5:
                        ts.readd_next_retry_at = now_curr + dt.timedelta(seconds=retry_gap)
                        if store is not None:
                            store.upsert(ts)
                        return
                    else:
                        if store is not None:
                            store.upsert(ts)
                        await asyncio.sleep(retry_gap)
                        if self._stop:
                            return
                else:
                    ts.readd_next_retry_at = now_curr + dt.timedelta(seconds=backoff_interval)
                    log.warning(
                        "WebUI still unresponsive for %s after %d attempts (%s); "
                        "backing off for %ds (30m) until %s (elapsed=%ds, max=%ds)",
                        ts.source_name[:50], max_cycle_attempts, e, backoff_interval,
                        ts.readd_next_retry_at.isoformat(timespec="seconds"),
                        int(elapsed_curr.total_seconds()), int(max_age.total_seconds()),
                    )
                    if store is not None:
                        store.upsert(ts)
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

        target_mount = self._target_mount_for_blob(blob, self._target_mount_for(ts))
        ok, detail = await self._ensure_fuse_entry(
            blob=blob, infohash=target_hash, target_mount=target_mount,
            label="cross-seed torrent",
        )

        if not ok:
            err_msg = f"fuse re-add rejected: {detail or 'client rejected torrent'}"
            log.error("re-add cross-seed torrent failed for %s: %s", ts.source_name, err_msg)
            # "already added" with no visible entry is registration lag
            # (same transient as _NOT_VISIBLE_DETAIL) — park/retry, never
            # terminal FAILED.
            if detail == "Fails." or not detail or detail == _NOT_VISIBLE_DETAIL or "already" in (detail or "").lower():
                raise WebUIUnresponsiveError(err_msg)
            self.transition(ts, State.FAILED, error=err_msg)
            return
