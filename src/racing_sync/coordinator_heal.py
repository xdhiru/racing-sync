"""RE_ADDING healer: sustained-missing selective re-download + move.

Split out of coordinator_readd: the heal clock
(``_readd_heal_after_seconds``) and the heal drive
(``_maybe_heal_readd_missing``). A sustained-missing set past the heal
window is selectively re-downloaded and moved; fail-closed throughout.
Calls back into the coordinator through ``self`` only.
"""

from __future__ import annotations

import asyncio
import logging
import stat
import time
from pathlib import Path

from .coordinator_errors import AbandonedError
from .coordinator_paths import _safe_ssd_join, left_on_disk_many
from .io_bounds import rpc
from .recovery import _save_path_on_ssd, _ssd_roots_known
from .state import TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class ReaddHealMixin:
    """RE_ADDING sustained-missing healer (duck-typed ``self``)."""

    def _readd_heal_after_seconds(self) -> float:
        """Sustained-missing window before the RE_ADDING healer fires."""
        try:
            raw = self.rt.seed.readd_heal_after_seconds
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return 7200.0
            return float(raw)
        except (TypeError, ValueError):
            return 7200.0

    async def _maybe_heal_readd_missing(
        self,
        ts: TorrentState,
        gate_missing: list[str],
        gate_target: Path,
        gate_blob: bytes | None,
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
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; not healing re-add"
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
            missing_key = tuple(
                sorted(n for n in (_base_name(m) for m in (gate_missing or [])) if n)
            )
        except Exception:
            return False
        if not missing_key:
            return False
        now_m = time.monotonic()
        try:
            rec = track.get(key)
        except Exception:
            rec = None
        if not isinstance(rec, (list, tuple)) or len(rec) != 3 or rec[1] != missing_key:
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
                "re-add heal: tracking %d missing file(s) for %s (clock started, threshold %.0fs)",
                len(missing_key),
                ts.source_name[:50],
                threshold,
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
                ts.source_name[:50],
                attempts,
                len(missing_key),
            )
            return False

        # Sizes from the blob expectation (the fuse gate's own source):
        # authoritative per-file sizes for the fresh check below.
        try:
            want_map = {
                n: int(s or 0) for n, s in (self._expected_fuse_files(gate_blob) or []) if n
            }
        except Exception:
            return False
        if not set(missing_key).issubset(set(want_map)):
            return False

        # Fresh uncached confirm (bypasses the 60s fuse_stat_cached): a
        # stale cache entry must never trigger a re-download. Compares
        # sizes too: a truncated-but-regular file is still missing.
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
                    try:
                        want = int(want_map.get(name, 0) or 0)
                    except (TypeError, ValueError):
                        want = 0
                    if want and int(st.st_size) != want:
                        still.append(f"{name} (size {st.st_size}!={want})")
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
        try:
            confirmed_key = tuple(sorted(n for n in (_base_name(c) for c in confirmed) if n))
        except Exception:
            return False
        if confirmed_key != missing_key:
            try:
                track[key] = [now_m, confirmed_key, attempts]
            except Exception:
                pass
            return False
        heal_names = list(missing_key)
        heal_set = set(heal_names)

        # SSD-side absent check: bytes still local means fuse lag, not data
        # loss — re-downloading would duplicate them. Sync exists() loop,
        # offloaded so slow SSD stats never stall the event loop.
        try:
            ssd_base = Path(ts.save_path) if ts.save_path else None
            if ssd_base is None or self._save_path_is_on_fuse(str(ssd_base)):
                ssd_base = Path(self.rt.storage.save_path)
        except Exception:
            return False

        def _ssd_has_any() -> str | None:
            """First heal name still on SSD; "" when unverifiable; None when clear.

            Size-aware on purpose: a truncated/zero-byte same-name leftover
            (interrupted earlier move) would otherwise suppress healing
            forever while the full file exists remotely. Only an exact-size
            match counts as "still local"; a size mismatch falls through to
            the next name. Unknown expected size (0) keeps the plain
            presence rule.
            """
            for name in heal_names:
                try:
                    p = _safe_ssd_join(ssd_base, name)
                except Exception:
                    return ""
                if p is None:
                    return ""
                try:
                    st = p.stat()
                except (FileNotFoundError, NotADirectoryError):
                    continue  # absent (as Path.exists() read it): keep looking
                except OSError:
                    return ""
                try:
                    want = int(want_map.get(name, 0) or 0)
                except (TypeError, ValueError):
                    want = 0
                if want <= 0 or int(st.st_size) == want:
                    return name
            return None

        try:
            _ssd_hit = await asyncio.to_thread(_ssd_has_any)
        except Exception:
            return False
        if _ssd_hit is not None:
            if _ssd_hit:
                log.info(
                    "re-add heal: %s still on SSD for %s (fuse lag, not loss); not re-downloading",
                    _ssd_hit,
                    ts.source_name[:50],
                )
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
            # Snapshot the pre-heal dest pointer: the helper overwrite below
            # is transient (retry continuity while it exists); success deletes
            # the helper, so the pointer is restored instead of dangling.
            try:
                orig_dest_infohash = ts.dest_infohash or ""
            except Exception:
                orig_dest_infohash = ""
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
                    getattr(self, "cfg", None),
                    "dest heal list_torrents",
                )
                entry = cur[0] if cur else None
            except Exception:
                return False
            if entry is not None:
                # Placement gate: never resume a helper entry whose bytes
                # are not under an SSD/dest root (foreign/stale mount, or a
                # fuse path). Unknown roots (test doubles) stay fail-open.
                try:
                    _entry_sp = str(getattr(entry, "save_path", "") or "")
                    _entry_foreign = bool(
                        _entry_sp
                        and _ssd_roots_known(self.cfg)
                        and not _save_path_on_ssd(self.cfg, _entry_sp)
                    )
                except Exception:
                    _entry_foreign = False
                if _entry_foreign:
                    log.warning(
                        "re-add heal: existing entry %s for %s sits at "
                        "foreign path %s (not under an SSD/dest root); "
                        "not resuming it as the heal helper",
                        helper_h[:10],
                        ts.source_name[:50],
                        _entry_sp,
                    )
                    return False
            if entry is None:
                try:
                    res = await rpc(
                        self.dest_client.add_torrent(
                            torrent_files=[bytes(gate_blob)],
                            save_path=str(ssd_base),
                            category="racing",
                            paused=True,
                            skip_check=False,
                        ),
                        getattr(self, "cfg", None),
                        "dest heal add_torrent",
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "re-add heal: re-add failed for %s: %s",
                        ts.source_name[:50],
                        e,
                    )
                    return False
                if not getattr(res, "accepted", False):
                    log.warning(
                        "re-add heal: re-add rejected for %s (%s)",
                        ts.source_name[:50],
                        getattr(res, "detail", ""),
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
                files = await rpc(
                    self.dest_client.get_torrent_files(helper_h),
                    getattr(self, "cfg", None),
                    "dest heal get_torrent_files",
                )
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
                await rpc(
                    self.dest_client.set_file_priorities(
                        helper_h,
                        {f.name: (1 if f.name in heal_set else 0) for f in files},
                    ),
                    getattr(self, "cfg", None),
                    "dest heal set_file_priorities",
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: priority set failed for %s: %s",
                    ts.source_name[:50],
                    e,
                )
                return False
            try:
                await rpc(
                    self.dest_client.resume(helper_h),
                    getattr(self, "cfg", None),
                    "dest heal resume",
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: resume failed for %s: %s",
                    ts.source_name[:50],
                    e,
                )
                return False
            # Point the row at the helper entry on SSD for the wait below
            # (_wait_for_completion derives its client hash + disk base
            # from these fields). The pointer is restored explicitly on
            # success (helper deleted); the DONE transition records the
            # fuse mount.
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
                    ts.source_name[:50],
                    e,
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            try:
                await rpc(
                    self.dest_client.pause(helper_h), getattr(self, "cfg", None), "dest heal pause"
                )
            except Exception:
                pass
            try:
                kind = (ts.classification_kind or "").strip() or self._classify_blob_kind(gate_blob)
            except Exception:
                kind = "unknown"
            try:
                remote = (
                    self.rt.transfer.remote_default
                    if kind in ("movie", "season")
                    else self.rt.transfer.remote_unsorted
                )
            except Exception:
                return False
            try:
                await self._rclone_move(ssd_base, remote, ts, files_from=heal_names, heal=True)
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: move failed for %s: %s",
                    ts.source_name[:50],
                    e,
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            stuck = await asyncio.to_thread(left_on_disk_many, ssd_base, heal_names)
            if stuck:
                log.warning(
                    "re-add heal: move left %d file(s) on disk for %s (e.g. %s); retry later",
                    len(stuck),
                    ts.source_name[:50],
                    stuck[0],
                )
                try:
                    track[key] = [now_m, missing_key, attempts + 1]
                except Exception:
                    pass
                return False
            try:
                await rpc(
                    self.dest_client.delete(helper_h, delete_files=False),
                    getattr(self, "cfg", None),
                    "dest heal delete",
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "re-add heal: helper delete failed for %s: %s",
                    ts.source_name[:50],
                    e,
                )
            # Restore the pre-heal dest pointer: the helper entry is gone,
            # so its hash would dangle. Next tick re-derives from the blob.
            # (Failure paths above keep the helper pointer deliberately —
            # the entry still exists for retry continuity.)
            try:
                if ts.dest_infohash != orig_dest_infohash:
                    ts.dest_infohash = orig_dest_infohash
                    self.store.upsert(ts)
            except Exception:
                pass
            log.warning(
                "re-add heal: recovered %d file(s) for %s "
                "(selective re-download + move); gate re-checks next tick",
                len(heal_names),
                ts.source_name[:50],
            )
            # Cap attempts so a fuse that never indexes the fresh move
            # cannot trigger a second re-download: further parks either
            # resolve via indexing or fall through to the max-age FAIL.
            # The gate-pass path clears the record when files appear.
            # Deliberately NOT popped here: clearing would restart the
            # clock and allow a second re-download while fuse indexes.
            try:
                track[key] = [time.monotonic(), missing_key, 3]
            except Exception:
                pass
            return True
        except (AbandonedError, asyncio.CancelledError):
            raise
        except Exception as e:  # noqa: BLE001
            log.warning(
                "re-add heal failed for %s: %s",
                ts.source_name[:50],
                e,
            )
            return False
        finally:
            if reserved:
                try:
                    await self._ssd_release(ts.source_infohash)
                except Exception:
                    pass
