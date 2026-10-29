"""MOVING phase mixin: verified moves, folder wipe, rclone execution.

Split out of the coordinator god-file. Only client-verified-complete
bytes move (progress authoritative, disk size second gate); leftovers,
uncertain files, and missing bytes park instead of wiping. Single files
move bare, seasons per verified file list, mixed per re-batched episode
group.

Patch-target compatibility: tests patch
``racing_sync.coordinator.{wipe_local_tree,classify}`` — those two
call sites resolve through the coordinator namespace via a deferred
import (same reason as coordinator_ssd's lazy lookup; note that a
module-level ``__getattr__`` would NOT work — implicit global lookups
inside functions never consult it). Everything else is imported
directly; nothing here is itself a patch target.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import stat
import time
from pathlib import Path

from .batcher import files_from_names, make_batches
from .clients.abstract import TorrentFile
from .coordinator_errors import (
    AbandonedError,
    RcloneTransientError,
    TorrentCheckingError,
    is_retryable_client_error,
    is_transient_rclone_stderr,
)
from .coordinator_paths import _safe_ssd_join, left_on_disk_many
from .io_bounds import rpc
from .rclone_ops import move_local_to_remote
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Unreadable-mount parks before a MOVING row fails for operator
# attention (errno-None OSErrors never heal by retrying).
_MOVE_ERRNO_FAIL_PARKS = 10


class MoveMixin:
    """Verified-move execution (duck-typed ``self``)."""

    # ---- state: MOVING ----

    def _park_moving(self, ts: TorrentState, reason: str, quiet: bool = False) -> None:
        """Stay in MOVING for retry next tick, recording WHY (stall diagnosis).

        Every _do_moving early return funnels here instead of a bare
        upsert: the park reason lands in last_error (cleared by transition()
        on advance, visible via API/DB meanwhile) and consecutive parks for
        the same row escalate from WARNING to ERROR so a gate that never
        passes (unverifiable pause, 0-transfer rclone, short bytes) can't
        idle silently as plain "MOVING" forever — but the ERROR repeats at
        most every 20th park (first at 5) so an expected settle wait (slow
        per-file progress flush) doesn't scream every 30s for hours.
        `quiet` skips even that (expected waits like an in-flight hash
        check): debug-level only, counting/upsert identical.
        Raises AbandonedError when
        the row was forgotten mid-flight (callers unwind quietly instead of
        resurrecting it via the park upsert).
        """
        if self._abandoned(ts):
            raise AbandonedError(
                f"row gone (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                "not parking in MOVING"
            )
        key = (ts.source_infohash or "").lower()
        try:
            parks = getattr(self, "_moving_parks", None)
            if not isinstance(parks, dict):
                parks = {}
                self._moving_parks = parks
            n = int(parks.get(key, 0) or 0) + 1
            parks[key] = n
            if len(parks) > 5000:
                for _k in list(parks.keys())[: len(parks) - 5000]:
                    parks.pop(_k, None)
        except Exception:
            n = 1
        if not quiet:
            # Any other park reason means the row did something besides
            # checking — reset the check-wait clock (it measures
            # *continuous* checking only).
            try:
                _csd = getattr(self, "_checking_track", None)
                if isinstance(_csd, dict):
                    _csd.pop(key, None)
            except Exception:
                pass
        try:
            ts.last_error = f"moving parked ({n}x): {reason}"[:500]
        except Exception:
            pass
        # Narrow park write: a concurrent transition (recovery, API retry)
        # must survive the park — a whole-row upsert here would regress it
        # (e.g. MOVING->RE_ADDING back to MOVING). The revision sync keeps
        # later transitions on this object valid.
        try:
            _up = getattr(self.store, "update_columns", None)
            if callable(_up):
                _nv = _up((ts.source_infohash or ""),
                          {"last_error": ts.last_error})
                if isinstance(_nv, int) and _nv > 0:
                    try:
                        ts.version = _nv
                    except Exception:
                        pass
                elif self._abandoned(ts):
                    raise AbandonedError(
                        f"row gone for {(ts.source_infohash or '')[:10]}; "
                        "not parking in MOVING"
                    )
                else:
                    self.store.upsert(ts)
            else:
                self.store.upsert(ts)
        except AbandonedError:
            raise
        except Exception:  # noqa: BLE001
            pass
        if quiet:
            log.debug(
                "MOVING parked (quiet) for %s: %s (parked %dx)",
                ts.source_name[:60], reason, n,
            )
        elif n >= 5 and (n == 5 or (n - 5) % 20 == 0):
            log.error(
                "MOVING stalled for %s: %s (parked %dx, still retrying)",
                ts.source_name[:60], reason, n,
            )
        elif n >= 5:
            log.debug(
                "MOVING still parked for %s: %s (parked %dx)",
                ts.source_name[:60], reason, n,
            )
        else:
            log.warning(
                "staying in MOVING for %s: %s",
                ts.source_name[:60], reason,
            )

    def _single_on_fuse(self, files: list, single_file: str, ts) -> bool:
        """True iff an already-remote single file needs no SSD download/move.

        Size-verified against the kind-appropriate fuse mount; anything
        unstatable answers False (safe direction: download it).
        """
        safe = _safe_ssd_join(Path("."), single_file)
        if safe is None:
            return False
        norm = str(safe)
        if not norm or norm == ".":
            return False
        match = next(
            (f for f in files
             if str(_safe_ssd_join(Path("."), getattr(f, "name", "") or "") or "") == norm),
            None,
        )
        if match is None:
            return False
        try:
            mount = self._target_mount_for(ts)
        except Exception:
            return False
        try:
            target = _safe_ssd_join(mount, norm)
            if target is None:
                return False
            return target.stat().st_size == (match.size_bytes or 0)
        except OSError:
            return False

    def _release_recheck_holder(self, ts) -> None:
        """Clear the global settle-recheck hold when it is this row.

        A holder that moves on (moved, failed, resumed to DOWNLOADING,
        forgotten) must not serialize all other singles behind a stale
        30-minute age-out.
        """
        try:
            _ri = getattr(self, "_recheck_in_flight", None)
            if (isinstance(_ri, (list, tuple)) and len(_ri) == 2
                    and _ri[0] == (ts.source_infohash or "").lower()):
                self._recheck_in_flight = None
        except Exception:
            pass

    def _heartbeat_recheck_holder(self, ts) -> None:
        """Refresh our global settle-recheck hold while our check runs.

        A live holder must not be stolen at age-out by a waiter: slow
        storage legitimately checks for tens of minutes. Forgotten rows
        run no worker, so they can't heartbeat — the age-out still
        reaps them. Bounded above by the shared check-wait clock, which
        fails a never-finishing check loudly.
        """
        try:
            _key = (ts.source_infohash or "").lower()
            _ri = getattr(self, "_recheck_in_flight", None)
            if (isinstance(_ri, (list, tuple)) and len(_ri) == 2
                    and _key and _ri[0] == _key):
                self._recheck_in_flight = [_key, time.monotonic()]
        except Exception:
            pass

    def _register_checking_park(self, ts: TorrentState,
                                  progress: float = 0.0) -> str | None:
        """Record a checking park against the check-progress watchdog.

        A healthy hash check climbs steadily; a wedged one sits still —
        elapsed time alone can't tell them apart on slow storage, so this
        tracks the client-reported check fraction instead:
        - progress advanced since last sighting → window restarts;
        - progress went backwards → the check restarted → clock restarts;
        - no advance for `download_check_stall_seconds` (1 hour default,
          0 disables) → terminal error (caller fails the row);
        - absolute `download_max_check_wait_seconds` backstops ultra-slow
          crawls that technically advance.
        Returns a terminal error string on trip, else None (recording the
        sighting). Any non-checking park clears the record in _park_moving;
        settle/fail paths pop it explicitly — a stale record can never nuke
        a later re-download of the same hash. Shared by the pre-read and
        pause-gate branches (both observe the same client state).
        """
        try:
            _prog = float(progress or 0.0)
        except (TypeError, ValueError):
            _prog = 0.0
        try:
            _stall = float(getattr(
                getattr(self.cfg, "general", None),
                "download_check_stall_seconds", 1800) or 0)
        except (TypeError, ValueError):
            _stall = 1800.0
        try:
            _abscap = float(getattr(
                getattr(self.cfg, "general", None),
                "download_max_check_wait_seconds", 43200) or 0)
        except (TypeError, ValueError):
            _abscap = 43200.0
        _ckey = (ts.source_infohash or "").lower()
        _now_m = time.monotonic()
        _rec = None
        try:
            _csd = getattr(self, "_checking_track", None)
            if not isinstance(_csd, dict):
                _csd = {}
                self._checking_track = _csd  # type: ignore[attr-defined]
            _raw = _csd.get(_ckey)
            if (isinstance(_raw, (list, tuple)) and len(_raw) == 3
                    and all(isinstance(v, (int, float))
                            and not isinstance(v, bool) for v in _raw)):
                _rec = [float(_raw[0]), float(_raw[1]), float(_raw[2])]
        except Exception:
            _csd, _rec = {}, None
        if _rec is None:
            try:
                _csd[_ckey] = [_now_m, _prog, _now_m]
            except Exception:
                pass
            return None
        _first, _last_p, _last_a = _rec
        if _prog > _last_p + 1e-9:
            _rec = [_first, _prog, _now_m]
        elif _last_p - _prog > 1e-9:
            # Went backwards: the check restarted from zero — fresh clock.
            _rec = [_now_m, _prog, _now_m]
        if _stall > 0 and _now_m - _rec[2] > _stall:
            try:
                if isinstance(_csd, dict):
                    _csd.pop(_ckey, None)
            except Exception:
                pass
            return (f"client hash check made no progress for {int(_stall)}s; "
                    f"failing for operator attention")
        if _abscap > 0 and _now_m - _rec[0] > _abscap:
            try:
                if isinstance(_csd, dict):
                    _csd.pop(_ckey, None)
            except Exception:
                pass
            return (f"client hash check exceeded {int(_abscap)}s overall; "
                    f"failing for operator attention")
        try:
            _csd[_ckey] = _rec
            if isinstance(_csd, dict) and len(_csd) > 5000:
                for _k in list(_csd.keys())[: len(_csd) - 5000]:
                    _csd.pop(_k, None)
        except Exception:
            pass
        return None

    async def _do_moving(self, ts: TorrentState) -> None:
        h = ts.dest_infohash or ts.source_infohash
        try:
            cls_files = await self.dest_client.get_torrent_files(h)
        except Exception as e:  # noqa: BLE001
            if not is_retryable_client_error(e):
                # Fatal disk/permission errors must fail loudly, never
                # park a fully-downloaded row on an unhealing error.
                raise
            if isinstance(e, OSError) and getattr(e, "errno", None) is None:
                # Unreadable-mount shape (broken mount, wedged FS): retrying
                # never heals it — fail after a bounded run of parks
                # instead of ERROR-logging forever. Bytes stay on SSD
                # (delete_files=False).
                try:
                    _emp = getattr(self, "_move_errno_parks", None)
                    if not isinstance(_emp, dict):
                        _emp = {}
                        self._move_errno_parks = _emp  # type: ignore[attr-defined]
                    _n = int(_emp.get(h.lower(), 0) or 0) + 1
                    _emp[h.lower()] = _n
                    if len(_emp) > 5000:
                        for _k in list(_emp.keys())[: len(_emp) - 5000]:
                            _emp.pop(_k, None)
                except Exception:
                    _n = 0
                if _n >= _MOVE_ERRNO_FAIL_PARKS:
                    try:
                        _emp.pop(h.lower(), None)
                    except Exception:
                        pass
                    await self._fail_downloading_row(
                        ts, h,
                        f"repeated unreadable-mount errors ({e}); failing "
                        f"for operator attention",
                        delete_files=False)
                    return
            # Every other phase parks on transient client trouble; a single
            # qB timeout here must not fail a fully-downloaded row.
            self._park_moving(
                ts,
                f"could not list files for {h[:10]} before move "
                f"({e}); will retry on next tick",
            )
            return
        try:
            _emp_ok = getattr(self, "_move_errno_parks", None)
            if isinstance(_emp_ok, dict):
                _emp_ok.pop(h.lower(), None)
        except Exception:
            pass
        # Patched in tests as racing_sync.coordinator.classify: resolve
        # through the coordinator namespace (ssd-precedent lazy lookup).
        from . import coordinator as _coordinator

        cls = _coordinator.classify(cls_files, self.cfg)
        # Pin the QUEUED-time classification: only fill when unknown. A fresh
        # file list (tracker sidecar added, metadata completed) can flip
        # episode<->mixed and would otherwise reroute the remote and branch
        # mid-flight. Layout below still follows the fresh list; routing
        # follows the pinned kind.
        pinned_kind = ts.classification_kind or "unknown"
        if (not pinned_kind or pinned_kind == "unknown") and cls.kind:
            ts.classification_kind = cls.kind
            pinned_kind = cls.kind
            try:
                self.store.upsert(ts)
            except Exception:  # noqa: BLE001
                pass
        elif cls.kind and cls.kind != pinned_kind:
            log.warning(
                "classification flipped %s -> %s for %s after queue; keeping pinned %s for routing",
                pinned_kind, cls.kind, ts.source_name, pinned_kind,
            )
        # Branch the move on the PINNED kind (falling back to the fresh one
        # when nothing was ever pinned): batches were downloaded and routed
        # under it, so a mid-flight flip (tracker sidecar added, metadata
        # completed) must not reroute the move or rewrite batch cursors via
        # the mixed branch. Fresh `cls` still supplies the file layout
        # (episodes/single_file/sizes) below.
        branch_kind = pinned_kind if pinned_kind not in ("", "unknown") else cls.kind

        # 0b. Never disturb an in-progress hash check. Pausing aborts it and
        # the resume below restarts it, which together livelock the check
        # forever on slow storage (observed: alternating checking/quiet
        # parks every tick, check never completing). A checking torrent
        # isn't writing, so there is nothing to pause — park quietly and
        # let it finish. One cheap state-only list (no files/trackers fan-out).
        # Bounded by the shared check-wait clock (12h default): a check that
        # never finishes fails loudly instead of waiting forever.
        try:
            _pre = await rpc(
                self.dest_client.list_torrents(hashes=[h]),
                getattr(self, "cfg", None), "dest pre-move state")
            _pre_state = _pre[0].state if _pre else ""
        except Exception:
            _pre = None
            _pre_state = ""
        if _pre is not None and not _pre:
            # Empty list (not an error): the entry may be registering —
            # but proceeding to pause a ghost entry is wrong. Park for
            # the next tick instead of pausing nothing.
            self._park_moving(
                ts,
                "dest entry not listed before move; will retry on next tick",
            )
            return
        try:
            _pre_checking = str(_pre_state or "").lower().startswith("check")
        except Exception:
            _pre_checking = False
        if _pre_checking:
            try:
                _pre_prog = float(getattr(_pre[0], "progress", 0.0) or 0.0)
            except (TypeError, ValueError, AttributeError):
                _pre_prog = 0.0
            _terminal = self._register_checking_park(ts, _pre_prog)
            if _terminal is not None:
                await self._fail_downloading_row(
                    ts, h, f"{_terminal} ({_pre_state})",
                    # MOVING bytes are complete on SSD: drop only the
                    # client entry, never the data.
                    delete_files=False)
                return
            self._heartbeat_recheck_holder(ts)
            self._park_moving(
                ts,
                f"client hash check running ({_pre_state}); "
                f"waiting it out without touching it",
                quiet=True,
            )
            return

        # 1. Pause torrent on VPS2 client BEFORE move begins to stop active seeding from SSD
        log.info("pausing torrent %s on VPS2 client before move", h[:10])
        paused, pause_err = await self._pause_verified(h)
        if not paused:
            if isinstance(pause_err, TorrentCheckingError):
                # Hash check in flight (slow storage can take a long while):
                # the torrent isn't writing, but moving mid-check risks
                # shipping bytes the check then fails — wait it out quietly
                # (no pause storm, no recheck pile-on, no error escalation).
                # The check flushes per-file progress when done and the next
                # tick moves normally. Bounded by the shared check-wait
                # clock: a check that never finishes fails loudly instead
                # of waiting forever.
                try:
                    _pe_prog = float(
                        getattr(pause_err, "progress", 0.0) or 0.0)
                except (TypeError, ValueError):
                    _pe_prog = 0.0
                _terminal = self._register_checking_park(ts, _pe_prog)
                if _terminal is not None:
                    await self._fail_downloading_row(
                        ts, h,
                        f"{_terminal} ({pause_err.client_state})",
                        # MOVING bytes are complete on SSD: drop only the
                        # client entry, never the data.
                        delete_files=False)
                    return
                self._heartbeat_recheck_holder(ts)
                self._park_moving(
                    ts,
                    f"client hash check running ({pause_err.client_state}); "
                    f"waiting it out without moving",
                    quiet=True,
                )
                return
            # Never move while the client is still writing, but don't fail
            # terminally on a transient WebUI hiccup — stay in MOVING so the
            # next tick retries (preserves downloaded bytes on SSD).
            self._park_moving(
                ts,
                f"could not pause torrent {h[:10]} before move "
                f"({pause_err}); will retry on next tick",
            )
            return

        # 2. Separate completed files from incomplete piece-boundary files
        src_dir = (
            Path(ts.save_path).resolve()
            if ts.save_path
            else Path(self.cfg.dest.save_path).resolve()
        )
        folder = self._season_folder_for(
            cls_files, ts.source_name, base_path=src_dir
        )

        completed_files: list[TorrentFile] = []
        incomplete_files: list[TorrentFile] = []
        uncertain_files: list[TorrentFile] = []
        # Bigger-than-metadata bytes are foreign until proven otherwise
        # (shared save_path collision: same normalized top, another hash).
        # They never move, never block the wipe as _blockers, and the
        # folder wipe below spares them (per-file wipe instead).
        oversized_names: set[str] = set()

        missing_on_disk: list[tuple[str, int, bool, object]] = []
        for f in cls_files:
            file_path = _safe_ssd_join(src_dir, f.name)
            if file_path is None:
                continue
            if not file_path.exists():
                # Not in any set below: record loudly so a desynced piece map
                # (files deleted from disk, client still complete) can't slip
                # silently into wipe + RE_ADDING. The client-complete flag
                # and priority drive the pre-wipe desync gate below.
                try:
                    _complete = (f.progress or 0.0) >= 0.999
                except (TypeError, ValueError, AttributeError):
                    _complete = False
                try:
                    _size = int(f.size_bytes or 0)
                except (TypeError, ValueError):
                    _size = 0
                missing_on_disk.append(
                    (f.name, _size, _complete, getattr(f, "priority", None)))
                continue
            # Client-verified complete AND present on disk at full size: the
            # ONLY set ever moved to remote.
            # NOTE: on-disk size alone must NOT mark completeness — qBittorrent
            # pre-allocates deselected files at full size, so a piece-boundary
            # partial of a deselected episode looks "full" while its progress
            # is < 1. Moving it would upload corrupt data (and could overwrite
            # an older batch's moved file). Progress is authoritative, disk
            # size is the second gate (a 1.0-progress truncated file must not
            # move either).
            if (f.progress or 0.0) >= 0.999:
                try:
                    # Single stat: is_file()+stat() doubles syscalls per
                    # file on the hot move path.
                    _st = file_path.stat()
                    _on_disk = _st.st_size if stat.S_ISREG(_st.st_mode) else -1
                except OSError:
                    _on_disk = -1
                if _on_disk == (f.size_bytes or 0):
                    completed_files.append(f)
                    continue
                if _on_disk > (f.size_bytes or 0):
                    # Bigger than the torrent metadata says: foreign bytes
                    # (shared save_path collision), never move nor delete
                    # individually — same handling as preallocated partials.
                    # Tracked separately (see oversized_names): not a wipe
                    # blocker, spared by the folder wipe.
                    try:
                        _onorm = str(file_path.relative_to(src_dir)).replace("\\", "/")
                    except ValueError:
                        _onorm = ""
                    if _onorm:
                        oversized_names.add(_onorm)
                    log.warning(
                        "file %s oversized on SSD (%d/%d B); leaving for folder wipe, never moving",
                        f.name, max(_on_disk, 0), f.size_bytes,
                    )
                    uncertain_files.append(f)
                    continue
                log.warning(
                    "file %s reports complete but is short on SSD (%d/%d B); not moving",
                    f.name, max(_on_disk, 0), f.size_bytes,
                )
                incomplete_files.append(f)
                continue
            try:
                on_disk = file_path.stat().st_size
            except OSError:
                continue
            if on_disk < (f.size_bytes or 0):
                incomplete_files.append(f)
            else:
                # Preallocated-but-incomplete (or laggy progress): move nothing
                # and delete nothing individually — left for the folder wipe.
                uncertain_files.append(f)
        if uncertain_files:
            log.info(
                "leaving %d unverified file(s) for folder wipe (not individually "
                "deleted, never moved): e.g. %s",
                len(uncertain_files), uncertain_files[0].name,
            )
        if missing_on_disk:
            log.warning(
                "move found %d file(s) missing on SSD (e.g. %s); they move nowhere "
                "and the fuse gate must still pass before re-add",
                len(missing_on_disk), missing_on_disk[0][0],
            )

        # 3. Clean up incomplete piece-boundary files so they are NOT moved to remote
        _post_checking = False
        if incomplete_files:
            # A hash check may have STARTED between the pre-move state read
            # (step 0b) and the file list above: progress dips mid-check, so
            # a complete file can now classify as incomplete and be unlinked
            # below as a piece-boundary partial. Re-read the cheap
            # state-only list; when checking, delete nothing individually
            # this tick — the next tick classifies correctly once the check
            # lands. Only paid when a delete is actually pending.
            try:
                _post = await rpc(
                    self.dest_client.list_torrents(hashes=[h]),
                    getattr(self, "cfg", None),
                    "dest post-list checking state")
                _post_state = str((_post[0].state if _post else "") or "")
                _post_checking = _post_state.lower().startswith("check")
            except Exception:
                _post_checking = False
            if _post_checking:
                log.info(
                    "client hash check started mid-move for %s; keeping %d "
                    "incomplete file(s) this tick without deleting",
                    h[:10], len(incomplete_files))
        for f in ([] if _post_checking else incomplete_files):
            file_path = _safe_ssd_join(src_dir, f.name)
            if file_path is None:
                continue
            # Never delete outside src_dir (traversal guard above) and never
            # follow symlinks out of the SSD tree.
            try:
                if file_path.is_symlink():
                    log.warning("refusing to delete symlink outside SSD tree: %s", f.name)
                    continue
                resolved = file_path.resolve()
                if resolved != src_dir.resolve() and not resolved.is_relative_to(src_dir.resolve()):
                    log.warning("refusing to delete outside SSD tree: %s", f.name)
                    continue
            except OSError:
                continue
            if file_path.exists():
                log.info(
                    "removing incomplete piece-boundary file: %s (%d/%d B)",
                    f.name, file_path.stat().st_size, f.size_bytes,
                )
                try:
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path, ignore_errors=True)
                except OSError as e:
                    log.warning("failed removing incomplete file %s: %s", f.name, e)

        # Also purge any leftover temporary extension files like .!qB or .parts for this torrent.
        # Offloaded: directory walks + per-file resolves must not block the
        # loop on big seasons.
        def _purge_temps() -> None:
            if folder and folder.exists() and folder.resolve() != src_dir.resolve():
                for temp_file in list(folder.glob("**/*.!qB")) + list(folder.glob("**/*.parts")):
                    try:
                        temp_file.unlink()
                    except OSError:
                        pass
            else:
                # Scoped to torrent-owned files only to avoid deleting concurrent downloads in shared save_path
                for f in cls_files:
                    base_file = _safe_ssd_join(src_dir, f.name)
                    if base_file is None:
                        continue
                    for ext in (".!qB", ".parts"):
                        temp_file = base_file.parent / f"{base_file.name}{ext}"
                        try:
                            resolved_tmp = temp_file.resolve()
                            if resolved_tmp != src_dir.resolve() and not resolved_tmp.is_relative_to(src_dir.resolve()):
                                continue
                        except OSError:
                            continue
                        if temp_file.exists():
                            try:
                                temp_file.unlink()
                            except OSError:
                                pass

        try:
            await asyncio.to_thread(_purge_temps)
        except Exception:
            pass

        # 4. Decide target remote from the PINNED kind (see above): batches
        # were moved under it, so the sweep must land beside them even if the
        # fresh file list classifies differently.
        if pinned_kind in ("movie", "season"):
            remote = self.cfg.rclone.remote.default
        else:
            remote = self.cfg.rclone.remote.unsorted

        # 5. Move completed files via rclone
        # (per-file --files-from-raw lists preserve the top dir so the
        # remote layout matches torrent-relative names.)
        if ts.batches_total > 1:
            _top_join = _safe_ssd_join(src_dir, ts.source_name)
            _top_exists = _top_join is not None and _top_join.exists()
            local_folder = folder if (folder and folder.exists()) else (_top_join if _top_exists else None)
            remaining_payload: list[Path] = []
            if local_folder and local_folder.exists() and local_folder.resolve() != src_dir.resolve():
                # Bounded, symlink-safe walk: rglob follows directory symlinks
                # (escape + loop risk), so skip links, contain results to
                # the folder, and cap the listing. Filter during iteration:
                # a 100k-file folder must not materialize 100k Paths before
                # the cap applies.
                try:
                    _scan: list = []
                    for _p in local_folder.rglob("*"):
                        try:
                            if _p.is_symlink():
                                continue
                            if not _p.is_file() or _p.name.endswith((".!qB", ".parts")):
                                continue
                        except OSError:
                            continue
                        _scan.append(_p)
                        if len(_scan) >= 100000:
                            break
                    _folder_real = local_folder.resolve()
                except OSError:
                    _scan = []
                    _folder_real = None
                for p in _scan:
                    try:
                        if _folder_real is not None and p.resolve().is_relative_to(_folder_real):
                            remaining_payload.append(p)
                    except OSError:
                        continue

            if ts.batch_index < ts.batches_total or remaining_payload:
                if local_folder and local_folder.exists():
                    # Move ONLY client-verified-complete leftovers via an exact
                    # --files-from-raw list (same mechanism batch moves use,
                    # preserving torrent-relative paths). Deselected files share
                    # pieces with batch episodes and sit preallocated-but-
                    # incomplete: a bare `<top>/**` move would upload that corrupt
                    # data (and could overwrite an older batch's moved file).
                    try:
                        top_rel = local_folder.resolve().relative_to(src_dir.resolve())
                        top = top_rel.parts[0] if top_rel.parts else ""
                    except Exception:
                        top = ""
                    if not top:
                        # Indeterminate scope (symlink escape — local_folder
                        # is guarded to be inside src_dir, so an empty top
                        # means the layout can't be scoped): refuse to move
                        # an unfiltered set. Park loud, don't fail a row
                        # whose layout may heal.
                        log.error(
                            "cannot scope leftover move for %s under %s; "
                            "staying in MOVING without moving",
                            ts.source_name, src_dir,
                        )
                        self._park_moving(
                            ts,
                            "leftover move scope indeterminate; staying in "
                            "MOVING without moving",
                        )
                        return
                    leftover_names: list[str] = []
                    for f in completed_files:
                        joined = _safe_ssd_join(src_dir, f.name or "")
                        if joined is None:
                            continue
                        try:
                            norm = str(joined.relative_to(src_dir)).replace("\\", "/")
                        except ValueError:
                            continue
                        if not norm:
                            continue
                        if top and not (norm == top or norm.startswith(top + "/")):
                            continue
                        if joined.is_file():
                            leftover_names.append(norm)
                    leftover_files = files_from_names(leftover_names)
                    if not leftover_files:
                        log.info(
                            "multi-batch torrent %s: no verified-complete leftovers to move "
                            "(%d remaining file(s) are incomplete boundary data); skipping remote move",
                            ts.source_name, len(remaining_payload),
                        )
                    else:
                        log.info(
                            "multi-batch torrent %s: moving %d verified-complete leftover file(s) "
                            "(batch %d/%d, %d remaining file(s) total)",
                            ts.source_name, len(leftover_files),
                            ts.batch_index, ts.batches_total, len(remaining_payload),
                        )
                        await self._rclone_move(src_dir, remote, ts, files_from=leftover_files)
                        # Same 0-transfer hazard as batch moves (rclone exits 0
                        # even when it transferred nothing): proceeding to the
                        # folder wipe below would destroy unmoved data. Stay MOVING.
                        stuck = await asyncio.to_thread(
                            left_on_disk_many, src_dir, leftover_files)
                        if stuck:
                            self._park_moving(
                                ts,
                                f"leftover sweep moved nothing "
                                f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                                f"staying in MOVING without wiping",
                            )
                            return
                else:
                    log.info(
                        "multi-batch torrent %s: batches already moved (no remaining local content)",
                        ts.source_name,
                    )
            else:
                log.info(
                    "multi-batch torrent %s: batches were already moved during downloading stage",
                    ts.source_name,
                )
        elif branch_kind in ("movie", "episode", "season", "unknown"):
            local: Path | None
            if branch_kind in ("movie", "episode") and cls.single_file:
                local = _safe_ssd_join(src_dir, cls.single_file)
                if local is None or not local.exists():
                    if folder is not None:
                        _alt = _safe_ssd_join(folder, cls.single_file)
                        if _alt is not None and _alt.exists():
                            local = _alt
                        elif self._single_on_fuse(cls_files, cls.single_file, ts):
                            log.info("single file %s already on remote; skipping move",
                                     cls.single_file)
                            local = None
                        else:
                            raise FileNotFoundError(f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}")
                    elif self._single_on_fuse(cls_files, cls.single_file, ts):
                        log.info("single file %s already on remote; skipping move",
                                 cls.single_file)
                        local = None
                    else:
                        raise FileNotFoundError(f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}")
            elif branch_kind in ("season", "unknown") or (branch_kind == "movie" and not cls.single_file):
                _src_top = _safe_ssd_join(src_dir, ts.source_name)
                if folder and folder.exists():
                    local = folder
                elif _src_top is not None and _src_top.exists():
                    local = _src_top
                else:
                    # Multi-top torrent (files under several top dirs, no
                    # shared folder on disk): no folder-level move — the
                    # verified-complete files move individually below
                    # (local=None disables the top filter; the
                    # nothing-verified branch resumes + parks).
                    log.info(
                        "multi-top content for %s has no shared folder on SSD; "
                        "moving verified-complete files individually",
                        ts.source_name,
                    )
                    local = None
                # Verified-complete per-file move (same guarantee as the sweep
                # above): a bare `<top>/**` move would also upload
                # preallocated-but-incomplete boundary files of content that
                # never needed downloading because it is already remote.
                # Torrent-relative names keep the remote/fuse layout identical.
                try:
                    top_rel = local.resolve().relative_to(src_dir.resolve())
                    top = top_rel.parts[0] if top_rel.parts else ""
                except Exception:
                    top = ""
                folder_names: list[str] = []
                for f in completed_files:
                    joined = _safe_ssd_join(src_dir, f.name or "")
                    if joined is None:
                        continue
                    try:
                        norm = str(joined.relative_to(src_dir)).replace("\\", "/")
                    except ValueError:
                        continue
                    if not norm:
                        continue
                    if top and not (norm == top or norm.startswith(top + "/")):
                        continue
                    if joined.is_file():
                        folder_names.append(norm)
                folder_names = files_from_names(folder_names)
                if folder_names:
                    await self._rclone_move(src_dir, remote, ts, files_from=folder_names)
                    # A 0-transfer folder move must not proceed to the wipe below.
                    stuck = await asyncio.to_thread(
                        left_on_disk_many, src_dir, folder_names)
                    if stuck:
                        self._park_moving(
                            ts,
                            f"folder move left {len(stuck)} file(s) on disk "
                            f"(e.g. {stuck[0]}); staying in MOVING without wiping",
                        )
                        return
                else:
                    # Single-flow with nothing verified: either already remote
                    # (safe to finish) or an incomplete download that advanced
                    # prematurely on torrent-level progress. The latter must
                    # NOT proceed to the folder wipe below — resume so the
                    # remaining bytes can finish, same self-heal as singles.
                    if not completed_files and (incomplete_files or uncertain_files):
                        resumed = False
                        try:
                            await self.dest_client.resume(h)
                            resumed = True
                        except Exception as e:  # noqa: BLE001
                            log.warning(
                                "could not resume incomplete folder %s for %s: %s",
                                local, ts.source_name, e,
                            )
                        if resumed:
                            # Bytes still downloading: hand back to the
                            # download loop instead of flapping pause/resume
                            # in MOVING every tick.
                            log.warning(
                                "folder %s for %s not verified complete; "
                                "resumed to finish downloading",
                                local, ts.source_name,
                            )
                            self._release_recheck_holder(ts)
                            self.transition(ts, State.DOWNLOADING)
                            return
                        log.warning(
                            "folder %s for %s has no verified-complete files "
                            "(%d incomplete, %d unverified); resume failed, "
                            "staying in MOVING without moving",
                            local, ts.source_name,
                            len(incomplete_files), len(uncertain_files),
                        )
                        self._park_moving(
                            ts,
                            f"folder {ts.source_name} not verified complete "
                            f"({len(incomplete_files)} incomplete, "
                            f"{len(uncertain_files)} unverified); "
                            f"resume failed, staying in MOVING without moving",
                        )
                        return
                    log.info(
                        "folder %s for %s has no verified-complete files to move "
                        "(already remote or incomplete boundary data)",
                        local, ts.source_name,
                    )
                local = None
            else:
                cand = _safe_ssd_join(src_dir, ts.source_name)
                if cand is not None and cand.exists():
                    local = cand
                else:
                    raise FileNotFoundError(f"completed content not found on SSD: {src_dir}/{ts.source_name}")
            if (
                branch_kind in ("movie", "episode")
                and cls.single_file
                and isinstance(local, Path)
                and local.is_file()
            ):
                # Single files move below via a bare `rclone move` for the
                # single file itself (verified above); verified-complete
                # siblings move right after via an exact files-from list.
                # Refuse to move client-unverified bytes: a partial file on
                # the remote is worse than waiting. Park in MOVING for
                # retry next tick (nothing is wiped).
                #
                # Self-heal: DOWNLOADING gates on torrent-level progress
                # (>=0.999), so a 99.9% torrent can advance while its single
                # file is still at 99.x% or short on disk. _do_moving pauses
                # at the top, which would freeze those last pieces forever.
                # Resume here so they can finish; next tick re-pauses and
                # moves when verified.
                verified_paths: set[Path] = set()
                for f in completed_files:
                    joined = _safe_ssd_join(src_dir, f.name or "")
                    if joined is None:
                        continue
                    try:
                        verified_paths.add(joined.resolve())
                    except OSError:
                        continue
                try:
                    local_real = local.resolve()
                except OSError:
                    local_real = None
                if local_real is None or local_real not in verified_paths:
                    detail = ""
                    _full_on_disk = False
                    try:
                        for f in cls_files:
                            joined = _safe_ssd_join(src_dir, f.name or "")
                            if joined is None:
                                continue
                            try:
                                if joined.resolve() == local_real:
                                    try:
                                        on_disk = local.stat().st_size if local.is_file() else -1
                                    except OSError:
                                        on_disk = -1
                                    detail = (
                                        f" (file progress={(f.progress or 0.0) * 100:.1f}%, "
                                        f"on-disk {max(on_disk, 0)}/{f.size_bytes or 0} B)"
                                    )
                                    try:
                                        _full_on_disk = (
                                            on_disk == (f.size_bytes or 0)
                                            and (f.size_bytes or 0) > 0
                                        )
                                    except (TypeError, ValueError):
                                        _full_on_disk = False
                                    break
                            except OSError:
                                continue
                    except Exception:
                        detail = ""
                    # Fresh client state (see 0b above): a check started
                    # since the tick began — or started pre-restart while
                    # our once-set was wiped — must not be resumed (restarts
                    # it from zero) or rechecked (resets it). Park quietly.
                    _check_now = False
                    _check_state_now = ""
                    try:
                        _cur = await rpc(
                            self.dest_client.list_torrents(hashes=[h]),
                            getattr(self, "cfg", None),
                            "dest check-guard state")
                        if _cur:
                            _check_state_now = str(
                                getattr(_cur[0], "state", "") or "")
                            _check_now = _check_state_now.lower().startswith(
                                "check")
                    except Exception:
                        _check_now = False
                    if _check_now:
                        self._heartbeat_recheck_holder(ts)
                        self._park_moving(
                            ts,
                            f"client hash check running ({_check_state_now}); "
                            f"waiting it out without touching it",
                            quiet=True,
                        )
                        return
                    try:
                        _rh = (ts.source_infohash or "").lower()
                    except Exception:
                        _rh = ""
                    try:
                        _rc0 = getattr(self, "_settle_rechecked", None)
                        _already_rechecked = bool(
                            _rh and isinstance(_rc0, set) and _rh in _rc0)
                    except Exception:
                        _already_rechecked = False
                    _rechecked_now = False
                    # Settle recheck (once per row, one in flight globally):
                    # full bytes on disk with 0/low client progress right
                    # after a fast download is qB's per-file reporting lag,
                    # not missing data — a force recheck flushes piece states
                    # so the file verifies next tick instead of ~25 min
                    # later. Skipped when the file is short (genuinely
                    # incomplete: resume path below). Globally serialized:
                    # concurrent rechecks pile onto the same starved disk
                    # pool that the fuse seeders already saturate — a waiter
                    # parks quietly and takes its turn (30-min stale holder
                    # takeover bounds a wedged holder).
                    _holder_blocked = False
                    if _full_on_disk and not _already_rechecked:
                        try:
                            _now_m2 = time.monotonic()
                            _holder = getattr(self, "_recheck_in_flight", None)
                            if (isinstance(_holder, (list, tuple))
                                    and len(_holder) == 2
                                    and _holder[0] and _holder[0] != _rh
                                    and isinstance(_holder[1], (int, float))
                                    and _now_m2 - float(_holder[1]) < 1800.0):
                                _holder_blocked = True
                        except Exception:
                            _holder_blocked = False
                    if _full_on_disk and not _already_rechecked and not _holder_blocked:
                        try:
                            _rc = getattr(self, "_settle_rechecked", None)
                            if not isinstance(_rc, set):
                                _rc = set()
                                self._settle_rechecked = _rc  # type: ignore[attr-defined]
                        except Exception:
                            _rc = set()
                        _recheck_ok = False
                        try:
                            await self.dest_client.recheck(h)
                            log.info(
                                "rechecking %s to flush lagging per-file progress",
                                ts.source_name[:60],
                            )
                            _recheck_ok = True
                        except Exception as e:  # noqa: BLE001
                            log.debug(
                                "settle recheck failed for %s: %s",
                                ts.source_name[:60], e,
                            )
                        if _recheck_ok:
                            _rechecked_now = True
                            try:
                                if len(_rc) >= 5000:
                                    # Bounded once-per-row memory: evict an
                                    # arbitrary half BEFORE adding (evicting
                                    # after could drop this very row, making
                                    # it recheck again next tick). A repeat
                                    # recheck is harmless; unbounded growth
                                    # is not.
                                    for _k in list(_rc)[:2500]:
                                        _rc.discard(_k)
                                _rc.add(_rh)
                            except Exception:
                                pass
                            try:
                                self._recheck_in_flight = [  # type: ignore[attr-defined]
                                    _rh, time.monotonic()]
                            except Exception:
                                pass
                    if _holder_blocked:
                        # Another row's check is running: issuing ours would
                        # pile onto the same starved pool, and resuming
                        # could abort theirs. Park quietly; the holder
                        # clears on settle (or ages out after 30 min).
                        resumed = False
                        _resume_note = ("not resumed (another row's hash check "
                                        "in flight; waiting turn),")
                    elif (not _full_on_disk) or (_full_on_disk and _already_rechecked):
                        # Resume only when it can help: short files needing
                        # bytes, or full files whose check already ran without
                        # resolving (possible corruption: refetch). Never
                        # right after issuing a recheck (it needs a tick to
                        # start; resume would abort it) and never during one
                        # (guarded above).
                        resumed = False
                        try:
                            await self.dest_client.resume(h)
                            resumed = True
                        except Exception as e:  # noqa: BLE001
                            log.warning(
                                "could not resume incomplete single file %s for %s: %s",
                                cls.single_file, ts.source_name, e,
                            )
                        _resume_note = ("resumed to finish downloading,"
                                        if resumed else "resume failed,")
                    else:
                        resumed = False
                        _resume_note = ("not resumed (bytes present; hash check "
                                        "will verify),")
                    if resumed:
                        # Bytes still downloading: hand back to the download
                        # loop instead of flapping pause/resume in MOVING.
                        log.warning(
                            "single file %s for %s is not client-verified complete%s; "
                            "resumed to finish downloading",
                            cls.single_file, ts.source_name, detail,
                        )
                        self._release_recheck_holder(ts)
                        self.transition(ts, State.DOWNLOADING)
                        return
                    try:
                        _mp = getattr(self, "_moving_parks", None)
                        _pn = int((_mp.get((ts.source_infohash or "").lower(), 0) or 0)) \
                            if isinstance(_mp, dict) else 0
                    except Exception:
                        _pn = 0
                    # This signature (full bytes, unverified) is an expected
                    # settle wait, not an incident: warn the first handful of
                    # parks, then debug — ERROR escalation stays in
                    # _park_moving for genuinely stuck rows.
                    _log = log.warning if _pn < 8 else log.debug
                    _log(
                        "single file %s for %s is not client-verified complete%s; "
                        "%s staying in MOVING without moving",
                        cls.single_file, ts.source_name, detail,
                        _resume_note,
                    )
                    self._park_moving(
                        ts,
                        f"single file {cls.single_file} not verified complete{detail}; "
                        f"{_resume_note} "
                        f"staying in MOVING without moving",
                    )
                    return
            if local is None:
                pass
            elif isinstance(local, Path) and local.is_dir():
                # Unreachable: folder layouts move per-file above by design.
                raise RuntimeError(f"refusing bare folder move of {local}")
            else:
                # Single-file layout: folder-wrapped singles (Top/file.mkv)
                # must move via files_from from src_dir so the top dir
                # survives on the remote. A bare `rclone move <file>
                # <remote:>` strips it (file lands in the remote root) and
                # the fuse gate then parks forever on Top/file.mkv. Flat
                # singles (no "/") keep the bare move.
                _single_wrapped = False
                _single_norm = ""
                try:
                    _cand = (cls.single_file or "").replace("\\", "/").strip("/")
                    if _cand and "/" in _cand:
                        _joined = _safe_ssd_join(src_dir, _cand)
                        if _joined is not None:
                            try:
                                if _joined.resolve() == local_real:
                                    _single_wrapped = True
                                    _single_norm = _cand
                            except OSError:
                                pass
                    if not _single_wrapped:
                        try:
                            _rel = local.resolve().relative_to(src_dir.resolve())
                            _rel_posix = str(_rel).replace("\\", "/")
                            if "/" in _rel_posix:
                                _single_wrapped = True
                                _single_norm = _cand if _cand else _rel_posix
                        except Exception:
                            pass
                except Exception:
                    _single_wrapped = False
                if _single_wrapped and _single_norm:
                    try:
                        _main_names: list[str] = []
                        for _cf in completed_files:
                            try:
                                _joined = _safe_ssd_join(src_dir, _cf.name or "")
                                if _joined is None:
                                    continue
                                if _joined.resolve() == local_real:
                                    _main_names.append(_cf.name or "")
                                    break
                            except OSError:
                                continue
                        if not _main_names:
                            _main_names = [_single_norm]
                        _sib_names: list[str] = []
                        for _cf in completed_files:
                            try:
                                _joined = _safe_ssd_join(src_dir, _cf.name or "")
                                if _joined is None:
                                    continue
                                if _joined.resolve() == local_real:
                                    continue
                                if not _joined.is_file():
                                    continue
                                _sib_names.append(_cf.name or "")
                            except OSError:
                                continue
                        _wrapped_names = files_from_names(
                            [n for n in ([*_main_names, *_sib_names]) if n])
                    except Exception:
                        _wrapped_names = []
                    if _wrapped_names:
                        log.info(
                            "moving %d verified file(s) for %s preserving "
                            "torrent layout (e.g. %s)",
                            len(_wrapped_names), ts.source_name[:60],
                            _wrapped_names[0],
                        )
                        await self._rclone_move(
                            src_dir, remote, ts, files_from=_wrapped_names)
                        _wrapped_stuck = await asyncio.to_thread(
                            left_on_disk_many, src_dir, _wrapped_names)
                        if _wrapped_stuck:
                            self._park_moving(
                                ts,
                                f"single-file layout move left {len(_wrapped_stuck)} file(s) "
                                f"on disk (e.g. {_wrapped_stuck[0]}); staying in "
                                f"MOVING without wiping",
                            )
                            return
                    else:
                        self._park_moving(
                            ts,
                            f"single file {cls.single_file} has no verified "
                            f"layout names; staying in MOVING without moving",
                        )
                        return
                    # Layout move done (main + siblings together); skip the
                    # bare/sibling path below.
                    _wrapped_done = True
                else:
                    _wrapped_done = False
                if not _wrapped_done:
                    await self._rclone_move(local, remote, ts)
                    try:
                        _left = local.is_symlink() or local.exists()
                    except OSError:
                        _left = True
                    if _left:
                        self._park_moving(
                            ts,
                            f"single-file move left {local} on disk; "
                            f"staying in MOVING without wiping",
                        )
                        return
                    # Dual-format/variant siblings (S01E01.mkv + S01E01.mp4,
                    # subs, extras): the bare move above ships only the single
                    # file, but the fuse gate expects EVERY blob file — a
                    # sibling left behind is wiped with the folder below and
                    # parks the gate forever. Move verified-complete siblings
                    # too (same verified set the folder branches use).
                    try:
                        _sib_names: list[str] = []
                        for _cf in completed_files:
                            try:
                                _joined = _safe_ssd_join(src_dir, _cf.name or "")
                                if _joined is None:
                                    continue
                                if _joined.resolve() == local_real:
                                    continue
                                if not _joined.is_file():
                                    continue
                                _sib_names.append(_cf.name or "")
                            except OSError:
                                continue
                        _sib_names = files_from_names(
                            [n for n in _sib_names if n])
                    except Exception:
                        _sib_names = []
                    if _sib_names:
                        log.info(
                            "moving %d verified sibling file(s) for %s "
                            "(e.g. %s)",
                            len(_sib_names), ts.source_name[:60],
                            _sib_names[0],
                        )
                        await self._rclone_move(
                            src_dir, remote, ts, files_from=_sib_names)
                        _sib_stuck = await asyncio.to_thread(
                            left_on_disk_many, src_dir, _sib_names)
                        if _sib_stuck:
                            self._park_moving(
                                ts,
                                f"sibling move left {len(_sib_stuck)} file(s) "
                                f"on disk (e.g. {_sib_stuck[0]}); staying in "
                                f"MOVING without wiping",
                            )
                            return
        else:
            # Mixed — per-episode moves with --include (single batch).
            # Only reachable when the pinned kind is mixed (see branch_kind
            # above), so cursor rewrites here can't corrupt single-flow rows.
            cap = self._frozen_batch_cap(ts)
            episodes = cls.episodes
            if not episodes:
                raise RuntimeError(
                    f"cannot move torrent {ts.source_name}: classification is '{cls.kind}' but no episodes found"
                )
            if cap <= 0:
                # Files are already on SSD; cap only affects move grouping.
                cap = sum(e.size_bytes for e in episodes) or 1
            batches = make_batches(episodes, cap_bytes=cap)
            if not batches:
                raise RuntimeError(
                    f"cannot move torrent {ts.source_name}: batching produced 0 batches for {len(episodes)} episodes"
                )
            for i, batch in enumerate(batches):
                ts.batch_index = i
                ts.batches_total = len(batches)
                self.store.upsert(ts)
                skip = await self._fuse_skipped(
                    [(e.file_name, e.size_bytes) for e in batch.episodes], branch_kind,
                )
                skip_norm = {(s or "").replace("\\", "/").strip("/") for s in skip}
                names = [n for n in batch.file_names() if n not in skip_norm]
                if not names:
                    log.info(
                        "mixed batch %d/%d for %s already on remote; skipping move",
                        i + 1, len(batches), ts.source_name,
                    )
                    continue
                await self._rclone_move(
                    src_dir,
                    remote,
                    ts,
                    files_from=names,
                    extra=self.cfg.rclone.batch_move_extra_flags,
                )
                stuck = await asyncio.to_thread(
                    left_on_disk_many, src_dir, names)
                if stuck:
                    self._park_moving(
                        ts,
                        f"mixed-torrent batch move moved nothing "
                        f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                        f"staying in MOVING without wiping",
                    )
                    return

        # 5b. Uncertain-file guard: per-file client progress can lag overall
        # completion (a rounded 100.0% overall with files at 99.x%, or a slow
        # client flush), and those files were NOT moved above. Folder-wiping
        # them destroys bytes that never reached the remote — the fuse gate
        # then parks forever on files that can never appear (and the row
        # eventually FAILEDs with data lost on both ends). Park in MOVING
        # instead (no wipe, no advance); next tick re-verifies with a fresh
        # file list, and resume lets lagging bytes finish. Only SELECTED
        # (priority>0) files block: deselected preallocated shells in
        # batch/mixed flows are wiped as today — their bytes were never had
        # (their own batch owns them), while a selected-but-unverified file
        # is real content mid-flush. Missing/None priority counts as
        # selected (fail-closed direction).
        _blockers = []
        try:
            for _uf in (uncertain_files or []):
                try:
                    _prio = getattr(_uf, "priority", None)
                    _prio = 1 if _prio is None else _prio
                    if isinstance(_prio, bool):
                        _prio = 1 if _prio else 0
                    _prio = int(_prio)
                except (TypeError, ValueError):
                    _prio = 1
                if _prio <= 0 or not getattr(_uf, "name", ""):
                    continue
                # Oversized (foreign) bytes never block: they belong to a
                # sibling collision, and stalling this row on them both
                # wedges the pipeline and risks their deletion below.
                try:
                    _unorm = str((_safe_ssd_join(src_dir, _uf.name) or "").relative_to(src_dir)).replace("\\", "/")
                except (ValueError, AttributeError):
                    continue
                if _unorm in oversized_names:
                    continue
                _blockers.append(_uf)
        except Exception:
            _blockers = []
        if _blockers:
            resumed = False
            try:
                await self.dest_client.resume(h)
                resumed = True
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "could not resume %s with %d unverified file(s): %s",
                    ts.source_name, len(_blockers), e,
                )
            _eg = ""
            try:
                _eg = str(_blockers[0].name or "")
            except Exception:
                pass
            if resumed:
                # Bytes still downloading: hand back to the download loop
                # instead of flapping pause/resume in MOVING every tick.
                log.warning(
                    "folder %s for %s has %d unverified selected file(s) "
                    "(e.g. %s); resumed to finish downloading",
                    folder or src_dir, ts.source_name, len(_blockers), _eg,
                )
                self._release_recheck_holder(ts)
                self.transition(ts, State.DOWNLOADING)
                return
            log.warning(
                "folder %s for %s has %d unverified selected file(s) "
                "(e.g. %s); resume failed, staying in MOVING without wiping",
                folder or src_dir, ts.source_name, len(_blockers), _eg,
            )
            self._park_moving(
                ts,
                f"folder {ts.source_name} has {len(_blockers)} unverified "
                f"selected file(s) (e.g. {_eg}); resume failed, "
                f"staying in MOVING without wiping",
            )
            return

        # 6. Persist the SSD torrent's bytes for RE_ADDING before the client
        # entry is deleted below. Rows adopted by recovery (fresh state.db)
        # carry no blob — without this, _re_add_cross_seed_torrent fails on
        # "missing blob" and the fuse gate has nothing to verify against.
        # Best-effort: a failed export only warns; downstream fails loudly.
        if not (ts._blob or ts.cross_seed_blob):
            try:
                stored = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                stored = None
            if isinstance(stored, (bytes, bytearray)) and stored:
                ts.cross_seed_blob = bytes(stored)
                ts._blob = bytes(stored)
        if not (ts._blob or ts.cross_seed_blob):
            export_fn = getattr(self.dest_client, "export_torrent", None)
            if callable(export_fn):
                try:
                    from .io_bounds import bounded as _bounded

                    exported = await _bounded(
                        export_fn(h), timeout=15.0,
                        label="dest pre-delete export")
                    if isinstance(exported, (bytes, bytearray)) and exported:
                        ts.cross_seed_blob = bytes(exported)
                        ts._blob = bytes(exported)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not export .torrent for %s before deleting SSD entry: %s",
                        h[:10], e,
                    )
        if ts._blob or ts.cross_seed_blob:
            try:
                self.store.upsert(ts)
            except Exception as e:  # noqa: BLE001
                log.warning("could not persist cross-seed blob for %s: %s", h[:10], e)

        # 7. Delete old torrent from VPS2 client (delete_files=False) before re-adding to FUSE.
        # A failed delete parks (no wipe): the entry still exists, so the
        # next tick retries instead of wiping toward a row whose client
        # state no longer matches. Same for a failing folder wipe below —
        # a fully-moved row must not FAILED over a releasable handle
        # (Windows open-handle): park and retry.
        try:
            await self.dest_client.delete(h, delete_files=False)
        except Exception as e:  # noqa: BLE001
            log.warning("could not delete old torrent from client after move: %s", e)
            self._park_moving(
                ts,
                f"client entry delete failed after move ({e}); staying in "
                f"MOVING without wiping",
            )
            return

        # 7b. Missing-but-client-complete gate: files the client calls
        # complete yet absent from SSD must not be wiped past silently —
        # the fuse gate would park forever on unseedable bytes. Expected
        # absences are excluded first: files in already-moved batches
        # (batch moves delete from SSD immediately) and, when batch
        # resolution fails, deselected files of multi-batch rows (moved
        # batches ride priority 0); then anything already verified on
        # the fuse remote. The remainder is desync: fail loudly with
        # bytes kept instead of wiping toward a hopeless RE_ADDING.
        _complete_missing: list[tuple[str, int, object]] = []
        try:
            for _mn, _ms, _mc, _mp in missing_on_disk:
                if _mc:
                    _complete_missing.append((_mn, _ms, _mp))
        except Exception:
            _complete_missing = []
        _desync: list[tuple[str, int]] = [
            (n, s) for (n, s, _p) in _complete_missing]
        if _desync and (ts.batches_total or 0) > 1:
            _moved: set[str] = set()
            try:
                _cap = self._frozen_batch_cap(ts)
                _batches = self._resolve_batches(
                    cls_files, branch_kind, _cap,
                    episodes=getattr(cls, "episodes", None))
                for _b in (_batches or [])[:max(0, int(ts.batch_index or 0))]:
                    for _ep in (getattr(_b, "episodes", None) or []):
                        _enm = getattr(_ep, "file_name", "") or ""
                        if _enm:
                            _moved.add(_enm.replace("\\", "/"))
            except Exception:
                pass

            def _deselected(_p) -> bool:
                try:
                    if _p is None:
                        return False
                    if isinstance(_p, bool):
                        return not _p
                    return int(_p) == 0
                except (TypeError, ValueError):
                    return False

            _desync = [(n, s) for (n, s, p) in _complete_missing
                       if n.replace("\\", "/") not in _moved
                       and not _deselected(p)]
        if _desync:
            try:
                _mounts = []
                for _ms in self._fuse_mount_strs():
                    try:
                        if _ms:
                            _mounts.append(Path(_ms))
                    except Exception:
                        continue
            except Exception:
                _mounts = []
            for _mt in _mounts:
                if not _desync:
                    break
                try:
                    _still = await self._missing_fuse_files(_mt, _desync)
                except Exception:
                    continue
                if _still and str(_still[0]).startswith("<"):
                    continue  # mount unverifiable: keep as desync
                _stillset = set(_still or [])
                _desync = [(n, s) for (n, s) in _desync if n in _stillset]
        if _desync:
            await self._fail_downloading_row(
                ts, h,
                f"{len(_desync)} client-complete file(s) missing on SSD "
                f"(e.g. {_desync[0][0]}); data lost, not wiping",
                delete_files=False)
            return

        # 8. Delete local content folder on SSD after move
        # (wipe resolves through the coordinator namespace: patched in
        # tests as racing_sync.coordinator.wipe_local_tree.)
        if folder and folder.resolve() != src_dir.resolve() and folder.exists():
            if oversized_names:
                # Foreign bytes share this folder: wipe per-file, sparing
                # them (and their parent dirs), so sibling data survives.
                await self._wipe_folder_except(folder, oversized_names, src_dir)
            else:
                log.info("deleting content folder after move: %s", folder)
                from . import coordinator as _coordinator

                try:
                    await _coordinator.wipe_local_tree(
                        folder,
                        base_dir=[self.cfg.ssd.path, Path(self.cfg.dest.save_path)],
                    )
                except OSError as e:
                    # Cleanup failure after a verified move: park, don't fail
                    # the fully-moved row (retry converges when the handle
                    # releases; the ERROR escalation pages if it never does).
                    self._park_moving(
                        ts,
                        f"folder wipe failed after move ({e}); staying in "
                        f"MOVING without advancing",
                    )
                    return

        try:
            _csd = getattr(self, "_checking_track", None)
            if isinstance(_csd, dict):
                _csd.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass
        self._release_recheck_holder(ts)
        self.transition(ts, State.RE_ADDING)

    async def _wipe_folder_except(self, folder: Path, keep: set[str],
                                    src_dir: Path) -> None:
        """Wipe a moved folder but spare foreign (oversized) files.

        Deletes every file under `folder` except the keep set (SSD-relative
        posix names, matched exactly or as a parent dir), then removes dirs
        left empty. Best-effort and off-loop; failures only warn — the row
        already completed its move.
        """
        log.info("wiping moved folder except %d foreign file(s): %s",
                 len(keep), folder)

        def _sweep() -> None:
            try:
                _paths = sorted(folder.rglob("*"), reverse=True)
            except OSError:
                return
            for _p in _paths:
                try:
                    if _p.is_symlink():
                        continue
                    try:
                        _rel = str(_p.relative_to(src_dir)).replace("\\", "/")
                    except ValueError:
                        continue
                    if not _rel or _rel == ".":
                        continue
                    _kept = any(_rel == _k or _k.startswith(_rel + "/")
                                for _k in keep)
                    if _kept:
                        continue
                    if _p.is_file():
                        _p.unlink()
                    elif _p.is_dir():
                        try:
                            _p.rmdir()
                        except OSError:
                            pass
                except OSError:
                    continue

        try:
            await asyncio.to_thread(_sweep)
        except Exception as e:  # noqa: BLE001
            log.warning("per-file wipe of %s failed: %s", folder, e)

    async def _rclone_move(
        self,
        local: Path,
        remote: str,
        ts: TorrentState,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
    ) -> None:
        async with self.move_sem:
            if files_from is not None:
                preview = ", ".join(files_from[:3]) + ("…" if len(files_from) > 3 else "")
                log.info("rclone move %s -> %s (files=%d, e.g. %s)",
                         local, remote, len(files_from), preview)
            else:
                log.info("rclone move %s -> %s (include=%s, extra=%s)", local, remote, include, extra)
            # RcloneTimeoutError propagates: the child is terminated and the
            # source tree intact, so callers park the row for retry (never
            # fail — failing would loop: re-download, stall again, fail…).
            res = await move_local_to_remote(self.cfg, local, remote, include=include,
                                             files_from=files_from, extra=extra)
            if not res.ok:
                err = res.stderr.strip()
                # Headline first: rclone puts the summary up top and the
                # detail at the bottom — keep both ends, not just the tail.
                _lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
                last_err = f"{_lines[0]} [...] {_lines[-1]}" if len(_lines) > 1 else (_lines[0] if _lines else f"rc={res.returncode}")
                if is_transient_rclone_stderr(err):
                    raise RcloneTransientError(
                        f"rclone transient failure (rc={res.returncode}): {last_err}")
                raise RuntimeError(f"rclone failed (rc={res.returncode}): {last_err}")