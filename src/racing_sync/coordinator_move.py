"""MOVING phase mixin: verified moves, folder wipe, rclone execution.

Split out of the coordinator god-file. Only client-verified-complete
bytes move (progress authoritative, disk size second gate); leftovers,
uncertain files, and missing bytes park instead of wiping. Single files
move bare, seasons per verified file list, mixed per re-batched episode
group. The shape machinery those branches run on (sort, purge, layout
moves) lives in ``coordinator_move_shapes`` (``MoveShapesMixin``).

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
import time
from pathlib import Path

from .classifier import Classification
from .clients.abstract import TorrentFile
from .coordinator_content import route_kind_is_sorted
from .coordinator_errors import (
    AbandonedError,
    RcloneTransientError,
    TorrentCheckingError,
    is_retryable_client_error,
    is_transient_rclone_stderr,
)
from .coordinator_move_checkpoint import (
    _MOVE_PLAN_VERSION as _MOVE_PLAN_VERSION,  # noqa: F401  (re-exported API)
    _MOVE_STAGE_ENTRY_DELETED as _MOVE_STAGE_ENTRY_DELETED,  # noqa: F401  (re-exported API)
    _MOVE_STAGE_MOVED,
    _MOVE_STAGE_WIPED as _MOVE_STAGE_WIPED,  # noqa: F401  (re-exported API)
    _MOVE_STAGES as _MOVE_STAGES,  # noqa: F401  (re-exported API)
    CheckpointMixin,
)
from .coordinator_move_shapes import MoveShapesMixin
from .coordinator_paths import _safe_ssd_join
from .io_bounds import rpc
from .rclone_ops import _tree_key, rclone_progress_map
from .state import State, TorrentState
from .transfer import build_transfer

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Unreadable-mount parks before a MOVING row fails for operator
# attention (errno-None OSErrors never heal by retrying).
_MOVE_ERRNO_FAIL_PARKS = 10

# Legacy stage-less MOVING rows whose dest entry is gone: bounded parks
# before failing for operator attention (re-listing can never bring a
# deleted entry back, so parking forever strands the row in MOVING).
_LEGACY_MOVE_FAIL_PARKS = 5


class MoveMixin(CheckpointMixin, MoveShapesMixin):
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
            # Live-transfer reset: a move pumping bytes is not stalled —
            # transferred growth restarts the park count instead of
            # escalating a healthy multi-hour move to ERROR.
            try:
                _prog_now = self._moving_transferred(key)
            except Exception:
                _prog_now = None
            if _prog_now is not None:
                try:
                    _marks = getattr(self, "_moving_progress_bytes", None)
                    if not isinstance(_marks, dict):
                        _marks = {}
                        self._moving_progress_bytes = _marks
                    _prev = _marks.get(key)
                    if isinstance(_prev, (int, float)) and _prog_now > _prev:
                        parks[key] = 0
                    _marks[key] = _prog_now
                    if len(_marks) > 5000:
                        for _k in list(_marks.keys())[: len(_marks) - 5000]:
                            _marks.pop(_k, None)
                except Exception:
                    pass
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
                _nv = _up((ts.source_infohash or ""), {"last_error": ts.last_error})
                if isinstance(_nv, int) and _nv > 0:
                    try:
                        ts.version = _nv
                    except Exception:
                        pass
                elif self._abandoned(ts):
                    raise AbandonedError(
                        f"row gone for {(ts.source_infohash or '')[:10]}; not parking in MOVING"
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
                ts.source_name[:60],
                reason,
                n,
            )
        elif n >= 5 and (n == 5 or (n - 5) % 20 == 0):
            log.error(
                "MOVING stalled for %s: %s (parked %dx, still retrying)",
                ts.source_name[:60],
                reason,
                n,
            )
        elif n >= 5:
            log.debug(
                "MOVING still parked for %s: %s (parked %dx)",
                ts.source_name[:60],
                reason,
                n,
            )
        else:
            log.warning(
                "staying in MOVING for %s: %s",
                ts.source_name[:60],
                reason,
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
            (
                f
                for f in files
                if str(_safe_ssd_join(Path("."), getattr(f, "name", "") or "") or "") == norm
            ),
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
            if (
                isinstance(_ri, (list, tuple))
                and len(_ri) == 2
                and _ri[0] == (ts.source_infohash or "").lower()
            ):
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
            if isinstance(_ri, (list, tuple)) and len(_ri) == 2 and _key and _ri[0] == _key:
                self._recheck_in_flight = [_key, time.monotonic()]
        except Exception:
            pass

    def _moving_transferred(self, row_key: str) -> int | None:
        """Live transferred bytes for a MOVING row, or None when unknown.

        Joins the row to its move tree (`_moving_tree`, recorded when the
        move spawned) and reads the rclone progress map. Pure lookup —
        never raises, never touches the loop.
        """
        try:
            _trees = getattr(self, "_moving_tree", None)
            if not isinstance(_trees, dict):
                return None
            _tkey = _trees.get((row_key or "").lower())
            if not _tkey:
                return None
            _prog = rclone_progress_map().get(_tkey)
            if _prog is None:
                return None
            try:
                return int(getattr(_prog, "transferred_bytes", 0) or 0)
            except (TypeError, ValueError):
                return None
        except Exception:
            return None

    def rclone_move_progress_map(self) -> dict[str, object]:
        """RcloneProgress by lowercase row infohash (for active-list cards).

        Only rows with a live move tree appear; entries vanish when the
        move completes. Never raises.
        """
        out: dict[str, object] = {}
        try:
            _trees = getattr(self, "_moving_tree", None)
            if not isinstance(_trees, dict):
                return out
            _live = rclone_progress_map()
            for _rowkey, _tkey in list(_trees.items()):
                try:
                    _prog = _live.get(_tkey)
                    if _prog is not None:
                        out[_rowkey] = _prog
                except Exception:
                    continue
        except Exception:
            pass
        return out

    def _register_checking_park(self, ts: TorrentState, progress: float = 0.0) -> str | None:
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
            _stall = float(getattr(self.rt.limits, "download_check_stall_seconds", 1800) or 0)
        except (TypeError, ValueError):
            _stall = 1800.0
        try:
            _abscap = float(getattr(self.rt.limits, "download_max_check_wait_seconds", 43200) or 0)
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
            if (
                isinstance(_raw, (list, tuple))
                and len(_raw) == 3
                and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in _raw)
            ):
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
            return (
                f"client hash check made no progress for {int(_stall)}s; "
                f"failing for operator attention"
            )
        if _abscap > 0 and _now_m - _rec[0] > _abscap:
            try:
                if isinstance(_csd, dict):
                    _csd.pop(_ckey, None)
            except Exception:
                pass
            return (
                f"client hash check exceeded {int(_abscap)}s overall; "
                f"failing for operator attention"
            )
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
        _stage = str(getattr(ts, "move_stage", "") or "").strip().lower()
        if _stage:
            await self._replay_move_checkpoint(ts, _stage)
            return
        cls_files = await self._list_files_for_move(ts, h)
        if cls_files is None:
            return
        cls, pinned_kind, branch_kind = self._classify_and_pin(ts, h, cls_files)

        # 0b. Never disturb an in-progress hash check. Pausing aborts it and
        # the resume below restarts it, which together livelock the check
        # forever on slow storage (observed: alternating checking/quiet
        # parks every tick, check never completing). A checking torrent
        # isn't writing, so there is nothing to pause — park quietly and
        # let it finish. One cheap state-only list (no files/trackers fan-out).
        # Bounded by the shared check-wait clock (12h default): a check that
        # never finishes fails loudly instead of waiting forever.
        _pre, _pre_state = await self._read_pre_move_state(h)
        if not await self._guard_legacy_move_entry(ts, h, _pre):
            return
        if not await self._guard_checking_before_move(ts, h, _pre, _pre_state):
            return

        if not await self._pause_before_move(ts, h):
            return

        # 2. Separate completed files from incomplete piece-boundary files
        src_dir = (
            Path(ts.save_path).resolve()
            if ts.save_path
            else Path(self.rt.storage.save_path).resolve()
        )
        folder = self._season_folder_for(cls_files, ts.source_name, base_path=src_dir)

        (
            completed_files,
            incomplete_files,
            uncertain_files,
            oversized_names,
            missing_on_disk,
        ) = self._sort_move_files(cls_files, src_dir)

        # 3. Clean up incomplete piece-boundary files so they are NOT moved to remote
        await self._purge_incomplete_files(h, src_dir, incomplete_files)
        await self._purge_temp_files(cls_files, folder, src_dir)

        # 4. Decide target remote from the PINNED kind (see above): batches
        # were moved under it, so the sweep must land beside them even if the
        # fresh file list classifies differently.
        if route_kind_is_sorted(pinned_kind):
            remote = self.rt.transfer.remote_default
        else:
            remote = self.rt.transfer.remote_unsorted

        # 5. Move completed files via rclone
        # (per-file --files-from-raw lists preserve the top dir so the
        # remote layout matches torrent-relative names.)
        if ts.batches_total > 1:
            if not await self._move_multi_batch_leftovers(
                ts, src_dir, folder, completed_files, remote
            ):
                return
        elif branch_kind in ("movie", "episode", "season", "unknown"):
            if not await self._move_single_or_folder(
                ts,
                h,
                cls,
                cls_files,
                src_dir,
                folder,
                completed_files,
                incomplete_files,
                uncertain_files,
                remote,
                branch_kind,
            ):
                return
        else:
            if not await self._move_mixed_batches(
                ts, cls, src_dir, completed_files, remote, branch_kind
            ):
                return
        # 5b. Uncertain-file guard.
        if not await self._guard_unverified_selected(
            ts, h, src_dir, folder, uncertain_files, oversized_names
        ):
            return

        # 6. Persist the cross-seed blob for RE_ADDING.
        await self._persist_cross_seed_blob(ts, h)

        # 7. Missing-but-client-complete desync gate.
        if not await self._guard_missing_complete(
            ts, h, cls, cls_files, branch_kind, missing_on_disk
        ):
            return

        # 8. Persist the cleanup plan before deleting the client entry.
        if not self._persist_moved_plan(ts, src_dir, folder, uncertain_files, oversized_names):
            return

        # 9. Delete old torrent from VPS2 client (delete_files=False) before
        # re-adding to FUSE. A failed delete parks at 'moved' (no wipe): the
        # next tick replays the delete only. Same for the cleanup below �?"
        # a fully-moved row must not FAILED over a releasable handle
        # (Windows open-handle): park and retry from the checkpoint.
        #
        # 9-10. The persisted 'moved' checkpoint is now the single entry
        # point for the destructive remainder: delete the client entry →
        # 'entry_deleted' → wipe SSD → 'wiped' → RE_ADDING. A fresh run and
        # a crash-resumed run therefore execute the same code (I8) instead
        # of two near-duplicate sequences that could drift apart.
        await self._replay_move_checkpoint(ts, _MOVE_STAGE_MOVED)

    async def _list_files_for_move(self, ts: TorrentState, h: str) -> list[TorrentFile] | None:
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
                        ts,
                        h,
                        f"repeated unreadable-mount errors ({e}); failing for operator attention",
                        delete_files=False,
                    )
                    return
            # Every other phase parks on transient client trouble; a single
            # qB timeout here must not fail a fully-downloaded row.
            self._park_moving(
                ts,
                f"could not list files for {h[:10]} before move ({e}); will retry on next tick",
            )
            return
        try:
            _emp_ok = getattr(self, "_move_errno_parks", None)
            if isinstance(_emp_ok, dict):
                _emp_ok.pop(h.lower(), None)
        except Exception:
            pass
        return cls_files

    def _classify_and_pin(
        self, ts: TorrentState, h: str, cls_files: list[TorrentFile]
    ) -> tuple[Classification, str, str]:
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
                # Narrow write: this runs after several awaits, and a
                # whole-row upsert would replay a stale snapshot over a
                # concurrent transition. Only the pin is ours to write.
                self.store.update_columns(
                    h, {"classification_kind": cls.kind}, expected_version=ts.version
                )
            except Exception:  # noqa: BLE001
                pass
        elif cls.kind and cls.kind != pinned_kind:
            log.warning(
                "classification flipped %s -> %s for %s after queue; keeping pinned %s for routing",
                pinned_kind,
                cls.kind,
                ts.source_name,
                pinned_kind,
            )
        # Branch the move on the PINNED kind (falling back to the fresh one
        # when nothing was ever pinned): batches were downloaded and routed
        # under it, so a mid-flight flip (tracker sidecar added, metadata
        # completed) must not reroute the move or rewrite batch cursors via
        # the mixed branch. Fresh `cls` still supplies the file layout
        # (episodes/single_file/sizes) below.
        branch_kind = pinned_kind if pinned_kind not in ("", "unknown") else cls.kind
        return cls, pinned_kind, branch_kind

    async def _read_pre_move_state(self, h: str) -> tuple[list | None, str]:
        try:
            _pre = await rpc(
                self.dest_client.list_torrents(hashes=[h]),
                getattr(self, "cfg", None),
                "dest pre-move state",
            )
            _pre_state = _pre[0].state if _pre else ""
        except Exception:
            _pre = None
            _pre_state = ""
        return _pre, _pre_state

    async def _guard_legacy_move_entry(self, ts: TorrentState, h: str, pre: list | None) -> bool:
        if pre is not None and not pre:
            # Empty list (not an error). A stage-less row may be a legacy
            # row whose entry was already deleted before the checkpoint
            # existed: re-listing forever (or wiping blind) is unsafe, so
            # fail closed with an explicit operator message after a bounded
            # run of parks (a briefly lagging list gets a few ticks to heal
            # first). Checkpointed rows never reach this branch (replay runs
            # at the top).
            try:
                _lmp = getattr(self, "_legacy_move_parks", None)
                if not isinstance(_lmp, dict):
                    _lmp = {}
                    self._legacy_move_parks = _lmp  # type: ignore[attr-defined]
                _n = int(_lmp.get(h.lower(), 0) or 0) + 1
                _lmp[h.lower()] = _n
                if len(_lmp) > 5000:
                    for _k in list(_lmp.keys())[: len(_lmp) - 5000]:
                        _lmp.pop(_k, None)
            except Exception:
                _n = 0
            if _n >= _LEGACY_MOVE_FAIL_PARKS:
                try:
                    _lmp.pop(h.lower(), None)
                except Exception:
                    pass
                await self._fail_downloading_row(
                    ts,
                    h,
                    "legacy MOVING row has no checkpoint and no dest "
                    "entry; failing for operator attention",
                    delete_files=False,
                )
                return False
            self._park_moving(
                ts,
                f"legacy MOVING row has no checkpoint; refusing unsafe "
                f"wipe (dest entry not listed before move) "
                f"({_n}/{_LEGACY_MOVE_FAIL_PARKS})",
            )
            return False
        if pre:
            # A successful listing (entry present) heals the legacy counter:
            # past missing-ticks no longer count toward a fail (mirrors the
            # _move_errno_parks reset after a clean file list).
            try:
                _lmp_ok = getattr(self, "_legacy_move_parks", None)
                if isinstance(_lmp_ok, dict):
                    _lmp_ok.pop(h.lower(), None)
            except Exception:
                pass
        return True

    async def _guard_checking_before_move(
        self,
        ts: TorrentState,
        h: str,
        pre: list | None,
        pre_state: str,
    ) -> bool:
        try:
            _pre_checking = str(pre_state or "").lower().startswith("check")
        except Exception:
            _pre_checking = False
        if _pre_checking:
            try:
                _pre_prog = float(getattr(pre[0], "progress", 0.0) or 0.0)
            except (TypeError, ValueError, AttributeError):
                _pre_prog = 0.0
            _terminal = self._register_checking_park(ts, _pre_prog)
            if _terminal is not None:
                await self._fail_downloading_row(
                    ts,
                    h,
                    f"{_terminal} ({pre_state})",
                    # MOVING bytes are complete on SSD: drop only the
                    # client entry, never the data.
                    delete_files=False,
                )
                return False
            self._heartbeat_recheck_holder(ts)
            self._park_moving(
                ts,
                f"client hash check running ({pre_state}); waiting it out without touching it",
                quiet=True,
            )
            return False
        return True

    async def _pause_before_move(self, ts: TorrentState, h: str) -> bool:
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
                    _pe_prog = float(getattr(pause_err, "progress", 0.0) or 0.0)
                except (TypeError, ValueError):
                    _pe_prog = 0.0
                _terminal = self._register_checking_park(ts, _pe_prog)
                if _terminal is not None:
                    await self._fail_downloading_row(
                        ts,
                        h,
                        f"{_terminal} ({pause_err.client_state})",
                        # MOVING bytes are complete on SSD: drop only the
                        # client entry, never the data.
                        delete_files=False,
                    )
                    return False
                self._heartbeat_recheck_holder(ts)
                self._park_moving(
                    ts,
                    f"client hash check running ({pause_err.client_state}); "
                    f"waiting it out without moving",
                    quiet=True,
                )
                return False
            # Never move while the client is still writing, but don't fail
            # terminally on a transient WebUI hiccup — stay in MOVING so the
            # next tick retries (preserves downloaded bytes on SSD).
            self._park_moving(
                ts,
                f"could not pause torrent {h[:10]} before move "
                f"({pause_err}); will retry on next tick",
            )
            return False
        return True

    async def _guard_unverified_selected(
        self,
        ts: TorrentState,
        h: str,
        src_dir: Path,
        folder: Path | None,
        uncertain_files: list[TorrentFile],
        oversized_names: set[str],
    ) -> bool:
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
            for _uf in uncertain_files or []:
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
                    _unorm = str(
                        (_safe_ssd_join(src_dir, _uf.name) or "").relative_to(src_dir)
                    ).replace("\\", "/")
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
                    ts.source_name,
                    len(_blockers),
                    e,
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
                    folder or src_dir,
                    ts.source_name,
                    len(_blockers),
                    _eg,
                )
                self._release_recheck_holder(ts)
                self.transition(ts, State.DOWNLOADING)
                return False
            log.warning(
                "folder %s for %s has %d unverified selected file(s) "
                "(e.g. %s); resume failed, staying in MOVING without wiping",
                folder or src_dir,
                ts.source_name,
                len(_blockers),
                _eg,
            )
            self._park_moving(
                ts,
                f"folder {ts.source_name} has {len(_blockers)} unverified "
                f"selected file(s) (e.g. {_eg}); resume failed, "
                f"staying in MOVING without wiping",
            )
            return False
        return True

    async def _persist_cross_seed_blob(self, ts: TorrentState, h: str) -> None:
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
                        export_fn(h), timeout=15.0, label="dest pre-delete export"
                    )
                    if isinstance(exported, (bytes, bytearray)) and exported:
                        ts.cross_seed_blob = bytes(exported)
                        ts._blob = bytes(exported)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "could not export .torrent for %s before deleting SSD entry: %s",
                        h[:10],
                        e,
                    )
        if ts._blob or ts.cross_seed_blob:
            try:
                self.store.set_blob(ts.source_infohash, bytes(ts._blob or ts.cross_seed_blob))
            except Exception as e:  # noqa: BLE001
                log.warning("could not persist cross-seed blob for %s: %s", h[:10], e)

    async def _guard_missing_complete(
        self,
        ts: TorrentState,
        h: str,
        cls: Classification,
        cls_files: list[TorrentFile],
        branch_kind: str,
        missing_on_disk: list[tuple[str, int, bool, object]],
    ) -> bool:
        # 7. Missing-but-client-complete gate: files the client calls
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
        _desync: list[tuple[str, int]] = [(n, s) for (n, s, _p) in _complete_missing]
        if _desync and (ts.batches_total or 0) > 1:
            _moved: set[str] = set()
            try:
                _cap = self._frozen_batch_cap(ts)
                _batches = self._resolve_batches(
                    cls_files, branch_kind, _cap, episodes=getattr(cls, "episodes", None)
                )
                for _b in (_batches or [])[: max(0, int(ts.batch_index or 0))]:
                    for _ep in getattr(_b, "episodes", None) or []:
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

            _desync = [
                (n, s)
                for (n, s, p) in _complete_missing
                if n.replace("\\", "/") not in _moved and not _deselected(p)
            ]
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
                ts,
                h,
                f"{len(_desync)} client-complete file(s) missing on SSD "
                f"(e.g. {_desync[0][0]}); data lost, not wiping",
                delete_files=False,
                entry_hash=h,
            )
            return False
        return True

    async def _rclone_move(
        self,
        local: Path,
        remote: str,
        ts: TorrentState,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
        heal: bool = False,
    ) -> None:
        # Heals ride the reserved heal slot (seconds-long repairs must not
        # queue behind bulk multi-hour moves); everything else shares
        # move_sem.
        _sem = self.heal_move_sem if heal else self.move_sem
        try:
            _mkey = (ts.source_infohash or "").lower()
        except Exception:
            _mkey = ""
        # Move-lane queue: wait for a lane slot BEFORE taking the
        # semaphore (parking under it would serialize the lane behind
        # waiters). Forced rows skip the queue (still serialized by the
        # semaphore below — force bypasses caps, never mutual exclusion).
        while not getattr(self, "_stop", False):
            try:
                _forced = self._is_forced(_mkey) if _mkey else False
            except Exception:
                _forced = False
            if _forced:
                break
            try:
                _mqueued, _mev = self.check_running_tasks(ts, "move")
            except Exception:
                break
            if not _mqueued:
                break
            try:
                from .task import handles_for

                for _hd in handles_for(self, _mkey):
                    try:
                        _hd.queued = True
                    except Exception:
                        pass
            except Exception:
                pass
            log.info(
                "moves full; %s waits for a slot (no churn)",
                (getattr(ts, "source_name", "") or _mkey)[:60],
            )
            if _mev is None:
                break
            try:

                async def _wait_mev(_mev=_mev) -> None:
                    # Bind `_mev` explicitly: the closure would otherwise
                    # capture the loop variable late (next tick's event).
                    try:
                        await asyncio.wait_for(_mev.wait(), 60.0)
                    except TimeoutError:
                        pass
                    try:
                        _mev.clear()
                    except Exception:
                        pass

                await _wait_mev()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
        try:
            from .task import handles_for as _hf2

            for _hd in _hf2(self, _mkey):
                try:
                    _hd.queued = False
                except Exception:
                    pass
        except Exception:
            pass
        if getattr(self, "_stop", False):
            # Shutdown requested while queued (or just before the
            # semaphore): never start a fresh transfer. Raise instead of
            # returning — callers treat a normal return as a completed
            # move and would commit checkpoints. The MOVING checkpoint
            # already persisted makes a restart safe.
            log.info(
                "move of %s aborted: shutting down", (getattr(ts, "source_name", "") or _mkey)[:60]
            )
            raise asyncio.CancelledError
        async with _sem:
            if files_from is not None:
                preview = ", ".join(files_from[:3]) + ("…" if len(files_from) > 3 else "")
                log.info(
                    "move %s -> %s (files=%d, e.g. %s)",
                    local,
                    remote,
                    len(files_from),
                    preview,
                )
            else:
                log.info("move %s -> %s (include=%s, extra=%s)", local, remote, include, extra)
            # RcloneTimeoutError propagates: the child is terminated and the
            # source tree intact, so callers park the row for retry (never
            # fail — failing would loop: re-download, stall again, fail…).
            # progress=True publishes live stats (long moves stay visible;
            # transferred growth resets the park-stall clock).
            try:
                _trees = getattr(self, "_moving_tree", None)
                if not isinstance(_trees, dict):
                    _trees = {}
                    self._moving_tree = _trees
                try:
                    _tkey = _tree_key(local)
                    _trees[(ts.source_infohash or "").lower()] = _tkey
                except Exception:
                    _tkey = None
                # The worker's handle owns this tree for cancel(): a
                # later TaskHandle.cancel() aborts exactly this proc.
                if _tkey:
                    try:
                        from .task import handles_for

                        for _hd in handles_for(self, (ts.source_infohash or "")):
                            try:
                                _hd.tree_key = _tkey
                            except Exception:
                                pass
                    except Exception:
                        pass
                if len(_trees) > 5000:
                    for _k in list(_trees.keys())[: len(_trees) - 5000]:
                        _trees.pop(_k, None)
            except Exception:
                pass
            provider = build_transfer(self.rt)
            res = await provider.move(
                local,
                remote,
                include=include,
                files_from=files_from,
                extra=extra,
            )
            if not res.ok:
                err = res.stderr.strip()
                # Headline first: rclone puts the summary up top and the
                # detail at the bottom — keep both ends, not just the tail.
                _lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
                last_err = (
                    f"{_lines[0]} [...] {_lines[-1]}"
                    if len(_lines) > 1
                    else (_lines[0] if _lines else f"rc={res.returncode}")
                )
                if is_transient_rclone_stderr(err):
                    raise RcloneTransientError(
                        f"rclone transient failure (rc={res.returncode}): {last_err}"
                    )
                raise RuntimeError(f"rclone failed (rc={res.returncode}): {last_err}")
