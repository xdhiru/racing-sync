"""MOVING phase shape mixin: verified-file sort, purge and layout moves.

Split out of coordinator_move.py. Owns the per-file classification that
builds the client-verified-complete set, the incomplete/temp purges that
must precede a move, and the layout-specific move paths (multi-batch
leftovers, single file, verified folder, siblings, mixed batches).

Kept self-contained on leaf imports so nothing here imports
``racing_sync.coordinator_move`` (that module owns ``MoveMixin``, whose
base chain includes this mixin). Cross-half calls go through ``self``.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import stat
import time
from pathlib import Path

from .batcher import files_from_names, make_batches
from .classifier import Classification
from .clients.abstract import TorrentFile
from .coordinator_paths import _safe_ssd_join, left_on_disk_many
from .io_bounds import rpc
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class MoveShapesMixin:
    """Verified-move shape machinery (duck-typed ``self``)."""

    def _sort_move_files(
        self, cls_files: list[TorrentFile], src_dir: Path
    ) -> tuple[
        list[TorrentFile],
        list[TorrentFile],
        list[TorrentFile],
        set[str],
        list[tuple[str, int, bool, object]],
    ]:
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
                missing_on_disk.append(self._missing_disk_entry(f))
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
                self._classify_complete_file(
                    f,
                    file_path,
                    src_dir,
                    completed_files,
                    incomplete_files,
                    uncertain_files,
                    oversized_names,
                )
            else:
                self._classify_incomplete_file(f, file_path, incomplete_files, uncertain_files)
        if uncertain_files:
            log.info(
                "leaving %d unverified file(s) for folder wipe (not individually "
                "deleted, never moved): e.g. %s",
                len(uncertain_files),
                uncertain_files[0].name,
            )
        if missing_on_disk:
            log.warning(
                "move found %d file(s) missing on SSD (e.g. %s); they move nowhere "
                "and the fuse gate must still pass before re-add",
                len(missing_on_disk),
                missing_on_disk[0][0],
            )

        return (
            completed_files,
            incomplete_files,
            uncertain_files,
            oversized_names,
            missing_on_disk,
        )

    def _missing_disk_entry(self, f: TorrentFile) -> tuple[str, int, bool, object]:
        try:
            _complete = (f.progress or 0.0) >= 0.999
        except (TypeError, ValueError, AttributeError):
            _complete = False
        try:
            _size = int(f.size_bytes or 0)
        except (TypeError, ValueError):
            _size = 0
        return (f.name, _size, _complete, getattr(f, "priority", None))

    def _classify_complete_file(
        self,
        f: TorrentFile,
        file_path: Path,
        src_dir: Path,
        completed_files: list[TorrentFile],
        incomplete_files: list[TorrentFile],
        uncertain_files: list[TorrentFile],
        oversized_names: set[str],
    ) -> None:
        try:
            # Single stat: is_file()+stat() doubles syscalls per
            # file on the hot move path.
            _st = file_path.stat()
            _on_disk = _st.st_size if stat.S_ISREG(_st.st_mode) else -1
        except OSError as e:
            # Transient stat failure (EACCES, ESTALE, fuse blip, AV
            # lock) says nothing about the bytes: classifying the
            # file as "short" would put it in incomplete_files and
            # step 3 would unlink a fully downloaded file. Treat it
            # as unverifiable instead — never moved, never deleted
            # (selected files also block the wipe in 5b).
            log.warning(
                "could not stat file %s on SSD (%s); leaving it "
                "unverified (never moved, never deleted)",
                f.name,
                e,
            )
            uncertain_files.append(f)
            return
        if _on_disk == (f.size_bytes or 0):
            completed_files.append(f)
            return
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
                f.name,
                max(_on_disk, 0),
                f.size_bytes,
            )
            uncertain_files.append(f)
            return
        log.warning(
            "file %s reports complete but is short on SSD (%d/%d B); not moving",
            f.name,
            max(_on_disk, 0),
            f.size_bytes,
        )
        incomplete_files.append(f)
        return

    def _classify_incomplete_file(
        self,
        f: TorrentFile,
        file_path: Path,
        incomplete_files: list[TorrentFile],
        uncertain_files: list[TorrentFile],
    ) -> None:
        try:
            on_disk = file_path.stat().st_size
        except OSError as e:
            # Same rule for the incomplete branch: a stat failure must
            # not silently drop the file to the folder wipe unverified.
            log.warning(
                "could not stat file %s on SSD (%s); leaving it "
                "unverified (never moved, never deleted)",
                f.name,
                e,
            )
            uncertain_files.append(f)
            return
        if on_disk < (f.size_bytes or 0):
            incomplete_files.append(f)
        else:
            # Preallocated-but-incomplete (or laggy progress): move nothing
            # and delete nothing individually — left for the folder wipe.
            uncertain_files.append(f)

    async def _purge_incomplete_files(
        self,
        h: str,
        src_dir: Path,
        incomplete_files: list[TorrentFile],
    ) -> None:
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
                    "dest post-list checking state",
                )
                _post_state = str((_post[0].state if _post else "") or "")
                _post_checking = _post_state.lower().startswith("check")
            except Exception:
                _post_checking = False
            if _post_checking:
                log.info(
                    "client hash check started mid-move for %s; keeping %d "
                    "incomplete file(s) this tick without deleting",
                    h[:10],
                    len(incomplete_files),
                )
        for f in [] if _post_checking else incomplete_files:
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
                    f.name,
                    file_path.stat().st_size,
                    f.size_bytes,
                )
                try:
                    if file_path.is_file():
                        file_path.unlink()
                    elif file_path.is_dir():
                        shutil.rmtree(file_path, ignore_errors=True)
                except OSError as e:
                    log.warning("failed removing incomplete file %s: %s", f.name, e)

    async def _purge_temp_files(
        self,
        cls_files: list[TorrentFile],
        folder: Path | None,
        src_dir: Path,
    ) -> None:
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
                            if (
                                resolved_tmp != src_dir.resolve()
                                and not resolved_tmp.is_relative_to(src_dir.resolve())
                            ):
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

    async def _move_multi_batch_leftovers(
        self,
        ts: TorrentState,
        src_dir: Path,
        folder: Path | None,
        completed_files: list[TorrentFile],
        remote: str,
    ) -> bool:
        _top_join = _safe_ssd_join(src_dir, ts.source_name)
        _top_exists = _top_join is not None and _top_join.exists()
        local_folder = (
            folder if (folder and folder.exists()) else (_top_join if _top_exists else None)
        )
        remaining_payload = self._remaining_payload(local_folder, src_dir)

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
                        ts.source_name,
                        src_dir,
                    )
                    self._park_moving(
                        ts,
                        "leftover move scope indeterminate; staying in MOVING without moving",
                    )
                    return False
                leftover_files = self._verified_names_under(src_dir, completed_files, top)
                if not leftover_files:
                    log.info(
                        "multi-batch torrent %s: no verified-complete leftovers to move "
                        "(%d remaining file(s) are incomplete boundary data); skipping remote move",
                        ts.source_name,
                        len(remaining_payload),
                    )
                else:
                    log.info(
                        "multi-batch torrent %s: moving %d verified-complete leftover file(s) "
                        "(batch %d/%d, %d remaining file(s) total)",
                        ts.source_name,
                        len(leftover_files),
                        ts.batch_index,
                        ts.batches_total,
                        len(remaining_payload),
                    )
                    await self._rclone_move(src_dir, remote, ts, files_from=leftover_files)
                    # Same 0-transfer hazard as batch moves (rclone exits 0
                    # even when it transferred nothing): proceeding to the
                    # folder wipe below would destroy unmoved data. Stay MOVING.
                    stuck = await asyncio.to_thread(left_on_disk_many, src_dir, leftover_files)
                    if stuck:
                        self._park_moving(
                            ts,
                            f"leftover sweep moved nothing "
                            f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                            f"staying in MOVING without wiping",
                        )
                        return False
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
        return True

    def _remaining_payload(self, local_folder: Path | None, src_dir: Path) -> list[Path]:
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
        return remaining_payload

    def _verified_names_under(
        self, src_dir: Path, completed_files: list[TorrentFile], top: str
    ) -> list[str]:
        names: list[str] = []
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
                names.append(norm)
        return files_from_names(names)

    async def _single_file_verified_or_park(
        self,
        ts: TorrentState,
        h: str,
        cls: Classification,
        cls_files: list[TorrentFile],
        src_dir: Path,
        local: Path,
        completed_files: list[TorrentFile],
    ) -> tuple[bool, Path | None]:
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
        verified_paths, local_real = self._single_verified_paths(src_dir, local, completed_files)
        if local_real is not None and local_real in verified_paths:
            return True, local_real
        detail, _full_on_disk = self._single_file_detail(cls_files, src_dir, local, local_real)
        _check_state_now = await self._move_check_state(h)
        if _check_state_now.lower().startswith("check"):
            self._heartbeat_recheck_holder(ts)
            self._park_moving(
                ts,
                f"client hash check running ({_check_state_now}); "
                f"waiting it out without touching it",
                quiet=True,
            )
            return False, None
        if not self._guard_completion_flush(ts, _full_on_disk, _check_state_now):
            return False, None
        _holder_blocked, _already_rechecked = await self._maybe_settle_recheck(ts, h, _full_on_disk)
        if not await self._resume_or_park_single(
            ts, h, cls, local, detail, _full_on_disk, _holder_blocked, _already_rechecked
        ):
            return False, None
        return True, local_real

    def _single_verified_paths(
        self, src_dir: Path, local: Path, completed_files: list[TorrentFile]
    ) -> tuple[set[Path], Path | None]:
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
        return verified_paths, local_real

    def _single_file_detail(
        self, cls_files: list[TorrentFile], src_dir: Path, local: Path, local_real: Path | None
    ) -> tuple[str, bool]:
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
                                on_disk == (f.size_bytes or 0) and (f.size_bytes or 0) > 0
                            )
                        except (TypeError, ValueError):
                            _full_on_disk = False
                        break
                except OSError:
                    continue
        except Exception:
            detail = ""
        return detail, _full_on_disk

    async def _move_check_state(self, h: str) -> str:
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
                "dest check-guard state",
            )
            if _cur:
                _check_state_now = str(getattr(_cur[0], "state", "") or "")
                _check_now = _check_state_now.lower().startswith("check")
        except Exception:
            _check_now = False
        return _check_state_now

    def _guard_completion_flush(
        self, ts: TorrentState, full_on_disk: bool, check_state_now: str
    ) -> bool:
        # Completion-flush guard (single-file settle only): full
        # bytes on disk while the client still reports an
        # actively-downloading state is libtorrent's 100% flush
        # pending, stretched to minutes on starved disks — not
        # per-file reporting lag. Forcing a recheck here aborts
        # the flush and wedges the single global hash-check
        # slot, queueing every later row as "checking resume
        # data". Park quietly (no recheck, no resume) and let
        # the flush land; the next tick verifies normally. Only
        # full-byte rows take this lane — short files still
        # fall through to the resume path below. Unknowable
        # states fail open toward the historic recheck.
        if full_on_disk:
            try:
                _flush_state = str(check_state_now or "").lower().replace(" ", "").replace("_", "")
            except Exception:
                _flush_state = ""
            if _flush_state in ("downloading", "stalleddl", "forceddl"):
                self._park_moving(
                    ts,
                    f"client still finalizing download "
                    f"({check_state_now}); waiting for the "
                    f"completion flush before verifying",
                    quiet=True,
                )
                return False
        return True

    async def _maybe_settle_recheck(
        self, ts: TorrentState, h: str, full_on_disk: bool
    ) -> tuple[bool, bool]:
        try:
            _rh = (ts.source_infohash or "").lower()
        except Exception:
            _rh = ""
        try:
            _rc0 = getattr(self, "_settle_rechecked", None)
            _already_rechecked = bool(_rh and isinstance(_rc0, set) and _rh in _rc0)
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
        if full_on_disk and not _already_rechecked:
            try:
                _now_m2 = time.monotonic()
                _holder = getattr(self, "_recheck_in_flight", None)
                if (
                    isinstance(_holder, (list, tuple))
                    and len(_holder) == 2
                    and _holder[0]
                    and _holder[0] != _rh
                    and isinstance(_holder[1], (int, float))
                    and _now_m2 - float(_holder[1]) < 1800.0
                ):
                    _holder_blocked = True
            except Exception:
                _holder_blocked = False
        if full_on_disk and not _already_rechecked and not _holder_blocked:
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
                    ts.source_name[:60],
                    e,
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
                        _rh,
                        time.monotonic(),
                    ]
                except Exception:
                    pass
        return _holder_blocked, _already_rechecked

    async def _resume_or_park_single(
        self,
        ts: TorrentState,
        h: str,
        cls: Classification,
        local: Path,
        detail: str,
        full_on_disk: bool,
        holder_blocked: bool,
        already_rechecked: bool,
    ) -> bool:
        if holder_blocked:
            # Another row's check is running: issuing ours would
            # pile onto the same starved pool, and resuming
            # could abort theirs. Park quietly; the holder
            # clears on settle (or ages out after 30 min).
            resumed = False
            _resume_note = "not resumed (another row's hash check in flight; waiting turn),"
        elif (not full_on_disk) or (full_on_disk and already_rechecked):
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
                    cls.single_file,
                    ts.source_name,
                    e,
                )
            _resume_note = "resumed to finish downloading," if resumed else "resume failed,"
        else:
            resumed = False
            _resume_note = "not resumed (bytes present; hash check will verify),"
        if resumed:
            # Bytes still downloading: hand back to the download
            # loop instead of flapping pause/resume in MOVING.
            log.warning(
                "single file %s for %s is not client-verified complete%s; "
                "resumed to finish downloading",
                cls.single_file,
                ts.source_name,
                detail,
            )
            self._release_recheck_holder(ts)
            self.transition(ts, State.DOWNLOADING)
            return False
        try:
            _mp = getattr(self, "_moving_parks", None)
            _pn = (
                int(_mp.get((ts.source_infohash or "").lower(), 0) or 0)
                if isinstance(_mp, dict)
                else 0
            )
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
            cls.single_file,
            ts.source_name,
            detail,
            _resume_note,
        )
        self._park_moving(
            ts,
            f"single file {cls.single_file} not verified complete{detail}; "
            f"{_resume_note} "
            f"staying in MOVING without moving",
        )
        return False
        return True

    async def _move_verified_folder(
        self,
        ts: TorrentState,
        h: str,
        src_dir: Path,
        folder: Path | None,
        completed_files: list[TorrentFile],
        incomplete_files: list[TorrentFile],
        uncertain_files: list[TorrentFile],
        remote: str,
    ) -> bool:
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
        folder_names = self._verified_names_under(src_dir, completed_files, top)
        if folder_names:
            await self._rclone_move(src_dir, remote, ts, files_from=folder_names)
            # A 0-transfer folder move must not proceed to the wipe below.
            stuck = await asyncio.to_thread(left_on_disk_many, src_dir, folder_names)
            if stuck:
                self._park_moving(
                    ts,
                    f"folder move left {len(stuck)} file(s) on disk "
                    f"(e.g. {stuck[0]}); staying in MOVING without wiping",
                )
                return False
        else:
            if not await self._resume_unverified_folder(
                ts, h, local, completed_files, incomplete_files, uncertain_files
            ):
                return False
        return True

    async def _resume_unverified_folder(
        self,
        ts: TorrentState,
        h: str,
        local: Path | None,
        completed_files: list[TorrentFile],
        incomplete_files: list[TorrentFile],
        uncertain_files: list[TorrentFile],
    ) -> bool:
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
                    local,
                    ts.source_name,
                    e,
                )
            if resumed:
                # Bytes still downloading: hand back to the
                # download loop instead of flapping pause/resume
                # in MOVING every tick.
                log.warning(
                    "folder %s for %s not verified complete; resumed to finish downloading",
                    local,
                    ts.source_name,
                )
                self._release_recheck_holder(ts)
                self.transition(ts, State.DOWNLOADING)
                return False
            log.warning(
                "folder %s for %s has no verified-complete files "
                "(%d incomplete, %d unverified); resume failed, "
                "staying in MOVING without moving",
                local,
                ts.source_name,
                len(incomplete_files),
                len(uncertain_files),
            )
            self._park_moving(
                ts,
                f"folder {ts.source_name} not verified complete "
                f"({len(incomplete_files)} incomplete, "
                f"{len(uncertain_files)} unverified); "
                f"resume failed, staying in MOVING without moving",
            )
            return False
        log.info(
            "folder %s for %s has no verified-complete files to move "
            "(already remote or incomplete boundary data)",
            local,
            ts.source_name,
        )
        return True

    async def _move_single_file_layout(
        self,
        ts: TorrentState,
        cls: Classification,
        src_dir: Path,
        local: Path,
        local_real: Path | None,
        completed_files: list[TorrentFile],
        remote: str,
    ) -> bool:
        # Single-file layout: folder-wrapped singles (Top/file.mkv)
        # must move via files_from from src_dir so the top dir
        # survives on the remote. A bare `rclone move <file>
        # <remote:>` strips it (file lands in the remote root) and
        # the fuse gate then parks forever on Top/file.mkv. Flat
        # singles (no "/") keep the bare move.
        _single_wrapped, _single_norm = self._wrapped_single_names(cls, src_dir, local, local_real)
        _wrapped_done = False
        if _single_wrapped and _single_norm:
            _wrapped_names = self._wrapped_layout_names(
                completed_files, src_dir, local_real, _single_norm
            )
            if _wrapped_names:
                log.info(
                    "moving %d verified file(s) for %s preserving torrent layout (e.g. %s)",
                    len(_wrapped_names),
                    ts.source_name[:60],
                    _wrapped_names[0],
                )
                await self._rclone_move(src_dir, remote, ts, files_from=_wrapped_names)
                _wrapped_stuck = await asyncio.to_thread(left_on_disk_many, src_dir, _wrapped_names)
                if _wrapped_stuck:
                    self._park_moving(
                        ts,
                        f"single-file layout move left {len(_wrapped_stuck)} file(s) "
                        f"on disk (e.g. {_wrapped_stuck[0]}); staying in "
                        f"MOVING without wiping",
                    )
                    return False
            else:
                self._park_moving(
                    ts,
                    f"single file {cls.single_file} has no verified "
                    f"layout names; staying in MOVING without moving",
                )
                return False
            # Layout move done (main + siblings together); skip the
            # bare/sibling path below.
            _wrapped_done = True
        if not _wrapped_done:
            await self._rclone_move(local, remote, ts)
            try:
                _left = local.is_symlink() or local.exists()
            except OSError:
                _left = True
            if _left:
                self._park_moving(
                    ts,
                    f"single-file move left {local} on disk; staying in MOVING without wiping",
                )
                return False
            if not await self._move_verified_siblings(
                ts, src_dir, local_real, completed_files, remote
            ):
                return False
        return True

    def _wrapped_single_names(
        self, cls: Classification, src_dir: Path, local: Path, local_real: Path | None
    ) -> tuple[bool, str]:
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
        return _single_wrapped, _single_norm

    def _wrapped_layout_names(
        self,
        completed_files: list[TorrentFile],
        src_dir: Path,
        local_real: Path | None,
        single_norm: str,
    ) -> list[str]:
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
                _main_names = [single_norm]
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
            _wrapped_names = files_from_names([n for n in ([*_main_names, *_sib_names]) if n])
        except Exception:
            _wrapped_names = []
        return _wrapped_names

    async def _move_verified_siblings(
        self,
        ts: TorrentState,
        src_dir: Path,
        local_real: Path | None,
        completed_files: list[TorrentFile],
        remote: str,
    ) -> bool:
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
            _sib_names = files_from_names([n for n in _sib_names if n])
        except Exception:
            _sib_names = []
        if _sib_names:
            log.info(
                "moving %d verified sibling file(s) for %s (e.g. %s)",
                len(_sib_names),
                ts.source_name[:60],
                _sib_names[0],
            )
            await self._rclone_move(src_dir, remote, ts, files_from=_sib_names)
            _sib_stuck = await asyncio.to_thread(left_on_disk_many, src_dir, _sib_names)
            if _sib_stuck:
                self._park_moving(
                    ts,
                    f"sibling move left {len(_sib_stuck)} file(s) "
                    f"on disk (e.g. {_sib_stuck[0]}); staying in "
                    f"MOVING without wiping",
                )
                return False
        return True

    async def _move_single_or_folder(
        self,
        ts: TorrentState,
        h: str,
        cls: Classification,
        cls_files: list[TorrentFile],
        src_dir: Path,
        folder: Path | None,
        completed_files: list[TorrentFile],
        incomplete_files: list[TorrentFile],
        uncertain_files: list[TorrentFile],
        remote: str,
        branch_kind: str,
    ) -> bool:
        local: Path | None
        if branch_kind in ("movie", "episode") and cls.single_file:
            local = _safe_ssd_join(src_dir, cls.single_file)
            if local is None or not local.exists():
                if folder is not None:
                    _alt = _safe_ssd_join(folder, cls.single_file)
                    if _alt is not None and _alt.exists():
                        local = _alt
                    elif self._single_on_fuse(cls_files, cls.single_file, ts):
                        log.info("single file %s already on remote; skipping move", cls.single_file)
                        local = None
                    else:
                        raise FileNotFoundError(
                            f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}"
                        )
                elif self._single_on_fuse(cls_files, cls.single_file, ts):
                    log.info("single file %s already on remote; skipping move", cls.single_file)
                    local = None
                else:
                    raise FileNotFoundError(
                        f"completed {cls.kind} file not found on SSD: {src_dir}/{cls.single_file}"
                    )
        elif branch_kind in ("season", "unknown") or (
            branch_kind == "movie" and not cls.single_file
        ):
            if not await self._move_verified_folder(
                ts, h, src_dir, folder, completed_files, incomplete_files, uncertain_files, remote
            ):
                return False
            local = None
        else:
            cand = _safe_ssd_join(src_dir, ts.source_name)
            if cand is not None and cand.exists():
                local = cand
            else:
                raise FileNotFoundError(
                    f"completed content not found on SSD: {src_dir}/{ts.source_name}"
                )
        local_real: Path | None = None
        if (
            branch_kind in ("movie", "episode")
            and cls.single_file
            and isinstance(local, Path)
            and local.is_file()
        ):
            proceed, local_real = await self._single_file_verified_or_park(
                ts, h, cls, cls_files, src_dir, local, completed_files
            )
            if not proceed:
                return False
        if local is None:
            pass
        elif isinstance(local, Path) and local.is_dir():
            # Unreachable: folder layouts move per-file above by design.
            raise RuntimeError(f"refusing bare folder move of {local}")
        else:
            if not await self._move_single_file_layout(
                ts, cls, src_dir, local, local_real, completed_files, remote
            ):
                return False
        return True

    async def _move_mixed_batches(
        self,
        ts: TorrentState,
        cls: Classification,
        src_dir: Path,
        completed_files: list[TorrentFile],
        remote: str,
        branch_kind: str,
    ) -> bool:
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
            # In-memory cursor only: persisting a mid-loop index here
            # would let the next tick re-derive batches fresh (cap or
            # file list may have shifted) under a stale index pointing
            # at different episodes. The loop replays idempotently —
            # moved batches skip via fuse — so a park loses no progress.
            ts.batch_index = i
            ts.batches_total = len(batches)
            skip = await self._fuse_skipped(
                [(e.file_name, e.size_bytes) for e in batch.episodes],
                branch_kind,
                subdir=self._seed_subdir_for(ts),
            )
            skip_norm = {(s or "").replace("\\", "/").strip("/") for s in skip}
            # Same guarantee as the strict branch: only a file the client
            # reports complete AND that stats at full size on SSD may move.
            # batch.file_names() also lists files step 3 left in place
            # during an in-flight hash check — those can be piece-boundary
            # partials, and --files-from-raw would upload them as-is.
            verified = set(files_from_names([f.name or "" for f in completed_files]))
            batch_names = batch.file_names()
            unverified = [n for n in batch_names if n not in verified and n not in skip_norm]
            if unverified:
                self._park_moving(
                    ts,
                    f"mixed-torrent batch {i + 1}/{len(batches)} has "
                    f"{len(unverified)} unverified file(s) (e.g. "
                    f"{unverified[0]}); staying in MOVING without moving",
                )
                return False
            names = [n for n in batch_names if n not in skip_norm]
            if not names:
                log.info(
                    "mixed batch %d/%d for %s already on remote; skipping move",
                    i + 1,
                    len(batches),
                    ts.source_name,
                )
                continue
            await self._rclone_move(
                src_dir,
                remote,
                ts,
                files_from=names,
                extra=self.rt.transfer.batch_extra_flags,
            )
            stuck = await asyncio.to_thread(left_on_disk_many, src_dir, names)
            if stuck:
                self._park_moving(
                    ts,
                    f"mixed-torrent batch move moved nothing "
                    f"({len(stuck)} file(s) still on disk, e.g. {stuck[0]}); "
                    f"staying in MOVING without wiping",
                )
                return False
        return True
