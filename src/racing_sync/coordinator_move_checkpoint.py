"""Durable MOVING checkpoint mixin: stage machine, plans and folder wipe.

Split out of coordinator_move.py. Owns the crash-safe checkpoint sequence
(`moved` -> `entry_deleted` -> `wiped`), the compact relative-path cleanup
plan used to replay it, and the per-file wipe that spares foreign files.

Kept self-contained on leaf imports so nothing here imports
``racing_sync.coordinator_move`` (that module owns ``MoveMixin``, whose base
is this mixin).
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from .clients.abstract import TorrentFile
from .coordinator_paths import _safe_ssd_join
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

# Durable MOVING checkpoints. The order is delete -> cleanup -> transition
# (kept from the historic ordering), so a crash at any point can be replayed
# idempotently without re-listing/re-pausing a client entry that may be gone.
_MOVE_STAGE_MOVED = "moved"
_MOVE_STAGE_ENTRY_DELETED = "entry_deleted"
_MOVE_STAGE_WIPED = "wiped"
_MOVE_STAGES = frozenset(
    {
        _MOVE_STAGE_MOVED,
        _MOVE_STAGE_ENTRY_DELETED,
        _MOVE_STAGE_WIPED,
    }
)
_MOVE_PLAN_VERSION = 1


class CheckpointMixin:
    """Durable move checkpoints (duck-typed ``self``)."""

    # ---- durable MOVING checkpoint ----

    def _persist_move_stage(self, ts: TorrentState, stage: str, plan: str | None = None) -> bool:
        """Checkpoint a move stage under a narrow MOVING-guarded write.

        Returns False (and leaves the in-memory row at its previous stage)
        when the row moved underneath us, is tombstoned, or the write fails.
        Callers must then park without any further destructive action.
        """
        stage = (stage or "").strip().lower()
        prev_stage = str(getattr(ts, "move_stage", "") or "")
        prev_plan = str(getattr(ts, "move_plan", "") or "")
        new_plan = prev_plan if plan is None else str(plan)
        ts.move_stage = stage
        ts.move_plan = new_plan
        _up = getattr(self.store, "update_columns", None)
        if not callable(_up):
            ts.move_stage, ts.move_plan = prev_stage, prev_plan
            return False
        try:
            _nv = _up(
                (ts.source_infohash or ""),
                {"move_stage": stage, "move_plan": new_plan},
                expected_state=State.MOVING,
            )
        except Exception:  # noqa: BLE001
            ts.move_stage, ts.move_plan = prev_stage, prev_plan
            return False
        if isinstance(_nv, int):
            if _nv > 0:
                try:
                    ts.version = _nv
                except Exception:
                    pass
                return True
            # Row vanished / state changed / tombstoned: never overwrite a
            # concurrent state. Keep memory at the last durable value.
            ts.move_stage, ts.move_plan = prev_stage, prev_plan
            return False
        # Non-int return from a test double: best-effort success.
        return True

    def _configured_ssd_roots(self) -> list[Path]:
        roots: list[Path] = []
        for raw in (
            getattr(self.rt.storage, "path", None),
            getattr(self.rt.storage, "save_path", None),
        ):
            try:
                if not raw:
                    continue
                p = Path(raw).resolve()
            except (TypeError, ValueError, OSError):
                continue
            if p not in roots:
                roots.append(p)
        return roots

    def _move_checkpoint_root(self, ts: TorrentState) -> Path:
        """Resolve+validate the SSD root a persisted move plan refers to."""
        _raw = getattr(ts, "save_path", "") or ""
        if not _raw:
            _raw = getattr(self.rt.storage, "save_path", "")
        try:
            root = Path(_raw).resolve()
        except (TypeError, ValueError, OSError) as e:
            raise ValueError(f"invalid SSD root {_raw!r}: {e}") from e
        roots = self._configured_ssd_roots()
        if not roots:
            raise ValueError("no configured SSD root is available")
        if not any(root == r or root.is_relative_to(r) for r in roots):
            raise ValueError(f"SSD root {root} is outside the configured roots")
        try:
            if not root.exists():
                raise ValueError(f"SSD root {root} is missing/unavailable")
        except OSError as e:
            raise ValueError(f"SSD root {root} is unreadable: {e}") from e
        return root

    @staticmethod
    def _plan_rel_path(root: Path, rel: object, label: str) -> Path:
        """Join+validate one relative plan path under `root` (no symlinks)."""
        if not isinstance(rel, str) or not rel:
            raise ValueError(f"{label} path is not a non-empty string")
        joined = _safe_ssd_join(root, rel)
        if joined is None:
            raise ValueError(f"{label} path {rel!r} is absolute or traverses outside the SSD root")
        norm = rel.replace("\\", "/").strip("/")
        cur = root
        try:
            for part in norm.split("/"):
                if not part:
                    continue
                cur = cur / part
                if cur.is_symlink():
                    raise ValueError(f"{label} path {rel!r} contains a symlink")
            resolved = joined.resolve()
        except OSError as e:
            raise ValueError(f"{label} path {rel!r} is unreadable: {e}") from e
        if resolved != root and not resolved.is_relative_to(root):
            raise ValueError(f"{label} path {rel!r} escapes the SSD root")
        return joined

    def _load_move_plan(
        self,
        ts: TorrentState,
        stage: str,
    ) -> tuple[Path, dict] | None:
        """Parse, schema-check and path-validate the persisted move plan.

        Any failure parks the row with an operator-visible reason and
        returns None: there is no safe cleanup without a trustworthy plan.
        """
        raw = str(getattr(ts, "move_plan", "") or "").strip()
        if not raw:
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} has no plan; refusing unsafe wipe",
            )
            return None
        try:
            plan = json.loads(raw)
        except Exception as e:  # noqa: BLE001
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} plan is corrupt (invalid JSON: {e}); "
                f"refusing unsafe wipe",
            )
            return None
        if not isinstance(plan, dict):
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} plan is corrupt (not an object); refusing unsafe wipe",
            )
            return None
        try:
            version = plan.get("version")
            if version != _MOVE_PLAN_VERSION:
                raise ValueError(f"unsupported plan version {version!r}")
            layout = plan.get("layout")
            if layout not in ("folder", "flat"):
                raise ValueError(f"unknown layout {layout!r}")
            keep = plan.get("keep", [])
            flat = plan.get("flat", [])
            folder = plan.get("folder", "")
            if not isinstance(keep, list) or not all(isinstance(v, str) for v in keep):
                raise ValueError("keep is not a list of strings")
            if not isinstance(flat, list) or not all(isinstance(v, str) for v in flat):
                raise ValueError("flat is not a list of strings")
            if not isinstance(folder, str):
                raise ValueError("folder is not a string")
            if layout == "folder" and not folder.strip("/").strip():
                raise ValueError("folder layout has no folder")
        except ValueError as e:
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} plan is corrupt ({e}); refusing unsafe wipe",
            )
            return None
        try:
            root = self._move_checkpoint_root(ts)
        except ValueError as e:
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} cannot be replayed: {e}; "
                f"refusing cleanup without the SSD root",
            )
            return None
        try:
            if layout == "folder":
                self._plan_rel_path(root, folder, "folder")
            for rel in keep:
                self._plan_rel_path(root, rel, "keep")
            for rel in flat:
                self._plan_rel_path(root, rel, "flat")
        except ValueError as e:
            self._park_moving(
                ts,
                f"move checkpoint {stage!r} plan is unsafe ({e}); refusing unsafe wipe",
            )
            return None
        return root, {
            "layout": layout,
            "folder": folder,
            "keep": list(keep),
            "flat": list(flat),
        }

    def _build_move_plan(
        self,
        src_dir: Path,
        folder: Path | None,
        keep: set[str],
        flat: list[str],
    ) -> str:
        """Build+validate the compact relative-path cleanup plan."""
        src_real = src_dir.resolve()

        def _rel(p: Path) -> str | None:
            try:
                r = p.resolve().relative_to(src_real)
            except (ValueError, OSError):
                return None
            return str(r).replace("\\", "/")

        folder_rel = ""
        if folder is not None:
            try:
                if folder.exists() and folder.resolve() != src_real:
                    folder_rel = _rel(folder) or ""
            except OSError:
                folder_rel = ""
        layout = "folder" if folder_rel else "flat"
        keep_rel = sorted({k for k in (keep or set()) if isinstance(k, str) and k})
        flat_rel: list[str] = []
        seen: set[str] = set()
        for rel in flat or []:
            if not isinstance(rel, str) or not rel or rel in seen:
                continue
            if _safe_ssd_join(src_dir, rel) is None:
                continue
            seen.add(rel)
            flat_rel.append(rel)
        plan = {
            "version": _MOVE_PLAN_VERSION,
            "layout": layout,
            "folder": folder_rel if layout == "folder" else "",
            "keep": keep_rel,
            "flat": flat_rel if layout == "flat" else [],
        }
        for rel in [*plan["keep"], *plan["flat"]]:
            if _safe_ssd_join(src_dir, rel) is None:
                raise ValueError(f"unsafe move plan path {rel!r}")
        if plan["folder"] and _safe_ssd_join(src_dir, plan["folder"]) is None:
            raise ValueError(f"unsafe move plan folder {plan['folder']!r}")
        return json.dumps(plan, separators=(",", ":"), sort_keys=True)

    def _finish_moving_tracking(self, ts: TorrentState) -> None:
        """Drop the check-watch and settle-recheck hold before RE_ADDING."""
        try:
            _csd = getattr(self, "_checking_track", None)
            if isinstance(_csd, dict):
                _csd.pop((ts.source_infohash or "").lower(), None)
        except Exception:
            pass
        self._release_recheck_holder(ts)

    async def _cleanup_move_checkpoint(
        self,
        ts: TorrentState,
        loaded: tuple[Path, dict] | None = None,
    ) -> bool:
        """Run (or retry) the persisted cleanup. True only when fully done."""
        if loaded is None:
            stage = (ts.move_stage or "").strip().lower() or _MOVE_STAGE_MOVED
            loaded = self._load_move_plan(ts, stage)
            if loaded is None:
                return False
        root, plan = loaded
        stage = (ts.move_stage or "").strip().lower() or _MOVE_STAGE_ENTRY_DELETED
        if plan["layout"] == "folder":
            folder = _safe_ssd_join(root, plan["folder"])
            if folder is None:
                self._park_moving(
                    ts,
                    f"move checkpoint {stage!r} folder is unsafe; refusing unsafe wipe",
                )
                return False
            try:
                if not folder.exists():
                    return True
                if plan["keep"]:
                    if not await self._wipe_folder_except(folder, set(plan["keep"]), root):
                        self._park_moving(
                            ts,
                            f"folder wipe left file(s) it owns under "
                            f"{plan['folder']}; staying at entry_deleted "
                            f"without advancing",
                        )
                        return False
                else:
                    log.info("deleting content folder after move: %s", folder)
                    from . import coordinator as _coordinator

                    await _coordinator.wipe_local_tree(
                        folder,
                        base_dir=[self.rt.storage.path, Path(self.rt.storage.save_path)],
                    )
            except OSError as e:
                self._park_moving(
                    ts,
                    f"folder wipe failed after move ({e}); staying at "
                    f"entry_deleted without advancing",
                )
                return False
            return True
        failed: list[str] = []
        for rel in plan["flat"]:
            p = _safe_ssd_join(root, rel)
            if p is None:
                failed.append(rel)
                continue
            try:
                if p.is_symlink():
                    failed.append(rel)
                    continue
                if not p.exists():
                    continue
                if not p.is_file():
                    failed.append(rel)
                    continue
                p.unlink()
                log.info("removed leftover shell %s after single move", rel)
            except OSError:
                failed.append(rel)
        if failed:
            self._park_moving(
                ts,
                f"flat cleanup left {len(failed)} file(s) it owns "
                f"(e.g. {failed[0]}); staying at entry_deleted without advancing",
            )
            return False
        return True

    async def _replay_move_checkpoint(self, ts: TorrentState, stage: str) -> None:
        """Replay a durable move stage without re-listing/pausing the client."""
        h = ts.dest_infohash or ts.source_infohash
        stage = (stage or "").strip().lower()
        if stage not in _MOVE_STAGES:
            self._park_moving(
                ts,
                f"unknown move checkpoint {stage!r}; refusing unsafe cleanup",
            )
            return
        loaded = None
        if stage in (_MOVE_STAGE_MOVED, _MOVE_STAGE_ENTRY_DELETED):
            loaded = self._load_move_plan(ts, stage)
            if loaded is None:
                return
        if stage == _MOVE_STAGE_MOVED:
            try:
                await self.dest_client.delete(h, delete_files=False)
            except Exception as e:  # noqa: BLE001
                log.warning("could not delete old torrent from client after move: %s", e)
                self._park_moving(
                    ts,
                    f"client entry delete failed after move ({e}); staying in "
                    f"MOVING with checkpoint 'moved'",
                )
                return
            if not self._persist_move_stage(ts, _MOVE_STAGE_ENTRY_DELETED):
                self._park_moving(
                    ts,
                    "could not persist move checkpoint 'entry_deleted'; "
                    "staying in MOVING without cleaning up",
                )
                return
            stage = _MOVE_STAGE_ENTRY_DELETED
        if stage == _MOVE_STAGE_ENTRY_DELETED:
            if not await self._cleanup_move_checkpoint(ts, loaded):
                return
            if not self._persist_move_stage(ts, _MOVE_STAGE_WIPED):
                self._park_moving(
                    ts,
                    "could not persist move checkpoint 'wiped'; "
                    "staying at entry_deleted without advancing",
                )
                return
            stage = _MOVE_STAGE_WIPED
        if stage == _MOVE_STAGE_WIPED:
            self._finish_moving_tracking(ts)
            self.transition(ts, self.rt.chain.after_move())

    def _persist_moved_plan(
        self,
        ts: TorrentState,
        src_dir: Path,
        folder: Path | None,
        uncertain_files: list[TorrentFile],
        oversized_names: set[str],
    ) -> bool:
        # 8. Build and persist the cleanup plan BEFORE deleting the client
        # entry. A failed checkpoint write parks with nothing destroyed.
        _flat_names: list[str] = []
        for _uf in uncertain_files:
            try:
                _unm = getattr(_uf, "name", "") or ""
                _up = _safe_ssd_join(src_dir, _unm)
                if _up is None:
                    continue
                _urel = str(_up.relative_to(src_dir)).replace("\\", "/")
                if _urel in oversized_names:
                    continue
                if _up.is_symlink() or not _up.is_file():
                    continue
                _flat_names.append(_urel)
            except (OSError, ValueError, AttributeError):
                continue
        try:
            _plan_json = self._build_move_plan(src_dir, folder, oversized_names, _flat_names)
        except Exception as e:  # noqa: BLE001
            self._park_moving(
                ts,
                f"could not build a safe move plan ({e}); staying in "
                f"MOVING without deleting or wiping",
            )
            return False
        if not self._persist_move_stage(ts, _MOVE_STAGE_MOVED, _plan_json):
            self._park_moving(
                ts,
                "could not persist the move checkpoint ('moved'); staying in "
                "MOVING without deleting or wiping",
            )
            return False
        return True

    async def _wipe_folder_except(self, folder: Path, keep: set[str], src_dir: Path) -> bool:
        """Wipe a moved folder but spare foreign (oversized) files.

        Deletes every file under `folder` except the keep set (SSD-relative
        posix names, matched exactly or as a parent dir), then removes dirs
        left empty. Returns False when an owned file could not be removed —
        callers must park at `entry_deleted` rather than claim a clean wipe.
        """
        log.info("wiping moved folder except %d foreign file(s): %s", len(keep), folder)

        def _kept(rel: str) -> bool:
            return any(rel == _k or _k.startswith(rel + "/") for _k in keep)

        def _sweep() -> bool:
            try:
                _paths = sorted(folder.rglob("*"), reverse=True)
            except OSError as e:
                log.warning("cannot list %s for per-file wipe: %s", folder, e)
                return False
            clean = True
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
                    if _kept(_rel):
                        continue
                    if _p.is_file():
                        try:
                            _p.unlink()
                        except OSError as e:
                            log.warning("could not remove %s: %s", _p, e)
                            clean = False
                    elif _p.is_dir():
                        try:
                            _p.rmdir()
                        except OSError:
                            pass
                except OSError:
                    clean = False
                    continue
            # Post-check: any file we own that survived (e.g. a failed
            # unlink that returned quietly) means the wipe is not done.
            try:
                for _p in folder.rglob("*"):
                    try:
                        if _p.is_symlink() or not _p.is_file():
                            continue
                        _rel = str(_p.relative_to(src_dir)).replace("\\", "/")
                    except (OSError, ValueError):
                        continue
                    if _rel and _rel != "." and not _kept(_rel):
                        clean = False
                        break
            except OSError:
                return False
            return clean

        try:
            return bool(await asyncio.to_thread(_sweep))
        except Exception as e:  # noqa: BLE001
            log.warning("per-file wipe of %s failed: %s", folder, e)
            return False
