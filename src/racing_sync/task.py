"""In-memory worker task handles (SQLite stays the truth).

One :class:`TaskHandle` per spawned worker complements the raw task sets:
a uniform ``cancel()`` (flag + wake + task-cancel + owned rclone abort)
replaces ad-hoc cancellation, and the ``event``/``force`` slots are the
park/wake primitives P3/P4 build on. Row state in SQLite is untouched —
handles only wrap the volatile worker side (same split MLTB uses between
its task dict and persistent store).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

#: Live handles by lowercase infohash. Bounded like the sibling maps.
_TASK_HANDLES_CAP = 5000


def short_gid(infohash: object) -> str:
    """12-char group id for an infohash (button/callback sized)."""
    try:
        return (str(infohash or "").strip().lower())[:12]
    except Exception:
        return ""


@dataclass(eq=False)
class TaskHandle:
    """Volatile handle for one worker task. Never raises."""

    infohash: str = ""
    gid: str = ""
    kind: str = "worker"
    event: asyncio.Event = field(default_factory=asyncio.Event)
    force: bool = False
    cancelled: bool = False
    # Lane-queue park flag (P4): an event-parked waiter keeps its worker
    # claim (no dupe spawns) but must NOT count as live lane usage —
    # live counters skip queued handles so freed slots wake waiters.
    queued: bool = False
    task: asyncio.Task | None = None
    tree_key: str | None = None
    progress: float | None = None
    speed_bps: int = 0
    eta_seconds: int = 0

    def cancel(self) -> bool:
        """Flag + wake + task-cancel + owned rclone abort.

        True when anything was live (a running task or an owned move
        tree); idempotent — a second call is a no-op False. The task
        cancel unwinds through the existing AbandonedError/quiet paths;
        the tree abort keeps a killed move from uploading/deleting
        behind the canceller (bounded terminate+reap; the guard drops
        only once the child is reaped).
        """
        try:
            if self.cancelled:
                return False
            self.cancelled = True
        except Exception:
            return False
        did = False
        try:
            self.event.set()
        except Exception:
            pass
        try:
            t = self.task
            if t is not None and not t.done():
                try:
                    t.cancel()
                except Exception:
                    pass
                did = True
        except Exception:
            pass
        try:
            if self.tree_key:
                from .rclone_ops import abort_move_tree

                try:
                    if abort_move_tree(self.tree_key):
                        did = True
                except Exception:
                    pass
        except Exception:
            pass
        return did


def handles_for(coord: object, infohash: str) -> list[TaskHandle]:
    """Live handles for a hash (copy). Never raises."""
    try:
        h = (infohash or "").strip().lower()
        if not h:
            return []
        _reg = getattr(coord, "_task_handles", None)
        if not isinstance(_reg, dict):
            return []
        _set = _reg.get(h)
        if not isinstance(_set, set):
            return []
        return [x for x in list(_set) if isinstance(x, TaskHandle)]
    except Exception:
        return []


def register_handle(coord: object, handle: TaskHandle) -> None:
    """Track a handle under its infohash (bounded). Never raises."""
    try:
        if not isinstance(handle, TaskHandle):
            return
        h = (handle.infohash or "").strip().lower()
        if not h:
            return
        _reg = getattr(coord, "_task_handles", None)
        if not isinstance(_reg, dict):
            _reg = {}
            try:
                coord._task_handles = _reg  # type: ignore[attr-defined]
            except Exception:
                return
        _set = _reg.get(h)
        if not isinstance(_set, set):
            _set = set()
            _reg[h] = _set
        _set.add(handle)
        if len(_reg) > _TASK_HANDLES_CAP:
            try:
                for _k in list(_reg.keys())[: len(_reg) - _TASK_HANDLES_CAP]:
                    if _k != h:
                        _reg.pop(_k, None)
            except Exception:
                pass
    except Exception:
        pass


def unregister_handle(coord: object, handle: TaskHandle) -> None:
    """Drop a finished handle. Never raises."""
    try:
        if not isinstance(handle, TaskHandle):
            return
        _reg = getattr(coord, "_task_handles", None)
        if not isinstance(_reg, dict):
            return
        h = (handle.infohash or "").strip().lower()
        _set = _reg.get(h)
        if isinstance(_set, set):
            try:
                _set.discard(handle)
            except Exception:
                pass
            if not _set:
                try:
                    _reg.pop(h, None)
                except Exception:
                    pass
    except Exception:
        pass
