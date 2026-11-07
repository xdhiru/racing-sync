"""Read-only task status views over live maps (P6 filter UI consumes these).

Each reader snapshots one lane — download progress from the ``_live``
maps, move progress from the rclone map, re-add/queue from row state —
without touching workers or the DB. SQLite stays the truth; these are
display projections only (same role as MLTB's ``*_status.py`` readers
over its task dict).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class TaskStatus(Protocol):
    """Display projection of one row's live lane."""

    label: str
    progress: float | None
    speed_bps: int
    eta_seconds: int | None
    state: str


@dataclass
class DownloadStatus:
    label: str = "Downloading"
    progress: float | None = None
    speed_bps: int = 0
    eta_seconds: int | None = None
    state: str = "downloading"


@dataclass
class MoveStatus:
    label: str = "Moving"
    progress: float | None = None
    speed_bps: int = 0
    eta_seconds: int | None = None
    state: str = "moving"


@dataclass
class ReaddStatus:
    label: str = "Re-adding"
    progress: float | None = None
    speed_bps: int = 0
    eta_seconds: int | None = None
    state: str = "readding"


@dataclass
class QueueStatus:
    label: str = "Queued"
    progress: float | None = None
    speed_bps: int = 0
    eta_seconds: int | None = None
    state: str = "queued"


def _num(value: object, default: float = 0.0) -> float:
    try:
        if isinstance(value, bool):
            return default
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def status_for(coord: object, ts: object) -> TaskStatus:
    """Pick the reader for a row's state and snapshot it. Never raises."""
    try:
        state_val = str(getattr(getattr(ts, "state", None), "value", "")
                        or "").lower()
    except Exception:
        state_val = ""
    try:
        h = str(getattr(ts, "source_infohash", "") or "").lower()
    except Exception:
        h = ""
    try:
        if state_val == "downloading":
            prog: float | None = None
            spd = 0
            try:
                _pm = getattr(coord, "live_progress_map", None)
                _sm = getattr(coord, "live_speed_map", None)
                _p = _pm() if callable(_pm) else {}
                _s = _sm() if callable(_sm) else {}
                if isinstance(_p, dict) and h in _p:
                    prog = _num(_p[h], -1.0)
                    prog = prog if 0.0 <= prog <= 1.0 else None
                if isinstance(_s, dict) and h in _s:
                    spd = max(0, int(_num(_s[h])))
            except Exception:
                pass
            return DownloadStatus(progress=prog, speed_bps=spd)
        if state_val == "moving":
            prog = None
            spd = 0
            eta: int | None = None
            try:
                _mm = getattr(coord, "rclone_move_progress_map", None)
                _m = _mm() if callable(_mm) else {}
                _rp = _m.get(h) if isinstance(_m, dict) else None
                if _rp is not None:
                    _pc = _num(getattr(_rp, "percent", 0.0), -1.0)
                    prog = _pc / 100.0 if 0.0 <= _pc <= 100.0 else None
                    spd = max(0, int(_num(
                        getattr(_rp, "speed_bps", 0))))
                    _e = int(_num(getattr(_rp, "eta_seconds", 0)))
                    eta = _e if _e > 0 else None
            except Exception:
                pass
            return MoveStatus(progress=prog, speed_bps=spd, eta_seconds=eta)
        if state_val == "re_adding":
            return ReaddStatus()
        if state_val in ("queued", "waiting_disk", "waiting_indexer",
                         "new"):
            _labels = {"queued": "Queued", "waiting_disk": "Wait SSD",
                       "waiting_indexer": "Wait indexer", "new": "New"}
            return QueueStatus(
                label=_labels.get(state_val, "Queued"), state=state_val)
        return QueueStatus(label=state_val or "unknown",
                           state=state_val or "unknown")
    except Exception:
        return QueueStatus()


def status_counts(coord: object, rows: list) -> dict[str, int]:
    """Count rows per status label (P6 filter badges). Never raises."""
    out: dict[str, int] = {}
    try:
        for ts in rows or []:
            try:
                _label = status_for(coord, ts).label
                out[_label] = out.get(_label, 0) + 1
            except Exception:
                continue
    except Exception:
        pass
    return out


__all__ = ["TaskStatus", "DownloadStatus", "MoveStatus", "ReaddStatus",
           "QueueStatus", "status_for", "status_counts"]
