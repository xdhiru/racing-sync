"""Question-sheet builders mixin (act sheet, keep/delete, group titles).

Split out of the telegram bot god-file. Pure construction over rows:
text + validated button rows, no sends. Resolution, execution, and
refresh stay with the bot; everything here runs through ``self`` for
the one live lookup (`_now_eligible_hashes`).
"""

from __future__ import annotations

from .state import State
from .telegram_buttons import ButtonMaker
from .telegram_views import (
    _member_button_labels,
    _safe_display_name,
    _size_compact,
)


class SheetsMixin:
    """Action/keep question sheets (duck-typed ``self``)."""

    def _group_title(self, members: list) -> str:
        """Display title for a group (lead row's name, truncated)."""
        try:
            if members:
                return _safe_display_name(str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"

    def _sheet_text_and_rows(
        self,
        members,
        title: str,
        size_bytes: object,
        notes: dict | None = None,
    ) -> tuple[str, list[list[tuple[str, str]]]]:
        """Action sheet text + self-explanatory button rows for a group.

        Every button names its verb and target ("Start <tracker> now",
        "Cancel <tracker>…") so no memorized command is needed; "…"
        marks actions that ask a follow-up question first. One button
        per row — narrow screens wrap multi-button rows into ambiguity.
        """
        try:
            _size = _size_compact(size_bytes)
        except Exception:
            _size = ""
        _tspec = _safe_display_name((title or "")[:60])
        _now_ok = set(self._now_eligible_hashes(members, notes))
        _labels = _member_button_labels(members or [])
        try:
            _n = len(_labels)
        except Exception:
            _n = 0
        _held_any = False
        _free_any = False
        try:
            for _ts in members or []:
                if getattr(_ts, "skipped", 0):
                    _held_any = True
                else:
                    _free_any = True
        except Exception:
            pass
        _inject_ok = False
        try:
            for _ts in members or []:
                if getattr(_ts, "state", None) in (State.WAITING_INDEXER, State.WAITING_DISK):
                    _inject_ok = True
                    break
        except Exception:
            pass
        _maker = ButtonMaker()
        # One row per copy: start and cancel side by side stay readable
        # while keeping the copy context shared.
        for _h, _lbl in _labels:
            if _h in _now_ok:
                _maker.add(f"▶ Start {_lbl} now", f"go:{_h}")
            _maker.add(f"✖ Cancel {_lbl}…", f"cancel:{_h}")
            _maker.newline()
        _leader = _labels[0][0] if _labels else ""
        if len(_labels) > 1 and _leader:
            _maker.add(f"✖ Cancel all {_n} copies…", f"cancel:all:{_leader}").newline()
        if _inject_ok and _leader:
            _maker.add("💉 Inject all to fuse…", f"inject:all:{_leader}").newline()
        if _free_any and _leader:
            _maker.add("⏭ Skip release (pause downloads)", f"skip:all:{_leader}").newline()
        if _held_any and _leader:
            _maker.add("⏪ Resume release (restart it)", f"resume:all:{_leader}").newline()
        _maker.add("Close", "abort")
        _text = (
            f"{_tspec}"
            + (f" · {_size}" if _size else "")
            + (f" · {_n} cop{'y' if _n == 1 else 'ies'} tracked" if _n else "")
            + "\nStart downloads now. Cancel/Inject ask first. "
            "Skip pauses, Resume restarts."
        )
        return _text, _maker.rows()

    def _keepq_text_and_rows(
        self,
        title: str,
        scope_label: str,
        scope: str,
        size_bytes: object = None,
        remember: bool = True,
    ) -> tuple[str, list[list[tuple[str, str]]]]:
        """Keep/delete question text + hash buttons for a cancel scope.

        `remember` (the Q1 answer) travels in the buttons
        (`keep:<scope>:<0|1>:<yes|no>`); the question text names which
        variant applies so a forwarded screenshot stays intelligible.
        """
        try:
            _size = _size_compact(size_bytes)
        except Exception:
            _size = ""
        _tspec = _safe_display_name((title or "")[:60]) + (f" · {_size}" if _size else "")
        _ig = "1" if remember else "0"
        _blocked = "blocked from returning" if remember else "allowed back later"
        _text = (
            f"Cancel {_tspec}{scope_label} — keep downloaded files? "
            f"({_blocked})"
            f"\nKeep untracks (data stays in place); "
            f"Delete wipes the torrent and its data."
        )
        _rows = (
            ButtonMaker()
            .add("✔ Keep files", f"keep:{scope}:{_ig}:yes")
            .newline()
            .add("✖ Delete files", f"keep:{scope}:{_ig}:no")
            .newline()
            .add("Close", "abort")
            .rows()
        )
        return _text, _rows
