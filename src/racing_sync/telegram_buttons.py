"""Inline-button construction with Telegram budget enforcement.

The Bot API caps ``callback_data`` at 64 *bytes* and truncates absurdly
long labels on mobile. Two postures:

- Question/action sheets (:class:`ButtonMaker`, strict): any violation
  raises :class:`ValueError` at build time. A broken question sheet must
  fail loud in tests — never ship a sheet with a silently missing answer
  button (the operator then has no way to answer).
- The periodic active-list render (:func:`validate_rows`, lenient):
  warns and drops the bad button. The list must always render even if
  one dynamic button misbehaves.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

#: Bot API limit for callback_data, in UTF-8 bytes (not characters).
MAX_CALLBACK_BYTES = 64
#: Labels longer than this render broken on narrow screens; truncated.
MAX_LABEL_CHARS = 60


def check_button(label: object, data: object) -> tuple[str, str]:
    """Validate one button, returning (label, data); raise on violation.

    Labels are coerced to text (display-only, truncated later); callback
    data must be non-empty text fitting the byte budget — it carries
    routing and must never be truncated.
    """
    text = str(label) if not isinstance(label, str) else label
    if not text:
        raise ValueError("button label must not be empty")
    if not isinstance(data, str) or not data:
        raise ValueError(f"button data must be non-empty text, got {data!r}")
    size = len(data.encode("utf-8"))
    if size > MAX_CALLBACK_BYTES:
        raise ValueError(
            f"button data exceeds {MAX_CALLBACK_BYTES} bytes ({size} bytes): {data[:60]!r}…"
        )
    return text, data


class ButtonMaker:
    """Fluent strict builder for one sheet's button layout.

    Buttons are validated eagerly at :meth:`add` time, so a sheet
    builder fails at the offending line instead of shipping short.
    """

    def __init__(self) -> None:
        self._rows: list[list[tuple[str, str]]] = [[]]

    def add(self, label: object, data: object) -> ButtonMaker:
        """Append a button to the current row."""
        self._rows[-1].append(check_button(label, data))
        return self

    def newline(self) -> ButtonMaker:
        """Start a new row (no-op when the current row is empty)."""
        if self._rows[-1]:
            self._rows.append([])
        return self

    def extend(self, rows: list[list[tuple[str, str]]] | tuple) -> ButtonMaker:
        """Append legacy ``(label, data)`` rows, validated each."""
        for row in rows or ():
            for label, data in row or ():
                self.add(label, data)
            self.newline()
        return self

    @classmethod
    def from_rows(cls, rows: list[list[tuple[str, str]]] | tuple) -> ButtonMaker:
        """Build from legacy rows (validates eagerly)."""
        return cls().extend(rows)

    def rows(self) -> list[list[tuple[str, str]]]:
        """Validated plain rows (labels untruncated)."""
        return [list(row) for row in self._rows if row]

    def build(self) -> Any:
        """Render to :class:`InlineKeyboardMarkup` (labels truncated)."""
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(label[:MAX_LABEL_CHARS], callback_data=data)
                    for (label, data) in row
                ]
                for row in self._rows
                if row
            ]
        )

    def __len__(self) -> int:
        return sum(len(row) for row in self._rows)


def validate_rows(rows: list | tuple | Any) -> list[list[tuple[str, str]]]:
    """Lenient validation for the critical render path: warn and drop.

    Accepts a :class:`ButtonMaker` (re-validated, never raises here) or
    legacy ``(label, data)`` rows. Invalid entries are logged with
    enough detail to find the builder; valid ones pass through with
    labels truncated.
    """
    if isinstance(rows, ButtonMaker):
        rows = rows.rows()
    clean: list[list[tuple[str, str]]] = []
    try:
        for row in rows or ():
            line: list[tuple[str, str]] = []
            for pair in row or ():
                try:
                    label, data = pair
                    label, data = check_button(label, data)
                except ValueError as e:  # noqa: BLE001
                    log.warning("dropping invalid button: %s", e)
                    continue
                except Exception as e:  # noqa: BLE001
                    log.warning("dropping malformed button pair %r: %s", pair, e)
                    continue
                line.append((label[:MAX_LABEL_CHARS], data))
            if line:
                clean.append(line)
    except Exception as e:  # noqa: BLE001
        log.warning("dropping malformed button rows %r: %s", rows, e)
    return clean
