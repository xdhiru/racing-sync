"""Telegram "push from VPS1" actions (split from the action mixins).

One body — :meth:`PushActionsMixin._push_source_command` — behind both
operator surfaces: the typed `/push_source <target>` command and the
`📤 Push from VPS1…` act-sheet button (with its Yes/No confirm). The
transport itself lives in `coordinator_push.PushMixin`; this module only
decides *which row* to hand it and reports the outcome.

Two rules pinned here:

* **inert unless opted in.** With `[source.rclone_push].enabled` anything
  but literally `true`, the command refuses naming the switch, the sheet
  shows no button, and no coordinator method is called at all.
* **one push per group.** The row is the group's *elected primary* — the
  same resolution `/inject` performs (`injectfuse_resolve` →
  `resolved["primary"]`) — so a group with three tracker copies still
  triggers exactly one `rclone copy`, and it is the copy the pipeline
  would have seeded.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .telegram_buttons import ButtonMaker
from .telegram_protocol import (
    _action_error_text,
    _push_enabled,
    _scrubbed,
)
from .telegram_views import _safe_display_name

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")

#: Refusal text naming the switch verbatim: an operator who sees this must
#: not have to hunt the config docs for the key.
PUSH_SWITCH_OFF = "Push is off — enable it with [source.rclone_push].enabled = true."

#: Usage answer for a token that is not a full hash and resolved to no group.
PUSH_NEEDS_TARGET = "Send /push_source <group number or full 40-char hash>."

#: Duplicate-tap answer (same wording as the cancel/forget guard).
PUSH_ALREADY_WORKING = "Already working on it — one tap is enough."


class PushActionsMixin:
    """`/push_source` + the act-sheet push button (duck-typed ``self``)."""

    def _push_switch_enabled(self) -> bool:
        """The operator's opt-in, read the same strict way as the backend."""
        try:
            cfg = getattr(self, "cfg", None)
            if cfg is None:
                cfg = getattr(getattr(self, "_coord", None), "cfg", None)
        except Exception:
            cfg = None
        return _push_enabled(cfg)

    async def _push_source_command(self, leader_hash: str, title: str, message: Any) -> None:
        """Copy one release from VPS1 straight to the seed remote.

        Guards run inline (target shape, attachment, the config switch, the
        in-flight dedupe) so a refusal answers this command; the push itself
        runs as a task, because the SSH preflight and launch can take a
        while and awaiting them inline would stall update polling (the
        `/cleanup` precedent). The task replies with the coordinator's own
        one-line reason and refreshes the card.
        """
        norm = (leader_hash or "").strip().lower()
        if len(norm) != 40 or any(c not in "0123456789abcdef" for c in norm):
            await self._reply(PUSH_NEEDS_TARGET, reply_to=message)
            return
        coord = getattr(self, "_coord", None)
        starter = getattr(coord, "start_source_push", None)
        if coord is None or not callable(starter):
            await self._reply("Action failed: bot not attached.", reply_to=message)
            return
        if not self._push_switch_enabled():
            await self._reply(PUSH_SWITCH_OFF, reply_to=message)
            return
        key = f"push:{norm}"
        if not self._claim_action(key):
            await self._reply(PUSH_ALREADY_WORKING, reply_to=message)
            return
        try:
            self._push_task = asyncio.create_task(self._push_source_task(norm, title, message))
        except Exception as e:  # noqa: BLE001
            self._release_action(key)
            await self._reply(_action_error_text(e), reply_to=message)

    async def _push_source_task(self, leader_hash: str, title: str, message: Any) -> None:
        """Resolve the group's primary, start one push, report the outcome."""
        key = f"push:{leader_hash}"
        coord = getattr(self, "_coord", None)
        try:
            row = await self._push_target_row(coord, leader_hash)
            if row is None:
                display = _safe_display_name(title or "") or leader_hash[:10]
                await self._reply(
                    f"Push failed: {display} is no longer tracked.",
                    reply_to=message,
                )
                return
            try:
                started, reason = await coord.start_source_push(row, reason="manual")
            except Exception as e:  # noqa: BLE001
                started, reason = False, _action_error_text(e)
            # The coordinator's line is the whole answer: it already names
            # what started or exactly why nothing did.
            await self._reply(str(reason or "")[:300], reply_to=message)
            log.debug(
                "push from VPS1 %s for %s",
                "started" if started else "refused",
                leader_hash[:10],
            )
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
        finally:
            self._release_action(key)
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass

    async def _push_target_row(self, coord: Any, leader_hash: str) -> Any:
        """The row to push: the group's elected primary, else the leader.

        Reuses the resolution `/inject` already performs, so the two verbs
        can never disagree about which copy of a release a group is. The
        row comes from the store (it carries the tracked state the backend
        gates on); a VPS1 that no longer lists the group simply means the
        tracked rows are all we have, and the last resort is the
        resolution's own row. Never raises: at worst the caller is told the
        release is gone.
        """
        primary_hash = ""
        resolved_row = None
        try:
            resolved = await coord.injectfuse_resolve(leader_hash)
        except (LookupError, ValueError) as e:
            # VPS1 no longer lists it (cleaned/pruned): fall back below.
            log.debug("push: VPS1 group resolve failed for %s: %s", leader_hash[:10], e)
            resolved = None
        except Exception as e:  # noqa: BLE001
            log.debug("push: VPS1 group resolve errored for %s: %s", leader_hash[:10], e)
            resolved = None
        if isinstance(resolved, dict):
            try:
                primary = resolved.get("primary")
                primary_hash = (
                    getattr(primary, "infohash", "") or getattr(primary, "hash", "") or ""
                ).lower()
                resolved_row = resolved.get("row")
            except Exception:
                primary_hash = ""
        store = getattr(self, "_store", None)
        for h in (primary_hash, leader_hash):
            if len(h) != 40 or not callable(getattr(store, "get", None)):
                continue
            try:
                row = await asyncio.to_thread(store.get, h)
            except Exception as e:  # noqa: BLE001
                log.debug("push: row read failed for %s: %s", h[:10], e)
                row = None
            if row is not None:
                return row
        return resolved_row

    async def _push_ask_send(self, leader_hash: str, message: Any) -> None:
        """Ask the push Yes/No question with hash-scoped buttons.

        Nothing has happened yet — the sheet is the only place a copy is
        committed to. Like the inject question the buttons carry the
        leader hash and the Yes path re-resolves the group live, so a
        stale sheet cannot push the wrong release.
        """
        try:
            _lead = (leader_hash or "").strip().lower()
            if not _lead:
                try:
                    await self._reply("Nothing to push (group changed).", reply_to=message)
                except Exception:
                    pass
                return
            display = _lead[:10]
            try:
                store = getattr(self, "_store", None)
                row = (
                    await asyncio.to_thread(store.get, _lead, include_blob=False)
                    if callable(getattr(store, "get", None))
                    else None
                )
                display = _safe_display_name(
                    str(getattr(row, "source_name", "") or "")[:60] or display
                )
            except Exception:
                pass
            _text = (
                f"Push {display} from VPS1? It copies the bytes the racing box "
                "already holds onto the seed remote — VPS2 downloads nothing. "
                "Choose below."
            )
            _rows = (
                ButtonMaker()
                .add("✔ Yes, push from VPS1", f"push:all:{_lead}:yes")
                .newline()
                .add("✖ No", f"push:all:{_lead}:no")
                .newline()
                .add("Close", "abort")
                .rows()
            )
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception as e:  # noqa: BLE001
            log.debug("push ask failed: %s", _scrubbed(e))
