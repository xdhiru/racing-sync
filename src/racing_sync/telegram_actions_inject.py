"""Telegram fuse-inject actions (split from telegram_actions.py).

`InjectActionsMixin` snaps a VPS1 group and asks the injectfuse
Yes/No question — single-hash entry plus the ask the group path
shares; moved verbatim, everything resolves through `self`
(instance state and sibling mixins).
"""

import logging
from typing import Any

from .state import State
from .telegram_buttons import ButtonMaker
from .telegram_protocol import (
    _action_error_text,
    _scrubbed,
)
from .telegram_views import (
    _safe_display_name,
    _tracker_domain,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class InjectActionsMixin:
    @staticmethod
    def _injectfuse_member_label(t: Any) -> str:
        """Short label for a VPS1 group member (tracker domain or hash)."""
        try:
            for u in list(getattr(t, "trackers", None) or []):
                try:
                    d = _tracker_domain(u)
                except Exception:
                    d = ""
                if d:
                    return str(d)[:30]
        except Exception:
            pass
        try:
            return (getattr(t, "infohash", "") or "")[:10] or "?"
        except Exception:
            return "?"

    @staticmethod
    def _injectfuse_snap(members: list) -> list[tuple[str, str]]:
        """Frozen [(hash, label)] snapshot for an injectfuse question."""
        snap: list[tuple[str, str]] = []
        seen: dict[str, int] = {}
        for t in members or []:
            try:
                h = (getattr(t, "infohash", "") or "").lower()
            except Exception:
                continue
            if not h:
                continue
            base = InjectActionsMixin._injectfuse_member_label(t)
            n = seen.get(base, 0)
            seen[base] = n + 1
            snap.append((h, base if n == 0 else f"{base} {h[:6]}"))
        return snap

    async def _inject_ask_send(self, resolved: dict, message: Any) -> None:
        """Ask the fuse-inject Yes/No question with hash-scoped buttons.

        Shared by the single, group, and tap-again ask paths: `resolved`
        is a coordinator.injectfuse_resolve() dict (members/title/size).
        The Yes button carries the resolved leader hash and re-resolves
        live at tap time, so no snapshot needs to survive between taps.
        """
        try:
            members = list(resolved.get("members") or [])
            _lead = ""
            try:
                _m0 = members[0] if members else None
                _lead = (getattr(_m0, "infohash", "") or "") or (
                    getattr(_m0, "source_infohash", "") or ""
                )
                _lead = _lead.lower() if _lead else ""
            except Exception:
                _lead = ""
            if not _lead:
                try:
                    await self._reply("Nothing to inject (group changed).", reply_to=message)
                except Exception:
                    pass
                return
            title = str(resolved.get("title") or _lead[:10])[:60]
            try:
                _n = len(members)
            except Exception:
                _n = 0
            extra = ""
            try:
                extra = str(resolved.get("extra_note") or "")
            except Exception:
                extra = ""
            if not extra:
                try:
                    if resolved.get("ignored"):
                        extra = " Prior cancel will be lifted."
                except Exception:
                    pass
            _text = (
                f"Inject {title} ({_n} cop{'y' if _n == 1 else 'ies'}) "
                "to fuse seeding? Files must already be at the "
                f"remote.{extra} Choose below."
            )
            _rows = (
                ButtonMaker()
                .add("✔ Yes, inject", f"inject:{_lead}:yes")
                .newline()
                .add("✖ No", f"inject:{_lead}:no")
                .newline()
                .add("Close", "abort")
                .rows()
            )
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception as e:  # noqa: BLE001
            log.debug("inject ask failed: %s", e)

    async def _start_injectfuse_single(self, full_hash: str, message: Any) -> None:
        """`/inject <hash>`: tracked or untracked VPS1 torrent.

        Resolves the live VPS1 group (category-agnostic, so torrents the
        poller never ingested work too) and arms the Yes/No question.
        Nothing changes until Yes — and Yes verifies fuse readiness
        first, so a premature tap only yields an informative message.
        Yes auto-lifts a prior /cancel and stops any SSD download first,
        so there is no unignore/cancel round-trip.
        """
        coord = getattr(self, "_coord", None)
        if coord is None:
            try:
                await self._reply("Action failed: bot not attached.", reply_to=message)
            except Exception:
                pass
            return
        try:
            resolved = await coord.injectfuse_resolve(full_hash)
        except (LookupError, ValueError) as e:
            try:
                await self._reply(_scrubbed(e)[:300], reply_to=message)
            except Exception:
                pass
            return
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        row = resolved.get("row")
        try:
            state = getattr(row, "state", None) if row is not None else None
        except Exception:
            state = None
        title = str(resolved.get("title") or full_hash[:10])[:60]
        if state in (State.DONE, State.RE_ADDING):
            try:
                await self._reply(
                    f"already {state.value}: {title} (fuse gate runs next tick)", reply_to=message
                )
            except Exception:
                pass
            return
        members = list(resolved.get("members") or [])
        snap = self._injectfuse_snap(members)
        if not snap:
            try:
                await self._reply("Nothing to inject (group changed).", reply_to=message)
            except Exception:
                pass
            return
        extra = ""
        try:
            if resolved.get("ignored"):
                extra = " Prior cancel will be lifted."
            elif state in (State.QUEUED, State.DOWNLOADING, State.MOVING):
                extra = f" SSD {state.value} will be stopped first."
        except Exception:
            pass
        try:
            _resolved_for_ask = dict(resolved)
        except Exception:
            _resolved_for_ask = resolved
        try:
            _resolved_for_ask["extra_note"] = extra
        except Exception:
            pass
        await self._inject_ask_send(_resolved_for_ask, message)
        try:
            if members:
                return _safe_display_name(str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"
