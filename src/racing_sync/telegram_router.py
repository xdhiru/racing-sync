"""Telegram callback router (split from telegram_bot.py).

`RouterMixin` carries `_on_action_button` — the hash-protocol tap
dispatcher — moved verbatim; every route resolves through `self`
(instance methods) and the `telegram_protocol` / `telegram_views`
helpers, so no coordinator import is needed here.
"""

import asyncio
import logging
from typing import Any

from .telegram_protocol import CancelScope, ScopeGone, _parse_action_data
from .telegram_views import _safe_display_name

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class RouterMixin:
    async def _on_action_button(self, query: Any, data: str) -> None:
        """Route hash-protocol taps (initial ack already sent).

        Every button carries its own scope (`<cmd>:<hash>` or
        `<cmd>:all:<hash>`), re-resolved live at tap time — group
        renumbering between list render and tap cannot misroute, so no
        snapshot, sequence, or expiry exists. Per-tap admin auth already
        ran in _handle_callback; execution re-checks liveness per hash.
        Short replies go back to the chat; the list refreshes after.
        The tapped sheet is deleted by the caller once this returns, so
        answered questions never linger behind their follow-up.
        """
        async def _say(text: str) -> None:
            try:
                await self._reply(text[:300],
                                  reply_to=getattr(query, "message", None))
            except Exception:
                pass

        async def _refresh() -> None:
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass

        async def _group_of(leader: str) -> list[str]:
            try:
                return await asyncio.to_thread(
                    self._live_group_hashes, leader)
            except Exception:
                return []

        try:
            cmd, _all, _h, _choice, _ig = _parse_action_data(data)
            if not cmd:
                await _say("Stale button — refresh the list")
                return
            if cmd == "abort":
                await self._delete_query_message(query)
                return
            if cmd == "go" and not _all and _h and not _choice:
                try:
                    result = await self._fetch_torrent(_h)
                except Exception as e:  # noqa: BLE001
                    result = f"Action failed: {e}"
                await _say(result)
                await _refresh()
                return
            if cmd == "cancel" and _h and not _choice:
                # First question is remember-or-not (keep/delete follows);
                # the scope travels in the buttons, nothing is stored.
                # Liveness now, not at execution: a dead scope gets its
                # answer here instead of wasted question sheets.
                _scope = CancelScope.from_parsed(_all, _h)
                try:
                    _r = await _scope.resolve(self)
                except ScopeGone as e:
                    await _say(e.reply)
                    await _refresh()
                    return
                except Exception as e:  # noqa: BLE001
                    await _say(f"Action failed: {e}")
                    return
                if _scope.kind == "group":
                    _subject = (
                        f"Forget {_safe_display_name((_r.title_raw or '')[:60])}"
                        f" · all {len(_r.hashes)} copies?")
                else:
                    _subject = (
                        f"Forget {_safe_display_name((_r.title_raw or '')[:60])}?")
                _text, _rows = _scope.remember_sheet(_subject)
                try:
                    await self._send_sheet(
                        _text, _rows,
                        reply_to=getattr(query, "message", None))
                except Exception:
                    pass
                return
            if cmd == "forget" and _h:
                # Q2 keep/delete for any scope, re-resolved live (the set
                # can shrink between taps, so the question names the live
                # set, not the Q1 set).
                if _ig is None:
                    _ig = True
                _scope = CancelScope.from_parsed(_all, _h)
                try:
                    _r = await _scope.resolve(self)
                except ScopeGone as e:
                    await _say(e.reply)
                    await _refresh()
                    return
                except Exception as e:  # noqa: BLE001
                    await _say(f"Action failed: {e}")
                    return
                _text, _rows = self._keepq_text_and_rows(
                    _r.title_raw, _r.qlabel,
                    _r.scope, None, remember=_ig)
                try:
                    await self._send_sheet(
                        _text, _rows,
                        reply_to=getattr(query, "message", None))
                except Exception:
                    pass
                return
            if cmd == "keep" and _choice in ("yes", "no"):
                if _ig is None:
                    _ig = True
                _scope = CancelScope.from_parsed(_all, _h)
                try:
                    _r = await _scope.resolve(self)
                except ScopeGone as e:
                    await _say(e.reply)
                    await _refresh()
                    return
                except Exception as e:  # noqa: BLE001
                    await _say(f"Action failed: {e}")
                    return
                result = await self._execute_snapshot_cancel(
                    _r.hashes, _r.etitle_raw,
                    delete_files=(_choice == "no"), remember=_ig)
                await _say(result)
                await _refresh()
                return
            if cmd == "inject" and _h and not _choice:
                coord = getattr(self, "_coord", None)
                if coord is None:
                    await _say("Action failed: bot not attached")
                    return
                try:
                    resolved = await coord.injectfuse_resolve(_h)
                except (LookupError, ValueError) as e:
                    await _say(str(e)[:300])
                    return
                except Exception as e:  # noqa: BLE001
                    await _say(f"Action failed: {e}")
                    return
                try:
                    _ask = dict(resolved)
                except Exception:
                    _ask = resolved
                await self._inject_ask_send(
                    _ask, getattr(query, "message", None))
                return
            if cmd == "inject" and _choice in ("yes", "no"):
                if _choice != "yes":
                    await _say("Not injected — nothing changed.")
                    await _refresh()
                    return
                try:
                    from .api import _hold_ops_lock
                except Exception:
                    _hold_ops_lock = None  # type: ignore[assignment]
                coord = getattr(self, "_coord", None)
                if coord is None:
                    await _say("Action failed: bot not attached")
                    await _refresh()
                    return
                try:
                    if _hold_ops_lock is not None:
                        async with _hold_ops_lock(coord):
                            result = await coord.injectfuse_confirmed([_h])
                    else:
                        result = await coord.injectfuse_confirmed([_h])
                except Exception as e:  # noqa: BLE001
                    result = f"Action failed: {e}"
                await _say(result[:300])
                await _refresh()
                return
            if cmd == "skip" and _all and _h:
                _hashes = await _group_of(_h)
                if not _hashes:
                    await _say("Group changed — tap /act again.")
                    return
                try:
                    result = await self._skip_torrents(_hashes, hold=True)
                except Exception as e:  # noqa: BLE001
                    result = f"Action failed: {e}"
                await _say(result)
                await _refresh()
                return
            if cmd == "resume" and _h:
                if _all:
                    _hashes = await _group_of(_h)
                    if not _hashes:
                        await _say("Group changed — tap /act again.")
                        return
                else:
                    _hashes = [_h]
                try:
                    result = await self._resume_hashes_reply(_hashes)
                except Exception as e:  # noqa: BLE001
                    result = f"Resume failed: {e}"
                await _say(result)
                await _refresh()
                return
            await _say("Stale button — refresh the list")
        except Exception as e:  # noqa: BLE001
            log.debug("action button handling failed: %s", e)
