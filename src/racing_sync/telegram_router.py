"""Telegram inbound handling (split from telegram_bot.py).

`RouterMixin` carries the inbound-update path — the update/callback
loops, the hash-protocol tap dispatcher, chat/document commands, and
the action guards — moved verbatim; every route resolves through
`self` (instance methods) and the `telegram_protocol` /
`telegram_views` helpers, so no coordinator import is needed here.
"""

import asyncio
import logging
import time
from typing import Any

from telegram.error import NetworkError, TimedOut

from .state import State, TorrentState, UpsertResult
from .telegram_protocol import (
    _CHAT_COMMANDS,
    CancelScope,
    ScopeGone,
    _action_error_text,
    _command_usage_for,
    _now_command,
    _parse_action_data,
    _scrubbed,
    _slow_tap,
)
from .telegram_views import (
    _group_active_items,
    _is_grace_note_for_prefer,
    _safe_display_name,
    _size_compact,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


def _tap_user_id(query: Any) -> int | None:
    """Tapper's user id (None when the update carries no usable sender)."""
    try:
        uid = getattr(getattr(query, "from_user", None), "id", None)
        return int(uid) if uid is not None else None
    except (TypeError, ValueError):
        return None


def _tg_actor_allowed(cfg: Any, user_id: object) -> bool:
    """Admin-allowlist gate for destructive commands and taps.

    Empty `admin_user_ids` (default) preserves today's behavior: anyone
    in the authorized chat may act. A non-empty list restricts destructive
    flows to those users; a missing user id fails closed only when the
    list is configured (channel posts without a sender cannot comply).
    """
    try:
        allowed = getattr(cfg, "admin_user_ids", None) or []
        if not isinstance(allowed, (list, tuple, set)):
            # Unconfigured (or a test double): no restriction.
            return True
        allowed = [a for a in allowed]
        if not allowed:
            return True
        if user_id is None:
            return False
        want = {str(int(a)) for a in allowed}
        return str(user_id) in want or str(int(str(user_id))) in want
    except Exception:
        # Fail closed: a broken allowlist must not open destructive flows.
        return False


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
            # Standalone (no reply_to): the tapped sheet is deleted by the
            # caller once this returns, so threading to it would dangle.
            try:
                await self._reply(text[:300])
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
                return await asyncio.to_thread(self._live_group_hashes, leader)
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
                    result = _action_error_text(e)
                await _say(result)
                await _refresh()
                return
            if cmd == "clean" and _h and not _choice:
                # Manual per-task VPS1 cleanup from the act sheet. The
                # command replies itself (no sheet to thread a reply to).
                try:
                    await self._clean_task_command(_h, None)
                except Exception as e:  # noqa: BLE001
                    await _say(_action_error_text(e))
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
                    await _say(_action_error_text(e))
                    return
                if _scope.kind == "group":
                    _subject = (
                        f"Forget {_safe_display_name((_r.title_raw or '')[:60])}"
                        f" · all {len(_r.hashes)} copies?"
                    )
                else:
                    _subject = f"Forget {_safe_display_name((_r.title_raw or '')[:60])}?"
                _text, _rows = _scope.remember_sheet(_subject)
                # Standalone sheet: query.message (the tapped sheet) is
                # deleted by the caller below — a reply_to would dangle off
                # a deleted message. If the question cannot be posted the
                # flow is lost, so keep the tapped sheet for a retry.
                if not await self._send_sheet(_text, _rows, opener=_tap_user_id(query)):
                    await _say("Could not post the question sheet; tap again.")
                    return True
                return False
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
                    await _say(_action_error_text(e))
                    return
                _text, _rows = self._keepq_text_and_rows(
                    _r.title_raw, _r.qlabel, _r.scope, None, remember=_ig
                )
                # Standalone (see remember-sheet above).
                if not await self._send_sheet(_text, _rows, opener=_tap_user_id(query)):
                    await _say("Could not post the question sheet; tap again.")
                    return True
                return False
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
                    await _say(_action_error_text(e))
                    return
                _hashes = _r.hashes
                _etitle = _r.etitle_raw
                _del_files = _choice == "no"
                _remember = bool(_ig)

                async def _run_cancel() -> None:
                    try:
                        _res = await self._execute_snapshot_cancel(
                            _hashes, _etitle, delete_files=_del_files, remember=_remember
                        )
                    except Exception as e:  # noqa: BLE001
                        _res = _action_error_text(e)
                    await _say(_res)
                    await _refresh()

                # Bulk wipes stagger 2s per copy; awaiting that inline stalls
                # polling for minutes and Telegram retries the update. The
                # per-hash in-flight guard already absorbs a re-tap.
                try:
                    self._cancel_task = asyncio.create_task(_run_cancel())
                except Exception:
                    # No loop to spawn on: never lose the tap.
                    await _run_cancel()
                return
            if cmd == "inject" and _h and not _choice:
                coord = getattr(self, "_coord", None)
                if coord is None:
                    await _say("Action failed: bot not attached")
                    return
                try:
                    resolved = await coord.injectfuse_resolve(_h)
                except (LookupError, ValueError) as e:
                    await _say(_scrubbed(e)[:300])
                    return
                except Exception as e:  # noqa: BLE001
                    await _say(_action_error_text(e))
                    return
                try:
                    _ask = dict(resolved)
                except Exception:
                    _ask = resolved
                await self._inject_ask_send(_ask, None)
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
                    result = _action_error_text(e)
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
                    result = _action_error_text(e)
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
                    result = f"Resume failed: {_scrubbed(e)}"
                await _say(result)
                await _refresh()
                return
            await _say("Stale button — refresh the list")
        except Exception as e:  # noqa: BLE001
            log.debug("action button handling failed: %s", e)

    async def _callback_loop(self) -> None:
        """Poll get_updates for pagination clicks and `/cancel_` commands."""
        assert self._bot is not None
        offset = 0
        while not self._stopped:
            try:
                updates = await self._bot.get_updates(
                    offset=offset,
                    timeout=10,
                    allowed_updates=["callback_query", "message", "channel_post"],
                )
                # A clean poll clears the conflict streak (only
                # consecutive conflicts fail fast).
                try:
                    self._poll_conflicts = 0
                except Exception:
                    pass
                for u in updates:
                    offset = max(offset, u.update_id + 1)
                    if u.callback_query:
                        await self._handle_callback(u.callback_query)
                        continue
                    msg = getattr(u, "message", None) or getattr(u, "channel_post", None)
                    if msg is not None:
                        await self._handle_chat_message(msg)
            except asyncio.CancelledError:
                return
            except (TimedOut, NetworkError):
                await asyncio.sleep(0.5)
            except Exception as e:
                msg = str(e).lower()
                if "conflict" in msg:
                    # Another bot session holds this token: backing off
                    # forever just burns API calls. Count consecutive
                    # conflicts; at 3 fail fast (stop the loop loudly —
                    # the coordinator will not respawn a conflicted poll
                    # and the double-run surfaces instead of hiding).
                    try:
                        _n = int(getattr(self, "_poll_conflicts", 0) or 0) + 1
                    except Exception:
                        _n = 1
                    try:
                        self._poll_conflicts = _n
                    except Exception:
                        pass
                    if _n >= 3:
                        try:
                            self._poll_conflict_fatal = True
                        except Exception:
                            pass
                        log.error(
                            "telegram polling conflict x%s (another bot "
                            "session with this token?); stopping callback "
                            "loop",
                            _n,
                        )
                        try:
                            self._stopped = True
                        except Exception:
                            pass
                        return
                    log.warning(
                        "telegram callback polling conflict (another bot session running?): %s", e
                    )
                    await asyncio.sleep(15)
                else:
                    log.debug("telegram callback polling error: %s", e)
                    await asyncio.sleep(2)

    #: Read-only chat commands: never debounced away (a /stats tap right
    #: after any command still answers; only mutating verbs are gated).
    _READ_ONLY_COMMANDS = frozenset({"stats", "find", "files"})

    def _debounced(self, key: str, *, window: float = 0.5) -> bool:
        """True when `key` fired within `window` seconds (caller skips).

        Shared by callback pagination and /cancel_/start_ chat commands: a
        double-tap (or flooding client) re-resolves and re-acts otherwise.
        Fail-closed on bookkeeping errors (drop the tap on doubt — a
        re-tap replays it, a double-act cannot be undone). Eviction
        purges expired windows only and never records past the cap, so
        no live window is ever flushed (no victim re-arms early).
        """
        now = time.monotonic()
        try:
            times = getattr(self, "_callback_times", None)
            if not isinstance(times, dict):
                times = {}
                self._callback_times = times
            last = float(times.get(key, 0.0) or 0.0)
        except Exception:
            return True
        if now - last < window:
            return True
        try:
            for k in [k for k, t in times.items() if now - float(t or 0.0) >= window]:
                times.pop(k, None)
            if len(times) < 1000:
                times[key] = now
            # At cap the tap still runs once; its window just isn't
            # recorded (memory stays bounded, no live window flushed).
        except Exception:
            pass
        return False

    def _claim_action(self, key: str, *, ttl: float = 600.0) -> bool:
        """Hold a destructive scope while it executes (True when free).

        Fail-closed like _debounced (tests build the bot without
        __init__): bookkeeping trouble refuses the hold — the tap answers
        "working on it" instead of risking a double forget. Stale holds
        (crashed mid-execution) expire via `ttl`; past the cap new holds
        are refused rather than evicting live ones.
        """
        now = time.monotonic()
        try:
            inflight = getattr(self, "_action_inflight", None)
            if not isinstance(inflight, dict):
                inflight = {}
                self._action_inflight = inflight
            for k in [k for k, t in inflight.items() if now - float(t or 0.0) > ttl]:
                inflight.pop(k, None)
            if key in inflight:
                return False
            if len(inflight) >= 1000:
                return False
            inflight[key] = now
            return True
        except Exception:
            return False

    def _refresh_actions(self, keys: list[str]) -> None:
        """Re-stamp live holds so a long run cannot expire mid-flight.

        A bulk wipe can outlive the hold TTL; without this a late tap would
        see no hold and start a second execution over the same copies.
        """
        try:
            inflight = getattr(self, "_action_inflight", None)
            if not isinstance(inflight, dict):
                return
            now = time.monotonic()
            for k in keys or []:
                if k in inflight:
                    inflight[k] = now
        except Exception:
            pass

    def _release_action(self, key: str) -> None:
        """Drop a destructive-scope hold (best-effort)."""
        try:
            inflight = getattr(self, "_action_inflight", None)
            if isinstance(inflight, dict):
                inflight.pop(key, None)
        except Exception:
            pass

    async def _handle_callback(self, query: Any) -> None:
        # Authenticate callback: query must originate from configured chat or user
        chat_id = None
        if hasattr(query, "message") and query.message and hasattr(query.message, "chat"):
            chat_id = getattr(query.message.chat, "id", None)
        user_id = getattr(getattr(query, "from_user", None), "id", None)
        cfg_chat = str(self._cfg.chat_id)
        if str(chat_id) != cfg_chat and str(user_id) != cfg_chat:
            log.warning("unauthorized callback query from chat=%s user=%s", chat_id, user_id)
            try:
                await query.answer("Unauthorized", show_alert=True)
            except Exception:
                pass
            return
        if not _tg_actor_allowed(self._cfg, user_id):
            log.warning("callback from non-admin user=%s (allowlist set)", user_id)
            try:
                await query.answer("Not authorized for destructive actions", show_alert=True)
            except Exception:
                pass
            return

        # Throttle callback handling (2s debounce per chat+user, destructive
        # taps bucketed harder below — a per-chat key lets one spammer
        # block pagination for all chats sharing it).
        try:
            _debounce_key = f"{chat_id}:{user_id}" if chat_id is not None else str(user_id)
        except Exception:
            _debounce_key = ""
        if self._debounced(_debounce_key):
            try:
                await query.answer("Slow down — one tap is enough")
            except Exception:
                pass
            return

        try:
            await query.answer()
        except Exception:
            pass

        data = str(getattr(query, "data", "") or "")
        # A destructive sheet belongs to the operator who opened it: its
        # keep/forget buttons finish a decision only that operator made.
        # Anyone else's tap is answered and the sheet is left for its owner.
        if data.startswith(("keep:", "forget:")):
            owner = None
            try:
                owners = getattr(self, "_sheet_owner", None)
                mid = getattr(getattr(query, "message", None), "message_id", None)
                if isinstance(owners, dict) and isinstance(mid, int):
                    owner = owners.get(mid)
            except Exception:
                owner = None
            tapper = _tap_user_id(query)
            if owner is not None and tapper is not None and tapper != owner:
                try:
                    await query.answer(
                        "This sheet belongs to another operator; open your own.",
                        show_alert=True,
                    )
                except Exception:
                    pass
                return
        if (
            # `pick:` is a legacy snapshot-index format: no parser
            # shape emits it, but old buttons must still answer
            # "Stale button" instead of hanging the spinner.
            data.startswith("pick:")
            or data.startswith("keep:")
            or data.startswith("forget:")
            or data.startswith("abort")
            or data.startswith("inject:")
            or data.startswith("go:")
            or data.startswith("cancel:")
            or data.startswith("skip:")
            or data.startswith("resume:")
            or data.startswith("clean:")
        ):
            if _slow_tap(data):
                # Feedback first: the execution below can outlive the tap
                # by a minute while the sheet stays live — a silent sheet
                # invites the re-tap this guard exists to absorb.
                await self._mark_sheet_working(query)
            _keep_sheet = await self._on_action_button(query, data)
            if data != "abort" and not _keep_sheet:
                # The tapped sheet served its purpose: remove it so
                # answered questions don't linger behind their follow-up
                # (`abort` already deletes itself above; the active-list
                # message never carries action buttons, only pagination,
                # so this only ever removes disposable sheets). Exact
                # match: "aborted"/"abortx" are stale buttons, and their
                # sheet was left behind when a prefix check skipped this.
                try:
                    await self._delete_query_message(query)
                except Exception:
                    pass
            return
        if not data.startswith("page:"):
            if data.startswith("status:"):
                _skey = data.split(":", 1)[1].strip().lower()
                if _skey == "ov":
                    try:
                        await self._send_overview_message(query)
                    except Exception:
                        pass
                    return
                try:
                    from .telegram_protocol import _STATUS_FILTERS

                    _valid = _skey in _STATUS_FILTERS
                except Exception:
                    _valid = _skey in ("all", "dl", "mv", "rd", "qd")
                if _valid:
                    try:
                        _cur = getattr(self, "_status_filter", "all") or "all"
                    except Exception:
                        _cur = "all"
                    # Filter flips re-render (buttons + list change): drop
                    # the dedup cache so the identical-text gate can't
                    # skip the button update. Same-filter taps keep it.
                    if _skey != _cur:
                        try:
                            self._status_filter = _skey
                        except Exception:
                            pass
                        self._current_page = 0
                        try:
                            self._last_active_cache = None
                        except Exception:
                            pass
                    await self._refresh_active_message()
                return
            return

        action = data.split(":", 1)[1]
        rows = await asyncio.to_thread(self._store.list_active_inflight)
        try:
            page_size = max(1, min(int(self._cfg.page_size), 50))
        except (TypeError, ValueError):
            page_size = 5
        # Page over rendered GROUPS (election identity), not raw rows:
        # the renderer groups same-file copies, so row-based wrapping
        # desyncs prev/next (taps wrap early or look dead) whenever
        # duplicates share a page. The status filter applies here too —
        # the keyboard's totals count what the renderer shows.
        try:
            total_groups = len(
                _group_active_items([(r, None) for r in self._positional_active_rows(rows or [])])
            )
        except Exception:
            try:
                total_groups = len(rows or [])
            except Exception:
                total_groups = 0
        total_pages = max(1, (total_groups + page_size - 1) // page_size)

        if action == "prev":
            self._current_page = (self._current_page - 1) % total_pages
        elif action == "next":
            self._current_page = (self._current_page + 1) % total_pages
        elif action == "refresh":
            pass

        # No cache clear: a page change alters the dedup key (edit still
        # fires); an unchanged render — the common refresh tap — hits the
        # dedup cache and skips the pointless edit instead.
        await self._refresh_active_message()

    # ---- task cancellation via `/cancel_` chat commands ----

    def _resolve_cancel_target(self, short: str, *, cmd: str = "cancel"):
        """Map a `/cancel_<prefix|full-hash>` token to its tracked row.

        Short prefixes search in-flight rows only, so `DONE`/`FAILED`
        history can't force ambiguity on a live torrent. A full 40-char
        hash addresses any tracked row (including `DONE`, e.g. to stop
        seeding it). Raises LookupError when unknown or ambiguous
        (prefix matches several live rows — resend with the full hash).
        """
        norm = (short or "").strip().lower()
        if not norm or any(c not in "0123456789abcdef" for c in norm):
            raise LookupError(f"not a torrent hash: {short!r}")
        if len(norm) < 4 and len(norm) != 40:
            # 1-3 char prefixes always match-or-error over a full table
            # scan; group numbers never reach here (digits route first).
            raise LookupError(
                f"/{cmd}_{norm} is too short — send at least 4 hex chars or the full 40-char hash"
            )
        try:
            if len(norm) == 40:
                row = self._store.get(norm)
                if row is not None:
                    return row
        except Exception:
            pass
        try:
            all_active = getattr(self._store, "all_active", None)
            rows = all_active() if callable(all_active) else self._store.all()
            candidates = [ts for ts in rows if (ts.source_infohash or "").lower().startswith(norm)]
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"cannot look up {short!r}: {e}") from e
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise LookupError(
                f"/{cmd}_{norm} matches {len(candidates)} torrents; send /{cmd}_<full 40-char hash>"
            )
        raise LookupError(
            f"no tracked torrent starts with {norm!r} (it may already be done/cancelled)"
        )

    def _fetch_new_eligible(self, row) -> bool:
        """True when a NEW row is actually waiting (preferred-copy grace).

        Mirrors /start_ list eligibility: the grace note is the signal,
        for watch drops (use the starting .torrent) and racing rows
        (use the VPS1 original) alike. Fail-closed (False) when the
        coordinator is unavailable — same as an empty note set.
        """
        try:
            if getattr(row, "state", None) != State.NEW:
                return False
            coord = getattr(self, "_coord", None)
            if coord is None:
                return False
            note_fn = getattr(coord, "_watch_wait_note", None)
            if not callable(note_fn):
                return False
            return _is_grace_note_for_prefer(note_fn(row))
        except Exception:
            return False

    def _resolve_fetch_target(self, short: str):
        """Map a `/start_<prefix|full-hash>` token to a waiting row.

        WAITING_INDEXER rows fetch the VPS1 original instead of waiting
        out Prowlarr. NEW grace-held rows (watch or racing) never wait
        on that state — allow fetch while grace-held, mirroring
        /start_ eligibility. Raises LookupError otherwise.
        """
        row = self._resolve_cancel_target(short, cmd="fetch")
        if row.state == State.WAITING_INDEXER:
            return row
        if self._fetch_new_eligible(row):
            return row
        raise LookupError(
            f"{(row.source_name or row.source_infohash[:10])[:50]} is "
            f"{row.state.value}, not waiting for the download indexer — "
            "nothing to fetch"
        )

    async def _handle_chat_message(self, message: Any) -> None:
        """Execute `/cancel_` / `/cancel_all` / `/start_` / `/resume_` / `/inject` commands.

        Legacy `/start_` + `/start_` parse as `/start_`; `/unhold_` +
        `/unignore_` parse as `/resume_` (same shapes, unified executors).

        Group commands (stable content ids from the active list) open
        member-choice buttons; full hashes and legacy hash prefixes act
        directly. Cancel always ends at a keep/delete question — nothing
        is wiped without an explicit choice. Injectfuse always ends at a
        Yes/No question — nothing changes state without verification.
        `/add` ingests a replied-to (or captioned) .torrent file as a
        Telegram-origin drop. Anything else is ignored. Only the
        configured chat/user may send commands. Dispatch is table-driven
        (:data:`_CHAT_COMMANDS`): first pattern match wins.
        """
        try:
            chat = getattr(message, "chat", None)
            chat_id = getattr(chat, "id", None)
            from_user = getattr(message, "from_user", None)
            user_id = getattr(from_user, "id", None)
            cfg_chat = str(self._cfg.chat_id)
            if str(chat_id) != cfg_chat and str(user_id) != cfg_chat:
                return
            # Channel posts carry no sender: note the origin (anonymous
            # destructive commands from the right chat still execute when
            # no admin allowlist is configured, but they must leave an
            # audit trail; a configured allowlist refuses them below).
            _via_channel = user_id is None
            text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
            text = str(text or "").strip()
            if not _tg_actor_allowed(self._cfg, user_id):
                try:
                    await self._reply("Not authorized for destructive actions.", reply_to=message)
                except Exception:
                    pass
                return
            _matched = None
            _match = None
            for _cmd in _CHAT_COMMANDS:
                try:
                    _m = _cmd.pattern.match(text)
                except Exception:
                    _m = None
                if _m is None:
                    continue
                _matched, _match = _cmd, _m
                break
            if _matched is None:
                # A bare verb from the slash menu ("/start") has no target:
                # answer with the usage line instead of ignoring the message.
                _usage = _command_usage_for(text)
                if _usage:
                    try:
                        await self._reply(_usage, reply_to=message)
                    except Exception:
                        pass
                return
            if _via_channel:
                try:
                    log.warning(
                        "channel-post command /%s without sender (chat=%s): %r",
                        _matched.name,
                        chat_id,
                        text[:80],
                    )
                except Exception:
                    pass
            # Debounce mutating verbs only (double-sent command must not
            # resolve+act twice): read-only /stats|/find|/files always
            # answer, even inside another command's window. The key carries
            # the verb AND its target, so /hold_x followed by /resume_x is
            # two actions rather than one dropped duplicate. A message that
            # carries a .torrent is never debounced: each upload is distinct
            # work, and the sender's passkey must not linger in chat.
            try:
                _has_doc = bool(getattr(message, "document", None)) or bool(
                    getattr(getattr(message, "reply_to_message", None), "document", None)
                )
            except Exception:
                _has_doc = False
            if _matched.name not in self._READ_ONLY_COMMANDS and not _has_doc:
                try:
                    _ckey = (
                        f"cmd:{_matched.name}:{chat_id}:{user_id}:"
                        f"{(text or '').strip().lower()[:200]}"
                        if chat_id is not None
                        else f"cmd:{_matched.name}:{user_id}:{(text or '').strip().lower()[:200]}"
                    )
                except Exception:
                    _ckey = ""
                if _ckey and self._debounced(_ckey, window=2.0):
                    try:
                        await self._reply(
                            f"Ignored duplicate {_matched.name} command (sent "
                            "twice in a row); re-run it in a moment.",
                            reply_to=message,
                        )
                    except Exception:
                        pass
                    return
            await _matched.handle(self, _match, message)
            return
        except Exception as e:  # noqa: BLE001
            log.debug("chat command handling failed: %s", e)

    async def _process_document_command(self, message: Any) -> None:
        """Ingest a .torrent file from chat as a Telegram-origin drop.

        Accepts `/add` replying to a document message, or a document
        sent with `/add` as its caption. The bytes flow through the
        same validation as watch-dir drops (suffix, size cap, bencode
        parse) and become a NEW row with `cross_seed_source="telegram"`,
        which the coordinator processes like a manual drop. After a
        successful ingest (or a duplicate of one) the file message is
        deleted best-effort: .torrent files embed the sender's tracker
        passkey and must not linger in chat history.
        """
        try:
            from .coordinator_content import announce_domain
        except Exception:
            announce_domain = lambda _u: ""  # noqa: E731
        try:
            from .watchdir import MAX_TORRENT_BYTES, _bencoded_info_hash
        except Exception:
            await self._reply("Action failed: torrent parser unavailable.", reply_to=message)
            return
        # Resolve the document: same-message caption case first, else the
        # replied-to message.
        doc = getattr(message, "document", None)
        doc_msg = message
        if doc is None:
            try:
                _replied = getattr(message, "reply_to_message", None)
            except Exception:
                _replied = None
            if _replied is not None:
                doc = getattr(_replied, "document", None)
                if doc is not None:
                    doc_msg = _replied
        if doc is None:
            await self._reply(
                "Reply to a .torrent file with /add (or send the file with /add as its caption).",
                reply_to=message,
            )
            return
        try:
            fname = str(getattr(doc, "file_name", "") or "")
            fsize = getattr(doc, "file_size", None)
            file_id = getattr(doc, "file_id", None)
        except Exception:
            fname, fsize, file_id = "", None, None
        if not fname.lower().endswith(".torrent"):
            await self._reply(
                f"Not a .torrent file ({fname or 'unnamed'}); nothing ingested.", reply_to=message
            )
            return
        try:
            fsize_int = int(fsize) if fsize is not None else -1
        except (TypeError, ValueError):
            fsize_int = -1
        if fsize_int < 0 or fsize_int > MAX_TORRENT_BYTES:
            await self._reply(
                f"Refusing {fname or 'file'} ({fsize_int} B; cap {MAX_TORRENT_BYTES} B).",
                reply_to=message,
            )
            return
        if not file_id:
            await self._reply(
                "Could not read the file from Telegram; try re-sending it.", reply_to=message
            )
            return
        bot = getattr(self, "_bot", None)
        if bot is None:
            await self._reply("Action failed: bot not attached.", reply_to=message)
            return
        try:
            tg_file = await bot.get_file(file_id)
        except Exception as e:  # noqa: BLE001
            await self._reply(
                f"Could not download the file from Telegram ({e}); try re-sending it.",
                reply_to=message,
            )
            return
        try:
            # Pass the buffer explicitly: some PTB versions require the
            # positional `out` argument (TypeError otherwise), others make
            # it optional — explicit works on both. Accept bytes, a buffer,
            # or None (filled buffer) back.
            import io as _io

            _buf = _io.BytesIO()
            _dl = tg_file.download_to_memory(_buf)
            if asyncio.iscoroutine(_dl):
                _dl = await _dl
            if isinstance(_dl, (bytes, bytearray)):
                data = bytes(_dl)
            elif hasattr(_dl, "getvalue"):
                data = bytes(_dl.getvalue())
            else:
                data = bytes(_buf.getvalue())
        except Exception as e:  # noqa: BLE001
            await self._reply(
                f"Could not download the file from Telegram ({e}); try re-sending it.",
                reply_to=message,
            )
            return
        if not data or len(data) > MAX_TORRENT_BYTES + 1:
            await self._reply("Refusing: file is empty or over the size cap.", reply_to=message)
            return
        try:
            infohash, name, size, announce = _bencoded_info_hash(data)
        except Exception as e:  # noqa: BLE001
            await self._reply(f"Not a valid .torrent file ({_scrubbed(e)}).", reply_to=message)
            return
        store = getattr(self, "_store", None)
        if store is None:
            await self._reply("Action failed: bot not attached.", reply_to=message)
            return
        try:
            existing = store.get(infohash, include_blob=False)
        except Exception:
            existing = None
        try:
            _ignored = store.is_ignored(infohash) is True
        except Exception:
            _ignored = False
        try:
            _domain = announce_domain(announce)
        except Exception:
            _domain = ""
        if existing is not None:
            try:
                _st = existing.state.value
            except Exception:
                _st = "tracked"
            if _st == State.FAILED.value:
                if _ignored:
                    await self._reply(
                        f"Ignored (previously cancelled): {name}. "
                        f"Unignore it first, then add again.",
                        reply_to=message,
                    )
                    await self._delete_chat_file(message, doc_msg)
                    return
                # Operator re-provided the .torrent for a failed release
                # (usually to fuse-seed it as the final goal): revive the
                # row into a fresh pipeline run instead of refusing it.
                coord = getattr(self, "_coord", None)
                _revive = getattr(coord, "revive_failed_row", None)
                if coord is not None and callable(_revive):
                    try:
                        _revived = _revive(
                            infohash=infohash,
                            name=name,
                            size=size,
                            announce=announce or "",
                            blob=bytes(data),
                            source_label="telegram",
                        )
                    except LookupError as e:
                        await self._reply(_scrubbed(e)[:300], reply_to=message)
                        await self._delete_chat_file(message, doc_msg)
                        return
                    except Exception as e:  # noqa: BLE001
                        await self._reply(_action_error_text(e), reply_to=message)
                        return
                    await self._reply(
                        f"Re-queued {name} (was failed) — downloading on VPS2, watch its card.",
                        reply_to=message,
                    )
                    await self._delete_chat_file(message, doc_msg)
                    return
            try:
                _pre = existing.state in (State.NEW, State.WAITING_INDEXER, State.WAITING_DISK)
            except Exception:
                _pre = False
            if _pre:
                # The operator just handed us the exact bytes for a row
                # that has none usable (e.g. a racing row whose VPS1
                # original vanished while it parks for a Prowlarr
                # cross-seed that never comes). Same infohash means same
                # content, so keeping them can never substitute a wrong
                # release — and /fetch (or the next retry) can download
                # from them instead of waiting on the indexer. Narrow
                # blob write only: the row's state is left untouched.
                try:
                    _kept = bool(await asyncio.to_thread(store.set_blob, infohash, bytes(data)))
                except Exception:
                    _kept = False
                if _kept:
                    # Fresh input resumes a held row (same as a watch-dir
                    # re-drop): the operator is acting on it again.
                    try:
                        _upd = getattr(store, "update_columns", None)
                        _resumed = (
                            bool(await asyncio.to_thread(_upd, infohash, {"skipped": 0}))
                            if callable(_upd)
                            else False
                        )
                    except Exception:
                        _resumed = False
                    _tail = f" — it can now download from your bytes: `{_now_command(infohash)}`."
                    if _resumed and getattr(existing, "skipped", 0):
                        _tail = " — hold cleared, it resumes next tick (bytes attached too)."
                    await self._reply(
                        f"Attached supplied .torrent to {name} ({_st}){_tail}", reply_to=message
                    )
                    await self._delete_chat_file(message, doc_msg)
                    return
            await self._reply(f"Already tracked: {name} ({_st}).", reply_to=message)
            await self._delete_chat_file(message, doc_msg)
            return
        if _ignored:
            await self._reply(
                f"Ignored (previously cancelled): {name}. Nothing ingested.", reply_to=message
            )
            await self._delete_chat_file(message, doc_msg)
            return
        try:
            ts = TorrentState(
                source_infohash=infohash,
                source_name=name,
                total_bytes=size,
                source_announce_url=announce or "",
                source_tracker=announce or "",
                cross_seed_blob=bytes(data),
                cross_seed_source="telegram",
                state=State.NEW,
            )
        except Exception as e:  # noqa: BLE001
            await self._reply(
                f"Action failed: could not track {name} ({_scrubbed(e)}).", reply_to=message
            )
            return
        try:
            ts._blob = bytes(data)
        except Exception:
            pass
        try:
            _res = store.upsert(ts)
        except Exception as e:  # noqa: BLE001
            await self._reply(
                f"Action failed: could not track {name} ({_scrubbed(e)}).", reply_to=message
            )
            return
        # A tombstoned hash refuses the write (forget/cancel deleted the row).
        # Answering "Queued" there would claim a download that never starts.
        try:
            _written = _res is UpsertResult.WRITTEN
        except Exception:
            _written = True
        if not _written:
            await self._reply(
                f"Not queued: {name} ({infohash[:10]}) is tombstoned "
                "(forgotten or cancelled); run unignore first.",
                reply_to=message,
            )
            await self._delete_chat_file(message, doc_msg)
            return
        try:
            log.info(
                "telegram ingest: %s (%s, %d bytes) announce=%s",
                name[:60],
                infohash[:10],
                size,
                _domain or "?",
            )
        except Exception:
            pass
        await self._reply(
            f"Queued {name} ({_size_compact(size)}) via "
            f"{_domain or 'unknown tracker'} — downloading on VPS2, "
            "watch its card.",
            reply_to=message,
        )
        await self._delete_chat_file(message, doc_msg)

    async def _delete_chat_file(self, cmd_msg: Any, doc_msg: Any) -> None:
        """Delete the ingested .torrent message (passkey hygiene).

        .torrent files embed the sender's per-user tracker passkey: once
        the bytes are consumed the chat copy is pure liability. Best-effort
        — the bot needs message-deletion rights (admin in groups); on
        failure the operator is nudged to remove it by hand. Skipped
        entirely when `telegram.delete_processed_torrent` is false.
        """
        try:
            if not bool(getattr(getattr(self, "_cfg", None), "delete_processed_torrent", True)):
                return
        except Exception:
            pass
        try:
            bot = getattr(self, "_bot", None)
            _target = doc_msg if doc_msg is not None else cmd_msg
            chat = getattr(_target, "chat", None)
            chat_id = getattr(chat, "id", None)
            if chat_id is None:
                chat_id = getattr(getattr(self, "_cfg", None), "chat_id", None)
            msg_id = getattr(_target, "message_id", None)
            if bot is None or not isinstance(msg_id, int):
                raise RuntimeError("cannot delete chat file")
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:  # noqa: BLE001
            try:
                log.warning("telegram could not delete .torrent message: %s", e)
            except Exception:
                pass
            try:
                await self._reply(
                    "Note: could not delete the .torrent from chat (the bot "
                    "needs message-deletion rights) — please remove it by "
                    "hand; it contains your tracker passkey.",
                    reply_to=cmd_msg,
                )
            except Exception:
                pass

    async def _reply(self, text: str, reply_to: Any = None) -> None:
        """Best-effort chat reply (plain text, no markdown to parse)."""
        bot = getattr(self, "_bot", None)
        if bot is None:
            return
        try:
            kwargs: dict[str, Any] = {}
            try:
                msg_id = getattr(reply_to, "message_id", None)
                if isinstance(msg_id, int) and msg_id > 0:
                    kwargs["reply_to_message_id"] = msg_id
            except Exception:
                pass
            sent = await bot.send_message(self._cfg.chat_id, text, **kwargs)
            self._note_outbound(getattr(sent, "message_id", None))
        except Exception as e:  # noqa: BLE001
            log.debug("cancel reply failed: %s", e)
