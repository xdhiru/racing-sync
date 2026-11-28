"""Telegram active-message refresh (split from telegram_bot.py).

`RefreshMixin` carries the live-list / detail refresh machinery —
`_refresh_active_message`, footer/overview rendering support,
repost + orphan-sweep + pinned reconciliation, and `notify` — moved
verbatim; everything resolves through `self` (instance state) and
the `telegram_protocol` / `telegram_views` helpers.
"""

import asyncio
import logging
import time
from typing import Any

from telegram import InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut

from .state import State, TorrentState
from .telegram_protocol import (
    _SSD_FREE_TTL_S,
    _VPS1_FREE_TTL_S,
    _is_parse_error,
    _is_transient_tg_error,
)
from .telegram_views import (
    _esc,
    _group_active_items,
    _inflight_note_for,
    _safe_truncate_markdown,
    _size_compact,
    _speed_human,
    _tg_len,
    render_active,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")

#: Unchanged-card refreshes between liveness probes. The refresh edits
#: one long-lived card, so an unchanged card is never touched — a card
#: deleted from the chat would stay gone until the content changed.
_ACTIVE_PROBE_EVERY = 20


#: Telegram's hard message limit, counted in UTF-16 units (see _tg_len).
_TG_MESSAGE_LIMIT_UNITS = 4096


def _fit_active_card(text: str) -> str:
    """Truncate the active card only when Telegram would reject it.

    The gate must count UTF-16 units: an emoji-heavy card can stay under
    4096 code points while exceeding Telegram's 4096-unit limit, so a
    code-point gate would skip truncation and the edit would be refused.
    """
    try:
        if _tg_len(text) > _TG_MESSAGE_LIMIT_UNITS:
            return _safe_truncate_markdown(text, _TG_MESSAGE_LIMIT_UNITS)
        return text
    except Exception:
        return text


class RefreshMixin:
    async def _mark_detail_cancelled(
        self,
        message_id: int,
        name: str,
        infohash: str,
        *,
        remember: bool = True,
        delete_files: bool = True,
    ) -> None:
        """Edit a forgotten row's detail card to CANCELLED (best-effort).

        The DB row is already deleted, so this uses the snapshotted message
        id. Never raises: a deleted card or a down Bot API must not fail
        the cancel itself.

        The line reports the policy the operator picked: whether the row was
        ignored (and so can be un-ignored) and whether the data was removed.
        """
        bot = getattr(self, "_bot", None)
        if bot is None or not message_id:
            return
        shown = (name or infohash[:10])[:100]
        _h = (infohash or "").lower()
        if delete_files:
            _what = "removed + ignored" if remember else "removed, can be re-added"
        else:
            _what = (
                "untracked + ignored, data left in place"
                if remember
                else "untracked, data left in place (can be re-added)"
            )
        _hint = (
            f"Unignore: `/unignore {_h}`" if remember else "Re-drop the .torrent to track it again"
        )
        text = f"🚫 CANCELLED `{_esc(shown)}`\n`{_h}`\n✗ Cancelled by operator ({_what})\n{_hint}"
        try:
            await bot.edit_message_text(
                text,
                chat_id=self._cfg.chat_id,
                message_id=message_id,
                parse_mode=ParseMode.MARKDOWN,
            )
        except TelegramError as e:
            if _is_parse_error(e):
                try:
                    await bot.edit_message_text(
                        text,
                        chat_id=self._cfg.chat_id,
                        message_id=message_id,
                    )
                except Exception:
                    log.debug("cancel card plain edit failed: %s", e)
            else:
                # "not modified" / "not found" / transient — nothing to do.
                log.debug("cancel card edit skipped: %s", e)
        except Exception as e:  # noqa: BLE001
            log.debug("cancel card edit failed: %s", e)

    # ---- active tasks list ----

    async def _refresh_active_message(self) -> None:
        # Serialize periodic refresh vs callback-triggered refresh so
        # concurrent edits can't interleave or race the dedup cache.
        # (Lazy: unit tests build the bot via object.__new__.)
        # Coalesce: a caller arriving while a refresh runs marks dirty
        # and returns — the holder re-runs once — so a burst of taps
        # (or tick+taps) pays one extra pass, not N queued full passes
        # each re-stating the SSD and re-sending notes.
        lock = getattr(self, "_active_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._active_lock = lock
        if lock.locked():
            try:
                self._refresh_dirty = True
            except Exception:
                pass
            return
        async with lock:
            while True:
                try:
                    self._refresh_dirty = False
                except Exception:
                    pass
                await self._refresh_active_message_inner()
                try:
                    dirty = bool(getattr(self, "_refresh_dirty", False))
                except Exception:
                    dirty = False
                if not dirty:
                    break

    async def render_active_now(self) -> bool:
        """Send a fresh Active Tasks card now (operator recovery).

        The periodic refresh edits one long-lived card in place, so a card
        deleted from the chat — or a bot that latched the "chat not found"
        kill switch — leaves the operator with nothing and no way back.
        Re-arms the switch, forgets the cached render and sends a brand
        new card as the newest message.

        Returns True when a card was sent.
        """
        try:
            old_id = self._active_msg_id
        except Exception:
            old_id = None
        try:
            old_int = int(old_id) if old_id is not None else 0
        except (TypeError, ValueError):
            old_int = 0
        # Re-arm the kill switch ("chat not found" latched it to -1 and
        # every refresh has skipped the send since) and forget the cached
        # render, so the refresh cannot skip the send.
        try:
            self._active_msg_id = None
            self._prev_active_msg_id = None
            self._last_active_cache = None
            self._send_uncertain_key = None
            self._send_uncertain_skips = 0
        except Exception:
            pass
        # The stale card is retired by the orphan sweep (bounded, best
        # effort) instead of blocking the send on a delete.
        if old_int > 0:
            try:
                self._remember_orphan(old_int)
            except Exception:
                pass
        try:
            await self._refresh_active_message_inner(force=True)
        except Exception:
            log.exception("active-tasks manual re-render failed")
            return False
        try:
            new_id = self._active_msg_id
        except Exception:
            new_id = None
        return isinstance(new_id, int) and new_id > 0

    def _valid_byte_count(self, value: object) -> int | None:
        """int byte count, or None when missing/nonsense. Never raises."""
        try:
            if isinstance(value, bool):
                return None
            if isinstance(value, float):
                value = int(value)
            if isinstance(value, int) and value >= 0:
                return value
        except Exception:
            pass
        return None

    def _overview_line(self) -> str:
        """One-line download overview for /stats ("" when nothing live)."""
        try:
            fn = getattr(getattr(self, "_coord", None), "live_totals", None)
            tot = fn() if callable(fn) else (0, 0)
            bps_raw, n_raw = tot[0], tot[1]
        except Exception:
            return ""
        # Strict types: test doubles answer truthy to everything
        # (MagicMock.__int__ is 1) and must never render fake totals.
        if isinstance(bps_raw, bool) or not isinstance(bps_raw, (int, float)):
            return ""
        if isinstance(n_raw, bool) or not isinstance(n_raw, (int, float)):
            return ""
        bps, n = int(bps_raw), int(n_raw)
        if n <= 0:
            return ""
        try:
            spd = _speed_human(bps) or "0 B/s"
        except Exception:
            return ""
        return f"▼ {spd} · {n} downloading"

    async def _active_footer(self) -> str:
        """Storage footer for the active-tasks message.

        ``______________________`` divider on top, then::

            ·  VPS1 Free: 64.5G
            ·  SSD Free: 44.0G  ·  rsv 2.9G

        VPS1 unknown (SFTP probe failed/unavailable) renders as
        ``·  VPS1 Free: ?``; segments with no data are omitted.
        Best effort — returns "" when nothing is known. Never raises,
        so a failing probe can never break the refresh. No warning
        flags: low space stays the cleanup janitor's job.
        """
        try:
            coord = getattr(self, "_coord", None)
            cfg = getattr(coord, "cfg", None)
            if coord is None or cfg is None:
                return ""
            # VPS2 SSD free: cheap local syscall, but cached — a burst of
            # taps shares one stat instead of each paying it (display-only
            # footer; the cleanup janitor owns real low-space reaction).
            ssd_free: int | None = None
            try:
                _now_s = time.monotonic()
                _cached_s = getattr(self, "_ssd_free_cache", None)
                if (
                    _cached_s is not None
                    and len(_cached_s) == 2
                    and _now_s - float(_cached_s[0]) < _SSD_FREE_TTL_S
                ):
                    ssd_free = self._valid_byte_count(_cached_s[1])
                else:
                    from .rclone_ops import ssd_free_bytes as _ssd_free

                    ssd_free = self._valid_byte_count(await asyncio.to_thread(_ssd_free, cfg))
                    self._ssd_free_cache = (_now_s, ssd_free)
            except Exception:
                ssd_free = None
            # SSD reservation ledger: in-memory, no I/O.
            reserved: int | None = None
            try:
                _fn = getattr(coord, "_ssd_reserved_total", None)
                reserved = self._valid_byte_count(_fn() if callable(_fn) else None)
            except Exception:
                reserved = None
            # VPS1 free over SFTP: cached, so the 45s refresh tick doesn't
            # chatter the channel for a footer line.
            vps1_free: int | None = None
            try:
                _now = time.monotonic()
                _cached = getattr(self, "_vps1_free_cache", None)
                if (
                    _cached is not None
                    and len(_cached) == 2
                    and _now - float(_cached[0]) < _VPS1_FREE_TTL_S
                ):
                    vps1_free = self._valid_byte_count(_cached[1])
                else:
                    _probe = getattr(coord, "_source_free_bytes", None)
                    if callable(_probe):
                        vps1_free = self._valid_byte_count(await _probe())
                        self._vps1_free_cache = (_now, vps1_free)
            except Exception:
                vps1_free = None
            if ssd_free is None and vps1_free is None and not reserved:
                return ""
            # 22 raw "_" would parse as empty italic entities in legacy
            # Markdown and render invisible (that's why dashes showed but
            # underscores didn't) — escape so Telegram shows literal ____.
            _lines = [_esc("_" * 35)]
            if vps1_free is not None:
                _lines.append(f"·  VPS1 Free: {_size_compact(vps1_free)}")
            else:
                # SFTP probe failed / unavailable — explicit unknown.
                _lines.append("·  VPS1 Free: ?")
            if ssd_free is not None:
                _ssd_seg = f"·  SSD Free: {_size_compact(ssd_free)}"
                if reserved:
                    _ssd_seg += f"  ·  rsv {_size_compact(reserved)}"
                _lines.append(_ssd_seg)
            elif reserved:
                _lines.append(f"·  SSD Free: ?  ·  rsv {_size_compact(reserved)}")
            return "\n".join(_lines)
        except Exception:
            return ""

    async def _refresh_active_message_inner(self, force: bool = False) -> None:
        assert self._bot is not None
        # Sentinel -1 means "stop trying to edit" (e.g. chat permission issue)
        if self._active_msg_id == -1 and not force:
            return
        # Retired active ids first: a flood-delayed delete from an earlier
        # repost must not strand an old card in history forever.
        try:
            await self._sweep_orphan_active_messages()
        except Exception:
            pass
        rows = await asyncio.to_thread(self._store.list_active_inflight)
        # Pull live progress + download rates from coordinator's tracker
        progress_map = self._coord.live_progress_map()
        try:
            speed_map = self._coord.live_speed_map()
            if not isinstance(speed_map, dict):
                speed_map = {}
        except Exception:
            speed_map = {}
        items: list[tuple[TorrentState, float | None]] = [
            (ts, progress_map.get(ts.source_infohash.lower())) for ts in rows
        ]
        # Stale-card net: re-queue detail updates whose card drifted from
        # the row (edits lost to flood bans, restarts mid-edit). Steady
        # state is silent — only drift enqueues, with live progress, and
        # the worker batch coalesces duplicates. Hashes on a card-update
        # fail streak sit out (their route already proved dead — every
        # refresh re-driving them burns an API call each pass); every 8th
        # skip re-drives once so a healed route still recovers, and any
        # success clears the streak.
        try:
            _sent = self._sent_state_map()
            try:
                _fail_streak = getattr(self, "_detail_fail_streak", None)
                if not isinstance(_fail_streak, dict):
                    _fail_streak = {}
            except Exception:
                _fail_streak = {}
            try:
                _drift_skip = getattr(self, "_detail_drift_skip", None)
                if not isinstance(_drift_skip, dict):
                    _drift_skip = {}
                    self._detail_drift_skip = _drift_skip
            except Exception:
                _drift_skip = {}
            for _ts, _prog in items:
                try:
                    _h = _ts.source_infohash or ""
                    if not _h:
                        continue
                    if _sent.get(_h) != _ts.state.value:
                        try:
                            _n = int(_fail_streak.get(_h, 0) or 0)
                        except Exception:
                            _n = 0
                        if _n >= 5:
                            try:
                                _c = int(_drift_skip.get(_h, 0) or 0) + 1
                                _drift_skip[_h] = _c
                            except Exception:
                                _c = 1
                            if _c < 8:
                                continue
                            try:
                                _drift_skip.pop(_h, None)
                            except Exception:
                                pass
                        self._enqueue_detail(_h, _prog)
                except Exception:
                    continue
            self._prune_detail_drift_skip({(t.source_infohash or "") for t, _ in items})
        except Exception:
            pass
        # Terminal-card net: DONE/FAILED rows leave the active list above,
        # so a single timeout on the final update would freeze the card at
        # MOVING/RE_ADDING forever. Retry recorded hashes a bounded number
        # of refreshes (one enqueue per refresh — no hot loop).
        try:
            _tmap = self._detail_terminal_map()
            _tstore = getattr(self, "_store", None)
            if _tmap and _tstore is not None:
                _sent2 = self._sent_state_map()
                for _hh, _n in list(_tmap.items()):
                    try:
                        _rr = await asyncio.to_thread(_tstore.get, _hh)
                    except Exception:
                        _rr = None
                    try:
                        _rv = _rr.state.value if _rr is not None else ""
                    except Exception:
                        _rv = ""
                    if _rr is None or _rv not in ("done", "failed") or _sent2.get(_hh) == _rv:
                        _tmap.pop(_hh, None)
                        continue
                    try:
                        _ni = int(_n or 0)
                    except (TypeError, ValueError):
                        _ni = 0
                    if _ni >= 15:
                        _tmap.pop(_hh, None)
                        log.debug(
                            "terminal card retry budget spent for %s; leaving card as-is", _hh[:10]
                        )
                        continue
                    _tmap[_hh] = _ni + 1
                    self._enqueue_detail(_hh, None)
        except Exception:
            pass
        # Deferral notes for waiting rows (watch election notes plus
        # same-content in-flight deferrals) so the list explains itself.
        # Best-effort: never break the refresh over a note.
        notes: dict[str, str] = {}
        try:
            _coord = getattr(self, "_coord", None)
            _wait_note = getattr(_coord, "_watch_wait_note", None)
            _inflight_of = getattr(_coord, "_inflight_same_content", None)
            for _ts, _ in items:
                try:
                    if _ts.state in (State.NEW, State.WAITING_DISK):
                        if callable(_wait_note):
                            _n = _wait_note(_ts) or ""
                            if _n:
                                notes[_ts.source_infohash or ""] = _n
                    elif _ts.state == State.WAITING_INDEXER and callable(_inflight_of):
                        _n = _inflight_note_for(_ts, _inflight_of(_ts)) or ""
                        if _n:
                            notes[_ts.source_infohash or ""] = _n
                except Exception:
                    continue
        except Exception:
            notes = {}
        footer = await self._active_footer()
        # Live rclone lines for MOVING rows (strict dict: test doubles
        # answer truthy to everything and must degrade to no lines).
        try:
            _mp_fn = getattr(self._coord, "rclone_move_progress_map", None)
            move_progress = _mp_fn() if callable(_mp_fn) else {}
            if not isinstance(move_progress, dict):
                move_progress = {}
        except Exception:
            move_progress = {}
        # Status filter (All/DL/MV/RD/QD buttons): the detail nets above
        # run on the FULL list (filtered-out cards still heal); only the
        # render + numbering narrow. Unknown keys show everything.
        try:
            _flt = getattr(self, "_status_filter", None) or "all"
        except Exception:
            _flt = "all"
        try:
            from .telegram_protocol import match_status_filter as _matchflt

            if _flt == "all":
                shown = items
            else:
                shown = [
                    (_ts, _pg)
                    for (_ts, _pg) in items
                    if _matchflt(getattr(getattr(_ts, "state", None), "value", ""), _flt)
                ]
        except Exception:
            shown = items
            try:
                _flt = "all"
            except Exception:
                pass
        text, cur_page, total_pages = render_active(
            shown,
            page=self._current_page,
            page_size=self._cfg.page_size,
            notes=notes,
            footer=footer,
            speeds=speed_map,
            move_progress=move_progress,
        )
        # Stamp the rendered generation: typed /act_N positions resolve
        # against the live list, so a count change between render and tap
        # (a group finished, a dupe arrived) warns instead of misacting.
        try:
            self._last_rendered_groups = len(_group_active_items(list(shown or [])))
            self._last_rendered_page = cur_page
        except Exception:
            pass
        text = _fit_active_card(text)
        self._current_page = cur_page
        try:
            _kb_filter = getattr(self, "_status_filter", None) or "all"
        except Exception:
            _kb_filter = "all"
        keyboard = self._build_keyboard(cur_page, total_pages, status_filter=_kb_filter)

        # Skip the API call if page, total_pages and text are identical
        # — unless a keep-at-bottom repost is due (position refreshes
        # even when the content is unchanged). Button digests stay in
        # the key: a button-only change must re-send.
        try:
            cache_key = (cur_page, total_pages, text)
        except Exception:
            cache_key = (cur_page, total_pages, text)

        repost_due = self._repost_due()
        if (
            cache_key == self._last_active_cache
            and self._active_msg_id is not None
            and not repost_due
            and not force
            and not self._active_probe_due()
        ):
            return
        # Uncertain-send guard: a send that timed out may still have landed
        # (lost response looks identical) — resending blindly next tick mints
        # detached duplicates with unknowable ids that no cleanup can ever
        # delete. While the text is unchanged, assume it landed and skip;
        # any text change (or due repost) sends again. First-send-ever (no
        # known id at all) keeps retrying: nothing could have landed before,
        # and the chat needs its active message.
        try:
            _ukey = getattr(self, "_send_uncertain_key", None)
        except Exception:
            _ukey = None
        if _ukey is not None and not repost_due and not force:
            try:
                _same_key = _ukey == cache_key
            except Exception:
                _same_key = False
            if _same_key:
                try:
                    _have_id = (
                        self._active_msg_id is not None or (self._prev_active_msg_id or 0) > 0
                    )
                except Exception:
                    _have_id = False
                if _have_id:
                    return
                # No known message at all: the chat may genuinely lack the
                # active message (first send truly failed). Retry throttled
                # — every 4th tick — so a landed-but-unconfirmed send mints
                # at most one duplicate per minute instead of one per
                # interval, while a real failure still appears within
                # minutes. Attempt branches below reset the counter.
                try:
                    _n = int(getattr(self, "_send_uncertain_skips", 0) or 0) + 1
                except Exception:
                    _n = 1
                try:
                    self._send_uncertain_skips = _n
                except Exception:
                    pass
                if _n < 4:
                    return

        if self._active_msg_id is None:
            # Retire the previous id before resending: a flood-delayed
            # delete must NEVER be followed by a send — that pair strands
            # the old card while the pointer moves on. Transient delete
            # failures abort this tick (the old card stays live); only a
            # deleted, already-gone, or undeletable id proceeds to send.
            try:
                _prev_id = int(self._prev_active_msg_id or 0)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                _prev_id = 0
            if _prev_id > 0:
                try:
                    await self._bot.delete_message(
                        chat_id=self._cfg.chat_id,
                        message_id=_prev_id,
                    )
                    self._prev_active_msg_id = None
                except (RetryAfter, TimedOut, NetworkError) as _prev_e:
                    log.warning(
                        "active-tasks previous message %s delete deferred "
                        "(transient %s); keeping it, will retry next "
                        "interval",
                        _prev_id,
                        _prev_e,
                    )
                    # Feed the orphan sweep too: if the retry path ever
                    # stops firing, the sweep still retires this id.
                    try:
                        self._remember_orphan(_prev_id)
                    except Exception:
                        pass
                    return
                except TelegramError as _prev_e:
                    _prev_msg = str(_prev_e).lower()
                    if self._delete_gone(_prev_msg):
                        log.debug(
                            "active-tasks previous message %s already gone (%s); resending",
                            _prev_id,
                            _prev_e,
                        )
                        self._prev_active_msg_id = None
                    elif self._delete_hopeless(_prev_msg):
                        log.warning(
                            "active-tasks previous message %s delete refused "
                            "(%s); abandoning it and resending",
                            _prev_id,
                            _prev_e,
                        )
                        self._prev_active_msg_id = None
                        # The sweep drops it as hopeless (or deletes it if
                        # rights are restored later).
                        try:
                            self._remember_orphan(_prev_id)
                        except Exception:
                            pass
                    elif (
                        _is_transient_tg_error(_prev_e)
                        or "flood control" in _prev_msg
                        or "too many requests" in _prev_msg
                        or "timed out" in _prev_msg
                        or "timeout" in _prev_msg
                        or "connection" in _prev_msg
                        or "retry after" in _prev_msg
                        or "retry in" in _prev_msg
                    ):
                        log.warning(
                            "active-tasks previous message %s delete "
                            "rate-limited (%s); keeping it, will retry next "
                            "interval",
                            _prev_id,
                            _prev_e,
                        )
                        return
                    else:
                        log.warning(
                            "active-tasks previous message %s delete failed "
                            "(%s); keeping it, will retry next interval",
                            _prev_id,
                            _prev_e,
                        )
                        return
                except Exception as _prev_e:  # noqa: BLE001
                    log.warning(
                        "active-tasks previous message %s delete failed "
                        "(%s); keeping it, will retry next interval",
                        _prev_id,
                        _prev_e,
                    )
                    return

            # New send attempt supersedes any uncertain earlier send.
            self._send_uncertain_key = None
            self._send_uncertain_skips = 0
            try:
                sent = await self._bot.send_message(
                    self._cfg.chat_id,
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
                self._active_msg_id = sent.message_id
                self._prev_active_msg_id = sent.message_id
                self._last_active_cache = cache_key
                self._last_repost_monotonic = time.monotonic()
                self._note_outbound(sent.message_id)
                await asyncio.to_thread(
                    self._store.set_meta, "telegram_active_msg_id", str(sent.message_id)
                )
            except (TimedOut, NetworkError) as e:
                log.warning("active-tasks send timed out (%s); will retry next interval", e)
                # Assume landed (lost responses look identical) — the
                # uncertain-send guard above skips blind resends while the
                # text is unchanged, bounding detached duplicates.
                self._last_active_cache = cache_key
                self._send_uncertain_key = cache_key
            except TelegramError as e:
                if _is_parse_error(e):
                    try:
                        sent = await self._bot.send_message(
                            self._cfg.chat_id,
                            text,
                            reply_markup=keyboard,
                        )
                        self._active_msg_id = sent.message_id
                        self._prev_active_msg_id = sent.message_id
                        self._last_active_cache = cache_key
                        self._last_repost_monotonic = time.monotonic()
                        self._note_outbound(sent.message_id)
                        await asyncio.to_thread(
                            self._store.set_meta, "telegram_active_msg_id", str(sent.message_id)
                        )
                    except Exception as e2:
                        log.warning("active-tasks plain send failed: %s", e2)
                        if _is_transient_tg_error(e2):
                            self._last_active_cache = cache_key
                            self._send_uncertain_key = cache_key
                else:
                    log.warning("active-tasks send failed: %s", e)
        else:
            # New edit attempt supersedes any uncertain earlier send.
            self._send_uncertain_key = None
            self._send_uncertain_skips = 0
            if repost_due:
                await self._repost_active_message(text, keyboard, cache_key)
                return
            try:
                await self._bot.edit_message_text(
                    text,
                    chat_id=self._cfg.chat_id,
                    message_id=self._active_msg_id,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
                self._last_active_cache = cache_key
            except TelegramError as e:
                msg = str(e).lower()
                # "Message is not modified" — Telegram returns 400 when byte-identical
                if "not modified" in msg:
                    self._last_active_cache = cache_key
                    return

                if _is_parse_error(e):
                    try:
                        await self._bot.edit_message_text(
                            text,
                            chat_id=self._cfg.chat_id,
                            message_id=self._active_msg_id,
                            reply_markup=keyboard,
                        )
                        self._last_active_cache = cache_key
                        return
                    except Exception as e2:
                        log.warning("active-tasks plain edit failed: %s", e2)

                # Rate limit / timeout / network error:
                # NEVER clear _active_msg_id on temporary glitches!
                if (
                    isinstance(e, (RetryAfter, TimedOut, NetworkError))
                    or "flood control" in msg
                    or "too many requests" in msg
                    or "timed out" in msg
                    or "timeout" in msg
                    or "connection" in msg
                ):
                    log.warning(
                        "active-tasks edit skipped due to temporary network/rate-limit (%s); will retry next interval",
                        e,
                    )
                    return

                # "Chat not found" — permission issue
                if "chat not found" in msg:
                    log.error(
                        "active-tasks edit failed: 'Chat not found'. "
                        "Disabling active-tasks updates."
                    )
                    self._active_msg_id = -1
                    await asyncio.to_thread(self._store.set_meta, "telegram_active_msg_id", "")
                    return

                # "Message to edit not found" / "MESSAGE_ID_INVALID" — deleted from chat
                if "message to edit not found" in msg or "message_id_invalid" in msg:
                    log.warning("active-tasks message not found in chat; will resend")
                    self._active_msg_id = None
                    self._last_active_cache = None
                    await asyncio.to_thread(self._store.set_meta, "telegram_active_msg_id", "")
                    return

                # Other errors — keep message_id for next retry
                log.warning("active-tasks edit failed (%s); keeping message_id for next retry", e)

    def _repost_interval(self) -> float:
        """Keep-at-bottom cadence in seconds; 0.0 means disabled."""
        try:
            raw = float(getattr(self._cfg, "active_repost_interval_seconds", 0) or 0)
        except (TypeError, ValueError):
            return 0.0
        if raw <= 0:
            return 0.0
        return max(5.0, raw)

    def _repost_due(self) -> bool:
        """True when the active message is buried AND a resend is due.

        Burial is proven by our own newer outbound traffic (per-torrent
        detail cards, notifications) carrying a higher message id — the Bot
        API exposes no chat history. No newer traffic means the message is
        still last and a delete+resend would be pure churn.
        """
        interval = self._repost_interval()
        if interval <= 0:
            return False
        if not self._active_msg_id or self._active_msg_id == -1:
            return False
        newest = getattr(self, "_newest_outbound_id", None)
        if not isinstance(newest, int) or newest <= self._active_msg_id:
            return False
        try:
            last = float(getattr(self, "_last_repost_monotonic", 0.0) or 0.0)
        except (TypeError, ValueError):
            last = 0.0
        return (time.monotonic() - last) >= interval

    def _prune_detail_drift_skip(self, live: set[str]) -> None:
        """Drop drift counters for hashes no longer on the active list.

        The counter is only cleared when it reaches the re-drive threshold,
        so a hash that drifted once and then settled kept its key forever —
        one entry per torrent the daemon had ever shown.
        """
        try:
            d = getattr(self, "_detail_drift_skip", None)
            if not isinstance(d, dict):
                return
            for k in [k for k in d if k not in live]:
                d.pop(k, None)
            if len(d) > 5000:
                for k in list(d.keys())[: len(d) - 5000]:
                    d.pop(k, None)
        except Exception:
            pass

    def _active_probe_due(self) -> bool:
        """True every N-th unchanged refresh: prove the card still exists.

        An unchanged card is never edited, so one deleted from the chat
        would stay gone until the content changed. A periodic edit
        attempt proves liveness (Telegram answers "not modified") and the
        message-not-found path resends it.
        """
        n = getattr(self, "_active_probe_count", 0) + 1
        if n >= _ACTIVE_PROBE_EVERY:
            # Probe fired: restart the cadence so the next probe is
            # another N-th unchanged refresh away.
            self._active_probe_count = 0
            return True
        self._active_probe_count = n
        return False

    def _note_outbound(self, message_id: object) -> None:
        """Record an outbound message id for burial detection (best-effort)."""
        try:
            mid = int(message_id)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return
        try:
            cur = getattr(self, "_newest_outbound_id", None)
            if not isinstance(cur, int) or mid > cur:
                self._newest_outbound_id = mid
        except Exception:
            pass

    def _orphan_ids(self) -> list[int]:
        """Retired active ids awaiting a delete retry (creates on demand).

        Unit-test doubles build the bot via object.__new__ (no __init__),
        so every access goes through here instead of assuming attributes.
        """
        try:
            ids = getattr(self, "_orphan_active_ids", None)
            if not isinstance(ids, list):
                ids = []
                self._orphan_active_ids = ids
            return ids
        except Exception:
            return []

    def _remember_orphan(self, message_id: object) -> None:
        """Queue a retired active id for the sweep; bounded, deduped."""
        try:
            mid = int(message_id)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return
        if mid <= 0:
            return
        try:
            if mid == self._active_msg_id:
                return
        except Exception:
            pass
        try:
            ids = self._orphan_ids()
            if mid in ids:
                return
            ids.append(mid)
            del ids[:-20]
        except Exception:
            pass

    @staticmethod
    def _delete_gone(msg: str) -> bool:
        """True when a delete failure means the message is already gone."""
        try:
            low = (msg or "").lower()
        except Exception:
            return False
        return any(
            s in low
            for s in (
                "message to delete not found",
                "message to edit not found",
                "message_id_invalid",
                "message not found",
                "couldn't find the message",
            )
        )

    @staticmethod
    def _delete_hopeless(msg: str) -> bool:
        """True when retrying a delete can never succeed (rights/chat)."""
        try:
            low = (msg or "").lower()
        except Exception:
            return False
        return any(
            s in low
            for s in (
                "chat not found",
                "not enough rights",
                "need administrator",
                "not an administrator",
                "bot was kicked",
                "bot was blocked",
                "forbidden",
            )
        )

    async def _sweep_orphan_active_messages(self) -> None:
        """Best-effort delete of retired active ids (bounded per refresh).

        Runs on every active refresh so a flood-delayed delete from a
        repost cannot strand an old Active Tasks card in history forever.
        At most 3 ids per pass so the sweep itself cannot cause a flood.
        Never raises.
        """
        try:
            bot = getattr(self, "_bot", None)
            if bot is None:
                return
            deleter = getattr(bot, "delete_message", None)
            if not callable(deleter):
                return
            try:
                chat_id = self._cfg.chat_id
            except Exception:
                return
            ids = self._orphan_ids()
            if not ids:
                return
            for mid in list(ids)[:3]:
                try:
                    await deleter(chat_id=chat_id, message_id=mid)
                except Exception as e:  # noqa: BLE001
                    msg = str(e).lower()
                    if self._delete_gone(msg) or self._delete_hopeless(msg):
                        try:
                            ids.remove(mid)
                        except ValueError:
                            pass
                        continue
                    log.warning(
                        "active-tasks orphan delete %s deferred (%s); will retry next interval",
                        mid,
                        e,
                    )
                    continue
                try:
                    ids.remove(mid)
                except ValueError:
                    pass
                log.info("active-tasks orphan %s deleted", mid)
        except Exception:
            pass

    async def _repost_active_message(
        self,
        text: str,
        keyboard: InlineKeyboardMarkup | None,
        cache_key: tuple[int, int, str],
    ) -> None:
        """Delete the old active message and resend it silently as newest.

        Delete-first with abort: the resend only goes out once the old
        card is gone (or proven already gone). A flood-delayed delete
        must NEVER be followed by a send — that pair strands the old
        card in history forever while the pointer moves on. Transient
        delete failures keep the old id so the next refresh retries;
        send failures degrade to "resend fresh next tick" with the
        uncertain-send guard bounding lost-response duplicates.

        Sends with notifications disabled so the periodic bump never
        buzzes the chat.
        """
        assert self._bot is not None
        old_id = self._active_msg_id
        try:
            old_int = int(old_id)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return
        if old_int <= 0:
            return
        try:
            await self._bot.delete_message(
                chat_id=self._cfg.chat_id,
                message_id=old_int,
            )
            # The old card is proven gone: forget the dead id so the
            # uncertain-send guard treats the bot as having no known
            # message and retries the send instead of stranding the chat.
            self._prev_active_msg_id = None
        except (RetryAfter, TimedOut, NetworkError) as e:
            log.warning(
                "active-tasks repost delete deferred "
                "(transient %s); keeping old message, will retry next "
                "interval",
                e,
            )
            try:
                self._remember_orphan(old_int)
            except Exception:
                pass
            return
        except TelegramError as e:
            msg = str(e).lower()
            if self._delete_gone(msg):
                log.debug("active-tasks repost: old message %s already gone; resending", old_int)
                # Already gone: the dead id must not survive as a
                # "live" id — the guard would skip the resend forever.
                self._prev_active_msg_id = None
            elif self._delete_hopeless(msg):
                log.warning(
                    "active-tasks repost delete refused (%s); keeping old message in place",
                    e,
                )
                try:
                    self._last_repost_monotonic = time.monotonic()
                except Exception:
                    pass
                return
            elif (
                _is_transient_tg_error(e)
                or "flood control" in msg
                or "too many requests" in msg
                or "timed out" in msg
                or "timeout" in msg
                or "connection" in msg
                or "retry after" in msg
                or "retry in" in msg
            ):
                log.warning(
                    "active-tasks repost delete rate-limited (%s); "
                    "keeping old message, will retry next interval",
                    e,
                )
                try:
                    self._remember_orphan(old_int)
                except Exception:
                    pass
                return
            else:
                log.warning(
                    "active-tasks repost delete failed (%s); keeping old "
                    "message, will retry next interval",
                    e,
                )
                return
        except Exception as e:  # noqa: BLE001
            log.warning(
                "active-tasks repost delete failed (%s); keeping old "
                "message, will retry next interval",
                e,
            )
            return
        try:
            try:
                sent = await self._bot.send_message(
                    self._cfg.chat_id,
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                    disable_notification=True,
                )
            except TelegramError as e:
                if _is_parse_error(e):
                    sent = await self._bot.send_message(
                        self._cfg.chat_id,
                        text,
                        reply_markup=keyboard,
                        disable_notification=True,
                    )
                else:
                    raise
        except (RetryAfter, TimedOut, NetworkError) as e:
            log.warning(
                "active-tasks repost skipped due to temporary network/rate-limit (%s); will retry next interval",
                e,
            )
            self._active_msg_id = None
            # Assume the resend landed (lost responses look identical) so
            # the uncertain-send guard skips blind resends while the text
            # is unchanged. A previous id always existed here (repost only
            # runs with a known id), so the first-send-ever exception in
            # the guard never triggers from this path.
            self._last_active_cache = cache_key
            self._send_uncertain_key = cache_key
            await asyncio.to_thread(self._store.set_meta, "telegram_active_msg_id", "")
            return
        except TelegramError as e:
            log.warning("active-tasks repost failed (%s); will resend next interval", e)
            self._active_msg_id = None
            if _is_transient_tg_error(e):
                self._last_active_cache = cache_key
                self._send_uncertain_key = cache_key
            else:
                self._last_active_cache = None
                self._send_uncertain_key = None
            await asyncio.to_thread(self._store.set_meta, "telegram_active_msg_id", "")
            return
        self._active_msg_id = sent.message_id
        self._prev_active_msg_id = sent.message_id
        self._last_active_cache = cache_key
        self._last_repost_monotonic = time.monotonic()
        self._note_outbound(sent.message_id)
        self._send_uncertain_key = None
        await asyncio.to_thread(
            self._store.set_meta, "telegram_active_msg_id", str(sent.message_id)
        )

    async def _send_overview_message(self, query: Any = None) -> None:
        """Answer the 📊 button: one-line fleet aggregate + state chips.

        `(bps, n)` downloads from live_totals(), rclone move count/rate
        from the progress map, per-state counts from the active list.
        Operator-requested, so a standalone reply (threaded to the tapped
        list message when available). Never raises.
        """
        try:
            _bps, _n = 0, 0
            try:
                _fn = getattr(getattr(self, "_coord", None), "live_totals", None)
                _tot = _fn() if callable(_fn) else (0, 0)
                _bps = int(_tot[0]) if not isinstance(_tot[0], bool) else 0
                _n = int(_tot[1]) if not isinstance(_tot[1], bool) else 0
            except Exception:
                pass
            _rn, _rs = 0, 0
            try:
                from .rclone_ops import rclone_progress_map

                _rm = rclone_progress_map()
                _rn = len(_rm)
                for _rp in list(_rm.values()):
                    try:
                        _rs += max(0, int(getattr(_rp, "speed_bps", 0) or 0))
                    except Exception:
                        continue
            except Exception:
                pass
            _counts: dict[str, int] = {}
            try:
                _rows = await asyncio.to_thread(self._store.list_active_inflight)
                for _r in _rows or []:
                    try:
                        _sv = getattr(getattr(_r, "state", None), "value", "")
                        _counts[_sv] = _counts.get(_sv, 0) + 1
                    except Exception:
                        continue
            except Exception:
                pass
            try:
                _dl = _speed_human(_bps) or "0 B/s"
            except Exception:
                _dl = "0 B/s"
            _parts = [f"▼ {_dl} · {_n} downloading"]
            if _rn:
                try:
                    _rl = _speed_human(_rs) or "0 B/s"
                except Exception:
                    _rl = "0 B/s"
                _parts.append(f"rclone {_rn} moving · {_rl}")
            _chips = []
            for _sv, _tag in (
                ("downloading", "DL"),
                ("moving", "MV"),
                ("re_adding", "RD"),
                ("queued", "Q"),
                ("waiting_disk", "WD"),
                ("waiting_indexer", "WI"),
                ("new", "NEW"),
            ):
                try:
                    if _counts.get(_sv):
                        _chips.append(f"{_tag}{_counts[_sv]}")
                except Exception:
                    continue
            if _chips:
                _parts.append(" · ".join(_chips))
            _text = "📊 " + " · ".join(_parts)
            try:
                _reply_to = getattr(query, "message", None)
            except Exception:
                _reply_to = None
            try:
                await self._reply(_text[:500], reply_to=_reply_to)
            except Exception:
                pass
        except Exception:
            pass

    async def _reconcile_pinned(self) -> None:
        """Telegram allows at most one pinned message per chat; pin ours."""
        if not self._cfg.pin_status_message or self._bot is None:
            return
        if self._active_msg_id is None or self._active_msg_id == self._pinned_message_id:
            return
        # A persisted disable survives restarts (an in-memory cfg flip
        # re-spams pin attempts every boot).
        try:
            if getattr(self, "_pin_disabled", False):
                return
            if not getattr(self, "_pin_disabled_checked", False):
                self._pin_disabled_checked = True
                try:
                    _dis = await asyncio.to_thread(self._store.get_meta, "telegram_pin_disabled")
                except Exception:
                    _dis = None
                if str(_dis or "") == "1":
                    self._pin_disabled = True
                    return
        except Exception:
            pass
        try:
            await self._bot.pin_chat_message(
                self._cfg.chat_id,
                self._active_msg_id,
            )
            self._pinned_message_id = self._active_msg_id
        except TelegramError as e:
            # Permanent failures (no rights, bad request) disable further
            # pin attempts persistently; transient rate-limit/network
            # errors keep the setting — the next refresh retries.
            msg = str(e).lower()
            transient = isinstance(e, (RetryAfter, TimedOut, NetworkError))
            transient = transient or "retry after" in msg or "flood" in msg or "timeout" in msg
            if transient:
                log.warning("pin deferred (transient %s); will retry", e)
                return
            log.warning("pin failed (%s); disabling persistently", e)
            try:
                self._pin_disabled = True
            except Exception:
                pass
            try:
                await asyncio.to_thread(self._store.set_meta, "telegram_pin_disabled", "1")
            except Exception:
                pass

    # ---- one-shot notification (errors that don't belong to a torrent) ----

    async def notify(self, message: str) -> None:
        if not self._cfg.enabled or self._bot is None:
            return
        truncated = _safe_truncate_markdown(message)
        try:
            sent = await self._bot.send_message(
                self._cfg.chat_id,
                truncated,
                parse_mode=ParseMode.MARKDOWN,
            )
            self._note_outbound(getattr(sent, "message_id", None))
        except TelegramError as e:
            # Fallback without parse_mode if Markdown parsing or entity error occurs
            log.warning("telegram notify markdown failed (%s); retrying as plain text", e)
            try:
                plain_text = message[:4093] + "..." if len(message) > 4096 else message
                sent = await self._bot.send_message(self._cfg.chat_id, plain_text)
                self._note_outbound(getattr(sent, "message_id", None))
            except TelegramError as e2:
                log.warning("telegram notify failed: %s", e2)
