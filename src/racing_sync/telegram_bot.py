"""Telegram bot.

Two surfaces, both using the chat's edit-in-place mechanism so chat history
stays clean:

  1. **Per-torrent detail message** — one Telegram message per source_infohash.
     Created when the torrent first leaves NEW. Edited in place as it advances
     through states (NEW → WAITING_INDEXER → ... → DONE). The
     final DONE message stays in the chat as a clean history record.
     The message_id is persisted in torrent_state.telegram_message_id.

   2. **Active-tasks message** — one message at the bottom of the chat that
      lists every torrent currently in flight (anything != DONE / FAILED).
      Edited every status_update_interval. Pinned if pin_status_message=true.
      Optionally re-posted (deleted + silently resent) when our own newer
      messages have buried it AND active_repost_interval_seconds has elapsed,
      so it returns to newest-message position without churning while it is
      already last.

App logging goes to local files only (no Telegram forwarding).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from typing import Any

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut

from .config import TelegramConfig
from .coordinator import Coordinator
from .state import State, StateStore, TorrentState
from .telegram_actions import ActionsMixin
from .telegram_buttons import validate_rows
from .telegram_protocol import (
    _CHAT_COMMANDS,
    _MATCH_MAX_BYTES,
    _MATCH_MIN_CHARS,
    ACT_CMD_RE,
    ADD_CMD_RE,
    CANCEL_CMD_RE,
    CANCEL_MATCH_RE,
    CANCEL_STALLED_RE,
    FILES_RE,
    FIND_RE,
    INJECTFUSE_CMD_RE,
    INJECTFUSE_HASH_RE,
    STATS_RE,
    TICK_RE,
    CancelScope,
    ScopeGone,
    _bot_command_menu,
    _flood_wait_seconds,
    _is_bulk_scope,
    _is_hex40,
    _is_parse_error,
    _is_transient_tg_error,
    _match_query,
    _match_token,
    _now_command,
    _parse_action_data,
    _resume_command,
    _slow_tap,
    _with_flood_margin,
)
from .telegram_refresh import RefreshMixin
from .telegram_router import RouterMixin
from .telegram_sheets import SheetsMixin
from .telegram_views import (
    _active_note,
    _active_state_text,
    _batch_display,
    _bytes_human,
    _compact_wait_note,
    _esc,
    _eta_human,
    _group_active_items,
    _inflight_note_for,
    _is_grace_note_for_prefer,
    _list_bar,
    _live_group_by_gid,
    _member_button_labels,
    _member_domain,
    _member_hash,
    _now_eligible_hashes,
    _pick_member_label,
    _retry_minutes,
    _row_text_bits,
    _safe_display_name,
    _safe_truncate_markdown,
    _short_tracker_label,
    _size_compact,
    _speed_human,
    _tracker_domain,
    render_active,
    render_detail,
)

__all__ = [
    "TelegramBot",
    "ACT_CMD_RE",
    "ADD_CMD_RE",
    "CANCEL_CMD_RE",
    "CANCEL_MATCH_RE",
    "CANCEL_STALLED_RE",
    "FILES_RE",
    "FIND_RE",
    "INJECTFUSE_CMD_RE",
    "INJECTFUSE_HASH_RE",
    "STATS_RE",
    "TICK_RE",
    "CancelScope",
    "ScopeGone",
    "_active_note",
    "_active_state_text",
    "_batch_display",
    "_bot_command_menu",
    "_bytes_human",
    "_CHAT_COMMANDS",
    "_compact_wait_note",
    "_esc",
    "_eta_human",
    "_flood_wait_seconds",
    "_group_active_items",
    "_inflight_note_for",
    "_is_bulk_scope",
    "_is_grace_note_for_prefer",
    "_is_hex40",
    "_list_bar",
    "_live_group_by_gid",
    "_MATCH_MAX_BYTES",
    "_MATCH_MIN_CHARS",
    "_match_query",
    "_match_token",
    "_member_button_labels",
    "_member_domain",
    "_member_hash",
    "_now_command",
    "_now_eligible_hashes",
    "_parse_action_data",
    "_pick_member_label",
    "_resume_command",
    "_retry_minutes",
    "_row_text_bits",
    "_safe_display_name",
    "_safe_truncate_markdown",
    "_short_tracker_label",
    "_size_compact",
    "_slow_tap",
    "_speed_human",
    "_tracker_domain",
    "_with_flood_margin",
    "render_active",
    "render_detail",
]

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Actor auth
# --------------------------------------------------------------------------- #


def _tg_actor_id(obj: Any) -> str | None:
    """Telegram user id behind a message or callback query, if present.

    Only numeric ids count: test doubles and channel posts without a
    sender yield None (unknown), which fails open for owner-binding but
    fails closed when an admin allowlist is configured.
    """
    try:
        user = getattr(obj, "from_user", None)
        uid = getattr(user, "id", None)
        if uid is None or isinstance(uid, bool):
            return None
        return str(int(str(uid)))
    except Exception:
        return None


def allowlist_open_warning(cfg: Any) -> str | None:
    """Warning when Telegram destructive commands are open to the chat.

    An empty `admin_user_ids` (the default) lets anyone in the authorized
    chat tap keep/delete/inject — worth one loud line at startup, not a
    behavior change.
    """
    try:
        if cfg is None:
            return None
        allowed = getattr(cfg, "admin_user_ids", None) or []
        if isinstance(allowed, (list, tuple, set)) and len(list(allowed)) == 0:
            return (
                "telegram admin_user_ids is empty: anyone in the "
                "authorized chat may tap destructive buttons "
                "(cancel/inject). Set admin_user_ids to restrict."
            )
    except Exception:
        # Cannot prove the allowlist is set: warn rather than stay silent
        # (this function returns a message, never a boolean).
        return (
            "telegram admin allowlist could not be read: destructive "
            "buttons may be open to the whole chat; check admin_user_ids."
        )
    return None


# --------------------------------------------------------------------------- #
# Active-tasks + detail rendering (telegram_views.py; re-exported below)
# --------------------------------------------------------------------------- #
# Active-tasks + detail rendering (telegram_views.py; re-exported below)
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Bot
# --------------------------------------------------------------------------- #


async def _publish_bot_commands(bot: object) -> None:
    """Best-effort setMyCommands: never break startup over the menu."""
    try:
        setter = getattr(bot, "set_my_commands", None)
        if not callable(setter):
            return
        await setter(_bot_command_menu())
    except Exception as e:  # noqa: BLE001
        log.debug("telegram command menu publish failed: %s", e)


class _FloodBackoff(Exception):
    """Raise out of a card send when Telegram rate-limits it.

    Carries the server's wait so the WORKER LOOP sleeps once per batch
    instead of each card stalling the cards behind it (head-of-line).
    Never raised to callers outside the loop: they treat it like any
    send error (log + retry next pass).
    """

    def __init__(self, seconds: float):
        super().__init__(f"telegram flood backoff {seconds:.0f}s")
        try:
            self.seconds = max(1.0, min(float(seconds), 300.0))
        except Exception:
            self.seconds = 5.0


def _is_auth_failure(e: BaseException) -> bool:
    """True when Telegram rejected the token (401/Unauthorized).

    A bad token fails every call forever — starting the loops anyway
    just burns API calls and hides the config error behind retries.
    """
    try:
        from telegram.error import InvalidToken, Unauthorized

        if isinstance(e, (InvalidToken, Unauthorized)):
            return True
    except Exception:
        pass
    try:
        msg = str(e).lower()
        if "401" in msg or "unauthorized" in msg:
            return True
    except Exception:
        pass
    return False


def _make_bot(token: str) -> Bot:
    """Bot with generous HTTP timeouts for slow routes to api.telegram.org.

    PTB defaults (5s connect/read) turn a merely slow route into a storm
    of `Timed out` edit/send failures — every 15s active refresh plus each
    detail card. 30s reads comfortably cover 10s long-poll get_updates
    and large edits; a truly dead route still fails fast enough for the
    retry-next-interval paths. Falls back to a default Bot if the
    request class is unavailable (older PTB).
    """
    try:
        from telegram.request import HTTPXRequest

        _req = HTTPXRequest(
            connect_timeout=10.0,
            read_timeout=30.0,
            write_timeout=20.0,
            pool_timeout=10.0,
        )
        return Bot(token=token, request=_req)
    except Exception as e:  # noqa: BLE001
        log.debug("telegram custom HTTP timeouts unavailable (%s); using defaults", e)
        return Bot(token=token)


class TelegramBot(SheetsMixin, RouterMixin, RefreshMixin, ActionsMixin):
    def __init__(self, cfg: TelegramConfig, coord: Coordinator, store: StateStore):
        self._cfg = cfg
        self._coord = coord
        self._store = store
        self._bot: Bot | None = None
        self._task: asyncio.Task | None = None
        self._callback_task: asyncio.Task | None = None
        self._stopped: bool = False
        self._current_page: int = 0
        self._active_msg_id: int | None = None
        self._prev_active_msg_id: int | None = None
        self._pinned_message_id: int | None = None
        # Pending detail-message work, drained by a background worker.
        self._detail_queue: asyncio.Queue[tuple[str, float | None]] | None = None
        self._detail_worker: asyncio.Task | None = None
        # In-process cache: source_infohash -> message_id, so we don't
        # need to hit state.db for every send.
        self._detail_cache: dict[str, int] = {}
        # Last successfully sent detail state per infohash — the stale-card
        # net re-queues rows whose card drifted (e.g. an edit lost to a
        # flood ban) so no card freezes at a dead state forever.
        self._detail_sent_state: dict[str, str] = {}
        # Cached "last active-tasks (page, total_pages, text, buttons)" so
        # we skip identical edits.
        self._last_active_cache: tuple | None = None
        # Unchanged refreshes since the last liveness probe of the card.
        self._active_probe_count: int = 0
        # Per-chat debounce (monotonic timestamps by chat/user key): one
        # chat's burst must not starve pagination for everyone else.
        self._callback_times: dict[str, float] = {}
        # Destructive scopes currently executing (monotonic start time by
        # scope key): the 0.5s tap debounce can't cover a 60s client
        # delete, so a re-tap mid-execution would run the whole forget a
        # second time and report "gone". Second tap gets "working on it".
        self._action_inflight: dict[str, float] = {}
        # Last monotonic timestamp of an active-message (re)post, for the
        # keep-at-bottom repost interval.
        self._last_repost_monotonic: float = 0.0
        # Newest outbound message id this bot knows it sent. The Bot API
        # cannot report chat history, so burial is detected from our own
        # traffic (per-torrent cards are what floods this chat): a repost
        # is only due when something newer than the active message exists.
        self._newest_outbound_id: int | None = None
        # Serializes periodic active-message refresh vs callback-triggered
        # refresh so they can't interleave edits / race the dedup cache.
        self._active_lock = asyncio.Lock()
        # VPS1 free-space probe cache (monotonic timestamp, bytes|None):
        # the footer refreshes every status tick but the SFTP probe is
        # reused for _VPS1_FREE_TTL_S so we don't chatter the channel.
        self._vps1_free_cache: tuple[float, int | None] | None = None
        # Retired active-tasks message ids whose delete hit a transient
        # (flood / timeout). The pointer has already moved on, so without
        # this list they would linger in chat history forever.
        self._orphan_active_ids: list[int] = []

    # ---- lifecycle ----

    async def start(self) -> None:
        if not self._cfg.enabled:
            return
        self._stopped = False
        try:
            _warn = allowlist_open_warning(self._cfg)
            if _warn:
                log.warning("telegram: %s", _warn)
        except Exception:
            pass
        bot_token = (
            self._cfg.bot_token.get_secret_value()
            if hasattr(self._cfg.bot_token, "get_secret_value")
            else str(self._cfg.bot_token)
        )
        self._bot = _make_bot(bot_token)
        # Publish the / menu (best-effort): operators discover /add by
        # typing `/` instead of remembering the exact shape.
        try:
            await _publish_bot_commands(self._bot)
        except Exception:
            pass
        # Per-torrent message queue: bounded so a torrent flood doesn't
        # grow memory. 256 is well over what any operator needs.
        self._detail_queue = asyncio.Queue(maxsize=256)
        # Pre-fill cache from the store so we don't re-send every
        # torrent on restart. Sent states pre-fill too, so the stale-card
        # net stays quiet until a card actually drifts. Narrow query
        # (ids + states only): startup must not hydrate full rows for
        # thousands of torrents just to learn "already sent".
        try:
            _prefill_fn = getattr(self._store, "all_card_prefill", None)
            if callable(_prefill_fn):
                _prefill_rows = await asyncio.to_thread(_prefill_fn)
            else:
                _prefill_rows = await asyncio.to_thread(self._store.all)
        except Exception:
            _prefill_rows = []
        for _row in _prefill_rows or []:
            try:
                if isinstance(_row, tuple) and len(_row) == 3:
                    _h, _mid, _st = _row
                else:
                    _h = _row.source_infohash
                    _mid = _row.telegram_message_id
                    _st = _row.state.value
                if _mid:
                    self._detail_cache[_h] = _mid
                self._detail_sent_state[_h] = _st
            except Exception:
                continue
        # Restore active message ID across restarts to prevent duplicate messages
        raw_active_id = await asyncio.to_thread(self._store.get_meta, "telegram_active_msg_id")
        if raw_active_id:
            try:
                self._active_msg_id = int(raw_active_id)
                self._prev_active_msg_id = self._active_msg_id
            except ValueError:
                pass
        # Assume the restored message is still last until our own traffic
        # proves otherwise — avoids an instant delete+resend on restart.
        self._newest_outbound_id = self._active_msg_id
        self._detail_worker = asyncio.create_task(
            self._detail_worker_loop(),
            name="rs-telegram-detail",
        )
        try:
            # Debounced "online" ping: a crash-loop restart must not spam
            # the chat — skip when the last ping is under 10 minutes old.
            _ping_at = None
            try:
                _get_meta = getattr(self._store, "get_meta", None)
                if callable(_get_meta):
                    _ping_at = await asyncio.to_thread(_get_meta, "telegram_online_ping_at")
            except Exception:
                _ping_at = None
            _recent = False
            try:
                _recent = (time.time() - float(_ping_at or 0)) < 600
            except (TypeError, ValueError):
                _recent = False
            if not _recent:
                # Send initial "online" message (separate from active-tasks)
                sent_online = await self._bot.send_message(self._cfg.chat_id, "racing-sync online")
                self._note_outbound(getattr(sent_online, "message_id", None))
                try:
                    _set_meta = getattr(self._store, "set_meta", None)
                    if callable(_set_meta):
                        await asyncio.to_thread(
                            _set_meta, "telegram_online_ping_at", str(time.time())
                        )
                except Exception:
                    pass
        except TelegramError as e:
            if _is_auth_failure(e):
                # 401/Unauthorized: the token is dead — every later call
                # fails the same way. Fail fast (no loops) so the config
                # error surfaces instead of hiding behind retries.
                log.error(
                    "telegram token rejected (%s); fix bot_token "
                    "and restart — not starting telegram loops",
                    e,
                )
                raise RuntimeError(f"telegram bot_token rejected: {e}") from e
            log.warning("telegram probe failed: %s", e)
        self._task = asyncio.create_task(self._loop(), name="rs-telegram")
        self._callback_task = asyncio.create_task(
            self._callback_loop(),
            name="rs-telegram-callbacks",
        )

    async def stop(self) -> None:
        self._stopped = True
        if self._callback_task:
            self._callback_task.cancel()
            try:
                await self._callback_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._detail_worker:
            self._detail_worker.cancel()
            try:
                await self._detail_worker
            except (asyncio.CancelledError, Exception):
                pass
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        # Close the Bot HTTP session: without this every restart leaks an
        # "Unclosed client session" (connector/DNS/sockets). PTB Bot
        # versions differ (shutdown/request/session), so try each closer
        # defensively with a budget.
        try:
            _bot = getattr(self, "_bot", None)
            if _bot is not None:
                _closers: list = []
                for _attr in ("shutdown", "close"):
                    try:
                        _fn = getattr(_bot, _attr, None)
                    except Exception:
                        _fn = None
                    if callable(_fn):
                        _closers.append((_attr, _fn))
                try:
                    _req = getattr(_bot, "request", None)
                    for _attr in ("shutdown", "close"):
                        try:
                            _fn = getattr(_req, _attr, None)
                        except Exception:
                            _fn = None
                        if callable(_fn):
                            _closers.append((f"request.{_attr}", _fn))
                except Exception:
                    pass
                for _name, _fn in _closers:
                    try:
                        _r = _fn()
                        if asyncio.isfuture(_r) or asyncio.iscoroutine(_r):
                            await asyncio.wait_for(_r, timeout=5.0)
                    except (TimeoutError, asyncio.CancelledError):
                        raise
                    except Exception:
                        continue
        except (TimeoutError, asyncio.CancelledError):
            raise
        except Exception:
            pass

    # ---- main loop ----

    async def _loop(self) -> None:
        assert self._bot is not None
        while not self._stopped:
            try:
                await self._refresh_active_message()
                await self._reconcile_pinned()
            except (TimedOut, NetworkError) as e:
                log.warning(
                    "telegram loop transient network error (%s); will retry next interval", e
                )
            except Exception as e:  # noqa: BLE001
                log.warning("telegram loop error: %s", e)
            await asyncio.sleep(self._cfg.status_update_interval)

    # ---- per-torrent: detail message ----

    async def ensure_detail_message(self, ts: TorrentState, progress: float | None = None) -> None:
        """Queue a per-torrent detail-message update.

        The actual send happens in a background worker that respects
        Telegram's rate limits. Many `ensure_detail_message` calls for
        many torrents in quick succession will be coalesced — only the
        **latest** state for each torrent gets sent.
        """
        if not self._cfg.enabled or self._bot is None:
            return
        if self._detail_queue is None:
            return
        # If a previous message for this torrent is already queued and not
        # yet picked up, we don't need to enqueue again — the worker
        # will pull the row from the store and use the latest state.
        # To keep things simple, we always enqueue. The worker pulls the
        # freshest state at send time, so duplicates are harmless.
        self._enqueue_detail(ts.source_infohash, progress)

    def _enqueue_detail(self, infohash: str, progress: float | None) -> None:
        """Enqueue a detail update, coalescing by infohash when full.

        On overflow, an older queued entry for the SAME torrent is dropped
        first (its state is stale anyway — the worker re-reads the store),
        so one hot torrent can't evict other torrents' updates. Only when
        no same-hash entry exists is the oldest entry evicted, with a log.
        """
        q = self._detail_queue
        if q is None:
            return
        try:
            q.put_nowait((infohash, progress))
            return
        except asyncio.QueueFull:
            pass
        try:
            pending: list[tuple[str, float | None]] = []
            coalesced = False
            while True:
                try:
                    item = q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item[0] == infohash and not coalesced:
                    coalesced = True
                    continue
                pending.append(item)
            for item in pending:
                try:
                    q.put_nowait(item)
                except asyncio.QueueFull:
                    break
            q.put_nowait((infohash, progress))
        except asyncio.QueueFull:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait((infohash, progress))
            except asyncio.QueueFull:
                log.warning(
                    "telegram detail queue full; dropping update for %s",
                    infohash[:10],
                )

    async def _detail_worker_loop(self) -> None:
        """Drain the per-torrent detail-message queue.

        We process at most one message per `outbound_rate` second to
        stay under Telegram's bot API rate limit. When the queue
        contains multiple updates for the same infohash, only the
        latest is sent (intermediate states are skipped — they would
        arrive in the same chat scroll anyway). Flood backoff sleeps
        HERE, once per batch: a banned card no longer stalls the cards
        behind it, and the unprocessed remainder requeues.
        """
        assert self._detail_queue is not None
        interval = 1.0 / max(0.1, float(getattr(self._cfg, "outbound_rate", 1.0)))
        while True:
            try:
                # Batch-wait: collect whatever is in the queue up to
                # `interval` seconds, but only send the latest state
                # for each torrent.
                batch: dict[str, float | None] = {}
                try:
                    first_hash, first_prog = await asyncio.wait_for(
                        self._detail_queue.get(),
                        timeout=interval,
                    )
                    batch[first_hash] = first_prog
                except TimeoutError:
                    pass
                # Drain anything else queued.
                while not self._detail_queue.empty():
                    h, p = self._detail_queue.get_nowait()
                    batch[h] = p
                if not batch:
                    continue
                pending = list(batch.items())
                for _i, (infohash, progress) in enumerate(pending):
                    try:
                        await self._send_one_detail(infohash, progress)
                    except _FloodBackoff as _fb:
                        log.warning(
                            "telegram detail batch flood; backing off "
                            "for %ds (%d card(s) requeued)",
                            int(_fb.seconds),
                            len(pending) - _i,
                        )
                        await asyncio.sleep(_fb.seconds)
                        for _rh, _rp in pending[_i:]:
                            self._enqueue_detail(_rh, _rp)
                        break
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                log.warning("telegram detail worker error: %s", e)
                await asyncio.sleep(1.0)

    def _sent_state_map(self) -> dict[str, str]:
        """Last successfully sent detail state per infohash (creates on demand).

        Unit-test doubles build the bot via object.__new__ (no __init__),
        so every access goes through here instead of assuming attributes.
        """
        try:
            m = getattr(self, "_detail_sent_state", None)
            if not isinstance(m, dict):
                m = {}
                self._detail_sent_state = m
            return m
        except Exception:
            return {}

    def _mark_detail_sent(self, infohash: str, state_value: str) -> None:
        try:
            self._sent_state_map()[infohash] = state_value
        except Exception:
            pass
        # A delivered card clears both its failure streak and any
        # terminal-retry record — future drift re-arms from zero.
        try:
            _streak = getattr(self, "_detail_fail_streak", None)
            if isinstance(_streak, dict):
                _streak.pop(infohash, None)
        except Exception:
            pass
        try:
            _tmap = getattr(self, "_detail_terminal_retry", None)
            if isinstance(_tmap, dict):
                _tmap.pop(infohash, None)
        except Exception:
            pass

    def _detail_terminal_map(self) -> dict[str, int]:
        """DONE/FAILED hashes whose final card update died transiently.

        Settled rows leave the active list, so the per-refresh stale-card
        net never sees them — without this map a single timeout on the
        DONE edit freezes the card at MOVING/RE_ADDING forever. Values
        are refresh-driven retry counts (bounded by the caller).
        """
        try:
            m = getattr(self, "_detail_terminal_retry", None)
            if not isinstance(m, dict):
                m = {}
                self._detail_terminal_retry = m
            if len(m) > 50:
                for k in list(m.keys())[: len(m) - 50]:
                    m.pop(k, None)
            return m
        except Exception:
            return {}

    def _note_detail_transient_failure(
        self,
        infohash: str,
        progress: float | None,
        state_value: str,
    ) -> None:
        """Requeue a transiently-failed card update (bounded fast retries).

        Immediate requeues are capped per hash (streak of 5): on a dead
        route the worker would otherwise hot-loop one card. Inflight rows
        keep healing via the refresh stale-net; terminal rows via the
        terminal map recorded here.
        """
        try:
            streak = getattr(self, "_detail_fail_streak", None)
            if not isinstance(streak, dict):
                streak = {}
                self._detail_fail_streak = streak
            n = int(streak.get(infohash, 0) or 0) + 1
            streak[infohash] = n
            if len(streak) > 500:
                for k in list(streak.keys())[: len(streak) - 500]:
                    streak.pop(k, None)
        except Exception:
            n = 6
        try:
            if state_value in ("done", "failed"):
                self._detail_terminal_map().setdefault(infohash, 0)
        except Exception:
            pass
        if n <= 5:
            self._enqueue_detail(infohash, progress)

    async def _send_one_detail(self, infohash: str, progress: float | None) -> None:
        """Send or edit the detail message for a single torrent."""
        if self._bot is None:
            return
        ts = await asyncio.to_thread(self._store.get, infohash)
        if ts is None:
            return
        note = ""
        try:
            if ts.state in (State.NEW, State.WAITING_DISK):
                _wn = getattr(getattr(self, "_coord", None), "_watch_wait_note", None)
                if callable(_wn):
                    note = _wn(ts) or ""
        except Exception:
            note = ""
        text = render_detail(ts, progress, note)
        # Grace-held rows are actionable: offer the override inline so
        # the operator doesn't have to remember the command shape
        # (now starts this copy at once with its starting bytes).
        try:
            if note.startswith("Waiting for preferred copy"):
                text += f"\nStart this copy now: `{_now_command(infohash)}`"
        except Exception:
            pass
        try:
            state_value = ts.state.value
        except Exception:
            state_value = ""
        msg_id = self._detail_cache.get(infohash)
        if msg_id is None:
            msg_id = await asyncio.to_thread(self._store.get_telegram_message_id, infohash)

        def _rate_limit_wait(err: BaseException) -> float | None:
            """Server's wait for a rate limit; None when not one.

            Pure calculator — the caller raises _FloodBackoff so the
            WORKER LOOP sleeps once per batch (never per card here).
            """
            _wait = _flood_wait_seconds(err)
            if _wait is None:
                return None
            log.warning(
                "telegram detail rate-limited for %s; backing off %ds",
                infohash[:10],
                int(_wait),
            )
            return _wait

        try:
            if msg_id is None:
                try:
                    sent = await self._bot.send_message(
                        self._cfg.chat_id,
                        text,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except TelegramError as e:
                    if _is_parse_error(e):
                        sent = await self._bot.send_message(
                            self._cfg.chat_id,
                            text,
                        )
                    else:
                        raise
                self._note_outbound(getattr(sent, "message_id", None))
                self._detail_cache[infohash] = sent.message_id
                await asyncio.to_thread(
                    self._store.set_telegram_message_id,
                    infohash,
                    sent.message_id,
                )
                self._mark_detail_sent(infohash, state_value)
            else:
                try:
                    await self._bot.edit_message_text(
                        text,
                        chat_id=self._cfg.chat_id,
                        message_id=msg_id,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                    self._mark_detail_sent(infohash, state_value)
                except TelegramError as e:
                    msg = str(e).lower()
                    if "not modified" in msg:
                        self._mark_detail_sent(infohash, state_value)
                        return
                    if "not found" in msg or "invalid" in msg:
                        # Message was deleted; resend.
                        try:
                            sent = await self._bot.send_message(
                                self._cfg.chat_id,
                                text,
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError as e2:
                            if _is_parse_error(e2):
                                sent = await self._bot.send_message(
                                    self._cfg.chat_id,
                                    text,
                                )
                            else:
                                raise
                        self._note_outbound(getattr(sent, "message_id", None))
                        self._detail_cache[infohash] = sent.message_id
                        await asyncio.to_thread(
                            self._store.set_telegram_message_id,
                            infohash,
                            sent.message_id,
                        )
                        self._mark_detail_sent(infohash, state_value)
                    elif _is_parse_error(e):
                        await self._bot.edit_message_text(
                            text,
                            chat_id=self._cfg.chat_id,
                            message_id=msg_id,
                        )
                        self._mark_detail_sent(infohash, state_value)
                    elif (_w := _rate_limit_wait(e)) is not None:
                        raise _FloodBackoff(_w) from e
                    elif _is_transient_tg_error(e):
                        # Timeout on EDIT: the row may settle (DONE/FAILED)
                        # right after, leaving the active list where the
                        # stale net could retry it — record + requeue
                        # bounded instead of dropping the update.
                        log.warning(
                            "telegram detail edit timed out for %s (%s); requeueing",
                            infohash[:10],
                            e,
                        )
                        self._note_detail_transient_failure(infohash, progress, state_value)
                    else:
                        log.warning(
                            "telegram detail edit failed for %s: %s",
                            infohash[:10],
                            e,
                        )
        except RetryAfter as e:
            if isinstance(e.retry_after, dt.timedelta):
                wait_s = _with_flood_margin(e.retry_after.total_seconds())
            else:
                wait_s = _with_flood_margin(e.retry_after)
            log.warning("telegram flood control hit; backing off for %ds", wait_s)
            raise _FloodBackoff(wait_s) from e
        except (TimedOut, NetworkError) as e:
            log.warning("telegram detail send timed out (%s); will retry next interval", e)
            self._note_detail_transient_failure(infohash, progress, state_value)
        except TelegramError as e:
            if (_w2 := _flood_wait_seconds(e)) is not None:
                log.warning(
                    "telegram detail rate-limited for %s; backing off %ds",
                    infohash[:10],
                    int(_w2),
                )
                raise _FloodBackoff(_w2) from e
            log.warning("telegram detail send failed for %s: %s", infohash[:10], e)

    # ---- active tasks pagination & callback handling ----

    def _build_keyboard(
        self,
        current_page: int,
        total_pages: int,
        extra_rows: list[list[tuple[str, str]]] | tuple = (),
        status_filter: str = "all",
    ) -> InlineKeyboardMarkup | None:
        """Pagination nav buttons plus extra action rows.

        Extra rows are ``(label, callback_data)`` pairs (hash-direct
        action buttons) or a ButtonMaker, already chunked by the caller —
        invalid entries are logged and skipped so one bad button can
        never break the periodic list render. The trailing filter row
        (All/DL/MV/RD/QD + overview) carries `status:` callbacks; the
        active filter renders bracketed.
        """
        if total_pages <= 1:
            buttons = [[InlineKeyboardButton("🔄 Refresh", callback_data="page:refresh")]]
        else:
            buttons = [
                [
                    InlineKeyboardButton("◀️ Prev", callback_data="page:prev"),
                    InlineKeyboardButton(
                        f"{current_page + 1} / {total_pages}", callback_data="page:refresh"
                    ),
                    InlineKeyboardButton("Next ▶️", callback_data="page:next"),
                ],
                [
                    InlineKeyboardButton("🔄 Refresh", callback_data="page:refresh"),
                ],
            ]
        try:
            for _row in validate_rows(extra_rows):
                _btns = []
                for _label, _data in _row:
                    _btns.append(InlineKeyboardButton(_label, callback_data=_data))
                if _btns:
                    buttons.append(_btns)
        except Exception:
            pass
        try:
            from .telegram_protocol import _STATUS_FILTER_BUTTONS

            _cur = status_filter or "all"
            _frow = []
            for _key, _label in _STATUS_FILTER_BUTTONS:
                try:
                    _txt = f"·{_label}·" if _key == _cur else _label
                    _frow.append(InlineKeyboardButton(_txt, callback_data=f"status:{_key}"))
                except Exception:
                    continue
            if _frow:
                buttons.append(_frow)
        except Exception:
            pass
        return InlineKeyboardMarkup(buttons)

    # ---- stateless hash protocol (no pending snapshots) ----

    def _live_group_hashes(self, leader_hash: str) -> list[str]:
        """Live tracked member hashes sharing the leader's content.

        Re-derives the group on every tap from the current store, so
        renumbering, regrouping, or forgotten rows can never misroute
        an `all:`-scoped button — an empty result means "re-tap".
        """
        try:
            norm = (leader_hash or "").strip().lower()
            if not _is_hex40(norm):
                return []
            store = getattr(self, "_store", None)
            if store is None:
                return []
            _get = getattr(store, "get", None)
            _all = getattr(store, "list_active_inflight", None)
            leader = _get(norm, include_blob=False) if callable(_get) else None
            rows = _all() if callable(_all) else []
            if leader is None:
                # Leader gone (forgotten/cancelled): siblings may still
                # be live — match by the leader's last known content via
                # members that share it. Without the row we have no
                # name/size, so resolve fails cleanly instead of guessing.
                return []
            try:
                from .content_keys import same_content as _same
            except Exception:
                _same = None  # type: ignore[assignment]
            out: list[str] = []
            for _r in rows or []:
                try:
                    _rh = (_r.source_infohash or "").lower()
                    if not _rh:
                        continue
                    if _same is not None:
                        if not _same(
                            _r.source_name or "",
                            _r.total_bytes or 0,
                            leader.source_name or "",
                            leader.total_bytes or 0,
                        ):
                            continue
                    elif (_r.source_name or "").strip().lower() != (
                        leader.source_name or ""
                    ).strip().lower():
                        continue
                    out.append(_rh)
                except Exception:
                    continue
            return out
        except Exception:
            return []

    def _now_eligible_hashes(
        self,
        members,
        notes: dict | None = None,
    ) -> list[str]:
        """Member hashes that may start at once (old fetch ∪ prefer sets)."""
        return _now_eligible_hashes(members, notes)

    async def _send_sheet(
        self,
        text: str,
        rows: list,
        reply_to: Any = None,
    ) -> None:
        """Best-effort sheet reply with action buttons (plain text).

        `rows` is a ButtonMaker (strict, validated at build time) or
        legacy ``(label, data)`` rows (validated leniently: invalid
        buttons are logged and dropped, never silently).
        """
        bot = getattr(self, "_bot", None)
        if bot is None:
            return
        try:
            _btns = []
            for _row in validate_rows(rows):
                _line = []
                for _label, _data in _row:
                    _line.append(InlineKeyboardButton(_label, callback_data=_data))
                if _line:
                    _btns.append(_line)
            kwargs: dict[str, Any] = {}
            try:
                msg_id = getattr(reply_to, "message_id", None)
                if isinstance(msg_id, int) and msg_id > 0:
                    kwargs["reply_to_message_id"] = msg_id
            except Exception:
                pass
            if _btns:
                kwargs["reply_markup"] = InlineKeyboardMarkup(_btns)
            sent = await bot.send_message(self._cfg.chat_id, text, **kwargs)
            self._note_outbound(getattr(sent, "message_id", None))
        except Exception as e:  # noqa: BLE001
            log.debug("sheet reply failed: %s", e)

    async def _delete_query_message(self, query: Any) -> None:
        """Best-effort delete of the message holding the tapped button."""
        try:
            bot = getattr(self, "_bot", None)
            msg = getattr(query, "message", None)
            if bot is None or msg is None:
                return
            try:
                mid = getattr(msg, "message_id", None)
            except Exception:
                mid = None
            if not isinstance(mid, int) or mid <= 0:
                return
            try:
                await bot.delete_message(self._cfg.chat_id, mid)
            except Exception as e:  # noqa: BLE001
                log.debug("sheet delete failed: %s", e)
        except Exception:
            pass

    async def _mark_sheet_working(self, query: Any) -> None:
        """Best-effort: append a working note to the tapped sheet's text.

        Only used for slow taps (delete-files keeps, confirmed
        injects): their execution outlives the tap by 10s+, and the
        sheet stays live meanwhile. Idempotent (skips when already
        marked); failure never blocks the execution itself.
        """
        try:
            bot = getattr(self, "_bot", None)
            msg = getattr(query, "message", None)
            if bot is None or msg is None:
                return
            old = getattr(msg, "text", None)
            mid = getattr(msg, "message_id", None)
            if not isinstance(old, str) or not isinstance(mid, int):
                return
            if mid <= 0 or old.rstrip().endswith("Working on it…"):
                return
            try:
                await bot.edit_message_text(
                    f"{old[:4000]}\n\n⏳ Working on it…",
                    chat_id=self._cfg.chat_id,
                    message_id=mid,
                )
            except Exception as e:  # noqa: BLE001
                log.debug("sheet working mark failed: %s", e)
        except Exception:
            pass

    async def _resume_hashes_reply(self, hashes: list[str]) -> str:
        """Resume outcome text for hashes (no send/refresh).

        Shared by the /resume_ entry point and hash-button taps:
        unholds tracked rows, lifts ignored hashes, composes one reply.
        """
        try:
            store = getattr(self, "_store", None)
            if store is None:
                return "Action failed: bot not attached."
            wanted = []
            try:
                for h in hashes or []:
                    _h = (h or "").strip().lower()
                    if len(_h) == 40 and all(c in "0123456789abcdef" for c in _h):
                        wanted.append(_h)
            except Exception:
                pass
            if not wanted:
                return "Resume failed: nothing tracked to act on."
            tracked: list[str] = []
            try:
                _get = getattr(store, "get", None)
                if callable(_get):
                    for _h in wanted:
                        try:
                            if _get(_h, include_blob=False) is not None:
                                tracked.append(_h)
                        except Exception:
                            continue
            except Exception:
                pass
            unskip_text = ""
            if tracked:
                try:
                    unskip_text = await self._skip_torrents(tracked, hold=False)
                except Exception as e:  # noqa: BLE001
                    unskip_text = f"Resume failed: {e}"
            lifted: list[str] = []
            try:
                for _h in wanted:
                    try:
                        _name, _err = await asyncio.to_thread(self._unignore_core, _h)
                    except Exception:
                        continue
                    if _name:
                        lifted.append(_name)
            except Exception:
                pass
            if not tracked and not lifted:
                return "Already gone from tracking" if not unskip_text else unskip_text
            reply = (unskip_text or "").strip()
            if lifted:
                shown = ", ".join(lifted[:3])
                if len(lifted) > 3:
                    shown += f" (+{len(lifted) - 3} more)"
                extra = (
                    f"Unignored {shown} — re-send .torrent via /add (or re-drop) to track again."
                )
                reply = f"{reply} {extra}".strip() if reply else extra
            return reply or "Resume failed: nothing changed."
        except Exception as e:  # noqa: BLE001
            return f"Resume failed: {e}"
