"""Telegram bot.

Two surfaces, both using the chat's edit-in-place mechanism so chat history
stays clean:

  1. **Per-torrent detail message** — one Telegram message per source_infohash.
     Created when the torrent first leaves NEW. Edited in place as it advances
     through states (NEW → QUERYING → WAITING_SEEDPOOL → ... → DONE). The
     final DONE message stays in the chat as a clean history record.
     The message_id is persisted in torrent_state.telegram_message_id.

  2. **Active-tasks message** — one message at the bottom of the chat that
     lists every torrent currently in flight (anything != DONE / FAILED).
     Edited every status_update_interval. Pinned if pin_status_message=true.

App logging goes to local files only (no Telegram forwarding).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut

from .config import TelegramConfig
from .coordinator import Coordinator
from .state import State, StateStore, TorrentState

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Format helpers
# --------------------------------------------------------------------------- #


_STATE_ICON = {
    State.NEW: "🆕 `NEW`",
    State.QUERYING: "🔍 `QUERY`",
    State.WAITING_SEEDPOOL: "⏳ `WAIT-SP`",
    State.WAITING_DISK: "💾 `WAIT-SSD`",
    State.QUEUED: "📋 `QUEUED`",
    State.DOWNLOADING: "⬇️ `DOWNLOADING`",
    State.MOVING: "📦 `MOVING`",
    State.RE_ADDING: "🔄 `RE-ADDING`",
    State.DONE: "✅ `DONE`",
    State.FAILED: "❌ `FAILED`",
}


def _esc(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("`", "\\`")
        .replace("[", "\\[")
        .replace("]", "\\]")
    )


def _short_name(name: str, limit: int = 56) -> str:
    """Trim long release names with an ellipsis in the middle."""
    if len(name) <= limit:
        return name
    head = (limit - 1) // 2
    tail = limit - 1 - head
    return name[:head] + "…" + name[-tail:]


def _bytes_human(n: int) -> str:
    if n < 1024:
        return f"{n}B"
    units = ("KB", "MB", "GB", "TB", "PB")
    f = float(n)
    idx = -1
    while f >= 1024 and idx < len(units) - 1:
        f /= 1024
        idx += 1
    return f"{f:.1f}{units[idx]}"


# --------------------------------------------------------------------------- #
# Message renderers
# --------------------------------------------------------------------------- #


def render_detail(ts: TorrentState, progress: float | None = None) -> str:
    """Per-torrent detail message (edited in place as state advances)."""
    icon = _STATE_ICON.get(ts.state, ts.state.value)
    title = _short_name(ts.source_name, 80)
    size = _bytes_human(ts.total_bytes)

    lines: list[str] = []
    lines.append(f"{icon} *{_esc(title)}*")
    lines.append(
        f"`{ts.source_infohash[:5]}` · {size} · "
        f"batch `{ts.batch_index}/{ts.batches_total}`"
    )

    # State-specific extras
    if ts.state == State.WAITING_SEEDPOOL and ts.seedpool_next_retry_at:
        when = ts.seedpool_next_retry_at.astimezone().strftime("%H:%M:%S")
        lines.append(
            f"Seedpool miss \\#{ts.seedpool_attempts}; "
            f"next retry at `{when}`"
        )
    elif ts.state == State.WAITING_DISK:
        lines.append("Waiting for SSD cap to free up")
    elif ts.state == State.QUEUED:
        lines.append("Queued for SSD download")
    elif ts.state == State.DOWNLOADING:
        if progress is not None:
            bar_len = 16
            filled = int(round(progress * bar_len))
            bar = "█" * filled + "░" * (bar_len - filled)
            pct = f"{progress * 100:5.1f}%"
            lines.append(f"`{bar}` {pct}")
        else:
            lines.append("Downloading\\…")
    elif ts.state == State.MOVING:
        lines.append("rclone moving to remote\\…")
    elif ts.state == State.RE_ADDING:
        lines.append("Re\\-adding on fuse mount")
    elif ts.state == State.DONE:
        lines.append("✓ Seeded from fuse mount")
    elif ts.state == State.FAILED:
        err = _esc(ts.last_error[:200]) if ts.last_error else "no detail"
        lines.append(f"✗ Failed: {err}")

    # Cross-seed info
    if ts.cross_seed_source:
        lines.append(
            f"SSD source: `{ts.cross_seed_source}`"
            + (f" · `{ts.cross_seed_infohash[:5]}`" if ts.cross_seed_infohash else "")
        )

    # Classifier + tracker context
    if ts.classification_kind and ts.classification_kind != "unknown":
        lines.append(f"Classifier: `{ts.classification_kind}`")
    if ts.source_tracker:
        lines.append(f"Source: `{_esc(_short_name(ts.source_tracker, 60))}`")

    return "\n".join(lines)


def render_active(
    active: list[tuple[TorrentState, float | None]],
    page: int = 0,
    page_size: int = 5,
) -> tuple[str, int, int]:
    """Render paginated list of active tasks with numbered items."""
    total_items = len(active)
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    cur_page = max(0, min(page, total_pages - 1))

    now = dt.datetime.now().strftime("%H:%M:%S")
    if not active:
        return (
            f"*Active Tasks*\n🕒 `{now}`\n\n_No active tasks in flight\\._",
            0,
            1,
        )

    start_idx = cur_page * page_size
    end_idx = min(start_idx + page_size, total_items)
    page_items = active[start_idx:end_idx]

    lines = [
        f"*Active Tasks* · *Page {cur_page + 1}/{total_pages}* ({total_items} in flight)",
        f"🕒 `{now}`",
        "",
    ]

    for i, (ts, progress) in enumerate(page_items):
        item_num = start_idx + i + 1
        name = _short_name(ts.source_name, 44)
        size = _bytes_human(ts.total_bytes)
        state_icon = _STATE_ICON.get(ts.state, f"`{ts.state.value}`")
        short_hash = ts.source_infohash[-5:]

        # Numbered item header
        lines.append(f"*{item_num}.* {state_icon} *{_esc(name)}*")

        # Subline details
        details: list[str] = [f"`{size}`"]

        if ts.state == State.DOWNLOADING:
            if progress is not None:
                pct = f"{progress * 100:.1f}%"
                details.append(f"`{pct}`")
            else:
                details.append("`Downloading`")
        elif ts.state == State.QUEUED:
            details.append("`Queued`")
        elif ts.state == State.MOVING:
            details.append("`Moving`")
        elif ts.state == State.RE_ADDING:
            details.append("`Re-adding`")
        elif ts.state == State.QUERYING:
            details.append("`Querying`")
        elif ts.state == State.WAITING_SEEDPOOL:
            details.append(f"`Wait-SP #{ts.seedpool_attempts}`")
        elif ts.state == State.WAITING_DISK:
            details.append("`Wait-SSD`")

        if ts.batches_total > 1:
            details.append(f"Batch `{ts.batch_index}/{ts.batches_total}`")

        details.append(f"`#{short_hash}`")

        lines.append(f"    ↳ {' · '.join(details)}")
        lines.append("")

    return "\n".join(lines).strip(), cur_page, total_pages


# --------------------------------------------------------------------------- #
# Bot
# --------------------------------------------------------------------------- #


@dataclass
class _ActiveProgress:
    """Per-torrent progress dict keyed by source_infohash (lowercase)."""

    data: dict[str, float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.data is None:
            self.data = {}


class TelegramBot:
    def __init__(self, cfg: TelegramConfig, coord: Coordinator,
                 store: StateStore):
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
        # Outbound rate limiter: Telegram's bot API allows ~30
        # messages/sec across all chats per bot. We self-throttle to
        # `outbound_rate` per second so a flood of state transitions
        # doesn't trigger HTTP 429 / Retry-After.
        self._rate_sem: asyncio.Semaphore | None = None
        # Pending detail-message work, drained by a background worker.
        self._detail_queue: asyncio.Queue[asyncio.Task] | None = None
        self._detail_worker: asyncio.Task | None = None
        # In-process cache: source_infohash -> message_id, so we don't
        # need to hit state.db for every send.
        self._detail_cache: dict[str, int] = {}
        # Cached "last active-tasks (page, total_pages, text)" so we skip identical edits.
        self._last_active_text: str = ""
        self._last_active_cache: tuple[int, int, str] | None = None

    # ---- lifecycle ----

    async def start(self) -> None:
        if not self._cfg.enabled:
            return
        self._stopped = False
        self._bot = Bot(token=self._cfg.bot_token)
        # Per-torrent message queue: bounded so a torrent flood doesn't
        # grow memory. 256 is well over what any operator needs.
        self._detail_queue = asyncio.Queue(maxsize=256)
        # Pre-fill cache from the store so we don't re-send every
        # torrent on restart.
        for ts in self._store.all():
            if ts.telegram_message_id:
                self._detail_cache[ts.source_infohash] = ts.telegram_message_id
        self._detail_worker = asyncio.create_task(
            self._detail_worker_loop(), name="rs-telegram-detail",
        )
        try:
            # Send initial "online" message (separate from active-tasks)
            await self._bot.send_message(self._cfg.chat_id, "racing-sync online")
        except TelegramError as e:
            log.warning("telegram probe failed: %s", e)
        self._task = asyncio.create_task(self._loop(), name="rs-telegram")
        self._callback_task = asyncio.create_task(
            self._callback_loop(), name="rs-telegram-callbacks",
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

    # ---- main loop ----

    async def _loop(self) -> None:
        assert self._bot is not None
        while not self._stopped:
            try:
                await self._refresh_active_message()
                await self._reconcile_pinned()
            except (TimedOut, NetworkError) as e:
                log.warning("telegram loop transient network error (%s); will retry next interval", e)
            except Exception as e:  # noqa: BLE001
                log.warning("telegram loop error: %s", e)
            await asyncio.sleep(self._cfg.status_update_interval)

    # ---- per-torrent: detail message ----

    async def ensure_detail_message(self, ts: TorrentState,
                                     progress: float | None = None) -> None:
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
        try:
            self._detail_queue.put_nowait((ts.source_infohash, progress))
        except asyncio.QueueFull:
            # Queue is small; if it's full, drop and let the next
            # iteration refill it.
            pass

    async def _detail_worker_loop(self) -> None:
        """Drain the per-torrent detail-message queue.

        We process at most one message per `outbound_rate` second to
        stay under Telegram's bot API rate limit. When the queue
        contains multiple updates for the same infohash, only the
        latest is sent (intermediate states are skipped — they would
        arrive in the same chat scroll anyway).
        """
        assert self._detail_queue is not None
        interval = max(1.0, 1.0 / max(1, getattr(self._cfg, "outbound_rate", 1)))
        while True:
            try:
                # Batch-wait: collect whatever is in the queue up to
                # `interval` seconds, but only send the latest state
                # for each torrent.
                batch: dict[str, float | None] = {}
                try:
                    first_hash, first_prog = await asyncio.wait_for(
                        self._detail_queue.get(), timeout=interval,
                    )
                    batch[first_hash] = first_prog
                except asyncio.TimeoutError:
                    pass
                # Drain anything else queued.
                while not self._detail_queue.empty():
                    h, p = self._detail_queue.get_nowait()
                    batch[h] = p
                if not batch:
                    continue
                for infohash, progress in batch.items():
                    await self._send_one_detail(infohash, progress)
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            except Exception as e:  # noqa: BLE001
                log.warning("telegram detail worker error: %s", e)
                await asyncio.sleep(1.0)

    async def _send_one_detail(self, infohash: str,
                                progress: float | None) -> None:
        """Send or edit the detail message for a single torrent."""
        if self._bot is None:
            return
        ts = self._store.get(infohash)
        if ts is None:
            return
        text = render_detail(ts, progress)
        msg_id = self._detail_cache.get(infohash) or \
            self._store.get_telegram_message_id(infohash)
        try:
            if msg_id is None:
                sent = await self._bot.send_message(
                    self._cfg.chat_id, text,
                    parse_mode=ParseMode.MARKDOWN,
                )
                self._detail_cache[infohash] = sent.message_id
                self._store.set_telegram_message_id(
                    infohash, sent.message_id,
                )
            else:
                try:
                    await self._bot.edit_message_text(
                        text,
                        chat_id=self._cfg.chat_id,
                        message_id=msg_id,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except TelegramError as e:
                    msg = str(e).lower()
                    if "not modified" in msg:
                        return
                    if "not found" in msg or "invalid" in msg:
                        # Message was deleted; resend.
                        sent = await self._bot.send_message(
                            self._cfg.chat_id, text,
                            parse_mode=ParseMode.MARKDOWN,
                        )
                        self._detail_cache[infohash] = sent.message_id
                        self._store.set_telegram_message_id(
                            infohash, sent.message_id,
                        )
                    else:
                        log.warning(
                            "telegram detail edit failed for %s: %s",
                            infohash[:10], e,
                        )
        except RetryAfter as e:
            wait_s = int(e.retry_after) + 1
            log.warning("telegram flood control hit; backing off for %ds", wait_s)
            await asyncio.sleep(wait_s)
            if self._detail_queue is not None:
                try:
                    self._detail_queue.put_nowait((infohash, progress))
                except asyncio.QueueFull:
                    pass
        except (TimedOut, NetworkError) as e:
            log.warning("telegram detail send timed out (%s); will retry next interval", e)
            if self._detail_queue is not None:
                try:
                    self._detail_queue.put_nowait((infohash, progress))
                except asyncio.QueueFull:
                    pass
        except TelegramError as e:
            log.warning("telegram detail send failed for %s: %s",
                        infohash[:10], e)

    # ---- active tasks pagination & callback handling ----

    def _build_keyboard(self, current_page: int, total_pages: int) -> InlineKeyboardMarkup | None:
        if total_pages <= 1:
            buttons = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="page:refresh")]
            ]
            return InlineKeyboardMarkup(buttons)

        buttons = [
            [
                InlineKeyboardButton("◀️ Prev", callback_data="page:prev"),
                InlineKeyboardButton(f"{current_page + 1} / {total_pages}", callback_data="page:refresh"),
                InlineKeyboardButton("Next ▶️", callback_data="page:next"),
            ],
            [
                InlineKeyboardButton("🔄 Refresh", callback_data="page:refresh"),
            ],
        ]
        return InlineKeyboardMarkup(buttons)

    async def _callback_loop(self) -> None:
        """Poll get_updates to handle pagination inline keyboard clicks."""
        assert self._bot is not None
        offset = 0
        while not self._stopped:
            try:
                updates = await self._bot.get_updates(
                    offset=offset,
                    timeout=10,
                    allowed_updates=["callback_query"],
                )
                for u in updates:
                    offset = max(offset, u.update_id + 1)
                    if u.callback_query:
                        await self._handle_callback(u.callback_query)
            except asyncio.CancelledError:
                return
            except (TimedOut, NetworkError):
                await asyncio.sleep(0.5)
            except Exception as e:
                msg = str(e).lower()
                if "conflict" in msg:
                    log.warning("telegram callback polling conflict (another bot session running?): %s", e)
                    await asyncio.sleep(15)
                else:
                    log.debug("telegram callback polling error: %s", e)
                    await asyncio.sleep(2)

    async def _handle_callback(self, query: Any) -> None:
        try:
            await query.answer()
        except Exception:
            pass

        data = str(getattr(query, "data", "") or "")
        if not data.startswith("page:"):
            return

        action = data.split(":", 1)[1]
        rows = self._store.list_active_inflight()
        page_size = self._cfg.page_size
        total_pages = max(1, (len(rows) + page_size - 1) // page_size)

        if action == "prev":
            self._current_page = (self._current_page - 1) % total_pages
        elif action == "next":
            self._current_page = (self._current_page + 1) % total_pages
        elif action == "refresh":
            pass

        self._last_active_cache = None
        await self._refresh_active_message()

    # ---- active tasks list ----

    async def _refresh_active_message(self) -> None:
        assert self._bot is not None
        # Sentinel -1 means "stop trying to edit" (e.g. chat permission issue)
        if self._active_msg_id == -1:
            return
        rows = self._store.list_active_inflight()
        # Pull live progress from coordinator's tracker
        progress_map = self._coord.live_progress_map()
        items: list[tuple[TorrentState, float | None]] = [
            (ts, progress_map.get(ts.source_infohash.lower()))
            for ts in rows
        ]
        text, cur_page, total_pages = render_active(
            items, page=self._current_page, page_size=self._cfg.page_size
        )
        self._current_page = cur_page
        keyboard = self._build_keyboard(cur_page, total_pages)

        # Skip the API call if page, total_pages, and text are identical
        cache_key = (cur_page, total_pages, text)
        if cache_key == self._last_active_cache and self._active_msg_id is not None:
            return
        self._last_active_text = text

        if self._active_msg_id is None:
            # Clean up previously known message if any to prevent duplicate message spam
            if self._prev_active_msg_id is not None and self._prev_active_msg_id > 0:
                try:
                    await self._bot.delete_message(
                        chat_id=self._cfg.chat_id,
                        message_id=self._prev_active_msg_id,
                    )
                except Exception:
                    pass
                self._prev_active_msg_id = None

            try:
                sent = await self._bot.send_message(
                    self._cfg.chat_id, text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                )
                self._active_msg_id = sent.message_id
                self._prev_active_msg_id = sent.message_id
                self._last_active_cache = cache_key
            except (TimedOut, NetworkError) as e:
                log.warning("active-tasks send timed out (%s); will retry next interval", e)
            except TelegramError as e:
                log.warning("active-tasks send failed: %s", e)
        else:
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
                    return

                # "Message to edit not found" / "MESSAGE_ID_INVALID" — deleted from chat
                if "message to edit not found" in msg or "message_id_invalid" in msg:
                    log.warning("active-tasks message not found in chat; will resend")
                    self._active_msg_id = None
                    self._last_active_cache = None
                    return

                # Other errors — keep message_id for next retry
                log.warning("active-tasks edit failed (%s); keeping message_id for next retry", e)

    async def _reconcile_pinned(self) -> None:
        """Telegram allows at most one pinned message per chat; pin ours."""
        if not self._cfg.pin_status_message or self._bot is None:
            return
        if self._active_msg_id is None or self._active_msg_id == self._pinned_message_id:
            return
        try:
            await self._bot.pin_chat_message(
                self._cfg.chat_id, self._active_msg_id,
            )
            self._pinned_message_id = self._active_msg_id
        except TelegramError as e:
            # Permission errors etc. — log warning and disable further pin attempts
            log.warning("pin failed (%s); disabling pin_status_message", e)
            self._cfg.pin_status_message = False

    # ---- one-shot notification (errors that don't belong to a torrent) ----

    async def notify(self, message: str) -> None:
        if not self._cfg.enabled or self._bot is None:
            return
        try:
            await self._bot.send_message(
                self._cfg.chat_id, message, parse_mode=ParseMode.MARKDOWN,
            )
        except TelegramError as e:
            log.warning("telegram notify failed: %s", e)