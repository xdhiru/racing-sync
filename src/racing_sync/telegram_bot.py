"""Telegram bot.

Two surfaces, both using the chat's edit-in-place mechanism so chat history
stays clean:

  1. **Per-torrent detail message** — one Telegram message per source_infohash.
     Created when the torrent first leaves NEW. Edited in place as it advances
     through states (NEW → QUERYING → WAITING_INDEXER → ... → DONE). The
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
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

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
    State.NEW: "🆕 NEW",
    State.QUERYING: "🔍 QUERY",
    State.WAITING_INDEXER: "⏳ WAIT-SP",
    State.WAITING_DISK: "💾 WAIT-SSD",
    State.QUEUED: "📋 QUEUED",
    State.DOWNLOADING: "⬇️ DOWNLOADING",
    State.MOVING: "📦 MOVING",
    State.RE_ADDING: "🔄 RE-ADDING",
    State.DONE: "✅ DONE",
    State.FAILED: "❌ FAILED",
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


def _strip_code_spans(text: str) -> str:
    """Remove `...` spans so delimiter balancing ignores literal * / _ inside code."""
    return re.sub(r"`[^`]*`", "", text)


def _unclosed_markdown_delims(text: str) -> str:
    """Return closers needed for delimiters left open in `text`.

    `*` and `_` inside `code` spans are literal and must not be counted —
    otherwise we append a spurious closer outside the span and break parsing.
    """
    to_close = ""
    if len(re.findall(r"(?<!\\)`", text)) % 2 != 0:
        to_close += "`"
    outside_code = _strip_code_spans(text)
    if len(re.findall(r"(?<!\\)\*", outside_code)) % 2 != 0:
        to_close += "*"
    if len(re.findall(r"(?<!\\)_", outside_code)) % 2 != 0:
        to_close += "_"
    return to_close


def _safe_truncate_markdown(text: str, max_len: int = 4096) -> str:
    """Truncate text to max_len while keeping markdown tags properly closed and ending with '...'."""
    if len(text) <= max_len:
        return text

    lines = text.split("\n")
    acc: list[str] = []
    curr_len = 0
    suffix = "\n..."
    budget = max(0, max_len - len(suffix))
    for line in lines:
        added = len(line) + (1 if acc else 0)
        if curr_len + added <= budget:
            acc.append(line)
            curr_len += added
        else:
            break

    if acc and len(acc) > 1:
        truncated = "\n".join(acc)
        suffix = "\n..."
    else:
        suffix = "..."
        truncated = text[: max(0, max_len - len(suffix))]

    to_close = _unclosed_markdown_delims(truncated)

    if to_close:
        excess = (len(truncated) + len(to_close) + len(suffix)) - max_len
        if excess > 0:
            truncated = truncated[:-excess]
            to_close = _unclosed_markdown_delims(truncated)

    return truncated + to_close + suffix


def _short_name(name: str, limit: int = 56) -> str:
    """Trim long release names with an ellipsis in the middle."""
    if len(name) <= limit:
        return name
    head = (limit - 1) // 2
    tail = limit - 1 - head
    return name[:head] + "…" + name[-tail:]


def _bytes_human(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    units = ("KB", "MB", "GB", "TB", "PB")
    f = float(n)
    idx = -1
    while f >= 1024 and idx < len(units) - 1:
        f /= 1024
        idx += 1
    return f"{f:.1f} {units[idx]}"


def _tracker_domain(url: str) -> str:
    """Extract host/domain from announce URL (e.g. nyaa.tracker.wf)."""
    if not url:
        return ""
    try:
        raw = url.strip()
        if "://" not in raw:
            raw = f"http://{raw}"
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


# --------------------------------------------------------------------------- #
# Message renderers
# --------------------------------------------------------------------------- #


def _retry_at_in_future(value: object) -> bool:
    """True iff value is a datetime in the future (naive treated as UTC)."""
    if not isinstance(value, dt.datetime):
        return False
    try:
        now = dt.datetime.now(dt.timezone.utc)
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value > now
    except Exception:
        return False


def render_detail(ts: TorrentState, progress: float | None = None) -> str:
    """Per-torrent detail message (edited in place as state advances)."""
    icon = _STATE_ICON.get(ts.state, ts.state.value.upper())
    name = ts.source_name.replace("`", "'").replace("\n", " ").replace("\r", " ").rstrip("\\")
    size = _bytes_human(ts.total_bytes)
    full_hash = (ts.source_infohash or "").lower()

    lines: list[str] = []
    # 1. Title line: state badge + full name copiable by click
    lines.append(f"{icon} `{name}`")

    # 2. Hash & size line: full hash copiable by click + size in plain text
    meta_parts = [f"`{full_hash}`", size]
    if ts.batches_total > 1:
        meta_parts.append(f"batch {ts.batch_index}/{ts.batches_total}")
    lines.append(" · ".join(meta_parts))

    # State-specific extras
    if ts.state == State.WAITING_INDEXER and ts.indexer_next_retry_at:
        when = ts.indexer_next_retry_at.astimezone().strftime("%H:%M:%S")
        lines.append(
            f"Indexer miss #{ts.indexer_attempts}; next retry at {when}"
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
            lines.append(f"{bar} {pct}")
        else:
            lines.append("Downloading…")
    elif ts.state == State.MOVING:
        lines.append("rclone moving to remote…")
    elif ts.state == State.RE_ADDING:
        if _retry_at_in_future(ts.readd_next_retry_at):
            mins = max(1, round((ts.readd_next_retry_at - dt.datetime.now(dt.timezone.utc)).total_seconds() / 60))
            lines.append(f"Re-adding on fuse mount (WebUI busy, retrying in {mins}m)")
        else:
            lines.append("Re-adding on fuse mount")
    elif ts.state == State.DONE:
        lines.append("✓ Seeded from fuse mount")
    elif ts.state == State.FAILED:
        from .logging_setup import sanitize_log_text as _san
        raw_err = (ts.last_error or "")[:200].replace("\n", " ").replace("\r", " ")
        err = _esc(_san(raw_err)) if raw_err else "no detail"
        lines.append(f"✗ Failed: {err}")

    # Cross-seed info
    if ts.cross_seed_source:
        cs_hash = (ts.cross_seed_infohash or "").lower()
        if cs_hash and len(cs_hash) == 40 and all(c in "0123456789abcdef" for c in cs_hash):
            lines.append(f"SSD source: {_esc(ts.cross_seed_source)} · `{cs_hash}`")
        else:
            lines.append(f"SSD source: {_esc(ts.cross_seed_source)}")

    # Classifier
    if ts.classification_kind and ts.classification_kind != "unknown":
        lines.append(f"Classifier: {_esc(ts.classification_kind)}")

    # Tracker domain only (not full announce URL)
    domain = _tracker_domain(ts.source_tracker) or _tracker_domain(ts.source_announce_url)
    if domain:
        lines.append(f"Source: {_esc(domain)}")

    return _safe_truncate_markdown("\n".join(lines))


def render_active(
    active: list[tuple[TorrentState, float | None]],
    page: int = 0,
    page_size: int = 5,
) -> tuple[str, int, int]:
    """Render paginated list of active tasks with numbered items."""
    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = 5
    page_size = max(1, min(page_size, 50))
    total_items = len(active)
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    cur_page = max(0, min(page, total_pages - 1))

    if not active:
        return (
            "*Active Tasks*\n\n_No active tasks in flight._",
            0,
            1,
        )

    start_idx = cur_page * page_size
    end_idx = min(start_idx + page_size, total_items)
    page_items = active[start_idx:end_idx]

    lines = [
        f"*Active Tasks* · *Page {cur_page + 1}/{total_pages}* ({total_items} in flight)",
        "",
    ]

    for i, (ts, progress) in enumerate(page_items):
        item_num = start_idx + i + 1
        name = (ts.source_name or "").replace("`", "'").replace("\n", " ").replace("\r", " ").rstrip("\\")
        size = _bytes_human(ts.total_bytes)
        full_hash = (ts.source_infohash or "").lower()

        # 1. Full name of the torrent, copiable by click (in backticks, no escape chars)
        lines.append(f"*{item_num}.* `{name}`")

        # 2. Size below the name, separator dot with space, and full hash copiable by click
        lines.append(f"  {size} · `{full_hash}`")

        # 3. Next line shows state, batch (if applicable), and tracker domain at the last
        if ts.state == State.DOWNLOADING:
            if progress is not None:
                state_text = f"⬇️ Downloading · {progress * 100:.1f}%"
            else:
                state_text = "⬇️ Downloading"
        elif ts.state == State.QUEUED:
            state_text = "📋 Queued"
        elif ts.state == State.MOVING:
            state_text = "📦 Moving"
        elif ts.state == State.RE_ADDING:
            if _retry_at_in_future(ts.readd_next_retry_at):
                mins = max(1, round((ts.readd_next_retry_at - dt.datetime.now(dt.timezone.utc)).total_seconds() / 60))
                state_text = f"🔄 Re-adding (retry in {mins}m)"
            else:
                state_text = "🔄 Re-adding"
        elif ts.state == State.QUERYING:
            state_text = "🔍 Querying"
        elif ts.state == State.WAITING_INDEXER:
            state_text = f"⏳ Waiting for Indexer (#{ts.indexer_attempts})"
        elif ts.state == State.WAITING_DISK:
            state_text = "💾 Waiting for SSD space"
        elif ts.state == State.DONE:
            state_text = "✅ Done"
        elif ts.state == State.FAILED:
            state_text = "❌ Failed"
        else:
            state_text = f"🆕 {ts.state.value.capitalize()}"

        if ts.batches_total > 1:
            state_text += f" · Batch {ts.batch_index}/{ts.batches_total}"

        domain = _tracker_domain(ts.source_announce_url) or _tracker_domain(ts.source_tracker)
        if domain:
            state_text += f" · {_esc(domain)}"

        lines.append(f"  {state_text}")
        lines.append("")

    rendered = _safe_truncate_markdown("\n".join(lines).strip())
    return rendered, cur_page, total_pages


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
        self._detail_queue: asyncio.Queue[tuple[str, float | None]] | None = None
        self._detail_worker: asyncio.Task | None = None
        # In-process cache: source_infohash -> message_id, so we don't
        # need to hit state.db for every send.
        self._detail_cache: dict[str, int] = {}
        # Cached "last active-tasks (page, total_pages, text)" so we skip identical edits.
        self._last_active_text: str = ""
        self._last_active_cache: tuple[int, int, str] | None = None
        self._last_callback_time: float = 0.0
        # Per-chat debounce (monotonic timestamps by chat/user key): one
        # chat's burst must not starve pagination for everyone else.
        self._callback_times: dict[str, float] = {}
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

    # ---- lifecycle ----

    async def start(self) -> None:
        if not self._cfg.enabled:
            return
        self._stopped = False
        bot_token = self._cfg.bot_token.get_secret_value() if hasattr(self._cfg.bot_token, "get_secret_value") else str(self._cfg.bot_token)
        self._bot = Bot(token=bot_token)
        # Per-torrent message queue: bounded so a torrent flood doesn't
        # grow memory. 256 is well over what any operator needs.
        self._detail_queue = asyncio.Queue(maxsize=256)
        # Pre-fill cache from the store so we don't re-send every
        # torrent on restart.
        all_items = await asyncio.to_thread(self._store.all)
        for ts in all_items:
            if ts.telegram_message_id:
                self._detail_cache[ts.source_infohash] = ts.telegram_message_id
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
            self._detail_worker_loop(), name="rs-telegram-detail",
        )
        try:
            # Send initial "online" message (separate from active-tasks)
            sent_online = await self._bot.send_message(self._cfg.chat_id, "racing-sync online")
            self._note_outbound(getattr(sent_online, "message_id", None))
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
        arrive in the same chat scroll anyway).
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
        ts = await asyncio.to_thread(self._store.get, infohash)
        if ts is None:
            return
        text = render_detail(ts, progress)
        msg_id = self._detail_cache.get(infohash)
        if msg_id is None:
            msg_id = await asyncio.to_thread(self._store.get_telegram_message_id, infohash)
        try:
            if msg_id is None:
                try:
                    sent = await self._bot.send_message(
                        self._cfg.chat_id, text,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except TelegramError as e:
                    msg = str(e).lower()
                    if "can't parse" in msg or "entity" in msg:
                        sent = await self._bot.send_message(
                            self._cfg.chat_id, text,
                        )
                    else:
                        raise
                self._note_outbound(getattr(sent, "message_id", None))
                self._detail_cache[infohash] = sent.message_id
                await asyncio.to_thread(
                    self._store.set_telegram_message_id,
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
                        try:
                            sent = await self._bot.send_message(
                                self._cfg.chat_id, text,
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError as e2:
                            msg2 = str(e2).lower()
                            if "can't parse" in msg2 or "entity" in msg2:
                                sent = await self._bot.send_message(
                                    self._cfg.chat_id, text,
                                )
                            else:
                                raise
                        self._note_outbound(getattr(sent, "message_id", None))
                        self._detail_cache[infohash] = sent.message_id
                        await asyncio.to_thread(
                            self._store.set_telegram_message_id,
                            infohash, sent.message_id,
                        )
                    elif "can't parse" in msg or "entity" in msg:
                        await self._bot.edit_message_text(
                            text,
                            chat_id=self._cfg.chat_id,
                            message_id=msg_id,
                        )
                    else:
                        log.warning(
                            "telegram detail edit failed for %s: %s",
                            infohash[:10], e,
                        )
        except RetryAfter as e:
            if isinstance(e.retry_after, dt.timedelta):
                wait_s = int(e.retry_after.total_seconds()) + 1
            else:
                wait_s = int(e.retry_after) + 1
            log.warning("telegram flood control hit; backing off for %ds", wait_s)
            await asyncio.sleep(wait_s)
            self._enqueue_detail(infohash, progress)
        except (TimedOut, NetworkError) as e:
            log.warning("telegram detail send timed out (%s); will retry next interval", e)
            self._enqueue_detail(infohash, progress)
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

        # Throttle callback handling (0.5s debounce per chat/user — a
        # global throttle lets one spammer block pagination for all chats).
        now = time.monotonic()
        try:
            _debounce_key = str(chat_id) if chat_id is not None else str(user_id)
        except Exception:
            _debounce_key = ""
        try:
            _times = getattr(self, "_callback_times", None)
            if not isinstance(_times, dict):
                _times = {}
                self._callback_times = _times
            _last = float(_times.get(_debounce_key, 0.0) or 0.0)
        except Exception:
            _last = 0.0
        if now - _last < 0.5:
            try:
                await query.answer()
            except Exception:
                pass
            return
        try:
            _times[_debounce_key] = now
            if len(_times) > 1000:
                for _k in list(_times.keys())[:500]:
                    _times.pop(_k, None)
        except Exception:
            pass
        self._last_callback_time = now

        try:
            await query.answer()
        except Exception:
            pass

        data = str(getattr(query, "data", "") or "")
        if not data.startswith("page:"):
            return

        action = data.split(":", 1)[1]
        rows = await asyncio.to_thread(self._store.list_active_inflight)
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
        # Serialize periodic refresh vs callback-triggered refresh so
        # concurrent edits can't interleave or race the dedup cache.
        # (Lazy: unit tests build the bot via object.__new__.)
        lock = getattr(self, "_active_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._active_lock = lock
        async with lock:
            await self._refresh_active_message_inner()

    async def _refresh_active_message_inner(self) -> None:
        assert self._bot is not None
        # Sentinel -1 means "stop trying to edit" (e.g. chat permission issue)
        if self._active_msg_id == -1:
            return
        rows = await asyncio.to_thread(self._store.list_active_inflight)
        # Pull live progress from coordinator's tracker
        progress_map = self._coord.live_progress_map()
        items: list[tuple[TorrentState, float | None]] = [
            (ts, progress_map.get(ts.source_infohash.lower()))
            for ts in rows
        ]
        text, cur_page, total_pages = render_active(
            items, page=self._current_page, page_size=self._cfg.page_size
        )
        if len(text) > 4096:
            text = _safe_truncate_markdown(text)
        self._current_page = cur_page
        keyboard = self._build_keyboard(cur_page, total_pages)

        # Skip the API call if page, total_pages, and text are identical —
        # unless a keep-at-bottom repost is due (position refreshes even
        # when the content is unchanged).
        cache_key = (cur_page, total_pages, text)
        repost_due = self._repost_due()
        if cache_key == self._last_active_cache and self._active_msg_id is not None and not repost_due:
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
                self._last_repost_monotonic = time.monotonic()
                self._note_outbound(sent.message_id)
                await asyncio.to_thread(
                    self._store.set_meta, "telegram_active_msg_id", str(sent.message_id)
                )
            except (TimedOut, NetworkError) as e:
                log.warning("active-tasks send timed out (%s); will retry next interval", e)
            except TelegramError as e:
                msg = str(e).lower()
                if "can't parse" in msg or "entity" in msg:
                    try:
                        sent = await self._bot.send_message(
                            self._cfg.chat_id, text,
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
                else:
                    log.warning("active-tasks send failed: %s", e)
        else:
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

                if "can't parse" in msg or "entity" in msg:
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
                    await asyncio.to_thread(
                        self._store.set_meta, "telegram_active_msg_id", ""
                    )
                    return

                # "Message to edit not found" / "MESSAGE_ID_INVALID" — deleted from chat
                if "message to edit not found" in msg or "message_id_invalid" in msg:
                    log.warning("active-tasks message not found in chat; will resend")
                    self._active_msg_id = None
                    self._last_active_cache = None
                    await asyncio.to_thread(
                        self._store.set_meta, "telegram_active_msg_id", ""
                    )
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

    async def _repost_active_message(
        self, text: str, keyboard: InlineKeyboardMarkup | None,
        cache_key: tuple[int, int, str],
    ) -> None:
        """Delete the old active message and resend it silently as newest.

        Sends with notifications disabled so the periodic bump never buzzes
        the chat. Any failure degrades to "resend fresh next tick" — the id
        is cleared so the next refresh takes the normal send path.
        """
        assert self._bot is not None
        old_id = self._active_msg_id
        try:
            await self._bot.delete_message(
                chat_id=self._cfg.chat_id,
                message_id=old_id,
            )
        except Exception:
            pass
        try:
            try:
                sent = await self._bot.send_message(
                    self._cfg.chat_id, text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard,
                    disable_notification=True,
                )
            except TelegramError as e:
                msg = str(e).lower()
                if "can't parse" in msg or "entity" in msg:
                    sent = await self._bot.send_message(
                        self._cfg.chat_id, text,
                        reply_markup=keyboard,
                        disable_notification=True,
                    )
                else:
                    raise
        except (RetryAfter, TimedOut, NetworkError) as e:
            log.warning("active-tasks repost skipped due to temporary network/rate-limit (%s); will retry next interval", e)
            self._active_msg_id = None
            self._last_active_cache = None
            await asyncio.to_thread(
                self._store.set_meta, "telegram_active_msg_id", ""
            )
            return
        except TelegramError as e:
            log.warning("active-tasks repost failed (%s); will resend next interval", e)
            self._active_msg_id = None
            self._last_active_cache = None
            await asyncio.to_thread(
                self._store.set_meta, "telegram_active_msg_id", ""
            )
            return
        self._active_msg_id = sent.message_id
        self._prev_active_msg_id = sent.message_id
        self._last_active_cache = cache_key
        self._last_repost_monotonic = time.monotonic()
        self._note_outbound(sent.message_id)
        await asyncio.to_thread(
            self._store.set_meta, "telegram_active_msg_id", str(sent.message_id)
        )

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
        truncated = _safe_truncate_markdown(message)
        try:
            sent = await self._bot.send_message(
                self._cfg.chat_id, truncated, parse_mode=ParseMode.MARKDOWN,
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