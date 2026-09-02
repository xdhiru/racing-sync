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

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TelegramError

from .config import TelegramConfig
from .coordinator import Coordinator
from .state import State, StateStore, TorrentState

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Format helpers
# --------------------------------------------------------------------------- #


_STATE_ICON = {
    State.NEW: "[NEW]",
    State.QUERYING: "[QUERY]",
    State.WAITING_SEEDPOOL: "[WAIT-SP]",
    State.WAITING_DISK: "[WAIT-SSD]",
    State.QUEUED: "[Q]",
    State.DOWNLOADING: "[DL]",
    State.MOVING: "[MOVE]",
    State.RE_ADDING: "[RE-ADD]",
    State.DONE: "[DONE]",
    State.FAILED: "[FAIL]",
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


def render_active(active: list[tuple[TorrentState, float | None]]) -> str:
    """Compact list of currently in-flight torrents.

    Each line: filename (short), size, hash-end, state, batch.
    """
    now = dt.datetime.now().strftime("%H:%M:%S")
    lines = [f"*Active tasks* \\- `{now}`", ""]
    if not active:
        lines.append("_idle_")
    else:
        for ts, progress in active[:30]:
            name = _short_name(ts.source_name, 36)
            size = _bytes_human(ts.total_bytes)
            state = _STATE_ICON.get(ts.state, ts.state.value)
            batch = f"{ts.batch_index}/{ts.batches_total}"
            line = (
                f"{state} `{name}`\n"
                f"    `{ts.source_infohash[-5:]}` · {size} · batch `{batch}`"
            )
            if progress is not None and ts.state == State.DOWNLOADING:
                pct = f"{progress * 100:4.0f}%"
                line += f" · {pct}"
            lines.append(line)
    return "\n".join(lines)


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
        self._active_msg_id: int | None = None
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
        # Cached "last active-tasks text" so we skip identical edits.
        self._last_active_text: str = ""

    # ---- lifecycle ----

    async def start(self) -> None:
        if not self._cfg.enabled:
            return
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

    async def stop(self) -> None:
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
        while True:
            try:
                await self._refresh_active_message()
                await self._reconcile_pinned()
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
        except TelegramError as e:
            log.warning("telegram detail send failed for %s: %s",
                        infohash[:10], e)

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
        text = render_active(items)
        # Skip the API call entirely if the text is identical to last
        # time. Saves 30+ API hits per minute when nothing is changing.
        if text == self._last_active_text and self._active_msg_id is not None:
            return
        self._last_active_text = text
        if self._active_msg_id is None:
            sent = await self._bot.send_message(
                self._cfg.chat_id, text,
                parse_mode=ParseMode.MARKDOWN,
            )
            self._active_msg_id = sent.message_id
        else:
            try:
                await self._bot.edit_message_text(
                    text,
                    chat_id=self._cfg.chat_id,
                    message_id=self._active_msg_id,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except TelegramError as e:
                msg = str(e).lower()
                # "Message is not modified" — Telegram returns 400 when the
                # new text is byte-identical to the existing one. This is
                # benign; just skip this tick.
                if "not modified" in msg:
                    return
                # "Message to edit not found" / "MESSAGE_ID_INVALID" — the
                # message was deleted from the chat; re-send.
                if "message to edit not found" in msg or "message_id_invalid" in msg:
                    log.warning("active-tasks message not found; resending")
                    self._active_msg_id = None
                    return
                # "Chat not found" — bot can send but not edit (permission issue,
                # wrong chat_id format for editing, or bot not admin in channel).
                # Don't spam; log once and stop trying to edit.
                if "chat not found" in msg:
                    log.error(
                        "active-tasks edit failed: 'Chat not found'. "
                        "Bot can send but not edit messages. "
                        "Check: bot is admin in channel, chat_id is numeric (-100...), "
                        "or disable active-tasks message. Disabling active-tasks updates."
                    )
                    self._active_msg_id = -1  # Sentinel: stop trying to edit
                    return
                # Rate limit / flood control — keep message_id and skip this interval
                if isinstance(e, RetryAfter) or "flood control" in msg or "too many requests" in msg:
                    log.warning("active-tasks edit rate-limited (%s); will retry on next interval", e)
                    return
                # Other errors — log and re-send once.
                log.warning("active-tasks edit failed (%s); resending", e)
                self._active_msg_id = None

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