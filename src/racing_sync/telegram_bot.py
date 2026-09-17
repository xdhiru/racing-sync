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
    State.WAITING_INDEXER: "⏳ WAIT-IDX",
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


def _is_parse_error(e: BaseException) -> bool:
    """True iff a Telegram error is a Markdown parse/entity failure.

    Those (and only those) are retried as plain text; every other error
    keeps its own handling at each call site.
    """
    msg = str(e).lower()
    return "can't parse" in msg or "entity" in msg


def _as_aware_utc(value: object) -> dt.datetime | None:
    """Coerce to aware UTC; naive legacy rows are treated as UTC, not local."""
    if not isinstance(value, dt.datetime):
        return None
    try:
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value
    except Exception:
        return None


def _retry_in_future_seconds(value: object) -> float | None:
    """Seconds until `value`, or None when not a future datetime."""
    aware = _as_aware_utc(value)
    if aware is None:
        return None
    try:
        delta = (aware - dt.datetime.now(dt.timezone.utc)).total_seconds()
        return delta if delta > 0 else None
    except Exception:
        return None


def _row_text_bits(ts: TorrentState) -> tuple[str, str, str]:
    """Sanitized (name, human size, lowercase hash) shared by both renderers."""
    name = (ts.source_name or "").replace("`", "'").replace("\n", " ").replace("\r", " ").rstrip("\\")
    return name, _bytes_human(ts.total_bytes), (ts.source_infohash or "").lower()


def _batch_display(ts: TorrentState) -> str:
    """1-based 'Batch i/n' label, or '' when not a multi-batch row.

    `batch_index` is 0-based while work is in flight and clamps to
    `batches_total` once the last batch is done — display
    min(index+1, total) so the first batch reads 'Batch 1/3' and a
    finished row reads 'Batch 3/3', never 'Batch 0/3' or 'Batch 4/3'.
    """
    try:
        total = int(ts.batches_total or 0)
        idx = int(ts.batch_index or 0)
    except (TypeError, ValueError):
        return ""
    if total <= 1:
        return ""
    return f"Batch {min(max(idx + 1, 1), total)}/{total}"


def _retry_minutes(ts: TorrentState) -> int | None:
    """Whole minutes until the re-add retry, or None when no timer is set."""
    _retry_s = _retry_in_future_seconds(ts.readd_next_retry_at)
    if _retry_s is None:
        return None
    return max(1, round(_retry_s / 60))


def render_detail(ts: TorrentState, progress: float | None = None,
                  note: str = "") -> str:
    """Per-torrent detail message (edited in place as state advances).

    `note` is an optional one-line extra (e.g. why a NEW row is waiting)
    rendered after the state-specific lines.
    """
    icon = _STATE_ICON.get(ts.state, ts.state.value.upper())
    name, size, full_hash = _row_text_bits(ts)

    lines: list[str] = []
    # 1. Title line: state badge + full name copiable by click
    lines.append(f"{icon} `{name}`")

    # 2. Hash & size line: full hash copiable by click + size in plain text
    # (detail card keeps the FULL hash; the Active Tasks list shows short).
    meta_parts = [f"`{full_hash}`", size]
    _bd = _batch_display(ts)
    if _bd:
        meta_parts.append(_bd.lower())
    lines.append(" · ".join(meta_parts))

    # State-specific extras
    if ts.state == State.WAITING_INDEXER and ts.indexer_next_retry_at:
        _when = _as_aware_utc(ts.indexer_next_retry_at)
        when = _when.astimezone().strftime("%H:%M:%S") if _when else "?"
        lines.append(
            f"Indexer miss #{ts.indexer_attempts}; next retry at {when}"
        )
    if ts.state == State.WAITING_INDEXER:
        lines.append(f"Fetch VPS1 original now: `{_fetch_command(full_hash)}`")
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
        if (ts.last_error or "").strip():
            from .logging_setup import sanitize_log_text as _san
            _reason = (ts.last_error or "").replace("\n", " ").replace("\r", " ")
            _reason = _san(_reason).strip()
            if len(_reason) > 160:
                _reason = _reason[:157] + "..."
            if _reason:
                lines.append(f"retrying: {_esc(_reason)}")
    elif ts.state == State.RE_ADDING:
        _mins = _retry_minutes(ts)
        if _mins is not None:
            lines.append(f"Re-adding on fuse mount (WebUI busy, retrying in {_mins}m)")
        else:
            lines.append("Re-adding on fuse mount")
    elif ts.state == State.DONE:
        lines.append("✓ Seeded from fuse mount")
    elif ts.state == State.FAILED:
        from .logging_setup import sanitize_log_text as _san
        raw_err = (ts.last_error or "")[:200].replace("\n", " ").replace("\r", " ")
        err = _esc(_san(raw_err)) if raw_err else "no detail"
        lines.append(f"✗ Failed: {err}")

    if note:
        lines.append(f"⏳ {_esc(note)}")

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


#: Hex chars of the infohash shown in the per-task `/cancel_` command.
#: 10 chars (40 bits) is unambiguous for hundreds of rows and short enough
#: to copy-paste; full 40-char hashes are also accepted by the watcher.
CANCEL_SHORT_LEN = 10

#: `/cancel_<hex>` (optional `@bot` suffix, extra trailing text ignored).
CANCEL_CMD_RE = re.compile(r"^/cancel_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/fetch_<hex>` — same shape: use the VPS1 original for the SSD
#: download of a WAITING_INDEXER row instead of waiting for Prowlarr.
FETCH_CMD_RE = re.compile(r"^/fetch_([0-9a-fA-F]+)(?:@[\w_]+)?\b")


def _cancel_command(infohash: str) -> str:
    """Copy-pasteable cancel command for one task (short hash)."""
    return f"/cancel_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _fetch_command(infohash: str) -> str:
    """Copy-pasteable fetch-original command for one task (short hash)."""
    return f"/fetch_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _flood_wait_seconds(e: BaseException, default: int = 5) -> int | None:
    """Seconds Telegram asks us to wait, or None when not rate-limited.

    Flood control does not always arrive as a `RetryAfter` instance — the
    observed failure mode is a plain `TelegramError("Flood control
    exceeded. Retry in N seconds")`, which the old code logged and
    dropped, freezing the card at its last state forever. Any
    rate-limit-shaped error sleeps out the requested window and retries
    instead of dropping the update.
    """
    try:
        if isinstance(e, RetryAfter):
            raw = e.retry_after
            if isinstance(raw, dt.timedelta):
                return int(raw.total_seconds()) + 1
            return int(raw) + 1
    except Exception:
        pass
    try:
        msg = str(e or "").lower()
    except Exception:
        return None
    if not any(k in msg for k in (
        "flood", "too many requests", "rate limit", "ratelimit",
        "slow mode", "slowmode",
    )) and re.search(r"retry (?:in|after) \d+", msg) is None:
        return None
    try:
        m = re.search(r"retry (?:in|after) (\d+)", msg)
        if m:
            return int(m.group(1)) + 1
    except Exception:
        pass
    return default


def render_active(
    active: list[tuple[TorrentState, float | None]],
    page: int = 0,
    page_size: int = 5,
    notes: dict[str, str] | None = None,
) -> tuple[str, int, int]:
    """Render paginated list of active tasks with numbered items.

    `notes` maps source_infohash -> one-line extra (e.g. why a NEW row is
    waiting); a present note replaces the state line with `⏳ <note>`.
    """
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
        name, size, full_hash = _row_text_bits(ts)
        short_hash = (full_hash or "")[:CANCEL_SHORT_LEN]
        try:
            note = (notes or {}).get(ts.source_infohash or "") or ""
        except Exception:
            note = ""

        # 1. Full name of the torrent, copiable by click (in backticks, no escape chars)
        lines.append(f"*{item_num}.* `{name}`")

        # 2. Size below the name, separator dot with space, and SHORT hash
        # copiable by click (detail card keeps the full 40-char hash).
        lines.append(f"  {size} · `{short_hash}`")

        # 3. State line — or the deferral note, which already names why
        # the row is waiting (shares the Cancel/tracker tail below).
        _bd = _batch_display(ts)
        if note and ts.state in (State.NEW, State.WAITING_DISK):
            state_text = f"⏳ {note}"
        elif ts.state == State.DOWNLOADING:
            if progress is not None:
                state_text = f"⬇️ Downloading · {progress * 100:.1f}%"
            else:
                state_text = "⬇️ Downloading"
        elif ts.state == State.QUEUED:
            state_text = "📋 Queued"
        elif ts.state == State.MOVING:
            if _bd:
                state_text = f"📦 Moving leftovers · {_bd}"
            else:
                state_text = "📦 Moving"
            if (ts.last_error or "").strip():
                state_text += " · retrying"
        elif ts.state == State.RE_ADDING:
            _mins = _retry_minutes(ts)
            if _mins is not None:
                state_text = f"🔄 Re-adding (retry in {_mins}m)"
            else:
                state_text = "🔄 Re-adding"
        elif ts.state == State.QUERYING:
            state_text = "🔍 Querying"
        elif ts.state == State.WAITING_INDEXER:
            state_text = f"⏳ Waiting for download indexer (#{ts.indexer_attempts})"
        elif ts.state == State.WAITING_DISK:
            state_text = "💾 Waiting for SSD space"
        elif ts.state == State.DONE:
            state_text = "✅ Done"
        elif ts.state == State.FAILED:
            state_text = "❌ Failed"
        else:
            state_text = f"🆕 {ts.state.value.capitalize()}"

        if _bd and ts.state != State.MOVING:
            state_text += f" · {_bd}"

        domain = _tracker_domain(ts.source_announce_url) or _tracker_domain(ts.source_tracker)
        if domain:
            state_text += f" · {_esc(domain)}"

        lines.append(f"  {state_text}")
        # Per-task copy-paste cancel command (no buttons, no confirm —
        # the user's sent message is final). In backticks so the
        # underscore in `/cancel_...` can't break Markdown parsing and
        # mobile clients offer tap-to-copy.
        lines.append(f"  Cancel: `{_cancel_command(full_hash)}`")
        if ts.state == State.WAITING_INDEXER:
            lines.append(f"  Fetch original: `{_fetch_command(full_hash)}`")
        lines.append("")

    rendered = _safe_truncate_markdown("\n".join(lines).strip())
    return rendered, cur_page, total_pages


# --------------------------------------------------------------------------- #
# Bot
# --------------------------------------------------------------------------- #


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
        # Cached "last active-tasks (page, total_pages, text)" so we skip identical edits.
        self._last_active_cache: tuple[int, int, str] | None = None
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
        # torrent on restart. Sent states pre-fill too, so the stale-card
        # net stays quiet until a card actually drifts.
        all_items = await asyncio.to_thread(self._store.all)
        for ts in all_items:
            if ts.telegram_message_id:
                self._detail_cache[ts.source_infohash] = ts.telegram_message_id
            try:
                self._detail_sent_state[ts.source_infohash] = ts.state.value
            except Exception:
                pass
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

    async def _send_one_detail(self, infohash: str,
                               progress: float | None) -> None:
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
        try:
            state_value = ts.state.value
        except Exception:
            state_value = ""
        msg_id = self._detail_cache.get(infohash)
        if msg_id is None:
            msg_id = await asyncio.to_thread(self._store.get_telegram_message_id, infohash)

        async def _retry_rate_limited(err: BaseException) -> bool:
            """Sleep out a rate limit and requeue; False when not one."""
            _wait = _flood_wait_seconds(err)
            if _wait is None:
                return False
            log.warning(
                "telegram detail rate-limited for %s; retrying in %ds",
                infohash[:10], _wait,
            )
            await asyncio.sleep(_wait)
            self._enqueue_detail(infohash, progress)
            return True

        try:
            if msg_id is None:
                try:
                    sent = await self._bot.send_message(
                        self._cfg.chat_id, text,
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except TelegramError as e:
                    if _is_parse_error(e):
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
                                self._cfg.chat_id, text,
                                parse_mode=ParseMode.MARKDOWN,
                            )
                        except TelegramError as e2:
                            if _is_parse_error(e2):
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
                        self._mark_detail_sent(infohash, state_value)
                    elif _is_parse_error(e):
                        await self._bot.edit_message_text(
                            text,
                            chat_id=self._cfg.chat_id,
                            message_id=msg_id,
                        )
                        self._mark_detail_sent(infohash, state_value)
                    elif await _retry_rate_limited(e):
                        return
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
            if await _retry_rate_limited(e):
                return
            log.warning("telegram detail send failed for %s: %s",
                        infohash[:10], e)

    # ---- active tasks pagination & callback handling ----

    def _build_keyboard(
        self,
        current_page: int,
        total_pages: int,
    ) -> InlineKeyboardMarkup | None:
        """Pagination nav buttons (cancel is via `/cancel_` chat commands)."""
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
                    log.warning("telegram callback polling conflict (another bot session running?): %s", e)
                    await asyncio.sleep(15)
                else:
                    log.debug("telegram callback polling error: %s", e)
                    await asyncio.sleep(2)

    def _debounced(self, key: str, *, window: float = 0.5) -> bool:
        """True when `key` fired within `window` seconds (caller skips).

        Shared by callback pagination and /cancel_/fetch_ chat commands: a
        double-tap (or flooding client) re-resolves and re-acts otherwise.
        Fail-open on bookkeeping errors (never drop user input on doubt).
        """
        now = time.monotonic()
        try:
            times = getattr(self, "_callback_times", None)
            if not isinstance(times, dict):
                times = {}
                self._callback_times = times
            last = float(times.get(key, 0.0) or 0.0)
        except Exception:
            return False
        if now - last < window:
            return True
        try:
            times[key] = now
            if len(times) > 1000:
                for k in list(times.keys())[:500]:
                    times.pop(k, None)
        except Exception:
            pass
        return False

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
        try:
            _debounce_key = str(chat_id) if chat_id is not None else str(user_id)
        except Exception:
            _debounce_key = ""
        if self._debounced(_debounce_key):
            try:
                await query.answer()
            except Exception:
                pass
            return

        try:
            await query.answer()
        except Exception:
            pass

        data = str(getattr(query, "data", "") or "")
        if not data.startswith("page:"):
            return

        action = data.split(":", 1)[1]
        rows = await asyncio.to_thread(self._store.list_active_inflight)
        try:
            page_size = max(1, min(int(self._cfg.page_size), 50))
        except (TypeError, ValueError):
            page_size = 5
        total_pages = max(1, (len(rows) + page_size - 1) // page_size)

        if action == "prev":
            self._current_page = (self._current_page - 1) % total_pages
        elif action == "next":
            self._current_page = (self._current_page + 1) % total_pages
        elif action == "refresh":
            pass

        self._last_active_cache = None
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
            candidates = [
                ts for ts in rows
                if (ts.source_infohash or "").lower().startswith(norm)
            ]
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"cannot look up {short!r}: {e}") from e
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            preview = ", ".join(
                f"{(ts.source_name or '?')[:30]} ({(ts.source_infohash or '')[:10]})"
                for ts in candidates[:5]
            )
            raise LookupError(
                f"/{cmd}_{norm} matches {len(candidates)} torrents: {preview}; "
                f"send /{cmd}_<full 40-char hash>"
            )
        raise LookupError(
            f"no tracked torrent starts with {norm!r} "
            "(it may already be done/cancelled)"
        )

    def _resolve_fetch_target(self, short: str):
        """Map a `/fetch_<prefix|full-hash>` token to a WAITING_INDEXER row.

        Same prefix/full-hash semantics as cancel; the resolved row must
        still be waiting for the download indexer, otherwise fetching the
        VPS1 original is meaningless. Raises LookupError otherwise.
        """
        row = self._resolve_cancel_target(short, cmd="fetch")
        if row.state != State.WAITING_INDEXER:
            raise LookupError(
                f"{(row.source_name or row.source_infohash[:10])[:50]} is "
                f"{row.state.value}, not waiting for the download indexer — "
                "nothing to fetch"
            )
        return row

    async def _handle_chat_message(self, message: Any) -> None:
        """Execute `/cancel_<hash>` / `/fetch_<hash>` commands in the chat.

        Both are no-confirm: the user's sent message is final. Cancel
        forgets+ignores the release; fetch flags a WAITING_INDEXER row to
        use the VPS1 original for the SSD download instead of waiting for
        Prowlarr. Anything else is ignored. Only the configured chat/user
        may send commands.
        """
        try:
            chat = getattr(message, "chat", None)
            chat_id = getattr(chat, "id", None)
            from_user = getattr(message, "from_user", None)
            user_id = getattr(from_user, "id", None)
            cfg_chat = str(self._cfg.chat_id)
            if str(chat_id) != cfg_chat and str(user_id) != cfg_chat:
                return
            # Same 0.5s debounce as callbacks: a double-sent /cancel_ or
            # /fetch_ must not resolve+act twice (double forget/double
            # re-inject). Namespaced apart from callback keys.
            try:
                _ckey = f"cmd:{chat_id}" if chat_id is not None else f"cmd:{user_id}"
            except Exception:
                _ckey = ""
            if _ckey and self._debounced(_ckey):
                return
            text = (
                getattr(message, "text", None)
                or getattr(message, "caption", None)
                or ""
            )
            text = str(text or "").strip()
            m_fetch = FETCH_CMD_RE.match(text)
            m_cancel = CANCEL_CMD_RE.match(text)
            if not m_fetch and not m_cancel:
                return
            if m_fetch:
                short = m_fetch.group(1)
                try:
                    target = await asyncio.to_thread(
                        self._resolve_fetch_target, short)
                    full_hash = target.source_infohash
                except LookupError as e:
                    await self._reply(str(e)[:300], reply_to=message)
                    return
                result = await self._fetch_torrent(full_hash)
                await self._reply(result[:300], reply_to=message)
                self._last_active_cache = None
                await self._refresh_active_message()
                return
            short = m_cancel.group(1)
            try:
                target = await asyncio.to_thread(
                    self._resolve_cancel_target, short)
                full_hash = target.source_infohash
            except LookupError as e:
                await self._reply(str(e)[:300], reply_to=message)
                return
            result = await self._cancel_torrent(full_hash)
            await self._reply(result[:300], reply_to=message)
            self._last_active_cache = None
            await self._refresh_active_message()
        except Exception as e:  # noqa: BLE001
            log.debug("chat command handling failed: %s", e)

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

    async def _cancel_torrent(self, infohash: str) -> str:
        """Forget + ignore one release (row, dest entries, SSD data)."""
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        try:
            from .forget import forget_torrent
        except Exception as e:  # noqa: BLE001
            return f"Cancel failed: {e}"
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Cancel failed: bot not attached"
        dest = getattr(coord, "dest_client", None)
        cfg = getattr(coord, "cfg", None)
        if dest is None or cfg is None:
            return "Cancel failed: coordinator not ready"
        # Snapshot the detail card before forget deletes the row (the
        # telegram_message_id lives on the row): on success the card is
        # edited to CANCELLED so it doesn't freeze at its last live state.
        detail_msg_id: int | None = None
        try:
            cached = getattr(self, "_detail_cache", None)
            if isinstance(cached, dict):
                cached_id = cached.get(infohash)
                if isinstance(cached_id, int) and cached_id > 0:
                    detail_msg_id = cached_id
            if detail_msg_id is None:
                detail_msg_id = await asyncio.to_thread(
                    store.get_telegram_message_id, infohash)
        except Exception:
            detail_msg_id = None
        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    result = await forget_torrent(
                        cfg, dest=dest, store=store, target=infohash,
                        apply=True, delete_files=True, ignore=True,
                    )
                    try:
                        await coord._ssd_release(
                            result.get("source_infohash") or infohash)
                    except Exception:
                        pass
            else:
                result = await forget_torrent(
                    cfg, dest=dest, store=store, target=infohash,
                    apply=True, delete_files=True, ignore=True,
                )
                try:
                    await coord._ssd_release(
                        result.get("source_infohash") or infohash)
                except Exception:
                    pass
        except LookupError:
            return "Already gone from tracking"
        except Exception as e:  # noqa: BLE001
            return f"Cancel failed: {e}"
        name = str(result.get("source_name") or infohash[:10])[:50]
        # The row (and its telegram_message_id) is gone: mark its detail
        # card cancelled (best-effort) and drop the cached id so a future
        # re-discovery of the same hash starts a fresh card instead of
        # editing this one.
        try:
            cached = getattr(self, "_detail_cache", None)
            if isinstance(cached, dict):
                cached.pop(infohash, None)
            sent_map = getattr(self, "_detail_sent_state", None)
            if isinstance(sent_map, dict):
                sent_map.pop(infohash, None)
        except Exception:
            pass
        if detail_msg_id:
            await self._mark_detail_cancelled(detail_msg_id, name, infohash)
        try:
            for pair in result.get("paired_cancelled") or []:
                try:
                    await coord._ssd_release(pair.get("source_infohash") or "")
                except Exception:
                    pass
        except Exception:
            pass
        pairs = result.get("paired_cancelled") or []
        pair_note = ""
        if pairs:
            pair_note = " + {} waiting pair{}: {}".format(
                len(pairs), "" if len(pairs) == 1 else "s",
                ", ".join(str(p.get("source_name") or p.get("source_infohash", "")[:10])[:40]
                          for p in pairs[:3]),
            )
            if len(pairs) > 3:
                pair_note += f" (+{len(pairs) - 3} more)"
        errs = result.get("errors") or []
        if errs:
            return f"Cancelled {name}{pair_note} with {len(errs)} error(s); check logs"
        return f"Cancelled {name}{pair_note} (removed + ignored)"

    async def _fetch_torrent(self, infohash: str) -> str:
        """Flag a WAITING_INDEXER row to use the VPS1 original now.

        Sets the sticky `force_direct` flag and wakes the row
        (WAITING_INDEXER -> QUERYING) so the next tick picks the racing
        torrent's own bytes for the SSD download instead of waiting out
        the Prowlarr retry window. VPS2 then leeches the private swarm,
        which counts toward ratio.
        """
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Fetch failed: bot not attached"

        def _flag() -> str:
            row = store.get(infohash)
            if row is None:
                raise LookupError("no longer tracked (done/cancelled?)")
            if row.state != State.WAITING_INDEXER:
                return (
                    f"{(row.source_name or infohash[:10])[:50]} is "
                    f"{row.state.value} — nothing to fetch"
                )
            # Fresh retry window for the direct phase, same as the automatic
            # timeout fallback: an explicit fetch buys full direct retries,
            # not just the remainder of the spent prowlarr window.
            row.force_direct = 1
            row.indexer_first_queried_at = dt.datetime.now(dt.timezone.utc)
            row.indexer_attempts = 0
            store.transition(row, State.QUERYING)
            return (
                f"Fetching VPS1 original for {(row.source_name or infohash[:10])[:50]} "
                "(bypassing Prowlarr; leeches the private swarm)"
            )

        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    outcome = await asyncio.to_thread(_flag)
            else:
                outcome = await asyncio.to_thread(_flag)
        except LookupError as e:
            return f"Fetch failed: {e}"
        except ValueError as e:
            # Illegal transition (row left WAITING_INDEXER concurrently).
            return f"Fetch failed: {e}"
        except Exception as e:  # noqa: BLE001
            return f"Fetch failed: {e}"
        return outcome

    async def _mark_detail_cancelled(
        self, message_id: int, name: str, infohash: str
    ) -> None:
        """Edit a forgotten row's detail card to CANCELLED (best-effort).

        The DB row is already deleted, so this uses the snapshotted message
        id. Never raises: a deleted card or a down Bot API must not fail
        the cancel itself.
        """
        bot = getattr(self, "_bot", None)
        if bot is None or not message_id:
            return
        shown = (name or infohash[:10])[:100]
        text = (
            f"🚫 CANCELLED `{_esc(shown)}`\n"
            f"`{(infohash or '').lower()}`\n"
            "✗ Cancelled by operator (removed + ignored)"
        )
        try:
            await bot.edit_message_text(
                text, chat_id=self._cfg.chat_id, message_id=message_id,
                parse_mode=ParseMode.MARKDOWN,
            )
        except TelegramError as e:
            if _is_parse_error(e):
                try:
                    await bot.edit_message_text(
                        text, chat_id=self._cfg.chat_id, message_id=message_id,
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
        # Stale-card net: re-queue detail updates whose card drifted from
        # the row (edits lost to flood bans, restarts mid-edit). Steady
        # state is silent — only drift enqueues, with live progress, and
        # the worker batch coalesces duplicates.
        try:
            _sent = self._sent_state_map()
            for _ts, _prog in items:
                try:
                    _h = _ts.source_infohash or ""
                    if not _h:
                        continue
                    if _sent.get(_h) != _ts.state.value:
                        self._enqueue_detail(_h, _prog)
                except Exception:
                    continue
        except Exception:
            pass
        # Deferral notes for waiting watch rows (e.g. "Waiting turn ·
        # <domain> copy first") so the list explains itself. Best-effort:
        # never break the refresh over a note.
        notes: dict[str, str] = {}
        try:
            _wait_note = getattr(getattr(self, "_coord", None), "_watch_wait_note", None)
            if callable(_wait_note):
                for _ts, _ in items:
                    try:
                        if _ts.state in (State.NEW, State.WAITING_DISK):
                            _n = _wait_note(_ts) or ""
                            if _n:
                                notes[_ts.source_infohash or ""] = _n
                    except Exception:
                        continue
        except Exception:
            notes = {}
        text, cur_page, total_pages = render_active(
            items, page=self._current_page, page_size=self._cfg.page_size,
            notes=notes,
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
                if _is_parse_error(e):
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
                if _is_parse_error(e):
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
            # Permanent failures (no rights, bad request) disable further
            # pin attempts; transient rate-limit/network errors keep the
            # setting — the next refresh retries.
            msg = str(e).lower()
            transient = isinstance(e, (RetryAfter, TimedOut, NetworkError))
            transient = transient or "retry after" in msg or "flood" in msg or "timeout" in msg
            if transient:
                log.warning("pin deferred (transient %s); will retry", e)
                return
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