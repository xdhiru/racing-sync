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
import hashlib
import logging
import re
import secrets
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


def _tg_pending_owner_ok(pending: dict, user_id: object) -> bool:
    """A tap belongs to the pending flow when owners match (or unknown).

    The pending records the commanding user at arm time; a different
    known tapper is rejected so one operator cannot forge taps into
    another's flow. Unknown either side fails open (single-operator
    chats, channel posts) — the seq token remains the binding there.
    """
    try:
        if not isinstance(pending, dict):
            return True
        owner = pending.get("user_id")
        if not owner or user_id is None:
            return True
        return str(owner) == str(user_id)
    except Exception:
        return False


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


def _short_tracker_label(domain: str) -> str:
    """Compress a tracker domain to its short label for the active list.

    Generic rule only (no tracker names hardcoded — just infrastructure
    words like ``tracker``/``www``): ``somewhere.example`` style hosts
    collapse to their registrable label, e.g. ``tracker.example.com``
    -> ``example`` and ``nyaa.tracker.wf`` -> ``nyaa``. Multi-part
    public suffixes (e.g. ``example.co.uk``) resolve to the registrable
    label. Used for both the title suffix and the wait-turn note's
    embedded domain so the two stay consistent.
    """
    if not domain:
        return ""
    try:
        d = (domain or "").strip().lower().strip(".")
        if ":" in d and not d.startswith("["):
            d = d.split(":")[0]
        parts = [p for p in d.split(".") if p]
        if not parts:
            return ""
        # Drop generic infrastructure labels (never actual tracker names).
        _generic = {"tracker", "trackers", "www", "www2", "api", "announce",
                    "bt", "ipv6"}
        kept = [p for p in parts if p not in _generic]
        if not kept:
            kept = parts
        if len(kept) == 1:
            return kept[0]
        if len(kept) == 2:
            # Possible ccTLD pair (example.co.uk already collapsed to 2
            # only when nothing was dropped) — first label is the owner.
            return kept[0]
        # 3+ labels left: generic ccTLD guard (host.example.co.uk).
        if len(kept[-1]) == 2 and len(kept[-2]) <= 3:
            return kept[-3]
        return kept[0]
    except Exception:
        return ""


def _size_compact(n: int) -> str:
    """Compact size for the active list: ``5.7 GB`` -> ``5.7G``."""
    try:
        s = _bytes_human(n)
    except Exception:
        return ""
    for long, short in ((" PB", "P"), (" TB", "T"), (" GB", "G"),
                        (" MB", "M"), (" KB", "K")):
        if s.endswith(long):
            return s[: -len(long)] + short
    return s


def _compact_wait_note(note: str) -> str | None:
    """Compact a watch-deferral note for the active list, or None.

    - ``Waiting for preferred copy · 1800s left`` -> ``Wait pref-copy 30m``
    - ``Waiting turn · <domain> copy first`` -> ``Wait <short> first``
      (``<short>`` via :func:`_short_tracker_label` when it looks like a
      domain; plain words like ``public``/``tracker``/``sibling`` kept).
    Returns None when the note is unrecognized (caller falls back to raw).
    """
    if not note:
        return None
    try:
        if note.startswith("Waiting for preferred copy"):
            m = re.search(r"(\d+)\s*s\s*left", note)
            if m:
                secs = int(m.group(1))
                mins = max(1, round(secs / 60))
                return f"Wait pref-copy {mins}m"
            return "Wait pref-copy"
        if note.startswith("Waiting turn"):
            m = re.search(r"Waiting turn\s*[·?]+\s*(.+?)\s*copy first", note)
            if not m:
                m = re.search(r"Waiting turn\s*[?]+\s*(.+?)\s*copy first", note)
            if m:
                who = (m.group(1) or "").strip()
                if "." in who:
                    who = _short_tracker_label(who) or who
                return f"Wait {who} first" if who else "Wait turn"
            return "Wait turn"
    except Exception:
        return None
    return None


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


def _safe_display_name(name: str) -> str:
    """Operator-controlled torrent name made safe for a Markdown code span.

    Backticks would close the span early (then `_`/`[` outside it break
    parsing or inject links); newlines would split the message. Replace
    backticks with a quote and flatten newlines — matches the renderer
    convention so headings and questions display identically.
    """
    return ((name or "").replace("`", "'").replace("\n", " ")
            .replace("\r", " ").rstrip("\\"))


def _row_text_bits(ts: TorrentState) -> tuple[str, str, str]:
    """Sanitized (name, human size, lowercase hash) shared by both renderers."""
    name = _safe_display_name(ts.source_name or "")
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
    # (detail card keeps the FULL hash; the Active Tasks list carries the
    # short hash inside its /cancel_ · /fetch_ · /prefer_ commands).
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

#: `/prefer_<hex>` — same shape: start a grace-held watch row's SSD
#: download now instead of waiting out its preferred-copy grace.
PREFER_CMD_RE = re.compile(r"^/prefer_([0-9a-fA-F]+)(?:@[\w_]+)?\b")


def _cancel_command(infohash: str) -> str:
    """Copy-pasteable cancel command for one task (short hash)."""
    return f"/cancel_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _fetch_command(infohash: str) -> str:
    """Copy-pasteable fetch command for one task (short hash)."""
    return f"/fetch_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _prefer_command(infohash: str) -> str:
    """Copy-pasteable prefer command for one task (short hash)."""
    return f"/prefer_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


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


def _active_group_key(name: str, size_bytes: object) -> tuple[str, int]:
    """Grouping key for the active-tasks list: normalized name + size.

    Same identity the watch election uses (one file, any tracker), so
    copies of a release render under one heading. Fail-open: anything
    unparsable groups by raw name.
    """
    try:
        from .coordinator_content import normalize_content_name
        norm = normalize_content_name(name or "")
    except Exception:
        norm = (name or "").strip().lower()
    try:
        size = int(size_bytes or 0)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        size = 0
    return (norm or (name or "").strip().lower(), size)


def _active_note(ts: TorrentState, notes: dict[str, str] | None) -> str:
    """One-line extra for a row (e.g. why a NEW row is waiting)."""
    try:
        return (notes or {}).get(ts.source_infohash or "") or ""
    except Exception:
        return ""


def _active_state_text(ts: TorrentState, progress: float | None, note: str) -> str:
    """One-line stage text for an active-tasks tracker row."""
    _bd = _batch_display(ts)
    if note and ts.state in (State.NEW, State.WAITING_DISK):
        _compact = _compact_wait_note(note)
        state_text = f"⏳ {_compact}" if _compact else f"⏳ {_esc(note)}"
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
        # A same-content deferral note (waiting on the in-flight copy)
        # replaces the stale miss count while it holds.
        state_text = (f"⏳ {_esc(note)}" if note
                      else f"⏳ Wait indexer miss #{ts.indexer_attempts}")
    elif ts.state == State.WAITING_DISK:
        state_text = "⏳ Wait SSD space"
    elif ts.state == State.DONE:
        state_text = "✅ Done"
    elif ts.state == State.FAILED:
        state_text = "❌ Failed"
    else:
        state_text = f"🆕 {ts.state.value.capitalize()}"

    if _bd and ts.state != State.MOVING:
        state_text += f" · {_bd}"
    return state_text


def _group_active_items(
    active: list[tuple[TorrentState, float | None]],
) -> list[tuple[tuple[str, int], list[tuple[TorrentState, float | None]]]]:
    """Group same-file copies (election identity) in first-seen order.

    Returns ``(key, members)`` pairs — the key feeds the stable group id
    used by group commands and pick-button callbacks.
    """
    by_key: dict[tuple[str, int], list[tuple[TorrentState, float | None]]] = {}
    order: list[tuple[str, int]] = []
    for _ts, _progress in active or []:
        try:
            _name0, _, _ = _row_text_bits(_ts)
        except Exception:
            _name0 = getattr(_ts, "source_name", "") or ""
        _key = _active_group_key(
            _name0, getattr(_ts, "total_bytes", 0))
        if _key not in by_key:
            by_key[_key] = []
            order.append(_key)
        by_key[_key].append((_ts, _progress))
    return [(_k, by_key[_k]) for _k in order]


def _live_group_by_gid(
    rows: list[TorrentState],
    gid: str,
) -> tuple[tuple[str, int], list[TorrentState]] | None:
    """Find the live group matching a group id (first match wins).

    Keys derive exactly like the renderer (`_row_text_bits` name +
    size), so the id a command was displayed with resolves back.
    """
    try:
        gid = (gid or "").strip().lower()
        if not gid:
            return None
        for (_key, _members) in _group_active_items(
                [(_r, None) for _r in rows or []]):
            if _key and _group_gid(_key) == gid:
                return _key, [_ts for (_ts, _) in _members]
        return None
    except Exception:
        return None


def _member_hash(ts: TorrentState) -> str:
    """Full lowercase infohash of a row, "" when unusable."""
    try:
        _h = _row_text_bits(ts)[2]
    except Exception:
        return ""
    _h = (_h or "").strip().lower()
    if len(_h) != 40 or any(_c not in "0123456789abcdef" for _c in _h):
        return ""
    return _h


def _member_domain(ts: TorrentState) -> str:
    """Full tracker host for labels, "" when unknown."""
    try:
        return (_tracker_domain(ts.source_announce_url)
                or _tracker_domain(ts.source_tracker) or "")
    except Exception:
        return ""


def _is_grace_note_for_prefer(note: object) -> bool:
    """True when a wait note marks a prefer-eligible NEW row."""
    try:
        return bool(note) and str(note).startswith("Waiting for preferred copy")
    except Exception:
        return False


def _inflight_note_for(ts: TorrentState, leader: object) -> str:
    """Deferral note for a row waiting on an in-flight same-content row.

    ``Waiting for <label> copy · <stage>`` (short tracker label, same
    convention as the wait notes) or "" when there is no usable leader.
    Strict shape validation: coordinator test doubles answer truthy to
    everything, and must never produce notes.
    """
    try:
        if leader is None:
            return ""
        if not isinstance(getattr(leader, "state", None), State):
            return ""
        _lh = getattr(leader, "source_infohash", None)
        if not isinstance(_lh, str) or not _lh.strip():
            return ""
        try:
            _dom = _member_domain(leader)  # type: ignore[arg-type]
        except Exception:
            _dom = ""
        try:
            _short = _short_tracker_label(_dom) or _lh.strip().lower()[:10]
        except Exception:
            _short = _lh.strip().lower()[:10]
        return f"Waiting for {_short} copy · {leader.state.value}"
    except Exception:
        return ""


def _group_gid(key: tuple[str, int]) -> str:
    """Stable 6-hex id for an active-tasks group (content token).

    Only used to resolve group commands typed from older messages;
    the list itself addresses groups by position number. Collisions
    are accepted (16M space, a handful of groups) — resolution takes
    the first live match.
    """
    try:
        raw = f"{key[0]}|{int(key[1])}"
    except Exception:
        try:
            raw = f"{key[0]}|0"
        except Exception:
            return ""
    try:
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6]
    except Exception:
        return ""


def _pick_member_label(ts: TorrentState, short_fallback: str = "") -> str:
    """Short tracker label for a member-choice button."""
    try:
        return (_short_tracker_label(_member_domain(ts))
                or short_fallback or "?")
    except Exception:
        return short_fallback or "?"


def pick_snapshot(
    members: list[tuple[TorrentState, float | None]],
    cmd: str,
    notes: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """Frozen member list for a group command: ``[(hash, label)]``.

    ``cmd`` cancel snapshots every member; fetch/prefer snapshot only
    eligible members. Labels are short tracker names (hash-qualified on
    repeats). The snapshot is taken once, at command-tap time — later
    buttons address members by index, so list renumbering mid-flow
    cannot misroute. Fail-open: [].
    """
    try:
        if cmd not in ("cancel", "fetch", "prefer"):
            return []
        rows: list[tuple[TorrentState, str]] = []
        for (ts, _progress) in members or []:
            _h = _member_hash(ts)
            if not _h:
                continue
            if cmd == "fetch" and ts.state != State.WAITING_INDEXER:
                continue
            if cmd == "prefer" and not (
                    ts.state == State.NEW and _is_grace_note_for_prefer(
                        _active_note(ts, notes))):
                continue
            rows.append((ts, _h))
        if not rows:
            return []
        out: list[tuple[str, str]] = []
        seen: dict[str, int] = {}
        for (ts, _h) in rows:
            _base = _pick_member_label(ts, _h[:10])
            _n = seen.get(_base, 0)
            seen[_base] = _n + 1
            _label = _base if _n == 0 else f"{_base} {_h[:6]}"
            out.append((_h, _label))
        return out
    except Exception:
        return []


#: Pending group-action lifetime: buttons referencing regrouped lists go
#: stale, so picks expire fast (re-tap the command for a fresh set).
_PENDING_TTL_S = 300.0


def render_pending_question(pending: dict | None) -> str:
    """Question section appended below the footer while a pick is pending.

    Pure (testable): ``pending`` carries kind/title/scope (+ frozen
    members/hashes) as set by the pick step. "" when nothing is pending.
    The title lets the user verify the locked group before tapping.
    """
    try:
        if not isinstance(pending, dict):
            return ""
        kind = str(pending.get("kind") or "")
        title = _safe_display_name(str(pending.get("title") or "")[:80])
        scope = _esc(str(pending.get("scope") or ""))
        try:
            _size = _size_compact(pending.get("size"))
        except Exception:
            _size = ""
        _tspec = f"`{title}`" + (f" · {_size}" if _size else "")
        if kind == "pick":
            cmd = str(pending.get("cmd") or "")
            verb = {"cancel": "Cancel", "fetch": "Fetch original for",
                    "prefer": "Prefer"}.get(cmd, "Act on")
            if scope == "all":
                what = "every copy"
            elif scope:
                what = f"the {scope} copy"
            else:
                what = "which copy"
            return (f"{verb} {_tspec} — {what}?\n"
                    f"Pick below (tracker names).")
        if kind == "keepq":
            return (f"Cancel {_tspec}{(' · ' + scope) if scope else ''} — "
                    f"keep downloaded files?")
        return ""
    except Exception:
        return ""


def render_active(
    active: list[tuple[TorrentState, float | None]],
    page: int = 0,
    page_size: int = 5,
    notes: dict[str, str] | None = None,
    footer: str = "",
) -> tuple[str, int, int]:
    """Render paginated active tasks, grouped by file for mobile width.

    Same-content copies (one release, many trackers) share one numbered
    heading ``N. `Name` · size``; each tracker gets a display-only line
    ``▸ <domain> <stage>``. Commands address the group by its number
    (``/cancel_3`` …) and open member-choice buttons below the list —
    the number is resolved once, at tap time, into a frozen member
    snapshot, so later renumbering cannot misroute the flow. Cancel
    always ends at a keep/delete question; fetch/prefer appear only
    while a member qualifies. Detail cards keep per-torrent text
    commands (full-hash cancel). Pages count groups; numbers are global
    across pages.

    Compact mobile layout (detail cards stay fully detailed). `notes`
    maps source_infohash -> one-line extra shown on that tracker's line.

    `footer` is an optional trailing line (e.g. storage stats) appended
    directly after (single newline, no blank gap); "" disables it.
    """
    try:
        page_size = int(page_size)
    except (TypeError, ValueError):
        page_size = 5
    page_size = max(1, min(page_size, 50))
    total_items = len(active)

    if not active:
        text = "📌 *Active Tasks*\n\n_No active tasks in flight._"
        if footer:
            text += f"\n{footer}"
        return (
            _safe_truncate_markdown(text),
            0,
            1,
        )

    # Group same-file copies (election identity) in first-seen order;
    # numbering below is global across pages.
    groups = _group_active_items(active)
    total_groups = len(groups)
    total_pages = max(1, (total_groups + page_size - 1) // page_size)
    cur_page = max(0, min(page, total_pages - 1))

    start_grp = cur_page * page_size
    end_grp = min(start_grp + page_size, total_groups)
    page_groups = groups[start_grp:end_grp]

    if total_groups == total_items:
        lines = [
            f"📌 *Active Tasks ({total_items})* · *Page {cur_page + 1}/{total_pages}*",
            "",
        ]
    else:
        lines = [
            f"📌 *Active Tasks ({total_items} copies · {total_groups} titles)*"
            f" · *Page {cur_page + 1}/{total_pages}*",
            "",
        ]

    for gi, (_gkey, members) in enumerate(page_groups):
        group_num = start_grp + gi + 1
        lead, _ = members[0]
        name, _size_long, _lead_hash = _row_text_bits(lead)
        size = _size_compact(lead.total_bytes)

        # 1. One heading per file (bare title so it wraps less).
        lines.append(f"{group_num}. `{name}` · {size}")

        # 2. Display-only tracker lines, no indent: the ▸ glyph plus the
        # blank line between groups already carries the hierarchy, and
        # leading spaces only push long statuses into a wrap.
        _has_fetch = False
        _has_prefer = False
        for (ts, progress) in members:
            domain_full = (_tracker_domain(ts.source_announce_url)
                           or _tracker_domain(ts.source_tracker))
            _note = _active_note(ts, notes)
            state_text = _active_state_text(ts, progress, _note)
            if domain_full:
                lines.append(f"▸ {_esc(domain_full)} {state_text}")
            else:
                lines.append(f"▸ {state_text}")
            if ts.state == State.WAITING_INDEXER:
                _has_fetch = True
            if (ts.state == State.NEW
                    and _is_grace_note_for_prefer(_note)):
                _has_prefer = True

        # 3. Short group commands on ONE line with no indent (the `/`
        # prefix marks them; every column counts against the wrap limit).
        # Positional group number — resolved once at tap time into a
        # frozen snapshot, so later renumbering cannot misroute.
        # Cancel always; fetch/prefer only when a member qualifies
        # right now.
        _cmds = [f"/cancel_{group_num}"]
        if _has_fetch:
            _cmds.append(f"/fetch_{group_num}")
        if _has_prefer:
            _cmds.append(f"/prefer_{group_num}")
        lines.append(" ".join(_esc(_c) for _c in _cmds))
        lines.append("")

    text = "\n".join(lines).strip()
    if footer:
        text += f"\n{footer}"
    rendered = _safe_truncate_markdown(text.strip())
    return rendered, cur_page, total_pages


#: How long a VPS1 free-space probe stays valid for the active-tasks
#: footer. Refresh ticks every status_update_interval (45s default); a
#: probe per tick would chatter the SFTP channel for a footer line.
_VPS1_FREE_TTL_S = 300.0


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
        # Cached "last active-tasks (page, total_pages, text, buttons)" so
        # we skip identical edits.
        self._last_active_cache: tuple | None = None
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
        # VPS1 free-space probe cache (monotonic timestamp, bytes|None):
        # the footer refreshes every status tick but the SFTP probe is
        # reused for _VPS1_FREE_TTL_S so we don't chatter the channel.
        self._vps1_free_cache: tuple[float, int | None] | None = None
        # Pending group action (member pick or keep/delete question) for
        # the two-step flows; armed by group commands, expires quickly.
        self._pending_pick: dict | None = None

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
            # Debounced "online" ping: a crash-loop restart must not spam
            # the chat — skip when the last ping is under 10 minutes old.
            _ping_at = None
            try:
                _get_meta = getattr(self._store, "get_meta", None)
                if callable(_get_meta):
                    _ping_at = await asyncio.to_thread(
                        _get_meta, "telegram_online_ping_at")
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
                            _set_meta, "telegram_online_ping_at",
                            str(time.time()))
                except Exception:
                    pass
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
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        raise
                    except Exception:
                        continue
        except (asyncio.TimeoutError, asyncio.CancelledError):
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
        # Grace-held rows are actionable: offer the override inline so the
        # operator doesn't have to remember the command shape.
        try:
            if note.startswith("Waiting for preferred copy"):
                text += f"\nPrefer this copy now: `{_prefer_command(infohash)}`"
        except Exception:
            pass
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
        extra_rows: list[list[tuple[str, str]]] | tuple = (),
    ) -> InlineKeyboardMarkup | None:
        """Pagination nav buttons plus pending-action rows.

        Extra rows are ``(label, callback_data)`` pairs (member-choice
        pick buttons or keep Yes/No), already chunked by the caller —
        stale/oversize entries are skipped defensively. Cancel/keep stay
        as chat commands (destructive = keep the copy-paste friction).
        """
        if total_pages <= 1:
            buttons = [
                [InlineKeyboardButton("🔄 Refresh", callback_data="page:refresh")]
            ]
        else:
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
        try:
            for _row in extra_rows or ():
                _btns = []
                for (_label, _data) in _row or ():
                    if not _label or not _data or len(_data) > 64:
                        continue
                    _btns.append(InlineKeyboardButton(
                        str(_label)[:60], callback_data=_data))
                if _btns:
                    buttons.append(_btns)
        except Exception:
            pass
        return InlineKeyboardMarkup(buttons)

    # ---- pending group actions (two-step pickers) ----

    def _pending_live(self) -> dict | None:
        """Live pending pick, else None (expired picks are purged)."""
        try:
            _p = getattr(self, "_pending_pick", None)
            if not isinstance(_p, dict):
                return None
            try:
                _exp = float(_p.get("expires", 0) or 0)
            except (TypeError, ValueError):
                return None
            if _exp and time.monotonic() > _exp:
                try:
                    self._pending_pick = None
                except Exception:
                    pass
                return None
            if not _p.get("kind") or not isinstance(
                    _p.get("members"), list):
                # keepq carries hashes instead of members.
                if not (_p.get("kind") == "keepq"
                        and isinstance(_p.get("hashes"), list)):
                    return None
            return _p
        except Exception:
            return None

    def _next_seq(self) -> str:
        """Next pending-flow sequence token (stale-tap guard).

        Unpredictable (not 1,2,3…): callback data is only obscure, so a
        sequential id lets anyone in the authorized chat forge taps into
        another operator's pending flow. Single pending per bot is
        intentional (single-operator chat); the token still binds each
        question to its own buttons until it expires.
        """
        try:
            return secrets.token_hex(8)
        except Exception:
            raise RuntimeError("no entropy for pending-flow token")

    def _set_pending_pick(self, cmd: str, title: str, size_bytes: object,
                           members: list[tuple[str, str]],
                           user_id: object = None) -> dict:
        """Arm a member-choice pick; members are frozen (hash, label)."""
        _p = {
            "kind": "pick", "seq": self._next_seq(), "cmd": cmd,
            "title": (title or "")[:80], "size": size_bytes,
            "members": [(h, label) for (h, label) in members or []],
            "user_id": str(user_id) if user_id is not None else None,
            "expires": time.monotonic() + _PENDING_TTL_S,
        }
        try:
            self._pending_pick = _p
        except Exception:
            pass
        try:
            self._last_active_cache = None
        except Exception:
            pass
        return _p

    def _set_pending_keepq(self, title: str, scope: str,
                            hashes: list[str], size_bytes: object = None,
                            user_id: object = None) -> dict:
        """Arm the keep/delete question over frozen hashes."""
        _p = {
            "kind": "keepq", "seq": self._next_seq(),
            "title": (title or "")[:80], "scope": scope or "",
            "size": size_bytes,
            "hashes": [h for h in hashes or [] if h],
            "user_id": str(user_id) if user_id is not None else None,
            "expires": time.monotonic() + _PENDING_TTL_S,
        }
        try:
            self._pending_pick = _p
        except Exception:
            pass
        try:
            self._last_active_cache = None
        except Exception:
            pass
        return _p

    def _pending_section(self) -> tuple[str, list[list[tuple[str, str]]]]:
        """Question text + keyboard rows for the live pending pick.

        Renders purely from the frozen snapshot — later renumbering or
        regrouping cannot shift what the buttons mean. Stale member
        hashes fail safe at execution (liveness re-checked).
        """
        try:
            _p = self._pending_live()
            if _p is None:
                return "", []
            _seq = str(_p.get("seq") or "")
            if _p.get("kind") == "keepq":
                return (
                    render_pending_question(_p),
                    [[("Keep files", f"keep:{_seq}:yes"),
                      ("Delete files", f"keep:{_seq}:no")],
                     [("Cancel", f"abort:{_seq}")]],
                )
            _btns = []
            _mems = _p.get("members") or []
            if str(_p.get("cmd") or "") == "cancel" and len(_mems) > 1:
                _btns.append((f"All ({len(_mems)})", f"pick:{_seq}:all"))
            for _i, (_h, _label) in enumerate(_mems):
                _btns.append((_label, f"pick:{_seq}:{_i}"))
            _rows = [_btns[i:i + 3] for i in range(0, len(_btns), 3)]
            _rows.append([("Cancel", f"abort:{_seq}")])
            return render_pending_question(_p), _rows
        except Exception:
            return "", []

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
        if not _tg_actor_allowed(self._cfg, user_id):
            log.warning("callback from non-admin user=%s (allowlist set)", user_id)
            try:
                await query.answer("Not authorized for destructive actions",
                                   show_alert=True)
            except Exception:
                pass
            return

        # Throttle callback handling (2s debounce per chat+user, destructive
        # taps bucketed harder below — a per-chat key lets one spammer
        # block pagination for all chats sharing it).
        try:
            _debounce_key = (f"{chat_id}:{user_id}"
                             if chat_id is not None else str(user_id))
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
        if (data.startswith("pick:") or data.startswith("keep:")
                or data.startswith("abort:")):
            await self._on_action_button(query, data)
            return
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
        if len(norm) < 4 and len(norm) != 40:
            # 1-3 char prefixes always match-or-error over a full table
            # scan; group numbers never reach here (digits route first).
            raise LookupError(
                f"/{cmd}_{norm} is too short — send at least 4 hex chars "
                f"or the full 40-char hash"
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
            candidates = [
                ts for ts in rows
                if (ts.source_infohash or "").lower().startswith(norm)
            ]
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"cannot look up {short!r}: {e}") from e
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            raise LookupError(
                f"/{cmd}_{norm} matches {len(candidates)} torrents; "
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
        """Execute `/cancel_` / `/fetch_` / `/prefer_` commands in the chat.

        Group commands (stable content ids from the active list) open
        member-choice buttons; full hashes and legacy hash prefixes act
        directly. Cancel always ends at a keep/delete question — nothing
        is wiped without an explicit choice. Anything else is ignored.
        Only the configured chat/user may send commands.
        """
        try:
            chat = getattr(message, "chat", None)
            chat_id = getattr(chat, "id", None)
            from_user = getattr(message, "from_user", None)
            user_id = getattr(from_user, "id", None)
            cfg_chat = str(self._cfg.chat_id)
            if str(chat_id) != cfg_chat and str(user_id) != cfg_chat:
                return
            # Same debounce as callbacks: a double-sent command must
            # not resolve+act twice (double forget/double prefer).
            # Namespaced apart from callback keys, keyed chat+user so one
            # spammer cannot block other operators.
            try:
                _ckey = (f"cmd:{chat_id}:{user_id}"
                         if chat_id is not None else f"cmd:{user_id}")
            except Exception:
                _ckey = ""
            if _ckey and self._debounced(_ckey, window=2.0):
                return
            text = (
                getattr(message, "text", None)
                or getattr(message, "caption", None)
                or ""
            )
            text = str(text or "").strip()
            m_fetch = FETCH_CMD_RE.match(text)
            m_prefer = PREFER_CMD_RE.match(text)
            m_cancel = CANCEL_CMD_RE.match(text)
            if not m_fetch and not m_prefer and not m_cancel:
                return
            if not _tg_actor_allowed(self._cfg, user_id):
                try:
                    await self._reply("Not authorized for destructive actions.",
                                      reply_to=message)
                except Exception:
                    pass
                return
            if m_fetch:
                await self._start_group_command(
                    "fetch", m_fetch.group(1), message)
                return
            if m_prefer:
                await self._start_group_command(
                    "prefer", m_prefer.group(1), message)
                return
            await self._start_group_command(
                "cancel", m_cancel.group(1), message)
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

    async def _execute_cancel_one(self, infohash: str, *, delete_files: bool) -> str:
        """Forget + ignore one release; files kept iff not `delete_files`."""
        _verb = "Cancelled" if delete_files else "Kept files for"
        _done = "(removed + ignored)" if delete_files else (
            "untracked + ignored, data left in place")
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        try:
            from .forget import forget_torrent
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Action failed: bot not attached"
        dest = getattr(coord, "dest_client", None)
        cfg = getattr(coord, "cfg", None)
        if dest is None or cfg is None:
            return "Action failed: coordinator not ready"
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
                        apply=True, delete_files=delete_files, ignore=True,
                    )
                    try:
                        await coord._ssd_release(
                            result.get("source_infohash") or infohash)
                    except Exception:
                        pass
            else:
                result = await forget_torrent(
                    cfg, dest=dest, store=store, target=infohash,
                    apply=True, delete_files=delete_files, ignore=True,
                )
                try:
                    await coord._ssd_release(
                        result.get("source_infohash") or infohash)
                except Exception:
                    pass
        except LookupError:
            return "Already gone from tracking"
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"
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
            return f"{_verb} {name}{pair_note} with {len(errs)} error(s); check logs"
        return f"{_verb} {name}{pair_note} {_done}"

    async def _execute_snapshot_cancel(self, hashes: list[str], title: str,
                                         *, delete_files: bool) -> str:
        """Forget frozen snapshot hashes (liveness re-checked each)."""
        try:
            live = []
            for h in hashes or []:
                try:
                    row = await asyncio.to_thread(self._store.get, h)
                except Exception:
                    row = None
                if row is not None:
                    live.append(h)
            if not live:
                return "Already gone from tracking"
            outs = []
            for h in live:
                try:
                    outs.append(await self._execute_cancel_one(
                        h, delete_files=delete_files))
                except Exception as e:  # noqa: BLE001
                    outs.append(f"Action failed: {e}")
            if len(outs) == 1:
                return outs[0]
            ok = sum(1 for o in outs
                     if o.startswith(("Cancelled", "Kept files")))
            verb = "Cancelled" if delete_files else "Kept files for"
            suffix = "" if delete_files else " (data left in place)"
            return f"{verb} {title} ({ok}/{len(outs)} copies){suffix}"
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"

    def _live_notes_for(self, members: list) -> dict[str, str]:
        """Wait-note map for prefer eligibility (best-effort, str-only)."""
        notes: dict[str, str] = {}
        try:
            fn = getattr(getattr(self, "_coord", None),
                         "_watch_wait_note", None)
            if not callable(fn):
                return notes
            for m in members or []:
                try:
                    if m.state not in (State.NEW, State.WAITING_DISK):
                        continue
                    n = fn(m)
                    if isinstance(n, str) and n:
                        notes[(m.source_infohash or "")] = n
                except Exception:
                    continue
        except Exception:
            pass
        return notes

    async def _start_single_command(self, kind: str, full_hash: str, message: Any) -> None:
        """Typed full-hash command: keepq for cancel, direct for fetch/prefer."""
        try:
            row = await asyncio.to_thread(self._store.get, full_hash)
        except Exception:
            row = None
        if row is None:
            try:
                await self._reply("Already gone from tracking", reply_to=message)
            except Exception:
                pass
            return
        title = str(getattr(row, "source_name", "") or full_hash[:10])[:60]
        if kind == "cancel":
            self._set_pending_keepq(
                title, "", [full_hash],
                getattr(row, "total_bytes", 0),
                user_id=_tg_actor_id(message))
            try:
                await self._refresh_active_message()
            except Exception:
                pass
            try:
                await self._reply(
                    f"Cancel {title} — keep downloaded files? Choose below.",
                    reply_to=message)
            except Exception:
                pass
            return
        try:
            if kind == "fetch":
                result = await self._fetch_torrent(full_hash)
            else:
                result = await self._prefer_torrent(full_hash)
        except Exception as e:  # noqa: BLE001
            result = f"Action failed: {e}"
        try:
            await self._reply(result[:300], reply_to=message)
        except Exception:
            pass
        try:
            self._last_active_cache = None
            await self._refresh_active_message()
        except Exception:
            pass

    def _group_title(self, members: list) -> str:
        """Display title for a group (lead row's name, truncated)."""
        try:
            if members:
                return _safe_display_name(
                    str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"

    async def _start_group_command(self, kind: str, token: str, message: Any) -> None:
        """Typed command: positional number, gid, or legacy hash prefix.

        Positional numbers (what the list shows) and group ids resolve
        against the LIVE list right now and freeze into a member
        snapshot — everything after (buttons, question, execution)
        references the snapshot, so renumbering mid-flow cannot
        misroute. Full hashes and legacy prefixes act on one row.
        """
        token = (token or "").strip().lower()
        _actor = _tg_actor_id(message)
        if len(token) == 40 and all(
                c in "0123456789abcdef" for c in token):
            await self._start_single_command(kind, token, message)
            return
        try:
            rows = await asyncio.to_thread(self._store.list_active_inflight)
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(f"Action failed: {e}", reply_to=message)
            except Exception:
                pass
            return
        try:
            groups = _group_active_items([(_r, None) for _r in rows or []])
        except Exception:
            groups = []
        found: tuple | None = None
        if token.isdigit():
            try:
                _n = int(token)
            except (TypeError, ValueError):
                _n = 0
            if 1 <= _n <= len(groups):
                _gk, _mem = groups[_n - 1]
                found = (_gk, [t for (t, _) in _mem])
        if found is None:
            # Group id (older messages) or legacy torrent prefix.
            _by_gid = _live_group_by_gid(list(rows or []), token)
            if _by_gid is not None:
                _gk, _mem = _by_gid
                found = (_gk, list(_mem))
        if found is None:
            try:
                target = await asyncio.to_thread(
                    self._resolve_cancel_target, token, cmd=kind)
                full_hash = target.source_infohash
            except LookupError as e:
                try:
                    await self._reply(str(e)[:300], reply_to=message)
                except Exception:
                    pass
                return
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(f"Action failed: {e}", reply_to=message)
                except Exception:
                    pass
                return
            await self._start_single_command(kind, full_hash, message)
            return
        _gkey, members = found
        title = self._group_title(members)
        try:
            _gsize = getattr(members[0], "total_bytes", 0) if members else 0
        except Exception:
            _gsize = 0
        _trip = [(_m, None) for _m in members]
        if kind == "cancel":
            snap = pick_snapshot(_trip, "cancel")
            if len(snap) <= 1:
                _hashes = [h for (h, _) in snap]
                if not _hashes:
                    try:
                        await self._reply("Already gone from tracking",
                                          reply_to=message)
                    except Exception:
                        pass
                    return
                _lbl = snap[0][1]
                self._set_pending_keepq(
                    title, f"{_lbl} copy", _hashes, _gsize,
                    user_id=_actor)
                reply = (f"Cancel {title} — keep downloaded files? "
                         f"Choose below.")
            else:
                self._set_pending_pick("cancel", title, _gsize, snap,
                                       user_id=_actor)
                reply = (f"Cancel {title} ({len(snap)} copies): "
                         f"pick below.")
            try:
                await self._refresh_active_message()
            except Exception:
                pass
            try:
                await self._reply(reply, reply_to=message)
            except Exception:
                pass
            return
        # fetch/prefer: snapshot eligible members into a titled pick —
        # even a single candidate goes through the buttons, so the group
        # name is always on screen (with a Cancel row) before anything
        # acts. Empty set replies inline.
        snap = pick_snapshot(
            _trip, kind, self._live_notes_for(members))
        if not snap:
            try:
                await self._reply(
                    f"Nothing to {kind} right now (states changed).",
                    reply_to=message)
            except Exception:
                pass
            return
        self._set_pending_pick(kind, title, _gsize, snap, user_id=_actor)
        try:
            await self._refresh_active_message()
        except Exception:
            pass
        try:
            await self._reply(
                f"{kind.capitalize()} {title}: pick a copy below.",
                reply_to=message)
        except Exception:
            pass

    async def _on_action_button(self, query: Any, data: str) -> None:
        """Route pick:/keep:/abort: taps (initial ack already sent).

        Buttons address snapshot indices/sequences, never live
        positions or hashes — renumbering mid-flow cannot misroute.
        Short replies go back to the chat; the list refreshes after.
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

        try:
            parts = (data or "").split(":")
            _tapper = _tg_actor_id(query)

            def _owner_ok(p) -> bool:
                if _tg_pending_owner_ok(p or {}, _tapper):
                    return True
                return False

            async def _refuse_foreign() -> None:
                await _say("That picker belongs to another operator — "
                           "tap the command again for your own.")

            if parts[0] == "abort" and len(parts) == 2:
                p = self._pending_live()
                if p is None or str(p.get("seq") or "") != parts[1]:
                    await _say("Expired — tap the command again")
                    return
                if not _owner_ok(p):
                    await _refuse_foreign()
                    return
                try:
                    self._pending_pick = None
                except Exception:
                    pass
                await _say("Picker cancelled — nothing acted on.")
                await _refresh()
                return
            if parts[0] == "keep" and len(parts) == 3:
                _, seq, which = parts
                p = self._pending_live()
                if (p is None or p.get("kind") != "keepq"
                        or str(p.get("seq") or "") != seq):
                    await _say("Expired — tap the command again")
                    return
                if not _owner_ok(p):
                    await _refuse_foreign()
                    return
                _hashes = [h for h in (p.get("hashes") or []) if h]
                _title = str(p.get("title") or "")
                try:
                    self._pending_pick = None
                except Exception:
                    pass
                result = await self._execute_snapshot_cancel(
                    _hashes, _title, delete_files=(which == "no"))
                await _say(result)
                await _refresh()
                return
            if parts[0] == "pick" and len(parts) == 3:
                _, seq, idx = parts
                p = self._pending_live()
                if (p is None or p.get("kind") != "pick"
                        or str(p.get("seq") or "") != seq):
                    await _say("Expired — tap the command again")
                    return
                if not _owner_ok(p):
                    await _refuse_foreign()
                    return
                _mems = list(p.get("members") or [])
                _cmd = str(p.get("cmd") or "")
                _title = str(p.get("title") or "")
                if idx == "all" and _cmd == "cancel" and len(_mems) > 1:
                    self._set_pending_keepq(
                        _title, f"all {len(_mems)} copies",
                        [h for (h, _) in _mems], p.get("size"),
                        user_id=_tapper)
                    await _refresh()
                    await _say("Keep downloaded files or delete them? Choose below.")
                    return
                try:
                    _i = int(idx)
                    _h, _lbl = _mems[_i]
                except (TypeError, ValueError, IndexError):
                    await _say("Expired — tap the command again")
                    return
                except Exception:
                    await _say("Expired — tap the command again")
                    return
                if _cmd == "cancel":
                    self._set_pending_keepq(
                        _title, f"{_lbl} copy", [_h], p.get("size"),
                        user_id=_tapper)
                    await _refresh()
                    await _say("Keep downloaded files or delete them? Choose below.")
                    return
                try:
                    self._pending_pick = None
                except Exception:
                    pass
                try:
                    if _cmd == "fetch":
                        result = await self._fetch_torrent(_h)
                    else:
                        result = await self._prefer_torrent(_h)
                except Exception as e:  # noqa: BLE001
                    result = f"Action failed: {e}"
                await _say(result)
                await _refresh()
                return
            await _say("Stale button — refresh the list")
        except Exception as e:  # noqa: BLE001
            log.debug("action button handling failed: %s", e)

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

    async def _prefer_torrent(self, short: str) -> str:
        """Start a grace-held watch row's SSD download now (operator override).

        The coordinator validates (NEW watch row, rank-2, in grace, no
        owner), records a one-shot exemption and returns the row to wake;
        the worker is spawned here (running loop) with next-tick pickup
        as the backstop. Never raises: all outcomes arrive as reply text.
        """
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        if coord is None:
            return "Prefer failed: bot not attached"
        prefer = getattr(coord, "prefer_grace_row", None)
        if not callable(prefer):
            return "Prefer failed: coordinator too old"
        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    row, outcome = await asyncio.to_thread(prefer, short)
            else:
                row, outcome = await asyncio.to_thread(prefer, short)
        except LookupError as e:
            return f"Prefer failed: {e}"
        except Exception as e:  # noqa: BLE001
            return f"Prefer failed: {e}"
        if row is not None:
            try:
                spawn = getattr(coord, "_spawn_worker", None)
                if callable(spawn):
                    spawn(row)
            except Exception as e:  # noqa: BLE001
                log.debug("prefer wake failed (%s); next tick picks it up", e)
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
            # VPS2 SSD free: cheap local syscall, probed every refresh.
            ssd_free: int | None = None
            try:
                from .rclone_ops import ssd_free_bytes as _ssd_free
                ssd_free = self._valid_byte_count(
                    await asyncio.to_thread(_ssd_free, cfg))
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
                if (_cached is not None and len(_cached) == 2
                        and _now - float(_cached[0]) < _VPS1_FREE_TTL_S):
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
                    elif (_ts.state == State.WAITING_INDEXER
                            and callable(_inflight_of)):
                        _n = _inflight_note_for(
                            _ts, _inflight_of(_ts)) or ""
                        if _n:
                            notes[_ts.source_infohash or ""] = _n
                except Exception:
                    continue
        except Exception:
            notes = {}
        footer = await self._active_footer()
        text, cur_page, total_pages = render_active(
            items, page=self._current_page, page_size=self._cfg.page_size,
            notes=notes, footer=footer,
        )
        if len(text) > 4096:
            text = _safe_truncate_markdown(text)
        self._current_page = cur_page
        # Pending two-step pick (member choice or keep/delete question):
        # question section under the footer, choice rows in the keyboard.
        try:
            qtext, qrows = self._pending_section()
        except Exception:
            qtext, qrows = "", []
        if qtext:
            text += f"\n{_esc('_' * 35)}\n{qtext}"
        keyboard = self._build_keyboard(cur_page, total_pages, qrows)

        # Skip the API call if page, total_pages, text and pending rows
        # are identical — unless a keep-at-bottom repost is due (position
        # refreshes even when the content is unchanged). Button digests
        # stay in the key: a button-only change must re-send, otherwise
        # taps desync from the frozen snapshot.
        try:
            cache_key = (cur_page, total_pages, text,
                         tuple(_d for _r in qrows for _, _d in _r))
        except Exception:
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