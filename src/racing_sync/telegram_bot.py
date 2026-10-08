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
import base64
import datetime as dt
import hashlib
import logging
import re
import time
from typing import Any
from urllib.parse import urlsplit

from telegram import Bot, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup
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
            return ("telegram admin_user_ids is empty: anyone in the "
                    "authorized chat may tap destructive buttons "
                    "(cancel/inject). Set admin_user_ids to restrict.")
    except Exception:
        pass
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
    # short hash inside its /cancel_ · /now_ commands).
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
        lines.append(f"Start SSD download now: `{_now_command(full_hash)}`")
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

    try:
        _held = bool(getattr(ts, "skipped", 0))
    except Exception:
        _held = False
    if _held:
        lines.append(f"⏭ Skipped — resume with `{_resume_command(full_hash)}`")

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

#: `/fetch_<hex>` — same shape: use the starting torrent for the SSD
#: download instead of waiting for Prowlarr (WAITING_INDEXER rows use
#: the VPS1 original; NEW grace-held rows use their starting bytes).
#: (Legacy alias: canonical verb is now `/now_`.)
FETCH_CMD_RE = re.compile(r"^/fetch_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/prefer_<hex>` — same shape: start a grace-held row's SSD
#: download now instead of waiting out its preferred-copy grace.
#: (Legacy alias: canonical verb is now `/now_`.)
PREFER_CMD_RE = re.compile(r"^/prefer_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/now_<hex>` — same shape, replaces `/fetch_` + `/prefer_` with one
#: state-dependent verb: WAITING_INDEXER rows use the starting torrent
#: (VPS1 original) instead of waiting out Prowlarr; NEW grace-held
#: rows (watch or racing) start their SSD download at once.
NOW_CMD_RE = re.compile(r"^/now_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/skip_<hex>` — same shape: hold a tracked release (whole group):
#: no workers, no Prowlarr queries, no downloads, no moves until
#: `/unskip_`. State is kept; `/add` / watch-dir re-drop resumes it.
SKIP_CMD_RE = re.compile(r"^/skip_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/unskip_<hex>` — same shape: resume a held release immediately.
#: (Legacy alias: canonical verb is now `/resume_`.)
UNSKIP_CMD_RE = re.compile(r"^/unskip_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/resume_<hex>` — same shape, replaces `/unskip_` + `/unignore_`
#: with one verb: held rows resume, cancelled (ignored) hashes are
#: lifted so they track again. Full 40-char hash also reaches ignored
#: entries (invisible in the list, like `/unignore_` before it).
RESUME_CMD_RE = re.compile(r"^/resume_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/unignore_<full-hash>` — legacy alias of `/resume_`: drop a
#: cancelled release from the ignore list (plus its forget tombstone)
#: so it can be tracked again. Full 40-char hash only: ignore entries
#: are invisible in the task list, so prefixes cannot be disambiguated
#: there.
UNIGNORE_CMD_RE = re.compile(r"^/unignore_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/unignore <full-hash>` — manual form (no underscore), same effect.
UNIGNORE_HASH_RE = re.compile(r"^/unignore\s+([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/injectfuse_<n|hex>` — list shortcut: group number or tracked
#: hash/prefix for manual fuse seeding (operator moved the bytes).
INJECTFUSE_CMD_RE = re.compile(r"^/injectfuse_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/act_<n|hex>` — action sheet for a content group: one entry point
#: showing only the currently eligible actions (hash-direct buttons).
#: Group number, tracked hash/prefix, or group id, like the other
#: group commands.
ACT_CMD_RE = re.compile(r"^/act_([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/injectfuse <full-hash>` — manual form: any VPS1 infohash, tracked
#: or not yet tracked by the app.
INJECTFUSE_HASH_RE = re.compile(r"^/injectfuse\s+([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/add` — ingest the .torrent file in the replied-to message (or in
#: the same message when sent with /add as caption) as a Telegram
#: origin drop, processed like a watch-dir file. Optional `@bot` suffix,
#: extra trailing text ignored.
ADD_CMD_RE = re.compile(r"^/add(?:@[\w_]+)?\b")


#: `/cancel_match <text>` — bulk cancel: every live group whose title
#: contains `<text>` (case-insensitive) is cancelled together after one
#: remember + one keep/delete question. Optional `@bot` suffix for group
#: chats; the text runs to end of line.
CANCEL_MATCH_RE = re.compile(r"^/cancel_match(?:@[\w_]+)?(?:\s+(.+?))?\s*$")

#: Shortest substring worth matching: 1 char matches nearly everything.
_MATCH_MIN_CHARS = 2
#: Longest query (UTF-8 bytes) whose base64url token still fits the
#: 64-byte button budget inside `keep:match:<tok>:1:yes`.
_MATCH_MAX_BYTES = 35
#: Valid token chars (base64url, padding stripped at encode time).
_MATCH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,47}$")


def _match_token(query: str) -> str:
    """URL-safe token for a match query, "" when it cannot fit a button."""
    try:
        raw = (query or "").strip().encode("utf-8")
        if not raw or len(raw) > _MATCH_MAX_BYTES:
            return ""
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    except Exception:
        return ""


def _match_query(token: str) -> str:
    """Decode a match token back to the query, "" when malformed."""
    try:
        tok = (token or "").strip()
        if not _MATCH_TOKEN_RE.match(tok):
            return ""
        padded = tok + "=" * (-len(tok) % 4)
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except Exception:
        return ""


def _cancel_command(infohash: str) -> str:
    """Copy-pasteable cancel command for one task (short hash)."""
    return f"/cancel_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _fetch_command(infohash: str) -> str:
    """Copy-pasteable fetch command for one task (short hash)."""
    return f"/fetch_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _prefer_command(infohash: str) -> str:
    """Copy-pasteable prefer command for one task (short hash).

    Legacy alias: canonical verb is now `/now_` (see _now_command).
    """
    return f"/prefer_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _now_command(infohash: str) -> str:
    """Copy-pasteable now command for one task (short hash)."""
    return f"/now_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _skip_command(infohash: str) -> str:
    """Copy-pasteable skip command for one task (short hash)."""
    return f"/skip_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _unskip_command(infohash: str) -> str:
    """Copy-pasteable unskip command for one task (short hash).

    Legacy alias: canonical verb is now `/resume_` (see _resume_command).
    """
    return f"/unskip_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _resume_command(infohash: str) -> str:
    """Copy-pasteable resume command for one task (short hash)."""
    return f"/resume_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


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
    try:
        _held = bool(getattr(ts, "skipped", 0))
    except Exception:
        _held = False
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
    if _held:
        state_text = f"⏭ Skipped · {state_text}"
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


def _is_hex40(s: object) -> bool:
    """True for a full 40-char lowercase hex infohash."""
    try:
        t = str(s or "").strip().lower()
    except Exception:
        return False
    return len(t) == 40 and all(c in "0123456789abcdef" for c in t)


def _now_eligible_hashes(members, notes: dict | None = None) -> list[str]:
    """Member hashes that may start at once (old fetch ∪ prefer sets).

    WAITING_INDEXER rows plus NEW grace-held rows; skipped rows never.
    Pure helper shared by the sheet builder and the group replies.
    """
    out: list[str] = []
    for _ts in members or []:
        try:
            _h = _member_hash(_ts)
            if not _h or getattr(_ts, "skipped", 0):
                continue
            if getattr(_ts, "state", None) == State.WAITING_INDEXER:
                out.append(_h)
                continue
            try:
                _note = (notes or {}).get(_h, "") if isinstance(
                    notes, dict) else ""
                if getattr(_ts, "state", None) == State.NEW \
                        and _is_grace_note_for_prefer(_note):
                    out.append(_h)
            except Exception:
                continue
        except Exception:
            continue
    return out


def _member_button_labels(members) -> list[tuple[str, str]]:
    """[(hash, label)] for hash-direct action buttons.

    Same short-tracker labels as the old member pick (hash-qualified
    on repeats); the callback carries the full hash, so no snapshot,
    index, or expiry is needed to route the tap.
    """
    try:
        out: list[tuple[str, str]] = []
        seen: dict[str, int] = {}
        for ts in members or []:
            try:
                _h = _member_hash(ts)
            except Exception:
                continue
            if not _h:
                continue
            _base = _pick_member_label(ts, _h[:10])
            _n = seen.get(_base, 0)
            seen[_base] = _n + 1
            out.append((_h, _base if _n == 0 else f"{_base} {_h[:6]}"))
        return out
    except Exception:
        return []


def _parse_action_data(data: str) -> tuple[str, bool, str, str, bool | None]:
    """Parse a hash-protocol callback into (cmd, all, hash, choice, remember).

    Shapes: `<cmd>:<40hex>`, `<cmd>:all:<40hex>`,
    `<cmd>:<40hex>:yes|no`, `<cmd>:all:<40hex>:yes|no` (keep/inject),
    `forget:<scope>`, `forget:<scope>:<0|1>`,
    `keep:<scope>:<0|1>:<yes|no>`, `abort`, plus the bulk-match scope
    `forget:match:<tok>:<0|1>` / `keep:match:<tok>:<0|1>:<yes|no>`
    (`<tok>` = base64url query, `match:<tok>` in the hash slot).
    `remember` is True/False when the buttons carried the Q1 remember
    choice, else None (legacy 3-part keep buttons predate it and mean
    remember, matching the old always-ignore cancel).
    Returns ("", False, "", "", None) when malformed — callers treat it
    as a stale button.
    """
    try:
        parts = str(data or "").split(":")
        if parts == ["abort"]:
            return ("abort", False, "", "", None)
        if len(parts) == 2 and parts[0] in (
                "go", "cancel", "inject", "skip", "resume", "forget"):
            if _is_hex40(parts[1]):
                return (parts[0], False, parts[1].lower(), "", None)
        if len(parts) == 3 and parts[0] in (
                "cancel", "keep", "inject", "skip", "resume",
                "go", "forget") and parts[1] == "all":
            if _is_hex40(parts[2]):
                return (parts[0], True, parts[2].lower(), "", None)
        if len(parts) == 3 and parts[0] in ("keep", "inject"):
            if _is_hex40(parts[1]) and parts[2] in ("yes", "no"):
                return (parts[0], False, parts[1].lower(), parts[2], True)
        if len(parts) == 4 and parts[0] in ("keep", "inject"):
            if _is_hex40(parts[1]) and parts[2] in ("0", "1") \
                    and parts[3] in ("yes", "no"):
                return (parts[0], False, parts[1].lower(), parts[3],
                        parts[2] == "1")
        if len(parts) == 5 and parts[0] in ("keep", "inject"):
            if parts[1] == "all" and _is_hex40(parts[2]) \
                    and parts[3] in ("0", "1") \
                    and parts[4] in ("yes", "no"):
                return (parts[0], True, parts[2].lower(), parts[4],
                        parts[3] == "1")
        if len(parts) == 3 and parts[0] == "forget":
            if _is_hex40(parts[1]) and parts[2] in ("0", "1"):
                return (parts[0], False, parts[1].lower(), "",
                        parts[2] == "1")
        if len(parts) == 4 and parts[0] == "forget":
            if parts[1] == "all" and _is_hex40(parts[2]) \
                    and parts[3] in ("0", "1"):
                return (parts[0], True, parts[2].lower(), "",
                        parts[3] == "1")
        # Bulk-match scope: the substring travels base64url-encoded and is
        # re-resolved live at every tap (`match:<tok>` in the hash slot).
        if len(parts) == 4 and parts[0] == "forget":
            if parts[1] == "match" and parts[3] in ("0", "1") \
                    and _MATCH_TOKEN_RE.match(parts[2] or ""):
                return (parts[0], False, f"match:{parts[2]}", "",
                        parts[3] == "1")
        if len(parts) == 5 and parts[0] == "keep":
            if parts[1] == "match" and parts[3] in ("0", "1") \
                    and parts[4] in ("yes", "no") \
                    and _MATCH_TOKEN_RE.match(parts[2] or ""):
                return (parts[0], False, f"match:{parts[2]}", parts[4],
                        parts[3] == "1")
    except Exception:
        pass
    return ("", False, "", "", None)


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
    ``▸ <domain> <stage>``. Each group carries one command (``/act_3``)
    opening the action sheet with only the currently eligible actions;
    the typed verbs (``/cancel_`` / ``/now_`` / ``/skip_`` / ``/resume_``
    / ``/injectfuse_`` with the same number or hash) all keep working
    as shortcuts. Detail cards keep per-torrent text commands
    (full-hash cancel). Pages count groups; numbers are global
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
        for (ts, progress) in members:
            domain_full = (_tracker_domain(ts.source_announce_url)
                           or _tracker_domain(ts.source_tracker))
            _note = _active_note(ts, notes)
            state_text = _active_state_text(ts, progress, _note)
            if domain_full:
                lines.append(f"▸ {_esc(domain_full)} {state_text}")
            else:
                lines.append(f"▸ {state_text}")

        # 3. One short group command (the `/` prefix marks it; every
        # column counts against the wrap limit). /act_N opens the action
        # sheet with only the currently eligible actions — the typed
        # verbs (/cancel_ /now_ /skip_ /resume_ /injectfuse_ with the same
        # number or hash) all keep working as shortcuts.
        _cmds = [f"/act_{group_num}"]
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


def _is_transient_tg_error(e: BaseException) -> bool:
    """True for timeout/network-class Telegram failures (worth a retry)."""
    try:
        if isinstance(e, (TimedOut, NetworkError)):
            return True
    except Exception:
        pass
    try:
        m = str(e).lower()
    except Exception:
        return False
    return any(s in m for s in (
        "timed out", "timeout", "connection", "temporary",
        "service unavailable", "bad gateway", "gateway timeout",
    ))


def _bot_command_menu() -> list:
    """Commands shown in the chat's `/` popup (Bot API menu).

    `/add` takes no argument (it acts on the replied-to or captioned
    .torrent); `/cancel_match` takes free text (any substring works),
    so both stay listed. The pickers need IDs the operator won't know,
    so they stay unlisted to keep the menu short.
    """
    try:
        return [BotCommand(
            "add",
            "Add a .torrent file — reply to it with /add, or send it with "
            "/add as caption",
        ), BotCommand(
            "cancel_match",
            "Cancel every active group whose title contains <text> — "
            "/cancel_match <text>",
        )]
    except Exception:
        return []


async def _publish_bot_commands(bot: object) -> None:
    """Best-effort setMyCommands: never break startup over the menu."""
    try:
        setter = getattr(bot, "set_my_commands", None)
        if not callable(setter):
            return
        await setter(_bot_command_menu())
    except Exception as e:  # noqa: BLE001
        log.debug("telegram command menu publish failed: %s", e)


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
        bot_token = self._cfg.bot_token.get_secret_value() if hasattr(self._cfg.bot_token, "get_secret_value") else str(self._cfg.bot_token)
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
        self, infohash: str, progress: float | None, state_value: str,
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
                    elif _is_transient_tg_error(e):
                        # Timeout on EDIT: the row may settle (DONE/FAILED)
                        # right after, leaving the active list where the
                        # stale net could retry it — record + requeue
                        # bounded instead of dropping the update.
                        log.warning(
                            "telegram detail edit timed out for %s (%s); requeueing",
                            infohash[:10], e,
                        )
                        self._note_detail_transient_failure(
                            infohash, progress, state_value)
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
            self._note_detail_transient_failure(infohash, progress, state_value)
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
        """Pagination nav buttons plus extra action rows.

        Extra rows are ``(label, callback_data)`` pairs (hash-direct
        action buttons), already chunked by the caller — stale/oversize
        entries are skipped defensively.
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
                    _rh = ((_r.source_infohash or "").lower())
                    if not _rh:
                        continue
                    if _same is not None:
                        if not _same(_r.source_name or "",
                                     _r.total_bytes or 0,
                                     leader.source_name or "",
                                     leader.total_bytes or 0):
                            continue
                    elif ((_r.source_name or "").strip().lower()
                            != (leader.source_name or "").strip().lower()):
                        continue
                    out.append(_rh)
                except Exception:
                    continue
            return out
        except Exception:
            return []

    def _now_eligible_hashes(
        self, members, notes: dict | None = None,
    ) -> list[str]:
        """Member hashes that may start at once (old fetch ∪ prefer sets)."""
        return _now_eligible_hashes(members, notes)

    def _sheet_text_and_rows(
        self, members, title: str, size_bytes: object,
        notes: dict | None = None,
    ) -> tuple[str, list[list[tuple[str, str]]]]:
        """Action sheet text + self-explanatory button rows for a group.

        Every button names its verb and target ("Start <tracker> now",
        "Cancel <tracker>…") so no memorized command is needed; "…"
        marks actions that ask a follow-up question first. One button
        per row — narrow screens wrap multi-button rows into ambiguity.
        """
        try:
            _size = _size_compact(size_bytes)
        except Exception:
            _size = ""
        _tspec = _safe_display_name((title or "")[:60])
        _now_ok = set(self._now_eligible_hashes(members, notes))
        _labels = _member_button_labels(members or [])
        try:
            _n = len(_labels)
        except Exception:
            _n = 0
        _held_any = False
        _free_any = False
        try:
            for _ts in members or []:
                if getattr(_ts, "skipped", 0):
                    _held_any = True
                else:
                    _free_any = True
        except Exception:
            pass
        _inject_ok = False
        try:
            for _ts in members or []:
                if getattr(_ts, "state", None) in (
                        State.WAITING_INDEXER, State.WAITING_DISK):
                    _inject_ok = True
                    break
        except Exception:
            pass
        _rows: list[list[tuple[str, str]]] = []
        # One row per copy: start and cancel side by side stay readable
        # while keeping the copy context shared.
        for (_h, _lbl) in _labels:
            _row: list[tuple[str, str]] = []
            if _h in _now_ok:
                _row.append((f"▶ Start {_lbl} now", f"go:{_h}"))
            _row.append((f"✖ Cancel {_lbl}…", f"cancel:{_h}"))
            _rows.append(_row)
        _leader = _labels[0][0] if _labels else ""
        if len(_labels) > 1 and _leader:
            _rows.append([(f"✖ Cancel all {_n} copies…",
                           f"cancel:all:{_leader}")])
        if _inject_ok and _leader:
            _rows.append([("💉 Inject all to fuse…",
                           f"inject:all:{_leader}")])
        if _free_any and _leader:
            _rows.append([("⏭ Skip release (pause downloads)",
                           f"skip:all:{_leader}")])
        if _held_any and _leader:
            _rows.append([("⏪ Resume release (restart it)",
                           f"resume:all:{_leader}")])
        _rows.append([("Close", "abort")])
        _text = (f"{_tspec}" + (f" · {_size}" if _size else "")
                 + (f" · {_n} cop{'y' if _n == 1 else 'ies'} tracked" if _n else "")
                 + "\nStart downloads now. Cancel/Inject ask first. "
                 "Skip pauses, Resume restarts.")
        return _text, _rows

    def _keepq_text_and_rows(
        self, title: str, scope_label: str, scope: str,
        size_bytes: object = None, remember: bool = True,
    ) -> tuple[str, list[list[tuple[str, str]]]]:
        """Keep/delete question text + hash buttons for a cancel scope.

        `remember` (the Q1 answer) travels in the buttons
        (`keep:<scope>:<0|1>:<yes|no>`); the question text names which
        variant applies so a forwarded screenshot stays intelligible.
        """
        try:
            _size = _size_compact(size_bytes)
        except Exception:
            _size = ""
        _tspec = _safe_display_name((title or "")[:60]) + (
            f" · {_size}" if _size else "")
        _ig = "1" if remember else "0"
        _blocked = "blocked from returning" if remember else \
            "allowed back later"
        _text = (f"Cancel {_tspec}{scope_label} — keep downloaded files? "
                 f"({_blocked})"
                 f"\nKeep untracks (data stays in place); "
                 f"Delete wipes the torrent and its data.")
        _rows = [[("✔ Keep files", f"keep:{scope}:{_ig}:yes")],
                 [("✖ Delete files", f"keep:{scope}:{_ig}:no")],
                 [("Close", "abort")]]
        return _text, _rows

    async def _send_sheet(
        self, text: str, rows: list, reply_to: Any = None,
    ) -> None:
        """Best-effort sheet reply with action buttons (plain text)."""
        bot = getattr(self, "_bot", None)
        if bot is None:
            return
        try:
            _btns = []
            for _row in rows or ():
                _line = []
                for (_label, _data) in _row or ():
                    if not _label or not _data or len(_data) > 64:
                        continue
                    _line.append(InlineKeyboardButton(
                        str(_label)[:60], callback_data=_data))
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
            sent = await bot.send_message(
                self._cfg.chat_id, text, **kwargs)
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
                    if len(_h) == 40 and all(
                            c in "0123456789abcdef" for c in _h):
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
                        _name, _err = await asyncio.to_thread(
                            self._unignore_core, _h)
                    except Exception:
                        continue
                    if _name:
                        lifted.append(_name)
            except Exception:
                pass
            if not tracked and not lifted:
                return ("Already gone from tracking"
                        if not unskip_text else unskip_text)
            reply = (unskip_text or "").strip()
            if lifted:
                shown = ", ".join(lifted[:3])
                if len(lifted) > 3:
                    shown += f" (+{len(lifted) - 3} more)"
                extra = (f"Unignored {shown} — re-send .torrent via /add "
                         f"(or re-drop) to track again.")
                reply = f"{reply} {extra}".strip() if reply else extra
            return reply or "Resume failed: nothing changed."
        except Exception as e:  # noqa: BLE001
            return f"Resume failed: {e}"

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
                or data.startswith("forget:")
                or data.startswith("abort")
                or data.startswith("inject:")
                or data.startswith("go:") or data.startswith("cancel:")
                or data.startswith("skip:") or data.startswith("resume:")):
            await self._on_action_button(query, data)
            if not data.startswith("abort"):
                # The tapped sheet served its purpose: remove it so
                # answered questions don't linger behind their follow-up
                # (`abort` already deletes itself above; the active-list
                # message never carries action buttons, only pagination,
                # so this only ever removes disposable sheets).
                try:
                    await self._delete_query_message(query)
                except Exception:
                    pass
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

    def _fetch_new_eligible(self, row) -> bool:
        """True when a NEW row is actually waiting (preferred-copy grace).

        Mirrors /prefer_ list eligibility: the grace note is the signal,
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

    def _fetch_watch_eligible(self, row) -> bool:
        """Deprecated alias of :meth:`_fetch_new_eligible`."""
        return self._fetch_new_eligible(row)

    def _resolve_fetch_target(self, short: str):
        """Map a `/fetch_<prefix|full-hash>` token to a waiting row.

        WAITING_INDEXER rows fetch the VPS1 original instead of waiting
        out Prowlarr. NEW grace-held rows (watch or racing) never wait
        on that state — allow fetch while grace-held, mirroring
        /prefer_ eligibility. Raises LookupError otherwise.
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
        """Execute `/cancel_` / `/cancel_match` / `/now_` / `/resume_` / `/injectfuse` commands.

    Legacy `/fetch_` + `/prefer_` parse as `/now_`; `/unskip_` +
    `/unignore_` parse as `/resume_` (same shapes, unified executors).

    Group commands (stable content ids from the active list) open
    member-choice buttons; full hashes and legacy hash prefixes act
    directly. Cancel always ends at a keep/delete question — nothing
    is wiped without an explicit choice. Injectfuse always ends at a
    Yes/No question — nothing changes state without verification.
    `/add` ingests a replied-to (or captioned) .torrent file as a
    Telegram-origin drop. Anything else is ignored. Only the
    configured chat/user may send commands.
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
            m_now = NOW_CMD_RE.match(text)
            m_skip = SKIP_CMD_RE.match(text)
            m_unskip = UNSKIP_CMD_RE.match(text)
            m_resume = RESUME_CMD_RE.match(text)
            m_unignore = UNIGNORE_CMD_RE.match(text)
            m_unignore_hash = UNIGNORE_HASH_RE.match(text)
            m_cancel = CANCEL_CMD_RE.match(text)
            m_cancel_match = CANCEL_MATCH_RE.match(text)
            m_injectfuse = INJECTFUSE_CMD_RE.match(text)
            m_injectfuse_hash = INJECTFUSE_HASH_RE.match(text)
            m_act = ACT_CMD_RE.match(text)
            m_add = ADD_CMD_RE.match(text)
            if (not m_fetch and not m_prefer and not m_now
                    and not m_cancel and not m_cancel_match
                    and not m_skip and not m_unskip and not m_resume
                    and not m_unignore and not m_unignore_hash
                    and not m_injectfuse and not m_injectfuse_hash
                    and not m_act and not m_add):
                return
            if not _tg_actor_allowed(self._cfg, user_id):
                try:
                    await self._reply("Not authorized for destructive actions.",
                                      reply_to=message)
                except Exception:
                    pass
                return
            if m_add:
                await self._process_document_command(message)
                return
            if m_fetch:
                await self._start_group_command(
                    "now", m_fetch.group(1), message)
                return
            if m_prefer:
                await self._start_group_command(
                    "now", m_prefer.group(1), message)
                return
            if m_now:
                await self._start_group_command(
                    "now", m_now.group(1), message)
                return
            if m_skip:
                await self._start_group_command(
                    "skip", m_skip.group(1), message)
                return
            if m_unskip:
                # Legacy alias: same shapes as /resume_ (digits, prefix,
                # full hash) via the unified resume entry point.
                await self._resume_command_entry(
                    m_unskip.group(1).lower(), message)
                return
            if m_resume:
                await self._resume_command_entry(
                    m_resume.group(1).lower(), message)
                return
            if m_unignore:
                await self._resume_command_entry(
                    m_unignore.group(1).lower(), message)
                return
            if m_unignore_hash:
                await self._resume_command_entry(
                    m_unignore_hash.group(1).lower(), message)
                return
            if m_injectfuse_hash:
                await self._start_single_command(
                    "injectfuse", m_injectfuse_hash.group(1).lower(), message)
                return
            if m_injectfuse:
                await self._start_group_command(
                    "injectfuse", m_injectfuse.group(1), message)
                return
            if m_act:
                await self._act_command_entry(
                    m_act.group(1).lower(), message)
                return
            if m_cancel_match:
                await self._cancel_match_command_entry(
                    m_cancel_match.group(1) or "", message)
                return
            await self._start_group_command(
                "cancel", m_cancel.group(1), message)
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
            await self._reply("Action failed: torrent parser unavailable.",
                              reply_to=message)
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
                "Reply to a .torrent file with /add (or send the file "
                "with /add as its caption).", reply_to=message)
            return
        try:
            fname = str(getattr(doc, "file_name", "") or "")
            fsize = getattr(doc, "file_size", None)
            file_id = getattr(doc, "file_id", None)
        except Exception:
            fname, fsize, file_id = "", None, None
        if not fname.lower().endswith(".torrent"):
            await self._reply(
                f"Not a .torrent file ({fname or 'unnamed'}); nothing ingested.",
                reply_to=message)
            return
        try:
            fsize_int = int(fsize) if fsize is not None else -1
        except (TypeError, ValueError):
            fsize_int = -1
        if fsize_int < 0 or fsize_int > MAX_TORRENT_BYTES:
            await self._reply(
                f"Refusing {fname or 'file'} ({fsize_int} B; cap "
                f"{MAX_TORRENT_BYTES} B).", reply_to=message)
            return
        if not file_id:
            await self._reply(
                "Could not read the file from Telegram; try re-sending it.",
                reply_to=message)
            return
        bot = getattr(self, "_bot", None)
        if bot is None:
            await self._reply("Action failed: bot not attached.",
                              reply_to=message)
            return
        try:
            tg_file = await bot.get_file(file_id)
        except Exception as e:  # noqa: BLE001
            await self._reply(
                f"Could not download the file from Telegram ({e}); "
                "try re-sending it.", reply_to=message)
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
                f"Could not download the file from Telegram ({e}); "
                "try re-sending it.", reply_to=message)
            return
        if not data or len(data) > MAX_TORRENT_BYTES + 1:
            await self._reply(
                "Refusing: file is empty or over the size cap.",
                reply_to=message)
            return
        try:
            infohash, name, size, announce = _bencoded_info_hash(data)
        except Exception as e:  # noqa: BLE001
            await self._reply(f"Not a valid .torrent file ({e}).",
                              reply_to=message)
            return
        store = getattr(self, "_store", None)
        if store is None:
            await self._reply("Action failed: bot not attached.",
                              reply_to=message)
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
                        reply_to=message)
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
                            infohash=infohash, name=name, size=size,
                            announce=announce or "", blob=bytes(data),
                            source_label="telegram")
                    except LookupError as e:
                        await self._reply(str(e)[:300], reply_to=message)
                        await self._delete_chat_file(message, doc_msg)
                        return
                    except Exception as e:  # noqa: BLE001
                        await self._reply(f"Action failed: {e}",
                                          reply_to=message)
                        return
                    await self._reply(
                        f"Re-queued {name} (was failed) — downloading "
                        f"on VPS2, watch its card.",
                        reply_to=message)
                    await self._delete_chat_file(message, doc_msg)
                    return
            try:
                _pre = existing.state in (State.NEW,
                                          State.WAITING_INDEXER,
                                          State.WAITING_DISK)
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
                    _kept = bool(await asyncio.to_thread(
                        store.set_blob, infohash, bytes(data)))
                except Exception:
                    _kept = False
                if _kept:
                    # Fresh input resumes a held row (same as a watch-dir
                    # re-drop): the operator is acting on it again.
                    try:
                        _upd = getattr(store, "update_columns", None)
                        _resumed = bool(await asyncio.to_thread(
                            _upd, infohash, {"skipped": 0})) if callable(_upd) else False
                    except Exception:
                        _resumed = False
                    _tail = (" — it can now download from your bytes: "
                             f"`{_now_command(infohash)}`.")
                    if _resumed and getattr(existing, "skipped", 0):
                        _tail = (" — hold cleared, it resumes next tick"
                                 " (bytes attached too).")
                    await self._reply(
                        f"Attached supplied .torrent to {name} ({_st}){_tail}",
                        reply_to=message)
                    await self._delete_chat_file(message, doc_msg)
                    return
            await self._reply(f"Already tracked: {name} ({_st}).",
                              reply_to=message)
            await self._delete_chat_file(message, doc_msg)
            return
        if _ignored:
            await self._reply(
                f"Ignored (previously cancelled): {name}. Nothing ingested.",
                reply_to=message)
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
            await self._reply(f"Action failed: could not track {name} ({e}).",
                              reply_to=message)
            return
        try:
            ts._blob = bytes(data)
        except Exception:
            pass
        try:
            store.upsert(ts)
        except Exception as e:  # noqa: BLE001
            await self._reply(f"Action failed: could not track {name} ({e}).",
                              reply_to=message)
            return
        try:
            log.info("telegram ingest: %s (%s, %d bytes) announce=%s",
                     name[:60], infohash[:10], size, _domain or "?")
        except Exception:
            pass
        await self._reply(
            f"Queued {name} ({_size_compact(size)}) via "
            f"{_domain or 'unknown tracker'} — downloading on VPS2, "
            "watch its card.", reply_to=message)
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
            if not bool(getattr(getattr(self, "_cfg", None),
                                "delete_processed_torrent", True)):
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
                    reply_to=cmd_msg)
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

    async def _execute_cancel_one(self, infohash: str, *, delete_files: bool,
                                    remember: bool = True) -> str:
        """Forget one release; files kept iff not `delete_files`.

        `remember=False` skips the ignore entry and hard-deletes the row
        (never-seen semantics): workers are stopped first so no stale
        upsert resurrects it, and a later re-drop re-ingests from scratch.
        """
        _verb = "Cancelled" if delete_files else "Kept files for"
        _done = ("(removed + ignored)" if remember
                 else "(removed, can be re-added)") if delete_files else (
            "untracked + ignored, data left in place" if remember else
            "untracked, data left in place (can be re-added)")
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
            _stop = getattr(coord, "stop_workers_for", None)
            if callable(_stop):
                try:
                    await _stop(infohash)
                except Exception:
                    pass
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    result = await forget_torrent(
                        cfg, dest=dest, store=store, target=infohash,
                        apply=True, delete_files=delete_files,
                        ignore=remember, hard=not remember,
                    )
                    try:
                        await coord._ssd_release(
                            result.get("source_infohash") or infohash)
                    except Exception:
                        pass
            else:
                result = await forget_torrent(
                    cfg, dest=dest, store=store, target=infohash,
                    apply=True, delete_files=delete_files,
                    ignore=remember, hard=not remember,
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
                                         *, delete_files: bool,
                                         remember: bool = True) -> str:
        """Forget resolved hashes (liveness re-checked each).

        `remember=False` forgets without the ignore entry and hard-deletes
        the rows (never-seen semantics): a later re-drop re-ingests from
        scratch. Workers are stopped first so no stale upsert resurrects.
        """
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
                        h, delete_files=delete_files, remember=remember))
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
        """Typed full-hash command: keepq for cancel, direct otherwise.

        fetch/prefer merged into "now": WAITING rows and grace-held rows
        both start at once via the starting-bytes path.
        """
        if kind in ("fetch", "prefer"):
            kind = "now"
        if kind == "injectfuse":
            await self._start_injectfuse_single(full_hash, message)
            return
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
        if kind in ("skip", "unskip"):
            # Group-scoped even from one hash: expand to the live group
            # first (single hash as the fallback when it is not grouped
            # anymore). See _group_hashes_for for why the hold covers
            # the release, not the tracker copy.
            try:
                _gh = await asyncio.to_thread(
                    self._group_hashes_for, full_hash)
            except Exception:
                _gh = None
            try:
                result = await self._skip_torrents(
                    _gh or [full_hash], hold=(kind == "skip"))
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
            return
        if kind == "cancel":
            _text = (
                f"Forget {title}?\n"
                f"🚫 Ignore blocks it from coming back; "
                f"👻 Just forget allows re-adding it later.")
            _rows = [[("🚫 Ignore + forget", f"forget:{full_hash}:1"),
                      ("👻 Just forget", f"forget:{full_hash}:0")],
                     [("Close", "abort")]]
            try:
                await self._send_sheet(_text, _rows, reply_to=message)
            except Exception:
                pass
            return
        try:
            if kind in ("fetch", "now"):
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
            base = TelegramBot._injectfuse_member_label(t)
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
                _lead = ((getattr(_m0, "infohash", "") or "")
                         or (getattr(_m0, "source_infohash", "") or ""))
                _lead = _lead.lower() if _lead else ""
            except Exception:
                _lead = ""
            if not _lead:
                try:
                    await self._reply("Nothing to inject (group changed).",
                                      reply_to=message)
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
            _text = (f"Inject {title} ({_n} cop{'y' if _n == 1 else 'ies'}) "
                     "to fuse seeding? Files must already be at the "
                     f"remote.{extra} Choose below.")
            _rows = [[("✔ Yes, inject", f"inject:{_lead}:yes")],
                     [("✖ No", f"inject:{_lead}:no")],
                     [("Close", "abort")]]
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception as e:  # noqa: BLE001
            log.debug("inject ask failed: %s", e)

    async def _start_injectfuse_single(self, full_hash: str, message: Any) -> None:
        """`/injectfuse <hash>`: tracked or untracked VPS1 torrent.

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
                await self._reply("Action failed: bot not attached.",
                                  reply_to=message)
            except Exception:
                pass
            return
        try:
            resolved = await coord.injectfuse_resolve(full_hash)
        except (LookupError, ValueError) as e:
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
        row = resolved.get("row")
        try:
            state = getattr(row, "state", None) if row is not None else None
        except Exception:
            state = None
        title = str(resolved.get("title") or full_hash[:10])[:60]
        if state in (State.DONE, State.RE_ADDING):
            try:
                await self._reply(
                    f"already {state.value}: {title} (fuse gate runs next tick)",
                    reply_to=message)
            except Exception:
                pass
            return
        members = list(resolved.get("members") or [])
        snap = self._injectfuse_snap(members)
        if not snap:
            try:
                await self._reply("Nothing to inject (group changed).",
                                  reply_to=message)
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
                return _safe_display_name(
                    str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"

    def _group_title(self, members: list) -> str:
        """Display title for a group (lead row's name, truncated)."""
        try:
            if members:
                return _safe_display_name(
                    str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"

    def _group_hashes_for(self, full_hash: str) -> list[str] | None:
        """Live group member hashes containing `full_hash`, else None.

        Group-scoped commands (/skip_, /unskip_) act on the whole
        release even from a single hash: every copy shares one hold
        because the hold covers the content (fuse-seeding), not the
        tracker — same all-or-nothing shape as injectfuse (cancel /
        fetch / prefer stay per-member: destructive or surgical).
        Fail-open None: the caller falls back to the single hash.
        """
        try:
            norm = (full_hash or "").strip().lower()
            if not norm:
                return None
            rows = self._store.list_active_inflight()
            for (_key, _members) in _group_active_items(
                    [(_r, None) for _r in rows or []]):
                _hs: list[str] = []
                _hit = False
                for (_t, _p) in _members or []:
                    try:
                        _h = _member_hash(_t)
                    except Exception:
                        continue
                    if not _h:
                        continue
                    _hs.append(_h)
                    if _h == norm:
                        _hit = True
                if _hit and _hs:
                    return _hs
            return None
        except Exception:
            return None

    async def _act_command_entry(self, token: str, message: Any) -> None:
        """Reply with the action sheet for a content group.

        Single displayed entry point (`/act_<n>`): resolves like the
        other group commands (number, group id, hash, prefix) and shows
        only the currently eligible actions as hash-direct buttons.
        """
        try:
            store = getattr(self, "_store", None)
            if store is None:
                try:
                    await self._reply("Action failed: bot not attached.",
                                      reply_to=message)
                except Exception:
                    pass
                return
            norm = (token or "").strip().lower()
            try:
                rows = await asyncio.to_thread(store.list_active_inflight)
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
            members: list = []
            if norm.isdigit():
                try:
                    _n = int(norm)
                except (TypeError, ValueError):
                    _n = 0
                if 1 <= _n <= len(groups):
                    _gk, _mem = groups[_n - 1]
                    members = [t for (t, _) in _mem]
            if not members and norm:
                try:
                    _by_gid = _live_group_by_gid(list(rows or []), norm)
                except Exception:
                    _by_gid = None
                if _by_gid is not None:
                    members = list(_by_gid[1])
            if not members and norm:
                try:
                    target = await asyncio.to_thread(
                        self._resolve_cancel_target, norm, cmd="act")
                    _gh = await asyncio.to_thread(
                        self._group_hashes_for,
                        (target.source_infohash or "").lower())
                    if _gh:
                        _all = {(_r.source_infohash or "").lower(): _r
                                for _r in rows or []}
                        members = [_all[h] for h in _gh if h in _all]
                    else:
                        members = [target]
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
            if not members:
                try:
                    await self._reply("No live group — refresh the list.",
                                      reply_to=message)
                except Exception:
                    pass
                return
            title = self._group_title(members)
            try:
                notes = self._live_notes_for(members)
            except Exception:
                notes = {}
            try:
                _text, _rows = self._sheet_text_and_rows(
                    members, title, getattr(members[0], "total_bytes", 0)
                    if members else 0, notes)
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(f"Action failed: {e}", reply_to=message)
                except Exception:
                    pass
                return
            try:
                await self._send_sheet(_text, _rows, reply_to=message)
            except Exception:
                pass
        except Exception as e:  # noqa: BLE001
            log.debug("act command failed: %s", e)

    def _live_match(self, query: str) -> list[tuple[str, list[str]]]:
        """Live (title, member-hashes) groups whose title contains `query`.

        Case-insensitive substring on the lead row's full name (the same
        name the list heading shows). Re-derived from the current store
        on every call, so the Q1 sheet, the Q2 sheet, and execution each
        see the live set — same stateless shape as `all:`-scoped buttons.
        """
        try:
            q = (query or "").strip().casefold()
            if not q:
                return []
            store = getattr(self, "_store", None)
            if store is None:
                return []
            _all = getattr(store, "list_active_inflight", None)
            rows = _all() if callable(_all) else []
            out: list[tuple[str, list[str]]] = []
            for (_key, _members) in _group_active_items(
                    [(_r, None) for _r in rows or []]):
                try:
                    _rows = [_t for (_t, _) in _members or []]
                    if not _rows:
                        continue
                    _name = str(getattr(_rows[0], "source_name", "") or "")
                    if q not in _name.casefold():
                        continue
                    _hashes = [_h for _h in
                               (_member_hash(_t) for _t in _rows) if _h]
                    if _hashes:
                        out.append((self._group_title(_rows), _hashes))
                except Exception:
                    continue
            return out
        except Exception:
            return []

    async def _cancel_match_command_entry(
            self, query: str, message: Any) -> None:
        """`/cancel_match <text>`: one remember + one keep/delete for all.

        No match → explanatory reply. A single live copy skips straight
        to its normal forget sheet; otherwise one sheet names every
        matched group and the two remember buttons carry the whole set
        (`forget:match:<tok>:<0|1>`), re-resolved live at each tap.
        """
        q = (query or "").strip()
        if len(q) < _MATCH_MIN_CHARS:
            try:
                await self._reply(
                    "Send /cancel_match <text> (at least "
                    f"{_MATCH_MIN_CHARS} characters) — cancels every "
                    "active group whose title contains <text>.",
                    reply_to=message)
            except Exception:
                pass
            return
        tok = _match_token(q)
        if not tok:
            try:
                await self._reply(
                    "That text is too long for buttons — shorten it to "
                    f"~{_MATCH_MAX_BYTES} characters.",
                    reply_to=message)
            except Exception:
                pass
            return
        try:
            matches = await asyncio.to_thread(self._live_match, q)
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(f"Action failed: {e}", reply_to=message)
            except Exception:
                pass
            return
        if not matches:
            try:
                await self._reply(
                    f"No active groups match "
                    f"'{_safe_display_name(q[:60])}'",
                    reply_to=message)
            except Exception:
                pass
            return
        if len(matches) == 1 and len(matches[0][1]) == 1:
            await self._start_single_command(
                "cancel", matches[0][1][0], message)
            return
        _total = sum(len(_hs) for (_, _hs) in matches)
        _shown = [f"• {_t} ({len(_hs)})" for (_t, _hs) in matches[:8]]
        if len(matches) > 8:
            _shown.append(f"• …and {len(matches) - 8} more")
        _text = (
            f"{len(matches)} groups "
            f"({_total} copies) match "
            f"'{_safe_display_name(q[:60])}':\n"
            + "\n".join(_shown) +
            "\nForget them all?\n"
            "🚫 Ignore blocks them from coming back; "
            "👻 Just forget allows re-adding later.")
        _rows = [[("🚫 Ignore + forget", f"forget:match:{tok}:1"),
                   ("👻 Just forget", f"forget:match:{tok}:0")],
                  [("Close", "abort")]]
        try:
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception:
            pass

    async def _start_group_command(self, kind: str, token: str, message: Any) -> None:
        """Typed command: positional number, gid, or legacy hash prefix.

        Positional numbers (what the list shows) and group ids resolve
        against the LIVE list right now; member buttons below carry full
        hashes re-resolved at tap time, so renumbering mid-flow cannot
        misroute. Full hashes and legacy prefixes act on one row.
        """
        token = (token or "").strip().lower()
        # Legacy aliases: fetch/prefer merged into the state-dependent
        # "now" verb (WAITING rows use original bytes, grace-held rows
        # start at once).
        if kind in ("fetch", "prefer"):
            kind = "now"
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
        if kind == "cancel":
            _labels = _member_button_labels(members)
            if len(_labels) <= 1:
                _hashes = [h for (h, _) in _labels]
                if not _hashes:
                    try:
                        await self._reply("Already gone from tracking",
                                          reply_to=message)
                    except Exception:
                        pass
                    return
                try:
                    await self._send_sheet(
                        f"Forget {title}?\n"
                        f"🚫 Ignore blocks it from coming back; "
                        f"👻 Just forget allows re-adding it later.",
                        [[("🚫 Ignore + forget",
                           f"forget:{_hashes[0]}:1"),
                          ("👻 Just forget",
                           f"forget:{_hashes[0]}:0")],
                         [("Close", "abort")]],
                        reply_to=message)
                except Exception:
                    pass
                return
            _rows: list[list[tuple[str, str]]] = [
                [(f"✖ Cancel {_lbl}…", f"cancel:{_h}")
                 for (_h, _lbl) in _labels][
                    i:i + 1]
                for i in range(0, len(_labels), 1)
            ]
            _rows.append([(
                f"✖ Cancel all {len(_labels)} copies…",
                f"cancel:all:{_labels[0][0]}")])
            _rows.append([("Close", "abort")])
            try:
                await self._send_sheet(
                    f"Cancel {title} ({len(_labels)} copies): "
                    f"pick below (tracker names).",
                    _rows, reply_to=message)
            except Exception:
                pass
            return
        # injectfuse: whole group, all-or-nothing. The question names
        # the LIVE VPS1 group (torrents added after the row was tracked
        # belong to it); the Yes button below re-resolves live too, so a
        # stale list can never misinject. Yes auto-lifts a prior cancel
        # and stops any SSD download first.
        if kind == "injectfuse":
            _lead_hashes = []
            for _m in members or []:
                try:
                    _mh = _member_hash(_m)
                except Exception:
                    _mh = ""
                if _mh:
                    _lead_hashes.append(_mh)
            if not _lead_hashes:
                try:
                    await self._reply("Already gone from tracking",
                                      reply_to=message)
                except Exception:
                    pass
                return
            try:
                coord = getattr(self, "_coord", None)
                if coord is None:
                    raise LookupError("bot not attached")
                resolved = await coord.injectfuse_resolve(_lead_hashes[0])
            except (LookupError, ValueError) as e:
                # VPS1 no longer lists the group (cleaned/pruned): fall
                # back to the tracked members — the remote bytes may
                # still be injectable.
                try:
                    _tracked = []
                    for _m in members or []:
                        try:
                            if _member_hash(_m):
                                _tracked.append(_m)
                        except Exception:
                            continue
                    if not _tracked:
                        raise
                    try:
                        _tsz = getattr(_tracked[0], "total_bytes", 0)
                    except Exception:
                        _tsz = 0
                    resolved = {"members": _tracked, "title": title,
                                "size": _tsz}
                except Exception:
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
            try:
                _resolved_for_ask = dict(resolved)
            except Exception:
                _resolved_for_ask = resolved
            await self._inject_ask_send(_resolved_for_ask, message)
            return
        # skip/unskip: whole group, all-or-nothing — no member pick
        # (a hold covers the release, every copy of it). Even one
        # member goes direct with the group name on screen.
        if kind in ("skip", "unskip"):
            _hashes = []
            for _m in members or []:
                try:
                    _mh = _member_hash(_m)
                except Exception:
                    _mh = ""
                if _mh:
                    _hashes.append(_mh)
            if not _hashes:
                try:
                    await self._reply("Already gone from tracking",
                                      reply_to=message)
                except Exception:
                    pass
                return
            try:
                result = await self._skip_torrents(
                    _hashes, hold=(kind == "skip"))
            except Exception as e:  # noqa: BLE001
                result = f"Action failed: {e}"
            try:
                await self._reply(result[:300], reply_to=message)
            except Exception:
                pass
            try:
                await self._refresh_active_message()
            except Exception:
                pass
            return
        # now: eligible members choose their own copy via hash buttons.
        # A lone eligible copy starts at once; several get one button
        # each. Empty set replies inline. No snapshot: each button
        # carries its copy's hash and execution re-checks eligibility.
        _notes = self._live_notes_for(members)
        _eligible = set(self._now_eligible_hashes(members, _notes))
        _labels = [(h, lbl) for (h, lbl) in _member_button_labels(members)
                   if h in _eligible]
        if not _labels:
            try:
                await self._reply(
                    f"Nothing to {kind} right now (states changed).",
                    reply_to=message)
            except Exception:
                pass
            return
        if len(_labels) == 1:
            try:
                result = await self._fetch_torrent(_labels[0][0])
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
            return
        _rows = [[(f"▶ Start {_lbl} now", f"go:{_h}")]
                   for (_h, _lbl) in _labels]
        _rows.append([("Close", "abort")])
        try:
            await self._send_sheet(
                f"Start {title}: pick a copy below (tracker names).",
                _rows, reply_to=message)
        except Exception:
            pass

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

        async def _row_title(h: str) -> str:
            try:
                store = getattr(self, "_store", None)
                _get = getattr(store, "get", None) if store else None
                row = await asyncio.to_thread(_get, h) \
                    if callable(_get) else None
                if row is not None:
                    return str(getattr(row, "source_name", "")
                               or h[:10])[:60]
            except Exception:
                pass
            return h[:10]

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
                if _all:
                    _hashes = await _group_of(_h)
                    if not _hashes:
                        await _say("Group changed — tap /act again.")
                        return
                    _title = await _row_title(_h)
                    _scope = f"all:{_h}"
                    _scope_label = f" · all {len(_hashes)} copies"
                else:
                    _title = await _row_title(_h)
                    _scope = _h
                    _scope_label = ""
                _text = (
                    f"Forget {_safe_display_name((_title or '')[:60])}"
                    f"{_scope_label}?\n"
                    f"🚫 Ignore blocks it from coming back; "
                    f"👻 Just forget allows re-adding it later.")
                _rows = [[("🚫 Ignore + forget", f"forget:{_scope}:1"),
                          ("👻 Just forget", f"forget:{_scope}:0")],
                         [("Close", "abort")]]
                try:
                    await self._send_sheet(
                        _text, _rows,
                        reply_to=getattr(query, "message", None))
                except Exception:
                    pass
                return
            if cmd == "forget" and _h.startswith("match:"):
                # Bulk-match scope: re-resolve the substring live; the set
                # can shrink between taps, so the keep question names the
                # live count, not the Q1 count.
                if _ig is None:
                    _ig = True
                _mtok = _h[len("match:"):]
                _mq = _match_query(_mtok)
                try:
                    _matches = await asyncio.to_thread(
                        self._live_match, _mq) if _mq else []
                except Exception:
                    _matches = []
                if not _matches:
                    await _say("Already gone from tracking")
                    await _refresh()
                    return
                _mtotal = sum(len(_hs) for (_, _hs) in _matches or [])
                _text, _rows = self._keepq_text_and_rows(
                    f"{len(_matches)} groups matching "
                    f"'{_safe_display_name(_mq[:40])}'",
                    f" · {_mtotal} copies",
                    f"match:{_mtok}", None, remember=_ig)
                try:
                    await self._send_sheet(
                        _text, _rows,
                        reply_to=getattr(query, "message", None))
                except Exception:
                    pass
                return
            if cmd == "keep" and _choice in ("yes", "no") \
                    and _h.startswith("match:"):
                if _ig is None:
                    _ig = True
                _mq = _match_query(_h[len("match:"):])
                try:
                    _matches = await asyncio.to_thread(
                        self._live_match, _mq) if _mq else []
                except Exception:
                    _matches = []
                _hashes = [_hh for (_, _hs) in _matches or []
                           for _hh in _hs or []]
                if not _hashes:
                    await _say("Already gone from tracking")
                    await _refresh()
                    return
                result = await self._execute_snapshot_cancel(
                    _hashes, f"{len(_matches)} matched groups",
                    delete_files=(_choice == "no"), remember=_ig)
                await _say(result)
                await _refresh()
                return
            if cmd == "forget" and _h:
                if _ig is None:
                    _ig = True
                if _all:
                    _hashes = await _group_of(_h)
                    if not _hashes:
                        await _say("Group changed — tap /act again.")
                        return
                    _title = await _row_title(_h)
                    _text, _rows = self._keepq_text_and_rows(
                        _title, f" · all {len(_hashes)} copies",
                        f"all:{_h}", None, remember=_ig)
                else:
                    _title = await _row_title(_h)
                    _text, _rows = self._keepq_text_and_rows(
                        _title, "", _h, None, remember=_ig)
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
                if _all:
                    _hashes = await _group_of(_h)
                    _scope_title = await _row_title(_h)
                else:
                    _hashes = [_h]
                    _scope_title = await _row_title(_h)
                if not _hashes:
                    await _say("Already gone from tracking")
                    await _refresh()
                    return
                result = await self._execute_snapshot_cancel(
                    _hashes, _scope_title, delete_files=(_choice == "no"),
                    remember=_ig)
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

    async def _fetch_torrent(self, infohash: str) -> str:
        """Flag a waiting row to use its starting torrent now.

        WAITING_INDEXER rows use their starting bytes instead of
        waiting out Prowlarr (force_direct + due timer): the
        VPS1 original when reachable, else a supplied .torrent
        attached via /add. NEW
        grace-held rows get their starting bytes instead (force_direct
        only — no state change; the NEW flow honors the flag next tick
        and a worker is woken for this tick): watch drops use the starting
        .torrent, racing rows use the VPS1 original.
        """
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Fetch failed: bot not attached"

        acted: list[str] = []

        def _flag() -> str:
            row = store.get(infohash)
            if row is None:
                raise LookupError("no longer tracked (done/cancelled?)")
            if getattr(row, "skipped", 0):
                return (
                    f"{(row.source_name or infohash[:10])[:50]} is skipped — "
                    f"{_resume_command(infohash)} first"
                )
            if row.state == State.WAITING_INDEXER:
                # Fresh retry window for the direct phase, same as the automatic
                # timeout fallback: an explicit fetch buys full direct retries,
                # not just the remainder of the spent prowlarr window. The row
                # stays WAITING_INDEXER with its timer due now, so the next
                # scheduler wakeup picks it up without an intermediate state.
                row.force_direct = 1
                row.indexer_first_queried_at = dt.datetime.now(dt.timezone.utc)
                row.indexer_attempts = 0
                row.indexer_next_retry_at = dt.datetime.now(dt.timezone.utc)
                try:
                    store.upsert(row)
                except Exception as e:  # noqa: BLE001
                    return f"Fetch failed: {e}"
                return (
                    f"Fetching original for {(row.source_name or infohash[:10])[:50]} "
                    f"(bypassing Prowlarr; VPS1 copy, else supplied .torrent)"
                )
            # NEW grace-held rows: "fetch" = use the dropped .torrent now
            # (skip Prowlarr search and grace hold). No state change —
            # NEW rows are worked every tick — and election/in-flight
            # deferrals still apply, since those guard live downloads,
            # not indexer waits.
            if row.state == State.NEW and self._fetch_new_eligible(row):
                try:
                    _upd = getattr(store, "update_columns", None)
                    if callable(_upd):
                        _applied = bool(_upd(
                            infohash, {"force_direct": 1},
                            expected_state=State.NEW))
                    else:
                        row.force_direct = 1
                        store.upsert(row)
                        _applied = True
                except Exception as e:  # noqa: BLE001
                    return f"Fetch failed: {e}"
                if not _applied:
                    return (
                        f"{(row.source_name or infohash[:10])[:50]} moved "
                        f"state — nothing to fetch"
                    )
                acted.append(infohash)
                try:
                    _is_watch = bool(coord._is_watch_row(row)) if callable(
                        getattr(coord, "_is_watch_row", None)) else False
                except Exception:
                    _is_watch = False
                if _is_watch:
                    return (
                        f"Using dropped .torrent directly for "
                        f"{(row.source_name or infohash[:10])[:50]} "
                        f"(skipping Prowlarr search and grace hold)"
                    )
                return (
                    f"Using VPS1 original directly for "
                    f"{(row.source_name or infohash[:10])[:50]} "
                    f"(skipping Prowlarr search and grace hold)"
                )
            try:
                _label = row.state.value
            except Exception:
                _label = "unknown"
            return (
                f"{(row.source_name or infohash[:10])[:50]} is "
                f"{_label} — nothing to fetch"
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
        # NEW watch rows stay NEW: wake one worker now so the flag takes
        # effect this tick (the scheduler is the backstop).
        try:
            if acted:
                _wrow = store.get(acted[0])
                if _wrow is not None:
                    _spawn = getattr(coord, "_spawn_worker", None)
                    if callable(_spawn):
                        _spawn(_wrow)
        except Exception:
            log.debug("fetch wake failed; next tick picks it up")
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

    async def _skip_torrents(self, hashes: list[str], *, hold: bool) -> str:
        """Hold (`hold=True`, /skip_) or resume (/unskip_) tracked releases.

        Whole-hash list (a group acts on every copy): the `skipped` flag
        keeps each row's state but gives it no workers — no Prowlarr
        queries, no downloads, no moves — until resumed. Resumed rows
        get a worker woken now (next tick is the backstop) so the
        normal flow (fuse check, VPS1/VPS2 checks, Prowlarr cross-seed
        search) restarts immediately. Never raises: all outcomes arrive
        as reply text.
        """
        verb = "skip" if hold else "unskip"
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return f"{verb.capitalize()} failed: bot not attached"
        wanted = []
        try:
            for h in hashes or []:
                _h = (h or "").strip().lower()
                if len(_h) == 40 and all(
                        c in "0123456789abcdef" for c in _h):
                    wanted.append(_h)
        except Exception:
            pass
        if not wanted:
            return f"{verb.capitalize()} failed: nothing tracked to act on"

        acted: list[str] = []
        gone = 0

        def _flag() -> None:
            nonlocal gone
            for h in wanted:
                try:
                    row = store.get(h, include_blob=False)
                except Exception:
                    row = None
                if row is None:
                    gone += 1
                    continue
                try:
                    _upd = getattr(store, "update_columns", None)
                    if callable(_upd):
                        _applied = bool(_upd(
                            h, {"skipped": 1 if hold else 0}))
                    else:
                        row.skipped = 1 if hold else 0
                        store.upsert(row)
                        _applied = True
                except Exception:
                    continue
                if _applied:
                    acted.append(h)

        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    await asyncio.to_thread(_flag)
            else:
                await asyncio.to_thread(_flag)
        except Exception as e:  # noqa: BLE001
            return f"{verb.capitalize()} failed: {e}"
        if not acted:
            return (f"Already gone from tracking"
                    if gone else f"Nothing to {verb} (states changed)")
        try:
            _names = []
            for h in acted:
                try:
                    _r = store.get(h, include_blob=False)
                    _names.append(str(getattr(_r, "source_name", "") or h[:10])[:50])
                except Exception:
                    _names.append(h[:10])
        except Exception:
            _names = [h[:10] for h in acted]
        _title = _names[0] if len(_names) == 1 else f"{len(acted)} copies"
        if not hold:
            try:
                _spawn = getattr(coord, "_spawn_worker", None)
                if callable(_spawn):
                    for h in acted:
                        try:
                            _wrow = store.get(h, include_blob=False)
                            if _wrow is not None:
                                _spawn(_wrow)
                        except Exception:
                            continue
            except Exception:
                log.debug("unskip wake failed; next tick picks it up")
            return (f"Resumed {_title} — fuse/VPS checks and Prowlarr "
                    f"cross-seed search run again next tick")
        return (f"Skipped {_title} — held with state kept, no workers "
                f"until {_resume_command(acted[0])}")

    def _unignore_core(self, target: str) -> tuple[str | None, str | None]:
        """Lift one ignore entry (+ tombstone); returns (name, error).

        Sync core behind _unignore_torrent and the /resume_ composer:
        (name, None) on success, (None, message) when there is nothing
        to lift or it fails. Full 40-char hash only (see _unignore_torrent).
        """
        try:
            norm = (target or "").strip().lower()
        except Exception:
            norm = ""
        if len(norm) != 40 or any(c not in "0123456789abcdef" for c in norm):
            return None, ("Send /resume_<full 40-char infohash> "
                           "(copy it from the detail card).")
        store = getattr(self, "_store", None)
        if store is None:
            return None, "Action failed: bot not attached."
        try:
            entry = store.find_ignored(norm)
        except LookupError as e:
            return None, str(e)[:300]
        except Exception as e:  # noqa: BLE001
            return None, f"Action failed: {e}"
        try:
            h = (entry.get("source_infohash") or "").lower()
            name = str(entry.get("source_name") or h[:10])[:60]
        except Exception:
            h, name = norm, norm[:10]
        try:
            store.unignore_torrent(h)
            store.clear_tombstone(h)
        except Exception as e:  # noqa: BLE001
            return None, f"Action failed: {e}"
        return name, None

    async def _unignore_torrent(self, target: str, message: Any) -> None:
        """Drop one cancelled release from the ignore list (+ tombstone).

        Legacy alias: canonical verb is now `/resume_`, which also
        resumes held rows. This path keeps the exact historical replies.
        Full 40-char hash only: ignored entries are invisible in the
        task list, so prefixes cannot be disambiguated there. Mirrors
        the `unignore` CLI (which refuses while the daemon runs, so
        this is the live path). After this, re-send the .torrent via
        /add or re-drop it. Never raises: all outcomes arrive as replies.
        """
        try:
            name, err = await asyncio.to_thread(self._unignore_core, target)
        except Exception as e:  # noqa: BLE001
            name, err = None, f"Action failed: {e}"
        if err is not None and name is None:
            try:
                await self._reply(err[:300], reply_to=message)
            except Exception:
                pass
            return
        try:
            await self._reply(
                f"Unignored {name} — re-send its .torrent via /add "
                f"(or re-drop it) to track it again.", reply_to=message)
        except Exception:
            pass

    async def _resume_command_entry(self, token: str, message: Any) -> None:
        """Unified resume: unhold tracked rows + lift ignored hashes.

        Replaces `/unskip_` + `/unignore_` with one verb branching on
        current state (digits address a live group like the other group
        commands; a full hash reaches tracked rows AND invisible ignore
        entries; shorter prefixes resolve tracked rows only — ignored
        entries still need the full hash, as with `/unignore_` before).
        Never raises: all outcomes arrive as replies.
        """
        try:
            store = getattr(self, "_store", None)
            if store is None:
                try:
                    await self._reply("Action failed: bot not attached.",
                                      reply_to=message)
                except Exception:
                    pass
                return
            norm = (token or "").strip().lower()
            hashes: list[str] = []
            if norm.isdigit():
                try:
                    rows = await asyncio.to_thread(store.list_active_inflight)
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
                try:
                    _n = int(norm)
                except (TypeError, ValueError):
                    _n = 0
                if 1 <= _n <= len(groups):
                    _gk, _mem = groups[_n - 1]
                    for _t in [t for (t, _) in _mem]:
                        try:
                            _h = _member_hash(_t)
                        except Exception:
                            continue
                        if _h:
                            hashes.append(_h)
                if not hashes:
                    try:
                        await self._reply(
                            f"No live group #{norm} — refresh the list.",
                            reply_to=message)
                    except Exception:
                        pass
                    return
            elif len(norm) == 40 and all(
                    c in "0123456789abcdef" for c in norm):
                # Single hash: the shared core unholds tracked rows and
                # lifts ignored hashes (each half no-ops cleanly absent).
                hashes = [norm]
            else:
                # Group id (older messages) or legacy torrent prefix for
                # tracked rows. A hex token matching nothing tracked keeps
                # the old /unignore_ hint (ignored entries need the full
                # hash); anything else keeps the resolver text.
                _by_gid = None
                try:
                    _rows_g = await asyncio.to_thread(
                        store.list_active_inflight)
                    _by_gid = _live_group_by_gid(
                        list(_rows_g or []), norm)
                except Exception:
                    _by_gid = None
                if _by_gid is not None:
                    _gk, _mem = _by_gid
                    for _t in list(_mem or []):
                        try:
                            _h = _member_hash(_t)
                        except Exception:
                            continue
                        if _h:
                            hashes.append(_h)
                if not hashes:
                    try:
                        target = await asyncio.to_thread(
                            self._resolve_cancel_target, norm, cmd="resume")
                        hashes = [(target.source_infohash or "").lower()]
                    except LookupError as e:
                        try:
                            _hexish = bool(norm) and all(
                                c in "0123456789abcdef" for c in norm)
                        except Exception:
                            _hexish = False
                        await self._reply(
                            ("Send /resume_<full 40-char infohash> "
                             "(copy it from the detail card).")
                            if _hexish else str(e)[:300],
                            reply_to=message)
                        return
                    except Exception as e:  # noqa: BLE001
                        try:
                            await self._reply(f"Action failed: {e}", reply_to=message)
                        except Exception:
                            pass
                        return
                if not hashes:
                    try:
                        await self._reply(
                            f"No live group #{norm} — refresh the list.",
                            reply_to=message)
                    except Exception:
                        pass
                    return
            try:
                reply = await self._resume_hashes_reply(hashes)
            except Exception as e:  # noqa: BLE001
                reply = f"Resume failed: {e}"
            try:
                await self._reply(reply[:300], reply_to=message)
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
        except Exception as e:  # noqa: BLE001
            log.debug("resume command failed: %s", e)

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
            "✗ Cancelled by operator (removed + ignored)\n"
            f"Unignore: `/resume_{(infohash or '').lower()}`"
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
        # Retired active ids first: a flood-delayed delete from an earlier
        # repost must not strand an old card in history forever.
        try:
            await self._sweep_orphan_active_messages()
        except Exception:
            pass
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
                    if (_rr is None or _rv not in ("done", "failed")
                            or _sent2.get(_hh) == _rv):
                        _tmap.pop(_hh, None)
                        continue
                    try:
                        _ni = int(_n or 0)
                    except (TypeError, ValueError):
                        _ni = 0
                    if _ni >= 15:
                        _tmap.pop(_hh, None)
                        log.debug(
                            "terminal card retry budget spent for %s; "
                            "leaving card as-is", _hh[:10])
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
        keyboard = self._build_keyboard(cur_page, total_pages)

        # Skip the API call if page, total_pages and text are identical
        # — unless a keep-at-bottom repost is due (position refreshes
        # even when the content is unchanged). Button digests stay in
        # the key: a button-only change must re-send.
        try:
            cache_key = (cur_page, total_pages, text)
        except Exception:
            cache_key = (cur_page, total_pages, text)

        repost_due = self._repost_due()
        if cache_key == self._last_active_cache and self._active_msg_id is not None and not repost_due:
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
        if _ukey is not None and not repost_due:
            try:
                _same_key = (_ukey == cache_key)
            except Exception:
                _same_key = False
            if _same_key:
                try:
                    _have_id = (self._active_msg_id is not None
                                or (self._prev_active_msg_id or 0) > 0)
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
                        "interval", _prev_id, _prev_e,
                    )
                    return
                except TelegramError as _prev_e:
                    _prev_msg = str(_prev_e).lower()
                    if self._delete_gone(_prev_msg):
                        log.debug(
                            "active-tasks previous message %s already gone "
                            "(%s); resending", _prev_id, _prev_e,
                        )
                        self._prev_active_msg_id = None
                    elif self._delete_hopeless(_prev_msg):
                        log.warning(
                            "active-tasks previous message %s delete refused "
                            "(%s); abandoning it and resending",
                            _prev_id, _prev_e,
                        )
                        self._prev_active_msg_id = None
                    elif (_is_transient_tg_error(_prev_e)
                            or "flood control" in _prev_msg
                            or "too many requests" in _prev_msg
                            or "timed out" in _prev_msg
                            or "timeout" in _prev_msg
                            or "connection" in _prev_msg
                            or "retry after" in _prev_msg
                            or "retry in" in _prev_msg):
                        log.warning(
                            "active-tasks previous message %s delete "
                            "rate-limited (%s); keeping it, will retry next "
                            "interval", _prev_id, _prev_e,
                        )
                        return
                    else:
                        log.warning(
                            "active-tasks previous message %s delete failed "
                            "(%s); keeping it, will retry next interval",
                            _prev_id, _prev_e,
                        )
                        return
                except Exception as _prev_e:  # noqa: BLE001
                    log.warning(
                        "active-tasks previous message %s delete failed "
                        "(%s); keeping it, will retry next interval",
                        _prev_id, _prev_e,
                    )
                    return

            # New send attempt supersedes any uncertain earlier send.
            self._send_uncertain_key = None
            self._send_uncertain_skips = 0
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
                # Assume landed (lost responses look identical) — the
                # uncertain-send guard above skips blind resends while the
                # text is unchanged, bounding detached duplicates.
                self._last_active_cache = cache_key
                self._send_uncertain_key = cache_key
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
        return any(s in low for s in (
            "message to delete not found",
            "message to edit not found",
            "message_id_invalid",
            "message not found",
            "couldn't find the message",
        ))

    @staticmethod
    def _delete_hopeless(msg: str) -> bool:
        """True when retrying a delete can never succeed (rights/chat)."""
        try:
            low = (msg or "").lower()
        except Exception:
            return False
        return any(s in low for s in (
            "chat not found",
            "not enough rights",
            "need administrator",
            "not an administrator",
            "bot was kicked",
            "bot was blocked",
            "forbidden",
        ))

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
                        "active-tasks orphan delete %s deferred (%s); "
                        "will retry next interval", mid, e,
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
        self, text: str, keyboard: InlineKeyboardMarkup | None,
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
        except (RetryAfter, TimedOut, NetworkError) as e:
            log.warning(
                "active-tasks repost delete deferred "
                "(transient %s); keeping old message, will retry next "
                "interval", e,
            )
            return
        except TelegramError as e:
            msg = str(e).lower()
            if self._delete_gone(msg):
                log.debug("active-tasks repost: old message %s already "
                          "gone; resending", old_int)
            elif self._delete_hopeless(msg):
                log.warning(
                    "active-tasks repost delete refused (%s); keeping "
                    "old message in place", e,
                )
                try:
                    self._last_repost_monotonic = time.monotonic()
                except Exception:
                    pass
                return
            elif (_is_transient_tg_error(e)
                    or "flood control" in msg
                    or "too many requests" in msg
                    or "timed out" in msg
                    or "timeout" in msg
                    or "connection" in msg
                    or "retry after" in msg
                    or "retry in" in msg):
                log.warning(
                    "active-tasks repost delete rate-limited (%s); "
                    "keeping old message, will retry next interval", e,
                )
                return
            else:
                log.warning(
                    "active-tasks repost delete failed (%s); keeping old "
                    "message, will retry next interval", e,
                )
                return
        except Exception as e:  # noqa: BLE001
            log.warning(
                "active-tasks repost delete failed (%s); keeping old "
                "message, will retry next interval", e,
            )
            return
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
            # Assume the resend landed (lost responses look identical) so
            # the uncertain-send guard skips blind resends while the text
            # is unchanged. A previous id always existed here (repost only
            # runs with a known id), so the first-send-ever exception in
            # the guard never triggers from this path.
            self._last_active_cache = cache_key
            self._send_uncertain_key = cache_key
            await asyncio.to_thread(
                self._store.set_meta, "telegram_active_msg_id", ""
            )
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
            await asyncio.to_thread(
                self._store.set_meta, "telegram_active_msg_id", ""
            )
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