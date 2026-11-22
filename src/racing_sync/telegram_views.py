"""Active-tasks + detail-card rendering (pure view layer).

Split out of the telegram bot god-file. Everything here is pure
functions over rows: markdown escaping, size/speed/ETA formatting,
grouping, and the two renderers. The only outbound dependency is
the protocol module for copy-pasteable command builders
(`render_detail` links them); the bot imports views, never the
reverse.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import time
from urllib.parse import urlsplit

from .state import State, TorrentState
from .telegram_protocol import _now_command, _resume_command

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


def _tg_len(text: str) -> int:
    """Length Telegram counts: UTF-16 code units, not code points.

    An astral character (emoji, rare CJK) is two units, so budgeting by
    code points lets an emoji-heavy card exceed 4096 and the edit is
    rejected outright.
    """
    try:
        return len(text.encode("utf-16-le")) // 2
    except Exception:
        return len(text)


def _truncate_to_units(text: str, max_units: int) -> str:
    """Longest prefix of `text` whose UTF-16 length fits `max_units`."""
    if max_units <= 0:
        return ""
    if _tg_len(text) <= max_units:
        return text
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _tg_len(text[:mid]) <= max_units:
            lo = mid
        else:
            hi = mid - 1
    return text[:lo]


def _safe_truncate_markdown(text: str, max_len: int = 4096) -> str:
    """Truncate text to max_len while keeping markdown tags properly closed and ending with '...'."""
    if _tg_len(text) <= max_len:
        return text

    lines = text.split("\n")
    acc: list[str] = []
    curr_len = 0
    suffix = "\n..."
    budget = max(0, max_len - _tg_len(suffix))
    for line in lines:
        added = _tg_len(line) + (1 if acc else 0)
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
        truncated = _truncate_to_units(text, max(0, max_len - _tg_len(suffix)))

    to_close = _unclosed_markdown_delims(truncated)

    if to_close:
        excess = (_tg_len(truncated) + _tg_len(to_close) + _tg_len(suffix)) - max_len
        if excess > 0:
            truncated = _truncate_to_units(truncated, max(0, _tg_len(truncated) - excess))
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
        _generic = {"tracker", "trackers", "www", "www2", "api", "announce", "bt", "ipv6"}
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
    for long, short in ((" PB", "P"), (" TB", "T"), (" GB", "G"), (" MB", "M"), (" KB", "K")):
        if s.endswith(long):
            return s[: -len(long)] + short
    return s


def _speed_human(bps: object) -> str:
    """Compact download rate for the active list (``1.2M/s``, "" when 0)."""
    try:
        n = int(bps or 0)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    if n <= 0:
        return ""
    return f"{_size_compact(n)}/s"


def _eta_human(secs: object) -> str:
    """Compact ETA for the active list (``45s`` / ``4m`` / ``2h``)."""
    try:
        s = int(secs or 0)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    if s <= 0:
        return ""
    if s < 90:
        return f"{s}s"
    if s < 5400:
        return f"{max(1, round(s / 60))}m"
    return f"{s // 3600}h"


def _list_bar(progress: float, width: int = 8) -> str:
    """Narrow progress bar for active-list rows (detail cards use 16)."""
    try:
        filled = int(round(max(0.0, min(1.0, float(progress))) * width))
    except (TypeError, ValueError):
        return ""
    return "█" * filled + "░" * (width - filled)


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


def _as_aware_utc(value: object) -> dt.datetime | None:
    """Coerce to aware UTC; naive legacy rows are treated as UTC, not local."""
    if not isinstance(value, dt.datetime):
        return None
    try:
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.UTC)
        return value
    except Exception:
        return None


def _retry_in_future_seconds(value: object) -> float | None:
    """Seconds until `value`, or None when not a future datetime."""
    aware = _as_aware_utc(value)
    if aware is None:
        return None
    try:
        delta = (aware - dt.datetime.now(dt.UTC)).total_seconds()
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
    return (name or "").replace("`", "'").replace("\n", " ").replace("\r", " ").rstrip("\\")


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


def render_detail(ts: TorrentState, progress: float | None = None, note: str = "") -> str:
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
        lines.append(f"Indexer miss #{ts.indexer_attempts}; next retry at {when}")
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


def _active_state_text(
    ts: TorrentState,
    progress: float | None,
    note: str,
    speed_bps: object = 0,
    move_progress: object = None,
) -> str:
    """One-line stage text for an active-tasks tracker row.

    DOWNLOADING rows additionally show bar + percent + rate + ETA when
    known (``⬇️ Downloading · 45.0% · ████░░░░ · 1.2M/s · ETA 4m``);
    MOVING rows show the rclone live line (``📦 Moving · 42% · ▓▓▓░░░ ·
    80M/s · ETA 12m``) when `move_progress` carries a fresh
    RcloneProgress; anything unknown is omitted rather than guessed.
    """
    _bd = _batch_display(ts)
    try:
        _held = bool(getattr(ts, "skipped", 0))
    except Exception:
        _held = False
    if note and ts.state in (State.NEW, State.WAITING_DISK):
        _compact = _compact_wait_note(note)
        state_text = f"⏳ {_compact}" if _compact else f"⏳ {_esc(note)}"
    elif ts.state == State.DOWNLOADING:
        parts = ["⬇️ Downloading"]
        if progress is not None:
            try:
                _pct = float(progress)
            except (TypeError, ValueError):
                _pct = None
            if _pct is not None:
                parts.append(f"{_pct * 100:.1f}%")
                _bar = _list_bar(_pct)
                if _bar:
                    parts.append(_bar)
        _spd = _speed_human(speed_bps)
        if _spd:
            parts.append(_spd)
        try:
            _p = float(progress) if progress is not None else None
            _bps = int(speed_bps or 0)
            _total = int(getattr(ts, "total_bytes", 0) or 0)
        except (TypeError, ValueError):
            _p, _bps, _total = None, 0, 0
        if _p is not None and _bps > 0 and _total > 0 and 0.0 <= _p < 1.0:
            _eta = _eta_human(int((_total * (1.0 - _p)) / _bps))
            if _eta:
                parts.append(f"ETA {_eta}")
        state_text = " · ".join(parts)
    elif ts.state == State.QUEUED:
        state_text = "📋 Queued"
    elif ts.state == State.MOVING:
        if _bd:
            state_text = f"📦 Moving leftovers · {_bd}"
        else:
            state_text = "📦 Moving"
        # Live rclone line when a move is actually pumping stats for
        # this row (stale/absent entries render the plain badge).
        try:
            _mp_pct = float(getattr(move_progress, "percent", 0.0) or 0.0)
            _mp_spd = int(getattr(move_progress, "speed_bps", 0) or 0)
            _mp_eta = int(getattr(move_progress, "eta_seconds", 0) or 0)
            _mp_age = time.monotonic() - float(getattr(move_progress, "updated_at", 0.0) or 0.0)
            _mp_fresh = 0.0 <= _mp_age < 120.0
        except Exception:
            _mp_pct, _mp_spd, _mp_eta, _mp_fresh = 0.0, 0, 0, False
        if _mp_fresh and (_mp_pct > 0 or _mp_spd > 0):
            _mp_parts = [f"{_mp_pct:.0f}%", _list_bar(_mp_pct / 100.0)]
            _mp_rate = _speed_human(_mp_spd)
            if _mp_rate:
                _mp_parts.append(_mp_rate)
            _mp_eta_h = _eta_human(_mp_eta)
            if _mp_eta_h:
                _mp_parts.append(f"ETA {_mp_eta_h}")
            state_text += " · " + " · ".join([p for p in _mp_parts if p])
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
        # replaces the stale miss count while it holds. A live retry
        # timer appends its countdown — positions would lie (waits are
        # timer/election-based, not FIFO), but the timer is exact.
        try:
            _retry_s = _retry_in_future_seconds(ts.indexer_next_retry_at)
            _retry = f" · retry in {max(1, round(_retry_s / 60))}m" if _retry_s else ""
        except Exception:
            _retry = ""
        state_text = (
            f"⏳ {_esc(note)}{_retry}"
            if note
            else f"⏳ Wait indexer miss #{ts.indexer_attempts}{_retry}"
        )
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
        _key = _active_group_key(_name0, getattr(_ts, "total_bytes", 0))
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
        for _key, _members in _group_active_items([(_r, None) for _r in rows or []]):
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
        return _tracker_domain(ts.source_announce_url) or _tracker_domain(ts.source_tracker) or ""
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
        return _short_tracker_label(_member_domain(ts)) or short_fallback or "?"
    except Exception:
        return short_fallback or "?"


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
                _note = (notes or {}).get(_h, "") if isinstance(notes, dict) else ""
                if getattr(_ts, "state", None) == State.NEW and _is_grace_note_for_prefer(_note):
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


def render_active(
    active: list[tuple[TorrentState, float | None]],
    page: int = 0,
    page_size: int = 5,
    notes: dict[str, str] | None = None,
    footer: str = "",
    speeds: dict[str, int] | None = None,
    move_progress: dict[str, object] | None = None,
) -> tuple[str, int, int]:
    """Render paginated active tasks, grouped by file for mobile width.

    Same-content copies (one release, many trackers) share one numbered
    heading ``N. `Name` · size``; each tracker gets a display-only line
    ``▸ <domain> <stage>`` (downloading rows add bar + rate + ETA when
    `speeds` maps their lowercase infohash to bytes/s). Each group
    carries one command (``/act_3``)
    opening the action sheet with only the currently eligible actions;
    the typed verbs (``/cancel_`` / ``/now_`` / ``/skip_`` / ``/resume_``
    / ``/injectfuse_`` with the same number or hash) all keep working
    as shortcuts. Detail cards keep per-torrent text commands
    (full-hash cancel). Pages count groups; numbers are global
    across pages.

    Compact mobile layout (detail cards stay fully detailed). `notes`
    maps source_infohash -> one-line extra shown on that tracker's line.
    `move_progress` maps lowercase infohash -> RcloneProgress for MOVING
    rows (live rclone line); absent/stale entries render plainly.

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
        for ts, progress in members:
            domain_full = _tracker_domain(ts.source_announce_url) or _tracker_domain(
                ts.source_tracker
            )
            _note = _active_note(ts, notes)
            try:
                _spd = (speeds or {}).get((ts.source_infohash or "").lower(), 0)
            except Exception:
                _spd = 0
            try:
                _mp = (move_progress or {}).get((ts.source_infohash or "").lower())
            except Exception:
                _mp = None
            state_text = _active_state_text(ts, progress, _note, _spd, move_progress=_mp)
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
