"""Telegram command + callback protocol (patterns, handlers, parsing).

Split out of the telegram bot god-file. Everything here is pure
(matchers, button-data shapes, flood math) except the ``_cmd_*``
handlers, which are unbound ``async (bot, match, message)`` functions
duck-typing the bot surface (`_start_group_command`,
`_resume_command_entry`, `_reply`, ...). No import of telegram_bot,
so the dependency runs one way: bot -> protocol.
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import logging
import re
from typing import Any, NamedTuple

from telegram import BotCommand
from telegram.error import NetworkError, RetryAfter, TimedOut

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
#: (Defined ahead of the command registry, which references it.)
CANCEL_MATCH_RE = re.compile(r"^/cancel_match(?:@[\w_]+)?(?:\s+(.+?))?\s*$")

#: `/cancel_stalled` — bulk cancel every live group whose download is
#: stalled (DOWNLOADING with 0 seeders and incomplete progress, per the
#: dest client at tap time). Optional `@bot` suffix; no argument.
CANCEL_STALLED_RE = re.compile(r"^/cancel_stalled(?:@[\w_]+)?\s*$")

#: Operator pause/resume for intake of NEW work (in-flight rows drain
#: normally; the flag resets on restart). Optional `@bot` suffix.
PAUSE_INTAKE_RE = re.compile(r"^/pause_intake(?:@[\w_]+)?\s*$")
RESUME_INTAKE_RE = re.compile(r"^/resume_intake(?:@[\w_]+)?\s*$")

#: `/tick` — run one full pipeline tick now (poll + schedule + janitor).
#: Optional `@bot` suffix; no argument.
TICK_RE = re.compile(r"^/tick(?:@[\w_]+)?\s*$")

#: `/stats` — pipeline snapshot (rows by state, workers, intake, ignored,
#: storage footer). Optional `@bot` suffix; no argument.
STATS_RE = re.compile(r"^/stats(?:@[\w_]+)?\s*$")

#: `/find <text>` — read-only Prowlarr search over the download-target
#: indexers (titles + sizes, nothing queued or downloaded). Optional
#: `@bot` suffix; the text runs to end of line.
FIND_RE = re.compile(r"^/find(?:@[\w_]+)?(?:\s+(.+?))?\s*$")

#: `/files_<n|hex>` — read-only file listing for a group (group number,
#: tracked hash/prefix, or group id, like the other group commands).
FILES_RE = re.compile(r"^/files_([0-9a-fA-F]+)(?:@[\w_]+)?\b")


def _scrubbed(e: object) -> str:
    """Sanitized exception text (no `Action failed:` prefix)."""
    try:
        from .logging_setup import sanitize_log_text
        return sanitize_log_text(str(e))
    except Exception:
        try:
            return type(e).__name__
        except Exception:
            return "error"


def _action_error_text(e: object) -> str:
    """User-facing failure line with secrets scrubbed.

    Exception text can carry tracker passkeys, tokens, passwords, and
    full announce URLs — none of which may reach the chat. Falls back
    to a bare line when the sanitizer itself is unavailable.
    """
    try:
        return f"Action failed: {_scrubbed(e)}"
    except Exception:
        return "Action failed."


class _ChatCommand(NamedTuple):
    """One typed chat command: name, matcher, and dispatch handler.

    Adding a command is one table row below — matching *is* the gate,
    so no separate "did we forget this pattern" check exists to drift
    (the class of bug that once left `forget:` taps unrouted).
    `handle` is an unbound ``async (bot, match, message)`` function.
    """
    name: str
    pattern: re.Pattern
    handle: Any


def _cmd_group(kind: str):
    """Handler factory: ``/verb_<token>`` via the group command."""
    async def _run(bot, match, message):
        await bot._start_group_command(kind, match.group(1), message)
    _run.__name__ = f"_cmd_group_{kind}"
    return _run


async def _cmd_resume(bot, match, message):
    """`/unskip_` / `/resume_` / `/unignore_` / `/unignore <hash>`."""
    await bot._resume_command_entry(match.group(1).lower(), message)


async def _cmd_single_injectfuse(bot, match, message):
    """`/injectfuse <full-hash>` (manual form)."""
    await bot._start_single_command(
        "injectfuse", match.group(1).lower(), message)


async def _cmd_act(bot, match, message):
    """`/act_<n|hex>` action sheet."""
    await bot._act_command_entry(match.group(1).lower(), message)


async def _cmd_cancel_match(bot, match, message):
    """`/cancel_match <text>` bulk cancel (empty text shows usage)."""
    await bot._cancel_match_command_entry(match.group(1) or "", message)


async def _cmd_cancel_stalled(bot, match, message):
    """`/cancel_stalled` bulk cancel of seederless downloads."""
    await bot._cancel_stalled_command_entry(message)


async def _cmd_pause_intake(bot, match, message):
    """`/pause_intake`: stop starting NEW work until resumed."""
    coord = getattr(bot, "_coord", None)
    setter = getattr(coord, "set_intake_paused", None)
    if coord is None or not callable(setter):
        try:
            await bot._reply("Action failed: bot not attached.",
                             reply_to=message)
        except Exception:
            pass
        return
    try:
        setter(True)
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass
        return
    try:
        await bot._reply(
            "Intake paused — no new downloads start until "
            "/resume_intake. In-flight rows drain normally "
            "(resets on restart).",
            reply_to=message)
    except Exception:
        pass


async def _cmd_resume_intake(bot, match, message):
    """`/resume_intake`: intake of NEW work resumes."""
    coord = getattr(bot, "_coord", None)
    setter = getattr(coord, "set_intake_paused", None)
    if coord is None or not callable(setter):
        try:
            await bot._reply("Action failed: bot not attached.",
                             reply_to=message)
        except Exception:
            pass
        return
    try:
        setter(False)
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass
        return
    try:
        await bot._reply("Intake resumed — NEW rows start workers again.",
                         reply_to=message)
    except Exception:
        pass


async def _cmd_tick(bot, match, message):
    """`/tick`: run one full pipeline tick now."""
    coord = getattr(bot, "_coord", None)
    step = getattr(coord, "_tick_inner", None)
    if coord is None or not callable(step):
        try:
            await bot._reply("Action failed: bot not attached.",
                             reply_to=message)
        except Exception:
            pass
        return
    try:
        await step()
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass
        return
    try:
        await bot._reply("Tick done.", reply_to=message)
    except Exception:
        pass
    try:
        bot._last_active_cache = None
        await bot._refresh_active_message()
    except Exception:
        pass


_STATS_ORDER = (
    ("new", "NEW"), ("queued", "QUEUED"), ("downloading", "DL"),
    ("moving", "MOVING"), ("waiting_indexer", "WAIT-IDX"),
    ("waiting_disk", "WAIT-SSD"), ("re_adding", "RE-ADDING"),
    ("done", "DONE"), ("failed", "FAILED"),
)


async def _cmd_stats(bot, match, message):
    """`/stats`: rows by state, workers, intake, ignored, storage."""
    store = getattr(bot, "_store", None)
    if store is None:
        try:
            await bot._reply("Action failed: bot not attached.",
                             reply_to=message)
        except Exception:
            pass
        return
    try:
        _all = getattr(store, "all", None)
        rows = list(_all() or []) if callable(_all) else []
    except Exception:
        rows = []
    counts: dict[str, int] = {}
    skipped = 0
    for r in rows:
        try:
            _st = getattr(getattr(r, "state", None), "value", None)
            if _st:
                counts[_st] = counts.get(_st, 0) + 1
            if int(getattr(r, "skipped", 0) or 0):
                skipped += 1
        except Exception:
            continue
    _parts = [f"{lbl} {counts.get(val, 0)}" for (val, lbl) in _STATS_ORDER]
    try:
        _tasks = getattr(getattr(bot, "_coord", None), "_tasks", None)
        _workers = len(_tasks) if isinstance(_tasks, (set, list)) else 0
    except Exception:
        _workers = 0
    try:
        _paused = bool(getattr(getattr(bot, "_coord", None),
                               "_intake_paused", False))
    except Exception:
        _paused = False
    try:
        _ign = getattr(store, "list_ignored", None)
        _ignored = len(_ign() or []) if callable(_ign) else 0
    except Exception:
        _ignored = 0
    _lines = ["📊 Stats",
              " · ".join(_parts),
              f"workers {_workers} · intake "
              f"{'paused' if _paused else 'running'} · held {skipped} · "
              f"ignored {_ignored}"]
    try:
        _ovfn = getattr(bot, "_overview_line", None)
        _ovl = _ovfn() if callable(_ovfn) else ""
    except Exception:
        _ovl = ""
    if _ovl:
        _lines.append(_ovl)
    try:
        _footer = await bot._active_footer()
    except Exception:
        _footer = ""
    if _footer:
        _lines.append(_footer)
    # Live diagnostics tail: the last warnings/errors from the in-memory
    # ring buffer, so /stats shows what the logs say without ssh. Best
    # effort — an unwired buffer simply yields no tail.
    try:
        from .logging_setup import get_ring_buffer
        _events = get_ring_buffer().snapshot(last=3,
                                             min_level=logging.WARNING)
    except Exception:
        _events = []
    for _ev in _events or []:
        try:
            _emsg = str(getattr(_ev, "message", "") or "")[:140]
            _elog = str(getattr(_ev, "logger", "") or "").split(".")[-1]
            if _emsg:
                _lines.append(f"⚠️ {_elog}: {_emsg}")
        except Exception:
            continue
    try:
        await bot._reply("\n".join(_lines)[:1000], reply_to=message)
    except Exception:
        pass


async def _cmd_add(bot, match, message):
    """`/add` document ingest (acts on the message, not the token)."""
    await bot._process_document_command(message)


async def _cmd_find(bot, match, message):
    """`/find <text>`: read-only Prowlarr search readout."""
    from .telegram_views import _size_compact

    q = ((match.group(1) if match else "") or "").strip()
    if not q:
        try:
            await bot._reply("Send /find <release name> to search the "
                             "download-target indexers (read-only).",
                             reply_to=message)
        except Exception:
            pass
        return
    coord = getattr(bot, "_coord", None)
    finder = getattr(coord, "find_release", None)
    if coord is None or not callable(finder):
        try:
            await bot._reply("Action failed: bot not attached.",
                             reply_to=message)
        except Exception:
            pass
        return
    try:
        res = await finder(q)
    except LookupError as e:
        try:
            await bot._reply(_scrubbed(e)[:300], reply_to=message)
        except Exception:
            pass
        return
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass
        return
    try:
        _idx = res.get("indexers") or []
        _total = sum(i.get("total", 0) for i in _idx)
        if not _total:
            await bot._reply(
                f"No hits for '{q[:60]}' on {len(_idx)} indexer(s).",
                reply_to=message)
            return
        _lines = [f"🔍 '{q[:60]}' — {_total} hit(s) "
                   f"on {len(_idx)} indexer(s):"]
        for entry in _idx:
            _hs = entry.get("hits") or []
            if not _hs:
                continue
            for h in _hs:
                try:
                    _t = str(h.get("title", "") or "")[:80]
                    _s = _size_compact(h.get("size_bytes", 0))
                except Exception:
                    continue
                _lines.append(f"• {entry.get('name', '?')}: {_t} ({_s})")
            if entry.get("total", 0) > len(_hs):
                _lines.append(
                    f"  +{entry['total'] - len(_hs)} more on "
                    f"{entry.get('name', '?')}")
        _lines.append("Read-only — nothing queued.")
        await bot._reply("\n".join(_lines)[:1500], reply_to=message)
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass


#: Chat command registry, in dispatch order. First match wins (patterns
#: are mutually exclusive by construction — see the registry test).
_CHAT_COMMANDS: tuple = (
    _ChatCommand("add", ADD_CMD_RE, _cmd_add),
    _ChatCommand("fetch", FETCH_CMD_RE, _cmd_group("fetch")),
    _ChatCommand("prefer", PREFER_CMD_RE, _cmd_group("prefer")),
    _ChatCommand("now", NOW_CMD_RE, _cmd_group("now")),
    _ChatCommand("skip", SKIP_CMD_RE, _cmd_group("skip")),
    _ChatCommand("unskip", UNSKIP_CMD_RE, _cmd_resume),
    _ChatCommand("resume", RESUME_CMD_RE, _cmd_resume),
    _ChatCommand("unignore", UNIGNORE_CMD_RE, _cmd_resume),
    _ChatCommand("unignore_hash", UNIGNORE_HASH_RE, _cmd_resume),
    _ChatCommand("injectfuse_hash", INJECTFUSE_HASH_RE,
                 _cmd_single_injectfuse),
    _ChatCommand("injectfuse", INJECTFUSE_CMD_RE, _cmd_group("injectfuse")),
    _ChatCommand("act", ACT_CMD_RE, _cmd_act),
    _ChatCommand("cancel_match", CANCEL_MATCH_RE, _cmd_cancel_match),
    _ChatCommand("cancel_stalled", CANCEL_STALLED_RE, _cmd_cancel_stalled),
    _ChatCommand("pause_intake", PAUSE_INTAKE_RE, _cmd_pause_intake),
    _ChatCommand("resume_intake", RESUME_INTAKE_RE, _cmd_resume_intake),
    _ChatCommand("tick", TICK_RE, _cmd_tick),
    _ChatCommand("stats", STATS_RE, _cmd_stats),
    _ChatCommand("find", FIND_RE, _cmd_find),
    _ChatCommand("files", FILES_RE, _cmd_group("files")),
    _ChatCommand("cancel", CANCEL_CMD_RE, _cmd_group("cancel")),
)


#: Shortest substring worth matching: 1 char matches nearly everything.
_MATCH_MIN_CHARS = 2
#: Longest query (UTF-8 bytes) whose base64url token still fits the
#: 64-byte button budget inside `keep:match:<tok>:1:yes`.
_MATCH_MAX_BYTES = 35
#: Valid token chars (base64url, padding stripped at encode time).
_MATCH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,47}$")


def _is_bulk_scope(h: str) -> bool:
    """True for re-resolved bulk scopes (`match:<tok>`, `stalled`).

    Bulk scopes carry no frozen hash list: every tap re-resolves the
    live set, so no stored pending state exists to expire.
    """
    try:
        return h == "stalled" or h.startswith("match:")
    except Exception:
        return False


class ScopeGone(Exception):
    """A cancel scope resolved empty — carries the user-facing reply."""

    def __init__(self, reply: str = "Already gone from tracking"):
        super().__init__(reply)
        self.reply = reply


class ResolvedScope(NamedTuple):
    """Live cancel scope: Q2 title, Q2 scope label, button scope, titles."""

    title_raw: str
    qlabel: str
    scope: str
    etitle_raw: str
    hashes: list[str]


class CancelScope(NamedTuple):
    """Declarative cancel scope: one model for all four Q1/Q2/execute paths.

    Kinds: `single` (one hash), `group` (leader hash, all live copies),
    `match` (base64url title substring), `stalled` (0-seeder predicate).
    `resolve` re-derives the live set on every call (raising
    :class:`ScopeGone` with the kind's reply when empty, ``RuntimeError``
    on infrastructure failure); `remember_sheet` builds the identical
    Q1 question for every scope. Callers pass subjects pre-sanitized —
    titles stay raw here so each renderer keeps its own convention.
    """

    kind: str
    key: str

    @classmethod
    def from_parsed(cls, all: bool, h: str) -> CancelScope:
        """Scope from a parsed callback (`_parse_action_data` output)."""
        if _is_bulk_scope(h):
            if h == "stalled":
                return cls("stalled", "")
            return cls("match", h[len("match:"):])
        if all:
            return cls("group", h)
        return cls("single", h)

    def to_data(self) -> str:
        """Button scope string (round-trips through the callback parser)."""
        if self.kind == "group":
            return f"all:{self.key}"
        if self.kind == "match":
            return f"match:{self.key}"
        if self.kind == "stalled":
            return "stalled"
        return self.key

    async def resolve(self, bot: Any) -> ResolvedScope:
        """Live (title, label, scope, execute-title, hashes) for the scope."""
        store = getattr(bot, "_store", None)
        if self.kind == "single":
            get = getattr(store, "get", None) if store else None
            try:
                row = await asyncio.to_thread(get, self.key) \
                    if callable(get) else None
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"resolve failed: {e}") from e
            if row is None:
                raise ScopeGone()
            title = str(getattr(row, "source_name", "")
                        or self.key[:10])[:60]
            return ResolvedScope(title, "", self.key, title, [self.key])
        if self.kind == "group":
            fn = getattr(bot, "_live_group_hashes", None)
            try:
                hashes = await asyncio.to_thread(fn, self.key) \
                    if callable(fn) else []
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"resolve failed: {e}") from e
            if not hashes:
                raise ScopeGone("Group changed — tap /act again.")
            title = self.key[:10]
            try:
                get = getattr(store, "get", None) if store else None
                leader = get(self.key) if callable(get) else None
                if leader is not None:
                    title = str(getattr(leader, "source_name", "")
                                or self.key[:10])[:60]
            except Exception:
                pass
            return ResolvedScope(
                title, f" · all {len(hashes)} copies",
                f"all:{self.key}", title, list(hashes))
        if self.kind == "match":
            return await self._resolve_match(bot)
        if self.kind == "stalled":
            fn = getattr(bot, "_live_stalled", None)
            if not callable(fn):
                raise RuntimeError("stalled resolve unavailable")
            groups = await fn()
            if not groups:
                raise ScopeGone()
            total = sum(len(hs) for (_, hs) in groups)
            return ResolvedScope(
                f"{len(groups)} stalled downloads",
                f" · {total} copies", "stalled",
                f"{len(groups)} stalled groups",
                [h for (_, hs) in groups for h in hs or []])
        raise ScopeGone()

    async def _resolve_match(self, bot: Any) -> ResolvedScope:
        """Match-scope resolution (substring token in :attr:`key`)."""
        q = _match_query(self.key)
        fn = getattr(bot, "_live_match", None)
        try:
            matches = await asyncio.to_thread(fn, q) \
                if (callable(fn) and q) else []
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"match resolve failed: {e}") from e
        if not matches:
            raise ScopeGone()
        total = sum(len(hs) for (_, hs) in matches)
        return ResolvedScope(
            f"{len(matches)} groups matching '{q[:40]}'",
            f" · {total} copies", f"match:{self.key}",
            f"{len(matches)} matched groups",
            [h for (_, hs) in matches for h in hs or []])

    def remember_sheet(self, subject: str) -> tuple[str, list]:
        """Q1 remember question for the scope (subject pre-sanitized)."""
        from .telegram_buttons import ButtonMaker

        scope = self.to_data()
        text = (f"{subject}\n"
                "🚫 Ignore blocks it from coming back; "
                "👻 Just forget allows re-adding it later.")
        rows = (ButtonMaker()
                .add("🚫 Ignore + forget", f"forget:{scope}:1")
                .add("👻 Just forget", f"forget:{scope}:0")
                .newline().add("Close", "abort").rows())
        return text, rows


def _slow_tap(data: str) -> bool:
    """True for taps whose execution is slow enough to need feedback.

    Delete-files keeps and confirmed injects run client deletes,
    verifications, and wipes (10s+ against a flaky client) while their
    sheet stays live with working buttons — the operator needs a
    working state on screen, or they re-tap into the in-flight guard
    (or worse, before it existed, into a duplicate execution).
    """
    try:
        return (data.startswith("keep:") and data.endswith(":no")) or \
            (data.startswith("inject:") and data.endswith(":yes"))
    except Exception:
        return False


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


#: Flood-wait safety margin: sleeping the exact requested window
#: re-triggers the ban on clock skew or a queued duplicate, so every
#: parsed/default wait is stretched (mirror-leech-telegram-bot
#: practice is `value * 1.2` on every send/edit path).
_FLOOD_MARGIN = 1.2


def _with_flood_margin(base: object) -> int:
    """Stretch a parsed flood wait by the safety margin (+1s headroom)."""
    try:
        return max(1, int(float(base or 0) * _FLOOD_MARGIN) + 1)
    except (TypeError, ValueError):
        return 1


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
                return _with_flood_margin(raw.total_seconds())
            return _with_flood_margin(raw)
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
            return _with_flood_margin(int(m.group(1)))
    except Exception:
        pass
    return _with_flood_margin(default)


def _is_parse_error(e: BaseException) -> bool:
    """True iff a Telegram error is a Markdown parse/entity failure.

    Those (and only those) are retried as plain text; every other error
    keeps its own handling at each call site.
    """
    msg = str(e).lower()
    return "can't parse" in msg or "entity" in msg


#: How long a VPS1 free-space probe stays valid for the active-tasks
#: footer. Refresh ticks every status_update_interval (45s default); a
#: probe per tick would chatter the SFTP channel for a footer line.
_VPS1_FREE_TTL_S = 300.0
#: Local SSD free-stat TTL for the active-list footer: display-only, so a
#: burst of taps shares one stat instead of each paying a syscall.
_SSD_FREE_TTL_S = 120.0


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


def _is_hex40(s: object) -> bool:
    """True for a full 40-char lowercase hex infohash."""
    try:
        t = str(s or "").strip().lower()
    except Exception:
        return False
    return len(t) == 40 and all(c in "0123456789abcdef" for c in t)


def _parse_action_data(data: str) -> tuple[str, bool, str, str, bool | None]:
    """Parse a hash-protocol callback into (cmd, all, hash, choice, remember).

    Shapes: `<cmd>:<40hex>`, `<cmd>:all:<40hex>`,
    `<cmd>:<40hex>:yes|no`, `<cmd>:all:<40hex>:yes|no` (keep/inject),
    `forget:<scope>`, `forget:<scope>:<0|1>`,
    `keep:<scope>:<0|1>:<yes|no>`, `abort`, bulk-match scopes
    `forget:match:<tok>:<0|1>` / `keep:match:<tok>:<0|1>:<yes|no>`
    (`<tok>` = base64url query, `match:<tok>` in the hash slot) and the
    predicate scope `forget:stalled:<0|1>` /
    `keep:stalled:<0|1>:<yes|no>` (`stalled` in the hash slot).
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
        # Predicate scope: fixed predicate, no token to carry.
        if len(parts) == 3 and parts[0] == "forget":
            if parts[1] == "stalled" and parts[2] in ("0", "1"):
                return (parts[0], False, "stalled", "",
                        parts[2] == "1")
        if len(parts) == 4 and parts[0] == "keep":
            if parts[1] == "stalled" and parts[2] in ("0", "1") \
                    and parts[3] in ("yes", "no"):
                return (parts[0], False, "stalled", parts[3],
                        parts[2] == "1")
    except Exception:
        pass
    return ("", False, "", "", None)


def _bot_command_menu() -> list:
    """Commands shown in the chat's `/` popup (Bot API menu).

    Listed commands take no ID the operator won't know (`/add` acts on
    the message, `/cancel_match` takes free text, the rest take none).
    The pickers need group numbers or hashes, so they stay unlisted to
    keep the menu short.
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
        ), BotCommand(
            "cancel_stalled",
            "Cancel every downloading group with no seeders — "
            "/cancel_stalled",
        ), BotCommand(
            "pause_intake",
            "Pause intake — no new downloads start until /resume_intake",
        ), BotCommand(
            "resume_intake",
            "Resume intake of new downloads",
        ), BotCommand(
            "tick",
            "Run one pipeline tick now (poll + schedule + janitor)",
        ), BotCommand(
            "stats",
            "Pipeline snapshot — rows by state, workers, storage",
        ), BotCommand(
            "find",
            "Search download-target indexers (read-only) — /find <text>",
        ), BotCommand(
            "files",
            "List a torrent's files (read-only) — /files_<n|hash>",
        )]
    except Exception:
        return []
