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

# --------------------------------------------------------------------------- #
# Command patterns
#
# One pattern per verb, accepting BOTH separators:
#   `/verb <target>`   — what you type (a space is easier to type)
#   `/verb_<target>`   — the same command with an underscore
# The target is a tracked hash/prefix or a group number from the card.
# --------------------------------------------------------------------------- #

#: `/cancel <target>` — cancel one release (hash, prefix or group number).
CANCEL_CMD_RE = re.compile(r"^/cancel(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/cancel_all <text>` — cancel every live group whose title contains
#: `<text>` (case-insensitive), after one remember + one keep/delete
#: question. Legacy alias: `/cancel_all`.
CANCEL_ALL_RE = re.compile(r"^/cancel_all(?:@[\w_]+)?(?:\s+(.+?))?\s*$")

#: `/cancel_lane` — the lane sheet (DL/MV/RD/QD) to cancel a whole lane.
#: A bare `/cancel` opens the same sheet. Legacy alias: `/cancel_lane`.
CANCEL_LANE_RE = re.compile(r"^/(?:cancel|cancel_lane)(?:@[\w_]+)?\s*$")

#: `/cancel_stalled` — cancel every group whose download is stalled
#: (downloading, no seeders, incomplete). No argument.
CANCEL_STALLED_RE = re.compile(r"^/cancel_stalled(?:@[\w_]+)?\s*$")

#: `/start <target>` — start now instead of waiting: a WAITING_INDEXER row
#: uses its starting torrent instead of waiting out Prowlarr, a NEW
#: grace-held row starts its SSD download at once. Legacy aliases:
#: `/now`, `/fetch`, `/prefer`.
START_CMD_RE = re.compile(r"^/start(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/force <target>` — start past the lane caps for a queued/moving row or
#: group (in-memory, disarms on lane exit or restart). Legacy alias:
#: `/force`.
FORCE_CMD_RE = re.compile(r"^/force(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/hold <target>` — hold a tracked release: no workers, no Prowlarr
#: queries, no downloads, no moves until `/unhold`. The row and its state
#: are kept. Legacy alias: `/skip`.
HOLD_CMD_RE = re.compile(r"^/hold(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/unhold <hash>` — resume a held release. A full 40-char hash also
#: lifts a cancelled (ignored) release, so it tracks again. Legacy
#: aliases: `/resume`, `/unskip`.
UNHOLD_CMD_RE = re.compile(r"^/unhold(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/unignore <hash>` — drop a cancelled release from the ignore list
#: (plus its forget tombstone) so it can be tracked again. Full hash
#: only: ignored rows are invisible in the list.
UNIGNORE_CMD_RE = re.compile(r"^/unignore(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/inject <target>` — manual fuse seeding: you moved the bytes to the
#: remote yourself. Legacy alias: `/inject`.
INJECT_CMD_RE = re.compile(r"^/inject(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/act <target>` — action sheet for a content group: one entry point
#: showing only the currently eligible actions (hash-direct buttons).
ACT_CMD_RE = re.compile(r"^/act(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/files <target>` — read-only file listing for one torrent/group.
FILES_RE = re.compile(r"^/files(?:_|\s+)([0-9a-fA-F]+)(?:@[\w_]+)?\b")

#: `/pause` — stop starting NEW work; in-flight rows drain normally.
#: Legacy alias: `/pause`.
PAUSE_RE = re.compile(r"^/pause(?:@[\w_]+)?\s*$")

#: `/resume` — resume intake of NEW work. The per-release resume is
#: `/unhold <hash>`.
RESUME_INTAKE_RE = re.compile(r"^/resume(?:@[\w_]+)?\s*$")


#: `/add` — ingest the .torrent file in the replied-to message (or in
#: the same message when sent with /add as caption) as a Telegram
#: origin drop, processed like a watch-dir file. Optional `@bot` suffix,
#: extra trailing text ignored.
ADD_CMD_RE = re.compile(r"^/add(?:@[\w_]+)?\b")

#: `/tick` — run one full pipeline tick now (poll + schedule + janitor).
#: Optional `@bot` suffix; no argument.
TICK_RE = re.compile(r"^/tick(?:@[\w_]+)?\s*$")

#: `/active` — send a fresh Active Tasks card now (recovery when the
#: long-lived card went missing or was deleted from the chat).
#: Optional `@bot` suffix; no argument.
ACTIVE_RE = re.compile(r"^/active(?:@[\w_]+)?\s*$")

#: `/stats` — pipeline snapshot (rows by state, workers, intake, ignored,
#: storage footer). Optional `@bot` suffix; no argument.
STATS_RE = re.compile(r"^/stats(?:@[\w_]+)?\s*$")

#: `/find <text>` — read-only Prowlarr search over the download-target
#: indexers (titles + sizes, nothing queued or downloaded). Optional
#: `@bot` suffix; the text runs to end of line.
FIND_RE = re.compile(r"^/find(?:@[\w_]+)?(?:\s+(.+?))?\s*$")


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
    """`/unhold_` / `/unignore_` / `/unignore <hash>`."""
    await bot._resume_command_entry(match.group(1).lower(), message)


async def _cmd_inject(bot, match, message):
    """`/inject <target>`: manual fuse seeding (you moved the bytes).

    A tracked group (number or hash prefix) gets the group sheet; a hash
    that is not tracked is injected directly, so any VPS1 infohash works.
    """
    token = (match.group(1) or "").strip().lower()
    try:
        rows = await asyncio.to_thread(bot._store.list_active_inflight)
    except Exception:
        rows = []
    try:
        members, _live_n, _by_number = bot._resolve_group_token(rows, token)
    except Exception:
        members = []
    if members:
        await bot._start_group_command("injectfuse", token, message)
        return
    await bot._start_single_command("injectfuse", token, message)


async def _cmd_act(bot, match, message):
    """`/act_<n|hex>` action sheet."""
    await bot._act_command_entry(match.group(1).lower(), message)


async def _cmd_cancel_match(bot, match, message):
    """`/cancel_all <text>` bulk cancel (empty text shows usage)."""
    await bot._cancel_match_command_entry(match.group(1) or "", message)


async def _cmd_cancel_stalled(bot, match, message):
    """`/cancel_stalled` bulk cancel of seederless downloads."""
    await bot._cancel_stalled_command_entry(message)


async def _cmd_cancel_status(bot, match, message):
    """`/cancel_lane` lane menu."""
    await bot._cancel_status_command_entry(message)


async def _cmd_pause_intake(bot, match, message):
    """`/pause`: stop starting NEW work until resumed."""
    coord = getattr(bot, "_coord", None)
    setter = getattr(coord, "set_intake_paused", None)
    if coord is None or not callable(setter):
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
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
            "/resume. In-flight rows drain normally "
            "(resets on restart).",
            reply_to=message,
        )
    except Exception:
        pass


async def _cmd_resume_intake(bot, match, message):
    """`/resume`: intake of NEW work resumes."""
    coord = getattr(bot, "_coord", None)
    setter = getattr(coord, "set_intake_paused", None)
    if coord is None or not callable(setter):
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
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
        await bot._reply("Intake resumed — NEW rows start workers again.", reply_to=message)
    except Exception:
        pass


async def _cmd_tick(bot, match, message):
    """`/tick`: run one full pipeline tick now, without blocking polling."""
    coord = getattr(bot, "_coord", None)
    step = getattr(coord, "_tick_inner", None)
    if coord is None or not callable(step):
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
        except Exception:
            pass
        return

    async def _run() -> None:
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

    # A full pass can take a minute (worker scheduling, janitor). Awaiting it
    # inline stalls polling for the whole pass and Telegram retries the
    # update; run it as a task and answer when it finishes. A second tap
    # answers instead of stacking another pass behind the locks.
    prev = getattr(bot, "_tick_task", None)
    if prev is not None and not prev.done():
        try:
            await bot._reply("Tick already running; its result will follow.", reply_to=message)
        except Exception:
            pass
        return
    try:
        bot._tick_task = asyncio.create_task(_run())
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass


async def _cmd_active(bot, match, message):
    """`/active`: send a fresh Active Tasks card now."""
    render = getattr(bot, "render_active_now", None)
    if not callable(render):
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
        except Exception:
            pass
        return
    try:
        ok = await render()
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass
        return
    try:
        await bot._reply(
            "Active tasks sent." if ok else "Active tasks could not be sent — see the log.",
            reply_to=message,
        )
    except Exception:
        pass


_STATS_ORDER = (
    ("new", "NEW"),
    ("queued", "QUEUED"),
    ("downloading", "DL"),
    ("moving", "MOVING"),
    ("waiting_indexer", "WAIT-IDX"),
    ("waiting_disk", "WAIT-SSD"),
    ("re_adding", "RE-ADDING"),
    ("done", "DONE"),
    ("failed", "FAILED"),
)


async def _cmd_stats(bot, match, message):
    """`/stats`: rows by state, workers, intake, ignored, storage."""
    store = getattr(bot, "_store", None)
    if store is None:
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
        except Exception:
            pass
        return
    try:
        _all = getattr(store, "all", None)
        # A full-table read must not run on the polling loop.
        rows = list(await asyncio.to_thread(_all) or []) if callable(_all) else []
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
        _paused = bool(getattr(getattr(bot, "_coord", None), "_intake_paused", False))
    except Exception:
        _paused = False
    try:
        _ign = getattr(store, "list_ignored", None)
        _ignored = len(_ign() or []) if callable(_ign) else 0
    except Exception:
        _ignored = 0
    _lines = [
        "📊 Stats",
        " · ".join(_parts),
        f"workers {_workers} · intake "
        f"{'paused' if _paused else 'running'} · held {skipped} · "
        f"ignored {_ignored}",
    ]
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

        _events = get_ring_buffer().snapshot(last=3, min_level=logging.WARNING)
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
            await bot._reply(
                "Send /find <release name> to search the download-target indexers (read-only).",
                reply_to=message,
            )
        except Exception:
            pass
        return
    coord = getattr(bot, "_coord", None)
    finder = getattr(coord, "find_release", None)
    if coord is None or not callable(finder):
        try:
            await bot._reply("Action failed: bot not attached.", reply_to=message)
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
            await bot._reply(f"No hits for '{q[:60]}' on {len(_idx)} indexer(s).", reply_to=message)
            return
        _lines = [f"🔍 '{q[:60]}' — {_total} hit(s) on {len(_idx)} indexer(s):"]
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
                _lines.append(f"  +{entry['total'] - len(_hs)} more on {entry.get('name', '?')}")
        _lines.append("Read-only — nothing queued.")
        await bot._reply("\n".join(_lines)[:1500], reply_to=message)
    except Exception as e:  # noqa: BLE001
        try:
            await bot._reply(_action_error_text(e), reply_to=message)
        except Exception:
            pass


#: Usage line per target-taking verb. The slash menu advertises the bare
#: verb, so a bare `/start` must answer with how to use it instead of
#: vanishing (the operator sees the name in the popup and hits enter).
_COMMAND_USAGE: dict[str, str] = {
    "start": "/start <group number or hash> — start it now instead of waiting",
    "force": "/force <group number or hash> — start it past the lane caps",
    "hold": "/hold <group number or hash> — hold it (no work until unheld)",
    "unhold": "/unhold <hash> — resume a held release",
    "unignore": "/unignore <full hash> — track a cancelled release again",
    "inject": "/inject <hash> — inject a torrent you moved to the remote",
    "act": "/act <group number or hash> — open the action sheet",
    "files": "/files <group number or hash> — list its files (read-only)",
    "cancel_all": "/cancel_all <text> — cancel every group whose title contains the text",
    "find": "/find <text> — search the download-target indexers (read-only)",
}


def _command_usage_for(text: str) -> str | None:
    """Usage line for a bare command word, or None when it is not one."""
    try:
        raw = str(text or "").strip()
        if not raw.startswith("/"):
            return None
        word = raw[1:].split("@", 1)[0].strip().lower()
        if not word or " " in word or "\t" in word:
            return None
        return _COMMAND_USAGE.get(word)
    except Exception:
        return None


#: Chat command registry, in dispatch order. First match wins (patterns
#: are mutually exclusive by construction — see the registry test).
#:
#: One entry per verb: each pattern also accepts the legacy spelling and
#: the underscore separator, so old bookmarks keep working while the
#: advertised surface stays small (one name per action).
_CHAT_COMMANDS: tuple = (
    _ChatCommand("add", ADD_CMD_RE, _cmd_add),
    _ChatCommand("cancel", CANCEL_CMD_RE, _cmd_group("cancel")),
    _ChatCommand("cancel_all", CANCEL_ALL_RE, _cmd_cancel_match),
    _ChatCommand("cancel_lane", CANCEL_LANE_RE, _cmd_cancel_status),
    _ChatCommand("cancel_stalled", CANCEL_STALLED_RE, _cmd_cancel_stalled),
    _ChatCommand("start", START_CMD_RE, _cmd_group("now")),
    _ChatCommand("force", FORCE_CMD_RE, _cmd_group("forcestart")),
    _ChatCommand("hold", HOLD_CMD_RE, _cmd_group("skip")),
    _ChatCommand("unhold", UNHOLD_CMD_RE, _cmd_resume),
    _ChatCommand("unignore", UNIGNORE_CMD_RE, _cmd_resume),
    _ChatCommand("inject", INJECT_CMD_RE, _cmd_inject),
    _ChatCommand("act", ACT_CMD_RE, _cmd_act),
    _ChatCommand("files", FILES_RE, _cmd_group("files")),
    _ChatCommand("find", FIND_RE, _cmd_find),
    _ChatCommand("pause", PAUSE_RE, _cmd_pause_intake),
    _ChatCommand("resume", RESUME_INTAKE_RE, _cmd_resume_intake),
    _ChatCommand("tick", TICK_RE, _cmd_tick),
    _ChatCommand("active", ACTIVE_RE, _cmd_active),
    _ChatCommand("stats", STATS_RE, _cmd_stats),
)


#: Shortest substring worth matching: 1 char matches nearly everything.
_MATCH_MIN_CHARS = 2
#: Longest query (UTF-8 bytes) whose base64url token still fits the
#: 64-byte button budget inside `keep:match:<tok>:1:yes`.
_MATCH_MAX_BYTES = 35
#: Valid token chars (base64url, padding stripped at encode time).
_MATCH_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,47}$")


#: Active-list status filters: key -> row states shown (`None` = all).
#: QD covers the queued/waiting lanes (QUEUED + NEW + WAITING_*).
_STATUS_FILTERS: dict[str, frozenset | None] = {
    "all": None,
    "dl": frozenset({"downloading"}),
    "mv": frozenset({"moving"}),
    "rd": frozenset({"re_adding"}),
    "qd": frozenset({"queued", "new", "waiting_disk", "waiting_indexer"}),
}
#: Filter keyboard buttons in order: (key, label). `ov` is the aggregate
#: overview (not a filter) and shares the `status:` callback namespace.
_STATUS_FILTER_BUTTONS: tuple = (
    ("all", "All"),
    ("dl", "DL"),
    ("mv", "MV"),
    ("rd", "RD"),
    ("qd", "QD"),
    ("ov", "📊"),
)


def match_status_filter(state_value: object, key: str) -> bool:
    """True when a row state passes the active-list filter. Never raises."""
    try:
        if (key or "all") == "all":
            return True
        _want = _STATUS_FILTERS.get(key or "all")
        if _want is None:
            return True
        return str(state_value or "").lower() in _want
    except Exception:
        return True


def _is_bulk_scope(h: str) -> bool:
    """True for re-resolved bulk scopes (`match:<tok>`, `stalled`,
    `status:<filter>`).

    Bulk scopes carry no frozen hash list: every tap re-resolves the
    live set, so no stored pending state exists to expire.
    """
    try:
        return h == "stalled" or h.startswith("match:") or h.startswith("status:")
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
    `match` (base64url title substring), `stalled` (0-seeder predicate),
    `status` (active-list filter key: `dl`/`mv`/`rd`/`qd`).
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
            if h.startswith("status:"):
                return cls("status", h[len("status:") :])
            return cls("match", h[len("match:") :])
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
        if self.kind == "status":
            return f"status:{self.key}"
        return self.key

    async def resolve(self, bot: Any) -> ResolvedScope:
        """Live (title, label, scope, execute-title, hashes) for the scope."""
        store = getattr(bot, "_store", None)
        if self.kind == "single":
            get = getattr(store, "get", None) if store else None
            try:
                row = (
                    await asyncio.to_thread(get, self.key, include_blob=False)
                    if callable(get)
                    else None
                )
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"resolve failed: {e}") from e
            if row is None:
                raise ScopeGone()
            title = str(getattr(row, "source_name", "") or self.key[:10])[:60]
            return ResolvedScope(title, "", self.key, title, [self.key])
        if self.kind == "group":
            fn = getattr(bot, "_live_group_hashes", None)
            try:
                hashes = await asyncio.to_thread(fn, self.key) if callable(fn) else []
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"resolve failed: {e}") from e
            if not hashes:
                raise ScopeGone("Group changed — tap /act again.")
            title = self.key[:10]
            try:
                get = getattr(store, "get", None) if store else None
                leader = (
                    await asyncio.to_thread(get, self.key, include_blob=False)
                    if callable(get)
                    else None
                )
                if leader is not None:
                    title = str(getattr(leader, "source_name", "") or self.key[:10])[:60]
            except Exception:
                pass
            return ResolvedScope(
                title, f" · all {len(hashes)} copies", f"all:{self.key}", title, list(hashes)
            )
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
                f" · {total} copies",
                "stalled",
                f"{len(groups)} stalled groups",
                [h for (_, hs) in groups for h in hs or []],
            )
        if self.kind == "status":
            # Fail closed: only the lane keys cancel. An unknown key
            # (all/ov/garbage) must never resolve to the whole list —
            # match_status_filter fail-opens to True for display.
            if self.key not in ("dl", "mv", "rd", "qd"):
                raise ScopeGone()
            _label = dict(_STATUS_FILTER_BUTTONS).get(self.key, self.key)
            _get_all = getattr(store, "list_active_inflight", None) if store else None
            try:
                rows = await asyncio.to_thread(_get_all) if callable(_get_all) else []
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"resolve failed: {e}") from e
            hashes = []
            for _r in rows or []:
                try:
                    _sv = getattr(getattr(_r, "state", None), "value", "")
                    if not match_status_filter(_sv, self.key):
                        continue
                    _rh = (getattr(_r, "source_infohash", "") or "").lower()
                    if _rh:
                        hashes.append(_rh)
                except Exception:
                    continue
            if not hashes:
                raise ScopeGone()
            _n = len(hashes)
            _title = f"{_n} {_label} {'copy' if _n == 1 else 'copies'}"
            return ResolvedScope(_title, "", f"status:{self.key}", _title, hashes)
        raise ScopeGone()

    async def _resolve_match(self, bot: Any) -> ResolvedScope:
        """Match-scope resolution (substring token in :attr:`key`)."""
        q = _match_query(self.key)
        fn = getattr(bot, "_live_match", None)
        try:
            matches = await asyncio.to_thread(fn, q) if (callable(fn) and q) else []
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"match resolve failed: {e}") from e
        if not matches:
            raise ScopeGone()
        total = sum(len(hs) for (_, hs) in matches)
        return ResolvedScope(
            f"{len(matches)} groups matching '{q[:40]}'",
            f" · {total} copies",
            f"match:{self.key}",
            f"{len(matches)} matched groups",
            [h for (_, hs) in matches for h in hs or []],
        )

    def remember_sheet(self, subject: str) -> tuple[str, list]:
        """Q1 remember question for the scope (subject pre-sanitized)."""
        from .telegram_buttons import ButtonMaker

        scope = self.to_data()
        text = (
            f"{subject}\n"
            "🚫 Ignore blocks it from coming back; "
            "👻 Just forget allows re-adding it later."
        )
        rows = (
            ButtonMaker()
            .add("🚫 Ignore + forget", f"forget:{scope}:1")
            .add("👻 Just forget", f"forget:{scope}:0")
            .newline()
            .add("Close", "abort")
            .rows()
        )
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
        return (data.startswith("keep:") and data.endswith(":no")) or (
            data.startswith("inject:") and data.endswith(":yes")
        )
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


def _now_command(infohash: str) -> str:
    """Copy-pasteable start command for one task (short hash)."""
    return f"/start_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _skip_command(infohash: str) -> str:
    """Copy-pasteable hold command for one task (short hash)."""
    return f"/hold_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


def _resume_command(infohash: str) -> str:
    """Copy-pasteable unhold command for one task (short hash)."""
    return f"/unhold_{(infohash or '').lower()[:CANCEL_SHORT_LEN]}"


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
    if (
        not any(
            k in msg
            for k in (
                "flood",
                "too many requests",
                "rate limit",
                "ratelimit",
                "slow mode",
                "slowmode",
            )
        )
        and re.search(r"retry (?:in|after) \d+", msg) is None
    ):
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
    return any(
        s in m
        for s in (
            "timed out",
            "timeout",
            "connection",
            "temporary",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        )
    )


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
        if len(parts) == 2 and parts[0] in ("go", "cancel", "inject", "skip", "resume", "forget"):
            if _is_hex40(parts[1]):
                return (parts[0], False, parts[1].lower(), "", None)
        if (
            len(parts) == 3
            and parts[0] in ("cancel", "keep", "inject", "skip", "resume", "go", "forget")
            and parts[1] == "all"
        ):
            if _is_hex40(parts[2]):
                return (parts[0], True, parts[2].lower(), "", None)
        if len(parts) == 3 and parts[0] in ("keep", "inject"):
            if _is_hex40(parts[1]) and parts[2] in ("yes", "no"):
                return (parts[0], False, parts[1].lower(), parts[2], True)
        if len(parts) == 4 and parts[0] in ("keep", "inject"):
            if _is_hex40(parts[1]) and parts[2] in ("0", "1") and parts[3] in ("yes", "no"):
                return (parts[0], False, parts[1].lower(), parts[3], parts[2] == "1")
        if len(parts) == 5 and parts[0] in ("keep", "inject"):
            if (
                parts[1] == "all"
                and _is_hex40(parts[2])
                and parts[3] in ("0", "1")
                and parts[4] in ("yes", "no")
            ):
                return (parts[0], True, parts[2].lower(), parts[4], parts[3] == "1")
        if len(parts) == 3 and parts[0] == "forget":
            if _is_hex40(parts[1]) and parts[2] in ("0", "1"):
                return (parts[0], False, parts[1].lower(), "", parts[2] == "1")
        if len(parts) == 4 and parts[0] == "forget":
            if parts[1] == "all" and _is_hex40(parts[2]) and parts[3] in ("0", "1"):
                return (parts[0], True, parts[2].lower(), "", parts[3] == "1")
        # Bulk-match scope: the substring travels base64url-encoded and is
        # re-resolved live at every tap (`match:<tok>` in the hash slot).
        if len(parts) == 4 and parts[0] == "forget":
            if (
                parts[1] == "match"
                and parts[3] in ("0", "1")
                and _MATCH_TOKEN_RE.match(parts[2] or "")
            ):
                return (parts[0], False, f"match:{parts[2]}", "", parts[3] == "1")
        if len(parts) == 5 and parts[0] == "keep":
            if (
                parts[1] == "match"
                and parts[3] in ("0", "1")
                and parts[4] in ("yes", "no")
                and _MATCH_TOKEN_RE.match(parts[2] or "")
            ):
                return (parts[0], False, f"match:{parts[2]}", parts[4], parts[3] == "1")
        # Predicate scope: fixed predicate, no token to carry.
        if len(parts) == 3 and parts[0] == "forget":
            if parts[1] == "stalled" and parts[2] in ("0", "1"):
                return (parts[0], False, "stalled", "", parts[2] == "1")
        if len(parts) == 4 and parts[0] == "keep":
            if parts[1] == "stalled" and parts[2] in ("0", "1") and parts[3] in ("yes", "no"):
                return (parts[0], False, "stalled", parts[3], parts[2] == "1")
        # Status scope: lane filter key, re-resolved live like stalled.
        # Only the cancellable lane keys (never all/ov/unknown — those
        # must not resolve to the whole list).
        if len(parts) == 3 and parts[0] == "cancel":
            if parts[1] == "status" and parts[2] in ("dl", "mv", "rd", "qd"):
                return (parts[0], False, f"status:{parts[2]}", "", None)
        if len(parts) == 4 and parts[0] == "forget":
            if (
                parts[1] == "status"
                and parts[3] in ("0", "1")
                and parts[2] in ("dl", "mv", "rd", "qd")
            ):
                return (parts[0], False, f"status:{parts[2]}", "", parts[3] == "1")
        if len(parts) == 5 and parts[0] == "keep":
            if (
                parts[1] == "status"
                and parts[3] in ("0", "1")
                and parts[4] in ("yes", "no")
                and parts[2] in ("dl", "mv", "rd", "qd")
            ):
                return (parts[0], False, f"status:{parts[2]}", parts[4], parts[3] == "1")
    except Exception:
        pass
    return ("", False, "", "", None)


def _bot_command_menu() -> list:
    """Commands shown in the chat's `/` popup (Bot API menu).

    EVERY command the bot understands is listed here, with a one-line
    description: the operator should never have to remember a name. The
    popup shows the bare verb, so a target is typed after a space
    (`/cancel 3`); the underscore form (`/cancel_3`) keeps working.
    """
    try:
        return [
            BotCommand(
                "active",
                "Show the Active Tasks card now (re-send it if it was deleted)",
            ),
            BotCommand(
                "add",
                "Add a .torrent — reply to it with /add, or send it with /add as caption",
            ),
            BotCommand(
                "act",
                "Open the action sheet for a group — /act <group number or hash>",
            ),
            BotCommand(
                "cancel",
                "Cancel one release — /cancel <group number or hash> (no argument: cancel by lane)",
            ),
            BotCommand(
                "cancel_all",
                "Cancel every group whose title contains the text — /cancel_all <text>",
            ),
            BotCommand(
                "cancel_lane",
                "Cancel a whole lane (DL/MV/RD/QD) — opens a sheet with live counts",
            ),
            BotCommand(
                "cancel_stalled",
                "Cancel every download with no seeders and no progress",
            ),
            BotCommand(
                "files",
                "List a torrent's files (read-only) — /files <group number or hash>",
            ),
            BotCommand(
                "find",
                "Search the download-target indexers (read-only) — /find <text>",
            ),
            BotCommand(
                "force",
                "Start a queued/moving row past the lane caps — /force <group number or hash>",
            ),
            BotCommand(
                "hold",
                "Hold a release (no work until unheld) — /hold <group number or hash>",
            ),
            BotCommand(
                "inject",
                "Inject a torrent you moved to the remote yourself — /inject <hash>",
            ),
            BotCommand(
                "pause",
                "Pause intake — no new downloads start until /resume",
            ),
            BotCommand(
                "resume",
                "Resume intake of new downloads",
            ),
            BotCommand(
                "start",
                "Start now instead of waiting — /start <group number or hash>",
            ),
            BotCommand(
                "stats",
                "Pipeline snapshot — rows by state, workers, storage",
            ),
            BotCommand(
                "tick",
                "Run one pipeline tick now (poll + schedule + janitor)",
            ),
            BotCommand(
                "unhold",
                "Resume a held release — /unhold <hash>",
            ),
            BotCommand(
                "unignore",
                "Track a cancelled release again — /unignore <full hash>",
            ),
        ]
    except Exception:
        return []
