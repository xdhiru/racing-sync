"""Debounce scoping, slow-down hints, fail-closed guards, per-hash claims."""

from __future__ import annotations

import re
import time

import pytest

from racing_sync.config import TelegramConfig
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_protocol import _ChatCommand


def _cmd_bot(**kw):
    from unittest.mock import MagicMock

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._callback_times = {}
    bot._action_inflight = {}
    bot._reply = MagicMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _msg(text):
    from unittest.mock import MagicMock

    m = MagicMock()
    m.chat.id = 12345
    m.from_user.id = 12345
    m.text = text
    m.caption = None
    # Real PTB messages carry None here; a MagicMock attribute is truthy
    # and would look like a .torrent upload to the debounce rule.
    m.document = None
    m.reply_to_message = None
    return m


@pytest.mark.anyio
async def test_read_only_commands_ignore_debounce_window():
    """A /stats tap right after a mutating command still answers."""
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (
        _ChatCommand("cancel", re.compile(r"^/cancel_"), _fake_handle),
        _ChatCommand("stats", re.compile(r"^/stats"), _fake_handle),
    )
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(_msg("/cancel_abc"))
        await bot._handle_chat_message(_msg("/stats"))
        await bot._handle_chat_message(_msg("/stats"))
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["handled", "handled", "handled"]


@pytest.mark.anyio
async def test_mutating_command_debounced():
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (_ChatCommand("cancel", re.compile(r"^/cancel_"), _fake_handle),)
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(_msg("/cancel_abc"))
        await bot._handle_chat_message(_msg("/cancel_abc"))
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["handled"]


@pytest.mark.anyio
async def test_debounced_callback_answers_slow_down():
    from unittest.mock import AsyncMock, MagicMock

    bot = _cmd_bot()
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = []
    bot._refresh_active_message = AsyncMock()
    bot._current_page = 0
    bot._last_active_cache = None

    def _q():
        q = MagicMock()
        q.message.chat.id = 12345
        q.from_user.id = 12345
        q.data = "page:refresh"
        q.answer = AsyncMock()
        return q

    q1, q2 = _q(), _q()
    await bot._handle_callback(q1)
    await bot._handle_callback(q2)
    q1.answer.assert_awaited_once_with()
    assert q2.answer.await_args_list[0].args[0].startswith("Slow down")


def test_guards_fail_closed_on_broken_bookkeeping():
    class _Broken(dict):
        def get(self, *a, **k):
            raise RuntimeError("boom")

        def items(self, *a, **k):
            raise RuntimeError("boom")

    bot = _cmd_bot()
    bot._callback_times = _Broken()
    assert bot._debounced("anything") is True
    bot._action_inflight = _Broken()
    assert bot._claim_action("anything") is False


def test_debounce_evict_never_flushes_live_windows():
    bot = _cmd_bot()
    now = time.monotonic()
    bot._callback_times = {f"k{i}": now for i in range(1000)}
    assert bot._debounced("brand-new") is False
    # All 1000 live windows survive; the table stays bounded.
    assert len(bot._callback_times) <= 1000
    assert bot._callback_times.get("k0") == now
    # Expired windows are purged on the next tap.
    bot._callback_times["old"] = now - 60.0
    assert bot._debounced("another") is False
    assert "old" not in bot._callback_times


@pytest.mark.anyio
async def test_overlapping_cancel_scope_conflicts_per_hash():
    """A held copy blocks an overlapping group cancel (no double forget)."""
    bot = _cmd_bot()
    now = time.monotonic()
    bot._action_inflight = {f"keep:{'a' * 40}:1:del": now}
    res = await bot._execute_snapshot_cancel(
        ["a" * 40, "b" * 40], "Twin.Show", delete_files=True, remember=True
    )
    assert res.startswith("Already working")
    # The sibling's speculative hold was rolled back, not leaked.
    assert f"keep:{'b' * 40}:1:del" not in bot._action_inflight
    assert bot._action_inflight == {f"keep:{'a' * 40}:1:del": now}


@pytest.mark.anyio
async def test_different_mutating_commands_both_run():
    """A different verb/target inside the window is not a duplicate."""
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append(match.string)

    fake_table = (
        _ChatCommand("skip", re.compile(r"^/hold_"), _fake_handle),
        _ChatCommand("resume", re.compile(r"^/unhold_"), _fake_handle),
    )
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(_msg("/hold_abc123"))
        await bot._handle_chat_message(_msg("/unhold_abc123"))
        await bot._handle_chat_message(_msg("/hold_def456"))
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["/hold_abc123", "/unhold_abc123", "/hold_def456"]


@pytest.mark.anyio
async def test_duplicate_command_answers_instead_of_silent_drop():
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (_ChatCommand("cancel", re.compile(r"^/cancel_"), _fake_handle),)
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(_msg("/cancel_abc"))
        await bot._handle_chat_message(_msg("/cancel_abc"))
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["handled"]
    assert bot._reply.call_count == 1
    assert "Ignored duplicate cancel" in bot._reply.call_args[0][0]


@pytest.mark.anyio
async def test_document_add_is_never_debounced():
    """Two .torrent uploads in a row are distinct work (and must be deleted)."""
    from unittest.mock import MagicMock

    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (_ChatCommand("add", re.compile(r"^/add"), _fake_handle),)
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        for _ in range(2):
            m = _msg("/add")
            m.document = MagicMock()
            await bot._handle_chat_message(m)
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["handled", "handled"]


@pytest.mark.anyio
async def test_abort_prefixed_data_still_removes_the_dead_sheet():
    """`aborted` is a stale button: answered AND its sheet deleted.

    The delete-skip used a prefix match, so any data starting with
    "abort" left the dead sheet in the chat.
    """
    from unittest.mock import AsyncMock, MagicMock

    bot = _cmd_bot()
    bot._delete_query_message = AsyncMock()
    q = MagicMock()
    q.message.chat.id = 12345
    q.from_user.id = 12345
    q.data = "aborted"
    q.answer = AsyncMock()

    await bot._handle_callback(q)

    bot._delete_query_message.assert_awaited_once()


@pytest.mark.anyio
async def test_send_sheet_binds_the_opener():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    bot = _cmd_bot()
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=777))
    await bot._send_sheet(
        "Forget X?",
        [[("Yes", "keep:" + "a" * 40 + ":1:yes")]],
        opener=111,
    )
    assert bot._sheet_owner == {777: 111}

    # Falls back to the replied-to message's sender for command sheets.
    bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=778))
    await bot._send_sheet(
        "Forget Y?",
        [[("Yes", "keep:" + "b" * 40 + ":1:yes")]],
        reply_to=SimpleNamespace(from_user=SimpleNamespace(id=222)),
    )
    assert bot._sheet_owner[778] == 222


@pytest.mark.anyio
async def test_destructive_sheet_answers_only_its_opener():
    from unittest.mock import AsyncMock, MagicMock

    def _tap(user, data="keep:" + "a" * 40 + ":1:yes"):
        q = MagicMock()
        q.message.chat.id = 12345
        q.message.message_id = 500
        q.from_user.id = user
        q.data = data
        q.answer = AsyncMock()
        return q

    bot = _cmd_bot()
    bot._sheet_owner = {500: 111}
    bot._on_action_button = AsyncMock()

    # Another member: refused, handler never runs.
    other = _tap(222)
    await bot._handle_callback(other)
    bot._on_action_button.assert_not_awaited()
    assert "another operator" in other.answer.await_args.args[0]

    # The opener: proceeds.
    own = _tap(111)
    await bot._handle_callback(own)
    bot._on_action_button.assert_awaited_once()
