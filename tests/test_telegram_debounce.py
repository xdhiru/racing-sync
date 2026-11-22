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
        _ChatCommand("skip", re.compile(r"^/skip_"), _fake_handle),
        _ChatCommand("resume", re.compile(r"^/resume_"), _fake_handle),
    )
    bot = _cmd_bot()
    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(_msg("/skip_abc123"))
        await bot._handle_chat_message(_msg("/resume_abc123"))
        await bot._handle_chat_message(_msg("/skip_def456"))
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["/skip_abc123", "/resume_abc123", "/skip_def456"]


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
