"""Follow-up sheets stand alone; channel-post commands leave a trail."""
from __future__ import annotations

import logging
import re

import pytest

from racing_sync.config import TelegramConfig
from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_protocol import _ChatCommand


def _bot(**kw):
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._callback_times = {}
    bot._action_inflight = {}
    bot._current_page = 0
    bot._last_active_cache = None
    bot._store = MagicMock()
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=5))
    bot._refresh_active_message = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _tap(data):
    from unittest.mock import AsyncMock, MagicMock
    q = MagicMock()
    q.message.chat.id = 12345
    q.message.message_id = 77
    q.from_user.id = 12345
    q.data = data
    q.answer = AsyncMock()
    return q


@pytest.mark.anyio
async def test_remember_sheet_has_no_reply_to_doomed_sheet():
    """Q1 is sent standalone: the tapped sheet is deleted right after."""
    bot = _bot()
    bot._store.get.return_value = TorrentState(
        source_infohash="b" * 40, source_name="Twin.Show",
        state=State.WAITING_INDEXER)
    await bot._handle_callback(_tap(f"cancel:{'b' * 40}"))
    _kwargs = bot._bot.send_message.call_args[1]
    assert "reply_to_message_id" not in _kwargs


@pytest.mark.anyio
async def test_action_result_has_no_reply_to_doomed_sheet():
    from unittest.mock import AsyncMock
    bot = _bot()
    bot._fetch_torrent = AsyncMock(return_value="Fetching original for X")
    bot._reply = AsyncMock()
    q = _tap(f"go:{'b' * 40}")
    await bot._handle_callback(q)
    _, kwargs = bot._reply.call_args
    assert "reply_to" not in kwargs


@pytest.mark.anyio
async def test_channel_post_command_warns_and_runs(caplog):
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (
        _ChatCommand("stats", re.compile(r"^/stats"), _fake_handle),
    )
    bot = _bot()
    from unittest.mock import MagicMock
    msg = MagicMock()
    msg.chat.id = 12345
    msg.from_user = None  # anonymous channel post: no sender
    msg.text = "/stats"
    msg.caption = None

    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        with caplog.at_level(logging.WARNING,
                             logger="racing_sync.telegram_bot"):
            await bot._handle_chat_message(msg)
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == ["handled"]
    assert any("without sender" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_channel_post_refused_when_allowlist_configured():
    import racing_sync.telegram_router as _router

    calls: list[str] = []

    async def _fake_handle(bot, match, message):
        calls.append("handled")

    fake_table = (
        _ChatCommand("stats", re.compile(r"^/stats"), _fake_handle),
    )
    bot = _bot()
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345",
                              admin_user_ids=[999])
    from unittest.mock import AsyncMock, MagicMock
    bot._reply = AsyncMock()
    msg = MagicMock()
    msg.chat.id = 12345
    msg.from_user = None
    msg.text = "/stats"
    msg.caption = None

    orig = _router._CHAT_COMMANDS
    _router._CHAT_COMMANDS = fake_table
    try:
        await bot._handle_chat_message(msg)
    finally:
        _router._CHAT_COMMANDS = orig
    assert calls == []
    bot._reply.assert_awaited_once()
