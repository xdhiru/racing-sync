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


def test_action_error_text_scrubs_secrets():
    from racing_sync.telegram_protocol import _action_error_text, _scrubbed

    evil = ValueError(
        "fetch blew up: passkey=SECRET123 "
        "https://tracker.example/announce?passkey=XYZ "
        "token abcdefghij1234567890")
    text = _action_error_text(evil)
    assert text.startswith("Action failed:")
    assert "SECRET123" not in text
    assert "XYZ" not in text
    assert "abcdefghij1234567890" not in text
    assert _scrubbed(evil) and "Action failed" not in _scrubbed(evil)


def _act_bot(rows, **kw):
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = list(rows)
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=5))
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _act_rows():
    from racing_sync.state import State, TorrentState
    mk = lambda h, name, size: TorrentState(  # noqa: E731
        source_infohash=h, source_name=name, state=State.QUEUED,
        total_bytes=size,
        source_announce_url="https://alpha.cc/announce/xyz")
    return [mk("a" * 40, "Show.One.S01E01", 1000),
            mk("b" * 40, "Show.Two.S01E01", 2000)]


@pytest.mark.anyio
async def test_act_number_warns_on_generation_change():
    """Typed /act_2 against a stale render warns (positions rot)."""
    from unittest.mock import MagicMock
    bot = _act_bot(_act_rows(), _last_rendered_groups=5)
    await bot._act_command_entry("2", MagicMock())
    text = bot._bot.send_message.call_args[0][1]
    assert text.startswith("⚠️ List changed since render (5→2 titles)")


@pytest.mark.anyio
async def test_act_number_quiet_when_generation_matches():
    from unittest.mock import MagicMock
    bot = _act_bot(_act_rows(), _last_rendered_groups=2)
    await bot._act_command_entry("2", MagicMock())
    text = bot._bot.send_message.call_args[0][1]
    assert "⚠️" not in text
    assert "Show.Two" in text


@pytest.mark.anyio
async def test_refresh_stamps_rendered_generation():
    """The active refresh records the group count taps resolve against."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import State, TorrentState

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(
        return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._detail_sent_state = {}
    bot._store.list_active_inflight.return_value = _act_rows()
    await bot._refresh_active_message_inner()
    assert bot._last_rendered_groups == 2
