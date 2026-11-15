"""`/active`: operator recovery — re-render the Active Tasks card fresh.

The periodic refresh edits one long-lived card in place, so a card
deleted from the chat — or a bot that latched the "chat not found" kill
switch — leaves the operator with no card and no way back. `/active`
sends a brand new one.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram.error import TelegramError
from test_telegram_resilience import _bot

from racing_sync.state import StateStore
from racing_sync.telegram_protocol import _cmd_active


@pytest.mark.anyio
async def test_render_active_now_sends_new_card_when_text_unchanged(tmp_path):
    """The cached render must not suppress the operator's fresh card."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")

        # Warm the cache: the first refresh sends, the second one skips.
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1

        # The stale card is retired by the orphan sweep (bounded, best
        # effort) — a flooded delete must not block the send.
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Flood control exceeded. Retry in 12 seconds")
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=9))

        ok = await bot.render_active_now()

        assert ok is True
        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 9
        # The old card id is queued for the orphan sweep, not dropped.
        assert bot._orphan_ids() == [7]
    finally:
        store.close()


@pytest.mark.anyio
async def test_render_active_now_rearms_kill_switch(tmp_path):
    """A latched "chat not found" switch must not block the operator."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(_active_msg_id=-1)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=21))

        ok = await bot.render_active_now()

        assert ok is True
        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 21
    finally:
        store.close()


@pytest.mark.anyio
async def test_render_active_now_reports_failed_send(tmp_path):
    """A failing send leaves no card behind and reports the failure."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(side_effect=TelegramError("Bad Request: chat not found"))

        ok = await bot.render_active_now()

        assert ok is False
        assert bot._active_msg_id is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_cmd_active_reports_success_and_failure():
    """The handler answers with the outcome of the manual re-render."""
    msg = MagicMock()

    bot = MagicMock()
    bot.render_active_now = AsyncMock(return_value=True)
    bot._reply = AsyncMock()
    await _cmd_active(bot, MagicMock(), msg)
    assert bot._reply.call_args[0][0] == "Active tasks sent."

    failing = MagicMock()
    failing.render_active_now = AsyncMock(return_value=False)
    failing._reply = AsyncMock()
    await _cmd_active(failing, MagicMock(), msg)
    assert failing._reply.call_args[0][0] == ("Active tasks could not be sent — see the log.")


@pytest.mark.anyio
async def test_cmd_active_without_bot_reports_not_attached():
    """A bot without the re-render hook answers like the other verbs."""
    msg = MagicMock()
    bot = MagicMock(spec=["_reply"])
    bot._reply = AsyncMock()

    await _cmd_active(bot, MagicMock(), msg)

    assert bot._reply.call_args[0][0] == "Action failed: bot not attached."
