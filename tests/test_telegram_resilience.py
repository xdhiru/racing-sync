"""Telegram resilience: uncertain sends don't duplicate; terminal cards heal."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram.error import TimedOut

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.telegram_bot import TelegramBot


def _bot(**kw):
    bot = object.__new__(TelegramBot)
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5,
                               status_update_interval=15)
    bot._callback_times = {}
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._pending_pick = None
    bot._coord = MagicMock()
    bot._coord.live_progress_map = MagicMock(return_value={})
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(
        return_value=SimpleNamespace(message_id=7))
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.delete_message = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _drain(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


@pytest.mark.anyio
async def test_active_send_timeout_skips_blind_resend(tmp_path):
    """Lost-response sends must not mint a duplicate every interval."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        # First send times out with no known id: throttled retries.
        await bot._refresh_active_message_inner()
        for _ in range(3):
            await bot._refresh_active_message_inner()
            assert bot._bot.send_message.await_count == 1
        # A previously known message id (e.g. repost deleted the old one
        # but the resend fate is unknown): static text skips entirely.
        bot._prev_active_msg_id = 42
        for _ in range(5):
            await bot._refresh_active_message_inner()
            assert bot._bot.send_message.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_active_send_timeout_records_uncertain_then_skips(tmp_path):
    """After one timeout, unchanged text skips; changed text sends."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._prev_active_msg_id = 5
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        # Unchanged text: throttled retries (3 skips, 4th sends) instead of
        # a duplicate every interval.
        await bot._refresh_active_message_inner()
        await bot._refresh_active_message_inner()
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 2
        # Changed text (new row appears): send proceeds.
        store.upsert(TorrentState(
            source_infohash="n" * 40, source_name="New Show",
            state=State.NEW))
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=9))
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        assert bot._active_msg_id == 9
    finally:
        store.close()


@pytest.mark.anyio
async def test_done_card_timeout_records_terminal_retry(tmp_path):
    """A timed-out DONE edit is requeued AND remembered past the active list."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(TorrentState(
            source_infohash=h, source_name="Done Show",
            state=State.DONE))
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._bot.edit_message_text = AsyncMock(
            side_effect=TimedOut("slow route"))
        bot._mark_detail_sent(h, "moving")  # card shows pre-DONE state

        await bot._send_one_detail(h, None)

        assert bot._detail_terminal_map().get(h) == 0
        assert [i[0] for i in _drain(bot._detail_queue)] == [h]
    finally:
        store.close()


@pytest.mark.anyio
async def test_terminal_net_requeues_until_card_lands(tmp_path):
    """Refresh loop retries recorded terminal cards; success clears them."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(TorrentState(
            source_infohash=h, source_name="Done Show",
            state=State.DONE))
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._active_msg_id = 50
        bot._active_footer = AsyncMock(return_value="")
        bot._mark_detail_sent(h, "moving")
        bot._detail_terminal_map()[h] = 0

        await bot._refresh_active_message_inner()

        assert h in [i[0] for i in _drain(bot._detail_queue)]
        assert bot._detail_terminal_map().get(h) == 1
        # Card lands: record cleared, no further retries.
        bot._mark_detail_sent(h, "done")
        await bot._refresh_active_message_inner()
        assert h not in bot._detail_terminal_map()
        assert bot._detail_queue.empty()
    finally:
        store.close()


@pytest.mark.anyio
async def test_transient_streak_caps_immediate_requeue(tmp_path):
    """A dead route must not hot-loop one card through the worker."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(TorrentState(
            source_infohash=h, source_name="Flaky Show",
            state=State.DOWNLOADING))
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._bot.edit_message_text = AsyncMock(
            side_effect=TimedOut("dead route"))

        for _ in range(8):
            await bot._send_one_detail(h, None)

        # 5 immediate requeues max; the rest rely on the refresh nets.
        assert bot._detail_queue.qsize() <= 5
    finally:
        store.close()
