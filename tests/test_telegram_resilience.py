"""Telegram resilience: uncertain sends don't duplicate; terminal cards heal."""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram.error import TelegramError, TimedOut

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


def _repost_bot(**kw):
    """Bot double with the repost knobs the inner refresh needs."""
    bot = _bot()
    bot._cfg.active_repost_interval_seconds = 15
    bot._last_repost_monotonic = time.monotonic() - 3600.0
    bot._newest_outbound_id = None
    bot._orphan_active_ids = []
    bot._send_uncertain_key = None
    bot._send_uncertain_skips = 0
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


@pytest.mark.anyio
async def test_repost_delete_flood_keeps_old_without_send(tmp_path):
    """The reported orphan: delete flood + successful send stranded history.

    Delete-first-with-abort must keep the old card and skip the send so
    the pointer never moves past an undeleted message.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError(
                "Flood control exceeded. Retry in 12 seconds"))
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id == 111
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_delete_timeout_keeps_old_without_send(tmp_path):
    """Timed-out deletes abort the repost the same way floods do."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(side_effect=TimedOut("slow"))
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id == 111
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_delete_gone_still_resends(tmp_path):
    """An already-deleted old card is safe to replace immediately."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Bad Request: message to delete not found"))
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 222
        assert bot._prev_active_msg_id == 222
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_send_failure_clears_for_fresh_resend(tmp_path):
    """Delete ok + send timeout keeps the old uncertain-send behavior."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock()
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.delete_message.assert_awaited_once()
        assert bot._active_msg_id is None
        assert bot._send_uncertain_key == ("p", 1, "text")
    finally:
        store.close()


@pytest.mark.anyio
async def test_fresh_send_prev_delete_flood_aborts_send(tmp_path):
    """A flood-delayed previous delete must abort the send, not orphan it."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=None, _prev_active_msg_id=42)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=77))
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError(
                "Flood control exceeded. Retry in 16 seconds"))

        await bot._refresh_active_message_inner()

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id is None
        assert bot._prev_active_msg_id == 42
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_fresh_send_prev_gone_still_sends(tmp_path):
    """An already-deleted previous id does not block the fresh send."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=None, _prev_active_msg_id=42)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(
            return_value=SimpleNamespace(message_id=77))
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Bad Request: message to delete not found"))

        await bot._refresh_active_message_inner()

        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 77
        assert bot._prev_active_msg_id == 77
    finally:
        store.close()


@pytest.mark.anyio
async def test_orphan_sweep_deletes_then_drops(tmp_path):
    """The sweep retires ids that delete cleanly."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=77)
        bot._store = store
        bot._orphan_active_ids = [42]
        bot._bot.delete_message = AsyncMock()

        await bot._sweep_orphan_active_messages()

        bot._bot.delete_message.assert_awaited_once_with(
            chat_id="1", message_id=42)
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_orphan_sweep_keeps_flooded_and_drops_gone(tmp_path):
    """Flooded orphans stay queued; already-gone ids are forgotten."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=77)
        bot._store = store
        bot._orphan_active_ids = [42, 43]
        bot._bot.delete_message = AsyncMock(side_effect=[
            TelegramError("Flood control exceeded. Retry in 12 seconds"),
            TelegramError("Bad Request: message to delete not found"),
        ])

        await bot._sweep_orphan_active_messages()

        assert bot._bot.delete_message.await_count == 2
        assert bot._orphan_ids() == [42]
    finally:
        store.close()
