"""Worker loop-level flood backoff, 401 fail-fast, narrow prefill, poll conflict."""

from __future__ import annotations

import asyncio

import pytest

from racing_sync.config import TelegramConfig
from racing_sync.telegram_bot import TelegramBot, _FloodBackoff


@pytest.mark.anyio
async def test_worker_loop_backs_off_once_and_requeues_remainder():
    """A flooded first card sleeps once at loop level; both cards send."""
    from unittest.mock import MagicMock

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123", outbound_rate=30.0)
    bot._bot = MagicMock()
    bot._detail_queue = asyncio.Queue()
    bot._detail_queue.put_nowait(("a" * 40, None))
    bot._detail_queue.put_nowait(("b" * 40, None))
    sent: list[str] = []

    async def _send(h, _p):
        if h == "a" * 40 and sent.count("a" * 40) == 0:
            sent.append(h)
            _fb = _FloodBackoff(30.0)
            _fb.seconds = 0.05
            raise _fb
        sent.append(h)

    bot._send_one_detail = _send  # type: ignore[method-assign]
    task = asyncio.create_task(bot._detail_worker_loop())
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # a sent, flooded, requeued; then a + b both delivered.
    assert sent.count("a" * 40) == 2
    assert sent.count("b" * 40) == 1


@pytest.mark.anyio
async def test_start_fails_fast_on_401():
    """A rejected token raises before any loop starts (no retry hiding)."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from telegram.error import TelegramError

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._store = MagicMock()
    bot._store.all.return_value = []
    bot._store.get_meta.return_value = None
    bot._detail_cache = {}
    bot._detail_sent_state = {}
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._newest_outbound_id = None
    bot._task = None
    bot._callback_task = None
    bot._detail_worker = None

    with patch("racing_sync.telegram_bot.Bot") as mock_bot_cls:
        mock_bot = MagicMock()
        mock_bot.send_message = AsyncMock(
            side_effect=TelegramError("Unauthorized: bot token invalid")
        )
        mock_bot_cls.return_value = mock_bot
        with pytest.raises(RuntimeError, match="bot_token rejected"):
            await bot.start()
    assert bot._task is None
    assert bot._callback_task is None
    if bot._detail_worker is not None:
        bot._detail_worker.cancel()
        try:
            await bot._detail_worker
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.anyio
async def test_start_prefills_from_narrow_query():
    """Startup learns sent states without hydrating full rows."""
    from unittest.mock import AsyncMock, MagicMock, patch

    store = MagicMock()
    store.all.side_effect = AssertionError("full scan at startup!")
    store.all_card_prefill.return_value = [("a" * 40, 11, "done")]
    store.get_meta.return_value = None

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._store = store
    bot._detail_cache = {}
    bot._detail_sent_state = {}
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._newest_outbound_id = None

    with patch("racing_sync.telegram_bot.Bot") as mock_bot_cls:
        mock_bot = MagicMock()
        mock_bot.send_message = AsyncMock()
        mock_bot_cls.return_value = mock_bot
        with patch("asyncio.create_task", return_value=MagicMock()):
            await bot.start()
    assert bot._detail_cache == {"a" * 40: 11}
    assert bot._sent_state_map() == {"a" * 40: "done"}


@pytest.mark.anyio
async def test_callback_loop_fails_fast_on_repeated_conflict():
    """Three consecutive poll conflicts stop the loop (no endless 15s naps)."""
    from unittest.mock import AsyncMock, MagicMock, patch

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._stopped = False
    bot._bot = MagicMock()
    bot._bot.get_updates = AsyncMock(
        side_effect=[
            Exception("Conflict: terminated by other getUpdates"),
            Exception("Conflict: terminated by other getUpdates"),
            Exception("Conflict: terminated by other getUpdates"),
        ]
    )
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await bot._callback_loop()
    assert bot._poll_conflicts == 3
    assert bot._poll_conflict_fatal is True
    assert bot._stopped is True
    # Two 15s naps, then fail-fast (no sleep after the third).
    assert mock_sleep.await_count == 2


def test_state_prefill_query_is_narrow(tmp_path):
    """all_card_prefill returns id/state triples, not full rows."""
    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "t.db")
    try:
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Show",
                state=State.DONE,
                total_bytes=100,
                telegram_message_id=77,
            )
        )
        rows = store.all_card_prefill()
    finally:
        try:
            store.close()
        except Exception:
            pass
    assert rows == [("a" * 40, 77, "done")]
