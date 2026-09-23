"""Cancelling via /cancel_ edits the torrent's detail card to CANCELLED.

Without this, forget deletes the DB row (which holds telegram_message_id)
and the detail card freezes forever at its last live state, while only a
separate "Cancelled ..." reply goes out.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from racing_sync.state import State, StateStore, TorrentState


def _bot(**kw):
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = MagicMock()
    bot._cfg.chat_id = "1"
    bot._cfg.page_size = 5
    bot._callback_times = {}
    bot._store = MagicMock()
    bot._current_page = 0
    bot._refresh_active_message = AsyncMock()
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=7))
    bot._bot.edit_message_text = AsyncMock()
    bot._detail_cache = {}
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _setup(tmp_path: Path, h: str, *, db_msg_id: int = 0):
    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash=h, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    if db_msg_id:
        store.set_telegram_message_id(h, db_msg_id)
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    return bot, store


@pytest.mark.anyio
async def test_cancel_edits_detail_card_from_cache(tmp_path: Path):
    h = "a" * 40
    bot, store = _setup(tmp_path, h)
    bot._detail_cache[h] = 777
    try:
        msg = await bot._execute_cancel_one(h, delete_files=True)
        assert msg.startswith("Cancelled")
        assert store.get(h) is None
        bot._bot.edit_message_text.assert_awaited_once()
        _, kwargs = bot._bot.edit_message_text.call_args
        assert kwargs["message_id"] == 777
        assert "CANCELLED" in kwargs.get("text", bot._bot.edit_message_text.call_args[0][0])
        # Cache evicted so a future re-discovery starts a fresh card.
        assert h not in bot._detail_cache
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_edits_detail_card_from_db(tmp_path: Path):
    h = "b" * 40
    bot, store = _setup(tmp_path, h, db_msg_id=555)
    try:
        msg = await bot._execute_cancel_one(h, delete_files=True)
        assert msg.startswith("Cancelled")
        bot._bot.edit_message_text.assert_awaited_once()
        _, kwargs = bot._bot.edit_message_text.call_args
        assert kwargs["message_id"] == 555
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_without_card_still_replies(tmp_path: Path):
    h = "c" * 40
    bot, store = _setup(tmp_path, h)
    try:
        msg = await bot._execute_cancel_one(h, delete_files=True)
        assert msg.startswith("Cancelled")
        bot._bot.edit_message_text.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_card_edit_failure_does_not_fail_cancel(tmp_path: Path):
    h = "d" * 40
    bot, store = _setup(tmp_path, h, db_msg_id=999)
    bot._bot.edit_message_text = AsyncMock(side_effect=Exception("message not found"))
    try:
        msg = await bot._execute_cancel_one(h, delete_files=True)
        assert msg.startswith("Cancelled")
        assert store.get(h) is None
        assert store.is_ignored(h) is True
    finally:
        store.close()
