"""Refresh upkeep: coalescing, footer TTLs, drift streak gate, orphans, pin persist."""

from __future__ import annotations

import asyncio

import pytest

from racing_sync.config import TelegramConfig
from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import TelegramBot


def _bot(**kw):
    from unittest.mock import AsyncMock, MagicMock

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=99))
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
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _drain(bot):
    got = []
    while not bot._detail_queue.empty():
        got.append(bot._detail_queue.get_nowait()[0])
    return got


@pytest.mark.anyio
async def test_concurrent_refresh_coalesces():
    """A tap arriving mid-refresh marks dirty; the holder re-runs once."""
    calls: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_inner():
        calls.append("run")
        started.set()
        await release.wait()

    bot = _bot()
    bot._active_lock = asyncio.Lock()
    bot._refresh_active_message_inner = _slow_inner

    holder = asyncio.create_task(bot._refresh_active_message())
    await started.wait()
    tap = asyncio.create_task(bot._refresh_active_message())
    await asyncio.sleep(0)
    try:
        assert bot._refresh_dirty is True
        assert tap.done()
    finally:
        release.set()
    await holder
    await tap
    assert calls == ["run", "run"]


@pytest.mark.anyio
async def test_ssd_free_stat_cached_per_ttl(tmp_path):
    """Bursts of taps share one SSD stat (display-only footer)."""
    from types import SimpleNamespace
    from unittest.mock import patch

    coord = SimpleNamespace(
        cfg=SimpleNamespace(
            ssd=SimpleNamespace(path=tmp_path),
            cleanup=SimpleNamespace(),
        ),
        _ssd_reserved_total=lambda: 0,
    )
    bot = object.__new__(TelegramBot)
    bot._coord = coord
    bot._ssd_free_cache = None
    with patch("racing_sync.rclone_ops.ssd_free_bytes", return_value=50 * 1024**3) as _free:
        f1 = await bot._active_footer()
        f2 = await bot._active_footer()
    assert f1 == f2 and "SSD Free" in f1
    assert _free.call_count == 1


@pytest.mark.anyio
async def test_drift_net_skips_fail_streak_hashes():
    """A dead card route is not re-driven every refresh."""
    h = "d" * 40

    def _run(**kw):
        bot = _bot(_detail_sent_state={h: "queued"})
        bot._detail_fail_streak = {h: 9}
        bot._store.list_active_inflight.return_value = [
            TorrentState(
                source_infohash=h, source_name="Drifted", state=State.DONE, total_bytes=1000
            ),
        ]
        for k, v in kw.items():
            setattr(bot, k, v)
        return bot

    bot = _run()
    await bot._refresh_active_message_inner()
    assert _drain(bot) == []

    # ...until 7 prior skips: the 8th re-drives once (healed route heals).
    bot2 = _run(_detail_drift_skip={h: 7})
    await bot2._refresh_active_message_inner()
    assert _drain(bot2) == [h]


@pytest.mark.anyio
async def test_hopeless_prev_id_remembered_for_sweep():
    """An abandoned prev id feeds the orphan sweep (rights may return)."""
    from unittest.mock import AsyncMock

    from telegram.error import TelegramError

    bot = _bot()
    bot._prev_active_msg_id = 111
    bot._bot.delete_message = AsyncMock(
        side_effect=TelegramError("Bad Request: not enough rights to delete")
    )
    bot._store.list_active_inflight.return_value = []
    await bot._refresh_active_message_inner()
    assert 111 in bot._orphan_ids()
    # The sweep retires it as hopeless on the next pass.
    await bot._sweep_orphan_active_messages()
    assert 111 not in bot._orphan_ids()


@pytest.mark.anyio
async def test_pin_failure_persists_without_mutating_cfg():
    from unittest.mock import AsyncMock

    from telegram.error import TelegramError

    bot = _bot()
    bot._cfg = TelegramConfig(
        enabled=True, bot_token="fake", chat_id="123", pin_status_message=True
    )
    bot._bot.pin_chat_message = AsyncMock(
        side_effect=TelegramError("Bad Request: not enough rights to pin")
    )
    bot._active_msg_id = 5
    bot._pinned_message_id = None
    bot._store.get_meta.return_value = None
    await bot._reconcile_pinned()
    # Shared config untouched; disable persisted to the store.
    assert bot._cfg.pin_status_message is True
    bot._store.set_meta.assert_called_with("telegram_pin_disabled", "1")
    # Second pass (and post-restart reads) skip the pin attempt.
    bot._bot.pin_chat_message.reset_mock()
    await bot._reconcile_pinned()
    bot._bot.pin_chat_message.assert_not_called()


@pytest.mark.anyio
async def test_already_deleted_prev_message_resends_instead_of_retrying(caplog):
    """BadRequest subclasses NetworkError: 'not found' must not defer forever."""
    import logging
    from unittest.mock import AsyncMock

    from telegram.error import BadRequest

    bot = _bot()
    bot._prev_active_msg_id = 421
    bot._bot.delete_message = AsyncMock(side_effect=BadRequest("Message to delete not found"))
    bot._store.list_active_inflight.return_value = []

    with caplog.at_level(logging.DEBUG):
        await bot._refresh_active_message_inner()

    # The stale id was dropped; the freshest send's id is the live pointer.
    assert bot._prev_active_msg_id == 99
    bot._bot.send_message.assert_awaited()
    assert not [r for r in caplog.records if "delete deferred" in r.getMessage()]


@pytest.mark.anyio
async def test_repost_already_deleted_message_keeps_going(caplog):
    import logging
    from unittest.mock import AsyncMock

    from telegram.error import BadRequest

    bot = _bot()
    bot._active_msg_id = 55
    bot._bot.delete_message = AsyncMock(side_effect=BadRequest("Message to delete not found"))
    bot._store.list_active_inflight.return_value = []

    with caplog.at_level(logging.DEBUG):
        await bot._repost_active_message("text", {}, None)

    assert bot._prev_active_msg_id == 99
    bot._bot.send_message.assert_awaited()
    assert not [r for r in caplog.records if "delete deferred" in r.getMessage()]
