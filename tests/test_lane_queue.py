"""P4: lane queues — check/start/wake without DB churn."""
from __future__ import annotations

import asyncio
import time

import pytest


def _coord(cap_dl=1, cap_mv=1):
    from unittest.mock import MagicMock
    from conftest import make_coordinator

    coord = make_coordinator(MagicMock())
    coord.cfg.max_active_downloads = cap_dl
    coord.cfg.max_concurrent_moves = cap_mv
    coord._stop = False
    return coord


def _row(h, state, **kw):
    from racing_sync.state import State, TorrentState
    args = dict(source_infohash=h, source_name=f"Show-{h[:6]}",
                state=state, total_bytes=1000)
    args.update(kw)
    return TorrentState(**args)


def _handle(h, kind):
    from racing_sync.task import TaskHandle, register_handle
    import asyncio as _a
    hd = TaskHandle(infohash=h, gid=h[:12], kind=kind)
    return hd


def test_check_running_tasks_gates_and_forces():
    from unittest.mock import MagicMock
    from conftest import make_coordinator
    from racing_sync.state import State
    from racing_sync.task import register_handle

    coord = _coord()
    ha = _handle("a" * 40, "downloading")
    register_handle(coord, ha)
    hb = _handle("b" * 40, "queued")
    register_handle(coord, hb)
    queued, ev = coord.check_running_tasks(
        _row("b" * 40, State.QUEUED), "dl")
    assert queued is True
    assert ev is not None
    # Forced rows never queue.
    coord._forced = {"b" * 40}
    queued2, _ = coord.check_running_tasks(
        _row("b" * 40, State.QUEUED), "dl")
    assert queued2 is False
    # Move lane mirrors with its own cap.
    hm = _handle("c" * 40, "moving")
    register_handle(coord, hm)
    hd = _handle("d" * 40, "moving")
    register_handle(coord, hd)
    queued3, ev3 = coord.check_running_tasks(
        _row("d" * 40, State.MOVING), "move")
    assert queued3 is True and ev3 is not None
    # No handles at all: admittable (today's edge behavior).
    coord2 = make_coordinator(MagicMock())
    coord2.cfg.max_active_downloads = 1
    assert coord2.check_running_tasks(
        _row("e" * 40, State.QUEUED), "dl") == (False, None)


@pytest.mark.anyio
async def test_park_waits_without_db_churn_and_wakes_by_priority():
    """Parked workers idle (no upsert); start_from_queued wakes oldest."""
    from unittest.mock import MagicMock
    from racing_sync.state import State
    from racing_sync.task import TaskHandle, register_handle
    import datetime as dt

    coord = _coord()
    store = coord.store
    # Lane full: A downloading.
    ha = _handle("a" * 40, "downloading")
    register_handle(coord, ha)
    old = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    new = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
    ts_b = _row("b" * 40, State.QUEUED, updated_at=old)
    ts_c = _row("c" * 40, State.QUEUED, updated_at=new)
    store.get = MagicMock(side_effect=lambda h, **k: {
        "b" * 40: ts_b, "c" * 40: ts_c}.get(h))
    hb = _handle("b" * 40, "queued")
    hc = _handle("c" * 40, "queued")
    register_handle(coord, hb)
    register_handle(coord, hc)

    park_b = asyncio.ensure_future(coord._park_queued_for_download_slot(ts_b))
    park_c = asyncio.ensure_future(coord._park_queued_for_download_slot(ts_c))
    await asyncio.sleep(0.1)
    assert not park_b.done() and not park_c.done()
    store.upsert.assert_not_called()
    assert set(coord._queued_lane["dl"]) == {"b" * 40, "c" * 40}
    # Still full: nothing wakes.
    assert await coord.start_from_queued() == 0
    # A leaves: oldest queued (B) wakes first.
    from racing_sync.task import unregister_handle
    unregister_handle(coord, ha)
    woken = await coord.start_from_queued()
    assert woken == 1
    assert await asyncio.wait_for(park_b, timeout=5.0) is False
    assert hb.queued is False
    assert not park_c.done()
    for p in (park_b, park_c):
        if not p.done():
            p.cancel()
    try:
        await asyncio.gather(park_b, park_c, return_exceptions=True)
    except (asyncio.CancelledError, Exception):
        pass


@pytest.mark.anyio
async def test_try_admit_honors_force():
    coord = _coord()
    from racing_sync.state import State
    coord._forced = {"b" * 40}
    assert await coord._try_admit_download(
        _row("b" * 40, State.QUEUED)) is True


@pytest.mark.anyio
async def test_forcestart_single_arms_and_wakes():
    """Full-hash /forcestart sets in-memory force + wakes the waiter."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.config import TelegramConfig
    from racing_sync.state import State
    from racing_sync.task import TaskHandle, register_handle
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._store = MagicMock()
    bot._store.get.return_value = _row("b" * 40, State.QUEUED)
    bot._coord = SimpleNamespace()
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    bot._last_active_cache = None
    hd = TaskHandle(infohash="b" * 40, gid="b" * 12, kind="queued")
    register_handle(bot, hd)
    await bot._start_single_command("forcestart", "b" * 40, MagicMock())
    assert bot._coord._forced == {"b" * 40}
    assert hd.force is True and hd.event.is_set()
    assert "Force-start armed" in bot._reply.call_args[0][0]


@pytest.mark.anyio
async def test_forcestart_group_number_arms_group():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.config import TelegramConfig
    from racing_sync.state import State
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = [
        _row("a" * 40, State.QUEUED, total_bytes=1000),
        _row("b" * 40, State.QUEUED, total_bytes=2000),
    ]
    bot._coord = SimpleNamespace()
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    bot._last_active_cache = None
    await bot._start_group_command("forcestart", "2", MagicMock())
    assert bot._coord._forced == {"b" * 40}
    assert "Force-start armed" in bot._reply.call_args[0][0]


def test_forcestart_command_registered():
    import re
    from racing_sync.telegram_protocol import _CHAT_COMMANDS

    names = [c.name for c in _CHAT_COMMANDS]
    assert "forcestart" in names
    row = next(c for c in _CHAT_COMMANDS if c.name == "forcestart")
    assert row.pattern.match("/forcestart_" + "a" * 40)
    assert not row.pattern.match("/now_" + "a" * 40)
