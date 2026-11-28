"""Regression (audit H1): positional commands honor the active status filter.

The active list numbers only the rows passing `_status_filter`
(telegram_refresh.py), but `/act_1`, `/start_1`, `/cancel_1` and
`/unhold_1` used to resolve their number against the raw unfiltered
store list. With a DL filter and a MOVING row first in that raw list,
`/act_1` (or `/cancel_1`) hit the MOVING torrent while the displayed
#1 was the DOWNLOADING one.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from racing_sync.state import State, TorrentState

# Raw store order: the MOVING row is first, exactly the misroute shape.
# Hashes must be valid hex — member buttons/hash parsing validate that.
MOVING_HASH = "a" * 40
DOWNLOADING_HASH = "d" * 40


def _store_rows():
    return [
        TorrentState(
            source_infohash=MOVING_HASH,
            source_name="Moving.Show",
            state=State.MOVING,
            total_bytes=1000,
        ),
        TorrentState(
            source_infohash=DOWNLOADING_HASH,
            source_name="Downloading.Show",
            state=State.DOWNLOADING,
            total_bytes=2000,
        ),
    ]


def _bot():
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = MagicMock()
    bot._cfg.chat_id = "1"
    bot._cfg.page_size = 5
    bot._callback_times = {}
    bot._current_page = 0
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = _store_rows()
    bot._refresh_active_message = AsyncMock()
    bot._reply = AsyncMock()
    bot._send_sheet = AsyncMock()
    bot._last_active_cache = None
    return bot


def _message():
    m = MagicMock()
    m.chat.id = "1"
    m.from_user.id = "9"
    m.text = "/start_1"
    m.caption = None
    m.message_id = 42
    return m


@pytest.mark.anyio
async def test_act_positional_resolves_within_status_filter():
    """`/act_1` under the DL filter opens the DOWNLOADING group."""
    bot = _bot()
    bot._live_notes_for = MagicMock(return_value={})
    bot._sheet_text_and_rows = MagicMock(return_value=("sheet", []))

    bot._status_filter = "dl"
    await bot._act_command_entry("1", _message())
    members = bot._sheet_text_and_rows.call_args.args[0]
    assert [r.source_infohash for r in members] == [DOWNLOADING_HASH]

    # Unfiltered behavior is unchanged: #1 is the raw first row.
    bot._status_filter = "all"
    bot._sheet_text_and_rows.reset_mock()
    await bot._act_command_entry("1", _message())
    members = bot._sheet_text_and_rows.call_args.args[0]
    assert [r.source_infohash for r in members] == [MOVING_HASH]


@pytest.mark.anyio
async def test_now_positional_resolves_within_status_filter():
    """`/start_1` under the DL filter starts the DOWNLOADING copy."""
    bot = _bot()
    bot._live_notes_for = MagicMock(return_value={})
    bot._now_eligible_hashes = MagicMock(
        side_effect=lambda members, notes: [r.source_infohash for r in members]
    )
    bot._fetch_torrent = AsyncMock(return_value="Fetching original")

    bot._status_filter = "dl"
    await bot._start_group_command("now", "1", _message())
    assert bot._fetch_torrent.await_args.args[0] == DOWNLOADING_HASH

    bot._status_filter = "all"
    bot._fetch_torrent.reset_mock()
    await bot._start_group_command("now", "1", _message())
    assert bot._fetch_torrent.await_args.args[0] == MOVING_HASH


@pytest.mark.anyio
async def test_cancel_positional_resolves_within_status_filter():
    """`/cancel_1` under the DL filter offers the DOWNLOADING hash only."""
    bot = _bot()

    bot._status_filter = "dl"
    await bot._start_group_command("cancel", "1", _message())
    rows = bot._send_sheet.await_args.args[1]
    data = [d for row in rows for (_label, d) in row]
    assert f"forget:{DOWNLOADING_HASH}:1" in data
    assert f"forget:{MOVING_HASH}:1" not in data

    bot._status_filter = "all"
    bot._send_sheet.reset_mock()
    await bot._start_group_command("cancel", "1", _message())
    rows = bot._send_sheet.await_args.args[1]
    data = [d for row in rows for (_label, d) in row]
    assert f"forget:{MOVING_HASH}:1" in data
    assert f"forget:{DOWNLOADING_HASH}:1" not in data


@pytest.mark.anyio
async def test_resume_positional_resolves_within_status_filter():
    """`/unhold_1` under the DL filter unholds the DOWNLOADING hash only."""
    bot = _bot()
    bot._resume_hashes_reply = AsyncMock(return_value="Resumed")

    bot._status_filter = "dl"
    await bot._resume_command_entry("1", _message())
    assert bot._resume_hashes_reply.await_args.args[0] == [DOWNLOADING_HASH]

    bot._status_filter = "all"
    bot._resume_hashes_reply.reset_mock()
    await bot._resume_command_entry("1", _message())
    assert bot._resume_hashes_reply.await_args.args[0] == [MOVING_HASH]
