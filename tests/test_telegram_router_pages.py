"""Router pagination pages over rendered groups; refresh taps keep the dedup cache."""
from __future__ import annotations

import pytest

from racing_sync.config import TelegramConfig
from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import TelegramBot


def _row(h: str, name: str, size: int = 1000) -> TorrentState:
    return TorrentState(
        source_infohash=h, source_name=name, state=State.QUEUED,
        total_bytes=size,
        source_announce_url="https://alpha.cc/announce/xyz")


def _page_bot(rows, **kw):
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345",
                              page_size=5)
    bot._callback_times = {}
    bot._current_page = 0
    bot._last_active_cache = None
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = list(rows)
    bot._refresh_active_message = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _query(data):
    from unittest.mock import AsyncMock, MagicMock
    q = MagicMock()
    q.message.chat.id = 12345
    q.from_user.id = 12345
    q.data = data
    q.answer = AsyncMock()
    return q


@pytest.mark.anyio
async def test_page_next_wraps_over_groups_not_rows():
    """6 rows in 2 groups, page_size 5: one page — next stays put.

    Row-based paging would wrap to page 2 (a dead tap the renderer
    then clamps); group-based paging matches what is rendered.
    """
    rows = [_row("a" * 40, "Twin.Show.S01E01"),
            _row("b" * 40, "Twin.Show.S01E01"),
            _row("c" * 40, "Twin.Show.S01E01"),
            _row("d" * 40, "Twin.Show.S01E01"),
            _row("e" * 40, "Twin.Show.S01E01"),
            _row("f" * 40, "Other.Show.S01E01")]
    bot = _page_bot(rows)
    await bot._handle_callback(_query("page:next"))
    assert bot._current_page == 0
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_page_next_advances_when_groups_span_pages():
    rows = [_row(f"{i:040x}"[-40:], f"Show.{i:02d}.S01E01", size=1000 + i)
            for i in range(6)]
    bot = _page_bot(rows)
    await bot._handle_callback(_query("page:next"))
    assert bot._current_page == 1
    await bot._handle_callback(_query("page:next"))
    # Debounce (0.5s window) absorbs the immediate re-tap.
    assert bot._current_page == 1


@pytest.mark.anyio
async def test_page_refresh_keeps_dedup_cache():
    """A refresh tap must not clear _last_active_cache: an unchanged
    render hits the dedup gate and skips the pointless edit."""
    bot = _page_bot([_row("a" * 40, "Twin.Show.S01E01")])
    sentinel = (0, 1, "cached-text")
    bot._last_active_cache = sentinel
    await bot._handle_callback(_query("page:refresh"))
    assert bot._last_active_cache is sentinel
    bot._refresh_active_message.assert_awaited_once()


def test_match_status_filter_shapes():
    from racing_sync.telegram_protocol import match_status_filter

    assert match_status_filter("downloading", "all") is True
    assert match_status_filter("downloading", "dl") is True
    assert match_status_filter("moving", "dl") is False
    assert match_status_filter("moving", "mv") is True
    assert match_status_filter("queued", "qd") is True
    assert match_status_filter("waiting_disk", "qd") is True
    assert match_status_filter("new", "qd") is True
    assert match_status_filter("re_adding", "rd") is True
    assert match_status_filter("downloading", "bogus") is True


@pytest.mark.anyio
async def test_status_tap_sets_filter_resets_page_clears_cache():
    bot = _page_bot([_row("a" * 40, "Twin.Show.S01E01")])
    bot._current_page = 2
    bot._status_filter = "all"
    bot._last_active_cache = (0, 1, "old")
    await bot._handle_callback(_query("status:dl"))
    assert bot._status_filter == "dl"
    assert bot._current_page == 0
    assert bot._last_active_cache is None
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_same_filter_tap_keeps_cache():
    bot = _page_bot([_row("a" * 40, "Twin.Show.S01E01")])
    bot._status_filter = "dl"
    sentinel = (0, 1, "cached")
    bot._last_active_cache = sentinel
    await bot._handle_callback(_query("status:dl"))
    assert bot._last_active_cache is sentinel
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_unknown_status_key_ignored():
    bot = _page_bot([_row("a" * 40, "Twin.Show.S01E01")])
    await bot._handle_callback(_query("status:bogus"))
    bot._refresh_active_message.assert_not_called()


@pytest.mark.anyio
async def test_filtered_refresh_narrows_render():
    """Inner honors _status_filter; detail nets still see everything."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.config import TelegramConfig
    from racing_sync.state import State, TorrentState
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(
        return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._coord.live_speed_map.return_value = {}
    bot._coord.rclone_move_progress_map.return_value = {}
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._detail_sent_state = {}
    bot._status_filter = "dl"
    mk = lambda h, st, name: TorrentState(  # noqa: E731
        source_infohash=h, source_name=name, state=st, total_bytes=1000,
        source_announce_url="https://alpha.cc/announce/xyz")
    bot._store.list_active_inflight.return_value = [
        mk("a" * 40, State.DOWNLOADING, "Dl.Show.S01E01"),
        mk("b" * 40, State.QUEUED, "Q.Show.S01E01"),
    ]
    await bot._refresh_active_message_inner()
    sent_text = bot._bot.send_message.call_args[0][1]
    assert "Dl.Show" in sent_text
    assert "Q.Show" not in sent_text
    assert bot._last_rendered_groups == 1


@pytest.mark.anyio
async def test_overview_button_sends_aggregate():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import State, TorrentState

    bot = _page_bot([])
    bot._coord = MagicMock()
    bot._coord.live_totals.return_value = (10_000_000, 2)
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = [
        TorrentState(source_infohash="a" * 40, source_name="D",
                     state=State.DOWNLOADING),
        TorrentState(source_infohash="b" * 40, source_name="M",
                     state=State.MOVING),
    ]
    bot._reply = AsyncMock()
    q = _query("status:ov")
    await bot._handle_callback(q)
    text = bot._reply.call_args[0][0]
    assert text.startswith("📊")
    assert "2 downloading" in text
    assert "DL1" in text and "MV1" in text
