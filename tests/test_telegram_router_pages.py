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
