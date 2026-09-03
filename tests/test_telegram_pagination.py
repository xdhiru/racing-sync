from __future__ import annotations

import pytest

from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import render_active, TelegramBot
from racing_sync.config import TelegramConfig


def test_render_active_empty():
    text, cur_page, total_pages = render_active([], page=0, page_size=5)
    assert cur_page == 0
    assert total_pages == 1
    assert "No active tasks" in text
    assert "`" not in text  # No backticks in idle message


def test_render_active_pagination_and_numbering():
    tasks = []
    for i in range(12):
        ts = TorrentState(
            source_infohash=f"hash{i:02d}abcdef1234567890abcdef1234567890",
            source_name=f"[SubsPlease] Release Title - {i+1:02d} (1080p) [ABCD{i:04d}].mkv",
            state=State.DOWNLOADING if i % 2 == 0 else State.QUEUED,
            source_announce_url="http://nyaa.tracker.wf:7777/announce",
            total_bytes=(i + 1) * 1024 * 1024 * 1024,
            batch_index=0,
            batches_total=2,
        )
        prog = 0.45 if i % 2 == 0 else None
        tasks.append((ts, prog))

    # Page 0 (items 1..5)
    text0, p0, total0 = render_active(tasks, page=0, page_size=5)
    assert p0 == 0
    assert total0 == 3
    assert "*Page 1/3*" in text0

    # 1. Full name is visible in backticks, and NO backslash escapes for brackets
    assert "*1.* `[SubsPlease] Release Title - 01 (1080p) [ABCD0000].mkv`" in text0
    assert "\\[" not in text0
    assert "\\]" not in text0

    # 2. No ↳ symbol anywhere
    assert "↳" not in text0

    # 3. Size in plain text, followed by dot and full hash in backticks
    assert "  1.0 GB · `hash00abcdef1234567890abcdef1234567890`" in text0

    # 4. Next line shows state, batch, and tracker domain at the end without backticks
    assert "  ⬇️ Downloading · 45.0% · Batch 0/2 · nyaa.tracker.wf" in text0
    assert "  📋 Queued · Batch 0/2 · nyaa.tracker.wf" in text0

    # Page 1 (items 6..10)
    text1, p1, total1 = render_active(tasks, page=1, page_size=5)
    assert p1 == 1
    assert total1 == 3
    assert "*Page 2/3*" in text1
    assert "*6.* `[SubsPlease] Release Title - 06 (1080p) [ABCD0005].mkv`" in text1
    assert "*10.* `[SubsPlease] Release Title - 10 (1080p) [ABCD0009].mkv`" in text1
    assert "*1.*" not in text1
    assert "*11.*" not in text1

    # Page 2 (items 11..12)
    text2, p2, total2 = render_active(tasks, page=2, page_size=5)
    assert p2 == 2
    assert total2 == 3
    assert "*Page 3/3*" in text2
    assert "*11.* `[SubsPlease] Release Title - 11 (1080p) [ABCD0010].mkv`" in text2
    assert "*12.* `[SubsPlease] Release Title - 12 (1080p) [ABCD0011].mkv`" in text2
    assert "*13.*" not in text2

    # Clamping out-of-bounds page
    text_clamp, p_clamp, total_clamp = render_active(tasks, page=99, page_size=5)
    assert p_clamp == 2
    assert total_clamp == 3
    assert "*Page 3/3*" in text_clamp


def test_keyboard_builder():
    bot = TelegramBot(TelegramConfig(bot_token="fake:token", chat_id="12345"), None, None)
    
    # 1 page -> refresh only
    kb1 = bot._build_keyboard(current_page=0, total_pages=1)
    assert kb1 is not None
    assert len(kb1.inline_keyboard) == 1
    assert kb1.inline_keyboard[0][0].text == "🔄 Refresh"

    # 3 pages -> Prev, Page, Next, Refresh
    kb3 = bot._build_keyboard(current_page=1, total_pages=3)
    assert kb3 is not None
    assert len(kb3.inline_keyboard) == 2
    row0 = kb3.inline_keyboard[0]
    assert row0[0].text == "◀️ Prev"
    assert row0[1].text == "2 / 3"
    assert row0[2].text == "Next ▶️"
    assert kb3.inline_keyboard[1][0].text == "🔄 Refresh"


def test_render_detail_formatting():
    from racing_sync.telegram_bot import render_detail

    ts = TorrentState(
        source_infohash="c27de123456789abcdef0123456789abcdef0123",
        source_name="[SubsPlease] Game.Day.Murders.S01E06.1080p.mkv",
        state=State.DONE,
        total_bytes=1900000000,
        source_tracker="https://aither.cc/announce/2xxxxxxxxxxxxxxsxxxxxxxxxxxxxxxxxx9b",
        classification_kind="movie",
        cross_seed_source="seedpool-cross-seed",
    )
    detail = render_detail(ts)

    # 1. State badge is plain text, name is full and in backticks (no backslashes)
    assert "✅ DONE `[SubsPlease] Game.Day.Murders.S01E06.1080p.mkv`" in detail
    assert "\\[" not in detail
    assert "\\]" not in detail

    # 2. Full 40-character hash in backticks
    assert "`c27de123456789abcdef0123456789abcdef0123` · 1.8 GB" in detail

    # 3. Source displays only domain (no secret passkey URL, no backticks)
    assert "Source: aither.cc" in detail
    assert "2xxxxxxxxxxxxxxs" not in detail

    # 4. Other fields not in backticks
    assert "SSD source: seedpool-cross-seed" in detail
    assert "Classifier: movie" in detail


@pytest.mark.anyio
async def test_refresh_live_status_filters_by_hashes():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator, LiveItem
    from racing_sync.clients.abstract import Torrent

    coord = object.__new__(Coordinator)
    coord.dest_client = AsyncMock()
    coord._live = {}

    # Case 1: when _live is empty, list_torrents should not even be called
    await coord._refresh_live_status()
    coord.dest_client.list_torrents.assert_not_called()

    # Case 2: when _live has entries, list_torrents should be called with hashes
    coord._live = {
        "hash1": LiveItem("hash1", "Show1", "downloading", 0.1, 100.0),
        "hash2": LiveItem("hash2", "Show2", "downloading", 0.5, 200.0),
    }
    coord.dest_client.list_torrents.return_value = [
        Torrent(
            hash="hash1",
            name="Show1",
            category="racing",
            save_path="",
            size_bytes=1024 * 1024 * 500,
            state="downloading",
            progress=0.8,
        )
    ]

    await coord._refresh_live_status()
    coord.dest_client.list_torrents.assert_awaited_once_with(hashes=["hash1", "hash2"])
    assert coord._live["hash1"].progress == 0.8

