from __future__ import annotations

from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import render_active, TelegramBot
from racing_sync.config import TelegramConfig


def test_render_active_empty():
    text, cur_page, total_pages = render_active([], page=0, page_size=5)
    assert cur_page == 0
    assert total_pages == 1
    assert "No active tasks" in text


def test_render_active_pagination_and_numbering():
    tasks = []
    for i in range(12):
        ts = TorrentState(
            source_infohash=f"hash{i:02d}abcdef1234567890",
            source_name=f"Release.Title.S01E{i+1:02d}.1080p.mkv",
            state=State.DOWNLOADING if i % 2 == 0 else State.QUEUED,
            total_bytes=(i + 1) * 1_000_000_000,
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
    assert "*1.*" in text0
    assert "*5.*" in text0
    assert "*6.*" not in text0
    assert "45.0%" in text0

    # Page 1 (items 6..10)
    text1, p1, total1 = render_active(tasks, page=1, page_size=5)
    assert p1 == 1
    assert total1 == 3
    assert "*Page 2/3*" in text1
    assert "*6.*" in text1
    assert "*10.*" in text1
    assert "*1.*" not in text1
    assert "*11.*" not in text1

    # Page 2 (items 11..12)
    text2, p2, total2 = render_active(tasks, page=2, page_size=5)
    assert p2 == 2
    assert total2 == 3
    assert "*Page 3/3*" in text2
    assert "*11.*" in text2
    assert "*12.*" in text2
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
