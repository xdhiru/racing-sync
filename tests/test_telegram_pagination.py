from __future__ import annotations

import pytest

from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import render_active, TelegramBot
from racing_sync.config import TelegramConfig


@pytest.fixture
def anyio_backend():
    return "asyncio"


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
        cross_seed_source="test-indexer-api-cross-seed",
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
    assert "SSD source: test-indexer-api-cross-seed" in detail
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


@pytest.mark.anyio
async def test_send_one_detail_handles_timedelta_retry_after():
    import asyncio
    import datetime as dt
    from unittest.mock import AsyncMock, MagicMock, patch
    from telegram.error import RetryAfter

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._store = MagicMock()
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")

    ts = TorrentState(source_infohash="abc123", source_name="Test")
    bot._store.get.return_value = ts
    bot._store.get_telegram_message_id.return_value = None

    class MockRetryAfter(RetryAfter):
        def __init__(self, retry_after):
            self._mock_retry = retry_after
            super().__init__(1)
        @property
        def retry_after(self):
            return self._mock_retry

    err = MockRetryAfter(dt.timedelta(seconds=5))
    bot._bot.send_message = AsyncMock(side_effect=err)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await bot._send_one_detail("abc123", 0.5)
        mock_sleep.assert_awaited_once_with(6)
    assert not bot._detail_queue.empty()


def test_render_active_deterministic_cache_key():
    import time
    ts = TorrentState(
        source_infohash="hash1",
        source_name="My.Show.S01E01",
        state=State.DOWNLOADING,
        total_bytes=1000,
    )
    t1, _, _ = render_active([(ts, 0.5)], page=0, page_size=5)
    time.sleep(0.01)
    t2, _, _ = render_active([(ts, 0.5)], page=0, page_size=5)
    assert t1 == t2


def test_telegram_outbound_rate_interval():
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123", outbound_rate=5)
    interval = 1.0 / max(1, getattr(bot._cfg, "outbound_rate", 1))
    assert pytest.approx(interval, 0.01) == 0.2

    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123", outbound_rate=1)
    interval = 1.0 / max(1, getattr(bot._cfg, "outbound_rate", 1))
    assert pytest.approx(interval, 0.01) == 1.0


def test_render_detail_and_active_4096_char_cap():
    from racing_sync.telegram_bot import render_detail
    ts = TorrentState(
        source_infohash="longhash123",
        source_name="A" * 5000,
        last_error="E" * 5000,
    )
    detail_text = render_detail(ts)
    assert len(detail_text) <= 4096
    assert detail_text.endswith("...")

    active_text, _, _ = render_active([(ts, 0.5)], page=0, page_size=1)
    assert len(active_text) <= 4096
    assert active_text.endswith("...")


@pytest.mark.anyio
async def test_active_repost_deletes_and_resends_as_newest():
    import time
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345",
                              active_repost_interval_seconds=10)
    bot._bot = MagicMock()
    bot._bot.delete_message = AsyncMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=222))
    bot._bot.edit_message_text = AsyncMock()
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = []
    bot._store.set_meta = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._current_page = 0
    bot._active_msg_id = 111
    bot._prev_active_msg_id = 111
    bot._last_active_cache = None
    bot._last_repost_monotonic = time.monotonic() - 30.0
    # Our own newer traffic (id 150) buried the status message (id 111).
    bot._newest_outbound_id = 150

    await bot._refresh_active_message()

    bot._bot.delete_message.assert_awaited_once_with(chat_id="12345", message_id=111)
    bot._bot.send_message.assert_awaited_once()
    send_kwargs = bot._bot.send_message.call_args[1]
    assert send_kwargs.get("disable_notification") is True
    bot._bot.edit_message_text.assert_not_called()
    assert bot._active_msg_id == 222


@pytest.mark.anyio
async def test_active_repost_skipped_when_already_last():
    """Interval elapsed but nothing newer arrived: no churn, stay put."""
    import time
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345",
                              active_repost_interval_seconds=10)
    bot._bot = MagicMock()
    bot._bot.delete_message = AsyncMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=333))
    bot._bot.edit_message_text = AsyncMock()
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = []
    bot._store.set_meta = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._current_page = 0
    bot._active_msg_id = 111
    bot._prev_active_msg_id = 111
    bot._last_active_cache = None
    bot._last_repost_monotonic = time.monotonic() - 3600.0
    # Newest known outbound IS the status message: still last.
    bot._newest_outbound_id = 111

    await bot._refresh_active_message()

    bot._bot.delete_message.assert_not_called()
    bot._bot.send_message.assert_not_called()
    bot._bot.edit_message_text.assert_awaited_once()
    assert bot._active_msg_id == 111


@pytest.mark.anyio
async def test_active_repost_skipped_when_disabled_or_not_due():
    import time
    from unittest.mock import AsyncMock, MagicMock
    for interval, last_repost_ago in ((0, 3600.0), (60, 5.0)):
        bot = object.__new__(TelegramBot)
        bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345",
                                  active_repost_interval_seconds=interval)
        bot._bot = MagicMock()
        bot._bot.delete_message = AsyncMock()
        bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=333))
        bot._bot.edit_message_text = AsyncMock()
        bot._store = MagicMock()
        bot._store.list_active_inflight.return_value = []
        bot._store.set_meta = MagicMock()
        bot._coord = MagicMock()
        bot._coord.live_progress_map.return_value = {}
        bot._current_page = 0
        bot._active_msg_id = 111
        bot._prev_active_msg_id = 111
        bot._last_active_cache = None
        bot._last_repost_monotonic = time.monotonic() - last_repost_ago

        await bot._refresh_active_message()

        bot._bot.delete_message.assert_not_called()
        bot._bot.send_message.assert_not_called()
        # Identical content still takes the quiet edit path, not a repost.
        bot._bot.edit_message_text.assert_awaited_once()
        assert bot._active_msg_id == 111


@pytest.mark.anyio
async def test_callback_authentication_and_throttling():
    from unittest.mock import AsyncMock, MagicMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._last_callback_time = 0.0
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = []
    bot._refresh_active_message = AsyncMock()

    # Unauthorized callback from wrong chat
    unauth_query = MagicMock()
    unauth_query.message.chat.id = 99999
    unauth_query.from_user.id = 99999
    unauth_query.answer = AsyncMock()

    await bot._handle_callback(unauth_query)
    unauth_query.answer.assert_awaited_once_with("Unauthorized", show_alert=True)
    bot._refresh_active_message.assert_not_called()

    # Authorized callback
    auth_query = MagicMock()
    auth_query.message.chat.id = 12345
    auth_query.data = "page:refresh"
    auth_query.answer = AsyncMock()

    await bot._handle_callback(auth_query)
    bot._refresh_active_message.assert_called_once()

    # Immediate second click (throttled)
    bot._refresh_active_message.reset_mock()
    await bot._handle_callback(auth_query)
    bot._refresh_active_message.assert_not_called()


@pytest.mark.anyio
async def test_detail_queue_overflow_evicts_oldest():
    import asyncio
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._bot = object()
    bot._detail_queue = asyncio.Queue(maxsize=2)

    ts1 = TorrentState(source_infohash="hash1", source_name="Show 1")
    ts2 = TorrentState(source_infohash="hash2", source_name="Show 2")
    ts3 = TorrentState(source_infohash="hash3", source_name="Show 3")

    await bot.ensure_detail_message(ts1, 0.1)
    await bot.ensure_detail_message(ts2, 0.2)
    assert bot._detail_queue.full()

    # Adding ts3 when full should evict ts1 and keep ts2, ts3
    await bot.ensure_detail_message(ts3, 0.3)
    assert bot._detail_queue.full()

    item1 = bot._detail_queue.get_nowait()
    assert item1 == ("hash2", 0.2)
    item2 = bot._detail_queue.get_nowait()
    assert item2 == ("hash3", 0.3)


@pytest.mark.anyio
async def test_active_msg_id_restored_from_store():
    from unittest.mock import AsyncMock, MagicMock, patch
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._store = MagicMock()
    bot._store.all.return_value = []
    bot._store.get_meta.return_value = "778899"
    bot._detail_cache = {}

    with patch("racing_sync.telegram_bot.Bot") as mock_bot_cls:
        mock_bot = MagicMock()
        mock_bot.send_message = AsyncMock()
        mock_bot_cls.return_value = mock_bot

        with patch("asyncio.create_task", return_value=MagicMock()):
            await bot.start()

    assert bot._active_msg_id == 778899
    assert bot._prev_active_msg_id == 778899
    bot._store.get_meta.assert_called_with("telegram_active_msg_id")


def test_safe_truncate_markdown_balances_tags():
    from racing_sync.telegram_bot import _safe_truncate_markdown

    # Unclosed backtick within max_len limit
    long_code = "`" + "A" * 5000
    truncated = _safe_truncate_markdown(long_code, max_len=100)
    assert len(truncated) <= 100
    assert truncated.endswith("...")
    assert truncated.count("`") % 2 == 0

    # Unclosed bold and italic
    long_bold = "*_bold_italic " + "B" * 5000
    truncated_bold = _safe_truncate_markdown(long_bold, max_len=100)
    assert len(truncated_bold) <= 100
    assert truncated_bold.endswith("...")
    assert truncated_bold.count("*") % 2 == 0
    assert truncated_bold.count("_") % 2 == 0


@pytest.mark.anyio
async def test_notify_falls_back_to_plain_text_on_parse_error():
    from unittest.mock import AsyncMock, MagicMock
    from telegram.error import TelegramError

    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._bot = MagicMock()

    # First call with MARKDOWN raises entity parse error; second call succeeds
    bot._bot.send_message = AsyncMock(
        side_effect=[
            TelegramError("Can't find end of the entity starting at byte offset 20"),
            MagicMock(message_id=999),
        ]
    )

    await bot.notify("Download [unclosed bracket failed for test_torrent_name")
    assert bot._bot.send_message.call_count == 2
    first_call = bot._bot.send_message.call_args_list[0]
    second_call = bot._bot.send_message.call_args_list[1]
    assert "parse_mode" in first_call[1]
    assert "parse_mode" not in second_call[1]




