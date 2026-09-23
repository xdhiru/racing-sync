from __future__ import annotations

import pytest
from conftest import make_coordinator

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
            source_name=f"[DummySub] Release Title - {i+1:02d} (1080p) [ABCD{i:04d}].mkv",
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
    assert "*Active Tasks (12)* · *Page 1/3*" in text0

    # 1. Full name is visible in backticks, and NO backslash escapes for brackets
    assert "1. `[DummySub] Release Title - 01 (1080p) [ABCD0000].mkv` · 1.0G" in text0
    assert "\\[" not in text0
    assert "\\]" not in text0

    # 2. No ↳ symbol anywhere
    assert "↳" not in text0

    # 3. Tracker + stage share one display-only ▸ line (no separate
    # Source:/size lines); positional group commands, never hashes.
    assert "▸ nyaa.tracker.wf ⬇️ Downloading · 45.0% · Batch 1/2" in text0
    assert "▸ nyaa.tracker.wf 📋 Queued · Batch 1/2" in text0
    assert "/cancel\\_1" in text0
    assert "Cancel:" not in text0

    # Page 1 (items 6..10)
    text1, p1, total1 = render_active(tasks, page=1, page_size=5)
    assert p1 == 1
    assert total1 == 3
    assert "*Active Tasks (12)* · *Page 2/3*" in text1
    assert "6. `[DummySub] Release Title - 06 (1080p) [ABCD0005].mkv`" in text1
    assert "10. `[DummySub] Release Title - 10 (1080p) [ABCD0009].mkv`" in text1
    assert "\n1. `" not in text1
    assert "11. `" not in text1

    # Page 2 (items 11..12)
    text2, p2, total2 = render_active(tasks, page=2, page_size=5)
    assert p2 == 2
    assert total2 == 3
    assert "*Active Tasks (12)* · *Page 3/3*" in text2
    assert "11. `[DummySub] Release Title - 11 (1080p) [ABCD0010].mkv`" in text2
    assert "12. `[DummySub] Release Title - 12 (1080p) [ABCD0011].mkv`" in text2
    assert "13. `" not in text2

    # Clamping out-of-bounds page
    text_clamp, p_clamp, total_clamp = render_active(tasks, page=99, page_size=5)
    assert p_clamp == 2
    assert total_clamp == 3
    assert "*Active Tasks (12)* · *Page 3/3*" in text_clamp


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
        source_name="[DummySub] Harbor.Lights.S01E06.1080p.mkv",
        state=State.DONE,
        total_bytes=1900000000,
        source_tracker="https://alpha.cc/announce/2xxxxxxxxxxxxxxsxxxxxxxxxxxxxxxxxx9b",
        classification_kind="movie",
        cross_seed_source="test-indexer-api-cross-seed",
    )
    detail = render_detail(ts)

    # 1. State badge is plain text, name is full and in backticks (no backslashes)
    assert "✅ DONE `[DummySub] Harbor.Lights.S01E06.1080p.mkv`" in detail
    assert "\\[" not in detail
    assert "\\]" not in detail

    # 2. Full 40-character hash in backticks
    assert "`c27de123456789abcdef0123456789abcdef0123` · 1.8 GB" in detail

    # 3. Source displays only domain (no secret passkey URL, no backticks)
    assert "Source: alpha.cc" in detail
    assert "2xxxxxxxxxxxxxxs" not in detail

    # 4. Other fields not in backticks
    assert "SSD source: test-indexer-api-cross-seed" in detail
    assert "Classifier: movie" in detail


@pytest.mark.anyio
async def test_refresh_live_status_filters_by_hashes():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import LiveItem
    from racing_sync.clients.abstract import Torrent

    coord = make_coordinator()
    coord.dest_client = AsyncMock()

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


def test_flood_wait_seconds_helper():
    import datetime as dt
    from unittest.mock import MagicMock
    from telegram.error import RetryAfter, TelegramError
    from racing_sync.telegram_bot import _flood_wait_seconds

    assert _flood_wait_seconds(TelegramError(
        "Flood control exceeded. Retry in 39 seconds")) == 40
    assert _flood_wait_seconds(TelegramError("Too Many Requests")) == 5
    assert _flood_wait_seconds(TelegramError("Bad Request: message is empty")) is None
    assert _flood_wait_seconds(ValueError("nope")) is None

    class _RA(RetryAfter):
        def __init__(self, v):
            self._v = v
            super().__init__(1)
        @property
        def retry_after(self):
            return self._v

    assert _flood_wait_seconds(_RA(33)) == 34
    assert _flood_wait_seconds(_RA(dt.timedelta(seconds=5))) == 6


@pytest.mark.anyio
async def test_send_one_detail_recovers_plain_flood_edit():
    """The reported bug: a flood-shaped plain TelegramError dropped the edit.

    First call hits flood on edit -> sleeps the requested window and
    re-queues; second call succeeds and the card carries the DONE state.
    """
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch
    from telegram.error import TelegramError

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._store = MagicMock()
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")

    h = "8af4598b" + "0" * 32
    ts = TorrentState(source_infohash=h, source_name="Show.S04E03",
                      state=State.DONE, total_bytes=1000)
    bot._store.get.return_value = ts
    bot._store.get_telegram_message_id.return_value = 111
    bot._detail_cache[h] = 111
    flood = TelegramError("Flood control exceeded. Retry in 39 seconds")
    bot._bot.edit_message_text = AsyncMock(side_effect=flood)

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await bot._send_one_detail(h, None)
        mock_sleep.assert_awaited_once_with(40)
    assert not bot._detail_queue.empty()
    # Nothing recorded as sent yet — the card still shows the old state.
    assert bot._sent_state_map().get(h) != State.DONE.value

    # Flood lifts: the re-queued update sends the fresh DONE text.
    bot._bot.edit_message_text = AsyncMock()
    h2, _ = bot._detail_queue.get_nowait()
    assert h2 == h
    await bot._send_one_detail(h2, None)
    sent_text = bot._bot.edit_message_text.call_args[0][0]
    assert "DONE" in sent_text
    assert bot._sent_state_map().get(h) == State.DONE.value


@pytest.mark.anyio
async def test_send_one_detail_send_path_flood_requeues():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch
    from telegram.error import TelegramError

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._store = MagicMock()
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")

    h = "b" * 40
    bot._store.get.return_value = TorrentState(
        source_infohash=h, source_name="New.Show", state=State.QUEUED)
    bot._store.get_telegram_message_id.return_value = None
    bot._bot.send_message = AsyncMock(
        side_effect=TelegramError("Too Many Requests: retry after 12"))

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await bot._send_one_detail(h, None)
        mock_sleep.assert_awaited_once_with(13)
    assert not bot._detail_queue.empty()


@pytest.mark.anyio
async def test_send_one_detail_non_flood_error_still_drops():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch
    from telegram.error import TelegramError

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._store = MagicMock()
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")

    h = "c" * 40
    bot._store.get.return_value = TorrentState(
        source_infohash=h, source_name="Bad.Show", state=State.DOWNLOADING)
    bot._store.get_telegram_message_id.return_value = 111
    bot._detail_cache[h] = 111
    bot._bot.edit_message_text = AsyncMock(
        side_effect=TelegramError("Bad Request: message is too long"))

    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await bot._send_one_detail(h, None)
        mock_sleep.assert_not_called()
    assert bot._detail_queue.empty()


@pytest.mark.anyio
async def test_stale_net_requeues_drifted_cards_only():
    """Active refresh heals cards whose sent state drifted from the row."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    async def _run(sent_map):
        bot = object.__new__(TelegramBot)
        bot._bot = MagicMock()
        bot._bot.edit_message_text = AsyncMock()
        bot._bot.send_message = AsyncMock(
            return_value=MagicMock(message_id=99))
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
        bot._detail_sent_state = dict(sent_map)
        h = "d" * 40
        bot._store.list_active_inflight.return_value = [
            TorrentState(source_infohash=h, source_name="Drifted",
                         state=State.DONE, total_bytes=1000),
        ]
        await bot._refresh_active_message_inner()
        got = []
        while not bot._detail_queue.empty():
            got.append(bot._detail_queue.get_nowait()[0])
        return got

    # Stale card (sent QUEUED, row DONE) gets re-queued...
    assert await _run({"d" * 40: "queued"}) == ["d" * 40]
    # ...while a fresh card stays silent.
    assert await _run({"d" * 40: "done"}) == []


def test_render_active_shows_wait_note_for_deferred_rows():
    waiter = TorrentState(
        source_infohash="e" * 40, source_name="Twin.Show.S01E01",
        state=State.NEW, total_bytes=1000,
        source_announce_url="https://alpha.cc/announce/xyz",
    )
    fresh = TorrentState(
        source_infohash="f" * 40, source_name="Fresh.Show.S01E01",
        state=State.NEW, total_bytes=1000,
    )
    notes = {"e" * 40: "Waiting turn · dl-indexer.example.net copy first"}
    text, _, _ = render_active([(waiter, None), (fresh, None)],
                               page=0, page_size=5, notes=notes)
    # Compact wait form with the short winning-tracker label on the
    # tracker's ▸ line (no separate Source: line).
    assert "1. `Twin.Show.S01E01`" in text
    assert "▸ alpha.cc ⏳ Wait dl-indexer first" in text
    # Row without a note keeps the plain NEW badge.
    assert "🆕 New" in text
    # Notes never leak onto other states.
    other = TorrentState(
        source_infohash="e" * 40, source_name="Twin.Show.S01E01",
        state=State.DOWNLOADING, total_bytes=1000,
    )
    text2, _, _ = render_active([(other, 0.5)], page=0, page_size=5,
                                notes=notes)
    assert "Waiting turn" not in text2
    assert "⬇️ Downloading · 50.0%" in text2


def test_render_active_groups_same_file_trackers():
    """Same file from two trackers: one heading, two lines, gid commands."""
    from racing_sync.telegram_bot import pick_snapshot

    lead = TorrentState(
        source_infohash="a" * 40, source_name="Twin.Show.S01E01",
        state=State.WAITING_INDEXER, total_bytes=1000, indexer_attempts=3,
        source_announce_url="https://alpha.cc/announce/xyz",
    )
    sib = TorrentState(
        source_infohash="b" * 40, source_name="Twin.Show.S01E01",
        state=State.DOWNLOADING, total_bytes=1000,
        source_announce_url="https://bte.example/announce",
    )
    other = TorrentState(
        source_infohash="c" * 40, source_name="Other.Show.S01E01",
        state=State.QUEUED, total_bytes=2000,
        source_announce_url="https://alpha.cc/announce/xyz",
    )
    text, cur, total = render_active(
        [(lead, None), (sib, 0.5), (other, None)], page=0, page_size=5)
    assert cur == 0 and total == 1
    # Copies vs titles in the header; pages count groups.
    assert "*Active Tasks (3 copies · 2 titles)* · *Page 1/1*" in text
    # One heading per file, size once: the shared name appears exactly once.
    assert text.count("Twin.Show.S01E01") == 1
    assert "1. `Twin.Show.S01E01`" in text
    assert "2. `Other.Show.S01E01`" in text
    # Display-only tracker lines, no per-tracker commands.
    assert "▸ alpha.cc ⏳ Wait indexer miss #3" in text
    assert "▸ bte.example ⬇️ Downloading · 50.0%" in text
    assert "Source:" not in text
    # Positional group commands on ONE line (never hashes).
    assert "/cancel\\_1 /fetch\\_1" in text
    assert "/cancel\\_aaaaaaaaaa" not in text
    assert "/fetch\\_aaaaaaaaaa" not in text
    assert "/keep" not in text and "/prefer" not in text
    # Sibling choice rides on pick snapshots (hash, label) pairs.
    from racing_sync.telegram_bot import pick_snapshot
    members = [(lead, None), (sib, 0.5)]
    snap = pick_snapshot(members, "fetch")
    assert snap == [("a" * 40, "alpha")]
    snap_cancel = pick_snapshot(members, "cancel")
    assert [h for (h, _) in snap_cancel] == ["a" * 40, "b" * 40]
    assert snap_cancel[1] == ("b" * 40, "bte")
    assert pick_snapshot(members, "prefer") == []


def test_render_active_sibling_prefer_uses_own_hash():
    """Prefer renders a group command; the snapshot carries each hash."""
    from racing_sync.telegram_bot import pick_snapshot

    lead = TorrentState(
        source_infohash="a" * 40, source_name="Twin.Show.S01E01",
        state=State.QUEUED, total_bytes=1000,
        source_announce_url="https://alpha.cc/announce/xyz",
    )
    sib = TorrentState(
        source_infohash="b" * 40, source_name="Twin.Show.S01E01",
        state=State.NEW, total_bytes=1000,
        source_announce_url="https://bte.example/announce",
    )
    notes = {"b" * 40: "Waiting for preferred copy (grace 12m left)"}
    text, _, _ = render_active([(lead, None), (sib, None)],
                               page=0, page_size=5, notes=notes)
    # One group, one heading; group commands on one line (no hashes).
    assert text.count("Twin.Show.S01E01") == 1
    assert "/cancel\\_1 /prefer\\_1" in text
    assert "/cancel\\_aaaaaaaaaa" not in text
    assert "/prefer\\_bbbbbbbbbb" not in text
    assert "/fetch" not in text
    # The grace-held sibling snapshots with its own hash.
    snap = pick_snapshot([(lead, None), (sib, None)], "prefer", notes)
    assert snap == [("b" * 40, "bte")]


def test_pick_snapshot_skips_malformed_and_disambiguates():
    """Bad hashes never snapshot; repeat labels gain the short hash."""
    from racing_sync.telegram_bot import pick_snapshot

    mk = lambda h, st, dom, name="Show.X": TorrentState(
        source_infohash=h, source_name=name,
        state=st, total_bytes=1000,
        source_announce_url=f"https://{dom}/announce")
    a = mk("a" * 40, State.WAITING_INDEXER, "alpha.cc", "Show.One.S01E01")
    bad = mk("zzz-not-hex", State.WAITING_INDEXER, "evil.example", "Show.One.S01E01")
    assert pick_snapshot([(a, None), (bad, None)], "fetch") == [
        ("a" * 40, "alpha")]

    b = mk("b" * 40, State.WAITING_INDEXER, "alpha.cc", "Show.Two.S01E01")
    assert pick_snapshot([(b, None)], "fetch") == [("b" * 40, "alpha")]
    # Same label twice inside one group: second gains the short hash.
    c = mk("c" * 40, State.WAITING_INDEXER, "alpha.cc", "Dupe.Show.S01E01")
    d = mk("d" * 40, State.WAITING_INDEXER, "alpha.cc", "Dupe.Show.S01E01")
    assert pick_snapshot([(c, None), (d, None)], "fetch") == [
        ("c" * 40, "alpha"), ("d" * 40, f"alpha {'d' * 6}")]


def test_render_detail_shows_wait_note():
    from racing_sync.telegram_bot import render_detail

    ts = TorrentState(source_infohash="e" * 40, source_name="Twin.Show",
                      state=State.NEW, total_bytes=1000)
    assert "Waiting turn" not in render_detail(ts)
    assert "⏳ Waiting turn · dl-indexer.example.net copy first" in render_detail(
        ts, None, "Waiting turn · dl-indexer.example.net copy first")


@pytest.mark.anyio
async def test_refresh_attaches_watch_wait_notes():
    """Active refresh resolves deferral notes via the coordinator."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(
        return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._coord._watch_wait_note = MagicMock(
        return_value="Waiting turn · dl-indexer.example.net copy first")
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._detail_sent_state = {}
    h = "e" * 40
    bot._store.list_active_inflight.return_value = [
        TorrentState(source_infohash=h, source_name="Twin.Show",
                     state=State.NEW, total_bytes=1000,
                     source_announce_url="https://alpha.cc/announce/xyz"),
    ]
    await bot._refresh_active_message_inner()
    sent_text = bot._bot.send_message.call_args[0][1]
    assert "⏳ Wait dl-indexer first" in sent_text
    bot._coord._watch_wait_note.assert_called_once()


def test_inflight_note_for_shapes():
    from unittest.mock import MagicMock
    from racing_sync.telegram_bot import _inflight_note_for

    ts = TorrentState(source_infohash="a" * 40, source_name="Show",
                      state=State.WAITING_INDEXER)
    leader = TorrentState(
        source_infohash="b" * 40, source_name="Show",
        state=State.DOWNLOADING, total_bytes=1000,
        source_announce_url="https://bte.example/announce")
    assert _inflight_note_for(ts, leader) == (
        "Waiting for bte copy · downloading")
    assert _inflight_note_for(ts, None) == ""
    # Test doubles answer truthy to everything: never a note.
    assert _inflight_note_for(ts, MagicMock()) == ""
    assert _inflight_note_for(ts, "not-a-row") == ""


def test_render_active_waiting_indexer_shows_deferral_note():
    h = "a" * 40
    ts = TorrentState(source_infohash=h, source_name="Twin.Show",
                      state=State.WAITING_INDEXER, total_bytes=1000,
                      indexer_attempts=13,
                      source_announce_url="https://alpha.cc/announce/xyz")
    text, _, _ = render_active(
        [(ts, None)], page=0, page_size=5,
        notes={h: "Waiting for bte copy · downloading"})
    assert "⏳ Waiting for bte copy · downloading" in text
    assert "miss #13" not in text
    # No note: the miss count stays.
    text2, _, _ = render_active([(ts, None)], page=0, page_size=5)
    assert "⏳ Wait indexer miss #13" in text2


@pytest.mark.anyio
async def test_refresh_attaches_inflight_deferral_note():
    """WAITING_INDEXER rows deferred to a leader explain themselves."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(
        return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._coord = MagicMock()
    bot._coord.live_progress_map.return_value = {}
    bot._coord._watch_wait_note = MagicMock(return_value="")
    bot._coord._inflight_same_content = MagicMock(return_value=TorrentState(
        source_infohash="b" * 40, source_name="Twin.Show",
        state=State.DOWNLOADING, total_bytes=1000,
        source_announce_url="https://bte.example/announce"))
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._detail_sent_state = {}
    h = "a" * 40
    bot._store.list_active_inflight.return_value = [
        TorrentState(source_infohash=h, source_name="Twin.Show",
                     state=State.WAITING_INDEXER, total_bytes=1000,
                     indexer_attempts=13,
                     source_announce_url="https://alpha.cc/announce/xyz"),
    ]
    await bot._refresh_active_message_inner()
    sent_text = bot._bot.send_message.call_args[0][1]
    assert "⏳ Waiting for bte copy · downloading" in sent_text
    assert "miss #13" not in sent_text


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


def _action_query(data, chat="12345", user="12345"):
    from unittest.mock import AsyncMock, MagicMock
    q = MagicMock()
    q.message.chat.id = int(chat)
    q.from_user.id = int(user)
    q.data = data
    q.answer = AsyncMock()
    return q


def _action_bot(**kw):
    from unittest.mock import AsyncMock
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
    bot._callback_times = {}
    bot._refresh_active_message = AsyncMock()
    bot._last_active_cache = None
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


@pytest.mark.anyio
async def test_callback_pick_prefer_runs_prefer_and_refreshes():
    """Tapping a member button executes prefer for that snapshot hash."""
    from unittest.mock import AsyncMock
    bot = _action_bot()
    bot._pending_pick = {
        "kind": "pick", "seq": "7", "cmd": "prefer", "title": "Twin.Show",
        "members": [("a" * 40, "alpha"), ("b" * 40, "bte")],
        "expires": 9999999999.0,
    }
    bot._prefer_torrent = AsyncMock(return_value="Preferred X")
    bot._reply = AsyncMock()
    await bot._handle_callback(_action_query("pick:7:1"))
    bot._prefer_torrent.assert_awaited_once_with("b" * 40)
    bot._reply.assert_awaited_once()
    assert bot._reply.call_args[0][0].startswith("Preferred")
    bot._refresh_active_message.assert_awaited_once()
    assert bot._pending_pick is None  # consumed: double-tap expires


@pytest.mark.anyio
async def test_callback_pick_cancel_advances_to_keep_question():
    """Tapping a member for cancel arms keepq (nothing deleted yet)."""
    from unittest.mock import AsyncMock
    bot = _action_bot()
    bot._pending_pick = {
        "kind": "pick", "seq": "7", "cmd": "cancel", "title": "Twin.Show",
        "members": [("a" * 40, "alpha"), ("b" * 40, "bte")],
        "expires": 9999999999.0,
    }
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    await bot._handle_callback(_action_query("pick:7:1"))
    _p = bot._pending_live()
    assert _p["kind"] == "keepq"
    assert _p["hashes"] == ["b" * 40]
    assert _p["scope"] == "bte copy"
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_callback_pick_all_arms_whole_group():
    from unittest.mock import AsyncMock
    bot = _action_bot()
    bot._pending_pick = {
        "kind": "pick", "seq": "7", "cmd": "cancel", "title": "Twin.Show",
        "members": [("a" * 40, "alpha"), ("b" * 40, "bte")],
        "expires": 9999999999.0,
    }
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    await bot._handle_callback(_action_query("pick:7:all"))
    _p = bot._pending_live()
    assert _p["kind"] == "keepq"
    assert sorted(_p["hashes"]) == ["a" * 40, "b" * 40]
    assert _p["scope"] == "all 2 copies"


@pytest.mark.anyio
async def test_callback_keep_yes_no_execute():
    """Yes keeps files; No deletes; both consume the pending."""
    from unittest.mock import AsyncMock

    async def _run(which):
        bot = _action_bot()
        bot._pending_pick = {
            "kind": "keepq", "seq": "9", "title": "Show", "scope": "",
            "hashes": ["a" * 40], "expires": 9999999999.0,
        }
        executed = {}

        async def _fake_snapshot_cancel(hashes, title, *, delete_files):
            executed["args"] = (list(hashes), title, delete_files)
            return "done"

        bot._execute_snapshot_cancel = _fake_snapshot_cancel
        bot._reply = AsyncMock()
        bot._refresh_active_message = AsyncMock()
        q = _action_query(f"keep:9:{which}")
        await bot._on_action_button(q, f"keep:9:{which}")
        return executed, bot

    executed, bot = await _run("yes")
    assert executed["args"] == (["a" * 40], "Show", False)
    assert bot._pending_pick is None
    executed, _ = await _run("no")
    assert executed["args"] == (["a" * 40], "Show", True)


@pytest.mark.anyio
async def test_callback_abort_drops_flow():
    from unittest.mock import AsyncMock
    bot = _action_bot()
    bot._pending_pick = {
        "kind": "pick", "seq": "7", "cmd": "cancel", "title": "Twin.Show",
        "members": [("a" * 40, "alpha")], "expires": 9999999999.0,
    }
    bot._prefer_torrent = AsyncMock()
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    await bot._handle_callback(_action_query("abort:7"))
    assert bot._pending_live() is None
    bot._prefer_torrent.assert_not_called()
    bot._reply.assert_awaited_once()
    assert "cancelled" in bot._reply.call_args[0][0].lower()
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_callback_stale_seq_replies_expired():
    from unittest.mock import AsyncMock
    bot = _action_bot()
    bot._prefer_torrent = AsyncMock()
    bot._fetch_torrent = AsyncMock()
    bot._reply = AsyncMock()
    # No pending at all (or wrong seq): nothing acts.
    await bot._handle_callback(_action_query("pick:99:0"))
    bot._prefer_torrent.assert_not_called()
    bot._fetch_torrent.assert_not_called()
    bot._reply.assert_awaited_once()
    assert "Expired" in bot._reply.call_args[0][0]
    bot._refresh_active_message.assert_not_called()


def test_keyboard_includes_pending_rows():
    bot = object.__new__(TelegramBot)
    kb = bot._build_keyboard(
        0, 1,
        [[("aither", "pick:3:0"), ("bte", "pick:3:1"),
          ("All (2)", "pick:3:all")],
         [("Keep files", "keep:4:yes"), ("Delete files", "keep:4:no")],
         [("Cancel", "abort:4")]])
    rows = kb.inline_keyboard
    assert rows[0][0].text == "🔄 Refresh"
    assert [b.text for b in rows[1]] == ["aither", "bte", "All (2)"]
    assert rows[1][0].callback_data == "pick:3:0"
    assert rows[2][0].text == "Keep files"
    assert rows[3][0].text == "Cancel"
    assert all(len(b.callback_data) <= 64 for r in rows for b in r
               if (b.callback_data or "").startswith(("pick:", "keep:", "abort:")))


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


@pytest.mark.anyio
async def test_detail_card_offers_prefer_for_grace_note():
    """Grace-held detail cards carry a copy-paste /prefer_ line."""
    from unittest.mock import AsyncMock, MagicMock

    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(
        return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._coord = MagicMock()
    bot._coord._watch_wait_note = MagicMock(
        return_value="Waiting for preferred copy · 42s left")
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._detail_cache = {}
    bot._note_outbound = MagicMock()
    bot._mark_detail_sent = MagicMock()
    bot._store.get_telegram_message_id = MagicMock(return_value=None)
    h = "e" * 40
    bot._store.get.return_value = TorrentState(
        source_infohash=h, source_name="Grace.Hold", state=State.NEW,
        total_bytes=1000, source_announce_url="https://alpha.cc/announce/xyz")
    await bot._send_one_detail(h, None)
    sent_text = bot._bot.send_message.call_args[0][1]
    assert "Prefer this copy now:" in sent_text
    assert f"/prefer_{h[:10]}" in sent_text


def test_render_active_offers_prefer_for_grace_note():
    """Grace-held rows get a positional prefer command + snapshot pick."""
    from racing_sync.telegram_bot import pick_snapshot

    h = "e" * 40
    ts = TorrentState(source_infohash=h, source_name="Grace.Hold",
                      state=State.NEW, total_bytes=1000,
                      source_announce_url="https://alpha.cc/announce/xyz")
    notes = {h: "Waiting for preferred copy ?? 42s left"}
    text, _, _ = render_active(
        [(ts, None)], page=0, page_size=5, notes=notes)
    assert "/cancel\\_1 /prefer\\_1" in text
    assert f"{h[:10]}" not in text  # no torrent hash in the message
    assert "Prefer this copy now:" not in text
    assert pick_snapshot([(ts, None)], "prefer", notes) == [(h, "alpha")]
    # Other notes (owner-deferred) and other states get no prefer.
    assert pick_snapshot(
        [(ts, None)], "prefer",
        {h: "Waiting turn ?? example.net copy first"}) == []
    ts_q = TorrentState(source_infohash=h, source_name="Grace.Hold",
                        state=State.QUEUED, total_bytes=1000)
    assert pick_snapshot([(ts_q, None)], "prefer", notes) == []


def test_short_tracker_label_generic_rule():
    from racing_sync.telegram_bot import _short_tracker_label
    assert _short_tracker_label("tracker.example.com") == "example"
    assert _short_tracker_label("nyaa.tracker.wf") == "nyaa"
    assert _short_tracker_label("example.cc") == "example"
    assert _short_tracker_label("host.example.co.uk") == "example"
    assert _short_tracker_label("") == ""


def test_size_compact():
    from racing_sync.telegram_bot import _size_compact
    assert _size_compact(int(3.1 * 1024 ** 3)) == "3.1G"
    assert _size_compact(1000) == "1000 B"


def test_compact_wait_note():
    from racing_sync.telegram_bot import _compact_wait_note
    assert _compact_wait_note("Waiting for preferred copy · 1908s left") == "Wait pref-copy 32m"
    assert _compact_wait_note("Waiting turn · tracker.example.com copy first") == "Wait example first"
    assert _compact_wait_note("Waiting turn · public copy first") == "Wait public first"
    assert _compact_wait_note("something else") is None


def test_render_active_appends_footer():
    ts = TorrentState(source_infohash="a" * 40, source_name="Show",
                      state=State.QUEUED, total_bytes=1000)
    footer = "______________________\n·  VPS1 Free: 10G\n·  SSD Free: 5G"
    text, _, _ = render_active([(ts, None)], page=0, page_size=5,
                               footer=footer)
    assert text.endswith(footer)
    # No footer by default — message ends with the command line.
    text2, _, _ = render_active([(ts, None)], page=0, page_size=5)
    assert "VPS1 Free" not in text2
    # Empty list carries the footer too.
    text3, _, _ = render_active([], footer=footer)
    assert "No active tasks" in text3
    assert text3.endswith(footer)


@pytest.mark.anyio
async def test_active_footer_without_coord_is_empty():
    bot = object.__new__(TelegramBot)
    bot._coord = None
    assert await bot._active_footer() == ""


@pytest.mark.anyio
async def test_active_footer_formats_storage(tmp_path):
    from types import SimpleNamespace

    async def _vps1_free():
        return 120 * 1024 ** 3

    coord = SimpleNamespace(
        cfg=SimpleNamespace(
            ssd=SimpleNamespace(path=tmp_path),
            cleanup=SimpleNamespace(
                low_watermark_free_bytes=200 * 1024 ** 3,
                critical_watermark_free_bytes=8 * 1024 ** 3,
            ),
        ),
        _ssd_reserved_total=lambda: 30 * 1024 ** 3,
        _source_free_bytes=_vps1_free,
    )
    bot = object.__new__(TelegramBot)
    bot._coord = coord
    bot._vps1_free_cache = None
    footer = await bot._active_footer()
    assert footer.splitlines()[0] == "\\_" * 35  # escaped: raw ___ parses as italic and goes invisible
    assert footer.splitlines()[1] == "·  VPS1 Free: 120.0G"
    assert footer.splitlines()[2].startswith("·  SSD Free: ")
    assert footer.splitlines()[2].endswith("·  rsv 30.0G")
    # No warning flags even under the low watermark — the cleanup
    # janitor owns low space.
    assert "⚠️" not in footer
    # Second call reuses the cached VPS1 probe.
    coord._source_free_bytes = None
    assert await bot._active_footer() == footer


@pytest.mark.anyio
async def test_active_footer_unknown_vps1(tmp_path):
    from types import SimpleNamespace

    async def _boom():
        raise OSError("sftp down")

    coord = SimpleNamespace(
        cfg=SimpleNamespace(
            ssd=SimpleNamespace(path=tmp_path),
            cleanup=SimpleNamespace(),
        ),
        _ssd_reserved_total=lambda: 0,
        _source_free_bytes=_boom,
    )
    bot = object.__new__(TelegramBot)
    bot._coord = coord
    bot._vps1_free_cache = None
    footer = await bot._active_footer()
    assert footer.splitlines()[1] == "·  VPS1 Free: ?"
    assert "SSD Free: " in footer


