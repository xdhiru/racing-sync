"""Telegram resilience: uncertain sends don't duplicate; terminal cards heal."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram.error import TelegramError, TimedOut

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_refresh import _ACTIVE_PROBE_EVERY


def _bot(**kw):
    bot = object.__new__(TelegramBot)
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5, status_update_interval=15)
    bot._callback_times = {}
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._pending_pick = None
    bot._coord = MagicMock()
    bot._coord.live_progress_map = MagicMock(return_value={})
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=7))
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.delete_message = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _drain(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


@pytest.mark.anyio
async def test_active_send_timeout_skips_blind_resend(tmp_path):
    """Lost-response sends must not mint a duplicate every interval."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        # First send times out with no known id: throttled retries.
        await bot._refresh_active_message_inner()
        for _ in range(3):
            await bot._refresh_active_message_inner()
            assert bot._bot.send_message.await_count == 1
        # A previously known message id (e.g. repost deleted the old one
        # but the resend fate is unknown): static text skips entirely.
        bot._prev_active_msg_id = 42
        for _ in range(5):
            await bot._refresh_active_message_inner()
            assert bot._bot.send_message.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_active_send_timeout_records_uncertain_then_skips(tmp_path):
    """After one timeout, unchanged text skips; changed text sends."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._prev_active_msg_id = 5
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        # Unchanged text: throttled retries (3 skips, 4th sends) instead of
        # a duplicate every interval.
        await bot._refresh_active_message_inner()
        await bot._refresh_active_message_inner()
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 2
        # Changed text (new row appears): send proceeds.
        store.upsert(
            TorrentState(source_infohash="n" * 40, source_name="New Show", state=State.NEW)
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=9))
        await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 1
        assert bot._active_msg_id == 9
    finally:
        store.close()


@pytest.mark.anyio
async def test_done_card_timeout_records_terminal_retry(tmp_path):
    """A timed-out DONE edit is requeued AND remembered past the active list."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(TorrentState(source_infohash=h, source_name="Done Show", state=State.DONE))
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._bot.edit_message_text = AsyncMock(side_effect=TimedOut("slow route"))
        bot._mark_detail_sent(h, "moving")  # card shows pre-DONE state

        await bot._send_one_detail(h, None)

        assert bot._detail_terminal_map().get(h) == 0
        assert [i[0] for i in _drain(bot._detail_queue)] == [h]
    finally:
        store.close()


@pytest.mark.anyio
async def test_terminal_net_requeues_until_card_lands(tmp_path):
    """Refresh loop retries recorded terminal cards; success clears them."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(TorrentState(source_infohash=h, source_name="Done Show", state=State.DONE))
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._active_msg_id = 50
        bot._active_footer = AsyncMock(return_value="")
        bot._mark_detail_sent(h, "moving")
        bot._detail_terminal_map()[h] = 0

        await bot._refresh_active_message_inner()

        assert h in [i[0] for i in _drain(bot._detail_queue)]
        assert bot._detail_terminal_map().get(h) == 1
        # Card lands: record cleared, no further retries.
        bot._mark_detail_sent(h, "done")
        await bot._refresh_active_message_inner()
        assert h not in bot._detail_terminal_map()
        assert bot._detail_queue.empty()
    finally:
        store.close()


@pytest.mark.anyio
async def test_transient_streak_caps_immediate_requeue(tmp_path):
    """A dead route must not hot-loop one card through the worker."""
    store = StateStore(tmp_path / "state.db")
    try:
        h = "d" * 40
        store.upsert(
            TorrentState(source_infohash=h, source_name="Flaky Show", state=State.DOWNLOADING)
        )
        store.set_telegram_message_id(h, 99)
        bot = _bot()
        bot._store = store
        bot._bot.edit_message_text = AsyncMock(side_effect=TimedOut("dead route"))

        for _ in range(8):
            await bot._send_one_detail(h, None)

        # 5 immediate requeues max; the rest rely on the refresh nets.
        assert bot._detail_queue.qsize() <= 5
    finally:
        store.close()


def _repost_bot(**kw):
    """Bot double with the repost knobs the inner refresh needs."""
    bot = _bot()
    bot._cfg.active_repost_interval_seconds = 15
    bot._last_repost_monotonic = time.monotonic() - 3600.0
    bot._newest_outbound_id = None
    bot._orphan_active_ids = []
    bot._send_uncertain_key = None
    bot._send_uncertain_skips = 0
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


@pytest.mark.anyio
async def test_repost_delete_flood_keeps_old_without_send(tmp_path):
    """The reported orphan: delete flood + successful send stranded history.

    Delete-first-with-abort must keep the old card and skip the send so
    the pointer never moves past an undeleted message.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Flood control exceeded. Retry in 12 seconds")
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id == 111
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_delete_timeout_keeps_old_without_send(tmp_path):
    """Timed-out deletes abort the repost the same way floods do."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(side_effect=TimedOut("slow"))
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id == 111
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_delete_gone_still_resends(tmp_path):
    """An already-deleted old card is safe to replace immediately."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Bad Request: message to delete not found")
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=222))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 222
        assert bot._prev_active_msg_id == 222
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_send_failure_clears_for_fresh_resend(tmp_path):
    """Delete ok + send timeout keeps the old uncertain-send behavior."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._bot.delete_message = AsyncMock()
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        await bot._repost_active_message("text", None, ("p", 1, "text"))

        bot._bot.delete_message.assert_awaited_once()
        assert bot._active_msg_id is None
        assert bot._send_uncertain_key == ("p", 1, "text")
    finally:
        store.close()


@pytest.mark.anyio
async def test_repost_send_failure_does_not_strand_the_card(tmp_path):
    """A failed resend must not strand the chat without any card.

    The delete succeeds and the send times out: the deleted card's id
    must not survive as a "live" id, or the uncertain-send guard skips
    every resend and the card never comes back.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=111, _prev_active_msg_id=111)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")

        # Render the real cache key (the live card is edited in place).
        await bot._refresh_active_message_inner()
        cache_key = bot._last_active_cache
        assert cache_key is not None

        bot._bot.delete_message = AsyncMock()
        bot._bot.send_message = AsyncMock(side_effect=TimedOut("slow"))

        await bot._repost_active_message("text", None, cache_key)

        assert bot._active_msg_id is None
        assert bot._prev_active_msg_id is None

        # The next refreshes must bring the card back (the uncertain-send
        # guard retries a throttled send while no message is known).
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=222))
        bot._active_footer = AsyncMock(return_value="")
        bot._store = store
        for _ in range(5):
            await bot._refresh_active_message_inner()

        assert bot._bot.send_message.await_count >= 1
        assert bot._active_msg_id == 222
    finally:
        store.close()


@pytest.mark.anyio
async def test_card_deleted_from_chat_is_detected_by_the_probe(tmp_path):
    """A card deleted from the chat is noticed by the liveness probe.

    Unchanged content never edits the card, so a deleted one would stay
    gone forever. Every N-th unchanged refresh edits anyway: the
    message-not-found answer proves the card died and triggers a resend.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot()
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")

        await bot._refresh_active_message_inner()
        assert bot._active_msg_id == 7

        # The card is deleted from the chat: edits now fail with
        # "message to edit not found".
        bot._bot.edit_message_text = AsyncMock(
            side_effect=TelegramError("Bad Request: message to edit not found")
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=8))

        # Unchanged card: no resend before the probe is due.
        for _ in range(3):
            await bot._refresh_active_message_inner()
        assert bot._bot.send_message.await_count == 0

        # The probe fires and the dead card is replaced.
        for _ in range(_ACTIVE_PROBE_EVERY + 3):
            await bot._refresh_active_message_inner()

        assert bot._active_msg_id == 8
    finally:
        store.close()


@pytest.mark.anyio
async def test_fresh_send_prev_delete_flood_aborts_send(tmp_path):
    """A flood-delayed previous delete must abort the send, not orphan it."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=None, _prev_active_msg_id=42)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Flood control exceeded. Retry in 16 seconds")
        )

        await bot._refresh_active_message_inner()

        bot._bot.send_message.assert_not_awaited()
        assert bot._active_msg_id is None
        assert bot._prev_active_msg_id == 42
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_fresh_send_prev_gone_still_sends(tmp_path):
    """An already-deleted previous id does not block the fresh send."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=None, _prev_active_msg_id=42)
        bot._store = store
        bot._active_footer = AsyncMock(return_value="")
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("Bad Request: message to delete not found")
        )

        await bot._refresh_active_message_inner()

        bot._bot.send_message.assert_awaited_once()
        assert bot._active_msg_id == 77
        assert bot._prev_active_msg_id == 77
    finally:
        store.close()


@pytest.mark.anyio
async def test_orphan_sweep_deletes_then_drops(tmp_path):
    """The sweep retires ids that delete cleanly."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=77)
        bot._store = store
        bot._orphan_active_ids = [42]
        bot._bot.delete_message = AsyncMock()

        await bot._sweep_orphan_active_messages()

        bot._bot.delete_message.assert_awaited_once_with(chat_id="1", message_id=42)
        assert bot._orphan_ids() == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_orphan_sweep_keeps_flooded_and_drops_gone(tmp_path):
    """Flooded orphans stay queued; already-gone ids are forgotten."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _repost_bot(_active_msg_id=77)
        bot._store = store
        bot._orphan_active_ids = [42, 43]
        bot._bot.delete_message = AsyncMock(
            side_effect=[
                TelegramError("Flood control exceeded. Retry in 12 seconds"),
                TelegramError("Bad Request: message to delete not found"),
            ]
        )

        await bot._sweep_orphan_active_messages()

        assert bot._bot.delete_message.await_count == 2
        assert bot._orphan_ids() == [42]
    finally:
        store.close()


def test_drift_skip_map_drops_settled_hashes():
    """A hash that left the active list must not keep a drift counter."""
    bot = _bot()
    bot._detail_drift_skip = {"a" * 40: 3, "b" * 40: 1}

    bot._prune_detail_drift_skip({"a" * 40})

    assert bot._detail_drift_skip == {"a" * 40: 3}


@pytest.mark.anyio
async def test_add_reports_a_tombstoned_write(tmp_path):
    """A refused (tombstoned) insert must not answer 'Queued'."""
    from types import SimpleNamespace

    from test_watchdir import _create_sample_torrent_data

    from racing_sync.state import StateStore

    blob = _create_sample_torrent_data("Show.S01E01", 100, "http://x/announce")
    from racing_sync.watchdir import _bencoded_info_hash

    infohash, _, _, _ = _bencoded_info_hash(blob)

    store = StateStore(tmp_path / "state.db")
    try:
        # Tombstoning stamps an existing row (a fresh insert is allowed).
        store.upsert(TorrentState(source_infohash=infohash, source_name="Show.S01E01"))
        store.tombstone(infohash)

        bot = _bot()
        bot._store = store
        bot._coord = None
        bot._reply = AsyncMock()
        bot._delete_chat_file = AsyncMock()

        class _TgFile:
            def download_to_memory(self, buf):
                buf.write(blob)
                return buf

        bot._bot = MagicMock()
        bot._bot.get_file = AsyncMock(return_value=_TgFile())

        message = SimpleNamespace(
            document=SimpleNamespace(file_name="show.torrent", file_size=len(blob), file_id="f1")
        )

        await bot._process_document_command(message)

        replies = [c.args[0] for c in bot._reply.await_args_list]
        assert any("tombstoned" in r for r in replies), replies
        assert not any(r.startswith("Queued ") for r in replies), replies
    finally:
        store.close()


@pytest.mark.anyio
async def test_send_one_detail_skips_the_blob():
    bot = _bot()
    bot._store = MagicMock()
    bot._store.get = MagicMock(return_value=None)

    await bot._send_one_detail("a" * 40, None)

    assert bot._store.get.call_args.kwargs.get("include_blob") is False


@pytest.mark.anyio
async def test_cancelled_card_reports_the_chosen_policy():
    """The card must not claim "removed + ignored" for just-forget."""
    bot = _bot()
    seen: list[str] = []

    async def _edit(text, **kwargs):
        seen.append(text)

    bot._bot.edit_message_text = _edit
    h = "a" * 40

    await bot._mark_detail_cancelled(7, "Show", h, remember=True, delete_files=True)
    assert "removed + ignored" in seen[-1]
    assert f"Unignore: `/unignore {h}`" in seen[-1]

    await bot._mark_detail_cancelled(7, "Show", h, remember=False, delete_files=True)
    assert "can be re-added" in seen[-1]
    assert "Unignore" not in seen[-1]

    await bot._mark_detail_cancelled(7, "Show", h, remember=True, delete_files=False)
    assert "data left in place" in seen[-1]
    assert f"Unignore: `/unignore {h}`" in seen[-1]


@pytest.mark.anyio
async def test_detail_edit_that_cannot_be_edited_resends(tmp_path):
    """A card that can never be edited must be replaced, not left frozen."""
    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "state.db")
    try:
        h = "a" * 40
        store.upsert(TorrentState(source_infohash=h, source_name="Show", state=State.DOWNLOADING))
        bot = _bot()
        bot._store = store
        bot._detail_cache = {h: 55}
        bot._detail_sent_state = {}
        bot._bot.edit_message_text = AsyncMock(
            side_effect=TelegramError("Bad Request: message can't be edited")
        )
        bot._bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=77))

        await bot._send_one_detail(h, 0.5)

        bot._bot.send_message.assert_awaited_once()
        assert bot._detail_cache[h] == 77
    finally:
        store.close()
