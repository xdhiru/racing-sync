"""The 🗄 filter tab: seeded (DONE) rows whose files are still on VPS1.

A release that reached DONE keeps its bytes on VPS1 until the cleanup
janitor's adaptive grace expires (up to `max_grace_hours`), and those rows
vanish from the Active Tasks card. The tab brings them back, with the truth
source being the coordinator's cached VPS1 listing — never a persisted
"cleaned" claim. Everything here guards that category against the three
ways it could lie: leaking DONE into the other tabs, becoming cancellable
through a lane tap, or rendering a fabricated list while the listing is
unavailable / the janitor is off.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig, TelegramConfig
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_protocol import (
    _CLEANUP_FILTER_KEY,
    _STATUS_FILTER_BUTTONS,
    _STATUS_FILTERS,
    CancelScope,
    ScopeGone,
    _parse_action_data,
    cleanup_tab_available,
    match_status_filter,
)
from racing_sync.telegram_views import render_active

RACING = "a" * 40
INJECTED = "b" * 40
DEST = "c" * 40
GONE = "d" * 40
LIVE_DL = "e" * 40
LIVE_MV = "f" * 40


def _torrent(h: str) -> Torrent:
    return Torrent(
        hash=h,
        name="Seeded.Show.S01E01",
        category="",
        save_path="/vps1/data",
        size_bytes=700,
        state="seeding",
        progress=1.0,
        trackers=[],
    )


def _row(
    h: str = RACING,
    name: str = "Seeded.Show.S01E01",
    state: State = State.DONE,
    *,
    injected: str = "",
    dest: str = "",
    hours_ago: float = 1.0,
) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    return TorrentState(
        source_infohash=h,
        dest_infohash=dest or h,
        source_name=name,
        state=state,
        total_bytes=1000,
        injected_private_hashes=injected,
        source_announce_url="https://alpha.cc/announce/xyz",
        completed_at=now - dt.timedelta(hours=hours_ago),
    )


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(enabled=True, dry_run=False, janitor_startup_delay_seconds=0.0)
    base.update(over)
    return CleanupConfig(**base)


def _janitor_coord(store, present: list[str] | None, *, enabled: bool = True) -> object:
    """Coordinator double whose ONLY VPS1 knowledge is the cached listing.

    `source_client.list_torrents` is an AsyncMock with no result: any RPC
    from the display path is a test failure, not just a slow test.
    """
    coord = make_coordinator(store)
    coord.cfg = MagicMock()
    coord.cfg.cleanup = _cleanup_cfg(enabled=enabled)
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(side_effect=AssertionError("RPC from bot path"))
    if present is None:
        coord._source_torrents_cache = []
        coord._source_torrents_cached_at = 0.0
    else:
        coord._source_torrents_cache = [_torrent(h) for h in present]
        coord._source_torrents_cached_at = time.monotonic()
    return coord


# ------------------------------------------------------------------ store


def test_list_done_reader_keeps_inflight_semantics(tmp_path):
    """`list_active_inflight` still excludes DONE; the new reader lists it."""
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING))
        store.upsert(_row(LIVE_DL, "Live.Show.S01E01", State.DOWNLOADING))
        assert [t.source_infohash for t in store.list_done_display()] == [RACING]
        inflight = store.list_active_inflight()
        assert [t.source_infohash for t in inflight] == [LIVE_DL]
        assert all(t.state != State.DONE for t in inflight)
    finally:
        store.close()


def test_list_done_reader_invalidates_on_write(tmp_path):
    """A row leaving DONE must not keep showing (shared invalidation)."""
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING))
        assert len(store.list_done_display()) == 1
        row = store.get(RACING)
        row.state = State.RE_ADDING
        store.upsert(row)
        assert store.list_done_display() == []
        assert [t.source_infohash for t in store.list_active_inflight()] == [RACING]
    finally:
        store.close()


# ------------------------------------------------------------ coordinator


def test_tab_shows_only_done_and_present(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING))
        store.upsert(_row(GONE, "Reclaimed.Show.S01E02"))
        coord = _janitor_coord(store, [RACING])
        rows, unavailable = coord.cleanup_pending_rows()
        assert unavailable is False
        assert [r.source_infohash for r in rows] == [RACING]
    finally:
        store.close()


def test_injected_private_hash_alone_counts_as_present(tmp_path):
    """The racing copy can be gone while an injected member still holds bytes."""
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING, injected=INJECTED))
        coord = _janitor_coord(store, [INJECTED])
        rows, _unavailable = coord.cleanup_pending_rows()
        assert [r.source_infohash for r in rows] == [RACING]
    finally:
        store.close()


def test_non_done_rows_never_enter_the_tab(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING, state=State.DOWNLOADING))
        store.upsert(_row(RACING, "Seeded.Show.S01E01", State.DONE, hours_ago=2))
        coord = _janitor_coord(store, [RACING])
        rows, _ = coord.cleanup_pending_rows()
        assert all(r.state == State.DONE for r in rows)
    finally:
        store.close()


def test_oldest_completion_first(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row("1" * 40, "Older.Show.S01E01", hours_ago=9))
        store.upsert(_row("2" * 40, "Newer.Show.S01E01", hours_ago=1))
        coord = _janitor_coord(store, ["1" * 40, "2" * 40])
        rows, _ = coord.cleanup_pending_rows()
        assert [r.source_infohash for r in rows] == ["1" * 40, "2" * 40]
    finally:
        store.close()


def test_vps1_present_hashes_is_a_cache_read(tmp_path):
    """Cheap and RPC-free: the bot must never trigger a VPS1 scan itself."""
    coord = _janitor_coord(MagicMock(), [RACING, INJECTED])
    assert coord.vps1_present_hashes() == {RACING, INJECTED}
    assert coord.source_client.list_torrents.await_count == 0


def test_vps1_present_hashes_none_without_listing(tmp_path):
    coord = _janitor_coord(MagicMock(), None)
    assert coord.vps1_present_hashes() is None
    # A listing older than the freshness window is "unknown", not "present".
    coord._source_torrents_cached_at = time.monotonic() - 10_000
    assert coord.vps1_present_hashes() is None


def test_vps1_present_hashes_ignores_non_hex_entries():
    coord = _janitor_coord(MagicMock(), [])
    coord._source_torrents_cache = [MagicMock(), _torrent(RACING)]
    coord._source_torrents_cached_at = time.monotonic()
    assert coord.vps1_present_hashes() == {RACING}


def test_listing_unavailable_is_reported_not_fabricated(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING))
        coord = _janitor_coord(store, None)
        rows, unavailable = coord.cleanup_pending_rows()
        assert rows == []
        assert unavailable is True
    finally:
        store.close()


def test_janitor_disabled_hides_the_whole_category(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row(RACING))
        coord = _janitor_coord(store, [RACING], enabled=False)
        assert coord.cleanup_tab_enabled() is False
        # The Telegram gate reads the same answer, strictly.
        assert cleanup_tab_available(coord) is False
        # Helper returns nothing at all — not "empty because reclaimed".
        assert coord.cleanup_pending_rows() == ([], False)
    finally:
        store.close()


def test_cleanup_tab_available_fails_closed_on_bare_doubles():
    assert cleanup_tab_available(MagicMock()) is False
    assert cleanup_tab_available(None) is False
    on = MagicMock()
    on.cleanup_tab_enabled.return_value = True
    assert cleanup_tab_available(on) is True


# --------------------------------------------------------------- protocol


def test_vc_filter_key_and_button_registered():
    assert _STATUS_FILTERS[_CLEANUP_FILTER_KEY] == frozenset({"done"})
    assert ("vc", "🗄") in _STATUS_FILTER_BUTTONS
    keys = [k for (k, _) in _STATUS_FILTER_BUTTONS]
    assert keys == ["all", "dl", "mv", "rd", "qd", "vc", "ov"]


def test_done_never_passes_another_filter():
    """The regression the 🗄 tab makes reachable: DONE must stay out of All/lane."""
    for key in ("all", "dl", "mv", "rd", "qd"):
        assert match_status_filter("done", key) is False, key
    assert match_status_filter("done", "vc") is True
    # Every other state keeps its old answer.
    assert match_status_filter("downloading", "all") is True
    assert match_status_filter("failed", "all") is True
    assert match_status_filter("moving", "mv") is True
    assert match_status_filter("queued", "qd") is True
    assert match_status_filter("re_adding", "rd") is True
    assert match_status_filter("downloading", "vc") is False
    assert match_status_filter("downloading", "bogus") is True


def test_status_vc_action_data_is_refused():
    """A handcrafted `cancel:status:vc` never parses into a cancel scope."""
    assert _parse_action_data("cancel:status:vc") == ("", False, "", "", None)
    assert _parse_action_data("forget:status:vc:1") == ("", False, "", "", None)
    assert _parse_action_data("keep:status:vc:1:yes") == ("", False, "", "", None)


@pytest.mark.anyio
async def test_cancel_scope_vc_raises_scope_gone():
    bot = object.__new__(TelegramBot)
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = [_row(RACING)]
    bot._live_stalled = MagicMock(return_value=[])
    with pytest.raises(ScopeGone):
        await CancelScope("status", "vc").resolve(bot)


# ---------------------------------------------------------------- keyboard


def _keyboard_bot(coord) -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123", page_size=5)
    bot._coord = coord
    return bot


def _filter_labels(bot) -> list[str]:
    kb = bot._build_keyboard(current_page=0, total_pages=1, status_filter="dl")
    return [b.text.strip("·") for b in kb.inline_keyboard[-1]]


def test_keyboard_shows_vc_only_when_janitor_enabled():
    on = _keyboard_bot(MagicMock())
    on._coord.cleanup_tab_enabled.return_value = True
    assert _filter_labels(on) == ["All", "DL", "MV", "RD", "QD", "🗄", "📊"]

    off = _keyboard_bot(MagicMock())
    off._coord.cleanup_tab_enabled.return_value = False
    assert _filter_labels(off) == ["All", "DL", "MV", "RD", "QD", "📊"]
    assert all(
        b.text != "·🗄·"
        for b in off._build_keyboard(
            current_page=0, total_pages=1, status_filter="vc"
        ).inline_keyboard[-1]
    )


def test_keyboard_vc_marks_active_filter():
    bot = _keyboard_bot(MagicMock())
    bot._coord.cleanup_tab_enabled.return_value = True
    kb = bot._build_keyboard(current_page=0, total_pages=1, status_filter="vc")
    assert [b.text for b in kb.inline_keyboard[-1]] == ["All", "DL", "MV", "RD", "QD", "·🗄·", "📊"]
    assert kb.inline_keyboard[-1][5].callback_data == "status:vc"


# ------------------------------------------------------------------ router


def _query(data):
    q = MagicMock()
    q.message.chat.id = 12345
    q.from_user.id = 12345
    q.data = data
    q.answer = AsyncMock()
    return q


def _page_bot(rows, coord=None, **kw) -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345", page_size=5)
    bot._callback_times = {}
    bot._current_page = 0
    bot._last_active_cache = None
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = list(rows)
    bot._coord = coord if coord is not None else MagicMock()
    bot._refresh_active_message = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


@pytest.mark.anyio
async def test_status_vc_tap_accepted_when_enabled():
    bot = _page_bot([_row(LIVE_DL, state=State.DOWNLOADING)])
    bot._coord.cleanup_tab_enabled.return_value = True
    await bot._handle_callback(_query("status:vc"))
    assert bot._status_filter == "vc"
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_status_vc_tap_ignored_when_disabled():
    bot = _page_bot([_row(LIVE_DL, state=State.DOWNLOADING)], _status_filter="all")
    bot._coord.cleanup_tab_enabled.return_value = False
    await bot._handle_callback(_query("status:vc"))
    assert bot._status_filter == "all"
    bot._refresh_active_message.assert_not_called()


@pytest.mark.anyio
async def test_page_next_counts_tab_groups():
    """Paging wraps over what the tab shows, not the hidden inflight list."""
    two_groups = [_row("1" * 40, "Older.Show.S01E01"), _row("2" * 40, "Newer.Show.S01E01")]
    bot = _page_bot([_row(LIVE_DL, state=State.DOWNLOADING)] * 9)
    bot._cfg.page_size = 1
    bot._status_filter = "vc"
    bot._coord.cleanup_tab_enabled.return_value = True
    bot._coord.cleanup_pending_rows.return_value = (two_groups, False)
    await bot._handle_callback(_query("page:next"))
    assert bot._current_page == 1
    # Nine in-flight rows would give 9 pages; the tab has two, so the
    # next tap wraps (debounce cleared so the re-tap is not absorbed).
    bot._callback_times = {}
    await bot._handle_callback(_query("page:next"))
    assert bot._current_page == 0


# ----------------------------------------------------------------- refresh


def _refresh_bot(coord, rows) -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._bot = MagicMock()
    bot._bot.edit_message_text = AsyncMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=99))
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = list(rows)
    bot._coord = coord
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="123")
    bot._current_page = 0
    bot._active_msg_id = None
    bot._prev_active_msg_id = None
    bot._last_active_cache = None
    bot._detail_cache = {}
    bot._detail_queue = asyncio.Queue()
    bot._detail_sent_state = {}
    return bot


def _vc_coord() -> MagicMock:
    coord = MagicMock()
    coord.live_progress_map.return_value = {}
    coord.live_speed_map.return_value = {}
    coord.rclone_move_progress_map.return_value = {}
    coord.cleanup_tab_enabled.return_value = True
    coord._cleanup_hint.return_value = "clean ~5h"
    return coord


@pytest.mark.anyio
async def test_tab_renders_rows_with_badge_and_eta():
    """The dead-hint regression: the janitor's ETA now reaches the card."""
    coord = _vc_coord()
    coord.cleanup_pending_rows.return_value = ([_row(RACING, "Seeded.Show.S01E01")], False)
    bot = _refresh_bot(coord, [_row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING)])
    bot._status_filter = "vc"
    await bot._refresh_active_message_inner()
    text = bot._bot.send_message.call_args[0][1]
    assert "Seeded.Show.S01E01" in text
    assert "Live.Show.S02E01" not in text
    assert "🗄 still on VPS1" in text
    # The ETA is the untouched `_cleanup_hint` string, appended per row.
    assert "clean ~5h" in text
    assert bot._last_rendered_groups == 1


@pytest.mark.anyio
async def test_tab_pagination_counts_groups():
    coord = _vc_coord()
    coord.cleanup_pending_rows.return_value = (
        [_row("1" * 40, "Older.Show.S01E01"), _row("2" * 40, "Newer.Show.S01E01")],
        False,
    )
    bot = _refresh_bot(coord, [])
    bot._cfg.page_size = 1
    bot._status_filter = "vc"
    await bot._refresh_active_message_inner()
    text = bot._bot.send_message.call_args[0][1]
    assert "Page 1/2" in text
    assert "Older.Show.S01E01" in text
    assert "Newer.Show.S01E01" not in text


@pytest.mark.anyio
async def test_unavailable_listing_shows_note_not_fabricated_rows():
    coord = _vc_coord()
    coord.cleanup_pending_rows.return_value = ([], True)
    bot = _refresh_bot(coord, [_row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING)])
    bot._status_filter = "vc"
    await bot._refresh_active_message_inner()
    text = bot._bot.send_message.call_args[0][1]
    assert "VPS1 listing unavailable" in text
    assert "Seeded" not in text
    assert "Live.Show" not in text


@pytest.mark.anyio
async def test_janitor_off_falls_back_to_all():
    coord = _vc_coord()
    coord.cleanup_tab_enabled.return_value = False
    coord.cleanup_pending_rows.return_value = ([], False)
    bot = _refresh_bot(coord, [_row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING)])
    bot._status_filter = "vc"
    await bot._refresh_active_message_inner()
    assert bot._status_filter == "all"
    text = bot._bot.send_message.call_args[0][1]
    assert "Live.Show.S02E01" in text
    assert "🗄" not in text
    coord.cleanup_pending_rows.assert_not_called()


@pytest.mark.anyio
async def test_all_tab_never_shows_a_done_row():
    """Even if the row source ever handed one in, All drops it."""
    coord = _vc_coord()
    coord.cleanup_tab_enabled.return_value = False
    bot = _refresh_bot(
        coord,
        [
            _row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING),
            _row(RACING, "Seeded.Show.S01E01"),
        ],
    )
    bot._status_filter = "all"
    await bot._refresh_active_message_inner()
    text = bot._bot.send_message.call_args[0][1]
    assert "Live.Show.S02E01" in text
    assert "Seeded.Show.S01E01" not in text


# ------------------------------------------------------------- numbering


def _vc_bot(coord, rows, **kw) -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="1", page_size=5)
    bot._callback_times = {}
    bot._current_page = 0
    bot._last_active_cache = None
    bot._store = MagicMock()
    bot._store.list_active_inflight.return_value = list(rows)
    bot._coord = coord
    bot._refresh_active_message = AsyncMock()
    bot._reply = AsyncMock()
    bot._send_sheet = AsyncMock()
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _message(text="/act_1"):
    m = MagicMock()
    m.chat.id = "1"
    m.from_user.id = "9"
    m.text = text
    m.caption = None
    m.message_id = 42
    return m


@pytest.mark.anyio
async def test_act_positional_resolves_tab_rows_in_display_order():
    """`/act_1` inside the tab opens the row shown as #1 (oldest first)."""
    coord = _vc_coord()
    oldest = _row("1" * 40, "Older.Show.S01E01", hours_ago=9)
    newest = _row("2" * 40, "Newer.Show.S01E01", hours_ago=1)
    coord.cleanup_pending_rows.return_value = ([oldest, newest], False)
    bot = _vc_bot(coord, [_row(LIVE_DL, state=State.DOWNLOADING)], _status_filter="vc")
    bot._live_notes_for = MagicMock(return_value={})
    bot._sheet_text_and_rows = MagicMock(return_value=("sheet", []))
    await bot._act_command_entry("1", _message())
    members = bot._sheet_text_and_rows.call_args.args[0]
    assert [r.source_infohash for r in members] == ["1" * 40]


@pytest.mark.anyio
async def test_act_positional_resolves_nothing_when_listing_unavailable():
    coord = _vc_coord()
    coord.cleanup_pending_rows.return_value = ([], True)
    bot = _vc_bot(coord, [_row(LIVE_DL, state=State.DOWNLOADING)], _status_filter="vc")
    bot._live_notes_for = MagicMock(return_value={})
    bot._sheet_text_and_rows = MagicMock(return_value=("sheet", []))
    await bot._act_command_entry("1", _message())
    bot._sheet_text_and_rows.assert_not_called()


def test_positional_rows_exclude_done_under_every_other_filter():
    coord = _vc_coord()
    coord.cleanup_tab_enabled.return_value = False
    rows = [
        _row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING),
        _row(RACING, "Seeded.Show.S01E01"),
    ]
    for flt in ("all", "dl", "mv", "rd", "qd"):
        bot = _vc_bot(coord, rows, _status_filter=flt)
        kept = bot._positional_active_rows(rows)
        assert RACING not in [r.source_infohash for r in kept], flt


# ----------------------------------------------------------- cancel_lane


@pytest.mark.anyio
async def test_cancel_lane_sheet_omits_the_tab():
    """🗄 is not a lane: no button, no count, even with DONE rows in play."""
    bot = _vc_bot(MagicMock(), [])
    bot._store.list_active_inflight.return_value = [
        _row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING),
        _row(LIVE_MV, "Moving.Show.S03E01", State.MOVING),
        _row(RACING, "Seeded.Show.S01E01"),
    ]
    bot._start_single_command = AsyncMock()
    await bot._cancel_status_command_entry(_message("/cancel_lane"))
    rows = bot._send_sheet.await_args.args[1]
    data = [d for row in rows for (_label, d) in row]
    labels = [label for row in rows for (label, _d) in row]
    assert "cancel:status:vc" not in data
    assert not any("🗄" in label for label in labels)
    assert "cancel:status:dl" in data
    assert "cancel:status:mv" in data
    bot._start_single_command.assert_not_awaited()


# ---------------------------------------------------------------- render


def test_render_badge_only_for_done_rows():
    done = _row(RACING, "Seeded.Show.S01E01")
    live = _row(LIVE_DL, "Live.Show.S02E01", State.DOWNLOADING)
    text, _, _ = render_active([(done, None), (live, None)], cleanup={RACING: "clean ready"})
    assert "✅ Done · clean ready · 🗄 still on VPS1" in text
    assert "🗄" not in text.split("Live.Show.S02E01")[1].split("1. ")[0]
    base, _, _ = render_active([(done, None)])
    assert base.endswith("🗄 still on VPS1") or "🗄 still on VPS1" in base


def test_render_unavailable_note_replaces_empty_message():
    text, _, _ = render_active([], unavailable_note="VPS1 listing unavailable")
    assert "VPS1 listing unavailable" in text
    assert "No active tasks" not in text
    plain, _, _ = render_active([])
    assert "No active tasks" in plain
