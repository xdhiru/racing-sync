"""Manual VPS1 cleanup commands (`/cleanup`, `/clean_`) and cleanup ETA hints.

Covers the two operator entry points (global pass + per-task), the act-sheet
button, and the display-only "clean ready / clean ~5h" hint shown in the
active list and act sheet. Automatic cleanup semantics are covered by
test_cleanup*.py; here the safety checks are exercised through the manual
path (which intentionally bypasses only grace/idle).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import CleanupConfig, TelegramConfig
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_protocol import (
    CLEAN_RE,
    CLEANUP_RE,
    _bot_command_menu,
    _command_usage_for,
    _parse_action_data,
)
from racing_sync.telegram_views import render_active

H = "a" * 40
FNAME = "Show.S01E01.mkv"
FSIZE = 700
GiB = 1024**3


def _cleanup_cfg(**over) -> CleanupConfig:
    base = dict(enabled=True, dry_run=False, janitor_startup_delay_seconds=0.0)
    base.update(over)
    return CleanupConfig(**base)


def _cfg(**over) -> MagicMock:
    """Config double shaped like the cleanup tests' fixture."""
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.cleanup = _cleanup_cfg(**over)
    return cfg


def _coord(store, src, cfg) -> object:
    coord = make_coordinator(store)
    coord.cfg = cfg
    coord.source_client = src
    coord.dest_client = MagicMock()
    coord.sftp = None
    coord._stop = False
    return coord


def _member(h: str = H, *, upspeed: int = 0, leechers: int = 0, ratio: float = 2.0) -> Torrent:
    return Torrent(
        hash=h,
        name=FNAME,
        category="",
        save_path="/vps1/data",
        size_bytes=FSIZE,
        state="seeding",
        progress=1.0,
        ratio=ratio,
        trackers=[],
        upspeed_bps=upspeed,
        num_leechers=leechers,
        added_on=0,
    )


class _Source:
    def __init__(self, members: list[Torrent]):
        self.members = members
        self.delete = AsyncMock()

    async def list_torrents(self, *, category=None, hashes=None):
        return list(self.members)


def _row(store: StateStore, h: str = H, state: State = State.DONE, **over) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    fields = dict(
        source_infohash=h,
        source_name=FNAME,
        dest_infohash=h,
        total_bytes=FSIZE,
        injected_private_hashes=h,
        state=state,
        completed_at=now - dt.timedelta(hours=100),
        vps1_last_activity_at=now - dt.timedelta(hours=10),
    )
    fields.update(over)
    ts = TorrentState(**fields)
    store.upsert(ts)
    return ts


def _task_setup(tmp_path, state: State = State.DONE, members=None, **cleanup_over):
    store = StateStore(tmp_path / "s.db")
    cfg = _cfg(**cleanup_over)
    cfg.dest.save_path = tmp_path
    src = _Source([_member()] if members is None else members)
    coord = _coord(store, src, cfg)
    row = _row(store, state=state)
    # The byte-level fuse/blob checks need a live mount; their own coverage
    # lives in test_cleanup*.py. Here they are stubbed to isolate the
    # manual path's ordering and refusal branches.
    coord._cleanup_fuse_healthy = AsyncMock(return_value=True)
    coord._cleanup_preserve_racing_blobs = AsyncMock(return_value=True)
    return coord, store, src, row


# --------------------------------------------------------------- protocol


def test_cleanup_commands_parse_and_are_advertised():
    assert CLEANUP_RE.match("/cleanup")
    assert CLEANUP_RE.match("/cleanup@my_bot")
    assert CLEANUP_RE.match("/cleanup ") is not None
    assert CLEANUP_RE.match("/cleanupx") is None
    assert CLEAN_RE.match("/clean 0123abcd")
    assert CLEAN_RE.match("/clean_0123abcd")
    assert CLEAN_RE.match("/clean_0123abcd@my_bot")
    # The two verbs cannot shadow each other either way.
    assert CLEANUP_RE.match("/clean_0123abcd") is None
    assert CLEAN_RE.match("/cleanup") is None

    assert "/cleanup" in _command_usage_for("/cleanup")
    assert "/clean" in _command_usage_for("/clean")
    commands = {c.command: c.description for c in _bot_command_menu()}
    assert "cleanup" in commands and "VPS1" in commands["cleanup"]
    assert "clean" in commands and "VPS1" in commands["clean"]

    # `clean:<hash>` is a valid callback shape (act-sheet button).
    assert _parse_action_data(f"clean:{H}") == ("clean", False, H, "", None)
    assert _parse_action_data("clean:not-a-hash") == ("", False, "", "", None)


# ------------------------------------------------------------ /cleanup


@pytest.mark.anyio
async def test_cleanup_now_disabled_replies_without_running(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        coord = _coord(store, _Source([]), _cfg(enabled=False))
        coord._source_free_bytes = AsyncMock(return_value=None)
        text = await coord._cleanup_now_command()
        assert "disabled" in text
        coord._source_free_bytes.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_now_requires_a_seed_entry(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        cfg = _cfg()
        cfg.pipeline.seed = "none"
        coord = _coord(store, _Source([]), cfg)
        text = await coord._cleanup_now_command()
        assert "no seeding entry" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_now_force_bypasses_startup_and_cadence(tmp_path):
    """A manual pass runs even inside the settle window and the interval."""
    store = StateStore(tmp_path / "s.db")
    try:
        cfg = _cfg(janitor_startup_delay_seconds=3600.0)
        coord = _coord(store, _Source([]), cfg)
        coord._source_free_bytes = AsyncMock(return_value=None)
        coord._cleanup_gate_t0 = time.monotonic()
        coord._cleanup_last_run_wall_ts = time.time()
        calls: list[dict] = []

        async def _fake_run(_cfg, **kw):
            calls.append(kw)
            return {
                "deleted": 0,
                "candidates": 0,
                "dry_run": False,
                "grace_s": 7200.0,
                "free": None,
                "bursting": False,
                "reason": "no rows",
            }

        coord._run_source_cleanup = _fake_run
        assert await coord._maybe_cleanup_source() is None
        assert calls == []

        text = await coord._cleanup_now_command()
        assert "no rows" in text
        assert len(calls) == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_now_dry_run_summary(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        coord = _coord(store, _Source([]), _cfg(dry_run=True))
        coord._source_free_bytes = AsyncMock(return_value=12 * GiB)
        coord._run_source_cleanup = AsyncMock(
            return_value={
                "deleted": 2,
                "candidates": 3,
                "dry_run": True,
                "grace_s": 7200.0,
                "free": 12 * GiB,
                "bursting": False,
                "reason": "",
            }
        )
        text = await coord._cleanup_now_command()
        assert "dry-run" in text
        assert "would delete 2 of 3" in text
        assert "VPS1 free" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_cleanup_now_live_summary_reports_counts(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        coord = _coord(store, _Source([]), _cfg())
        coord._source_free_bytes = AsyncMock(return_value=12 * GiB)
        coord._run_source_cleanup = AsyncMock(
            return_value={
                "deleted": 3,
                "candidates": 4,
                "dry_run": False,
                "grace_s": 7200.0,
                "free": 12 * GiB,
                "bursting": False,
                "reason": "",
            }
        )
        text = await coord._cleanup_now_command()
        assert "deleted 3 of 4" in text
        assert "live" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_run_source_cleanup_summary_on_empty_table(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        cfg = _cfg()
        coord = _coord(store, _Source([]), cfg)
        summary = await coord._run_source_cleanup(
            cfg.cleanup,
            free_bytes=None,
            arrivals=0.0,
            bytes_rate=0.0,
            projected=None,
            bursting=False,
            urgent=False,
        )
        assert summary["reason"] == "no rows"
        assert summary["deleted"] == 0
        assert summary["candidates"] == 0
        assert summary["dry_run"] is False
        assert set(summary) == {
            "deleted",
            "candidates",
            "dry_run",
            "grace_s",
            "free",
            "bursting",
            "reason",
        }
    finally:
        store.close()


# --------------------------------------------------------------- /clean


@pytest.mark.anyio
async def test_clean_refuses_non_done_rows(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path, state=State.MOVING)
    try:
        text = await coord._clean_task_command(H)
        assert "only completed rows" in text
        assert "MOVING" in text
        coord._cleanup_fuse_healthy.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_refuses_active_swarm(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path, members=[_member(upspeed=1_000_000)])
    try:
        text = await coord._clean_task_command(H)
        assert "race activity" in text
        assert "Deleted" not in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_refuses_unmet_hr_minimums(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path, members=[_member(ratio=1.0)], min_ratio=2.0)
    try:
        text = await coord._clean_task_command(H)
        assert "H&R minimums" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_refuses_unverified_fuse(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path)
    coord._cleanup_fuse_healthy = AsyncMock(return_value=False)
    try:
        text = await coord._clean_task_command(H)
        assert "not verified" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_refuses_unpreservable_blob(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path)
    coord._cleanup_preserve_racing_blobs = AsyncMock(return_value=False)
    try:
        text = await coord._clean_task_command(H)
        assert "could not preserve" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_refuses_when_no_live_vps1_group(tmp_path):
    coord, store, _src, _row_ = _task_setup(tmp_path, members=[])
    try:
        text = await coord._clean_task_command(H)
        assert "No live VPS1 copy" in text
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_deletes_the_group_on_success(tmp_path):
    coord, store, src, _row_ = _task_setup(tmp_path)
    try:
        text = await coord._clean_task_command(H)
        assert "Deleted 1 VPS1 torrent(s)" in text
        src.delete.assert_awaited_once()
        assert src.delete.await_args.args == (H,)
        assert src.delete.await_args.kwargs == {"delete_files": True}
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_dry_run_deletes_nothing(tmp_path):
    coord, store, src, _row_ = _task_setup(tmp_path, dry_run=True)
    coord._delete_source_group = AsyncMock(return_value=True)
    try:
        text = await coord._clean_task_command(H)
        assert "dry-run is on" in text
        assert "would delete 1 VPS1 torrent(s)" in text
        coord._delete_source_group.assert_not_awaited()
        src.delete.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_resolves_unique_prefix(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        h = "deadbeef" + "0" * 32
        cfg = _cfg()
        src = _Source([_member(h)])
        coord = _coord(store, src, cfg)
        _row(store, h=h)
        coord._cleanup_fuse_healthy = AsyncMock(return_value=True)
        coord._cleanup_preserve_racing_blobs = AsyncMock(return_value=True)
        coord._delete_source_group = AsyncMock(return_value=True)
        text = await coord._clean_task_command("deadbeef")
        assert "Deleted 1" in text
        # Shorter than 6 hex chars is refused with the usage hint.
        short = await coord._clean_task_command("dead")
        assert "full 40-char hash" in short
    finally:
        store.close()


@pytest.mark.anyio
async def test_clean_reports_ambiguous_prefix(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        _row(store, h="abc123" + "0" * 34)
        _row(store, h="abc123" + "1" * 34)
        coord = _coord(store, _Source([]), _cfg())
        text = await coord._clean_task_command("abc123")
        assert "matches 2 torrents" in text
        assert "full 40-char hash" in text
    finally:
        store.close()


# -------------------------------------------------------------- act sheet


def _sheet_bot(*, enabled: bool = True, hint: str = "clean ~5h") -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._coord = SimpleNamespace(
        cfg=SimpleNamespace(cleanup=SimpleNamespace(enabled=enabled)),
        _cleanup_hint=lambda _ts: hint,
    )
    return bot


def _sheet_labels(rows) -> list[str]:
    return [label for row in rows for (label, _data) in row]


def _sheet_datas(rows) -> list[str]:
    return [data for row in rows for (_label, data) in row]


def test_act_sheet_offers_clean_button_only_for_done_groups():
    done = TorrentState(source_infohash=H, source_name=FNAME, total_bytes=FSIZE, state=State.DONE)
    _text, rows = _sheet_bot()._sheet_text_and_rows([done], FNAME, FSIZE, {})
    labels = _sheet_labels(rows)
    assert "🧹 Clean VPS1 copies" in labels
    assert f"clean:{H}" in _sheet_datas(rows)
    assert labels[-1] == "Close"  # Close stays last
    assert all(len(d.encode("utf-8")) <= 64 for d in _sheet_datas(rows))

    queued = TorrentState(
        source_infohash=H, source_name=FNAME, total_bytes=FSIZE, state=State.QUEUED
    )
    _t2, rows2 = _sheet_bot()._sheet_text_and_rows([queued], FNAME, FSIZE, {})
    assert "🧹 Clean VPS1 copies" not in _sheet_labels(rows2)

    _t3, rows3 = _sheet_bot(enabled=False)._sheet_text_and_rows([done], FNAME, FSIZE, {})
    assert "🧹 Clean VPS1 copies" not in _sheet_labels(rows3)


@pytest.mark.anyio
async def test_act_command_appends_cleanup_hint(tmp_path):
    store = StateStore(tmp_path / "s.db")
    try:
        done = TorrentState(
            source_infohash=H,
            source_name=FNAME,
            total_bytes=FSIZE,
            state=State.DONE,
            source_announce_url="https://alpha.cc/announce/xyz",
        )
        store.upsert(done)
        bot = object.__new__(TelegramBot)
        bot._cfg = TelegramConfig(enabled=True, bot_token="fake", chat_id="12345")
        bot._coord = _sheet_bot()._coord
        bot._store = store
        bot._bot = MagicMock()
        bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=5))
        bot._sheet_owner = {}
        bot._last_rendered_groups = 0
        await bot._act_command_entry(H, MagicMock())
        text = bot._bot.send_message.call_args[0][1]
        assert "Cleanup: clean ~5h" in text
        keyboard = bot._bot.send_message.call_args[1]["reply_markup"].inline_keyboard
        assert "🧹 Clean VPS1 copies" in [b.text for row in keyboard for b in row]
    finally:
        store.close()


@pytest.mark.anyio
async def test_router_routes_clean_button_to_task_cleaner():
    bot = object.__new__(TelegramBot)
    bot._clean_task_command = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    await bot._on_action_button(MagicMock(), f"clean:{H}")
    bot._clean_task_command.assert_awaited_once_with(H, None)
    bot._refresh_active_message.assert_awaited_once()


# ----------------------------------------------------------- cleanup hint


def _hint_coord(**over) -> object:
    coord = make_coordinator()
    coord.cfg = _cfg(**over)
    # At the low watermark -> minimum grace (2 h by default).
    coord._last_source_free_bytes = 15 * GiB
    return coord


def test_hint_empty_for_non_done_and_disabled():
    coord = _hint_coord()
    now = dt.datetime.now(dt.UTC)
    moving = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.MOVING,
        completed_at=now - dt.timedelta(hours=100),
    )
    assert coord._cleanup_hint(moving) == ""

    done = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
        completed_at=now - dt.timedelta(hours=1),
    )
    assert _hint_coord(enabled=False)._cleanup_hint(done) == ""

    no_seed = _hint_coord()
    no_seed.cfg.pipeline.seed = "none"
    assert no_seed._cleanup_hint(done) == ""


def test_hint_ready_past_grace():
    coord = _hint_coord()
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
        completed_at=now - dt.timedelta(hours=100),
    )
    assert coord._cleanup_hint(ts) == "clean ready"


def test_hint_ready_by_idle_fast_lane():
    coord = _hint_coord()
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
        completed_at=now - dt.timedelta(hours=1),
        vps1_last_activity_at=now - dt.timedelta(hours=2),
    )
    assert coord._cleanup_hint(ts) == "clean ready"


def test_hint_estimates_remaining_grace():
    coord = _hint_coord()
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
        completed_at=now - dt.timedelta(hours=1),
    )
    hint = coord._cleanup_hint(ts)
    assert hint.startswith("clean ~")
    assert hint == "clean ~1h"


# -------------------------------------------------------- active-list hint


def test_render_active_appends_cleanup_hint():
    ts = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
        source_announce_url="https://alpha.cc/announce/xyz",
    )
    base, _, _ = render_active([(ts, None)])
    hinted, _, _ = render_active([(ts, None)], cleanup={H: "clean ~5h"})
    assert "✅ Done · clean ~5h" in hinted
    assert "clean ~5h" not in base
    # Empty/absent dict leaves the render byte-identical.
    assert render_active([(ts, None)], cleanup={})[0] == base
    assert render_active([(ts, None)], cleanup=None)[0] == base


@pytest.mark.anyio
async def test_pending_summary_counts_ready_and_next(tmp_path):
    """The active-card footer shows completed rows awaiting VPS1 removal."""
    store = StateStore(tmp_path / "state.db")
    try:
        now = dt.datetime.now(dt.UTC)
        _row(store, h="a" * 40)  # completed 100h ago -> grace elapsed
        _row(
            store,
            h="b" * 40,
            completed_at=now,
            vps1_last_activity_at=None,
        )
        coord = _coord(store, _Source([]), _cfg())

        text = await coord._cleanup_pending_summary()

        assert text.startswith("🧹 VPS1 cleanup: 2 done")
        assert "1 ready" in text
        assert "next in ~" in text
    finally:
        store.close()


def test_detail_card_shows_cleanup_hint():
    from racing_sync.telegram_views import render_detail

    ts = TorrentState(
        source_infohash=H,
        source_name=FNAME,
        total_bytes=FSIZE,
        state=State.DONE,
    )

    assert "VPS1 cleanup: clean ~2h" in render_detail(ts, cleanup="clean ~2h")
    assert "VPS1 cleanup" not in render_detail(ts)


@pytest.mark.anyio
async def test_cleanup_command_runs_in_background():
    """A multi-minute burst pass must not block Telegram update polling."""
    bot = object.__new__(TelegramBot)
    bot._coord = SimpleNamespace(_cleanup_now_command=AsyncMock(return_value="🧹 done"))
    bot._reply = AsyncMock()
    bot._refresh_active_message = AsyncMock()
    bot._last_active_cache = None
    message = SimpleNamespace()

    await bot._cleanup_now_command(message)

    task = getattr(bot, "_cleanup_task", None)
    assert task is not None
    await asyncio.wait_for(task, 1.0)
    bot._reply.assert_awaited_once()
    assert "done" in bot._reply.await_args.args[0]
