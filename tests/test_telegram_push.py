"""The operator surface for "push from VPS1": `/push_source` + the act sheet.

Fictional names only ("Example.Show.S01E01…-ExampleGroup", "seedremote"),
as everywhere in this repo.

The backend (`coordinator_push.PushMixin`) is pinned by
`test_coordinator_push.py`; this file pins only the operator half:

* the group target is the resolution `/inject` already performs — the
  elected primary, so one tap means exactly one `start_source_push`;
* positional numbers honour the displayed status filter;
* with `[source.rclone_push].enabled` anything but literally `true` the
  command refuses naming the switch, calls no coordinator method, and the
  act sheet shows no button;
* the backend's one-line answer is relayed verbatim (both directions);
* the sheet button asks first (Yes/No), `✖ No` and `Close` change nothing,
  a second tap is absorbed by the in-flight guard;
* every `push:` callback shape parses and stays inside the 64-byte budget.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from racing_sync.clients.abstract import Torrent
from racing_sync.state import State, TorrentState
from racing_sync.telegram_bot import TelegramBot
from racing_sync.telegram_protocol import (
    PUSH_SOURCE_RE,
    _parse_action_data,
    _push_enabled,
    _slow_tap,
)

LEADER = "a" * 40
SIBLING = "b" * 40
ELECTED = "c" * 40
NAME = "Example.Show.S01E01.1080p.WEB-DL.x264-ExampleGroup"
OTHER_NAME = "Example.Show.S01E02.1080p.WEB-DL.x264-ExampleGroup"
REASON = "pushing 2 file(s) from VPS1 to seedremote:Unsorted"


class _Store:
    """Minimal duck of `StateStore`: hash -> row, plus the active list."""

    def __init__(self, rows: list[TorrentState]) -> None:
        self._rows = {r.source_infohash: r for r in rows}

    def get(self, h, include_blob=True):  # noqa: ANN001, ARG002 (duck-typed)
        return self._rows.get((h or "").lower())

    def list_active_inflight(self) -> list[TorrentState]:
        return [r for r in self._rows.values() if r.state not in (State.DONE, State.FAILED)]


def _row(h: str, *, state: State = State.WAITING_INDEXER, name: str = NAME) -> TorrentState:
    return TorrentState(source_infohash=h, source_name=name, total_bytes=1000, state=state)


def _cfg(*, enabled: bool = True) -> SimpleNamespace:
    """`[source.rclone_push]` exactly as the strict reader expects it."""
    return SimpleNamespace(source=SimpleNamespace(rclone_push=SimpleNamespace(enabled=enabled)))


def _coord(
    store: _Store,
    *,
    cfg: SimpleNamespace | None = None,
    elected: str | None = None,
    started: bool = True,
    reason: str = REASON,
    live: bool = False,
    resolve_error: BaseException | None = None,
) -> SimpleNamespace:
    async def _resolve(h):  # noqa: ANN001 (duck-typed coordinator)
        if resolve_error is not None:
            raise resolve_error
        want = (elected or str(h or "")).lower()
        return {
            "primary": Torrent(
                hash=want,
                name=NAME,
                category="",
                save_path="/vps1",
                size_bytes=1000,
                state="seeding",
                progress=1.0,
            ),
            "members": [],
            "row": _row(want),
            "title": NAME,
            "size": 1000,
        }

    return SimpleNamespace(
        cfg=cfg if cfg is not None else _cfg(),
        store=store,
        injectfuse_resolve=AsyncMock(side_effect=_resolve),
        start_source_push=AsyncMock(return_value=(started, reason)),
        push_live_for=lambda _h: live,
    )


def _bot(store: _Store, coord: SimpleNamespace) -> TelegramBot:
    bot = object.__new__(TelegramBot)
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5, admin_user_ids=[])
    bot._callback_times = {}
    bot._action_inflight = {}
    bot._store = store
    bot._coord = coord
    bot._last_active_cache = None
    bot._last_rendered_groups = 0
    bot._status_filter = "all"
    bot._sheet_owner = {}
    bot._bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=7)))
    bot._send_sheet = AsyncMock(return_value=True)
    bot._refresh_active_message = AsyncMock()
    bot._delete_query_message = AsyncMock()
    bot._mark_sheet_working = AsyncMock()
    return bot


def _reply_texts(bot: TelegramBot) -> list[str]:
    return [str(c.args[1]) for c in bot._bot.send_message.await_args_list]


def _sheet_datas(rows: list) -> list[str]:
    return [data for row in rows for (_label, data) in row]


def _sheet_labels(rows: list) -> list[str]:
    return [label for row in rows for (label, _data) in row]


def _message(text: str = "/push_source 1") -> SimpleNamespace:
    return SimpleNamespace(
        chat=SimpleNamespace(id="1"),
        from_user=SimpleNamespace(id="9"),
        message_id=51,
        text=text,
        caption=None,
        document=None,
        reply_to_message=None,
    )


# --------------------------------------------------------------- command path


@pytest.mark.anyio
async def test_push_targets_the_elected_primary_exactly_once():
    """Three tracked copies, one `start_source_push` — on the elected one."""
    store = _Store([_row(LEADER), _row(SIBLING), _row(ELECTED)])
    coord = _coord(store, elected=ELECTED)
    bot = _bot(store, coord)
    await bot._start_group_command("pushsource", "1", _message())
    await bot._push_task

    assert coord.start_source_push.await_count == 1
    pushed = coord.start_source_push.await_args.args[0]
    assert pushed.source_infohash == ELECTED
    assert _reply_texts(bot) == [REASON]


@pytest.mark.anyio
async def test_push_falls_back_to_the_leader_when_the_primary_is_untracked():
    store = _Store([_row(LEADER), _row(SIBLING)])
    coord = _coord(store, elected=ELECTED)
    bot = _bot(store, coord)
    await bot._start_group_command("pushsource", "1", _message())
    await bot._push_task

    pushed = coord.start_source_push.await_args.args[0]
    assert pushed.source_infohash == LEADER


@pytest.mark.anyio
async def test_push_falls_back_when_vps1_no_longer_lists_the_group():
    """/inject's resolution failing is not a refusal: the tracked row is enough."""
    store = _Store([_row(LEADER)])
    coord = _coord(store, resolve_error=LookupError("hash not on VPS1"))
    bot = _bot(store, coord)
    await bot._start_group_command("pushsource", "1", _message())
    await bot._push_task

    assert coord.start_source_push.await_count == 1
    assert coord.start_source_push.await_args.args[0].source_infohash == LEADER


@pytest.mark.anyio
async def test_push_positional_number_follows_the_displayed_filter():
    """`#1` means the first row the list shows, not the first row stored."""
    store = _Store([_row(LEADER, state=State.MOVING), _row(ELECTED, name=OTHER_NAME)])
    coord = _coord(store)
    bot = _bot(store, coord)

    bot._status_filter = "qd"
    await bot._start_group_command("pushsource", "1", _message())
    await bot._push_task
    assert coord.injectfuse_resolve.await_args.args[0] == ELECTED

    bot._status_filter = "all"
    coord.injectfuse_resolve.reset_mock()
    await bot._start_group_command("pushsource", "1", _message())
    await bot._push_task
    assert coord.injectfuse_resolve.await_args.args[0] == LEADER


@pytest.mark.anyio
async def test_push_command_passes_reason_manual():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    await bot._push_source_command(LEADER, NAME, _message())
    await bot._push_task
    assert coord.start_source_push.await_args.kwargs == {"reason": "manual"}


@pytest.mark.anyio
async def test_push_switch_off_refuses_naming_the_switch():
    """Off means off: the answer names the key, nothing else happens."""
    store = _Store([_row(LEADER)])
    coord = _coord(store, cfg=_cfg(enabled=False))
    bot = _bot(store, coord)
    await bot._push_source_command(LEADER, NAME, _message())

    text = _reply_texts(bot)[0]
    assert "[source.rclone_push].enabled" in text
    coord.start_source_push.assert_not_awaited()
    coord.injectfuse_resolve.assert_not_awaited()
    assert bot._action_inflight == {}
    bot._refresh_active_message.assert_not_awaited()


@pytest.mark.anyio
async def test_push_switch_off_refuses_a_mock_config():
    """A MagicMock config (truthy attributes) must read as off, never on."""
    store = _Store([_row(LEADER)])
    coord = _coord(store, cfg=MagicMock())
    bot = _bot(store, coord)
    await bot._push_source_command(LEADER, NAME, _message())
    assert "Push is off" in _reply_texts(bot)[0]
    coord.start_source_push.assert_not_awaited()


@pytest.mark.anyio
async def test_push_backend_answer_is_verbatim():
    store = _Store([_row(LEADER)])
    why = "push refused: VPS2 is already downloading for this content"
    coord = _coord(store, started=False, reason=why)
    bot = _bot(store, coord)
    await bot._push_source_command(LEADER, NAME, _message())
    await bot._push_task
    assert _reply_texts(bot) == [why]


@pytest.mark.anyio
async def test_push_guard_is_released_and_the_card_refreshed():
    store = _Store([_row(LEADER)])
    bot = _bot(store, _coord(store))
    await bot._push_source_command(LEADER, NAME, _message())
    assert f"push:{LEADER}" in bot._action_inflight
    await bot._push_task
    assert bot._action_inflight == {}
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_push_duplicate_tap_is_deduped():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    bot._action_inflight[f"push:{LEADER}"] = time.monotonic()
    await bot._push_source_command(LEADER, NAME, _message())
    assert _reply_texts(bot)[0].startswith("Already working on it")
    coord.start_source_push.assert_not_awaited()


@pytest.mark.anyio
async def test_push_short_token_answers_with_usage():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    await bot._push_source_command("abc123", "", _message())
    assert "/push_source" in _reply_texts(bot)[0]
    coord.start_source_push.assert_not_awaited()


# ------------------------------------------------------------------ act sheet


def _sheet_bot(*, enabled: bool = True, live: bool = False, members: list | None = None):
    store = _Store(members or [_row(LEADER)])
    coord = _coord(store, cfg=_cfg(enabled=enabled), live=live)
    bot = _bot(store, coord)
    return bot


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (State.NEW, True),
        (State.WAITING_INDEXER, True),
        (State.WAITING_DISK, True),
        (State.QUEUED, True),
        (State.DOWNLOADING, False),
        (State.MOVING, False),
        (State.RE_ADDING, False),
        (State.DONE, False),
        (State.FAILED, False),
    ],
)
def test_push_button_offers_only_states_the_push_can_take(state, expected):
    bot = _sheet_bot()
    _text, rows = bot._sheet_text_and_rows([_row(LEADER, state=state)], NAME, 1000, {})
    labels = _sheet_labels(rows)
    assert ("📤 Push from VPS1…" in labels) is expected
    assert (f"push:all:{LEADER}" in _sheet_datas(rows)) is expected
    assert all(len(d.encode("utf-8")) <= 64 for d in _sheet_datas(rows))


def test_push_button_absent_with_switch_off_and_sheet_unchanged():
    off = _sheet_bot(enabled=False)
    _text, rows = off._sheet_text_and_rows([_row(LEADER)], NAME, 1000, {})
    assert "📤 Push from VPS1…" not in _sheet_labels(rows)
    assert "Push" not in _text

    on = _sheet_bot(enabled=True)
    _text_on, rows_on = on._sheet_text_and_rows([_row(LEADER)], NAME, 1000, {})
    assert "📤 Push from VPS1…" in _sheet_labels(rows_on)
    assert "Cancel/Inject/Push ask first" in _text_on
    # Everything else the sheet offers is the same in both shapes.
    assert [d for d in _sheet_datas(rows_on) if not d.startswith("push:")] == _sheet_datas(rows)


def test_push_button_hidden_while_a_push_is_live():
    bot = _sheet_bot(live=True)
    _text, rows = bot._sheet_text_and_rows([_row(LEADER)], NAME, 1000, {})
    assert "📤 Push from VPS1…" not in _sheet_labels(rows)
    assert "Cancel/Inject ask first" in _text


def test_push_enabled_reader_is_strict():
    assert _push_enabled(_cfg(enabled=True)) is True
    assert _push_enabled(_cfg(enabled=False)) is False
    assert _push_enabled(SimpleNamespace(source=SimpleNamespace())) is False
    assert _push_enabled(SimpleNamespace()) is False
    assert _push_enabled(MagicMock()) is False
    assert _push_enabled(None) is False


# --------------------------------------------------------------------- router


@pytest.mark.anyio
async def test_push_tap_asks_before_anything_changes():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    await bot._on_action_button(MagicMock(), f"push:all:{LEADER}")

    text, rows = bot._send_sheet.await_args.args
    assert "Example" in text or LEADER[:10] in text
    assert f"push:all:{LEADER}:yes" in _sheet_datas(rows)
    assert f"push:all:{LEADER}:no" in _sheet_datas(rows)
    assert ("Close", "abort") in [b for row in rows for b in row]
    coord.start_source_push.assert_not_awaited()


@pytest.mark.anyio
async def test_push_yes_round_trip_pushes_once():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    await bot._on_action_button(MagicMock(), f"push:all:{LEADER}:yes")
    await bot._push_task

    assert coord.start_source_push.await_count == 1
    assert _reply_texts(bot)[-1] == REASON


@pytest.mark.anyio
async def test_push_no_changes_nothing():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    await bot._on_action_button(MagicMock(), f"push:all:{LEADER}:no")

    assert "Not pushed — nothing changed." in _reply_texts(bot)
    coord.start_source_push.assert_not_awaited()
    bot._refresh_active_message.assert_awaited_once()


@pytest.mark.anyio
async def test_push_close_button_changes_nothing():
    store = _Store([_row(LEADER)])
    coord = _coord(store)
    bot = _bot(store, coord)
    q = MagicMock()
    q.data = "abort"
    await bot._on_action_button(q, "abort")
    bot._delete_query_message.assert_awaited_once()
    coord.start_source_push.assert_not_awaited()


@pytest.mark.anyio
async def test_callback_gate_routes_push_taps():
    """Without the `push:` prefix gate the tap would hang forever."""
    store = _Store([_row(LEADER)])
    bot = _bot(store, _coord(store))
    bot._push_ask_send = AsyncMock()
    q = MagicMock()
    q.data = f"push:all:{LEADER}"
    q.message.chat.id = "1"
    q.from_user.id = 9
    await bot._handle_callback(q)
    bot._push_ask_send.assert_awaited_once()

    slow = MagicMock()
    slow.data = f"push:all:{LEADER}:yes"
    slow.message.chat.id = "1"
    slow.from_user.id = 9
    bot._push_source_command = AsyncMock()
    # A different opener: the per-tap debounce is per chat+user, and the
    # two taps here are two steps of one flow, not a double-tap.
    bot._callback_times = {}
    await bot._handle_callback(slow)
    bot._mark_sheet_working.assert_awaited_once()
    bot._push_source_command.assert_awaited_once_with(LEADER, "", None)


# -------------------------------------------------------------- protocol bits


def test_push_callback_shapes_parse():
    assert _parse_action_data(f"push:{LEADER}") == ("push", False, LEADER, "", None)
    assert _parse_action_data(f"push:all:{LEADER}") == ("push", True, LEADER, "", None)
    assert _parse_action_data(f"push:{LEADER}:yes") == ("push", False, LEADER, "yes", None)
    assert _parse_action_data(f"push:{LEADER}:no") == ("push", False, LEADER, "no", None)
    assert _parse_action_data(f"push:all:{LEADER}:yes") == ("push", True, LEADER, "yes", None)
    assert _parse_action_data(f"push:all:{LEADER}:no") == ("push", True, LEADER, "no", None)
    for bad in (f"push:{LEADER}:maybe", "push:all:xyz", f"push:{'a' * 39}", "pushx:" + LEADER):
        assert _parse_action_data(bad) == ("", False, "", "", None), bad


def test_push_callback_data_fits_the_byte_budget():
    for data in (
        f"push:{LEADER}",
        f"push:all:{LEADER}",
        f"push:{LEADER}:yes",
        f"push:{LEADER}:no",
        f"push:all:{LEADER}:yes",
        f"push:all:{LEADER}:no",
    ):
        assert len(data.encode("utf-8")) <= 64, data


def test_push_yes_tap_is_flagged_slow():
    """The SSH preflight runs under the tap: it needs the working state."""
    assert _slow_tap(f"push:all:{LEADER}:yes") is True
    assert _slow_tap(f"push:all:{LEADER}:no") is False
    assert _slow_tap(f"push:all:{LEADER}") is False


def test_push_source_pattern_accepts_both_separators():
    for text, token in (
        ("/push_source 3", "3"),
        ("/push_source_3", "3"),
        (f"/push_source {'c' * 40}", "c" * 40),
        ("/push_source_abc123", "abc123"),
        ("/push_source 3@my_bot", "3"),
    ):
        m = PUSH_SOURCE_RE.match(text)
        assert m is not None, text
        assert m.group(1) == token, text
    for text in ("/push_source", "/push_source_", "/push", "/pushsource 3", "/push_sourcex 3"):
        assert PUSH_SOURCE_RE.match(text) is None, text
