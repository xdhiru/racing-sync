"""ButtonMaker budget enforcement + sheet-builder sweep (revamp item 2)."""

from unittest.mock import AsyncMock, MagicMock

import pytest


def _bot_with(rows):
    import tempfile
    from pathlib import Path

    from racing_sync.state import StateStore
    from racing_sync.telegram_bot import TelegramBot

    tmp = Path(tempfile.mkdtemp())
    store = StateStore(tmp / "state.db")
    for r in rows:
        store.upsert(r)
    bot = object.__new__(TelegramBot)
    bot._cfg = MagicMock()
    bot._cfg.chat_id = "1"
    bot._cfg.page_size = 5
    bot._callback_times = {}
    bot._store = store
    bot._coord = MagicMock()
    bot._current_page = 0
    bot._refresh_active_message = AsyncMock()
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=7))
    return bot, store


def _sheet_datas(bot):
    out = []
    for c in bot._bot.send_message.await_args_list:
        try:
            markup = c.kwargs.get("reply_markup")
        except Exception:
            continue
        if markup is None:
            continue
        for r in markup.inline_keyboard:
            out.extend(b.callback_data for b in r)
    return out


def test_check_button_rejects_bad_buttons():
    import pytest as _pytest

    from racing_sync.telegram_buttons import (
        MAX_CALLBACK_BYTES,
        check_button,
    )

    assert check_button("Ok", "go:abc") == ("Ok", "go:abc")
    assert check_button("x" * 200, "abort")[0] == "x" * 200
    with _pytest.raises(ValueError):
        check_button("", "abort")
    with _pytest.raises(ValueError):
        check_button("Ok", "")
    with _pytest.raises(ValueError):
        check_button("Ok", "k:" + "y" * 70)
    # Byte budget, not character budget: 32×2-byte chars fit exactly.
    assert check_button("Ok", "é" * 32)[1] == "é" * 32
    with _pytest.raises(ValueError):
        check_button("Ok", "é" * 33)
    assert MAX_CALLBACK_BYTES == 64


def test_maker_layout_and_truncation():
    from racing_sync.telegram_buttons import ButtonMaker

    maker = (
        ButtonMaker()
        .add("A", "a:1")
        .add("B", "b:2")
        .newline()
        .add("C", "c:3")
        .newline()
        .add("D" * 100, "d:4")
    )
    rows = maker.rows()
    assert rows == [[("A", "a:1"), ("B", "b:2")], [("C", "c:3")], [("D" * 100, "d:4")]]
    assert len(maker) == 4
    markup = maker.build()
    assert markup.inline_keyboard[2][0].text == "D" * 60
    assert markup.inline_keyboard[2][0].callback_data == "d:4"
    # Trailing newline collapses (no empty row shipped).
    assert len(ButtonMaker().add("X", "x").newline().rows()) == 1
    # Legacy rows validate eagerly.
    with pytest.raises(ValueError):
        ButtonMaker.from_rows([[("Ok", "k:" + "y" * 70)]])
    assert ButtonMaker.from_rows([[("Ok", "ok:1")], [("Close", "abort")]]).rows() == [
        [("Ok", "ok:1")],
        [("Close", "abort")],
    ]


def test_validate_rows_warns_and_drops(caplog):
    import logging

    from racing_sync.telegram_buttons import validate_rows

    with caplog.at_level(logging.WARNING):
        clean = validate_rows(
            [[("Ok", "ok:1"), ("", "nope"), ("Big", "k:" + "y" * 70)], [("Close", "abort")]]
        )
    assert clean == [[("Ok", "ok:1")], [("Close", "abort")]]
    assert "dropping invalid button" in caplog.text


@pytest.mark.anyio
async def test_sheet_builders_all_fit_budget():
    """Every real sheet builder emits buttons within 64 bytes."""
    from racing_sync.state import State, TorrentState

    bot, store = _bot_with(
        [
            TorrentState(
                source_infohash="a" * 40,
                source_name="Fit.Show",
                total_bytes=1000,
                state=State.QUEUED,
            ),
            TorrentState(
                source_infohash="b" * 40,
                source_name="Fit.Show",
                total_bytes=1000,
                state=State.QUEUED,
            ),
        ]
    )
    try:
        m = MagicMock()
        m.chat.id = "1"
        m.from_user.id = "9"
        m.text = "/act_1"
        m.caption = None
        m.message_id = 42
        await bot._handle_chat_message(m)
        _text, _rows = bot._sheet_text_and_rows(
            [store.get("a" * 40), store.get("b" * 40)], "Fit.Show", 1000, {}
        )
        _t2, _rows2 = bot._keepq_text_and_rows(
            "Fit.Show", " · all 2 copies", "all:" + "a" * 40, 1000
        )
        for data in _sheet_datas(bot) + [d for r in list(_rows) + list(_rows2) for (_, d) in r]:
            assert len(data.encode("utf-8")) <= 64, data
    finally:
        store.close()
