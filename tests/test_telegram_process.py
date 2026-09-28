"""Tests for Telegram /add .torrent ingestion (chat -> VPS2 pipeline)."""
from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import make_coordinator  # noqa: F401 (keeps sys.path consistent)

from racing_sync.state import State, StateStore
from racing_sync.telegram_bot import TelegramBot
from racing_sync.watchdir import _bencode, _bencoded_info_hash


def _torrent_bytes(name="Show.S01E01.mkv",
                   announce="https://tracker.example/announce/SECRETKEY"):
    blob = _bencode({
        b"announce": announce.encode(),
        b"info": {
            b"name": name.encode(),
            b"length": 100,
            b"piece length": 16384,
            b"pieces": b"0" * 20,
        },
    })
    assert isinstance(blob, (bytes, bytearray))
    return bytes(blob)


def _bot(store, **kw):
    bot = object.__new__(TelegramBot)
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5)
    bot._callback_times = {}
    bot._store = store
    bot._bot = SimpleNamespace(
        get_file=AsyncMock(),
        delete_message=AsyncMock(),
        send_message=AsyncMock(
            return_value=SimpleNamespace(message_id=7)),
    )
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _doc(file_name="show.torrent", size=200, file_id="fid1", mid=50):
    return SimpleNamespace(
        chat=SimpleNamespace(id="1"),
        from_user=SimpleNamespace(id="9"),
        message_id=mid,
        text=None,
        caption=None,
        document=SimpleNamespace(
            file_name=file_name, file_size=size, file_id=file_id,
            mime_type="application/x-bittorrent"),
        reply_to_message=None,
    )


def _cmd(text="/add", mid=51, user="9", reply_to=None):
    return SimpleNamespace(
        chat=SimpleNamespace(id="1"),
        from_user=SimpleNamespace(id=user),
        message_id=mid,
        text=text,
        caption=None,
        document=None,
        reply_to_message=reply_to,
    )


def _sent_texts(bot):
    return [str(c.args[1]) for c in
            bot._bot.send_message.await_args_list]


@pytest.mark.anyio
async def test_process_reply_ingests_and_deletes(tmp_path):
    """Reply /add: valid .torrent becomes a telegram NEW row; file deleted."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        infohash, name, size, announce = _bencoded_info_hash(blob)
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        file_msg = _doc(mid=50)
        await bot._handle_chat_message(_cmd(reply_to=file_msg))

        row = store.get(infohash)
        assert row is not None
        assert row.state == State.NEW
        assert row.cross_seed_source == "telegram"
        assert row.source_name == name
        assert row.total_bytes == size
        assert bytes(store.get_blob(infohash)) == blob
        # File message deleted (passkey hygiene).
        bot._bot.delete_message.assert_awaited_once_with(
            chat_id="1", message_id=50)
        # Confirm names the domain, never the passkeyed URL.
        texts = _sent_texts(bot)
        assert any("Queued" in t for t in texts)
        assert any("tracker.example" in t for t in texts)
        assert all("SECRETKEY" not in t for t in texts)
        assert all(announce not in t for t in texts)
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_caption_variant(tmp_path):
    """File sent with /add as caption processes its own document."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes(name="Other.mkv")
        infohash, _, _, _ = _bencoded_info_hash(blob)
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        msg = _doc(mid=60)
        msg.caption = "/add"
        await bot._handle_chat_message(msg)

        assert store.get(infohash) is not None
        bot._bot.delete_message.assert_awaited_once_with(
            chat_id="1", message_id=60)
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_without_document_hints(tmp_path):
    """/add with nothing to process: usage hint, no ingest, no delete."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store)
        await bot._handle_chat_message(_cmd())
        assert store.all() == []
        bot._bot.get_file.assert_not_called()
        bot._bot.delete_message.assert_not_called()
        assert any("Reply to" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_rejects_non_torrent(tmp_path):
    """Wrong suffix: refused before any download."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store)
        file_msg = _doc(file_name="notes.txt")
        await bot._handle_chat_message(_cmd(reply_to=file_msg))
        assert store.all() == []
        bot._bot.get_file.assert_not_called()
        bot._bot.delete_message.assert_not_called()
        assert any("Not a .torrent" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_rejects_oversize_without_download(tmp_path):
    """Declared size over cap: refused, Telegram file never fetched."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store)
        file_msg = _doc(size=100 * 1024 * 1024)
        await bot._handle_chat_message(_cmd(reply_to=file_msg))
        assert store.all() == []
        bot._bot.get_file.assert_not_called()
        assert any("Refusing" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_rejects_invalid_bytes(tmp_path):
    """Garbage bytes: no row, file left for the operator (evidence)."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(b"junk"))))
        await bot._handle_chat_message(_cmd(reply_to=_doc()))
        assert store.all() == []
        bot._bot.delete_message.assert_not_called()
        assert any("Not a valid" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_duplicate_reports_and_still_deletes(tmp_path):
    """Already-tracked infohash: reported, and the chat copy still removed."""
    from racing_sync.state import TorrentState

    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        infohash, _, _, _ = _bencoded_info_hash(blob)
        store.upsert(TorrentState(
            source_infohash=infohash, source_name="Show",
            state=State.DOWNLOADING))
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        await bot._handle_chat_message(_cmd(reply_to=_doc(mid=50)))

        assert store.get(infohash).state == State.DOWNLOADING
        assert any("Already tracked" in t for t in _sent_texts(bot))
        bot._bot.delete_message.assert_awaited_once_with(
            chat_id="1", message_id=50)
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_respects_ignore_list(tmp_path):
    """Cancelled (ignored) infohash: not revived, chat copy still removed."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        infohash, _, _, _ = _bencoded_info_hash(blob)
        store.ignore_torrent(infohash, "Show")
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        await bot._handle_chat_message(_cmd(reply_to=_doc(mid=50)))

        assert store.get(infohash) is None
        assert any("Ignored" in t for t in _sent_texts(bot))
        bot._bot.delete_message.assert_awaited_once_with(
            chat_id="1", message_id=50)
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_unauthorized_user_refused(tmp_path):
    """Allowlist set: strangers cannot start downloads via /add."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        bot = _bot(store)
        bot._cfg = SimpleNamespace(chat_id="1", page_size=5,
                                   admin_user_ids=[4242])
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        await bot._handle_chat_message(_cmd(user="9", reply_to=_doc()))

        assert store.all() == []
        bot._bot.get_file.assert_not_called()
        assert any("Not authorized" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_wrong_chat_ignored(tmp_path):
    """Messages outside the configured chat never act."""
    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store)
        msg = _cmd(reply_to=_doc())
        msg.chat = SimpleNamespace(id="2")
        msg.from_user = SimpleNamespace(id="3")
        await bot._handle_chat_message(msg)
        assert store.all() == []
        bot._bot.send_message.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_delete_failure_nudges_manual_removal(tmp_path):
    """No deletion rights: ingest still lands, operator told to remove by hand."""
    from telegram.error import TelegramError

    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        infohash, _, _, _ = _bencoded_info_hash(blob)
        bot = _bot(store)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))
        bot._bot.delete_message = AsyncMock(
            side_effect=TelegramError("not enough rights"))

        await bot._handle_chat_message(_cmd(reply_to=_doc(mid=50)))

        assert store.get(infohash) is not None
        texts = _sent_texts(bot)
        assert any("Queued" in t for t in texts)
        assert any("remove it by hand" in t for t in texts)
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_delete_disabled_keeps_file(tmp_path):
    """delete_processed_torrent=false: ingest lands, chat copy untouched."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _torrent_bytes()
        infohash, _, _, _ = _bencoded_info_hash(blob)
        bot = _bot(store)
        bot._cfg = SimpleNamespace(chat_id="1", page_size=5,
                                   delete_processed_torrent=False)
        bot._bot.get_file = AsyncMock(return_value=SimpleNamespace(
            download_to_memory=AsyncMock(return_value=io.BytesIO(blob))))

        await bot._handle_chat_message(_cmd(reply_to=_doc(mid=50)))

        assert store.get(infohash) is not None
        texts = _sent_texts(bot)
        assert any("Queued" in t for t in texts)
        bot._bot.delete_message.assert_not_called()
        assert all("remove it by hand" not in t for t in texts)
    finally:
        store.close()
