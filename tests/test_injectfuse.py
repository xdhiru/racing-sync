"""Telegram /injectfuse: manual fuse seeding with Yes/No confirm.

Operator moved the bytes to the remote by hand; the command verifies
fuse readiness first and changes nothing unless every member verifies:
tracked rows keep their prior state, untracked groups gain no row.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.state import ALLOWED, State, StateStore, TorrentState
from racing_sync.telegram_bot import (
    INJECTFUSE_CMD_RE,
    INJECTFUSE_HASH_RE,
    TelegramBot,
)
from racing_sync.watchdir import _bencode

H1 = "a" * 40
H2 = "b" * 40
NAME = "Show.S01E01.1080p"
TRACKER1 = "https://tracker.example/announce?passkey=AAA"
TRACKER2 = "https://beta.example/announce?passkey=BBB"


def _torrent(h, tracker, size=500):
    return Torrent(
        hash=h, name=NAME, category="", save_path="/vps1",
        size_bytes=size, state="seeding", progress=1.0,
        trackers=[tracker], files=[],
    )


def _blob(fname="Solo.mkv", size=500):
    return _bencode({
        b"info": {b"name": fname.encode(), b"length": size},
    })


def _coord(tmp_path, torrents, store=None):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    ssd.mkdir(exist_ok=True)
    fuse.mkdir(exist_ok=True)
    fuse_u.mkdir(exist_ok=True)
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse_u
    coord.cfg.classifier._episode_re = None
    coord.store = store or StateStore(tmp_path / "state.db")
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=list(torrents))
    coord.sftp = None
    coord.dest_client = AsyncMock()
    coord._live = {}
    return coord, ssd, fuse


def _bot(store, coord=None, **kw):
    bot = object.__new__(TelegramBot)
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5)
    bot._callback_times = {}
    bot._store = store
    bot._coord = coord
    bot._pending_pick = None
    bot._last_active_cache = None
    bot._bot = SimpleNamespace(
        send_message=AsyncMock(
            return_value=SimpleNamespace(message_id=7)),
    )
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _cmd(text, mid=51):
    return SimpleNamespace(
        chat=SimpleNamespace(id="1"),
        from_user=SimpleNamespace(id="9"),
        message_id=mid,
        text=text, caption=None, document=None,
        reply_to_message=None,
    )

def _sent_texts(bot):
    return [str(c.args[1]) for c in
            bot._bot.send_message.await_args_list]


# ---- state edges ----

def test_allowed_edges_for_injectfuse():
    assert State.RE_ADDING in ALLOWED[State.QUERYING]
    assert State.RE_ADDING in ALLOWED[State.WAITING_INDEXER]
    assert State.RE_ADDING in ALLOWED[State.WAITING_DISK]
    assert State.RE_ADDING in ALLOWED[State.FAILED]
    assert State.RE_ADDING in ALLOWED[State.NEW]


# ---- coordinator core ----

@pytest.mark.anyio
async def test_resolve_unknown_hash(tmp_path):
    coord, _, _ = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    try:
        with pytest.raises(LookupError):
            await coord.injectfuse_resolve("c" * 40)
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_resolve_ignored_group_still_resolves(tmp_path):
    coord, _, _ = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    try:
        coord.store.ignore_torrent(H1, NAME)
        resolved = await coord.injectfuse_resolve(H1)
        assert resolved["ignored"] is True
        assert len(resolved["members"]) == 1
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_resolve_finds_row_by_sibling_hash(tmp_path):
    coord, _, _ = _coord(
        tmp_path, [_torrent(H1, TRACKER1), _torrent(H2, TRACKER2)])
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        resolved = await coord.injectfuse_resolve(H2)
        assert len(resolved["members"]) == 2
        assert resolved["row"] is not None
        assert resolved["row"].source_infohash == H1
        assert resolved["title"].startswith("Show")
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_verify_ready_no_state_change(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        resolved = await coord.injectfuse_resolve(H1)
        verified = await coord.injectfuse_verify(resolved["members"])
        assert verified["ready"] is True
        assert verified["problems"] == []
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_verify_missing_no_state_change(tmp_path):
    coord, _, _fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        resolved = await coord.injectfuse_resolve(H1)
        verified = await coord.injectfuse_verify(resolved["members"])
        assert verified["ready"] is False
        assert any("Solo.mkv" in p for p in verified["problems"])
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_drives_waiting_row(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("injecting")
        assert coord.store.get(H1).state == State.RE_ADDING
        # The verified blob must reach the row or the fuse gate parks
        # forever on "missing blob".
        assert bytes(coord.store.get_blob(H1)) == blob
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_keeps_existing_blob(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    other = _blob("Other.mkv", 500)
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER, cross_seed_blob=other))
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("injecting")
        assert bytes(coord.store.get_blob(H1)) == other
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_missing_keeps_prior_state(tmp_path):
    coord, _, _fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("Not injected")
        assert "still waiting_indexer" in out
        assert "Solo.mkv" in out
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_preempts_active_download(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.DOWNLOADING))
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("injecting")
        assert "SSD downloading stopped" in out
        assert coord.store.get(H1).state == State.RE_ADDING
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_lifts_cancel(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        coord.store.ignore_torrent(H1, NAME)
        coord.store.tombstone(H1)
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("injecting")
        assert coord.store.get(H1).state == State.RE_ADDING
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_creates_row_when_untracked(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        assert coord.store.get(H1) is None
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("injecting")
        row = coord.store.get(H1)
        assert row is not None
        assert row.state == State.RE_ADDING
        assert row.cross_seed_source == "injectfuse"
        assert bytes(row.cross_seed_blob) == blob
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_untracked_missing_creates_nothing(tmp_path):
    coord, _, _fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    try:
        out = await coord.injectfuse_confirmed([H1])
        assert out.startswith("Not injected")
        assert coord.store.get(H1) is None
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_confirmed_done_noop(tmp_path):
    coord, _, fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    blob = _blob()
    coord.source_client.export_torrent = AsyncMock(return_value=blob)
    (fuse / "Solo.mkv").write_bytes(b"x" * 500)
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.DONE))
        out = await coord.injectfuse_confirmed([H1])
        assert "already done" in out
        assert coord.store.get(H1).state == State.DONE
    finally:
        coord.store.close()


# ---- telegram layer ----

def test_regexes():
    assert INJECTFUSE_CMD_RE.match("/injectfuse_3")
    assert INJECTFUSE_CMD_RE.match("/injectfuse_" + H1)
    assert not INJECTFUSE_CMD_RE.match("/injectfuse " + H1)
    assert INJECTFUSE_HASH_RE.match("/injectfuse " + H1)
    assert INJECTFUSE_HASH_RE.match("/injectfuse  " + H1.upper())
    assert not INJECTFUSE_HASH_RE.match("/injectfuse_3")


def test_pending_section_injectq():
    bot = _bot(MagicMock())
    bot._set_pending_injectq("Show X", [(H1, "trk"), (H2, "trk2")], 500,
                             user_id="9")
    qtext, qrows = bot._pending_section()
    assert "Show X" in qtext
    assert "2 torrent(s)" in qtext
    assert qrows[0][0][1].startswith("inject:")
    assert qrows[0][1] == ("No", qrows[0][1][1])
    assert qrows[1] == [("Cancel", qrows[1][0][1])]


@pytest.mark.anyio
async def test_chat_hash_unknown_replies(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.source_client = AsyncMock()
        coord.source_client.list_torrents = AsyncMock(return_value=[])
        bot = _bot(store, coord=coord)
        await bot._handle_chat_message(
            SimpleNamespace(
                chat=SimpleNamespace(id="1"),
                from_user=SimpleNamespace(id="9"),
                message_id=51, text=f"/injectfuse {H1}",
                caption=None, document=None, reply_to_message=None))
        texts = _sent_texts(bot)
        assert any("not on VPS1" in t for t in texts)
        assert bot._pending_pick is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_hash_arms_question(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.source_client = AsyncMock()
        coord.source_client.list_torrents = AsyncMock(
            return_value=[_torrent(H1, TRACKER1)])
        coord.store = store
        bot = _bot(store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        await bot._handle_chat_message(
            SimpleNamespace(
                chat=SimpleNamespace(id="1"),
                from_user=SimpleNamespace(id="9"),
                message_id=51, text=f"/injectfuse {H1}",
                caption=None, document=None, reply_to_message=None))
        texts = _sent_texts(bot)
        assert any("to fuse seeding? " in t for t in texts)
        pend = bot._pending_live()
        assert pend is not None and pend.get("kind") == "injectq"
        assert [h for (h, _) in pend["members"]] == [H1]
    finally:
        store.close()


@pytest.mark.anyio
async def test_group_number_arms_question(tmp_path):
    """`/injectfuse_1` on a listed waiting group arms Yes/No, changes nothing."""
    coord, _, _fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        bot = _bot(coord.store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        msg = SimpleNamespace(
            chat=SimpleNamespace(id="1"),
            from_user=SimpleNamespace(id="9"),
            message_id=51, text="/injectfuse_1",
            caption=None, document=None, reply_to_message=None)
        await bot._handle_chat_message(msg)
        texts = _sent_texts(bot)
        assert any("to fuse seeding? " in t for t in texts)
        pend = bot._pending_live()
        assert pend is not None and pend.get("kind") == "injectq"
        assert [h for (h, _) in pend["members"]] == [H1]
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_underscore_hash_tracked_arms_question(tmp_path):
    """/injectfuse_<full-hash> on a tracked row resolves like the hash form."""
    coord, _, _fuse = _coord(tmp_path, [_torrent(H1, TRACKER1)])
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        bot = _bot(coord.store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        msg = SimpleNamespace(
            chat=SimpleNamespace(id="1"),
            from_user=SimpleNamespace(id="9"),
            message_id=51, text=f"/injectfuse_{H1}",
            caption=None, document=None, reply_to_message=None)
        await bot._handle_chat_message(msg)
        pend = bot._pending_live()
        assert pend is not None and pend.get("kind") == "injectq"
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


def _query(uid="9"):
    return SimpleNamespace(
        from_user=SimpleNamespace(id=uid),
        message=SimpleNamespace(message_id=60),
        data="",
    )


@pytest.mark.anyio
async def test_yes_executes_and_clears(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = MagicMock()
        coord.injectfuse_confirmed = AsyncMock(return_value="injecting 1")
        bot = _bot(store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        p = bot._set_pending_injectq("Show X", [(H1, "trk")], 500,
                                     user_id="9")
        q = _query()
        q.data = f"inject:{p['seq']}:yes"
        await bot._on_action_button(q, q.data)
        coord.injectfuse_confirmed.assert_awaited_once_with([H1])
        assert bot._pending_pick is None
        assert any("injecting 1" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_changes_nothing(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = MagicMock()
        coord.injectfuse_confirmed = AsyncMock()
        bot = _bot(store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        p = bot._set_pending_injectq("Show X", [(H1, "trk")], 500,
                                     user_id="9")
        q = _query()
        q.data = f"inject:{p['seq']}:no"
        await bot._on_action_button(q, q.data)
        coord.injectfuse_confirmed.assert_not_called()
        assert bot._pending_pick is None
        assert any("nothing changed" in t for t in _sent_texts(bot))
    finally:
        store.close()


@pytest.mark.anyio
async def test_foreign_tap_refused(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = MagicMock()
        coord.injectfuse_confirmed = AsyncMock()
        bot = _bot(store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        p = bot._set_pending_injectq("Show X", [(H1, "trk")], 500,
                                     user_id="9")
        q = _query(uid="10")
        q.data = f"inject:{p['seq']}:yes"
        await bot._on_action_button(q, q.data)
        coord.injectfuse_confirmed.assert_not_called()
        assert bot._pending_pick is not None
    finally:
        store.close()


@pytest.mark.anyio
async def test_dispatcher_routes_inject_callbacks(tmp_path):
    """_handle_callback must forward inject: taps (regression: silently dropped)."""
    from unittest.mock import AsyncMock

    store = StateStore(tmp_path / "state.db")
    try:
        bot = _bot(store, coord=MagicMock())
        bot._on_action_button = AsyncMock()
        q = SimpleNamespace(
            message=SimpleNamespace(chat=SimpleNamespace(id="1")),
            from_user=SimpleNamespace(id="9"),
            data="inject:deadbeef:yes",
            answer=AsyncMock(),
        )
        await bot._handle_callback(q)
        bot._on_action_button.assert_awaited_once()
        assert bot._on_action_button.await_args[0][1] == "inject:deadbeef:yes"
    finally:
        store.close()


@pytest.mark.anyio
async def test_group_question_names_live_vps1_count(tmp_path):
    """Later VPS1 additions join the question, not just the tracked row."""
    coord, _, _fuse = _coord(
        tmp_path, [_torrent(H1, TRACKER1), _torrent(H2, TRACKER2)])
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        bot = _bot(coord.store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        msg = SimpleNamespace(
            chat=SimpleNamespace(id="1"),
            from_user=SimpleNamespace(id="9"),
            message_id=51, text="/injectfuse_1",
            caption=None, document=None, reply_to_message=None)
        await bot._handle_chat_message(msg)
        texts = _sent_texts(bot)
        assert any("(2 copies)" in t for t in texts)
        pend = bot._pending_live()
        assert pend is not None and pend.get("kind") == "injectq"
        assert sorted(h for (h, _) in pend["members"]) == [H1, H2]
        assert coord.store.get(H1).state == State.WAITING_INDEXER
    finally:
        coord.store.close()


@pytest.mark.anyio
async def test_group_question_falls_back_when_vps1_gone(tmp_path):
    """VPS1 entry removed: question still arms from the tracked snapshot."""
    coord, _, _fuse = _coord(tmp_path, [])
    try:
        coord.store.upsert(TorrentState(
            source_infohash=H1, source_name=NAME, total_bytes=500,
            state=State.WAITING_INDEXER))
        bot = _bot(coord.store, coord=coord)
        bot._refresh_active_message = AsyncMock()
        msg = SimpleNamespace(
            chat=SimpleNamespace(id="1"),
            from_user=SimpleNamespace(id="9"),
            message_id=51, text="/injectfuse_1",
            caption=None, document=None, reply_to_message=None)
        await bot._handle_chat_message(msg)
        pend = bot._pending_live()
        assert pend is not None and pend.get("kind") == "injectq"
        assert [h for (h, _) in pend["members"]] == [H1]
    finally:
        coord.store.close()
