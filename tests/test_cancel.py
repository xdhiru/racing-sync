"""Cancel / ignore-list / full-reset / telegram-cancel coverage."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.state import State, StateStore, TorrentState

# ---------------------------------------------------------------------------
# ignore list (state.py)
# ---------------------------------------------------------------------------

def test_ignore_crud_and_find(tmp_path: Path):
    store = StateStore(tmp_path / "s.db")
    try:
        assert store.is_ignored("a" * 40) is False
        assert store.list_ignored() == []
        store.ignore_torrent("A" * 40, "Show.One")
        assert store.is_ignored("a" * 40) is True
        assert store.is_ignored("b" * 40) is False
        rows = store.list_ignored()
        assert len(rows) == 1 and rows[0]["source_name"] == "Show.One"
        assert store.find_ignored("a" * 40)["source_name"] == "Show.One"
        assert store.find_ignored("show.one")["source_name"] == "Show.One"
        assert store.unignore_torrent("a" * 40) is True
        assert store.is_ignored("a" * 40) is False
        assert store.unignore_torrent("a" * 40) is False
        with pytest.raises(LookupError):
            store.find_ignored("zzz")
        with pytest.raises(ValueError):
            store.ignore_torrent("  ")
    finally:
        store.close()


def test_group_is_ignored_strictness(tmp_path: Path):
    from racing_sync.coordinator import _group_is_ignored

    def _t(h):
        return Torrent(hash=h, name="n", category="", save_path="",
                       size_bytes=1, state="", progress=0.0)

    # Bare MagicMock stores must never veto (they answer truthy to all).
    assert _group_is_ignored([_t("a" * 40)], MagicMock()) is False
    assert _group_is_ignored([_t("a" * 40)], None) is False

    store = StateStore(tmp_path / "s.db")
    try:
        assert _group_is_ignored([_t("a" * 40)], store) is False
        store.ignore_torrent("a" * 40)
        assert _group_is_ignored([_t("a" * 40), _t("b" * 40)], store) is True
    finally:
        store.close()


# ---------------------------------------------------------------------------
# forget: blob cache + ignore flag
# ---------------------------------------------------------------------------

class _FakeDest:
    def __init__(self) -> None:
        self.entries: dict[str, dict] = {}

    async def list_torrents(self, *, category=None, hashes=None):
        wanted = {h.lower() for h in hashes} if hashes is not None else None
        return [Torrent(hash=h, name="n", save_path=e["save_path"],
                        category="racing", size_bytes=10,
                        state="downloading", progress=0.5)
                for h, e in self.entries.items()
                if wanted is None or h in wanted]

    async def get_torrent(self, h: str):
        h = h.lower()
        if h not in self.entries:
            return None
        e = self.entries[h]
        return Torrent(hash=h, name="n", save_path=e["save_path"],
                       category="racing", size_bytes=10,
                       state="downloading", progress=0.5)

    async def get_torrent_files(self, h: str):
        return list(self.entries[h.lower()]["files"])

    async def delete(self, h: str, *, delete_files: bool = False):
        self.entries.pop(h.lower(), None)


def _forget_cfg(tmp: Path) -> MagicMock:
    cfg = MagicMock()
    cfg.ssd.path = tmp / "ssd"
    cfg.dest.save_path = tmp / "ssd"
    cfg.general.state_db = tmp / "state.db"
    return cfg


@pytest.mark.anyio
async def test_forget_removes_blob_cache(tmp_path: Path):
    from racing_sync.forget import forget_torrent

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    blob_dir = tmp_path / "watch_cross_seeds" / ("a" * 40)
    blob_dir.mkdir(parents=True)
    (blob_dir / ("a" * 40 + ".torrent")).write_bytes(b"d8:announce1:xee")
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    dest = _FakeDest()
    dest.entries["a" * 40] = {"save_path": str(ssd), "files": []}

    # Dry-run plans the blob dir but touches nothing.
    dry = await forget_torrent(
        _forget_cfg(tmp_path), dest=dest, store=store, target="a" * 40,
        apply=False, delete_files=True,
    )
    assert str(blob_dir) in dry["local_paths"]
    assert blob_dir.exists()

    result = await forget_torrent(
        _forget_cfg(tmp_path), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True,
    )
    assert result["errors"] == []
    assert not blob_dir.exists()
    assert store.get("a" * 40) is None


@pytest.mark.anyio
async def test_forget_ignore_records_and_blocks_rediscovery(tmp_path: Path):
    from racing_sync.coordinator import _group_is_ignored
    from racing_sync.forget import forget_torrent

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    dest = _FakeDest()

    result = await forget_torrent(
        _forget_cfg(tmp_path), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True, ignore=True,
    )
    assert result["errors"] == []
    assert result["ignored"] is True
    assert store.is_ignored("a" * 40) is True
    group = [Torrent(hash="a" * 40, name="Show", category="", save_path="",
                     size_bytes=10, state="racing", progress=1.0)]
    assert _group_is_ignored(group, store) is True


# ---------------------------------------------------------------------------
# recovery + re-inject + late-seed skip cancelled releases
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_recovery_skips_ignored_unadopted(tmp_path: Path):
    from racing_sync.recovery import reconcile

    cfg = MagicMock()
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse"
    cfg.dest.save_path = tmp_path / "ssd"
    cfg.ssd.path = tmp_path / "ssd"
    store = StateStore(tmp_path / "state.db")
    store.ignore_torrent("b" * 40, "Ignored.Show")
    dest = AsyncMock()
    dest.list_torrents = AsyncMock(return_value=[
        Torrent(hash="b" * 40, name="Ignored.Show", category="racing",
                save_path=str(tmp_path / "fuse"), size_bytes=10,
                state="seeding", progress=1.0),
    ])
    try:
        rpt = await reconcile(cfg, dest=dest, store=store)
        assert store.all() == []
        assert rpt.adopted == [] and rpt.unknowns == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_reinject_and_late_seed_skip_ignored(tmp_path: Path):

    store = StateStore(tmp_path / "state.db")
    try:
        store.ignore_torrent("c" * 40, "Ignored.Show")
        coord = make_coordinator()
        coord.store = store
        coord.dest_client = AsyncMock()
        coord.cfg = MagicMock()
        group = [Torrent(hash="c" * 40, name="Ignored.Show", category="",
                         save_path="", size_bytes=10, state="racing", progress=1.0)]
        ts = TorrentState(source_infohash="d" * 40, source_name="Other",
                          injected_private_hashes="", state=State.DONE)
        # Late-seed: ignored arrivals never injected.
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"x")
        await coord._check_and_inject_late_cross_seeds(ts, group)
        coord.dest_client.add_torrent.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_discovery_skips_ignored_group(tmp_path: Path):
    """An ignored hash on VPS1 produces no NEW row (no zombie pipeline)."""

    store = StateStore(tmp_path / "state.db")
    try:
        store.ignore_torrent("e" * 40, "Ignored.Show")
        coord = make_coordinator()
        coord.cfg = MagicMock()
        coord.cfg.source.category = ""
        coord.cfg.source.min_age_seconds = 0
        coord.cfg.cleanup.enabled = False
        coord.cfg.max_active_downloads = 3
        coord.cfg.max_concurrent_moves = 3
        coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
        coord.store = store
        coord.watch = None
        src = MagicMock()
        src.list_torrents = AsyncMock(return_value=[
            Torrent(hash="e" * 40, name="Ignored.Show", category="",
                    save_path="", size_bytes=10, state="seeding", progress=1.0,
                    trackers=["https://alpha.cc/announce/x"], added_on=0),
        ])
        coord.source_client = src
        coord._tasks = set()
        coord._running_infohashes = set()
        coord._stop = False
        coord._live = {}
        coord._source_torrents_cache = []
        coord._source_torrents_cached_at = 0.0
        await coord._tick_inner()
        assert store.all() == []
    finally:
        store.close()


# ---------------------------------------------------------------------------
# CLI: forget --ignore, unignore, run --full
# ---------------------------------------------------------------------------

_MINIMAL = """
[source]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8081"
save_path = "{ssd}"

[ssd]
path = "{ssd}"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[rclone.remote]
default = "remote:movies/"
unsorted = "remote:unsorted/"

[rclone.fuse]
mount = "{fuse}"
mount_unsorted = "{fuse}"
"""


def _cli_config(tmp_path: Path) -> Path:
    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    fuse = tmp_path / "fuse"
    fuse.mkdir(exist_ok=True)
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        "[general]\n"
        f'state_db = "{(tmp_path / "state.db").as_posix()}"\n'
        f'log_dir = "{(tmp_path / "logs").as_posix()}"\n'
        + _MINIMAL.format(ssd=ssd.as_posix(), fuse=fuse.as_posix())
    )
    return cfg_file


def test_forget_cli_ignore_flag(tmp_path: Path, monkeypatch):
    from racing_sync.__main__ import main

    cfg_file = _cli_config(tmp_path)
    (tmp_path / "ssd" / "Show").mkdir()
    from racing_sync.state import StateStore as _Store

    store = _Store(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="f" * 40, source_name="Show",
                              save_path=str(tmp_path / "ssd"), state=State.MOVING))
    store.close()

    class _Dest:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            pass

        async def close(self):
            pass

        async def list_torrents(self, *, category=None, hashes=None):
            return []

        async def get_torrent_files(self, h):
            return []

        async def get_torrent(self, h):
            return None

        async def delete(self, h, *, delete_files=False):
            pass

    monkeypatch.setattr("racing_sync.clients.qbittorrent.QBittorrentClient", _Dest)
    rc = main(["forget", "--config", str(cfg_file), "f" * 40, "--apply", "--ignore"])
    assert rc == 0
    store = _Store(tmp_path / "state.db")
    try:
        assert store.get("f" * 40) is None
        assert store.is_ignored("f" * 40) is True
    finally:
        store.close()


def test_unignore_cli(tmp_path: Path, capsys):
    from racing_sync.__main__ import main
    from racing_sync.state import StateStore as _Store

    cfg_file = _cli_config(tmp_path)
    store = _Store(tmp_path / "state.db")
    store.ignore_torrent("a" * 40, "Show.One")
    store.ignore_torrent("b" * 40, "Show.Two")
    store.close()

    assert main(["unignore", "--config", str(cfg_file), "--list"]) == 0
    out = capsys.readouterr().out
    assert "Show.One" in out and "Show.Two" in out

    assert main(["unignore", "--config", str(cfg_file), "show.one"]) == 0
    store = _Store(tmp_path / "state.db")
    try:
        assert store.is_ignored("a" * 40) is False
        assert store.is_ignored("b" * 40) is True
    finally:
        store.close()

    assert main(["unignore", "--config", str(cfg_file), "nope"]) == 1


def test_unignore_lifts_tombstone_so_redrop_reprocesses(tmp_path: Path, capsys):
    """Single unignore clears the ignore entry AND the forget tombstone."""
    from racing_sync.__main__ import main
    from racing_sync.state import StateStore as _Store

    cfg_file = _cli_config(tmp_path)
    store = _Store(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="c" * 40, source_name="Kept.Show",
                              save_path=str(tmp_path / "ssd"), state=State.MOVING))
    assert store.tombstone("c" * 40) is True
    store.ignore_torrent("c" * 40, "Kept.Show")
    store.close()

    assert main(["unignore", "--config", str(cfg_file), "kept.show"]) == 0
    out = capsys.readouterr().out
    assert "tombstone lifted" in out

    store = _Store(tmp_path / "state.db")
    try:
        assert store.is_ignored("c" * 40) is False
        # Re-dropped file re-ingests: the write lands instead of dropping.
        store.upsert(TorrentState(source_infohash="c" * 40, source_name="Kept.Show",
                                  save_path=str(tmp_path / "ssd"), state=State.NEW))
        assert store.get("c" * 40) is not None
    finally:
        store.close()


def test_unignore_all_clears_every_entry_and_tombstone(tmp_path: Path, capsys):
    from racing_sync.__main__ import main
    from racing_sync.state import StateStore as _Store

    cfg_file = _cli_config(tmp_path)
    store = _Store(tmp_path / "state.db")
    for h, name in (("a" * 40, "Show.One"), ("b" * 40, "Show.Two")):
        store.upsert(TorrentState(source_infohash=h, source_name=name,
                                  save_path=str(tmp_path / "ssd"), state=State.MOVING))
        assert store.tombstone(h) is True
        store.ignore_torrent(h, name)
    store.close()

    assert main(["unignore", "--config", str(cfg_file), "--all"]) == 0
    out = capsys.readouterr().out
    assert "unignored 2 release(s)" in out

    store = _Store(tmp_path / "state.db")
    try:
        assert store.list_ignored() == []
        for h in ("a" * 40, "b" * 40):
            store.upsert(TorrentState(source_infohash=h, source_name="X",
                                      save_path=str(tmp_path / "ssd"), state=State.NEW))
            assert store.get(h) is not None
    finally:
        store.close()

    assert main(["unignore", "--config", str(cfg_file), "--all"]) == 0
    assert "ignore list is empty" in capsys.readouterr().out


def test_full_reset_clears_client_and_blobs(tmp_path: Path, monkeypatch, capsys):
    from racing_sync.__main__ import _do_full_reset

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    (ssd / "Pack").mkdir()
    (ssd / "Pack" / "a.mkv").write_bytes(b"x")
    blob_dir = tmp_path / "watch_cross_seeds" / ("a" * 40)
    blob_dir.mkdir(parents=True)
    (blob_dir / "x.torrent").write_bytes(b"d8:announce1:xee")

    cfg = MagicMock()
    cfg.general.state_db = tmp_path / "state.db"
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd

    deleted = []

    class _Dest:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            pass

        async def close(self):
            pass

        async def list_torrents(self, *, category=None):
            assert category == "racing"
            return [Torrent(hash="a" * 40, name="Pack", category="racing",
                            save_path=str(ssd), size_bytes=10,
                            state="seeding", progress=1.0)]

        async def delete(self, h, *, delete_files=False):
            deleted.append((h, delete_files))

    monkeypatch.setattr("racing_sync.clients.qbittorrent.QBittorrentClient", _Dest)
    import asyncio

    lines = asyncio.run(_do_full_reset(cfg))
    assert deleted == [("a" * 40, True)]
    assert not blob_dir.exists()
    assert any("deleted dest entry" in ln for ln in lines)
    # SSD orphans are wiped but the SSD root itself survives.
    assert ssd.is_dir()
    assert not (ssd / "Pack").exists()
    assert any("SSD data" in ln for ln in lines)


# ---------------------------------------------------------------------------
# telegram cancel flow (copy-paste `/cancel_` commands, no buttons/confirm)
# ---------------------------------------------------------------------------

def _bot(**kw):
    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = MagicMock()
    bot._cfg.chat_id = "1"
    bot._cfg.page_size = 5
    bot._callback_times = {}
    bot._store = MagicMock()
    bot._current_page = 0
    bot._refresh_active_message = AsyncMock()
    bot._bot = MagicMock()
    bot._bot.send_message = AsyncMock(return_value=MagicMock(message_id=7))
    for k, v in kw.items():
        setattr(bot, k, v)
    return bot


def _message(chat="1", user="9", text="/cancel_aaaaaaaaaa"):
    m = MagicMock()
    m.chat.id = chat
    m.from_user.id = user
    m.text = text
    m.caption = None
    m.message_id = 42
    return m


def _query(data, user="9", chat="1"):
    q = MagicMock()
    q.data = data
    q.message.chat.id = chat
    q.from_user.id = user
    q.answer = AsyncMock()
    q.message.message_id = 43
    return q


@pytest.mark.anyio
async def test_admin_allowlist_refuses_stranger_command(tmp_path: Path):
    """With admin_user_ids set, other chat members cannot start flows."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._store = store
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5,
                               admin_user_ids=[4242])
    try:
        store.upsert(TorrentState(source_infohash="a" * 40,
                                  source_name="Show", state=State.MOVING))
        await bot._handle_chat_message(_message(user="9", text="/cancel_1"))
        sent = [c.args[1] for c in
                bot._bot.send_message.await_args_list]
        assert any("Not authorized" in str(t) for t in sent)
        # Listed admin proceeds normally (remember question, nothing deleted).
        bot._bot.send_message.reset_mock()
        bot._callback_times.clear()
        await bot._handle_chat_message(
            _message(user="4242", text="/cancel_1"))
        assert store.get("a" * 40) is not None
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"forget:{'a' * 40}:1" in _data
    finally:
        store.close()


@pytest.mark.anyio
async def test_stranger_tap_refused_by_allowlist(tmp_path: Path):
    """Per-tap admin auth replaces picker owner-binding: strangers refused."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._store = store
    bot._cfg = SimpleNamespace(chat_id="1", page_size=5,
                               admin_user_ids=[4242])
    try:
        store.upsert(TorrentState(source_infohash="a" * 40,
                                  source_name="Show", state=State.MOVING))
        store.upsert(TorrentState(source_infohash="b" * 40,
                                  source_name="Show", state=State.QUEUED))
        # Stranger tap refused before any routing.
        q = _query(f"cancel:{'a' * 40}", user="777")
        await bot._handle_callback(q)
        q.answer.assert_awaited_with("Not authorized for destructive actions",
                                     show_alert=True)
        assert store.get("a" * 40) is not None
        # Listed admin's tap opens the keep question.
        bot._callback_times.clear()
        q2 = _query(f"cancel:{'a' * 40}", user="4242")
        await bot._handle_callback(q2)
        assert store.get("a" * 40) is not None
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_torrent_forgets_and_ignores(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    try:
        msg = await bot._execute_cancel_one("a" * 40, delete_files=True)
        assert msg.startswith("Cancelled")
        assert store.get("a" * 40) is None
        assert store.is_ignored("a" * 40) is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_torrent_auto_cancels_waiting_pairs(tmp_path: Path):
    """Cancelling the SSD owner reports its deferred watch pairs."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Shared.Show",
                              save_path=str(ssd), state=State.DOWNLOADING,
                              cross_seed_source="watch-dir",
                              source_announce_url="https://alpha.cc/announce/xyz",
                              total_bytes=1000))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Shared.Show",
                              state=State.NEW, cross_seed_source="watch-dir",
                              source_announce_url="https://alpha.cc/announce/xyz",
                              total_bytes=1000))
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    try:
        msg = await bot._execute_cancel_one("a" * 40, delete_files=True)
        assert msg.startswith("Cancelled")
        assert "+ 1 waiting pair" in msg
        assert "Shared.Show" in msg
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None
        assert store.is_ignored("b" * 40) is True
    finally:
        store.close()


def test_render_active_has_cancel_command_per_task():
    from racing_sync.telegram_bot import render_active

    ts = TorrentState(source_infohash="a" * 40, source_name="Show",
                      state=State.DOWNLOADING, total_bytes=1000)
    ts2 = TorrentState(source_infohash="b" * 40, source_name="Show2",
                       state=State.QUEUED, total_bytes=2000)
    text, _, _ = render_active([(ts, 0.5), (ts2, None)], page=0, page_size=5)
    # One positional action entry per group (escaped underscore, no labels).
    assert "/act\\_1" in text
    assert "/act\\_2" in text
    assert "/cancel\\_1" not in text
    assert "/cancel\\_aaaaaaaaaa" not in text
    assert "Cancel:" not in text
    # No inline-button artefacts in the text itself.
    assert "✅" not in text


def test_resolve_cancel_target(tmp_path: Path):
    bot = _bot()
    store = StateStore(tmp_path / "state.db")
    bot._store = store
    try:
        store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show.One",
                                  state=State.MOVING))
        store.upsert(TorrentState(source_infohash="a" * 39 + "b", source_name="Show.Two",
                                  state=State.QUEUED))
        store.upsert(TorrentState(source_infohash="b" * 40, source_name="Show.Three",
                                  state=State.QUEUED))
        # Full hash resolves.
        assert bot._resolve_cancel_target("a" * 40).source_name == "Show.One"
        assert bot._resolve_cancel_target("b" * 40).source_name == "Show.Three"
        # Unique short prefix resolves.
        assert bot._resolve_cancel_target("b" * 10).source_name == "Show.Three"
        # Ambiguous prefix raises (minimum 4 chars for a prefix scan).
        with pytest.raises(LookupError, match="matches 2"):
            bot._resolve_cancel_target("a" * 4)
        # Unknown prefix raises.
        with pytest.raises(LookupError, match="no tracked torrent"):
            bot._resolve_cancel_target("d" * 10)
        # Non-hex raises.
        with pytest.raises(LookupError):
            bot._resolve_cancel_target("zzz")
    finally:
        store.close()


def test_resolve_cancel_target_ignores_done_history(tmp_path: Path):
    # A DONE row sharing the live prefix must not force ambiguity, and a
    # full hash still addresses the DONE row (e.g. to stop seeding it).
    bot = _bot()
    store = StateStore(tmp_path / "state.db")
    bot._store = store
    try:
        store.upsert(TorrentState(source_infohash="a" * 10 + "0" * 30,
                                  source_name="Old.Done", state=State.DONE))
        store.upsert(TorrentState(source_infohash="a" * 10 + "1" * 30,
                                  source_name="Live.One", state=State.QUEUED))
        assert bot._resolve_cancel_target("a" * 10).source_name == "Live.One"
        assert bot._resolve_cancel_target("a" * 10 + "0" * 30).source_name == "Old.Done"
        # Two live rows on one prefix still refuse.
        store.upsert(TorrentState(source_infohash="a" * 10 + "2" * 30,
                                  source_name="Live.Two", state=State.QUEUED))
        with pytest.raises(LookupError, match="matches 2"):
            bot._resolve_cancel_target("a" * 10)
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_cancel_arms_keep_question(tmp_path: Path):
    """Sending `/cancel_<short>` asks remember-first — nothing deleted yet."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/cancel_aaaaaaaaaa"))
        # Row untouched; remember question asked with hash buttons instead.
        assert store.get("a" * 40) is not None
        assert store.is_ignored("a" * 40) is False
        bot._bot.send_message.assert_awaited_once()
        sent_text = bot._bot.send_message.call_args[0][1]
        assert sent_text.startswith("Forget Show?")
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"forget:{'a' * 40}:1" in _data
        assert f"forget:{'a' * 40}:0" in _data
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_cancel_unknown_and_unauthorized(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._store = store
    try:
        # Unknown hash replies instead of cancelling.
        await bot._handle_chat_message(_message(text="/cancel_dddddddddd"))
        bot._bot.send_message.assert_awaited_once()
        # Non-command text is ignored silently.
        bot._bot.send_message.reset_mock()
        await bot._handle_chat_message(_message(text="hello there"))
        bot._bot.send_message.assert_not_called()
        # Wrong chat is ignored silently.
        await bot._handle_chat_message(_message(chat="999", user="999",
                                                text="/cancel_aaaaaaaaaa"))
        bot._bot.send_message.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_cancel_supports_full_hash_and_suffix(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="c" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    try:
        await bot._handle_chat_message(
            _message(text=f"/cancel_{'c' * 40}@mybot"))
        # Full hash also lands on the remember question (never instant).
        assert store.get("c" * 40) is not None
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"forget:{'c' * 40}:1" in _data
        assert f"forget:{'c' * 40}:0" in _data
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_torrent_releases_ssd_budget(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              save_path=str(ssd), state=State.MOVING))
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    coord._ssd_release = AsyncMock()
    bot = _bot()
    bot._coord = coord
    bot._store = store
    try:
        msg = await bot._execute_cancel_one("a" * 40, delete_files=True)
        assert msg.startswith("Cancelled")
        coord._ssd_release.assert_awaited_once_with("a" * 40)
    finally:
        store.close()


def test_render_active_shows_fetch_only_for_waiting_indexer():
    from racing_sync.telegram_bot import render_active

    waiting = TorrentState(source_infohash="a" * 40, source_name="Show",
                           state=State.WAITING_INDEXER, total_bytes=1000,
                           indexer_attempts=9)
    downloading = TorrentState(source_infohash="b" * 40, source_name="Show2",
                               state=State.DOWNLOADING, total_bytes=2000)
    text, _, _ = render_active([(waiting, None), (downloading, 0.5)],
                               page=0, page_size=5)
    assert "/act\\_1" in text
    assert "/act\\_2" in text
    assert "Fetch original:" not in text
    assert "/cancel\\_1" not in text
    assert "/now\\_aaaaaaaaaa" not in text


def test_render_detail_shows_fetch_hint_for_waiting_indexer():
    from racing_sync.telegram_bot import render_detail

    waiting = TorrentState(source_infohash="a" * 40, source_name="Show",
                           state=State.WAITING_INDEXER, total_bytes=1000,
                           indexer_attempts=9)
    detail = render_detail(waiting)
    # Full hash untouched in the detail card; now hint added.
    assert "`" + "a" * 40 + "`" in detail
    assert "`/now_aaaaaaaaaa`" in detail

    downloading = TorrentState(source_infohash="b" * 40, source_name="Show2",
                               state=State.DOWNLOADING, total_bytes=2000)
    assert "/now_" not in render_detail(downloading)


def test_resolve_fetch_target_requires_waiting(tmp_path: Path):
    bot = _bot()
    store = StateStore(tmp_path / "state.db")
    bot._store = store
    try:
        store.upsert(TorrentState(source_infohash="a" * 40, source_name="Waiting.One",
                                  state=State.WAITING_INDEXER))
        store.upsert(TorrentState(source_infohash="b" * 40, source_name="Busy.One",
                                  state=State.DOWNLOADING))
        assert bot._resolve_fetch_target("a" * 10).source_name == "Waiting.One"
        assert bot._resolve_fetch_target("a" * 40).source_name == "Waiting.One"
        # Full hash of a non-waiting row explains itself.
        with pytest.raises(LookupError, match="not waiting"):
            bot._resolve_fetch_target("b" * 40)
        # Unknown prefix raises.
        with pytest.raises(LookupError, match="no tracked torrent"):
            bot._resolve_fetch_target("d" * 10)
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_fetch_flags_and_wakes_row(tmp_path: Path):
    """Sending `/fetch_<short>` sets force_direct and marks the timer due."""
    import datetime as dt

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              state=State.WAITING_INDEXER, indexer_attempts=9))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/fetch_aaaaaaaaaa"))
        row = store.get("a" * 40)
        assert row.force_direct == 1
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_next_retry_at is not None
        assert row.indexer_next_retry_at <= dt.datetime.now(dt.timezone.utc)
        bot._bot.send_message.assert_awaited_once()
        sent_text = bot._bot.send_message.call_args[0][1]
        assert sent_text.startswith("Fetching original for")
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_fetch_opens_fresh_direct_window(tmp_path: Path):
    """Manual fetch restarts the retry clock, not just the flag."""
    import datetime as dt

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash="e" * 40, source_name="Old",
        state=State.WAITING_INDEXER, indexer_attempts=40,
        indexer_first_queried_at=dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(hours=23),
    ))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/fetch_eeeeeeeeee"))
        row = store.get("e" * 40)
        assert row.force_direct == 1
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_attempts == 0
        fresh = (dt.datetime.now(dt.timezone.utc)
                 - row.indexer_first_queried_at).total_seconds()
        assert fresh < 60
    finally:
        store.close()


@pytest.mark.anyio
async def test_group_now_singleton_executes_immediately(tmp_path: Path):
    """One eligible copy starts at once (no pick step for singletons)."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Lone.Show",
                              state=State.WAITING_INDEXER, indexer_attempts=2,
                              total_bytes=1000))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/fetch_1"))
        row = store.get("a" * 40)
        assert row.force_direct == 1
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Fetching original for Lone.Show")
    finally:
        store.close()


@pytest.mark.anyio
async def test_group_now_multi_offers_per_copy_start_buttons(tmp_path: Path):
    """Several eligible copies each get their own Start button (by tracker)."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Twin.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER,
                              source_announce_url="https://alpha.cc/announce"))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Twin.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER,
                              source_announce_url="https://bte.example/announce"))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/now_1"))
        assert store.get("a" * 40).force_direct == 0  # nothing started yet
        _kwargs = bot._bot.send_message.call_args[1]
        _labels = [b.text for r in
                   _kwargs["reply_markup"].inline_keyboard for b in r]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert any(t.startswith("▶ Start ") for t in _labels)
        assert f"go:{'a' * 40}" in _data
        assert f"go:{'b' * 40}" in _data
    finally:
        store.close()


def test_keepq_text_shows_size_or_omits():
    from racing_sync.telegram_bot import TelegramBot

    bot = TelegramBot.__new__(TelegramBot)
    _text, _rows = bot._keepq_text_and_rows("Big.Show", " · all 2 copies",
                                            "all:" + "a" * 40, 2_600_000_000,
                                            remember=True)
    assert "Big.Show" in _text and "all 2 copies" in _text
    assert "2.4G" in _text
    _pairs = [t for r in _rows for t in r]
    _flat = [d for (_, d) in _pairs]
    assert f"keep:all:{'a' * 40}:1:yes" in _flat
    assert f"keep:all:{'a' * 40}:1:no" in _flat
    assert ("Close", "abort") in _pairs
    _text0, _rows0 = bot._keepq_text_and_rows("Big.Show", "", "a" * 40,
                                              remember=False)
    _flat0 = [d for r in _rows0 for (_, d) in r]
    assert f"keep:{'a' * 40}:0:yes" in _flat0
    assert f"keep:{'a' * 40}:0:no" in _flat0
    assert "allowed back later" in _text0
    assert [t for (t, _) in _pairs] == [
        "✔ Keep files", "✖ Delete files", "Close"]
    # Rows without size still render (no dangling separator).
    _text2, _ = bot._keepq_text_and_rows("Big.Show", "", "a" * 40)
    assert "Big.Show" in _text2 and "·" not in _text2.split("—")[0]
    assert "data stays in place" in _text2 and "wipes" in _text2


def test_sheet_buttons_fit_telegram_callback_budget():
    """Every hash-protocol callback shape fits 64 bytes and parses."""
    from racing_sync.telegram_bot import _parse_action_data

    _h = "a" * 40
    for _data in (f"go:{_h}", f"cancel:{_h}",
                  f"cancel:all:{_h}", f"forget:{_h}:1",
                  f"forget:all:{_h}:0", f"keep:{_h}:1:yes",
                  f"keep:all:{_h}:0:no", f"inject:{_h}",
                  f"inject:{_h}:yes", f"skip:all:{_h}", f"resume:{_h}",
                  f"resume:all:{_h}", "abort"):
        assert len(_data) <= 64, _data
        assert _parse_action_data(_data)[0] in (
            "go", "cancel", "forget", "keep", "inject", "skip",
            "resume", "abort")
    # Remember choice travels in keep buttons (legacy 3-part = remember).
    assert _parse_action_data(f"keep:{_h}:1:yes") == (
        "keep", False, _h, "yes", True)
    assert _parse_action_data(f"keep:{_h}:0:no") == (
        "keep", False, _h, "no", False)
    assert _parse_action_data(f"keep:{_h}:yes") == (
        "keep", False, _h, "yes", True)
    assert _parse_action_data(f"forget:{_h}:0") == (
        "forget", False, _h, "", False)
    assert _parse_action_data("pick:7:1") == ("", False, "", "", None)
    assert _parse_action_data("keep:9:yes") == ("", False, "", "", None)
    assert _parse_action_data("abort:7") == ("", False, "", "", None)


def test_keepq_text_sanitizes_backtick_title():
    from racing_sync.telegram_bot import TelegramBot

    bot = TelegramBot.__new__(TelegramBot)
    _text, _rows = bot._keepq_text_and_rows("Evil` — `x", "", "a" * 40)
    assert "`" not in _text.replace("`Evil' — 'x`", "")  # span stays closed
    assert "Evil' — 'x" in _text


@pytest.mark.anyio
async def test_chat_message_fetch_rejects_non_waiting(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Busy",
                              state=State.DOWNLOADING))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/fetch_bbbbbbbbbb"))
        row = store.get("b" * 40)
        assert row.force_direct == 0
        assert row.state == State.DOWNLOADING
        bot._bot.send_message.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_fetch_torrent_marks_stale_row(tmp_path: Path):
    """Direct executor call on a row that already moved on explains itself."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="c" * 40, source_name="Moved",
                              state=State.QUEUED))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        msg = await bot._fetch_torrent("c" * 40)
        assert "nothing to fetch" in msg
        assert store.get("c" * 40).force_direct == 0
    finally:
        store.close()


def _grace_coord_stub(*, watch: bool):
    """Coordinator double: grace-held NEW rows, watch-or-racing origin."""
    coord = SimpleNamespace(
        _watch_wait_note=lambda row: "Waiting for preferred copy · 100s left",
        _is_watch_row=lambda row: watch,
        _spawn_worker=MagicMock(),
    )
    return coord


@pytest.mark.anyio
async def test_fetch_torrent_new_watch_grace_uses_dropped(tmp_path: Path):
    """NEW grace-held watch drop: force_direct set, stays NEW, woken."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash="f" * 40, source_name="Drop.One",
        state=State.NEW, cross_seed_source="watch-dir"))
    bot = _bot()
    bot._store = store
    bot._coord = _grace_coord_stub(watch=True)
    try:
        msg = await bot._fetch_torrent("f" * 40)
        row = store.get("f" * 40)
        assert row.force_direct == 1
        assert row.state == State.NEW
        assert "dropped .torrent directly" in msg
        bot._coord._spawn_worker.assert_called_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_fetch_torrent_new_racing_grace_uses_vps1(tmp_path: Path):
    """NEW grace-held racing row: force_direct set, stays NEW, woken."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash="e" * 40, source_name="Race.One",
        state=State.NEW, cross_seed_source="prowlarr"))
    bot = _bot()
    bot._store = store
    bot._coord = _grace_coord_stub(watch=False)
    try:
        msg = await bot._fetch_torrent("e" * 40)
        row = store.get("e" * 40)
        assert row.force_direct == 1
        assert row.state == State.NEW
        assert "VPS1 original directly" in msg
        bot._coord._spawn_worker.assert_called_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_fetch_torrent_new_without_grace_refuses(tmp_path: Path):
    """NEW rows with no grace note have nothing to fetch."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash="d" * 40, source_name="Fresh.One",
        state=State.NEW, cross_seed_source="watch-dir"))
    bot = _bot()
    bot._store = store
    bot._coord = SimpleNamespace(
        _watch_wait_note=lambda row: "",
        _is_watch_row=lambda row: True,
        _spawn_worker=MagicMock(),
    )
    try:
        msg = await bot._fetch_torrent("d" * 40)
        assert "nothing to fetch" in msg
        assert store.get("d" * 40).force_direct == 0
        bot._coord._spawn_worker.assert_not_called()
    finally:
        store.close()


def test_now_eligibility_covers_waiting_and_new_grace():
    """Group now covers WAITING_INDEXER + NEW grace-held members."""
    from racing_sync.telegram_bot import _now_eligible_hashes

    mk = lambda h, st, dom: TorrentState(  # noqa: E731
        source_infohash=h, source_name="Show.X", state=st,
        total_bytes=1000, source_announce_url=f"https://{dom}/announce")
    waiting = mk("a" * 40, State.WAITING_INDEXER, "alpha.cc")
    grace = mk("b" * 40, State.NEW, "bte.example")
    busy = mk("c" * 40, State.DOWNLOADING, "gamma.cc")
    notes = {"b" * 40: "Waiting for preferred copy · 90s left"}
    members = [waiting, grace, busy]
    assert _now_eligible_hashes(members, notes) == [
        "a" * 40, "b" * 40]


def test_resolve_fetch_target_allows_new_grace(tmp_path: Path):
    """Prefix resolution accepts grace-held NEW rows, rejects others."""
    bot = _bot()
    store = StateStore(tmp_path / "state.db")
    bot._store = store
    bot._coord = _grace_coord_stub(watch=True)
    try:
        store.upsert(TorrentState(source_infohash="f" * 40, source_name="Drop",
                                  state=State.NEW,
                                  cross_seed_source="watch-dir"))
        store.upsert(TorrentState(source_infohash="b" * 40, source_name="Busy",
                                  state=State.DOWNLOADING))
        assert bot._resolve_fetch_target("f" * 10).source_name == "Drop"
        with pytest.raises(LookupError, match="not waiting"):
            bot._resolve_fetch_target("b" * 40)
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_skip_and_unskip_full_hash(tmp_path: Path):
    """/skip_<hash> holds the row; /unskip_<hash> resumes + wakes a worker."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Show",
                              state=State.WAITING_INDEXER))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/skip_" + "a" * 40))
        assert store.get("a" * 40).skipped == 1
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Skipped")

        bot._callback_times.clear()
        await bot._handle_chat_message(_message(text="/unskip_" + "a" * 40))
        assert store.get("a" * 40).skipped == 0
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Resumed")
        bot._coord._spawn_worker.assert_called_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_resume_group_unholds_and_lifts(tmp_path: Path):
    """/resume_1 resumes held copies and lifts their ignore entries."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.NEW, skipped=1))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER,
                              skipped=1))
    store.ignore_torrent("b" * 40, "Same.Show")
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/resume_1"))
        assert store.get("a" * 40).skipped == 0
        assert store.get("b" * 40).skipped == 0
        assert store.is_ignored("b" * 40) is False
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Resumed")
        assert "Unignored" in sent
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_resume_ignored_only_hash(tmp_path: Path):
    """/resume_<hash> on an ignored-only hash lifts it (unignore parity)."""
    store = StateStore(tmp_path / "state.db")
    store.ignore_torrent("c" * 40, "Gone")
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/resume_" + "c" * 40))
        assert store.is_ignored("c" * 40) is False
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Unignored")
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_skip_group_number(tmp_path: Path):
    """/skip_1 holds every copy in the group at once."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.NEW))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/skip_1"))
        assert store.get("a" * 40).skipped == 1
        assert store.get("b" * 40).skipped == 1
        sent = bot._bot.send_message.call_args[0][1]
        assert "2 copies" in sent
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_skip_single_hash_holds_whole_group(tmp_path: Path):
    """A single hash still holds the release: every grouped copy."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.NEW))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Same.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/skip_" + "a" * 40))
        assert store.get("a" * 40).skipped == 1
        assert store.get("b" * 40).skipped == 1
        sent = bot._bot.send_message.call_args[0][1]
        assert "2 copies" in sent
    finally:
        store.close()


@pytest.mark.anyio
async def test_fetch_torrent_refuses_held_row(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="d" * 40, source_name="Held",
                              state=State.WAITING_INDEXER, skipped=1))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        msg = await bot._fetch_torrent("d" * 40)
        assert "skipped" in msg
        assert store.get("d" * 40).force_direct == 0
    finally:
        store.close()


def test_render_active_shows_skip_and_unskip():
    """Held rows get /resume_N + ⏭; running rows get /skip_N."""
    from racing_sync.telegram_bot import render_active

    held = TorrentState(source_infohash="a" * 40, source_name="Held",
                        state=State.WAITING_INDEXER, total_bytes=1000,
                        skipped=1)
    text, _, _ = render_active([(held, None)], page=0, page_size=5)
    assert "⏭ Skipped" in text
    assert "/act\\_1" in text
    assert "/skip\\_1" not in text
    assert "/now\\_1" not in text

    free = TorrentState(source_infohash="b" * 40, source_name="Free",
                        state=State.WAITING_INDEXER, total_bytes=1000)
    text, _, _ = render_active([(free, None)], page=0, page_size=5)
    assert "/act\\_1" in text
    assert "/resume\\_1" not in text


def test_member_labels_include_held_but_eligibility_skips_them():
    """Held members get cancel buttons but never start-now buttons."""
    from racing_sync.telegram_bot import _member_button_labels, _now_eligible_hashes

    mk = lambda h, st: TorrentState(  # noqa: E731
        source_infohash=h, source_name="Show.X", state=st,
        total_bytes=1000, source_announce_url="https://alpha.cc/announce")
    grace = mk("b" * 40, State.NEW)
    grace.skipped = 1
    notes = {"b" * 40: "Waiting for preferred copy · 90s left"}
    assert _member_button_labels([grace]) == [("b" * 40, "alpha")]
    assert _now_eligible_hashes([grace], notes) == []
    assert _member_button_labels([mk("c" * 40, State.NEW)]) != []


def test_render_detail_shows_held_resume():
    """Held detail cards carry the /resume_ line."""
    from racing_sync.telegram_bot import render_detail

    ts = TorrentState(source_infohash="e" * 40, source_name="Held",
                      state=State.WAITING_INDEXER, total_bytes=1000,
                      skipped=1)
    detail = render_detail(ts)
    assert "Skipped" in detail
    assert f"/resume_{'e' * 10}" in detail


def test_full_reset_refuses_symlink_ssd(tmp_path: Path, monkeypatch):
    from racing_sync.__main__ import _do_full_reset

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    (ssd / "Pack").mkdir()
    link = tmp_path / "ssd-link"
    try:
        link.symlink_to(ssd, target_is_directory=True)
    except OSError:
        import pytest as _pt
        _pt.skip("symlinks unavailable")
    cfg = MagicMock()
    cfg.general.state_db = tmp_path / "state.db"
    cfg.ssd.path = link
    cfg.dest.save_path = link
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse"

    class _Dest:
        def __init__(self, *a, **k):
            pass

        async def start(self):
            pass

        async def close(self):
            pass

        async def list_torrents(self, *, category=None):
            return []

    monkeypatch.setattr("racing_sync.clients.qbittorrent.QBittorrentClient", _Dest)
    import asyncio

    lines = asyncio.run(_do_full_reset(cfg))
    assert any("refusing to wipe SSD" in ln for ln in lines)
    # Real SSD data untouched through the symlink refusal.
    assert (ssd / "Pack").exists()


@pytest.mark.anyio
async def test_chat_command_double_tap_debounced(tmp_path: Path):
    """A double-sent /cancel_ resolves+acts once (0.5s debounce)."""
    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/cancel_dddddddddd"))
        await bot._handle_chat_message(_message(text="/cancel_dddddddddd"))
        # Unknown hash replies once; the second tap is dropped silently.
        bot._bot.send_message.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_prefer_starts_grace_held_row(tmp_path: Path):
    """`/prefer_<hash>` (legacy alias of `/now_`) starts a grace-held drop."""
    from conftest import make_coordinator

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        coord.cfg.general.preferred_copy_grace_seconds = 3600
        coord.cfg.prowlarr.enabled = True
        coord.cfg.prowlarr.download_indexers = [MagicMock()]
        coord.cfg.prowlarr.is_download_indexer = lambda url: False
        coord._spawn_worker = MagicMock()
        ts = TorrentState(source_infohash="e" * 40, source_name="Prefer.Me",
                          source_announce_url="https://alpha.cc/announce/xyz",
                          source_tracker="https://alpha.cc/announce/xyz",
                          cross_seed_blob=b"d8:announce...",
                          cross_seed_source="watch-dir", state=State.NEW)
        ts._blob = b"d8:announce..."
        store.upsert(ts)
        bot = _bot()
        bot._coord = coord
        bot._store = store
        await bot._handle_chat_message(_message(text=f"/prefer_{'e' * 40}"))
        bot._bot.send_message.assert_awaited_once()
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Using dropped .torrent directly for")
        row = store.get("e" * 40)
        assert row.force_direct == 1
        coord._spawn_worker.assert_called_once()
        # Unknown hashes get an explanatory reply, not a crash.
        # (Clear the debounce so the second command is processed.)
        bot._callback_times.clear()
        bot._bot.send_message.reset_mock()
        await bot._handle_chat_message(_message(text="/prefer_dddddddddd"))
        bot._bot.send_message.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_chat_message_now_starts_waiting_and_grace_rows(tmp_path: Path):
    """`/now_<hash>` unites fetch+prefer: WAITING rows and grace rows start."""
    from conftest import make_coordinator

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        coord.cfg.general.preferred_copy_grace_seconds = 3600
        coord.cfg.prowlarr.enabled = True
        coord.cfg.prowlarr.download_indexers = [MagicMock()]
        coord.cfg.prowlarr.is_download_indexer = lambda url: False
        coord._spawn_worker = MagicMock()
        store.upsert(TorrentState(
            source_infohash="f" * 40, source_name="Waiting.Now",
            state=State.WAITING_INDEXER, indexer_attempts=3))
        bot = _bot()
        bot._coord = coord
        bot._store = store
        await bot._handle_chat_message(_message(text=f"/now_{'f' * 40}"))
        row = store.get("f" * 40)
        assert row.force_direct == 1
        assert row.indexer_attempts == 0
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Fetching original for")
    finally:
        store.close()


def _twins_store(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    for h in ("a" * 40, "b" * 40):
        store.upsert(TorrentState(source_infohash=h, source_name="Twin.Show",
                                  save_path=str(ssd), state=State.NEW,
                                  cross_seed_source="watch-dir",
                                  source_announce_url="https://alpha.cc/announce/xyz",
                                  total_bytes=1000))
    return store, ssd


def _twins_bot(store, ssd, tmp_path: Path):
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    return bot, coord


def _tap(data):
    q = MagicMock()
    q.message = MagicMock()
    q.data = data
    q.answer = AsyncMock()
    return q


@pytest.mark.anyio
async def test_cancel_group_member_then_yes_keeps_files(tmp_path: Path):
    """Group cancel → member → Just forget → Yes: only it goes, no ignore."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        # Twins share name+size: group 1. Buttons carry each copy's hash.
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"cancel:{'b' * 40}" in _data
        assert any(d.startswith("cancel:all:") for d in _data)
        # Tap the b-copy → remember question (nothing deleted yet).
        await bot._on_action_button(
            _tap(f"cancel:{'b' * 40}"), f"cancel:{'b' * 40}")
        assert store.get("b" * 40) is not None
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"forget:{'b' * 40}:1" in _data
        assert f"forget:{'b' * 40}:0" in _data
        # Just forget (no remember) → keep question for files.
        await bot._on_action_button(
            _tap(f"forget:{'b' * 40}:0"), f"forget:{'b' * 40}:0")
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"keep:{'b' * 40}:0:yes" in _data
        # Yes: forget with files kept and NOT ignored; a-copy untouched.
        await bot._on_action_button(
            _tap(f"keep:{'b' * 40}:0:yes"), f"keep:{'b' * 40}:0:yes")
        assert store.get("b" * 40) is None
        assert store.is_ignored("b" * 40) is False
        assert store.get("a" * 40) is not None
        assert store.is_ignored("a" * 40) is False
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Kept files")
        assert "re-added" in sent
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_all_then_no_deletes_everything(tmp_path: Path):
    """All + ignore + No: every copy forgotten, ignored, files deleted."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        _all = next(d for d in _data if d.startswith("cancel:all:"))
        _leader = _all.split(":")[2]
        await bot._on_action_button(_tap(_all), _all)
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        _forget = next(d for d in _data if d.startswith("forget:all:"))
        assert _forget.endswith(":1") or _forget.endswith(":0")
        await bot._on_action_button(_tap(_forget), _forget)
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        _ig = _forget.split(":")[3]
        assert f"keep:all:{_leader}:{_ig}:yes" in _data
        await bot._on_action_button(
            _tap(f"keep:all:{_leader}:{_ig}:no"),
            f"keep:all:{_leader}:{_ig}:no")
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Cancelled")
        if _ig == "1":
            assert store.is_ignored("a" * 40) is True
            assert store.is_ignored("b" * 40) is True
        else:
            assert store.is_ignored("a" * 40) is False
            assert store.is_ignored("b" * 40) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_abort_deletes_sheet_and_acts_on_nothing(tmp_path: Path):
    """Abort removes the sheet message; rows untouched."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    bot._bot.delete_message = AsyncMock()
    try:
        await bot._handle_chat_message(_message(text="/cancel_1"))
        q = _tap("abort")
        q.message.message_id = 61
        await bot._on_action_button(q, "abort")
        bot._bot.delete_message.assert_awaited_once()
        assert store.get("a" * 40) is not None
        assert store.get("b" * 40) is not None
    finally:
        store.close()


def test_act_regex_shapes():
    from racing_sync.telegram_bot import ACT_CMD_RE

    assert ACT_CMD_RE.match("/act_3")
    assert ACT_CMD_RE.match("/act_" + "a" * 40)
    assert not ACT_CMD_RE.match("/act")
    assert not ACT_CMD_RE.match("/act_xyz")


@pytest.mark.anyio
async def test_act_command_replies_sheet_with_hash_buttons(tmp_path: Path):
    """/act_1 shows eligible actions only; every button is hash-scoped."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Twin.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER))
    store.upsert(TorrentState(source_infohash="b" * 40, source_name="Twin.Show",
                              total_bytes=1000, state=State.WAITING_INDEXER))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/act_1"))
        sent = bot._bot.send_message.call_args[0][1]
        assert "Twin.Show" in sent
        _kwargs = bot._bot.send_message.call_args[1]
        _data = [b.callback_data for r in
                 _kwargs["reply_markup"].inline_keyboard for b in r]
        assert f"go:{'a' * 40}" in _data
        assert f"cancel:{'b' * 40}" in _data
        assert any(d.startswith("cancel:all:") for d in _data)
        assert "abort" in _data
        assert not any(d.startswith("pick:") for d in _data)
        assert store.get("a" * 40) is not None  # sheet changes nothing
        _labels = [b.text for r in
                   _kwargs["reply_markup"].inline_keyboard for b in r]
        # Every button names its verb and target — no memorized commands.
        assert any(t.startswith("▶ Start ") for t in _labels)
        assert any(t.startswith("✖ Cancel ") for t in _labels)
        assert any("Skip" in t or "Resume" in t or "Inject" in t
                   for t in _labels)
    finally:
        store.close()


@pytest.mark.anyio
async def test_shifted_numbering_cannot_misroute(tmp_path: Path):
    """Hash buttons outlive regrouping: old taps can't hit new rows."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        # Group 1 = twins. Buttons carry each copy's own hash...
        await bot._handle_chat_message(_message(text="/cancel_1"))
        # ...then the world changes: twins gone from tracking, a new
        # same-named row appears (fresh drop re-takes group 1).
        store.tombstone("a" * 40)
        store.tombstone("b" * 40)
        store.upsert(TorrentState(source_infohash="c" * 40,
                                  source_name="Twin.Show",
                                  save_path=str(ssd), state=State.NEW,
                                  total_bytes=1000))
        # The old tap still addresses only the b-copy hash, gone now —
        # nothing acted on, certainly not the new row.
        await bot._on_action_button(
            _tap(f"cancel:{'b' * 40}"), f"cancel:{'b' * 40}")
        await bot._on_action_button(
            _tap(f"keep:{'b' * 40}:yes"), f"keep:{'b' * 40}:yes")
        assert store.get("c" * 40) is not None  # untouched
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Already gone")
    finally:
        store.close()

@pytest.mark.anyio
async def test_chat_message_unignore_lifts_ignore_and_tombstone(tmp_path: Path):
    """/unignore_<full hash> makes a cancelled release addable again."""
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="c" * 40, source_name="Gone",
                              state=State.WAITING_INDEXER))
    store.ignore_torrent("c" * 40, "Gone")
    store.tombstone("c" * 40)
    assert store.is_ignored("c" * 40) is True
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/unignore_" + "c" * 40))
        assert store.is_ignored("c" * 40) is False
        assert store.get("c" * 40) is not None
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Unignored")
    finally:
        store.close()

@pytest.mark.anyio
async def test_chat_message_unignore_rejects_short_hash(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/unignore_abc123"))
        sent = bot._bot.send_message.call_args[0][1]
        assert "full 40-char" in sent
    finally:
        store.close()

@pytest.mark.anyio
async def test_chat_message_unignore_space_form(tmp_path: Path):
    """/unignore <hash> (no underscore) works like the underscore form."""
    store = StateStore(tmp_path / "state.db")
    store.ignore_torrent("c" * 40, "Gone")
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/unignore " + "c" * 40))
        assert store.is_ignored("c" * 40) is False
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Unignored")
    finally:
        store.close()


# ---------------------------------------------------------------------------
# /cancel_match bulk cancel (substring over live group titles)
# ---------------------------------------------------------------------------

def _sheet_data(bot):
    _kwargs = bot._bot.send_message.call_args[1]
    return [b.callback_data for r in
            _kwargs["reply_markup"].inline_keyboard for b in r]


def _match_bot(tmp_path: Path, rows):
    """Real-store bot with a ready coordinator mock, rows upserted."""
    from racing_sync.state import StateStore

    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    store = StateStore(tmp_path / "state.db")
    for r in rows:
        store.upsert(r)
    coord = MagicMock()
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    cfg.general.state_db = tmp_path / "state.db"
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.get_torrent = AsyncMock(return_value=None)
    bot = _bot()
    bot._coord = coord
    bot._store = store
    return bot, store, ssd


@pytest.mark.anyio
async def test_cancel_match_bulk_forgets_matched_groups_only(tmp_path: Path):
    """/cancel_match evil → Q1 remember → Q2 keep/delete → matched gone."""
    _root = tmp_path / "ssd"
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Flower.of.Evil.S01",
                     total_bytes=1000, save_path=str(_root),
                     state=State.DOWNLOADING),
        TorrentState(source_infohash="b" * 40, source_name="Flower.of.Evil.S01",
                     total_bytes=1000, save_path=str(_root),
                     state=State.QUEUED),
        TorrentState(source_infohash="c" * 40, source_name="Flower.of.Evil.S02",
                     total_bytes=2000, save_path=str(_root),
                     state=State.MOVING),
        TorrentState(source_infohash="d" * 40, source_name="Unrelated.Show",
                     total_bytes=3000, save_path=str(_root),
                     state=State.MOVING),
    ])
    try:
        await bot._handle_chat_message(_message(text="/cancel_match evil"))
        _data = _sheet_data(bot)
        _forget = sorted(d for d in _data if d.startswith("forget:match:"))
        assert _forget == [d for d in _forget if d.endswith((":1", ":0"))]
        assert len(_forget) == 2
        _tok = _forget[0].split(":")[2]

        bot._callback_times.clear()
        await bot._handle_callback(_query(f"forget:match:{_tok}:1"))
        _data = _sheet_data(bot)
        assert f"keep:match:{_tok}:1:yes" in _data
        assert f"keep:match:{_tok}:1:no" in _data

        bot._callback_times.clear()
        await bot._handle_callback(_query(f"keep:match:{_tok}:1:no"))
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None
        assert store.get("c" * 40) is None
        assert store.get("d" * 40) is not None
        assert store.is_ignored("a" * 40) is True
        assert store.is_ignored("c" * 40) is True
        assert store.is_ignored("d" * 40) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_match_reresolves_between_questions(tmp_path: Path):
    """A group forgotten after Q1 is not acted on at Q2."""
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Evil.One",
                     state=State.QUEUED),
        TorrentState(source_infohash="b" * 40, source_name="Evil.Two",
                     state=State.QUEUED),
    ])
    try:
        await bot._handle_chat_message(_message(text="/cancel_match evil"))
        _tok = next(d.split(":")[2] for d in _sheet_data(bot)
                    if d.startswith("forget:match:"))
        await bot._handle_callback(_query(f"forget:match:{_tok}:0"))
        # Sibling flow removes one group before the keep tap.
        assert store.hard_delete("b" * 40) is True
        bot._callback_times.clear()
        await bot._handle_callback(_query(f"keep:match:{_tok}:0:yes"))
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None
        assert store.is_ignored("a" * 40) is False
        sent = bot._bot.send_message.call_args[0][1]
        assert "Kept files for" in sent
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_match_single_copy_uses_normal_sheet(tmp_path: Path):
    """One matching copy skips the bulk sheet for its forget question."""
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Only.Evil",
                     state=State.QUEUED),
        TorrentState(source_infohash="d" * 40, source_name="Unrelated",
                     state=State.QUEUED),
    ])
    try:
        await bot._handle_chat_message(_message(text="/cancel_match only"))
        _data = _sheet_data(bot)
        assert f"forget:{'a' * 40}:1" in _data
        assert not any(d.startswith("forget:match:") for d in _data)
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_match_no_match_short_and_long(tmp_path: Path):
    """Unknown text, missing text, and 1-char text all explain."""
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Some.Show",
                     state=State.QUEUED),
    ])
    try:
        await bot._handle_chat_message(_message(text="/cancel_match zzz-nope"))
        assert "No active groups match" in \
            bot._bot.send_message.call_args[0][1]
        bot._callback_times.clear()
        await bot._handle_chat_message(_message(text="/cancel_match"))
        assert "/cancel_match <text>" in \
            bot._bot.send_message.call_args[0][1]
        bot._callback_times.clear()
        await bot._handle_chat_message(_message(text="/cancel_match e"))
        assert "/cancel_match <text>" in \
            bot._bot.send_message.call_args[0][1]
        bot._callback_times.clear()
        await bot._handle_chat_message(
            _message(text="/cancel_match " + "x" * 36))
        assert "too long" in bot._bot.send_message.call_args[0][1]
    finally:
        store.close()


@pytest.mark.anyio
async def test_callback_router_forwards_forget_taps(tmp_path: Path):
    """`forget:` taps must reach the dispatcher (regression: router gap)."""
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Routed.Show",
                     state=State.QUEUED),
    ])
    try:
        await bot._handle_callback(_query(f"forget:{'a' * 40}:1"))
        _data = _sheet_data(bot)
        assert f"keep:{'a' * 40}:1:yes" in _data
        assert f"keep:{'a' * 40}:1:no" in _data
    finally:
        store.close()


@pytest.mark.anyio
async def test_answered_sheets_are_deleted(tmp_path: Path):
    """Each tapped sheet is deleted; replies and follow-ups stay."""
    bot, store, _ssd = _match_bot(tmp_path, [
        TorrentState(source_infohash="a" * 40, source_name="Gone.Show",
                     state=State.QUEUED),
    ])
    bot._bot.delete_message = AsyncMock()
    try:
        await bot._handle_callback(_query(f"cancel:{'a' * 40}"))
        bot._bot.delete_message.assert_awaited_once_with("1", 43)
        bot._bot.delete_message.reset_mock()
        bot._callback_times.clear()
        await bot._handle_callback(_query(f"forget:{'a' * 40}:1"))
        bot._bot.delete_message.assert_awaited_once_with("1", 43)
        assert f"keep:{'a' * 40}:1:yes" in _sheet_data(bot)
        bot._bot.delete_message.reset_mock()
        bot._callback_times.clear()
        await bot._handle_callback(_query(f"keep:{'a' * 40}:1:yes"))
        bot._bot.delete_message.assert_awaited_once_with("1", 43)
        sent = bot._bot.send_message.call_args[0][1]
        assert "Kept files for" in sent
        assert store.get("a" * 40) is None
        # Close deletes exactly once (explicit path, router skips it).
        bot._bot.delete_message.reset_mock()
        bot._callback_times.clear()
        await bot._handle_callback(_query("abort"))
        bot._bot.delete_message.assert_awaited_once_with("1", 43)
    finally:
        store.close()


def test_cancel_match_protocol_shapes_fit_buttons():
    """Match tokens round-trip and every button shape fits 64 bytes."""
    from racing_sync.telegram_bot import (
        _bot_command_menu,
        _match_query,
        _match_token,
        _parse_action_data,
    )

    assert any(c.command == "cancel_match" for c in _bot_command_menu())
    tok = _match_token("Flower.of.Evil")
    assert _match_query(tok) == "Flower.of.Evil"
    assert _match_token("x" * 36) == ""
    assert _match_query("!!!") == ""
    assert _match_query("") == ""
    for data in (f"forget:match:{tok}:1", f"forget:match:{tok}:0",
                 f"keep:match:{tok}:1:yes", f"keep:match:{tok}:0:no"):
        assert len(data) <= 64
        assert _parse_action_data(data)[0] in ("forget", "keep")
    assert _parse_action_data(f"keep:match:{tok}:1:yes") == (
        "keep", False, f"match:{tok}", "yes", True)
    assert _parse_action_data(f"forget:match:{tok}:0") == (
        "forget", False, f"match:{tok}", "", False)
