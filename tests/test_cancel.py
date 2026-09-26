"""Cancel / ignore-list / full-reset / telegram-cancel coverage."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent, TorrentFile
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
        assert bot._pending_live() is None
        sent = [c.args[1] for c in
                bot._bot.send_message.await_args_list]
        assert any("Not authorized" in str(t) for t in sent)
        # Listed admin proceeds normally.
        bot._bot.send_message.reset_mock()
        bot._callback_times.clear()
        await bot._handle_chat_message(
            _message(user="4242", text="/cancel_1"))
        assert bot._pending_live() is not None
    finally:
        store.close()


@pytest.mark.anyio
async def test_pending_flow_bound_to_commanding_user(tmp_path: Path):
    """A different known user cannot tap another operator's picker."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    bot = _bot()
    bot._store = store
    bot._coord = MagicMock()
    bot._refresh_active_message = AsyncMock()
    try:
        store.upsert(TorrentState(source_infohash="a" * 40,
                                  source_name="Show", state=State.MOVING))
        store.upsert(TorrentState(source_infohash="b" * 40,
                                  source_name="Show", state=State.QUEUED))
        await bot._handle_chat_message(
            _message(user="4242", text="/cancel_1"))
        p = bot._pending_live()
        assert p is not None and p["user_id"] == "4242"
        # Foreign tap refused, flow intact.
        q = _query(f"pick:{p['seq']}:0", user="777")
        await bot._on_action_button(q, q.data)
        assert bot._pending_live() is not None
        # Owner tap advances.
        q2 = _query(f"pick:{p['seq']}:0", user="4242")
        await bot._on_action_button(q2, q2.data)
        p2 = bot._pending_live()
        assert p2 is not None and p2["kind"] == "keepq"
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
    # Positional group commands (escaped underscore, no labels).
    assert "/cancel\\_1" in text
    assert "/cancel\\_2" in text
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
    """Sending `/cancel_<short>` arms keepq — nothing deleted yet."""
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
        # Row untouched; pending keep-question armed instead.
        assert store.get("a" * 40) is not None
        assert store.is_ignored("a" * 40) is False
        _p = bot._pending_live()
        assert _p["kind"] == "keepq" and _p["hashes"] == ["a" * 40]
        bot._bot.send_message.assert_awaited_once()
        sent_text = bot._bot.send_message.call_args[0][1]
        assert sent_text.startswith("Cancel Show")
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
        # Full hash also lands on the keep question (never instant).
        assert store.get("c" * 40) is not None
        assert bot._pending_live()["hashes"] == ["c" * 40]
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
    assert "/cancel\\_1 /fetch\\_1" in text
    assert "Fetch original:" not in text
    assert "/cancel\\_2" in text
    assert "/fetch\\_aaaaaaaaaa" not in text


def test_render_detail_shows_fetch_hint_for_waiting_indexer():
    from racing_sync.telegram_bot import render_detail

    waiting = TorrentState(source_infohash="a" * 40, source_name="Show",
                           state=State.WAITING_INDEXER, total_bytes=1000,
                           indexer_attempts=9)
    detail = render_detail(waiting)
    # Full hash untouched in the detail card; fetch hint added.
    assert "`" + "a" * 40 + "`" in detail
    assert "`/fetch_aaaaaaaaaa`" in detail

    downloading = TorrentState(source_infohash="b" * 40, source_name="Show2",
                               state=State.DOWNLOADING, total_bytes=2000)
    assert "/fetch_" not in render_detail(downloading)


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
    """Sending `/fetch_<short>` sets force_direct and wakes to QUERYING."""
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
        assert row.state == State.QUERYING
        bot._bot.send_message.assert_awaited_once()
        sent_text = bot._bot.send_message.call_args[0][1]
        assert sent_text.startswith("Fetching VPS1 original")
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
        assert row.state == State.QUERYING
        assert row.indexer_attempts == 0
        fresh = (dt.datetime.now(dt.timezone.utc)
                 - row.indexer_first_queried_at).total_seconds()
        assert fresh < 60
    finally:
        store.close()


@pytest.mark.anyio
async def test_group_fetch_singleton_opens_titled_pick(tmp_path: Path):
    """Even one eligible copy goes through buttons: title + Cancel shown."""
    from racing_sync.telegram_bot import render_pending_question

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(source_infohash="a" * 40, source_name="Lone.Show",
                              state=State.WAITING_INDEXER, indexer_attempts=2,
                              total_bytes=1000))
    bot = _bot()
    bot._coord = MagicMock()
    bot._store = store
    try:
        await bot._handle_chat_message(_message(text="/fetch_1"))
        # Nothing executed: a titled pick with one button + Cancel.
        _p = bot._pending_live()
        assert _p["kind"] == "pick" and _p["cmd"] == "fetch"
        assert [h for (h, _) in _p["members"]] == ["a" * 40]
        _qtext, _qrows = bot._pending_section()
        assert "Lone.Show" in _qtext and "1000 B" in _qtext
        assert _qrows[0][0][0] != "Cancel"  # member button first...
        assert _qrows[-1] == [("Cancel", f"abort:{_p['seq']}")]
        row = store.get("a" * 40)
        assert row.state == State.WAITING_INDEXER  # untouched
        assert row.force_direct == 0
    finally:
        store.close()


def test_pending_question_shows_size_or_omits():
    from racing_sync.telegram_bot import render_pending_question

    assert "2.4G" in render_pending_question({
        "kind": "pick", "cmd": "cancel", "title": "Big.Show",
        "size": 2_600_000_000, "members": [], "seq": "1",
        "expires": 9999999999.0})
    assert "2.4G" in render_pending_question({
        "kind": "keepq", "title": "Big.Show", "scope": "bte copy",
        "size": 2_600_000_000, "hashes": [], "seq": "1",
        "expires": 9999999999.0})
    # Legacy pendings without size still render (no dangling separator).
    _q = render_pending_question({
        "kind": "keepq", "title": "Big.Show", "scope": "",
        "hashes": [], "seq": "1", "expires": 9999999999.0})
    assert "Big.Show" in _q and "·" not in _q.split("—")[0]


def test_pending_seq_unpredictable_and_prefix_min_length():
    """Seq tokens are random hex (no 1,2,3… forgery); <4-char prefixes rejected."""
    from racing_sync.telegram_bot import TelegramBot

    bot = TelegramBot.__new__(TelegramBot)
    bot._pending_seq = 0
    seen = {bot._next_seq() for _ in range(10)}
    assert len(seen) == 10  # no repeats in a short run
    assert all(len(s) == 16 and all(c in "0123456789abcdef" for c in s)
               for s in seen)

    bot._store = None
    with pytest.raises(LookupError, match="too short"):
        bot._resolve_cancel_target("a", cmd="cancel")
    with pytest.raises(LookupError, match="too short"):
        bot._resolve_cancel_target("ab", cmd="fetch")


def test_pending_question_sanitizes_backtick_title():
    from racing_sync.telegram_bot import render_pending_question

    _q = render_pending_question({
        "kind": "pick", "cmd": "cancel", "title": "Evil` — `x",
        "members": [], "seq": "1", "expires": 9999999999.0})
    assert "`" not in _q.replace("`Evil' — 'x`", "")  # span stays closed
    assert "Evil' — 'x" in _q


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
    """`/prefer_<hash>` exempts a grace-held drop and wakes a worker."""
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
        assert sent.startswith("Preferred")
        coord._spawn_worker.assert_called_once()
        assert "e" * 40 in (coord._grace_exempt or {})
        # Unknown hashes get an explanatory reply, not a crash.
        # (Clear the debounce so the second command is processed.)
        bot._callback_times.clear()
        bot._bot.send_message.reset_mock()
        await bot._handle_chat_message(_message(text="/prefer_dddddddddd"))
        bot._bot.send_message.assert_awaited_once()
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
async def test_cancel_group_pick_member_then_yes_keeps_files(tmp_path: Path):
    """Group cancel → pick one sibling → Yes: only it goes, files kept."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        # Twins share name+size: group 1. Snapshot freezes both hashes
        # (store order: newest first — derive indices, don't assume).
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _p = bot._pending_live()
        assert _p["kind"] == "pick"
        _hashes = [h for (h, _) in _p["members"]]
        assert sorted(_hashes) == ["a" * 40, "b" * 40]
        _bi = _hashes.index("b" * 40)
        _seq = _p["seq"]
        # Tap the b-copy; nothing deleted yet, keep-question armed.
        await bot._on_action_button(
            _tap(f"pick:{_seq}:{_bi}"), f"pick:{_seq}:{_bi}")
        _p = bot._pending_live()
        assert _p["kind"] == "keepq"
        assert _p["hashes"] == ["b" * 40]
        assert store.get("b" * 40) is not None
        # Yes: forget with files kept; the a-copy keeps seeding tracked.
        _seq = _p["seq"]
        await bot._on_action_button(
            _tap(f"keep:{_seq}:yes"), f"keep:{_seq}:yes")
        assert store.get("b" * 40) is None
        assert store.is_ignored("b" * 40) is True
        assert store.get("a" * 40) is not None
        assert store.is_ignored("a" * 40) is False
        assert bot._pending_live() is None  # consumed
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Kept files")
    finally:
        store.close()


@pytest.mark.anyio
async def test_cancel_all_then_no_deletes_everything(tmp_path: Path):
    """All + No: every copy forgotten with files deleted."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _seq = bot._pending_live()["seq"]
        await bot._on_action_button(
            _tap(f"pick:{_seq}:all"), f"pick:{_seq}:all")
        _p = bot._pending_live()
        assert _p["kind"] == "keepq"
        assert sorted(_p["hashes"]) == ["a" * 40, "b" * 40]
        _seq = _p["seq"]
        await bot._on_action_button(
            _tap(f"keep:{_seq}:no"), f"keep:{_seq}:no")
        assert store.get("a" * 40) is None
        assert store.get("b" * 40) is None
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Cancelled")
    finally:
        store.close()


@pytest.mark.anyio
async def test_pick_abort_cancels_nothing(tmp_path: Path):
    """Abort button drops the flow; rows untouched, pending cleared."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _seq = bot._pending_live()["seq"]
        await bot._on_action_button(
            _tap(f"abort:{_seq}"), f"abort:{_seq}")
        assert bot._pending_live() is None
        assert store.get("a" * 40) is not None
        assert store.get("b" * 40) is not None
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Picker cancelled")
    finally:
        store.close()


@pytest.mark.anyio
async def test_shifted_numbering_cannot_misroute(tmp_path: Path):
    """Number resolved once at tap: later regrouping can't redirect."""
    store, ssd = _twins_store(tmp_path)
    bot, coord = _twins_bot(store, ssd, tmp_path)
    try:
        # Group 1 = twins. Snapshot freezes their hashes...
        await bot._handle_chat_message(_message(text="/cancel_1"))
        _p = bot._pending_live()
        _hashes = [h for (h, _) in _p["members"]]
        assert sorted(_hashes) == ["a" * 40, "b" * 40]
        _seq = _p["seq"]
        # ...then the world changes: twins gone from tracking, a new
        # same-named row appears (fresh drop re-takes group 1).
        store.tombstone("a" * 40)
        store.tombstone("b" * 40)
        store.upsert(TorrentState(source_infohash="c" * 40,
                                  source_name="Twin.Show",
                                  save_path=str(ssd), state=State.NEW,
                                  total_bytes=1000))
        # The old tap still addresses only the snapshotted hashes, both
        # gone now — nothing acted on, certainly not the new row.
        _bi = _hashes.index("b" * 40)
        await bot._on_action_button(
            _tap(f"pick:{_seq}:{_bi}"), f"pick:{_seq}:{_bi}")
        _p = bot._pending_live()
        assert _p["kind"] == "keepq"
        assert _p["hashes"] == ["b" * 40]
        _seq = _p["seq"]
        await bot._on_action_button(
            _tap(f"keep:{_seq}:yes"), f"keep:{_seq}:yes")
        assert store.get("c" * 40) is not None  # untouched
        sent = bot._bot.send_message.call_args[0][1]
        assert sent.startswith("Already gone")
    finally:
        store.close()
