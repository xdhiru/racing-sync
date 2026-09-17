"""Cancel / ignore-list / full-reset / telegram-cancel coverage."""
from __future__ import annotations

from pathlib import Path
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
        msg = await bot._cancel_torrent("a" * 40)
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
        msg = await bot._cancel_torrent("a" * 40)
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
    # One copy-pasteable command per task, short hash in backticks.
    assert "`/cancel_aaaaaaaaaa`" in text
    assert "`/cancel_bbbbbbbbbb`" in text
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
        # Ambiguous prefix raises.
        with pytest.raises(LookupError, match="matches 2"):
            bot._resolve_cancel_target("a")
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
async def test_chat_message_cancel_executes_without_confirm(tmp_path: Path):
    """Sending `/cancel_<short>` forgets+ignores immediately and replies."""
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
        assert store.get("a" * 40) is None
        assert store.is_ignored("a" * 40) is True
        bot._bot.send_message.assert_awaited_once()
        sent_text = bot._bot.send_message.call_args[0][1]
        assert sent_text.startswith("Cancelled")
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
        assert store.get("c" * 40) is None
        assert store.is_ignored("c" * 40) is True
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
        msg = await bot._cancel_torrent("a" * 40)
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
    assert "`/fetch_aaaaaaaaaa`" in text
    assert text.count("Fetch original:") == 1
    assert "`/cancel_aaaaaaaaaa`" in text
    assert "`/cancel_bbbbbbbbbb`" in text


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
