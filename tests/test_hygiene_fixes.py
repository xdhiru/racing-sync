"""Focused tests for the hygiene-fix batch (flap trip, late-seed memo,
mount short-circuit, added_on skew, protected-pattern validation).
"""
from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _ts(state=None, **kw):
    from racing_sync.state import State, TorrentState
    base = dict(source_infohash="a" * 40, source_name="Show",
                state=State.RE_ADDING if state is None else state)
    base.update(kw)
    return TorrentState(**base)


def test_done_rapid_demotions_count_flaps(tmp_path):
    from racing_sync.state import State, StateStore

    store = StateStore(tmp_path / "s.db")
    try:
        ts = _ts()
        store.upsert(ts)
        # First DONE entry anchors completed_at=now.
        store.transition(ts, State.DONE)
        assert ts.readd_cycles == 0
        # Rapid demotions (fresh DONE) accumulate...
        store.transition(ts, State.RE_ADDING)
        assert ts.readd_cycles == 1
        store.transition(ts, State.DONE)
        store.transition(ts, State.RE_ADDING)
        assert ts.readd_cycles == 2
        # ...while a demotion long after DONE starts a new incident count.
        store.transition(ts, State.DONE)
        ts.completed_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3)
        store.upsert(ts)
        store.transition(ts, State.RE_ADDING)
        assert ts.readd_cycles == 0
        # Fresh lifecycles reset the counter.
        store.transition(ts, State.FAILED, error="x")
        store.transition(ts, State.QUEUED)
        assert ts.readd_cycles == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_re_add_trips_on_flap_limit():
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import State

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.store = MagicMock()
    transitioned = {}

    def _transition(ts, dst, error=""):
        transitioned["dst"] = dst
        ts.state = dst

    coord.transition = _transition
    ts = _ts(state=State.RE_ADDING, readd_cycles=5,
             readd_first_attempted_at=dt.datetime.now(dt.timezone.utc))
    await coord._do_re_add(ts)
    assert transitioned.get("dst") == State.FAILED


@pytest.mark.anyio
async def test_late_seed_healthy_check_memoized(tmp_path):
    """A quiet healthy check backs off; the next tick is a client-traffic no-op."""
    from racing_sync.clients.abstract import AddResult, Torrent
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import State
    from racing_sync.watchdir import _bencode

    fuse = tmp_path / "fuse"
    fuse.mkdir()
    fname = "Movie.Title.mkv"
    fsize = 1000
    (fuse / fname).write_bytes(b"q" * fsize)
    blob = _bencode({
        b"announce": b"http://tracker.example/announce",
        b"info": {
            b"name": fname.encode(),
            b"length": fsize,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.cfg.rclone.fuse.mount = str(fuse)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse)
    coord._target_mount_for = MagicMock(return_value=fuse)
    coord.dest_client = MagicMock()
    coord.store = MagicMock()
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=blob)
    coord._failed_late_cross_seeds = {}
    # First attempt rejected -> deferred; retry succeeds -> injected.
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=None, accepted=False, detail=None))
    coord.dest_client.get_torrent = AsyncMock(return_value=None)

    ts = _ts(state=State.DONE, source_infohash="src1",
             source_name="Movie.Title", save_path=str(fuse),
             injected_private_hashes="")
    group = [Torrent(hash="late1", name="Movie.Title", category="",
                     save_path="", size_bytes=1000, state="seeding", progress=1.0)]
    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert "late1" in coord._failed_late_cross_seeds

    coord._failed_late_cross_seeds["late1"] = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=31))
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="late1", accepted=True, detail=None))
    coord.dest_client.get_torrent = AsyncMock(
        return_value=MagicMock(save_path=str(fuse)))
    coord._save_path_points_at_target = MagicMock(return_value=True)
    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert "late1" in ts.injected_private_hashes

    coord.dest_client.reset_mock()
    await coord._check_and_inject_late_cross_seeds(ts, group)
    # Healthy + nothing new: memo engaged — the following tick is silent.
    coord.dest_client.reset_mock()
    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert coord.dest_client.method_calls == []


@pytest.mark.anyio
async def test_missing_fuse_files_short_circuits_dead_mount(tmp_path):
    from racing_sync.coordinator import Coordinator

    coord = object.__new__(Coordinator)
    missing = await coord._missing_fuse_files(
        tmp_path / "no-such-mount", [("a.mkv", 1), ("b.mkv", 2)])
    assert len(missing) == 1
    assert missing[0].startswith("<mount unavailable")


def test_cleanup_idle_rejects_skewed_added_on():
    import time

    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import State, TorrentState

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    cfg = MagicMock()
    cfg.idle_confirm_minutes = 45.0
    now = dt.datetime.now(dt.timezone.utc)
    ts = TorrentState(source_infohash="h", source_name="S", state=State.DONE)

    def _member(added):
        return Torrent(hash="h", name="S", category="", save_path="",
                       size_bytes=1, state="seeding", progress=1.0,
                       added_on=added)

    # Future timestamp (VPS1 clock ahead) fails closed.
    assert coord._cleanup_idle_confirmed(
        ts, [_member(int(time.time()) + 3600)], now, cfg) is False
    # Near-epoch bogus timestamp fails closed.
    assert coord._cleanup_idle_confirmed(ts, [_member(1)], now, cfg) is False
    # Genuinely old + quiet passes.
    assert coord._cleanup_idle_confirmed(
        ts, [_member(int(time.time()) - 7200)], now, cfg) is True


def test_protected_patterns_reject_blank():
    import pytest as _pt
    from pydantic import ValidationError

    from racing_sync.config import CleanupConfig

    with _pt.raises(ValidationError):
        CleanupConfig(protected_patterns=[""])
    with _pt.raises(ValidationError):
        CleanupConfig(protected_patterns=["   "])
    # Sane values still pass.
    assert CleanupConfig(protected_patterns=["My.Show"]).protected_patterns == ["My.Show"]


def test_rclone_flags_reject_hijack():
    import pytest as _pt
    from pydantic import ValidationError

    from racing_sync.config import FuseConfig, RcloneConfig, RemoteConfig

    def _cfg(**kw):
        base = dict(
            remote=RemoteConfig(default="rem:/a/", unsorted="rem:/u/"),
            fuse=FuseConfig(mount="/m", mount_unsorted="/m/u"),
            extra_move_flags=[],
            batch_move_extra_flags=[],
        )
        base.update(kw)
        return RcloneConfig(**base)

    with _pt.raises(ValidationError):
        _cfg(extra_move_flags=["--config=/evil.conf"])
    with _pt.raises(ValidationError):
        _cfg(batch_move_extra_flags=["--password-command=echo x"])
    # Legit tuning still passes.
    assert _cfg(extra_move_flags=["--transfers=4", "--s3-chunk-size=64M"]).extra_move_flags


@pytest.mark.anyio
async def test_fuse_first_add_verified_before_trust(tmp_path):
    """Accepted-but-invisible fuse adds park (never DONE, never destructive).

    The fuse index lags when rclone is busy with another move: verification
    waits (4x2s, no deletes/replaces), then reports not-visible so callers
    retry. A visible entry trusts immediately.
    """
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import AddResult
    from racing_sync.coordinator import Coordinator, _NOT_VISIBLE_DETAIL

    def _coord_with_add(add_result, get_result):
        coord = object.__new__(Coordinator)
        coord.dest_client = AsyncMock()
        coord.dest_client.add_torrent = AsyncMock(return_value=add_result)
        coord.dest_client.get_torrent = AsyncMock(return_value=get_result)
        coord.dest_client.delete = AsyncMock()
        return coord

    fuse = tmp_path / "fuse"
    # Invisible: get_torrent stays None through all 4 polls.
    coord = _coord_with_add(
        AddResult(hash="ab" * 20, accepted=True, detail="Ok."), None)
    ok, detail = await coord._ensure_fuse_entry(
        blob=b"d8:announce1:a4:infod4:name1:x6:lengthi1ee",
        infohash="ab" * 20, target_mount=fuse, label="t")
    assert ok is False
    assert detail == _NOT_VISIBLE_DETAIL
    coord.dest_client.delete.assert_not_called()

    # Visible at target: trusted on the first poll (no 8s wait).
    entry = MagicMock(save_path=str(fuse))
    coord = _coord_with_add(
        AddResult(hash="ab" * 20, accepted=True, detail="Ok."), entry)
    ok, _ = await coord._ensure_fuse_entry(
        blob=b"d8:announce1:a4:infod4:name1:x6:lengthi1ee",
        infohash="ab" * 20, target_mount=fuse, label="t")
    assert ok is True
    assert coord.dest_client.get_torrent.await_count == 1


@pytest.mark.anyio
async def test_deluge_scan_cached_across_hash_lookups():
    """Single-hash get_torrent bursts share one full scan (5s TTL)."""
    from unittest.mock import AsyncMock

    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge", host="http://localhost:8112", password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1",
                     "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)
    client._rpc = AsyncMock(return_value={
        "hash_1": {"name": "T1", "progress": 100.0, "state": "Seeding",
                   "total_size": 1000, "label": "", "save_path": "/d",
                   "ratio": 0.0, "trackers": [], "time_added": 1},
    })
    client.get_torrent_files = AsyncMock(return_value=[])
    assert await client.get_torrent("hash_1") is not None
    assert await client.get_torrent("hash_1") is not None
    assert await client.list_torrents() is not None
    assert client._rpc.await_count == 1


def test_sftp_close_never_leaks_wedged_member():
    """close() marks dead + closes even when a holder wedges the lock."""
    import threading

    from racing_sync.sftp_source import _SFTPConnection

    cfg = MagicMock()
    cfg.ssh_key_path = None
    cfg.ssh_password = ""
    m = _SFTPConnection(cfg)
    held = threading.Event()
    release = threading.Event()

    def _holder():
        m._lock.acquire()
        held.set()
        assert release.wait(timeout=10)
        m._lock.release()

    t = threading.Thread(target=_holder, daemon=True)
    t.start()
    assert held.wait(timeout=10)
    m.close()  # must return (not hang 5s+), transports cleared
    assert m._closed is True
    assert m._sftp is None and m._client is None
    release.set()
    t.join(timeout=10)


@pytest.mark.anyio
async def test_watchdir_no_reemit_without_pickup(tmp_path):
    """Kept files are never evicted from _seen (no duplicate WatchItems)."""
    from racing_sync.config import WatchDirConfig
    from racing_sync.watchdir import WatchDirScanner, _bencode

    watch = tmp_path / "watch"
    watch.mkdir()
    blob = _bencode({
        b"announce": b"http://tracker.example/announce",
        b"info": {b"name": b"a", b"length": 10, b"piece length": 16384,
                  b"pieces": b"12345678901234567890"},
    })
    (watch / "a.torrent").write_bytes(blob)
    cfg = WatchDirConfig(path=watch, glob="*.torrent", delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, None)
    # Fill _seen with 6000 stale (non-resident) hashes + the resident one.
    scanner._seen = {f"stale{i:05d}" for i in range(6000)}
    first = await scanner.scan_once()
    resident = {i.infohash for i in first}
    assert len(resident) == 1
    scanner._seen |= resident
    second = await scanner.scan_once()
    assert second == []
    assert resident <= scanner._seen  # resident never evicted
    assert len(scanner._seen) <= 5001  # only non-resident overflow bounded


@pytest.mark.anyio
async def test_telegram_debounce_is_per_chat():
    """One chat's burst must not drop another chat's pagination."""
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.telegram_bot import TelegramBot

    bot = object.__new__(TelegramBot)
    bot._cfg = MagicMock()
    bot._cfg.chat_id = "1"
    bot._cfg.page_size = 5
    bot._callback_times = {}
    bot._last_callback_time = 0.0
    bot._store = MagicMock()
    bot._store.list_active_inflight = MagicMock(return_value=[])
    bot._current_page = 0
    bot._refresh_active_message = AsyncMock()

    def _query(chat, user):
        q = MagicMock()
        q.message.chat.id = chat
        q.from_user.id = user
        q.data = "page:next"
        q.answer = AsyncMock()
        return q

    await bot._handle_callback(_query("1", "9"))
    # Same chat immediately again: debounced...
    await bot._handle_callback(_query("1", "9"))
    # ...but a different chat (same authorized user) goes through.
    q_other = _query("9", "1")
    await bot._handle_callback(q_other)
    assert bot._refresh_active_message.await_count == 2


def test_unknown_config_keys_warned(tmp_path, caplog):
    """Typo'd keys log warnings instead of silently ignored."""
    import logging

    from racing_sync.config import AppConfig

    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        "[general]\nmax_active_download = 3\n"
        "[source]\ntype = \"qbittorrent\"\nhost = \"http://127.0.0.1:8080\"\n"
        "[dest]\nhost = \"http://127.0.0.1:8081\"\nsave_path = \"/d\"\n"
        "[ssd]\npath = \"/d\"\nmax_inflight_bytes = 10\n"
        "skip_movie_larger_than_bytes = 10\n"
        "[rclone.remote]\ndefault = \"rem:/a/\"\nunsorted = \"rem:/u/\"\n"
        "[rclone.fuse]\nmount = \"/m\"\nmount_unsorted = \"/m/u\"\n"
    )
    with caplog.at_level(logging.WARNING):
        AppConfig.from_toml(cfg_file)
    assert any("max_active_download" in r.message for r in caplog.records)
