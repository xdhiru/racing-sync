"""Focused tests for the hygiene-fix batch (flap trip, late-seed memo,
mount short-circuit, added_on skew, protected-pattern validation).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _ts(state=None, **kw):
    from racing_sync.state import TorrentState, State
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
    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert "late1" in ts.injected_private_hashes

    coord.dest_client.reset_mock()
    await coord._check_and_inject_late_cross_seeds(ts, group)
    # Healthy + nothing new: memoized, zero dest traffic.
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
    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    import time

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
    from racing_sync.config import CleanupConfig
    import pytest as _pt

    with _pt.raises(Exception):
        CleanupConfig(protected_patterns=[""])
    with _pt.raises(Exception):
        CleanupConfig(protected_patterns=["   "])
    # Sane values still pass.
    assert CleanupConfig(protected_patterns=["My.Show"]).protected_patterns == ["My.Show"]


def test_rclone_flags_reject_hijack():
    from racing_sync.config import FuseConfig, RcloneConfig, RemoteConfig
    import pytest as _pt

    def _cfg(**kw):
        base = dict(
            remote=RemoteConfig(default="rem:/a/", unsorted="rem:/u/"),
            fuse=FuseConfig(mount="/m", mount_unsorted="/m/u"),
            extra_move_flags=[],
            batch_move_extra_flags=[],
        )
        base.update(kw)
        return RcloneConfig(**base)

    with _pt.raises(Exception):
        _cfg(extra_move_flags=["--config=/evil.conf"])
    with _pt.raises(Exception):
        _cfg(batch_move_extra_flags=["--password-command=echo x"])
    # Legit tuning still passes.
    assert _cfg(extra_move_flags=["--transfers=4", "--s3-chunk-size=64M"]).extra_move_flags


@pytest.mark.anyio
async def test_deluge_scan_cached_across_hash_lookups():
    """Single-hash get_torrent bursts share one full scan (5s TTL)."""
    from unittest.mock import AsyncMock
    from racing_sync.config import SourceConfig
    from racing_sync.clients.deluge import DelugeClient

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
