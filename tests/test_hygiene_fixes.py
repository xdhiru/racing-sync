"""Focused tests for the hygiene-fix batch (flap trip, late-seed memo,
mount short-circuit, added_on skew, protected-pattern validation).
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator


def _ts(state=None, **kw):
    from racing_sync.state import State, TorrentState

    base = dict(
        source_infohash="a" * 40,
        source_name="Show",
        state=State.RE_ADDING if state is None else state,
    )
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
        ts.completed_at = dt.datetime.now(dt.UTC) - dt.timedelta(days=3)
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
    from racing_sync.state import State

    coord = make_coordinator()
    coord.cfg.fuse_reinject_delay_seconds = 0
    transitioned = {}

    def _transition(ts, dst, error=""):
        transitioned["dst"] = dst
        ts.state = dst

    coord.transition = _transition
    ts = _ts(
        state=State.RE_ADDING,
        readd_cycles=5,
        readd_first_attempted_at=dt.datetime.now(dt.UTC),
    )
    await coord._do_re_add(ts)
    assert transitioned.get("dst") == State.FAILED


@pytest.mark.anyio
async def test_late_seed_healthy_check_memoized(tmp_path):
    """A quiet healthy check backs off; the next tick is a client-traffic no-op."""
    from racing_sync.clients.abstract import AddResult, Torrent
    from racing_sync.state import State
    from racing_sync.watchdir import _bencode

    fuse = tmp_path / "fuse"
    fuse.mkdir()
    fname = "Movie.Title.mkv"
    fsize = 1000
    (fuse / fname).write_bytes(b"q" * fsize)
    blob = _bencode(
        {
            b"announce": b"http://tracker.example/announce",
            b"info": {
                b"name": fname.encode(),
                b"length": fsize,
                b"piece length": 16384,
                b"pieces": b"12345678901234567890",
            },
        }
    )

    coord = make_coordinator()
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.cfg.rclone.fuse.mount = str(fuse)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse)
    coord._target_mount_for = MagicMock(return_value=fuse)
    coord.dest_client = MagicMock()
    # Late-seed/repair paths load racing bytes through the DB-first loader.
    coord._load_racing_torrent_bytes = AsyncMock(return_value=blob)
    coord._failed_late_cross_seeds = {}
    # First attempt rejected -> deferred; retry succeeds -> injected.
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash=None, accepted=False, detail=None)
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=None)

    ts = _ts(
        state=State.DONE,
        source_infohash="src1",
        source_name="Movie.Title",
        save_path=str(fuse),
        injected_private_hashes="",
    )
    group = [
        Torrent(
            hash="late1",
            name="Movie.Title",
            category="",
            save_path="",
            size_bytes=1000,
            state="seeding",
            progress=1.0,
        )
    ]
    await coord._check_and_inject_late_cross_seeds(ts, group)
    assert "late1" in coord._failed_late_cross_seeds

    coord._failed_late_cross_seeds["late1"] = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=31)
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="late1", accepted=True, detail=None)
    )
    coord.dest_client.get_torrent = AsyncMock(return_value=MagicMock(save_path=str(fuse)))
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

    coord = make_coordinator()
    missing = await coord._missing_fuse_files(
        tmp_path / "no-such-mount", [("a.mkv", 1), ("b.mkv", 2)]
    )
    assert len(missing) == 1
    assert missing[0].startswith("<mount unavailable")


def test_cleanup_idle_rejects_skewed_added_on():
    import time

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    cfg = MagicMock()
    cfg.idle_confirm_minutes = 45.0
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(source_infohash="h", source_name="S", state=State.DONE)

    def _member(added):
        return Torrent(
            hash="h",
            name="S",
            category="",
            save_path="",
            size_bytes=1,
            state="seeding",
            progress=1.0,
            added_on=added,
        )

    # Future timestamp (VPS1 clock ahead) fails closed.
    assert coord._cleanup_idle_confirmed(ts, [_member(int(time.time()) + 3600)], now, cfg) is False
    # Near-epoch bogus timestamp fails closed.
    assert coord._cleanup_idle_confirmed(ts, [_member(1)], now, cfg) is False
    # Genuinely old + quiet passes.
    assert coord._cleanup_idle_confirmed(ts, [_member(int(time.time()) - 7200)], now, cfg) is True


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
    from racing_sync.coordinator import _NOT_VISIBLE_DETAIL

    def _coord_with_add(add_result, get_result):
        coord = make_coordinator()
        coord.dest_client = AsyncMock()
        coord.dest_client.add_torrent = AsyncMock(return_value=add_result)
        coord.dest_client.get_torrent = AsyncMock(return_value=get_result)
        coord.dest_client.delete = AsyncMock()
        return coord

    fuse = tmp_path / "fuse"
    # Invisible: get_torrent stays None through all 4 polls.
    coord = _coord_with_add(AddResult(hash="ab" * 20, accepted=True, detail="Ok."), None)
    ok, detail = await coord._ensure_fuse_entry(
        blob=b"d8:announce1:a4:infod4:name1:x6:lengthi1ee",
        infohash="ab" * 20,
        target_mount=fuse,
        label="t",
    )
    assert ok is False
    assert detail == _NOT_VISIBLE_DETAIL
    coord.dest_client.delete.assert_not_called()

    # Visible at target: trusted on the first poll (no 8s wait).
    entry = MagicMock(save_path=str(fuse))
    coord = _coord_with_add(AddResult(hash="ab" * 20, accepted=True, detail="Ok."), entry)
    ok, _ = await coord._ensure_fuse_entry(
        blob=b"d8:announce1:a4:infod4:name1:x6:lengthi1ee",
        infohash="ab" * 20,
        target_mount=fuse,
        label="t",
    )
    assert ok is True
    assert coord.dest_client.get_torrent.await_count == 1


@pytest.mark.anyio
async def test_fuse_replace_verifies_bytes_before_success(tmp_path):
    """The replace exit must run the same byte check as a fresh add.

    A gate→add race can leave the replacement pointing at a target whose
    bytes are gone; trusting save_path alone marked such rows injected.
    """
    from unittest.mock import AsyncMock

    from test_watchdir import _create_sample_torrent_data

    from racing_sync.clients.abstract import AddResult, Torrent
    from racing_sync.coordinator import _NOT_VISIBLE_DETAIL

    h = "cd" * 20
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    blob = _create_sample_torrent_data("Top.Show", 300, "http://x/announce")

    coord = make_coordinator()
    coord.dest_client = AsyncMock()
    # First add is a duplicate whose entry sits on the SSD; after the
    # delete+re-add the entry points at the fuse target.
    coord.dest_client.add_torrent = AsyncMock(
        side_effect=[
            AddResult(hash=h, accepted=False, detail="already added"),
            AddResult(hash=h, accepted=True, detail="Ok."),
        ]
    )
    coord.dest_client.get_torrent = AsyncMock(
        side_effect=[
            Torrent(
                hash=h,
                name="Top.Show",
                category="racing",
                save_path="/ssd/x",
                size_bytes=300,
                state="pausedUP",
                progress=1.0,
            ),
            Torrent(
                hash=h,
                name="Top.Show",
                category="racing",
                save_path=str(fuse),
                size_bytes=300,
                state="pausedUP",
                progress=1.0,
            ),
        ]
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.delete = AsyncMock()

    ok, detail = await coord._ensure_fuse_entry(blob=blob, infohash=h, target_mount=fuse, label="t")

    assert ok is False
    assert detail == _NOT_VISIBLE_DETAIL


@pytest.mark.anyio
async def test_done_row_in_fuse_subfolder_verifies_at_its_own_path(tmp_path):
    """A row adopted at mount/SUB must verify there, not at the mount root.

    Verifying at the configured root reports every expected file missing,
    so cleanup keeps VPS1 forever and repair defers on a healthy seed.
    """
    from unittest.mock import AsyncMock, MagicMock

    from test_watchdir import _create_sample_torrent_data

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State, TorrentState

    h = "ef" * 20
    fuse = tmp_path / "fuse"
    sub = fuse / "SUB"
    sub.mkdir(parents=True)
    blob = _create_sample_torrent_data("Top.Show", 300, "http://x/announce")
    (sub / "Top.Show").write_bytes(b"x" * 300)

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = str(fuse)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse / "unsorted")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h,
                name="Top.Show",
                category="racing",
                save_path=str(sub),
                size_bytes=300,
                state="seeding",
                progress=1.0,
            )
        ]
    )

    ts = TorrentState(
        source_infohash=h,
        dest_infohash=h,
        source_name="Top.Show",
        save_path=str(sub),
        total_bytes=300,
        state=State.DONE,
    )
    ts._blob = blob

    assert await coord._verified_fuse_mount_for_done_row(ts) == sub


@pytest.mark.anyio
async def test_deluge_scan_cached_across_hash_lookups():
    """Single-hash get_torrent bursts share one full scan (5s TTL)."""
    from unittest.mock import AsyncMock

    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={
            "enabled": True,
            "ssh_host": "127.0.0.1",
            "ssh_password": "pwd",
            "state_dir": "/var/lib/deluged/state",
        },
    )
    client = DelugeClient(cfg)
    client._rpc = AsyncMock(
        return_value={
            "hash_1": {
                "name": "T1",
                "progress": 100.0,
                "state": "Seeding",
                "total_size": 1000,
                "label": "",
                "save_path": "/d",
                "ratio": 0.0,
                "trackers": [],
                "time_added": 1,
            },
        }
    )
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
    blob = _bencode(
        {
            b"announce": b"http://tracker.example/announce",
            b"info": {
                b"name": b"a",
                b"length": 10,
                b"piece length": 16384,
                b"pieces": b"12345678901234567890",
            },
        }
    )
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
        '[source]\ntype = "qbittorrent"\nhost = "http://127.0.0.1:8080"\n'
        '[dest]\nhost = "http://127.0.0.1:8081"\nsave_path = "/d"\n'
        '[ssd]\npath = "/d"\nmax_inflight_bytes = 10\n'
        "skip_movie_larger_than_bytes = 10\n"
        '[rclone.remote]\ndefault = "rem:/a/"\nunsorted = "rem:/u/"\n'
        '[rclone.fuse]\nmount = "/m"\nmount_unsorted = "/m/u"\n'
    )
    with caplog.at_level(logging.WARNING):
        AppConfig.from_toml(cfg_file)
    assert any("max_active_download" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_ssd_backed_entry_is_not_a_fuse_seed(tmp_path):
    """A hash carried by an SSD-backed entry must not count as seeded.

    Such an entry passes an entry-only gate while nothing serves from the
    mount; it has to be re-injected like a missing one.
    """
    from test_watchdir import _create_sample_torrent_data

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State, TorrentState

    h = "ab" * 20
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.rclone.fuse.mount = str(fuse)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse / "unsorted")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h,
                name="Show",
                category="racing",
                save_path=str(ssd),  # still on the SSD, not seeding from fuse
                size_bytes=100,
                state="pausedUP",
                progress=1.0,
            )
        ]
    )
    coord._reinject_missing_done_entries = AsyncMock(return_value=True)

    ts = TorrentState(
        source_infohash=h,
        dest_infohash=h,
        source_name="Show",
        save_path=str(ssd),
        total_bytes=100,
        state=State.DONE,
    )
    ts._blob = _create_sample_torrent_data("Show", 100, "http://x/announce")

    await coord._verified_fuse_mount_for_done_row(ts)

    coord._reinject_missing_done_entries.assert_awaited_once()


@pytest.mark.anyio
async def test_demote_reraises_abandoned_error(tmp_path, monkeypatch):
    """A forgotten row must unwind, not log "demote failed" and carry on."""
    from types import SimpleNamespace

    from racing_sync.coordinator_errors import AbandonedError
    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[SimpleNamespace(name="a.mkv", size_bytes=10)]
    )
    coord.store = MagicMock()
    coord.store.get.return_value = TorrentState(
        source_infohash="a" * 40, source_name="Show", state=State.DONE
    )
    coord.transition = MagicMock(side_effect=AbandonedError("row gone"))

    import racing_sync.recovery as recovery_mod

    monkeypatch.setattr(recovery_mod, "find_content_on_ssd", lambda *a, **k: tmp_path)

    ts = TorrentState(source_infohash="a" * 40, source_name="Show", state=State.DONE)
    with pytest.raises(AbandonedError):
        await coord._demote_false_done_to_moving(ts)


@pytest.mark.anyio
async def test_blob_healing_prefers_the_rows_own_hash():
    """Healing from an injected cross-seed would change future re-adds."""
    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.store = MagicMock()

    async def _export(h):
        return b"own-bytes" if h == "f" * 40 else b"injected-bytes"

    coord.dest_client = AsyncMock()
    coord.dest_client.export_torrent = _export

    ts = TorrentState(
        source_infohash="s" * 40,
        dest_infohash="F" * 40,
        source_name="Show",
        injected_private_hashes="a" * 40,  # sorts first
        state=State.DONE,
    )

    assert await coord._cleanup_heal_blob(ts, {"a" * 40, "f" * 40}, dry_run=False) is True
    assert ts.cross_seed_blob == b"own-bytes"
    coord.store.set_blob.assert_called_once_with("s" * 40, b"own-bytes")


@pytest.mark.anyio
async def test_fuse_saved_done_row_verifies_recorded_entries(tmp_path):
    """A lost injected entry must be re-added even when save_path is on fuse.

    Recorded hashes are skipped by the new-torrent loop and the source
    repair only covers the original, so nothing else re-adds an injected
    private once its entry disappears.
    """
    from racing_sync.state import State, TorrentState

    fuse = tmp_path / "fuse"
    fuse.mkdir()

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = str(fuse)
    coord.cfg.rclone.fuse.mount_unsorted = str(fuse / "unsorted")
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord._verified_fuse_mount_for_done_row = AsyncMock(return_value=fuse)
    coord._failed_late_cross_seeds = {}
    coord._ensure_source_fuse_entry = AsyncMock()
    coord._ensure_original_fuse_entry = AsyncMock()

    ts = TorrentState(
        source_infohash="a" * 40,
        dest_infohash="a" * 40,
        injected_private_hashes="b" * 40,
        source_name="Show",
        save_path=str(fuse),
        total_bytes=100,
        state=State.DONE,
    )

    await coord._check_and_inject_late_cross_seeds(ts, [])

    coord._verified_fuse_mount_for_done_row.assert_awaited_once()
