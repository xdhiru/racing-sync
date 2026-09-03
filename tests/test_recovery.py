from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from racing_sync.config import AppConfig
from racing_sync.recovery import reconcile
from racing_sync.state import State, StateStore
from racing_sync.clients.abstract import Torrent


@pytest.mark.anyio
async def test_reconcile_adopts_fuse_and_completed_torrents(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    # Mock dest client returning torrents already on VPS2
    dest = AsyncMock()
    dest.list_torrents.return_value = [
        # Torrent 1: on fuse mount, complete
        Torrent(
            hash="1111111111111111111111111111111111111111",
            name="Seeding.On.Fuse.1080p.mkv",
            size_bytes=1000,
            save_path="/mnt/fuse/torrents",
            category="racing",
            progress=1.0,
            state="seeding",
        ),
        # Torrent 2: on unsorted fuse mount
        Torrent(
            hash="2222222222222222222222222222222222222222",
            name="Episode.S01E01.1080p.mkv",
            size_bytes=2000,
            save_path="/mnt/fuse/unsorted",
            category="racing",
            progress=1.0,
            state="uploading",
        ),
        # Torrent 3: incomplete on SSD (unknown)
        Torrent(
            hash="3333333333333333333333333333333333333333",
            name="Incomplete.Download.mkv",
            size_bytes=3000,
            save_path="/home/kevin/torrents/qbittorrent",
            category="racing",
            progress=0.3,
            state="downloading",
        ),
    ]

    report = await reconcile(cfg, dest=dest, store=store)

    assert len(report.kept) == 2
    assert len(report.unknowns) == 1

    # Check store has adopted the 2 fuse/completed torrents as DONE
    t1 = store.get("1111111111111111111111111111111111111111")
    assert t1 is not None
    assert t1.state == State.DONE

    t2 = store.get("2222222222222222222222222222222222222222")
    assert t2 is not None
    assert t2.state == State.DONE

    t3 = store.get("3333333333333333333333333333333333333333")
    assert t3 is None


@pytest.mark.anyio
async def test_fix_orphan_already_downloading(tmp_path: Path):
    from racing_sync.recovery import fix_orphan
    from racing_sync.state import TorrentState

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    ts = TorrentState("hash1", state=State.DOWNLOADING)
    store.upsert(ts)

    dest = AsyncMock()
    cfg = MagicMock()
    cfg.dest.save_path = Path("/srv/qbittorrent/data")

    # fix_orphan should NOT raise ValueError when already in DOWNLOADING
    res = await fix_orphan(ts, cfg, dest=dest, store=store, sftp_bytes=b"dummy-bytes")
    assert res == State.DOWNLOADING.value
    recovered = store.get("hash1")
    assert recovered is not None
    assert recovered.state == State.DOWNLOADING


@pytest.mark.anyio
async def test_do_re_add_fails_if_add_torrent_rejected():
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))

    # When add_torrent returns accepted=False
    coord.dest_client.add_torrent.return_value = AddResult(
        hash=None, accepted=False, detail="invalid torrent file"
    )

    ts = TorrentState(
        "hash1",
        source_name="Test.Release",
        state=State.RE_ADDING,
        _blob=b"torrent-bytes",
    )
    coord.transition = lambda t, s, error=None: setattr(t, "state", s)

    await coord._do_re_add(ts)

    # Must transition to FAILED, NOT DONE!
    assert ts.state == State.FAILED


@pytest.mark.anyio
async def test_check_and_inject_late_cross_seeds():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult, Torrent

    coord = object.__new__(Coordinator)
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"torrent-bytes")
    coord.dest_client.add_torrent.return_value = AddResult(hash="newhash", accepted=True)

    ts = TorrentState(
        "sourcehash",
        source_name="Show.Release",
        dest_infohash="desthash",
        injected_private_hashes="already1,already2",
        state=State.DONE,
    )

    group = [
        Torrent(hash="sourcehash", name="Show.Release", category="racing", save_path="", size_bytes=100, state="", progress=1.0),
        Torrent(hash="already1", name="Show.Release", category="racing", save_path="", size_bytes=100, state="", progress=1.0),
        Torrent(hash="newhash", name="Show.Release", category="racing", save_path="", size_bytes=100, state="", progress=1.0),
    ]

    await coord._check_and_inject_late_cross_seeds(ts, group)

    coord.dest_client.add_torrent.assert_awaited_once_with(
        torrent_files=[b"torrent-bytes"],
        save_path=str(Path("/mnt/fuse")),
        category="racing",
        paused=False,
        skip_check=True,
        tags=["racing", "fuse"],
    )

    assert "newhash" in ts.injected_private_hashes
    coord.store.upsert.assert_called_once_with(ts)


@pytest.mark.anyio
async def test_check_and_inject_late_cross_seeds_recognizes_existing_torrent():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult, Torrent

    coord = object.__new__(Coordinator)
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord._target_mount_for = MagicMock(return_value=Path("/mnt/fuse"))
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"torrent-bytes")
    # qBittorrent returns "Fails." because torrent is already in qBittorrent
    coord.dest_client.add_torrent.return_value = AddResult(hash=None, accepted=False, detail="Fails.")
    # get_torrent confirms it exists on dest_client
    coord.dest_client.get_torrent.return_value = Torrent(
        hash="existinghash", name="Show.Release", category="racing", save_path="/mnt/fuse", size_bytes=100, state="uploading", progress=1.0
    )

    ts = TorrentState(
        "sourcehash",
        source_name="Show.Release",
        dest_infohash="desthash",
        injected_private_hashes="",
        state=State.DONE,
    )

    group = [
        Torrent(hash="sourcehash", name="Show.Release", category="racing", save_path="", size_bytes=100, state="", progress=1.0),
        Torrent(hash="existinghash", name="Show.Release", category="racing", save_path="", size_bytes=100, state="", progress=1.0),
    ]

    await coord._check_and_inject_late_cross_seeds(ts, group)

    # Must be marked as injected despite add_torrent returning "Fails."
    assert "existinghash" in ts.injected_private_hashes
    coord.store.upsert.assert_called_once_with(ts)


@pytest.mark.anyio
async def test_reconcile_links_same_name_torrents_to_single_state(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import StateStore, State
    from racing_sync.recovery import reconcile
    from racing_sync.clients.abstract import Torrent

    cfg = MagicMock()
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_unsorted"
    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    # Destination client has 2 torrents on VPS2 with identical names (e.g. public + private)
    t1 = Torrent(hash="hash1", name="Anime.Ep1", category="racing", save_path=str(cfg.rclone.fuse.mount), size_bytes=1000, state="seeding", progress=1.0)
    t2 = Torrent(hash="hash2", name="Anime.Ep1", category="racing", save_path=str(cfg.rclone.fuse.mount), size_bytes=1000, state="seeding", progress=1.0)

    dest = AsyncMock()
    dest.list_torrents.return_value = [t1, t2]

    rpt = await reconcile(cfg, dest=dest, store=store)

    assert len(rpt.kept) == 2
    rows = store.all()
    # Should only create 1 primary TorrentState row, with hash2 in injected_private_hashes
    assert len(rows) == 1
    assert rows[0].source_infohash == "hash1"
    assert rows[0].injected_private_hashes == "hash2"
    assert rows[0].state == State.DONE




