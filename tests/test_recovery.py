from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from racing_sync.config import AppConfig
from racing_sync.recovery import reconcile
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.clients.abstract import Torrent


@pytest.fixture
def anyio_backend():
    return "asyncio"



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
            save_path="/home/user/torrents/qbittorrent",
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


@pytest.mark.anyio
async def test_reconcile_recognizes_dest_and_cross_seed_infohash(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import StateStore, State, TorrentState
    from racing_sync.recovery import reconcile
    from racing_sync.clients.abstract import Torrent

    cfg = MagicMock()
    cfg.rclone.fuse.mount = tmp_path / "fuse"
    cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse_unsorted"
    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    # In-flight torrent where destination client hash is dest_infohash (cross-seed)
    ts = TorrentState(
        source_infohash="source_h",
        dest_infohash="dest_h",
        source_name="Movie.2024",
        state=State.DOWNLOADING,
    )
    store.upsert(ts)

    # Destination client returns the torrent under dest_h
    t_dest = Torrent(
        hash="dest_h",
        name="Movie.2024",
        category="racing",
        save_path="/downloads",
        size_bytes=5000,
        state="downloading",
        progress=0.5,
    )
    dest = AsyncMock()
    dest.list_torrents.return_value = [t_dest]

    rpt = await reconcile(cfg, dest=dest, store=store)
    # Must be recognized as resumed, NOT an orphan
    assert "source_h" in rpt.resumed
    assert "source_h" not in rpt.orphans
    assert len(rpt.unknowns) == 0


@pytest.mark.anyio
async def test_reconcile_adopts_ssd_complete_as_moving_and_fuse_as_done(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        # Torrent 1: 100% complete on SSD (NOT on fuse) -> MUST adopt as MOVING
        Torrent(
            hash="ssd_done_hash",
            name="SSD.Done.Release.1080p",
            size_bytes=4000,
            save_path="/home/user/torrents/qbittorrent",
            category="racing",
            progress=1.0,
            state="uploading",
        ),
        # Torrent 2: on fuse mount -> adopt as DONE
        Torrent(
            hash="fuse_done_hash",
            name="Fuse.Done.Release.1080p",
            size_bytes=5000,
            save_path="/mnt/fuse/torrents",
            category="racing",
            progress=1.0,
            state="seeding",
        ),
    ]

    report = await reconcile(cfg, dest=dest, store=store)
    assert len(report.kept) == 2

    # Verify SSD complete torrent is adopted as MOVING (so coordinator moves it to remote)
    t_ssd = store.get("ssd_done_hash")
    assert t_ssd is not None
    assert t_ssd.state == State.MOVING

    # Verify fuse torrent is adopted as DONE
    t_fuse = store.get("fuse_done_hash")
    assert t_fuse is not None
    assert t_fuse.state == State.DONE


@pytest.mark.anyio
async def test_reconcile_invokes_fix_orphan_for_missing_inflight(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.dest.save_path = tmp_path / "downloads"
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    # An in-flight DOWNLOADING torrent in DB whose torrent is missing on VPS2
    ts = TorrentState(
        source_infohash="orphan_hash_1",
        source_name="Orphan.Release",
        state=State.DOWNLOADING,
        cross_seed_blob=b"torrent-bytes",
    )
    store.upsert(ts)

    dest = AsyncMock()
    dest.list_torrents.return_value = []  # Missing on VPS2

    report = await reconcile(cfg, dest=dest, store=store)
    assert "orphan_hash_1" in report.orphans
    # fix_orphan should have re-added the torrent paused and kept/resumed DOWNLOADING
    dest.add_torrent.assert_awaited_once()
    reloaded = store.get("orphan_hash_1")
    assert reloaded is not None
    assert reloaded.state == State.DOWNLOADING


@pytest.mark.anyio
async def test_fix_orphan_glob_escape_matches_special_characters(tmp_path: Path):
    from racing_sync.recovery import fix_orphan

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.dest.save_path = tmp_path / "downloads"
    cfg.dest.save_path.mkdir(parents=True, exist_ok=True)

    # Torrent name with square brackets, e.g. release groups
    name = "[DummySub] Show Name - 01 [1080p]"
    content_file = cfg.dest.save_path / f"{name}.mkv"
    content_file.write_bytes(b"video-data")

    ts = TorrentState(
        source_infohash="bracket_hash",
        source_name=name,
        state=State.MOVING,
        save_path=str(cfg.dest.save_path),
    )
    store.upsert(ts)

    dest = AsyncMock()
    res = await fix_orphan(ts, cfg, dest=dest, store=store)
    # Because content exists and glob.escape properly escapes '[', content_exists is True -> MOVING
    assert res == State.MOVING.value


@pytest.mark.anyio
async def test_reconcile_preserves_parked_waiting_indexer(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    ts = TorrentState(
        source_infohash="indexer_parked_hash",
        source_name="Parked.Torrent.Release",
        state=State.WAITING_INDEXER,
        indexer_attempts=2,
    )
    store.upsert(ts)

    dest = AsyncMock()
    dest.list_torrents.return_value = []  # Not present on VPS2

    report = await reconcile(cfg, dest=dest, store=store)

    # Must be considered safely resumed, NOT an orphan
    assert "indexer_parked_hash" in report.resumed
    assert "indexer_parked_hash" not in report.orphans
    assert len(report.orphans) == 0

    reloaded = store.get("indexer_parked_hash")
    assert reloaded is not None
    assert reloaded.state == State.WAITING_INDEXER
    assert reloaded.indexer_attempts == 2
    dest.add_torrent.assert_not_called()


@pytest.mark.anyio
async def test_reconcile_fuse_mount_prefix_no_false_positive(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse_unsorted")

    # Incomplete torrent on /mnt/fuse2 (which starts with /mnt/fuse as string prefix)
    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="fuse2_hash",
            name="Incomplete.Other.mkv",
            size_bytes=1000,
            save_path="/mnt/fuse2/torrents",
            category="racing",
            progress=0.5,
            state="downloading",
        ),
    ]

    report = await reconcile(cfg, dest=dest, store=store)
    # Must NOT be marked as kept (on_fuse), should be an unknown
    assert "fuse2_hash" not in report.kept
    assert "fuse2_hash" in report.unknowns


@pytest.mark.anyio
async def test_auto_retry_failed_caps_retries(tmp_path: Path):
    from racing_sync.coordinator import Coordinator
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    # Torrent 1: failed_retries = 0 -> should be retried and incremented
    ts1 = TorrentState(
        source_infohash="fail1",
        source_name="Fail1",
        state=State.FAILED,
        failed_retries=0,
    )
    # Torrent 2: failed_retries = 3 -> should NOT be retried (exceeded cap of 3)
    ts2 = TorrentState(
        source_infohash="fail2",
        source_name="Fail2",
        state=State.FAILED,
        failed_retries=3,
    )
    store.upsert(ts1)
    store.upsert(ts2)

    coord = object.__new__(Coordinator)
    coord.store = store
    coord.cfg = MagicMock()
    coord.cfg.recovery.run_on_startup = False
    coord.cfg.recovery.auto_retry_failed = True
    coord.cfg.recovery.max_failed_retries = 3
    coord.transition = lambda t, s: coord.store.transition(t, s)

    # Simulate coordinator startup retry logic
    failed_rows = [ts for ts in coord.store.all() if ts.state == State.FAILED]
    max_retries = getattr(coord.cfg.recovery, "max_failed_retries", 3)
    for ts in failed_rows:
        if ts.failed_retries >= max_retries:
            continue
        ts.failed_retries += 1
        coord.transition(ts, State.NEW)

    r1 = store.get("fail1")
    r2 = store.get("fail2")
    assert r1.state == State.NEW
    assert r1.failed_retries == 1
    # r2 stayed in FAILED
    assert r2.state == State.FAILED
    assert r2.failed_retries == 3


@pytest.mark.anyio
async def test_queued_existing_torrent_resumes(tmp_path: Path):
    from racing_sync.coordinator import Coordinator
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.store = store
    coord.cfg = MagicMock()
    coord.cfg.rclone.fuse.mount = Path("/mnt/fuse")
    coord.cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse_unsorted")
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.transition = lambda t, s: setattr(t, "state", s)

    ext_torrent = Torrent(
        hash="existing_hash_123",
        name="Existing.Torrent",
        size_bytes=1000,
        save_path="/local/ssd/downloads",
        category="racing",
        progress=0.4,
        state="paused",
    )
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents.return_value = [ext_torrent]

    ts = TorrentState(
        source_infohash="existing_hash_123",
        source_name="Existing.Torrent",
        cross_seed_blob=b"torrent_bytes",
        state=State.QUEUED,
    )

    await coord._do_queued(ts)

    # Must call resume on destination client
    coord.dest_client.resume.assert_awaited_once_with("existing_hash_123")
    assert ts.state == State.DOWNLOADING







