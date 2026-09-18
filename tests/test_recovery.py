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

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")
    cfg.dest.save_path = ssd
    cfg.ssd.path = ssd

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
        # Torrent 3: incomplete on SSD -> adopted as DOWNLOADING, not abandoned
        Torrent(
            hash="3333333333333333333333333333333333333333",
            name="Incomplete.Download.mkv",
            size_bytes=3000,
            save_path=str(ssd),
            category="racing",
            progress=0.3,
            state="downloading",
        ),
        # Torrent 4: incomplete outside any SSD root -> still unknown
        Torrent(
            hash="4444444444444444444444444444444444444444",
            name="Stray.Download.mkv",
            size_bytes=4000,
            save_path="/mnt/other/place",
            category="racing",
            progress=0.3,
            state="downloading",
        ),
    ]

    report = await reconcile(cfg, dest=dest, store=store)

    assert len(report.kept) == 2
    assert len(report.unknowns) == 1
    assert report.unknowns == ["4444444444444444444444444444444444444444"]
    assert len(report.adopted) == 3

    # Check store has adopted the 2 fuse/completed torrents as DONE
    t1 = store.get("1111111111111111111111111111111111111111")
    assert t1 is not None
    assert t1.state == State.DONE

    t2 = store.get("2222222222222222222222222222222222222222")
    assert t2 is not None
    assert t2.state == State.DONE

    # Partial SSD torrent resumes instead of being abandoned
    t3 = store.get("3333333333333333333333333333333333333333")
    assert t3 is not None
    assert t3.state == State.DOWNLOADING
    assert t3.dest_infohash == "3333333333333333333333333333333333333333"
    assert t3.save_path == str(ssd).replace("\\", "/")
    assert "3333333333333333333333333333333333333333" in report.resumed

    t4 = store.get("4444444444444444444444444444444444444444")
    assert t4 is None


@pytest.mark.anyio
async def test_reconcile_heals_unknown_done_classification(tmp_path: Path):
    """Pre-fix DONE rows (fast-tracked without kind) get classified in place.

    Without this a season pack at the default mount keeps kind="unknown"
    (-> unsorted) and its RE_ADDING fuse gate parks forever checking the
    wrong directory.
    """
    from racing_sync.clients.abstract import TorrentFile

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    h = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ts = TorrentState(
        source_infohash=h,
        source_name="Heal.Pack.S01",
        dest_infohash=h,
        save_path="/mnt/fuse/torrents",
        state=State.DONE,
    )
    assert ts.classification_kind == "unknown"
    store.upsert(ts)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")
    cfg.dest.save_path = Path("/home/kevin/torrents/qbittorrent")
    cfg.ssd.path = Path("/home/kevin/torrents/qbittorrent")

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash=h, name="Heal.Pack.S01", size_bytes=200,
            save_path="/mnt/fuse/torrents", category="racing",
            progress=1.0, state="seeding",
        ),
    ]
    dest.get_torrent_files.return_value = [
        TorrentFile(name="Heal.Pack.S01/Heal.Pack.S01E01.mkv", size_bytes=100, progress=1.0),
        TorrentFile(name="Heal.Pack.S01/Heal.Pack.S01E02.mkv", size_bytes=100, progress=1.0),
    ]

    report = await reconcile(cfg, dest=dest, store=store)

    healed = store.get(h)
    assert healed is not None
    assert healed.state == State.DONE
    assert healed.classification_kind == "season"
    assert h in report.kept


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
async def test_check_and_inject_late_cross_seeds(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult, Torrent
    from racing_sync.watchdir import _bencode

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    fname = "Late.New.Release.mkv"
    fsize = 100
    (fuse_dir / fname).write_bytes(b"x" * fsize)
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
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord._save_path_points_at_target = MagicMock(return_value=True)
    coord.dest_client.get_torrent = AsyncMock(return_value=MagicMock(save_path="x"))
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=blob)
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

    # The not-yet-injected source torrent is repaired first, then the late
    # arrival: two fuse adds total.
    assert coord.dest_client.add_torrent.await_count == 2
    assert "sourcehash" in ts.injected_private_hashes
    assert "newhash" in ts.injected_private_hashes
    assert coord.store.upsert.call_count == 2


@pytest.mark.anyio
async def test_check_and_inject_late_cross_seeds_recognizes_existing_torrent(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult, Torrent
    from racing_sync.watchdir import _bencode

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    fname = "Existing.Show.Release.mkv"
    fsize = 100
    (fuse_dir / fname).write_bytes(b"y" * fsize)
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
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord._target_mount_for = MagicMock(return_value=fuse_dir)
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=blob)
    # qBittorrent returns "Fails." because torrent is already in qBittorrent
    coord.dest_client.add_torrent.return_value = AddResult(hash=None, accepted=False, detail="Fails.")
    # get_torrent confirms it exists on dest_client
    coord.dest_client.get_torrent.return_value = Torrent(
        hash="existinghash", name="Show.Release", category="racing", save_path=str(fuse_dir), size_bytes=100, state="uploading", progress=1.0
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
    # (source repair upserts first, then the late arrival: two upserts).
    assert "existinghash" in ts.injected_private_hashes
    assert "sourcehash" in ts.injected_private_hashes
    assert coord.store.upsert.call_count == 2


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
            save_path="/home/kevin/torrents/qbittorrent",
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
async def test_reconcile_adopts_fuse_entry_with_missing_files_as_moving_when_on_ssd(tmp_path: Path):
    """Fuse-pointing entry + bytes on SSD = never-moved data: adopt MOVING fixed up."""
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()
    (ssd_dir / "Stranded.Movie.2026.mkv").write_bytes(b"s" * 100)

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.dest.save_path = ssd_dir
    cfg.ssd.path = ssd_dir
    cfg.rclone.fuse.mount = fuse_dir
    cfg.rclone.fuse.mount_unsorted = fuse_dir / "unsorted"

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="stranded_hash",
            name="Stranded.Movie.2026",
            size_bytes=100,
            save_path=str(fuse_dir),
            category="racing",
            progress=1.0,
            state="seeding",
        ),
    ]
    dest.get_torrent_files.return_value = [
        TorrentFile(name="Stranded.Movie.2026.mkv", size_bytes=100, progress=1.0),
    ]

    report = await reconcile(cfg, dest=dest, store=store)

    t = store.get("stranded_hash")
    assert t is not None
    assert t.state == State.MOVING
    assert t.save_path == str(ssd_dir)
    assert "stranded_hash" in report.kept


@pytest.mark.anyio
async def test_reconcile_keeps_done_when_fuse_files_present(tmp_path: Path):
    """Bytes verified behind the fuse path: adoption stays DONE."""
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Good.Movie.2026.mkv").write_bytes(b"g" * 100)

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.dest.save_path = tmp_path / "ssd"
    cfg.ssd.path = tmp_path / "ssd"
    cfg.rclone.fuse.mount = fuse_dir
    cfg.rclone.fuse.mount_unsorted = fuse_dir / "unsorted"

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="good_hash",
            name="Good.Movie.2026",
            size_bytes=100,
            save_path=str(fuse_dir),
            category="racing",
            progress=1.0,
            state="seeding",
        ),
    ]
    dest.get_torrent_files.return_value = [
        TorrentFile(name="Good.Movie.2026.mkv", size_bytes=100, progress=1.0),
    ]

    await reconcile(cfg, dest=dest, store=store)

    t = store.get("good_hash")
    assert t is not None
    assert t.state == State.DONE


@pytest.mark.anyio
async def test_reconcile_adopts_fuse_incomplete_as_re_adding(tmp_path: Path):
    """Fuse-pointing but incomplete entries must never adopt as DONE."""
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    store = StateStore(tmp_path / "test.db")

    cfg = MagicMock()
    cfg.dest.save_path = ssd_dir
    cfg.ssd.path = ssd_dir
    cfg.rclone.fuse.mount = fuse_dir
    cfg.rclone.fuse.mount_unsorted = fuse_dir / "unsorted"

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="partial_fuse_hash",
            name="Partial.Fuse.Release.2026",
            size_bytes=100,
            save_path=str(fuse_dir),
            category="racing",
            progress=0.5,
            state="downloading",
        ),
    ]

    await reconcile(cfg, dest=dest, store=store)

    t = store.get("partial_fuse_hash")
    assert t is not None
    assert t.state == State.RE_ADDING


@pytest.mark.anyio
async def test_reconcile_keeps_done_when_fuse_files_missing_everywhere(tmp_path: Path):
    """Files missing everywhere: keep DONE (mount may be warming), don't strand.

    Verification may only upgrade handling toward a verified-good path; a
    dead mount also shows nothing, and FAILED churn would cause re-downloads.
    Late injections stay gated downstream regardless.
    """
    from racing_sync.clients.abstract import TorrentFile

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    ssd_dir = tmp_path / "ssd"
    ssd_dir.mkdir()

    db_path = tmp_path / "test.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.dest.save_path = ssd_dir
    cfg.ssd.path = ssd_dir
    cfg.rclone.fuse.mount = fuse_dir
    cfg.rclone.fuse.mount_unsorted = fuse_dir / "unsorted"

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="ghost_hash",
            name="Ghost.Movie.2026",
            size_bytes=100,
            save_path=str(fuse_dir),
            category="racing",
            progress=1.0,
            state="seeding",
        ),
    ]
    dest.get_torrent_files.return_value = [
        TorrentFile(name="Ghost.Movie.2026.mkv", size_bytes=100, progress=1.0),
    ]

    await reconcile(cfg, dest=dest, store=store)

    t = store.get("ghost_hash")
    assert t is not None
    assert t.state == State.DONE


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
    name = "[SubsPlease] Show Name - 01 [1080p]"
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
async def test_reconcile_preserves_parked_waiting_seedpool(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    ts = TorrentState(
        source_infohash="seedpool_parked_hash",
        source_name="Parked.Torrent.Release",
        state=State.WAITING_SEEDPOOL,
        seedpool_attempts=2,
    )
    store.upsert(ts)

    dest = AsyncMock()
    dest.list_torrents.return_value = []  # Not present on VPS2

    report = await reconcile(cfg, dest=dest, store=store)

    # Must be considered safely resumed, NOT an orphan
    assert "seedpool_parked_hash" in report.resumed
    assert "seedpool_parked_hash" not in report.orphans
    assert len(report.orphans) == 0

    reloaded = store.get("seedpool_parked_hash")
    assert reloaded is not None
    assert reloaded.state == State.WAITING_SEEDPOOL
    assert reloaded.seedpool_attempts == 2
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


@pytest.mark.anyio
async def test_save_path_on_ssd_membership(tmp_path: Path):
    from racing_sync.recovery import _save_path_on_ssd

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    cfg = MagicMock()
    cfg.dest.save_path = ssd
    cfg.ssd.path = ssd

    assert _save_path_on_ssd(cfg, str(ssd)) is True
    assert _save_path_on_ssd(cfg, str(ssd / "Pack")) is True
    assert _save_path_on_ssd(cfg, "/mnt/fuse/torrents") is False
    assert _save_path_on_ssd(cfg, "") is False
    assert _save_path_on_ssd(cfg, "/etc") is False
    # Unresolvable roots (test doubles) never claim membership.
    assert _save_path_on_ssd(MagicMock(), str(ssd)) is False


@pytest.mark.anyio
async def test_reconcile_warns_when_adopting_rows(tmp_path: Path, caplog):
    import logging

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)
    ssd = tmp_path / "ssd"
    ssd.mkdir()

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")
    cfg.dest.save_path = ssd
    cfg.ssd.path = ssd

    dest = AsyncMock()
    dest.list_torrents.return_value = [
        Torrent(
            hash="p" * 40,
            name="Partial.Pack.S01",
            size_bytes=999,
            save_path=str(ssd),
            category="racing",
            progress=0.2,
            state="downloading",
        ),
    ]

    with caplog.at_level(logging.WARNING, logger="racing_sync.recovery"):
        report = await reconcile(cfg, dest=dest, store=store)

    assert report.adopted == ["p" * 40]
    assert any("forget" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_do_downloading_fresh_row_prioritizes_batch_zero(tmp_path: Path):
    """Adopted DOWNLOADING rows (batches_total=0) restart priorities at batch 0.

    Recovery adoptions skip QUEUED setup, so without this the client keeps
    downloading a stale batch selection while the loop waits on batch 0.
    """
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord._tg = None
    coord.store = MagicMock()
    coord.transition = MagicMock(side_effect=lambda t, s, **kw: setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.max_inflight_bytes = 10_000
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.dest_client = AsyncMock()
    files = [
        TorrentFile(name="Pack/a.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/b.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/c.bin", size_bytes=4000, progress=0.0),
    ]
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord._wait_for_completion = AsyncMock()
    coord._move_and_clean_batch = AsyncMock()

    ts = TorrentState(
        source_infohash="a" * 40,
        source_name="Pack",
        dest_infohash="a" * 40,
        save_path=str(ssd),
        total_bytes=12000,
        classification_kind="movie",
        batches_total=0,
        batch_index=0,
        state=State.DOWNLOADING,
    )

    await coord._do_downloading(ts)

    # Batches resolved on entry; the first priority map must select batch 0
    # only (a+b under a 10k cap) and the torrent must be resumed.
    assert ts.batches_total == 2
    first_map = coord.dest_client.set_file_priorities.call_args_list[0][0][1]
    assert first_map["Pack/a.bin"] == 1
    assert first_map["Pack/b.bin"] == 1
    assert first_map["Pack/c.bin"] == 0
    coord.dest_client.resume.assert_called()
    assert ts.state == State.MOVING



