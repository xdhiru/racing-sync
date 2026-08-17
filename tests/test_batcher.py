from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from conftest import make_coordinator

from racing_sync.batcher import make_batches
from racing_sync.classifier import Episode


def test_batches_fit_under_cap():
    eps = [
        Episode(f"S01E{i:02d}.mkv", 1, i, 2_000_000_000)
        for i in range(1, 11)  # 10 eps of 2 GiB
    ]
    cap = 6_000_000_000  # 6 GiB cap
    batches = make_batches(eps, cap_bytes=cap)
    for b in batches:
        assert b.size_bytes <= cap
    # 2 GiB x 10 = 20 GiB, cap 6 GiB -> should be at least 4 batches
    assert len(batches) >= 4
    # All episodes preserved
    assert sum(len(b.episodes) for b in batches) == 10


def test_single_oversize_episode_becomes_own_batch():
    big = Episode("S01E01.mkv", 1, 1, 10_000_000_000)
    small = Episode("S01E02.mkv", 1, 2, 1_000_000_000)
    batches = make_batches([big, small], cap_bytes=5_000_000_000)
    # big > cap so it gets its own batch
    assert any(b.size_bytes > 5_000_000_000 for b in batches)
    # small fits with itself
    assert any(len(b.episodes) == 1 and b.size_bytes == 1_000_000_000 for b in batches)


def test_batches_in_order():
    eps = [Episode(f"S01E{i:02d}.mkv", 1, i, 1_000_000_000) for i in range(1, 6)]
    batches = make_batches(eps, cap_bytes=3_000_000_000)
    seq: list[int] = []
    for b in batches:
        for e in b.episodes:
            seq.append(e.episode)
    assert seq == [1, 2, 3, 4, 5]


def test_file_names_are_per_file():
    eps = [Episode("S01E01.mkv", 1, 1, 1), Episode("S01E02.mkv", 1, 2, 1)]
    b = make_batches(eps, cap_bytes=10)[0]
    assert b.file_names() == ["S01E01.mkv", "S01E02.mkv"]


def test_file_names_need_no_glob_escaping():
    """--files-from-raw matches literally, so brackets need no escaping."""
    eps = [
        Episode("[DummySub] Show [1080p].mkv", 1, 1, 1),
        Episode("Show?Part{1}*test.mkv", 1, 2, 1),
    ]
    b = make_batches(eps, cap_bytes=10)[0]
    assert b.file_names() == [
        "[DummySub] Show [1080p].mkv",
        "Show?Part{1}*test.mkv",
    ]


def test_file_names_normalize_subfolders_and_backslashes():
    from racing_sync.batcher import escape_rclone_glob

    # escape_rclone_glob is still used by whole-folder <top>/** moves
    assert escape_rclone_glob(r"dir\file*") == r"dir\\file\*"

    eps = [
        Episode("Season 1/S01E01.mkv", 1, 1, 100),
        Episode(r"Season 1\S01E02 [1080p].mkv", 1, 2, 100),
    ]
    b = make_batches(eps, cap_bytes=1000)[0]
    assert b.file_names() == [
        "Season 1/S01E01.mkv",
        "Season 1/S01E02 [1080p].mkv",
    ]


def test_files_from_names_dedupes_and_skips_empties():
    from racing_sync.batcher import files_from_names

    assert files_from_names([
        "Top/Sub/a.mkv",
        "Top\\Sub\\a.mkv",
        "Top/c.mkv",
        "",
        "root.mkv",
    ]) == [
        "Top/Sub/a.mkv",
        "Top/c.mkv",
        "root.mkv",
    ]


def test_make_batches_caps_file_count():
    eps = [Episode(f"S01E{i:03d}.mkv", 1, i, 10) for i in range(1, 251)]
    # With a massive byte cap, batches should still be capped at max_files=100
    batches = make_batches(eps, cap_bytes=1_000_000, max_files=100)
    assert len(batches) == 3
    assert len(batches[0].episodes) == 100
    assert len(batches[1].episodes) == 100
    assert len(batches[2].episodes) == 50


def test_make_batches_empty_input():
    assert make_batches([], cap_bytes=1000) == []


@pytest.mark.anyio
async def test_moving_empty_episodes_raises_and_prevents_wipe(tmp_path):
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.classifier import Classification

    coord = make_coordinator()
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100_000_000
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.rclone.remote.default = "remote:TV"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord._season_folder_for = MagicMock(return_value=None)
    coord.transition = MagicMock()

    ts = TorrentState(
        source_infohash="testhash",
        source_name="Empty.Show.S01",
        classification_kind="mixed",
        batches_total=0,
        state=State.MOVING,
    )

    with patch(
        "racing_sync.coordinator.classify",
        return_value=Classification(kind="mixed", episodes=[], single_file=None, total_bytes=0),
    ):
        with pytest.raises(RuntimeError, match="no episodes found"):
            await coord._do_moving(ts)

    # Client delete and wipe must not be called
    coord.dest_client.delete.assert_not_called()
    coord.transition.assert_not_called()


@pytest.mark.anyio
async def test_do_downloading_iterates_batches():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord._wait_for_completion = AsyncMock()
    coord._prepare_next_batch = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))

    ts = TorrentState(
        source_infohash="testhash",
        source_name="Test.Show.S01",
        classification_kind="season",
        batches_total=3,
        batch_index=0,
        state=State.DOWNLOADING,
    )

    await coord._do_downloading(ts)

    # _wait_for_completion called 3 times (once per batch)
    assert coord._wait_for_completion.await_count == 3
    # Dummy path (no dest_client): no isolated reset, no priority flips —
    # batch_index still advances to MOVING.
    assert coord._prepare_next_batch.await_count == 0
    assert ts.batch_index == 3
    assert ts.state == State.MOVING
    assert coord.transition.called


@pytest.mark.anyio
async def test_batch_interleaved_download_move_and_clean(tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord._wait_for_completion = AsyncMock()
    coord._prepare_next_batch = AsyncMock()
    # Isolated batches: fresh delete+re-add between batches (mocked here;
    # covered end-to-end in test_isolated_reset_*).
    coord._reset_torrent_for_next_batch = AsyncMock(return_value=True)

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: listed files leave local disk.
        for name in files_from or []:
            p = save_dir / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)
    coord.dest_client = MagicMock()
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.resume = AsyncMock()

    # Create dummy files on disk in tmp_path
    save_dir = tmp_path / "downloads"
    save_dir.mkdir()
    ep1_path = save_dir / "S01E01.mkv"
    ep2_path = save_dir / "S01E02.mkv"
    ep1_path.write_bytes(b"ep1_data")
    ep2_path.write_bytes(b"ep2_data")

    # Mock get_torrent_files
    tf1 = TorrentFile(name="S01E01.mkv", size_bytes=8, progress=1.0)
    tf2 = TorrentFile(name="S01E02.mkv", size_bytes=8, progress=0.0)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[tf1, tf2])

    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = save_dir
    coord.cfg.rclone.remote.default = "remote:tv"
    coord.cfg.rclone.batch_move_extra_flags = []

    ts = TorrentState(
        source_infohash="testhash",
        source_name="Test.Show.S01",
        classification_kind="season",
        save_path=str(save_dir),
        batches_total=2,
        batch_index=0,
        state=State.DOWNLOADING,
    )

    await coord._do_downloading(ts)

    # Both batches moved via rclone
    assert coord._rclone_move.await_count == 2
    # Client paused before each move; isolated reset (not in-place priority
    # flip) prepares the next batch — resume happens inside the reset.
    assert coord.dest_client.pause.await_count == 2
    assert coord._reset_torrent_for_next_batch.await_count == 1
    # Batch files left local disk via the (simulated) rclone move
    assert not ep1_path.exists()
    assert not ep2_path.exists()
    assert ts.batch_index == 2
    assert ts.state == State.MOVING


@pytest.mark.anyio
async def test_coordinator_gate_uses_min_total_and_batch_cap():
    from unittest.mock import MagicMock, patch
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    # Global ledger budget uses the STABLE configured cap (not free-shrunk live cap).
    batch_cap = 20 * 1024 * 1024 * 1024
    coord.cfg.ssd.max_inflight_bytes = batch_cap

    # Total season is 100 GB, configured cap is 20 GB → estimate is one batch.
    total_season_bytes = 100 * 1024 * 1024 * 1024
    assert coord._ssd_estimate_for_new(total_season_bytes) == batch_cap

    # ssd_has_room is called with the estimate (20GB), NOT the full 100GB
    ts = TorrentState(
        source_infohash="seasonhash",
        source_name="Big.Show.S01",
        total_bytes=total_season_bytes,
        state=State.WAITING_DISK,
    )

    with patch("racing_sync.coordinator.ssd_has_room") as mock_has_room:
        # Mock room only for 25 GB (enough for 20 GB estimate, but NOT 100 GB)
        mock_has_room.side_effect = lambda cfg, needed: needed <= 25 * 1024 * 1024 * 1024

        await coord._wait_disk_then_queue(ts)

        # Should have transitioned to QUEUED because 20 GB <= 25 GB
        assert ts.state == State.QUEUED
        mock_has_room.assert_called_once_with(coord.cfg, batch_cap)


@pytest.mark.anyio
async def test_wait_for_completion_resolves_when_expected_files_complete(tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState
    from racing_sync.clients.abstract import TorrentFile, Torrent

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.general.dest_poll_interval = 0.01
    coord.cfg.general.download_stall_timeout_seconds = 0
    coord.cfg.dest.save_path = tmp_path

    t_item = Torrent(
        hash="hash1",
        name="Show.S01",
        size_bytes=2000,
        progress=0.5,  # Overall torrent only 50%
        state="downloading",
        category="racing",
        save_path="/tmp",
    )
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(return_value=t_item)

    # Batch only includes S01E01, which is at 100% AND present on SSD at
    # full size (completion requires both client progress and disk bytes).
    (tmp_path / "S01E01.mkv").write_bytes(b"x" * 1000)
    f1 = TorrentFile(name="S01E01.mkv", size_bytes=1000, progress=1.0)
    f2 = TorrentFile(name="S01E02.mkv", size_bytes=1000, progress=0.0)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[f1, f2])

    ts = TorrentState(source_infohash="hash1", source_name="Show.S01",
                      save_path=str(tmp_path))
    # Waiting for only S01E01 should return immediately because S01E01 is complete
    await coord._wait_for_completion(ts, expected_files=["S01E01.mkv"])


@pytest.mark.anyio
async def test_wait_for_completion_waits_when_bytes_missing_on_disk(tmp_path):
    """Client-complete but short on SSD keeps polling (desynced piece map)."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState
    from racing_sync.clients.abstract import TorrentFile, Torrent

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.general.dest_poll_interval = 0.01
    coord.cfg.general.download_stall_timeout_seconds = 0
    coord.cfg.dest.save_path = tmp_path

    calls = {"n": 0}

    async def _stop_after_second(_h):
        calls["n"] += 1
        if calls["n"] >= 2:
            coord._stop = True
        return Torrent(hash="hash1", name="Show.S01", size_bytes=2000,
                       progress=1.0, state="downloading", category="racing",
                       save_path="/tmp")

    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(side_effect=_stop_after_second)
    f1 = TorrentFile(name="S01E01.mkv", size_bytes=1000, progress=1.0)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[f1])

    ts = TorrentState(source_infohash="hash1", source_name="Show.S01",
                      save_path=str(tmp_path))
    # No S01E01.mkv on disk: must NOT return as complete on the first poll
    # (progress-only logic would); it keeps polling until stopped.
    await coord._wait_for_completion(ts, expected_files=["S01E01.mkv"])
    assert calls["n"] == 2


@pytest.mark.anyio
async def test_do_moving_sweep_moves_only_verified_complete_leftovers(tmp_path):
    """Piece-boundary partials must never reach the remote via the sweep.

    Regression: qBittorrent pre-allocates deselected files at full size, so
    a partial of a not-yet-processed episode looks "full" on disk while its
    progress is < 1. The old bare `<top>/**` sweep uploaded such corrupt
    data (potentially overwriting an older batch's moved file). The sweep
    must transfer only client-verified-complete files, leave the partial
    for the folder wipe, and still finish RE_ADDING.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    top = ssd / "Big.Show.S01"
    top.mkdir()
    partial = top / "Big.Show.S01E04.mkv"
    partial.write_bytes(b"e" * 500)  # preallocated full size...
    cover = top / "cover.jpg"
    cover.write_bytes(b"cover!")
    gone_locally = "Big.Show.S01/Big.Show.S01E01.mkv"  # moved by its batch

    cls_files = [
        TorrentFile(name=gone_locally, size_bytes=500, progress=1.0),
        TorrentFile(name="Big.Show.S01/Big.Show.S01E04.mkv", size_bytes=500, progress=0.4),
        TorrentFile(name="Big.Show.S01/cover.jpg", size_bytes=6, progress=1.0),
    ]

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=cls_files)
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: listed files leave local disk.
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="b" * 40,
        source_name="Big.Show.S01",
        dest_infohash="b" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=3,
        batch_index=3,  # all batches moved during downloading
        state=State.MOVING,
    )
    coord.store.upsert(ts)

    row = coord.store.get("b" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        await coord._do_moving(row)

    assert row.state == State.RE_ADDING
    # Exactly one sweep move, covering ONLY the verified-complete leftover.
    coord._rclone_move.assert_awaited_once()
    files_from = coord._rclone_move.call_args.kwargs.get("files_from")
    assert files_from == ["Big.Show.S01/cover.jpg"]
    # The preallocated partial was neither moved nor individually deleted.
    assert partial.exists()
    assert not cover.exists()


@pytest.mark.anyio
async def test_move_and_clean_batch_raises_when_files_remain(tmp_path):
    """A 0-transfer rclone move (exit 0, filters matched nothing) must not advance.

    Regression: the old unconditional local cleanup deleted batch files that
    never reached the remote, silently losing data while the batch was
    marked complete.
    """
    from racing_sync.coordinator import BatchMoveIncompleteError
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    ssd = tmp_path / "ssd"
    top = ssd / "Pack"
    top.mkdir(parents=True)
    (top / "a.mkv").write_bytes(b"data")

    coord = make_coordinator()
    coord.cfg.dest.save_path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord._rclone_move = AsyncMock()  # exit 0 but moves nothing

    ts = TorrentState(
        source_infohash="d" * 40,
        source_name="Pack",
        save_path=str(ssd),
        classification_kind="movie",
        state=State.DOWNLOADING,
    )
    batch = Batch(episodes=[Episode("Pack/a.mkv", 0, 1, 4)])

    with pytest.raises(BatchMoveIncompleteError):
        await coord._move_and_clean_batch(ts, batch)
    # Evidence preserved for retry — nothing deleted, nothing advanced.
    assert (top / "a.mkv").exists()


@pytest.mark.anyio
async def test_move_and_clean_batch_passes_when_rclone_moved_files(tmp_path):
    """When rclone really moved the files (gone locally), no error."""
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    ssd = tmp_path / "ssd"
    (ssd / "Pack").mkdir(parents=True)

    coord = make_coordinator()
    coord.cfg.dest.save_path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="e" * 40,
        source_name="Pack",
        save_path=str(ssd),
        classification_kind="movie",
        state=State.DOWNLOADING,
    )
    batch = Batch(episodes=[Episode("Pack/a.mkv", 0, 1, 4)])
    await coord._move_and_clean_batch(ts, batch)


@pytest.mark.anyio
async def test_do_moving_sweep_stays_moving_when_leftovers_stuck(tmp_path):
    """Sweep 0-transfer must not proceed to the folder wipe (data loss)."""
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    top = ssd / "Big.Show.S01"
    top.mkdir(parents=True)
    cover = top / "cover.jpg"
    cover.write_bytes(b"cover!")

    cls_files = [
        TorrentFile(name="Big.Show.S01/cover.jpg", size_bytes=6, progress=1.0),
    ]

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=cls_files)
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")
    coord._rclone_move = AsyncMock()  # exit 0 but moves nothing

    ts = TorrentState(
        source_infohash="f" * 40,
        source_name="Big.Show.S01",
        dest_infohash="f" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=3,
        batch_index=3,
        state=State.MOVING,
    )
    coord.store.upsert(ts)

    row = coord.store.get("f" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as wipe:
        await coord._do_moving(row)

    assert row.state == State.MOVING
    wipe.assert_not_called()
    assert cover.exists()


@pytest.mark.anyio
async def test_do_moving_skips_move_when_no_verified_leftovers(tmp_path):
    """All-remaining-are-partials: no rclone call at all, still RE_ADDING."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    top = ssd / "Big.Show.S01"
    top.mkdir()
    (top / "Big.Show.S01E04.mkv").write_bytes(b"e" * 500)

    cls_files = [
        TorrentFile(name="Big.Show.S01/Big.Show.S01E04.mkv", size_bytes=500, progress=0.4),
    ]

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=cls_files)
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")
    coord._rclone_move = AsyncMock()

    ts = TorrentState(
        source_infohash="c" * 40,
        source_name="Big.Show.S01",
        dest_infohash="c" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=3,
        batch_index=3,
        state=State.MOVING,
    )
    coord.store.upsert(ts)

    row = coord.store.get("c" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        await coord._do_moving(row)

    assert row.state == State.RE_ADDING
    coord._rclone_move.assert_not_called()


def test_make_batches_huge_season_many_small_nested_episodes():
    """900 GB pack of ~1 GB nested episodes under a 37 GiB cap.

    Every batch fits the cap, every episode lands in exactly one batch, and
    file lists keep full nested paths (remote layout == torrent layout).
    """
    from racing_sync.classifier import Episode

    eps = [
        Episode(f"Giant.S01/Part{i // 100 + 1:02d}/Giant.S01E{i:03d}.mkv", 1, i, 1_000_000_000)
        for i in range(1, 901)
    ]
    batches = make_batches(eps, cap_bytes=37 * 1024**3)
    per_batch = (37 * 1024**3) // 1_000_000_000  # whole 1 GB episodes per batch
    assert len(batches) == -(-900 // per_batch)
    seen: list[str] = []
    for b in batches:
        assert b.size_bytes <= 37 * 1024**3
        assert len(b.episodes) <= 100
        for name in b.file_names():
            assert name.startswith("Giant.S01/")
        seen.extend(e.file_name for e in b.episodes)
    assert sorted(seen) == sorted(e.file_name for e in eps)


@pytest.mark.anyio
async def test_do_moving_skips_move_when_already_batched(tmp_path):
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord._rclone_move = AsyncMock()

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Show.S01",
        classification_kind="season",
        batches_total=2,  # Multi-batch: batches were already moved during downloading
        batch_index=2,
        state=State.MOVING,
    )

    with patch("racing_sync.coordinator.classify") as mock_classify:
        mock_cls = MagicMock()
        mock_cls.kind = "season"
        mock_classify.return_value = mock_cls

        await coord._do_moving(ts)

        # _rclone_move should NOT be called because batches were already moved
        coord._rclone_move.assert_not_called()
        # Old torrent is deleted from client and state transitions to RE_ADDING
        coord.dest_client.delete.assert_called_once_with("hash1", delete_files=False)
        assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_do_moving_purges_only_own_temp_files_when_no_season_folder(tmp_path):
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.remote.default = "remote:media"
    coord.dest_client = MagicMock()

    own_file = tmp_path / "My.Movie.2024.1080p.mkv"
    own_file.write_bytes(b"x" * 100)
    own_temp = tmp_path / "My.Movie.2024.1080p.mkv.!qB"
    own_temp.write_bytes(b"temp")

    concurrent_temp1 = tmp_path / "Other.Download.2024.mkv.!qB"
    concurrent_temp1.write_bytes(b"other temp")
    concurrent_temp2 = tmp_path / "Another.Download.2024.mkv.parts"
    concurrent_temp2.write_bytes(b"parts")

    cls_file = TorrentFile(
        name="My.Movie.2024.1080p.mkv",
        size_bytes=100,
        progress=1.0,
        priority=1,
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[cls_file])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: the single file leaves local disk.
        if local.is_file():
            local.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="hash2",
        source_name="My.Movie.2024.1080p",
        classification_kind="movie",
        state=State.MOVING,
    )

    with patch("racing_sync.coordinator.classify") as mock_classify:
        mock_cls = MagicMock()
        mock_cls.kind = "movie"
        mock_cls.single_file = "My.Movie.2024.1080p.mkv"
        mock_classify.return_value = mock_cls

        await coord._do_moving(ts)

    # Torrent's own temporary file should be unlinked
    assert not own_temp.exists()
    # Concurrent downloads' temporary files must NOT be unlinked
    assert concurrent_temp1.exists()
    assert concurrent_temp2.exists()


@pytest.mark.anyio
async def test_do_downloading_falls_back_to_full_move_when_batch_unresolvable():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord._tg = None
    coord.dest_client = MagicMock()
    coord._get_batches_for_torrent = AsyncMock(return_value=[])
    coord._wait_for_completion = AsyncMock()

    ts = TorrentState(
        source_infohash="testhash",
        source_name="Test.Show.S01",
        classification_kind="season",
        batches_total=2,
        batch_index=0,
        state=State.DOWNLOADING,
    )

    # Transient batch-resolution failure must NOT raise/FAILED after
    # successful downloads; it falls back to a full move via MOVING.
    await coord._do_downloading(ts)

    # Batch index must NOT be advanced when batch move cannot be resolved
    assert ts.batch_index == 0
    coord.store.transition.assert_called_once()
    assert coord.store.transition.call_args[0][1] == State.MOVING


@pytest.mark.anyio
async def test_wait_for_completion_fails_fast_on_missing_expected_files():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import Torrent, TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.dest_client = MagicMock()
    coord.dest_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash="h1", name="Show", category="racing", save_path="",
            size_bytes=1000, state="downloading", progress=0.5, trackers=[],
        )
    )
    # Torrent metadata is loaded with only Ep01, but batch expects Ep02
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[
            TorrentFile(name="Show.S01E01.mkv", size_bytes=500, progress=1.0, priority=1)
        ]
    )

    ts = TorrentState(
        source_infohash="h1",
        source_name="Show.S01",
        classification_kind="season",
        state=State.DOWNLOADING,
    )

    with pytest.raises(RuntimeError, match="expected batch files missing from torrent"):
        await coord._wait_for_completion(ts, expected_files=["Show.S01E01.mkv", "Show.S01E02.mkv"])


@pytest.mark.anyio
async def test_do_moving_moves_remaining_files_when_batches_incomplete(tmp_path):
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.remote.default = "remote:media/"
    coord.dest_client = MagicMock()

    show_folder = tmp_path / "Show.S01"
    show_folder.mkdir()
    ep2_file = show_folder / "Show.S01E02.mkv"
    ep2_file.write_bytes(b"ep2 content")

    cls_file = TorrentFile(
        name="Show.S01/Show.S01E02.mkv",
        size_bytes=len(b"ep2 content"),
        progress=1.0,
        priority=1,
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[cls_file])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: listed files leave local disk.
        for name in files_from or []:
            p = tmp_path / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Show.S01",
        classification_kind="season",
        batches_total=2,
        batch_index=1,  # Only batch 1 finished; crash before final move
        state=State.MOVING,
    )

    with patch("racing_sync.coordinator.classify") as mock_classify, \
         patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        mock_cls = MagicMock()
        mock_cls.kind = "season"
        mock_cls.single_file = None
        mock_classify.return_value = mock_cls

        await coord._do_moving(ts)

    # Incomplete batches with remaining payload files MUST be moved before cleanup
    coord._rclone_move.assert_called_once()
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_do_moving_preserves_top_folder_on_remote(tmp_path):
    """Whole-folder moves must keep the top dir (same layout as batch moves).

    `rclone move <folder> <remote>` strips the top dir, so fuse re-adds
    (save_path/mount + torrent-relative names) would point at missing data.
    Moving verified per-file names from src_dir preserves it.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.remote.default = "remote:media"
    coord.dest_client = MagicMock()

    show_folder = tmp_path / "Show.S01"
    show_folder.mkdir()
    (show_folder / "Show.S01E01.mkv").write_bytes(b"ep1 content")

    cls_file = TorrentFile(
        name="Show.S01/Show.S01E01.mkv",
        size_bytes=len(b"ep1 content"),
        progress=1.0,
        priority=1,
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[cls_file])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: listed files leave local disk.
        for name in files_from or []:
            p = local / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Show.S01",
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        save_path=str(tmp_path),
        state=State.MOVING,
    )

    with patch("racing_sync.coordinator.classify") as mock_classify, \
         patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        mock_cls = MagicMock()
        mock_cls.kind = "season"
        mock_cls.single_file = None
        mock_cls.episodes = []
        mock_classify.return_value = mock_cls

        await coord._do_moving(ts)

    coord._rclone_move.assert_called_once()
    call = coord._rclone_move.call_args
    # Moved from src_dir (not the folder itself) with verified per-file names
    # (top dir preserved in the relative paths, never bare-moved).
    assert call.args[0] == tmp_path
    assert call.args[1] == "remote:media"
    assert call.kwargs.get("files_from") == ["Show.S01/Show.S01E01.mkv"]
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_do_moving_fallback_never_bare_moves_folder(tmp_path):
    """Folder-detection fallback must still preserve the top dir.

    Regression (Harbor.Lights.S03 pack): when `_season_folder_for` finds nothing
    but `src_dir/<torrent>` exists on disk, a bare
    `rclone move <folder> <remote>` would upload the CONTENTS and land the
    episodes flat in the remote root. Must move verified per-file names
    from src_dir instead.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.remote.default = "remote:media"
    coord.dest_client = MagicMock()

    pack_folder = tmp_path / "Harbor.Lights.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Raccoon"
    pack_folder.mkdir()
    (pack_folder / "Harbor.Lights.S03E01.mkv").write_bytes(b"ep1 content")

    cls_file = TorrentFile(
        name="Harbor.Lights.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Raccoon/Harbor.Lights.S03E01.mkv",
        size_bytes=len(b"ep1 content"),
        progress=1.0,
        priority=1,
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[cls_file])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        # Simulate a real rclone move: listed files leave local disk.
        for name in files_from or []:
            p = local / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)
    # Simulate folder detection finding nothing (e.g. odd file order).
    coord._season_folder_for = MagicMock(return_value=None)

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Harbor.Lights.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Raccoon",
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        save_path=str(tmp_path),
        state=State.MOVING,
    )

    with patch("racing_sync.coordinator.classify") as mock_classify, \
         patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        mock_cls = MagicMock()
        mock_cls.kind = "season"
        mock_cls.single_file = None
        mock_cls.episodes = []
        mock_classify.return_value = mock_cls

        await coord._do_moving(ts)

    coord._rclone_move.assert_called_once()
    call = coord._rclone_move.call_args
    # Moved from the PARENT (not the folder itself) with top-preserving include.
    assert call.args[0] == tmp_path
    assert call.args[1] == "remote:media"
    assert call.kwargs.get("files_from") == [
        "Harbor.Lights.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Raccoon/Harbor.Lights.S03E01.mkv"
    ]
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_batch_cap_bytes_single_helper():
    coord = make_coordinator()
    coord.cfg.ssd.max_inflight_bytes = 50_000_000_000
    coord.cfg.general.disk_safety_margin_bytes = 0

    with patch("racing_sync.coordinator.ssd_max_inflight_bytes", return_value=50_000_000_000):
        assert coord._batch_cap_bytes() == 50_000_000_000


@pytest.mark.anyio
async def test_frozen_batch_cap_stable_across_free_space_changes():
    """Batch cap must freeze per row so boundaries never shift mid-download."""
    from racing_sync.state import TorrentState

    coord = make_coordinator()
    coord._batch_cap_cache = {}
    coord._batch_cap_bytes = MagicMock(return_value=10_000_000_000)
    ts = TorrentState(source_infohash="frozen_cap_hash")

    assert coord._frozen_batch_cap(ts) == 10_000_000_000
    # Free space drops: dynamic helper would shrink, frozen must not.
    coord._batch_cap_bytes = MagicMock(return_value=1_000_000_000)
    assert coord._frozen_batch_cap(ts) == 10_000_000_000


@pytest.mark.anyio
async def test_get_batches_respects_custom_episode_regex():
    import re
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.state import TorrentState

    coord = make_coordinator()
    # Custom episode regex for e.g. "Show - 01.mkv" (no 'S01E' prefix)
    coord.cfg.classifier._episode_re = re.compile(r"Show\s+-\s+(\d+)")
    coord._batch_cap_bytes = MagicMock(return_value=10_000_000_000)

    files = [
        TorrentFile(name="Show - 01.mkv", size_bytes=1000, progress=1.0),
        TorrentFile(name="Show - 02.mkv", size_bytes=1000, progress=1.0),
    ]
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files.return_value = files

    ts = TorrentState(source_infohash="custom_ep_hash", total_bytes=2000)
    batches = await coord._get_batches_for_torrent(ts)
    assert len(batches) == 1
    assert len(batches[0].episodes) == 2
    assert batches[0].episodes[0].file_name == "Show - 01.mkv"


@pytest.mark.anyio
async def test_fuse_skipped_matches_size_only(tmp_path):
    """Only size-verified fuse files are skipped; missing/short ones download."""

    fuse = tmp_path / "fuse"
    (fuse / "Pack").mkdir(parents=True)
    (fuse / "Pack" / "a.mkv").write_bytes(b"x" * 100)
    (fuse / "Pack" / "b.mkv").write_bytes(b"short")

    coord = make_coordinator()
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.dest.save_path = tmp_path / "ssd"

    skipped = await coord._fuse_skipped(
        [("Pack/a.mkv", 100), ("Pack/b.mkv", 100), ("Pack/missing.mkv", 50)],
        "movie",
    )
    assert skipped == {"Pack/a.mkv"}
    # Episodes route at the unsorted mount, which is empty here.
    assert await coord._fuse_skipped([("Pack/a.mkv", 100)], "episode") == set()


@pytest.mark.anyio
async def test_wait_for_completion_empty_expected_returns_immediately():
    """A fully-skipped batch waits on nothing (and fetches nothing)."""
    from unittest.mock import AsyncMock
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()

    ts = TorrentState(source_infohash="e" * 40, state=State.DOWNLOADING)
    await coord._wait_for_completion(ts, expected_files=[])

    coord.dest_client.get_torrent.assert_not_called()


@pytest.mark.anyio
async def test_move_and_clean_batch_honors_skip(tmp_path):
    """Already-remote batch members are neither moved nor verified."""
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    ssd = tmp_path / "ssd"
    (ssd / "Pack").mkdir(parents=True)
    (ssd / "Pack" / "b.mkv").write_bytes(b"data")

    coord = make_coordinator()
    coord.cfg.dest.save_path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = local / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="f" * 40,
        source_name="Pack",
        save_path=str(ssd),
        classification_kind="movie",
        state=State.DOWNLOADING,
    )
    batch = Batch(episodes=[
        Episode("Pack/a.mkv", 0, 1, 4),
        Episode("Pack/b.mkv", 0, 2, 4),
    ])
    await coord._move_and_clean_batch(ts, batch, skip={"Pack/a.mkv"})

    files_from = coord._rclone_move.call_args.kwargs.get("files_from")
    assert files_from == ["Pack/b.mkv"]
    assert not (ssd / "Pack" / "b.mkv").exists()


@pytest.mark.anyio
async def test_setup_queued_download_skips_remote_batch0(tmp_path):
    """Batch-0 members already on the remote stay deselected from the start."""
    from racing_sync.clients.abstract import AddResult, TorrentFile
    from racing_sync.state import TorrentState, State

    fuse = tmp_path / "fuse"
    (fuse / "Pack").mkdir(parents=True)
    (fuse / "Pack" / "a.bin").write_bytes(b"x" * 4000)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 10_000
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="dd" * 20, accepted=True, detail="Ok.")
    )
    files = [
        TorrentFile(name="Pack/a.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/b.bin", size_bytes=4000, progress=0.0),
        TorrentFile(name="Pack/c.bin", size_bytes=4000, progress=0.0),
    ]
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="e" * 40, source_name="Pack", total_bytes=12000,
        cross_seed_blob=b"blob", save_path=str(tmp_path), state=State.QUEUED,
    )

    await coord._do_queued(ts)

    assert ts.state == State.DOWNLOADING
    assert ts.batches_total == 2
    prio_map = coord.dest_client.set_file_priorities.call_args[0][1]
    assert prio_map["Pack/a.bin"] == 0
    assert prio_map["Pack/b.bin"] == 1
    assert prio_map["Pack/c.bin"] == 0
    coord.dest_client.resume.assert_called_once()


@pytest.mark.anyio
async def test_setup_queued_download_single_on_fuse_goes_moving(tmp_path):
    """A single file already archived needs no SSD download at all."""
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "Movie.mkv").write_bytes(b"x" * 100)

    coord = make_coordinator()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.skip_movie_larger_than_bytes = 50
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Movie.mkv", size_bytes=100, progress=0.0)]
    )
    coord.dest_client.resume = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash="f" * 40, source_name="Movie", total_bytes=100,
        dest_infohash="f" * 40, save_path=str(tmp_path), state=State.QUEUED,
    )

    await coord._setup_queued_download(ts, b"blob")

    # On fuse despite exceeding the skip cap: no SSD needed, no failure.
    assert ts.state == State.MOVING
    coord.dest_client.resume.assert_not_called()
    coord.dest_client.delete.assert_not_called()


@pytest.mark.anyio
async def test_do_moving_mixed_skips_remote_episode(tmp_path):
    """Mixed redo: the already-archived episode is not moved again."""
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    fuse_unsorted = tmp_path / "fuse-unsorted"
    fuse_unsorted.mkdir()
    (fuse_unsorted / "Show.S01E01.mkv").write_bytes(b"x" * 1000)
    for name in ("Show.S01E01.mkv", "Show.S01E02.mkv", "Extra1.mp4", "Extra2.mp4"):
        (tmp_path / name).write_bytes(b"x" * 1000)

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 1500
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = tmp_path / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = fuse_unsorted
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile("Show.S01E01.mkv", 1000, progress=1.0),
        TorrentFile("Show.S01E02.mkv", 1000, progress=1.0),
        TorrentFile("Extra1.mp4", 1000, progress=1.0),
        TorrentFile("Extra2.mp4", 1000, progress=1.0),
    ])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = local / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="m" * 40, source_name="Show.Mixed", save_path=str(tmp_path),
        classification_kind="mixed", state=State.MOVING,
    )

    await coord._do_moving(ts)

    # E01 already archived: only E02 moves; E01 stays untouched locally.
    assert coord._rclone_move.await_count == 1
    moved = coord._rclone_move.call_args.kwargs.get("files_from")
    assert moved == ["Show.S01E02.mkv"]
    assert (tmp_path / "Show.S01E01.mkv").exists()
    assert not (tmp_path / "Show.S01E02.mkv").exists()
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_move_and_clean_batch_all_skipped_skips_rclone(tmp_path):
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    coord = make_coordinator()
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord._rclone_move = AsyncMock()

    ts = TorrentState(
        source_infohash="g" * 40,
        source_name="Pack",
        save_path=str(ssd),
        classification_kind="movie",
        state=State.DOWNLOADING,
    )
    batch = Batch(episodes=[Episode("Pack/a.mkv", 0, 1, 4)])
    await coord._move_and_clean_batch(ts, batch, skip={"Pack/a.mkv"})

    coord._rclone_move.assert_not_called()


@pytest.mark.anyio
async def test_isolated_reset_deletes_with_files_and_reads_next_batch(tmp_path):
    """Fresh delete+re-add isolates batches so only complete files move."""
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult, TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.rclone.fuse.mount = tmp_path / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="b" * 40, accepted=True, detail="Ok.")
    )
    files = [
        TorrentFile(name="Show.S01E01.mkv", size_bytes=8, progress=0.0),
        TorrentFile(name="Show.S01E02.mkv", size_bytes=8, progress=0.0),
    ]
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord._fuse_skipped = AsyncMock(return_value=set())
    coord._frozen_batch_cap = MagicMock(return_value=8)
    coord._await_hash_for_name = AsyncMock(return_value=None)

    ts = TorrentState(
        source_infohash="a" * 40,
        source_name="Show",
        dest_infohash="a" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=2,
        batch_index=0,
        state=State.DOWNLOADING,
    )
    ts._blob = b"fake-torrent-bytes"
    ts.cross_seed_blob = b"fake-torrent-bytes"

    ok = await coord._reset_torrent_for_next_batch(ts, 1)

    assert ok == 1
    # Old entry removed WITH files to clear shared-piece partials.
    coord.dest_client.delete.assert_awaited_once()
    assert coord.dest_client.delete.call_args.kwargs.get("delete_files") is True
    # Fresh paused re-add, then only next batch selected.
    coord.dest_client.add_torrent.assert_awaited_once()
    assert coord.dest_client.add_torrent.call_args.kwargs.get("paused") is True
    prio = coord.dest_client.set_file_priorities.call_args[0][1]
    assert prio["Show.S01E02.mkv"] == 1
    assert prio["Show.S01E01.mkv"] == 0
    coord.dest_client.resume.assert_awaited_once()
    assert ts.dest_infohash == "b" * 40


@pytest.mark.anyio
async def test_isolated_reset_missing_blob_parks_without_delete():
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord.store.get_blob = MagicMock(return_value=b"")
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()

    ts = TorrentState(
        source_infohash="c" * 40,
        source_name="Show",
        classification_kind="season",
        batches_total=2,
        batch_index=0,
        state=State.DOWNLOADING,
    )
    ts._blob = b""
    ts.cross_seed_blob = b""

    ok = await coord._reset_torrent_for_next_batch(ts, 1)

    assert ok is False
    coord.dest_client.delete.assert_not_called()
    coord.dest_client.add_torrent.assert_not_called()
@pytest.mark.anyio
async def test_park_moving_records_reason_and_escalates(tmp_path, caplog):
    """MOVING parks must name their gate instead of idling silently.

    Regression (core stall): _do_moving early returns parked with only a
    WARNING, so a gate that never passes (unverifiable pause, 0-transfer
    rclone) idled forever as plain "MOVING". Parks now stamp last_error
    and escalate to ERROR after consecutive parks.
    """
    import logging
    from racing_sync.state import StateStore, TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord.store = StateStore(tmp_path / "state.db")
    ts = TorrentState(source_infohash="e" * 40, source_name="Stalled.Show",
                      state=State.MOVING)
    coord.store.upsert(ts)

    with caplog.at_level(logging.WARNING):
        for _ in range(4):
            coord._park_moving(ts, "could not pause torrent eeee before move")
    assert "(4x)" in (coord.store.get("e" * 40).last_error or "")
    assert not any(r.levelno >= logging.ERROR for r in caplog.records)

    coord._park_moving(ts, "could not pause torrent eeee before move")
    assert "(5x)" in (coord.store.get("e" * 40).last_error or "")
    assert any(r.levelno >= logging.ERROR and "MOVING stalled" in r.message
               for r in caplog.records)

    # Leaving MOVING resets the counter (fresh re-entry starts at 1x).
    coord.transition(ts, State.RE_ADDING)
    assert ts.state == State.RE_ADDING
    assert ts.last_error == ""
    coord._park_moving(ts, "boom")
    assert "(1x)" in (coord.store.get("e" * 40).last_error or "")


@pytest.mark.anyio
async def test_do_moving_branches_on_pinned_kind_despite_flip(tmp_path):
    """A mid-flight classification flip must not reroute the move.

    Regression: routing was pinned to the QUEUED-time kind but the move
    branch still followed the fresh file list, so extras appearing
    mid-flight (season -> mixed) rewrote batch cursors via the mixed
    branch. The move now branches on the pinned kind with the fresh
    layout: cursors untouched, leftovers swept, RE_ADDING reached.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from pathlib import Path
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    top = ssd / "Pack"
    top.mkdir(parents=True)
    # 9 episodes + 2 extras: 9/11 < 90% -> fresh classify is "mixed",
    # while QUEUED saw the pack as a season.
    names = [f"Pack.S01E{i:02d}.mkv" for i in range(1, 10)] + ["Extra1.mp4", "Extra2.mp4"]
    for n in names:
        (top / n).write_bytes(b"x" * 100)
    cls_files = [TorrentFile(name=f"Pack/{n}", size_bytes=100, progress=1.0)
                 for n in names]

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=cls_files)
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.export_torrent = AsyncMock(return_value=b"blob")

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(source_infohash="f" * 40, source_name="Pack",
                      dest_infohash="f" * 40, save_path=str(ssd),
                      classification_kind="season", batches_total=0,
                      batch_index=0, state=State.MOVING)
    coord.store.upsert(ts)
    row = coord.store.get("f" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        await coord._do_moving(row)

    assert row.state == State.RE_ADDING
    # Sweep branch (pinned season): cursors untouched, no batch extras.
    assert row.batches_total == 0
    coord._rclone_move.assert_awaited_once()
    assert coord._rclone_move.call_args.kwargs.get("extra") is None

@pytest.mark.anyio
async def test_do_moving_timeout_parks_instead_of_failing(tmp_path):
    """A hung rclone move must park in MOVING, never FAIL (bytes intact).

    Live incident: 3 `rclone move` to remotedrive: hung with zero output;
    6 MOVING workers wedged (moves=6/3 every tick), SSD never freed, fuse
    never injected. Timeouts now park for retry with the reason recorded.
    """
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.rclone_ops import RcloneTimeoutError
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    (ssd / "Show.S01E01.mkv").write_bytes(b"x" * 100)

    coord = make_coordinator()
    coord._stop = False
    coord.store = StateStore(tmp_path / "state.db")
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=100, progress=1.0),
    ])

    coord._rclone_move = AsyncMock(
        side_effect=RcloneTimeoutError("rclone timeout after 21600s"))
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(source_infohash="t" * 40, source_name="Show.S01E01",
                      dest_infohash="t" * 40, save_path=str(ssd),
                      classification_kind="episode", batches_total=0,
                      state=State.MOVING)
    coord.store.upsert(ts)

    await coord._process_torrent(coord.store.get("t" * 40))

    row = coord.store.get("t" * 40)
    assert row.state == State.MOVING  # parked, not FAILED
    assert "timed out" in (row.last_error or "")
    # Source bytes untouched by the terminated child.
    assert (ssd / "Show.S01E01.mkv").exists()


@pytest.mark.anyio
async def test_batch_loop_timeout_parks_downloading_without_retry_burn(tmp_path):
    """A batch-move timeout parks at once; same-tick retries would each
    block for the full timeout again."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.rclone_ops import RcloneTimeoutError
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()
    coord._get_batches_for_torrent = AsyncMock(return_value=[
        Batch(episodes=[Episode("Show.S01E01.mkv", 1, 1, 100)]),
        Batch(episodes=[Episode("Show.S01E02.mkv", 1, 2, 100)]),
    ])
    coord._wait_for_completion = AsyncMock()
    coord._fuse_skipped = AsyncMock(return_value=set())
    coord._move_and_clean_batch = AsyncMock(
        side_effect=RcloneTimeoutError("rclone timeout after 21600s"))

    ts = TorrentState(source_infohash="u" * 40, source_name="Show.S01",
                      classification_kind="season", batches_total=2,
                      batch_index=0, state=State.DOWNLOADING)

    await coord._do_downloading(ts)

    assert ts.state == State.DOWNLOADING
    assert coord._move_and_clean_batch.await_count == 1  # no retry burn
    assert "timed out" in (ts.last_error or "")
    coord.transition.assert_not_called()


@pytest.mark.anyio
async def test_process_torrent_timeout_safety_net_parks(tmp_path):
    """A timeout leaking from any future path still parks, never FAILs."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.rclone_ops import RcloneTimeoutError
    from racing_sync.state import StateStore, TorrentState, State

    coord = make_coordinator()
    coord._stop = False
    coord.store = StateStore(tmp_path / "state.db")
    coord._process_torrent_inner = AsyncMock(
        side_effect=RcloneTimeoutError("rclone timeout after 21600s"))

    ts = TorrentState(source_infohash="v" * 40, source_name="Show",
                      state=State.DOWNLOADING)
    coord.store.upsert(ts)

    await coord._process_torrent(coord.store.get("v" * 40))

    row = coord.store.get("v" * 40)
    assert row.state == State.DOWNLOADING
    assert "timed out" in (row.last_error or "")

@pytest.mark.anyio
async def test_queued_burst_respects_download_cap(tmp_path):
    """A fresh-start burst of NEW workers must not exceed max_active_downloads.

    Live incident: 12 workers scheduled for state=new rows sailed past the
    tick gate (snapshot-QUEUED-only) into 5 concurrent DOWNLOADING against
    max 3. The QUEUED edge now parks extras; reservations stay intact and
    parked rows proceed without re-adding once slots free.
    """
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import StateStore, TorrentState, State
    from racing_sync.clients.abstract import TorrentFile, AddResult

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()

    coord = make_coordinator()
    coord._stop = False
    coord._running_infohashes = set()  # tick-managed live set, mirrored here
    coord.store = StateStore(tmp_path / "state.db")
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.ssd.max_inflight_bytes = 100_000_000_000
    coord.cfg.max_active_downloads = 3
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])  # brand-new
    added: list[str] = []

    async def _fake_add(**kw):
        h = f"{len(added) + 10:040d}"
        added.append(h)
        return AddResult(hash=h, accepted=True, detail="ok")

    coord.dest_client.add_torrent = AsyncMock(side_effect=_fake_add)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=100, progress=0.0),
    ])
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()

    rows = []
    for i in range(5):
        ts = TorrentState(source_infohash=f"{i + 1:040d}", source_name=f"Show{i}",
                          save_path=str(ssd), total_bytes=100,
                          classification_kind="unknown",
                          cross_seed_blob=b"blob",
                          state=State.QUEUED)
        coord.store.upsert(ts)
        rows.append(ts)

    async def _run_queued(key: str) -> None:
        # Mirror the tick: schedule (live set) -> worker -> done (discard
        # when the worker returns without continuing into downloading).
        coord._running_infohashes.add(key)
        try:
            await coord._do_queued(coord.store.get(key))
        finally:
            row = coord.store.get(key)
            if row is None or row.state != State.DOWNLOADING:
                coord._running_infohashes.discard(key)

    for ts in rows:
        await _run_queued(ts.source_infohash)

    states = [coord.store.get(ts.source_infohash).state for ts in rows]
    assert states.count(State.DOWNLOADING) == 3
    assert states.count(State.QUEUED) == 2
    # Parked rows added nothing and hold no client entry.
    assert len(added) == 3

    # Two slots free up -> parked rows proceed exactly once each, no churn.
    drained = 0
    for ts in rows:
        r = coord.store.get(ts.source_infohash)
        if r.state == State.DOWNLOADING and drained < 2:
            coord.transition(r, State.MOVING)
            coord._running_infohashes.discard(r.source_infohash.lower())
            drained += 1
    for ts in rows:
        r = coord.store.get(ts.source_infohash)
        if r.state == State.QUEUED:
            await _run_queued(r.source_infohash)
    states = [coord.store.get(ts.source_infohash).state for ts in rows]
    assert states.count(State.DOWNLOADING) == 3
    assert len(added) == 5
    assert len(set(added)) == 5


@pytest.mark.anyio
async def test_queued_existing_entry_parks_when_full(tmp_path):
    """The resume path for already-added entries obeys the cap too."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore, TorrentState, State

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()

    coord = make_coordinator()
    coord._stop = False
    coord.store = StateStore(tmp_path / "state.db")
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.max_active_downloads = 1
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[
        Torrent(hash="e" * 40, name="Show", category="racing", save_path=str(ssd),
                size_bytes=100, state="pausedDL", progress=0.5),
    ])
    coord.dest_client.resume = AsyncMock()

    # One DOWNLOADING row already fills the single slot.
    coord.store.upsert(TorrentState(source_infohash="d" * 40, source_name="Busy",
                                    state=State.DOWNLOADING))
    ts = TorrentState(source_infohash="e" * 40, source_name="Show",
                      dest_infohash="e" * 40, save_path=str(ssd),
                      cross_seed_blob=b"blob",
                      state=State.QUEUED)
    coord.store.upsert(ts)

    coord._running_infohashes.add("e" * 40)
    await coord._do_queued(coord.store.get("e" * 40))

    row = coord.store.get("e" * 40)
    assert row.state == State.QUEUED
    coord.dest_client.resume.assert_not_called()

    # Drain -> resume proceeds, exactly once.
    busy = coord.store.get("d" * 40)
    coord.transition(busy, State.MOVING)
    await coord._do_queued(coord.store.get("e" * 40))
    assert coord.store.get("e" * 40).state == State.DOWNLOADING
    coord.dest_client.resume.assert_awaited_once()


def test_queued_gate_ignores_non_int_configs():
    """MagicMock test doubles must never report a full cap."""
    from unittest.mock import MagicMock

    coord = make_coordinator()
    coord.cfg = MagicMock()  # max_active_downloads is a MagicMock
    assert coord._try_admit_download(MagicMock()) is True
    assert coord._park_queued_for_download_slot(MagicMock()) is False

@pytest.mark.anyio
async def test_setup_footprint_excludes_remote_skipped_bytes(tmp_path):
    """Reservation must cover remaining work, not the full torrent.

    Live case: a 32 GB season with most batches already moved held 32 GB
    while only ~12 GB still needed SSD, blocking a 16 GB waiter. The setup
    footprint now subtracts fuse-present bytes.
    """
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    # 2 of 3 episodes already on the remote (exact sizes).
    (fuse / "Show.S01E01.mkv").write_bytes(b"x" * 100)
    (fuse / "Show.S01E02.mkv").write_bytes(b"x" * 100)

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.ssd.max_inflight_bytes = 100_000
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse
    coord._batch_cap_cache = {}
    coord._ssd_reserved = {}
    coord._ssd_lock = None
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=100, progress=1.0),
        TorrentFile(name="Show.S01E02.mkv", size_bytes=100, progress=1.0),
        TorrentFile(name="Show.S01E03.mkv", size_bytes=100, progress=0.0),
    ])
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(source_infohash="s" * 40, source_name="Show.S01",
                      dest_infohash="s" * 40, save_path=str(ssd),
                      total_bytes=300, state=State.QUEUED)
    # Admission held the full 300; setup must shrink to the ~100 remaining.
    coord._ssd_reserved["s" * 40] = 300

    await coord._setup_queued_download(ts, b"blob")

    assert ts.state == State.DOWNLOADING
    assert coord._ssd_reserved["s" * 40] == 100


@pytest.mark.anyio
async def test_setup_footprint_zero_when_fully_remote(tmp_path):
    """Everything already moved -> hold nothing, still finish to MOVING."""
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "Show.S01E01.mkv").write_bytes(b"x" * 100)
    (fuse / "Show.S01E02.mkv").write_bytes(b"x" * 100)

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.ssd.max_inflight_bytes = 100_000
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse
    coord._batch_cap_cache = {}
    coord._ssd_reserved = {}
    coord._ssd_lock = None
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="Show.S01E01.mkv", size_bytes=100, progress=1.0),
        TorrentFile(name="Show.S01E02.mkv", size_bytes=100, progress=1.0),
    ])
    coord.dest_client.set_file_priorities = AsyncMock()
    coord.dest_client.resume = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(source_infohash="z" * 40, source_name="Show.S01",
                      dest_infohash="z" * 40, save_path=str(ssd),
                      total_bytes=200, state=State.QUEUED)
    coord._ssd_reserved["z" * 40] = 200

    await coord._setup_queued_download(ts, b"blob")

    assert ts.state == State.DOWNLOADING
    assert coord._ssd_reserved["z" * 40] == 0
    coord.dest_client.resume.assert_not_called()  # nothing needs downloading

@pytest.mark.anyio
async def test_download_loop_tightens_reservation_per_batch(tmp_path):
    """Completed batches must stop blocking waiters mid-season.

    After batch 0 of 2 moves, the reservation shrinks to the remaining
    batches (not the original max), freeing budget while the season is
    still downloading.
    """
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.state import TorrentState, State
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    coord = make_coordinator()
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda t, s, error="": setattr(t, "state", s))
    coord.cfg = MagicMock()
    coord.dest_client = AsyncMock()
    coord._get_batches_for_torrent = AsyncMock(return_value=[
        Batch(episodes=[Episode("Show.S01E01.mkv", 1, 1, 100)]),
        Batch(episodes=[Episode("Show.S01E02.mkv", 1, 2, 100)]),
    ])
    coord._wait_for_completion = AsyncMock()
    # cur_skip (batch 0, nothing remote yet), adjust (batch 0 now remote),
    # cur_skip (batch 1).
    coord._fuse_skipped = AsyncMock(side_effect=[
        set(), {"Show.S01E01.mkv"}, set(),
    ])
    coord._move_and_clean_batch = AsyncMock()
    coord._reset_torrent_for_next_batch = AsyncMock(side_effect=lambda ts, nxt: nxt)
    coord._ssd_reserved = {"k" * 40: 200}
    coord._ssd_lock = None

    ts = TorrentState(source_infohash="k" * 40, source_name="Show.S01",
                      classification_kind="season", batches_total=2,
                      batch_index=0, state=State.DOWNLOADING)

    await coord._do_downloading(ts)

    assert ts.state == State.MOVING
    assert coord._ssd_reserved["k" * 40] == 100
