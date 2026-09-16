from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from racing_sync.batcher import make_batches
from racing_sync.classifier import Episode


@pytest.fixture
def anyio_backend():
    return "asyncio"



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


def test_include_patterns_are_per_file():
    eps = [Episode("S01E01.mkv", 1, 1, 1), Episode("S01E02.mkv", 1, 2, 1)]
    b = make_batches(eps, cap_bytes=10)[0]
    pats = b.include_patterns()
    assert pats == ["--include=**/S01E01.mkv", "--include=**/S01E02.mkv"]


def test_include_patterns_escapes_glob_metacharacters():
    eps = [
        Episode("[SubsPlease] Show [1080p].mkv", 1, 1, 1),
        Episode("Show?Part{1}*test.mkv", 1, 2, 1),
    ]
    b = make_batches(eps, cap_bytes=10)[0]
    pats = b.include_patterns()
    assert pats == [
        r"--include=**/\[SubsPlease\] Show \[1080p\].mkv",
        r"--include=**/Show\?Part\{1\}\*test.mkv",
    ]


def test_include_patterns_subfolders_and_backslashes():
    from racing_sync.batcher import escape_rclone_glob

    # Direct escape_rclone_glob escapes backslashes
    assert escape_rclone_glob(r"dir\file*") == r"dir\\file\*"

    eps = [
        Episode("Season 1/S01E01.mkv", 1, 1, 100),
        Episode(r"Season 1\S01E02 [1080p].mkv", 1, 2, 100),
    ]
    b = make_batches(eps, cap_bytes=1000)[0]
    pats = b.include_patterns()
    assert pats == [
        "--include=**/Season 1/S01E01.mkv",
        r"--include=**/Season 1/S01E02 \[1080p\].mkv",
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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.classifier import Classification

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100_000_000
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.rclone.remote.default = "remote:TV"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    coord._season_folder_for = MagicMock(return_value=None)
    coord.store = MagicMock()
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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord.store = MagicMock()
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
    # _prepare_next_batch called 2 times (for batch 1 and 2)
    assert coord._prepare_next_batch.await_count == 2
    assert ts.batch_index == 3
    assert ts.state == State.MOVING
    assert coord.transition.called


@pytest.mark.anyio
async def test_batch_interleaved_download_move_and_clean(tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord.store = MagicMock()
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord._wait_for_completion = AsyncMock()
    coord._prepare_next_batch = AsyncMock()
    coord._rclone_move = AsyncMock()
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
    coord._effective_inflight_cap = MagicMock(return_value=10)

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
    # Client paused before move and resumed for next batch
    assert coord.dest_client.pause.await_count == 2
    assert coord.dest_client.resume.await_count == 1
    # Local files wiped after batch move
    assert not ep1_path.exists()
    assert not ep2_path.exists()
    assert ts.batch_index == 2
    assert ts.state == State.MOVING


@pytest.mark.anyio
async def test_coordinator_gate_uses_min_total_and_batch_cap():
    from unittest.mock import MagicMock, patch
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()

    # Total season is 100 GB, batch cap is 20 GB
    total_season_bytes = 100 * 1024 * 1024 * 1024
    batch_cap = 20 * 1024 * 1024 * 1024

    with patch("racing_sync.coordinator.ssd_max_inflight_bytes", return_value=batch_cap):
        effective = coord._effective_inflight_cap(total_season_bytes)
        assert effective == batch_cap

        # ssd_has_room is called with the batch cap (20GB), NOT the full 100GB
        ts = TorrentState(
            source_infohash="seasonhash",
            source_name="Big.Show.S01",
            total_bytes=total_season_bytes,
            state=State.WAITING_DISK,
        )

        with patch("racing_sync.coordinator.ssd_has_room") as mock_has_room:
            # Mock room only for 25 GB (enough for 20 GB batch cap, but NOT 100 GB)
            mock_has_room.side_effect = lambda cfg, needed: needed <= 25 * 1024 * 1024 * 1024

            await coord._wait_disk_then_queue(ts)

            # Should have transitioned to QUEUED because 20 GB <= 25 GB
            assert ts.state == State.QUEUED
            mock_has_room.assert_called_once_with(coord.cfg, batch_cap)


@pytest.mark.anyio
async def test_wait_for_completion_resolves_when_expected_files_complete():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState
    from racing_sync.clients.abstract import TorrentFile, Torrent

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord.cfg = MagicMock()
    coord.cfg.general.dest_poll_interval = 0.01
    coord.cfg.general.download_stall_timeout_seconds = 0

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

    # Batch only includes S01E01, which is at 100%
    f1 = TorrentFile(name="S01E01.mkv", size_bytes=1000, progress=1.0)
    f2 = TorrentFile(name="S01E02.mkv", size_bytes=1000, progress=0.0)
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[f1, f2])

    ts = TorrentState(source_infohash="hash1", source_name="Show.S01")
    # Waiting for only S01E01 should return immediately because S01E01 is complete
    await coord._wait_for_completion(ts, expected_files=["S01E01.mkv"])


@pytest.mark.anyio
async def test_do_moving_skips_move_when_already_batched(tmp_path):
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = object.__new__(Coordinator)
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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = object.__new__(Coordinator)
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
    coord._rclone_move = AsyncMock()

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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord._tg = None
    coord.store = MagicMock()
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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import Torrent, TorrentFile

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
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
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = object.__new__(Coordinator)
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
    coord._rclone_move = AsyncMock()

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
    Moving from src_dir with `<top>/**` preserves it.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = object.__new__(Coordinator)
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
    coord._rclone_move = AsyncMock()

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
    # Moved from src_dir (not the folder itself) with a top-preserving include.
    assert call.args[0] == tmp_path
    assert call.args[1] == "remote:media"
    assert call.kwargs.get("include") == ["--include=Show.S01/**"]
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_do_moving_fallback_never_bare_moves_folder(tmp_path):
    """Folder-detection fallback must still preserve the top dir.

    Regression (Vigil.S03 pack): when `_season_folder_for` finds nothing
    but `src_dir/<torrent>` exists on disk, a bare
    `rclone move <folder> <remote>` would upload the CONTENTS and land the
    episodes flat in the remote root. Must move from the parent with
    `<top>/**` instead.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import TorrentFile

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.remote.default = "remote:media"
    coord.dest_client = MagicMock()

    pack_folder = tmp_path / "Vigil.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Kitsune"
    pack_folder.mkdir()
    (pack_folder / "Vigil.S03E01.mkv").write_bytes(b"ep1 content")

    cls_file = TorrentFile(
        name="Vigil.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Kitsune/Vigil.S03E01.mkv",
        size_bytes=len(b"ep1 content"),
        progress=1.0,
        priority=1,
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[cls_file])
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord._rclone_move = AsyncMock()
    # Simulate folder detection finding nothing (e.g. odd file order).
    coord._season_folder_for = MagicMock(return_value=None)

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Vigil.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Kitsune",
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
    assert call.kwargs.get("include") == [
        "--include=Vigil.S03.1080p.iP.WEB-DL.AAC2.0.H.264-Kitsune/**"
    ]
    assert ts.state == State.RE_ADDING


@pytest.mark.anyio
async def test_batch_cap_bytes_single_helper():
    from racing_sync.coordinator import Coordinator
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.ssd.max_inflight_bytes = 50_000_000_000
    coord.cfg.general.disk_safety_margin_bytes = 0

    with patch("racing_sync.coordinator.ssd_max_inflight_bytes", return_value=50_000_000_000):
        assert coord._batch_cap_bytes() == 50_000_000_000


@pytest.mark.anyio
async def test_get_batches_respects_custom_episode_regex():
    import re
    from racing_sync.coordinator import Coordinator
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.state import TorrentState

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
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