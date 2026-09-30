"""Tests for download settle handling: lag recheck and park-escalation throttle."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.clients.abstract import TorrentFile


def _moving_single(tmp_path, *, progress=0.0):
    """Single-file MOVING row, full bytes on disk, lagging client progress."""
    from racing_sync.clients.abstract import Torrent

    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    (ssd / "Solo.mkv").write_bytes(b"x" * 1000)
    files = [TorrentFile(name="Solo.mkv", size_bytes=1000,
                         progress=progress, priority=1)]

    coord = make_coordinator()
    coord._stop = False
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
    coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
    coord._rclone_move = AsyncMock()
    ts = TorrentState(
        source_infohash="s" * 40, source_name="Solo",
        dest_infohash="s" * 40, save_path=str(ssd),
        classification_kind="movie",
        batches_total=1, batch_index=0, state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_settle_recheck_fires_once_then_parks(tmp_path):
    """Full bytes + lagging progress: one force recheck, then park (no wipe)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    row = store.get("s" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree",
               new_callable=AsyncMock) as mock_wipe:
        await coord._do_moving(row)
        assert row.state == State.MOVING
        coord.dest_client.recheck.assert_awaited_once_with("s" * 40)
        mock_wipe.assert_not_awaited()
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
        # Second tick: no second recheck (once per row), still parked.
        await coord._do_moving(store.get("s" * 40))
        assert coord.dest_client.recheck.await_count == 1
        assert store.get("s" * 40).state == State.MOVING
    store.close()


@pytest.mark.anyio
async def test_no_recheck_when_file_short(tmp_path):
    """Genuinely short file: pre-existing missing-file path, no recheck.

    Short files are unlinked as incomplete partials before the branch, so
    the move cannot find them (FileNotFoundError, pre-existing behavior).
    The settle recheck must not fire there — rechecking cannot create
    bytes, and the wipe below is never reached either.
    """
    coord, store = _moving_single(tmp_path, progress=0.0)
    (tmp_path / "ssd" / "Solo.mkv").write_bytes(b"x" * 100)  # short
    row = store.get("s" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree",
               new_callable=AsyncMock) as mock_wipe:
        with pytest.raises(FileNotFoundError):
            await coord._do_moving(row)
        coord.dest_client.recheck.assert_not_called()
        mock_wipe.assert_not_awaited()
    store.close()


def test_park_escalation_throttles_after_first_error(caplog):
    """_park_moving: ERROR at 5, then debug until every 20th park."""
    import logging

    coord = make_coordinator()
    coord.store = MagicMock()
    ts = TorrentState(source_infohash="p" * 40, source_name="Parked",
                      state=State.MOVING)
    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        for _ in range(30):
            coord._park_moving(ts, "waiting")
    errors = [r for r in caplog.records
              if r.levelno == logging.ERROR
              and "MOVING stalled" in r.getMessage()]
    # Parks 5 and 25 only.
    assert len(errors) == 2
    assert "(parked 5x" in errors[0].getMessage()
    assert "(parked 25x" in errors[1].getMessage()



@pytest.mark.anyio
async def test_pause_verified_short_circuits_checking(tmp_path):
    """A hash-checking torrent yields a wait-it-out marker, not retries."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator_errors import TorrentCheckingError

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingResumeData", category="racing",
            save_path=str(tmp_path / "ssd")))
        ok, err = await coord._pause_verified("s" * 40)
        assert ok is False
        assert isinstance(err, TorrentCheckingError)
        assert "checkingResumeData" in err.client_state
        # One pause RPC + one read: no 3-attempt storm against the check.
        assert coord.dest_client.pause.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_waits_out_check_quietly(tmp_path):
    """Checking at move time: quiet park, no resume storm, no wipe, no move."""
    import logging

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd")))
        row = store.get("s" * 40)
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock) as mock_wipe:
            for _ in range(7):
                await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord._rclone_move.assert_not_called()
        mock_wipe.assert_not_awaited()
        # Resume would fight the check (and recheck must not pile on).
        coord.dest_client.resume.assert_not_called()
        assert coord._moving_parks.get("s" * 40) == 7
    finally:
        store.close()


@pytest.mark.anyio
async def test_quiet_park_never_errors(tmp_path, caplog):
    """quiet=True parks stay at debug no matter the count."""
    import logging

    coord = make_coordinator()
    coord.store = MagicMock()
    ts = TorrentState(source_infohash="q" * 40, source_name="Quiet",
                      state=State.MOVING)
    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        for _ in range(30):
            coord._park_moving(ts, "hash check running", quiet=True)
    assert [r for r in caplog.records
            if r.levelno >= logging.WARNING] == []


@pytest.mark.anyio
async def test_check_wait_fails_after_cap(tmp_path):
    """A hash check that never finishes fails loudly after the cap."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_max_check_wait_seconds = 3600
        coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd")))
        coord.dest_client.delete = AsyncMock()
        coord._ssd_release = AsyncMock()
        coord._notify_telegram = AsyncMock()

        # Fresh check: parks quietly, clock starts.
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        # Clock artificially aged past the cap: next tick fails terminally.
        coord._checking_track["s" * 40][0] -= 7200.0
        await coord._do_moving(store.get("s" * 40))
        row = store.get("s" * 40)
        assert row.state == State.FAILED
        assert "hash check" in (row.last_error or "")
        coord._notify_telegram.assert_awaited_once()
        coord._ssd_release.assert_awaited_once_with("s" * 40)
    finally:
        store.close()


@pytest.mark.anyio
async def test_check_wait_zero_disables_cap(tmp_path):
    """max_check_wait=0: indefinite quiet wait (old behavior)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_max_check_wait_seconds = 0
        coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd")))
        coord._notify_telegram = AsyncMock()
        for _ in range(3):
            await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord._notify_telegram.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_other_park_resets_check_clock(tmp_path):
    """A non-checking park clears the check-wait clock (continuous only)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.state import State

        coord._checking_track = {"s" * 40: [1.0, 0.5, 1.0]}
        coord._park_moving(store.get("s" * 40), "ordinary hiccup")
        assert coord._checking_track.get("s" * 40) is None
        assert store.get("s" * 40).state == State.MOVING
    finally:
        store.close()


@pytest.mark.anyio
async def test_preread_checking_parks_without_touching(tmp_path):
    """Top pre-read sees checking: no pause, no resume, no recheck, no move."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        checking = Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd"))
        coord.dest_client.list_torrents = AsyncMock(return_value=[checking])
        coord.dest_client.pause = AsyncMock()
        coord.dest_client.resume = AsyncMock()
        coord.dest_client.recheck = AsyncMock()
        coord._rclone_move = AsyncMock()
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord.dest_client.pause.assert_not_called()
        coord.dest_client.resume.assert_not_called()
        coord.dest_client.recheck.assert_not_called()
        coord._rclone_move.assert_not_called()
        mock_wipe.assert_not_awaited()
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_single_file_skips_resume_while_check_runs(tmp_path):
    """Check starting mid-tick: branch parks without resume or recheck."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        idle = Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="pausedDL", category="racing",
            save_path=str(tmp_path / "ssd"))
        checking = Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingResumeData", category="racing",
            save_path=str(tmp_path / "ssd"))
        # Top pre-read + pause gate see idle; branch re-read sees checking.
        coord.dest_client.list_torrents = AsyncMock(
            side_effect=[[idle], [checking]])
        coord.dest_client.pause = AsyncMock()
        coord.dest_client.resume = AsyncMock()
        coord.dest_client.recheck = AsyncMock()
        coord._rclone_move = AsyncMock()
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord.dest_client.resume.assert_not_called()
        coord.dest_client.recheck.assert_not_called()
        coord._rclone_move.assert_not_called()
        mock_wipe.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_batch_pause_checking_parks_at_once(tmp_path):
    """Batched pause gate: checking parks DOWNLOADING without retry burn."""
    from racing_sync.batcher import Batch
    from racing_sync.classifier import Episode
    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import State

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        ts = store.get("s" * 40)
        ts.state = State.DOWNLOADING
        ts.batches_total = 2
        ts.batch_index = 0
        store.upsert(ts)
        coord.dest_client.get_torrent = AsyncMock(return_value=Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=0.5,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd")))
        coord.dest_client.pause = AsyncMock()
        coord._get_batches_for_torrent = AsyncMock(return_value=[
            Batch(episodes=[Episode("Solo.mkv", 0, 1, 1000)])])
        coord._fuse_skipped = AsyncMock(return_value=set())
        coord._wait_for_completion = AsyncMock()
        coord._move_and_clean_batch = AsyncMock()

        await coord._do_downloading(store.get("s" * 40))

        assert store.get("s" * 40).state == State.DOWNLOADING
        coord._move_and_clean_batch.assert_not_called()
        # One pause + one verify read, not the 3-attempt storm.
        assert coord.dest_client.pause.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_preread_check_expiry_fails_row(tmp_path):
    """Pre-read path shares the check-wait clock (no bypass)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_max_check_wait_seconds = 3600
        coord.dest_client.list_torrents = AsyncMock(return_value=[Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=1.0,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd"))])
        coord.dest_client.delete = AsyncMock()
        coord._ssd_release = AsyncMock()
        coord._notify_telegram = AsyncMock()

        # Fresh sighting: quiet park, clock starts.
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord.dest_client.pause.assert_not_called()
        # Aged past the cap: terminal fail with Telegram page.
        coord._checking_track["s" * 40][0] -= 7200.0
        await coord._do_moving(store.get("s" * 40))
        row = store.get("s" * 40)
        assert row.state == State.FAILED
        assert "hash check" in (row.last_error or "")
        coord._notify_telegram.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_check_stall_frozen_progress_fails(tmp_path):
    """Same check fraction across the stall window: terminal fail."""
    import asyncio

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_check_stall_seconds = 0.15
        coord.cfg.general.download_max_check_wait_seconds = 3600
        checking = Torrent(
            hash="s" * 40, name="Solo", size_bytes=1000, progress=0.5,
            state="checkingDL", category="racing",
            save_path=str(tmp_path / "ssd"))
        coord.dest_client.list_torrents = AsyncMock(return_value=[checking])
        coord.dest_client.delete = AsyncMock()
        coord._ssd_release = AsyncMock()
        coord._notify_telegram = AsyncMock()

        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        await asyncio.sleep(0.25)
        await coord._do_moving(store.get("s" * 40))
        row = store.get("s" * 40)
        assert row.state == State.FAILED
        assert "no progress" in (row.last_error or "")
        coord._notify_telegram.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_check_advance_resets_stall_window(tmp_path):
    """A climbing check never trips, even past the original window."""
    import asyncio

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_check_stall_seconds = 0.15
        coord.cfg.general.download_max_check_wait_seconds = 3600
        prog = {"v": 0.5}

        async def _checking(*a, **k):
            return [Torrent(
                hash="s" * 40, name="Solo", size_bytes=1000,
                progress=prog["v"], state="checkingDL", category="racing",
                save_path=str(tmp_path / "ssd"))]

        coord.dest_client.list_torrents = AsyncMock(side_effect=_checking)
        coord._notify_telegram = AsyncMock()

        await coord._do_moving(store.get("s" * 40))
        await asyncio.sleep(0.25)  # past the window, but then it advances
        prog["v"] = 0.7
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord._notify_telegram.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_check_restart_resets_clock(tmp_path):
    """Progress going backwards = fresh check, not a stalled one."""
    import asyncio

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_check_stall_seconds = 0.15
        coord.cfg.general.download_max_check_wait_seconds = 3600
        prog = {"v": 0.5}

        async def _checking(*a, **k):
            return [Torrent(
                hash="s" * 40, name="Solo", size_bytes=1000,
                progress=prog["v"], state="checkingDL", category="racing",
                save_path=str(tmp_path / "ssd"))]

        coord.dest_client.list_torrents = AsyncMock(side_effect=_checking)
        coord._notify_telegram = AsyncMock()

        await coord._do_moving(store.get("s" * 40))
        await asyncio.sleep(0.25)  # window elapsed...
        prog["v"] = 0.1  # ...but the check restarted from zero
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord._notify_telegram.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_check_absolute_cap_despite_progress(tmp_path):
    """Ultra-slow crawls still trip the absolute backstop."""
    import asyncio

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_check_stall_seconds = 3600
        coord.cfg.general.download_max_check_wait_seconds = 0.2
        prog = {"v": 0.5}

        async def _checking(*a, **k):
            return [Torrent(
                hash="s" * 40, name="Solo", size_bytes=1000,
                progress=prog["v"], state="checkingDL", category="racing",
                save_path=str(tmp_path / "ssd"))]

        coord.dest_client.list_torrents = AsyncMock(side_effect=_checking)
        coord.dest_client.delete = AsyncMock()
        coord._ssd_release = AsyncMock()
        coord._notify_telegram = AsyncMock()

        await coord._do_moving(store.get("s" * 40))
        prog["v"] = 0.6  # advancing, but too slowly overall
        await asyncio.sleep(0.3)
        await coord._do_moving(store.get("s" * 40))
        row = store.get("s" * 40)
        assert row.state == State.FAILED
        assert "exceeded" in (row.last_error or "")
    finally:
        store.close()
