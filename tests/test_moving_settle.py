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
    files = [TorrentFile(name="Solo.mkv", size_bytes=1000, progress=progress, priority=1)]

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
        source_infohash="s" * 40,
        source_name="Solo",
        dest_infohash="s" * 40,
        save_path=str(ssd),
        classification_kind="movie",
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_settle_recheck_fires_once_then_hands_to_downloading(tmp_path):
    """Full bytes + lagging progress: one force recheck, then park (no wipe)."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    row = store.get("s" * 40)
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
        await coord._do_moving(row)
        assert row.state == State.MOVING
        coord.dest_client.recheck.assert_awaited_once_with("s" * 40)
        mock_wipe.assert_not_awaited()
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
        # Second tick: no second recheck (once per row); resumed bytes
        # hand back to the download loop instead of flapping in MOVING.
        await coord._do_moving(store.get("s" * 40))
        assert coord.dest_client.recheck.await_count == 1
        assert store.get("s" * 40).state == State.DOWNLOADING
        mock_wipe.assert_not_awaited()
    store.close()


@pytest.mark.anyio
async def test_settle_recheck_skipped_while_flush_pending(tmp_path):
    """Full bytes + 100% + still downloading: flush pending, not lag.

    Regression: force-rechecking here aborts libtorrent's completion
    flush and wedges the single global check slot, queueing every later
    row as "checking resume data". Must park quietly (no recheck, no
    resume) and let the flush land.
    """
    from types import SimpleNamespace

    coord, store = _moving_single(tmp_path, progress=0.0)
    row = store.get("s" * 40)
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[SimpleNamespace(state="downloading", progress=1.0)]
    )
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
        await coord._do_moving(row)
        assert store.get("s" * 40).state == State.MOVING
        coord.dest_client.recheck.assert_not_called()
        coord.dest_client.resume.assert_not_called()
        coord._rclone_move.assert_not_called()
        mock_wipe.assert_not_awaited()
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
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
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
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
    ts = TorrentState(source_infohash="p" * 40, source_name="Parked", state=State.MOVING)
    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        for _ in range(30):
            coord._park_moving(ts, "waiting")
    errors = [
        r
        for r in caplog.records
        if r.levelno == logging.ERROR and "MOVING stalled" in r.getMessage()
    ]
    # Parks 5 and 25 only.
    assert len(errors) == 2
    assert "(parked 5x" in errors[0].getMessage()
    assert "(parked 25x" in errors[1].getMessage()


@pytest.mark.anyio
async def test_park_moving_preserves_concurrent_state(tmp_path):
    """A park must not regress a state moved by someone else meanwhile."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        stale = store.get("s" * 40)
        assert stale.state == State.MOVING
        # Concurrent writer advances the row first.
        other = store.get("s" * 40)
        store.transition(other, State.RE_ADDING)
        assert store.get("s" * 40).state == State.RE_ADDING
        # The stale park records its reason but keeps the winner's state.
        coord._park_moving(stale, "short bytes")
        row = store.get("s" * 40)
        assert row.state == State.RE_ADDING
        assert "short bytes" in (row.last_error or "")
    finally:
        store.close()


@pytest.mark.anyio
async def test_pause_verified_short_circuits_checking(tmp_path):
    """A hash-checking torrent yields a wait-it-out marker, not retries."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator_errors import TorrentCheckingError

    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        coord.dest_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="s" * 40,
                name="Solo",
                size_bytes=1000,
                progress=1.0,
                state="checkingResumeData",
                category="racing",
                save_path=str(tmp_path / "ssd"),
            )
        )
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

        coord.dest_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="s" * 40,
                name="Solo",
                size_bytes=1000,
                progress=1.0,
                state="checkingDL",
                category="racing",
                save_path=str(tmp_path / "ssd"),
            )
        )
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
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
    ts = TorrentState(source_infohash="q" * 40, source_name="Quiet", state=State.MOVING)
    with caplog.at_level(logging.DEBUG, logger="racing_sync.coordinator"):
        for _ in range(30):
            coord._park_moving(ts, "hash check running", quiet=True)
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


@pytest.mark.anyio
async def test_check_wait_fails_after_cap(tmp_path):
    """A hash check that never finishes fails loudly after the cap."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    try:
        from racing_sync.clients.abstract import Torrent
        from racing_sync.state import State

        coord.cfg.general.download_max_check_wait_seconds = 3600
        coord.dest_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="s" * 40,
                name="Solo",
                size_bytes=1000,
                progress=1.0,
                state="checkingDL",
                category="racing",
                save_path=str(tmp_path / "ssd"),
            )
        )
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
        coord.dest_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="s" * 40,
                name="Solo",
                size_bytes=1000,
                progress=1.0,
                state="checkingDL",
                category="racing",
                save_path=str(tmp_path / "ssd"),
            )
        )
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
            hash="s" * 40,
            name="Solo",
            size_bytes=1000,
            progress=1.0,
            state="checkingDL",
            category="racing",
            save_path=str(tmp_path / "ssd"),
        )
        coord.dest_client.list_torrents = AsyncMock(return_value=[checking])
        coord.dest_client.pause = AsyncMock()
        coord.dest_client.resume = AsyncMock()
        coord.dest_client.recheck = AsyncMock()
        coord._rclone_move = AsyncMock()
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
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
            hash="s" * 40,
            name="Solo",
            size_bytes=1000,
            progress=1.0,
            state="pausedDL",
            category="racing",
            save_path=str(tmp_path / "ssd"),
        )
        checking = Torrent(
            hash="s" * 40,
            name="Solo",
            size_bytes=1000,
            progress=1.0,
            state="checkingResumeData",
            category="racing",
            save_path=str(tmp_path / "ssd"),
        )
        # Top pre-read + pause gate see idle; branch re-read sees checking.
        coord.dest_client.list_torrents = AsyncMock(side_effect=[[idle], [checking]])
        coord.dest_client.pause = AsyncMock()
        coord.dest_client.resume = AsyncMock()
        coord.dest_client.recheck = AsyncMock()
        coord._rclone_move = AsyncMock()
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
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
        coord.dest_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="s" * 40,
                name="Solo",
                size_bytes=1000,
                progress=0.5,
                state="checkingDL",
                category="racing",
                save_path=str(tmp_path / "ssd"),
            )
        )
        coord.dest_client.pause = AsyncMock()
        coord._get_batches_for_torrent = AsyncMock(
            return_value=[Batch(episodes=[Episode("Solo.mkv", 0, 1, 1000)])]
        )
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
        coord.dest_client.list_torrents = AsyncMock(
            return_value=[
                Torrent(
                    hash="s" * 40,
                    name="Solo",
                    size_bytes=1000,
                    progress=1.0,
                    state="checkingDL",
                    category="racing",
                    save_path=str(tmp_path / "ssd"),
                )
            ]
        )
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
            hash="s" * 40,
            name="Solo",
            size_bytes=1000,
            progress=0.5,
            state="checkingDL",
            category="racing",
            save_path=str(tmp_path / "ssd"),
        )
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
            return [
                Torrent(
                    hash="s" * 40,
                    name="Solo",
                    size_bytes=1000,
                    progress=prog["v"],
                    state="checkingDL",
                    category="racing",
                    save_path=str(tmp_path / "ssd"),
                )
            ]

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
            return [
                Torrent(
                    hash="s" * 40,
                    name="Solo",
                    size_bytes=1000,
                    progress=prog["v"],
                    state="checkingDL",
                    category="racing",
                    save_path=str(tmp_path / "ssd"),
                )
            ]

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
            return [
                Torrent(
                    hash="s" * 40,
                    name="Solo",
                    size_bytes=1000,
                    progress=prog["v"],
                    state="checkingDL",
                    category="racing",
                    save_path=str(tmp_path / "ssd"),
                )
            ]

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


@pytest.mark.anyio
async def test_recheck_serialized_across_rows(tmp_path):
    """One settle recheck in flight: the second row waits its turn."""
    from racing_sync.clients.abstract import TorrentFile
    from racing_sync.state import State, StateStore, TorrentState
    from unittest.mock import AsyncMock, MagicMock, patch

    def _row(h, name):
        ssd = tmp_path / "ssd"
        ssd.mkdir(exist_ok=True)
        (ssd / name).write_bytes(b"x" * 500)
        files = [TorrentFile(name=name, size_bytes=500, progress=0.5, priority=1)]
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
        coord.store = store
        coord.dest_client = AsyncMock()
        coord.dest_client.get_torrent_files = AsyncMock(return_value=files)
        coord._rclone_move = AsyncMock()
        ts = TorrentState(
            source_infohash=h,
            source_name=name.rsplit(".", 1)[0],
            dest_infohash=h,
            save_path=str(ssd),
            classification_kind="movie",
            batches_total=1,
            batch_index=0,
            state=State.MOVING,
        )
        store.upsert(ts)
        return coord

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _row("a" * 40, "A.mkv")
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
            await coord._do_moving(store.get("a" * 40))
        assert coord.dest_client.recheck.await_count == 1
        assert coord._recheck_in_flight[0] == "a" * 40

        # Second row, same tick window: defers, no second recheck storm.
        coord2 = _row("b" * 40, "B.mkv")
        coord2._recheck_in_flight = coord._recheck_in_flight
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
            await coord2._do_moving(store.get("b" * 40))
        assert store.get("b" * 40).state == State.MOVING
        coord2.dest_client.recheck.assert_not_called()

        # Stale holder (over 30 min old): takeover allowed.
        coord2._recheck_in_flight[1] -= 2000.0
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
            await coord2._do_moving(store.get("b" * 40))
        assert coord2.dest_client.recheck.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_check_timeout_keeps_ssd_bytes(tmp_path):
    """Wedged MOVING hash check: fail the row but drop only the client
    entry (delete_files=False) -- complete SSD bytes must survive."""
    import time

    coord, store = _moving_single(tmp_path, progress=1.0)
    row = store.get("s" * 40)
    # Client reports a hash check in flight...
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[SimpleNamespace(state="checkingDL", progress=0.5)]
    )
    # ...that never advances: pre-seed a stale check-watch record so the
    # shared clock trips terminal on this tick.
    _now = time.monotonic()
    coord._checking_track = {"s" * 40: [_now - 10000.0, 0.5, _now - 10000.0]}
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
        await coord._do_moving(row)
    assert store.get("s" * 40).state == State.FAILED
    coord.dest_client.delete.assert_awaited_once_with("s" * 40, delete_files=False)
    mock_wipe.assert_not_awaited()
    assert (tmp_path / "ssd" / "Solo.mkv").exists()
    store.close()


@pytest.mark.anyio
async def test_fail_downloading_row_defaults_to_corpse_cleanup(tmp_path):
    """Pre-move callers keep delete_files=True: paused partials must not
    leak dest entries that nothing else reaps."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    row = store.get("s" * 40)
    await coord._fail_downloading_row(row, "s" * 40, "tracker deleted it")
    coord.dest_client.delete.assert_awaited_once_with("s" * 40, delete_files=True)
    assert store.get("s" * 40).state == State.FAILED
    store.close()


@pytest.mark.anyio
async def test_mid_move_check_skips_incomplete_unlink(tmp_path):
    """A check starting between the pre-move state read and the file list
    dips progress: the file classifies incomplete, but the post-list
    re-read sees checking and nothing is unlinked. Control phase proves
    the same file IS unlinked when no check is running."""
    from racing_sync.clients.abstract import TorrentFile

    coord, store = _moving_single(tmp_path, progress=1.0)
    # Complete per client, short on disk -> would classify incomplete.
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Solo.mkv", size_bytes=2000, progress=1.0, priority=1)]
    )
    _downloading = SimpleNamespace(state="downloading", progress=1.0)
    _checking = SimpleNamespace(state="checkingDL", progress=0.5)

    # Phase 1: check starts mid-move -> file survives.
    coord.dest_client.list_torrents = AsyncMock(side_effect=[[_downloading], [_checking]])
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        await coord._do_moving(store.get("s" * 40))
    assert (tmp_path / "ssd" / "Solo.mkv").exists()

    # Phase 2 (control): no check running -> true partial is unlinked
    # (the pre-existing FileNotFoundError for vanished singles then
    # surfaces downstream; what matters here is the file is gone).
    coord.dest_client.list_torrents = AsyncMock(side_effect=[[_downloading], [_downloading]])
    with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
        with pytest.raises(FileNotFoundError):
            await coord._do_moving(store.get("s" * 40))
    assert not (tmp_path / "ssd" / "Solo.mkv").exists()
    store.close()


@pytest.mark.anyio
async def test_pause_verified_fails_closed_on_read_error():
    """An unverifiable pause is not a success: callers must park, never
    move under a possibly-writing client."""
    coord = make_coordinator()
    coord.dest_client = AsyncMock()
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.get_torrent = AsyncMock(side_effect=TimeoutError("wedged"))
    ok, err = await coord._pause_verified("a" * 40)
    assert ok is False
    assert err is not None
    assert coord.dest_client.pause.await_count == 3


@pytest.mark.anyio
async def test_moving_errno_none_fails_after_parks(tmp_path):
    """Unreadable-mount OSErrors (errno None) never heal: bounded parks,
    then FAILED with bytes intact (entry dropped, data kept)."""
    coord, store = _moving_single(tmp_path, progress=1.0)
    coord.dest_client.get_torrent_files = AsyncMock(side_effect=OSError("wedged mount"))
    try:
        for _ in range(9):
            await coord._do_moving(store.get("s" * 40))
            assert store.get("s" * 40).state == State.MOVING
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.FAILED
        coord.dest_client.delete.assert_awaited_once_with("s" * 40, delete_files=False)
        assert (tmp_path / "ssd" / "Solo.mkv").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_empty_prelist_parks_without_pause(tmp_path):
    """An unlisted entry parks for the next tick instead of pausing a
    ghost entry."""
    coord, store = _moving_single(tmp_path, progress=1.0)
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.pause = AsyncMock()
    try:
        await coord._do_moving(store.get("s" * 40))
        assert store.get("s" * 40).state == State.MOVING
        coord.dest_client.pause.assert_not_called()
    finally:
        store.close()


def _moved_season_row(tmp_path):
    """Season MOVING row whose folder move succeeds (mock deletes)."""
    from types import SimpleNamespace

    ssd = tmp_path / "ssd"
    top = ssd / "Show.S01"
    top.mkdir(parents=True, exist_ok=True)
    (top / "Show.S01E01.mkv").write_bytes(b"x" * 100)
    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store)
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[SimpleNamespace(state="downloading", progress=1.0)]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[
            TorrentFile(name="Show.S01/Show.S01E01.mkv", size_bytes=100, progress=1.0),
        ]
    )
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.get_torrent = AsyncMock(
        return_value=SimpleNamespace(state="pausedDL", progress=1.0)
    )
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)
    ts = TorrentState(
        source_infohash="w" * 40,
        source_name="Show.S01",
        dest_infohash="w" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
    )
    ts._blob = b"blob"
    store.upsert(ts)
    return coord, store, ssd


def _checkpoint_fixture(tmp_path, stage, plan, *, files=None, ssd_exists=True):
    """MOVING row with a persisted checkpoint; replay must not re-list it."""
    import json

    ssd = tmp_path / "ssd"
    if ssd_exists:
        ssd.mkdir(exist_ok=True)
    for rel, data in (files or {}).items():
        p = ssd / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store)
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
    coord.dest_client = AsyncMock()
    coord.dest_client.delete = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(
        side_effect=AssertionError("replay must not re-list files")
    )
    coord.dest_client.list_torrents = AsyncMock(
        side_effect=AssertionError("replay must not re-read client state")
    )
    coord.dest_client.pause = AsyncMock(side_effect=AssertionError("replay must not pause"))
    coord.dest_client.resume = AsyncMock(side_effect=AssertionError("replay must not resume"))
    coord._rclone_move = AsyncMock()
    ts = TorrentState(
        source_infohash="c" * 40,
        source_name="Show.S01",
        dest_infohash="c" * 40,
        save_path=str(ssd),
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
        move_stage=stage,
        move_plan=plan if isinstance(plan, str) else json.dumps(plan),
    )
    ts._blob = b"blob"
    store.upsert(ts)
    return coord, store, ssd


@pytest.mark.anyio
async def test_moving_wipe_failure_replays_from_checkpoint(tmp_path):
    """Wipe failure parks at entry_deleted; retry cleans without re-listing."""
    import shutil

    coord, store, _ssd = _moved_season_row(tmp_path)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            mock_wipe.side_effect = PermissionError("open handle")
            await coord._do_moving(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "entry_deleted"
        assert "open handle" in (row.last_error or "")
        files_calls = coord.dest_client.get_torrent_files.await_count
        list_calls = coord.dest_client.list_torrents.await_count
        pause_calls = coord.dest_client.pause.await_count

        async def _wipe_ok(path, *, base_dir=None):
            shutil.rmtree(path)

        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe2:
            mock_wipe2.side_effect = _wipe_ok
            await coord._do_moving(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.RE_ADDING
        assert row.move_stage == "" and row.move_plan == ""
        assert not (tmp_path / "ssd" / "Show.S01").exists()
        # Replay never re-listed/re-paused the (already deleted) entry.
        assert coord.dest_client.get_torrent_files.await_count == files_calls
        assert coord.dest_client.list_torrents.await_count == list_calls
        assert coord.dest_client.pause.await_count == pause_calls
        assert coord.dest_client.delete.await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_delete_failure_retries_delete_only(tmp_path):
    """Delete failure parks at 'moved'; retry deletes then cleans up."""
    import shutil

    coord, store, _ssd = _moved_season_row(tmp_path)
    try:
        coord.dest_client.delete = AsyncMock(side_effect=OSError("gone"))
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
            await coord._do_moving(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "moved"
        files_calls = coord.dest_client.get_torrent_files.await_count
        list_calls = coord.dest_client.list_torrents.await_count
        pause_calls = coord.dest_client.pause.await_count

        coord.dest_client.delete = AsyncMock()

        async def _wipe_ok(path, *, base_dir=None):
            shutil.rmtree(path)

        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            mock_wipe.side_effect = _wipe_ok
            await coord._do_moving(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.RE_ADDING
        assert coord.dest_client.delete.await_count == 1
        assert coord.dest_client.get_torrent_files.await_count == files_calls
        assert coord.dest_client.list_torrents.await_count == list_calls
        assert coord.dest_client.pause.await_count == pause_calls
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_replay_moved_retries_ambiguous_delete(tmp_path):
    """Ambiguous delete: stay at 'moved' (no cleanup) and retry delete."""
    plan = {"version": 1, "layout": "folder", "folder": "Show.S01", "keep": [], "flat": []}
    coord, store, ssd = _checkpoint_fixture(
        tmp_path, "moved", plan, files={"Show.S01/Show.S01E01.mkv": b"x" * 100}
    )
    try:
        coord.dest_client.delete = AsyncMock(side_effect=OSError("timeout"))
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.MOVING and row.move_stage == "moved"
        assert (ssd / "Show.S01" / "Show.S01E01.mkv").exists()
        mock_wipe.assert_not_awaited()

        coord.dest_client.delete = AsyncMock()
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe2:
            mock_wipe2.side_effect = lambda path, *, base_dir=None: __import__("shutil").rmtree(
                path
            )
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        assert coord.dest_client.delete.await_count == 1
        assert not (ssd / "Show.S01").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_crash_after_delete_before_checkpoint(tmp_path):
    """Stage 'moved' with the entry already gone: delete is idempotent."""
    import shutil

    plan = {"version": 1, "layout": "folder", "folder": "Show.S01", "keep": [], "flat": []}
    coord, store, ssd = _checkpoint_fixture(
        tmp_path, "moved", plan, files={"Show.S01/Show.S01E01.mkv": b"x" * 100}
    )
    try:

        async def _wipe_ok(path, *, base_dir=None):
            shutil.rmtree(path)

        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            mock_wipe.side_effect = _wipe_ok
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        assert row.move_stage == "" and row.move_plan == ""
        coord.dest_client.delete.assert_awaited_once_with("c" * 40, delete_files=False)
        assert not (ssd / "Show.S01").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_crash_after_wipe_before_checkpoint(tmp_path):
    """Stage 'entry_deleted' with the tree already gone: just checkpoint+advance."""
    plan = {"version": 1, "layout": "folder", "folder": "Show.S01", "keep": [], "flat": []}
    coord, store, _ssd = _checkpoint_fixture(tmp_path, "entry_deleted", plan)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        assert row.move_stage == ""
        mock_wipe.assert_not_awaited()
        coord.dest_client.delete.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_partial_flat_wipe_stays_entry_deleted(tmp_path):
    """One flat unlink failure keeps the checkpoint at entry_deleted."""
    from pathlib import Path

    plan = {"version": 1, "layout": "flat", "folder": "", "keep": [], "flat": ["a.bin", "b.bin"]}
    coord, store, ssd = _checkpoint_fixture(
        tmp_path, "entry_deleted", plan, files={"a.bin": b"a" * 10, "b.bin": b"b" * 10}
    )
    real_unlink = Path.unlink

    def _flaky_unlink(self, *a, **k):
        if self.name == "b.bin":
            raise OSError("busy")
        return real_unlink(self, *a, **k)

    try:
        with patch.object(Path, "unlink", _flaky_unlink):
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "entry_deleted"
        assert not (ssd / "a.bin").exists()
        assert (ssd / "b.bin").exists()
        assert "flat cleanup left" in (row.last_error or "")

        await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.RE_ADDING
        assert not (ssd / "b.bin").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_corrupt_plan_parks_without_wiping(tmp_path):
    """Corrupt checkpoint JSON: operator-visible park, never a blind wipe."""
    coord, store, ssd = _checkpoint_fixture(
        tmp_path, "entry_deleted", "{not-json", files={"Show.S01/Show.S01E01.mkv": b"x" * 100}
    )
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "entry_deleted"
        assert "corrupt" in (row.last_error or "")
        assert (ssd / "Show.S01" / "Show.S01E01.mkv").exists()
        mock_wipe.assert_not_awaited()
        coord.dest_client.delete.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_plan_traversal_parks_without_wiping(tmp_path):
    """Plan paths escaping the SSD root are rejected before any cleanup."""
    outside = tmp_path / "escape.bin"
    outside.write_bytes(b"keep me")
    plan = {"version": 1, "layout": "flat", "folder": "", "keep": [], "flat": ["../escape.bin"]}
    coord, store, _ssd = _checkpoint_fixture(tmp_path, "entry_deleted", plan)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "entry_deleted"
        assert "unsafe" in (row.last_error or "")
        assert outside.exists()
        mock_wipe.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_missing_ssd_root_parks_without_wiping(tmp_path):
    """A vanished SSD root parks instead of joining paths against nothing."""
    plan = {"version": 1, "layout": "folder", "folder": "Show.S01", "keep": [], "flat": []}
    coord, store, _ssd = _checkpoint_fixture(tmp_path, "entry_deleted", plan, ssd_exists=False)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("c" * 40))
        row = store.get("c" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == "entry_deleted"
        assert "SSD root" in (row.last_error or "")
        mock_wipe.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_legacy_empty_prelist_fails_closed(tmp_path):
    """Stage-less row with an unlisted entry: explicit unsafe-wipe refusal."""
    coord, store = _moving_single(tmp_path, progress=1.0)
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("s" * 40))
        row = store.get("s" * 40)
        assert row.state == State.MOVING
        assert "legacy MOVING row has no checkpoint" in (row.last_error or "")
        coord.dest_client.pause.assert_not_called()
        coord.dest_client.delete.assert_not_called()
        mock_wipe.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_checkpoint_write_failure_parks_before_delete(tmp_path):
    """A lost checkpoint write parks with no destructive action."""
    coord, store, ssd = _moved_season_row(tmp_path)
    original = store.update_columns

    def _lose_move_write(h, mapping, *, expected_state=None, expected_version=None):
        if isinstance(mapping, dict) and "move_stage" in mapping:
            return 0
        return original(
            h, mapping, expected_state=expected_state, expected_version=expected_version
        )

    store.update_columns = _lose_move_write
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.MOVING
        assert row.move_stage == ""
        assert "could not persist the move checkpoint" in (row.last_error or "")
        coord.dest_client.delete.assert_not_called()
        mock_wipe.assert_not_awaited()
        assert (ssd / "Show.S01").exists()
    finally:
        store.update_columns = original
        store.close()


@pytest.mark.anyio
async def test_moving_delete_fail_parks_without_wipe(tmp_path):
    """Post-move entry-delete failure parks (no wipe): the next tick
    retries instead of advancing past a mismatched client state."""
    from unittest.mock import patch

    coord, store, _ssd = _moved_season_row(tmp_path)
    coord.dest_client.delete = AsyncMock(side_effect=OSError("gone"))
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("w" * 40))
        assert store.get("w" * 40).state == State.MOVING
        mock_wipe.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_moving_wipe_fail_parks_fully_moved_row(tmp_path):
    """A failing folder wipe (open handle) parks the fully-moved row
    instead of FAILED."""
    from unittest.mock import patch

    coord, store, _ssd = _moved_season_row(tmp_path)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            mock_wipe.side_effect = PermissionError("open handle")
            await coord._do_moving(store.get("w" * 40))
        assert store.get("w" * 40).state == State.MOVING
        mock_wipe.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_rclone_move_error_carries_headline_and_tail(tmp_path):
    """Move failures report rclone's headline and tail, not just the tail."""
    import asyncio
    from unittest.mock import patch

    from racing_sync.rclone_ops import RcloneResult

    coord = make_coordinator()
    coord._move_sem = asyncio.Semaphore(1)
    coord.cfg = MagicMock()
    ts = TorrentState(source_infohash="r" * 40, source_name="R", state=State.MOVING)
    res = RcloneResult(
        returncode=1,
        stdout="",
        stderr="NOTICE: headline here\nline2\nERROR: tail detail",
        duration=1.0,
    )
    with patch("racing_sync.coordinator_move.move_local_to_remote", AsyncMock(return_value=res)):
        with pytest.raises(RuntimeError) as exc:
            await coord._rclone_move(tmp_path, "remote:x", ts)
    assert "headline here" in str(exc.value)
    assert "tail detail" in str(exc.value)


@pytest.mark.anyio
async def test_moving_desync_fails_without_wipe(tmp_path):
    """A client-complete file absent from SSD (and not moved, not on
    fuse) fails loudly with bytes kept — never wiped toward a hopeless
    RE_ADDING."""
    from types import SimpleNamespace

    from unittest.mock import patch

    ssd = tmp_path / "ssd"
    top = ssd / "Big.Show.S01"
    top.mkdir(parents=True)
    (top / "Big.Show.S01E02.mkv").write_bytes(b"y" * 100)

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store)
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = MagicMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[SimpleNamespace(state="downloading", progress=1.0)]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[
            TorrentFile(
                name="Big.Show.S01/Big.Show.S01E01.mkv", size_bytes=100, progress=1.0, priority=1
            ),
            TorrentFile(
                name="Big.Show.S01/Big.Show.S01E02.mkv", size_bytes=100, progress=1.0, priority=1
            ),
        ]
    )
    coord.dest_client.pause = AsyncMock()
    coord.dest_client.get_torrent = AsyncMock(
        return_value=SimpleNamespace(state="pausedDL", progress=1.0)
    )
    coord.dest_client.delete = AsyncMock()

    async def _fake_move(local, remote, ts, *, include=None, files_from=None, extra=None):
        for name in files_from or []:
            p = ssd / name
            if p.is_file():
                p.unlink()

    coord._rclone_move = AsyncMock(side_effect=_fake_move)

    ts = TorrentState(
        source_infohash="d" * 40,
        source_name="Big.Show.S01",
        classification_kind="season",
        batches_total=1,
        batch_index=0,
        state=State.MOVING,
    )
    ts._blob = b"blob"
    store.upsert(ts)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("d" * 40))
        row = store.get("d" * 40)
        assert row.state == State.FAILED
        assert "missing on SSD" in (row.last_error or "")
        coord.dest_client.delete.assert_awaited_once_with("d" * 40, delete_files=False)
        mock_wipe.assert_not_called()
    finally:
        store.close()


def test_recheck_holder_release_and_heartbeat():
    """Stale-holder release clears only our own hold; heartbeat refreshes
    a live hold (forgotten rows can't heartbeat, so age-out still reaps)."""
    coord = make_coordinator()
    ts = TorrentState(source_infohash="a" * 40, source_name="A", state=State.MOVING)
    coord._recheck_in_flight = ["a" * 40, 123.0]
    coord._heartbeat_recheck_holder(ts)
    assert coord._recheck_in_flight[0] == "a" * 40
    assert coord._recheck_in_flight[1] != 123.0
    coord._release_recheck_holder(ts)
    assert coord._recheck_in_flight is None
    coord._recheck_in_flight = ["b" * 40, 1.0]
    coord._release_recheck_holder(ts)
    assert coord._recheck_in_flight == ["b" * 40, 1.0]


@pytest.mark.anyio
async def test_settle_rechecked_evicts_bounded(tmp_path):
    """The once-per-row set evicts instead of wipe-clearing at cap."""
    coord, store = _moving_single(tmp_path, progress=0.0)
    coord._settle_rechecked = {f"{i:040d}" for i in range(5000)}
    try:
        with patch("racing_sync.coordinator.wipe_local_tree", new_callable=AsyncMock):
            await coord._do_moving(store.get("s" * 40))
        assert len(coord._settle_rechecked) <= 5001
        assert "s" * 40 in coord._settle_rechecked
    finally:
        store.close()


@pytest.mark.anyio
async def test_heal_move_bypasses_bulk_semaphore(tmp_path):
    """Seconds-long heals ride the reserved slot instead of queueing
    behind bulk multi-hour moves; bulk still waits its turn."""
    import asyncio
    from unittest.mock import patch

    from racing_sync.rclone_ops import RcloneResult

    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord._move_sem = asyncio.Semaphore(1)
    coord._heal_move_sem = asyncio.Semaphore(1)
    await coord._move_sem.acquire()  # bulk slots exhausted
    ts = TorrentState(source_infohash="h" * 40, source_name="H", state=State.RE_ADDING)
    res = RcloneResult(returncode=0, stdout="", stderr="", duration=0.1)
    try:
        with patch(
            "racing_sync.coordinator_move.move_local_to_remote", AsyncMock(return_value=res)
        ):
            await asyncio.wait_for(
                coord._rclone_move(tmp_path, "remote:x", ts, heal=True), timeout=5
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(
                    coord._rclone_move(tmp_path, "remote:x", ts, heal=False), timeout=0.2
                )
    finally:
        coord._move_sem.release()
