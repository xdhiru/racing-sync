"""Pre-pick manual-move wait: content already on the fuse target.

A user rclone-moves a finished racing torrent to the remote by hand while
racing-sync is offline, then starts the daemon. VPS2 has no client entry,
so the exact-infohash adoption misses and the row would query Prowlarr /
reserve SSD budget for bytes that are already on the mount.

The pre-pick check probes the torrent-name root first (one stat for the
common "not on fuse" case), then verifies every expected file:

  present -> admit from the row's own blob (no pick, no SSD budget);
  partial -> park and re-check every `manual_move_check_seconds`, for up
             to `manual_move_timeout_seconds`, then fall back to the pick;
  absent  -> today's pick -> SSD flow, unchanged.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode

H = "a" * 40
TOP = "Manual.Move.Show.S01.1080p-GRP"
EP1 = f"{TOP}/ep01.mkv"
EP2 = f"{TOP}/ep02.mkv"
SIZE1 = 120
SIZE2 = 240


def _blob() -> bytes:
    return _bencode(
        {
            b"info": {
                b"name": TOP.encode(),
                b"files": [
                    {b"length": SIZE1, b"path": [b"ep01.mkv"]},
                    {b"length": SIZE2, b"path": [b"ep02.mkv"]},
                ],
            },
        }
    )


def _scaffold(
    tmp_path,
    *,
    state=State.NEW,
    fuse_files=(),
    root_exists=True,
    timeout=43200,
    check=1800,
    fuse_wait_first_at=None,
    kind="movie",
    blob=None,
    name=TOP,
    total_bytes=None,
    files_mount="fuse",
):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    fuse_u = tmp_path / "fuse-unsorted"
    for d in (ssd, fuse, fuse_u):
        d.mkdir(parents=True, exist_ok=True)
    if root_exists:
        (fuse / TOP).mkdir(parents=True, exist_ok=True)
    base = fuse if files_mount == "fuse" else fuse_u
    for rel, size in fuse_files:
        target = base / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x" * size)

    if blob is None:
        blob = _blob()
    store = StateStore(tmp_path / "state.db")

    coord = make_coordinator(store)
    coord.cfg = MagicMock()
    coord.cfg.source.min_age_seconds = 0
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse_u
    coord.cfg.rclone.fuse.manual_move_timeout_seconds = timeout
    coord.cfg.rclone.fuse.manual_move_check_seconds = check
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._stop = False
    coord._schedule_telegram_update = MagicMock()
    # Keep the real DB transition (state + revision) without the Telegram
    # / ledger side effects of Coordinator.transition.
    coord.transition = lambda ts, dst, error="", batch_index=None: store.transition(
        ts, dst, error=error, batch_index=batch_index
    )
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()

    snap = Torrent(
        hash=H,
        name=name,
        category="racing",
        save_path="/vps1",
        size_bytes=SIZE1 + SIZE2 if total_bytes is None else total_bytes,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord._source_torrents_cache = [snap]
    coord.source_client.get_torrent = AsyncMock(return_value=snap)

    ts = TorrentState(
        source_infohash=H,
        source_name=name,
        classification_kind=kind,
        total_bytes=SIZE1 + SIZE2 if total_bytes is None else total_bytes,
        cross_seed_blob=blob,
        state=state,
        fuse_wait_first_at=fuse_wait_first_at,
    )
    store.upsert(ts)
    return coord, store, blob, fuse


@pytest.mark.anyio
async def test_present_admits_without_pick(tmp_path):
    """Every expected file present: admit from the row blob, no Prowlarr."""
    coord, store, blob, _ = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1), (EP2, SIZE2)])
    coord._admit_resolved = AsyncMock()
    try:
        ts = store.get(H)
        await coord._do_new(ts)

        coord._pick_and_admit.assert_not_called()
        coord._admit_resolved.assert_awaited_once()
        kwargs = coord._admit_resolved.await_args.kwargs
        assert kwargs["blob"] == blob
        assert kwargs["size_bytes"] == SIZE1 + SIZE2
        assert kwargs["display_name"] == TOP[:60]
        assert ts.cross_seed_source == "fuse-existing"
        assert ts.cross_seed_infohash == H
        assert ts.cross_seed_blob == blob
    finally:
        store.close()


@pytest.mark.anyio
async def test_partial_parks_and_waits(tmp_path):
    """One of two files: park WAITING_INDEXER, timer armed, no pick."""
    coord, store, _blob_bytes, _ = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1)])
    try:
        before = dt.datetime.now(dt.UTC)
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.WAITING_INDEXER
        assert row.fuse_wait_first_at is not None
        assert abs(row.fuse_wait_first_at - before).total_seconds() < 60
        assert row.indexer_next_retry_at is not None
        delay = (row.indexer_next_retry_at - before).total_seconds()
        assert abs(delay - 1800) < 60
        assert row.public_export_next_retry_at is None
        assert "waiting for the manual move" in row.last_error
        assert "1/2 files" in row.last_error
    finally:
        store.close()


@pytest.mark.anyio
async def test_wake_before_timeout_rearms_without_pick(tmp_path):
    """A wave before the ceiling re-arms the timer, window start intact."""
    start = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=100)
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path,
        state=State.WAITING_INDEXER,
        fuse_files=[(EP1, SIZE1)],
        fuse_wait_first_at=start,
    )
    try:
        before = dt.datetime.now(dt.UTC)
        await coord._do_waiting_indexer(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.WAITING_INDEXER
        assert row.fuse_wait_first_at is not None
        assert abs(row.fuse_wait_first_at - start).total_seconds() < 1
        assert row.indexer_next_retry_at is not None
        assert abs((row.indexer_next_retry_at - before).total_seconds() - 1800) < 60
    finally:
        store.close()


@pytest.mark.anyio
async def test_timeout_falls_back_to_pick(tmp_path):
    """Window exhausted: clear the stamp and run the normal pick."""
    start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=13)
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path,
        state=State.WAITING_INDEXER,
        fuse_files=[(EP1, SIZE1)],
        timeout=3600,
        fuse_wait_first_at=start,
    )
    try:
        ts = store.get(H)
        await coord._do_waiting_indexer(ts)

        coord._pick_and_admit.assert_awaited_once()
        # The stamp stays as a latch: an oversize row that cannot download
        # must not start a fresh window on every wake.
        assert ts.fuse_wait_first_at is not None
    finally:
        store.close()


@pytest.mark.anyio
async def test_absent_picks_immediately(tmp_path):
    """Nothing at the target: today's flow, no wait window."""
    coord, store, _blob_bytes, _ = _scaffold(tmp_path, root_exists=False)
    try:
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_awaited_once()
        row = store.get(H)
        assert row.fuse_wait_first_at is None
        assert row.indexer_next_retry_at is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_unverifiable_mount_never_waits(tmp_path, monkeypatch):
    """A mount outage is not evidence: pick instead of parking."""
    import racing_sync.fuse_gate as fuse_gate

    monkeypatch.setattr(fuse_gate, "mount_is_mounted", lambda _m: False)
    coord, store, _blob_bytes, _ = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1), (EP2, SIZE2)])
    try:
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_awaited_once()
        assert store.get(H).fuse_wait_first_at is None
    finally:
        store.close()


def test_config_manual_move_window_validation():
    """timeout must be 0 (disabled) or >= one check interval."""
    from pydantic import ValidationError

    from racing_sync.config import FuseConfig

    with pytest.raises(ValidationError):
        FuseConfig(
            mount="/mnt/fuse",
            mount_unsorted="/mnt/fuse-u",
            manual_move_check_seconds=1800,
            manual_move_timeout_seconds=600,
        )
    # 0 disables the wait and is always accepted.
    cfg = FuseConfig(
        mount="/mnt/fuse",
        mount_unsorted="/mnt/fuse-u",
        manual_move_check_seconds=1800,
        manual_move_timeout_seconds=0,
    )
    assert cfg.manual_move_timeout_seconds == 0


@pytest.mark.anyio
async def test_present_admits_to_queued_without_reservation(tmp_path):
    """Full presence through the real admit tail: QUEUED, no SSD budget."""
    coord, store, _blob_bytes, _ = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1), (EP2, SIZE2)])
    try:
        ts = store.get(H)
        await coord._do_new(ts)

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.QUEUED
        assert getattr(coord, "_ssd_reserved", {}) == {}
    finally:
        store.close()


@pytest.mark.anyio
async def test_timeout_zero_disables_the_wait_and_the_precheck(tmp_path):
    """timeout=0 means "old behaviour": no probe, pick runs even when present."""
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path,
        fuse_files=[(EP1, SIZE1), (EP2, SIZE2)],
        timeout=0,
    )
    try:
        await coord._do_new(store.get(H))

        coord._pick_and_admit.assert_awaited_once()
        row = store.get(H)
        assert row.state == State.NEW
        assert row.fuse_wait_first_at is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_watch_origin_label_is_preserved(tmp_path):
    """A watch drop keeps its origin label; the re-add flow keys off it."""
    coord, store, blob, _ = _scaffold(tmp_path, fuse_files=[(EP1, SIZE1), (EP2, SIZE2)])
    coord._admit_resolved = AsyncMock()
    try:
        ts = store.get(H)
        ts.cross_seed_source = "watch-dir"
        await coord._admit_fuse_existing(ts, blob)

        assert ts.cross_seed_source == "watch-dir"
        assert ts.cross_seed_infohash == H
        assert ts.cross_seed_blob == blob
        coord._admit_resolved.assert_awaited_once()
    finally:
        store.close()


EPISODE = "Example.Show.S01E01.576p.WEB-DL.EAC3.H264-DUMMY.mkv"


def _episode_blob() -> bytes:
    return _bencode({b"info": {b"name": EPISODE.encode(), b"length": SIZE1}})


@pytest.mark.anyio
async def test_present_under_sorted_mount_for_an_unclassified_row(tmp_path):
    """Regression: a fresh row has no kind, so the sorted mount must be probed.

    The unsorted mount is usually nested inside the sorted one; probing only
    the kind-derived target made every manual season move look absent.
    """
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path, kind="", fuse_files=[(EP1, SIZE1), (EP2, SIZE2)]
    )
    coord._admit_resolved = AsyncMock()
    try:
        ts = store.get(H)
        await coord._do_new(ts)

        coord._pick_and_admit.assert_not_called()
        coord._admit_resolved.assert_awaited_once()
        assert ts.cross_seed_source == "fuse-existing"
    finally:
        store.close()


@pytest.mark.anyio
async def test_present_under_unsorted_mount_for_an_unclassified_row(tmp_path):
    """Episodes live under the nested unsorted mount; still found."""
    blob = _episode_blob()
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path,
        kind="",
        blob=blob,
        name=EPISODE,
        total_bytes=SIZE1,
        root_exists=False,
        fuse_files=[(EPISODE, SIZE1)],
        files_mount="unsorted",
    )
    coord._admit_resolved = AsyncMock()
    try:
        ts = store.get(H)
        await coord._do_new(ts)

        coord._pick_and_admit.assert_not_called()
        coord._admit_resolved.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_absent_with_a_running_window_keeps_waiting(tmp_path):
    """A copy seen earlier keeps the window open even if it is briefly absent."""
    start = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=120)
    coord, store, _blob_bytes, _ = _scaffold(
        tmp_path,
        state=State.WAITING_INDEXER,
        root_exists=False,
        fuse_wait_first_at=start,
    )
    try:
        before = dt.datetime.now(dt.UTC)
        await coord._do_waiting_indexer(store.get(H))

        coord._pick_and_admit.assert_not_called()
        row = store.get(H)
        assert row.state == State.WAITING_INDEXER
        assert row.fuse_wait_first_at is not None
        assert abs((row.indexer_next_retry_at - before).total_seconds() - 1800) < 60
        assert "manual move" in (row.last_error or "")
    finally:
        store.close()


@pytest.mark.anyio
async def test_oversize_single_file_waits_instead_of_failing(tmp_path):
    """A file too big for the SSD waits for the external copy, then fails."""
    from pathlib import Path as _Path

    from racing_sync.clients.abstract import AddResult, TorrentFile
    from racing_sync.config import AppConfig

    cfg = AppConfig.from_toml(_Path(__file__).parent.parent / "config.example.toml")
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    cfg.dest.save_path = ssd
    cfg.ssd.path = ssd
    cfg.rclone.fuse.mount = fuse
    cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    cfg.ssd.skip_movie_larger_than_bytes = 10_000
    cfg.ssd.max_inflight_bytes = 100_000_000_000
    cfg.rclone.fuse.manual_move_timeout_seconds = 43200
    cfg.rclone.fuse.manual_move_check_seconds = 1800

    coord = make_coordinator()
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client.add_torrent = AsyncMock(
        return_value=AddResult(hash="dd" * 20, accepted=True, detail="Ok.")
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack/huge.bin", size_bytes=50_000, progress=0.0)]
    )
    coord.dest_client.delete = AsyncMock()
    coord._await_hash_for_name = AsyncMock(return_value="dd" * 20)
    coord._ssd_release = AsyncMock()
    coord._schedule_telegram_update = MagicMock()

    def _set_state(t, s, error="", **kwargs):
        t.state = s
        t.last_error = error

    coord.transition = MagicMock(side_effect=_set_state)

    ts = TorrentState(
        source_infohash="e" * 40,
        source_name="Pack",
        total_bytes=51000,
        cross_seed_blob=b"blob",
        save_path=str(ssd),
        state=State.QUEUED,
    )
    await coord._do_queued(ts)

    assert ts.state == State.WAITING_INDEXER, "an unfittable file must wait, not fail"
    assert ts.fuse_wait_first_at is not None
    assert "manual move" in (ts.last_error or "")
    coord.dest_client.delete.assert_awaited_once_with("dd" * 20, delete_files=False)
    coord._ssd_release.assert_awaited()
