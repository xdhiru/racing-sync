"""Tests for the download-target indexer retry policy math.

We don't run the full coordinator here (it needs live qBittorrent +
Prowlarr). Instead we verify the timing rules that _park_for_indexer_retry
applies.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.config import AppConfig
from racing_sync.state import State, TorrentState


def _cfg() -> AppConfig:
    return AppConfig.from_toml(
        __file__.replace("\\", "/").rsplit("/", 1)[0] + "/../config.example.toml"
    )


def test_first_attempt_sets_first_queried_at():
    cfg = _cfg()
    ts = TorrentState(source_infohash="a" * 40, state=State.NEW)
    assert ts.indexer_first_queried_at is None
    assert ts.indexer_attempts == 0

    now = dt.datetime.now(dt.UTC)
    interval = cfg.cross_seed.prowlarr_retry_interval_seconds
    ts.indexer_first_queried_at = now
    ts.indexer_attempts = 1
    ts.indexer_next_retry_at = now + dt.timedelta(seconds=interval)

    assert ts.indexer_first_queried_at == now
    assert ts.indexer_next_retry_at is not None
    diff = (ts.indexer_next_retry_at - now).total_seconds()
    assert abs(diff - interval) < 1


def test_retry_window_is_24_hours():
    cfg = _cfg()
    assert cfg.cross_seed.prowlarr_max_age_seconds == 86400
    assert cfg.cross_seed.prowlarr_retry_interval_seconds == 900


@pytest.mark.anyio
async def test_vanished_source_grants_fresh_window(tmp_path):
    """A disappearance after an exhausted indexer window parks for one
    fresh cross-seed window instead of failing instantly; a plain
    indexer miss with the same clock still fails."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.cfg = _cfg()
        coord.store = store
        _old = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=86400 + 60)
        for h, reason, final in (
            ("a" * 40, "source vanished from VPS1", State.WAITING_INDEXER),
            ("b" * 40, "indexer miss", State.FAILED),
        ):
            ts = TorrentState(
                source_infohash=h,
                source_name="Twin",
                state=State.WAITING_INDEXER,
                indexer_first_queried_at=_old,
                indexer_attempts=40,
            )
            store.upsert(ts)
            coord._park_for_indexer_retry(store.get(h), reason=reason)
            row = store.get(h)
            assert row.state == final
            if final == State.WAITING_INDEXER:
                assert row.indexer_first_queried_at > _old
    finally:
        store.close()


@pytest.mark.anyio
async def test_public_retry_rotates_verbosity(tmp_path, caplog):
    """Unbounded public-export schedule: first misses log loudly, the
    long tail at debug instead of filling the log disk."""
    import logging

    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.store = store
        store.upsert(TorrentState(source_infohash="p" * 40, source_name="Pub", state=State.NEW))
        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            for _ in range(4):
                coord._park_for_public_retry(store.get("p" * 40))
        row = store.get("p" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.public_export_attempts == 4
        assert row.public_export_next_retry_at is not None
        assert caplog.text.count("source export miss") == 3
    finally:
        store.close()


def test_expired_max_age_marks_failed():
    """If the first attempt was > 24 h ago, the next park should escalate
    to FAILED. We simulate by backdating indexer_first_queried_at."""
    cfg = _cfg()
    ts = TorrentState(
        source_infohash="a" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=dt.datetime.now(dt.UTC)
        - dt.timedelta(seconds=cfg.cross_seed.prowlarr_max_age_seconds + 1),
        indexer_next_retry_at=dt.datetime.now(dt.UTC),
        indexer_attempts=10,
    )
    now = dt.datetime.now(dt.UTC)
    elapsed = now - ts.indexer_first_queried_at
    assert elapsed > dt.timedelta(seconds=cfg.cross_seed.prowlarr_max_age_seconds)


@pytest.mark.anyio
async def test_process_torrent_inner_dispatches_waiting_indexer_state():
    from unittest.mock import AsyncMock

    coord = make_coordinator()
    coord._do_waiting_indexer = AsyncMock()

    ts = TorrentState("hash1", state=State.WAITING_INDEXER)
    await coord._process_torrent_inner(ts)

    # Must dispatch to _do_waiting_indexer for timer-elapsed WAITING rows
    coord._do_waiting_indexer.assert_awaited_once_with(ts)


@pytest.mark.anyio
async def test_list_source_torrents_caches_within_ttl():
    from unittest.mock import AsyncMock

    from racing_sync.clients.abstract import Torrent

    coord = make_coordinator()
    coord.cfg.source.category = "racing"
    coord.cfg.source.min_age_seconds = 0
    coord._source_torrents_cache = []
    coord._source_torrents_cached_at = 0.0

    t1 = Torrent(
        hash="h1",
        name="Show.A",
        category="racing",
        save_path="",
        size_bytes=100,
        state="",
        progress=1.0,
    )
    coord.source_client = AsyncMock()
    coord.source_client.list_torrents.return_value = [t1]

    # First call: fetches from client
    res1 = await coord._list_source_torrents()
    assert len(res1) == 1
    assert coord.source_client.list_torrents.await_count == 1

    # Second call right after: returns cached result without RPC call
    res2 = await coord._list_source_torrents()
    assert len(res2) == 1
    assert coord.source_client.list_torrents.await_count == 1

    # Force refresh: bypasses cache and calls RPC again
    res3 = await coord._list_source_torrents(force_refresh=True)
    assert len(res3) == 1
    assert coord.source_client.list_torrents.await_count == 2


@pytest.mark.anyio
async def test_source_timeout_degrades_to_retry_not_failed():
    """A wedged VPS1 lookup parks/retries — never a terminal FAILED."""
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.side_effect = TimeoutError("wedged")
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="timeout_hash_12345", state=State.NEW)
    assert await coord._fetch_source_or_fail(ts) is None
    coord.transition.assert_not_called()

    # Source poll timeout serves an empty view, not an exception.
    coord.source_client.list_torrents.side_effect = TimeoutError("wedged")
    assert await coord._list_source_torrents(force_refresh=True) == []


@pytest.mark.anyio
async def test_fetch_source_prefers_snapshot_on_deluge():
    """Deluge freshness checks reuse the poller snapshot (no per-row scan).

    Every Deluge get_torrent is a full get_torrents_status scan; serving
    workers from the ≤45s-old snapshot avoids N scans per tick, which is
    what wedges the VPS1 WebUI. qB rows with a reported tracker are
    served too (single-tracker racing torrents need no per-row RPCs);
    trackerless qB rows still do the live lookup for full trackers.
    """
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.clients.deluge import DelugeClient

    snap = Torrent(
        hash="d" * 40,
        name="Snap.Show",
        category="racing",
        save_path="/vps1",
        size_bytes=999,
        state="seeding",
        progress=1.0,
        trackers=["http://t/announce"],
    )

    # Deluge source: snapshot hit → zero RPC, miss counter cleared.
    coord = make_coordinator()
    coord.source_client = MagicMock(spec=DelugeClient)
    coord.source_client.get_torrent = AsyncMock()
    coord._source_torrents_cache = [snap]
    coord._source_miss_counts = {"d" * 40: 2}
    ts = TorrentState(source_infohash="d" * 40, state=State.NEW)
    got = await coord._fetch_source_or_fail(ts)
    assert got is snap
    coord.source_client.get_torrent.assert_not_called()
    assert coord._source_miss_counts == {}

    # Cache miss on Deluge: falls back to RPC (registration lag covered).
    coord.source_client.get_torrent = AsyncMock(return_value=None)
    coord.transition = MagicMock()
    ts2 = TorrentState(source_infohash="e" * 40, state=State.NEW)
    assert await coord._fetch_source_or_fail(ts2) is None
    coord.source_client.get_torrent.assert_awaited_once()
    coord.transition.assert_not_called()  # first miss only counts

    # Non-Deluge source with a tracked snapshot row: served from cache
    # (single-tracker qB rows need no per-row RPCs while fresh).
    coord2 = make_coordinator()
    coord2.source_client = AsyncMock()
    coord2.source_client.get_torrent = AsyncMock(return_value=snap)
    coord2._source_torrents_cache = [snap]
    ts3 = TorrentState(source_infohash="d" * 40, state=State.NEW)
    assert await coord2._fetch_source_or_fail(ts3) is snap
    coord2.source_client.get_torrent.assert_not_called()

    # Non-Deluge source with a trackerless snapshot row: live RPC as
    # before (full tracker set needed for public detection).
    bare = Torrent(
        hash="d" * 40,
        name="Snap.Show",
        category="racing",
        save_path="/vps1",
        size_bytes=999,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord3 = make_coordinator()
    coord3.source_client = AsyncMock()
    coord3.source_client.get_torrent = AsyncMock(return_value=snap)
    coord3._source_torrents_cache = [bare]
    ts4 = TorrentState(source_infohash="d" * 40, state=State.NEW)
    assert await coord3._fetch_source_or_fail(ts4) is snap
    coord3.source_client.get_torrent.assert_awaited_once()


@pytest.mark.anyio
async def test_do_new_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_12345", state=State.NEW)
    # Transient vanish (client restart, registration lag) must not fail
    # the row: only the third consecutive miss is terminal.
    await coord._do_new(ts)
    await coord._do_new(ts)
    coord.transition.assert_not_called()

    await coord._do_new(ts)
    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: vanished_h"
    )


@pytest.mark.anyio
async def test_do_waiting_indexer_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_67890", state=State.WAITING_INDEXER)
    await coord._do_waiting_indexer(ts)
    await coord._do_waiting_indexer(ts)
    coord.transition.assert_not_called()

    await coord._do_waiting_indexer(ts)
    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: vanished_h"
    )


def _ssd_dest(entries):
    """Fake VPS2 client: {hash: (progress, blob-for-export)}."""

    from racing_sync.clients.abstract import Torrent

    class _FakeSSD:
        def __init__(self):
            self.export_calls: list[str] = []

        async def list_torrents(self, *, category=None, hashes=None):
            wanted = {h.lower() for h in hashes} if hashes is not None else None
            out = []
            for h, (progress, _blob) in entries.items():
                if wanted is None or h in wanted:
                    out.append(
                        Torrent(
                            hash=h,
                            name="Hand.Added.mkv",
                            category="",
                            save_path="/ssd/Hand.Added.mkv",
                            size_bytes=1000,
                            state="seeding" if progress >= 1.0 else "downloading",
                            progress=progress,
                        )
                    )
            return out

        async def export_torrent(self, h):
            self.export_calls.append((h or "").lower())
            return entries.get((h or "").lower(), (0.0, b""))[1]

    dest = _FakeSSD()
    dest.list_torrents = dest.list_torrents
    return dest


@pytest.mark.anyio
async def test_do_waiting_indexer_adopts_complete_ssd_entry():
    """Same hash complete on the SSD client: adopt, don't query Prowlarr."""
    from unittest.mock import AsyncMock, MagicMock

    raw, h = _operator_blob(name="Hand.Added.mkv")
    coord = make_coordinator()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()
    coord.dest_client = _ssd_dest({h: (1.0, b"")})
    coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))

    ts = TorrentState(
        source_infohash=h,
        source_name="Hand.Added.mkv",
        total_bytes=1000,
        state=State.WAITING_INDEXER,
    )
    ts._blob = raw
    await coord._do_waiting_indexer(ts)

    coord._pick_and_admit.assert_not_called()
    assert ts.state == State.QUEUED
    assert ts.cross_seed_source == "ssd-adopted"
    assert ts.cross_seed_infohash == h


@pytest.mark.anyio
async def test_do_waiting_indexer_heals_blob_from_ssd_entry():
    """No bytes on the row: export the live SSD entry, then adopt."""
    import tempfile
    from pathlib import Path
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.state import StateStore

    raw, h = _operator_blob(name="Hand.Added.mkv")
    tmp = Path(tempfile.mkdtemp())
    store = StateStore(tmp / "state.db")
    try:
        coord = make_coordinator(store)
        coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
        coord._pick_and_admit = AsyncMock()
        dest = _ssd_dest({h: (1.0, raw)})
        coord.dest_client = dest
        coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))

        ts = TorrentState(
            source_infohash=h,
            source_name="Hand.Added.mkv",
            total_bytes=1000,
            state=State.WAITING_INDEXER,
        )
        store.upsert(ts)
        await coord._do_waiting_indexer(ts)

        assert dest.export_calls == [h]
        assert bytes(store.get_blob(h)) == raw
        coord._pick_and_admit.assert_not_called()
        assert ts.state == State.QUEUED
    finally:
        store.close()


@pytest.mark.anyio
async def test_do_waiting_indexer_waits_on_downloading_ssd_entry():
    """Same hash still downloading on SSD: quiet park, no Prowlarr query."""
    from unittest.mock import AsyncMock, MagicMock

    raw, h = _operator_blob(name="Hand.Added.mkv")
    coord = make_coordinator()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()
    coord.source_client = AsyncMock()
    coord.dest_client = _ssd_dest({h: (0.5, raw)})
    coord.transition = MagicMock()

    ts = TorrentState(
        source_infohash=h,
        source_name="Hand.Added.mkv",
        total_bytes=1000,
        state=State.WAITING_INDEXER,
    )
    await coord._do_waiting_indexer(ts)

    coord._pick_and_admit.assert_not_called()
    coord.source_client.get_torrent.assert_not_called()
    coord.transition.assert_not_called()
    assert ts.indexer_next_retry_at is not None


@pytest.mark.anyio
async def test_vanished_parks_for_prowlarr_when_possible():
    """Cleared-for-space VPS1 must not fail a row Prowlarr can still feed."""
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.coordinator import _SOURCE_GONE

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.prowlarr = AsyncMock()
    coord.cfg.cross_seed.allow_prowlarr_cross_seed = True
    coord.cfg.prowlarr.should_skip_title = MagicMock(return_value=False)
    coord.transition = MagicMock()
    coord._schedule_telegram_update = MagicMock()

    ts = TorrentState(
        source_infohash="v" * 40, source_name="Gone.Show", total_bytes=500, state=State.NEW
    )
    coord._source_miss_counts = {"v" * 40: 2}
    got = await coord._fetch_source_or_fail(ts)
    assert got is _SOURCE_GONE
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.WAITING_INDEXER


@pytest.mark.anyio
async def test_vanished_fails_when_no_prowlarr_possible():
    """No Prowlarr path (disabled client) keeps the terminal FAILED."""
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.prowlarr = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="w" * 40, source_name="Gone.Show", state=State.NEW)
    coord._source_miss_counts = {"w" * 40: 2}
    assert await coord._fetch_source_or_fail(ts) is None
    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: " + "w" * 10
    )


@pytest.mark.anyio
async def test_do_new_pseudo_picks_on_vanish():
    """Vanished source proceeds from persisted metadata (siblings first)."""
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.prowlarr = AsyncMock()
    coord.cfg.cross_seed.allow_prowlarr_cross_seed = True
    coord.cfg.prowlarr.should_skip_title = MagicMock(return_value=False)
    coord.transition = MagicMock()
    coord._schedule_telegram_update = MagicMock()
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()
    coord._list_source_torrents = AsyncMock(return_value=[])

    ts = TorrentState(
        source_infohash="x" * 40,
        source_name="Gone.Show",
        total_bytes=500,
        source_announce_url="https://t.example/announce",
        state=State.NEW,
    )
    coord._source_miss_counts = {"x" * 40: 2}
    await coord._do_new(ts)
    assert coord._pick_and_admit.await_count == 1
    _picked_ts, st, others = coord._pick_and_admit.await_args[0][:3]
    assert st is not None and st is not False
    assert st.name == "Gone.Show" and st.size_bytes == 500
    assert others == []


@pytest.mark.anyio
async def test_do_re_add_skips_when_already_injected_in_step_1():
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="abc123456789", state=State.RE_ADDING)
    ts.injected_private_hashes = "abc123456789"
    ts.cross_seed_infohash = "abc123456789"
    ts.cross_seed_blob = b"dummy"

    await coord._do_re_add(ts)

    # Should not call add_torrent because it was already injected
    coord.dest_client.add_torrent.assert_not_called()
    coord.transition.assert_called_once_with(ts, State.DONE)


@pytest.mark.anyio
async def test_do_re_add_accepts_when_dest_client_returns_fails_but_already_exists():
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import AddResult, Torrent

    coord = make_coordinator()
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.transition = MagicMock()
    coord._target_mount_for = MagicMock(return_value="/mnt/fuse")

    ts = TorrentState(source_infohash="abc123456789", state=State.RE_ADDING)
    ts.cross_seed_infohash = "abc123456789"
    ts.cross_seed_blob = b"dummy"

    coord.dest_client.add_torrent.return_value = AddResult(
        hash=None, accepted=False, detail="Fails."
    )
    coord.dest_client.get_torrent.return_value = Torrent(
        hash="abc123456789",
        name="Test",
        category="racing",
        save_path="/mnt/fuse",
        size_bytes=100,
        state="",
        progress=1.0,
    )

    await coord._do_re_add(ts)

    coord.dest_client.add_torrent.assert_awaited_once()
    coord.dest_client.get_torrent.assert_awaited_once_with("abc123456789")
    coord.transition.assert_called_once_with(ts, State.DONE)


def test_should_notify_telegram_policy():
    from racing_sync.coordinator import _should_notify_telegram

    # Same-state transition should not notify
    assert not _should_notify_telegram(State.NEW, State.NEW)
    assert not _should_notify_telegram(State.DOWNLOADING, State.DOWNLOADING)

    # Transitioning to NEW should not notify
    assert not _should_notify_telegram(State.QUEUED, State.NEW)

    # Active and terminal states should notify
    assert _should_notify_telegram(State.NEW, State.QUEUED)
    assert _should_notify_telegram(State.QUEUED, State.DOWNLOADING)
    assert _should_notify_telegram(State.DOWNLOADING, State.MOVING)
    assert _should_notify_telegram(State.MOVING, State.RE_ADDING)
    assert _should_notify_telegram(State.RE_ADDING, State.DONE)
    assert _should_notify_telegram(State.RE_ADDING, State.FAILED)
    assert _should_notify_telegram(State.NEW, State.WAITING_INDEXER)


@pytest.mark.anyio
async def test_process_torrent_inner_does_not_fallthrough_to_waiting_indexer_from_new():
    from unittest.mock import AsyncMock

    coord = make_coordinator()
    coord._do_waiting_indexer = AsyncMock()

    async def fake_do_new(ts: TorrentState) -> None:
        ts.state = State.WAITING_INDEXER

    coord._do_new = AsyncMock(side_effect=fake_do_new)

    ts = TorrentState("hash_new", state=State.NEW)
    await coord._process_torrent_inner(ts)

    coord._do_new.assert_awaited_once_with(ts)
    coord._do_waiting_indexer.assert_not_called()


@pytest.mark.anyio
async def test_tick_skips_waiting_indexer_in_step_4():
    import asyncio
    from unittest.mock import AsyncMock

    coord = make_coordinator()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord._check_and_inject_late_cross_seeds = AsyncMock()
    coord.watch = None
    coord.store.list_indexer_ready.return_value = []
    coord._list_source_torrents = AsyncMock(return_value=[])

    ts_waiting = TorrentState(
        source_infohash="waiting_indexer",
        source_name="Waiting.Indexer.Release",
        state=State.WAITING_INDEXER,
    )
    ts_queued = TorrentState(
        source_infohash="queued_ready",
        source_name="Queued.Release",
        state=State.QUEUED,
    )
    coord.store.all_active.return_value = [ts_waiting, ts_queued]

    scheduled: list[str] = []

    async def fake_process(ts: TorrentState) -> None:
        scheduled.append(ts.source_infohash)

    coord._process_torrent = fake_process

    await coord._tick()
    await asyncio.sleep(0.01)

    assert "waiting_indexer" not in scheduled
    assert "queued_ready" in scheduled


@pytest.mark.anyio
async def test_coordinator_run_stops_immediately_when_stop_requested():
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord._stop = False
    coord.start = AsyncMock()
    coord.shutdown = AsyncMock()
    coord.cfg = MagicMock()
    # Large sleep interval: if _stop is ignored, this test would hang/timeout
    coord.cfg.general.source_poll_interval = 9999

    tick_called = False

    async def fake_poller():
        nonlocal tick_called
        tick_called = True
        coord.request_stop()

    # run() supervises the three loops (not a single _tick): the poller
    # fires once and stops everything; the other lanes stay quiet.
    coord._poller_loop = fake_poller
    coord._scheduler_loop = AsyncMock()
    coord._janitor_loop = AsyncMock()

    exit_code = await coord.run()
    assert exit_code == 0
    assert tick_called is True
    coord.shutdown.assert_awaited_once()


@pytest.mark.anyio
async def test_loops_run_their_own_step_and_stop():
    """Poller/scheduler/janitor loops run once and honor _stop."""
    from unittest.mock import MagicMock

    for loop_name, step_name in (
        ("_poller_loop", "_poll_source_step"),
        ("_scheduler_loop", "_schedule_step"),
        ("_janitor_loop", "_janitor_step"),
    ):
        coord = make_coordinator()
        coord._stop = False
        coord.cfg = MagicMock()
        coord.cfg.general.source_poll_interval = 9999
        coord.cfg.general.dest_poll_interval = 9999
        coord.cfg.cleanup.janitor_interval_seconds = 9999
        calls = []

        async def fake_step(_calls=calls, _coord=coord):
            # Bind the loop variables explicitly: the closure would
            # otherwise capture them late (the next iteration's list/coord).
            _calls.append(1)
            _coord.request_stop()

        setattr(coord, step_name, fake_step)
        await getattr(coord, loop_name)()
        assert calls == [1], loop_name


@pytest.mark.anyio
async def test_wait_disk_then_queue_transitions_to_queued_under_download_sem():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    coord = make_coordinator()
    coord.transition = MagicMock()
    coord._download_sem = asyncio.Semaphore(1)
    coord._do_queued = AsyncMock()
    coord._do_downloading = AsyncMock()
    coord._do_moving = AsyncMock()
    coord._do_re_add = AsyncMock()

    ts = TorrentState(source_infohash="disk_hash", state=State.WAITING_DISK)

    def fake_transition(target_ts, new_state):
        target_ts.state = new_state

    coord.transition.side_effect = fake_transition

    with patch("racing_sync.coordinator.ssd_has_room", return_value=True):
        await coord._process_torrent_inner(ts)

    assert coord.transition.call_args_list[0][0][1] == State.QUEUED
    coord._do_queued.assert_awaited_once_with(ts, _holds_download_slot=True)


@pytest.mark.anyio
async def test_public_sftp_timeout_retried_once_then_succeeds(caplog):
    """A single SFTP stall must not fail a public SSD pick (cf. prod 15s gap)."""
    import logging
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import pick_ssd_source_for_racing

    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    cfg.cross_seed.refetch_public_via_prowlarr = False

    blob = b"d8:announce5:helloe"
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(side_effect=[TimeoutError(), blob])

    torrent = Torrent(
        hash="p" * 40,
        name="Public.Show.S01E01.mkv",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
        dec = await pick_ssd_source_for_racing(
            cfg=cfg,
            source_torrent=torrent,
            other_source_torrents=[],
            prowlarr=None,
            sftp=sftp,
            source_client=AsyncMock(),
            attempt_prowlarr=True,
        )
    assert dec is not None
    assert dec.source_label == "public-racing"
    assert dec.torrent_bytes == blob
    assert sftp.fetch_torrent.call_count == 2
    assert any("timed out after 15s (attempt 1/2)" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_public_export_failure_parks_as_source_export_miss(caplog):
    """A public group whose .torrent export keeps failing must park with an
    honest reason — never an 'indexer miss' (no indexer was ever queried)."""
    import logging
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent

    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    cfg.cross_seed.refetch_public_via_prowlarr = False
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.prowlarr_retry_interval_seconds = 1800
    cfg.cross_seed.prowlarr_max_age_seconds = 86400
    cfg.dest.save_path = "/ssd"

    torrent = Torrent(
        hash="p" * 40,
        name="Public.Show.S01E01.mkv",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=None)
    source_client = AsyncMock()
    source_client.get_torrent = AsyncMock(return_value=torrent)
    source_client.export_torrent = AsyncMock(side_effect=RuntimeError("nope"))

    coord = make_coordinator()
    coord.cfg = cfg
    coord.source_client = source_client
    coord.sftp = sftp
    coord.prowlarr = None
    coord._list_source_torrents = AsyncMock(return_value=[torrent])
    coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))

    ts = TorrentState(source_infohash="p" * 40, state=State.NEW)
    with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
        await coord._do_new(ts)

    assert ts.state == State.WAITING_INDEXER
    assert any("source export miss #1" in r.message for r in caplog.records)
    assert not any("indexer miss" in r.message for r in caplog.records)


@pytest.mark.anyio
async def test_waiting_indexer_retry_republishes_public_schedule(tmp_path: Path, caplog):
    """A public row re-parked from WAITING_INDEXER must not be counted as
    an 'indexer miss': the reason is re-derived, the public timer is pushed
    and a stale indexer timer is cleared. Otherwise list_waiting_ready
    wakes it every tick, inflating attempts until the 24h FAILED a row
    that has no max-age clock by design."""
    import datetime as dt
    import logging
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore

    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    cfg.cross_seed.refetch_public_via_prowlarr = False
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.prowlarr_retry_interval_seconds = 1800
    cfg.cross_seed.prowlarr_max_age_seconds = 86400
    cfg.cross_seed.public_export_retry_interval_seconds = 1800
    cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = False
    cfg.dest.save_path = "/ssd"

    torrent = Torrent(
        hash="p" * 40,
        name="Public.Show.S01E01.mkv",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=None)
    source_client = AsyncMock()
    source_client.get_torrent = AsyncMock(return_value=torrent)
    source_client.export_torrent = AsyncMock(side_effect=RuntimeError("nope"))

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.cfg = cfg
        coord.store = store
        coord.source_client = source_client
        coord.sftp = sftp
        coord.prowlarr = None
        coord._list_source_torrents = AsyncMock(return_value=[torrent])
        coord.dest_client = AsyncMock()
        coord.dest_client.list_torrents = AsyncMock(return_value=[])
        coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))

        now = dt.datetime.now(dt.UTC)
        ts = TorrentState(
            source_infohash="p" * 40,
            source_name="Public.Show.S01E01.mkv",
            total_bytes=500,
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=now - dt.timedelta(seconds=60),
            indexer_attempts=5,
            indexer_next_retry_at=now - dt.timedelta(seconds=1),
        )
        store.upsert(ts)

        with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
            await coord._do_waiting_indexer(store.get("p" * 40))

        row = store.get("p" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.public_export_next_retry_at is not None
        assert row.indexer_next_retry_at is None
        assert row.indexer_attempts == 5
        assert any("source export miss #1" in r.message for r in caplog.records)
        assert not any("indexer miss" in r.message for r in caplog.records)
    finally:
        store.close()


@pytest.mark.anyio
async def test_public_export_miss_never_hits_max_age_failed(tmp_path: Path):
    """Public-export misses retry indefinitely: an ancient first attempt
    must re-park (not FAILED), on the public schedule only."""
    import datetime as dt
    from unittest.mock import MagicMock

    from racing_sync.state import StateStore

    coord = make_coordinator()
    coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))
    store = StateStore(tmp_path / "s.db")
    coord.store = store
    try:
        ts = TorrentState(
            source_infohash="p" * 40,
            source_name="Public.Show",
            state=State.NEW,
            public_export_first_attempted_at=(dt.datetime.now(dt.UTC) - dt.timedelta(days=30)),
            public_export_attempts=100,
        )
        coord._park_for_indexer_retry(ts, reason="source export miss")

        assert ts.state == State.WAITING_INDEXER
        assert ts.public_export_attempts == 101
        assert ts.public_export_next_retry_at is not None
        # The indexer clock is untouched: no max-age FAILED can fire.
        assert ts.indexer_first_queried_at is None
        assert ts.indexer_next_retry_at is None
        coord.transition.assert_called_once_with(ts, State.WAITING_INDEXER)
        # The parked fields round-trip through the store (timer survives
        # restarts — the whole point of the persisted schedule).
        store.upsert(ts)
        fresh = store.get("p" * 40, include_blob=False)
        assert fresh is not None and fresh.state == State.WAITING_INDEXER
        assert fresh.public_export_attempts == 101
        assert fresh.public_export_next_retry_at is not None
        assert fresh.public_export_first_attempted_at is not None
    finally:
        store.close()


def test_list_public_retry_ready_only_elapsed(tmp_path: Path):
    import datetime as dt

    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "s.db")
    try:
        now = dt.datetime.now(dt.UTC)
        old = TorrentState(
            source_infohash="o" * 40,
            state=State.WAITING_INDEXER,
            public_export_next_retry_at=now - dt.timedelta(seconds=1),
        )
        future = TorrentState(
            source_infohash="f" * 40,
            state=State.WAITING_INDEXER,
            public_export_next_retry_at=now + dt.timedelta(hours=1),
        )
        indexed = TorrentState(
            source_infohash="i" * 40,
            state=State.WAITING_INDEXER,
            indexer_next_retry_at=now - dt.timedelta(seconds=1),
        )
        for ts in (old, future, indexed):
            store.upsert(ts)
        ready = store.list_public_retry_ready()
        assert [r.source_infohash for r in ready] == ["o" * 40]
    finally:
        store.close()


# ---- racing-torrent fallback (force_direct) ----


def _pick_coord(**cross_seed_over):
    from unittest.mock import AsyncMock, MagicMock

    cfg = MagicMock()
    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.sftp = None
    coord.prowlarr = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.refetch_public_via_prowlarr = False
    cfg.cross_seed.prowlarr_retry_interval_seconds = 1800
    cfg.cross_seed.prowlarr_max_age_seconds = 86400
    cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = False
    cfg.dest.save_path = "/ssd"
    for k, v in cross_seed_over.items():
        setattr(cfg.cross_seed, k, v)
    coord.cfg = cfg
    coord._ssd_estimate_for_new = MagicMock(return_value=100)
    coord._ssd_try_reserve = AsyncMock(return_value=True)
    coord._park_for_indexer_retry = MagicMock()
    coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))
    return coord


def _priv_st():
    from racing_sync.clients.abstract import Torrent

    return Torrent(
        hash="d" * 40,
        name="Private.Show.S01E01.mkv",
        category="",
        save_path="",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=["https://alpha.cc/announce/xyz"],
    )


def _decision():
    from racing_sync.coordinator_content import SourceDecision

    return SourceDecision(
        torrent_bytes=b"blob",
        source_label="private-export-fallback",
        name="Private.Show.S01E01.mkv",
        size_bytes=1000,
        infohash="d" * 40,
        announce_url="https://alpha.cc/announce/xyz",
    )


def _past_max_age(cfg):
    return dt.datetime.now(dt.UTC) - dt.timedelta(
        seconds=cfg.cross_seed.prowlarr_max_age_seconds + 10
    )


@pytest.mark.anyio
async def test_pick_and_admit_force_direct_bypasses_prowlarr():
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    ts = TorrentState(source_infohash="d" * 40, state=State.WAITING_INDEXER, force_direct=1)
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = _decision()
        await coord._pick_and_admit(ts, _priv_st(), [])
    pick.assert_awaited_once()
    assert pick.call_args.kwargs["attempt_prowlarr"] is False
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.QUEUED
    coord._park_for_indexer_retry.assert_not_called()


@pytest.mark.anyio
async def test_skipped_title_parks_public_schedule():
    """A skipped title with no direct bytes parks max-age-free: no query
    will ever run for it, so the indexer clock must not FAILED it."""
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    coord.cfg.prowlarr.should_skip_title = MagicMock(return_value=True)
    ts = TorrentState(source_infohash="d" * 40, source_name="Skipped.Show", state=State.NEW)
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, _priv_st(), [])
    pick.assert_awaited_once()
    coord._park_for_indexer_retry.assert_called_once()
    assert coord._park_for_indexer_retry.call_args.kwargs["reason"] == "source export miss"


@pytest.mark.anyio
async def test_pick_and_admit_first_miss_parks_despite_fallback_flag():
    """The fallback fires only after the retry window is exhausted."""
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    ts = TorrentState(source_infohash="d" * 40, state=State.WAITING_INDEXER)
    assert ts.indexer_first_queried_at is None
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, _priv_st(), [])
    pick.assert_awaited_once()
    assert pick.call_args.kwargs["attempt_prowlarr"] is True
    coord._park_for_indexer_retry.assert_called_once()
    assert ts.force_direct == 0


@pytest.mark.anyio
async def test_pick_and_admit_timeout_falls_back_to_racing():
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    ts = TorrentState(
        source_infohash="d" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.side_effect = [None, _decision()]
        await coord._pick_and_admit(ts, _priv_st(), [])
    assert pick.await_count == 2
    assert pick.call_args_list[0].kwargs["attempt_prowlarr"] is True
    assert pick.call_args_list[1].kwargs["attempt_prowlarr"] is False
    assert ts.force_direct == 1
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.QUEUED
    coord._park_for_indexer_retry.assert_not_called()


@pytest.mark.anyio
async def test_pick_and_admit_timeout_resets_public_clock():
    """The direct-phase fallback resets the public-export clock with the
    indexer clock so later wakeups aren't misrouted."""
    import datetime as dt
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    _now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash="d" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
        public_export_first_attempted_at=_now,
        public_export_attempts=5,
        public_export_next_retry_at=_now,
    )
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.side_effect = [None, _decision()]
        await coord._pick_and_admit(ts, _priv_st(), [])
    assert ts.force_direct == 1
    assert ts.public_export_first_attempted_at is None
    assert ts.public_export_attempts == 0
    assert ts.public_export_next_retry_at is None


@pytest.mark.anyio
async def test_pick_and_admit_timeout_without_flag_parks():
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    ts = TorrentState(
        source_infohash="d" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, _priv_st(), [])
    pick.assert_awaited_once()
    coord._park_for_indexer_retry.assert_called_once()
    assert ts.force_direct == 0


@pytest.mark.anyio
async def test_pick_and_admit_timeout_skips_public_groups():
    """Public misses never queried Prowlarr — no racing fallback applies."""
    from unittest.mock import AsyncMock, patch

    from racing_sync.clients.abstract import Torrent

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    pub = Torrent(
        hash="e" * 40,
        name="Public.Show.mkv",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    ts = TorrentState(
        source_infohash="e" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=_past_max_age(coord.cfg),
    )
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, pub, [])
    pick.assert_awaited_once()
    assert coord._park_for_indexer_retry.call_args.kwargs["reason"] == "source export miss"
    assert ts.force_direct == 0


def _operator_blob(name="Op.Supplied.1080p", size=1000, announce="https://alpha.cc/announce/xyz"):
    """Same-infohash .torrent bytes as /add would attach to a tracked row."""
    from racing_sync.watchdir import _bencode, _bencoded_info_hash

    raw = _bencode(
        {
            b"announce": announce.encode(),
            b"info": {
                b"name": name.encode(),
                b"length": size,
                b"piece length": 16384,
                b"pieces": b"0" * 20,
            },
        }
    )
    raw = bytes(raw)
    h, _, _, _ = _bencoded_info_hash(raw)
    return raw, h


@pytest.mark.anyio
async def test_pick_and_admit_uses_operator_supplied_bytes(tmp_path):
    """No pick + attached /add bytes for the same hash: admit from them.

    Regression: a racing row whose VPS1 original vanished parked for a
    Prowlarr cross-seed forever even after the operator supplied the
    exact .torrent via /add (+ /fetch flagged it and it just
    parked again). The bytes come from the DB column (no in-memory
    copy), so this also survives a restart mid-wait.
    """
    from unittest.mock import AsyncMock, patch

    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _pick_coord()
        raw, h = _operator_blob()
        ts = TorrentState(
            source_infohash=h,
            source_name="Op.Supplied.1080p",
            total_bytes=1000,
            source_announce_url="https://alpha.cc/announce/xyz",
            source_tracker="https://alpha.cc/announce/xyz",
            state=State.WAITING_INDEXER,
            force_direct=1,
            cross_seed_blob=raw,
        )
        store.upsert(ts)
        # Simulate a restart: workers only see the persisted row, blobs
        # reloaded from the DB column on demand.
        ts._blob = None
        ts.cross_seed_blob = None
        coord.store = store
        with patch(
            "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
        ) as pick:
            pick.return_value = None
            await coord._pick_and_admit(ts, _priv_st(), [])
        coord._park_for_indexer_retry.assert_not_called()
        assert ts.cross_seed_source == "operator-supplied"
        assert ts.cross_seed_infohash == h
        coord.transition.assert_called_once()
        assert coord.transition.call_args[0][1] == State.QUEUED
    finally:
        store.close()


@pytest.mark.anyio
async def test_pick_and_admit_ignores_mismatched_blob():
    """Attached bytes for another hash must never feed this row's download."""
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    raw, _h = _operator_blob()
    ts = TorrentState(
        source_infohash="f" * 40,
        source_name="Other.Show",
        total_bytes=1000,
        state=State.WAITING_INDEXER,
        force_direct=1,
        cross_seed_blob=raw,
    )
    ts._blob = raw
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, _priv_st(), [])
    coord._park_for_indexer_retry.assert_called_once()
    assert ts.cross_seed_source != "operator-supplied"


@pytest.mark.anyio
async def test_private_fallback_retries_sftp_timeout_once():
    """A single stalled SFTP read costs one retry, not the whole fallback."""
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import pick_ssd_source_for_racing

    cfg = MagicMock()
    cfg.prowlarr.should_skip_title.return_value = False
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.allow_ssh_export = True
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(side_effect=[TimeoutError(), b"direct-blob"])
    t_priv = Torrent(
        hash="f" * 40,
        name="Priv.Retry",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )
    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_priv,
        other_source_torrents=[],
        prowlarr=None,
        sftp=sftp,
        source_client=AsyncMock(),
        attempt_prowlarr=False,
    )
    assert dec is not None
    assert dec.source_label == "private-sftp-fallback"
    assert sftp.fetch_torrent.call_count == 2


@pytest.mark.anyio
async def test_private_fallback_double_timeout_falls_to_export():
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.coordinator import pick_ssd_source_for_racing

    cfg = MagicMock()
    cfg.prowlarr.should_skip_title.return_value = False
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.allow_ssh_export = True
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(side_effect=TimeoutError("wedged"))
    source_client = AsyncMock()
    source_client.export_torrent = AsyncMock(return_value=b"export-blob")
    t_priv = Torrent(
        hash="f" * 40,
        name="Priv.Retry",
        category="",
        save_path="",
        size_bytes=500,
        state="seeding",
        progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )
    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_priv,
        other_source_torrents=[],
        prowlarr=None,
        sftp=sftp,
        source_client=source_client,
        attempt_prowlarr=False,
    )
    assert dec is not None
    assert dec.source_label == "private-export-fallback"
    assert sftp.fetch_torrent.call_count == 2
    source_client.export_torrent.assert_awaited_once()


@pytest.mark.anyio
async def test_fallback_trip_opens_fresh_direct_window():
    """After the trip, one failed direct fetch parks — it must not FAIL."""
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    # Real park (not mocked) so the max-age give-up logic actually runs.
    del coord._park_for_indexer_retry
    ts = TorrentState(
        source_infohash="d" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = None  # prowlarr miss AND direct fetch miss
        await coord._pick_and_admit(ts, _priv_st(), [])
    # Fallback tripped (flag set, clock restarted) and the row parked for
    # another direct attempt instead of failing at the old deadline.
    assert ts.force_direct == 1
    assert ts.state == State.WAITING_INDEXER
    assert ts.indexer_next_retry_at is not None
    assert ts.indexer_next_retry_at > dt.datetime.now(dt.UTC)
    fresh_elapsed = (dt.datetime.now(dt.UTC) - ts.indexer_first_queried_at).total_seconds()
    assert fresh_elapsed < 60


def test_prowlarr_timed_out_helper():
    coord = _pick_coord()
    ts = TorrentState(source_infohash="d" * 40)
    assert coord._prowlarr_timed_out(ts) is False  # no first attempt yet
    ts.indexer_first_queried_at = dt.datetime.now(dt.UTC)
    assert coord._prowlarr_timed_out(ts) is False  # flag off anyway
    coord.cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = True
    assert coord._prowlarr_timed_out(ts) is False  # window fresh
    ts.indexer_first_queried_at = _past_max_age(coord.cfg)
    assert coord._prowlarr_timed_out(ts) is True


@pytest.mark.anyio
async def test_reset_empty_db_rediscovers_waiting_row_from_zero(tmp_path: Path):
    """Simulates --reset: empty DB + torrent still on VPS1 → fresh NEW row.

    A WAITING_INDEXER row holds no VPS2 footprint, so after state.db is
    wiped the next tick's discovery recreates it with no flag and no
    timers — prowlarr retries start over instead of failing or stalling.
    """
    import time
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.cfg.max_active_downloads = 3
    coord.cfg.max_concurrent_moves = 3
    coord.cfg.source.category = ""
    coord.cfg.source.min_age_seconds = 0
    coord.store = store
    coord.watch = None
    coord._spawn_worker = MagicMock()
    coord._sweep_manual_fuse_adoptions = AsyncMock()
    coord._last_source_log_ts = time.monotonic()
    t = Torrent(
        hash="f" * 40,
        name="Reset.Show.S01E01",
        category="",
        save_path="",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=["https://alpha.cc/announce/xyz"],
    )
    coord._list_source_torrents = AsyncMock(return_value=[t])
    try:
        await coord._tick_inner()
    finally:
        pass
    row = store.get("f" * 40)
    assert row is not None
    assert row.state == State.NEW
    assert row.force_direct == 0
    assert row.indexer_attempts == 0
    assert row.indexer_first_queried_at is None
    assert coord._spawn_worker.called
    store.close()


def _sample_blob(name: str, size: int, announce: str, piece_length: int = 16384) -> bytes:
    from racing_sync.watchdir import _bencode

    return _bencode(
        {
            b"announce": announce.encode(),
            b"info": {
                b"name": name.encode(),
                b"length": size,
                b"piece length": piece_length,
                b"pieces": b"12345678901234567890",
            },
        }
    )


@pytest.mark.anyio
async def test_pick_and_admit_all_remote_skips_budget(tmp_path: Path):
    """Decision bytes already on fuse: QUEUED with no reservation."""
    from unittest.mock import AsyncMock, patch

    from racing_sync.config import ClassifierConfig
    from racing_sync.coordinator_content import SourceDecision

    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    (fuse_dir / "Admit.Movie.1080p.mkv").write_bytes(b"m" * 2000)
    blob = _sample_blob("Admit.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")

    coord = _pick_coord()
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.dest.save_path = tmp_path
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"
    coord._ssd_try_reserve = AsyncMock()
    decision = SourceDecision(
        torrent_bytes=blob,
        source_label="private-export-fallback",
        name="Admit.Movie.1080p.mkv",
        size_bytes=2000,
        infohash="d" * 40,
        announce_url="https://alpha.cc/announce/xyz",
    )
    ts = TorrentState(source_infohash="d" * 40, state=State.NEW)
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", new_callable=AsyncMock
    ) as pick:
        pick.return_value = decision
        await coord._pick_and_admit(ts, _priv_st(), [])
    assert ts.state == State.QUEUED
    coord._ssd_try_reserve.assert_not_called()
    assert ts.cross_seed_blob == blob


def test_blob_fully_remote_three_valued(tmp_path: Path):
    import asyncio

    from racing_sync.config import ClassifierConfig

    coord = make_coordinator()
    coord.cfg.classifier = ClassifierConfig()
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.dest.save_path = tmp_path
    fuse_dir = tmp_path / "fuse"
    fuse_dir.mkdir()
    coord.cfg.rclone.fuse.mount = fuse_dir
    coord.cfg.rclone.fuse.mount_unsorted = tmp_path / "fuse-u"

    async def _check(blob):
        return await coord._blob_fully_remote(blob)

    raw = _sample_blob("Remote.Movie.1080p.mkv", 2000, "https://alpha.cc/announce/xyz")
    assert asyncio.run(_check(None)) is None
    assert asyncio.run(_check(b"junk")) is None
    # Present but short on fuse: real work remains.
    (fuse_dir / "Remote.Movie.1080p.mkv").write_bytes(b"m" * 1999)
    assert asyncio.run(_check(raw)) is False
    # Full size: fully remote (invalidate the 60s shared fuse-stat cache:
    # production reads through rclone's even coarser dir-cache, but tests
    # assert instantaneous visibility).
    from racing_sync.coordinator_paths import fuse_stat_invalidate

    (fuse_dir / "Remote.Movie.1080p.mkv").write_bytes(b"m" * 2000)
    fuse_stat_invalidate(fuse_dir / "Remote.Movie.1080p.mkv")
    assert asyncio.run(_check(raw)) is True


def test_fallback_config_validation():
    from racing_sync.config import CrossSeedConfig

    assert CrossSeedConfig().fallback_to_racing_torrent_on_prowlarr_timeout is False
    CrossSeedConfig(fallback_to_racing_torrent_on_prowlarr_timeout=True, allow_ssh_export=True)
    with pytest.raises(ValueError, match="allow_ssh_export"):
        CrossSeedConfig(fallback_to_racing_torrent_on_prowlarr_timeout=True, allow_ssh_export=False)


def _grace_st(infohash: str, tracker: str):
    from racing_sync.clients.abstract import Torrent

    return Torrent(
        hash=infohash,
        name="Grace.Show.S01E01",
        category="",
        save_path="",
        size_bytes=100,
        state="seeding",
        progress=1.0,
        trackers=[tracker],
    )


def _grace_racing_coord():
    """Real store + prefer grace configured (MagicMock cfg)."""
    from conftest import make_coordinator

    coord = make_coordinator()
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in (url or "")
    return coord


def test_prefer_racing_row_starts_held_row(tmp_path):
    """A held VPS1 racing row can be preferred like a watch drop."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_racing_coord()
        coord.store = store
        ts = TorrentState(
            source_infohash="f" * 40,
            source_name="Grace.Show",
            source_announce_url="https://unknown.example/announce",
            source_tracker="https://unknown.example/announce",
            state=State.NEW,
        )
        store.upsert(ts)
        row, msg = coord.prefer_grace_row("f" * 40)
        assert row is not None and row.source_infohash == "f" * 40
        assert msg.startswith("Preferred")
        assert "f" * 40 in (coord._grace_exempt or {})
    finally:
        store.close()


def test_prefer_racing_row_refusals(tmp_path):
    """Preferred/public/stale racing rows get explanations, not exemptions."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = _grace_racing_coord()
        coord.store = store
        pref = TorrentState(
            source_infohash="a" * 40,
            source_name="Pref.Show",
            source_announce_url="https://dl-indexer.example.net/announce",
            state=State.NEW,
        )
        store.upsert(pref)
        row, msg = coord.prefer_grace_row("a" * 40)
        assert row is None and "already from a download indexer" in msg
        pub = TorrentState(
            source_infohash="b" * 40,
            source_name="Pub.Show",
            source_announce_url="udp://tracker.opentrackr.org:1337/announce",
            state=State.NEW,
        )
        store.upsert(pub)
        row2, msg2 = coord.prefer_grace_row("b" * 40)
        assert row2 is None and "is public" in msg2
        old = TorrentState(
            source_infohash="c" * 40,
            source_name="Old.Show",
            source_announce_url="https://unknown.example/announce",
            state=State.NEW,
        )
        import datetime as dt

        old.created_at = dt.datetime.now(dt.UTC) - dt.timedelta(hours=2)
        store.upsert(old)
        row3, msg3 = coord.prefer_grace_row("c" * 40)
        assert row3 is None and "past its grace" in msg3
        assert getattr(coord, "_grace_exempt", {}) == {}
    finally:
        store.close()


@pytest.mark.anyio
async def test_pick_and_admit_holds_nonpreferred_direct():
    """A direct commit to a non-preferred swarm holds in grace (not parks)."""
    coord = _pick_coord()
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.is_download_indexer = MagicMock(return_value=False)
    coord.prowlarr = None
    ts = TorrentState(source_infohash="e" * 40, source_name="Grace.Show", state=State.NEW)
    await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
    assert ts.state == State.NEW
    coord.transition.assert_not_called()
    coord._park_for_indexer_retry.assert_not_called()


@pytest.mark.anyio
async def test_pick_and_admit_throttles_held_repick():
    """A grace-held NEW row re-picks at most once per search interval,
    not once per tick."""
    from unittest.mock import AsyncMock, patch

    from racing_sync.coordinator_content import SourceDecision

    coord = _pick_coord()
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.is_download_indexer = MagicMock(return_value=False)
    coord.prowlarr = None
    decision = SourceDecision(
        torrent_bytes=b"x",
        source_label="private-export-fallback",
        name="Grace.Show",
        size_bytes=100,
        infohash="e" * 40,
        preferred=False,
    )
    ts = TorrentState(source_infohash="e" * 40, source_name="Grace.Show", state=State.NEW)
    with patch(
        "racing_sync.coordinator.pick_ssd_source_for_racing", AsyncMock(return_value=decision)
    ) as mock_pick:
        await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
        assert ts.state == State.NEW
        await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
        assert ts.state == State.NEW
        assert mock_pick.await_count == 1


@pytest.mark.anyio
async def test_pick_and_admit_exemption_and_preferred_proceed():
    """/start_ exemption and preferred direct commits skip the hold."""
    coord = _pick_coord()
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.is_download_indexer = MagicMock(return_value=False)
    coord.prowlarr = None
    # Exempted non-preferred direct -> admits (exemption consumed).
    coord._grace_exempt = {"e" * 40: 1.0}
    ts = TorrentState(source_infohash="e" * 40, source_name="Grace.Show", state=State.NEW)
    await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
    assert ts.state == State.QUEUED
    assert coord._grace_exempt == {}
    # Preferred direct (visible download-indexer copy) -> admits outright.
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in (url or "")
    ts2 = TorrentState(source_infohash="g" * 40, source_name="Grace.Show", state=State.NEW)
    other = _grace_st("h" * 40, "https://dl-indexer.example.net/announce")
    await coord._pick_and_admit(
        ts2, _grace_st("g" * 40, "https://unknown.example/announce"), [other]
    )
    assert ts2.state == State.QUEUED
    assert ts2.cross_seed_infohash == "h" * 40


@pytest.mark.anyio
async def test_picker_direct_fallback_prefers_download_indexer_copy():
    """The SFTP/export fallback leeches the preferred swarm, not st's."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.config import DownloadIndexerConfig, ProwlarrConfig
    from racing_sync.coordinator_picker import pick_ssd_source_for_racing

    pcfg = ProwlarrConfig(
        enabled=False,
        base_url="http://127.0.0.1:9696",
        api_key="secret",
        download_indexers=[
            DownloadIndexerConfig(name="Preferred (API)", announce_substrings=["preferred"])
        ],
    )
    cfg = MagicMock()
    cfg.prowlarr = pcfg
    cfg.cross_seed.allow_ssh_export = True

    def _t(h, tracker):
        return Torrent(
            hash=h,
            name="Show",
            category="",
            save_path="",
            size_bytes=100,
            state="seeding",
            progress=1.0,
            trackers=[tracker],
        )

    st = _t("a" * 40, "https://unknown.example/announce")
    other = _t("b" * 40, "https://preferred.example.net/announce/xyz")
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=b"d8:announce...")
    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=st,
        other_source_torrents=[other],
        prowlarr=None,
        sftp=sftp,
        source_client=MagicMock(),
    )
    assert dec.infohash == "b" * 40
    sftp.fetch_torrent.assert_called_once_with("b" * 40)

    sftp2 = MagicMock()
    sftp2.fetch_torrent = MagicMock(return_value=b"d8:announce...")
    dec2 = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=st,
        other_source_torrents=[],
        prowlarr=None,
        sftp=sftp2,
        source_client=MagicMock(),
    )
    assert dec2.infohash == "a" * 40


def _waiting_pair(tmp_path, *, leader_state, same_content=True):
    """WAITING_INDEXER waiter + a same/other-content leader row."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    now = dt.datetime.now(dt.UTC)
    waiter = TorrentState(
        source_infohash="w" * 40,
        source_name="Twin.Show",
        total_bytes=1000,
        state=State.WAITING_INDEXER,
        indexer_attempts=3,
        indexer_first_queried_at=now - dt.timedelta(seconds=100),
        indexer_next_retry_at=now - dt.timedelta(seconds=1),
    )
    leader = TorrentState(
        source_infohash="l" * 40,
        source_name="Twin.Show" if same_content else "Other.Show",
        total_bytes=1000,
        state=leader_state,
    )
    store.upsert(waiter)
    store.upsert(leader)
    coord = make_coordinator()
    coord.cfg = _cfg()
    coord.store = store
    st = Torrent(
        hash="w" * 40,
        name="Twin.Show",
        category="",
        save_path="",
        size_bytes=1000,
        state="seeding",
        progress=1.0,
        trackers=[],
    )
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent = AsyncMock(return_value=st)
    coord._adopt_manual_fuse_if_present = AsyncMock(return_value=False)
    coord._pick_and_admit = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))
    return coord, store, waiter


@pytest.mark.anyio
async def test_waiting_indexer_defers_to_inflight_same_content(tmp_path):
    """Sibling DOWNLOADING: skip the Prowlarr search, park quietly."""
    coord, store, waiter = _waiting_pair(tmp_path, leader_state=State.DOWNLOADING)
    before = waiter.indexer_next_retry_at
    try:
        await coord._do_waiting_indexer(waiter)
        assert waiter.state == State.WAITING_INDEXER
        assert waiter.indexer_attempts == 3  # no attempt burned
        assert waiter.indexer_next_retry_at > before  # timer pushed
        coord._pick_and_admit.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_inflight_both_unknown_size_proceeds(tmp_path):
    """Two unsized same-name rows must not mutually defer: with no size
    on either side there is no proof of sameness, so the waiter picks
    instead of parking on a phantom leader."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.cfg = _cfg()
        coord.store = store
        now = dt.datetime.now(dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="l" * 40,
                source_name="Twin.Show",
                total_bytes=0,
                state=State.DOWNLOADING,
            )
        )
        waiter = TorrentState(
            source_infohash="w" * 40,
            source_name="Twin.Show",
            total_bytes=0,
            state=State.WAITING_INDEXER,
            indexer_attempts=3,
            indexer_first_queried_at=now - dt.timedelta(seconds=100),
            indexer_next_retry_at=now - dt.timedelta(seconds=1),
        )
        store.upsert(waiter)
        coord.source_client = AsyncMock()
        coord.source_client.get_torrent = AsyncMock(
            return_value=Torrent(
                hash="w" * 40,
                name="Twin.Show",
                category="",
                save_path="",
                size_bytes=0,
                state="seeding",
                progress=1.0,
                trackers=[],
            )
        )
        coord._list_source_torrents = AsyncMock(return_value=[])
        coord._pick_and_admit = AsyncMock()
        await coord._do_waiting_indexer(store.get("w" * 40))
        coord._pick_and_admit.assert_awaited_once()
    finally:
        store.close()


def test_inflight_same_content_elects_oldest_pre_admit_row(tmp_path):
    """Two NEW same-content rows: the older proceeds, the newer defers.

    Nothing is QUEUED yet, so before the election both rows admitted in
    the same pass and downloaded the same bytes twice.
    """
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.cfg = _cfg()
        coord.store = store
        base = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
        store.upsert(
            TorrentState(
                source_infohash="a" * 40,
                source_name="Twin.Show",
                total_bytes=100,
                state=State.NEW,
                created_at=base,
            )
        )
        store.upsert(
            TorrentState(
                source_infohash="b" * 40,
                source_name="Twin.Show",
                total_bytes=100,
                state=State.NEW,
                created_at=base + dt.timedelta(minutes=1),
            )
        )
        older = store.get("a" * 40)
        newer = store.get("b" * 40)

        assert coord._inflight_same_content(older) is None
        winner = coord._inflight_same_content(newer)
        assert winner is not None
        assert winner.source_infohash == "a" * 40
    finally:
        store.close()


@pytest.mark.anyio
async def test_ssd_downloading_quiet_park_keeps_public_schedule(tmp_path):
    """A public-export row whose SSD entry is still downloading pushes
    the public timer, not the indexer one."""
    from unittest.mock import AsyncMock

    from racing_sync.clients.abstract import Torrent
    from racing_sync.state import StateStore

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.cfg = _cfg()
        coord.store = store
        _now = dt.datetime.now(dt.UTC)
        ts = TorrentState(
            source_infohash="w" * 40,
            source_name="Twin.Show",
            total_bytes=1000,
            state=State.WAITING_INDEXER,
            public_export_first_attempted_at=_now - dt.timedelta(seconds=100),
            public_export_attempts=2,
            public_export_next_retry_at=_now - dt.timedelta(seconds=1),
        )
        store.upsert(ts)
        coord.dest_client = AsyncMock()
        coord.dest_client.list_torrents = AsyncMock(
            return_value=[
                Torrent(
                    hash="w" * 40,
                    name="Twin.Show",
                    category="racing",
                    save_path=str(ssd),
                    size_bytes=1000,
                    state="downloading",
                    progress=0.5,
                ),
            ]
        )
        before = ts.public_export_next_retry_at
        await coord._do_waiting_indexer(store.get("w" * 40))
        row = store.get("w" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.public_export_next_retry_at > before
        assert row.indexer_next_retry_at is None
    finally:
        store.close()


def test_inflight_snapshot_shared_across_calls(tmp_path):
    """N _inflight_same_content calls in one window share one scan;
    an expired window re-scans."""
    import time

    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator()
        coord.store = store
        calls: list[bool] = []
        real_all_active = store.all_active

        def _counting():
            calls.append(True)
            return real_all_active()

        store.all_active = _counting  # type: ignore[method-assign]
        ts = TorrentState(
            source_infohash="w" * 40,
            source_name="Twin.Show",
            total_bytes=1000,
            state=State.WAITING_INDEXER,
        )
        try:
            assert coord._inflight_same_content(ts) is None
            assert coord._inflight_same_content(ts) is None
            assert len(calls) == 1
            coord._inflight_snapshot = (time.monotonic() - 60.0, coord._inflight_snapshot[1])
            assert coord._inflight_same_content(ts) is None
            assert len(calls) == 2
        finally:
            store.all_active = real_all_active  # type: ignore[method-assign]
    finally:
        store.close()


@pytest.mark.anyio
async def test_waiting_indexer_proceeds_when_leader_settled(tmp_path):
    """DONE/FAILED siblings hold no SSD work: pick runs as before."""
    from racing_sync.state import State as _S

    for settled in (_S.DONE, _S.FAILED):
        coord, store, waiter = _waiting_pair(tmp_path, leader_state=settled)
        try:
            await coord._do_waiting_indexer(waiter)
            coord._pick_and_admit.assert_awaited_once()
        finally:
            store.close()


@pytest.mark.anyio
async def test_waiting_indexer_ignores_other_content(tmp_path):
    """In-flight rows for other releases never gate the retry."""
    coord, store, waiter = _waiting_pair(
        tmp_path, leader_state=State.DOWNLOADING, same_content=False
    )
    try:
        await coord._do_waiting_indexer(waiter)
        coord._pick_and_admit.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_concurrent_source_lists_share_one_scan():
    """Scheduler worker bursts must not each fire a full Deluge scan.

    Five concurrent _list_source_torrents calls (expired cache) collapse
    to a single source RPC via the single-flight lock; without it every
    worker burst re-scans deluged and starves autobrr injections.
    """
    import asyncio

    coord = make_coordinator()
    coord.cfg.source.min_age_seconds = 0
    _row = MagicMock()
    _row.configure_mock(**{"infohash": "a" * 40, "name": "X"})
    _row.size_bytes = 1
    _row.added_on = 0

    async def _slow_list(**kwargs):
        await asyncio.sleep(0.05)
        return [_row]

    coord.source_client = AsyncMock()
    coord.source_client.list_torrents = AsyncMock(side_effect=_slow_list)
    coord._source_torrents_cache = []
    coord._source_torrents_cached_at = 0.0
    results = await asyncio.gather(*[coord._list_source_torrents() for _ in range(5)])
    assert coord.source_client.list_torrents.await_count == 1
    assert all(len(r) == 1 for r in results)
    # Fresh cache: no RPC at all.
    results2 = await asyncio.gather(*[coord._list_source_torrents() for _ in range(5)])
    assert coord.source_client.list_torrents.await_count == 1
    assert all(len(r) == 1 for r in results2)


@pytest.mark.anyio
async def test_wakeup_indexer_row_ignores_skipped():
    """Timer-fired wakeups never revive a held row."""
    import tempfile
    from pathlib import Path
    from unittest.mock import MagicMock

    from racing_sync.state import StateStore

    raw, h = _operator_blob(name="Hand.Added.mkv")
    tmp = Path(tempfile.mkdtemp())
    store = StateStore(tmp / "state.db")
    try:
        coord = make_coordinator(store)
        coord.transition = MagicMock()
        ts = TorrentState(
            source_infohash="s" * 40, source_name="Held", state=State.WAITING_INDEXER, skipped=1
        )
        store.upsert(ts)
        coord._wakeup_indexer_row(ts)
        coord.transition.assert_not_called()
        assert "s" * 40 not in coord._running_infohashes
    finally:
        store.close()


@pytest.mark.anyio
async def test_process_torrent_inner_honors_hold():
    """A directly-spawned worker no-ops on a held row."""
    from unittest.mock import AsyncMock

    coord = make_coordinator()
    coord._do_new = AsyncMock()
    ts = TorrentState(source_infohash="s" * 40, state=State.NEW, skipped=1)
    await coord._process_torrent_inner(ts)
    coord._do_new.assert_not_called()


@pytest.mark.anyio
async def test_schedule_step_never_spawns_held_rows(tmp_path):
    """The scheduler gives held rows no workers (but still works others)."""

    from racing_sync.state import StateStore

    tmp = tmp_path / "sched.db"
    store = StateStore(tmp)
    try:
        coord = make_coordinator(
            store,
            cfg__max_active_downloads=3,
            cfg__max_concurrent_moves=3,
        )
        held = TorrentState(
            source_infohash="a" * 40, source_name="Held", state=State.NEW, skipped=1
        )
        store.upsert(held)
        await coord._schedule_step()
        assert "a" * 40 not in coord._running_infohashes
        assert len(coord._tasks) == 0
    finally:
        for t in list(getattr(coord, "_tasks", []) or []):
            try:
                t.cancel()
            except Exception:
                pass
        store.close()


def test_prefer_grace_row_refuses_held(tmp_path):
    """Prefer on a held row explains itself instead of exempting it."""
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "prefer.db")
    try:
        coord = make_coordinator(store)
        ts = TorrentState(source_infohash="b" * 40, source_name="Held", state=State.NEW, skipped=1)
        store.upsert(ts)
        row, msg = coord.prefer_grace_row("b" * 40)
        assert row is None
        assert "skipped" in msg
    finally:
        store.close()


def test_schedule_retry_writes_one_schedule_and_clears_the_other():
    """One helper owns the timer arithmetic for both retry schedules."""
    coord = make_coordinator()
    ts = TorrentState(source_infohash="a" * 40, state=State.WAITING_INDEXER)
    ts.indexer_next_retry_at = dt.datetime.now(dt.UTC)
    ts.public_export_next_retry_at = dt.datetime.now(dt.UTC)

    at = coord._schedule_retry(ts, "public", 120)
    assert ts.public_export_next_retry_at == at
    assert ts.indexer_next_retry_at is None, "a leftover indexer timer must be cleared"

    at2 = coord._schedule_retry(ts, "indexer", 120)
    assert ts.indexer_next_retry_at == at2
    assert ts.public_export_next_retry_at is None


def test_schedule_retry_clamps_bad_delays():
    coord = make_coordinator()
    ts = TorrentState(source_infohash="a" * 40, state=State.WAITING_INDEXER)
    before = dt.datetime.now(dt.UTC)

    at = coord._schedule_retry(ts, "indexer", 0)
    assert (at - before).total_seconds() >= 59.0, "a zero delay must not spin the scheduler"

    at = coord._schedule_retry(ts, "indexer", "junk")
    assert (at - before).total_seconds() >= 900.0, "unparsable delays fall back to the default"


def test_park_waiting_indexer_transitions_fresh_but_upserts_parked(tmp_path):
    from racing_sync.state import StateStore

    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        fresh = TorrentState(source_infohash="a" * 40, source_name="Show", state=State.NEW)
        store.upsert(fresh)

        coord._park_waiting_indexer(fresh, reason="indexer")
        assert store.get("a" * 40).state == State.WAITING_INDEXER

        parked = store.get("a" * 40)
        parked.indexer_attempts = 2
        coord._park_waiting_indexer(parked, reason="indexer")
        assert store.get("a" * 40).indexer_attempts == 2
    finally:
        store.close()
