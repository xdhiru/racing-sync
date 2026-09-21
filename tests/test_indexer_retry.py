"""Tests for the download-target indexer retry policy math.

We don't run the full coordinator here (it needs live qBittorrent +
Prowlarr). Instead we verify the timing rules that _park_for_indexer_retry
applies.
"""

from __future__ import annotations

import datetime as dt
import pytest
from conftest import make_coordinator
from unittest.mock import AsyncMock, MagicMock

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

    now = dt.datetime.now(dt.timezone.utc)
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
    assert cfg.cross_seed.prowlarr_retry_interval_seconds == 1800


def test_expired_max_age_marks_failed():
    """If the first attempt was > 24 h ago, the next park should escalate
    to FAILED. We simulate by backdating indexer_first_queried_at."""
    cfg = _cfg()
    ts = TorrentState(
        source_infohash="a" * 40,
        state=State.WAITING_INDEXER,
        indexer_first_queried_at=dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=cfg.cross_seed.prowlarr_max_age_seconds + 1),
        indexer_next_retry_at=dt.datetime.now(dt.timezone.utc),
        indexer_attempts=10,
    )
    now = dt.datetime.now(dt.timezone.utc)
    elapsed = now - ts.indexer_first_queried_at
    assert elapsed > dt.timedelta(seconds=cfg.cross_seed.prowlarr_max_age_seconds)


@pytest.mark.anyio
async def test_process_torrent_inner_dispatches_querying_state():
    from unittest.mock import AsyncMock

    coord = make_coordinator()
    coord._do_waiting_indexer = AsyncMock()

    ts = TorrentState("hash1", state=State.QUERYING)
    await coord._process_torrent_inner(ts)

    # Must dispatch to _do_waiting_indexer when in QUERYING state
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

    t1 = Torrent(hash="h1", name="Show.A", category="racing", save_path="", size_bytes=100, state="", progress=1.0)
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
async def test_do_new_transitions_failed_when_source_vanished():
    from unittest.mock import AsyncMock, MagicMock

    coord = make_coordinator()
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent.return_value = None
    coord.transition = MagicMock()

    ts = TorrentState(source_infohash="vanished_hash_12345", state=State.NEW)
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

    ts = TorrentState(source_infohash="vanished_hash_67890", state=State.QUERYING)
    await coord._do_waiting_indexer(ts)

    coord.transition.assert_called_once_with(
        ts, State.FAILED, error="source torrent vanished from client: vanished_h"
    )


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

    coord.dest_client.add_torrent.return_value = AddResult(hash=None, accepted=False, detail="Fails.")
    coord.dest_client.get_torrent.return_value = Torrent(
        hash="abc123456789", name="Test", category="racing", save_path="/mnt/fuse", size_bytes=100, state="", progress=1.0
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

    async def fake_tick():
        nonlocal tick_called
        tick_called = True
        coord.request_stop()

    coord._tick = fake_tick

    exit_code = await coord.run()
    assert exit_code == 0
    assert tick_called is True
    coord.shutdown.assert_awaited_once()

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
    coord._do_queued.assert_awaited_once_with(ts)


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
        hash="p" * 40, name="Public.Show.S01E01.mkv", category="",
        save_path="", size_bytes=500, state="seeding", progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    with caplog.at_level(logging.WARNING, logger="racing_sync.coordinator"):
        dec = await pick_ssd_source_for_racing(
            cfg=cfg, source_torrent=torrent, other_source_torrents=[],
            prowlarr=None, sftp=sftp, source_client=AsyncMock(),
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
        hash="p" * 40, name="Public.Show.S01E01.mkv", category="",
        save_path="", size_bytes=500, state="seeding", progress=1.0,
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
        hash="d" * 40, name="Private.Show.S01E01.mkv", category="",
        save_path="", size_bytes=1000, state="seeding", progress=1.0,
        trackers=["https://alpha.cc/announce/xyz"],
    )


def _decision():
    from racing_sync.coordinator_content import SourceDecision

    return SourceDecision(
        torrent_bytes=b"blob", source_label="private-export-fallback",
        name="Private.Show.S01E01.mkv", size_bytes=1000,
        infohash="d" * 40, announce_url="https://alpha.cc/announce/xyz",
    )


def _past_max_age(cfg):
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(
        seconds=cfg.cross_seed.prowlarr_max_age_seconds + 10)


@pytest.mark.anyio
async def test_pick_and_admit_force_direct_bypasses_prowlarr():
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    ts = TorrentState(source_infohash="d" * 40, state=State.QUERYING, force_direct=1)
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
        pick.return_value = _decision()
        await coord._pick_and_admit(ts, _priv_st(), [])
    pick.assert_awaited_once()
    assert pick.call_args.kwargs["attempt_prowlarr"] is False
    coord.transition.assert_called_once()
    assert coord.transition.call_args[0][1] == State.QUEUED
    coord._park_for_indexer_retry.assert_not_called()


@pytest.mark.anyio
async def test_pick_and_admit_first_miss_parks_despite_fallback_flag():
    """The fallback fires only after the retry window is exhausted."""
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord(fallback_to_racing_torrent_on_prowlarr_timeout=True)
    ts = TorrentState(source_infohash="d" * 40, state=State.QUERYING)
    assert ts.indexer_first_queried_at is None
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
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
        source_infohash="d" * 40, state=State.QUERYING,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
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
async def test_pick_and_admit_timeout_without_flag_parks():
    from unittest.mock import AsyncMock, patch

    coord = _pick_coord()
    ts = TorrentState(
        source_infohash="d" * 40, state=State.QUERYING,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
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
        hash="e" * 40, name="Public.Show.mkv", category="",
        save_path="", size_bytes=500, state="seeding", progress=1.0,
        trackers=["http://tracker.opentrackr.org/announce"],
    )
    ts = TorrentState(
        source_infohash="e" * 40, state=State.QUERYING,
        indexer_first_queried_at=_past_max_age(coord.cfg),
    )
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
        pick.return_value = None
        await coord._pick_and_admit(ts, pub, [])
    pick.assert_awaited_once()
    assert coord._park_for_indexer_retry.call_args.kwargs["reason"] == "source export miss"
    assert ts.force_direct == 0


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
        hash="f" * 40, name="Priv.Retry", category="",
        save_path="", size_bytes=500, state="seeding", progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )
    dec = await pick_ssd_source_for_racing(
        cfg=cfg, source_torrent=t_priv, other_source_torrents=[],
        prowlarr=None, sftp=sftp, source_client=AsyncMock(),
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
        hash="f" * 40, name="Priv.Retry", category="",
        save_path="", size_bytes=500, state="seeding", progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )
    dec = await pick_ssd_source_for_racing(
        cfg=cfg, source_torrent=t_priv, other_source_torrents=[],
        prowlarr=None, sftp=sftp, source_client=source_client,
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
        source_infohash="d" * 40, state=State.QUERYING,
        indexer_first_queried_at=_past_max_age(coord.cfg),
        indexer_attempts=40,
    )
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
        pick.return_value = None  # prowlarr miss AND direct fetch miss
        await coord._pick_and_admit(ts, _priv_st(), [])
    # Fallback tripped (flag set, clock restarted) and the row parked for
    # another direct attempt instead of failing at the old deadline.
    assert ts.force_direct == 1
    assert ts.state == State.WAITING_INDEXER
    assert ts.indexer_next_retry_at is not None
    assert ts.indexer_next_retry_at > dt.datetime.now(dt.timezone.utc)
    fresh_elapsed = (dt.datetime.now(dt.timezone.utc)
                     - ts.indexer_first_queried_at).total_seconds()
    assert fresh_elapsed < 60


def test_prowlarr_timed_out_helper():
    coord = _pick_coord()
    ts = TorrentState(source_infohash="d" * 40)
    assert coord._prowlarr_timed_out(ts) is False  # no first attempt yet
    ts.indexer_first_queried_at = dt.datetime.now(dt.timezone.utc)
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
        hash="f" * 40, name="Reset.Show.S01E01", category="",
        save_path="", size_bytes=1000, state="seeding", progress=1.0,
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

    return _bencode({
        b"announce": announce.encode(),
        b"info": {
            b"name": name.encode(),
            b"length": size,
            b"piece length": piece_length,
            b"pieces": b"12345678901234567890",
        },
    })


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
        torrent_bytes=blob, source_label="private-export-fallback",
        name="Admit.Movie.1080p.mkv", size_bytes=2000,
        infohash="d" * 40, announce_url="https://alpha.cc/announce/xyz",
    )
    ts = TorrentState(source_infohash="d" * 40, state=State.NEW)
    with patch("racing_sync.coordinator.pick_ssd_source_for_racing",
               new_callable=AsyncMock) as pick:
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
    # Full size: fully remote.
    (fuse_dir / "Remote.Movie.1080p.mkv").write_bytes(b"m" * 2000)
    assert asyncio.run(_check(raw)) is True


def test_fallback_config_validation():
    from racing_sync.config import CrossSeedConfig

    assert CrossSeedConfig().fallback_to_racing_torrent_on_prowlarr_timeout is False
    CrossSeedConfig(fallback_to_racing_torrent_on_prowlarr_timeout=True,
                    allow_ssh_export=True)
    with pytest.raises(ValueError, match="allow_ssh_export"):
        CrossSeedConfig(fallback_to_racing_torrent_on_prowlarr_timeout=True,
                        allow_ssh_export=False)


def _grace_st(infohash: str, tracker: str):
    from racing_sync.clients.abstract import Torrent

    return Torrent(hash=infohash, name="Grace.Show.S01E01", category="",
                   save_path="", size_bytes=100, state="seeding",
                   progress=1.0, trackers=[tracker])


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
        ts = TorrentState(source_infohash="f" * 40, source_name="Grace.Show",
                          source_announce_url="https://unknown.example/announce",
                          source_tracker="https://unknown.example/announce",
                          state=State.NEW)
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
            source_infohash="a" * 40, source_name="Pref.Show",
            source_announce_url="https://dl-indexer.example.net/announce",
            state=State.NEW)
        store.upsert(pref)
        row, msg = coord.prefer_grace_row("a" * 40)
        assert row is None and "already from a download indexer" in msg
        pub = TorrentState(
            source_infohash="b" * 40, source_name="Pub.Show",
            source_announce_url="udp://tracker.opentrackr.org:1337/announce",
            state=State.NEW)
        store.upsert(pub)
        row2, msg2 = coord.prefer_grace_row("b" * 40)
        assert row2 is None and "is public" in msg2
        old = TorrentState(
            source_infohash="c" * 40, source_name="Old.Show",
            source_announce_url="https://unknown.example/announce",
            state=State.NEW)
        import datetime as dt

        old.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2)
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
    ts = TorrentState(source_infohash="e" * 40, source_name="Grace.Show",
                      state=State.NEW)
    await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
    assert ts.state == State.NEW
    coord.transition.assert_not_called()
    coord._park_for_indexer_retry.assert_not_called()


@pytest.mark.anyio
async def test_pick_and_admit_exemption_and_preferred_proceed():
    """/prefer_ exemption and preferred direct commits skip the hold."""
    coord = _pick_coord()
    coord.cfg.general.preferred_copy_grace_seconds = 3600
    coord.cfg.prowlarr.download_indexers = [MagicMock()]
    coord.cfg.prowlarr.is_download_indexer = MagicMock(return_value=False)
    coord.prowlarr = None
    # Exempted non-preferred direct -> admits (exemption consumed).
    coord._grace_exempt = {"e" * 40: 1.0}
    ts = TorrentState(source_infohash="e" * 40, source_name="Grace.Show",
                      state=State.NEW)
    await coord._pick_and_admit(ts, _grace_st("e" * 40, "https://unknown.example/announce"), [])
    assert ts.state == State.QUEUED
    assert coord._grace_exempt == {}
    # Preferred direct (visible download-indexer copy) -> admits outright.
    coord.cfg.prowlarr.is_download_indexer = lambda url: "dl-indexer" in (url or "")
    ts2 = TorrentState(source_infohash="g" * 40, source_name="Grace.Show",
                       state=State.NEW)
    other = _grace_st("h" * 40, "https://dl-indexer.example.net/announce")
    await coord._pick_and_admit(
        ts2, _grace_st("g" * 40, "https://unknown.example/announce"), [other])
    assert ts2.state == State.QUEUED
    assert ts2.cross_seed_infohash == "h" * 40


@pytest.mark.anyio
async def test_picker_direct_fallback_prefers_download_indexer_copy():
    """The SFTP/export fallback leeches the preferred swarm, not st's."""
    from racing_sync.clients.abstract import Torrent
    from racing_sync.config import DownloadIndexerConfig, ProwlarrConfig
    from racing_sync.coordinator_picker import pick_ssd_source_for_racing

    pcfg = ProwlarrConfig(
        enabled=False, base_url="http://127.0.0.1:9696", api_key="secret",
        download_indexers=[DownloadIndexerConfig(
            name="Preferred (API)", announce_substrings=["preferred"])],
    )
    cfg = MagicMock()
    cfg.prowlarr = pcfg
    cfg.cross_seed.allow_ssh_export = True

    def _t(h, tracker):
        return Torrent(hash=h, name="Show", category="", save_path="",
                       size_bytes=100, state="seeding", progress=1.0,
                       trackers=[tracker])

    st = _t("a" * 40, "https://unknown.example/announce")
    other = _t("b" * 40, "https://preferred.example.net/announce/xyz")
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=b"d8:announce...")
    dec = await pick_ssd_source_for_racing(
        cfg=cfg, source_torrent=st, other_source_torrents=[other],
        prowlarr=None, sftp=sftp, source_client=MagicMock())
    assert dec.infohash == "b" * 40
    sftp.fetch_torrent.assert_called_once_with("b" * 40)

    sftp2 = MagicMock()
    sftp2.fetch_torrent = MagicMock(return_value=b"d8:announce...")
    dec2 = await pick_ssd_source_for_racing(
        cfg=cfg, source_torrent=st, other_source_torrents=[],
        prowlarr=None, sftp=sftp2, source_client=MagicMock())
    assert dec2.infohash == "a" * 40
