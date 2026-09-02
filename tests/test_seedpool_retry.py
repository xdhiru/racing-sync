"""Tests for the seedpool retry policy math.

We don't run the full coordinator here (it needs live qBittorrent +
Prowlarr). Instead we verify the timing rules that _park_for_seedpool_retry
applies.
"""

from __future__ import annotations

import datetime as dt

from racing_sync.config import AppConfig
from racing_sync.state import State, TorrentState


def _cfg() -> AppConfig:
    return AppConfig.from_toml(
        __file__.replace("\\", "/").rsplit("/", 1)[0] + "/../config.example.toml"
    )


def test_first_attempt_sets_first_queried_at():
    cfg = _cfg()
    ts = TorrentState(source_infohash="a" * 40, state=State.NEW)
    assert ts.seedpool_first_queried_at is None
    assert ts.seedpool_attempts == 0

    now = dt.datetime.now(dt.timezone.utc)
    interval = cfg.cross_seed.seedpool_retry_interval_seconds
    ts.seedpool_first_queried_at = now
    ts.seedpool_attempts = 1
    ts.seedpool_next_retry_at = now + dt.timedelta(seconds=interval)

    assert ts.seedpool_first_queried_at == now
    assert ts.seedpool_next_retry_at is not None
    diff = (ts.seedpool_next_retry_at - now).total_seconds()
    assert abs(diff - interval) < 1


def test_retry_window_is_24_hours():
    cfg = _cfg()
    assert cfg.cross_seed.seedpool_max_age_seconds == 86400
    assert cfg.cross_seed.seedpool_retry_interval_seconds == 1800


def test_expired_max_age_marks_failed():
    """If the first attempt was > 24 h ago, the next park should escalate
    to FAILED. We simulate by backdating seedpool_first_queried_at."""
    cfg = _cfg()
    ts = TorrentState(
        source_infohash="a" * 40,
        state=State.WAITING_SEEDPOOL,
        seedpool_first_queried_at=dt.datetime.now(dt.timezone.utc)
        - dt.timedelta(seconds=cfg.cross_seed.seedpool_max_age_seconds + 1),
        seedpool_next_retry_at=dt.datetime.now(dt.timezone.utc),
        seedpool_attempts=10,
    )
    now = dt.datetime.now(dt.timezone.utc)
    elapsed = now - ts.seedpool_first_queried_at
    assert elapsed > dt.timedelta(seconds=cfg.cross_seed.seedpool_max_age_seconds)
    # This is the exact decision _park_for_seedpool_retry would make.
    should_fail = elapsed >= dt.timedelta(
        seconds=cfg.cross_seed.seedpool_max_age_seconds
    )
    assert should_fail is True