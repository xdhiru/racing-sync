"""M6 API: /24 throttle + LRU, auth-fail context, readyz depth, counters."""
from __future__ import annotations

import logging
import time

import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

from racing_sync.config import APIConfig, AppConfig

from test_api import _api_coord
from racing_sync.api import build_app


def _clear_throttle():
    from racing_sync import api as api_mod
    api_mod._AUTH_FAILURES.clear()
    api_mod._AUTH_SUBNET_FAILURES.clear()
    api_mod._AUTH_LAST_SEEN.clear()


def test_subnet_spray_trips_throttle_together():
    """11 distinct IPs in one /24: throttled with no IP over the limit."""
    from racing_sync import api as api_mod

    _clear_throttle()
    try:
        out = [api_mod._note_auth_failure(f"10.10.10.{i}")
               for i in range(1, 12)]
        assert out[:10] == [False] * 10
        assert out[10] is True
        # Per-IP buckets hold one failure each — the /24 fired, not an IP.
        assert all(len(api_mod._AUTH_FAILURES[f"10.10.10.{i}"]) == 1
                   for i in range(1, 12))
    finally:
        _clear_throttle()


def test_throttle_eviction_is_lru_not_oldest_half():
    """Over-cap pressure drops stale entries; live bans survive."""
    import time as _time
    from racing_sync import api as api_mod

    _clear_throttle()
    try:
        stale = _time.monotonic() - 1000.0
        for i in range(api_mod._AUTH_CAP + 5):
            ip = f"192.0.2.{i % 250}.{i // 250}"
            api_mod._AUTH_FAILURES[ip] = [stale]
            api_mod._AUTH_LAST_SEEN[ip] = stale
        hot = "198.51.100.7"
        api_mod._AUTH_FAILURES[hot] = [_time.monotonic()] * 11
        api_mod._AUTH_LAST_SEEN[hot] = _time.monotonic()
        assert api_mod._note_auth_failure("203.0.113.9") is False
        # Stale spray gone; the live ban and the newcomer survive.
        assert hot in api_mod._AUTH_FAILURES
        assert "203.0.113.9" in api_mod._AUTH_FAILURES
        assert len(api_mod._AUTH_FAILURES) <= api_mod._AUTH_CAP
    finally:
        _clear_throttle()


def test_auth_failure_log_carries_endpoint_and_user(caplog):
    coord, _cfg = _api_coord()
    app = build_app(coord)
    client = TestClient(app, raise_server_exceptions=False)
    _clear_throttle()
    try:
        with caplog.at_level(logging.WARNING, logger="racing_sync.api"):
            assert client.get(
                "/api/state",
                headers={"X-Api-Token": "wrong"}).status_code == 401
    finally:
        _clear_throttle()
    rec = next(r for r in caplog.records
               if "api auth failure" in r.message)
    assert "/api/state" in rec.message and "GET" in rec.message


def test_readyz_not_ready_without_tick_or_paths():
    """Strict-typed checks: doubles and missing probes read NOT ready."""
    coord, _cfg = _api_coord()
    app = build_app(coord)
    client = TestClient(app)
    resp = client.get("/readyz", headers={"X-Api-Token": "secret"})
    assert resp.status_code == 503
    data = resp.json()
    assert data["ready"] is False
    assert data["checks"]["tick_fresh"] is False
    assert data["checks"]["fuse_mount"] is False
    assert data["checks"]["ssd_writable"] is False


def test_metrics_falls_back_to_bounded_sample():
    """Doubles without count_by_state keep the old sampled rows line."""
    from racing_sync.state import State, TorrentState

    coord, _cfg = _api_coord()
    del coord.store.count_by_state  # legacy double shape
    coord.store.all_active.return_value = [
        TorrentState(source_infohash="a" * 40, state=State.MOVING),
    ]
    coord._tasks = set()
    coord._started_at = 0.0
    app = build_app(coord)
    client = TestClient(app)
    body = client.get("/metrics",
                      headers={"X-Api-Token": "secret"}).text
    assert 'racing_sync_rows{state="moving"} 1' in body


def test_count_by_state_counts_all_states(tmp_path):
    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "c.db")
    try:
        store.upsert(TorrentState(
            source_infohash="a" * 40, source_name="A",
            state=State.DONE, total_bytes=1))
        store.upsert(TorrentState(
            source_infohash="b" * 40, source_name="B",
            state=State.FAILED, total_bytes=1))
        store.upsert(TorrentState(
            source_infohash="c" * 40, source_name="C",
            state=State.WAITING_DISK, total_bytes=1))
        counts = store.count_by_state()
    finally:
        try:
            store.close()
        except Exception:
            pass
    assert counts == {"done": 1, "failed": 1, "waiting_disk": 1}


@pytest.mark.anyio
async def test_tick_records_health_attrs():
    from unittest.mock import AsyncMock
    from conftest import make_coordinator

    coord = make_coordinator()
    coord._tick_inner = AsyncMock()
    await coord._tick()
    assert isinstance(coord._last_tick_at, float)
    assert coord._last_tick_duration_s >= 0.0
    assert coord._last_tick_at <= time.monotonic()


def test_prowlarr_and_rclone_counters_roundtrip():
    from racing_sync.prowlarr import prowlarr_counters
    from racing_sync.rclone_ops import rclone_counters

    assert prowlarr_counters() == {"searches": 0, "hits": 0, "errors": 0}
    assert rclone_counters() == {"moves": 0, "move_ok": 0, "move_fail": 0}


def test_forget_keeps_per_pair_attribution_and_identity(caplog):
    import logging
    from unittest.mock import AsyncMock, patch

    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="secret",
                        trust_nginx_header=False)
    coord = MagicMock()
    coord.cfg = cfg
    coord._ssd_release = AsyncMock()
    app = build_app(coord)
    client = TestClient(app, raise_server_exceptions=False)
    headers = {"X-Api-Token": "secret"}

    planned = {
        "applied": True,
        "source_infohash": "b" * 40,
        "source_name": "Pack",
        "dest_entries": [],
        "local_paths": [],
        "skipped_paths": [],
        "errors": [],
        "paired_cancelled": [
            {"source_infohash": "c" * 40, "source_name": "Pair",
             "errors": ["boom"]},
        ],
    }
    with patch("racing_sync.api.forget_torrent",
               new_callable=AsyncMock) as mock_forget:
        mock_forget.return_value = planned
        with caplog.at_level(logging.WARNING, logger="racing_sync.api"):
            resp = client.post("/api/forget/" + "b" * 40 + "?confirm=true",
                               headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["paired_cancelled"] == [
        {"source_infohash": "c" * 40, "source_name": "Pair",
         "errors": ["boom"]}]
    # Paired SSD release ran (inside the ops lock, not after it).
    coord._ssd_release.assert_any_await("c" * 40)
    assert any("by token" in r.message for r in caplog.records)


def test_ts_to_dict_carries_admission_flags():
    from racing_sync.api import _ts_to_dict
    from racing_sync.state import State, TorrentState
    from unittest.mock import MagicMock

    ts = TorrentState(source_infohash="a" * 40, source_name="Show",
                      state=State.QUEUED, skipped=2, force_direct=1,
                      fuse_verified=1, version=3)
    d = _ts_to_dict(ts)
    assert (d["skipped"], d["force_direct"], d["fuse_verified"],
            d["version"]) == (2, 1, 1, 3)
    # Test doubles answer truthy to everything: strict zeros, not 1s.
    assert _ts_to_dict(MagicMock())["version"] == 0


def test_oversize_body_rejected_before_handlers():
    coord, _cfg = _api_coord()
    app = build_app(coord)
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post("/api/recover", content=b"x" * (2 * 1024 * 1024),
                       headers={"X-Api-Token": "secret"})
    assert resp.status_code == 413


def test_authed_endpoints_rate_limited_healthz_spared():
    import racing_sync.api as api_mod

    coord, _cfg = _api_coord()
    app = build_app(coord)
    client = TestClient(app, raise_server_exceptions=False)
    headers = {"X-Api-Token": "secret"}
    api_mod._AUTHD_HITS.clear()
    try:
        codes = [client.get("/api/state", headers=headers).status_code
                 for _ in range(305)]
        assert 429 in codes
        assert codes.count(429) == 5
        # Liveness never throttles.
        assert client.get("/healthz").status_code == 200
    finally:
        api_mod._AUTHD_HITS.clear()
