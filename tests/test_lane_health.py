"""H1: production loop lane heartbeats, /readyz freshness, /metrics lanes."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from conftest import make_coordinator
from fastapi.testclient import TestClient

from racing_sync.api import build_app
from racing_sync.config import APIConfig


def _lane_coord(tmp_path):
    """Bare real Coordinator + real config namespace (strict lane methods)."""
    coord = make_coordinator()
    cfg = SimpleNamespace(
        api=APIConfig(enabled=True, api_token="secret", trust_nginx_header=False),
        rclone=SimpleNamespace(fuse=SimpleNamespace(mount=str(tmp_path))),
        ssd=SimpleNamespace(path=str(tmp_path)),
        general=SimpleNamespace(source_poll_interval=30, dest_poll_interval=30),
    )
    coord.cfg = cfg
    coord.source_client = MagicMock()
    coord.dest_client = MagicMock()
    coord.store.all_active.return_value = []
    coord._stop = False
    return coord


def _readyz(coord):
    client = TestClient(build_app(coord))
    return client.get("/readyz", headers={"X-Api-Token": "secret"})


def test_lane_health_map_replaces_non_dict_double():
    coord = make_coordinator()
    coord._lane_health = MagicMock()
    assert isinstance(coord._lane_health_map(), dict)
    coord._lane_note_attempt("poller")
    assert isinstance(coord._lane_health, dict)
    assert coord.lane_health_snapshot()["poller"]["last_attempt_at"] is not None


@pytest.mark.anyio
@pytest.mark.parametrize(
    "loop_name,step_name,lane",
    [
        ("_poller_loop", "_poll_source_step", "poller"),
        ("_scheduler_loop", "_schedule_step", "scheduler"),
        ("_janitor_loop", "_janitor_step", "janitor"),
    ],
)
async def test_loops_record_lane_success(anyio_backend, loop_name, step_name, lane):
    coord = make_coordinator()
    coord._stop = False
    coord.cfg.general.source_poll_interval = 9999
    coord.cfg.general.dest_poll_interval = 9999
    coord.cfg.cleanup.janitor_interval_seconds = 9999

    async def fake_step():
        coord.request_stop()

    setattr(coord, step_name, fake_step)
    await getattr(coord, loop_name)()

    entry = coord.lane_health_snapshot()[lane]
    assert entry["last_attempt_at"] is not None
    assert entry["last_success_at"] is not None
    assert entry["last_duration_s"] >= 0.0
    assert entry["consecutive_failures"] == 0
    assert entry["last_error"] == ""


@pytest.mark.anyio
async def test_poller_loop_records_failure(anyio_backend):
    coord = make_coordinator()
    coord._stop = False
    coord.cfg.general.source_poll_interval = 9999

    async def failing_step():
        coord._stop = True
        raise RuntimeError("poller boom")

    coord._poll_source_step = failing_step
    await coord._poller_loop()

    entry = coord.lane_health_snapshot()["poller"]
    assert entry["last_attempt_at"] is not None
    assert entry["last_success_at"] is None
    assert entry["last_failure_at"] is not None
    assert entry["consecutive_failures"] == 1
    assert "poller boom" in entry["last_error"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "loop_name,step_name,lane",
    [
        ("_poller_loop", "_poll_source_step", "poller"),
        ("_scheduler_loop", "_schedule_step", "scheduler"),
    ],
)
async def test_loop_cancel_is_not_a_failure(anyio_backend, loop_name, step_name, lane):
    coord = make_coordinator()
    coord._stop = False
    coord.cfg.general.source_poll_interval = 9999
    coord.cfg.general.dest_poll_interval = 9999

    async def cancel_step():
        raise asyncio.CancelledError

    setattr(coord, step_name, cancel_step)
    await getattr(coord, loop_name)()

    entry = coord.lane_health_snapshot()[lane]
    assert entry["last_attempt_at"] is not None
    assert entry["last_success_at"] is None
    assert entry["last_failure_at"] is None
    assert entry["consecutive_failures"] == 0
    assert entry["last_error"] == ""


def test_readyz_200_with_fresh_required_lanes(tmp_path):
    coord = _lane_coord(tmp_path)
    coord._started_at = time.monotonic() - 5.0
    for name in ("poller", "scheduler"):
        coord._lane_note_success(name, coord._lane_note_attempt(name) - 0.1)

    resp = _readyz(coord)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ready"] is True
    for name in ("poller", "scheduler"):
        lane = data["lanes"][name]
        assert lane["required"] is True
        assert lane["ok"] is True
        assert lane["last_success_age_s"] is not None
        assert lane["last_attempt_age_s"] is not None
        assert lane["consecutive_failures"] == 0
        assert lane["last_error"] == ""
    assert data["checks"]["poller_fresh"] is True
    assert data["checks"]["scheduler_fresh"] is True
    assert data["checks"]["tick_fresh"] is True
    assert data["lanes"]["janitor"]["required"] is False
    assert data["lanes"]["qb_listener"]["required"] is False
    assert TestClient(build_app(coord)).get("/readyz").status_code == 401


def test_readyz_503_when_required_lane_stale(tmp_path):
    coord = _lane_coord(tmp_path)
    now = time.monotonic()
    coord._started_at = now - 5.0
    coord._lane_note_success("poller", now - 0.1)
    coord._lane_note_success("scheduler", now - 0.1)
    coord._lane_health["scheduler"]["last_success_at"] = now - 301.0
    coord._lane_health["scheduler"]["last_attempt_at"] = now - 301.0

    resp = _readyz(coord)
    assert resp.status_code == 503
    data = resp.json()
    assert data["ready"] is False
    assert data["checks"]["poller_fresh"] is True
    assert data["checks"]["scheduler_fresh"] is False
    assert data["checks"]["tick_fresh"] is False
    assert data["lanes"]["poller"]["ok"] is True
    assert data["lanes"]["scheduler"]["ok"] is False


def test_readyz_threshold_follows_configured_interval(tmp_path):
    coord = _lane_coord(tmp_path)
    coord.cfg.general.source_poll_interval = 200
    now = time.monotonic()
    coord._started_at = now - 5.0
    coord._lane_note_success("poller", now - 0.1)
    coord._lane_note_success("scheduler", now - 0.1)
    # source threshold = max(300, 3*200) = 600s: a 400s-old pass is fresh.
    coord._lane_health["poller"]["last_success_at"] = now - 400.0
    coord._lane_health["poller"]["last_attempt_at"] = now - 400.0

    assert _readyz(coord).status_code == 200


def test_readyz_200_during_startup_grace(tmp_path):
    coord = _lane_coord(tmp_path)
    coord._started_at = time.monotonic()

    resp = _readyz(coord)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ready"] is True
    assert data["lanes"]["poller"]["ok"] is True
    assert data["lanes"]["scheduler"]["ok"] is True
    assert data["lanes"]["poller"]["last_success_age_s"] is None


def test_optional_lane_failures_do_not_gate_readiness(tmp_path):
    coord = _lane_coord(tmp_path)
    # Janitor floor 300s -> 900s threshold; both optional lanes are older
    # than that, so they read stale while readiness stays True.
    coord.cfg.cleanup = SimpleNamespace(janitor_interval_seconds=300)
    now = time.monotonic()
    coord._started_at = now - 5000.0
    coord._lane_note_success("poller", now - 0.1)
    coord._lane_note_success("scheduler", now - 0.1)
    for name in ("janitor", "qb_listener"):
        coord._lane_health[name] = {
            "last_attempt_at": now - 1000.0,
            "last_success_at": None,
            "last_failure_at": now - 1000.0,
            "last_duration_s": 0.5,
            "consecutive_failures": 4,
            "last_error": f"{name} boom",
        }

    resp = _readyz(coord)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ready"] is True
    assert data["lanes"]["janitor"]["ok"] is False
    assert data["lanes"]["qb_listener"]["ok"] is False
    assert data["lanes"]["janitor"]["consecutive_failures"] == 4
    assert data["lanes"]["janitor"]["last_error"] == "janitor boom"


def test_janitor_freshness_follows_configured_interval(tmp_path):
    coord = _lane_coord(tmp_path)
    # Configured 200s is floored to the loop's 300s sleep -> 900s threshold.
    coord.cfg.cleanup = SimpleNamespace(janitor_interval_seconds=200)
    now = time.monotonic()
    coord._started_at = now - 5.0
    coord._lane_note_success("poller", now - 0.1)
    coord._lane_note_success("scheduler", now - 0.1)
    coord._lane_health["janitor"] = {
        "last_attempt_at": now - 700.0,
        "last_success_at": now - 700.0,
        "last_failure_at": None,
        "last_duration_s": 0.5,
        "consecutive_failures": 0,
        "last_error": "",
    }

    resp = _readyz(coord)
    assert resp.status_code == 200
    data = resp.json()
    assert data["lanes"]["janitor"]["ok"] is True
    assert data["lanes"]["janitor"]["last_success_age_s"] >= 700.0


def test_readyz_tick_fresh_is_required_lane_alias(tmp_path):
    coord = _lane_coord(tmp_path)
    now = time.monotonic()
    coord._started_at = now - 5.0
    coord._lane_note_success("poller", now - 0.1)
    coord._lane_note_success("scheduler", now - 0.1)
    coord._lane_health["scheduler"]["last_success_at"] = now - 301.0
    coord._lane_health["scheduler"]["last_attempt_at"] = now - 301.0

    checks = _readyz(coord).json()["checks"]
    assert checks["poller_fresh"] is True
    assert checks["scheduler_fresh"] is False
    assert checks["tick_fresh"] is False


def test_metrics_exposes_lane_and_derived_tick_aliases(tmp_path):
    coord = _lane_coord(tmp_path)
    now = time.monotonic()
    coord._started_at = now - 10.0
    coord._lane_health = {
        "poller": {
            "last_attempt_at": now - 5.0,
            "last_success_at": now - 4.0,
            "last_failure_at": None,
            "last_duration_s": 1.25,
            "consecutive_failures": 0,
            "last_error": "",
        },
        "scheduler": {
            "last_attempt_at": now - 21.0,
            "last_success_at": now - 20.0,
            "last_failure_at": None,
            "last_duration_s": 2.5,
            "consecutive_failures": 0,
            "last_error": "",
        },
        "janitor": {
            "last_attempt_at": now - 900.0,
            "last_success_at": None,
            "last_failure_at": now - 900.0,
            "last_duration_s": 0.5,
            "consecutive_failures": 3,
            "last_error": "janitor boom",
        },
    }
    client = TestClient(build_app(coord))
    resp = client.get("/metrics", headers={"X-Api-Token": "secret"})
    assert resp.status_code == 200
    body = resp.text
    assert 'racing_sync_lane_success_age_seconds{lane="poller"}' in body
    assert 'racing_sync_lane_attempt_age_seconds{lane="poller"}' in body
    assert 'racing_sync_lane_latency_seconds{lane="poller"} 1.250' in body
    assert 'racing_sync_lane_consecutive_failures{lane="poller"} 0' in body
    assert 'racing_sync_lane_consecutive_failures{lane="janitor"} 3' in body
    # Legacy aliases derive from lane health when _last_tick_* is unset.
    assert "racing_sync_tick_latency_seconds 2.500" in body
    line = next(ln for ln in body.splitlines() if ln.startswith("racing_sync_tick_age_seconds "))
    assert 20.0 <= float(line.split()[1]) <= 30.0
    # A lane with no data emits nothing.
    assert 'lane="qb_listener"' not in body
