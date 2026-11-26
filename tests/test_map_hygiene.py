"""I15: one periodic sweep bounds every in-memory bookkeeping map.

Forgotten/cancelled rows never pop their own entries, so TTL maps must
shed expired keys even while small, and hash-keyed maps need a hard cap.
"""

from __future__ import annotations

import datetime as dt
import time

from conftest import make_coordinator


def test_sweep_prunes_expired_ttl_memos():
    coord = make_coordinator()
    now = time.monotonic()
    coord._fuse_outage_until = {"dead": now - 1.0, "live": now + 300.0}
    coord._fuse_healthy_until = {"late:dead": now - 1.0, "orig:live": now + 300.0}
    coord._waiting_disk_next_check = {"dead": now - 1.0, "live": now + 60.0}
    coord._supplement_ok_at = {"dead": now - 1.0, "live": now + 600.0}
    coord._failed_late_cross_seeds = {
        "dead": dt.datetime.now(dt.UTC) - dt.timedelta(hours=2),
        "live": dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5),
    }

    coord._sweep_runtime_maps()

    assert coord._fuse_outage_until == {"live": now + 300.0}
    assert coord._fuse_healthy_until == {"orig:live": now + 300.0}
    assert coord._waiting_disk_next_check == {"live": now + 60.0}
    assert coord._supplement_ok_at == {"live": now + 600.0}
    assert set(coord._failed_late_cross_seeds) == {"live"}


def test_sweep_drops_unparsable_ttl_values():
    """A corrupt memo must not keep itself alive (or crash the sweep)."""
    coord = make_coordinator()
    coord._fuse_outage_until = {"junk": "not-a-number", "none": None}

    coord._sweep_runtime_maps()

    assert coord._fuse_outage_until == {}


def test_sweep_caps_hash_keyed_maps_by_age_of_insertion():
    coord = make_coordinator()
    coord._moving_parks = {f"{i:040x}": 0 for i in range(5003)}
    coord._readd_heal_track = {f"{i + 6000:040x}": [0.0, (), 0] for i in range(5002)}
    coord._state_budget_hits = {f"{i + 9000:040x}": (None, 1) for i in range(5001)}

    coord._sweep_runtime_maps()

    assert len(coord._moving_parks) == 5000
    assert f"{0:040x}" not in coord._moving_parks
    assert len(coord._readd_heal_track) == 5000
    assert len(coord._state_budget_hits) == 5000


def test_sweep_is_throttled():
    coord = make_coordinator()
    coord._map_sweep_at = 0.0
    coord._supplement_ok_at = {"dead": 0.0}
    coord._sweep_runtime_maps()
    assert coord._supplement_ok_at == {}

    coord._supplement_ok_at = {"dead-again": 0.0}
    coord._sweep_runtime_maps()
    assert coord._supplement_ok_at == {"dead-again": 0.0}, "a second pass inside 60s is a no-op"


def test_late_seed_sweep_delegates_to_the_shared_sweep():
    coord = make_coordinator()
    coord._fuse_outage_until = {"dead": 0.0, "live": time.monotonic() + 300.0}

    coord._late_seed_sweep()

    assert set(coord._fuse_outage_until) == {"live"}
