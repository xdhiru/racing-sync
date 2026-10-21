"""Live-status refresh: parallel chunk fetch."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from conftest import make_coordinator


@pytest.mark.anyio
async def test_refresh_live_status_fetches_chunks_in_parallel():
    """4 hashes in 4 chunks must overlap in flight, and every entry
    updates from its chunk's rows."""
    coord = make_coordinator()
    coord._live = {
        f"{i:040d}": SimpleNamespace(progress=0.0, size_mb=0,
                                      dlspeed_bps=0,
                                      source_infohash=f"{i:040d}")
        for i in range(4)
    }
    in_flight = 0
    peak = 0

    async def _list(hashes=None, **kw):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await asyncio.sleep(0.05)
        finally:
            in_flight -= 1
        return [SimpleNamespace(hash=h, progress=0.5, size_bytes=100,
                                dlspeed_bps=10) for h in (hashes or [])]

    coord.dest_client = SimpleNamespace(list_torrents=_list)
    with patch("racing_sync.coordinator_live.chunked",
               side_effect=lambda keys: [[k] for k in keys]):
        await coord._refresh_live_status()
    assert peak >= 2
    assert all(v.progress == 0.5 for v in coord._live.values())
    assert all(v.dlspeed_bps == 10 for v in coord._live.values())
