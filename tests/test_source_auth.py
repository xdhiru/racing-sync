"""Source-auth fail-fast: bad credentials surface loudly, rows survive."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.http_base import AuthError
from racing_sync.state import State, StateStore, TorrentState


@pytest.mark.anyio
async def test_source_list_auth_error_propagates():
    """AuthError must reach the poller/scheduler AuthError arms, not be
    served as infinite stale cache."""
    coord = make_coordinator()
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(
        side_effect=AuthError("bad creds"))
    coord.cfg.source.category = ""
    with pytest.raises(AuthError):
        await coord._list_source_torrents(force_refresh=True)


@pytest.mark.anyio
async def test_source_list_outage_still_serves_stale(tmp_path):
    """Non-auth outages keep the degraded stale-cache behavior."""
    coord = make_coordinator()
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(
        side_effect=TimeoutError("wedged"))
    coord.cfg.source.category = ""
    coord._source_torrents_cache = []
    coord._source_torrents_cached_at = 0.0
    assert await coord._list_source_torrents(force_refresh=True) == []


@pytest.mark.anyio
async def test_worker_leaves_row_on_auth_error(tmp_path):
    """A mid-flight AuthError leaves the row untouched for the next tick
    instead of failing it over an ops incident."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store=store)
        ts = TorrentState(source_infohash="a" * 40, source_name="X",
                          state=State.DOWNLOADING)
        store.upsert(ts)

        async def _boom(_ts):
            raise AuthError("session expired")

        coord._process_torrent_inner = _boom  # type: ignore[method-assign]
        await coord._process_torrent(ts)
        assert store.get("a" * 40).state == State.DOWNLOADING
    finally:
        store.close()
