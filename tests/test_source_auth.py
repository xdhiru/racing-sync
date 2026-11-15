"""Source-auth fail-fast: bad credentials surface loudly, rows survive."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.clients.http_base import AuthError
from racing_sync.state import State, StateStore, TorrentState


def _src(h, trackers=("https://alpha.cc/announce/xyz",)):
    return Torrent(
        hash=h,
        name="Show",
        save_path="/x",
        category="",
        state="downloading",
        progress=0.0,
        size_bytes=10,
        trackers=list(trackers),
    )


def test_cached_source_lookup_shares_index_per_tick():
    """One O(N) index build per cache version, shared across lookups."""
    coord = make_coordinator()
    coord._source_torrents_cache = [_src("a" * 40)]
    assert coord._cached_source_torrent("a" * 40) is not None
    assert coord._cached_source_torrent("b" * 40) is None
    idx = coord._source_cache_index
    assert coord._cached_source_torrent("a" * 40) is not None
    assert coord._source_cache_index is idx
    # Cache replaced on refresh: index rebuilds, old entry gone.
    coord._source_torrents_cache = [_src("b" * 40)]
    assert coord._cached_source_torrent("a" * 40) is None
    assert coord._cached_source_torrent("b" * 40) is not None
    assert coord._source_cache_index is not idx


@pytest.mark.anyio
async def test_source_list_auth_error_propagates():
    """AuthError must reach the poller/scheduler AuthError arms, not be
    served as infinite stale cache."""
    coord = make_coordinator()
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(side_effect=AuthError("bad creds"))
    coord.cfg.source.category = ""
    with pytest.raises(AuthError):
        await coord._list_source_torrents(force_refresh=True)


@pytest.mark.anyio
async def test_source_list_outage_still_serves_stale(tmp_path):
    """Non-auth outages keep the degraded stale-cache behavior."""
    coord = make_coordinator()
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(side_effect=TimeoutError("wedged"))
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
        ts = TorrentState(source_infohash="a" * 40, source_name="X", state=State.DOWNLOADING)
        store.upsert(ts)

        async def _boom(_ts):
            raise AuthError("session expired")

        coord._process_torrent_inner = _boom  # type: ignore[method-assign]
        await coord._process_torrent(ts)
        assert store.get("a" * 40).state == State.DOWNLOADING
    finally:
        store.close()


@pytest.mark.anyio
async def test_stale_source_expires_after_outage():
    """A wedged source serves stale briefly, then expires to empty so
    intake stops grouping ghosts for a dead VPS1."""
    import time

    from racing_sync.clients.abstract import Torrent

    def _coord_with_cache(age_s):
        coord = make_coordinator()
        coord.source_client = MagicMock()
        coord.source_client.list_torrents = AsyncMock(side_effect=TimeoutError("wedged"))
        coord.cfg.source.category = ""
        coord._source_torrents_cache = [
            Torrent(
                hash="a" * 40,
                name="Ghost",
                save_path="/x",
                category="",
                state="downloading",
                progress=0.0,
                size_bytes=10,
            )
        ]
        coord._source_torrents_cached_at = 0.0
        coord._source_last_success_mono = time.monotonic() - age_s
        return coord

    # Recent success: degraded stale view.
    assert len(await _coord_with_cache(60)._list_source_torrents(force_refresh=True)) == 1
    # Dead for 10 minutes: expired to empty.
    assert await _coord_with_cache(600)._list_source_torrents(force_refresh=True) == []


@pytest.mark.anyio
async def test_unknown_age_fails_min_age_gate():
    """Torrents with no added_on are excluded when min_age_seconds > 0."""
    import time

    from racing_sync.clients.abstract import Torrent

    coord = make_coordinator()
    now = time.time()
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash="a" * 40,
                name="Known",
                save_path="/x",
                category="",
                state="downloading",
                progress=0.0,
                size_bytes=10,
                added_on=now - 3600,
            ),
            Torrent(
                hash="b" * 40,
                name="Unknown",
                save_path="/x",
                category="",
                state="downloading",
                progress=0.0,
                size_bytes=10,
                added_on=0,
            ),
        ]
    )
    coord.cfg.source.category = ""
    coord.cfg.source.min_age_seconds = 60
    got = await coord._list_source_torrents(force_refresh=True)
    assert [t.infohash for t in got] == ["a" * 40]
