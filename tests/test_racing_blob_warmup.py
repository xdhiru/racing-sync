"""Bulk racing-torrent warm-up: one fetch, only the wanted bytes cached.

The racing box hands over every `.torrent` it knows in ONE tar stream.
The warm-up may cache only the bytes of the racing torrents, and only
when they really are that torrent: bytes of a different torrent would
re-add the wrong hash and hide the racing seed.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.state import StateStore
from racing_sync.watchdir import _bencode, _bencoded_info_hash


def _torrent_bytes(name: str, size: int = 1024) -> bytes:
    """Real .torrent bytes for a fictional release."""
    info = {
        "name": name.encode(),
        "length": size,
        "piece length": 16384,
        "pieces": b"\x00" * 20,
    }
    return _bencode({"announce": "http://tracker.example.invalid/announce", "info": info})


def _infohash(blob: bytes) -> str:
    """The infohash these bytes really carry."""
    v1, _name, _size, _announce = _bencoded_info_hash(blob)
    return v1.lower()


def _racing_torrent(infohash: str, name: str) -> Torrent:
    """A racing-client entry; clients may report the hash uppercased."""
    return Torrent(
        hash=infohash.upper(),
        name=name,
        category="racing",
        save_path="/downloads",
        size_bytes=1024,
        state="seeding",
        progress=1.0,
        trackers=[],
    )


class _FakeBulkSFTP:
    """Stand-in for the SFTP exporter's bulk fetch (sync, like the real one)."""

    def __init__(self, blobs: dict[str, bytes]):
        self._blobs = blobs
        self.calls = 0

    def fetch_all_torrents(self) -> dict[str, bytes]:
        self.calls += 1
        return dict(self._blobs)


@pytest.fixture
def store(tmp_path):
    st = StateStore(tmp_path / "state.db")
    try:
        yield st
    finally:
        st.close()


def _coord(store, blobs, racing) -> object:
    coord = make_coordinator(store)
    coord.cfg = MagicMock()
    coord.sftp = _FakeBulkSFTP(blobs)
    coord._list_source_torrents = AsyncMock(return_value=racing)
    coord._stop = False
    return coord


@pytest.mark.anyio
async def test_warmup_caches_only_the_wanted_racing_hashes(store, caplog):
    wanted = _torrent_bytes("Example.Show.S01E01.mkv")
    sibling = _torrent_bytes("Example.Show.S01E02.mkv")
    unrelated = _torrent_bytes("Other.Release.S01E01.mkv")
    h_wanted = _infohash(wanted)
    h_sibling = _infohash(sibling)
    h_unrelated = _infohash(unrelated)
    coord = _coord(
        store,
        {h_wanted: wanted, h_sibling: sibling, h_unrelated: unrelated},
        [
            _racing_torrent(h_wanted, "Example.Show.S01E01.mkv"),
            _racing_torrent(h_sibling, "Example.Show.S01E02.mkv"),
        ],
    )

    with caplog.at_level(logging.INFO):
        await coord._racing_blob_warmup()

    assert coord.sftp.calls == 1
    assert coord.store.get_racing_blob(h_wanted) == wanted
    assert coord.store.get_racing_blob(h_sibling) == sibling
    # Not a racing torrent of this box: never cached.
    assert coord.store.get_racing_blob(h_unrelated) == b""
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "2/2 racing torrent(s) cached" in text


@pytest.mark.anyio
async def test_warmup_rejects_bytes_of_a_different_torrent(store):
    wanted = _torrent_bytes("Example.Show.S01E01.mkv")
    other = _torrent_bytes("Other.Release.S01E01.mkv")
    h_wanted = _infohash(wanted)
    h_other = _infohash(other)
    # The stream carries the wrong bytes under the racing hash.
    coord = _coord(
        store,
        {h_wanted: other},
        [_racing_torrent(h_wanted, "Example.Show.S01E01.mkv")],
    )

    await coord._racing_blob_warmup()

    assert coord.store.get_racing_blob(h_wanted) == b""
    assert coord.store.get_racing_blob(h_other) == b""


@pytest.mark.anyio
async def test_warmup_without_a_bulk_source_is_quiet(store):
    coord = _coord(store, {}, [])
    coord.sftp = None

    await coord._racing_blob_warmup()  # must not raise

    assert coord._list_source_torrents.await_count == 0


@pytest.mark.anyio
async def test_warmup_survives_a_broken_transport(store):
    class _Boom:
        def fetch_all_torrents(self):
            raise OSError("link down")

    coord = _coord(store, {}, [])
    coord.sftp = _Boom()

    await coord._racing_blob_warmup()  # must not raise


@pytest.mark.anyio
async def test_loop_does_nothing_when_the_interval_is_zero(store):
    blob = _torrent_bytes("Example.Show.S01E01.mkv")
    h = _infohash(blob)
    coord = _coord(store, {h: blob}, [_racing_torrent(h, "Example.Show.S01E01.mkv")])
    coord.cfg.source.deluge_sftp.bulk_fetch_interval_seconds = 0
    calls: list[int] = []

    async def fake_warmup():
        calls.append(1)
        coord._stop = True

    coord._racing_blob_warmup = fake_warmup
    await coord._racing_blob_loop()

    assert calls == []
    assert coord.sftp.calls == 0
    assert coord.store.get_racing_blob(h) == b""


@pytest.mark.anyio
async def test_loop_does_nothing_when_disabled(store):
    blob = _torrent_bytes("Example.Show.S01E01.mkv")
    h = _infohash(blob)
    coord = _coord(store, {h: blob}, [_racing_torrent(h, "Example.Show.S01E01.mkv")])
    coord.cfg.source.deluge_sftp.bulk_fetch_interval_seconds = 60
    coord.cfg.source.deluge_sftp.bulk_fetch_enabled = False
    calls: list[int] = []

    async def fake_warmup():
        calls.append(1)
        coord._stop = True

    coord._racing_blob_warmup = fake_warmup
    await coord._racing_blob_loop()

    assert calls == []


@pytest.mark.anyio
async def test_loop_runs_a_pass_when_enabled(store):
    blob = _torrent_bytes("Example.Show.S01E01.mkv")
    h = _infohash(blob)
    coord = _coord(store, {h: blob}, [_racing_torrent(h, "Example.Show.S01E01.mkv")])
    coord.cfg.source.deluge_sftp.bulk_fetch_interval_seconds = 1
    coord.cfg.source.deluge_sftp.bulk_fetch_enabled = True
    calls: list[int] = []

    async def fake_warmup():
        calls.append(1)
        coord._stop = True

    coord._racing_blob_warmup = fake_warmup
    await coord._racing_blob_loop()

    assert calls == [1]
