"""E7 regression: a successful cross-seed re-add records the injected hash.

`_re_add_cross_seed_torrent` used to fall through after a successful
`_ensure_fuse_entry`, never recording the injected hash. The next
RE_ADDING cycle then paid a duplicate add/verify (the early-return guards
check `ts.injected_private_hashes`). On success the row must now remember
the hash and persist it.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode, _bencoded_info_hash

SRC = "c" * 40


def _blob() -> bytes:
    return _bencode({
        b"info": {
            b"name": b"Show.Batch",
            b"length": 100,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })


def _scaffold(tmp_path):
    blob = _blob()
    blob_hash = _bencoded_info_hash(blob)[0].lower()

    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator(store)
    coord._target_mount_for = MagicMock(return_value=tmp_path / "fuse")
    coord._target_mount_for_blob = MagicMock(return_value=tmp_path / "fuse")
    coord._ensure_fuse_entry = AsyncMock(return_value=(True, "Ok."))
    coord.transition = MagicMock(
        side_effect=lambda t, s, error="": setattr(t, "state", s))

    ts = TorrentState(
        source_infohash=SRC,
        source_name="Show.Batch",
        dest_infohash="d" * 40,
        cross_seed_blob=blob,
        state=State.RE_ADDING,
        readd_attempts=1,
    )
    store.upsert(ts)
    return coord, store, blob_hash


@pytest.mark.anyio
async def test_success_records_and_persists_injected_hash(tmp_path):
    coord, store, blob_hash = _scaffold(tmp_path)
    try:
        await coord._re_add_cross_seed_torrent(store.get(SRC))

        coord._ensure_fuse_entry.assert_awaited_once()
        row = store.get(SRC)
        assert row is not None
        # Persisted: a fresh row read from the DB reflects the injection.
        assert row.injected_private_hashes == blob_hash
        assert blob_hash in (row.injected_private_hashes or "").split(",")
    finally:
        store.close()


@pytest.mark.anyio
async def test_second_call_skips_inject(tmp_path):
    coord, store, blob_hash = _scaffold(tmp_path)
    try:
        await coord._re_add_cross_seed_torrent(store.get(SRC))
        await coord._re_add_cross_seed_torrent(store.get(SRC))

        # Early-return guards on injected_private_hashes skip the
        # duplicate add/verify on the dest client.
        coord._ensure_fuse_entry.assert_awaited_once()
        row = store.get(SRC)
        assert row.injected_private_hashes == blob_hash
    finally:
        store.close()
