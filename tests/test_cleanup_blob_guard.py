"""Pre-delete blob guard: never lose a racing .torrent with VPS1.

The janitor may delete a VPS1 content group at MOVING or at DONE while some
racing torrents were never injected. Their .torrent bytes only ever come
from VPS1, so the DB must hold a copy before the group disappears —
otherwise the re-inject loop and the late-seed repair can never rebuild
those seeds.

Covers the guard itself, its wiring into the delete loop and the DB-first
racing-bytes loader.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig, CleanupConfig
from racing_sync.state import State, StateStore, TorrentState

FIXTURES = Path(__file__).parent / "fixtures" / "config"
H = "a" * 40
NAME = "Show.S01E01.mkv"
SIZE = 700
BLOB = b"fake-torrent-bytes"


class _FakeSource:
    def __init__(self, members: list[Torrent]):
        self.members = members
        self.deleted: list[tuple[str, bool]] = []

    async def list_torrents(self, *, category=None, hashes=None):
        return list(self.members)

    async def delete(self, h: str, *, delete_files: bool = False):
        self.deleted.append((h.lower(), delete_files))


class _FakeDest:
    """Dest entries the fuse-seed verifier inspects."""

    def __init__(self, entries: list[Torrent] | None = None, *, fail: bool = False):
        self.entries = entries or []
        self.fail = fail

    async def list_torrents(self, *, category=None, hashes=None):
        if self.fail:
            raise RuntimeError("dest unreachable")
        want = {h.lower() for h in hashes} if hashes is not None else None
        return [t for t in self.entries if want is None or t.hash.lower() in want]


def _fuse_seed(h: str, save_path: str = "/mnt/fuse") -> Torrent:
    return Torrent(
        hash=h,
        name=NAME,
        category="",
        save_path=save_path,
        size_bytes=SIZE,
        state="seeding",
        progress=1.0,
    )


def _torrent_bytes(name: str = NAME, size: int = SIZE) -> bytes:
    from racing_sync.watchdir import _bencode

    return _bencode(
        {
            b"announce": b"http://tracker.example/announce",
            b"info": {
                b"name": name.encode(),
                b"length": size,
                b"piece length": 16384,
                b"pieces": b"0" * 20,
            },
        }
    )


def _hash_of(blob: bytes) -> str:
    from racing_sync.watchdir import _bencoded_info_hash

    return _bencoded_info_hash(blob)[0].lower()


def _member(h: str = H) -> Torrent:
    return Torrent(
        hash=h,
        name=NAME,
        category="",
        save_path="/vps1/data",
        size_bytes=SIZE,
        state="seeding",
        progress=1.0,
        ratio=2.0,
        trackers=[],
        upspeed_bps=0,
        num_leechers=0,
        added_on=0,
    )


def _row(store: StateStore, *, injected: str = "", infohash: str = H) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    ts = TorrentState(
        source_infohash=infohash,
        source_name=NAME,
        dest_infohash=infohash,
        total_bytes=SIZE,
        injected_private_hashes=injected,
        state=State.DONE,
        completed_at=now - dt.timedelta(hours=100),
        vps1_last_activity_at=now - dt.timedelta(hours=10),
    )
    store.upsert(ts)
    return ts


def _cleanup_cfg(*, dry_run: bool = False) -> CleanupConfig:
    return CleanupConfig(
        enabled=True,
        dry_run=dry_run,
        janitor_startup_delay_seconds=0.0,
        delete_stagger_seconds=0.0,
    )


def _coord(store: StateStore, src: _FakeSource, cfg: CleanupConfig, dest=None):
    """Coordinator wired for the delete loop (fuse verdict stubbed)."""
    app = AppConfig.from_toml(FIXTURES / "minimal.toml")
    app.cleanup = cfg
    coord = make_coordinator(store=store, cfg=app)
    coord.source_client = src
    coord.dest_client = dest if dest is not None else _FakeDest([])
    coord.sftp = None
    coord._stop = False
    coord._source_free_bytes = AsyncMock(return_value=None)
    # The guard is what is under test; fuse health has its own tests.
    coord._cleanup_fuse_healthy = AsyncMock(return_value=True)
    coord._delete_source_group = AsyncMock(wraps=coord._delete_source_group)
    return coord


@pytest.mark.anyio
async def test_uninjected_member_blob_preserved_then_deleted(tmp_path: Path):
    """Fetchable racing bytes are written to the DB before the VPS1 delete."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg())
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        store.set_racing_blob.assert_called_once_with(H, BLOB)
        assert store.get_racing_blob(H) == BLOB
        coord._delete_source_group.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unavailable_blob_keeps_vps1_and_warns(tmp_path: Path, caplog):
    """No bytes anywhere: fail closed, keep the VPS1 copy this run."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg())
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=None)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        with caplog.at_level("WARNING", logger="racing_sync.coordinator"):
            await coord._run_source_cleanup(coord.rt.cleanup.config)

        coord._delete_source_group.assert_not_awaited()
        store.set_racing_blob.assert_not_called()
        assert store.get_racing_blob(H) == b""
        assert "cannot be preserved" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_fuse_seeded_member_skips_fetch_and_delete_proceeds(tmp_path: Path):
    """A member proven to seed from fuse costs no blob I/O and deletes."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, injected=H)
        coord = _coord(
            store,
            _FakeSource([_member()]),
            _cleanup_cfg(),
            dest=_FakeDest([_fuse_seed(H)]),
        )
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        coord._load_racing_torrent_bytes.assert_not_awaited()
        store.set_racing_blob.assert_not_called()
        coord._delete_source_group.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_recorded_injected_but_not_seeding_preserves_bytes(tmp_path: Path, caplog):
    """Bookkeeping alone is not evidence: a missing fuse seed preserves first."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, injected=H)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg())
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        with caplog.at_level("WARNING", logger="racing_sync.coordinator"):
            await coord._run_source_cleanup(coord.rt.cleanup.config)

        store.set_racing_blob.assert_called_once_with(H, BLOB)
        coord._delete_source_group.assert_awaited_once()
        assert "recorded as injected but does not seed" in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_ssd_placed_entry_is_not_a_fuse_seed(tmp_path: Path):
    """An SSD save_path is a download in progress, not a VPS2 seed."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store)
        dest = _FakeDest([_fuse_seed(H, save_path="/downloads")])
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg(), dest=dest)
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        store.set_racing_blob.assert_called_once_with(H, BLOB)
        coord._delete_source_group.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unverifiable_fuse_lookup_preserves_before_delete(tmp_path: Path):
    """A dest lookup failure counts as "not seeded": preserve, never assume."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, injected=H)
        dest = _FakeDest(fail=True)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg(), dest=dest)
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        store.set_racing_blob.assert_called_once_with(H, BLOB)
        coord._delete_source_group.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_unverifiable_fuse_lookup_without_bytes_keeps_vps1(tmp_path: Path):
    """Fail-closed chain: no seed proof and no bytes means no delete."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, injected=H)
        dest = _FakeDest(fail=True)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg(), dest=dest)
        coord._load_racing_torrent_bytes = AsyncMock(return_value=None)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        store.set_racing_blob.assert_not_called()
        coord._delete_source_group.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_dry_run_checks_readability_without_writing(tmp_path: Path):
    """Rehearsal proves the bytes are fetchable but persists nothing."""
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg(dry_run=True))
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        await coord._run_source_cleanup(coord.rt.cleanup.config)

        coord._load_racing_torrent_bytes.assert_awaited_once_with(H)
        store.set_racing_blob.assert_not_called()
        assert store.get_racing_blob(H) == b""
        # The dry-run delete path only logs: no client delete, no write.
        coord._delete_source_group.assert_awaited_once()
        assert coord.source_client.deleted == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_source_member_uses_the_racing_loader(tmp_path: Path):
    """Racing bytes always come through the hash-validated loader."""
    store = StateStore(tmp_path / "state.db")
    try:
        ts = _row(store)
        coord = _coord(store, _FakeSource([_member()]), _cleanup_cfg())
        coord._load_racing_torrent_bytes = AsyncMock(return_value=BLOB)
        store.set_racing_blob = MagicMock(wraps=store.set_racing_blob)

        ok = await coord._cleanup_preserve_racing_blobs(ts, [_member()], dry_run=False)

        assert ok is True
        coord._load_racing_torrent_bytes.assert_awaited_once_with(H)
        store.set_racing_blob.assert_called_once_with(H, BLOB)
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_prefers_db_blob(tmp_path: Path):
    """A persisted racing blob is returned without touching VPS1."""
    racing = _torrent_bytes()
    h = _hash_of(racing)
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, infohash=h)
        assert store.set_racing_blob(h, racing) is True
        coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"other")

        got = await coord._load_racing_torrent_bytes(h)

        assert got == racing
        coord._fetch_racing_torrent_bytes.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_rejects_row_download_blob_for_the_source_hash(tmp_path: Path):
    """The row blob is the download torrent, not the racing torrent.

    Returning it made re-inject add the seedpool download under the racing
    hash; qB stored the download's own hash, so the racing entry never
    appeared and the racing seed was never created.
    """
    racing = _torrent_bytes()
    h = _hash_of(racing)
    download = _torrent_bytes(name="Example.Download.S01E01.mkv", size=SIZE + 1)
    assert _hash_of(download) != h
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, infohash=h)
        assert store.set_blob(h, download) is True
        coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=racing)

        got = await coord._load_racing_torrent_bytes(h)

        assert got == racing
        coord._fetch_racing_torrent_bytes.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_drops_a_poisoned_racing_cache(tmp_path: Path):
    racing = _torrent_bytes()
    h = _hash_of(racing)
    download = _torrent_bytes(name="Example.Download.S01E01.mkv", size=SIZE + 1)
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, infohash=h)
        assert store.set_racing_blob(h, download) is True
        coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=racing)

        assert await coord._load_racing_torrent_bytes(h) == racing
        assert store.get_racing_blob(h) == b""
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_falls_back_to_vps1_on_db_miss(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=BLOB)

        got = await coord._load_racing_torrent_bytes(H.upper())

        assert got == BLOB
        coord._fetch_racing_torrent_bytes.assert_awaited_once_with(H.upper())
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_ignores_mock_blob(tmp_path: Path):
    """Test doubles return a Mock from get_blob; never treat it as bytes."""
    racing = _torrent_bytes()
    h = _hash_of(racing)
    store = MagicMock()
    coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
    coord._fetch_racing_torrent_bytes = AsyncMock(return_value=racing)

    got = await coord._load_racing_torrent_bytes(h)

    assert got == racing
    assert isinstance(store.get_blob.return_value, MagicMock)
    coord._fetch_racing_torrent_bytes.assert_awaited_once()

    # Malformed hashes never reach the store or VPS1.
    coord._fetch_racing_torrent_bytes.reset_mock()
    assert await coord._load_racing_torrent_bytes("not-a-hash") is None
    coord._fetch_racing_torrent_bytes.assert_not_awaited()


@pytest.mark.anyio
async def test_racing_blob_cache_needs_no_state_row(tmp_path: Path):
    """Sibling racing torrents have no row; the cache still persists them."""
    store = StateStore(tmp_path / "state.db")
    other = "f" * 40
    try:
        assert store.set_racing_blob(other, BLOB) is True
        assert store.get_racing_blob(other) == BLOB
        # The row-scoped writer still refuses (documents the distinction).
        assert store.set_blob(other, BLOB) is False
        # Overwrite is idempotent and last-write-wins.
        assert store.set_racing_blob(other, b"newer") is True
        assert store.get_racing_blob(other) == b"newer"
        store.delete_racing_blob(other)
        assert store.get_racing_blob(other) == b""
    finally:
        store.close()


@pytest.mark.anyio
async def test_loader_prefers_racing_cache_over_row_blob(tmp_path: Path):
    racing = _torrent_bytes()
    h = _hash_of(racing)
    download = _torrent_bytes(name="Example.Download.S01E01.mkv", size=SIZE + 1)
    store = StateStore(tmp_path / "state.db")
    try:
        _row(store, infohash=h)
        assert store.set_blob(h, download) is True
        assert store.set_racing_blob(h, racing) is True
        coord = make_coordinator(store=store, cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
        coord._fetch_racing_torrent_bytes = AsyncMock(return_value=b"vps1")

        assert await coord._load_racing_torrent_bytes(h) == racing
        coord._fetch_racing_torrent_bytes.assert_not_awaited()
    finally:
        store.close()


def test_torrent_blob_table_is_created(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        names = {
            row[0]
            for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "torrent_blob" in names
    finally:
        store.close()
