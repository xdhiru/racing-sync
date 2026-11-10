"""Manual fuse adoption: same infohash already seeding from fuse on VPS2.

Covers the reported incident: operator manually moves files to the rclone
remote and adds the VPS1 torrent on VPS2 pointing at the fuse mount with no
category/tags. The row was stuck in WAITING_INDEXER waiting for a prowlarr
hit that would never come, because:

  - recovery only lists category="racing" (manual add invisible), and
  - NEW/WAITING_INDEXER workers never checked dest before querying prowlarr.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.recovery import reconcile
from racing_sync.state import State, StateStore, TorrentState


def _fuse_cfg(tmp_path: Path, coordinator):
    fuse = tmp_path / "fuse"
    fuse.mkdir(exist_ok=True)
    (fuse / "unsorted").mkdir(exist_ok=True)
    coordinator.cfg.rclone.fuse.mount = fuse
    coordinator.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coordinator.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    return fuse


def _seeded_fuse_torrent(fuse: Path, h: str, name: str, *, category: str = "") -> Torrent:
    # No category by default: reproduces the manual add without settings.
    return Torrent(
        hash=h,
        name=name,
        category=category,
        save_path=str(fuse),
        size_bytes=100,
        state="seeding",
        progress=1.0,
    )


@pytest.mark.anyio
async def test_new_adopts_manual_fuse_without_category(tmp_path: Path):
    """_do_new fast-tracks to DONE without touching prowlarr."""
    store = StateStore(tmp_path / "state.db")
    h = "a" * 40
    ts = TorrentState(source_infohash=h, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"x" * 100)

    coord.source_client = AsyncMock()
    coord.source_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash=h, name="Pack.2026", category="", save_path="",
            size_bytes=100, state="seeding", progress=1.0,
            trackers=["https://tracker.animebytes.tv/announce"],
        )
    )
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.source_client.export_torrent = AsyncMock(return_value=b"blob")
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Pack.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100, progress=1.0)]
    )
    # Must not be reached when manual fuse is present.
    coord.prowlarr = AsyncMock()
    coord.prowlarr.best_match = AsyncMock(
        side_effect=AssertionError("prowlarr must not be queried for manual fuse")
    )
    coord.sftp = None

    await coord._do_new(ts)

    reloaded = store.get(h)
    assert reloaded is not None
    assert reloaded.state == State.DONE
    assert reloaded.dest_infohash == h
    assert reloaded.save_path == str(fuse)
    coord.prowlarr.best_match.assert_not_called()
    # Category-agnostic: hash lookup, never a category filter.
    _, kwargs = coord.dest_client.list_torrents.call_args
    assert kwargs.get("hashes") == [h]
    assert "category" not in kwargs


@pytest.mark.anyio
async def test_waiting_indexer_adopts_manual_fuse(tmp_path: Path):
    """Parked rows adopt on retry instead of another prowlarr miss."""
    store = StateStore(tmp_path / "state.db")
    h = "b" * 40
    ts = TorrentState(source_infohash=h, source_name="Pack.2026", state=State.WAITING_INDEXER)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"y" * 100)

    coord.source_client = AsyncMock()
    coord.source_client.get_torrent = AsyncMock(
        return_value=Torrent(
            hash=h, name="Pack.2026", category="", save_path="",
            size_bytes=100, state="seeding", progress=1.0,
            trackers=["https://beta.me/announce"],
        )
    )
    coord.source_client.list_torrents = AsyncMock(return_value=[])
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Pack.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100, progress=1.0)]
    )
    coord.prowlarr = AsyncMock()
    coord.prowlarr.best_match = AsyncMock(
        side_effect=AssertionError("prowlarr must not be queried for manual fuse")
    )
    coord.sftp = None

    await coord._do_waiting_indexer(ts)

    assert store.get(h).state == State.DONE


@pytest.mark.anyio
async def test_manual_fuse_missing_bytes_does_not_adopt(tmp_path: Path):
    """skip_check ghost (complete but bytes absent, nothing on SSD): keep flow."""
    store = StateStore(tmp_path / "state.db")
    h = "c" * 40
    ts = TorrentState(source_infohash=h, source_name="Ghost.2026", state=State.NEW)
    store.upsert(ts)

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd

    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Ghost.2026")]
    )
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Ghost.2026.mkv", size_bytes=100, progress=1.0)]
    )

    adopted = await coord._adopt_manual_fuse_if_present(ts)

    assert adopted is False
    assert store.get(h).state == State.NEW


@pytest.mark.anyio
async def test_manual_fuse_incomplete_or_off_fuse_does_not_adopt(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"z" * 100)

    # Incomplete (still downloading) on fuse.
    h1 = "d" * 40
    ts1 = TorrentState(source_infohash=h1, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts1)
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h1, name="Pack.2026", category="", save_path=str(fuse),
                size_bytes=100, state="downloading", progress=0.5,
            )
        ]
    )
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[])
    assert await coord._adopt_manual_fuse_if_present(ts1) is False
    assert store.get(h1).state == State.NEW

    # Complete but on SSD, not fuse.
    h2 = "e" * 40
    ts2 = TorrentState(source_infohash=h2, source_name="Pack.2026", state=State.NEW)
    store.upsert(ts2)
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[
            Torrent(
                hash=h2, name="Pack.2026", category="racing",
                save_path="/local/ssd", size_bytes=100,
                state="seeding", progress=1.0,
            )
        ]
    )
    assert await coord._adopt_manual_fuse_if_present(ts2) is False
    assert store.get(h2).state == State.NEW


@pytest.mark.anyio
async def test_tick_sweep_adopts_parked_rows_in_one_lookup(tmp_path: Path):
    """_sweep_manual_fuse_adoptions batch-adopts without per-row RPC storm."""
    from racing_sync.coordinator import Coordinator  # noqa: F401  (ensures import ok)

    store = StateStore(tmp_path / "state.db")
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir(exist_ok=True)
    (fuse / "A.mkv").write_bytes(b"a" * 50)
    (fuse / "B.mkv").write_bytes(b"b" * 60)

    ha, hb = "a" * 40, "b" * 40
    store.upsert(TorrentState(source_infohash=ha, source_name="A", state=State.WAITING_INDEXER))
    store.upsert(TorrentState(source_infohash=hb, source_name="B", state=State.NEW))

    coord = make_coordinator()
    coord.store = store
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._running_infohashes = set()
    coord.dest_client = AsyncMock()

    async def _list_torrents(*, category=None, hashes=None):
        assert category is None  # category-agnostic
        assert hashes is not None
        out = []
        if ha in hashes:
            out.append(_seeded_fuse_torrent(fuse, ha, "A"))
        if hb in hashes:
            out.append(_seeded_fuse_torrent(fuse, hb, "B"))
        return out

    coord.dest_client.list_torrents = AsyncMock(side_effect=_list_torrents)

    async def _files(h):
        if h == ha:
            return [TorrentFile(name="A.mkv", size_bytes=50, progress=1.0)]
        return [TorrentFile(name="B.mkv", size_bytes=60, progress=1.0)]

    coord.dest_client.get_torrent_files = AsyncMock(side_effect=_files)

    await coord._sweep_manual_fuse_adoptions()

    assert coord.dest_client.list_torrents.await_count == 1
    assert store.get(ha).state == State.DONE
    assert store.get(hb).state == State.DONE


@pytest.mark.anyio
async def test_tick_sweep_skips_live_workers(tmp_path: Path):
    """Rows with a live worker are left to their own _do_* check (no race)."""
    store = StateStore(tmp_path / "state.db")
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir(exist_ok=True)
    (fuse / "A.mkv").write_bytes(b"a" * 10)

    ha = "a" * 40
    store.upsert(TorrentState(source_infohash=ha, source_name="A", state=State.NEW))

    coord = make_coordinator()
    coord.store = store
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord._running_infohashes = {ha}
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])

    await coord._sweep_manual_fuse_adoptions()

    # Skipped before any dest RPC.
    coord.dest_client.list_torrents.assert_not_called()
    assert store.get(ha).state == State.NEW


@pytest.mark.anyio
async def test_adopt_skipped_when_sweep_covered(tmp_path: Path):
    """A workerless row checked by this tick's sweep skips its own dest
    lookup; rows with live workers still check."""
    store = StateStore(tmp_path / "state.db")
    h = "f" * 40
    store.upsert(TorrentState(source_infohash=h, source_name="Pack",
                              state=State.NEW))
    coord = make_coordinator()
    coord.store = store
    coord._running_infohashes = set()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    try:
        ts = store.get(h)
        # No sweep yet: lookup runs.
        assert await coord._adopt_manual_fuse_if_present(ts) is False
        assert coord.dest_client.list_torrents.await_count == 1
        # Sweep covered workerless rows this tick: lookup skipped.
        coord._note_fuse_sweep_covered()
        assert await coord._adopt_manual_fuse_if_present(ts) is False
        assert coord.dest_client.list_torrents.await_count == 1
        # Live worker of its own: the sweep skips it, so it checks.
        coord._running_infohashes = {h}
        assert await coord._adopt_manual_fuse_if_present(ts) is False
        assert coord.dest_client.list_torrents.await_count == 2
    finally:
        store.close()


@pytest.mark.anyio
async def test_queued_prefers_fuse_complete_over_ssd(tmp_path: Path):
    """SSD+fuse duplicates: the fuse-complete entry wins adoption instead
    of the first-listed SSD partial."""
    from racing_sync.watchdir import _bencode

    store = StateStore(tmp_path / "state.db")
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    h = "g" * 40
    blob = bytes(_bencode({
        b"info": {b"name": b"Pack.2026.mkv", b"length": 100},
    }))
    store.upsert(TorrentState(
        source_infohash=h, source_name="Pack.2026", state=State.QUEUED,
        cross_seed_blob=blob))

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"x" * 100)
    coord.dest_client = AsyncMock()
    # SSD entry listed first: must lose to the fuse-complete copy.
    coord.dest_client.list_torrents = AsyncMock(return_value=[
        Torrent(hash=h, name="Pack.2026", category="racing",
                save_path=str(ssd), size_bytes=100,
                state="seeding", progress=1.0),
        _seeded_fuse_torrent(fuse, h, "Pack.2026"),
    ])
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100,
                                  progress=1.0)]
    )
    try:
        await coord._do_queued(store.get(h))
        row = store.get(h)
        assert row.state == State.DONE
        assert row.save_path == str(fuse)
    finally:
        store.close()


@pytest.mark.anyio
async def test_queued_reinject_releases_download_slot(tmp_path: Path):
    """Fuse re-inject runs with the download slot released (it downloads
    nothing); the slot is re-acquired before DONE."""
    import asyncio

    from racing_sync.watchdir import _bencode

    store = StateStore(tmp_path / "state.db")
    h = "r" * 40
    blob = bytes(_bencode({
        b"info": {b"name": b"Pack.2026.mkv", b"length": 100},
    }))
    store.upsert(TorrentState(
        source_infohash=h, source_name="Pack.2026", state=State.QUEUED,
        cross_seed_blob=blob))

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    (fuse / "Pack.2026.mkv").write_bytes(b"x" * 100)
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = True
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(
        return_value=[_seeded_fuse_torrent(fuse, h, "Pack.2026")])
    coord.dest_client.get_torrent_files = AsyncMock(
        return_value=[TorrentFile(name="Pack.2026.mkv", size_bytes=100,
                                  progress=1.0)]
    )
    sem = asyncio.Semaphore(1)
    coord._download_sem = sem
    await sem.acquire()  # caller holds the only slot
    seen: dict[str, int] = {}

    async def _inject(_ts):
        seen["value"] = sem._value

    coord._re_inject_racing_torrents = AsyncMock(side_effect=_inject)
    try:
        await coord._do_queued(store.get(h), _holds_download_slot=True)
        assert seen["value"] == 1  # slot freed during inject
        assert sem._value == 0  # re-acquired after
        assert store.get(h).state == State.DONE
    finally:
        sem.release()
        store.close()


def _category_seed(fuse: Path, h: str, name: str, files: list[tuple[str, int]]):
    """A complete, byte-verified seed living under a non-racing category."""
    cat = fuse / "SUB"
    cat.mkdir(exist_ok=True)
    for rel, size in files:
        target = cat / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x" * size)
    entry = Torrent(
        hash=h, name=name, category="SUB", save_path=str(cat),
        size_bytes=sum(size for _, size in files), state="seeding", progress=1.0,
    )
    return cat, entry


@pytest.mark.anyio
async def test_re_add_adopts_seed_under_category_subfolder(tmp_path: Path):
    """RE_ADDING row already seeding from /fuse/SUB must not re-inject.

    The gate derives the mount root from the blob and reports the
    subfolder bytes missing; the entry's own save_path is authoritative.
    """
    store = StateStore(tmp_path / "state.db")
    h = "9" * 40
    store.upsert(TorrentState(
        source_infohash=h, source_name="voices38-demon.slayer",
        dest_infohash=h, state=State.RE_ADDING))

    coord = make_coordinator()
    coord.store = store
    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir()
    cat, entry = _category_seed(
        fuse, h, "voices38-demon.slayer",
        [("voices38.nfo", 10), ("game.iso", 90)],
    )
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    coord.cfg.fuse_reinject_delay_seconds = 0
    coord.cfg.fuse_reinject_retry_gap_seconds = 120
    coord.cfg.fuse_reinject_backoff_seconds = 1800
    coord.cfg.fuse_reinject_max_age_seconds = 86400
    coord.cfg.cross_seed.inject_racing_torrents_to_fuse = False
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[entry])
    coord.dest_client.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="voices38.nfo", size_bytes=10, progress=1.0),
        TorrentFile(name="game.iso", size_bytes=90, progress=1.0),
    ])
    coord.dest_client.add_torrent = AsyncMock(
        side_effect=AssertionError("already seeding; must not re-add"))

    try:
        await coord._do_re_add(store.get(h))
        row = store.get(h)
        assert row.state == State.DONE
        assert row.save_path == str(cat)
        coord.dest_client.add_torrent.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_reconcile_keeps_done_seed_under_other_category(tmp_path: Path):
    """DONE row hidden from the racing snapshot keeps DONE via hash lookup."""
    store = StateStore(tmp_path / "state.db")
    h = "8" * 40
    store.upsert(TorrentState(
        source_infohash=h, source_name="voices38-demon.slayer",
        dest_infohash=h, state=State.DONE))

    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir()
    cat, entry = _category_seed(
        fuse, h, "voices38-demon.slayer", [("game.iso", 100)])

    cfg = MagicMock()
    cfg.rclone.fuse.mount = fuse
    cfg.rclone.fuse.mount_unsorted = fuse / "unsorted"
    cfg.dest.save_path = tmp_path / "ssd"
    cfg.ssd.path = tmp_path / "ssd"

    dest = AsyncMock()

    async def _list(*, category=None, hashes=None):
        if category == "racing":
            return []
        assert hashes and h in hashes
        return [entry]

    dest.list_torrents = AsyncMock(side_effect=_list)
    dest.get_torrent_files = AsyncMock(return_value=[
        TorrentFile(name="game.iso", size_bytes=100, progress=1.0),
    ])

    try:
        report = await reconcile(cfg, dest=dest, store=store)
        row = store.get(h)
        assert row.state == State.DONE
        assert h not in report.re_added
        assert h in report.kept
    finally:
        store.close()


@pytest.mark.anyio
async def test_queued_incomplete_fuse_entry_routes_to_readd(
        tmp_path: Path, caplog):
    """An incomplete fuse entry is never adopted as an SSD download."""
    store = StateStore(tmp_path / "state.db")
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    h = "i" * 40
    keep_path = str(ssd / "Pack.2026")
    store.upsert(TorrentState(
        source_infohash=h, source_name="Pack.2026", state=State.QUEUED,
        save_path=keep_path, cross_seed_blob=b"blob"))

    coord = make_coordinator()
    coord.store = store
    fuse = _fuse_cfg(tmp_path, coord)
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[Torrent(
        hash=h, name="Pack.2026", category="racing", save_path=str(fuse),
        size_bytes=100, state="downloading", progress=0.5)])
    coord._prepare_next_batch = AsyncMock()
    coord._park_queued_for_download_slot = AsyncMock(return_value=False)
    try:
        with caplog.at_level(logging.WARNING,
                             logger="racing_sync.coordinator"):
            await coord._do_queued(store.get(h))
        row = store.get(h)
        assert row.state == State.RE_ADDING
        assert row.dest_infohash == h
        assert row.save_path == keep_path
        coord.dest_client.resume.assert_not_awaited()
        coord._prepare_next_batch.assert_not_awaited()
        coord._park_queued_for_download_slot.assert_not_awaited()
        assert str(fuse).replace("\\", "/") in caplog.text
    finally:
        store.close()


@pytest.mark.anyio
async def test_queued_foreign_existing_entry_fails_closed(tmp_path: Path):
    """A non-fuse entry outside every known root is not adopted/resumed."""
    store = StateStore(tmp_path / "state.db")
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    h = "j" * 40
    foreign = "/etc/foreign"
    store.upsert(TorrentState(
        source_infohash=h, source_name="Pack.2026", state=State.QUEUED,
        cross_seed_blob=b"blob"))

    coord = make_coordinator()
    coord.store = store
    _fuse_cfg(tmp_path, coord)
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[Torrent(
        hash=h, name="Pack.2026", category="racing", save_path=foreign,
        size_bytes=100, state="downloading", progress=0.5)])
    coord._prepare_next_batch = AsyncMock()
    coord._park_queued_for_download_slot = AsyncMock(return_value=False)
    try:
        await coord._do_queued(store.get(h))
        row = store.get(h)
        assert row.state == State.FAILED
        assert row.save_path == ""
        assert foreign in (row.last_error or "")
        coord.dest_client.resume.assert_not_awaited()
        coord._prepare_next_batch.assert_not_awaited()
        coord._park_queued_for_download_slot.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_queued_unknown_roots_keep_legacy_adoption(tmp_path: Path):
    """MagicMock roots are unknowable: preserve legacy adopt/resume."""
    store = StateStore(tmp_path / "state.db")
    h = "l" * 40
    adopted_path = str(tmp_path / "ssd" / "Pack.2026")
    store.upsert(TorrentState(
        source_infohash=h, source_name="Pack.2026", state=State.QUEUED,
        cross_seed_blob=b"blob"))

    coord = make_coordinator()
    coord.store = store
    _fuse_cfg(tmp_path, coord)
    # cfg.dest / cfg.ssd remain MagicMock: roots unknown -> fail-open.
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[Torrent(
        hash=h, name="Pack.2026", category="racing", save_path=adopted_path,
        size_bytes=100, state="downloading", progress=0.5)])
    coord._prepare_next_batch = AsyncMock()
    coord._park_queued_for_download_slot = AsyncMock(return_value=False)
    coord.dest_client.resume = AsyncMock()
    try:
        await coord._do_queued(store.get(h))
        row = store.get(h)
        assert row.state == State.DOWNLOADING
        assert row.save_path == adopted_path
        coord.dest_client.resume.assert_awaited_once_with(h)
        coord._prepare_next_batch.assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_readd_heal_foreign_helper_not_resumed(tmp_path: Path):
    """The healer never resumes a helper entry outside the SSD roots."""
    from racing_sync.watchdir import _bencode, _bencoded_info_hash

    top = "Batch"
    missing = f"{top}/ep04v2.mkv"
    blob = bytes(_bencode({
        b"info": {
            b"name": top.encode(),
            b"files": [
                {b"length": 100, b"path": [b"ep01.mkv"]},
                {b"length": 200, b"path": [b"ep04v2.mkv"]},
            ],
        },
    }))
    blob_hash, _, _, _ = _bencoded_info_hash(blob)

    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    ssd.mkdir()
    (fuse / top).mkdir(parents=True)
    (fuse / top / "ep01.mkv").write_bytes(b"x" * 100)
    h = "k" * 40

    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash=h, source_name="Show.Batch",
        dest_infohash=blob_hash, save_path=str(ssd),
        cross_seed_blob=blob, state=State.RE_ADDING))
    coord = make_coordinator()
    coord.store = store
    coord._stop = False
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.rclone.fuse.mount = fuse
    coord.cfg.rclone.fuse.mount_unsorted = fuse
    coord.cfg.fuse_readd_heal_after_seconds = 7200
    coord._readd_heal_track = {
        h: [time.monotonic() - 10800, (missing,), 0],
    }
    coord._ssd_try_reserve = AsyncMock(return_value=True)
    coord._ssd_release = AsyncMock()
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[Torrent(
        hash=blob_hash, name="Show.Batch", category="racing",
        save_path=str(fuse), size_bytes=300, state="downloading",
        progress=0.5)])
    try:
        healed = await coord._maybe_heal_readd_missing(
            store.get(h), [missing], fuse, blob)
        assert healed is False
        coord.dest_client.set_file_priorities.assert_not_awaited()
        coord.dest_client.resume.assert_not_awaited()
        coord._ssd_release.assert_awaited_once()
    finally:
        store.close()
