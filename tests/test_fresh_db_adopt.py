"""End-to-end repro: fresh state.db + SSD-complete public torrent on VPS2.

VPS1 (racing): 1 public torrent + 3 private cross-seeds, same content/name.
VPS2 (dest):   the public torrent 100% complete on SSD, never moved to remote.

Expected: adopt -> move SSD file to remote/fuse -> delete SSD entry ->
re-inject public + privates at fuse -> DONE. The SSD torrent must NOT be
left behind while fuse injections happen.
"""

from __future__ import annotations

import shutil
from pathlib import Path, PurePath
from unittest.mock import MagicMock

import pytest

from racing_sync.clients.abstract import AddResult, Torrent
from racing_sync.config import AppConfig  # noqa: F401  (spec reference)
from racing_sync.coordinator import Coordinator
from racing_sync.recovery import reconcile
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.watchdir import _bencode, _bencoded_info_hash, extract_torrent_files_from_bencoded

PUB_ANNOUNCE = "http://tracker.opentrackr.org/announce"
PRIV_ANNOUNCES = [
    "https://alpha.cc/announce/aaaa",
    "https://beta.me/announce/bbbb",
    "https://privatetracker\.example\.net/announce/cccc",
]
FNAME = "[DummySub] Rising Troupe - Sunflower Parade - 11 (1080p) [C571A56E].mkv"
FSIZE = 5_000_000


def _mk_blob(announce: str) -> bytes:
    # NOTE: infohash covers ONLY the info dict (not announce), so same
    # file/size/pieces => same hash. Real cross-seeds differ (piece size,
    # padding, `source` tag), giving distinct hashes for identical filenames.
    # Embed the tracker in `info.source` so public+privates are distinct
    # hashes sharing one filename — the reported incident shape.
    return _bencode({
        b"announce": announce.encode(),
        b"info": {
            b"name": FNAME.encode(),
            b"length": FSIZE,
            b"piece length": 262144,
            b"pieces": b"12345678901234567890",
            b"source": announce.encode(),
        },
    })


class _FakeSource:
    """VPS1 racing client: public + 3 privates, same content."""

    def __init__(self, blobs: list[bytes], announces: list[str]):
        self.torrents = []
        self.blobs = {}
        for blob, ann in zip(blobs, announces, strict=True):
            ih, name, size, _ = _bencoded_info_hash(blob)
            self.torrents.append(Torrent(
                hash=ih.lower(), name=name, category="", save_path="",
                size_bytes=size, state="seeding", progress=1.0,
                trackers=[ann], added_on=0,
            ))
            self.blobs[ih.lower()] = blob

    async def list_torrents(self, *, category=None, hashes=None):
        out = list(self.torrents)
        if hashes is not None:
            want = {h.lower() for h in hashes}
            out = [t for t in out if t.hash.lower() in want]
        return out

    async def get_torrent(self, h: str):
        for t in self.torrents:
            if t.hash.lower() == h.lower():
                return t
        return None

    async def export_torrent(self, h: str) -> bytes:
        return self.blobs[h.lower()]


class _FakeDest:
    """VPS2 long-term client with real duplicate/hash semantics."""

    def __init__(self):
        self.entries: dict[str, dict] = {}
        self.events: list[tuple] = []

    def seed(self, blob: bytes, save_path: str, category: str, progress: float = 1.0):
        ih, name, size, _ = _bencoded_info_hash(blob)
        files = extract_torrent_files_from_bencoded(blob)
        self.entries[ih.lower()] = {
            "name": name, "save_path": save_path, "category": category,
            "files": files, "size": size, "progress": progress,
            "paused": True, "blob": blob,
        }
        return ih.lower()

    def _row(self, h: str) -> Torrent:
        e = self.entries[h.lower()]
        t = Torrent(
            hash=h.lower(), name=e["name"], category=e["category"],
            save_path=e["save_path"], size_bytes=e["size"],
            state=("seeding" if e["progress"] >= 1.0 else "downloading"),
            progress=e["progress"], trackers=[],
        )
        t.files = list(e["files"])
        return t

    async def list_torrents(self, *, category=None, hashes=None):
        out = []
        for h, e in self.entries.items():
            if category is not None and e["category"] != category:
                continue
            if hashes is not None and h not in {x.lower() for x in hashes}:
                continue
            out.append(self._row(h))
        return out

    async def get_torrent(self, h: str):
        if h.lower() not in self.entries:
            return None
        return self._row(h)

    async def get_torrent_files(self, h: str):
        return list(self.entries[h.lower()]["files"])

    async def get_trackers(self, h: str):
        return []

    async def add_torrent(self, *, urls=None, torrent_files=None, save_path,
                          category="", paused=True, skip_check=False,
                          content_layout=None, tags=None):
        blob = torrent_files[0]
        ih, name, size, _ = _bencoded_info_hash(blob)
        ih = ih.lower()
        self.events.append(("add", ih, save_path))
        if ih in self.entries:
            return AddResult(hash=ih, accepted=False, detail="Fails.")
        files = extract_torrent_files_from_bencoded(blob)
        self.entries[ih] = {
            "name": name, "save_path": save_path, "category": category,
            "files": files, "size": size,
            "progress": 1.0 if skip_check else 0.0,
            "paused": paused, "blob": blob,
        }
        return AddResult(hash=None, accepted=True, detail="Ok.")

    async def set_file_priorities(self, h, priorities):
        return None

    async def pause(self, h: str):
        self.entries[h.lower()]["paused"] = True

    async def resume(self, h: str):
        # Like real qB: hash-check on resume; complete iff files are on disk.
        e = self.entries[h.lower()]
        e["paused"] = False
        ok = True
        for f in e["files"]:
            p = Path(e["save_path"]) / f.name
            try:
                if not (p.exists() and p.stat().st_size == f.size_bytes):
                    ok = False
                    break
            except OSError:
                ok = False
                break
        e["progress"] = 1.0 if ok else 0.0

    async def delete(self, h: str, *, delete_files: bool = False):
        e = self.entries.pop(h.lower(), None)
        if e and delete_files:
            for f in e["files"]:
                try:
                    (Path(e["save_path"]) / f.name).unlink()
                except OSError:
                    pass

    async def export_torrent(self, h: str) -> bytes:
        return self.entries[h.lower()]["blob"]


def _rclone_fake(ssd: Path, fuse: Path, calls: list):
    """Faithful rclone move stand-in: honors --include, preserves rel paths."""

    async def _move(local, remote, ts, *, include=None, extra=None):
        calls.append(("rclone", str(local), include))
        local = Path(local)
        if local.is_file():
            targets = [(local, Path(local.name))]
        else:
            pats = []
            for inc in include or []:
                p = inc.split("=", 1)[1] if "=" in inc else inc
                pats.append(p.replace("\\[", "[").replace("\\]", "]"))
            targets = []
            for src_file in sorted(local.rglob("*")):
                if not src_file.is_file():
                    continue
                rel = src_file.relative_to(local).as_posix()
                if not pats or any(PurePath(rel).match(p) for p in pats):
                    targets.append((src_file, Path(rel)))
        for src_file, rel in targets:
            dst = fuse / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst)
            src_file.unlink()

    return _move


def _make_coord(ssd: Path, fuse: Path, store: StateStore, src, dest) -> Coordinator:
    coord = object.__new__(Coordinator)
    cfg = MagicMock()
    cfg.source.category = ""
    cfg.source.min_age_seconds = 0
    cfg.dest.save_path = ssd
    cfg.ssd.path = ssd
    cfg.ssd.max_inflight_bytes = 100_000_000_000
    cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    cfg.general.disk_safety_margin_bytes = 0
    cfg.general.download_stall_timeout_seconds = 0
    cfg.general.dest_poll_interval = 0
    cfg.cross_seed.allow_ssh_export = False
    cfg.cross_seed.allow_prowlarr_cross_seed = False
    cfg.cross_seed.inject_racing_torrents_to_fuse = True
    cfg.cross_seed.refetch_public_via_prowlarr = False
    cfg.prowlarr.enabled = False
    cfg.fuse_reinject_delay_seconds = 0
    cfg.fuse_reinject_max_age_seconds = 86400
    cfg.fuse_reinject_retry_gap_seconds = 120
    cfg.fuse_reinject_backoff_seconds = 1800
    cfg.rclone.remote.default = "remote:media"
    cfg.rclone.remote.unsorted = "remote:unsorted"
    cfg.rclone.fuse.mount = fuse
    cfg.rclone.fuse.mount_unsorted = fuse
    cfg.rclone.batch_move_extra_flags = []
    cfg.max_active_downloads = 3
    cfg.max_concurrent_moves = 2
    coord.cfg = cfg
    coord.store = store
    coord.source_client = src
    coord.dest_client = dest
    coord.sftp = None
    coord.prowlarr = None
    coord.watch = None
    coord._stop = False
    coord._live = {}
    coord._tasks = set()
    coord._running_infohashes = set()
    coord._source_torrents_cache = []
    coord._source_torrents_cached_at = 0.0
    coord._failed_late_cross_seeds = {}
    return coord


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_fresh_db_ssd_public_moves_before_injecting(tmp_path: Path):
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    ssd.mkdir()
    fuse.mkdir()
    (ssd / FNAME).write_bytes(b"D" * FSIZE)

    pub_blob = _mk_blob(PUB_ANNOUNCE)
    priv_blobs = [_mk_blob(a) for a in PRIV_ANNOUNCES]
    pub_hash = _bencoded_info_hash(pub_blob)[0].lower()
    priv_hashes = [_bencoded_info_hash(b)[0].lower() for b in priv_blobs]

    src = _FakeSource([pub_blob, *priv_blobs], [PUB_ANNOUNCE, *PRIV_ANNOUNCES])
    dest = _FakeDest()
    dest.seed(pub_blob, str(ssd), "racing", progress=1.0)  # previous run, SSD-complete

    store = StateStore(tmp_path / "state.db")  # fresh
    coord = _make_coord(ssd, fuse, store, src, dest)
    coord._rclone_move = _rclone_fake(ssd, fuse, dest.events)

    # 1. Startup recovery, as run() does.
    await reconcile(coord.cfg, dest=dest, store=store)
    rows = store.all()
    assert len(rows) == 1
    assert rows[0].state == State.MOVING

    # 2. Worker drives the adopted row to DONE.
    await coord._process_torrent_inner(store.get(rows[0].source_infohash))
    row = store.get(rows[0].source_infohash)

    # 3. The SSD file must have been MOVED (not left behind)...
    rclone_evts = [e for e in dest.events if e[0] == "rclone"]
    assert rclone_evts, "rclone move never ran before injection"
    assert (fuse / FNAME).exists()
    assert (fuse / FNAME).stat().st_size == FSIZE
    assert not (ssd / FNAME).exists()
    # ...the SSD entry deleted...
    assert pub_hash not in dest.entries or dest.entries[pub_hash]["save_path"] == str(fuse)
    # ...all four hashes injected at fuse, strictly AFTER the move...
    fuse_adds = [e for e in dest.events if e[0] == "add" and e[2] == str(fuse)]
    assert {e[1] for e in fuse_adds} == {pub_hash, *priv_hashes}
    last_rclone_idx = max(dest.events.index(e) for e in rclone_evts)
    first_add_idx = min(dest.events.index(e) for e in fuse_adds)
    assert last_rclone_idx < first_add_idx, "fuse injection happened before the rclone move"
    # ...and the row is DONE with everything recorded.
    assert row.state == State.DONE
    assert {h for h in row.injected_private_hashes.split(",") if h} == {pub_hash, *priv_hashes}


@pytest.mark.anyio
async def test_fresh_db_unadopted_ssd_public_moves_before_injecting(tmp_path: Path):
    """Same, but the SSD torrent is invisible to recovery (no category)."""
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    ssd.mkdir()
    fuse.mkdir()
    (ssd / FNAME).write_bytes(b"D" * FSIZE)

    pub_blob = _mk_blob(PUB_ANNOUNCE)
    priv_blobs = [_mk_blob(a) for a in PRIV_ANNOUNCES]
    pub_hash = _bencoded_info_hash(pub_blob)[0].lower()
    priv_hashes = [_bencoded_info_hash(b)[0].lower() for b in priv_blobs]

    src = _FakeSource([pub_blob, *priv_blobs], [PUB_ANNOUNCE, *PRIV_ANNOUNCES])
    dest = _FakeDest()
    dest.seed(pub_blob, str(ssd), "", progress=1.0)  # no category -> not adopted

    store = StateStore(tmp_path / "state.db")
    coord = _make_coord(ssd, fuse, store, src, dest)
    coord._rclone_move = _rclone_fake(ssd, fuse, dest.events)

    await reconcile(coord.cfg, dest=dest, store=store)
    assert store.all() == []

    # Mimic _tick discovery of the public primary.
    ts = TorrentState(source_infohash=pub_hash, source_name=FNAME,
                      total_bytes=FSIZE, state=State.NEW)
    store.upsert(ts)
    await coord._process_torrent_inner(store.get(pub_hash))
    row = store.get(pub_hash)

    rclone_evts = [e for e in dest.events if e[0] == "rclone"]
    assert rclone_evts, "rclone move never ran before injection"
    assert (fuse / FNAME).exists()
    assert not (ssd / FNAME).exists()
    fuse_adds = [e for e in dest.events if e[0] == "add" and e[2] == str(fuse)]
    assert {e[1] for e in fuse_adds} == {pub_hash, *priv_hashes}
    last_rclone_idx = max(dest.events.index(e) for e in rclone_evts)
    first_add_idx = min(dest.events.index(e) for e in fuse_adds)
    assert last_rclone_idx < first_add_idx, "fuse injection happened before the rclone move"
    assert row.state == State.DONE


@pytest.mark.anyio
async def test_false_done_with_ssd_save_path_demotes_and_moves_before_injecting(tmp_path: Path):
    """Regression for the reported incident: falsely adopted DONE must not inject.

    Fresh-DB recovery (or a pre-fix DB) may hold a DONE row whose save_path
    still points at SSD — the rclone move never ran. Late cross-seeds from
    VPS1 must defer (no fuse adds), the row must demote DONE->MOVING, and the
    subsequent worker must move first and only then inject all four hashes.
    """
    ssd = tmp_path / "ssd"
    fuse = tmp_path / "fuse"
    ssd.mkdir()
    fuse.mkdir()
    (ssd / FNAME).write_bytes(b"D" * FSIZE)

    pub_blob = _mk_blob(PUB_ANNOUNCE)
    priv_blobs = [_mk_blob(a) for a in PRIV_ANNOUNCES]
    pub_hash = _bencoded_info_hash(pub_blob)[0].lower()
    priv_hashes = [_bencoded_info_hash(b)[0].lower() for b in priv_blobs]

    src = _FakeSource([pub_blob, *priv_blobs], [PUB_ANNOUNCE, *PRIV_ANNOUNCES])
    dest = _FakeDest()
    dest.seed(pub_blob, str(ssd), "racing", progress=1.0)

    store = StateStore(tmp_path / "state.db")
    coord = _make_coord(ssd, fuse, store, src, dest)
    coord._rclone_move = _rclone_fake(ssd, fuse, dest.events)

    # Pre-fix DONE row pointing at SSD (move never ran).
    ts = TorrentState(
        source_infohash=pub_hash, source_name=FNAME, dest_infohash=pub_hash,
        save_path=str(ssd), total_bytes=FSIZE, state=State.DONE,
    )
    store.upsert(ts)

    group = list(src.torrents)
    assert len(group) == 4

    # 1. Late tick must NOT inject; must demote to MOVING.
    await coord._check_and_inject_late_cross_seeds(store.get(pub_hash), group)
    row = store.get(pub_hash)
    assert row.state == State.MOVING, "false DONE must demote to MOVING before any injection"
    assert row.save_path == str(ssd)
    assert [e for e in dest.events if e[0] == "add"] == []
    assert (ssd / FNAME).exists()
    assert not (fuse / FNAME).exists()

    # 2. Worker moves first, then injects all four at fuse.
    await coord._process_torrent_inner(store.get(pub_hash))
    row = store.get(pub_hash)
    rclone_evts = [e for e in dest.events if e[0] == "rclone"]
    assert rclone_evts, "rclone move never ran after demotion"
    assert (fuse / FNAME).exists()
    assert not (ssd / FNAME).exists()
    fuse_adds = [e for e in dest.events if e[0] == "add" and e[2] == str(fuse)]
    assert {e[1] for e in fuse_adds} == {pub_hash, *priv_hashes}
    assert max(dest.events.index(e) for e in rclone_evts) < min(dest.events.index(e) for e in fuse_adds)
    assert row.state == State.DONE
