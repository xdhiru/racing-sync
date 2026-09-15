from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.forget import forget_torrent, resolve_row
from racing_sync.state import State, StateStore, TorrentState


@pytest.mark.anyio
async def test_candidate_local_paths_dedups_top_dir(tmp_path: Path):
    """Two files under the same top dir must yield one local path."""
    from racing_sync.forget import _candidate_local_paths

    ssd = tmp_path / "ssd"
    ssd.mkdir()
    dest = FakeDest()
    dest.seed(
        "a" * 40, str(ssd),
        [TorrentFile(name="Show/ep1.mkv", size_bytes=10),
         TorrentFile(name="Show/ep2.mkv", size_bytes=10)],
    )
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    row = MagicMock()
    row.save_path = str(ssd)

    paths, skipped = await _candidate_local_paths(cfg, dest, row, ["a" * 40])
    assert [str(p) for p in paths] == [str(ssd / "Show")]
    assert skipped == []


class FakeDest:
    """Minimal dest client double backed by real tmp-dir files."""

    def __init__(self) -> None:
        self.entries: dict[str, dict] = {}
        self.delete_calls: list[tuple[str, bool]] = []

    def seed(self, infohash: str, save_path: str, files: list[TorrentFile]) -> None:
        self.entries[infohash.lower()] = {"save_path": save_path, "files": list(files)}

    async def list_torrents(self, *, category=None, hashes=None):
        wanted = {h.lower() for h in hashes} if hashes is not None else None
        out = []
        for h, e in self.entries.items():
            if wanted is not None and h not in wanted:
                continue
            out.append(Torrent(hash=h, name="n", save_path=e["save_path"],
                               category="racing", size_bytes=10,
                               state="downloading", progress=0.5))
        return out

    async def get_torrent(self, h: str):
        h = h.lower()
        if h not in self.entries:
            return None
        e = self.entries[h]
        return Torrent(hash=h, name="n", save_path=e["save_path"],
                       category="racing", size_bytes=10,
                       state="downloading", progress=0.5)

    async def get_torrent_files(self, h: str):
        return list(self.entries[h.lower()]["files"])

    async def delete(self, h: str, *, delete_files: bool = False):
        self.delete_calls.append((h.lower(), delete_files))
        e = self.entries.pop(h.lower(), None)
        if e and delete_files:
            for f in e["files"]:
                p = Path(e["save_path"]) / f.name
                try:
                    if p.is_file():
                        p.unlink()
                except OSError:
                    pass


def _cfg(ssd: Path) -> MagicMock:
    cfg = MagicMock()
    cfg.ssd.path = ssd
    cfg.dest.save_path = ssd
    return cfg


def _row(h: str = "a" * 40, name: str = "Pack.One", **kw) -> TorrentState:
    return TorrentState(source_infohash=h, source_name=name, state=State.MOVING, **kw)


def test_resolve_row_by_hash_case_insensitive(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(dest_infohash="B" * 40))
    assert resolve_row(store, "b" * 40).source_infohash == "a" * 40
    with pytest.raises(LookupError):
        resolve_row(store, "c" * 40)


def test_resolve_row_name_ambiguity(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row("a" * 40, "Show.S01.Pack"))
    store.upsert(_row("b" * 40, "Show.S01.Extras"))
    assert resolve_row(store, "extras").source_infohash == "b" * 40
    with pytest.raises(LookupError, match="matches 2"):
        resolve_row(store, "show.s01")


@pytest.mark.anyio
async def test_forget_dry_run_changes_nothing(tmp_path: Path):
    ssd = tmp_path / "ssd"
    top = ssd / "Pack.One"
    top.mkdir(parents=True)
    (top / "a.mkv").write_bytes(b"x" * 10)
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(ssd)))
    dest = FakeDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Pack.One/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="A" * 40,
        apply=False, delete_files=True,
    )

    assert result["applied"] is False
    assert result["dest_entries"] == ["a" * 40]
    assert result["local_paths"] == [str(top)]
    assert result["errors"] == []
    assert "a" * 40 in dest.entries
    assert (top / "a.mkv").exists()
    assert store.get("a" * 40) is not None


@pytest.mark.anyio
async def test_forget_apply_removes_everything(tmp_path: Path):
    ssd = tmp_path / "ssd"
    top = ssd / "Pack.One"
    top.mkdir(parents=True)
    (top / "a.mkv").write_bytes(b"x" * 10)
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(ssd)))
    dest = FakeDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Pack.One/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="pack.one",
        apply=True, delete_files=True,
    )

    assert result["applied"] is True
    assert result["errors"] == []
    assert dest.delete_calls == [("a" * 40, True)]
    assert not top.exists()
    assert store.get("a" * 40) is None


@pytest.mark.anyio
async def test_forget_keep_files_leaves_data(tmp_path: Path):
    ssd = tmp_path / "ssd"
    top = ssd / "Pack.One"
    top.mkdir(parents=True)
    (top / "a.mkv").write_bytes(b"x" * 10)
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(ssd)))
    dest = FakeDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Pack.One/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=False,
    )

    assert dest.delete_calls == [("a" * 40, False)]
    assert (top / "a.mkv").exists()
    assert store.get("a" * 40) is None
    assert result["errors"] == []


@pytest.mark.anyio
async def test_forget_skips_paths_outside_ssd(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (outside / "Stray").mkdir()
    (outside / "Stray" / "x.mkv").write_bytes(b"x")
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(outside)))
    dest = FakeDest()
    dest.seed("a" * 40, str(outside), [TorrentFile(name="Stray/x.mkv", size_bytes=1)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True,
    )

    # The client removes its own data; our direct wipe stays out of scope.
    assert result["local_paths"] == []
    assert len(result["skipped_paths"]) == 1
    assert not (outside / "Stray" / "x.mkv").exists()
    assert store.get("a" * 40) is None
    assert result["errors"] == []


@pytest.mark.anyio
async def test_forget_keep_files_skips_out_of_scope_wipe(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    outside = tmp_path / "elsewhere"
    (outside / "Stray").mkdir(parents=True)
    (outside / "Stray" / "x.mkv").write_bytes(b"x")
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(outside)))
    dest = FakeDest()
    dest.seed("a" * 40, str(outside), [TorrentFile(name="Stray/x.mkv", size_bytes=1)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=False,
    )

    assert dest.delete_calls == [("a" * 40, False)]
    assert (outside / "Stray" / "x.mkv").exists()
    assert store.get("a" * 40) is None
    assert result["errors"] == []


@pytest.mark.anyio
async def test_forget_missing_client_entry_still_drops_row(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(ssd)))
    dest = FakeDest()

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True,
    )

    assert result["dest_entries"] == []
    assert store.get("a" * 40) is None


def _watch_pair_store(tmp_path: Path):
    """Owner DOWNLOADING + two NEW waiters (same content) + bystanders."""
    from racing_sync.state import StateStore
    store = StateStore(tmp_path / "state.db")

    def _w(h, name="Shared.Show.S01E01", size=1000, state=State.NEW,
           source="watch-dir"):
        ts = TorrentState(source_infohash=h, source_name=name,
                          total_bytes=size, cross_seed_source=source,
                          source_announce_url="https://alpha.cc/announce/xyz",
                          state=state)
        store.upsert(ts)
        return ts

    owner = _w("a" * 40, state=State.DOWNLOADING)
    w1 = _w("b" * 40)
    w2 = _w("c" * 40)
    other = _w("d" * 40, name="Other.Show.S01E01")
    diff_size = _w("e" * 40, size=2000)
    return store, owner, (w1, w2), (other, diff_size)


@pytest.mark.anyio
async def test_forget_cascades_to_waiting_pairs(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store, _owner, waiters, bystanders = _watch_pair_store(tmp_path)
    dest = FakeDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Shared/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True, ignore=True,
    )

    assert store.get("a" * 40) is None
    assert store.get("b" * 40) is None
    assert store.get("c" * 40) is None
    # Bystanders (other content / other size) survive untouched.
    assert store.get("d" * 40) is not None
    assert store.get("e" * 40) is not None
    # ... but everything cancelled is ignored so nothing comes back.
    for h in ("a" * 40, "b" * 40, "c" * 40):
        assert store.is_ignored(h) is True
    assert store.is_ignored("d" * 40) is False
    paired = {p["source_infohash"] for p in result["paired_cancelled"]}
    assert paired == {"b" * 40, "c" * 40}
    assert result["errors"] == []


@pytest.mark.anyio
async def test_forget_dry_run_plans_pairs_without_deleting(tmp_path: Path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store, _owner, _waiters, _bystanders = _watch_pair_store(tmp_path)
    dest = FakeDest()

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=False, delete_files=True, ignore=True,
    )

    assert result["applied"] is False
    assert {p["source_infohash"] for p in result["paired_cancelled"]} == {"b" * 40, "c" * 40}
    assert store.get("a" * 40) is not None
    assert store.get("b" * 40) is not None
    assert store.is_ignored("a" * 40) is False


@pytest.mark.anyio
async def test_forget_waiter_cancels_alone(tmp_path: Path):
    """Cancelling a waiter leaves the owner and its siblings running."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store, _owner, _waiters, _bystanders = _watch_pair_store(tmp_path)
    dest = FakeDest()

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="b" * 40,
        apply=True, delete_files=True, ignore=True,
    )

    assert store.get("b" * 40) is None
    assert store.get("a" * 40) is not None
    assert store.get("c" * 40) is not None
    assert result["paired_cancelled"] == []


@pytest.mark.anyio
async def test_forget_done_owner_cascades_nothing(tmp_path: Path):
    """DONE releases election — a same-content NEW row is independent."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(TorrentState(
        source_infohash="a" * 40, source_name="Shared.Show.S01E01",
        total_bytes=1000, cross_seed_source="watch-dir",
        source_announce_url="https://alpha.cc/announce/xyz",
        state=State.DONE))
    store.upsert(TorrentState(
        source_infohash="b" * 40, source_name="Shared.Show.S01E01",
        total_bytes=1000, cross_seed_source="watch-dir",
        source_announce_url="https://alpha.cc/announce/xyz",
        state=State.NEW))
    dest = FakeDest()

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True, ignore=True,
    )

    assert store.get("b" * 40) is not None
    assert result["paired_cancelled"] == []


@pytest.mark.anyio
async def test_forget_verify_after_delete_reports_survivors(tmp_path: Path):
    """An entry surviving delete is an error, not silent success."""
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    store = StateStore(tmp_path / "state.db")
    store.upsert(_row(save_path=str(ssd)))

    class StubbornDest(FakeDest):
        async def delete(self, h: str, *, delete_files: bool = False):
            self.delete_calls.append((h.lower(), delete_files))
            # Simulates a no-op delete: entry stays listed.

    dest = StubbornDest()
    dest.seed("a" * 40, str(ssd), [TorrentFile(name="Pack.One/a.mkv", size_bytes=10)])

    result = await forget_torrent(
        _cfg(ssd), dest=dest, store=store, target="a" * 40,
        apply=True, delete_files=True,
    )

    assert any("still present after delete" in e for e in result["errors"])
    # The row is kept (not resurrected later) so the operator can retry.
    assert store.get("a" * 40) is not None
    assert any("kept db row" in e for e in result["errors"])


@pytest.mark.anyio
async def test_worker_never_resurrects_forgotten_row(tmp_path: Path):
    """A row deleted mid-flight (forget) must stay deleted.

    Terminal transitions, park funnels and the worker entry all refuse
    to write for a gone row instead of upsert-resurrecting it.
    """
    from conftest import make_coordinator
    from racing_sync.coordinator_errors import AbandonedError

    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row())
        coord = make_coordinator(store)
        store.delete("a" * 40)  # operator forget lands mid-flight
        with pytest.raises(AbandonedError):
            coord.transition(_row(), State.RE_ADDING)
        with pytest.raises(AbandonedError):
            coord._park_moving(_row(), "test park")
        await coord._process_torrent(_row())
        assert store.get("a" * 40) is None
    finally:
        store.close()


def test_resolve_row_rejects_short_hash_and_hash_dupes(tmp_path: Path):
    """1-char fragments never match hashes; dupes error instead of first-wins."""
    store = StateStore(tmp_path / "state.db")
    try:
        store.upsert(_row("a" * 40, "Show.S01.Pack"))
        store.upsert(_row("b" * 40, "Show.S02.Pack", dest_infohash="a" * 40))
        # "a" is a substring of the first row's hash but must not resolve.
        with pytest.raises(LookupError):
            resolve_row(store, "a")
        # Full hash shared by two rows (repack dupe) is ambiguous.
        with pytest.raises(LookupError, match="by hash"):
            resolve_row(store, "a" * 40)
    finally:
        store.close()


def test_is_fuse_save_path_resolves_symlinks(tmp_path: Path):
    """A symlinked save_path pointing at fuse still counts as fuse."""
    from racing_sync.forget import _is_fuse_save_path

    fuse = tmp_path / "fuse"
    fuse.mkdir()
    (fuse / "unsorted").mkdir()
    link = tmp_path / "ssdlink"
    try:
        link.symlink_to(fuse, target_is_directory=True)
    except OSError:
        pytest.skip("symlinks unavailable")
    cfg = MagicMock()
    cfg.rclone.fuse.mount = str(fuse)
    cfg.rclone.fuse.mount_unsorted = str(fuse / "unsorted")
    assert _is_fuse_save_path(cfg, str(link)) is True
    assert _is_fuse_save_path(cfg, str(link / "sub")) is True
    assert _is_fuse_save_path(cfg, str(tmp_path / "ssd")) is False


def test_watch_cross_seed_dir_rejects_hostile_hash(tmp_path: Path):
    """Infohash path joins accept 40-hex only (no traversal, no root)."""
    from racing_sync.coordinator_paths import _watch_cross_seed_dir

    db = tmp_path / "state.db"
    good = _watch_cross_seed_dir(db, "a" * 40)
    assert good == tmp_path / "watch_cross_seeds" / ("a" * 40)
    assert _watch_cross_seed_dir(db, "../../evil") is None
    assert _watch_cross_seed_dir(db, "") is None
    assert _watch_cross_seed_dir(db, "z" * 40) is None
