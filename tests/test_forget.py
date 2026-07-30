from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.forget import forget_torrent, resolve_row
from racing_sync.state import State, StateStore, TorrentState


@pytest.fixture
def anyio_backend():
    return "asyncio"


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
