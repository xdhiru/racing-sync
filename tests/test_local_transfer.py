"""local_move: verified transfer to a mounted local directory.

The source may only be deleted after the destination copy is complete and
size-verified; conflicts, escapes and a missing mount fail loudly. Re-runs
after a crash finish the job instead of duplicating or failing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.chain import Chain
from racing_sync.config import AppConfig
from racing_sync.config_summary import summarize
from racing_sync.local_transfer import (
    LocalMoveError,
    move_local_to_local,
)


def _write(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _config(tmp_path: Path, *, transfer_path: str | None = None) -> AppConfig:
    nas = transfer_path if transfer_path is not None else (tmp_path / "nas").as_posix()
    text = f"""
[general]

[client]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8080"

[storage]
path = "{(tmp_path / "ssd").as_posix()}"
save_path = "{(tmp_path / "ssd").as_posix()}"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[transfer]
provider = "local_move"
path = "{nas}"

[seed]
path = "{(tmp_path / "fuse").as_posix()}"
"""
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return AppConfig.from_toml(path)


def test_local_move_chain_and_summary(tmp_path: Path):
    cfg = _config(tmp_path)
    chain = Chain.from_config(cfg)

    assert chain.transfer == "local_move"
    assert chain.moves is True
    assert chain.seeds is True

    shape = summarize(cfg, tmp_path / "config.toml")["shape"]
    assert shape["transfer"]["provider"] == "local_move"
    assert shape["transfer"]["remote_default"] == str(tmp_path / "nas")


def test_local_move_needs_an_absolute_disjoint_destination(tmp_path: Path):
    text = tmp_path / "config.toml"
    _config(tmp_path)
    raw = text.read_text(encoding="utf-8")

    text.write_text(
        raw.replace('path = "' + (tmp_path / "nas").as_posix() + '"', 'path = "relative/nas"'),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="absolute"):
        AppConfig.from_toml(text)

    text.write_text(
        raw.replace(
            'path = "' + (tmp_path / "nas").as_posix() + '"',
            'path = "' + (tmp_path / "ssd" / "nas").as_posix() + '"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="overlaps"):
        AppConfig.from_toml(text)

    nas_line = 'path = "' + (tmp_path / "nas").as_posix() + '"'
    text.write_text(
        "\n".join(line for line in raw.splitlines() if line.strip() != nas_line),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"\[transfer\]\.path"):
        AppConfig.from_toml(text)


@pytest.mark.anyio
async def test_moves_the_tree_and_deletes_sources(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    dest.mkdir()
    _write(local / "Show.S01" / "ep01.mkv", b"x" * 10)
    _write(local / "Show.S01" / "ep02.mkv", b"y" * 20)

    res = await move_local_to_local(local, dest)

    assert res.ok
    assert (dest / "Show.S01" / "ep01.mkv").read_bytes() == b"x" * 10
    assert (dest / "Show.S01" / "ep02.mkv").read_bytes() == b"y" * 20
    assert not any(p.is_file() for p in local.rglob("*"))


@pytest.mark.anyio
async def test_files_from_moves_only_listed_paths(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    dest.mkdir()
    _write(local / "a.mkv", b"a")
    _write(local / "b.mkv", b"b")

    await move_local_to_local(local, dest, files_from=["a.mkv"])

    assert (dest / "a.mkv").is_file()
    assert (local / "b.mkv").is_file()
    assert not (dest / "b.mkv").exists()


@pytest.mark.anyio
async def test_rerun_finishes_an_interrupted_move(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    dest.mkdir()
    _write(dest / "a.mkv", b"abc")
    _write(local / "a.mkv", b"abc")

    res = await move_local_to_local(local, dest)

    assert res.ok
    assert not (local / "a.mkv").exists()
    assert (dest / "a.mkv").read_bytes() == b"abc"


@pytest.mark.anyio
async def test_empty_source_with_populated_destination_is_success(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    local.mkdir()
    _write(dest / "a.mkv", b"abc")

    res = await move_local_to_local(local, dest)

    assert res.ok


@pytest.mark.anyio
async def test_size_conflict_keeps_both_copies(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    dest.mkdir()
    _write(local / "a.mkv", b"abc")
    _write(dest / "a.mkv", b"abcdef")

    with pytest.raises(LocalMoveError, match="different size"):
        await move_local_to_local(local, dest)

    assert (local / "a.mkv").read_bytes() == b"abc"
    assert (dest / "a.mkv").read_bytes() == b"abcdef"


@pytest.mark.anyio
async def test_missing_mount_is_refused(tmp_path):
    local = tmp_path / "local"
    _write(local / "a.mkv", b"abc")

    with pytest.raises(LocalMoveError, match="does not exist"):
        await move_local_to_local(local, tmp_path / "unmounted")

    assert (local / "a.mkv").is_file()


@pytest.mark.anyio
async def test_destination_inside_source_is_refused(tmp_path):
    local = tmp_path / "local"
    _write(local / "a.mkv", b"abc")

    with pytest.raises(LocalMoveError, match="inside the source"):
        await move_local_to_local(local, local / "nested")


@pytest.mark.anyio
async def test_unsafe_paths_are_refused(tmp_path):
    local = tmp_path / "local"
    dest = tmp_path / "nas"
    dest.mkdir()
    _write(local / "a.mkv", b"abc")

    with pytest.raises(LocalMoveError, match="unsafe|empty"):
        await move_local_to_local(local, dest, files_from=["../escape.mkv"])
    with pytest.raises(LocalMoveError, match="empty"):
        await move_local_to_local(local, dest, files_from=[])


@pytest.mark.anyio
async def test_the_mover_uses_the_local_provider(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock, MagicMock

    from conftest import make_coordinator

    from racing_sync.local_transfer import LocalMoveResult
    from racing_sync.state import TorrentState

    cfg = _config(tmp_path)
    coord = make_coordinator(cfg=cfg)
    coord._stop = False
    coord._schedule_telegram_update = MagicMock()
    seen = {}

    async def fake_local(local, dest_dir, *, files_from=None, timeout=None):  # noqa: ASYNC109
        seen["args"] = (Path(local), Path(dest_dir), list(files_from or []))
        return LocalMoveResult(0)

    rclone = AsyncMock()
    monkeypatch.setattr("racing_sync.transfer.move_local_to_local", fake_local)
    monkeypatch.setattr("racing_sync.transfer.move_local_to_remote", rclone)
    local = tmp_path / "local"
    _write(local / "a.mkv", b"abc")
    ts = TorrentState(source_infohash="f" * 40, source_name="Local.Move")

    await coord._rclone_move(local, str(tmp_path / "nas"), ts, files_from=["a.mkv"])

    assert seen["args"][0] == local
    assert seen["args"][1] == tmp_path / "nas"
    assert seen["args"][2] == ["a.mkv"]
    rclone.assert_not_called()
