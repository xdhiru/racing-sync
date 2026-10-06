"""Watch-row identity cache + watchdir loop offload + bad-file cap."""
from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from racing_sync import coordinator_content as _cc
from racing_sync.state import TorrentState


def _cfg(tmp_path: Path):
    db = tmp_path / "state.db"
    db.write_bytes(b"")
    return SimpleNamespace(general=SimpleNamespace(state_db=db))


def _row(h: str, source: str = ""):
    return TorrentState(source_infohash=h, source_name="X",
                        cross_seed_source=source)


@pytest.mark.anyio
async def test_is_watch_row_case_insensitive_and_cached(tmp_path: Path):
    """Uppercase hashes match the lowercase blob dir; second hit is cached."""
    _cc._WATCH_ROW_CACHE.clear()
    cfg = _cfg(tmp_path)
    blob_dir = (tmp_path / "watch_cross_seeds" / ("a" * 40))
    blob_dir.mkdir(parents=True)
    (blob_dir / "x.torrent").write_bytes(b"d1:xe")
    try:
        assert _cc.is_watch_row(_row("A" * 40), cfg) is True
        key = next(iter(_cc._WATCH_ROW_CACHE))
        first = _cc._WATCH_ROW_CACHE[key]
        assert _cc.is_watch_row(_row("A" * 40), cfg) is True
        # Cache hit: same record, no fresh stat.
        assert _cc._WATCH_ROW_CACHE[key] == first
    finally:
        _cc._WATCH_ROW_CACHE.clear()


@pytest.mark.anyio
async def test_is_watch_row_negative(tmp_path: Path):
    _cc._WATCH_ROW_CACHE.clear()
    try:
        assert _cc.is_watch_row(_row("b" * 40), _cfg(tmp_path)) is False
        assert _cc.is_watch_row(_row(""), _cfg(tmp_path)) is False
    finally:
        _cc._WATCH_ROW_CACHE.clear()


@pytest.mark.anyio
async def test_bad_files_capped(tmp_path: Path):
    """600 broken drops must not grow _bad_files without bound."""
    import asyncio as _asyncio

    from racing_sync.config import WatchDirConfig
    from racing_sync.watchdir import WatchDirScanner

    watch = tmp_path / "watch"
    watch.mkdir()
    old = time.time() - 100
    for i in range(600):
        p = watch / f"bad{i:03d}.torrent"
        p.write_bytes(b"not a torrent {{{" + str(i).encode())
        os.utime(p, (old, old))
    cfg = WatchDirConfig(path=watch, glob="*.torrent",
                         delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    # Silence per-file warnings for speed of assertion, not behavior.
    items = await scanner.scan_once()
    assert items == []
    assert len(scanner._bad_files) <= scanner._BAD_FILES_MAX
    assert len(scanner._bad_files) > 0
