"""Watch-dir mixed-case sweep: Show.Torrent must be found on Linux."""
from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import WatchDirConfig
from racing_sync.watchdir import WatchDirScanner, _bencode


def _torrent_bytes(name="Mixed.Case.Release", length=1000):
    return _bencode({
        b"announce": b"http://tracker.example.com/announce",
        b"info": {
            b"name": name.encode(),
            b"length": length,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })


@pytest.mark.anyio
async def test_mixed_case_torrent_found(tmp_path: Path):
    """Regression: entries.add ran before entries=set() (NameError per
    file, swallowed) so mixed-case drops were silently missed."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "Show.Torrent").write_bytes(_torrent_bytes())
    (watch_dir / "lower.torrent").write_bytes(
        _torrent_bytes("Lower.Release"))
    (watch_dir / "UPPER.TORRENT").write_bytes(
        _torrent_bytes("Upper.Release"))

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent",
                         delete_after_pickup=False)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    items = await scanner.scan_once()
    assert sorted(i.name for i in items) == [
        "Lower.Release", "Mixed.Case.Release", "Upper.Release",
    ]
