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


@pytest.mark.anyio
async def test_delete_refuses_swapped_file(tmp_path: Path):
    """A replacement swapped in after the scan is never unlinked."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    target = watch_dir / "swap.torrent"
    target.write_bytes(_torrent_bytes("Original.Release"))

    cfg = WatchDirConfig(path=watch_dir, glob="*.torrent",
                         delete_after_pickup=True)
    scanner = WatchDirScanner(cfg, prowlarr=None)
    items = await scanner.scan_once()
    assert len(items) == 1

    # Attacker/operator swaps the file before delete runs.
    target.write_bytes(_torrent_bytes("Replacement.Release", length=999))
    await scanner.delete_picked_up(items[0])
    assert target.exists()
    assert target.stat().st_size != items[0].file_size

    # Untouched file deletes normally (fresh scanner: _seen already
    # holds the first hash, which is correct — it was never picked up).
    target.write_bytes(_torrent_bytes("Original.Release"))
    scanner2 = WatchDirScanner(cfg, prowlarr=None)
    items2 = await scanner2.scan_once()
    assert len(items2) == 1
    await scanner2.delete_picked_up(items2[0])
    assert not target.exists()
