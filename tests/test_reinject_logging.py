"""Re-inject diagnostics: real conflicts are loud, neighbours are not.

A box with hundreds of racing torrents compares every title against the
row's size; logging each unrelated one buried the real signal (2,284
lines in a day). Only same-release size conflicts are worth a line.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig
from racing_sync.state import TorrentState

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _torrent(hash_char: str, name: str, size: int) -> Torrent:
    return Torrent(
        hash=hash_char * 40,
        name=name,
        category="racing",
        save_path="/downloads",
        size_bytes=size,
        state="seeding",
        progress=1.0,
        trackers=[],
    )


def _coord():
    coord = make_coordinator(cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
    coord.store = MagicMock()
    coord.prowlarr = None
    return coord


@pytest.mark.anyio
async def test_only_same_release_size_conflicts_are_logged(caplog):
    coord = _coord()
    coord._list_source_torrents = AsyncMock(
        return_value=[
            _torrent("1", "Other.Show.S01.1080p", 10),
            _torrent("2", "Show.S01", 50),
        ]
    )
    ts = TorrentState(source_infohash="a" * 40, source_name="Show.S01", total_bytes=100)

    with caplog.at_level(logging.DEBUG):
        await coord._re_inject_racing_torrents(ts)

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "size mismatch for Show.S01" in text
    assert "Other.Show" not in text
    assert "skipped 1 unrelated racing torrent" in text


@pytest.mark.anyio
async def test_uninjectable_seed_escalates_once(caplog):
    coord = _coord()
    ts = TorrentState(source_infohash="b" * 40, source_name="Stuck.Seed")
    ts.source_infohash = "b" * 40

    with caplog.at_level(logging.ERROR):
        for _ in range(23):
            coord._note_seed_uninjectable(ts, ("b" * 40))
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        coord._note_seed_uninjectable(ts, ("b" * 40))

    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "no .torrent bytes" in errors[0]
    assert "re-add its .torrent" in errors[0]
