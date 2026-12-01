"""Single-box ownership: one client serves both roles.

On one box the entries our re-add points at the seed path are finished
work. They must not be re-ingested as fresh racing torrents, and the
janitor must never delete the copy that is seeding.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.chain import Chain
from racing_sync.clients.abstract import Torrent
from racing_sync.config import AppConfig
from racing_sync.state import TorrentState

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _load(name: str) -> AppConfig:
    return AppConfig.from_toml(FIXTURES / f"{name}.toml")


def _torrent(name: str, save_path: str, size: int = 1000) -> Torrent:
    return Torrent(
        hash="c" * 40,
        name=name,
        category="racing",
        save_path=save_path,
        size_bytes=size,
        state="seeding",
        progress=1.0,
        trackers=[],
    )


def test_topology_is_derived_from_the_origins():
    assert Chain.from_config(_load("minimal")).single_box is True
    assert Chain.from_config(_load("split")).single_box is False


def test_default_ports_compare_equal(tmp_path: Path):
    text = (FIXTURES / "minimal.toml").read_text(encoding="utf-8")
    path = tmp_path / "config.toml"
    path.write_text(
        text.replace('host = "http://127.0.0.1:8080"', 'host = "http://box"', 1).replace(
            'host = "http://127.0.0.1:8080"', 'host = "http://box:80"', 1
        ),
        encoding="utf-8",
    )
    assert Chain.from_config(AppConfig.from_toml(path)).single_box is True


def test_different_ports_are_two_boxes(tmp_path: Path):
    text = (FIXTURES / "minimal.toml").read_text(encoding="utf-8")
    path = tmp_path / "config.toml"
    path.write_text(
        text.replace('host = "http://127.0.0.1:8080"', 'host = "http://box:8080"', 1).replace(
            'host = "http://127.0.0.1:8080"', 'host = "http://box:8081"', 1
        ),
        encoding="utf-8",
    )
    assert Chain.from_config(AppConfig.from_toml(path)).single_box is False


@pytest.mark.anyio
async def test_single_box_intake_ignores_seeded_entries():
    cfg = _load("minimal")
    coord = make_coordinator(cfg=cfg)
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = _AsyncList(
        [
            _torrent("Racing.Show", "/downloads/Racing.Show"),
            _torrent("Seeded.Show", "/mnt/fuse/Seeded.Show"),
        ]
    )

    listed = await coord._list_source_torrents_locked(force_refresh=True)

    assert [t.name for t in listed] == ["Racing.Show"]


@pytest.mark.anyio
async def test_split_intake_keeps_both_entries():
    cfg = _load("split")
    coord = make_coordinator(cfg=cfg)
    coord.source_client = MagicMock()
    coord.source_client.list_torrents = _AsyncList(
        [
            _torrent("Racing.Show", "/srv/qbittorrent/data/Racing.Show"),
            _torrent("Seeded.Show", "/mnt/remote/qbittorrent/Seeded.Show"),
        ]
    )

    listed = await coord._list_source_torrents_locked(force_refresh=True)

    assert {t.name for t in listed} == {"Racing.Show", "Seeded.Show"}


def test_cleanup_never_deletes_the_seeding_entry_on_one_box():
    cfg = _load("minimal")
    coord = make_coordinator(cfg=cfg)
    ts = TorrentState(source_infohash="d" * 40, source_name="Show.S01", total_bytes=1000)
    racing = _torrent("Show.S01", "/downloads/Show.S01")
    seeded = _torrent("Show.S01", "/mnt/fuse/Show.S01")

    group = coord._cleanup_group_for(ts, {"r": racing, "s": seeded})

    assert group == [racing]


def test_cleanup_group_unchanged_on_two_boxes():
    cfg = _load("split")
    coord = make_coordinator(cfg=cfg)
    ts = TorrentState(source_infohash="d" * 40, source_name="Show.S01", total_bytes=1000)
    racing = _torrent("Show.S01", "/srv/qbittorrent/data/Show.S01")
    other = _torrent("Show.S01", "/srv/qbittorrent/data/Show.S01.backup")

    group = coord._cleanup_group_for(ts, {"r": racing, "o": other})

    assert sorted(t.save_path for t in group) == sorted([racing.save_path, other.save_path])


class _AsyncList:
    """Tiny awaitable-callable returning a fixed list."""

    def __init__(self, items):
        self._items = items

    async def __call__(self, *args, **kwargs):
        return list(self._items)
