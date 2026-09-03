from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import AppConfig
from racing_sync.classifier import classify
from racing_sync.clients.abstract import TorrentFile


def _cfg() -> AppConfig:
    return AppConfig.from_toml(
        Path(__file__).parent.parent / "config.example.toml"
    )


def test_single_file_is_movie():
    files = [TorrentFile("Movie.2024.1080p.mkv", 5_000_000_000)]
    cls = classify(files, _cfg())
    assert cls.kind == "movie"
    assert cls.single_file == "Movie.2024.1080p.mkv"


def test_season_with_episodes():
    files = [
        TorrentFile(f"Show.Name.S01E{i:02d}.1080p.WEB.mkv", 2_000_000_000)
        for i in range(1, 11)
    ]
    cls = classify(files, _cfg())
    assert cls.kind == "season"
    assert len(cls.episodes) == 10
    assert cls.episodes[0].season == 1 and cls.episodes[0].episode == 1
    assert cls.episodes[-1].episode == 10


def test_mixed_classification_rare_case():
    # 3 episode-tagged files + 1 non-tagged sample/extras file.
    # With ceiling rounding (>= 90%), this is treated as mixed.
    files = [
        TorrentFile("S01E01.mkv", 1_000_000_000),
        TorrentFile("S01E02.mkv", 1_000_000_000),
        TorrentFile("S01E03.mkv", 1_000_000_000),
        TorrentFile("sample.mkv", 100_000_000),
    ]
    cls = classify(files, _cfg())
    assert cls.kind == "mixed"


def test_season_with_one_extras_is_still_season():
    files = (
        [TorrentFile(f"S01E{i:02d}.mkv", 2_000_000_000) for i in range(1, 10)]
        + [TorrentFile("sample.mkv", 100_000_000)]
    )
    cls = classify(files, _cfg())
    assert cls.kind == "season"


def test_parse_episode_case_insensitive():
    from racing_sync.classifier import parse_episode
    assert parse_episode("Show.s10e22.mkv") == (10, 22)
    assert parse_episode("Show.S01E01.mkv") == (1, 1)
    assert parse_episode("no.episode.tag.mkv") is None


def test_single_file_episode_is_classified_as_episode():
    files = [TorrentFile("Game.Day.Murders.S01E08.1080p.mkv", 1_900_000_000)]
    cls = classify(files, _cfg())
    assert cls.kind == "episode"
    assert cls.single_file == "Game.Day.Murders.S01E08.1080p.mkv"
    assert len(cls.episodes) == 1
    assert cls.episodes[0].season == 1
    assert cls.episodes[0].episode == 8


def test_single_episode_with_nfo_is_classified_as_episode():
    files = [
        TorrentFile("Show.S02E05.1080p.mkv", 2_000_000_000),
        TorrentFile("Show.S02E05.nfo", 2000),
    ]
    cls = classify(files, _cfg())
    assert cls.kind == "episode"
    assert cls.single_file == "Show.S02E05.1080p.mkv"
    assert len(cls.episodes) == 1
    assert cls.episodes[0].season == 2
    assert cls.episodes[0].episode == 5


def test_coordinator_target_mount_routing():
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState

    cfg = _cfg()
    # Mock Coordinator with minimal fields to test _target_mount_for
    coord = object.__new__(Coordinator)
    coord.cfg = cfg

    ts_movie = TorrentState("hash1", classification_kind="movie")
    ts_season = TorrentState("hash2", classification_kind="season")
    ts_ep = TorrentState("hash3", classification_kind="episode")
    ts_mixed = TorrentState("hash4", classification_kind="mixed")

    # Movies and full seasons -> default mount
    assert coord._target_mount_for(ts_movie) == Path(cfg.rclone.fuse.mount)
    assert coord._target_mount_for(ts_season) == Path(cfg.rclone.fuse.mount)

    # Individual episodes and mixed -> mount_unsorted
    assert coord._target_mount_for(ts_ep) == Path(cfg.rclone.fuse.mount_unsorted)
    assert coord._target_mount_for(ts_mixed) == Path(cfg.rclone.fuse.mount_unsorted)


@pytest.mark.anyio
async def test_do_moving_raises_if_season_folder_missing(tmp_path: Path):
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState

    cfg = _cfg()
    coord = object.__new__(Coordinator)
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files.return_value = [
        TorrentFile("Show.S01E01.mkv", 1000),
        TorrentFile("Show.S01E02.mkv", 1000),
    ]
    coord.dest_client.get_torrent.return_value = None

    ts = TorrentState(
        source_infohash="hash1",
        source_name="Show.S01.1080p",
        classification_kind="season",
        save_path=str(tmp_path),
    )

    with pytest.raises(FileNotFoundError, match="completed season content not found"):
        await coord._do_moving(ts)