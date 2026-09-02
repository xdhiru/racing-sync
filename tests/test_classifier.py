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