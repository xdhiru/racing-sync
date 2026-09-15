from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import AppConfig
from racing_sync.classifier import classify
from racing_sync.clients.abstract import TorrentFile
from racing_sync.coordinator import Coordinator


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


@pytest.mark.anyio
async def test_do_queued_deletes_torrent_when_oversize_movie_skipped(tmp_path: Path):
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State
    from racing_sync.clients.abstract import AddResult

    cfg = _cfg()
    cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord = object.__new__(Coordinator)
    coord.cfg = cfg
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents.return_value = []
    coord.dest_client.add_torrent.return_value = AddResult(hash="moviehash", accepted=True)
    coord._await_hash_for_name = AsyncMock(return_value="moviehash")

    # 200 GB movie (exceeds threshold)
    coord.dest_client.get_torrent_files.return_value = [
        TorrentFile("BigMovie.2024.1080p.mkv", 200_000_000_000),
    ]

    ts = TorrentState(
        source_infohash="moviehash",
        source_name="BigMovie.2024.1080p",
        save_path=str(tmp_path),
        _blob=b"fake-torrent-blob",
    )
    coord.transition = lambda t, s, error=None: setattr(t, "state", s)

    await coord._do_queued(ts)

    assert ts.state == State.FAILED
    coord.dest_client.delete.assert_awaited_once_with("moviehash", delete_files=True)


@pytest.mark.anyio
async def test_do_moving_moves_mixed_content_in_batches(tmp_path: Path):
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State

    cfg = _cfg()
    cfg.dest.save_path = str(tmp_path)
    cfg.ssd.path = tmp_path
    coord = object.__new__(Coordinator)
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.dest_client = AsyncMock()
    coord._rclone_move = AsyncMock()
    coord.transition = MagicMock()
    coord._season_folder_for = MagicMock(return_value=None)

    # Create files on disk for mixed content: 2 episodes + 2 non-episode files
    # (< 90% episodes -> mixed)
    f1 = tmp_path / "Show.S01E01.mkv"
    f2 = tmp_path / "Show.S01E02.mkv"
    f3 = tmp_path / "Extra1.mp4"
    f4 = tmp_path / "Extra2.mp4"
    for f in (f1, f2, f3, f4):
        f.write_bytes(b"x" * 1000)

    coord.dest_client.get_torrent_files.return_value = [
        TorrentFile("Show.S01E01.mkv", 1000, progress=1.0),
        TorrentFile("Show.S01E02.mkv", 1000, progress=1.0),
        TorrentFile("Extra1.mp4", 1000, progress=1.0),
        TorrentFile("Extra2.mp4", 1000, progress=1.0),
    ]

    ts = TorrentState(
        source_infohash="mixed_hash",
        source_name="Show.S01.Mixed",
        classification_kind="mixed",
        save_path=str(tmp_path),
    )

    await coord._do_moving(ts)

    # _rclone_move must be called with include patterns for the mixed batches
    assert coord._rclone_move.await_count > 0
    coord.transition.assert_called_once_with(ts, State.RE_ADDING)


def test_classifier_config_custom_episode_regex():
    from racing_sync.config import ClassifierConfig

    default_cfg = ClassifierConfig()
    assert default_cfg._episode_re.search("Show.S01E05.mkv") is not None
    assert default_cfg._episode_re.search("Show.1x05.mkv") is None

    custom_cfg = ClassifierConfig(episode_regex=r"(?i)\b\d+x\d+\b")
    assert custom_cfg._episode_re.pattern == r"(?i)\b\d+x\d+\b"
    assert custom_cfg._episode_re.search("Show.1x05.mkv") is not None
    assert custom_cfg._episode_re.search("Show.S01E05.mkv") is None


def test_classify_with_custom_episode_regex():
    cfg = _cfg()
    # Non-standard episode naming: 1x01
    files = [TorrentFile("Anime.Show.1x01.1080p.mkv", 500_000_000)]

    # With default regex, not recognized as episode -> movie
    cls_default = classify(files, cfg)
    assert cls_default.kind == "movie"

    # Override episode_regex to match 1x01
    cfg.classifier.episode_regex = r"(?i)\b\d+x\d+\b"
    assert cfg.is_episode("Anime.Show.1x01.1080p.mkv") is True

    cls_custom = classify(files, cfg)
    assert cls_custom.kind == "episode"
    assert len(cls_custom.episodes) == 1
    assert cls_custom.episodes[0].season == 1 and cls_custom.episodes[0].episode == 1


def test_season_folder_for_security(tmp_path):
    cfg = _cfg()
    cfg.dest.save_path = str(tmp_path / "downloads")
    coord = object.__new__(Coordinator)
    coord.cfg = cfg

    # Valid season folder
    files = [
        TorrentFile("Show.S01/ep1.mkv", 1000),
        TorrentFile("Show.S01/ep2.mkv", 1000),
    ]
    folder = coord._season_folder_for(files, "Show.S01")
    assert folder == (tmp_path / "downloads" / "Show.S01").resolve()

    # Traversal attempt with ..
    evil_files = [
        TorrentFile("../escaped/ep1.mkv", 1000),
        TorrentFile("../escaped/ep2.mkv", 1000),
    ]
    assert coord._season_folder_for(evil_files, "Evil") is None

    # Custom base_path override
    custom_base = tmp_path / "custom_save_path"
    folder_custom = coord._season_folder_for(files, "Show.S01", base_path=custom_base)
    assert folder_custom == (custom_base / "Show.S01").resolve()
