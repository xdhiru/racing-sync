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
    files = [TorrentFile("Harbor.Lights.S01E08.1080p.mkv", 1_900_000_000)]
    cls = classify(files, _cfg())
    assert cls.kind == "episode"
    assert cls.single_file == "Harbor.Lights.S01E08.1080p.mkv"
    assert len(cls.episodes) == 1
    assert cls.episodes[0].season == 1
    assert cls.episodes[0].episode == 8


def test_three_digit_episode_is_classified_as_episode():
    """Long-running dailies (S36E171, S62E010): 3-digit episode numbers are
    still individual episodes and route to unsorted/, not the movie path."""
    files = [TorrentFile(
        "Daily.Cookoff.S36E171.2026.06.10.1080p.AMZN.WEB-DL.DDP2.0.H.264-Raccoon.mkv",
        2_600_000_000,
    )]
    cls = classify(files, _cfg())
    assert cls.kind == "episode"
    assert len(cls.episodes) == 1
    assert cls.episodes[0].season == 36
    assert cls.episodes[0].episode == 171


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


def test_season_folder_for_ignores_file_order(tmp_path):
    """Top dir comes from ALL files, not files[0] (qB order not guaranteed).

    Regression: a root-level extra listed first used to yield None, pushing
    _do_moving into a bare `rclone move <folder>` fallback that strips the
    top dir on the remote (season packs landing flat in the remote root).
    """
    from racing_sync.clients.abstract import TorrentFile

    cfg = _cfg()
    cfg.dest.save_path = str(tmp_path / "downloads")
    coord = object.__new__(Coordinator)
    coord.cfg = cfg

    files = [
        TorrentFile("sample.mkv", 100),
        TorrentFile("Harbor.Lights.S03.1080p/Harbor.Lights.S03E01.mkv", 1000),
        TorrentFile("Harbor.Lights.S03.1080p/Harbor.Lights.S03E02.mkv", 1000),
    ]
    # Not all files share the top dir -> None (conservative), but a pure
    # pack in any order must still resolve.
    assert coord._season_folder_for(files, "Harbor.Lights.S03.1080p") is None

    pack_shuffled = [
        TorrentFile("Harbor.Lights.S03.1080p/Harbor.Lights.S03E02.mkv", 1000),
        TorrentFile("Harbor.Lights.S03.1080p/Harbor.Lights.S03E01.mkv", 1000),
        TorrentFile("Harbor.Lights.S03.1080p/Harbor.Lights.S03E03.mkv", 1000),
    ]
    folder = coord._season_folder_for(pack_shuffled, "Harbor.Lights.S03.1080p")
    assert folder == (tmp_path / "downloads" / "Harbor.Lights.S03.1080p").resolve()


def test_parse_episode_without_digits_returns_none():
    from racing_sync.classifier import parse_episode
    import re
    # Custom regex matching words without digits
    regex = re.compile(r"SPECIAL")
    assert parse_episode("Show.SPECIAL.mkv", regex) is None


def test_multi_file_without_episode_tags_is_movie():
    files = [
        TorrentFile("Movie.Name.2024/Movie.Name.2024.1080p.mkv", 4_000_000_000),
        TorrentFile("Movie.Name.2024/Sample/sample.mkv", 50_000_000),
        TorrentFile("Movie.Name.2024/Movie.Name.2024.nfo", 2_000),
    ]
    cls = classify(files, _cfg())
    assert cls.kind == "movie"
    assert cls.single_file is None
    assert cls.episodes == []


def test_season_with_subtitles_filters_non_video_from_episodes():
    # 5 episodes with corresponding .srt subtitles and an .nfo file
    files = []
    for i in range(1, 6):
        files.append(TorrentFile(f"Show.S01E{i:02d}.1080p.mkv", 1_000_000_000))
        files.append(TorrentFile(f"Show.S01E{i:02d}.1080p.srt", 50_000))
    files.append(TorrentFile("Show.S01.nfo", 2_000))

    cls = classify(files, _cfg())
    assert cls.kind == "season"
    assert len(cls.episodes) == 5
    # Ensure every episode in cls.episodes is the mkv video file, not srt/nfo
    for i, ep in enumerate(cls.episodes, start=1):
        assert ep.season == 1
        assert ep.episode == i
        assert ep.file_name.endswith(".mkv")
        assert ep.size_bytes == 1_000_000_000


def test_classifier_false_positive_prevention():
    from racing_sync.classifier import parse_episode

    # Video resolutions (e.g. 1920x1080 -> 20x10, 3840x2160 -> 40x21, 1280x720 -> 80x72)
    assert parse_episode("Movie.2020.1920x1080.mkv") is None
    assert parse_episode("Movie.2020.3840x2160.mkv") is None
    assert parse_episode("Movie.2020.1280x720.mkv") is None

    # Title words with 'e' followed by digits (e.g. Se7en, Blade2, Drive1997, Scene2)
    assert parse_episode("Se7en.1995.1080p.mkv") is None
    assert parse_episode("Blade2.2002.1080p.mkv") is None
    assert parse_episode("Drive1997.mkv") is None
    assert parse_episode("Scene2.mkv") is None

    # Resolution, codec, and audio channels
    assert parse_episode("Sample.720p.mkv") is None
    assert parse_episode("Film.720p.x264.mkv") is None
    assert parse_episode("Film.1080p.x265.5.1.mkv") is None
    assert parse_episode("Film.2160p.TrueHD.7.1.mkv") is None

    # Ensure movie classification routes correctly to movie, NOT episode
    cfg = _cfg()
    movie_res = [TorrentFile("Movie.2020.1920x1080.mkv", 4_000_000_000)]
    assert classify(movie_res, cfg).kind == "movie"

    movie_se7en = [TorrentFile("Se7en.1995.1080p.BluRay.x264.DTS.5.1.mkv", 10_000_000_000)]
    assert classify(movie_se7en, cfg).kind == "movie"


def test_classifier_episode_delimiter_support():
    from racing_sync.classifier import parse_episode

    # Standard delimited episodes
    assert parse_episode("Show.Name.S01E08.1080p.mkv") == (1, 8)
    assert parse_episode("Show.Name.s01e02.mkv") == (1, 2)
    assert parse_episode("Show.Name.1x08.720p.mkv") == (1, 8)
    assert parse_episode("Show.Name.EP08.mkv") == (1, 8)
    assert parse_episode("Show.Name.E08.mkv") == (1, 8)
    assert parse_episode("Show.Name.Episode.08.mkv") == (1, 8)
    assert parse_episode("Show.Name.Season.1.Episode.2.mkv") == (1, 2)
    assert parse_episode("Show_Name_S02E05.mkv") == (2, 5)
    assert parse_episode("Show Name - [01x08] - Title.mkv") == (1, 8)
    assert parse_episode("S01E01 - Pilot.mkv") == (1, 1)

