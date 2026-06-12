"""Classify a torrent's files as movie / season / episode.

req #6:
  - Movie or full season folders go to rclone remote default.
  - Individual episodes (matching S00E00 regex) go to remote unsorted.

We also reject single-file torrents (movies) larger than `skip_movie_larger_than_bytes`.
"""

from __future__ import annotations

import functools
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass

from pathlib import Path

from .clients.abstract import TorrentFile
from .config import AppConfig

log = logging.getLogger(__name__)

NON_VIDEO_EXTENSIONS = {
    ".nfo", ".srt", ".sub", ".idx", ".txt", ".jpg", ".jpeg", ".png",
    ".torrent", ".sfv", ".md5", ".sha", ".sha1",
}


@dataclass(slots=True)
class Episode:
    file_name: str            # exact file path inside the torrent
    season: int
    episode: int
    size_bytes: int


@dataclass(slots=True)
class Classification:
    kind: str                 # "movie" | "season" | "episode" | "mixed" | "unknown"
    episodes: list[Episode]   # populated for season / episode / mixed
    single_file: str | None   # populated for movie and episode
    total_bytes: int


EP_RE = re.compile(
    r"[Ss](\d{1,2})[Ee](\d{1,2})"       # S01E08, s1e2
    r"|(\d{1,2})x(\d{1,2})"             # 1x08
    r"|[Ee][Pp]?(\d{1,3})"              # E08, EP08
    r"|(?:\b|_)[Ss]eason\s*(\d{1,2})\s*[Ee]pisode\s*(\d{1,2})"
)


@functools.lru_cache(maxsize=128)
def _compile_pattern(pattern_str: str) -> re.Pattern[str]:
    return re.compile(pattern_str)


@functools.lru_cache(maxsize=2048)
def parse_episode(name: str, regex: re.Pattern[str] | str | None = None) -> tuple[int, int] | None:
    """Return (season, episode) parsed from filename, or None."""
    if regex is None:
        pattern = EP_RE
    elif isinstance(regex, str):
        pattern = _compile_pattern(regex)
    else:
        pattern = regex

    m = pattern.search(name)
    if not m:
        return None

    # If pattern has capture groups, use them
    groups = m.groups()
    if len(groups) >= 2 and groups[0] and groups[1]:
        try:
            return int(groups[0]), int(groups[1])
        except ValueError:
            pass
    elif len(groups) == 1 and groups[0]:
        try:
            return 1, int(groups[0])
        except ValueError:
            pass

    # Otherwise extract all numeric sequences from the matched span (e.g. S01E08 -> [1, 8])
    nums = re.findall(r"\d+", m.group(0))
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    elif len(nums) == 1:
        return 1, int(nums[0])
    return None


def classify(files: Iterable[TorrentFile], cfg: AppConfig) -> Classification:
    """Classify a torrent given its files.

    Routing rules:
      - Individual episodes (single episode matching episode_regex) -> kind='episode'
        (routed to rclone remote unsorted/ and fuse mount_unsorted).
      - Full seasons (multi-episode packs >= 90% episodes) -> kind='season'
        (routed to rclone remote default and fuse mount).
      - Movies (no episode match) -> kind='movie'
        (routed to rclone remote default and fuse mount).
    """
    files = list(files)
    total = sum(f.size_bytes for f in files)

    if not files:
        return Classification(kind="unknown", episodes=[], single_file=None, total_bytes=0)

    ep_regex = getattr(cfg.classifier, "_episode_re", None) or EP_RE

    # Filter out non-video files (.srt, .nfo, etc.) when evaluating episodes
    video_files = [f for f in files if Path(f.name).suffix.lower() not in NON_VIDEO_EXTENSIONS]
    eval_files = video_files if video_files else files

    # Check for episode matches across evaluated files
    eps: list[Episode] = []
    for f in eval_files:
        parsed = parse_episode(f.name, ep_regex)
        if parsed:
            eps.append(Episode(f.name, parsed[0], parsed[1], f.size_bytes))
    eps.sort(key=lambda e: (e.season, e.episode))

    # Deduplicate eps per (season, episode), keeping the largest file
    best_by_ep: dict[tuple[int, int], Episode] = {}
    for ep in eps:
        key = (ep.season, ep.episode)
        if key not in best_by_ep or ep.size_bytes > best_by_ep[key].size_bytes:
            best_by_ep[key] = ep
    deduped_eps = sorted(best_by_ep.values(), key=lambda e: (e.season, e.episode))

    distinct_eps = {(e.season, e.episode) for e in deduped_eps}

    # Case 1: Exactly 1 distinct episode found -> individual episode torrent (routes to unsorted)
    if len(distinct_eps) == 1:
        main_ep = deduped_eps[0]
        return Classification(
            kind="episode",
            episodes=[main_ep],
            single_file=main_ep.file_name,
            total_bytes=total,
        )

    # Case 2: No episodes found at all
    if not deduped_eps:
        # If single file -> movie
        if len(files) == 1:
            f = files[0]
            if f.size_bytes > cfg.ssd.skip_movie_larger_than_bytes:
                log.warning(
                    "movie '%s' (%d B) exceeds skip threshold (%d B)",
                    f.name, f.size_bytes, cfg.ssd.skip_movie_larger_than_bytes,
                )
            return Classification(
                kind="movie", episodes=[], single_file=f.name, total_bytes=total,
            )
        # Multi-file but no episode tag -> treat as movie bundle (default remote)
        return Classification(
            kind="movie", episodes=[], single_file=None, total_bytes=total,
        )

    # Case 3: Multiple distinct episodes found (len(distinct_eps) >= 2) -> full season pack
    # If >= 90% of evaluated files carry an episode tag, treat as a season.
    # Use ceiling: e.g. 4 files where 3 are episodes is still a season.
    if len(deduped_eps) >= max(1, int(-(-len(eval_files) * 9 // 10))):
        return Classification(kind="season", episodes=deduped_eps, single_file=None, total_bytes=total)

    # Mixed (rare): multiple episodes with lots of non-episode files
    return Classification(kind="mixed", episodes=deduped_eps, single_file=None, total_bytes=total)


def should_skip_movie(classification: Classification, cfg: AppConfig) -> bool:
    if classification.kind != "movie":
        return False
    return classification.total_bytes > cfg.ssd.skip_movie_larger_than_bytes


def file_total_size(files: Iterable[TorrentFile]) -> int:
    return sum(f.size_bytes for f in files)