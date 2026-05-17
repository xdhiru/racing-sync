"""Classify a torrent's files as movie / season / episode.

req #6:
  - Movie or full season folders go to rclone remote default.
  - Individual episodes (matching S00E00 regex) go to remote unsorted.

We also reject single-file torrents (movies) larger than `skip_movie_larger_than_bytes`.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable

from .clients.abstract import TorrentFile
from .config import AppConfig

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Episode:
    file_name: str            # exact file path inside the torrent
    season: int
    episode: int
    size_bytes: int


@dataclass(slots=True)
class Classification:
    kind: str                 # "movie" | "episode" | "season" | "mixed" | "unknown"
    # When kind in ("season", "episode", "mixed"), episode details live here.
    episodes: list[Episode]
    # When kind in ("movie", "episode", "unknown"), the single-file fallback
    single_file: str | None
    total_bytes: int

    @property
    def target_remote(self) -> str:
        # Resolved by the coordinator with config.
        return ""


EP_RE = re.compile(r"(?i)\bS(\d{1,2})E(\d{1,2})\b")


def parse_episode(name: str, regex: re.Pattern[str] | str | None = None) -> tuple[int, int] | None:
    """Return (season, episode) parsed from filename, or None."""
    if regex is None:
        pattern = EP_RE
    elif isinstance(regex, str):
        pattern = re.compile(regex)
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
    return 1, 1


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

    # Check for episode matches across all files
    eps: list[Episode] = []
    for f in files:
        parsed = parse_episode(f.name, ep_regex)
        if parsed:
            eps.append(Episode(f.name, parsed[0], parsed[1], f.size_bytes))
    eps.sort(key=lambda e: (e.season, e.episode))

    distinct_eps = {(e.season, e.episode) for e in eps}

    # Case 1: Exactly 1 distinct episode found -> individual episode torrent (routes to unsorted)
    if len(distinct_eps) == 1:
        # Pick the largest file as the primary episode file (e.g. video over .nfo/.srt)
        main_ep = max(eps, key=lambda e: e.size_bytes)
        return Classification(
            kind="episode",
            episodes=[main_ep],
            single_file=main_ep.file_name,
            total_bytes=total,
        )

    # Case 2: No episodes found at all
    if not eps:
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
        # Multi-file but no episode tag -> treat as movie/season bundle (default remote)
        return Classification(
            kind="season", episodes=[], single_file=None, total_bytes=total,
        )

    # Case 3: Multiple distinct episodes found (len(distinct_eps) >= 2) -> full season pack
    # If >= 90% of files carry an episode tag, treat as a season.
    # Use ceiling: e.g. 4 files where 3 are episodes is still a season.
    if len(eps) >= max(1, int(-(-len(files) * 9 // 10))):
        return Classification(kind="season", episodes=eps, single_file=None, total_bytes=total)

    # Mixed (rare): multiple episodes with lots of non-episode files
    return Classification(kind="mixed", episodes=eps, single_file=None, total_bytes=total)


def should_skip_movie(classification: Classification, cfg: AppConfig) -> bool:
    if classification.kind != "movie":
        return False
    return classification.total_bytes > cfg.ssd.skip_movie_larger_than_bytes


def file_total_size(files: Iterable[TorrentFile]) -> int:
    return sum(f.size_bytes for f in files)