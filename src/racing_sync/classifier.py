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
    kind: str                 # "movie" | "season" | "mixed" | "unknown"
    # When kind == "season", every file's episode lives here, sorted.
    episodes: list[Episode]
    # When kind == "movie" | "unknown", the single-file fallback (or empty)
    single_file: str | None
    total_bytes: int

    @property
    def target_remote(self) -> str:
        # Resolved by the coordinator with config.
        return ""


EP_RE = re.compile(r"(?i)\bS(\d{1,2})E(\d{1,2})\b")


def parse_episode(name: str) -> tuple[int, int] | None:
    """Return (season, episode) parsed from filename, or None."""
    m = EP_RE.search(name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def classify(files: Iterable[TorrentFile], cfg: AppConfig) -> Classification:
    """Classify a torrent given its files."""
    files = list(files)
    total = sum(f.size_bytes for f in files)

    if not files:
        return Classification(kind="unknown", episodes=[], single_file=None, total_bytes=0)

    # Case 1: single file -> movie
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

    # Case 2: episode regex matched anywhere?
    eps = [Episode(f.name, *parse_episode(f.name), f.size_bytes)
           for f in files if parse_episode(f.name)]
    eps.sort(key=lambda e: (e.season, e.episode))

    if not eps:
        # Multi-file torrent but no episode tag. Treat as a season-like bundle
        # (rare; user can rerun with corrected regex). Default to default remote.
        return Classification(
            kind="season", episodes=[], single_file=None, total_bytes=total,
        )

    # If >= 90% of files carry an episode tag, treat as a season.
    # Use ceiling: a torrent with 4 files where 3 are episodes should still
    # be considered a season because release tagging is usually consistent.
    if len(eps) >= max(1, int(-(-len(files) * 9 // 10))):
        return Classification(kind="season", episodes=eps, single_file=None, total_bytes=total)

    # Mixed (rare): prefer per-file routing to unsorted for the episode-tagged files.
    return Classification(kind="mixed", episodes=eps, single_file=None, total_bytes=total)


def should_skip_movie(classification: Classification, cfg: AppConfig) -> bool:
    if classification.kind != "movie":
        return False
    return classification.total_bytes > cfg.ssd.skip_movie_larger_than_bytes


def file_total_size(files: Iterable[TorrentFile]) -> int:
    return sum(f.size_bytes for f in files)