"""Classify a torrent's files as movie / season / episode.

req #6:
  - Movie or full season folders go to rclone remote default.
  - Individual episodes (matching S00E00 regex) go to remote unsorted.

Feasibility is enforced per individual file (see oversize_single_file):
anything — movie, season pack, game, disc image — flows through download
(batched as needed); only a single file bigger than the SSD cap is refused.
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
    ".ass", ".ssa", ".vtt", ".smi", ".sup",
    ".rar", ".zip", ".par2", ".cue", ".m3u",
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


_DELIM_START = r"(?:(?<=[._\-\s\[\(/\\])|^)"
_DELIM_END = r"(?:(?=[._\-\s\]\)/\\])|$)"

_BLACKLIST_TAGS_RE = re.compile(
    r"(?i)(?:(?<=[._\-\s\[\(/\\])|^)"
    r"(?:"
    r"\d{3,4}x\d{3,4}"                   # 1920x1080, 3840x2160, 1280x720, etc.
    r"|\d{3,4}[pi]"                      # 720p, 1080p, 1080i, 2160p, 480p, 576p
    r"|[xh]\.?26[45]"                    # x264, x265, h264, h265, x.264
    r"|(?:5\.1|7\.1|2\.0)"               # audio channels (narrow: avoid matching versions like 1.0)
    r"|4k|8k"                            # 4k, 8k
    r")"
    r"(?:(?=[._\-\s\]\)/\\])|$)"
)

EP_RE = re.compile(
    r"(?i)"
    + _DELIM_START
    + r"(?:"
    r"[Ss](\d{1,2})[._\-\s]*[Ee][Pp]?(\d{1,3})"
    r"|(?<!\d)(\d{1,2})x(\d{1,2})(?!\d)"
    r"|(?:[Ee][Pp]?|[Ee]pisode)[._\-\s]*(\d{1,3})(?!\d)"
    r"|[Ss]eason[._\-\s]*(\d{1,2})[._\-\s]*[Ee]pisode[._\-\s]*(\d{1,3})"
    r")"
    + _DELIM_END
)


@functools.lru_cache(maxsize=128)
def _compile_pattern(pattern_str: str) -> re.Pattern[str]:
    return re.compile(pattern_str)


@functools.lru_cache(maxsize=2048)
def parse_episode(name: str, regex: re.Pattern[str] | str | None = None) -> tuple[int, int] | None:
    """Return (season, episode) parsed from filename, or None."""
    if not isinstance(name, str) or not name:
        return None
    if regex is not None and not isinstance(regex, (str, re.Pattern)):
        return None
    if regex is None:
        pattern = EP_RE
    elif isinstance(regex, str):
        pattern = _compile_pattern(regex)
    else:
        pattern = regex

    clean_name = _BLACKLIST_TAGS_RE.sub(" ", name)
    m = pattern.search(clean_name)
    if not m:
        return None

    # If pattern has capture groups, use non-None matched groups
    matched_groups = [g for g in m.groups() if g is not None]
    if len(matched_groups) >= 2:
        try:
            return int(matched_groups[0]), int(matched_groups[1])
        except ValueError:
            pass
    elif len(matched_groups) == 1:
        try:
            return 1, int(matched_groups[0])
        except ValueError:
            pass

    # Otherwise extract all numeric sequences from the matched span (e.g. S01E08 -> [1, 8])
    nums = re.findall(r"\d+", m.group(0))
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    elif len(nums) == 1:
        return 1, int(nums[0])
    return None


def _safe_file_name_size(f: object) -> tuple[str, int] | None:
    """Coerce a client file entry to (name, size); None when unaddressable.

    Client names must stay byte-identical for priority maps, so well-formed
    str names pass through untouched; only hostile/None values coerce.
    """
    try:
        raw_name = getattr(f, "name", "")
        name = raw_name if isinstance(raw_name, str) else str(raw_name or "")
    except Exception:
        return None
    if not name:
        return None
    try:
        size = int(getattr(f, "size_bytes", 0) or 0)
    except (TypeError, ValueError):
        size = 0
    if size < 0:
        size = 0
    return name, size


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
    normed: list[tuple[TorrentFile, str, int]] = []
    for f in files:
        coerced = _safe_file_name_size(f)
        if coerced is None:
            continue
        normed.append((f, coerced[0], coerced[1]))
    total = sum(size for _, _, size in normed)

    if not normed:
        return Classification(kind="unknown", episodes=[], single_file=None, total_bytes=0)

    ep_re_attr = getattr(cfg.classifier, "_episode_re", None)
    ep_regex = ep_re_attr if isinstance(ep_re_attr, (re.Pattern, str)) else EP_RE

    # Filter out non-video files (.srt, .nfo, etc.) when evaluating episodes.
    # Operates on coerced (name, size) pairs so hostile entries can't crash
    # the suffix/parse path; Episode file_names stay client-identical for
    # well-formed inputs.
    video = [(f, n, s) for f, n, s in normed
             if Path(n).suffix.lower() not in NON_VIDEO_EXTENSIONS]
    eval_trip = video if video else normed

    # Check for episode matches across evaluated files
    eps: list[Episode] = []
    for _, name, size in eval_trip:
        parsed = parse_episode(name, ep_regex)
        if parsed:
            eps.append(Episode(name, parsed[0], parsed[1], size))
    eps.sort(key=lambda e: (e.season, e.episode))

    # Group eps per (season, episode), keeping EVERY file variant.
    # An episode released in two containers (S01E01.mkv + S01E01.mp4)
    # must move both: keeping only the largest leaves the loser
    # unselected, and the later folder wipe deletes it (data loss).
    best_by_ep: dict[tuple[int, int], Episode] = {}
    for ep in eps:
        key = (ep.season, ep.episode)
        if key not in best_by_ep or ep.size_bytes > best_by_ep[key].size_bytes:
            best_by_ep[key] = ep
    deduped_eps = sorted(eps, key=lambda e: (e.season, e.episode))

    distinct_eps = {(e.season, e.episode) for e in deduped_eps}

    # Case 1: Exactly 1 distinct episode found -> individual episode torrent (routes to unsorted)
    # Guard: a single episode tag alongside other unrelated videos (e.g. S01E01.mkv
    # + Movie.mkv) must NOT be classified as a lone episode — fall through to
    # mixed/season logic below.
    if len(distinct_eps) == 1:
        sole_key = next(iter(distinct_eps))
        main_ep = best_by_ep[sole_key]
        if len(eval_trip) == 1 or main_ep.size_bytes >= int(0.9 * total):
            return Classification(
                kind="episode",
                episodes=[main_ep],
                single_file=main_ep.file_name,
                total_bytes=total,
            )
        # else: fall through — episode is a minority of the payload

    # Case 2: No episodes found at all
    if not deduped_eps:
        # If single file -> movie
        if len(normed) == 1:
            _, name, size = normed[0]
            if size > cfg.ssd.skip_movie_larger_than_bytes:
                log.warning(
                    "movie '%s' (%d B) exceeds skip threshold (%d B)",
                    name, size, cfg.ssd.skip_movie_larger_than_bytes,
                )
            return Classification(
                kind="movie", episodes=[], single_file=name, total_bytes=total,
            )
        # Multi-file but no episode tag -> treat as movie bundle (default remote)
        return Classification(
            kind="movie", episodes=[], single_file=None, total_bytes=total,
        )

    # Case 3: Multiple distinct episodes found (len(distinct_eps) >= 2) -> full season pack
    # If >= 90% of evaluated files carry an episode tag, treat as a season.
    # Use ceiling: e.g. 4 files where 3 are episodes is still a season.
    # NOTE: use pre-dedup `eps` count — deduped count undercounts when 2 files
    # map to the same (season, episode) (e.g. mkv + mp4 per episode).
    if len(eps) >= max(1, int(-(-len(eval_trip) * 9 // 10))):
        return Classification(kind="season", episodes=deduped_eps, single_file=None, total_bytes=total)

    # Mixed (rare): multiple episodes with lots of non-episode files
    return Classification(kind="mixed", episodes=deduped_eps, single_file=None, total_bytes=total)


def oversize_single_file(files: Iterable[TorrentFile], cfg: AppConfig) -> str | None:
    """Name of a single file that can never fit the SSD cap, else None.

    Batched/type-agnostic flows stream multi-file torrents of any total size
    through the SSD in chunks, so total size never disqualifies content —
    only an individual file bigger than `skip_movie_larger_than_bytes`
    (games, disc images, giant episodes alike) is refused upfront.
    """
    try:
        ssd_cfg = cfg.ssd
    except AttributeError:
        return None
    cap_raw = getattr(ssd_cfg, "skip_movie_larger_than_bytes", 0)
    # Strictly numeric only: a MagicMock (unit tests) int()s to 1, which
    # would refuse every file. Real configs always carry an int here.
    if isinstance(cap_raw, bool) or not isinstance(cap_raw, (int, float)):
        return None
    cap = int(cap_raw)
    if cap <= 0:
        return None
    for f in files:
        try:
            size = int(f.size_bytes or 0)
        except (TypeError, ValueError):
            continue
        if size > cap and getattr(f, "name", ""):
            return f.name
    return None