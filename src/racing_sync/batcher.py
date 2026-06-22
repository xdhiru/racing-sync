"""SSD-aware episode batching.

Given a sorted list of Episode objects and the available SSD bytes,
split them into batches whose total size fits inside the SSD cap.

req #7: handle big seasons as multiple batches.
req #8: each batch is moved independently with rclone include patterns.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .classifier import Episode

log = logging.getLogger(__name__)


DEFAULT_MAX_BATCH_FILES = 100


def escape_rclone_glob(s: str) -> str:
    """Escape glob metacharacters for literal matching in rclone filter patterns."""
    res = []
    for ch in s:
        if ch in ("\\", "*", "?", "[", "]", "{", "}"):
            res.append(f"\\{ch}")
        else:
            res.append(ch)
    return "".join(res)


@dataclass(slots=True)
class Batch:
    """A contiguous slice of episodes that fits in one SSD round-trip."""

    episodes: list[Episode]

    @property
    def size_bytes(self) -> int:
        return sum(e.size_bytes for e in self.episodes)

    @property
    def first_season_ep(self) -> tuple[int, int] | None:
        if not self.episodes:
            return None
        return self.episodes[0].season, self.episodes[0].episode

    @property
    def last_season_ep(self) -> tuple[int, int] | None:
        if not self.episodes:
            return None
        return self.episodes[-1].season, self.episodes[-1].episode

    def include_patterns(self) -> list[str]:
        """Rclone --include patterns for this batch's episodes with subfolder and glob escaping."""
        patterns: list[str] = []
        for e in self.episodes:
            # Normalize path separators to POSIX forward slashes
            normalized = e.file_name.replace("\\", "/").strip("/")
            parts = normalized.split("/")
            escaped_path = "/".join(escape_rclone_glob(p) for p in parts if p)
            if not escaped_path.startswith("**/"):
                escaped_path = f"**/{escaped_path}"
            patterns.append(f"--include={escaped_path}")
        return patterns


def make_batches(
    episodes: list[Episode], *, cap_bytes: int, max_files: int = DEFAULT_MAX_BATCH_FILES
) -> list[Batch]:
    """Greedy first-fit-decreasing on already-sorted episodes.

    Episodes are already (season, episode) sorted by the classifier.
    Since (season, episode) order is roughly monotonic, a greedy linear pass
    works well. Batches are bounded by both `cap_bytes` and `max_files`
    to prevent exceeding OS argument limits (ARG_MAX) when constructing rclone CLI flags.
    If a single episode exceeds `cap_bytes`, it gets its own
    batch (and will fail at the SSD free check upstream).
    """
    if cap_bytes <= 0:
        raise ValueError("cap_bytes must be positive")
    if max_files <= 0:
        raise ValueError("max_files must be positive")

    if not episodes:
        return []

    batches: list[Batch] = []
    cur: list[Episode] = []
    cur_size = 0
    for ep in episodes:
        if cur and (cur_size + ep.size_bytes > cap_bytes or len(cur) >= max_files):
            batches.append(Batch(episodes=cur))
            cur, cur_size = [], 0
        cur.append(ep)
        cur_size += ep.size_bytes
        if ep.size_bytes > cap_bytes:
            # Single episode too big — emit alone, caller decides what to do.
            batches.append(Batch(episodes=cur))
            cur, cur_size = [], 0
    if cur:
        batches.append(Batch(episodes=cur))

    log.info(
        "batched %d episodes into %d batches (cap=%d B, max_files=%d)",
        len(episodes), len(batches), cap_bytes, max_files,
    )
    for i, b in enumerate(batches):
        log.debug(
            "batch %d: %s..%s, %d episodes, %d B",
            i, b.first_season_ep, b.last_season_ep, len(b.episodes), b.size_bytes,
        )
    return batches