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
        """Rclone --include patterns for this batch's episodes only.

        We include the literal file name. Rclone applies filters in a
        case-insensitive glob fashion; escaping brackets etc. is overkill
        for typical release filenames.
        """
        return [f"--include={e.file_name}" for e in self.episodes]


def make_batches(
    episodes: list[Episode], *, cap_bytes: int
) -> list[Batch]:
    """Greedy first-fit-decreasing on already-sorted episodes.

    Episodes are already (season, episode) sorted by the classifier.
    Since (season, episode) order is roughly monotonic, a greedy linear pass
    works well. If a single episode exceeds `cap_bytes`, it gets its own
    batch (and will fail at the SSD free check upstream).
    """
    if cap_bytes <= 0:
        raise ValueError("cap_bytes must be positive")

    batches: list[Batch] = []
    cur: list[Episode] = []
    cur_size = 0
    for ep in episodes:
        if cur and cur_size + ep.size_bytes > cap_bytes:
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
        "batched %d episodes into %d batches (cap=%d B)",
        len(episodes), len(batches), cap_bytes,
    )
    for i, b in enumerate(batches):
        log.debug(
            "batch %d: %s..%s, %d episodes, %d B",
            i, b.first_season_ep, b.last_season_ep, len(b.episodes), b.size_bytes,
        )
    return batches