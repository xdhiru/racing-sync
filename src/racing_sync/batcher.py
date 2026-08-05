"""SSD-aware episode batching.

Given a sorted list of Episode objects and the available SSD bytes,
split them into batches whose total size fits inside the SSD cap.

req #7: handle big seasons as multiple batches.
req #8: each batch is moved independently with rclone include patterns.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
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


def files_from_names(names: Iterable[str]) -> list[str]:
    """Normalized torrent-relative names for rclone --files-from-raw.

    Shared by batch moves and the end-of-download leftover sweep so both
    address torrent-relative paths identically. Only the listed files can
    transfer — piece-boundary partials of deselected files are never named
    and therefore can neither reach the remote nor overwrite an older
    batch's moved file. Raw mode needs no glob escaping and preserves the
    relative tree on the remote exactly.

    Untrusted torrent file names are filtered: absolute paths, ``..``
    segments, empty names, and embedded newlines are skipped (never
    converted to a relative path) so a crafted torrent cannot escape the
    local source dir via ``--files-from-raw``.
    """
    out: list[str] = []
    seen: set[str] = set()
    for raw in names:
        if not raw or not isinstance(raw, str):
            continue
        # Reject absolute paths before stripping: "/etc/passwd" must not
        # silently become "etc/passwd".
        stripped = raw.strip()
        if stripped.startswith("/") or stripped.startswith("\\"):
            log.warning("batcher: skipping absolute file name for file list: %r", raw[:100])
            continue
        # Windows drive-absolute ("C:/x", "C:\\x") — reject, don't relativize.
        if len(stripped) >= 2 and stripped[1] == ":" and stripped[0].isalpha():
            log.warning("batcher: skipping drive-absolute file name for file list: %r", raw[:100])
            continue
        if "\n" in raw or "\r" in raw or "\0" in raw:
            log.warning("batcher: skipping file name with control chars for file list: %r", raw[:100])
            continue
        # Normalize path separators to POSIX forward slashes
        normalized = raw.replace("\\", "/").strip("/")
        parts = [p for p in normalized.split("/") if p]
        if not parts:
            log.warning("batcher: skipping empty file name for file list")
            continue
        if any(p in (".", "..") for p in parts):
            log.warning("batcher: skipping traversal file name for file list: %r", raw[:100])
            continue
        name = "/".join(parts)
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


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

    def file_names(self) -> list[str]:
        """Torrent-relative names for this batch's rclone --files-from-raw list."""
        return files_from_names([e.file_name for e in self.episodes])


def make_batches(
    episodes: list[Episode], *, cap_bytes: int, max_files: int = DEFAULT_MAX_BATCH_FILES
) -> list[Batch]:
    """Greedy sequential batches over already-sorted episodes.

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
        if ep.size_bytes < 0:
            raise ValueError(f"episode {ep.file_name!r} has negative size {ep.size_bytes}")
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


def make_file_batches(
    files: list, *, cap_bytes: int, max_files: int = DEFAULT_MAX_BATCH_FILES
) -> list[Batch]:
    """Greedy size-capped groups over arbitrary torrent files.

    Type-agnostic counterpart to make_batches for multi-file content that is
    not episodic (games, disc images, complete packs with plain numbering):
    files stream through the SSD in name-sorted groups instead of needing
    the whole torrent on disk at once. Members are synthesized Episode
    entries (season 0) so every downstream consumer (priorities, wait
    lists, include patterns, cleanup) works unchanged.
    """
    ordered = sorted(
        [f for f in files if getattr(f, "name", "")],
        key=lambda f: f.name.replace("\\", "/"),
    )
    episodes: list[Episode] = []
    for idx, f in enumerate(ordered, start=1):
        try:
            size = int(float(getattr(f, "size_bytes", 0) or 0))
        except (TypeError, ValueError):
            size = 0
        if size < 0:
            raise ValueError(f"file {f.name!r} has negative size {size}")
        episodes.append(Episode(f.name, 0, idx, size))
    return make_batches(episodes, cap_bytes=cap_bytes, max_files=max_files)