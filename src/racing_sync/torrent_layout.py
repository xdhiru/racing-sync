"""Torrent top-folder layout rules shared by the move and fuse code.

A torrent is *wrapped* when every file name carries the same top folder
(``Top/file.mkv``), as libtorrent/qBittorrent report for multi-file
torrents, and *flat* when names are bare files (``file.mkv``). The VPS2
download torrent and the VPS1 racing torrent can disagree on that shape,
so re-adding to a fuse mount must join the prefix the target actually has.

Pure helpers only: no filesystem or client I/O, and the bencode decoder is
imported lazily inside the blob helpers.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

_MEDIA_SUFFIXES = frozenset(
    {
        ".mkv",
        ".mp4",
        ".avi",
        ".m4v",
        ".mov",
        ".wmv",
        ".flv",
        ".webm",
        ".mpg",
        ".mpeg",
        ".ts",
        ".m2ts",
        ".iso",
        ".rar",
        ".zip",
        ".7z",
        ".srt",
        ".nfo",
        ".sub",
        ".ass",
    }
)

# rclone remote paths look like ``name:path``; single-letter names are
# treated as Windows drives (``C:``) rather than remotes.
_REMOTE_RE = re.compile(r"^([A-Za-z0-9_-]+):")


def _normalize(name: object) -> str:
    """Backslash-to-slash form with leading whitespace and ``/`` removed."""
    if not isinstance(name, str):
        return ""
    return name.replace("\\", "/").strip().lstrip("/")


def common_top(names: Iterable[str]) -> str | None:
    """Return the shared top folder of wrapped torrent names, else None.

    Every normalized name must contain a path separator and start with the
    same first segment; a single flat name (``file.mkv``) or a mix of tops
    means there is no safe prefix to join.
    """
    cleaned = [n for n in (_normalize(raw) for raw in names or ()) if n]
    if not cleaned or any("/" not in n for n in cleaned):
        return None
    top = cleaned[0].split("/", 1)[0]
    if top in (".", ".."):
        return None
    if any(n.split("/", 1)[0] != top for n in cleaned[1:]):
        return None
    return top


def is_flat(names: Iterable[str]) -> bool:
    """True when names exist and none of them carries a path separator."""
    cleaned = [n for n in (_normalize(raw) for raw in names or ()) if n]
    return bool(cleaned) and not any("/" in n for n in cleaned)


def blob_files(blob: bytes | None) -> list:
    """Decode a .torrent blob into TorrentFile entries, or [] on any failure.

    The watchdir decoder is imported lazily so this module stays cheap to
    import and free of client/coordinator dependencies.
    """
    if not isinstance(blob, bytes) or not blob:
        return []
    try:
        from .watchdir import extract_torrent_files_from_bencoded

        return list(extract_torrent_files_from_bencoded(blob) or [])
    except Exception:
        return []


def blob_common_top(blob: bytes | None) -> str | None:
    """Wrapped top folder of a decoded torrent blob, or None when flat/bad."""
    return common_top(getattr(f, "name", "") for f in blob_files(blob))


def blob_is_wrapped(blob: bytes | None) -> bool:
    """True when any decoded file name sits under a folder (wrapped layout)."""
    return any("/" in _normalize(getattr(f, "name", "")) for f in blob_files(blob))


def looks_like_single_file(name: str) -> bool:
    """True when the basename ends in a media/archive suffix, not release junk.

    Folder names can be dot-heavy (``Example.Show.S24E32.1080p.WEB.H264-GROUP``),
    so only the final suffix decides; ``.mkv`` wins, ``.H264-GROUP`` does not.
    """
    if not isinstance(name, str) or not name.strip():
        return False
    base = name.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    suffix = Path(base).suffix.lower()
    return bool(suffix) and suffix in _MEDIA_SUFFIXES


def join_remote(remote: str, segment: str) -> str:
    """Append one safe path segment to an rclone remote or local directory.

    Unsafe segments (separators, ``.``, ``..``, empty) leave ``remote``
    untouched and never raise, so callers cannot be steered outside it.
    """
    seg = segment.strip() if isinstance(segment, str) else ""
    if not seg:
        return remote
    if "/" in seg or "\\" in seg or seg in (".", ".."):
        return remote
    match = _REMOTE_RE.match(remote) if isinstance(remote, str) else None
    if match is not None:
        name = match.group(1)
        if not (len(name) == 1 and name.isalpha()):
            return remote.rstrip("/\\") + "/" + seg
    try:
        return str(Path(remote) / seg)
    except Exception:
        return remote


__all__ = [
    "blob_common_top",
    "blob_files",
    "blob_is_wrapped",
    "common_top",
    "is_flat",
    "join_remote",
    "looks_like_single_file",
]
