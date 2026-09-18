"""Watch-dir scanner.

req #3:
  - A directory on VPS2 where the user drops .torrent files manually.
  - For each new file:
      1. Parse the .torrent metadata (name, files, infohash).
      2. If watch_dir.query_prowlarr is true and a hit is found on the
         configured download_indexer, use the prowlarr .torrent instead.
      3. Otherwise use the dropped file directly.
  - Emit a QueueItem for the coordinator to process.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

import aiohttp

from .config import WatchDirConfig
from .prowlarr import ProwlarrClient, TorrentHit

log = logging.getLogger(__name__)


@dataclass(slots=True)
class WatchItem:
    torrent_path: Path
    infohash: str
    name: str
    size_bytes: int
    announce_url: str = ""
    torrent_bytes: bytes = b""
    # Filled in by the scanner if prowlarr was consulted.
    prowlarr_hit: TorrentHit | None = None
    # Final .torrent bytes the coordinator should hand to qBittorrent:
    prefer_dropped: bool = False


MAX_TORRENT_BYTES: int = 20 * 1024 * 1024  # 20 MiB safety cap
MAX_BENCODE_DEPTH: int = 64
MAX_BENCODE_ITEMS: int = 200_000  # cap total list/dict elements to bound DoS


def _bdecode(
    data: bytes, pos: int = 0, *, depth: int = 0, _items: list[int] | None = None
) -> tuple[int, object]:
    if _items is None:
        _items = [0]
    def _count(n: int = 1) -> None:
        _items[0] += n
        if _items[0] > MAX_BENCODE_ITEMS:
            raise ValueError(f"bencode item limit exceeded ({MAX_BENCODE_ITEMS})")
    if depth > MAX_BENCODE_DEPTH:
        raise ValueError(f"bencode recursion depth exceeded: {depth}")
    if pos >= len(data):
        raise ValueError(f"unexpected EOF at position {pos}")

    ch = data[pos:pos + 1]
    pos += 1
    if ch == b"i":
        try:
            end = data.index(b"e", pos)
        except ValueError as e:
            raise ValueError(f"unterminated integer starting at position {pos - 1}") from e
        num_str = data[pos:end]
        if not num_str or (num_str.startswith(b"-0") or (len(num_str) > 1 and num_str.startswith(b"0"))):
            raise ValueError(f"invalid integer format {num_str!r} at position {pos}")
        try:
            val = int(num_str)
        except ValueError as e:
            raise ValueError(f"invalid integer {num_str!r} at position {pos}") from e
        return end + 1, val

    if ch == b"l":
        out_list: list[object] = []
        while True:
            if pos >= len(data):
                raise ValueError(f"unexpected EOF in list at position {pos}")
            if data[pos:pos + 1] == b"e":
                return pos + 1, out_list
            pos, item = _bdecode(data, pos, depth=depth + 1, _items=_items)
            _count(1)
            out_list.append(item)

    if ch == b"d":
        out_dict: dict[bytes, object] = {}
        while True:
            if pos >= len(data):
                raise ValueError(f"unexpected EOF in dict at position {pos}")
            if data[pos:pos + 1] == b"e":
                return pos + 1, out_dict
            pos, k = _bdecode(data, pos, depth=depth + 1, _items=_items)
            if not isinstance(k, (bytes, str)):
                raise ValueError(f"dict key must be bytes/str, got {type(k)} at position {pos}")
            k_bytes = k if isinstance(k, bytes) else k.encode("utf-8")
            pos, v = _bdecode(data, pos, depth=depth + 1, _items=_items)
            _count(1)
            out_dict[k_bytes] = v

    if ch.isdigit():
        try:
            colon = data.index(b":", pos)
        except ValueError as e:
            raise ValueError(f"unterminated string length at position {pos - 1}") from e
        try:
            length = int(data[pos - 1:colon])
        except ValueError as e:
            raise ValueError(f"invalid string length at position {pos - 1}") from e
        if length < 0:
            raise ValueError(f"negative string length {length} at position {pos - 1}")
        start = colon + 1
        end = start + length
        if end > len(data):
            raise ValueError(
                f"string length {length} extends past EOF (start={start}, end={end}, total={len(data)})"
            )
        return end, data[start:end]

    raise ValueError(f"bad bencode prefix {ch!r} at position {pos - 1}")


def _bencoded_info_hash(data: bytes) -> tuple[str, str, int, str]:
    """Decode a bencoded .torrent and return (infohash, name, total_size, announce_url).

    We avoid `bencodepy` / `torf` as a dep by writing a minimal decoder good
    enough for top-level info extraction. The raw bytes of the info dict
    are captured directly from the byte stream without re-encoding to preserve
    the true SHA1 infohash even for torrents with non-standard key sorting.
    """
    if not data:
        raise ValueError("empty torrent data")
    if len(data) > MAX_TORRENT_BYTES:
        raise ValueError(f"torrent file exceeds maximum allowed size ({len(data)} > {MAX_TORRENT_BYTES})")

    if not data.startswith(b"d"):
        raise ValueError("torrent has no root dict")

    pos = 1
    root: dict[bytes, object] = {}
    raw_info_bytes: bytes | None = None
    # Share one item budget across the whole root dict: per-key _bdecode
    # calls without it let a many-small-keys dict bypass MAX_BENCODE_ITEMS.
    _budget: list[int] = [0]

    while True:
        if pos >= len(data):
            raise ValueError("unexpected EOF in root dict")
        if data[pos:pos + 1] == b"e":
            break
        pos, k = _bdecode(data, pos, depth=1, _items=_budget)
        if not isinstance(k, (bytes, str)):
            raise ValueError("dict key must be string/bytes")
        k_bytes = k if isinstance(k, bytes) else k.encode("utf-8")

        val_start = pos
        pos, v = _bdecode(data, pos, depth=1, _items=_budget)
        val_end = pos
        if k_bytes == b"info":
            raw_info_bytes = data[val_start:val_end]
        root[k_bytes] = v
        # _bdecode only counts containers: count top-level pairs here so a
        # flat many-small-keys dict cannot bypass MAX_BENCODE_ITEMS.
        _budget[0] += 1
        if _budget[0] > MAX_BENCODE_ITEMS:
            raise ValueError(f"bencode item limit exceeded ({MAX_BENCODE_ITEMS})")

    info = root.get(b"info")
    if not isinstance(info, dict) or raw_info_bytes is None:
        raise ValueError("torrent has no info dict")

    name = info.get(b"name", b"")
    if isinstance(name, bytes):
        name = name.decode("utf-8", errors="replace")
    announces: list[str] = []
    announce = root.get(b"announce", b"")
    if isinstance(announce, bytes):
        announces.append(announce.decode("utf-8", errors="replace"))
    elif isinstance(announce, str):
        announces.append(announce)
    announce_list = root.get(b"announce-list")
    if isinstance(announce_list, list):
        for tier in announce_list:
            if isinstance(tier, list):
                for u in tier:
                    if isinstance(u, bytes):
                        announces.append(u.decode("utf-8", errors="replace"))
                    elif isinstance(u, str):
                        announces.append(u)
    announce_str = ",".join(dict.fromkeys(a for a in announces if a))
    pieces = info.get(b"files") or None
    total = 0
    if pieces is None:
        length_raw = info.get(b"length", 0)
        total = length_raw if isinstance(length_raw, int) and length_raw >= 0 else 0
    elif isinstance(pieces, list):
        for f in pieces:
            if isinstance(f, dict):
                length_raw = f.get(b"length", 0)
                if isinstance(length_raw, int) and length_raw >= 0:
                    total += length_raw

    infohash = hashlib.sha1(raw_info_bytes).hexdigest().lower()
    return infohash, name, total, announce_str


def extract_torrent_files_from_bencoded(data: bytes) -> list[Any]:
    """Extract list of TorrentFile entries from a bencoded .torrent payload."""
    from .clients.abstract import TorrentFile

    if not data or not data.startswith(b"d"):
        return []
    if len(data) > MAX_TORRENT_BYTES:
        return []
    pos = 1
    root: dict[bytes, object] = {}
    _budget: list[int] = [0]
    while True:
        if pos >= len(data) or data[pos:pos + 1] == b"e":
            break
        pos, k = _bdecode(data, pos, depth=1, _items=_budget)
        if not isinstance(k, (bytes, str)):
            break
        k_bytes = k if isinstance(k, bytes) else k.encode("utf-8")
        pos, v = _bdecode(data, pos, depth=1, _items=_budget)
        root[k_bytes] = v
        _budget[0] += 1
        if _budget[0] > MAX_BENCODE_ITEMS:
            raise ValueError(f"bencode item limit exceeded ({MAX_BENCODE_ITEMS})")

    info = root.get(b"info")
    if not isinstance(info, dict):
        return []

    top_name = info.get(b"name", b"")
    if isinstance(top_name, bytes):
        top_name = top_name.decode("utf-8", errors="replace")

    files_list: list[TorrentFile] = []
    pieces = info.get(b"files")
    def _safe_part(p: str) -> str | None:
        # Path parts come from untrusted .torrent metadata: reject
        # traversal, absolute, and control-char segments.
        if not p or p in (".", ".."):
            return None
        if "/" in p or "\\" in p or "\n" in p or "\r" in p or "\0" in p:
            return None
        if len(p) >= 2 and p[1] == ":" and p[0].isalpha():
            return None
        return p

    if isinstance(pieces, list):
        top_safe = _safe_part(str(top_name)) if top_name else None
        # Fall back to a neutral top when the torrent's name itself is
        # unsafe — never propagate traversal into downstream joins.
        top_prefix = top_safe if top_safe else "torrent"
        for f in pieces:
            if isinstance(f, dict):
                length_raw = f.get(b"length", 0)
                length = length_raw if isinstance(length_raw, int) and length_raw >= 0 else 0
                path_parts = f.get(b"path", [])
                parts_str: list[str] = []
                if isinstance(path_parts, list):
                    for p in path_parts:
                        if isinstance(p, bytes):
                            s = p.decode("utf-8", errors="replace")
                        elif isinstance(p, str):
                            s = p
                        else:
                            continue
                        safe = _safe_part(s)
                        if safe is None:
                            continue
                        parts_str.append(safe)
                rel_path = "/".join(parts_str) if parts_str else "file"
                full_name = f"{top_prefix}/{rel_path}"
                files_list.append(TorrentFile(name=full_name, size_bytes=length, progress=1.0))
    else:
        length_raw = info.get(b"length", 0)
        length = length_raw if isinstance(length_raw, int) and length_raw >= 0 else 0
        safe_top = _safe_part(str(top_name)) if top_name else None
        files_list.append(TorrentFile(name=str(safe_top or "file"), size_bytes=length, progress=1.0))

    return files_list


def _bencode(obj: object) -> bytes:
    if isinstance(obj, int):
        return b"i" + str(obj).encode() + b"e"
    if isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    if isinstance(obj, str):
        b = obj.encode()
        return str(len(b)).encode() + b":" + b
    if isinstance(obj, list):
        return b"l" + b"".join(_bencode(x) for x in obj) + b"e"
    if isinstance(obj, dict):
        out = b"d"
        for k in sorted(obj.keys()):
            out += _bencode(k) + _bencode(obj[k])
        return out + b"e"
    raise TypeError(f"cannot bencode {type(obj)}")


def parse_torrent_file(path: Path) -> tuple[str, str, int, str, bytes]:
    # Reject symlinks — watch-dir must not follow links to /etc/passwd etc.
    if path.is_symlink():
        raise ValueError(f"refusing symlink torrent: {path}")
    try:
        st = path.stat()
        if st.st_size > MAX_TORRENT_BYTES:
            raise ValueError(f"torrent file exceeds maximum allowed size ({st.st_size} > {MAX_TORRENT_BYTES})")
        if st.st_size == 0:
            raise ValueError("empty torrent file")
    except OSError as e:
        raise ValueError(f"cannot stat torrent file {path}: {e}") from e
    data = path.read_bytes()
    # Re-check after read (TOCTOU): file may have grown between stat and read.
    if len(data) > MAX_TORRENT_BYTES:
        raise ValueError(f"torrent file exceeds maximum allowed size ({len(data)} > {MAX_TORRENT_BYTES})")
    infohash, name, total, announce = _bencoded_info_hash(data)
    return infohash, name, total, announce, data


# ---------- scanner ----------


class WatchDirScanner:
    # Metadata-only file cache: (mtime, size, infohash, name, size, announce).
    # Torrent bytes are re-read on emission only, so the cache can never
    # hold onto megabytes per file.
    _FILE_CACHE_MAX = 512

    def __init__(self, cfg: WatchDirConfig, prowlarr: ProwlarrClient | None):
        self._cfg = cfg
        self._prowlarr = prowlarr
        self._seen: set[str] = set()  # infohashes already picked up
        self._file_cache: dict[Path, tuple[float, int, str, str, int, str]] = {}
        self._bad_files: dict[Path, tuple[float, int]] = {}

    async def scan_once(self) -> list[WatchItem]:
        out: list[WatchItem] = []
        current_files: set[Path] = set()
        current_infohashes: set[str] = set()
        for entry in sorted(Path(self._cfg.path).glob(self._cfg.glob)):
            if entry.is_symlink():
                log.warning("watch-dir: skipping symlink %s", entry.name)
                continue
            if not entry.is_file():
                continue
            current_files.add(entry)
            try:
                st = entry.stat()
                mtime, fsize = st.st_mtime, st.st_size
                if fsize == 0 or fsize > MAX_TORRENT_BYTES:
                    if fsize > MAX_TORRENT_BYTES:
                        log.warning(
                            "watch-dir: skipping oversize torrent %s (%d B)",
                            entry.name, fsize,
                        )
                    continue

                # Skip known bad files unless they have been modified
                bad = self._bad_files.get(entry)
                if bad and bad[0] == mtime and bad[1] == fsize:
                    continue

                cached = self._file_cache.get(entry)
                if cached and cached[0] == mtime and cached[1] == fsize:
                    infohash, name, size, announce = (
                        cached[2], cached[3], cached[4], cached[5]
                    )
                    # Bytes are never cached: re-read for emission (the file
                    # is small — parse already size-checked it).
                    try:
                        data = entry.read_bytes()
                    except OSError as e:
                        log.warning("watch-dir: error accessing %s (%s)", entry.name, e)
                        continue
                    if len(data) != fsize:
                        self._file_cache.pop(entry, None)
                        continue
                else:
                    try:
                        infohash, name, size, announce, data = parse_torrent_file(entry)
                        self._file_cache[entry] = (mtime, fsize, infohash, name, size, announce)
                        if len(self._file_cache) > self._FILE_CACHE_MAX:
                            # Evict oldest-inserted (dicts preserve order).
                            for k in list(self._file_cache.keys())[: len(self._file_cache) - self._FILE_CACHE_MAX]:
                                self._file_cache.pop(k, None)
                        self._bad_files.pop(entry, None)
                    except Exception as parse_err:
                        # Half-write guard: if modified recently (< 2s), wait for write to settle
                        if time.time() - mtime < 2.0:
                            continue
                        self._bad_files[entry] = (mtime, fsize)
                        log.warning("watch-dir: skipping invalid torrent %s (%s)", entry.name, parse_err)
                        continue
            except Exception as e:  # noqa: BLE001
                log.warning("watch-dir: error accessing %s (%s)", entry.name, e)
                continue

            current_infohashes.add(infohash)
            if infohash in self._seen:
                continue
            self._seen.add(infohash)
            item = WatchItem(
                torrent_path=entry,
                infohash=infohash,
                name=name,
                size_bytes=size,
                announce_url=announce,
                torrent_bytes=data,
                prefer_dropped=False,
            )
            out.append(item)
            from .logging_setup import sanitize_log_text
            safe_name = sanitize_log_text(name.replace("\n", " ").replace("\r", " "))[:200]
            safe_announce = sanitize_log_text(announce.replace("\n", " ").replace("\r", " "))[:200]
            log.info(
                "watch-dir picked up: %s (%s) announce=%s",
                safe_name, infohash[:10], safe_announce,
            )

        # Prune deleted files from caches
        for k in set(self._file_cache.keys()) - current_files:
            self._file_cache.pop(k, None)
        for k in set(self._bad_files.keys()) - current_files:
            self._bad_files.pop(k, None)

        # Prune _seen for items no longer in watchdir if delete_after_pickup is active,
        # or cap _seen size to prevent memory leak (deterministic FIFO by
        # insertion order is impossible on a set — keep the intersection plus
        # an arbitrary-but-bounded sample).
        if self._cfg.delete_after_pickup:
            self._seen &= current_infohashes
        else:
            # Resident hashes must never be evicted: with pickup files kept
            # on disk, an evicted hash re-emits as a duplicate WatchItem on
            # the next scan. Only non-resident overflow is bounded.
            keep = set(self._seen & current_infohashes)
            overflow = sorted(self._seen - keep)[: max(0, 5000 - len(keep))]
            self._seen = keep | set(overflow)

        return out

    async def delete_picked_up(self, item: WatchItem) -> None:
        if not self._cfg.delete_after_pickup:
            return
        try:
            item.torrent_path.unlink()
            self._file_cache.pop(item.torrent_path, None)
            self._seen.discard(item.infohash)
        except OSError as e:
            log.warning("watch-dir: delete failed %s: %s", item.torrent_path, e)