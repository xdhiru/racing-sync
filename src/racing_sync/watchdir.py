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
from typing import AsyncIterator, Iterable

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
    # Filled in by the scanner if prowlarr was consulted.
    prowlarr_hit: TorrentHit | None = None
    # Final .torrent bytes the coordinator should hand to qBittorrent:
    prefer_dropped: bool = False


def _bencoded_info_hash(data: bytes) -> tuple[str, int]:
    """Decode a bencoded .torrent and return (name, total_size).

    We avoid `bencodepy` / `torf` as a dep by writing a minimal decoder good
    enough for top-level info extraction.
    """
    pos, root = _bdecode(data, 0)
    info = root.get(b"info")
    if info is None:
        raise ValueError("torrent has no info dict")
    name = info.get(b"name", b"")
    if isinstance(name, bytes):
        name = name.decode("utf-8", errors="replace")
    pieces = info.get(b"files") or None
    total = 0
    if pieces is None:
        total = int(info.get(b"length", 0))
    else:
        for f in pieces:
            total += int(f.get(b"length", 0))
    # infohash v1 = SHA1 of bencoded info dict
    # We need to re-bencode the info dict at its original position. Easiest:
    # sniff by replaying: bencode re-encoding may differ in key order, but
    # we used sorted dicts in `_bdecode`. So the encoded form should be stable.
    info_bytes = _bencode(info)
    infohash = hashlib.sha1(info_bytes).hexdigest().lower()
    return infohash, name, total


def _bdecode(data: bytes, pos: int) -> tuple[int, object]:
    ch = data[pos:pos + 1]
    pos += 1
    if ch == b"i":
        end = data.index(b"e", pos)
        return end + 1, int(data[pos:end])
    if ch == b"l":
        out = []
        while data[pos:pos + 1] != b"e":
            pos, v = _bdecode(data, pos)
            out.append(v)
        return pos + 1, out
    if ch == b"d":
        out: dict[bytes, object] = {}
        while data[pos:pos + 1] != b"e":
            pos, k = _bdecode(data, pos)
            pos, v = _bdecode(data, pos)
            out[k] = v
        return pos + 1, out
    if ch.isdigit():
        colon = data.index(b":", pos)
        length = int(data[pos:colon])
        pos = colon + 1
        return pos + length, data[pos:pos + length]
    raise ValueError(f"bad bencode at {pos}: {ch!r}")


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


def parse_torrent_file(path: Path) -> tuple[str, str, int]:
    data = path.read_bytes()
    return _bencoded_info_hash(data)


# ---------- scanner ----------


class WatchDirScanner:
    def __init__(self, cfg: WatchDirConfig, prowlarr: ProwlarrClient | None):
        self._cfg = cfg
        self._prowlarr = prowlarr
        self._seen: set[str] = set()  # infohashes already picked up

    async def scan_once(self) -> list[WatchItem]:
        out: list[WatchItem] = []
        for entry in sorted(Path(self._cfg.path).glob(self._cfg.glob)):
            try:
                infohash, name, size = parse_torrent_file(entry)
            except Exception as e:  # noqa: BLE001
                log.warning("watch-dir: skipping %s (%s)", entry, e)
                continue
            if infohash in self._seen:
                continue
            self._seen.add(infohash)
            item = WatchItem(
                torrent_path=entry,
                infohash=infohash,
                name=name,
                size_bytes=size,
                prefer_dropped=False,
            )
            if self._cfg.query_prowlarr and self._prowlarr is not None:
                try:
                    hit = await self._prowlarr.best_match(name)
                except Exception as e:  # noqa: BLE001
                    log.warning("prowlarr search failed for %s: %s", name, e)
                    hit = None
                item.prowlarr_hit = hit
                if hit is not None and not self._cfg.prefer_prowlarr_result:
                    item.prefer_dropped = True
            else:
                item.prefer_dropped = True
            out.append(item)
            log.info(
                "watch-dir picked up: %s (%s) -> prowlarr=%s prefer_dropped=%s",
                name, infohash[:10],
                item.prowlarr_hit is not None, item.prefer_dropped,
            )
        return out

    async def delete_picked_up(self, item: WatchItem) -> None:
        if not self._cfg.delete_after_pickup:
            return
        try:
            item.torrent_path.unlink()
        except OSError as e:
            log.warning("watch-dir: delete failed %s: %s", item.torrent_path, e)