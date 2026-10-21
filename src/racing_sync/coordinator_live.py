"""Live-download tracking mixin: _live map refresh + snapshots.

Split out of the coordinator god-file. ``_live`` (dest-hash -> LiveItem)
is the only live-download state outside the DB: the download poll loop
maintains progress/speed, the periodic refresh re-queries the dest
client (skipping entirely when empty), workers drop their entries on
every exit path, and the Telegram bot reads the guarded snapshot maps.
"""

from __future__ import annotations

import asyncio
import logging

from .io_bounds import chunked, rpc
from .state import TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class LiveStatusMixin:
    """In-flight download tracking (duck-typed ``self``)."""

    async def _refresh_live_status(self) -> None:
        """Re-query VPS2 progress and update the live map.

        Chunks fetch in parallel (4-wide, each RPC-bounded): 10k live
        hashes no longer pay 100 serial round-trips per scheduler tick.
        One chunk failing aborts the refresh like the old serial loop —
        the next tick retries; partial rows are never applied.
        """
        if not self._live:
            return
        _chunks = list(chunked(list(self._live.keys())))
        _sem = asyncio.Semaphore(4)

        async def _one(_chunk):
            async with _sem:
                return await rpc(
                    self.dest_client.list_torrents(hashes=_chunk),
                    getattr(self, "cfg", None), "dest live list_torrents",
                ) or []

        try:
            _parts = await asyncio.gather(*(_one(c) for c in _chunks))
        except Exception as e:  # noqa: BLE001
            log.warning("list_torrents for live status failed: %s", e)
            return
        rows = [t for _part in _parts for t in _part]
        for t in rows:
            item = self._live.get(t.hash.lower())
            if item is None:
                continue
            item.progress = t.progress
            item.size_mb = t.size_bytes / (1024 * 1024)
            try:
                item.dlspeed_bps = int(getattr(t, "dlspeed_bps", 0) or 0)
            except (TypeError, ValueError):
                item.dlspeed_bps = 0
            # No absolute ETA here: the Telegram renderer derives it
            # from speed + remaining bytes (the old (1-progress)*60
            # placeholder misled operators).

    def live_progress_map(self) -> dict[str, float]:
        """Snapshot of in-flight download progress keyed by infohash.

        Used by the Telegram bot for the active-tasks message. Entries are
        keyed by dest hash for client polling, so alias each to its source
        hash too — cross-seed rows (dest != source) would otherwise always
        report no progress.
        """
        out: dict[str, float] = {}
        for h, item in self._live.items():
            try:
                out[h.lower()] = item.progress
                _src = (getattr(item, "source_infohash", "") or "").lower()
                if _src and _src not in out:
                    out[_src] = item.progress
            except Exception:
                continue
        return out

    def live_speed_map(self) -> dict[str, int]:
        """Snapshot of in-flight download speeds (bytes/s) by infohash.

        Same dest+source aliasing as :meth:`live_progress_map`; unknown
        or odd values read 0 (never negative, never non-numeric). Feeds
        the Telegram active-tasks speed/ETA display.
        """
        out: dict[str, int] = {}
        for h, item in self._live.items():
            try:
                speed = int(getattr(item, "dlspeed_bps", 0) or 0)
            except (TypeError, ValueError):
                speed = 0
            try:
                out[h.lower()] = max(0, speed)
                _src = (getattr(item, "source_infohash", "") or "").lower()
                if _src and _src not in out:
                    out[_src] = max(0, speed)
            except Exception:
                continue
        return out

    def live_totals(self) -> tuple[int, int]:
        """(Total in-flight download bytes/s, live download count).

        Overview aggregate for the Telegram /stats snapshot; same
        guarded coercion as :meth:`live_speed_map`.
        """
        try:
            items = list((getattr(self, "_live", None) or {}).values())
        except Exception:
            return (0, 0)
        total = 0
        for item in items:
            try:
                total += max(0, int(getattr(item, "dlspeed_bps", 0) or 0))
            except (TypeError, ValueError):
                continue
            except Exception:
                continue
        return (total, len(items))

    def _drop_live_for(self, ts: TorrentState) -> None:
        """Pop _live entries owned by this worker (throw-path backstop).

        Expected exits pop their own keys; an unexpected throw between
        insert and pop would otherwise poll a dead hash forever. Ended
        workers own nothing live by definition, so this is idempotent.
        """
        try:
            want = (ts.source_infohash or "").lower()
            live = getattr(self, "_live", None)
            if not want or not isinstance(live, dict) or not live:
                return
            for k in [k for k, v in live.items()
                      if (getattr(v, "source_infohash", "") or "").lower() == want]:
                live.pop(k, None)
        except Exception:
            pass
