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

#: Central dest-poll cadence (seconds): one chunked list_torrents every
#: pass serves all workers (O(polls), not O(workers x polls)).
_QB_LISTENER_INTERVAL_S = 4.0
#: Snapshot older than this is stale: workers fall back to a direct
#: per-row fetch (listener down/disabled must not blind them).
_LIVE_SNAPSHOT_STALE_S = 10.0


class LiveStatusMixin:
    """In-flight download tracking (duck-typed ``self``)."""

    async def _refresh_live_status(self) -> list:
        """Re-query VPS2 progress and update the live map.

        Chunks fetch in parallel (4-wide, each RPC-bounded): 10k live
        hashes no longer pay 100 serial round-trips per scheduler tick.
        One chunk failing aborts the refresh like the old serial loop —
        the next tick retries; partial rows are never applied. Returns
        the fetched rows and stashes them (`_live_last`) with a timestamp
        so workers read the central snapshot instead of polling per-row.
        """
        if not self._live:
            try:
                self._live_last = {}
                self._live_last_at = 0.0
            except Exception:
                pass
            return []
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
            return []
        rows = [t for _part in _parts for t in _part]
        try:
            _snap: dict[str, object] = {}
            for t in rows:
                try:
                    _snap[str(getattr(t, "hash", "") or "").lower()] = t
                except Exception:
                    continue
            self._live_last = _snap
            try:
                import time as _time
                self._live_last_at = _time.monotonic()
            except Exception:
                pass
        except Exception:
            pass
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
        return rows

    def _live_snapshot_for(self, dest_hash: str) -> object | None:
        """Central snapshot row for a dest hash, or None when stale/absent.

        Workers prefer this over a per-row `get_torrent` RPC; a missing
        or older-than-stale snapshot means the listener is down and the
        worker falls back to a direct fetch (never blind).
        """
        try:
            import time as _time
            _at = float(getattr(self, "_live_last_at", 0.0) or 0.0)
            if _time.monotonic() - _at > _LIVE_SNAPSHOT_STALE_S:
                return None
            _snap = getattr(self, "_live_last", None)
            if not isinstance(_snap, dict):
                return None
            return _snap.get((dest_hash or "").lower())
        except Exception:
            return None

    def _consume_live_verdict(self, dest_hash: str) -> str | None:
        """Pop a pending central verdict for a dest hash. Never raises."""
        try:
            _v = getattr(self, "_live_verdicts", None)
            if not isinstance(_v, dict):
                return None
            return _v.pop((dest_hash or "").lower(), None)
        except Exception:
            return None

    async def _qb_listener_pass(self) -> None:
        """One central dest poll: refresh + detect + notify.

        Verdicts (`complete`/`error`/`missingFiles`/`stalled`) land in
        `_live_verdicts` by dest hash and the row's TaskHandles get their
        event set so workers wake instead of sleeping out poll intervals.
        Workers consume `complete` as a skip-RPC fast path; every other
        verdict only wakes them (per-row gates — tracker health, stall
        watchdog, byte-on-disk — still decide in the worker, which keeps
        the second gate). Never raises.
        """
        try:
            if getattr(self, "_stop", False):
                return
            try:
                _dest = getattr(self, "dest_client", None)
            except Exception:
                _dest = None
            if _dest is None or not getattr(self, "_live", None):
                return
            rows = await self._refresh_live_status()
            if not rows:
                return
            try:
                import time as _time
                _now = _time.monotonic()
            except Exception:
                _now = 0.0
            try:
                _stall_timeout = float(getattr(
                    getattr(getattr(self, "cfg", None), "general", None),
                    "download_stall_timeout_seconds", 0) or 0)
            except (TypeError, ValueError):
                _stall_timeout = 0.0
            except Exception:
                _stall_timeout = 0.0
            try:
                _verdicts = getattr(self, "_live_verdicts", None)
                if not isinstance(_verdicts, dict):
                    _verdicts = {}
                    self._live_verdicts = _verdicts  # type: ignore[attr-defined]
            except Exception:
                _verdicts = {}
            try:
                _still = getattr(self, "_live_still", None)
                if not isinstance(_still, dict):
                    _still = {}
                    self._live_still = _still  # type: ignore[attr-defined]
            except Exception:
                _still = {}
            for t in rows:
                try:
                    _dh = str(getattr(t, "hash", "") or "").lower()
                    if not _dh:
                        continue
                    _item = (getattr(self, "_live", None) or {}).get(_dh)
                    _src = ((getattr(_item, "source_infohash", "") or "")
                            .lower()) if _item is not None else ""
                    _state = str(getattr(t, "state", "") or "").lower()
                    try:
                        _prog = float(getattr(t, "progress", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        _prog = 0.0
                    try:
                        _done = bool(getattr(t, "is_complete", None)
                                     and t.is_complete()) or _prog >= 1.0
                    except Exception:
                        _done = _prog >= 1.0
                    _verdict: str | None = None
                    if _done:
                        _verdict = "complete"
                    elif "missingfiles" in _state.replace(" ", ""):
                        _verdict = "missingFiles"
                    elif "error" in _state:
                        _verdict = "error"
                    else:
                        try:
                            _prev = _still.get(_dh)
                            if (isinstance(_prev, (list, tuple))
                                    and len(_prev) == 2
                                    and abs(float(_prev[0]) - _prog) < 1e-9):
                                if (_stall_timeout > 0 and _now - float(
                                        _prev[1]) > _stall_timeout):
                                    _verdict = "stalled"
                            else:
                                _still[_dh] = [_prog, _now]
                        except Exception:
                            pass
                    if _verdict is not None:
                        try:
                            _verdicts[_dh] = _verdict
                        except Exception:
                            pass
                        if _src:
                            try:
                                from .task import handles_for
                                for _hd in handles_for(self, _src):
                                    try:
                                        _hd.event.set()
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                except Exception:
                    continue
        except Exception:
            pass

    async def _qb_listener_loop(self) -> None:
        """Supervised central dest-poll loop (every ~4s). Never raises."""
        try:
            while not getattr(self, "_stop", False):
                try:
                    await self._qb_listener_pass()
                except asyncio.CancelledError:
                    return
                except Exception as e:  # noqa: BLE001
                    try:
                        log.debug("qb listener pass failed: %s", e)
                    except Exception:
                        pass
                try:
                    await asyncio.sleep(_QB_LISTENER_INTERVAL_S)
                except asyncio.CancelledError:
                    return
                except Exception:
                    pass
        except asyncio.CancelledError:
            return
        except Exception:
            pass

    def live_progress_map(self) -> dict[str, float]:
        """Snapshot of in-flight download progress keyed by infohash.

        Used by the Telegram bot for the active-tasks message. Entries are
        keyed by dest hash for client polling, so alias each to its source
        hash too — cross-seed rows (dest != source) would otherwise always
        report no progress.
        """
        out: dict[str, float] = {}
        try:
            _items = list((getattr(self, "_live", None) or {}).items())
        except Exception:
            return out
        for h, item in _items:
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
        try:
            _items = list((getattr(self, "_live", None) or {}).items())
        except Exception:
            return out
        for h, item in _items:
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
