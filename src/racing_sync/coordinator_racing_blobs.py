"""Bulk racing-torrent warm-up: cache VPS1 .torrent bytes in one pass.

The racing box is reached over a flaky SFTP link: one read per racing
torrent makes every injection depend on its own round trip, and a single
flaky read costs the racing seed. `SFTPExporter.fetch_all_torrents()`
streams every `.torrent` the racing box knows in ONE tar pass over the
existing SSH session; this mixin turns that stream into a DB cache
(`torrent_blob`) of the bytes the racing torrents actually need, so the
later injections never need the link at all.

The transport is SYNC and blocking: it runs on a worker thread, and the
warm-up never raises — a bulk fetch failure must not disturb the other
lanes.
"""

from __future__ import annotations

import asyncio
import logging

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

#: Default cadence of the bulk warm-up when the config omits the key.
_DEFAULT_BULK_FETCH_INTERVAL_S = 900

#: Sleep step of the wait between passes: the wait is chopped up so a
#: stop request is honoured within a second, not after the full interval.
_BULK_FETCH_SLEEP_STEP_S = 1.0


class RacingBlobMixin:
    """Bulk racing-torrent byte caching (duck-typed ``self``)."""

    def _racing_blob_settings(self) -> tuple[bool, int]:
        """(enabled, interval_seconds) for the bulk warm-up.

        Read defensively: the config keys may be absent (older config
        files) and a qB source has no `[source.deluge_sftp]` section at
        all. A missing section or a non-numeric interval disables the
        loop; an interval of `0` disables it explicitly.
        """
        try:
            section = getattr(
                getattr(getattr(self, "cfg", None), "source", None), "deluge_sftp", None
            )
            if section is None:
                return False, 0
            if not bool(getattr(section, "bulk_fetch_enabled", True)):
                return False, 0
            raw = getattr(section, "bulk_fetch_interval_seconds", _DEFAULT_BULK_FETCH_INTERVAL_S)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return False, 0
            interval = int(raw)
        except (AttributeError, TypeError, ValueError):
            return False, 0
        if interval <= 0:
            return False, 0
        return True, interval

    async def _racing_blob_wanted(self) -> set[str]:
        """Lowercase infohashes of the racing torrents VPS1 knows now."""
        wanted: set[str] = set()
        for t in await self._list_source_torrents() or []:
            h = str(getattr(t, "infohash", "") or "").lower()
            if h:
                wanted.add(h)
        return wanted

    async def _racing_blob_warmup(self) -> None:
        """One bulk pass: cache the bytes of every wanted racing torrent.

        The bulk stream carries every `.torrent` the racing box has; only
        the racing torrents are wanted here, so the rest is dropped. Every
        candidate is validated against its own infohash before it is
        cached: bytes that belong to another torrent are never stored
        (they would re-add the wrong hash and hide the racing seed).

        Never raises: a bulk fetch failure is a warning, not an error the
        lanes have to care about.
        """
        try:
            fetch = getattr(getattr(self, "sftp", None), "fetch_all_torrents", None)
            if not callable(fetch):
                log.debug("racing blob warm-up skipped: no bulk torrent source")
                return
            try:
                wanted = await self._racing_blob_wanted()
            except Exception as e:  # noqa: BLE001
                log.warning("racing blob warm-up: could not list racing torrents: %s", e)
                return
            if not wanted:
                log.debug("racing blob warm-up: no racing torrents to cache")
                return
            blobs = await asyncio.to_thread(fetch)
            cached = 0
            for raw_hash, blob in list((blobs or {}).items()):
                h = str(raw_hash or "").lower()
                if h not in wanted:
                    continue
                if not isinstance(blob, (bytes, bytearray)) or not blob:
                    continue
                candidate = bytes(blob)
                matched = self._blob_matches_infohash(candidate, h)
                if matched is not True:
                    if matched is False:
                        log.warning(
                            "racing blob warm-up: bytes for %s belong to a different "
                            "torrent; not caching them",
                            h[:10],
                        )
                    else:
                        log.debug(
                            "racing blob warm-up: bytes for %s are not a decodable "
                            "torrent; not caching them",
                            h[:10],
                        )
                    continue
                try:
                    await asyncio.to_thread(self.store.set_racing_blob, h, candidate)
                except Exception as e:  # noqa: BLE001
                    log.warning("racing blob warm-up: could not cache bytes for %s: %s", h[:10], e)
                    continue
                cached += 1
            log.info(
                "racing blob warm-up: %d/%d racing torrent(s) cached from the bulk fetch",
                cached,
                len(wanted),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("racing blob warm-up failed: %s", e)

    async def _racing_blob_loop(self) -> None:
        """Periodic bulk warm-up of the racing torrent bytes.

        Disabled by config (`bulk_fetch_enabled = false` or
        `bulk_fetch_interval_seconds = 0`): the loop then does nothing at
        all. Otherwise one pass runs and the wait is chopped into short
        steps, so a stop request is honoured quickly. Never raises.
        """
        try:
            enabled, interval = self._racing_blob_settings()
        except Exception:  # noqa: BLE001
            enabled, interval = False, 0
        if not enabled:
            log.debug("racing blob warm-up disabled; not running")
            return
        while not getattr(self, "_stop", False):
            try:
                await self._racing_blob_warmup()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("racing blob warm-up pass failed: %s", e)
            if getattr(self, "_stop", False):
                break
            remaining = float(interval)
            while remaining > 0 and not getattr(self, "_stop", False):
                step = min(_BULK_FETCH_SLEEP_STEP_S, remaining)
                try:
                    await asyncio.sleep(step)
                except asyncio.CancelledError:
                    raise
                remaining -= step
