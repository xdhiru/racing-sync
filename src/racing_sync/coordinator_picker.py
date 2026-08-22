"""Cross-seed SSD-source picker (req #1 / #2).

Stateless choice of which ``.torrent`` bytes feed the VPS2 SSD download.
Split out of the coordinator god-file verbatim; re-exported from
``racing_sync.coordinator`` for backwards compatibility.
"""

from __future__ import annotations

import asyncio
import logging

from .clients.abstract import Torrent, TorrentClient
from .clients.deluge import DelugeClient
from .config import AppConfig
from .coordinator_content import (
    SourceDecision,
    _looks_public,
    _verified_cross_seed_blob,
    indexer_slug,
)
from .prowlarr import ProwlarrClient
from .sftp_source import SFTPExporter

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


async def pick_ssd_source_for_racing(
    *,
    cfg: AppConfig,
    source_torrent: Torrent,
    other_source_torrents: list[Torrent],
    prowlarr: ProwlarrClient | None,
    sftp: SFTPExporter | None,
    source_client: TorrentClient,
    attempt_prowlarr: bool = True,
) -> SourceDecision | None:
    """Decide which .torrent bytes to feed VPS2 SSD.

    Returns:
      - SourceDecision if we have a candidate right now, OR
      - None if we should park the row in WAITING_INDEXER and retry later
        (only when `attempt_prowlarr=True` and we had a real miss on the
        download-target indexers; otherwise we fall through to the SFTP
        fallback even when the indexers returned no hit).

    Logic per req #1 / #2:
      (a) one or more *public* torrents for the file on VPS1   → use the
                                                               racing client's
                                                               own .torrent for
                                                               SSD download.
                                                               Prowlarr is
                                                               NEVER queried
                                                               in this case.
      (b) only *private* torrents (per tracker_map)            → query the
                                                               configured
                                                               download-target
                                                               indexers in
                                                               priority order
                                                               for a cross-seed
                                                               copy
    """

    # If the source torrent is itself a "public" tracker, we use IT for
    # the SSD download directly. We do NOT consult Prowlarr — the racing
    # public torrent already works, fetching a download-indexer copy would
    # be redundant. The only exception is the rare case where the racing
    # client's .torrent is unreachable on VPS1 (then refetch_public_via_prowlarr
    # can fall back to Prowlarr as a last resort).
    publics = [t for t in [source_torrent] + other_source_torrents
               if _looks_public(t.trackers)]

    if publics:
        chosen = publics[0]
        # req #1: when a public torrent exists on VPS1, use IT for the
        # SSD download directly. We do NOT consult Prowlarr by default
        # — the racing public torrent already works, fetching a
        # download-indexer copy would be redundant.
        #
        # The racing-client torrent's .torrent bytes are obtained either
        # via qBittorrent's `/api/v2/torrents/export` endpoint (handled
        # by the source_client.export_torrent() helper) or via SFTP from
        # the Deluge state directory.
        if cfg.cross_seed.allow_ssh_export and sftp is not None:
            log.info(
                "public racing torrent present; "
                "SFTP-exporting %s from VPS1 for SSD download",
                chosen.infohash[:10],
            )
            blob = None
            # One retry on timeout: workers share a single SFTP connection
            # behind a lock, so bursts can stall one fetch past the budget.
            # A clean miss (file absent) returns None and is not retried.
            for attempt in (1, 2):
                try:
                    blob = await asyncio.wait_for(
                        asyncio.to_thread(sftp.fetch_torrent, chosen.infohash),
                        timeout=15.0,
                    )
                    break
                except TimeoutError:
                    log.warning("sftp fetch %s timed out after 15s (attempt %d/2)",
                                chosen.infohash[:10], attempt)
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp fetch %s failed: %s", chosen.infohash[:10], e)
                    break
            if blob:
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label="public-racing",
                    name=chosen.name,
                    size_bytes=chosen.size_bytes,
                    infohash=chosen.infohash,
                    announce_url=(
                        chosen.trackers[0]
                        if chosen.trackers else ""
                    ),
                )
        # If allow_ssh_export=false (or SFTP returned nothing for the
        # source_infohash), try the source client's own export endpoint
        # (qBittorrent /torrents/export; Deluge daemons have no equivalent
        # RPC, so a failure there after an SFTP miss is routine).
        try:
            blob = await asyncio.wait_for(
                source_client.export_torrent(chosen.infohash),
                timeout=15.0,
            )
        except AttributeError:
            blob = None
        except Exception as e:  # noqa: BLE001
            log.warning("source export_torrent failed for %s: %s",
                        chosen.infohash[:10], e)
            blob = None
        if blob:
            log.info(
                "public racing torrent present; "
                "fetched %s via qB export endpoint for SSD download",
                chosen.infohash[:10],
            )
            return SourceDecision(
                torrent_bytes=blob,
                source_label="public-racing",
                name=chosen.name,
                size_bytes=chosen.size_bytes,
                infohash=chosen.infohash,
                announce_url=(
                    chosen.trackers[0]
                    if chosen.trackers else ""
                ),
            )

        # Last resort in the public branch: only if the user explicitly
        # asked for it, fetch a Prowlarr cross-seed copy. This is rare
        # and intended for cases where the racing client's torrent file
        # is unreachable (e.g. VPS1 crash mid-cycle).
        if (cfg.cross_seed.refetch_public_via_prowlarr
                and cfg.cross_seed.allow_prowlarr_cross_seed
                and prowlarr is not None
                and not cfg.prowlarr.should_skip_title(chosen.name)):
            log.warning(
                "racing client's public .torrent unavailable; "
                "falling back to Prowlarr cross-seed for %s",
                chosen.name,
            )
            try:
                hit = await prowlarr.best_match(
                    chosen.name, target_size=chosen.size_bytes,
                    indexers=prowlarr.get_download_indexers(),
                )
            except Exception as e:  # noqa: BLE001
                log.warning("download-indexer search failed for %s: %s",
                            chosen.name, e)
                hit = None
            verified = None
            hit_size = 0
            if hit:
                hit_size = hit.size_bytes
                blob = await prowlarr.download_torrent(hit)
                verified = _verified_cross_seed_blob(
                    blob, target_name=chosen.name,
                    target_size=chosen.size_bytes, hit_title=hit.title,
                )
            if verified is not None:
                blob, real_hash, announce = verified
                return SourceDecision(
                    torrent_bytes=blob,
                    source_label=f"public-{indexer_slug(hit.indexer)}-fallback",
                    name=chosen.name,
                    size_bytes=hit_size,
                    infohash=real_hash,
                    announce_url=announce or (chosen.trackers[0] if chosen.trackers else ""),
                )

        log.warning(
            "public torrent present but could not export .torrent for %s (sftp=%s, qb=%s)",
            chosen.name,
            cfg.cross_seed.allow_ssh_export and sftp is not None,
            source_client is not None,
        )
        return None

    # All torrents are private. Preferred SSD source is a cross-seed from the
    # configured download-target indexers (tried in priority order) — unless
    # the title matches skip_query_substrings (a query can never match, so
    # don't park for one) or Prowlarr is unavailable/disabled. Those cases
    # fall straight through to the direct-export fallback below instead of
    # entering the WAITING_INDEXER retry loop for a query that will never
    # run. Private torrents are never downloaded directly on SSD unless
    # allow_ssh_export (or the source export endpoint) is used.
    should_skip_prowlarr = cfg.prowlarr.should_skip_title(source_torrent.name)
    can_query_prowlarr = (
        prowlarr is not None
        and cfg.cross_seed.allow_prowlarr_cross_seed
        and not should_skip_prowlarr
    )
    if should_skip_prowlarr:
        log.info(
            "prowlarr: skipping cross-seed query for %r (matches skip_query_substrings); "
            "using the racing torrent's own bytes",
            source_torrent.name,
        )
    elif can_query_prowlarr and attempt_prowlarr:
        log.info(
            "private release; querying Prowlarr download-target indexer(s) %s for cross-seed of %s",
            cfg.prowlarr.download_indexer_names,
            source_torrent.name,
        )
        try:
            hit = await prowlarr.best_match(
                source_torrent.name, target_size=source_torrent.size_bytes,
                indexers=prowlarr.get_download_indexers(),
            )
        except Exception as e:  # noqa: BLE001
            log.warning("download-indexer search failed for %s: %s",
                        source_torrent.name, e)
            hit = None
        verified = None
        hit_size = 0
        if hit:
            hit_size = hit.size_bytes
            log.info(
                "prowlarr hit: %s (size=%d B, indexer=%s)",
                hit.title, hit.size_bytes, hit.indexer,
            )
            blob = await prowlarr.download_torrent(hit)
            verified = _verified_cross_seed_blob(
                blob, target_name=source_torrent.name,
                target_size=source_torrent.size_bytes, hit_title=hit.title,
            )
        if verified is not None:
            blob, real_hash, announce = verified
            return SourceDecision(
                torrent_bytes=blob,
                source_label=f"{indexer_slug(hit.indexer)}-cross-seed",
                name=source_torrent.name,
                size_bytes=hit_size,
                infohash=real_hash,
                announce_url=announce or (source_torrent.trackers[0] if source_torrent.trackers else ""),
            )
        if attempt_prowlarr:
            log.info(
                "no prowlarr cross-seed yet for %s; will park and retry",
                source_torrent.name,
            )
            return None

    # Direct-export fallback: VPS2 leeches the racing private torrent's own
    # bytes. Reached when the Prowlarr query is skipped by config,
    # unavailable/disabled, or bypassed by the caller — never after a real
    # query that merely missed with attempt_prowlarr=True (that parks above).
    if cfg.cross_seed.allow_ssh_export:
        blob = None
        label = ""
        if sftp is not None:
            log.info(
                "Prowlarr bypass/fallback: SFTP-exporting private torrent %s from VPS1",
                source_torrent.infohash[:10],
            )
            # One retry on timeout (same rationale as the public branch:
            # shared transports stall single calls past the budget; a
            # clean miss returns None and is not retried).
            for attempt in (1, 2):
                try:
                    blob = await asyncio.wait_for(
                        asyncio.to_thread(sftp.fetch_torrent, source_torrent.infohash),
                        timeout=15.0,
                    )
                    break
                except TimeoutError:
                    log.warning("sftp fallback fetch %s timed out after 15s (attempt %d/2)",
                                source_torrent.infohash[:10], attempt)
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp fallback fetch %s failed: %s", source_torrent.infohash[:10], e)
                    break
            if blob:
                label = "private-sftp-fallback"
        if blob is None:
            # qBittorrent sources expose every torrent via /torrents/export;
            # Deluge sources rely on SFTP (their RPC has no torrent-file
            # method), so a failure here after an SFTP miss is routine.
            try:
                blob = await asyncio.wait_for(
                    source_client.export_torrent(source_torrent.infohash),
                    timeout=15.0,
                )
            except AttributeError:
                blob = None
            except Exception as e:  # noqa: BLE001
                if sftp is not None and isinstance(source_client, DelugeClient):
                    log.debug("private export fallback unavailable for %s: %s",
                              source_torrent.infohash[:10], e)
                else:
                    log.warning("private export fallback fetch %s failed: %s",
                                source_torrent.infohash[:10], e)
                blob = None
            if blob:
                label = "private-export-fallback"
        if blob:
            return SourceDecision(
                torrent_bytes=blob,
                source_label=label,
                name=source_torrent.name,
                size_bytes=source_torrent.size_bytes,
                infohash=source_torrent.infohash,
                announce_url=(
                    source_torrent.trackers[0]
                    if source_torrent.trackers else ""
                ),
            )

    log.warning(
        "no public torrent and no download-target indexer cross-seed available for %s",
        source_torrent.name,
    )
    return None


__all__ = ["pick_ssd_source_for_racing"]
