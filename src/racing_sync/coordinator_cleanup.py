"""VPS1 cleanup janitor mixin (disk pressure + grace).

Split out of the coordinator god-file. Deletes racing torrents from VPS1
once VPS2 has secured the content (DONE past adaptive grace and idle, or
MOVING under disk pressure with bytes verified on SSD). Fail-closed
throughout: any doubt keeps VPS1.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path

from .clients.abstract import Torrent
from .coordinator_content import cleanup_grace_seconds, normalize_content_name
from .coordinator_paths import _safe_ssd_join
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class CleanupMixin:
    """VPS1 janitor (duck-typed ``self``: cfg/store/source_client/dest_client)."""

    def _cleanup_arrivals_per_hour(self, now_mono: float) -> float:
        """Rolling intake velocity: new racing releases in the trailing hour.

        Spam-uploader bursts raise this; the janitor shortens deletion grace
        accordingly. Prunes entries older than one hour (bounded memory).
        Tolerant of object.__new__ test doubles (missing attribute).
        """
        try:
            arrivals = getattr(self, "_arrival_times", None)
            if not arrivals:
                return 0.0
            cutoff = now_mono - 3600.0
            kept = [t for t in arrivals if t >= cutoff]
            try:
                self._arrival_times = kept
            except Exception:
                pass
            return float(len(kept))
        except Exception:
            return 0.0

    async def _source_free_bytes(self) -> int | None:
        """Free bytes on the VPS1 racing filesystem, or None when unknown.

        Measured over the existing SFTP channel (Deluge sources). Unknown
        covers qBittorrent sources (no SSH channel), reconnect failures and
        statvfs errors — callers degrade to time-only grace, never to zero.
        """
        sftp = getattr(self, "sftp", None)
        if sftp is None:
            return None
        try:
            state_dir = self.cfg.source.deluge_sftp.state_dir  # type: ignore[union-attr]
            path = state_dir.as_posix() if hasattr(state_dir, "as_posix") else str(state_dir)
        except Exception:
            return None
        try:
            free = await asyncio.wait_for(
                asyncio.to_thread(sftp.disk_free_bytes, path),
                timeout=15.0,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: VPS1 free-space probe failed: %s", e)
            return None
        return free if isinstance(free, int) and free >= 0 else None

    async def _maybe_cleanup_source(self) -> None:
        """Hourly gate for the VPS1 cleanup janitor; no-op unless enabled."""
        cfg = getattr(getattr(self, "cfg", None), "cleanup", None)
        if cfg is None or not getattr(cfg, "enabled", False):
            return
        now_mono = time.monotonic()
        try:
            last = float(getattr(self, "_cleanup_last_run", 0.0) or 0.0)
        except (TypeError, ValueError):
            last = 0.0
        try:
            interval = int(getattr(cfg, "janitor_interval_seconds", 3600) or 3600)
        except (TypeError, ValueError):
            interval = 3600
        if now_mono - last < interval:
            return
        self._cleanup_last_run = now_mono
        await self._run_source_cleanup(cfg)

    async def _run_source_cleanup(self, cfg: object) -> None:
        """Delete VPS1 racing torrents whose content VPS2 fully secured.

        Two deletion classes (see [cleanup] docs): normal DONE rows past
        adaptive grace (or verifiably idle), and early MOVING rows under disk
        pressure. Dry-run only logs. Fail-closed throughout: any doubt about
        bytes, health, identity or activity skips the row for this run.
        """
        now_utc = dt.datetime.now(dt.timezone.utc)
        arrivals = self._cleanup_arrivals_per_hour(time.monotonic())
        free_bytes = await self._source_free_bytes()
        grace_s = cleanup_grace_seconds(cfg, free_bytes, arrivals)
        low_free = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
        if free_bytes is not None:
            try:
                crit = int(getattr(cfg, "critical_watermark_free_bytes", 0) or 0)
            except (TypeError, ValueError):
                crit = 0
            if crit and free_bytes <= crit:
                log.warning(
                    "cleanup: VPS1 disk CRITICAL (%d bytes free); grace floored at minimum",
                    free_bytes,
                )
        log.info(
            "cleanup janitor: VPS1 free=%s arrivals=%.1f/h grace=%ds",
            f"{free_bytes} B" if free_bytes is not None else "unknown",
            arrivals, int(grace_s),
        )

        try:
            rows = [ts for ts in self.store.all() if ts.state in (State.DONE, State.MOVING)]
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list state rows: %s", e)
            return
        if not rows:
            return
        try:
            src_torrents = await self.source_client.list_torrents(
                category=self.cfg.source.category
            )
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list VPS1 torrents: %s", e)
            return
        by_hash = {t.infohash.lower(): t for t in src_torrents}

        try:
            cap = max(1, int(getattr(cfg, "per_run_cap", 10) or 10))
        except (TypeError, ValueError):
            cap = 10
        dry_run = bool(getattr(cfg, "dry_run", True))

        candidates: list[tuple[float, float, TorrentState, list[Torrent]]] = []
        for ts in rows:
            try:
                group = self._cleanup_group_for(ts, by_hash)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: group resolve failed for %s: %s",
                            ts.source_infohash[:10], e)
                continue
            if not group:
                continue
            if self._cleanup_protected(ts):
                continue
            try:
                verdict = await self._cleanup_verdict(ts, group, grace_s, now_utc, dry_run, free_bytes)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: verdict failed for %s: %s",
                            ts.source_infohash[:10], e)
                continue
            if verdict is None:
                continue
            candidates.append(verdict)

        if not candidates:
            return
        # Biggest-first under disk pressure (a finished 50GB pack beats ten
        # 3GB episodes when the next race needs a landing zone), else oldest.
        pressured = free_bytes is not None and low_free and free_bytes < low_free
        if pressured:
            candidates.sort(key=lambda c: (-c[1], c[0]))
        else:
            candidates.sort(key=lambda c: (c[0], -c[1]))

        deleted = 0
        for _, _, ts, group in candidates:
            # The cap bounds real deletions; dry-run logs every candidate
            # so rehearsal output matches a live run instead of stopping
            # at phantom counts.
            if not dry_run and deleted >= cap:
                break
            try:
                ok = await self._delete_source_group(ts, group, cfg, dry_run)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: delete failed for %s: %s",
                            ts.source_infohash[:10], e)
                continue
            if ok:
                deleted += 1
        log.info("cleanup janitor: %s %d content group(s)%s",
                 "would delete" if dry_run else "deleted",
                 deleted, f" (cap {cap})" if deleted >= cap else "")

    def _cleanup_group_for(self, ts: TorrentState, by_hash: dict[str, Torrent]) -> list[Torrent]:
        """Live VPS1 torrents holding this row's content.

        Same normalized release name plus matching total size (0-size rows,
        e.g. fresh adoptions, match on name alone). Only live client entries
        are returned — already-vanished torrents need no work.
        """
        want_norm = normalize_content_name(ts.source_name or "")
        if not want_norm:
            return []
        out: list[Torrent] = []
        for t in by_hash.values():
            if normalize_content_name(t.name or "") != want_norm:
                continue
            if ts.total_bytes and t.size_bytes and t.size_bytes != ts.total_bytes:
                continue
            out.append(t)
        return out

    def _cleanup_protected(self, ts: TorrentState) -> bool:
        """True when the release matches a protected pattern (never delete)."""
        try:
            patterns = list(getattr(self.cfg.cleanup, "protected_patterns", []) or [])
        except Exception:
            return False
        name = (ts.source_name or "").lower()
        for pat in patterns:
            try:
                text = str(pat).strip().lower()
                if text and text in name:
                    log.info("cleanup: %s matches protected pattern %r; keeping",
                             ts.source_name[:60], pat)
                    return True
            except Exception:
                continue
        return False

    def _cleanup_member_active(self, t: Torrent, cfg: object) -> bool:
        """True when a VPS1 torrent shows race activity (do not delete).

        Upload rate at/above the threshold OR any leechers attached means the
        swarm is still working — or the client simply doesn't report stats
        (all zeros is "unknown", handled by the caller, not here).
        """
        try:
            thresh = int(getattr(cfg, "activity_upspeed_bps", 65536) or 0)
        except (TypeError, ValueError):
            thresh = 65536
        try:
            return int(t.upspeed_bps or 0) >= thresh or int(t.num_leechers or 0) > 0
        except (TypeError, ValueError):
            return False

    def _cleanup_minimums_met(self, group: list[Torrent], cfg: object) -> bool:
        """H&R minimums across every group member (0 = disabled).

        Conservative AND-semantics: every copy must satisfy ratio/seed-time,
        since each client entry carries its own tracker obligation. Unknown
        stats (<=0) fail closed when the corresponding minimum is enabled.
        """
        try:
            min_ratio = float(getattr(cfg, "min_ratio", 0.0) or 0.0)
        except (TypeError, ValueError):
            min_ratio = 0.0
        try:
            min_seed_s = float(getattr(cfg, "min_seed_hours", 0.0) or 0.0) * 3600.0
        except (TypeError, ValueError):
            min_seed_s = 0.0
        if min_ratio <= 0 and min_seed_s <= 0:
            return True
        for m in group:
            try:
                ratio = float(m.ratio or 0.0)
            except (TypeError, ValueError):
                ratio = 0.0
            try:
                seed_s = float(m.seeding_time_seconds or 0)
            except (TypeError, ValueError):
                seed_s = 0.0
            if min_ratio > 0 and ratio < min_ratio:
                return False
            if min_seed_s > 0 and seed_s < min_seed_s:
                return False
        return True

    async def _cleanup_verdict(
        self, ts: TorrentState, group: list[Torrent],
        grace_s: float, now_utc: dt.datetime, dry_run: bool,
        free_bytes: int | None,
    ) -> tuple[float, float, TorrentState, list[Torrent]] | None:
        """Decide deletion for one row; None = keep this run.

        Returns (completed_sort_key, size_bytes, ts, group) for candidates.
        Updates vps1_last_activity_at when swarm activity is observed (skipped
        in dry-run mode, which performs zero state mutations).
        """
        cfg = self.cfg.cleanup
        # 1. Observe swarm activity first: any active member bumps the stamp
        #    and vetoes deletion this run (the race is still going).
        lively = [m for m in group if self._cleanup_member_active(m, cfg)]
        if lively:
            if not dry_run:
                ts.vps1_last_activity_at = now_utc
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
            log.debug("cleanup: %s still active on VPS1 (%d/%d racing); keeping",
                      ts.source_name[:60], len(lively), len(group))
            return None
        # 2. Opt-in H&R minimums (default off — same-account fuse seeding
        #    keeps the account compliant; see [cleanup] docs).
        if not self._cleanup_minimums_met(group, cfg):
            return None
        # 3. State-specific safety + timing.
        completed = ts.completed_at
        age_s = (now_utc - completed).total_seconds() if completed else 0.0
        if ts.state == State.DONE:
            if not await self._cleanup_fuse_healthy(ts):
                return None
            if age_s < grace_s and not self._cleanup_idle_confirmed(ts, group, now_utc, cfg):
                return None
        elif ts.state == State.MOVING:
            # Early pressure path: bytes are 100% on the VPS2 SSD (MOVING is
            # only entered after full completion) but not yet moved. Allowed
            # exclusively under disk pressure with an idle swarm, and only
            # while the SSD bytes are verifiably still present.
            try:
                low_free = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
            except (TypeError, ValueError):
                low_free = 0
            if not (low_free and free_bytes is not None and free_bytes < low_free):
                return None
            if not self._cleanup_idle_confirmed(ts, group, now_utc, cfg):
                return None
            if not await self._cleanup_ssd_bytes_present(ts):
                return None
        else:
            return None
        sort_key = completed.timestamp() if completed else now_utc.timestamp()
        if ts.total_bytes:
            size = float(ts.total_bytes)
        else:
            # Fresh adoptions carry no total: dedupe by save_path so
            # cross-seeds sharing the same on-disk files don't multiply.
            by_path: dict[str, int] = {}
            for m in group:
                key = m.save_path or ""
                by_path[key] = max(by_path.get(key, 0), int(m.size_bytes or 0))
            size = float(sum(by_path.values()) or 0)
        return (sort_key, size, ts, group)

    def _cleanup_idle_confirmed(
        self, ts: TorrentState, group: list[Torrent],
        now_utc: dt.datetime, cfg: object,
    ) -> bool:
        """Fast lane: swarm quiet now (checked by caller) AND quiet long enough.

        Primary evidence is a previously recorded activity stamp older than
        the confirm window. Fallback for rows that were never observed
        active (e.g. pre-existing idle backlog): every member's client
        `added_on` predates the window — a torrent cannot have raced before
        it existed, so old + quiet now means finished. Unknown `added_on`
        (<=0) fails closed and waits out the grace period instead.
        """
        try:
            window_s = float(getattr(cfg, "idle_confirm_minutes", 45) or 0) * 60.0
        except (TypeError, ValueError):
            window_s = 45.0 * 60.0
        if window_s <= 0:
            return True
        last = ts.vps1_last_activity_at
        if last is not None:
            try:
                return (now_utc - last).total_seconds() >= window_s
            except Exception:
                return False
        if not group:
            return False
        try:
            # Cross-host clock skew guard: VPS1 added_on is compared against
            # VPS2 now. Future timestamps (VPS1 ahead) and near-epoch values
            # (bogus/unknown, e.g. 1) fail closed instead of fast-laning.
            now_ts = now_utc.timestamp()
            youngest_added = 0
            for m in group:
                added = int(m.added_on or 0)
                if added <= 1_000_000_000:
                    return False
                if added > now_ts + 300:
                    return False
                youngest_added = max(youngest_added, added)
            added_dt = dt.datetime.fromtimestamp(youngest_added, tz=dt.timezone.utc)
            return (now_utc - added_dt).total_seconds() >= window_s
        except Exception:
            return False

    async def _cleanup_fuse_healthy(self, ts: TorrentState) -> bool:
        """Re-verify the row really seeds from fuse (entries AND bytes).

        Single batched entry lookup by hashes, plus a file-presence check at
        the fuse target whenever the torrent bytes are available. Entries
        alone are not proof (a skip_check entry reports complete with zero
        bytes). Any doubt fails closed: keep VPS1.
        """
        hashes = {
            h.lower() for h in (
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            ) if h
        }
        if not hashes:
            log.warning("cleanup: %s has no recorded fuse hashes; keeping VPS1",
                        ts.source_name[:60])
            return False
        try:
            present = await self.dest_client.list_torrents(hashes=list(hashes))
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: dest lookup failed for %s: %s",
                        ts.source_infohash[:10], e)
            return False
        have = {t.hash.lower() for t in (present or [])}
        missing = sorted(hashes - have)
        if missing:
            log.warning("cleanup: %d fuse entr%s missing for %s (%s…); keeping VPS1",
                        len(missing), "y" if len(missing) == 1 else "ies",
                        ts.source_name[:60], missing[0][:10])
            return False
        blob = ts._blob or ts.cross_seed_blob
        if not blob and getattr(self, "store", None) is not None:
            try:
                blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                blob = None
        expected = self._expected_fuse_files(blob)
        if not expected:
            # No bytes to verify against (e.g. adopted rows predate blob
            # persistence): entries existing is the best available signal.
            return True
        try:
            target = self._target_mount_for_blob(blob, self._target_mount_for(ts))
        except Exception:  # noqa: BLE001
            return False
        try:
            file_missing = await self._missing_fuse_files(target, expected)
        except Exception:  # noqa: BLE001
            return False
        if file_missing:
            log.warning("cleanup: %d fuse file(s) missing for %s at %s; keeping VPS1",
                        len(file_missing), ts.source_name[:60], target)
            return False
        return True

    async def _cleanup_ssd_bytes_present(self, ts: TorrentState) -> bool:
        """Confirm the MOVING row's bytes are still on the VPS2 SSD.

        Guards the early pressure path: if the SSD bytes vanished (manual
        wipe, failed worker cleanup), deleting VPS1 would leave no copy
        anywhere mid-pipeline.
        """
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        if not h:
            return False
        try:
            files = await self.dest_client.get_torrent_files(h)
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list SSD files for %s: %s",
                        ts.source_infohash[:10], e)
            return False
        expected = [
            (f.name, f.size_bytes, getattr(f, "progress", None))
            for f in (files or []) if getattr(f, "name", "")
        ]
        if not expected:
            return False
        base = Path(ts.save_path) if ts.save_path else Path(self.cfg.dest.save_path)
        for name, want, progress in expected:
            try:
                p = _safe_ssd_join(base, name)
                if p is None:
                    return False
                if not p.exists():
                    return False
                # Size alone is not proof: qB pre-allocates deselected files
                # at full size. Require client-verified progress when known.
                if isinstance(progress, (int, float)) and want:
                    if progress < 0.999:
                        return False
                if want and p.stat().st_size < want:
                    return False
            except OSError:
                return False
        return True

    async def _delete_source_group(
        self, ts: TorrentState, group: list[Torrent], cfg: object, dry_run: bool
    ) -> bool:
        """Atomically delete one content group from the VPS1 client.

        Members sharing a save_path hold the same on-disk files: entries for
        all of them are removed while files are deleted exactly once (first
        member). Distinct save_paths are handled as independent partitions.
        Any failure aborts that partition for retry next run.
        """
        try:
            delete_files = bool(getattr(cfg, "delete_files", True))
        except (TypeError, ValueError):
            delete_files = True
        # Partitioned by (save_path, size): same-directory same-size
        # members are cross-seeds sharing files (deleted exactly once via
        # the first member); different sizes are different files even under
        # one directory (0-size rows match on name alone) and each
        # partition's files must go, or entries vanish while data leaks.
        parts: dict[tuple[str, int], list[Torrent]] = {}
        for m in group:
            try:
                size_key = int(m.size_bytes or 0)
            except (TypeError, ValueError):
                size_key = 0
            parts.setdefault((m.save_path or "", size_key), []).append(m)
        ok = True
        for (save_path, _size_key), members in parts.items():
            hashes = [m.infohash for m in members]
            # Members sharing a save_path hold the same on-disk files
            # (deleted exactly once via the first member), so the freed
            # size is one copy, not the sum over cross-seeds.
            total_mb = (max((m.size_bytes or 0) for m in members) if members else 0) // (1024 * 1024)
            if dry_run:
                log.info(
                    "cleanup dry-run: would delete %d VPS1 torrent(s) for %s "
                    "(~%d MB, save_path=%s, delete_files=%s)",
                    len(hashes), ts.source_name[:60], total_mb,
                    save_path or "?", delete_files,
                )
                continue
            log.warning(
                "cleanup: deleting %d VPS1 torrent(s) for %s (~%d MB, save_path=%s)",
                len(hashes), ts.source_name[:60], total_mb, save_path or "?",
            )
            first, rest = hashes[0], hashes[1:]
            try:
                await self.source_client.delete(first, delete_files=delete_files)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: delete %s failed: %s", first[:10], e)
                ok = False
                continue
            for h in rest:
                try:
                    await self.source_client.delete(h, delete_files=False)
                except Exception as e:  # noqa: BLE001
                    log.warning("cleanup: delete entry %s failed: %s", h[:10], e)
                    ok = False
        return ok


__all__ = ["CleanupMixin"]
