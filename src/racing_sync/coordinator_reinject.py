"""RE_ADDING re-injection: watch-dir, racing, tracker_map supplements.

Split out of coordinator_readd: the ``_re_inject_*`` entry points that
put watch-dir drops, racing-client torrents, and Prowlarr tracker_map
supplements onto the fuse mount with skip_check=True, always behind a
per-blob fuse gate. Calls back into the coordinator through ``self``
only; no import of coordinator_readd (no cycle).
"""

from __future__ import annotations

import asyncio
import logging
import time

from .content_keys import same_content as _same_content_key, same_release_name as _same_release_name
from .coordinator_content import _matches_release, _verified_cross_seed_blob
from .coordinator_errors import _NOT_VISIBLE_DETAIL, AbandonedError, WebUIUnresponsiveError
from .coordinator_fuse import _is_duplicate_add_detail, _record_injected_hash
from .coordinator_inject import _group_is_ignored
from .coordinator_paths import _watch_cross_seed_dir
from .state import TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class ReinjectMixin:
    """RE_ADDING fuse re-injection + supplements (duck-typed ``self``)."""

    async def _re_inject_watch_dir_torrents(self, ts: TorrentState) -> None:
        """Re-add every watch-dir dropped torrent and discovered cross-seeds onto FUSE."""
        watch_cross_dir = _watch_cross_seed_dir(self.rt.paths.state_db, ts.source_infohash)
        if watch_cross_dir is None:
            log.warning(
                "re-inject watch-dir torrents for %s: no blob dir "
                "(unsafe infohash or in-memory DB); skipping",
                ts.source_name[:50],
            )
            return
        if not watch_cross_dir.exists():
            log.warning(
                "re-inject watch-dir torrents for %s: blob dir %s missing; skipping",
                ts.source_name[:50],
                watch_cross_dir,
            )
            return
        injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        injected_set = set(injected)

        try:
            paths = sorted(watch_cross_dir.glob("*.torrent"))
        except Exception as e:  # noqa: BLE001
            log.warning("re-inject watch-dir list failed for %s: %s", ts.source_name[:50], e)
            return

        async def _load(p):
            """Read + decode + fuse-gate one drop (no state change)."""
            try:
                blob = await asyncio.to_thread(p.read_bytes)
            except Exception as e:  # noqa: BLE001
                return (p, None, f"failed to read ({e})")
            try:
                from .watchdir import _bencoded_info_hash

                h, _, _, _ = _bencoded_info_hash(blob)
                h_low = (h or "").lower()
            except Exception:
                return (p, None, "undecodable .torrent")
            if not h_low:
                return (p, None, "undecodable .torrent")
            # Fuse gate (fail-closed): never point a re-added torrent at
            # missing/unverifiable data.
            target_mount = self._target_mount_for_ts_blob(ts, blob)
            watch_expected = self._expected_fuse_files(blob)
            if not watch_expected:
                return (p, (h_low, None, None), "cannot decode file list; skipping blind injection")
            watch_missing = await self._missing_fuse_files(target_mount, watch_expected)
            if watch_missing:
                return (
                    p,
                    (h_low, None, None),
                    f"fuse content missing at {target_mount} "
                    f"({len(watch_missing)} files); skipping blind injection",
                )
            return (p, (h_low, blob, target_mount), "")

        try:
            _loaded = await asyncio.gather(*(_load(p) for p in paths))
        except Exception as e:  # noqa: BLE001
            log.warning("re-inject watch-dir load failed for %s: %s", ts.source_name[:50], e)
            return

        async def _inject(item) -> tuple[str, bool, str]:
            """Ensure one gated member; (h_low, ok, detail).

            Transport errors propagate (the row parks and retries) —
            only client rejections return; a flap must never read as a
            hard skip.
            """
            h_low, blob, target_mount = item
            ok, detail = await self._ensure_fuse_entry(
                blob=blob,
                infohash=h_low,
                target_mount=target_mount,
                label="watch-dir torrent",
            )
            return (h_low, bool(ok), detail or "")

        try:
            _ready = [(p, pay, prob) for p, pay, prob in _loaded]
            _sem = asyncio.Semaphore(4)

            async def _one(_h_low, _blob, _target):
                async with _sem:
                    return await _inject((_h_low, _blob, _target))

            _results: dict[str, tuple[bool, str]] = {}
            _pending = [
                (pay[0], pay[1], pay[2])
                for _, pay, prob in _ready
                if pay is not None
                and pay[1] is not None
                and pay[0] not in injected_set
                and not prob
            ]
            if _pending:
                _outs = await asyncio.gather(*(_one(h, b, t) for h, b, t in _pending))
                for (h, _b, _t), (_h2, ok, detail) in zip(_pending, _outs, strict=False):
                    _results[h] = (ok, detail)
                    injected_set.add(h)
            for p, pay, prob in _ready:
                if pay is None:
                    log.warning("failed to read watch-dir torrent %s: %s", p.name, prob)
                    continue
                h_low = pay[0]
                if prob:
                    log.warning("re-inject watch-dir torrent %s: %s", h_low[:10], prob)
                    continue
                if h_low in injected:
                    continue
                if h_low not in _results:
                    continue
                ok, detail = _results[h_low]
                if ok:
                    injected.append(h_low)
                    continue
                # Split like _re_add_cross_seed_torrent: transient client
                # states park the row for retry; a hard rejection (e.g.
                # invalid blob) skips just this torrent so one bad .torrent
                # can't hold the whole row (and its moved bytes) hostage.
                if not detail or detail == _NOT_VISIBLE_DETAIL or _is_duplicate_add_detail(detail):
                    log.warning("re-inject watch-dir torrent %s rejected: %s", h_low[:10], detail)
                    raise WebUIUnresponsiveError(
                        f"re-inject watch-dir torrent {h_low[:10]} rejected: {detail}"
                    )
                log.warning(
                    "re-inject watch-dir torrent %s hard-rejected (%s); skipping it",
                    h_low[:10],
                    detail,
                )
                continue
        finally:
            try:
                for _h in injected:
                    _record_injected_hash(ts, _h)
            except Exception:
                pass

    async def _re_inject_racing_torrents(self, ts: TorrentState) -> None:
        """Re-add every racing-client torrent matching this content onto VPS2
        pointing at the fuse mount with skip_check=True.
        """
        injected = [h.lower() for h in ts.injected_private_hashes.split(",") if h]
        injected_set = set(injected)
        added = 0

        try:
            racing = await self._list_source_torrents()
        except Exception as e:  # noqa: BLE001
            # No early return: an unreachable VPS1 must not skip the
            # tracker_map supplements below (often the only remaining
            # cross-seed source) — proceed with an empty racing set.
            log.warning("could not list racing torrents for re-injection: %s", e)
            racing = []

        try:
            want_size = int(ts.total_bytes or 0)
        except (TypeError, ValueError):
            want_size = 0

        matches = []
        unrelated = 0
        for t in racing or []:
            try:
                t_size = int(getattr(t, "size_bytes", 0) or 0)
            except (TypeError, ValueError):
                t_size = 0
            # Same name alone is not same content: a different-sized
            # same-name torrent (repack, mislabel) must not ride this row's
            # injection. Unknown sizes (0) still match (single rule).
            if not _same_content_key(t.name or "", t_size, ts.source_name or "", want_size):
                # Same release name with a different size is a real conflict
                # (repack/mislabel) worth a line. Everything else is an
                # unrelated title: counting them keeps the log readable on a
                # box with hundreds of racing torrents.
                if (
                    want_size
                    and t_size
                    and t_size != want_size
                    and _same_release_name(t.name or "", ts.source_name or "")
                ):
                    log.info(
                        "re-inject: size mismatch for %s (%s: VPS1 %d != row %d); skipping",
                        (t.name or "")[:50],
                        (t.infohash or "")[:10],
                        t_size,
                        want_size,
                    )
                else:
                    unrelated += 1
                continue
            matches.append(t)
        if unrelated:
            log.debug(
                "re-inject: skipped %d unrelated racing torrent(s) for %s",
                unrelated,
                (ts.source_name or "")[:50],
            )

        async def _load(t):
            """Fetch + gate one match (no state change)."""
            try:
                h_low = (t.infohash or "").lower()
            except Exception:
                return (t, None, "unreadable infohash; skipping")
            if not h_low:
                return (t, None, "unreadable infohash; skipping")
            if _group_is_ignored([t], getattr(self, "store", None)):
                return (t, None, "cancelled; skipping")
            try:
                # DB first: VPS1 may already be cleaned for this row.
                blob = await self._load_racing_torrent_bytes(t.infohash)
            except Exception as e:  # noqa: BLE001
                return (t, None, f"fetch failed ({e})")
            if not blob:
                # DB miss + SFTP clean-miss + no export endpoint (Deluge has
                # no core.get_torrent_file) surface nowhere else — log
                # loudly so a skipped racing seed is visible.
                return (
                    t,
                    None,
                    "no .torrent bytes available; skipping for now (late-seed job will retry)",
                )
            # Per-torrent mount: ts.kind may be stale "unknown" (fresh
            # adoption); the blob's own layout is authoritative.
            target_mount = self._target_mount_for_ts_blob(ts, blob)
            # Per-match fuse gate (fail-closed): only inject torrents
            # whose content is actually available at the target (same
            # release name does not guarantee the bytes were moved).
            match_expected = self._expected_fuse_files(blob)
            if not match_expected:
                return (t, None, "cannot decode file list; skipping blind injection")
            match_missing = await self._missing_fuse_files(target_mount, match_expected)
            if match_missing:
                return (
                    t,
                    None,
                    f"fuse content missing at {target_mount} "
                    f"({len(match_missing)} files); skipping blind injection",
                )
            return (t, (h_low, blob, target_mount), "")

        # Already-recorded matches skip the fetch entirely (no SFTP/export
        # round-trip for hashes known injected).
        try:
            fresh = [
                t for t in matches if (getattr(t, "infohash", "") or "").lower() not in injected_set
            ]
        except Exception:
            fresh = list(matches)
        try:
            _loaded = await asyncio.gather(*(_load(t) for t in fresh))
        except Exception as e:  # noqa: BLE001
            log.warning("re-inject racing load failed for %s: %s", ts.source_name[:60], e)
            return

        async def _inject(item) -> tuple[str, bool, str]:
            """Ensure one gated match; (h_low, ok, detail).

            Transport errors propagate (the row parks and retries) —
            only client rejections return.
            """
            h_low, blob, target_mount = item
            ok, detail = await self._ensure_fuse_entry(
                blob=blob,
                infohash=h_low,
                target_mount=target_mount,
                label="racing torrent",
            )
            return (h_low, bool(ok), detail or "")

        try:
            _sem = asyncio.Semaphore(4)

            async def _one(_h_low, _blob, _target):
                async with _sem:
                    return await _inject((_h_low, _blob, _target))

            _results: dict[str, tuple[bool, str]] = {}
            _pending = [
                (pay[0], pay[1], pay[2])
                for _, pay, prob in _loaded
                if pay is not None
                and pay[1] is not None
                and pay[0] not in injected_set
                and not prob
            ]
            if _pending:
                _outs = await asyncio.gather(*(_one(h, b, t) for h, b, t in _pending))
                for (h, _b, _t), (_h2, ok, detail) in zip(_pending, _outs, strict=False):
                    _results[h] = (ok, detail)
                    injected_set.add(h)
            for t, pay, prob in _loaded:
                try:
                    _t_hash = (t.infohash or "")[:10]
                    _t_name = (t.name or "")[:50]
                except Exception:
                    _t_hash, _t_name = "?", "?"
                if pay is None:
                    if prob == "cancelled; skipping":
                        log.info(
                            "re-inject: skipping cancelled torrent %s (%s)",
                            _t_hash,
                            (_t_name or "")[:40],
                        )
                    else:
                        log.warning("re-inject: %s for %s (%s); skipping", prob, _t_hash, _t_name)
                    continue
                h_low = pay[0]
                if prob:
                    log.warning("re-inject: %s for %s (%s); skipping", prob, _t_hash, _t_name)
                    continue
                if h_low in injected:
                    continue
                if h_low not in _results:
                    continue
                ok, detail = _results[h_low]
                if ok:
                    injected.append(h_low)
                    added += 1
                    continue
                # Split like _re_add_cross_seed_torrent: transient client
                # states park the row for retry; a hard rejection skips
                # just this torrent so the cross-seed still lands.
                if not detail or detail == _NOT_VISIBLE_DETAIL or _is_duplicate_add_detail(detail):
                    log.warning(
                        "re-inject: add %s rejected: %s",
                        _t_hash,
                        detail,
                    )
                    raise WebUIUnresponsiveError(f"re-inject add {_t_hash} rejected: {detail}")
                log.warning(
                    "re-inject: add %s hard-rejected (%s); skipping it",
                    _t_hash,
                    detail,
                )
                continue
        finally:
            try:
                for _h in injected:
                    _record_injected_hash(ts, _h)
            except Exception:
                pass

        if added == 0:
            pending = [t for t in matches if t.infohash.lower() not in injected_set]
            if pending:
                log.warning(
                    "re-inject: 0/%d racing torrent(s) injected for %s "
                    "(VPS1 still lists them); row proceeds with cross-seed "
                    "only and the late-seed job will retry the racing injection",
                    len(pending),
                    ts.source_name[:60],
                )
            elif not matches:
                log.info(
                    "re-inject: no racing torrents left on VPS1 for %s; cross-seed only",
                    ts.source_name[:60],
                )

        # Supplementary private cross-seeds harvested via Prowlarr: VPS1
        # is not the whole world — tracker_map indexers may hold the same
        # release under a different infohash (piece size, source tag).
        # Same fail-closed rules as racing members (per-blob fuse gate,
        # skip_check only onto verified bytes). Best-effort: never raises.
        try:
            await self._re_inject_tracker_map_supplements(ts)
        except (asyncio.CancelledError, AbandonedError):
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("re-inject supplements failed for %s: %s", ts.source_name[:60], e)

    # Prowlarr supplement sweep interval (seconds): extra fuse seeds are
    # opportunistic, not urgent — one search per row per window, while
    # VPS1 late arrivals are still checked every tick by the caller.
    _SUPPLEMENT_RETRY_S = 21600.0
    # Failed/empty searches re-run soon (a new release under a new hash
    # may appear at any time); only a search that actually injected a
    # hash earns the long suppress window.
    _SUPPLEMENT_FAIL_S = 300.0

    async def _re_inject_tracker_map_supplements(self, ts: TorrentState) -> None:
        """Inject same-release torrents from tracker_map indexers to fuse.

        VPS1 supplements the racing set; it does not bound it. For each
        configured tracker_map indexer with an exact same-release hit
        (normalized name + size), download the .torrent, verify the
        payload, and inject it onto the fuse mount with skip_check=True
        — but only when its files are already stat-able at the target
        (same bytes the row moved; never blind). Membership in
        tracker_map is the opt-in: empty map = no-op. Never raises
        (except cancellation/abandonment, which propagate).
        """
        if getattr(self, "_stop", False):
            return
        try:
            prowlarr = getattr(self, "prowlarr", None)
            if prowlarr is None:
                return
            cfg_prowlarr = getattr(getattr(self, "cfg", None), "prowlarr", None)
            if cfg_prowlarr is None:
                return
            if not self.rt.acquire.inject_racing_torrents_to_fuse:
                return
            tmap = getattr(cfg_prowlarr, "tracker_map", None)
            entries = getattr(tmap, "entries", None)
            if not isinstance(entries, dict) or not entries:
                return
            try:
                if callable(
                    getattr(cfg_prowlarr, "should_skip_title", None)
                ) and cfg_prowlarr.should_skip_title(ts.source_name or ""):
                    return
            except Exception:
                pass
        except (asyncio.CancelledError, AbandonedError):
            raise
        except Exception:
            return
        try:
            row_key = (ts.source_infohash or "").lower()
        except Exception:
            return
        if not row_key:
            return
        try:
            memo = getattr(self, "_supplement_ok_at", None)
            if not isinstance(memo, dict):
                memo = {}
                self._supplement_ok_at = memo  # type: ignore[attr-defined]
            now_m = time.monotonic()
            nxt = memo.get(row_key)
            try:
                nxt_f = float(nxt) if nxt is not None else 0.0
            except (TypeError, ValueError):
                nxt_f = 0.0
            if nxt_f and now_m < nxt_f:
                return
        except (asyncio.CancelledError, AbandonedError):
            raise
        except Exception:
            return
        try:
            indexers = []
            seen: set[str] = set()
            for _sub, name in entries.items():
                try:
                    idx = prowlarr.get_indexer_by_name(name)
                except Exception:
                    continue
                if idx is None:
                    continue
                try:
                    if not bool(getattr(idx, "enable", False)):
                        continue
                    low = str(getattr(idx, "name", "") or "").lower()
                except Exception:
                    continue
                if not low or low in seen:
                    continue
                seen.add(low)
                indexers.append(idx)
            if not indexers:
                return
            try:
                hits_by_indexer = await asyncio.wait_for(
                    prowlarr.search_indexers_parallel(indexers, ts.source_name or ""),
                    timeout=30.0,
                )
            except (asyncio.CancelledError, AbandonedError):
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("supplement search failed for %s: %s", ts.source_name[:60], e)
                try:
                    memo[row_key] = time.monotonic() + self._SUPPLEMENT_FAIL_S
                except Exception:
                    pass
                return
            try:
                known = {
                    (h or "").lower()
                    for h in (
                        ts.source_infohash,
                        ts.dest_infohash,
                        ts.cross_seed_infohash,
                        *ts.injected_private_hashes.split(","),
                    )
                    if h
                }
            except Exception:
                known = set()
            try:
                self._target_mount_for(ts)
            except Exception:
                return
            added = 0
            changed = False

            async def _fetch_hit(hit):
                """Download + verify one supplement hit (no state change)."""
                try:
                    ht = getattr(hit, "title", "") or ""
                    hs = int(getattr(hit, "size_bytes", 0) or 0)
                except (TypeError, ValueError):
                    return None
                try:
                    _kind = (ts.classification_kind or "").strip() or None
                except Exception:
                    _kind = None
                try:
                    if not _matches_release(
                        ht,
                        hs,
                        ts.source_name or "",
                        int(ts.total_bytes or 0),
                        kind=_kind,
                        unify_separators=True,
                    ):
                        return None
                except Exception:
                    return None
                try:
                    blob = await asyncio.wait_for(prowlarr.download_torrent(hit), timeout=30.0)
                except (asyncio.CancelledError, AbandonedError):
                    raise
                except Exception as e:  # noqa: BLE001
                    log.debug("supplement fetch failed for %s: %s", ht[:50], e)
                    return None
                try:
                    verified = _verified_cross_seed_blob(
                        blob,
                        target_name=ts.source_name or "",
                        target_size=int(ts.total_bytes or 0),
                        hit_title=ht,
                        kind=_kind,
                    )
                except Exception:
                    return None
                if verified is None:
                    return None
                _blob, real_hash, _ann = verified
                try:
                    h_low = (real_hash or "").lower()
                except Exception:
                    return None
                if not h_low:
                    return None
                return (hit, _blob, h_low)

            try:
                _all_hits = [
                    hit for idx_hits in (hits_by_indexer or {}).values() for hit in (idx_hits or [])
                ]
                _sem = asyncio.Semaphore(4)

                async def _one(_hit):
                    async with _sem:
                        return await _fetch_hit(_hit)

                _downloads = await asyncio.gather(*(_one(h) for h in _all_hits))
            except (asyncio.CancelledError, AbandonedError):
                raise
            except Exception as e:  # noqa: BLE001
                log.debug("supplement fetch phase failed for %s: %s", ts.source_name[:60], e)
                _downloads = []
            for _item in _downloads:
                if _item is None:
                    continue
                hit, _blob, h_low = _item
                if h_low in known:
                    continue
                try:
                    if self._abandoned(ts):
                        raise AbandonedError(f"row gone for {(ts.source_infohash or '')[:10]}")
                except AbandonedError:
                    raise
                except Exception:
                    pass
                try:
                    if (
                        getattr(self, "store", None) is not None
                        and callable(getattr(self.store, "is_ignored", None))
                        and self.store.is_ignored(h_low) is True
                    ):
                        log.info("supplement: skipping cancelled torrent %s", h_low[:10])
                        known.add(h_low)
                        continue
                except Exception:
                    pass
                try:
                    match_expected = self._expected_fuse_files(_blob)
                except Exception:
                    match_expected = None
                if not match_expected:
                    continue
                try:
                    match_mount = self._target_mount_for_ts_blob(ts, _blob)
                    match_missing = await self._missing_fuse_files(match_mount, match_expected)
                except Exception:
                    continue
                if match_missing:
                    continue
                try:
                    ok, _detail = await self._ensure_fuse_entry(
                        blob=_blob,
                        infohash=h_low,
                        target_mount=match_mount,
                        label="tracker_map supplement",
                    )
                except (asyncio.CancelledError, AbandonedError):
                    raise
                except Exception as e:  # noqa: BLE001
                    log.debug("supplement inject failed for %s: %s", h_low[:10], e)
                    continue
                if not ok:
                    continue
                if _record_injected_hash(ts, h_low):
                    changed = True
                known.add(h_low)
                added += 1
                log.info(
                    "supplement: injected %s for %s from %s",
                    h_low[:10],
                    ts.source_name[:60],
                    getattr(hit, "indexer", "?"),
                )
            # Memo only after ≥1 INJECTED hash: a blob that fetched and
            # verified but whose entry never became visible (registration
            # lag) must retry soon, not six hours later — otherwise one
            # unlucky event hides the supplement for the rest of the day.
            try:
                if added > 0:
                    memo[row_key] = time.monotonic() + self._SUPPLEMENT_RETRY_S
                else:
                    memo[row_key] = time.monotonic() + self._SUPPLEMENT_FAIL_S
                if len(memo) > 5000:
                    for _k in list(memo.keys())[: len(memo) - 5000]:
                        memo.pop(_k, None)
            except Exception:
                pass
            if changed:
                try:
                    self.store.upsert(ts)
                except Exception:
                    pass
        except (asyncio.CancelledError, AbandonedError):
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("re-inject supplements failed for %s: %s", ts.source_name[:60], e)
            return
