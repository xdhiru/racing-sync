"""Manual-fuse adoption and fuse content-wait admission.

Extracted from coordinator_admit.py; `AdmitMixin` composes this mixin."""

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path

from .coordinator_content import WATCH_ORIGIN_LABELS
from .coordinator_paths import _safe_ssd_join
from .io_bounds import chunked, offload, rpc
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


class FuseAdmitMixin:
    async def _verify_and_adopt_manual_fuse(self, ts: TorrentState, ext) -> bool:
        """Verify a dest entry as a manual fuse seed and adopt to DONE.

        `ext` is a dest `Torrent` for the same infohash found via a
        category-agnostic hash lookup (manual adds usually carry no
        category/tags). Adoption requires on-fuse save_path + client-complete
        + bytes stat-able at the fuse target (skip_check ghosts must never
        mark DONE). Best-effort: any unverifiable outcome returns False and
        the caller continues the normal SSD/prowlarr flow.

        Returns True when the row was transitioned to DONE (or was already
        DONE by a concurrent actor).
        """
        try:
            if ts.state == State.DONE:
                return True
            if ts.state not in (
                State.NEW,
                State.WAITING_INDEXER,
                State.WAITING_DISK,
                State.RE_ADDING,
            ):
                return False
            try:
                if getattr(self, "store", None) is not None and hasattr(self.store, "is_ignored"):
                    if self.store.is_ignored(ts.source_infohash) is True:
                        return False
            except Exception:
                pass
            if ext is None:
                return False
            if not self._save_path_is_on_fuse(getattr(ext, "save_path", "")):
                return False
            try:
                complete = ext.is_complete() if hasattr(ext, "is_complete") else False
                if callable(complete):
                    complete = complete()
            except Exception:
                return False
            if not complete:
                return False
            try:
                fuse_files = await rpc(
                    self.dest_client.get_torrent_files(ext.hash),
                    getattr(self, "cfg", None),
                    "dest fuse get_torrent_files",
                )
            except Exception as e:  # noqa: BLE001
                log.warning(
                    "manual fuse check: could not list files for %s: %s",
                    ts.source_infohash[:10],
                    e,
                )
                return False
            fuse_expected = [
                (f.name, f.size_bytes) for f in (fuse_files or []) if getattr(f, "name", "")
            ]
            # Empty file list: nothing to verify against — do not adopt blind.
            # Recovery keeps the same fail-closed stance; a warming mount also
            # shows nothing and must park, not DONE.
            if not fuse_expected:
                return False
            try:
                fuse_missing = await self._missing_fuse_files(
                    Path(getattr(ext, "save_path", "")), fuse_expected
                )
            except Exception:
                return False
            if fuse_missing:
                log.warning(
                    "manual fuse check: %s reports complete on fuse %s but %d/%d files missing "
                    "(e.g. %s); keeping normal flow instead of DONE",
                    ts.source_infohash[:10],
                    getattr(ext, "save_path", "?"),
                    len(fuse_missing),
                    len(fuse_expected),
                    fuse_missing[0],
                )
                return False
            log.info(
                "torrent %s is already completed on VPS2 fuse mount (manual add, category-agnostic); marking DONE",
                ts.source_infohash[:10],
            )
            ts.dest_infohash = ext.hash.lower()
            ts.save_path = getattr(ext, "save_path", "")
            try:
                # Same patch-target treatment as above (classify).
                from . import coordinator as _coordinator

                fast_kind = _coordinator.classify(fuse_files, self.cfg).kind
            except Exception:  # noqa: BLE001
                fast_kind = "unknown"
            if fast_kind and fast_kind != ts.classification_kind:
                ts.classification_kind = fast_kind
                try:
                    self.store.upsert(ts)
                except Exception:  # noqa: BLE001
                    pass
            try:
                if self.rt.acquire.inject_racing_torrents_to_fuse:
                    await self._re_inject_racing_torrents(ts)
            except Exception as e:  # noqa: BLE001
                # Manual entry already seeds; a failed re-inject must not block
                # DONE — the late-seed job repairs the racing injection.
                log.warning(
                    "manual fuse check: re-inject for %s failed (keeping DONE): %s",
                    ts.source_infohash[:10],
                    e,
                )
            try:
                self.transition(ts, State.DONE)
            except ValueError:
                # Concurrent actor already moved the row (e.g. tick sweep beat
                # this worker to DONE). Treat as adopted, never overwrite.
                try:
                    fresh = self.store.get(ts.source_infohash, include_blob=False)
                except Exception:
                    fresh = None
                if fresh is not None and fresh.state == State.DONE:
                    try:
                        ts.state = fresh.state
                        ts.dest_infohash = fresh.dest_infohash or ts.dest_infohash
                        ts.save_path = fresh.save_path or ts.save_path
                    except Exception:
                        pass
                    return True
                log.warning(
                    "manual fuse check: could not transition %s to DONE from %s",
                    ts.source_infohash[:10],
                    ts.state.value,
                )
                return False
            return True
        except Exception as e:  # noqa: BLE001
            log.warning(
                "manual fuse check failed for %s: %s",
                getattr(ts, "source_infohash", "?")[:10],
                e,
            )
            return False

    async def _adopt_manual_fuse_if_present(self, ts: TorrentState) -> bool:
        """Category-agnostic check for a manually added fuse seed (same infohash).

        Used before any prowlarr query in _do_new/_do_waiting_indexer so an
        operator-added torrent (no category/tags) fast-tracks to DONE instead
        of parking in WAITING_INDEXER. Single-hash dest lookup — cheap and
        independent of the racing-category filter recovery relies on.
        """
        try:
            dest_client = getattr(self, "dest_client", None)
            if dest_client is None:
                return False
            if ts.state not in (
                State.NEW,
                State.WAITING_INDEXER,
                State.WAITING_DISK,
                State.RE_ADDING,
            ):
                return False
            h = (ts.source_infohash or "").lower()
            if not h:
                return False
            if self._fuse_sweep_covers(ts):
                # The tick sweep already looked this workerless row up
                # (no entry then): skip the redundant per-row RPC. Rows
                # with live workers are never covered — the sweep skips
                # them, so their own check still runs.
                return False
            try:
                existing = await rpc(
                    dest_client.list_torrents(hashes=[h]),
                    getattr(self, "cfg", None),
                    "dest manual fuse lookup",
                )
            except Exception as e:  # noqa: BLE001
                log.warning("manual fuse check: dest lookup failed for %s: %s", h[:10], e)
                return False
            if not existing:
                return False
            ext = next((t for t in existing if (t.hash or "").lower() == h), existing[0])
            return await self._verify_and_adopt_manual_fuse(ts, ext)
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse check failed for %s: %s", ts.source_infohash[:10], e)
            return False

    async def _fuse_content_evidence(
        self, ts: TorrentState, *, blob: bytes | None = None
    ) -> tuple[str, bytes | None, int, int]:
        """Is this row's content already on the fuse target? (pre-pick)

        Answers one of three, costing at most ONE stat for the common
        "not on fuse" case (a normal new torrent):

          "present" — every expected file size-verifies at the target, so
              the row admits straight from its own blob (no Prowlarr
              query, no SSD budget);
          "partial" — at least one expected file exists but not all: an
              operator's manual rclone move is in progress (or the fuse
              index lags), so the caller parks and re-checks on a timer;
          "absent"  — nothing at the target, or nothing verifiable (no
              blob, undecodable, mount outage): keep the existing
              pick -> SSD flow unchanged.

        Cheapness: probe the torrent-name root first (`missing_under`
        with want=0 is presence-only). Only when that root exists are the
        bytes decoded and every expected file verified.

        Returns (status, blob, present_count, expected_count).
        """
        # Mount resolution never stats: a broken/unconfigured fuse section
        # is simply "absent" (the pick below owns that failure loudly).
        try:
            target = self._target_mount_for(ts)
            probe_name = (ts.source_name or "").strip()
            root = _safe_ssd_join(target, probe_name)
        except Exception:
            return "absent", None, 0, 0
        if root is None:
            return "absent", None, 0, 0
        try:
            probe_name = root.relative_to(target).as_posix()
        except Exception:
            return "absent", None, 0, 0
        # A fresh racing row has no classification yet, so `_target_mount_for`
        # answers with the UNSORTED mount. Season/movie roots live under the
        # sorted mount (and the unsorted mount is usually nested inside it),
        # so probe every configured mount and keep the one that has the root.
        # The blob decides the authoritative mount only after it is decoded.
        mounts: list[Path] = [target]
        try:
            for _raw in (
                self.rt.seed.mount,
                self.rt.seed.mount_unsorted,
            ):
                if not isinstance(_raw, (str, Path)):
                    continue
                _m = Path(str(_raw))
                if _m not in mounts:
                    mounts.append(_m)
        except Exception:
            pass
        hit_mount: Path | None = None
        try:
            from .fuse_gate import missing_under

            for _m in mounts:
                probe = await missing_under(_m, [(probe_name, 0)], fresh=False)
                # None (unstatable) or a marker is not evidence: never start
                # a manual-move wait from an outage.
                if probe is None:
                    continue
                if not probe:
                    hit_mount = _m
                    break
        except Exception:
            hit_mount = None
        if hit_mount is None:
            return "absent", None, 0, 0

        try:
            if not blob:
                blob = ts._blob or ts.cross_seed_blob
            if not blob:
                try:
                    blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                except Exception:
                    blob = None
            if not blob:
                h = (ts.source_infohash or "").lower()
                if h and not self._is_watch_row(ts):
                    # Racing rows may never have persisted bytes: export
                    # them once (the same call the pick would make later)
                    # and cache them for the admit tail.
                    try:
                        blob = await self._fetch_racing_torrent_bytes(h)
                    except Exception:
                        blob = None
                    if blob:
                        try:
                            await asyncio.to_thread(self.store.set_blob, ts.source_infohash, blob)
                        except Exception:
                            pass
            if not blob:
                return "absent", None, 0, 0

            expected = self._expected_fuse_files(blob)
            if not expected:
                return "absent", None, 0, 0
            from .fuse_gate import verify_blob_on_mount

            check = await verify_blob_on_mount(hit_mount, blob, expected=expected, confirm=True)
            if check.verified:
                return "present", bytes(blob), len(expected), len(expected)
            if check.expected is None or check.unverifiable:
                # Mount outage: the existing outage paths park; do not
                # start a manual-move window on unverifiable evidence.
                return "absent", None, 0, 0
            present = len(expected) - len(check.missing)
            if present > 0:
                return "partial", bytes(blob), present, len(expected)
            return "absent", None, 0, 0
        except Exception as e:  # noqa: BLE001
            log.warning(
                "fuse content evidence check failed for %s: %s",
                (ts.source_infohash or "?")[:10],
                e,
            )
            return "absent", None, 0, 0

    def _admit_existing_library(self, ts: TorrentState, blob: bytes) -> None:
        """Adopt-only: the content is already seeded at the seed path.

        No client entry is added (the library already seeds it); the row is
        done once its bytes are verified there.
        """
        ts.cross_seed_infohash = (ts.source_infohash or "").lower()
        try:
            if (ts.cross_seed_source or "") not in WATCH_ORIGIN_LABELS:
                ts.cross_seed_source = "existing"
        except Exception:
            ts.cross_seed_source = "existing"
        ts.cross_seed_blob = bytes(blob)
        ts._blob = bytes(blob)
        ts.fuse_wait_first_at = None
        try:
            ts.save_path = str(self.rt.seed.mount or "")
        except Exception:
            pass
        if int(ts.total_bytes or 0) <= 0:
            try:
                ts.total_bytes = sum(
                    int(_sz or 0) for _n, _sz in (self._expected_fuse_files(blob) or [])
                )
            except Exception:
                pass
        log.info(
            "adopting existing library copy of %s (seed provider = existing); no entry added",
            (ts.source_name or "")[:60],
        )
        self.transition(ts, State.DONE)

    async def _admit_fuse_existing(self, ts: TorrentState, blob: bytes) -> None:
        """Admit a row whose bytes are already verified on the fuse target.

        The row seeds from its own torrent blob (no cross-seed lookup);
        `_admit_resolved`'s all-remote shortcut then takes it straight to
        QUEUED with no SSD reservation. That helper owns the transition.
        """
        ts.cross_seed_infohash = (ts.source_infohash or "").lower()
        # Keep a watch-origin label: the re-add flow keys its own
        # (multi-blob) re-injection off it. Racing rows get a label that
        # says where the bytes were found.
        try:
            if (ts.cross_seed_source or "") not in WATCH_ORIGIN_LABELS:
                ts.cross_seed_source = "fuse-existing"
        except Exception:
            ts.cross_seed_source = "fuse-existing"
        ts.cross_seed_blob = bytes(blob)
        ts._blob = bytes(blob)
        ts.fuse_wait_first_at = None
        # Same pre-admission defaults the pick path sets (the row never
        # downloads, but QUEUED logic reads them).
        try:
            ts.save_path = str(self.rt.storage.save_path)
        except Exception:
            pass
        if int(ts.total_bytes or 0) <= 0:
            try:
                ts.total_bytes = sum(
                    int(_sz or 0) for _n, _sz in (self._expected_fuse_files(blob) or [])
                )
            except Exception:
                pass
        log.info(
            "content for %s already on fuse; admitting without an SSD pick",
            (ts.source_name or "")[:60],
        )
        await self._admit_resolved(
            ts,
            blob=bytes(blob),
            size_bytes=int(ts.total_bytes or 0),
            display_name=(ts.source_name or "")[:60],
        )

    def _fuse_mounts_configured(self) -> bool:
        """True when at least one fuse mount is a real configured path."""
        try:
            mounts = (self.rt.seed.mount, self.rt.seed.mount_unsorted)
        except Exception:
            return False
        for raw in mounts:
            if isinstance(raw, (str, Path)) and str(raw).strip():
                return True
        return False

    def _manual_move_wait_limits(self) -> tuple[float, float]:
        """(check_seconds, timeout_seconds); timeout 0 disables the wait."""
        try:
            timeout_s = float(self.rt.seed.manual_move_timeout_seconds)
        except (TypeError, ValueError):
            timeout_s = 43200.0
        try:
            check_s = float(self.rt.seed.manual_move_check_seconds)
        except (TypeError, ValueError):
            check_s = 1800.0
        return check_s, timeout_s

    def _park_fuse_content_wait(
        self, ts: TorrentState, *, present: int = 0, total: int = 0
    ) -> bool:
        """Park awaiting a manual copy and re-check on a timer; False = give up.

        Uses WAITING_INDEXER (the scheduler already wakes those rows) with
        `indexer_next_retry_at` as the wake timer and `fuse_wait_first_at`
        as the persisted window start, so a restart keeps the deadline.
        Returns False when the wait is disabled (timeout 0, or no fuse
        mount configured) or the ceiling is reached. The stamp is kept on
        expiry as a latch: callers that would otherwise park again (an
        oversize row that cannot download at all) must fail instead of
        restarting the window forever.
        """
        if not self._fuse_mounts_configured():
            ts.fuse_wait_first_at = None
            return False
        check_s, timeout_s = self._manual_move_wait_limits()
        if timeout_s <= 0:
            ts.fuse_wait_first_at = None
            return False
        now = dt.datetime.now(dt.UTC)
        start = ts.fuse_wait_first_at
        if start is None:
            start = now
            ts.fuse_wait_first_at = now
        elapsed = (now - start).total_seconds()
        if elapsed >= timeout_s:
            log.info(
                "manual-move wait over for %s (%ds >= %ds); falling back to the SSD pick",
                (ts.source_name or "")[:60],
                int(elapsed),
                int(timeout_s),
            )
            # Keep the stamp: it latches "already gave up" so a caller that
            # cannot download anyway (oversize single file) fails instead
            # of starting a fresh window on the next wake.
            return False
        ts.indexer_next_retry_at = now + dt.timedelta(seconds=max(60.0, check_s))
        ts.public_export_next_retry_at = None
        if total > 0:
            _what = f"{present}/{total} files present"
        else:
            _what = "nothing visible yet"
        reason = (
            f"fuse content not fully there yet ({_what}); waiting for "
            f"the manual move (elapsed {int(elapsed)}s / timeout {int(timeout_s)}s)"
        )
        log.info("%s: %s", (ts.source_name or "")[:60], reason)
        if ts.state != State.WAITING_INDEXER:
            # A fresh row transitions (notifies once); an already-parked
            # row must not re-transition (illegal), so the narrow write
            # below carries the new timer instead.
            self.transition(ts, State.WAITING_INDEXER)
        self._park_row(
            ts,
            reason,
            level=None,
            extra={
                "indexer_next_retry_at": ts.indexer_next_retry_at.isoformat(),
                "public_export_next_retry_at": "",
                "fuse_wait_first_at": ts.fuse_wait_first_at.isoformat(),
            },
        )
        try:
            self._schedule_telegram_update(ts)
        except Exception:
            pass
        return True

    async def _fuse_content_precheck(self, ts: TorrentState, *, blob: bytes | None = None) -> bool:
        """Pre-pick manual-move handling; True when the caller must stop.

        Shared by `_do_new`, `_do_waiting_indexer` and `_do_new_watch_dir`
        so a wake wave re-checks fuse before querying indexers. With the
        wait disabled (timeout 0) this is a no-op: no probe, no stats.

        External acquisition (`[acquire] provider = "external"`) reuses the
        same window but never falls back to a download: an absent or
        incomplete copy keeps waiting, and an expired window fails the row
        with where the copy was expected.
        """
        _check_s, _timeout_s = self._manual_move_wait_limits()
        external = self.rt.chain.waits_for_copy
        if _timeout_s <= 0:
            if external:
                self._fail_external_wait(
                    ts, "the manual-copy wait is disabled (manual_move_timeout_seconds = 0)"
                )
                return True
            return False
        try:
            status, found, present, total = await self._fuse_content_evidence(ts, blob=blob)
        except Exception:
            status, found, present, total = "absent", None, 0, 0
        if status == "present" and found:
            if self.rt.chain.adds_seed_entry:
                await self._admit_fuse_existing(ts, found)
            else:
                self._admit_existing_library(ts, found)
            return True
        if status == "partial":
            if self._park_fuse_content_wait(ts, present=present, total=total):
                return True
            if external:
                self._fail_external_wait(ts, "the copy is incomplete")
                return True
            # Ceiling reached/disabled: fall through to the normal flow.
            return False
        # Absent: a wait already running (a copy seen earlier, or an
        # oversize row parked on this path) keeps going until its ceiling
        # instead of dropping straight back to the pick.
        if ts.fuse_wait_first_at is not None:
            if self._park_fuse_content_wait(ts, present=0, total=0):
                return True
            if external:
                self._fail_external_wait(ts, "the copy never appeared")
                return True
            return False
        if external:
            # First miss: open the window and wait for the operator.
            if self._park_fuse_content_wait(ts, present=0, total=0):
                return True
            self._fail_external_wait(ts, "the copy never appeared")
            return True
        return False

    def _fail_external_wait(self, ts: TorrentState, detail: str) -> None:
        """External acquisition has no download fallback: fail with where we looked."""
        try:
            paths = ", ".join(str(path) for path in self.rt.seed.mounts()) or "<no seed path>"
        except Exception:
            paths = "<no seed path>"
        message = f"external copy unavailable: {detail} (expected under {paths})"
        try:
            self.transition(ts, State.FAILED, error=message)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not fail %s after the external wait: %s",
                (ts.source_infohash or "")[:10],
                e,
            )
            return
        log.warning("%s: %s", (ts.source_name or ts.source_infohash)[:60], message)

    async def _sweep_manual_fuse_adoptions(self) -> None:
        """Batch-adopt pre-SSD rows whose infohash already seeds from fuse.

        Tick-level responsiveness net: WAITING_INDEXER rows only wake on their
        30m retry timer, so a manual add between retries would otherwise sit
        parked. One batched hash lookup per tick covers all pre-SSD rows at
        once (qB accepts pipe-separated hashes); rows with live workers are
        skipped — their own _do_* check adopts without racing the worker.
        """
        try:
            dest_client = getattr(self, "dest_client", None)
            store = getattr(self, "store", None)
            if dest_client is None or store is None:
                return
            try:
                pre_ssd = await offload(
                    store.list_by_state,
                    State.NEW,
                    State.WAITING_INDEXER,
                    State.WAITING_DISK,
                )
            except Exception:
                return
            if not pre_ssd:
                return
            running = getattr(self, "_running_infohashes", None)
            candidates: list[TorrentState] = []
            hashes: list[str] = []
            seen: set[str] = set()
            for ts in pre_ssd:
                try:
                    h = (ts.source_infohash or "").lower()
                except Exception:
                    continue
                if not h or h in seen:
                    continue
                try:
                    if isinstance(running, set) and h in running:
                        continue
                except Exception:
                    pass
                try:
                    if (
                        hasattr(store, "is_ignored")
                        and store.is_ignored(ts.source_infohash) is True
                    ):
                        continue
                except Exception:
                    pass
                seen.add(h)
                hashes.append(h)
                candidates.append(ts)
            if not hashes:
                self._note_fuse_sweep_covered()
                return
            try:
                # Chunked + RPC-bounded: >~100 hashes risks URL limits
                # downstream, and one hung lookup must not stall the tick.
                _sweep_parts = await asyncio.gather(
                    *(
                        rpc(
                            dest_client.list_torrents(hashes=_c),
                            getattr(self, "cfg", None),
                            "dest fuse sweep list",
                        )
                        for _c in chunked(hashes)
                    )
                )
                existing = [t for _part in _sweep_parts for t in (_part or [])]
            except Exception as e:  # noqa: BLE001
                log.warning("manual fuse sweep: dest lookup failed: %s", e)
                return
            if not existing:
                self._note_fuse_sweep_covered()
                return
            by_hash = {}
            for t in existing or []:
                try:
                    by_hash[(t.hash or "").lower()] = t
                except Exception:
                    continue
            # Verify candidates concurrently (4-wide): each verify is
            # files + fuse stats + maybe re-inject, and sequential
            # verification holds the poller tick for minutes at 50 rows.
            # Per-candidate isolation is preserved (fresh-state re-read
            # inside, broad catch outside).
            _verify_sem = asyncio.Semaphore(4)

            async def _verify_one(_ts):
                async with _verify_sem:
                    try:
                        h = (_ts.source_infohash or "").lower()
                        ext = by_hash.get(h)
                        if ext is None:
                            return
                        # Re-read: a worker may have moved this row after the
                        # snapshot above — never overwrite a fresh state.
                        try:
                            fresh = await asyncio.to_thread(
                                store.get, _ts.source_infohash, include_blob=False
                            )
                        except Exception:
                            fresh = None
                        if fresh is None or fresh.state != _ts.state:
                            return
                        if fresh.state not in (
                            State.NEW,
                            State.WAITING_INDEXER,
                            State.WAITING_DISK,
                        ):
                            return
                        if getattr(fresh, "skipped", 0):
                            return
                        await self._verify_and_adopt_manual_fuse(fresh, ext)
                    except Exception:
                        return

            await asyncio.gather(*(_verify_one(ts) for ts in candidates))
            self._note_fuse_sweep_covered()
        except Exception as e:  # noqa: BLE001
            log.warning("manual fuse sweep failed: %s", e)

    def _note_fuse_sweep_covered(self) -> None:
        """Stamp a completed fuse sweep: per-row adopt checks covered by
        it may skip their own dest lookup until the next sweep."""
        try:
            self._last_fuse_sweep_mono = time.monotonic()
        except Exception:
            pass

    def _fuse_sweep_covers(self, ts: TorrentState) -> bool:
        """True when the last sweep already checked this row this tick.

        Only workerless rows are covered: the sweep skips hashes with
        live workers, so a row running its own _do_* check must still
        look itself up.
        """
        try:
            _at = float(getattr(self, "_last_fuse_sweep_mono", 0.0) or 0.0)
            if time.monotonic() - _at <= 0:
                return False
            try:
                _interval = float(
                    getattr(
                        getattr(getattr(self, "cfg", None), "general", None),
                        "source_poll_interval",
                        30,
                    )
                    or 30
                )
            except (TypeError, ValueError):
                _interval = 30.0
            if time.monotonic() - _at > max(30.0, _interval):
                return False
            _running = getattr(self, "_running_infohashes", None)
            if isinstance(_running, set) and ((ts.source_infohash or "").lower() in _running):
                return False
            return True
        except Exception:
            return False
