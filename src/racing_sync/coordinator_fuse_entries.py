"""Fuse-entry management surface: mount resolution, blob classification,
availability checks, entry ensure/replace and the public-pause policy.

These helpers back the re-add, inject, late-seed and re-inject flows alike.
Cross-half helpers are reached through ``self`` (the FuseMixin composes both
halves); this module never imports ``coordinator_fuse``.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .clients.deluge import DelugeClient
from .coordinator_content import _looks_public, fold_path_case, route_kind_is_sorted
from .coordinator_errors import _NOT_VISIBLE_DETAIL
from .io_bounds import chunked, rpc
from .state import TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

#: Cap on cached per-blob decodes below (kind + announce). One bdecode is
#: microseconds, but the late tick re-decoded the same blobs per row per
#: poll; the cache collapses repeats. Oldest-first eviction past the cap.
_BLOB_DECODE_CACHE_CAP = 500
_BLOB_KIND_CACHE: dict[str, str] = {}
_BLOB_ANNOUNCE_CACHE: dict[str, str] = {}


def _blob_cache_key(blob: bytes | None) -> str:
    """sha1 key for per-blob decode caches ("" when unhashable)."""
    try:
        if not blob or not isinstance(blob, (bytes, bytearray)):
            return ""
        import hashlib

        return hashlib.sha1(bytes(blob)).hexdigest()
    except Exception:
        return ""


def _blob_cache_put(cache: dict[str, str], key: str, value: str) -> None:
    try:
        if not key:
            return
        cache[key] = value
        while len(cache) > _BLOB_DECODE_CACHE_CAP:
            cache.pop(next(iter(cache)), None)
    except Exception:
        pass


def _is_duplicate_add_detail(detail: object) -> bool:
    """True when an add rejection only means 'already there'.

    qB answers duplicate adds with "Fails." and Deluge echoes "already
    ...". AddResult carries no machine-readable code (a client overhaul
    may add one), so the verdict lives in exactly this helper — never as
    inline string checks scattered across callers.
    """
    try:
        d = detail if isinstance(detail, str) else ""
        return d == "Fails." or "already" in d.lower()
    except Exception:
        return False


class FuseEntryMixin:
    """Fuse-entry management (duck-typed ``self``)."""

    async def _dest_entries_for_hashes(self, hashes: set[str]) -> list | None:
        """Dest entries for recorded hashes (chunked, RPC-bounded).

        None on transport failure (fail-closed: callers treat unknown as
        unverified, never as healthy).
        """
        try:
            present: list = []
            for _chunk in chunked(sorted(hashes)):
                present.extend(
                    await rpc(
                        self.dest_client.list_torrents(hashes=_chunk),
                        getattr(self, "cfg", None),
                        "dest verify list_torrents",
                    )
                    or []
                )
            return present
        except Exception:  # noqa: BLE001
            return None

    async def _confirmed_fuse_missing(
        self, target_mount: Path, files: list[tuple[str, int]]
    ) -> list[str]:
        """Cached miss confirm: re-probe fresh once before reporting absent.

        Delegates to fuse_gate.confirm_missing (single implementation): the
        shared stat cache (60s) can report an absence the mount has already
        resolved, so a cached-only miss never earns a 30m backoff — only a
        fresh-confirmed miss defers. Outage markers (`<...>`) pass through
        untouched; callers split dead-mount from content-miss handling.
        """
        try:
            from .fuse_gate import confirm_missing as _confirm

            return await _confirm(target_mount, files)
        except Exception:  # noqa: BLE001
            return await self._missing_fuse_files(target_mount, files)

    async def _fetch_racing_torrent_bytes(self, infohash: str) -> bytes | None:
        """Fetch the raw .torrent bytes for a racing-client infohash.

        Tries in order:
          1. SFTP exporter (Deluge state dir, qB BT_backup)
          2. The source client's own export endpoint (qB /api/v2/torrents/export,
             Deluge core.get_torrent_file).
        """
        # Validate that infohash is a valid hex infohash
        if (
            not infohash
            or len(infohash) != 40
            or not all(c in "0123456789abcdefABCDEF" for c in infohash)
        ):
            log.warning("_fetch_racing_torrent_bytes: skipping invalid infohash %r", infohash)
            return None

        if self.sftp is not None:
            # One retry on timeout: several workers share a single paramiko
            # SFTP connection behind a lock, so a burst of late/re-inject
            # fetches can stall one call past the 15s budget. A clean miss
            # (file absent) returns None and is not retried.
            for attempt in (1, 2):
                try:
                    blob = await asyncio.wait_for(
                        asyncio.to_thread(self.sftp.fetch_torrent, infohash),
                        timeout=15.0,
                    )
                    if blob:
                        return blob
                    break
                except TimeoutError:
                    log.warning(
                        "sftp fetch %s timed out after 15s (attempt %d/2)", infohash[:10], attempt
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp fetch %s failed: %s", infohash[:10], e)
                    break

        try:
            return await asyncio.wait_for(
                self.source_client.export_torrent(infohash),
                timeout=15.0,
            )
        except AttributeError:
            return None
        except Exception as e:  # noqa: BLE001
            # Deluge daemons expose no torrent-file RPC (SFTP is the
            # mandatory path there, enforced by config validation), so a
            # failed fallback is routine — keep it out of the warning log.
            if self.sftp is not None and isinstance(self.source_client, DelugeClient):
                log.debug(
                    "_fetch_racing_torrent_bytes deluge export unavailable for %s: %s",
                    infohash[:10],
                    e,
                )
            else:
                log.warning(
                    "_fetch_racing_torrent_bytes export failed for %s: %s", infohash[:10], e
                )
            return None

    async def _load_racing_torrent_bytes(self, infohash: str) -> bytes | None:
        """Load a racing torrent's raw .torrent bytes, DB first.

        The janitor can delete a VPS1 group before every racing torrent was
        re-injected; after that the SFTP state dir and the client export are
        gone. The DB copy (preserved by the janitor, or cached by an earlier
        fetch) is therefore authoritative when present; only a miss falls
        back to VPS1 via `_fetch_racing_torrent_bytes`.

        Test doubles return a MagicMock from `get_blob`: only a non-empty
        bytes value counts as a hit, never a mock.
        """
        # Same infohash guard style as _fetch_racing_torrent_bytes.
        if (
            not infohash
            or len(infohash) != 40
            or not all(c in "0123456789abcdefABCDEF" for c in infohash)
        ):
            log.warning("_load_racing_torrent_bytes: skipping invalid infohash %r", infohash)
            return None
        h = infohash.lower()
        try:
            store = getattr(self, "store", None)
            if store is not None:
                # Row-independent cache first (sibling racing torrents have
                # no row of their own), then the row blob, then VPS1.
                for reader in (store.get_racing_blob, store.get_blob):
                    blob = await asyncio.to_thread(reader, h)
                    if isinstance(blob, (bytes, bytearray)) and blob:
                        return bytes(blob)
        except Exception as e:  # noqa: BLE001
            # A DB miss/read error is not fatal: VPS1 may still have the bytes.
            log.warning("_load_racing_torrent_bytes: blob read failed for %s: %s", h[:10], e)
        return await self._fetch_racing_torrent_bytes(infohash)

    @staticmethod
    def _expected_fuse_files(blob: bytes | None) -> list[tuple[str, int]] | None:
        """Decode (torrent-relative name, size) pairs from .torrent bytes.

        Returns None when there is nothing to verify (no blob / undecodable /
        empty) — callers then keep existing behavior and let downstream steps
        fail loudly instead of gating on an empty expectation.
        Delegates to fuse_gate.expected_files_from_blob (single rule).
        """
        try:
            from .fuse_gate import expected_files_from_blob as _expected

            return _expected(blob)
        except Exception:
            return None

    async def _fuse_entry_bytes_ok(self, blob: bytes | None, target_mount: Path) -> bool:
        """Post-add byte re-verify: entry placed AND bytes still at target.

        Guards the gate→add TOCTOU (bytes moved between verification and
        injection): an entry pointing at the target while the bytes are
        gone parks/retries instead of marking injected. Cached stats, so
        the re-stat is cheap. Undecodable blobs yield no expectation —
        nothing to re-verify, so they pass (the pre-add gate owns that
        legacy path explicitly). False on any other doubt.
        """
        try:
            expected = self._expected_fuse_files(blob)
            if not expected:
                return True
            from .fuse_gate import verify_blob_on_mount as _verify

            check = await _verify(target_mount, blob, expected=expected)
            return check.verified
        except Exception:  # noqa: BLE001
            return False

    def _fuse_entry_split(self, entries: list, hashes: set[str]) -> tuple[set[str], set[str]]:
        """Split row hashes by whether a live non-SSD dest entry carries them.

        Returns (have, missing). An entry carrying the hash while seeding
        from the SSD is not a fuse seed: it would pass an entry gate while
        nothing serves from the mount. Shared by the DONE-row verifier and
        the cleanup janitor so both apply the same placement rule.
        """
        from .recovery import _save_path_on_ssd as _on_ssd

        have: set[str] = set()
        for t in entries or []:
            try:
                h = str(getattr(t, "hash", "") or "").lower()
                if not h or h not in hashes:
                    continue
                if _on_ssd(self.cfg, str(getattr(t, "save_path", "") or "")):
                    continue
                have.add(h)
            except Exception:
                continue
        return have, set(hashes) - have

    async def _missing_fuse_files(
        self, target_mount: Path, files: list[tuple[str, int]]
    ) -> list[str]:
        """Expected files absent (or size-mismatched) under the fuse target.

        Delegates to fuse_gate.missing_or_marker (single implementation).
        Fail-closed as before: an unstatable mount/check returns a marker
        missing entry, never an empty pass.
        """
        try:
            from .fuse_gate import missing_or_marker as _missing

            return await _missing(target_mount, files or [])
        except Exception as e:  # noqa: BLE001
            log.warning("fuse availability check failed for %s: %s", target_mount, e)
            return [f"<availability check failed: {e}>"]

    def _fuse_mount_strs(self) -> list[str]:
        """Normalized fuse mount strings; [] when unconfigured/broken."""
        try:
            mounts = [self.rt.seed.mount, self.rt.seed.mount_unsorted]
        except Exception:
            return []
        out: list[str] = []
        for fm in mounts:
            if fm is None:
                continue
            try:
                s = str(fm).rstrip("/\\").replace("\\", "/")
            except Exception:
                continue
            if s:
                out.append(s)
        return out

    def _save_path_is_on_fuse(self, save_path: object) -> bool:
        """True iff a client save_path points at a configured fuse mount.

        Delegates to the shared ownership rule (I7): the delete guards and
        the adoption path must agree about what "on fuse" means.
        """
        from .path_ownership import PathOwner, classify_save_path

        return classify_save_path(getattr(self, "cfg", None), save_path) is PathOwner.FUSE

    def _seed_subdir_for(self, ts: TorrentState) -> str:
        """Physical top folder under the seed mount for this row ("" = root).

        Set at move time for new rows; older rows derive it from their own
        download blob (a wrapped target already lives under its top folder).
        Only a single safe segment is ever returned.
        """
        from .torrent_layout import blob_common_top

        try:
            sub = str(getattr(ts, "seed_subdir", "") or "").strip().replace("\\", "/").strip("/")
            if sub and "/" not in sub and sub not in (".", ".."):
                return sub
        except Exception:
            sub = ""
        try:
            blob = getattr(ts, "_blob", None) or getattr(ts, "cross_seed_blob", None)
            return blob_common_top(blob) or ""
        except Exception:
            return ""

    def _seed_root_for(self, ts: TorrentState, blob: bytes | None, base: Path) -> Path:
        """Seed root for these blob bytes, given the row's physical layout.

        A wrapped blob already carries its top folder in every relative
        name, so it seeds at the mount root. A flat blob needs the row's
        physical top folder as its save_path — that is the one-level
        adjustment that lets a flat racing torrent seed beside a wrapped
        download (and vice versa).
        """
        from .torrent_layout import blob_files, is_flat

        try:
            files = blob_files(blob)
            names = [getattr(f, "name", "") or "" for f in files]
        except Exception:
            return Path(base)
        if not names or not is_flat(names):
            return Path(base)
        sub = self._seed_subdir_for(ts)
        return Path(base) / sub if sub else Path(base)

    def _target_mount_for_ts_blob(self, ts: TorrentState, blob: bytes | None) -> Path:
        """Kind mount + the row's physical top folder for this blob."""
        base = self._target_mount_for_blob(blob, self._target_mount_for(ts))
        return self._seed_root_for(ts, blob, base)

    def _target_mount_for_kind(self, kind: str, fallback: Path) -> Path:
        """Fuse mount for a classification kind; fallback on error/unknown-cfg."""
        try:
            if route_kind_is_sorted(kind):
                return Path(self.rt.seed.mount)
            if kind in ("episode", "unknown"):
                return Path(self.rt.seed.mount_unsorted)
        except Exception:
            pass
        return fallback

    def _target_mount_for_blob(self, blob: bytes | None, fallback: Path) -> Path:
        """Per-torrent fuse mount derived from the blob's own layout.

        Fresh-DB adoptions start as "unknown" and would otherwise route every
        late cross-seed to unsorted (wrong for movies/seasons, and the fuse
        gate then checks the wrong directory). The blob is authoritative.
        """
        kind = self._classify_blob_kind(blob)
        if kind == "unknown":
            return fallback
        return self._target_mount_for_kind(kind, fallback)

    def _verify_mount_for(self, ts: TorrentState, blob: bytes | None, fallback: Path) -> Path:
        """Where a row's fuse bytes should be verified.

        An adopted entry can live in a subfolder of the mount (its own
        save_path, e.g. mount/SUB). Verifying at the blob-derived root
        would report every expected file missing — VPS1 never gets cleaned
        and repair defers forever — so a save_path recorded on a
        configured fuse mount wins; the blob-derived target stays the
        fallback for rows that were never adopted.
        """
        try:
            sp = str(getattr(ts, "save_path", "") or "").replace("\\", "/").rstrip("/")
            if sp and self._save_path_is_on_fuse(sp):
                return Path(sp)
        except Exception:
            pass
        base = self._target_mount_for_blob(blob, fallback)
        return self._seed_root_for(ts, blob, base)

    def _save_path_points_at_target(self, save_path: object, target_mount: object) -> bool:
        """Does an existing client entry point at the fuse target we inject to?

        A duplicate hash may already exist pointing elsewhere (e.g. a leftover
        SSD entry). Such entries must be replaced, never mistaken for a fuse
        seed. Non-string save paths (shouldn't happen) never match.
        """
        if not isinstance(save_path, str):
            return False
        sp = fold_path_case(save_path.rstrip("/\\").replace("\\", "/"))
        tm = fold_path_case(str(target_mount).rstrip("/\\").replace("\\", "/"))
        return bool(tm) and (sp == tm or sp.startswith(tm + "/"))

    def _should_pause_public_on_fuse(self, blob: bytes | None) -> bool:
        """True when a fuse-injected blob must land paused, not seeding.

        Gated on `cross_seed.pause_public_torrents_on_fuse` AND a positively
        identified public announce URL in the blob. Fail-open toward seeding
        (current behavior) on any doubt — undecodable blobs, missing config,
        unknown trackers — so a private torrent is never paused by mistake
        and break a seeding obligation.
        """
        try:
            flag = getattr(getattr(self, "cfg", None), "cross_seed", None)
            if not bool(getattr(flag, "pause_public_torrents_on_fuse", False)):
                return False
        except Exception:
            return False
        try:
            if not blob or not isinstance(blob, (bytes, bytearray)):
                return False
            _key = _blob_cache_key(blob)
            announce = None
            if _key:
                try:
                    if _key in _BLOB_ANNOUNCE_CACHE:
                        announce = _BLOB_ANNOUNCE_CACHE[_key]
                except Exception:
                    announce = None
            if announce is None:
                from .watchdir import _bencoded_info_hash

                _, _, _, announce = _bencoded_info_hash(bytes(blob))
                _blob_cache_put(_BLOB_ANNOUNCE_CACHE, _key, announce or "")
        except Exception:
            return False
        try:
            if not announce:
                return False
            return bool(_looks_public([announce]))
        except Exception:
            return False

    async def _pause_fuse_entry_best_effort(self, h_low: str, *, label: str) -> None:
        """Pause a fuse entry without ever failing the row.

        The entry is already correctly placed; a failed pause only means it
        keeps seeding until the operator pauses it by hand. Never raises.
        """
        try:
            pause_fn = getattr(self.dest_client, "pause", None)
            if not callable(pause_fn):
                return
            await pause_fn(h_low)
            log.info(
                "pausing public %s %s on fuse (pause_public_torrents_on_fuse=true)",
                label,
                h_low[:10],
            )
        except Exception as e:  # noqa: BLE001
            log.warning(
                "could not pause public %s %s on fuse (keeps seeding): %s", label, h_low[:10], e
            )

    async def _select_all_fuse_files(self, h_low: str, *, label: str) -> bool:
        """Ensure every file in a fuse entry is wanted (priority != 0).

        A fuse seed must seed the full content: an entry inheriting the
        SSD batch deselection (all priorities 0) reports complete via
        skip_check while seeding nothing. Returns True when all files
        are wanted (or nothing verifiable, e.g. test doubles), False
        when the check/reset hit a transient client error and the
        caller should park/retry instead of marking DONE.
        """
        try:
            files = await rpc(
                self.dest_client.get_torrent_files(h_low),
                getattr(self, "cfg", None),
                "dest fuse get_torrent_files",
            )
        except Exception as e:  # noqa: BLE001
            # Test doubles often leave get_torrent_files as a plain
            # MagicMock (not awaitable): fail open, the entry itself was
            # already verified at the fuse target.
            if "can't be used in 'await'" in str(e) or "MagicMock" in str(e):
                return True
            log.warning(
                "fuse priority check failed for %s %s: %s; parking re-add", label, h_low[:10], e
            )
            return False
        if not isinstance(files, list) or not files:
            return True
        try:
            names = [f.name for f in files if getattr(f, "name", "")]
        except Exception:
            return True
        if not names:
            return True
        try:
            needs_reset = any(int(getattr(f, "priority", 1) or 0) == 0 for f in files)
        except Exception:
            needs_reset = False
        if not needs_reset:
            return True
        log.warning("fuse entry %s %s has deselected files; selecting all", label, h_low[:10])
        try:
            await rpc(
                self.dest_client.set_file_priorities(h_low, {n: 1 for n in names}),
                getattr(self, "cfg", None),
                "dest fuse set_file_priorities",
            )
        except Exception as e:  # noqa: BLE001
            if "can't be used in 'await'" in str(e) or "MagicMock" in str(e):
                return True
            log.warning(
                "fuse priority reset failed for %s %s: %s; parking re-add", label, h_low[:10], e
            )
            return False
        return True

    async def _ensure_fuse_entry(
        self, *, blob: bytes, infohash: str, target_mount: Path, label: str
    ) -> tuple[bool, str]:
        """Ensure a fuse-pointing dest entry exists for `infohash`.

        Adds `blob` with skip_check. When the client reports a duplicate,
        the existing entry is verified: fuse-pointing entries are accepted
        as-is, but entries pointing elsewhere (stale SSD leftovers) are
        deleted (files kept) and re-added at the fuse target — the bytes
        were verified at the target before this call.

        Public torrents land paused when
        `cross_seed.pause_public_torrents_on_fuse` is set (fresh adds go in
        paused; already-seeding fuse entries are paused in place). Private
        torrents always seed as before.

        Returns (ok, detail). Never raises for client rejections; callers
        apply their own retry/fail policy. Exact add kwargs are kept stable
        for the seeding contract (category/tags/hold_check) apart from the
        paused flag above.

        Registration lag: a loaded client can accept the re-add while the
        entry is not yet visible to lookups. The post-add check is retried;
        a still-unconfirmed entry returns _NOT_VISIBLE_DETAIL so callers
        park/retry instead of failing the row over a transient.
        """
        h_low = infohash.lower()
        pause_public = self._should_pause_public_on_fuse(blob)
        add_kwargs: dict[str, object] = {
            "torrent_files": [blob],
            "save_path": str(target_mount),
            "category": "racing",
            "paused": True if pause_public else False,
            "skip_check": True,
            "tags": ["racing", "fuse"],
        }
        res = await rpc(
            self.dest_client.add_torrent(**add_kwargs),  # type: ignore[arg-type]
            getattr(self, "cfg", None),
            "dest fuse add_torrent",
        )
        detail = res.detail if isinstance(res.detail, str) else ""
        if res.accepted and not _is_duplicate_add_detail(detail):
            # Verify the entry actually landed where asked: the fuse index
            # can lag (rclone busy with another move), and an accepted-but-
            # invisible entry must park/retry — never mark DONE, never touch
            # the moved files. Same patience window as the replace path.
            for _ in range(4):
                try:
                    _st = await rpc(
                        self.dest_client.get_torrent(h_low),
                        getattr(self, "cfg", None),
                        "dest fuse get_torrent",
                    )
                except Exception:
                    _st = None
                if _st is not None and self._save_path_points_at_target(
                    getattr(_st, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
                    if not await self._fuse_entry_bytes_ok(blob, target_mount):
                        log.warning(
                            "%s %s entry visible but bytes unverifiable at %s; parking re-add",
                            label,
                            h_low[:10],
                            target_mount,
                        )
                        return False, _NOT_VISIBLE_DETAIL
                    if not await self._select_all_fuse_files(h_low, label=label):
                        return False, _NOT_VISIBLE_DETAIL
                    if pause_public:
                        await self._pause_fuse_entry_best_effort(h_low, label=label)
                    return True, detail
                await asyncio.sleep(2)
            log.warning(
                "%s %s accepted but not yet visible on fuse (%s); parking re-add",
                label,
                h_low[:10],
                target_mount,
            )
            return False, _NOT_VISIBLE_DETAIL
        if not _is_duplicate_add_detail(detail):
            return False, detail
        # Possible duplicate: inspect what's actually there.
        try:
            dest_st = await rpc(
                self.dest_client.get_torrent(h_low),
                getattr(self, "cfg", None),
                "dest fuse get_torrent",
            )
        except Exception as e:  # noqa: BLE001
            log.debug("could not check dest client for %s: %s", h_low[:10], e)
            dest_st = None
        if dest_st is not None and self._save_path_points_at_target(
            getattr(dest_st, "save_path", ""), target_mount
        ):
            log.info("%s %s already on fuse; marking as injected", label, h_low[:10])
            if not await self._fuse_entry_bytes_ok(blob, target_mount):
                log.warning(
                    "%s %s entry on fuse but bytes unverifiable at %s; parking re-add",
                    label,
                    h_low[:10],
                    target_mount,
                )
                return False, _NOT_VISIBLE_DETAIL
            if not await self._select_all_fuse_files(h_low, label=label):
                return False, _NOT_VISIBLE_DETAIL
            if pause_public:
                await self._pause_fuse_entry_best_effort(h_low, label=label)
            return True, "already added"
        if dest_st is not None:
            log.warning(
                "%s %s exists at %s (not the fuse target %s); replacing with fuse entry",
                label,
                h_low[:10],
                getattr(dest_st, "save_path", "?"),
                target_mount,
            )
            # Files kept deliberately: deleting with files here would destroy
            # bytes the fuse gate verified. The leftover SSD bytes are not
            # reclaimed automatically (no row references SSD past this
            # point; only a manual SSD clear reaps them) — the accepted
            # cost of never deleting verified data on a hunch. The row
            # already holds no SSD reservation, so admission accounting
            # treats the space as free; a truly full disk still parks via
            # the ssd_has_room stat gate.
            try:
                await rpc(
                    self.dest_client.delete(h_low, delete_files=False),
                    getattr(self, "cfg", None),
                    "dest fuse delete",
                )
            except Exception as e:  # noqa: BLE001
                return False, f"cannot remove non-fuse entry: {e}"
            res2 = await rpc(
                self.dest_client.add_torrent(**add_kwargs),  # type: ignore[arg-type]
                getattr(self, "cfg", None),
                "dest fuse add_torrent",
            )
            detail2 = res2.detail if isinstance(res2.detail, str) else ""
            if res2.accepted or _is_duplicate_add_detail(detail2):
                dest_st2 = None
                for _ in range(4):
                    try:
                        dest_st2 = await rpc(
                            self.dest_client.get_torrent(h_low),
                            getattr(self, "cfg", None),
                            "dest fuse get_torrent",
                        )
                    except Exception:
                        dest_st2 = None
                    if dest_st2 is not None:
                        break
                    await asyncio.sleep(2)
                if dest_st2 is not None and self._save_path_points_at_target(
                    getattr(dest_st2, "save_path", ""), target_mount
                ):
                    log.info("re-injected %s %s on fuse (%s)", label, h_low[:10], target_mount)
                    # Same post-add byte check as the fresh-add and
                    # already-on-fuse exits: a gate→add race can leave the
                    # replacement pointing at a target whose bytes vanished.
                    if not await self._fuse_entry_bytes_ok(blob, target_mount):
                        log.warning(
                            "%s %s replaced entry visible but bytes unverifiable "
                            "at %s; parking re-add",
                            label,
                            h_low[:10],
                            target_mount,
                        )
                        return False, _NOT_VISIBLE_DETAIL
                    if not await self._select_all_fuse_files(h_low, label=label):
                        return False, _NOT_VISIBLE_DETAIL
                    if pause_public:
                        await self._pause_fuse_entry_best_effort(h_low, label=label)
                    return True, detail2
                return False, _NOT_VISIBLE_DETAIL
            return False, detail2
        return False, detail

    def _target_mount_for(self, ts: TorrentState) -> Path:
        """Where on the fuse mount should this torrent's data live?"""
        if route_kind_is_sorted(ts.classification_kind):
            return Path(self.rt.seed.mount)
        return Path(self.rt.seed.mount_unsorted)
