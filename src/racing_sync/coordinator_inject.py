"""Manual inject-to-fuse flow (/inject) mixin.

Split out of the coordinator god-file. The operator moved release bytes
to the remote by hand; this drives verified groups into RE_ADDING (or
adopts them) with an all-or-nothing verify-first contract: any problem
reports WITHOUT changing state. Prior cancels auto-lift, SSD-active
rows abandon their partial first, DONE/RE_ADDING rows report no-op.

``_group_is_ignored`` lives here too (group veto rule); coordinator.py
re-exports it so existing import sites keep working.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .clients.abstract import Torrent
from .content_keys import same_content as _same_content_key
from .coordinator_content import _looks_public
from .io_bounds import chunked, offload, rpc
from .recovery import _save_path_on_ssd
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")


def _group_is_ignored(group: list[Torrent], store) -> bool:
    """True when any member hash of a same-content group is cancelled.

    Group members are duplicate client entries for one release, so one
    cancelled hash vetoes the whole group (otherwise the next duplicate
    would resurrect it on the following tick). Requires an actual `True`
    so bare MagicMock stores never veto in unit tests.
    """
    try:
        is_ignored = getattr(store, "is_ignored", None)
        if not callable(is_ignored):
            return False
        for t in group or []:
            try:
                if t.infohash and is_ignored(t.infohash) is True:
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


class InjectMixin:
    """Manual inject-to-fuse flow (duck-typed ``self``)."""

    # ---- Telegram /inject: operator-moved manual seeding ----

    # Row states /inject may drive into RE_ADDING (operator moved the
    # bytes to the remote by hand; the fuse gate re-verifies before any
    # skip_check inject). One command covers every case: waiting/failed rows
    # drive straight there, SSD-active rows (QUEUED/DOWNLOADING/MOVING)
    # abandon their partial download first (dest entry removed, reservation
    # released), cancelled/untracked groups are adopted; DONE/RE_ADDING are
    # no-ops.
    _INJECTFUSE_DRIVABLE = frozenset(
        {
            State.NEW,
            State.WAITING_INDEXER,
            State.WAITING_DISK,
            State.FAILED,
        }
    )
    _INJECTFUSE_ACTIVE = frozenset(
        {
            State.QUEUED,
            State.DOWNLOADING,
            State.MOVING,
        }
    )

    def _injectfuse_group_for(self, anchor, live: list) -> list:
        """Live VPS1 torrents holding the anchor's content.

        Same normalized release + matching total (0-size anchors match on
        name alone). Mirrors _cleanup_group_for (which works on a by-hash
        map) for a flat live list.
        """
        try:
            _anchor_name = getattr(anchor, "name", "") or ""
            if not _anchor_name.strip():
                return []
        except Exception:
            return []
        try:
            want_size = int(getattr(anchor, "size_bytes", 0) or 0)
        except (TypeError, ValueError):
            want_size = 0
        out = []
        for t in live or []:
            try:
                try:
                    t_size = int(getattr(t, "size_bytes", 0) or 0)
                except (TypeError, ValueError):
                    t_size = 0
                if not _same_content_key(
                    getattr(t, "name", "") or "",
                    t_size,
                    getattr(anchor, "name", "") or "",
                    want_size,
                    allow_unknown=True,
                ):
                    continue
                out.append(t)
            except Exception:
                continue
        return out

    def _injectfuse_find_row(self, members: list):
        """Tracked row for any group member hash (or normalized name+size)."""
        store = getattr(self, "store", None)
        if store is None or not members:
            return None
        try:
            get = getattr(store, "get", None)
            if callable(get):
                for t in members:
                    try:
                        h = (getattr(t, "infohash", "") or "").lower()
                    except Exception:
                        continue
                    if not h:
                        continue
                    try:
                        found = get(h, include_blob=False)
                    except Exception:
                        continue
                    if found is not None:
                        return found
        except Exception:
            pass
        try:
            anchor = members[0]
            name = getattr(anchor, "name", "") or ""
            size = int(getattr(anchor, "size_bytes", 0) or 0)
            for m in getattr(store, "find_by_name", lambda _n: [])(name) or []:
                try:
                    try:
                        m_size = int(getattr(m, "total_bytes", 0) or 0)
                    except (TypeError, ValueError):
                        m_size = 0
                    if not _same_content_key(
                        getattr(m, "source_name", "") or "",
                        m_size,
                        name,
                        size,
                        allow_unknown=True,
                    ):
                        continue
                    return m
                except Exception:
                    continue
        except Exception:
            pass
        return None

    async def injectfuse_resolve(self, infohash: str) -> dict:
        """Resolve a VPS1 infohash to its content group + tracked row.

        Category-agnostic (manual commands reach torrents the racing poll
        never ingests). Raises LookupError when unknown. Cancelled
        (ignored) groups are NOT refused: explicit /inject intent
        overrides the ignore — apply auto-lifts it so one command (plus
        Yes) is the whole manual path. No state change.
        """
        norm = (infohash or "").strip().lower()
        if len(norm) != 40 or not all(c in "0123456789abcdef" for c in norm):
            raise ValueError("send /inject <full 40-char hash> from VPS1")
        try:
            live = await rpc(
                self.source_client.list_torrents(),
                getattr(self, "cfg", None),
                "source injectfuse list",
            )
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"cannot list VPS1 torrents: {e}") from e
        anchor = None
        for t in live or []:
            try:
                if (getattr(t, "infohash", "") or "").lower() == norm:
                    anchor = t
                    break
            except Exception:
                continue
        if anchor is None:
            raise LookupError(f"hash {norm[:10]} not on VPS1 (typo, or already removed)")
        group = self._injectfuse_group_for(anchor, live)
        if not group:
            raise LookupError(f"hash {norm[:10]} not on VPS1")
        try:
            ignored = bool(_group_is_ignored(group, getattr(self, "store", None)))
        except Exception:
            ignored = False
        primary = next(
            (t for t in group if _looks_public(getattr(t, "trackers", None) or [])),
            group[0],
        )
        return {
            "primary": primary,
            "members": group,
            "row": self._injectfuse_find_row(group),
            "title": (getattr(primary, "name", "") or norm[:10])[:60],
            "size": int(getattr(primary, "size_bytes", 0) or 0),
            "ignored": ignored,
        }

    async def _injectfuse_verify_one(
        self,
        t,
    ) -> tuple[str, bytes | None, list[str]]:
        """Verify one group member: fetchable bytes + fuse presence.

        Pure check, no state change. Split out so the group verify can
        run members concurrently (each member pays an SFTP/export fetch
        plus a fuse stat; serial groups stall the confirm round-trip).
        """
        try:
            h = (getattr(t, "infohash", "") or "").lower()
            name = (getattr(t, "name", "") or h[:10])[:50]
        except Exception:
            return "", None, []
        if not h:
            return "", None, []
        try:
            blob = await self._fetch_racing_torrent_bytes(h)
        except Exception as e:  # noqa: BLE001
            blob = None
            log.debug("injectfuse: fetch %s failed: %s", h[:10], e)
        if not blob:
            return h, None, [f"{name}: no .torrent bytes (retry later)"]
        blobs = {h: bytes(blob)}
        try:
            expected = self._expected_fuse_files(blob)
        except Exception:
            expected = None
        if not expected:
            return h, None, [f"{name}: undecodable .torrent"]
        try:
            kind = self._classify_blob_kind(blob)
            try:
                fallback = Path(self.cfg.rclone.fuse.mount_unsorted)
            except Exception:
                fallback = Path(".")
            target = self._target_mount_for_kind(kind, fallback)
            if not self._save_path_is_on_fuse(str(target)):
                # Never stat the SSD dir as fuse: bytes present on SSD
                # are not on fuse, and a false "present" would pass
                # verification for data never seeded. Fail closed.
                raise ValueError(f"fuse target {target} not on a fuse mount")
        except Exception:
            return h, None, [f"{name}: no fuse target"]
        try:
            missing = await self._missing_fuse_files(target, expected)
        except Exception as e:  # noqa: BLE001
            return h, None, [f"{name}: fuse check failed ({e})"]
        if missing:
            if len(missing) == 1 and not str(missing[0]).startswith("<"):
                return h, None, [f"{name}: missing {missing[0]}"]
            shown = ", ".join(str(m)[:60] for m in missing[:3])
            if len(missing) > 3:
                shown += f" (+{len(missing) - 3} more)"
            return h, None, [f"{name}: {len(missing)} missing at {target} (e.g. {shown})"]
        return h, blobs[h], []

    async def injectfuse_verify(self, members: list) -> dict:
        """All-or-nothing fuse readiness for a group. No state change.

        Every member needs fetchable .torrent bytes AND zero missing /
        size-mismatched files at its blob-derived target mount. Members
        verify concurrently (4-wide); results merge in member order so
        the problem list is stable. Returns {"ready", "blobs",
        "problems"} — problems lists "name: reason".
        """
        members = list(members or [])
        _sem = asyncio.Semaphore(4)

        async def _one(t):
            async with _sem:
                return await self._injectfuse_verify_one(t)

        _results = await asyncio.gather(*(_one(t) for t in members))
        problems: list[str] = []
        blobs: dict[str, bytes] = {}
        ready = True
        for h, blob, probs in _results:
            if probs:
                ready = False
                problems.extend(probs)
                continue
            if h and blob:
                blobs[h] = blob
        return {"ready": ready and bool(members), "blobs": blobs, "problems": problems}

    def _injectfuse_lift_cancel(self, members: list) -> bool:
        """Clear ignore entries + forget tombstones for a group (best-effort).

        Explicit /inject intent overrides a prior /cancel: returns True
        when anything was lifted so callers can name it in the reply.
        """
        lifted = False
        store = getattr(self, "store", None)
        if store is None:
            return False
        seen: set[str] = set()
        for t in members or []:
            try:
                h = (getattr(t, "infohash", "") or "").lower()
            except Exception:
                continue
            if not h or h in seen:
                continue
            seen.add(h)
            try:
                if store.unignore_torrent(h):
                    lifted = True
            except Exception:
                pass
            try:
                if store.clear_tombstone(h):
                    lifted = True
            except Exception:
                pass
        return lifted

    def _injectfuse_unhold(self, row) -> None:
        """Clear a /skip hold so the RE_ADDING worker actually runs."""
        try:
            if not getattr(row, "skipped", 0):
                return
        except Exception:
            return
        store = getattr(self, "store", None)
        if store is None:
            return
        try:
            upd = getattr(store, "update_columns", None)
            if callable(upd):
                upd(row.source_infohash, {"skipped": 0})
            else:
                row.skipped = 0
                store.upsert(row)
        except Exception:
            pass

    async def _injectfuse_preempt_ssd(self, row) -> None:
        """Abandon an SSD-active download so the row can go RE_ADDING.

        Releases the SSD reservation, drops the frozen batch cap, and
        best-effort deletes the SSD-side dest entries. Files are only
        deleted for entries whose save_path is under a configured SSD
        root (`_save_path_on_ssd`); fuse entries and foreign/unknown
        paths (other mounts, network shares, empty save_path) are removed
        file-less so the preempt never deletes data we do not own.
        Never raises: a leftover partial only wastes SSD until the next
        janitor pass; the state drive matters.
        """
        try:
            await self._ssd_release(row.source_infohash or "")
        except Exception:
            pass
        try:
            self._drop_frozen_batch_cap(row)
        except Exception:
            pass
        dest = getattr(self, "dest_client", None)
        if dest is None:
            return
        hashes: list[str] = []
        for raw in (
            getattr(row, "dest_infohash", "") or "",
            getattr(row, "cross_seed_infohash", "") or "",
            getattr(row, "source_infohash", "") or "",
        ):
            h = (raw or "").strip().lower()
            if h and h not in hashes:
                hashes.append(h)
        try:
            for extra in (getattr(row, "injected_private_hashes", "") or "").split(","):
                h = (extra or "").strip().lower()
                if h and h not in hashes:
                    hashes.append(h)
        except Exception:
            pass
        if not hashes:
            return
        try:
            _parts = await asyncio.gather(
                *(
                    rpc(
                        dest.list_torrents(hashes=_chunk),
                        getattr(self, "cfg", None),
                        "injectfuse preempt list",
                    )
                    for _chunk in chunked(hashes)
                )
            )
        except Exception as e:  # noqa: BLE001
            log.debug(
                "injectfuse: preempt list failed for %s: %s", (row.source_infohash or "")[:10], e
            )
            return
        entries: list = []
        for _part in _parts or []:
            if isinstance(_part, list):
                entries.extend(_part)
        if not entries:
            return
        _sem = asyncio.Semaphore(4)

        async def _drop(entry) -> None:
            async with _sem:
                try:
                    h = (getattr(entry, "hash", "") or "").lower()
                    if not h:
                        return
                    # Only a save_path proven to sit under a configured
                    # SSD root owns local bytes. Fuse entries and any
                    # foreign/unknown path (other disk, network share,
                    # empty save_path) are removed file-less: `on_fuse`
                    # alone is not ownership (audit A3).
                    cfg = getattr(self, "cfg", None)
                    save_path = getattr(entry, "save_path", "") or ""
                    on_ssd = False
                    try:
                        on_ssd = bool(cfg is not None and _save_path_on_ssd(cfg, save_path))
                    except Exception:
                        on_ssd = False
                    await rpc(dest.delete(h, delete_files=on_ssd), cfg, "injectfuse preempt delete")
                    log.info(
                        "injectfuse: preempt removed entry %s (delete_files=%s)", h[:10], on_ssd
                    )
                except Exception as e:  # noqa: BLE001
                    log.debug("injectfuse: preempt delete failed: %s", e)

        await asyncio.gather(*(_drop(e) for e in entries))

    def _injectfuse_verified_blob(self, resolved: dict, blobs: dict) -> bytes | None:
        """Primary verified .torrent bytes (fallback: first available)."""
        try:
            primary = resolved.get("primary")
            p_hash = (getattr(primary, "infohash", "") or "").lower()
        except Exception:
            p_hash = ""
        if isinstance(blobs, dict):
            try:
                if p_hash and blobs.get(p_hash):
                    return bytes(blobs[p_hash])
            except Exception:
                pass
            try:
                for v in blobs.values():
                    if v:
                        return bytes(v)
            except Exception:
                pass
        return None

    async def _injectfuse_blob_available(self, row, resolved: dict, blobs: dict) -> bool:
        """True when a blob is on hand for the row (no mutation).

        Pre-flight for destructive steps: verify-first means the SSD
        preempt never runs when the drive would fail on missing bytes
        right after. Dest entries are only deleted once the follow-up
        ensure is known to succeed.
        """
        try:
            if getattr(row, "_blob", None) or getattr(row, "cross_seed_blob", None):
                return True
        except Exception:
            pass
        try:
            store = getattr(self, "store", None)
            if store is not None and hasattr(store, "get_blob"):
                if await offload(store.get_blob, row.source_infohash or ""):
                    return True
        except Exception:
            pass
        try:
            return bool(self._injectfuse_verified_blob(resolved, blobs))
        except Exception:
            return False

    async def _injectfuse_ensure_blob(self, row, resolved: dict, blobs: dict) -> bool:
        """Carry the verified blob onto a tracked row (in-memory).

        The fuse gate reads `ts._blob or cross_seed_blob or store blob`:
        a tracked row driven without one parks forever on "missing blob".
        Only fills when the DB has nothing (never overwrites a good
        prowlarr/SFTP blob); the following transition write persists it
        in the same guarded UPDATE, so no version race. True when the row
        now carries blob bytes. The store read is sync SQLite, offloaded
        so a slow disk never stalls the event loop.
        """
        try:
            if getattr(row, "_blob", None) or getattr(row, "cross_seed_blob", None):
                return True
        except Exception:
            pass
        try:
            store = getattr(self, "store", None)
            if store is not None and hasattr(store, "get_blob"):
                if await offload(store.get_blob, row.source_infohash or ""):
                    return True
        except Exception:
            pass
        blob = self._injectfuse_verified_blob(resolved, blobs)
        if not blob:
            return False
        try:
            row.cross_seed_blob = bytes(blob)
        except Exception:
            return False
        try:
            row._blob = bytes(blob)
        except Exception:
            pass
        return True

    async def injectfuse_apply(self, resolved: dict, blobs: dict[str, bytes]) -> str:
        """Drive a verified group into RE_ADDING (or create its row).

        Callers must run injectfuse_verify first and pass its blobs: on
        ANY problem the caller reports and changes nothing — this method
        assumes verification passed. Single manual path for every case:
        a prior /cancel is auto-lifted, SSD-active rows abandon their
        partial download first; DONE/RE_ADDING rows report no-op.
        """
        members = list(resolved.get("members") or [])
        primary = resolved.get("primary")
        title = str(resolved.get("title") or "group")[:60]
        if not members or primary is None:
            raise LookupError("empty group; nothing to inject")
        lifted = self._injectfuse_lift_cancel(members)
        lift_note = " (cancel lifted)" if lifted else ""
        # Re-resolve the row: the poller may have ingested it since resolve.
        row = self._injectfuse_find_row(members)
        if row is not None:
            try:
                fresh = self.store.get(row.source_infohash, include_blob=False)
            except Exception:
                fresh = row
            if fresh is None:
                raise LookupError("already gone from tracking")
            row = fresh
            if row.state in (State.DONE, State.RE_ADDING):
                return f"already {row.state.value}: {title} (fuse gate runs next tick)"
            if row.state in self._INJECTFUSE_ACTIVE:
                prev = row.state.value
                if not await self._injectfuse_blob_available(row, resolved, blobs):
                    raise LookupError("primary .torrent bytes missing; run again")
                await self._injectfuse_preempt_ssd(row)
                try:
                    fresh2 = self.store.get(row.source_infohash, include_blob=False)
                    if fresh2 is not None:
                        row = fresh2
                except Exception:
                    pass
                self._injectfuse_unhold(row)
                if not await self._injectfuse_ensure_blob(row, resolved, blobs):
                    raise LookupError("primary .torrent bytes missing; run again")
                self.transition(row, State.RE_ADDING)
                return (
                    f"injecting {len(members)} torrent(s) for {title} "
                    f"(SSD {prev} stopped{lift_note}; "
                    "fuse gate runs next tick)"
                )
            if row.state not in self._INJECTFUSE_DRIVABLE:
                raise LookupError(
                    f"{title} is {row.state.value}; manual inject needs a waiting/failed row"
                )
            self._injectfuse_unhold(row)
            if not await self._injectfuse_ensure_blob(row, resolved, blobs):
                raise LookupError("primary .torrent bytes missing; run again")
            self.transition(row, State.RE_ADDING)
            return (
                f"injecting {len(members)} torrent(s) for {title} "
                f"(fuse gate runs next tick{lift_note})"
            )
        try:
            p_hash = (getattr(primary, "infohash", "") or "").lower()
            p_name = getattr(primary, "name", "") or p_hash[:10]
            try:
                p_size = int(getattr(primary, "size_bytes", 0) or 0)
            except (TypeError, ValueError):
                p_size = 0
            trackers = list(getattr(primary, "trackers", None) or [])
        except Exception:
            raise LookupError("group changed; run the command again") from None
        blob = blobs.get(p_hash) if isinstance(blobs, dict) else None
        if not blob:
            raise LookupError("primary .torrent bytes missing; run again")
        try:
            base = str(self.cfg.dest.save_path)
        except Exception:
            base = ""
        ts = TorrentState(
            source_infohash=p_hash,
            source_name=p_name,
            total_bytes=p_size,
            source_announce_url=trackers[0] if trackers else "",
            source_tracker=trackers[0] if trackers else "",
            cross_seed_infohash=p_hash,
            cross_seed_source="injectfuse",
            cross_seed_blob=bytes(blob),
            classification_kind="unknown",
            save_path=base,
            state=State.NEW,
        )
        try:
            ts._blob = bytes(blob)
        except Exception:
            pass
        try:
            # NEW + transition (not a bare RE_ADDING upsert): the insert
            # is the standard ingestion edge, then the guarded transition
            # refuses tombstoned/concurrent rows and runs the log +
            # Telegram + SSD-cleanup hooks like every other drive.
            # (coordinator.transition itself refuses brand-new rows via
            # the abandoned-row guard, so the NEW insert comes first.)
            self.store.upsert(ts)
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"could not track group: {e}") from e
        try:
            fresh = self.store.get(p_hash, include_blob=False)
        except Exception:
            fresh = None
        if fresh is None:
            raise LookupError("could not track group: row vanished after insert")
        try:
            fresh.cross_seed_blob = bytes(blob)
        except Exception:
            pass
        try:
            fresh._blob = bytes(blob)
        except Exception:
            pass
        try:
            self.transition(fresh, State.RE_ADDING)
        except Exception as e:  # noqa: BLE001
            raise LookupError(f"could not track group: {e}") from e
        try:
            self._drop_frozen_batch_cap(ts)
        except Exception:
            pass
        log.info(
            "injectfuse: adopted %s (%d torrent(s)) straight to re_adding",
            p_name[:60],
            len(members),
        )
        return (
            f"injecting {len(members)} torrent(s) for {p_name[:60]} "
            "(adopted; fuse gate runs next tick)"
        )

    async def injectfuse_confirmed(self, hashes: list[str]) -> str:
        """Execute a confirmed /inject: resolve live, verify, drive.

        All-or-nothing per group: any verification problem reports WITHOUT
        changing state (tracked rows keep their prior state; untracked
        groups gain no row). Every confirmed hash is resolved and driven
        (hashes landing in one content group drive once); the live VPS1
        group at confirm time governs (frozen question hashes only seed
        the lookup). Short strings — Telegram truncates for chat.
        """
        clean: list[str] = []
        for h in hashes or []:
            try:
                n = (h or "").strip().lower()
            except Exception:
                continue
            if len(n) == 40 and all(c in "0123456789abcdef" for c in n) and n not in clean:
                clean.append(n)
        if not clean:
            return "Not injected — nothing to verify (expired?)."
        outs: list[str] = []
        driven: set[str] = set()
        for h in clean:
            try:
                resolved = await self.injectfuse_resolve(h)
            except (LookupError, ValueError) as e:
                outs.append(f"Not injected — {e}")
                continue
            except Exception as e:  # noqa: BLE001
                outs.append(f"Not injected — verify failed ({e})")
                continue
            try:
                _primary = resolved.get("primary")
                _p_hash = (getattr(_primary, "infohash", "") or "").lower()
            except Exception:
                _p_hash = ""
            if _p_hash and _p_hash in driven:
                continue
            if _p_hash:
                driven.add(_p_hash)
            try:
                verified = await self.injectfuse_verify(resolved.get("members") or [])
            except Exception as e:  # noqa: BLE001
                outs.append(f"Not injected — verify failed ({e})")
                continue
            if not verified.get("ready"):
                suffix = ""
                try:
                    row = resolved.get("row")
                    if row is not None and getattr(row, "state", None) is not None:
                        suffix = f" — still {row.state.value}"
                except Exception:
                    suffix = ""
                probs = list(verified.get("problems") or ["unverifiable"])
                extra = f" (+{len(probs) - 3} more)" if len(probs) > 3 else ""
                outs.append(f"Not injected{suffix}: " + "; ".join(probs[:3]) + extra)
                continue
            try:
                outs.append(await self.injectfuse_apply(resolved, verified.get("blobs") or {}))
            except (LookupError, ValueError) as e:
                outs.append(f"Not injected — {e}")
            except Exception as e:  # noqa: BLE001
                outs.append(f"Not injected — {e}")
        return "\n".join(outs) if outs else "Not injected — nothing to verify (expired?)."
