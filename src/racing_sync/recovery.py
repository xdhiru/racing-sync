"""Recovery reconciler (req #4).

On startup, look at every torrent currently in VPS2's qBittorrent and compare
it against the state DB + filesystem reality. Decide what should have
happened vs what did, and either:
  - skip (state matches reality)
  - resume from the right state (download incomplete / move in progress)
  - force a re-add (DB says DONE but torrent is missing on fuse)

This is the safety net against prior crashes.
"""

from __future__ import annotations

import glob
import logging
from collections.abc import Iterable
from pathlib import Path

from .clients.abstract import TorrentClient, TorrentFile
from .config import AppConfig
from .rclone_ops import ssd_free_bytes
from .state import State, StateStore, TorrentState

log = logging.getLogger(__name__)


class RecoveryReport:
    def __init__(self) -> None:
        self.kept: list[str] = []
        self.resumed: list[str] = []
        self.re_added: list[str] = []
        self.orphans: list[str] = []   # in DB but not on VPS2
        self.unknowns: list[str] = []  # on VPS2 but not in DB

    def summary(self) -> str:
        return (
            f"kept={len(self.kept)} resumed={len(self.resumed)} "
            f"re_added={len(self.re_added)} orphans={len(self.orphans)} "
            f"unknowns={len(self.unknowns)}"
        )


async def reconcile(
    cfg: AppConfig,
    *,
    dest: TorrentClient,
    store: StateStore,
) -> RecoveryReport:
    """Walk VPS2's torrents, compare with state DB, fix mismatches.

    We filter VPS2's torrent list to the racing category because the
    state DB only tracks racing-originated torrents. The other long-
    term seeds on VPS2 are not ours to manage.
    """
    rpt = RecoveryReport()

    # 1. Snapshot reality — restrict to the racing category so we
    #    don't churn through 7000+ long-term seeds on every startup.
    actual = await dest.list_torrents(category="racing")
    actual_by_hash: dict[str, object] = {t.hash.lower(): t for t in actual}

    # 2. Snapshot DB
    all_rows = store.all()

    for ts in all_rows:
        h = ts.source_infohash.lower()
        known_hashes = {
            k.lower()
            for k in (ts.source_infohash, ts.dest_infohash, ts.cross_seed_infohash)
            if k
        }
        present = any(k in actual_by_hash for k in known_hashes)
        if ts.state == State.DONE:
            if present:
                rpt.kept.append(h)
            else:
                # Lost — re-add pointing at fuse. The data is on remote.
                rpt.re_added.append(h)
                ts.state = State.RE_ADDING
                store.upsert(ts)
        elif ts.state == State.FAILED:
            # Leave for manual retry
            rpt.kept.append(h)
        elif ts.state in (State.NEW, State.QUERYING, State.WAITING_DISK):
            # Not yet added to VPS2 — safe to leave for coordinator to process
            rpt.resumed.append(h)
        else:
            # In-flight (QUEUED, DOWNLOADING, MOVING, RE_ADDING): check if torrent still exists
            if present:
                rpt.resumed.append(h)
            else:
                rpt.orphans.append(h)
                sftp_bytes = store.get_blob(ts.source_infohash) or None
                await fix_orphan(ts, cfg, dest=dest, store=store, sftp_bytes=sftp_bytes)

    # 3. Anything on VPS2 not in the DB?
    # If it is already seeding from the fuse mount, adopt it as DONE.
    # If it is 100% complete on SSD, adopt it as MOVING so coordinator can move it to remote.
    db_hashes: set[str] = set()
    for ts in all_rows:
        for k in (ts.source_infohash, ts.dest_infohash, ts.cross_seed_infohash):
            if k:
                db_hashes.add(k.lower())
        if ts.injected_private_hashes:
            for iph in ts.injected_private_hashes.split(","):
                if iph.strip():
                    db_hashes.add(iph.strip().lower())

    fuse_mounts = [
        str(fm).rstrip("/\\").replace("\\", "/")
        for fm in (cfg.rclone.fuse.mount, cfg.rclone.fuse.mount_unsorted)
        if fm
    ]
    for h, t in actual_by_hash.items():
        if h not in db_hashes:
            save_path = getattr(t, "save_path", "").rstrip("/\\").replace("\\", "/")
            on_fuse = any(save_path.startswith(fm) for fm in fuse_mounts if fm)
            comp = getattr(t, "is_complete", False)
            is_done = comp() if callable(comp) else bool(comp)
            if on_fuse or is_done:
                name = getattr(t, "name", h)
                matches = store.find_by_name(name)
                if matches:
                    existing = matches[0]
                    curr = [x for x in existing.injected_private_hashes.split(",") if x]
                    if h not in curr and h.lower() != existing.source_infohash.lower():
                        curr.append(h)
                        existing.injected_private_hashes = ",".join(curr)
                        store.upsert(existing)
                        rpt.kept.append(h)
                        log.info(
                            "reconcile: linked existing cross-seed %s to adopted release %s",
                            h[:10], name,
                        )
                        continue
                adopt_state = State.DONE if on_fuse else State.MOVING
                log.info(
                    "reconcile: adopting existing completed/fuse torrent on VPS2 as %s: %s (%s)",
                    adopt_state.value, name, h[:10],
                )
                ts = TorrentState(
                    source_infohash=h,
                    source_name=name,
                    dest_infohash=h,
                    save_path=save_path,
                    total_bytes=getattr(t, "size_bytes", 0),
                    state=adopt_state,
                )
                store.upsert(ts)
                rpt.kept.append(h)
            else:
                rpt.unknowns.append(h)

    log.info("recovery: %s", rpt.summary())
    return rpt


async def fix_orphan(
    ts: TorrentState,
    cfg: AppConfig,
    *,
    dest: TorrentClient,
    store: StateStore,
    sftp_bytes: bytes | None = None,
) -> str:
    """Decide how to fix a missing in-flight torrent and apply it.

    Returns the new state value (string) after the fix.
    """
    h = ts.source_infohash

    if ts.state in (State.DOWNLOADING, State.QUEUED):
        # The torrent disappeared from VPS2 but DB still expects SSD work.
        # We need to re-add it. Caller (coordinator) provides .torrent bytes.
        if sftp_bytes is None:
            log.error("orphan %s: no .torrent bytes available to re-add", h)
            store.transition(ts, State.FAILED, error="orphan: no .torrent bytes")
            return State.FAILED.value
        # Add paused, then resume and let coordinator drive.
        await dest.add_torrent(
            torrent_files=[sftp_bytes],
            save_path=ts.save_path or str(cfg.dest.save_path),
            category="racing",
            paused=True,
            skip_check=False,
        )
        if ts.state != State.DOWNLOADING:
            store.transition(ts, State.DOWNLOADING)
        else:
            store.upsert(ts)
        return State.DOWNLOADING.value

    if ts.state == State.RE_ADDING:
        log.info("orphan %s: already in RE_ADDING; will re-add to fuse", h)
        return State.RE_ADDING.value

    if ts.state == State.MOVING:
        src_path = Path(ts.save_path) if ts.save_path else Path(cfg.dest.save_path)
        escaped_name = glob.escape(ts.source_name)
        content_exists = (
            (src_path / ts.source_name).exists()
            or any(src_path.glob(f"{escaped_name}*"))
        )
        if content_exists:
            log.info("orphan %s: files still exist on SSD; will resume move", h)
            return State.MOVING.value

        # Files no longer on SSD; rclone move completed, proceed to re-add to fuse
        log.info("orphan %s: files moved from SSD; will re-add to fuse", h)
        store.transition(ts, State.RE_ADDING)
        return State.RE_ADDING.value

    log.warning("orphan %s: cannot infer recovery path from state %s",
                h, ts.state.value)
    store.transition(ts, State.FAILED, error=f"orphan in state {ts.state.value}")
    return State.FAILED.value