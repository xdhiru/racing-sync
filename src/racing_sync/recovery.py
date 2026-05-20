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
        present = h in actual_by_hash
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
        else:
            # In-flight: check if torrent still exists
            if present:
                rpt.resumed.append(h)
            else:
                rpt.orphans.append(h)

    # 3. Anything on VPS2 not in the DB?
    # If it is already seeding from the fuse mount or 100% complete, adopt it into state DB as DONE
    # so we don't treat it as a new release and re-download/re-move it.
    db_hashes = {ts.source_infohash.lower() for ts in all_rows}
    fuse_mounts = [
        str(cfg.rclone.fuse.mount).rstrip("/"),
        str(cfg.rclone.fuse.mount_unsorted).rstrip("/"),
    ]
    for h, t in actual_by_hash.items():
        if h not in db_hashes:
            save_path = getattr(t, "save_path", "").rstrip("/")
            on_fuse = any(save_path.startswith(fm) for fm in fuse_mounts if fm)
            is_done = getattr(t, "is_complete", lambda: False)()
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
                log.info(
                    "reconcile: adopting existing completed/fuse torrent on VPS2 as DONE: %s (%s)",
                    name, h[:10],
                )
                ts = TorrentState(
                    source_infohash=h,
                    source_name=name,
                    dest_infohash=h,
                    save_path=save_path,
                    total_bytes=getattr(t, "size_bytes", 0),
                    state=State.DONE,
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

    if ts.state == State.MOVING:
        # The torrent was being moved. We assume rclone already ran and the
        # files now live on the remote. Re-add to fuse.
        log.info("orphan %s: assuming rclone move completed; will re-add", h)
        store.transition(ts, State.RE_ADDING)
        return State.RE_ADDING.value

    log.warning("orphan %s: cannot infer recovery path from state %s",
                h, ts.state.value)
    store.transition(ts, State.FAILED, error=f"orphan in state {ts.state.value}")
    return State.FAILED.value