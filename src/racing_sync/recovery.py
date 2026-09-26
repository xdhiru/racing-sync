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

import asyncio
import glob
import logging
from collections.abc import Iterable
from pathlib import Path

from .clients.abstract import TorrentClient, TorrentFile
from .config import AppConfig
from .coordinator_errors import AbandonedError
from .io_bounds import bounded, offload, rpc
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
        self.adopted: list[str] = []   # fresh DB rows rebuilt from VPS2 state

    def summary(self) -> str:
        return (
            f"kept={len(self.kept)} resumed={len(self.resumed)} "
            f"re_added={len(self.re_added)} orphans={len(self.orphans)} "
            f"unknowns={len(self.unknowns)} adopted={len(self.adopted)}"
        )


def _safe_top(name: str) -> str | None:
    """First path segment or None when the torrent name is unsafe.

    Rejects absolute paths, drive letters, and any ``..`` segment so a
    crafted name can never escape the SSD/fuse root via ``(root / top)``.
    """
    norm = name.replace("\\", "/").strip()
    if not norm or norm.startswith("/") or ".." in norm.split("/"):
        return None
    # Windows drive (C:/...) or UNC would escape the root join.
    if len(norm) >= 2 and norm[1] == ":":
        return None
    norm = norm.strip("/")
    if not norm:
        return None
    top = norm.split("/")[0]
    if top in ("", ".", ".."):
        return None
    return top


def _safe_join(root: Path, name: str) -> Path | None:
    """Join torrent-relative `name` under `root`, or None when unsafe."""
    norm = name.replace("\\", "/").strip()
    if not norm or norm.startswith("/") or ".." in norm.split("/"):
        return None
    if len(norm) >= 2 and norm[1] == ":":
        return None
    return root / norm


def _hash_is_ignored(store, infohash: str) -> bool:
    """Strict bool check against the ignore list (cancelled releases).

    Requires an actual `True` (not truthy): bare MagicMock stores answer
    truthy to everything and must never veto adoption in unit tests.
    """
    try:
        fn = getattr(store, "is_ignored", None)
        if not callable(fn):
            return False
        return fn(infohash) is True
    except Exception:
        return False


def _save_path_on_ssd(cfg: AppConfig, save_path: str) -> bool:
    """True iff a client entry's save_path lives under a configured SSD root.

    Gates DOWNLOADING adoption of partial torrents: entries pointing anywhere
    else (stale paths, other mounts) stay unknowns. Defensive against test
    doubles — unresolvable roots simply yield False.
    """
    from .coordinator_content import fold_path_case

    sp = fold_path_case((save_path or "").rstrip("/\\").replace("\\", "/"))
    if not sp:
        return False
    roots: list[str] = []
    for raw in (getattr(cfg.dest, "save_path", None), getattr(cfg.ssd, "path", None)):
        # Real configs always carry str/Path here; anything else (e.g. a
        # MagicMock in unit tests) means membership is unknowable -> False.
        if not isinstance(raw, (str, Path)):
            continue
        r = fold_path_case(str(raw).rstrip("/\\").replace("\\", "/"))
        if r:
            roots.append(r)
    if not roots:
        return False
    return any(sp == r or sp.startswith(r + "/") for r in roots)


def find_content_on_ssd(cfg: AppConfig, expected: list[tuple[str, int]]) -> Path | None:
    """Locate single-top content under the SSD roots.

    Used when a client entry claims a fuse location but the bytes aren't
    there (never-moved SSD data with a wrong entry path). Only unambiguous
    single-top layouts qualify; returns the SSD root dir or None.
    Local SSD stats are cheap — no threading needed.
    """
    tops: set[str] = set()
    for name, _ in expected:
        top = _safe_top(name)
        if top is None:
            return None
        tops.add(top)
    if len(tops) != 1:
        return None
    top = next(iter(tops))
    roots: list[Path] = []
    for raw in (getattr(cfg.dest, "save_path", None), getattr(cfg.ssd, "path", None)):
        try:
            p = raw if isinstance(raw, Path) else Path(str(raw))
        except Exception:
            continue
        if p not in roots:
            roots.append(p)
    for root in roots:
        try:
            cand = root / top
            if not cand.exists():
                continue
            if cand.is_dir():
                # An empty directory proves nothing (wiped leftovers);
                # only adopt when real bytes remain.
                try:
                    if not any(cand.iterdir()):
                        continue
                except OSError:
                    continue
            return root
        except OSError:
            continue
    return None


def _classify_kind_for_files(files: list, cfg: AppConfig) -> str:
    """Best-effort classify of a dest file list; never raises.

    Fresh-DB adoptions previously left `classification_kind="unknown"`, which
    made `_target_mount_for()` route movies/seasons to the unsorted mount and
    sent late cross-seeds (and their fuse gate) at the wrong directory.
    Returns "unknown" when files are missing or classification fails (e.g.
    MagicMock cfg in unit tests).
    """
    try:
        if not files:
            return "unknown"
        from .classifier import classify as _classify

        kind = _classify(files, cfg).kind
        return kind if isinstance(kind, str) and kind else "unknown"
    except Exception:
        return "unknown"


async def _classify_adopted(
    cfg: AppConfig, dest: TorrentClient, h: str
) -> str:
    """Fetch dest files and classify; "unknown" on any failure."""
    try:
        files = await rpc(dest.get_torrent_files(h), cfg,
                          "reconcile classify get_torrent_files")
    except Exception:
        return "unknown"
    try:
        return _classify_kind_for_files(files, cfg)
    except Exception:
        return "unknown"


async def _adopt_blob(dest: TorrentClient, h: str) -> bytes:
    """Best-effort .torrent bytes for a freshly adopted entry (b"" when absent).

    Adopted rows (fresh DB, --reset) previously carried no blob, so every
    downstream consumer needing bytes (batch re-resolve, re-add) parked or
    wedged identically forever. Export from the dest client when it
    supports it; failures keep the old blobless behavior.
    """
    try:
        export = getattr(dest, "export_torrent", None)
        if not callable(export):
            return b""
        blob = await bounded(export(h), timeout=30.0,
                             label="reconcile export_torrent")
        if isinstance(blob, (bytes, bytearray)) and blob:
            return bytes(blob)
        return b""
    except Exception:
        return b""


async def _missing_under(mount: Path, expected: list[tuple[str, int]]) -> list[str] | None:
    """Files listed-but-absent under `mount`, or None when unverifiable.

    Unverifiable (RPC/stat failure) returns None so callers preserve today's
    trust behavior — absence of evidence must never manufacture terminal
    failures (a warming/dead mount also shows nothing).
    """
    def _check() -> list[str]:
        missing: list[str] = []
        for name, want in expected:
            target = _safe_join(mount, name)
            if target is None:
                missing.append(name)
                continue
            try:
                actual = target.stat().st_size
            except OSError:
                missing.append(name)
                continue
            if want and actual != want:
                missing.append(f"{name} (size {actual}!={want})")
        return missing

    try:
        return await asyncio.to_thread(_check)
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile: fuse availability check failed for %s: %s", mount, e)
        return None


async def _verify_fuse_adopted(
    cfg: AppConfig, dest: TorrentClient, t: object, save_path: str
) -> tuple[bool, str, str, bool]:
    """Decide how to adopt an on-fuse + complete entry.

    Returns (adopt_as_done, effective_save_path, classification_kind,
    bytes_verified). A client entry added with skip_check=True reports
    complete with zero bytes present, so bytes are verified before
    trusting DONE. Verification may only upgrade handling toward a
    verified-good path (SSD content found -> MOVING with corrected
    path); every other outcome preserves today's DONE adoption while
    logging loudly — late injections stay gated downstream regardless.

    bytes_verified is True only when bytes were actually observed
    (fuse files all present, or content found on SSD). Unverifiable
    outcomes (RPC failure, empty file list, warming mount) keep DONE
    but leave the flag clear so the janitor re-probes instead of
    clearing the VPS1 copy on trust alone.
    """
    h = str(getattr(t, "hash", "") or "")
    name = str(getattr(t, "name", "") or h)
    try:
        files = await rpc(dest.get_torrent_files(h), cfg,
                          "reconcile verify get_torrent_files")
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile: cannot list files for %s; keeping DONE trust: %s",
                    h[:10], e)
        return True, save_path, "unknown", False
    try:
        expected = [
            (str(f.name), int(f.size_bytes or 0))
            for f in (files or []) if getattr(f, "name", "")
        ]
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile: cannot decode file list for %s; keeping DONE trust: %s",
                    h[:10], e)
        return True, save_path, "unknown", False
    kind = _classify_kind_for_files(files, cfg)
    if not expected:
        return True, save_path, kind, False
    try:
        mount = Path(save_path)
    except Exception:
        return True, save_path, kind, False
    missing = await _missing_under(mount, expected)
    if missing is None:
        return True, save_path, kind, False
    if not missing:
        return True, save_path, kind, True
    ssd_root = find_content_on_ssd(cfg, expected)
    if ssd_root is not None:
        log.warning(
            "reconcile: %s claims fuse %s but %d/%d files missing; content found on SSD at %s — adopting as MOVING",
            name[:60], save_path, len(missing), len(expected), ssd_root,
        )
        return False, str(ssd_root), kind, True
    log.warning(
        "reconcile: %s claims fuse %s but %d/%d files missing (e.g. %s); keeping DONE (mount may be warming); late injections stay gated",
        name[:60], save_path, len(missing), len(expected), missing[0],
    )
    return True, save_path, kind, False


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

    Full synchronous pass (snapshot + inline verification), used by the
    API recover endpoint and existing callers. Startup uses the split
    form instead: `_reconcile_snapshot(verify=False)` for a fast serve,
    then `reconcile_verify()` in the background.
    """
    rpt = await _reconcile_snapshot(cfg, dest=dest, store=store, verify=True)
    return rpt


async def reconcile_verify(
    cfg: AppConfig,
    *,
    dest: TorrentClient,
    store: StateStore,
    concurrency: int = 4,
) -> dict[str, int]:
    """Background verification for optimistically adopted rows.

    Runs after startup serves: for DONE rows still fuse-unverified it
    runs the byte verification (stamping verified, or demoting SSD-backed
    ghosts to MOVING); for adopted rows with unknown kind it classifies;
    for blobless rows it re-exports bytes; for orphaned in-flight rows it
    runs fix_orphan. Bounded concurrency, per-row isolation (one bad row
    never kills the pass), fail-closed throughout. CancelledError
    propagates so shutdown stays prompt. Returns outcome counts.
    """
    counts = {"verified": 0, "demoted": 0, "classified": 0,
              "blob_healed": 0, "orphans_fixed": 0, "skipped": 0,
              "failed": 0}
    try:
        actual = await bounded(
            dest.list_torrents(category="racing"),
            timeout=30.0, label="verify list_torrents",
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile_verify: cannot list dest entries: %s", e)
        return counts
    actual_by_hash: dict[str, object] = {}
    for t in actual or []:
        try:
            h = (getattr(t, "hash", "") or "").strip().lower()
            if h:
                actual_by_hash[h] = t
        except Exception:
            continue
    try:
        all_rows = await offload(store.all)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile_verify: cannot read state DB: %s", e)
        return counts
    try:
        sem = asyncio.Semaphore(max(1, int(concurrency or 4)))
    except (TypeError, ValueError):
        sem = asyncio.Semaphore(4)

    async def _one(ts) -> None:
        try:
            async with sem:
                await _verify_row(cfg, dest, store, ts, actual_by_hash,
                                  counts)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            counts["failed"] += 1
            log.warning("reconcile_verify: row %s failed: %s",
                        (getattr(ts, "source_infohash", "") or "")[:10], e)

    await asyncio.gather(*[_one(ts) for ts in all_rows or []])
    log.info("reconcile_verify: %s", ", ".join(
        f"{k}={v}" for k, v in counts.items()))
    return counts


async def _verify_row(cfg, dest, store, ts, actual_by_hash, counts) -> None:
    """Verify one row in the background pass (never raises fatally)."""
    try:
        h = (ts.source_infohash or "").lower()
        if not h:
            counts["skipped"] += 1
            return
        known = {h}
        for k in (ts.dest_infohash, ts.cross_seed_infohash):
            if k:
                known.add(k.lower())
        if ts.injected_private_hashes:
            for iph in ts.injected_private_hashes.split(","):
                if iph.strip():
                    known.add(iph.strip().lower())
        present = any(k in actual_by_hash for k in known)
        hit = next((k for k in known if k in actual_by_hash), None)
        # Orphaned in-flight row: run the orphan fix.
        if not present and ts.state in (
                State.QUEUED, State.DOWNLOADING, State.MOVING,
                State.RE_ADDING):
            try:
                sftp_bytes = await offload(store.get_blob, h) or None
            except Exception:
                sftp_bytes = None
            try:
                await fix_orphan(ts, cfg, dest=dest, store=store,
                                 sftp_bytes=sftp_bytes)
                counts["orphans_fixed"] += 1
            except Exception as e:  # noqa: BLE001
                log.error("reconcile_verify: fix_orphan failed for %s: %s",
                          h[:10], e)
                counts["failed"] += 1
            return
        if not present:
            counts["skipped"] += 1
            return
        # DONE but fuse-unverified: run the byte verification now.
        if ts.state == State.DONE and not ts.fuse_verified:
            try:
                t = actual_by_hash.get(hit or h)
                sp = ""
                try:
                    sp = str(getattr(t, "save_path", "") or "")
                except Exception:
                    pass
                ok, use_path, kind, verified = await _verify_fuse_adopted(
                    cfg, dest, t, sp)
                if verified and ok:
                    if kind and kind != "unknown":
                        ts.classification_kind = kind
                    try:
                        blob = await _adopt_blob(dest, hit or h)
                        if blob:
                            try:
                                await offload(store.set_blob, h, blob)
                            except Exception:
                                pass
                            counts["blob_healed"] += 1
                    except Exception:
                        pass
                    try:
                        await offload(store.set_fuse_verified, h, True)
                    except Exception:
                        pass
                    ts.fuse_verified = 1
                    counts["verified"] += 1
                elif not ok:
                    # SSD-backed ghost: demote so the bytes really move.
                    ts.save_path = use_path or ts.save_path
                    try:
                        await offload(_force_state, store, ts, State.MOVING)
                    except Exception:
                        pass
                    counts["demoted"] += 1
                else:
                    counts["skipped"] += 1
            except Exception as e:  # noqa: BLE001
                log.warning("reconcile_verify: fuse verify failed for %s: %s",
                            h[:10], e)
                counts["failed"] += 1
            return
        # Adopted rows with unknown kind: classify + heal the blob.
        if (ts.classification_kind or "unknown") == "unknown" and ts.state in (
                State.DONE, State.MOVING, State.DOWNLOADING, State.RE_ADDING):
            try:
                kind = await _classify_adopted(cfg, dest, hit or h)
                if kind and kind != "unknown":
                    ts.classification_kind = kind
                    try:
                        store.upsert(ts)
                    except Exception:
                        pass
                    counts["classified"] += 1
                else:
                    counts["skipped"] += 1
            except Exception as e:  # noqa: BLE001
                log.warning("reconcile_verify: classify failed for %s: %s",
                            h[:10], e)
                counts["failed"] += 1
            try:
                if not ts.cross_seed_blob:
                    blob = await _adopt_blob(dest, hit or h)
                    if blob:
                        try:
                            await offload(store.set_blob, h, blob)
                        except Exception:
                            pass
                        counts["blob_healed"] += 1
            except Exception:
                pass
            return
        counts["skipped"] += 1
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile_verify: row %s failed: %s",
                    (getattr(ts, "source_infohash", "") or "")[:10], e)
        counts["failed"] += 1


async def _reconcile_snapshot(
    cfg: AppConfig,
    *,
    dest: TorrentClient,
    store: StateStore,
    verify: bool,
) -> RecoveryReport:
    """Snapshot + reconcile without (verify=False) per-row RPC verification.

    The optimistic form adopts entries as DONE (fuse-unverified),
    MOVING/DOWNLOADING/RE_ADDING with kind unknown and no blob, and
    leaves orphans and DONE ghost-checks for `reconcile_verify()`.
    Fail-closed throughout: unverified rows keep their VPS1 copies
    (janitor requires fuse_verified) and no state is trusted on RPC
    failure. With verify=True this is today's full inline behavior.
    """
    rpt = RecoveryReport()

    # 1. Snapshot reality — restrict to the racing category so we
    #    don't churn through 7000+ long-term seeds on every startup.
    # Retry transient WebUI hiccups so one timeout doesn't kill startup.
    # Each attempt is deadline-bounded: a dead dest fails this step in
    # seconds per attempt instead of wedging startup forever.
    actual: list = []
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            actual = await bounded(
                dest.list_torrents(category="racing"),
                timeout=30.0, label="reconcile list_torrents",
            )
            last_err = None
            break
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning("reconcile: list_torrents attempt %d/3 failed: %s", attempt, e)
            if attempt < 3:
                await asyncio.sleep(2 * attempt)
    if last_err is not None:
        # Fail-closed boot: an empty dest view would read every DONE row as
        # "lost" and mass-demote to RE_ADDING below. Aborting startup on a
        # wedged VPS2 is correct — steady-state loops tolerate the same
        # outage once running, but reconcile must never act on blindness.
        raise RuntimeError(f"reconcile: cannot list dest torrents after 3 attempts: {last_err}") from last_err
    actual_by_hash: dict[str, object] = {}
    for t in actual:
        h = (getattr(t, "hash", "") or "").strip().lower()
        if h:
            actual_by_hash[h] = t

    # 2. Snapshot DB (offloaded: a WAL-locked disk must not stall startup).
    try:
        all_rows = await offload(store.all)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"reconcile: cannot read state DB: {e}") from e

    for ts in all_rows:
        h = ts.source_infohash.lower()
        known_hashes = {
            k.lower()
            for k in (ts.source_infohash, ts.dest_infohash, ts.cross_seed_infohash)
            if k
        }
        # An injected private hash still seeding covers presence too —
        # otherwise DONE rows with vanished source/dest falsely re-add.
        if ts.injected_private_hashes:
            for iph in ts.injected_private_hashes.split(","):
                if iph.strip():
                    known_hashes.add(iph.strip().lower())
        present = any(k in actual_by_hash for k in known_hashes)
        if ts.state == State.DONE:
            if present:
                if verify and ts.classification_kind == "unknown":
                    # Pre-fix rows fast-tracked QUEUED->DONE without
                    # classification gate every later check at unsorted even
                    # when the pack lives at the default mount. Heal once,
                    # best-effort, from the live dest file list.
                    # (Snapshot-only mode defers this to reconcile_verify.)
                    hit = next((k for k in known_hashes if k in actual_by_hash), None)
                    if hit is not None:
                        try:
                            _files = await rpc(
                                dest.get_torrent_files(hit), cfg,
                                "reconcile heal get_torrent_files",
                            )
                            _kind = _classify_kind_for_files(_files or [], cfg)
                        except Exception:
                            _kind = "unknown"
                        if _kind and _kind != "unknown":
                            ts.classification_kind = _kind
                            try:
                                store.upsert(ts)
                            except Exception:
                                pass
                            log.info(
                                "reconcile: classified previously-unknown DONE %s as %s",
                                ts.source_name[:60], _kind,
                            )
                # Ghost check: a skip_check entry reports complete with zero
                # bytes. Verify cheaply at startup; a warming mount keeps
                # trust, SSD-resident bytes demote to MOVING for a real move.
                # (Snapshot-only mode defers this to reconcile_verify.)
                if verify:
                    try:
                        _hit = next((k for k in known_hashes if k in actual_by_hash), None)
                        if _hit is not None:
                            _t = actual_by_hash[_hit]
                            _sp = str(getattr(_t, "save_path", "") or "")
                            _fl = await rpc(
                                dest.get_torrent_files(_hit), cfg,
                                "reconcile ghost get_torrent_files",
                            )
                            _exp = [
                                (str(f.name), int(f.size_bytes or 0))
                                for f in (_fl or []) if getattr(f, "name", "")
                            ]
                            if _exp and _sp:
                                _miss = await _missing_under(Path(_sp), _exp)
                                if _miss:
                                    _root = find_content_on_ssd(cfg, _exp)
                                    if _root is not None:
                                        log.warning(
                                            "reconcile: DONE %s missing %d/%d files at %s; "
                                            "bytes on SSD at %s — demoting to MOVING",
                                            ts.source_name[:60], len(_miss), len(_exp),
                                            _sp, _root,
                                        )
                                        ts.save_path = str(_root)
                                        store.transition(ts, State.MOVING)
                                        rpt.resumed.append(h)
                                        continue
                                    log.warning(
                                        "reconcile: DONE %s missing %d/%d files at %s; "
                                        "keeping DONE (mount may be warming)",
                                        ts.source_name[:60], len(_miss), len(_exp), _sp,
                                    )
                    except Exception as e:  # noqa: BLE001
                        log.warning("reconcile: ghost check failed for %s: %s", h[:10], e)
                rpt.kept.append(h)
            else:
                # Lost — re-add pointing at fuse. The data is on remote.
                rpt.re_added.append(h)
                store.transition(ts, State.RE_ADDING)
        elif ts.state == State.FAILED:
            # Leave for manual retry
            rpt.kept.append(h)
        elif ts.state in (State.NEW, State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK):
            # Not yet added to VPS2 — safe to leave for coordinator to process
            rpt.resumed.append(h)
        else:
            # In-flight (QUEUED, DOWNLOADING, MOVING, RE_ADDING): check if torrent still exists
            if present:
                rpt.resumed.append(h)
            else:
                rpt.orphans.append(h)
                if not verify:
                    # Snapshot-only mode: the orphan stays in its in-flight
                    # state; reconcile_verify() runs fix_orphan in the
                    # background. Workers re-check presence before acting.
                    continue
                try:
                    sftp_bytes = await asyncio.to_thread(store.get_blob, ts.source_infohash) or None
                except Exception as e:
                    log.warning("reconcile: get_blob failed for %s: %s", h[:10], e)
                    sftp_bytes = None
                try:
                    await fix_orphan(ts, cfg, dest=dest, store=store, sftp_bytes=sftp_bytes)
                except Exception as e:
                    # One bad orphan must never kill startup — park it FAILED.
                    log.error("reconcile: fix_orphan failed for %s: %s", h[:10], e)
                    try:
                        _force_state(store, ts, State.FAILED,
                                     error=f"recovery failed: {e}")
                    except Exception:
                        pass

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

    # DONE rows re-added below must not lose their cross-seed blob — fetch it
    # lazily only when we actually adopt, to avoid pulling MB blobs per row.
    fuse_mounts = [
        str(fm).rstrip("/\\").replace("\\", "/")
        for fm in (cfg.rclone.fuse.mount, cfg.rclone.fuse.mount_unsorted)
        if fm
    ]
    for h, t in actual_by_hash.items():
        if h.lower() not in db_hashes:
            if _hash_is_ignored(store, h):
                log.info("reconcile: skipping cancelled release %s (%s)",
                         getattr(t, "name", h)[:60], h[:10])
                continue
            save_path = (getattr(t, "save_path", "") or "").rstrip("/\\").replace("\\", "/")
            on_fuse = any(save_path == fm or save_path.startswith(fm + "/") for fm in fuse_mounts if fm)
            # Placement gate: only adopt entries under known SSD/fuse roots.
            # With unresolvable roots (e.g. MagicMock cfg in unit tests)
            # membership is unknowable — preserve legacy adopt behavior.
            _roots_known = any(
                isinstance(raw, (str, Path)) and str(raw).strip()
                for raw in (getattr(cfg.dest, "save_path", None),
                            getattr(cfg.ssd, "path", None))
            )
            on_ssd = _save_path_on_ssd(cfg, save_path)
            if save_path and not on_fuse and not on_ssd and _roots_known:
                # Foreign placement (stale path, other mount): adopting it
                # would later move arbitrary directories to the remote.
                log.warning(
                    "reconcile: ignoring unplaced entry %s (%s) at %s; "
                    "not SSD nor fuse — left as unknown",
                    getattr(t, "name", h)[:60], h[:10], save_path,
                )
                rpt.unknowns.append(h)
                continue
            comp = getattr(t, "is_complete", False)
            is_done = comp() if callable(comp) else bool(comp)
            name = getattr(t, "name", h) or h
            try:
                size_bytes = int(float(getattr(t, "size_bytes", 0) or 0))
            except (TypeError, ValueError):
                size_bytes = 0
            if on_fuse or is_done:
                matches = store.find_by_name(name)
                if matches:
                    from .coordinator_content import normalize_content_name

                    norm = normalize_content_name(name)
                    same_release = [
                        m for m in matches
                        if normalize_content_name(m.source_name or "") == norm
                    ]
                    existing = same_release[0] if same_release else None
                    if existing is None:
                        log.warning(
                            "reconcile: %s (%s) name-matches %d row(s) but none "
                            "is the same release; adopting fresh instead of merging",
                            name[:60], h[:10], len(matches),
                        )
                    elif _hash_is_ignored(store, h):
                        log.info("reconcile: skipping link of cancelled %s (%s)",
                                 name[:60], h[:10])
                        continue
                    else:
                        curr = [x.strip() for x in existing.injected_private_hashes.split(",") if x.strip()]
                        curr_lower = {x.lower() for x in curr}
                        if h.lower() not in curr_lower and h.lower() != existing.source_infohash.lower():
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
                use_save_path = save_path
                kind = "unknown"
                fuse_verified = False
                if on_fuse and is_done:
                    # A skip_check entry reports complete with zero bytes —
                    # verify before trusting DONE (never-moved SSD data behind
                    # a fuse-pointing entry must go through MOVING instead).
                    # Unverifiable outcomes keep DONE (mount may be warming)
                    # but stay fuse-unverified so the janitor re-probes
                    # instead of clearing VPS1 on trust alone.
                    # Snapshot-only mode adopts optimistically (DONE,
                    # unverified, unknown kind/blob); reconcile_verify()
                    # verifies in the background.
                    if verify:
                        ok, use_save_path, kind, fuse_verified = await _verify_fuse_adopted(cfg, dest, t, save_path)
                        adopt_state = State.DONE if ok else State.MOVING
                elif on_fuse and not is_done:
                    # Fuse-pointing but incomplete: the client itself says
                    # bytes are missing. Never DONE (would strand an
                    # unseedable entry as terminal). Park in RE_ADDING for
                    # the gated fuse retry instead.
                    if verify:
                        kind = await _classify_adopted(cfg, dest, h)
                    adopt_state = State.RE_ADDING
                else:
                    # SSD-complete (or fuse-incomplete) adoption: classify now
                    # so _target_mount_for() routes movies/seasons to the
                    # default mount instead of defaulting unknown->unsorted.
                    # Best-effort; a failed file listing keeps "unknown".
                    # Snapshot-only mode defers classification to
                    # reconcile_verify (routing heals then).
                    if verify:
                        kind = await _classify_adopted(cfg, dest, h)
                log.info(
                    "reconcile: adopting existing completed/fuse torrent on VPS2 as %s: %s (%s) "
                    "save_path=%s on_fuse=%s complete=%s kind=%s",
                    adopt_state.value, name, h[:10],
                    use_save_path, on_fuse, is_done, kind,
                )
                ts = TorrentState(
                    source_infohash=h,
                    source_name=name,
                    dest_infohash=h,
                    save_path=use_save_path,
                    total_bytes=size_bytes,
                    classification_kind=kind,
                    state=adopt_state,
                    fuse_verified=1 if fuse_verified else 0,
                )
                if verify:
                    _adopted_blob = await _adopt_blob(dest, h)
                    if _adopted_blob:
                        ts.cross_seed_blob = _adopted_blob
                        ts._blob = _adopted_blob
                store.upsert(ts)
                rpt.kept.append(h)
                rpt.adopted.append(h)
            elif _save_path_on_ssd(cfg, save_path):
                # Partial SSD download with no DB row (e.g. --reset wiped the
                # batch cursors mid-download): resume it as DOWNLOADING instead
                # of abandoning it as unknown. Batches re-resolve from the live
                # file list and priorities restart at batch 0 downstream.
                # Snapshot-only mode defers classification/blob to
                # reconcile_verify.
                if verify:
                    kind = await _classify_adopted(cfg, dest, h)
                else:
                    kind = "unknown"
                log.info(
                    "reconcile: adopting partial SSD torrent on VPS2 as downloading: %s (%s) "
                    "save_path=%s complete=%s kind=%s",
                    name, h[:10], save_path, is_done, kind,
                )
                ts = TorrentState(
                    source_infohash=h,
                    source_name=name,
                    dest_infohash=h,
                    save_path=save_path,
                    total_bytes=size_bytes,
                    classification_kind=kind,
                    state=State.DOWNLOADING,
                )
                if verify:
                    _adopted_blob = await _adopt_blob(dest, h)
                    if _adopted_blob:
                        ts.cross_seed_blob = _adopted_blob
                        ts._blob = _adopted_blob
                store.upsert(ts)
                rpt.resumed.append(h)
                rpt.adopted.append(h)
            else:
                rpt.unknowns.append(h)

    log.info("recovery: %s", rpt.summary())
    if rpt.adopted:
        preview = ", ".join(h[:10] for h in rpt.adopted[:5])
        log.warning(
            "recovery adopted %d torrent(s) from VPS2 client state with no DB row "
            "(e.g. %s); --reset clears bookkeeping only — in-flight work resumes; "
            "use 'forget' to abandon a torrent entirely",
            len(rpt.adopted), preview,
        )
    return rpt


def _force_state(store: StateStore, ts: TorrentState, dst: State, *, error: str = "") -> None:
    """transition() when legal, else force-assign + upsert.

    Recovery must never crash on odd rows (e.g. a state the machine no
    longer allows from here); a non-empty error is preserved verbatim
    for operators, an empty one leaves last_error untouched.
    """
    try:
        store.transition(ts, dst, error=error)
    except AbandonedError:
        # Tombstoned mid-recovery (concurrent forget): the operator won —
        # leave the tombstone alone instead of resurrecting it.
        return
    except ValueError:
        ts.state = dst
        if error:
            ts.last_error = error
        store.upsert(ts)


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
            _force_state(store, ts, State.FAILED, error="orphan: no .torrent bytes")
            return State.FAILED.value
        # Add paused, then resolve the new hash, resume, and let coordinator drive.
        try:
            res = await dest.add_torrent(
                torrent_files=[sftp_bytes],
                save_path=ts.save_path or str(cfg.dest.save_path),
                category="racing",
                paused=True,
                skip_check=False,
            )
        except Exception as e:
            log.error("orphan %s: re-add failed: %s", h[:10] if len(h) > 10 else h, e)
            _force_state(store, ts, State.FAILED, error=f"orphan re-add failed: {e}")
            return State.FAILED.value
        new_hash = ""
        if res is not None and getattr(res, "hash", None):
            new_hash = str(res.hash).lower()
        if not new_hash:
            try:
                from .watchdir import _bencoded_info_hash
                new_hash, _, _, _ = _bencoded_info_hash(sftp_bytes)
                new_hash = (new_hash or "").lower()
            except Exception:
                new_hash = ""
        if new_hash:
            ts.dest_infohash = new_hash
        if not ts.save_path:
            ts.save_path = str(cfg.dest.save_path)
        try:
            await dest.resume(new_hash or h)
        except Exception as e:
            log.warning("orphan %s: resume after re-add failed: %s", h, e)
        if ts.state != State.DOWNLOADING:
            _force_state(store, ts, State.DOWNLOADING)
        else:
            store.upsert(ts)
        return State.DOWNLOADING.value

    if ts.state == State.RE_ADDING:
        log.info("orphan %s: already in RE_ADDING; will re-add to fuse", h)
        return State.RE_ADDING.value

    if ts.state == State.MOVING:
        src_path = Path(ts.save_path) if ts.save_path else Path(cfg.dest.save_path)
        # Torrent display name often differs from the on-disk top folder, so
        # prefer the exact file list decoded from the stored .torrent bytes.
        expected_names: list[str] | None = None
        if sftp_bytes:
            try:
                from .watchdir import extract_torrent_files_from_bencoded
                expected_names = [
                    f.name for f in extract_torrent_files_from_bencoded(sftp_bytes)
                ]
            except Exception:
                expected_names = None

        def _content_exists() -> bool:
            if expected_names:
                for name in expected_names:
                    target = _safe_join(src_path, name)
                    if target is not None and target.exists():
                        return True
                return False
            # Fall back to the display name; never let ".." escape src_path.
            safe_display = _safe_join(src_path, ts.source_name)
            if safe_display is not None and safe_display.exists():
                return True
            escaped_name = glob.escape(ts.source_name)
            try:
                return any(src_path.glob(f"{escaped_name}*"))
            except (OSError, ValueError):
                return False

        try:
            content_exists = await asyncio.to_thread(_content_exists)
        except Exception as e:
            log.warning("orphan %s: SSD check failed: %s", h, e)
            return State.MOVING.value
        if content_exists:
            log.info("orphan %s: files still exist on SSD; will resume move", h)
            return State.MOVING.value

        # Files no longer on SSD; rclone move completed, proceed to re-add to fuse
        log.info("orphan %s: files moved from SSD; will re-add to fuse", h)
        _force_state(store, ts, State.RE_ADDING)
        return State.RE_ADDING.value

    log.warning("orphan %s: cannot infer recovery path from state %s",
                h, ts.state.value)
    _force_state(store, ts, State.FAILED, error=f"orphan in state {ts.state.value}")
    return State.FAILED.value