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


def _save_path_on_ssd(cfg: AppConfig, save_path: str) -> bool:
    """True iff a client entry's save_path lives under a configured SSD root.

    Gates DOWNLOADING adoption of partial torrents: entries pointing anywhere
    else (stale paths, other mounts) stay unknowns. Defensive against test
    doubles — unresolvable roots simply yield False.
    """
    sp = (save_path or "").rstrip("/\\").replace("\\", "/")
    if not sp:
        return False
    roots: list[str] = []
    for raw in (getattr(cfg.dest, "save_path", None), getattr(cfg.ssd, "path", None)):
        # Real configs always carry str/Path here; anything else (e.g. a
        # MagicMock in unit tests) means membership is unknowable -> False.
        if not isinstance(raw, (str, Path)):
            continue
        r = str(raw).rstrip("/\\").replace("\\", "/")
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
            if (root / top).exists():
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
        files = await dest.get_torrent_files(h)
    except Exception:
        return "unknown"
    try:
        return _classify_kind_for_files(files, cfg)
    except Exception:
        return "unknown"


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
) -> tuple[bool, str, str]:
    """Decide how to adopt an on-fuse + complete entry.

    Returns (adopt_as_done, effective_save_path, classification_kind). A
    client entry added with skip_check=True reports complete with zero bytes
    present, so bytes are verified before trusting DONE. Verification may
    only upgrade handling toward a verified-good path (SSD content found ->
    MOVING with corrected path); every other outcome preserves today's DONE
    adoption while logging loudly — late injections stay gated downstream
    regardless.
    """
    h = str(getattr(t, "hash", "") or "")
    name = str(getattr(t, "name", "") or h)
    try:
        files = await dest.get_torrent_files(h)
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile: cannot list files for %s; keeping DONE trust: %s",
                    h[:10], e)
        return True, save_path, "unknown"
    try:
        expected = [
            (str(f.name), int(f.size_bytes or 0))
            for f in (files or []) if getattr(f, "name", "")
        ]
    except Exception as e:  # noqa: BLE001
        log.warning("reconcile: cannot decode file list for %s; keeping DONE trust: %s",
                    h[:10], e)
        return True, save_path, "unknown"
    kind = _classify_kind_for_files(files, cfg)
    if not expected:
        return True, save_path, kind
    try:
        mount = Path(save_path)
    except Exception:
        return True, save_path, kind
    missing = await _missing_under(mount, expected)
    if missing is None:
        return True, save_path, kind
    if not missing:
        return True, save_path, kind
    ssd_root = find_content_on_ssd(cfg, expected)
    if ssd_root is not None:
        log.warning(
            "reconcile: %s claims fuse %s but %d/%d files missing; content found on SSD at %s — adopting as MOVING",
            name[:60], save_path, len(missing), len(expected), ssd_root,
        )
        return False, str(ssd_root), kind
    log.warning(
        "reconcile: %s claims fuse %s but %d/%d files missing (e.g. %s); keeping DONE (mount may be warming); late injections stay gated",
        name[:60], save_path, len(missing), len(expected), missing[0],
    )
    return True, save_path, kind


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
    actual_by_hash: dict[str, object] = {}
    for t in actual:
        h = (getattr(t, "hash", "") or "").strip().lower()
        if h:
            actual_by_hash[h] = t

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
                if ts.classification_kind == "unknown":
                    # Pre-fix rows fast-tracked QUEUED->DONE without
                    # classification gate every later check at unsorted even
                    # when the pack lives at the default mount. Heal once,
                    # best-effort, from the live dest file list.
                    hit = next((k for k in known_hashes if k in actual_by_hash), None)
                    if hit is not None:
                        try:
                            _files = await dest.get_torrent_files(hit)
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
                        store.transition(ts, State.FAILED, error=f"recovery failed: {e}")
                    except Exception:
                        try:
                            ts.state = State.FAILED
                            ts.last_error = f"recovery failed: {e}"
                            store.upsert(ts)
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
            save_path = (getattr(t, "save_path", "") or "").rstrip("/\\").replace("\\", "/")
            on_fuse = any(save_path == fm or save_path.startswith(fm + "/") for fm in fuse_mounts if fm)
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
                    existing = matches[0]
                    curr = [x for x in existing.injected_private_hashes.split(",") if x]
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
                if on_fuse and is_done:
                    # A skip_check entry reports complete with zero bytes —
                    # verify before trusting DONE (never-moved SSD data behind
                    # a fuse-pointing entry must go through MOVING instead).
                    ok, use_save_path, kind = await _verify_fuse_adopted(cfg, dest, t, save_path)
                    adopt_state = State.DONE if ok else State.MOVING
                elif on_fuse and not is_done:
                    # Fuse-pointing but incomplete: the client itself says
                    # bytes are missing. Never DONE (would strand an
                    # unseedable entry as terminal). Park in RE_ADDING for
                    # the gated fuse retry instead.
                    kind = await _classify_adopted(cfg, dest, h)
                    adopt_state = State.RE_ADDING
                else:
                    # SSD-complete (or fuse-incomplete) adoption: classify now
                    # so _target_mount_for() routes movies/seasons to the
                    # default mount instead of defaulting unknown->unsorted.
                    # Best-effort; a failed file listing keeps "unknown".
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
                )
                store.upsert(ts)
                rpt.kept.append(h)
                rpt.adopted.append(h)
            elif _save_path_on_ssd(cfg, save_path):
                # Partial SSD download with no DB row (e.g. --reset wiped the
                # batch cursors mid-download): resume it as DOWNLOADING instead
                # of abandoning it as unknown. Batches re-resolve from the live
                # file list and priorities restart at batch 0 downstream.
                kind = await _classify_adopted(cfg, dest, h)
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
            try:
                store.transition(ts, State.FAILED, error="orphan: no .torrent bytes")
            except ValueError:
                ts.state = State.FAILED
                ts.last_error = "orphan: no .torrent bytes"
                store.upsert(ts)
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
            try:
                store.transition(ts, State.FAILED, error=f"orphan re-add failed: {e}")
            except ValueError:
                ts.state = State.FAILED
                ts.last_error = f"orphan re-add failed: {e}"
                store.upsert(ts)
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
            try:
                store.transition(ts, State.DOWNLOADING)
            except ValueError:
                ts.state = State.DOWNLOADING
                store.upsert(ts)
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
        try:
            store.transition(ts, State.RE_ADDING)
        except ValueError:
            ts.state = State.RE_ADDING
            store.upsert(ts)
        return State.RE_ADDING.value

    log.warning("orphan %s: cannot infer recovery path from state %s",
                h, ts.state.value)
    try:
        store.transition(ts, State.FAILED, error=f"orphan in state {ts.state.value}")
    except ValueError:
        ts.state = State.FAILED
        ts.last_error = f"orphan in state {ts.state.value}"
        store.upsert(ts)
    return State.FAILED.value