"""Abandon a torrent entirely.

Recovery re-adopts anything on the VPS2 client into fresh DB rows, so there
is no way to make the pipeline *stop* wanting a torrent by clearing state.
`forget` is that off-switch: it drops the DB row, deletes the matching dest
client entries, and (unless --keep-files) wipes the torrent's local SSD
data. Fuse/remote copies are never touched — abandoning local tracking of a
fused seed only stops reseeding it from VPS2.

The CLI defaults to a dry-run plan; pass --apply to execute. The API
endpoint always applies (it sits behind the same operator auth as retry).

Forgetting an SSD owner also forgets the watch-dir rows currently
deferred on it (same content, not yet started): they can never proceed
usefully without it, and hunting them down one by one is busywork. The
cascade is reported, never silent.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from .coordinator_content import watch_election_winner
from .rclone_ops import validate_safe_delete_path, wipe_local_tree
from .state import State

log = logging.getLogger(__name__)


def _row_hashes(ts) -> set[str]:
    out: set[str] = set()
    for k in (ts.source_infohash, ts.dest_infohash, ts.cross_seed_infohash):
        if k:
            out.add(k.lower())
    for part in (ts.injected_private_hashes or "").split(","):
        if part.strip():
            out.add(part.strip().lower())
    return out


def resolve_row(store, target: str):
    """Find the single DB row matching an infohash or unique name substring.

    Hash matching is exact-first: a full 40-char hash matching several
    rows (duplicate dest hashes from repacks) errors instead of deleting
    an arbitrary first row. Short hash fragments (<4 chars) never match —
    a 1-char fragment matches nearly every row. Name substrings keep the
    existing unique-or-error behavior.

    Raises LookupError when nothing matches or the target is ambiguous.
    """
    norm = (target or "").strip()
    if not norm:
        raise LookupError("forget target must not be empty")
    rows = store.all()
    low = norm.lower()
    hash_hits = []
    for ts in rows:
        hashes = _row_hashes(ts)
        if low in hashes:
            # Exact match always counts (even short: a row literally
            # hashed "e" would be bizarre, but exact is exact).
            hash_hits.append(ts)
        elif len(low) >= 4 and any(low in h for h in hashes):
            hash_hits.append(ts)
    if len(hash_hits) > 1:
        preview = ", ".join(
            f"{t.source_name} ({(t.source_infohash or '')[:10]})" for t in hash_hits[:5])
        raise LookupError(
            f"forget target {target!r} matches {len(hash_hits)} torrents by hash: "
            f"{preview}; use a 40-char infohash to pick one"
        )
    if hash_hits:
        return hash_hits[0]
    matches = [ts for ts in rows if low in (ts.source_name or "").lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        preview = ", ".join(f"{t.source_name} ({(t.source_infohash or '')[:10]})" for t in matches[:5])
        raise LookupError(
            f"forget target {target!r} matches {len(matches)} torrents: {preview}; "
            "use a 40-char infohash to pick one"
        )
    raise LookupError(f"no torrent matching {target!r}")


def _ssd_bases(cfg) -> list[Path]:
    bases: list[Path] = []
    for raw in (getattr(cfg.ssd, "path", None), getattr(cfg.dest, "save_path", None)):
        if isinstance(raw, Path) and raw not in bases:
            bases.append(raw)
    return bases


def _watch_cross_seeds_dir(cfg, source_infohash: str) -> Path | None:
    """Cached .torrent dir for one row (sibling of state.db), if configured."""
    from .coordinator_paths import _watch_cross_seed_dir

    try:
        db = getattr(cfg.general, "state_db", "")
    except Exception:
        return None
    if not str(db):
        return None
    return _watch_cross_seed_dir(db, source_infohash)


async def _candidate_local_paths(cfg, dest, row, entry_hashes: list[str]) -> tuple[list[Path], list[str]]:
    """Local SSD paths owned by this torrent (validated) + skipped reasons."""
    bases = _ssd_bases(cfg)
    if not bases:
        return [], ["no SSD base directories configured"]
    # Discover the on-disk layout from the client before deleting entries.
    files: list = []
    base_dir: Path | None = None
    for h in entry_hashes:
        try:
            found = await dest.get_torrent_files(h)
        except Exception as e:  # noqa: BLE001
            log.warning("forget: cannot list files for %s: %s", h[:10], e)
            continue
        if found:
            files = list(found)
            try:
                entry = await dest.get_torrent(h)
                sp = (getattr(entry, "save_path", "") or "") if entry else ""
            except Exception:  # noqa: BLE001
                sp = ""
            base_dir = Path(sp) if sp else None
            break
    if base_dir is None:
        sp = getattr(row, "save_path", "") or ""
        base_dir = Path(sp) if sp else None
    if base_dir is None:
        return [], ["torrent save path unknown; skipping local cleanup"]
    tops: list[str] = []
    seen_tops: set[str] = set()
    for f in files:
        raw = (getattr(f, "name", "") or "").replace("\\", "/").strip("/")
        if not raw or "\n" in raw or "\r" in raw or "\0" in raw:
            continue
        parts = [p for p in raw.split("/") if p]
        if not parts or any(p in (".", "..") for p in parts):
            continue
        top = parts[0]
        if top.startswith("/") or (len(top) >= 2 and top[1] == ":" and top[0].isalpha()):
            continue
        if not top or top in seen_tops:
            continue
        seen_tops.add(top)
        tops.append(top)
    if not tops:
        return [], ["no file list available; skipping local cleanup"]
    ok: list[Path] = []
    skipped: list[str] = []
    for top in tops:
        cand = base_dir / top
        try:
            validate_safe_delete_path(cand, base_dir=bases)
        except ValueError as e:
            skipped.append(f"{cand}: {e}")
            continue
        ok.append(cand)
    return ok, skipped


async def _remove_path(path: Path, bases: list[Path]) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or path.is_file():
        await asyncio.to_thread(path.unlink)
        return
    await wipe_local_tree(path, base_dir=bases)


def _paired_waiter_identities(store, cfg, target) -> list[dict]:
    """Watch rows currently deferred on `target` (same content, not started).

    Only rows whose election winner IS the target are paired: cancelling a
    waiter leaves its siblings alone, cancelling an owner takes its
    waiters. DONE/FAILED/gone owners release election, so their rows never
    match. Never raises (empty on any doubt).
    """
    try:
        rows = store.all()
    except Exception:
        return []
    target_hash = (target.source_infohash or "").lower()
    if not target_hash:
        return []
    out: list[dict] = []
    for cand in rows or []:
        try:
            ch = (cand.source_infohash or "").lower()
            if not ch or ch == target_hash:
                continue
            if cand.state not in (State.NEW, State.WAITING_DISK):
                continue
            winner = watch_election_winner(rows, cand, cfg)
            if winner is not None and (winner.source_infohash or "").lower() == target_hash:
                out.append({"source_infohash": cand.source_infohash,
                            "source_name": cand.source_name})
        except Exception:
            continue
    return sorted(out, key=lambda d: d["source_infohash"])


def _fuse_mounts(cfg) -> list[str]:
    """Normalized fuse mount prefixes (posix, no trailing slash)."""
    out: list[str] = []
    try:
        fuse = getattr(getattr(cfg, "rclone", None), "fuse", None)
        for raw in (getattr(fuse, "mount", None), getattr(fuse, "mount_unsorted", None)):
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            try:
                norm = str(raw).rstrip("/\\").replace("\\", "/")
            except Exception:
                continue
            if norm and norm not in out:
                out.append(norm)
    except Exception:
        pass
    return out


def _is_fuse_save_path(cfg, save_path: str) -> bool:
    sp = (save_path or "").rstrip("/\\").replace("\\", "/")
    if not sp:
        return False
    # Resolve symlinks/.. on both sides (fail-safe direction): an
    # SSD-looking path that resolves under a fuse mount must still count
    # as fuse (delete_files=False), or a symlinked save_path bypasses the
    # protection and deletes remote data.
    candidates = {sp}
    try:
        candidates.add(str(Path(sp).resolve()).replace("\\", "/"))
    except OSError:
        pass
    for fm in _fuse_mounts(cfg):
        if not fm:
            continue
        fm_cands = {fm}
        try:
            fm_cands.add(str(Path(fm).resolve()).replace("\\", "/"))
        except OSError:
            pass
        for cand in candidates:
            for base in fm_cands:
                if cand == base or cand.startswith(base + "/"):
                    return True
    return False


async def _forget_one(
    cfg,
    *,
    dest,
    store,
    row,
    apply: bool,
    delete_files: bool = True,
    ignore: bool = False,
) -> dict:
    """Forget a single already-resolved row (no cascade)."""
    known = sorted(_row_hashes(row))
    entries: dict[str, object] = {}
    if known:
        try:
            for t in await dest.list_torrents(hashes=known) or []:
                h = (getattr(t, "hash", "") or "").lower()
                if h:
                    entries[h] = t
        except Exception as e:  # noqa: BLE001
            log.warning("forget: cannot list dest entries for %s: %s",
                        row.source_infohash[:10], e)
    local_paths, skipped = await _candidate_local_paths(
        cfg, dest, row, [h for h in known if h in entries] or known[:1],
    )
    # Cached .torrent blobs for this row (sibling of state.db): planned in
    # dry-run like any other local path, removed on apply.
    blob_dir = _watch_cross_seeds_dir(cfg, row.source_infohash)
    if blob_dir is not None:
        try:
            if blob_dir.exists() or blob_dir.is_symlink():
                local_paths.append(blob_dir)
        except OSError as e:
            skipped.append(f"{blob_dir}: {e}")
    result: dict = {
        "applied": apply,
        "delete_files": delete_files,
        "ignored": bool(apply and ignore),
        "source_infohash": row.source_infohash,
        "source_name": row.source_name,
        "state": row.state.value,
        "dest_entries": sorted(entries),
        "local_paths": [str(p) for p in local_paths],
        "skipped_paths": list(skipped),
        "errors": [],
    }
    if not apply:
        return result
    still: set[str] = set()
    for h in sorted(entries):
        try:
            entry = entries[h]
            entry_save = (getattr(entry, "save_path", "") or "")
            # Fuse/remote copies are never touched: entries seeding from the
            # fuse mount are removed without files even when the caller asked
            # for delete_files=True (SSD entries still use the caller flag).
            effective_delete_files = (
                False if _is_fuse_save_path(cfg, entry_save) else delete_files
            )
            await dest.delete(h, delete_files=effective_delete_files)
            log.info("forget: deleted dest entry %s (delete_files=%s)", h[:10], effective_delete_files)
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"dest entry {h[:10]}: {e}")
    if sorted(entries):
        # Verify the deletes actually landed: a silently surviving entry
        # is re-adopted by recovery (or re-attached by a same-hash row)
        # and looks exactly like "cancel didn't remove it". Retry once,
        # then report instead of claiming success.
        try:
            remaining_entries = {
                (getattr(t, "hash", "") or "").lower(): t
                for t in await dest.list_torrents(hashes=sorted(entries)) or []
                if getattr(t, "hash", "")
            }
        except Exception as e:  # noqa: BLE001
            remaining_entries = {}
            log.warning("forget: cannot verify dest deletes for %s: %s",
                        row.source_infohash[:10], e)
        for h in sorted(remaining_entries):
            try:
                entry_save = (getattr(remaining_entries[h], "save_path", "") or "")
                if not entry_save and h in entries:
                    entry_save = (getattr(entries[h], "save_path", "") or "")
                effective_delete_files = (
                    False if _is_fuse_save_path(cfg, entry_save) else delete_files
                )
                await dest.delete(h, delete_files=effective_delete_files)
                log.info("forget: retry deleted dest entry %s", h[:10])
            except Exception as e:  # noqa: BLE001
                result["errors"].append(f"dest entry retry {h[:10]}: {e}")
        try:
            still = {
                (getattr(t, "hash", "") or "").lower()
                for t in await dest.list_torrents(hashes=sorted(remaining_entries)) or []
                if getattr(t, "hash", "")
            }
        except Exception:
            still = set()
        for h in sorted(still):
            result["errors"].append(
                f"dest entry {h[:10]} still present after delete")
        if still:
            log.warning("forget: %d dest entr%s still present for %s after delete",
                        len(still), "y" if len(still) == 1 else "ies",
                        row.source_infohash[:10])
    if delete_files:
        bases = _ssd_bases(cfg)
        try:
            state_parent = Path(getattr(cfg.general, "state_db", "")).parent
        except Exception:
            state_parent = None
        for p in local_paths:
            try:
                if blob_dir is not None and Path(p) == blob_dir:
                    # Blob cache: guarded by the state.db parent, not SSD bases.
                    if state_parent is None:
                        raise ValueError("state.db parent unknown; refusing blob cache delete")
                    validate_safe_delete_path(Path(p), base_dir=state_parent)
                    if Path(p).is_symlink():
                        await asyncio.to_thread(Path(p).unlink)
                    else:
                        await asyncio.to_thread(shutil.rmtree, str(p), True)
                else:
                    await _remove_path(p, bases)
                log.info("forget: removed local path %s", p)
            except Exception as e:  # noqa: BLE001
                result["errors"].append(f"local path {p}: {e}")
    if ignore:
        try:
            store.ignore_torrent(row.source_infohash, row.source_name)
            log.info("forget: ignoring %s going forward", row.source_infohash[:10])
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"ignore list: {e}")
            result["ignored"] = False
    if still:
        # Dest entries survived deletion: dropping the row would let
        # recovery re-adopt the survivor as a fresh row (resurrecting the
        # cancel). Keep the row so the operator can retry the forget.
        log.warning("forget: keeping row for %s (%d dest entries survive)",
                    row.source_infohash[:10], len(still))
        result["errors"].append(
            f"kept db row: {len(still)} dest entr"
            f"{'y' if len(still) == 1 else 'ies'} still present")
        return result
    try:
        # Tombstone, not hard-delete: in-flight workers, retries and
        # re-discovery refuse tombstoned hashes instead of resurrecting
        # them; the janitor hard-deletes after the TTL.
        store.tombstone(row.source_infohash)
    except Exception as e:  # noqa: BLE001
        result["errors"].append(f"db row: {e}")
    return result


async def forget_torrent(
    cfg,
    *,
    dest,
    store,
    target: str,
    apply: bool,
    delete_files: bool = True,
    ignore: bool = False,
) -> dict:
    """Plan (apply=False) or execute (apply=True) abandoning one torrent.

    Returns a result dict with the row identity, planned/removed dest
    entries and local paths, skipped paths, per-step errors, and
    `paired_cancelled` (same-content watch rows deferred on this one,
    planned or also forgotten with identical flags). Lookup failures raise
    LookupError; operational errors are collected, never raised mid-way
    (a half-finished forget must be visible, not silent).

    `ignore=True` additionally records the release on the ignore list so
    discovery/recovery/re-injection never pick it up again while it stays
    on the VPS1 racing client (only meaningful with apply=True).
    """
    row = resolve_row(store, target)
    paired = _paired_waiter_identities(store, cfg, row)
    result = await _forget_one(
        cfg, dest=dest, store=store, row=row,
        apply=apply, delete_files=delete_files, ignore=ignore,
    )
    result["paired_cancelled"] = []
    if not apply:
        result["paired_cancelled"] = [
            {"source_infohash": p["source_infohash"],
             "source_name": p["source_name"], "errors": []}
            for p in paired
        ]
        return result
    for p in paired:
        h = p["source_infohash"]
        try:
            prow = store.get(h)
        except Exception:
            prow = None
        if prow is None:
            continue
        try:
            pres = await _forget_one(
                cfg, dest=dest, store=store, row=prow,
                apply=True, delete_files=delete_files, ignore=ignore,
            )
        except LookupError:
            continue
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"paired {(h or '')[:10]}: {e}")
            continue
        log.info("forget: auto-cancelled waiting pair %s (%s) with %s",
                 (p["source_name"] or "?")[:60], (h or "")[:10],
                 (row.source_name or "?")[:60])
        result["paired_cancelled"].append({
            "source_infohash": h,
            "source_name": p["source_name"],
            "errors": list(pres.get("errors") or []),
        })
        for e in pres.get("errors") or []:
            result["errors"].append(f"paired {(h or '')[:10]}: {e}")
    return result
