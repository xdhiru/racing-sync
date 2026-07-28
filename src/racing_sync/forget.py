"""Abandon a torrent entirely.

Recovery re-adopts anything on the VPS2 client into fresh DB rows, so there
is no way to make the pipeline *stop* wanting a torrent by clearing state.
`forget` is that off-switch: it drops the DB row, deletes the matching dest
client entries, and (unless --keep-files) wipes the torrent's local SSD
data. Fuse/remote copies are never touched — abandoning local tracking of a
fused seed only stops reseeding it from VPS2.

The CLI defaults to a dry-run plan; pass --apply to execute. The API
endpoint always applies (it sits behind the same operator auth as retry).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from .rclone_ops import validate_safe_delete_path, wipe_local_tree

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

    Raises LookupError when nothing matches or the name matches several rows.
    """
    norm = (target or "").strip()
    if not norm:
        raise LookupError("forget target must not be empty")
    rows = store.all()
    low = norm.lower()
    for ts in rows:
        if low in _row_hashes(ts):
            return ts
    matches = [ts for ts in rows if low in (ts.source_name or "").lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        preview = ", ".join(f"{t.source_name} ({t.source_infohash[:10]})" for t in matches[:5])
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
    for f in files:
        norm = (getattr(f, "name", "") or "").replace("\\", "/").strip("/")
        if not norm or norm in tops:
            continue
        tops.append(norm.split("/")[0] if "/" in norm else norm)
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


async def forget_torrent(
    cfg,
    *,
    dest,
    store,
    target: str,
    apply: bool,
    delete_files: bool = True,
) -> dict:
    """Plan (apply=False) or execute (apply=True) abandoning one torrent.

    Returns a result dict with the row identity, planned/removed dest
    entries and local paths, skipped paths, and per-step errors. Lookup
    failures raise LookupError; operational errors are collected, never
    raised mid-way (a half-finished forget must be visible, not silent).
    """
    row = resolve_row(store, target)
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
    result: dict = {
        "applied": apply,
        "delete_files": delete_files,
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
    for h in sorted(entries):
        try:
            await dest.delete(h, delete_files=delete_files)
            log.info("forget: deleted dest entry %s (delete_files=%s)", h[:10], delete_files)
        except Exception as e:  # noqa: BLE001
            result["errors"].append(f"dest entry {h[:10]}: {e}")
    if delete_files:
        bases = _ssd_bases(cfg)
        for p in local_paths:
            try:
                await _remove_path(p, bases)
                log.info("forget: removed local path %s", p)
            except Exception as e:  # noqa: BLE001
                result["errors"].append(f"local path {p}: {e}")
    try:
        store.delete(row.source_infohash)
    except Exception as e:  # noqa: BLE001
        result["errors"].append(f"db row: {e}")
    return result
