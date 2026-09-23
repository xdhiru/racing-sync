"""Central guards for every destructive path (reset, forget, wipes, janitor).

All deletion-adjacent code funnels through here so the next C3/C4-class
bug is removed by construction instead of re-implemented per call site:

- :func:`validate_safe_delete_path` — never delete roots, bases, or
  outside-base paths.
- :func:`is_safe_dir_to_clear` — refuse clearing system/checkout/CWD
  directories (reset/wipe entry point).
- :func:`state_db_parent_refusal` — refuse destructive steps when
  state.db itself lives somewhere unsafe (root, system dir, checkout).
- :func:`fuse_roots` / :func:`overlaps_fuse` — fuse mounts are never
  touched: wiping through a mount deletes remote data.
- :func:`clear_dir_children` — validated per-child directory clearer
  (never the root itself).

Pure stdlib: no imports from the package, so any module (including
``__main__`` and ``forget``) can use it without import cycles.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable


def validate_safe_delete_path(
    path: Path, base_dir: Path | Iterable[Path] | None = None
) -> None:
    """Raise ValueError when `path` must not be deleted.

    Refuses filesystem roots unconditionally; with `base_dir`, also
    refuses the bases themselves and anything outside them.
    """
    resolved = path.resolve()
    if resolved == Path(resolved.anchor) or str(resolved) in ("/", "\\"):
        raise ValueError(f"refusing to delete filesystem root: {path}")
    if base_dir is not None:
        bases = [base_dir] if isinstance(base_dir, Path) else list(base_dir)
        bases_resolved = [b.resolve() for b in bases]
        for br in bases_resolved:
            if resolved == br:
                raise ValueError(f"refusing to delete base directory: {path}")
        if not any(resolved.is_relative_to(br) for br in bases_resolved):
            raise ValueError(
                f"path {path} is not within allowed base directories: {bases}"
            )


def is_safe_dir_to_clear(path: Path, label: str = "log dir") -> str | None:
    """Return None when `path` is safe to clear children of, else a reason.

    `--reset` / `--full` delete every child of the configured dir. A
    misconfigured path (filesystem root, /var/log, home dir, symlink to
    elsewhere) would wipe data outside racing-sync. Fail closed.
    """
    log_dir = path
    try:
        # Never follow a symlink to an unexpected target.
        if log_dir.is_symlink():
            return f"refusing to clear {label} (is a symlink): {log_dir}"
        resolved = log_dir.resolve()
    except OSError as e:
        return f"refusing to clear {label} (cannot resolve {log_dir}): {e}"
    anchor = Path(resolved.anchor)
    if resolved == anchor or str(resolved) in ("/", "\\"):
        return f"refusing to clear filesystem root: {log_dir}"
    # Never wipe the current working directory (e.g. log_dir="." on a repo
    # checkout would delete every child of the project).
    try:
        if resolved == Path.cwd().resolve():
            return f"refusing to clear current working directory: {log_dir}"
    except OSError:
        pass
    # Never wipe a directory that looks like a source checkout: wiping it
    # would delete code, .git history, and the DB alongside logs.
    try:
        markers = ("pyproject.toml", ".git", "src", "run.py")
        if any((resolved / m).exists() for m in markers):
            return f"refusing to clear directory containing project files: {log_dir}"
    except OSError:
        pass
    # Well-known system/profile roots: clearing them would destroy data
    # far beyond racing-sync logs. Compare both Path and posix forms so
    # POSIX-style config values are caught on Windows test hosts too.
    denied = {
        Path("/var"), Path("/var/log"), Path("/etc"), Path("/usr"),
        Path("/bin"), Path("/sbin"), Path("/home"), Path("/root"),
        Path("/tmp"), Path("/var/tmp"),
    }
    denied_posix = {p.as_posix() for p in denied} | {
        "/var", "/var/log", "/etc", "/usr", "/bin", "/sbin",
        "/home", "/root", "/tmp", "/var/tmp", "/",
    }
    try:
        home = Path.home().resolve()
        denied.add(home)
        denied_posix.add(home.as_posix())
    except Exception:
        pass
    if resolved in denied or resolved.as_posix() in denied_posix:
        return f"refusing to clear system directory: {log_dir}"
    # Also match the raw configured value: on Windows Path("/var/log")
    # resolves to C:/var/log, hiding the POSIX system path.
    try:
        raw_posix = log_dir.as_posix()
    except Exception:
        raw_posix = str(log_dir)
    if raw_posix in denied_posix or raw_posix.rstrip("/") in denied_posix:
        return f"refusing to clear system directory: {log_dir}"
    # Shallow paths (e.g. /data, C:\\logs) are one typo away from a system
    # dir; require at least 3 parts (anchor + 2 levels) to clear.
    if len(resolved.parts) < 3 and resolved.parent in (anchor, resolved):
        # e.g. "/x" or "C:\\x" — allow only when it already looks like an
        # app dir? Fail closed: refuse bare top-level dirs.
        return f"refusing to clear top-level directory: {log_dir}"
    return None


def state_db_parent_refusal(cfg: object) -> str | None:
    """Refuse destructive reset steps when state.db lives somewhere unsafe.

    A fat-fingered state_db (system path, symlink, filesystem root, repo
    checkout) plus --reset/--full must never delete outside racing-sync
    data. Narrower than is_safe_dir_to_clear (which guards clearing
    directory CHILDREN): home/CWD parents are fine for unlinking three
    bookkeeping files, but system dirs, roots and checkouts are not.
    """
    try:
        db = Path(str(getattr(getattr(cfg, "general", cfg), "state_db", "")))
    except Exception:
        return "cannot resolve state_db path"
    try:
        if db.is_symlink():
            return f"refusing reset: state_db is a symlink: {db}"
    except OSError:
        pass
    try:
        resolved = db.resolve()
        parent = resolved.parent
        anchor = Path(resolved.anchor)
    except OSError as e:
        return f"refusing reset: cannot resolve state_db {db}: {e}"
    if parent == anchor or str(parent) in ("/", "\\"):
        return f"refusing reset: state_db at filesystem root: {db}"
    try:
        markers = ("pyproject.toml", ".git", "src", "run.py")
        if any((parent / m).exists() for m in markers):
            return f"refusing reset: state_db inside project checkout: {db}"
    except OSError:
        pass
    denied_posix = {
        "/var", "/var/log", "/etc", "/usr", "/bin", "/sbin",
        "/home", "/root", "/tmp", "/var/tmp", "/",
    }
    if parent.as_posix() in denied_posix or str(parent) in denied_posix:
        return f"refusing reset: state_db inside system directory: {db}"
    try:
        raw_posix = db.as_posix()
    except Exception:
        raw_posix = str(db)
    if raw_posix in denied_posix or raw_posix.rstrip("/") in denied_posix:
        return f"refusing reset: state_db inside system directory: {db}"
    return None


def fuse_roots(cfg: object) -> list[Path]:
    """Configured rclone fuse mounts (never touched by any reset/wipe)."""
    roots: list[Path] = []
    try:
        fuse = getattr(getattr(cfg, "rclone", None), "fuse", None)
        for raw in (getattr(fuse, "mount", None),
                    getattr(fuse, "mount_unsorted", None)):
            if isinstance(raw, (str, Path)) and str(raw).strip():
                roots.append(Path(str(raw)))
    except Exception:
        return []
    return roots


def overlaps_fuse(path: Path, fuse_mounts: list[Path]) -> bool:
    """True when `path` is, contains, or sits inside a fuse mount."""
    try:
        resolved = path.resolve()
    except OSError:
        return False
    for f in fuse_mounts or []:
        try:
            if not f.exists():
                continue
            fres = f.resolve()
        except OSError:
            continue
        # Exact, nested either way: dir inside fuse wipes remote,
        # fuse inside dir wipes the mount via the clear.
        if resolved == fres or resolved.is_relative_to(fres) or fres.is_relative_to(resolved):
            return True
    return False


def clear_dir_children(root: Path, *, base_desc: str) -> list[str]:
    """Delete every child of `root` (never `root` itself). Returns log lines."""
    lines: list[str] = []
    try:
        children = sorted(root.iterdir())
    except OSError as e:
        return [f"could not list {base_desc} {root}: {e}"]
    for child in children:
        try:
            validate_safe_delete_path(child, base_dir=root)
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            elif child.is_file() or child.is_symlink():
                child.unlink()
            else:
                try:
                    child.unlink()
                except OSError as e:
                    lines.append(f"could not delete {base_desc} entry {child}: {e}")
                    continue
            lines.append(f"deleted {base_desc} entry: {child}")
        except Exception as e:  # noqa: BLE001
            lines.append(f"could not delete {base_desc} entry {child}: {e}")
    if not lines:
        lines.append(f"{base_desc} already empty: {root}")
    return lines


def db_sidecar_paths(db: Path) -> list[Path]:
    """Return state.db plus its WAL/SHM sidecars without with_suffix crashes.

    `Path.with_suffix("-wal")` raises on suffix-less names (e.g. "state"),
    so build sidecars by string suffix instead.
    """
    return [db, Path(str(db) + "-wal"), Path(str(db) + "-shm")]


__all__ = [
    "validate_safe_delete_path",
    "is_safe_dir_to_clear",
    "state_db_parent_refusal",
    "fuse_roots",
    "overlaps_fuse",
    "clear_dir_children",
    "db_sidecar_paths",
]
