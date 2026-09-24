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

import os
import shutil
import sqlite3
import time
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


def daemon_lock_path(cfg: object) -> Path | None:
    """Lock-file path next to state.db (sibling `<name>.lock`)."""
    try:
        db = str(getattr(getattr(cfg, "general", cfg), "state_db", "") or "")
        if not db:
            return None
        return Path(db + ".lock")
    except Exception:
        return None


class DaemonLock:
    """Cross-process mutual exclusion for state.db (H26).

    OS-level file lock (fcntl/msvcrt), never a PID file: the OS releases
    it on process death, so stale locks are impossible by construction.
    One holder at a time across daemon and mutating CLI commands.

    Usage: daemon holds for its lifetime (refuses to start a second
    instance); `forget --apply` / `unignore` take it non-blocking and
    refuse with "stop the daemon first" instead of racing the tick.
    """

    def __init__(self, path: Path):
        self._path = path
        self._fh = None
        self._locked = False

    @property
    def held(self) -> bool:
        return self._locked

    def acquire(self, *, blocking: bool = False) -> bool:
        """Take the lock; non-blocking by default (CLI fail-fast).

        Returns True when held (idempotent). Never raises: any doubt
        returns False and the caller refuses the mutation.
        """
        if self._locked:
            return True
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fh = open(self._path, "a+b")
        except OSError:
            return False
        try:
            if not _lock_file_nonblocking(fh):
                try:
                    fh.close()
                except OSError:
                    pass
                return False
        except OSError:
            try:
                fh.close()
            except OSError:
                pass
            return False
        self._fh = fh
        self._locked = True
        return True

    def release(self) -> None:
        fh, self._fh = self._fh, None
        self._locked = False
        if fh is None:
            return
        try:
            _unlock_file(fh)
        except OSError:
            pass
        try:
            fh.close()
        except OSError:
            pass

    def __enter__(self) -> "DaemonLock":
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def __del__(self) -> None:  # pragma: no cover (GC safety net)
        try:
            self.release()
        except Exception:
            pass


def _lock_file_nonblocking(fh) -> bool:
    """OS file lock without waiting; False when held elsewhere."""
    try:
        import fcntl  # noqa: PLC0415 (posix only)

        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (OSError, IOError):
            return False
    except ImportError:
        pass
    try:
        import msvcrt  # noqa: PLC0415 (windows only)

        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    except ImportError:
        pass
    # Last resort (no fcntl/msvcrt): exclusive-create pid file. Stale
    # entries are reaped via liveness probe where supported.
    try:
        probe = str(fh.name) + ".pid" if getattr(fh, "name", None) else None
        if probe is None:
            return False
        fd = os.open(probe, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, str(os.getpid()).encode())
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        return _pidfile_holder_alive(str(fh.name) + ".pid")
    except OSError:
        return False


def _pidfile_holder_alive(pid_path: str) -> bool:
    """True when a pid-file holder looks live (fail-closed True on doubt)."""
    try:
        with open(pid_path, "r", encoding="utf-8", errors="replace") as f:
            pid = int((f.read() or "").strip().split()[0])
    except Exception:
        return True
    if pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        try:
            os.unlink(pid_path)
        except OSError:
            pass
        return False
    except (PermissionError, OSError):
        return True


def _unlock_file(fh) -> None:
    try:
        import fcntl  # noqa: PLC0415

        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except (OSError, IOError):
            pass
        return
    except ImportError:
        pass
    try:
        import msvcrt  # noqa: PLC0415

        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
    except ImportError:
        pass


def backup_db(cfg: object, *, tag: str = "manual") -> str | None:
    """Timestamped consistent snapshot of state.db (VACUUM INTO).

    Returns the backup path, or None when there is nothing to back up
    (missing DB) or the snapshot failed (logged by the caller via the
    returned reason — callers treat None as "proceed without backup"
    only for read-only paths; destructive paths must refuse instead).
    Old backups for the same DB are pruned to the newest 7.
    """
    try:
        db = str(getattr(getattr(cfg, "general", cfg), "state_db", "") or "")
        if not db:
            return None
        src = Path(db)
        if not src.is_file():
            return None
        stamp = time.strftime("%Y%m%d-%H%M%S")
        safe_tag = "".join(c if (c.isalnum() or c in ("-", "_")) else "_"
                           for c in (tag or "manual"))[:24] or "manual"
        dest = src.parent / f"{src.name}.{stamp}.{safe_tag}.bak"
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(src), timeout=30.0)
            try:
                conn.execute(f"VACUUM INTO '{str(dest).replace(chr(39), chr(39)*2)}'")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            return None
        if not dest.is_file():
            return None
        # Prune: newest 7 backups for this DB survive.
        try:
            siblings = sorted(src.parent.glob(f"{src.name}.*.bak"),
                              key=lambda p: p.name)
            for old in siblings[:-7]:
                try:
                    old.unlink()
                except OSError:
                    pass
        except OSError:
            pass
        return str(dest)
    except Exception:
        return None


__all__ = [
    "validate_safe_delete_path",
    "is_safe_dir_to_clear",
    "state_db_parent_refusal",
    "fuse_roots",
    "overlaps_fuse",
    "clear_dir_children",
    "db_sidecar_paths",
    "daemon_lock_path",
    "DaemonLock",
    "backup_db",
]
