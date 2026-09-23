"""CLI entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shutil
import signal
import sys
from pathlib import Path

from .config import AppConfig
from .coordinator import Coordinator
from .logging_setup import setup_logging


async def _runner(coord: Coordinator) -> int:
    log = logging.getLogger("racing_sync")
    loop = asyncio.get_running_loop()
    main_task = asyncio.create_task(coord.run())

    def _signal_handler(*_args: object) -> None:
        log.info("Signal received, stopping...")
        coord.request_stop()
        if not main_task.done():
            loop.call_soon_threadsafe(main_task.cancel)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, _signal_handler)
            except (ValueError, OSError):
                pass

    try:
        res = await main_task
        # Coordinator.run() returns None on success; be explicit so a
        # truthy non-int (e.g. True) never becomes exit 1.
        if res is None or res is False or res is True:
            return 0
        try:
            return int(res)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return 0
    except asyncio.CancelledError:
        return 130
    finally:
        if not main_task.done():
            main_task.cancel()
            try:
                await main_task
            except (asyncio.CancelledError, Exception):
                pass
        # Bounded shutdown: a wedged close (dead mount, hung socket) must
        # delay SIGTERM, never block it forever. SQLite WAL recovers
        # natively, so abandoning a stuck shutdown is safe.
        try:
            await asyncio.wait_for(coord.shutdown(), timeout=30.0)
        except asyncio.TimeoutError:
            log.warning("shutdown timed out after 30s; exiting anyway")


def _is_safe_dir_to_clear(path: Path, label: str = "log dir") -> str | None:
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


def _state_db_parent_refusal(cfg: AppConfig) -> str | None:
    """Refuse destructive reset steps when state.db lives somewhere unsafe.

    A fat-fingered state_db (system path, symlink, filesystem root, repo
    checkout) plus --reset/--full must never delete outside racing-sync
    data. Narrower than _is_safe_dir_to_clear (which guards clearing
    directory CHILDREN): home/CWD parents are fine for unlinking three
    bookkeeping files, but system dirs, roots and checkouts are not.
    """
    try:
        db = Path(str(cfg.general.state_db))
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


def _clear_dir_children(root: Path, *, base_desc: str) -> list[str]:
    """Delete every child of `root` (never `root` itself). Returns log lines."""
    from .rclone_ops import validate_safe_delete_path

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


def _db_sidecar_paths(db: Path) -> list[Path]:
    """Return state.db plus its WAL/SHM sidecars without with_suffix crashes.

    `Path.with_suffix("-wal")` raises on suffix-less names (e.g. "state"),
    so build sidecars by string suffix instead.
    """
    return [db, Path(str(db) + "-wal"), Path(str(db) + "-shm")]


def _fuse_roots(cfg: AppConfig) -> list[Path]:
    """Configured rclone fuse mounts (never touched by any reset/wipe)."""
    roots: list[Path] = []
    try:
        for raw in (getattr(cfg.rclone.fuse, "mount", None),
                    getattr(cfg.rclone.fuse, "mount_unsorted", None)):
            if isinstance(raw, (str, Path)) and str(raw).strip():
                roots.append(Path(str(raw)))
    except Exception:
        return []
    return roots


def _overlaps_fuse(path: Path, fuse_roots: list[Path]) -> bool:
    """True when `path` is, contains, or sits inside a fuse mount."""
    try:
        resolved = path.resolve()
    except OSError:
        return False
    for f in fuse_roots or []:
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


def _do_reset(cfg: AppConfig) -> list[str]:
    """Fresh start: delete state.db (+WAL/SHM) and clear the log directory.

    Only touches the exact paths from the loaded config. Returns human-readable
    lines describing what was removed (also printed to stdout by the caller).
    Never raises on missing files — a fresh start on a clean machine is fine.
    """
    removed: list[str] = []
    db_refusal = _state_db_parent_refusal(cfg)
    try:
        db = Path(cfg.general.state_db)
    except Exception:
        db = None
    if db is not None and db_refusal is not None:
        removed.append(f"{db_refusal}; state.db kept")
        db = None
    if db is not None:
        for candidate in _db_sidecar_paths(db):
            try:
                if candidate.is_file():
                    candidate.unlink()
                    removed.append(f"deleted file: {candidate}")
            except OSError as e:
                removed.append(f"could not delete {candidate}: {e}")
    try:
        log_dir = Path(cfg.general.log_dir)
    except Exception:
        log_dir = None
    if log_dir is not None:
        refusal = _is_safe_dir_to_clear(log_dir, "log dir")
        if refusal is None and _overlaps_fuse(log_dir, _fuse_roots(cfg)):
            # log_dir on/under a fuse mount would delete remote data.
            refusal = f"refusing to clear log dir (overlaps fuse mount): {log_dir}"
        if refusal is not None:
            removed.append(refusal)
        elif log_dir.is_dir():
            # Reuse the validated clearer (per-child validate_safe_delete_path)
            # instead of a raw iterdir+rmtree loop.
            removed.extend(_clear_dir_children(log_dir, base_desc="log"))
        else:
            removed.append(f"log dir does not exist, nothing to clear: {log_dir}")
    if not removed:
        removed.append("nothing to reset (no state.db or log entries found)")
    return removed


async def _do_full_reset(cfg: AppConfig) -> list[str]:
    """True clean slate for testing: dest racing entries + SSD + blob cache.

    Runs after `_do_reset` (state.db + logs already gone) and before the
    coordinator starts, so nothing re-adopts mid-wipe. Removes every
    `racing`-category entry on the dest client *with files*, wipes the
    cached .torrent blob dir, and drops the ignore list with the DB.
    Fuse/remote copies are never touched. Best-effort per step — failures
    are reported, never raised (leftovers are re-adopted and resume).
    """
    from .clients.qbittorrent import QBittorrentClient
    from .rclone_ops import validate_safe_delete_path

    done: list[str] = []
    dest = QBittorrentClient(cfg.dest, label="dest-reset")
    try:
        await dest.start()
    except Exception as e:
        done.append(f"full reset: dest client unreachable ({e}); client entries kept")
        try:
            await dest.close()
        except Exception:
            pass
        dest = None  # type: ignore[assignment]
    if dest is not None:
        try:
            try:
                racing = await dest.list_torrents(category="racing") or []
            except Exception as e:
                racing = []
                done.append(f"full reset: cannot list dest entries ({e})")
            for t in racing:
                h = (getattr(t, "hash", "") or "").lower()
                if not h:
                    continue
                try:
                    await dest.delete(h, delete_files=True)
                    done.append(f"deleted dest entry with files: {h[:10]}")
                except Exception as e:  # noqa: BLE001
                    done.append(f"could not delete dest entry {h[:10]}: {e}")
        finally:
            try:
                await dest.close()
            except Exception:
                pass
    # Cached .torrent blobs (sibling of state.db): the whole directory.
    # Guarded by the state_db parent check (base_dir=parent alone is
    # worthless when parent is / or /etc).
    blob_refusal = _state_db_parent_refusal(cfg)
    try:
        blob_root = Path(cfg.general.state_db).parent / "watch_cross_seeds"
        if blob_refusal is not None:
            done.append(f"{blob_refusal}; blob cache kept")
        elif blob_root.is_dir() and not blob_root.is_symlink():
            validate_safe_delete_path(
                blob_root, base_dir=Path(cfg.general.state_db).parent)
            shutil.rmtree(blob_root)
            done.append(f"deleted blob cache: {blob_root}")
        else:
            done.append("blob cache absent, nothing to clear")
    except Exception as e:  # noqa: BLE001
        done.append(f"could not clear blob cache: {e}")
    # SSD data (children only, never the SSD root itself). Dest entries
    # were already deleted with files above; this catches orphans from
    # crashed downloads or non-racing categories left mid-testing.
    try:
        raw_roots = [getattr(cfg.ssd, "path", None),
                     getattr(cfg.dest, "save_path", None)]
        seen: set[str] = set()
        ssd_roots: list[Path] = []
        for raw in raw_roots:
            if not isinstance(raw, (str, Path)) or not str(raw).strip():
                continue
            try:
                p = Path(str(raw))
            except Exception:
                continue
            try:
                key = str(p.resolve())
            except OSError:
                key = str(p)
            if key in seen:
                continue
            seen.add(key)
            ssd_roots.append(p)
        # Fuse mounts are never touched: refuse an SSD root that *is* a
        # fuse mount (misconfiguration would delete remote data).
        fuse_roots = _fuse_roots(cfg)
        for root in ssd_roots:
            try:
                if root.is_symlink():
                    done.append(f"full reset: refusing to wipe SSD dir (is a symlink): {root}")
                    continue
                resolved = root.resolve()
            except OSError as e:
                done.append(f"full reset: cannot resolve SSD dir {root}: {e}")
                continue
            try:
                overlap = _overlaps_fuse(resolved, fuse_roots)
                if overlap:
                    done.append(f"full reset: refusing to wipe SSD dir (overlaps fuse mount): {root}")
                    continue
            except OSError:
                pass
            refusal = _is_safe_dir_to_clear(root, "SSD dir")
            if refusal is not None:
                done.append(f"full reset: {refusal}; SSD data kept")
                continue
            if not root.is_dir():
                done.append(f"SSD dir absent, nothing to clear: {root}")
                continue
            done.extend(_clear_dir_children(root, base_desc="SSD data"))
    except Exception as e:  # noqa: BLE001
        done.append(f"could not clear SSD data: {e}")
    if not done:
        done.append("nothing to fully reset")
    return done


def _cmd_forget(cfg: AppConfig, args: argparse.Namespace) -> int:
    """Run the forget off-switch (dry-run plan by default, --apply to delete)."""
    from .clients.qbittorrent import QBittorrentClient
    from .forget import forget_torrent
    from .state import StateStore

    async def _run() -> dict:
        store = StateStore(cfg.general.state_db)
        dest = QBittorrentClient(cfg.dest, label="dest-forget")
        try:
            await dest.start()
            return await forget_torrent(
                cfg, dest=dest, store=store,
                target=args.target, apply=args.apply,
                delete_files=not args.keep_files,
                ignore=bool(getattr(args, "ignore", False)),
            )
        finally:
            try:
                await dest.close()
            except Exception:
                pass
            try:
                store.close()
            except Exception:
                pass

    try:
        result = asyncio.run(_run())
    except LookupError as e:
        print(f"forget: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"forget failed: {e}", file=sys.stderr)
        return 1
    if not result["applied"]:
        print("dry-run plan (pass --apply to execute):")
    else:
        print("forget applied:")
    src_name = result.get("source_name") or "?"
    src_hash = result.get("source_infohash") or ""
    src_state = result.get("state") or "?"
    print(f"  torrent: {src_name} ({src_hash[:10]}) [{src_state}]")
    for h in result["dest_entries"]:
        print(f"  dest entry: {h[:10]}")
    for pair in result.get("paired_cancelled") or []:
        _ph = pair.get("source_infohash") or ""
        _pn = pair.get("source_name") or "?"
        _verb = "auto-cancelled" if result["applied"] else "would auto-cancel"
        print(f"  waiting pair ({_verb}): {_pn[:60]} ({_ph[:10]})")
        for e in pair.get("errors") or []:
            print(f"  error: {e}")
    for p in result["local_paths"]:
        verb = "removed" if result["applied"] and result["delete_files"] else "planned"
        print(f"  local path ({verb}): {p}")
    for s in result["skipped_paths"]:
        print(f"  skipped: {s}")
    for e in result["errors"]:
        print(f"  error: {e}")
    if result.get("ignored"):
        print("  ignored: will not be picked up again while listed")
    return 1 if result["errors"] else 0


def _cmd_unignore(cfg: AppConfig, args: argparse.Namespace) -> int:
    """List or remove cancelled-release ignore entries."""
    from .state import StateStore

    try:
        store = StateStore(cfg.general.state_db)
    except Exception as e:
        print(f"forget failed: {e}", file=sys.stderr)
        return 1
    try:
        if getattr(args, "list", False) or (not getattr(args, "target", None)
                                            and not getattr(args, "all", False)):
            rows = store.list_ignored()
            if not rows:
                print("ignore list is empty")
            for r in rows:
                print(f"  {(r['source_name'] or '?')[:60]} ({(r['source_infohash'] or '')[:10]})")
            return 0
        if getattr(args, "all", False):
            rows = store.list_ignored()
            if not rows:
                print("ignore list is empty")
                return 0
            n = 0
            for r in rows:
                h = r["source_infohash"] or ""
                if store.unignore_torrent(h):
                    n += 1
                store.clear_tombstone(h)
                print(f"  unignored: {(r['source_name'] or '?')[:60]} ({h[:10]})")
            print(f"unignored {n} release(s); re-drop their .torrent files to reprocess")
            return 0
        try:
            found = store.find_ignored(args.target)
        except LookupError as e:
            print(f"unignore: {e}", file=sys.stderr)
            return 1
        if store.unignore_torrent(found["source_infohash"]):
            print(f"unignored: {(found['source_name'] or '?')[:60]} ({found['source_infohash'][:10]})")
        else:
            print("unignore: entry already gone")
        if store.clear_tombstone(found["source_infohash"]):
            print("tombstone lifted: re-dropped files will reprocess immediately")
        return 0
    finally:
        try:
            store.close()
        except Exception:
            pass


def _check_config_env(cfg: AppConfig) -> list[str]:
    """Best-effort environment checks beyond schema validation.

    Schema validation already ran (from_toml). Here: filesystem reachability
    (log dir writable, state/db parents, SSD/fuse paths exist) and the rclone
    binary being present + executable. Returns problem strings (empty = OK).
    Never raises — every probe is guarded.
    """
    problems: list[str] = []

    def _need_dir(label: str, raw: object, *, must_exist: bool, writable: bool = False) -> None:
        try:
            p = Path(str(raw))
        except Exception:
            problems.append(f"{label}: not a path: {raw!r}")
            return
        try:
            target = p if must_exist else p.parent
            if must_exist and not p.exists():
                problems.append(f"{label} does not exist: {p}")
                return
            if writable:
                try:
                    target.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    problems.append(f"{label} not writable ({p}): {e}")
                    return
                if not os.access(str(target), os.W_OK):
                    problems.append(f"{label} not writable: {target}")
        except Exception as e:  # noqa: BLE001
            problems.append(f"{label} check failed ({raw!r}): {e}")

    try:
        _need_dir("general.log_dir", cfg.general.log_dir, must_exist=False, writable=True)
    except Exception:
        pass
    for label, val in (
        ("dest.save_path", getattr(cfg.dest, "save_path", "")),
        ("ssd.path", getattr(cfg.ssd, "path", "")),
    ):
        try:
            _need_dir(label, val, must_exist=True)
        except Exception:
            pass
    # state.db itself is created on first run — its parent must exist.
    try:
        _need_dir("general.state_db parent", Path(str(getattr(cfg.general, "state_db", ""))).parent,
                  must_exist=True)
    except Exception:
        pass
    if getattr(cfg, "watch_dir", None) is not None:
        try:
            _need_dir("watch_dir.path", cfg.watch_dir.path, must_exist=True)
        except Exception:
            pass
    # rclone binary: absolute + exists + executable (PATH hijack / typo guard).
    try:
        b = Path(str(cfg.rclone.binary))
        if not b.is_absolute():
            problems.append(f"rclone.binary must be absolute: {b}")
        elif not b.exists():
            problems.append(f"rclone.binary not found: {b}")
        elif not os.access(str(b), os.X_OK):
            problems.append(f"rclone.binary not executable: {b}")
    except Exception as e:  # noqa: BLE001
        problems.append(f"rclone.binary check failed: {e}")
    # Cross-field advisories: fatal overlap is rejected by the AppConfig
    # validator; these stay check-config warnings (existing setups keep
    # running, new ones get told).
    try:
        if cfg.rclone.remote.default == cfg.rclone.remote.unsorted:
            problems.append(
                "rclone.remote.default == rclone.remote.unsorted "
                f"({cfg.rclone.remote.default!r}): movies and unsorted "
                "content share one remote path — intentional?"
            )
    except Exception:
        pass
    for _label, _raw in (
        ("ssd.path", getattr(cfg.ssd, "path", "")),
        ("dest.save_path", getattr(cfg.dest, "save_path", "")),
        ("general.state_db", getattr(cfg.general, "state_db", "")),
    ):
        try:
            if str(_raw).strip() and not Path(str(_raw)).is_absolute():
                problems.append(
                    f"{_label} is relative ({_raw!r}): resolves against the "
                    "working directory — use an absolute path for a daemon"
                )
        except Exception:
            pass
    try:
        _cap = int(getattr(cfg.ssd, "max_inflight_bytes", 0) or 0)
        _total = shutil.disk_usage(str(cfg.ssd.path)).total
        if _cap > _total:
            problems.append(
                f"ssd.max_inflight_bytes ({_cap}) exceeds the SSD disk total "
                f"({_total}): grows can overcommit past ENOSPC — size the cap "
                "below the disk"
            )
    except Exception:
        pass
    return problems


def _check_config_warnings(cfg: AppConfig) -> list[str]:
    """Non-fatal check-config advisories (printed, exit still 0).

    Fuse mounts fail soft by design: the daemon parks fuse-gated work
    until the mount appears instead of refusing to start, so a down
    mount must not fail check-config — but the operator should know.
    """
    warnings: list[str] = []
    for label, val in (
        ("rclone.fuse.mount", getattr(cfg.rclone.fuse, "mount", "")),
        ("rclone.fuse.mount_unsorted", getattr(cfg.rclone.fuse, "mount_unsorted", "")),
    ):
        try:
            p = Path(str(val))
        except Exception:
            continue
        try:
            if not p.exists():
                warnings.append(
                    f"{label} does not exist ({p}): fuse-gated work parks "
                    "until the mount appears — start the mount first if rows "
                    "should flow immediately"
                )
        except Exception:
            pass
    return warnings


def main(argv: list[str] | None = None) -> int:
    prog = Path(sys.argv[0]).name if argv is None and sys.argv else "racing-sync"
    parser = argparse.ArgumentParser(prog=prog)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run the coordinator")
    p_run.add_argument("--config", type=Path, required=True)
    p_run.add_argument(
        "--reset",
        action="store_true",
        help="Fresh start: delete state.db (+WAL/SHM) and clear the log "
             "directory before starting. Bookkeeping only — torrents on the "
             "clients/SSD are re-adopted by recovery and resume; use "
             "'forget' to abandon a torrent entirely. Requires --yes.",
    )
    p_run.add_argument(
        "--full",
        action="store_true",
        help="With --reset: also drop dest racing entries (with files), wipe "
             "SSD data and the cached .torrent blobs — a true clean slate "
             "for testing. Implies --reset. Fuse/remote copies are untouched. "
             "Requires --yes.",
    )
    p_run.add_argument(
        "--yes",
        action="store_true",
        help="Confirm the destructive --reset / --full wipe (no prompt otherwise).",
    )

    p_forget = sub.add_parser(
        "forget",
        help="Abandon a torrent: drop its DB row, delete dest client "
             "entries and (by default) its local SSD data.",
    )
    p_forget.add_argument("--config", type=Path, required=True)
    p_forget.add_argument(
        "target",
        help="40-char infohash (any known hash) or a unique name substring.",
    )
    p_forget.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete. Without it, only prints the dry-run plan.",
    )
    p_forget.add_argument(
        "--keep-files",
        action="store_true",
        help="Remove client entries + DB row but keep local data files.",
    )
    p_forget.add_argument(
        "--ignore",
        action="store_true",
        help="With --apply: also record the release as cancelled so it is "
             "never picked up again while listed on VPS1. Use 'unignore' to "
             "lift it.",
    )

    p_unignore = sub.add_parser(
        "unignore",
        help="Lift a cancellation: list the ignore list (--list) or remove one entry.",
    )
    p_unignore.add_argument("--config", type=Path, required=True)
    p_unignore.add_argument(
        "target", nargs="?",
        help="40-char infohash or unique name substring. Omit with --list/--all.",
    )
    p_unignore.add_argument(
        "--list", action="store_true",
        help="List cancelled releases instead of removing one.",
    )
    p_unignore.add_argument(
        "--all", action="store_true",
        help="Unignore every entry and lift their forget tombstones, so "
             "re-dropped files reprocess immediately (single-target unignore "
             "lifts that entry's tombstone too).",
    )

    p_check = sub.add_parser("check-config", help="Validate config and exit")
    p_check.add_argument("--config", type=Path, required=True)

    args = parser.parse_args(argv)
    try:
        cfg = AppConfig.from_toml(args.config)
    except FileNotFoundError as e:
        print(f"Invalid config: file not found: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        # TOML decode errors + pydantic ValidationError — clean message, no traceback.
        print(f"Invalid config {args.config}: {e}", file=sys.stderr)
        return 2

    if args.cmd == "check-config":
        problems = _check_config_env(cfg)
        for w in _check_config_warnings(cfg):
            print(f"config warning: {w}")
        if problems:
            for p in problems:
                print(f"config problem: {p}", file=sys.stderr)
            return 2
        print(f"OK: {args.config}")
        return 0

    if args.cmd == "forget":
        return _cmd_forget(cfg, args)

    if args.cmd == "unignore":
        return _cmd_unignore(cfg, args)

    if getattr(args, "reset", False) or getattr(args, "full", False):
        if getattr(args, "full", False) and not getattr(args, "yes", False):
            print(
                "refusing --full without --yes: this drops dest racing "
                "entries (with files), wipes SSD data and the cached "
                ".torrent blobs. Re-run with 'run --full --yes' to confirm.",
                file=sys.stderr,
            )
            return 2
        if getattr(args, "reset", False) and not getattr(args, "yes", False):
            print(
                "refusing --reset without --yes: this deletes state.db "
                "(+WAL/SHM) and clears the log directory. Re-run with "
                "'run --reset --yes' to confirm.",
                file=sys.stderr,
            )
            return 2
        for line in _do_reset(cfg):
            print(line)
        if getattr(args, "full", False):
            for line in asyncio.run(_do_full_reset(cfg)):
                print(line)

    try:
        setup_logging(cfg)
    except Exception as e:
        print(f"Failed to initialise logging ({cfg.general.log_dir}): {e}", file=sys.stderr)
        return 2
    log = logging.getLogger("racing_sync")
    log.info("starting racing-sync")

    try:
        coord = Coordinator(cfg)
    except Exception as e:
        print(f"Failed to initialise coordinator: {e}", file=sys.stderr)
        return 2
    try:
        return asyncio.run(_runner(coord))
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        # Startup failures (SFTP/auth/probe) previously dumped a raw
        # traceback. Print a clean error plus the actionable hint instead.
        print(f"Failed to start: {e}", file=sys.stderr)
        hint = _startup_hint(e)
        if hint:
            print(hint, file=sys.stderr)
        return 2


def _startup_hint(exc: BaseException) -> str:
    """Actionable hint for common startup failures (SFTP/host-key)."""
    text = f"{type(exc).__name__}: {exc}"
    lowered = text.lower()
    if "known_hosts" in lowered or "host key" in lowered:
        return (
            "Hint: the SSH host key is not trusted. Either pin it:\n"
            "  ssh-keyscan -p <ssh_port> <ssh_host>"
            " >> ~/.ssh/known_hosts\n"
            "  (or into [source.deluge_sftp].known_hosts_path), or set\n"
            "  [source.deluge_sftp].auto_add_host_key = true "
            "(weaker: vulnerable to first-connection MITM)."
        )
    if "authentication" in lowered or "auth" in lowered:
        return (
            "Hint: SSH auth failed. Check [source.deluge_sftp].ssh_user plus "
            "ssh_key_path/ssh_key_passphrase or ssh_password."
        )
    return ""


if __name__ == "__main__":
    sys.exit(main())