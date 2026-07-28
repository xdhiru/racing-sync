"""CLI entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
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
        return int(res or 0)
    except asyncio.CancelledError:
        return 130
    finally:
        if not main_task.done():
            main_task.cancel()
            try:
                await main_task
            except (asyncio.CancelledError, Exception):
                pass
        await coord.shutdown()


def _do_reset(cfg: AppConfig) -> list[str]:
    """Fresh start: delete state.db (+WAL/SHM) and clear the log directory.

    Only touches the exact paths from the loaded config. Returns human-readable
    lines describing what was removed (also printed to stdout by the caller).
    Never raises on missing files — a fresh start on a clean machine is fine.
    """
    removed: list[str] = []
    try:
        db = Path(cfg.general.state_db)
    except Exception:
        db = None
    if db is not None:
        for candidate in (db, db.with_suffix(db.suffix + "-wal"), db.with_suffix(db.suffix + "-shm")):
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
        if log_dir.is_dir():
            for child in sorted(log_dir.iterdir()):
                try:
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child)
                    elif child.is_file() or child.is_symlink():
                        child.unlink()
                    else:
                        continue
                    removed.append(f"deleted log entry: {child}")
                except OSError as e:
                    removed.append(f"could not delete {child}: {e}")
        else:
            removed.append(f"log dir does not exist, nothing to clear: {log_dir}")
    if not removed:
        removed.append("nothing to reset (no state.db or log entries found)")
    return removed


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
    print(f"  torrent: {result['source_name']} ({result['source_infohash'][:10]}) [{result['state']}]")
    for h in result["dest_entries"]:
        print(f"  dest entry: {h[:10]}")
    for p in result["local_paths"]:
        verb = "removed" if result["applied"] and result["delete_files"] else "planned"
        print(f"  local path ({verb}): {p}")
    for s in result["skipped_paths"]:
        print(f"  skipped: {s}")
    for e in result["errors"]:
        print(f"  error: {e}")
    return 1 if result["errors"] else 0


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
             "'forget' to abandon a torrent entirely.",
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
        print(f"OK: {args.config}")
        return 0

    if args.cmd == "forget":
        return _cmd_forget(cfg, args)

    if getattr(args, "reset", False):
        for line in _do_reset(cfg):
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
    if "known_hosts" in text or "host key" in text.lower():
        return (
            "Hint: the SSH host key is not trusted. Either pin it:\n"
            "  ssh-keyscan -p <ssh_port> <ssh_host>"
            " >> ~/.ssh/known_hosts\n"
            "  (or into [source.deluge_sftp].known_hosts_path), or set\n"
            "  [source.deluge_sftp].auto_add_host_key = true "
            "(weaker: vulnerable to first-connection MITM)."
        )
    if "Authentication" in text or "auth" in text.lower():
        return (
            "Hint: SSH auth failed. Check [source.deluge_sftp].ssh_user plus "
            "ssh_key_path/ssh_key_passphrase or ssh_password."
        )
    return ""


if __name__ == "__main__":
    sys.exit(main())