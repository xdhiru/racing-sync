"""CLI entrypoint."""

from __future__ import annotations

import argparse
import asyncio
import logging
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="racing-sync")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run the coordinator")
    p_run.add_argument("--config", type=Path, required=True)

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


if __name__ == "__main__":
    sys.exit(main())