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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="racing-sync")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Run the coordinator")
    p_run.add_argument("--config", type=Path, required=True)

    p_check = sub.add_parser("check-config", help="Validate config and exit")
    p_check.add_argument("--config", type=Path, required=True)

    args = parser.parse_args(argv)
    cfg = AppConfig.from_toml(args.config)

    if args.cmd == "check-config":
        print(f"OK: {args.config}")
        return 0

    setup_logging(cfg)
    log = logging.getLogger("racing_sync")
    log.info("starting racing-sync")

    coord = Coordinator(cfg)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    main_task = None
    
    def _signal_handler():
        nonlocal main_task
        log.info("Signal received, stopping...")
        coord.request_stop()
        if main_task and not main_task.done():
            main_task.cancel()
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        main_task = loop.create_task(coord.run())
        return loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        return 130
    finally:
        if main_task and not main_task.done():
            main_task.cancel()
        loop.run_until_complete(coord.shutdown())
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())