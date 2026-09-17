"""Direct launcher: run the app from source without pip-installing it.

Usage:
    python3 run.py run --config config.toml
    python3 run.py check-config --config config.toml

Ctrl+C stops gracefully. Third-party dependencies (aiohttp, pydantic, ...)
must exist in the active Python environment; only racing-sync itself runs
uninstalled, straight from ./src, so `git pull` + restart is the upgrade.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    src = Path(__file__).resolve().parent / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


_ensure_src_on_path()

from racing_sync.__main__ import main  # noqa: E402  (import after sys.path fix)


if __name__ == "__main__":
    raise SystemExit(main())
