"""Direct launcher: run the app from source without pip-installing it.

Usage:
    python3 run.py run --config config.toml
    python3 run.py run --config config.toml --reset [--full]
    python3 run.py check-config --config config.toml
    python3 run.py forget --config config.toml <infohash|name> [--apply] [--keep-files] [--ignore]
    python3 run.py unignore --config config.toml [--list|<infohash|name>]
    python3 run.py --help
    python3 run.py run --help

--reset gives a fresh start: it deletes state.db (+WAL/SHM) and clears the
log directory from the loaded config, then starts normally. Bookkeeping
only — torrents on the clients/SSD are re-adopted by recovery and resume;
use 'forget' to abandon one entirely (dry-run by default, --apply deletes).
--reset --full additionally drops dest racing entries (with files), wipes
SSD data and the cached .torrent blobs: a true clean slate for testing
(fuse/remote copies are never touched).

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
