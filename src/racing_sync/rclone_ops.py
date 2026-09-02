"""rclone subprocess wrapper.

We drive rclone via `asyncio.create_subprocess_exec` so the coordinator can
await moves without blocking. Each rclone invocation runs as:

    rclone move <local> <remote> <extra_move_flags...>

Batch moves (per-episode) additionally carry `--include=...` patterns so
only the targeted episodes of the season folder are uploaded.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from .config import AppConfig

log = logging.getLogger(__name__)


@dataclass(slots=True)
class RcloneResult:
    returncode: int
    stdout: str
    stderr: str
    duration: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class RcloneError(RuntimeError):
    pass


def _env(cfg: AppConfig) -> dict[str, str]:
    env = dict(os.environ)
    if cfg.rclone.config_path:
        env["RCLONE_CONFIG"] = str(cfg.rclone.config_path)
    return env


def build_move_cmd(cfg: AppConfig, source: Path, dest_remote: str,
                   *, include: list[str] | None = None,
                   extra: list[str] | None = None) -> list[str]:
    cmd = [str(cfg.rclone.binary), "move", str(source), dest_remote]
    if cfg.rclone.config_path:
        cmd.extend(["--config", str(cfg.rclone.config_path)])
    cmd.extend(cfg.rclone.extra_move_flags)
    if include:
        cmd.extend(include)
    if extra:
        cmd.extend(extra)
    return cmd


async def run_rclone(
    cfg: AppConfig,
    cmd: list[str],
    *,
    timeout: float = 6 * 3600,
) -> RcloneResult:
    log.info("rclone: %s", " ".join(cmd))
    t0 = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_env(cfg),
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RcloneError(f"rclone timeout after {timeout}s: {cmd}")
    dt = time.monotonic() - t0
    stdout = stdout_b.decode("utf-8", errors="replace")
    stderr = stderr_b.decode("utf-8", errors="replace")
    res = RcloneResult(returncode=proc.returncode or 0,
                       stdout=stdout, stderr=stderr, duration=dt)
    if not res.ok:
        log.error("rclone failed (%d) in %.1fs:\n%s", res.returncode, dt, stderr[-2000:])
    else:
        log.info("rclone ok in %.1fs", dt)
    return res


async def move_local_to_remote(
    cfg: AppConfig,
    local: Path,
    dest_remote: str,
    *,
    include: list[str] | None = None,
    extra: list[str] | None = None,
) -> RcloneResult:
    if not local.exists():
        raise FileNotFoundError(f"rclone source missing: {local}")
    cmd = build_move_cmd(cfg, local, dest_remote, include=include, extra=extra)
    return await run_rclone(cfg, cmd)


async def wipe_local_tree(path: Path) -> None:
    """req #8: after each batch, remove the season folder before the next batch."""
    if not path.exists():
        return
    log.info("wiping local tree: %s", path)
    # shutil.rmtree is async-incompatible; run in default executor.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, shutil.rmtree, path, True)


async def wipe_local_files(paths: list[Path]) -> None:
    if not paths:
        return
    loop = asyncio.get_running_loop()

    def _rm(p: Path) -> None:
        try:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            elif p.exists():
                p.unlink()
        except OSError as e:
            log.warning("rm %s: %s", p, e)

    await asyncio.gather(*(loop.run_in_executor(None, _rm, p) for p in paths))





def disk_free_bytes_at(path: Path) -> int:
    return shutil.disk_usage(str(path)).free


def ssd_has_room(cfg: AppConfig, extra_bytes: int = 0) -> bool:
    """True iff `ssd.path` has at least `extra_bytes + safety_margin` free."""
    free = disk_free_bytes_at(cfg.ssd.path)
    needed = extra_bytes + cfg.general.disk_safety_margin_bytes
    return free >= needed


def ssd_free_bytes(cfg: AppConfig) -> int:
    return disk_free_bytes_at(cfg.ssd.path)


def ssd_max_inflight_bytes(cfg: AppConfig) -> int:
    """Batcher cap is the configured max, capped by actual free space - safety margin."""
    free = disk_free_bytes_at(cfg.ssd.path)
    usable = max(0, free - cfg.general.disk_safety_margin_bytes)
    return min(cfg.ssd.max_inflight_bytes, usable)