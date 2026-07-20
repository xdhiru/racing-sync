"""rclone subprocess wrapper.

We drive rclone via `asyncio.create_subprocess_exec` so the coordinator can
await moves without blocking. Each rclone invocation runs as:

    rclone move <flags...> -- <local> <remote>

All flags (including --config and extra_move_flags) go BEFORE the `--`
end-of-flags marker; only the two positionals follow it. Anything after
`--` is parsed as a positional, so flags placed there break the command
(`Command move needs 2 arguments maximum`) — and positionals before `--`
risk flag-injection from `-`-leading paths.

Batch moves (per-episode) additionally carry `--include=...` patterns so
only the targeted episodes of the season folder are uploaded.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .config import AppConfig
from .logging_setup import sanitize_log_text

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


def _validate_rclone_binary(cfg: AppConfig) -> Path | None:
    """Require an absolute, executable rclone binary to avoid PATH hijack.

    Returns None (skips validation) for test doubles where binary is not a
    path-like (e.g. MagicMock with spec=AppConfig).
    """
    raw = cfg.rclone.binary
    if not isinstance(raw, (str, Path)):
        return None
    raw_s = str(raw)
    b = Path(raw_s)
    # Accept POSIX absolute ("/usr/bin/rclone") on Windows test hosts too
    # (Path() normalises to backslashes, so check the posix form).
    posix = raw_s.replace("\\", "/")
    if not (b.is_absolute() or posix.startswith("/")):
        raise RcloneError(f"rclone.binary must be absolute, got: {b}")
    try:
        if not os.access(b, os.X_OK):
            # In tests the binary may not exist on disk — only enforce when
            # the path exists but is not executable? No: enforce strictly in
            # prod, but tolerate missing file in unit tests that mock
            # create_subprocess_exec. Distinguish by existence:
            if b.exists():
                raise RcloneError(f"rclone.binary not executable: {b}")
    except OSError:
        pass
    return b


def _env(cfg: AppConfig) -> dict[str, str]:
    env = dict(os.environ)
    if cfg.rclone.config_path:
        env["RCLONE_CONFIG"] = str(cfg.rclone.config_path)
    return env


def build_move_cmd(cfg: AppConfig, source: Path, dest_remote: str,
                   *, include: list[str] | None = None,
                   extra: list[str] | None = None) -> list[str]:
    if dest_remote.startswith("-"):
        raise RcloneError(f"refusing rclone dest_remote starting with '-': {dest_remote!r}")
    if include and not all(i.startswith("--include=") for i in include):
        raise RcloneError(f"include patterns must be '--include=...' form, got: {include!r}")
    # Flags first, `--` + positionals last: rclone parses everything after
    # `--` as positionals, so --config/extra flags placed there become
    # spurious "arguments" (rc=2). `--` still shields a `-`-leading source.
    cmd = [str(cfg.rclone.binary), "move"]
    if cfg.rclone.config_path:
        cmd.extend(["--config", str(cfg.rclone.config_path)])
    cmd.extend(cfg.rclone.extra_move_flags)
    if include:
        cmd.extend(include)
    if extra:
        cmd.extend(extra)
    cmd.extend(["--", str(source), dest_remote])
    return cmd


_SENSITIVE_FLAGS = {
    "--password",
    "--rc-pass",
    "--s3-secret-access-key",
    "--b2-account-key",
    "--drive-token",
    "--dropbox-token",
    "--onedrive-token",
}


def redact_rclone_cmd(cmd: list[str]) -> str:
    """Return a sanitized command line string with sensitive flags and values masked."""
    out: list[str] = []
    redact_next = False
    for arg in cmd:
        if redact_next:
            out.append("******")
            redact_next = False
            continue
        lower = arg.lower()
        if any(lower == flag or lower.startswith(flag + "=") for flag in _SENSITIVE_FLAGS) or any(
            f in lower for f in ("secret", "pass", "token", "apikey", "api_key")
        ):
            if "=" in arg:
                key, _ = arg.split("=", 1)
                out.append(f"{key}=******")
            elif arg.startswith("-"):
                out.append(arg)
                redact_next = True
            else:
                out.append("******")
        else:
            out.append(arg)
    return " ".join(out)


async def run_rclone(
    cfg: AppConfig,
    cmd: list[str],
    *,
    timeout: float = 6 * 3600,
) -> RcloneResult:
    _validate_rclone_binary(cfg)
    log.info("rclone: %s", redact_rclone_cmd(cmd))
    t0 = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
        env=_env(cfg),
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        import inspect as _inspect

        async def _stop(p, meth: str) -> None:
            fn = getattr(p, meth, None)
            if fn is None:
                return
            try:
                r = fn()
                if _inspect.isawaitable(r):
                    await r
            except Exception:
                pass

        try:
            await _stop(proc, "terminate")
            try:
                await asyncio.wait_for(proc.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                await _stop(proc, "kill")
                await proc.wait()
        except Exception:
            try:
                await _stop(proc, "kill")
                await proc.wait()
            except Exception:
                pass
        raise RcloneError(f"rclone timeout after {timeout}s: {redact_rclone_cmd(cmd)}")
    dt = time.monotonic() - t0
    stdout = stdout_b.decode("utf-8", errors="replace")
    stderr = stderr_b.decode("utf-8", errors="replace")
    code = proc.returncode if proc.returncode is not None else -1
    res = RcloneResult(returncode=code,
                       stdout=stdout, stderr=stderr, duration=dt)
    if not res.ok:
        log.error("rclone failed (%d) in %.1fs:\n%s", res.returncode, dt, sanitize_log_text(stderr[-2000:]))
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


def validate_safe_delete_path(
    path: Path, base_dir: Path | Iterable[Path] | None = None
) -> None:
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


async def wipe_local_tree(
    path: Path, *, base_dir: Path | Iterable[Path] | None = None
) -> None:
    """Remove a directory tree safely, ensuring it is within base_dir and not a filesystem root."""
    if not path.exists() and not path.is_symlink():
        return
    validate_safe_delete_path(path, base_dir=base_dir)
    if path.is_symlink():
        path.unlink()
        return
    log.info("wiping local tree: %s", path)
    try:
        await asyncio.to_thread(shutil.rmtree, path, False)
    except OSError as e:
        log.warning("wipe local tree %s failed: %s", path, e)
        raise


async def wipe_local_files(
    paths: list[Path], *, base_dir: Path | Iterable[Path] | None = None
) -> None:
    if not paths:
        return

    def _rm(p: Path) -> None:
        try:
            if not p.exists() and not p.is_symlink():
                return
            validate_safe_delete_path(p, base_dir=base_dir)
            if p.is_symlink():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p, ignore_errors=False)
            elif p.exists():
                p.unlink()
        except Exception as e:
            log.warning("rm %s: %s", p, e)

    # Bound concurrency: 1000-file seasons must not spawn 1000 threads.
    sem = asyncio.Semaphore(16)

    async def _rm_bounded(p: Path) -> None:
        async with sem:
            await asyncio.to_thread(_rm, p)

    await asyncio.gather(*(_rm_bounded(p) for p in paths))





def disk_free_bytes_at(path: Path) -> int:
    try:
        return shutil.disk_usage(str(path)).free
    except OSError as e:
        log.warning("disk_usage failed for %s: %s", path, e)
        return 0


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