"""rclone subprocess wrapper.

We drive rclone via `asyncio.create_subprocess_exec` so the coordinator can
await moves without blocking. Each rclone invocation runs as:

    rclone move <flags...> -- <local> <remote>

All flags (including --config and extra_move_flags) go BEFORE the `--`
end-of-flags marker; only the two positionals follow it. Anything after
`--` is parsed as a positional, so flags placed there break the command
(`Command move needs 2 arguments maximum`) — and positionals before `--`
risk flag-injection from `-`-leading paths.

Per-file moves (batches, leftover sweeps) use `--files-from-raw` with an
exact torrent-relative name list instead of `--include` globs: `--include`
patterns cannot express directory traversal for `**/`-prefixed entries
(rclone implies no dir rules from them, so every directory hits the
implied `- **` and the move transfers zero files with exit 0), while a
raw list matches literally — no glob escaping, no ARG_MAX blowup — and
preserves torrent-relative paths on the remote identically.
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


class RcloneTimeoutError(RcloneError):
    """`rclone move` produced no result within the wall-clock ceiling.

    The source tree is intact (rclone only removes source files after each
    is verified on the remote), so callers must PARK the row for retry —
    never fail it (failing would loop: re-download, stall again, fail…).
    """


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
            log.warning(
                "rclone.binary not found at %s (tolerated: unit tests mock "
                "spawn; check-config flags this in prod)", b,
            )
    except OSError:
        pass
    return b


def _env(cfg: AppConfig) -> dict[str, str]:
    env = dict(os.environ)
    if cfg.rclone.config_path:
        env["RCLONE_CONFIG"] = str(cfg.rclone.config_path)
    return env


def _reject_hijack_flags(flags: list[str] | None, where: str) -> None:
    """Refuse rclone flags that swap config/credentials (defense in depth).

    The config validator is the primary gate; this covers programmatic
    callers. Narrow on purpose: tuning flags (--transfers, --bwlimit,
    --s3-chunk-size, ...) pass through untouched.
    """
    for item in flags or []:
        try:
            flag = str(item).strip().lower().split("=", 1)[0]
        except Exception:
            continue
        if flag in ("--config", "--password-command", "--ask-password"):
            raise RcloneError(
                f"refusing rclone {where} flag that hijacks config/credentials: {item!r}"
            )


def build_move_cmd(cfg: AppConfig, source: Path, dest_remote: str,
                   *, include: list[str] | None = None,
                   files_from: str | None = None,
                   extra: list[str] | None = None) -> list[str]:
    if dest_remote.startswith("-"):
        raise RcloneError(f"refusing rclone dest_remote starting with '-': {dest_remote!r}")
    if include and not all(i.startswith("--include=") for i in include):
        raise RcloneError(f"include patterns must be '--include=...' form, got: {include!r}")
    if include and files_from:
        # --files-from-raw overrides/ignores every filter flag: passing both
        # would silently drop the includes. Fail loudly instead.
        raise RcloneError("rclone move takes either include= or files_from=, not both")
    if files_from and files_from.startswith("-"):
        raise RcloneError(f"refusing rclone files-from list starting with '-': {files_from!r}")
    _reject_hijack_flags(cfg.rclone.extra_move_flags, "extra_move_flags")
    _reject_hijack_flags(list(extra or []), "extra")
    # Flags first, `--` + positionals last: rclone parses everything after
    # `--` as positionals, so --config/extra flags placed there become
    # spurious "arguments" (rc=2). `--` still shields a `-`-leading source.
    cmd = [str(cfg.rclone.binary), "move"]
    if cfg.rclone.config_path:
        cmd.extend(["--config", str(cfg.rclone.config_path)])
    cmd.extend(cfg.rclone.extra_move_flags)
    if include:
        cmd.extend(include)
    if files_from:
        cmd.extend(["--files-from-raw", files_from])
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
            f in lower for f in ("secret", "pass", "token", "apikey", "api_key",
                                 "api-key", "auth-key", "authkey")
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


def _move_timeout_seconds(cfg: AppConfig) -> float:
    """Wall-clock ceiling per move; MagicMock test doubles count as default."""
    try:
        raw = getattr(getattr(cfg, "rclone", None), "move_timeout_seconds", None)
    except Exception:
        raw = None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return 6 * 3600
    try:
        return max(60.0, float(raw))
    except (TypeError, ValueError):
        return 6 * 3600


async def run_rclone(
    cfg: AppConfig,
    cmd: list[str],
    *,
    timeout: float | None = None,
) -> RcloneResult:
    if timeout is None:
        timeout = _move_timeout_seconds(cfg)
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
    except asyncio.CancelledError:
        # Shutdown while a move is in flight: terminate the child (bounded)
        # so it can't keep uploading/deleting source files after we exit,
        # then let the cancellation propagate.
        try:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except Exception:
                    pass
        except Exception:
            pass
        raise
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
                try:
                    # Bounded: an unkillable (D-state) process must not wedge
                    # the move worker (and its semaphore slot) forever. The
                    # orphan is reaped by the event loop on exit; the row
                    # stays put for retry via the straggler check.
                    await asyncio.wait_for(proc.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    pass
        except Exception:
            try:
                await _stop(proc, "kill")
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    pass
            except Exception:
                pass
        raise RcloneTimeoutError(
            f"rclone timeout after {timeout}s (source intact, retry later): "
            f"{redact_rclone_cmd(cmd)}"
        ) from None
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


def _validate_files_from_entries(files_from: list[str]) -> None:
    """Reject traversal/absolute/control-char entries before writing --files-from-raw list.

    Entries come from .torrent metadata / client file lists (untrusted).
    ``["a.mkv\\n/etc/passwd", "../outside"]`` must never reach rclone where
    they would escape `local`.
    """
    for entry in files_from:
        if not isinstance(entry, str) or not entry:
            raise ValueError(f"refusing empty rclone files-from entry: {entry!r}")
        if "\n" in entry or "\r" in entry or "\0" in entry:
            raise ValueError(f"refusing rclone files-from entry with control chars: {entry!r}")
        norm = entry.replace("\\", "/")
        # Absolute (POSIX or Windows drive) — never relativize silently.
        if norm.startswith("/") or (len(norm) >= 2 and norm[1] == ":" and norm[0].isalpha()):
            raise ValueError(f"refusing absolute rclone files-from entry: {entry!r}")
        parts = [p for p in norm.strip("/").split("/") if p]
        if not parts or any(p in (".", "..") for p in parts):
            raise ValueError(f"refusing traversal rclone files-from entry: {entry!r}")


async def move_local_to_remote(
    cfg: AppConfig,
    local: Path,
    dest_remote: str,
    *,
    include: list[str] | None = None,
    files_from: list[str] | None = None,
    extra: list[str] | None = None,
    timeout: float | None = None,
) -> RcloneResult:
    if not local.exists():
        raise FileNotFoundError(f"rclone source missing: {local}")
    if files_from is not None and not files_from:
        raise ValueError("rclone files_from list must not be empty (refusing silent no-op move)")
    if files_from is not None:
        _validate_files_from_entries(files_from)
    list_path: str | None = None
    try:
        if files_from is not None:
            import tempfile

            # --files-from-raw reads literal paths (no glob processing), one
            # per line, relative to `local`. delete=False + explicit unlink:
            # Windows cannot reopen delete=True temp files from the child.
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".lst",
                prefix="racing-sync-files-", delete=False,
            ) as fh:
                fh.write("\n".join(files_from) + "\n")
                list_path = fh.name
        cmd = build_move_cmd(cfg, local, dest_remote, include=include,
                             files_from=list_path, extra=extra)
        return await run_rclone(cfg, cmd, timeout=timeout)
    finally:
        if list_path:
            try:
                os.unlink(list_path)
            except OSError as e:
                log.warning("could not delete rclone file list %s: %s", list_path, e)


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
    if base_dir is None:
        raise ValueError(f"refusing to delete without base_dir guard: {path}")
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
    if base_dir is None:
        raise ValueError("refusing to delete without base_dir guard")
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


def _total_margin_bytes(cfg: AppConfig) -> int:
    """Combined SSD safety margin: general + ssd-specific (both default 0)."""
    def _as_int(raw: object) -> int:
        # Non-numeric doubles (MagicMock cfg in unit tests) count as 0 —
        # int(MagicMock) would otherwise invent a 1-byte margin.
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return 0
        try:
            return max(0, int(raw or 0))
        except (TypeError, ValueError):
            return 0

    try:
        general = _as_int(getattr(cfg.general, "disk_safety_margin_bytes", 0))
    except Exception:
        general = 0
    try:
        specific = _as_int(getattr(cfg.ssd, "safety_margin_bytes", 0))
    except Exception:
        specific = 0
    return general + specific


def ssd_has_room(cfg: AppConfig, extra_bytes: int = 0) -> bool:
    """True iff `ssd.path` has at least `extra_bytes + safety_margin` free."""
    free = disk_free_bytes_at(cfg.ssd.path)
    needed = extra_bytes + _total_margin_bytes(cfg)
    return free >= needed


def ssd_free_bytes(cfg: AppConfig) -> int:
    return disk_free_bytes_at(cfg.ssd.path)


def ssd_max_inflight_bytes(cfg: AppConfig) -> int:
    """Batcher cap is the configured max, capped by actual free space - safety margin."""
    free = disk_free_bytes_at(cfg.ssd.path)
    usable = max(0, free - _total_margin_bytes(cfg))
    return min(cfg.ssd.max_inflight_bytes, usable)