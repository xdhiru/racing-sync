"""Local transfer provider: move staged files to a mounted local directory.

Same safety bar as the rclone path: never delete a source until the
destination copy is complete and size-verified; refuse path escapes,
size conflicts and a destination inside the source; a missing destination
base (unmounted NAS) fails loudly instead of silently creating a
directory on the system disk.

Every step is idempotent: a re-run after a crash sees the destination
copy and only finishes the source delete.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path


class LocalMoveError(RuntimeError):
    pass


class LocalMoveTimeout(LocalMoveError):
    pass


@dataclass(frozen=True)
class LocalMoveResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration: float = 0.0

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def _safe_rel(name: object) -> str:
    text = str(name or "").replace("\\", "/").strip()
    parts = [part for part in text.split("/") if part not in ("", ".")]
    if not parts or text.startswith("/") or ".." in parts:
        raise LocalMoveError(f"refusing unsafe relative path: {name!r}")
    return "/".join(parts)


def _wanted_names(local: Path, files_from: list[str] | None) -> list[str]:
    if files_from is not None:
        if not files_from:
            raise LocalMoveError("files_from list must not be empty (refusing a silent no-op move)")
        return sorted({_safe_rel(name) for name in files_from})
    try:
        names = [p.relative_to(local).as_posix() for p in local.rglob("*") if p.is_file()]
    except OSError as e:
        raise LocalMoveError(f"could not list {local}: {e}") from None
    return sorted(set(names))


def _move_one(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(src, dst)
        return
    except OSError:
        pass
    # Different filesystems: copy beside the destination, verify, rename,
    # then delete the source. Never delete before the copy is complete.
    tmp = dst.with_name(f"{dst.name}.part-{os.getpid()}-{time.monotonic_ns()}")
    try:
        shutil.copy2(src, tmp)
        if tmp.stat().st_size != src.stat().st_size:
            raise LocalMoveError(f"copy size mismatch for {src} -> {dst}")
        os.replace(tmp, dst)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    src.unlink()


def _run(local: Path, dest_dir: Path, names: list[str]) -> int:
    try:
        local_real, dest_real = local.resolve(), dest_dir.resolve()
    except OSError:
        local_real, dest_real = local, dest_dir
    if dest_real == local_real or local_real in dest_real.parents:
        raise LocalMoveError(f"local_move destination {dest_dir} lies inside the source tree")
    if not dest_dir.exists():
        raise LocalMoveError(
            f"local_move destination does not exist: {dest_dir} (is the mount up?)"
        )
    if not dest_dir.is_dir():
        raise LocalMoveError(f"local_move destination is not a directory: {dest_dir}")
    moved = 0
    for rel in names:
        src = local / rel
        dst = dest_dir / rel
        src_file = src.is_file()
        dst_file = dst.is_file()
        if not src_file and not dst_file:
            raise LocalMoveError(
                f"source and destination both missing for {rel!r} — refusing to continue"
            )
        if src_file and dst_file:
            if src.stat().st_size != dst.stat().st_size:
                raise LocalMoveError(
                    f"destination exists with a different size for {rel!r} — refusing to overwrite"
                )
            src.unlink()  # duplicate left by an interrupted run
            continue
        if src_file:
            _move_one(src, dst)
            moved += 1
    missing = [rel for rel in names if not (dest_dir / rel).is_file()]
    if missing:
        raise LocalMoveError(
            f"{len(missing)} file(s) missing at the destination after the move "
            f"(e.g. {missing[0]!r})"
        )
    return moved


async def move_local_to_local(
    local: Path,
    dest_dir: Path,
    *,
    files_from: list[str] | None = None,
    timeout: float | None = None,  # noqa: ASYNC109 (wait_for enforces the deadline)
) -> LocalMoveResult:
    start = time.monotonic()
    if not local.exists():
        raise LocalMoveError(f"local_move source missing: {local}")
    names = _wanted_names(local, files_from)
    if not names:
        # Source already empty: only a populated destination proves the
        # bytes made it (mirrors the rclone provider's source-empty check).
        try:
            populated = dest_dir.is_dir() and any(p.is_file() for p in dest_dir.rglob("*"))
        except OSError:
            populated = False
        if populated:
            return LocalMoveResult(0, stdout="source empty; destination populated", duration=0.0)
        raise LocalMoveError(
            f"source {local} is empty and the destination holds nothing — "
            "refusing a silent no-op (data loss?)"
        )
    try:
        moved = await asyncio.wait_for(
            asyncio.to_thread(_run, local, dest_dir, names), timeout=timeout
        )
    except TimeoutError:
        raise LocalMoveTimeout(f"local move {local} -> {dest_dir} exceeded {timeout}s") from None
    return LocalMoveResult(
        returncode=0,
        stdout=f"{moved} file(s) moved",
        duration=time.monotonic() - start,
    )
