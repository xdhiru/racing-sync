"""Transfer seam: one provider object behind every move.

The move path builds a provider once per call and awaits ``provider.move``
instead of branching on the configured provider name. Both providers return
a result object carrying ``ok``, ``stderr``, ``returncode`` and ``duration``,
so callers keep one failure-handling path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from .config import AppConfig
from .local_transfer import LocalMoveError, LocalMoveTimeout, move_local_to_local
from .rclone_ops import RcloneTimeoutError, move_local_to_remote
from .runtime import Runtime


@runtime_checkable
class TransferProvider(Protocol):
    async def move(
        self,
        local: Path,
        target: str,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
    ) -> object: ...


class RcloneTransfer:
    """The default provider: ``rclone move`` with live progress."""

    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg

    async def move(
        self,
        local: Path,
        target: str,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
    ) -> object:
        return await move_local_to_remote(
            self._cfg,
            local,
            target,
            include=include,
            files_from=files_from,
            extra=extra,
            progress=True,
        )


class LocalTransfer:
    """Verified copy to a mounted local directory (``local_move``)."""

    def __init__(self, timeout: float | None) -> None:
        self._timeout = timeout

    async def move(
        self,
        local: Path,
        target: str,
        *,
        include: list[str] | None = None,
        files_from: list[str] | None = None,
        extra: list[str] | None = None,
    ) -> object:
        if include:
            raise LocalMoveError(
                "local_move does not support include patterns yet; "
                'use provider = "rclone" for filtered moves'
            )
        try:
            return await move_local_to_local(
                local,
                Path(target),
                files_from=files_from,
                timeout=self._timeout,
            )
        except LocalMoveTimeout as e:
            # Same park-for-retry contract as an rclone timeout: the source
            # tree is intact, so callers must not fail the row.
            raise RcloneTimeoutError(str(e)) from None


def build_transfer(rt: Runtime) -> TransferProvider:
    if rt.transfer.provider == "local_move":
        return LocalTransfer(rt.transfer.timeout_seconds)
    return RcloneTransfer(rt.config)
