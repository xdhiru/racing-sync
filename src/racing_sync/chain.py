"""The pipeline as a chain: which links exist and where stages hand off.

Today's deployment runs every link (observe → acquire → stage → transfer →
seed). Disabling transfer or seed shortens the chain; the engine then takes
a *derived* shortcut instead of walking a stage that does not exist:

- no transfer → a finished download is terminal (`DOWNLOADING` → `DONE`);
- no seed     → a finished move is terminal (`MOVING` → `DONE`).

`Chain` is the single place that answers "what comes next", so no call
site hard-codes `RE_ADDING`.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import AppConfig
from .state import State


@dataclass(frozen=True)
class Chain:
    """One deployment's pipeline shape."""

    observe: tuple[str, ...]
    acquire: str
    stage: bool
    transfer: str
    seed: str
    cleanup: bool

    @classmethod
    def from_config(cls, cfg: AppConfig) -> Chain:
        pipeline = getattr(cfg, "pipeline", None)
        acquire_on = bool(getattr(pipeline, "acquire", True))
        transfer_on = bool(getattr(pipeline, "transfer", True))
        seed_on = bool(getattr(pipeline, "seed", True))
        watch_on = getattr(cfg, "watch_dir", None) is not None
        rclone = getattr(cfg, "rclone", None)
        # A partial config double without [rclone] keeps today's full chain.
        has_remote = True if rclone is None else getattr(rclone, "remote", None) is not None
        has_fuse = True if rclone is None else getattr(rclone, "fuse", None) is not None
        cleanup = getattr(getattr(cfg, "cleanup", None), "enabled", False)
        return cls(
            observe=("source", "dest", *(("watch",) if watch_on else ())),
            acquire="auto" if acquire_on else "none",
            stage=acquire_on,
            transfer="rclone" if transfer_on and has_remote else "none",
            seed="path" if seed_on and has_fuse else "none",
            cleanup=bool(cleanup),
        )

    @property
    def downloads(self) -> bool:
        return self.acquire != "none" and self.stage

    @property
    def moves(self) -> bool:
        return self.transfer != "none"

    @property
    def seeds(self) -> bool:
        return self.seed != "none"

    def after_download(self) -> State:
        """Where a verified, completed SSD download goes."""
        return State.MOVING if self.moves else State.DONE

    def after_move(self) -> State:
        """Where a verified, completed transfer goes."""
        return State.RE_ADDING if self.seeds else State.DONE

    def shortcuts(self) -> frozenset[tuple[State, State]]:
        """Tolerated edges that exist only because a link is absent."""
        out = set()
        if not self.moves:
            out.add((State.DOWNLOADING, State.DONE))
        if not self.seeds:
            out.add((State.MOVING, State.DONE))
        return frozenset(out)

    def providers(self) -> dict[str, str]:
        return {
            "observe": ", ".join(self.observe),
            "acquire": self.acquire,
            "stage": "ssd" if self.stage else "none",
            "transfer": self.transfer,
            "seed": self.seed,
            "cleanup": "on" if self.cleanup else "off",
        }
