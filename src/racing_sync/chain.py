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
from urllib.parse import urlsplit

from .config import AppConfig
from .state import State


def provider_name(value: object, default: str) -> str:
    """Trust only real provider names; a partial config double keeps the default."""
    return value if isinstance(value, str) else default


def _same_client(source_host: object, dest_host: object) -> bool:
    """True when both WebUI origins address the same client instance."""

    def key(raw: object) -> tuple[str, int] | None:
        try:
            parts = urlsplit(str(raw).strip())
        except Exception:
            return None
        host = (parts.hostname or "").lower()
        if not host:
            return None
        port = parts.port or (443 if parts.scheme == "https" else 80)
        return host, int(port)

    left, right = key(source_host), key(dest_host)
    return bool(left is not None and left == right)


@dataclass(frozen=True)
class Chain:
    """One deployment's pipeline shape."""

    observe: tuple[str, ...]
    acquire: str
    stage: bool
    transfer: str
    seed: str
    cleanup: bool
    #: Source and dest point at the same client (one box).
    single_box: bool = False

    @classmethod
    def from_config(cls, cfg: AppConfig) -> Chain:
        pipeline = getattr(cfg, "pipeline", None)
        acquire = provider_name(getattr(pipeline, "acquire", None), "auto")
        transfer = provider_name(getattr(pipeline, "transfer", None), "rclone")
        seed = provider_name(getattr(pipeline, "seed", None), "path")
        watch_on = getattr(cfg, "watch_dir", None) is not None
        rclone = getattr(cfg, "rclone", None)
        # A partial config double without [rclone] keeps today's full chain.
        has_remote = True if rclone is None else getattr(rclone, "remote", None) is not None
        has_fuse = True if rclone is None else getattr(rclone, "fuse", None) is not None
        cleanup = getattr(getattr(cfg, "cleanup", None), "enabled", False)
        return cls(
            observe=("source", "dest", *(("watch",) if watch_on else ())),
            acquire=acquire,
            stage=acquire == "auto",
            transfer=transfer if has_remote or transfer != "rclone" else "none",
            seed=seed if has_fuse or seed != "path" else "none",
            cleanup=bool(cleanup),
            single_box=_same_client(
                getattr(cfg.source, "host", "") if hasattr(cfg, "source") else "",
                getattr(cfg.dest, "host", "") if hasattr(cfg, "dest") else "",
            ),
        )

    @property
    def downloads(self) -> bool:
        return self.acquire == "auto" and self.stage

    @property
    def waits_for_copy(self) -> bool:
        """The operator supplies the bytes; the daemon verifies and waits."""
        return self.acquire == "external"

    @property
    def moves(self) -> bool:
        return self.transfer in ("rclone", "local_move")

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
