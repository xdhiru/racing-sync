"""Config UX: a starter file, feature toggles, explanations, chain report.

Everything here edits or reads the file — nothing writes into the state DB
(check-config opens it read-only, if at all). Edits are line-based so an
operator's comments and ordering survive; `enable` only appends, `disable`
flips the boolean and keeps every key.
"""

from __future__ import annotations

import re
import sqlite3
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from .chain import Chain
from .config import AppConfig
from .config_alias import normalize_config
from .config_disposition import FEATURE_SECTIONS
from .runtime import Runtime


@dataclass(frozen=True)
class Feature:
    section: str
    title: str
    summary: str
    block: str
    required: tuple[str, ...] = ()


FEATURES: dict[str, Feature] = {
    "indexers": Feature(
        section="indexers",
        title="Indexers (cross-seed + download-target search)",
        summary="Query Prowlarr for a copy when the racing client's own .torrent is not usable.",
        required=("base_url", "api_key", "[[indexers.download_indexers]]"),
        block="""[indexers]
enabled = true
base_url = "http://127.0.0.1:9696"
# api_key = "..."        (required: Prowlarr API key)
# [[indexers.download_indexers]]
# name = "My Indexer (API)"        (exact Prowlarr name)
# announce_substrings = ["my-indexer"]
""",
    ),
    "watch": Feature(
        section="watch",
        title="Watch directory (manual .torrent drops)",
        summary="Pick up .torrent files dropped into a directory and process them like racing releases.",
        required=("path",),
        block="""[watch]
enabled = true
# path = "/srv/qbittorrent/watch"
glob = "*.torrent"
""",
    ),
    "telegram": Feature(
        section="telegram",
        title="Telegram control plane",
        summary="Per-torrent status cards plus a pinned active-tasks list; commands act on rows.",
        required=("bot_token", "chat_id"),
        block="""[telegram]
enabled = true
# bot_token = "123456:ABC..."        (required)
# chat_id = "-100..."                (required)
""",
    ),
    "api": Feature(
        section="api",
        title="HTTP API (FastAPI)",
        summary="Readiness, SSD status and recover/scan/retry endpoints; needs a token or a trusted proxy.",
        required=("api_token",),
        block="""[api]
enabled = true
host = "127.0.0.1"
port = 8765
# api_token = "..."        (required unless trust_nginx_header = true)
""",
    ),
    "cleanup": Feature(
        section="cleanup",
        title="Source cleanup janitor",
        summary="Delete racing-client copies once the content is verified on the seed path.",
        block="""[cleanup]
enabled = true
dry_run = true
# Space out deletions: remove_torrent(+data) stalls a shared Deluge daemon.
delete_stagger_seconds = 60
""",
    ),
    "logging_sink": Feature(
        section="logging_sink",
        title="Centralised log sink",
        summary="POST structured log events to an HTTPS collector.",
        required=("url",),
        block="""[logging_sink]
enabled = true
# url = "https://logs.example.com/ingest"        (required)
""",
    ),
}


def _validate_feature_registry() -> None:
    """`FEATURES` must cover `FEATURE_SECTIONS` exactly, one feature each.

    The section names live in `config_disposition` (one source for the file
    surface); this module owns only the prose. A toggle added on one side
    without the other fails here — and in `tests/test_config_ux.py`.
    """
    registry = set(FEATURE_SECTIONS)
    assert len(registry) == len(FEATURE_SECTIONS), "duplicate section in FEATURE_SECTIONS"
    assert set(FEATURES) == registry, (
        f"FEATURES and FEATURE_SECTIONS disagree: "
        f"missing={sorted(registry - set(FEATURES))}, "
        f"extra={sorted(set(FEATURES) - registry)}"
    )
    for name, spec in FEATURES.items():
        assert spec.section == name, f"feature {name!r} points at section {spec.section!r}"


_validate_feature_registry()

STARTER = """\
# racing-sync — one box: race here, keep seeding from the seed path.
# Edit the three paths/hosts below, then:
#   racing-sync check-config --config config.toml
#   racing-sync run --config config.toml
# Every key is documented in config.example.toml; `config explain <section>`
# prints the short version.

[general]
state_db = "/var/lib/racing-sync/state.db"
log_dir = "/var/log/racing-sync"
source_poll_interval = 120
dest_poll_interval = 60
client_rpc_timeout_seconds = 30

[client]
type = "qbittorrent"
host = "http://127.0.0.1:8080"
username = "admin"
password = ""
category = ""

[dest]
# The same client on one box. Point this at a second client to split.
host = "http://127.0.0.1:8080"
username = "admin"
password = ""

[storage]
path = "/srv/qbittorrent/data"
save_path = "/srv/qbittorrent/data"
max_inflight_bytes = 42949672960
skip_movie_larger_than_bytes = 42949672960

[transfer]
provider = "rclone"        # rclone | local_move | none | external

[remote]
path = "remote:qbittorrent/"
unsorted = "remote:qbittorrent/unsorted/"

[seed]
provider = "path"          # path | existing | none
path = "/mnt/remote/qbittorrent"

# Optional: `racing-sync config enable <feature>` appends its documented block.
[indexers]
enabled = false

[watch]
enabled = false

[telegram]
enabled = false

[api]
enabled = false

[cleanup]
enabled = false

[logging_sink]
enabled = false
"""


class ConfigUxError(RuntimeError):
    pass


def _feature(name: str) -> Feature:
    try:
        return FEATURES[name]
    except KeyError:
        known = ", ".join(FEATURES)
        raise ConfigUxError(f"unknown feature {name!r} (choose one of: {known})") from None


def init_config(path: Path, *, force: bool = False) -> None:
    """Write the compact single-box starter; refuse to clobber unless forced."""
    target = Path(path)
    if target.exists() and not force:
        raise ConfigUxError(f"{target} already exists — pass --force to overwrite it")
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(STARTER)
    try:
        AppConfig.from_toml(target)
    except Exception as e:
        target.unlink(missing_ok=True)
        raise ConfigUxError(f"internal error: the starter does not load ({e})") from None


def _read(path: Path) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigUxError(f"{path} does not exist — run `config init` first") from None


def _write(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


def _section_span(lines: list[str], section: str) -> tuple[int, int] | None:
    header = f"[{section}]"
    for i, line in enumerate(lines):
        if line.strip() == header:
            end = len(lines)
            for j in range(i + 1, len(lines)):
                stripped = lines[j].strip()
                if stripped.startswith("[") and not stripped.startswith("#"):
                    end = j
                    break
            return i, end
    return None


def set_enabled(path: Path, feature: str, enabled: bool) -> tuple[bool, list[str]]:
    """Flip a feature's switch in place. Returns (changed, missing required keys)."""
    spec = _feature(feature)
    text = _read(path)
    lines = text.splitlines()
    span = _section_span(lines, spec.section)
    if span is None:
        if not enabled:
            return False, []
        body = text if text.endswith("\n") else text + "\n"
        _write(path, body + "\n" + spec.block.strip("\n") + "\n")
        return True, list(spec.required)
    start, end = span
    value = "true" if enabled else "false"
    present = False
    for i in range(start + 1, end):
        match = re.match(r"^(\s*)enabled\s*=", lines[i])
        if match:
            lines[i] = f"{match.group(1)}enabled = {value}"
            present = True
            break
    if not present:
        lines.insert(start + 1, f"enabled = {value}")
    _write(path, "\n".join(lines) + "\n")
    if not enabled:
        return True, []
    body = "\n".join(lines[start:end])
    missing = [key for key in spec.required if key not in body]
    return True, missing


def explain(section: str | None = None) -> str:
    if section is None:
        lines = ["Sections (config enable/disable <name>):"]
        width = max(len(name) for name in FEATURES)
        for name, spec in FEATURES.items():
            lines.append(f"  {name.ljust(width)}  {spec.summary}")
        lines.append("")
        lines.append("Also: client, dest, storage, transfer, remote, seed, general (core).")
        return "\n".join(lines)
    key = section.strip().lower()
    try:
        spec = FEATURES[key]
    except KeyError:
        known = ", ".join(FEATURES)
        raise ConfigUxError(f"unknown section {section!r} (choose one of: {known})") from None
    lines = [f"[{spec.section}] — {spec.title}", spec.summary, ""]
    if spec.required:
        lines.append("Required when enabled: " + ", ".join(spec.required))
    else:
        lines.append("No extra keys required.")
    lines.append("")
    lines.append(spec.block.strip("\n"))
    return "\n".join(lines)


def _state_counts(cfg: AppConfig) -> dict[str, int]:
    """Read-only row counts by state; {} when the DB is absent/unreadable."""
    try:
        raw = str(cfg.general.state_db)
    except Exception:
        return {}
    if not raw.strip():
        return {}
    path = Path(raw)
    try:
        if not path.exists():
            return {}
        uri = f"file:{quote(path.as_posix(), safe='/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=1.0)
        try:
            rows = conn.execute(
                "SELECT state, COUNT(*) FROM torrent_state GROUP BY state"
            ).fetchall()
        finally:
            conn.close()
        return {str(state): int(count) for state, count in rows}
    except Exception:
        return {}


def _disabled_features(cfg: AppConfig) -> list[str]:
    out: list[str] = []
    try:
        rt = Runtime(cfg)
    except Exception:
        return out
    checks = (
        ("indexers", lambda: rt.indexers.enabled),
        ("watch", lambda: rt.intake.watch is not None),
        ("telegram", lambda: cfg.telegram.enabled),
        ("api", lambda: cfg.api.enabled),
        ("cleanup", lambda: cfg.cleanup.enabled),
        ("logging_sink", lambda: cfg.logging_sink.enabled),
    )
    for name, enabled in checks:
        try:
            if not enabled():
                out.append(name)
        except Exception:
            continue
    return out


def check_report(cfg: AppConfig, config_path: Path | str) -> list[str]:
    """Chain, disabled links, retired keys and stranding warnings for check-config."""
    chain = Chain.from_config(cfg)
    rt = Runtime(cfg)
    lines = [
        "topology: " + ("single (one client)" if chain.single_box else "split (two clients)"),
        (
            f"chain: observe={','.join(chain.observe)} acquire={chain.acquire} "
            f"transfer={chain.transfer} seed={chain.seed} "
            f"cleanup={'on' if chain.cleanup else 'off'}"
        ),
        (
            "targets: transfer="
            + (rt.transfer.remote_default or "-")
            + "  seed="
            + (str(rt.seed.mount) if rt.seed.mount is not None else "-")
        ),
    ]
    disabled = _disabled_features(cfg)
    if disabled:
        lines.append("disabled: " + ", ".join(disabled))
    try:
        with open(config_path, "rb") as fh:
            normalized = normalize_config(tomllib.load(fh))
        for message in normalized.deprecations:
            lines.append(f"deprecated: {message}")
        for key in normalized.unknown:
            lines.append(f"unknown key: {key} — ignored (possible typo)")
    except Exception:
        pass
    counts = _state_counts(cfg)
    if not chain.moves and counts.get("moving"):
        lines.append(
            f"warning: {counts['moving']} row(s) are MOVING while transfer is off — "
            "the next start drains them (or fails a partially moved one)"
        )
    if not chain.adds_seed_entry and counts.get("re_adding"):
        lines.append(
            f"warning: {counts['re_adding']} row(s) are RE_ADDING while no seed entry "
            "is added — the next start marks them DONE"
        )
    return lines
