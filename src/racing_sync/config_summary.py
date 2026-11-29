"""Read-only summary of the resolved configuration (refactor phase 0).

Pins today's configuration surface as a canonical, secret-free document:
the resolved shape (topology + which pipeline links are possible), how many
keys the file actually sets versus takes from defaults, and a digest of the
whole resolved model. Later refactor phases compare against this so a
config rewrite cannot silently change behaviour.

Never touches the state DB, the network or the filesystem beyond reading
the config file itself (via AppConfig.from_toml).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import tomllib
from pathlib import Path

from pydantic import BaseModel, SecretStr

from .config import AppConfig
from .config_disposition import (
    Key,
    extras_prefixes,
    flatten_leaves,
    model_leaves,
    strip_indices,
)

_SECRET_NAME_HINTS = (
    "password",
    "passphrase",
    "token",
    "secret",
    "api_key",
    "apikey",
    "auth_key",
)

_REDACTED = "***"


def _is_secret_name(name: object) -> bool:
    try:
        low = str(name).lower()
    except Exception:
        return False
    return any(h in low for h in _SECRET_NAME_HINTS)


def _redact(value: object, *, key: object = None) -> object:
    """JSON-safe, secret-free, platform-normalised copy of `value`."""
    if isinstance(value, SecretStr):
        return _REDACTED
    if isinstance(value, BaseModel):
        return {k: _redact(v, key=k) for k, v in value.model_dump(mode="python").items()}
    if isinstance(value, Path):
        return str(value).replace("\\", "/")
    if isinstance(value, dict):
        return {str(k): _redact(v, key=k) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_redact(v, key=key) for v in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, bool) or value is None or isinstance(value, (int, float, str)):
        if key is not None and _is_secret_name(key) and isinstance(value, str):
            return _REDACTED
        return value
    return str(value)


def _explicit_keys(path: Path) -> tuple[set[Key], list[str], set[Key]]:
    """(known-space leaves, new-spelling leftovers, raw leaves).

    Keys are normalised to the internal schema first, so an old and a new
    spelling of the same config classify identically. Raw leaves keep the
    section names as written, for the per-section count.
    """
    try:
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
    except Exception:
        return set(), [], set()
    raw = flatten_leaves(data)
    try:
        from .config_alias import normalize_config

        normalized = normalize_config(data)
    except Exception:
        return raw, [], raw
    return flatten_leaves(normalized.data), list(normalized.unknown), raw


def _classify_keys(explicit: set[Key], model: type[BaseModel]) -> tuple[set[Key], list[Key]]:
    """(known explicit keys, unknown explicit keys) — indices collapsed."""
    known_paths = model_leaves(model)
    extras = extras_prefixes(model)
    known: set[Key] = set()
    unknown: list[Key] = []
    for key in explicit:
        plain = strip_indices(key)
        if plain in known_paths or any(plain[: len(pref)] == pref for pref in extras):
            known.add(plain)
            continue
        unknown.append(key)
    return known, sorted(unknown)


def _host_key(raw: object) -> str:
    try:
        return str(raw).strip().rstrip("/").lower()
    except Exception:
        return ""


def _bool_attr(obj: object, attr: str) -> bool:
    try:
        return bool(getattr(obj, attr, False))
    except Exception:
        return False


def _shape(cfg: AppConfig) -> dict:
    """Resolved deployment shape: topology plus the providers the config allows."""
    source_host = _host_key(getattr(cfg.source, "host", ""))
    dest_host = _host_key(getattr(cfg.dest, "host", ""))
    split = bool(source_host and dest_host and source_host != dest_host)

    observe = ["source", "dest"]
    if getattr(cfg, "watch_dir", None) is not None:
        observe.append("watch")

    cs = getattr(cfg, "cross_seed", None)
    prow = getattr(cfg, "prowlarr", None)
    acquire: list[str] = []
    if _bool_attr(prow, "enabled") and _bool_attr(cs, "allow_prowlarr_cross_seed"):
        acquire.append("prowlarr_cross_seed")
    if _bool_attr(cs, "refetch_public_via_prowlarr"):
        acquire.append("public_refetch")
    try:
        sftp = getattr(cfg.source, "deluge_sftp", None)
    except Exception:
        sftp = None
    if _bool_attr(sftp, "enabled"):
        acquire.append("sftp_export")
    elif str(getattr(cfg.source, "type", "")).lower() == "qbittorrent":
        acquire.append("client_export")
    if "watch" in observe:
        acquire.append("watch_dir")

    try:
        indexers = len(list(getattr(prow, "download_indexers", None) or []))
    except Exception:
        indexers = 0
    try:
        tracker_map = len(prow.tracker_map.model_dump() or {})
    except Exception:
        tracker_map = 0

    return {
        "mode": "split" if split else "single",
        "observe": observe,
        "source": {
            "type": str(getattr(cfg.source, "type", "")),
            "host": str(getattr(cfg.source, "host", "")),
            "category": str(getattr(cfg.source, "category", "")),
            "min_age_seconds": int(getattr(cfg.source, "min_age_seconds", 0) or 0),
        },
        "dest": {
            "type": "qbittorrent",
            "host": str(getattr(cfg.dest, "host", "")),
            "save_path": _redact(getattr(cfg.dest, "save_path", "")),
        },
        "acquire": acquire,
        "acquire_detail": {
            "prowlarr_enabled": _bool_attr(prow, "enabled"),
            "download_indexers": indexers,
            "tracker_map_entries": tracker_map,
            "allow_prowlarr_cross_seed": _bool_attr(cs, "allow_prowlarr_cross_seed"),
            "allow_ssh_export": _bool_attr(cs, "allow_ssh_export"),
            "fallback_to_racing_torrent": _bool_attr(
                cs, "fallback_to_racing_torrent_on_prowlarr_timeout"
            ),
        },
        "stage": {
            "path": _redact(getattr(cfg.ssd, "path", "")),
            "max_inflight_bytes": int(getattr(cfg.ssd, "max_inflight_bytes", 0) or 0),
            "skip_single_file_bytes": int(getattr(cfg.ssd, "skip_movie_larger_than_bytes", 0) or 0),
        },
        "transfer": {
            "provider": "rclone",
            "binary": _redact(getattr(cfg.rclone, "binary", "")),
            "remote_default": str(getattr(cfg.rclone.remote, "default", "")),
            "remote_unsorted": str(getattr(cfg.rclone.remote, "unsorted", "")),
            "max_concurrent_moves": int(getattr(cfg.rclone, "max_concurrent_moves", 0) or 0),
            "move_timeout_seconds": float(getattr(cfg.rclone, "move_timeout_seconds", 0) or 0),
        },
        "seed": {
            "provider": "fuse",
            "mount": _redact(getattr(cfg.rclone.fuse, "mount", "")),
            "mount_unsorted": _redact(getattr(cfg.rclone.fuse, "mount_unsorted", "")),
            "reinject_delay_seconds": int(getattr(cfg, "fuse_reinject_delay_seconds", 0) or 0),
            "manual_move_timeout_seconds": int(
                getattr(cfg, "fuse_manual_move_timeout_seconds", 0) or 0
            ),
        },
        "cleanup": {
            "enabled": _bool_attr(getattr(cfg, "cleanup", None), "enabled"),
            "dry_run": _bool_attr(getattr(cfg, "cleanup", None), "dry_run"),
        },
        "control": {
            "telegram": _bool_attr(getattr(cfg, "telegram", None), "enabled"),
            "api": _bool_attr(getattr(cfg, "api", None), "enabled"),
            "logging_sink": _bool_attr(getattr(cfg, "logging_sink", None), "enabled"),
        },
    }


def summarize(cfg: AppConfig, config_path: Path | str) -> dict:
    """Canonical summary document for one loaded config."""
    path = Path(str(config_path))
    explicit, alias_unknown, raw = _explicit_keys(path)
    known, unknown = _classify_keys(explicit, AppConfig)
    model_fields = len(model_leaves(AppConfig))
    by_section: dict[str, int] = {}
    for key in raw:
        if key:
            by_section[key[0]] = by_section.get(key[0], 0) + 1
    shape = _shape(cfg)
    resolved = _redact(cfg)
    digest_src = json.dumps(
        {"shape": shape, "resolved": resolved},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(digest_src.encode("utf-8")).hexdigest()[:16]
    try:
        import racing_sync

        version = str(getattr(racing_sync, "__version__", ""))
    except Exception:
        version = ""
    return {
        "config": str(path),
        "app_version": version,
        "shape": shape,
        "keys": {
            "explicit": len(raw),
            "model": model_fields,
            "defaulted": max(0, model_fields - len(known)),
            "unknown": [*(".".join(key) for key in unknown), *dict.fromkeys(alias_unknown)],
            "by_section": dict(sorted(by_section.items())),
        },
        "resolved": resolved,
        "digest": f"sha256:{digest}",
    }


def format_summary(summary: dict) -> str:
    """Human-readable form; no secret values ever reach this function."""
    shape = summary.get("shape") or {}
    keys = summary.get("keys") or {}
    control = shape.get("control") or {}
    control_on = ", ".join(k for k, v in control.items() if v) or "none"
    cleanup = shape.get("cleanup") or {}
    transfer = shape.get("transfer") or {}
    seed = shape.get("seed") or {}
    source = shape.get("source") or {}
    dest = shape.get("dest") or {}

    def _join(values: object) -> str:
        try:
            return ", ".join(str(v) for v in values) or "none"
        except Exception:
            return "none"

    lines = [
        f"Config:   {summary.get('config', '')}  (racing-sync {summary.get('app_version', '')})",
        (
            f"Topology: {shape.get('mode', '?')}"
            f"  (source {source.get('type', '?')} @ {source.get('host', '?')};"
            f" dest {dest.get('type', '?')} @ {dest.get('host', '?')})"
        ),
        f"Observe:  {_join(shape.get('observe'))}",
        f"Acquire:  {_join(shape.get('acquire'))}",
        (
            f"Transfer: {transfer.get('provider', '?')} -> {transfer.get('remote_default', '')}"
            f" (unsorted {transfer.get('remote_unsorted', '')},"
            f" timeout {int(float(transfer.get('move_timeout_seconds', 0) or 0))}s)"
        ),
        (
            f"Seed:     {seed.get('provider', '?')} -> {seed.get('mount', '')}"
            f" (+ {seed.get('mount_unsorted', '')})"
        ),
        f"Cleanup:  {'on' if cleanup.get('enabled') else 'off'}   Control: {control_on}",
        (
            f"Keys:     {keys.get('explicit', 0)} explicit / {keys.get('defaulted', 0)}"
            f" defaulted / {len(keys.get('unknown') or [])} unknown"
        ),
        f"Digest:   {summary.get('digest', '')}",
    ]
    return "\n".join(lines)


def summary_json(summary: dict) -> str:
    return json.dumps(summary, indent=2, sort_keys=True, default=str)


__all__ = [
    "format_summary",
    "summarize",
    "summary_json",
]
