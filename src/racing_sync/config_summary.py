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
from collections.abc import Callable
from pathlib import Path

from pydantic import BaseModel, SecretStr

from .chain import provider_name
from .config import AppConfig
from .config_disposition import (
    Key,
    extras_prefixes,
    flatten_leaves,
    model_leaves,
    strip_indices,
)
from .runtime import Runtime

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

#: Internal fields the alias layer sets; not part of the documented file
#: surface, so they are left out of key counts and the digest.
_INTERNAL_SECTIONS = frozenset({"pipeline"})


def _public_leaves(model: type[BaseModel]) -> set[Key]:
    return {path for path in model_leaves(model) if path[0] not in _INTERNAL_SECTIONS}


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
    known_space = {
        key for key in flatten_leaves(normalized.data) if key and key[0] not in _INTERNAL_SECTIONS
    }
    return known_space, list(normalized.unknown), raw


def _classify_keys(explicit: set[Key], model: type[BaseModel]) -> tuple[set[Key], list[Key]]:
    """(known explicit keys, unknown explicit keys) — indices collapsed."""
    known_paths = _public_leaves(model)
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


def _view(default: object, reader: Callable[[], object]) -> object:
    """Read a Runtime view; a partial config double keeps today's default."""
    try:
        return reader()
    except Exception:
        return default


def _shape(cfg: AppConfig) -> dict:
    """Resolved deployment shape: topology plus the providers the config allows."""
    rt = Runtime(cfg)
    chain = rt.chain

    # `Chain` decides when the origins address the same client, but a partial
    # config double has no hosts to compare: keep today's "single" there.
    source_host = _host_key(getattr(cfg.source, "host", ""))
    dest_host = _host_key(getattr(cfg.dest, "host", ""))
    split = bool(source_host and dest_host and source_host != dest_host and not chain.single_box)

    observe = list(chain.observe)

    prow = getattr(cfg, "prowlarr", None)
    acquire: list[str] = []
    try:
        sftp = getattr(cfg.source, "deluge_sftp", None)
    except Exception:
        sftp = None
    if chain.waits_for_copy:
        acquire.append("external_copy")
    elif chain.adopts_local:
        acquire.append("local_client")
    else:
        if _bool_attr(rt.indexers, "enabled") and _bool_attr(
            rt.acquire, "allow_prowlarr_cross_seed"
        ):
            acquire.append("prowlarr_cross_seed")
        if _bool_attr(rt.acquire, "refetch_public_via_prowlarr"):
            acquire.append("public_refetch")
        # `rt.intake.blob_source` additionally requires a Deluge source; the
        # summary has always reported any enabled export, so keep that test.
        if _bool_attr(sftp, "enabled"):
            acquire.append("sftp_export")
        elif str(_view("", lambda: rt.intake.client_type)).lower() == "qbittorrent":
            acquire.append("client_export")
    if "watch" in observe:
        acquire.append("watch_dir")

    # Runtime exposes the real entry count; the summary has always reported
    # the dumped model's top-level keys (the nested `entries` table = 1).
    try:
        tracker_map = len(prow.tracker_map.model_dump() or {})
    except Exception:
        tracker_map = 0

    return {
        "mode": "split" if split else "single",
        "observe": observe,
        "source": {
            "type": str(_view("", lambda: rt.intake.client_type)),
            "host": str(getattr(cfg.source, "host", "")),
            "category": str(_view("", lambda: rt.intake.category)),
            "min_age_seconds": int(_view(0, lambda: rt.intake.min_age_seconds) or 0),
        },
        "dest": {
            "type": "qbittorrent",
            "host": str(getattr(cfg.dest, "host", "")),
            "save_path": _redact(_view("", lambda: rt.storage.save_path)),
        },
        "acquire": acquire,
        "acquire_detail": {
            "prowlarr_enabled": _bool_attr(rt.indexers, "enabled"),
            "download_indexers": _view(0, lambda: len(rt.indexers.download_indexer_names)),
            "tracker_map_entries": tracker_map,
            "allow_prowlarr_cross_seed": _bool_attr(rt.acquire, "allow_prowlarr_cross_seed"),
            "allow_ssh_export": _bool_attr(rt.acquire, "allow_ssh_export"),
            "fallback_to_racing_torrent": _bool_attr(rt.acquire, "fallback_to_racing_torrent"),
        },
        "stage": {
            "path": _redact(_view("", lambda: rt.storage.path)),
            "max_inflight_bytes": int(_view(0, lambda: rt.storage.max_inflight_bytes) or 0),
            "skip_single_file_bytes": int(_view(0, lambda: rt.storage.skip_single_file_bytes) or 0),
        },
        "transfer": {
            "provider": rt.transfer.provider,
            "binary": _redact(_view("", lambda: rt.transfer.binary)),
            "remote_default": str(_view("", lambda: rt.transfer.remote_default)),
            "remote_unsorted": str(_view("", lambda: rt.transfer.remote_unsorted)),
            # Runtime.limits applies the [general] override; the summary has
            # always reported the [rclone] value, so read the field directly.
            "max_concurrent_moves": int(getattr(cfg.rclone, "max_concurrent_moves", 0) or 0),
            "move_timeout_seconds": float(_view(0, lambda: rt.transfer.timeout_seconds) or 0),
        },
        "seed": {
            "provider": _seed_provider(cfg),
            "mount": _redact(_view("", lambda: rt.seed.mount) or ""),
            "mount_unsorted": _redact(_view("", lambda: rt.seed.mount_unsorted) or ""),
            "reinject_delay_seconds": int(_view(0, lambda: rt.seed.reinject_delay_seconds) or 0),
            # Runtime reads fuse.manual_move_timeout directly; the AppConfig
            # property keeps today's 43200 fallback when there is no fuse.
            "manual_move_timeout_seconds": int(
                getattr(cfg, "fuse_manual_move_timeout_seconds", 0) or 0
            ),
        },
        "cleanup": {
            "enabled": _bool_attr(rt.cleanup, "enabled"),
            "dry_run": _bool_attr(getattr(cfg, "cleanup", None), "dry_run"),
        },
        "control": {
            "telegram": _bool_attr(rt.control, "telegram"),
            "api": _bool_attr(rt.control, "api"),
            "logging_sink": _bool_attr(getattr(cfg, "logging_sink", None), "enabled"),
        },
    }


def _seed_provider(cfg: AppConfig) -> str:
    """fuse (shows as "fuse" for continuity) | existing | none."""
    raw = getattr(cfg.pipeline, "seed", None) if hasattr(cfg, "pipeline") else None
    provider = provider_name(raw, "path")
    if getattr(cfg.rclone, "fuse", None) is None:
        return "none"
    return "fuse" if provider == "path" else ("existing" if provider == "existing" else "none")


def summarize(cfg: AppConfig, config_path: Path | str) -> dict:
    """Canonical summary document for one loaded config."""
    path = Path(str(config_path))
    explicit, alias_unknown, raw = _explicit_keys(path)
    known, unknown = _classify_keys(explicit, AppConfig)
    model_fields = len(_public_leaves(AppConfig))
    by_section: dict[str, int] = {}
    for key in raw:
        if key:
            by_section[key[0]] = by_section.get(key[0], 0) + 1
    shape = _shape(cfg)
    resolved = _redact(cfg)
    for name in _INTERNAL_SECTIONS:
        resolved.pop(name, None)
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
