"""Every config field's fate in the capability-chain refactor.

One inventory, checked by `tests/test_config_disposition.py`, so a setting
cannot be silently dropped or forgotten while the config is re-spelled:

- **keep**: unlisted — the field keeps its spelling;
- **rename**: `RENAMES` maps today's path to the new spelling (a section
  prefix covers every descendant that keeps its name);
- **deprecate**: `DEPRECATED` — still honoured, warned about once per load;
- **remove**: `REMOVED` — refused with a migration hint;
- **new-only**: `NEW_ONLY` — keys that exist only in the new spelling
  (link switches and provider choices), consumed by `config_alias`.

The same module owns the link/provider registry (`LINK_PROVIDERS`,
`SUPPORTED_PROVIDERS`, `DEFAULT_PROVIDER`) and the optional feature sections
(`FEATURE_SECTIONS`), so `config_alias` and `config_ux` import one table
instead of each keeping a copy that can drift.

Paths are dotted field paths of the *internal* schema (`rclone.fuse.mount`).
Alias direction is new -> internal; the tables read old -> new because that
is the direction a reader (and `config migrate`) needs.
"""

from __future__ import annotations

import typing
from typing import Any

from pydantic import BaseModel

Key = tuple[str, ...]

#: Internal path -> canonical new path. Prefixes allowed when every
#: descendant keeps its field name (`source` -> `client`).
RENAMES: dict[str, str] = {
    # [source] (+ [source.nginx], [source.deluge_sftp]) -> [client]
    "source": "client",
    # [ssd] and [dest].save_path -> [storage]
    "ssd": "storage",
    "dest.save_path": "storage.save_path",
    # [rclone] splits into [remote] (where) and [transfer] (how)
    "rclone.binary": "remote.binary",
    "rclone.config_path": "remote.config_path",
    "rclone.remote.default": "remote.path",
    "rclone.remote.unsorted": "remote.unsorted",
    "rclone.extra_move_flags": "transfer.extra_flags",
    "rclone.batch_move_extra_flags": "transfer.batch_extra_flags",
    "rclone.max_concurrent_moves": "transfer.max_concurrent_moves",
    "rclone.move_timeout_seconds": "transfer.timeout_seconds",
    # [rclone.fuse] -> [seed] (paths and timers keep their units)
    "rclone.fuse.mount": "seed.path",
    "rclone.fuse.mount_unsorted": "seed.unsorted_path",
    "rclone.fuse.reinject_delay_seconds": "seed.reinject_delay_seconds",
    "rclone.fuse.reinject_retry_gap_seconds": "seed.reinject_retry_gap_seconds",
    "rclone.fuse.reinject_backoff_seconds": "seed.reinject_backoff_seconds",
    "rclone.fuse.reinject_max_age_seconds": "seed.reinject_max_age_seconds",
    "rclone.fuse.readd_heal_missing_after_seconds": "seed.readd_heal_missing_after_seconds",
    "rclone.fuse.manual_move_check_seconds": "seed.manual_move_check_seconds",
    "rclone.fuse.manual_move_timeout_seconds": "seed.manual_move_timeout_seconds",
    # [watch_dir] -> [watch]
    "watch_dir": "watch",
    # [prowlarr] -> [indexers]
    "prowlarr": "indexers",
    # [cross_seed] (the acquisition strategy) -> [acquire]
    "cross_seed": "acquire",
}

#: Still honoured, but the canonical spelling is the value (one warning per
#: key per load). These are duplicate-precedence keys the new schema drops.
DEPRECATED: dict[str, str] = {
    "general.max_active_downloads": "[dest].max_active_downloads",
    "general.max_concurrent_moves": "[transfer].max_concurrent_moves",
    "rclone.reinject_delay_seconds": "[seed].reinject_delay_seconds",
}

#: Refused at load with the hint as the message. Empty today.
REMOVED: dict[str, str] = {}

#: Provider names each link's file surface knows (the full chain vocabulary).
#: Later phases enable the rest — see `SUPPORTED_PROVIDERS`.
LINK_PROVIDERS: dict[str, tuple[str, ...]] = {
    "acquire": ("auto", "cross_seed", "local", "direct_export", "external", "none"),
    "transfer": ("rclone", "local_move", "none", "external"),
    "seed": ("path", "existing", "none"),
}

#: Providers this release can actually run per link (a subset of
#: `LINK_PROVIDERS`). Everything else is refused as "not available yet".
SUPPORTED_PROVIDERS: dict[str, tuple[str, ...]] = {
    "acquire": ("auto", "local", "external"),
    "transfer": ("rclone", "local_move", "none", "external"),
    "seed": ("path", "existing", "none"),
}

#: What an unset provider means per link (legacy behaviour).
DEFAULT_PROVIDER: dict[str, str] = {
    "acquire": "auto",
    "transfer": "rclone",
    "seed": "path",
}

#: The optional sections `config enable|disable` toggles, in listing order.
FEATURE_SECTIONS: tuple[str, ...] = (
    "indexers",
    "watch",
    "telegram",
    "api",
    "cleanup",
    "logging_sink",
)

#: `NEW_ONLY` hints for the provider keys, generated from the registry so the
#: text cannot advertise names the alias layer refuses (or hide one it runs).
PROVIDER_HINTS: dict[str, str] = {
    f"{link}.provider": " | ".join(names) for link, names in SUPPORTED_PROVIDERS.items()
}

#: Keys that exist only in the new spelling; resolved by `config_alias`.
NEW_ONLY: dict[str, str] = {
    "watch.enabled": "absent = on when the section is non-empty",
    "indexers.enabled": "absent = on when the section carries content",
    "acquire.enabled": 'false is refused — use provider = "external" for adopt-only',
    "acquire.provider": PROVIDER_HINTS["acquire.provider"],
    "transfer.enabled": 'false is the same as provider = "none"',
    "transfer.provider": PROVIDER_HINTS["transfer.provider"],
    "transfer.path": "local_move destination root (absolute)",
    "transfer.unsorted": "defaults to <path>/unsorted",
    "seed.enabled": 'false is the same as provider = "none"',
    "seed.provider": PROVIDER_HINTS["seed.provider"],
    "seed.unsorted_path": "defaults to <seed.path>/unsorted",
}


def _model_types(annotation: object) -> list[type[BaseModel]]:
    """BaseModel classes reachable from an annotation (lists/unions included)."""
    try:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return [annotation]
    except Exception:
        return []
    found: list[type[BaseModel]] = []
    try:
        for arg in typing.get_args(annotation) or ():
            found.extend(_model_types(arg))
    except Exception:
        pass
    return found


def is_dict_annotation(annotation: object) -> bool:
    try:
        return typing.get_origin(annotation) is dict
    except Exception:
        return False


def model_leaves(model: type[BaseModel], prefix: Key = ()) -> set[Key]:
    """Field paths of a model tree; list multiplicity collapses (no indices)."""
    out: set[Key] = set()
    try:
        fields = model.model_fields or {}
    except Exception:
        return out
    for name, field in fields.items():
        subs = _model_types(field.annotation)
        path = prefix + (name,)
        if subs:
            for sub in subs:
                out |= model_leaves(sub, path)
        elif is_dict_annotation(field.annotation):
            # Arbitrary keys: represented as a prefix, not as leaves.
            continue
        else:
            out.add(path)
    return out


def extras_prefixes(model: type[BaseModel], prefix: Key = ()) -> set[Key]:
    """Paths that accept arbitrary keys (extra='allow' models, dict fields)."""
    out: set[Key] = set()
    try:
        fields = model.model_fields or {}
    except Exception:
        return out
    for name, field in fields.items():
        path = prefix + (name,)
        if is_dict_annotation(field.annotation):
            out.add(path)
            continue
        for sub in _model_types(field.annotation):
            cfg = getattr(sub, "model_config", None) or {}
            if cfg.get("extra") == "allow":
                out.add(path)
            else:
                out |= extras_prefixes(sub, path)
    return out


def flatten_leaves(value: Any, prefix: Key = ()) -> set[Key]:
    """Leaf paths of a nested mapping (lists carry their indices)."""
    out: set[Key] = set()
    if isinstance(value, dict):
        for k, v in value.items():
            out |= flatten_leaves(v, prefix + (str(k),))
        return out
    if isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            out |= flatten_leaves(v, prefix + (str(i),))
        return out
    out.add(prefix)
    return out


def strip_indices(key: Key) -> Key:
    return tuple(part for part in key if not part.isdigit())


def rename_prefixes() -> list[tuple[Key, Key]]:
    """(new path parts, internal path parts) for every rename, deepest first."""
    pairs = [(tuple(new.split(".")), tuple(old.split("."))) for old, new in RENAMES.items()]
    # Deepest new path first: [storage].save_path lands before the [ssd]
    # remainder merges into the same [storage] table.
    return sorted(pairs, key=lambda pair: len(pair[0]), reverse=True)


def disposition(path: str) -> str:
    """keep | rename | deprecate | remove for an internal dotted path."""
    if path in REMOVED:
        return "remove"
    if path in DEPRECATED:
        return "deprecate"
    for old in RENAMES:
        if path == old or path.startswith(old + "."):
            return "rename"
    return "keep"


def canonical_leaves(model: type[BaseModel]) -> tuple[set[str], set[str]]:
    """New-spelling field paths (and arbitrary-key prefixes) for a model tree.

    The alias adapter uses this to spot typos inside a new table before the
    rename moves the table: a key that is not in the canonical set (and not
    under an arbitrary-key prefix such as a tracker map) is reported — and
    dropped — under the spelling the user actually wrote.
    """
    pairs = [(tuple(old.split(".")), tuple(new.split("."))) for old, new in RENAMES.items()]

    def _map(path: Key) -> str:
        for old, new in pairs:
            if path == old:
                return ".".join(new)
            if len(path) > len(old) and path[: len(old)] == old:
                return ".".join(new + path[len(old) :])
        return ".".join(path)

    return (
        {_map(path) for path in model_leaves(model)},
        {_map(path) for path in extras_prefixes(model)},
    )
