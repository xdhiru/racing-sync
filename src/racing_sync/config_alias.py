"""Resolve the new capability-chain spellings onto the internal schema.

`[client]`, `[storage]`, `[remote]`, `[transfer]`, `[seed]`, `[indexers]`,
`[watch]` and `[acquire]` are rewritten into today's tables *before*
validation, so every caller keeps reading `cfg.source`, `cfg.dest`,
`cfg.rclone`, ... — the rename lands without touching behaviour.

Rules:

- both spellings of the same key are accepted when they agree; different
  values are refused rather than guessed;
- new-only switches (`enabled`, `provider`) are resolved here. A pipeline
  the daemon cannot honour yet is refused, so a config never claims a shape
  the runtime does not run;
- deprecated keys are kept working and reported once per load.

Unknown keys inside a new table are collected (not silently dropped) so
`from_toml` and `config summary` can still flag typos.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .config import AppConfig
from .config_disposition import (
    DEPRECATED,
    NEW_ONLY,
    REMOVED,
    canonical_leaves,
    rename_prefixes,
)

#: Top-level tables that belong to the new spelling.
NEW_SECTIONS = (
    "acquire",
    "client",
    "indexers",
    "remote",
    "seed",
    "storage",
    "transfer",
    "watch",
)

#: Provider names per link (the full chain surface; later phases enable the rest).
_PROVIDERS: dict[str, tuple[str, ...]] = {
    "acquire": ("auto", "cross_seed", "local", "direct_export", "external", "none"),
    "transfer": ("rclone", "local_move", "none", "external"),
    "seed": ("path", "existing", "none"),
}

#: The provider this release actually runs for each link.
_CURRENT_PROVIDER = {"acquire": "auto", "transfer": "rclone", "seed": "path"}

#: Everything a new-spelling table may contain, resolved from the schema.
_KNOWN_LEAVES, _KNOWN_PREFIXES = canonical_leaves(AppConfig)
_KNOWN_LEAVES |= set(NEW_ONLY)


@dataclass(frozen=True)
class NormalizedConfig:
    """Rewritten config data plus what the rewrite noticed."""

    data: dict[str, Any]
    deprecations: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()


def normalize_config(raw: Mapping[str, Any]) -> NormalizedConfig:
    """Rewrite new-spelling tables into the internal schema (pure, no I/O)."""
    if not isinstance(raw, Mapping):
        raise ValueError("config root must be a table")
    data = copy.deepcopy(dict(raw))
    aliases = tuple(name for name in NEW_SECTIONS if name in data)
    deprecations = _deprecations(data)
    _reject_removed(data)
    unknown: list[str] = []

    # Typos are pruned before the rename absorbs the table, so they are
    # reported under the spelling the user wrote (`client.hots`), not the
    # internal one the rename turned it into.
    for name in NEW_SECTIONS:
        section = data.get(name)
        if isinstance(section, dict):
            _prune_unknown(section, name, _KNOWN_LEAVES, _KNOWN_PREFIXES, unknown)

    _require_dest(data, aliases)
    _resolve_links(data)
    _resolve_indexers(data)
    _resolve_watch(data)
    _move_new_to_internal(data)
    _require_core(data, aliases)
    for name in NEW_SECTIONS:
        if name in data:
            _collect_leftovers(data.pop(name), name, unknown)

    return NormalizedConfig(
        data=data,
        deprecations=deprecations,
        unknown=tuple(unknown),
        aliases=aliases,
    )


def _deprecations(data: Mapping[str, Any]) -> tuple[str, ...]:
    out = []
    for path, hint in DEPRECATED.items():
        if _has_path(data, tuple(path.split("."))):
            out.append(f"`{path}` is deprecated — prefer {hint}; the old key still wins")
    return tuple(out)


def _reject_removed(data: Mapping[str, Any]) -> None:
    for path, hint in REMOVED.items():
        if _has_path(data, tuple(path.split("."))):
            raise ValueError(f"`{path}` is no longer supported — {hint}")


def _has_path(node: Any, parts: tuple[str, ...]) -> bool:
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


def _prune_unknown(
    node: dict[str, Any],
    prefix: str,
    known: set[str],
    prefixes: set[str],
    out: list[str],
) -> None:
    """Drop (and report) leaves a new-spelling table cannot contain."""
    for key in list(node):
        value = node[key]
        path = f"{prefix}.{key}"
        if isinstance(value, dict):
            _prune_unknown(value, path, known, prefixes, out)
            continue
        if isinstance(value, list) and any(isinstance(item, dict) for item in value):
            # List multiplicity collapses: [[indexers.download_indexers]]
            # elements share the field path of the element model.
            for item in value:
                if isinstance(item, dict):
                    _prune_unknown(item, path, known, prefixes, out)
            continue
        if path in known or any(path == p or path.startswith(p + ".") for p in prefixes):
            continue
        out.append(path)
        del node[key]


def _require_dest(data: dict[str, Any], aliases: tuple[str, ...]) -> None:
    """Check before the move: [storage].save_path must not invent [dest]."""
    if aliases and "dest" not in data:
        raise ValueError(
            "the new spelling needs a [dest] table as well: the client that "
            "seeds finished content. Racing and seeding from one client is "
            "not available yet."
        )


def _resolve_links(data: dict[str, Any]) -> None:
    """Resolve link switches/providers into the internal `pipeline` flags.

    Acquire stays on: without it nothing can reach staging yet (external
    acquisition lands with its own provider). Transfer/seed may be
    disabled — the chain then terminates earlier (see `racing_sync.chain`).
    """
    pipeline: dict[str, bool] = {}
    for link in ("acquire", "transfer", "seed"):
        section = data.get(link)
        if section is None:
            continue
        if not isinstance(section, dict):
            raise ValueError(f"[{link}] must be a table")
        enabled = section.pop("enabled", None)
        provider = section.pop("provider", None)
        if enabled is not None and not isinstance(enabled, bool):
            raise ValueError(f"[{link}].enabled must be true or false")
        if provider is not None and provider not in _PROVIDERS[link]:
            raise ValueError(
                f"[{link}].provider = {provider!r} is not a known provider "
                f"(choose one of: {', '.join(_PROVIDERS[link])})"
            )
        if link == "acquire":
            if enabled is False:
                raise ValueError(
                    "[acquire].enabled = false is not available yet — external "
                    "acquisition (bytes placed by the operator) lands with its "
                    "own provider. Remove the section to keep today's pipeline."
                )
            if provider not in (None, "auto"):
                raise ValueError(
                    f"[acquire].provider = {provider!r} is not available yet — "
                    "this release runs provider = 'auto'"
                )
            pipeline["acquire"] = True
            continue

        disabling = enabled is False or provider == "none"
        if enabled is False and provider in ("rclone", "path"):
            raise ValueError(
                f"[{link}].enabled = false contradicts provider = {provider!r} — "
                "drop the provider or set enabled = true"
            )
        if enabled is True and provider == "none":
            raise ValueError(
                f"[{link}].enabled = true contradicts provider = 'none' — "
                "drop the provider or set enabled = false"
            )
        if not disabling and provider is not None and provider != _CURRENT_PROVIDER[link]:
            raise ValueError(
                f"[{link}].provider = {provider!r} is not available yet — "
                f"this release runs provider = {_CURRENT_PROVIDER[link]!r}"
            )
        pipeline[link] = not disabling
        if link == "seed":
            _derive_seed_unsorted(section)
    if pipeline:
        data.setdefault("pipeline", {}).update(pipeline)


def _derive_seed_unsorted(section: dict[str, Any]) -> None:
    """`unsorted_path` defaults to `<path>/unsorted` (the documented layout)."""
    if "unsorted_path" in section:
        return
    path = section.get("path")
    if isinstance(path, str) and path.strip():
        section["unsorted_path"] = path.strip().rstrip("/\\") + "/unsorted"


def _resolve_indexers(data: dict[str, Any]) -> None:
    section = data.get("indexers")
    if not isinstance(section, dict):
        return
    enabled = section.pop("enabled", None)
    if enabled is not None and not isinstance(enabled, bool):
        raise ValueError("[indexers].enabled must be true or false")
    # Absent switch: a table that carries content means it is meant to be on.
    section["enabled"] = bool(section) if enabled is None else enabled


def _resolve_watch(data: dict[str, Any]) -> None:
    section = data.get("watch")
    if not isinstance(section, dict):
        return
    enabled = section.pop("enabled", None)
    if enabled is not None and not isinstance(enabled, bool):
        raise ValueError("[watch].enabled must be true or false")
    if enabled is False:
        # Keys stay in the file; re-enabling is a flip, not a rewrite.
        del data["watch"]
        return
    if "path" not in section:
        if section or enabled is True:
            raise ValueError(
                "[watch].path is required when [watch] is present "
                "(remove the table to disable the watch dir)"
            )
        del data["watch"]


def _move_new_to_internal(data: dict[str, Any]) -> None:
    for new_parts, old_parts in rename_prefixes():
        found, value = _pop_path(data, new_parts)
        if found:
            _merge_into(data, old_parts, value, ".".join(new_parts), ".".join(old_parts))


def _pop_path(node: Any, parts: tuple[str, ...]) -> tuple[bool, Any]:
    if not isinstance(node, dict) or not parts:
        return False, None
    head, rest = parts[0], parts[1:]
    if not rest:
        if head in node:
            return True, node.pop(head)
        return False, None
    child = node.get(head)
    if not isinstance(child, dict):
        return False, None
    found, value = _pop_path(child, rest)
    if found and not child:
        node.pop(head, None)
    return found, value


def _merge_into(
    dst: dict[str, Any],
    parts: tuple[str, ...],
    value: Any,
    new_label: str,
    old_label: str,
) -> None:
    head, rest = parts[0], parts[1:]
    if not rest:
        if head not in dst:
            dst[head] = value
        else:
            dst[head] = _merge_same(dst[head], value, new_label, old_label)
        return
    node = dst.setdefault(head, {})
    if not isinstance(node, dict):
        raise ValueError(_conflict(new_label, old_label))
    _merge_into(node, rest, value, new_label, old_label)


def _merge_same(old: Any, new: Any, new_label: str, old_label: str) -> Any:
    if old == new:
        return old
    if isinstance(old, dict) and isinstance(new, dict):
        for key, value in new.items():
            if key in old:
                old[key] = _merge_same(old[key], value, f"{new_label}.{key}", f"{old_label}.{key}")
            else:
                old[key] = value
        return old
    raise ValueError(_conflict(new_label, old_label))


def _conflict(new_label: str, old_label: str) -> str:
    return (
        f"config sets {new_label} and {old_label} to different values — "
        "keep one spelling (the old and new names are the same setting)"
    )


def _require_core(data: dict[str, Any], aliases: tuple[str, ...]) -> None:
    if not aliases:
        return
    dest = data.get("dest")
    if not isinstance(dest, dict):
        return  # malformed table: let schema validation report it
    if "storage" in aliases:
        if not isinstance(data.get("ssd"), dict) or "path" not in data["ssd"]:
            raise ValueError("[storage].path is required: the SSD staging directory")
        if not dest.get("save_path"):
            raise ValueError("[storage].save_path is required: where [dest] stages downloads")
    pipeline = data.get("pipeline") or {}
    if "seed" in aliases and pipeline.get("seed", True):
        fuse = data.get("rclone", {}).get("fuse")
        if not isinstance(fuse, dict) or "mount" not in fuse:
            raise ValueError(
                "[seed].path is required: where finished content is seeded from "
                "(a fuse mount or a plain directory), or set enabled = false"
            )
    if "remote" in aliases and pipeline.get("transfer", True):
        remote = data.get("rclone", {}).get("remote")
        if not isinstance(remote, dict) or "default" not in remote or "unsorted" not in remote:
            raise ValueError(
                "[remote].path and [remote].unsorted are required "
                "(rclone 'name:path/' destinations for folders and single episodes), "
                "or set [transfer] enabled = false"
            )


def _collect_leftovers(node: Any, prefix: str, out: list[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            _collect_leftovers(value, f"{prefix}.{key}", out)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            _collect_leftovers(value, f"{prefix}[{index}]", out)
    else:
        out.append(prefix)
