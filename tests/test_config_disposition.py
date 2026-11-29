"""The config disposition registry must stay exhaustive.

Every field of the internal schema has a fate: kept, renamed to the new
spelling, deprecated (kept + warned), or removed. A new setting without an
entry here fails this test, which is the point — the capability-chain
refactor must never silently drop or forget a knob.
"""

from __future__ import annotations

from racing_sync.config import AppConfig
from racing_sync.config_disposition import (
    DEPRECATED,
    NEW_ONLY,
    REMOVED,
    RENAMES,
    disposition,
    extras_prefixes,
    model_leaves,
    rename_prefixes,
)

FIELDS = {".".join(path) for path in model_leaves(AppConfig) | extras_prefixes(AppConfig)}


def _covers(prefix: str, path: str) -> bool:
    return path == prefix or path.startswith(prefix + ".")


def test_every_field_has_a_disposition():
    assert FIELDS, "schema introspection found no fields"
    for path in sorted(FIELDS):
        assert disposition(path) in {"keep", "rename", "deprecate", "remove"}, path


def test_rename_prefixes_cover_real_fields():
    for old, new in RENAMES.items():
        covered = [path for path in FIELDS if _covers(old, path)]
        assert covered, f"rename {old!r} -> {new!r} covers no schema field"
        assert new, f"rename {old!r} has an empty target"


def test_rename_prefixes_do_not_overlap():
    keys = sorted(RENAMES)
    for i, first in enumerate(keys):
        for second in keys[i + 1 :]:
            assert not _covers(first, second), (first, second)
            assert not _covers(second, first), (first, second)


def test_each_renamed_field_is_covered_exactly_once():
    renamed = {path for path in FIELDS if disposition(path) == "rename"}
    covered: list[str] = []
    for old in RENAMES:
        covered.extend(path for path in renamed if _covers(old, path))
    assert sorted(covered) == sorted(renamed)


def test_rename_targets_are_unique_and_new():
    targets = list(RENAMES.values())
    assert len(targets) == len(set(targets)), "two fields renamed to the same path"
    clashes = sorted(set(targets) & FIELDS)
    assert not clashes, f"rename targets collide with existing fields: {clashes}"


def test_deprecated_and_removed_keys_exist():
    for path, hint in DEPRECATED.items():
        assert path in FIELDS, f"deprecated key {path!r} is not a schema field"
        assert hint.strip(), f"deprecated key {path!r} has no replacement hint"
    for path, hint in REMOVED.items():
        assert path in FIELDS, f"removed key {path!r} is not a schema field"
        assert hint.strip(), f"removed key {path!r} has no migration hint"


def test_new_only_keys_live_outside_the_internal_namespace():
    internal = {path.split(".", 1)[0] for path in FIELDS}
    for path in NEW_ONLY:
        head = path.split(".", 1)[0]
        assert head not in internal, f"new-only key {path!r} reuses internal section {head!r}"
    assert not set(NEW_ONLY) & FIELDS


def test_rename_order_moves_deep_targets_first():
    """[storage].save_path must land before the [ssd] remainder merges."""
    new_paths = [".".join(new) for new, _old in rename_prefixes()]
    assert new_paths.index("storage.save_path") < new_paths.index("storage")
