"""One shared rule for where a release physically lands on the seed remote.

Extracted from the MOVING stage so the decision exists exactly once. Two
callers: ``MoveMixin._seed_layout_for`` (VPS2 move stage, names from the
SSD file listing) and the upcoming VPS1 push feature (names from a
.torrent blob, ``batches_total=0``, no SSD state — the decision must be
identical from either source).

Pure: the caller supplies every already-resolved string (remote bases,
racing wrapper), so this module never touches the filesystem or a client.
Consistent with the consumer side ``coordinator_fuse_entries._seed_root_for``
— wrapped blobs seed at the kind mount root, flat blobs one level inside
``mount/<seed_subdir>``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .coordinator_content import route_kind_is_sorted
from .torrent_layout import common_top, is_flat, join_remote


@dataclass(frozen=True)
class SeedLayout:
    """The physical seed-layout decision for one release."""

    remote_base: str
    """Remote the kind routes to (remote_default for sorted kinds)."""

    seed_subdir: str
    """Physical top folder under the seed mount, pinned on the row ("" = root)."""

    wrapper: str
    """Extra folder the move joins onto remote_base ("" = move to the base)."""

    target_remote: str
    """Where files actually move: join_remote(remote_base, wrapper)."""


def decide_seed_layout(
    *,
    kind: str,
    target_names: Iterable[str],
    batches_total: int,
    pinned_seed_subdir: str,
    racing_wrapper: str,
    remote_default: str,
    remote_unsorted: str,
) -> SeedLayout:
    """Decide remote base, pinned seed_subdir and the move target.

    Callers: the VPS2 move stage (via ``MoveMixin._seed_layout_for``) and
    the VPS1 push feature (blob-derived names, ``batches_total=0``).

    Rules (unchanged from the inline move-stage version this was extracted
    from):

    - sorted kinds (movie/season/mixed) route to ``remote_default``,
      everything else to ``remote_unsorted``;
    - ``batches_total > 1``: batches already moved under the target's own
      layout; keep its top and add no wrapper (a wrapper now would split
      the files across two roots);
    - wrapped names: ``seed_subdir`` is the shared top, no wrapper — the
      top already exists on the remote;
    - flat names: wrap into ``pinned_seed_subdir`` when one was pinned
      before (crash-replay stability), else into ``racing_wrapper``;
    - anything else: ``seed_subdir`` empty, no wrapper.

    ``racing_wrapper`` must be the already-resolved string (the
    coordinator's sibling lookup is async); pass "" when there is no
    sibling evidence.
    """
    remote = remote_default if route_kind_is_sorted(kind) else remote_unsorted
    try:
        batches = int(batches_total or 0)
    except (TypeError, ValueError):
        batches = 0
    target_top = common_top(target_names)
    wrapper = ""
    if batches > 1:
        seed_subdir = target_top or ""
    elif target_top:
        seed_subdir = target_top
    elif is_flat(target_names):
        # A replayed attempt must keep the layout chosen before the
        # crash; a fresh row derives it from its racing siblings.
        wrapper = str(pinned_seed_subdir or "").strip()
        if not wrapper:
            wrapper = str(racing_wrapper or "").strip()
        seed_subdir = wrapper
    else:
        seed_subdir = ""
    return SeedLayout(
        remote_base=remote,
        seed_subdir=seed_subdir,
        wrapper=wrapper,
        target_remote=join_remote(remote, wrapper),
    )


__all__ = ["SeedLayout", "decide_seed_layout"]
