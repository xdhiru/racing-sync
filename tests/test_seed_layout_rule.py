"""Unit tests for the shared seed-layout rule (seed_layout.decide_seed_layout).

The move stage and the upcoming VPS1 push feature must make the identical
decision from these inputs, so the pure helper is pinned here directly:
no filesystem, no clients, fictional release names only. Joined remote
strings are asserted exactly, against the real ``join_remote`` behaviour
(rclone-style ``name:path`` bases so the slash form is platform-stable).
"""

from __future__ import annotations

from racing_sync.seed_layout import SeedLayout, decide_seed_layout
from racing_sync.torrent_layout import blob_files, join_remote
from racing_sync.watchdir import _bencode

SORTED = "remote:media"
UNSORTED = "remote:unsorted"

EPISODE_NAME = "Example.Show.S01E01.1080p.WEB.DL.AAC2.0.H.264-GROUP.mkv"
EPISODE_TOP = "Example.Show.S01E01.1080p.WEB.DL.AAC2.0.H.264-GROUP"
WRAPPED_NAMES = [f"{EPISODE_TOP}/{EPISODE_NAME}", f"{EPISODE_TOP}/sample.nfo"]
FLAT_NAMES = [EPISODE_NAME]
MOVIE_TOP = "Example.Film.2026.1080p.WEB.x264-GROUP"

# A minimal wrapped .torrent blob; the push feature derives names exactly
# like this (from the blob, with no SSD listing).
WRAPPED_BLOB = _bencode(
    {
        b"announce": b"http://tracker.example/announce",
        b"info": {
            b"name": EPISODE_TOP.encode(),
            b"files": [
                {b"length": 1000, b"path": [b"sample.nfo"]},
                {b"length": 5000, b"path": [EPISODE_NAME.encode()]},
            ],
            b"piece length": 16384,
            b"pieces": b"0" * 20,
        },
    }
)


def _decide(**kw):
    base = {
        "kind": "episode",
        "target_names": FLAT_NAMES,
        "batches_total": 1,
        "pinned_seed_subdir": "",
        "racing_wrapper": "",
        "remote_default": SORTED,
        "remote_unsorted": UNSORTED,
    }
    base.update(kw)
    return decide_seed_layout(**base)


def test_wrapped_multi_file_episode_keeps_its_own_top():
    layout = _decide(target_names=WRAPPED_NAMES)
    assert isinstance(layout, SeedLayout)
    assert layout.remote_base == UNSORTED
    assert layout.seed_subdir == EPISODE_TOP
    assert layout.wrapper == ""
    # No extra wrapper: the top already exists on the remote.
    assert layout.target_remote == UNSORTED


def test_flat_single_file_episode_wraps_into_the_racing_sibling_folder():
    layout = _decide(target_names=FLAT_NAMES, racing_wrapper=EPISODE_TOP)
    assert layout.remote_base == UNSORTED
    assert layout.seed_subdir == EPISODE_TOP
    assert layout.wrapper == EPISODE_TOP
    assert layout.target_remote == join_remote(UNSORTED, EPISODE_TOP)
    assert layout.target_remote == f"{UNSORTED}/{EPISODE_TOP}"


def test_flat_with_no_sibling_evidence_stays_at_the_base():
    layout = _decide(target_names=FLAT_NAMES)
    assert layout.seed_subdir == ""
    assert layout.wrapper == ""
    assert layout.target_remote == UNSORTED


def test_sorted_kinds_route_to_the_default_remote():
    for kind in ("movie", "season", "mixed"):
        layout = _decide(kind=kind, target_names=[f"{MOVIE_TOP}/{MOVIE_TOP}.mkv"])
        assert layout.remote_base == SORTED, kind
        assert layout.seed_subdir == MOVIE_TOP, kind
        assert layout.target_remote == SORTED, kind


def test_sorted_kind_flat_with_wrapper_joins_the_default_remote():
    layout = _decide(kind="movie", target_names=["Example.Film.2026.mkv"], racing_wrapper=MOVIE_TOP)
    assert layout.remote_base == SORTED
    assert layout.seed_subdir == MOVIE_TOP
    assert layout.target_remote == f"{SORTED}/{MOVIE_TOP}"


def test_multi_batch_keeps_the_targets_own_top_and_adds_no_wrapper():
    # Batches already moved under the target layout; a wrapper now would
    # split the files across two roots.
    layout = _decide(
        target_names=WRAPPED_NAMES,
        batches_total=3,
        racing_wrapper=EPISODE_TOP,
    )
    assert layout.seed_subdir == EPISODE_TOP
    assert layout.wrapper == ""
    assert layout.target_remote == UNSORTED


def test_replay_reuses_a_pinned_seed_subdir_verbatim():
    # Crash replay: the pre-crash layout wins over fresh sibling evidence.
    pinned = "Example.Show.S01E01.REPACK.WEB-DL.1080p.H.264-OTHER"
    layout = _decide(target_names=FLAT_NAMES, pinned_seed_subdir=pinned, racing_wrapper=EPISODE_TOP)
    assert layout.seed_subdir == pinned
    assert layout.wrapper == pinned
    assert layout.target_remote == join_remote(UNSORTED, pinned)


def test_pinned_subdir_is_not_reapplied_to_wrapped_or_multi_batch_rows():
    # Only the flat branch may reuse the pinned value; wrapped targets and
    # multi-batch moves keep their own top with no wrapper (matches the
    # original inline rule exactly).
    layout = _decide(target_names=WRAPPED_NAMES, pinned_seed_subdir="Stale.Top")
    assert layout.seed_subdir == EPISODE_TOP
    assert layout.wrapper == ""
    assert layout.target_remote == UNSORTED


def test_mixed_tops_without_common_prefix_seed_at_the_base():
    layout = _decide(target_names=["Top.A/one.mkv", "Top.B/two.mkv"])
    assert layout.seed_subdir == ""
    assert layout.wrapper == ""
    assert layout.target_remote == UNSORTED


def test_blob_derived_names_with_zero_batches():
    # The push feature's call shape: names straight from a .torrent blob,
    # batches_total=0, no SSD listing.
    names = [getattr(f, "name", "") for f in blob_files(WRAPPED_BLOB)]
    layout = _decide(target_names=names, batches_total=0)
    assert layout.seed_subdir == EPISODE_TOP
    assert layout.wrapper == ""
    assert layout.target_remote == UNSORTED


def test_empty_kind_routes_unsorted_like_the_move_stage():
    layout = _decide(kind="", target_names=WRAPPED_NAMES)
    assert layout.remote_base == UNSORTED
