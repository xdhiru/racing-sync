"""Refactor regression tests: content_keys + fuse_gate single rules."""

from __future__ import annotations

import pytest


def test_same_release_name_ignores_case_and_tags():
    from racing_sync.content_keys import same_release_name

    assert same_release_name("Show.S01 [FL]", "show.s01")
    assert same_release_name("Movie.2024.mkv", "movie.2024")
    assert not same_release_name("Show.S01", "Show.S02")


def test_same_content_size_rule_matches_unknown():
    from racing_sync.content_keys import same_content

    # Both known, equal -> same.
    assert same_content("Twin.Show", 1000, "twin.show", 1000)
    # Both known, differ -> different.
    assert not same_content("Twin.Show", 1000, "Twin.Show", 2000)
    # Either unknown -> match (fail toward dedup, like watch election).
    assert same_content("Twin.Show", 0, "Twin.Show", 1000)
    assert same_content("Twin.Show", 1000, "Twin.Show", 0)
    # Different names never match, even with unknown sizes.
    assert not same_content("A.Show", 0, "B.Show", 0)


def test_content_key_stable():
    from racing_sync.content_keys import content_key

    assert content_key("Show.S01 [A]", 100) == content_key("show.s01", 100)
    assert content_key("Show.S01", 100) != content_key("Show.S01", 200)


def test_fuse_gate_expected_none_when_no_blob():
    from racing_sync.fuse_gate import expected_files_from_blob

    assert expected_files_from_blob(None) is None
    assert expected_files_from_blob(b"") is None
    assert expected_files_from_blob(b"not-a-torrent") is None


@pytest.mark.anyio
async def test_intake_handoff_off_for_bare_doubles():
    """Bare test doubles never handoff (scheduler backstop covers them)."""
    import sys

    sys.path.insert(0, "tests")
    from conftest import make_coordinator
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    ts = TorrentState(source_infohash="a" * 40, source_name="Fresh",
                      state=State.NEW)
    assert await coord._drain_intake([ts], []) == 0


@pytest.mark.anyio
async def test_intake_handoff_spawns_fresh_new_when_started():
    """Production poller pass starts workers for fresh NEW arrivals."""
    import sys
    from unittest.mock import MagicMock

    sys.path.insert(0, "tests")
    from conftest import make_coordinator
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._coordinator_started = True
    coord._spawn_worker = MagicMock()
    fresh = TorrentState(source_infohash="b" * 40, source_name="Fresh",
                         state=State.NEW)
    parked = TorrentState(source_infohash="c" * 40, source_name="Parked",
                          state=State.WAITING_INDEXER)
    assert await coord._drain_intake([fresh, parked, None], []) == 1
    coord._spawn_worker.assert_called_once_with(fresh)


@pytest.mark.anyio
async def test_intake_handoff_skips_running_and_held():
    import sys
    from unittest.mock import MagicMock

    sys.path.insert(0, "tests")
    from conftest import make_coordinator
    from racing_sync.state import TorrentState, State

    coord = make_coordinator()
    coord._coordinator_started = True
    coord._spawn_worker = MagicMock()
    coord._running_infohashes.add("d" * 40)
    running = TorrentState(source_infohash="d" * 40, source_name="Run",
                           state=State.NEW)
    held = TorrentState(source_infohash="e" * 40, source_name="Held",
                        state=State.NEW, skipped=1)
    assert await coord._drain_intake([running, held], []) == 0
    coord._spawn_worker.assert_not_called()


@pytest.mark.anyio
async def test_fuse_gate_skipped_strict_equality(tmp_path):
    """Unknown-size wants only skip actual zero-byte files (legacy rule)."""
    from racing_sync.fuse_gate import skipped_present

    mount = tmp_path / "fuse"
    (mount / "d").mkdir(parents=True)
    (mount / "d" / "zero.mkv").write_bytes(b"")
    (mount / "d" / "real.mkv").write_bytes(b"x" * 10)

    # want=0 skips only the actual empty file, not real bytes.
    assert await skipped_present(mount, [("d/zero.mkv", 0)]) == {"d/zero.mkv"}
    assert await skipped_present(mount, [("d/real.mkv", 0)]) == set()
    # Exact size match skips.
    assert await skipped_present(mount, [("d/real.mkv", 10)]) == {"d/real.mkv"}
    # Size mismatch does not skip.
    assert await skipped_present(mount, [("d/real.mkv", 11)]) == set()
    # Missing file does not skip.
    assert await skipped_present(mount, [("d/nope.mkv", 5)]) == set()
