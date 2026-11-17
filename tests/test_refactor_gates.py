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
    # Both unknown still match for grouping/dedup...
    assert same_content("Twin.Show", 0, "Twin.Show", 0)
    # ...but deferral decisions require a known size, or distinct
    # unsized releases mutually defer forever.
    assert not same_content("Twin.Show", 0, "Twin.Show", 0, require_size=True)
    assert same_content("Twin.Show", 0, "Twin.Show", 1000, require_size=True)
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

    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    ts = TorrentState(source_infohash="a" * 40, source_name="Fresh", state=State.NEW)
    assert await coord._drain_intake([ts], []) == 0


@pytest.mark.anyio
async def test_intake_handoff_spawns_fresh_new_when_started():
    """Production poller pass starts workers for fresh NEW arrivals."""
    import sys
    from unittest.mock import MagicMock

    sys.path.insert(0, "tests")
    from conftest import make_coordinator

    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord._coordinator_started = True
    coord._spawn_worker = MagicMock()
    fresh = TorrentState(source_infohash="b" * 40, source_name="Fresh", state=State.NEW)
    parked = TorrentState(
        source_infohash="c" * 40, source_name="Parked", state=State.WAITING_INDEXER
    )
    assert await coord._drain_intake([fresh, parked, None], []) == 1
    coord._spawn_worker.assert_called_once_with(fresh)


@pytest.mark.anyio
async def test_intake_handoff_skips_running_and_held():
    import sys
    from unittest.mock import MagicMock

    sys.path.insert(0, "tests")
    from conftest import make_coordinator

    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    coord._coordinator_started = True
    coord._spawn_worker = MagicMock()
    coord._running_infohashes.add("d" * 40)
    running = TorrentState(source_infohash="d" * 40, source_name="Run", state=State.NEW)
    held = TorrentState(source_infohash="e" * 40, source_name="Held", state=State.NEW, skipped=1)
    assert await coord._drain_intake([running, held], []) == 0
    coord._spawn_worker.assert_not_called()


@pytest.mark.anyio
async def test_intake_pause_holds_new_and_resumes():
    """Paused intake starts nothing; resume restores the handoff."""
    import sys
    from unittest.mock import MagicMock

    sys.path.insert(0, "tests")
    from conftest import make_coordinator

    from racing_sync.state import State, TorrentState

    coord = make_coordinator()
    assert coord._intake_paused is False
    coord._coordinator_started = True
    coord._spawn_worker = MagicMock()
    fresh = TorrentState(source_infohash="f" * 40, source_name="Fresh", state=State.NEW)
    assert coord.set_intake_paused(True) is True
    assert await coord._drain_intake([fresh], []) == 0
    coord._spawn_worker.assert_not_called()
    assert coord.set_intake_paused(False) is False
    assert await coord._drain_intake([fresh], []) == 1
    coord._spawn_worker.assert_called_once_with(fresh)


@pytest.mark.anyio
async def test_schedule_step_paused_intake_parks_new(tmp_path):
    """Paused scheduler starts no workers for NEW rows."""
    import sys

    sys.path.insert(0, "tests")
    from conftest import make_coordinator

    from racing_sync.state import State, StateStore, TorrentState

    store = StateStore(tmp_path / "paused.db")
    try:
        coord = make_coordinator(store)
        coord.set_intake_paused(True)
        store.upsert(TorrentState(source_infohash="a" * 40, source_name="New", state=State.NEW))
        await coord._schedule_step()
        assert "a" * 40 not in coord._running_infohashes
        assert len(coord._tasks) == 0
    finally:
        for t in list(getattr(coord, "_tasks", []) or []):
            try:
                t.cancel()
            except Exception:
                pass
        store.close()


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


@pytest.mark.anyio
async def test_fuse_gate_reports_unmounted_mount(tmp_path, monkeypatch):
    """An empty non-mountpoint dir is "unavailable", not "content missing".

    The stat on an unmounted mountpoint succeeds, so without the mount
    probe every expected file reads missing and the healer can re-download
    bytes that are present on the remote.
    """
    from racing_sync import fuse_gate
    from racing_sync.fuse_gate import missing_under

    mount = tmp_path / "fuse"
    mount.mkdir()
    monkeypatch.setattr(fuse_gate, "_POSIX", True)
    monkeypatch.setattr(fuse_gate.os.path, "ismount", lambda _p: False)

    got = await missing_under(mount, [("Show/a.mkv", 10)])
    assert got == [f"<mount unavailable: {mount}>"]


@pytest.mark.anyio
async def test_fuse_gate_trusts_a_populated_directory(tmp_path, monkeypatch):
    """A directory holding entries is a working tree even off-mountpoint."""
    from racing_sync import fuse_gate
    from racing_sync.fuse_gate import missing_under

    mount = tmp_path / "fuse"
    (mount / "Show").mkdir(parents=True)
    (mount / "Show" / "a.mkv").write_bytes(b"x" * 10)
    monkeypatch.setattr(fuse_gate, "_POSIX", True)
    monkeypatch.setattr(fuse_gate.os.path, "ismount", lambda _p: False)

    assert await missing_under(mount, [("Show/a.mkv", 10)]) == []


def test_title_normalizer_is_single_implementation():
    """Prowlarr matching delegates to the one coordinator normalizer."""
    from racing_sync.coordinator_content import normalize_content_name
    from racing_sync.prowlarr import _norm_title_for_match

    titles = [
        "Show.S01E01.1080p",
        "Show.S01E01.1080p.mkv",
        "Show.S01E01.1080p [A1B2C3D4]",
        "Show.S01E01.1080p [A] [B].mkv.torrent",
        "Movie.2024.mp4.torrent",
        "Example Outpost S04E08",
        "Example.Outpost.S04E08",
    ]
    for t in titles:
        assert _norm_title_for_match(t) == normalize_content_name(t, unify_separators=True)
    # Unification only affects separators; grouping default stays strict.
    assert normalize_content_name("Show.S01") != normalize_content_name("Show S01")
    assert normalize_content_name("Show.S01", unify_separators=True) == normalize_content_name(
        "Show S01", unify_separators=True
    )


def test_size_tolerance_per_kind():
    """Series packs tolerate repack-scale diffs; movies keep the tight rule."""
    from racing_sync.coordinator_content import size_within_tolerance

    target = 10_000_000_000
    repack_diff = 60_000_000  # 60 MiB repack shift
    # Legacy default unchanged.
    assert size_within_tolerance(target, target + repack_diff) is False
    assert size_within_tolerance(target, target + repack_diff, kind=None) is False
    assert size_within_tolerance(target, target + repack_diff, kind="movie") is False
    assert size_within_tolerance(target, target + repack_diff, kind="unknown") is False
    # Series kinds admit it.
    for kind in ("episode", "season", "mixed"):
        assert size_within_tolerance(target, target + repack_diff, kind=kind) is True
    # ...but not anything: 500 MiB still parks everywhere.
    for kind in (None, "movie", "episode", "season", "mixed", "unknown"):
        assert size_within_tolerance(target, target + 500_000_000, kind=kind) is False
    # Unknown sizes still pass.
    assert size_within_tolerance(0, target, kind="movie") is True
    assert size_within_tolerance(target, 0, kind="episode") is True


def test_matches_release_kind_and_separators():
    """One predicate: kind window for supplements, unify opt-in for recall."""
    from racing_sync.coordinator_content import _matches_release

    # Punctuation variants need the opt-in.
    assert not _matches_release("Show.S01", 1000, "Show S01", 1000)
    assert _matches_release("Show.S01", 1000, "Show S01", 1000, unify_separators=True)
    # Repack-scale episode diffs admit with kind, park without.
    target = 10_000_000_000
    assert not _matches_release("Show.S01", target, "Show.S01", target + 60_000_000)
    assert _matches_release("Show.S01", target, "Show.S01", target + 60_000_000, kind="episode")
    # Release groups still discriminate.
    assert not _matches_release(
        "Show.S01-GRP1", target, "Show.S01-GRP2", target, kind="episode", unify_separators=True
    )
