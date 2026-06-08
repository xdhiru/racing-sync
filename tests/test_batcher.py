from __future__ import annotations

import pytest

from racing_sync.batcher import make_batches
from racing_sync.classifier import Episode


def test_batches_fit_under_cap():
    eps = [
        Episode(f"S01E{i:02d}.mkv", 1, i, 2_000_000_000)
        for i in range(1, 11)  # 10 eps of 2 GiB
    ]
    cap = 6_000_000_000  # 6 GiB cap
    batches = make_batches(eps, cap_bytes=cap)
    for b in batches:
        assert b.size_bytes <= cap
    # 2 GiB x 10 = 20 GiB, cap 6 GiB -> should be at least 4 batches
    assert len(batches) >= 4
    # All episodes preserved
    assert sum(len(b.episodes) for b in batches) == 10


def test_single_oversize_episode_becomes_own_batch():
    big = Episode("S01E01.mkv", 1, 1, 10_000_000_000)
    small = Episode("S01E02.mkv", 1, 2, 1_000_000_000)
    batches = make_batches([big, small], cap_bytes=5_000_000_000)
    # big > cap so it gets its own batch
    assert any(b.size_bytes > 5_000_000_000 for b in batches)
    # small fits with itself
    assert any(len(b.episodes) == 1 and b.size_bytes == 1_000_000_000 for b in batches)


def test_batches_in_order():
    eps = [Episode(f"S01E{i:02d}.mkv", 1, i, 1_000_000_000) for i in range(1, 6)]
    batches = make_batches(eps, cap_bytes=3_000_000_000)
    seq: list[int] = []
    for b in batches:
        for e in b.episodes:
            seq.append(e.episode)
    assert seq == [1, 2, 3, 4, 5]


def test_include_patterns_are_per_file():
    eps = [Episode("S01E01.mkv", 1, 1, 1), Episode("S01E02.mkv", 1, 2, 1)]
    b = make_batches(eps, cap_bytes=10)[0]
    pats = b.include_patterns()
    assert pats == ["--include=S01E01.mkv", "--include=S01E02.mkv"]


def test_include_patterns_escapes_glob_metacharacters():
    eps = [
        Episode("[DummySub] Show [1080p].mkv", 1, 1, 1),
        Episode("Show?Part{1}*test.mkv", 1, 2, 1),
    ]
    b = make_batches(eps, cap_bytes=10)[0]
    pats = b.include_patterns()
    assert pats == [
        r"--include=\[DummySub\] Show \[1080p\].mkv",
        r"--include=Show\?Part\{1\}\*test.mkv",
    ]


@pytest.mark.anyio
async def test_do_downloading_iterates_batches():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.state import TorrentState, State

    coord = object.__new__(Coordinator)
    coord._stop = False
    coord._live = {}
    coord.store = MagicMock()
    coord._wait_for_completion = AsyncMock()
    coord._prepare_next_batch = AsyncMock()
    coord.transition = MagicMock(side_effect=lambda ts, s: setattr(ts, "state", s))

    ts = TorrentState(
        source_infohash="testhash",
        source_name="Test.Show.S01",
        classification_kind="season",
        batches_total=3,
        batch_index=0,
        state=State.DOWNLOADING,
    )

    await coord._do_downloading(ts)

    # _wait_for_completion called 3 times (once per batch)
    assert coord._wait_for_completion.await_count == 3
    # _prepare_next_batch called 2 times (for batch 1 and 2)
    assert coord._prepare_next_batch.await_count == 2
    assert ts.batch_index == 3
    assert ts.state == State.MOVING
    assert coord.transition.called