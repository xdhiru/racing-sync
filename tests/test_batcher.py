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