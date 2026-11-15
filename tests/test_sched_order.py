"""Scheduler order key + deterministic primary election."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

from racing_sync.coordinator import _elect_primary_torrent, _sched_priority
from racing_sync.state import State, TorrentState


def _row(h, when):
    ts = TorrentState(source_infohash=h, source_name="X", state=State.QUEUED)
    ts.updated_at = when
    return ts


def test_sched_priority_none_sorts_last():
    dated = _row("a" * 40, dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc))
    undated = _row("b" * 40, None)
    assert _sched_priority(dated) < _sched_priority(undated)


def test_sched_priority_mixed_naive_aware_chronological():
    naive_later = _row("a" * 40, dt.datetime(2020, 1, 2))  # assumed UTC
    aware_earlier = _row("b" * 40, dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc))
    assert _sched_priority(aware_earlier) < _sched_priority(naive_later)
    broken = _row("c" * 40, "not-a-date")
    assert _sched_priority(naive_later) < _sched_priority(broken)


def _t(h, name, size, trackers):
    return SimpleNamespace(infohash=h, name=name, size_bytes=size, trackers=trackers)


_PRIV = ["https://alpha.cc/announce/xyz"]
_PUB = ["udp://tracker.opentrackr.org:1337/announce"]


def test_elect_primary_prefers_public_regardless_of_order():
    group = [
        _t("b" * 40, "Show", 100, _PRIV),
        _t("a" * 40, "Show", 100, _PUB),
        _t("c" * 40, "Show", 100, _PRIV),
    ]
    assert _elect_primary_torrent(group).infohash == "a" * 40
    assert _elect_primary_torrent(list(reversed(group))).infohash == "a" * 40


def test_elect_primary_all_private_is_order_stable():
    group = [
        _t("c" * 40, "Show", 300, _PRIV),
        _t("a" * 40, "Show", 100, _PRIV),
        _t("b" * 40, "Show", 200, _PRIV),
    ]
    assert _elect_primary_torrent(group).infohash == "a" * 40
    assert _elect_primary_torrent(list(reversed(group))).infohash == "a" * 40
