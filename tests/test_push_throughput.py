"""A push's MEASURED speed is charged to the indexer wait before it collapses.

Fictional names only ("Example.Show.S01E01.1080p.WEB-DL-GROUP"), as everywhere
in this repo; no `*.log` or `debug-config.toml` is ever opened by these tests.

The wait already collapses under VPS1 disk pressure to "how long until VPS1
fills". That answers the wrong question on its own: the operator's rule is to
keep waiting for a cross-seed only while a push of THIS release could still
finish before the space is needed, so

    effective = max(floor, min(configured, runway − push_estimate − margin))

pinned here:

* the byte parser: `rclone lsjson --files-only -R` sizes, metadata only —
  nested paths, a name with commas and spaces, an empty dir, malformed JSON,
  and never an invented byte;
* the throughput estimate: a bounded EMA fed only by live pushes, seeded only
  from plausible samples, with a copy `push_liveness` judged dead rolled back,
  and UNKNOWN (None) until two valid samples exist;
* `push_seconds_estimate`: unknown size ⇒ unknown duration (never a small
  number), and the conservative fallback speed never reaches the deadline;
* the deadline term and its reason string, which names the push only when the
  push is what shortened the wait;
* `_prowlarr_timed_out` and `_park_for_indexer_retry` still agree at the same
  instant WITH the push term applied;
* the byte counters are durable checkpoint columns (v6 → v7) and the sweep
  records them without ever touching the FUSE mount or reading content.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync import coordinator_push
from racing_sync.config import AppConfig
from racing_sync.coordinator_push import PushMixin
from racing_sync.rclone_ops import RcloneResult
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.state_schema import SOURCE_PUSH_COLUMNS, PushState
from racing_sync.vps1_push import (
    PushFile,
    PushProgress,
    parse_remote_sizes,
    push_byte_totals,
    remote_progress,
)

ROOT = Path(__file__).resolve().parents[1]
GIB = 1024**3
MIB = 1024**2

SHOW = "Example.Show.S01E01.1080p.WEB-DL-GROUP"
FILE_ONE = "Example.Show/Season 01/Example.Show.S01E01.1080p.WEB-DL-GROUP.mkv"
# A name that would break any whitespace-separated parser: commas AND spaces.
FILE_TWO = "Example.Show/Season 01/Example.Show.S01E02, pt2 1080p WEB-DL-GROUP.mkv"
STRAY = "Other.Show/Season 03/Other.Show.S03E04.1080p.WEB-DL-GROUP.mkv"

WAIT = 28800
RETRY_GAP = 900
POLL = 120.0


def _lsjson(*entries: tuple[str, int, bool]) -> str:
    """A realistic `lsjson --files-only -R` document: one object per entry."""
    return json.dumps(
        [
            {
                "Path": path,
                "Name": path.rsplit("/", 1)[-1],
                "Size": size,
                "IsDir": is_dir,
                "MimeType": "" if is_dir else "video/x-matroska",
            }
            for path, size, is_dir in entries
        ]
    )


# --------------------------------------------------------------------------- #
# the parser: metadata only, never a byte invented
# --------------------------------------------------------------------------- #


def test_parser_reads_nested_and_punctuated_names():
    text = _lsjson(
        ("Example.Show", 0, True),
        (FILE_ONE, 4 * GIB, False),
        (FILE_TWO, 1536 * MIB, False),
        (STRAY, 9 * GIB, False),
    )
    sizes = parse_remote_sizes(text)
    assert sizes[FILE_ONE] == 4 * GIB
    assert sizes[FILE_TWO] == 1536 * MIB
    assert sizes[STRAY] == 9 * GIB
    # A directory is not an object with bytes; it never counts as landed.
    assert "Example.Show" not in sizes


def test_parser_survives_every_malformed_shape_without_inventing_bytes():
    assert parse_remote_sizes("") == {}
    assert parse_remote_sizes(None) == {}
    assert parse_remote_sizes("{ this is not json") == {}
    assert parse_remote_sizes("[]") == {}
    assert parse_remote_sizes('{"Path": "a", "Size": 1}') == {}
    assert parse_remote_sizes('[{"Path": "a"}, "b", 3, null]') == {}
    # An entry whose Size the remote did not publish is skipped, not zeroed:
    # a bogus 0 would drag a byte delta down and read as a copy going backwards.
    assert parse_remote_sizes('[{"Path": "a", "Size": null},{"Path":"b","Size":"x"}]') == {}
    assert parse_remote_sizes('[{"Path": "a", "Size": -5}]') == {}
    assert parse_remote_sizes('[{"Path": "a", "Size": true}]') == {}
    # `./` and a trailing separator normalize exactly like every other
    # listing comparison in the repo (`_norm_listed_path`, shared with the
    # move stage's `_remote_has_bytes` — the byte parser must not invent its
    # own path rules).
    assert parse_remote_sizes('[{"Path": "./A/B/c.mkv/", "Size": 7}]') == {"A/B/c.mkv": 7}


def test_byte_totals_land_only_wanted_bytes():
    sizes = parse_remote_sizes(_lsjson((FILE_ONE, 2048, False), (STRAY, 999999, False)))
    files = [PushFile(FILE_ONE, 4096), PushFile(FILE_TWO, 4096)]
    # Expected is the push's own obligation (both wanted files); landed is
    # what the remote can show for them — the stray 999999 B is not ours.
    assert push_byte_totals(files, sizes) == (2048, 8192)


def test_byte_totals_refuse_a_partial_expectation():
    """One file whose size the torrent never gave ⇒ the expectation is
    unknowable, so the pair is (0, 0): a partial sum would read as a push
    that is nearly done and shorten a deadline on nothing."""
    sizes = parse_remote_sizes(_lsjson((FILE_ONE, 2048, False)))
    assert push_byte_totals([PushFile(FILE_ONE, 4096), PushFile(FILE_TWO, 0)], sizes) == (0, 0)
    assert push_byte_totals([], sizes) == (0, 0)
    assert push_byte_totals([PushFile(FILE_ONE, 4096)], {}) == (0, 0)


def test_byte_totals_never_count_more_than_the_file_owns():
    """A remote that reports a bigger object than the torrent's file cannot
    push the landed total above the expected total."""
    sizes = parse_remote_sizes(_lsjson((FILE_ONE, 10 * GIB, False)))
    assert push_byte_totals([PushFile(FILE_ONE, 4096)], sizes) == (4096, 4096)


def test_push_progress_is_still_a_two_tuple_of_file_counts():
    """The completion check stayed name coverage: a byte-aware poll must not
    cost every existing `(present, total) = ...` caller a rewrite."""
    progress = PushProgress(3, 7, bytes_present=100, bytes_total=900)
    present, total = progress
    assert (present, total) == (3, 7)
    assert progress == (3, 7)
    assert (progress.bytes_present, progress.bytes_total) == (100, 900)
    # A poll with no byte answer carries zeros, and is equal to the old shape.
    assert PushProgress(0, 1) == (0, 1)


@pytest.mark.anyio
async def test_remote_poll_uses_one_metadata_listing(monkeypatch):
    seen: dict = {}

    async def _fake_run(cfg, cmd, **kwargs):
        seen["cmd"] = list(cmd)
        return RcloneResult(
            returncode=0, stdout=_lsjson((FILE_ONE, 1024, False)), stderr="", duration=0.1
        )

    monkeypatch.setattr("racing_sync.vps1_push.run_rclone", _fake_run)
    progress = await remote_progress(MagicMock(), "seedremote:TV", [PushFile(FILE_ONE, 4096)])
    assert tuple(progress) == (1, 1)
    assert (progress.bytes_present, progress.bytes_total) == (1024, 4096)
    assert seen["cmd"][1:] == ["lsjson", "--files-only", "-R", "seedremote:TV"]


# --------------------------------------------------------------------------- #
# the estimate: a bounded EMA of live pushes, unknown until measured twice
# --------------------------------------------------------------------------- #


class _Pusher(PushMixin):
    """The mixined estimator on its own, so the rules are tested in isolation."""

    def __init__(self, samples=None, poll=120.0) -> None:
        if samples is not None:
            self._push_throughput_bps = samples
        self._poll = poll

    def _push_poll_seconds(self) -> float:
        return self._poll


def _seed(owner) -> float | None:
    """The pending first sample, or None while nothing has been seeded.

    Read with `getattr` on purpose: an estimate that has never seen a sample
    has no attribute at all, and "no attribute" and "None" are the same answer
    to the only question that matters — is there a speed yet?
    """
    return getattr(owner, "_push_throughput_seed", None)


def test_first_sample_seeds_and_stays_unknown():
    p = _Pusher()
    assert p._measured_push_throughput_bps() is None
    p._note_push_throughput_sample("a" * 40, 10 * MIB, 10.0)
    # One poll is one lucky observation, not a speed the deadline may trust.
    assert p._measured_push_throughput_bps() is None
    assert _seed(p) == pytest.approx(1 * MIB)
    p._note_push_throughput_sample("a" * 40, 10 * MIB, 10.0)
    assert p._measured_push_throughput_bps() == pytest.approx(1 * MIB)
    assert _seed(p) is None


def test_zero_progress_polls_never_sample():
    p = _Pusher()
    p._note_push_throughput_sample("b" * 40, 0, 120.0)
    p._note_push_throughput_sample("b" * 40, -500, 120.0)
    p._note_push_throughput_sample("b" * 40, 10 * MIB, None)
    p._note_push_throughput_sample("b" * 40, 10 * MIB, 0.0)
    p._note_push_throughput_sample("b" * 40, 10 * MIB, -1.0)
    p._note_push_throughput_sample("b" * 40, float("nan"), 1.0)
    p._note_push_throughput_sample("b" * 40, 10 * MIB, float("inf"))
    assert p._measured_push_throughput_bps() is None
    assert _seed(p) is None


def test_estimate_is_bounded_and_smoothed():
    p = _Pusher()
    p._note_push_throughput_sample("c" * 40, 10 * MIB, 10.0)
    # A terabyte in a poll is a listing artefact, never a measured speed.
    p._note_push_throughput_sample("c" * 40, 1024**4, 1.0)
    speed = p._measured_push_throughput_bps()
    assert speed is not None
    assert speed <= coordinator_push._PUSH_THROUGHPUT_MAX_BPS
    # 0.7 * 1 MiB/s + 0.3 * cap: real history still dominates.
    assert speed == pytest.approx(0.7 * 1 * MIB + 0.3 * coordinator_push._PUSH_THROUGHPUT_MAX_BPS)


def test_a_dead_copies_sample_is_rolled_back_and_that_hash_is_remembered():
    p = _Pusher()
    h = "d" * 40
    p._note_push_throughput_sample(h, 10 * MIB, 10.0)
    p._note_push_throughput_sample(h, 30 * MIB, 10.0)
    assert p._measured_push_throughput_bps() is not None
    # `push_liveness` judged THIS copy dead: its sample must not have trained
    # the estimate (a usage-error copy runs at bytes-per-second noise).
    p._push_throughput_retract(h)
    assert p._measured_push_throughput_bps() is None
    assert _seed(p) == pytest.approx(1 * MIB)
    # And nothing else this checkpoint reports afterwards counts either.
    p._note_push_throughput_sample(h, 500 * MIB, 1.0)
    assert p._measured_push_throughput_bps() is None


def test_a_alive_launch_forgives_the_hash_again():
    p = _Pusher()
    h = "e" * 40
    p._push_throughput_retract(h)
    p._note_push_throughput_sample(h, 10 * MIB, 10.0)
    assert p._measured_push_throughput_bps() is None
    p._push_throughput_alive(h)
    p._note_push_throughput_sample(h, 10 * MIB, 10.0)
    p._note_push_throughput_sample(h, 10 * MIB, 10.0)
    assert p._measured_push_throughput_bps() == pytest.approx(1 * MIB)


def test_the_dead_hash_memory_is_bounded():
    p = _Pusher()
    for i in range(coordinator_push._PUSH_THROUGHPUT_DEAD_CAP + 500):
        p._push_throughput_retract(f"{i:040x}")
    assert len(p._push_throughput_dead) == coordinator_push._PUSH_THROUGHPUT_DEAD_CAP


def test_seconds_estimate_unknown_size_is_unknown_duration():
    p = _Pusher(samples=10 * MIB)
    assert p.push_seconds_estimate(0) is None
    assert p.push_seconds_estimate(None) is None
    assert p.push_seconds_estimate(-5) is None
    assert p.push_seconds_estimate("nonsense") is None
    # Never a small number for a row whose bytes nobody measured.
    assert p.push_seconds_estimate(100 * MIB) == pytest.approx(10.0)


def test_seconds_estimate_falls_back_slow_never_fast():
    """No measurement ⇒ the documented conservative (SLOW) speed.

    5 MiB/s is slower than any link this daemon has ever pushed over, so an
    unmeasured 100 MiB release is estimated at 20 s, never at the 1 s a
    measured fast link would give it. That is the direction that can only
    ever be too pessimistic — which is why the deadline is allowed to ignore
    it entirely rather than trust it.
    """
    p = _Pusher()
    assert p._measured_push_throughput_bps() is None
    estimate = p.push_seconds_estimate(100 * MIB)
    assert estimate == pytest.approx((100 * MIB) / coordinator_push._PUSH_THROUGHPUT_DEFAULT_BPS)
    assert estimate == pytest.approx(20.0)
    fast = _Pusher(samples=100 * MIB)
    assert fast.push_seconds_estimate(100 * MIB) == pytest.approx(1.0)
    assert estimate > fast.push_seconds_estimate(100 * MIB)


def test_deadline_term_refuses_an_unmeasured_speed():
    """The conservative fallback is for HUMAN estimates only; the deadline is
    charged only by a speed this daemon actually watched land."""
    p = _Pusher()
    assert p.push_term_for_deadline(4 * GIB) is None
    p._push_throughput_bps = 2 * MIB
    assert p.push_term_for_deadline(0) is None
    assert p.push_term_for_deadline(None) is None
    needed, margin, speed = p.push_term_for_deadline(1 * GIB)
    assert needed == pytest.approx(512.0)
    assert margin == pytest.approx(300.0)
    assert speed == pytest.approx(2 * MIB)


def test_deadline_margin_is_the_poll_cadence_with_a_floor():
    assert _Pusher(poll=120.0).push_deadline_margin_seconds() == pytest.approx(300.0)
    assert _Pusher(poll=600.0).push_deadline_margin_seconds() == pytest.approx(1200.0)

    class _Broken(_Pusher):
        def _push_poll_seconds(self) -> float:
            raise RuntimeError("no cadence")

    # An unreadable cadence means NO margin, which means no push term at all:
    # the deadline fails closed toward today, never toward a shorter wait.
    assert _Broken().push_deadline_margin_seconds() is None
    assert _Broken(samples=2 * MIB).push_term_for_deadline(1 * GIB) is None


# --------------------------------------------------------------------------- #
# the wait: runway minus the push, or exactly the projection-only rule
# --------------------------------------------------------------------------- #

# The urgent-box setup from tests/test_indexer_wait_collapse.py: one arrival of
# 20 GiB/h against 40 GiB free projects 7200 s, and `urgent` alone is pressure.
PROJECTION = 7200.0


def _cfg(*, wait: int = WAIT, retry_gap: int = RETRY_GAP) -> AppConfig:
    cfg = AppConfig.from_toml(ROOT / "config.example.toml")
    cfg.cross_seed.indexer_wait_seconds = wait
    cfg.cross_seed.prowlarr_retry_interval_seconds = retry_gap
    cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = True
    cfg.cleanup.low_watermark_free_bytes = 0
    cfg.cleanup.fill_lookahead_hours = 2.0
    cfg.cleanup.burst_arrivals_per_hour = 8.0
    return cfg


def _coord(store: StateStore, cfg: AppConfig, *, speed: float | None, poll: float = POLL):
    """A coordinator under real janitor pressure with a push speed to charge.

    `_cleanup_pressure` is NOT mocked, and `_source_free_bytes` explodes if
    called: the push term rides the janitor's cached probe exactly like the
    projection it shortens.
    """
    coord = make_coordinator(store)
    coord.cfg = cfg
    now = time.monotonic()
    coord._last_source_free_bytes = 40 * GIB
    coord._arrival_bytes = [(now, 20 * GIB)]
    coord._arrival_times = [now]
    coord._cleanup_urgent = True
    coord._source_free_bytes = MagicMock(side_effect=AssertionError("must not probe VPS1"))
    coord._push_is_live = MagicMock(return_value=False)
    coord._push_poll_seconds = MagicMock(return_value=poll)
    if speed is not None:
        coord._push_throughput_bps = speed
    return coord


def _row(store: StateStore, h: str, elapsed_s: float, *, size: int = 0) -> TorrentState:
    now = dt.datetime.now(dt.UTC)
    store.upsert(
        TorrentState(
            source_infohash=h,
            source_name=SHOW,
            total_bytes=size,
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=now - dt.timedelta(seconds=elapsed_s),
            indexer_attempts=max(1, int(elapsed_s // RETRY_GAP)),
            indexer_next_retry_at=now,
        )
    )
    return store.get(h)


def _projected_seconds(coord) -> float | None:
    """The janitor's own projection, read straight from `_cleanup_pressure`."""
    return coord._cleanup_pressure(coord.rt.cleanup.config, coord._last_source_free_bytes)[2]


def _projection_only(coord, ts) -> float:
    """What the rule produced before the push term existed, recomputed here."""
    _a, _r, projected, urgent, bursting = coord._cleanup_pressure(
        coord.rt.cleanup.config, coord._last_source_free_bytes
    )
    assert bursting or urgent
    assert projected is not None
    floor = max(600.0, 2.0 * coord._indexer_retry_gap_seconds())
    return max(floor, min(coord._indexer_configured_wait_seconds(), float(projected)))


def test_the_projection_is_the_7200s_this_file_is_built_on(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=None)
        assert _projection_only(coord, _row(store, "a" * 40, 600)) == pytest.approx(PROJECTION)
    finally:
        store.close()


def test_unknown_speed_means_no_extra_collapse(tmp_path):
    """THE fail-closed test: nothing measured ⇒ the wait is byte-for-byte what
    the projection-only rule produced, and the reason says nothing about a
    push that was never timed."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=None)
        row = _row(store, "b" * 40, 600, size=4 * GIB)
        wait_s, reason = coord._indexer_effective_wait_seconds(row)
        assert wait_s == pytest.approx(PROJECTION)
        assert "push" not in reason
        # A coordinator without the push lane at all (a bare double) agrees.
        bare = _coord(store, _cfg(), speed=None)
        bare.push_term_for_deadline = None  # type: ignore[assignment]
        assert bare._indexer_effective_wait_seconds(row) == (wait_s, reason)
    finally:
        store.close()


def test_unknown_row_size_means_no_push_term(tmp_path):
    """A measured speed cannot price a release whose bytes nobody knows."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        wait_s, reason = coord._indexer_effective_wait_seconds(_row(store, "c" * 40, 600))
        assert wait_s == pytest.approx(PROJECTION)
        assert "push" not in reason
    finally:
        store.close()


def test_known_speed_shortens_the_wait_by_the_push_plus_margin(tmp_path):
    """1 GiB at 2 MiB/s is 512 s of copy, plus 300 s of poll margin off a
    7200 s runway."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        row = _row(store, "d" * 40, 600, size=1 * GIB)
        wait_s, reason = coord._indexer_effective_wait_seconds(row)
        assert wait_s == pytest.approx(PROJECTION - 512.0 - 300.0)
        assert "push" in reason
        assert "512s to push 1073741824 B at 2097152 B/s" in reason
        assert "300s poll margin" in reason
        assert "projected to fill in 7200s" in reason
        # It can only ever SHORTEN: never above the configured window.
        assert wait_s < PROJECTION < WAIT
    finally:
        store.close()


def test_a_push_that_cannot_finish_in_time_hits_the_floor(tmp_path):
    """20 GiB at 2 MiB/s is 10240 s — longer than the whole runway. The wait
    goes to the floor and never below it, and never to zero."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        row = _row(store, "e" * 40, 600, size=20 * GIB)
        wait_s, reason = coord._indexer_effective_wait_seconds(row)
        assert wait_s == max(600.0, 2 * RETRY_GAP) == 1800.0
        assert "push" in reason
    finally:
        store.close()


def test_a_push_with_time_to_spare_changes_nothing(tmp_path):
    """A long runway minus a short push is still a long runway.

    1000 GiB free against 20 GiB/h projects 50 h — far past the 8 h window —
    so charging 512 s of copy plus 300 s of margin to it leaves the window
    whole and the reason empty, exactly like a calm box.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        coord._last_source_free_bytes = 1000 * GIB
        coord._arrival_bytes = [(time.monotonic(), 20 * GIB)]
        assert _projected_seconds(coord) == pytest.approx(180000.0)
        assert _projection_only(coord, _row(store, "f0" * 20, 600)) == float(WAIT)
        row = _row(store, "f" * 40, 600, size=1 * GIB)
        assert coord._indexer_effective_wait_seconds(row) == (float(WAIT), "")
    finally:
        store.close()


def test_the_margin_follows_the_push_cadence(tmp_path):
    """No new config key: the slack is two polls of the push's own clock."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB, poll=900.0)
        row = _row(store, "g" * 40, 600, size=1 * GIB)
        wait_s, reason = coord._indexer_effective_wait_seconds(row)
        assert wait_s == pytest.approx(PROJECTION - 512.0 - 1800.0)
        assert "1800s poll margin" in reason
    finally:
        store.close()


def test_trigger_and_escalation_agree_with_the_push_term(tmp_path):
    """The existing no-drift guarantee, extended: one row, two readers, one
    verdict — WITH the push charged.

    The window is 7200 − 512 − 300 = 6388 s. A 6500 s-old row is past it for
    BOTH the racing-torrent/push trigger and the FAILED escalation; a 6000 s
    row is inside it for both. Mocking the pressure keeps both reads on the
    same instant.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        window = PROJECTION - 512.0 - 300.0
        coord._cleanup_pressure = MagicMock(return_value=(10.0, 10 * GIB, PROJECTION, False, True))

        stale = _row(store, "h" * 40, 6500, size=1 * GIB)
        assert coord._prowlarr_timed_out(stale) is True
        coord._park_for_indexer_retry(stale, reason="indexer miss")
        failed = store.get("h" * 40)
        assert failed.state == State.FAILED
        assert "push" in failed.last_error

        fresh = _row(store, "i" * 40, int(window) - 400, size=1 * GIB)
        assert coord._prowlarr_timed_out(fresh) is False
        coord._park_for_indexer_retry(fresh, reason="indexer miss")
        row = store.get("i" * 40)
        assert row.state == State.WAITING_INDEXER
        assert row.indexer_next_retry_at > dt.datetime.now(dt.UTC)
    finally:
        store.close()


def test_a_live_push_still_rearms_however_short_the_push_term_made_it(tmp_path):
    """The escalation suppression is untouched by the new term: bytes that are
    mid-copy never get their row FAILED for the disk they are freeing."""
    store = StateStore(tmp_path / "state.db")
    try:
        coord = _coord(store, _cfg(), speed=2 * MIB)
        coord._push_is_live = MagicMock(return_value=True)
        row = _row(store, "j" * 40, 6500, size=20 * GIB)
        before = row.indexer_first_queried_at
        coord._park_for_indexer_retry(row, reason="indexer miss")
        after = store.get("j" * 40)
        assert after.state == State.WAITING_INDEXER
        assert after.indexer_first_queried_at == before
        assert after.indexer_next_retry_at > dt.datetime.now(dt.UTC)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the sweep: bytes recorded, one metadata call, the mount never touched
# --------------------------------------------------------------------------- #

HASH = "ab" * 20
TARGET = "seedremote:Unsorted"
MOUNT = "/mnt/seed"


def _listing(sizes: list[tuple[str, int]]) -> str:
    """`lsjson --files-only -R` for exactly these landed objects."""
    return _lsjson(*[(path, size, False) for path, size in sizes])


def _age_checkpoint(store: StateStore, *, landed: int, age_s: float) -> None:
    """Say "this much had landed `age_s` ago" about the existing live push.

    The denominator of a byte delta is the stamp the PREVIOUS poll wrote, not
    a wall-clock guess, so a test of two polls has to age it deliberately.
    """
    assert store.note_push_progress(HASH, bytes_present=landed) is True
    stamp = dt.datetime.now(dt.UTC) - dt.timedelta(seconds=age_s)
    with store._lock:
        store._conn.execute(
            "UPDATE source_push SET updated_at = ? WHERE infohash = ?",
            (stamp.isoformat(), HASH),
        )


def _old_checkpoint(store: StateStore, *, landed: int, expected: int, age_s: float) -> None:
    assert (
        store.claim_push(
            HASH,
            src_dir="/data/racing",
            target_remote=TARGET,
            files_total=2,
            bytes_total=expected,
            deadline_at=dt.datetime.now(dt.UTC) + dt.timedelta(hours=6),
        )
        is not None
    )
    _age_checkpoint(store, landed=landed, age_s=age_s)


def _listing_writer(sizes: list[tuple[str, int]]):
    """A `run_rclone` double that answers one fixed `lsjson` listing."""
    payload = _listing(sizes)

    async def _fake(cfg, cmd, **kwargs):
        return RcloneResult(returncode=0, stdout=payload, stderr="", duration=0.1)

    return _fake


def _sweep_setup(store: StateStore, sizes: list[tuple[str, int]]):
    """A coordinator sweeping one live push against a fake `run_rclone`.

    Only the subprocess seam is patched, so the real `remote_progress`, the
    real `PushProgress` and the real `PushMixin` bookkeeping all run; the fuse
    mount and VPS2's SSD are not reachable from any of them.
    """
    calls: list[list[str]] = []

    async def _fake_run(cfg, cmd, **kwargs):
        calls.append(list(cmd))
        return RcloneResult(returncode=0, stdout=_listing(sizes), stderr="", duration=0.1)

    coord = make_coordinator(store)
    coord._push_switch_on = MagicMock(return_value=True)
    coord._push_due = MagicMock(return_value=True)
    coord._push_wanted_files = AsyncMock(
        return_value=[PushFile(FILE_ONE, 4 * GIB), PushFile(FILE_TWO, 2 * GIB)]
    )
    coord._push_liveness = AsyncMock()
    coord._fuse_content_evidence = AsyncMock(side_effect=AssertionError("no mount reads"))
    return coord, calls, _fake_run


def _waiting_row(store: StateStore, size: int) -> None:
    store.upsert(
        TorrentState(
            source_infohash=HASH,
            source_name=SHOW,
            total_bytes=size,
            state=State.WAITING_INDEXER,
        )
    )


@pytest.mark.anyio
async def test_sweep_records_bytes_with_one_metadata_call(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.db")
    try:
        _old_checkpoint(store, landed=1 * GIB, expected=6 * GIB, age_s=600.0)
        _waiting_row(store, 6 * GIB)
        coord, calls, fake = _sweep_setup(store, [(FILE_ONE, 2 * GIB)])
        monkeypatch.setattr("racing_sync.vps1_push.run_rclone", fake)

        await coord.push_sweep()

        push = store.get_push(HASH)
        assert push is not None
        # Names stay name coverage; the sizes are the new half of the record.
        assert (push.files_present, push.files_total) == (1, 2)
        assert (push.bytes_present, push.bytes_total) == (2 * GIB, 6 * GIB)
        assert push.state is PushState.RUNNING

        # Exactly ONE remote listing per push per poll, and it is the metadata
        # one: `lsjson --files-only -R <remote:dir>`.
        assert len(calls) == 1
        assert calls[0][1:] == ["lsjson", "--files-only", "-R", TARGET]
        joined = " ".join(calls[0])
        # Never the FUSE mount, never VPS2's SSD, never a content read or hash.
        assert MOUNT not in joined
        for forbidden in ("hashsum", "md5sum", "checkfile", "--checksum", "--download"):
            assert forbidden not in joined

        # One delta is a SEED, not a speed: 1 GiB landed over the 600 s the
        # checkpoint had been sitting since its last note.
        assert coord._measured_push_throughput_bps() is None
        assert _seed(coord) == pytest.approx(1 * GIB / 600.0, rel=1e-3)
        # So the wait still refuses to charge a push for this row.
        assert coord.push_term_for_deadline(6 * GIB) is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_second_poll_turns_two_samples_into_a_speed(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.db")
    try:
        _old_checkpoint(store, landed=1 * GIB, expected=6 * GIB, age_s=600.0)
        _waiting_row(store, 6 * GIB)
        coord, _calls, fake = _sweep_setup(store, [(FILE_ONE, 2 * GIB)])
        monkeypatch.setattr("racing_sync.vps1_push.run_rclone", fake)
        await coord.push_sweep()
        assert coord._measured_push_throughput_bps() is None

        # Poll two: 2 GiB more landed over 300 s.
        _age_checkpoint(store, landed=2 * GIB, age_s=300.0)
        coord._push_handoff = AsyncMock()
        monkeypatch.setattr(
            "racing_sync.vps1_push.run_rclone",
            _listing_writer([(FILE_ONE, 3 * GIB), (FILE_TWO, 1 * GIB)]),
        )
        await coord.push_sweep()

        seed = 1 * GIB / 600.0
        sample = (4 * GIB - 2 * GIB) / 300.0
        assert coord._measured_push_throughput_bps() == pytest.approx(
            seed * (1 - coordinator_push._PUSH_THROUGHPUT_ALPHA)
            + sample * coordinator_push._PUSH_THROUGHPUT_ALPHA,
            rel=1e-3,
        )
        assert _seed(coord) is None
        assert store.get_push(HASH).bytes_present == 4 * GIB
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_landed_byte_advance_skips_the_liveness_read(tmp_path, monkeypatch):
    """The single-file release that used to read `0/1` for hours.

    Names did not advance this poll, but bytes did — that IS proof of life, so
    the log probe is not consulted and no dead verdict can be built on a poll
    that watched a gigabyte land.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        _old_checkpoint(store, landed=1 * GIB, expected=6 * GIB, age_s=600.0)
        _waiting_row(store, 6 * GIB)
        coord, _calls, fake = _sweep_setup(store, [(FILE_ONE, 2 * GIB)])
        monkeypatch.setattr("racing_sync.vps1_push.run_rclone", fake)
        await coord.push_sweep()
        coord._push_liveness.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_copy_judged_dead_never_taught_the_estimate(tmp_path, monkeypatch):
    """Poll two lands nothing and the job's own log says it died.

    The sample poll one folded in is rolled back, so a usage-error copy that
    reported a few bytes before it quit cannot become this daemon's opinion of
    the link speed — and therefore cannot shorten anybody's wait. The hash
    stays remembered until a fresh launch forgives it.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        _old_checkpoint(store, landed=1 * GIB, expected=6 * GIB, age_s=600.0)
        _waiting_row(store, 6 * GIB)
        coord, _calls, fake = _sweep_setup(store, [(FILE_ONE, 2 * GIB)])
        monkeypatch.setattr("racing_sync.vps1_push.run_rclone", fake)
        await coord.push_sweep()
        assert _seed(coord) is not None

        def _die(_store, _push, infohash, _now, *, present):
            # What the real `_push_liveness` does on a `failed` verdict.
            coord._push_throughput_retract(infohash)
            store.finish_push(infohash, PushState.FAILED, last_error="Error: unknown flag")

        _age_checkpoint(store, landed=2 * GIB, age_s=300.0)
        coord._push_liveness = AsyncMock(side_effect=_die)
        await coord.push_sweep()
        assert _seed(coord) is None
        assert coord._measured_push_throughput_bps() is None
        # ...and no later delta from that dead checkpoint counts either.
        coord._note_push_throughput_sample(HASH, 400 * MIB, 1.0)
        assert _seed(coord) is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_keeps_the_last_byte_reading_when_the_poll_has_none(tmp_path, monkeypatch):
    """A 2-tuple poll (an older caller, a double) must not zero the counters.

    `note_push_progress(..., bytes_present=None)` leaves the stored reading:
    "no observation" and "zero landed" are different facts, and the estimate
    may only ever move on the first one.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        _old_checkpoint(store, landed=1 * GIB, expected=6 * GIB, age_s=600.0)
        _waiting_row(store, 6 * GIB)
        coord, _calls, _fake = _sweep_setup(store, [])
        monkeypatch.setattr(coordinator_push, "remote_progress", AsyncMock(return_value=(1, 2)))
        await coord.push_sweep()
        push = store.get_push(HASH)
        assert push is not None
        assert push.bytes_present == 1 * GIB
        assert push.bytes_total == 6 * GIB
        assert _seed(coord) is None
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the checkpoint columns themselves (schema v7)
# --------------------------------------------------------------------------- #


def test_the_byte_columns_are_part_of_the_single_spec():
    from racing_sync.state_schema import _SOURCE_PUSH_COLUMN_SPECS

    assert {"bytes_present", "bytes_total"} <= set(SOURCE_PUSH_COLUMNS)
    # One spec tuple drives DDL, to_row() and the column diff: nothing to drift.
    for name, ddl in _SOURCE_PUSH_COLUMN_SPECS:
        if name in ("bytes_present", "bytes_total"):
            assert ddl.upper() == "INTEGER NOT NULL DEFAULT 0", name
