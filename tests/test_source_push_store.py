"""source_push (schema v6): durable "push from VPS1" checkpoints.

Covers fresh-DB creation, the v5 -> v6 upgrade for a database written before
the table existed, the atomic claim/dedupe primitive, the reset semantics of a
fresh operator tap, the progress counters, and the single expiry rule behind
``push_is_live``. All data below is fictional.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from racing_sync.state import (
    _SCHEMA_VERSION,
    _SOURCE_PUSH_COLUMN_SPECS,
    _TORRENT_STATE_COLUMN_SPECS,
    SOURCE_PUSH_COLUMNS,
    PushState,
    SourcePush,
    StateStore,
)

HASH = "d" * 40
OTHER = "e" * 40
STAMP = "2026-01-01T00:00:00+00:00"

PUSH = {
    "group_key": "fake-show-s01e01",
    "content_name": "Fake Show S01E01 1080p FKEGRP",
    "src_dir": "/downloads/fake-show-s01e01",
    "target_remote": "seedremote2:Fake Show (2026) [1080p FKEGRP]",
    "list_path": "/tmp/fake-push-files-from-raw.txt",
    "log_path": "/tmp/fake-push-rclone.log",
}


def _deadline(hours: int = 6) -> dt.datetime:
    return dt.datetime.now(dt.UTC) + dt.timedelta(hours=hours)


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


def test_fresh_db_creates_the_source_push_table(tmp_path: Path):
    from racing_sync.state import TORRENT_STATE_COLUMNS

    store = StateStore(tmp_path / "state.db")
    try:
        tables = {
            row[0]
            for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "source_push" in tables
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)

        cols = [row[1] for row in store._conn.execute("PRAGMA table_info(source_push)").fetchall()]
        # One spec tuple drives the DDL: table order == column order.
        assert cols == list(SOURCE_PUSH_COLUMNS)
        assert tuple(name for name, _ddl in _SOURCE_PUSH_COLUMN_SPECS) == SOURCE_PUSH_COLUMNS
        # The new table never disturbs the existing ones.
        state_cols = [
            row[1] for row in store._conn.execute("PRAGMA table_info(torrent_state)").fetchall()
        ]
        assert state_cols == list(TORRENT_STATE_COLUMNS)

        indexes = {
            row[0]
            for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "ix_push_state" in indexes
    finally:
        store.close()


def test_v5_db_upgrades_and_gains_source_push(tmp_path: Path):
    """A database written before the table existed gets it on the next open."""
    db = tmp_path / "v5-state.db"
    conn = sqlite3.connect(str(db))
    try:
        body = ",\n    ".join(f"{name} {ddl}" for name, ddl in _TORRENT_STATE_COLUMN_SPECS)
        conn.execute(f"CREATE TABLE torrent_state (\n    {body}\n);")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        conn.execute("CREATE TABLE ignored_torrents (source_infohash TEXT PRIMARY KEY);")
        conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '5')")
        conn.execute(
            "INSERT INTO torrent_state "
            "(source_infohash, source_name, state, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (HASH, "Legacy Row", "done", STAMP, STAMP),
        )
        conn.commit()
    finally:
        conn.close()

    store = StateStore(db)
    try:
        tables = {
            row[0]
            for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "source_push" in tables
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)

        # The pre-existing row survived the upgrade untouched.
        legacy = store.get(HASH, include_blob=False)
        assert legacy is not None
        assert legacy.source_name == "Legacy Row"

        # And the new table is immediately usable.
        push = store.start_push(OTHER, **PUSH, deadline_at=_deadline())
        assert push is not None
        assert store.get_push(OTHER) is not None
    finally:
        store.close()

    # Reopening an already-v6 database is a no-op (idempotent CREATE).
    store = StateStore(db)
    try:
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)
        assert store.get_push(OTHER) is not None
        assert store.get(HASH, include_blob=False) is not None
    finally:
        store.close()


def test_spec_is_all_not_null_with_defaults_where_possible():
    """Keyed exactly like torrent_state.

    `infohash` is the TEXT PRIMARY KEY; `state` and both clocks are NOT NULL
    *without* a DEFAULT (like torrent_state's state/created_at/updated_at) so
    every writer must set them explicitly; everything else defaults, so a
    partial record cannot be stored.
    """
    assert _SOURCE_PUSH_COLUMN_SPECS[0] == ("infohash", "TEXT PRIMARY KEY")
    required = {"infohash", "state", "started_at", "updated_at"}
    for name, ddl in _SOURCE_PUSH_COLUMN_SPECS:
        if name == "infohash":
            continue
        assert "NOT NULL" in ddl.upper(), name
        if name in required:
            continue
        assert "DEFAULT" in ddl.upper(), name


# --------------------------------------------------------------------------- #
# start_push / claim_push
# --------------------------------------------------------------------------- #


def test_start_push_stores_the_full_record(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        deadline = _deadline()
        push = store.start_push(HASH.upper(), **PUSH, deadline_at=deadline, files_total=12)
        assert push is not None
        got = store.get_push(HASH)
        assert got is not None
        # Canonical key: hashes are hex, case-insensitive.
        assert got.infohash == HASH
        assert got.group_key == PUSH["group_key"]
        assert got.content_name == PUSH["content_name"]
        assert got.src_dir == PUSH["src_dir"]
        assert got.target_remote == PUSH["target_remote"]
        assert got.list_path == PUSH["list_path"]
        assert got.log_path == PUSH["log_path"]
        assert got.state is PushState.RUNNING
        assert got.files_total == 12
        assert got.files_present == 0
        assert got.last_error == ""
        assert got.deadline_at is not None
        assert got.deadline_at == deadline
        assert got.started_at == got.updated_at
    finally:
        store.close()


def test_start_push_resets_a_prior_record(tmp_path: Path):
    """A fresh operator tap discards the previous attempt's residue."""
    store = StateStore(tmp_path / "state.db")
    try:
        first = store.start_push(HASH, **PUSH, deadline_at=_deadline())
        assert first is not None
        store.note_push_progress(HASH, files_present=7, files_total=9)
        assert store.finish_push(HASH, PushState.FAILED, last_error="rclone said no") is True

        again = store.start_push(HASH, **{**PUSH, "src_dir": "/downloads/other"})
        assert again is not None
        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.RUNNING
        assert got.files_present == 0
        assert got.files_total == 0
        assert got.last_error == ""
        assert got.src_dir == "/downloads/other"
    finally:
        store.close()


def test_start_push_without_hash_is_refused(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push("", **PUSH) is None
        assert store.claim_push("   ", **PUSH) is None
        assert store.get_push("") is None
        assert store.push_is_live("") is False
    finally:
        store.close()


def test_claim_push_dedupes_while_live(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        won = store.claim_push(HASH, **PUSH, deadline_at=_deadline())
        assert won is not None
        loser = store.claim_push(HASH, **PUSH, deadline_at=_deadline())
        assert loser is None
        # The winner's record is untouched by the loser's attempt.
        got = store.get_push(HASH)
        assert got is not None
        assert got.target_remote == PUSH["target_remote"]
        assert store.push_is_live(HASH) is True
        # A different group's hash is never blocked.
        assert store.claim_push(OTHER, **PUSH, deadline_at=_deadline()) is not None
    finally:
        store.close()


def test_claim_push_allows_after_finish(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is None

        for state in (PushState.DONE, PushState.FAILED, PushState.ABANDONED):
            assert store.finish_push(HASH, state, last_error=f"settled as {state.value}") is True
            assert store.push_is_live(HASH) is False
            assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None
            assert store.finish_push(HASH, PushState.ABANDONED) is True
    finally:
        store.close()


def test_claim_push_allows_after_the_deadline(tmp_path: Path):
    """A copy that died with a live state stops blocking at its deadline."""
    store = StateStore(tmp_path / "state.db")
    try:
        push = store.start_push(HASH, **PUSH, deadline_at=_deadline(hours=-1))
        assert push is not None
        assert store.push_is_live(HASH) is False
        claimed = store.claim_push(HASH, **{**PUSH, "log_path": "/tmp/new.log"})
        assert claimed is not None
        got = store.get_push(HASH)
        assert got is not None
        assert got.log_path == "/tmp/new.log"
        assert store.push_is_live(HASH) is True
    finally:
        store.close()


def test_claim_push_takes_over_a_verifying_record_only_when_expired(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        assert store.finish_push(HASH, PushState.VERIFYING) is True
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is None

        assert store.start_push(HASH, **PUSH, deadline_at=_deadline(hours=-1)) is not None
        assert store.finish_push(HASH, PushState.VERIFYING) is True
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None
    finally:
        store.close()


def test_claim_push_without_deadline_never_expires(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.claim_push(HASH, **PUSH) is not None
        got = store.get_push(HASH)
        assert got is not None
        assert got.deadline_at is None
        assert store.push_is_live(HASH) is True
        assert store.claim_push(HASH, **PUSH) is None
    finally:
        store.close()


def test_claim_push_is_atomic_across_threads(tmp_path: Path):
    """Concurrent taps: exactly one claim wins, the rest dedupe."""
    import threading

    store = StateStore(tmp_path / "state.db")
    try:
        won: list[bool] = []
        barrier = threading.Barrier(4)

        def tap() -> None:
            barrier.wait()
            won.append(store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None)

        threads = [threading.Thread(target=tap) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(won) == 4
        assert sum(won) == 1
        assert store.push_is_live(HASH) is True
    finally:
        store.close()


def test_claim_push_survives_a_reopen(tmp_path: Path):
    """Restart persistence: the lock is the DB's, not the process's."""
    db = tmp_path / "state.db"
    store = StateStore(db)
    try:
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline(), files_total=5) is not None
        assert store.note_push_progress(HASH, files_present=3) is True
    finally:
        store.close()

    store = StateStore(db)
    try:
        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.RUNNING
        assert got.files_total == 5
        assert got.files_present == 3
        assert got.src_dir == PUSH["src_dir"]
        assert store.push_is_live(HASH) is True
        # A second process (after the restart) still dedupes.
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is None
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Progress, finish, list, clear
# --------------------------------------------------------------------------- #


def test_note_push_progress_updates_only_the_counters(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        deadline = _deadline()
        assert store.start_push(HASH, **PUSH, deadline_at=deadline, files_total=20) is not None
        before = store.get_push(HASH)
        assert before is not None

        assert store.note_push_progress(HASH, files_present=9) is True
        after = store.get_push(HASH)
        assert after is not None
        assert after.files_present == 9
        # The untouched counter and every other column stay as written.
        assert after.files_total == 20
        assert after.state is PushState.RUNNING
        assert after.src_dir == PUSH["src_dir"]
        assert after.deadline_at == deadline

        assert store.note_push_progress(HASH, files_present=11, files_total=11) is True
        done = store.get_push(HASH)
        assert done is not None
        assert (done.files_present, done.files_total) == (11, 11)

        # Nothing provided, nothing written; unknown hash and negative clamp.
        assert store.note_push_progress(HASH) is False
        assert store.note_push_progress(OTHER, files_present=1) is False
        assert store.note_push_progress(HASH, files_present=-4) is True
        assert store.get_push(HASH).files_present == 0
    finally:
        store.close()


def test_finish_push_records_the_error_text(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        error = "rclone: Failed to copy: x5 (fake group)"
        assert store.finish_push(HASH, PushState.FAILED, last_error=error) is True
        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.FAILED
        assert got.last_error == error
        # State accepts the plain value too (Telegram/API callers pass strings).
        assert store.finish_push(HASH, "abandoned", last_error="") is True
        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.ABANDONED
        assert got.last_error == ""
        # Counters and paths survive a finish for the post-mortem card.
        assert got.content_name == PUSH["content_name"]
        assert got.target_remote == PUSH["target_remote"]

        assert store.finish_push(HASH, "not-a-state") is False
        assert store.finish_push("", PushState.DONE) is False
        assert store.finish_push(OTHER, PushState.DONE) is False
        assert store.get_push(HASH).state is PushState.ABANDONED
    finally:
        store.close()


def test_list_pushes_filters_by_state(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        assert store.start_push(OTHER, **{**PUSH, "group_key": "other-group"}) is not None
        assert store.finish_push(OTHER, PushState.DONE) is True
        third = "f" * 40
        assert store.start_push(third, **PUSH) is not None
        assert store.finish_push(third, PushState.VERIFYING) is True

        assert {p.infohash for p in store.list_pushes()} == {HASH, OTHER, third}
        assert [p.infohash for p in store.list_pushes((PushState.RUNNING,))] == [HASH]
        assert {p.infohash for p in store.list_pushes(["running", "verifying"])} == {HASH, third}
        assert [p.infohash for p in store.list_pushes((PushState.DONE,))] == [OTHER]
        assert store.list_pushes(("nonsense",)) == []
        assert store.list_pushes(()) == store.list_pushes()
    finally:
        store.close()


def test_clear_push_frees_the_hash(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        assert store.clear_push(HASH) is True
        assert store.clear_push(HASH) is False
        assert store.get_push(HASH) is None
        assert store.push_is_live(HASH) is False
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None
    finally:
        store.close()


def test_push_is_live_expiry_rule(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        now = dt.datetime.now(dt.UTC)
        assert store.push_is_live(HASH) is False

        soon = now + dt.timedelta(minutes=5)
        assert store.start_push(HASH, **PUSH, deadline_at=soon) is not None

        assert store.push_is_live(HASH) is True
        assert store.push_is_live(HASH, now=now + dt.timedelta(hours=1)) is False

        assert store.finish_push(HASH, PushState.VERIFYING) is True
        assert store.push_is_live(HASH) is True
        assert store.finish_push(HASH, PushState.DONE) is True
        assert store.push_is_live(HASH) is False
    finally:
        store.close()


def test_corrupt_push_state_reads_as_failed_and_stops_blocking(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, deadline_at=_deadline()) is not None
        with store._lock:
            store._conn.execute(
                "UPDATE source_push SET state = 'garbage' WHERE infohash = ?", (HASH,)
            )

        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.FAILED
        assert store.push_is_live(HASH) is False
        # A garbled record must not lock the hash forever.
        assert store.claim_push(HASH, **PUSH, deadline_at=_deadline()) is not None
    finally:
        store.close()


def test_source_push_row_round_trips_every_column(tmp_path: Path):
    """to_row() covers the spec exactly, like TorrentState.to_row()."""
    store = StateStore(tmp_path / "state.db")
    try:
        wanted = SourcePush(
            infohash=HASH,
            group_key="g",
            content_name="n",
            src_dir="s",
            target_remote="r",
            list_path="l",
            log_path="L",
            state=PushState.VERIFYING,
            deadline_at=_deadline(),
            files_total=3,
            files_present=2,
            last_error="e",
        )
        assert tuple(wanted.to_row()) == SOURCE_PUSH_COLUMNS

        assert (
            store.start_push(
                wanted.infohash,
                group_key=wanted.group_key,
                content_name=wanted.content_name,
                src_dir=wanted.src_dir,
                target_remote=wanted.target_remote,
                list_path=wanted.list_path,
                log_path=wanted.log_path,
                deadline_at=wanted.deadline_at,
                files_total=wanted.files_total,
            )
            is not None
        )
        assert store.finish_push(HASH, wanted.state, last_error=wanted.last_error) is True
        assert store.note_push_progress(HASH, files_present=wanted.files_present) is True

        stored = store.get_push(HASH)
        assert stored is not None
        row, want = stored.to_row(), wanted.to_row()
        # started_at/updated_at are store-managed clocks; everything else
        # must round-trip byte for byte through the single column spec.
        for name in SOURCE_PUSH_COLUMNS:
            if name in ("started_at", "updated_at"):
                assert name in row
                continue
            assert row[name] == want[name], name
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# byte counters (schema v7)
# --------------------------------------------------------------------------- #


def test_v6_db_upgrades_the_push_byte_columns(tmp_path: Path):
    """A checkpoint table written before the byte columns gains them, idempotently.

    `CREATE TABLE IF NOT EXISTS` never backfills, so without the column diff a
    v7 build would INSERT columns the v6 table does not have and every push
    note would fail. The v6 row must survive untouched, and reopening must not
    ALTER twice.
    """
    from racing_sync.state_schema import _SOURCE_PUSH_COLUMN_SPECS

    byte_columns = {"bytes_present", "bytes_total"}
    db = tmp_path / "v6-state.db"
    conn = sqlite3.connect(str(db))
    try:
        body = ",\n    ".join(f"{name} {ddl}" for name, ddl in _TORRENT_STATE_COLUMN_SPECS)
        conn.execute(f"CREATE TABLE torrent_state (\n    {body}\n);")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        push_body = ",\n    ".join(
            f"{name} {ddl}" for name, ddl in _SOURCE_PUSH_COLUMN_SPECS if name not in byte_columns
        )
        conn.execute(f"CREATE TABLE source_push (\n    {push_body}\n);")
        conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '6')")
        conn.execute(
            "INSERT INTO source_push "
            "(infohash, group_key, src_dir, target_remote, list_path, log_path, state, "
            "started_at, updated_at, deadline_at, files_total, files_present, last_error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                HASH,
                PUSH["group_key"],
                PUSH["src_dir"],
                PUSH["target_remote"],
                PUSH["list_path"],
                PUSH["log_path"],
                "running",
                STAMP,
                STAMP,
                "",
                11,
                4,
                "",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    store = StateStore(db)
    try:
        cols = [row[1] for row in store._conn.execute("PRAGMA table_info(source_push)").fetchall()]
        assert cols == list(SOURCE_PUSH_COLUMNS)
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)

        # The pre-upgrade checkpoint reads back with the new counters at zero,
        # i.e. "never observed" — not as a copy that lost all its bytes.
        before = store.get_push(HASH)
        assert before is not None
        assert (before.files_total, before.files_present) == (11, 4)
        assert (before.bytes_total, before.bytes_present) == (0, 0)
        assert before.state is PushState.RUNNING
        assert before.target_remote == PUSH["target_remote"]

        # And the new columns are immediately writable.
        assert store.note_push_progress(HASH, bytes_present=900, bytes_total=4096) is True
        after = store.get_push(HASH)
        assert after is not None
        assert (after.bytes_present, after.bytes_total) == (900, 4096)
        # A narrow note still touches nothing else.
        assert after.state is PushState.RUNNING
        assert after.files_present == 4
    finally:
        store.close()

    # Reopening an already-v7 database is a no-op (idempotent column diff).
    store = StateStore(db)
    try:
        again = store.get_push(HASH)
        assert again is not None
        assert (again.bytes_present, again.bytes_total) == (900, 4096)
    finally:
        store.close()


def test_push_byte_counters_persist_update_and_survive_a_reopen(tmp_path: Path):
    db = tmp_path / "state.db"
    store = StateStore(db)
    try:
        assert store.start_push(HASH, **PUSH, files_total=2, bytes_total=6000) is not None
        got = store.get_push(HASH)
        assert got is not None
        # Stamped at launch, so `bytes_total` means something from poll one.
        assert (got.bytes_total, got.bytes_present) == (6000, 0)

        assert store.note_push_progress(HASH, files_present=1, bytes_present=1500) is True
        got = store.get_push(HASH)
        assert got is not None
        assert (got.bytes_present, got.bytes_total, got.files_present) == (1500, 6000, 1)

        # Bytes only: the file counters keep their last reading (and reverse).
        assert store.note_push_progress(HASH, bytes_present=5100) is True
        got = store.get_push(HASH)
        assert got is not None
        assert (got.bytes_present, got.files_present) == (5100, 1)

        # No observation is not zero: passing nothing leaves the byte pair.
        assert store.note_push_progress(HASH, files_present=2) is True
        got = store.get_push(HASH)
        assert got is not None
        assert (got.bytes_present, got.bytes_total, got.files_present) == (5100, 6000, 2)

        # Clamped like the file counters; a negative reading lands at 0.
        assert store.note_push_progress(HASH, bytes_present=-9, bytes_total=-1) is True
        got = store.get_push(HASH)
        assert got is not None
        assert (got.bytes_present, got.bytes_total) == (0, 0)
    finally:
        store.close()

    # Restart: the byte counters are durable, which is the whole point.
    store = StateStore(db)
    try:
        assert store.start_push(HASH, **PUSH, bytes_total=8000) is not None
        assert store.note_push_progress(HASH, bytes_present=3000, files_present=1) is True
    finally:
        store.close()

    store = StateStore(db)
    try:
        got = store.get_push(HASH)
        assert got is not None
        assert (got.bytes_present, got.bytes_total, got.files_present) == (3000, 8000, 1)
        # A claim RESETs them exactly like the file counters do: the previous
        # copy's residue must never price a brand-new one.
        assert store.claim_push(OTHER, **PUSH, files_total=3, bytes_total=9000) is not None
        fresh = store.get_push(OTHER)
        assert fresh is not None
        assert (fresh.bytes_present, fresh.bytes_total) == (0, 9000)
    finally:
        store.close()


def test_finish_push_leaves_the_byte_counters_readable(tmp_path: Path):
    """The post-mortem card (and a stalled-copy report) needs both pairs."""
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.start_push(HASH, **PUSH, files_total=2, bytes_total=6000) is not None
        assert store.note_push_progress(HASH, files_present=1, bytes_present=1500) is True
        assert store.finish_push(HASH, PushState.FAILED, last_error="copy died") is True
        got = store.get_push(HASH)
        assert got is not None
        assert got.state is PushState.FAILED
        assert (got.bytes_present, got.bytes_total) == (1500, 6000)
        assert (got.files_present, got.files_total) == (1, 2)
    finally:
        store.close()
