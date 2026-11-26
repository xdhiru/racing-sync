"""Regression test for audit finding B1: pre-1.0.0 state DB upgrade order.

A legacy state.db has a torrent_state table without the columns added after
1.0.0 (deleted_at, public_export_next_retry_at, move_stage, ...). Because
CREATE TABLE IF NOT EXISTS never backfills them, StateStore.__init__ must
run its ALTER TABLE migrations before creating SCHEMA_INDEXES:
ix_public_export_retry and ix_active_updated reference migrated columns, so
indexing first raises ``sqlite3.OperationalError: no such column`` and the
daemon cannot start. Fresh DBs never expose this because SCHEMA_TABLES
already contains every column.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from racing_sync.state import State, StateStore, TorrentState, UpsertResult

# Faithful early-version table: the original CREATE TABLE minus every column
# the ALTER TABLE loop adds (deleted_at, public_export_*, move_stage/-plan,
# force_direct, skipped, readd_*, failed_retries, completed_at,
# vps1_last_activity_at, batch_cap_bytes, readd_cycles, version,
# fuse_verified, telegram_message_id).
LEGACY_SCHEMA = """
CREATE TABLE torrent_state (
    source_infohash          TEXT PRIMARY KEY,
    dest_infohash            TEXT NOT NULL DEFAULT '',
    source_name              TEXT NOT NULL DEFAULT '',
    source_tracker           TEXT NOT NULL DEFAULT '',
    source_announce_url      TEXT NOT NULL DEFAULT '',
    classification_kind      TEXT NOT NULL DEFAULT 'unknown',
    total_bytes              INTEGER NOT NULL DEFAULT 0,
    save_path                TEXT NOT NULL DEFAULT '',
    cross_seed_infohash      TEXT NOT NULL DEFAULT '',
    cross_seed_source        TEXT NOT NULL DEFAULT '',
    cross_seed_blob          BLOB NOT NULL DEFAULT '',
    injected_private_hashes  TEXT NOT NULL DEFAULT '',
    indexer_first_queried_at TEXT NOT NULL DEFAULT '',
    indexer_next_retry_at    TEXT NOT NULL DEFAULT '',
    indexer_attempts         INTEGER NOT NULL DEFAULT 0,
    state                    TEXT NOT NULL,
    batch_index              INTEGER NOT NULL DEFAULT 0,
    batches_total            INTEGER NOT NULL DEFAULT 0,
    last_error               TEXT NOT NULL DEFAULT '',
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);
"""

LEGACY_HASH = "a" * 40
NEW_HASH = "b" * 40
STAMP = "2026-01-01T00:00:00+00:00"


def _make_legacy_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(LEGACY_SCHEMA)
        conn.execute(
            "INSERT INTO torrent_state "
            "(source_infohash, source_name, state, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (LEGACY_HASH, "Legacy Row", "waiting_indexer", STAMP, STAMP),
        )
        conn.commit()
    finally:
        conn.close()


def test_state_store_upgrades_pre_1_0_0_db(tmp_path: Path):
    db = tmp_path / "legacy-state.db"
    _make_legacy_db(db)

    # Pre-fix this raised OperationalError ("no such column:
    # public_export_next_retry_at") from the index script before the
    # migrations could add the column.
    store = StateStore(db)
    try:
        indexes = {
            row[0]
            for row in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert {"ix_public_export_retry", "ix_active_updated"} <= indexes

        columns = {
            row[1] for row in store._conn.execute("PRAGMA table_info(torrent_state)").fetchall()
        }
        # The upgrade diff is derived from the one column spec: a legacy DB
        # must end up with exactly that set, no additions or omissions.
        from racing_sync.state import _SCHEMA_VERSION, TORRENT_STATE_COLUMNS

        assert columns == set(TORRENT_STATE_COLUMNS)
        assert {
            "deleted_at",
            "public_export_first_attempted_at",
            "public_export_next_retry_at",
            "public_export_attempts",
            "move_stage",
            "move_plan",
            "version",
        } <= columns
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)

        # The legacy row survived the migration and is readable/writable
        # (deleted_at backfilled to '').
        legacy = store.get(LEGACY_HASH)
        assert legacy is not None
        assert legacy.source_name == "Legacy Row"
        assert legacy.deleted_at is None
        legacy.source_name = "Legacy Row Renamed"
        assert store.upsert(legacy) is UpsertResult.WRITTEN

        # Normal upsert/get round-trip on the migrated table.
        retry_at = dt.datetime(2026, 2, 1, tzinfo=dt.UTC)
        ts = TorrentState(
            source_infohash=NEW_HASH,
            source_name="Upgraded Row",
            state=State.WAITING_INDEXER,
            public_export_attempts=3,
            public_export_next_retry_at=retry_at,
            move_stage="moved",
            deleted_at=None,
        )
        assert store.upsert(ts) is UpsertResult.WRITTEN
        got = store.get(NEW_HASH)
        assert got is not None
        assert got.source_name == "Upgraded Row"
        assert got.state == State.WAITING_INDEXER
        assert got.public_export_attempts == 3
        assert got.public_export_next_retry_at == retry_at
        assert got.move_stage == "moved"

        # And the migrated table participates in tombstone reads/writes.
        assert store.tombstone(NEW_HASH) is True
        assert store.get(NEW_HASH) is None
    finally:
        store.close()


def test_schema_spec_is_the_single_source_of_truth():
    """Columns, projection, to_row and dataclass fields come from one tuple."""
    from dataclasses import fields

    from racing_sync.state import (
        _TORRENT_STATE_COLUMN_SPECS,
        _TORRENT_STATE_COLUMNS_NO_BLOB,
        TORRENT_STATE_COLUMNS,
        TorrentState,
    )

    names = tuple(name for name, _ddl in _TORRENT_STATE_COLUMN_SPECS)
    assert names == TORRENT_STATE_COLUMNS
    assert len(set(names)) == len(names)

    persisted = {f.name for f in fields(TorrentState)} - {"_blob"}
    assert persisted == set(TORRENT_STATE_COLUMNS)

    row = TorrentState(source_infohash="a" * 40).to_row()
    assert tuple(row) == TORRENT_STATE_COLUMNS

    projection = [part.strip() for part in _TORRENT_STATE_COLUMNS_NO_BLOB.split(",")]
    assert len(projection) == len(TORRENT_STATE_COLUMNS)
    assert "'' AS cross_seed_blob" in projection
    assert set(projection) - {"'' AS cross_seed_blob"} == set(TORRENT_STATE_COLUMNS) - {
        "cross_seed_blob"
    }


def test_fresh_schema_columns_match_the_spec_order(tmp_path: Path):
    from racing_sync.state import TORRENT_STATE_COLUMNS, StateStore

    store = StateStore(tmp_path / "fresh.db")
    try:
        cols = [r[1] for r in store._conn.execute("PRAGMA table_info(torrent_state)").fetchall()]
        assert tuple(cols) == TORRENT_STATE_COLUMNS
    finally:
        store.close()


def test_schema_version_recorded_and_foreign_versions_rewritten(tmp_path: Path, caplog):
    import logging

    from racing_sync.state import _SCHEMA_VERSION, StateStore

    db = tmp_path / "versioned.db"
    store = StateStore(db)
    try:
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)
        store.set_meta("schema_version", "999")
    finally:
        store.close()

    with caplog.at_level(logging.WARNING):
        store = StateStore(db)
    try:
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)
        assert "schema_version" in caplog.text
    finally:
        store.close()
