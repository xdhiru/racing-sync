"""seed_subdir (schema v5): fuse seeding keeps each row's physical top folder.

Covers the dataclass/schema round-trip, the version-bumping narrow write, and
the automatic column migration of a pre-v5 database.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from racing_sync.state import (
    _SCHEMA_VERSION,
    _TORRENT_STATE_COLUMN_SPECS,
    StateStore,
    TorrentState,
    UpsertResult,
)

HASH = "c" * 40
STAMP = "2026-01-01T00:00:00+00:00"


def test_seed_subdir_round_trips_through_upsert(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.upsert(TorrentState(source_infohash=HASH, seed_subdir="Top")) is (
            UpsertResult.WRITTEN
        )

        got = store.get(HASH, include_blob=False)
        assert got is not None
        assert got.seed_subdir == "Top"

        rows = store.all()
        assert [r.seed_subdir for r in rows] == ["Top"]
    finally:
        store.close()


def test_update_columns_seed_subdir_bumps_version(tmp_path: Path):
    store = StateStore(tmp_path / "state.db")
    try:
        assert store.upsert(TorrentState(source_infohash=HASH)) is UpsertResult.WRITTEN
        before = store.get(HASH, include_blob=False)
        assert before is not None

        new_rev = store.update_columns(HASH, {"seed_subdir": "Top"})
        assert new_rev > 0
        assert new_rev > before.version

        after = store.get(HASH, include_blob=False)
        assert after is not None
        assert after.seed_subdir == "Top"
        assert after.version == new_rev
    finally:
        store.close()


def test_pre_v5_db_gains_seed_subdir(tmp_path: Path):
    db = tmp_path / "v4-state.db"
    # Pre-v5 table: every current column except seed_subdir.
    body = ",\n    ".join(
        f"{name} {ddl}" for name, ddl in _TORRENT_STATE_COLUMN_SPECS if name != "seed_subdir"
    )
    conn = sqlite3.connect(str(db))
    try:
        conn.execute(f"CREATE TABLE torrent_state (\n    {body}\n);")
        conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        conn.execute("INSERT INTO meta (key, value) VALUES ('schema_version', '4')")
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
        columns = {
            row[1] for row in store._conn.execute("PRAGMA table_info(torrent_state)").fetchall()
        }
        assert "seed_subdir" in columns
        assert store.get_meta("schema_version") == str(_SCHEMA_VERSION)

        legacy = store.get(HASH, include_blob=False)
        assert legacy is not None
        assert legacy.seed_subdir == ""
    finally:
        store.close()
