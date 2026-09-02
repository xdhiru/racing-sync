"""Persistent state for in-flight torrent processing.

This is the heart of req #4 (recovery): the state machine is the source of
truth that lets the coordinator resume work after a crash. Transitions are
explicitly enumerated so we can audit them in tests.
"""

from __future__ import annotations

import datetime as dt
import enum
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# State machine
# --------------------------------------------------------------------------- #


class State(str, enum.Enum):
    """Lifecycle of a torrent we are processing on VPS2.

    SSD download + rclone move + fuse re-add has these stages:

      NEW                 we noticed the torrent on VPS1, need to make decisions
      QUERYING            asking Seedpool for a cross-seed torrent (req #1,#2,#3)
      WAITING_SEEDPOOL    Seedpool returned no hit; we park and retry later
      WAITING_DISK        waiting for SSD to have room (cap in use)
      QUEUED              ready to add to qBittorrent on VPS2
      DOWNLOADING         qBittorrent is downloading on VPS2 SSD
      MOVING              rclone is moving the local SSD data to the remote
      RE_ADDING           re-adding the private torrent pointing at fuse mount
      DONE                all done; VPS2 is seeding from fuse
      FAILED              terminal failure; requires manual inspection
    """

    NEW = "new"
    QUERYING = "querying"
    WAITING_SEEDPOOL = "waiting_seedpool"
    WAITING_DISK = "waiting_disk"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    MOVING = "moving"
    RE_ADDING = "re_adding"
    DONE = "done"
    FAILED = "failed"


# Allowed transitions (everything else raises ValueError).
ALLOWED: dict[State, set[State]] = {
    State.NEW: {State.QUERYING, State.WAITING_SEEDPOOL, State.WAITING_DISK,
                State.QUEUED, State.DOWNLOADING, State.MOVING, State.RE_ADDING,
                State.DONE, State.FAILED},
    State.QUERYING: {State.WAITING_SEEDPOOL, State.WAITING_DISK, State.QUEUED,
                State.DOWNLOADING, State.FAILED},
    State.WAITING_SEEDPOOL: {State.QUERYING, State.WAITING_DISK,
                State.QUEUED, State.FAILED},
    State.WAITING_DISK: {State.QUEUED, State.DOWNLOADING, State.FAILED},
    State.QUEUED: {State.DOWNLOADING, State.FAILED},
    State.DOWNLOADING: {State.MOVING, State.FAILED},
    State.MOVING: {State.RE_ADDING, State.FAILED},
    State.RE_ADDING: {State.DONE, State.FAILED},
    State.DONE: set(),
    State.FAILED: {State.QUEUED},  # allow manual retry
}


def check_transition(src: State, dst: State) -> None:
    if dst not in ALLOWED[src]:
        raise ValueError(f"illegal state transition: {src.value} -> {dst.value}")


# --------------------------------------------------------------------------- #
# Persistent record
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class TorrentState:
    # Identity
    source_infohash: str        # VPS1 racing client infohash
    dest_infohash: str = ""     # VPS2 qBittorrent infohash (may equal source for public)
    # Source torrent meta
    source_name: str = ""
    source_tracker: str = ""
    source_announce_url: str = ""
    # Files / classification
    classification_kind: str = "unknown"
    total_bytes: int = 0
    # SSD-side state
    save_path: str = ""
    # Cross-seed bookkeeping
    cross_seed_infohash: str = ""  # prowlarr / SFTP-fetched torrent used on SSD
    cross_seed_source: str = ""    # "prowlarr" | "sftp" | "self"
    injected_private_hashes: str = ""  # CSV of private hashes re-added to fuse
    # Seedpool retry policy
    seedpool_first_queried_at: dt.datetime | None = None
    seedpool_next_retry_at: dt.datetime | None = None
    seedpool_attempts: int = 0
    # Lifecycle
    state: State = State.NEW
    batch_index: int = 0
    batches_total: int = 0
    last_error: str = ""
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    updated_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    # Telegram detail message id (0 if not yet sent).
    telegram_message_id: int = 0
    # Transient in-memory storage for the .torrent bytes during processing
    _blob: bytes = b""

    def to_row(self) -> dict:
        return {
            "source_infohash": self.source_infohash,
            "dest_infohash": self.dest_infohash,
            "source_name": self.source_name,
            "source_tracker": self.source_tracker,
            "source_announce_url": self.source_announce_url,
            "classification_kind": self.classification_kind,
            "total_bytes": self.total_bytes,
            "save_path": self.save_path,
            "cross_seed_infohash": self.cross_seed_infohash,
            "cross_seed_source": self.cross_seed_source,
            "injected_private_hashes": self.injected_private_hashes,
            "seedpool_first_queried_at":
                self.seedpool_first_queried_at.isoformat()
                if self.seedpool_first_queried_at else "",
            "seedpool_next_retry_at":
                self.seedpool_next_retry_at.isoformat()
                if self.seedpool_next_retry_at else "",
            "seedpool_attempts": self.seedpool_attempts,
            "state": self.state.value,
            "batch_index": self.batch_index,
            "batches_total": self.batches_total,
            "last_error": self.last_error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "telegram_message_id": self.telegram_message_id,
        }


SCHEMA = """
CREATE TABLE IF NOT EXISTS torrent_state (
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
    injected_private_hashes  TEXT NOT NULL DEFAULT '',
    seedpool_first_queried_at TEXT NOT NULL DEFAULT '',
    seedpool_next_retry_at    TEXT NOT NULL DEFAULT '',
    seedpool_attempts         INTEGER NOT NULL DEFAULT 0,
    state                    TEXT NOT NULL,
    batch_index              INTEGER NOT NULL DEFAULT 0,
    batches_total            INTEGER NOT NULL DEFAULT 0,
    last_error               TEXT NOT NULL DEFAULT '',
    telegram_message_id      INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_state ON torrent_state(state);
CREATE INDEX IF NOT EXISTS ix_seedpool_retry
    ON torrent_state(state, seedpool_next_retry_at);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT NOT NULL,
    source_infohash TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);
"""


class StateStore:
    """Thin synchronous wrapper over sqlite3.

    Async callers should run operations in a thread to keep the event loop
    unblocked. The DB is small (hundreds of rows at most) and writes are
    single-record, so the latency is fine.
    """

    def __init__(self, db_path: Path):
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # ---- CRUD ----

    def get(self, source_infohash: str) -> TorrentState | None:
        row = self._conn.execute(
            "SELECT * FROM torrent_state WHERE source_infohash = ?",
            (source_infohash,),
        ).fetchone()
        return _row_to_state(row) if row else None

    def upsert(self, ts: TorrentState) -> None:
        ts.updated_at = dt.datetime.now(dt.timezone.utc)
        row = ts.to_row()
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        updates = ", ".join(f"{k}=excluded.{k}" for k in row if k != "source_infohash")
        self._conn.execute(
            f"INSERT INTO torrent_state ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT(source_infohash) DO UPDATE SET {updates}",
            tuple(row.values()),
        )

    def list_by_state(self, *states: State) -> list[TorrentState]:
        if not states:
            return []
        qmarks = ",".join(["?"] * len(states))
        rows = self._conn.execute(
            f"SELECT * FROM torrent_state WHERE state IN ({qmarks}) ORDER BY updated_at",
            [s.value for s in states],
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def list_seedpool_ready(self, now: dt.datetime | None = None) -> list[TorrentState]:
        """Rows in WAITING_SEEDPOOL whose retry timer has elapsed.

        Used by the coordinator tick to decide which rows to wake up and
        re-query Seedpool.
        """
        now = now or dt.datetime.now(dt.timezone.utc)
        rows = self._conn.execute(
            "SELECT * FROM torrent_state WHERE state = 'waiting_seedpool' "
            "AND seedpool_next_retry_at != '' "
            "AND seedpool_next_retry_at <= ? ORDER BY seedpool_next_retry_at",
            (now.isoformat(),),
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def list_active_inflight(self) -> list[TorrentState]:
        """Rows that should appear in the active-tasks Telegram message.

        Excludes DONE and FAILED — those have a settled detail message in
        chat history and should not clutter the live list.
        """
        rows = self._conn.execute(
            "SELECT * FROM torrent_state WHERE state NOT IN ('done','failed') "
            "ORDER BY updated_at DESC"
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def get_telegram_message_id(self, source_infohash: str) -> int | None:
        row = self._conn.execute(
            "SELECT telegram_message_id FROM torrent_state WHERE source_infohash = ?",
            (source_infohash,),
        ).fetchone()
        if row is None or not row["telegram_message_id"]:
            return None
        return int(row["telegram_message_id"])

    def set_telegram_message_id(self, source_infohash: str, message_id: int) -> None:
        self._conn.execute(
            "UPDATE torrent_state SET telegram_message_id = ?, "
            "updated_at = ? WHERE source_infohash = ?",
            (message_id, dt.datetime.now(dt.timezone.utc).isoformat(),
             source_infohash),
        )

    def all_active(self) -> list[TorrentState]:
        rows = self._conn.execute(
            "SELECT * FROM torrent_state WHERE state != 'done' AND state != 'failed' "
            "ORDER BY updated_at"
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def all(self) -> list[TorrentState]:
        rows = self._conn.execute(
            "SELECT * FROM torrent_state ORDER BY updated_at DESC"
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def find_by_name(self, source_name: str) -> list[TorrentState]:
        rows = self._conn.execute(
            "SELECT * FROM torrent_state WHERE source_name = ? ORDER BY updated_at DESC",
            (source_name,),
        ).fetchall()
        return [_row_to_state(r) for r in rows]

    def delete(self, source_infohash: str) -> None:
        self._conn.execute(
            "DELETE FROM torrent_state WHERE source_infohash = ?", (source_infohash,)
        )

    # ---- convenience ----

    def transition(self, ts: TorrentState, dst: State,
                   *, error: str = "", batch_index: int | None = None) -> None:
        check_transition(ts.state, dst)
        ts.state = dst
        if error:
            ts.last_error = error
        if batch_index is not None:
            ts.batch_index = batch_index
        self.upsert(ts)
        log.info("state %s -> %s for %s", ts.source_infohash[:8], dst.value, ts.source_name)

    def append_log(self, level: str, message: str,
                   source_infohash: str | None = None) -> None:
        self._conn.execute(
            "INSERT INTO run_log (ts, source_infohash, level, message) VALUES (?,?,?,?)",
            (dt.datetime.now(dt.timezone.utc).isoformat(), source_infohash, level, message),
        )

    def iter_logs(self, limit: int = 200) -> Iterator[sqlite3.Row]:
        return self._conn.execute(
            "SELECT ts, source_infohash, level, message FROM run_log ORDER BY id DESC LIMIT ?",
            (limit,),
        )


def _row_to_state(row: sqlite3.Row) -> TorrentState:
    sp_first = row["seedpool_first_queried_at"]
    sp_next = row["seedpool_next_retry_at"]
    return TorrentState(
        source_infohash=row["source_infohash"],
        dest_infohash=row["dest_infohash"],
        source_name=row["source_name"],
        source_tracker=row["source_tracker"],
        source_announce_url=row["source_announce_url"],
        classification_kind=row["classification_kind"],
        total_bytes=row["total_bytes"],
        save_path=row["save_path"],
        cross_seed_infohash=row["cross_seed_infohash"],
        cross_seed_source=row["cross_seed_source"],
        injected_private_hashes=row["injected_private_hashes"],
        seedpool_first_queried_at=(
            dt.datetime.fromisoformat(sp_first) if sp_first else None
        ),
        seedpool_next_retry_at=(
            dt.datetime.fromisoformat(sp_next) if sp_next else None
        ),
        seedpool_attempts=row["seedpool_attempts"],
        state=State(row["state"]),
        batch_index=row["batch_index"],
        batches_total=row["batches_total"],
        last_error=row["last_error"],
        created_at=dt.datetime.fromisoformat(row["created_at"]),
        updated_at=dt.datetime.fromisoformat(row["updated_at"]),
        telegram_message_id=row["telegram_message_id"],
    )