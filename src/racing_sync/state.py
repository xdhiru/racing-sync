"""Persistent state for in-flight torrent processing.

This is the heart of req #4 (recovery): the state machine is the source of
truth that lets the coordinator resume work after a crash. Transitions are
explicitly enumerated so we can audit them in tests.
"""

from __future__ import annotations

import datetime as dt
import enum
import logging
import re
import sqlite3
import threading
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
      QUERYING            asking the download-target indexers for a cross-seed torrent (req #1,#2,#3)
      WAITING_INDEXER     no download-target indexer returned a hit yet; we park and retry later
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
    WAITING_INDEXER = "waiting_indexer"
    WAITING_DISK = "waiting_disk"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    MOVING = "moving"
    RE_ADDING = "re_adding"
    DONE = "done"
    FAILED = "failed"


# Allowed transitions (everything else raises ValueError).
# Pre-SSD states (NEW/QUERYING/WAITING_INDEXER/WAITING_DISK) may fast-track
# straight to DONE when a manual fuse seed is detected: the operator added
# the same infohash on VPS2 pointing at the fuse mount (any category) with
# verified bytes, so no SSD download / rclone move is needed.
ALLOWED: dict[State, set[State]] = {
    State.NEW: {State.QUERYING, State.WAITING_INDEXER, State.WAITING_DISK,
                State.QUEUED, State.DOWNLOADING, State.MOVING, State.RE_ADDING,
                State.DONE, State.FAILED},
    State.QUERYING: {State.WAITING_INDEXER, State.WAITING_DISK, State.QUEUED,
                State.DOWNLOADING, State.DONE, State.FAILED},
    State.WAITING_INDEXER: {State.QUERYING, State.WAITING_DISK,
                State.QUEUED, State.DONE, State.FAILED},
    State.WAITING_DISK: {State.QUEUED, State.DOWNLOADING, State.DONE, State.FAILED},
    State.QUEUED: {State.DOWNLOADING, State.MOVING, State.WAITING_DISK,
                   State.RE_ADDING, State.DONE, State.FAILED},
    State.DOWNLOADING: {State.MOVING, State.FAILED},
    State.MOVING: {State.RE_ADDING, State.FAILED},
    State.RE_ADDING: {State.DONE, State.FAILED},
    # DONE -> MOVING is the fresh-DB self-heal: recovery may have adopted an
    # SSD-complete torrent as DONE (e.g. stale DB, misclassified save_path);
    # the late cross-seed guard demotes it back to MOVING so the rclone move
    # runs before any fuse injection. DONE -> RE_ADDING stays for lost fuses.
    State.DONE: {State.RE_ADDING, State.MOVING},
    State.FAILED: {State.QUEUED, State.NEW},  # allow manual and auto retry
}


def check_transition(src: State, dst: State) -> None:
    if dst not in ALLOWED[src]:
        raise ValueError(f"illegal state transition: {src.value} -> {dst.value}")


# DONE -> RE_ADDING / DONE -> MOVING demotions look like fresh incidents, but a
# row flapping rapidly (lost fuse entry re-added, lost again, ...) would reset
# its re-add timer forever and never trip the 24h max-age guard. Count rapid
# demotions (within _FLAP_WINDOW_SECONDS of entering DONE) in readd_cycles;
# a long healthy DONE period resets the count (unrelated later incident).
_FLAP_WINDOW_SECONDS = 24 * 3600
# Consecutive rapid DONE -> re-add demotions before the row is marked FAILED
# for operator attention.
_MAX_READD_CYCLES = 5


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
    # Raw .torrent bytes used on SSD (prowlarr / SFTP-fetched). Persisted
    # so that recovery after a restart can re-add the cross-seed torrent
    # when only the racing torrents survived.
    cross_seed_blob: bytes = b""
    injected_private_hashes: str = ""  # CSV of private hashes re-added to fuse
    # Download-target indexer retry policy
    indexer_first_queried_at: dt.datetime | None = None
    indexer_next_retry_at: dt.datetime | None = None
    indexer_attempts: int = 0
    # Re-add retry policy
    readd_first_attempted_at: dt.datetime | None = None
    readd_next_retry_at: dt.datetime | None = None
    readd_attempts: int = 0
    # Failure retry tracking
    failed_retries: int = 0
    # VPS1 cleanup bookkeeping (see [cleanup]):
    # - completed_at: last time the row entered DONE (grace anchor).
    # - vps1_last_activity_at: last time the VPS1 swarm showed upload
    #   activity (idle detection for fast-lane deletion).
    completed_at: dt.datetime | None = None
    vps1_last_activity_at: dt.datetime | None = None
    # Lifecycle
    state: State = State.NEW
    batch_index: int = 0
    batches_total: int = 0
    # Frozen per-torrent SSD batch cap (persisted so restarts keep the same
    # batch boundaries instead of re-freezing at whatever free space says).
    batch_cap_bytes: int = 0
    # Consecutive rapid DONE -> RE_ADDING/MOVING demotions (flap counter for
    # the re-add max-age guard; reset by long healthy DONE periods and fresh
    # pipeline entries).
    readd_cycles: int = 0
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
            "cross_seed_blob": self.cross_seed_blob,
            "injected_private_hashes": self.injected_private_hashes,
            "indexer_first_queried_at":
                self.indexer_first_queried_at.isoformat()
                if self.indexer_first_queried_at else "",
            "indexer_next_retry_at":
                self.indexer_next_retry_at.isoformat()
                if self.indexer_next_retry_at else "",
            "indexer_attempts": self.indexer_attempts,
            "readd_first_attempted_at": (
                self.readd_first_attempted_at.isoformat()
                if self.readd_first_attempted_at else ""
            ),
            "readd_next_retry_at": (
                self.readd_next_retry_at.isoformat()
                if self.readd_next_retry_at else ""
            ),
            "readd_attempts": self.readd_attempts,
            "failed_retries": self.failed_retries,
            "completed_at":
                self.completed_at.isoformat()
                if self.completed_at else "",
            "vps1_last_activity_at":
                self.vps1_last_activity_at.isoformat()
                if self.vps1_last_activity_at else "",
            "state": self.state.value,
            "batch_index": self.batch_index,
            "batches_total": self.batches_total,
            "batch_cap_bytes": self.batch_cap_bytes,
            "readd_cycles": self.readd_cycles,
            "last_error": self.last_error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "telegram_message_id": self.telegram_message_id,
        }


SCHEMA_TABLES = """
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
    cross_seed_blob          BLOB NOT NULL DEFAULT '',
    injected_private_hashes  TEXT NOT NULL DEFAULT '',
    indexer_first_queried_at TEXT NOT NULL DEFAULT '',
    indexer_next_retry_at    TEXT NOT NULL DEFAULT '',
    indexer_attempts         INTEGER NOT NULL DEFAULT 0,
    readd_first_attempted_at  TEXT NOT NULL DEFAULT '',
    readd_next_retry_at       TEXT NOT NULL DEFAULT '',
    readd_attempts            INTEGER NOT NULL DEFAULT 0,
    failed_retries           INTEGER NOT NULL DEFAULT 0,
    completed_at             TEXT NOT NULL DEFAULT '',
    vps1_last_activity_at    TEXT NOT NULL DEFAULT '',
    state                    TEXT NOT NULL,
    batch_index              INTEGER NOT NULL DEFAULT 0,
    batches_total            INTEGER NOT NULL DEFAULT 0,
    batch_cap_bytes          INTEGER NOT NULL DEFAULT 0,
    readd_cycles             INTEGER NOT NULL DEFAULT 0,
    last_error               TEXT NOT NULL DEFAULT '',
    telegram_message_id      INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT NOT NULL,
    source_infohash TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ignored_torrents (
    source_infohash TEXT PRIMARY KEY,
    source_name     TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT ''
);
"""

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_state ON torrent_state(state);
CREATE INDEX IF NOT EXISTS ix_indexer_retry
    ON torrent_state(state, indexer_next_retry_at);
CREATE INDEX IF NOT EXISTS ix_source_name ON torrent_state(source_name);
"""

SCHEMA = SCHEMA_TABLES + SCHEMA_INDEXES


_TORRENT_STATE_COLUMNS_NO_BLOB = (
    "source_infohash, dest_infohash, source_name, source_tracker, source_announce_url, "
    "classification_kind, total_bytes, save_path, cross_seed_infohash, cross_seed_source, "
    "'' AS cross_seed_blob, injected_private_hashes, indexer_first_queried_at, "
    "indexer_next_retry_at, indexer_attempts, readd_first_attempted_at, "
    "readd_next_retry_at, readd_attempts, failed_retries, completed_at, "
    "vps1_last_activity_at, state, batch_index, batches_total, batch_cap_bytes, readd_cycles, "
    "last_error, created_at, updated_at, telegram_message_id"
)


class StateStore:
    """SQLite-backed persistent store for TorrentState records.

    Async callers should run operations in a thread to keep the event loop
    unblocked. The DB is small (hundreds of rows at most) and writes are
    single-record, so the latency is fine.
    """

    def __init__(self, db_path: Path):
        self._lock = threading.RLock()
        self._log_append_counter = 0
        self._db_path = db_path
        self._closed = False
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(db_path), isolation_level=None, timeout=30.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        journal_mode = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
        try:
            mode = (journal_mode[0] if journal_mode else "").lower()
        except Exception:
            mode = ""
        if mode != "wal":
            log.warning("state DB journal_mode is %r, not WAL; performance may degrade", journal_mode)
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        # Bound WAL growth on long-lived coordinators doing per-tick upserts.
        try:
            self._conn.execute("PRAGMA journal_size_limit=67108864")
        except Exception:
            pass
        self._conn.executescript(SCHEMA_TABLES)
        self._conn.executescript(SCHEMA_INDEXES)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("StateStore is closed")

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._conn.close()
            finally:
                self._closed = True

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- CRUD ----

    def get(self, source_infohash: str, include_blob: bool = True) -> TorrentState | None:
        self._ensure_open()
        cols = "*" if include_blob else _TORRENT_STATE_COLUMNS_NO_BLOB
        with self._lock:
            row = self._conn.execute(
                f"SELECT {cols} FROM torrent_state WHERE source_infohash = ?",
                (source_infohash,),
            ).fetchone()
            return _row_to_state(row) if row else None

    def get_blob(self, source_infohash: str) -> bytes:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute(
                "SELECT cross_seed_blob FROM torrent_state WHERE source_infohash = ?",
                (source_infohash,),
            ).fetchone()
            if row and row["cross_seed_blob"]:
                return bytes(row["cross_seed_blob"])
            return b""

    def upsert(self, ts: TorrentState) -> None:
        self._ensure_open()
        with self._lock:
            ts.updated_at = dt.datetime.now(dt.timezone.utc)
            row = ts.to_row()
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            updates = []
            for k in row:
                if k in ("source_infohash", "created_at"):
                    continue
                if k == "cross_seed_blob":
                    updates.append(
                        f"{k} = CASE WHEN length(excluded.{k}) > 0 THEN excluded.{k} ELSE torrent_state.{k} END"
                    )
                else:
                    updates.append(f"{k} = excluded.{k}")
            updates_str = ", ".join(updates)
            self._conn.execute(
                f"INSERT INTO torrent_state ({cols}) VALUES ({placeholders}) "
                f"ON CONFLICT(source_infohash) DO UPDATE SET {updates_str}",
                tuple(row.values()),
            )

    def list_by_state(self, *states: State) -> list[TorrentState]:
        if not states:
            return []
        qmarks = ",".join(["?"] * len(states))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state IN ({qmarks}) ORDER BY updated_at",
                [s.value for s in states],
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def list_indexer_ready(self, now: dt.datetime | None = None) -> list[TorrentState]:
        """Rows in WAITING_INDEXER whose retry timer has elapsed.

        Used by the coordinator tick to decide which rows to wake up and
        re-query the download-target indexers.
        """
        now = now or dt.datetime.now(dt.timezone.utc)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state = 'waiting_indexer' "
                "AND indexer_next_retry_at != '' "
                "AND indexer_next_retry_at <= ? ORDER BY indexer_next_retry_at",
                (now.isoformat(),),
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def list_active_inflight(self) -> list[TorrentState]:
        """Rows that should appear in the active-tasks Telegram message.

        Excludes DONE and FAILED — those have a settled detail message in
        chat history and should not clutter the live list.
        """
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state NOT IN ('done','failed') "
                "ORDER BY updated_at DESC"
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def get_telegram_message_id(self, source_infohash: str) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT telegram_message_id FROM torrent_state WHERE source_infohash = ?",
                (source_infohash,),
            ).fetchone()
            if row is None or not row["telegram_message_id"]:
                return None
            return int(row["telegram_message_id"])

    def set_telegram_message_id(self, source_infohash: str, message_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE torrent_state SET telegram_message_id = ?, "
                "updated_at = ? WHERE source_infohash = ?",
                (message_id, dt.datetime.now(dt.timezone.utc).isoformat(),
                 source_infohash),
            )

    def all_active(self, include_blob: bool = False, limit: int | None = None) -> list[TorrentState]:
        cols = _TORRENT_STATE_COLUMNS_NO_BLOB if not include_blob else "*"
        with self._lock:
            if limit is not None:
                try:
                    limit = int(limit)
                except (TypeError, ValueError):
                    limit = None
            if limit is not None and limit > 0:
                rows = self._conn.execute(
                    f"SELECT {cols} FROM torrent_state WHERE state != 'done' AND state != 'failed' "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [_row_to_state(r) for r in rows]
            rows = self._conn.execute(
                f"SELECT {cols} FROM torrent_state WHERE state != 'done' AND state != 'failed' "
                "ORDER BY updated_at"
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def all(self, include_blob: bool = False, limit: int | None = None, offset: int = 0) -> list[TorrentState]:
        cols = "*" if include_blob else _TORRENT_STATE_COLUMNS_NO_BLOB
        with self._lock:
            if limit is not None:
                try:
                    limit = int(limit)
                except (TypeError, ValueError):
                    limit = None
                try:
                    offset = int(offset)
                except (TypeError, ValueError):
                    offset = 0
                if limit is not None and limit > 0:
                    rows = self._conn.execute(
                        f"SELECT {cols} FROM torrent_state ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                        (limit, max(0, offset)),
                    ).fetchall()
                    return [_row_to_state(r) for r in rows]
            rows = self._conn.execute(
                f"SELECT {cols} FROM torrent_state ORDER BY updated_at DESC"
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def find_by_name(self, source_name: str) -> list[TorrentState]:
        clean_name = source_name.strip()
        for ext in (".mkv", ".mp4", ".avi", ".ts", ".m4v", ".torrent"):
            if clean_name.lower().endswith(ext):
                clean_name = clean_name[:-len(ext)].strip()
                break
        stripped_name = re.sub(r"\s*\[[^\]]+\]\s*$", "", clean_name).strip()

        def _escape_like(s: str) -> str:
            # Escape LIKE wildcards so `100%` / `S01_E01` match literally.
            return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state "
                "WHERE source_name = ? OR source_name = ? "
                "OR source_name = ? OR source_name LIKE ? ESCAPE '\\' "
                "OR source_name LIKE ? ESCAPE '\\' "
                "ORDER BY updated_at DESC",
                (
                    source_name,
                    clean_name,
                    stripped_name,
                    f"{_escape_like(clean_name)}.%",
                    f"{_escape_like(stripped_name)}%",
                ),
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def delete(self, source_infohash: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM torrent_state WHERE source_infohash = ?", (source_infohash,)
            )

    # ---- ignore list (cancelled releases) ----

    def ignore_torrent(self, source_infohash: str, source_name: str = "") -> None:
        """Never pick up this release again (cancelled by the operator).

        Checked at discovery, recovery adoption, re-injection and late-seed
        time so a cancelled torrent stays cancelled while it remains on the
        VPS1 racing client. Lives in state.db: `--reset` clears it (fresh
        start means fresh intent).
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            raise ValueError("ignore_torrent requires an infohash")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO ignored_torrents "
                "(source_infohash, source_name, created_at) VALUES (?,?,?)",
                (norm, source_name or "",
                 dt.datetime.now(dt.timezone.utc).isoformat()),
            )

    def unignore_torrent(self, source_infohash: str) -> bool:
        """Drop an ignore entry; True when one existed."""
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM ignored_torrents WHERE source_infohash = ?",
                (norm,),
            )
            return (cur.rowcount or 0) > 0

    def is_ignored(self, source_infohash: str) -> bool:
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        try:
            with self._lock:
                row = self._conn.execute(
                    "SELECT 1 FROM ignored_torrents WHERE source_infohash = ?",
                    (norm,),
                ).fetchone()
                return row is not None
        except Exception:
            return False

    def list_ignored(self) -> list[dict]:
        """All ignore entries, newest first."""
        self._ensure_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_infohash, source_name, created_at "
                "FROM ignored_torrents ORDER BY created_at DESC"
            ).fetchall()
            return [
                {"source_infohash": r["source_infohash"],
                 "source_name": r["source_name"],
                 "created_at": r["created_at"]}
                for r in rows
            ]

    def find_ignored(self, target: str) -> dict:
        """Single ignore entry by infohash or unique name substring.

        Raises LookupError when nothing matches or several match.
        """
        norm = (target or "").strip()
        if not norm:
            raise LookupError("ignore target must not be empty")
        rows = self.list_ignored()
        low = norm.lower()
        for r in rows:
            if low == (r["source_infohash"] or "").lower():
                return r
        matches = [r for r in rows if low in (r["source_name"] or "").lower()]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            preview = ", ".join(
                f"{r['source_name']} ({(r['source_infohash'] or '')[:10]})"
                for r in matches[:5]
            )
            raise LookupError(
                f"ignore target {target!r} matches {len(matches)} entries: {preview}; "
                "use a 40-char infohash to pick one"
            )
        raise LookupError(f"no ignored torrent matching {target!r}")

    # ---- convenience ----

    def transition(self, ts: TorrentState, dst: State,
                   *, error: str = "", batch_index: int | None = None) -> None:
        check_transition(ts.state, dst)
        src = ts.state
        now = dt.datetime.now(dt.timezone.utc)
        # Snapshot everything this method mutates: a failed upsert (SQLITE_FULL,
        # locked) must leave the in-memory object identical to the DB row.
        _snapshot = (
            ts.state, ts.last_error,
            ts.indexer_first_queried_at, ts.indexer_next_retry_at,
            ts.indexer_attempts, ts.readd_first_attempted_at,
            ts.readd_next_retry_at, ts.readd_attempts, ts.failed_retries,
            ts.completed_at, ts.batch_index, ts.readd_cycles,
        )
        try:
            ts.state = dst
            ts.last_error = error
            if dst in (State.NEW, State.QUEUED):
                ts.indexer_first_queried_at = None
                ts.indexer_next_retry_at = None
                ts.indexer_attempts = 0
                ts.readd_first_attempted_at = None
                ts.readd_next_retry_at = None
                ts.readd_attempts = 0
                ts.readd_cycles = 0
                # NOTE: failed_retries is intentionally preserved here (lifetime
                # cap for auto_retry_failed). Callers requesting a fresh retry
                # (e.g. POST /api/retry) reset it explicitly before transition.
            elif dst == State.DONE:
                ts.failed_retries = 0
                ts.readd_first_attempted_at = None
                ts.readd_next_retry_at = None
                ts.readd_attempts = 0
                # Grace anchor for the VPS1 cleanup janitor ([cleanup]): every
                # entry into DONE restarts the clock (e.g. after a lost-fuse
                # re-add cycle finishes seeding again).
                ts.completed_at = now
            elif dst in (State.RE_ADDING, State.MOVING) and src == State.DONE:
                # Demotion of a DONE row back into the pipeline:
                #  - DONE -> RE_ADDING: lost fuse torrent (recovery) or late
                #    cross-seed repair; stale timers from the previous cycle
                #    must not instantly trip the max-age guard in _do_re_add.
                #  - DONE -> MOVING: self-heal for falsely adopted DONE (SSD
                #    bytes never moved); start a fresh move cycle.
                # A demotion long after DONE is a new incident (reset the
                # flap count); a rapid one keeps accumulating toward FAILED.
                ts.readd_first_attempted_at = None
                ts.readd_next_retry_at = None
                ts.readd_attempts = 0
                try:
                    rapid = (
                        ts.completed_at is not None
                        and (now - ts.completed_at).total_seconds() < _FLAP_WINDOW_SECONDS
                    )
                except Exception:
                    rapid = True
                ts.readd_cycles = (ts.readd_cycles + 1) if rapid else 0
            if batch_index is not None:
                ts.batch_index = batch_index
            self.upsert(ts)
        except Exception:
            (ts.state, ts.last_error,
             ts.indexer_first_queried_at, ts.indexer_next_retry_at,
             ts.indexer_attempts, ts.readd_first_attempted_at,
             ts.readd_next_retry_at, ts.readd_attempts, ts.failed_retries,
             ts.completed_at, ts.batch_index, ts.readd_cycles) = _snapshot
            raise
        log.info("state %s -> %s for %s", ts.source_infohash[:8], dst.value, ts.source_name)

    def append_log(self, level: str, message: str,
                   source_infohash: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO run_log (ts, source_infohash, level, message) VALUES (?,?,?,?)",
                (dt.datetime.now(dt.timezone.utc).isoformat(), source_infohash, level, message),
            )
            self._log_append_counter += 1
            if self._log_append_counter >= 500:
                self._log_append_counter = 0
                self._conn.execute(
                    "DELETE FROM run_log WHERE id NOT IN (SELECT id FROM run_log ORDER BY id DESC LIMIT 5000)"
                )

    def prune_logs(self, max_records: int = 5000) -> None:
        # SQLite LIMIT -1 means "no limit" and would invert the prune.
        try:
            max_records = int(max_records)
        except (TypeError, ValueError):
            return
        if max_records < 1:
            return
        with self._lock:
            self._conn.execute(
                "DELETE FROM run_log WHERE id NOT IN (SELECT id FROM run_log ORDER BY id DESC LIMIT ?)",
                (max_records,),
            )

    def iter_logs(self, limit: int = 200) -> list[sqlite3.Row]:
        try:
            limit = int(limit)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            limit = 200
        # SQLite LIMIT -1 means "no limit" — clamp to avoid API-driven OOM.
        limit = max(1, min(limit, 5000))
        with self._lock:
            return self._conn.execute(
                "SELECT ts, source_infohash, level, message FROM run_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        """Get a metadata value by key."""
        with self._lock:
            cur = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cur.fetchone()
            if row is None:
                return default
            return str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        """Set or update a metadata key/value."""
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )



def _safe_state(value: object) -> State:
    """Parse a state string without crashing startup on corrupt/legacy rows."""
    try:
        return State(str(value))
    except ValueError:
        log.warning("state DB has unknown state %r; treating as FAILED", value)
        return State.FAILED


def _safe_int(value: object, default: int = 0) -> int:
    """Exact int parse; one corrupt cell must not kill a listing."""
    try:
        if value is None or value == "":
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        # Exact first (avoids float precision loss >2**53), float fallback
        # for "1e3"/"10.0" legacy cells.
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_dt(value: object) -> dt.datetime | None:
    """Parse ISO datetime or return None on corrupt/legacy values."""
    if not value:
        return None
    try:
        s = str(value).strip()
        # Legacy cells with trailing Z (UTC) — fromisoformat needs +00:00.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    except (TypeError, ValueError):
        log.warning("state DB has corrupt datetime %r; treating as None", value)
        return None


def _row_to_state(row: sqlite3.Row) -> TorrentState:
    keys = row.keys()
    idx_first = row["indexer_first_queried_at"] if "indexer_first_queried_at" in keys else ""
    idx_next = row["indexer_next_retry_at"] if "indexer_next_retry_at" in keys else ""
    idx_attempts = row["indexer_attempts"] if "indexer_attempts" in keys else 0
    ra_first = row["readd_first_attempted_at"] if "readd_first_attempted_at" in keys else ""
    ra_next = row["readd_next_retry_at"] if "readd_next_retry_at" in keys else ""
    ra_attempts = row["readd_attempts"] if "readd_attempts" in keys else 0

    blob_raw = row["cross_seed_blob"] if "cross_seed_blob" in keys else b""
    if not blob_raw:
        blob_bytes = b""
    elif isinstance(blob_raw, (bytes, bytearray, memoryview)):
        blob_bytes = bytes(blob_raw)
    elif isinstance(blob_raw, str):
        blob_bytes = blob_raw.encode("utf-8")
    else:
        try:
            blob_bytes = bytes(blob_raw)
        except Exception:
            blob_bytes = b""

    return TorrentState(
        source_infohash=row["source_infohash"] if "source_infohash" in keys else "",
        dest_infohash=row["dest_infohash"] if "dest_infohash" in keys else "",
        source_name=row["source_name"] if "source_name" in keys else "",
        source_tracker=row["source_tracker"] if "source_tracker" in keys else "",
        source_announce_url=row["source_announce_url"] if "source_announce_url" in keys else "",
        classification_kind=row["classification_kind"] if "classification_kind" in keys else "unknown",
        total_bytes=_safe_int(row["total_bytes"]) if "total_bytes" in keys else 0,
        save_path=row["save_path"] if "save_path" in keys else "",
        cross_seed_infohash=row["cross_seed_infohash"] if "cross_seed_infohash" in keys else "",
        cross_seed_source=row["cross_seed_source"] if "cross_seed_source" in keys else "",
        cross_seed_blob=blob_bytes,
        injected_private_hashes=row["injected_private_hashes"] if "injected_private_hashes" in keys else "",
        indexer_first_queried_at=_safe_dt(idx_first),
        indexer_next_retry_at=_safe_dt(idx_next),
        indexer_attempts=_safe_int(idx_attempts),
        readd_first_attempted_at=_safe_dt(ra_first),
        readd_next_retry_at=_safe_dt(ra_next),
        readd_attempts=_safe_int(ra_attempts),
        failed_retries=_safe_int(row["failed_retries"]) if "failed_retries" in keys else 0,
        completed_at=(
            _safe_dt(row["completed_at"])
            if ("completed_at" in keys and row["completed_at"])
            else None
        ),
        vps1_last_activity_at=(
            _safe_dt(row["vps1_last_activity_at"])
            if ("vps1_last_activity_at" in keys and row["vps1_last_activity_at"])
            else None
        ),
        state=_safe_state(row["state"]) if "state" in keys else State.NEW,
        batch_index=_safe_int(row["batch_index"]) if "batch_index" in keys else 0,
        batches_total=_safe_int(row["batches_total"]) if "batches_total" in keys else 0,
        batch_cap_bytes=_safe_int(row["batch_cap_bytes"]) if "batch_cap_bytes" in keys else 0,
        readd_cycles=_safe_int(row["readd_cycles"]) if "readd_cycles" in keys else 0,
        last_error=row["last_error"] if "last_error" in keys else "",
        created_at=(
            _safe_dt(row["created_at"])
            or dt.datetime.now(dt.timezone.utc)
        ) if "created_at" in keys else dt.datetime.now(dt.timezone.utc),
        updated_at=(
            _safe_dt(row["updated_at"])
            or dt.datetime.now(dt.timezone.utc)
        ) if "updated_at" in keys else dt.datetime.now(dt.timezone.utc),
        telegram_message_id=_safe_int(row["telegram_message_id"]) if "telegram_message_id" in keys else 0,
    )