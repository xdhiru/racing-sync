"""SQLite-backed implementation of the persistent state store.

Imports the state machine and schema from ``state_schema`` (never from
``state``, which re-exports this module) so the facade and the store cannot
form an import cycle. ``state.py`` re-exports the public names defined here.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import enum
import logging
import re
import sqlite3
import threading
import time
from pathlib import Path

from .coordinator_errors import AbandonedError
from .state_schema import (
    _FLAP_WINDOW_SECONDS,
    _SCHEMA_VERSION,
    _TORRENT_STATE_COLUMN_SPECS,
    _TORRENT_STATE_COLUMNS_NO_BLOB,
    SCHEMA_INDEXES,
    SCHEMA_TABLES,
    State,
    TorrentState,
    check_transition,
)

# Same logger name as the state.py facade this module backs, so log records
# still carry "racing_sync.state" and existing output stays unchanged.
log = logging.getLogger("racing_sync.state")


# Forget-tombstone lifetime: stamped rows stay invisible/refusing for a
# day (covers in-flight workers, retries and re-discovery), then GC.
_TOMBSTONE_TTL_SECONDS = 24 * 3600

# Display-list sharing window (seconds): Telegram taps/scrapes without an
# intervening row write share one active-tasks scan.
_DISPLAY_CACHE_S = 10.0


class UpsertResult(enum.Enum):
    """Outcome of StateStore.upsert()."""

    WRITTEN = "written"
    TOMBSTONED = "tombstoned"
    # Guarded upsert lost a race: the row is gone or its state/version moved
    # on since this snapshot was read. Nothing was written.
    CONFLICT = "conflict"


class StateStore:
    """SQLite-backed persistent store for TorrentState records.

    Async callers should run operations in a thread to keep the event loop
    unblocked. The DB is small (hundreds of rows at most) and writes are
    single-record, so the latency is fine.
    """

    def __init__(self, db_path: Path):
        self._lock = threading.RLock()
        self._tx_local = threading.local()
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
            # Without WAL, synchronous=NORMAL risks losing the last
            # committed transaction on a crash/power cut — enforce FULL
            # so every commit is durable. Loud because this usually means
            # the DB sits on a filesystem without WAL support.
            log.warning(
                "state DB journal_mode is %r, not WAL; forcing "
                "synchronous=FULL for durability (performance may degrade)",
                journal_mode,
            )
            self._conn.execute("PRAGMA synchronous=FULL")
        else:
            # WAL + NORMAL: commits are not fsynced until a checkpoint, so a
            # power cut can roll back the most recent transactions (the DB
            # itself stays consistent). The tradeoff is deliberate — every
            # per-tick upsert fsyncing would dominate the write path — and
            # survivable: a lost MOVING checkpoint replays from the last
            # durable one, and recovery re-derives rows from the clients.
            self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        # Bound WAL growth on long-lived coordinators doing per-tick upserts.
        try:
            self._conn.execute("PRAGMA journal_size_limit=67108864")
        except Exception:
            pass
        self._conn.executescript(SCHEMA_TABLES)
        # Upgrade path: diff the one column spec against the live table.
        # CREATE TABLE IF NOT EXISTS never backfills, so a pre-1.0.0 DB is
        # missing every column added since; _row_to_state tolerates that on
        # read, but upsert INSERTs every column and would OperationalError.
        # This MUST run before SCHEMA_INDEXES: ix_public_export_retry and
        # ix_active_updated reference migrated columns, so indexing a
        # pre-1.0.0 DB first raises "no such column" and the daemon cannot
        # start (B1).
        try:
            _have = {
                _r[1] for _r in self._conn.execute("PRAGMA table_info(torrent_state)").fetchall()
            }
        except Exception as e:  # noqa: BLE001
            _have = set()
            log.warning("state DB: could not read torrent_state columns: %s", e)
        for _name, _ddl in _TORRENT_STATE_COLUMN_SPECS:
            # A PRIMARY KEY column cannot be added by ALTER; only legacy
            # upgrades of non-key columns exist.
            if _name in _have or "PRIMARY KEY" in _ddl.upper():
                continue
            try:
                self._conn.execute(f"ALTER TABLE torrent_state ADD COLUMN {_name} {_ddl}")
            except Exception as e:
                # Duplicate-column is the expected rerun path (the diff above
                # already handles it); anything else (readonly, full-disk,
                # corrupt) must be loud.
                if "duplicate column" not in str(e).lower():
                    log.warning("state DB migration %r failed: %s", _name, e)
        # Indexes last: they need every migrated column above. Existing DBs
        # may also be missing an index whose columns only just appeared.
        self._conn.executescript(SCHEMA_INDEXES)
        # Retired states: QUERYING was folded into WAITING_INDEXER (the
        # wakeup hop that created it is gone; workers handle timer-elapsed
        # WAITING_INDEXER rows directly). A pre-upgrade row parked there
        # keeps its retry timers — both schedules are covered by
        # list_waiting_ready — so it resumes on the next tick instead of
        # falling into FAILED via _safe_state's unknown-state mapping.
        try:
            _cur = self._conn.execute(
                "UPDATE torrent_state SET state = 'waiting_indexer', "
                "updated_at = ? WHERE state = 'querying'",
                (dt.datetime.now(dt.UTC).isoformat(),),
            )
            if (_cur.rowcount or 0) > 0:
                log.info(
                    "state DB: migrated %d retired 'querying' row(s) to 'waiting_indexer'",
                    _cur.rowcount,
                )
        except Exception:
            pass
        # Single deleted_at sentinel: legacy NULL rows are writable but
        # invisible (every read requires ''), so normalize once at open.
        # A real tombstone is always a timestamp string, never NULL.
        try:
            _normed = (
                self._conn.execute(
                    "UPDATE torrent_state SET deleted_at = '' WHERE deleted_at IS NULL"
                ).rowcount
                or 0
            )
        except Exception:
            _normed = 0
        if _normed:
            log.warning("state DB: normalized %d legacy NULL deleted_at row(s) to ''", _normed)
        # Schema version marker: the column diff above is the migration
        # mechanism, but an operator (or a future fenced migration) needs to
        # see which schema this file was last opened with. A missing marker
        # is a pre-versioning DB that just got upgraded; a different one is
        # worth a line in the log in either direction.
        try:
            _stored = self.get_meta("schema_version")
            if _stored is None:
                self.set_meta("schema_version", str(_SCHEMA_VERSION))
            else:
                try:
                    _stored_i = int(_stored)
                except (TypeError, ValueError):
                    _stored_i = -1
                if _stored_i != _SCHEMA_VERSION:
                    log.warning(
                        "state DB schema_version %s != %d from this build; "
                        "applied the current column set",
                        _stored,
                        _SCHEMA_VERSION,
                    )
                    self.set_meta("schema_version", str(_SCHEMA_VERSION))
        except Exception:
            pass

    def _ensure_open(self) -> None:
        if self._closed:
            # Post-shutdown drains surface here (500s / frozen cards):
            # loud, so the shutdown ordering bug gets fixed, not masked.
            log.warning("StateStore used after close (post-shutdown drain?)")
            raise RuntimeError("StateStore is closed")

    def _invalidate_display(self) -> None:
        """Drop the cached active-tasks list after any row write.

        list_active_inflight() is display-only (Telegram renders); every
        structural write invalidates so taps never render pre-mutation
        rows, while tap bursts without intervening writes share one scan.
        """
        try:
            self._display_cache = None
        except Exception:
            pass

    @contextlib.contextmanager
    def _write_tx(self):
        """Atomic write transaction (BEGIN IMMEDIATE … COMMIT).

        Holds the store RLock for the whole sequence so a check-then-write
        (tombstone probe → write, version read → guarded update) cannot
        interleave with another thread, while IMMEDIATE reserves the DB
        against other connections. Re-entrant on the same thread (the RLock
        is held; nested sequences join the outer transaction) so public
        methods compose (transition → upsert) without deadlock.
        """
        self._ensure_open()
        with self._lock:
            _local = getattr(self, "_tx_local", None)
            if _local is None:
                _local = threading.local()
                try:
                    self._tx_local = _local
                except Exception:
                    pass
            depth = getattr(_local, "depth", 0) or 0
            if depth:
                yield
                return
            _local.depth = 1
            try:
                self._conn.execute("BEGIN IMMEDIATE")
            except Exception:
                _local.depth = 0
                raise
            try:
                try:
                    yield
                except BaseException:
                    # BaseException (KeyboardInterrupt, CancelledError) must
                    # unwind the transaction too, or the connection stays
                    # open and every later BEGIN IMMEDIATE fails.
                    try:
                        self._conn.execute("ROLLBACK")
                    except BaseException:
                        pass
                    raise
                # COMMIT lives inside the rollback guard: a failed COMMIT
                # (ENOSPC, SQLITE_BUSY after busy_timeout, disk I/O error)
                # leaves the connection inside a transaction, wedging every
                # write until restart. Roll back, then surface the error.
                try:
                    self._conn.execute("COMMIT")
                except BaseException:
                    try:
                        self._conn.execute("ROLLBACK")
                    except BaseException:
                        pass
                    raise
            finally:
                # Last-resort guard for a failed ROLLBACK above: never leave
                # the connection mid-transaction. No-op after a successful
                # COMMIT (in_transaction is False).
                try:
                    if self._conn.in_transaction:
                        self._conn.execute("ROLLBACK")
                except BaseException:
                    pass
                _local.depth = 0

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                if self._conn is not None:
                    self._conn.close()
            finally:
                self._conn = None
                self._closed = True

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- CRUD ----

    def get(self, source_infohash: str, include_blob: bool = True) -> TorrentState | None:
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        cols = "*" if include_blob else _TORRENT_STATE_COLUMNS_NO_BLOB
        with self._lock:
            row = self._conn.execute(
                f"SELECT {cols} FROM torrent_state WHERE source_infohash = ? AND deleted_at = ''",
                (norm,),
            ).fetchone()
            return _row_to_state(row) if row else None

    def get_many(self, hashes, *, include_blob: bool = False) -> dict:
        """Batch fetch: {lowercased hash: TorrentState | None}.

        One round-trip per ~500 hashes (SQLite variable cap) instead of
        one per hash — the SSD prune's stale sweep uses this so an
        admission with a big ledger doesn't pay thousands of WAL reads.
        Missing/tombstoned hashes map to None, exactly like get().
        """
        self._ensure_open()
        try:
            wanted: list[str] = []
            seen: set[str] = set()
            for h in hashes or []:
                try:
                    n = (h or "").strip().lower()
                except Exception:
                    continue
                if n and n not in seen:
                    seen.add(n)
                    wanted.append(n)
        except Exception:
            return {}
        out: dict = {n: None for n in wanted}
        if not wanted:
            return out
        cols = "*" if include_blob else _TORRENT_STATE_COLUMNS_NO_BLOB
        with self._lock:
            for i in range(0, len(wanted), 500):
                chunk = wanted[i : i + 500]
                try:
                    qmarks = ",".join(["?"] * len(chunk))
                    rows = self._conn.execute(
                        f"SELECT {cols} FROM torrent_state "
                        f"WHERE source_infohash IN ({qmarks}) "
                        "AND deleted_at = ''",
                        chunk,
                    ).fetchall()
                except Exception as e:
                    # Explicit signal, not a silent partial map: the only
                    # caller (SSD-prune sweep) treats missing as absent, so
                    # the contract stays fail-open — but it must be loud.
                    log.warning("get_many chunk failed (%d hashes): %s", len(chunk), e)
                    continue
                for r in rows or []:
                    try:
                        ts = _row_to_state(r)
                        out[(ts.source_infohash or "").lower()] = ts
                    except Exception:
                        continue
        return out

    def get_blob(self, source_infohash: str) -> bytes:
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT cross_seed_blob FROM torrent_state WHERE source_infohash = ? "
                "AND deleted_at = ''",
                (norm,),
            ).fetchone()
            if row and row["cross_seed_blob"]:
                return bytes(row["cross_seed_blob"])
            return b""

    def _is_tombstoned(self, source_infohash: str) -> bool:
        """True when the hash carries a live forget tombstone.

        Fail-CLOSED: a store error propagates instead of returning False —
        both callers (upsert, transition) run it inside a write txn where
        the follow-up read/write would fail the same way, so swallowing
        here could only resurrect a forgotten row, never save real work.
        """
        norm = (source_infohash or "").strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT deleted_at FROM torrent_state WHERE source_infohash = ?",
                (norm,),
            ).fetchone()
        return bool(row and row["deleted_at"])

    def upsert(
        self,
        ts: TorrentState,
        *,
        expected_version: int | None = None,
        expected_state: State | None = None,
    ) -> UpsertResult:
        """Write the full row, or refuse when the hash is tombstoned.

        Returns WRITTEN after the row is persisted (and the in-memory
        revision synced), or TOMBSTONED when a forget tombstone suppressed
        the write. Callers must treat only WRITTEN as proof of ingestion.

        Pass expected_version/expected_state when writing a row that was
        read earlier: a whole-row write then loses its race loudly
        (CONFLICT, nothing written) instead of clobbering a concurrent
        transition. A guarded write never inserts.
        """
        self._ensure_open()
        norm = (ts.source_infohash or "").strip().lower()
        if norm:
            # Canonical key: hashes are hex, case-insensitive. Normalizing
            # on write keeps get/tombstone/clear lookups consistent.
            ts.source_infohash = norm
        guarded = expected_version is not None or expected_state is not None
        with self._write_tx():
            if self._is_tombstoned(ts.source_infohash):
                # Forget tombstone: the operator abandoned this torrent — drop
                # the write instead of resurrecting it. Applies to creates too
                # (re-discovery while tombstoned stays away until GC expiry).
                return UpsertResult.TOMBSTONED
            if guarded:
                cur = self._conn.execute(
                    "SELECT state, version FROM torrent_state WHERE source_infohash = ?",
                    (ts.source_infohash,),
                ).fetchone()
                if cur is None:
                    return UpsertResult.CONFLICT
                if expected_version is not None:
                    try:
                        if int(cur["version"] or 0) != int(expected_version):
                            return UpsertResult.CONFLICT
                    except (TypeError, ValueError):
                        return UpsertResult.CONFLICT
                if expected_state is not None and cur["state"] != expected_state.value:
                    return UpsertResult.CONFLICT
            ts.updated_at = dt.datetime.now(dt.UTC)
            row = ts.to_row()
            cols = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            updates = []
            for k in row:
                if k in ("source_infohash", "created_at", "deleted_at"):
                    # deleted_at excluded: a stale in-memory row upserted
                    # after forget()/tombstone() must never clear the
                    # stamp via ON CONFLICT (only clear_tombstone lifts).
                    continue
                if k == "version":
                    # Monotonic revision: concurrent writers bump past each
                    # other instead of resetting (transition() guards on it).
                    updates.append("version = torrent_state.version + 1")
                elif k == "cross_seed_blob":
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
            self._invalidate_display()
            # Sync the revision back: an upsert-then-transition sequence on
            # the same object must not trip the transition version guard
            # below (only *other* writers invalidate this snapshot).
            try:
                _vrow = self._conn.execute(
                    "SELECT version FROM torrent_state WHERE source_infohash = ?",
                    (norm,),
                ).fetchone()
                if _vrow is not None:
                    ts.version = int(_vrow["version"] or 0)
            except Exception:
                pass
        return UpsertResult.WRITTEN

    def list_by_state(self, *states: State) -> list[TorrentState]:
        self._ensure_open()
        if not states:
            return []
        qmarks = ",".join(["?"] * len(states))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state IN ({qmarks}) AND deleted_at = '' ORDER BY updated_at",
                [s.value for s in states],
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def list_indexer_ready(self, now: dt.datetime | None = None) -> list[TorrentState]:
        """Rows in WAITING_INDEXER whose retry timer has elapsed.

        Used by the coordinator tick to decide which rows to wake up and
        re-query the download-target indexers.
        """
        return self.list_waiting_ready(now=now, which="indexer")

    def list_waiting_ready(
        self,
        now: dt.datetime | None = None,
        *,
        which: str = "both",
    ) -> list[TorrentState]:
        """WAITING_INDEXER rows whose indexer and/or public timer elapsed.

        One scan instead of two per tick (the two schedules are
        independent — a row may be due on either, both, or neither).
        `which` is "indexer", "public", or "both" (default).
        """
        self._ensure_open()
        now = now or dt.datetime.now(dt.UTC)
        clauses: list[str] = []
        if which in ("indexer", "both"):
            clauses.append("(indexer_next_retry_at != '' AND indexer_next_retry_at <= ?)")
        if which in ("public", "both"):
            clauses.append(
                "(public_export_next_retry_at != '' AND public_export_next_retry_at <= ?)"
            )
        if not clauses:
            return []
        where = " OR ".join(clauses)
        params = tuple([now.isoformat()] * len(clauses))
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state = 'waiting_indexer' "
                f"AND deleted_at = '' AND ({where}) "
                "ORDER BY indexer_next_retry_at, public_export_next_retry_at",
                params,
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def list_active_inflight(self) -> list[TorrentState]:
        """Rows that should appear in the active-tasks Telegram message.

        Excludes DONE and FAILED — those have a settled detail message in
        chat history and should not clutter the live list.

        Display path: results are shared for _DISPLAY_CACHE_S across taps
        without an intervening row write (every structural write
        invalidates), so tap storms and scrapes don't each pay a full
        table scan + ORDER BY. Scheduler/worker paths use all_active()
        uncached — they must see fresh rows.
        """
        self._ensure_open()
        try:
            _mc = getattr(self, "_display_cache", None)
            if (
                isinstance(_mc, tuple)
                and len(_mc) == 2
                and time.monotonic() - float(_mc[0]) < _DISPLAY_CACHE_S
            ):
                return list(_mc[1])
        except Exception:
            pass
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state WHERE state NOT IN ('done','failed') "
                "AND deleted_at = '' "
                "ORDER BY updated_at DESC"
            ).fetchall()
            out = [_row_to_state(r) for r in rows]
        try:
            self._display_cache = (time.monotonic(), out)
        except Exception:
            pass
        return out

    def get_telegram_message_id(self, source_infohash: str) -> int | None:
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT telegram_message_id FROM torrent_state WHERE source_infohash = ? "
                "AND deleted_at = ''",
                (norm,),
            ).fetchone()
            if row is None or not row["telegram_message_id"]:
                return None
            return int(row["telegram_message_id"])

    def set_telegram_message_id(self, source_infohash: str, message_id: int) -> None:
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        with self._lock:
            self._conn.execute(
                "UPDATE torrent_state SET telegram_message_id = ?, "
                "updated_at = ? WHERE source_infohash = ? AND deleted_at = ''",
                (message_id, dt.datetime.now(dt.UTC).isoformat(), norm),
            )

    def all_active(
        self, include_blob: bool = False, limit: int | None = None
    ) -> list[TorrentState]:
        self._ensure_open()
        cols = _TORRENT_STATE_COLUMNS_NO_BLOB if not include_blob else "*"
        with self._lock:
            if limit is not None:
                try:
                    limit = int(limit)
                except (TypeError, ValueError):
                    limit = None
            if limit is not None and limit > 0:
                rows = self._conn.execute(
                    f"SELECT {cols} FROM torrent_state WHERE state != 'done' AND state != 'failed' AND deleted_at = '' "
                    "ORDER BY updated_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [_row_to_state(r) for r in rows]
            rows = self._conn.execute(
                f"SELECT {cols} FROM torrent_state WHERE state != 'done' AND state != 'failed' AND deleted_at = '' "
                "ORDER BY updated_at"
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def all(
        self, include_blob: bool = False, limit: int | None = None, offset: int = 0
    ) -> list[TorrentState]:
        self._ensure_open()
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
                        f"SELECT {cols} FROM torrent_state WHERE deleted_at = '' ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                        (limit, max(0, offset)),
                    ).fetchall()
                    return [_row_to_state(r) for r in rows]
            rows = self._conn.execute(
                f"SELECT {cols} FROM torrent_state WHERE deleted_at = '' ORDER BY updated_at DESC"
            ).fetchall()
            return [_row_to_state(r) for r in rows]

    def all_card_prefill(self) -> list[tuple[str, int, str]]:
        """Narrow startup prefill for Telegram card caches.

        `(source_infohash, telegram_message_id, state)` only — the bot
        startup path must not hydrate full rows (blobs/notes/errors) for
        thousands of torrents just to learn "already sent".
        """
        self._ensure_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_infohash, telegram_message_id, state "
                "FROM torrent_state WHERE deleted_at = ''"
            ).fetchall()
        out: list[tuple[str, int, str]] = []
        for r in rows:
            try:
                out.append(
                    (
                        str(r["source_infohash"]),
                        int(r["telegram_message_id"] or 0),
                        str(r["state"] or ""),
                    )
                )
            except Exception:
                continue
        return out

    def count_by_state(self) -> dict[str, int]:
        """Live-table row counts per state (narrow GROUP BY, no row scan).

        Feeds `/metrics` without the 5000-row truncation of sampled
        scans — DONE/FAILED totals live outside `all_active`.
        """
        self._ensure_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS n FROM torrent_state "
                "WHERE deleted_at = '' GROUP BY state"
            ).fetchall()
        out: dict[str, int] = {}
        for r in rows:
            try:
                out[str(r["state"])] = int(r["n"])
            except Exception:
                continue
        return out

    def find_by_name(self, source_name: str) -> list[TorrentState]:
        self._ensure_open()
        clean_name = (source_name or "").strip()
        if not clean_name:
            # Empty pattern would degrade to LIKE '%' (match everything).
            return []
        for ext in (".mkv", ".mp4", ".avi", ".ts", ".m4v", ".torrent"):
            if clean_name.lower().endswith(ext):
                clean_name = clean_name[: -len(ext)].strip()
                break
        if not clean_name:
            # ".mkv" alone strips to nothing — same '%' hazard.
            return []
        stripped_name = re.sub(r"\s*\[[^\]]+\]\s*$", "", clean_name).strip()
        if not stripped_name:
            # "[FL]" alone strips to nothing — same '%' hazard.
            return []

        def _escape_like(s: str) -> str:
            # Escape LIKE wildcards so `100%` / `S01_E01` match literally.
            return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_TORRENT_STATE_COLUMNS_NO_BLOB} FROM torrent_state "
                "WHERE deleted_at = '' AND (source_name = ? OR source_name = ? "
                "OR source_name = ? OR source_name LIKE ? ESCAPE '\\' "
                "OR source_name LIKE ? ESCAPE '\\') "
                "ORDER BY updated_at DESC LIMIT 200",
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
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        # Never hard-delete a live row: a later whole-row upsert would
        # resurrect it as a zombie. Stamp a tombstone instead (invisible
        # to reads, refusing writes, GC'd after TTL); hard-delete only
        # when there is no live row to protect. A tombstone failure
        # re-raises instead of falling through to a live-row DELETE.
        if self.tombstone(norm):
            return
        with self._write_tx():
            # tombstone() also returns False for an already-stamped row; a
            # bare DELETE here would erase that tombstone and reopen the
            # resurrection window. Only an absent row may fall through.
            if self._is_tombstoned(norm):
                return
            self._conn.execute("DELETE FROM torrent_state WHERE source_infohash = ?", (norm,))
            self._invalidate_display()

    def hard_delete(self, source_infohash: str) -> bool:
        """Delete a row outright, tombstone or not; True when one was removed.

        ONLY for forget-without-remember (never-seen semantics): the
        caller must have stopped the row's workers and removed its dest
        entries first, otherwise a stale in-flight upsert resurrects it
        as a zombie — exactly what delete()/tombstone() protects
        against. No ignore entry is touched (the caller owns that).
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        with self._lock:
            cur = self._conn.execute("DELETE FROM torrent_state WHERE source_infohash = ?", (norm,))
            if (cur.rowcount or 0) > 0:
                self._invalidate_display()
            return (cur.rowcount or 0) > 0

    def tombstone(self, source_infohash: str) -> bool:
        """Stamp a forget tombstone; True when a live row was stamped.

        The row stays (invisible to reads, refusing writes) for
        _TOMBSTONE_TTL_SECONDS so in-flight workers, retries and
        re-discovery cannot resurrect it, then gc_tombstones() hard-deletes
        it. Idempotent: re-stamping refreshes the stamp.
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        now = dt.datetime.now(dt.UTC).isoformat()
        with self._write_tx():
            cur = self._conn.execute(
                "UPDATE torrent_state SET deleted_at = ?, updated_at = ?, "
                "version = version + 1 "
                "WHERE source_infohash = ? AND deleted_at = ''",
                (now, now, norm),
            )
            if (cur.rowcount or 0) > 0:
                self._invalidate_display()
                return True
            # Already tombstoned: refresh the stamp (extends the TTL) but
            # report False — no live row was stamped by this call.
            self._conn.execute(
                "UPDATE torrent_state SET deleted_at = ?, updated_at = ?, "
                "version = version + 1 "
                "WHERE source_infohash = ?",
                (now, now, norm),
            )
            self._invalidate_display()
            return False

    def clear_tombstone(self, source_infohash: str) -> bool:
        """Lift a forget tombstone early; True when one was cleared.

        Explicit operator override for `unignore`: the hash becomes
        re-ingestible immediately instead of waiting out the TTL.
        In-flight workers may still hold a stale in-memory row, but the
        coordinator-level `_abandoned()` re-check bounds that race.
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        with self._lock:
            cur = self._conn.execute(
                "UPDATE torrent_state SET deleted_at = '', updated_at = ?, "
                "version = version + 1 "
                "WHERE source_infohash = ? AND deleted_at != ''",
                (dt.datetime.now(dt.UTC).isoformat(), norm),
            )
            if (cur.rowcount or 0) > 0:
                self._invalidate_display()
            return (cur.rowcount or 0) > 0

    def gc_tombstones(self, ttl_seconds: int = _TOMBSTONE_TTL_SECONDS) -> int:
        """Hard-delete tombstones older than the TTL. Returns rows removed."""
        self._ensure_open()
        try:
            cutoff = (
                dt.datetime.now(dt.UTC) - dt.timedelta(seconds=max(0, int(ttl_seconds or 0)))
            ).isoformat()
        except (TypeError, ValueError):
            return 0
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM torrent_state WHERE deleted_at != '' AND deleted_at <= ?",
                (cutoff,),
            )
            if (cur.rowcount or 0) > 0:
                self._invalidate_display()
            return cur.rowcount or 0

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
                (norm, source_name or "", dt.datetime.now(dt.UTC).isoformat()),
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
                {
                    "source_infohash": r["source_infohash"],
                    "source_name": r["source_name"],
                    "created_at": r["created_at"],
                }
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
                f"{r['source_name']} ({(r['source_infohash'] or '')[:10]})" for r in matches[:5]
            )
            raise LookupError(
                f"ignore target {target!r} matches {len(matches)} entries: {preview}; "
                "use a 40-char infohash to pick one"
            )
        raise LookupError(f"no ignored torrent matching {target!r}")

    # ---- convenience ----

    def transition(
        self, ts: TorrentState, dst: State, *, error: str = "", batch_index: int | None = None
    ) -> None:
        """Move a row to `dst`, refusing concurrent-move clobbers.

        The whole check-act sequence runs in one IMMEDIATE transaction:
        the DB state AND revision are re-read and must still equal the
        in-memory snapshot, otherwise another writer (API retry, recovery,
        a second worker, or a bare upsert park) moved the row and this
        transition raises AbandonedError instead of silently overwriting
        its state. The write itself is guarded
        (WHERE state+version match) so a tombstone or move landing
        between the read and the write fails the rowcount instead of
        diverging memory from the DB. Callers holding only stale
        snapshots therefore fail loudly (worker wrapper unwinds quietly;
        API maps to 409).
        """
        if self._is_tombstoned(ts.source_infohash):
            # Forget won this row: transitioning would upsert-resurrect it.
            raise AbandonedError(
                f"row tombstoned (forgotten?) for {(ts.source_infohash or '')[:10]}; "
                f"refusing {ts.state.value} -> {dst.value}"
            )
        check_transition(ts.state, dst)
        src = ts.state
        now = dt.datetime.now(dt.UTC)
        try:
            expected_version = int(ts.version or 0)
        except (TypeError, ValueError):
            expected_version = 0
        # Snapshot everything this method mutates: a failed write
        # (SQLITE_FULL, locked, lost race) must leave the in-memory
        # object identical to the DB row.
        _snapshot = (
            ts.state,
            ts.last_error,
            ts.move_stage,
            ts.move_plan,
            ts.indexer_first_queried_at,
            ts.indexer_next_retry_at,
            ts.indexer_attempts,
            ts.readd_first_attempted_at,
            ts.readd_next_retry_at,
            ts.readd_attempts,
            ts.failed_retries,
            ts.completed_at,
            ts.batch_index,
            ts.readd_cycles,
            ts.version,
            ts.updated_at,
        )
        try:
            with self._write_tx():
                _norm = (ts.source_infohash or "").strip().lower()
                _cur = self._conn.execute(
                    "SELECT state, version, deleted_at FROM torrent_state "
                    "WHERE source_infohash = ?",
                    (_norm,),
                ).fetchone()
                if _cur is not None:
                    if _cur["deleted_at"]:
                        raise AbandonedError(
                            f"row tombstoned (forgotten?) for "
                            f"{(ts.source_infohash or '')[:10]}; "
                            f"refusing {src.value} -> {dst.value}"
                        )
                    _db_state = str(_cur["state"] or "")
                    if _db_state != src.value:
                        raise AbandonedError(
                            f"row moved concurrently for "
                            f"{(ts.source_infohash or '')[:10]} "
                            f"(expected {src.value}, found {_db_state}); "
                            f"refusing {src.value} -> {dst.value}"
                        )
                    try:
                        _db_version = int(_cur["version"] or 0)
                    except (TypeError, ValueError):
                        _db_version = -1
                    if _db_version != expected_version:
                        raise AbandonedError(
                            f"row written concurrently for "
                            f"{(ts.source_infohash or '')[:10]} "
                            f"(revision {_db_version} != {expected_version}); "
                            f"refusing {src.value} -> {dst.value}"
                        )
                ts.state = dst
                ts.last_error = error
                ts.updated_at = now
                if src == State.MOVING or dst == State.MOVING:
                    # The move checkpoint is scoped to a single MOVING
                    # episode: leaving it (or starting a fresh one) must
                    # never replay a stale destructive plan.
                    ts.move_stage = ""
                    ts.move_plan = ""
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
                if _cur is None:
                    # Brand-new row: nothing to clobber, plain insert path.
                    # A tombstone that landed since the pre-check makes
                    # upsert refuse; never report success without a row.
                    if self.upsert(ts) is not UpsertResult.WRITTEN:
                        raise AbandonedError(
                            f"row insert refused (tombstoned) for "
                            f"{ts.source_infohash or ''}; "
                            f"refusing {src.value} -> {dst.value}"
                        )
                else:
                    self._transition_write(ts, src, expected_version)
                    ts.version = expected_version + 1
        except Exception:
            (
                ts.state,
                ts.last_error,
                ts.move_stage,
                ts.move_plan,
                ts.indexer_first_queried_at,
                ts.indexer_next_retry_at,
                ts.indexer_attempts,
                ts.readd_first_attempted_at,
                ts.readd_next_retry_at,
                ts.readd_attempts,
                ts.failed_retries,
                ts.completed_at,
                ts.batch_index,
                ts.readd_cycles,
                ts.version,
                ts.updated_at,
            ) = _snapshot
            raise
        log.info("state %s -> %s for %s", ts.source_infohash[:8], dst.value, ts.source_name)

    def _transition_write(self, ts: TorrentState, src: State, expected_version: int) -> None:
        """Persist a transitioned row under the state+version guard.

        Same column semantics as upsert's conflict branch (blob kept when
        the incoming bytes are empty), but as a guarded UPDATE: zero
        affected rows means a tombstone or concurrent move landed between
        the guard read and this write — AbandonedError, never silent
        divergence. Caller owns the surrounding transaction.
        """
        row = ts.to_row()
        sets: list[str] = []
        params: list = []
        for k, v in row.items():
            if k in ("source_infohash", "created_at", "deleted_at", "version"):
                continue
            if k == "cross_seed_blob":
                sets.append(f"{k} = CASE WHEN length(?) > 0 THEN ? ELSE torrent_state.{k} END")
                params.extend([v, v])
            else:
                sets.append(f"{k} = ?")
                params.append(v)
        sets.append("version = torrent_state.version + 1")
        cur = self._conn.execute(
            f"UPDATE torrent_state SET {', '.join(sets)} "
            "WHERE source_infohash = ? AND state = ? AND version = ? "
            "AND deleted_at = ''",
            (*params, (ts.source_infohash or "").strip().lower(), src.value, expected_version),
        )
        if (cur.rowcount or 0) <= 0:
            raise AbandonedError(
                f"row changed under transition for "
                f"{(ts.source_infohash or '')[:10]}; "
                f"refusing {src.value} -> {ts.state.value}"
            )
        self._invalidate_display()

    def _wal_checkpoint(self) -> None:
        """Best-effort TRUNCATE checkpoint after heavy log prunes: bounds
        WAL growth on long-lived coordinators doing per-tick upserts."""
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass

    def append_log(self, level: str, message: str, source_infohash: str | None = None) -> None:
        self._ensure_open()
        # Single transaction: a crash between INSERT and prune must never
        # leave the table over cap (or a prune without its insert).
        # The checkpoint runs after COMMIT — never inside the txn.
        _pruned = False
        with self._write_tx():
            self._conn.execute(
                "INSERT INTO run_log (ts, source_infohash, level, message) VALUES (?,?,?,?)",
                (dt.datetime.now(dt.UTC).isoformat(), source_infohash, level, message),
            )
            self._log_append_counter += 1
            if self._log_append_counter >= 500:
                self._log_append_counter = 0
                self._conn.execute(
                    "DELETE FROM run_log WHERE id NOT IN (SELECT id FROM run_log ORDER BY id DESC LIMIT 5000)"
                )
                _pruned = True
        if _pruned:
            self._wal_checkpoint()

    def prune_logs(self, max_records: int = 5000) -> None:
        self._ensure_open()
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
            self._wal_checkpoint()

    def iter_logs(self, limit: int = 200) -> list[sqlite3.Row]:
        self._ensure_open()
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
        self._ensure_open()
        with self._lock:
            cur = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cur.fetchone()
            if row is None:
                return default
            return str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        """Set or update a metadata key/value."""
        self._ensure_open()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )

    # ---- narrow single-column updates (no whole-row overwrite) ----

    def set_fuse_verified(self, source_infohash: str, verified: bool = True) -> bool:
        """Stamp/clear the fuse-verified flag; True when a row was updated.

        Narrow UPDATE so a verification probe never clobbers concurrent
        worker mutations to the rest of the row. Advisory-only: no
        version bump (see update_columns).
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        with self._lock:
            cur = self._conn.execute(
                "UPDATE torrent_state SET fuse_verified = ?, updated_at = ? "
                "WHERE source_infohash = ? AND deleted_at = ''",
                (1 if verified else 0, dt.datetime.now(dt.UTC).isoformat(), norm),
            )
            return (cur.rowcount or 0) > 0

    def set_blob(self, source_infohash: str, blob: bytes) -> bool:
        """Persist fetched .torrent bytes; True when a row was updated.

        Refuses empty blobs (never wipe a good cached copy with nothing).
        No version bump: bytes are content-addressed (same infohash means
        identical bytes), so a concurrent transition snapshot holds the
        same content and must not fail over it.
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm or not blob:
            return False
        with self._lock:
            cur = self._conn.execute(
                "UPDATE torrent_state SET cross_seed_blob = ?, updated_at = ? "
                "WHERE source_infohash = ? AND deleted_at = ''",
                (bytes(blob), dt.datetime.now(dt.UTC).isoformat(), norm),
            )
            return (cur.rowcount or 0) > 0

    def set_racing_blob(self, infohash: str, blob: bytes) -> bool:
        """Persist a racing-client .torrent by hash (no torrent_state row needed).

        Companion to `set_blob` (which is row-scoped): sibling racing
        torrents of a content group have no row, but the janitor must
        preserve their bytes before deleting the VPS1 copy.
        """
        self._ensure_open()
        norm = (infohash or "").strip().lower()
        if not norm or not blob:
            return False
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO torrent_blob (infohash, blob, updated_at) "
                    "VALUES (?, ?, ?) ON CONFLICT(infohash) DO UPDATE SET "
                    "blob = excluded.blob, updated_at = excluded.updated_at",
                    (norm, bytes(blob), dt.datetime.now(dt.UTC).isoformat()),
                )
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("state DB: could not cache .torrent for %s: %s", norm[:10], e)
            return False

    def get_racing_blob(self, infohash: str) -> bytes:
        """Cached racing-client .torrent bytes, b"" when absent."""
        self._ensure_open()
        norm = (infohash or "").strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT blob FROM torrent_blob WHERE infohash = ?", (norm,)
            ).fetchone()
            if row and row["blob"]:
                return bytes(row["blob"])
            return b""

    def delete_racing_blob(self, infohash: str) -> None:
        self._ensure_open()
        norm = (infohash or "").strip().lower()
        with self._lock:
            self._conn.execute("DELETE FROM torrent_blob WHERE infohash = ?", (norm,))

    # Columns a narrow update may touch (everything but identity,
    # creation, and the tombstone stamp itself).
    _UPDATABLE_COLUMNS = frozenset(
        {
            "dest_infohash",
            "source_name",
            "source_tracker",
            "source_announce_url",
            "classification_kind",
            "total_bytes",
            "save_path",
            "cross_seed_infohash",
            "cross_seed_source",
            "cross_seed_blob",
            "injected_private_hashes",
            "indexer_first_queried_at",
            "indexer_next_retry_at",
            "indexer_attempts",
            "public_export_first_attempted_at",
            "public_export_next_retry_at",
            "public_export_attempts",
            "fuse_wait_first_at",
            "fuse_verified",
            "force_direct",
            "skipped",
            "readd_first_attempted_at",
            "readd_next_retry_at",
            "readd_attempts",
            "failed_retries",
            "completed_at",
            "vps1_last_activity_at",
            "state",
            "move_stage",
            "move_plan",
            "batch_index",
            "batches_total",
            "batch_cap_bytes",
            "readd_cycles",
            "last_error",
            "telegram_message_id",
            "updated_at",
        }
    )

    # Narrow writes touching only advisory/display columns below skip the
    # version bump: a background timer/display park must not invalidate
    # an in-flight transition snapshot (spurious AbandonedError → 409 /
    # worker unwind). The loss direction is harmless — advisory columns
    # are refreshed every tick, while a transition clobbering them loses
    # nothing durable. Everything else (routing, cursors, operator
    # intent, state) keeps the bump so concurrent moves still fail loud.
    _UNVERSIONED_COLUMNS = frozenset(
        {
            "vps1_last_activity_at",
            "fuse_verified",
            "telegram_message_id",
            "indexer_first_queried_at",
            "indexer_next_retry_at",
            "indexer_attempts",
            "public_export_first_attempted_at",
            "public_export_next_retry_at",
            "public_export_attempts",
            "readd_first_attempted_at",
            "readd_next_retry_at",
            "readd_attempts",
            "readd_cycles",
            "completed_at",
        }
    )

    def update_columns(
        self,
        source_infohash: str,
        mapping: dict | None = None,
        *,
        expected_state: State | None = None,
        expected_version: int | None = None,
    ) -> int:
        """Narrow UPDATE of a few columns; returns the new revision (0 = no row).

        Unlike upsert() this never touches `state` unless asked and never
        inserts: timer/display parks use it so a concurrent state move is
        preserved (optionally enforced via expected_state/version, which
        turn a lost race into 0 instead of a clobber). Tombstoned rows
        never match. Datetimes serialize to ISO, States to values.

        The revision bumps only when a state-affecting column is written
        (see _VERSIONED_COLUMNS vs _UNVERSIONED_COLUMNS): advisory-only
        parks must not invalidate in-flight transition snapshots.
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm or not isinstance(mapping, dict):
            return 0
        sets: list[str] = []
        params: list = []
        bump = False
        for k, v in mapping.items():
            # updated_at/version are managed by this method itself —
            # accepting them would duplicate the SET column.
            if k not in self._UPDATABLE_COLUMNS or k in ("updated_at", "version"):
                continue
            try:
                if isinstance(v, State):
                    v = v.value
                elif isinstance(v, dt.datetime):
                    v = v.isoformat()
                elif isinstance(v, bool):
                    v = int(v)
            except Exception:
                continue
            sets.append(f"{k} = ?")
            params.append(v)
            if k not in self._UNVERSIONED_COLUMNS:
                bump = True
        if not sets:
            return 0
        clauses = ["source_infohash = ?", "deleted_at = ''"]
        params.append(norm)
        if expected_state is not None:
            clauses.append("state = ?")
            params.append(expected_state.value)
        if expected_version is not None:
            try:
                clauses.append("version = ?")
                params.append(int(expected_version))
            except (TypeError, ValueError):
                return 0
        now = dt.datetime.now(dt.UTC).isoformat()
        _ver = ", version = version + 1" if bump else ""
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE torrent_state SET {', '.join(sets)}, "
                f"updated_at = ?{_ver} "
                f"WHERE {' AND '.join(clauses)}",
                (*params[: len(sets)], now, *params[len(sets) :]),
            )
            if (cur.rowcount or 0) <= 0:
                return 0
            if bump:
                # Advisory-only parks skip invalidation too (like the
                # version bump): the 10s display TTL bounds their
                # staleness, and per-tick timer churn must not nuke the
                # shared scan.
                self._invalidate_display()
            row = self._conn.execute(
                "SELECT version FROM torrent_state WHERE source_infohash = ?",
                (norm,),
            ).fetchone()
            try:
                return int(row["version"] or 0) if row else 0
            except (TypeError, ValueError):
                return 0

    def set_vps1_activity(self, source_infohash: str, when: dt.datetime | None = None) -> bool:
        """Stamp swarm-activity observation; True when a row was updated.

        Narrow UPDATE so the janitor's activity bump never clobbers
        concurrent worker mutations to the rest of the row. Advisory-only:
        no version bump, so background stamps never invalidate in-flight
        transition snapshots.
        """
        self._ensure_open()
        norm = (source_infohash or "").strip().lower()
        if not norm:
            return False
        try:
            stamp = (when or dt.datetime.now(dt.UTC)).isoformat()
        except Exception:
            return False
        with self._lock:
            cur = self._conn.execute(
                "UPDATE torrent_state SET vps1_last_activity_at = ?, "
                "updated_at = ? "
                "WHERE source_infohash = ? AND deleted_at = ''",
                (stamp, dt.datetime.now(dt.UTC).isoformat(), norm),
            )
            return (cur.rowcount or 0) > 0

    def list_public_retry_ready(self, now: dt.datetime | None = None) -> list[TorrentState]:
        """Rows in WAITING_INDEXER whose public-export timer has elapsed.

        The public-export schedule is independent of the download-target
        indexer schedule: these rows retry the VPS1 export indefinitely
        instead of sharing the indexer's max-age clock.
        """
        return self.list_waiting_ready(now=now, which="public")


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
    except (TypeError, ValueError, OverflowError):
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
        parsed = dt.datetime.fromisoformat(s)
        # Legacy naive cells break aware arithmetic (now_utc - completed).
        # Normalize to UTC instead of raising per-row forever.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.UTC)
        return parsed
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
        classification_kind=row["classification_kind"]
        if "classification_kind" in keys
        else "unknown",
        total_bytes=_safe_int(row["total_bytes"]) if "total_bytes" in keys else 0,
        save_path=row["save_path"] if "save_path" in keys else "",
        cross_seed_infohash=row["cross_seed_infohash"] if "cross_seed_infohash" in keys else "",
        cross_seed_source=row["cross_seed_source"] if "cross_seed_source" in keys else "",
        cross_seed_blob=blob_bytes,
        injected_private_hashes=row["injected_private_hashes"]
        if "injected_private_hashes" in keys
        else "",
        indexer_first_queried_at=_safe_dt(idx_first),
        indexer_next_retry_at=_safe_dt(idx_next),
        indexer_attempts=_safe_int(idx_attempts),
        public_export_first_attempted_at=(
            _safe_dt(row["public_export_first_attempted_at"])
            if (
                "public_export_first_attempted_at" in keys
                and row["public_export_first_attempted_at"]
            )
            else None
        ),
        public_export_next_retry_at=(
            _safe_dt(row["public_export_next_retry_at"])
            if ("public_export_next_retry_at" in keys and row["public_export_next_retry_at"])
            else None
        ),
        public_export_attempts=(
            _safe_int(row["public_export_attempts"]) if "public_export_attempts" in keys else 0
        ),
        fuse_wait_first_at=(
            _safe_dt(row["fuse_wait_first_at"])
            if ("fuse_wait_first_at" in keys and row["fuse_wait_first_at"])
            else None
        ),
        fuse_verified=(_safe_int(row["fuse_verified"]) if "fuse_verified" in keys else 0),
        force_direct=_safe_int(row["force_direct"]) if "force_direct" in keys else 0,
        skipped=_safe_int(row["skipped"]) if "skipped" in keys else 0,
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
        move_stage=(str(row["move_stage"] or "") if "move_stage" in keys else ""),
        move_plan=(str(row["move_plan"] or "") if "move_plan" in keys else ""),
        batch_index=_safe_int(row["batch_index"]) if "batch_index" in keys else 0,
        batches_total=_safe_int(row["batches_total"]) if "batches_total" in keys else 0,
        batch_cap_bytes=_safe_int(row["batch_cap_bytes"]) if "batch_cap_bytes" in keys else 0,
        readd_cycles=_safe_int(row["readd_cycles"]) if "readd_cycles" in keys else 0,
        version=_safe_int(row["version"]) if "version" in keys else 0,
        last_error=row["last_error"] if "last_error" in keys else "",
        created_at=(_safe_dt(row["created_at"]) or dt.datetime.now(dt.UTC))
        if "created_at" in keys
        else dt.datetime.now(dt.UTC),
        updated_at=(_safe_dt(row["updated_at"]) or dt.datetime.now(dt.UTC))
        if "updated_at" in keys
        else dt.datetime.now(dt.UTC),
        telegram_message_id=_safe_int(row["telegram_message_id"])
        if "telegram_message_id" in keys
        else 0,
        deleted_at=(
            _safe_dt(row["deleted_at"]) if ("deleted_at" in keys and row["deleted_at"]) else None
        ),
    )
