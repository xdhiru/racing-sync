"""State machine, row model and SQLite schema for the persistent state store.

Split out of ``state.py`` so the store implementation (``state_store.py``)
can import the schema without the public facade importing it back; ``state.py``
re-exports everything here as its public surface.
"""

from __future__ import annotations

import datetime as dt
import enum
from dataclasses import dataclass, field, fields as dataclass_fields

# --------------------------------------------------------------------------- #
# State machine
# --------------------------------------------------------------------------- #


class State(str, enum.Enum):  # noqa: UP042  (StrEnum changes str(Enum); .value is the contract)
    """Lifecycle of a torrent we are processing on VPS2.

    SSD download + rclone move + fuse re-add has these stages:

      NEW                 we noticed the torrent on VPS1, need to make decisions
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
    WAITING_INDEXER = "waiting_indexer"
    WAITING_DISK = "waiting_disk"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    MOVING = "moving"
    RE_ADDING = "re_adding"
    DONE = "done"
    FAILED = "failed"


# Allowed transitions (everything else raises ValueError).
# Pre-SSD states (NEW/WAITING_INDEXER/WAITING_DISK) may fast-track
# straight to DONE when a manual fuse seed is detected: the operator added
# the same infohash on VPS2 pointing at the fuse mount (any category) with
# verified bytes, so no SSD download / rclone move is needed.
ALLOWED: dict[State, set[State]] = {
    State.NEW: {
        State.WAITING_INDEXER,
        State.WAITING_DISK,
        State.QUEUED,
        State.DOWNLOADING,
        State.MOVING,
        State.RE_ADDING,
        State.DONE,
        State.FAILED,
    },
    State.WAITING_INDEXER: {
        State.WAITING_DISK,
        State.QUEUED,
        State.DONE,
        State.FAILED,
        State.RE_ADDING,
    },
    State.WAITING_DISK: {
        State.QUEUED,
        State.DOWNLOADING,
        State.DONE,
        State.FAILED,
        State.RE_ADDING,
    },
    State.QUEUED: {
        State.DOWNLOADING,
        State.MOVING,
        State.WAITING_DISK,
        State.RE_ADDING,
        State.DONE,
        State.FAILED,
    },
    # DOWNLOADING -> RE_ADDING is the /inject manual preempt: the operator
    # moved the bytes to the remote by hand, fuse verification passed, so the
    # SSD download is abandoned (dest entry removed, reservation released)
    # and the row goes fuse-gated RE_ADDING instead of finishing the download.
    # DOWNLOADING -> DONE / MOVING -> DONE exist for shortened chains (no
    # transfer, no seed). `chain.Chain` decides which edge a deployment
    # actually takes; the table only tolerates them.
    State.DOWNLOADING: {State.MOVING, State.DONE, State.QUEUED, State.FAILED, State.RE_ADDING},
    State.MOVING: {State.RE_ADDING, State.DONE, State.DOWNLOADING, State.FAILED},
    State.RE_ADDING: {State.DONE, State.MOVING, State.FAILED},
    # DONE -> MOVING is the fresh-DB self-heal: recovery may have adopted an
    # SSD-complete torrent as DONE (e.g. stale DB, misclassified save_path);
    # the late cross-seed guard demotes it back to MOVING so the rclone move
    # runs before any fuse injection. DONE -> RE_ADDING stays for lost fuses.
    State.DONE: {State.RE_ADDING, State.MOVING},
    # Telegram /inject (operator moved the bytes to the remote by
    # hand): pre-SSD, failed AND SSD-active rows may jump to RE_ADDING, where
    # the fuse gate re-verifies everything before any skip_check inject.
    # SSD-active rows abandon their partial download (dest entry removed,
    # reservation released) — no /cancel round-trip needed.
    State.FAILED: {State.QUEUED, State.NEW, State.RE_ADDING},  # allow manual and auto retry
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
    source_infohash: str  # VPS1 racing client infohash
    dest_infohash: str = ""  # VPS2 qBittorrent infohash (may equal source for public)
    # Source torrent meta
    source_name: str = ""
    source_tracker: str = ""
    source_announce_url: str = ""
    # Files / classification
    classification_kind: str = "unknown"
    total_bytes: int = 0
    # SSD-side state
    save_path: str = ""
    # Physical top-level folder under the fuse seed mount ("" = mount root);
    # seeded rows keep their real layout instead of the flat default.
    seed_subdir: str = ""
    # Cross-seed bookkeeping
    cross_seed_infohash: str = ""  # prowlarr / SFTP-fetched torrent used on SSD
    cross_seed_source: str = ""  # "prowlarr" | "sftp" | "self"
    # Raw .torrent bytes used on SSD (prowlarr / SFTP-fetched). Persisted
    # so that recovery after a restart can re-add the cross-seed torrent
    # when only the racing torrents survived.
    cross_seed_blob: bytes = b""
    injected_private_hashes: str = ""  # CSV of private hashes re-added to fuse
    # Download-target indexer retry policy
    indexer_first_queried_at: dt.datetime | None = None
    indexer_next_retry_at: dt.datetime | None = None
    indexer_attempts: int = 0
    # VPS1 public-torrent export retry policy (separate from the
    # download-target indexer schedule above): a transient export glitch
    # must retry indefinitely on its own timer instead of sharing the
    # indexer's max-age clock (which would FAILED a row whose source
    # torrent still exists on VPS1).
    public_export_first_attempted_at: dt.datetime | None = None
    public_export_next_retry_at: dt.datetime | None = None
    public_export_attempts: int = 0
    # Manual-move wait window (pre-pick): set when a torrent's expected
    # files are only PARTLY on the fuse target (an operator rclone-move in
    # progress). While set, the row parks as WAITING_INDEXER — the park
    # reuses `indexer_next_retry_at` as the wake timer — and falls back to
    # the SSD download once `manual_move_timeout_seconds` elapsed since
    # this stamp. Persisted so a restart keeps the deadline.
    fuse_wait_first_at: dt.datetime | None = None
    # Fuse Adoption verification: 1 once the row's bytes have actually been
    # observed on the fuse mount (not merely claimed complete by a
    # skip_check client). The janitor only clears the VPS1 copy for
    # fuse-verified DONE rows; unverified rows are re-probed (blob
    # re-export + live byte check) until they verify or the operator
    # intervenes. Never 1 on trust alone.
    fuse_verified: int = 0
    # Re-add retry policy
    readd_first_attempted_at: dt.datetime | None = None
    readd_next_retry_at: dt.datetime | None = None
    readd_attempts: int = 0
    # Failure retry tracking
    failed_retries: int = 0
    # Operator/auto fallback to the racing client's own .torrent for the
    # SSD download (Telegram /start_ or prowlarr-timeout fallback). Sticky:
    # once set, every SSD-source pick bypasses Prowlarr for this row.
    force_direct: int = 0
    # Operator hold (Telegram /hold_): the row keeps its state but gets
    # no workers — no Prowlarr queries, no downloads, no moves — until
    # /unhold_ (or fresh input via /add / watch-dir re-drop) resumes it.
    # Sticky across restarts like force_direct.
    skipped: int = 0
    # VPS1 cleanup bookkeeping (see [cleanup]):
    # - completed_at: last time the row entered DONE (the pressure-order
    #   anchor, "then oldest"; never an eligibility clock).
    # - vps1_last_activity_at: last time the VPS1 swarm showed upload
    #   activity (the keep for a live race, and the settle-window evidence
    #   for the MOVING early-pressure path).
    completed_at: dt.datetime | None = None
    vps1_last_activity_at: dt.datetime | None = None
    # Lifecycle
    state: State = State.NEW
    # Durable MOVING checkpoint: "", "moved", "entry_deleted", "wiped".
    # move_plan is compact JSON (relative paths only) describing the local
    # cleanup that must still run after the dest entry is deleted.
    move_stage: str = ""
    move_plan: str = ""
    batch_index: int = 0
    batches_total: int = 0
    # Frozen per-torrent SSD batch cap (persisted so restarts keep the same
    # batch boundaries instead of re-freezing at whatever free space says).
    batch_cap_bytes: int = 0
    # Consecutive rapid DONE -> RE_ADDING/MOVING demotions (flap counter for
    # the re-add max-age guard; reset by long healthy DONE periods and fresh
    # pipeline entries).
    readd_cycles: int = 0
    # Optimistic-locking revision, bumped on every write. transition()
    # re-reads inside its transaction and refuses to overwrite a state
    # that moved underneath a stale in-memory row (the second writer
    # loses loudly instead of clobbering silently).
    version: int = 0
    last_error: str = ""
    created_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    updated_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    # Telegram detail message id (0 if not yet sent).
    telegram_message_id: int = 0
    # Forget tombstone (forget() stamps instead of deleting): tombstoned
    # rows are invisible to every read and refuse every write for
    # _TOMBSTONE_TTL_SECONDS, then the janitor hard-deletes them. New
    # writers are safe by default — no per-site forget guards needed.
    deleted_at: dt.datetime | None = None
    # Transient in-memory storage for the .torrent bytes during processing
    _blob: bytes = b""

    def to_row(self) -> dict:
        row = {
            "source_infohash": self.source_infohash,
            "dest_infohash": self.dest_infohash,
            "source_name": self.source_name,
            "source_tracker": self.source_tracker,
            "source_announce_url": self.source_announce_url,
            "classification_kind": self.classification_kind,
            "total_bytes": self.total_bytes,
            "save_path": self.save_path,
            "seed_subdir": self.seed_subdir,
            "cross_seed_infohash": self.cross_seed_infohash,
            "cross_seed_source": self.cross_seed_source,
            "cross_seed_blob": self.cross_seed_blob,
            "injected_private_hashes": self.injected_private_hashes,
            "indexer_first_queried_at": self.indexer_first_queried_at.isoformat()
            if self.indexer_first_queried_at
            else "",
            "indexer_next_retry_at": self.indexer_next_retry_at.isoformat()
            if self.indexer_next_retry_at
            else "",
            "indexer_attempts": self.indexer_attempts,
            "public_export_first_attempted_at": (
                self.public_export_first_attempted_at.isoformat()
                if self.public_export_first_attempted_at
                else ""
            ),
            "public_export_next_retry_at": (
                self.public_export_next_retry_at.isoformat()
                if self.public_export_next_retry_at
                else ""
            ),
            "public_export_attempts": self.public_export_attempts,
            "fuse_wait_first_at": (
                self.fuse_wait_first_at.isoformat() if self.fuse_wait_first_at else ""
            ),
            "fuse_verified": int(self.fuse_verified or 0),
            "force_direct": int(self.force_direct or 0),
            "skipped": int(self.skipped or 0),
            "readd_first_attempted_at": (
                self.readd_first_attempted_at.isoformat() if self.readd_first_attempted_at else ""
            ),
            "readd_next_retry_at": (
                self.readd_next_retry_at.isoformat() if self.readd_next_retry_at else ""
            ),
            "readd_attempts": self.readd_attempts,
            "failed_retries": self.failed_retries,
            "completed_at": self.completed_at.isoformat() if self.completed_at else "",
            "vps1_last_activity_at": self.vps1_last_activity_at.isoformat()
            if self.vps1_last_activity_at
            else "",
            "state": self.state.value,
            "move_stage": self.move_stage,
            "move_plan": self.move_plan,
            "batch_index": self.batch_index,
            "batches_total": self.batches_total,
            "batch_cap_bytes": self.batch_cap_bytes,
            "readd_cycles": self.readd_cycles,
            "version": int(self.version or 0),
            "last_error": self.last_error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "telegram_message_id": self.telegram_message_id,
            "deleted_at": self.deleted_at.isoformat() if self.deleted_at else "",
        }
        # Canonical schema order, and a column added to the schema without a
        # value here raises instead of writing a partial INSERT.
        return {name: row[name] for name in TORRENT_STATE_COLUMNS}


# --------------------------------------------------------------------------- #
# Source push checkpoint (VPS1 -> VPS2 rclone copy of a private group)
# --------------------------------------------------------------------------- #


class PushState(str, enum.Enum):  # noqa: UP042  (StrEnum changes str(Enum); .value is the contract)
    """Lifecycle of one in-flight "push from VPS1" job (a `SourcePush`).

    running      the detached rclone copy on VPS1 is (assumed) still copying
    verifying    the copy claims the files are there; remote bytes are being checked
    done         verification passed, the group is complete on the remote
    failed       copy or verification broke; needs an operator decision
    abandoned    superseded (fresh tap, cancel, deadline passed); kept as
                 history until the next start/claim for the hash replaces it

    Only `running` and `verifying` are live states: they hold the dedupe lock
    so a second tap never launches a competing copy of the same group.
    """

    RUNNING = "running"
    VERIFYING = "verifying"
    DONE = "done"
    FAILED = "failed"
    ABANDONED = "abandoned"


#: States meaning "work is happening right now" — the claim/dedupe set.
LIVE_PUSH_STATES: tuple[PushState, ...] = (PushState.RUNNING, PushState.VERIFYING)


@dataclass(slots=True)
class SourcePush:
    """Durable checkpoint of one push from VPS1, keyed by the racing hash.

    One record per content group, owned by the VPS1 racing torrent's infohash
    (all lowercase). Surviving an app restart is the point: the runner reads
    this back to resume verification instead of launching a second copy.
    """

    # Identity: the VPS1 racing torrent hash the push was started for.
    infohash: str
    # Normalized content key of the group the push serves ("" when unknown).
    group_key: str = ""
    # Display name only — never used for matching or layout decisions.
    content_name: str = ""
    # The download directory on VPS1. NOT the row's save_path, which always
    # means the VPS2 SSD.
    src_dir: str = ""
    # Full "remote:path/..." prefix the copy writes into.
    target_remote: str = ""
    # Path of the --files-from-raw list file written on VPS1 ("" if unused).
    list_path: str = ""
    # Path of the detached rclone log file on VPS1.
    log_path: str = ""
    state: PushState = PushState.RUNNING
    started_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    updated_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    # Watchdog expiry for the live states. "" means no expiry: the record
    # stays live until finish_push/clear_push says otherwise.
    deadline_at: dt.datetime | None = None
    # Metadata-only progress counters (file counts, never bytes).
    files_total: int = 0
    files_present: int = 0
    last_error: str = ""

    def to_row(self) -> dict:
        row = {
            "infohash": self.infohash,
            "group_key": self.group_key,
            "content_name": self.content_name,
            "src_dir": self.src_dir,
            "target_remote": self.target_remote,
            "list_path": self.list_path,
            "log_path": self.log_path,
            "state": self.state.value if isinstance(self.state, PushState) else str(self.state),
            "started_at": self.started_at.isoformat() if self.started_at else "",
            "updated_at": self.updated_at.isoformat() if self.updated_at else "",
            "deadline_at": self.deadline_at.isoformat() if self.deadline_at else "",
            "files_total": int(self.files_total or 0),
            "files_present": int(self.files_present or 0),
            "last_error": self.last_error,
        }
        # Same guarantee as TorrentState.to_row(): a column added to the spec
        # without a value here raises instead of writing a partial INSERT.
        return {name: row[name] for name in SOURCE_PUSH_COLUMNS}


# --------------------------------------------------------------------------- #
# Schema: one source of truth
# --------------------------------------------------------------------------- #

# Every torrent_state column in table order: (name, type/defaults). The
# CREATE TABLE text, the blob-less SELECT projection, to_row()'s column set
# and the upgrade path all derive from this tuple, so the schema and the
# column lists cannot drift apart. New columns go here (and in to_row /
# _row_to_state); the module-level check below refuses an inconsistent pair.
_TORRENT_STATE_COLUMN_SPECS: tuple[tuple[str, str], ...] = (
    ("source_infohash", "TEXT PRIMARY KEY"),
    ("dest_infohash", "TEXT NOT NULL DEFAULT ''"),
    ("source_name", "TEXT NOT NULL DEFAULT ''"),
    ("source_tracker", "TEXT NOT NULL DEFAULT ''"),
    ("source_announce_url", "TEXT NOT NULL DEFAULT ''"),
    ("classification_kind", "TEXT NOT NULL DEFAULT 'unknown'"),
    ("total_bytes", "INTEGER NOT NULL DEFAULT 0"),
    ("save_path", "TEXT NOT NULL DEFAULT ''"),
    ("seed_subdir", "TEXT NOT NULL DEFAULT ''"),
    ("cross_seed_infohash", "TEXT NOT NULL DEFAULT ''"),
    ("cross_seed_source", "TEXT NOT NULL DEFAULT ''"),
    ("cross_seed_blob", "BLOB NOT NULL DEFAULT ''"),
    ("injected_private_hashes", "TEXT NOT NULL DEFAULT ''"),
    ("indexer_first_queried_at", "TEXT NOT NULL DEFAULT ''"),
    ("indexer_next_retry_at", "TEXT NOT NULL DEFAULT ''"),
    ("indexer_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("public_export_first_attempted_at", "TEXT NOT NULL DEFAULT ''"),
    ("public_export_next_retry_at", "TEXT NOT NULL DEFAULT ''"),
    ("public_export_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("fuse_wait_first_at", "TEXT NOT NULL DEFAULT ''"),
    ("fuse_verified", "INTEGER NOT NULL DEFAULT 0"),
    ("force_direct", "INTEGER NOT NULL DEFAULT 0"),
    ("skipped", "INTEGER NOT NULL DEFAULT 0"),
    ("readd_first_attempted_at", "TEXT NOT NULL DEFAULT ''"),
    ("readd_next_retry_at", "TEXT NOT NULL DEFAULT ''"),
    ("readd_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("failed_retries", "INTEGER NOT NULL DEFAULT 0"),
    ("completed_at", "TEXT NOT NULL DEFAULT ''"),
    ("vps1_last_activity_at", "TEXT NOT NULL DEFAULT ''"),
    ("state", "TEXT NOT NULL"),
    ("move_stage", "TEXT NOT NULL DEFAULT ''"),
    ("move_plan", "TEXT NOT NULL DEFAULT ''"),
    ("batch_index", "INTEGER NOT NULL DEFAULT 0"),
    ("batches_total", "INTEGER NOT NULL DEFAULT 0"),
    ("batch_cap_bytes", "INTEGER NOT NULL DEFAULT 0"),
    ("readd_cycles", "INTEGER NOT NULL DEFAULT 0"),
    ("version", "INTEGER NOT NULL DEFAULT 0"),
    ("last_error", "TEXT NOT NULL DEFAULT ''"),
    ("telegram_message_id", "INTEGER NOT NULL DEFAULT 0"),
    ("created_at", "TEXT NOT NULL"),
    ("updated_at", "TEXT NOT NULL"),
    ("deleted_at", "TEXT NOT NULL DEFAULT ''"),
)

TORRENT_STATE_COLUMNS: tuple[str, ...] = tuple(name for name, _ddl in _TORRENT_STATE_COLUMN_SPECS)

# Same idea for the source_push table: the CREATE TABLE text and SourcePush's
# column set come from this tuple, so they cannot drift apart.
_SOURCE_PUSH_COLUMN_SPECS: tuple[tuple[str, str], ...] = (
    ("infohash", "TEXT PRIMARY KEY"),
    ("group_key", "TEXT NOT NULL DEFAULT ''"),
    ("content_name", "TEXT NOT NULL DEFAULT ''"),
    ("src_dir", "TEXT NOT NULL DEFAULT ''"),
    ("target_remote", "TEXT NOT NULL DEFAULT ''"),
    ("list_path", "TEXT NOT NULL DEFAULT ''"),
    ("log_path", "TEXT NOT NULL DEFAULT ''"),
    ("state", "TEXT NOT NULL"),
    ("started_at", "TEXT NOT NULL"),
    ("updated_at", "TEXT NOT NULL"),
    ("deadline_at", "TEXT NOT NULL DEFAULT ''"),
    ("files_total", "INTEGER NOT NULL DEFAULT 0"),
    ("files_present", "INTEGER NOT NULL DEFAULT 0"),
    ("last_error", "TEXT NOT NULL DEFAULT ''"),
)

SOURCE_PUSH_COLUMNS: tuple[str, ...] = tuple(name for name, _ddl in _SOURCE_PUSH_COLUMN_SPECS)

# Projection without the blob for get/list paths that never need the bytes.
_TORRENT_STATE_COLUMNS_NO_BLOB = ", ".join(
    "'' AS cross_seed_blob" if name == "cross_seed_blob" else name for name in TORRENT_STATE_COLUMNS
)

#: Bumped whenever a column spec or a data migration changes. Recorded in
#: `meta` so an upgraded or downgraded DB shows up in the log; the column
#: diff at open is the actual migration mechanism.
_SCHEMA_VERSION = 6

#: Persisted TorrentState fields: everything except the transient blob copy.
_PERSISTED_FIELDS = frozenset(f.name for f in dataclass_fields(TorrentState)) - {"_blob"}

if _PERSISTED_FIELDS != frozenset(TORRENT_STATE_COLUMNS):
    raise RuntimeError(
        "torrent_state schema drift: fields without a column "
        f"{sorted(_PERSISTED_FIELDS - frozenset(TORRENT_STATE_COLUMNS))}; columns "
        f"without a field {sorted(frozenset(TORRENT_STATE_COLUMNS) - _PERSISTED_FIELDS)}"
    )

_PUSH_PERSISTED_FIELDS = frozenset(f.name for f in dataclass_fields(SourcePush))

if _PUSH_PERSISTED_FIELDS != frozenset(SOURCE_PUSH_COLUMNS):
    raise RuntimeError(
        "source_push schema drift: fields without a column "
        f"{sorted(_PUSH_PERSISTED_FIELDS - frozenset(SOURCE_PUSH_COLUMNS))}; columns "
        f"without a field {sorted(frozenset(SOURCE_PUSH_COLUMNS) - _PUSH_PERSISTED_FIELDS)}"
    )


def _create_torrent_state_sql() -> str:
    body = ",\n    ".join(f"{name} {ddl}" for name, ddl in _TORRENT_STATE_COLUMN_SPECS)
    return f"CREATE TABLE IF NOT EXISTS torrent_state (\n    {body}\n);"


def _create_source_push_sql() -> str:
    body = ",\n    ".join(f"{name} {ddl}" for name, ddl in _SOURCE_PUSH_COLUMN_SPECS)
    return f"CREATE TABLE IF NOT EXISTS source_push (\n    {body}\n);"


SCHEMA_TABLES = (
    _create_torrent_state_sql()
    + """

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

-- Racing-client .torrent cache, independent of torrent_state rows: a
-- content group's sibling racing torrents have no row of their own, yet
-- their bytes must survive a VPS1 cleanup so the fuse re-add (and the
-- late-seed repair) can still inject them.
CREATE TABLE IF NOT EXISTS torrent_blob (
    infohash    TEXT PRIMARY KEY,
    blob        BLOB NOT NULL,
    updated_at  TEXT NOT NULL
);

"""
    # Durable "push from VPS1" checkpoint, keyed by the racing hash rather
    # than by a torrent_state row (like torrent_blob above): the group may be
    # served by sibling hashes with no row of their own, and a copy started
    # over SSH keeps running on VPS1 after this process dies, so its state
    # must survive a restart instead of being re-launched by the next tap.
    + _create_source_push_sql()
    + """
"""
)

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_state ON torrent_state(state);
CREATE INDEX IF NOT EXISTS ix_updated_at ON torrent_state(updated_at);
CREATE INDEX IF NOT EXISTS ix_indexer_retry
    ON torrent_state(state, indexer_next_retry_at);
CREATE INDEX IF NOT EXISTS ix_public_export_retry
    ON torrent_state(state, public_export_next_retry_at);
CREATE INDEX IF NOT EXISTS ix_source_name ON torrent_state(source_name);
CREATE INDEX IF NOT EXISTS ix_active_updated ON torrent_state(updated_at)
    WHERE deleted_at = '';
CREATE INDEX IF NOT EXISTS ix_push_state ON source_push(state, deadline_at);
"""

# Convenience concatenation for fresh databases only. Upgrade paths must
# NOT use this order: StateStore.__init__ runs SCHEMA_TABLES, then the ALTER
# migrations, then SCHEMA_INDEXES, because some indexes reference columns
# that only the migrations add to pre-1.0.0 tables.
SCHEMA = SCHEMA_TABLES + SCHEMA_INDEXES
