"""Persistent state for in-flight torrent processing.

This is the heart of req #4 (recovery): the state machine is the source of
truth that lets the coordinator resume work after a crash. Transitions are
explicitly enumerated so we can audit them in tests.
"""

from __future__ import annotations

import contextlib  # noqa: F401  (kept for module-surface compat)
import datetime as dt  # noqa: F401  (kept for module-surface compat)
import enum  # noqa: F401  (kept for module-surface compat)
import logging
import re  # noqa: F401  (kept for module-surface compat)
import sqlite3  # noqa: F401  (kept for module-surface compat)
import threading  # noqa: F401  (kept for module-surface compat)
import time  # noqa: F401  (kept for module-surface compat)
from dataclasses import dataclass, field, fields as dataclass_fields  # noqa: F401
from pathlib import Path  # noqa: F401  (kept for module-surface compat)

from .coordinator_errors import AbandonedError  # noqa: F401  (kept for module-surface compat)
from .state_schema import (
    _FLAP_WINDOW_SECONDS as _FLAP_WINDOW_SECONDS,  # noqa: F401  (re-exported API)
    _MAX_READD_CYCLES as _MAX_READD_CYCLES,  # noqa: F401  (re-exported API)
    _PERSISTED_FIELDS as _PERSISTED_FIELDS,  # noqa: F401  (kept for module-surface compat)
    _SCHEMA_VERSION as _SCHEMA_VERSION,  # noqa: F401  (re-exported API)
    _TORRENT_STATE_COLUMN_SPECS as _TORRENT_STATE_COLUMN_SPECS,  # noqa: F401  (re-exported API)
    _TORRENT_STATE_COLUMNS_NO_BLOB as _TORRENT_STATE_COLUMNS_NO_BLOB,  # noqa: F401
    ALLOWED as ALLOWED,  # noqa: F401  (re-exported API)
    SCHEMA as SCHEMA,  # noqa: F401  (re-exported API)
    SCHEMA_INDEXES as SCHEMA_INDEXES,  # noqa: F401  (re-exported API)
    SCHEMA_TABLES as SCHEMA_TABLES,  # noqa: F401  (re-exported API)
    TORRENT_STATE_COLUMNS as TORRENT_STATE_COLUMNS,  # noqa: F401  (re-exported API)
    State as State,  # noqa: F401  (re-exported API)
    TorrentState as TorrentState,  # noqa: F401  (re-exported API)
    _create_torrent_state_sql as _create_torrent_state_sql,  # noqa: F401  (compat)
    check_transition as check_transition,  # noqa: F401  (re-exported API)
)
from .state_store import (
    _DISPLAY_CACHE_S as _DISPLAY_CACHE_S,  # noqa: F401  (kept for module-surface compat)
    _TOMBSTONE_TTL_SECONDS as _TOMBSTONE_TTL_SECONDS,  # noqa: F401  (tests import from here)
    StateStore as StateStore,  # noqa: F401  (re-exported API)
    UpsertResult as UpsertResult,  # noqa: F401  (re-exported API)
    _row_to_state as _row_to_state,  # noqa: F401  (kept for module-surface compat)
    _safe_dt as _safe_dt,  # noqa: F401  (kept for module-surface compat)
    _safe_int as _safe_int,  # noqa: F401  (kept for module-surface compat)
    _safe_state as _safe_state,  # noqa: F401  (kept for module-surface compat)
)

log = logging.getLogger(__name__)
