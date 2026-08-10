"""Logging setup.

Provides:
  - Rotating file handler (one per day, retained N days)
  - Structured JSON line file for machine parsing
  - In-memory ring buffer that the Telegram bot drains for the live status
  - Optional HTTP POST sink for centralised logging
  - Console handler (stderr) at INFO by default
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import logging.handlers
import queue
import re
import sys
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque
from urllib.parse import urlsplit

import aiohttp

from .config import AppConfig, LoggingSinkConfig

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s :: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

SENSITIVE_KEY_WORDS = ("passkey", "api_key", "apikey", "token", "auth", "secret", "password")

_SENSITIVE_PARAM_RE = re.compile(
    r"((?:passkey|passwd|pwd|pass[_-]?key|api[_-]?key|auth[_-]?key|secret[_-]?key|token|secret|password|auth)\s*[:=]\s*[\"']?)([^&\s\"'},;]+)",
    re.IGNORECASE,
)
_BEARER_TOKEN_RE = re.compile(
    r"((?:Bearer|Basic|Token|ApiKey)\s+)[A-Za-z0-9_\-\.~\+/=]+",
    re.IGNORECASE,
)
_SENSITIVE_KEY_RE = re.compile(
    r"passkey|passwd|pwd|pass[_-]?key|api[_-]?key|auth[_-]?key|secret[_-]?key|token|secret|password|^auth$|auth[_-]",
    re.IGNORECASE,
)
_SENSITIVE_HEADER_KEYS = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "x-api-token"}
)


class ENOSPCSafeTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Timed file handler that disables itself on ENOSPC instead of traceback-storming.

    Once the log disk is full, every emit() would raise OSError(28) and the
    logging module prints a full traceback to stderr per message — which
    fills the disk faster and buries the real error. Fail silent after the
    first ENOSPC: drop file output, keep console/ring, and print one line.
    """

    _disabled_due_to_enospc: bool = False

    def emit(self, record: logging.LogRecord) -> None:
        if self._disabled_due_to_enospc:
            return
        try:
            super().emit(record)
        except OSError as e:
            import errno as _errno

            if e.errno == _errno.ENOSPC:
                self._disabled_due_to_enospc = True
                try:
                    sys.stderr.write(
                        f"[log] disabling file handler {self.baseFilename}: "
                        "no space left on device (logging to console/ring only)\n"
                    )
                    sys.stderr.flush()
                except Exception:
                    pass
                try:
                    self.close()
                except Exception:
                    pass
                return
            self.handleError(record)
        except Exception:
            self.handleError(record)


def sanitize_log_text(text: str) -> str:
    """Scrub sensitive credentials, tokens, and passkeys from log messages."""
    text = _SENSITIVE_PARAM_RE.sub(r"\1***", text)
    text = _BEARER_TOKEN_RE.sub(r"\1***", text)
    return text


def is_sensitive_key(key: str) -> bool:
    """Check if a dictionary key indicates sensitive credentials."""
    k_lower = key.lower()
    if k_lower in _SENSITIVE_HEADER_KEYS:
        return True
    return bool(_SENSITIVE_KEY_RE.search(key))


class SanitizingFormatter(logging.Formatter):
    """Formatter that sanitizes sensitive tokens, passkeys, and passwords from formatted text."""

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)
        return sanitize_log_text(formatted)


# --------------------------------------------------------------------------- #
# Ring buffer
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class LogEvent:
    ts: dt.datetime
    level: int
    logger: str
    message: str


class RingBufferHandler(logging.Handler):
    """Keep the last N log records in memory for quick diagnostic dumps."""

    def __init__(self, capacity: int = 500):
        super().__init__()
        self._buf: Deque[LogEvent] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            ev = LogEvent(
                ts=dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc),
                level=record.levelno,
                logger=record.name,
                message=sanitize_log_text(record.getMessage())[:2000],
            )
            with self._lock:
                self._buf.append(ev)
        except Exception:
            self.handleError(record)

    def snapshot(self, last: int = 8, min_level: int = logging.INFO) -> list[LogEvent]:
        with self._lock:
            data = list(self._buf)
        data = [e for e in data if e.level >= min_level]
        return data[-last:]


# --------------------------------------------------------------------------- #
# JSONL file handler
# --------------------------------------------------------------------------- #


class JsonlFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": sanitize_log_text(record.getMessage()),
        }
        if record.exc_info:
            payload["exc"] = sanitize_log_text(self.formatException(record.exc_info))
        for k, v in record.__dict__.items():
            if k in (
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "levelname", "levelno", "lineno", "message", "module",
                "msecs", "msg", "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
                "taskName",
            ):
                continue
            if is_sensitive_key(k):
                payload[k] = "***"
                continue
            try:
                payload[k] = _scrub_value(v)
            except Exception:
                try:
                    payload[k] = sanitize_log_text(repr(v))
                except Exception:
                    payload[k] = "<unserializable>"
        return json.dumps(payload, ensure_ascii=False)


def _scrub_value(v: object, depth: int = 0) -> object:
    """Recursively scrub secrets from extras (dicts/lists/tuples/sets)."""
    if depth > 5:
        return "<max-depth>"
    if isinstance(v, str):
        return sanitize_log_text(v)
    if isinstance(v, dict):
        out: dict[object, object] = {}
        for dk, dv in v.items():
            try:
                if isinstance(dk, str) and is_sensitive_key(dk):
                    out[dk] = "***"
                else:
                    out[dk] = _scrub_value(dv, depth + 1)
            except Exception:
                out[dk] = "<unserializable>"
        try:
            json.dumps(out)
            return out
        except TypeError:
            return sanitize_log_text(repr(v))
    if isinstance(v, (list, tuple)):
        cleaned = [_scrub_value(x, depth + 1) for x in v]
        return cleaned if isinstance(v, list) else tuple(cleaned)
    if isinstance(v, (set, frozenset)):
        try:
            return sorted((_scrub_value(x, depth + 1) for x in v), key=repr)
        except Exception:
            return sanitize_log_text(repr(v))
    try:
        json.dumps(v)
        return v
    except TypeError:
        return sanitize_log_text(repr(v))


# --------------------------------------------------------------------------- #
# HTTP sink
# --------------------------------------------------------------------------- #


class HTTPSinkHandler(logging.handlers.QueueHandler):
    """POST log records to a central collector.

    Uses QueueHandler so the network call happens on a dedicated thread and
    never blocks the main loop.
    """

    def __init__(self, cfg: LoggingSinkConfig):
        # Bounded queue — an unreachable collector + log flood must not OOM.
        # Oldest records are dropped via put_nowait fallback in emit().
        self._q: queue.Queue[logging.LogRecord] = queue.Queue(1000)
        super().__init__(self._q)
        self._cfg = cfg
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="rs-log-sink", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
        if not self._cfg.enabled:
            return
        try:
            self._q.put_nowait(record)
        except queue.Full:
            try:
                self._q.get_nowait()
            except queue.Empty:
                pass
            try:
                self._q.put_nowait(record)
            except queue.Full:
                pass

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)
        super().close()

    def _run(self) -> None:
        import urllib.request

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "racing-sync-log-sink/1.0",
        }
        auth_token = (
            self._cfg.auth_token.get_secret_value()
            if hasattr(self._cfg.auth_token, "get_secret_value")
            else str(self._cfg.auth_token)
        )
        if auth_token:
            parsed = urlsplit(self._cfg.url)
            is_local = parsed.hostname in ("localhost", "127.0.0.1", "::1")
            if parsed.scheme == "https" or is_local:
                headers["Authorization"] = f"Bearer {auth_token}"
            else:
                sys.stderr.write(
                    f"[log-sink] Warning: refusing to send bearer auth token over insecure unencrypted {parsed.scheme}:// to {parsed.hostname}\n"
                )

        while not self._stop.is_set():
            try:
                record = self._q.get(timeout=1.0)
            except queue.Empty:
                continue
            payload = {
                "ts": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": sanitize_log_text(record.getMessage()),
            }
            try:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(self._cfg.url, data=data, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    resp.read()
            except Exception as e:  # noqa: BLE001
                sys.stderr.write(f"[log-sink] {e}\n")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


_ring: RingBufferHandler | None = None


def get_ring_buffer() -> RingBufferHandler:
    if _ring is None:
        raise RuntimeError("logging not initialised")
    return _ring


def setup_logging(cfg: AppConfig) -> None:
    """Wire up file handlers, console, sink and ring buffer."""
    global _ring

    log_dir: Path = cfg.general.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Wipe anything pre-existing (e.g. uvicorn defaults) and close them
    # so repeated setup_logging() in tests doesn't leak FDs.
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    # Scope DEBUG to racing_sync application logger
    logging.getLogger("racing_sync").setLevel(logging.DEBUG)

    # Console (stderr) at INFO
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(SanitizingFormatter(LOG_FORMAT, DATE_FORMAT))
    root.addHandler(console)

    # Rotating human-readable log (ENOSPC-safe: a full log disk disables
    # file output instead of traceback-storming per message).
    fh = ENOSPCSafeTimedRotatingFileHandler(
        log_dir / "racing-sync.log",
        when="midnight",
        backupCount=cfg.general.log_retention_days,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(SanitizingFormatter(LOG_FORMAT, DATE_FORMAT))
    root.addHandler(fh)

    # Structured JSONL
    jh = ENOSPCSafeTimedRotatingFileHandler(
        log_dir / "racing-sync.jsonl",
        when="midnight",
        backupCount=cfg.general.log_retention_days,
        encoding="utf-8",
    )
    jh.setLevel(logging.DEBUG)
    jh.setFormatter(JsonlFormatter())
    root.addHandler(jh)

    # Ring buffer for live status
    _ring = RingBufferHandler(capacity=300)
    _ring.setLevel(logging.INFO)
    root.addHandler(_ring)

    # HTTP sink
    if cfg.logging_sink.enabled:
        level_name = str(cfg.logging_sink.forward_min_level or "INFO").upper()
        if level_name not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            level_name = "INFO"
        sink = HTTPSinkHandler(cfg.logging_sink)
        sink.setLevel(getattr(logging, level_name))
        root.addHandler(sink)

    # Silence overly chatty libraries (httpx/httpcore back the Telegram
    # bot's HTTP calls and log every request at INFO without this).
    for noisy in ("aiohttp.access", "asyncio", "urllib3", "paramiko", "uvicorn",
                  "httpx", "httpcore", "telegram", "telegram.ext"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Fail-fast hint for the ENOSPC feedback loop: logs + SSD/state on one
    # mount means a full SSD kills logging and state.db together.
    try:
        log_dev = log_dir.stat().st_dev
        for _label, _raw in (
            ("state_db", cfg.general.state_db),
            ("ssd.path", cfg.ssd.path),
            ("dest.save_path", cfg.dest.save_path),
        ):
            try:
                _p = Path(str(_raw)).parent if str(_raw).endswith(".db") else Path(str(_raw))
                if _p.exists() and _p.stat().st_dev == log_dev:
                    logging.getLogger("racing_sync").warning(
                        "log dir %s shares a filesystem with %s (%s) — "
                        "move logs to a separate mount so a full SSD does not "
                        "take down logging/state.db",
                        log_dir, _label, _p,
                    )
                    break
            except OSError:
                continue
    except OSError:
        pass

    logging.getLogger("racing_sync").info(
        "logging initialised: dir=%s retention=%dd",
        log_dir, cfg.general.log_retention_days,
    )


# A tiny async helper used by the Telegram bot if it wants to forward an
# async aiohttp session instead of the threaded sink. Kept here so the API
# stays symmetrical with the other handlers.


async def post_log_event_async(
    session: aiohttp.ClientSession, url: str, payload: dict[str, Any]
) -> None:
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as r:
            await r.read()
    except Exception:
        pass