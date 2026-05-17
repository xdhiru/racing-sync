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
import sys
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque

import aiohttp

from .config import AppConfig, LoggingSinkConfig

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s :: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


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
    """A logging handler that keeps the last N events in a deque.

    Read by the Telegram bot to render the "recent activity" section of the
    live status message.
    """

    def __init__(self, capacity: int = 200):
        super().__init__()
        self._buf: Deque[LogEvent] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            ev = LogEvent(
                ts=dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc),
                level=record.levelno,
                logger=record.name,
                message=record.getMessage(),
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
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k in (
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "levelname", "levelno", "lineno", "message", "module",
                "msecs", "msg", "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
                "taskName",
            ):
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except TypeError:
                payload[k] = repr(v)
        return json.dumps(payload, ensure_ascii=False)


# --------------------------------------------------------------------------- #
# HTTP sink
# --------------------------------------------------------------------------- #


class HTTPSinkHandler(logging.handlers.QueueHandler):
    """POST log records to a central collector.

    Uses QueueHandler so the network call happens on a dedicated thread and
    never blocks the main loop.
    """

    def __init__(self, cfg: LoggingSinkConfig):
        super().__init__(queue.Queue(-1))
        self._cfg = cfg
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="rs-log-sink", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:  # type: ignore[override]
        if not self._cfg.enabled:
            return
        super().emit(record)

    def _run(self) -> None:
        sess = None
        try:
            import requests  # type: ignore

            sess = requests.Session()
            sess.headers.update(
                {"Content-Type": "application/json"},
            )
            if self._cfg.auth_token:
                sess.headers["Authorization"] = f"Bearer {self._cfg.auth_token}"

            while not self._stop.is_set():
                try:
                    record: logging.LogRecord = self.queue.get(timeout=1.0)  # type: ignore[assignment]
                except queue.Empty:
                    continue
                payload = {
                    "ts": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                try:
                    sess.post(self._cfg.url, json=payload, timeout=5)
                except Exception as e:  # noqa: BLE001
                    sys.stderr.write(f"[log-sink] {e}\n")
        except Exception as e:  # noqa: BLE001
            sys.stderr.write(f"[log-sink thread] {e}\n")
        finally:
            if sess is not None:
                sess.close()


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
    root.setLevel(logging.DEBUG)
    # Wipe anything pre-existing (e.g. uvicorn defaults)
    for h in list(root.handlers):
        root.removeHandler(h)

    # Console (stderr) at INFO
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    root.addHandler(console)

    # Rotating human-readable log
    fh = logging.handlers.TimedRotatingFileHandler(
        log_dir / "racing-sync.log",
        when="midnight",
        backupCount=cfg.general.log_retention_days,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    root.addHandler(fh)

    # Structured JSONL
    jh = logging.handlers.TimedRotatingFileHandler(
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
        sink = HTTPSinkHandler(cfg.logging_sink)
        sink.setLevel(getattr(logging, cfg.logging_sink.forward_min_level))
        root.addHandler(sink)

    # Silence overly chatty libraries
    for noisy in ("aiohttp.access", "asyncio", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

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
    except aiohttp.ClientError:
        pass