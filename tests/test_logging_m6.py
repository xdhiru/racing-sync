"""M6 logging/shutdown: handler disable policy, sink opt-in log, ring tail."""

from __future__ import annotations

import errno
import logging

import pytest


def _record(msg="x"):
    return logging.LogRecord("t", logging.WARNING, __file__, 1, msg, None, None)


def _handler(tmp_path, monkeypatch):
    from racing_sync.logging_setup import ENOSPCSafeTimedRotatingFileHandler as H

    h = H(str(tmp_path / "t.log"), when="midnight", backupCount=1)
    h.handleError = lambda *a: None  # type: ignore[method-assign]
    return h


def test_handler_disables_after_persistent_oserror(tmp_path, monkeypatch):
    import logging.handlers as _lh

    h = _handler(tmp_path, monkeypatch)

    def _boom(self, record):
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr(_lh.TimedRotatingFileHandler, "emit", _boom)
    h.emit(_record())
    h.emit(_record())
    assert h._disabled_due_to_enospc is False
    h.emit(_record())
    assert h._disabled_due_to_enospc is True


def test_handler_disables_immediately_on_enospc(tmp_path, monkeypatch):
    import logging.handlers as _lh

    h = _handler(tmp_path, monkeypatch)

    def _boom(self, record):
        raise OSError(errno.ENOSPC, "full")

    monkeypatch.setattr(_lh.TimedRotatingFileHandler, "emit", _boom)
    h.emit(_record())
    assert h._disabled_due_to_enospc is True


def test_handler_streak_resets_on_success(tmp_path, monkeypatch):
    import logging.handlers as _lh

    h = _handler(tmp_path, monkeypatch)
    real_emit = _lh.TimedRotatingFileHandler.emit

    def _boom(self, record):
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr(_lh.TimedRotatingFileHandler, "emit", _boom)
    h.emit(_record())
    h.emit(_record())
    monkeypatch.setattr(_lh.TimedRotatingFileHandler, "emit", real_emit)
    h.emit(_record())  # success resets the streak
    monkeypatch.setattr(_lh.TimedRotatingFileHandler, "emit", _boom)
    h.emit(_record())
    h.emit(_record())
    assert h._disabled_due_to_enospc is False
    h.emit(_record())
    assert h._disabled_due_to_enospc is True


def _sink_cfg(tmp_path, **kw):
    from types import SimpleNamespace

    base = dict(
        enabled=True,
        url="http://logs.example/ingest",
        auth_token="s3cret",
        forward_min_level="INFO",
        allow_plaintext=True,
    )
    base.update(kw)
    return SimpleNamespace(
        general=SimpleNamespace(
            log_dir=tmp_path, log_retention_days=1, state_db=str(tmp_path / "s.db")
        ),
        ssd=SimpleNamespace(path=tmp_path),
        dest=SimpleNamespace(save_path=tmp_path),
        logging_sink=SimpleNamespace(**base),
    )


def test_sink_plaintext_opt_in_logged_loudly(tmp_path):
    import logging as _logging

    from racing_sync import logging_setup as _ls

    seen: list[str] = []

    class _H(_logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    app_log = _logging.getLogger("racing_sync")
    hold = _H()
    app_log.addHandler(hold)
    root = _logging.getLogger()
    old_handlers, old_level = root.handlers[:], root.level
    try:
        _ls.setup_logging(_sink_cfg(tmp_path))
    finally:
        app_log.removeHandler(hold)
        for h in list(root.handlers):
            try:
                root.removeHandler(h)
                _stop = getattr(h, "_stop", None)
                if _stop is not None:
                    try:
                        _stop.set()
                    except Exception:
                        pass
                h.close()
            except Exception:
                pass
        for h in old_handlers:
            try:
                root.addHandler(h)
            except Exception:
                pass
        try:
            root.setLevel(old_level)
        except Exception:
            pass
    assert any("PLAINTEXT" in m for m in seen)


@pytest.mark.anyio
async def test_post_log_event_returns_bool_and_logs_non_2xx(caplog):
    from racing_sync.logging_setup import post_log_event_async

    class _Resp:
        def __init__(self, status):
            self.status = status

        async def read(self):
            return b""

    class _Post:
        def __init__(self, status=None, boom=False):
            self._status = status
            self._boom = boom

        async def __aenter__(self):
            if self._boom:
                raise OSError("down")
            return _Resp(self._status)

        async def __aexit__(self, *a):
            return False

    class _Session:
        def __init__(self, status=None, boom=False):
            self._status = status
            self._boom = boom

        def post(self, *a, **k):
            return _Post(self._status, self._boom)

    assert await post_log_event_async(_Session(200), "http://x/ingest", {}) is True
    with caplog.at_level(logging.WARNING, logger="racing_sync.logging_setup"):
        assert await post_log_event_async(_Session(500), "http://x/ingest", {}) is False
    assert any("500" in r.message for r in caplog.records)
    assert await post_log_event_async(_Session(boom=True), "http://x/ingest", {}) is False


@pytest.mark.anyio
async def test_stats_carries_ring_tail():
    """The dead ring buffer lives: /stats shows recent warnings."""
    from unittest.mock import AsyncMock, MagicMock

    from racing_sync import logging_setup as _ls
    from racing_sync.telegram_protocol import _cmd_stats

    rb = _ls.RingBufferHandler(capacity=10)
    rb.emit(
        logging.LogRecord(
            "racing_sync.fuse",
            logging.WARNING,
            __file__,
            1,
            "fuse wobbling passkey=SECRET",
            None,
            None,
        )
    )
    old, _ls._ring = _ls._ring, rb
    try:
        bot = MagicMock()
        bot._store = MagicMock()
        bot._store.all.return_value = []
        bot._store.list_ignored.return_value = []
        bot._coord = None
        bot._overview_line = MagicMock(return_value="")
        bot._active_footer = AsyncMock(return_value="")
        bot._reply = AsyncMock()
        msg = MagicMock()
        await _cmd_stats(bot, MagicMock(), msg)
    finally:
        _ls._ring = old
    text = bot._reply.call_args[0][0]
    assert "fuse wobbling" in text
    assert "SECRET" not in text  # scrubbed at ring-ingress


def test_request_stop_is_info_not_warning(caplog):
    from conftest import make_coordinator

    coord = make_coordinator()
    with caplog.at_level(logging.INFO, logger="racing_sync.coordinator"):
        coord.request_stop()
    assert coord._stop is True
    assert not [
        r for r in caplog.records if r.levelno >= logging.WARNING and "stop requested" in r.message
    ]
    assert any("stop requested" in r.message for r in caplog.records)
