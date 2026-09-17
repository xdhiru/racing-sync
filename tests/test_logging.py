from __future__ import annotations

import json
import logging
import time
from unittest.mock import MagicMock, patch

from racing_sync.config import LoggingSinkConfig
from racing_sync.logging_setup import HTTPSinkHandler


def test_http_sink_handler_posts_log():
    cfg = LoggingSinkConfig(
        enabled=True,
        url="https://example.com/logs",
        auth_token="secret-token",
    )

    mock_resp = MagicMock()
    mock_resp.read.return_value = b"OK"
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        handler = HTTPSinkHandler(cfg)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="hello from sink test",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

        # Allow worker thread time to process
        for _ in range(50):
            if mock_urlopen.called:
                break
            time.sleep(0.05)

        handler.close()
        assert not handler._thread.is_alive()

        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://example.com/logs"
        assert req.headers["Authorization"] == "Bearer secret-token"
        assert req.headers["Content-type"] == "application/json"

        body = json.loads(req.data.decode("utf-8"))
        assert body["message"] == "hello from sink test"
        assert body["level"] == "INFO"
        assert body["logger"] == "test_logger"


def test_http_sink_refuses_bearer_over_insecure_http():
    cfg = LoggingSinkConfig(
        enabled=True,
        url="http://remote-collector.com/logs",
        auth_token="secret-token",
    )

    mock_resp = MagicMock()
    mock_resp.read.return_value = b"OK"
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        handler = HTTPSinkHandler(cfg)
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="insecure test",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

        for _ in range(50):
            if mock_urlopen.called:
                break
            time.sleep(0.05)

        handler.close()
        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        # Authorization header must NOT be attached over insecure remote HTTP
        assert "Authorization" not in req.headers


def test_jsonl_formatter_scrubs_secrets_and_extra():
    from racing_sync.logging_setup import JsonlFormatter

    formatter = JsonlFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Downloading https://tracker.com/announce?passkey=SECRET_PASSKEY&user=123",
        args=(),
        exc_info=None,
    )
    # Extra dictionary attributes with sensitive and non-sensitive keys
    record.__dict__["api_key"] = "SUPER_SECRET_KEY"
    record.__dict__["custom_token"] = "TOKEN_VALUE"
    record.__dict__["safe_field"] = "safe_value"

    formatted = formatter.format(record)
    data = json.loads(formatted)

    # Message must have passkey masked
    assert "SECRET_PASSKEY" not in data["message"]
    assert "passkey=***" in data["message"]

    # Sensitive extra keys must be masked
    assert data["api_key"] == "***"
    assert data["custom_token"] == "***"
    assert data["safe_field"] == "safe_value"


def test_setup_logging_silences_chatty_http_libraries(tmp_path):
    from pathlib import Path

    from racing_sync.config import AppConfig
    from racing_sync.logging_setup import setup_logging

    cfg = AppConfig.from_toml(Path(__file__).parent.parent / "config.example.toml")
    cfg.general.log_dir = tmp_path / "logs"
    cfg.logging_sink.enabled = False
    setup_logging(cfg)
    try:
        for name in ("httpx", "httpcore", "httpcore.connection", "httpcore.http11"):
            assert logging.getLogger(name).getEffectiveLevel() >= logging.WARNING
    finally:
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)


def test_sanitize_redacts_announce_path_passkey():
    from racing_sync.logging_setup import sanitize_log_text

    leaked = (
        "watch-dir picked up: Show (ff3d13ab68) "
        "announce=https://dl-indexer.example.net/announce/e4a7c2f19b83d05a6c7e1f349a8bd6e55"
    )
    clean = sanitize_log_text(leaked)
    assert "e4a7c2f19b83d05a6c7e1f349a8bd6e55" not in clean
    assert "https://dl-indexer.example.net/announce/..." in clean
    # Bare infohashes (content IDs, not credentials) are untouched.
    assert sanitize_log_text("torrent (ff3d13ab68) done") == "torrent (ff3d13ab68) done"
    assert sanitize_log_text("hash " + "a" * 40 + " ok") == "hash " + "a" * 40 + " ok"
    # Multiple announce URLs in one line each redact independently.
    multi = "a=https://x.test/announce/111,b=https://y.test/announce/222"
    assert sanitize_log_text(multi) == "a=https://x.test/announce/...,b=https://y.test/announce/..."


def test_sanitizing_formatter_scrubs_secrets_in_text_logs():
    from racing_sync.logging_setup import SanitizingFormatter, LOG_FORMAT, DATE_FORMAT

    formatter = SanitizingFormatter(LOG_FORMAT, DATE_FORMAT)
    record = logging.LogRecord(
        name="racing_sync.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=15,
        msg="Connecting with token=SUPER_SECRET_TOKEN and Authorization: Bearer MY_SECRET_AUTH_BEARER",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    assert "SUPER_SECRET_TOKEN" not in formatted
    assert "token=***" in formatted
    assert "MY_SECRET_AUTH_BEARER" not in formatted
    assert "Bearer ***" in formatted


def test_logging_sink_plaintext_fails_closed():
    """Plaintext http:// to a non-local host needs explicit opt-in."""
    import pydantic

    try:
        LoggingSinkConfig(enabled=True, url="http://logs.example.com/ingest")
        assert False, "expected ValidationError"
    except pydantic.ValidationError as e:
        assert "allow_plaintext" in str(e)
    # Opt-in, https, and localhost stay valid.
    LoggingSinkConfig(enabled=True, url="http://logs.example.com/ingest",
                      allow_plaintext=True)
    LoggingSinkConfig(enabled=True, url="https://logs.example.com/ingest")
    LoggingSinkConfig(enabled=True, url="http://localhost:8080/ingest")


