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

