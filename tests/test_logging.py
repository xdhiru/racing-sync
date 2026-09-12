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
        url="http://example.com/logs",
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

        handler._stop.set()
        handler._thread.join(timeout=2.0)

        assert mock_urlopen.called
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "http://example.com/logs"
        assert req.headers["Authorization"] == "Bearer secret-token"
        assert req.headers["Content-type"] == "application/json"

        body = json.loads(req.data.decode("utf-8"))
        assert body["message"] == "hello from sink test"
        assert body["level"] == "INFO"
        assert body["logger"] == "test_logger"
