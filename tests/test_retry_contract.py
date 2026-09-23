"""Retryability contract: fatal disk/permission errors fail fast, transients park."""
from __future__ import annotations

import errno

import aiohttp

from racing_sync.coordinator_errors import (
    WebUIUnresponsiveError,
    is_fatal_os_error,
    is_retryable_client_error,
)


def test_fatal_errnos_never_retryable():
    for code in (errno.ENOSPC, errno.EACCES, errno.EPERM, errno.EROFS,
                 errno.EFBIG, getattr(errno, "EDQUOT", 122)):
        exc = OSError(code, "disk/permission failure")
        assert exc.errno == code
        assert is_fatal_os_error(exc) is True
        assert is_retryable_client_error(exc) is False


def test_transient_errors_retryable():
    assert is_retryable_client_error(TimeoutError("t")) is True
    assert is_retryable_client_error(
        WebUIUnresponsiveError("wedged")) is True
    assert is_retryable_client_error(
        aiohttp.ClientConnectionError("reset")) is True
    assert is_retryable_client_error(
        ConnectionError("refused")) is True
    for code in (errno.EPIPE, errno.ECONNRESET, errno.ETIMEDOUT,
                 errno.ECONNREFUSED, errno.ENETUNREACH, errno.EAGAIN):
        assert is_retryable_client_error(OSError(code, "blip")) is True
    # Errno-less OSErrors keep the historical park-and-retry behavior.
    assert is_retryable_client_error(OSError("string-only failure")) is True


def test_unexpected_errors_not_retryable():
    assert is_retryable_client_error(ValueError("bug")) is False
    assert is_retryable_client_error(RuntimeError("bug")) is False
    assert is_fatal_os_error(ValueError("bug")) is False
