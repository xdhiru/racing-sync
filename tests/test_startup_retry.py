"""Boot-step backoff: transient blips heal, auth fails fast."""
from __future__ import annotations

import asyncio

import pytest

from racing_sync.clients.http_base import AuthError
from racing_sync.coordinator import _start_with_backoff


@pytest.mark.anyio
async def test_success_first_try():
    calls = []

    async def _ok():
        calls.append(1)
        return "up"

    assert await _start_with_backoff("x", _ok, delays=(0.01,)) == "up"
    assert calls == [1]


@pytest.mark.anyio
async def test_transient_then_success():
    calls = []

    async def _flaky():
        calls.append(1)
        if len(calls) < 3:
            raise OSError("blip")
        return "up"

    assert await _start_with_backoff("x", _flaky, delays=(0.01, 0.01)) == "up"
    assert len(calls) == 3


@pytest.mark.anyio
async def test_auth_fails_fast():
    calls = []

    async def _bad():
        calls.append(1)
        raise AuthError("bad credentials")

    with pytest.raises(AuthError):
        await _start_with_backoff("x", _bad, attempts=5, delays=(0.01,))
    assert len(calls) == 1


@pytest.mark.anyio
async def test_exhausted_raises_last():
    calls = []

    async def _down():
        calls.append(1)
        raise OSError(f"blip {len(calls)}")

    with pytest.raises(OSError, match="blip 2"):
        await _start_with_backoff("x", _down, attempts=2, delays=(0.01,))
    assert len(calls) == 2


@pytest.mark.anyio
async def test_sync_factory_runs_off_loop():
    seen = []

    def _dial():
        try:
            asyncio.get_running_loop()
            seen.append(True)
        except RuntimeError:
            seen.append(False)
        return "connected"

    assert await _start_with_backoff("sftp", _dial, delays=(0.01,)) == "connected"
    assert seen == [False]


@pytest.mark.anyio
async def test_cancelled_propagates():
    async def _slow():
        await asyncio.sleep(30)
        return "never"

    with pytest.raises(asyncio.CancelledError):
        task = asyncio.ensure_future(_start_with_backoff("x", _slow))
        await asyncio.sleep(0.05)
        task.cancel()
        await task
