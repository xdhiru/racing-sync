"""Uniform I/O bounds: timeouts, offload, chunking."""
from __future__ import annotations

import asyncio

import pytest

from racing_sync.io_bounds import (
    bounded,
    chunked,
    offload,
    rpc_timeout_seconds,
)


def test_rpc_timeout_clamped_with_fallbacks():
    from types import SimpleNamespace

    assert rpc_timeout_seconds(None) == 30.0
    assert rpc_timeout_seconds(object()) == 30.0
    good = SimpleNamespace(general=SimpleNamespace(
        client_rpc_timeout_seconds=45))
    assert rpc_timeout_seconds(good) == 45.0
    low = SimpleNamespace(general=SimpleNamespace(
        client_rpc_timeout_seconds=1))
    assert rpc_timeout_seconds(low) == 5.0
    high = SimpleNamespace(general=SimpleNamespace(
        client_rpc_timeout_seconds=9999))
    assert rpc_timeout_seconds(high) == 300.0
    bad = SimpleNamespace(general=SimpleNamespace(
        client_rpc_timeout_seconds="soon"))
    assert rpc_timeout_seconds(bad) == 30.0


@pytest.mark.anyio
async def test_bounded_returns_and_times_out():
    async def fast():
        return 42

    assert await bounded(fast(), timeout=5, label="t-fast") == 42

    async def slow():
        await asyncio.sleep(30)
        return 1

    with pytest.raises(TimeoutError, match="t-slow"):
        await bounded(slow(), timeout=0.05, label="t-slow")


@pytest.mark.anyio
async def test_bounded_propagates_cancellation():
    async def stuck():
        await asyncio.sleep(30)

    task = asyncio.ensure_future(bounded(stuck(), timeout=30, label="t-x"))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_offload_runs_blocking_fn():
    import time

    def blocking():
        time.sleep(0.01)
        return "done"

    assert await offload(blocking) == "done"


def test_chunked_bounds_and_defaults():
    assert chunked(range(250), 100) == [list(range(100)), list(range(100, 200)), list(range(200, 250))]
    assert chunked([], 100) == []
    assert chunked([1, 2, 3], 0) == [[1, 2, 3]]
    assert chunked(None) == []
