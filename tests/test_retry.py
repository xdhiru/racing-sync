"""P5: central read-retry policy."""

from __future__ import annotations

import asyncio

import pytest


@pytest.mark.anyio
async def test_retry_then_success_and_delays():
    from unittest.mock import AsyncMock, patch
    from racing_sync.retry import retryable

    fn = AsyncMock(side_effect=[TimeoutError("b1"), OSError("b2"), "ok"])
    with patch("asyncio.sleep", new_callable=AsyncMock) as _sleep:
        assert await retryable()(fn)() == "ok"
    assert fn.await_count == 3
    assert [c.args[0] for c in _sleep.await_args_list] == [1.0, 2.0]


@pytest.mark.anyio
async def test_exhaustion_reraises_last():
    from unittest.mock import AsyncMock, patch
    from racing_sync.retry import retryable

    fn = AsyncMock(
        side_effect=[TimeoutError("one"), TimeoutError("two"), TimeoutError("three"), "never"]
    )
    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(TimeoutError, match="three"):
            await retryable()(fn)()
    assert fn.await_count == 3


@pytest.mark.anyio
async def test_deterministic_errors_fail_fast():
    from unittest.mock import AsyncMock, patch
    from racing_sync.retry import retryable

    for exc in (ValueError("bad"), RuntimeError("nope"), LookupError("gone")):
        fn = AsyncMock(side_effect=exc)
        with patch("asyncio.sleep", new_callable=AsyncMock) as _sleep:
            with pytest.raises(type(exc)):
                await retryable()(fn)()
        assert fn.await_count == 1
        _sleep.assert_not_awaited()


@pytest.mark.anyio
async def test_cancel_is_never_swallowed():
    from unittest.mock import AsyncMock, patch
    from racing_sync.retry import retryable

    fn = AsyncMock(side_effect=asyncio.CancelledError())
    with patch("asyncio.sleep", new_callable=AsyncMock) as _sleep:
        with pytest.raises(asyncio.CancelledError):
            await retryable()(fn)()
    assert fn.await_count == 1
    _sleep.assert_not_awaited()


@pytest.mark.anyio
async def test_single_attempt_and_custom_delays():
    from unittest.mock import AsyncMock, patch
    from racing_sync.retry import retryable

    fn = AsyncMock(side_effect=TimeoutError("x"))
    with patch("asyncio.sleep", new_callable=AsyncMock) as _sleep:
        with pytest.raises(TimeoutError):
            await retryable(attempts=1)(fn)()
    assert fn.await_count == 1
    _sleep.assert_not_awaited()

    fn2 = AsyncMock(side_effect=[OSError("a"), OSError("b"), "ok"])
    with patch("asyncio.sleep", new_callable=AsyncMock) as _sleep2:
        assert await retryable(5, (0.5,))(fn2)() == "ok"
    assert [c.args[0] for c in _sleep2.await_args_list] == [0.5, 0.5]


def test_is_retryable_read_error_shapes():
    from racing_sync.retry import is_retryable_read_error

    assert is_retryable_read_error(TimeoutError("t")) is True
    assert is_retryable_read_error(ConnectionError("c")) is True
    assert is_retryable_read_error(OSError("o")) is True
    assert is_retryable_read_error(ValueError("v")) is False
    assert is_retryable_read_error(RuntimeError("r")) is False
    try:
        import aiohttp
        from unittest.mock import MagicMock

        r404 = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=404)
        r400 = aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=400)
        assert is_retryable_read_error(r404) is True
        assert is_retryable_read_error(r400) is False
        assert is_retryable_read_error(aiohttp.ClientConnectionError("down")) is True
    except ImportError:
        pass


def test_decorator_preserves_name():
    from racing_sync.retry import retryable

    async def _my_read():
        return 1

    assert retryable()(_my_read).__name__ == "_my_read"
