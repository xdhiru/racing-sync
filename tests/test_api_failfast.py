"""Supervisor fail-fast: a dead enabled API stops the daemon."""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from conftest import make_coordinator


def _coord(api_enabled: bool):
    coord = make_coordinator()
    coord.cfg = MagicMock()
    coord.cfg.api.enabled = api_enabled
    coord._api_task = None
    return coord


@pytest.mark.anyio
async def test_no_task_no_fail():
    assert _coord(True)._api_task_failed() is False


@pytest.mark.anyio
async def test_disabled_api_ignores_dead_task():
    coord = _coord(False)
    t: asyncio.Future = asyncio.Future()
    t.set_exception(OSError("port in use"))
    coord._api_task = t
    assert coord._api_task_failed() is False
    t.exception()  # retrieve: unretrieved failures fail the loop


@pytest.mark.anyio
async def test_failed_task_stops():
    coord = _coord(True)
    t: asyncio.Future = asyncio.Future()
    t.set_exception(OSError("port in use"))
    coord._api_task = t
    assert coord._api_task_failed() is True


@pytest.mark.anyio
async def test_clean_or_pending_task_keeps_running():
    coord = _coord(True)
    done_ok: asyncio.Future = asyncio.Future()
    done_ok.set_result(None)
    coord._api_task = done_ok
    assert coord._api_task_failed() is False
    cancelled: asyncio.Future = asyncio.Future()
    cancelled.cancel()
    coord._api_task = cancelled
    assert coord._api_task_failed() is False
    coord._api_task = asyncio.Future()
    assert coord._api_task_failed() is False
