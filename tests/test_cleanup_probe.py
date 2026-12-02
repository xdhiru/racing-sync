"""Cleanup free-space probe: ask Deluge over RPC, fall back to SFTP.

The janitor needs VPS1 free space to scale deletion grace. Measuring it
over SFTP competes with .torrent fetches for the pool and goes blind when
the transport wedges; `core.get_free_space` is one daemon RPC.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync.clients.deluge import DelugeClient
from racing_sync.config import AppConfig

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _split() -> AppConfig:
    return AppConfig.from_toml(FIXTURES / "split.toml")


@pytest.mark.anyio
async def test_rpc_probe_answers_without_touching_sftp():
    coord = make_coordinator(cfg=_split())
    coord.source_client = SimpleNamespace(get_free_space=AsyncMock(return_value=1234))
    coord.sftp = MagicMock()

    assert await coord._source_free_bytes() == 1234

    coord.sftp.disk_free_bytes.assert_not_called()


@pytest.mark.anyio
async def test_unknown_rpc_answer_falls_back_to_sftp():
    coord = make_coordinator(cfg=_split())
    coord.source_client = SimpleNamespace(get_free_space=AsyncMock(return_value=None))
    coord.sftp = MagicMock()
    coord.sftp.disk_free_bytes = MagicMock(return_value=4242)

    assert await coord._source_free_bytes() == 4242

    coord.sftp.disk_free_bytes.assert_called_once()


@pytest.mark.anyio
async def test_rpc_error_falls_back_to_sftp():
    coord = make_coordinator(cfg=_split())
    coord.source_client = SimpleNamespace(
        get_free_space=AsyncMock(side_effect=RuntimeError("boom"))
    )
    coord.sftp = MagicMock()
    coord.sftp.disk_free_bytes = MagicMock(return_value=999)

    assert await coord._source_free_bytes() == 999


@pytest.mark.anyio
async def test_client_without_the_method_falls_back():
    coord = make_coordinator(cfg=_split())
    coord.source_client = SimpleNamespace()
    coord.sftp = MagicMock()
    coord.sftp.disk_free_bytes = MagicMock(return_value=77)

    assert await coord._source_free_bytes() == 77


@pytest.mark.anyio
async def test_qbittorrent_source_has_no_probe():
    coord = make_coordinator(cfg=AppConfig.from_toml(FIXTURES / "minimal.toml"))
    coord.source_client = SimpleNamespace(get_free_space=AsyncMock(return_value=1))
    coord.sftp = None

    assert await coord._source_free_bytes() is None


@pytest.mark.anyio
async def test_deluge_client_parses_free_space():
    client = DelugeClient(_split().source)
    client._rpc = AsyncMock(return_value=4294967296)

    assert await client.get_free_space("/home/deluge/.config/deluge/state") == 4294967296
    client._rpc.assert_awaited_once_with(
        "core.get_free_space", ["/home/deluge/.config/deluge/state"]
    )


@pytest.mark.anyio
@pytest.mark.parametrize("raw", [None, "nope", True, -1])
async def test_deluge_free_space_rejects_nonsense(raw):
    client = DelugeClient(_split().source)
    client._rpc = AsyncMock(return_value=raw)

    assert await client.get_free_space("/state") is None


@pytest.mark.anyio
async def test_deluge_free_space_error_is_none():
    client = DelugeClient(_split().source)
    client._rpc = AsyncMock(side_effect=RuntimeError("Unknown method"))

    assert await client.get_free_space("/state") is None
