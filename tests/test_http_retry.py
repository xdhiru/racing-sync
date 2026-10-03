"""POST/mutation retry gate: only idempotent requests re-send blindly.

A lost response never proves the server skipped the call, so mutations
(qB POSTs, Deluge non-read RPCs) get exactly one attempt. Reads keep all
retries. Deluge reads run over POST too, classified by RPC method prefix.
"""
from __future__ import annotations

import aiohttp
import pytest
from conftest import make_coordinator  # noqa: F401 (keeps sys.path consistent)

from racing_sync.clients.http_base import (
    HTTPClientBase,
    _is_idempotent_request,
)
from racing_sync.config import HTTPClientConfig


def test_idempotent_classifier():
    assert _is_idempotent_request("GET", "/api/v2/torrents/info", None) is True
    assert _is_idempotent_request("get", "json", None) is True
    assert _is_idempotent_request("POST", "/api/v2/torrents/add", None) is False
    assert _is_idempotent_request("POST", "/api/v2/torrents/delete", None) is False
    assert _is_idempotent_request(
        "POST", "json", {"method": "core.get_torrents_status"}) is True
    assert _is_idempotent_request(
        "POST", "json", {"method": "core.add_torrent_file"}) is False
    assert _is_idempotent_request(
        "POST", "json", {"method": "core.remove_torrent"}) is False
    assert _is_idempotent_request(
        "POST", "json", {"method": "web.connect"}) is True
    assert _is_idempotent_request("POST", "json", None) is False
    assert _is_idempotent_request(None, None, None) is False


class _FakeResponse:
    def __init__(self, status=200, headers=None):
        self.status = status
        self.headers = headers or {}
        self.request_info = None
        self.history = ()

    async def read(self):
        return b""

    def close(self):
        pass

    @property
    def content(self):
        class _C:
            async def iter_chunked(self, _n):
                return
                yield
        return _C()


class _FakeSession:
    def __init__(self, script):
        self._script = list(script)
        self.calls = 0

    async def request(self, *args, **kwargs):
        self.calls += 1
        effect = self._script.pop(0) if self._script else _FakeResponse(200)
        if isinstance(effect, BaseException):
            raise effect
        return effect


def _client(session):
    cfg = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        username="user",
        password="pass",
        nginx_mode="off",
    )
    client = HTTPClientBase(cfg, label="test-retry-gate")
    client._session = session
    client._authed = True
    return client


@pytest.mark.anyio
async def test_get_retries_network_errors():
    sess = _FakeSession([
        aiohttp.ClientConnectionError("blip"),
        _FakeResponse(200),
    ])
    client = _client(sess)
    r = await client.request("GET", "/api/v2/torrents/info")
    assert r.status == 200
    assert sess.calls == 2


@pytest.mark.anyio
async def test_post_mutation_single_shot_on_network_error():
    sess = _FakeSession([aiohttp.ClientConnectionError("blip")])
    client = _client(sess)
    with pytest.raises(aiohttp.ClientConnectionError):
        await client.request(
            "POST", "/api/v2/torrents/add", data={"x": "y"})
    assert sess.calls == 1


@pytest.mark.anyio
async def test_deluge_read_rpc_retries():
    sess = _FakeSession([
        aiohttp.ClientConnectionError("blip"),
        _FakeResponse(200),
    ])
    client = _client(sess)
    r = await client.request(
        "POST", "json",
        json_body={"method": "core.get_torrents_status", "params": []})
    assert r.status == 200
    assert sess.calls == 2


@pytest.mark.anyio
async def test_deluge_mutation_rpc_single_shot():
    sess = _FakeSession([aiohttp.ClientConnectionError("blip")])
    client = _client(sess)
    with pytest.raises(aiohttp.ClientConnectionError):
        await client.request(
            "POST", "json",
            json_body={"method": "core.add_torrent_file", "params": []})
    assert sess.calls == 1


@pytest.mark.anyio
async def test_transient_status_retried_for_get_only():
    sess = _FakeSession([_FakeResponse(503), _FakeResponse(200)])
    client = _client(sess)
    r = await client.request("GET", "/api/v2/torrents/info")
    assert r.status == 200
    assert sess.calls == 2


@pytest.mark.anyio
async def test_transient_status_not_retried_for_mutation():
    sess = _FakeSession([_FakeResponse(503), _FakeResponse(200)])
    client = _client(sess)
    with pytest.raises(aiohttp.ClientResponseError):
        await client.request("POST", "/api/v2/torrents/delete")
    assert sess.calls == 1
