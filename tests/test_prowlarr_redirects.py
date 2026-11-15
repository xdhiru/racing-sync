"""Regression tests for G1: download_torrent must not follow redirects blindly.

`download_torrent` validates the initial enclosure URL, but a 302 from
Prowlarr/an indexer/a proxy can point anywhere. aiohttp strips its own
Authorization/Cookie on a host change but not the custom X-Api-Key, so
automatic redirects both leaked the key and bypassed the URL gate (SSRF
towards link-local/metadata hosts). Redirects are now followed manually
with per-hop validation and per-hop auth decisions.
"""

from __future__ import annotations

import pytest

from racing_sync.config import DownloadIndexerConfig, ProwlarrConfig
from racing_sync.prowlarr import ProwlarrClient, ProwlarrError, TorrentHit

_BODY = b"d8:announce14:http://tracker"

_REDIRECT_STATUSES = (301, 302, 303, 307, 308)


class _FakeContent:
    def __init__(self, body: bytes) -> None:
        self._body = body

    async def iter_chunked(self, size: int):
        for start in range(0, len(self._body), size):
            yield self._body[start : start + size]


class _FakeResponse:
    def __init__(self, status: int, body: bytes = b"", headers: dict | None = None) -> None:
        self.status = status
        self.headers = dict(headers or {})
        if body:
            self.headers.setdefault("Content-Length", str(len(body)))
        self.content = _FakeContent(body)

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise AssertionError(f"unexpected HTTP status {self.status} in fake response")


class _FakeResponseCM:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    async def __aenter__(self) -> _FakeResponse:
        return self._response

    async def __aexit__(self, *exc) -> bool:
        return False


class _FakeSession:
    """Session double replaying canned responses and recording every GET."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []

    def get(self, url, **kwargs) -> _FakeResponseCM:
        self.calls.append({"url": url, **kwargs})
        if not self._responses:
            raise AssertionError(f"unexpected extra GET: {url!r}")
        return _FakeResponseCM(self._responses.pop(0))


def _client(base_url: str = "http://127.0.0.1:9696") -> ProwlarrClient:
    cfg = ProwlarrConfig(
        enabled=True,
        base_url=base_url,
        api_key="secret",
        download_indexers=[DownloadIndexerConfig(name="indexer1")],
    )
    return ProwlarrClient(cfg)


def _hit(url: str) -> TorrentHit:
    return TorrentHit(
        title="Test",
        guid="1",
        indexer="indexer1",
        indexer_id=1,
        size_bytes=100,
        download_url=url,
        magnet_url="",
        info_url="",
        publish_date="",
    )


def _redirect(location: str, status: int = 302) -> _FakeResponse:
    return _FakeResponse(status, headers={"Location": location})


def _api_key(headers) -> str | None:
    return (headers or {}).get("X-Api-Key")


@pytest.mark.anyio
async def test_same_origin_redirect_is_followed_with_key():
    client = _client()
    session = _FakeSession(
        [
            _redirect("/download/2"),
            _FakeResponse(200, body=_BODY),
        ]
    )
    client._session = session

    data = await client.download_torrent(_hit("http://127.0.0.1:9696/api/v1/download/1"))

    assert data == _BODY
    assert len(session.calls) == 2
    assert session.calls[0]["url"] == "http://127.0.0.1:9696/api/v1/download/1"
    assert session.calls[1]["url"] == "http://127.0.0.1:9696/download/2"
    # Same origin as base_url on both hops: the key must be sent to both.
    assert _api_key(session.calls[0]["headers"]) == "secret"
    assert _api_key(session.calls[1]["headers"]) == "secret"
    # Redirect chasing is manual, never delegated to aiohttp.
    assert all(call["allow_redirects"] is False for call in session.calls)


@pytest.mark.anyio
async def test_cross_origin_redirect_drops_api_key():
    client = _client()
    session = _FakeSession(
        [
            _redirect("http://evil.example/file"),
            _FakeResponse(200, body=_BODY),
        ]
    )
    client._session = session

    data = await client.download_torrent(_hit("http://127.0.0.1:9696/api/v1/download/1"))

    assert data == _BODY
    assert len(session.calls) == 2
    # Initial same-origin request authenticates...
    assert _api_key(session.calls[0]["headers"]) == "secret"
    # ...but the cross-origin hop must carry no credentials at all.
    assert session.calls[1]["url"] == "http://evil.example/file"
    assert _api_key(session.calls[1].get("headers")) is None


@pytest.mark.anyio
async def test_redirect_to_link_local_literal_is_refused():
    client = _client()
    session = _FakeSession(
        [
            _redirect("http://169.254.169.254/latest/meta-data?token=SECRET"),
        ]
    )
    client._session = session

    with pytest.raises(ProwlarrError) as exc_info:
        await client.download_torrent(_hit("http://127.0.0.1:9696/api/v1/download/1"))

    # Local verdict: refuse before issuing a second request.
    assert len(session.calls) == 1
    msg = str(exc_info.value)
    assert "169.254.169.254" in msg
    # Query material is scrubbed from the reported target.
    assert "SECRET" not in msg


@pytest.mark.anyio
async def test_redirect_loop_over_cap_is_refused():
    client = _client()
    session = _FakeSession([_redirect("/next") for _ in range(6)])
    client._session = session

    with pytest.raises(ProwlarrError, match="redirect"):
        await client.download_torrent(_hit("http://127.0.0.1:9696/api/v1/download/1"))

    # 1 initial + 5 allowed hops; the 6th Location is refused.
    assert len(session.calls) == 6
