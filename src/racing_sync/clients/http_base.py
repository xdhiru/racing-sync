"""HTTP helpers with nginx + client auth handling."""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any

import aiohttp

from ..config import HTTPClientConfig

log = logging.getLogger(__name__)


# The racing client and destination qBittorrent instances may not have
# IPv6 reachable from VPS2. We force IPv4-only DNS resolution across the
# whole app so we never try to connect to a v6 address and hang.
_IPV4_ONLY = socket.AF_INET


class AuthError(RuntimeError):
    pass


def _ensure_base_url(host: str) -> str:
    """Normalise a config-provided host into a valid aiohttp base_url.

    aiohttp requires the base_url to end with exactly one '/'. We strip
    any trailing slashes and add exactly one back, so the user can
    provide either `http://host`, `http://host/`, or
    `http://host/some/path` and we end up with `http://host/some/path/`.

    Raises ValueError for empty / malformed input.
    """
    if not host or not host.strip():
        raise ValueError("host must not be empty")
    if "://" not in host:
        raise ValueError(f"host must include scheme (http:// or https://), got: {host!r}")
    return host.rstrip("/") + "/"





class HTTPClientBase:
    """Wraps aiohttp with:
      - persistent cookie jar
      - optional nginx basic-auth bypass (POST creds, reuse session cookie)
      - 401/403 retry with re-login
    """

    def __init__(self, cfg: HTTPClientConfig, label: str = "client"):
        self._cfg = cfg
        self._label = label
        self._session: aiohttp.ClientSession | None = None
        self._auth_lock = asyncio.Lock()
        self._authed = False

    async def start(self) -> None:
        headers: dict[str, str] = {}
        # mode="basic": send the Authorization header on every request
        # preemptively. This works against nginx `auth_basic` and any
        # other proxy that follows RFC 7617.
        if self._cfg.nginx_mode == "basic" and self._cfg.username:
            import base64
            creds = f"{self._cfg.username}:{self._cfg.password}".encode()
            headers["Authorization"] = "Basic " + base64.b64encode(creds).decode()
        # Set Origin and Referer to satisfy WebUI CSRF protection (e.g. qBittorrent)
        host_clean = str(self._cfg.host).rstrip("/")
        headers["Origin"] = host_clean
        headers["Referer"] = host_clean + "/"

        self._session = aiohttp.ClientSession(
            base_url=_ensure_base_url(self._cfg.host),
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            timeout=aiohttp.ClientTimeout(total=60),
            headers=headers,
            connector=aiohttp.TCPConnector(family=_IPV4_ONLY),
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._session:
            raise RuntimeError("HTTP session not started")
        return self._session

    async def _auth(self, force: bool = False) -> None:
        async with self._auth_lock:
            if self._authed and not force:
                return
            if self._cfg.nginx_mode == "form_post":
                if not self._cfg.nginx_url:
                    raise AuthError(
                        f"[{self._label}] nginx_mode=form_post but no nginx_url"
                    )
                log.debug(
                    "[%s] nginx form POST %s", self._label, self._cfg.nginx_url
                )
                form = dict(self._cfg.nginx_extra_fields)
                form[self._cfg.nginx_user_field] = self._cfg.username
                form[self._cfg.nginx_pass_field] = self._cfg.password
                async with self.session.post(self._cfg.nginx_url, data=form) as r:
                    if r.status >= 400:
                        raise AuthError(
                            f"nginx auth failed for {self._label}: HTTP {r.status}"
                        )
            # mode="basic": nothing to do here; the Authorization header
            # was set in start() and travels with every request.
            await self._do_client_auth()
            self._authed = True

    async def _do_client_auth(self) -> None:
        raise NotImplementedError

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: Any = None,
        files: Any = None,
        json_body: Any | None = None,
        headers: dict[str, str] | None = None,
        retry_auth: bool = True,
    ) -> aiohttp.ClientResponse:
        if not self._authed:
            await self._auth()

        path_clean = path.lstrip("/")

        async def _do() -> aiohttp.ClientResponse:
            for attempt in range(3):
                try:
                    return await self.session.request(
                        method,
                        path_clean,
                        params=params,
                        data=data,
                        json=json_body,
                        headers=headers,
                    )
                except (
                    aiohttp.ClientOSError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ClientConnectionResetError,
                ) as e:
                    if attempt == 2:
                        raise
                    log.warning(
                        "[%s] %s %s network error (%s); retrying in %.1fs",
                        self._label, method, path, e, 0.5 * (attempt + 1),
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
            raise RuntimeError("unreachable")

        r = await _do()
        if r.status in (401, 403) and retry_auth:
            log.warning(
                "[%s] %s %s -> %d; re-authenticating",
                self._label, method, path, r.status,
            )
            # Retry the full login+request sequence a few times with
            # backoff. The WebUI can transiently refuse auth during
            # startup, after a settings change, or while a session
            # cookie is being rotated; a single retry is often not
            # enough. Only AuthError (or persistent 401/403) escapes.
            last_exc: AuthError | None = None
            for attempt in range(3):
                self._authed = False
                try:
                    await self._auth(force=True)
                except AuthError as e:
                    last_exc = e
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                r2 = await _do()
                if r2.status in (401, 403):
                    await r2.read()
                    last_exc = AuthError(
                        f"[{self._label}] auth failed: HTTP {r2.status} on {path}"
                    )
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                # Success — return r2 (or fall through to error path below).
                r = r2
                break
            else:
                # All attempts exhausted.
                assert last_exc is not None
                raise last_exc
            # If we broke out of the loop with r set, fall through.
            if r.status >= 400:
                body = await r.text()
                raise aiohttp.ClientResponseError(
                    request_info=r.request_info,
                    history=r.history,
                    status=r.status,
                    message=body[:500],
                )
            return r

        if r.status >= 400:
            body = await r.text()
            raise aiohttp.ClientResponseError(
                request_info=r.request_info,
                history=r.history,
                status=r.status,
                message=body[:500],
            )
        return r


def get_json(client: HTTPClientBase, path: str, **params: Any) -> "asyncio.Future[Any]":
    async def _go() -> Any:
        async with await client.request("GET", path, params=params or None) as r:
            return await r.json()
    return asyncio.ensure_future(_go())


async def get_json_async(client: HTTPClientBase, path: str, **params: Any) -> Any:
    async with await client.request("GET", path, params=params or None) as r:
        return await r.json()


async def get_bytes_async(client: HTTPClientBase, path: str, **params: Any) -> bytes:
    async with await client.request("GET", path, params=params or None) as r:
        return await r.read()