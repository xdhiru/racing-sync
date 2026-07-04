"""HTTP helpers with nginx + client auth handling."""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any
import urllib.parse

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


def _add_single_file_field(fd: aiohttp.FormData, field_name: str, file_spec: Any) -> None:
    """Add a file field preserving filename, content_type, and encoding if present."""
    if isinstance(file_spec, (tuple, list)):
        if len(file_spec) == 1:
            val = file_spec[0]
            fname = field_name if isinstance(val, (bytes, bytearray)) else None
            fd.add_field(field_name, val, filename=fname)
        elif len(file_spec) == 2:
            # (filename, content)
            fd.add_field(field_name, file_spec[1], filename=str(file_spec[0]))
        elif len(file_spec) == 3:
            # (filename, content, content_type)
            fd.add_field(
                field_name,
                file_spec[1],
                filename=str(file_spec[0]),
                content_type=str(file_spec[2]) if file_spec[2] else None,
            )
        elif len(file_spec) >= 4:
            # (filename, content, content_type, headers_or_encoding)
            encoding = (
                file_spec[3]
                if isinstance(file_spec[3], str)
                else (file_spec[3].get("Content-Transfer-Encoding") if isinstance(file_spec[3], dict) else None)
            )
            fd.add_field(
                field_name,
                file_spec[1],
                filename=str(file_spec[0]),
                content_type=str(file_spec[2]) if file_spec[2] else None,
                content_transfer_encoding=encoding,
            )
    else:
        fname = field_name if isinstance(file_spec, (bytes, bytearray)) else None
        fd.add_field(field_name, file_spec, filename=fname)


def _populate_form_data(fd: aiohttp.FormData, data: Any, files: Any) -> None:
    if isinstance(data, dict):
        for k, v in data.items():
            fd.add_field(k, str(v))
    if isinstance(files, dict):
        for k, v in files.items():
            _add_single_file_field(fd, k, v)
    elif isinstance(files, (list, tuple)):
        for item in files:
            if isinstance(item, (list, tuple)):
                if len(item) == 2:
                    _add_single_file_field(fd, str(item[0]), item[1])
                elif len(item) >= 3:
                    # Flat tuple: (fieldname, filename, content, [content_type], [headers])
                    _add_single_file_field(fd, str(item[0]), item[1:])


def _clone_formdata(fd: aiohttp.FormData) -> aiohttp.FormData:
    """Rebuild a FormData so retries send a fresh (unconsumed) body.

    aiohttp payload objects can be consumed on the first send; reusing the
    same FormData across network/transient retries risks truncated re-sends
    (notably `torrents/add` with 20MiB blobs). Re-add each stored field.
    """
    try:
        fields = list(getattr(fd, "_fields", []) or [])
    except Exception:
        return fd
    if not fields:
        return fd
    out = aiohttp.FormData()
    try:
        out._quote_fields = getattr(fd, "_quote_fields", True)
        out._charset = getattr(fd, "_charset", None)
    except Exception:
        pass
    for entry in fields:
        try:
            dtype, headers, value = entry
        except Exception:
            continue
        try:
            name = dtype.get("name") if hasattr(dtype, "get") else None
            filename = dtype.get("filename") if hasattr(dtype, "get") else None
        except Exception:
            name, filename = None, None
        if name is None:
            continue
        ctype = headers.get("Content-Type") if isinstance(headers, dict) else None
        try:
            out.add_field(name, value, filename=filename, content_type=ctype)
        except Exception:
            try:
                out.add_field(name, value)
            except Exception:
                pass
    return out





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
        if self._session and not self._session.closed:
            return
        self._authed = False
        headers: dict[str, str] = {}
        # mode="basic": send the Authorization header on every request
        # preemptively. This works against nginx `auth_basic` and any
        # other proxy that follows RFC 7617.
        if self._cfg.nginx_mode == "basic" and self._cfg.username:
            import base64
            pw = self._cfg.password.get_secret_value() if hasattr(self._cfg.password, "get_secret_value") else str(self._cfg.password)
            creds = f"{self._cfg.username}:{pw}".encode()
            headers["Authorization"] = "Basic " + base64.b64encode(creds).decode()
        # Set Origin and Referer to satisfy WebUI CSRF protection (e.g. qBittorrent)
        # RFC 6454: Origin must be scheme://netloc without any path component.
        host_str = str(self._cfg.host)
        host_clean = host_str.rstrip("/")
        parsed = urllib.parse.urlsplit(host_str)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else host_clean
        headers["Origin"] = origin
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
        self._session = None
        self._authed = False

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
                pw = self._cfg.password.get_secret_value() if hasattr(self._cfg.password, "get_secret_value") else str(self._cfg.password)
                form[self._cfg.nginx_pass_field] = pw
                async with self.session.post(
                    self._cfg.nginx_url, data=form, allow_redirects=True
                ) as r:
                    if r.status >= 400:
                        raise AuthError(
                            f"nginx auth failed for {self._label}: HTTP {r.status}"
                        )
                    body = await r.text()
                    body_lower = body.lower()
                    if any(
                        marker in body_lower
                        for marker in (
                            "invalid password",
                            "invalid credentials",
                            "login failed",
                            'type="password"',
                        )
                    ):
                        raise AuthError(
                            f"nginx form auth rejected credentials for {self._label} (HTTP {r.status})"
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

        def _get_request_data() -> Any:
            if files is not None:
                fd = aiohttp.FormData()
                _populate_form_data(fd, data, files)
                return fd
            if isinstance(data, aiohttp.FormData):
                return _clone_formdata(data)
            return data

        path_clean = path.lstrip("/")

        async def _do() -> aiohttp.ClientResponse:
            for attempt in range(3):
                req_data = _get_request_data()
                try:
                    return await self.session.request(
                        method,
                        path_clean,
                        params=params,
                        data=req_data,
                        json=json_body,
                        headers=headers,
                    )
                except (
                    aiohttp.ClientOSError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ClientConnectionResetError,
                    aiohttp.ClientConnectorError,
                    TimeoutError,
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

        # Retry transient gateway / rate-limit errors
        _TRANSIENT_STATUSES = (429, 502, 503, 504)
        for attempt in range(3):
            if r.status not in _TRANSIENT_STATUSES:
                break
            delay = 0.5 * (2 ** attempt)
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = min(5.0, float(retry_after))
                except ValueError:
                    pass
            log.warning(
                "[%s] %s %s -> HTTP %d; retrying in %.1fs (attempt %d/3)",
                self._label, method, path, r.status, delay, attempt + 1,
            )
            await r.read()
            r.close()
            await asyncio.sleep(delay)
            r = await _do()

        if r.status in (401, 403) and retry_auth:
            log.warning(
                "[%s] %s %s -> %d; re-authenticating",
                self._label, method, path, r.status,
            )
            await r.read()
            r.close()
            # Retry the full login+request sequence a few times with
            # backoff. The WebUI can transiently refuse auth during
            # startup, after a settings change, or while a session
            # cookie is being rotated; a single retry is often not
            # enough. Only AuthError (or persistent 401) escapes.
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
                if r2.status == 401:
                    try:
                        await r2.read()
                    finally:
                        r2.close()
                    last_exc = AuthError(
                        f"[{self._label}] auth failed: HTTP 401 on {path}"
                    )
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                elif r2.status == 403:
                    # 403 after successful login indicates permission/CSRF denial,
                    # not an auth credential failure. Do not conflate with AuthError.
                    r = r2
                    break
                # Success or other status — return r2 (or fall through to error path below).
                r = r2
                break
            else:
                # All attempts exhausted.
                assert last_exc is not None
                raise last_exc

        # A post-login response can still be a transient gateway/rate-limit
        # error — retry it instead of surfacing a hard failure.
        for attempt in range(3):
            if r.status not in _TRANSIENT_STATUSES:
                break
            delay = 0.5 * (2 ** attempt)
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = min(5.0, float(retry_after))
                except ValueError:
                    pass
            log.warning(
                "[%s] %s %s -> HTTP %d after re-auth; retrying in %.1fs (attempt %d/3)",
                self._label, method, path, r.status, delay, attempt + 1,
            )
            await r.read()
            r.close()
            await asyncio.sleep(delay)
            r = await _do()

        if r.status >= 400:
            try:
                body = await r.text()
            finally:
                r.close()
            raise aiohttp.ClientResponseError(
                request_info=r.request_info,
                history=r.history,
                status=r.status,
                message=body[:500],
            )
        return r


async def get_json_async(client: HTTPClientBase, path: str, **params: Any) -> Any:
    async with await client.request("GET", path, params=params or None) as r:
        return await r.json()


async def get_bytes_async(client: HTTPClientBase, path: str, **params: Any) -> bytes:
    async with await client.request("GET", path, params=params or None) as r:
        return await r.read()