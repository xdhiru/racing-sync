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
# IPv6 reachable from VPS2, so IPv4-only DNS resolution is the default
# (see HTTPClientConfig.use_ipv6). Dual-stack happy-eyeballs to
# link-local/ULA addresses has wedged handshakes on seedboxes.
_IPV4_ONLY = socket.AF_INET


def _connector_for(cfg: HTTPClientConfig) -> aiohttp.TCPConnector:
    """Shared connector shape: bounded pool, cached DNS, configured family."""
    try:
        use_v6 = bool(getattr(cfg, "use_ipv6", False))
    except Exception:
        use_v6 = False
    return aiohttp.TCPConnector(
        family=socket.AF_UNSPEC if use_v6 else _IPV4_ONLY,
        limit=100,
        limit_per_host=20,
        ttl_dns_cache=300,
    )


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
    host = host.strip()
    if "://" not in host:
        raise ValueError(f"host must include scheme (http:// or https://), got: {host!r}")
    parts = urllib.parse.urlsplit(host)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"host scheme must be http(s)://, got: {host!r}")
    if not parts.hostname:
        raise ValueError(f"host must include a hostname, got: {host!r}")
    if parts.query or parts.fragment:
        raise ValueError(f"host must not include query/fragment, got: {host!r}")
    if parts.username or parts.password:
        # Credentials in the URL would flow into AuthError messages and
        # request logs via host interpolation — refuse (message deliberately
        # omits the host so the secret never lands in logs either).
        raise ValueError("host must not embed credentials (user:pass@); "
                         "use the username/password settings instead")
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
    File-like values are rewound with seek(0) where possible; small
    bytes values are re-buffered.
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
        # Rewind file-likes so retries don't send truncated bodies.
        try:
            if hasattr(value, "seek") and hasattr(value, "tell"):
                try:
                    value.seek(0)
                except Exception:
                    pass
            elif isinstance(value, (bytearray, memoryview)):
                value = bytes(value)
        except Exception:
            pass
        # Headers may be CIMultiDict (not dict) — duck-type the lookup so
        # retries keep the original Content-Type (e.g. application/x-bittorrent).
        try:
            ctype = headers.get("Content-Type") if hasattr(headers, "get") else None
        except Exception:
            ctype = None
        try:
            out.add_field(name, value, filename=filename, content_type=ctype)
        except Exception:
            try:
                out.add_field(name, value)
            except Exception:
                pass
    return out





# Transient gateway / rate-limit statuses worth retrying.
# NOTE: deliberately excludes 500 — qB returns 500 for application errors
# (e.g. missing torrent) that must surface immediately, not be retried.
_TRANSIENT_STATUSES = (408, 425, 429, 502, 503, 504)


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
        # Single-flight re-auth: concurrent 401/403s share one login
        # instead of each worker logging in sequentially (N×3 logins).
        self._auth_future: asyncio.Future | None = None

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
        # RFC 6454: Origin must be scheme://host[:port] without path, params,
        # query, fragment — and MUST NOT include userinfo.
        host_str = str(self._cfg.host)
        host_clean = host_str.strip().rstrip("/")
        parsed = urllib.parse.urlsplit(host_str.strip())
        if parsed.scheme and parsed.hostname:
            origin = f"{parsed.scheme}://{parsed.hostname}"
            try:
                if parsed.port:
                    origin += f":{parsed.port}"
            except ValueError:
                pass
        else:
            origin = host_clean
        headers["Origin"] = origin
        # Referer keeps the configured sub-path (some reverse proxies check
        # it) but must never leak userinfo: rebuild from the parsed,
        # userinfo-stripped origin + path.
        if parsed.scheme and parsed.hostname:
            _path = parsed.path or "/"
            if not _path.endswith("/"):
                _path += "/"
            headers["Referer"] = origin + _path
        else:
            headers["Referer"] = host_clean + "/"

        self._session = aiohttp.ClientSession(
            base_url=_ensure_base_url(self._cfg.host),
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            timeout=aiohttp.ClientTimeout(total=60),
            headers=headers,
            connector=_connector_for(self._cfg),
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
                    # NOTE: do NOT check for bare `type="password"` here —
                    # the qBittorrent login page legitimately contains a
                    # password input even after successful nginx auth, which
                    # caused false-positive AuthErrors.
                    body = await r.text()
                    body_lower = body.lower()
                    if any(
                        marker in body_lower
                        for marker in (
                            "invalid password",
                            "invalid credentials",
                            "login failed",
                            "authentication failed",
                            "access denied",
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

    async def _login_singleflight(self, *, force: bool = False) -> bool:
        """One shared login for initial auth and concurrent 401/403s.

        The first caller becomes the leader and performs the login
        OUTSIDE the lock (no head-of-line blocking); concurrent callers
        wait on one future instead of each logging in sequentially.
        With force=True the cached flag is ignored (re-auth after a
        401/403). The leader retries transient failures (auth refused,
        network blips) with backoff; only a persistent failure returns
        False.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        for _round in range(2):
            async with self._auth_lock:
                if self._authed and not force:
                    return True
                fut = self._auth_future
                if fut is None or fut.done():
                    try:
                        fut = loop.create_future()
                    except Exception:
                        return False
                    self._auth_future = fut
                    owner = True
                else:
                    owner = False
            if not owner:
                try:
                    await fut
                except AuthError:
                    return False
                except asyncio.CancelledError:
                    # Structured shutdown: never swallow cancellation as
                    # "not authed" — propagate so SIGTERM actually stops.
                    raise
                except Exception:
                    return bool(self._authed)
                if bool(self._authed):
                    return True
                # Spurious wakeup: the leader's own retry invalidated the
                # flag again before we ran (or a racing login landed and
                # died). Rejoin the election once instead of failing the
                # request over an ordering artifact.
                force = False
                continue
            break
        else:
            return False
        # Leader: full login+request sequence retried a few times with
        # backoff. The WebUI can transiently refuse auth during startup,
        # after a settings change, or while a session cookie rotates; a
        # single retry is often not enough. Network blips during re-auth
        # retry like auth refusals instead of escaping as connection
        # errors. Only persistent failure escapes (as False).
        try:
            for attempt in range(3):
                self._authed = False
                try:
                    await self._auth(force=True)
                except AuthError as e:
                    log.warning(
                        "[%s] re-auth attempt %d/3 refused (%s)",
                        self._label, attempt + 1, e,
                    )
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                except (aiohttp.ClientError, asyncio.TimeoutError,
                        OSError) as e:
                    log.warning(
                        "[%s] re-auth attempt %d/3 hit transient error (%s)",
                        self._label, attempt + 1, e,
                    )
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                # No exception: the login stands (real _auth() sets the
                # flag itself; test doubles may not — normalize here).
                self._authed = True
                if not fut.done():
                    fut.set_result(True)
                return True
            if not fut.done():
                fut.set_exception(AuthError(
                    f"[{self._label}] re-auth failed after 3 attempts"))
            return False
        except asyncio.CancelledError:
            if not fut.done():
                try:
                    fut.cancel()
                except Exception:
                    pass
            raise
        except Exception as e:  # noqa: BLE001
            if not fut.done():
                try:
                    fut.set_exception(
                        e if isinstance(e, AuthError) else AuthError(str(e)))
                except Exception:
                    pass
            return False
        finally:
            try:
                async with self._auth_lock:
                    if self._auth_future is fut:
                        self._auth_future = None
            except Exception:
                pass

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
            # Initial login also goes through single-flight: workers
            # starting while another task re-authenticates must join it
            # instead of each running a direct login (N× logins storm).
            try:
                _ok = await self._login_singleflight()
            except AuthError as e:
                raise e
            if not _ok:
                raise AuthError(
                    f"[{self._label}] initial authentication failed for {path}"
                )

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
                    aiohttp.ClientConnectionError,
                    asyncio.TimeoutError,
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
            try:
                await r.read()
            finally:
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
            # Single-flight re-auth (forced: the 401/403 proves the cached
            # flag stale): concurrent workers share one login instead of
            # each logging in sequentially. Only a persistent failure (or
            # persistent 401) escapes as AuthError.
            last_exc: AuthError | None = None
            for attempt in range(2):
                try:
                    _ok = await self._login_singleflight(force=True)
                except AuthError as e:
                    last_exc = e
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                if not _ok:
                    last_exc = AuthError(
                        f"[{self._label}] re-auth failed for {path}"
                    )
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                r2 = await _do()
                if r2.status == 401:
                    try:
                        await r2.read()
                    finally:
                        r2.close()
                    # Session rotated again under us: invalidate so the
                    # next round really re-logs in, then retry once more.
                    self._authed = False
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
            try:
                await r.read()
            finally:
                r.close()
            await asyncio.sleep(delay)
            r = await _do()

        if r.status >= 400:
            try:
                try:
                    body = await r.text()
                except Exception:
                    # Binary error pages (or undecodable bytes) must not mask
                    # the original HTTP status.
                    body = f"<undecodable body, status={r.status}>"
            finally:
                try:
                    r.close()
                except Exception:
                    pass
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