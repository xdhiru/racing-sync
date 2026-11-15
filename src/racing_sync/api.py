"""Optional FastAPI control plane.

Endpoints:
  GET  /api/state            -> all rows from state DB
  GET  /api/active           -> only in-flight rows
  GET  /api/logs?limit=N     -> recent run_log rows
  POST /api/recover          -> trigger reconciler now
  POST /api/retry/{hash}     -> FAILED -> QUEUED
  POST /api/forget/{hash}    -> abandon a torrent (row + dest entries + SSD data)
  GET  /api/ssd              -> free bytes on the configured SSD path
  POST /api/scan-watch       -> force a watch-dir scan
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from pydantic import BaseModel

try:
    from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    Depends = Header = HTTPException = Query = Request = object  # type: ignore[assignment,misc]
    FastAPI = Any  # type: ignore[assignment,misc]

from .config import AppConfig
from .coordinator import Coordinator
from .coordinator_errors import AbandonedError
from .forget import forget_torrent
from .rclone_ops import ssd_free_bytes
from .recovery import reconcile
from .state import State

log = logging.getLogger(__name__)


_INFOHASH_RE = re.compile(r"[0-9a-f]{40}")

# Brute-force throttle: per-IP auth-failure timestamps (monotonic),
# last-seen map for LRU eviction, and per-/24 buckets so a many-IP
# spray from one subnet trips the same limit (IPv4 only; IPv6 and
# hostnames stay per-key).
_AUTH_FAILURES: dict[str, list[float]] = {}
_AUTH_LAST_SEEN: dict[str, float] = {}
_AUTH_SUBNET_FAILURES: dict[str, list[float]] = {}
_AUTH_WINDOW_S = 60.0
_AUTH_MAX_FAILURES = 10
_AUTH_CAP = 1000

#: Authed-endpoint rate-limit buckets: per-IP request timestamps
#: (monotonic). 300/min is far above any legitimate poller (15s
#: metrics + operator use) and far below abuse.
_AUTHD_HITS: dict[str, list[float]] = {}


def _subnet_key(ip: str) -> str | None:
    """`/24` bucket for an IPv4 literal; None otherwise. Never raises."""
    try:
        import ipaddress as _ip

        try:
            addr = _ip.ip_address((ip or "").strip())
        except ValueError:
            return None
        if isinstance(addr, _ip.IPv4Address):
            return str(_ip.ip_network(f"{addr}/24", strict=False))
    except Exception:
        pass
    return None


def _prune_bucket(bucket: list[float], cutoff: float) -> list[float]:
    try:
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)
    except Exception:
        pass
    return bucket


def _note_auth_failure(ip: str) -> bool:
    """Record an auth failure; True when the IP is now throttled (429)."""
    now = time.monotonic()
    try:
        fails = _AUTH_FAILURES.get(ip)
        if not isinstance(fails, list):
            fails = []
            _AUTH_FAILURES[ip] = fails
        cutoff = now - _AUTH_WINDOW_S
        _prune_bucket(fails, cutoff)
        fails.append(now)
        try:
            _AUTH_LAST_SEEN[ip] = now
        except Exception:
            pass
        throttled = len(fails) > _AUTH_MAX_FAILURES
        # Same window against the whole /24: a spray of distinct source
        # IPs from one subnet trips the limit together.
        try:
            _sub = _subnet_key(ip)
        except Exception:
            _sub = None
        if _sub is not None:
            try:
                _sfails = _AUTH_SUBNET_FAILURES.get(_sub)
                if not isinstance(_sfails, list):
                    _sfails = []
                    _AUTH_SUBNET_FAILURES[_sub] = _sfails
                _prune_bucket(_sfails, cutoff)
                _sfails.append(now)
                try:
                    _AUTH_LAST_SEEN[f"subnet:{_sub}"] = now
                except Exception:
                    pass
                if len(_sfails) > _AUTH_MAX_FAILURES:
                    throttled = True
            except Exception:
                pass
        if len(_AUTH_FAILURES) + len(_AUTH_SUBNET_FAILURES) > _AUTH_CAP:
            # Evict idle/expired entries first (a many-IP spray must not
            # flush real throttles); then least-recently-seen (LRU) down
            # to the cap — never a blind oldest-half flush of live bans.
            try:
                for k, v in list(_AUTH_FAILURES.items()):
                    try:
                        if not v or all(t < cutoff for t in v):
                            _AUTH_FAILURES.pop(k, None)
                            _AUTH_LAST_SEEN.pop(k, None)
                    except Exception:
                        _AUTH_FAILURES.pop(k, None)
            except Exception:
                pass
            try:
                for k, v in list(_AUTH_SUBNET_FAILURES.items()):
                    try:
                        if not v or all(t < cutoff for t in v):
                            _AUTH_SUBNET_FAILURES.pop(k, None)
                            _AUTH_LAST_SEEN.pop(f"subnet:{k}", None)
                    except Exception:
                        _AUTH_SUBNET_FAILURES.pop(k, None)
            except Exception:
                pass
            try:
                _over = len(_AUTH_FAILURES) + len(_AUTH_SUBNET_FAILURES) - _AUTH_CAP
                if _over > 0:
                    _ranked = sorted(
                        ((k, float(_AUTH_LAST_SEEN.get(k, 0.0))) for k in list(_AUTH_FAILURES)),
                        key=lambda kv: kv[1],
                    )
                    for k, _ in _ranked[:_over]:
                        _AUTH_FAILURES.pop(k, None)
                        _AUTH_LAST_SEEN.pop(k, None)
            except Exception:
                pass
        return throttled
    except Exception:
        return False


def _host_is_trusted(client_host: str, trusted_proxies: set[str]) -> bool:
    """Normalize both sides before comparing.

    `request.client.host` is an IP literal (never the string "localhost")
    and may arrive as IPv6-mapped IPv4 (`::ffff:127.0.0.1`) behind dual-stack
    servers; "localhost" in config means loopback (127.0.0.1/::1).
    Entries may be plain IPs, "localhost", or CIDR ranges
    ("10.0.0.0/8", "::1/128") for reverse-proxy subnets.
    """
    import ipaddress as _ip

    host = (client_host or "").strip().lower()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if host.startswith("::ffff:"):
        host = host[7:]
    try:
        host_ip = _ip.ip_address(host)
    except ValueError:
        host_ip = None
    expanded: set[str] = set()
    networks: list = []
    for entry in trusted_proxies:
        e = (entry or "").strip().lower().strip("[]")
        if e == "localhost":
            expanded.update({"127.0.0.1", "::1"})
        elif "/" in e:
            try:
                networks.append(_ip.ip_network(e, strict=False))
            except ValueError:
                continue
        elif e:
            expanded.add(e)
    if host in expanded:
        return True
    if host_ip is not None:
        for net in networks:
            try:
                if host_ip in net:
                    return True
            except Exception:
                continue
    return False


@asynccontextmanager
async def _hold_ops_lock(coord: Coordinator) -> AsyncIterator[None]:
    """Serialize API-triggered ops against the coordinator tick.

    Tolerates hand-built test doubles without a real lock.
    """
    lock = getattr(coord, "_ops_lock", None)
    if lock is None or not hasattr(lock, "__aenter__"):
        yield
    else:
        async with lock:
            yield


#: Readiness gates on the two intake/scheduling lanes; janitor and
#: qb_listener are reported for observability but never gate readiness.
_REQUIRED_LANES = ("poller", "scheduler")
_ALL_LANES = ("poller", "scheduler", "janitor", "qb_listener")
#: Default loop interval when config is absent/broken (MagicMock doubles).
_LANE_DEFAULT_INTERVAL_S = 30.0
#: qb_listener loop cadence (coordinator_live._QB_LISTENER_INTERVAL_S).
_QB_LISTENER_INTERVAL_S = 4.0
#: Janitor default when [cleanup] is absent; its loop floors at 300s.
_JANITOR_DEFAULT_INTERVAL_S = 3600.0
_JANITOR_MIN_INTERVAL_S = 300.0
#: Freshness floor: a slow configured interval still gets a wide window.
_LANE_MIN_FRESH_S = 300.0
#: Legacy `_last_tick_at` window for test doubles / single-pass callers.
_LEGACY_TICK_FRESH_S = 300.0


def _strict_float(value: object) -> float | None:
    """Strict numeric coercion: bools, doubles, NaN/inf read as None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _strict_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(0, int(value))


def _strict_text(value: object, limit: int = 300) -> str:
    if not isinstance(value, str):
        return ""
    return value[:limit]


def _lane_entry(raw: object) -> dict[str, Any]:
    """Defensive, strict-typed lane entry (never raises)."""
    src = raw if isinstance(raw, dict) else {}
    duration = _strict_float(src.get("last_duration_s"))
    return {
        "last_attempt_at": _strict_float(src.get("last_attempt_at")),
        "last_success_at": _strict_float(src.get("last_success_at")),
        "last_failure_at": _strict_float(src.get("last_failure_at")),
        "last_duration_s": (max(0.0, duration) if duration is not None else None),
        "consecutive_failures": _strict_count(src.get("consecutive_failures")),
        "last_error": _strict_text(src.get("last_error")),
    }


def _lane_health(coord: Coordinator) -> dict[str, dict[str, Any]]:
    """Strict snapshot of every lane; absent lanes read as empty entries."""
    snap = None
    try:
        fn = getattr(coord, "lane_health_snapshot", None)
        if callable(fn):
            snap = fn()
    except Exception:
        snap = None
    out: dict[str, dict[str, Any]] = {}
    for name in _ALL_LANES:
        raw = snap.get(name) if isinstance(snap, dict) else None
        out[name] = _lane_entry(raw)
    return out


def _lane_interval(cfg: AppConfig, name: str) -> float:
    try:
        value = getattr(getattr(cfg, "general", None), name, None)
    except Exception:
        return _LANE_DEFAULT_INTERVAL_S
    interval = _strict_float(value)
    if interval is None or interval <= 0:
        return _LANE_DEFAULT_INTERVAL_S
    return interval


def _janitor_interval(cfg: AppConfig) -> float:
    """Janitor cadence: configured interval floored like the loop itself."""
    try:
        value = getattr(getattr(cfg, "cleanup", None), "janitor_interval_seconds", None)
    except Exception:
        return _JANITOR_DEFAULT_INTERVAL_S
    interval = _strict_float(value)
    if interval is None or interval <= 0:
        interval = _JANITOR_DEFAULT_INTERVAL_S
    return max(_JANITOR_MIN_INTERVAL_S, interval)


def _lane_threshold(interval: float) -> float:
    return max(_LANE_MIN_FRESH_S, 3.0 * interval)


def _lane_fresh(
    entry: dict[str, Any],
    *,
    now: float,
    threshold: float,
    started_at: float | None,
    legacy_tick: float | None,
) -> bool:
    """Lane freshness (H1).

    A success is the anchor; a newer in-flight attempt replaces it so a
    legitimately long pass is not declared stale mid-run (a hung one
    still trips after the threshold). No success yet falls back to the
    attempt, then to startup grace, then to the legacy tick field used
    by test doubles / single-pass callers.
    """
    success = entry.get("last_success_at")
    attempt = entry.get("last_attempt_at")
    if success is not None:
        if attempt is not None and attempt > success:
            ref = attempt
        else:
            ref = success
    elif attempt is not None:
        ref = attempt
    elif started_at is not None:
        ref = started_at
    elif legacy_tick is not None:
        ref = legacy_tick
        threshold = _LEGACY_TICK_FRESH_S
    else:
        return False
    age = now - ref
    return 0.0 <= age <= threshold


def _lane_body(
    entry: dict[str, Any],
    *,
    required: bool,
    ok: bool,
    now: float,
) -> dict[str, Any]:
    def _age(value: float | None) -> float | None:
        return round(max(0.0, now - value), 3) if value is not None else None

    return {
        "required": bool(required),
        "ok": bool(ok),
        "last_success_age_s": _age(entry.get("last_success_at")),
        "last_attempt_age_s": _age(entry.get("last_attempt_at")),
        "last_duration_s": entry.get("last_duration_s"),
        "consecutive_failures": entry.get("consecutive_failures", 0),
        "last_error": entry.get("last_error") or "",
    }


class RetryResult(BaseModel):
    source_infohash: str
    new_state: str


class ForgetResult(BaseModel):
    source_infohash: str
    source_name: str
    applied: bool
    dest_entries: list[str]
    local_paths: list[str]
    skipped_paths: list[str]
    errors: list[str]
    ignored: bool = False
    paired_cancelled: list[dict[str, Any]] = []


def build_app(coord: Coordinator) -> FastAPI:
    if not HAS_FASTAPI:
        raise RuntimeError(
            "FastAPI is required to run the control plane API. "
            "Install it via 'pip install racing-sync[api]'."
        )
    cfg = coord.cfg
    # No unauthenticated docs/openapi endpoints: the control plane sits
    # behind token/nginx auth, its schema surface should too.
    app = FastAPI(
        title="racing-sync", version="0.1.0", docs_url=None, openapi_url=None, redoc_url=None
    )

    # Request guards: 1 MiB body cap (no endpoint takes a body — query
    # params only) + a generous authed rate limit. /healthz stays
    # unguarded (load-balancer liveness must never 429).
    try:
        from fastapi.responses import JSONResponse as _JSONResponse

        @app.middleware("http")
        async def _request_guards(request, call_next):
            try:
                _path = request.url.path or ""
            except Exception:
                _path = ""
            if _path != "/healthz":
                try:
                    _cl = request.headers.get("content-length")
                    if _cl is not None and int(str(_cl).strip()) > 1048576:
                        return _JSONResponse({"detail": "body too large"}, status_code=413)
                except Exception:
                    pass
                try:
                    _rip = request.client.host if request.client else "unknown"
                except Exception:
                    _rip = "unknown"
                try:
                    _bucket = _AUTHD_HITS.get(_rip)
                    if not isinstance(_bucket, list):
                        _bucket = []
                        _AUTHD_HITS[_rip] = _bucket
                    _now = time.monotonic()
                    _cut = _now - 60.0
                    while _bucket and _bucket[0] < _cut:
                        _bucket.pop(0)
                    _bucket.append(_now)
                    if len(_bucket) > 300:
                        return _JSONResponse(
                            {"detail": "rate limited; backing off"}, status_code=429
                        )
                    if len(_AUTHD_HITS) > 1000:
                        for k in list(_AUTHD_HITS)[: len(_AUTHD_HITS) - 1000]:
                            _AUTHD_HITS.pop(k, None)
                except Exception:
                    pass
            try:
                return await call_next(request)
            except Exception:
                raise
    except Exception:
        pass

    def auth(
        request: Request,
        x_authenticated_user: str | None = Header(default=None),
        x_api_token: str | None = Header(default=None),
    ) -> str:
        if not cfg.api.enabled:
            raise HTTPException(403, "api disabled")
        client_host = request.client.host if request.client else ""
        trusted_proxies = set(
            getattr(cfg.api, "trusted_proxies", ["127.0.0.1", "::1", "localhost"])
        )
        if cfg.api.trust_nginx_header and x_authenticated_user and x_authenticated_user.strip():
            if not _host_is_trusted(client_host, trusted_proxies):
                log.warning("api auth rejected for untrusted proxy %r", client_host)
                raise HTTPException(403, "untrusted proxy for nginx auth header")
            return x_authenticated_user.strip()
        try:
            token_str = (
                cfg.api.api_token.get_secret_value()
                if hasattr(cfg.api.api_token, "get_secret_value")
                else str(cfg.api.api_token)
            )
        except Exception:
            token_str = ""
        # Compare stripped UTF-8 bytes: config validation strips for its
        # checks, so compare the same way (a trailing-space token
        # validates but could never authenticate), and non-ASCII tokens
        # must 401, not 500 out of compare_digest.
        try:
            want = (token_str or "").strip().encode("utf-8")
            got = (x_api_token or "").strip().encode("utf-8")
            if want and got and secrets.compare_digest(got, want):
                # A correct token returns BEFORE _note_auth_failure: the
                # throttle counts failures only, so valid operators can
                # never trip it (and a spray with the right token is
                # already authenticated — nothing to throttle).
                return "token"
        except Exception:
            pass
        _fail_ip = (client_host or "unknown").strip() or "unknown"
        try:
            _ep = request.url.path
            _method = request.method
        except Exception:
            _ep, _method = "?", "?"
        try:
            _ident = (x_authenticated_user or "").strip() or (
                "token" if (x_api_token or "").strip() else "-"
            )
        except Exception:
            _ident = "-"
        if _note_auth_failure(_fail_ip):
            log.warning(
                "api auth throttled for %s (%s %s user=%s; too many failures)",
                _fail_ip,
                _method,
                _ep,
                _ident,
            )
            raise HTTPException(429, "too many auth failures; backing off")
        log.warning("api auth failure from %s (%s %s user=%s)", _fail_ip, _method, _ep, _ident)
        raise HTTPException(401, "auth required")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        """Liveness probe (no auth): process is up and serving."""
        return {"status": "ok"}

    @app.get("/readyz", dependencies=[Depends(auth)])
    async def readyz() -> Any:
        """Readiness probe (authed): DB + clients + fuse + SSD + lanes.

        Every check is strict-typed (test doubles answer truthy to
        everything — a MagicMock must read as NOT ready, never ready).
        Only the poller and scheduler lanes gate readiness; janitor and
        qb_listener are reported but never turn a healthy daemon 503.
        Returns 200 when ready, 503 with the same JSON body otherwise.
        """
        checks: dict[str, bool] = {}
        try:
            await asyncio.to_thread(coord.store.all_active, False, 1)
            checks["state_db"] = True
        except Exception:
            checks["state_db"] = False
        for name in ("source_client", "dest_client"):
            try:
                checks[name] = getattr(coord, name, None) is not None
            except Exception:
                checks[name] = False
        # Fuse mount present (the seeding lane depends on it).
        try:
            _mount = getattr(getattr(getattr(cfg, "rclone", None), "fuse", None), "mount", None)
            _mpath = os.fspath(_mount) if _mount is not None else ""
            checks["fuse_mount"] = bool(_mpath) and os.path.isdir(_mpath)
        except Exception:
            checks["fuse_mount"] = False
        # SSD path writable with readable free space.
        try:
            _spath = getattr(getattr(cfg, "ssd", None), "path", None)
            _spath_s = os.fspath(_spath) if _spath is not None else ""
            _writable = bool(_spath_s) and os.access(_spath_s, os.W_OK)
            _free = await asyncio.to_thread(ssd_free_bytes, cfg)
            checks["ssd_writable"] = bool(_writable and int(_free) >= 0)
        except Exception:
            checks["ssd_writable"] = False
        # Per-lane freshness: production loops heartbeat independently;
        # `_last_tick_at` is only written by the test/manual single-pass
        # path, so it is a back-compat fallback, never the primary signal.
        now = time.monotonic()
        lanes = _lane_health(coord)
        try:
            started_at = _strict_float(getattr(coord, "_started_at", None))
        except Exception:
            started_at = None
        try:
            legacy_tick = _strict_float(getattr(coord, "_last_tick_at", None))
        except Exception:
            legacy_tick = None
        thresholds = {
            "poller": _lane_threshold(_lane_interval(cfg, "source_poll_interval")),
            "scheduler": _lane_threshold(_lane_interval(cfg, "dest_poll_interval")),
            "janitor": _lane_threshold(_janitor_interval(cfg)),
            "qb_listener": _lane_threshold(_QB_LISTENER_INTERVAL_S),
        }
        lane_body: dict[str, dict[str, Any]] = {}
        for name in _ALL_LANES:
            required = name in _REQUIRED_LANES
            ok = _lane_fresh(
                lanes[name],
                now=now,
                threshold=thresholds[name],
                started_at=started_at,
                legacy_tick=legacy_tick,
            )
            lane_body[name] = _lane_body(lanes[name], required=required, ok=ok, now=now)
        checks["poller_fresh"] = bool(lane_body["poller"]["ok"])
        checks["scheduler_fresh"] = bool(lane_body["scheduler"]["ok"])
        checks["tick_fresh"] = bool(checks["poller_fresh"] and checks["scheduler_fresh"])
        ready = all(checks.values())
        from fastapi.responses import JSONResponse

        return JSONResponse(
            {"ready": ready, "checks": checks, "lanes": lane_body},
            status_code=200 if ready else 503,
        )

    @app.get("/metrics", dependencies=[Depends(auth)])
    async def metrics() -> Any:
        """Minimal Prometheus metrics (authed): no Telegram spam for ops."""
        from fastapi.responses import PlainTextResponse

        # 10s TTL: scrapes every 15s must not each pay a full table scan
        # + SSD stat on an SSD-constrained host.
        try:
            _cache = getattr(metrics, "_cache", None)
            if (
                _cache is not None
                and isinstance(_cache, dict)
                and time.monotonic() - float(_cache.get("at", 0.0)) < 10.0
                and isinstance(_cache.get("body"), str)
            ):
                return PlainTextResponse(_cache["body"])
        except Exception:
            _cache = None
        lines: list[str] = []
        try:
            started = float(getattr(coord, "_started_at", 0.0) or 0.0)
        except (TypeError, ValueError):
            started = 0.0
        uptime = max(0.0, time.monotonic() - started) if started else 0.0
        lines.append(f"racing_sync_uptime_seconds {uptime:.0f}")
        # Authoritative per-state counts (no 5000-row truncation: DONE and
        # FAILED live outside all_active, so a sample scan undercounts
        # exactly the states operators watch). Legacy doubles without
        # count_by_state fall back to the bounded active sample.
        counts: dict[str, int] = {}
        try:
            _counter = getattr(coord.store, "count_by_state", None)
            if callable(_counter):
                _raw = await asyncio.to_thread(_counter)
                for _k, _v in dict(_raw or {}).items():
                    try:
                        counts[str(_k)] = int(_v)
                    except (TypeError, ValueError):
                        continue
        except Exception:
            counts = {}
        if not counts:
            try:
                rows = await asyncio.to_thread(coord.store.all_active, False, 5000)
            except Exception:
                rows = []
            for t in rows or []:
                try:
                    st = t.state.value
                except Exception:
                    st = "unknown"
                counts[st] = counts.get(st, 0) + 1
        for st in sorted(counts):
            lines.append(f'racing_sync_rows{{state="{st}"}} {counts[st]}')
        try:
            lines.append(f"racing_sync_waiting_disk_depth {int(counts.get('waiting_disk', 0))}")
        except Exception:
            pass
        try:
            free_bytes = await asyncio.to_thread(ssd_free_bytes, cfg)
            lines.append(f"racing_sync_ssd_free_bytes {int(free_bytes)}")
        except Exception:
            pass
        try:
            tasks = getattr(coord, "_tasks", set())
            lines.append(f"racing_sync_worker_tasks {len(tasks or [])}")
        except Exception:
            pass
        # Per-lane health: every production loop heartbeats on its own
        # clock, so a wedged lane shows an ever-growing age even though
        # `_last_tick_at` (test/manual path) is never written in run().
        _lanes = _lane_health(coord)
        _now = time.monotonic()
        for _name in _ALL_LANES:
            _ent = _lanes.get(_name) or {}
            _has_data = _ent.get("consecutive_failures", 0) > 0 or any(
                _ent.get(key) is not None
                for key in (
                    "last_attempt_at",
                    "last_success_at",
                    "last_failure_at",
                    "last_duration_s",
                )
            )
            if not _has_data:
                continue
            _succ = _ent.get("last_success_at")
            _att = _ent.get("last_attempt_at")
            _ldur = _ent.get("last_duration_s")
            if _succ is not None:
                lines.append(
                    f'racing_sync_lane_success_age_seconds{{lane="{_name}"}} '
                    f"{max(0.0, _now - _succ):.0f}"
                )
            if _att is not None:
                lines.append(
                    f'racing_sync_lane_attempt_age_seconds{{lane="{_name}"}} '
                    f"{max(0.0, _now - _att):.0f}"
                )
            if _ldur is not None:
                lines.append(
                    f'racing_sync_lane_latency_seconds{{lane="{_name}"}} {float(_ldur):.3f}'
                )
            lines.append(
                f'racing_sync_lane_consecutive_failures{{lane="{_name}"}} '
                f"{int(_ent.get('consecutive_failures', 0) or 0)}"
            )
        # Legacy tick aliases: prefer the single-pass fields when present;
        # otherwise derive from the required lanes so production exports
        # them too (age = oldest required-lane reference = worst staleness;
        # latency = slowest required-lane pass).
        try:
            _tick_dur = _strict_float(getattr(coord, "_last_tick_duration_s", None))
        except Exception:
            _tick_dur = None
        if _tick_dur is None:
            _durs = [
                _lanes[_n]["last_duration_s"]
                for _n in _REQUIRED_LANES
                if _lanes[_n].get("last_duration_s") is not None
            ]
            if _durs:
                _tick_dur = max(_durs)
        if _tick_dur is not None:
            lines.append(f"racing_sync_tick_latency_seconds {float(_tick_dur):.3f}")
        try:
            _tick_at = _strict_float(getattr(coord, "_last_tick_at", None))
        except Exception:
            _tick_at = None
        if _tick_at is None:
            _refs = []
            for _name in _REQUIRED_LANES:
                _ent = _lanes[_name]
                _succ = _ent.get("last_success_at")
                _att = _ent.get("last_attempt_at")
                if _succ is not None:
                    _ref = _att if (_att is not None and _att > _succ) else _succ
                elif _att is not None:
                    _ref = _att
                else:
                    _ref = None
                if _ref is not None:
                    _refs.append(_ref)
            if _refs:
                _tick_at = min(_refs)
        if _tick_at is not None:
            lines.append(
                f"racing_sync_tick_age_seconds {max(0.0, time.monotonic() - _tick_at):.0f}"
            )
        # Indexer + move counters (best-effort; absent when never used).
        try:
            from .prowlarr import prowlarr_counters as _pc

            _p = _pc()
            lines.append(f"racing_sync_prowlarr_searches_total {int(_p.get('searches', 0))}")
            lines.append(f"racing_sync_prowlarr_hits_total {int(_p.get('hits', 0))}")
            lines.append(f"racing_sync_prowlarr_errors_total {int(_p.get('errors', 0))}")
        except Exception:
            pass
        try:
            from .rclone_ops import rclone_counters as _rc

            _r = _rc()
            lines.append(f"racing_sync_rclone_moves_total {int(_r.get('moves', 0))}")
            lines.append(f"racing_sync_rclone_move_ok_total {int(_r.get('move_ok', 0))}")
            lines.append(f"racing_sync_rclone_move_fail_total {int(_r.get('move_fail', 0))}")
        except Exception:
            pass
        body = "\n".join(lines) + "\n"
        try:
            metrics._cache = {"at": time.monotonic(), "body": body}  # type: ignore[attr-defined]
        except Exception:
            pass
        return PlainTextResponse(body)

    @app.get("/api/state", dependencies=[Depends(auth)])
    async def state(
        limit: int = Query(default=500, ge=1, le=5000),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        # SQL-level pagination: never materialize the whole table for a
        # 50k-row request.
        rows = await asyncio.to_thread(coord.store.all, False, limit, offset)
        return [_ts_to_dict(t) for t in rows]

    @app.get("/api/active", dependencies=[Depends(auth)])
    async def active(
        limit: int = Query(default=500, ge=1, le=5000),
    ) -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(coord.store.all_active, False, limit)
        return [_ts_to_dict(t) for t in rows]

    @app.get("/api/logs", dependencies=[Depends(auth)])
    async def logs(
        limit: int = Query(default=200, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        rows = await asyncio.to_thread(lambda: list(coord.store.iter_logs(limit=limit)))
        return [dict(r) for r in rows]

    @app.get("/api/ssd", dependencies=[Depends(auth)])
    async def ssd() -> dict[str, Any]:
        try:
            free_bytes = await asyncio.to_thread(ssd_free_bytes, cfg)
        except HTTPException:
            raise
        except Exception as e:  # noqa: BLE001
            raise HTTPException(503, f"ssd path unavailable: {e}") from e
        return {"free_bytes": free_bytes, "path": str(cfg.ssd.path)}

    @app.post("/api/recover", dependencies=[Depends(auth)])
    async def recover() -> dict[str, Any]:
        async with _hold_ops_lock(coord):
            rpt = await reconcile(cfg, dest=coord.dest_client, store=coord.store)
        return {
            "summary": rpt.summary(),
            "kept": rpt.kept,
            "resumed": rpt.resumed,
            "re_added": rpt.re_added,
            "orphans": rpt.orphans,
            "unknowns": rpt.unknowns,
            "adopted": rpt.adopted,
        }

    @app.post("/api/scan-watch", dependencies=[Depends(auth)])
    async def scan_watch() -> dict[str, Any]:
        if coord.watch is None:
            return {"items": 0, "note": "watch_dir not configured"}
        async with _hold_ops_lock(coord):
            items = await coord.scan_watch()
        return {"items": len(items)}

    @app.post("/api/retry/{source_infohash}", dependencies=[Depends(auth)])
    async def retry(source_infohash: str) -> RetryResult:
        normalized = (source_infohash or "").strip().lower()
        if not _INFOHASH_RE.fullmatch(normalized):
            raise HTTPException(422, "must be 40-char hex infohash")

        def _do_retry() -> str:
            ts = coord.store.get(normalized)
            if ts is None:
                raise HTTPException(404, "unknown hash")
            if ts.state != State.FAILED:
                raise HTTPException(409, f"state is {ts.state.value}")
            ts.failed_retries = 0
            try:
                coord.store.transition(ts, State.QUEUED, error="")
            except AbandonedError as e:
                # Row moved (or was forgotten) concurrently.
                raise HTTPException(409, f"state changed concurrently: {e}") from e
            except ValueError as e:
                # Row moved concurrently (e.g. tick rescheduled it).
                raise HTTPException(409, f"state changed concurrently: {e}") from e
            return ts.state.value

        async with _hold_ops_lock(coord):
            new_state = await asyncio.to_thread(_do_retry)
        return RetryResult(source_infohash=normalized, new_state=new_state)

    @app.post("/api/forget/{source_infohash}")
    async def forget(
        source_infohash: str,
        delete_files: bool = Query(default=True),
        ignore: bool = Query(default=False),
        confirm: bool = Query(default=False, description="must be true: forget is destructive"),
        requester: str = Depends(auth),
    ) -> ForgetResult:
        normalized = (source_infohash or "").strip().lower()
        if not _INFOHASH_RE.fullmatch(normalized):
            raise HTTPException(422, "must be 40-char hex infohash")
        if not confirm:
            raise HTTPException(400, "forget is destructive; re-issue with ?confirm=true")
        try:
            log.warning(
                "api forget %s by %s (delete_files=%s, ignore=%s)",
                normalized[:10],
                requester,
                delete_files,
                ignore,
            )
        except Exception:
            pass
        async with _hold_ops_lock(coord):
            try:
                _api_stop = getattr(coord, "stop_workers_for", None)
                result = await forget_torrent(
                    cfg,
                    dest=coord.dest_client,
                    store=coord.store,
                    target=normalized,
                    apply=True,
                    delete_files=delete_files,
                    ignore=ignore,
                    stop_workers=_api_stop if callable(_api_stop) else None,
                )
            except LookupError as e:
                raise HTTPException(404, str(e)) from e
            # Free the SSD budget immediately (forget bypasses transition hooks).
            try:
                await coord._ssd_release(result.get("source_infohash") or normalized)
            except Exception:
                pass
            # Same for the quiet-wait / MOVING-park maps (transition pops
            # bypassed); the admission set reconciles itself via
            # _running_infohashes, and the prune reaps the rest.
            try:
                _gone = (result.get("source_infohash") or normalized).lower()
                _wd = getattr(coord, "_waiting_disk_next_check", None)
                if isinstance(_wd, dict):
                    _wd.pop(_gone, None)
                _mp = getattr(coord, "_moving_parks", None)
                if isinstance(_mp, dict):
                    _mp.pop(_gone, None)
            except Exception:
                pass
            # Paired releases stay inside the same ops lock: the lock is
            # what serializes budget maps against the tick, and a paired
            # release outside it races a concurrent admission.
            paired: list[dict[str, Any]] = []
            try:
                for pair in result.get("paired_cancelled") or []:
                    try:
                        _ph = (
                            ((pair.get("source_infohash") or "").strip().lower())
                            if isinstance(pair, dict)
                            else str(pair or "").strip().lower()
                        )
                    except Exception:
                        continue
                    if not _ph:
                        continue
                    try:
                        await coord._ssd_release(_ph)
                    except Exception:
                        pass
                    try:
                        if isinstance(pair, dict):
                            paired.append(
                                {
                                    "source_infohash": _ph,
                                    "source_name": str(pair.get("source_name") or "")[:200],
                                    "errors": [str(e)[:300] for e in (pair.get("errors") or [])],
                                }
                            )
                        else:
                            paired.append({"source_infohash": _ph, "source_name": "", "errors": []})
                    except Exception:
                        paired.append({"source_infohash": _ph, "source_name": "", "errors": []})
            except Exception:
                pass
        return ForgetResult(
            source_infohash=result["source_infohash"],
            source_name=result["source_name"],
            applied=result["applied"],
            dest_entries=result["dest_entries"],
            local_paths=result["local_paths"],
            skipped_paths=result["skipped_paths"],
            errors=result["errors"],
            ignored=bool(result.get("ignored", False)),
            paired_cancelled=paired,
        )

    return app


def _ts_to_dict(t) -> dict[str, Any]:
    try:
        from .logging_setup import sanitize_log_text

        safe_error = sanitize_log_text(str(t.last_error or ""))[:500]
    except Exception:
        safe_error = str(t.last_error or "")[:500]
    try:
        created = t.created_at.isoformat() if t.created_at is not None else ""
    except Exception:
        created = ""
    try:
        updated = t.updated_at.isoformat() if t.updated_at is not None else ""
    except Exception:
        updated = ""
    try:
        state_val = t.state.value
    except Exception:
        state_val = str(getattr(t, "state", "unknown"))
    try:
        name_val = str(t.source_name or "")[:200]
    except Exception:
        name_val = ""

    # Admission/transport flags operators filter on (strict numerics —
    # test doubles answer truthy to everything and must read 0).
    def _flag(name: str) -> int:
        try:
            v = getattr(t, name, 0)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                return 0
            return int(v)
        except Exception:
            return 0

    return {
        "source_infohash": t.source_infohash,
        "dest_infohash": t.dest_infohash,
        "source_name": name_val,
        "classification_kind": t.classification_kind,
        "state": state_val,
        "total_bytes": t.total_bytes,
        "batch_index": t.batch_index,
        "batches_total": t.batches_total,
        "last_error": safe_error,
        "created_at": created,
        "updated_at": updated,
        "skipped": _flag("skipped"),
        "force_direct": _flag("force_direct"),
        "fuse_verified": _flag("fuse_verified"),
        "version": _flag("version"),
    }


async def serve(coord: Coordinator) -> None:
    import uvicorn

    cfg = coord.cfg.api
    app = build_app(coord)
    kwargs: dict[str, Any] = {}
    try:
        cert = (getattr(cfg, "tls_certfile", "") or "").strip()
        key = (getattr(cfg, "tls_keyfile", "") or "").strip()
        if cert and key:
            kwargs["ssl_certfile"] = cert
            kwargs["ssl_keyfile"] = key
    except Exception:
        pass
    config = uvicorn.Config(
        app,
        host=cfg.host,
        port=cfg.port,
        log_level="info",
        **kwargs,
    )
    server = uvicorn.Server(config)
    # Stashed for graceful shutdown (should_exit instead of task-cancel).
    try:
        coord._api_server = server  # type: ignore[attr-defined]
    except Exception:
        pass
    await server.serve()
