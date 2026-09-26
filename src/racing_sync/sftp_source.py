"""SFTP / SSH exporter for downloading .torrent files from a remote client.

Used as a fallback when a torrent's .torrent file is not otherwise retrievable:
  - Deluge state dir: ~/.config/deluge/state/<infohash>.torrent
  - qBittorrent state dir: ~/.local/share/qBittorrent/BT_backup/<infohash>.torrent
"""

from __future__ import annotations

import io
import logging
import socket
import threading
import time
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import paramiko  # type: ignore[import-untyped]

from .config import DelugeSFTPConfig

log = logging.getLogger(__name__)

MAX_TORRENT_BYTES: int = 20 * 1024 * 1024  # 20 MiB safety cap, matching Prowlarr

# Max wait for the shared-connection lock before failing fast (seconds).
# Must stay comfortably below the coordinator's 15s asyncio.wait_for budget
# around SFTP calls so contention surfaces as a catchable timeout there.
_SFTP_LOCK_TIMEOUT: float = 10.0

# Pool lease budget (seconds): how long a call waits for ANY free pool
# member before failing fast. Also kept below the coordinator's 15s budget
# so the wait + a fetch attempt still fit inside it.
_POOL_LEASE_TIMEOUT: float = 8.0
_POOL_SIZE_DEFAULT: int = 3
_POOL_SIZE_MIN: int = 1
_POOL_SIZE_MAX: int = 8


def _coerce_pool_size(raw: object) -> int:
    """Clamp pool size to [_POOL_SIZE_MIN, _POOL_SIZE_MAX]; default on garbage."""
    if isinstance(raw, bool):
        return _POOL_SIZE_DEFAULT
    try:
        n = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _POOL_SIZE_DEFAULT
    return max(_POOL_SIZE_MIN, min(_POOL_SIZE_MAX, n))

_HEX40_RE = None  # lazy compiled in list_state_dir to avoid import cost

import re as _re
_HEX40_RE = _re.compile(r"[0-9a-fA-F]{40}")


class SFTPError(RuntimeError):
    pass


def _ipv4_socket(host: str, port: int, *, timeout: float = 15) -> socket.socket:
    """Resolve `host` to an IPv4 address and return a connected socket.

    paramiko's `SSHClient.connect(sock=...)` accepts a pre-opened socket,
    so we use this to force IPv4 and avoid hangs against IPv6-only
    records that aren't actually routable.
    """
    infos = socket.getaddrinfo(
        host, port, family=socket.AF_INET, type=socket.SOCK_STREAM,
    )
    if not infos:
        raise SFTPError(f"no IPv4 address for {host}")
    last_err: Exception | None = None
    for family, kind, proto, _canon, sockaddr in infos:
        s: socket.socket | None = None
        try:
            s = socket.socket(family, kind, proto)
            s.settimeout(timeout)
            s.connect(sockaddr)
            return s
        except OSError as e:
            if s is not None:
                try:
                    s.close()
                except Exception:
                    pass
            last_err = e
            continue
    raise SFTPError(f"could not connect to {host}:{port} (IPv4): {last_err}")


class _SFTPConnection:
    """One SSH/SFTP connection (single transport, lock-guarded).

    Internal unit of the pooled SFTPExporter below; API mirrors the old
    single-connection exporter so delegation is mechanical. All public
    methods are thread-safe via the re-entrant instance lock.
    """

    def __init__(self, cfg: DelugeSFTPConfig):
        self._cfg = cfg
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None
        self._lock = threading.RLock()
        # Set by close(): the member is dead — _lease revives it on demand
        # instead of queueing behind its (possibly wedged-holder) lock.
        self._closed = True

    def __enter__(self) -> _SFTPConnection:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _creds_present(self) -> bool:
        if self._cfg.ssh_key_path:
            return True
        pwd = (
            self._cfg.ssh_password.get_secret_value()
            if hasattr(self._cfg.ssh_password, "get_secret_value")
            else str(self._cfg.ssh_password)
        )
        return bool((pwd or "").strip())

    def connect(self) -> None:
        with self._lock:
            self.close()
            if not self._creds_present():
                raise SFTPError("no SSH credentials: set ssh_key_path or ssh_password")
            # close() marks _closed; a fresh dial revives the member.
            self._closed = False
            sock: socket.socket | None = None
            try:
                self._client = paramiko.SSHClient()
                if self._cfg.known_hosts_path:
                    self._client.load_host_keys(str(self._cfg.known_hosts_path))
                else:
                    try:
                        self._client.load_system_host_keys()
                    except Exception:
                        pass
                if self._cfg.auto_add_host_key:
                    log.warning(
                        "sftp: auto-adding host key for %s (MITM risk; pin via known_hosts_path)",
                        self._cfg.ssh_host,
                    )
                    self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                else:
                    self._client.set_missing_host_key_policy(paramiko.RejectPolicy())
                kwargs: dict = {
                    "hostname": self._cfg.ssh_host,
                    "port": self._cfg.ssh_port,
                    "username": self._cfg.ssh_user,
                    "timeout": 15,
                    "banner_timeout": 15,
                    "auth_timeout": 15,
                    # Never offer agent keys / ~/.ssh keys by surprise — only
                    # the explicitly configured credential.
                    "allow_agent": False,
                    "look_for_keys": False,
                }
                if self._cfg.ssh_key_path:
                    # Load the key manually when a passphrase is configured, so
                    # paramiko can decrypt the (possibly encrypted) private key.
                    # Without a passphrase we can let paramiko load it via
                    # `key_filename=` itself, but loading it explicitly here
                    # keeps both code paths uniform.
                    try:
                        passphrase = (
                            self._cfg.ssh_key_passphrase.get_secret_value()
                            if hasattr(self._cfg.ssh_key_passphrase, "get_secret_value")
                            else str(self._cfg.ssh_key_passphrase)
                        ) or None
                        pkey = self._load_private_key(
                            self._cfg.ssh_key_path,
                            passphrase=passphrase,
                        )
                    except paramiko.PasswordRequiredException as e:
                        raise SFTPError(
                            f"SSH key {self._cfg.ssh_key_path} is encrypted but "
                            f"ssh_key_passphrase is empty/missing"
                        ) from e
                    except paramiko.SSHException as e:
                        raise SFTPError(
                            f"could not load SSH key {self._cfg.ssh_key_path}: {e}"
                        ) from e
                    kwargs["pkey"] = pkey
                else:
                    kwargs["password"] = (
                        self._cfg.ssh_password.get_secret_value()
                        if hasattr(self._cfg.ssh_password, "get_secret_value")
                        else str(self._cfg.ssh_password)
                    )
                # Force IPv4 resolution: the racing VPS may not have a routable
                # IPv6 address and paramiko defaults to getaddrinfo's first
                # result, which can be a hung AAAA connection. We pre-resolve
                # with AF_INET, open a socket ourselves, and pass it to connect().
                sock = _ipv4_socket(
                    self._cfg.ssh_host, self._cfg.ssh_port, timeout=15
                )
                kwargs["sock"] = sock
                self._client.connect(**kwargs)
                self._sftp = self._client.open_sftp()
                log.info("sftp connected to %s:%d", self._cfg.ssh_host, self._cfg.ssh_port)
            except Exception:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                self.close()
                raise

    @staticmethod
    def _load_private_key(path: Path, *, passphrase: str | None) -> paramiko.PKey:
        """Try RSA, then ECDSA, then Ed25519 key loaders.

        Note: DSA was removed from paramiko 4.x and is no longer
        supported. SSH DSA keys have been deprecated by NIST since 2011
        and rejected by OpenSSH since 7.4; we don't try to load them.
        """
        errors: list[str] = []
        for loader in (
            paramiko.RSAKey.from_private_key_file,
            paramiko.ECDSAKey.from_private_key_file,
            paramiko.Ed25519Key.from_private_key_file,
        ):
            try:
                return loader(str(path), password=passphrase)
            except paramiko.PasswordRequiredException:
                raise  # bubble up — caller decides how to handle it
            except Exception as e:  # noqa: BLE001
                errors.append(f"{loader.__name__}: {e}")
        raise paramiko.SSHException(
            "could not parse private key as RSA/ECDSA/Ed25519: "
            + " | ".join(errors)
        )

    def close(self) -> None:
        # Best-effort: never block shutdown on a wedged holder, and never
        # leak the transport. Mark dead first so new leases skip this member;
        # then close even if the lock stays held — the in-flight holder will
        # error out of its op (callers already retry on exactly that).
        # Re-entrant same-thread acquisition succeeds immediately, so nested
        # callers (connect() under fetch_torrent's guard) are unaffected.
        self._closed = True
        try:
            acquired = self._lock.acquire(timeout=5)
        except Exception:
            acquired = False
        try:
            if self._sftp is not None:
                try:
                    self._sftp.close()
                except Exception:  # noqa: BLE001
                    pass
                self._sftp = None
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:  # noqa: BLE001
                    pass
                self._client = None
        finally:
            if acquired:
                try:
                    self._lock.release()
                except Exception:
                    pass

    @staticmethod
    def _acquire_or_busy(lock: threading.RLock, what: str) -> bool:
        """Fail-fast lock acquisition for SFTP entry points.

        Callers invoke fetches via asyncio.to_thread with their own timeout
        and abandon the thread on expiry — but an abandoned thread stuck in
        a wedged transport keeps holding this lock, so unbounded waiters
        pile up behind it (and behind the bounded to_thread pool). Waiting
        briefly then failing lets every caller hit its existing retry path
        instead of wedging the whole runtime.
        """
        try:
            if lock.acquire(timeout=_SFTP_LOCK_TIMEOUT):
                return True
        except Exception:
            pass
        raise TimeoutError(f"sftp busy: {what} gave up waiting for the connection lock")

    # ---- torrent file ----

    def fetch_torrent(self, infohash: str) -> bytes | None:
        """Return the .torrent bytes for `infohash` or None if missing."""
        if not infohash or len(infohash) != 40 or not all(c in "0123456789abcdefABCDEF" for c in infohash):
            return None
        # State dirs live on case-sensitive filesystems with lowercase names.
        infohash = infohash.lower()

        if not self._acquire_or_busy(self._lock, f"fetch {infohash[:10]}"):
            return None  # unreachable: helper raises; kept for clarity
        try:
            return self._fetch_torrent_locked(infohash)
        finally:
            try:
                self._lock.release()
            except Exception:
                pass

    def _fetch_torrent_locked(self, infohash: str) -> bytes | None:
        with self._lock:
            # Reconnect if connection dropped
            if (self._client is None
                    or self._sftp is None
                    or self._client.get_transport() is None
                    or not self._client.get_transport().is_active()):
                log.info("sftp connection dropped or not active; reconnecting...")
                try:
                    self.connect()
                except Exception as e:
                    log.warning("sftp reconnect failed: %s", e)
                    return None

            state_posix = PurePosixPath(
                self._cfg.state_dir.as_posix()
                if hasattr(self._cfg.state_dir, "as_posix")
                else str(self._cfg.state_dir).replace("\\", "/")
            )
            candidates = [
                state_posix / f"{infohash}.torrent",
                state_posix.parent / "BT_backup" / f"{infohash}.torrent",
            ]
            for path in candidates:
                path_str = path.as_posix()
                try:
                    with self._sftp.open(path_str, "rb") as f:  # type: ignore[union-attr]
                        data = f.read(MAX_TORRENT_BYTES + 1)
                    if len(data) > MAX_TORRENT_BYTES:
                        log.warning(
                            "sftp: %s exceeds %d bytes limit", path_str, MAX_TORRENT_BYTES
                        )
                        continue
                    if data.startswith(b"d"):
                        return data
                    log.warning("sftp: %s does not look like a bencoded torrent", path_str)
                except FileNotFoundError:
                    continue
                except OSError as e:
                    # Paramiko surfaces missing files as IOError(errno 2),
                    # which may not subclass FileNotFoundError — treat as miss.
                    if getattr(e, "errno", None) == 2:
                        continue
                    log.warning("sftp: read %s failed: %s", path_str, e)
                    continue
                except (paramiko.SSHException, EOFError) as e:
                    log.warning("sftp: read %s failed: %s", path_str, e)
                    continue
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp: unexpected error reading %s: %s", path_str, e)
                    continue
            return None

    def fetch_many(self, infohashes: Iterable[str]) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for h in infohashes:
            try:
                data = self.fetch_torrent(h)
            except Exception:
                continue
            if data:
                out[h] = data
        return out

    def disk_free_bytes(self, path: str) -> int | None:
        """Free bytes on the remote filesystem containing `path`.

        Used by the VPS1 cleanup janitor to scale grace with real disk
        pressure. Prefers SFTP statvfs, falling back to `df -kP` over the
        SSH channel (some paramiko versions lack SFTPClient.statvfs).
        Returns None when unknown (disconnected, unsupported, any error) —
        callers degrade to time-only grace, never to zero.
        """
        if not self._acquire_or_busy(self._lock, f"disk-free {path}"):
            return None  # unreachable: helper raises; kept for clarity
        try:
            with self._lock:
                if (self._client is None
                        or self._sftp is None
                        or self._client.get_transport() is None
                        or not self._client.get_transport().is_active()):
                    try:
                        self.connect()
                    except Exception as e:
                        log.warning("sftp disk-free reconnect failed: %s", e)
                        return None
                statvfs = getattr(self._sftp, "statvfs", None)
                if callable(statvfs):
                    try:
                        st = statvfs(path)
                    except Exception as e:
                        log.warning("sftp statvfs %s failed: %s", path, e)
                        return self._disk_free_via_df(path)
                    return self._free_from_statvfs(st)
                return self._disk_free_via_df(path)
        finally:
            try:
                self._lock.release()
            except Exception:
                pass

    @staticmethod
    def _free_from_statvfs(st: object) -> int | None:
        try:
            frsize = int(getattr(st, "f_frsize", 0) or 0) or int(getattr(st, "f_bsize", 0) or 0)
            avail = int(getattr(st, "f_bavail", 0) or 0)
            if frsize <= 0 or avail < 0:
                return None
            return avail * frsize
        except (TypeError, ValueError):
            return None

    def _disk_free_via_df(self, path: str) -> int | None:
        """Parse `df -kP` (POSIX, 1K blocks) for free bytes. Caller holds the lock."""
        import shlex

        client = self._client
        if client is None:
            return None
        try:
            _, stdout, _ = client.exec_command(f"df -kP {shlex.quote(path)}")
            # Bound the read at the channel: a wedged transport must not
            # hold the member lock forever. (A per-call helper thread was
            # the old mechanism — it leaked one thread per wedged df.)
            try:
                stdout.channel.settimeout(10.0)
            except Exception:
                pass
            out = stdout.read()
            if out is None:
                return None
            out = out.decode("utf-8", errors="replace")
        except Exception as e:
            log.warning("ssh df %s failed: %s", path, e)
            return None
        try:
            lines = [ln.split() for ln in out.splitlines() if ln.split()]
            # Header + one data line; data line has >=6 fields with Available 4th.
            data = lines[1] if len(lines) > 1 else []
            if len(data) < 6:
                log.warning("ssh df %s returned unparsable output: %r", path, out[:200])
                return None
            return int(data[3]) * 1024
        except (TypeError, ValueError, IndexError) as e:
            log.warning("ssh df %s returned unparsable output: %s", path, e)
            return None

    def list_state_dir(self) -> list[str]:
        if not self._acquire_or_busy(self._lock, "list state dir"):
            raise SFTPError("sftp busy")  # unreachable: helper raises
        try:
            with self._lock:
                return self._list_state_dir_locked()
        finally:
            try:
                self._lock.release()
            except Exception:
                pass

    def _list_state_dir_locked(self) -> list[str]:
        with self._lock:
            # Mirror fetch_torrent: reconnect if the connection dropped.
            if (self._client is None
                    or self._sftp is None
                    or self._client.get_transport() is None
                    or not self._client.get_transport().is_active()):
                log.info("sftp connection dropped or not active; reconnecting...")
                try:
                    self.connect()
                except Exception as e:
                    raise SFTPError(f"sftp reconnect failed: {e}") from e
            if self._sftp is None:
                raise SFTPError("not connected")
            out: list[str] = []
            remote_dir = PurePosixPath(
                self._cfg.state_dir.as_posix()
                if hasattr(self._cfg.state_dir, "as_posix")
                else str(self._cfg.state_dir).replace("\\", "/")
            ).as_posix()
            try:
                entries = self._sftp.listdir_attr(remote_dir)
            except OSError as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            except (paramiko.SSHException, EOFError) as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            # Bound the backlog: a 100k-file state dir must not OOM the
            # daemon — stop collecting past the cap (coordinator treats
            # the rest as "not yet seen", retried next poll).
            _LIST_CAP = 20000
            for entry in entries:
                if len(out) >= _LIST_CAP:
                    log.warning("sftp state dir exceeds %d entries; truncating list",
                                _LIST_CAP)
                    break
                name = entry.filename
                if name.endswith(".torrent"):
                    digest = name[: -len(".torrent")]
                    if _HEX40_RE is not None and _HEX40_RE.fullmatch(digest):
                        out.append(digest.lower())
            return out


class SFTPExporter:
    """Pooled SFTP exporter: N independent SSH/SFTP connections.

    Re-inject bursts from concurrent coordinator workers used to serialize
    on a single paramiko transport behind one lock (the 15s-timeout
    clusters in the log). The pool leases a free member per call with
    round-robin start and fails over when one wedges; only when every
    member is busy does the caller get a fast TimeoutError for its
    existing retry path.

    Public API is unchanged from the old single-connection exporter
    (connect/close/fetch_torrent/fetch_many/disk_free_bytes/list_state_dir
    plus the context manager), so all existing call sites keep working.
    `pool_size` defaults to `[source.deluge_sftp].pool_size` (3).
    """

    def __init__(self, cfg: DelugeSFTPConfig, pool_size: int | None = None):
        self._cfg = cfg
        if pool_size is None:
            pool_size = _coerce_pool_size(getattr(cfg, "pool_size", _POOL_SIZE_DEFAULT))
        else:
            pool_size = _coerce_pool_size(pool_size)
        self._pool_size = pool_size
        self._members: list[_SFTPConnection] = []
        self._pool_lock = threading.Lock()
        self._shutdown = False
        self._rr = 0

    def __enter__(self) -> SFTPExporter:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def pool_size(self) -> int:
        return self._pool_size

    # ---- pool plumbing ----

    def _members_snapshot(self) -> list[_SFTPConnection]:
        with self._pool_lock:
            if not self._members:
                self._members = [_SFTPConnection(self._cfg) for _ in range(self._pool_size)]
            return list(self._members)

    def _try_revive(self, m: _SFTPConnection) -> bool:
        """Best-effort re-dial of a closed member. True when usable."""
        try:
            if getattr(self, "_shutdown", False):
                return False
            if not getattr(m, "_closed", False):
                return True
            m.connect()
            return not getattr(m, "_closed", False)
        except Exception:
            return False

    def _lease(self, what: str) -> _SFTPConnection:
        """Return a member with its lock held; caller must _release() it.

        Rotating start spreads concurrent callers across members. Each pass
        sweeps non-blocking first (a wedged member never stalls failover to
        a free one), then blocks in short slices so release wakes promptly
        without sleep-spinning. Closed members are re-dialed on demand so a
        transient outage heals without a restart, and a fully-wedged pool
        still fails fast at the deadline.
        """
        members = self._members_snapshot()
        with self._pool_lock:
            start = self._rr % len(members)
            self._rr += 1
        # Revive closed members before leasing: without this a burst of
        # transient failures closes every member and the pool stays dead
        # until restart (only connect() revived, called at startup).
        # Never revive after close(): shutdown must stay shut (no
        # use-after-close re-dial from a late worker).
        if getattr(self, "_shutdown", False):
            raise RuntimeError("sftp pool is closed")
        for i in range(len(members)):
            m = members[(start + i) % len(members)]
            try:
                if getattr(m, "_closed", False):
                    self._try_revive(m)
            except Exception:
                continue
        deadline = time.monotonic() + _POOL_LEASE_TIMEOUT
        while True:
            for i in range(len(members)):
                m = members[(start + i) % len(members)]
                try:
                    if getattr(m, "_closed", False):
                        continue
                    if m._lock.acquire(blocking=False):
                        if getattr(m, "_closed", False):
                            try:
                                m._lock.release()
                            except Exception:
                                pass
                            continue
                        return m
                except Exception:
                    continue
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"sftp busy: {what} gave up waiting for a free connection"
                )
            for i in range(len(members)):
                m = members[(start + i) % len(members)]
                try:
                    if getattr(m, "_closed", False):
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    if m._lock.acquire(timeout=min(0.05, remaining)):
                        if getattr(m, "_closed", False):
                            try:
                                m._lock.release()
                            except Exception:
                                pass
                            continue
                        return m
                except Exception:
                    continue

    def _release(self, m: _SFTPConnection) -> None:
        try:
            m._lock.release()
        except Exception:
            pass

    # ---- lifecycle ----

    def connect(self) -> None:
        # Members are independent transports: dial in parallel instead of
        # serially (~3x45s worst case before). First failure closes EVERY
        # member and raises: pending dials that succeed after the raise
        # would otherwise leak (only already-completed ones were closed).
        import concurrent.futures as _fut

        members = self._members_snapshot()
        try:
            with _fut.ThreadPoolExecutor(
                max_workers=max(1, len(members)), thread_name_prefix="sftp-dial",
            ) as pool:
                futs = {pool.submit(m.connect): m for m in members}
                for fut in _fut.as_completed(futs):
                    exc = fut.exception()
                    if exc is not None:
                        raise exc
        except Exception:
            for m in members:
                try:
                    m.close()
                except Exception:
                    pass
            raise

    def close(self) -> None:
        try:
            with self._pool_lock:
                self._shutdown = True
                members = list(self._members)
        except Exception:
            members = list(getattr(self, "_members", None) or [])
        for m in members:
            try:
                m.close()
            except Exception:  # noqa: BLE001
                pass

    # ---- operations (lease a member, delegate, release) ----

    def fetch_torrent(self, infohash: str) -> bytes | None:
        """Return the .torrent bytes for `infohash` or None if missing."""
        if not infohash or len(infohash) != 40 or not all(c in "0123456789abcdefABCDEF" for c in infohash):
            return None
        infohash = infohash.lower()
        m = self._lease(f"fetch {infohash[:10]}")
        try:
            return m.fetch_torrent(infohash)
        finally:
            self._release(m)

    def fetch_many(self, infohashes: Iterable[str]) -> dict[str, bytes]:
        out: dict[str, bytes] = {}
        for h in infohashes:
            try:
                data = self.fetch_torrent(h)
            except Exception:
                continue
            if data:
                out[h] = data
        return out

    def disk_free_bytes(self, path: str) -> int | None:
        """Free bytes on the remote filesystem containing `path` (see member)."""
        m = self._lease(f"disk-free {path}")
        try:
            return m.disk_free_bytes(path)
        finally:
            self._release(m)

    def list_state_dir(self) -> list[str]:
        m = self._lease("list state dir")
        try:
            return m.list_state_dir()
        finally:
            self._release(m)
