"""SFTP / SSH exporter for downloading .torrent files from a remote client.

Used as a fallback when a torrent's .torrent file is not otherwise retrievable:
  - Deluge state dir: ~/.config/deluge/state/<infohash>.torrent
  - qBittorrent state dir: ~/.local/share/qBittorrent/BT_backup/<infohash>.torrent
"""

from __future__ import annotations

import logging
import re as _re
import socket
import tarfile
import threading
import time
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import paramiko  # type: ignore[import-untyped]

from .config import DelugeSFTPConfig

log = logging.getLogger(__name__)


def _torrent_identity(data: bytes) -> str | None:
    """Infohash of a bencoded torrent, or None when it is not one."""
    try:
        from .watchdir import _bencoded_info_hash, is_bencoded_torrent

        if not is_bencoded_torrent(data):
            return None
        v1, _name, _size, _announce = _bencoded_info_hash(data)
    except Exception:
        return None
    return (v1 or "").lower() or None


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

# Bound for every SFTP file read on a connected channel (seconds): without
# it a wedged transport stalls fetch_torrent forever, and the caller's
# wait_for abandon leaves the thread holding the member lock.
_SFTP_IO_TIMEOUT_S: float = 15.0

# Bulk fetch bounds: one `tar` stream replaces N per-file reads, so the
# transfer is bigger and slower than a single file — give it room, but
# never unbounded (a runaway archive must not eat the disk or the loop).
_BULK_IO_TIMEOUT_S: float = 120.0
_BULK_MAX_FILES: int = 5000
_BULK_MAX_TOTAL_BYTES: int = 128 * 1024 * 1024  # 128 MiB

# Short remote probes (the VPS1 push preflight: `command -v`, `listremotes`).
# Bounded like stat/df: a wedged channel must return "unknown", never hold a
# pool member. Kept above the transport's own 15s dial timeouts.
_SHELL_CMD_TIMEOUT_S: float = 30.0

# Budget for the detached push launch's dedicated dial: only the socket,
# session-channel open and the exec REQUEST are waited on (never the remote
# job itself), so this stays short.
_DETACH_CHANNEL_TIMEOUT_S: float = 30.0

# Cap for `write_remote_file`: the push file list is a few KB of names; a
# payload this size means a caller passed content by mistake (and content
# never travels over the push transport).
_MAX_REMOTE_WRITE_BYTES: int = 4 * 1024 * 1024  # 4 MiB

# Infohash filenames in the deluge/qBittorrent state dirs.
_HEX40_RE = _re.compile(r"[0-9a-fA-F]{40}")


def _coerce_pool_size(raw: object) -> int:
    """Clamp pool size to [_POOL_SIZE_MIN, _POOL_SIZE_MAX]; default on garbage."""
    if isinstance(raw, bool):
        return _POOL_SIZE_DEFAULT
    try:
        n = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _POOL_SIZE_DEFAULT
    return max(_POOL_SIZE_MIN, min(_POOL_SIZE_MAX, n))


class SFTPError(RuntimeError):
    pass


def _close_exec_streams(*streams) -> None:
    """Best-effort close of exec_command streams plus their channel.

    Leaked channels pin server-side sessions until transport teardown —
    the janitor's periodic df/stat probes used to exhaust them
    ("no more sessions"). Never raises.
    """
    seen_channels: list = []
    for s in streams:
        try:
            if s is None:
                continue
            try:
                ch = getattr(s, "channel", None)
            except Exception:
                ch = None
            if ch is not None and not any(ch is c for c in seen_channels):
                seen_channels.append(ch)
            close = getattr(s, "close", None)
            if callable(close):
                close()
        except Exception:
            continue
    for ch in seen_channels:
        try:
            close = getattr(ch, "close", None)
            if callable(close):
                close()
        except Exception:
            continue


def _ipv4_socket(host: str, port: int, *, timeout: float = 15) -> socket.socket:
    """Resolve `host` to an IPv4 address and return a connected socket.

    paramiko's `SSHClient.connect(sock=...)` accepts a pre-opened socket,
    so we use this to force IPv4 and avoid hangs against IPv6-only
    records that aren't actually routable.
    """
    infos = socket.getaddrinfo(
        host,
        port,
        family=socket.AF_INET,
        type=socket.SOCK_STREAM,
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


def _secret_value(raw: object) -> str:
    """Plain text of a SecretStr-or-string config value (never raises)."""
    try:
        return str(raw.get_secret_value() if hasattr(raw, "get_secret_value") else raw)
    except Exception:
        return ""


def _creds_present(cfg: DelugeSFTPConfig) -> bool:
    """True when one explicit SSH credential (key path or password) exists."""
    if getattr(cfg, "ssh_key_path", None):
        return True
    return bool(_secret_value(getattr(cfg, "ssh_password", "")).strip())


def _load_private_key(path: object, *, passphrase: str | None) -> paramiko.PKey:
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
        "could not parse private key as RSA/ECDSA/Ed25519: " + " | ".join(errors)
    )


def _apply_host_key_policy(client: paramiko.SSHClient, cfg: DelugeSFTPConfig) -> None:
    """Load trusted host keys and set the missing-host-key policy.

    Single source of truth for host-key handling: pooled members and the
    dedicated detached-launch dial go through here, so the second one can
    never drift into a weaker policy than the first.
    """
    if getattr(cfg, "known_hosts_path", None):
        client.load_host_keys(str(cfg.known_hosts_path))
    else:
        try:
            client.load_system_host_keys()
        except Exception:
            pass
    if getattr(cfg, "auto_add_host_key", False):
        log.warning(
            "sftp: auto-adding host key for %s (MITM risk; pin via known_hosts_path)",
            cfg.ssh_host,
        )
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.RejectPolicy())


def _ssh_connect_kwargs(cfg: DelugeSFTPConfig) -> dict:
    """Auth kwargs for `SSHClient.connect()` — the configured credential only.

    Raises SFTPError (never paramiko internals) for key problems so every
    dial path reports the same operator-readable failure.
    """
    kwargs: dict = {
        "hostname": cfg.ssh_host,
        "port": cfg.ssh_port,
        "username": cfg.ssh_user,
        "timeout": 15,
        "banner_timeout": 15,
        "auth_timeout": 15,
        # Never offer agent keys / ~/.ssh keys by surprise — only the
        # explicitly configured credential.
        "allow_agent": False,
        "look_for_keys": False,
    }
    if getattr(cfg, "ssh_key_path", None):
        # Load the key manually when a passphrase is configured, so paramiko
        # can decrypt the (possibly encrypted) private key. Without one we
        # could let paramiko load it via `key_filename=`, but loading it
        # explicitly here keeps both code paths uniform.
        try:
            pkey = _load_private_key(
                cfg.ssh_key_path,
                passphrase=_secret_value(getattr(cfg, "ssh_key_passphrase", "")) or None,
            )
        except paramiko.PasswordRequiredException as e:
            raise SFTPError(
                f"SSH key {cfg.ssh_key_path} is encrypted but ssh_key_passphrase is empty/missing"
            ) from e
        except paramiko.SSHException as e:
            raise SFTPError(f"could not load SSH key {cfg.ssh_key_path}: {e}") from e
        kwargs["pkey"] = pkey
    else:
        kwargs["password"] = _secret_value(getattr(cfg, "ssh_password", ""))
    return kwargs


def _dial_client(cfg: DelugeSFTPConfig) -> paramiko.SSHClient:
    """Build, authenticate and return a LIVE SSHClient on its own transport.

    Shared by pooled members and by the detached push launch, so an
    authenticated dial exists exactly once. Raises on failure (SFTPError,
    paramiko or OSError) and never leaks the half-open socket/transport.
    """
    if not _creds_present(cfg):
        raise SFTPError("no SSH credentials: set ssh_key_path or ssh_password")
    client: paramiko.SSHClient | None = None
    sock: socket.socket | None = None
    try:
        client = paramiko.SSHClient()
        _apply_host_key_policy(client, cfg)
        # Force IPv4 resolution: the racing VPS may not have a routable IPv6
        # address and paramiko defaults to getaddrinfo's first result, which
        # can be a hung AAAA connection. We pre-resolve with AF_INET, open a
        # socket ourselves, and pass it to connect().
        sock = _ipv4_socket(cfg.ssh_host, cfg.ssh_port, timeout=15)
        client.connect(sock=sock, **_ssh_connect_kwargs(cfg))
        return client
    except BaseException:
        # BaseException on purpose: CancelledError/KeyboardInterrupt must
        # also close the half-open socket/transport instead of leaking it
        # while the caller's task unwinds.
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        raise


def _launch_detached(cfg: DelugeSFTPConfig, command: str) -> bool:
    """Start `command` on a DEDICATED transport and leave immediately.

    Why a dedicated dial instead of a pool lease: a push is a multi-hour
    `rclone copy`. Leasing it would occupy one of the few pool members for
    its whole life, and closing that member (pool `close()`, or
    `_reconnect_for_retry()` rebuilding the transport) tears down its
    channels and orphans the transfer. A throwaway transport dies right
    after the launch — which is safe, because `command` is built as
    `setsid … </dev/null … &`: setsid puts the job in its own session with
    no controlling terminal, </dev/null detaches stdin and the trailing &
    lets the login shell exit, so the session teardown cannot SIGHUP it.

    `Channel.exec_command()` blocks only until the server ANSWERS the exec
    request (sshd has forked the shell), never until the job finishes, so a
    return without error is the confirmation we need. Nothing here reads the
    exit status. Best-effort: any failure is False, never an exception.
    """
    if not command or not command.strip():
        log.warning("sftp: refusing to launch an empty detached command")
        return False
    client: paramiko.SSHClient | None = None
    channel = None
    try:
        client = _dial_client(cfg)
        try:
            transport = client.get_transport()
        except Exception:
            transport = None
        if transport is None or not transport.is_active():
            log.warning("sftp: detached launch found no live transport")
            return False
        try:
            channel = transport.open_session(timeout=_DETACH_CHANNEL_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: detached launch could not open a session: %r", e)
            return False
        if channel is None:
            log.warning("sftp: detached launch got no session channel")
            return False
        # Confirm the channel really opened BEFORE handing it the command:
        # an unopened/refused channel has no id and is already closed.
        try:
            if int(channel.get_id()) < 0 or bool(channel.is_closed()):
                log.warning("sftp: detached launch session channel is not usable")
                return False
        except Exception:
            pass
        try:
            channel.settimeout(_DETACH_CHANNEL_TIMEOUT_S)
        except Exception:
            pass
        try:
            channel.exec_command(command)
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: detached launch exec request failed: %r", e)
            return False
        log.info("sftp: detached remote command started (%d chars)", len(command))
        return True
    except Exception as e:  # noqa: BLE001
        # socket.timeout stringifies to "" — log the repr for diagnosis.
        log.warning("sftp: detached launch failed: %r", e)
        return False
    finally:
        # Deliberate: drop the channel and the transport without waiting for
        # the remote job (see the docstring — setsid already detached it).
        if channel is not None:
            try:
                channel.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def _remote_write_refusal(path: object, data: object) -> str:
    """Reason this (path, payload) pair must never be written remotely; "" = safe.

    Shared by the pooled member and the dedicated writer below, so the two
    paths cannot drift into different safety rules — both refuse relative,
    flag-looking, metacharacter-bearing paths, empty files and content-sized
    payloads.
    """
    if not isinstance(path, str) or not path.strip():
        return "refusing to write a remote file with an empty path"
    clean = path.strip()
    if clean != path or "\n" in clean or "\r" in clean or "\0" in clean:
        return "refusing to write a remote file to an unsafe path"
    if not clean.startswith("/"):
        # Absolute POSIX only: a relative path resolves against the SSH
        # user's home, and a `-`-leading one reads as a flag.
        return f"refusing to write remote file {clean} (want an absolute path)"
    if any(c in clean for c in (";", "`", "$")):
        return f"refusing to write remote file {clean} (shell metacharacters)"
    payload = bytes(data or b"")
    if not payload:
        return f"refusing to write an empty remote file {clean}"
    if len(payload) > _MAX_REMOTE_WRITE_BYTES:
        return (
            f"refusing to write {len(payload)} bytes to {clean} "
            f"(cap {_MAX_REMOTE_WRITE_BYTES}: content never goes over SSH)"
        )
    return ""


def _bound_shell_timeout(timeout: object) -> float:
    """A sane positive bound for one remote shell call (never raises)."""
    try:
        return max(1.0, float(timeout))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return _SHELL_CMD_TIMEOUT_S


def _run_shell_dedicated(
    cfg: DelugeSFTPConfig, command: str, timeout: float = _SHELL_CMD_TIMEOUT_S
) -> tuple[int | None, str, str]:
    """Run one short command on a DEDICATED transport; never a pool lease.

    Why this exists next to `_launch_detached`: the push lane's *short* calls
    (preflight's `command -v` / `listremotes`, the log liveness read) are not
    the launch, and until now they leased a pooled member — so they queued
    behind, and competed with, the racing client's .torrent fetches. On a
    saturated VPS1 uplink those leases died with `Timeout opening channel`,
    which read as "push unavailable". A dial of its own cannot be starved by
    the pool and can never starve it.

    Same contract as the pooled `run_shell`: (exit status, stdout, stderr),
    never raises, streams always closed (a leaked channel pins a server-side
    session). Reuses `_dial_client`, so host-key policy and credentials keep
    exactly one implementation. Best-effort by design: an unreachable host
    comes back as (None, "", reason) and the caller turns it into a refusal.
    """
    if not command or not command.strip():
        return (None, "", "empty command")
    bound = _bound_shell_timeout(timeout)
    client: paramiko.SSHClient | None = None
    try:
        client = _dial_client(cfg)
    except Exception as e:  # noqa: BLE001
        # socket.timeout stringifies to "" — repr keeps the diagnosis usable.
        return (None, "", f"dedicated dial failed: {type(e).__name__}: {e!r}")
    stdin = stdout = stderr = None
    try:
        transport = client.get_transport()
        if transport is None or not transport.is_active():
            return (None, "", "dedicated dial has no live transport")
        stdin, stdout, stderr = client.exec_command(command, timeout=bound)
        try:
            stdout.channel.settimeout(bound)
        except Exception:
            pass
        # Both streams before the status: a full stdout buffer deadlocks
        # against a status wait (same ordering as the pooled member).
        try:
            out = (stdout.read() or b"").decode("utf-8", errors="replace")
        except Exception as e:  # noqa: BLE001
            return (None, "", f"stdout read failed: {type(e).__name__}: {e}")
        try:
            err = (stderr.read() or b"").decode("utf-8", errors="replace")
        except Exception:
            err = ""
        try:
            status = stdout.channel.recv_exit_status()
        except Exception:
            status = None
        return (status, out, err)
    except Exception as e:  # noqa: BLE001
        return (None, "", f"command failed to run: {type(e).__name__}: {e}")
    finally:
        _close_exec_streams(stdin, stdout, stderr)
        try:
            client.close()
        except Exception:
            pass


def _write_remote_file_dedicated(cfg: DelugeSFTPConfig, path: str, data: bytes) -> bool:
    """Write a SMALL remote file over a DEDICATED transport (see above).

    The push file list is the only payload this may carry; the shared
    `_remote_write_refusal` gate enforces that. True only when the bytes
    landed, and the tightening chmod is best-effort exactly like the pooled
    writer's. Never touches (or revives) a pool member, so it stays available
    while the pool is closed.
    """
    refusal = _remote_write_refusal(path, data)
    clean = str(path or "").strip()
    if refusal:
        log.warning("sftp: %s", refusal)
        return False
    payload = bytes(data or b"")
    client: paramiko.SSHClient | None = None
    sftp: paramiko.SFTPClient | None = None
    try:
        client = _dial_client(cfg)
        sftp = client.open_sftp()
        try:
            channel = sftp.get_channel()
            if channel is not None:
                channel.settimeout(_SFTP_IO_TIMEOUT_S)
        except Exception:
            pass
        with sftp.open(clean, "wb") as fh:
            fh.write(payload)
        # 0600: the list is a path inventory of the racing box, so it stays
        # unreadable to other accounts on a shared host.
        try:
            sftp.chmod(clean, 0o600)
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: could not chmod %s to 0600: %r", clean, e)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("sftp: dedicated write of %s failed: %r", clean, e)
        return False
    finally:
        for closer in (sftp, client):
            if closer is not None:
                try:
                    closer.close()
                except Exception:
                    pass


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
        return _creds_present(self._cfg)

    def connect(self) -> None:
        with self._lock:
            self.close()
            if not self._creds_present():
                raise SFTPError("no SSH credentials: set ssh_key_path or ssh_password")
            # close() marks _closed; a fresh dial revives the member.
            self._closed = False
            try:
                # Same dial the detached push launch uses, so host-key policy
                # and credential handling exist in exactly one place.
                client = _dial_client(self._cfg)
            except BaseException:
                # BaseException on purpose: CancelledError/KeyboardInterrupt
                # must also close the half-open transport instead of leaking
                # it while the task unwinds.
                self.close()
                raise
            if self._closed:
                # close() landed while this dial was in flight (pool shutdown
                # takes the lock non-blockingly and never waits for it): the
                # caller above already tore down what it could see, so drop
                # this transport instead of installing one nobody leases.
                try:
                    client.close()
                except Exception:
                    pass
                raise SFTPError("sftp member closed during dial")
            self._client = client
            try:
                self._sftp = self._client.open_sftp()
            except BaseException:
                self.close()
                raise
            try:
                _chan = self._sftp.get_channel()
                if _chan is not None:
                    _chan.settimeout(_SFTP_IO_TIMEOUT_S)
            except Exception:
                pass
            log.info("sftp connected to %s:%d", self._cfg.ssh_host, self._cfg.ssh_port)

    @staticmethod
    def _load_private_key(path: Path, *, passphrase: str | None) -> paramiko.PKey:
        """Key loaders shared with the module-level dial (see `_load_private_key`)."""
        return _load_private_key(path, passphrase=passphrase)

    def close(self) -> None:
        # Best-effort, never blocks: mark dead first so new leases skip
        # this member, then close even if the lock stays held — the
        # in-flight holder errors out of its op (callers already retry on
        # exactly that). A 5s lock wait here used to stall shutdown behind
        # one wedged holder; non-blocking acquire keeps it instant.
        # Re-entrant same-thread acquisition succeeds immediately, so nested
        # callers (connect() under fetch_torrent's guard) are unaffected.
        self._closed = True
        try:
            acquired = self._lock.acquire(blocking=False)
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

    def _reconnect_for_retry(self) -> bool:
        """Drop a dead transport and dial again; True when usable.

        A half-dead SFTP channel can look active while every read stalls
        until the IO timeout and then fails. Re-dialling gives the retry a
        fresh transport instead of the same broken one.
        """
        try:
            self.close()
        except Exception:
            pass
        try:
            self.connect()
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: reconnect failed: %s", e)
            return False

    # ---- torrent file ----

    def _torrent_dirs(self) -> list[str]:
        """Remote dirs that hold .torrent files (state dir + BT_backup).

        POSIX paths: these are remote paths, and the config value may be
        written with Windows separators on a dev box.
        """
        out: list[str] = []
        try:
            state = self._cfg.state_dir
        except Exception:
            state = None
        if state is not None:
            state_posix = PurePosixPath(
                state.as_posix() if hasattr(state, "as_posix") else str(state).replace("\\", "/")
            )
            out.append(state_posix.as_posix())
            out.append((state_posix.parent / "BT_backup").as_posix())
        seen: set[str] = set()
        dirs: list[str] = []
        for d in out:
            if d in seen:
                continue
            seen.add(d)
            dirs.append(d)
        return dirs

    @staticmethod
    def _bulk_tar_command(dirs: list[str]) -> str:
        """One `tar` command that streams every .torrent from `dirs`."""
        import shlex

        parts = ["tar", "-cf", "-", "--wildcards"]
        for d in dirs:
            parts += ["-C", shlex.quote(d), "*.torrent"]
        return " ".join(parts)

    def fetch_all_torrents(self) -> dict[str, bytes]:
        """Every .torrent under the configured dirs, keyed by infohash.

        One `tar` stream over the existing SSH session replaces N
        per-file SFTP reads: the fragile part is the transport, not the
        file count. Best-effort — a failure returns whatever was parsed
        (possibly empty) and never raises; the per-file path stays as
        the fallback.
        """
        out: dict[str, bytes] = {}
        dirs = self._torrent_dirs()
        if not dirs:
            return out
        try:
            with self._lock:
                try:
                    transport = self._client.get_transport() if self._client else None
                except Exception:
                    transport = None
                if transport is None or not transport.is_active():
                    log.info("sftp connection dropped or not active; reconnecting...")
                    try:
                        self.connect()
                    except Exception as e:
                        log.warning("sftp reconnect failed: %s", e)
                        return out
                try:
                    _stdin, stdout, _stderr = self._client.exec_command(
                        self._bulk_tar_command(dirs),
                        timeout=_BULK_IO_TIMEOUT_S,
                    )
                except Exception as e:
                    log.warning("sftp bulk torrent fetch could not start: %s", e)
                    return out
                try:
                    _channel = stdout.channel
                    _channel.settimeout(_BULK_IO_TIMEOUT_S)
                except Exception:
                    pass
                total = 0
                try:
                    with tarfile.open(fileobj=stdout, mode="r|") as tf:
                        for member in tf:
                            if not member.isfile() or not member.name.endswith(".torrent"):
                                continue
                            if len(out) >= _BULK_MAX_FILES or total >= _BULK_MAX_TOTAL_BYTES:
                                break
                            if int(member.size or 0) > MAX_TORRENT_BYTES:
                                continue
                            try:
                                fh = tf.extractfile(member)
                            except Exception:
                                continue
                            if fh is None:
                                continue
                            try:
                                data = fh.read(MAX_TORRENT_BYTES + 1)
                            except Exception:
                                continue
                            finally:
                                try:
                                    fh.close()
                                except Exception:
                                    pass
                            if len(data) > MAX_TORRENT_BYTES:
                                continue
                            parsed = _torrent_identity(data)
                            if not parsed:
                                continue
                            out[parsed] = data
                            total += len(data)
                except Exception as e:
                    log.warning("sftp bulk torrent fetch failed: %s", e)
                try:
                    _rc = stdout.channel.recv_exit_status()
                except Exception:
                    _rc = None
                if _rc not in (0, None) and not out:
                    log.warning(
                        "sftp bulk torrent fetch exited %s (no .torrent stream); "
                        "falling back to per-file reads",
                        _rc,
                    )
        except Exception as e:  # noqa: BLE001
            log.warning("sftp bulk torrent fetch failed: %s", e)
        return out

    def fetch_torrent(self, infohash: str) -> bytes | None:
        """Return the .torrent bytes for `infohash` or None if missing."""
        if (
            not infohash
            or len(infohash) != 40
            or not all(c in "0123456789abcdefABCDEF" for c in infohash)
        ):
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
            if (
                self._client is None
                or self._sftp is None
                or self._client.get_transport() is None
                or not self._client.get_transport().is_active()
            ):
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
                data: bytes | None = None
                for _attempt in (1, 2):
                    try:
                        with self._sftp.open(path_str, "rb") as f:  # type: ignore[union-attr]
                            data = f.read(MAX_TORRENT_BYTES + 1)
                        break
                    except FileNotFoundError:
                        data = None
                        break
                    except OSError as e:
                        # Paramiko surfaces missing files as IOError(errno 2),
                        # which may not subclass FileNotFoundError — treat
                        # as a miss.
                        if getattr(e, "errno", None) == 2:
                            data = None
                            break
                        log.warning("sftp: read %s failed: %s", path_str, e)
                        if not self._reconnect_for_retry():
                            break
                    except (paramiko.SSHException, EOFError) as e:
                        log.warning("sftp: read %s failed: %s", path_str, e)
                        if not self._reconnect_for_retry():
                            break
                    except Exception as e:  # noqa: BLE001
                        log.warning("sftp: unexpected error reading %s: %s", path_str, e)
                        break
                if data is None:
                    continue
                if len(data) > MAX_TORRENT_BYTES:
                    log.warning("sftp: %s exceeds %d bytes limit", path_str, MAX_TORRENT_BYTES)
                    continue
                from .watchdir import is_bencoded_torrent

                if is_bencoded_torrent(data):
                    return data
                log.warning(
                    "sftp: %s does not look like a bencoded torrent (%d bytes, head=%s)",
                    path_str,
                    len(data),
                    data[:12].hex(),
                )
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
                if (
                    self._client is None
                    or self._sftp is None
                    or self._client.get_transport() is None
                    or not self._client.get_transport().is_active()
                ):
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
                        log.warning("sftp statvfs %s failed: %r", path, e)
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
        """Free bytes for one path (POSIX). Caller holds the lock.

        Prefers `stat -f` on the exact path: plain `df` walks every
        mounted filesystem, so one wedged FUSE mount stalls ALL probes
        (the regular 10s channel timeouts seen in production). Falls
        back to `df -kP` parsing when stat is unavailable.
        """
        import shlex

        client = self._client
        if client is None:
            return None
        stdin, stdout, stderr = None, None, None
        try:
            try:
                stdin, stdout, stderr = client.exec_command(
                    f"stat -f -c '%a %S' {shlex.quote(path)}"
                )
                try:
                    stdout.channel.settimeout(10.0)
                except Exception:
                    pass
                out = stdout.read()
            finally:
                _close_exec_streams(stdin, stdout, stderr)
                stdin = stdout = stderr = None
            if out is None:
                return None
            parts = out.decode("utf-8", errors="replace").split()
            if len(parts) >= 2:
                try:
                    return int(parts[0]) * int(parts[1])
                except (TypeError, ValueError):
                    pass
        except Exception as e:  # noqa: BLE001
            # socket.timeout stringifies to "" — log the repr so the
            # next diagnosis isn't another empty "failed: " line.
            # Falls through to the df fallback below (as before).
            log.warning("ssh stat %s failed: %r", path, e)
        stdin, stdout, stderr = None, None, None
        try:
            try:
                stdin, stdout, stderr = client.exec_command(f"df -kP {shlex.quote(path)}")
            except Exception as e:  # noqa: BLE001
                log.warning("ssh df %s failed: %r", path, e)
                return None
            # Bound the read at the channel: a wedged transport must not
            # hold the member lock forever. (A per-call helper thread was
            # the old mechanism — it leaked one thread per wedged df.)
            try:
                stdout.channel.settimeout(10.0)
            except Exception:
                pass
            try:
                out = stdout.read()
            except Exception as e:  # noqa: BLE001
                log.warning("ssh df %s failed: %r", path, e)
                return None
            if out is None:
                return None
            out = out.decode("utf-8", errors="replace")
        finally:
            _close_exec_streams(stdin, stdout, stderr)
        try:
            lines = [ln.split() for ln in out.splitlines() if ln.split()]
            # Data is the LAST non-empty line: long device names wrap onto
            # extra lines under -P, so lines[1] can be a fragment.
            # Unwrapped: 6 fields with Available 4th. Wrapped: the device
            # spills above, leaving 5 numeric-led fields (Available 3rd).
            data = lines[-1] if lines else []
            if len(data) >= 6:
                avail = data[3]
            elif len(data) == 5 and data[0].isdigit():
                avail = data[2]
            else:
                log.warning("ssh df %s returned unparsable output: %r", path, out[:200])
                return None
            return int(avail) * 1024
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
            if (
                self._client is None
                or self._sftp is None
                or self._client.get_transport() is None
                or not self._client.get_transport().is_active()
            ):
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
            # Bound the backlog: a 100k-file state dir must not OOM the
            # daemon — stop collecting past the cap (coordinator treats
            # the rest as "not yet seen", retried next poll). Streams via
            # listdir_iter when available so a huge dir never materializes
            # fully (listdir_attr fetches everything first).
            _LIST_CAP = 20000
            _lister = getattr(self._sftp, "listdir_iter", None)
            try:
                if callable(_lister):
                    iterator = _lister(remote_dir)
                else:
                    iterator = iter(self._sftp.listdir_attr(remote_dir))
            except OSError as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            except (paramiko.SSHException, EOFError) as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            try:
                for entry in iterator:
                    if len(out) >= _LIST_CAP:
                        log.warning("sftp state dir exceeds %d entries; truncating list", _LIST_CAP)
                        break
                    try:
                        name = entry.filename
                    except AttributeError:
                        continue
                    if name.endswith(".torrent"):
                        digest = name[: -len(".torrent")]
                        if _HEX40_RE.fullmatch(digest):
                            out.append(digest.lower())
            except OSError as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            except (paramiko.SSHException, EOFError) as e:
                raise SFTPError(f"sftp listdir failed for {remote_dir}: {e}") from e
            finally:
                try:
                    close = getattr(iterator, "close", None)
                    if callable(close):
                        close()
                except Exception:
                    pass
            return out

    # ---- short remote commands, small file writes, detached launches ----
    #
    # These back the VPS1 push transport (see vps1_push.py). All three are
    # best-effort by contract: they return a failure value instead of
    # raising, because a push must never take the daemon down with it.

    def run_shell(
        self, command: str, timeout: float = _SHELL_CMD_TIMEOUT_S
    ) -> tuple[int | None, str, str]:
        """Run one short remote command; return (exit_status, stdout, stderr).

        Bounded and stream-cleaning like the stat/df probes (a leaked exec
        channel pins a server-side session). Never raises: an unreachable
        host, a wedged channel or a busy lock comes back as a non-zero/None
        status with the reason in stderr, which is exactly what a preflight
        check needs to degrade to "unknown". Caller-provided command string;
        every interpolated token must already be `shlex.quote`d by the caller.
        """
        if not command or not command.strip():
            return (None, "", "empty command")
        try:
            bound = max(1.0, float(timeout))
        except (TypeError, ValueError):
            bound = _SHELL_CMD_TIMEOUT_S
        out = ""
        err = ""
        status: int | None = None
        try:
            self._acquire_or_busy(self._lock, f"shell {command[:60]!r}")
        except Exception as e:  # noqa: BLE001
            return (None, "", str(e))
        try:
            with self._lock:
                if (
                    self._client is None
                    or self._client.get_transport() is None
                    or not self._client.get_transport().is_active()
                ):
                    try:
                        self.connect()
                    except Exception as e:  # noqa: BLE001
                        return (None, "", f"sftp connect failed: {e}")
                if self._client is None:
                    return (None, "", "sftp not connected")
                stdin = stdout = stderr = None
                try:
                    stdin, stdout, stderr = self._client.exec_command(command, timeout=bound)
                    try:
                        stdout.channel.settimeout(bound)
                    except Exception:
                        pass
                    # Read both streams before the exit status: a full stdout
                    # buffer deadlocks against a status wait.
                    try:
                        out = (stdout.read() or b"").decode("utf-8", errors="replace")
                    except Exception as e:  # noqa: BLE001
                        return (None, out, f"stdout read failed: {type(e).__name__}: {e}")
                    try:
                        err = (stderr.read() or b"").decode("utf-8", errors="replace")
                    except Exception:
                        err = ""
                    try:
                        status = stdout.channel.recv_exit_status()
                    except Exception:
                        status = None
                except Exception as e:  # noqa: BLE001
                    return (None, out, f"command failed to run: {type(e).__name__}: {e}")
                finally:
                    _close_exec_streams(stdin, stdout, stderr)
        except Exception as e:  # noqa: BLE001
            return (None, out, f"shell probe failed: {type(e).__name__}: {e}")
        finally:
            try:
                self._lock.release()
            except Exception:
                pass
        return (status, out, err)

    def write_remote_file(self, path: str, data: bytes) -> bool:
        """Write a SMALL remote file (0600) over SFTP; True when it landed.

        Used for the push file list only — never content (see the project's
        no-content-over-the-push rule), which the byte cap also enforces.
        The push lane itself uses `write_remote_file_dedicated` below.
        """
        refusal = _remote_write_refusal(path, data)
        clean = str(path or "").strip()
        if refusal:
            log.warning("sftp: %s", refusal)
            return False
        payload = bytes(data or b"")
        try:
            self._acquire_or_busy(self._lock, f"write {clean}")
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: could not write %s: %s", clean, e)
            return False
        try:
            with self._lock:
                if (
                    self._sftp is None
                    or self._client is None
                    or self._client.get_transport() is None
                    or not self._client.get_transport().is_active()
                ):
                    try:
                        self.connect()
                    except Exception as e:  # noqa: BLE001
                        log.warning("sftp: could not write %s: connect failed: %s", clean, e)
                        return False
                if self._sftp is None:
                    return False
                try:
                    with self._sftp.open(clean, "wb") as fh:
                        fh.write(payload)
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp: write of %s failed: %r", clean, e)
                    return False
                # 0600: the list is a path inventory of the racing box, so it
                # stays unreadable to other accounts on a shared host. A
                # server without the POSIX rename/chmod extension only loses
                # the tightening, not the write.
                try:
                    self._sftp.chmod(clean, 0o600)
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp: could not chmod %s to 0600: %r", clean, e)
                return True
        except Exception as e:  # noqa: BLE001
            log.warning("sftp: write of %s failed: %r", clean, e)
            return False
        finally:
            try:
                self._lock.release()
            except Exception:
                pass

    def launch_detached(self, command: str) -> bool:
        """Start `command` on a DEDICATED transport (never a pool member).

        See `_launch_detached`: the caller's command must already carry
        `setsid … </dev/null … &` so the job outlives this session. Safe to
        call after `close()` — it does not revive (or need) pool members.
        """
        return _launch_detached(self._cfg, command)

    def run_shell_dedicated(
        self, command: str, timeout: float = _SHELL_CMD_TIMEOUT_S
    ) -> tuple[int | None, str, str]:
        """`run_shell` on a throwaway transport — this member is never used.

        See `_run_shell_dedicated`; identical shape and identical
        never-raises contract, so a caller can switch dial without a branch.
        """
        return _run_shell_dedicated(self._cfg, command, timeout)

    def write_remote_file_dedicated(self, path: str, data: bytes) -> bool:
        """`write_remote_file` on a throwaway transport (see the module fn)."""
        return _write_remote_file_dedicated(self._cfg, path, data)


class SFTPExporter:
    """Pooled SFTP exporter: N independent SSH/SFTP connections.

    Re-inject bursts from concurrent coordinator workers used to serialize
    on a single paramiko transport behind one lock (the 15s-timeout
    clusters in the log). The pool leases a free member per call with
    round-robin start and fails over when one wedges; only when every
    member is busy does the caller get a fast TimeoutError for its
    existing retry path.

    Public API is unchanged from the old single-connection exporter
    (connect/close/start_torrent/start_many/disk_free_bytes/list_state_dir
    plus the context manager), so all existing call sites keep working.
    `pool_size` defaults to `[source.deluge_sftp].pool_size` (3).

    The push lane is the deliberate exception to the pooling: its short calls
    go through `run_shell_dedicated` / `write_remote_file_dedicated` (and its
    launch through `launch_detached`), none of which lease a member, so a
    push can never queue behind — or be starved by — a racing .torrent fetch,
    and a saturated pool can never make a push look unavailable.
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
        # Dial epoch: bumped on close() so a dial that completes after
        # shutdown tears its fresh transport down instead of leaking it
        # on a member the pool will never lease again.
        self._generation = 0

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
        # Never revive after close(): shutdown must stay shut (no
        # use-after-close re-dial from a late worker).
        if getattr(self, "_shutdown", False):
            raise RuntimeError("sftp pool is closed")
        deadline = time.monotonic() + _POOL_LEASE_TIMEOUT
        while True:
            for i in range(len(members)):
                m = members[(start + i) % len(members)]
                try:
                    if getattr(m, "_closed", False):
                        # Lazy single revive: dial only the member we may
                        # actually lease. Reviving the whole pool per call
                        # pays a dial storm after every transient outage.
                        self._try_revive(m)
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
                raise TimeoutError(f"sftp busy: {what} gave up waiting for a free connection")
            for i in range(len(members)):
                m = members[(start + i) % len(members)]
                try:
                    # No revive here: the first pass already tried. Dialing
                    # per 50ms slice would storm a down host while waiting.
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

    # Dial budget for the whole pool: one wedged dial must not hold
    # connect() (and the boot that calls it) forever.
    _DIAL_TIMEOUT_S = 60.0

    def connect(self) -> None:
        # Members are independent transports: dial in parallel instead of
        # serially (~3x45s worst case before). First failure closes EVERY
        # member and raises: pending dials that succeed after the raise
        # would otherwise leak (only already-completed ones were closed).
        import concurrent.futures as _fut

        members = self._members_snapshot()
        gen = self._generation

        def _dial_one(m: _SFTPConnection) -> None:
            m.connect()
            if gen != self._generation or getattr(self, "_shutdown", False):
                # Late success past close()/shutdown: tear the fresh
                # transport down at once instead of leaking it on a
                # member the pool will never lease again.
                try:
                    m.close()
                except Exception:
                    pass
                raise SFTPError("sftp pool closed during dial")

        pool = _fut.ThreadPoolExecutor(
            max_workers=max(1, len(members)),
            thread_name_prefix="sftp-dial",
        )
        futs: dict = {}
        try:
            futs = {pool.submit(_dial_one, m): m for m in members}
            try:
                for fut in _fut.as_completed(futs, timeout=self._DIAL_TIMEOUT_S):
                    exc = fut.exception()
                    if exc is not None:
                        raise exc
            except _fut.TimeoutError as e:
                raise SFTPError(f"sftp dial timed out after {self._DIAL_TIMEOUT_S:.0f}s") from e
        except Exception:
            # Invalidate every in-flight dial BEFORE closing: one still
            # inside connect() would otherwise finish after this loop,
            # install its transport, and see an unchanged generation —
            # leaving a live connection on a member the pool considers
            # poisoned (_dial_one tears down only a stale generation).
            try:
                self._generation += 1
            except Exception:
                pass
            for m in members:
                try:
                    m.close()
                except Exception:
                    pass
            raise
        finally:
            # Never wait for hung dials on the way out: running threads
            # die on their own socket timeouts; the transports they would
            # have installed are closed by the failure path above (or by
            # close() on shutdown).
            try:
                for _f in futs:
                    _f.cancel()
            except Exception:
                pass
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                try:
                    pool.shutdown(wait=False)
                except Exception:
                    pass

    def close(self) -> None:
        try:
            with self._pool_lock:
                self._shutdown = True
                self._generation += 1
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
        if (
            not infohash
            or len(infohash) != 40
            or not all(c in "0123456789abcdefABCDEF" for c in infohash)
        ):
            return None
        infohash = infohash.lower()
        m = self._lease(f"fetch {infohash[:10]}")
        try:
            return m.fetch_torrent(infohash)
        finally:
            self._release(m)

    def fetch_all_torrents(self) -> dict[str, bytes]:
        """Every .torrent the remote knows, keyed by infohash.

        One leased member runs a single `tar` stream: the per-file path
        costs one SFTP session per torrent, which is exactly what breaks
        when the link to the racing box is flaky. SYNC and blocking — run
        via asyncio.to_thread.
        """
        m = self._lease("fetch-all-torrents")
        try:
            return m.fetch_all_torrents()
        finally:
            self._release(m)

    def fetch_many(self, infohashes: Iterable[str]) -> dict[str, bytes]:
        """Fetch many .torrents, one pool member per hash concurrently.

        SYNC and blocking — run via asyncio.to_thread, never on the loop
        (a wedged member stalls its worker thread until its own timeouts
        fire; the pool lease still fails fast for the rest). Members are
        independent transports, so hashes fan out across
        min(len, pool_size) threads. Per-hash failures skip (a missing
        torrent must not fail its siblings).
        """
        import concurrent.futures as _fut

        hashes = [h for h in (infohashes or [])]
        if not hashes:
            return {}
        out: dict[str, bytes] = {}

        def _one(h: str) -> tuple[str, bytes | None]:
            try:
                return h, self.fetch_torrent(h)
            except Exception:
                return h, None

        _workers = max(1, min(len(hashes), self._pool_size))
        with _fut.ThreadPoolExecutor(
            max_workers=_workers, thread_name_prefix="sftp-fetch"
        ) as _pool:
            for h, data in _pool.map(_one, hashes):
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

    def run_shell(
        self, command: str, timeout: float = _SHELL_CMD_TIMEOUT_S
    ) -> tuple[int | None, str, str]:
        """Run one short remote command on a leased member (see the member).

        The lease is held only for the duration of the command — probes are
        sub-second, so they never sit behind a bulk fetch. SYNC and blocking:
        run it via asyncio.to_thread.
        """
        m = self._lease(f"shell {command[:40]!r}")
        try:
            return m.run_shell(command, timeout)
        finally:
            self._release(m)

    def write_remote_file(self, path: str, data: bytes) -> bool:
        """Write a small (0600) remote file over SFTP (see the member)."""
        m = self._lease(f"write {path}")
        try:
            return m.write_remote_file(path, data)
        finally:
            self._release(m)

    def launch_detached(self, command: str) -> bool:
        """Launch `command` on its OWN transport — no lease, no revive.

        Deliberately does not touch the pool: a multi-hour push would occupy
        a member for its whole life, and `close()` / `_reconnect_for_retry()`
        would drop the transport under it and orphan the transfer. Because it
        dials from the config directly, this still works while the pool is
        shut down (a push started before shutdown keeps running on the
        racing box regardless of what this process does).
        """
        return _launch_detached(self._cfg, command)

    def run_shell_dedicated(
        self, command: str, timeout: float = _SHELL_CMD_TIMEOUT_S
    ) -> tuple[int | None, str, str]:
        """One short remote command on a DEDICATED transport; never a lease.

        The push lane's preflight (`command -v`, `rclone listremotes`) and
        its log liveness read run here, so they can neither queue behind a
        racing .torrent fetch nor steal a member from one. Same
        (status, stdout, stderr) shape and same never-raises contract as
        `run_shell`, and — like `launch_detached` — it works with the pool
        closed, because it only ever dials from the config through
        `_dial_client` (the one host-key/credential path).
        """
        return _run_shell_dedicated(self._cfg, command, timeout)

    def write_remote_file_dedicated(self, path: str, data: bytes) -> bool:
        """Write a small remote file on a DEDICATED transport; never a lease.

        The push's `--files-from-raw` list goes this way for the same reason
        the probes do: the pooled members are the racing path's bandwidth, and
        a `TimeoutError()` on a list write used to be a refused push. The
        pooled `write_remote_file` stays available for every other caller.
        """
        return _write_remote_file_dedicated(self._cfg, path, data)
