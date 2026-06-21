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
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import paramiko  # type: ignore[import-untyped]

from .config import DelugeSFTPConfig

log = logging.getLogger(__name__)

MAX_TORRENT_BYTES: int = 20 * 1024 * 1024  # 20 MiB safety cap, matching Prowlarr


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



class SFTPExporter:
    def __init__(self, cfg: DelugeSFTPConfig):
        self._cfg = cfg
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None
        self._lock = threading.RLock()

    def __enter__(self) -> "SFTPExporter":
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def connect(self) -> None:
        with self._lock:
            self.close()
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
                    self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                else:
                    self._client.set_missing_host_key_policy(paramiko.RejectPolicy())
                kwargs: dict = {
                    "hostname": self._cfg.ssh_host,
                    "port": self._cfg.ssh_port,
                    "username": self._cfg.ssh_user,
                    "timeout": 15,
                    "allow_agent": True,
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
        with self._lock:
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

    # ---- torrent file ----

    def fetch_torrent(self, infohash: str) -> bytes | None:
        """Return the .torrent bytes for `infohash` or None if missing."""
        if not infohash or len(infohash) != 40 or not all(c in "0123456789abcdefABCDEF" for c in infohash):
            return None

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
                except (OSError, paramiko.SSHException, EOFError) as e:
                    log.warning("sftp: read %s failed: %s", path_str, e)
                    continue
                except Exception as e:  # noqa: BLE001
                    log.warning("sftp: unexpected error reading %s: %s", path_str, e)
                    continue
            return None

    def fetch_many(self, infohashes: Iterable[str]) -> dict[str, bytes]:
        return {h: data for h, data in ((h, self.fetch_torrent(h)) for h in infohashes) if data}

    def list_state_dir(self) -> list[str]:
        with self._lock:
            if self._sftp is None:
                raise SFTPError("not connected")
            out: list[str] = []
            remote_dir = PurePosixPath(
                self._cfg.state_dir.as_posix()
                if hasattr(self._cfg.state_dir, "as_posix")
                else str(self._cfg.state_dir).replace("\\", "/")
            ).as_posix()
            for entry in self._sftp.listdir_attr(remote_dir):
                name = entry.filename
                if name.endswith(".torrent"):
                    out.append(name[: -len(".torrent")])
            return out
