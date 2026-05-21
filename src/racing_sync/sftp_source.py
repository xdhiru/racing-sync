"""SFTP / SSH exporter for downloading .torrent files from a remote client.

Used as a fallback when a torrent's .torrent file is not otherwise retrievable:
  - Deluge state dir: ~/.config/deluge/state/<infohash>.torrent
  - qBittorrent state dir: ~/.local/share/qBittorrent/BT_backup/<infohash>.torrent
"""

from __future__ import annotations

import io
import logging
import socket
from pathlib import Path
from typing import Iterable

import paramiko

from .config import DelugeSFTPConfig

log = logging.getLogger(__name__)


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
        try:
            s = socket.socket(family, kind, proto)
            s.settimeout(timeout)
            s.connect(sockaddr)
            return s
        except OSError as e:
            last_err = e
            continue
    raise SFTPError(f"could not connect to {host}:{port} (IPv4): {last_err}")



class SFTPExporter:
    def __init__(self, cfg: DelugeSFTPConfig):
        self._cfg = cfg
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def __enter__(self) -> "SFTPExporter":
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def connect(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
                pkey = self._load_private_key(
                    self._cfg.ssh_key_path,
                    passphrase=self._cfg.ssh_key_passphrase or None,
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
            kwargs["password"] = self._cfg.ssh_password
        # Force IPv4 resolution: the racing VPS may not have a routable
        # IPv6 address and paramiko defaults to getaddrinfo's first
        # result, which can be a hung AAAA connection. We pre-resolve
        # with AF_INET, open a socket ourselves, and pass it to connect().
        kwargs["sock"] = _ipv4_socket(
            self._cfg.ssh_host, self._cfg.ssh_port, timeout=15
        )
        self._client.connect(**kwargs)
        self._sftp = self._client.open_sftp()
        log.info("sftp connected to %s:%d", self._cfg.ssh_host, self._cfg.ssh_port)

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
        if self._sftp is not None:
            self._sftp.close()
        if self._client is not None:
            self._client.close()

    # ---- torrent file ----

    def fetch_torrent(self, infohash: str) -> bytes | None:
        """Return the .torrent bytes for `infohash` or None if missing."""
        if not infohash or len(infohash) != 40 or not all(c in "0123456789abcdefABCDEF" for c in infohash):
            return None

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

        candidates = [
            Path(self._cfg.state_dir) / f"{infohash}.torrent",
        ]
        # Some setups have the file under BT_backup/<hash>.torrent (qBittorrent)
        candidates.append(
            Path(self._cfg.state_dir).parent
            / "BT_backup"
            / f"{infohash}.torrent"
        )
        for path in candidates:
            try:
                with self._sftp.open(str(path), "rb") as f:  # type: ignore[union-attr]
                    data = f.read()
                if data.startswith(b"d"):
                    return data
                log.warning("sftp: %s does not look like a bencoded torrent", path)
            except FileNotFoundError:
                continue
            except (OSError, paramiko.SSHException, EOFError) as e:
                log.warning("sftp: read %s failed: %s", path, e)
                continue
            except Exception as e:  # noqa: BLE001
                log.warning("sftp: unexpected error reading %s: %s", path, e)
                continue
        return None

    def fetch_many(self, infohashes: Iterable[str]) -> dict[str, bytes]:
        return {h: data for h, data in ((h, self.fetch_torrent(h)) for h in infohashes) if data}

    def list_state_dir(self) -> list[str]:
        if self._sftp is None:
            raise SFTPError("not connected")
        out: list[str] = []
        for entry in self._sftp.listdir_attr(str(self._cfg.state_dir)):
            name = entry.filename
            if name.endswith(".torrent"):
                out.append(name[: -len(".torrent")])
        return out