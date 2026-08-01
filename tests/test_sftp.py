from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from racing_sync.config import DelugeSFTPConfig
from racing_sync.sftp_source import _SFTPConnection


def _sftp_cfg(**overrides):
    args: dict = {
        "enabled": True,
        "ssh_host": "localhost",
        "ssh_user": "x",
        "ssh_password": "pwd",
        "state_dir": Path("/tmp"),
    }
    args.update(overrides)
    return DelugeSFTPConfig(**args)


def _live_member(cfg, payload: bytes = b"d4:infod4:name4:teste"):
    """Hand-built pool member with a working (mocked) transport."""
    m = object.__new__(_SFTPConnection)
    m._cfg = cfg
    m._lock = threading.RLock()
    mock_client = MagicMock()
    mock_client.get_transport().is_active.return_value = True
    mock_sftp = MagicMock()
    file_mock = MagicMock()
    file_mock.read.return_value = payload
    file_mock.__enter__.return_value = file_mock
    mock_sftp.open.return_value = file_mock
    m._client = mock_client
    m._sftp = mock_sftp
    return m


def test_sftp_exporter_requires_auth():
    with pytest.raises(Exception):
        DelugeSFTPConfig(
            enabled=True,
            ssh_host="localhost",
            ssh_user="x",
            state_dir=Path("/tmp"),
        )


def test_sftp_config_rejects_empty_or_whitespace_password():
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="Deluge SFTP requires ssh_password or ssh_key_path when enabled"):
        DelugeSFTPConfig(
            enabled=True,
            ssh_host="localhost",
            ssh_user="x",
            ssh_password="",
            state_dir=Path("/tmp"),
        )

    with pytest.raises(ValidationError, match="Deluge SFTP requires ssh_password or ssh_key_path when enabled"):
        DelugeSFTPConfig(
            enabled=True,
            ssh_host="localhost",
            ssh_user="x",
            ssh_password="   ",
            state_dir=Path("/tmp"),
        )



def test_pool_size_config_defaults_and_validates():
    from pydantic import ValidationError
    from racing_sync.sftp_source import SFTPExporter, _coerce_pool_size

    assert _sftp_cfg().pool_size == 3
    assert SFTPExporter(_sftp_cfg()).pool_size == 3
    assert SFTPExporter(_sftp_cfg(), pool_size=1).pool_size == 1
    assert _coerce_pool_size(99) == 8
    assert _coerce_pool_size(0) == 1
    assert _coerce_pool_size("nope") == 3
    assert _coerce_pool_size(True) == 3
    with pytest.raises(ValidationError):
        _sftp_cfg(pool_size=0)
    with pytest.raises(ValidationError):
        _sftp_cfg(pool_size=9)


def test_sftp_exporter_close_cleans_resources():
    from racing_sync.sftp_source import SFTPExporter

    exporter = SFTPExporter(_sftp_cfg(), pool_size=2)
    exporter._members = [_live_member(exporter._cfg), _live_member(exporter._cfg)]
    sftps = [m._sftp for m in exporter._members]
    clients = [m._client for m in exporter._members]

    exporter.close()

    for m, mock_sftp, mock_client in zip(exporter._members, sftps, clients, strict=True):
        mock_sftp.close.assert_called_once()
        mock_client.close.assert_called_once()
        assert m._sftp is None
        assert m._client is None


def test_sftp_exporter_connect_closes_prior_connection():
    from unittest.mock import MagicMock, patch

    exporter = _SFTPConnection(_sftp_cfg())
    old_sftp = MagicMock()
    old_client = MagicMock()
    exporter._sftp = old_sftp
    exporter._client = old_client

    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        new_client = MagicMock()
        mock_ssh_cls.return_value = new_client
        new_client.open_sftp.return_value = MagicMock()

        exporter.connect()

        # Prior resources must be closed
        old_sftp.close.assert_called_once()
        old_client.close.assert_called_once()
        assert exporter._client is new_client


def test_pool_connect_opens_one_ssh_per_member():
    from unittest.mock import MagicMock, patch
    from racing_sync.sftp_source import SFTPExporter

    exporter = SFTPExporter(_sftp_cfg(), pool_size=3)
    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.open_sftp.return_value = MagicMock()

        exporter.connect()

        assert mock_ssh_cls.call_count == 3
        assert len(exporter._members) == 3


def test_sftp_pool_serves_concurrent_fetches():
    """Re-inject bursts must fly in parallel, not serialize on one transport."""
    import concurrent.futures
    import time
    from racing_sync.sftp_source import SFTPExporter

    exporter = SFTPExporter(_sftp_cfg(), pool_size=3)

    def _slow_open(path, mode):
        time.sleep(0.05)
        file_mock = MagicMock()
        file_mock.read.return_value = b"d4:infod4:name4:teste"
        file_mock.__enter__.return_value = file_mock
        return file_mock

    members = []
    for _ in range(3):
        m = _live_member(exporter._cfg)
        m._sftp.open.side_effect = _slow_open
        members.append(m)
    exporter._members = members

    start_barrier = threading.Barrier(6)

    def _fetch():
        start_barrier.wait(timeout=10)
        return exporter.fetch_torrent("a" * 40)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: _fetch(), range(6)))

    assert len(results) == 6
    assert all(r == b"d4:infod4:name4:teste" for r in results)
    # Every fetch cost exactly one open (no reconnect storms) ...
    assert sum(m._sftp.open.call_count for m in members) == 6
    # ... spread over more than one member (parallel, not serialized).
    assert sum(1 for m in members if m._sftp.open.call_count > 0) >= 2


def test_sftp_host_key_policy_defaults_to_reject():
    import paramiko
    from unittest.mock import MagicMock, patch

    exporter = _SFTPConnection(_sftp_cfg())

    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.open_sftp.return_value = MagicMock()

        exporter.connect()

        client.load_system_host_keys.assert_called_once()
        args, _ = client.set_missing_host_key_policy.call_args
        assert isinstance(args[0], paramiko.RejectPolicy)


def test_sftp_host_key_policy_custom_known_hosts():
    from unittest.mock import MagicMock, patch

    known_hosts = Path("/tmp/custom_known_hosts")
    exporter = _SFTPConnection(_sftp_cfg(known_hosts_path=known_hosts))

    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.open_sftp.return_value = MagicMock()

        exporter.connect()

        client.load_host_keys.assert_called_once_with(str(known_hosts))


def test_sftp_host_key_policy_auto_add_opt_in():
    import paramiko
    from unittest.mock import MagicMock, patch

    exporter = _SFTPConnection(_sftp_cfg(auto_add_host_key=True))

    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.open_sftp.return_value = MagicMock()

        exporter.connect()

        args, _ = client.set_missing_host_key_policy.call_args
        assert isinstance(args[0], paramiko.AutoAddPolicy)


def test_ipv4_socket_closes_socket_on_oserror():
    import socket
    from unittest.mock import MagicMock, patch
    from racing_sync.sftp_source import _ipv4_socket, SFTPError

    mock_s1 = MagicMock()
    mock_s1.connect.side_effect = OSError("connect failed")

    with patch("socket.getaddrinfo") as mock_gai, patch("socket.socket") as mock_sock:
        mock_gai.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 22)),
        ]
        mock_sock.return_value = mock_s1

        with pytest.raises(SFTPError):
            _ipv4_socket("127.0.0.1", 22, timeout=5)

        # Socket must have been closed when connect failed
        mock_s1.close.assert_called_once()


def test_connect_failure_cleans_up_socket_and_client():
    from unittest.mock import MagicMock, patch

    exporter = _SFTPConnection(_sftp_cfg())
    mock_sock = MagicMock()

    with patch("racing_sync.sftp_source._ipv4_socket", return_value=mock_sock), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.connect.side_effect = RuntimeError("auth error")

        with pytest.raises(RuntimeError, match="auth error"):
            exporter.connect()

        mock_sock.close.assert_called_once()
        client.close.assert_called_once()
        assert exporter._client is None
        assert exporter._sftp is None


def test_fetch_torrent_uses_posix_path_and_caps_read():
    from unittest.mock import MagicMock
    from racing_sync.sftp_source import MAX_TORRENT_BYTES

    exporter = _SFTPConnection(
        _sftp_cfg(state_dir=Path(r"\var\data\deluge\state"))
    )
    mock_client = MagicMock()
    mock_client.get_transport().is_active.return_value = True
    mock_sftp = MagicMock()
    exporter._client = mock_client
    exporter._sftp = mock_sftp

    # 1. Normal file read
    mock_file = MagicMock()
    mock_file.read.return_value = b"d8:announcee"
    mock_file.__enter__.return_value = mock_file
    mock_sftp.open.return_value = mock_file

    hash_val = "1" * 40
    data = exporter.fetch_torrent(hash_val)
    assert data == b"d8:announcee"

    # Verify POSIX forward slashes were used in SFTP remote open call
    opened_path = mock_sftp.open.call_args[0][0]
    assert "\\" not in opened_path
    assert opened_path == f"/var/data/deluge/state/{hash_val}.torrent"
    # Verify read was capped
    mock_file.read.assert_called_once_with(MAX_TORRENT_BYTES + 1)

    # 2. Oversize file (> 20MB)
    mock_file.reset_mock()
    mock_file.read.return_value = b"d" + (b"0" * (MAX_TORRENT_BYTES + 10))
    mock_sftp.open.return_value = mock_file
    data_oversize = exporter.fetch_torrent(hash_val)
    assert data_oversize is None


def test_disk_free_bytes_prefers_statvfs():
    import threading
    from unittest.mock import MagicMock

    exporter = object.__new__(_SFTPConnection)
    exporter._lock = threading.RLock()
    mock_client = MagicMock()
    mock_client.get_transport().is_active.return_value = True
    mock_sftp = MagicMock()
    mock_sftp.statvfs.return_value = MagicMock(f_frsize=4096, f_bavail=1000)
    exporter._client = mock_client
    exporter._sftp = mock_sftp

    assert exporter.disk_free_bytes("/data") == 4096 * 1000
    mock_client.exec_command.assert_not_called()


def test_disk_free_bytes_falls_back_to_df_without_statvfs():
    """Paramiko builds without SFTPClient.statvfs (as on the VPS) must still
    report free space via `df -kP` instead of degrading to unknown."""
    import threading
    from unittest.mock import MagicMock

    exporter = object.__new__(_SFTPConnection)
    exporter._lock = threading.RLock()
    mock_client = MagicMock()
    mock_client.get_transport().is_active.return_value = True
    mock_stdout = MagicMock()
    mock_stdout.read.return_value = (
        b"Filesystem 1024-blocks Used Available Capacity Mounted on\n"
        b"/dev/sda1 80000000 76000000 4000000 95% /home\n"
    )
    mock_client.exec_command.return_value = (MagicMock(), mock_stdout, MagicMock())
    # No statvfs attribute at all, mirroring the older paramiko on the VPS.
    mock_sftp = MagicMock(spec=[])
    exporter._client = mock_client
    exporter._sftp = mock_sftp

    assert exporter.disk_free_bytes("/home/user/.config/deluge/state") == 4000000 * 1024
    mock_client.exec_command.assert_called_once()


def test_disk_free_bytes_none_on_unparsable_df():
    import threading
    from unittest.mock import MagicMock

    exporter = object.__new__(_SFTPConnection)
    exporter._lock = threading.RLock()
    mock_client = MagicMock()
    mock_client.get_transport().is_active.return_value = True
    mock_stdout = MagicMock()
    mock_stdout.read.return_value = b"garbage\n"
    mock_client.exec_command.return_value = (MagicMock(), mock_stdout, MagicMock())
    exporter._client = mock_client
    exporter._sftp = MagicMock(spec=[])

    assert exporter.disk_free_bytes("/data") is None


def _wedge_lock_in_thread(lock: threading.RLock):
    """Hold `lock` from a daemon thread; returns (ready, release, thread)."""
    holder_ready = threading.Event()
    release_holder = threading.Event()

    def _hold():
        lock.acquire()
        holder_ready.set()
        try:
            assert release_holder.wait(timeout=30)
        finally:
            lock.release()

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    assert holder_ready.wait(timeout=10)
    return holder_ready, release_holder, holder


def test_member_fetch_fails_fast_when_lock_wedged(monkeypatch):
    """A wedged member lock must fail fast (TimeoutError), not hang.

    Regression for the ~2min silent stall: abandoned to_thread workers kept
    the shared lock while callers queued with no bound. Now the waiter fails
    fast (TimeoutError) so coordinator retry/backoff paths engage.
    """
    import time
    import racing_sync.sftp_source as sftp_mod

    monkeypatch.setattr(sftp_mod, "_SFTP_LOCK_TIMEOUT", 0.2)

    member = object.__new__(_SFTPConnection)
    member._lock = threading.RLock()
    member._client = MagicMock()
    member._sftp = MagicMock()

    # Wedge the lock from ANOTHER thread (same-thread re-acquire is legal
    # for RLock and must keep working).
    _, release_holder, holder = _wedge_lock_in_thread(member._lock)
    try:
        start = time.monotonic()
        with pytest.raises(TimeoutError, match="sftp busy"):
            member.fetch_torrent("a" * 40)
        assert time.monotonic() - start < 5.0
    finally:
        release_holder.set()
        holder.join(timeout=10)
    # Lock usable again afterwards.
    assert member._lock.acquire(blocking=False)
    member._lock.release()


def test_pool_fails_over_wedged_member():
    """One wedged pool member must not wedge pool callers (the log incident).

    With three SFTP timeouts in a row during re-inject bursts, a single
    stuck transport previously stalled every later fetch. The pool serves
    from a free member instead.
    """
    from racing_sync.sftp_source import SFTPExporter

    exporter = SFTPExporter(_sftp_cfg(), pool_size=2)
    exporter._members = [_live_member(exporter._cfg), _live_member(exporter._cfg)]

    _, release_holder, holder = _wedge_lock_in_thread(exporter._members[0]._lock)
    try:
        assert exporter.fetch_torrent("a" * 40) == b"d4:infod4:name4:teste"
        # The wedged member was skipped; the healthy one served.
        assert exporter._members[0]._sftp.open.call_count == 0
        assert exporter._members[1]._sftp.open.call_count == 1
    finally:
        release_holder.set()
        holder.join(timeout=10)


def test_pool_all_wedged_fails_fast(monkeypatch):
    """Wedged-everywhere degrades to a fast TimeoutError for retry paths."""
    import time
    import racing_sync.sftp_source as sftp_mod
    from racing_sync.sftp_source import SFTPExporter

    monkeypatch.setattr(sftp_mod, "_POOL_LEASE_TIMEOUT", 0.2)

    exporter = SFTPExporter(_sftp_cfg(), pool_size=2)
    exporter._members = [_live_member(exporter._cfg), _live_member(exporter._cfg)]

    holders = [_wedge_lock_in_thread(m._lock) for m in exporter._members]
    try:
        start = time.monotonic()
        with pytest.raises(TimeoutError, match="sftp busy"):
            exporter.fetch_torrent("a" * 40)
        assert time.monotonic() - start < 5.0
    finally:
        for _, release_holder, holder in holders:
            release_holder.set()
            holder.join(timeout=10)


def test_pool_close_does_not_hang_on_wedged_member():
    import time
    from racing_sync.sftp_source import SFTPExporter

    exporter = SFTPExporter(_sftp_cfg(), pool_size=2)
    exporter._members = [_live_member(exporter._cfg), _live_member(exporter._cfg)]
    healthy_sftp = exporter._members[1]._sftp

    _, release_holder, holder = _wedge_lock_in_thread(exporter._members[0]._lock)
    try:
        start = time.monotonic()
        exporter.close()  # must return promptly, not hang forever
        elapsed = time.monotonic() - start
    finally:
        release_holder.set()
        holder.join(timeout=10)
    assert elapsed < 30.0
    # Healthy member still closed.
    healthy_sftp.close.assert_called_once()

