from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import DelugeSFTPConfig


def test_sftp_exporter_requires_auth():
    with pytest.raises(Exception):
        DelugeSFTPConfig(
            enabled=True,
            ssh_host="localhost",
            ssh_user="x",
            state_dir=Path("/tmp"),
        )


def test_sftp_exporter_close_cleans_resources():
    from unittest.mock import MagicMock
    from racing_sync.sftp_source import SFTPExporter

    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)
    mock_sftp = MagicMock()
    mock_client = MagicMock()
    exporter._sftp = mock_sftp
    exporter._client = mock_client

    exporter.close()

    mock_sftp.close.assert_called_once()
    mock_client.close.assert_called_once()
    assert exporter._sftp is None
    assert exporter._client is None


def test_sftp_exporter_connect_closes_prior_connection():
    from unittest.mock import MagicMock, patch
    from racing_sync.sftp_source import SFTPExporter

    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)
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


def test_sftp_exporter_fetch_torrent_thread_safety():
    import concurrent.futures
    from unittest.mock import MagicMock
    from racing_sync.sftp_source import SFTPExporter

    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)
    mock_client = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_client.get_transport.return_value = mock_transport
    mock_sftp = MagicMock()
    exporter._client = mock_client
    exporter._sftp = mock_sftp

    file_mock = MagicMock()
    file_mock.read.return_value = b"d4:infod4:name4:teste"
    file_mock.__enter__.return_value = file_mock
    mock_sftp.open.return_value = file_mock

    valid_hash = "a" * 40
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(exporter.fetch_torrent, valid_hash) for _ in range(10)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    assert len(results) == 10
    assert all(r == b"d4:infod4:name4:teste" for r in results)


def test_sftp_host_key_policy_defaults_to_reject():
    import paramiko
    from unittest.mock import MagicMock, patch
    from racing_sync.sftp_source import SFTPExporter

    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)

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
    from racing_sync.sftp_source import SFTPExporter

    known_hosts = Path("/tmp/custom_known_hosts")
    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        known_hosts_path=known_hosts,
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)

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
    from racing_sync.sftp_source import SFTPExporter

    cfg = DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        auto_add_host_key=True,
        state_dir=Path("/tmp"),
    )
    exporter = SFTPExporter(cfg)

    with patch("racing_sync.sftp_source._ipv4_socket"), \
         patch("paramiko.SSHClient") as mock_ssh_cls:
        client = MagicMock()
        mock_ssh_cls.return_value = client
        client.open_sftp.return_value = MagicMock()

        exporter.connect()

        args, _ = client.set_missing_host_key_policy.call_args
        assert isinstance(args[0], paramiko.AutoAddPolicy)

