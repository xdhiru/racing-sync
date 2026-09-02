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