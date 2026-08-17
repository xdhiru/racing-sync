"""Pytest fixtures shared across the test suite."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from racing_sync.config import AppConfig
from racing_sync.coordinator import Coordinator


@pytest.fixture(scope="session")
def example_config() -> AppConfig:
    return AppConfig.from_toml(
        Path(__file__).parent.parent / "config.example.toml"
    )


@pytest.fixture
def anyio_backend():
    return "asyncio"


def make_coordinator(store=None, **attrs) -> Coordinator:
    """Bare Coordinator with mock cfg/store plus worker bookkeeping.

    `cfg` is a bare MagicMock (no value defaults) and `store` defaults to
    a MagicMock, so scheduling semantics match hand-built scaffolds
    exactly. Clients (`source_client`/`dest_client`) are deliberately NOT
    provided — several code paths branch on `hasattr` for them, so each
    test sets the clients it exercises. Extra attributes — including
    dotted cfg paths like ``cfg__ssd__path`` — are assigned verbatim:

        coord = make_coordinator(store, cfg__dest__save_path=tmp_path)
    """
    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.store = store if store is not None else MagicMock()
    coord._running_infohashes = set()
    coord._tasks = set()
    coord._live = {}
    for key, value in attrs.items():
        target = coord
        *path, last = key.split("__")
        for part in path:
            target = getattr(target, part)
        setattr(target, last, value)
    return coord