"""Pytest fixtures shared across the test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import AppConfig


@pytest.fixture(scope="session")
def example_config() -> AppConfig:
    return AppConfig.from_toml(
        Path(__file__).parent.parent / "config.example.toml"
    )