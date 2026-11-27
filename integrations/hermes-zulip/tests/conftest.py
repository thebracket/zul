"""Pytest bootstrap for the adapter tests.

Unit tests run against the faithful minimal Hermes stub in ``hermes_stub`` so
they need no Hermes install or third-party gateway dependencies.  The separate
``test_real_hermes_plugin.py`` exercises the real checkout when ``HERMES_REPO``
is set.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_DIR.parent
_PYTHON_SRC = _REPO_ROOT / "python"


def _prepend(path: Path) -> None:
    resolved = str(path)
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


_prepend(_TESTS_DIR / "hermes_stub")
_prepend(_TESTS_DIR)
_prepend(_PYTHON_SRC)


@pytest.fixture(autouse=True)
def _clean_zulip_env(monkeypatch):
    """Start every test without inherited ZULIP_* settings."""
    for name in list(os.environ):
        if name.startswith("ZULIP_"):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _clear_platform_locks():
    """Keep the stub's process-wide lock registry isolated per test."""
    from gateway.platforms.base import _LOCKS

    _LOCKS.clear()
    yield
    _LOCKS.clear()


@pytest.fixture
def fake_bridge(monkeypatch):
    """Install a fake ``zul_bridge`` extension in the adapter module."""
    import hermes_zulip.adapter as adapter_module
    from fake_bridge import FakeBridgeModule

    module = FakeBridgeModule()
    monkeypatch.setattr(adapter_module, "zul_bridge", module, raising=False)
    return module


@pytest.fixture
def config():
    from gateway.config import PlatformConfig

    return PlatformConfig(enabled=True, extra={})
