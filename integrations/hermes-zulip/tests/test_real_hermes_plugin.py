"""Optional integration check against a real Hermes checkout.

Set ``HERMES_REPO`` to a Hermes Agent checkout to run this test.  It runs in a
subprocess so the unit tests' lightweight stub modules never shadow the real
``gateway``/``hermes_cli`` packages.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_HERMES_REPO = os.environ.get("HERMES_REPO")
_PLUGIN_DIR = Path(__file__).resolve().parents[1] / "python" / "hermes_zulip"


@pytest.mark.skipif(not _HERMES_REPO, reason="HERMES_REPO is not set")
def test_real_hermes_loads_and_registers_plugin(tmp_path):
    repo = Path(_HERMES_REPO)
    if not (repo / "hermes_cli" / "plugins.py").is_file():
        pytest.skip("HERMES_REPO does not look like a Hermes checkout")

    home = tmp_path / "hermes-home"
    plugins = home / "plugins"
    plugins.mkdir(parents=True)
    shutil.copytree(_PLUGIN_DIR, plugins / "zulip",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (home / "config.yaml").write_text(
        "plugins:\n  enabled:\n    - zulip-platform\n", encoding="utf-8")

    script = textwrap.dedent(
        """
        import os
        from gateway.config import Platform, PlatformConfig
        from gateway.platform_registry import platform_registry
        from hermes_cli.plugins import PluginManager

        manager = PluginManager(scope_key=os.environ["HERMES_HOME"])
        manager.discover_and_load()
        loaded = manager._plugins.get("zulip-platform")
        assert loaded is not None, "plugin was not discovered"
        assert loaded.error is None, loaded.error
        assert loaded.enabled is True
        entry = platform_registry.get("zulip")
        assert entry is not None, "platform was not registered"
        assert entry.adapter_factory is not None
        assert entry.validate_config(PlatformConfig(enabled=True, extra={})) is True
        adapter = entry.adapter_factory(PlatformConfig(enabled=True, extra={}))
        assert adapter.platform is Platform("zulip")
        assert adapter.splits_long_messages is True
        assert entry.parse_target_ref_fn("stream:core/ops")[0].startswith("zulip:v1:stream:0:")
        print("ok")
        """
    )
    env = {
        **os.environ,
        "PYTHONPATH": str(repo),
        "HERMES_HOME": str(home),
        "ZULIP_SITE": "https://chat.example.com",
        "ZULIP_EMAIL": "bot@example.com",
        "ZULIP_API_KEY": "secret-key",
    }
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert completed.returncode == 0, (
        f"real Hermes plugin load failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}")
    assert "ok" in completed.stdout
