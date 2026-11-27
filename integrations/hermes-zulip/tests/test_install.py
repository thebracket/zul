"""Tests for ``hermes_zulip.install``, the Hermes plugin installer."""

from __future__ import annotations

import importlib.metadata
import os
import shutil
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import hermes_zulip.install as install_module
import pytest
import yaml

_PYTHON_SRC = Path(__file__).resolve().parents[1] / "python"
_PLUGIN_YAML = _PYTHON_SRC / "hermes_zulip" / "plugin.yaml"
_PYPROJECT = _PYTHON_SRC.parent / "pyproject.toml"

_EXPECTED_INIT = (
    "from hermes_zulip.adapter import register\n"
    "\n"
    '__all__ = ["register"]\n'
)


def _installed_files(home: Path) -> set[Path]:
    return {path.relative_to(home) for path in home.rglob("*") if path.is_file()}


def test_explicit_home_wins_over_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "env-home"))
    home = tmp_path / "explicit"

    destination = install_module.install_plugin(home)

    assert destination == home / "plugins" / "zulip"
    assert _installed_files(home) == {
        Path("plugins/zulip/plugin.yaml"),
        Path("plugins/zulip/__init__.py"),
    }
    assert not (tmp_path / "env-home").exists()


def test_hermes_home_environment_variable_is_used(tmp_path, monkeypatch):
    home = tmp_path / "env-home"
    monkeypatch.setenv("HERMES_HOME", str(home))

    assert install_module.install_plugin() == home / "plugins" / "zulip"
    assert (home / "plugins" / "zulip" / "plugin.yaml").is_file()


def test_default_home_is_dot_hermes(tmp_path, monkeypatch):
    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))

    assert install_module.install_plugin() == tmp_path / ".hermes" / "plugins" / "zulip"


def test_installed_files_match_the_package(tmp_path):
    plugin_dir = install_module.install_plugin(tmp_path)

    assert (plugin_dir / "plugin.yaml").read_text(encoding="utf-8") == _PLUGIN_YAML.read_text(
        encoding="utf-8"
    )
    assert (plugin_dir / "__init__.py").read_text(encoding="utf-8") == _EXPECTED_INIT


def test_identical_files_are_left_alone(tmp_path, monkeypatch):
    home = tmp_path / "home"
    install_module.install_plugin(home)

    real_replace = os.replace
    replacements: list[tuple[object, object]] = []

    def spy_replace(src, dst):
        replacements.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(install_module.os, "replace", spy_replace)
    install_module.install_plugin(home)

    assert replacements == []


def test_changed_files_are_rewritten_atomically(tmp_path, monkeypatch):
    home = tmp_path / "home"
    plugin_dir = install_module.install_plugin(home)
    (plugin_dir / "plugin.yaml").write_text("stale manifest\n", encoding="utf-8")
    (plugin_dir / "__init__.py").write_text("stale module\n", encoding="utf-8")

    real_replace = os.replace
    replacements: list[tuple[Path, Path]] = []

    def spy_replace(src, dst):
        replacements.append((Path(src), Path(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(install_module.os, "replace", spy_replace)
    install_module.install_plugin(home)

    assert (plugin_dir / "plugin.yaml").read_text(encoding="utf-8") == _PLUGIN_YAML.read_text(
        encoding="utf-8"
    )
    assert (plugin_dir / "__init__.py").read_text(encoding="utf-8") == _EXPECTED_INIT
    # Both changed files went through a temp file and an atomic replace.
    assert {destination for _, destination in replacements} == {
        plugin_dir / "plugin.yaml",
        plugin_dir / "__init__.py",
    }
    assert all(source.parent == plugin_dir and source != destination
               for source, destination in replacements)
    # Atomic replacement leaves no temporary file behind.
    assert sorted(path.name for path in plugin_dir.iterdir()) == ["__init__.py", "plugin.yaml"]


def test_plugin_key_matches_the_manifest():
    manifest = yaml.safe_load(_PLUGIN_YAML.read_text(encoding="utf-8"))

    assert install_module.PLUGIN_KEY == manifest["name"]
    assert install_module.NEXT_STEP == f"hermes plugins enable {manifest['name']}"


def test_installer_writes_no_credentials(tmp_path, monkeypatch):
    secret = "not-a-real-secret-but-unique-4f7d1c"
    monkeypatch.setenv("ZULIP_API_KEY", secret)
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")

    plugin_dir = install_module.install_plugin(tmp_path)

    for path in plugin_dir.rglob("*"):
        if path.is_file():
            assert secret not in path.read_text(encoding="utf-8"), path


def test_main_prints_destination_and_next_step(tmp_path, capsys):
    home = tmp_path / "home"

    assert install_module.main(["--hermes-home", str(home)]) == 0

    out = capsys.readouterr().out
    assert str(home / "plugins" / "zulip") in out
    # Hermes keys a flat plugin by its manifest name, not the directory name.
    assert "hermes plugins enable zulip-platform" in out


def test_main_print_path_is_only_the_path(tmp_path, capsys):
    home = tmp_path / "home"

    assert install_module.main(["--hermes-home", str(home), "--print-path"]) == 0

    assert capsys.readouterr().out.strip() == str(home / "plugins" / "zulip")


def test_module_entry_point(tmp_path):
    home = tmp_path / "home"
    env = dict(os.environ)
    env.pop("HERMES_HOME", None)
    # The subprocess runs from the source tree; the installer no longer imports
    # the adapter/gateway, so it needs no Hermes runtime.
    source_paths = [str(_PYTHON_SRC)]
    existing = env.get("PYTHONPATH")
    if existing:
        source_paths.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(source_paths)

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "hermes_zulip.install",
            "--hermes-home",
            str(home),
            "--print-path",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == str(home / "plugins" / "zulip")
    assert (home / "plugins" / "zulip" / "plugin.yaml").is_file()
    assert (home / "plugins" / "zulip" / "__init__.py").is_file()


def test_console_script_is_declared_in_pyproject():
    pyproject = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    assert (
        pyproject["project"]["scripts"]["hermes-zulip-install"]
        == "hermes_zulip.install:main"
    )


def test_console_script_entry_point_is_declared():
    try:
        distribution = importlib.metadata.distribution("zul-bridge")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("zul-bridge is not installed in this environment")

    scripts = {
        entry.name: entry.value
        for entry in distribution.entry_points
        if entry.group == "console_scripts"
    }
    assert scripts.get("hermes-zulip-install") == "hermes_zulip.install:main"


def test_console_script_installs_the_plugin(tmp_path):
    script = shutil.which("hermes-zulip-install")
    installed_script = Path(sys.executable).with_name("hermes-zulip-install")
    candidates = [Path(script)] if script else []
    candidates.append(installed_script)
    executable = next((path for path in candidates if path.is_file()), None)
    if executable is None:
        pytest.skip("the hermes-zulip-install console script is not installed")

    home = tmp_path / "home"
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    env.pop("HERMES_HOME", None)
    completed = subprocess.run(
        [str(executable), "--hermes-home", str(home)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert str(home / "plugins" / "zulip") in completed.stdout
    assert "Next: hermes plugins enable zulip-platform" in completed.stdout
    assert (home / "plugins" / "zulip" / "plugin.yaml").is_file()
    assert (home / "plugins" / "zulip" / "__init__.py").is_file()


def test_installer_imports_without_the_hermes_gateway(tmp_path):
    script = textwrap.dedent(
        """
        import sys

        class Blocker:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "gateway" or fullname.startswith("gateway."):
                    raise ImportError("gateway blocked for this test")
                return None

        sys.meta_path.insert(0, Blocker())
        import hermes_zulip.chat_ids
        import hermes_zulip.install as install

        assert install.PLUGIN_KEY
        print("ok")
        """
    )
    env = {**os.environ, "PYTHONPATH": str(_PYTHON_SRC)}
    env.pop("HERMES_HOME", None)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, (
        f"installer import failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}")
    assert "ok" in completed.stdout
