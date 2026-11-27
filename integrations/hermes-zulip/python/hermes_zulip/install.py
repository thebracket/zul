"""Install the Hermes Zulip platform plugin into a Hermes home directory.

The installer copies the two files Hermes needs into
``<hermes_home>/plugins/<name>/``: the packaged ``plugin.yaml`` manifest and a
one-line ``__init__.py`` that re-exports the adapter's ``register`` entry point.
It reads no credentials and writes only those two files, so it is safe to run
before Zulip is configured.

    hermes-zulip-install
    hermes-zulip-install --hermes-home /path/to/hermes-home
    python -m hermes_zulip.install --print-path
"""

from __future__ import annotations

import argparse
import contextlib
import os
import tempfile
from importlib import resources
from pathlib import Path

PLUGIN_NAME = "zulip"
# Hermes keys a flat user plugin by its manifest ``name`` (the directory name is
# not used). ``plugin.yaml`` declares ``zulip-platform``, so that is the key
# ``hermes plugins enable`` accepts.
PLUGIN_KEY = "zulip-platform"
NEXT_STEP = f"hermes plugins enable {PLUGIN_KEY}"

_INIT_SOURCE = """\
from hermes_zulip.adapter import register

__all__ = ["register"]
"""


def _resolve_hermes_home(hermes_home: Path | None) -> Path:
    """Pick the Hermes home: explicit argument, then ``HERMES_HOME``, then the default."""
    if hermes_home is not None:
        return Path(hermes_home).expanduser()
    env_home = os.environ.get("HERMES_HOME", "").strip()
    if env_home:
        return Path(env_home).expanduser()
    return Path.home() / ".hermes"


def _plugin_manifest() -> str:
    """Read ``plugin.yaml`` from the installed package (wheel or source tree)."""
    return resources.files(__package__).joinpath("plugin.yaml").read_text(encoding="utf-8")


def _write_if_changed(path: Path, content: str) -> bool:
    """Write *content* atomically, leaving an identical file untouched.

    Returns ``True`` when the file was (re)written.  ``os.replace`` makes the
    replacement atomic on every platform Python supports here, so a crash never
    leaves a partial manifest behind.
    """
    data = content.encode("utf-8")
    try:
        if path.read_bytes() == data:
            return False
    except FileNotFoundError:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    return True


def install_plugin(hermes_home: Path | None = None) -> Path:
    """Install the plugin and return its directory under the resolved Hermes home.

    *hermes_home* wins over the ``HERMES_HOME`` environment variable, which wins
    over ``~/.hermes``.  Re-running with unchanged files rewrites nothing;
    changed files are replaced atomically.
    """
    plugin_dir = _resolve_hermes_home(hermes_home) / "plugins" / PLUGIN_NAME
    plugin_dir.mkdir(parents=True, exist_ok=True)
    _write_if_changed(plugin_dir / "plugin.yaml", _plugin_manifest())
    _write_if_changed(plugin_dir / "__init__.py", _INIT_SOURCE)
    return plugin_dir


def main(argv: list[str] | None = None) -> int:
    """Console-script entry point."""
    parser = argparse.ArgumentParser(
        prog="hermes-zulip-install",
        description="Install the Hermes Zulip platform plugin into a Hermes home.",
    )
    parser.add_argument(
        "--hermes-home",
        type=Path,
        metavar="PATH",
        help="Hermes home directory (default: $HERMES_HOME or ~/.hermes)",
    )
    parser.add_argument(
        "--print-path",
        action="store_true",
        help="print only the installed plugin directory",
    )
    args = parser.parse_args(argv)

    destination = install_plugin(args.hermes_home)
    if args.print_path:
        print(destination)
    else:
        print(f"Installed the Zulip plugin in {destination}")
        print(f"Next: {NEXT_STEP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
