"""Public surface of the compiled ``zul_bridge`` extension.

Maturin's mixed Python/Rust layout installs the extension inside this package
as ``zul_bridge.zul_bridge``; re-export its names so callers keep using
``import zul_bridge``.

A source checkout run with ``PYTHONPATH=python`` shadows the installed wheel's
package with this file.  In that case load the compiled submodule from the
installed distribution, so the plugin still sees the extension.
"""

from __future__ import annotations

import importlib.machinery
import importlib.util
import sys
from pathlib import Path

try:
    from .zul_bridge import Bridge, __version__, config_status
except ImportError:
    _here = Path(__file__).resolve().parent
    _loaded = None
    for _entry in sys.path:
        _candidate = Path(_entry or ".") / "zul_bridge"
        try:
            if _candidate.resolve() == _here:
                continue
        except OSError:
            continue
        for _suffix in importlib.machinery.EXTENSION_SUFFIXES:
            _path = _candidate / f"zul_bridge{_suffix}"
            if not _path.is_file():
                continue
            _spec = importlib.util.spec_from_file_location("zul_bridge.zul_bridge", _path)
            if _spec is None or _spec.loader is None:
                continue
            _module = importlib.util.module_from_spec(_spec)
            sys.modules["zul_bridge.zul_bridge"] = _module
            _spec.loader.exec_module(_module)
            _loaded = _module
            break
        if _loaded is not None:
            break
    if _loaded is None:
        raise
    Bridge = _loaded.Bridge
    config_status = _loaded.config_status
    __version__ = _loaded.__version__

__all__ = ["Bridge", "config_status", "__version__"]
