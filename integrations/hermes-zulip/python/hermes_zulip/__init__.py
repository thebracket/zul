"""Hermes platform plugin for Zulip.

The adapter (and through it ``gateway``) is imported lazily so packaging and
pure helpers such as ``hermes_zulip.install`` and ``hermes_zulip.chat_ids``
work outside the Hermes interpreter. Plugin loaders access ``register`` as a
module attribute, which resolves through ``__getattr__``.
"""

from __future__ import annotations

from typing import Any

__all__ = ["register"]


def __getattr__(name: str) -> Any:
    if name == "register":
        from .adapter import register

        return register
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
