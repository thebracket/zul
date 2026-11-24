"""Minimal ``gateway.platforms._shared`` stub mirroring the public helper contracts."""

from __future__ import annotations

import contextlib
import os
from typing import Any, Callable, Iterable, Optional


def get_scoped_secret(name: str, default: Any = None, *, external_fallback: bool = False) -> Any:
    value = os.environ.get(name)
    return value if value is not None else default


def extra_or_secret(extra: Optional[dict], key: str, env: str, default: Any = "",
                    *, blank_is_unset: bool = True) -> Any:
    env_value = os.environ.get(env)
    if env_value is not None and str(env_value).strip():
        return env_value
    value = (extra or {}).get(key)
    if value is None or (blank_is_unset and isinstance(value, str) and not value.strip()):
        return default
    return value


def seed_extra_from_env(spec: Iterable[tuple[str, str, Callable[[str], Any] | None]], *,
                        home_env: str | None = None, home_default: str = "") -> dict:
    seed: dict[str, Any] = {}
    for env, key, conv in spec:
        raw = str(get_scoped_secret(env, "") or "").strip()
        if not raw:
            continue
        with contextlib.suppress(ValueError):
            seed[key] = conv(raw) if conv else raw
    if home_env:
        home = str(get_scoped_secret(home_env, "") or "").strip() or home_default
        if home:
            seed["home_channel"] = {
                "chat_id": home,
                "name": get_scoped_secret(f"{home_env}_NAME", "Home"),
            }
    return seed


def send_error(message: Any, *, nothing_sent: bool = False) -> dict:
    return {"error": str(message), **({"retryable": True} if nothing_sent else {})}


DELIVERED, PARTIAL, UNCERTAIN, NONE = "delivered", "partial", "uncertain", "none"


def delivery_state(result: Any) -> str:
    """Mirror of the real helper: an explicit ``delivery`` wins, then ``retryable``."""
    if not isinstance(result, dict) or not result.get("error"):
        return DELIVERED
    if result.get("delivery") in (PARTIAL, UNCERTAIN, NONE):
        return result["delivery"]
    return NONE if result.get("retryable") is True else UNCERTAIN
