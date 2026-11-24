"""Minimal ``gateway.platforms.helpers`` stub with faithful dedup semantics."""

from __future__ import annotations

import time
from typing import Any


class MessageDeduplicator:
    """TTL dedup cache; same public behaviour as the Hermes helper."""

    def __init__(self, max_size: int = 2000, ttl_seconds: float = 300):
        self._seen: dict[str, float] = {}
        self._max_size = max_size
        self._ttl = ttl_seconds

    def is_duplicate(self, msg_id: str) -> bool:
        if not msg_id:
            return False
        now = time.time()
        if msg_id in self._seen:
            if now - self._seen[msg_id] < self._ttl:
                return True
            del self._seen[msg_id]
        self._seen[msg_id] = now
        if len(self._seen) > self._max_size:
            cutoff = now - self._ttl
            self._seen = {k: v for k, v in self._seen.items() if v > cutoff}
        return False

    def contains(self, msg_id: str) -> bool:
        seen_at = self._seen.get(msg_id) if msg_id else None
        if seen_at is None:
            return False
        if time.time() - seen_at < self._ttl:
            return True
        del self._seen[msg_id]
        return False

    def discard(self, msg_id: str) -> None:
        self._seen.pop(msg_id, None)

    def clear(self) -> None:
        self._seen.clear()

    def absorb(self, other: "MessageDeduplicator") -> None:
        cutoff = time.time() - self._ttl
        self._seen.update({k: v for k, v in other._seen.items()
                           if v > cutoff and k not in self._seen})


def carry_inbound_dedup(predecessor: Any, adapter: Any) -> None:
    """Mirror the Hermes reconnect contract: absorb any MessageDeduplicator attribute."""
    if predecessor is None:
        return
    for name, previous in vars(predecessor).items():
        current = getattr(adapter, name, None)
        if (isinstance(previous, MessageDeduplicator)
                and isinstance(current, MessageDeduplicator)
                and current is not previous):
            current.absorb(previous)
