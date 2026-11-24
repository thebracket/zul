"""Minimal ``gateway.platforms.base`` stub for adapter tests.

Implements the public seams the adapter relies on: lifecycle flags, fatal-error
reporting, the scoped platform lock, ``build_source``, message chunking, and
``handle_message`` capture.  It intentionally does not reproduce the full
Hermes runner.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..config import Platform
from .event import MessageEvent, SessionSource

# Process-wide, scope/identity-keyed lock registry (the real helper is
# machine-global through the status subsystem).
_LOCKS: dict[tuple[str, str], int] = {}


@dataclass
class SendResult:
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None
    raw_response: Any = None
    retryable: bool = False
    retry_after: Optional[float] = None
    continuation_message_ids: tuple = ()
    error_kind: Optional[str] = None


class BasePlatformAdapter(ABC):
    def __init__(self, config, platform: Platform):
        self.config = config
        self.platform = platform
        self._message_handler: Optional[Callable] = None
        self._running = False
        self._fatal_error_code: Optional[str] = None
        self._fatal_error_message: Optional[str] = None
        self._fatal_error_retryable = True
        self._platform_lock_scope: Optional[str] = None
        self._platform_lock_identity: Optional[str] = None
        self.handled_events: list[MessageEvent] = []
        self.fatal_notifications = 0
        self.max_message_length = 4096

    @property
    def name(self) -> str:
        return self.platform.value.title()

    @property
    def is_connected(self) -> bool:
        return self._running

    @property
    def has_fatal_error(self) -> bool:
        return self._fatal_error_message is not None

    @property
    def fatal_error_code(self) -> Optional[str]:
        return self._fatal_error_code

    @property
    def fatal_error_message(self) -> Optional[str]:
        return self._fatal_error_message

    @property
    def fatal_error_retryable(self) -> bool:
        return self._fatal_error_retryable

    def _mark_connected(self, *, listener_base: Optional[str] = None) -> None:
        self._running = True
        self._fatal_error_code = self._fatal_error_message = None
        self._fatal_error_retryable = True

    def _mark_disconnected(self) -> None:
        self._running = False

    def _set_fatal_error(self, code: str, message: str, *, retryable: bool) -> None:
        self._running = False
        self._fatal_error_code = code
        self._fatal_error_message = message
        self._fatal_error_retryable = retryable

    async def _notify_fatal_error(self) -> None:
        self.fatal_notifications += 1

    def _acquire_platform_lock(self, scope: str, identity: str, resource_desc: str) -> bool:
        self._platform_lock_scope, self._platform_lock_identity = scope, identity
        key = (scope, identity)
        if key in _LOCKS:
            self._set_fatal_error(f"{scope}_lock", f"{resource_desc} already in use", retryable=True)
            return False
        _LOCKS[key] = id(self)
        return True

    def _release_platform_lock(self) -> None:
        identity = self._platform_lock_identity
        if not identity:
            return
        _LOCKS.pop((self._platform_lock_scope, identity), None)
        self._platform_lock_identity = None

    def build_source(self, chat_id: str, chat_name: Optional[str] = None, chat_type: str = "dm",
                     user_id: Optional[str] = None, user_name: Optional[str] = None,
                     thread_id: Optional[str] = None, chat_topic: Optional[str] = None,
                     user_id_alt: Optional[str] = None, chat_id_alt: Optional[str] = None,
                     is_bot: bool = False, scope_id: Optional[str] = None,
                     guild_id: Optional[str] = None, parent_chat_id: Optional[str] = None,
                     message_id: Optional[str] = None, **_: Any) -> SessionSource:
        return SessionSource(
            platform=self.platform,
            chat_id=str(chat_id),
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=None if user_id in (None, "") else str(user_id),
            user_name=user_name,
            thread_id=thread_id or None,
            chat_topic=(chat_topic or "").strip() or None,
            user_id_alt=user_id_alt,
            chat_id_alt=chat_id_alt,
            is_bot=is_bot,
            scope_id=scope_id,
            guild_id=guild_id,
            parent_chat_id=parent_chat_id,
            message_id=message_id,
        )

    @staticmethod
    def truncate_message(content: str, max_length: int = 4096,
                         len_fn: Optional[Callable[[str], int]] = None) -> list[str]:
        length = len_fn or len
        if length(content) <= max_length:
            return [content]
        chunks: list[str] = []
        remaining = content
        while length(remaining) > max_length:
            split_at = remaining.rfind("\n", 0, max_length)
            if split_at < max_length // 2:
                split_at = remaining.rfind(" ", 0, max_length)
            if split_at < 1:
                split_at = max_length
            chunks.append(remaining[:split_at])
            remaining = remaining[split_at:].lstrip()
        chunks.append(remaining)
        return chunks

    async def handle_message(self, event: MessageEvent) -> None:
        self.handled_events.append(event)
        if self._message_handler is not None:
            result = self._message_handler(event)
            if asyncio.iscoroutine(result):
                await result

    @abstractmethod
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict] = None) -> SendResult:
        raise NotImplementedError

    @abstractmethod
    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        raise NotImplementedError
