"""Fake ``zul_bridge`` extension used by the adapter tests.

The fake mirrors the real extension's public surface and records calls so the
adapter's blocking calls can be asserted without Zulip or a wheel install.
"""

from __future__ import annotations

import queue
import threading
from typing import Any


class FakeBridge:
    def __init__(self, base_url: str = "", email: str = "", api_key: str = "",
                 *, event_types: Any = None, narrow: Any = None,
                 all_public_streams: bool = False, queue_timeout_secs: Any = None):
        self.base_url = base_url
        self.email = email
        self.api_key = api_key
        self.queue_timeout_secs = queue_timeout_secs
        self.sent: list[tuple] = []
        self.typing: list[tuple] = []
        self.close_count = 0
        self.events: queue.Queue[Any] = queue.Queue()
        self.next_message_id = 1000
        self.whoami_result = {"email": email, "full_name": "Zulip Bot"}
        self.whoami_error: BaseException | None = None
        self.whoami_started = threading.Event()
        self.whoami_gate: threading.Event | None = None
        self.send_error: BaseException | None = None
        self.send_failures: list[BaseException | None] = []
        self.typing_error: BaseException | None = None
        self._lock = threading.Lock()

    def whoami(self) -> dict[str, str]:
        self.whoami_started.set()
        if self.whoami_gate is not None:
            self.whoami_gate.wait(5)
        if self.whoami_error is not None:
            raise self.whoami_error
        return dict(self.whoami_result)

    def _next_send_failure(self) -> None:
        failure = self.send_failures.pop(0) if self.send_failures else None
        if failure is not None:
            raise failure
        if self.send_error is not None:
            raise self.send_error

    def _next_id(self) -> int:
        with self._lock:
            message_id = self.next_message_id
            self.next_message_id += 1
            return message_id

    def send_stream(self, stream: str, topic: str, content: str) -> int:
        self._next_send_failure()
        message_id = self._next_id()
        self.sent.append(("stream", stream, topic, content, message_id))
        return message_id

    def send_direct_ids(self, recipients: list[int], content: str) -> int:
        self._next_send_failure()
        message_id = self._next_id()
        self.sent.append(("direct_ids", tuple(recipients), content, message_id))
        return message_id

    def send_direct_emails(self, recipients: list[str], content: str) -> int:
        self._next_send_failure()
        message_id = self._next_id()
        self.sent.append(("direct_emails", tuple(recipients), content, message_id))
        return message_id

    def typing_stream(self, stream_id: int, topic: str, op: str) -> None:
        if self.typing_error is not None:
            raise self.typing_error
        self.typing.append(("stream", stream_id, topic, op))

    def typing_direct(self, recipients: list[int], op: str) -> None:
        if self.typing_error is not None:
            raise self.typing_error
        self.typing.append(("direct", tuple(recipients), op))

    def next_event(self, timeout_ms: int) -> dict | None:
        # Keep test teardown fast; a real long-poll is exercised by the bridge tests.
        timeout = min(max(timeout_ms, 1) / 1000.0, 0.02)
        try:
            item = self.events.get(timeout=timeout)
        except queue.Empty:
            return None
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self) -> None:
        self.close_count += 1


class FakeBridgeModule:
    """Stand-in for the imported ``zul_bridge`` module."""

    def __init__(self) -> None:
        self.instances: list[FakeBridge] = []
        self.constructor_error: BaseException | None = None
        self.whoami_error: BaseException | None = None
        self.whoami_result: dict[str, str] | None = None
        self.whoami_started = threading.Event()
        self.whoami_gate: threading.Event | None = None
        self.send_error: BaseException | None = None
        self.send_failures: list[BaseException | None] = []
        self.typing_error: BaseException | None = None
        self.config_status_result: dict[str, bool] = {
            "base_url": True, "email": True, "api_key": True,
        }
        self.config_status_error: BaseException | None = None
        self.config_status_calls = 0

    def Bridge(self, base_url: str = "", email: str = "", api_key: str = "", **kwargs: Any) -> FakeBridge:
        if self.constructor_error is not None:
            raise self.constructor_error
        bridge = FakeBridge(base_url=base_url, email=email, api_key=api_key, **kwargs)
        if self.whoami_error is not None:
            bridge.whoami_error = self.whoami_error
        if self.whoami_result is not None:
            bridge.whoami_result = dict(self.whoami_result)
        bridge.whoami_started = self.whoami_started
        bridge.whoami_gate = self.whoami_gate
        if self.send_error is not None:
            bridge.send_error = self.send_error
        bridge.send_failures = self.send_failures
        if self.typing_error is not None:
            bridge.typing_error = self.typing_error
        self.instances.append(bridge)
        return bridge

    def config_status(self) -> dict[str, bool]:
        self.config_status_calls += 1
        if self.config_status_error is not None:
            raise self.config_status_error
        return dict(self.config_status_result)


def make_stream_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "id": 1,
        "kind": "stream",
        "stream_id": 12,
        "stream": "core",
        "topic": "ops",
        "sender_id": 7,
        "sender_name": "Ada Lovelace",
        "sender_email": "ada@example.com",
        "content": "<p>hello</p>",
        "plain_text": "hello",
        "flags": ["mentioned"],
        "timestamp": 1760000000,
        "permalink": "https://chat.example.com/#narrow/stream/12/topic/ops",
        "dm_participants": [],
    }
    event.update(overrides)
    return event


def make_dm_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "id": 2,
        "kind": "dm",
        "stream_id": None,
        "stream": None,
        "topic": None,
        "sender_id": 7,
        "sender_name": "Ada Lovelace",
        "sender_email": "ada@example.com",
        "content": "<p>direct hello</p>",
        "plain_text": "direct hello",
        "flags": [],
        "timestamp": 1760000100,
        "permalink": None,
        "dm_participants": [
            {"id": 7, "name": "Ada Lovelace", "email": "ada@example.com"},
            {"id": 1, "name": "Zulip Bot", "email": "bot@example.com"},
        ],
    }
    event.update(overrides)
    return event
