"""Connection lifecycle tests against the fake bridge."""

from __future__ import annotations

import asyncio

from gateway.platforms.base import _LOCKS
from hermes_zulip.adapter import ZulipAdapter


def _env(monkeypatch, site="https://chat.example.com", email="bot@example.com", key="secret-key"):
    monkeypatch.setenv("ZULIP_SITE", site)
    monkeypatch.setenv("ZULIP_EMAIL", email)
    monkeypatch.setenv("ZULIP_API_KEY", key)


def _lock_identities():
    return set(_LOCKS)


async def _wait_for(predicate, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


async def test_connect_success_captures_account_and_releases_on_disconnect(
        config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is True
    assert adapter.is_connected is True
    assert adapter._bot_email == "bot@example.com"
    assert adapter._bot_name == "Zulip Bot"
    assert adapter._poll_task is not None and not adapter._poll_task.done()
    assert len(fake_bridge.instances) == 1
    bridge = fake_bridge.instances[0]
    assert bridge.base_url == "https://chat.example.com"
    assert ("zulip", "https://chat.example.com:bot@example.com") in _lock_identities()

    await adapter.disconnect()
    assert adapter.is_connected is False
    assert adapter._bridge is None
    assert adapter._poll_task is None
    assert bridge.close_count == 1
    assert ("zulip", "https://chat.example.com:bot@example.com") not in _lock_identities()

    # Disconnect is idempotent: no second close, no exception, still disconnected.
    await adapter.disconnect()
    assert bridge.close_count == 1
    assert adapter.is_connected is False


async def test_connect_rejects_missing_configuration(config, fake_bridge):
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "config_missing"
    assert adapter.fatal_error_retryable is False
    assert fake_bridge.instances == []
    assert _lock_identities() == set()


async def test_connect_rejects_missing_api_key_without_libzul_fallback(
        config, fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    fake_bridge.config_status_result = {"base_url": True, "email": True, "api_key": False}
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "config_missing"
    assert "ZULIP_API_KEY" in (adapter.fatal_error_message or "")
    assert fake_bridge.instances == []


async def test_connect_accepts_api_key_from_libzul_config(
        config, fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    fake_bridge.config_status_result = {"base_url": True, "email": True, "api_key": True}
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is True
    try:
        assert fake_bridge.instances[0].api_key == ""
    finally:
        await adapter.disconnect()


async def test_connect_releases_lock_when_bridge_construction_fails(
        config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    fake_bridge.constructor_error = RuntimeError("bad api_key=super-secret")
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "connect_failed"
    assert adapter.fatal_error_retryable is True
    assert "secret-key" not in (adapter.fatal_error_message or "")
    assert "super-secret" not in (adapter.fatal_error_message or "")
    assert _lock_identities() == set()


async def test_connect_closes_bridge_and_releases_lock_when_whoami_fails(
        config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    fake_bridge.whoami_error = RuntimeError("whoami exploded")
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "connect_failed"
    assert fake_bridge.instances[0].close_count == 1
    assert _lock_identities() == set()


async def test_connect_returns_false_when_platform_lock_is_held(
        config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    first = ZulipAdapter(config)
    second = ZulipAdapter(config)
    assert await first.connect() is True
    try:
        assert await second.connect() is False
        assert second.fatal_error_code == "zulip_lock"
        assert second.fatal_error_retryable is True
        assert len(fake_bridge.instances) == 1
    finally:
        await first.disconnect()


async def test_poll_fatal_error_is_retryable_and_stops_loop(
        config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is True
    fake_bridge.instances[0].events.put(RuntimeError("queue broke"))
    try:
        assert await _wait_for(lambda: adapter._poll_task.done(), timeout=2.0)
        assert adapter.fatal_error_code == "poll_failed"
        assert adapter.fatal_error_retryable is True
        assert adapter.fatal_notifications == 1
    finally:
        await adapter.disconnect()


async def test_poll_loop_dispatches_events(config, fake_bridge, monkeypatch):
    _env(monkeypatch)
    adapter = ZulipAdapter(config)
    assert await adapter.connect() is True
    fake_bridge.instances[0].events.put({
        "id": 5, "kind": "stream", "stream_id": 12, "stream": "core", "topic": "ops",
        "sender_id": 7, "sender_name": "Ada", "sender_email": "ada@example.com",
        "plain_text": "hi", "content": "<p>hi</p>", "flags": ["mentioned"],
        "timestamp": 1760000000, "dm_participants": [],
    })
    try:
        assert await _wait_for(lambda: len(adapter.handled_events) == 1)
        assert adapter.handled_events[0].text == "hi"
    finally:
        await adapter.disconnect()
