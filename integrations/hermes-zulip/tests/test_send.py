"""Outbound send, chunking, typing, and chat-info tests."""

from __future__ import annotations

from hermes_zulip.adapter import ZulipAdapter
from hermes_zulip.chat_ids import encode_dm, encode_stream


def _connected(config, fake_bridge, monkeypatch, max_length=10000):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret-key")
    adapter = ZulipAdapter(config)
    adapter.max_message_length = max_length
    adapter._bridge = fake_bridge.Bridge(
        base_url="https://chat.example.com", email="bot@example.com", api_key="secret-key")
    adapter._running = True
    return adapter


async def test_send_stream_single_chunk(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    result = await adapter.send(encode_stream("core", "ops", 12), "hello")
    assert result.success is True
    assert result.message_id == "1000"
    assert result.continuation_message_ids == ()
    assert bridge.sent == [("stream", "core", "ops", "hello", 1000)]


async def test_send_sets_splits_long_messages(config):
    assert ZulipAdapter.splits_long_messages is True


async def test_send_stream_chunks_long_content(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch, max_length=10)
    bridge = fake_bridge.instances[-1]
    content = "first part second part third"
    result = await adapter.send(encode_stream("core", "ops", 12), content)
    assert result.success is True
    assert len(bridge.sent) > 1
    assert all(len(entry[3]) <= 10 for entry in bridge.sent)
    assert "".join(entry[3] for entry in bridge.sent).replace(" ", "") == content.replace(" ", "")
    ids = [str(entry[4]) for entry in bridge.sent]
    assert result.message_id == ids[-1]
    assert result.continuation_message_ids == tuple(ids[:-1])


async def test_send_accepts_target_ref_as_home_channel(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    result = await adapter.send("stream:core/ops", "hello")
    assert result.success is True
    assert bridge.sent == [("stream", "core", "ops", "hello", 1000)]


async def test_send_dm_by_numeric_ids(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    chat_id = encode_dm([(7, "a@example.com"), (8, "b@example.com")])
    result = await adapter.send(chat_id, "hi")
    assert result.success is True
    assert bridge.sent[0][0] == "direct_ids"
    assert bridge.sent[0][1] == (7, 8)


async def test_send_dm_chunks_long_content(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch, max_length=10)
    bridge = fake_bridge.instances[-1]
    chat_id = encode_dm([(7, "a@example.com")])
    content = "first part second part third"
    result = await adapter.send(chat_id, content)
    assert result.success is True
    assert len(bridge.sent) > 1
    assert all(entry[0] == "direct_ids" for entry in bridge.sent)
    assert all(len(entry[2]) <= 10 for entry in bridge.sent)
    ids = [str(entry[3]) for entry in bridge.sent]
    assert result.message_id == ids[-1]
    assert result.continuation_message_ids == tuple(ids[:-1])


async def test_send_dm_falls_back_to_emails_when_ids_unknown(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    chat_id = encode_dm([(0, "a@example.com")])
    result = await adapter.send(chat_id, "hi")
    assert result.success is True
    assert bridge.sent[0][0] == "direct_emails"
    assert bridge.sent[0][1] == ("a@example.com",)


async def test_send_converts_exception_and_redacts_secret(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    bridge.send_error = RuntimeError("send failed api_key=secret-key")
    result = await adapter.send(encode_stream("core", "ops", 12), "hello")
    assert result.success is False
    assert "secret-key" not in (result.error or "")
    assert "redacted" in (result.error or "")


async def test_send_marks_partial_delivery_when_a_later_chunk_fails(
        config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch, max_length=10)
    bridge = fake_bridge.instances[-1]
    bridge.send_failures = [None, RuntimeError("chunk two is down")]
    result = await adapter.send(encode_stream("core", "ops", 12), "first part second part third")
    assert result.success is False
    assert len(bridge.sent) == 1
    assert result.raw_response == {
        "delivered_message_ids": [str(bridge.sent[0][4])],
        "partial_overflow": True,
    }


async def test_send_before_connect_fails(config):
    adapter = ZulipAdapter(config)
    result = await adapter.send(encode_stream("core", "ops", 12), "hello")
    assert result.success is False
    assert "not connected" in (result.error or "").lower()


async def test_send_rejects_invalid_chat_id(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    result = await adapter.send("telegram:1", "hello")
    assert result.success is False
    assert "chat_id" in (result.error or "").lower()


async def test_typing_stream_uses_the_canonical_stream_id(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    await adapter.send_typing(encode_stream("core", "ops", 12))
    await adapter.stop_typing(encode_stream("core", "ops", 12))
    assert bridge.typing == [("stream", 12, "ops", "start"), ("stream", 12, "ops", "stop")]


async def test_typing_unknown_stream_id_is_noop(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    await adapter.send_typing(encode_stream("unknown", "topic", 0))
    await adapter.stop_typing(encode_stream("unknown", "topic", 0))
    assert bridge.typing == []


async def test_typing_dm_uses_the_canonical_recipient_ids(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    chat_id = encode_dm([(7, "a@example.com"), (8, "b@example.com")])
    await adapter.send_typing(chat_id)
    await adapter.stop_typing(chat_id)
    assert bridge.typing == [("direct", (7, 8), "start"), ("direct", (7, 8), "stop")]


async def test_typing_dm_without_numeric_ids_is_noop(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    await adapter.send_typing(encode_dm([(0, "a@example.com")]))
    assert bridge.typing == []


async def test_typing_failure_is_swallowed(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    bridge = fake_bridge.instances[-1]
    bridge.typing_error = RuntimeError("typing blocked")
    await adapter.send_typing(encode_stream("core", "ops", 12))
    await adapter.stop_typing(encode_stream("core", "ops", 12))


async def test_get_chat_info_stream_and_dm(config):
    adapter = ZulipAdapter(config)
    info = await adapter.get_chat_info(encode_stream("core", "ops", 12))
    assert info == {"name": "#core > ops", "type": "group"}
    dm = await adapter.get_chat_info(encode_dm([(7, "a@example.com")]))
    assert dm == {"name": "a@example.com", "type": "dm"}
    assert await adapter.get_chat_info("bogus") == {"name": "bogus", "type": "dm"}


async def test_decode_round_trip_through_send(config, fake_bridge, monkeypatch):
    adapter = _connected(config, fake_bridge, monkeypatch)
    chat_id = encode_stream("a/b", "t:1", 3)
    await adapter.send(chat_id, "hello")
    sent = fake_bridge.instances[-1].sent[0]
    assert sent[:3] == ("stream", "a/b", "t:1")
