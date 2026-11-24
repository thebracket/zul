"""Inbound mapping: streams, topics, mentions, DMs, dedup, and self-filter."""

from __future__ import annotations

import pytest
from fake_bridge import make_dm_event, make_stream_event
from gateway.platforms.helpers import carry_inbound_dedup
from hermes_zulip.adapter import ZulipAdapter
from hermes_zulip.chat_ids import DMTarget, StreamTarget, decode_chat_id


def _adapter(config, **kwargs):
    adapter = ZulipAdapter(config)
    adapter._bot_email = "bot@example.com"
    adapter._bot_name = "Zulip Bot"
    for key, value in kwargs.items():
        setattr(adapter, key, value)
    return adapter


async def _handle(adapter, event):
    before = len(adapter.handled_events)
    await adapter._handle_event(event)
    if len(adapter.handled_events) == before:
        return None
    return adapter.handled_events[-1]


async def test_stream_mapping_and_source_fields(config):
    adapter = _adapter(config)
    event = make_stream_event(plain_text="hello there")
    message = await _handle(adapter, event)
    assert message is not None
    assert message.text == "hello there"
    assert message.message_id == "1"
    assert message.raw_message is event
    assert message.source.platform.value == "zulip"
    assert message.source.chat_type == "group"
    assert decode_chat_id(message.source.chat_id) == StreamTarget(12, "core", "ops")
    assert message.source.chat_name == "#core > ops"
    assert message.source.thread_id == "ops"
    assert message.source.chat_topic == "ops"
    assert message.source.user_id == "ada@example.com"
    assert message.source.user_id_alt == "7"
    assert message.source.user_name == "Ada Lovelace"
    assert message.source.message_id == "1"
    assert message.timestamp.timestamp() == pytest.approx(1760000000)


async def test_stream_prefers_plain_text_over_rendered_content(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_stream_event(
        plain_text="plain", content="<p>rendered <b>html</b></p>",
        flags=["mentioned"], id=11))
    assert message.text == "plain"


async def test_stream_falls_back_to_content_when_plain_text_missing(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_stream_event(
        plain_text="", content="fallback text", flags=["mentioned"], id=12))
    assert message.text == "fallback text"


async def test_self_message_is_ignored(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_stream_event(sender_email="BOT@example.com")) is None
    assert adapter.handled_events == []


async def test_dedup_by_zulip_message_id(config):
    adapter = _adapter(config)
    event = make_stream_event(id=77)
    assert await _handle(adapter, event) is not None
    assert await _handle(adapter, event) is None
    assert len(adapter.handled_events) == 1


async def test_dedup_survives_reconnect_carry(config):
    old = _adapter(config)
    assert await _handle(old, make_stream_event(id=88)) is not None
    new = _adapter(config)
    carry_inbound_dedup(old, new)
    assert await _handle(new, make_stream_event(id=88)) is None
    assert await _handle(new, make_stream_event(id=89)) is not None


async def test_require_mention_default_true_drops_unmentioned(config):
    adapter = _adapter(config)
    event = make_stream_event(flags=[])
    assert await _handle(adapter, event) is None
    assert adapter.handled_events == []


async def test_require_mention_true_accepts_mentioned_flag(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_stream_event(flags=["mentioned"])) is not None


async def test_require_mention_true_accepts_wildcard_mention(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_stream_event(
        id=13, flags=["wildcard_mentioned"], plain_text="@all standup")) is not None


async def test_require_mention_true_accepts_rendered_token_and_strips_it(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_stream_event(
        id=14, flags=[], plain_text="@**Zulip Bot** please summarize"))
    assert message is not None
    assert message.text == "please summarize"


async def test_silent_mention_token_is_stripped(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_stream_event(
        id=15, flags=[], plain_text="@_**Zulip Bot** quiet ping"))
    assert message is not None
    assert message.text == "quiet ping"


async def test_mention_only_message_is_dropped_after_stripping(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_stream_event(
        id=16, flags=["mentioned"], plain_text="@**Zulip Bot**")) is None


async def test_require_mention_false_accepts_plain_stream_message(config):
    adapter = _adapter(config, require_mention=False)
    message = await _handle(adapter, make_stream_event(id=17, flags=[], plain_text="no mention"))
    assert message is not None
    assert message.text == "no mention"


async def test_allowed_streams_filter_is_case_insensitive(config):
    adapter = _adapter(config, allowed_streams=frozenset({"core"}))
    assert await _handle(adapter, make_stream_event(id=18, stream="Core", flags=["mentioned"])) is not None
    assert await _handle(adapter, make_stream_event(id=19, stream="random", flags=["mentioned"])) is None


async def test_empty_allowed_streams_accepts_all(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_stream_event(id=20, stream="anything", flags=["mentioned"])) is not None


async def test_dm_mapping_filters_bot_and_builds_source(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_dm_event())
    assert message is not None
    assert message.text == "direct hello"
    assert message.source.chat_type == "dm"
    assert message.source.thread_id is None
    assert message.source.chat_name == "Ada Lovelace"
    target = decode_chat_id(message.source.chat_id)
    assert isinstance(target, DMTarget)
    assert target.recipients == ((7, "ada@example.com"),)
    assert message.source.user_id == "ada@example.com"


async def test_group_dm_keeps_other_recipients(config):
    adapter = _adapter(config)
    event = make_dm_event(id=21, dm_participants=[
        {"id": 7, "name": "Ada", "email": "ada@example.com"},
        {"id": 8, "name": "Grace", "email": "grace@example.com"},
        {"id": 1, "name": "Zulip Bot", "email": "bot@example.com"},
    ])
    message = await _handle(adapter, event)
    assert message.source.chat_name == "Ada, Grace"
    target = decode_chat_id(message.source.chat_id)
    assert target.recipients == ((7, "ada@example.com"), (8, "grace@example.com"))


async def test_bot_only_dm_is_dropped(config):
    adapter = _adapter(config)
    event = make_dm_event(id=22, dm_participants=[
        {"id": 1, "name": "Zulip Bot", "email": "bot@example.com"},
    ])
    assert await _handle(adapter, event) is None


async def test_empty_dm_text_is_dropped(config):
    adapter = _adapter(config)
    assert await _handle(adapter, make_dm_event(id=23, plain_text="", content="")) is None


async def test_user_id_falls_back_to_numeric_sender_id(config):
    adapter = _adapter(config)
    message = await _handle(adapter, make_stream_event(
        id=24, sender_email="", flags=["mentioned"]))
    assert message.source.user_id == "7"
    assert message.source.user_id_alt == "7"


async def test_invalid_kind_is_ignored(config):
    adapter = _adapter(config)
    assert await _handle(adapter, {"id": 25, "kind": "reaction"}) is None
    assert await _handle(adapter, {"kind": "stream"}) is None


async def test_inbound_caches_stream_id_for_typing(config):
    adapter = _adapter(config)
    await _handle(adapter, make_stream_event(id=26, stream="core", topic="ops", stream_id=12,
                                             flags=["mentioned"]))
    assert adapter._stream_ids[("core", "ops")] == 12


async def test_inbound_caches_dm_typing_recipients(config):
    adapter = _adapter(config)
    await _handle(adapter, make_dm_event(id=27))
    chat_id = adapter.handled_events[-1].source.chat_id
    assert adapter._dm_typing_ids[chat_id] == (7,)
