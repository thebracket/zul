"""Chat-id codec tests: canonical round-trips and target parsing."""

from __future__ import annotations

import pytest
from hermes_zulip.chat_ids import (
    DMTarget,
    StreamTarget,
    decode_chat_id,
    encode_dm,
    encode_stream,
    parse_target_ref,
    validate_target_ref,
)


def test_stream_round_trip_plain():
    chat_id = encode_stream("core", "ops", 12)
    assert chat_id == "zulip:v1:stream:12:core:ops"
    assert decode_chat_id(chat_id) == StreamTarget(stream_id=12, stream="core", topic="ops")


@pytest.mark.parametrize("stream,topic", [
    ("a/b:c|d%e#f@g h", "t/1:2|3%4#5@6 ü"),
    ("#general", "topic with spaces"),
    ("λambda", "日本語のトピック"),
    ("100%", "a|b"),
])
def test_stream_round_trip_special_characters(stream, topic):
    chat_id = encode_stream(stream, topic, 42)
    assert decode_chat_id(chat_id) == StreamTarget(stream_id=42, stream=stream, topic=topic)


def test_stream_unknown_id_defaults_to_zero():
    assert encode_stream("core", "ops") == "zulip:v1:stream:0:core:ops"
    assert decode_chat_id(encode_stream("core", "ops")).stream_id == 0


def test_dm_round_trip_ids_and_emails():
    chat_id = encode_dm([(8, "b@example.com"), (7, "a@example.com")])
    assert chat_id.startswith("zulip:v1:dm:")
    assert "%40" in chat_id
    assert decode_chat_id(chat_id) == DMTarget(recipients=((7, "a@example.com"), (8, "b@example.com")))


def test_dm_is_order_independent():
    first = encode_dm([(8, "b@example.com"), (7, "a@example.com")])
    second = encode_dm([(7, "a@example.com"), (8, "b@example.com")])
    assert first == second


@pytest.mark.parametrize("email", [
    "a:b|c%d#e@f g",
    "λ@example.com",
    "user+tag@example.co.uk",
])
def test_dm_round_trip_special_email(email):
    chat_id = encode_dm([(3, email)])
    assert decode_chat_id(chat_id) == DMTarget(recipients=((3, email),))


def test_dm_accepts_mapping_and_object_recipients():
    class Participant:
        def __init__(self, participant_id, email):
            self.id = participant_id
            self.email = email

    expected = ((4, "d@example.com"), (5, "e@example.com"))
    assert decode_chat_id(encode_dm([Participant(4, "d@example.com"),
                                     {"id": 5, "email": "e@example.com"}])) == DMTarget(expected)


def test_encode_dm_rejects_empty_recipients():
    with pytest.raises(ValueError):
        encode_dm([])


@pytest.mark.parametrize("chat_id", [
    "",
    "zulip:v1:",
    "zulip:v1:group:1:x",
    "zulip:v1:stream:notanint:core:ops",
    "zulip:v1:stream:1:core",
    "zulip:v1:dm:",
    "zulip:v1:dm:7",
    "zulip:v1:dm:x:a%40example.com",
    "telegram:1",
])
def test_decode_invalid_chat_ids(chat_id):
    with pytest.raises(ValueError):
        decode_chat_id(chat_id)


def test_parse_canonical_stream_ref():
    ref = encode_stream("core", "ops", 12)
    assert parse_target_ref(ref) == (ref, "ops")


def test_parse_canonical_dm_ref():
    ref = encode_dm([(7, "a@example.com")])
    assert parse_target_ref(ref) == (ref, None)


@pytest.mark.parametrize("ref,stream,topic", [
    ("stream:core/ops", "core", "ops"),
    ("#core/ops", "core", "ops"),
    ("stream:#general/topic with spaces", "#general", "topic with spaces"),
])
def test_parse_stream_target_forms(ref, stream, topic):
    chat_id, thread_id = parse_target_ref(ref)
    assert decode_chat_id(chat_id) == StreamTarget(stream_id=0, stream=stream, topic=topic)
    assert thread_id == topic


def test_parse_email_target():
    chat_id, thread_id = parse_target_ref("@ada@example.com")
    assert thread_id is None
    assert decode_chat_id(chat_id) == DMTarget(recipients=((0, "ada@example.com"),))


@pytest.mark.parametrize("ref", [
    "", "   ", "core", "stream:core", "#core", "@", "@not an email",
    "zulip:v1:unknown", "zulip:v1:dm:",
])
def test_parse_invalid_targets(ref):
    assert parse_target_ref(ref) is None
    assert validate_target_ref(ref) is False


def test_validate_accepts_supported_forms():
    for ref in ("stream:core/ops", "#core/ops", "@a@example.com",
                encode_stream("core", "ops", 1), encode_dm([(2, "b@example.com")])):
        assert validate_target_ref(ref) is True
