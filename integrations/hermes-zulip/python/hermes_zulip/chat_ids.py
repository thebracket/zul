"""Stable, URL-encoded chat ids for the Zulip Hermes adapter.

The adapter keys Hermes sessions on a compact string because Hermes builds a
session key per chat, not per (stream, topic) pair.  Stream ids carry the stream
id, name, and topic; direct-message ids carry the recipient ids and email
addresses.  Every variable-length component is percent-encoded so names and
topics may contain the delimiters (``:``, ``|``, ``@``, ``#``, ``%``, ``/``),
spaces, and non-ASCII text without ambiguity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Union
from urllib.parse import quote, unquote

_PREFIX = "zulip:v1:"
_STREAM_KIND = "stream"
_DM_KIND = "dm"


@dataclass(frozen=True)
class StreamTarget:
    """A decoded Zulip stream/topic destination."""

    stream_id: int
    stream: str
    topic: str


@dataclass(frozen=True)
class DMTarget:
    """A decoded Zulip direct-message destination."""

    recipients: tuple[tuple[int, str], ...]


ZulipTarget = Union[StreamTarget, DMTarget]


def _encode_component(value: object) -> str:
    """Percent-encode one variable-length chat-id component."""
    return quote(str(value), safe="")


def _coerce_recipient(recipient: tuple[int, str]) -> tuple[int, str]:
    """Normalize one ``(numeric id, email)`` recipient pair.

    A missing or malformed id becomes ``0``; send paths then fall back to the
    email address.
    """
    if not isinstance(recipient, tuple) or len(recipient) != 2:
        raise TypeError("a Zulip DM recipient must be an (id, email) tuple")
    raw_id, email = recipient
    try:
        recipient_id = int(raw_id or 0)
    except (TypeError, ValueError):
        recipient_id = 0
    return recipient_id, str(email or "")


def encode_stream(stream: str, topic: str, stream_id: int = 0) -> str:
    """Encode a stream/topic target; ``stream_id`` defaults to 0 (unknown)."""
    try:
        numeric_id = int(stream_id or 0)
    except (TypeError, ValueError):
        numeric_id = 0
    return ":".join((
        _PREFIX + _STREAM_KIND,
        str(numeric_id),
        _encode_component(stream),
        _encode_component(topic),
    ))


def encode_dm(recipients: Iterable[tuple[int, str]]) -> str:
    """Encode one or more DM recipients.

    Recipients are sorted by ``(id, email)`` so the same participant set always
    produces the same session key, regardless of Zulip's participant ordering.
    """
    normalized = sorted({_coerce_recipient(recipient) for recipient in recipients},
                        key=lambda item: (item[0], item[1]))
    if not normalized:
        raise ValueError("a direct-message target needs at least one recipient")
    payload = "|".join(f"{recipient_id}:{_encode_component(email)}"
                       for recipient_id, email in normalized)
    return f"{_PREFIX}{_DM_KIND}:{payload}"


def decode_chat_id(chat_id: str) -> ZulipTarget:
    """Decode a canonical chat id, raising ``ValueError`` when it is malformed."""
    if not isinstance(chat_id, str) or not chat_id.startswith(_PREFIX):
        raise ValueError("not a Zulip chat id")
    remainder = chat_id[len(_PREFIX):]
    kind, _, payload = remainder.partition(":")
    if kind == _STREAM_KIND:
        stream_id_raw, _, rest = payload.partition(":")
        stream_raw, separator, topic_raw = rest.partition(":")
        if not separator:
            raise ValueError("malformed Zulip stream chat id")
        try:
            stream_id = int(stream_id_raw)
        except ValueError as exc:
            raise ValueError("malformed Zulip stream id") from exc
        return StreamTarget(stream_id=stream_id, stream=unquote(stream_raw), topic=unquote(topic_raw))
    if kind == _DM_KIND:
        if not payload:
            raise ValueError("malformed Zulip DM chat id")
        recipients = []
        for entry in payload.split("|"):
            id_raw, separator, email_raw = entry.partition(":")
            if not separator:
                raise ValueError("malformed Zulip DM recipient")
            try:
                recipient_id = int(id_raw)
            except ValueError as exc:
                raise ValueError("malformed Zulip DM recipient id") from exc
            recipients.append((recipient_id, unquote(email_raw)))
        return DMTarget(recipients=tuple(recipients))
    raise ValueError("unknown Zulip chat id kind")


def parse_target_ref(ref: str) -> tuple[str, str | None] | None:
    """Parse native outbound target syntax into ``(chat_id, thread_id)``.

    Accepted forms:

    - a canonical chat id produced by :func:`encode_stream` / :func:`encode_dm`;
    - ``stream:<name>/<topic>`` or ``#<name>/<topic>`` (stream id unknown);
    - ``@<email>`` (direct message by email).
    """
    if not isinstance(ref, str):
        return None
    candidate = ref.strip()
    if not candidate:
        return None
    if candidate.startswith(_PREFIX):
        try:
            target = decode_chat_id(candidate)
        except ValueError:
            return None
        return candidate, target.topic if isinstance(target, StreamTarget) else None

    stream_ref = None
    if candidate.startswith("#"):
        stream_ref = candidate[1:]
    elif candidate.lower().startswith("stream:"):
        stream_ref = candidate[len("stream:"):]
    if stream_ref is not None:
        stream, separator, topic = stream_ref.partition("/")
        if not separator or not stream.strip() or not topic.strip():
            return None
        return encode_stream(stream.strip(), topic.strip()), topic.strip()

    if candidate.startswith("@"):
        email = candidate[1:].strip()
        if not email or any(character.isspace() for character in email):
            return None
        return encode_dm([(0, email)]), None
    return None


def validate_target_ref(ref: str) -> bool:
    """Return whether *ref* is a target this adapter can send to."""
    return parse_target_ref(ref) is not None
