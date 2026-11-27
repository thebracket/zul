"""Hermes platform adapter for Zulip, backed by the ``zul_bridge`` extension.

The adapter is deliberately thin: ``zul_bridge`` owns the Zulip HTTP client and
event queue, Hermes owns sessions and authorization, and this module maps
streams/topics and direct messages onto Hermes ``SessionSource`` values and
sends.  ``zul_bridge`` is imported defensively so plugin discovery, ``hermes
plugins list``, and ``hermes config`` keep working before the wheel is built or
installed.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import re
from typing import Any

try:  # The extension is an optional runtime dependency; discovery must not fail.
    import zul_bridge
except Exception:  # pragma: no cover - exercised by tests without the wheel
    zul_bridge = None

from gateway.config import Platform
from gateway.platforms._shared import (
    extra_or_secret,
    get_scoped_secret,
    seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator

from .chat_ids import (
    DMTarget,
    StreamTarget,
    decode_chat_id,
    encode_dm,
    encode_stream,
    parse_target_ref,
    validate_target_ref,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_MESSAGE_LENGTH = 10000
INBOUND_DEDUP_TTL_SECONDS = 600
_BRIDGE_POLL_TIMEOUT_MS = 1000

_TRUTHY = frozenset({"1", "true", "yes", "on"})
_SECRET_PATTERN = re.compile(
    r"(?i)\b(api[_-]?key|token|password|secret)\b(\s*[=:]\s*)\S+")
_URL_USERINFO_PATTERN = re.compile(r"(https?://)[^/@\s]+@")


def _as_bool(value: Any, default: bool = False) -> bool:
    """Coerce env/YAML booleans without treating the string ``"false"`` as true."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in _TRUTHY


def _as_int(value: Any, default: int | None = None) -> int | None:
    """Coerce a positive integer setting; an invalid value falls back to *default*."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _csv(value: Any) -> tuple[str, ...]:
    """Parse a comma-separated or list-shaped setting into stripped non-empty values."""
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        items = value
    else:
        items = str(value).split(",")
    return tuple(str(item).strip() for item in items if str(item).strip())


def _redact(text: Any, secret: str | None = None) -> str:
    """Remove credentials from an error string before it reaches a log or result."""
    cleaned = str(text or "")
    if secret:
        cleaned = cleaned.replace(secret, "<redacted>")
    cleaned = _SECRET_PATTERN.sub(r"\1\2<redacted>", cleaned)
    return _URL_USERINFO_PATTERN.sub(r"\1<redacted>@", cleaned)


def _site_from(extra: dict[str, Any]) -> str:
    """Resolve the realm URL with libzul's precedence.

    libzul reads ``ZULIP_BASE_URL`` before ``ZULIP_SITE``, so the adapter does
    too; ``config.extra["site"]`` is the fallback when neither env var is set.
    """
    for name in ("ZULIP_BASE_URL", "ZULIP_SITE"):
        value = str(get_scoped_secret(name, "") or "").strip()
        if value:
            return value
    return (extra_or_secret(extra, "site", "ZULIP_SITE")
            or extra_or_secret(extra, "base_url", "ZULIP_BASE_URL"))


def _email_from(extra: dict[str, Any]) -> str:
    return extra_or_secret(extra, "email", "ZULIP_EMAIL")


def _api_key_from(extra: dict[str, Any]) -> str:
    return extra_or_secret(extra, "api_key", "ZULIP_API_KEY")


def _require_mention_from(extra: dict[str, Any]) -> bool:
    return _as_bool(extra_or_secret(extra, "require_mention", "ZULIP_REQUIRE_MENTION", True), True)


def _allowed_streams_from(extra: dict[str, Any]) -> frozenset[str]:
    values = _csv(extra_or_secret(extra, "allowed_streams", "ZULIP_ALLOWED_STREAMS", ""))
    return frozenset(value.lower() for value in values)


def _max_message_length_from(extra: dict[str, Any]) -> int:
    return _as_int(extra_or_secret(extra, "max_message_length", "ZULIP_MAX_MESSAGE_LENGTH"),
                   DEFAULT_MAX_MESSAGE_LENGTH) or DEFAULT_MAX_MESSAGE_LENGTH


def _queue_timeout_from(extra: dict[str, Any]) -> int | None:
    return _as_int(extra_or_secret(extra, "queue_timeout_secs", "ZULIP_QUEUE_TIMEOUT_SECS"))


def _bridge_kwargs(extra: dict[str, Any]) -> dict[str, Any]:
    """Bridge constructor arguments shared by the live and standalone paths."""
    return {
        "base_url": str(_site_from(extra) or "").strip(),
        "email": str(_email_from(extra) or "").strip(),
        "api_key": str(_api_key_from(extra) or "").strip(),
        "queue_timeout_secs": _queue_timeout_from(extra),
    }


def _make_bridge(settings: dict[str, Any]):
    """Pass only complete, profile-resolved credentials across the native boundary."""
    if not all(settings.get(name) for name in ("base_url", "email", "api_key")):
        raise ValueError("complete profile-scoped Zulip credentials are required")
    return zul_bridge.Bridge(**settings)  # type: ignore[union-attr]


def _missing_config(extra: dict[str, Any]) -> list[str]:
    """Required credential names missing from the current Hermes profile."""
    values = {
        "ZULIP_SITE": str(_site_from(extra) or "").strip(),
        "ZULIP_EMAIL": str(_email_from(extra) or "").strip(),
        "ZULIP_API_KEY": str(_api_key_from(extra) or "").strip(),
    }
    return [name for name, value in values.items() if not value]


def _message_text(event: dict[str, Any]) -> str:
    """The bridge's extracted plain text; rendered HTML never reaches the agent.

    ``content`` is libzul's ``rendered_html`` fallback for diagnostics only.  A
    message with no ``plain_text`` (for example an image-only DM) is dropped by
    the empty-text guards rather than handed to the agent as markup.
    """
    plain = event.get("plain_text")
    return plain if isinstance(plain, str) else ""


def _mention_patterns(bot_name: str) -> tuple[re.Pattern[str], ...]:
    """Mention tokens for the bot in both markdown and rendered plain text.

    Zulip's event ``plain_text`` is rendered from the message HTML, so a
    mention of "Zulip Bot" arrives as ``@Zulip Bot`` rather than the source
    form ``@**Zulip Bot**``. Both shapes are accepted, plus the silent
    ``@_**Zulip Bot**`` source form. The rendered form must start a token, so
    ``me@Zulip Bot.example`` is not a mention; it must also end at a word
    boundary, so a bot named "Bot" does not match ``@Botanist``. Neither check
    can separate a longer display name that shares the bot's prefix and
    continues with a space (``@Zulip Bot Jr``); only event flags can.
    """
    if not bot_name:
        return ()
    escaped = re.escape(bot_name)
    return (
        re.compile(r"@_?\*\*" + escaped + r"\*\*[ \t]*"),
        re.compile(r"(?<![\w@])@" + escaped + r"(?!\w)[ \t]*"),
    )


def _decode_target(chat_id: str) -> StreamTarget | DMTarget:
    """Decode a canonical chat id, or a target ref usable as a home channel."""
    try:
        return decode_chat_id(chat_id)
    except ValueError:
        parsed = parse_target_ref(chat_id)
        if parsed is None:
            raise
        return decode_chat_id(parsed[0])


class ZulipAdapter(BasePlatformAdapter):
    """A Hermes gateway adapter for Zulip streams/topics and DMs."""

    MAX_MESSAGE_LENGTH = DEFAULT_MAX_MESSAGE_LENGTH
    splits_long_messages = True

    def __init__(self, config):
        super().__init__(config, Platform("zulip"))
        extra = getattr(config, "extra", {}) or {}
        self.site = str(_site_from(extra) or "").strip()
        self.email = str(_email_from(extra) or "").strip()
        self.api_key = str(_api_key_from(extra) or "").strip()
        self.require_mention = _require_mention_from(extra)
        self.allowed_streams = _allowed_streams_from(extra)
        self.max_message_length = _max_message_length_from(extra)
        self.queue_timeout_secs = _queue_timeout_from(extra)
        self._bridge = None
        self._poll_task: asyncio.Task[None] | None = None
        self._bot_email = self.email.lower()
        self._bot_name = ""
        self._inbound_dedup = MessageDeduplicator(ttl_seconds=INBOUND_DEDUP_TTL_SECONDS)

    @property
    def name(self) -> str:
        return "Zulip"

    def max_message_length_for_chat(self, chat_id: str) -> int:
        """Honor the env-configured limit on framework paths that budget size."""
        return self.max_message_length

    # -- Connection lifecycle -------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Construct the bridge, identify the bot, and start the poll loop."""
        if self._bridge is not None and self._running:
            return True
        if self._bridge is not None or self._poll_task is not None:
            # A previous connect failed or its poller died; drop the stale bridge
            # and lock before building a replacement.
            await self.disconnect()

        extra = getattr(self.config, "extra", {}) or {}
        settings = _bridge_kwargs(extra)
        missing = [env for env, key in (("ZULIP_SITE", "base_url"),
                   ("ZULIP_EMAIL", "email"), ("ZULIP_API_KEY", "api_key"))
                   if not settings[key]]
        if missing:
            logger.error("[Zulip] missing configuration: %s", ", ".join(missing))
            self._set_fatal_error(
                "config_missing",
                f"Zulip is not configured: {', '.join(missing)} must be set",
                retryable=False,
            )
            return False

        self.site, self.email, self.api_key = (settings[key] for key in
                                              ("base_url", "email", "api_key"))
        identity = f"{self.site}:{self.email}"
        if not self._acquire_platform_lock("zulip", identity, f"Zulip account {self.email}"):
            return False

        bridge = None
        try:
            bridge = await asyncio.to_thread(_make_bridge, settings)
            account = await asyncio.to_thread(bridge.whoami)
        except asyncio.CancelledError:
            if bridge is not None:
                await self._close_bridge(bridge)
            self._release_platform_lock()
            raise
        except Exception as exc:
            if bridge is not None:
                await self._close_bridge(bridge)
            self._release_platform_lock()
            self._set_fatal_error("connect_failed", _redact(exc, self.api_key), retryable=True)
            logger.error("[Zulip] connect failed: %s", _redact(exc, self.api_key))
            return False

        account = account if isinstance(account, dict) else {}
        account_email = str(account.get("email") or "").strip()
        self._bot_email = (account_email or self.email).lower()
        self._bot_name = str(account.get("full_name") or "").strip()
        self._bridge = bridge
        self._poll_task = asyncio.create_task(self._poll_loop(bridge), name="zulip-poll")
        self._mark_connected()
        logger.info("[Zulip] connected to %s as %s", self.site, self._bot_email)
        return True

    async def disconnect(self) -> None:
        """Stop polling, close the bridge, and release the account lock."""
        self._running = False
        poll_task, self._poll_task = self._poll_task, None
        if poll_task is not None:
            poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await poll_task
        bridge, self._bridge = self._bridge, None
        if bridge is not None:
            await self._close_bridge(bridge)
        self._release_platform_lock()
        self._mark_disconnected()

    async def _close_bridge(self, bridge: Any) -> None:
        try:
            await asyncio.to_thread(bridge.close)
        except Exception as exc:
            logger.debug("[Zulip] bridge close failed: %s", _redact(exc, self.api_key))

    async def _poll_loop(self, bridge: Any) -> None:
        """Deliver normalized bridge events until stopped or the poller fails fatally."""
        while self._running:
            try:
                event = await asyncio.to_thread(bridge.next_event, _BRIDGE_POLL_TIMEOUT_MS)
            except asyncio.CancelledError:
                raise
            except RuntimeError as exc:
                detail = _redact(exc, self.api_key)
                logger.error("[Zulip] event stream failed: %s", detail)
                self._set_fatal_error("poll_failed", detail, retryable=True)
                await self._notify_fatal_error()
                return
            except Exception as exc:
                logger.warning("[Zulip] event poll error: %s", _redact(exc, self.api_key))
                await asyncio.sleep(0.5)
                continue
            if not event or not self._running:
                continue
            try:
                await self._handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                # One malformed message must not take down the poll loop; never
                # log event content, which is user conversation text.
                logger.warning("[Zulip] dropping malformed inbound event")

    # -- Inbound mapping ------------------------------------------------------

    async def _handle_event(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        kind = event.get("kind")
        if kind not in {"stream", "dm"}:
            return
        message_id = event.get("id")
        if message_id is None:
            return
        message_id_str = str(message_id)

        sender_email = str(event.get("sender_email") or "")
        if self._bot_email and sender_email.lower() == self._bot_email:
            return  # never answer our own messages
        if self._inbound_dedup.is_duplicate(message_id_str):
            return

        text = _message_text(event)
        mapping = self._stream_mapping(event, text) if kind == "stream" else self._dm_mapping(event, text)
        if mapping is None:
            return
        text = mapping["text"]

        built = self.build_source(
            chat_id=mapping["chat_id"],
            chat_name=mapping["chat_name"],
            chat_type=mapping["chat_type"],
            user_id=mapping["user_id"],
            user_name=mapping["user_name"],
            thread_id=mapping["thread_id"],
            chat_topic=mapping["topic"],
            user_id_alt=mapping["user_id_alt"],
            message_id=message_id_str,
        )
        timestamp = self._event_timestamp(event)
        await self.handle_message(MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=built,
            raw_message=event,
            message_id=message_id_str,
            timestamp=timestamp,
        ))

    def _stream_mapping(self, event: dict[str, Any], text: str) -> dict[str, Any] | None:
        stream = str(event.get("stream") or "")
        topic = str(event.get("topic") or "")
        if not stream:
            return None
        if self.allowed_streams and stream.lower() not in self.allowed_streams:
            return None

        flags = event.get("flags")
        flag_set = set(flags) if isinstance(flags, (list, tuple, set)) else set()
        patterns = _mention_patterns(self._bot_name)
        mentioned = bool({"mentioned", "wildcard_mentioned"} & flag_set)
        token_present = any(pattern.search(text) for pattern in patterns)
        if self.require_mention and not (mentioned or token_present):
            return None
        for pattern in patterns:
            text = pattern.sub("", text)
        text = text.strip()
        if not text:
            return None

        stream_id = _as_int(event.get("stream_id"), 0) or 0
        sender_id = event.get("sender_id")
        sender_email = str(event.get("sender_email") or "")
        return {
            "text": text,
            "chat_id": encode_stream(stream, topic, stream_id),
            "chat_name": f"#{stream} > {topic}",
            "chat_type": "group",
            "thread_id": topic,
            "topic": topic,
            "user_id": sender_email or (str(sender_id) if sender_id is not None else None),
            "user_id_alt": str(sender_id) if sender_id is not None else None,
            "user_name": event.get("sender_name"),
        }

    def _dm_mapping(self, event: dict[str, Any], text: str) -> dict[str, Any] | None:
        # Zulip private messages include both one-to-one and group DMs.
        # Apply the transport-level opt-out before either becomes a core event.
        if (getattr(self.config, "extra", None) or {}).get("dm_policy") == "disabled":
            return None
        participants = event.get("dm_participants")
        if not isinstance(participants, list):
            participants = []
        recipients: list[tuple[int, str]] = []
        names: list[str] = []
        for participant in participants:
            if not isinstance(participant, dict):
                continue
            email = str(participant.get("email") or "")
            if self._bot_email and email.lower() == self._bot_email:
                continue  # the bot is always a DM participant; it is not a target
            try:
                participant_id = int(participant.get("id") or 0)
            except (TypeError, ValueError):
                participant_id = 0
            recipients.append((participant_id, email))
            names.append(str(participant.get("name") or email))
        if not recipients:
            return None  # bot-only DM or an unusable participant list
        text = text.strip()
        if not text:
            return None

        sender_id = event.get("sender_id")
        sender_email = str(event.get("sender_email") or "")
        return {
            "text": text,
            "chat_id": encode_dm(recipients),
            "chat_name": ", ".join(names),
            "chat_type": "group" if len(set(recipients)) > 1 else "dm",
            "thread_id": None,
            "topic": None,
            "user_id": sender_email or (str(sender_id) if sender_id is not None else None),
            "user_id_alt": str(sender_id) if sender_id is not None else None,
            "user_name": event.get("sender_name"),
        }

    @staticmethod
    def _event_timestamp(event: dict[str, Any]) -> datetime.datetime:
        raw = event.get("timestamp")
        try:
            return datetime.datetime.fromtimestamp(float(raw), tz=datetime.timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            return datetime.datetime.now(datetime.timezone.utc)

    # -- Outbound -------------------------------------------------------------

    async def send(self, chat_id: str, content: str, reply_to: str | None = None,
                   metadata: dict[str, Any] | None = None) -> SendResult:
        bridge = self._bridge
        if bridge is None:
            return SendResult(success=False, error="Zulip bridge is not connected")
        try:
            target = _decode_target(chat_id)
        except ValueError:
            return SendResult(success=False, error="invalid Zulip chat_id")
        if not (content or "").strip():
            return SendResult(success=False, error="nothing to send")
        chunks = self.truncate_message(content or "", self.max_message_length)

        try:
            message_ids = await asyncio.to_thread(_send_chunks, bridge, target, chunks)
        except _ChunkSendError as exc:
            return self._send_failure_result(exc.cause, exc.delivered_message_ids)
        except Exception as exc:
            return self._send_failure_result(exc, [])
        return SendResult(
            success=True,
            message_id=message_ids[-1] if message_ids else None,
            continuation_message_ids=tuple(message_ids[:-1]),
        )

    def _send_failure_result(self, cause: BaseException, delivered: list[str]) -> SendResult:
        detail = _redact(cause, self.api_key)
        logger.error("[Zulip] send failed: %s", detail)
        result = SendResult(success=False, error=detail)
        if delivered:
            result.raw_response = {
                "delivered_message_ids": delivered,
                "partial_overflow": True,
            }
        return result

    async def send_typing(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        await self._send_typing_op(chat_id, "start")

    async def stop_typing(self, chat_id: str) -> None:
        await self._send_typing_op(chat_id, "stop")

    async def _send_typing_op(self, chat_id: str, op: str) -> None:
        bridge = self._bridge
        if bridge is None:
            return
        try:
            target = _decode_target(chat_id)
        except ValueError:
            return
        try:
            if isinstance(target, StreamTarget):
                if not target.stream_id:
                    logger.debug("[Zulip] typing skipped: unknown stream id for %s", target.stream)
                    return
                await asyncio.to_thread(bridge.typing_stream, target.stream_id, target.topic, op)
            else:
                recipient_ids = tuple(recipient_id for recipient_id, _ in target.recipients
                                      if recipient_id > 0)
                if not recipient_ids:
                    logger.debug("[Zulip] typing skipped: no numeric DM recipients")
                    return
                await asyncio.to_thread(bridge.typing_direct, list(recipient_ids), op)
        except Exception as exc:
            logger.debug("[Zulip] typing %s failed: %s", op, _redact(exc, self.api_key))

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        try:
            target = _decode_target(chat_id)
        except ValueError:
            return {"name": chat_id, "type": "dm"}
        if isinstance(target, StreamTarget):
            return {"name": f"#{target.stream} > {target.topic}", "type": "group"}
        return {"name": ", ".join(email for _, email in target.recipients if email) or "Direct message",
                "type": "dm"}


# -- Plugin entry points ------------------------------------------------------

def check_requirements() -> bool:
    """Passive probe: is the ``zul_bridge`` extension importable right now?"""
    return zul_bridge is not None


def validate_config(config) -> bool:
    """Whether the profile has enough configuration to connect.

    Site, email, and API key may come from env/``config.extra`` or from
    libzul's own settings; ``_missing_config`` applies that rule everywhere.
    """
    return not _missing_config(getattr(config, "extra", None) or {})


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from profile-scoped env before construction.

    The API key is deliberately not seeded: the adapter reads it from the
    profile's env at connect time, and ``hermes status`` output must not gain a
    credential copy.  The home channel enables ``deliver=zulip`` cron jobs.
    """
    site = (str(get_scoped_secret("ZULIP_BASE_URL", "") or "").strip()
            or str(get_scoped_secret("ZULIP_SITE", "") or "").strip())
    email = str(get_scoped_secret("ZULIP_EMAIL", "") or "").strip()
    if not site or not email:
        return None
    seeded = seed_extra_from_env(
        (
            ("ZULIP_REQUIRE_MENTION", "require_mention", lambda value: _as_bool(value, True)),
            ("ZULIP_ALLOWED_STREAMS", "allowed_streams", None),
            ("ZULIP_MAX_MESSAGE_LENGTH", "max_message_length", int),
            ("ZULIP_QUEUE_TIMEOUT_SECS", "queue_timeout_secs", int),
        ),
        home_env="ZULIP_HOME_CHANNEL",
    )
    return {"site": site, "email": email, **seeded}


def _dm_send_recipients(target: DMTarget) -> tuple[str, list[Any]]:
    """Choose the bridge DM call: numeric ids when every recipient has one, else emails."""
    recipient_ids = [recipient_id for recipient_id, _ in target.recipients if recipient_id > 0]
    if len(recipient_ids) == len(target.recipients):
        return "ids", recipient_ids
    emails = [email for _, email in target.recipients if email]
    if len(emails) != len(target.recipients):
        raise ValueError("a Zulip DM recipient has neither a user id nor an email")
    return "emails", emails


class _ChunkSendError(Exception):
    """A chunked send failed after zero or more chunks reached the chat."""

    def __init__(self, cause: BaseException, delivered_message_ids: list[str]) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.delivered_message_ids = delivered_message_ids


def _send_chunks(bridge: Any, target: StreamTarget | DMTarget, chunks: list[str]) -> list[str]:
    """Send chunks in order through the bridge, returning the landed message ids."""
    delivered: list[str] = []
    for chunk in chunks:
        try:
            if isinstance(target, StreamTarget):
                sent_id = bridge.send_stream(target.stream, target.topic, chunk)
            else:
                method, recipients = _dm_send_recipients(target)
                if method == "ids":
                    sent_id = bridge.send_direct_ids(recipients, chunk)
                else:
                    sent_id = bridge.send_direct_emails(recipients, chunk)
        except Exception as exc:
            raise _ChunkSendError(exc, delivered) from exc
        delivered.append(str(sent_id))
    return delivered


def _pre_send_error(message: str) -> dict[str, Any]:
    """A standalone failure that provably never left the process.

    ``delivery: "none"`` tells Hermes' ``delivery_state`` this payload is safe to
    queue for the live adapter's reconnect; omitting ``retryable`` keeps the
    failed send itself from being retried in place.
    """
    return {**send_error(message), "delivery": "none"}


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: str | None = None,
                           media_files: list[str] | None = None,
                           force_document: bool = False) -> dict[str, Any]:
    """Out-of-process cron delivery: open a short-lived bridge, send, close."""
    if media_files:
        return _pre_send_error(
            "Zulip is text-only in this adapter version; media delivery is not supported")
    extra = getattr(pconfig, "extra", None) or {}
    missing = _missing_config(extra)
    if missing:
        return _pre_send_error(
            f"Zulip standalone send: {', '.join(missing)} must be configured")
    try:
        target = _decode_target(chat_id)
    except ValueError:
        return _pre_send_error("Zulip standalone send: invalid chat_id")
    if isinstance(target, StreamTarget):
        topic = target.topic or (thread_id or "")
        if not topic:
            return _pre_send_error("Zulip standalone send: a stream target needs a topic")
        target = StreamTarget(stream_id=target.stream_id, stream=target.stream, topic=topic)

    if not (message or "").strip():
        return _pre_send_error("Zulip standalone send: nothing to send")
    if zul_bridge is None:
        return _pre_send_error("Zulip standalone send: the zul_bridge extension is not installed")
    settings = _bridge_kwargs(extra)
    chunks = ZulipAdapter.truncate_message(message or "", _max_message_length_from(extra))

    bridge = None
    try:
        bridge = await asyncio.to_thread(_make_bridge, settings)
    except Exception as exc:
        detail = _redact(exc, settings["api_key"])
        logger.error("[Zulip] standalone send failed: %s", detail)
        return send_error(f"Zulip standalone send failed: {detail}", nothing_sent=True)
    try:
        message_ids = await asyncio.to_thread(_send_chunks, bridge, target, chunks)
    except _ChunkSendError as exc:
        detail = _redact(exc.cause, settings["api_key"])
        logger.error("[Zulip] standalone send failed: %s", detail)
        result = send_error(f"Zulip standalone send failed: {detail}")
        if exc.delivered_message_ids:
            result["delivered_message_ids"] = list(exc.delivered_message_ids)
            result["partial_overflow"] = True
            result["delivery"] = "partial"
        return result
    except Exception as exc:
        detail = _redact(exc, settings["api_key"])
        logger.error("[Zulip] standalone send failed: %s", detail)
        return send_error(f"Zulip standalone send failed: {detail}")
    finally:
        try:
            await asyncio.to_thread(bridge.close)
        except Exception as exc:
            logger.debug("[Zulip] standalone bridge close failed: %s",
                         _redact(exc, settings["api_key"]))
    return {
        "success": True,
        "message_id": message_ids[-1] if message_ids else None,
        **({"delivered_message_ids": message_ids} if len(message_ids) > 1 else {}),
    }


def register(ctx) -> None:
    """Hermes plugin entry point."""
    ctx.register_platform(
        name="zulip",
        label="Zulip",
        adapter_factory=ZulipAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        required_env=["ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"],
        install_hint=(
            "Build and install the zul_bridge wheel from integrations/hermes-zulip "
            "(see that directory's README), then reinstall this plugin"
        ),
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ZULIP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ZULIP_ALLOWED_USERS",
        allow_all_env="ZULIP_ALLOW_ALL_USERS",
        parse_target_ref_fn=parse_target_ref,
        validate_target_ref_fn=validate_target_ref,
        max_message_length=ZulipAdapter.MAX_MESSAGE_LENGTH,
        emoji="💬",
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Zulip. Zulip renders standard Markdown. "
            "Stream replies must include a topic; reply in the stream and topic "
            "carried by the conversation rather than starting a new topic."
        ),
    )
