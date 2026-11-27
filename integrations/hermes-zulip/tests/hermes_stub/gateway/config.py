"""Minimal ``gateway.config`` stub for the Zulip adapter tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class Platform(Enum):
    """Enough of Hermes' dynamic platform enum for adapter tests."""

    TELEGRAM = "telegram"
    DISCORD = "discord"
    SLACK = "slack"

    @classmethod
    def _missing_(cls, value):
        if not isinstance(value, str) or not value.strip():
            return None
        value = value.strip().lower()
        if value in cls._value2member_map_:
            return cls._value2member_map_[value]
        pseudo = object.__new__(cls)
        pseudo._value_ = value
        pseudo._name_ = value.upper().replace("-", "_").replace(" ", "_")
        cls._value2member_map_[value] = pseudo
        cls._member_map_[pseudo._name_] = pseudo
        return pseudo


@dataclass
class HomeChannel:
    platform: Platform
    chat_id: str
    name: str
    thread_id: Optional[str] = None


@dataclass
class PlatformConfig:
    enabled: bool = False
    token: Optional[str] = None
    api_key: Optional[str] = None
    home_channel: Optional[HomeChannel] = None
    typing_indicator: bool = True
    extra: dict[str, Any] = field(default_factory=dict)
