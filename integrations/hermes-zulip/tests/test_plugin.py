"""Plugin registration, manifest, env seeding, config validation, and standalone send."""

from __future__ import annotations

from pathlib import Path

import hermes_zulip.adapter as adapter_module
import yaml
from gateway.config import PlatformConfig
from hermes_zulip.adapter import (
    ZulipAdapter,
    _env_enablement,
    _standalone_send,
    check_requirements,
    register,
    validate_config,
)
from hermes_zulip.chat_ids import encode_dm, encode_stream

PLUGIN_YAML = Path(__file__).resolve().parents[1] / "python" / "hermes_zulip" / "plugin.yaml"


class RecordingContext:
    def __init__(self):
        self.kwargs = None

    def register_platform(self, **kwargs):
        self.kwargs = kwargs


def test_register_exposes_platform_contract():
    ctx = RecordingContext()
    register(ctx)
    kwargs = ctx.kwargs
    assert kwargs is not None
    assert kwargs["name"] == "zulip"
    assert kwargs["label"] == "Zulip"
    assert kwargs["adapter_factory"] is ZulipAdapter
    assert kwargs["check_fn"] is check_requirements
    assert kwargs["validate_config"] is validate_config
    assert kwargs["required_env"] == ["ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"]
    assert "README" in kwargs["install_hint"]
    assert kwargs["env_enablement_fn"] is _env_enablement
    assert kwargs["cron_deliver_env_var"] == "ZULIP_HOME_CHANNEL"
    assert kwargs["standalone_sender_fn"] is not None
    assert kwargs["allowed_users_env"] == "ZULIP_ALLOWED_USERS"
    assert kwargs["allow_all_env"] == "ZULIP_ALLOW_ALL_USERS"
    assert kwargs["parse_target_ref_fn"] is not None
    assert kwargs["validate_target_ref_fn"] is not None
    assert kwargs["max_message_length"] == 10000
    assert kwargs["emoji"] == "💬"
    assert kwargs["allow_update_command"] is True
    assert "Markdown" in kwargs["platform_hint"]
    assert "topic" in kwargs["platform_hint"]


def test_plugin_yaml_manifest():
    data = yaml.safe_load(PLUGIN_YAML.read_text(encoding="utf-8"))
    assert data["name"] == "zulip-platform"
    assert data["kind"] == "platform"
    assert data["label"] == "Zulip"
    assert data["version"] == "0.1.0"
    assert data["description"]
    assert data["author"]
    assert data["homepage"]
    assert "python_dependencies" not in data
    required = {entry["name"] if isinstance(entry, dict) else entry for entry in data["requires_env"]}
    assert required == {"ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"}
    optional = {entry["name"] if isinstance(entry, dict) else entry for entry in data["optional_env"]}
    assert {"ZULIP_REQUIRE_MENTION", "ZULIP_ALLOWED_STREAMS", "ZULIP_ALLOWED_USERS",
            "ZULIP_ALLOW_ALL_USERS", "ZULIP_HOME_CHANNEL", "ZULIP_MAX_MESSAGE_LENGTH",
            "ZULIP_QUEUE_TIMEOUT_SECS"} <= optional
    api_key = next(entry for entry in data["requires_env"] if entry["name"] == "ZULIP_API_KEY")
    assert api_key["password"] is True


def test_check_requirements_matches_extension_presence(monkeypatch):
    monkeypatch.setattr(adapter_module, "zul_bridge", object(), raising=False)
    assert check_requirements() is True
    monkeypatch.setattr(adapter_module, "zul_bridge", None, raising=False)
    assert check_requirements() is False


def test_env_enablement_seeds_non_secret_options(monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "super-secret")
    monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
    monkeypatch.setenv("ZULIP_ALLOWED_STREAMS", "core, Random")
    monkeypatch.setenv("ZULIP_MAX_MESSAGE_LENGTH", "1234")
    monkeypatch.setenv("ZULIP_QUEUE_TIMEOUT_SECS", "30")
    monkeypatch.setenv("ZULIP_HOME_CHANNEL", "#ops > general")
    seeded = _env_enablement()
    assert seeded["site"] == "https://chat.example.com"
    assert seeded["email"] == "bot@example.com"
    assert seeded["require_mention"] is False
    assert seeded["allowed_streams"] == "core, Random"
    assert seeded["max_message_length"] == 1234
    assert seeded["queue_timeout_secs"] == 30
    assert seeded["home_channel"] == {"chat_id": "#ops > general", "name": "Home"}
    assert "api_key" not in seeded
    assert "super-secret" not in repr(seeded)


def test_env_enablement_requires_site_and_email(monkeypatch):
    assert _env_enablement() is None
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    assert _env_enablement() is None
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    assert _env_enablement() is not None


def test_env_enablement_accepts_base_url_alias(monkeypatch):
    monkeypatch.setenv("ZULIP_BASE_URL", "https://base.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    seeded = _env_enablement()
    assert seeded["site"] == "https://base.example.com"


def test_validate_config_from_env(config, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    assert validate_config(config) is True


def test_validate_config_from_extra(config, fake_bridge):
    config.extra.update({
        "site": "https://chat.example.com",
        "email": "bot@example.com",
        "api_key": "secret",
    })
    assert validate_config(config) is True
    assert fake_bridge.config_status_calls == 0


def test_validate_config_falls_back_to_libzul_status(config, fake_bridge):
    config.extra.update({"site": "https://chat.example.com", "email": "bot@example.com"})
    fake_bridge.config_status_result = {"base_url": True, "email": True, "api_key": True}
    assert validate_config(config) is True
    assert fake_bridge.config_status_calls == 1

    fake_bridge.config_status_result = {"base_url": True, "email": True, "api_key": False}
    assert validate_config(config) is False


def test_validate_config_without_extension_does_not_fall_back(config, monkeypatch):
    monkeypatch.setattr(adapter_module, "zul_bridge", None, raising=False)
    config.extra.update({"site": "https://chat.example.com", "email": "bot@example.com"})
    assert validate_config(config) is False


def test_validate_config_rejects_missing_site_or_email(config, fake_bridge):
    assert validate_config(config) is False
    assert fake_bridge.config_status_calls == 0


def test_target_ref_registration_functions():
    ctx = RecordingContext()
    register(ctx)
    parse_ref = ctx.kwargs["parse_target_ref_fn"]
    validate_ref = ctx.kwargs["validate_target_ref_fn"]
    assert parse_ref is adapter_module.parse_target_ref
    assert validate_ref is adapter_module.validate_target_ref


def test_adapter_uses_env_configuration(fake_bridge, config, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_BASE_URL", "https://ignored.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setenv("ZULIP_REQUIRE_MENTION", "false")
    monkeypatch.setenv("ZULIP_ALLOWED_STREAMS", "core,random")
    monkeypatch.setenv("ZULIP_MAX_MESSAGE_LENGTH", "777")
    monkeypatch.setenv("ZULIP_QUEUE_TIMEOUT_SECS", "15")
    adapter = ZulipAdapter(config)
    assert adapter.site == "https://chat.example.com"
    assert adapter.email == "bot@example.com"
    assert adapter.api_key == "secret"
    assert adapter.require_mention is False
    assert adapter.allowed_streams == frozenset({"core", "random"})
    assert adapter.max_message_length == 777
    assert adapter.queue_timeout_secs == 15


def test_adapter_uses_base_url_alias(fake_bridge, config, monkeypatch):
    monkeypatch.setenv("ZULIP_BASE_URL", "https://base.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    assert ZulipAdapter(config).site == "https://base.example.com"


async def test_standalone_send_stream_text(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    pconfig = PlatformConfig(enabled=True, extra={})
    result = await _standalone_send(pconfig, encode_stream("core", "ops", 12), "hello")
    assert result["success"] is True
    assert result["message_id"] == "1000"
    assert fake_bridge.instances[0].sent == [("stream", "core", "ops", "hello", 1000)]
    assert fake_bridge.instances[0].close_count == 1


async def test_standalone_send_dm_uses_emails_when_id_unknown(fake_bridge, monkeypatch):
    pconfig = PlatformConfig(enabled=True, extra={
        "site": "https://chat.example.com",
        "email": "bot@example.com",
        "api_key": "secret",
    })
    result = await _standalone_send(pconfig, encode_dm([(0, "a@example.com")]), "hello")
    assert result["success"] is True
    assert fake_bridge.instances[0].sent[0][0] == "direct_emails"


async def test_standalone_send_chunks_and_closes(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setenv("ZULIP_MAX_MESSAGE_LENGTH", "10")
    pconfig = PlatformConfig(enabled=True, extra={})
    result = await _standalone_send(pconfig, encode_stream("core", "ops", 12),
                                    "first part second part third")
    bridge = fake_bridge.instances[0]
    assert result["success"] is True
    assert len(bridge.sent) > 1
    assert all(len(entry[3]) <= 10 for entry in bridge.sent)
    assert result["message_id"] == str(bridge.sent[-1][4])
    assert result["delivered_message_ids"] == [str(entry[4]) for entry in bridge.sent]
    assert bridge.close_count == 1


async def test_standalone_send_accepts_target_ref_home_channel(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    "stream:core/ops", "hello")
    assert result["success"] is True
    assert fake_bridge.instances[0].sent == [("stream", "core", "ops", "hello", 1000)]


async def test_standalone_send_rejects_media_without_connecting(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    result = await _standalone_send(
        PlatformConfig(enabled=True, extra={}),
        encode_stream("core", "ops", 12),
        "caption",
        media_files=["/tmp/image.png"],
    )
    assert result["error"]
    assert result["retryable"] is True
    assert "text-only" in result["error"].lower()
    assert fake_bridge.instances == []


async def test_standalone_send_reports_missing_config(fake_bridge):
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "ZULIP_SITE" in result["error"]
    assert fake_bridge.instances == []


async def test_standalone_send_redacts_secret_on_constructor_error(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret-key")
    fake_bridge.constructor_error = RuntimeError("bad api_key=secret-key")
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "error" in result
    assert "secret-key" not in result["error"]


async def test_standalone_send_closes_bridge_on_send_error(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    fake_bridge.send_error = RuntimeError("network down")
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "error" in result
    assert fake_bridge.instances[0].close_count == 1


async def test_standalone_send_without_extension(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setattr(adapter_module, "zul_bridge", None, raising=False)
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "zul_bridge" in result["error"]
    assert fake_bridge.instances == []
