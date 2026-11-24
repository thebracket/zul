"""Plugin registration, manifest, env seeding, config validation, and standalone send."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import hermes_zulip.adapter as adapter_module
import pytest
import yaml
from gateway.config import PlatformConfig
from gateway.platforms._shared import delivery_state
from hermes_zulip.adapter import (
    DEFAULT_MAX_MESSAGE_LENGTH,
    ZulipAdapter,
    _env_enablement,
    _missing_config,
    _redact,
    _standalone_send,
    check_requirements,
    register,
    validate_config,
)
from hermes_zulip.chat_ids import encode_dm, encode_stream

PLUGIN_YAML = Path(__file__).resolve().parents[1] / "python" / "hermes_zulip" / "plugin.yaml"
_HERMES_REPO = os.environ.get("HERMES_REPO")


class RecordingContext:
    def __init__(self):
        self.kwargs = None

    def register_platform(self, **kwargs):
        self.kwargs = kwargs


def test_register_exposes_platform_contract(monkeypatch):
    monkeypatch.setattr(ZulipAdapter, "MAX_MESSAGE_LENGTH", 12345)
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
    # The registration derives from the class constant, not the module default.
    assert kwargs["max_message_length"] == 12345
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


def test_import_without_the_extension_keeps_discovery_working(tmp_path):
    integration = Path(__file__).resolve().parents[1]
    script = textwrap.dedent(
        """
        import sys

        class Blocker:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "zul_bridge" or fullname.startswith("zul_bridge."):
                    raise ImportError("zul_bridge blocked for this test")
                return None

        sys.meta_path.insert(0, Blocker())
        import hermes_zulip.adapter as adapter
        from gateway.config import PlatformConfig

        assert adapter.zul_bridge is None
        assert adapter.check_requirements() is False
        assert adapter.validate_config(PlatformConfig(enabled=True, extra={
            "site": "https://chat.example.com",
            "email": "bot@example.com",
            "api_key": "secret",
        })) is True
        print("ok")
        """
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join((
            str(integration / "python"),
            str(integration / "tests" / "hermes_stub"),
        )),
    }
    for key in [name for name in env if name.startswith("ZULIP_")]:
        env.pop(key)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, (
        f"import without zul_bridge failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}")
    assert "ok" in completed.stdout


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
    fake_bridge.config_status_result = {"base_url": False, "email": False, "api_key": False}
    assert validate_config(config) is False
    assert fake_bridge.config_status_calls == 1


def test_validate_config_accepts_libzul_site_and_email(config, fake_bridge):
    fake_bridge.config_status_result = {"base_url": True, "email": True, "api_key": True}
    assert validate_config(config) is True


def test_missing_config_returns_the_env_names(config, fake_bridge):
    fake_bridge.config_status_result = {"base_url": False, "email": False, "api_key": False}
    assert _missing_config({}) == ["ZULIP_SITE", "ZULIP_EMAIL", "ZULIP_API_KEY"]

    config.extra.update({"site": "https://chat.example.com", "email": "bot@example.com"})
    assert _missing_config(config.extra) == ["ZULIP_API_KEY"]

    fake_bridge.config_status_result = {"base_url": False, "email": False, "api_key": True}
    assert _missing_config(config.extra) == []


def test_target_ref_registration_functions():
    ctx = RecordingContext()
    register(ctx)
    parse_ref = ctx.kwargs["parse_target_ref_fn"]
    validate_ref = ctx.kwargs["validate_target_ref_fn"]
    assert parse_ref is adapter_module.parse_target_ref
    assert validate_ref is adapter_module.validate_target_ref


def test_adapter_uses_env_configuration(fake_bridge, config, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://ignored.example.com")
    monkeypatch.setenv("ZULIP_BASE_URL", "https://chat.example.com")
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


def test_env_enablement_prefers_base_url_over_site(monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://site.example.com")
    monkeypatch.setenv("ZULIP_BASE_URL", "https://base.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    assert _env_enablement()["site"] == "https://base.example.com"


def test_adapter_site_extra_wins_when_no_env_is_set(config):
    config.extra["site"] = "https://extra.example.com"
    assert ZulipAdapter(config).site == "https://extra.example.com"


def test_adapter_site_extra_base_url_alias_when_no_env_is_set(config):
    config.extra["base_url"] = "https://extra.example"
    assert ZulipAdapter(config).site == "https://extra.example"


def test_adapter_uses_base_url_alias(fake_bridge, config, monkeypatch):
    monkeypatch.setenv("ZULIP_BASE_URL", "https://base.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    assert ZulipAdapter(config).site == "https://base.example.com"


def test_adapter_length_limit_is_visible_to_framework_calls(fake_bridge, config, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setenv("ZULIP_MAX_MESSAGE_LENGTH", "777")
    adapter = ZulipAdapter(config)
    assert adapter.max_message_length_for_chat("any-chat") == 777
    assert ZulipAdapter.MAX_MESSAGE_LENGTH == DEFAULT_MAX_MESSAGE_LENGTH


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
    assert "retryable" not in result
    assert result["delivery"] == "none"
    assert delivery_state(result) == "none"
    assert "text-only" in result["error"].lower()
    assert fake_bridge.instances == []


async def test_standalone_send_permanent_target_errors_are_not_retryable(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    pconfig = PlatformConfig(enabled=True, extra={})
    invalid = await _standalone_send(pconfig, "telegram:1", "hello")
    assert "invalid chat_id" in invalid["error"]
    assert "retryable" not in invalid
    assert invalid["delivery"] == "none"
    assert delivery_state(invalid) == "none"
    empty = await _standalone_send(pconfig, encode_stream("core", "ops", 12), "   ")
    assert "nothing to send" in empty["error"]
    assert "retryable" not in empty
    assert empty["delivery"] == "none"
    assert delivery_state(empty) == "none"
    no_topic = await _standalone_send(pconfig, encode_stream("core", "", 12), "hello")
    assert "needs a topic" in no_topic["error"]
    assert "retryable" not in no_topic
    assert no_topic["delivery"] == "none"
    assert delivery_state(no_topic) == "none"
    assert fake_bridge.instances == []


async def test_standalone_send_reports_missing_config(fake_bridge):
    fake_bridge.config_status_result = {"base_url": False, "email": False, "api_key": False}
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "ZULIP_SITE" in result["error"]
    assert "retryable" not in result
    assert result["delivery"] == "none"
    assert delivery_state(result) == "none"
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
    assert result["retryable"] is True
    assert delivery_state(result) == "none"


async def test_standalone_send_closes_bridge_on_send_error(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    fake_bridge.send_error = RuntimeError("network down")
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "error" in result
    assert "delivery" not in result
    assert delivery_state(result) == "uncertain"
    assert "partial_overflow" not in result
    assert "delivered_message_ids" not in result
    assert fake_bridge.instances[0].close_count == 1


async def test_standalone_send_generic_failure_is_uncertain_and_redacted(
        fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret-key")

    def explode(*args, **kwargs):
        raise RuntimeError("standalone exploded api_key=secret-key")

    monkeypatch.setattr(adapter_module, "_send_chunks", explode)
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "standalone exploded" in result["error"]
    assert "secret-key" not in result["error"]
    assert "delivery" not in result
    assert delivery_state(result) == "uncertain"
    assert "partial_overflow" not in result
    assert fake_bridge.instances[0].close_count == 1


async def test_standalone_send_reports_partial_delivery(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setenv("ZULIP_MAX_MESSAGE_LENGTH", "10")
    fake_bridge.send_failures = [None, RuntimeError("second chunk is down")]
    result = await _standalone_send(
        PlatformConfig(enabled=True, extra={}),
        encode_stream("core", "ops", 12),
        "first part second part third",
    )
    bridge = fake_bridge.instances[0]
    assert result["error"]
    assert "retryable" not in result
    assert result["delivery"] == "partial"
    assert delivery_state(result) == "partial"
    assert result["partial_overflow"] is True
    assert result["delivered_message_ids"] == [str(bridge.sent[0][4])]
    assert len(bridge.sent) == 1
    assert bridge.close_count == 1


def test_redact_removes_token_pattern_and_url_userinfo_independently():
    cleaned = _redact("token=abc123 https://user:pass@host/x", "unrelated-secret")
    assert "abc123" not in cleaned
    assert "user:pass@" not in cleaned
    assert "<redacted>" in cleaned


async def test_standalone_send_without_extension(fake_bridge, monkeypatch):
    monkeypatch.setenv("ZULIP_SITE", "https://chat.example.com")
    monkeypatch.setenv("ZULIP_EMAIL", "bot@example.com")
    monkeypatch.setenv("ZULIP_API_KEY", "secret")
    monkeypatch.setattr(adapter_module, "zul_bridge", None, raising=False)
    result = await _standalone_send(PlatformConfig(enabled=True, extra={}),
                                    encode_stream("core", "ops", 12), "hello")
    assert "zul_bridge" in result["error"]
    assert "retryable" not in result
    assert result["delivery"] == "none"
    assert delivery_state(result) == "none"
    assert fake_bridge.instances == []


@pytest.mark.skipif(not _HERMES_REPO, reason="HERMES_REPO is not set")
def test_real_hermes_delivery_state_reads_the_pre_send_envelope(tmp_path):
    repo = Path(_HERMES_REPO)
    if not (repo / "gateway" / "platforms" / "_shared.py").is_file():
        pytest.skip("HERMES_REPO does not look like a Hermes checkout")
    integration = Path(__file__).resolve().parents[1]
    script = textwrap.dedent(
        """
        import asyncio

        import hermes_zulip.adapter as adapter
        from gateway.config import PlatformConfig
        from gateway.platforms._shared import delivery_state
        from hermes_zulip.adapter import _standalone_send
        from hermes_zulip.chat_ids import encode_stream

        config = PlatformConfig(enabled=True, extra={
            "site": "https://chat.example.com",
            "email": "bot@example.com",
            "api_key": "secret-key",
        })
        media = asyncio.run(_standalone_send(
            config, "stream:core/ops", "hello", media_files=["/tmp/image.png"],
        ))
        no_topic = asyncio.run(_standalone_send(
            config, encode_stream("core", "", 12), "hello",
        ))
        for label, result in (("media", media), ("no_topic", no_topic)):
            assert "error" in result, (label, result)
            assert "retryable" not in result, (label, result)
            assert result["delivery"] == "none", (label, result)
            assert delivery_state(result) == "none", (label, result)

        class DummyBridge:
            def close(self):
                pass

        # Exercise the post-connect boundary through the real helper without a
        # Zulip server: the bridge constructor and chunk sender are replaced.
        adapter.zul_bridge = object()
        adapter._make_bridge = lambda extra: DummyBridge()

        def failing_send(cause):
            def fail(bridge, target, chunks):
                raise cause

            adapter._send_chunks = fail
            return asyncio.run(_standalone_send(
                config, encode_stream("core", "ops", 12), "hello",
            ))

        uncertain = failing_send(RuntimeError("send exploded"))
        assert "delivery" not in uncertain, uncertain
        assert delivery_state(uncertain) == "uncertain", uncertain
        partial = failing_send(adapter._ChunkSendError(RuntimeError("send exploded"), ["42"]))
        assert partial["delivery"] == "partial", partial
        assert delivery_state(partial) == "partial", partial
        print("ok")
        """
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join((str(repo), str(integration / "python"))),
    }
    for key in [name for name in env if name.startswith("ZULIP_")]:
        env.pop(key)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 0, (
        f"real Hermes delivery_state check failed\nstdout:\n{completed.stdout}"
        f"\nstderr:\n{completed.stderr}")
    assert "ok" in completed.stdout
