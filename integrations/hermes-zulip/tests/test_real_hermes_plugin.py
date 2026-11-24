"""Optional integration check against a real Hermes checkout.

Set ``HERMES_REPO`` to a Hermes Agent checkout to run this test.  It runs in a
subprocess so the unit tests' lightweight stub modules never shadow the real
``gateway``/``hermes_cli`` packages.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import hermes_zulip.install as install_module
import pytest

_HERMES_REPO = os.environ.get("HERMES_REPO")


@pytest.mark.skipif(not _HERMES_REPO, reason="HERMES_REPO is not set")
def test_real_hermes_loads_and_registers_plugin(tmp_path):
    repo = Path(_HERMES_REPO)
    if not (repo / "hermes_cli" / "plugins.py").is_file():
        pytest.skip("HERMES_REPO does not look like a Hermes checkout")

    home = tmp_path / "hermes-home"
    install_module.install_plugin(home)
    (home / "config.yaml").write_text(
        "plugins:\n  enabled:\n    - zulip-platform\n", encoding="utf-8")

    script = textwrap.dedent(
        """
        import asyncio
        import os

        from gateway.config import Platform, PlatformConfig
        from gateway.platform_registry import platform_registry
        from gateway.platforms.event import MessageEvent
        from gateway.platforms.helpers import carry_inbound_dedup
        from gateway.session import SessionSource
        from hermes_cli.plugins import PluginManager

        manager = PluginManager(scope_key=os.environ["HERMES_HOME"])
        manager.discover_and_load()
        loaded = manager._plugins.get("zulip-platform")
        assert loaded is not None, "plugin was not discovered"
        assert loaded.error is None, loaded.error
        assert loaded.enabled is True
        entry = platform_registry.get("zulip")
        assert entry is not None, "platform was not registered"
        assert entry.adapter_factory is not None
        assert entry.check_fn() is True
        assert entry.validate_config(PlatformConfig(enabled=True, extra={})) is True
        assert entry.parse_target_ref_fn("stream:core/ops")[0].startswith("zulip:v1:stream:0:")

        def stream_event(message_id):
            return {
                "id": message_id, "kind": "stream", "stream_id": 12, "stream": "core",
                "topic": "ops", "sender_id": 7, "sender_name": "Ada Lovelace",
                "sender_email": "ada@example.com",
                "content": '<p>hey <span class="user-mention">@Zulip Bot</span> summarize</p>',
                "plain_text": "hey @Zulip Bot summarize", "flags": [],
                "timestamp": 1760000000, "dm_participants": [],
            }

        def dm_event(message_id):
            return {
                "id": message_id, "kind": "dm", "stream_id": None, "stream": None,
                "topic": None, "sender_id": 7, "sender_name": "Ada Lovelace",
                "sender_email": "ada@example.com",
                "content": "<p>direct hello</p>", "plain_text": "direct hello",
                "flags": [], "timestamp": 1760000100,
                "dm_participants": [
                    {"id": 7, "name": "Ada Lovelace", "email": "ada@example.com"},
                    {"id": 1, "name": "Zulip Bot", "email": "bot@example.com"},
                ],
            }

        class FakeBridge:
            def __init__(self):
                self.sent = []

            def send_stream(self, stream, topic, content):
                self.sent.append((stream, topic, content))
                return 501

        def make_adapter(captured):
            adapter = entry.adapter_factory(PlatformConfig(enabled=True, extra={}))
            adapter._bot_email = "bot@example.com"
            adapter._bot_name = "Zulip Bot"

            async def capture(event):
                captured.append(event)

            adapter.handle_message = capture
            return adapter

        async def run():
            captured = []
            adapter = make_adapter(captured)
            adapter._bridge = FakeBridge()
            adapter.max_message_length = 10
            assert adapter.platform is Platform("zulip")
            assert adapter.splits_long_messages is True
            assert len(adapter.truncate_message("first part second part third", 10)) > 1

            await adapter._handle_event(stream_event(41))
            assert len(captured) == 1, captured
            stream_message = captured[0]
            assert isinstance(stream_message, MessageEvent), type(stream_message)
            assert isinstance(stream_message.source, SessionSource)
            assert stream_message.text == "hey summarize", stream_message.text
            source = stream_message.source
            assert source.platform is Platform("zulip")
            assert source.chat_type == "group"
            assert source.chat_name == "#core > ops"
            assert source.thread_id == "ops"
            assert source.chat_topic == "ops"
            assert source.user_id == "ada@example.com"
            assert source.user_id_alt == "7"
            assert source.user_name == "Ada Lovelace"
            assert source.message_id == "41"
            assert source.chat_id.startswith("zulip:v1:stream:12:")

            await adapter._handle_event(dm_event(42))
            assert len(captured) == 2, captured
            dm_message = captured[1]
            assert dm_message.text == "direct hello"
            assert dm_message.source.chat_type == "dm"
            assert dm_message.source.thread_id is None
            assert dm_message.source.chat_name == "Ada Lovelace"
            assert dm_message.source.user_id == "ada@example.com"
            assert dm_message.source.chat_id.startswith("zulip:v1:dm:")

            result = await adapter.send(
                stream_message.source.chat_id, "first part second part third")
            assert result.success is True, result.error
            assert len(adapter._bridge.sent) > 1
            assert all(stream == "core" and topic == "ops" and content
                       for stream, topic, content in adapter._bridge.sent)

            carried = []
            replacement = make_adapter(carried)
            carry_inbound_dedup(adapter, replacement)
            await replacement._handle_event(stream_event(41))
            assert carried == [], "the reconnect dedup carry must drop the replayed id"

        asyncio.run(run())
        print("ok")
        """
    )
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join((
            str(repo),
            str(Path(__file__).resolve().parents[1] / "python"),
        )),
        "HERMES_HOME": str(home),
        "ZULIP_SITE": "https://chat.example.com",
        "ZULIP_EMAIL": "bot@example.com",
        "ZULIP_API_KEY": "secret-key",
    }
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert completed.returncode == 0, (
        f"real Hermes plugin load failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}")
    assert "ok" in completed.stdout
