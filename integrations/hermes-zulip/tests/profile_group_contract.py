"""Offline real-Hermes/native contract probe; run with their PYTHONPATH."""
import asyncio
from types import SimpleNamespace

import zul_bridge
from gateway.config import PlatformConfig
from gateway.session import build_session_key
from gateway.authz_mixin import GatewayAuthorizationMixin
from hermes_zulip import adapter as module
from hermes_zulip.chat_ids import DMTarget, decode_chat_id


async def main():
    from gateway.platform_registry import PlatformEntry, platform_registry
    platform_registry.register(PlatformEntry("zulip", "Zulip", module.ZulipAdapter, lambda: True))
    # Native settings really see a dummy personal account. The profile resolver
    # deliberately has no credentials; none may be borrowed from native config.
    assert all(zul_bridge.config_status().values())
    from agent.secret_scope import (set_secret_scope, reset_secret_scope,
                                    set_multiplex_context, reset_multiplex_context)
    multiplex = set_multiplex_context(True)
    token = set_secret_scope({})
    try:
        for extra in ({}, {"site": "https://team.invalid", "email": "team@example.invalid"}):
            adapter = module.ZulipAdapter(PlatformConfig(enabled=True, extra=extra))
            assert module._missing_config(extra)
            assert await adapter.connect() is False
            assert adapter.fatal_error_code == "config_missing"
        try:
            module._make_bridge({"base_url": "https://team.invalid", "email": "team@example.invalid", "api_key": ""})
        except ValueError:
            pass
        else:
            raise AssertionError("native boundary accepted partial credentials")
    finally:
        reset_secret_scope(token)
        reset_multiplex_context(multiplex)

    adapter = module.ZulipAdapter(PlatformConfig(enabled=True, extra={}))
    adapter._bot_email = "bot@example.invalid"
    captured = []
    async def capture(event):
        captured.append(event.source)
    adapter.handle_message = capture
    participants = [{"id": i, "email": email} for i, email in enumerate(
        ("bot@example.invalid", "alice@example.invalid", "bob@example.invalid"), 1)]
    for sender in (1, 2):
        await adapter._handle_event(dict(id=sender, kind="dm", plain_text="hello",
            sender_id=sender + 1, sender_email=participants[sender]["email"], dm_participants=participants))
    alice, bob = captured
    assert alice.chat_type == bob.chat_type == "group"
    assert alice.chat_id == bob.chat_id
    assert build_session_key(alice, group_sessions_per_user=True) != build_session_key(bob, group_sessions_per_user=True)
    assert build_session_key(alice, group_sessions_per_user=False) == build_session_key(bob, group_sessions_per_user=False)
    target = decode_chat_id(alice.chat_id)
    assert isinstance(target, DMTarget)
    assert {email for _, email in target.recipients} == {"alice@example.invalid", "bob@example.invalid"}

    sent = []
    adapter._bridge = SimpleNamespace(send_direct_ids=lambda ids, text: sent.append((ids, text)) or 99)
    assert (await adapter.send(alice.chat_id, "reply")).success
    assert sent == [([2, 3], "reply")]

    # Exercise core's actual authorization dispatch, with a DM-only policy
    # grant. Anonymous sources make the denied group stop before other grants.
    policy = SimpleNamespace(
        _adapter_profile_for_source=lambda source: None,
        _chat_scoped_grant=lambda source, profile, is_group, delegation: not is_group)
    alice.user_id = None
    assert GatewayAuthorizationMixin._principal_authorized(policy, alice, allow_adapter_delegation=True) is False
    alice.chat_type = "dm"
    assert GatewayAuthorizationMixin._principal_authorized(policy, alice, allow_adapter_delegation=True) is True
    # Intake denial must precede core grants, even for an authorized human.
    for dm_policy in ("disabled", "open", None):
        for count in (2, 3):
            extra = {} if dm_policy is None else {"dm_policy": dm_policy}
            adapter = module.ZulipAdapter(PlatformConfig(enabled=True, extra=extra))
            adapter._bot_email = "bot@example.invalid"
            received = []
            async def authorize(event):
                received.append(GatewayAuthorizationMixin._principal_authorized(
                    SimpleNamespace(_adapter_profile_for_source=lambda source: None,
                        _chat_scoped_grant=lambda *args, **kwargs: True),
                    event.source, allow_adapter_delegation=True))
            adapter.handle_message = authorize
            await adapter._handle_event(dict(id=100, kind="dm", plain_text="hello",
                sender_id=2, sender_email="alice@example.invalid", dm_participants=participants[:count]))
            assert received == ([] if dm_policy == "disabled" else [True])
    print("profile/group/DM policy real contract ok")


if __name__ == "__main__":
    asyncio.run(main())
