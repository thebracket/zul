# Isolated profile trial (operational notes)

These notes supplement the install README, based on a CPython 3.14 managed
Hermes 0.23.3 trial with this adapter. They are not a new installer feature.
Check your Hermes version's profile and messaging documentation first.

## 1. Discover the runtime; stage without connecting

Identify the live gateway process executable and launch arguments. Do not assume
that the shell Python or a historical `hermes-agent/venv` path is its interpreter.
Hermes bootstrap can re-exec into managed Python and replace dependency paths.
A separate 3.11 venv successfully importing the extension does not establish
that a managed 3.14 gateway can import it.

Build with the existing maturin commands in the README, or use a reviewed wheel
for the host OS/architecture. The `cp311-abi3` wheel was imported successfully on
CPython 3.14; abi3 does not make a Linux wheel portable to another OS or CPU.
Use bounded Cargo parallelism on shared hosts. No global pip install is needed
for the profile-local workaround below.

Create an empty profile with the supported CLI:

```sh
hermes profile create zulip-trial --no-credentials --no-skills --no-alias
hermes -p zulip-trial config set platforms.zulip.enabled false
```

Set `PROFILE_HOME` to that profile's actual directory and `BUILD_PY` to an
isolated build interpreter holding the wheel. The existing installer supports:

```sh
"$BUILD_PY" -m hermes_zulip.install --hermes-home "$PROFILE_HOME"
hermes -p zulip-trial plugins enable zulip-platform
```

**Packaging gap:** the repository installer writes only `plugin.yaml` and an
`__init__.py` importing `hermes_zulip.adapter`. It does not vendor the wheel,
install dependencies for a managed interpreter, or create the bootstrap below.
A wheel installed only in `BUILD_PY` is therefore insufficient.

### Manual profile-only workaround (not performed by the installer)

In the tested deployment, the reviewed wheel's `hermes_zulip/` and
`zul_bridge/` package directories (including the native shared library) were
copied into `PROFILE_HOME/plugins/zulip/runtime/`. Preserve the wheel contents
and verify their hashes against the reviewed build. The local plugin entry
point was replaced with this dependency-path bootstrap:

```python
from pathlib import Path
import sys

_runtime = str(Path(__file__).resolve().parent / "runtime")
sys.path.insert(0, _runtime)
try:
    from hermes_zulip.adapter import register
finally:
    sys.path.remove(_runtime)

__all__ = ["register"]
```

This is a deployment-local workaround, not isolation between conflicting Python
package versions in one process. Re-running the stock installer replaces this
entry point; preserve/review it deliberately. A supported vendoring installer
and its tests are follow-up work, not implemented here. Before enabling the
platform, import the directory plugin with the **actual gateway interpreter**
and verify `hermes_zulip.adapter.__file__`, `zul_bridge.__file__` and
`adapter.zul_bridge is not None`. A discovery listing alone proves neither the
native import nor a connection. Do not modify Hermes core or shared packages
to fix a profile-local path problem.

## 2. Configure identity and access before activation

Use a Zulip **Generic bot**'s own email and API key, not the owner's human key
or an incoming-webhook bot. Put realm URL, bot email and key in this profile's
`.env` using a local hidden-input prompt; never in shell arguments, chat,
version control or the default profile. Permissions should be owner-only.
Do not copy personal `.env`, OAuth stores, memory, skills or MCP settings.
Profile-scoped missing/partial credentials must fail closed, even when the
process or libzul global settings contain another account.

With those credentials, use authenticated **read-only** `GET /api/v1/users/me`,
`GET /api/v1/users/{user_id}` and subscriptions lookups. Refuse redirects for
credential-bearing requests. Confirm the bot identity, intended active human
users and exact stream subscription. Bot-visible emails may be masked, e.g.
`user123@chat.example.com`; never infer them from a person's public email.
The adapter emits email as `source.user_id` and numeric ID as `user_id_alt`.
Core's profile-extra fallback compares the primary ID, so configure **both**
allowlists with the actual API-visible emails plus numeric IDs. IDs alone are
not a reliable replacement for the primary email on all core paths.

Example only: replace the illustrative identity and stream with verified values.
Use CLI settings, not hand-edited YAML; keep nonsecret settings in config.

```sh
hermes -p zulip-trial config set platforms.zulip.allowed_users '["user123@chat.example.com","123"]'
hermes -p zulip-trial config set platforms.zulip.extra.allowed_users 'user123@chat.example.com,123'
hermes -p zulip-trial config set platforms.zulip.allow_all_users false
hermes -p zulip-trial config set platforms.zulip.extra.allow_all_users false
hermes -p zulip-trial config set platforms.zulip.extra.allowed_streams support
hermes -p zulip-trial config set platforms.zulip.extra.require_mention true
hermes -p zulip-trial config set platforms.zulip.extra.dm_policy allowlist
hermes -p zulip-trial config set platforms.zulip.extra.group_policy allowlist
hermes -p zulip-trial config set platform_toolsets.zulip '[]'
hermes -p zulip-trial config set platform_toolsets.cli '[]'
hermes -p zulip-trial config set mcp_servers '{}'
hermes -p zulip-trial config set memory.memory_enabled false
hermes -p zulip-trial config set memory.user_profile_enabled false
hermes -p zulip-trial config set memory.provider none
```

Verify the effective tool schema is empty, not merely the configured toolset
list. Disable unrelated plugins and check no personal memory was imported.
Keep allow-all false; do not use trusted inbound/service authorization for chat.
Review profile-scoped environment overrides as well as YAML settings.

**Channel vs DM policy:** stream restrictions are case-insensitive and apply
only to stream messages. Stream intake requires a mention by default (including
wildcard mentions); there is **no topic filter**. A preferred test topic is a
convention, not an access boundary. Ordinary allowlisted DMs need no mention.
`extra.dm_policy: disabled` explicitly drops both ordinary and group DMs before
core dispatch; `allowlist` permits mapping and relies on core sender checks.
The adapter itself does not independently filter `extra.allowed_users`.

A private conversation with multiple non-bot participants remains
`chat_type=group`, not a one-to-one DM. It uses group-scope authorization and
per-user session keys with Hermes' default `group_sessions_per_user=true`.
Stream topics use Hermes' thread-session policy (shared by default); enable
`thread_sessions_per_user` separately if that is the desired contract. Neither
setting changes Zulip recipients. Sender allowlisting is not a check that every
group-DM recipient is allowlisted; do not assume that stronger privacy boundary.

## 3. Model credentials are a separate prerequisite

An inference provider need not appear in `hermes plugins list`. Check the
provider's actual model/auth path before enabling a similarly named plugin
(an image-generation plugin is not an inference provider). Use supported
profile model selection and verify a harmless tool-free inference independently.
Hermes may inherit root OAuth credentials, including refresh writes; this is
not a copied profile auth store. Do not copy tokens. Configure fallback models
only when their credentials are actually available to the profile; listing a
fallback does not prove it can run or establish a cost policy.

## 4. Reuse the existing host gateway, then smoke-test

Before creating another gateway, inspect current host ownership, multiplex
mode, profile discovery and cron scheduler scope. On the tested host the
existing multiplexer served the trial with `gateway.multiplex_profiles=true`
and `gateway.standalone=false`. A second standalone gateway was **not** an
acceptable isolation method: it could start another cron ticker. No supported
per-instance cron-disable control was established. Do not install a second
service or blindly restart the default gateway. Profile creation can trigger
Hermes' default auto-multiplex migration: compare nonsecret default-config
fingerprints before/after and investigate rather than blindly restoring.

Enable only after offline access/schema tests and authenticated identity checks.
The multiplexer watcher discovers eligible profiles. **A config re-scan is not
necessarily a live adapter rebuild:** in the tested core it skips an already
connected platform. For changed DM intake policy, a profile-only withdraw and
re-serve was needed. The tested compatibility control was setting the trial's
`gateway.standalone=true`, waiting for its *unserved* acknowledgement, then
restoring false and waiting for a fresh connection. This opt-out is version
specific/deprecated and may emit a CLI unknown-key warning even where runtime
source supports it; inspect current supported controls before using it. Never
start a standalone gateway during that interval. Preserve other profiles and
any in-flight work; coordinate this brief trial interruption with its operator.

Read back actual config and live profile/platform status. Verify with synthetic
messages through the installed adapter **and** core: allowed DM without mention,
allowed stream mention, denied nonallowlisted sender, denied other stream and
unmentioned stream, self filtering, group-DM group scope and distinct DM/user
session keys. Do not send unsolicited messages as a smoke test.

A logged connection and native library mapped in the gateway are stronger than
a plugin listing, but do not prove reply delivery. Let an authorized human send
one benign mention in the allowed channel and one ordinary DM while the trial
is connected. Confirm returned replies. Record authenticated / connected /
end-to-end states separately; the bridge does not expose an affirmative live
queue-registration status. Do not run a second live queue smoke against the
same bot merely to obtain a log line.

## 5. Safe removal

Withdraw only the trial profile using the host's verified profile-scoped
control; wait for it to be stopped and unrouted. Disable its Zulip platform
and plugin via profile-qualified CLI settings. Delete the trial interactively
with `hermes profile delete zulip-trial` only if its sessions/artifacts are no
longer needed. Remove only the deployment's own isolated wheel/runtime files.
Do not stop/uninstall the host gateway, delete default data, revoke shared
OAuth credentials or remove shared packages as trial cleanup.
