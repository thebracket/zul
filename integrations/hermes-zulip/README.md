# Hermes Zulip adapter

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) gateway plugin
that connects an agent to Zulip. Streams and topics become group sessions,
direct messages become DM sessions, and replies land back in the topic that
started the conversation.

The plugin ships a PyO3 bridge over this repository's `libzul` client, so one
wheel installs everything the adapter needs. Validated against Hermes Agent
commit `24b148198e4c1ff8b49b9b18863d093ea20b9586`.

## Architecture

```
Zulip server
  ▲  REST sends (stream/DM)        │  event queue long-poll (get_events)
  │                                ▼
  │                        ┌───────────────────┐
  │                        │  zul_bridge (PyO3)│  blocking Python API, GIL released
  │                        └─────────┬─────────┘
  │                                  │  libzul async client + queue poller
  │                                  │
  │                        ┌─────────▼─────────┐
  └────────────────────────┤  hermes_zulip     │  platform adapter
                           │  (Hermes plugin)  │  session mapping, sends, typing
                           └─────────┬─────────┘
                                     │
                           ┌─────────▼─────────┐
                           │  Hermes gateway   │  sessions, authz, cron delivery
                           └───────────────────┘
```

- **Hermes gateway** owns sessions, authorization, retries, and cron delivery.
- **`hermes_zulip`** is the platform plugin: it maps Zulip events onto Hermes
  `MessageEvent`/`SessionSource` values and calls the bridge for outbound
  messages.
- **`zul_bridge`** is a compiled abi3 extension. A background thread drives a
  tokio runtime over `libzul`: sends are one-shot commands, inbound messages
  come from a long-polled Zulip event queue.
- **libzul** implements the Zulip REST client, configuration loading, and event
  normalization shared with the rest of this repository.

The bridge imports defensively inside the plugin, so plugin discovery and
`hermes plugins list` work before the wheel is installed; the adapter only
refuses to connect.

## Requirements

- Hermes Agent (tested commit `24b148198e4c1ff8b49b9b18863d093ea20b9586`).
- Python 3.11 or newer in the environment Hermes runs under. Hermes currently
  runs CPython 3.14; the wheel is abi3 (`cp311-abi3`), so one build covers that
  whole range.
- A Rust toolchain **only when installing from source**, because maturin builds
  the extension during `pip install`. Installing a prebuilt wheel needs no Rust.

## Install

First identify the Python interpreter used by the **running gateway** (its
process executable / service command), not just the shell's `python` or an old
`hermes-agent/venv` path. Managed Hermes installations can re-exec into a
CPython 3.14 environment under their install store. Installing in a scratch
venv or launching `python -m hermes_cli.main` does not guarantee that venv
survives Hermes bootstrap. The following commands describe the existing
shared-interpreter installation; set `HERMES_PY` to the verified interpreter.
For a profile-only installation that avoids changing shared packages, see
[the isolated trial guide](PROFILE_SETUP.md), including its explicit manual
vendoring workaround and installer limitation.

```sh
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
# Set this to the verified gateway interpreter before continuing:
: "${HERMES_PY:?Set HERMES_PY to the gateway Python executable}"

# From a checkout of this repository (builds the Rust extension):
uv pip install --python "$HERMES_PY" ./integrations/hermes-zulip

# Equivalent with plain pip, using the same interpreter:
# "$HERMES_PY" -m pip install ./integrations/hermes-zulip

# Copy the plugin into "$HERMES_HOME/plugins/zulip":
"$HERMES_PY" -m hermes_zulip.install

# Enable it (the enable key is the manifest name, `zulip-platform`):
hermes plugins enable zulip-platform
```

`hermes-zulip-install` is the same installer as a console script, available when
that venv is on `PATH` or invoked as `"$HERMES_HOME/hermes-agent/venv/bin/hermes-zulip-install"`.
Use `--hermes-home PATH` for a non-default profile, or `--print-path` to print
only the plugin directory. `HERMES_HOME` is honored when no argument is given;
otherwise the installer uses `~/.hermes`. Re-running the installer is
idempotent: unchanged files are left alone, changed files are replaced
atomically. It never reads or writes credentials.

If Hermes was installed another way (system Python, pipx, a distro package),
substitute that interpreter above. `hermes pm` manages Hermes' own optional
dependencies (Node, Chromium, MCP helpers); it does not install this wheel.

For an existing multi-profile gateway, use the profile-scoped activation and
reload procedure in [PROFILE_SETUP.md](PROFILE_SETUP.md); do not blindly restart
the host or start a second gateway. Verify discovery with:

```sh
hermes plugins list          # zulip-platform should be listed and enabled
```

If your shell cannot find `hermes`, call the installation's launcher for this
command (for example `"$HERMES_HOME/hermes-agent/venv/bin/hermes"`).

## Zulip setup

1. **Create a bot.** In Zulip: *Personal settings → Bots → Add a new bot*, type
   **Generic bot**, and give it a name (for example `Hermes`). A generic bot can
   call the REST API; incoming-webhook bots cannot.
2. **Subscribe the bot to its streams.** Open each stream's settings →
   *Subscribers* and add the bot. Zulip only delivers message events for
   streams the bot is subscribed to. Mentioning the bot does not subscribe it.
3. **Copy the API key.** The bot's row in *Bots* has an API key; use it with the
   bot's email in the configuration below. The key is a credential: keep it in
   `.env`, never in `config.yaml` or a committed file.

Direct messages need no subscriptions. To let people DM the bot, they must pass
the gateway's authorization rules described below.

## Configuration

Credentials and most settings come from the environment. Put them in
`$HERMES_HOME/.env` (Hermes loads it at startup) or export them before starting
the gateway. See [`.env.example`](.env.example) for a copyable template.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ZULIP_SITE` | required | Realm URL, e.g. `https://chat.example.com`. `ZULIP_BASE_URL` is an alias and wins when both are set. |
| `ZULIP_EMAIL` | required | Email address of the bot account. |
| `ZULIP_API_KEY` | required | API key of the bot account. |
| `ZULIP_REQUIRE_MENTION` | `true` | In streams, only messages that mention the bot reach the agent. |
| `ZULIP_ALLOWED_STREAMS` | empty (all) | Comma-separated stream names to accept, case-insensitive. Empty accepts every stream the bot is subscribed to. |
| `ZULIP_ALLOWED_USERS` | empty | Comma-separated Zulip user IDs or emails allowed to use the bot. |
| `ZULIP_ALLOW_ALL_USERS` | `false` | Truthy value allows every Zulip user; overrides `ZULIP_ALLOWED_USERS`. |
| `ZULIP_HOME_CHANNEL` | empty | Default target for cron/home delivery: `stream:<stream>/<topic>`, `#<stream>/<topic>`, `@<email>`, or a canonical chat id. |
| `ZULIP_MAX_MESSAGE_LENGTH` | `10000` | Split outbound messages longer than this many characters. |
| `ZULIP_QUEUE_TIMEOUT_SECS` | unset (server default) | Idle timeout for the Zulip event queue, in seconds. |

Environment values win over `config.yaml`. Non-secret settings can also live in
the platform block (this nested form is accepted; the top-level
`platforms.zulip` block is equivalent):

```yaml
gateway:
  platforms:
    zulip:
      enabled: true
      home_channel:
        chat_id: "stream:support/triage"
        name: "Support triage"
      extra:
        site: https://chat.example.com
        email: bot@example.com
        require_mention: true
        allowed_streams: support,eng
```

Keep `ZULIP_API_KEY` in the active Hermes profile's `.env` only. The adapter
requires site, email, and API key resolved by Hermes for that profile (with
`extra` as the profile-local fallback). It never borrows missing credentials
from libzul's process environment or global XDG settings. Normal `zul` CLI
configuration defaults are unchanged.

## Behavior

**Sessions.** A stream message maps to a group session keyed by stream and
topic (`chat_name` is `#stream > topic`, `thread_id`/`chat_topic` are the topic),
so each topic is its own conversation. A direct message maps to a DM session
keyed by its non-bot participants; the bot itself is excluded from the chat id.
The adapter never answers its own messages and drops repeated message IDs
within 10 minutes.

**Mentions.** With `ZULIP_REQUIRE_MENTION` on (the default), a stream message
must either carry Zulip's event-level `mentioned`/`wildcard_mentioned` flag or
contain the rendered mention token `@Bot Name` in the text. The token is
stripped before the agent sees the message. Silent mentions render without
`@`, so they are detected only through the event flags; a bot named `Bot` does
not match `@Botanist`, and the rendered token must start a word, so
`me@Bot.example` is not a mention. A longer display name that shares the bot's
name prefix and continues with a space (`@Bot Jr`) cannot be told apart from a
mention of the bot in plain text; only the event flags can distinguish them.
Set `ZULIP_REQUIRE_MENTION=false` to accept every subscribed stream message.

**Long replies.** Text is split at `ZULIP_MAX_MESSAGE_LENGTH` (newline, then
space, then hard split) and sent chunk by chunk in order. If a later chunk
fails, the result reports the message IDs that already landed plus
`partial_overflow`, which tells Hermes not to resend the earlier chunks.

**Typing.** Typing notifications are best effort. DMs use numeric recipient
ids. Stream typing requires the canonical stream id: a target parsed from
`stream:<name>/<topic>` has no numeric id, so typing is a no-op there and is
only logged at debug level.

**Cron and home delivery.** `ZULIP_HOME_CHANNEL` names the default target for
cron jobs with `deliver=zulip` and for `/sethome`. Out-of-process cron sends
open a short-lived bridge with the same settings, send the chunks, and close
it. Text-only limits apply there too.

**Outbound targets.** Besides the canonical chat ids Hermes stores on sessions,
targets accept:

- `stream:<name>/<topic>` — stream and topic by name;
- `#<name>/<topic>` — shorthand for the same;
- `@<email>` — a direct message to one Zulip user.

**Authorization.** The gateway authorizes senders before dispatch. Configure
both `platforms.zulip.allowed_users` and `extra.allowed_users` with authenticated
bot-visible emails plus numeric IDs; see [PROFILE_SETUP.md](PROFILE_SETUP.md)
for the multiplex core path and masked-email caveat. Keep allow-all false for
a restricted trial. Ordinary allowlisted DMs do not require a mention;
`extra.dm_policy: disabled` drops ordinary and private group DMs before core
checks, while `allowlist` enables normal core sender authorization. Private
group DMs retain group scope, not DM-only permissions. `ZULIP_ALLOWED_STREAMS`
narrows streams only; there is no topic filter.

## Limits and residuals

This is a text-only v1:

- Media sends are rejected with a non-retryable error; nothing is uploaded.
  Inbound media is not downloaded or forwarded.
- No message edits, reactions, deletion, or token streaming. Replies are plain
  sends; Hermes' normal chunking applies.
- A queue that Zulip rejects, or that fails three consecutive polls, is deleted
  and re-registered. The replacement starts at a new server cursor, so events
  produced during that gap are not delivered. Discarding the queue and a
  successful replacement both restart the failure budget; a fatal poller error
  is reported only after five consecutive registration failures.
- Silent mentions (the no-notification mention form) render without `@`, so
  detection depends on the event-level flags Zulip supplies. If the flags are
  absent, a silent mention is treated as an ordinary message and dropped when
  mentions are required.
- Event `content` is the rendered content Zulip returns (libzul registers with
  `apply_markdown=true`), not raw Markdown source. It is kept for diagnostics
  only: the agent never receives it. A message with no `plain_text` (for
  example an image-only DM) is dropped rather than delivered as markup.
- Non-message and malformed message events are skipped without stopping the
  poller.
- One gateway process holds one bridge per bot account (a platform lock keyed by
  site and email); a second adapter for the same account fails to connect.
- The platform registration advertises the 10000-character default for Hermes'
  cron pre-chunking budget. Raising `ZULIP_MAX_MESSAGE_LENGTH` changes live
  sends (including the standalone cron sender), not that advertised budget.

## Testing

Rust tests (wiremock; no network) and the workspace checks:

```sh
cargo fmt --manifest-path integrations/hermes-zulip/Cargo.toml -- --check
cargo clippy --manifest-path integrations/hermes-zulip/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path integrations/hermes-zulip/Cargo.toml
cargo test -p libzul
cargo build --workspace
cargo fmt --all --check
```

Python tests run against a small Hermes stub, so they need no Hermes install.
Build the wheel with maturin and install it into a CPython 3.14 venv, then run
pytest, the real-checkout variant, and the two smoke scripts:

```sh
cd integrations/hermes-zulip
uv venv --python 3.14 .venv
uv pip install --python .venv/bin/python maturin pytest pytest-asyncio PyYAML
.venv/bin/maturin build --release
uv pip install --python .venv/bin/python --force-reinstall target/wheels/zul_bridge-0.1.0-*.whl

PYTHONPATH=python .venv/bin/python -m pytest tests -q
HERMES_REPO=/path/to/hermes-agent PYTHONPATH=python .venv/bin/python -m pytest tests -q
.venv/bin/python tests/python_smoke.py
```

`python_smoke.py` starts a local stdlib fake Zulip server and exercises the
installed extension: import/version, `whoami`, sends, typing, long-poll
timeout, GIL release (while `next_event` waits, and across `Bridge()` and
`config_status()` with a slow settings read), close, and the missing-config
error. It is not collected by pytest.

`live_smoke.py` talks to the real realm using credentials from the environment
or libzul settings, prints the account display name and realm host, polls for
one second, and closes. It sends nothing:

```sh
# From the repository root, with the repo .env holding the bot credentials:
set -a; . ./.env; set +a
integrations/hermes-zulip/.venv/bin/python integrations/hermes-zulip/tests/live_smoke.py
```

Pytest collects the adapter tests plus `tests/test_install.py`, which covers
explicit/`HERMES_HOME`/default home resolution, idempotent re-runs, atomic
rewrites, installed file contents, absence of credential values, the module and
console-script entry points, and importing the installer without the gateway.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `hermes plugins list` shows `zulip-platform` but it fails to load with an import error | The wheel is missing from the environment Hermes runs under. Run `"$HERMES_PY" -c "import zul_bridge"` with the same interpreter and reinstall. |
| Plugin is discovered but not enabled | `hermes plugins enable zulip-platform`. The enable key is the manifest `name`, not the install directory (`plugins/zulip`). Restart the gateway. |
| `Zulip is not configured: ... must be set` | Set the named `ZULIP_*` variables in `$HERMES_HOME/.env` (or the profile's env) and restart. `ZULIP_SITE`/`ZULIP_BASE_URL`, `ZULIP_EMAIL`, and `ZULIP_API_KEY` are all required. |
| `Zulip is not configured` while libzul's own settings exist | Expected: the Hermes adapter deliberately requires complete profile-scoped credentials; libzul's global settings are not an adapter fallback. Populate only the intended profile's `.env`. |
| No stream traffic reaches the agent | The bot must be subscribed to the stream; `ZULIP_REQUIRE_MENTION` must be satisfied; `ZULIP_ALLOWED_STREAMS` and the user allowlist can filter traffic. Check the gateway log for dropped or malformed events. |
| Replies work in DMs but not streams | Stream replies need a topic. Use a session that came from a stream message, or target `stream:<name>/<topic>`. |
| Typing indicator never appears | Expected for targets parsed from a stream name (no numeric stream id). A canonical chat id from an inbound message carries the id and works. |
| Cron delivery fails with a topic error | `ZULIP_HOME_CHANNEL` must be a full `stream:<name>/<topic>` target; a stream without a topic is rejected. |

## Bridge API

The wheel also exposes `zul_bridge` for direct use:

```python
import zul_bridge

bridge = zul_bridge.Bridge(
    base_url="https://chat.example.com",   # omit to use libzul config / ZULIP_* env
    email="bot@example.com",
    api_key="...",
    event_types=["message"],               # optional
    narrow=[["channel", "core"]],          # optional
    all_public_streams=False,
    queue_timeout_secs=None,
)
bridge.whoami()                             # {"email": ..., "full_name": ...}
bridge.send_stream("core", "ops", "hello")  # -> message id
bridge.send_direct_ids([7, 8], "hello")
bridge.send_direct_emails(["a@example.com"], "hello")
bridge.typing_stream(12, "ops", "start")
bridge.typing_direct([7, 8], "stop")
event = bridge.next_event(1000)             # dict or None on timeout
bridge.close()
```

Explicit constructor arguments override libzul settings; otherwise
`ZULIP_BASE_URL` (which wins over `ZULIP_SITE`), `ZULIP_EMAIL`, and
`ZULIP_API_KEY` are read.
Missing values raise `ValueError` before any network call, and the API key is
never included in error text. Blocking methods release the GIL. Normalized
events carry `id`, `kind` (`stream`/`dm`), `stream_id`, `stream`, `topic`,
`sender_id`, `sender_name`, `sender_email`, `content`, `plain_text`, `flags`,
`timestamp`, `permalink`, and `dm_participants`. The event queue is bounded at
1024 entries; when a stalled consumer fills it, the poller pauses and the
buffer stays on the server. `close()` stops the poller, deletes the queue once,
clears the queue id, joins the runtime thread, and is idempotent.
