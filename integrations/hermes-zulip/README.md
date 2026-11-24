# Hermes Zulip bridge

PyO3 extension module (`zul_bridge`) that gives the Hermes Zulip adapter a
blocking Python API over `libzul`'s async Zulip client.

This is a standalone crate: it is intentionally not a member of the repository
workspace, so `cargo build --workspace` does not need Python headers.

## Build

```sh
cd integrations/hermes-zulip
uv venv .venv
uv pip install --python .venv/bin/python maturin
.venv/bin/maturin build --release
uv pip install --python .venv/bin/python target/wheels/zul_bridge-*.whl
```

The wheel is abi3 for CPython 3.11 and newer (built here against 3.14). The
`extension-module` feature is enabled only by maturin; a plain `cargo test`
keeps libpython linked so the Rust test harness can start.  Maturin's mixed
layout installs the compiled module as `zul_bridge.zul_bridge` inside the
`python/zul_bridge/` package, whose `__init__.py` re-exports the public API so
`import zul_bridge` keeps working.

## Hermes plugin

`python/hermes_zulip/` is a Hermes platform plugin. It imports `zul_bridge`
defensively, so `hermes plugins list` and `hermes config` work before the wheel
is installed; the adapter only refuses to connect until it is.

Install the bridge wheel into the Python environment Hermes runs under, then
link or copy the plugin directory into the user plugin directory:

```sh
# in the environment Hermes uses (its venv, pipx env, ...)
uv pip install target/wheels/zul_bridge-*.whl

mkdir -p ~/.hermes/plugins
ln -sfn "$PWD/python/hermes_zulip" ~/.hermes/plugins/zulip
hermes plugins enable zulip-platform
```

Set the required env values (in `~/.hermes/.env` or the profile's env):

```sh
ZULIP_SITE=https://chat.example.com   # ZULIP_BASE_URL is also accepted
ZULIP_EMAIL=bot@example.com
ZULIP_API_KEY=...
```

Optional settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `ZULIP_REQUIRE_MENTION` | `true` | In streams, only messages that mention the bot reach the agent |
| `ZULIP_ALLOWED_STREAMS` | empty (all) | Comma-separated stream names, case-insensitive |
| `ZULIP_ALLOWED_USERS` | empty | Comma-separated user ids/emails allowed to talk to the bot |
| `ZULIP_ALLOW_ALL_USERS` | false | Allow every Zulip user (overrides the allowlist) |
| `ZULIP_HOME_CHANNEL` | empty | Cron delivery target: `stream:<stream>/<topic>`, `@<email>`, or a canonical chat id |
| `ZULIP_MAX_MESSAGE_LENGTH` | `10000` | Split outbound messages above this length |
| `ZULIP_QUEUE_TIMEOUT_SECS` | bridge default | Optional idle event-queue timeout |

Restart the gateway after changing env. Streams map to Hermes group sessions
with the topic as `thread_id`; DMs map to DM sessions. Outbound targets accept a
canonical chat id, `stream:<name>/<topic>` / `#<name>/<topic>`, or `@<email>`.
The adapter is text-only in this version: media sends return an error instead of
uploading.

Run the Python adapter tests (they use a local Hermes stub by default; set
`HERMES_REPO` to also run the real-checkout plugin-load test):

```sh
uv pip install --python .venv/bin/python pytest pytest-asyncio PyYAML
PYTHONPATH=python .venv/bin/python -m pytest tests -q
```

## Test

```sh
cargo fmt --manifest-path integrations/hermes-zulip/Cargo.toml -- --check
cargo clippy --manifest-path integrations/hermes-zulip/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path integrations/hermes-zulip/Cargo.toml
```

Wiremock covers account lookup, stream and direct sends, typing notifications,
event normalization, long-poll timeout, queue re-registration, shutdown queue
deletion, and sending while an event long-poll is in flight.

## API

```python
import zul_bridge

bridge = zul_bridge.Bridge(
    base_url="https://chat.example.com",
    email="bot@example.com",
    api_key="...",                 # or omit to use libzul config / ZULIP_* env
    event_types=["message"],       # optional
    narrow=[["channel", "core"]],  # optional
    all_public_streams=False,
    queue_timeout_secs=None,
)
bridge.whoami()                    # {"email": ..., "full_name": ...}
bridge.send_stream("core", "ops", "hello")      # -> message id
bridge.send_direct_ids([7, 8], "hello")         # -> message id
bridge.send_direct_emails(["a@example.com"], "hello")
bridge.typing_stream(12, "ops", "start")
bridge.typing_direct([7, 8], "stop")
event = bridge.next_event(1000)    # dict or None on timeout
bridge.close()
```

Explicit constructor arguments override libzul settings; otherwise
`ZULIP_BASE_URL`/`ZULIP_SITE`, `ZULIP_EMAIL`, and `ZULIP_API_KEY` are used.
Missing required values raise `ValueError` before any network call. The API key
is never included in error text.

All blocking methods release the GIL while they wait. One background thread runs
a current-thread tokio runtime: commands return through oneshot replies, and the
event poller long-polls `get_events`, re-registering with bounded backoff after
request errors. `next_event` is single-consumer (`RuntimeError` after repeated
poller failures); `close()` stops the poller, deletes the queue best effort, and
is idempotent.

Event dictionaries contain `id`, `kind` (`"stream"`/`"dm"`), `stream_id`,
`stream`, `topic`, `sender_id`, `sender_name`, `sender_email`, `content`,
`plain_text`, `flags`, `timestamp`, `permalink`, and `dm_participants`.
`content` is the message content carried by the event payload (libzul registers
with `apply_markdown=true`, so Zulip supplies rendered content); `plain_text` is
the extracted text. Non-message and malformed message events are skipped without
stopping the poller.
