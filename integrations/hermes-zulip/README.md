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
| `ZULIP_REQUIRE_MENTION` | `true` | In streams, only messages that mention the bot (event flags or rendered `@Bot Name`) reach the agent |
| `ZULIP_ALLOWED_STREAMS` | empty (all) | Comma-separated stream names, case-insensitive |
| `ZULIP_ALLOWED_USERS` | empty | Comma-separated user ids/emails allowed to talk to the bot |
| `ZULIP_ALLOW_ALL_USERS` | false | Allow every Zulip user (overrides the allowlist) |
| `ZULIP_HOME_CHANNEL` | empty | Cron delivery target: `stream:<stream>/<topic>`, `@<email>`, or a canonical chat id |
| `ZULIP_MAX_MESSAGE_LENGTH` | `10000` | Split outbound messages above this length |
| `ZULIP_QUEUE_TIMEOUT_SECS` | bridge default | Optional idle event-queue timeout |

Restart the gateway after changing env. Streams map to Hermes group sessions
with the topic as `thread_id`; DMs map to DM sessions. Outbound targets accept a
canonical chat id, `stream:<name>/<topic>` / `#<name>/<topic>`, or `@<email>`.
Typing notifications need a canonical stream id: a target parsed from
`stream:<name>/<topic>` carries id 0 and is a documented typing no-op. The
adapter is text-only in this version: media sends return an error instead of
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
event normalization (including event-level message flags), long-poll timeout,
bounded event delivery, backpressure and a dropped consumer, non-default queue
options, queue re-registration and its delete-before-register order, the fatal
poller and registration paths, shutdown queue deletion, and sending while an
event long-poll is in flight.

The committed Python smoke test is not collected by pytest (its name does not
start with `test_`). It starts a local stdlib HTTP fake Zulip server, imports the
installed extension, and checks import/version, `whoami`, sending, typing, the
long-poll timeout, GIL release while `next_event` waits, in `Bridge()` and in
`config_status()` against a multi-MB settings file, close, and the
missing-config error. Build and install the wheel into a CPython 3.14
environment first:

```sh
cd integrations/hermes-zulip
uv venv --python 3.14 .venv
uv pip install --python .venv/bin/python maturin
.venv/bin/maturin build --release
uv pip install --python .venv/bin/python target/wheels/zul_bridge-*.whl
.venv/bin/python tests/python_smoke.py
```

The smoke test points `HOME`/XDG at a temporary directory and clears `ZULIP_*`,
so an ambient libzul config cannot change its result.

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
Missing required values raise `ValueError` before any network call. If libzul's
settings file cannot be loaded, that sanitized cause is appended to the same
error when explicit values are incomplete. The API key is never included in
error text.

All blocking methods, including the constructor and `config_status`, release the
GIL while they wait. One background thread runs
a current-thread tokio runtime: commands return through oneshot replies, and the
event poller long-polls `get_events`, retrying the same queue with bounded
backoff on request errors. A queue that the server rejects as invalid, or that
fails three consecutive polls, is deleted best effort and replaced with a fresh
registration; the replacement starts at a new server cursor, so events produced
during that gap are not delivered. Five consecutive failures end the poller and
surface as `RuntimeError` from `next_event`. `next_event` is single-consumer;
`close()` stops the poller, deletes the queue once, clears the queue id, and
joins the runtime thread.

Normalized events buffer in a bounded channel of 1024 entries. When a stalled
consumer fills it, the poller pauses briefly and retries instead of blocking the
runtime or dropping events; while it is paused no new pages are read, so the
buffer stays on the Zulip server rather than growing process memory. Draining
`next_event` resumes delivery.

Event dictionaries contain `id`, `kind` (`"stream"`/`"dm"`), `stream_id`,
`stream`, `topic`, `sender_id`, `sender_name`, `sender_email`, `content`,
`plain_text`, `flags`, `timestamp`, `permalink`, and `dm_participants`.
`content` is the message content carried by the event payload (libzul registers
with `apply_markdown=true`, so Zulip supplies rendered content); `plain_text` is
the extracted text. libzul builds both from the same inner `content` field, so
raw and rendered content are value-identical by construction and no test can
discriminate between them. This rendered-content behavior is an accepted
limitation; consumers that need plain text should read `plain_text`. `flags`
comes from the event-level `flags` array Zulip sends beside `message` (the inner
form is accepted as a fallback). Non-message and malformed message events are
skipped without stopping the poller.
