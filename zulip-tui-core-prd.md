# Zulip TUI/Core PRD

## Summary

Build a lightweight, terminal-first Zulip client with a reusable Rust core, a scriptable CLI, and a fast TUI. The first version should focus only on Zulip reading, digest/search, local cache, and safe draft/reply mechanics. It should be useful over SSH, avoid Electron, and provide a clean base for future LibreQoS-specific triage features without requiring them now.

Working binaries in this PRD: `zul-cli` for scriptable CLI workflows and `zul-tui` for the terminal UI.

## Background

Herbert wants a Zulip command-line and TUI experience that is actually maintained and pleasant to use. Zulip's stream/topic/message model maps well to a terminal UI, and the API is approachable enough for a focused client.

Current local seed:

- `scripts/libreqos_zulip.py` can authenticate to Zulip, list subscriptions, read stream messages, render a simple digest, and send attributed stream messages.
- Credentials currently live in `/home/herbert/.openclaw/secrets/libreqos-zulip.env`.
- The existing helper is useful, but it is a script rather than a proper reusable client core.

This PRD intentionally scopes out GitHub, bugbot, and the larger command-centre idea. Those can be future consumers of the same Zulip cache/core.

## Goals

- Provide a fast terminal client for Zulip streams, topics, messages, search, and digest review.
- Share one Rust core between CLI and TUI.
- Cache Zulip data locally in SQLite for fast startup and offline read-only use.
- Support script-friendly commands with human and JSON output.
- Make the TUI keyboard-first, dense, and usable over SSH.
- Make outbound sends explicit and safe.
- Keep the first coding loop small enough for reliable agentic implementation.

## Non-Goals

- Do not implement GitHub integration in v1.
- Do not implement bugbot, AI triage, or cross-system correlation in v1.
- Do not build an Electron or web wrapper.
- Do not implement a background daemon in v1.
- Do not attempt to replace every Zulip web feature.
- Do not auto-send generated content.
- Do not manage LibreQoS operational actions.

## Target Users

- Herbert, primarily over terminal/SSH.
- Any technical Zulip user who prefers a keyboard-driven workflow.
- Cron or automation jobs that need stable digest/search output.

## Product Shape

The tool should feel more like `gh`, `lazygit`, or `k9s` than a general chat app:

- Fast startup.
- Strong keyboard navigation.
- Narrow, composable CLI commands.
- Local cache first, explicit sync/refresh.
- Read-only by default.
- Writes require clear destination preview and confirmation.

## MVP Scope

The first useful version should provide:

- Config and auth for one Zulip realm.
- Sync of subscribed streams and recent messages.
- Local SQLite cache.
- CLI commands for sync, list streams, list topics, read topic, search, and digest.
- TUI with stream list, topic list, message thread, detail/status bar, and command palette.
- Offline read-only mode from cache.
- Draft reply composer and guarded send path.
- Human output and `--json` for CLI commands.

## CLI Requirements

Representative commands:

```bash
zul-cli auth login
zul-cli auth status
zul-cli sync --hours 24
zul-cli streams
zul-cli topics "core team"
zul-cli topic "core team" "Release 1.6"
zul-cli search "installer"
zul-cli digest --hours 24
zul-cli reply --stream "core team" --topic "Release 1.6" --message "Looking now."
zul-tui
```

CLI behavior:

- Human-readable text by default.
- `--json` for machine-readable output.
- `--dry-run` for write-capable commands.
- `--yes` required for non-interactive sends.
- Stable exit codes for scripts and cron.
- No secrets printed in logs or output.
- Useful errors when auth/config is missing.

## TUI Requirements

Main layout:

- Left pane: streams with unread/local activity counts.
- Middle pane: topics for the selected stream, sorted by activity.
- Right pane: message thread for the selected topic.
- Bottom bar: sync state, active account, selected realm, offline/online state, key hints, pending draft state.

Core views:

- Stream/topic browser.
- Topic thread reader.
- Search results.
- Digest view.
- Draft reply composer.
- Config/auth status view.
- Command palette.

Keyboard model:

- `j`/`k` or arrows: move selection.
- `h`/`l` or tab/shift-tab: change pane focus.
- `/`: search.
- `r`: draft reply.
- `R`: refresh/sync current view.
- `o`: open source URL in browser or print/copy permalink.
- `y`: copy permalink where clipboard support exists.
- `:` or `Ctrl-P`: command palette.
- `q`: back/quit, depending on current depth.

Command palette examples:

- `sync now`
- `open stream`
- `open topic`
- `search`
- `reply`
- `save draft`
- `discard draft`
- `send draft`
- `copy permalink`
- `open in browser`
- `mark read`
- `show unread`
- `digest 24h`
- `toggle offline`

The TUI should not require mouse support.

## Zulip API Requirements

Start with a hand-written typed client using `reqwest`.

Required endpoints/capabilities:

- Get current user/auth sanity check.
- List subscribed streams.
- Fetch messages by stream and optional topic.
- Fetch recent messages by time window.
- Run Zulip narrow searches.
- Send stream message to a stream/topic.

Useful later:

- Register event queue for live updates.
- Mark messages read/unread.
- Topic move/rename handling.
- Edit/delete awareness.
- Direct/private messages.

Normalize API responses into app-owned structs before caching. Preserve raw JSON optionally for debugging and schema drift.

## Config and Auth

Config locations:

- Linux config: `$XDG_CONFIG_HOME/zul/config.toml`
- Linux cache: `$XDG_CACHE_HOME/zul/cache.sqlite`

Environment overrides:

- `ZULIP_BASE_URL` or `ZULIP_SITE`
- `ZULIP_EMAIL`
- `ZULIP_API_KEY`

Example config:

```toml
[zulip]
base_url = "https://example.zulipchat.com"
email = "bot@example.com"

[cache]
ttl_minutes = 30
default_sync_hours = 24

[ui]
start_view = "streams"

[safety]
read_only_by_default = true
require_confirmation_for_sends = true
```

Secrets:

- Prefer OS keyring where available.
- Support env-only mode for servers and cron.
- Never write secrets to logs, JSON output, or crash dumps.

## Local Cache

Use SQLite with migrations from the start.

Suggested tables:

- `zulip_streams`
- `zulip_topics`
- `zulip_messages`
- `zulip_users`
- `drafts`
- `sync_state`
- `read_state`
- `app_metadata`
- `audit_log`

Message fields:

- Zulip message ID.
- Stream ID/name.
- Topic.
- Sender ID/name/email if available.
- Timestamp.
- Rendered HTML.
- Normalized plain text.
- Raw JSON blob where useful.
- Permalink, if available or constructible.
- Last fetched/updated timestamp.

Cache behavior:

- TUI starts from cache immediately.
- Sync is explicit in v1.
- Offline mode is read-only.
- Search works over cached plain text.
- Store enough metadata to avoid duplicate inserts.
- Keep schema simple enough to evolve.

Search:

- Use SQLite FTS for v1.
- Search command should support all cached messages.
- Later semantic/vector search can be added, but is not required for v1.

## Digest Requirements

`zul-cli digest --hours 24` should produce a useful summary without AI requirements:

- Activity grouped by stream/topic.
- Likely bug/help/question items using simple keyword heuristics.
- Mentions of configured names/aliases.
- Most active topics.
- Recent unanswered questions where detectable.
- Source message IDs and permalinks.

Output:

- Human text by default.
- JSON with stable fields under `--json`.

## Outbound Safety

Default mode is read-only.

Reply/send flow:

1. User invokes reply.
2. Composer shows exact destination:
   - realm
   - stream
   - topic
   - account identity
3. User writes or edits message.
4. Tool shows preview.
5. User explicitly confirms send.

CLI sends:

- `--dry-run` prints destination and content.
- Non-interactive send requires `--yes`.
- Sent actions are recorded in `audit_log`.
- Avoid duplicate sends with a local sent-action hash.

If assistant-generated content is ever added later, it must be draft-only by default and visibly attributed where appropriate.

## Recommended Rust Stack

- CLI: `clap` with derive API.
- TUI: `ratatui` and `crossterm`.
- Async runtime: `tokio`.
- HTTP: `reqwest`.
- JSON: `serde`, `serde_json`.
- SQLite: `sqlx` with SQLite or `rusqlite`.
- Config: `figment` or `config`, plus `directories`.
- Secrets: `keyring`, with env-var fallback.
- Dates/times: `time` or `chrono`.
- Logging: `tracing`, `tracing-subscriber`.
- Errors: `anyhow` for app errors, `thiserror` for domain/library errors.
- Tests: `wiremock`, `insta`, `tempfile`, `assert_cmd`, `predicates`.

Recommendation: use one Rust workspace with a reusable core library and one binary.

## Proposed Module Layout

```text
src/
  main.rs
  cli.rs
  config.rs
  auth.rs
  app.rs

  zulip/
    mod.rs
    client.rs
    models.rs
    endpoints.rs
    normalize.rs

  cache/
    mod.rs
    db.rs
    migrations/
    repositories.rs

  digest/
    mod.rs
    heuristics.rs
    render.rs

  tui/
    mod.rs
    event.rs
    state.rs
    views/
      streams.rs
      topics.rs
      thread.rs
      search.rs
      digest.rs
      composer.rs
      command.rs

  commands/
    auth.rs
    sync.rs
    streams.rs
    topics.rs
    search.rs
    digest.rs
    reply.rs
    tui.rs

  output/
    mod.rs
    json.rs
    text.rs
```

## Testing Strategy

Unit tests:

- Config loading precedence.
- Zulip response normalization.
- Digest grouping and keyword heuristics.
- Plain-text extraction from Zulip HTML.
- Cache repository behavior.
- Send safety rules.

Integration tests:

- Zulip client against `wiremock`.
- SQLite migrations against temp DB.
- CLI behavior with `assert_cmd`.
- JSON output snapshots with `insta`.

TUI tests:

- State reducer/navigation tests first.
- Minimal rendering snapshots for key views.
- Avoid brittle full-screen golden tests early.

Manual verification:

- Run against a test Zulip realm or recorded fixtures.
- Verify offline mode from cache.
- Verify env-only auth for cron/server use.
- Verify no send can happen without confirmation or `--yes`.

## Phased Roadmap

### Phase 1: CLI Core and Cache

- Rust workspace and binary.
- Config/auth loading.
- Zulip typed client.
- SQLite cache/migrations.
- `sync`, `streams`, `topics`, `topic`, `search`, `digest`.
- Human and JSON output.
- Tests for config, cache, client, digest, and CLI output.

### Phase 2: TUI Reader

- Ratatui shell.
- Stream/topic/thread panes.
- Cache-first startup.
- Manual sync/refresh.
- Search view.
- Digest view.
- Basic keybindings.

### Phase 3: Draft Composer and Safe Send

- Draft reply composer.
- CLI reply with `--dry-run` and `--yes`.
- TUI send preview and confirmation.
- Audit log.
- Duplicate-send guard.

### Phase 4: Better Zulip Fidelity

- Event queue/live-ish updates.
- Mark read/unread.
- Topic rename/move handling.
- Edit/delete awareness.
- Better permalinks and browser/clipboard integration.

### Phase 5: Future Extensions

- LibreQoS-specific digest profile.
- GitHub correlation.
- Bugbot-style triage.
- AI summary drafts.
- Background daemon.

These are intentionally outside the core PRD.

## Success Metrics

- `zul-cli digest --hours 24` can replace the current Zulip summary script for read-only digest use.
- `zul-tui` starts quickly from cache and makes stream/topic review faster than opening Zulip web for routine triage.
- Cached search finds known messages reliably.
- Human and JSON outputs are stable enough for cron/OpenClaw integration.
- No send action can happen accidentally in normal use.
- The codebase has a clean core/TUI separation that can support future LibreQoS-specific features.

## Key Risks

- TUI scope creep before CLI/cache core is solid.
- Zulip read state and topic rename behavior becoming subtle.
- Terminal UX becoming harder than the web app.
- Accidental sends from fast keyboard workflows.
- Cache drift if sync status is not obvious.
- Over-building future command-centre architecture before the Zulip client is pleasant.

## Open Questions

- Should the binaries stay named `zul-cli` and `zul-tui`, or should a future release add shorter aliases?
- Should v1 support multiple Zulip realms or only one?
- Should v1 include sends, or defer all writes until the reader is solid?
- Should SQLite use `sqlx` or `rusqlite`?
- Should local read/handled state stay separate from Zulip read state in v1?
- What are Herbert's preferred default streams and aliases for digest mentions?

## Suggested First Coding Loop Prompt

Build Phase 1 of `zul-cli`: a Rust CLI core for Zulip sync, digest, and search backed by SQLite.

Requirements:

- Create a Rust workspace with separate `zul-cli` and `zul-tui` binaries.
- Use `clap`, `tokio`, `reqwest`, `serde`, `tracing`, and SQLite.
- Load Zulip config from env vars and a config file.
- Implement `auth status`, `sync --hours <n>`, `streams`, `topics <stream>`, `topic <stream> <topic>`, `search <query>`, and `digest --hours <n>`.
- Cache Zulip streams, topics, messages, sender, timestamps, normalized plain text, source IDs, and raw JSON.
- Provide human output and `--json`.
- Add SQLite migrations from the start.
- Include `--dry-run` plumbing for future write commands, but do not implement sends in the first loop.
- Add tests for config loading, Zulip normalization, digest grouping, cache migrations, and CLI JSON output.
