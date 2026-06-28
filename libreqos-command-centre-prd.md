# LibreQoS Command Centre PRD

## Summary

Build a lightweight, terminal-first LibreQoS operator tool that starts as a Zulip and GitHub triage console, then grows into a command centre for bugbot-style analysis and safe operational actions.

The first useful version should replace the current ad hoc morning Zulip digest and GitHub watcher scripts with one shared Rust core, a scriptable CLI, a fast TUI, and a local SQLite cache. It should be useful over SSH, safe by default, trace every summary back to source messages/issues, and avoid an Electron wrapper.

## Background

Current working pieces already exist in the OpenClaw workspace:

- `scripts/libreqos_zulip.py` authenticates to Zulip, reads subscribed streams, generates a keyword-based summary, and can send attributed urgent stream messages.
- `scripts/libreqos_github_watch.py` watches open issues and PRs across `LibreQoE/LibreQoS`, `LibreQoE/lts2`, `LibreQoE/libremapper`, and `LibreQoE/lqos_api`.
- Cron jobs deliver a daily Zulip summary at 08:00 America/Chicago and a GitHub watch at 08:10.
- State is currently split across simple script state files and OpenClaw memory.

The next step is to turn these pieces into a standalone operator tool rather than a pile of helper scripts.

## Goals

- Provide a fast daily triage console for LibreQoS Zulip and GitHub activity.
- Offer both a CLI and a TUI backed by the same cache, config, and domain model.
- Make Zulip streams/topics and GitHub issues/PRs searchable, linkable, and digestible.
- Preserve provenance for every summary, triage note, and correlation.
- Support safe outbound actions through draft-review-send workflows.
- Keep the MVP narrow enough that it can be built by an agentic coding loop without becoming a general platform too early.

## Non-Goals

- Do not build an Electron desktop app.
- Do not perform autonomous public posting, issue closing, PR merging, or label changes in v1.
- Do not build full LibreQoS operational control actions in v1.
- Do not attempt multi-user collaboration in v1.
- Do not replace Zulip or GitHub as systems of record.
- Do not make AI-generated summaries authoritative without source links.

## Target Users

- Herbert, as LibreQoS lead developer and overloaded triage point.
- Future LibreQoS maintainers who need a dense terminal workflow for project attention.
- Automation jobs that need stable text/JSON output for digests and alerts.

## Product Shape

The tool should feel closer to `gh`, `lazygit`, or `k9s` than to a chat app:

- Dense, keyboard-driven, and fast.
- Scriptable from cron and shell pipelines.
- Useful over SSH.
- Read-only by default.
- Explicit and careful for any external write.

Working name in this PRD: `lqosctl`.

## MVP Scope

Phase 1 should provide:

- Config and auth for Zulip and GitHub.
- Incremental sync into SQLite for selected Zulip streams/topics and configured GitHub repos.
- CLI digest output for Zulip and GitHub activity.
- JSON output for automation.
- Local search over cached messages/issues.
- TUI reader with inbox, digest list, item detail, and source links.
- Triage heuristics for bug-like reports, unanswered questions, mentions, and new GitHub items.
- Manual handled/saved/snoozed state.
- Draft-only outbound queue for replies/comments, with explicit send approval.

## Existing Integration Requirements

Zulip:

- Credentials may be loaded from env vars for automation:
  - `ZULIP_SITE` or `ZULIP_BASE_URL`
  - `ZULIP_EMAIL`
  - `ZULIP_API_KEY`
- Existing bot can see at least the `core team` stream.
- Existing urgent sends are attributed with:
  - `From Herbert via HBot, his AI assistant:`
- The new tool must preserve explicit attribution for assistant-originated or delegated sends.

GitHub:

- Repos to watch by default:
  - `LibreQoE/LibreQoS`
  - `LibreQoE/lts2`
  - `LibreQoE/libremapper`
  - `LibreQoE/lqos_api`
- Use `GITHUB_TOKEN` or `GH_TOKEN`.
- GitHub access must support private repos visible to Herbert's token.

## Core Workflows

### Morning Triage

1. Run `lqosctl sync`.
2. Open `lqosctl tui` or run `lqosctl inbox`.
3. Review new high-signal Zulip topics, GitHub issues, PR review requests, and bugbot notes.
4. Mark items handled, snooze them, save them, or create local follow-up notes.
5. Optionally create drafts for Zulip replies or GitHub comments.
6. Review and explicitly send approved drafts.

### Digest Generation

1. Run `lqosctl digest --since 24h`.
2. Output grouped activity by source, stream/topic, repo, priority, and likely action.
3. Include source links and item IDs.
4. Support `--json` for cron/OpenClaw integration.

### Zulip Thread Review

1. Select a stream/topic from the TUI or use CLI:
   - `lqosctl zulip topic "core team" "Release 1.6"`
2. Read the message timeline from cache.
3. Refresh on demand.
4. Copy/open source permalink.
5. Draft a reply, but do not send without confirmation.

### GitHub Triage

1. Review new issues/PRs from configured repos.
2. See labels, author, age, review state, and update time.
3. Link related Zulip messages manually or from suggestions.
4. Create local triage notes with suggested type, severity, component, owner, and next action.
5. Create GitHub comment/label drafts for later approval.

### Correlation Review

1. See suggested links between Zulip messages and GitHub items.
2. Inspect why a link was suggested.
3. Accept, reject, or manually create links.
4. Accepted manual links override inferred links.

## CLI Requirements

Representative commands:

```bash
lqosctl auth zulip
lqosctl auth github
lqosctl sync
lqosctl sync zulip --hours 24
lqosctl sync github
lqosctl inbox
lqosctl inbox --json
lqosctl digest --since 24h
lqosctl digest zulip --hours 24
lqosctl search "installer"
lqosctl zulip stream "core team"
lqosctl zulip topic "core team" "Release 1.6"
lqosctl github triage
lqosctl links suggest
lqosctl link zulip:123456 github:issue:LibreQoE/lts2#49
lqosctl outbox list
lqosctl outbox send <action-id>
lqosctl tui
```

CLI behavior:

- Human-readable output by default.
- `--json` for machine-readable output.
- `--dry-run` for write-capable commands.
- `--yes` required for non-interactive mutation.
- Stable exit codes for cron and shell automation.
- No secrets printed in logs or JSON.

## TUI Requirements

Main layout:

- Left pane: sources and filters.
  - Inbox
  - Zulip
  - GitHub
  - Bugbot
  - Saved
  - Snoozed
  - Drafts
  - Sent/Audit
- Center pane: current item list.
- Right pane: selected item detail.
- Bottom bar: sync state, active account, offline/online state, stale data warning, key hints.

Views:

- Inbox view.
- Digest view.
- Zulip stream/topic view.
- GitHub issue/PR triage view.
- Correlation/evidence view.
- Draft/outbox approval view.
- Command palette.

Interaction model:

- Keyboard-first.
- No mouse dependency.
- Fast startup from local cache.
- Explicit refresh/sync command.
- Clear visual distinction between cached and freshly synced data.
- Confirmation modals for any external write.

Command palette examples:

- `sync now`
- `reply`
- `draft zulip reply`
- `draft github comment`
- `mark handled`
- `save for later`
- `snooze until tomorrow`
- `summarize thread`
- `open source`
- `copy permalink`
- `suggest links`
- `accept link`
- `reject link`
- `label bug`
- `assign me`
- `create issue from message`
- `show unread mentions`
- `filter source:github label:bug`
- `run digest 24h`

## Data Model

Suggested core entities:

- `SourceItem`: normalized inbound object from Zulip, GitHub issue, PR, comment, review, CI event, or future bot finding.
- `Thread`: conversation grouping such as Zulip stream/topic, GitHub issue, or PR.
- `WorkItem`: inferred actionable unit backed by one or more source items.
- `Actor`: human, bot, GitHub user, Zulip user, or system identity.
- `Link`: explicit or inferred relationship between entities.
- `TriageNote`: local analysis, severity, category, owner suggestion, duplicate suspicion, and next action.
- `OutboundAction`: draft, approved, sent, failed, or cancelled action.
- `Provenance`: source system, canonical URL, external ID, fetch timestamp, sync cursor, and raw payload hash.

Suggested state categories:

- `remote_mirror`: latest known Zulip/GitHub facts.
- `derived`: correlations, summaries, rankings, and triage suggestions.
- `operator_state`: read, seen, handled, snoozed, pinned, saved.
- `outbox`: proposed or approved outbound changes.
- `audit_log`: every outbound attempt and local triage decision.

## SQLite Cache

Use SQLite as the primary local database.

Suggested tables:

- `zulip_streams`
- `zulip_topics`
- `zulip_messages`
- `github_items`
- `github_comments`
- `source_items`
- `threads`
- `work_items`
- `actors`
- `identity_mappings`
- `links`
- `triage_notes`
- `outbound_actions`
- `audit_log`
- `sync_state`
- `app_metadata`

Store raw JSON payloads where useful for debugging and schema drift. Use migrations from the start.

Search:

- Use SQLite FTS for exact text and keyword search.
- Add vector search later only if summaries/correlation need semantic search beyond FTS.
- Keep explicit links and manual linking higher priority than fuzzy matching.

## Correlation Strategy

Use layered correlation. Do not rely on one magic matcher.

1. Explicit references:
   - GitHub issue/PR URLs in Zulip.
   - `#1234`, `GH-1234`, commit hashes, branch names.
   - Zulip message links in GitHub comments.
2. Identity mapping:
   - Maintain local mappings between Zulip users and GitHub users.
   - Allow confidence levels and manual correction.
3. Text similarity:
   - Match titles, error strings, stack traces, interface names, package names, and LibreQoS terms.
   - Store similarity score and reason.
4. Temporal proximity:
   - Messages shortly before/after issue creation, PR updates, CI failures, or releases.
5. Manual links:
   - CLI/TUI commands to link/unlink items.
   - Manual links override inferred links.

Every inferred relationship must expose confidence and evidence.

## Bugbot-Style Triage

Triage should produce structured local notes, not just prose.

Fields:

- Summary.
- Type: bug, support, enhancement, regression, infra, docs.
- Severity and confidence.
- Affected component.
- Related issues/PRs/Zulip threads.
- Reproduction clues.
- Missing information.
- Suggested labels.
- Suggested owner or reviewer.
- Suggested next action.
- Draft response, if useful.

Default stance:

- Advisory only.
- No automatic external posting.
- Source links shown beside every claim.

## Outbound Safety

Default mode is read-only.

Outbound actions should follow a draft-review-send model:

- Zulip reply draft.
- GitHub issue comment draft.
- Label suggestion.
- Assignment suggestion.
- Close/duplicate recommendation.
- PR review comment draft.
- Bugbot summary comment draft.

Safety requirements:

- No external send without explicit approval.
- Show exact destination, account, message body, and side effects.
- Require stronger confirmation for closing issues, merging PRs, broad label changes, assigning others, or posting to high-visibility Zulip streams.
- Support `--dry-run` everywhere a write might happen.
- Require `--yes` for non-interactive writes.
- Keep an audit log with before/after state.
- Use idempotency keys or local sent-action hashes to avoid duplicate posts.
- Never send generated summaries as if they are human-authored unless explicitly confirmed.

## Recommended Rust Stack

- CLI: `clap` with derive API.
- TUI: `ratatui` and `crossterm`.
- Async runtime: `tokio`.
- HTTP: `reqwest`.
- JSON: `serde`, `serde_json`.
- SQLite: `sqlx` with SQLite for async access, or `rusqlite` if simpler blocking DB access is preferred.
- Config: `figment` or `config`, plus `directories`.
- Secrets: `keyring`, with env-var fallback.
- Dates/times: `time` or `chrono`.
- Logging: `tracing`, `tracing-subscriber`.
- Errors: `anyhow` for application errors, `thiserror` for domain/library errors.
- Tests: `wiremock`, `insta`, `tempfile`, `assert_cmd`, `predicates`.

Recommendation: start with one Rust workspace containing a reusable core library and one binary. Keep Zulip, GitHub, cache, digest, and TUI state separated.

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
    digest.rs

  github/
    mod.rs
    client.rs
    models.rs
    triage.rs

  cache/
    mod.rs
    db.rs
    migrations/
    repositories.rs

  triage/
    mod.rs
    heuristics.rs
    correlation.rs
    notes.rs

  tui/
    mod.rs
    event.rs
    state.rs
    views/
      inbox.rs
      digest.rs
      triage.rs
      command.rs
      outbox.rs

  commands/
    digest.rs
    sync.rs
    search.rs
    triage.rs
    config.rs
    outbox.rs

  script/
    mod.rs
    output.rs
```

## Config and Auth

Config locations:

- Linux config: `$XDG_CONFIG_HOME/lqosctl/config.toml`
- Linux cache: `$XDG_CACHE_HOME/lqosctl/cache.sqlite`

Environment overrides:

- `ZULIP_BASE_URL`
- `ZULIP_SITE`
- `ZULIP_EMAIL`
- `ZULIP_API_KEY`
- `GITHUB_TOKEN`
- `GH_TOKEN`

Example config:

```toml
[zulip]
base_url = "https://example.zulipchat.com"
email = "bot@example.com"
streams = ["core team"]

[cache]
ttl_minutes = 30

[github]
owner = "LibreQoE"
repos = ["LibreQoS", "lts2", "libremapper", "lqos_api"]

[safety]
read_only_by_default = true
require_confirmation_for_writes = true
```

Secrets should use OS keyring where possible. Env-only mode should remain available for servers, cron, and OpenClaw jobs.

## API Client Approach

Zulip:

- Hand-write a small typed client with `reqwest`.
- Support subscribed streams, messages by stream/topic/time window, narrow searches, and send message.
- Normalize messages into app-owned structs before caching.
- Preserve raw JSON optionally for debugging.

GitHub:

- Use `octocrab` or direct REST via `reqwest`.
- Start with issues/PR list, labels, comments, assignees, timestamps, review state, and URLs.
- Keep GitHub integration behind module boundaries so Zulip remains usable alone.

## Testing Strategy

Unit tests:

- Digest grouping.
- Triage heuristics.
- Explicit-link extraction.
- Config loading precedence.
- Cache repositories.
- Outbound safety rules.

Integration tests:

- Zulip client against `wiremock`.
- GitHub client against `wiremock` or fixtures.
- SQLite migrations against temp DB.
- CLI behavior with `assert_cmd`.
- JSON output snapshots with `insta`.

TUI tests:

- State reducer tests first.
- Minimal rendering snapshots for key screens.
- Avoid brittle full-screen golden tests early.

Manual verification:

- Run against a test Zulip realm or recorded fixtures.
- Verify offline mode from cache.
- Verify env-only auth for cron/server use.
- Verify no write command sends without confirmation or `--yes`.

## Phased Roadmap

### Phase 1: Zulip CLI Digest

- Config/auth.
- Zulip fetch for configured streams.
- SQLite cache and migrations.
- `sync`, `digest`, and `search`.
- Human and JSON output.
- Preserve source message IDs and permalinks.

### Phase 2: TUI Reader

- Ratatui shell.
- Inbox/digest list.
- Topic/message detail view.
- Manual refresh.
- Fast offline startup from cache.
- Handled/saved/snoozed local state.

### Phase 3: Triage Layer

- Heuristics for urgent items, unanswered questions, mentions, and bug-like reports.
- Stored local triage notes.
- Command palette.
- Exportable summaries.
- Audit log foundation.

### Phase 4: GitHub Integration

- GitHub issue/PR sync.
- New item detection replacing the current GitHub watcher script.
- Issue/PR triage panels.
- Manual and suggested Zulip/GitHub links.
- Labels/status awareness.

### Phase 5: Draft Outbox and Safe Sends

- Zulip reply drafts.
- GitHub comment drafts.
- Outbox review view.
- Explicit send approval.
- `--dry-run` and `--yes` CLI paths.
- Sent-action hashes to avoid duplicates.

### Phase 6: Command Centre

- Optional local daemon or Unix socket API.
- Background sync jobs.
- Deeper bugbot analysis.
- CI/check views.
- Future LibreQoS operational views and runbooks.

## Success Metrics

- Herbert can run one command to get a trustworthy daily LibreQoS digest.
- The TUI starts quickly from cache and makes source review faster than opening Zulip and GitHub separately.
- Summaries consistently include links back to source messages/issues.
- The tool can replace the current Zulip summary and GitHub watcher scripts for read-only daily triage.
- No write action can be sent accidentally in normal use.
- JSON output is stable enough for cron/OpenClaw integration.

## Key Risks

- Scope creep from "triage console" into "everything command centre".
- Incorrect correlation between Zulip and GitHub items.
- AI summaries overstating certainty.
- Local cache drift and stale data confusion.
- Permissions mismatch between Zulip and GitHub actions.
- Terminal UX becoming more complex than the web apps.
- Accidental public sends from fast keyboard workflows.

## Open Questions

- Should the binary be named `lqosctl`, `libreqos`, or something else?
- Should v1 include GitHub from the start, or should Zulip CLI/TUI land first?
- Should the initial SQLite layer use `sqlx` or `rusqlite`?
- Should the tool have a background daemon early, or should sync remain explicit until the TUI is useful?
- Which Zulip streams/topics should be default subscriptions for the LibreQoS workflow?
- What are the first triage labels/categories Herbert actually wants to see daily?
- Should AI/bugbot triage be local-model only by default, or provider-configurable?

## Suggested First Coding Loop Prompt

Build Phase 1 of `lqosctl`: a Rust CLI for Zulip digest/search backed by SQLite.

Requirements:

- Create a Rust workspace with one binary, `lqosctl`.
- Use `clap`, `tokio`, `reqwest`, `serde`, `tracing`, and SQLite.
- Load Zulip config from env vars and a config file.
- Implement `sync zulip --hours <n>`, `digest zulip --hours <n>`, and `search <query>`.
- Cache Zulip streams, topics, messages, sender, timestamps, normalized text, source IDs, and raw JSON.
- Provide human output and `--json`.
- Include `--dry-run` plumbing for future write commands, even if v1 is read-only.
- Add tests for config loading, digest grouping, cache migrations, and CLI JSON output.
- Do not implement external sends in the first loop.
