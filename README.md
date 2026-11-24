# zul

A Rust library to help access Zulip, along with a TUI for using Zulip on the go. It can handle most Zulip operations, and includes
a nice TUI zulip client (that supports image rendering in terminals that support it) and a stdio MCP server for agent integration.
Not a heavily supported system; I created it because I needed it for a project.

<img width="1440" height="804" alt="image" src="https://github.com/user-attachments/assets/07062395-08c0-4d7d-a4d2-a156b57d75ca" />

## Crates

### libzul

Core services for Zulip: an async API client, configuration and credential handling, local SQLite caching, message digests, and real-time event subscriptions.

### zul-cli

A terminal-first command-line client built on libzul. It supports authentication, syncing, streams, topics, search, digests, replies, and listening to channels.

### zul-tui

A full-screen Ratatui interface built on libzul for browsing streams, topics, and messages interactively.

### zul-mcp

A stdio Model Context Protocol server built on libzul. It implements MCP 2026-07-28, exposes Zulip read, search, and send tools (sends are dry-run unless explicitly confirmed), and advertises the MCP Events extension with a `zulip.message` event so a capable host can wake an agent when a message arrives. It reuses the same configuration and credentials as the other crates. See [`crates/zul-mcp/README.md`](crates/zul-mcp/README.md) for client setup, the tool reference, and watch/automation examples.

## AI Usage Disclosure

This crate was largely created with LLM coding agents, and then fixed by hand.
