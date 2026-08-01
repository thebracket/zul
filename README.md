# zul

A Rust library to help access Zulip, along with a TUI for using Zulip on the go.

## Crates

### libzul

Core services for Zulip: an async API client, configuration and credential handling, local SQLite caching, message digests, and real-time event subscriptions.

### zul-cli

A terminal-first command-line client built on libzul. It supports authentication, syncing, streams, topics, search, digests, replies, and listening to channels.

### zul-tui

A full-screen Ratatui interface built on libzul for browsing streams, topics, and messages interactively.

## AI Usage Disclosure

This crate was largely created with LLM coding agents, and then fixed by hand.
