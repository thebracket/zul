//! Core Zulip services for the `zul` command-line and terminal interfaces.
//!
//! `libzul` owns the mechanics that are independent of a particular user
//! interface: configuration discovery, authentication persistence, Zulip API
//! calls, local SQLite persistence, cache queries, message sending safeguards,
//! and digest construction.
//!
//! UI crates should usually start with [`Settings::load`] and [`App::open`],
//! then use [`App`] methods to sync, query, search, digest, or send messages.

#![warn(missing_docs)]

/// High-level application service that coordinates configuration, Zulip, and cache access.
pub mod app;
/// Authentication and saved credential helpers.
pub mod auth;
/// SQLite-backed local cache and persistence primitives.
pub mod cache;
/// Configuration loading, environment overrides, and filesystem paths.
pub mod config;
/// Cached-message digest construction.
pub mod digest;
/// Zulip API client, models, and response normalization.
pub mod zulip;

pub use app::{App, PollResult, ReplyResult, SyncResult};
pub use auth::{login, AuthLogin, AuthStatus, SavedAuth};
pub use config::{
    AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, ThemeConfig, UiConfig, ZulipConfig,
};
