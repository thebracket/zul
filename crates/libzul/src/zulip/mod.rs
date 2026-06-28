//! Zulip API types, client calls, and response normalization.

/// Async HTTP client for Zulip REST endpoints.
pub mod client;
/// Domain and wire models used by Zulip API calls.
pub mod models;
/// Helpers for converting Zulip JSON responses into cached domain models.
pub mod normalize;
