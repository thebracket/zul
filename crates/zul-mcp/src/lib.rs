//! `zul-mcp` serves Zulip data to MCP clients over newline-delimited JSON-RPC
//! on stdio.
//!
//! This library exposes the transport-independent pieces: JSON-RPC envelopes
//! ([`protocol`]), request dispatch and concurrency ([`server`]), the tool
//! catalog and handlers ([`tools`]), and MCP Events discovery and polling
//! ([`events`]). The binary in `main.rs` owns stdin/stdout framing and the
//! event loop.

/// MCP Events discovery and stateless poll delivery.
pub mod events;
/// JSON-RPC 2.0 wire shapes and the MCP `_meta` version gate.
pub mod protocol;
/// Request dispatch, concurrency, and the active-request registry.
pub mod server;
/// Tool catalog and handlers, independent of the JSON-RPC transport.
pub mod tools;
