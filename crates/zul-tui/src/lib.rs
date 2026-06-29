//! Terminal user interface for browsing and replying to cached Zulip messages.

mod tui;

pub use tui::{run, run_with_startup_credentials, StartupCredentials};
