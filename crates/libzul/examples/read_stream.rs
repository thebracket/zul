//! Read recent messages from a stream, optionally narrowed to one topic.
//!
//! Credentials come from `ZULIP_BASE_URL`, `ZULIP_EMAIL`, and `ZULIP_API_KEY`
//! (exported, loaded from a `.env` file, or saved earlier via
//! `zul-cli auth login`).
//!
//! Usage:
//! ```text
//! cargo run --example read_stream -- <stream> [topic] [hours]
//! ```

use anyhow::{Context, Result};
use libzul::zulip::client::ZulipClient;
use libzul::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let stream = std::env::args()
        .nth(1)
        .context("usage: read_stream <stream> [topic] [hours]")?;
    let topic = std::env::args().nth(2);
    let hours: i64 = std::env::args()
        .nth(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(24);

    let _ = dotenvy::dotenv();
    let (base_url, email, api_key) = Settings::load()?.require_auth()?;
    let client = ZulipClient::new(base_url, email, api_key);

    let messages = client
        .stream_messages(&stream, topic.as_deref(), hours)
        .await
        .with_context(|| format!("reading #{stream}"))?;

    for message in messages {
        println!(
            "[{}] {} ({}) {}",
            message.id, message.sender_name, message.topic, message.plain_text
        );
    }

    Ok(())
}
