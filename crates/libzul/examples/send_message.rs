//! Connect to a Zulip realm with an API bot and send one stream message.
//!
//! Credentials come from `ZULIP_BASE_URL`, `ZULIP_EMAIL`, and `ZULIP_API_KEY`
//! (exported, loaded from a `.env` file, or saved earlier via
//! `zul-cli auth login`).
//!
//! Usage:
//! ```text
//! cargo run --example send_message -- [stream] [topic] [content]
//! ```

use anyhow::{Context, Result};
use libzul::zulip::client::ZulipClient;
use libzul::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let stream = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "general".to_string());
    let topic = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "examples".to_string());
    let content = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "Hello from libzul!".to_string());

    let _ = dotenvy::dotenv();
    let (base_url, email, api_key) = Settings::load()?.require_auth()?;
    let client = ZulipClient::new(base_url, email, api_key);

    let message_id = client
        .send_stream_message(&stream, &topic, &content)
        .await
        .with_context(|| format!("sending to #{stream} > {topic}"))?;
    println!("Sent message {message_id} to #{stream} > {topic}");

    Ok(())
}
