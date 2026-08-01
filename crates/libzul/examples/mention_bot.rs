//! A minimal bot that replies "I'm here" whenever it is @-mentioned.
//!
//! It subscribes to a stream (default `general`), watches for messages that
//! mention the API user, and replies in the same thread.
//!
//! Credentials come from `ZULIP_BASE_URL`, `ZULIP_EMAIL`, and `ZULIP_API_KEY`
//! (exported, loaded from a `.env` file, or saved earlier via
//! `zul-cli auth login`).
//!
//! Usage:
//! ```text
//! cargo run --example mention_bot -- [stream]
//! ```

use anyhow::{Context, Result};
use libzul::zulip::client::ZulipClient;
use libzul::zulip::events::ChannelSubscriptionOptions;
use libzul::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let stream = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "general".to_string());

    let _ = dotenvy::dotenv();
    let (base_url, email, api_key) = Settings::load()?.require_auth()?;
    let client = ZulipClient::new(base_url, email, api_key);

    let mut subscription = client
        .subscribe_channel(ChannelSubscriptionOptions::new(&stream))
        .await?;
    println!("Listening for @mentions in #{stream}...");

    while let Some(message) = subscription.recv().await {
        let message = message.context("receiving message")?;
        let mentioned = message
            .raw_json
            .get("mentioned")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        if !mentioned {
            continue;
        }

        let reply_id = client
            .send_stream_message(&message.stream_name, &message.topic, "I'm here")
            .await?;
        println!("Replied to {} (message {reply_id})", message.sender_name);
    }

    Ok(())
}
