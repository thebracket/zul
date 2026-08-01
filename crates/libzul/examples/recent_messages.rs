//! Fetch all messages from the last N hours (default 24) and print them.
//!
//! Credentials come from `ZULIP_BASE_URL`, `ZULIP_EMAIL`, and `ZULIP_API_KEY`
//! (exported, loaded from a `.env` file, or saved earlier via
//! `zul-cli auth login`).
//!
//! Usage:
//! ```text
//! cargo run --example recent_messages -- [hours]
//! ```

use anyhow::Result;
use libzul::zulip::client::ZulipClient;
use libzul::Settings;

#[tokio::main]
async fn main() -> Result<()> {
    let hours: i64 = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(24);

    let _ = dotenvy::dotenv();
    let (base_url, email, api_key) = Settings::load()?.require_auth()?;
    let client = ZulipClient::new(base_url, email, api_key);

    let messages = client.recent_messages(hours).await?;
    println!("{} message(s) in the last {hours} hour(s):", messages.len());

    for message in messages {
        println!(
            "[{}] #{} > {} ({}) {}",
            message.id, message.stream_name, message.topic, message.sender_name, message.plain_text
        );
    }

    Ok(())
}
