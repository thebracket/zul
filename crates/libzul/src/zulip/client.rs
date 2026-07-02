use crate::zulip::models::{
    CurrentUser, CurrentUserResponse, ImageBytes, MessagesResponse, SendMessageResponse,
    SubscriptionsResponse, ZulipApiResponse, ZulipMessage, ZulipStream,
};
use crate::zulip::normalize;
use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use serde_json::json;
use std::collections::HashSet;

const MAX_IMAGE_BYTES: u64 = 10 * 1024 * 1024;

/// Async Zulip REST client using email/API-key basic authentication.
#[derive(Clone)]
pub struct ZulipClient {
    http: Client,
    base_url: String,
    email: String,
    api_key: String,
}

impl ZulipClient {
    /// Create a client for a Zulip realm and account.
    pub fn new(base_url: String, email: String, api_key: String) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            email,
            api_key,
        }
    }

    #[allow(dead_code)]
    /// Fetch the current Zulip user.
    pub async fn current_user(&self) -> Result<CurrentUser> {
        let data: CurrentUserResponse = self
            .get("/api/v1/users/me")
            .send()
            .await?
            .json::<ZulipApiResponse<CurrentUserResponse>>()
            .await?
            .into_data()?;
        Ok(CurrentUser {
            email: data.email,
            full_name: data.full_name,
        })
    }

    /// Fetch streams subscribed by the current user.
    pub async fn subscriptions(&self) -> Result<Vec<ZulipStream>> {
        let data = self
            .get("/api/v1/users/me/subscriptions")
            .send()
            .await?
            .json::<ZulipApiResponse<SubscriptionsResponse>>()
            .await?
            .into_data()?;
        Ok(data
            .subscriptions
            .into_iter()
            .map(normalize::subscription)
            .collect())
    }

    /// Fetch recent messages newer than the requested lookback window.
    pub async fn recent_messages(&self, hours: i64) -> Result<Vec<ZulipMessage>> {
        let cutoff = chrono::Utc::now() - chrono::Duration::hours(hours);
        let mut anchor = "newest".to_string();
        let mut messages = Vec::new();
        let mut seen = HashSet::new();

        for _ in 0..25 {
            let page = self.messages_page(&anchor, None, 1000, 0, false).await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();

            let mut oldest_id = i64::MAX;
            let mut saw_older_than_cutoff = false;
            let mut added_new = false;

            for raw in page {
                let message = normalize::message(raw).context("normalizing recent messages")?;
                oldest_id = oldest_id.min(message.id);
                if !seen.insert(message.id) {
                    continue;
                }
                added_new = true;
                if message.timestamp >= cutoff {
                    messages.push(message);
                } else {
                    saw_older_than_cutoff = true;
                }
            }

            if saw_older_than_cutoff || oldest_id == i64::MAX || !added_new || page_len < 1000 {
                break;
            }
            anchor = oldest_id.to_string();
        }

        Ok(messages)
    }

    /// Fetch messages newer than a known Zulip message ID.
    pub async fn messages_after(&self, anchor_id: i64) -> Result<Vec<ZulipMessage>> {
        let mut anchor = anchor_id.to_string();
        let mut messages = Vec::new();
        let mut seen = HashSet::new();

        for _ in 0..25 {
            let page = self.messages_page(&anchor, None, 0, 1000, false).await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();

            let mut newest_id = anchor.parse::<i64>().unwrap_or(anchor_id);
            let mut added_new = false;

            for raw in page {
                let message = normalize::message(raw).context("normalizing polled messages")?;
                newest_id = newest_id.max(message.id);
                if seen.insert(message.id) {
                    added_new = true;
                    messages.push(message);
                }
            }

            if !added_new || newest_id.to_string() == anchor || page_len < 1000 {
                break;
            }
            anchor = newest_id.to_string();
        }

        Ok(messages)
    }

    /// Fetch one message by ID, refreshing mutable fields such as reactions.
    pub async fn message_by_id(&self, message_id: i64) -> Result<Option<ZulipMessage>> {
        let mut messages = self
            .messages_page(&message_id.to_string(), None, 0, 0, true)
            .await?
            .into_iter()
            .map(normalize::message)
            .collect::<Result<Vec<_>>>()
            .context("normalizing refreshed Zulip message")?;
        Ok(messages.pop())
    }

    #[allow(dead_code)]
    /// Fetch recent messages narrowed to a stream and optional topic.
    pub async fn stream_messages(
        &self,
        stream: &str,
        topic: Option<&str>,
        hours: i64,
    ) -> Result<Vec<ZulipMessage>> {
        let mut narrow = vec![json!({"operator":"stream","operand":stream})];
        if let Some(topic) = topic {
            narrow.push(json!({"operator":"topic","operand":topic}));
        }
        self.narrow_messages(narrow, hours).await
    }

    #[allow(dead_code)]
    /// Search recent Zulip messages through the Zulip API.
    pub async fn search(&self, query: &str, hours: i64) -> Result<Vec<ZulipMessage>> {
        self.narrow_messages(vec![json!({"operator":"search","operand":query})], hours)
            .await
    }

    #[allow(dead_code)]
    async fn narrow_messages(
        &self,
        narrow: Vec<serde_json::Value>,
        hours: i64,
    ) -> Result<Vec<ZulipMessage>> {
        let narrow = serde_json::to_string(&narrow)?;
        let cutoff = chrono::Utc::now() - chrono::Duration::hours(hours);
        self.messages_page("newest", Some(&narrow), 1000, 0, false)
            .await?
            .into_iter()
            .map(normalize::message)
            .filter(|item| item.as_ref().map(|m| m.timestamp >= cutoff).unwrap_or(true))
            .collect::<Result<Vec<_>>>()
            .context("normalizing Zulip messages")
    }

    async fn messages_page(
        &self,
        anchor: &str,
        narrow: Option<&str>,
        num_before: u16,
        num_after: u16,
        include_anchor: bool,
    ) -> Result<Vec<serde_json::Value>> {
        let num_before = num_before.to_string();
        let num_after = num_after.to_string();
        let include_anchor = if include_anchor { "true" } else { "false" };
        let mut request = self.get("/api/v1/messages").query(&[
            ("anchor", anchor),
            ("num_before", num_before.as_str()),
            ("num_after", num_after.as_str()),
            ("apply_markdown", "true"),
            ("client_gravatar", "false"),
            ("include_anchor", include_anchor),
        ]);
        if let Some(narrow) = narrow {
            request = request.query(&[("narrow", narrow)]);
        }
        let data = request
            .send()
            .await?
            .json::<ZulipApiResponse<MessagesResponse>>()
            .await?
            .into_data()?;
        Ok(data.messages)
    }

    /// Send a stream message and return the created Zulip message ID.
    pub async fn send_stream_message(
        &self,
        stream: &str,
        topic: &str,
        message: &str,
    ) -> Result<i64> {
        let data = self
            .post("/api/v1/messages")
            .form(&[
                ("type", "stream"),
                ("to", stream),
                ("topic", topic),
                ("content", message),
            ])
            .send()
            .await?
            .json::<ZulipApiResponse<SendMessageResponse>>()
            .await?
            .into_data()?;
        Ok(data.id)
    }

    /// Send a direct message and return the created Zulip message ID.
    pub async fn send_direct_message(
        &self,
        recipients: &[serde_json::Value],
        message: &str,
    ) -> Result<i64> {
        let to = serde_json::to_string(recipients)?;
        let data = self
            .post("/api/v1/messages")
            .form(&[("type", "direct"), ("to", &to), ("content", message)])
            .send()
            .await?
            .json::<ZulipApiResponse<SendMessageResponse>>()
            .await?
            .into_data()?;
        Ok(data.id)
    }

    /// Fetch an authenticated image URL for terminal rendering.
    pub async fn image_bytes(&self, absolute_url: &str) -> Result<ImageBytes> {
        let response = self
            .http
            .get(absolute_url)
            .basic_auth(&self.email, Some(&self.api_key))
            .send()
            .await
            .with_context(|| format!("fetching image {absolute_url}"))?
            .error_for_status()
            .with_context(|| format!("fetching image {absolute_url}"))?;

        if let Some(length) = response.content_length() {
            if length > MAX_IMAGE_BYTES {
                anyhow::bail!("image is too large ({length} bytes)");
            }
        }

        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string());
        if let Some(content_type) = content_type.as_deref() {
            if !content_type.starts_with("image/") {
                anyhow::bail!("image URL returned non-image content type {content_type}");
            }
        }

        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("reading image {absolute_url}"))?;
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            anyhow::bail!("image is too large ({} bytes)", bytes.len());
        }

        Ok(ImageBytes {
            bytes: bytes.to_vec(),
            content_type,
        })
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .get(format!("{}{}", self.base_url, path))
            .basic_auth(&self.email, Some(&self.api_key))
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .post(format!("{}{}", self.base_url, path))
            .basic_auth(&self.email, Some(&self.api_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    #[ignore = "requires binding a local mock server port"]
    async fn messages_after_uses_forward_anchor_query() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/messages"))
            .and(query_param("anchor", "10"))
            .and(query_param("num_before", "0"))
            .and(query_param("num_after", "1000"))
            .and(query_param("include_anchor", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "messages": [{
                    "id": 11,
                    "stream_id": 1,
                    "display_recipient": "core",
                    "subject": "ops",
                    "sender_id": 2,
                    "sender_full_name": "Ada",
                    "sender_email": "ada@example.com",
                    "timestamp": 1_700_000_000,
                    "content": "<p>hello</p>"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ZulipClient::new(
            server.uri(),
            "ada@example.com".to_string(),
            "key".to_string(),
        );
        let messages = client.messages_after(10).await.unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, 11);
    }

    #[tokio::test]
    #[ignore = "requires binding a local mock server port"]
    async fn message_by_id_includes_anchor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/messages"))
            .and(query_param("anchor", "10"))
            .and(query_param("num_before", "0"))
            .and(query_param("num_after", "0"))
            .and(query_param("include_anchor", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "messages": [{
                    "id": 10,
                    "stream_id": 1,
                    "display_recipient": "core",
                    "subject": "ops",
                    "sender_id": 2,
                    "sender_full_name": "Ada",
                    "sender_email": "ada@example.com",
                    "timestamp": 1_700_000_000,
                    "content": "<p>hello</p>",
                    "reactions": [{
                        "emoji_name": "thumbs_up",
                        "emoji_code": "1f44d",
                        "reaction_type": "unicode_emoji"
                    }]
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ZulipClient::new(
            server.uri(),
            "ada@example.com".to_string(),
            "key".to_string(),
        );
        let message = client.message_by_id(10).await.unwrap().unwrap();

        assert_eq!(message.id, 10);
        assert_eq!(message.raw_json["reactions"][0]["emoji_name"], "thumbs_up");
    }
}
