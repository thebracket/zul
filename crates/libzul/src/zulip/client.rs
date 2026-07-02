use crate::zulip::events::{
    ChannelSubscriptionOptions, EventQueue, EventQueueOptions, EventsPage, GetEventsResponse,
    MessageSubscription, RegisterEventQueueResponse, ZulipEvent,
};
use crate::zulip::models::{
    CurrentUser, CurrentUserResponse, EmptyResponse, ImageBytes, MessagesResponse,
    SendMessageResponse, SubscriptionsResponse, UploadFileResponse, ZulipApiResponse, ZulipMessage,
    ZulipStream,
};
use crate::zulip::normalize;
use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use serde_json::json;
use std::collections::HashSet;
use tokio::sync::{mpsc, oneshot};

const MAX_IMAGE_BYTES: u64 = 10 * 1024 * 1024;

/// Async Zulip REST client using email/API-key basic authentication.
#[derive(Clone)]
pub struct ZulipClient {
    http: Client,
    base_url: String,
    email: String,
    api_key: String,
}

/// File uploaded to Zulip storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedFile {
    /// URL returned by Zulip for the uploaded file.
    pub url: String,
    /// Filename returned by Zulip, or the requested filename if omitted.
    pub filename: String,
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

    /// Upload file bytes and return the uploaded file metadata.
    pub async fn upload_file_bytes(
        &self,
        filename: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> Result<UploadedFile> {
        let part = Part::bytes(bytes)
            .file_name(filename.to_string())
            .mime_str(content_type)
            .context("setting upload content type")?;
        let form = Form::new().part("filename", part);
        let data = self
            .post("/api/v1/user_uploads")
            .multipart(form)
            .send()
            .await?
            .json::<ZulipApiResponse<UploadFileResponse>>()
            .await?
            .into_data()?;
        let url = data
            .url
            .or(data.uri)
            .context("upload response missing url")?;
        Ok(UploadedFile {
            url,
            filename: data.filename.unwrap_or_else(|| filename.to_string()),
        })
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

    /// Register a Zulip real-time event queue.
    pub async fn register_event_queue(&self, options: &EventQueueOptions) -> Result<EventQueue> {
        let event_types = serde_json::to_string(&options.event_types)?;
        let narrow = serde_json::to_string(&options.narrow)?;
        let all_public_streams = options.all_public_streams.to_string();
        let mut form = vec![
            ("event_types", event_types),
            ("apply_markdown", "true".to_string()),
            ("client_gravatar", "false".to_string()),
            ("all_public_streams", all_public_streams),
        ];
        if !options.narrow.is_empty() {
            form.push(("narrow", narrow));
        }
        let idle_queue_timeout;
        if let Some(timeout) = options.idle_queue_timeout_secs {
            idle_queue_timeout = timeout.to_string();
            form.push(("idle_queue_timeout", idle_queue_timeout));
        }
        let data = self
            .post("/api/v1/register")
            .form(&form)
            .send()
            .await?
            .json::<ZulipApiResponse<RegisterEventQueueResponse>>()
            .await?
            .into_data()?;
        let queue_id = data
            .queue_id
            .context("register event queue response missing queue_id")?;
        Ok(EventQueue {
            queue_id,
            last_event_id: data.last_event_id,
            idle_queue_timeout_secs: data.idle_queue_timeout_secs,
            longpoll_timeout_seconds: data.event_queue_longpoll_timeout_seconds,
        })
    }

    /// Fetch events from a registered Zulip event queue.
    pub async fn get_events(
        &self,
        queue_id: &str,
        last_event_id: i64,
        dont_block: bool,
    ) -> Result<EventsPage> {
        let last_event_id = last_event_id.to_string();
        let dont_block = dont_block.to_string();
        let data = self
            .get("/api/v1/events")
            .query(&[
                ("queue_id", queue_id),
                ("last_event_id", last_event_id.as_str()),
                ("dont_block", dont_block.as_str()),
            ])
            .send()
            .await?
            .json::<ZulipApiResponse<GetEventsResponse>>()
            .await?
            .into_data()?;
        let queue_id = data.queue_id.unwrap_or_else(|| queue_id.to_string());
        let events = data
            .events
            .into_iter()
            .map(parse_event)
            .collect::<Result<Vec<_>>>()?;
        Ok(EventsPage { queue_id, events })
    }

    /// Delete a Zulip event queue.
    pub async fn delete_event_queue(&self, queue_id: &str) -> Result<()> {
        self.delete("/api/v1/events")
            .form(&[("queue_id", queue_id)])
            .send()
            .await?
            .json::<ZulipApiResponse<EmptyResponse>>()
            .await?
            .into_data()?;
        Ok(())
    }

    /// Subscribe to messages from one Zulip channel.
    pub async fn subscribe_channel(
        &self,
        options: ChannelSubscriptionOptions,
    ) -> Result<MessageSubscription> {
        let event_options = options.event_queue_options();
        let queue = self.register_event_queue(&event_options).await?;
        let buffer = options.buffer.max(1);
        let (tx, rx) = mpsc::channel(buffer);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let client = self.clone();
        tokio::spawn(async move {
            run_channel_subscription(client, event_options, queue, tx, shutdown_rx).await;
        });
        Ok(MessageSubscription::new(rx, shutdown_tx))
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

    fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .delete(format!("{}{}", self.base_url, path))
            .basic_auth(&self.email, Some(&self.api_key))
    }
}

fn parse_event(raw: serde_json::Value) -> Result<ZulipEvent> {
    let id = raw
        .get("id")
        .and_then(|value| value.as_i64())
        .context("event missing id")?;
    let event_type = raw
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string();
    Ok(ZulipEvent {
        id,
        event_type,
        raw_json: raw,
    })
}

async fn run_channel_subscription(
    client: ZulipClient,
    event_options: EventQueueOptions,
    mut queue: EventQueue,
    tx: mpsc::Sender<Result<ZulipMessage>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut re_registered_after_bad_queue = false;
    loop {
        let result = tokio::select! {
            _ = &mut shutdown => break,
            result = client.get_events(&queue.queue_id, queue.last_event_id, false) => result,
        };

        match result {
            Ok(page) => {
                re_registered_after_bad_queue = false;
                for event in page.events {
                    queue.last_event_id = queue.last_event_id.max(event.id);
                    if event.event_type != "message" {
                        continue;
                    }
                    let Some(message_json) = event.raw_json.get("message").cloned() else {
                        continue;
                    };
                    let message =
                        normalize::message(message_json).context("normalizing Zulip message event");
                    if tx.send(message).await.is_err() {
                        let _ = client.delete_event_queue(&queue.queue_id).await;
                        return;
                    }
                }
            }
            Err(err) if err.to_string().contains("BAD_EVENT_QUEUE_ID") => {
                if re_registered_after_bad_queue {
                    let _ = tx.send(Err(err)).await;
                    break;
                }
                match client.register_event_queue(&event_options).await {
                    Ok(new_queue) => {
                        queue = new_queue;
                        re_registered_after_bad_queue = true;
                    }
                    Err(register_err) => {
                        let _ = tx.send(Err(register_err)).await;
                        break;
                    }
                }
            }
            Err(err) => {
                let _ = tx.send(Err(err)).await;
                break;
            }
        }
    }
    let _ = client.delete_event_queue(&queue.queue_id).await;
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
