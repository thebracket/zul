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
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};

const MAX_IMAGE_BYTES: u64 = 10 * 1024 * 1024;

/// Messages requested per forward page by [`ZulipClient::messages_after`].
const MESSAGES_PAGE_SIZE: usize = 1000;

/// Forward pages fetched by [`ZulipClient::messages_after_with_truncation`].
const MESSAGES_AFTER_MAX_PAGES: usize = 25;

/// Backward pages fetched by [`ZulipClient::recent_messages`] while walking
/// toward its time cutoff.
///
/// Intentionally independent of [`MESSAGES_AFTER_MAX_PAGES`]: the backward walk
/// stops at the cutoff or a short page, while the forward walk caps a live
/// cursor window. Both use the same page size.
const RECENT_MAX_PAGES: usize = 25;

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

        for _ in 0..RECENT_MAX_PAGES {
            let page = self
                .messages_page(&anchor, None, MESSAGES_PAGE_SIZE as u16, 0, false)
                .await?;
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

            if saw_older_than_cutoff
                || oldest_id == i64::MAX
                || !added_new
                || page_len < MESSAGES_PAGE_SIZE
            {
                break;
            }
            anchor = oldest_id.to_string();
        }

        Ok(messages)
    }

    /// Fetch the ID of the newest message visible to the current user.
    ///
    /// Returns `None` when the realm has no messages. Zulip returns no rows for
    /// `anchor=newest` with a zero window (`num_before=0`, `num_after=0`), even
    /// with `include_anchor=true`, so the request asks for the one message
    /// before the anchor (`num_before=1`, `num_after=0`,
    /// `include_anchor=true`). The largest integer `id` in that response is the
    /// newest message.
    pub async fn latest_message_id(&self) -> Result<Option<i64>> {
        let data = self
            .get("/api/v1/messages")
            .query(&[
                ("anchor", "newest"),
                ("num_before", "1"),
                ("num_after", "0"),
                ("apply_markdown", "true"),
                ("client_gravatar", "false"),
                ("include_anchor", "true"),
            ])
            .send()
            .await?
            .json::<ZulipApiResponse<MessagesResponse>>()
            .await?
            .into_data()?;
        Ok(newest_integer_id(&data.messages))
    }

    /// Fetch messages newer than a known Zulip message ID.
    pub async fn messages_after(&self, anchor_id: i64) -> Result<Vec<ZulipMessage>> {
        Ok(self.messages_after_with_truncation(anchor_id).await?.0)
    }

    /// Fetch messages newer than a known Zulip message ID, reporting whether
    /// the forward pagination limit cut the batch short.
    ///
    /// The flag is `true` only when the page limit was reached while the last
    /// page was full of messages newer than the anchor, so more history exists
    /// beyond the requested window. A short or empty final page, or a page
    /// that adds no new messages, is complete for the anchor and reports
    /// `false`.
    pub async fn messages_after_with_truncation(
        &self,
        anchor_id: i64,
    ) -> Result<(Vec<ZulipMessage>, bool)> {
        let source = HttpPageSource { client: self };
        collect_messages_after(&source, anchor_id, MESSAGES_AFTER_MAX_PAGES).await
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

/// Return the largest integer `id` in a raw message list, ignoring absent or
/// non-integer IDs.
fn newest_integer_id(messages: &[serde_json::Value]) -> Option<i64> {
    messages
        .iter()
        .filter_map(|message| message.get("id"))
        .filter_map(serde_json::Value::as_i64)
        .max()
}

type PageFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<serde_json::Value>>> + Send + 'a>>;

/// One source of forward message pages, so pagination can be tested without a
/// network.
///
/// The returned future is `Send` and the source is `Sync`, so callers that
/// await forward pagination keep a `Send` future (the TUI polls on a spawned
/// task).
trait PageSource: Sync {
    /// Fetch the page of messages after `anchor`.
    fn page<'a>(&'a self, anchor: &'a str) -> PageFuture<'a>;
}

struct HttpPageSource<'a> {
    client: &'a ZulipClient,
}

impl PageSource for HttpPageSource<'_> {
    fn page<'a>(&'a self, anchor: &'a str) -> PageFuture<'a> {
        Box::pin(async move {
            self.client
                .messages_page(anchor, None, 0, MESSAGES_PAGE_SIZE as u16, false)
                .await
        })
    }
}

/// Page forward from `anchor_id` until a short page, no progress, or
/// `max_pages`.
///
/// Returns the collected messages and whether `max_pages` cut the batch short
/// while the last page was full and still advancing.
async fn collect_messages_after(
    source: &impl PageSource,
    anchor_id: i64,
    max_pages: usize,
) -> Result<(Vec<ZulipMessage>, bool)> {
    let mut anchor = anchor_id.to_string();
    let mut messages = Vec::new();
    let mut seen = HashSet::new();
    let mut truncated = false;

    for page_index in 0..max_pages {
        let page = source.page(&anchor).await?;
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

        if !added_new || newest_id.to_string() == anchor || page_len < MESSAGES_PAGE_SIZE {
            break;
        }
        anchor = newest_id.to_string();
        if page_index + 1 == max_pages {
            truncated = true;
        }
    }

    Ok((messages, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn raw_message(id: serde_json::Value) -> serde_json::Value {
        json!({
            "id": id,
            "stream_id": 1,
            "display_recipient": "core",
            "subject": "ops",
            "sender_id": 2,
            "sender_full_name": "Ada",
            "sender_email": "ada@example.com",
            "timestamp": 1_700_000_000,
            "content": "<p>hello</p>"
        })
    }

    fn raw_page(start: i64, len: usize) -> Vec<serde_json::Value> {
        (start..start + len as i64)
            .map(|id| raw_message(json!(id)))
            .collect()
    }

    struct FakePages {
        pages: Mutex<Vec<Vec<serde_json::Value>>>,
        anchors: Mutex<Vec<String>>,
    }

    impl PageSource for FakePages {
        fn page<'a>(&'a self, anchor: &'a str) -> PageFuture<'a> {
            self.anchors
                .lock()
                .expect("anchors lock")
                .push(anchor.to_string());
            let page = self.pages.lock().expect("pages lock").remove(0);
            Box::pin(async move { Ok(page) })
        }
    }

    #[test]
    fn newest_integer_id_ignores_absent_and_non_integer_ids() {
        let messages = vec![
            json!({"id": 7}),
            json!({"id": "12"}),
            json!({"id": 99}),
            json!({"other": 1}),
            json!({"id": 3.5}),
        ];
        assert_eq!(newest_integer_id(&messages), Some(99));
        assert_eq!(newest_integer_id(&[]), None);
    }

    #[tokio::test]
    async fn truncation_flag_follows_the_page_cap_boundary() {
        let full_then_full = FakePages {
            pages: Mutex::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(1011, MESSAGES_PAGE_SIZE),
            ]),
            anchors: Mutex::new(Vec::new()),
        };
        let (messages, truncated) = collect_messages_after(&full_then_full, 10, 2)
            .await
            .expect("full pages");
        assert!(truncated, "a full final page cut by the cap is truncated");
        assert_eq!(messages.len(), MESSAGES_PAGE_SIZE * 2);
        assert_eq!(messages.first().map(|message| message.id), Some(11));
        assert_eq!(messages.last().map(|message| message.id), Some(2010));
        assert_eq!(
            *full_then_full.anchors.lock().expect("anchors lock"),
            vec!["10".to_string(), "1010".to_string()]
        );

        let duplicate_full_page = FakePages {
            pages: Mutex::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(11, MESSAGES_PAGE_SIZE),
            ]),
            anchors: Mutex::new(Vec::new()),
        };
        let (messages, truncated) = collect_messages_after(&duplicate_full_page, 10, 2)
            .await
            .expect("duplicate full pages");
        assert!(
            !truncated,
            "a full page that adds no new messages is complete, not truncated"
        );
        assert_eq!(messages.len(), MESSAGES_PAGE_SIZE);
        assert_eq!(
            *duplicate_full_page.anchors.lock().expect("anchors lock"),
            vec!["10".to_string(), "1010".to_string()]
        );

        let full_then_short = FakePages {
            pages: Mutex::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(1011, MESSAGES_PAGE_SIZE - 1),
            ]),
            anchors: Mutex::new(Vec::new()),
        };
        let (messages, truncated) = collect_messages_after(&full_then_short, 10, 2)
            .await
            .expect("full then short");
        assert!(!truncated, "a short final page proves the batch complete");
        assert_eq!(messages.len(), MESSAGES_PAGE_SIZE * 2 - 1);

        let short = FakePages {
            pages: Mutex::new(vec![raw_page(11, MESSAGES_PAGE_SIZE - 1)]),
            anchors: Mutex::new(Vec::new()),
        };
        let (_, truncated) = collect_messages_after(&short, 10, 2)
            .await
            .expect("short page");
        assert!(!truncated);
        assert_eq!(
            *short.anchors.lock().expect("anchors lock"),
            vec!["10".to_string()]
        );
    }

    #[tokio::test]
    async fn latest_message_id_chooses_the_newest_integer_id() {
        let server = MockServer::start().await;
        // Real Zulip returns no rows for a zero-window newest anchor, so a
        // request with `num_before=0` must not be what the client sends.
        Mock::given(method("GET"))
            .and(path("/api/v1/messages"))
            .and(query_param("anchor", "newest"))
            .and(query_param("num_before", "0"))
            .and(query_param("num_after", "0"))
            .and(query_param("include_anchor", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "messages": [],
            })))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v1/messages"))
            .and(query_param("anchor", "newest"))
            .and(query_param("num_before", "1"))
            .and(query_param("num_after", "0"))
            .and(query_param("include_anchor", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "messages": [
                    raw_message(json!(41)),
                    raw_message(json!("999")),
                    raw_message(json!(43)),
                    raw_message(json!(175119)),
                ],
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ZulipClient::new(
            server.uri(),
            "ada@example.com".to_string(),
            "key".to_string(),
        );
        assert_eq!(
            client.latest_message_id().await.unwrap(),
            Some(175119),
            "the one-message window before the newest anchor carries the newest id"
        );
        server.verify().await;
    }

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
