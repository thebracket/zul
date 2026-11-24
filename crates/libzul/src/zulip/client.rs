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
use std::future::Future;
use std::pin::Pin;

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
                .messages_page(&anchor, None, MESSAGES_PAGE_SIZE as u16, 0)
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
    /// Returns `None` when the realm has no messages. The request asks for the
    /// newest anchor itself (`anchor=newest`, `include_anchor=true`) with no
    /// messages before or after it, so the response carries at most one message.
    pub async fn latest_message_id(&self) -> Result<Option<i64>> {
        let data = self
            .get("/api/v1/messages")
            .query(&[
                ("anchor", "newest"),
                ("num_before", "0"),
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
        self.messages_page("newest", Some(&narrow), 1000, 0)
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
    ) -> Result<Vec<serde_json::Value>> {
        let num_before = num_before.to_string();
        let num_after = num_after.to_string();
        let mut request = self.get("/api/v1/messages").query(&[
            ("anchor", anchor),
            ("num_before", num_before.as_str()),
            ("num_after", num_after.as_str()),
            ("apply_markdown", "true"),
            ("client_gravatar", "false"),
            ("include_anchor", "false"),
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

/// Return the largest integer `id` in a raw message list, ignoring absent or
/// non-integer IDs.
fn newest_integer_id(messages: &[serde_json::Value]) -> Option<i64> {
    messages
        .iter()
        .filter_map(|message| message.get("id"))
        .filter_map(serde_json::Value::as_i64)
        .max()
}

type PageFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<serde_json::Value>>> + 'a>>;

/// One source of forward message pages, so pagination can be tested without a
/// network.
trait PageSource {
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
                .messages_page(anchor, None, 0, MESSAGES_PAGE_SIZE as u16)
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
    use std::cell::RefCell;
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
        pages: RefCell<Vec<Vec<serde_json::Value>>>,
        anchors: RefCell<Vec<String>>,
    }

    impl PageSource for FakePages {
        fn page<'a>(&'a self, anchor: &'a str) -> PageFuture<'a> {
            self.anchors.borrow_mut().push(anchor.to_string());
            let page = self.pages.borrow_mut().remove(0);
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
            pages: RefCell::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(1011, MESSAGES_PAGE_SIZE),
            ]),
            anchors: RefCell::new(Vec::new()),
        };
        let (messages, truncated) = collect_messages_after(&full_then_full, 10, 2)
            .await
            .expect("full pages");
        assert!(truncated, "a full final page cut by the cap is truncated");
        assert_eq!(messages.len(), MESSAGES_PAGE_SIZE * 2);
        assert_eq!(messages.first().map(|message| message.id), Some(11));
        assert_eq!(messages.last().map(|message| message.id), Some(2010));
        assert_eq!(
            *full_then_full.anchors.borrow(),
            vec!["10".to_string(), "1010".to_string()]
        );

        let duplicate_full_page = FakePages {
            pages: RefCell::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(11, MESSAGES_PAGE_SIZE),
            ]),
            anchors: RefCell::new(Vec::new()),
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
            *duplicate_full_page.anchors.borrow(),
            vec!["10".to_string(), "1010".to_string()]
        );

        let full_then_short = FakePages {
            pages: RefCell::new(vec![
                raw_page(11, MESSAGES_PAGE_SIZE),
                raw_page(1011, MESSAGES_PAGE_SIZE - 1),
            ]),
            anchors: RefCell::new(Vec::new()),
        };
        let (messages, truncated) = collect_messages_after(&full_then_short, 10, 2)
            .await
            .expect("full then short");
        assert!(!truncated, "a short final page proves the batch complete");
        assert_eq!(messages.len(), MESSAGES_PAGE_SIZE * 2 - 1);

        let short = FakePages {
            pages: RefCell::new(vec![raw_page(11, MESSAGES_PAGE_SIZE - 1)]),
            anchors: RefCell::new(Vec::new()),
        };
        let (_, truncated) = collect_messages_after(&short, 10, 2)
            .await
            .expect("short page");
        assert!(!truncated);
        assert_eq!(*short.anchors.borrow(), vec!["10".to_string()]);
    }

    #[tokio::test]
    async fn latest_message_id_chooses_the_newest_integer_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/messages"))
            .and(query_param("anchor", "newest"))
            .and(query_param("num_before", "0"))
            .and(query_param("num_after", "0"))
            .and(query_param("include_anchor", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "messages": [
                    raw_message(json!(41)),
                    raw_message(json!("999")),
                    raw_message(json!(43)),
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
        assert_eq!(client.latest_message_id().await.unwrap(), Some(43));
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
}
