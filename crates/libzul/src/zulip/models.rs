use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Zulip stream metadata stored in the local cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipStream {
    /// Zulip stream ID.
    pub id: i64,
    /// Stream display name.
    pub name: String,
    /// Optional stream description.
    pub description: Option<String>,
}

/// Cached topic summary for a stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipTopic {
    /// Zulip stream ID.
    pub stream_id: i64,
    /// Stream display name.
    pub stream_name: String,
    /// Topic name.
    pub name: String,
    /// Number of cached messages in the topic.
    pub message_count: i64,
    /// Timestamp of the newest cached message in the topic.
    pub last_message_at: Option<DateTime<Utc>>,
}

/// Normalized Zulip message stored in the local cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipMessage {
    /// Zulip message ID.
    pub id: i64,
    /// Zulip stream ID, if the API response included one.
    pub stream_id: Option<i64>,
    /// Stream display name.
    pub stream_name: String,
    /// Topic name.
    pub topic: String,
    /// Sender Zulip user ID, if present.
    pub sender_id: Option<i64>,
    /// Sender display name.
    pub sender_name: String,
    /// Sender email address, if present.
    pub sender_email: Option<String>,
    /// Message timestamp.
    pub timestamp: DateTime<Utc>,
    /// Zulip-rendered HTML content.
    pub rendered_html: String,
    /// Plain-text approximation of the rendered content.
    pub plain_text: String,
    /// Zulip permalink, if present.
    pub permalink: Option<String>,
    /// Original message JSON for forward compatibility.
    pub raw_json: serde_json::Value,
}

/// Renderable image attachment discovered in a Zulip message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageAttachment {
    /// Zulip message ID that referenced the image.
    pub message_id: i64,
    /// Original image URL as it appeared in rendered Zulip HTML.
    pub source_url: String,
    /// Absolute URL suitable for fetching the image.
    pub absolute_url: String,
    /// Optional alternate text from the HTML.
    pub alt_text: Option<String>,
    /// Best-effort filename or label for placeholder rendering.
    pub filename_hint: Option<String>,
}

impl ImageAttachment {
    /// Stable cache key for this image URL.
    pub fn cache_key(&self) -> String {
        self.absolute_url.clone()
    }

    /// Human-facing label for terminal placeholders.
    pub fn label(&self) -> String {
        self.filename_hint
            .as_ref()
            .or(self.alt_text.as_ref())
            .cloned()
            .unwrap_or_else(|| self.absolute_url.clone())
    }
}

/// Image bytes fetched for terminal rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageBytes {
    /// Raw encoded image bytes.
    pub bytes: Vec<u8>,
    /// HTTP content type when available.
    pub content_type: Option<String>,
}

/// Current Zulip user identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CurrentUser {
    /// User email address.
    pub email: String,
    /// User full display name.
    pub full_name: String,
}

/// Generic Zulip API response envelope.
#[derive(Debug, Deserialize)]
pub struct ZulipApiResponse<T> {
    /// Zulip result status, usually `success` or `error`.
    pub result: String,
    /// Zulip message text for errors or status details.
    pub msg: String,
    #[serde(flatten)]
    /// Endpoint-specific response payload.
    pub data: T,
}

/// Response payload for the subscriptions endpoint.
#[derive(Debug, Deserialize)]
pub struct SubscriptionsResponse {
    /// Streams subscribed by the current user.
    pub subscriptions: Vec<ApiSubscription>,
}

/// Raw stream subscription from the Zulip API.
#[derive(Debug, Deserialize)]
pub struct ApiSubscription {
    /// Zulip stream ID.
    pub stream_id: i64,
    /// Stream display name.
    pub name: String,
    /// Optional stream description.
    pub description: Option<String>,
}

/// Response payload for the messages endpoint.
#[derive(Debug, Deserialize)]
pub struct MessagesResponse {
    /// Raw message objects returned by Zulip.
    pub messages: Vec<serde_json::Value>,
}

/// Response payload for the current-user endpoint.
#[derive(Debug, Deserialize)]
pub struct CurrentUserResponse {
    /// User email address.
    pub email: String,
    /// User full display name.
    pub full_name: String,
}

/// Response payload for the send-message endpoint.
#[derive(Debug, Deserialize)]
pub struct SendMessageResponse {
    /// ID of the created Zulip message.
    pub id: i64,
}

impl<T> ZulipApiResponse<T> {
    /// Return the endpoint payload or convert a Zulip error into an `anyhow` error.
    pub fn into_data(self) -> anyhow::Result<T> {
        if self.result == "success" {
            Ok(self.data)
        } else {
            anyhow::bail!("Zulip API error: {}", self.msg);
        }
    }
}
