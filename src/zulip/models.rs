use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipStream {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipTopic {
    pub stream_id: i64,
    pub stream_name: String,
    pub name: String,
    pub message_count: i64,
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ZulipMessage {
    pub id: i64,
    pub stream_id: Option<i64>,
    pub stream_name: String,
    pub topic: String,
    pub sender_id: Option<i64>,
    pub sender_name: String,
    pub sender_email: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub rendered_html: String,
    pub plain_text: String,
    pub permalink: Option<String>,
    pub raw_json: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CurrentUser {
    pub email: String,
    pub full_name: String,
}

#[derive(Debug, Deserialize)]
pub struct ZulipApiResponse<T> {
    pub result: String,
    pub msg: String,
    #[serde(flatten)]
    pub data: T,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionsResponse {
    pub subscriptions: Vec<ApiSubscription>,
}

#[derive(Debug, Deserialize)]
pub struct ApiSubscription {
    pub stream_id: i64,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MessagesResponse {
    pub messages: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct CurrentUserResponse {
    pub email: String,
    pub full_name: String,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageResponse {
    pub id: i64,
}

impl<T> ZulipApiResponse<T> {
    pub fn into_data(self) -> anyhow::Result<T> {
        if self.result == "success" {
            Ok(self.data)
        } else {
            anyhow::bail!("Zulip API error: {}", self.msg);
        }
    }
}
