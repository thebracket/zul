use crate::zulip::models::{
    CurrentUser, CurrentUserResponse, MessagesResponse, SendMessageResponse, SubscriptionsResponse,
    ZulipApiResponse, ZulipMessage, ZulipStream,
};
use crate::zulip::normalize;
use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use std::collections::HashSet;

#[derive(Clone)]
pub struct ZulipClient {
    http: Client,
    base_url: String,
    email: String,
    api_key: String,
}

impl ZulipClient {
    pub fn new(base_url: String, email: String, api_key: String) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            email,
            api_key,
        }
    }

    #[allow(dead_code)]
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

    pub async fn recent_messages(&self, hours: i64) -> Result<Vec<ZulipMessage>> {
        let cutoff = chrono::Utc::now() - chrono::Duration::hours(hours);
        let mut anchor = "newest".to_string();
        let mut messages = Vec::new();
        let mut seen = HashSet::new();

        for _ in 0..25 {
            let page = self.messages_page(&anchor, None).await?;
            if page.is_empty() {
                break;
            }

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

            if saw_older_than_cutoff || oldest_id == i64::MAX || !added_new {
                break;
            }
            anchor = oldest_id.to_string();
        }

        Ok(messages)
    }

    #[allow(dead_code)]
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
        self.messages_page("newest", Some(&narrow))
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
    ) -> Result<Vec<serde_json::Value>> {
        let mut request = self.get("/api/v1/messages").query(&[
            ("anchor", anchor),
            ("num_before", "1000"),
            ("num_after", "0"),
            ("apply_markdown", "true"),
            ("client_gravatar", "false"),
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
