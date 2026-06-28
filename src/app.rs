use crate::auth::AuthStatus;
use crate::cache::{send_hash, Cache, StreamActivity, SyncRecord};
use crate::config::Settings;
use crate::zulip::client::ZulipClient;
use crate::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use anyhow::{Context, Result};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct SyncResult {
    pub streams: usize,
    pub messages: usize,
    pub hours: i64,
    pub offline: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplyResult {
    DryRun {
        realm: String,
        account: String,
        stream: String,
        topic: String,
        message: String,
        content_hash: String,
    },
    Sent {
        realm: String,
        account: String,
        stream: String,
        topic: String,
        zulip_message_id: i64,
        content_hash: String,
    },
}

pub struct App {
    settings: Settings,
    cache: Cache,
    offline: bool,
}

impl App {
    pub fn open(settings: Settings, offline: bool) -> Result<Self> {
        let cache = Cache::open(&settings.paths.cache_file)?;
        Ok(Self {
            settings,
            cache,
            offline,
        })
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn offline(&self) -> bool {
        self.offline
    }

    pub async fn auth_status(&self) -> AuthStatus {
        let problems = self.settings.auth_problems();
        if problems.is_empty() {
            let base_url = self.settings.zulip.base_url.clone().unwrap_or_default();
            let email = self.settings.zulip.email.clone().unwrap_or_default();
            if self.offline {
                return AuthStatus::Ready { base_url, email };
            }
            match self.client() {
                Ok(client) => match client.current_user().await {
                    Ok(_) => AuthStatus::Ready { base_url, email },
                    Err(err) => AuthStatus::Missing {
                        problems: vec![format!("Zulip auth check failed: {err}")],
                    },
                },
                Err(err) => AuthStatus::Missing {
                    problems: vec![format!("Zulip auth check failed: {err}")],
                },
            }
        } else {
            AuthStatus::Missing { problems }
        }
    }

    pub async fn sync(&self, hours: i64) -> Result<SyncResult> {
        if self.offline {
            return Ok(SyncResult {
                streams: self.streams()?.len(),
                messages: self.recent_messages(hours)?.len(),
                hours,
                offline: true,
            });
        }
        let client = self.client()?;
        let streams = client
            .subscriptions()
            .await
            .context("fetching Zulip streams")?;
        self.cache.upsert_streams(&streams)?;
        let messages = client
            .recent_messages(hours)
            .await
            .context("fetching Zulip messages")?;
        self.cache.upsert_messages(&messages)?;
        self.cache.set_sync_state("recent", "ok", None)?;
        Ok(SyncResult {
            streams: streams.len(),
            messages: messages.len(),
            hours,
            offline: false,
        })
    }

    pub fn streams(&self) -> Result<Vec<ZulipStream>> {
        self.cache.streams()
    }

    pub fn topics(&self, stream: &str) -> Result<Vec<ZulipTopic>> {
        self.cache.topics(stream)
    }

    pub fn topic(&self, stream: &str, topic: &str) -> Result<Vec<ZulipMessage>> {
        self.cache.messages_for_topic(stream, topic)
    }

    pub fn recent_messages(&self, hours: i64) -> Result<Vec<ZulipMessage>> {
        self.cache.recent_messages(hours)
    }

    pub fn search(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        self.cache.search_messages(query)
    }

    pub fn stream_activity(&self) -> Result<Vec<StreamActivity>> {
        self.cache.stream_activity()
    }

    pub fn sync_state(&self) -> Result<Option<SyncRecord>> {
        self.cache.sync_state("recent")
    }

    pub async fn reply(
        &self,
        stream: &str,
        topic: &str,
        message: &str,
        dry_run: bool,
        yes: bool,
    ) -> Result<ReplyResult> {
        let (realm, account, _) = self.settings.require_auth()?;
        let hash = send_hash(stream, topic, message);
        if self.cache.has_sent_hash(&hash)? {
            anyhow::bail!("duplicate send blocked: this stream/topic/message was already sent");
        }
        if dry_run {
            self.cache.record_send(stream, topic, message, None, true)?;
            return Ok(ReplyResult::DryRun {
                realm,
                account,
                stream: stream.to_string(),
                topic: topic.to_string(),
                message: message.to_string(),
                content_hash: hash,
            });
        }
        if self.offline {
            anyhow::bail!("offline mode is read-only; use --dry-run to preview a reply");
        }
        if !yes {
            anyhow::bail!(
                "sending requires --yes; use --dry-run to preview destination and content"
            );
        }
        self.cache.record_send_attempt(stream, topic, message)?;
        let client = self.client()?;
        let id = match client.send_stream_message(stream, topic, message).await {
            Ok(id) => id,
            Err(err) => {
                anyhow::bail!(
                    "send outcome unknown after audit-first attempt was recorded; duplicate retry is blocked for safety: {err}"
                );
            }
        };
        self.cache.complete_send_attempt(&hash, id)?;
        Ok(ReplyResult::Sent {
            realm,
            account,
            stream: stream.to_string(),
            topic: topic.to_string(),
            zulip_message_id: id,
            content_hash: hash,
        })
    }

    fn client(&self) -> Result<ZulipClient> {
        let (base_url, email, api_key) = self.settings.require_auth()?;
        Ok(ZulipClient::new(base_url, email, api_key))
    }
}
