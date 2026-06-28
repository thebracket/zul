use crate::auth::AuthStatus;
use crate::cache::{send_hash, Cache, StreamActivity, SyncRecord};
use crate::config::Settings;
use crate::zulip::client::ZulipClient;
use crate::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use anyhow::{Context, Result};
use serde::Serialize;

/// Summary of a cache synchronization attempt.
#[derive(Debug, Serialize)]
pub struct SyncResult {
    /// Number of streams known after the sync.
    pub streams: usize,
    /// Number of recent messages fetched or read from cache.
    pub messages: usize,
    /// Lookback window used for the sync.
    pub hours: i64,
    /// Whether the operation avoided the network.
    pub offline: bool,
}

/// Result of polling Zulip for messages newer than the local cache.
#[derive(Debug, Clone, Serialize)]
pub struct PollResult {
    /// Number of new messages inserted into the cache.
    pub new_messages: usize,
    /// Highest message ID known after polling.
    pub latest_message_id: Option<i64>,
    /// Whether polling was skipped because the app is offline.
    pub offline: bool,
}

/// Result of preparing or sending a Zulip reply.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ReplyResult {
    /// Send was validated and recorded, but no network send occurred.
    DryRun {
        /// Zulip realm URL.
        realm: String,
        /// Zulip account email.
        account: String,
        /// Destination stream name.
        stream: String,
        /// Destination topic name.
        topic: String,
        /// Message body that would be sent.
        message: String,
        /// Hash used to prevent duplicate sends.
        content_hash: String,
    },
    /// Message was sent to Zulip.
    Sent {
        /// Zulip realm URL.
        realm: String,
        /// Zulip account email.
        account: String,
        /// Destination stream name.
        stream: String,
        /// Destination topic name.
        topic: String,
        /// Message ID returned by Zulip.
        zulip_message_id: i64,
        /// Hash used to prevent duplicate sends.
        content_hash: String,
    },
}

/// High-level service for Zulip API access plus local cache queries.
pub struct App {
    settings: Settings,
    cache: Cache,
    offline: bool,
}

impl App {
    /// Open the local cache and build an application service from settings.
    pub fn open(settings: Settings, offline: bool) -> Result<Self> {
        let cache = Cache::open(&settings.paths.cache_file)?;
        Ok(Self {
            settings,
            cache,
            offline,
        })
    }

    /// Return the settings used by this application instance.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Return whether this instance is in read-only offline mode.
    pub fn offline(&self) -> bool {
        self.offline
    }

    /// Check whether local credentials are present and valid.
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

    /// Fetch streams and recent messages into the local cache.
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

    /// Poll for messages newer than the highest cached message ID.
    pub async fn poll_new_messages(&self) -> Result<PollResult> {
        if self.offline {
            return Ok(PollResult {
                new_messages: 0,
                latest_message_id: self.cache.max_message_id()?,
                offline: true,
            });
        }
        let client = self.client()?;
        let previous_max = self.cache.max_message_id()?;
        let messages = match previous_max {
            Some(anchor_id) => client
                .messages_after(anchor_id)
                .await
                .context("fetching Zulip messages after latest cached message")?,
            None => {
                let streams = client
                    .subscriptions()
                    .await
                    .context("fetching Zulip streams for initial poll")?;
                self.cache.upsert_streams(&streams)?;
                client
                    .recent_messages(self.settings.cache.default_sync_hours)
                    .await
                    .context("fetching initial Zulip messages")?
            }
        };
        let new_messages = messages.len();
        if !messages.is_empty() {
            self.cache.upsert_messages(&messages)?;
        }
        self.cache.set_sync_state(
            "recent",
            "ok",
            Some(&format!("poll: {new_messages} new messages")),
        )?;
        Ok(PollResult {
            new_messages,
            latest_message_id: self.cache.max_message_id()?,
            offline: false,
        })
    }

    /// Poll using owned settings, useful from background threads.
    pub async fn poll_new_messages_with_settings(
        settings: Settings,
        offline: bool,
    ) -> Result<PollResult> {
        if offline {
            let cache = Cache::open(&settings.paths.cache_file)?;
            return Ok(PollResult {
                new_messages: 0,
                latest_message_id: cache.max_message_id()?,
                offline: true,
            });
        }

        let previous_max = {
            let cache = Cache::open(&settings.paths.cache_file)?;
            cache.max_message_id()?
        };

        let (base_url, email, api_key) = settings.require_auth()?;
        let client = ZulipClient::new(base_url, email, api_key);
        let (streams, messages) = match previous_max {
            Some(anchor_id) => (None, client.messages_after(anchor_id).await?),
            None => {
                let streams = client.subscriptions().await?;
                let messages = client
                    .recent_messages(settings.cache.default_sync_hours)
                    .await?;
                (Some(streams), messages)
            }
        };

        let new_messages = messages.len();
        let cache = Cache::open(&settings.paths.cache_file)?;
        if let Some(streams) = streams {
            cache.upsert_streams(&streams)?;
        }
        if !messages.is_empty() {
            cache.upsert_messages(&messages)?;
        }
        cache.set_sync_state(
            "recent",
            "ok",
            Some(&format!("poll: {new_messages} new messages")),
        )?;

        Ok(PollResult {
            new_messages,
            latest_message_id: cache.max_message_id()?,
            offline: false,
        })
    }

    /// List cached streams.
    pub fn streams(&self) -> Result<Vec<ZulipStream>> {
        self.cache.streams()
    }

    /// List cached topics for a stream.
    pub fn topics(&self, stream: &str) -> Result<Vec<ZulipTopic>> {
        self.cache.topics(stream)
    }

    /// List cached messages for a stream topic.
    pub fn topic(&self, stream: &str, topic: &str) -> Result<Vec<ZulipMessage>> {
        self.cache.messages_for_topic(stream, topic)
    }

    /// List cached messages newer than the requested lookback window.
    pub fn recent_messages(&self, hours: i64) -> Result<Vec<ZulipMessage>> {
        self.cache.recent_messages(hours)
    }

    /// Search cached messages using SQLite FTS, with a LIKE fallback.
    pub fn search(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        self.cache.search_messages(query)
    }

    /// Return cached message counts per stream.
    pub fn stream_activity(&self) -> Result<Vec<StreamActivity>> {
        self.cache.stream_activity()
    }

    /// Return the last recorded recent-sync state.
    pub fn sync_state(&self) -> Result<Option<SyncRecord>> {
        self.cache.sync_state("recent")
    }

    /// Send or dry-run a stream reply with duplicate-send protection.
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
