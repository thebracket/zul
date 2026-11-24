//! Real-time Zulip event queue and channel subscription types.

use crate::zulip::models::ZulipMessage;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// Options for registering a Zulip event queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventQueueOptions {
    /// Event types requested from Zulip.
    pub event_types: Vec<String>,
    /// Message narrow filters for the queue.
    pub narrow: Vec<[String; 2]>,
    /// Whether the queue should include messages from all public channels.
    pub all_public_streams: bool,
    /// Requested idle queue timeout in seconds.
    pub idle_queue_timeout_secs: Option<u64>,
}

impl Default for EventQueueOptions {
    fn default() -> Self {
        Self {
            event_types: vec!["message".to_string()],
            narrow: Vec::new(),
            all_public_streams: false,
            idle_queue_timeout_secs: None,
        }
    }
}

/// Registered Zulip event queue metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventQueue {
    /// Zulip queue ID.
    pub queue_id: String,
    /// Last event ID acknowledged by the client.
    pub last_event_id: i64,
    /// Server-side idle timeout for this queue.
    pub idle_queue_timeout_secs: Option<u64>,
    /// Client-side long-poll timeout recommended by Zulip.
    pub longpoll_timeout_seconds: Option<u64>,
}

/// One raw event received from Zulip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ZulipEvent {
    /// Event ID.
    pub id: i64,
    /// Event type, e.g. `message`.
    pub event_type: String,
    /// Full raw Zulip event payload.
    pub raw_json: serde_json::Value,
}

/// Page of events returned from a Zulip event queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventsPage {
    /// Zulip queue ID.
    pub queue_id: String,
    /// Events newer than the acknowledged event ID.
    pub events: Vec<ZulipEvent>,
}

/// Options for subscribing to messages from one Zulip channel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelSubscriptionOptions {
    /// Zulip channel/stream name.
    pub channel: String,
    /// Optional Zulip topic name.
    pub topic: Option<String>,
    /// Receiver buffer size.
    pub buffer: usize,
    /// Whether higher-level callers should cache received messages.
    pub cache: bool,
}

impl ChannelSubscriptionOptions {
    /// Build options for a channel subscription with conservative defaults.
    pub fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            topic: None,
            buffer: 100,
            cache: true,
        }
    }

    /// Convert channel subscription options to Zulip event queue options.
    pub fn event_queue_options(&self) -> EventQueueOptions {
        let mut narrow = vec![["channel".to_string(), self.channel.clone()]];
        if let Some(topic) = &self.topic {
            narrow.push(["topic".to_string(), topic.clone()]);
        }
        EventQueueOptions {
            event_types: vec!["message".to_string()],
            narrow,
            all_public_streams: false,
            idle_queue_timeout_secs: None,
        }
    }
}

/// Receiver for a channel message subscription.
pub struct MessageSubscription {
    receiver: mpsc::Receiver<Result<ZulipMessage>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl MessageSubscription {
    /// Create a subscription wrapper from a receiver and shutdown signal.
    pub(crate) fn new(
        receiver: mpsc::Receiver<Result<ZulipMessage>>,
        shutdown: oneshot::Sender<()>,
    ) -> Self {
        Self {
            receiver,
            shutdown: Some(shutdown),
        }
    }

    /// Receive the next subscribed message asynchronously.
    pub async fn recv(&mut self) -> Option<Result<ZulipMessage>> {
        self.receiver.recv().await
    }

    /// Receive the next subscribed message from synchronous code.
    pub fn blocking_recv(&mut self) -> Option<Result<ZulipMessage>> {
        self.receiver.blocking_recv()
    }

    /// Close the subscription and request event-queue cleanup.
    pub fn close(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.receiver.close();
    }
}

impl Drop for MessageSubscription {
    fn drop(&mut self) {
        self.close();
    }
}

/// Raw Zulip register response fields used by the event API.
#[derive(Debug, Deserialize)]
pub(crate) struct RegisterEventQueueResponse {
    pub queue_id: Option<String>,
    pub last_event_id: i64,
    pub idle_queue_timeout_secs: Option<u64>,
    pub event_queue_longpoll_timeout_seconds: Option<u64>,
}

/// Raw Zulip events response.
#[derive(Debug, Deserialize)]
pub(crate) struct GetEventsResponse {
    pub queue_id: Option<String>,
    pub events: Vec<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zulip::models::ZulipMessageType;
    use chrono::Utc;

    #[test]
    fn channel_subscription_options_build_narrow() {
        let mut options = ChannelSubscriptionOptions::new("Denmark");
        options.topic = Some("Copenhagen".to_string());

        assert_eq!(
            options.event_queue_options().narrow,
            vec![
                ["channel".to_string(), "Denmark".to_string()],
                ["topic".to_string(), "Copenhagen".to_string()]
            ]
        );
    }

    #[test]
    fn message_subscription_supports_blocking_recv_and_close() {
        let (tx, rx) = mpsc::channel(1);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let mut subscription = MessageSubscription::new(rx, shutdown_tx);
        tx.blocking_send(Ok(test_message())).unwrap();

        assert_eq!(subscription.blocking_recv().unwrap().unwrap().id, 7);
        subscription.close();
        assert!(shutdown_rx.try_recv().is_ok());
    }

    fn test_message() -> ZulipMessage {
        ZulipMessage {
            id: 7,
            message_type: ZulipMessageType::Stream,
            stream_id: Some(1),
            stream_name: "Denmark".to_string(),
            topic: "Copenhagen".to_string(),
            dm_conversation_key: None,
            dm_participants: Vec::new(),
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some("ada@example.com".to_string()),
            timestamp: Utc::now(),
            rendered_html: "<p>hi</p>".to_string(),
            plain_text: "hi".to_string(),
            permalink: None,
            raw_json: serde_json::json!({"id": 7}),
        }
    }
}
