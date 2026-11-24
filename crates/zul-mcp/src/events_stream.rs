//! MCP Events push delivery for `zulip.message` streams.
//!
//! One [`StreamHub`] owns the subscribers for the configured account. A single
//! upstream poll loop fetches messages after the oldest subscriber cursor and
//! fans each message out once; every subscriber has its own bounded queue and
//! its own writer task, which is the only writer for that subscription. This
//! keeps per-subscription ordering (active, events, heartbeats, termination)
//! independent of other streams and tool calls.
//!
//! The hub never advances a subscriber cursor past a message it has not
//! evaluated, and a full queue terminates that subscriber instead of dropping
//! messages. Upstream failures are reported as `notifications/events/error`
//! and retried with jittered backoff; the poll loop only claims progress after
//! a successful fetch.

use crate::events::{
    self, configured_realm, encode_cursor, matches_message, occurrence, PollFilter, StreamParams,
    MESSAGES_AFTER_BATCH_LIMIT,
};
use crate::protocol::{self, RequestId};
use crate::tools::one_line;
use chrono::Utc;
use libzul::zulip::client::ZulipClient;
use libzul::zulip::models::ZulipMessage;
use libzul::Settings;
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Notification carrying the subscription's start position.
pub(crate) const ACTIVE: &str = "notifications/events/active";
/// Notification carrying one event occurrence.
pub(crate) const EVENT: &str = "notifications/events/event";
/// Notification proving the stream is alive even when idle.
pub(crate) const HEARTBEAT: &str = "notifications/events/heartbeat";
/// Notification for a recoverable upstream failure.
pub(crate) const ERROR: &str = "notifications/events/error";
/// Notification for a server-initiated end of the stream.
pub(crate) const TERMINATED: &str = "notifications/events/terminated";

/// Notification metadata key that demultiplexes one subscription.
pub(crate) const SUBSCRIPTION_ID_KEY: &str = "io.modelcontextprotocol/subscriptionId";

/// Default interval between shared upstream polls.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(events::NEXT_POLL_MS as u64);
/// Default granted heartbeat interval.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = DEFAULT_POLL_INTERVAL;
/// Default number of queued occurrences per subscription.
const DEFAULT_SUBSCRIBER_CAPACITY: usize = 64;
/// Default number of concurrently active streams.
const DEFAULT_MAX_STREAMS: usize = 8;
/// Longest delay between retries after a recoverable upstream failure.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Smallest heartbeat a caller may request.
pub const MIN_HEARTBEAT_MS: i64 = 1_000;
/// Largest heartbeat a caller may request.
pub const MAX_HEARTBEAT_MS: i64 = 60_000;

/// Timing and capacity limits for event streams.
#[derive(Debug, Clone, Copy)]
pub struct EventsConfig {
    /// Interval between shared upstream polls.
    pub poll_interval: Duration,
    /// Granted heartbeat interval when the caller does not suggest one.
    pub heartbeat_interval: Duration,
    /// Occurrences queued per subscriber before the subscriber is terminated.
    pub subscriber_capacity: usize,
    /// Concurrent stream limit for the process.
    pub max_streams: usize,
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            subscriber_capacity: DEFAULT_SUBSCRIBER_CAPACITY,
            max_streams: DEFAULT_MAX_STREAMS,
        }
    }
}

impl EventsConfig {
    /// Clamp a client heartbeat suggestion, or fall back to the configured
    /// granted interval.
    pub fn granted_heartbeat(&self, requested_ms: Option<i64>) -> Duration {
        match requested_ms {
            Some(ms) => Duration::from_millis(ms.clamp(MIN_HEARTBEAT_MS, MAX_HEARTBEAT_MS) as u64),
            None => Duration::from_millis(self.heartbeat_interval.as_millis().max(1) as u64),
        }
    }
}

/// One frame pushed to a subscriber by the shared poll loop.
#[derive(Debug)]
pub(crate) enum Delivery {
    /// A message that matched the subscriber's cursor and filter.
    Message(Rc<ZulipMessage>),
    /// A recoverable upstream failure; the stream stays alive.
    Error { code: i64, message: String },
    /// The subscriber's replay window was truncated; announce a safe cursor.
    Truncated { cursor: String },
    /// The server is closing this subscription.
    Terminate { code: i64, message: String },
}

/// Why a subscription could not be registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubscribeError {
    /// The process already runs the maximum number of streams.
    TooManyStreams { limit: usize },
    /// A stream is already bound to a different realm.
    RealmMismatch,
    /// The server is shutting down.
    ShuttingDown,
}

impl SubscribeError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::TooManyStreams { limit } => {
                format!("too many active event streams (limit {limit}); cancel one and retry")
            }
            Self::RealmMismatch => {
                "event streams are already bound to another Zulip realm".to_string()
            }
            Self::ShuttingDown => "server is shutting down".to_string(),
        }
    }
}

struct Account {
    settings: Rc<Settings>,
    realm: String,
}

struct Subscriber {
    generation: u64,
    cursor: Rc<Cell<i64>>,
    filter: PollFilter,
    max_age_ms: Option<i64>,
    account_email: Option<String>,
    tx: mpsc::Sender<Delivery>,
}

/// A registered subscription: its delivery queue, cursor, and lifetime guard.
pub(crate) struct Subscription {
    pub(crate) rx: mpsc::Receiver<Delivery>,
    pub(crate) id: Value,
    pub(crate) cursor: Rc<Cell<i64>>,
    pub(crate) heartbeat: Duration,
    pub(crate) start_cursor: i64,
    realm: String,
    _guard: SubscriptionGuard,
}

struct SubscriptionGuard {
    hub: Rc<StreamHub>,
    key: String,
    generation: u64,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.hub.remove(&self.key, self.generation);
    }
}

/// Shared upstream poll loop and subscriber registry for one account.
pub struct StreamHub {
    config: EventsConfig,
    subscribers: RefCell<HashMap<String, Subscriber>>,
    account: RefCell<Option<Account>>,
    poller: RefCell<Option<JoinHandle<()>>>,
    poller_running: Cell<bool>,
    next_generation: Cell<u64>,
    rng: Cell<u64>,
    shutting_down: Cell<bool>,
}

impl StreamHub {
    /// Create an idle hub with the given timing and capacity limits.
    pub fn new(config: EventsConfig) -> Self {
        Self {
            config,
            subscribers: RefCell::new(HashMap::new()),
            account: RefCell::new(None),
            poller: RefCell::new(None),
            poller_running: Cell::new(false),
            next_generation: Cell::new(0),
            rng: Cell::new(0x9e37_79b9_7f4a_7c15),
            shutting_down: Cell::new(false),
        }
    }

    /// Register one subscription and make sure the shared poller is running.
    pub(crate) fn subscribe(
        self: &Rc<Self>,
        id: RequestId,
        start_cursor: i64,
        params: StreamParams,
        heartbeat: Duration,
        settings: Rc<Settings>,
    ) -> Result<Subscription, SubscribeError> {
        if self.shutting_down.get() {
            return Err(SubscribeError::ShuttingDown);
        }

        let realm = configured_realm(&settings);
        {
            let mut account = self.account.borrow_mut();
            match account.as_ref() {
                Some(existing) if existing.realm != realm => {
                    return Err(SubscribeError::RealmMismatch)
                }
                Some(_) => {}
                None => {
                    *account = Some(Account {
                        settings: Rc::clone(&settings),
                        realm: realm.clone(),
                    });
                }
            }
        }

        let mut subscribers = self.subscribers.borrow_mut();
        if subscribers.len() >= self.config.max_streams {
            return Err(SubscribeError::TooManyStreams {
                limit: self.config.max_streams,
            });
        }

        let key = id.registry_key();
        let generation = self.next_generation.get().wrapping_add(1);
        self.next_generation.set(generation);
        let cursor = Rc::new(Cell::new(start_cursor));
        let capacity = self.config.subscriber_capacity.max(1);
        let (tx, rx) = mpsc::channel(capacity);
        let account_email = settings.zulip.email.clone();
        subscribers.insert(
            key.clone(),
            Subscriber {
                generation,
                cursor: Rc::clone(&cursor),
                filter: params.arguments,
                max_age_ms: params.max_age_ms,
                account_email,
                tx,
            },
        );

        if !self.poller_running.get() {
            self.poller_running.set(true);
            let hub = Rc::clone(self);
            *self.poller.borrow_mut() = Some(tokio::task::spawn_local(poll_loop(hub)));
        }
        drop(subscribers);

        Ok(Subscription {
            rx,
            id: id.to_value(),
            cursor,
            heartbeat,
            start_cursor,
            realm,
            _guard: SubscriptionGuard {
                hub: Rc::clone(self),
                key,
                generation,
            },
        })
    }

    /// Abort the shared poller and refuse new subscriptions.
    pub(crate) fn shutdown(&self) {
        self.shutting_down.set(true);
        self.poller_running.set(false);
        if let Some(handle) = self.poller.borrow_mut().take() {
            handle.abort();
        }
    }

    fn remove(&self, key: &str, generation: u64) {
        let mut subscribers = self.subscribers.borrow_mut();
        if subscribers
            .get(key)
            .is_some_and(|subscriber| subscriber.generation == generation)
        {
            subscribers.remove(key);
        }
    }

    /// Snapshot the subscribers that exist when a fetch starts.
    ///
    /// A subscriber registered while the upstream request is in flight is not
    /// in the snapshot: replaying this batch for it could skip messages between
    /// its older cursor and the fetch anchor, so it waits for the next fetch,
    /// which starts at the new minimum cursor.
    fn snapshot(&self) -> Vec<(String, u64, i64)> {
        self.subscribers
            .borrow()
            .iter()
            .map(|(key, subscriber)| (key.clone(), subscriber.generation, subscriber.cursor.get()))
            .collect()
    }

    fn anchor_for(&self, targets: &[(String, u64, i64)]) -> Option<i64> {
        targets.iter().map(|(_, _, cursor)| *cursor).min()
    }

    fn account(&self) -> Option<Rc<Settings>> {
        self.account
            .borrow()
            .as_ref()
            .map(|account| Rc::clone(&account.settings))
    }

    fn realm(&self) -> String {
        self.account
            .borrow()
            .as_ref()
            .map(|account| account.realm.clone())
            .unwrap_or_default()
    }

    /// Fan one ascending batch out to every subscriber that still needs it.
    ///
    /// Evaluation (cursor check, age, filter) and queueing happen together, so
    /// a message is enqueued at most once per subscriber. Filtered and aged
    /// messages still advance the cursor.
    fn dispatch(&self, messages: Vec<ZulipMessage>, targets: &[(String, u64, i64)]) {
        if targets.is_empty() {
            return;
        }
        let targets: HashMap<&str, u64> = targets
            .iter()
            .map(|(key, generation, _)| (key.as_str(), *generation))
            .collect();
        let now_ms = Utc::now().timestamp_millis();
        let cap_hit = messages.len() >= MESSAGES_AFTER_BATCH_LIMIT;
        // A safe resume point is the cursor from before this batch: matching
        // messages in the batch are evaluated but may still be queued, so a
        // truncated notification must not claim they were delivered.
        let batch_start: HashMap<String, i64> = self
            .subscribers
            .borrow()
            .iter()
            .map(|(key, subscriber)| (key.clone(), subscriber.cursor.get()))
            .collect();
        let mut truncated: HashSet<String> = HashSet::new();
        let mut overflowed: Vec<(String, mpsc::Sender<Delivery>)> = Vec::new();

        for message in messages {
            let message = Rc::new(message);
            let mut stale: Vec<String> = Vec::new();
            {
                let mut subscribers = self.subscribers.borrow_mut();
                for (key, subscriber) in subscribers.iter_mut() {
                    if targets.get(key.as_str()) != Some(&subscriber.generation) {
                        continue;
                    }
                    if message.id <= subscriber.cursor.get() {
                        continue;
                    }
                    if let Some(max_age_ms) = subscriber.max_age_ms {
                        let age_ms = now_ms.saturating_sub(message.timestamp.timestamp_millis());
                        if age_ms > max_age_ms {
                            subscriber.cursor.set(message.id);
                            truncated.insert(key.clone());
                            continue;
                        }
                    }
                    if !matches_message(
                        &subscriber.filter,
                        &message,
                        subscriber.account_email.as_deref(),
                    ) {
                        subscriber.cursor.set(message.id);
                        continue;
                    }
                    match subscriber
                        .tx
                        .try_send(Delivery::Message(Rc::clone(&message)))
                    {
                        Ok(()) => subscriber.cursor.set(message.id),
                        Err(mpsc::error::TrySendError::Full(_)) => stale.push(key.clone()),
                        Err(mpsc::error::TrySendError::Closed(_)) => stale.push(key.clone()),
                    }
                }
                for key in stale {
                    if let Some(subscriber) = subscribers.remove(&key) {
                        if subscriber.tx.is_closed() {
                            continue;
                        }
                        overflowed.push((key, subscriber.tx));
                    }
                }
            }
        }

        if cap_hit {
            let subscribers = self.subscribers.borrow();
            for (key, subscriber) in subscribers.iter() {
                if targets.get(key.as_str()) == Some(&subscriber.generation) {
                    truncated.insert(key.clone());
                }
            }
        }

        for key in truncated {
            let cursor = batch_start.get(&key).copied().unwrap_or(0);
            self.push_truncated(&key, cursor);
        }
        for (key, tx) in overflowed {
            self.subscribers.borrow_mut().remove(&key);
            terminate(tx, protocol::INTERNAL_ERROR, overflow_message());
        }
    }

    fn push_truncated(&self, key: &str, cursor: i64) {
        let subscribers = self.subscribers.borrow();
        let Some(subscriber) = subscribers.get(key) else {
            return;
        };
        let cursor = encode_cursor(&self.realm(), cursor);
        let tx = subscriber.tx.clone();
        drop(subscribers);
        match tx.try_send(Delivery::Truncated { cursor }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.subscribers.borrow_mut().remove(key);
                terminate(tx, protocol::INTERNAL_ERROR, overflow_message());
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.subscribers.borrow_mut().remove(key);
            }
        }
    }

    fn report_error(&self, message: String) {
        let mut overflowed = Vec::new();
        {
            let subscribers = self.subscribers.borrow();
            for (key, subscriber) in subscribers.iter() {
                match subscriber.tx.try_send(Delivery::Error {
                    code: protocol::INTERNAL_ERROR,
                    message: message.clone(),
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        overflowed.push((key.clone(), subscriber.tx.clone()))
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        overflowed.push((key.clone(), subscriber.tx.clone()))
                    }
                }
            }
        }
        for (key, tx) in overflowed {
            self.subscribers.borrow_mut().remove(&key);
            if !tx.is_closed() {
                terminate(tx, protocol::INTERNAL_ERROR, overflow_message());
            }
        }
    }

    fn terminate_all(&self, code: i64, message: String) {
        let senders: Vec<mpsc::Sender<Delivery>> = self
            .subscribers
            .borrow_mut()
            .drain()
            .map(|(_, subscriber)| subscriber.tx)
            .collect();
        for tx in senders {
            terminate(tx, code, message.clone());
        }
    }

    fn next_backoff(&self, current: Duration) -> Duration {
        current.saturating_mul(2).min(MAX_BACKOFF)
    }
}

fn overflow_message() -> String {
    "subscriber too slow: the event queue overflowed; reconnect with your last cursor".to_string()
}

/// Deliver a terminal frame after any messages already queued for the
/// subscriber, so termination is always the last notification it receives.
fn terminate(tx: mpsc::Sender<Delivery>, code: i64, message: String) {
    tokio::task::spawn_local(async move {
        let _ = tx.send(Delivery::Terminate { code, message }).await;
    });
}

async fn poll_loop(hub: Rc<StreamHub>) {
    let interval = hub.config.poll_interval.max(Duration::from_millis(1));
    let mut delay = interval;
    loop {
        tokio::time::sleep(delay).await;
        let targets = hub.snapshot();
        let Some(anchor) = hub.anchor_for(&targets) else {
            hub.poller_running.set(false);
            return;
        };
        let Some(settings) = hub.account() else {
            hub.poller_running.set(false);
            return;
        };
        let client = match settings.require_auth() {
            Ok((base_url, email, api_key)) => ZulipClient::new(base_url, email, api_key),
            Err(err) => {
                hub.terminate_all(
                    protocol::INTERNAL_ERROR,
                    format!("events/stream failed: {}", one_line(&err.to_string())),
                );
                hub.poller_running.set(false);
                return;
            }
        };

        match client.messages_after(anchor).await {
            Ok(mut messages) => {
                messages.sort_by_key(|message| message.id);
                hub.dispatch(messages, &targets);
                delay = interval;
            }
            Err(err) => {
                let message = one_line(&err.to_string());
                if auth_revoked(&message) {
                    hub.terminate_all(
                        protocol::INTERNAL_ERROR,
                        format!("Zulip authentication failed: {message}"),
                    );
                    hub.poller_running.set(false);
                    return;
                }
                hub.report_error(format!("events/stream upstream poll failed: {message}"));
                delay = jittered(&hub, hub.next_backoff(delay));
            }
        }

        if hub.subscribers.borrow().is_empty() {
            hub.poller_running.set(false);
            return;
        }
    }
}

/// Add deterministic pseudo-random jitter of roughly ±25% to a retry delay.
fn jittered(hub: &StreamHub, duration: Duration) -> Duration {
    let seed = hub
        .rng
        .get()
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    hub.rng.set(seed);
    let base = duration.as_millis().max(1) as u64;
    let quarter = base / 4;
    let spread = quarter.saturating_mul(2).max(1);
    let adjustment = (seed >> 33) % spread;
    Duration::from_millis((base - quarter).max(1) + adjustment)
}

fn auth_revoked(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        "unauthorized",
        "forbidden",
        "invalid api key",
        "api key is not valid",
        "authentication failed",
        "not authorized",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn subscription_meta(id: &Value) -> Value {
    let mut meta = serde_json::Map::new();
    meta.insert(SUBSCRIPTION_ID_KEY.to_string(), id.clone());
    Value::Object(meta)
}

/// Build `notifications/events/active` for one subscription.
pub(crate) fn active_notification(
    id: &Value,
    cursor: &str,
    truncated: bool,
    heartbeat_ms: i64,
) -> Value {
    protocol::notification(
        ACTIVE,
        json!({
            "cursor": cursor,
            "truncated": truncated,
            "heartbeatMs": heartbeat_ms,
            "_meta": subscription_meta(id),
        }),
    )
}

/// Build `notifications/events/event` from the shared 2000-char projection.
pub(crate) fn event_notification(message: &ZulipMessage, realm: &str, id: &Value) -> Value {
    let mut params = occurrence(message, realm);
    params["_meta"] = subscription_meta(id);
    protocol::notification(EVENT, params)
}

/// Build `notifications/events/heartbeat` for an honest cursor.
pub(crate) fn heartbeat_notification(id: &Value, cursor: &str) -> Value {
    protocol::notification(
        HEARTBEAT,
        json!({
            "cursor": cursor,
            "_meta": subscription_meta(id),
        }),
    )
}

/// Build `notifications/events/error` for a recoverable upstream failure.
pub(crate) fn error_notification(id: &Value, code: i64, message: &str) -> Value {
    protocol::notification(
        ERROR,
        json!({
            "error": {"code": code, "message": message},
            "_meta": subscription_meta(id),
        }),
    )
}

/// Build `notifications/events/terminated` for a server-ended stream.
pub(crate) fn terminated_notification(id: &Value, code: i64, message: &str) -> Value {
    protocol::notification(
        TERMINATED,
        json!({
            "error": {"code": code, "message": message},
            "_meta": subscription_meta(id),
        }),
    )
}

/// Build the `StreamEventsResult` body sent after `terminated`.
pub(crate) fn stream_result(id: &Value) -> Value {
    json!({
        "resultType": "complete",
        "_meta": subscription_meta(id),
    })
}

/// How a served stream ended.
pub(crate) enum StreamEnd {
    /// The server closed the subscription; `terminated` was already written.
    Terminated,
    /// The writer or hub went away; write nothing further.
    Closed,
}

/// Write the active frame, then deliver frames until the stream ends.
pub(crate) async fn serve(subscription: Subscription, tx: mpsc::Sender<String>) -> StreamEnd {
    let Subscription {
        mut rx,
        id,
        cursor,
        heartbeat,
        start_cursor,
        realm,
        _guard,
    } = subscription;
    let heartbeat_ms = heartbeat.as_millis() as i64;
    let active = active_notification(
        &id,
        &encode_cursor(&realm, start_cursor),
        false,
        heartbeat_ms,
    );
    if tx.send(protocol::to_line(&active)).await.is_err() {
        return StreamEnd::Closed;
    }

    let period = heartbeat.max(Duration::from_millis(1));
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            biased;
            delivery = rx.recv() => {
                let Some(delivery) = delivery else {
                    return StreamEnd::Closed;
                };
                let frame = match delivery {
                    Delivery::Message(message) => event_notification(&message, &realm, &id),
                    Delivery::Error { code, message } => error_notification(&id, code, &message),
                    Delivery::Truncated { cursor } => {
                        active_notification(&id, &cursor, true, heartbeat_ms)
                    }
                    Delivery::Terminate { code, message } => {
                        let terminated = terminated_notification(&id, code, &message);
                        let _ = tx.send(protocol::to_line(&terminated)).await;
                        return StreamEnd::Terminated;
                    }
                };
                if tx.send(protocol::to_line(&frame)).await.is_err() {
                    return StreamEnd::Closed;
                }
            }
            _ = ticker.tick() => {
                let heartbeat = heartbeat_notification(
                    &id,
                    &encode_cursor(&realm, cursor.get()),
                );
                if tx.send(protocol::to_line(&heartbeat)).await.is_err() {
                    return StreamEnd::Closed;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EVENT_NAME;
    use libzul::zulip::models::ZulipMessage;
    use serde_json::json;
    use tokio::task::LocalSet;

    fn message(id: i64, timestamp: chrono::DateTime<Utc>) -> ZulipMessage {
        ZulipMessage {
            id,
            stream_id: Some(1),
            stream_name: "core".to_string(),
            topic: "ops".to_string(),
            sender_id: Some(2),
            sender_name: "Grace".to_string(),
            sender_email: Some("grace@example.com".to_string()),
            timestamp,
            rendered_html: "<p>hello</p>".to_string(),
            plain_text: "hello".to_string(),
            permalink: None,
            raw_json: json!({"type": "stream"}),
        }
    }

    fn settings() -> Settings {
        let mut settings = Settings {
            zulip: libzul::ZulipConfig::default(),
            cache: libzul::CacheConfig::default(),
            ui: libzul::UiConfig::default(),
            safety: libzul::SafetyConfig::default(),
            digest: libzul::DigestConfig::default(),
            themes: std::collections::BTreeMap::new(),
            paths: libzul::AppPaths {
                config_file: std::path::PathBuf::from("config.toml"),
                cache_file: std::path::PathBuf::from("cache.sqlite"),
                secret_file: std::path::PathBuf::from("api_key"),
            },
        };
        settings.zulip.base_url = Some("https://chat.example".to_string());
        settings.zulip.email = Some("ada@example.com".to_string());
        settings.zulip.api_key = Some("test-key".to_string());
        settings
    }

    fn stream_params() -> StreamParams {
        events::parse_stream_params(&json!({"name": EVENT_NAME})).expect("params")
    }

    fn hub(config: EventsConfig) -> Rc<StreamHub> {
        Rc::new(StreamHub::new(config))
    }

    fn config() -> EventsConfig {
        EventsConfig {
            poll_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(30),
            subscriber_capacity: 8,
            max_streams: 8,
        }
    }

    #[tokio::test]
    async fn heartbeat_suggestion_is_clamped() {
        let config = config();
        assert_eq!(
            config.granted_heartbeat(Some(10)),
            Duration::from_millis(MIN_HEARTBEAT_MS as u64)
        );
        assert_eq!(
            config.granted_heartbeat(Some(10_000_000)),
            Duration::from_millis(MAX_HEARTBEAT_MS as u64)
        );
        assert_eq!(
            config.granted_heartbeat(Some(5_000)),
            Duration::from_secs(5)
        );
        assert_eq!(config.granted_heartbeat(None), Duration::from_secs(30));
    }

    #[test]
    fn notification_envelopes_use_the_exact_subscription_id() {
        let id = json!("abc");
        let active = active_notification(&id, "v1:https://chat.example|7", false, 15_000);
        assert_eq!(active["method"], ACTIVE);
        assert_eq!(active["params"]["_meta"][SUBSCRIPTION_ID_KEY], json!("abc"));
        assert_eq!(active["params"]["heartbeatMs"], 15_000);
        assert_eq!(active["params"]["truncated"], false);

        let heartbeat = heartbeat_notification(&id, "v1:https://chat.example|7");
        assert_eq!(heartbeat["params"]["cursor"], "v1:https://chat.example|7");
        assert_eq!(
            heartbeat["params"]["_meta"][SUBSCRIPTION_ID_KEY],
            json!("abc")
        );

        let terminated = terminated_notification(&json!(4), -32603, "slow");
        assert_eq!(terminated["params"]["error"]["code"], -32603);
        assert_eq!(terminated["params"]["_meta"][SUBSCRIPTION_ID_KEY], json!(4));

        let result = stream_result(&json!(4));
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["_meta"][SUBSCRIPTION_ID_KEY], json!(4));
    }

    #[tokio::test]
    async fn a_full_queue_terminates_the_slow_subscriber_without_dropping() {
        LocalSet::new()
            .run_until(async {
                let hub = hub(EventsConfig {
                    subscriber_capacity: 1,
                    ..config()
                });
                let mut subscription = hub
                    .subscribe(
                        RequestId::Number(1.into()),
                        100,
                        stream_params(),
                        Duration::from_secs(30),
                        Rc::new(settings()),
                    )
                    .expect("subscription");
                let targets = hub.snapshot();
                hub.dispatch(
                    vec![message(101, Utc::now()), message(102, Utc::now())],
                    &targets,
                );
                assert_eq!(hub.subscribers.borrow().len(), 0);
                assert!(matches!(
                    subscription.rx.recv().await,
                    Some(Delivery::Message(ref message)) if message.id == 101
                ));
                match subscription.rx.recv().await {
                    Some(Delivery::Terminate { code, message }) => {
                        assert_eq!(code, protocol::INTERNAL_ERROR);
                        assert!(message.contains("too slow"), "{message}");
                    }
                    other => panic!("expected termination, got {other:?}"),
                }
                assert!(subscription.rx.recv().await.is_none());
                hub.shutdown();
            })
            .await;
    }

    #[tokio::test]
    async fn serve_reports_overflow_as_terminated_after_queued_events() {
        LocalSet::new()
            .run_until(async {
                let hub = hub(EventsConfig {
                    subscriber_capacity: 1,
                    ..config()
                });
                let subscription = hub
                    .subscribe(
                        RequestId::Number(1.into()),
                        100,
                        stream_params(),
                        Duration::from_secs(30),
                        Rc::new(settings()),
                    )
                    .expect("subscription");
                let targets = hub.snapshot();
                hub.dispatch(
                    vec![message(101, Utc::now()), message(102, Utc::now())],
                    &targets,
                );
                let (tx, mut rx) = mpsc::channel(8);
                let serving = tokio::task::spawn_local(serve(subscription, tx));

                let active = next_line(&mut rx).await;
                assert_eq!(active["method"], ACTIVE);
                let event = next_line(&mut rx).await;
                assert_eq!(event["method"], EVENT);
                assert_eq!(event["params"]["data"]["message_id"], 101);
                let terminated = next_line(&mut rx).await;
                assert_eq!(terminated["method"], TERMINATED);
                assert!(terminated["params"]["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("too slow")));
                assert!(matches!(
                    serving.await.expect("serve task"),
                    StreamEnd::Terminated
                ));
                hub.shutdown();
            })
            .await;
    }

    async fn next_line(rx: &mut mpsc::Receiver<String>) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("frame within a second")
            .expect("frame");
        serde_json::from_str(&line).expect("frame is JSON")
    }

    #[tokio::test]
    async fn max_streams_is_enforced() {
        LocalSet::new()
            .run_until(async {
                let hub = hub(EventsConfig {
                    max_streams: 2,
                    ..config()
                });
                let mut held = Vec::new();
                for id in [1, 2] {
                    held.push(
                        hub.subscribe(
                            RequestId::Number(id.into()),
                            100,
                            stream_params(),
                            Duration::from_secs(30),
                            Rc::new(settings()),
                        )
                        .expect("subscription"),
                    );
                }
                let error = match hub.subscribe(
                    RequestId::Number(3.into()),
                    100,
                    stream_params(),
                    Duration::from_secs(30),
                    Rc::new(settings()),
                ) {
                    Ok(_) => panic!("third stream must be rejected"),
                    Err(error) => error,
                };
                assert_eq!(error, SubscribeError::TooManyStreams { limit: 2 });
                drop(held);
                hub.shutdown();
            })
            .await;
    }

    #[tokio::test]
    async fn max_age_marks_truncation_and_still_advances_the_cursor() {
        LocalSet::new()
            .run_until(async {
                let hub = hub(config());
                let mut params = stream_params();
                params.max_age_ms = Some(0);
                let mut subscription = hub
                    .subscribe(
                        RequestId::Number(1.into()),
                        100,
                        params,
                        Duration::from_secs(30),
                        Rc::new(settings()),
                    )
                    .expect("subscription");
                let cursor = Rc::clone(&subscription.cursor);
                let old = Utc::now() - chrono::Duration::seconds(10);
                hub.dispatch(vec![message(101, old)], &hub.snapshot());
                assert_eq!(cursor.get(), 101);
                match subscription.rx.try_recv() {
                    Ok(Delivery::Truncated { cursor }) => {
                        assert_eq!(cursor, "v1:https://chat.example|100");
                    }
                    other => panic!("expected a truncated notification, got {other:?}"),
                }
                assert!(hub.subscribers.borrow().contains_key("n:1"));
                drop(subscription);
                hub.shutdown();
            })
            .await;
    }
}
