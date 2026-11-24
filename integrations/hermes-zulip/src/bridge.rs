//! Native core of the Hermes Zulip bridge.
//!
//! One dedicated OS thread owns a current-thread tokio runtime. Python callers
//! enqueue commands and wait for a oneshot reply; a background task long-polls
//! the Zulip event queue and pushes normalized events into a std channel.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use libzul::config::{Settings, ZulipConfig};
use libzul::zulip::client::ZulipClient;
use libzul::zulip::events::{EventQueue, EventQueueOptions};
use libzul::zulip::models::ZulipMessageType;
use libzul::zulip::normalize;
use serde_json::{json, Value};
use tokio::runtime::Builder;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};

const EVENT_BACKOFF_START: Duration = Duration::from_millis(100);
const EVENT_BACKOFF_MAX: Duration = Duration::from_secs(2);
const EVENT_MAX_CONSECUTIVE_FAILURES: u32 = 5;
const QUEUE_DELETE_TIMEOUT: Duration = Duration::from_secs(2);

/// Queue-shaping options supplied by the Python constructor.
#[derive(Default)]
pub(crate) struct EventOptions {
    /// Event types requested from the queue.
    pub event_types: Option<Vec<String>>,
    /// Narrow filters for the queue.
    pub narrow: Option<Vec<Vec<String>>>,
    /// Whether the queue includes all public streams.
    pub all_public_streams: bool,
    /// Optional idle queue timeout.
    pub queue_timeout_secs: Option<u64>,
}

/// Resolved bridge configuration.
///
/// `Debug` output redacts the API key; never log the credential itself.
pub(crate) struct BridgeConfig {
    /// Zulip realm base URL.
    pub base_url: String,
    /// Account email address.
    pub email: String,
    /// Account API key.
    pub api_key: String,
    /// Event types requested from the queue.
    pub event_types: Vec<String>,
    /// Narrow filters for the queue.
    pub narrow: Vec<[String; 2]>,
    /// Whether the queue includes all public streams.
    pub all_public_streams: bool,
    /// Optional idle queue timeout.
    pub queue_timeout_secs: Option<u64>,
}

impl std::fmt::Debug for BridgeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BridgeConfig")
            .field("base_url", &self.base_url)
            .field("email", &self.email)
            .field("api_key", &"<redacted>")
            .field("event_types", &self.event_types)
            .field("narrow", &self.narrow)
            .field("all_public_streams", &self.all_public_streams)
            .field("queue_timeout_secs", &self.queue_timeout_secs)
            .finish()
    }
}

impl BridgeConfig {
    fn event_options(&self) -> EventQueueOptions {
        EventQueueOptions {
            event_types: self.event_types.clone(),
            narrow: self.narrow.clone(),
            all_public_streams: self.all_public_streams,
            idle_queue_timeout_secs: self.queue_timeout_secs,
        }
    }
}

/// Merge explicit Python arguments over `libzul` settings and environment.
///
/// Missing required values are reported together, before any network call.
pub(crate) fn resolve_config(
    base_url: &str,
    email: &str,
    api_key: &str,
    event_types: Option<Vec<String>>,
    narrow: Option<Vec<Vec<String>>>,
    all_public_streams: bool,
    queue_timeout_secs: Option<u64>,
) -> Result<BridgeConfig, String> {
    let loaded = Settings::load().ok();
    merge_config(
        loaded.as_ref().map(|settings| &settings.zulip),
        base_url,
        email,
        api_key,
        EventOptions {
            event_types,
            narrow,
            all_public_streams,
            queue_timeout_secs,
        },
    )
}

pub(crate) fn merge_config(
    zulip: Option<&ZulipConfig>,
    base_url: &str,
    email: &str,
    api_key: &str,
    options: EventOptions,
) -> Result<BridgeConfig, String> {
    let base_url = pick(base_url, zulip.and_then(|zulip| zulip.base_url.as_deref()));
    let email = pick(email, zulip.and_then(|zulip| zulip.email.as_deref()));
    let api_key = pick(api_key, zulip.and_then(|zulip| zulip.api_key.as_deref()));

    let mut missing = Vec::new();
    if base_url.is_none() {
        missing.push("base_url");
    }
    if email.is_none() {
        missing.push("email");
    }
    if api_key.is_none() {
        missing.push("api_key");
    }
    if !missing.is_empty() {
        return Err(format!(
            "missing Zulip configuration: {}; pass them to Bridge(...) or set ZULIP_BASE_URL (or ZULIP_SITE), ZULIP_EMAIL, and ZULIP_API_KEY",
            missing.join(", ")
        ));
    }

    let event_types = match options.event_types {
        Some(types) if !types.is_empty() => types,
        _ => vec!["message".to_string()],
    };
    let narrow = match options.narrow {
        Some(pairs) => pairs
            .into_iter()
            .map(|pair| {
                <[String; 2]>::try_from(pair)
                    .map_err(|_| "narrow entries must be [operator, operand] pairs".to_string())
            })
            .collect::<Result<Vec<_>, String>>()?,
        None => Vec::new(),
    };

    Ok(BridgeConfig {
        base_url: base_url.expect("checked above"),
        email: email.expect("checked above"),
        api_key: api_key.expect("checked above"),
        event_types,
        narrow,
        all_public_streams: options.all_public_streams,
        queue_timeout_secs: options.queue_timeout_secs,
    })
}

fn pick(explicit: &str, fallback: Option<&str>) -> Option<String> {
    if explicit.is_empty() {
        fallback
            .filter(|value| !value.is_empty())
            .map(ToString::to_string)
    } else {
        Some(explicit.to_string())
    }
}

/// Report which libzul-resolved credentials are present, without their values.
pub(crate) fn config_status() -> Value {
    match Settings::load() {
        Ok(settings) => json!({
            "base_url": settings.zulip.base_url.as_deref().is_some_and(|value| !value.is_empty()),
            "email": settings.zulip.email.as_deref().is_some_and(|value| !value.is_empty()),
            "api_key": settings.zulip.api_key.as_deref().is_some_and(|value| !value.is_empty()),
        }),
        Err(_) => json!({
            "base_url": false,
            "email": false,
            "api_key": false,
        }),
    }
}

/// Convert a raw `message` event into the Python-facing event dictionary.
///
/// Non-message events and malformed messages return `None`; callers advance
/// the queue cursor regardless so one bad event cannot stall the poller.
pub(crate) fn normalize_event(event: &Value) -> Option<Value> {
    if event.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    let raw_message = event.get("message")?.clone();
    let message = normalize::message(raw_message).ok()?;
    let flags = message
        .raw_json
        .get("flags")
        .and_then(Value::as_array)
        .map(|flags| {
            flags
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let content = message
        .raw_json
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let kind = match message.message_type {
        ZulipMessageType::Stream => "stream",
        ZulipMessageType::Direct => "dm",
    };
    let participants = message
        .dm_participants
        .iter()
        .map(|participant| {
            json!({
                "id": participant.id,
                "name": participant.name,
                "email": participant.email,
            })
        })
        .collect::<Vec<_>>();

    Some(json!({
        "id": message.id,
        "kind": kind,
        "stream_id": message.stream_id,
        "stream": message.stream_name,
        "topic": message.topic,
        "sender_id": message.sender_id,
        "sender_name": message.sender_name,
        "sender_email": message.sender_email,
        "content": content,
        "plain_text": message.plain_text,
        "flags": flags,
        "timestamp": message.timestamp.timestamp(),
        "permalink": message.permalink,
        "dm_participants": participants,
    }))
}

enum Command {
    WhoAmI(oneshot::Sender<Result<Value, String>>),
    SendStream {
        stream: String,
        topic: String,
        content: String,
        reply: oneshot::Sender<Result<i64, String>>,
    },
    SendDirectIds {
        recipients: Vec<i64>,
        content: String,
        reply: oneshot::Sender<Result<i64, String>>,
    },
    SendDirectEmails {
        recipients: Vec<String>,
        content: String,
        reply: oneshot::Sender<Result<i64, String>>,
    },
    TypingStream {
        stream_id: i64,
        topic: String,
        op: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    TypingDirect {
        recipients: Vec<i64>,
        op: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown(oneshot::Sender<()>),
}

/// Blocking facade over the dedicated tokio runtime.
pub(crate) struct BridgeCore {
    commands: tokio_mpsc::UnboundedSender<Command>,
    events: Mutex<Receiver<Result<Value, String>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
    #[cfg(test)]
    queue_id: Arc<Mutex<Option<String>>>,
}

impl BridgeCore {
    /// Start the runtime thread and register the event queue.
    pub fn new(config: BridgeConfig) -> Result<Self, String> {
        let event_options = config.event_options();
        let client = ZulipClient::new(config.base_url, config.email, config.api_key);
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| format!("building tokio runtime: {err}"))?;
        let (event_tx, event_rx) = mpsc::channel();
        let (command_tx, mut command_rx) = tokio_mpsc::unbounded_channel();
        let thread_queue_id = Arc::new(Mutex::new(None));
        let poller_queue_id = Arc::clone(&thread_queue_id);
        #[cfg(test)]
        let queue_id_for_tests = Arc::clone(&thread_queue_id);

        let thread = thread::Builder::new()
            .name("zul-bridge".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    let (stop_tx, stop_rx) = oneshot::channel::<()>();
                    let mut poller = Some(tokio::spawn(event_loop(
                        client.clone(),
                        event_options,
                        event_tx,
                        poller_queue_id,
                        stop_rx,
                    )));

                    while let Some(command) = command_rx.recv().await {
                        match command {
                            Command::Shutdown(reply) => {
                                let _ = stop_tx.send(());
                                if let Some(poller) = poller.take() {
                                    poller.abort();
                                }
                                delete_queue(&client, &thread_queue_id).await;
                                let _ = reply.send(());
                                break;
                            }
                            command => {
                                let client = client.clone();
                                tokio::spawn(async move {
                                    execute(&client, command).await;
                                });
                            }
                        }
                    }
                });
            })
            .map_err(|err| format!("starting zul-bridge thread: {err}"))?;

        Ok(Self {
            commands: command_tx,
            events: Mutex::new(event_rx),
            thread: Mutex::new(Some(thread)),
            #[cfg(test)]
            queue_id: queue_id_for_tests,
        })
    }

    /// Fetch the authenticated account.
    pub fn whoami(&self) -> Result<Value, String> {
        self.request(Command::WhoAmI)
    }

    /// Send a stream message and return its Zulip message ID.
    pub fn send_stream(&self, stream: &str, topic: &str, content: &str) -> Result<i64, String> {
        self.request(|reply| Command::SendStream {
            stream: stream.to_string(),
            topic: topic.to_string(),
            content: content.to_string(),
            reply,
        })
    }

    /// Send a direct message to user IDs and return its Zulip message ID.
    pub fn send_direct_ids(&self, recipients: &[i64], content: &str) -> Result<i64, String> {
        self.request(|reply| Command::SendDirectIds {
            recipients: recipients.to_vec(),
            content: content.to_string(),
            reply,
        })
    }

    /// Send a direct message to email addresses and return its Zulip message ID.
    pub fn send_direct_emails(&self, recipients: &[String], content: &str) -> Result<i64, String> {
        self.request(|reply| Command::SendDirectEmails {
            recipients: recipients.to_vec(),
            content: content.to_string(),
            reply,
        })
    }

    /// Send a stream typing notification.
    pub fn typing_stream(&self, stream_id: i64, topic: &str, op: &str) -> Result<(), String> {
        self.request(|reply| Command::TypingStream {
            stream_id,
            topic: topic.to_string(),
            op: op.to_string(),
            reply,
        })
    }

    /// Send a direct-message typing notification.
    pub fn typing_direct(&self, recipients: &[i64], op: &str) -> Result<(), String> {
        self.request(|reply| Command::TypingDirect {
            recipients: recipients.to_vec(),
            op: op.to_string(),
            reply,
        })
    }

    /// Wait up to `timeout` for the next normalized message event.
    ///
    /// A timeout returns `Ok(None)`; fatal poller failures are reported as
    /// `Err` so `next_event` can raise `RuntimeError`.
    pub fn next_event(&self, timeout: Duration) -> Result<Option<Value>, String> {
        let receiver = self
            .events
            .lock()
            .map_err(|_| "bridge event queue lock poisoned".to_string())?;
        match receiver.recv_timeout(timeout) {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(message)) => Err(message),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => Err("bridge event queue is closed".to_string()),
        }
    }

    /// Stop the poller, delete the queue, and join the runtime thread.
    ///
    /// Safe to call more than once.
    pub fn close(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.commands.send(Command::Shutdown(reply_tx)).is_ok() {
            let _ = reply_rx.blocking_recv();
        }
        if let Ok(mut thread) = self.thread.lock() {
            if let Some(thread) = thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// Current registered queue ID, if any. Used by shutdown cleanup and tests.
    #[cfg(test)]
    pub(crate) fn queue_id(&self) -> Option<String> {
        self.queue_id
            .lock()
            .ok()
            .and_then(|queue_id| queue_id.clone())
    }

    fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, String>>) -> Command,
    ) -> Result<T, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(command(reply_tx))
            .map_err(|_| "bridge is closed".to_string())?;
        reply_rx
            .blocking_recv()
            .map_err(|_| "bridge worker stopped".to_string())?
    }
}

impl Drop for BridgeCore {
    fn drop(&mut self) {
        self.close();
    }
}

async fn execute(client: &ZulipClient, command: Command) {
    match command {
        Command::WhoAmI(reply) => {
            let result = client
                .current_user()
                .await
                .map(|user| json!({"email": user.email, "full_name": user.full_name}))
                .map_err(|err| format!("whoami failed: {err}"));
            let _ = reply.send(result);
        }
        Command::SendStream {
            stream,
            topic,
            content,
            reply,
        } => {
            let result = client
                .send_stream_message(&stream, &topic, &content)
                .await
                .map_err(|err| format!("send_stream failed: {err}"));
            let _ = reply.send(result);
        }
        Command::SendDirectIds {
            recipients,
            content,
            reply,
        } => {
            let recipients = recipients.into_iter().map(Value::from).collect::<Vec<_>>();
            let result = client
                .send_direct_message(&recipients, &content)
                .await
                .map_err(|err| format!("send_direct_ids failed: {err}"));
            let _ = reply.send(result);
        }
        Command::SendDirectEmails {
            recipients,
            content,
            reply,
        } => {
            let recipients = recipients
                .into_iter()
                .map(Value::String)
                .collect::<Vec<_>>();
            let result = client
                .send_direct_message(&recipients, &content)
                .await
                .map_err(|err| format!("send_direct_emails failed: {err}"));
            let _ = reply.send(result);
        }
        Command::TypingStream {
            stream_id,
            topic,
            op,
            reply,
        } => {
            let result = client
                .send_typing_stream(stream_id, &topic, &op)
                .await
                .map_err(|err| format!("typing_stream failed: {err}"));
            let _ = reply.send(result);
        }
        Command::TypingDirect {
            recipients,
            op,
            reply,
        } => {
            let result = client
                .send_typing_direct(&recipients, &op)
                .await
                .map_err(|err| format!("typing_direct failed: {err}"));
            let _ = reply.send(result);
        }
        Command::Shutdown(_) => {}
    }
}

async fn event_loop(
    client: ZulipClient,
    options: EventQueueOptions,
    event_tx: Sender<Result<Value, String>>,
    queue_id: Arc<Mutex<Option<String>>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut queue: Option<EventQueue> = None;
    let mut failures = 0u32;
    let mut backoff = EVENT_BACKOFF_START;

    loop {
        if queue.is_none() {
            let registered = tokio::select! {
                _ = &mut stop => return,
                result = register_queue(&client, &options, &queue_id) => result,
            };
            match registered {
                Ok(new_queue) => {
                    queue = Some(new_queue);
                    backoff = EVENT_BACKOFF_START;
                }
                Err(err) => {
                    failures += 1;
                    if failures >= EVENT_MAX_CONSECUTIVE_FAILURES {
                        let _ = event_tx.send(Err(format!(
                            "Zulip event queue registration failed repeatedly: {err}"
                        )));
                        delete_queue(&client, &queue_id).await;
                        return;
                    }
                    tokio::select! {
                        _ = &mut stop => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(EVENT_BACKOFF_MAX);
                    continue;
                }
            }
        }

        let active = queue.as_mut().expect("queue registered above");
        let result = tokio::select! {
            _ = &mut stop => return,
            result = client.get_events(&active.queue_id, active.last_event_id, false) => result,
        };

        match result {
            Ok(page) => {
                failures = 0;
                backoff = EVENT_BACKOFF_START;
                if page.queue_id != active.queue_id {
                    set_queue_id(&queue_id, &page.queue_id);
                    active.queue_id = page.queue_id;
                }
                for event in page.events {
                    active.last_event_id = active.last_event_id.max(event.id);
                    if event.event_type != "message" {
                        continue;
                    }
                    let Some(normalized) = normalize_event(&event.raw_json) else {
                        continue;
                    };
                    if event_tx.send(Ok(normalized)).is_err() {
                        delete_queue(&client, &queue_id).await;
                        return;
                    }
                }
            }
            Err(err) => {
                failures += 1;
                if failures >= EVENT_MAX_CONSECUTIVE_FAILURES {
                    let _ =
                        event_tx.send(Err(format!("Zulip event stream failed repeatedly: {err}")));
                    delete_queue(&client, &queue_id).await;
                    return;
                }
                queue = None;
                tokio::select! {
                    _ = &mut stop => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(EVENT_BACKOFF_MAX);
            }
        }
    }
}

async fn register_queue(
    client: &ZulipClient,
    options: &EventQueueOptions,
    queue_id: &Arc<Mutex<Option<String>>>,
) -> Result<EventQueue, String> {
    let queue = client
        .register_event_queue(options)
        .await
        .map_err(|err| err.to_string())?;
    set_queue_id(queue_id, &queue.queue_id);
    Ok(queue)
}

fn set_queue_id(queue_id: &Arc<Mutex<Option<String>>>, value: &str) {
    if let Ok(mut current) = queue_id.lock() {
        *current = Some(value.to_string());
    }
}

async fn delete_queue(client: &ZulipClient, queue_id: &Arc<Mutex<Option<String>>>) {
    let queue = queue_id.lock().ok().and_then(|queue| queue.clone());
    if let Some(queue) = queue {
        let _ = tokio::time::timeout(QUEUE_DELETE_TIMEOUT, client.delete_event_queue(&queue)).await;
    }
}
