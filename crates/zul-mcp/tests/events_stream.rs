//! Wire-level tests for `events/stream` push delivery.
//!
//! Each test drives the real [`Server`] dispatcher under a `LocalSet` with fast
//! poll and heartbeat intervals, a wiremock Zulip realm, and an injected
//! [`Settings`] so nothing reads the developer's configuration.

use libzul::{AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zul_mcp::events;
use zul_mcp::events_stream::EventsConfig;
use zul_mcp::server::{Server, ServerConfig};
use zul_mcp::tools::ToolContext;

const API_KEY: &str = "zul-mcp-stream-test-key-4c9f-DO-NOT-LEAK";
const SUBSCRIPTION_ID: &str = "io.modelcontextprotocol/subscriptionId";
const FRAME_TIMEOUT: Duration = Duration::from_secs(5);

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs() as i64
}

fn settings(dir: &Path, base_url: &str) -> Rc<Settings> {
    let mut settings = Settings {
        zulip: ZulipConfig::default(),
        cache: CacheConfig::default(),
        ui: UiConfig::default(),
        safety: SafetyConfig::default(),
        digest: DigestConfig::default(),
        themes: BTreeMap::new(),
        paths: AppPaths {
            config_file: dir.join("config.toml"),
            cache_file: dir.join("cache.sqlite"),
            secret_file: dir.join("api_key"),
        },
    };
    settings.zulip.base_url = Some(base_url.trim_end_matches('/').to_string());
    settings.zulip.email = Some("ada@example.com".to_string());
    settings.zulip.api_key = Some(API_KEY.to_string());
    Rc::new(settings)
}

fn server_config(poll_ms: u64, heartbeat_ms: u64, max_streams: usize) -> ServerConfig {
    server_config_with_timeout(poll_ms, heartbeat_ms, max_streams, Duration::from_secs(30))
}

fn server_config_with_timeout(
    poll_ms: u64,
    heartbeat_ms: u64,
    max_streams: usize,
    request_timeout: Duration,
) -> ServerConfig {
    ServerConfig {
        request_timeout,
        max_inflight: 8,
        events: EventsConfig {
            poll_interval: Duration::from_millis(poll_ms),
            heartbeat_interval: Duration::from_millis(heartbeat_ms),
            subscriber_capacity: 8,
            max_streams,
        },
    }
}

fn message(id: i64, stream: &str, topic: &str, sender: &str, email: &str, timestamp: i64) -> Value {
    json!({
        "id": id,
        "type": "stream",
        "stream_id": 1,
        "display_recipient": stream,
        "subject": topic,
        "sender_id": 2,
        "sender_full_name": sender,
        "sender_email": email,
        "timestamp": timestamp,
        "content": "<p>hello</p>",
        "permalink": format!("https://chat.example/#narrow/stream/{id}"),
    })
}

async fn mount_after<F>(server: &MockServer, plan: F) -> Arc<AtomicUsize>
where
    F: Fn(i64) -> Vec<Value> + Send + Sync + 'static,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let responder = move |request: &wiremock::Request| {
        counter.fetch_add(1, Ordering::SeqCst);
        let anchor = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "anchor")
            .and_then(|(_, value)| value.parse::<i64>().ok());
        let messages = anchor.map(&plan).unwrap_or_default();
        ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": messages,
        }))
    };
    mount_with(server, responder).await;
    calls
}

async fn mount_with<F>(server: &MockServer, responder: F)
where
    F: Fn(&wiremock::Request) -> ResponseTemplate + Send + Sync + 'static,
{
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .respond_with(responder)
        .mount(server)
        .await;
}

fn stream_request(id: Value, cursor: &str, extras: Value) -> String {
    let mut params = extras;
    params["name"] = json!("zulip.message");
    params["cursor"] = json!(cursor);
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    });
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "events/stream",
        "params": params,
    })
    .to_string()
}

fn null_cursor_stream_request(id: Value, extras: Value) -> String {
    let mut params = extras;
    params["name"] = json!("zulip.message");
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
    });
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "events/stream",
        "params": params,
    })
    .to_string()
}

fn raw_stream_request(id: Value, params: Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "events/stream",
        "params": params,
    })
    .to_string()
}

fn subscription_id(frame: &Value) -> &Value {
    &frame["params"]["_meta"][SUBSCRIPTION_ID]
}

struct Wire {
    rx: mpsc::Receiver<String>,
    seen: Vec<String>,
    pending: std::collections::VecDeque<Value>,
}

impl Wire {
    fn new(rx: mpsc::Receiver<String>) -> Self {
        Self {
            rx,
            seen: Vec::new(),
            pending: std::collections::VecDeque::new(),
        }
    }

    async fn next(&mut self) -> Value {
        if let Some(frame) = self.pending.pop_front() {
            return frame;
        }
        self.recv().await
    }

    async fn recv(&mut self) -> Value {
        let line = tokio::time::timeout(FRAME_TIMEOUT, self.rx.recv())
            .await
            .expect("frame within the timeout")
            .expect("frame");
        let value = serde_json::from_str(&line)
            .unwrap_or_else(|err| panic!("stdout frame is not JSON ({err}): {line}"));
        self.seen.push(line);
        value
    }

    async fn notification(&mut self, method: &str, id: &Value) -> Value {
        let matches = |frame: &Value| {
            frame.get("method").and_then(Value::as_str) == Some(method)
                && subscription_id(frame) == id
        };
        if let Some(index) = self.pending.iter().position(matches) {
            return self.pending.remove(index).expect("buffered frame");
        }
        loop {
            let frame = self.recv().await;
            if matches(&frame) {
                return frame;
            }
            self.pending.push_back(frame);
        }
    }

    async fn quiet(&mut self) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
        let mut frames = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return frames;
            }
            match tokio::time::timeout(remaining, self.rx.recv()).await {
                Ok(Some(line)) => {
                    let value = serde_json::from_str(&line).expect("frame is JSON");
                    self.seen.push(line);
                    frames.push(value);
                }
                Ok(None) | Err(_) => return frames,
            }
        }
    }

    fn assert_no_api_key(&self) {
        for line in &self.seen {
            assert!(
                !line.contains(API_KEY),
                "API key leaked into a stream frame: {line}"
            );
        }
    }
}

fn cursor(base: &str, id: i64) -> String {
    format!("v1:{base}|{id}")
}

#[tokio::test]
async fn active_comes_first_and_events_carry_the_exact_envelope() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| {
        if anchor == 100 {
            vec![message(
                101,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                timestamp,
            )]
        } else {
            Vec::new()
        }
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);

            let start = cursor(&base, 100);
            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;

            let active = wire.next().await;
            assert_eq!(active["method"], "notifications/events/active");
            assert_eq!(active["params"]["cursor"], start);
            assert_eq!(active["params"]["truncated"], false);
            assert_eq!(active["params"]["heartbeatMs"], 5_000);
            assert_eq!(subscription_id(&active), &json!(1));

            let event = wire.next().await;
            assert_eq!(
                event["method"], "notifications/events/event",
                "a normal stream sends exactly one active before its first event: {event}"
            );
            assert_eq!(event["params"]["eventId"], json!(format!("{base}#101")));
            assert_eq!(event["params"]["name"], "zulip.message");
            assert_eq!(event["params"]["cursor"], json!(cursor(&base, 101)));
            assert_eq!(
                event["params"]["timestamp"],
                json!(chrono::DateTime::from_timestamp(timestamp, 0)
                    .expect("timestamp")
                    .to_rfc3339())
            );
            assert_eq!(subscription_id(&event), &json!(1));
            let data = &event["params"]["data"];
            assert_eq!(data["message_id"], 101);
            assert_eq!(data["realm"], base);
            assert_eq!(data["stream"], "core team");
            assert_eq!(data["topic"], "ops");
            assert_eq!(data["sender_email"], "grace@example.com");
            assert_eq!(data["text"], "hello");
            assert_eq!(data["text_truncated"], false);

            // A string request id must be preserved everywhere it appears.
            server
                .handle_line(&stream_request(json!("abc"), &start, json!({})))
                .await;
            let active = wire
                .notification("notifications/events/active", &json!("abc"))
                .await;
            assert_eq!(active["params"]["cursor"], start);
            assert_eq!(subscription_id(&active), &json!("abc"));
            let event = wire
                .notification("notifications/events/event", &json!("abc"))
                .await;
            assert_eq!(event["params"]["data"]["message_id"], 101);
            assert_eq!(subscription_id(&event), &json!("abc"));

            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn events_arrive_in_ascending_order_even_when_the_fetch_is_unsorted() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| {
        if anchor == 100 {
            vec![
                message(103, "core", "ops", "Grace", "grace@example.com", timestamp),
                message(101, "core", "ops", "Grace", "grace@example.com", timestamp),
                message(102, "core", "ops", "Grace", "grace@example.com", timestamp),
            ]
        } else {
            Vec::new()
        }
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            let mut ids = Vec::new();
            let mut cursors = Vec::new();
            for _ in 0..3 {
                let event = wire
                    .notification("notifications/events/event", &json!(1))
                    .await;
                ids.push(event["params"]["data"]["message_id"].clone());
                cursors.push(event["params"]["cursor"].clone());
            }
            assert_eq!(ids, vec![json!(101), json!(102), json!(103)]);
            assert_eq!(
                cursors,
                vec![
                    json!(cursor(&base, 101)),
                    json!(cursor(&base, 102)),
                    json!(cursor(&base, 103)),
                ]
            );
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn backpressure_terminates_the_stream_with_terminated_then_a_result() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| {
        if anchor == 100 {
            vec![
                message(101, "core", "ops", "Grace", "grace@example.com", timestamp),
                message(102, "core", "ops", "Grace", "grace@example.com", timestamp),
            ]
        } else {
            Vec::new()
        }
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            // Two held permits fill the stdout queue, so the stream task parks
            // while the hub fills and overflows its one-slot subscriber queue.
            let (tx, rx) = mpsc::channel(2);
            let writer = tx.clone();
            let first = writer.reserve().await.expect("reserve one");
            let second = writer.reserve().await.expect("reserve two");
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut config = server_config(30, 5_000, 8);
            config.events.subscriber_capacity = 1;
            let mut server = Server::with_context(tx, config, tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop(first);
            drop(second);

            let mut methods = Vec::new();
            let mut events = Vec::new();
            loop {
                let frame = wire.next().await;
                if let Some(method) = frame.get("method").and_then(Value::as_str) {
                    methods.push(method.to_string());
                    if method == "notifications/events/event" {
                        events.push(frame["params"]["data"]["message_id"].clone());
                    }
                    if method == "notifications/events/terminated" {
                        assert_eq!(subscription_id(&frame), &json!(1));
                        assert!(frame["params"]["error"]["message"]
                            .as_str()
                            .is_some_and(|message| message.contains("too slow")));
                    }
                    continue;
                }
                assert_eq!(frame["id"], json!(1));
                assert_eq!(frame["result"]["resultType"], "complete");
                assert_eq!(
                    frame["result"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                    json!(1)
                );
                break;
            }

            assert_eq!(
                events,
                vec![json!(101)],
                "the message that fitted must not be dropped"
            );
            let terminated = methods
                .iter()
                .position(|method| method == "notifications/events/terminated")
                .expect("terminated notification");
            let result = methods.len();
            assert!(
                terminated + 1 == result,
                "terminated must come last before the result: {methods:?}"
            );
            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn idle_streams_heartbeat_with_an_honest_cursor() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_after(&mock, |_| Vec::new()).await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(500, 50, 8), tools);
            let mut wire = Wire::new(rx);

            let start = cursor(&base, 100);
            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            let active = wire.next().await;
            assert_eq!(active["params"]["heartbeatMs"], 50);

            let heartbeat = wire
                .notification("notifications/events/heartbeat", &json!(1))
                .await;
            assert_eq!(heartbeat["params"]["cursor"], start);
            assert_eq!(subscription_id(&heartbeat), &json!(1));

            let extra = wire.quiet().await;
            let heartbeats = extra
                .iter()
                .filter(|frame| {
                    frame.get("method").and_then(Value::as_str)
                        == Some("notifications/events/heartbeat")
                })
                .count();
            assert!(
                heartbeats >= 1,
                "an idle stream must keep heartbeating: {extra:?}"
            );
            for frame in &extra {
                assert_eq!(frame["params"]["cursor"], start);
                assert_ne!(
                    frame.get("method").and_then(Value::as_str),
                    Some("notifications/events/event")
                );
            }

            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn two_streams_share_one_upstream_fetch() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    let calls = mount_after(&mock, move |anchor| {
        if anchor == 100 {
            vec![message(
                101,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                timestamp,
            )]
        } else {
            Vec::new()
        }
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(1_000, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            for id in [1, 2] {
                server
                    .handle_line(&stream_request(json!(id), &start, json!({})))
                    .await;
            }
            let first_active = wire
                .notification("notifications/events/active", &json!(1))
                .await;
            let second_active = wire
                .notification("notifications/events/active", &json!(2))
                .await;
            assert_eq!(subscription_id(&first_active), &json!(1));
            assert_eq!(subscription_id(&second_active), &json!(2));

            let first = wire
                .notification("notifications/events/event", &json!(1))
                .await;
            let second = wire
                .notification("notifications/events/event", &json!(2))
                .await;
            assert_eq!(first["params"]["data"]["message_id"], 101);
            assert_eq!(second["params"]["data"]["message_id"], 101);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "two subscribers must share one upstream poll"
            );

            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn cancelling_one_stream_leaves_the_other_delivering() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| match anchor {
        100 => vec![message(
            101,
            "core team",
            "ops",
            "Grace",
            "grace@example.com",
            timestamp,
        )],
        101 => vec![message(
            102,
            "core team",
            "ops",
            "Grace",
            "grace@example.com",
            timestamp,
        )],
        _ => Vec::new(),
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            for id in [1, 2] {
                server
                    .handle_line(&stream_request(json!(id), &start, json!({})))
                    .await;
            }
            for id in [1, 2] {
                let event = wire
                    .notification("notifications/events/event", &json!(id))
                    .await;
                assert_eq!(
                    event["params"]["data"]["message_id"], 101,
                    "id {id} received the wrong first event; seen={:?}",
                    wire.seen
                );
            }

            server
                .handle_line(
                    &json!({
                        "jsonrpc": "2.0",
                        "method": "notifications/cancelled",
                        "params": {"requestId": 1, "reason": "test"},
                    })
                    .to_string(),
                )
                .await;

            let event = wire
                .notification("notifications/events/event", &json!(2))
                .await;
            assert_eq!(event["params"]["data"]["message_id"], 102);

            let after = wire.quiet().await;
            for frame in &after {
                assert_ne!(
                    subscription_id(frame),
                    &json!(1),
                    "a cancelled stream must emit nothing further: {frame}"
                );
                assert_ne!(frame["id"], json!(1), "a cancelled stream must not answer");
            }
            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn filters_and_exclude_self_still_advance_the_cursor() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| {
        if anchor != 100 {
            return Vec::new();
        }
        vec![
            message(101, "core", "ops", "Grace", "grace@example.com", timestamp),
            message(
                102,
                "core",
                "other",
                "Grace",
                "grace@example.com",
                timestamp,
            ),
            message(103, "other", "ops", "Grace", "grace@example.com", timestamp),
            message(104, "core", "ops", "Bob", "bob@example.com", timestamp),
            message(105, "core", "ops", "Ada", "ada@example.com", timestamp),
        ]
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(100, 50, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &start,
                    json!({"arguments": {
                        "stream": "core",
                        "topic": "ops",
                        "sender_email": "grace@example.com",
                    }}),
                ))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            let event = wire
                .notification("notifications/events/event", &json!(1))
                .await;
            assert_eq!(event["params"]["data"]["message_id"], 101);

            let mut advanced = false;
            for _ in 0..100 {
                let heartbeat = wire
                    .notification("notifications/events/heartbeat", &json!(1))
                    .await;
                if heartbeat["params"]["cursor"] == json!(cursor(&base, 105)) {
                    advanced = true;
                    break;
                }
            }
            assert!(
                advanced,
                "filtered and self messages must still advance the heartbeat cursor"
            );

            let extra = wire.quiet().await;
            for frame in extra.iter().filter(|frame| {
                frame.get("method").and_then(Value::as_str) == Some("notifications/events/event")
            }) {
                assert_eq!(frame["params"]["data"]["message_id"], 101);
            }
            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn upstream_failure_reports_error_then_recovers() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    mount_with(&mock, move |request: &wiremock::Request| {
        let call = counter.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return ResponseTemplate::new(500).set_body_json(json!({
                "result": "error",
                "msg": "boom",
                "messages": [],
            }));
        }
        let anchor = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "anchor")
            .and_then(|(_, value)| value.parse::<i64>().ok());
        let messages = if anchor == Some(100) {
            vec![message(
                101,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                timestamp,
            )]
        } else {
            Vec::new()
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": messages,
        }))
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(50, 20, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            let error = wire
                .notification("notifications/events/error", &json!(1))
                .await;
            assert!(
                error["params"]["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("boom")),
                "the upstream reason must be reported: {error}"
            );
            assert_eq!(subscription_id(&error), &json!(1));

            let mut event = None;
            let mut honest_heartbeats = 0;
            while event.is_none() {
                let frame = wire.next().await;
                match frame.get("method").and_then(Value::as_str) {
                    Some("notifications/events/heartbeat") => {
                        assert_eq!(
                            frame["params"]["cursor"], start,
                            "a failing upstream must not advance the heartbeat cursor"
                        );
                        honest_heartbeats += 1;
                    }
                    Some("notifications/events/event") => event = Some(frame),
                    _ => {}
                }
            }
            let event = event.expect("recovered event");
            assert_eq!(event["params"]["data"]["message_id"], 101);
            assert_eq!(subscription_id(&event), &json!(1));
            assert!(
                honest_heartbeats >= 1,
                "the stream must stay alive while retrying"
            );
            assert!(calls.load(Ordering::SeqCst) >= 2);

            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn invalid_stream_requests_are_immediate_protocol_errors() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_after(&mock, |_| Vec::new()).await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(500, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let modern = json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28"});

            // 1: missing _meta must be rejected by the version gate.
            server
                .handle_line(&raw_stream_request(
                    json!(1),
                    json!({"name": "zulip.message", "cursor": cursor(&base, 100)}),
                ))
                .await;
            // 2: unknown event name.
            server
                .handle_line(&raw_stream_request(
                    json!(2),
                    json!({"name": "other.event", "_meta": modern}),
                ))
                .await;
            // 3: invalid heartbeat.
            server
                .handle_line(&raw_stream_request(
                    json!(3),
                    json!({"name": "zulip.message", "heartbeatMs": -1, "_meta": modern}),
                ))
                .await;
            // 4: malformed cursor.
            server
                .handle_line(&raw_stream_request(
                    json!(4),
                    json!({"name": "zulip.message", "cursor": "forty-two", "_meta": modern}),
                ))
                .await;
            // 5: cursor minted for another realm.
            server
                .handle_line(&raw_stream_request(
                    json!(5),
                    json!({
                        "name": "zulip.message",
                        "cursor": "v1:https://other.example|9",
                        "_meta": modern,
                    }),
                ))
                .await;
            // 6: missing name.
            server
                .handle_line(&raw_stream_request(json!(6), json!({"_meta": modern})))
                .await;

            let expected = [
                (1, -32022),
                (2, -32602),
                (3, -32602),
                (4, -32602),
                (5, -32602),
                (6, -32602),
            ];
            for (id, code) in expected {
                let response = wire.next().await;
                assert_eq!(response["id"], json!(id));
                assert_eq!(
                    response["error"]["code"],
                    json!(code),
                    "unexpected error for id {id}: {response}"
                );
                assert!(response.get("method").is_none());
            }
            let after = wire.quiet().await;
            assert!(
                after.is_empty(),
                "a rejected stream must not emit notifications: {after:?}"
            );
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn the_ninth_stream_is_rejected() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_after(&mock, |_| Vec::new()).await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(500, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            for id in 1..=8 {
                server
                    .handle_line(&stream_request(json!(id), &start, json!({})))
                    .await;
                let active = wire
                    .notification("notifications/events/active", &json!(id))
                    .await;
                assert_eq!(subscription_id(&active), &json!(id));
            }

            server
                .handle_line(&stream_request(json!(9), &start, json!({})))
                .await;
            let response = wire.next().await;
            assert_eq!(response["id"], json!(9));
            assert_eq!(response["error"]["code"], json!(-32603));
            assert!(response["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("too many")));

            wire.assert_no_api_key();
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn max_age_announces_truncation_at_the_aged_cursor() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let old = now() - 3_600;
    mount_after(&mock, move |anchor| {
        if anchor == 100 {
            vec![message(
                101,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                old,
            )]
        } else {
            Vec::new()
        }
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    let poll = events::poll(
        &settings,
        events::parse_poll_params(&json!({
            "name": "zulip.message",
            "cursor": cursor(&base, 100),
            "maxAgeMs": 60_000,
        }))
        .expect("poll params"),
    )
    .await
    .expect("poll result");
    assert_eq!(poll["truncated"], true);
    assert_eq!(poll["events"], json!([]));
    assert_eq!(
        poll["cursor"],
        json!(cursor(&base, 101)),
        "poll's cursor advances past the aged match"
    );

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 50, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &start,
                    json!({"maxAgeMs": 60_000}),
                ))
                .await;
            let active = wire.next().await;
            assert_eq!(active["method"], "notifications/events/active");
            assert_eq!(active["params"]["truncated"], false);
            assert_eq!(active["params"]["cursor"], start);

            let truncated = wire
                .notification("notifications/events/active", &json!(1))
                .await;
            assert_eq!(truncated["params"]["truncated"], true);
            assert_eq!(
                truncated["params"]["cursor"],
                json!(cursor(&base, 101)),
                "aged truncation must announce the advanced cursor so a resume progresses"
            );

            let after = wire.quiet().await;
            let mut heartbeats = 0;
            for frame in &after {
                match frame.get("method").and_then(Value::as_str) {
                    Some("notifications/events/heartbeat") => {
                        heartbeats += 1;
                        assert_eq!(
                            frame["params"]["cursor"],
                            json!(cursor(&base, 101)),
                            "heartbeats must report the advanced resume position"
                        );
                    }
                    Some("notifications/events/event") => {
                        panic!("an aged message must not be emitted: {frame}")
                    }
                    _ => {}
                }
            }
            assert!(
                heartbeats > 0,
                "the stream must heartbeat after the truncation announcement"
            );
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn max_age_resume_from_the_announced_cursor_progresses() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let old = now() - 3_600;
    let fresh = now();
    mount_after(&mock, move |anchor| match anchor {
        100 => vec![message(
            101,
            "core",
            "ops",
            "Grace",
            "grace@example.com",
            old,
        )],
        101 => vec![message(
            102,
            "core",
            "ops",
            "Grace",
            "grace@example.com",
            fresh,
        )],
        _ => Vec::new(),
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    let poll = events::poll(
        &settings,
        events::parse_poll_params(&json!({
            "name": "zulip.message",
            "cursor": cursor(&base, 101),
            "maxAgeMs": 60_000,
        }))
        .expect("poll params"),
    )
    .await
    .expect("poll result");
    assert_eq!(
        poll["truncated"], false,
        "resuming past the aged message must not re-truncate"
    );
    assert_eq!(poll["events"][0]["data"]["message_id"], 102);
    assert_eq!(poll["cursor"], json!(cursor(&base, 102)));

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let resume = cursor(&base, 101);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &resume,
                    json!({"maxAgeMs": 60_000}),
                ))
                .await;
            let active = wire.next().await;
            assert_eq!(active["method"], "notifications/events/active");
            assert_eq!(active["params"]["cursor"], resume);
            assert_eq!(active["params"]["truncated"], false);

            let event = wire.next().await;
            assert_eq!(event["method"], "notifications/events/event");
            assert_eq!(event["params"]["data"]["message_id"], 102);

            for frame in wire.quiet().await {
                assert_ne!(
                    frame["params"]["truncated"],
                    json!(true),
                    "a resumed stream must not repeat the aged truncation: {frame}"
                );
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn duplicate_stream_id_aborts_the_first_stream_and_answers_once() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_after(&mock, |_| Vec::new()).await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(500, 5_000, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);
            let request = stream_request(json!(1), &start, json!({}));

            server.handle_line(&request).await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            server.handle_line(&request).await;
            let duplicate = wire.next().await;
            assert_eq!(duplicate["id"], json!(1));
            assert_eq!(duplicate["error"]["code"], json!(-32600));
            assert_eq!(duplicate["error"]["message"], "duplicate active request id");

            let after = wire.quiet().await;
            for frame in &after {
                assert_ne!(
                    subscription_id(frame),
                    &json!(1),
                    "the aborted stream must not emit: {frame}"
                );
                assert_ne!(frame["id"], json!(1));
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn null_cursor_setup_fails_at_the_stream_cap_before_the_upstream_probe() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let latest_calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&latest_calls);
    mount_with(&mock, move |request: &wiremock::Request| {
        let anchor = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "anchor")
            .map(|(_, value)| value.to_string());
        if anchor.as_deref() == Some("newest") {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": [],
        }))
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(5_000, 5_000, 1), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            server
                .handle_line(&null_cursor_stream_request(json!(2), json!({})))
                .await;
            let response = wire.next().await;
            assert_eq!(response["id"], json!(2));
            assert_eq!(response["error"]["code"], json!(-32603));
            assert!(response["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("too many")));
            assert_eq!(
                latest_calls.load(Ordering::SeqCst),
                0,
                "an over-cap null-cursor stream must not probe upstream"
            );

            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn stalled_null_cursor_setup_times_out_and_releases_capacity() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_with(&mock, |request: &wiremock::Request| {
        let anchor = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "anchor")
            .map(|(_, value)| value.to_string());
        if anchor.as_deref() == Some("newest") {
            return ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(2))
                .set_body_json(json!({
                    "result": "success",
                    "msg": "",
                    "messages": [],
                }));
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": [],
        }))
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(
                tx,
                server_config_with_timeout(5_000, 5_000, 1, Duration::from_millis(50)),
                tools,
            );
            let mut wire = Wire::new(rx);

            let started = Instant::now();
            server
                .handle_line(&null_cursor_stream_request(json!(1), json!({})))
                .await;
            let response = wire.next().await;
            assert_eq!(response["id"], json!(1));
            assert_eq!(response["error"]["code"], json!(-32603));
            assert!(response["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("timed out")));
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "setup must be bounded by the request timeout"
            );

            // The timed-out setup must have released its reserved slot.
            server
                .handle_line(&stream_request(json!(2), &cursor(&base, 100), json!({})))
                .await;
            let active = wire
                .notification("notifications/events/active", &json!(2))
                .await;
            assert_eq!(subscription_id(&active), &json!(2));

            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn null_cursor_stream_starts_at_the_newest_message() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_with(&mock, move |request: &wiremock::Request| {
        let anchor = request
            .url
            .query_pairs()
            .find(|(key, _)| key == "anchor")
            .map(|(_, value)| value.to_string());
        let messages = if anchor.as_deref() == Some("newest") {
            vec![message(
                777,
                "core",
                "ops",
                "Grace",
                "grace@example.com",
                timestamp,
            )]
        } else {
            Vec::new()
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": messages,
        }))
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);

            server
                .handle_line(&null_cursor_stream_request(json!(1), json!({})))
                .await;
            let active = wire
                .notification("notifications/events/active", &json!(1))
                .await;
            assert_eq!(
                active["params"]["cursor"],
                json!(cursor(&base, 777)),
                "a null-cursor stream must start at the resolved newest id, not 0"
            );
            assert_eq!(active["params"]["truncated"], false);

            for frame in wire.quiet().await {
                assert_ne!(
                    frame.get("method").and_then(Value::as_str),
                    Some("notifications/events/event"),
                    "setup must resolve the newest id instead of replaying from 0: {frame}"
                );
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn shared_fetch_uses_the_minimum_cursor_and_per_subscriber_filters() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    let calls = mount_after(&mock, move |anchor| {
        if anchor != 100 {
            return Vec::new();
        }
        vec![
            message(
                101,
                "core",
                "alpha",
                "Grace",
                "grace@example.com",
                timestamp,
            ),
            message(102, "core", "beta", "Grace", "grace@example.com", timestamp),
            message(103, "core", "alpha", "Bob", "bob@example.com", timestamp),
        ]
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(1_000, 200, 8), tools);
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &start,
                    json!({"arguments": {"stream": "core", "topic": "alpha"}}),
                ))
                .await;
            server
                .handle_line(&stream_request(
                    json!(2),
                    &cursor(&base, 102),
                    json!({"arguments": {"sender_email": "bob@example.com"}}),
                ))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;
            wire.notification("notifications/events/active", &json!(2))
                .await;

            let first = wire
                .notification("notifications/events/event", &json!(1))
                .await;
            let last = wire
                .notification("notifications/events/event", &json!(1))
                .await;
            let second = wire
                .notification("notifications/events/event", &json!(2))
                .await;
            assert_eq!(first["params"]["data"]["message_id"], 101);
            assert_eq!(last["params"]["data"]["message_id"], 103);
            assert_eq!(second["params"]["data"]["message_id"], 103);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "both subscriptions must share one fetch from the minimum cursor"
            );

            let mut advanced = false;
            for _ in 0..20 {
                let heartbeat = wire
                    .notification("notifications/events/heartbeat", &json!(2))
                    .await;
                if heartbeat["params"]["cursor"] == json!(cursor(&base, 103)) {
                    advanced = true;
                    break;
                }
            }
            assert!(
                advanced,
                "the second subscription must advance past its filtered 102"
            );

            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn idle_streams_are_exempt_from_the_request_timeout() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    mount_after(&mock, |_| Vec::new()).await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(
                tx,
                server_config_with_timeout(30_000, 50, 8, Duration::from_millis(50)),
                tools,
            );
            let mut wire = Wire::new(rx);
            let start = cursor(&base, 100);

            server
                .handle_line(&stream_request(json!(1), &start, json!({})))
                .await;
            wire.notification("notifications/events/active", &json!(1))
                .await;

            tokio::time::sleep(Duration::from_millis(150)).await;
            let heartbeat = wire
                .notification("notifications/events/heartbeat", &json!(1))
                .await;
            assert_eq!(heartbeat["params"]["cursor"], start);

            let after = wire.quiet().await;
            for frame in &after {
                assert_ne!(frame["id"], json!(1), "a stream must not time out: {frame}");
                assert_ne!(
                    frame.get("method").and_then(Value::as_str),
                    Some("notifications/events/error"),
                    "a stream must not report a request timeout: {frame}"
                );
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn poll_and_stream_agree_that_filtered_old_messages_are_not_truncation() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let timestamp = now();
    mount_after(&mock, move |anchor| {
        if anchor != 100 {
            return Vec::new();
        }
        vec![
            message(
                101,
                "other",
                "ops",
                "Grace",
                "grace@example.com",
                timestamp - 3_600,
            ),
            message(102, "core", "ops", "Grace", "grace@example.com", timestamp),
        ]
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    let params = events::parse_poll_params(&json!({
        "name": "zulip.message",
        "arguments": {"stream": "core"},
        "cursor": cursor(&base, 100),
        "maxAgeMs": 60_000,
    }))
    .expect("poll params");
    let poll = events::poll(&settings, params).await.expect("poll result");
    assert_eq!(poll["truncated"], false);
    assert_eq!(poll["events"][0]["data"]["message_id"], 102);

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &cursor(&base, 100),
                    json!({"arguments": {"stream": "core"}, "maxAgeMs": 60_000}),
                ))
                .await;
            let active = wire.next().await;
            assert_eq!(active["params"]["truncated"], false);
            let event = wire.next().await;
            assert_eq!(event["params"]["data"]["message_id"], 102);
            for frame in wire.quiet().await {
                assert_ne!(frame["params"]["truncated"], json!(true), "{frame}");
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}

#[tokio::test]
async fn poll_and_stream_agree_that_matching_old_messages_are_truncation() {
    let dir = TempDir::new().expect("temp dir");
    let mock = MockServer::start().await;
    let old = now() - 3_600;
    mount_after(&mock, move |anchor| {
        if anchor != 100 {
            return Vec::new();
        }
        vec![message(
            101,
            "core",
            "ops",
            "Grace",
            "grace@example.com",
            old,
        )]
    })
    .await;
    let settings = settings(dir.path(), &mock.uri());
    let base = mock.uri();

    let params = events::parse_poll_params(&json!({
        "name": "zulip.message",
        "arguments": {"stream": "core"},
        "cursor": cursor(&base, 100),
        "maxAgeMs": 60_000,
    }))
    .expect("poll params");
    let poll = events::poll(&settings, params).await.expect("poll result");
    assert_eq!(poll["truncated"], true);
    assert_eq!(poll["events"], json!([]));
    assert_eq!(
        poll["cursor"],
        json!(cursor(&base, 101)),
        "poll advances past an aged match"
    );

    LocalSet::new()
        .run_until(async {
            let (tx, rx) = mpsc::channel(64);
            let tools = Rc::new(ToolContext::with_settings(Rc::clone(&settings)));
            let mut server = Server::with_context(tx, server_config(30, 5_000, 8), tools);
            let mut wire = Wire::new(rx);

            server
                .handle_line(&stream_request(
                    json!(1),
                    &cursor(&base, 100),
                    json!({"arguments": {"stream": "core"}, "maxAgeMs": 60_000}),
                ))
                .await;
            let active = wire.next().await;
            assert_eq!(active["params"]["truncated"], false);
            let truncated = wire
                .notification("notifications/events/active", &json!(1))
                .await;
            assert_eq!(truncated["params"]["truncated"], true);
            assert_eq!(
                truncated["params"]["cursor"], poll["cursor"],
                "the stream announces the same advanced cursor poll returned"
            );
            for frame in wire.quiet().await {
                assert_ne!(
                    frame.get("method").and_then(Value::as_str),
                    Some("notifications/events/event"),
                    "{frame}"
                );
            }
            server.drain(Duration::from_millis(50)).await;
        })
        .await;
}
