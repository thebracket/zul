//! Handler and protocol tests for the MCP Events extension.
//!
//! Every poll test points libzul at a wiremock realm and a temporary cache path,
//! so nothing here reads the developer's real configuration or credentials.

use libzul::{AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zul_mcp::events::{self, EventsError};
use zul_mcp::protocol;

const API_KEY: &str = "zul-mcp-events-test-key-8d1a-DO-NOT-LEAK";

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs() as i64
}

fn settings(dir: &Path, server: &MockServer) -> Settings {
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
    settings.zulip.base_url = Some(server.uri());
    settings.zulip.email = Some("ada@example.com".to_string());
    settings.zulip.api_key = Some(API_KEY.to_string());
    settings
}

fn stream_message(
    id: i64,
    stream: &str,
    topic: &str,
    sender: &str,
    email: &str,
    timestamp: i64,
    content: &str,
) -> Value {
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
        "content": content,
        "permalink": format!("https://chat.example/#narrow/stream/{id}"),
    })
}

fn direct_message(id: i64, sender: &str, email: &str, timestamp: i64, content: &str) -> Value {
    json!({
        "id": id,
        "type": "private",
        "display_recipient": [{"id": 2, "email": email, "full_name": sender}],
        "subject": "",
        "sender_id": 2,
        "sender_full_name": sender,
        "sender_email": email,
        "timestamp": timestamp,
        "content": content,
    })
}

async fn mock_messages_after(server: &MockServer, anchor: i64, messages: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .and(query_param("anchor", anchor.to_string()))
        .and(query_param("num_before", "0"))
        .and(query_param("num_after", "1000"))
        .and(query_param("include_anchor", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": messages,
        })))
        .mount(server)
        .await;
}

async fn poll(settings: &Settings, params: Value) -> Value {
    let params = events::parse_poll_params(&params).expect("valid poll params");
    events::poll(settings, params).await.expect("poll result")
}

fn event_ids(result: &Value) -> Vec<i64> {
    result["events"]
        .as_array()
        .expect("events array")
        .iter()
        .map(|event| {
            event["data"]["message_id"]
                .as_i64()
                .expect("message_id in event data")
        })
        .collect()
}

fn assert_invalid_params(params: Value) {
    assert!(
        matches!(
            events::parse_poll_params(&params),
            Err(EventsError::InvalidParams(_))
        ),
        "expected invalid params for {params}"
    );
}

#[test]
fn discovery_and_events_list_advertise_the_extension() {
    let discover = protocol::discover_result();
    assert_eq!(
        discover["capabilities"],
        json!({
            "tools": {},
            "extensions": {
                "io.modelcontextprotocol/events": {},
            },
        })
    );
    assert!(
        discover["capabilities"].get("events").is_none(),
        "events must not be a top-level capability: {discover}"
    );
    assert!(
        discover["capabilities"]["extensions"]["io.modelcontextprotocol/events"]
            .get("listChanged")
            .is_none(),
        "the extension must not advertise listChanged: {discover}"
    );

    let list = events::list_result();
    assert_eq!(list["resultType"], "complete");
    let descriptors = list["events"].as_array().expect("events array");
    assert_eq!(descriptors.len(), 1);
    let event = &descriptors[0];
    assert_eq!(event["name"], events::EVENT_NAME);
    assert_eq!(event["delivery"], json!(["poll", "push"]));
    assert!(!event["description"]
        .as_str()
        .expect("description")
        .is_empty());

    let input = &event["inputSchema"];
    assert_eq!(input["type"], "object");
    assert_eq!(input["required"], json!([]));
    assert_eq!(input["additionalProperties"], false);
    for name in ["stream", "topic", "sender_email"] {
        assert_eq!(input["properties"][name]["type"], "string");
    }
    assert_eq!(input["properties"]["exclude_self"]["type"], "boolean");
    assert_eq!(input["properties"]["exclude_self"]["default"], true);

    let payload = &event["payloadSchema"];
    assert_eq!(payload["type"], "object");
    assert_eq!(payload["additionalProperties"], false);
    assert_eq!(payload["properties"]["message_id"]["type"], "integer");
    assert_eq!(
        payload["properties"]["message_type"]["enum"],
        json!(["stream", "private"])
    );
    assert_eq!(payload["properties"]["timestamp"]["format"], "date-time");
    for name in ["stream", "topic", "sender_email", "permalink"] {
        assert_eq!(
            payload["properties"][name]["type"],
            json!(["string", "null"])
        );
    }
    let required = payload["required"].as_array().expect("required array");
    let properties = payload["properties"]
        .as_object()
        .expect("payload properties");
    assert_eq!(required.len(), properties.len());
    for name in properties.keys() {
        assert!(
            required.contains(&Value::String(name.clone())),
            "{name} must be required in the payload schema"
        );
    }
}

#[tokio::test]
async fn null_cursor_starts_from_now_without_replaying_backlog() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .and(query_param("anchor", "newest"))
        .and(query_param("num_before", "0"))
        .and(query_param("num_after", "0"))
        .and(query_param("include_anchor", "true"))
        .and(query_param("apply_markdown", "true"))
        .and(query_param("client_gravatar", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": [stream_message(
                900,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                now(),
                "<p>newest</p>",
            )],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let settings = settings(dir.path(), &server);
    let result = poll(
        &settings,
        json!({"name": events::EVENT_NAME, "cursor": null}),
    )
    .await;

    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["events"], json!([]), "null must not replay backlog");
    assert_eq!(result["cursor"], json!(format!("v1:{}|900", server.uri())));
    assert_eq!(result["truncated"], false);
    assert_eq!(result["hasMore"], false);
    assert_eq!(result["nextPollMs"], 15_000);
    assert!(!result.to_string().contains(API_KEY));
    server.verify().await;
}

#[tokio::test]
async fn null_cursor_on_an_empty_realm_uses_zero_and_normalizes_the_realm() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .and(query_param("anchor", "newest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "messages": [],
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut settings = settings(dir.path(), &server);
    settings.zulip.base_url = Some(format!("{}/", server.uri()));
    let result = poll(
        &settings,
        json!({"name": events::EVENT_NAME, "cursor": null}),
    )
    .await;

    assert_eq!(result["events"], json!([]));
    assert_eq!(result["cursor"], json!(format!("v1:{}|0", server.uri())));
    assert_eq!(result["truncated"], false);
    assert_eq!(result["hasMore"], false);
    server.verify().await;
}

#[tokio::test]
async fn filters_apply_exact_stream_topic_sender_and_exclude_self() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    let base = now();
    mock_messages_after(
        &server,
        0,
        vec![
            stream_message(
                10,
                "core team",
                "ops",
                "Ada",
                "ada@example.com",
                base - 50,
                "<p>a</p>",
            ),
            stream_message(
                11,
                "core team",
                "release",
                "Grace",
                "grace@example.com",
                base - 40,
                "<p>b</p>",
            ),
            stream_message(
                12,
                "other",
                "ops",
                "Ada",
                "ada@example.com",
                base - 30,
                "<p>c</p>",
            ),
            direct_message(13, "Grace", "grace@example.com", base - 20, "<p>d</p>"),
            stream_message(
                14,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 10,
                "<p>e</p>",
            ),
        ],
    )
    .await;
    let settings = settings(dir.path(), &server);
    let cursor = json!(format!("v1:{}|0", server.uri()));

    let combined = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"stream": "core team", "topic": "ops", "exclude_self": false},
            "cursor": cursor.clone(),
        }),
    )
    .await;
    assert_eq!(event_ids(&combined), vec![10, 14]);

    let default_exclude = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "cursor": cursor.clone(),
        }),
    )
    .await;
    assert_eq!(
        event_ids(&default_exclude),
        vec![11, 13, 14],
        "exclude_self defaults to true and skips Ada's messages"
    );

    let sender = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"sender_email": "ADA@EXAMPLE.COM", "exclude_self": false},
            "cursor": cursor.clone(),
        }),
    )
    .await;
    assert_eq!(event_ids(&sender), vec![10, 12]);

    let stream_only = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"stream": "core team", "exclude_self": false},
            "cursor": cursor.clone(),
        }),
    )
    .await;
    assert_eq!(event_ids(&stream_only), vec![10, 11, 14]);
    assert!(
        !event_ids(&stream_only).contains(&13),
        "a stream filter must never match a direct message"
    );

    let wrong_topic = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"stream": "core team", "topic": "Ops"},
            "cursor": cursor,
        }),
    )
    .await;
    assert_eq!(event_ids(&wrong_topic), Vec::<i64>::new());

    assert_invalid_params(json!({
        "name": events::EVENT_NAME,
        "arguments": {"topic": "ops"},
    }));
}

#[tokio::test]
async fn direct_messages_use_null_stream_and_topic() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    let base = now();
    mock_messages_after(
        &server,
        0,
        vec![
            stream_message(
                10,
                "core team",
                "ops",
                "Ada",
                "ada@example.com",
                base - 10,
                "<p>a</p>",
            ),
            direct_message(13, "Grace", "grace@example.com", base - 5, "<p>dm</p>"),
        ],
    )
    .await;
    let settings = settings(dir.path(), &server);
    let result = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"exclude_self": false},
            "cursor": format!("v1:{}|0", server.uri()),
        }),
    )
    .await;

    assert_eq!(event_ids(&result), vec![10, 13]);
    let stream_event = &result["events"][0];
    assert_eq!(stream_event["data"]["message_type"], "stream");
    assert_eq!(stream_event["data"]["stream"], "core team");
    assert_eq!(stream_event["data"]["topic"], "ops");

    let direct = &result["events"][1];
    assert_eq!(direct["name"], events::EVENT_NAME);
    assert_eq!(direct["eventId"], json!(format!("{}#13", server.uri())));
    assert_eq!(direct["cursor"], json!(format!("v1:{}|13", server.uri())));
    let data = &direct["data"];
    assert_eq!(data["message_id"], 13);
    assert_eq!(data["realm"], server.uri());
    assert_eq!(data["message_type"], "private");
    assert!(
        data["stream"].is_null(),
        "direct messages have no filterable stream"
    );
    assert!(data["topic"].is_null());
    assert_eq!(data["sender_name"], "Grace");
    assert_eq!(data["sender_email"], "grace@example.com");
    assert_eq!(data["text"], "dm");
    assert_eq!(data["text_truncated"], false);
    assert!(data["permalink"].is_null());
    chrono::DateTime::parse_from_rfc3339(data["timestamp"].as_str().expect("timestamp"))
        .expect("RFC3339 timestamp");

    let mut keys: Vec<&str> = data
        .as_object()
        .expect("event data object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "message_id",
            "message_type",
            "permalink",
            "realm",
            "sender_email",
            "sender_name",
            "stream",
            "text",
            "text_truncated",
            "timestamp",
            "topic",
        ],
        "event data must match the payload schema and never leak raw fields"
    );
    assert!(!result.to_string().contains("raw_json"));
    assert!(!result.to_string().contains("rendered_html"));
}

#[tokio::test]
async fn payload_text_is_capped_at_two_thousand_characters() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    let base = now();
    let long = "x".repeat(2100);
    mock_messages_after(
        &server,
        0,
        vec![
            stream_message(
                10,
                "core team",
                "ops",
                "Ada",
                "ada@example.com",
                base - 10,
                &format!("<p>{long}</p>"),
            ),
            stream_message(
                11,
                "core team",
                "ops",
                "Ada",
                "ada@example.com",
                base - 5,
                &format!("<p>{}</p>", "y".repeat(2000)),
            ),
        ],
    )
    .await;
    let settings = settings(dir.path(), &server);
    let result = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "arguments": {"exclude_self": false},
            "cursor": format!("v1:{}|0", server.uri()),
        }),
    )
    .await;

    let events = result["events"].as_array().expect("events");
    let truncated = &events[0]["data"];
    assert_eq!(
        truncated["text"].as_str().expect("text").chars().count(),
        2000
    );
    assert_eq!(truncated["text_truncated"], true);

    let exact = &events[1]["data"];
    assert_eq!(exact["text"].as_str().expect("text").chars().count(), 2000);
    assert_eq!(exact["text_truncated"], false);
}

#[tokio::test]
async fn max_events_reports_has_more_and_resumes_from_the_last_emitted_event() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    let base = now();
    mock_messages_after(
        &server,
        0,
        vec![
            stream_message(
                22,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 5,
                "<p>22</p>",
            ),
            stream_message(
                20,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 15,
                "<p>20</p>",
            ),
            stream_message(
                21,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 10,
                "<p>21</p>",
            ),
        ],
    )
    .await;
    mock_messages_after(
        &server,
        21,
        vec![stream_message(
            22,
            "core team",
            "ops",
            "Grace",
            "grace@example.com",
            base - 5,
            "<p>22</p>",
        )],
    )
    .await;
    let settings = settings(dir.path(), &server);

    let first = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "cursor": format!("v1:{}|0", server.uri()),
            "maxEvents": 2,
        }),
    )
    .await;
    assert_eq!(
        event_ids(&first),
        vec![20, 21],
        "events are sorted ascending"
    );
    assert_eq!(first["hasMore"], true);
    assert_eq!(first["truncated"], false);
    assert_eq!(
        first["cursor"],
        json!(format!("v1:{}|21", server.uri())),
        "hasMore must resume from the last emitted event, not the last fetched one"
    );

    let second = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "cursor": first["cursor"].clone(),
            "maxEvents": 2,
        }),
    )
    .await;
    assert_eq!(event_ids(&second), vec![22]);
    assert_eq!(second["hasMore"], false);
    assert_eq!(second["cursor"], json!(format!("v1:{}|22", server.uri())));
}

#[tokio::test]
async fn max_age_marks_truncated_and_advances_past_skipped_messages() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    let base = now();
    mock_messages_after(
        &server,
        0,
        vec![
            stream_message(
                30,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 10,
                "<p>fresh</p>",
            ),
            stream_message(
                31,
                "core team",
                "ops",
                "Grace",
                "grace@example.com",
                base - 3600,
                "<p>old</p>",
            ),
        ],
    )
    .await;
    let settings = settings(dir.path(), &server);

    let aged = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "cursor": format!("v1:{}|0", server.uri()),
            "maxAgeMs": 60_000,
        }),
    )
    .await;
    assert_eq!(event_ids(&aged), vec![30]);
    assert_eq!(aged["truncated"], true);
    assert_eq!(aged["hasMore"], false);
    assert_eq!(
        aged["cursor"],
        json!(format!("v1:{}|31", server.uri())),
        "the cursor advances past a message skipped for age"
    );

    let complete = poll(
        &settings,
        json!({
            "name": events::EVENT_NAME,
            "cursor": format!("v1:{}|0", server.uri()),
        }),
    )
    .await;
    assert_eq!(event_ids(&complete), vec![30, 31]);
    assert_eq!(complete["truncated"], false);
}

#[tokio::test]
async fn malformed_and_foreign_cursors_are_rejected_without_contacting_zulip() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let settings = settings(dir.path(), &server);

    let cursors = vec![
        "bogus".to_string(),
        "v1:https://other.example|5".to_string(),
        format!("v1:{}/|5", server.uri()),
        format!("v1:{}|abc", server.uri()),
        format!("v1:{}|-1", server.uri()),
        format!("v1:{}|+5", server.uri()),
        format!("v1:{}|05", server.uri()),
    ];
    for cursor in cursors {
        let params = events::parse_poll_params(&json!({
            "name": events::EVENT_NAME,
            "cursor": cursor.clone(),
        }))
        .expect("structurally valid cursor type");
        let error = events::poll(&settings, params)
            .await
            .expect_err("cursor must be rejected");
        assert!(
            matches!(error, EventsError::InvalidParams(_)),
            "cursor {cursor:?} must be invalid params"
        );
        assert!(!error.message().contains(API_KEY));
    }
    server.verify().await;
}

#[test]
fn invalid_poll_params_are_rejected() {
    for params in [
        json!({}),
        json!({"name": 1}),
        json!({"name": "other.event"}),
        json!({"name": events::EVENT_NAME, "arguments": null}),
        json!({"name": events::EVENT_NAME, "arguments": {"stream": 1}}),
        json!({"name": events::EVENT_NAME, "arguments": {"unknown": true}}),
        json!({"name": events::EVENT_NAME, "arguments": {"exclude_self": "yes"}}),
        json!({"name": events::EVENT_NAME, "cursor": 1}),
        json!({"name": events::EVENT_NAME, "maxAgeMs": -1}),
        json!({"name": events::EVENT_NAME, "maxEvents": 0}),
        json!({"name": events::EVENT_NAME, "maxEvents": 201}),
        json!({"name": events::EVENT_NAME, "unexpected": true}),
    ] {
        assert_invalid_params(params);
    }
}

#[tokio::test]
async fn errors_never_include_the_api_key() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "error",
            "msg": "Invalid API key",
            "code": "UNAUTHORIZED",
            "messages": [],
        })))
        .mount(&server)
        .await;

    let mut settings = settings(dir.path(), &server);
    let params = events::parse_poll_params(&json!({
        "name": events::EVENT_NAME,
        "cursor": format!("v1:{}|0", server.uri()),
    }))
    .expect("valid params");
    let error = events::poll(&settings, params)
        .await
        .expect_err("upstream rejection must fail");
    assert!(matches!(error, EventsError::Internal(_)));
    assert!(error.message().contains("Invalid API key"));
    assert!(!error.message().contains(API_KEY));

    settings.zulip.api_key = None;
    let params = events::parse_poll_params(&json!({
        "name": events::EVENT_NAME,
        "cursor": format!("v1:{}|0", server.uri()),
    }))
    .expect("valid params");
    let error = events::poll(&settings, params)
        .await
        .expect_err("missing credentials must fail");
    assert!(matches!(error, EventsError::Internal(_)));
    assert!(error.message().contains("missing Zulip API key"));
    assert!(!error.message().contains(API_KEY));
    assert!(!error.message().contains('\n'), "errors stay one line");
}
