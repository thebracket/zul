//! Handler tests for the Zulip read tools.
//!
//! Every test injects a `libzul::App` pointed at a temporary SQLite cache, so
//! nothing here reads the developer's real configuration or credentials.

use libzul::cache::Cache;
use libzul::zulip::models::{ZulipMessage, ZulipStream};
use libzul::zulip::normalize;
use libzul::{
    App, AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zul_mcp::tools::{call, invalid_arguments_result, ToolContext, ToolFailure};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs() as i64
}

fn cache_path(dir: &Path) -> std::path::PathBuf {
    dir.join("cache.sqlite")
}

fn settings(dir: &Path) -> Settings {
    Settings {
        zulip: ZulipConfig::default(),
        cache: CacheConfig::default(),
        ui: UiConfig::default(),
        safety: SafetyConfig::default(),
        digest: DigestConfig::default(),
        themes: BTreeMap::new(),
        paths: AppPaths {
            config_file: dir.join("config.toml"),
            cache_file: cache_path(dir),
            secret_file: dir.join("api_key"),
        },
    }
}

fn context(dir: &Path) -> ToolContext {
    let app = App::open(settings(dir), false).expect("open temp cache");
    ToolContext::with_app(Rc::new(app))
}

fn seed(dir: &Path, streams: &[ZulipStream], messages: &[ZulipMessage]) {
    let cache = Cache::open(&cache_path(dir)).expect("open seed cache");
    cache.upsert_streams(streams).expect("seed streams");
    cache.upsert_messages(messages).expect("seed messages");
}

fn stream(id: i64, name: &str, description: Option<&str>) -> ZulipStream {
    ZulipStream {
        id,
        name: name.to_string(),
        description: description.map(ToString::to_string),
    }
}

fn message(
    id: i64,
    stream_name: &str,
    topic: &str,
    sender: &str,
    timestamp: i64,
    content: &str,
) -> ZulipMessage {
    normalize::message(json!({
        "id": id,
        "stream_id": 1,
        "display_recipient": stream_name,
        "subject": topic,
        "sender_id": 2,
        "sender_full_name": sender,
        "sender_email": "ada@example.com",
        "timestamp": timestamp,
        "content": content,
        "permalink": format!("https://chat.example/#narrow/stream/{id}"),
    }))
    .expect("normalize fixture")
}

async fn invoke(context: &ToolContext, name: &str, arguments: Value) -> Value {
    match call(context, name, Some(&arguments)).await {
        Ok(result) => result,
        Err(ToolFailure::InvalidArguments(message)) => invalid_arguments_result(&message),
        Err(ToolFailure::UnknownTool(name)) => panic!("unexpected unknown tool: {name}"),
    }
}

fn structured(result: &Value) -> &Value {
    assert_eq!(result["resultType"], "complete", "result: {result}");
    assert_ne!(result["isError"], true, "unexpected tool error: {result}");
    let structured = result
        .get("structuredContent")
        .unwrap_or_else(|| panic!("missing structuredContent: {result}"));
    let text = result["content"][0]["text"].as_str().expect("text content");
    let parsed: Value = serde_json::from_str(text).expect("text content is compact JSON");
    assert_eq!(&parsed, structured, "text and structuredContent diverged");
    structured
}

fn assert_tool_error(result: &Value) {
    assert_eq!(result["resultType"], "complete", "result: {result}");
    assert_eq!(result["isError"], true, "expected a tool error: {result}");
    assert!(
        result.get("structuredContent").is_none(),
        "tool errors must not carry a structured payload: {result}"
    );
    assert!(
        result["content"][0]["text"]
            .as_str()
            .is_some_and(|text| !text.is_empty()),
        "tool errors need a text explanation: {result}"
    );
}

#[tokio::test]
async fn lists_streams_topics_and_topic_messages() {
    let dir = TempDir::new().expect("temp dir");
    let base = now();
    let messages = [
        message(1, "core team", "ops", "Ada", base - 300, "<p>first</p>"),
        message(2, "core team", "ops", "Grace", base - 200, "<p>second</p>"),
        message(3, "core team", "ops", "Ada", base - 100, "<p>third</p>"),
    ];
    seed(
        dir.path(),
        &[
            stream(1, "core team", Some("Core")),
            stream(2, "Alpha", None),
        ],
        &messages,
    );
    let context = context(dir.path());

    let listed = invoke(&context, "list_streams", json!({})).await;
    let listed = structured(&listed);
    assert_eq!(
        listed["streams"],
        json!([
            {"id": 2, "name": "Alpha", "description": null},
            {"id": 1, "name": "core team", "description": "Core"},
        ])
    );

    let topics = invoke(&context, "list_topics", json!({"stream": "core team"})).await;
    let topics = structured(&topics);
    assert_eq!(topics["stream"], "core team");
    let topics = topics["topics"].as_array().expect("topics array");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0]["name"], "ops");
    assert_eq!(topics[0]["message_count"], 3);
    assert_eq!(
        topics[0]["last_message_at"],
        messages[2].timestamp.to_rfc3339()
    );

    let unknown = invoke(&context, "list_topics", json!({"stream": "missing"})).await;
    assert_eq!(structured(&unknown)["topics"], json!([]));

    let read = invoke(
        &context,
        "read_topic",
        json!({"stream": "core team", "topic": "ops", "limit": 2}),
    )
    .await;
    let read = structured(&read);
    assert_eq!(read["stream"], "core team");
    assert_eq!(read["topic"], "ops");
    assert_eq!(read["count"], 2);
    let returned = read["messages"].as_array().expect("messages array");
    assert_eq!(
        returned
            .iter()
            .map(|m| m["id"].as_i64())
            .collect::<Vec<_>>(),
        vec![Some(2), Some(3)],
        "read_topic keeps the newest messages, oldest first"
    );

    let newest = &returned[1];
    let mut keys: Vec<&str> = newest
        .as_object()
        .expect("message object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "id",
            "permalink",
            "sender_email",
            "sender_name",
            "stream",
            "text",
            "text_truncated",
            "timestamp",
            "topic",
        ],
        "message projection must not leak raw_json or rendered_html"
    );
    assert_eq!(newest["stream"], "core team");
    assert_eq!(newest["topic"], "ops");
    assert_eq!(newest["sender_name"], "Ada");
    assert_eq!(newest["sender_email"], "ada@example.com");
    assert_eq!(newest["timestamp"], messages[2].timestamp.to_rfc3339());
    assert_eq!(newest["text"], "third");
    assert_eq!(newest["text_truncated"], false);
    assert_eq!(
        newest["permalink"],
        format!("https://chat.example/#narrow/stream/{}", 3)
    );

    let all = invoke(
        &context,
        "read_topic",
        json!({"stream": "core team", "topic": "ops"}),
    )
    .await;
    assert_eq!(structured(&all)["count"], 3);
}

#[tokio::test]
async fn read_topic_truncates_long_text() {
    let dir = TempDir::new().expect("temp dir");
    let long = "x".repeat(4200);
    seed(
        dir.path(),
        &[],
        &[message(
            7,
            "core team",
            "ops",
            "Ada",
            now(),
            &format!("<p>{long}</p>"),
        )],
    );
    let context = context(dir.path());

    let read = invoke(
        &context,
        "read_topic",
        json!({"stream": "core team", "topic": "ops"}),
    )
    .await;
    let read = structured(&read);
    let text = read["messages"][0]["text"].as_str().expect("message text");
    assert_eq!(text.chars().count(), 4000);
    assert_eq!(read["messages"][0]["text_truncated"], true);
}

#[tokio::test]
async fn recent_returns_newest_first_and_applies_the_window() {
    let dir = TempDir::new().expect("temp dir");
    let base = now();
    seed(
        dir.path(),
        &[],
        &[
            message(1, "core team", "ops", "Ada", base - 3600, "<p>hour</p>"),
            message(2, "core team", "ops", "Ada", base - 30 * 3600, "<p>old</p>"),
            message(3, "core team", "ops", "Ada", base - 60, "<p>minute</p>"),
        ],
    );
    let context = context(dir.path());

    let recent = invoke(&context, "recent", json!({"hours": 24, "limit": 2})).await;
    let recent = structured(&recent);
    assert_eq!(recent["hours"], 24);
    assert_eq!(recent["count"], 2);
    assert_eq!(
        recent["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .map(|m| m["id"].as_i64())
            .collect::<Vec<_>>(),
        vec![Some(3), Some(1)],
        "recent is newest-first and excludes messages older than the window"
    );
}

#[tokio::test]
async fn search_returns_cached_matches_and_respects_the_limit() {
    let dir = TempDir::new().expect("temp dir");
    let base = now();
    seed(
        dir.path(),
        &[],
        &[
            message(
                1,
                "core team",
                "ops",
                "Ada",
                base - 300,
                "<p>installer bug</p>",
            ),
            message(
                2,
                "core team",
                "ops",
                "Ada",
                base - 200,
                "<p>installer crash</p>",
            ),
            message(3, "core team", "ops", "Ada", base - 100, "<p>unrelated</p>"),
        ],
    );
    let context = context(dir.path());

    let found = invoke(&context, "search", json!({"query": "installer"})).await;
    let found = structured(&found);
    assert_eq!(found["query"], "installer");
    assert_eq!(found["count"], 2);
    for message in found["messages"].as_array().expect("messages") {
        assert!(message["text"]
            .as_str()
            .is_some_and(|text| text.contains("installer")));
    }

    let limited = invoke(
        &context,
        "search",
        json!({"query": "installer", "limit": 1}),
    )
    .await;
    assert_eq!(structured(&limited)["count"], 1);

    let blank = invoke(&context, "search", json!({"query": "   "})).await;
    assert_tool_error(&blank);

    let missing = invoke(&context, "search", json!({})).await;
    assert_tool_error(&missing);
}

#[tokio::test]
async fn argument_edges_are_tool_errors() {
    let dir = TempDir::new().expect("temp dir");
    let context = context(dir.path());

    for arguments in [
        json!({"stream": "core", "topic": "ops", "limit": 0}),
        json!({"stream": "core", "topic": "ops", "limit": 501}),
        json!({"stream": "core", "topic": "ops", "limit": "10"}),
        json!({"stream": "core"}),
    ] {
        let result = invoke(&context, "read_topic", arguments).await;
        assert_tool_error(&result);
    }

    for arguments in [
        json!({"hours": 0}),
        json!({"hours": 169}),
        json!({"limit": 0}),
        json!({"limit": 501}),
    ] {
        let result = invoke(&context, "recent", arguments).await;
        assert_tool_error(&result);
    }

    let unknown = invoke(&context, "recent", json!({"stream": "core"})).await;
    assert_tool_error(&unknown);
}

#[tokio::test]
async fn sync_without_credentials_is_a_tool_error() {
    let dir = TempDir::new().expect("temp dir");
    let context = context(dir.path());

    let result = invoke(&context, "sync", json!({})).await;
    assert_tool_error(&result);
    let text = result["content"][0]["text"].as_str().expect("text content");
    assert!(text.contains("sync failed"), "message: {text}");
    assert!(!text.contains('\n'), "message must stay one line: {text}");
}

#[tokio::test]
async fn sync_populates_the_cache_for_later_reads() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/users/me/subscriptions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "subscriptions": [{
                "stream_id": 1,
                "name": "core team",
                "description": "Core",
            }],
        })))
        .expect(1)
        .mount(&server)
        .await;
    let message_body = json!({
        "result": "success",
        "msg": "",
        "messages": [{
            "id": 11,
            "stream_id": 1,
            "display_recipient": "core team",
            "subject": "ops",
            "sender_id": 2,
            "sender_full_name": "Ada",
            "sender_email": "ada@example.com",
            "timestamp": now(),
            "content": "<p>hello from wiremock</p>",
        }],
    });
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .and(query_param("anchor", "newest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(message_body.clone()))
        .expect(1)
        .mount(&server)
        .await;
    // `sync` refreshes cached messages authored by the current account.
    Mock::given(method("GET"))
        .and(path("/api/v1/messages"))
        .and(query_param("anchor", "11"))
        .respond_with(ResponseTemplate::new(200).set_body_json(message_body))
        .expect(1)
        .mount(&server)
        .await;

    let mut settings = settings(dir.path());
    settings.zulip.base_url = Some(server.uri());
    settings.zulip.email = Some("ada@example.com".to_string());
    settings.zulip.api_key = Some("test-key".to_string());
    let app = App::open(settings, false).expect("open temp cache");
    let context = ToolContext::with_app(Rc::new(app));

    let synced = invoke(&context, "sync", json!({"hours": 1})).await;
    let synced = structured(&synced);
    assert_eq!(
        synced,
        &json!({
            "streams": 1,
            "messages": 1,
            "refreshed_messages": 1,
            "hours": 1,
            "offline": false
        })
    );

    let read = invoke(
        &context,
        "read_topic",
        json!({"stream": "core team", "topic": "ops"}),
    )
    .await;
    let read = structured(&read);
    assert_eq!(read["count"], 1);
    assert_eq!(read["messages"][0]["id"], 11);
    assert_eq!(read["messages"][0]["text"], "hello from wiremock");
}
