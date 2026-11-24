//! Handler tests for the Zulip send tool.
//!
//! Every test uses a temporary SQLite cache and a wiremock realm, so nothing
//! here reads the developer's configuration or contacts a real Zulip server.

use libzul::cache::{send_hash, Cache};
use libzul::{
    App, AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;
use tempfile::TempDir;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use zul_mcp::tools::{call, invalid_arguments_result, ToolContext, ToolFailure};

const API_KEY: &str = "zul-mcp-send-test-key-4c1f-DO-NOT-LEAK";
const STREAM: &str = "core-team";
const TOPIC: &str = "ops";
const MESSAGE: &str = "hello-from-mcp";
const SENT_ID: i64 = 42;

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

fn context_with_key(dir: &Path, server: &MockServer, api_key: Option<&str>) -> ToolContext {
    let mut settings = settings(dir);
    settings.zulip.base_url = Some(server.uri());
    settings.zulip.email = Some("ada@example.com".to_string());
    settings.zulip.api_key = api_key.map(ToString::to_string);
    let app = App::open(settings, false).expect("open temp cache");
    ToolContext::with_app(Rc::new(app))
}

fn context(dir: &Path, server: &MockServer) -> ToolContext {
    context_with_key(dir, server, Some(API_KEY))
}

async fn invoke(context: &ToolContext, arguments: Value) -> Value {
    match call(context, "send_message", Some(&arguments)).await {
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

fn assert_tool_error(result: &Value) -> String {
    assert_eq!(result["resultType"], "complete", "result: {result}");
    assert_eq!(result["isError"], true, "expected a tool error: {result}");
    assert!(
        result.get("structuredContent").is_none(),
        "tool errors must not carry a structured payload: {result}"
    );
    let text = result["content"][0]["text"]
        .as_str()
        .expect("tool errors need a text explanation");
    assert!(!text.is_empty(), "tool errors need a text explanation");
    assert!(!text.contains('\n'), "error messages stay one line: {text}");
    text.to_string()
}

fn success_response() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "result": "success",
        "msg": "",
        "id": SENT_ID,
    }))
}

fn send_mock() -> Mock {
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .and(body_string_contains("type=stream"))
        .and(body_string_contains(format!("to={STREAM}")))
        .and(body_string_contains(format!("topic={TOPIC}")))
        .and(body_string_contains(format!("content={MESSAGE}")))
        .respond_with(success_response())
}

#[tokio::test]
async fn dry_run_is_the_default_and_performs_no_http() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let context = context(dir.path(), &server);

    let result = invoke(
        &context,
        json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE}),
    )
    .await;
    let dry = structured(&result);
    assert_eq!(dry["status"], "dry_run");
    assert_eq!(dry["realm"], server.uri());
    assert_eq!(dry["account"], "ada@example.com");
    assert_eq!(dry["stream"], STREAM);
    assert_eq!(dry["topic"], TOPIC);
    assert_eq!(dry["message"], MESSAGE);
    assert_eq!(dry["content_hash"], send_hash(STREAM, TOPIC, MESSAGE));
    assert!(!result.to_string().contains(API_KEY));

    server.verify().await;
    let cache = Cache::open(&cache_path(dir.path())).expect("open cache");
    assert!(
        !cache
            .has_sent_hash(&send_hash(STREAM, TOPIC, MESSAGE))
            .expect("audit query"),
        "a dry run must not record a real send"
    );
}

#[tokio::test]
async fn dry_run_then_confirm_sends_exactly_once_and_records_it() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    send_mock().expect(1).mount(&server).await;
    let context = context(dir.path(), &server);
    let arguments = json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE});

    let planned = invoke(&context, arguments.clone()).await;
    assert_eq!(structured(&planned)["status"], "dry_run");

    let sent = invoke(
        &context,
        json!({
            "stream": STREAM,
            "topic": TOPIC,
            "message": MESSAGE,
            "confirm": true,
        }),
    )
    .await;
    let sent = structured(&sent);
    assert_eq!(sent["status"], "sent");
    assert_eq!(sent["realm"], server.uri());
    assert_eq!(sent["account"], "ada@example.com");
    assert_eq!(sent["stream"], STREAM);
    assert_eq!(sent["topic"], TOPIC);
    assert_eq!(sent["zulip_message_id"], SENT_ID);
    assert_eq!(sent["content_hash"], send_hash(STREAM, TOPIC, MESSAGE));
    assert!(
        sent.get("message").is_none(),
        "a sent result must not echo the message body: {sent}"
    );

    server.verify().await;
    let cache = Cache::open(&cache_path(dir.path())).expect("open cache");
    assert!(
        cache
            .has_sent_hash(&send_hash(STREAM, TOPIC, MESSAGE))
            .expect("audit query"),
        "a confirmed send must be recorded in the audit log"
    );
}

#[tokio::test]
async fn repeated_confirmed_send_is_blocked_as_a_duplicate() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    send_mock().expect(1).mount(&server).await;
    let context = context(dir.path(), &server);
    let arguments = json!({
        "stream": STREAM,
        "topic": TOPIC,
        "message": MESSAGE,
        "confirm": true,
    });

    let first = invoke(&context, arguments.clone()).await;
    assert_eq!(structured(&first)["status"], "sent");

    let second = invoke(&context, arguments).await;
    let text = assert_tool_error(&second);
    assert!(
        text.contains("duplicate send blocked"),
        "expected the libzul duplicate guard, got: {text}"
    );
    assert!(!second.to_string().contains(API_KEY));

    server.verify().await;
}

#[tokio::test]
async fn missing_credentials_are_a_tool_error_without_a_request_or_body_echo() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let context = context_with_key(dir.path(), &server, None);

    let result = invoke(
        &context,
        json!({
            "stream": STREAM,
            "topic": TOPIC,
            "message": "body-that-must-not-echo",
            "confirm": true,
        }),
    )
    .await;
    let text = assert_tool_error(&result);
    assert!(text.contains("send_message failed"), "message: {text}");
    assert!(
        text.contains("missing Zulip API key"),
        "the missing credential must be named: {text}"
    );
    assert!(
        !text.contains("body-that-must-not-echo"),
        "an auth failure must not echo the message body: {text}"
    );

    server.verify().await;
}

#[tokio::test]
async fn rejected_send_is_a_tool_error_without_leaking_the_key_or_body() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "error",
            "msg": "Invalid API key",
            "code": "UNAUTHORIZED",
        })))
        .expect(1)
        .mount(&server)
        .await;
    let context = context(dir.path(), &server);

    let result = invoke(
        &context,
        json!({
            "stream": STREAM,
            "topic": TOPIC,
            "message": "body-that-must-not-echo",
            "confirm": true,
        }),
    )
    .await;
    let text = assert_tool_error(&result);
    assert!(text.contains("send_message failed"), "message: {text}");
    assert!(
        text.contains("duplicate retry is blocked"),
        "the audit-first safety note must surface: {text}"
    );
    assert!(
        !text.contains("body-that-must-not-echo"),
        "a rejected send must not echo the message body: {text}"
    );
    assert!(!text.contains(API_KEY), "the API key leaked: {text}");

    server.verify().await;
}

#[tokio::test]
async fn invalid_send_arguments_are_tool_errors_without_a_request() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/messages"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let context = context(dir.path(), &server);
    let oversized = "x".repeat(10_001);

    let cases: Vec<(Value, &str)> = vec![
        (
            json!({"stream": " ", "topic": TOPIC, "message": MESSAGE}),
            "stream must not be blank",
        ),
        (
            json!({"topic": TOPIC, "message": MESSAGE}),
            "missing required argument: stream",
        ),
        (
            json!({"stream": STREAM, "topic": "", "message": MESSAGE}),
            "topic must not be blank",
        ),
        (
            json!({"stream": STREAM, "topic": TOPIC, "message": "\n"}),
            "message must not be blank",
        ),
        (
            json!({"stream": STREAM, "topic": TOPIC, "message": oversized}),
            "message must be at most 10000 characters",
        ),
        (
            json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE, "confirm": "yes"}),
            "confirm must be a boolean",
        ),
        (
            json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE, "extra": 1}),
            "does not accept these arguments",
        ),
    ];

    for (arguments, expected) in cases {
        let result = invoke(&context, arguments).await;
        let text = assert_tool_error(&result);
        assert!(text.contains(expected), "expected {expected:?} in {text:?}");
    }

    server.verify().await;
}

#[tokio::test]
async fn api_key_never_appears_in_any_tool_result() {
    let dir = TempDir::new().expect("temp dir");
    let server = MockServer::start().await;
    send_mock().expect(1).mount(&server).await;
    let context = context(dir.path(), &server);

    let results = [
        invoke(
            &context,
            json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE}),
        )
        .await,
        invoke(
            &context,
            json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE, "confirm": true}),
        )
        .await,
        invoke(
            &context,
            json!({"stream": STREAM, "topic": TOPIC, "message": MESSAGE, "confirm": true}),
        )
        .await,
        invoke(
            &context,
            json!({"stream": "", "topic": TOPIC, "message": MESSAGE}),
        )
        .await,
    ];
    assert_eq!(results[0]["structuredContent"]["status"], "dry_run");
    assert_eq!(results[1]["structuredContent"]["status"], "sent");
    assert_eq!(results[2]["isError"], true);
    assert_eq!(results[3]["isError"], true);

    for result in &results {
        assert!(
            !result.to_string().contains(API_KEY),
            "API key leaked into a tool result: {result}"
        );
        let text = result["content"][0]["text"].as_str().expect("text content");
        assert!(!text.contains(API_KEY), "API key leaked into text: {text}");
    }

    server.verify().await;
}
