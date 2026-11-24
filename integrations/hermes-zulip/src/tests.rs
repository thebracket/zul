use std::time::{Duration, Instant};

use libzul::config::ZulipConfig;
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::bridge::{
    config_status, merge_config, normalize_event, BridgeConfig, BridgeCore, EventOptions,
};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

fn start_server() -> MockServer {
    block_on(MockServer::start())
}

fn mount(server: &MockServer, mock: Mock) {
    block_on(mock.mount(server));
}

fn verify(server: &MockServer) {
    block_on(server.verify());
}

fn config(server: &MockServer) -> BridgeConfig {
    merge_config(
        None,
        &server.uri(),
        "ada@example.com",
        "test-key",
        EventOptions::default(),
    )
    .expect("test config")
}

fn register_mock(queue_id: &str) -> Mock {
    Mock::given(method("POST"))
        .and(path("/api/v1/register"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": "success",
            "msg": "",
            "queue_id": queue_id,
            "last_event_id": 0,
            "event_queue_longpoll_timeout_seconds": 30
        })))
}

fn empty_events_mock(queue_id: &str, delay: Duration) -> Mock {
    Mock::given(method("GET"))
        .and(path("/api/v1/events"))
        .and(query_param("queue_id", queue_id))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(delay)
                .set_body_json(json!({
                    "result": "success",
                    "msg": "",
                    "queue_id": queue_id,
                    "events": []
                })),
        )
}

fn message_event(message: serde_json::Value) -> serde_json::Value {
    json!({"id": 3, "type": "message", "message": message})
}

fn wait_for_queue(core: &BridgeCore, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if core.queue_id().as_deref() == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "queue {expected} was not registered"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn whoami_returns_email_and_full_name() {
    let server = start_server();
    mount(
        &server,
        Mock::given(method("GET"))
            .and(path("/api/v1/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "email": "ada@example.com",
                "full_name": "Ada Lovelace"
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    let account = core.whoami().expect("whoami");
    assert_eq!(account["email"], "ada@example.com");
    assert_eq!(account["full_name"], "Ada Lovelace");

    core.close();
    verify(&server);
}

#[test]
fn send_stream_returns_the_message_id() {
    let server = start_server();
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/messages"))
            .and(body_string_contains("type=stream"))
            .and(body_string_contains("to=core"))
            .and(body_string_contains("topic=ops"))
            .and(body_string_contains("content=hello"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "id": 321
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    assert_eq!(core.send_stream("core", "ops", "hello").unwrap(), 321);

    core.close();
    verify(&server);
}

#[test]
fn direct_sends_accept_ids_and_emails() {
    let server = start_server();
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/messages"))
            .and(body_string_contains("type=direct"))
            .and(body_string_contains("to=%5B7%2C8%5D"))
            .and(body_string_contains("content=ids"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "id": 41
            })))
            .expect(1),
    );
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/messages"))
            .and(body_string_contains("type=direct"))
            .and(body_string_contains(
                "to=%5B%22ada%40example.com%22%2C%22bob%40example.com%22%5D",
            ))
            .and(body_string_contains("content=emails"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "id": 42
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    assert_eq!(core.send_direct_ids(&[7, 8], "ids").unwrap(), 41);
    assert_eq!(
        core.send_direct_emails(
            &["ada@example.com".to_string(), "bob@example.com".to_string()],
            "emails"
        )
        .unwrap(),
        42
    );

    core.close();
    verify(&server);
}

#[test]
fn typing_methods_post_the_expected_forms() {
    let server = start_server();
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/typing"))
            .and(body_string_contains("type=stream"))
            .and(body_string_contains("op=start"))
            .and(body_string_contains("to=17"))
            .and(body_string_contains("topic=ops"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": ""
            })))
            .expect(1),
    );
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/typing"))
            .and(body_string_contains("type=direct"))
            .and(body_string_contains("op=stop"))
            .and(body_string_contains("to=%5B7%2C8%5D"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": ""
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    core.typing_stream(17, "ops", "start").unwrap();
    core.typing_direct(&[7, 8], "stop").unwrap();

    core.close();
    verify(&server);
}

#[test]
fn normalize_event_maps_stream_messages() {
    let event = message_event(json!({
        "id": 55,
        "type": "stream",
        "stream_id": 12,
        "display_recipient": "core",
        "subject": "ops",
        "sender_id": 2,
        "sender_full_name": "Ada Lovelace",
        "sender_email": "ada@example.com",
        "timestamp": 1_700_000_000,
        "content": "<p>hello world</p>",
        "flags": ["read", "mentioned"],
        "permalink": "https://chat.example.com/#narrow/stream/12/topic/ops/near/55"
    }));

    let normalized = normalize_event(&event).expect("stream event");
    assert_eq!(normalized["id"], 55);
    assert_eq!(normalized["kind"], "stream");
    assert_eq!(normalized["stream_id"], 12);
    assert_eq!(normalized["stream"], "core");
    assert_eq!(normalized["topic"], "ops");
    assert_eq!(normalized["sender_id"], 2);
    assert_eq!(normalized["sender_name"], "Ada Lovelace");
    assert_eq!(normalized["sender_email"], "ada@example.com");
    assert_eq!(normalized["content"], "<p>hello world</p>");
    assert_eq!(normalized["plain_text"], "hello world");
    assert_eq!(normalized["flags"], json!(["read", "mentioned"]));
    assert_eq!(normalized["timestamp"], 1_700_000_000);
    assert_eq!(
        normalized["permalink"],
        "https://chat.example.com/#narrow/stream/12/topic/ops/near/55"
    );
    assert_eq!(normalized["dm_participants"], json!([]));
}

#[test]
fn normalize_event_maps_direct_messages() {
    let event = message_event(json!({
        "id": 56,
        "type": "private",
        "display_recipient": [
            {"id": 1, "full_name": "Ada Lovelace", "email": "ada@example.com"},
            {"id": 2, "full_name": "Bob", "email": "bob@example.com"}
        ],
        "subject": "",
        "sender_id": 2,
        "sender_full_name": "Bob",
        "sender_email": "bob@example.com",
        "timestamp": 1_700_000_001,
        "content": "<p>hi there</p>",
        "flags": ["read"]
    }));

    let normalized = normalize_event(&event).expect("direct event");
    assert_eq!(normalized["kind"], "dm");
    assert_eq!(normalized["stream"], "Direct messages");
    assert_eq!(normalized["topic"], "Ada Lovelace, Bob");
    assert_eq!(normalized["plain_text"], "hi there");
    assert_eq!(
        normalized["dm_participants"],
        json!([
            {"id": 1, "name": "Ada Lovelace", "email": "ada@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"}
        ])
    );
}

#[test]
fn normalize_event_skips_non_messages_and_malformed_messages() {
    assert!(normalize_event(&json!({"id": 1, "type": "heartbeat"})).is_none());
    assert!(normalize_event(&json!({"id": 2, "type": "message"})).is_none());
    assert!(normalize_event(&message_event(json!({
        "id": 3,
        "type": "stream",
        "content": "no timestamp"
    })))
    .is_none());
}

#[test]
fn next_event_times_out_with_none() {
    let server = start_server();
    mount(&server, register_mock("q-timeout").expect(1));
    mount(
        &server,
        empty_events_mock("q-timeout", Duration::from_millis(10)),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    wait_for_queue(&core, "q-timeout");
    assert!(core
        .next_event(Duration::from_millis(200))
        .expect("timeout is not an error")
        .is_none());

    core.close();
    verify(&server);
}

#[test]
fn shutdown_deletes_the_queue_and_is_idempotent() {
    let server = start_server();
    mount(&server, register_mock("q-shutdown").expect(1));
    mount(
        &server,
        empty_events_mock("q-shutdown", Duration::from_millis(1500)),
    );
    mount(
        &server,
        Mock::given(method("DELETE"))
            .and(path("/api/v1/events"))
            .and(body_string_contains("queue_id=q-shutdown"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": ""
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    wait_for_queue(&core, "q-shutdown");
    core.close();
    core.close();

    verify(&server);
}

#[test]
fn queue_expiry_reregisters_and_keeps_delivering_events() {
    let server = start_server();
    mount(&server, register_mock("q-old").up_to_n_times(1).expect(1));
    mount(&server, register_mock("q-new").expect(1));
    mount(
        &server,
        Mock::given(method("GET"))
            .and(path("/api/v1/events"))
            .and(query_param("queue_id", "q-old"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "result": "error",
                "msg": "Bad event queue id",
                "code": "BAD_EVENT_QUEUE_ID"
            })))
            .up_to_n_times(1)
            .expect(1),
    );
    mount(
        &server,
        Mock::given(method("GET"))
            .and(path("/api/v1/events"))
            .and(query_param("queue_id", "q-new"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "queue_id": "q-new",
                "events": [
                    {"id": 2, "type": "message", "message": {"id": 9, "type": "stream"}},
                    message_event(json!({
                        "id": 55,
                        "type": "stream",
                        "stream_id": 12,
                        "display_recipient": "core",
                        "subject": "ops",
                        "sender_id": 2,
                        "sender_full_name": "Ada Lovelace",
                        "sender_email": "ada@example.com",
                        "timestamp": 1_700_000_000,
                        "content": "<p>after re-register</p>",
                        "flags": ["read"]
                    }))
                ]
            })))
            .up_to_n_times(1)
            .expect(1),
    );
    mount(
        &server,
        empty_events_mock("q-new", Duration::from_millis(10)),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    let event = core
        .next_event(Duration::from_secs(3))
        .expect("event after re-registration")
        .expect("event present");
    assert_eq!(event["id"], 55);
    assert_eq!(event["plain_text"], "after re-register");
    assert_eq!(core.queue_id().as_deref(), Some("q-new"));

    core.close();
    verify(&server);
}

#[test]
fn sends_proceed_while_an_event_long_poll_is_in_flight() {
    let server = start_server();
    mount(&server, register_mock("q-slow").expect(1));
    mount(&server, empty_events_mock("q-slow", Duration::from_secs(3)));
    mount(
        &server,
        Mock::given(method("POST"))
            .and(path("/api/v1/messages"))
            .and(body_string_contains("type=stream"))
            .and(body_string_contains("content=quick"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": "success",
                "msg": "",
                "id": 77
            })))
            .expect(1),
    );

    let core = BridgeCore::new(config(&server)).expect("bridge");
    wait_for_queue(&core, "q-slow");
    std::thread::sleep(Duration::from_millis(100));

    let started = Instant::now();
    let id = core.send_stream("core", "ops", "quick").expect("send");
    let elapsed = started.elapsed();
    assert_eq!(id, 77);
    assert!(
        elapsed < Duration::from_millis(1000),
        "send waited for the long-poll: {elapsed:?}"
    );

    core.close();
    verify(&server);
}

#[test]
fn explicit_config_overrides_loaded_values() {
    let loaded = ZulipConfig {
        base_url: Some("https://env.example.com".to_string()),
        email: Some("env@example.com".to_string()),
        api_key: Some("env-key".to_string()),
    };
    let config = merge_config(
        Some(&loaded),
        "https://explicit.example.com",
        "explicit@example.com",
        "explicit-key",
        EventOptions::default(),
    )
    .expect("merged config");

    assert_eq!(config.base_url, "https://explicit.example.com");
    assert_eq!(config.email, "explicit@example.com");
    assert_eq!(config.api_key, "explicit-key");
    assert_eq!(config.event_types, vec!["message".to_string()]);
    assert!(config.narrow.is_empty());
}

#[test]
fn empty_explicit_config_falls_back_to_loaded_values() {
    let loaded = ZulipConfig {
        base_url: Some("https://env.example.com".to_string()),
        email: Some("env@example.com".to_string()),
        api_key: Some("env-key".to_string()),
    };
    let config =
        merge_config(Some(&loaded), "", "", "", EventOptions::default()).expect("fallback config");

    assert_eq!(config.base_url, "https://env.example.com");
    assert_eq!(config.email, "env@example.com");
    assert_eq!(config.api_key, "env-key");
}

#[test]
fn missing_config_names_every_field_without_leaking_the_key() {
    let err = merge_config(None, "", "", "", EventOptions::default()).expect_err("missing");
    assert!(err.contains("base_url"), "message: {err}");
    assert!(err.contains("email"), "message: {err}");
    assert!(err.contains("api_key"), "message: {err}");

    let loaded = ZulipConfig {
        base_url: None,
        email: None,
        api_key: Some("super-secret-key".to_string()),
    };
    let err = merge_config(Some(&loaded), "", "", "", EventOptions::default())
        .expect_err("missing base URL and email");
    assert!(
        !err.contains("super-secret-key"),
        "the API key leaked: {err}"
    );
    assert!(
        !err.contains("api_key"),
        "present fields are not missing: {err}"
    );
}

#[test]
fn narrow_must_be_operator_operand_pairs() {
    let err = merge_config(
        None,
        "https://chat.example.com",
        "ada@example.com",
        "key",
        EventOptions {
            event_types: Some(Vec::new()),
            narrow: Some(vec![vec!["channel".to_string()]]),
            ..EventOptions::default()
        },
    )
    .expect_err("invalid narrow");
    assert!(err.contains("narrow"), "message: {err}");
}

#[test]
fn config_status_reports_presence_only() {
    let status = config_status();
    for field in ["base_url", "email", "api_key"] {
        assert!(
            status.get(field).is_some_and(serde_json::Value::is_boolean),
            "config_status.{field} must be a bool: {status}"
        );
    }
}
