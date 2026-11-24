//! MCP Events discovery and poll delivery for Zulip messages.
//!
//! The extension advertises one `zulip.message` event type. `events/poll` is
//! stateless: its opaque cursor encodes the realm and the last Zulip message ID
//! already considered, so every request can be answered from its parameters,
//! the configured credentials, and the Zulip API.
//!
//! Cursors are realm-scoped. A cursor minted for another realm, or a malformed
//! one, is rejected instead of being used as an unrestricted query.

use crate::protocol;
use crate::tools::{cap_text_at, one_line};
use chrono::Utc;
use libzul::zulip::client::ZulipClient;
use libzul::zulip::models::ZulipMessage;
use libzul::Settings;
use serde_json::{json, Map, Value};

/// Event name advertised by this server.
pub const EVENT_NAME: &str = "zulip.message";

/// Suggested delay between polls, in milliseconds.
pub const NEXT_POLL_MS: i64 = 15_000;

/// Stream label used for direct-message events, which have no Zulip stream.
const DIRECT_MESSAGE_STREAM: &str = "direct message";

/// Default cap on events returned by one poll.
const DEFAULT_MAX_EVENTS: i64 = 50;

/// Largest cap a caller may request on one poll.
const MAX_MAX_EVENTS: i64 = 200;

/// Longest message text carried in an event payload, in Unicode scalar values.
const MAX_EVENT_TEXT_CHARS: usize = 2000;

/// `ZulipClient::messages_after` stops after 25 pages of 1000 messages. A batch
/// that reaches this size may have more history behind it, so poll and stream
/// delivery report `truncated` instead of silently claiming completeness.
pub(crate) const MESSAGES_AFTER_BATCH_LIMIT: usize = 25_000;

/// Prefix of the opaque, versioned cursor format.
const CURSOR_PREFIX: &str = "v1:";

/// Return the complete `events/list` result.
pub fn list_result() -> Value {
    let mut body = Map::new();
    body.insert("events".to_string(), json!([descriptor()]));
    protocol::complete_result(body)
}

fn descriptor() -> Value {
    json!({
        "name": EVENT_NAME,
        "description": "A message was posted to a Zulip stream topic or direct message \
            conversation visible to the configured account.",
        "delivery": ["poll", "push"],
        "inputSchema": {
            "type": "object",
            "properties": {
                "stream": {
                    "type": "string",
                    "description": "Only emit messages from this stream display name."
                },
                "topic": {
                    "type": "string",
                    "description": "Only emit messages in this exact topic; requires stream."
                },
                "sender_email": {
                    "type": "string",
                    "format": "email",
                    "description": "Only emit messages from this sender, compared case-insensitively."
                },
                "exclude_self": {
                    "type": "boolean",
                    "default": true,
                    "description": "Skip messages sent by the configured account."
                }
            },
            "required": [],
            "additionalProperties": false
        },
        "payloadSchema": {
            "type": "object",
            "properties": {
                "message_id": {"type": "integer"},
                "realm": {"type": "string", "format": "uri"},
                "message_type": {"type": "string", "enum": ["stream", "private"]},
                "stream": {"type": ["string", "null"]},
                "topic": {"type": ["string", "null"]},
                "sender_name": {"type": "string"},
                "sender_email": {"type": ["string", "null"], "format": "email"},
                "timestamp": {"type": "string", "format": "date-time"},
                "text": {"type": "string"},
                "text_truncated": {"type": "boolean"},
                "permalink": {"type": ["string", "null"], "format": "uri"}
            },
            "required": [
                "message_id",
                "realm",
                "message_type",
                "stream",
                "topic",
                "sender_name",
                "sender_email",
                "timestamp",
                "text",
                "text_truncated",
                "permalink"
            ],
            "additionalProperties": false
        }
    })
}

/// Validated `events/poll` parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollParams {
    /// Filters applied before any event is emitted.
    pub arguments: PollFilter,
    /// Opaque resume cursor, or `None` to start from now.
    pub cursor: Option<String>,
    /// Maximum age of replayed events, in milliseconds.
    pub max_age_ms: Option<i64>,
    /// Maximum number of events this response may carry.
    pub max_events: usize,
}

/// Validated filter arguments for the `zulip.message` event type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollFilter {
    /// Exact stream display name.
    pub stream: Option<String>,
    /// Exact topic, only valid together with `stream`.
    pub topic: Option<String>,
    /// Case-insensitive sender email.
    pub sender_email: Option<String>,
    /// Skip messages sent by the configured account; defaults to true.
    pub exclude_self: bool,
}

/// Validated `events/stream` parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamParams {
    /// Filters applied before any event is emitted.
    pub arguments: PollFilter,
    /// Opaque resume cursor, or `None` to start from the newest message.
    pub cursor: Option<String>,
    /// Maximum age of replayed events, in milliseconds.
    pub max_age_ms: Option<i64>,
    /// Client heartbeat suggestion in milliseconds, before clamping.
    pub heartbeat_ms: Option<i64>,
}

/// Parse the shared filter arguments used by poll and stream requests.
fn parse_filter(object: &Map<String, Value>, context: &str) -> Result<PollFilter, PollError> {
    let mut arguments = match object.get("arguments") {
        None => Map::new(),
        Some(Value::Object(arguments)) => arguments.clone(),
        Some(_) => {
            return Err(invalid_params(format!(
                "{context} arguments must be a JSON object"
            )))
        }
    };
    let stream = take_string(&mut arguments, "stream")?;
    let topic = take_string(&mut arguments, "topic")?;
    let sender_email = take_string(&mut arguments, "sender_email")?;
    let exclude_self = match arguments.remove("exclude_self") {
        None => true,
        Some(Value::Bool(value)) => value,
        Some(_) => return Err(invalid_params("exclude_self must be a boolean")),
    };
    reject_remaining(arguments, &format!("{context} arguments"))?;
    if topic.is_some() && stream.is_none() {
        return Err(invalid_params(format!("{context} topic requires stream")));
    }
    Ok(PollFilter {
        stream,
        topic,
        sender_email,
        exclude_self,
    })
}

fn parse_cursor_param(object: &Map<String, Value>) -> Result<Option<String>, PollError> {
    match object.get("cursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(cursor)) => Ok(Some(cursor.clone())),
        Some(_) => Err(invalid_params("cursor must be a string or null")),
    }
}

fn parse_max_age(object: &Map<String, Value>) -> Result<Option<i64>, PollError> {
    match object.get("maxAgeMs") {
        None => Ok(None),
        Some(Value::Number(number)) => Ok(Some(
            number
                .as_i64()
                .filter(|value| *value >= 0)
                .ok_or_else(|| invalid_params("maxAgeMs must be a non-negative integer"))?,
        )),
        Some(_) => Err(invalid_params("maxAgeMs must be a non-negative integer")),
    }
}

/// Why an `events/poll` request could not produce a result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollError {
    /// The request parameters are malformed or inconsistent with this realm.
    InvalidParams(String),
    /// Zulip configuration or an upstream request failed.
    Internal(String),
}

impl PollError {
    /// The human-readable reason, without any credential values.
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidParams(message) | Self::Internal(message) => message,
        }
    }
}

/// Validate the `events/poll` params object.
pub fn parse_poll_params(params: &Value) -> Result<PollParams, PollError> {
    let object = params
        .as_object()
        .ok_or_else(|| invalid_params("events/poll params must be a JSON object"))?;

    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "name" | "arguments" | "cursor" | "maxAgeMs" | "maxEvents" | "_meta"
        ) {
            return Err(invalid_params(format!(
                "events/poll does not accept parameter: {key}"
            )));
        }
    }

    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_params("events/poll requires a string name"))?;
    if name != EVENT_NAME {
        return Err(invalid_params(format!("unknown event name: {name}")));
    }

    let arguments = parse_filter(object, "events/poll")?;
    let cursor = parse_cursor_param(object)?;
    let max_age_ms = parse_max_age(object)?;

    let max_events = match object.get("maxEvents") {
        None => DEFAULT_MAX_EVENTS,
        Some(Value::Number(number)) => number
            .as_i64()
            .filter(|value| (1..=MAX_MAX_EVENTS).contains(value))
            .ok_or_else(|| {
                invalid_params(format!(
                    "maxEvents must be an integer between 1 and {MAX_MAX_EVENTS}"
                ))
            })?,
        Some(_) => {
            return Err(invalid_params(format!(
                "maxEvents must be an integer between 1 and {MAX_MAX_EVENTS}"
            )))
        }
    };

    Ok(PollParams {
        arguments,
        cursor,
        max_age_ms,
        max_events: max_events as usize,
    })
}

/// Validate the `events/stream` params object.
pub fn parse_stream_params(params: &Value) -> Result<StreamParams, PollError> {
    let object = params
        .as_object()
        .ok_or_else(|| invalid_params("events/stream params must be a JSON object"))?;

    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "name" | "arguments" | "cursor" | "maxAgeMs" | "heartbeatMs" | "_meta"
        ) {
            return Err(invalid_params(format!(
                "events/stream does not accept parameter: {key}"
            )));
        }
    }

    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_params("events/stream requires a string name"))?;
    if name != EVENT_NAME {
        return Err(invalid_params(format!("unknown event name: {name}")));
    }

    let heartbeat_ms = match object.get("heartbeatMs") {
        None => None,
        Some(Value::Number(number)) => Some(
            number
                .as_i64()
                .filter(|value| *value >= 0)
                .ok_or_else(|| invalid_params("heartbeatMs must be a non-negative integer"))?,
        ),
        Some(_) => return Err(invalid_params("heartbeatMs must be a non-negative integer")),
    };

    Ok(StreamParams {
        arguments: parse_filter(object, "events/stream")?,
        cursor: parse_cursor_param(object)?,
        max_age_ms: parse_max_age(object)?,
        heartbeat_ms,
    })
}

/// Execute one `events/poll` request against the configured Zulip realm.
pub async fn poll(settings: &Settings, params: PollParams) -> Result<Value, PollError> {
    let realm = configured_realm(settings);
    let resume = match params.cursor.as_deref() {
        Some(cursor) => {
            let (cursor_realm, message_id) = parse_cursor(cursor)?;
            if cursor_realm != realm {
                return Err(invalid_params("cursor belongs to another realm"));
            }
            Some(message_id)
        }
        None => None,
    };

    let (base_url, email, api_key) = settings.require_auth().map_err(upstream_failure)?;
    let client = ZulipClient::new(base_url, email, api_key);

    let Some(anchor) = resume else {
        let latest = client.latest_message_id().await.map_err(upstream_failure)?;
        return Ok(poll_result(
            Vec::new(),
            encode_cursor(&realm, latest.unwrap_or(0)),
            false,
            false,
        ));
    };

    let mut messages = client
        .messages_after(anchor)
        .await
        .map_err(upstream_failure)?;
    messages.sort_by_key(|message| message.id);

    let now_ms = Utc::now().timestamp_millis();
    let account_email = settings.zulip.email.as_deref();
    let mut candidates: Vec<&ZulipMessage> = Vec::new();
    let mut truncated = messages.len() >= MESSAGES_AFTER_BATCH_LIMIT;
    let mut last_evaluated = anchor;
    for message in &messages {
        if message.id <= anchor {
            continue;
        }
        last_evaluated = last_evaluated.max(message.id);
        if !matches_message(&params.arguments, message, account_email) {
            continue;
        }
        if let Some(max_age_ms) = params.max_age_ms {
            let age_ms = now_ms.saturating_sub(message.timestamp.timestamp_millis());
            if age_ms > max_age_ms {
                truncated = true;
                continue;
            }
        }
        candidates.push(message);
    }

    let has_more = candidates.len() > params.max_events;
    let selected = &candidates[..candidates.len().min(params.max_events)];
    let events: Vec<Value> = selected
        .iter()
        .map(|message| occurrence(message, &realm))
        .collect();
    let cursor_id = if has_more {
        selected.last().map(|message| message.id).unwrap_or(anchor)
    } else {
        last_evaluated
    };

    Ok(poll_result(
        events,
        encode_cursor(&realm, cursor_id),
        truncated,
        has_more,
    ))
}

fn poll_result(events: Vec<Value>, cursor: String, truncated: bool, has_more: bool) -> Value {
    let mut body = Map::new();
    body.insert("events".to_string(), Value::Array(events));
    body.insert("cursor".to_string(), Value::String(cursor));
    body.insert("truncated".to_string(), Value::Bool(truncated));
    body.insert("hasMore".to_string(), Value::Bool(has_more));
    body.insert("nextPollMs".to_string(), json!(NEXT_POLL_MS));
    protocol::complete_result(body)
}

pub(crate) fn configured_realm(settings: &Settings) -> String {
    settings
        .zulip
        .base_url
        .as_deref()
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string()
}

/// Encode the opaque resume position for a realm and message ID.
pub fn encode_cursor(realm: &str, message_id: i64) -> String {
    format!("{CURSOR_PREFIX}{realm}|{message_id}")
}

pub(crate) fn parse_cursor(cursor: &str) -> Result<(&str, i64), PollError> {
    let rest = cursor
        .strip_prefix(CURSOR_PREFIX)
        .ok_or_else(|| invalid_params("invalid cursor"))?;
    let (realm, id_text) = rest
        .rsplit_once('|')
        .ok_or_else(|| invalid_params("invalid cursor"))?;
    if realm.is_empty() || id_text.is_empty() || !id_text.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid_params("invalid cursor"));
    }
    let message_id = id_text
        .parse::<i64>()
        .map_err(|_| invalid_params("invalid cursor"))?;
    if message_id < 0 || message_id.to_string() != id_text {
        return Err(invalid_params("invalid cursor"));
    }
    Ok((realm, message_id))
}

fn event_id(realm: &str, message_id: i64) -> String {
    format!("{realm}#{message_id}")
}

pub(crate) fn occurrence(message: &ZulipMessage, realm: &str) -> Value {
    json!({
        "eventId": event_id(realm, message.id),
        "name": EVENT_NAME,
        "timestamp": message.timestamp.to_rfc3339(),
        "data": event_data(message, realm),
        "cursor": encode_cursor(realm, message.id),
    })
}

fn event_data(message: &ZulipMessage, realm: &str) -> Value {
    let message_type = message_type(message);
    let (stream, topic) = if message_type == "stream" {
        (
            Some(message.stream_name.clone()),
            Some(message.topic.clone()),
        )
    } else {
        (Some(DIRECT_MESSAGE_STREAM.to_string()), None)
    };
    let (text, text_truncated) = cap_text_at(&message.plain_text, MAX_EVENT_TEXT_CHARS);
    json!({
        "message_id": message.id,
        "realm": realm,
        "message_type": message_type,
        "stream": stream,
        "topic": topic,
        "sender_name": message.sender_name,
        "sender_email": message.sender_email,
        "timestamp": message.timestamp.to_rfc3339(),
        "text": text,
        "text_truncated": text_truncated,
        "permalink": message.permalink,
    })
}

fn message_type(message: &ZulipMessage) -> &'static str {
    match message.raw_json.get("type").and_then(Value::as_str) {
        Some("stream") => "stream",
        Some("private") => "private",
        _ if message.stream_id.is_some() => "stream",
        _ => "private",
    }
}

pub(crate) fn matches_message(
    filter: &PollFilter,
    message: &ZulipMessage,
    account_email: Option<&str>,
) -> bool {
    let message_type = message_type(message);
    if let Some(stream) = filter.stream.as_deref() {
        if message_type != "stream" || message.stream_name != stream {
            return false;
        }
    }
    if let Some(topic) = filter.topic.as_deref() {
        if message_type != "stream" || message.topic != topic {
            return false;
        }
    }
    if let Some(wanted) = filter.sender_email.as_deref() {
        let Some(actual) = message.sender_email.as_deref() else {
            return false;
        };
        if !actual.eq_ignore_ascii_case(wanted) {
            return false;
        }
    }
    if filter.exclude_self {
        if let (Some(account), Some(sender)) = (account_email, message.sender_email.as_deref()) {
            if sender.eq_ignore_ascii_case(account) {
                return false;
            }
        }
    }
    true
}

fn take_string(args: &mut Map<String, Value>, name: &str) -> Result<Option<String>, PollError> {
    match args.remove(name) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(invalid_params(format!("{name} must be a string"))),
    }
}

fn reject_remaining(args: Map<String, Value>, context: &str) -> Result<(), PollError> {
    if args.is_empty() {
        return Ok(());
    }
    let mut names: Vec<&str> = args.keys().map(String::as_str).collect();
    names.sort_unstable();
    Err(invalid_params(format!(
        "{context} does not accept: {}",
        names.join(", ")
    )))
}

fn invalid_params(message: impl Into<String>) -> PollError {
    PollError::InvalidParams(message.into())
}

fn internal(message: impl Into<String>) -> PollError {
    PollError::Internal(message.into())
}

fn upstream_failure(error: impl std::fmt::Display) -> PollError {
    internal(format!(
        "events/poll failed: {}",
        one_line(&error.to_string())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_and_rejects_tampering() {
        let cursor = encode_cursor("https://chat.example", 42);
        assert_eq!(cursor, "v1:https://chat.example|42");
        assert_eq!(
            parse_cursor(&cursor).expect("valid cursor"),
            ("https://chat.example", 42)
        );

        for invalid in [
            "",
            "42",
            "v1:",
            "v1:https://chat.example",
            "v1:https://chat.example|",
            "v1:|42",
            "v1:https://chat.example|abc",
            "v1:https://chat.example|-1",
            "v1:https://chat.example|+1",
            "v1:https://chat.example|01",
            "v2:https://chat.example|1",
        ] {
            assert!(
                matches!(parse_cursor(invalid), Err(PollError::InvalidParams(_))),
                "{invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn poll_params_apply_defaults_and_reject_unknown_arguments() {
        let params = parse_poll_params(&json!({
            "name": EVENT_NAME,
            "arguments": {"stream": "core team"},
        }))
        .expect("valid params");
        assert_eq!(params.max_events, DEFAULT_MAX_EVENTS as usize);
        assert_eq!(params.arguments.stream.as_deref(), Some("core team"));
        assert!(params.arguments.exclude_self);
        assert_eq!(params.max_age_ms, None);
        assert_eq!(params.cursor, None);

        let params = parse_poll_params(&json!({
            "name": EVENT_NAME,
            "arguments": {"stream": "core", "topic": "ops", "exclude_self": false},
            "cursor": "v1:https://chat.example|7",
            "maxAgeMs": 0,
            "maxEvents": 200,
        }))
        .expect("valid params");
        assert_eq!(params.max_events, 200);
        assert_eq!(params.max_age_ms, Some(0));
        assert!(!params.arguments.exclude_self);

        let cases = [
            json!({"_meta": {}}),
            json!({"name": "other.event"}),
            json!({"name": EVENT_NAME, "arguments": {"topic": "ops"}}),
            json!({"name": EVENT_NAME, "arguments": {"unknown": true}}),
            json!({"name": EVENT_NAME, "arguments": {"exclude_self": "yes"}}),
            json!({"name": EVENT_NAME, "arguments": {"stream": 1}}),
            json!({"name": EVENT_NAME, "arguments": null}),
            json!({"name": EVENT_NAME, "cursor": 1}),
            json!({"name": EVENT_NAME, "maxAgeMs": -1}),
            json!({"name": EVENT_NAME, "maxAgeMs": 1.5}),
            json!({"name": EVENT_NAME, "maxEvents": 0}),
            json!({"name": EVENT_NAME, "maxEvents": 201}),
            json!({"name": EVENT_NAME, "unknown": true}),
        ];
        for params in cases {
            assert!(
                matches!(parse_poll_params(&params), Err(PollError::InvalidParams(_))),
                "{params} must be rejected"
            );
        }
    }

    #[test]
    fn stream_params_share_filters_and_reject_poll_only_fields() {
        let params = parse_stream_params(&json!({
            "name": EVENT_NAME,
            "arguments": {"stream": "core", "topic": "ops", "exclude_self": false},
            "cursor": "v1:https://chat.example|7",
            "maxAgeMs": 100,
            "heartbeatMs": 300,
        }))
        .expect("valid stream params");
        assert_eq!(params.arguments.stream.as_deref(), Some("core"));
        assert_eq!(params.arguments.topic.as_deref(), Some("ops"));
        assert!(!params.arguments.exclude_self);
        assert_eq!(params.cursor.as_deref(), Some("v1:https://chat.example|7"));
        assert_eq!(params.max_age_ms, Some(100));
        assert_eq!(params.heartbeat_ms, Some(300));

        for invalid in [
            json!({}),
            json!({"name": "other.event"}),
            json!({"name": EVENT_NAME, "maxEvents": 10}),
            json!({"name": EVENT_NAME, "maxEvents": 10, "maxAgeMs": 0}),
            json!({"name": EVENT_NAME, "heartbeatMs": -1}),
            json!({"name": EVENT_NAME, "heartbeatMs": 1.5}),
            json!({"name": EVENT_NAME, "heartbeatMs": "fast"}),
            json!({"name": EVENT_NAME, "arguments": {"topic": "ops"}}),
            json!({"name": EVENT_NAME, "arguments": {"unknown": true}}),
            json!({"name": EVENT_NAME, "cursor": 4}),
            json!({"name": EVENT_NAME, "maxAgeMs": -1}),
        ] {
            assert!(
                matches!(
                    parse_stream_params(&invalid),
                    Err(PollError::InvalidParams(_))
                ),
                "{invalid} must be rejected"
            );
        }
    }

    #[test]
    fn descriptor_freezes_the_event_contract() {
        let result = list_result();
        assert_eq!(result["resultType"], "complete");
        let events = result["events"].as_array().expect("events array");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["name"], EVENT_NAME);
        assert!(event["description"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert_eq!(event["delivery"], json!(["poll", "push"]));

        let input = &event["inputSchema"];
        assert_eq!(input["type"], "object");
        assert_eq!(input["required"], json!([]));
        assert_eq!(input["additionalProperties"], false);
        assert_eq!(input["properties"]["stream"]["type"], "string");
        assert_eq!(input["properties"]["topic"]["type"], "string");
        assert_eq!(input["properties"]["sender_email"]["type"], "string");
        assert_eq!(input["properties"]["exclude_self"]["type"], "boolean");
        assert_eq!(input["properties"]["exclude_self"]["default"], true);

        let payload = &event["payloadSchema"];
        assert_eq!(payload["type"], "object");
        assert_eq!(payload["additionalProperties"], false);
        assert_eq!(
            payload["required"],
            json!([
                "message_id",
                "realm",
                "message_type",
                "stream",
                "topic",
                "sender_name",
                "sender_email",
                "timestamp",
                "text",
                "text_truncated",
                "permalink"
            ])
        );
        assert_eq!(payload["properties"]["message_id"]["type"], "integer");
        assert_eq!(payload["properties"]["timestamp"]["format"], "date-time");
    }
}
