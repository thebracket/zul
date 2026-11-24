//! Tool catalog and handlers, independent of the JSON-RPC transport.
//!
//! The catalog is static and cheap: `tools/list` never touches configuration
//! or the network. [`ToolContext`] owns the single lazily opened [`libzul::App`]
//! shared by every handler, so a process loads settings and opens the cache at
//! most once. Tests inject an app pointed at a temporary cache instead.

use crate::protocol;
use libzul::zulip::models::ZulipMessage;
use libzul::{App, AuthStatus, Settings};
use serde_json::{json, Map, Value};
use std::cell::RefCell;
use std::rc::Rc;

/// Tool name for the local identity probe.
pub const WHOAMI: &str = "whoami";
/// Tool name for fetching streams and recent messages into the cache.
pub const SYNC: &str = "sync";
/// Tool name for listing cached streams.
pub const LIST_STREAMS: &str = "list_streams";
/// Tool name for listing cached topics in one stream.
pub const LIST_TOPICS: &str = "list_topics";
/// Tool name for reading the most recent messages in one topic.
pub const READ_TOPIC: &str = "read_topic";
/// Tool name for reading recent messages across the cache.
pub const RECENT: &str = "recent";
/// Tool name for searching cached messages.
pub const SEARCH: &str = "search";

const DEFAULT_HOURS: i64 = 24;
const MAX_HOURS: i64 = 168;
const DEFAULT_READ_LIMIT: i64 = 100;
const MAX_READ_LIMIT: i64 = 500;
const DEFAULT_SEARCH_LIMIT: i64 = 50;
const MAX_SEARCH_LIMIT: i64 = 100;

/// Longest message text returned by a read tool, in Unicode scalar values.
const MAX_MESSAGE_TEXT_CHARS: usize = 4000;

/// Return the descriptors for every advertised tool, in catalog order.
pub fn catalog() -> Value {
    json!([
        {
            "name": WHOAMI,
            "description": "Report the local Zulip realm URL, account email, and whether Zulip access is ready. This reads local configuration and does not populate the cache.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }
        },
        {
            "name": SYNC,
            "description": "Fetch subscribed streams and recent messages from Zulip into the local cache. Call sync before list_streams, list_topics, read_topic, recent, or search when the cache may be empty.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "hours": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 168,
                        "default": DEFAULT_HOURS
                    }
                },
                "required": [],
                "additionalProperties": false
            }
        },
        {
            "name": LIST_STREAMS,
            "description": "List streams from the local cache, sorted by name. Call sync first if the cache may be empty.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }
        },
        {
            "name": LIST_TOPICS,
            "description": "List cached topics in one stream, newest activity first. An unknown stream returns an empty list; call sync first if the cache may be empty.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "stream": {"type": "string"}
                },
                "required": ["stream"],
                "additionalProperties": false
            }
        },
        {
            "name": READ_TOPIC,
            "description": "Read the most recent cached messages in a stream topic, oldest first. Call sync first if the cache may be empty; message text is capped at 4000 characters.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "stream": {"type": "string"},
                    "topic": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": DEFAULT_READ_LIMIT
                    }
                },
                "required": ["stream", "topic"],
                "additionalProperties": false
            }
        },
        {
            "name": RECENT,
            "description": "Read cached messages from the last N hours, newest first. Call sync first if the cache may be empty.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "hours": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 168,
                        "default": DEFAULT_HOURS
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": DEFAULT_READ_LIMIT
                    }
                },
                "required": [],
                "additionalProperties": false
            }
        },
        {
            "name": SEARCH,
            "description": "Search messages already in the local cache and return them in relevance order. Searches cover only cached messages; call sync first to widen coverage.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": DEFAULT_SEARCH_LIMIT
                    }
                },
                "required": ["query"],
                "additionalProperties": false
            }
        }
    ])
}

/// Process-wide Zulip access shared by tool handlers.
pub struct ToolContext {
    app: RefCell<Option<Result<Rc<App>, String>>>,
}

impl ToolContext {
    /// Build a context that loads settings and opens the cache on first use.
    pub fn new() -> Self {
        Self {
            app: RefCell::new(None),
        }
    }

    /// Build a context around an already-open app.
    ///
    /// Tests use this to point handlers at a temporary SQLite cache; production
    /// uses [`ToolContext::new`] so configuration is loaded lazily once.
    pub fn with_app(app: Rc<App>) -> Self {
        Self {
            app: RefCell::new(Some(Ok(app))),
        }
    }

    fn app(&self) -> Result<Rc<App>, String> {
        let mut app = self.app.borrow_mut();
        if let Some(existing) = app.as_ref() {
            return existing.clone();
        }
        let loaded = Settings::load()
            .map_err(|err| format!("failed to load Zulip settings: {err}"))
            .and_then(|settings| {
                App::open(settings, false)
                    .map(Rc::new)
                    .map_err(|err| format!("failed to open the local Zulip cache: {err}"))
            });
        *app = Some(loaded.clone());
        loaded
    }
}

impl Default for ToolContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a `tools/call` did not produce a normal result.
pub enum ToolFailure {
    /// The requested tool is not in the catalog.
    UnknownTool(String),
    /// The arguments do not match the tool's input schema.
    InvalidArguments(String),
}

/// Invoke a tool by name.
pub async fn call(
    context: &ToolContext,
    name: &str,
    arguments: Option<&Value>,
) -> Result<Value, ToolFailure> {
    match name {
        WHOAMI => {
            let args = arguments_object(arguments, WHOAMI)?;
            ensure_consumed(args, WHOAMI)?;
            Ok(whoami(context).await)
        }
        SYNC => {
            let mut args = arguments_object(arguments, SYNC)?;
            let hours = integer_argument(&mut args, "hours", DEFAULT_HOURS, 1, MAX_HOURS)?;
            ensure_consumed(args, SYNC)?;
            Ok(sync(context, hours).await)
        }
        LIST_STREAMS => {
            let args = arguments_object(arguments, LIST_STREAMS)?;
            ensure_consumed(args, LIST_STREAMS)?;
            Ok(list_streams(context))
        }
        LIST_TOPICS => {
            let mut args = arguments_object(arguments, LIST_TOPICS)?;
            let stream = string_argument(&mut args, "stream")?;
            ensure_consumed(args, LIST_TOPICS)?;
            Ok(list_topics(context, &stream))
        }
        READ_TOPIC => {
            let mut args = arguments_object(arguments, READ_TOPIC)?;
            let stream = string_argument(&mut args, "stream")?;
            let topic = string_argument(&mut args, "topic")?;
            let limit =
                integer_argument(&mut args, "limit", DEFAULT_READ_LIMIT, 1, MAX_READ_LIMIT)?;
            ensure_consumed(args, READ_TOPIC)?;
            Ok(read_topic(context, &stream, &topic, limit))
        }
        RECENT => {
            let mut args = arguments_object(arguments, RECENT)?;
            let hours = integer_argument(&mut args, "hours", DEFAULT_HOURS, 1, MAX_HOURS)?;
            let limit =
                integer_argument(&mut args, "limit", DEFAULT_READ_LIMIT, 1, MAX_READ_LIMIT)?;
            ensure_consumed(args, RECENT)?;
            Ok(recent(context, hours, limit))
        }
        SEARCH => {
            let mut args = arguments_object(arguments, SEARCH)?;
            let query = string_argument(&mut args, "query")?;
            let limit = integer_argument(
                &mut args,
                "limit",
                DEFAULT_SEARCH_LIMIT,
                1,
                MAX_SEARCH_LIMIT,
            )?;
            ensure_consumed(args, SEARCH)?;
            if query.trim().is_empty() {
                return Err(ToolFailure::InvalidArguments(
                    "search query must not be blank".to_string(),
                ));
            }
            Ok(search(context, query.trim(), limit))
        }
        other => Err(ToolFailure::UnknownTool(other.to_string())),
    }
}

/// Build an `isError` tool result for unusable arguments.
pub fn invalid_arguments_result(message: &str) -> Value {
    error_result(message)
}

async fn whoami(context: &ToolContext) -> Value {
    match context.app() {
        Ok(app) => {
            let status = app.auth_status().await;
            let settings = app.settings();
            let (base_url, email) = match &status {
                AuthStatus::Ready { base_url, email } => {
                    (Some(base_url.clone()), Some(email.clone()))
                }
                AuthStatus::Missing { .. } => (
                    present(settings.zulip.base_url.clone()),
                    present(settings.zulip.email.clone()),
                ),
            };
            let problems = match status {
                AuthStatus::Ready { .. } => Vec::new(),
                AuthStatus::Missing { problems } => problems,
            };
            let ready = problems.is_empty();
            complete_result(json!({
                "base_url": base_url,
                "email": email,
                "ready": ready,
                "problems": problems,
            }))
        }
        Err(problem) => complete_result(json!({
            "base_url": null,
            "email": null,
            "ready": false,
            "problems": [problem],
        })),
    }
}

async fn sync(context: &ToolContext, hours: i64) -> Value {
    let app = match context.app() {
        Ok(app) => app,
        Err(problem) => return tool_error(SYNC, problem),
    };
    match app.sync(hours).await {
        Ok(result) => complete_result(json!({
            "streams": result.streams,
            "messages": result.messages,
            "hours": result.hours,
            "offline": result.offline,
        })),
        Err(err) => tool_error(SYNC, err),
    }
}

fn list_streams(context: &ToolContext) -> Value {
    with_app(context, LIST_STREAMS, |app| match app.streams() {
        Ok(streams) => {
            let streams: Vec<Value> = streams
                .iter()
                .map(|stream| {
                    json!({
                        "id": stream.id,
                        "name": stream.name,
                        "description": stream.description,
                    })
                })
                .collect();
            complete_result(json!({"streams": streams}))
        }
        Err(err) => tool_error(LIST_STREAMS, err),
    })
}

fn list_topics(context: &ToolContext, stream: &str) -> Value {
    with_app(context, LIST_TOPICS, |app| match app.topics(stream) {
        Ok(topics) => {
            let topics: Vec<Value> = topics
                .iter()
                .map(|topic| {
                    json!({
                        "name": topic.name,
                        "message_count": topic.message_count,
                        "last_message_at": topic
                            .last_message_at
                            .as_ref()
                            .map(|timestamp| timestamp.to_rfc3339()),
                    })
                })
                .collect();
            complete_result(json!({"stream": stream, "topics": topics}))
        }
        Err(err) => tool_error(LIST_TOPICS, err),
    })
}

fn read_topic(context: &ToolContext, stream: &str, topic: &str, limit: i64) -> Value {
    with_app(context, READ_TOPIC, |app| match app.topic(stream, topic) {
        Ok(mut messages) => {
            let limit = limit as usize;
            if messages.len() > limit {
                let excess = messages.len() - limit;
                messages.drain(..excess);
            }
            let count = messages.len();
            let messages: Vec<Value> = messages.iter().map(message_view).collect();
            complete_result(json!({
                "stream": stream,
                "topic": topic,
                "count": count,
                "messages": messages,
            }))
        }
        Err(err) => tool_error(READ_TOPIC, err),
    })
}

fn recent(context: &ToolContext, hours: i64, limit: i64) -> Value {
    with_app(context, RECENT, |app| match app.recent_messages(hours) {
        Ok(mut messages) => {
            messages.truncate(limit as usize);
            let count = messages.len();
            let messages: Vec<Value> = messages.iter().map(message_view).collect();
            complete_result(json!({
                "hours": hours,
                "count": count,
                "messages": messages,
            }))
        }
        Err(err) => tool_error(RECENT, err),
    })
}

fn search(context: &ToolContext, query: &str, limit: i64) -> Value {
    with_app(context, SEARCH, |app| match app.search(query) {
        Ok(mut messages) => {
            messages.truncate(limit as usize);
            let count = messages.len();
            let messages: Vec<Value> = messages.iter().map(message_view).collect();
            complete_result(json!({
                "query": query,
                "count": count,
                "messages": messages,
            }))
        }
        Err(err) => tool_error(SEARCH, err),
    })
}

fn message_view(message: &ZulipMessage) -> Value {
    let (text, text_truncated) = cap_text(&message.plain_text);
    json!({
        "id": message.id,
        "stream": message.stream_name,
        "topic": message.topic,
        "sender_name": message.sender_name,
        "sender_email": message.sender_email,
        "timestamp": message.timestamp.to_rfc3339(),
        "text": text,
        "text_truncated": text_truncated,
        "permalink": message.permalink,
    })
}

fn cap_text(text: &str) -> (String, bool) {
    match text.char_indices().nth(MAX_MESSAGE_TEXT_CHARS) {
        Some((end, _)) => (text[..end].to_string(), true),
        None => (text.to_string(), false),
    }
}

fn present(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

fn complete_result(structured: Value) -> Value {
    let text = structured.to_string();
    let mut body = Map::new();
    body.insert(
        "content".to_string(),
        json!([{"type": "text", "text": text}]),
    );
    body.insert("structuredContent".to_string(), structured);
    protocol::complete_result(body)
}

fn error_result(message: impl Into<String>) -> Value {
    let mut body = Map::new();
    body.insert(
        "content".to_string(),
        json!([{"type": "text", "text": message.into()}]),
    );
    body.insert("isError".to_string(), json!(true));
    protocol::complete_result(body)
}

/// Run one cache operation against the shared app, or report why it is unavailable.
fn with_app(context: &ToolContext, tool: &str, operation: impl FnOnce(&App) -> Value) -> Value {
    match context.app() {
        Ok(app) => operation(&app),
        Err(problem) => tool_error(tool, problem),
    }
}

fn tool_error(tool: &str, problem: impl std::fmt::Display) -> Value {
    error_result(format!("{tool} failed: {}", one_line(&problem.to_string())))
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn arguments_object(
    arguments: Option<&Value>,
    tool: &str,
) -> Result<Map<String, Value>, ToolFailure> {
    match arguments {
        None | Some(Value::Null) => Ok(Map::new()),
        Some(Value::Object(map)) => Ok(map.clone()),
        Some(_) => Err(ToolFailure::InvalidArguments(format!(
            "{tool} arguments must be a JSON object"
        ))),
    }
}

fn string_argument(args: &mut Map<String, Value>, name: &str) -> Result<String, ToolFailure> {
    match args.remove(name) {
        Some(Value::String(text)) => Ok(text),
        Some(_) => Err(ToolFailure::InvalidArguments(format!(
            "{name} must be a string"
        ))),
        None => Err(ToolFailure::InvalidArguments(format!(
            "missing required argument: {name}"
        ))),
    }
}

fn integer_argument(
    args: &mut Map<String, Value>,
    name: &str,
    default: i64,
    min: i64,
    max: i64,
) -> Result<i64, ToolFailure> {
    match args.remove(name) {
        None => Ok(default),
        Some(Value::Number(number)) => number
            .as_i64()
            .filter(|value| (min..=max).contains(value))
            .ok_or_else(|| {
                ToolFailure::InvalidArguments(format!(
                    "{name} must be an integer between {min} and {max}"
                ))
            }),
        Some(_) => Err(ToolFailure::InvalidArguments(format!(
            "{name} must be an integer between {min} and {max}"
        ))),
    }
}

fn ensure_consumed(args: Map<String, Value>, tool: &str) -> Result<(), ToolFailure> {
    if args.is_empty() {
        return Ok(());
    }
    let mut names: Vec<&str> = args.keys().map(String::as_str).collect();
    names.sort_unstable();
    Err(ToolFailure::InvalidArguments(format!(
        "{tool} does not accept these arguments: {}",
        names.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_lists_strict_tools_in_order() {
        let tools = catalog();
        let tools = tools.as_array().expect("catalog is an array");
        let names: Vec<&str> = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name"))
            .collect();
        assert_eq!(
            names,
            vec![
                WHOAMI,
                SYNC,
                LIST_STREAMS,
                LIST_TOPICS,
                READ_TOPIC,
                RECENT,
                SEARCH
            ]
        );
        for tool in tools {
            assert!(
                tool["description"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "tool description missing: {tool}"
            );
            let schema = &tool["inputSchema"];
            assert_eq!(schema["type"], "object", "schema type: {tool}");
            assert!(
                schema["properties"].is_object(),
                "schema properties: {tool}"
            );
            assert!(
                schema["required"].is_array(),
                "schema required array: {tool}"
            );
            assert_eq!(
                schema["additionalProperties"], false,
                "schema must reject unknown arguments: {tool}"
            );
        }
    }

    #[test]
    fn caps_text_by_character_count() {
        let short = "x".repeat(MAX_MESSAGE_TEXT_CHARS - 1);
        assert_eq!(cap_text(&short), (short, false));

        let exact = "x".repeat(MAX_MESSAGE_TEXT_CHARS);
        assert_eq!(cap_text(&exact), (exact, false));

        let long = "x".repeat(MAX_MESSAGE_TEXT_CHARS + 1);
        let (text, truncated) = cap_text(&long);
        assert!(truncated);
        assert_eq!(text.chars().count(), MAX_MESSAGE_TEXT_CHARS);

        let unicode = "é".repeat(MAX_MESSAGE_TEXT_CHARS + 5);
        let (text, truncated) = cap_text(&unicode);
        assert!(truncated);
        assert_eq!(text.chars().count(), MAX_MESSAGE_TEXT_CHARS);
    }

    #[test]
    fn error_results_have_no_structured_payload() {
        let result = error_result("sync failed: missing credentials");
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["isError"], true);
        assert!(result.get("structuredContent").is_none());
        assert!(
            result.get("ttlMs").is_none() && result.get("cacheScope").is_none(),
            "tool errors must not carry cache hints: {result}"
        );
        assert_eq!(result["content"][0]["type"], "text");
    }

    #[test]
    fn complete_result_text_matches_structured_content() {
        let result = complete_result(json!({
            "base_url": "https://chat.example.com",
            "email": "user@example.com",
            "ready": true,
            "problems": [],
        }));
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["structuredContent"]["ready"], true);
        let text = result["content"][0]["text"].as_str().expect("text content");
        let parsed: Value = serde_json::from_str(text).expect("text is JSON");
        assert_eq!(parsed, result["structuredContent"]);
        assert!(!text.contains("api_key"));
    }

    #[test]
    fn arguments_reject_unknown_fields() {
        let result = ensure_consumed(
            Map::from_iter([("stream".to_string(), json!("core"))]),
            LIST_STREAMS,
        );
        assert!(matches!(result, Err(ToolFailure::InvalidArguments(_))));
    }
}
