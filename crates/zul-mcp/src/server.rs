//! Request dispatch, in-flight registry, and per-request limits.
//!
//! The binary (`main.rs`) owns stdin framing and the stdout writer; this module
//! receives complete JSON-RPC lines, runs each ordinary request as a local task
//! under a timeout and a concurrency cap, and enqueues complete response lines.
//! Streaming methods are exempt from both limits through [`is_long_lived`].

use crate::events::{self, PollError};
use crate::events_stream::{self, EventsConfig, StreamEnd, StreamHub};
use crate::protocol::{self, Envelope, RequestId};
use crate::tools::{self, ToolContext, ToolFailure};
use libzul::zulip::client::ZulipClient;
use serde_json::{Map, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};
const LEGACY_INITIALIZE_MESSAGE: &str = "legacy initialize is not supported: this server requires \
MCP 2026-07-28; set the client's protocol to \"2026-07-28\" and retry";

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_MAX_INFLIGHT: usize = 64;

/// Limits applied to request tasks and event streams.
#[derive(Debug, Clone, Copy)]
pub struct ServerConfig {
    /// Maximum time one request may run before it is cancelled with `-32603`.
    pub request_timeout: Duration,
    /// Maximum number of concurrent ordinary request tasks.
    pub max_inflight: usize,
    /// Timing and capacity limits for long-lived event streams.
    pub events: EventsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            events: EventsConfig::default(),
        }
    }
}

/// One request task tracked for duplicate detection and cancellation.
struct ActiveRequest {
    generation: u64,
    abort: AbortHandle,
    long_lived: bool,
}

/// Everything a request task needs besides the request future itself.
struct RequestTask {
    tx: mpsc::Sender<String>,
    active: Rc<RefCell<HashMap<String, ActiveRequest>>>,
    key: String,
    generation: u64,
    timeout: Option<Duration>,
    id: RequestId,
    inflight_guard: InflightGuard,
    registry_guard: ActiveGuard,
}

/// Everything a stream task needs besides its request parameters.
struct StreamTask {
    tx: mpsc::Sender<String>,
    id: RequestId,
    tools: Rc<ToolContext>,
    hub: Rc<StreamHub>,
    events: EventsConfig,
    registry_guard: ActiveGuard,
}

/// Dispatcher for inbound frames and owner of in-flight request tasks.
pub struct Server {
    tx: mpsc::Sender<String>,
    tools: Rc<ToolContext>,
    hub: Rc<StreamHub>,
    active: Rc<RefCell<HashMap<String, ActiveRequest>>>,
    inflight: Rc<Cell<usize>>,
    tasks: JoinSet<()>,
    config: ServerConfig,
    next_generation: u64,
    dropped_response: Cell<bool>,
}

impl Server {
    /// Create a dispatcher with the default request limits.
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self::with_config(tx, ServerConfig::default())
    }

    /// Create a dispatcher with explicit request limits.
    pub fn with_config(tx: mpsc::Sender<String>, config: ServerConfig) -> Self {
        Self::with_context(tx, config, Rc::new(ToolContext::new()))
    }

    /// Create a dispatcher around an explicit tool context.
    ///
    /// Tests use this to inject settings aimed at a mock realm; production uses
    /// [`Server::new`] or [`Server::with_config`].
    pub fn with_context(
        tx: mpsc::Sender<String>,
        config: ServerConfig,
        tools: Rc<ToolContext>,
    ) -> Self {
        let hub = Rc::new(StreamHub::new(config.events));
        Self {
            tx,
            tools,
            hub,
            active: Rc::new(RefCell::new(HashMap::new())),
            inflight: Rc::new(Cell::new(0)),
            tasks: JoinSet::new(),
            config,
            next_generation: 0,
            dropped_response: Cell::new(false),
        }
    }

    /// Parse and dispatch one stdin line.
    pub async fn handle_line(&mut self, line: &str) {
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(err) => {
                self.reject(format!("parse error: {err}"));
                return;
            }
        };
        match protocol::parse_envelope(value) {
            Envelope::Request { id, method, params } => self.submit(id, method, params),
            Envelope::Notification { method, params } => {
                self.handle_notification(&method, params.as_ref())
            }
            Envelope::Invalid { id, message } => {
                self.send(protocol::error(
                    id.as_ref(),
                    protocol::INVALID_REQUEST,
                    message,
                    None,
                ));
            }
        }
    }

    /// Report a line that could not be parsed because it exceeded the size cap.
    pub fn reject(&self, message: impl Into<String>) {
        self.send(protocol::error(None, protocol::PARSE_ERROR, message, None));
    }

    /// Cancel streams and the shared poller, let queued requests finish within
    /// `grace`, then cancel the rest.
    pub async fn drain(&mut self, grace: Duration) {
        self.hub.shutdown();
        let long_lived: Vec<String> = self
            .active
            .borrow()
            .iter()
            .filter(|(_, request)| request.long_lived)
            .map(|(key, _)| key.clone())
            .collect();
        for key in long_lived {
            if let Some(request) = self.active.borrow_mut().remove(&key) {
                request.abort.abort();
            }
        }
        let _ = tokio::time::timeout(grace, async {
            while let Some(finished) = self.tasks.join_next().await {
                report_task(finished);
            }
        })
        .await;
        self.tasks.abort_all();
        while let Some(finished) = self.tasks.join_next().await {
            report_task(finished);
        }
    }

    fn submit(&mut self, id: RequestId, method: String, params: Option<Value>) {
        let key = id.registry_key();

        let previous = self.active.borrow_mut().remove(&key);
        if let Some(previous) = previous {
            previous.abort.abort();
            self.send(protocol::error(
                Some(&id),
                protocol::INVALID_REQUEST,
                "duplicate active request id",
                None,
            ));
            self.reap();
            return;
        }

        let long_lived = is_long_lived(&method);
        let inflight = self.inflight.get();
        if !long_lived && inflight >= self.config.max_inflight {
            self.send(protocol::error(
                Some(&id),
                protocol::INTERNAL_ERROR,
                format!(
                    "server busy: {inflight} in-flight requests (limit {}); retry later",
                    self.config.max_inflight
                ),
                None,
            ));
            return;
        }

        self.next_generation = self.next_generation.wrapping_add(1);
        let generation = self.next_generation;
        let counted = !long_lived;
        if counted {
            self.inflight.set(inflight + 1);
        }
        let tools = Rc::clone(&self.tools);
        let active = Rc::clone(&self.active);
        let task_key = key.clone();
        // The guards live in the task future, so aborting before the first
        // poll still releases the counter and registry entry.
        let inflight_guard = InflightGuard {
            inflight: Rc::clone(&self.inflight),
            counted,
        };
        let registry_guard = ActiveGuard {
            active: Rc::clone(&self.active),
            key: key.clone(),
            generation,
        };

        let handle = if long_lived {
            let task = StreamTask {
                tx: self.tx.clone(),
                id,
                tools,
                hub: Rc::clone(&self.hub),
                events: self.config.events,
                registry_guard,
            };
            self.tasks.spawn_local(run_stream(task, params))
        } else {
            self.tasks.spawn_local(run_request(
                RequestTask {
                    tx: self.tx.clone(),
                    active: Rc::clone(&self.active),
                    key,
                    generation,
                    timeout: counted.then_some(self.config.request_timeout),
                    id,
                    inflight_guard,
                    registry_guard,
                },
                async move { handle_request(&tools, &method, params.as_ref()).await },
            ))
        };
        active.borrow_mut().insert(
            task_key,
            ActiveRequest {
                generation,
                abort: handle,
                long_lived,
            },
        );
        self.reap();
    }

    /// Stop one subscription or request named by `notifications/cancelled`.
    ///
    /// Cancellation removes the registry entry first, so an aborted stream task
    /// can never emit a final result. Notifications never receive a response.
    fn handle_notification(&mut self, method: &str, params: Option<&Value>) {
        if method != "notifications/cancelled" {
            return;
        }
        let Some(request_id) = params
            .and_then(|params| params.get("requestId"))
            .and_then(RequestId::from_value)
        else {
            return;
        };
        let removed = self.active.borrow_mut().remove(&request_id.registry_key());
        if let Some(request) = removed {
            request.abort.abort();
            self.reap();
        }
    }

    fn reap(&mut self) {
        while let Some(finished) = self.tasks.try_join_next() {
            report_task(finished);
        }
    }

    /// Enqueue a response generated by the read loop without ever blocking it.
    ///
    /// A client that stops reading stdout must not be able to wedge the stdin
    /// loop before it observes EOF: when the bounded writer queue is full the
    /// response is dropped (once, with a diagnostic) rather than awaited.
    fn send(&self, response: Value) {
        match self.tx.try_send(protocol::to_line(&response)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !self.dropped_response.replace(true) {
                    eprintln!(
                        "zul-mcp: stdout queue full; dropping responses until the client reads"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// A streaming method stays open for the connection's lifetime, so it is not
/// counted against the ordinary request cap and has no request timeout. The
/// stream hub separately bounds how many may run at once.
fn is_long_lived(method: &str) -> bool {
    matches!(method, "events/stream")
}

/// Remove the registry entry only while `generation` still owns it.
fn claim(active: &Rc<RefCell<HashMap<String, ActiveRequest>>>, key: &str, generation: u64) -> bool {
    let mut active = active.borrow_mut();
    match active.get(key) {
        Some(request) if request.generation == generation => {
            active.remove(key);
            true
        }
        _ => false,
    }
}

/// Drop-time decrement for the ordinary in-flight request counter.
struct InflightGuard {
    inflight: Rc<Cell<usize>>,
    counted: bool,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if self.counted {
            self.inflight.set(self.inflight.get().saturating_sub(1));
        }
    }
}

/// Drop-time registry cleanup for aborted or cancelled request tasks.
struct ActiveGuard {
    active: Rc<RefCell<HashMap<String, ActiveRequest>>>,
    key: String,
    generation: u64,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let mut active = self.active.borrow_mut();
        if active
            .get(&self.key)
            .is_some_and(|request| request.generation == self.generation)
        {
            active.remove(&self.key);
        }
    }
}

async fn run_request<F>(task: RequestTask, request: F)
where
    F: Future<Output = Result<Value, RpcError>>,
{
    let RequestTask {
        tx,
        active,
        key,
        generation,
        timeout,
        id,
        inflight_guard,
        registry_guard,
    } = task;
    let _inflight_guard = inflight_guard;
    let _guard = registry_guard;
    let response = match timeout {
        Some(limit) => match tokio::time::timeout(limit, request).await {
            Ok(Ok(result)) => protocol::success(&id, result),
            Ok(Err(err)) => protocol::error(Some(&id), err.code, err.message, err.data),
            Err(_) => protocol::error(
                Some(&id),
                protocol::INTERNAL_ERROR,
                "request timed out",
                None,
            ),
        },
        None => match request.await {
            Ok(result) => protocol::success(&id, result),
            Err(err) => protocol::error(Some(&id), err.code, err.message, err.data),
        },
    };
    // Losing the claim means a duplicate id replaced this task; emit nothing.
    //
    // Keep the registry entry until the response is actually enqueued: with a
    // full writer queue, removing the entry first would let a duplicate id for
    // this task be treated as a fresh request and produce a second response.
    // Reserving first and then claiming leaves no await point between the
    // ownership check and the enqueue, so a duplicate that arrives while the
    // queue is full is still rejected and an aborted generation emits nothing.
    let Ok(permit) = tx.reserve().await else {
        return;
    };
    if !claim(&active, &key, generation) {
        return;
    }
    permit.send(protocol::to_line(&response));
}

fn report_task(finished: Result<(), tokio::task::JoinError>) {
    match finished {
        Ok(()) => {}
        Err(err) if err.is_cancelled() => {}
        Err(err) => eprintln!("zul-mcp: request task failed: {err}"),
    }
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

fn rpc_error(code: i64, message: impl Into<String>) -> RpcError {
    RpcError {
        code,
        message: message.into(),
        data: None,
    }
}

fn rpc_error_with_data(code: i64, message: impl Into<String>, data: Value) -> RpcError {
    RpcError {
        code,
        message: message.into(),
        data: Some(data),
    }
}

async fn handle_request(
    tools: &ToolContext,
    method: &str,
    params: Option<&Value>,
) -> Result<Value, RpcError> {
    match method {
        "server/discover" => return Ok(protocol::discover_result()),
        "initialize" => {
            return Err(rpc_error(
                protocol::METHOD_NOT_FOUND,
                LEGACY_INITIALIZE_MESSAGE,
            ));
        }
        _ => {}
    }

    // Deterministic stalling hook for the server's timeout/cap/abort tests.
    #[cfg(test)]
    if method == "test/stall" {
        return std::future::pending().await;
    }

    // Unknown methods are reported before the metadata gate.
    if !is_known_method(method) {
        return Err(rpc_error(
            protocol::METHOD_NOT_FOUND,
            format!("unknown method: {method}"),
        ));
    }

    check_version(params)?;

    match method {
        "tools/list" => {
            let mut body = Map::new();
            body.insert("tools".to_string(), tools::catalog());
            Ok(protocol::complete_cached(body))
        }
        "tools/call" => call_tool(tools, params).await,
        "events/list" => Ok(events::list_result()),
        "events/poll" => poll_events(tools, params).await,
        other => Err(rpc_error(
            protocol::METHOD_NOT_FOUND,
            format!("unknown method: {other}"),
        )),
    }
}

/// Reject requests that do not declare the one supported MCP revision.
fn check_version(params: Option<&Value>) -> Result<(), RpcError> {
    match protocol::check_request_version(params) {
        protocol::VersionCheck::Supported => Ok(()),
        protocol::VersionCheck::Missing => Err(rpc_error_with_data(
            protocol::UNSUPPORTED_PROTOCOL_VERSION,
            format!(
                "missing protocol version metadata; set params._meta[\"io.modelcontextprotocol/protocolVersion\"] to \"{}\"",
                protocol::PROTOCOL_VERSION
            ),
            protocol::unsupported_version_data(None),
        )),
        protocol::VersionCheck::Unsupported(requested) => Err(rpc_error_with_data(
            protocol::UNSUPPORTED_PROTOCOL_VERSION,
            format!(
                "unsupported protocol version; this server supports only MCP {}",
                protocol::PROTOCOL_VERSION
            ),
            protocol::unsupported_version_data(Some(&requested)),
        )),
    }
}

/// Methods that require per-request protocol metadata. Discovery is exempt and
/// `initialize` is handled before this check. `events/stream` is dispatched by
/// the long-lived request path and performs the same gate there.
fn is_known_method(method: &str) -> bool {
    matches!(
        method,
        "tools/list" | "tools/call" | "events/list" | "events/poll"
    )
}

/// Serve one `events/stream` request: validate, subscribe, then write frames
/// until the subscription ends. Only a server-initiated end produces a final
/// JSON-RPC result.
async fn run_stream(task: StreamTask, params: Option<Value>) {
    let StreamTask {
        tx,
        id,
        tools,
        hub,
        events,
        registry_guard,
    } = task;
    // Kept until the task ends so a duplicate id or cancellation can abort it
    // and release the registry entry.
    let _registry_guard = registry_guard;

    let subscription = match stream_setup(&tools, &hub, &events, &id, params.as_ref()).await {
        Ok(subscription) => subscription,
        Err(err) => {
            let response = protocol::error(Some(&id), err.code, err.message, err.data);
            let _ = tx.send(protocol::to_line(&response)).await;
            return;
        }
    };

    if let StreamEnd::Terminated = events_stream::serve(subscription, tx.clone()).await {
        let result = protocol::success(&id, events_stream::stream_result(&id.to_value()));
        let _ = tx.send(protocol::to_line(&result)).await;
    }
}

async fn stream_setup(
    tools: &ToolContext,
    hub: &Rc<StreamHub>,
    events: &EventsConfig,
    id: &RequestId,
    params: Option<&Value>,
) -> Result<events_stream::Subscription, RpcError> {
    check_version(params)?;
    let params = params.ok_or_else(|| {
        rpc_error(
            protocol::INVALID_PARAMS,
            "events/stream requires a params object",
        )
    })?;
    let params = events::parse_stream_params(params).map_err(poll_error)?;

    let settings = tools.settings().map_err(|problem| {
        rpc_error(
            protocol::INTERNAL_ERROR,
            format!("events/stream failed: {}", tools::one_line(&problem)),
        )
    })?;
    let realm = events::configured_realm(&settings);
    let start_cursor = match params.cursor.as_deref() {
        Some(cursor) => {
            let (cursor_realm, message_id) = events::parse_cursor(cursor).map_err(poll_error)?;
            if cursor_realm != realm {
                return Err(rpc_error(
                    protocol::INVALID_PARAMS,
                    "cursor belongs to another realm",
                ));
            }
            message_id
        }
        None => {
            let (base_url, email, api_key) = settings.require_auth().map_err(stream_auth_error)?;
            let client = ZulipClient::new(base_url, email, api_key);
            client
                .latest_message_id()
                .await
                .map_err(|err| {
                    rpc_error(
                        protocol::INTERNAL_ERROR,
                        format!(
                            "events/stream failed: {}",
                            tools::one_line(&err.to_string())
                        ),
                    )
                })?
                .unwrap_or(0)
        }
    };
    if params.cursor.is_some() {
        settings.require_auth().map_err(stream_auth_error)?;
    }

    let heartbeat = events.granted_heartbeat(params.heartbeat_ms);
    hub.subscribe(id.clone(), start_cursor, params, heartbeat, settings)
        .map_err(|err| rpc_error(protocol::INTERNAL_ERROR, err.message()))
}

fn stream_auth_error(problem: impl std::fmt::Display) -> RpcError {
    rpc_error(
        protocol::INTERNAL_ERROR,
        format!(
            "events/stream failed: {}",
            tools::one_line(&problem.to_string())
        ),
    )
}

async fn poll_events(tools: &ToolContext, params: Option<&Value>) -> Result<Value, RpcError> {
    let params = params.ok_or_else(|| {
        rpc_error(
            protocol::INVALID_PARAMS,
            "events/poll requires a params object",
        )
    })?;
    let params = events::parse_poll_params(params).map_err(poll_error)?;
    let settings = tools.settings().map_err(|problem| {
        rpc_error(
            protocol::INTERNAL_ERROR,
            format!("events/poll failed: {}", tools::one_line(&problem)),
        )
    })?;
    events::poll(&settings, params).await.map_err(poll_error)
}

fn poll_error(error: PollError) -> RpcError {
    let code = match &error {
        PollError::InvalidParams(_) => protocol::INVALID_PARAMS,
        PollError::Internal(_) => protocol::INTERNAL_ERROR,
    };
    rpc_error(code, error.message())
}

async fn call_tool(tools: &ToolContext, params: Option<&Value>) -> Result<Value, RpcError> {
    let params = params.ok_or_else(|| {
        rpc_error(
            protocol::INVALID_PARAMS,
            "tools/call requires a params object",
        )
    })?;
    let name = params.get("name").and_then(Value::as_str).ok_or_else(|| {
        rpc_error(
            protocol::INVALID_PARAMS,
            "tools/call requires a string name",
        )
    })?;
    match tools::call(tools, name, params.get("arguments")).await {
        Ok(result) => Ok(result),
        Err(ToolFailure::UnknownTool(name)) => Err(rpc_error(
            protocol::INVALID_PARAMS,
            format!("unknown tool: {name}"),
        )),
        Err(ToolFailure::InvalidArguments(message)) => {
            Ok(tools::invalid_arguments_result(&message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Instant;
    use tokio::task::LocalSet;

    fn modern(params: Value) -> Value {
        let mut params = params;
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": protocol::PROTOCOL_VERSION,
        });
        params
    }

    fn stalled(id: Value) -> String {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "test/stall",
            "params": modern(json!({})),
        })
        .to_string()
    }

    fn discover(id: Value) -> String {
        json!({"jsonrpc": "2.0", "id": id, "method": "server/discover"}).to_string()
    }

    async fn next_response(rx: &mut mpsc::Receiver<String>) -> Value {
        let line = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("response within a second")
            .expect("response line");
        serde_json::from_str(&line).expect("response is JSON")
    }

    #[tokio::test]
    async fn discovery_ignores_missing_metadata() {
        let tools = ToolContext::new();
        let result = handle_request(&tools, "server/discover", None)
            .await
            .expect("discovery result");
        assert_eq!(result["resultType"], "complete");
        assert_eq!(
            result["supportedVersions"],
            json!([protocol::PROTOCOL_VERSION])
        );
    }

    #[tokio::test]
    async fn legacy_initialize_is_rejected_with_guidance() {
        let tools = ToolContext::new();
        let err = handle_request(&tools, "initialize", None)
            .await
            .expect_err("initialize must fail");
        assert_eq!(err.code, protocol::METHOD_NOT_FOUND);
        assert!(err.message.contains("2026-07-28"));
        assert!(err.message.contains("protocol"));
        assert!(err.message.contains("requires"));
    }

    #[tokio::test]
    async fn version_metadata_is_required_and_exact() {
        let tools = ToolContext::new();
        let missing = handle_request(&tools, "tools/list", Some(&json!({})))
            .await
            .expect_err("missing metadata must fail");
        assert_eq!(missing.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);
        assert_eq!(
            missing.data.as_ref().expect("data")["requested"],
            Value::Null
        );

        let unsupported = handle_request(
            &tools,
            "tools/list",
            Some(&json!({"_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"}})),
        )
        .await
        .expect_err("unsupported metadata must fail");
        assert_eq!(unsupported.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);
        let data = unsupported.data.expect("data");
        assert_eq!(data["supported"], json!([protocol::PROTOCOL_VERSION]));
        assert_eq!(data["requested"], "2025-11-25");
    }

    #[tokio::test]
    async fn gate_applies_to_tools_call_but_unknown_methods_win_first() {
        let tools = ToolContext::new();
        let call = handle_request(
            &tools,
            "tools/call",
            Some(&json!({"name": "whoami", "arguments": {}})),
        )
        .await
        .expect_err("tools/call without metadata must fail");
        assert_eq!(call.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);

        let unknown = handle_request(&tools, "prompts/list", None)
            .await
            .expect_err("unknown method must fail");
        assert_eq!(unknown.code, protocol::METHOD_NOT_FOUND);

        let unsupported = handle_request(
            &tools,
            "tools/call",
            Some(&json!({
                "name": "whoami",
                "arguments": {},
                "_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"},
            })),
        )
        .await
        .expect_err("unsupported metadata must fail");
        assert_eq!(unsupported.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);
    }

    #[tokio::test]
    async fn events_list_requires_metadata_and_returns_the_descriptor() {
        let tools = ToolContext::new();
        let missing = handle_request(&tools, "events/list", Some(&json!({})))
            .await
            .expect_err("missing metadata must fail");
        assert_eq!(missing.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);

        let result = handle_request(&tools, "events/list", Some(&modern(json!({}))))
            .await
            .expect("events/list result");
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["events"].as_array().map(Vec::len), Some(1));
        assert_eq!(result["events"][0]["name"], events::EVENT_NAME);
        assert_eq!(result["events"][0]["delivery"], json!(["poll", "push"]));
    }

    #[tokio::test]
    async fn events_poll_validates_params_before_loading_configuration() {
        let tools = ToolContext::new();
        let missing = handle_request(&tools, "events/poll", Some(&json!({})))
            .await
            .expect_err("missing metadata must fail");
        assert_eq!(missing.code, protocol::UNSUPPORTED_PROTOCOL_VERSION);

        let unknown = handle_request(
            &tools,
            "events/poll",
            Some(&modern(json!({"name": "other.event"}))),
        )
        .await
        .expect_err("unknown event name must fail");
        assert_eq!(unknown.code, protocol::INVALID_PARAMS);
        assert!(unknown.message.contains("unknown event name"));

        let bad_argument = handle_request(
            &tools,
            "events/poll",
            Some(&modern(json!({
                "name": events::EVENT_NAME,
                "arguments": {"topic": "ops"},
            }))),
        )
        .await
        .expect_err("topic without stream must fail");
        assert_eq!(bad_argument.code, protocol::INVALID_PARAMS);

        let unknown_argument = handle_request(
            &tools,
            "events/poll",
            Some(&modern(json!({
                "name": events::EVENT_NAME,
                "arguments": {"bogus": true},
            }))),
        )
        .await
        .expect_err("unknown argument must fail");
        assert_eq!(unknown_argument.code, protocol::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn events_stream_rejects_invalid_params_without_opening_a_stream() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::new(tx);
                server
                    .handle_line(
                        &json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "events/stream",
                            "params": modern(json!({"name": "other.event"})),
                        })
                        .to_string(),
                    )
                    .await;

                let response = next_response(&mut rx).await;
                assert_eq!(response["id"], 1);
                assert_eq!(response["error"]["code"], protocol::INVALID_PARAMS);
                assert!(server.active.borrow().is_empty());
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), rx.recv())
                        .await
                        .is_err(),
                    "a rejected stream must not emit notifications"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn tools_list_is_complete_and_private() {
        let tools = ToolContext::new();
        let result = handle_request(&tools, "tools/list", Some(&modern(json!({}))))
            .await
            .expect("tools/list result");
        assert_eq!(result["resultType"], "complete");
        assert!(result["ttlMs"].is_number());
        assert_eq!(result["cacheScope"], "private");
        let catalog = result["tools"].as_array().expect("tools array");
        assert_eq!(catalog.len(), 8);
        assert_eq!(catalog[0]["name"], tools::WHOAMI);
        assert_eq!(catalog[7]["name"], tools::SEND_MESSAGE);
    }

    #[tokio::test]
    async fn unknown_method_and_tool_are_protocol_errors() {
        let tools = ToolContext::new();
        let method = handle_request(&tools, "prompts/list", Some(&modern(json!({}))))
            .await
            .expect_err("unknown method must fail");
        assert_eq!(method.code, protocol::METHOD_NOT_FOUND);

        let tool = handle_request(
            &tools,
            "tools/call",
            Some(&modern(json!({"name": "missing", "arguments": {}}))),
        )
        .await
        .expect_err("unknown tool must fail");
        assert_eq!(tool.code, protocol::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn malformed_arguments_are_a_tool_error_result() {
        let tools = ToolContext::new();
        let result = handle_request(
            &tools,
            "tools/call",
            Some(&modern(
                json!({"name": "whoami", "arguments": {"unexpected": true}}),
            )),
        )
        .await
        .expect("tool result");
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["isError"], true);
    }

    #[tokio::test]
    async fn request_timeout_answers_once_and_clears_the_registry() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_millis(50),
                        max_inflight: 8,
                        events: EventsConfig::default(),
                    },
                );
                let started = Instant::now();
                server.handle_line(&stalled(json!(1))).await;

                let response = next_response(&mut rx).await;
                assert_eq!(response["id"], 1);
                assert_eq!(response["error"]["code"], protocol::INTERNAL_ERROR);
                assert!(response["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("timed out")));
                assert!(server.active.borrow().is_empty());
                assert_eq!(server.inflight.get(), 0);
                assert!(started.elapsed() < Duration::from_secs(1));
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), rx.recv())
                        .await
                        .is_err(),
                    "a timed-out request must not emit a second response"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn duplicate_active_id_aborts_the_first_task_and_answers_once() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_secs(30),
                        max_inflight: 8,
                        events: EventsConfig::default(),
                    },
                );
                server.handle_line(&stalled(json!(1))).await;
                server.handle_line(&stalled(json!(1))).await;

                let response = next_response(&mut rx).await;
                assert_eq!(response["id"], 1);
                assert_eq!(response["error"]["code"], protocol::INVALID_REQUEST);
                assert_eq!(response["error"]["message"], "duplicate active request id");
                assert!(server.active.borrow().is_empty());
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), rx.recv())
                        .await
                        .is_err(),
                    "the aborted first request must not answer"
                );
                assert_eq!(server.inflight.get(), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn duplicate_id_is_rejected_while_the_first_response_waits_for_the_writer_queue() {
        LocalSet::new()
            .run_until(async {
                // Three slots, all held by the test, so the two request
                // responses park waiting for capacity and exactly one slot is
                // still free when the duplicate's immediate reply is sent.
                let (tx, mut rx) = mpsc::channel(3);
                let writer_tx = tx.clone();
                let permits = [
                    writer_tx.reserve().await.expect("reserve slot one"),
                    writer_tx.reserve().await.expect("reserve slot two"),
                    writer_tx.reserve().await.expect("reserve slot three"),
                ];
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_secs(30),
                        max_inflight: 8,
                        events: EventsConfig::default(),
                    },
                );

                server.handle_line(&discover(json!(1))).await;
                server.handle_line(&discover(json!(2))).await;
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(
                    server.inflight.get(),
                    2,
                    "both responses must still be waiting for capacity"
                );
                assert!(
                    server.active.borrow().contains_key("n:1")
                        && server.active.borrow().contains_key("n:2"),
                    "the registry must keep responses that are waiting for the writer"
                );

                // Release the writer slots, then deliver the duplicate before
                // the parked tasks can poll for capacity. The duplicate is
                // answered with one -32600, and id 2's parked success must
                // never be enqueued.
                drop(permits);
                server.handle_line(&discover(json!(2))).await;

                let duplicate = next_response(&mut rx).await;
                assert_eq!(duplicate["id"], 2);
                assert_eq!(duplicate["error"]["code"], protocol::INVALID_REQUEST);
                assert_eq!(duplicate["error"]["message"], "duplicate active request id");

                let untouched = next_response(&mut rx).await;
                assert_eq!(untouched["id"], 1, "the untouched request still completes");

                assert!(
                    tokio::time::timeout(Duration::from_millis(100), rx.recv())
                        .await
                        .is_err(),
                    "a duplicate id during a blocked enqueue must yield exactly one response"
                );
                assert!(server.active.borrow().is_empty());
                assert_eq!(server.inflight.get(), 0);
            })
            .await;
    }

    #[tokio::test]
    async fn in_flight_cap_rejects_extra_requests() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_secs(30),
                        max_inflight: 2,
                        events: EventsConfig::default(),
                    },
                );
                server.handle_line(&stalled(json!(1))).await;
                server.handle_line(&stalled(json!(2))).await;
                server.handle_line(&stalled(json!(3))).await;

                let response = next_response(&mut rx).await;
                assert_eq!(response["id"], 3);
                assert_eq!(response["error"]["code"], protocol::INTERNAL_ERROR);
                assert!(response["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("in-flight")));
                assert_eq!(server.active.borrow().len(), 2);
                assert_eq!(server.inflight.get(), 2);

                server.drain(Duration::from_millis(20)).await;
                assert!(server.active.borrow().is_empty());
                assert_eq!(server.inflight.get(), 0);
                assert!(server.tasks.is_empty());
            })
            .await;
    }

    #[tokio::test]
    async fn long_lived_methods_bypass_the_request_cap() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_secs(30),
                        max_inflight: 1,
                        events: EventsConfig::default(),
                    },
                );
                server.handle_line(&stalled(json!(1))).await;
                server
                    .handle_line(
                        &json!({
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "events/stream",
                            "params": modern(json!({"name": "other.event"})),
                        })
                        .to_string(),
                    )
                    .await;

                let response = next_response(&mut rx).await;
                assert_eq!(response["id"], 2);
                assert_eq!(
                    response["error"]["code"],
                    protocol::INVALID_PARAMS,
                    "events/stream is exempt from the in-flight cap"
                );
                server.drain(Duration::from_millis(20)).await;
            })
            .await;
    }

    #[tokio::test]
    async fn drain_bounds_a_never_completing_task() {
        LocalSet::new()
            .run_until(async {
                let (tx, mut rx) = mpsc::channel(8);
                let mut server = Server::with_config(
                    tx,
                    ServerConfig {
                        request_timeout: Duration::from_secs(30),
                        max_inflight: 8,
                        events: EventsConfig::default(),
                    },
                );
                server.handle_line(&stalled(json!(1))).await;
                let started = Instant::now();
                server.drain(Duration::from_millis(50)).await;
                assert!(started.elapsed() < Duration::from_secs(1));
                assert!(server.active.borrow().is_empty());
                assert_eq!(server.inflight.get(), 0);
                assert!(server.tasks.is_empty());
                assert!(rx.try_recv().is_err());
            })
            .await;
    }
}
