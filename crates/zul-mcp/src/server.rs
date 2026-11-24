//! Request dispatch, concurrency, and the active-request registry.
//!
//! The stdin loop parses one frame at a time and hands every request to a local
//! task, so a slow tool call cannot block later frames. A single writer task in
//! the binary owns stdout; handlers only enqueue complete JSON lines.

use crate::protocol::{self, Envelope, RequestId};
use crate::tools::{self, ToolFailure, ToolState};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

const LEGACY_INITIALIZE_MESSAGE: &str = "legacy initialize is not supported: this server requires \
MCP 2026-07-28; set the client's protocol to \"2026-07-28\" and retry";

/// Dispatcher for inbound frames and owner of in-flight request tasks.
pub struct Server {
    tx: mpsc::Sender<String>,
    tools: Rc<ToolState>,
    active: Rc<RefCell<HashSet<String>>>,
    tasks: JoinSet<()>,
}

impl Server {
    /// Create a dispatcher that writes response frames through `tx`.
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self {
            tx,
            tools: Rc::new(ToolState::default()),
            active: Rc::new(RefCell::new(HashSet::new())),
            tasks: JoinSet::new(),
        }
    }

    /// Parse and dispatch one stdin line.
    pub async fn handle_line(&mut self, line: &str) {
        let value = match serde_json::from_str::<Value>(line) {
            Ok(value) => value,
            Err(err) => {
                self.reject(format!("parse error: {err}")).await;
                return;
            }
        };
        match protocol::parse_envelope(value) {
            Envelope::Request { id, method, params } => self.submit(id, method, params).await,
            Envelope::Notification => {}
            Envelope::Invalid { id, message } => {
                self.send(protocol::error(
                    id.as_ref(),
                    protocol::INVALID_REQUEST,
                    message,
                    None,
                ))
                .await;
            }
        }
    }

    /// Report a line that could not be parsed because it exceeded the size cap.
    pub async fn reject(&self, message: impl Into<String>) {
        self.send(protocol::error(None, protocol::PARSE_ERROR, message, None))
            .await;
    }

    /// Let queued requests finish within `grace`, then cancel the rest.
    pub async fn drain(&mut self, grace: Duration) {
        let _ = tokio::time::timeout(grace, async {
            while let Some(finished) = self.tasks.join_next().await {
                if let Err(err) = finished {
                    eprintln!("zul-mcp: request task failed: {err}");
                }
            }
        })
        .await;
        self.tasks.abort_all();
    }

    async fn submit(&mut self, id: RequestId, method: String, params: Option<Value>) {
        let key = id.registry_key();
        if !self.active.borrow_mut().insert(key) {
            self.send(protocol::error(
                Some(&id),
                protocol::INVALID_REQUEST,
                "duplicate active request id",
                None,
            ))
            .await;
            return;
        }

        let tx = self.tx.clone();
        let active = Rc::clone(&self.active);
        let tools = Rc::clone(&self.tools);
        self.tasks.spawn_local(async move {
            let outcome = handle_request(&tools, &method, params.as_ref()).await;
            let response = match outcome {
                Ok(result) => protocol::success(&id, result),
                Err(err) => protocol::error(Some(&id), err.code, err.message, err.data),
            };
            let _ = tx.send(protocol::to_line(&response)).await;
            active.borrow_mut().remove(&id.registry_key());
        });
        self.reap();
    }

    fn reap(&mut self) {
        while let Some(finished) = self.tasks.try_join_next() {
            if let Err(err) = finished {
                eprintln!("zul-mcp: request task failed: {err}");
            }
        }
    }

    async fn send(&self, response: Value) {
        let _ = self.tx.send(protocol::to_line(&response)).await;
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

async fn handle_request(
    tools: &ToolState,
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

    match protocol::check_request_version(params) {
        protocol::VersionCheck::Supported => {}
        protocol::VersionCheck::Missing => {
            return Err(RpcError {
                code: protocol::UNSUPPORTED_PROTOCOL_VERSION,
                message: format!(
                    "missing protocol version metadata; set params._meta[\"io.modelcontextprotocol/protocolVersion\"] to \"{}\"",
                    protocol::PROTOCOL_VERSION
                ),
                data: Some(protocol::unsupported_version_data(None)),
            });
        }
        protocol::VersionCheck::Unsupported(requested) => {
            return Err(RpcError {
                code: protocol::UNSUPPORTED_PROTOCOL_VERSION,
                message: format!(
                    "unsupported protocol version; this server supports only MCP {}",
                    protocol::PROTOCOL_VERSION
                ),
                data: Some(protocol::unsupported_version_data(Some(&requested))),
            });
        }
    }

    match method {
        "tools/list" => Ok(json!({
            "resultType": "complete",
            "ttlMs": 0,
            "cacheScope": "private",
            "tools": tools::catalog(),
        })),
        "tools/call" => call_tool(tools, params).await,
        other => Err(rpc_error(
            protocol::METHOD_NOT_FOUND,
            format!("unknown method: {other}"),
        )),
    }
}

async fn call_tool(tools: &ToolState, params: Option<&Value>) -> Result<Value, RpcError> {
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

    fn modern(params: Value) -> Value {
        let mut params = params;
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": protocol::PROTOCOL_VERSION,
        });
        params
    }

    #[tokio::test]
    async fn discovery_ignores_missing_metadata() {
        let tools = ToolState::default();
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
        let tools = ToolState::default();
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
        let tools = ToolState::default();
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
    async fn tools_list_is_complete_and_private() {
        let tools = ToolState::default();
        let result = handle_request(&tools, "tools/list", Some(&modern(json!({}))))
            .await
            .expect("tools/list result");
        assert_eq!(result["resultType"], "complete");
        assert!(result["ttlMs"].is_number());
        assert_eq!(result["cacheScope"], "private");
        let catalog = result["tools"].as_array().expect("tools array");
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0]["name"], tools::WHOAMI);
    }

    #[tokio::test]
    async fn unknown_method_and_tool_are_protocol_errors() {
        let tools = ToolState::default();
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
        let tools = ToolState::default();
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
}
