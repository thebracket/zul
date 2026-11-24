//! JSON-RPC 2.0 wire shapes for MCP over newline-delimited stdio.
//!
//! This module owns the envelope: request IDs that preserve string versus
//! number identity, success and error responses, the modern `resultType`
//! contract, its shared cache hints, and the `params._meta` protocol-version
//! gate. Business logic lives in [`crate::tools`]; request dispatch and
//! concurrency live in [`crate::server`]; stdin framing, the bounded line
//! reader, and the stdout writer live in the `zul-mcp` binary (`main.rs`).

use serde_json::{json, Map, Number, Value};

/// The one MCP revision this server implements.
pub const PROTOCOL_VERSION: &str = "2026-07-28";

/// Request metadata key that carries the client's protocol revision.
pub const VERSION_META_KEY: &str = "io.modelcontextprotocol/protocolVersion";

/// JSON-RPC parse error.
pub const PARSE_ERROR: i64 = -32700;
/// JSON-RPC invalid request.
pub const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC unknown method.
pub const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC invalid method parameters.
pub const INVALID_PARAMS: i64 = -32602;
/// JSON-RPC internal error, used for request timeouts and overload.
pub const INTERNAL_ERROR: i64 = -32603;
/// MCP unsupported protocol version.
pub const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

/// Cache lifetime advertised on cacheable complete results.
pub const TTL_MS: u64 = 0;
/// Cache scope advertised on cacheable complete results.
pub const CACHE_SCOPE: &str = "private";

/// A JSON-RPC request identifier that keeps its original JSON type.
#[derive(Debug, Clone, PartialEq)]
pub enum RequestId {
    /// Numeric identifier.
    Number(Number),
    /// String identifier.
    String(String),
    /// Explicit JSON `null` identifier.
    Null,
}

impl RequestId {
    /// Parse an identifier value, rejecting arrays, objects, and booleans.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(number) => Some(Self::Number(number.clone())),
            Value::String(text) => Some(Self::String(text.clone())),
            Value::Null => Some(Self::Null),
            Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
        }
    }

    /// Render the identifier back into the exact JSON shape it arrived as.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Number(number) => Value::Number(number.clone()),
            Self::String(text) => Value::String(text.clone()),
            Self::Null => Value::Null,
        }
    }

    /// Derive a stable key that distinguishes numeric from string identifiers.
    pub fn registry_key(&self) -> String {
        match self {
            Self::Number(number) => format!("n:{number}"),
            Self::String(text) => format!("s:{text}"),
            Self::Null => "null".to_string(),
        }
    }
}

/// A parsed inbound frame.
pub enum Envelope {
    /// A request that must receive exactly one response.
    Request {
        /// Identifier to echo in the response.
        id: RequestId,
        /// JSON-RPC method name.
        method: String,
        /// Optional method parameters.
        params: Option<Value>,
    },
    /// A notification that must never receive a response.
    Notification,
    /// A frame that is not a valid JSON-RPC request.
    Invalid {
        /// Identifier to echo when it could be recovered.
        id: Option<RequestId>,
        /// Human-readable reason for the rejection.
        message: String,
    },
}

/// Parse a JSON value into a request, notification, or invalid-request result.
pub fn parse_envelope(value: Value) -> Envelope {
    let Value::Object(mut object) = value else {
        return Envelope::Invalid {
            id: None,
            message: "request must be a JSON object".to_string(),
        };
    };

    let id = match object.get("id") {
        None => None,
        Some(value) => match RequestId::from_value(value) {
            Some(id) => Some(id),
            None => {
                return Envelope::Invalid {
                    id: None,
                    message: "request id must be a string, number, or null".to_string(),
                };
            }
        },
    };

    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Envelope::Invalid {
            id,
            message: "jsonrpc must be \"2.0\"".to_string(),
        };
    }

    let method = match object.remove("method") {
        Some(Value::String(method)) if !method.is_empty() => method,
        _ => {
            return Envelope::Invalid {
                id,
                message: "method must be a non-empty string".to_string(),
            };
        }
    };

    let params = match object.remove("params") {
        None => None,
        Some(Value::Object(map)) => Some(Value::Object(map)),
        Some(_) => {
            return Envelope::Invalid {
                id,
                message: "params must be a JSON object".to_string(),
            };
        }
    };

    match id {
        Some(id) => Envelope::Request { id, method, params },
        None => Envelope::Notification,
    }
}

/// Build a successful JSON-RPC response.
pub fn success(id: &RequestId, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id.to_value(),
        "result": result,
    })
}

/// Build a JSON-RPC error response.
pub fn error(
    id: Option<&RequestId>,
    code: i64,
    message: impl Into<String>,
    data: Option<Value>,
) -> Value {
    let message = message.into();
    let error = match data {
        Some(data) => json!({"code": code, "message": message, "data": data}),
        None => json!({"code": code, "message": message}),
    };
    json!({
        "jsonrpc": "2.0",
        "id": id.map(RequestId::to_value).unwrap_or(Value::Null),
        "error": error,
    })
}

/// Serialize one response into the single compact line written to stdout.
pub fn to_line(response: &Value) -> String {
    serde_json::to_string(response).expect("serializing a JSON value cannot fail")
}

/// Result of checking the request's declared protocol revision.
pub enum VersionCheck {
    /// The request declares the supported revision.
    Supported,
    /// The request carries no protocol revision metadata.
    Missing,
    /// The request declares a different revision; the value is preserved.
    Unsupported(Value),
}

/// Read `params._meta["io.modelcontextprotocol/protocolVersion"]`.
pub fn check_request_version(params: Option<&Value>) -> VersionCheck {
    let requested = params
        .and_then(|params| params.get("_meta"))
        .and_then(|meta| meta.get(VERSION_META_KEY));
    match requested {
        Some(Value::String(version)) if version == PROTOCOL_VERSION => VersionCheck::Supported,
        Some(other) => VersionCheck::Unsupported(other.clone()),
        None => VersionCheck::Missing,
    }
}

/// Build the `data` payload for `-32022` unsupported-version errors.
pub fn unsupported_version_data(requested: Option<&Value>) -> Value {
    json!({
        "supported": [PROTOCOL_VERSION],
        "requested": requested.cloned().unwrap_or(Value::Null),
    })
}

/// Build a complete-result object from its body fields.
pub fn complete_result(mut body: Map<String, Value>) -> Value {
    body.insert(
        "resultType".to_string(),
        Value::String("complete".to_string()),
    );
    Value::Object(body)
}

/// Build a complete result carrying the standard catalog cache hints.
pub fn complete_cached(mut body: Map<String, Value>) -> Value {
    body.insert("ttlMs".to_string(), json!(TTL_MS));
    body.insert("cacheScope".to_string(), json!(CACHE_SCOPE));
    complete_result(body)
}

/// Build the single modern discovery result.
pub fn discover_result() -> Value {
    let mut body = Map::new();
    body.insert("supportedVersions".to_string(), json!([PROTOCOL_VERSION]));
    body.insert("capabilities".to_string(), json!({"tools": {}}));
    body.insert(
        "_meta".to_string(),
        json!({
            "io.modelcontextprotocol/serverInfo": {
                "name": "zul-mcp",
                "version": env!("CARGO_PKG_VERSION"),
            },
        }),
    );
    body.insert(
        "instructions".to_string(),
        json!(
            "zul-mcp exposes read-only Zulip tools over MCP 2026-07-28. \
    Call tools/list to discover them. Tools may report missing credentials until \
    Zulip auth is configured; discovery and tools/list do not require it."
        ),
    );
    complete_cached(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: Value) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/list",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                },
            },
        })
    }

    #[test]
    fn preserves_identifier_types() {
        let cases = [json!(7), json!("7"), Value::Null];
        for value in cases {
            match parse_envelope(request(value.clone())) {
                Envelope::Request { id, .. } => assert_eq!(id.to_value(), value),
                _ => panic!("expected a request"),
            }
        }
    }

    #[test]
    fn distinguishes_notifications_from_requests() {
        let notification = json!({"jsonrpc": "2.0", "method": "tools/list"});
        assert!(matches!(
            parse_envelope(notification),
            Envelope::Notification
        ));
    }

    #[test]
    fn rejects_non_object_params() {
        let frame = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": []});
        match parse_envelope(frame) {
            Envelope::Invalid { id, .. } => assert_eq!(id, Some(RequestId::Number(1.into()))),
            _ => panic!("expected invalid request"),
        }
    }

    #[test]
    fn version_gate_reports_missing_and_unsupported() {
        assert!(matches!(check_request_version(None), VersionCheck::Missing));
        assert!(matches!(
            check_request_version(Some(&json!({"_meta": {}}))),
            VersionCheck::Missing
        ));
        match check_request_version(Some(&json!({
            "_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"},
        }))) {
            VersionCheck::Unsupported(requested) => assert_eq!(requested, json!("2025-11-25")),
            _ => panic!("expected an unsupported version"),
        }
        assert!(matches!(
            check_request_version(Some(&json!({
                "_meta": {"io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION},
            }))),
            VersionCheck::Supported
        ));
    }

    #[test]
    fn error_omits_data_when_not_supplied() {
        let response = error(None, INVALID_REQUEST, "bad", None);
        assert_eq!(response["id"], Value::Null);
        assert!(response["error"].get("data").is_none());
        assert!(response.get("resultType").is_none());
    }

    #[test]
    fn discovery_advertises_only_tools() {
        let result = discover_result();
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["supportedVersions"], json!([PROTOCOL_VERSION]));
        assert_eq!(result["capabilities"], json!({"tools": {}}));
        assert_eq!(
            result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "zul-mcp"
        );
        assert!(result["instructions"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert_eq!(result["cacheScope"], CACHE_SCOPE);
        assert_eq!(result["ttlMs"], TTL_MS);
    }

    #[test]
    fn complete_results_share_one_envelope() {
        let mut body = Map::new();
        body.insert("value".to_string(), json!(1));
        let plain = complete_result(body.clone());
        assert_eq!(plain["resultType"], "complete");
        assert_eq!(plain["value"], 1);
        assert!(plain.get("ttlMs").is_none());
        assert!(plain.get("cacheScope").is_none());

        let cached = complete_cached(body);
        assert_eq!(cached["resultType"], "complete");
        assert_eq!(cached["ttlMs"], TTL_MS);
        assert_eq!(cached["cacheScope"], CACHE_SCOPE);
        assert_eq!(cached["value"], 1);
    }
}
