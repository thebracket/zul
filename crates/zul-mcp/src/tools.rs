//! Tool catalog and handlers, independent of the JSON-RPC transport.
//!
//! The catalog is static and cheap: `tools/list` never touches configuration
//! or the network. Tool handlers own their own state, including the single
//! lazily opened [`libzul::App`] used by `whoami`.

use libzul::{App, AuthStatus, Settings};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::rc::Rc;

/// Tool name for the local identity probe.
pub const WHOAMI: &str = "whoami";

/// Return the descriptors for every advertised tool.
pub fn catalog() -> Value {
    json!([
        {
            "name": WHOAMI,
            "description": "Report the local Zulip realm URL, account email, and whether Zulip access is ready.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }
    ])
}

/// Lazily initialized, process-wide view of local Zulip configuration.
#[derive(Default)]
pub struct ToolState {
    app: RefCell<Option<Result<Rc<App>, String>>>,
}

impl ToolState {
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

/// Why a `tools/call` did not produce a normal result.
pub enum ToolFailure {
    /// The requested tool is not in the catalog.
    UnknownTool(String),
    /// The arguments do not match the tool's input schema.
    InvalidArguments(String),
}

/// Invoke a tool by name.
pub async fn call(
    state: &ToolState,
    name: &str,
    arguments: Option<&Value>,
) -> Result<Value, ToolFailure> {
    match name {
        WHOAMI => {
            ensure_no_arguments(arguments)?;
            Ok(whoami(state).await)
        }
        other => Err(ToolFailure::UnknownTool(other.to_string())),
    }
}

/// Build an `isError` tool result for unusable arguments.
pub fn invalid_arguments_result(message: &str) -> Value {
    json!({
        "resultType": "complete",
        "content": [{"type": "text", "text": message}],
        "isError": true,
    })
}

async fn whoami(state: &ToolState) -> Value {
    match state.app() {
        Ok(app) => {
            let status = app.auth_status().await;
            let (base_url, email) = match &status {
                AuthStatus::Ready { base_url, email } => {
                    (Some(base_url.clone()), Some(email.clone()))
                }
                AuthStatus::Missing { .. } => (
                    present(app.settings().zulip.base_url.clone()),
                    present(app.settings().zulip.email.clone()),
                ),
            };
            let problems = match status {
                AuthStatus::Ready { .. } => Vec::new(),
                AuthStatus::Missing { problems } => problems,
            };
            let ready = problems.is_empty();
            complete_result(base_url, email, ready, problems)
        }
        Err(problem) => complete_result(None, None, false, vec![problem]),
    }
}

fn present(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

fn complete_result(
    base_url: Option<String>,
    email: Option<String>,
    ready: bool,
    problems: Vec<String>,
) -> Value {
    let structured = json!({
        "base_url": base_url,
        "email": email,
        "ready": ready,
        "problems": problems,
    });
    let text = structured.to_string();
    json!({
        "resultType": "complete",
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
    })
}

fn ensure_no_arguments(arguments: Option<&Value>) -> Result<(), ToolFailure> {
    match arguments {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Object(map)) if map.is_empty() => Ok(()),
        Some(Value::Object(map)) => {
            let mut names: Vec<&str> = map.keys().map(String::as_str).collect();
            names.sort_unstable();
            Err(ToolFailure::InvalidArguments(format!(
                "whoami does not accept arguments; unexpected: {}",
                names.join(", ")
            )))
        }
        Some(_) => Err(ToolFailure::InvalidArguments(
            "whoami arguments must be a JSON object".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_one_strict_whoami_schema() {
        let tools = catalog();
        let tools = tools.as_array().expect("catalog is an array");
        assert_eq!(tools.len(), 1);
        let tool = &tools[0];
        assert_eq!(tool["name"], WHOAMI);
        assert!(tool["description"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert_eq!(tool["inputSchema"]["type"], "object");
        assert!(tool["inputSchema"]["properties"].is_object());
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }

    #[test]
    fn whoami_accepts_only_empty_arguments() {
        assert!(ensure_no_arguments(None).is_ok());
        assert!(ensure_no_arguments(Some(&json!({}))).is_ok());
        assert!(matches!(
            ensure_no_arguments(Some(&json!({"stream": "general"}))),
            Err(ToolFailure::InvalidArguments(_))
        ));
        assert!(matches!(
            ensure_no_arguments(Some(&json!(["general"]))),
            Err(ToolFailure::InvalidArguments(_))
        ));
    }

    #[test]
    fn complete_result_text_matches_structured_content() {
        let result = complete_result(
            Some("https://chat.example.com".to_string()),
            Some("user@example.com".to_string()),
            true,
            Vec::new(),
        );
        assert_eq!(result["resultType"], "complete");
        assert_eq!(result["structuredContent"]["ready"], true);
        assert!(result["structuredContent"]["problems"]
            .as_array()
            .is_some_and(Vec::is_empty));
        let text = result["content"][0]["text"].as_str().expect("text content");
        let parsed: Value = serde_json::from_str(text).expect("text is JSON");
        assert_eq!(parsed, result["structuredContent"]);
        assert!(!text.contains("api_key"));
    }
}
