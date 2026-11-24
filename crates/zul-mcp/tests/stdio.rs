//! End-to-end stdio tests for the `zul-mcp` binary.
//!
//! Each test spawns the real binary with a private home directory, strips any
//! developer Zulip credentials from the environment, and speaks newline-
//! delimited JSON-RPC over its pipes.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

fn isolated_command(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zul-mcp"));
    command
        .current_dir(dir)
        .env("HOME", dir)
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_CACHE_HOME", dir.join("cache"))
        .env("XDG_DATA_HOME", dir.join("data"));
    for (key, _) in std::env::vars() {
        if key.starts_with("ZULIP_") || key.starts_with("ZUL_") {
            command.env_remove(key);
        }
    }
    command
}

struct Harness {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stderr: Option<ChildStderr>,
    output: Receiver<String>,
    reader: Option<JoinHandle<()>>,
}

impl Harness {
    fn start(dir: &Path) -> Self {
        Self::start_with_env(dir, &[])
    }

    fn start_with_env(dir: &Path, environment: &[(&str, &str)]) -> Self {
        let mut command = isolated_command(dir);
        for (key, value) in environment {
            command.env(key, value);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().expect("spawn zul-mcp");
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (sender, output) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child: Some(child),
            stdin: Some(stdin),
            stderr: Some(stderr),
            output,
            reader: Some(reader),
        }
    }

    fn send(&mut self, request: &Value) {
        self.send_line(&request.to_string());
    }

    fn send_line(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        writeln!(stdin, "{line}").expect("write request");
        stdin.flush().expect("flush request");
    }

    fn send_raw(&mut self, bytes: &[u8]) {
        let stdin = self.stdin.as_mut().expect("stdin open");
        stdin.write_all(bytes).expect("write raw request");
        stdin.flush().expect("flush raw request");
    }

    fn recv(&self) -> String {
        self.output
            .recv_timeout(RESPONSE_TIMEOUT)
            .expect("server response within timeout")
    }

    fn close_stdin(&mut self) {
        drop(self.stdin.take());
    }

    fn finish(mut self) -> (Vec<String>, ExitStatus, String) {
        drop(self.stdin.take());
        let mut child = self.child.take().expect("child process");

        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll child") {
                break status;
            }
            if started.elapsed() > SHUTDOWN_TIMEOUT {
                let _ = child.kill();
                let _ = child.wait();
                panic!("zul-mcp did not exit within 2 seconds of stdin close");
            }
            thread::sleep(Duration::from_millis(10));
        };

        let mut stderr = String::new();
        if let Some(mut pipe) = self.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        self.reader
            .take()
            .expect("stdout reader handle")
            .join()
            .expect("stdout reader");

        let mut remaining = Vec::new();
        while let Ok(line) = self.output.try_recv() {
            remaining.push(line);
        }
        (remaining, status, stderr)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn collect(harness: &Harness, expected: usize) -> Vec<(String, Value)> {
    (0..expected)
        .map(|_| {
            let line = harness.recv();
            assert_eq!(line, line.trim(), "stdout line has surrounding whitespace");
            let value: Value = serde_json::from_str(&line)
                .unwrap_or_else(|err| panic!("stdout line is not JSON ({err}): {line}"));
            assert!(value.is_object(), "stdout line is not an object: {line}");
            assert_eq!(value["jsonrpc"], "2.0", "not a JSON-RPC frame: {line}");
            (line, value)
        })
        .collect()
}

fn id_key(value: &Value) -> String {
    match &value["id"] {
        Value::String(text) => format!("s:{text}"),
        Value::Number(number) => format!("n:{number}"),
        Value::Null => "null".to_string(),
        other => panic!("unexpected response id: {other}"),
    }
}

fn by_id(responses: &[(String, Value)]) -> HashMap<String, Value> {
    let map: HashMap<String, Value> = responses
        .iter()
        .map(|(_, value)| (id_key(value), value.clone()))
        .collect();
    assert_eq!(
        map.len(),
        responses.len(),
        "responses repeated an id: {:?}",
        responses.iter().map(|(line, _)| line).collect::<Vec<_>>()
    );
    map
}

fn meta() -> Value {
    json!({"io.modelcontextprotocol/protocolVersion": "2026-07-28"})
}

fn modern(id: Value, method: &str, params: Value) -> Value {
    let mut params = params;
    params["_meta"] = meta();
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

#[test]
fn serves_discovery_tools_and_protocol_errors() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut harness = Harness::start(dir.path());

    // 1: discovery without any metadata or parameters.
    harness.send(&json!({"jsonrpc": "2.0", "id": 1, "method": "server/discover"}));
    // 2: modern metadata is required for other requests.
    harness.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}));
    // 3: unsupported revision is reported with detail.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/list",
        "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"}},
    }));
    // 4: the supported catalog.
    harness.send(&modern(json!(4), "tools/list", json!({})));
    // 5: unknown method with valid metadata.
    harness.send(&modern(json!(5), "prompts/list", json!({})));
    // 6: malformed JSON.
    harness.send_line("{not json");
    // 7: legacy initialize.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
    }));
    // 8: a notification must not produce a response.
    harness.send(&json!({"jsonrpc": "2.0", "method": "tools/list", "params": {}}));
    // 9: string ids are echoed as strings.
    harness.send(&json!({"jsonrpc": "2.0", "id": "abc", "method": "server/discover"}));
    // 10: whoami without credentials must stay local and report not ready.
    harness.send(&modern(
        json!(10),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    // 11: unknown tool.
    harness.send(&modern(
        json!(11),
        "tools/call",
        json!({"name": "sends_message", "arguments": {}}),
    ));
    // 12: malformed arguments are a tool error, not a protocol error.
    harness.send(&modern(
        json!(12),
        "tools/call",
        json!({"name": "whoami", "arguments": {"stream": "general"}}),
    ));
    // 13: still serving after all of the above.
    harness.send(&json!({"jsonrpc": "2.0", "id": 13, "method": "server/discover"}));
    // 14: structurally invalid requests are rejected at the envelope layer.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 14,
        "method": "tools/list",
        "params": [],
    }));
    // 15: sync without credentials is a tool error, not a panic.
    harness.send(&modern(
        json!(15),
        "tools/call",
        json!({"name": "sync", "arguments": {}}),
    ));
    // 16: tools/call without metadata hits the version gate.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 16,
        "method": "tools/call",
        "params": {"name": "whoami", "arguments": {}},
    }));
    // 17: an unknown method without metadata is still -32601.
    harness.send(&json!({"jsonrpc": "2.0", "id": 17, "method": "prompts/list"}));
    // 18: tools/call with an unsupported revision hits the version gate.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 18,
        "method": "tools/call",
        "params": {
            "name": "whoami",
            "arguments": {},
            "_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"},
        },
    }));
    // 19: an unknown method with an unsupported revision is also -32601.
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 19,
        "method": "prompts/list",
        "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2025-11-25"}},
    }));

    let raw = collect(&harness, 18);
    let responses = by_id(&raw);

    let discover = responses.get("n:1").expect("discovery response");
    assert_eq!(discover["id"], json!(1));
    let result = &discover["result"];
    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["supportedVersions"], json!(["2026-07-28"]));
    assert_eq!(
        result["capabilities"],
        json!({
            "tools": {},
            "extensions": {
                "io.modelcontextprotocol/events": {},
            },
        })
    );
    assert!(
        result["capabilities"].get("events").is_none(),
        "events must be an extension, not a top-level capability: {result}"
    );
    assert!(
        result["capabilities"]["extensions"]["io.modelcontextprotocol/events"]
            .get("listChanged")
            .is_none(),
        "the events extension must not advertise listChanged: {result}"
    );
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "zul-mcp"
    );
    assert_eq!(
        result["_meta"]["io.modelcontextprotocol/serverInfo"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    assert!(result["instructions"]
        .as_str()
        .is_some_and(|text| !text.is_empty()));
    assert!(result["ttlMs"].is_number());
    assert_eq!(result["cacheScope"], "private");

    let missing_meta = responses.get("n:2").expect("missing metadata response");
    assert_eq!(missing_meta["error"]["code"], -32022);
    assert_eq!(
        missing_meta["error"]["data"]["supported"],
        json!(["2026-07-28"])
    );
    assert_eq!(missing_meta["error"]["data"]["requested"], Value::Null);

    let unsupported = responses.get("n:3").expect("unsupported version response");
    assert_eq!(unsupported["error"]["code"], -32022);
    assert_eq!(
        unsupported["error"]["data"]["supported"],
        json!(["2026-07-28"])
    );
    assert_eq!(unsupported["error"]["data"]["requested"], "2025-11-25");

    let list = &responses.get("n:4").expect("tools/list response")["result"];
    assert_eq!(list["resultType"], "complete");
    assert!(list["ttlMs"].is_number());
    assert_eq!(list["cacheScope"], "private");
    let tools = list["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();
    assert_eq!(
        names,
        vec![
            "whoami",
            "sync",
            "list_streams",
            "list_topics",
            "read_topic",
            "recent",
            "search",
            "send_message"
        ]
    );
    for tool in tools {
        assert!(tool["description"]
            .as_str()
            .is_some_and(|text| !text.is_empty()));
        assert_eq!(tool["inputSchema"]["type"], "object");
        assert!(tool["inputSchema"]["properties"].is_object());
        assert!(tool["inputSchema"]["required"].is_array());
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
    let whoami = &tools[0];
    assert_eq!(whoami["name"], "whoami");

    let unknown = responses.get("n:5").expect("unknown method response");
    assert_eq!(unknown["error"]["code"], -32601);

    let parse = responses.get("null").expect("parse error response");
    assert_eq!(parse["id"], Value::Null);
    assert_eq!(parse["error"]["code"], -32700);

    let initialize = responses.get("n:7").expect("initialize response");
    assert_eq!(initialize["error"]["code"], -32601);
    let message = initialize["error"]["message"]
        .as_str()
        .expect("initialize message");
    assert!(message.contains("requires MCP 2026-07-28"), "{message}");
    assert!(
        message.contains("set the client's protocol to \"2026-07-28\""),
        "{message}"
    );

    let string_id = responses.get("s:abc").expect("string-id response");
    assert_eq!(string_id["id"], json!("abc"));
    assert!(string_id["id"].is_string());

    let who = &responses.get("n:10").expect("whoami response")["result"];
    assert_eq!(who["resultType"], "complete");
    let content = who["content"].as_array().expect("content array");
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["type"], "text");
    let structured = &who["structuredContent"];
    assert_eq!(structured["ready"], false);
    assert!(structured["base_url"].is_null());
    assert!(structured["email"].is_null());
    assert!(
        structured["problems"]
            .as_array()
            .is_some_and(|problems| !problems.is_empty()),
        "missing credentials must be reported: {structured}"
    );
    for problem in structured["problems"].as_array().expect("problems") {
        assert!(problem.as_str().is_some_and(|text| !text.is_empty()));
    }
    assert!(
        structured["problems"]
            .as_array()
            .expect("problems")
            .iter()
            .any(|problem| problem
                .as_str()
                .is_some_and(|text| text.contains("missing Zulip API key"))),
        "the missing API key must be named: {structured}"
    );
    let text = content[0]["text"].as_str().expect("text content");
    let parsed: Value = serde_json::from_str(text).expect("text content is JSON");
    assert_eq!(&parsed, structured);

    let unknown_tool = responses.get("n:11").expect("unknown tool response");
    assert_eq!(unknown_tool["error"]["code"], -32602);

    let bad_arguments = &responses.get("n:12").expect("tool error response")["result"];
    assert_eq!(bad_arguments["isError"], true);
    assert!(bad_arguments["content"]
        .as_array()
        .is_some_and(|content| !content.is_empty()));

    let still_up = &responses.get("n:13").expect("final discovery response")["result"];
    assert_eq!(still_up["resultType"], "complete");

    let invalid_shape = responses.get("n:14").expect("invalid request response");
    assert_eq!(invalid_shape["error"]["code"], -32600);
    assert!(invalid_shape["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("params")));

    let sync = &responses.get("n:15").expect("sync response")["result"];
    assert_eq!(sync["resultType"], "complete");
    assert_eq!(sync["isError"], true);
    assert!(
        sync.get("structuredContent").is_none(),
        "a failed sync must not carry a structured payload: {sync}"
    );
    assert!(sync["content"][0]["text"]
        .as_str()
        .is_some_and(|text| text.contains("sync failed")));

    let gate_call = responses.get("n:16").expect("tools/call gate response");
    assert_eq!(gate_call["error"]["code"], -32022);

    let unknown_without_meta = responses
        .get("n:17")
        .expect("unknown method without metadata");
    assert_eq!(unknown_without_meta["error"]["code"], -32601);

    let gate_call_unsupported = responses
        .get("n:18")
        .expect("tools/call unsupported revision");
    assert_eq!(gate_call_unsupported["error"]["code"], -32022);

    let unknown_unsupported = responses
        .get("n:19")
        .expect("unknown method with unsupported revision");
    assert_eq!(unknown_unsupported["error"]["code"], -32601);

    for (line, value) in &raw {
        assert!(
            !line.contains("api_key") && !value.to_string().contains("api_key"),
            "tool output leaked an API key field: {line}"
        );
    }

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
    assert!(
        stderr.contains("serving MCP"),
        "expected diagnostics on stderr, got: {stderr:?}"
    );
}

#[test]
fn send_message_without_credentials_is_a_tool_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut harness = Harness::start(dir.path());

    harness.send(&modern(
        json!(1),
        "tools/call",
        json!({
            "name": "send_message",
            "arguments": {
                "stream": "core",
                "topic": "ops",
                "message": "hello from stdio",
                "confirm": true,
            },
        }),
    ));
    let line = harness.recv();
    let response: Value = serde_json::from_str(&line).expect("send_message response is JSON");
    let result = &response["result"];
    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["isError"], true);
    assert!(
        result.get("structuredContent").is_none(),
        "a failed send must not carry a structured payload: {result}"
    );
    let text = result["content"][0]["text"].as_str().expect("text content");
    assert!(
        text.contains("missing Zulip API key"),
        "the missing credential must be named: {text}"
    );
    assert!(
        !text.contains("hello from stdio"),
        "an auth failure must not echo the message body: {text}"
    );

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
}

#[test]
fn answers_requests_read_before_stdin_closes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut harness = Harness::start(dir.path());

    harness.send(&json!({"jsonrpc": "2.0", "id": 1, "method": "server/discover"}));
    harness.send(&modern(json!(2), "tools/list", json!({})));
    harness.send(&modern(json!(3), "sends_message", json!({})));
    harness.send_line("{not json");
    harness.close_stdin();

    let responses = collect(&harness, 4);
    let responses = by_id(&responses);
    assert_eq!(
        responses.get("n:1").expect("discovery")["result"]["resultType"],
        "complete"
    );
    assert_eq!(
        responses.get("n:2").expect("tools list")["result"]["tools"]
            .as_array()
            .map(Vec::len),
        Some(8)
    );
    assert_eq!(
        responses.get("n:3").expect("unknown method")["error"]["code"],
        -32601
    );
    assert_eq!(
        responses.get("null").expect("parse error")["error"]["code"],
        -32700
    );

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
}

#[test]
fn rejects_oversized_lines_and_keeps_serving() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut harness = Harness::start(dir.path());

    let mut oversized = vec![b'x'; 1024 * 1024 + 64];
    oversized.push(b'\n');
    harness.send_raw(&oversized);
    harness.send(&json!({"jsonrpc": "2.0", "id": 1, "method": "server/discover"}));

    let responses = collect(&harness, 2);
    let responses = by_id(&responses);

    let oversized_error = responses.get("null").expect("oversized line response");
    assert_eq!(oversized_error["error"]["code"], -32700);
    assert!(oversized_error["error"]["message"]
        .as_str()
        .is_some_and(|message| message.contains("1 MiB")));

    let discover = responses.get("n:1").expect("discovery response");
    assert_eq!(discover["result"]["resultType"], "complete");

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
}

#[test]
fn exits_promptly_when_stdout_closes() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut command = isolated_command(dir.path());
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn zul-mcp");
    let mut stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("piped stdout"));

    writeln!(
        stdin,
        "{}",
        json!({"jsonrpc": "2.0", "id": 1, "method": "server/discover"})
    )
    .expect("write discovery");
    stdin.flush().expect("flush discovery");
    let mut line = String::new();
    stdout
        .read_line(&mut line)
        .expect("read discovery response");
    assert_eq!(
        serde_json::from_str::<Value>(line.trim()).expect("discovery response is JSON")["jsonrpc"],
        "2.0"
    );
    drop(stdout);

    // This response cannot be written. The server must exit even though stdin
    // is still open, rather than waiting on the parked stdin read.
    let _ = writeln!(
        stdin,
        "{}",
        json!({"jsonrpc": "2.0", "id": 2, "method": "server/discover"})
    );
    let _ = stdin.flush();

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if started.elapsed() > SHUTDOWN_TIMEOUT {
            let _ = child.kill();
            let _ = child.wait();
            panic!("zul-mcp did not exit after stdout closed");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "exit status: {status:?}");
    drop(stdin);
}

#[test]
fn zul_dotenv_controls_dotenv_loading() {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(
        dir.path().join(".env"),
        "ZULIP_BASE_URL=https://dotenv.example\nZULIP_EMAIL=dotenv@example.com\n",
    )
    .expect("write dotenv file");

    // Unset ZUL_DOTENV keeps the repository convention: discover `.env` in cwd.
    let mut loaded = Harness::start(dir.path());
    loaded.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    let line = loaded.recv();
    let response: Value = serde_json::from_str(&line).expect("whoami response is JSON");
    let structured = &response["result"]["structuredContent"];
    assert_eq!(structured["base_url"], "https://dotenv.example");
    assert_eq!(structured["email"], "dotenv@example.com");
    let (remaining, status, stderr) = loaded.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout: {remaining:?}"
    );

    // An empty ZUL_DOTENV disables dotenv loading entirely.
    let mut disabled = Harness::start_with_env(dir.path(), &[("ZUL_DOTENV", "")]);
    disabled.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    let line = disabled.recv();
    let response: Value = serde_json::from_str(&line).expect("whoami response is JSON");
    let structured = &response["result"]["structuredContent"];
    assert!(structured["base_url"].is_null());
    assert!(structured["email"].is_null());
    let (remaining, status, stderr) = disabled.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout: {remaining:?}"
    );
}

#[test]
fn zul_dotenv_path_selects_the_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("explicit.env");
    std::fs::write(
        &path,
        "ZULIP_BASE_URL=https://dotenv-path.example\nZULIP_EMAIL=path@example.com\n",
    )
    .expect("write explicit env file");
    let path = path.to_str().expect("utf-8 env path");

    // The working directory deliberately has no `.env`; only ZUL_DOTENV can
    // supply these values.
    let mut harness = Harness::start_with_env(dir.path(), &[("ZUL_DOTENV", path)]);
    harness.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    let line = harness.recv();
    let response: Value = serde_json::from_str(&line).expect("whoami response is JSON");
    let structured = &response["result"]["structuredContent"];
    assert_eq!(structured["base_url"], "https://dotenv-path.example");
    assert_eq!(structured["email"], "path@example.com");
    assert_eq!(structured["ready"], false);
    assert!(
        structured["problems"]
            .as_array()
            .expect("problems")
            .iter()
            .any(|problem| problem
                .as_str()
                .is_some_and(|text| text.contains("missing Zulip API key"))),
        "the explicit env file has no key, so the missing-key problem must be named: {structured}"
    );

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
}

#[test]
fn whoami_with_missing_api_key_reports_it_without_contacting_the_realm() {
    let dir = tempfile::tempdir().expect("temp dir");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let mut harness = Harness::start_with_env(
        dir.path(),
        &[
            ("ZULIP_BASE_URL", &base_url),
            ("ZULIP_EMAIL", "ada@example.com"),
        ],
    );

    harness.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    let line = harness.recv();
    let response: Value = serde_json::from_str(&line).expect("whoami response is JSON");
    let structured = &response["result"]["structuredContent"];
    assert_eq!(structured["ready"], false);
    assert_eq!(structured["base_url"], base_url);
    assert_eq!(structured["email"], "ada@example.com");
    assert!(
        structured["problems"]
            .as_array()
            .expect("problems")
            .iter()
            .any(|problem| problem
                .as_str()
                .is_some_and(|text| text.contains("missing Zulip API key"))),
        "the missing API key must be named: {structured}"
    );

    match listener.accept() {
        Err(err) if err.kind() == ErrorKind::WouldBlock => {}
        Ok(_) => panic!("whoami must not contact the realm while auth is incomplete"),
        Err(err) => panic!("checking for a whoami connection failed: {err}"),
    }

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
}

const COMPLETE_CREDENTIALS_KEY: &str = "zul-mcp-stdio-key-7b2e-DO-NOT-LEAK";

/// Serve one JSON response and return the request line and headers.
fn serve_one_response(listener: TcpListener, body: &'static str) -> JoinHandle<String> {
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept HTTP request");
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let read = socket.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .expect("write response");
        socket.flush().expect("flush response");
        String::from_utf8_lossy(&request).into_owned()
    })
}

/// Serve one successful Zulip current-user request.
fn serve_one_current_user(listener: TcpListener) -> JoinHandle<String> {
    serve_one_response(
        listener,
        r#"{"result":"success","msg":"","email":"ada@example.com","full_name":"Ada Lovelace"}"#,
    )
}

#[test]
fn whoami_with_complete_credentials_never_leaks_the_api_key() {
    let dir = tempfile::tempdir().expect("temp dir");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let responder = serve_one_current_user(listener);
    let mut harness = Harness::start_with_env(
        dir.path(),
        &[
            ("ZULIP_BASE_URL", &base_url),
            ("ZULIP_EMAIL", "ada@example.com"),
            ("ZULIP_API_KEY", COMPLETE_CREDENTIALS_KEY),
        ],
    );

    harness.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));
    let line = harness.recv();
    let response: Value = serde_json::from_str(&line).expect("whoami response is JSON");
    let result = &response["result"];
    assert_eq!(result["resultType"], "complete");
    assert_ne!(result["isError"], true);
    let structured = &result["structuredContent"];
    assert_eq!(structured["ready"], true);
    assert_eq!(structured["base_url"], base_url);
    assert_eq!(structured["email"], "ada@example.com");
    assert_eq!(structured["problems"], json!([]));
    let text = result["content"][0]["text"].as_str().expect("text content");
    let parsed: Value = serde_json::from_str(text).expect("text content is JSON");
    assert_eq!(&parsed, structured);

    assert!(
        !line.contains(COMPLETE_CREDENTIALS_KEY),
        "API key leaked into the stdout line: {line}"
    );
    assert!(
        !structured.to_string().contains(COMPLETE_CREDENTIALS_KEY),
        "API key leaked into structuredContent: {structured}"
    );
    assert!(
        !text.contains(COMPLETE_CREDENTIALS_KEY),
        "API key leaked into the text mirror: {text}"
    );

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    for output in &remaining {
        assert!(
            !output.contains(COMPLETE_CREDENTIALS_KEY),
            "API key leaked to stdout: {output}"
        );
    }
    assert!(
        !stderr.contains(COMPLETE_CREDENTIALS_KEY),
        "API key leaked to stderr: {stderr}"
    );

    let request = responder.join().expect("responder thread");
    assert!(
        request.starts_with("GET /api/v1/users/me"),
        "whoami must authenticate against the realm: {request}"
    );
}

#[test]
fn events_list_and_poll_serve_the_extension() {
    let dir = tempfile::tempdir().expect("temp dir");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let responder = serve_one_response(
        listener,
        r#"{"result":"success","msg":"","messages":[{"id":77}]}"#,
    );
    let mut harness = Harness::start_with_env(
        dir.path(),
        &[
            ("ZULIP_BASE_URL", &base_url),
            ("ZULIP_EMAIL", "ada@example.com"),
            ("ZULIP_API_KEY", COMPLETE_CREDENTIALS_KEY),
        ],
    );

    harness.send(&modern(json!(1), "events/list", json!({})));
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "events/list",
        "params": {},
    }));
    harness.send(&modern(
        json!(3),
        "events/stream",
        json!({"name": "zulip.message"}),
    ));
    harness.send(&modern(
        json!(4),
        "events/poll",
        json!({"name": "zulip.message", "cursor": null}),
    ));
    harness.send(&modern(
        json!(5),
        "events/poll",
        json!({"name": "other.event"}),
    ));
    harness.send(&json!({
        "jsonrpc": "2.0",
        "id": 6,
        "method": "events/poll",
        "params": {"name": "zulip.message"},
    }));

    let raw = collect(&harness, 6);
    let responses = by_id(&raw);

    let list = &responses.get("n:1").expect("events/list response")["result"];
    assert_eq!(list["resultType"], "complete");
    let events = list["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "zulip.message");
    assert_eq!(events[0]["delivery"], json!(["poll", "push"]));

    assert_eq!(
        responses.get("n:2").expect("events/list without metadata")["error"]["code"],
        -32022
    );
    assert_eq!(
        responses.get("n:3").expect("events/stream response")["error"]["code"],
        -32601
    );

    let poll = &responses.get("n:4").expect("events/poll response")["result"];
    assert_eq!(poll["resultType"], "complete");
    assert_eq!(poll["events"], json!([]));
    assert_eq!(poll["cursor"], json!(format!("v1:{base_url}|77")));
    assert_eq!(poll["truncated"], false);
    assert_eq!(poll["hasMore"], false);
    assert_eq!(poll["nextPollMs"], 15_000);
    assert!(!poll.to_string().contains(COMPLETE_CREDENTIALS_KEY));

    assert_eq!(
        responses.get("n:5").expect("unknown event response")["error"]["code"],
        -32602
    );
    assert_eq!(
        responses.get("n:6").expect("events/poll without metadata")["error"]["code"],
        -32022
    );

    let (remaining, status, stderr) = harness.finish();
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "unexpected extra stdout lines: {remaining:?}"
    );
    assert!(!stderr.contains(COMPLETE_CREDENTIALS_KEY));

    let request = responder.join().expect("responder thread");
    assert!(
        request.starts_with("GET /api/v1/messages?"),
        "events/poll must ask the realm for the newest message: {request}"
    );
    for parameter in [
        "anchor=newest",
        "num_before=0",
        "num_after=0",
        "include_anchor=true",
    ] {
        assert!(
            request.contains(parameter),
            "latest-message request is missing {parameter}: {request}"
        );
    }
}

#[test]
fn whoami_never_leaks_a_configured_api_key_when_the_realm_stalls() {
    const KEY: &str = "zul-mcp-fake-key-9f3c-DO-NOT-LEAK";

    let dir = tempfile::tempdir().expect("temp dir");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let base_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let mut harness = Harness::start_with_env(
        dir.path(),
        &[
            ("ZULIP_BASE_URL", &base_url),
            ("ZULIP_EMAIL", "ada@example.com"),
            ("ZULIP_API_KEY", KEY),
        ],
    );

    harness.send(&modern(
        json!(1),
        "tools/call",
        json!({"name": "whoami", "arguments": {}}),
    ));

    // Hold the accepted connection open without responding, parking whoami.
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut connection = None;
    while Instant::now() < deadline {
        match listener.accept() {
            Ok((socket, _)) => {
                connection = Some(socket);
                break;
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("accepting the whoami connection failed: {err}"),
        }
    }
    let connection = connection.expect("whoami should connect to the stalled realm");

    // Another request still completes while whoami is parked.
    harness.send(&json!({"jsonrpc": "2.0", "id": 2, "method": "server/discover"}));
    let discover_line = harness.recv();
    let discover: Value = serde_json::from_str(&discover_line).expect("discovery response is JSON");
    assert_eq!(discover["id"], 2);

    let started = Instant::now();
    let (remaining, status, stderr) = harness.finish();
    assert!(
        started.elapsed() < SHUTDOWN_TIMEOUT,
        "closing stdin must abort the parked whoami"
    );
    assert!(status.success(), "exit status {status:?}; stderr: {stderr}");
    assert!(
        remaining.is_empty(),
        "the aborted whoami must not answer: {remaining:?}"
    );
    for output in std::iter::once(&discover_line).chain(remaining.iter()) {
        assert!(!output.contains(KEY), "API key leaked to stdout: {output}");
    }
    assert!(!stderr.contains(KEY), "API key leaked to stderr: {stderr}");
    drop(connection);
}

#[test]
fn exits_when_the_stdout_queue_saturates() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut command = isolated_command(dir.path());
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn zul-mcp");
    let mut stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");

    // Parse errors are answered by the read loop itself through `Server::send`
    // (try_send), unlike tool responses which wait for writer capacity. More
    // frames than the writer queue (256) and the OS pipe can buffer therefore
    // drive the drop-on-full arm. The trailing discoveries then keep the
    // writer blocked at EOF so the drain timeout also fires. The test
    // deliberately never reads stdout until after the child exits.
    const MALFORMED: usize = 2000;
    const DISCOVERIES: usize = 2000;
    for _ in 0..MALFORMED {
        writeln!(stdin, "{{not json").expect("write malformed frame");
    }
    for id in 0..DISCOVERIES {
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc": "2.0", "id": id, "method": "server/discover"})
        )
        .expect("write discovery");
    }
    stdin.flush().expect("flush frames");
    drop(stdin);

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if started.elapsed() > Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("zul-mcp deadlocked with a saturated stdout queue");
        }
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "exit status: {status:?}");

    let mut stderr_text = String::new();
    let _ = stderr.read_to_string(&mut stderr_text);
    assert!(
        stderr_text.contains("stdout queue full; dropping responses until the client reads"),
        "expected the read-loop drop diagnostic; stderr: {stderr_text:?}"
    );
    assert_eq!(
        stderr_text
            .matches("stdout queue full; dropping responses")
            .count(),
        1,
        "the drop diagnostic is emitted only once; stderr: {stderr_text:?}"
    );
    assert!(
        stderr_text.contains("stdout writer did not shut down promptly"),
        "expected the saturated writer to hit its drain timeout; stderr: {stderr_text:?}"
    );

    let mut stdout_text = String::new();
    let _ = stdout.read_to_string(&mut stdout_text);
    let answered = stdout_text
        .lines()
        .filter(|line| serde_json::from_str::<Value>(line).is_ok())
        .count();
    assert!(
        answered > 0,
        "the client should still receive some responses"
    );
    assert!(
        answered < MALFORMED + DISCOVERIES,
        "a saturated queue must drop responses instead of delivering all {} (got {answered})",
        MALFORMED + DISCOVERIES
    );

    drop(stdout);
    drop(child);
}
