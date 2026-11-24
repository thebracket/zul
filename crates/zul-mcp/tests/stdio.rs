//! End-to-end stdio tests for the `zul-mcp` binary.
//!
//! Each test spawns the real binary with a private home directory, strips any
//! developer Zulip credentials from the environment, and speaks newline-
//! delimited JSON-RPC over its pipes.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
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
        let mut command = isolated_command(dir);
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

    let responses = collect(&harness, 14);
    let responses = by_id(&responses);

    let discover = responses.get("n:1").expect("discovery response");
    assert_eq!(discover["id"], json!(1));
    let result = &discover["result"];
    assert_eq!(result["resultType"], "complete");
    assert_eq!(result["supportedVersions"], json!(["2026-07-28"]));
    assert_eq!(result["capabilities"], json!({"tools": {}}));
    assert!(
        result["capabilities"].get("extensions").is_none(),
        "events extension must not be advertised yet: {result}"
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
            "search"
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
    assert!(
        structured["problems"]
            .as_array()
            .is_some_and(|problems| !problems.is_empty()),
        "missing credentials must be reported: {structured}"
    );
    for problem in structured["problems"].as_array().expect("problems") {
        assert!(problem.as_str().is_some_and(|text| !text.is_empty()));
    }
    assert!(structured["base_url"].is_null() || structured["base_url"].is_string());
    assert!(structured["email"].is_null() || structured["email"].is_string());
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

    for line in responses.keys() {
        assert!(
            !line.contains("api_key"),
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
        Some(7)
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
