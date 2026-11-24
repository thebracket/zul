//! `zul-mcp` serves Zulip data to MCP clients over newline-delimited JSON-RPC
//! on stdio.
//!
//! The server implements MCP revision 2026-07-28 only: discovery, the tool
//! catalog, and modern per-request `_meta` versioning. Discovery and
//! `tools/list` work without Zulip credentials; tool calls report local
//! readiness instead of failing at startup.
//!
//! Frames are read with a bounded line buffer, dispatched to local tasks so a
//! slow tool cannot block stdin, and written by a single task fed through a
//! bounded queue. Stderr is reserved for diagnostics.

use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use zul_mcp::protocol;
use zul_mcp::server::Server;

/// Largest request line accepted on stdin, in bytes.
const MAX_LINE_BYTES: usize = 1024 * 1024;

/// Capacity of the stdout queue. Bounds buffered output when the client reads slowly.
const OUTPUT_QUEUE: usize = 256;

/// Time allowed for queued requests to finish after stdin closes.
const EOF_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

/// Time allowed for the writer to drain queued frames after stdin closes.
const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let local = tokio::task::LocalSet::new();
    let result = runtime.block_on(local.run_until(serve()));
    // Do not wait for the blocking stdin reader if it is still parked in a
    // read. All frames that matter have already been written by `serve`.
    runtime.shutdown_background();
    result
}

async fn serve() -> anyhow::Result<()> {
    eprintln!(
        "zul-mcp: serving MCP {} over stdio",
        protocol::PROTOCOL_VERSION
    );

    let (tx, rx) = mpsc::channel::<String>(OUTPUT_QUEUE);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut writer = tokio::task::spawn_local(writer_task(rx, shutdown_tx));
    let mut server = Server::new(tx.clone());
    let mut stdin = tokio::io::stdin();
    let mut lines = LineReader::new();
    let mut read_error: Option<io::Error> = None;

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => break,
            event = lines.next(&mut stdin) => match event {
                Ok(Some(ReadEvent::Line(line))) => server.handle_line(&line).await,
                Ok(Some(ReadEvent::Oversized)) => {
                    server
                        .reject("request line exceeds the 1 MiB limit")
                        .await;
                }
                Ok(None) => break,
                Err(err) => {
                    read_error = Some(err);
                    break;
                }
            },
        }
    }

    server.drain(EOF_DRAIN_TIMEOUT).await;
    drop(server);
    drop(tx);

    match tokio::time::timeout(WRITER_DRAIN_TIMEOUT, &mut writer).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("zul-mcp: writer task failed: {err}"),
        Err(_) => {
            eprintln!("zul-mcp: stdout writer did not shut down promptly");
            writer.abort();
        }
    }

    if let Some(err) = read_error {
        return Err(anyhow::anyhow!("reading stdin: {err}"));
    }
    eprintln!("zul-mcp: shutting down");
    Ok(())
}

async fn writer_task(mut rx: mpsc::Receiver<String>, shutdown: watch::Sender<bool>) {
    let mut stdout = tokio::io::stdout();
    while let Some(line) = rx.recv().await {
        if let Err(err) = write_frame(&mut stdout, &line).await {
            eprintln!("zul-mcp: writing stdout failed: {err}");
            let _ = shutdown.send(true);
            return;
        }
    }
}

async fn write_frame(stdout: &mut (impl AsyncWrite + Unpin), line: &str) -> io::Result<()> {
    stdout.write_all(line.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}

/// One complete stdin event or a line that exceeded the size cap.
enum ReadEvent {
    Line(String),
    Oversized,
}

/// Bounded newline-delimited reader.
///
/// The buffer grows to at most [`MAX_LINE_BYTES`] plus one read chunk. Longer
/// lines are discarded up to their newline and reported once as `Oversized`.
struct LineReader {
    pending: Vec<u8>,
    discarding: bool,
    eof: bool,
}

impl LineReader {
    fn new() -> Self {
        Self {
            pending: Vec::new(),
            discarding: false,
            eof: false,
        }
    }

    async fn next<R>(&mut self, reader: &mut R) -> io::Result<Option<ReadEvent>>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            if let Some(index) = self.pending.iter().position(|byte| *byte == b'\n') {
                let mut line: Vec<u8> = self.pending.drain(..=index).collect();
                line.pop();
                trim_carriage_return(&mut line);
                if line.len() > MAX_LINE_BYTES {
                    return Ok(Some(ReadEvent::Oversized));
                }
                if line.iter().all(u8::is_ascii_whitespace) {
                    continue;
                }
                return Ok(Some(ReadEvent::Line(
                    String::from_utf8_lossy(&line).into_owned(),
                )));
            }

            if self.eof {
                if self.discarding {
                    self.discarding = false;
                    return Ok(Some(ReadEvent::Oversized));
                }
                if self.pending.is_empty() {
                    return Ok(None);
                }
                let mut line = std::mem::take(&mut self.pending);
                trim_carriage_return(&mut line);
                if line.iter().all(u8::is_ascii_whitespace) {
                    return Ok(None);
                }
                return Ok(Some(ReadEvent::Line(
                    String::from_utf8_lossy(&line).into_owned(),
                )));
            }

            if !self.discarding && self.pending.len() > MAX_LINE_BYTES {
                self.pending.clear();
                self.discarding = true;
            }

            let mut chunk = [0u8; 8192];
            let read = reader.read(&mut chunk).await?;
            if read == 0 {
                self.eof = true;
                continue;
            }
            if self.discarding {
                if let Some(index) = chunk[..read].iter().position(|byte| *byte == b'\n') {
                    self.discarding = false;
                    self.pending.extend_from_slice(&chunk[index + 1..read]);
                    return Ok(Some(ReadEvent::Oversized));
                }
            } else {
                self.pending.extend_from_slice(&chunk[..read]);
            }
        }
    }
}

fn trim_carriage_return(line: &mut Vec<u8>) {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_bounded_lines_and_rejects_oversized_ones() {
        let mut payload = vec![b'x'; MAX_LINE_BYTES + 8];
        payload.extend_from_slice(b"\n{\"ok\":true}\n");
        let (mut input, mut output) = tokio::io::duplex(payload.len() + 16);
        input.write_all(&payload).await.expect("write payload");
        input.shutdown().await.expect("close payload");

        let mut reader = LineReader::new();
        match reader.next(&mut output).await.expect("first event") {
            Some(ReadEvent::Oversized) => {}
            Some(ReadEvent::Line(line)) => panic!("expected oversized line, got {line}"),
            None => panic!("expected oversized line, got EOF"),
        }
        match reader.next(&mut output).await.expect("second event") {
            Some(ReadEvent::Line(line)) => assert_eq!(line, "{\"ok\":true}"),
            _ => panic!("expected the following line"),
        }
        assert!(reader.next(&mut output).await.expect("eof").is_none());
    }

    #[tokio::test]
    async fn accepts_a_final_line_without_newline() {
        let (mut input, mut output) = tokio::io::duplex(64);
        input.write_all(b"{\"ok\":true}").await.expect("write");
        input.shutdown().await.expect("close");

        let mut reader = LineReader::new();
        match reader.next(&mut output).await.expect("event") {
            Some(ReadEvent::Line(line)) => assert_eq!(line, "{\"ok\":true}"),
            _ => panic!("expected the final line"),
        }
        assert!(reader.next(&mut output).await.expect("eof").is_none());
    }
}
