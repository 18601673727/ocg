use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use serde_json::{json, Value};

use super::client::{line, DeadlineSocket};
use super::harness::{Result, Timeouts, BODY_LIMIT};

pub const API_KEY: &str = "canonical-smoke-credential";
pub const MODEL: &str = "mock-model";

#[derive(Clone)]
pub enum Reply {
    Text(Vec<String>),
    /// Reasoning deltas followed by the user-visible answer.
    Reasoning {
        reasoning: Vec<String>,
        text: Vec<String>,
    },
    NativePwd,
    /// One delta, then the response stays open until OCG closes it.
    Hang,
    /// One delta, then these once `released` is set.
    Gated {
        released: Arc<AtomicBool>,
        rest: Vec<String>,
    },
}

pub struct ProviderFixture {
    pub address: SocketAddr,
    reply: Arc<Mutex<Reply>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    diagnostics: Arc<Mutex<Vec<String>>>,
    completions: Arc<AtomicUsize>,
}

impl ProviderFixture {
    pub fn start(reply: Reply, timeouts: Timeouts, deadline: Instant) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let reply = Arc::new(Mutex::new(reply));
        let worker_reply = Arc::clone(&reply);
        let worker_stop = Arc::clone(&stop);
        let diagnostics = Arc::new(Mutex::new(Vec::new()));
        let worker_diagnostics = Arc::clone(&diagnostics);
        let completions = Arc::new(AtomicUsize::new(0));
        let worker_completions = Arc::clone(&completions);
        let worker = thread::Builder::new()
            .name("smoke-provider".into())
            .spawn(move || {
                while !worker_stop.load(Ordering::SeqCst) && Instant::now() < deadline {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let reply = match worker_reply.lock() {
                                Ok(reply) => reply.clone(),
                                Err(error) => {
                                    eprintln!("provider reply: {error}");
                                    break;
                                }
                            };
                            // A hanging reply lasts until OCG closes it, never
                            // past the scenario.
                            let expires = if matches!(reply, Reply::Hang | Reply::Gated { .. }) {
                                deadline
                            } else {
                                (Instant::now() + timeouts.http).min(deadline)
                            };
                            if let Err(error) =
                                serve(stream, &reply, expires, &worker_completions, timeouts)
                            {
                                match worker_diagnostics.lock() {
                                    Ok(mut diagnostics) if diagnostics.len() < 64 => {
                                        diagnostics.push(error.to_string())
                                    }
                                    Ok(_) => {}
                                    Err(error) => eprintln!("provider diagnostics: {error}"),
                                }
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(timeouts.poll)
                        }
                        Err(error) => {
                            eprintln!("smoke provider accept: {error}");
                            break;
                        }
                    }
                }
            })?;
        Ok(Self {
            address,
            reply,
            stop,
            worker: Some(worker),
            diagnostics,
            completions,
        })
    }

    /// How many chat completion requests the provider has received.
    pub fn completions(&self) -> usize {
        self.completions.load(Ordering::SeqCst)
    }

    pub fn set_reply(&mut self, reply: Reply) -> Result<()> {
        *self.reply.lock().map_err(|error| error.to_string())? = reply;
        Ok(())
    }

    pub fn diagnostics(&self) -> String {
        match self.diagnostics.lock() {
            Ok(diagnostics) => format!("{diagnostics:?}"),
            Err(error) => error.to_string(),
        }
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            // Each accepted connection expires at the shared HTTP deadline.
            worker.join().map_err(|_| "provider fixture panicked")?;
        }
        Ok(())
    }
}

impl Drop for ProviderFixture {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("smoke provider cleanup: {error}");
        }
    }
}

fn chunk(delta: Value, finish: Value) -> Value {
    json!({
        "id": "chatcmpl-smoke", "object": "chat.completion.chunk",
        "created": 1700000000, "model": MODEL,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}], "usage": null
    })
}

fn serve(
    stream: TcpStream,
    reply: &Reply,
    deadline: Instant,
    completions: &AtomicUsize,
    timeouts: Timeouts,
) -> Result<()> {
    // Some platforms hand an accepted socket the listener's non-blocking mode.
    stream.set_nonblocking(false)?;
    let mut reader = BufReader::new(DeadlineSocket::new(stream, deadline));
    let request = line(&mut reader, 8192)?.ok_or("provider request closed")?;
    let mut length = 0;
    let mut authorized = false;
    let mut header_bytes = 0;
    loop {
        let header = line(&mut reader, 8192)?.ok_or("provider headers closed")?;
        header_bytes += header.len();
        if header_bytes > 65536 {
            return Err("provider headers exceed limit".into());
        }
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse()?;
            }
            if name.eq_ignore_ascii_case("authorization") {
                authorized = value.trim() == format!("Bearer {API_KEY}");
            }
        }
    }
    if length > BODY_LIMIT {
        return Err("provider request exceeds limit".into());
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    if !authorized {
        return Err("provider did not receive the configured Vault credential".into());
    }
    let (content_type, payload) = if request == "GET /v1/models HTTP/1.1" {
        ("application/json", json!({"object": "list", "data": [{"id": MODEL, "object": "model", "owned_by": "smoke"}]}).to_string())
    } else if request == "POST /v1/chat/completions HTTP/1.1" {
        let body: Value = serde_json::from_slice(&bytes)?;
        if body["model"] != MODEL || body["stream"] != true {
            return Err(format!("unexpected provider request: {body}").into());
        }
        completions.fetch_add(1, Ordering::SeqCst);
        match reply {
            Reply::Hang => return hang(reader.into_inner(), timeouts),
            Reply::Gated { released, rest } => {
                return gated(reader.into_inner(), released, rest, timeouts)
            }
            _ => {}
        }
        let has_tool = body["messages"]
            .as_array()
            .is_some_and(|messages| messages.iter().any(|message| message["role"] == "tool"));
        let events = match reply {
            Reply::Text(parts) => parts
                .iter()
                .map(|part| chunk(json!({"content": part}), Value::Null))
                .chain([chunk(json!({}), json!("stop"))])
                .collect::<Vec<_>>(),
            Reply::Reasoning { reasoning, text } => reasoning
                .iter()
                .map(|part| chunk(json!({"reasoning_content": part}), Value::Null))
                .chain(
                    text.iter()
                        .map(|part| chunk(json!({"content": part}), Value::Null)),
                )
                .chain([chunk(json!({}), json!("stop"))])
                .collect::<Vec<_>>(),
            Reply::NativePwd if has_tool => vec![
                chunk(
                    json!({"role": "assistant", "content": "mock done"}),
                    Value::Null,
                ),
                chunk(json!({}), json!("stop")),
            ],
            Reply::Hang | Reply::Gated { .. } => {
                unreachable!("an open-ended reply is served on its own")
            }
            Reply::NativePwd => vec![
                chunk(
                    json!({"role": "assistant", "tool_calls": [{
                        "index": 0, "id": "call_smoke_pwd", "type": "function",
                        "function": {"name": "process_exec", "arguments": "{\"program\":\"pwd\",\"args\":[]}"}
                    }]}),
                    Value::Null,
                ),
                chunk(json!({}), json!("tool_calls")),
            ],
        };
        let payload = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>()
            + "data: [DONE]\n\n";
        ("text/event-stream", payload)
    } else {
        return Err(format!("unexpected provider route: {request}").into());
    };
    let mut socket = reader.into_inner();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload.len()).as_bytes())?;
    // Fragment even UTF-8 characters and SSE boundaries to exercise partial reads.
    for bytes in payload.as_bytes().chunks(7) {
        socket.write_all(bytes)?;
    }
    socket.flush()?;
    Ok(())
}

fn write_chunk(socket: &mut DeadlineSocket, data: &str) -> std::io::Result<()> {
    socket.write_all(format!("{:x}\r\n{data}\r\n", data.len()).as_bytes())?;
    socket.flush()
}

/// Begin a streamed reply with one delta.
fn open_reply(socket: &mut DeadlineSocket) -> std::io::Result<()> {
    socket.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    )?;
    write_chunk(
        socket,
        &format!(
            "data: {}\n\n",
            chunk(json!({"content": "partial "}), Value::Null)
        ),
    )
}

/// Start a streamed reply and keep it open until OCG closes the connection,
/// which a write then reports.
fn hang(mut socket: DeadlineSocket, timeouts: Timeouts) -> Result<()> {
    open_reply(&mut socket)?;
    loop {
        thread::sleep(timeouts.poll);
        if write_chunk(&mut socket, ": waiting\n\n").is_err() {
            return Ok(());
        }
    }
}

/// Start a streamed reply, then finish it once the test releases it.
fn gated(
    mut socket: DeadlineSocket,
    released: &AtomicBool,
    rest: &[String],
    timeouts: Timeouts,
) -> Result<()> {
    open_reply(&mut socket)?;
    while !released.load(Ordering::SeqCst) {
        thread::sleep(timeouts.poll);
        write_chunk(&mut socket, ": waiting\n\n")?;
    }
    for part in rest {
        write_chunk(
            &mut socket,
            &format!("data: {}\n\n", chunk(json!({"content": part}), Value::Null)),
        )?;
    }
    write_chunk(
        &mut socket,
        &format!(
            "data: {}\n\ndata: [DONE]\n\n",
            chunk(json!({}), json!("stop"))
        ),
    )?;
    socket.write_all(b"0\r\n\r\n")?;
    socket.flush()?;
    Ok(())
}
