use std::io::{BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
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
    Reasoning { reasoning: Vec<String>, text: Vec<String> },
    NativePwd,
}

pub struct ProviderFixture {
    pub address: SocketAddr,
    reply: Arc<Mutex<Reply>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    diagnostics: Arc<Mutex<Vec<String>>>,
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
                            if let Err(error) = serve(
                                stream,
                                &reply,
                                (Instant::now() + timeouts.http).min(deadline),
                            ) {
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
        })
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

fn serve(stream: TcpStream, reply: &Reply, deadline: Instant) -> Result<()> {
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
