//! The real executable with an isolated data directory and its loopback
//! Control Plane, read by this test as a local client of its event streams:
//! a Job's outputs arrive on an open stream as the daemon stores them, are
//! replayed from the database, by another process too, and resume after a
//! cursor; a stopped Job's stream ends with its recorded settlement; and a
//! daemon stopped by SIGTERM while a stream is open exits and frees its port.
//!
//! No Provider is called. The targets are HTTPS endpoints on this machine:
//! one holds every connection, silent, until the test lets it go, and then
//! refuses, so its Job fails while a client listens; the other stays silent,
//! so its Job runs until it is stopped. Streams of Jobs completed through a
//! scripted Provider are covered by the in-crate tests, not here.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(30);
const TOKEN: &str = "C3-Acceptance-Token_0123456789.abcdefghij~";
const CREDENTIAL_REF: &str = "OCG_C3_ACCEPTANCE_KEY";

fn ocg(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .env("OCG_DATA_DIR", root)
        .env(CREDENTIAL_REF, "not-a-real-key")
        .env_remove("OCG_CONTROL_PORT")
        .env_remove("OCG_CONTROL_TOKEN")
        .stdin(Stdio::null());
    command
}

/// `ocg register-llm` for `endpoint`: the Executor and LLM Profile it
/// printed.
fn register(root: &Path, endpoint: &str) -> (String, String) {
    let output = ocg(root)
        .arg("register-llm")
        .env("OCG_PROVIDER_ENDPOINT", endpoint)
        .env("OCG_PROVIDER_MODEL", "acceptance-model")
        .env("OCG_PROVIDER_CREDENTIAL_REF", CREDENTIAL_REF)
        .output()
        .expect("run register-llm");
    assert!(
        output.status.success(),
        "register-llm failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed: Value = serde_json::from_slice(&output.stdout).expect("one JSON object");
    let text = |field: &str| printed[field].as_str().expect("an identity").to_owned();
    (text("executor"), text("llm_profile"))
}

/// A port nothing listens on once this returns.
fn closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
        .port()
}

/// Accepts every connection and never reads or writes; once released, it
/// closes them and stops listening, so every later connection is refused.
struct Held {
    port: u16,
    released: Arc<AtomicBool>,
    accepted: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Held {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("non-blocking");
        let port = listener.local_addr().expect("address").port();
        let released = Arc::new(AtomicBool::new(false));
        let releasing = released.clone();
        let accepted = Arc::new(AtomicBool::new(false));
        let observed = accepted.clone();
        let thread = std::thread::spawn(move || {
            let mut held = Vec::new();
            while !releasing.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        observed.store(true, Ordering::SeqCst);
                        held.push(stream);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        Self {
            port,
            released,
            accepted,
            thread: Some(thread),
        }
    }

    fn release(&mut self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("the held listener stopped");
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.release();
    }
}

/// The daemon with its Control Plane on `port`.
struct Daemon {
    child: Child,
    address: SocketAddr,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl Daemon {
    fn start(root: &Path, port: u16) -> Self {
        let mut child = ocg(root)
            .env("OCG_CONTROL_PORT", port.to_string())
            .env("OCG_CONTROL_TOKEN", TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start the executable");
        let (lines, received) = flume::unbounded();
        let stderr = child.stderr.take().expect("stderr");
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                lines.send(line).ok();
            }
        });
        let deadline = Instant::now() + WAIT;
        let mut seen = Vec::new();
        let address = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Ok(line) = received.recv_timeout(remaining) else {
                child.kill().ok();
                panic!("the daemon did not start listening; it said {seen:?}");
            };
            assert!(!line.contains(TOKEN), "the token is never printed");
            seen.push(line.clone());
            if let Some(address) = line.strip_prefix("OCG control plane listening on http://") {
                break address.trim().parse().expect("an address");
            }
        };
        Self {
            child,
            address,
            reader: Some(reader),
        }
    }

    /// Stops it with a graceful termination signal, waits for it, and
    /// returns how long it took.
    fn stop(mut self) -> Duration {
        let started = Instant::now();
        let signalled = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGTERM");
        assert!(signalled.success());
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                break status;
            }
            assert!(started.elapsed() < WAIT, "the daemon did not exit");
            std::thread::sleep(Duration::from_millis(20));
        };
        let took = started.elapsed();
        assert!(status.success(), "the daemon exited with {status}");
        if let Some(reader) = self.reader.take() {
            reader.join().expect("stderr reader");
        }
        took
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if self.reader.is_some() {
            self.child.kill().ok();
            self.child.wait().ok();
        }
    }
}

/// Sends one request and returns the connection, past the response's
/// head, with its status and headers.
fn send(
    address: SocketAddr,
    line: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
) -> (u16, Vec<String>, BufReader<TcpStream>) {
    let stream = TcpStream::connect(address).expect("connect");
    stream.set_read_timeout(Some(WAIT)).expect("timeout");
    let mut head = format!(
        "{line} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n",
        address.port()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    head.push_str("\r\n");
    head.push_str(body.unwrap_or(""));
    (&stream).write_all(head.as_bytes()).expect("send");
    let mut reader = BufReader::new(stream);
    let mut lines = Vec::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("a head");
        let line = line.trim_end().to_owned();
        if line.is_empty() {
            break;
        }
        lines.push(line);
    }
    let status = lines[0]
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status");
    (status, lines[1..].to_vec(), reader)
}

fn request(address: SocketAddr, line: &str, key: Option<&str>, body: Option<&str>) -> Value {
    let headers = key
        .map(|key| vec![("Idempotency-Key", key)])
        .unwrap_or_default();
    let (_, _, mut reader) = send(address, line, &headers, body);
    let mut body = String::new();
    reader.read_to_string(&mut body).expect("read");
    assert!(!body.contains(TOKEN), "the token is never reflected");
    serde_json::from_str(&body).expect("a JSON body")
}

fn admit(address: SocketAddr, key: &str, executor: &str, llm: &str) -> String {
    let body = json!({
        "llm_profile": llm,
        "executor": executor,
        "objective": "Say hello",
        "name": key,
    })
    .to_string();
    let answer = request(address, "POST /api/v1/jobs", Some(key), Some(&body));
    assert_eq!(answer["admission"]["status"], "admitted", "{answer}");
    answer["admission"]["job"]
        .as_str()
        .expect("a Job")
        .to_owned()
}

fn job(address: SocketAddr, job: &str) -> Value {
    request(address, &format!("GET /api/v1/jobs/{job}"), None, None)["job"].clone()
}

fn outputs(address: SocketAddr, job: &str) -> Vec<Value> {
    request(
        address,
        &format!("GET /api/v1/jobs/{job}/outputs?limit=100"),
        None,
        None,
    )["outputs"]
        .as_array()
        .expect("outputs")
        .clone()
}

/// An event: its id, name and JSON data.
#[derive(Debug)]
struct Event {
    id: Option<String>,
    name: String,
    data: Value,
}

/// An event stream, read as it arrives.
struct Events {
    reader: BufReader<TcpStream>,
    text: String,
}

fn events(address: SocketAddr, job: &str, last: Option<&str>) -> Events {
    let headers = last
        .map(|last| vec![("Last-Event-ID", last)])
        .unwrap_or_default();
    let (status, head, reader) = send(
        address,
        &format!("GET /api/v1/jobs/{job}/events"),
        &headers,
        None,
    );
    assert_eq!(status, 200, "{head:?}");
    assert!(
        head.iter()
            .any(|line| line.eq_ignore_ascii_case("content-type: text/event-stream; charset=utf-8")),
        "{head:?}"
    );
    Events {
        reader,
        text: String::new(),
    }
}

impl Events {
    /// The next event, past heartbeats; `None` at the end of the stream.
    fn next(&mut self) -> Option<Event> {
        loop {
            while let Some(end) = self.text.find("\n\n") {
                let block = self.text[..end].to_owned();
                self.text.drain(..end + 2);
                if block.starts_with(':') {
                    continue;
                }
                let (mut id, mut name, mut data) = (None, String::new(), Value::Null);
                for line in block.lines() {
                    match line.split_once(": ") {
                        Some(("id", value)) => id = Some(value.to_owned()),
                        Some(("event", value)) => name = value.to_owned(),
                        Some(("data", value)) => {
                            data = serde_json::from_str(value).expect("JSON data");
                        }
                        _ => panic!("an unexpected line {line:?}"),
                    }
                }
                return Some(Event { id, name, data });
            }
            // One chunk of the chunked body.
            let mut size = String::new();
            if self.reader.read_line(&mut size).expect("a chunk size") == 0 {
                return None;
            }
            let size = usize::from_str_radix(size.trim_end(), 16).expect("a chunk size");
            let mut chunk = vec![0; size + 2];
            self.reader.read_exact(&mut chunk).expect("a chunk");
            if size == 0 {
                return None;
            }
            self.text
                .push_str(std::str::from_utf8(&chunk[..size]).expect("UTF-8"));
        }
    }

    fn event(&mut self) -> Event {
        self.next().expect("an event")
    }

    /// The outputs to the settlement, and the settlement.
    fn until_settled(&mut self) -> (Vec<Value>, Value) {
        let mut delivered = Vec::new();
        loop {
            let event = self.event();
            match event.name.as_str() {
                "output" => {
                    let output = event.data["output"].clone();
                    assert_eq!(event.id, Some(output["seq"].to_string()));
                    delivered.push(output);
                }
                "job" => assert_eq!(event.id, None),
                "settlement" => {
                    assert_eq!(event.id, None);
                    return (delivered, event.data);
                }
                other => panic!("an unexpected event {other}: {event:?}"),
            }
        }
    }
}

#[test]
fn the_executable_streams_job_outputs_live_and_replays_them() {
    let root = std::env::temp_dir().join(format!("ocg-c3-acceptance-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");
    let mut held = Held::start();
    let silent = Held::start();
    let (executor, held_llm) = register(&root, &format!("https://127.0.0.1:{}/v1", held.port));
    let (_, silent_llm) = register(&root, &format!("https://127.0.0.1:{}/v1", silent.port));

    let port = closed_port();
    let daemon = Daemon::start(&root, port);
    let address = daemon.address;

    // Live: the client listens before the Job has any output; the Job
    // fails once its Provider lets go, and its result and settlement
    // arrive on the same stream.
    let failing = admit(address, "c3-acceptance-failing-01", &executor, &held_llm);
    let mut live = events(address, &failing, None);
    let first = live.event();
    assert_eq!((first.name.as_str(), &first.id), ("job", &None));
    assert_eq!(first.data["job"]["settlement"]["state"], "unsettled");
    held.release();
    let (delivered, settlement) = live.until_settled();
    assert_eq!(settlement["settlement"]["state"], "failed");
    assert!(
        live.next().is_none(),
        "the stream ends after its settlement"
    );
    let stored = outputs(address, &failing);
    assert_eq!(delivered, stored, "every stored output, as stored");
    assert_eq!(stored.last().expect("a result")["kind"], "result");

    // Replayed from the start, and resumed after the last output.
    let mut replay = events(address, &failing, None);
    assert_eq!(replay.event().data["job"], job(address, &failing));
    assert_eq!(replay.until_settled(), (stored.clone(), settlement.clone()));
    let last = stored.last().expect("a result")["seq"].to_string();
    let mut resumed = events(address, &failing, Some(&last));
    resumed.event();
    assert_eq!(resumed.until_settled(), (Vec::new(), settlement.clone()));

    // A stopped Job: its stream ends with the settlement its record holds,
    // unknown with its stop confirmed, and no result it never had.
    let running = admit(address, "c3-acceptance-running-01", &executor, &silent_llm);
    let mut stopping = events(address, &running, None);
    stopping.event();
    let deadline = Instant::now() + WAIT;
    while !silent.accepted.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the Job never reached its Provider"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let stop = request(
        address,
        &format!("POST /api/v1/jobs/{running}/stop"),
        None,
        None,
    );
    assert_eq!(stop["stop"]["status"], "requested", "{stop}");
    let (delivered, stopped) = stopping.until_settled();
    assert_eq!(stopped["settlement"]["state"], "unknown");
    assert!(stopped["stop_confirmed_at_us"].is_i64());
    assert!(delivered.iter().all(|output| output["kind"] != "result"));

    // SIGTERM while a client listens: the stream ends, the daemon exits
    // within its bounds and its port is free.
    let waiting = admit(address, "c3-acceptance-waiting-01", &executor, &silent_llm);
    let mut open = events(address, &waiting, None);
    open.event();
    let took = daemon.stop();
    assert!(
        took < Duration::from_secs(15),
        "the daemon took {took:?} to exit"
    );
    while let Some(event) = open.next() {
        assert_ne!(event.name, "settlement", "no settlement it did not observe");
    }
    TcpListener::bind(("127.0.0.1", port)).expect("the port was released");

    // Another process replays the same history from the database alone.
    let daemon = Daemon::start(&root, port);
    let address = daemon.address;
    let mut again = events(address, &failing, Some("0"));
    again.event();
    assert_eq!(again.until_settled(), (stored, settlement));
    daemon.stop();
    drop(silent);
    std::fs::remove_dir_all(&root).ok();
}
