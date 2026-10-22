//! The real executable with an isolated data directory and its loopback
//! Control Plane, driven over HTTP by this test as a local client: the
//! operator registers an LLM target with `ocg register-llm`, then the daemon
//! admits Jobs under idempotency keys, reports them and their outputs,
//! reconciles keys, stops a running Job, and answers the same keys after a
//! restart.
//!
//! No Provider is called. The targets are HTTPS endpoints on this machine
//! that never answer: one refuses every connection, so its Job fails before
//! anything is delivered; the other accepts connections and stays silent,
//! so its Job runs until it is stopped. No Job of this test can complete;
//! completion through a scripted Provider is covered by the in-crate hosted
//! tests, not here.

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
const TOKEN: &str = "C2-Acceptance-Token_0123456789.abcdefghij~";
const CREDENTIAL_REF: &str = "OCG_C2_ACCEPTANCE_KEY";

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

/// Accepts every connection and never reads or writes, until dropped.
struct Silent {
    port: u16,
    stopped: Arc<AtomicBool>,
    accepted: Arc<AtomicBool>,
}

impl Silent {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("non-blocking");
        let port = listener.local_addr().expect("address").port();
        let stopped = Arc::new(AtomicBool::new(false));
        let stopping = stopped.clone();
        let accepted = Arc::new(AtomicBool::new(false));
        let observed = accepted.clone();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while !stopping.load(Ordering::SeqCst) {
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
            stopped,
            accepted,
        }
    }
}

impl Drop for Silent {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
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

    /// Stops it with a graceful termination signal and waits for it.
    fn stop(mut self) {
        let signalled = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("send SIGTERM");
        assert!(signalled.success());
        let deadline = Instant::now() + WAIT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("poll the child") {
                break status;
            }
            assert!(Instant::now() < deadline, "the daemon did not exit");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "the daemon exited with {status}");
        if let Some(reader) = self.reader.take() {
            reader.join().expect("stderr reader");
        }
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

struct Answer {
    status: u16,
    body: Value,
}

fn request(address: SocketAddr, line: &str, key: Option<&str>, body: Option<&str>) -> Answer {
    let mut stream = TcpStream::connect(address).expect("connect");
    stream.set_read_timeout(Some(WAIT)).expect("timeout");
    let mut head = format!(
        "{line} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n",
        address.port()
    );
    if let Some(key) = key {
        head.push_str(&format!("Idempotency-Key: {key}\r\n"));
    }
    if let Some(body) = body {
        head.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    head.push_str("\r\n");
    head.push_str(body.unwrap_or(""));
    stream.write_all(head.as_bytes()).expect("send");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read");
    let (head, body) = raw.split_once("\r\n\r\n").expect("a head");
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status");
    assert!(!body.contains(TOKEN), "the token is never reflected");
    Answer {
        status,
        body: serde_json::from_str(body).expect("a JSON body"),
    }
}

fn submit(address: SocketAddr, key: &str, body: &Value) -> Answer {
    request(
        address,
        "POST /api/v1/jobs",
        Some(key),
        Some(&body.to_string()),
    )
}

fn lookup(address: SocketAddr, key: &str) -> Answer {
    request(address, "GET /api/v1/admissions", Some(key), None)
}

fn job(address: SocketAddr, job: &str) -> Value {
    let answer = request(address, &format!("GET /api/v1/jobs/{job}"), None, None);
    assert_eq!(answer.status, 200, "{}", answer.body);
    answer.body["job"].clone()
}

fn admitted(answer: &Answer) -> String {
    assert_eq!(
        answer.body["admission"]["status"], "admitted",
        "{}",
        answer.body
    );
    answer.body["admission"]["job"]
        .as_str()
        .expect("a Job")
        .to_owned()
}

fn until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "not reached in time");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn the_executable_admits_reconciles_and_stops_jobs_over_http() {
    let root = std::env::temp_dir().join(format!("ocg-c2-acceptance-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");
    let silent = Silent::start();
    let refusing = format!("https://127.0.0.1:{}/v1", closed_port());
    let stalling = format!("https://127.0.0.1:{}/v1", silent.port);

    // The operator registers the targets; registering again reuses them.
    let (executor, refused_llm) = register(&root, &refusing);
    let (same_executor, stalled_llm) = register(&root, &stalling);
    assert_eq!(executor, same_executor, "one Executor serves both");
    assert_ne!(refused_llm, stalled_llm);
    assert_eq!(
        register(&root, &refusing),
        (executor.clone(), refused_llm.clone())
    );

    let port = closed_port();
    let daemon = Daemon::start(&root, port);
    let address = daemon.address;
    assert_eq!(address.port(), port);
    let health = request(address, "GET /api/v1/health", None, None);
    assert_eq!(health.body["database"]["accepting_commands"], true);

    // A Job whose Provider refuses the connection: admitted, then failed
    // before anything was delivered, with its failure as its result.
    let failing_key = "acceptance-failing-0001";
    let failing_body = json!({
        "llm_profile": refused_llm,
        "executor": executor,
        "objective": "Say hello",
        "name": "acceptance failing",
    });
    let created = submit(address, failing_key, &failing_body);
    assert_eq!(created.status, 201, "{}", created.body);
    let failing = admitted(&created);
    until(|| job(address, &failing)["settlement"]["state"] != "unsettled");
    let record = job(address, &failing);
    assert_eq!(record["settlement"]["state"], "failed");
    assert_eq!(record["name"], "acceptance failing");
    let outputs = request(
        address,
        &format!("GET /api/v1/jobs/{failing}/outputs"),
        None,
        None,
    );
    let outputs = outputs.body["outputs"].as_array().expect("outputs").clone();
    assert_eq!(outputs.last().expect("a result")["kind"], "result");
    let tools = request(
        address,
        &format!("GET /api/v1/jobs/{failing}/tools"),
        None,
        None,
    );
    assert_eq!(tools.status, 200, "{}", tools.body);
    assert_eq!(tools.body["tools"], json!([]));
    assert_eq!(tools.body["has_more"], false);
    let bad_tools = request(
        address,
        &format!("GET /api/v1/jobs/{failing}/tools?limit=0"),
        None,
        None,
    );
    assert_eq!(bad_tools.status, 400);
    let responses = request(
        address,
        &format!("GET /api/v1/jobs/{failing}/responses"),
        None,
        None,
    );
    assert_eq!(responses.status, 200);
    assert_eq!(responses.body["responses"], json!([]));
    assert_eq!(responses.body["has_more"], false);
    assert_eq!(responses.body["next_after"], Value::Null);
    assert_eq!(
        request(
            address,
            &format!("GET /api/v1/jobs/{failing}/responses?after=0"),
            None,
            None
        )
        .status,
        400
    );
    assert_eq!(
        request(address, "GET /api/v1/jobs/job:999999/responses", None, None).status,
        404
    );
    // The same key and request name the same Job; another request is refused.
    let replay = submit(address, failing_key, &failing_body);
    assert_eq!((replay.status, admitted(&replay)), (200, failing.clone()));
    let mut other = failing_body.clone();
    other["objective"] = json!("Say goodbye");
    let conflict = submit(address, failing_key, &other);
    assert_eq!(conflict.status, 409);
    assert_eq!(conflict.body["error"]["code"], "idempotency_key_conflict");
    assert_eq!(admitted(&lookup(address, failing_key)), failing);
    let unknown = lookup(address, "acceptance-never-used-01");
    assert_eq!(unknown.body["admission"]["status"], "not_found");

    // A Job whose Provider never answers runs until it is stopped; its stop
    // is requested, and its record says how it settled.
    let running_key = "acceptance-running-0001";
    let running_body = json!({
        "llm_profile": stalled_llm,
        "executor": executor,
        "objective": "Wait for an answer",
        "name": "acceptance running",
    });
    let running = admitted(&submit(address, running_key, &running_body));
    assert_eq!(job(address, &running)["settlement"]["state"], "unsettled");
    until(|| silent.accepted.load(Ordering::SeqCst));
    let stop = request(
        address,
        &format!(
            "POST /api/v1/conversations/{}/stop",
            job(address, &running)["conversation"]
                .as_str()
                .expect("conversation")
        ),
        None,
        Some("{}"),
    );
    assert_eq!(stop.status, 202, "{}", stop.body);
    assert_eq!(stop.body["status"], "requested");
    until(|| !job(address, &running)["stop_confirmed_at_us"].is_null());
    let record = job(address, &running);
    assert_eq!(
        record["settlement"]["state"], "unknown",
        "a stopped request is not known to have failed, and is never Cancelled"
    );
    let again = request(
        address,
        &format!(
            "POST /api/v1/conversations/{}/stop",
            job(address, &running)["conversation"]
                .as_str()
                .expect("conversation")
        ),
        None,
        Some("{}"),
    );
    assert_eq!(again.body["status"], "already_settled");
    daemon.stop();

    // After a restart, the keys still name their Jobs, and replays run
    // nothing.
    let daemon = Daemon::start(&root, port);
    let address = daemon.address;
    assert_eq!(admitted(&lookup(address, failing_key)), failing);
    assert_eq!(admitted(&lookup(address, running_key)), running);
    let replay = submit(address, running_key, &running_body);
    assert_eq!((replay.status, admitted(&replay)), (200, running.clone()));
    assert_eq!(job(address, &running)["settlement"]["state"], "unknown");
    let stop = request(
        address,
        &format!(
            "POST /api/v1/conversations/{}/stop",
            job(address, &running)["conversation"]
                .as_str()
                .expect("conversation")
        ),
        None,
        Some("{}"),
    );
    assert_eq!(stop.body["status"], "already_settled");
    // Jobs are numbered in order: none after it repeats either submission.
    let last = running[4..].parse::<u64>().expect("a number");
    for later in last + 1..last + 32 {
        let answer = request(
            address,
            &format!("GET /api/v1/jobs/job:{later}"),
            None,
            None,
        );
        if answer.status == 200 {
            let name = &answer.body["job"]["name"];
            assert!(name != "acceptance running" && name != "acceptance failing");
        }
    }
    daemon.stop();
    drop(silent);
    std::fs::remove_dir_all(&root).ok();
}
