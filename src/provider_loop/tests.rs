use super::*;
use crate::contracts::JobLaunchRequest;
use crate::http::NativeHttp;
use crate::orchestration::canonical_control::CanonicalControlService;
use crate::orchestration::domain::{Call, JobState};
use crate::orchestration::execution_dispatch::{ExecutionEvent, ProviderExecutionConfig};
use crate::orchestration::execution_runtime::{ExecutionRuntime, ExecutionRuntimeHandle};
use crate::orchestration::journal::EventKind;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(5);

/// One scripted response from the provider stub, for one physical HTTP attempt.
///
/// The sequence is indexed by physical attempt, not by logical round, so a
/// fixture can say "fail this way twice, then answer normally" and assert the
/// exact number of requests the bounded retry produced.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StubStep {
    /// A non-2xx response with a small body.
    Status(u16),
    /// A non-2xx response carrying a `Retry-After` cooldown, with its seconds.
    Throttled(u16, u64),
    /// A complete SSE stream: these deltas, then `[DONE]`.
    Stream(&'static [&'static str]),
    /// An SSE stream that stops mid-flight: these deltas are delivered, then the
    /// connection is closed without `[DONE]`, which is a premature EOF.
    TruncatedStream(&'static [&'static str]),
    /// An SSE stream that stops mid-flight, carrying these already-encoded SSE
    /// frames verbatim. Needed when the wire shape under test is not an ordinary
    /// text delta, such as a real OpenAI `tool_calls` fragment.
    TruncatedFrames(&'static [&'static str]),
}

/// The scripted attempts for one label, and how many have been served.
type StubScript = Vec<StubStep>;

fn sse_chunk(content: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"choices":[{"index":0,"delta":{"content":content},"finish_reason":null}]})
    )
}

/// One OpenAI-compatible `tool_calls` SSE frame: the call's id and function name
/// plus an argument fragment, exactly as a real provider streams them.
fn sse_tool_call(id: &str, name: &str, arguments: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":id,
            "function":{"name":name,"arguments":arguments}}]},"finish_reason":null}]})
    )
}

/// The real tool-call frames a discarded physical attempt streams.
///
/// A tool that exists in the Native Tool registry, sent as the two argument
/// fragments providers actually split it into, which concatenate into complete,
/// valid arguments `{"path":"."}`. Built once through the same encoder the rest
/// of the stub uses, then leaked for the process lifetime because a scripted
/// step holds `&'static [&'static str]`.
fn discarded_tool_frames() -> &'static [&'static str] {
    static FRAMES: std::sync::OnceLock<[&'static str; 2]> = std::sync::OnceLock::new();
    FRAMES
        .get_or_init(|| {
            [
                Box::leak(
                    sse_tool_call("call-discarded", "filesystem_list", "{\"path\":\"")
                        .into_boxed_str(),
                ),
                Box::leak(
                    sse_tool_call("call-discarded", "filesystem_list", ".\"}")
                        .into_boxed_str(),
                ),
            ]
        })
        .as_slice()
}

fn sse_final(content: &str) -> String {
    format!(
        "data: {}\n\n",
        json!({"choices":[{"index":0,"delta":{"content":content},"finish_reason":"stop"}]})
    )
}

struct ProviderStub {
    endpoint: String,
    requests: flume::Receiver<String>,
    closed: flume::Receiver<String>,
    /// Physical requests served per label, in order. This is the observable the
    /// bounded retry is asserted against.
    served: Arc<Mutex<Vec<String>>>,
    scripts: Arc<Mutex<std::collections::HashMap<String, StubScript>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    handlers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl ProviderStub {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let address = listener.local_addr().expect("stub address");
        listener.set_nonblocking(true).expect("nonblocking accept");
        let (requests, received) = flume::unbounded();
        let (closed, closures) = flume::unbounded();
        let served: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let scripts: Arc<Mutex<std::collections::HashMap<String, StubScript>>> =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let shutdown = stop.clone();
        let handlers = Arc::new(Mutex::new(Vec::new()));
        let connection_handlers = Arc::clone(&handlers);
        let connection_served = Arc::clone(&served);
        let connection_scripts = Arc::clone(&scripts);
        let thread = thread::spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let requests = requests.clone();
                        let closed = closed.clone();
                        let served = Arc::clone(&connection_served);
                        let scripts = Arc::clone(&connection_scripts);
                        let handler = thread::spawn(move || {
                            let mut stream = stream;
                            stream
                                .set_nonblocking(false)
                                .expect("blocking fixture socket");
                            stream.set_read_timeout(Some(WAIT)).expect("read timeout");
                            stream.set_write_timeout(Some(WAIT)).expect("write timeout");
                            let mut reader =
                                BufReader::new(stream.try_clone().expect("clone socket"));
                            let mut length = 0;
                            loop {
                                let mut line = String::new();
                                reader.read_line(&mut line).expect("request headers");
                                if line == "\r\n" || line.is_empty() {
                                    break;
                                }
                                if let Some((key, value)) = line.split_once(':') {
                                    if key.eq_ignore_ascii_case("content-length") {
                                        length = value.trim().parse().expect("content length");
                                    }
                                }
                            }
                            let mut body = vec![0; length];
                            reader.read_exact(&mut body).expect("request body");
                            let body: Value = serde_json::from_slice(&body).expect("provider JSON");
                            let label = body["messages"]
                                .as_array()
                                .expect("messages")
                                .iter()
                                .rev()
                                .find(|message| message["role"] == "user")
                                .and_then(|message| message["content"].as_str())
                                .expect("user objective")
                                .to_string();
                            requests.send(label.clone()).expect("request observer");
                            // The attempt index is decided before the response is
                            // written, so a scripted sequence is replayed by
                            // physical attempt and never by wall-clock timing.
                            let step = {
                                let mut served = served.lock().expect("served labels");
                                let attempt = served.iter().filter(|seen| **seen == label).count();
                                served.push(label.clone());
                                scripts
                                    .lock()
                                    .expect("stub scripts")
                                    .get(&label)
                                    .and_then(|script| {
                                        script
                                            .get(attempt)
                                            .or_else(|| script.last())
                                            .copied()
                                    })
                            };
                            if let Some(step) = step {
                                serve_step(step, &mut stream);
                                closed.send(label).expect("closure observer");
                                return;
                            }
                            if label == "http500" {
                                stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 7\r\nConnection: close\r\n\r\nfailure").expect("error response");
                            } else if label.starts_with("silent") {
                                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").expect("stream head");
                                stream.flush().expect("flush head");
                                // No subsequent chunk can wake the client. Only its
                                // cancellation token can make this socket close.
                                let mut byte = [0];
                                assert_eq!(
                                    reader.read(&mut byte).expect("cancel closes socket"),
                                    0
                                );
                            } else {
                                if label == "slow-head" {
                                    thread::sleep(Duration::from_secs(31));
                                }
                                let tool_messages = body["messages"]
                                    .as_array()
                                    .expect("messages")
                                    .iter()
                                    .filter(|message| message["role"] == "tool")
                                    .collect::<Vec<_>>();
                                let chunk = |delta: Value, reason: Value| {
                                    let value = json!({"choices":[{"index":0,"delta":delta,"finish_reason":reason}]});
                                    format!("data: {value}\n\n")
                                };
                                let chunks = if label == "retry-empty" && tool_messages.len() < 6 {
                                    if let Some(message) = tool_messages.last() {
                                        let observation =
                                            message["content"].as_str().expect("repair");
                                        assert!(observation.contains("InvalidArguments"));
                                        assert!(observation.contains("path"));
                                        assert!(observation.contains("expected string"));
                                    }
                                    vec![chunk(
                                        json!({"tool_calls": [{"index": 0,
                                            "id": format!("retry-{}", tool_messages.len()),
                                            "function": {"name": "filesystem_list", "arguments": "{\"path\":42}"}
                                        }]}),
                                        json!("tool_calls"),
                                    )]
                                } else if matches!(
                                    label.as_str(),
                                    "tool-loop" | "fragmented-tool-loop"
                                ) && tool_messages.is_empty()
                                {
                                    let repeated = label == "fragmented-tool-loop";
                                    vec![
                                        chunk(
                                            json!({"tool_calls": [{"index": 0, "id": "native-list",
                                                "function": {"name": "filesystem_list", "arguments": "{\"path\":"}
                                            }]}),
                                            Value::Null,
                                        ),
                                        chunk(
                                            if repeated {
                                                json!({"tool_calls": [{"index": 0, "id": "native-list",
                                                    "function": {"name": "filesystem_list", "arguments": "\".\"}"}
                                                }]})
                                            } else {
                                                json!({"tool_calls": [{"index": 0, "function": {"arguments": "\".\"}"}}]})
                                            },
                                            json!("tool_calls"),
                                        ),
                                    ]
                                } else {
                                    if matches!(
                                        label.as_str(),
                                        "tool-loop" | "fragmented-tool-loop"
                                    ) {
                                        let content = tool_messages.last().expect("tool result")
                                            ["content"]
                                            .as_str()
                                            .expect("content");
                                        let result: Value = serde_json::from_str(content)
                                            .expect("canonical tool result");
                                        assert_eq!(result["success"], true);
                                    }
                                    [("Real ", Value::Null), ("reply", json!("stop"))]
                                        .into_iter()
                                        .map(|(content, reason)| {
                                            let delta = match label.as_str() {
                                                "empty-final" | "retry-empty" => {
                                                    json!({"reasoning_content": "Hidden reasoning"})
                                                }
                                                "blank-final" => json!({"content": " \n\t"}),
                                                _ => json!({"content": content}),
                                            };
                                            chunk(delta, reason)
                                        })
                                        .collect::<Vec<_>>()
                                };
                                let body = format!("{}data: [DONE]\n\n", chunks.concat());
                                write!(
                                    stream,
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                )
                                .expect("stream head");
                                stream.write_all(body.as_bytes()).expect("stream body");
                                stream.flush().expect("flush body");
                            }
                            closed.send(label).expect("closure observer");
                        });
                        connection_handlers
                            .lock()
                            .expect("stub handlers")
                            .push(handler);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("stub accept: {error}"),
                }
            }
        });
        Self {
            endpoint: format!("http://{address}/v1"),
            requests: received,
            closed: closures,
            served,
            scripts,
            stop,
            thread: Some(thread),
            handlers,
        }
    }

    /// Answer this label's physical attempts from `script`.
    fn script(&self, label: &str, script: StubScript) {
        self.scripts
            .lock()
            .expect("stub scripts")
            .insert(label.to_string(), script);
    }

    /// How many physical requests this label has received so far.
    fn attempts(&self, label: &str) -> usize {
        self.served
            .lock()
            .expect("served labels")
            .iter()
            .filter(|seen| *seen == label)
            .count()
    }

    /// How many physical responses the stub has finished writing. A settled round
    /// has closed every connection it opened, so this matches the attempts a
    /// completed fixture served.
    fn closures(&self) -> usize {
        self.served.lock().expect("served labels").len()
    }

    fn request(&self, expected: &str) {
        assert_eq!(
            self.requests.recv_timeout(WAIT).expect("upstream request"),
            expected
        );
    }

    fn closure(&self, expected: &str) {
        assert_eq!(
            self.closed.recv_timeout(WAIT).expect("upstream closure"),
            expected
        );
    }
}

/// Write one scripted response for a single physical attempt.
fn serve_step(step: StubStep, stream: &mut impl Write) {
    match step {
        StubStep::Status(status) => {
            let reason = match status {
                401 => "Unauthorized",
                408 => "Request Timeout",
                500 => "Internal Server Error",
                502 => "Bad Gateway",
                503 => "Service Unavailable",
                504 => "Gateway Timeout",
                529 => "Site Overloaded",
                other => panic!("unscripted status {other}"),
            };
            let body = format!("{reason}");
            write!(
                stream,
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("error response");
            stream.flush().expect("flush error response");
        }
        StubStep::Throttled(status, seconds) => {
            let body = "slow down";
            write!(
                stream,
                "HTTP/1.1 {status} Too Many Requests\r\nRetry-After: {seconds}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("throttled response");
            stream.flush().expect("flush throttled response");
        }
        StubStep::Stream(deltas) => {
            let body = format!("{}data: [DONE]\n\n", stream_body(deltas, true));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .expect("stream head");
            stream.write_all(body.as_bytes()).expect("stream body");
            stream.flush().expect("flush body");
        }
        StubStep::TruncatedFrames(frames) => {
            write_truncated_head(stream);
            let body = frames.concat();
            write!(stream, "{:x}\r\n{body}\r\n", body.len()).expect("raw chunk");
            stream.flush().expect("flush raw chunk");
            let _ = stream.flush();
        }
        StubStep::TruncatedStream(deltas) => {
            // A chunked head with no terminating chunk: the client has delivered
            // the deltas and then observes a premature EOF.
            write_truncated_head(stream);
            let body = stream_body(deltas, false);
            write!(stream, "{:x}\r\n{body}\r\n", body.len()).expect("truncated chunk");
            stream.flush().expect("flush truncated chunk");
            // Closing here is the transport interruption this step models.
            let _ = stream.flush();
        }
    }
}

/// A chunked SSE head with no terminating chunk: the client has received every
/// frame the stub sends and then observes a premature EOF when the socket closes.
fn write_truncated_head(stream: &mut impl Write) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    )
    .expect("truncated stream head");
}

fn stream_body(deltas: &'static [&'static str], terminal: bool) -> String {
    deltas
        .iter()
        .enumerate()
        .map(|(index, delta)| {
            if index + 1 == deltas.len() && terminal {
                sse_final(delta)
            } else {
                sse_chunk(delta)
            }
        })
        .collect()
}

impl Drop for ProviderStub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            if let Err(panic) = thread.join() {
                if !thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
        for handler in self.handlers.lock().expect("stub handlers").drain(..) {
            if let Err(panic) = handler.join() {
                if !thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }
}

struct Fixture {
    root: TempDir,
    domain: DomainRepository,
    handler: CanonicalProviderCallHandler,
    tools: BoundedDispatcher,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("project");
        let domain = DomainRepository::open(root.path()).expect("domain");
        let tools = BoundedDispatcher::new(8).expect("tool dispatcher");
        let handler = CanonicalProviderCallHandler::new(ProviderHandlerConfig {
            transport: Arc::new(NativeHttp::new().expect("native HTTP")),
            project_root: root.path().to_path_buf(),
            permission_policy: PermissionPolicy::allow_all(),
            cancelled: Arc::new(AtomicBool::new(false)),
            native_tool_dispatcher: tools.clone(),
            in_flight_limit: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
            governor: crate::orchestration::governor::Governor::new(),
        });
        Self {
            root,
            domain,
            handler,
            tools,
        }
    }

    fn admitted(
        &mut self,
        endpoint: &str,
        label: &str,
        economic: bool,
    ) -> (Call, ExecutionEnvelope, flume::Receiver<ExecutionEvent>) {
        let project = self
            .domain
            .ensure_project(self.root.path())
            .expect("Project");
        let job = self
            .domain
            .create_job(&project.id, &json!({"objective":label}).to_string())
            .expect("Job");
        let (attempt, executor) = self
            .domain
            .dispatch_job(&job.id, "provider")
            .expect("Attempt/Executor");
        let payload = json!({"executor_transport":"provider","arguments":{"model":"example","messages":[{"role":"user","content":label}]}}).to_string();
        let call = self
            .domain
            .create_call_with_effect(
                &attempt.id,
                Some(&executor.id),
                attempt.generation,
                EffectIntentKind::StrictFenced,
                &payload,
            )
            .expect("Call/Intent");
        self.domain
            .set_provider_config(
                &call.id,
                "local",
                "local/example",
                "example",
                endpoint,
                None,
            )
            .expect("freeze config");
        let budget = BudgetConfig {
            require_quota: !economic,
            ..BudgetConfig::default()
        };
        let assessment = self
            .domain
            .admit_dispatch(
                &project.id,
                attempt.generation,
                &call.id,
                &budget,
                QuotaFacts::unknown(),
            )
            .expect("economic admission");
        assert_eq!(assessment.is_allowed(), economic);
        self.domain
            .mark_dispatch_queued(&call.id)
            .expect("queue intent");
        let (events, receiver) = flume::unbounded();
        let envelope = ExecutionEnvelope {
            call_id: call.id.clone(),
            job_id: job.id,
            attempt_id: attempt.id,
            executor_id: Some(executor.id),
            generation: attempt.generation,
            payload,
            dispatch_id: None,
            events,
            provider_config: Some(ProviderExecutionConfig {
                provider_key: "local".into(),
                model: "local/example".into(),
                upstream_model_id: "example".into(),
                endpoint: endpoint.into(),
                credential_ref: None,
            }),
            cancelled: CallCancellation::new(),
        };
        (call, envelope, receiver)
    }

    fn execute(&self, envelope: ExecutionEnvelope) -> Result<()> {
        execute_provider_envelope_sync(&self.handler, envelope)
    }

    fn terminal(&self, call: &Call, state: &str) {
        let attempt = self
            .domain
            .attempt(&call.attempt_id)
            .expect("Attempt")
            .expect("Attempt exists");
        assert_eq!(attempt.state.to_string(), state);
        assert!(!attempt.authoritative);
        let job = self
            .domain
            .job(&attempt.job_id)
            .expect("Job")
            .expect("Job exists");
        assert_eq!(job.state.to_string(), state);
        assert!(job.authoritative_attempt_id.is_none());
        let events = self.domain.events_after(0, 4096).expect("journal");
        let root = events
            .iter()
            .find(|event| {
                event.kind == EventKind::AttemptUpdated
                    && event.entity_id == attempt.id
                    && event.payload["state"] == state
            })
            .expect("terminal Attempt root");
        assert!(root.caused_by_seq.is_none());
        let job_event = events
            .iter()
            .find(|event| {
                event.kind == EventKind::JobUpdated
                    && event.entity_id == job.id
                    && event.payload["state"] == state
            })
            .expect("Job settlement event");
        assert_eq!(job_event.caused_by_seq, Some(root.seq));
        for event in events.iter().filter(|event| {
            event.kind == EventKind::ExecutorUpdated
                && event.attempt_id.as_deref() == Some(attempt.id.as_str())
                && event.payload["state"] == state
        }) {
            assert_eq!(event.caused_by_seq, Some(root.seq));
        }
        assert!(self.tools.is_empty().expect("no tools"));
    }

    fn no_residue(&self) {
        assert_no_residue(&self.domain);
    }
}

fn assert_no_residue(domain: &DomainRepository) {
    let snapshot = domain.execution_snapshot().expect("residue snapshot");
    assert!(snapshot
        .jobs
        .iter()
        .all(|job| !matches!(job.state, JobState::Running | JobState::Cancelling)));
    assert!(snapshot
        .attempts
        .iter()
        .all(|attempt| !attempt.authoritative));
    assert!(snapshot
        .calls
        .iter()
        .all(|call| !matches!(call.state.as_str(), "created" | "running")));
    assert!(snapshot
        .dispatch_intents
        .iter()
        .all(|intent| !matches!(intent.state.as_str(), "pending" | "queued" | "running")));
}

#[test]
fn pre_execution_invariants_settle_current_authority_without_external_effects() {
    let upstream = ProviderStub::start();
    for scenario in [
        "missing_intent",
        "economic",
        "frozen_mismatch",
        "failed_call",
        "fenced_intent",
        "executor_missing",
        "invalid_payload",
        "missing_config",
        "missing_credential",
        "shutdown",
    ] {
        let mut fixture = Fixture::new();
        let (call, mut envelope, _events) =
            fixture.admitted(&upstream.endpoint, scenario, scenario != "economic");
        match scenario {
            "missing_intent" => {
                // No domain API can create this corruption; inject only the
                // precondition, never its expected lifecycle settlement.
                rusqlite::Connection::open(fixture.domain.path())
                    .expect("fixture DB")
                    .execute(
                        "DELETE FROM domain_dispatch_intents WHERE call_id=?1",
                        [&call.id],
                    )
                    .expect("remove intent");
            }
            "frozen_mismatch" => {
                envelope
                    .provider_config
                    .as_mut()
                    .expect("config")
                    .upstream_model_id = "different".into()
            }
            "failed_call" => {
                fixture
                    .domain
                    .fail_call(
                        &call.id,
                        &call.attempt_id,
                        call.generation,
                        "other actor failed Call",
                    )
                    .expect("fail Call only");
            }
            "fenced_intent" => fixture
                .domain
                .finish_dispatch_intent(
                    &call.id,
                    "fenced",
                    EffectIntentState::NotStarted,
                    Some("external fence"),
                )
                .expect("fence intent only"),
            "executor_missing" => {
                let database =
                    rusqlite::Connection::open(fixture.domain.path()).expect("fixture DB");
                database
                    .pragma_update(None, "foreign_keys", "OFF")
                    .expect("fault injection");
                database
                    .execute(
                        "DELETE FROM domain_executors WHERE id=?1",
                        [call.executor_id.as_deref().expect("Executor")],
                    )
                    .expect("remove Executor");
            }
            "invalid_payload" => envelope.payload = "{".into(),
            "missing_config" => envelope.provider_config = None,
            "missing_credential" => {
                let reference = format!("ocg-test-missing-{}", call.id);
                envelope
                    .provider_config
                    .as_mut()
                    .expect("config")
                    .credential_ref = Some(reference.clone());
                rusqlite::Connection::open(fixture.domain.path())
                    .expect("fixture DB")
                    .execute(
                        "UPDATE domain_dispatch_intents SET credential_ref=?2 WHERE call_id=?1",
                        rusqlite::params![call.id, reference],
                    )
                    .expect("missing credential precondition");
            }
            "shutdown" => fixture
                .handler
                .config
                .cancelled
                .store(true, Ordering::SeqCst),
            _ => {}
        }
        assert!(fixture.execute(envelope).is_err(), "{scenario}");
        fixture.terminal(&call, "failed");
        assert_eq!(
            fixture
                .domain
                .call(&call.id)
                .expect("Call visible even without intent")
                .state,
            "failed"
        );
        if let Some(intent) = fixture.domain.dispatch_intent(&call.id).expect("intent") {
            assert_eq!(intent.effect_state, EffectIntentState::NotStarted);
        }
        fixture.no_residue();
    }
    assert!(upstream.requests.is_empty());
}

#[test]
fn stale_identity_and_frozen_mismatch_cannot_settle_replacement() {
    let upstream = ProviderStub::start();
    for mismatch in [false, true] {
        let mut fixture = Fixture::new();
        let (old, mut envelope, _events) = fixture.admitted(&upstream.endpoint, "old", true);
        let replacement = fixture
            .domain
            .replace_attempt(&envelope.job_id, "provider")
            .expect("replace");
        if mismatch {
            envelope
                .provider_config
                .as_mut()
                .expect("config")
                .upstream_model_id = "different".into();
        }
        let cursor = fixture.domain.journal_head().expect("cursor");
        assert!(fixture.execute(envelope).is_err());
        assert_eq!(fixture.domain.journal_head().expect("cursor"), cursor);
        assert_eq!(
            fixture
                .domain
                .authority(&replacement.attempt.id)
                .expect("authority")
                .expect("still live")
                .generation,
            2
        );
        assert_eq!(
            fixture.domain.call(&old.id).expect("old Call").state,
            "unknown"
        );
        assert!(upstream.requests.is_empty());
        let intent = fixture
            .domain
            .create_call_with_effect(
                &replacement.attempt.id,
                Some(&replacement.executor.id),
                2,
                EffectIntentKind::StrictFenced,
                "{}",
            )
            .expect("replacement Call");
        fixture
            .domain
            .start_call(&intent.id, &replacement.attempt.id, 2)
            .expect("replacement claim");
        fixture
            .domain
            .finish_call(&intent.id, &replacement.attempt.id, 2, "{}")
            .expect("replacement completes");
        fixture
            .domain
            .finish_attempt(&replacement.attempt.id, true)
            .expect("replacement settles");
        fixture.no_residue();
    }
}

#[test]
fn start_failure_distinguishes_stale_claimed_and_unsuccessful_calls() {
    let upstream = ProviderStub::start();
    for scenario in ["lost", "claimed", "failed", "generation"] {
        let mut fixture = Fixture::new();
        let (call, mut envelope, _events) = fixture.admitted(&upstream.endpoint, scenario, true);
        let replacement = match scenario {
            "lost" => Some(
                fixture
                    .domain
                    .replace_attempt(&envelope.job_id, "provider")
                    .expect("replacement"),
            ),
            "claimed" => {
                fixture
                    .domain
                    .start_call(&call.id, &call.attempt_id, call.generation)
                    .expect("other claimant");
                None
            }
            "failed" => {
                fixture
                    .domain
                    .fail_call(
                        &call.id,
                        &call.attempt_id,
                        call.generation,
                        "other actor failure",
                    )
                    .expect("Call failed");
                None
            }
            "generation" => {
                envelope.generation += 1;
                None
            }
            _ => unreachable!(),
        };
        let error = fixture
            .domain
            .start_call(&call.id, &envelope.attempt_id, envelope.generation)
            .expect_err("start failure");
        let cursor = fixture.domain.journal_head().expect("cursor");
        fail_authoritative_provider_call(fixture.root.path(), &envelope, &error.to_string(), false);
        if scenario == "failed" {
            fixture.terminal(&call, "failed");
        } else {
            assert_eq!(
                fixture.domain.journal_head().expect("no failure event"),
                cursor
            );
            if let Some(replacement) = replacement {
                assert!(fixture
                    .domain
                    .authority(&replacement.attempt.id)
                    .expect("authority")
                    .is_some());
                fixture
                    .domain
                    .finish_attempt(&replacement.attempt.id, true)
                    .expect("new owner settles");
            } else {
                if scenario == "generation" {
                    fixture
                        .domain
                        .start_call(&call.id, &call.attempt_id, call.generation)
                        .expect("correct generation claims");
                }
                fixture
                    .domain
                    .finish_call(&call.id, &call.attempt_id, call.generation, "{}")
                    .expect("original claimant finishes");
                fixture
                    .domain
                    .finish_attempt(&call.attempt_id, true)
                    .expect("original owner settles");
            }
        }
        fixture.no_residue();
    }
    assert!(upstream.requests.is_empty());
}

#[test]
fn unclaimed_failure_cannot_race_past_an_existing_claim_or_generation_fence() {
    let mut fixture = Fixture::new();
    let (call, envelope, _events) = fixture.admitted("http://127.0.0.1:1/v1", "claim", true);
    fixture
        .domain
        .start_call(&call.id, &call.attempt_id, call.generation)
        .expect("other actor wins claim");
    let cursor = fixture.domain.journal_head().expect("cursor");
    assert!(!fixture
        .domain
        .fail_unclaimed_call(
            &call.id,
            &call.attempt_id,
            call.generation,
            "pre-execution failure"
        )
        .expect("guard"));
    assert!(!fixture
        .domain
        .fail_unclaimed_call(
            &call.id,
            &call.attempt_id,
            call.generation + 1,
            "stale generation"
        )
        .expect("guard"));
    assert_eq!(fixture.domain.journal_head().expect("no event"), cursor);
    fail_authoritative_provider_call(
        fixture.root.path(),
        &envelope,
        "pre-execution failure",
        false,
    );
    assert_eq!(fixture.domain.journal_head().expect("no event"), cursor);
    fixture
        .domain
        .finish_call(&call.id, &call.attempt_id, call.generation, "{}")
        .expect("claimant completes");
    fixture
        .domain
        .finish_attempt(&call.attempt_id, true)
        .expect("settlement");
    fixture.no_residue();
}

/// A round that never fails costs exactly one physical HTTP attempt.
#[test]
fn streaming_success_uses_one_physical_attempt_and_settles_the_attempt() {
    let upstream = ProviderStub::start();
    upstream.script("success", vec![StubStep::Stream(&["Real ", "reply"])]);
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&upstream.endpoint, "success", true);

    fixture.execute(envelope).expect("streamed round");

    assert_eq!(upstream.attempts("success"), 1);
    assert_eq!(upstream.attempts("success"), upstream.closures());
    assert!(events.try_iter().any(|event| matches!(
        event,
        ExecutionEvent::Provider(ChatStreamEvent::TextDelta { .. })
    )));
    fixture.terminal(&call, "completed");
    fixture.no_residue();
}

/// HTTP 401 describes the request, not the transport, so the Call settles on the
/// first attempt and nothing is repeated.
#[test]
fn non_retryable_http_status_is_never_repeated() {
    let upstream = ProviderStub::start();
    upstream.script("unauthorized", vec![StubStep::Status(401)]);
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&upstream.endpoint, "unauthorized", true);

    let error = fixture.execute(envelope).expect_err("401 must fail");
    assert!(error.to_string().contains("401"), "{error}");

    assert_eq!(upstream.attempts("unauthorized"), 1);
    // The round opened and the Call failed; no provider content was ever
    // published, and no round was reset because nothing was provisional.
    let published = events.try_iter().collect::<Vec<_>>();
    assert_eq!(published.len(), 2, "{published:?}");
    assert!(matches!(
        published[0],
        ExecutionEvent::Provider(ChatStreamEvent::RoundBegan)
    ));
    assert!(matches!(&published[1], ExecutionEvent::Failed(_)));
    fixture.terminal(&call, "failed");
    fixture.no_residue();
}

/// A 503 is provider weather: the same logical round is re-requested and its
/// replacement completes, with the Job, Attempt and Call untouched.
#[test]
fn retryable_http_status_repeats_the_same_logical_round() {
    let upstream = ProviderStub::start();
    upstream.script(
        "overloaded",
        vec![StubStep::Status(503), StubStep::Stream(&["Real ", "reply"])],
    );
    let mut fixture = Fixture::new();
    let (call, envelope, _events) = fixture.admitted(&upstream.endpoint, "overloaded", true);

    fixture.execute(envelope).expect("round recovered");

    assert_eq!(upstream.attempts("overloaded"), 2);
    // One provider Call: the retry stayed inside the round rather than
    // admitting a new Call, a new Attempt or a new Job.
    let calls = fixture
        .domain
        .calls_for_attempt(&call.attempt_id)
        .expect("calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, call.id);
    let response: Value = serde_json::from_str(
        calls[0].response.as_deref().expect("provider response"),
    )
    .expect("response JSON");
    assert_eq!(response["content"], "Real reply");
    assert_eq!(response["rounds"], 1);
    fixture.terminal(&call, "completed");
    fixture.no_residue();
}

/// An exhausted budget settles the Call exactly once, on the final failure.
#[test]
fn retryable_http_status_exhausts_the_bounded_round_budget() {
    let upstream = ProviderStub::start();
    upstream.script("always-overloaded", vec![StubStep::Status(503)]);
    let mut fixture = Fixture::new();
    let (call, envelope, _events) =
        fixture.admitted(&upstream.endpoint, "always-overloaded", true);

    fixture
        .execute(envelope)
        .expect_err("exhausted budget must fail");

    assert_eq!(
        upstream.attempts("always-overloaded"),
        MAX_PHYSICAL_ATTEMPTS_PER_ROUND
    );
    let calls = fixture
        .domain
        .calls_for_attempt(&call.attempt_id)
        .expect("calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].state, "failed");
    fixture.terminal(&call, "failed");
    fixture.no_residue();
}

/// A premature EOF after partial output resets that round's provisional text and
/// the replacement starts clean, on the same Call and the same assistant
/// message.
#[test]
fn partial_stream_then_premature_eof_resets_the_round_before_retrying() {
    let upstream = ProviderStub::start();
    upstream.script(
        "truncated",
        vec![
            StubStep::TruncatedStream(&["half-written answ"]),
            StubStep::Stream(&["complete ", "answer"]),
        ],
    );
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&upstream.endpoint, "truncated", true);

    fixture.execute(envelope).expect("round recovered");

    assert_eq!(upstream.attempts("truncated"), 2);
    let published = events.try_iter().collect::<Vec<_>>();
    let shapes = published
        .iter()
        .map(|event| match event {
            ExecutionEvent::Provider(ChatStreamEvent::TextDelta { delta }) => {
                format!("delta:{delta}")
            }
            ExecutionEvent::Provider(ChatStreamEvent::RoundBegan) => "round-began".to_string(),
            ExecutionEvent::Provider(ChatStreamEvent::RoundReset) => "round-reset".to_string(),
            other => format!("{other:?}"),
        })
        .collect::<Vec<_>>();
    // The failed attempt's provisional text is published, then invalidated, and
    // the replacement appends from a clean presentation state: the two halves are
    // never concatenated.
    assert_eq!(
        shapes,
        vec![
            "round-began",
            "delta:half-written answ",
            "round-reset",
            "delta:complete ",
            "delta:answer",
            "Finished",
        ],
        "{shapes:?}"
    );
    let calls = fixture
        .domain
        .calls_for_attempt(&call.attempt_id)
        .expect("calls");
    assert_eq!(calls.len(), 1);
    let response: Value = serde_json::from_str(
        calls[0].response.as_deref().expect("provider response"),
    )
    .expect("response JSON");
    assert_eq!(response["content"], "complete answer");
    assert_eq!(response["rounds"], 1);
    fixture.terminal(&call, "completed");
    fixture.no_residue();
}

/// A tool call emitted by a physical attempt that then fails must never become a
/// durable Call, so a replacement attempt cannot double a native side effect.
///
/// The first attempt streams a real OpenAI `tool_calls` fragment — an id, an
/// existing Native Tool name and complete arguments — and the connection then
/// dies before the round can complete.
#[test]
fn discarded_tool_call_from_a_failed_attempt_is_never_admitted() {
    let upstream = ProviderStub::start();
    upstream.script(
        "truncated-tool",
        vec![
            StubStep::TruncatedFrames(discarded_tool_frames()),
            StubStep::Stream(&["done ", "without ", "tools"]),
        ],
    );
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&upstream.endpoint, "truncated-tool", true);

    fixture.execute(envelope).expect("round recovered");

    // Two physical attempts, both against the same logical round.
    assert_eq!(upstream.attempts("truncated-tool"), 2);

    let published = events.try_iter().collect::<Vec<_>>();
    // The discarded attempt really did reach tool-call parsing: its id, name and
    // argument fragments were decoded and published before the transport died.
    // Without this the no-native-Call assertion below would be vacuous.
    let decoded = published
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Provider(ChatStreamEvent::ToolCallStart { id, name, .. }) => {
                Some(format!("start:{id}:{name}"))
            }
            ExecutionEvent::Provider(ChatStreamEvent::ToolCallArgumentsDelta { id, delta, .. }) => {
                Some(format!("args:{id}:{delta}"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decoded,
        vec![
            "start:call-discarded:filesystem_list",
            "args:call-discarded:{\"path\":\"",
            "start:call-discarded:filesystem_list",
            "args:call-discarded:.\"}",
        ],
        "the discarded attempt never reached tool-call parsing: {published:?}"
    );
    // The fragments reconstructed a complete, valid argument object for a tool
    // that really exists, so this attempt would have been dispatched had the
    // transport not died first.
    let arguments = decoded
        .iter()
        .filter(|event| event.starts_with("args:"))
        .map(|event| event.trim_start_matches("args:call-discarded:"))
        .collect::<String>();
    assert_eq!(arguments, "{\"path\":\".\"}");
    assert_eq!(
        serde_json::from_str::<Value>(&arguments).expect("tool arguments"),
        json!({"path": "."})
    );
    // That attempt never completed: no `ToolCallComplete` was ever produced,
    // because the round was abandoned mid-stream.
    assert!(
        !published
            .iter()
            .any(|event| matches!(
                event,
                ExecutionEvent::Provider(ChatStreamEvent::ToolCallComplete { .. })
            )),
        "{published:?}"
    );
    // No user-visible text was produced by the discarded attempt, so there was no
    // provisional presentation to reset; the reset boundary is reserved for
    // output the user actually saw. What protects the side effect is that the
    // replacement attempt starts from a fresh decoding state, so the discarded
    // fragments never become a tool Call.
    assert!(
        !published
            .iter()
            .any(|event| matches!(
                event,
                ExecutionEvent::Provider(ChatStreamEvent::RoundReset)
            )),
        "a tool-call-only attempt has no provisional presentation to reset: {published:?}"
    );

    // The side-effect invariant: only the provider Call exists durably. No
    // Native Tool Call was admitted, dispatched or completed from attempt #1, and
    // the dispatcher drained empty.
    let calls = fixture
        .domain
        .calls_for_attempt(&call.attempt_id)
        .expect("calls");
    assert_eq!(calls.len(), 1, "a discarded tool call was admitted: {calls:?}");
    assert_eq!(calls[0].id, call.id);
    assert_eq!(calls[0].state, "completed");
    let input: Value = serde_json::from_str(&calls[0].request).expect("Call request");
    assert_eq!(
        input["executor_transport"], "provider",
        "only the provider Call survived"
    );

    // The replacement attempt's response is the one that settled the round.
    let response: Value = serde_json::from_str(
        calls[0].response.as_deref().expect("provider response"),
    )
    .expect("response JSON");
    assert_eq!(response["content"], "done without tools");
    assert_eq!(response["rounds"], 1);
    fixture.terminal(&call, "completed");
    fixture.no_residue();
}

/// Cancellation interrupts a provider-requested cooldown and no further physical
/// attempt is issued.
#[test]
fn cancellation_during_retry_backoff_stops_immediately_and_owns_settlement() {
    let upstream = ProviderStub::start();
    // A provider-asked cooldown is never collapsed by the test seam, so the
    // cancellation lands while the round is genuinely waiting.
    upstream.script("throttled", vec![StubStep::Throttled(429, 30)]);
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&upstream.endpoint, "throttled", true);
    let cancelled = envelope.cancelled.clone();
    let handler = fixture.handler.clone();

    let worker = thread::spawn(move || execute_provider_envelope_sync(&handler, envelope));
    // Wait for the throttling response to land, then cancel while the round is
    // inside its provider-requested cooldown.
    upstream.request("throttled");
    fixture
        .domain
        .request_cancel(&call.attempt_id)
        .expect("request cancel");
    cancelled.cancel();
    fixture
        .domain
        .confirm_cancel(&call.attempt_id, true)
        .expect("confirm cancel");
    let result = worker.join().expect("provider worker");

    assert!(result.is_err(), "a cancelled round never completes");
    // One physical request only: the cooldown was interrupted, not waited out,
    // so no second attempt reached the provider.
    assert_eq!(upstream.attempts("throttled"), 1);
    let published = events.try_iter().collect::<Vec<_>>();
    assert!(
        !published
            .iter()
            .any(|event| matches!(event, ExecutionEvent::Finished)),
        "{published:?}"
    );
    // Cancellation, not a provider failure, owns settlement: the throttle never
    // reached the failure path that would overwrite `cancelled` with `failed`.
    let settled = fixture.domain.call(&call.id).expect("Call");
    assert_ne!(settled.state, "failed", "{settled:?}");
    fixture.terminal(&call, "cancelled");
    fixture.no_residue();
}

#[test]
fn reasoning_provider_can_wait_more_than_thirty_seconds_for_a_response_head() {
    let upstream = ProviderStub::start();
    let mut fixture = Fixture::new();
    let (call, envelope, _events) = fixture.admitted(&upstream.endpoint, "slow-head", true);
    fixture
        .execute(envelope)
        .expect("reasoning provider response");
    upstream.request("slow-head");
    upstream.closure("slow-head");
    fixture.terminal(&call, "completed");
    fixture.no_residue();
}

#[test]
fn native_file_read_reports_byte_pagination_for_continuation() {
    let root = tempfile::tempdir().expect("project");
    std::fs::write(root.path().join("sample"), "first line\nsecond line\n").expect("file");
    let executor = crate::native_tools::NativeToolExecutor::new(root.path()).expect("native tools");
    let read = |arguments| {
        executor.execute(
            "filesystem.read",
            &arguments,
            crate::native_tools::PermissionClass::ReadOnly,
            PermissionPolicy::allow_all(),
            &AtomicBool::new(false),
        )
    };
    let first = read(json!({"path":"sample","offset":0,"limit":5}));
    assert!(first.success && first.truncated);
    assert_eq!(first.output["content"], "first");
    assert_eq!(first.metadata["nextOffset"], 5);
    let next = read(json!({"path":"sample","offset":first.metadata["nextOffset"]}));
    assert!(next.success && !next.truncated);
    assert_eq!(next.output["content"], " line\nsecond line\n");
    std::fs::write(root.path().join("quoted"), "\"".repeat(20_000)).expect("large file");
    let large = read(json!({"path":"quoted"}));
    assert!(large.success && large.truncated);
    assert_eq!(
        large.output["content"]
            .as_str()
            .expect("structured content")
            .len(),
        8192
    );
    assert_eq!(large.metadata["nextOffset"], 8192);
    assert!(large.to_value().to_string().len() <= crate::native_tools::TOOL_OUTPUT_CAP);
}

/// A refused connection is typed reconnectable, so the bounded round budget
/// retries it and then settles the Call as failed with no residue.
#[test]
fn connection_refused_exhausts_the_bounded_round_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("unused address");
    let endpoint = format!("http://{}/v1", listener.local_addr().expect("address"));
    drop(listener);
    let mut fixture = Fixture::new();
    let (call, envelope, events) = fixture.admitted(&endpoint, "refused", true);
    let started = Instant::now();

    let error = fixture.execute(envelope).expect_err("refused must fail");

    // The refusal is observed by the transport seam as reconnectable, so it is
    // repeated up to the bound rather than terminalizing on the first attempt.
    assert!(
        error.transport_fault() == TransportFault::Reconnectable,
        "{error}"
    );
    // The bound is finite, so an unreachable endpoint still settles promptly
    // instead of retrying forever.
    assert!(
        started.elapsed() < MAX_PHYSICAL_ATTEMPTS_PER_ROUND as u32 * 4 * Duration::from_secs(1),
        "refused round took {:?}",
        started.elapsed()
    );
    // Only the round boundary opened: no response byte ever reached the
    // decoder, so there was nothing provisional to reset.
    let published = events.try_iter().collect::<Vec<_>>();
    assert_eq!(published.len(), 2, "{published:?}");
    assert!(matches!(
        published[0],
        ExecutionEvent::Provider(ChatStreamEvent::RoundBegan)
    ));
    assert!(matches!(&published[1], ExecutionEvent::Failed(_)));
    let calls = fixture
        .domain
        .calls_for_attempt(&call.attempt_id)
        .expect("calls");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].state, "failed");
    fixture.terminal(&call, "failed");
    fixture.no_residue();
}

#[test]
fn empty_user_visible_final_fails_and_settles_the_attempt() {
    let upstream = ProviderStub::start();
    for label in ["retry-empty", "empty-final", "blank-final"] {
        let mut fixture = Fixture::new();
        let (call, envelope, _events) = fixture.admitted(&upstream.endpoint, label, true);
        let error = fixture
            .execute(envelope)
            .expect_err("empty final must fail");
        assert!(error
            .to_string()
            .contains("no user-visible assistant content"));
        for _ in 0..if label == "retry-empty" { 7 } else { 1 } {
            upstream.request(label);
            upstream.closure(label);
        }
        assert_eq!(
            fixture
                .domain
                .calls_for_attempt(&call.attempt_id)
                .expect("calls")
                .len(),
            1
        );
        assert_eq!(fixture.domain.call(&call.id).expect("Call").state, "failed");
        let intent = fixture
            .domain
            .dispatch_intent(&call.id)
            .expect("Intent")
            .expect("exists");
        assert_eq!(intent.state, "failed");
        // The upstream request was dispatched, so retain conservative failure accounting.
        assert_eq!(intent.effect_state, EffectIntentState::Unknown);
        fixture.terminal(&call, "failed");
        fixture.no_residue();
    }
}

#[test]
fn native_tool_loop_preserves_fragmented_arguments_and_completes() {
    let upstream = ProviderStub::start();
    for label in ["tool-loop", "fragmented-tool-loop"] {
        let mut fixture = Fixture::new();
        let (call, envelope, _events) = fixture.admitted(&upstream.endpoint, label, true);
        let tools = fixture.tools.clone();
        let handler = crate::native_tools::NativeToolCallHandler::new(
            fixture.root.path().to_path_buf(),
            PermissionPolicy::allow_all(),
            Arc::new(AtomicBool::new(false)),
        );
        let worker = thread::spawn(move || run_native_tool_worker(&tools, &handler));
        let result = fixture.execute(envelope);
        fixture.tools.shutdown();
        worker
            .join()
            .expect("native worker")
            .expect("native execution");
        result.expect("normal tool loop");
        for _ in 0..2 {
            upstream.request(label);
            upstream.closure(label);
        }
        fixture.terminal(&call, "completed");
        let calls = fixture
            .domain
            .calls_for_attempt(&call.attempt_id)
            .expect("calls");
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.state == "completed"));
        let provider = fixture.domain.call(&call.id).expect("provider");
        let response: Value =
            serde_json::from_str(provider.response.as_deref().expect("response")).expect("JSON");
        assert_eq!(response["content"], "Real reply");
        assert_eq!(response["rounds"], 2);
        fixture.no_residue();
    }
}

#[test]
fn repeated_tool_metadata_does_not_discard_argument_deltas() {
    let mut summary = ChatStreamSummary::default();
    for arguments in ["{\"path\":", "\".\"}"] {
        apply_chunk_json(
            &mut summary,
            &json!({"choices": [{"delta": {
                "tool_calls": [{"index": 0, "id": "native-list", "function": {
                    "name": "filesystem_list", "arguments": arguments
                }}]
            }}]}),
        )
        .expect("stream chunk");
    }
    assert_eq!(summary.tool_calls.len(), 1);
    assert_eq!(summary.tool_calls[0].arguments, "{\"path\":\".\"}");
}

#[test]
fn queued_cancellation_is_idempotent_and_cannot_become_failure() {
    let upstream = ProviderStub::start();
    for settled in [false, true] {
        let mut fixture = Fixture::new();
        let (call, envelope, _events) = fixture.admitted(&upstream.endpoint, "cancelled", true);
        envelope.cancelled.cancel();
        envelope.cancelled.cancel();
        if settled {
            fixture
                .domain
                .request_cancel(&call.attempt_id)
                .expect("request");
            fixture
                .domain
                .confirm_cancel(&call.attempt_id, true)
                .expect("confirm");
        }
        let cursor = fixture.domain.journal_head().expect("cursor");
        assert!(fixture.execute(envelope).is_err());
        if settled {
            assert_eq!(
                fixture.domain.journal_head().expect("no failure events"),
                cursor
            );
        }
        fixture.terminal(&call, "cancelled");
        fixture.no_residue();
    }
    assert!(upstream.requests.is_empty());
}

fn wait_terminal(domain: &DomainRepository, job_id: &str, state: JobState) {
    let deadline = Instant::now() + WAIT;
    loop {
        let job = domain.job(job_id).expect("Job").expect("exists");
        if job.state == state {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Job did not reach {state}: {job:?}"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn chat_queue_supersede_cancel_and_empty_final_keep_one_consumer_progressing() {
    let upstream = ProviderStub::start();
    let root = tempfile::tempdir().expect("project");
    std::fs::create_dir(root.path().join(".ocg")).expect("marker");
    let profile_path = root.path().join("profile.yaml");
    let config = json!({"profile":{"origin":"new","defaultModel":"local/example"},"models":{"providers":{"local":{"placeholder":false,"label":"local","endpoint":upstream.endpoint}},"models":{"local/example":{"placeholder":false,"provider":"local","id":"example"}}}});
    std::fs::write(
        &profile_path,
        serde_yaml_ng::to_string(&config).expect("profile YAML"),
    )
    .expect("profile");
    let service = CanonicalControlService::open(root.path())
        .expect("control")
        .with_profile_service(crate::profile::ProfileService::with_workspace(
            &profile_path,
            root.path(),
        ));
    let domain = DomainRepository::open(root.path()).expect("domain");
    let project = domain.ensure_project(root.path()).expect("Project");
    service
        .register_project("register", root.path(), 0)
        .expect("register Project");
    service
        .set_project_defaults(
            "defaults",
            &project.id,
            json!({"provider":"local","model":"local/example"}),
            0,
        )
        .expect("defaults");
    drop(domain);
    let runtime = ExecutionRuntime::start(
        root.path(),
        8,
        8,
        1,
        Arc::new(NativeHttp::new().expect("HTTP")),
        PermissionPolicy::allow_all(),
    )
    .expect("execution runtime");
    let service = service.with_runtime_handle(ExecutionRuntimeHandle::new(&runtime));
    let domain = DomainRepository::open(root.path()).expect("domain");
    let send = |command: &str, session: &str| {
        let response = service
            .launch_chat(
                JobLaunchRequest {
                    command_id: command.into(),
                    draft_id: command.into(),
                    project_id: project.id.clone(),
                    session_id: session.into(),
                    objective: command.into(),
                    success_criteria: None,
                    constraints: None,
                    hard_budget_micros: 0,
                    resource_commitment: None,
                },
                0,
            )
            .expect("launch Chat");
        assert_eq!(response.outcome, "accepted");
        response.job_id.expect("Job")
    };
    for replacement in [false, true] {
        let blocker = if replacement {
            "silent-replacement"
        } else {
            "silent-explicit"
        };
        let blocker_job = send(blocker, blocker);
        upstream.request(blocker);
        let old_label = if replacement {
            "queued-old"
        } else {
            "queued-cancel"
        };
        let old = send(old_label, "session");
        let attempt = domain
            .attempts_for_job(&old)
            .expect("Attempt")
            .pop()
            .expect("exists");
        assert_eq!(
            domain.calls_for_attempt(&attempt.id).expect("Call")[0].state,
            "created"
        );
        if !replacement {
            assert!(service.cancel_chat("session").expect("cancel"));
            assert!(!service.cancel_chat("session").expect("duplicate cancel"));
        }
        let next_label = if replacement {
            "next-turn-replacement"
        } else {
            "next-turn-cancel"
        };
        let next = send(next_label, "session");
        wait_terminal(&domain, &old, JobState::Cancelled);
        assert!(service.cancel_chat(blocker).expect("cancel silent stream"));
        upstream.closure(blocker);
        upstream.request(next_label);
        upstream.closure(next_label);
        wait_terminal(&domain, &blocker_job, JobState::Cancelled);
        wait_terminal(&domain, &next, JobState::Completed);
        assert!(
            upstream.requests.is_empty(),
            "cancelled turn reached upstream"
        );
        assert_eq!(
            domain
                .dispatch_intent(&domain.calls_for_attempt(&attempt.id).expect("Call")[0].id)
                .expect("Intent")
                .expect("exists")
                .effect_state,
            EffectIntentState::NotStarted
        );
        assert!(!service.cancel_chat("session").expect("already completed"));
    }
    let empty = send("retry-empty", "session");
    wait_terminal(&domain, &empty, JobState::Failed);
    for _ in 0..7 {
        upstream.request("retry-empty");
        upstream.closure("retry-empty");
    }
    assert!(!service
        .cancel_chat("session")
        .expect("failed turn is terminal"));
    let next = send("after-empty", "session");
    wait_terminal(&domain, &next, JobState::Completed);
    upstream.request("after-empty");
    upstream.closure("after-empty");
    assert_no_residue(&domain);
    runtime.shutdown().expect("shutdown");
}
