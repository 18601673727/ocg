#![cfg(unix)]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ntex::web::{self, App, HttpResponse, HttpServer};
use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::persistence::{Entry, OutcomeCause, OutcomeKind, StopProof, ToolDecision};
use redb::{ReadableDatabase, TableDefinition};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(45);
const TOKEN: &str = "K3-Acceptance_0123456789.abcdefghijklmnop";
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn wait_for(mut condition: impl FnMut() -> bool) {
    let until = Instant::now() + WAIT;
    while !condition() {
        assert!(
            Instant::now() < until,
            "scenario condition did not become true"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
fn wait_long(mut condition: impl FnMut() -> bool) {
    let until = Instant::now() + Duration::from_secs(150);
    while !condition() {
        assert!(Instant::now() < until, "recurrence did not advance");
        std::thread::sleep(Duration::from_millis(100));
    }
}
fn command(data: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .env("OCG_DATA_DIR", data)
        .env("OCG_K_SCENARIOS_PROVIDER", "scripted-test-key")
        .env_remove("HTTPS_PROXY")
        .env_remove("HTTP_PROXY")
        .env_remove("ALL_PROXY")
        .env_remove("https_proxy")
        .env_remove("http_proxy")
        .env_remove("all_proxy")
        .env_remove("OCG_UI_DIR")
        .env_remove("OCG_CONTROL_PORT")
        .env_remove("OCG_CONTROL_TOKEN")
        .stdin(Stdio::null());
    command
}

struct Daemon {
    child: Child,
    address: SocketAddr,
    reader: Option<std::thread::JoinHandle<()>>,
}
impl Daemon {
    fn start(data: &Path) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut child = command(data)
            .env("OCG_CONTROL_PORT", port.to_string())
            .env("OCG_CONTROL_TOKEN", TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = child.stderr.take().unwrap();
        let (sender, lines) = flume::bounded(256);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                assert!(!line.contains(TOKEN));
                sender.try_send(line).ok();
            }
        });
        let until = Instant::now() + WAIT;
        let mut seen = Vec::new();
        let address = loop {
            let line = lines
                .recv_timeout(until.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|_| panic!("daemon did not start: {seen:?}"));
            if let Some(address) = line.strip_prefix("OCG control plane listening on http://") {
                break address.parse().unwrap();
            }
            seen.push(line);
        };
        Self {
            child,
            address,
            reader: Some(reader),
        }
    }
    fn stop(mut self, crash: bool) {
        if crash {
            self.child.kill().unwrap();
        } else {
            assert!(
                Command::new("kill")
                    .args(["-TERM", &self.child.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let until = Instant::now() + WAIT;
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < until, "daemon failed to stop");
            std::thread::sleep(Duration::from_millis(10));
        }
        self.reader.take().unwrap().join().unwrap();
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

fn request(address: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let mut stream = TcpStream::connect_timeout(&address, WAIT).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    let body = body.map(Value::to_string).unwrap_or_default();
    let key = SEQUENCE.fetch_add(1, Ordering::SeqCst);
    write!(stream, "{method} /api/v1/{path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nIdempotency-Key: K3-scenario-request-{key:016}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(!body.contains(TOKEN));
    (
        status,
        serde_json::from_str(body).unwrap_or_else(|_| panic!("invalid response {head}: {body}")),
    )
}

struct Response {
    body: String,
    status: u16,
    gate: Option<Arc<AtomicBool>>,
}
fn done(text: &str) -> Response {
    calls(text, vec![])
}
fn calls(text: &str, calls: Vec<Value>) -> Response {
    let has_calls = !calls.is_empty();
    let mut delta = json!({"content":text});
    if has_calls {
        delta["tool_calls"] = json!(calls);
    }
    Response {
        status: 200,
        gate: None,
        body: format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":delta,"finish_reason":if has_calls {"tool_calls"} else {"stop"}}],"usage":{"prompt_tokens":10,"completion_tokens":2}})
        ),
    }
}
fn call(index: usize, name: &str, arguments: Value) -> Value {
    json!({"index":index,"id":format!("call_{index}"),"type":"function","function":{"name":name,"arguments":arguments.to_string()}})
}
fn dispatch(index: usize, agent: &str, project: &str, marker: &str, keep_open: bool) -> Value {
    call(
        index,
        "job_dispatch",
        json!({"agent":agent,"project":project,"objective":marker,"keep_open":keep_open}),
    )
}
fn marker(body: &Value) -> String {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .filter_map(|message| message["content"].as_str())
        .find_map(|text| {
            text.find("K3:").map(|at| {
                text[at..]
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .trim_end_matches('.')
                    .to_owned()
            })
        })
        .unwrap_or_default()
}
fn after_tools(body: &Value) -> bool {
    body["messages"]
        .as_array()
        .unwrap()
        .last()
        .is_some_and(|m| m["role"] == "tool")
}
fn held(mut response: Response, gate: &Arc<AtomicBool>) -> Response {
    response.gate = Some(gate.clone());
    response
}

struct Provider {
    address: SocketAddr,
    stop: flume::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Provider {
    fn start(script: impl Fn(&Value) -> Response + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let script = Arc::new(script);
        let (stop, stopped) = flume::bounded(1);
        let (ready, started) = flume::bounded(1);
        let thread = std::thread::spawn(move || {
            ntex::rt::System::new("k-scenarios-provider", ntex::rt::DefaultRuntime).block_on(
                async move {
                    let server = HttpServer::new(async move || {
                        let script = script.clone();
                        App::new().default_service(web::to(
                            async move |body: web::types::Json<Value>| {
                                let response = script(&body);

                                if let Some(gate) = response.gate {
                                    while !gate.load(Ordering::SeqCst) {
                                        ntex::time::sleep(Duration::from_millis(10)).await;
                                    }
                                }
                                if response.status == 200 {
                                    HttpResponse::Ok()
                                        .content_type("text/event-stream")
                                        .body(response.body)
                                } else {
                                    HttpResponse::BadRequest()
                                        .content_type("application/json")
                                        .body(response.body)
                                }
                            },
                        ))
                    })
                    .workers(2)
                    .disable_signals()
                    .listen(listener)
                    .unwrap()
                    .run();
                    let finished = ntex::rt::spawn(server.clone());
                    ready.send(()).unwrap();
                    stopped.recv_async().await.ok();
                    server.stop(false).await;
                    finished.await.unwrap().unwrap();
                },
            );
        });
        started.recv_timeout(WAIT).unwrap();
        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        self.stop.send(()).ok();
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

#[derive(Default)]
struct ScriptState {
    agents: Vec<String>,
    project: String,
    children: Vec<String>,
    counts: HashMap<String, usize>,
}
impl ScriptState {
    fn count(&mut self, marker: &str) -> usize {
        let count = self.counts.entry(marker.into()).or_default();
        let value = *count;
        *count += 1;
        value
    }
    fn remember(&mut self, body: &Value) {
        for message in body["messages"].as_array().unwrap() {
            if message["role"] != "tool" {
                continue;
            }
            if let Some(content) = message["content"]
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                && let Some(id) = content["conversation"].as_str()
                && !self.children.iter().any(|child| child == id)
            {
                self.children.push(id.into());
            }
        }
    }
}

struct Host {
    root: PathBuf,
    data: PathBuf,
    workspace: PathBuf,
    daemon: Option<Daemon>,
    _provider: Provider,
    model: String,
    executor: String,
    project: String,
}
impl Host {
    fn new(
        name: &str,
        script: impl Fn(&Value) -> Response + Send + Sync + 'static,
        commands: Value,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "ocg-k3-{name}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let data = root.join("data");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let provider = Provider::start(script);
        let output = command(&data)
            .arg("register-llm")
            .env("OCG_PROVIDER_ENDPOINT", "https://k-scenarios.invalid/v1")
            .env("OCG_PROVIDER_MODEL", "k-scenario")
            .env("OCG_PROVIDER_CREDENTIAL_REF", "OCG_K_SCENARIOS_PROVIDER")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "register: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let registered: Value = serde_json::from_slice(&output.stdout).unwrap();
        let daemon = Daemon::start(&data);
        let mut host = Self {
            root,
            data,
            workspace,
            daemon: Some(daemon),
            _provider: provider,
            model: registered["llm_profile"].as_str().unwrap().into(),
            executor: registered["executor"].as_str().unwrap().into(),
            project: String::new(),
        };
        host.post(&format!("profiles/{}",host.model), json!({"kind":"llm","name":"scripted","model":"k-scenario","endpoint":format!("http://{}/v1",host._provider.address),"declared_capabilities":["chat","tools"]}));
        let pool = host.executor.replace("executor:", "execution_pool:");
        host.post(&format!("pools/{pool}"), json!({"concurrency_limit":16}));
        host.project = host.post(
            "projects",
            json!({"name":name,"root":host.workspace,"commands":commands}),
        )["project"]["id"]
            .as_str()
            .unwrap()
            .into();
        host
    }
    fn address(&self) -> SocketAddr {
        self.daemon.as_ref().unwrap().address
    }
    fn get(&self, path: &str) -> Value {
        let (status, value) = request(self.address(), "GET", path, None);
        assert_eq!(status, 200, "GET {path}: {value}");
        value
    }
    fn post(&self, path: &str, body: Value) -> Value {
        let (status, value) = request(self.address(), "POST", path, Some(&body));
        assert!(
            (200..300).contains(&status),
            "POST {path}: {status} {value}"
        );
        value
    }
    fn agent(
        &self,
        name: &str,
        targets: &[String],
        approve: &[&str],
        grant: Option<&str>,
    ) -> String {
        self.post("profiles",json!({"kind":"agent","name":name,"llm_targets":[self.model],"default_model":self.model,"dispatch_to":targets,"approve":approve,"tool_grant":grant}))["profile_id"].as_str().unwrap().into()
    }
    fn submit(&self, agent: &str, objective: &str, conversation: Option<&str>) -> (String, String) {
        let mut body = json!({"agent_profile":agent,"llm_profile":self.model,"executor":self.executor,"objective":objective});
        if let Some(conversation) = conversation {
            body["conversation"] = json!(conversation);
        } else {
            body["project"] = json!(self.project);
        }
        let value = self.post("jobs", body);
        let admission = &value["admission"];
        (
            admission["conversation"].as_str().unwrap().into(),
            admission["job"].as_str().unwrap().into(),
        )
    }
    fn turns(&self, conversation: &str) -> Vec<Value> {
        self.get(&format!("conversations/{conversation}/turns?limit=100"))["turns"]
            .as_array()
            .unwrap()
            .clone()
    }
    fn children(&self, conversation: &str) -> Vec<Value> {
        self.get(&format!("conversations/{conversation}/children?limit=100"))["children"]
            .as_array()
            .unwrap()
            .clone()
    }
    fn cards(&self, conversation: &str) -> Vec<Value> {
        self.get(&format!("conversations/{conversation}/cards"))["cards"]
            .as_array()
            .unwrap()
            .clone()
    }
    fn terminal(&self, turn: &str) -> bool {
        self.get(&format!("jobs/{turn}"))["job"]["settlement"]["state"] != "unsettled"
    }
    fn restart(&mut self, crash: bool) {
        self.daemon.take().unwrap().stop(crash);
        self.daemon = Some(Daemon::start(&self.data));
    }
    fn inspect(&mut self, inspect: impl FnOnce(&DatabaseHandle)) {
        self.daemon.take().unwrap().stop(false);
        let directory = DataDirectory::bootstrap(self.data.clone().into_os_string()).unwrap();
        let worker = DatabaseWorker::start(&directory).unwrap();
        inspect(&worker.handle());
        worker.shutdown().unwrap();
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        if let Some(daemon) = self.daemon.take() {
            daemon.stop(false);
        }
        std::fs::remove_dir_all(&self.root).ok();
    }
}
fn id(value: &str) -> ocg::persistence::JobId {
    ocg::persistence::JobId::new(value.strip_prefix("job:").unwrap().parse().unwrap())
}
fn db_wait<F: std::future::Future>(future: F) -> F::Output {
    struct WakeThread(std::thread::Thread);
    impl std::task::Wake for WakeThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(WakeThread(std::thread::current())));
    let mut future = std::pin::pin!(future);
    let until = Instant::now() + WAIT;
    loop {
        if let std::task::Poll::Ready(value) = future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&waker))
        {
            return value;
        }
        assert!(Instant::now() < until);
        std::thread::park_timeout(Duration::from_millis(25));
    }
}
fn notices(turns: &[Value], event: &str) -> Vec<Value> {
    turns
        .iter()
        .flat_map(|turn| turn["entries"].as_array().unwrap())
        .filter(|entry| entry["kind"] == "notice" && entry["event"] == event)
        .cloned()
        .collect()
}

#[test]
#[ignore = "real host acceptance"]
fn scenario_01_clean_caches_and_keep_chatting() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let gate = Arc::new(AtomicBool::new(false));
    let release = gate.clone();
    let host = Host::new(
        "clean-caches",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if after_tools(body) {
                state.remember(body);
                return done("Dispatched");
            }
            if marker == "K3:S1R" && n == 0 {
                return calls(
                    "Cleaning",
                    (0..2)
                        .map(|i| {
                            dispatch(
                                i,
                                &state.agents[0],
                                &state.project,
                                &format!("K3:S1C{i}"),
                                false,
                            )
                        })
                        .collect(),
                );
            }
            if marker.starts_with("K3:S1C") {
                return held(done("Cache clean"), &release);
            }
            done("Answered while children run")
        },
        json!([]),
    );
    let child = host.agent("cleaner", &[], &[], None);
    let root_agent = host.agent("coordinator", std::slice::from_ref(&child), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child];
        state.project = host.project.clone();
    }
    let (root, first) = host.submit(&root_agent, "K3:S1R", None);
    wait_for(|| host.children(&root).len() == 2 && host.terminal(&first));
    let children = host.children(&root);
    wait_for(|| {
        state
            .lock()
            .unwrap()
            .counts
            .keys()
            .filter(|key| key.starts_with("K3:S1C"))
            .count()
            == 2
    });
    for text in ["First user message", "Second user message"] {
        let (_, turn) = host.submit(&root_agent, text, Some(&root));
        wait_for(|| host.terminal(&turn));
        assert!(host.turns(&root).iter().any(|turn| {
            turn["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["text"] == text)
        }));
    }
    assert!(children.iter().all(|child| child["state"] == "open"));
    gate.store(true, Ordering::SeqCst);
    wait_for(|| {
        host.children(&root)
            .iter()
            .all(|child| child["state"] == "settled")
    });
    wait_for(|| notices(&host.turns(&root), "ClosedSelf").len() == 2);
    for child in children {
        assert_eq!(
            notices(&host.turns(&root), "ClosedSelf")
                .iter()
                .filter(|entry| entry["child"] == child["id"])
                .count(),
            1
        );
    }
}

#[test]
#[ignore = "real host acceptance; includes four recurrence intervals"]
fn scenario_02_nightly_progress_failure_hold_and_unhold() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let host = Host::new(
        "nightly",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if after_tools(body) {
                state.remember(body);
                return done("Scheduled");
            }
            if marker == "K3:S2R" && n == 0 {
                return calls(
                    "Nightly",
                    vec![call(
                        0,
                        "job_dispatch",
                        json!({"agent":state.agents[0],"project":state.project,"objective":"K3:S2C","every_seconds":60}),
                    )],
                );
            }
            if marker == "K3:S2C" && n == 2 {
                return Response {
                    status: 400,
                    gate: None,
                    body: json!({"error":{"message":"scripted non-retryable failure"}}).to_string(),
                };
            }
            done("Nightly result")
        },
        json!([]),
    );
    let child = host.agent("nightly worker", &[], &[], None);
    let root_agent = host.agent(
        "nightly coordinator",
        std::slice::from_ref(&child),
        &[],
        None,
    );
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child];
        state.project = host.project.clone();
    }
    let (root, first) = host.submit(&root_agent, "K3:S2R", None);
    wait_for(|| host.children(&root).len() == 1 && host.terminal(&first));
    let child = host.children(&root)[0]["id"].as_str().unwrap().to_owned();
    wait_long(|| {
        state
            .lock()
            .unwrap()
            .counts
            .get("K3:S2C")
            .copied()
            .unwrap_or(0)
            >= 2
            && host
                .turns(&child)
                .iter()
                .all(|turn| turn["job"]["settlement"]["state"] == "completed")
    });
    assert_eq!(host.turns(&root).len(), 1);
    let progress = host.get(&format!("conversations/{root}/notices?limit=100"));
    assert!(progress["notices"].as_array().unwrap().len() >= 2);
    wait_long(|| {
        state
            .lock()
            .unwrap()
            .counts
            .get("K3:S2C")
            .copied()
            .unwrap_or(0)
            >= 3
            && host.turns(&root).len() == 2
    });
    host.post(&format!("conversations/{child}/hold"), json!({}));
    let before = state.lock().unwrap().counts["K3:S2C"];
    eprintln!("scenario 2: holding for two 60-second intervals");
    std::thread::sleep(Duration::from_secs(125));
    assert_eq!(state.lock().unwrap().counts["K3:S2C"], before);
    host.post(&format!("conversations/{child}/unhold"), json!({}));
    wait_long(|| state.lock().unwrap().counts["K3:S2C"] > before);
}

#[test]
#[ignore = "real host acceptance"]
fn scenario_03_grandchild_approval_executes_exactly_once() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let mut host = Host::new(
        "grandchild",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if after_tools(body) {
                state.remember(body);
                return done("Done");
            }
            if marker == "K3:S3R" && n == 0 {
                return calls(
                    "Child",
                    vec![dispatch(
                        0,
                        &state.agents[1],
                        &state.project,
                        "K3:S3C",
                        true,
                    )],
                );
            }
            if marker == "K3:S3C" && n == 0 {
                return calls(
                    "Grandchild",
                    vec![dispatch(
                        0,
                        &state.agents[0],
                        &state.project,
                        "K3:S3G",
                        true,
                    )],
                );
            }
            if marker == "K3:S3G" && n == 0 {
                return calls(
                    "Please approve",
                    vec![call(0, "workspace_run_command", json!({"command":"run"}))],
                );
            }
            done("Done")
        },
        json!([{"name":"run","argv":["sh","-c","echo ran >> ran.txt"]}]),
    );
    let grand_agent = host.agent("grandchild", &[], &["commands"], Some("code"));
    let child_agent = host.agent("child", std::slice::from_ref(&grand_agent), &[], None);
    let root_agent = host.agent("root", std::slice::from_ref(&child_agent), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![grand_agent.clone(), child_agent];
        state.project = host.project.clone();
    }
    let (root, _) = host.submit(&root_agent, "K3:S3R", None);
    wait_for(|| host.cards(&root).len() == 1);
    let card = host.cards(&root)[0].clone();
    let child = host.children(&root)[0]["id"].as_str().unwrap().to_owned();
    assert_eq!(card["parent"], child);
    let grandchild = card["conversation"].as_str().unwrap().to_owned();
    assert_eq!(host.children(&child)[0]["id"], grandchild);
    let approval = card["approval"].as_array().unwrap();
    let path = format!(
        "approvals/job:{}/{}/{}",
        approval[0], approval[1], approval[2]
    );
    let address = host.address();
    let mut attempts = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        attempts.push(std::thread::spawn(move || {
            request(address, "POST", &path, Some(&json!({"decision":"approve"})))
        }));
    }
    let answers = attempts
        .into_iter()
        .map(|attempt| attempt.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        answers.iter().filter(|(status, _)| *status == 200).count(),
        1,
        "{answers:?}"
    );
    assert_eq!(
        answers
            .iter()
            .filter(|(_, body)| body["error"]["code"] == "approval_decided")
            .count(),
        1
    );
    wait_for(|| {
        host.workspace.join("ran.txt").exists()
            && host
                .turns(&grandchild)
                .iter()
                .filter(|turn| turn["job"]["settlement"]["state"] == "completed")
                .count()
                == 2
    });
    let turn = host.turns(&grandchild)[0]["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let tools = host.get(&format!("jobs/{turn}/tools"));
    assert_eq!(tools["tools"].as_array().unwrap().len(), 1);
    assert_eq!(tools["tools"][0]["request"], 1);
    let (_, again) = host.submit(&grand_agent, "Do not run twice", Some(&grandchild));
    wait_for(|| host.terminal(&again));
    assert_eq!(
        std::fs::read_to_string(host.workspace.join("ran.txt")).unwrap(),
        "ran\n"
    );
    host.inspect(|db| {
        let db = db.clone();
        db_wait(async move {
            let calls = db.tool_evidence(id(&turn), None, 100).await.unwrap();
            assert_eq!(calls.len(), 1);
            assert!(calls[0].approval.is_some());
            assert!(
                db.tool_evidence(id(&again), None, 100)
                    .await
                    .unwrap()
                    .is_empty()
            );
        });
    });
}

#[test]
#[ignore = "real host acceptance"]
fn scenario_04_talk_to_a_child_then_close_it() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let host = Host::new(
        "talk-to-child",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if after_tools(body) {
                return done("Dispatched");
            }
            if marker == "K3:S4R" && n == 0 {
                return calls(
                    "Child",
                    vec![dispatch(
                        0,
                        &state.agents[0],
                        &state.project,
                        "K3:S4C",
                        true,
                    )],
                );
            }
            done("Using --release")
        },
        json!([]),
    );
    let child_agent = host.agent("child", &[], &[], None);
    let root_agent = host.agent("root", std::slice::from_ref(&child_agent), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child_agent.clone()];
        state.project = host.project.clone();
    }
    let (root, _) = host.submit(&root_agent, "K3:S4R", None);
    wait_for(|| {
        host.children(&root).len() == 1
            && host.turns(&root).len() >= 2
            && host
                .turns(&root)
                .iter()
                .all(|turn| turn["job"]["settlement"]["state"] == "completed")
    });
    let child = host.children(&root)[0]["id"].as_str().unwrap().to_owned();
    let before = host.turns(&root).len();
    let (_, turn) = host.submit(&child_agent, "use --release", Some(&child));
    wait_for(|| host.terminal(&turn));
    assert_eq!(host.turns(&root).len(), before);
    assert_eq!(host.children(&root)[0]["state"], "open");
    host.post(&format!("conversations/{child}/close"), json!({}));
    wait_for(|| host.children(&root)[0]["state"] == "settled");
}

#[test]
#[ignore = "real host acceptance; includes SIGKILL and an inherited quarantine"]
fn scenario_05_crash_preserves_queues_and_quarantines_the_spawning_turn() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let provider_gate = Arc::new(AtomicBool::new(false));
    let held_provider = provider_gate.clone();
    let root_gate = Arc::new(AtomicBool::new(false));
    let held_root = root_gate.clone();
    let mut host = Host::new(
        "crash",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if after_tools(body) {
                state.remember(body);
                return done("Dispatched");
            }
            if marker == "K3:S5R" {
                if n == 0 {
                    return calls(
                        "Two children",
                        vec![
                            dispatch(0, &state.agents[0], &state.project, "K3:S5A", true),
                            dispatch(1, &state.agents[0], &state.project, "K3:S5B", true),
                        ],
                    );
                }
                return held(done("Both crashes observed"), &held_root);
            }
            if marker == "K3:S5A" && n == 0 {
                return calls(
                    "Sleeping",
                    vec![call(0, "workspace_run_command", json!({"command":"sleep"}))],
                );
            }
            if marker == "K3:S5B" && n == 0 {
                return held(done("Held provider"), &held_provider);
            }
            done("Queued message answered")
        },
        json!([{"name":"sleep","argv":["sleep","30"]}]),
    );
    let child_agent = host.agent("crash child", &[], &[], Some("code"));
    let root_agent = host.agent("root", std::slice::from_ref(&child_agent), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child_agent.clone()];
        state.project = host.project.clone();
    }
    let (root, first) = host.submit(&root_agent, "K3:S5R", None);
    wait_for(|| host.children(&root).len() == 2 && host.terminal(&first));
    let children = host.children(&root);
    let mut a = String::new();
    let mut b = String::new();
    for child in children {
        let child = child["id"].as_str().unwrap().to_owned();
        let turns = host.turns(&child);
        let text = turns[0]["entries"][0]["text"].as_str().unwrap();
        if text == "K3:S5A" {
            a = child;
        } else {
            b = child;
        }
    }
    let a_turn = host.turns(&a)[0]["job"]["id"].as_str().unwrap().to_owned();
    let b_turn = host.turns(&b)[0]["job"]["id"].as_str().unwrap().to_owned();
    wait_for(|| {
        !host.get(&format!("jobs/{a_turn}/tools"))["tools"]
            .as_array()
            .unwrap()
            .is_empty()
            && state.lock().unwrap().counts.contains_key("K3:S5B")
    });
    let (_, a_queued) = host.submit(&child_agent, "Queued A", Some(&a));
    let (_, b_queued) = host.submit(&child_agent, "Queued B", Some(&b));
    let crashed_at = Instant::now();
    host.restart(true);
    wait_for(|| {
        host.terminal(&b_queued)
            && host
                .cards(&root)
                .iter()
                .any(|card| card["kind"] == "quarantine")
    });
    assert_eq!(
        host.get(&format!("jobs/{a_turn}"))["job"]["settlement"]["state"],
        "unknown"
    );
    assert_eq!(
        host.get(&format!("jobs/{b_turn}"))["job"]["settlement"]["state"],
        "unknown"
    );
    assert!(
        !host
            .turns(&a)
            .iter()
            .find(|turn| turn["job"]["id"] == a_turn)
            .unwrap()["uncertain_calls"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!host.get(&format!("jobs/{b_turn}"))["job"]["stop_confirmed_at_us"].is_null());
    assert!(host.get(&format!("jobs/{a_queued}"))["job"]["started_at_us"].is_null());
    let root_turns = host.turns(&root);
    let wake = root_turns
        .iter()
        .find(|turn| {
            turn["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["kind"] == "notice")
                .count()
                == 2
        })
        .expect("one wake holds both attention entries");
    assert_eq!(
        wake["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["event"] == "Stalled")
            .count(),
        1
    );
    let (status, reply) = request(
        host.address(),
        "POST",
        &format!("jobs/{a_turn}/release-slot"),
        Some(&json!({})),
    );
    assert_eq!(status, 422);
    assert_eq!(reply["error"]["code"], "confirm_required");
    std::thread::sleep(Duration::from_secs(31).saturating_sub(crashed_at.elapsed()));
    host.post(
        &format!("jobs/{a_turn}/release-slot"),
        json!({"confirm":true}),
    );
    wait_for(|| host.terminal(&a_queued));
    provider_gate.store(true, Ordering::SeqCst);
    root_gate.store(true, Ordering::SeqCst);
    wait_for(|| {
        host.turns(&root)
            .iter()
            .all(|turn| turn["job"]["settlement"]["state"] != "unsettled")
    });
    host.inspect(|db| {
        let db = db.clone();
        db_wait(async move {
            let a = db.job(id(&a_turn)).await.unwrap().unwrap();
            let b = db.job(id(&b_turn)).await.unwrap().unwrap();
            assert_eq!(a.outcome.unwrap().cause, Some(OutcomeCause::Abandoned));
            assert_eq!(b.outcome.unwrap().cause, Some(OutcomeCause::Abandoned));
            assert_eq!(a.stop_confirmed_by, Some(StopProof::User));
            assert_eq!(b.stop_confirmed_by, Some(StopProof::Proof));
        });
    });
}

#[test]
#[ignore = "real host acceptance"]
fn scenario_06_stop_keeps_queued_input_and_close_voids_the_tree() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let stop_gate = Arc::new(AtomicBool::new(false));
    let held_stop = stop_gate.clone();
    let queued_seen = Arc::new(AtomicBool::new(false));
    let seen = queued_seen.clone();
    let mut host = Host::new(
        "stop-close",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if body.to_string().contains("queued while running") {
                seen.store(true, Ordering::SeqCst);
            }
            if after_tools(body) {
                return done("Dispatched");
            }
            if marker == "K3:S6STOP" && n == 0 {
                return held(done("Stop me"), &held_stop);
            }
            if marker == "K3:S6TREE" && n == 0 {
                return calls(
                    "Child",
                    vec![dispatch(
                        0,
                        &state.agents[0],
                        &state.project,
                        "K3:S6C",
                        false,
                    )],
                );
            }
            if marker == "K3:S6C" && n == 0 {
                return calls(
                    "Approval",
                    vec![call(
                        0,
                        "workspace_write_file",
                        json!({"operation":"create","path":"never.txt","content":"never"}),
                    )],
                );
            }
            done("Done")
        },
        json!([]),
    );
    let child_agent = host.agent("approval child", &[], &["writes"], Some("edit"));
    let root_agent = host.agent("root", std::slice::from_ref(&child_agent), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child_agent];
        state.project = host.project.clone();
    }
    let (root, running) = host.submit(&root_agent, "K3:S6STOP", None);
    wait_for(|| state.lock().unwrap().counts.contains_key("K3:S6STOP"));
    let (_, queued) = host.submit(&root_agent, "queued while running", Some(&root));
    host.post(&format!("conversations/{root}/stop"), json!({}));
    wait_for(|| host.terminal(&running) && host.terminal(&queued));
    let (_, next) = host.submit(&root_agent, "Read my queued message", Some(&root));
    wait_for(|| host.terminal(&next));
    assert!(queued_seen.load(Ordering::SeqCst));
    stop_gate.store(true, Ordering::SeqCst);
    let (tree, _) = host.submit(&root_agent, "K3:S6TREE", None);
    wait_for(|| !host.cards(&tree).is_empty());
    let card = host.cards(&tree)[0].clone();
    host.post(&format!("conversations/{tree}/close"), json!({}));
    wait_for(|| host.get(&format!("jobs/{tree}"))["job"]["settlement"]["state"] != "unsettled");
    assert!(host.cards(&tree).is_empty());
    assert!(!host.workspace.join("never.txt").exists());
    let approval = card["approval"].as_array().unwrap();
    let path = format!(
        "approvals/job:{}/{}/{}",
        approval[0], approval[1], approval[2]
    );
    let (status, reply) = request(
        host.address(),
        "POST",
        &path,
        Some(&json!({"decision":"approve"})),
    );
    assert_eq!(status, 422);
    assert_eq!(reply["error"]["code"], "approval_decided");
    let approval_turn = approval[0].as_u64().unwrap();
    let request = approval[1].as_u64().unwrap() as u32;
    let index = approval[2].as_u64().unwrap() as u32;
    host.inspect(|db| { let db=db.clone(); db_wait(async move {
        let stopped=db.job(id(&running)).await.unwrap().unwrap(); assert_eq!(stopped.outcome.as_ref().unwrap().kind,OutcomeKind::Unknown); assert!(matches!(stopped.outcome.unwrap().cause,Some(OutcomeCause::Stopped { .. })));
        assert!(db.turn_entries(id(&queued)).await.unwrap().iter().any(|(_,entry)|matches!(&entry.entry,Entry::UserMessage { text,.. } if text=="queued while running")));
        assert!(db.job(id(&tree)).await.unwrap().unwrap().subtree_settled);
        assert!(matches!(db.tool_evidence(ocg::persistence::JobId::new(approval_turn),None,100).await.unwrap()[0].decision.fact,ToolDecision::AwaitingApproval(_)));
    }); });
    let directory = DataDirectory::bootstrap(host.data.clone().into_os_string()).unwrap();
    let database = redb::Database::open(directory.database_path().join("ocg.redb")).unwrap();
    let tx = database.begin_read().unwrap();
    let table = tx
        .open_table(TableDefinition::<(u64, u32, u32), &[u8]>::new("approval"))
        .unwrap();
    let row = table.get((approval_turn, request, index)).unwrap().unwrap();
    assert_eq!(row.value()[2], 4, "Voided decision tag");
}

// Scenario 7 is deferred until the first system Job exists; it is not a passed scenario.

#[test]
#[ignore = "real host acceptance"]
fn scenario_08_failing_child_loop_holds_parent_only_and_user_message_lifts_it() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let looping = Arc::new(AtomicBool::new(true));
    let loop_script = looping.clone();
    let host = Host::new(
        "failure-loop",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if marker == "K3:S8C" {
                return Response {
                    status: 400,
                    gate: None,
                    body: json!({"error":{"message":"permanent scripted failure"}}).to_string(),
                };
            }
            if after_tools(body) {
                state.remember(body);
                return done("Continue");
            }
            if marker == "K3:S8R" && n == 0 {
                return calls(
                    "Child",
                    vec![dispatch(
                        0,
                        &state.agents[0],
                        &state.project,
                        "K3:S8C",
                        true,
                    )],
                );
            }
            if loop_script.load(Ordering::SeqCst) {
                return calls(
                    "Retry by message",
                    vec![call(
                        0,
                        "job_message",
                        json!({"conversation":state.children[0],"text":"Try again"}),
                    )],
                );
            }
            done("User ended the loop")
        },
        json!([]),
    );
    let child_agent = host.agent("failing child", &[], &[], None);
    let root_agent = host.agent("root", std::slice::from_ref(&child_agent), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child_agent.clone()];
        state.project = host.project.clone();
    }
    let (root, _) = host.submit(&root_agent, "K3:S8R", None);
    wait_for(|| {
        host.cards(&root)
            .iter()
            .any(|card| card["kind"] == "loop_hold")
    });
    let child = host.children(&root)[0]["id"].as_str().unwrap().to_owned();
    let (_, turn) = host.submit(&child_agent, "Child can still run", Some(&child));
    wait_for(|| host.terminal(&turn));
    assert_eq!(
        host.get(&format!("jobs/{turn}"))["job"]["settlement"]["state"],
        "failed"
    );
    assert!(
        host.cards(&root)
            .iter()
            .any(|card| card["kind"] == "loop_hold")
    );
    looping.store(false, Ordering::SeqCst);
    let (_, turn) = host.submit(&root_agent, "I am here", Some(&root));
    wait_for(|| host.terminal(&turn));
    assert!(
        !host
            .cards(&root)
            .iter()
            .any(|card| card["kind"] == "loop_hold")
    );
}

#[test]
#[ignore = "real host acceptance"]
fn scenario_09_ten_completions_batch_once_then_eight_actions_hold() {
    let state = Arc::new(Mutex::new(ScriptState::default()));
    let script = state.clone();
    let gate = Arc::new(AtomicBool::new(false));
    let held_root = gate.clone();
    let looping = Arc::new(AtomicBool::new(false));
    let loop_script = looping.clone();
    let host = Host::new(
        "ten-children",
        move |body| {
            let marker = marker(body);
            let mut state = script.lock().unwrap();
            let n = state.count(&marker);
            if marker == "K3:S9C" {
                return done("Child done");
            }
            if after_tools(body) {
                state.remember(body);
                return if n == 1 {
                    held(done("Dispatched ten"), &held_root)
                } else {
                    done("Messaged")
                };
            }
            if marker == "K3:S9R" && n == 0 {
                return calls(
                    "Ten children",
                    (0..10)
                        .map(|i| dispatch(i, &state.agents[0], &state.project, "K3:S9C", true))
                        .collect(),
                );
            }
            if loop_script.load(Ordering::SeqCst) {
                return calls(
                    "Act on child",
                    vec![call(
                        0,
                        "job_message",
                        json!({"conversation":state.children[0],"text":"One more"}),
                    )],
                );
            }
            done("Report the batch")
        },
        json!([]),
    );
    let child = host.agent("child", &[], &[], None);
    let root_agent = host.agent("root", std::slice::from_ref(&child), &[], None);
    {
        let mut state = state.lock().unwrap();
        state.agents = vec![child];
        state.project = host.project.clone();
    }
    let (root, first) = host.submit(&root_agent, "K3:S9R", None);
    wait_for(|| {
        host.children(&root).len() == 10
            && host.children(&root).iter().all(|child| {
                host.turns(child["id"].as_str().unwrap())[0]["job"]["settlement"]["state"]
                    == "completed"
            })
    });
    assert!(!host.terminal(&first));
    let turns = host.turns(&root);
    assert_eq!(turns.len(), 2);
    assert_eq!(
        turns[0]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["kind"] == "notice")
            .count(),
        10
    );
    gate.store(true, Ordering::SeqCst);
    wait_for(|| {
        host.turns(&root)
            .iter()
            .all(|turn| turn["job"]["settlement"]["state"] == "completed")
    });
    assert!(host.cards(&root).is_empty());
    let report_turn = host.turns(&root)[0]["job"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        host.get(&format!("jobs/{report_turn}/tools"))["tools"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    looping.store(true, Ordering::SeqCst);
    host.submit(&root_agent, "Start acting", Some(&root));
    wait_for(|| {
        host.cards(&root)
            .iter()
            .any(|card| card["kind"] == "loop_hold")
    });
    let counted = host
        .turns(&root)
        .iter()
        .filter(|turn| {
            turn["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["kind"] == "notice")
                && turn["job"]["settlement"]["state"] == "completed"
        })
        .filter(|turn| {
            let id = turn["job"]["id"].as_str().unwrap();
            host.get(&format!("jobs/{id}/tools"))["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|call| call["name"] == "job_message")
        })
        .count();
    assert_eq!(
        counted, 8,
        "reporting ten completions did not consume a loop round"
    );
    eprintln!("scenario 7: deferred until the first system Job exists");
}
