//! Opt-in disposable OpenCode 2.0.15 provider-gateway proof.
use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::budget::BudgetConfig;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::Controller;
use ocg::orchestration::plugin;
use ocg::process::FakeGitHost;
use ocg::provider_gateway::{GatewayRoute, ProviderGateway};
use ocg::provider_transport::ProviderTransportConfig;
use ocg::runtime::compat::v2_client::{ServiceRegistration, V2SessionClient};
use ocg::runtime::compat::LeadSelection;
use ocg::runtime::lifecycle::RuntimeAdapter;
use ocg::verification::config::VerificationConfig;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const PASSWORD: &str = "disposable-gateway-real-host";

struct Server(Child, String);
impl Drop for Server {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn fake_provider(tool_file: String) -> (String, Arc<Mutex<Vec<Value>>>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = format!("http://{}/v1", listener.local_addr().unwrap());
    let captured = Arc::new(Mutex::new(Vec::new()));
    let requests = captured.clone();
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(80);
        while Instant::now() < deadline {
            let (mut socket, _) = match listener.accept() {
                Ok(found) => found,
                Err(_) => {
                    thread::sleep(Duration::from_millis(20));
                    continue;
                }
            };
            socket
                .set_read_timeout(Some(Duration::from_secs(8)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 4096];
            let end = loop {
                let count = socket.read(&mut chunk).unwrap_or(0);
                if count == 0 {
                    break None;
                }
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break Some(i + 4);
                }
                if bytes.len() > 65536 {
                    break None;
                }
            };
            let Some(end) = end else {
                continue;
            };
            let header = String::from_utf8_lossy(&bytes[..end]).into_owned();
            let length = header
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if length > 2_000_000 {
                continue;
            }
            while bytes.len() - end < length {
                let count = socket.read(&mut chunk).unwrap_or(0);
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            if bytes.len() - end != length {
                continue;
            }
            let body: Value = serde_json::from_slice(&bytes[end..]).unwrap_or_default();
            let header_lower = header.to_ascii_lowercase();
            assert!(
                !header_lower.contains("x-ocg-"),
                "internal metadata escaped gateway"
            );
            requests.lock().unwrap().push(body.clone());
            let base = json!({"id":"chatcmpl-gateway-proof","object":"chat.completion.chunk","created":1,"model":"capture-test-upstream"});
            let user = body["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|m| m["role"] == "user")
                .filter_map(|m| m["content"].as_str())
                .next_back()
                .unwrap_or("");
            let tool_result = body["messages"]
                .as_array()
                .is_some_and(|messages| messages.iter().any(|m| m["role"] == "tool"));
            let frames = if user.contains("GATEWAY_WORKER") && !tool_result {
                vec![
                    json!({"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_gateway_worker","type":"function","function":{"name":"subagent","arguments":"{\"agent\":\"gateway-worker\",\"description\":\"Gateway worker proof\",\"prompt\":\"Return exactly GATEWAY_WORKER_OK.\"}"}}]},"finish_reason":null}]}),
                    json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}),
                ]
            } else if user.contains("GATEWAY_TOOL") && !tool_result {
                vec![
                    json!({"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_gateway_read","type":"function","function":{"name":"read","arguments":json!({"path":tool_file}).to_string()}}]},"finish_reason":null}]}),
                    json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}),
                ]
            } else {
                vec![
                    json!({"choices":[{"index":0,"delta":{"role":"assistant","content":"GATEWAY_PROOF_OK"},"finish_reason":null}]}),
                    json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
                ]
            };
            let _ = socket.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            );
            for frame in frames {
                let mut item = base.clone();
                item["choices"] = frame["choices"].clone();
                let _ = socket.write_all(format!("data: {item}\n\n").as_bytes());
            }
            let mut usage = base;
            usage["choices"] = json!([]);
            usage["usage"] = json!({"prompt_tokens":3,"completion_tokens":2,"total_tokens":5});
            let _ = socket.write_all(format!("data: {usage}\n\ndata: [DONE]\n\n").as_bytes());
        }
    });
    (address, captured, handle)
}

fn isolated(
    command: &mut Command,
    root: &Path,
    config_dir: &Path,
    content: &str,
    invocation: &str,
) {
    command
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("home/.config"))
        .env("XDG_DATA_HOME", root.join("home/.local/share"))
        .env("XDG_STATE_HOME", root.join("home/.local/state"))
        .env("XDG_CACHE_HOME", root.join("home/.cache"))
        .env("OPENCODE_CONFIG_DIR", config_dir)
        .env("OPENCODE_CONFIG_CONTENT", content)
        .env("OPENCODE_SERVER_PASSWORD", PASSWORD)
        .env("OCG_PROVIDER_INVOCATION", invocation)
        .env("OCG_PROVIDER_ID", "capture")
        .env("OCG_ORCHESTRATION_ENABLED", "0")
        .env_remove("OPENCODE_CONFIG")
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_USER_CONFIG")
        .env_remove("OCG_V2_SERVER_URL")
        .env_remove("OCG_V2_SERVER_PASSWORD");
}

fn real_case(worker: bool, tool: bool, hard_budget: bool) {
    if std::env::var("OCG_REAL_GATEWAY_SMOKE").as_deref() != Ok("1") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::write(project.join(".ocg.yaml"), "{}\n").unwrap();
    std::fs::write(project.join("sample.txt"), "GATEWAY_READ_OK\n").unwrap();
    plugin::materialize_v2_with(&project, ocg::runtime::compat::v2_adapter().plugin_source())
        .unwrap();
    let config_dir = plugin::v2_config_dir(&project);
    let (upstream, requests, _provider) =
        fake_provider(project.join("sample.txt").to_string_lossy().to_string());
    let route = GatewayRoute {
        provider: "capture".into(),
        model: "capture-test-upstream".into(),
        upstream: ProviderTransportConfig::new(upstream, "upstream-test-secret"),
    };
    let gateway = ProviderGateway::start(
        project.clone(),
        project.to_string_lossy().to_string(),
        route,
        if hard_budget {
            BudgetConfig {
                currency: Some("USD".into()),
                hard_limit_micros: Some(120),
                estimated_operation_cost_micros: Some(60),
                require_quota: false,
            }
        } else {
            BudgetConfig::default()
        },
    )
    .unwrap();
    let config = json!({
        "model":"capture/test#high", "default_agent":"root",
        "provider":{"capture":{"npm":"@ai-sdk/openai-compatible","name":"Capture",
            "options":{"baseURL":gateway.url(),"apiKey":gateway.token()},
            "models":{"test":{"id":"capture-test-upstream","name":"Capture Test","limit":{"context":200000,"output":32000},
                "variants":{"high":{"reasoning_effort":"high"}}}}}},
        "agent":{"root":{"mode":"primary","model":"capture/test#high","steps":4,"prompt":"Answer briefly. You may use the read and subagent tools."},
            "gateway-worker":{"mode":"subagent","model":"capture/test#high","description":"Gateway proof worker","prompt":"Reply exactly GATEWAY_WORKER_OK."}},
        "permission":{"subagent":{"gateway-worker":"allow"}}
    }).to_string();
    let program = std::env::var("OCG_REAL_OPENCODE").unwrap_or_else(|_| "opencode".into());
    let mut command = Command::new(&program);
    command
        .args(["serve", "--hostname", "127.0.0.1", "--port", "0"])
        .current_dir(&project)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    isolated(
        &mut command,
        root.path(),
        &config_dir,
        &config,
        gateway.invocation(),
    );
    let mut child = command.spawn().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let address = line
        .trim()
        .strip_prefix("server listening on ")
        .expect("real OpenCode server handshake")
        .to_string();
    thread::spawn(move || {
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            line.clear();
        }
    });
    let server = Server(child, address);
    let registration = ServiceRegistration::new(&server.1, PASSWORD);
    gateway.attach_runtime(registration.clone()).unwrap();
    let mut client = V2SessionClient::connect(&registration, project.to_string_lossy()).unwrap();
    let session = RuntimeAdapter::resolve_execution(&mut client).unwrap();
    RuntimeAdapter::prepare_execution(
        &mut client,
        &session,
        &LeadSelection {
            level: "high".into(),
            agent: "root".into(),
            provider_id: "capture".into(),
            model_id: "test".into(),
            variant: Some("high".into()),
        }
        .runtime_profile(),
    )
    .unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let controller = Controller::new(
        &project,
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        VerificationConfig::default(),
        &git,
        &clock,
    );
    controller
        .admit_user_task(session.as_str(), "gateway proof")
        .unwrap();
    let run = |prompt: &str| {
        let mut cmd = Command::new(&program);
        cmd.args([
            "run",
            "--server",
            &server.1,
            "--session",
            session.as_str(),
            "--title",
            "Gateway proof",
            "--auto",
            "--format",
            "json",
            "--agent",
            "root",
            prompt,
        ])
        .current_dir(&project);
        isolated(
            &mut cmd,
            root.path(),
            &config_dir,
            &config,
            gateway.invocation(),
        );
        let output = cmd.output().unwrap();
        (
            output.status,
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let (status, stdout, stderr) = run(if worker {
        "GATEWAY_WORKER"
    } else if tool {
        "GATEWAY_TOOL"
    } else {
        "Return exactly GATEWAY_PROOF_OK."
    });
    if hard_budget {
        assert_eq!(
            requests.lock().unwrap().len(),
            0,
            "unknown upper bound must prevent all upstream requests: {stderr} {stdout}"
        );
        return;
    }
    assert!(
        status.success(),
        "root failed: {stderr} {stdout}; upstream={:?}",
        requests.lock().unwrap().len()
    );
    let count = requests.lock().unwrap().len();
    if tool {
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .any(
                    |body| body["messages"].as_array().is_some_and(|messages| messages
                        .iter()
                        .any(|message| message["role"] == "tool"
                            && message["content"].to_string().contains("GATEWAY_READ_OK")))
                ),
            "tool result never reached upstream: {stderr} {stdout}"
        );
        assert!(
            stdout.contains("GATEWAY_PROOF_OK"),
            "final output missing: {stderr} {stdout}"
        );
    } else if worker {
        assert!(
            count >= 2,
            "worker request did not reach upstream: {stderr} {stdout}"
        );
        let snapshot = ocg::orchestration::replay::SnapshotService::open(&project)
            .unwrap()
            .snapshot()
            .unwrap();
        let dispatches: Vec<_> = snapshot.dispatches.values().collect();
        assert!(dispatches
            .iter()
            .any(|d| d.execution_id == session.as_str()));
        assert!(dispatches
            .iter()
            .any(|d| d.execution_id != session.as_str() && d.root_id == session.as_str()));
        assert!(dispatches
            .iter()
            .all(|d| d.mission_id == dispatches[0].mission_id));
    } else {
        assert!(count == 1 || count == 2, "root plus optional title should send at most two requests; got {count}: {stderr} {stdout}");
    }
}

#[test]
fn real_root_unknown_cost_blocks_before_upstream() {
    real_case(false, false, true);
}

#[test]
fn real_worker_lineage_through_gateway() {
    real_case(true, false, false);
}

#[test]
fn real_tool_result_through_gateway() {
    real_case(false, true, false);
}

#[test]
fn real_uncapped_root_through_gateway() {
    real_case(false, false, false);
}
