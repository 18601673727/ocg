//! End-to-end tests for effective runtime-state ownership.
//!
//! Every test runs the real `ocg` binary against an isolated HOME and a fake
//! OpenCode executable. The fake speaks just enough of the OpenCode 2 HTTP
//! surface for OCG to start its own private runtime, apply a Lead contract and
//! read the effective state back. Nothing here touches the network, a real
//! daemon or a credential store.
//!
//! The fake echoes back exactly the model OCG sent, so the reported Effective
//! state is *derived from* the observable runtime transaction rather than
//! asserted against a constant.

mod common;

use common::{project_profile, read_yaml, write_yaml, TestDir};
use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path).expect("metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod");
}

/// The state a fake runtime accumulates while OCG talks to it.
#[derive(Default)]
struct RuntimeState {
    /// Every `METHOD path` the fake served.
    requests: Mutex<Vec<String>>,
    /// The last `model` object OCG sent to `POST /api/session/{id}/model`.
    model: Mutex<Option<Value>>,
    /// The last agent OCG selected.
    agent: Mutex<Option<String>>,
}

impl RuntimeState {
    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests").clone()
    }

    fn model(&self) -> Option<Value> {
        self.model.lock().expect("model").clone()
    }
}

/// A fake OpenCode 2 runtime. When `ready` is false the executable still prints
/// the startup handshake, but nothing listens on the advertised port.
struct FakeRuntime {
    program: PathBuf,
    state: Arc<RuntimeState>,
}

/// Read one HTTP request and return `(method, path, body)`.
fn read_request(stream: &mut std::net::TcpStream) -> Option<(String, String, String)> {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 1024];
    loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&buffer[..read]);
        if let Some(end) = find_subslice(&raw, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&raw[..end]).into_owned();
            let length = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            let body_start = end + 4;
            let mut body = raw[body_start..].to_vec();
            while body.len() < length {
                let read = stream.read(&mut buffer).ok()?;
                if read == 0 {
                    break;
                }
                body.extend_from_slice(&buffer[..read]);
            }
            let mut parts = head.lines().next()?.split_whitespace();
            let method = parts.next()?.to_string();
            let path = parts.next()?.to_string();
            return Some((method, path, String::from_utf8_lossy(&body).into_owned()));
        }
    }
    None
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn respond(stream: &mut std::net::TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        if status == 204 { "No Content" } else { "OK" },
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// Serve the routes `V2SessionClient` uses, echoing the applied model back.
fn serve(state: Arc<RuntimeState>, listener: TcpListener) {
    for stream in listener.incoming().take(64) {
        let Ok(mut stream) = stream else { break };
        let Some((method, path, body)) = read_request(&mut stream) else {
            continue;
        };
        state
            .requests
            .lock()
            .expect("requests")
            .push(format!("{method} {path}"));
        if method == "GET" && path.starts_with("/api/config") {
            // Readiness only: the owned-server handshake checks the status of
            // this route and ignores the body. OpenCode 2 exposes no catalogue
            // of config-declared providers, so nothing parses provider/model
            // tokens from it anymore; any 200 answer serves the probe.
            respond(
                &mut stream,
                200,
                &json!([{
                    "info": {
                        "providers": {
                            "openai": {"models": {"gpt-5.6-sol": {}, "gpt-6-astra": {}}},
                            "volcengine-coding-plan": {"models": {"kimi-k2.7-code": {}, "kimi-k3": {}}},
                            "opencode-go": {"models": {"deepseek-v4.1-flash": {}, "glm-5.3-flash": {}, "glm-5.3": {}}}
                        }
                    }
                }])
                .to_string(),
            );
        } else if method == "GET" && path.starts_with("/api/session?") {
            respond(
                &mut stream,
                200,
                &json!({"data": [{"id": "ses_fake"}], "cursor": {}}).to_string(),
            );
        } else if method == "POST" && path.ends_with("/agent") {
            let agent = serde_json::from_str::<Value>(&body).ok().and_then(|value| {
                value
                    .get("agent")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
            *state.agent.lock().expect("agent") = agent;
            respond(&mut stream, 204, "");
        } else if method == "POST" && path.ends_with("/model") {
            let model = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|value| value.get("model").cloned());
            *state.model.lock().expect("model") = model;
            respond(&mut stream, 204, "");
        } else if method == "GET" && path.starts_with("/api/session/") {
            let agent = state
                .agent
                .lock()
                .expect("agent")
                .clone()
                .unwrap_or_else(|| "lead".to_string());
            let model = state.model.lock().expect("model").clone().unwrap_or_else(
                || json!({"id": "gpt-6-astra", "providerID": "openai", "variant": "default"}),
            );
            respond(
                &mut stream,
                200,
                &json!({"data": {"agent": agent, "model": model}}).to_string(),
            );
        } else {
            respond(&mut stream, 404, "{}");
        }
    }
}

/// Build a fake OpenCode executable.
///
/// `Some(port)` advertises a live fake runtime; `None` advertises a dead port
/// so the runtime never becomes ready.
fn fake_runtime(dir: &TestDir, name: &str, ready: bool) -> FakeRuntime {
    let state = Arc::new(RuntimeState::default());
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake runtime");
    let port = listener.local_addr().expect("addr").port();
    if ready {
        let state_for_thread = state.clone();
        std::thread::spawn(move || serve(state_for_thread, listener));
    } else {
        drop(listener);
    }

    let path = dir.join(name);
    let script = format!(
        "#!/bin/sh\n\
         if [ \"$1\" = \"--version\" ]; then echo \"2.0.11\"; exit 0; fi\n\
         if [ \"$1\" = \"models\" ]; then\n\
           printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra \\\n\
             volcengine-coding-plan/kimi-k2.7-code volcengine-coding-plan/kimi-k3 \\\n\
             opencode-go/deepseek-v4.1-flash opencode-go/glm-5.3-flash opencode-go/glm-5.3\n\
           exit 0\n\
         fi\n\
         if [ \"$1\" = \"serve\" ]; then\n\
           echo \"server listening on http://127.0.0.1:{port}\"\n\
           echo \"server password local-secret\"\n\
           sleep 60\n\
           exit 0\n\
         fi\n\
         echo \"fake opencode: unexpected arguments\" >&2\n\
         exit 1\n"
    );
    fs::write(&path, script).expect("write fake opencode");
    #[cfg(unix)]
    make_executable(&path);
    FakeRuntime {
        program: path,
        state,
    }
}

fn base_command(cwd: &Path, dir: &TestDir, user: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(cwd)
        .env("HOME", dir.join("home"))
        .env("PATH", "/usr/bin:/bin")
        .env("OCG_USER_CONFIG", user)
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_USER_CONFIG")
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_API_BASE")
        .env_remove("OCG_CACHE_DIR")
        .env_remove("OCG_TELEMETRY")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("OCG_DISABLE_PROXY")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN");
    command
}

fn run(cwd: &Path, dir: &TestDir, user: &Path, program: &Path, args: &[&str]) -> Output {
    seed_profile(cwd);
    base_command(cwd, dir, user)
        .env("OCG_OPENCODE_BIN", program)
        .args(args)
        .output()
        .expect("run ocg")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn user_path(dir: &TestDir) -> PathBuf {
    dir.join("user.yaml")
}

/// Every runtime-state surface needs an OCG-owned Profile. Seed the project
/// unless the test wrote its own configuration.
fn seed_profile(project: &Path) {
    let path = project.join(".ocg.yaml");
    if !path.is_file() {
        write_yaml(&path, &project_profile());
    }
}

#[test]
fn doctor_effective_reports_configured_resolved_and_effective_from_the_live_runtime() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = user_path(&dir);
    let fake = fake_runtime(&dir, "fake-ok", true);

    let output = run(
        &project,
        &dir,
        &user,
        &fake.program,
        &["doctor", "--effective"],
    );
    let text = stdout(&output);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    assert!(text.contains("Effective State"), "{text}");
    // Status is rendered first, independent of label length.
    assert!(text.contains("[PASS] configured"), "{text}");
    assert!(text.contains("lead on openai/gpt-5.6-sol"), "{text}");
    assert!(text.contains("[PASS] resolved"), "{text}");
    assert!(
        text.contains("[PASS] effective") && text.contains("ses_fake"),
        "the effective state must come from the live session: {text}"
    );
    assert!(
        text.contains("ocg-managed-invocation (pid "),
        "the invocation-owned runtime must be identified without exposing its endpoint: {text}"
    );
    assert!(!text.contains("http://127.0.0.1:"), "{text}");
    // The fake runtime really was used: OCG read the session back.
    assert!(
        fake.state
            .requests()
            .iter()
            .any(|request| request == "GET /api/session/ses_fake"),
        "{:?}",
        fake.state.requests()
    );
}

#[test]
fn doctor_without_effective_never_starts_a_runtime() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = user_path(&dir);
    let fake = fake_runtime(&dir, "fake-ok", true);

    let output = run(&project, &dir, &user, &fake.program, &["doctor"]);
    let text = stdout(&output);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    assert!(
        text.contains("not checked (pass --effective"),
        "the default doctor must say the effective state was not checked: {text}"
    );
    assert!(
        !fake
            .state
            .requests()
            .iter()
            .any(|request| request.contains("/api/")),
        "the default doctor must not talk to a runtime: {:?}",
        fake.state.requests()
    );
}

#[test]
fn status_effective_reports_the_three_states_for_the_active_level() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = user_path(&dir);
    let fake = fake_runtime(&dir, "fake-ok", true);

    let output = run(
        &project,
        &dir,
        &user,
        &fake.program,
        &["status", "--effective"],
    );
    let text = stdout(&output);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    assert!(
        text.contains("Runtime state (selected model gpt-5.6-sol)"),
        "{text}"
    );
    assert!(text.contains("configured       [PASS]"), "{text}");
    assert!(text.contains("resolved         [PASS]"), "{text}");
    assert!(
        text.contains("was not checked against the runtime: OpenCode 2 exposes no model catalogue"),
        "{text}"
    );
    assert!(text.contains("effective        [PASS]"), "{text}");
    assert!(text.contains("openai/gpt-5.6-sol"), "{text}");
}

#[test]
fn a_runtime_that_never_becomes_ready_is_reported_distinctly() {
    use ocg::proxy::{resolve, MapProxyEnv, NoStaticProxy};
    use ocg::runtime::compat::v2_server::{OwnedV2Server, StartupBudget};
    use std::time::Duration;

    let dir = TestDir::new();
    // The executable prints the handshake but nothing listens on the port.
    let fake = fake_runtime(&dir, "fake-dead", false);
    let proxy = resolve(false, &MapProxyEnv::new(), &NoStaticProxy).child_env();
    let error = OwnedV2Server::start_with(
        &fake.program,
        "{}",
        &[],
        &proxy,
        StartupBudget {
            handshake_deadline: Duration::from_secs(1),
            attempts: 2,
            interval: Duration::from_millis(10),
        },
    )
    .err()
    .expect("unready runtime must fail");
    assert!(error.to_string().contains("never became ready"), "{error}");
}

/// The removed `ocg config lead <tier>` switch is replaced by explicit Profile
/// editing. Editing configuration must never start a runtime or apply a model.
#[test]
fn a_profile_edit_reports_the_selection_without_starting_a_runtime() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = user_path(&dir);
    let fake = fake_runtime(&dir, "fake-ok", true);

    let output = run(
        &project,
        &dir,
        &user,
        &fake.program,
        &[
            "config",
            "model",
            "add",
            "selected",
            "openai",
            "gpt-6-astra",
            "--default",
        ],
    );
    let text = stdout(&output);
    assert_eq!(output.status.code(), Some(0), "stderr: {}", stderr(&output));
    assert!(text.contains("\"defaultModel\": \"selected\""), "{text}");
    assert!(!text.contains("http://127.0.0.1:"), "{text}");
    // Configuration editing is offline: it must not contact the runtime.
    assert!(
        !fake
            .state
            .requests()
            .iter()
            .any(|request| request.contains("/api/")),
        "a Profile edit must not activate a runtime: {:?}",
        fake.state.requests()
    );
    assert!(fake.state.model().is_none(), "no model was applied");
}

#[test]
fn a_selected_variant_is_rejected_when_unsupported_and_nothing_is_written() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = user_path(&dir);
    let fake = fake_runtime(&dir, "fake-ok", true);

    let mut config = project_profile();
    config["models"]["models"]["gpt-5.6-sol"]["variant"] = json!("bogus-variant");
    write_yaml(&project.join(".ocg.yaml"), &config);
    let before = read_yaml(&project.join(".ocg.yaml"));

    let output = run(
        &project,
        &dir,
        &user,
        &fake.program,
        &[
            "config",
            "model",
            "add",
            "chosen",
            "openai",
            "gpt-5.6-sol",
            "--default",
        ],
    );
    let stderr = stderr(&output);
    assert!(!output.status.success());
    assert!(stderr.contains("unsupported variant"), "{stderr}");
    assert!(
        before == read_yaml(&project.join(".ocg.yaml")),
        "a rejected edit must write nothing"
    );
    assert!(
        !fake
            .state
            .requests()
            .iter()
            .any(|request| request.contains("/api/")),
        "a rejected edit must not activate a runtime: {:?}",
        fake.state.requests()
    );
}
