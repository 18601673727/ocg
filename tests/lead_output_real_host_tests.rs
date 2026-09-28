//! Gated real OpenCode V2 latest-output smoke.
//!
//! This test is opt-in because it starts a real OpenCode server and performs
//! provider-backed completions. It uses a disposable project/session and a
//! short, fixed local server credential; no credential or endpoint is printed.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::Controller;
use ocg::orchestration::plugin;
use ocg::process::FakeGitHost;
use ocg::runtime::compat::v2_client::{ServiceRegistration, V2SessionClient};
use ocg::runtime::compat::LeadSelection;
use ocg::runtime::lifecycle::RuntimeAdapter;
use ocg::verification::config::VerificationConfig;
use serde_json::Value;
use std::ffi::OsString;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GATE: &str = "OCG_REAL_LATEST_OUTPUT_SMOKE";
const PROGRAM_ENV: &str = "OCG_REAL_OPENCODE";
const PASSWORD: &str = "ocg-real-output-smoke-local";
const TIMEOUT: Duration = Duration::from_secs(120);

struct RealServer {
    child: Child,
    url: String,
}

impl RealServer {
    fn start(program: &Path, config_content: &str, extra_env: &[(OsString, OsString)]) -> Self {
        let mut command = Command::new(program);
        command
            .args(["serve", "--hostname", "127.0.0.1", "--port", "0"])
            .env("OPENCODE_CONFIG_CONTENT", config_content)
            .env("OPENCODE_SERVER_PASSWORD", PASSWORD)
            .env_remove("OPENCODE_CONFIG")
            .env_remove("OCG_V2_SERVER_URL")
            .env_remove("OCG_V2_SERVER_PASSWORD")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("start real OpenCode V2 server");
        let stdout = child.stdout.take().expect("server stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line).expect("read server handshake");
        let url = line
            .trim()
            .strip_prefix("server listening on ")
            .unwrap_or_else(|| panic!("real OpenCode V2 did not report its local endpoint"))
            .to_string();
        thread::spawn(move || {
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 {
                line.clear();
            }
        });
        Self { child, url }
    }
}

impl Drop for RealServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn visible_text(message: &Value) -> Option<String> {
    let content = message.get("content")?;
    let text = match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => {
            let mut values = Vec::new();
            for part in parts {
                if part.get("type").and_then(Value::as_str) == Some("text") {
                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                        values.push(text.to_string());
                    }
                }
            }
            values.join("\n\n")
        }
        _ => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

fn latest_text(messages: &[Value], marker: &str) -> Option<String> {
    messages.iter().rev().find_map(|message| {
        if message.get("type").and_then(Value::as_str) != Some("assistant")
            || message.get("finish").and_then(Value::as_str) != Some("stop")
        {
            return None;
        }
        visible_text(message).filter(|text| text.contains(marker))
    })
}

fn wait_for_report(path: &Path, expected: &str) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if fs::read_to_string(path).is_ok_and(|text| text == expected) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("real OpenCode V2 completion did not persist the expected visible report");
}

fn wait_for_visible_text(client: &V2SessionClient, session: &str, marker: &str) -> String {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        let messages = client
            .context_messages(session)
            .expect("read real OpenCode V2 context");
        if let Some(text) = latest_text(&messages, marker) {
            return text;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("real OpenCode V2 completion did not expose its visible assistant text");
}

fn run_client(
    program: &Path,
    server: &RealServer,
    config_dir: &Path,
    session: &str,
    agent: &str,
    marker: &str,
) -> Output {
    Command::new(program)
        .args(["run", "--server"])
        .arg(&server.url)
        .args([
            "--session",
            session,
            "--agent",
            agent,
            "--model",
            "opencode/space-bunny-free",
        ])
        .arg(marker)
        .env("OPENCODE_SERVER_PASSWORD", PASSWORD)
        .env("OPENCODE_CONFIG_DIR", config_dir)
        .output()
        .expect("run real OpenCode V2 client")
}

#[test]
fn real_current_root_completion_replaces_latest_output_even_when_agent_is_build() {
    if std::env::var(GATE).as_deref() != Ok("1") {
        return;
    }
    let program =
        std::env::var(PROGRAM_ENV).expect("OCG_REAL_OPENCODE must name a real OpenCode binary");
    let program = PathBuf::from(program);
    assert!(
        program.is_file(),
        "OCG_REAL_OPENCODE must name a real OpenCode binary"
    );

    let root = tempfile::tempdir().expect("disposable real-output project");
    let project = root.path().to_path_buf();
    fs::create_dir_all(project.join("src")).expect("create project source");
    fs::write(project.join("src/main.rs"), "fn main() {}\n").expect("write project source");
    fs::write(project.join(".ocg.yaml"), "{}\n").expect("write project config");

    let plugin_path =
        plugin::materialize_v2_with(&project, ocg::runtime::compat::v2_adapter().plugin_source())
            .expect("materialize real V2 plugin");
    assert!(plugin_path.is_file());
    let config_dir = plugin::v2_config_dir(&project);
    let config_content = serde_json::json!({
        "agent": {
            "lead-high": {
                "mode": "primary",
                "model": "opencode/space-bunny-free",
                "steps": 1,
                "prompt": "Reply exactly with the requested marker."
            },
            "build": {
                "mode": "primary",
                "model": "opencode/space-bunny-free",
                "steps": 1,
                "prompt": "Reply exactly with the requested marker."
            }
        }
    })
    .to_string();
    let env = vec![
        (
            OsString::from("OCG_BRIDGE"),
            OsString::from(env!("CARGO_BIN_EXE_ocg")),
        ),
        (
            OsString::from("OCG_PROJECT"),
            project.clone().into_os_string(),
        ),
        (
            OsString::from("OCG_ORCHESTRATION_ENABLED"),
            OsString::from("1"),
        ),
        (
            OsString::from("OCG_REPORTS_LATEST_LEAD_OUTPUT"),
            OsString::from("1"),
        ),
        (
            OsString::from("OCG_CONTEXT_GOVERNOR_ENABLED"),
            OsString::from("0"),
        ),
        (
            OsString::from("OCG_USER_CONFIG"),
            root.path().join("no-user.yaml").into_os_string(),
        ),
        (
            OsString::from("OCG_PROJECT_CONFIG"),
            project.join(".ocg.yaml").into_os_string(),
        ),
        (
            OsString::from("OPENCODE_CONFIG_DIR"),
            config_dir.clone().into_os_string(),
        ),
    ];
    let server = RealServer::start(&program, &config_content, &env);
    let registration = ServiceRegistration::new(server.url.clone(), PASSWORD);
    let mut client = V2SessionClient::connect(&registration, project.to_string_lossy().as_ref())
        .expect("connect real OpenCode V2 client");
    let session =
        RuntimeAdapter::resolve_execution(&mut client).expect("resolve real root session");
    let lead = LeadSelection {
        level: "high".to_string(),
        agent: "lead-high".to_string(),
        provider_id: "opencode".to_string(),
        model_id: "space-bunny-free".to_string(),
        variant: None,
    };
    RuntimeAdapter::prepare_execution(&mut client, &session, &lead.runtime_profile())
        .expect("prepare real root Lead");

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
        .admit_user_task(session.as_str(), "real latest output smoke")
        .expect("admit real root Mission");

    let marker_one = "OCG_REAL_LATEST_OUTPUT_ONE_20260924";
    let first = run_client(
        &program,
        &server,
        &config_dir,
        session.as_str(),
        "lead-high",
        marker_one,
    );
    assert!(
        first.status.success(),
        "first real OpenCode V2 completion failed"
    );
    let first_visible = wait_for_visible_text(&client, session.as_str(), marker_one);
    let report = ocg::reports::latest_lead_output_path(&project);
    wait_for_report(&report, &first_visible);
    assert_eq!(fs::read_to_string(&report).unwrap(), first_visible);

    let marker_two = "OCG_REAL_LATEST_OUTPUT_TWO_20260924";
    let second = run_client(
        &program,
        &server,
        &config_dir,
        session.as_str(),
        "build",
        marker_two,
    );
    assert!(
        second.status.success(),
        "second real OpenCode V2 completion failed"
    );
    let second_info = client
        .session_info(session.as_str())
        .expect("read switched root session");
    assert_eq!(
        second_info.get("agent").and_then(Value::as_str),
        Some("build")
    );
    let second_visible = wait_for_visible_text(&client, session.as_str(), marker_two);
    wait_for_report(&report, &second_visible);
    assert_eq!(fs::read_to_string(&report).unwrap(), second_visible);
}
