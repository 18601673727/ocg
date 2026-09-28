//! CLI-level tests for `ocg serve`.
//!
//! These spawn the real `ocg` binary, read the announced loopback address from
//! its stdout, and drive the server over a raw TCP socket. That makes the test a
//! genuine cross-process check: the server process commits the mutation and an
//! independent authority reader observes it afterward.

use ocg::orchestration::{mission, Mission};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct ChildServer {
    child: Child,
    addr: SocketAddr,
}

impl Drop for ChildServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    // `serve` requires a structurally valid OCG Profile; it never bootstraps one.
    std::fs::write(
        dir.path().join(".ocg.yaml"),
        serde_yaml_ng::to_string(&serde_json::json!({
            "profile": {"origin": "new"},
            "models": {
                "providers": {"local": {"label": "Local", "placeholder": false}},
                "models": {"local": {"provider": "local", "id": "local", "placeholder": false}}
            }
        }))
        .unwrap(),
    )
    .unwrap();
    dir
}

fn base_command(work: &Path, project: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(project)
        .env("OCG_USER_CONFIG", work.join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE_BIN")
        .stdin(Stdio::null());
    command
}

fn start_server(work: &Path, project: &Path) -> ChildServer {
    let mut child = base_command(work, project)
        .args(["serve", "--addr", "127.0.0.1:0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ocg serve");

    let stdout = child.stdout.take().expect("child stdout");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => {
                    let _ = sender.send(None);
                    return;
                }
                Ok(_) => {
                    if line.contains("listening on") {
                        let _ = sender.send(Some(line.trim().to_string()));
                        return;
                    }
                }
            }
        }
    });

    let announced = receiver
        .recv_timeout(Duration::from_secs(15))
        .expect("ocg serve did not announce in time")
        .expect("ocg serve exited before announcing an address");
    let address = announced
        .rsplit("http://")
        .next()
        .expect("announced address")
        .trim()
        .to_string();
    let addr: SocketAddr = address.parse().expect("announced loopback address");

    ChildServer { child, addr }
}

fn http(addr: SocketAddr, method: &str, target: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut request =
        format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    stream.write_all(request.as_bytes()).unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response terminator");
    let head = String::from_utf8_lossy(&bytes[..split]);
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let body = String::from_utf8_lossy(&bytes[split + 4..]).into_owned();
    (status, body)
}

fn fixture_mission(project: &Path) -> String {
    let admitted = Mission::admit("task-cli-serve-0001", "task", "session-1", 1);
    let mission_id = admitted.mission_id.clone();
    mission::save(project, &admitted).unwrap();
    mission_id
}

#[test]
fn serve_refuses_non_loopback_and_unknown_options() {
    let work = project();
    let refused = base_command(work.path(), work.path())
        .args(["serve", "--addr", "0.0.0.0:0"])
        .output()
        .expect("run ocg serve");
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("loopback"),
        "stderr={}",
        String::from_utf8_lossy(&refused.stderr)
    );

    let unknown = base_command(work.path(), work.path())
        .args(["serve", "--nope"])
        .output()
        .expect("run ocg serve");
    assert!(!unknown.status.success());
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("unknown serve option"),
        "stderr={}",
        String::from_utf8_lossy(&unknown.stderr)
    );
}

#[test]
fn cli_serve_serves_snapshot_and_accepts_a_cross_process_mutation() {
    let work = project();
    let mission_id = fixture_mission(work.path());
    let server = start_server(work.path(), work.path());

    let (status, body) = http(server.addr, "GET", "/api/v1/snapshot", None);
    assert_eq!(status, 200, "body={body}");
    let snapshot: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(snapshot["snapshot"]["missions"]
        .get(mission_id.as_str())
        .is_some());

    let budget_target = format!("/api/v1/budgets/{mission_id}");
    let (status, body) = http(
        server.addr,
        "PUT",
        &budget_target,
        Some("{\"limit_micros\":777000,\"currency\":\"USD\"}"),
    );
    assert_eq!(status, 200, "body={body}");
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(value["budget"]["hard_limit_micros"], 777000);

    // The server process committed the mutation; this process observes it in the
    // durable authority.
    let reloaded = mission::load(work.path(), &mission_id).unwrap().unwrap();
    assert_eq!(reloaded.budget.receipt().hard_limit_micros, Some(777000));

    // A read-only CLI process in yet another process sees the same durable state.
    let listed = base_command(work.path(), work.path())
        .args(["budget", "--json"])
        .output()
        .expect("run ocg budget");
    assert!(listed.status.success());
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let missions = listed["missions"].as_array().unwrap();
    assert!(missions
        .iter()
        .any(|mission| mission["hard_limit_micros"] == 777000));
}
