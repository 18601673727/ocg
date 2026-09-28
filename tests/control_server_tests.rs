//! Integration tests for the Phase 2B-2 loopback control server.
//!
//! Every test uses a real TCP listener on an ephemeral loopback port and a raw
//! HTTP client, so the request parsing, routing, error envelopes, SSE framing
//! and timeouts are exercised end to end. No external network is touched.

use ocg::control_server::{ControlServer, ServerConfig, MAX_BODY_BYTES};
use ocg::orchestration::{
    mission, policy, ApprovalRequest, ApprovalStatus, Cursor, DomainEvent, Mission, PolicyAction,
    SnapshotConfig, SnapshotService,
};
use ocg::resources::{self, ResourceIdentity, ResourceRegistry};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn mission(index: u64, now: i64) -> Mission {
    Mission::admit(&format!("task-srv-{index:016}"), "task", "session-1", now)
}

fn append_mission(service: &SnapshotService, index: u64) -> Cursor {
    service
        .append(DomainEvent::MissionUpsert {
            mission: mission(index, index as i64),
        })
        .unwrap()
        .expect("a distinct mission is a state change")
}

fn pending_approval(root: &Path, mission: &Mission, now: i64) -> String {
    let approval_id = policy::approval_id(
        &mission.mission_id,
        mission.generation,
        PolicyAction::EnsureExecution,
        None,
    );
    let request = ApprovalRequest {
        approval_id: approval_id.clone(),
        mission_id: mission.mission_id.clone(),
        generation: mission.generation,
        action: PolicyAction::EnsureExecution,
        current_execution_id: None,
        requested_at: now,
    };
    policy::ensure_pending(root, &request).unwrap();
    approval_id
}

// -- raw HTTP client ---------------------------------------------------------

#[derive(Debug)]
struct RawResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl RawResponse {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|error| {
            panic!("response body is not JSON: {error}\nbody={}", self.body)
        })
    }
}

fn send_and_read(addr: SocketAddr, request: &str) -> RawResponse {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .expect("connect to the control server");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).unwrap();
    parse_response(&bytes)
}

fn parse_response(bytes: &[u8]) -> RawResponse {
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response has a header terminator");
    let head = String::from_utf8_lossy(&bytes[..split]);
    let body = String::from_utf8_lossy(&bytes[split + 4..]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    RawResponse {
        status,
        headers,
        body,
    }
}

fn raw_request(
    addr: SocketAddr,
    method: &str,
    target: &str,
    body: Option<&str>,
    extra: &[(&str, &str)],
) -> RawResponse {
    let mut request =
        format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in extra {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    if let Some(body) = body {
        request.push_str(body);
    }
    send_and_read(addr, &request)
}

// -- SSE client --------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct SseFrame {
    id: Option<String>,
    event: Option<String>,
    data: Option<String>,
}

struct SseReader {
    stream: TcpStream,
    buffer: Vec<u8>,
    deadline: Instant,
}

fn open_events(addr: SocketAddr, target: &str, last_event_id: Option<&str>) -> SseReader {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .expect("connect to the event stream");
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    let mut request =
        format!("GET {target} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n");
    if let Some(value) = last_event_id {
        request.push_str(&format!("Last-Event-ID: {value}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).unwrap();

    let mut buffer = Vec::new();
    loop {
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..index]).into_owned();
            let status = head
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or(0);
            assert_eq!(status, 200, "expected an SSE 200, got:\n{head}");
            buffer.drain(..index + 4);
            break;
        }
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).expect("read SSE handshake");
        if read == 0 {
            panic!("the stream closed before the SSE handshake completed");
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    SseReader {
        stream,
        buffer,
        deadline: Instant::now() + Duration::from_secs(10),
    }
}

impl SseReader {
    fn read_frame(&mut self) -> Option<SseFrame> {
        loop {
            if let Some(index) = self.buffer.windows(2).position(|window| window == b"\n\n") {
                let frame: Vec<u8> = self.buffer.drain(..index + 2).collect();
                return Some(parse_sse_frame(&frame));
            }
            if Instant::now() >= self.deadline {
                return None;
            }
            let mut chunk = [0u8; 1024];
            match self.stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(read) => self.buffer.extend_from_slice(&chunk[..read]),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue
                }
                Err(_) => return None,
            }
        }
    }
}

fn parse_sse_frame(frame: &[u8]) -> SseFrame {
    let text = String::from_utf8_lossy(frame);
    let mut parsed = SseFrame::default();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("id: ") {
            parsed.id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("event: ") {
            parsed.event = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("data: ") {
            parsed.data = Some(value.to_string());
        }
    }
    parsed
}

// -- server harness ----------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TestServer {
    fn start(root: &Path, config: ServerConfig) -> Self {
        let server = ControlServer::bind("127.0.0.1:0", root, config).expect("bind the server");
        let addr = server.local_addr();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let _ = server.serve(thread_stop);
        });
        Self {
            addr,
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn project() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn profile_api_bootstrap_and_edit_share_the_same_project_yaml() {
    let dir = project();
    let source = dir.path().join("opencode.json");
    std::fs::write(
        &source,
        r#"{"model":"source/example", "apiKey":"NEVER_EXPOSE"}"#,
    )
    .unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let response = raw_request(server.addr, "GET", "/api/v1/profile", None, &[]);
    assert_eq!(response.status, 200);
    let view = response.json();
    assert!(view["profile"].is_null());
    let choices = view["candidates"].as_array().unwrap();
    let candidate = choices
        .iter()
        .find(|value| {
            value["model_ids"]
                .as_array()
                .is_some_and(|models| models.iter().any(|model| model == "source/example"))
        })
        .unwrap();
    assert!(!response.body.contains("NEVER_EXPOSE"));
    let choice = serde_json::json!({"choice":"import", "location":candidate["location"], "sha256":candidate["sha256"]}).to_string();
    let response = raw_request(
        server.addr,
        "POST",
        "/api/v1/profile/bootstrap",
        Some(&choice),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(response.status, 200, "{}", response.body);
    let created = response.json();
    assert_eq!(created["profile"]["defaultModel"], "source/example");
    assert!(!response.body.contains("NEVER_EXPOSE"));
    assert!(dir.path().join(".ocg.yaml").exists());
    let duplicate = raw_request(
        server.addr,
        "POST",
        "/api/v1/profile/bootstrap",
        Some(r#"{"choice":"new"}"#),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(duplicate.status, 400);
    let mut edited = created["profile"].clone();
    edited["models"]["source/example"]["placeholder"] = serde_json::json!(true);
    let edit = serde_json::json!({"revision":created["revision"], "profile":edited}).to_string();
    let response = raw_request(
        server.addr,
        "PUT",
        "/api/v1/profile",
        Some(&edit),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(
        response.json()["profile"]["models"]["source/example"]["placeholder"],
        true
    );
    let stale = raw_request(
        server.addr,
        "PUT",
        "/api/v1/profile",
        Some(&edit),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(stale.status, 400);
}

// -- snapshot ----------------------------------------------------------------

#[test]
fn snapshot_route_returns_the_authority_and_cursor_atomically() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    let first = mission(1, 1);
    service
        .append(DomainEvent::MissionUpsert {
            mission: first.clone(),
        })
        .unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let response = raw_request(server.addr, "GET", "/api/v1/snapshot", None, &[]);
    assert_eq!(response.status, 200);
    let value = response.json();
    assert_eq!(value["cursor"]["epoch"], 1);
    assert_eq!(value["cursor"]["seq"], 1);
    assert!(value["snapshot"]["missions"]
        .get(&first.mission_id)
        .is_some());
}

#[test]
fn mutation_after_http_snapshot_is_replayed_without_a_gap() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    append_mission(&service, 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let response = raw_request(server.addr, "GET", "/api/v1/snapshot", None, &[]);
    assert_eq!(response.status, 200);
    let snapshot = response.json();
    assert_eq!(snapshot["api_version"], "v1");
    assert_eq!(snapshot["schema_version"], 1);
    let epoch = snapshot["cursor"]["epoch"].as_u64().unwrap();
    let seq = snapshot["cursor"]["seq"].as_u64().unwrap();

    let committed = append_mission(&service, 2);
    let mut reader = open_events(
        server.addr,
        &format!("/api/v1/events?epoch={epoch}&after={seq}"),
        None,
    );
    let frame = reader.read_frame().expect("post-snapshot event");
    assert_eq!(
        frame.id,
        Some(format!("{}:{}", committed.epoch, committed.seq))
    );
    assert_eq!(frame.event.as_deref(), Some("mission_upsert"));
}

// -- approvals ---------------------------------------------------------------

#[test]
fn approval_mutations_commit_through_the_authority_and_return_the_cursor() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    let admitted = mission(1, 1);
    mission::save(dir.path(), &admitted).unwrap();
    let approval_id = pending_approval(dir.path(), &admitted, 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let before = service.head().unwrap();
    let approved = raw_request(
        server.addr,
        "POST",
        &format!("/api/v1/approvals/{approval_id}/approve"),
        Some("{\"note\":\"looks good\"}"),
        &[],
    );
    assert_eq!(approved.status, 200, "body={}", approved.body);
    let value = approved.json();
    assert_eq!(value["approval"]["status"], "approved");
    let cursor = value["cursor"]["seq"].as_u64().unwrap();
    assert!(
        cursor > before.seq,
        "the mutation returns a post-commit cursor"
    );
    assert_eq!(
        policy::load_approval(dir.path(), &approval_id)
            .unwrap()
            .unwrap()
            .status,
        ApprovalStatus::Approved
    );
    let snapshot = service.snapshot().unwrap();
    assert_eq!(
        snapshot.approvals[&approval_id].status,
        ApprovalStatus::Approved
    );
    let replay = service.replay_after(before);
    let events = match replay {
        ocg::orchestration::ReplayAfter::Success { events } => events,
        other => panic!("expected approval event, got {other:?}"),
    };
    assert!(events.iter().any(|event| {
        matches!(
            &event.event,
            DomainEvent::ApprovalUpsert { approval }
                if approval.approval_id == approval_id
                    && approval.status == ApprovalStatus::Approved
        )
    }));

    let rejected = raw_request(
        server.addr,
        "POST",
        &format!("/api/v1/approvals/{approval_id}/reject"),
        None,
        &[],
    );
    assert_eq!(rejected.status, 200);
    assert_eq!(rejected.json()["approval"]["status"], "rejected");
    assert_eq!(
        policy::load_approval(dir.path(), &approval_id)
            .unwrap()
            .unwrap()
            .status,
        ApprovalStatus::Rejected
    );
}

#[test]
fn unknown_approval_is_a_typed_404() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let response = raw_request(
        server.addr,
        "POST",
        "/api/v1/approvals/apr-missing/approve",
        None,
        &[],
    );
    assert_eq!(response.status, 404);
    assert_eq!(response.json()["error"]["code"], "not_found");
}

#[test]
fn approvals_listing_reports_durable_records() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let admitted = mission(1, 1);
    mission::save(dir.path(), &admitted).unwrap();
    let approval_id = pending_approval(dir.path(), &admitted, 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let response = raw_request(server.addr, "GET", "/api/v1/approvals", None, &[]);
    assert_eq!(response.status, 200);
    let value = response.json();
    let approvals = value["approvals"].as_array().unwrap();
    assert_eq!(approvals.len(), 1);
    assert_eq!(approvals[0]["approval_id"], approval_id);
}

// -- budget ------------------------------------------------------------------

#[test]
fn budget_put_and_get_round_trip_authoritatively() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    let admitted = mission(1, 1);
    let mission_id = admitted.mission_id.clone();
    mission::save(dir.path(), &admitted).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let before = service.head().unwrap();
    let put = raw_request(
        server.addr,
        "PUT",
        &format!("/api/v1/budgets/{mission_id}"),
        Some("{\"limit_micros\":500000,\"currency\":\"usd\"}"),
        &[],
    );
    assert_eq!(put.status, 200, "body={}", put.body);
    let value = put.json();
    assert_eq!(value["budget"]["hard_limit_micros"], 500000);
    assert_eq!(value["budget"]["currency"], "USD");
    assert!(value["cursor"]["seq"].as_u64().unwrap() > before.seq);

    let get = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/budgets/{mission_id}"),
        None,
        &[],
    );
    assert_eq!(get.status, 200);
    assert_eq!(get.json()["budget"]["hard_limit_micros"], 500000);

    let missing = raw_request(
        server.addr,
        "GET",
        "/api/v1/budgets/task-missing",
        None,
        &[],
    );
    assert_eq!(missing.status, 404);

    let invalid = raw_request(
        server.addr,
        "PUT",
        &format!("/api/v1/budgets/{mission_id}"),
        Some("{\"limit_micros\":0,\"currency\":\"USD\"}"),
        &[],
    );
    assert_eq!(invalid.status, 400);
    assert_eq!(invalid.json()["error"]["code"], "invalid_request");
}

// -- events ------------------------------------------------------------------

#[test]
fn events_replay_then_live_tail_is_ordered_and_gap_free() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    append_mission(&service, 1);
    append_mission(&service, 2);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let mut reader = open_events(server.addr, "/api/v1/events?epoch=1&after=0", None);
    let mut ids = Vec::new();
    for _ in 0..2 {
        let frame = reader.read_frame().expect("replayed frame");
        assert_eq!(frame.event.as_deref(), Some("mission_upsert"));
        ids.push(frame.id.expect("replayed frame has an id"));
    }

    // Append the remaining events from another thread while the stream is open.
    // The tail must pick them up in order with no gap behind the replay.
    let root = dir.path().to_path_buf();
    let writer = thread::spawn(move || {
        let service = SnapshotService::open(&root).unwrap();
        for index in 3..=10 {
            append_mission(&service, index);
        }
    });
    for _ in 2..10 {
        let frame = reader.read_frame().expect("live frame");
        ids.push(frame.id.expect("live frame has an id"));
    }
    writer.join().unwrap();

    let expected: Vec<String> = (1..=10).map(|seq| format!("1:{seq}")).collect();
    assert_eq!(ids, expected);
}

#[test]
fn last_event_id_resumes_and_only_advances_the_same_epoch_cursor() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    for index in 1..=5 {
        append_mission(&service, index);
    }
    let server = TestServer::start(dir.path(), ServerConfig::default());

    // Reconnect after id 1:2: the stream resumes at 1:3, not from the start.
    let mut resumed = open_events(server.addr, "/api/v1/events?epoch=1&after=0", Some("1:2"));
    assert_eq!(resumed.read_frame().unwrap().id.as_deref(), Some("1:3"));

    // Last-Event-ID may only advance the query cursor: after=4 wins over 1:2.
    let mut advanced = open_events(server.addr, "/api/v1/events?epoch=1&after=4", Some("1:2"));
    assert_eq!(advanced.read_frame().unwrap().id.as_deref(), Some("1:5"));

    // After=0 with a later Last-Event-ID advances to just past it.
    let mut later = open_events(server.addr, "/api/v1/events?epoch=1&after=0", Some("1:4"));
    assert_eq!(later.read_frame().unwrap().id.as_deref(), Some("1:5"));

    // A different epoch is an explicit failure, never a silent reset.
    let conflict = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=1&after=0",
        None,
        &[("Last-Event-ID", "2:0")],
    );
    assert_eq!(conflict.status, 409);
    assert_eq!(conflict.json()["error"]["code"], "wrong_epoch");
}

#[test]
fn invalid_cursors_are_explicit() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    append_mission(&service, 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let wrong_epoch = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=9&after=0",
        None,
        &[],
    );
    assert_eq!(wrong_epoch.status, 409);
    assert_eq!(wrong_epoch.json()["error"]["code"], "wrong_epoch");

    let future = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=1&after=99",
        None,
        &[],
    );
    assert_eq!(future.status, 409);
    assert_eq!(future.json()["error"]["code"], "future_cursor");

    let missing = raw_request(server.addr, "GET", "/api/v1/events", None, &[]);
    assert_eq!(missing.status, 400);
    assert_eq!(missing.json()["error"]["code"], "invalid_request");

    let malformed = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=abc&after=0",
        None,
        &[],
    );
    assert_eq!(malformed.status, 400);
    assert_eq!(malformed.json()["error"]["code"], "invalid_request");
}

#[test]
fn a_pruned_prefix_is_expired_and_never_partially_replayed() {
    let dir = project();
    let config = ServerConfig {
        snapshot: SnapshotConfig::new(2),
        ..ServerConfig::default()
    };
    let server = TestServer::start(dir.path(), config);

    // Pruning is a property of the writer's retention, so append through a
    // service configured with the same bound.
    let service = SnapshotService::open_with_config(dir.path(), SnapshotConfig::new(2)).unwrap();
    for index in 1..=4 {
        append_mission(&service, index);
    }

    let expired = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=1&after=0",
        None,
        &[],
    );
    assert_eq!(expired.status, 410);
    let value = expired.json();
    assert_eq!(value["error"]["code"], "replay_expired");
    assert_eq!(value["error"]["requested_seq"], 0);
    assert!(value["error"]["floor_seq"].as_u64().unwrap() >= 1);
}

#[test]
fn heartbeats_carry_no_event_id() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let config = ServerConfig {
        poll_interval: Duration::from_millis(10),
        heartbeat: Duration::from_millis(20),
        ..ServerConfig::default()
    };
    let server = TestServer::start(dir.path(), config);

    let mut reader = open_events(server.addr, "/api/v1/events?epoch=1&after=0", None);
    let frame = reader.read_frame().expect("heartbeat frame");
    assert_eq!(frame.id, None, "a heartbeat must not carry an id");
    assert_eq!(frame.event, None, "a heartbeat is a comment");
    assert_eq!(frame.data, None, "a heartbeat carries no data");
}

#[test]
fn a_midstream_epoch_change_requires_reset_and_closes() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    let head = append_mission(&service, 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let mut reader = open_events(
        server.addr,
        &format!("/api/v1/events?epoch={}&after={}", head.epoch, head.seq),
        None,
    );

    let snapshot = service.snapshot().unwrap();
    SnapshotService::begin_new_epoch_after_continuity_loss(
        dir.path(),
        snapshot,
        "test asserted continuity loss",
    )
    .unwrap();

    let frame = reader.read_frame().expect("reset control frame");
    assert_eq!(frame.event.as_deref(), Some("reset_required"));
    assert_eq!(frame.id, None);
    let data: Value = serde_json::from_str(frame.data.as_deref().unwrap()).unwrap();
    assert_eq!(data["error"]["code"], "wrong_epoch");
    assert_eq!(data["current_cursor"]["epoch"], 2);
}

// -- resources ---------------------------------------------------------------

#[test]
fn resources_route_lists_the_authoritative_registry() {
    let dir = project();
    let service = SnapshotService::open(dir.path()).unwrap();
    let mut registry = ResourceRegistry::new(1);
    let identity = ResourceIdentity::for_model("openai", "gpt-5");
    registry.observe_available(&identity, "probe", 1);
    resources::save(dir.path(), &registry).unwrap();
    assert!(service.head().unwrap().seq >= 1);
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let response = raw_request(server.addr, "GET", "/api/v1/resources", None, &[]);
    assert_eq!(response.status, 200);
    let value = response.json();
    assert_eq!(value["resources"].as_array().unwrap().len(), 1);
    assert_eq!(value["corrupt"], false);
}

// -- protocol errors ---------------------------------------------------------

#[test]
fn unknown_routes_and_methods_are_typed() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let missing = raw_request(server.addr, "GET", "/api/v1/nope", None, &[]);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.json()["error"]["code"], "not_found");

    let method = raw_request(server.addr, "POST", "/api/v1/snapshot", None, &[]);
    assert_eq!(method.status, 405);
    let allow = method
        .headers
        .iter()
        .find(|(name, _)| name == "allow")
        .map(|(_, value)| value.as_str())
        .unwrap_or_default();
    assert_eq!(allow, "GET");
}

#[test]
fn malformed_and_oversize_requests_are_rejected() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let malformed = send_and_read(server.addr, "GARBAGE\r\n\r\n");
    assert_eq!(malformed.status, 400);
    assert_eq!(malformed.json()["error"]["code"], "malformed_request");

    let oversize_length = (MAX_BODY_BYTES + 1).to_string();
    let oversized = raw_request(
        server.addr,
        "POST",
        "/api/v1/approvals/apr-missing/approve",
        None,
        &[("Content-Length", oversize_length.as_str())],
    );
    assert_eq!(oversized.status, 413);
    assert_eq!(oversized.json()["error"]["code"], "payload_too_large");

    let bad_json = raw_request(
        server.addr,
        "POST",
        "/api/v1/approvals/apr-missing/approve",
        Some("not-json"),
        &[],
    );
    assert_eq!(bad_json.status, 400);
    assert_eq!(bad_json.json()["error"]["code"], "malformed_json");

    let huge_value = "x".repeat(ocg::control_server::MAX_HEADER_BYTES + 1);
    let oversized_head = send_and_read(
        server.addr,
        &format!(
            "GET /api/v1/snapshot HTTP/1.1\r\nHost: localhost\r\nX-Large: {huge_value}\r\n\r\n"
        ),
    );
    assert_eq!(oversized_head.status, 431);
    assert_eq!(oversized_head.json()["error"]["code"], "headers_too_large");

    let duplicate_length = send_and_read(
        server.addr,
        "POST /api/v1/approvals/apr-missing/approve HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
    );
    assert_eq!(duplicate_length.status, 400);
    assert_eq!(
        duplicate_length.json()["error"]["code"],
        "malformed_request"
    );

    let trailing = send_and_read(
        server.addr,
        "POST /api/v1/approvals/apr-missing/approve HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n{}",
    );
    assert_eq!(trailing.status, 400);
    assert_eq!(trailing.json()["error"]["code"], "malformed_request");
}

#[test]
fn bind_refuses_non_loopback_addresses() {
    let dir = project();
    for refused in ["0.0.0.0:0", "192.168.1.10:0", "[::]:0", "not-an-address"] {
        assert!(
            ControlServer::bind(refused, dir.path(), ServerConfig::default()).is_err(),
            "{refused} must be refused"
        );
    }
    // A loopback bind still works and reports a loopback address.
    let server = ControlServer::bind("127.0.0.1:0", dir.path(), ServerConfig::default()).unwrap();
    assert!(server.local_addr().ip().is_loopback());
    assert!(server.base_url().starts_with("http://127.0.0.1:"));

    // An invalid server configuration fails before binding.
    let config = ServerConfig {
        max_clients: 0,
        ..ServerConfig::default()
    };
    assert!(ControlServer::bind("127.0.0.1:0", dir.path(), config).is_err());
}

/// A cross-process-shaped check: the mutation is applied by the server process
/// while another handle observes the durable authority afterward.
#[test]
fn a_mutation_is_visible_to_an_independent_authority_reader() {
    let dir = project();
    let reader = SnapshotService::open(dir.path()).unwrap();
    let admitted = mission(1, 1);
    let mission_id = admitted.mission_id.clone();
    mission::save(dir.path(), &admitted).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    let put = raw_request(
        server.addr,
        "PUT",
        &format!("/api/v1/budgets/{mission_id}"),
        Some("{\"limit_micros\":123456,\"currency\":\"USD\"}"),
        &[],
    );
    assert_eq!(put.status, 200);

    let reloaded = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(reloaded.budget.receipt().hard_limit_micros, Some(123456));
    assert!(reader.head().unwrap().seq >= 2);
}

#[test]
fn sse_observes_a_real_cli_process_mutation() {
    let dir = project();
    // The CLI mutation needs a structurally valid OCG Profile.
    std::fs::write(
        dir.path().join(".ocg.yaml"),
        serde_yaml_ng::to_string(&serde_json::json!({
            "profile": {"origin": "new"},
            "models": {"providers": {"local": {"label": "Local", "placeholder": false}},
                "models": {"local": {"provider": "local", "id": "local", "placeholder": false}}}
        }))
        .unwrap(),
    )
    .unwrap();
    let admitted = mission(1, 1);
    let mission_id = admitted.mission_id.clone();
    mission::save(dir.path(), &admitted).unwrap();
    let authority = SnapshotService::open(dir.path()).unwrap();
    let head = authority.head().unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let mut reader = open_events(
        server.addr,
        &format!("/api/v1/events?epoch={}&after={}", head.epoch, head.seq),
        None,
    );

    let output = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .current_dir(dir.path())
        .env("OCG_USER_CONFIG", dir.path().join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .args([
            "budget",
            "set",
            "--mission",
            &mission_id,
            "--limit",
            "321000",
            "--currency",
            "USD",
        ])
        .output()
        .expect("run the independent ocg mutation");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let frame = reader.read_frame().expect("cross-process journal event");
    assert_eq!(frame.event.as_deref(), Some("mission_upsert"));
    let expected_id = format!("1:{}", head.seq + 1);
    assert_eq!(frame.id.as_deref(), Some(expected_id.as_str()));
    let event: Value = serde_json::from_str(frame.data.as_deref().unwrap()).unwrap();
    assert_eq!(
        event["event"]["mission"]["budget"]["hard_limit"]["micros"],
        321000
    );
    let snapshot = authority.snapshot().unwrap();
    assert_eq!(
        snapshot.missions[&mission_id]
            .budget
            .receipt()
            .hard_limit_micros,
        Some(321000)
    );
}

#[test]
fn corrupt_authority_is_an_explicit_service_failure() {
    let dir = project();
    SnapshotService::open(dir.path()).unwrap();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    std::fs::write(ocg::orchestration::state_path(dir.path()), b"{}").unwrap();

    let snapshot = raw_request(server.addr, "GET", "/api/v1/snapshot", None, &[]);
    assert_eq!(snapshot.status, 503);
    assert_eq!(snapshot.json()["error"]["code"], "persistence_unavailable");
    let events = raw_request(
        server.addr,
        "GET",
        "/api/v1/events?epoch=1&after=0",
        None,
        &[],
    );
    assert_eq!(events.status, 503);
    assert_eq!(events.json()["error"]["code"], "persistence_unavailable");
}

// -- canonical WorkNode/Run control surface -----------------------------------

/// A project with the OCG boundary marker, so canonical routes are available.
fn canonical_project() -> tempfile::TempDir {
    let dir = project();
    std::fs::write(dir.path().join(".ocg.yaml"), "{}\n").unwrap();
    dir
}

fn canonical_mission(root: &Path, id: &str) -> ocg::orchestration::substrate::DispatchWitness {
    let mut repo = ocg::orchestration::substrate::SubstrateRepository::open(root).unwrap();
    let mission = ocg::orchestration::substrate::MissionId::new(id).unwrap();
    repo.create_live_mission(
        &mission,
        "canonical inspector",
        ocg::orchestration::substrate::RunContract {
            executor: "lead-high".into(),
            model: "openai/gpt-6-astra".into(),
            role: "lead".into(),
        },
        "lead-session",
        10,
    )
    .unwrap()
}

#[test]
fn canonical_project_import_validates_the_boundary_and_persists_identity() {
    let dir = canonical_project();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let body = serde_json::json!({
        "command_id": "cmd-import-1",
        "root": dir.path().to_string_lossy(),
    })
    .to_string();
    let response = raw_request(
        server.addr,
        "POST",
        "/api/v1/canonical/projects/import",
        Some(&body),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(response.status, 200, "{}", response.body);
    let json = response.json();
    assert_eq!(json["api_version"], "ocg.canonical.v1");
    assert_eq!(json["command_id"], "cmd-import-1");
    assert_eq!(json["accepted"], true);
    assert_eq!(json["project"]["marker"], true);
    let project_id = json["project"]["project_id"].as_str().unwrap().to_string();

    // Re-importing the same root is idempotent, not a duplicate identity.
    let repeat = raw_request(
        server.addr,
        "POST",
        "/api/v1/canonical/projects/import",
        Some(&body),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(repeat.json()["project"]["project_id"], project_id);
    let listed = raw_request(server.addr, "GET", "/api/v1/canonical/projects", None, &[]);
    assert_eq!(listed.json()["projects"].as_array().unwrap().len(), 1);
}

#[test]
fn canonical_configuration_and_pre_run_mission_commands_are_acknowledged() {
    let dir = canonical_project();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let import = raw_request(
        server.addr,
        "POST",
        "/api/v1/canonical/projects/import",
        Some(
            &serde_json::json!({"command_id":"cmd-import-2","root":dir.path().to_string_lossy()})
                .to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    let project_id = import.json()["project"]["project_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Global configuration persists and increments a revision.
    let global = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/configuration",
        Some(
            &serde_json::json!({
                "command_id":"cmd-global-1",
                "configuration":{"provider":"openai","model":"gpt-6-astra","profile":"careful","routing":"balanced","resource_budget":{"hard_limit":42}}
            })
            .to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(global.status, 200, "{}", global.body);
    assert_eq!(global.json()["accepted"], true);
    assert_eq!(global.json()["revision"], 1);
    let read = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/configuration?project_id={project_id}"),
        None,
        &[],
    );
    assert_eq!(read.json()["configuration"]["global"]["profile"], "careful");

    // Project defaults are a separate, per-Project scope. They survive a later
    // global configuration change and are never reported as the global scope.
    let defaults = raw_request(
        server.addr,
        "PUT",
        &format!("/api/v1/canonical/configuration/projects/{project_id}"),
        Some(
            &serde_json::json!({"command_id":"cmd-project-1","defaults":{"profile":"fast","hard_budget":10}}).to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(defaults.status, 200, "{}", defaults.body);
    assert_eq!(defaults.json()["accepted"], true);
    assert_eq!(
        defaults.json()["configuration"]["project_defaults"]["defaults"]["profile"],
        "fast"
    );
    let after_defaults = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/configuration",
        Some(
            &serde_json::json!({"command_id":"cmd-global-2","configuration":{"profile":"balanced"}}).to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(after_defaults.status, 200, "{}", after_defaults.body);
    let reread = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/configuration?project_id={project_id}"),
        None,
        &[],
    );
    let view = reread.json();
    assert_eq!(view["configuration"]["global"]["profile"], "balanced");
    assert_eq!(
        view["configuration"]["project_defaults"]["defaults"]["profile"],
        "fast"
    );

    // A Mission with no dispatched Run accepts pre-run configuration...
    let mut repo = ocg::orchestration::substrate::SubstrateRepository::open(dir.path()).unwrap();
    let mission = ocg::orchestration::substrate::MissionId::new("wn-undispatched").unwrap();
    repo.create_mission(&mission, "goal", 1).unwrap();
    drop(repo);
    let pre_run = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/missions/wn-undispatched/configuration",
        Some(
            &serde_json::json!({"command_id":"cmd-mission-1","configuration":{"profile":"careful","hard_budget":7}}).to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(pre_run.status, 200, "{}", pre_run.body);
    assert_eq!(pre_run.json()["revision"], 1);

    // ...and a dispatched Mission refuses it, because the Run contract is frozen.
    canonical_mission(dir.path(), "wn-dispatched");
    let frozen = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/missions/wn-dispatched/configuration",
        Some(
            &serde_json::json!({"command_id":"cmd-mission-2","configuration":{"profile":"fast"}})
                .to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    assert_eq!(frozen.status, 400, "{}", frozen.body);
    assert!(frozen.body.contains("frozen"), "{}", frozen.body);
}

#[test]
fn canonical_work_snapshot_and_event_tail_project_durable_state() {
    let dir = canonical_project();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let import = raw_request(
        server.addr,
        "POST",
        "/api/v1/canonical/projects/import",
        Some(
            &serde_json::json!({"command_id":"cmd-import-3","root":dir.path().to_string_lossy()})
                .to_string(),
        ),
        &[("Content-Type", "application/json")],
    );
    let project_id = import.json()["project"]["project_id"]
        .as_str()
        .unwrap()
        .to_string();
    let witness = canonical_mission(dir.path(), "wn-projected");
    let snapshot = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/work?project_id={project_id}&mission_id=wn-projected"),
        None,
        &[],
    );
    assert_eq!(snapshot.status, 200, "{}", snapshot.body);
    let json = snapshot.json();
    assert_eq!(
        json["mission"]["runs"][0]["witness"]["dispatch_id"],
        witness.dispatch_id
    );
    assert_eq!(json["mission"]["runs"][0]["contract"]["role"], "lead");
    assert!(json["cursor"].as_u64().unwrap() >= 3);
    let events = raw_request(
        server.addr,
        "GET",
        &format!(
            "/api/v1/canonical/work/events?project_id={project_id}&mission_id=wn-projected&after=0"
        ),
        None,
        &[],
    );
    assert_eq!(events.status, 200, "{}", events.body);
    let list = events.json();
    assert!(list["events"].as_array().unwrap().len() >= 3);
    assert_eq!(list["events"][0]["mission_id"], "wn-projected");
    // Resuming from the head returns nothing new.
    let tail = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/work/events?project_id={project_id}&mission_id=wn-projected&after={}", json["cursor"]),
        None,
        &[],
    );
    assert!(tail.json()["events"].as_array().unwrap().is_empty());
    // A project identity that does not own this boundary is refused.
    let wrong = raw_request(
        server.addr,
        "GET",
        "/api/v1/canonical/work?project_id=project-unknown&mission_id=wn-projected",
        None,
        &[],
    );
    assert_eq!(wrong.status, 400, "{}", wrong.body);
}

#[test]
fn canonical_routes_fail_closed_without_a_project_boundary() {
    let dir = project();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let response = raw_request(server.addr, "GET", "/api/v1/canonical/projects", None, &[]);
    // Without a boundary the legacy routes still work and the canonical
    // surface reports an explicit error instead of guessing.
    assert_eq!(response.status, 409, "{}", response.body);
    assert!(response.body.contains("boundary"), "{}", response.body);
    let snapshot = raw_request(server.addr, "GET", "/api/v1/snapshot", None, &[]);
    assert_eq!(snapshot.status, 200, "{}", snapshot.body);
    assert_eq!(snapshot.json()["api_version"], "v1");
}

#[test]
fn canonical_routes_answer_a_loopback_pwa_preflight_and_refuse_other_origins() {
    let dir = canonical_project();
    let server = TestServer::start(dir.path(), ServerConfig::default());

    // A preflight from the PWA's own loopback origin is answered with the
    // methods that route actually allows, and the concrete origin is echoed.
    let preflight = raw_request(
        server.addr,
        "OPTIONS",
        "/api/v1/canonical/configuration",
        None,
        &[
            ("Origin", "http://localhost:3000"),
            ("Access-Control-Request-Method", "PUT"),
        ],
    );
    assert_eq!(preflight.status, 204, "{}", preflight.body);
    let allow_origin = preflight
        .headers
        .iter()
        .find(|(name, _)| name == "access-control-allow-origin")
        .map(|(_, value)| value.clone());
    assert_eq!(allow_origin.as_deref(), Some("http://localhost:3000"));
    let allow_methods = preflight
        .headers
        .iter()
        .find(|(name, _)| name == "access-control-allow-methods")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    assert!(allow_methods.contains("PUT"), "{allow_methods}");
    assert!(allow_methods.contains("OPTIONS"), "{allow_methods}");
    assert!(
        !allow_methods.contains('*'),
        "a wildcard method list would expose the whole surface: {allow_methods}"
    );

    // A normal canonical read from the same origin carries the allow header.
    let read = raw_request(
        server.addr,
        "GET",
        "/api/v1/canonical/projects",
        None,
        &[("Origin", "http://127.0.0.1:3000")],
    );
    assert_eq!(read.status, 200, "{}", read.body);
    assert!(read
        .headers
        .iter()
        .any(|(name, value)| name == "access-control-allow-origin"
            && value == "http://127.0.0.1:3000"));

    // A non-loopback origin is refused rather than reflected.
    let foreign = raw_request(
        server.addr,
        "OPTIONS",
        "/api/v1/canonical/configuration",
        None,
        &[("Origin", "https://example.invalid")],
    );
    assert_eq!(foreign.status, 403, "{}", foreign.body);
    assert!(!foreign
        .headers
        .iter()
        .any(|(name, _)| name == "access-control-allow-origin"));
    // A request with no Origin at all is a plain local request and still works.
    let plain = raw_request(server.addr, "GET", "/api/v1/canonical/projects", None, &[]);
    assert_eq!(plain.status, 200, "{}", plain.body);
}

// -- the served bytes must satisfy the declared contract ---------------------

/// Deserialize a real HTTP body into the contract type the PWA decodes into.
///
/// The generated TypeScript in `frontend/components/ocg/contracts` is a
/// projection of the Rust structs in `src/contracts.rs`. This test closes the
/// loop: it takes the bytes a real loopback server actually sent and requires
/// them to satisfy that same declaration. A route that started answering with
/// an anonymous `json!` literal, or drifted from the declared shape, fails here
/// rather than in the browser.
fn assert_contract<T: serde::de::DeserializeOwned>(response: &RawResponse, what: &str) -> T {
    assert_eq!(response.status, 200, "{what}: {}", response.body);
    serde_json::from_str(&response.body).unwrap_or_else(|error| {
        panic!(
            "{what} does not satisfy the declared contract: {error}\nbody={}",
            response.body
        )
    })
}

#[test]
fn canonical_routes_answer_with_the_declared_contract() {
    use ocg::contracts;
    use serde::de::DeserializeOwned;

    let dir = canonical_project();
    let server = TestServer::start(dir.path(), ServerConfig::default());
    let json_headers = [("Content-Type", "application/json")];

    let import = raw_request(
        server.addr,
        "POST",
        "/api/v1/canonical/projects/import",
        Some(
            &serde_json::json!({
                "command_id": "cmd-contract-import",
                "root": dir.path().to_string_lossy(),
            })
            .to_string(),
        ),
        &json_headers,
    );
    let imported: contracts::CanonicalProjectResponse = assert_contract(&import, "project import");
    assert_eq!(
        imported.api_version,
        contracts::CANONICAL_CONTROL_API_VERSION
    );
    assert!(imported.accepted);
    let project_id = imported.project.project_id;

    let listed = raw_request(server.addr, "GET", "/api/v1/canonical/projects", None, &[]);
    let projects: contracts::CanonicalProjectsResponse = assert_contract(&listed, "project list");
    assert!(projects.projects.iter().any(|p| p.project_id == project_id));

    let configuration = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/configuration?project_id={project_id}"),
        None,
        &[],
    );
    let read: contracts::CanonicalConfigurationEnvelope =
        assert_contract(&configuration, "configuration read");
    assert_eq!(read.configuration.project.project_id, project_id);

    // Every `Option` field of `GlobalConfiguration` is implicitly optional to
    // serde, so a body naming only some of them is accepted with the rest null.
    // The answer still has to carry the whole struct, because that is what the
    // PWA decodes.
    let partial = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/configuration",
        Some(
            &serde_json::json!({
                "command_id": "cmd-contract-partial",
                "configuration": { "profile": "careful" },
            })
            .to_string(),
        ),
        &json_headers,
    );
    let partial: contracts::CanonicalConfigurationResponse =
        assert_contract(&partial, "partial configuration write");
    assert_eq!(
        partial.configuration.global.profile.as_deref(),
        Some("careful")
    );
    assert_eq!(partial.configuration.global.runtime, None);
    assert_eq!(partial.configuration.global.resource_budget, None);

    let complete_body = serde_json::json!({
        "command_id": "cmd-contract-global",
        "configuration": {
            "provider": "openai",
            "model": "gpt-6-astra",
            "profile": "careful",
            "routing": "balanced",
            "runtime": null,
            "resource_budget": { "hard_limit": 42 },
        },
    })
    .to_string();
    let written = raw_request(
        server.addr,
        "PUT",
        "/api/v1/canonical/configuration",
        Some(&complete_body),
        &json_headers,
    );
    let ack: contracts::CanonicalConfigurationResponse =
        assert_contract(&written, "configuration write");
    assert_eq!(ack.command_id, "cmd-contract-global");
    assert!(ack.accepted);
    // A budget sent without a unit is filled in by the backend, so the PWA
    // never has to guess one when reading it back.
    let budget = ack.configuration.global.resource_budget.expect("a budget");
    assert_eq!(budget.hard_limit, 42.0);
    assert_eq!(budget.unit, "USD");

    let mission = canonical_mission(dir.path(), "wn-contract");
    let _ = mission;

    let events = raw_request(
        server.addr,
        "GET",
        &format!(
            "/api/v1/canonical/work/events?project_id={project_id}&mission_id=wn-contract&after=0"
        ),
        None,
        &[],
    );
    let tail: contracts::CanonicalEventsEnvelope = assert_contract(&events, "work events");
    assert_eq!(tail.project_id, project_id);
    assert_eq!(tail.mission_id, "wn-contract");

    let mission_config = raw_request(
        server.addr,
        "GET",
        "/api/v1/canonical/missions/wn-contract/configuration",
        None,
        &[],
    );
    let envelope: contracts::CanonicalMissionConfigEnvelope =
        assert_contract(&mission_config, "mission configuration");
    assert_eq!(envelope.mission_id, "wn-contract");

    let dashboard = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/dashboard?project_id={project_id}"),
        None,
        &[],
    );
    let _: contracts::CanonicalDashboardResponse = assert_contract(&dashboard, "dashboard");

    let snapshot = raw_request(
        server.addr,
        "GET",
        &format!("/api/v1/canonical/work?project_id={project_id}&mission_id=wn-contract"),
        None,
        &[],
    );
    let _: contracts::CanonicalWorkSnapshot = assert_contract(&snapshot, "work snapshot");

    // The error envelope the PWA decodes is the one the server emits.
    let refused = raw_request(
        server.addr,
        "GET",
        "/api/v1/canonical/configuration?project_id=project-unknown",
        None,
        &[],
    );
    assert_eq!(refused.status, 400, "{}", refused.body);
    let error: contracts::ApiErrorEnvelope =
        serde_json::from_str(&refused.body).unwrap_or_else(|e| {
            panic!(
                "error envelope does not satisfy the contract: {e}\nbody={}",
                refused.body
            )
        });
    assert!(!error.error.code.is_empty());
    assert!(!error.error.message.is_empty());

    fn require_owned<T: DeserializeOwned>(_: &T) {}
    let _ = require_owned::<contracts::ApiErrorEnvelope>;
}
