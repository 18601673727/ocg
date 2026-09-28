//! Cross-process STDIO MCP integration and authority-convergence tests.

use ocg::orchestration::policy::{self, ApprovalRequest, PolicyAction};
use ocg::orchestration::replay::{DomainEvent, SnapshotConfig, SnapshotService};
use ocg::orchestration::{mission, ControlService, Mission};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

struct McpChild {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl McpChild {
    fn start(project: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
            .arg("mcp")
            .current_dir(project)
            .env("OCG_USER_CONFIG", project.join("no-user.yaml"))
            .env_remove("OCG_PROJECT_CONFIG")
            .env_remove("OCG_HOME")
            .env_remove("OPENCODE_CONFIG_DIR")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn ocg mcp");
        let stdin = child.stdin.take().expect("MCP stdin");
        let stdout = BufReader::new(child.stdout.take().expect("MCP stdout"));
        Self {
            child,
            stdin: Some(stdin),
            stdout,
        }
    }

    fn raw(&mut self, line: &str) -> Value {
        let stdin = self.stdin.as_mut().expect("open stdin");
        writeln!(stdin, "{line}").unwrap();
        stdin.flush().unwrap();
        let mut response = String::new();
        self.stdout.read_line(&mut response).unwrap();
        assert!(!response.is_empty(), "MCP process closed unexpectedly");
        serde_json::from_str(&response).unwrap_or_else(|error| {
            panic!("protocol stdout was not one clean JSON message: {error}: {response:?}")
        })
    }

    fn rpc(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.raw(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string())
    }

    fn call(&mut self, id: u64, name: &str, arguments: Value) -> Value {
        self.rpc(id, "tools/call", json!({"name":name,"arguments":arguments}))
    }

    fn finish(mut self) -> String {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for MCP EOF shutdown");
        assert!(
            status.success(),
            "MCP did not exit cleanly on EOF: {status}"
        );
        let mut stderr = String::new();
        std::io::Read::read_to_string(self.child.stderr.as_mut().expect("MCP stderr"), &mut stderr)
            .unwrap();
        stderr
    }
}

fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    // `ocg mcp` is a non-interactive surface: it requires an existing Profile
    // and never bootstraps one.
    std::fs::write(
        dir.path().join(".ocg.yaml"),
        serde_yaml_ng::to_string(&json!({
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

fn pending(root: &Path, mission: &Mission, suffix: &str) -> String {
    let id = format!("apr-{suffix}");
    policy::ensure_pending(
        root,
        &ApprovalRequest {
            approval_id: id.clone(),
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            action: PolicyAction::EnsureExecution,
            current_execution_id: mission.session_id.clone(),
            requested_at: 2,
        },
    )
    .unwrap();
    id
}

fn structured(response: &Value) -> &Value {
    &response["result"]["structuredContent"]
}

#[test]
fn stdio_handshake_tools_reads_mutations_replay_and_errors_share_authority() {
    let dir = project();
    let mission = Mission::admit("task-mcp-0001", "bounded task", "execution-1", 1);
    mission::save(dir.path(), &mission).unwrap();
    let approve_id = pending(dir.path(), &mission, "approve-0001");
    let reject_id = pending(dir.path(), &mission, "reject-0001");

    let mut mcp = McpChild::start(dir.path());
    let initialized = mcp.rpc(
        1,
        "initialize",
        json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}),
    );
    assert_eq!(initialized["result"]["protocolVersion"], "2025-06-18");

    // Notification: no response is emitted; the next request must align to one
    // clean JSON line on stdout.
    writeln!(
        mcp.stdin.as_mut().unwrap(),
        "{}",
        json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )
    .unwrap();
    mcp.stdin.as_mut().unwrap().flush().unwrap();

    let listed = mcp.rpc(2, "tools/list", json!({}));
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert_eq!(names.len(), 10);
    for expected in [
        "ocg_state_summary",
        "ocg_mission_list",
        "ocg_mission_get",
        "ocg_events_replay",
        "ocg_approvals_list",
        "ocg_resources_list",
        "ocg_budget_get",
        "ocg_approval_approve",
        "ocg_approval_reject",
        "ocg_budget_set",
    ] {
        assert!(names.contains(&expected), "missing {expected}");
    }
    assert!(!names
        .iter()
        .any(|name| name.contains("shell") || name.contains("file")));

    let summary = mcp.call(3, "ocg_state_summary", json!({}));
    assert_eq!(structured(&summary)["mission_count"], 1);
    assert_eq!(structured(&summary)["pending_approval_count"], 2);
    let before = structured(&summary)["cursor"].clone();

    let with_opencode_meta = mcp.rpc(
        31,
        "tools/call",
        json!({"name":"ocg_state_summary","arguments":{},"_meta":{"sessionID":"ses_test"}}),
    );
    assert_eq!(structured(&with_opencode_meta)["mission_count"], 1);

    let mission_get = mcp.call(
        4,
        "ocg_mission_get",
        json!({"mission_id":mission.mission_id}),
    );
    assert_eq!(
        structured(&mission_get)["mission"]["mission_id"],
        mission.mission_id
    );

    let budget = mcp.call(
        5,
        "ocg_budget_set",
        json!({"mission_id":mission.mission_id,"limit_micros":777000,"currency":"usd"}),
    );
    assert_eq!(structured(&budget)["budget"]["hard_limit_micros"], 777000);
    assert_eq!(structured(&budget)["budget"]["currency"], "USD");

    // Independent ControlService/CLI authority observes the MCP mutation.
    let control = ControlService::open(dir.path()).unwrap();
    assert_eq!(
        control
            .budget(&mission.mission_id)
            .unwrap()
            .budget
            .hard_limit_micros,
        Some(777000)
    );

    let replay = mcp.call(
        6,
        "ocg_events_replay",
        json!({"epoch":before["epoch"],"after":before["seq"],"limit":10}),
    );
    let events = structured(&replay)["events"].as_array().unwrap();
    assert!(events
        .iter()
        .any(|event| event["event"]["kind"] == "mission_upsert"));

    let approved = mcp.call(
        7,
        "ocg_approval_approve",
        json!({"approval_id":approve_id,"note":"reviewed"}),
    );
    assert_eq!(structured(&approved)["outcome"], "approved");
    let rejected = mcp.call(8, "ocg_approval_reject", json!({"approval_id":reject_id}));
    assert_eq!(structured(&rejected)["outcome"], "rejected");

    let fabricated = mcp.call(
        9,
        "ocg_approval_approve",
        json!({"approval_id":"apr-does-not-exist"}),
    );
    assert_eq!(structured(&fabricated)["error"]["code"], "not_found");
    assert_eq!(fabricated["result"]["isError"], true);

    let malformed_input = mcp.call(
        10,
        "ocg_budget_set",
        json!({"mission_id":mission.mission_id,"limit_micros":"many","currency":"USD","force":true}),
    );
    assert_eq!(
        structured(&malformed_input)["error"]["code"],
        "invalid_argument"
    );
    let unchanged = control.budget(&mission.mission_id).unwrap();
    assert_eq!(unchanged.budget.hard_limit_micros, Some(777000));

    let unknown = mcp.call(11, "ocg_shell", json!({"command":"whoami"}));
    assert_eq!(structured(&unknown)["error"]["code"], "unsupported");
    let future = mcp.call(
        12,
        "ocg_events_replay",
        json!({"epoch":before["epoch"],"after":999999}),
    );
    assert_eq!(structured(&future)["error"]["code"], "future_cursor");
    let wrong_epoch = mcp.call(13, "ocg_events_replay", json!({"epoch":999,"after":0}));
    assert_eq!(structured(&wrong_epoch)["error"]["code"], "wrong_epoch");

    let parse_error = mcp.raw("not json");
    assert_eq!(parse_error["error"]["code"], -32700);
    assert_eq!(mcp.rpc(14, "ping", json!({}))["result"], json!({}));

    let stderr = mcp.finish();
    assert!(stderr.is_empty(), "unexpected MCP diagnostics: {stderr}");
}

#[test]
fn replay_expiry_is_explicit_through_the_child_protocol() {
    let dir = project();
    let authority = SnapshotService::open_with_config(dir.path(), SnapshotConfig::new(2)).unwrap();
    for index in 1..=4 {
        authority
            .append(DomainEvent::MissionUpsert {
                mission: Mission::admit(
                    &format!("task-expired-{index:04}"),
                    "task",
                    "execution",
                    index,
                ),
            })
            .unwrap();
    }
    let mut mcp = McpChild::start(dir.path());
    let response = mcp.call(
        1,
        "ocg_events_replay",
        json!({"epoch":1,"after":0,"limit":2}),
    );
    assert_eq!(structured(&response)["error"]["code"], "replay_expired");
    assert_eq!(structured(&response)["error"]["detail"]["floor_seq"], 3);
    mcp.finish();
}
