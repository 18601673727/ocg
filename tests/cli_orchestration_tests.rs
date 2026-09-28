//! CLI-level orchestration tests: plugin injection, the hidden bridge and the
//! disabled escape hatch.

mod common;

use common::{project_profile, write_yaml, TestDir};
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// Every orchestration surface needs a structurally valid OCG Profile. Seed it
/// unless the test wrote its own project config.
fn seed_profile(cwd: &Path) {
    let path = cwd.join(".ocg.yaml");
    if !path.is_file() {
        write_yaml(&path, &project_profile());
    }
}

/// Write a project config that always carries the OCG Profile, so a test can
/// state only the section it is about.
fn write_project(path: &Path, mut value: Value) {
    let profile = project_profile();
    for key in ["profile", "models"] {
        if value.get(key).is_none() {
            value[key] = profile[key].clone();
        }
    }
    write_yaml(path, &value);
}

fn base_command(cwd: &Path, work: &Path) -> Command {
    seed_profile(cwd);
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(cwd)
        .env("OCG_USER_CONFIG", work.join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("OCG_USER_CONFIG")
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_API_BASE")
        .env_remove("OCG_CACHE_DIR");
    command
}

fn bridge(cwd: &Path, work: &Path, event: &str, payload: &Value) -> Value {
    seed_profile(cwd);
    let mut child = base_command(cwd, work)
        .args(["__bridge", event, "--project", cwd.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bridge");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(payload.to_string().as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("bridge JSON")
}

fn project(dir: &TestDir) -> std::path::PathBuf {
    let project = dir.project();
    std::fs::write(project.join("parser.rs"), "pub fn parse() {}\n").unwrap();
    project
}

/// A fake `opencode` reporting a fixed version, so runtime-facing config
/// output resolves a deterministic runtime family regardless of whatever
/// OpenCode the host happens to have installed.
#[cfg(unix)]
fn fake_runtime(dir: &TestDir, name: &str, version: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let script = dir.join(name);
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo {version}; exit 0; fi\nexit 0\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    script
}

#[cfg(unix)]
#[test]
fn enabled_build_injects_plugin_and_preserves_user_plugins() {
    let dir = TestDir::new();
    let project = project(&dir);
    write_project(
        &project.join(".ocg.yaml"),
        json!({"opencode": {"plugin": ["my-user-plugin"]}}),
    );
    // The V1 contract: the generated local adapter is injected as a file://
    // entry in the singular `plugin` array.
    let runtime = fake_runtime(&dir, "fake-opencode-v1", "1.18.31");
    let output = base_command(&project, dir.path())
        .env("OCG_OPENCODE", &runtime)
        .args(["build", "--pretty"])
        .output()
        .expect("run ocg");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: Value = serde_json::from_slice(&output.stdout).unwrap();
    let plugins = config["plugin"].as_array().expect("plugin array");
    let names: Vec<String> = plugins
        .iter()
        .map(|value| value.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names.iter().any(|name| name == "my-user-plugin"));
    assert!(names
        .iter()
        .any(|name| name.ends_with("ocg-orchestration.js") && name.starts_with("file://")));
}

#[cfg(unix)]
#[test]
fn disabled_orchestration_emits_no_plugin() {
    let dir = TestDir::new();
    let project = project(&dir);
    let runtime = fake_runtime(&dir, "fake-opencode-v1", "1.18.31");
    let output = base_command(&project, dir.path())
        .env("OCG_ORCHESTRATION", "0")
        .env("OCG_OPENCODE", &runtime)
        .args(["build"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let config: Value = serde_json::from_slice(&output.stdout).unwrap();
    let has_ocg = config
        .get("plugin")
        .and_then(Value::as_array)
        .map(|plugins| {
            plugins.iter().any(|value| {
                value
                    .as_str()
                    .map(|name| name.contains("ocg-orchestration.js"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    assert!(
        !has_ocg,
        "disabled orchestration must preserve the explicit no-hook path: {config}"
    );
    assert!(!project.join(".ocg").join("orchestration").exists());
}

#[cfg(unix)]
#[test]
fn disabled_orchestration_launch_preserves_the_no_hook_path() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = project(&dir);
    let marker = dir.join("launched");
    let script = dir.join("fake-opencode-no-hook.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nprintf launched > \"{}\"\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = base_command(&project, dir.path())
        .env("OCG_ORCHESTRATION", "0")
        .env("OCG_OPENCODE", &script)
        .args(["run", "hello"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(marker.exists());
    assert!(
        !project.join(".ocg").exists(),
        "the explicit no-hook launch must not materialize local plugin state"
    );
}

#[test]
fn bridge_chat_message_prepares_context_and_state() {
    let dir = TestDir::new();
    let project = project(&dir);
    let value = bridge(
        &project,
        dir.path(),
        "chat.message",
        &json!({"session_id": "cli-session", "text": "fix the parser"}),
    );
    assert_eq!(value["ok"], json!(true));
    assert!(value["context"]
        .as_str()
        .unwrap()
        .contains("fix the parser"));
    assert!(project
        .join(".ocg")
        .join("orchestration")
        .join("state.json")
        .is_file());
}

#[test]
fn bridge_session_context_supplies_the_baseline_on_every_dispatch() {
    let dir = TestDir::new();
    let project = project(&dir);
    // Task identity enters through prompt admission, never through dispatch
    // content: the baseline names the task only after it was admitted.
    let admitted = bridge(
        &project,
        dir.path(),
        "session.prompt",
        &json!({"session_id": "cli-session", "text": "fix the parser"}),
    );
    // V2 prompt admission is fail-closed when the invocation has no verified
    // runtime client and canonical Lead contract. This CLI helper intentionally
    // does not start a V2 service, so it must not create task state locally.
    assert_eq!(admitted["ok"], json!(false));
    assert!(admitted["error"]
        .as_str()
        .unwrap()
        .contains("invocation channel unavailable"));
}

#[test]
fn bridge_session_prompt_admission_drives_task_identity() {
    let dir = TestDir::new();
    let project = project(&dir);

    // A genuine admission establishes the task identity.
    let first = bridge(
        &project,
        dir.path(),
        "session.prompt",
        &json!({"session_id": "cli-session", "text": "fix the parser"}),
    );
    // Prompt admission is fail-closed without a verified V2 runtime client;
    // ordinary CLI bridge invocations cannot manufacture Lead authority.
    assert_eq!(first["ok"], json!(false));
    assert!(first["error"]
        .as_str()
        .unwrap()
        .contains("invocation channel unavailable"));
}

#[test]
fn bridge_is_disabled_and_empty_when_orchestration_is_off() {
    let dir = TestDir::new();
    let project = project(&dir);
    let value = base_command(&project, dir.path())
        .env("OCG_ORCHESTRATION", "0")
        .args([
            "__bridge",
            "chat.message",
            "--project",
            project.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(br#"{"session_id":"s","text":"hello"}"#)
                .unwrap();
            child.wait_with_output()
        })
        .unwrap();
    let value: Value = serde_json::from_slice(&value.stdout).unwrap();
    assert_eq!(value["ok"], json!(false));
    assert_eq!(value["disabled"], json!(true));
    assert!(!project.join(".ocg").exists());
}

#[test]
fn bridge_unknown_event_is_soft() {
    let dir = TestDir::new();
    let project = project(&dir);
    let value = bridge(&project, dir.path(), "no.such.event", &json!({}));
    assert_eq!(value["ok"], json!(false));
}

#[cfg(unix)]
#[test]
fn launch_fails_clearly_when_the_plugin_cannot_be_materialized() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    std::fs::write(project.join("parser.rs"), "pub fn parse() {}\n").unwrap();
    write_project(
        &project.join(".ocg.yaml"),
        json!({"profile": {"origin": "new", "defaultModel": "gpt-5.6-sol"},
               "models": {"providers": {"openai": {"label": "OpenAI", "placeholder": false}},
                          "models": {"gpt-5.6-sol": {"provider": "openai", "id": "gpt-5.6-sol", "placeholder": false}}}}),
    );
    // A file where the state directory must be makes materialization fail.
    std::fs::write(project.join(".ocg"), b"not a directory").unwrap();

    let script = dir.join("fake-opencode.sh");
    std::fs::write(
        &script,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\necho launched\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let project_arg = project.to_string_lossy().into_owned();
    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", &script)
        .args(["--project", project_arg.as_str(), "run", "hello"])
        .output()
        .expect("run ocg");
    assert!(
        !output.status.success(),
        "a broken file:// integration must fail the launch, not warn"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.to_lowercase().contains("orchestration")
            || stderr.to_lowercase().contains("plugin")
            || stderr.to_lowercase().contains("directory"),
        "stderr should explain the materialization failure: {stderr}"
    );
}

fn bridge_with_stdin(
    cwd: &Path,
    work: &Path,
    event: &str,
    body: &str,
    orchestration: Option<&str>,
) -> Value {
    let mut command = base_command(cwd, work);
    if let Some(value) = orchestration {
        command.env("OCG_ORCHESTRATION", value);
    }
    let mut child = command
        .args(["__bridge", event, "--project", cwd.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bridge");
    let mut stdin = child.stdin.take().unwrap();
    let body = body.to_string();
    // Write in a thread: the bridge may stop reading at its cap and close the
    // pipe, which surfaces as a BrokenPipe on the writer. That is expected.
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(body.as_bytes());
    });
    let output = child.wait_with_output().unwrap();
    let _ = writer.join();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("bridge JSON")
}

#[test]
fn disabled_bridge_drains_large_and_repeated_payloads_without_a_race() {
    let dir = TestDir::new();
    let project = project(&dir);
    // Large (but under the 4 MiB cap) so the disabled path must still drain.
    let large = format!(
        "{{\"session_id\":\"s\",\"text\":\"{}\"}}",
        "x".repeat(1024 * 1024)
    );
    for _ in 0..8 {
        let value = bridge_with_stdin(&project, dir.path(), "chat.message", &large, Some("0"));
        assert_eq!(value["ok"], json!(false));
        assert_eq!(value["disabled"], json!(true));
    }
    assert!(!project.join(".ocg").exists());
}

#[test]
fn oversized_bridge_payload_is_rejected_fail_soft_without_state() {
    let dir = TestDir::new();
    let project = project(&dir);
    let huge = "x".repeat(5 * 1024 * 1024);
    let value = bridge_with_stdin(&project, dir.path(), "chat.message", &huge, None);
    assert_eq!(value["ok"], json!(false));
    assert!(value["error"].as_str().unwrap_or_default().contains("cap"));
    assert!(!project.join(".ocg").exists());
}

#[test]
fn the_bridge_path_carries_the_mandatory_budget_configuration() {
    // `ocg __bridge context.observe` can drive a provider-costly rollover
    // resume, so the bridge must parse the mandatory budget configuration
    // rather than silently falling back to the permissive controller defaults.
    // An invalid budget config therefore fails the bridge closed.
    let dir = TestDir::new();
    let broken_project = project(&dir);
    write_project(
        &broken_project.join(".ocg.yaml"),
        // `hardLimitMicros` without a currency is invalid.
        json!({"budget": {"hardLimitMicros": 1000000}}),
    );
    let value = bridge(&broken_project, dir.path(), "context.observe", &json!({}));
    assert_eq!(value["ok"], json!(false));
    assert!(
        value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("budget"),
        "the bridge must consult the budget configuration: {value}"
    );

    // A valid budget config is parsed and does not fail the bridge.
    let dir = TestDir::new();
    let valid_project = project(&dir);
    write_project(
        &valid_project.join(".ocg.yaml"),
        json!({"budget": {
            "currency": "USD",
            "hardLimitMicros": 1000000,
            "estimatedOperationCostMicros": 100000
        }}),
    );
    let value = bridge(&valid_project, dir.path(), "context.observe", &json!({}));
    assert!(
        !value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("budget"),
        "a valid budget configuration must not error: {value}"
    );
}

#[cfg(unix)]
#[test]
fn models_does_not_require_plugin_materialization_or_a_writable_project() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = project(&dir);
    // A file where the state directory must be makes materialization fail.
    std::fs::write(project.join(".ocg"), b"not a directory").unwrap();
    seed_profile(&project);
    let marker = dir.join("models-marker");
    let script = dir.join("fake-opencode.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nprintf ok > \"{}\"\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let project_arg = project.to_string_lossy().into_owned();
    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", &script)
        .args(["--project", project_arg.as_str(), "models"])
        .output()
        .expect("run ocg models");
    assert!(
        output.status.success(),
        "ocg models must not require plugin materialization: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(marker.is_file(), "ocg models must still reach OpenCode");
    // The state path is untouched (still the original file).
    assert!(project.join(".ocg").is_file());
}

#[cfg(unix)]
#[test]
fn launch_propagates_explicit_config_paths_to_the_bridge_environment() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = project(&dir);
    let user_config = dir.join("explicit-user.yaml");
    let project_config = dir.join("explicit-project.yaml");
    write_yaml(&user_config, &json!({}));
    write_project(
        &project_config,
        json!({"verification": {"stages": {"normal": {"commands": ["cargo check --locked"]}}}}),
    );
    let marker = dir.join("bridge-env");
    let script = dir.join("fake-opencode.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nprintf '%s\\n%s\\n' \"$OCG_USER_CONFIG\" \"$OCG_PROJECT_CONFIG\" > \"{}\"\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = base_command(&project, dir.path())
        .env("OCG_OPENCODE", &script)
        .args([
            "--user-config",
            user_config.to_str().unwrap(),
            "--project-config",
            project_config.to_str().unwrap(),
            "run",
            "hello",
        ])
        .output()
        .expect("launch ocg");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let values = std::fs::read_to_string(marker).unwrap();
    let lines: Vec<&str> = values.lines().collect();
    assert_eq!(
        lines,
        [
            user_config.to_str().unwrap(),
            project_config.to_str().unwrap()
        ]
    );
}
