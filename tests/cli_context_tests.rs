//! CLI tests for `ocg context`, `ocg context symbols` and `ocg cache`.
//!
//! These are deterministic and offline: the context engine never networks.

mod common;

use common::{project_profile, write_yaml, TestDir};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn base_command(cwd: &Path, work: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(cwd)
        .env("OCG_USER_CONFIG", work.join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE_BIN");
    command
}

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    // Dry-run and launch require a structurally valid OCG Profile.
    let profile = cwd.join(".ocg.yaml");
    if !profile.is_file() {
        write_yaml(&profile, &project_profile());
    }
    base_command(cwd, work)
        .args(args)
        .output()
        .expect("run ocg")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn write_project_file(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

#[test]
fn context_plan_is_printed_and_deterministic() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/parser.rs", "pub fn parse() {}\n");
    let project_arg = project.to_string_lossy().into_owned();

    let first = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "fix", "parser"],
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let text = stdout(&first);
    assert!(text.contains("context plan"), "{text}");
    assert!(text.contains("src/parser.rs"), "{text}");

    let second = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "fix", "parser"],
    );
    assert_eq!(text, stdout(&second));
}

#[test]
fn context_symbols_returns_a_json_diagnostic() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        "src/parser.rs",
        "pub fn parse(input: &str) -> u32 {\n    0\n}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "symbols", "parse"],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_str(&stdout(&output)).expect("json");
    assert_eq!(value["query"], "parse");
    assert_eq!(value["definition"]["name"], "parse");
    assert!(!value["symbols"].as_array().unwrap().is_empty());
}

#[test]
fn cache_stats_and_clean_never_touch_the_runtime() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    let runtime = project.join(".ocg/runtime/opencode");
    fs::create_dir_all(&runtime).unwrap();
    let project_arg = project.to_string_lossy().into_owned();

    let plan = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "a"],
    );
    assert!(plan.status.success());

    let stats = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "cache", "stats"],
    );
    assert!(stats.status.success());
    assert!(stdout(&stats).contains("context cache"));

    let clean = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "cache", "clean"],
    );
    assert!(clean.status.success());
    assert!(stdout(&clean).contains("removed"));
    assert!(runtime.exists(), "cache clean must not remove the runtime");
}

#[test]
fn dry_run_does_not_prepare_context() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    write_project_file(
        &project,
        ".ocg.yaml",
        &serde_yaml_ng::to_string(&project_profile()).unwrap(),
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--dry-run"],
    );
    assert!(output.status.success());
    // The Profile file is configuration, not prepared runtime state.
    assert!(
        !project.join(".ocg/context").exists(),
        "--dry-run must stay pure and not prepare context"
    );
}

#[test]
fn disabled_context_reports_no_work_and_writes_nothing() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    let mut config = project_profile();
    config["context"] = serde_json::json!({"enabled": false});
    write_project_file(
        &project,
        ".ocg.yaml",
        &serde_yaml_ng::to_string(&config).unwrap(),
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "a"],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout(&output).contains("disabled"));
    assert!(
        !project.join(".ocg/context").exists(),
        "disabled context must not create index or cache state"
    );
}

#[cfg(unix)]
#[test]
fn run_activates_orchestration_plugin_without_preparing_context() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    let marker = dir.join("marker.txt");
    let script = dir.join("fake-opencode.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\nprintf done > \"{}\"\n",
        marker.display()
    );
    fs::write(&script, body).unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

    let project_arg = project.to_string_lossy().into_owned();
    write_project_file(
        &project,
        ".ocg.yaml",
        &serde_yaml_ng::to_string(&project_profile()).unwrap(),
    );
    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", &script)
        .args(["--project", project_arg.as_str(), "run", "hello"])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(marker.is_file(), "the launch must still reach OpenCode");
    // An ordinary launch now materializes the generated orchestration plugin
    // (that is the activation), but it still does not build the context index.
    assert!(
        project
            .join(".ocg/orchestration/plugin/ocg-orchestration.js")
            .is_file(),
        "an ordinary launch must materialize the orchestration plugin"
    );
    assert!(
        !project.join(".ocg/index").exists(),
        "an ordinary launch must not build the context index"
    );
}
