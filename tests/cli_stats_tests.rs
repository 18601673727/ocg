//! CLI tests for `ocg stats`: read-only, offline, local-only aggregation.

mod common;

use common::TestDir;
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
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_TELEMETRY");
    command
}

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    base_command(cwd, work)
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

fn write_project_file(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

#[test]
fn stats_on_missing_telemetry_succeeds_and_creates_no_state() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "stats"],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("telemetry"), "{text}");
    assert!(text.contains("no telemetry events recorded yet"), "{text}");
    assert!(text.contains("enabled:     yes"), "{text}");
    assert!(text.contains("local-only:  yes"), "{text}");
    assert!(
        !project.join(".ocg").exists(),
        "stats must not create telemetry state"
    );
}

#[test]
fn stats_reports_disabled_telemetry() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        ".ocg.yaml",
        "{\"telemetry\": {\"enabled\": false}}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "stats"],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("enabled:     no"), "{text}");
    assert!(
        !project.join(".ocg/telemetry").exists(),
        "stats must not create telemetry state"
    );
}

#[test]
fn context_records_estimates_and_never_the_task_text() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        "src/parser.rs",
        "pub fn parse() -> u32 {\n    0\n}\n",
    );
    write_project_file(
        &project,
        "src/lexer.rs",
        "pub fn lex() -> u32 {\n    1\n}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let secret_task = "SECRET-TASK-TEXT fix the parser";

    let context = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", secret_task],
    );
    assert!(context.status.success(), "{}", stderr(&context));

    let telemetry =
        fs::read_to_string(project.join(".ocg/telemetry/events.jsonl")).expect("telemetry file");
    assert!(!telemetry.contains("SECRET-TASK-TEXT"), "{telemetry}");
    assert!(!telemetry.contains("fix the parser"), "{telemetry}");
    assert!(telemetry.contains("task-"), "{telemetry}");
    assert!(
        telemetry.contains("\"source\":\"estimated\""),
        "{telemetry}"
    );

    let stats = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "stats"],
    );
    assert!(stats.status.success(), "{}", stderr(&stats));
    let text = stdout(&stats);
    assert!(text.contains("estimate only"), "{text}");
    assert!(text.contains("type:        context"), "{text}");
    assert!(text.contains("candidate:"), "{text}");
    assert!(text.contains("capsule:"), "{text}");
    assert!(text.contains("capabilities (1):"), "{text}");
    assert!(text.contains("filesystem: 1"), "{text}");
    assert!(!text.contains("SECRET-TASK-TEXT"), "{text}");

    // The second identical plan uses the cache; the aggregate reports the hit.
    let context_again = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", secret_task],
    );
    assert!(context_again.status.success());
    let stats_again = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "stats"],
    );
    let text = stdout(&stats_again);
    assert!(text.contains("cache hits:  1"), "{text}");
    assert!(text.contains("cache misses: 1"), "{text}");
}

#[test]
fn stats_rejects_unknown_options_and_positional_arguments() {
    let dir = TestDir::new();
    let unknown = run(dir.path(), dir.path(), &["stats", "--bogus"]);
    assert_eq!(unknown.status.code(), Some(2));
    assert!(
        stderr(&unknown).contains("unknown option"),
        "{}",
        stderr(&unknown)
    );

    let extra = run(dir.path(), dir.path(), &["stats", "extra"]);
    assert_eq!(extra.status.code(), Some(2));
    assert!(
        stderr(&extra).contains("takes no arguments"),
        "{}",
        stderr(&extra)
    );
}

#[test]
fn stats_pretty_is_json_with_stable_fields() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() -> u32 {\n    0\n}\n");
    let project_arg = project.to_string_lossy().into_owned();
    let context = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "a"],
    );
    assert!(context.status.success());

    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "stats", "--pretty"],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let value: Value = serde_json::from_str(&stdout(&output)).expect("stats json");
    assert_eq!(value["enabled"], true);
    assert_eq!(value["local_only"], true);
    assert_eq!(value["events"], 1);
    assert_eq!(value["aggregate"]["events"], 1);
    assert_eq!(value["aggregate"]["outcomes"]["success"], 1);
    assert!(value["aggregate"]["context"]["candidate_bytes"].is_u64());
    assert!(value["path"]
        .as_str()
        .unwrap()
        .ends_with(".ocg/telemetry/events.jsonl"));
}

#[test]
fn stats_after_verify_reports_attempts_and_log_bytes() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() -> u32 {\n    0\n}\n");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("check.sh");
        fs::write(
            &script,
            "#!/bin/sh\necho 'noise progress'\necho 'error: boom at src/lib.rs:1:5'\necho 'test result: FAILED. 0 passed; 1 failed; 0 ignored'\nexit 1\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        let config = serde_json::json!({
            "verification": {
                "stages": {"normal": {"commands": [
                    {"program": script.to_string_lossy(), "args": []}
                ]}}
            }
        });
        write_project_file(&project, ".ocg.yaml", &format!("{config}\n"));
        let project_arg = project.to_string_lossy().into_owned();

        let verify = run(
            dir.path(),
            dir.path(),
            &["--project", &project_arg, "verify", "normal"],
        );
        // The failing verification itself is a normal exit code 1.
        assert_eq!(verify.status.code(), Some(1), "{}", stderr(&verify));

        let stats = run(
            dir.path(),
            dir.path(),
            &["--project", &project_arg, "stats"],
        );
        assert!(stats.status.success(), "{}", stderr(&stats));
        let text = stdout(&stats);
        assert!(text.contains("type:        verification"), "{text}");
        assert!(text.contains("attempts:    1"), "{text}");
        assert!(text.contains("failed:      1"), "{text}");
        assert!(text.contains("outcome:     failure"), "{text}");
        assert!(text.contains("raw:"), "{text}");
        assert!(text.contains("distilled:"), "{text}");

        // Command strings and raw output never enter telemetry.
        let telemetry = fs::read_to_string(project.join(".ocg/telemetry/events.jsonl")).unwrap();
        assert!(!telemetry.contains("check.sh"), "{telemetry}");
        assert!(!telemetry.contains("error: boom"), "{telemetry}");
    }
}
