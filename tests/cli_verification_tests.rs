//! CLI tests for `ocg verify`, `ocg tools` and `ocg checkpoint`, plus one
//! end-to-end fixture flow. Deterministic and offline: no network and no paid
//! API is used. The only child process is a local test script.

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
        .env_remove("OCG_OPENCODE_BIN");
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
fn verify_without_configured_commands_runs_nothing() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "verify", "normal"],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("not_run"), "{text}");
    assert!(text.contains("no commands"), "{text}");
}

#[test]
fn tools_reports_git_only_boundary() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "tools",
            "commit",
            "the",
            "git",
            "fix",
        ],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("filesystem"), "{text}");
    assert!(text.contains("git"), "{text}");
    assert!(text.contains("cloud"), "{text}"); // denied list
    assert!(text.contains("does not activate"), "{text}");
}

#[test]
fn invalid_verification_config_is_reported() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        ".ocg.yaml",
        "{\"verification\": {\"stages\": {\"turbo\": {}}}}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "validate"],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("verification"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn shell_control_commands_are_rejected() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        ".ocg.yaml",
        "{\"verification\": {\"stages\": {\"fast\": {\"commands\": [\"cargo check; rm -rf .\"]}}}}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "validate"],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("forbidden shell operator")
            || stderr(&output).contains("verification"),
        "{}",
        stderr(&output)
    );
}

#[cfg(unix)]
fn write_failing_test_script(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let body = "#!/bin/sh\n\
                cat <<'EOF'\n\
                running 3 tests\n\
                test tests::a ... ok\n\
                test tests::b ... FAILED\n\
                error[E0308]: mismatched types\n\
                  --> src/lib.rs:4:5\n\
                test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 1 filtered out\n\
                EOF\n\
                exit 101\n";
    write_project_file(
        path.parent().unwrap(),
        path.file_name().unwrap().to_str().unwrap(),
        body,
    );
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[cfg(unix)]
#[test]
fn verify_runs_configured_command_distills_failure_and_keeps_raw_logs() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    let script = dir.join("failing-tests.sh");
    write_failing_test_script(&script);
    let config = serde_json::json!({
        "verification": {
            "stages": {
                "normal": {
                    "commands": [{"program": script.to_string_lossy(), "args": []}]
                }
            }
        }
    });
    write_project_file(&project, ".ocg.yaml", &format!("{config}\n"));
    let project_arg = project.to_string_lossy().into_owned();

    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "verify", "normal", "--pretty"],
    );
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let report: Value = serde_json::from_str(&stdout(&output)).expect("json report");
    assert_eq!(report["stage"], "normal");
    assert_eq!(report["results"][0]["success"], false);
    assert_eq!(report["results"][0]["exit"]["code"], 101);
    let failed = report["results"][0]["failed_tests"].as_array().unwrap();
    assert!(failed.iter().any(|value| value == "tests::b"), "{report}");
    let locations = report["results"][0]["source_locations"].as_array().unwrap();
    assert!(
        locations.iter().any(|value| value["path"] == "src/lib.rs"),
        "{report}"
    );
    let raw_log = report["results"][0]["raw_log"].as_str().unwrap();
    let log_path = project.join(raw_log);
    assert!(log_path.is_file(), "{}", log_path.display());
    let log_text = fs::read_to_string(&log_path).unwrap();
    assert!(log_text.contains("mismatched types"));

    // `cache clean` must never remove verification logs.
    let clean = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "cache", "clean"],
    );
    assert!(clean.status.success());
    assert!(log_path.is_file(), "cache clean removed a raw log");
}

#[test]
fn checkpoint_save_list_show_and_staleness() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/a.rs", "pub fn a() {}\n");
    let project_arg = project.to_string_lossy().into_owned();

    let save = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "checkpoint",
            "save",
            "--phase",
            "verify-to-debug",
            "--task",
            "fix a",
            "--decision",
            "prefer the smallest fix",
        ],
    );
    assert!(save.status.success(), "{}", stderr(&save));
    let saved = stdout(&save);
    let id = saved
        .split_whitespace()
        .map(|token| token.trim_matches(|ch| ch == '(' || ch == ')'))
        .find_map(|token| {
            let candidate = token.strip_suffix(".json").unwrap_or(token);
            candidate.starts_with("cp-").then(|| candidate.to_string())
        })
        .expect("checkpoint id in output");

    let list = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "checkpoint", "list"],
    );
    assert!(list.status.success(), "{}", stderr(&list));
    assert!(
        stdout(&list).contains("verify_to_debug"),
        "{}",
        stdout(&list)
    );

    // Edit a planned source: the checkpoint must be marked stale, not reused.
    fs::write(project.join("src/a.rs"), "pub fn a() { let _ = 1; }\n").unwrap();
    // `--pretty` may appear before the id: global options are position
    // independent.
    let show = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "checkpoint",
            "show",
            "--pretty",
            &id,
        ],
    );
    assert!(show.status.success(), "{}", stderr(&show));
    let value: Value = serde_json::from_str(&stdout(&show)).expect("json");
    assert_eq!(value["stale"], true, "{value}");
    let reasons = value["reasons"].as_array().unwrap();
    assert!(
        reasons
            .iter()
            .any(|reason| reason.as_str().unwrap_or("").contains("a.rs")),
        "{value}"
    );
}

/// The full fixture flow: context/capsule -> edit -> stale -> targeted verify
/// -> distilled failure -> checkpoint.
#[cfg(unix)]
#[test]
fn fixture_flow_context_edit_stale_verify_checkpoint() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/parser.rs", "pub fn parse() -> u32 { 0 }\n");
    write_project_file(
        &project,
        "tests/parser.rs",
        "use crate::parser::parse;\n#[test]\nfn parses() { let _ = parse(); }\n",
    );
    // A local git repository so the context engine can compute a diff.
    for args in [
        vec!["init", "-q"],
        vec!["add", "-A"],
        vec![
            "-c",
            "user.email=ocg@localhost",
            "-c",
            "user.name=test",
            "commit",
            "-q",
            "-m",
            "init",
        ],
    ] {
        let status = Command::new("git")
            .args(&args)
            .current_dir(&project)
            .status()
            .expect("git");
        assert!(status.success(), "git {args:?} failed");
    }
    // A failing verification command.
    let script = dir.join("failing.sh");
    write_failing_test_script(&script);
    let config = serde_json::json!({
        "verification": {
            "includeTestProposal": true,
            "stages": {"normal": {"commands": [{"program": script.to_string_lossy(), "args": []}]}}
        }
    });
    write_project_file(&project, ".ocg.yaml", &format!("{config}\n"));
    // Edit a source file so the diff and targeted proposal are non-empty.
    fs::write(
        project.join("src/parser.rs"),
        "pub fn parse() -> u32 { 1 }\n",
    )
    .unwrap();
    let project_arg = project.to_string_lossy().into_owned();

    // 1. Context plan includes the capability plan, capsule and proposal.
    let context = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "context",
            "fix",
            "parse",
            "--pretty",
        ],
    );
    assert!(context.status.success(), "{}", stderr(&context));
    let plan: Value = serde_json::from_str(&stdout(&context)).expect("plan json");
    assert_eq!(plan["sections"][0]["name"], "gear_instructions");
    assert_eq!(plan["sections"][7]["name"], "verification_state");
    assert!(plan["capsule"].is_object(), "{plan}");
    let candidates = plan["test_proposal"]["candidates"].as_array().unwrap();
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate["path"] == "tests/parser.rs"),
        "{plan}"
    );
    assert_eq!(plan["test_proposal"]["complete"], false);

    // 2. Save a checkpoint from the capsule, then edit the source so it goes
    //    stale instead of being silently reused.
    let save_early = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "checkpoint",
            "save",
            "--phase",
            "explore-to-build",
            "--task",
            "fix parse",
        ],
    );
    assert!(save_early.status.success(), "{}", stderr(&save_early));
    let early_id = checkpoint_id(&stdout(&save_early));
    fs::write(
        project.join("src/parser.rs"),
        "pub fn parse() -> u32 { 2 }\n",
    )
    .unwrap();
    let show = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "checkpoint",
            "show",
            &early_id,
            "--pretty",
        ],
    );
    assert!(show.status.success(), "{}", stderr(&show));
    let loaded: Value = serde_json::from_str(&stdout(&show)).expect("checkpoint json");
    assert_eq!(loaded["stale"], true, "{loaded}");

    // 3. Verify runs the configured command and distills the failure.
    let verify = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "verify", "normal", "--pretty"],
    );
    assert_eq!(verify.status.code(), Some(1), "{}", stderr(&verify));
    let report: Value = serde_json::from_str(&stdout(&verify)).expect("report json");
    assert_eq!(report["results"][0]["success"], false);
    assert!(report["results"][0]["raw_log"].is_string());
    assert!(report["results"][0]["output"]["failed_tests"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "tests::b"));

    // 4. Save the verify -> debug checkpoint and list both.
    let save = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "checkpoint",
            "save",
            "--phase",
            "verify-to-debug",
            "--task",
            "fix parse",
        ],
    );
    assert!(save.status.success(), "{}", stderr(&save));
    assert!(
        stdout(&save).contains("checkpoint saved"),
        "{}",
        stdout(&save)
    );
    let list = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "checkpoint", "list"],
    );
    assert!(list.status.success());
    assert!(stdout(&list).contains("explore_to_build"));
    assert!(stdout(&list).contains("verify_to_debug"));
}

fn checkpoint_id(output: &str) -> String {
    output
        .split_whitespace()
        .map(|token| token.trim_matches(|ch| ch == '(' || ch == ')'))
        .find_map(|token| {
            let candidate = token.strip_suffix(".json").unwrap_or(token);
            candidate.starts_with("cp-").then(|| candidate.to_string())
        })
        .expect("checkpoint id in output")
}

#[test]
fn capabilities_disabled_is_reflected_and_not_planned() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    write_project_file(
        &project,
        ".ocg.yaml",
        "{\"capabilities\": {\"enabled\": false}}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();

    let tools = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "tools",
            "commit",
            "the",
            "git",
            "branch",
            "--pretty",
        ],
    );
    assert!(tools.status.success(), "{}", stderr(&tools));
    let plan: Value = serde_json::from_str(&stdout(&tools)).expect("json");
    assert_eq!(plan["enabled"], false, "{plan}");
    assert!(
        plan["capabilities"].as_array().unwrap().is_empty(),
        "{plan}"
    );
    assert!(
        plan["notes"][0]
            .as_str()
            .unwrap_or("")
            .contains("capabilities.enabled=false"),
        "{plan}"
    );

    let text = run(
        dir.path(),
        dir.path(),
        &[
            "--project",
            &project_arg,
            "tools",
            "commit",
            "the",
            "git",
            "branch",
        ],
    );
    assert!(text.status.success());
    assert!(
        stdout(&text).contains("capability planning is disabled"),
        "{}",
        stdout(&text)
    );

    let context = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "context", "commit", "--pretty"],
    );
    assert!(context.status.success(), "{}", stderr(&context));
    let plan: Value = serde_json::from_str(&stdout(&context)).expect("plan json");
    assert_eq!(plan["capabilities"]["enabled"], false, "{plan}");
    assert!(
        plan["capabilities"]["capabilities"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{plan}"
    );
}

#[test]
fn legacy_commands_reject_unknown_options() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let cases: Vec<Vec<&str>> = vec![
        vec!["validate", "--bogus"],
        vec!["status", "--bogus"],
        vec!["routing", "--bogus"],
        vec!["layers", "--bogus"],
        vec!["build", "--bogus"],
        vec!["version", "--bogus"],
        vec!["doctor", "--bogus"],
        vec!["upgrade", "--bogus"],
        vec!["cache", "stats", "--bogus"],
        vec!["context", "task", "--bogus"],
    ];
    for case in cases {
        let mut args = vec!["--project", project_arg.as_str()];
        args.extend(case.iter().copied());
        let output = run(dir.path(), dir.path(), &args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{case:?} stdout={} stderr={}",
            stdout(&output),
            stderr(&output)
        );
        assert!(
            stderr(&output).contains("unknown option"),
            "{case:?}: {}",
            stderr(&output)
        );
    }
}

#[test]
fn verify_rejects_extra_positional_arguments() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "verify", "normal", "extra"],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr(&output).contains("at most one stage"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn checkpoint_list_rejects_extra_arguments() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "checkpoint", "list", "--bogus"],
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(
        stderr(&output).contains("checkpoint list takes no arguments"),
        "{}",
        stderr(&output)
    );

    let show = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "checkpoint", "show", "--bogus"],
    );
    assert_eq!(show.status.code(), Some(2));
    assert!(
        stderr(&show).contains("unknown checkpoint show option"),
        "{}",
        stderr(&show)
    );
}

#[cfg(unix)]
#[test]
fn verify_skips_context_when_disabled_but_still_runs_commands() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    let script = dir.join("failing-context.sh");
    write_failing_test_script(&script);
    let config = serde_json::json!({
        "context": {"enabled": false},
        "verification": {
            "stages": {"normal": {"commands": [{"program": script.to_string_lossy(), "args": []}]}}
        }
    });
    write_project_file(&project, ".ocg.yaml", &format!("{config}\n"));
    let project_arg = project.to_string_lossy().into_owned();

    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "verify", "normal", "--pretty"],
    );
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    let report: Value = serde_json::from_str(&stdout(&output)).expect("report json");
    assert_eq!(report["results"][0]["success"], false);
    assert!(report["test_proposal"].is_null(), "{report}");
    let notes: Vec<&str> = report["notes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        notes
            .iter()
            .any(|note| note.contains("context is disabled")),
        "{report}"
    );
    // No context index or cache was created; only verification logs exist.
    assert!(!project.join(".ocg/index/context-index.json").exists());
    assert!(!project.join(".ocg/cache").exists());
    assert!(project.join(".ocg/logs").is_dir());
}
