//! CLI tests for the `ocg doctor` sections added with local telemetry:
//! repository map/index, symbol index, context cache, checkpoints, verification,
//! telemetry and capability planning. Doctor is strictly read-only.

mod common;

use common::TestDir;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn base_command(cwd: &Path, work: &Path) -> Command {
    seed_profile(cwd);
    build_command(cwd, work)
}

fn build_command(cwd: &Path, work: &Path) -> Command {
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
        .env_remove("OCG_TELEMETRY")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("OCG_ORCHESTRATION")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env("PATH", "");
    command
}

/// Doctor needs a structurally valid OCG Profile. Seed the project unless the
/// test wrote its own config, so unrelated doctor sections exercise real
/// configuration rather than a Profile-missing failure.
fn seed_profile(target: &Path) {
    let path = target.join(".ocg.yaml");
    if path.is_file() {
        return;
    }
    if let Ok(text) = serde_yaml_ng::to_string(&common::project_profile()) {
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(&path, text);
    }
}

/// Seed the project named by `--project` in `args`, else the working directory.
fn seed_profile_for(args: &[&str], cwd: &Path) {
    let target = args
        .windows(2)
        .find(|pair| pair[0] == "--project")
        .map(|pair| PathBuf::from(pair[1]))
        .unwrap_or_else(|| cwd.to_path_buf());
    seed_profile(&target);
}

/// Build a command whose project Profile is seeded for `args`.
fn doctor_command(cwd: &Path, work: &Path, args: &[&str]) -> Command {
    seed_profile_for(args, cwd);
    let mut command = build_command(cwd, work);
    command.args(args);
    command
}

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    doctor_command(cwd, work, args).output().expect("run ocg")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Collapse rendered wrapping (newlines + indentation) back to single spaces so
/// an assertion can target a logical phrase without depending on column layout.
fn flattened(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Whether `text` contains a status-first check with `token` before `label`.
/// Status must precede the label on its own rendered line.
fn has_status_first_check(text: &str, token: &str, label: &str) -> bool {
    text.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with(&format!("[{token}]")) && line.contains(label)
    })
}

fn write_project_file(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

#[cfg(unix)]
fn write_executable(path: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn doctor_reports_the_new_sections_and_creates_nothing() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    for label in [
        "repository map/index",
        "symbol index",
        "context cache",
        "task checkpoints",
        "verification config",
        "telemetry",
        "tool capability planner",
        "sensitive-file exclusions",
        "orchestration",
        "orchestration plugin",
        "orchestration state",
        "orchestration missions",
        "projection",
        "context activation",
        "verification integration",
    ] {
        assert!(text.contains(label), "missing '{label}' in\n{text}");
    }
    // The output is grouped by subsystem with concise human-facing names.
    for section in [
        "Environment & Proxy",
        "Configuration",
        "Lead",
        "Workers",
        "Runtime",
        "Effective State",
        "Project Infrastructure",
        "Summary",
    ] {
        assert!(
            text.lines().any(|line| line == section),
            "missing section '{section}' in\n{text}"
        );
    }
    // Status is always first, so a check is scannable independent of label.
    assert!(
        has_status_first_check(&text, "PASS", "project root"),
        "{text}"
    );
    assert!(
        has_status_first_check(&text, "INFO", "repository map/index"),
        "{text}"
    );
    // Missing optional state is informational, never a failure.
    assert!(text.contains("not built"), "{text}");
    assert!(text.contains("none saved"), "{text}");
    assert!(text.contains("not present"), "{text}");
    assert!(
        !project.join(".ocg").exists(),
        "doctor must not create index, cache, logs or telemetry"
    );
}

#[test]
fn doctor_warns_on_a_corrupt_index_without_failing_or_creating_state() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, "src/lib.rs", "pub fn a() {}\n");
    write_project_file(&project, ".ocg/index/context-index.json", "{not valid json");
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(
        output.status.success(),
        "a corrupt index is a warning, not a failure: {}",
        stdout(&output)
    );
    let text = stdout(&output);
    assert!(text.contains("repository map/index"), "{text}");
    assert!(text.contains("unreadable or corrupt"), "{text}");
    assert!(
        !project.join(".ocg/telemetry").exists(),
        "doctor must not create telemetry"
    );
}

#[test]
fn doctor_reports_missions_and_warns_on_a_corrupt_record_without_quarantining() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        ".ocg/orchestration/missions/task-0123456789abcdef.json",
        r#"{"schema_version":1,"mission_id":"task-0123456789abcdef","generation":1,
            "status":"active","phase":"build","session_id":"s"}"#,
    );
    write_project_file(
        &project,
        ".ocg/orchestration/missions/task-fedcba9876543210.json",
        "{not valid json",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("orchestration missions"), "{text}");
    assert!(text.contains("1 mission(s)"), "{text}");
    assert!(text.contains("1 unreadable record(s)"), "{text}");
    // Doctor stays read-only: it reports the unreadable record, it never
    // quarantines or replaces it.
    assert!(
        project
            .join(".ocg/orchestration/missions/task-fedcba9876543210.json")
            .is_file(),
        "doctor must not move durable Mission records"
    );
    assert!(!project
        .join(".ocg/orchestration/missions/task-fedcba9876543210.corrupt.json")
        .exists());
}

#[test]
fn doctor_warns_on_corrupt_telemetry_lines_but_does_not_fail() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(&project, ".ocg/telemetry/events.jsonl", "not json at all\n");
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("telemetry"), "{text}");
    assert!(text.contains("corrupt line"), "{text}");
    // The corrupt line itself is never echoed.
    assert!(!text.contains("not json at all"), "{text}");
}

#[test]
fn doctor_warns_on_unsupported_telemetry_schema_without_printing_the_event() {
    let dir = TestDir::new();
    let project = dir.project();
    write_project_file(
        &project,
        ".ocg/telemetry/events.jsonl",
        "{\"schema_version\":999,\"timestamp\":1,\"task_id\":\"task-future\",\"outcome\":\"success\"}\n",
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("unsupported schema line"), "{text}");
    assert!(!text.contains("task-future"), "{text}");
}

#[cfg(unix)]
#[test]
fn doctor_reports_static_config_separately_from_available_runtime_models() {
    let dir = TestDir::new();
    let project = dir.project();
    let script = dir.join("fake-opencode.sh");
    write_executable(
        &script,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra volcengine-coding-plan/kimi-k2.7-code volcengine-coding-plan/kimi-k3 opencode-go/deepseek-v4.1-flash opencode-go/glm-5.3-flash opencode-go/glm-5.3; exit 0; fi\nexit 2\n",
    );
    let output = base_command(&project, dir.path())
        .env("OCG_OPENCODE", &script)
        .args(["--disable-proxy", "doctor"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("static config"), "{text}");
    assert!(text.contains("runtime family"), "{text}");
    assert!(
        text.contains("openai/gpt-5.6-sol (variant provider-default)"),
        "{text}"
    );
    assert!(text.contains("disabled by CLI"), "{text}");
}

#[cfg(unix)]
#[test]
fn doctor_distinguishes_missing_provider_and_missing_model() {
    let dir = TestDir::new();
    let project = dir.project();
    let script = dir.join("fake-opencode-partial.sh");
    write_executable(
        &script,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nexit 2\n",
    );
    // The catalogue exposes only one `openai` model, so one route under a
    // present provider is a *missing model* while every other provider is a
    // *missing provider*. Doctor must distinguish them.
    let mut profile = common::project_profile();
    profile["routing"]["roles"] = serde_json::json!({
        "build": {"model": "gpt-6-astra", "description": "Implementation."},
        "verify": {"model": "glm-5.3-flash", "description": "Independent review."}
    });
    fs::write(
        project.join(".ocg.yaml"),
        serde_yaml_ng::to_string(&profile).unwrap(),
    )
    .unwrap();
    let output = base_command(&project, dir.path())
        .env("OCG_OPENCODE", &script)
        .arg("doctor")
        .output()
        .unwrap();
    assert!(!output.status.success(), "missing routes must fail doctor");
    let text = stdout(&output);
    // Long explanations wrap onto continuation lines, so match the logical
    // phrase against the whitespace-normalized output.
    let flat = flattened(&text);
    assert!(flat.contains("provider is not currently exposed"), "{text}");
    assert!(flat.contains("model is not currently exposed"), "{text}");
    // A FAIL must be visible in the summary, not only in the failing line.
    assert!(text.contains("failures"), "{text}");
    assert!(!text.contains("0 failures"), "{text}");
}

/// The layering section, the effective Lead contracts, the worker router and
/// the summary are all present for a plain project with no overrides.
#[test]
fn doctor_reports_layering_contracts_and_summary() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    for label in [
        "Configuration",
        "Lead",
        "Workers",
        "Summary",
        "defaults",
        "user config",
        "project config",
        "project root",
        "Profile",
        "runnable models",
        "default agent",
    ] {
        assert!(text.contains(label), "missing '{label}' in\n{text}");
    }
    // Doctor reports the OCG-owned Profile selection, not a fixed tier.
    assert!(
        text.contains("openai/gpt-5.6-sol (variant provider-default)"),
        "{text}"
    );
    assert!(!text.to_lowercase().contains("throttle"), "{text}");
    // The Worker Router is reported alongside, and is not the Lead.
    assert!(
        text.contains("volcengine-coding-plan/kimi-k2.7-code"),
        "{text}"
    );
    assert!(text.contains("opencode-go/deepseek-v4.1-flash"), "{text}");
    // A clean environment has no FAIL.
    assert!(text.contains("0 failures"), "{text}");
}

#[test]
fn doctor_reports_missing_project_config_as_info() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("project config"), "{text}");
    assert!(text.contains("not present"), "{text}");
}

#[test]
fn doctor_marks_user_and_project_layers_as_found() {
    let dir = TestDir::new();
    let project = dir.project();
    let user = dir.join("user-config.yaml");
    let project_config = project.join("project-config.yaml");
    // A user layer may set policy, but the project Profile stays authoritative.
    let mut profile = common::project_profile();
    profile["profile"]["defaultModel"] = serde_json::json!("gpt-6-astra");
    fs::write(
        &user,
        serde_yaml_ng::to_string(&serde_json::json!({
            "observability": {"enabled": true}
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(&project_config, serde_yaml_ng::to_string(&profile).unwrap()).unwrap();
    let output = doctor_command(dir.path(), dir.path(), &["doctor"])
        .args([
            "--user-config",
            user.to_str().unwrap(),
            "--project-config",
            project_config.to_str().unwrap(),
            "doctor",
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains(user.to_str().unwrap()), "{text}");
    assert!(text.contains(project_config.to_str().unwrap()), "{text}");
    // Both layers are applied, and the project Profile selection is reported.
    assert!(
        text.lines()
            .any(|line| line.contains("lead model") && line.contains("openai/gpt-6-astra")),
        "{text}"
    );
}

#[cfg(unix)]
#[test]
fn doctor_warns_about_a_socks_proxy_without_leaking_its_value() {
    let dir = TestDir::new();
    let project = dir.project();
    let output = doctor_command(
        dir.path(),
        dir.path(),
        &["--project", project.to_str().unwrap(), "doctor"],
    )
    .env(
        "ALL_PROXY",
        "socks5h://alice:wonderland@proxy-internal:1080",
    )
    .args(["--project", project.to_str().unwrap(), "doctor"])
    .output()
    .unwrap();
    // SOCKS is a warning, never a failure: the child keeps the value.
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("ALL_PROXY"), "{text}");
    assert!(text.contains("SOCKS"), "{text}");
    assert!(text.contains("preserved"), "{text}");
    assert!(text.contains("ignore it"), "{text}");
    for secret in ["wonderland", "alice", "proxy-internal", "1080"] {
        assert!(!text.contains(secret), "leaked '{secret}' in\n{text}");
    }
}

#[cfg(unix)]
#[test]
fn doctor_reports_a_normal_http_proxy_without_its_value() {
    let dir = TestDir::new();
    let project = dir.project();
    let output = doctor_command(
        dir.path(),
        dir.path(),
        &["--project", project.to_str().unwrap(), "doctor"],
    )
    .env("HTTP_PROXY", "http://proxy-internal:3128")
    .env("HTTPS_PROXY", "http://proxy-internal:3129")
    .env("NO_PROXY", "localhost,.internal")
    .args(["--project", project.to_str().unwrap(), "doctor"])
    .output()
    .unwrap();
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    assert!(text.contains("HTTP_PROXY"), "{text}");
    assert!(text.contains("HTTPS_PROXY"), "{text}");
    assert!(text.contains("NO_PROXY"), "{text}");
    assert!(text.contains("present"), "{text}");
    assert!(!text.contains("proxy-internal"), "leaked value in\n{text}");
}

#[cfg(unix)]
#[test]
fn doctor_never_prints_proxy_or_provider_credentials() {
    let dir = TestDir::new();
    let project = dir.project();
    // Build the token shape at runtime so this test source does not itself look
    // like a committed credential to the repository hygiene scan.
    let fake_pat = format!("ghp_{}", "0123456789abcdefghijklmnopqrstuvwxyz");
    let output = doctor_command(
        dir.path(),
        dir.path(),
        &["--project", project.to_str().unwrap(), "doctor"],
    )
    .env("HTTPS_PROXY", "http://alice:wonderland@proxy-internal:3128")
    .env("ALL_PROXY", "socks5://bob:builder@proxy-internal:1080")
    .env("GH_TOKEN", &fake_pat)
    .env("GITHUB_TOKEN", "github_pat_value_that_must_not_render")
    .args(["--project", project.to_str().unwrap(), "doctor"])
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "warnings must not fail doctor: {}",
        stdout(&output)
    );
    let text = stdout(&output);
    assert!(!text.contains(&fake_pat), "leaked GH_TOKEN in\n{text}");
    for secret in [
        "wonderland",
        "alice",
        "bob",
        "builder",
        "proxy-internal",
        "github_pat_value_that_must_not_render",
    ] {
        assert!(!text.contains(secret), "leaked '{secret}' in\n{text}");
    }
}

#[cfg(unix)]
#[test]
fn doctor_warns_when_the_system_and_runtime_versions_differ() {
    let dir = TestDir::new();
    let project = dir.project();
    let bin = dir.join("bin");
    fs::create_dir_all(&bin).unwrap();
    write_executable(
        &bin.join("opencode"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 9.9.9; exit 0; fi\nexit 2\n",
    );
    let runtime = dir.join("fake-runtime.sh");
    write_executable(
        &runtime,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra volcengine-coding-plan/kimi-k2.7-code volcengine-coding-plan/kimi-k3 opencode-go/deepseek-v4.1-flash opencode-go/glm-5.3-flash opencode-go/glm-5.3; exit 0; fi\nexit 2\n",
    );
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = doctor_command(
        dir.path(),
        dir.path(),
        &["--project", project.to_str().unwrap(), "doctor"],
    )
    .env("PATH", path)
    .env("OCG_OPENCODE", &runtime)
    .args(["--project", project.to_str().unwrap(), "doctor"])
    .output()
    .unwrap();
    let text = stdout(&output);
    assert!(text.contains("opencode on PATH"), "{text}");
    assert!(text.contains("9.9.9"), "{text}");
    assert!(text.contains("system PATH is 9.9.9"), "{text}");
    assert!(text.contains("runtime (explicit) is 1.18.31"), "{text}");
}

/// Parse the four summary counters out of a rendered doctor report.
fn summary_counts(text: &str) -> (usize, usize, usize, usize) {
    let flat = flattened(text);
    let after = flat
        .split("Summary")
        .last()
        .unwrap_or_else(|| panic!("no Summary section in\n{text}"));
    let words: Vec<&str> = after.split_whitespace().collect();
    let number_before = |marker: &str| -> usize {
        words
            .iter()
            .position(|word| *word == marker)
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| words.get(index))
            .and_then(|word| word.parse().ok())
            .unwrap_or_else(|| panic!("no counter before '{marker}' in '{after}'"))
    };
    (
        number_before("passed"),
        number_before("warnings"),
        number_before("failures"),
        number_before("informational"),
    )
}

#[test]
fn doctor_puts_status_before_the_label_even_for_long_labels() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    // Labels longer than the historical 18-character column are still
    // status-first, so the status indicator never drifts.
    assert!(
        has_status_first_check(&text, "INFO", "sensitive-file exclusions"),
        "{text}"
    );
    assert!(
        has_status_first_check(&text, "PASS", "tool capability planner"),
        "{text}"
    );
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') {
            assert!(line.starts_with("  ["), "status must be first: {line:?}");
            assert!(
                ["PASS", "WARN", "FAIL", "INFO"]
                    .iter()
                    .any(|token| trimmed.starts_with(&format!("[{token}]"))),
                "unexpected status token: {line:?}"
            );
        }
    }
}

#[test]
fn doctor_separates_logical_sections_with_a_blank_line() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    let lines: Vec<&str> = text.lines().collect();
    for section in [
        "Environment & Proxy",
        "Configuration",
        "Lead",
        "Workers",
        "Runtime",
        "Effective State",
        "Project Infrastructure",
        "Summary",
    ] {
        let index = lines
            .iter()
            .position(|line| *line == section)
            .unwrap_or_else(|| panic!("missing section '{section}' in\n{text}"));
        assert!(index > 0, "'{section}' must not be the first line");
        assert_eq!(
            lines[index - 1],
            "",
            "section '{section}' must be preceded by a blank line\n{text}"
        );
    }
}

#[test]
fn doctor_summary_counts_derive_from_the_rendered_checks() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    let (passed, warnings, failures, informational) = summary_counts(&text);
    let count = |token: &str| {
        text.lines()
            .filter(|line| line.starts_with(&format!("  [{token}]")))
            .count()
    };
    assert_eq!(count("PASS"), passed, "{text}");
    assert_eq!(count("WARN"), warnings, "{text}");
    assert_eq!(count("FAIL"), failures, "{text}");
    assert_eq!(count("INFO"), informational, "{text}");
    assert_eq!(failures, 0, "a clean environment has no FAIL: {text}");
}

#[test]
fn doctor_output_is_plain_text_without_required_ansi() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    // Redirected/SSH/pasted output must not depend on color: no escape bytes.
    assert!(!text.contains('\u{1b}'), "doctor emitted ANSI in\n{text}");
}

#[test]
fn doctor_is_deterministic_for_a_fixed_environment() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let first = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    let second = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(first.status.success(), "{}", stdout(&first));
    assert!(second.status.success(), "{}", stdout(&second));
    let first = stdout(&first);
    let second = stdout(&second);
    // Stable header and stable counters; only volatile fields (for example the
    // update-cache age) may differ between runs.
    assert_eq!(first.lines().next(), second.lines().next());
    assert_eq!(first.lines().last(), second.lines().last());
    assert_eq!(summary_counts(&first), summary_counts(&second));
}

#[cfg(unix)]
#[test]
fn doctor_wraps_long_diagnostics_onto_continuation_lines() {
    let dir = TestDir::new();
    let project = dir.project();
    let runtime = dir.join("fake-v2.sh");
    write_executable(
        &runtime,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 2.0.11; exit 0; fi\nexit 2\n",
    );
    let output = doctor_command(
        dir.path(),
        dir.path(),
        &["--project", project.to_str().unwrap(), "doctor"],
    )
    .env("OCG_OPENCODE", &runtime)
    .args(["--project", project.to_str().unwrap(), "doctor"])
    .output()
    .unwrap();
    assert!(output.status.success(), "{}", stdout(&output));
    let text = stdout(&output);
    let flat = flattened(&text);
    assert!(
        flat.contains(
            "not checked (pass --effective to probe the catalogue of an OCG-owned OpenCode V2 runtime)"
        ),
        "{text}"
    );
    let lines: Vec<&str> = text.lines().collect();
    let start = lines
        .iter()
        .position(|line| line.contains("not checked (pass --effective"))
        .unwrap_or_else(|| panic!("the long V2 warning is missing from\n{text}"));
    assert!(
        lines.len() > start + 1,
        "the long explanation must continue on another line\n{text}"
    );
    let continuation = lines[start + 1];
    assert!(
        continuation.starts_with("  ") && !continuation.trim_start().starts_with('['),
        "secondary text must be an indented continuation\n{text}"
    );
    for line in &lines {
        // A single unbreakable path may exceed the width, and a structured
        // `[LABEL] name <path>` line is long only because of the path it
        // reports (CI workspaces and cross-compile target dirs are deep).
        // Prose — the non-path tokens — never exceeds the width.
        let unbreakable = line.split_whitespace().count() <= 1;
        let mut prose = line.chars().count();
        for word in line.split_whitespace() {
            if word.contains('/') {
                prose -= word.chars().count();
            }
        }
        assert!(
            unbreakable || prose <= 100,
            "line is an unbounded wall of text ({} chars): {line:?}",
            line.chars().count()
        );
    }
}
