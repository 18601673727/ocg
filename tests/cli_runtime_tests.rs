//! CLI-level tests for the runtime commands and explicit-executable env.
//!
//! These never touch the network: `upgrade` points the API base at an
//! unreachable local address, and `version`/`doctor` are non-mutating.

mod common;

use common::{project_profile, write_yaml, TestDir};
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// Runtime-facing surfaces need a structurally valid OCG Profile. Seed it
/// unless the test wrote its own project config.
fn seed_profile(cwd: &Path) {
    let path = cwd.join(".ocg.yaml");
    if !path.is_file() {
        write_yaml(&path, &project_profile());
    }
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

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}

#[test]
fn version_reports_gear_platform_and_runtime_without_mutating() {
    let dir = TestDir::new();
    let output = run(dir.path(), dir.path(), &["version"]);
    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("OCG"), "{text}");
    assert!(text.contains("platform:"), "{text}");
    assert!(text.contains("opencode:"), "{text}");
    assert!(
        !dir.path().join(".ocg").exists(),
        "version must not bootstrap a runtime"
    );
}

#[test]
fn canonical_opencode_env_wins_over_the_compatibility_alias() {
    let dir = TestDir::new();
    let canonical = dir.join("canonical-opencode");
    let alias = dir.join("alias-opencode");
    write_executable(&canonical, "#!/bin/sh\necho 1.18.31\n");
    write_executable(&alias, "#!/bin/sh\necho 1.0.0\n");

    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", &canonical)
        .env("OCG_OPENCODE_BIN", &alias)
        .arg("version")
        .output()
        .expect("run");
    assert!(output.status.success());
    let text = stdout(&output);
    assert!(text.contains("1.18.31"), "{text}");
    assert!(
        text.contains(canonical.to_string_lossy().as_ref()),
        "canonical path should be reported: {text}"
    );
}

#[test]
fn broken_explicit_override_errors_instead_of_falling_back() {
    let dir = TestDir::new();
    let launch = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", "/definitely/not/here/opencode")
        .arg("run")
        .output()
        .expect("run");
    assert!(!launch.status.success());
    assert!(
        String::from_utf8_lossy(&launch.stderr).contains("explicit"),
        "{}",
        String::from_utf8_lossy(&launch.stderr)
    );
}

#[test]
fn doctor_is_read_only_and_reports_the_checks() {
    let dir = TestDir::new();

    // Keep this generic doctor contract independent of any host OpenCode
    // installation or provider state.
    let empty_bin = dir.join("empty-bin");
    fs::create_dir_all(&empty_bin).expect("create empty bin");

    let output = base_command(dir.path(), dir.path())
        .env("PATH", &empty_bin)
        .arg("doctor")
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = stdout(&output);
    assert!(text.contains("doctor"), "{text}");
    assert!(text.contains("platform"), "{text}");
    assert!(text.contains("config/routing"), "{text}");
    assert!(text.contains("runtime"), "{text}");
    assert!(
        !dir.path().join(".ocg").exists(),
        "doctor must not bootstrap a runtime"
    );
}

#[test]
fn doctor_explains_bootstrap_when_no_runtime_is_present() {
    let dir = TestDir::new();
    // Isolate PATH so a host system `opencode` cannot change the outcome.
    let empty_bin = dir.join("empty-bin");
    fs::create_dir_all(&empty_bin).expect("create empty bin");
    let output = base_command(dir.path(), dir.path())
        .env("PATH", &empty_bin)
        .arg("doctor")
        .output()
        .expect("run");
    let text = stdout(&output);
    let lower = text.to_lowercase();
    assert!(
        lower.contains("not installed") || lower.contains("bootstrap"),
        "{text}"
    );
}

#[test]
fn doctor_reports_config_routing_failure() {
    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(
        &project.join(".ocg.yaml"),
        &json!({"routing": {"roles": {"build": {"model": "does-not-exist"}}}}),
    );
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "doctor"],
    );
    assert!(!output.status.success());
    assert!(stdout(&output).contains("config/routing"));
}

#[test]
fn upgrade_fails_cleanly_without_network_and_preserves_the_cli() {
    let dir = TestDir::new();
    // Isolate PATH so the host's real `opencode upgrade` is never invoked.
    let empty_bin = dir.join("empty-bin");
    fs::create_dir_all(&empty_bin).expect("create empty bin");
    let output = base_command(dir.path(), dir.path())
        .env("PATH", &empty_bin)
        .env("OCG_API_BASE", "https://127.0.0.1:1")
        .arg("upgrade")
        .output()
        .expect("run");
    assert!(
        !output.status.success(),
        "upgrade must fail without network"
    );
    let combined = format!(
        "{}{}",
        stdout(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined.contains("self-update skipped"), "{combined}");

    // The CLI is still usable afterwards.
    let version = run(dir.path(), dir.path(), &["version"]);
    assert!(version.status.success());
    assert!(stdout(&version).contains("OCG"));
}
