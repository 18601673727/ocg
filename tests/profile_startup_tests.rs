//! Profile startup boundaries do not depend on the available OpenCode runtime.
use std::path::Path;
use std::process::{Command, Output};

fn ocg(project: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ocg"))
        .arg("--project")
        .arg(project)
        .args(args)
        .env("HOME", project)
        .env("XDG_CONFIG_HOME", project.join("xdg"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_USER_CONFIG")
        .output()
        .unwrap()
}

#[test]
fn missing_profile_refuses_headless_and_serve_without_creating_it() {
    let dir = tempfile::tempdir().unwrap();
    for command in [vec!["run", "hello"], vec!["serve"]] {
        let output = ocg(dir.path(), &command);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("OCG Profile is required"));
        assert!(!dir.path().join(".ocg.yaml").exists());
    }
}

#[test]
fn placeholder_profile_is_structural_but_blocks_actual_execution() {
    let dir = tempfile::tempdir().unwrap();
    assert!(ocg(dir.path(), &["init"]).status.success());
    let path = dir.path().join(".ocg.yaml");
    let before = std::fs::read(&path).unwrap();
    assert!(ocg(dir.path(), &["init"]).status.success());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let valid = ocg(dir.path(), &["validate"]);
    assert!(
        valid.status.success(),
        "{}",
        String::from_utf8_lossy(&valid.stderr)
    );
    let output = ocg(dir.path(), &["run", "hello"]);
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("No runnable provider/model configured")
    );
}

#[cfg(unix)]
#[test]
fn placeholder_execution_never_starts_external_runtime_or_inference() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    assert!(ocg(dir.path(), &["init"]).status.success());
    let program = dir.path().join("fake-opencode");
    let marker = dir.path().join("external-runtime-was-called");
    std::fs::write(
        &program,
        format!("#!/bin/sh\ntouch '{}'\nexit 0\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .arg("--project")
        .arg(dir.path())
        .args(["run", "reply exactly: 321"])
        .env("OCG_OPENCODE", &program)
        .env("HOME", dir.path())
        .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("No runnable provider/model configured")
    );
    assert!(
        !marker.exists(),
        "placeholder execution contacted external runtime"
    );
}

#[test]
fn placeholder_profile_can_start_control_service_without_inference() {
    use std::io::BufRead;
    let dir = tempfile::tempdir().unwrap();
    assert!(ocg(dir.path(), &["init"]).status.success());
    let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .arg("--project")
        .arg(dir.path())
        .args(["serve", "--addr", "127.0.0.1:0"])
        .env("HOME", dir.path())
        .env("XDG_CONFIG_HOME", dir.path().join("xdg"))
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    let read = std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line);
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(read.unwrap() > 0, "serve did not publish a URL");
    assert!(line.starts_with("listening on http://127.0.0.1:"), "{line}");
}

#[test]
fn config_cli_edits_the_same_project_profile_without_legacy_tiers() {
    let dir = tempfile::tempdir().unwrap();
    let fresh = ocg(dir.path(), &["config", "profile"]);
    assert!(fresh.status.success());
    assert_eq!(String::from_utf8_lossy(&fresh.stdout).trim(), "null");
    assert!(!dir.path().join(".ocg.yaml").exists());
    let created = ocg(dir.path(), &["config", "new"]);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(ocg(
        dir.path(),
        &["config", "provider", "add", "my-provider", "My Provider"]
    )
    .status
    .success());
    assert!(ocg(
        dir.path(),
        &[
            "config",
            "model",
            "add",
            "selected",
            "my-provider",
            "my-model",
            "--default"
        ]
    )
    .status
    .success());
    let status = ocg(dir.path(), &["status"]);
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(String::from_utf8_lossy(&status.stdout).contains("model choice  selected"));
    let output = ocg(dir.path(), &["build"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(config["model"], "my-provider/my-model");
    assert_eq!(config["default_agent"], "lead");
    assert!(config["agent"].get("lead-low").is_none());
    assert!(!ocg(
        dir.path(),
        &["config", "lead", "high", "--model", "other/model"]
    )
    .status
    .success());
    assert!(!ocg(dir.path(), &["throttle", "high"]).status.success());
}
