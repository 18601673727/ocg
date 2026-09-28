//! Phase B project-boundary tests.
//!
//! A project is initialized by `.ocg.yaml`. These tests pin the
//! deterministic nearest-ancestor resolution, canonicalization, non-leakage
//! between sibling/parent/unrelated projects and state placement under the
//! resolved root. No network and no paid API is used.

mod common;

use common::{project_profile, write_yaml, TestDir};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn base_command(cwd: &Path, work: &Path) -> Command {
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
        .env_remove("OCG_CACHE_DIR")
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

fn write_file(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

/// A directory that carries the boundary marker and one source file.
fn initialized_project(root: &Path) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    write_yaml(&root.join(".ocg.yaml"), &project_profile());
    write_file(root, "src/parser.rs", "pub fn parse() -> u32 { 0 }\n");
    root.to_path_buf()
}

fn has_state(root: &Path) -> bool {
    root.join(".ocg").exists()
}

#[test]
fn descendant_cwd_places_state_at_the_project_root() {
    let dir = TestDir::new();
    let project = initialized_project(&dir.join("project"));
    let descendant = project.join("deep/nested");
    fs::create_dir_all(&descendant).unwrap();

    let output = run(&descendant, dir.path(), &["context", "fix", "parser"]);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(
        stdout(&output).contains("src/parser.rs"),
        "{}",
        stdout(&output)
    );

    // Every context artifact is rooted at the boundary, never at the caller.
    assert!(project.join(".ocg/index/context-index.json").is_file());
    assert!(project.join(".ocg/cache/context").is_dir());
    assert!(project.join(".ocg/telemetry/events.jsonl").is_file());
    assert!(
        !descendant.join(".ocg").exists(),
        "state must not be placed next to the descendant cwd"
    );
}

#[test]
fn descendant_resolution_prefers_the_nearest_initialized_project() {
    let dir = TestDir::new();
    let outer = initialized_project(&dir.join("outer"));
    let inner = initialized_project(&outer.join("inner"));
    let leaf = inner.join("src/deep");
    fs::create_dir_all(&leaf).unwrap();

    let output = run(&leaf, dir.path(), &["context", "fix", "parser"]);
    assert!(output.status.success(), "{}", stderr(&output));

    assert!(has_state(&inner), "the nearest project must own the state");
    assert!(
        !has_state(&outer),
        "a parent project must not receive a child project's state"
    );
}

#[cfg(unix)]
#[test]
fn canonicalized_cwd_resolves_to_one_boundary() {
    use std::os::unix::fs::symlink;

    let dir = TestDir::new();
    let project = initialized_project(&dir.join("project"));
    let nested = project.join("deep");
    fs::create_dir_all(&nested).unwrap();
    let alias = dir.join("alias");
    symlink(&project, &alias).unwrap();

    // Run through the symlinked spelling of the same directory.
    let output = run(
        &alias.join("deep"),
        dir.path(),
        &["context", "fix", "parser"],
    );
    assert!(output.status.success(), "{}", stderr(&output));

    let canonical_project = project.canonicalize().unwrap();
    assert!(canonical_project
        .join(".ocg/index/context-index.json")
        .is_file());
    assert!(!nested.join(".ocg").exists());

    // Both spellings must report the exact same canonical cache directory, so a
    // symlink cannot create a second, divergent boundary.
    let via_alias = run(&alias.join("deep"), dir.path(), &["cache", "stats"]);
    let via_real = run(&nested, dir.path(), &["cache", "stats"]);
    let alias_line = cache_dir_line(&via_alias);
    let real_line = cache_dir_line(&via_real);
    assert_eq!(alias_line, real_line, "symlink and real path must agree");
    let expected = canonical_project
        .join(".ocg/cache/context")
        .to_string_lossy()
        .into_owned();
    assert!(alias_line.ends_with(&expected), "{alias_line}");
}

fn cache_dir_line(output: &Output) -> String {
    stdout(output)
        .lines()
        .find(|line| line.starts_with("context cache:"))
        .unwrap_or_default()
        .to_string()
}

#[test]
fn sibling_projects_do_not_share_or_leak_state() {
    let dir = TestDir::new();
    let a = initialized_project(&dir.join("a"));
    let b = initialized_project(&dir.join("b"));
    fs::create_dir_all(a.join("deep")).unwrap();
    fs::create_dir_all(b.join("deep")).unwrap();

    let from_a = run(&a.join("deep"), dir.path(), &["context", "fix", "parser"]);
    assert!(from_a.status.success(), "{}", stderr(&from_a));
    assert!(has_state(&a));
    assert!(!has_state(&b), "sibling project must stay untouched");

    let from_b = run(&b.join("deep"), dir.path(), &["context", "fix", "parser"]);
    assert!(from_b.status.success(), "{}", stderr(&from_b));
    assert!(has_state(&b));
}

#[test]
fn uninitialized_directory_refuses_project_state_and_writes_nothing() {
    let dir = TestDir::new();
    let a = initialized_project(&dir.join("a"));
    let b = initialized_project(&dir.join("b"));
    let plain = dir.join("plain");
    fs::create_dir_all(&plain).unwrap();

    let output = run(&plain, dir.path(), &["context", "fix", "parser"]);
    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    let message = stderr(&output);
    assert!(
        message.contains("initialized project") && message.contains(".ocg.yaml"),
        "{message}"
    );

    assert!(
        !has_state(&plain),
        "no state in the uninitialized directory"
    );
    assert!(
        !has_state(&a),
        "an unrelated sibling project must not receive it"
    );
    assert!(
        !has_state(&b),
        "an unrelated sibling project must not receive it"
    );
}

#[test]
fn read_only_commands_work_outside_an_initialized_project() {
    let dir = TestDir::new();
    let plain = dir.join("plain");
    fs::create_dir_all(&plain).unwrap();

    // Reporting commands work anywhere, including outside a project.
    for args in [vec!["layers"], vec!["stats"]] {
        let output = run(&plain, dir.path(), &args);
        assert!(
            output.status.success(),
            "{args:?} must work outside a project: {}",
            stderr(&output)
        );
    }
    // `status` and `doctor` report the missing Profile as a diagnostic.
    for args in [vec!["status"], vec!["doctor"]] {
        let output = run(&plain, dir.path(), &args);
        let text = format!("{}{}", stdout(&output), stderr(&output));
        assert!(
            text.contains("OCG Profile"),
            "{args:?} must report the missing Profile: {text}"
        );
    }
    // Configuration *output* needs a selection: it fails with an actionable
    // error and never invents one.
    for args in [vec!["build"], vec!["validate"]] {
        let output = run(&plain, dir.path(), &args);
        assert!(
            !output.status.success(),
            "{args:?} must not invent a Profile"
        );
        assert!(
            stderr(&output).contains("Profile"),
            "{args:?}: {}",
            stderr(&output)
        );
    }
    assert!(
        !has_state(&plain),
        "read-only commands must not create project state"
    );
}

#[cfg(unix)]
#[test]
fn launch_places_the_plugin_under_the_root_but_runs_the_child_at_the_caller() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = initialized_project(&dir.join("project"));
    let descendant = project.join("deep/nested");
    fs::create_dir_all(&descendant).unwrap();

    let cwd_marker = dir.join("child-cwd");
    let script = dir.join("fake-opencode.sh");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\nprintf '%s\\n' \"$PWD\" > \"{}\"\nprintf launched\n",
            cwd_marker.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

    let output = base_command(&descendant, dir.path())
        .env("OCG_OPENCODE", &script)
        .args(["run", "hello"])
        .output()
        .expect("launch ocg");
    assert!(output.status.success(), "{}", stderr(&output));

    // Generated plugin state is inside the resolved boundary.
    assert!(project
        .join(".ocg/orchestration/plugin/ocg-orchestration.js")
        .is_file());
    assert!(!descendant.join(".ocg").exists());

    // The child OpenCode process still starts in the invocation directory.
    let reported = fs::read_to_string(&cwd_marker).unwrap();
    assert_eq!(
        Path::new(reported.trim()).canonicalize().unwrap(),
        descendant.canonicalize().unwrap()
    );
}

#[test]
fn checkpoint_and_bridge_state_land_under_the_resolved_root() {
    let dir = TestDir::new();
    let project = initialized_project(&dir.join("project"));
    let descendant = project.join("deep/nested");
    fs::create_dir_all(&descendant).unwrap();

    let save = run(
        &descendant,
        dir.path(),
        &[
            "checkpoint",
            "save",
            "--phase",
            "explore-to-build",
            "--task",
            "fix parser",
        ],
    );
    assert!(save.status.success(), "{}", stderr(&save));
    assert!(project.join(".ocg/checkpoints").is_dir());

    // The hidden bridge resolves the same boundary from the descendant cwd.
    let mut child = base_command(&descendant, dir.path())
        .args(["__bridge", "chat.message"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bridge");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(br#"{"session_id":"s","text":"fix the parser"}"#)
        .unwrap();
    let bridged = child.wait_with_output().unwrap();
    assert!(bridged.status.success(), "{}", stderr(&bridged));
    assert!(
        project.join(".ocg/orchestration/state.json").is_file(),
        "orchestration state must live under the resolved root"
    );
    assert!(!descendant.join(".ocg").exists());
}
