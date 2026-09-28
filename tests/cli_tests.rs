//! End-to-end CLI tests for the `ocg` binary.

mod common;

use common::{write_json, write_yaml, TestDir};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

#[cfg(unix)]
fn assert_same_existing_directory(observed: &Path, expected: &Path) {
    let observed_resolved = fs::canonicalize(observed).unwrap_or_else(|error| {
        panic!(
            "cannot resolve observed directory {}: {error}",
            observed.display()
        )
    });
    let expected_resolved = fs::canonicalize(expected).unwrap_or_else(|error| {
        panic!(
            "cannot resolve expected directory {}: {error}",
            expected.display()
        )
    });
    assert_eq!(
        observed_resolved,
        expected_resolved,
        "child used the wrong working directory (observed {}, expected {})",
        observed.display(),
        expected.display()
    );
}

/// Give the test's global OCG config a returning-user Profile.
fn ensure_profile(path: &Path) {
    if !path.is_file() {
        write_yaml(path, &common::project_profile());
    }
}

fn base_command(cwd: &Path, work: &Path) -> Command {
    let profile = work.join("global.yaml");
    ensure_profile(&profile);
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(cwd)
        .env("OCG_USER_CONFIG", &profile)
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_TRACE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_API_BASE")
        .env_remove("OCG_CACHE_DIR")
        .env_remove("OCG_DISABLE_PROXY")
        .env_remove("GH_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy");
    command
}

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    base_command(cwd, work)
        .args(args)
        .output()
        .expect("run ocg")
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON: {error}\nstdout={}\nstderr={}",
            stdout_text(output),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn version_and_help() {
    let dir = TestDir::new();
    let output = run(dir.path(), dir.path(), &["version"]);
    assert!(output.status.success());
    assert!(stdout_text(&output).contains("OCG"));

    let output = run(dir.path(), dir.path(), &["help"]);
    assert!(output.status.success());
    assert!(stdout_text(&output).contains("Usage:"));
}

#[test]
fn dry_run_uses_the_profile_default_selection() {
    let dir = TestDir::new();
    let output = run(dir.path(), dir.path(), &["--dry-run"]);
    assert!(output.status.success());
    let config = stdout_json(&output);
    assert_eq!(config["default_agent"], json!("lead"));
    assert_eq!(config["model"], json!("openai/gpt-5.6-sol"));
}

#[test]
fn model_option_selects_any_configured_profile_resource() {
    let dir = TestDir::new();
    for (args, model) in [
        (
            vec!["--model", "gpt-6-astra", "--dry-run"],
            "openai/gpt-6-astra",
        ),
        (
            vec!["--model=gpt-6-astra", "--dry-run"],
            "openai/gpt-6-astra",
        ),
        (
            vec!["--model", "kimi-k3", "--dry-run"],
            "volcengine-coding-plan/kimi-k3",
        ),
    ] {
        let output = run(dir.path(), dir.path(), &args);
        assert!(output.status.success(), "args {args:?}");
        let config = stdout_json(&output);
        assert_eq!(config["default_agent"], json!("lead"), "args {args:?}");
        assert_eq!(config["model"], json!(model), "args {args:?}");
    }
}

#[test]
fn unknown_model_selection_is_rejected() {
    let dir = TestDir::new();
    let output = run(dir.path(), dir.path(), &["--model", "nope", "--dry-run"]);
    assert!(!output.status.success());
    assert!(stdout_text(&output).is_empty());
}

#[test]
fn worker_routing_is_independent_of_the_lead_selection() {
    let dir = TestDir::new();
    let output = run(
        dir.path(),
        dir.path(),
        &["--model", "gpt-6-astra", "--dry-run"],
    );
    let config = stdout_json(&output);
    assert_eq!(
        config["agent"]["ocg-explore"]["model"],
        json!("volcengine-coding-plan/kimi-k2.7-code")
    );
    assert_eq!(
        config["agent"]["ocg-build"]["model"],
        json!("opencode-go/deepseek-v4.1-flash")
    );
}

/// Runtime-facing config output (`--dry-run`, `build`) must follow the
/// detected runtime family, agreeing with a real launch: V1 emits the V1
/// contract (a `file://` local plugin entry and `task` delegation keys), V2
/// emits the V2 contract (local discovery via OPENCODE_CONFIG_DIR, no
/// `file://` entry, `subagent` delegation keys).
#[cfg(unix)]
#[test]
fn dry_run_and_build_follow_the_detected_runtime_family() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(&dir.join("global.yaml"), &common::project_profile());
    let project_arg = project.to_string_lossy().into_owned();

    for (version, is_v2) in [("1.18.31", false), ("2.0.11", true)] {
        let script = dir.join(&format!("fake-opencode-{version}"));
        fs::write(
            &script,
            format!("#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo {version}; exit 0; fi\nexit 0\n"),
        )
        .expect("write script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

        for args in [
            vec!["--project", project_arg.as_str(), "--dry-run"],
            vec!["--project", project_arg.as_str(), "build"],
        ] {
            let output = base_command(dir.path(), dir.path())
                .env("OCG_OPENCODE", &script)
                .args(&args)
                .output()
                .expect("run ocg");
            assert!(
                output.status.success(),
                "{version} {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let config = stdout_json(&output);

            let plugins: Vec<String> = config
                .get("plugin")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|value| value.as_str().unwrap_or_default().to_string())
                        .collect()
                })
                .unwrap_or_default();
            let has_local_uri = plugins.iter().any(|entry| {
                entry.starts_with("file://") && entry.ends_with("ocg-orchestration.js")
            });
            let lead_permission = &config["agent"]["lead"]["permission"];

            if is_v2 {
                // The V2 runtime discovers the local adapter through
                // OPENCODE_CONFIG_DIR/plugins; a V1-style file:// entry in the
                // package plugin array is the confirmed dry-run defect.
                assert!(
                    !has_local_uri,
                    "{version} {args:?}: V2 output must not emit the V1 file:// plugin contract: {plugins:?}"
                );
                assert!(
                    lead_permission.get("subagent").is_some(),
                    "{version} {args:?}"
                );
                assert!(lead_permission.get("task").is_none(), "{version} {args:?}");
            } else {
                assert!(
                    has_local_uri,
                    "{version} {args:?}: V1 output must inject the file:// plugin: {plugins:?}"
                );
                assert!(
                    plugins
                        .iter()
                        .any(|entry| entry.contains("/orchestration/plugin/")),
                    "{version} {args:?}: the V1 URI must point at the V1 plugin dir: {plugins:?}"
                );
                assert!(lead_permission.get("task").is_some(), "{version} {args:?}");
                assert!(
                    lead_permission.get("subagent").is_none(),
                    "{version} {args:?}"
                );
            }
        }
    }
}

/// A broken explicit runtime override is authoritative for config output too:
/// `--dry-run` and `build` must fail exactly like a launch instead of silently
/// falling back to another family.
#[cfg(unix)]
#[test]
fn dry_run_and_build_fail_on_a_broken_explicit_runtime() {
    let dir = TestDir::new();
    for args in [["--dry-run"].as_slice(), ["build"].as_slice()] {
        let output = base_command(dir.path(), dir.path())
            .env("OCG_OPENCODE", "/definitely/not/here/opencode")
            .args(args)
            .output()
            .expect("run ocg");
        assert!(!output.status.success(), "{args:?} must fail");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("explicit"),
            "{args:?}: the broken override must be reported: {stderr}"
        );
    }
}

/// With no runtime anywhere, config output keeps the deterministic historical
/// v1 contract (a real launch would bootstrap, but dry-run/build are
/// read-only) and says so on stderr.
#[cfg(unix)]
#[test]
fn dry_run_without_any_runtime_keeps_the_v1_contract_with_a_warning() {
    let dir = TestDir::new();
    let empty_bin = dir.join("empty-bin");
    fs::create_dir_all(&empty_bin).expect("create empty bin");

    let output = base_command(dir.path(), dir.path())
        .env("PATH", &empty_bin)
        .arg("--dry-run")
        .output()
        .expect("run ocg");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("could not determine the OpenCode version"),
        "the v1 fallback must be announced: {stderr}"
    );
    let config = stdout_json(&output);
    let plugins: Vec<String> = config["plugin"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|value| value.as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default();
    assert!(
        plugins
            .iter()
            .any(|entry| entry.starts_with("file://") && entry.ends_with("ocg-orchestration.js")),
        "the deterministic v1 contract must inject the file:// plugin: {plugins:?}"
    );
    assert!(config["agent"]["lead"]["permission"].get("task").is_some());
}

#[test]
fn report_subcommands() {
    let dir = TestDir::new();
    let status = run(dir.path(), dir.path(), &["status"]);
    assert!(status.status.success());
    let status_text = stdout_text(&status);
    assert!(status_text.contains("Profile"), "{status_text}");
    assert!(
        !status_text.to_lowercase().contains("throttle"),
        "{status_text}"
    );

    let routing = run(dir.path(), dir.path(), &["routing"]);
    assert!(routing.status.success());
    assert!(stdout_text(&routing).contains("kimi-k2.7-code"));

    let validate = run(dir.path(), dir.path(), &["validate"]);
    assert!(validate.status.success());
    assert!(stdout_text(&validate).contains("configuration is structurally valid"));

    let layers = run(dir.path(), dir.path(), &["layers"]);
    assert!(layers.status.success());
    assert!(stdout_text(&layers).contains("OCG home"));
}

/// When a Lead model declares no reasoning variant, status and doctor must say
/// `provider-default` rather than inventing a value.
#[test]
fn status_and_doctor_report_provider_default_when_no_variant_is_configured() {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    let mut config = common::project_profile();
    config["profile"]["defaultModel"] = json!("kimi-k3");
    write_yaml(&dir.join("global.yaml"), &config);
    let project_arg = project.to_string_lossy().into_owned();

    let status = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "status"],
    );
    assert!(status.status.success(), "{}", stdout_text(&status));
    let text = stdout_text(&status);
    assert!(text.contains("provider-default"), "{text}");
    assert!(text.contains("volcengine-coding-plan/kimi-k3"), "{text}");
    assert!(
        !text.contains("kimi-k3 variant"),
        "status must not fabricate a variant: {text}"
    );

    // Doctor output under test is config-only. Keep the test hermetic rather
    // than accidentally probing whichever OpenCode happens to be on PATH.
    let fake = dir.join("doctor-opencode.sh");
    fs::write(
        &fake,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'opencode v1.18.31'; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra volcengine-coding-plan/kimi-k2.7-code volcengine-coding-plan/kimi-k3 opencode-go/deepseek-v4.1-flash opencode-go/glm-5.3-flash opencode-go/glm-5.3; exit 0; fi\nexit 0\n",
    )
    .expect("write hermetic OpenCode");
    #[cfg(unix)]
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).expect("chmod hermetic OpenCode");
    let doctor = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE", fake)
        .args(["--project", &project_arg, "--disable-proxy", "doctor"])
        .output()
        .expect("run hermetic doctor");
    assert!(doctor.status.success(), "{}", stdout_text(&doctor));
    let text = stdout_text(&doctor);
    assert!(text.contains("provider-default"), "{text}");
    assert!(text.contains("volcengine-coding-plan/kimi-k3"), "{text}");
}

#[test]
fn project_override_changes_routing_without_leaking() {
    let dir = TestDir::new();
    let project = dir.project();
    let mut config = common::project_profile();
    config["profile"]["defaultModel"] = json!("gpt-6-astra");
    config["routing"]["roles"]["build"] =
        json!({"model": "glm-5.3", "variant": "high", "description": "Implementation."});
    write_yaml(&dir.join("global.yaml"), &config);
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--dry-run"],
    );
    let config = stdout_json(&output);
    assert_eq!(config["default_agent"], json!("lead"));
    assert_eq!(config["model"], json!("openai/gpt-6-astra"));
    assert_eq!(
        config["agent"]["ocg-build"]["model"],
        json!("opencode-go/glm-5.3")
    );

    // The project-local .ocg.yaml is ignored; the global profile applies everywhere.
    let plain = run(dir.path(), dir.path(), &["--dry-run"]);
    assert_eq!(stdout_json(&plain)["model"], json!("openai/gpt-6-astra"));
}

#[test]
fn invalid_override_fails() {
    let dir = TestDir::new();
    let project = dir.project();
    let mut config = common::project_profile();
    config["routing"]["roles"]["build"] = json!({"model": "does-not-exist"});
    write_yaml(&dir.join("global.yaml"), &config);
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--dry-run"],
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("does-not-exist"));
}

#[test]
fn usage_failures_exit_non_zero() {
    let dir = TestDir::new();
    for args in [
        vec!["not-a-command"],
        vec!["--nope"],
        // The fixed-tier surfaces are removed, not deprecated aliases.
        vec!["throttle", "high"],
        vec!["--throttle", "high", "--dry-run"],
        vec!["high", "--dry-run"],
        vec!["low", "run", "hello"],
        vec!["config", "lead", "high"],
        vec!["--project", "/definitely/not/a/directory"],
    ] {
        let output = run(dir.path(), dir.path(), &args);
        assert!(
            !output.status.success(),
            "expected failure for args {args:?}"
        );
        assert_eq!(output.status.code(), Some(2), "args {args:?}");
    }
}

#[test]
fn model_selection_after_the_command_is_parsed() {
    let dir = TestDir::new();
    let pretty = run(dir.path(), dir.path(), &["build", "--pretty"]);
    assert!(pretty.status.success());
    assert!(
        stdout_text(&pretty).contains("\n  \"$schema\""),
        "build --pretty should be pretty-printed"
    );

    let selected = run(dir.path(), dir.path(), &["--model", "gpt-6-astra", "build"]);
    assert_eq!(stdout_json(&selected)["model"], json!("openai/gpt-6-astra"));
}

#[cfg(unix)]
#[test]
fn run_execs_the_configured_binary_with_config_and_cwd() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(&dir.join("global.yaml"), &common::project_profile());
    let record = dir.join("record.txt");
    let config_file = dir.join("config.json");
    let preflight_config_file = dir.join("preflight-config.json");
    let script = dir.join("fake-opencode.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"models\" ]; then printf '%s' \"$OPENCODE_CONFIG_CONTENT\" > \"{preflight_config_file}\"; printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\npwd > \"{record}\"\nprintf '%s\\n' \"$@\" >> \"{record}\"\nprintf '%s' \"$OPENCODE_CONFIG_CONTENT\" > \"{config_file}\"\n",
        record = record.display(),
        config_file = config_file.display(),
        preflight_config_file = preflight_config_file.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let project_arg = project.to_string_lossy().into_owned();
    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE_BIN", &script)
        .args(["--project", project_arg.as_str(), "run", "hello world"])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let recorded = fs::read_to_string(&record).expect("record");
    let lines: Vec<&str> = recorded.lines().collect();
    assert_same_existing_directory(Path::new(lines[0]), &project);
    assert_eq!(lines[1], "run");
    assert_eq!(lines[2], "hello world");

    let config: Value =
        serde_json::from_str(&fs::read_to_string(&config_file).expect("config")).expect("parse");
    assert_eq!(config["default_agent"], json!("lead"));
    assert!(
        config["plugin"]
            .as_array()
            .is_some_and(|plugins| !plugins.is_empty()),
        "the coding launch must retain the generated plugin"
    );
    let preflight: Value = serde_json::from_str(
        &fs::read_to_string(&preflight_config_file).expect("preflight config"),
    )
    .expect("parse preflight config");
    assert!(
        preflight.get("plugin").is_none(),
        "model preflight must not load the not-yet-materialized OCG plugin"
    );
}

#[cfg(unix)]
#[test]
fn launch_exports_the_exact_lead_contract_for_each_selected_model() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let mut config = common::project_profile();
    config["models"]["models"]["gpt-5.6-sol"]["variant"] = json!("medium");
    write_yaml(&dir.join("global.yaml"), &config);
    let record = dir.join("lead-contract.json");
    let script = dir.join("fake-opencode-contract.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\nprintf '%s' \"$OCG_LEAD_CONTRACT\" > \"{}\"\n",
        record.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    for (choice, model, variant) in [
        ("gpt-5.6-sol", "gpt-5.6-sol", Some(json!("medium"))),
        ("gpt-6-astra", "gpt-6-astra", None),
    ] {
        let output = base_command(dir.path(), dir.path())
            .env("OCG_OPENCODE_BIN", &script)
            .args(["--model", choice, "run", "hello"])
            .output()
            .expect("run");
        assert!(
            output.status.success(),
            "{choice}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let contract: Value =
            serde_json::from_str(&fs::read_to_string(&record).expect("contract")).expect("json");
        assert_eq!(contract["agent"], json!("lead"));
        assert_eq!(contract["level"], json!(choice));
        assert_eq!(contract["provider_id"], json!("openai"));
        assert_eq!(contract["model_id"], json!(model));
        // An absent variant is omitted from the exported contract, never null.
        assert_eq!(contract.get("variant"), variant.as_ref(), "{choice}");
    }
}

/// The `--dry-run` config and the config/contract the bridge actually receives
/// must agree for every selected Profile model. This is the contract test that
/// closes the "config changes but the runtime does not" gap.
#[cfg(unix)]
#[test]
fn dry_run_effective_config_matches_the_runtime_bridge_contract() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(&project.join(".ocg.yaml"), &common::project_profile());
    let project_arg = project.to_string_lossy().into_owned();

    for choice in ["gpt-5.6-sol", "gpt-6-astra", "gpt-5.6-sol"] {
        let record = dir.join(&format!("runtime-{choice}"));
        let config_file = record.join("config.json");
        let contract_file = record.join("contract.json");
        fs::create_dir_all(&record).unwrap();
        let script = dir.join(&format!("fake-opencode-{choice}.sh"));
        let body = format!(
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\nprintf '%s' \"$OPENCODE_CONFIG_CONTENT\" > \"{}\"\nprintf '%s' \"$OCG_LEAD_CONTRACT\" > \"{}\"\n",
            config_file.display(),
            contract_file.display()
        );
        fs::write(&script, body).unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

        let launch = base_command(dir.path(), dir.path())
            .env("OCG_OPENCODE_BIN", &script)
            .args(["--project", &project_arg, "--model", choice, "run", "hello"])
            .output()
            .expect("run");
        assert!(
            launch.status.success(),
            "{choice}: {}",
            String::from_utf8_lossy(&launch.stderr)
        );

        // Pin the same runtime for the dry-run so both sides resolve the same
        // runtime family: the contract under test is "dry-run output equals
        // what the launch hands the runtime", not host-runtime detection.
        let dry = base_command(dir.path(), dir.path())
            .env("OCG_OPENCODE_BIN", &script)
            .args(["--project", &project_arg, "--model", choice, "--dry-run"])
            .output()
            .expect("dry-run");
        assert!(dry.status.success(), "{choice}: dry-run failed");
        let dry_config = stdout_json(&dry);

        let runtime_config: Value =
            serde_json::from_str(&fs::read_to_string(&config_file).expect("runtime config"))
                .expect("runtime config json");
        let contract: Value =
            serde_json::from_str(&fs::read_to_string(&contract_file).expect("contract"))
                .expect("contract json");

        // Top-level selection and the Lead agent must agree.
        assert_eq!(runtime_config["default_agent"], dry_config["default_agent"]);
        assert_eq!(runtime_config["model"], dry_config["model"]);
        let agent = contract["agent"].as_str().expect("contract agent");
        assert_eq!(dry_config["default_agent"], json!(agent));
        assert_eq!(
            dry_config["agent"][agent], runtime_config["agent"][agent],
            "{choice}: dry-run and runtime lead agent differ"
        );
        let full = format!(
            "{}/{}",
            contract["provider_id"].as_str().unwrap(),
            contract["model_id"].as_str().unwrap()
        );
        assert_eq!(dry_config["agent"][agent]["model"], json!(full));
        // A variant the contract omits must be absent from the config too.
        assert_eq!(
            dry_config["agent"][agent].get("variant"),
            contract.get("variant")
        );
    }
}

/// The Profile is resolved from scratch on every invocation. `build` must not
/// persist a selection, so a different `--model` after a prior invocation cannot
/// leave a sticky Profile behind for a new session.
#[test]
fn building_a_profile_leaves_no_session_state() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let before = write_profile(&project);

    let first = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--model", "gpt-6-astra", "build"],
    );
    let second = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "build"],
    );
    assert_eq!(stdout_json(&first)["model"], json!("openai/gpt-6-astra"));
    assert_eq!(stdout_json(&second)["model"], json!("openai/gpt-5.6-sol"));
    assert!(
        !project.join(".ocg").exists(),
        "resolving a profile must not create session/profile state"
    );
    assert_eq!(
        fs::read_to_string(project.join(".ocg.yaml")).expect("profile"),
        before,
        "build must never rewrite the OCG Profile"
    );
}

fn write_profile(project: &Path) -> String {
    let path = project.join(".ocg.yaml");
    write_yaml(&path, &common::project_profile());
    fs::read_to_string(&path).expect("profile")
}

#[cfg(unix)]
#[test]
fn missing_active_lead_model_blocks_launch_without_fallback() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let marker = dir.join("must-not-launch");
    let script = dir.join("fake-opencode-missing-model.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nprintf launched > \"{}\"\n",
        marker.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE_BIN", &script)
        .args(["--model", "gpt-6-astra", "run", "hello"])
        .output()
        .expect("run");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("openai/gpt-6-astra"), "{stderr}");
    assert!(stderr.contains("required Lead model"), "{stderr}");
    assert!(
        !marker.exists(),
        "OpenCode must not launch with a false fallback"
    );
}

#[cfg(unix)]
#[test]
fn unavailable_model_probe_warns_but_does_not_block_launch() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let marker = dir.join("launched");
    let script = dir.join("fake-opencode-probe-failure.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then echo probe-diagnostic-must-not-be-echoed >&2; exit 9; fi\nprintf launched > \"{}\"\n",
        marker.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE_BIN", &script)
        .args(["run", "hello"])
        .output()
        .expect("run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("runtime model check could not be completed"),
        "{stderr}"
    );
    // The probe's own diagnostics can contain arbitrary third-party text and
    // must not be echoed back.
    assert!(
        !stderr.contains("probe-diagnostic-must-not-be-echoed"),
        "{stderr}"
    );
    assert!(marker.exists());
}

#[cfg(unix)]
#[test]
fn filesystem_aliases_resolve_to_the_same_directory() {
    use std::os::unix::fs::symlink;

    let dir = TestDir::new();
    let project = dir.project();
    let alias = dir.join("project-alias");
    symlink(&project, &alias).expect("create directory alias");

    assert_ne!(
        alias, project,
        "the fixture must use distinct lexical paths"
    );
    assert_same_existing_directory(&alias, &project);
}

#[cfg(unix)]
#[test]
fn legacy_opencode_bin_variable_is_honored() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let marker = dir.join("marker.txt");
    let script = dir.join("legacy-opencode.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nprintf done > \"{}\"\n",
        marker.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE_BIN", &script)
        .arg("run")
        .output()
        .expect("run");
    assert!(output.status.success());
    assert!(marker.is_file());
}

#[test]
fn disable_proxy_flag_is_accepted_before_and_after_the_command() {
    let dir = TestDir::new();
    for args in [
        vec!["--disable-proxy", "--dry-run"],
        vec!["build", "--disable-proxy"],
        vec!["build", "--disable-proxy", "--pretty"],
    ] {
        let output = run(dir.path(), dir.path(), &args);
        assert!(
            output.status.success(),
            "args {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
fn child_env(dir: &TestDir, extra_env: &[(&str, &str)], args: &[&str]) -> Vec<String> {
    use std::os::unix::fs::PermissionsExt;

    let record = dir.join("child-env.txt");
    let script = dir.join("fake-opencode-env.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"models\" ]; then printf '%s\\n' openai/gpt-5.6-sol; exit 0; fi\nenv > \"{}\"\n",
        record.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let mut command = base_command(dir.path(), dir.path());
    command.env("OCG_OPENCODE_BIN", &script);
    for (name, value) in extra_env {
        command.env(name, value);
    }
    let output = command.args(args).output().expect("run ocg");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::read_to_string(&record)
        .expect("read child env")
        .lines()
        .map(str::to_string)
        .collect()
}

#[cfg(unix)]
fn env_value(lines: &[String], name: &str) -> Option<String> {
    let prefix = format!("{name}=");
    lines
        .iter()
        .find_map(|line| line.strip_prefix(&prefix).map(str::to_string))
}

#[cfg(unix)]
#[test]
fn child_process_receives_the_resolved_proxy_only() {
    let dir = TestDir::new();
    let lines = child_env(
        &dir,
        &[
            ("HTTPS_PROXY", "http://proxy-internal:3128"),
            ("https_proxy", "http://stale:1"),
            ("NO_PROXY", "localhost"),
        ],
        &["run", "hello"],
    );
    // The resolved value wins in both spellings; a stale lower-case variable
    // cannot survive.
    for name in ["HTTPS_PROXY", "https_proxy"] {
        assert_eq!(
            env_value(&lines, name).as_deref(),
            Some("http://proxy-internal:3128"),
            "{name}"
        );
    }
    assert_eq!(env_value(&lines, "HTTP_PROXY"), None);
    assert_eq!(env_value(&lines, "http_proxy"), None);
    for name in ["NO_PROXY", "no_proxy"] {
        assert_eq!(
            env_value(&lines, name).as_deref(),
            Some("localhost"),
            "{name}"
        );
    }
}

#[cfg(unix)]
#[test]
fn model_probe_child_receives_the_resolved_proxy_and_no_stale_spelling() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let record = dir.join("models-env.txt");
    let script = dir.join("fake-opencode-models-env.sh");
    let body = format!(
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 1.18.31; exit 0; fi\nif [ \"$1\" = \"models\" ]; then env > \"{record}\"; printf '%s\\n' openai/gpt-5.6-sol openai/gpt-6-astra; exit 0; fi\nexit 0\n",
        record = record.display()
    );
    fs::write(&script, body).expect("write script");
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).expect("chmod");

    let output = base_command(dir.path(), dir.path())
        .env("OCG_OPENCODE_BIN", &script)
        .env("HTTPS_PROXY", "http://proxy-internal:3128")
        .env("all_proxy", "socks5://stale:1")
        .args(["run", "hello"])
        .output()
        .expect("run ocg");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines: Vec<String> = fs::read_to_string(&record)
        .expect("read models child env")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(
        env_value(&lines, "HTTPS_PROXY").as_deref(),
        Some("http://proxy-internal:3128")
    );
    assert_eq!(
        env_value(&lines, "https_proxy").as_deref(),
        Some("http://proxy-internal:3128")
    );
    // An unsupported SOCKS value the user configured is preserved verbatim for
    // the child under its original spelling rather than silently dropped.
    assert_eq!(
        env_value(&lines, "all_proxy").as_deref(),
        Some("socks5://stale:1")
    );
    assert_eq!(
        env_value(&lines, "ALL_PROXY"),
        None,
        "the unconfigured upper-case spelling must stay absent"
    );
}

#[cfg(unix)]
#[test]
fn disable_proxy_flag_strips_every_proxy_variable_for_the_launch() {
    let dir = TestDir::new();
    let lines = child_env(
        &dir,
        &[
            ("HTTPS_PROXY", "http://proxy-internal:3128"),
            ("http_proxy", "http://proxy-internal:3128"),
        ],
        &["--disable-proxy", "run", "hello"],
    );
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ] {
        assert_eq!(env_value(&lines, name), None, "{name} must be stripped");
    }
}

#[cfg(unix)]
#[test]
fn disable_proxy_environment_strips_every_proxy_variable_for_the_launch() {
    let dir = TestDir::new();
    let lines = child_env(
        &dir,
        &[
            ("OCG_DISABLE_PROXY", "true"),
            ("HTTPS_PROXY", "http://proxy-internal:3128"),
        ],
        &["run", "hello"],
    );
    assert_eq!(env_value(&lines, "HTTPS_PROXY"), None);
}

/// `init` creates an OCG-owned placeholder Profile. It is structurally valid,
/// creates no real provider, and must not become runnable on its own.
#[test]
fn init_creates_a_placeholder_profile_that_is_not_runnable() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();

    let output = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .current_dir(dir.path())
        .env("OCG_USER_CONFIG", dir.join("global.yaml"))
        .args(["--project", &project_arg, "init"])
        .output()
        .expect("run init");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout_text(&output).contains("created"));

    let path = dir.join("global.yaml");
    assert!(path.is_file(), "init must create the global YAML");
    let text = fs::read_to_string(&path).expect("read init file");
    assert!(text.contains("placeholder"), "{text}");
    // No credentials or local state are ever written.
    let lower = text.to_lowercase();
    for token in ["token", "password", "secret", "api_key", "authorization"] {
        assert!(!lower.contains(token), "init leaked '{token}'");
    }

    // Structurally valid, but every resource is a non-runnable placeholder.
    let validate = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "validate"],
    );
    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stderr)
    );
    let status = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "status"],
    );
    let status_text = stdout_text(&status);
    assert!(
        status_text.contains("placeholder-only true"),
        "{status_text}"
    );
    let dry_run = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--dry-run"],
    );
    assert!(!dry_run.status.success());
    assert!(
        String::from_utf8_lossy(&dry_run.stderr).contains("No runnable provider/model configured")
    );
}

#[test]
fn init_is_safe_to_repeat_and_does_not_overwrite() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();

    let invoke_init = || {
        Command::new(env!("CARGO_BIN_EXE_ocg"))
            .current_dir(dir.path())
            .env("OCG_USER_CONFIG", dir.join("global.yaml"))
            .args(["--project", &project_arg, "init"])
            .output()
            .expect("run init")
    };
    assert!(invoke_init().status.success());
    let path = dir.join("global.yaml");
    let original = fs::read_to_string(&path).expect("read init file");
    let edited = format!("{original}# keep my edit\n");
    fs::write(&path, &edited).expect("edit");

    let again = invoke_init();
    assert!(again.status.success());
    assert!(stdout_text(&again).contains("already exists"));
    assert_eq!(fs::read_to_string(&path).expect("reread"), edited);
}

#[test]
fn init_refuses_on_stale_global_json() {
    let dir = TestDir::new();
    let project = dir.project();
    write_json(&dir.join("global.json"), &json!({}));
    let project_arg = project.to_string_lossy().into_owned();

    let output = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .current_dir(dir.path())
        .env("OCG_USER_CONFIG", dir.join("global.yaml"))
        .args(["--project", &project_arg, "init"])
        .output()
        .expect("run init");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("global.json"), "{stderr}");
    assert!(stderr.contains("global.yaml"), "{stderr}");
    assert!(!dir.join("global.yaml").exists());
}

#[test]
fn project_local_profile_and_json_are_ignored() {
    let dir = TestDir::new();
    let project = dir.project();
    write_json(
        &project.join(".ocg.json"),
        &json!({"throttle": {"default": "high"}}),
    );
    let project_arg = project.to_string_lossy().into_owned();

    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "--dry-run"],
    );
    assert!(output.status.success(), "{}", stdout_text(&output));
}

#[test]
fn stale_user_json_is_rejected_by_the_binary() {
    let dir = TestDir::new();
    let user_yaml = dir.join("user-config.yaml");
    write_json(
        &dir.join("user-config.json"),
        &json!({"throttle": {"default": "high"}}),
    );

    let output = base_command(dir.path(), dir.path())
        .args(["--user-config", user_yaml.to_str().unwrap(), "--dry-run"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("user-config.json"), "{stderr}");
    assert!(stderr.contains("user-config.yaml"), "{stderr}");
}

#[test]
fn layers_command_reports_yaml_paths() {
    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(&dir.join("global.yaml"), &common::project_profile());
    let project_arg = project.to_string_lossy().into_owned();
    let output = run(
        dir.path(),
        dir.path(),
        &["--project", &project_arg, "layers"],
    );
    assert!(output.status.success());
    let text = stdout_text(&output);
    assert!(text.contains("global.yaml"), "{text}");
}

/// The OCG-owned Profile is a project YAML. No configuration surface may write
/// a JSON config, and an existing JSON config is refused rather than migrated.
#[test]
fn config_never_persists_to_a_json_path() {
    let dir = TestDir::new();
    let project = dir.project();
    let project_arg = project.to_string_lossy().into_owned();
    let user = dir.join("user-config.json");

    // A JSON user config that exists is refused with an actionable message.
    fs::write(&user, "{}\n").expect("write json config");
    let refused = run(
        dir.path(),
        dir.path(),
        &[
            "--user-config",
            user.to_str().unwrap(),
            "--project",
            &project_arg,
            "config",
            "new",
        ],
    );
    assert_eq!(refused.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("YAML only"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );

    // A YAML destination is accepted, and JSON remains untouched.
    fs::remove_file(&user).expect("remove json config");
    let user_yaml = dir.join("user-config.yaml");
    let created = run(
        dir.path(),
        dir.path(),
        &[
            "--user-config",
            user_yaml.to_str().unwrap(),
            "--project",
            &project_arg,
            "config",
            "new",
        ],
    );
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(user_yaml.is_file());
    assert!(!user.exists(), "no JSON config may be written");
}

#[test]
fn ocg_owned_args_are_consumed_and_the_rest_is_forwarded_faithfully() {
    use ocg::cli::{parse, Command};

    let argv = |args: &[&str]| {
        args.iter()
            .map(std::ffi::OsString::from)
            .collect::<Vec<_>>()
    };

    // OCG-owned flags before the command are consumed; everything after `run`
    // is forwarded verbatim, even when it looks OCG-owned.
    let cli = parse(argv(&[
        "--model", "my-model", "run", "--model", "x", "hello",
    ]))
    .unwrap();
    assert_eq!(cli.model_choice.as_deref(), Some("my-model"));
    match cli.command {
        Command::Run(args) => {
            let forwarded: Vec<String> = args
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();
            assert_eq!(forwarded, ["--model", "x", "hello"]);
        }
        other => panic!("expected run, got {other:?}"),
    }

    // A `run` subcommand with no OCG flags forwards every argument untouched.
    let cli = parse(argv(&["run", "--model", "z", "--project", "/tmp/x"])).unwrap();
    assert!(cli.model_choice.is_none());
    match cli.command {
        Command::Run(args) => {
            let forwarded: Vec<String> = args
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();
            assert_eq!(forwarded, ["--model", "z", "--project", "/tmp/x"]);
        }
        other => panic!("expected run, got {other:?}"),
    }

    // `--` terminates OCG parsing and forwards the remainder.
    let cli = parse(argv(&["--", "run", "a", "b"])).unwrap();
    match cli.command {
        Command::Run(args) => {
            let forwarded: Vec<String> = args
                .iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect();
            assert_eq!(forwarded, ["a", "b"]);
        }
        other => panic!("expected run, got {other:?}"),
    }
}
