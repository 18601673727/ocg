//! End-to-end orchestration controller tests.
//!
//! These drive the Rust controller with deterministic fakes: a `FakeGitHost`,
//! a `FixedClock` and a `FakeCaptureRunner`. No model, network or real OpenCode
//! process is involved.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::bridge::BridgeContext;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::{BuildDecision, Controller};
use ocg::orchestration::handoff::{Role, Severity};
use ocg::orchestration::state::{Attempts, OrchestrationPhase};
use ocg::process::{CapturedOutput, FakeCaptureRunner, FakeGitHost};
use ocg::runtime::compat::LeadSelection;
use ocg::runtime::compat::{BridgeRuntimeClient, MemorySessionClient};
use ocg::runtime::lifecycle::RuntimeProfile;
use ocg::telemetry::{TelemetryConfig, TelemetryStats, TelemetryStore};
use ocg::verification::config::VerificationConfig;
use serde_json::json;
use std::cell::RefCell;
use std::fs;
use std::path::Path;
use std::rc::Rc;

const TASK: &str = "update module_1 parser";

fn fixture(root: &Path) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("src/module_1.rs"),
        "pub fn parse_1(value: u32) -> u32 {\n    value + 1\n}\n",
    )
    .unwrap();
    fs::write(root.join("src/module_2.rs"), "pub fn other() {}\n").unwrap();
    // Enough repository material that the 40% ratio cap is not trivially
    // binding on the required fields.
    for index in 3..40 {
        fs::write(
            root.join(format!("src/module_{index}.rs")),
            format!("pub fn helper_{index}() -> u32 {{\n    {index}\n}}\n"),
        )
        .unwrap();
    }
    fs::write(root.join("Cargo.toml"), "[package]\nname = \"fixture\"\n").unwrap();
}

fn verification() -> VerificationConfig {
    VerificationConfig::from_config(&json!({
        "verification": {
            "stages": {"normal": {"commands": [{"program": "cargo", "args": ["test"]}]}},
            "defaultStage": "normal"
        }
    }))
    .unwrap()
}

fn explore_json() -> String {
    json!({
        "goal": "make the parser safe",
        "constraints": ["never break the public API"],
        "findings": [
            {"summary": "parse_1 is recursive", "severity": "critical", "source": "src/module_1.rs"}
        ],
        "files": ["src/module_1.rs"],
        "symbols": [{"name": "parse_1"}]
    })
    .to_string()
}

fn failing_runner() -> FakeCaptureRunner {
    FakeCaptureRunner::new().with_failure(
        "cargo",
        &["test"],
        101,
        "error[E0308]: mismatched types\n --> src/module_1.rs:4:5\n",
    )
}

fn passing_runner() -> FakeCaptureRunner {
    FakeCaptureRunner::new().with_success(
        "cargo",
        &["test"],
        "test result: ok. 3 passed; 0 failed\n",
    )
}

#[test]
fn ordinary_activation_prepares_context_and_state() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let context = controller
        .prepare_lead_context("session-activation", TASK)
        .unwrap();
    assert!(context.dynamic_context.contains(TASK));
    assert!(context.metrics.model_dynamic_context_bytes > 0);
    let loaded = controller.load_state();
    assert!(loaded.exists);
    assert!(loaded.state.session("session-activation").is_some());
    let state_dir = ocg::orchestration::state::state_dir(dir.path());
    assert!(state_dir.is_dir());
}

#[test]
fn explore_to_build_preserves_required_fields_and_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    let explore = controller
        .prepare_handoff("s", Role::Explore, TASK)
        .unwrap();
    assert_eq!(explore.destination, Role::Explore);
    let digest = controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    assert!(digest.structured);
    assert!(
        digest.checkpoint_id.is_some(),
        "an Explore→Build checkpoint must exist"
    );

    let build = controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    assert_eq!(build.source, Role::Explore);
    assert_eq!(build.destination, Role::Build);
    assert_eq!(build.capsule.transition.as_str(), "explore_to_build");
    assert_eq!(build.capsule.goal.as_deref(), Some("make the parser safe"));
    assert!(build
        .capsule
        .hard_constraints
        .iter()
        .any(|constraint| constraint == "never break the public API"));
    assert!(build
        .capsule
        .findings
        .iter()
        .any(|finding| finding.severity == Severity::Critical));
    assert!(build
        .capsule
        .symbols
        .iter()
        .any(|symbol| symbol.name == "parse_1"));
    // Bounded by the configured runtime optimization envelope. Required
    // evidence is never dropped to satisfy it; the tight 4096/40 gate is a
    // fixture regression check, not a runtime rule.
    let envelope = OrchestrationConfig::default();
    assert!(build.capsule.measured_bytes() <= envelope.max_handoff_bytes);
    assert!(
        build.capsule.measured_bytes()
            <= build.metrics.rich_capsule_bytes as usize * envelope.max_handoff_ratio_percent / 100,
        "capsule {} rich {}",
        build.capsule.measured_bytes(),
        build.metrics.rich_capsule_bytes
    );
}

#[test]
fn build_to_verify_pass_has_no_debug() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let runner = passing_runner();
    let outcome = controller.after_build("s", &runner, None).unwrap();
    match outcome.decision {
        BuildDecision::Passed { verification, .. } => {
            assert_eq!(verification.outcome, "passed");
        }
        other => panic!("expected pass, got {other:?}"),
    }
    assert!(outcome.checkpoint_id.is_some());
    let state = controller.load_state();
    assert_eq!(
        state.state.session("s").unwrap().phase,
        OrchestrationPhase::Done
    );
}

#[test]
fn failure_escalates_through_bounded_retry_to_debug() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let runner = failing_runner();

    // Attempt 1 fails: bounded retry.
    let first = controller.after_build("s", &runner, None).unwrap();
    assert!(matches!(first.decision, BuildDecision::RetryBuild { .. }));
    // Attempt 2 fails: bounded retry.
    let second = controller.after_build("s", &runner, None).unwrap();
    assert!(matches!(second.decision, BuildDecision::RetryBuild { .. }));
    // Attempt 3 fails: budget exhausted, Debug is recommended with a reason.
    let third = controller.after_build("s", &runner, None).unwrap();
    match third.decision {
        BuildDecision::Debug {
            reason, handoff, ..
        } => {
            // The reason is explainable but never contains a configured
            // command string.
            assert!(reason.contains("failed"));
            assert!(
                !reason.contains("cargo"),
                "debug reason leaked a command: {reason}"
            );
            assert!(reason.contains("src/module_1.rs"), "{reason}");
            assert_eq!(handoff.destination, Role::Debug);
            // Debug carries failures/evidence/raw-log refs and a failing location.
            assert!(!handoff.capsule.failures.is_empty());
            assert!(handoff.capsule.findings.is_empty());
            assert!(handoff.capsule.files.is_empty());
            assert!(handoff
                .capsule
                .first_failing_location()
                .map(|location| location.display())
                .unwrap_or_default()
                .contains("src/module_1.rs"));
        }
        other => panic!("expected debug, got {other:?}"),
    }
}

#[test]
fn debug_retry_budget_escalates() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    controller
        .after_build("s", &failing_runner(), None)
        .unwrap();
    // The default debug budget is one hand-off; a second escalates.
    let first = controller.debug_handoff("s").unwrap();
    assert_eq!(first.destination, Role::Debug);
    let second = controller.debug_handoff("s").unwrap();
    assert!(second
        .stale_reasons
        .iter()
        .any(|reason| reason.contains("budget exhausted")));
}

#[test]
fn fail_then_success_is_a_pass() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let first = controller
        .after_build("s", &failing_runner(), None)
        .unwrap();
    assert!(matches!(first.decision, BuildDecision::RetryBuild { .. }));
    let second = controller
        .after_build("s", &passing_runner(), None)
        .unwrap();
    assert!(matches!(second.decision, BuildDecision::Passed { .. }));
}

#[test]
fn no_configured_verification_reports_not_configured() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let empty =
        VerificationConfig::from_config(&json!({"verification": {"enabled": true}})).unwrap();
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        empty,
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let outcome = controller
        .after_build("s", &passing_runner(), None)
        .unwrap();
    match outcome.decision {
        BuildDecision::NotConfigured { note } => assert!(note.contains("no trusted verification")),
        other => panic!("expected not-configured, got {other:?}"),
    }
}

#[test]
fn stale_checkpoint_is_marked_not_reused() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller
        .prepare_handoff("s", Role::Explore, TASK)
        .unwrap();
    let digest = controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    assert!(digest.checkpoint_id.is_some());
    // Change a source the checkpoint fingerprinted (the explore capsule only
    // records the task, so touch a file and force a git change instead).
    fs::write(
        dir.path().join("src/module_1.rs"),
        "pub fn parse_1() -> u32 { 2 }\n",
    )
    .unwrap();
    let build = controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    // A stale checkpoint is surfaced as a projection note, never hidden.
    let stale_note = build
        .stale_reasons
        .iter()
        .chain(build.capsule.omitted.iter())
        .any(|note| {
            note.contains("stale") || note.contains("changed") || note.contains("git state")
        });
    assert!(
        stale_note,
        "stale checkpoint must be visible: {:?}",
        build.stale_reasons
    );
}

#[test]
fn corrupt_state_and_checkpoint_recover_fail_soft() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let state_path = ocg::orchestration::state::state_path(dir.path());
    fs::create_dir_all(state_path.parent().unwrap()).unwrap();
    fs::write(&state_path, "{ this is not json").unwrap();

    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let loaded = controller.load_state();
    assert!(loaded.corrupt);
    // The controller still works and rewrites valid state.
    let context = controller.prepare_lead_context("s", TASK).unwrap();
    assert!(!context.dynamic_context.is_empty());
    assert!(!controller.load_state().corrupt);

    // A corrupt checkpoint referenced by a session is ignored, not fatal.
    let checkpoints = ocg::orchestration::checkpoint::checkpoints_dir(dir.path());
    fs::create_dir_all(&checkpoints).unwrap();
    fs::write(checkpoints.join("cp-bad.json"), "{not json").unwrap();
    let loaded = controller.load_state();
    let mut state = loaded.state;
    if let Some(session) = state.session("s").cloned() {
        let mut session = session;
        session.checkpoints.push("cp-bad".to_string());
        state.upsert(session, 1_001);
    }
    ocg::orchestration::state::save(dir.path(), &state).unwrap();
    let handoff = controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    assert!(!handoff.dynamic_context.is_empty());
}

#[test]
fn disabled_orchestration_emits_no_plugin_and_no_state() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let config = OrchestrationConfig {
        enabled: false,
        ..OrchestrationConfig::default()
    };
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        config.clone(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);
    let value = bridge.dispatch("chat.message", &json!({"session_id": "s", "text": TASK}));
    assert_eq!(value["ok"], json!(false));
    assert_eq!(value["disabled"], json!(true));
    assert!(!ocg::orchestration::state::state_dir(dir.path()).exists());

    // The generated config has no plugin entry when orchestration is disabled.
    let mut config_value = json!({});
    assert!(!ocg::orchestration::plugin::has_ocg_plugin(&config_value));
    ocg::orchestration::plugin::inject_plugin(
        &mut config_value,
        "file:///tmp/ocg-orchestration.js",
    );
    assert!(ocg::orchestration::plugin::has_ocg_plugin(&config_value));
}

#[test]
fn v2_plugin_contract_uses_local_discovery_and_subagent_tool() {
    use ocg::orchestration::plugin;

    let dir = tempfile::tempdir().unwrap();
    let path = plugin::materialize_v2_with(dir.path(), plugin::v2_plugin_source()).unwrap();
    assert_eq!(path, plugin::v2_plugin_path(dir.path()));
    assert!(path.is_file());
    assert_eq!(
        path.parent().unwrap().parent().unwrap(),
        plugin::v2_config_dir(dir.path())
    );

    // The v2 adapter is thin: no request-message Lead rewrite, and the
    // delegation hooks target the renamed `subagent` tool through the V2
    // `ctx.tool.hook("execute.before")` registration (the event carries the
    // tool name, unlike the V1 hook's `input`).
    let source = plugin::v2_plugin_source();
    assert!(source.contains("event.tool !== \"subagent\""));
    assert!(source.contains("ctx.tool.hook(\"execute.before\""));
    // The repository baseline is injected at model dispatch through the
    // session `context` hook; prompt admission is reported through a strictly
    assert!(source.contains("ctx.session.hook(\"context\""));
    assert!(source.contains("ctx.session.hook(\"prompt\""));
    // The V2 prompt hook uses strictBridge for fail-closed authority enforcement.
    assert!(source.contains("strictBridge(\"session.prompt\""));
    assert!(source.contains("const STRICT_BRIDGE_TIMEOUT_MS = 30000"));
    assert!(source.contains("child.kill(\"SIGKILL\")"));
    assert!(source.contains("if (settled) return"));
    assert!(source.contains("event.system.push({ type: \"text\""));
    // The V2 runtime may be Node, so the bridge is spawned via
    // `node:child_process` with an exact argv and no shell.
    assert!(source.contains("import { spawn } from \"node:child_process\""));
    assert!(source.contains("shell: false"));
    assert!(!source.contains("output.message.agent"));
    assert!(!source.contains("enforceLeadContract"));
}

/// Every file under a directory, recursively.
fn files_under(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut result = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                result.push(path);
            }
        }
    }
    result.sort();
    result
}

/// The exact VM-observed failure: a V1 plugin artifact exists, then a V2
/// launch/config materialization occurs, and OpenCode 2 discovers the stale V1
/// adapter through a shared config root. The V2 config dir is dedicated, so the
/// runtime-visible plugin namespace must contain only a valid V2 plugin, and
/// legacy generated artifacts from earlier layouts must be migrated away.
#[test]
fn v2_materialization_never_exposes_v1_or_legacy_artifacts_to_the_runtime() {
    use ocg::orchestration::plugin;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let state = ocg::orchestration::state::state_dir(root);

    // A V1 launch materialized the V1 adapter at its own path.
    plugin::materialize(root).unwrap();
    // Legacy generated artifacts from earlier 0.3.0 builds: one sharing the V1
    // state root (holding V1 bytes — the observed collision) and one from a
    // double-applied orchestration state path.
    let legacy_shared = state.join("plugins");
    let legacy_nested = state.join("orchestration").join("plugins");
    fs::create_dir_all(&legacy_shared).unwrap();
    fs::create_dir_all(&legacy_nested).unwrap();
    fs::write(
        legacy_shared.join("ocg-orchestration.js"),
        plugin::plugin_source(),
    )
    .unwrap();
    fs::write(
        legacy_nested.join("ocg-orchestration.js"),
        plugin::v2_plugin_source(),
    )
    .unwrap();

    // A V2 launch/config materialization occurs.
    let path = plugin::materialize_v2_with(root, plugin::v2_plugin_source()).unwrap();
    assert_eq!(path, plugin::v2_plugin_path(root));

    // The runtime-visible plugin namespace (everything OpenCode 2 can discover
    // under OPENCODE_CONFIG_DIR) contains exactly one file: the valid V2
    // adapter. No V1 artifact, no legacy artifact.
    let config_dir = plugin::v2_config_dir(root);
    assert_eq!(
        path.parent().unwrap().parent().unwrap(),
        config_dir,
        "the adapter must live at <config dir>/plugins/"
    );
    let discovered = files_under(&config_dir);
    assert_eq!(discovered, vec![path.clone()], "V2 discovered namespace");
    let source = fs::read_to_string(&path).unwrap();
    assert_eq!(source, plugin::v2_plugin_source());
    assert!(source.contains("export default {"));
    assert!(source.contains("id: \"ocg-orchestration\""));
    assert!(!source.contains("export const server"));

    // The V1 config root is not inside (or equal to) the V2 config root, so a
    // V2 runtime can never reach the V1 adapter through local discovery.
    assert!(!config_dir.join("plugin").exists());
    assert_ne!(config_dir, state);

    // Legacy generated artifacts are migrated away, directories included.
    assert!(!state.join("plugins").exists());
    assert!(!state.join("orchestration").exists());

    // The V1 artifact is untouched and still satisfies a V1 launch.
    assert_eq!(
        fs::read_to_string(plugin::plugin_path(root)).unwrap(),
        plugin::plugin_source()
    );
}

/// Legacy cleanup is scoped to OCG-generated files: a user-owned file at a
/// legacy location is never deleted, and its directory is never pruned.
#[test]
fn v2_legacy_migration_preserves_user_owned_files() {
    use ocg::orchestration::plugin;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let state = ocg::orchestration::state::state_dir(root);
    let legacy_shared = state.join("plugins");
    fs::create_dir_all(&legacy_shared).unwrap();
    let user_file = legacy_shared.join("ocg-orchestration.js");
    fs::write(
        &user_file,
        "// hand-written by the user\nexport default {};\n",
    )
    .unwrap();
    let sibling = legacy_shared.join("user-notes.txt");
    fs::write(&sibling, "keep me\n").unwrap();

    plugin::materialize_v2_with(root, plugin::v2_plugin_source()).unwrap();

    assert!(user_file.is_file(), "user-owned file must survive");
    assert_eq!(
        fs::read_to_string(&user_file).unwrap(),
        "// hand-written by the user\nexport default {};\n"
    );
    assert!(sibling.is_file());
    // The dedicated V2 config root is unaffected by the legacy directory.
    assert_eq!(
        files_under(&plugin::v2_config_dir(root)),
        vec![plugin::v2_plugin_path(root)]
    );
}

#[test]
fn secret_shaped_task_never_enters_handoff_or_telemetry() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
    let output = format!("goal: {secret}\nfindings:\n- {secret}\n");
    controller
        .prepare_handoff("s", Role::Explore, TASK)
        .unwrap();
    let digest = controller.consume_explore_result("s", &output).unwrap();
    let handoff = controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let serialized = serde_json::to_string(&handoff.capsule).unwrap();
    assert!(!serialized.contains(&secret), "{serialized}");
    assert!(!digest
        .findings
        .iter()
        .any(|finding| finding.summary.contains(&secret)));
}

#[test]
fn telemetry_records_orchestration_transitions_without_raw_content() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let runner = FakeCaptureRunner::new();
    let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::default());
    let value = bridge.dispatch("chat.message", &json!({"session_id": "s", "text": TASK}));
    assert_eq!(value["ok"], json!(true));

    let store = TelemetryStore::new(dir.path(), TelemetryConfig::default());
    let log = store.read();
    assert_eq!(log.events.len(), 1);
    let event = &log.events[0];
    assert_eq!(event.task_type.as_deref(), Some("orchestration"));
    assert_eq!(event.orchestration.phase.as_deref(), Some("idle"));
    assert!(event.orchestration.model_dynamic_context_bytes > 0);
    // The raw task text never lands in the event.
    let raw = fs::read_to_string(store.path()).unwrap();
    assert!(!raw.contains(TASK), "{raw}");

    let stats = TelemetryStats::collect(&store);
    assert_eq!(stats.aggregate.orchestration.transitions, 1);
    assert!(stats.render().contains("orchestration"));
}

#[test]
fn dynamic_suffix_is_deterministic_and_stable() {
    let make = || {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1_000);
        let controller = Controller::new(
            dir.path(),
            OrchestrationConfig::default(),
            ContextConfig::default(),
            CapabilityConfig::default(),
            verification(),
            &git,
            &clock,
        );
        controller
            .prepare_handoff("s", Role::Build, TASK)
            .unwrap()
            .dynamic_context
    };
    assert_eq!(make(), make());
}

#[test]
fn bridge_handles_explore_and_build_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let runner = failing_runner();
    let bridge = bridge_with_runtime(&controller, &runner);

    let before = bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-explore", "prompt": TASK}}),
    );
    assert_eq!(before["ok"], json!(true));
    assert!(before["context"]
        .as_str()
        .unwrap()
        .contains("ocg role handoff"));

    let after_explore = bridge.dispatch(
        "tool.execute.after",
        &json!({
            "session_id": "s",
            "args": {"subagent_type": "ocg-explore", "prompt": TASK},
            "result": {"output": explore_json()}
        }),
    );
    assert_eq!(after_explore["ok"], json!(true));
    assert!(after_explore["checkpoint"].is_string());

    let build_before = bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-build", "prompt": TASK}}),
    );
    assert_eq!(build_before["ok"], json!(true));

    let build_after = bridge.dispatch(
        "tool.execute.after",
        &json!({
            "session_id": "s",
            "args": {"subagent_type": "ocg-build", "prompt": TASK},
            "result": {"output": "done"}
        }),
    );
    assert_eq!(build_after["ok"], json!(true));
    // The fake runner fails, so the feedback must mention verification and a retry.
    let context = build_after["context"].as_str().unwrap();
    assert!(context.contains("verification"), "{context}");
}

#[test]
fn fake_capture_output_marks_truncation() {
    // A guard that the controller never treats truncated output as a pass.
    let output = CapturedOutput {
        exit: ocg::process::ProcessExit::Code(101),
        success: false,
        stdout: Vec::new(),
        stderr: b"error: boom\n".to_vec(),
        stdout_truncated: false,
        stderr_truncated: true,
        duration_ms: 1,
    };
    assert!(!output.success);
    assert!(output.truncated());
}

#[test]
fn distinct_explore_and_build_prompts_keep_findings_and_retry_budget() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    // Explore and Build get genuinely different delegated prompts.
    controller
        .prepare_handoff("s", Role::Explore, "explore the parser call paths")
        .unwrap();
    controller
        .consume_explore_result("s", &explore_json())
        .unwrap();

    let build = controller
        .prepare_handoff("s", Role::Build, "implement the parse_1 fix now")
        .unwrap();
    // The Explore finding survived the different Build prompt.
    assert!(
        build
            .capsule
            .findings
            .iter()
            .any(|finding| finding.severity == Severity::Critical),
        "findings must survive a differing Build prompt: {:?}",
        build.capsule.findings
    );
    assert_eq!(build.capsule.task, "implement the parse_1 fix now");
    assert_eq!(build.capsule.task_id, Controller::task_id(TASK));

    // Repeated Build retries use differing prompts but share one retry budget.
    let runner = failing_runner();
    let first = controller.after_build("s", &runner, None).unwrap();
    assert!(matches!(
        first.decision,
        BuildDecision::RetryBuild { attempt: 1, .. }
    ));
    controller
        .prepare_handoff("s", Role::Build, "retry the parse_1 fix with logging")
        .unwrap();
    let second = controller.after_build("s", &runner, None).unwrap();
    assert!(matches!(
        second.decision,
        BuildDecision::RetryBuild { attempt: 2, .. }
    ));
    controller
        .prepare_handoff("s", Role::Build, "retry the parse_1 fix once more")
        .unwrap();
    let third = controller.after_build("s", &runner, None).unwrap();
    assert!(
        matches!(third.decision, BuildDecision::Debug { .. }),
        "a bounded budget must reach Debug, got {:?}",
        third.decision
    );
    // The Explore findings are still in the session, not reset by the prompts.
    let state = controller.load_state();
    assert!(
        !state.state.session("s").unwrap().findings.is_empty(),
        "session findings must survive retries"
    );
    assert_eq!(state.state.session("s").unwrap().attempts.build, 3);
}

#[test]
fn chat_message_with_a_new_task_id_resets_the_session() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore")
        .unwrap();
    controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    assert!(!controller
        .load_state()
        .state
        .session("s")
        .unwrap()
        .findings
        .is_empty());

    // A different user message is a new overall task and must reset.
    controller
        .prepare_lead_context("s", "unrelated follow-up task")
        .unwrap();
    let session = controller.load_state().state.session("s").cloned().unwrap();
    assert!(session.findings.is_empty());
    assert_eq!(session.attempts.build, 0);
    // A later hand-off re-initializes from the existing (reset) session.
    let build = controller
        .prepare_handoff("s", Role::Build, "build the unrelated task")
        .unwrap();
    assert!(build.capsule.findings.is_empty());
}

#[test]
fn debug_delegation_is_narrow_and_has_no_selected_source() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_handoff("s", Role::Build, TASK).unwrap();
    let runner = failing_runner();
    controller.after_build("s", &runner, None).unwrap();
    controller.after_build("s", &runner, None).unwrap();
    let third = controller.after_build("s", &runner, None).unwrap();
    assert!(matches!(third.decision, BuildDecision::Debug { .. }));

    // A normal plugin delegation to Debug uses the narrow projection.
    let debug = controller
        .prepare_handoff("s", Role::Debug, "diagnose the failing test")
        .unwrap();
    assert_eq!(debug.destination, Role::Debug);
    assert!(debug.capsule.goal.is_none());
    assert!(debug.capsule.findings.is_empty());
    assert!(debug.capsule.files.is_empty());
    assert!(debug.capsule.symbols.is_empty());
    assert!(debug.capsule.decisions.is_empty());
    assert!(!debug.capsule.failures.is_empty());
    assert!(debug.capsule.verification.is_some());
    assert!(
        !debug.dynamic_context.contains("selected source"),
        "Debug delegation must not append selected source: {}",
        debug.dynamic_context
    );
}

#[test]
fn secret_shaped_task_goal_constraint_slice_and_verification_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
    // Put a secret-shaped literal inside a selected symbol body so the context
    // engine slices it into the rich input.
    fs::write(
        dir.path().join("src/module_1.rs"),
        format!("pub fn parse_1() {{\n    let key = \"{secret}\";\n    let _ = key;\n}}\n"),
    )
    .unwrap();

    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    // The delegated task itself is secret-shaped.
    controller
        .prepare_lead_context("s", "update module_1 parse_1")
        .unwrap();
    let handoff = controller
        .prepare_handoff("s", Role::Build, &format!("update parse_1 with {secret}"))
        .unwrap();
    let serialized = serde_json::to_string(&handoff.capsule).unwrap();
    assert!(!serialized.contains(&secret), "{serialized}");
    assert!(
        !handoff.dynamic_context.contains(&secret),
        "dynamic context leaked"
    );
    // The source slice containing the secret was dropped.
    assert!(
        handoff
            .capsule
            .omitted
            .iter()
            .any(|note| note.contains("source slice")),
        "{:?}",
        handoff.capsule.omitted
    );

    // A secret in an Explore goal/constraint is dropped before projection.
    let structured = format!(
        "{{\"goal\": \"{secret}\", \"constraints\": [\"{secret}\"], \"findings\": [{{\"summary\": \"ok\"}}]}}"
    );
    controller.consume_explore_result("s", &structured).unwrap();
    let build = controller
        .prepare_handoff("s", Role::Build, "build it")
        .unwrap();
    let text = serde_json::to_string(&build.capsule).unwrap();
    assert!(!text.contains(&secret), "{text}");

    // A secret in verification distilled output is dropped from the Build→Verify
    // capsule.
    let secret_runner = FakeCaptureRunner::new().with_failure(
        "cargo",
        &["test"],
        101,
        &format!("error: leaking {secret} at src/module_1.rs:4:5\n"),
    );
    controller
        .prepare_handoff("s", Role::Build, "build it")
        .unwrap();
    let outcome = controller.after_build("s", &secret_runner, None).unwrap();
    let verify = outcome
        .verify_handoff
        .as_ref()
        .expect("Build→Verify capsule");
    let verify_text = serde_json::to_string(verify).unwrap();
    assert!(!verify_text.contains(&secret), "{verify_text}");
}

#[test]
fn bridge_varying_prompts_participate_in_checkpoints_and_telemetry() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let runner = failing_runner();
    let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::default());

    bridge.dispatch("chat.message", &json!({"session_id": "s", "text": TASK}));
    bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-explore", "prompt": "explore prompt"}}),
    );
    bridge.dispatch(
        "tool.execute.after",
        &json!({
            "session_id": "s",
            "args": {"subagent_type": "ocg-explore", "prompt": "explore prompt"},
            "result": {"output": explore_json()}
        }),
    );
    // Three Build attempts with three different prompts.
    for (index, prompt) in ["build one", "build two", "build three"].iter().enumerate() {
        let before = bridge.dispatch(
            "tool.execute.before",
            &json!({"session_id": "s", "args": {"subagent_type": "ocg-build", "prompt": prompt}}),
        );
        assert_eq!(before["ok"], json!(true), "build before {index}");
        let after = bridge.dispatch(
            "tool.execute.after",
            &json!({
                "session_id": "s",
                "args": {"subagent_type": "ocg-build", "prompt": prompt},
                "result": {"output": "done"}
            }),
        );
        assert_eq!(after["ok"], json!(true), "build after {index}");
    }

    // Build→Verify and Verify→Debug checkpoints must exist.
    let (summaries, corrupt) = ocg::orchestration::checkpoint::list(dir.path());
    assert_eq!(corrupt, 0);
    let phases: Vec<&str> = summaries
        .iter()
        .map(|summary| summary.phase.as_str())
        .collect();
    assert!(phases.contains(&"build_to_verify"), "{phases:?}");
    assert!(phases.contains(&"verify_to_debug"), "{phases:?}");

    // Telemetry recorded the phase transitions (including build/verify/debug).
    let store = TelemetryStore::new(dir.path(), TelemetryConfig::default());
    let stats = TelemetryStats::collect(&store);
    assert!(stats.aggregate.orchestration.transitions >= 6);
    assert!(stats.aggregate.orchestration.build_attempts >= 1);
    assert!(stats.aggregate.orchestration.debug_attempts >= 1);
    let raw = fs::read_to_string(store.path()).unwrap();
    assert!(!raw.contains(TASK), "raw task text must not be stored");
}

/// A FakeGitHost describing a real repository with a status entry and a diff.
fn fake_repo(root: &Path, status: &str, diff: &str) -> FakeGitHost {
    FakeGitHost::new()
        .with_stdout(&["rev-parse", "--is-inside-work-tree"], "true\n")
        .with_stdout(
            &["rev-parse", "--show-toplevel"],
            &format!("{}\n", root.display()),
        )
        .with_stdout(&["rev-parse", "HEAD"], "abc123\n")
        .with_stdout(&["symbolic-ref", "--short", "-q", "HEAD"], "main\n")
        .with_stdout(
            &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
            status,
        )
        .with_stdout(&["ls-files", "-s", "-z"], "")
        .with_stdout(
            &["diff", "--no-color", "--no-ext-diff", "--unified=3"],
            diff,
        )
}

const REAL_DIFF: &str = "diff --git a/src/module_1.rs b/src/module_1.rs\n--- a/src/module_1.rs\n+++ b/src/module_1.rs\n@@ -1,2 +1,2 @@\n-old_parse()\n+new_parse()\n";

#[test]
fn debug_to_build_is_a_real_transition_with_evidence_and_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = fake_repo(dir.path(), " M src/module_1.rs\0", REAL_DIFF);
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore prompt")
        .unwrap();
    controller
        .consume_explore_result("s", &explore_json())
        .unwrap();

    // Fail Build three times to reach the Debug recommendation.
    controller
        .prepare_handoff("s", Role::Build, "build it")
        .unwrap();
    let runner = failing_runner();
    for _ in 0..3 {
        controller.after_build("s", &runner, None).unwrap();
    }
    let debug = controller
        .prepare_handoff("s", Role::Debug, "diagnose the failure")
        .unwrap();
    assert_eq!(debug.capsule.transition.as_str(), "verify_to_debug");

    // Returning to Build is a distinct typed transition.
    let back = controller
        .prepare_handoff("s", Role::Build, "apply the root-cause fix")
        .unwrap();
    assert_eq!(back.source, Role::Debug);
    assert_eq!(back.destination, Role::Build);
    assert_eq!(back.capsule.transition.as_str(), "debug_to_build");
    // Fix feedback (failures + verification) survives; raw unrelated logs do not.
    assert!(!back.capsule.failures.is_empty());
    assert!(back.capsule.verification.is_some());
    assert!(back.capsule.raw_log_refs.is_empty());
    // The recorded constraint survives as a fix constraint.
    assert!(back
        .capsule
        .hard_constraints
        .iter()
        .any(|constraint| constraint == "never break the public API"));

    let (summaries, corrupt) = ocg::orchestration::checkpoint::list(dir.path());
    assert_eq!(corrupt, 0);
    assert!(
        summaries
            .iter()
            .any(|summary| summary.phase.as_str() == "debug_to_build"),
        "Debug→Build checkpoint must exist: {:?}",
        summaries
            .iter()
            .map(|s| s.phase.as_str())
            .collect::<Vec<_>>()
    );
}

#[test]
fn build_to_verify_carries_a_real_diff_and_excludes_unrelated_or_sensitive_content() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    // A real diff with a sensitive path and a secret-shaped body line alongside
    // the legitimate change.
    let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
    let status = " M src/module_1.rs\0 M .env\0";
    let diff = format!(
        "diff --git a/src/module_1.rs b/src/module_1.rs\n--- a/src/module_1.rs\n+++ b/src/module_1.rs\n@@ -1,2 +1,2 @@\n-old_parse()\n+new_parse()\ndiff --git a/.env b/.env\n--- a/.env\n+++ b/.env\n@@ -1,1 +1,1 @@\n+TOKEN={secret}\n"
    );
    let git = fake_repo(dir.path(), status, &diff);
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore prompt")
        .unwrap();
    // An unrelated exploratory finding that must not leak into Verify.
    let unrelated = "unrelated speculative exploration note";
    let structured = json!({
        "goal": "make the parser safe",
        "constraints": ["never break the public API"],
        "findings": [{"summary": unrelated, "severity": "critical"}],
    })
    .to_string();
    controller.consume_explore_result("s", &structured).unwrap();
    controller
        .prepare_handoff("s", Role::Build, "build it")
        .unwrap();

    let outcome = controller
        .after_build("s", &failing_runner(), None)
        .unwrap();
    let verify = outcome
        .verify_handoff
        .as_ref()
        .expect("Build→Verify capsule");
    // The real changed path and hunk survive.
    let diff_context = verify
        .diff_context
        .as_deref()
        .expect("Verify must carry a bounded diff");
    assert!(diff_context.contains("src/module_1.rs"), "{diff_context}");
    assert!(
        diff_context.contains("new_parse") || diff_context.contains("@@ -1,2 +1,2 @@"),
        "{diff_context}"
    );
    // A real content fingerprint, not a fabricated task id.
    assert!(verify
        .diff_ref
        .as_deref()
        .map(|reference| reference.starts_with("sha256:"))
        .unwrap_or(false));
    // The exploratory finding is excluded from Verify.
    let text = serde_json::to_string(verify).unwrap();
    assert!(!text.contains(unrelated), "{text}");
    assert!(verify.findings.is_empty());
    // Sensitive path and secret content are excluded.
    assert!(!diff_context.contains(".env"), "{diff_context}");
    assert!(!diff_context.contains(&secret), "{diff_context}");
    assert!(!text.contains(&secret), "{text}");
}

#[test]
fn explore_handoffs_include_the_response_contract_but_others_do_not() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    controller.prepare_lead_context("s", TASK).unwrap();
    let explore = controller
        .prepare_handoff("s", Role::Explore, "explore prompt")
        .unwrap();
    assert!(
        explore
            .dynamic_context
            .contains("Explore response contract"),
        "{}",
        explore.dynamic_context
    );
    let deep = controller
        .prepare_handoff("s", Role::ExploreDeep, "deep explore prompt")
        .unwrap();
    assert!(deep.dynamic_context.contains("Explore response contract"));
    let build = controller
        .prepare_handoff("s", Role::Build, "build prompt")
        .unwrap();
    assert!(!build.dynamic_context.contains("Explore response contract"));
}

#[test]
fn debug_budget_is_enforced_through_the_bridge_path() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let runner = failing_runner();
    let bridge = bridge_with_runtime(&controller, &runner);

    bridge.dispatch("chat.message", &json!({"session_id": "s", "text": TASK}));
    bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-build", "prompt": "build"}}),
    );
    // Fail the build budget so Debug is recommended.
    for _ in 0..3 {
        bridge.dispatch(
            "tool.execute.after",
            &json!({
                "session_id": "s",
                "args": {"subagent_type": "ocg-build", "prompt": "build"},
                "result": {"output": "done"}
            }),
        );
    }

    // First Debug delegation consumes attempt 1 and is not escalated.
    let first = bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-debug", "prompt": "diagnose"}}),
    );
    assert_eq!(first["ok"], json!(true));
    assert_eq!(first["destination"], json!("debug"));
    assert!(!first["context"]
        .as_str()
        .unwrap()
        .contains("user escalation required"));

    // Second Debug delegation exceeds the default max (1) and escalates.
    let second = bridge.dispatch(
        "tool.execute.before",
        &json!({"session_id": "s", "args": {"subagent_type": "ocg-debug", "prompt": "diagnose again"}}),
    );
    assert_eq!(second["ok"], json!(true));
    assert!(
        second["context"]
            .as_str()
            .unwrap()
            .contains("user escalation required"),
        "{}",
        second["context"]
    );
    let state = controller.load_state();
    assert_eq!(state.state.session("s").unwrap().attempts.debug, 2);
}

#[test]
fn secret_user_message_is_not_persisted_in_state_or_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
    let message = format!("update parse_1 with {secret} please");
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = Controller::new(
        dir.path(),
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        &git,
        &clock,
    );
    let context = controller.prepare_lead_context("s", &message).unwrap();
    assert!(!context.dynamic_context.contains(&secret));
    controller
        .prepare_handoff("s", Role::Explore, &message)
        .unwrap();
    controller
        .consume_explore_result("s", "{\"goal\":\"ok\"}")
        .unwrap();

    let state_text = fs::read_to_string(ocg::orchestration::state::state_path(dir.path())).unwrap();
    assert!(
        !state_text.contains(&secret),
        "state leaked the secret: {state_text}"
    );
    let checkpoints = ocg::orchestration::checkpoint::checkpoints_dir(dir.path());
    for entry in fs::read_dir(&checkpoints).unwrap() {
        let path = entry.unwrap().path();
        let text = fs::read_to_string(&path).unwrap_or_default();
        assert!(
            !text.contains(&secret),
            "checkpoint {} leaked the secret",
            path.display()
        );
    }
}

// ---- Repeated-context (snapshot deduplication) regressions -----------------

fn lead_controller<'a>(
    root: &'a Path,
    git: &'a FakeGitHost,
    clock: &'a FixedClock,
) -> Controller<'a> {
    Controller::new(
        root,
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        git,
        clock,
    )
}

/// Create a bridge with a `MemorySessionClient` as the invocation-scoped
/// runtime and a canonical Lead contract. Required for `session.prompt`
/// authority-gate tests (P0-J).
fn bridge_with_runtime<'a>(
    controller: &'a Controller<'a>,
    runner: &'a FakeCaptureRunner,
) -> BridgeContext<'a> {
    let client: Rc<RefCell<Box<dyn BridgeRuntimeClient>>> =
        Rc::new(RefCell::new(Box::new(MemorySessionClient::new())));
    let profile = RuntimeProfile::new(
        "lead-high".to_string(),
        "openai/gpt-6-astra".to_string(),
        Some("low".to_string()),
    );
    let lead = LeadSelection {
        level: "high".to_string(),
        agent: "lead-high".to_string(),
        provider_id: "openai".to_string(),
        model_id: "gpt-6-astra".to_string(),
        variant: Some("low".to_string()),
    };
    BridgeContext::new(controller, runner, TelemetryConfig::disabled())
        .with_bridge_runtime(client, profile)
        .with_lead_contract(lead)
}

fn dispatch_chat(bridge: &BridgeContext<'_>, session: &str, text: &str) -> serde_json::Value {
    bridge.dispatch(
        "chat.message",
        &json!({"session_id": session, "text": text}),
    )
}

/// A. First prompt injects the full snapshot.
/// B/C. Later prompts with an unchanged repository snapshot are deduplicated.
/// E. A distinct session still receives its own initial context.
#[test]
fn lead_context_snapshot_is_injected_once_and_deduplicated_per_session() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    // A. The first prompt injects the full repository snapshot plus metadata.
    let first = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(first["ok"], json!(true));
    assert_eq!(first["cached"], json!(false));
    let first_context = first["context"].as_str().unwrap();
    assert!(first_context.contains(TASK));
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();
    assert!(first_id.starts_with("sha256:"), "{first_id}");
    assert!(first["bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        first["estimated_tokens"].as_u64().unwrap() as usize,
        first["bytes"].as_u64().unwrap() as usize / 4
    );
    assert!(first["file_count"].as_u64().unwrap() > 0);
    assert!(first["symbol_count"].as_u64().unwrap() > 0);
    // The baseline generation is persisted on the session.
    let session = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(
        session.repository_generation_id.as_deref(),
        Some(first_id.as_str())
    );

    // B. The second prompt with an unchanged repository returns no context.
    let second = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(second["ok"], json!(true));
    assert_eq!(second["cached"], json!(true));
    assert_eq!(second["context"], json!(""));
    assert_eq!(second["snapshot_id"], json!(first_id));

    // C. The third prompt stays deduplicated.
    let third = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(third["cached"], json!(true));
    assert_eq!(third["context"], json!(""));

    // E. A distinct session gets its own initial snapshot. The identity is a
    // property of the repository snapshot, so it is legitimately the same.
    let other = dispatch_chat(&bridge, "lead-2", TASK);
    assert_eq!(other["cached"], json!(false));
    assert!(!other["context"].as_str().unwrap().is_empty());
    assert_eq!(other["snapshot_id"], json!(first_id));
}

/// D. A material repository change produces a new identity and a fresh snapshot.
#[test]
fn lead_context_snapshot_changes_when_repository_context_changes() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let first = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();
    assert!(first["context"].as_str().unwrap().contains("parse_1"));

    // Edit the selected file so the repository snapshot materially changes.
    fs::write(
        dir.path().join("src/module_1.rs"),
        "pub fn parse_1(value: u32) -> u32 {\n    value + 2\n}\n// changed\n",
    )
    .unwrap();

    let second = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(second["ok"], json!(true));
    assert_eq!(second["cached"], json!(false));
    let second_id = second["snapshot_id"].as_str().unwrap().to_string();
    assert_ne!(second_id, first_id, "changed context must change identity");
    assert!(!second["context"].as_str().unwrap().is_empty());

    // The next unchanged prompt is deduplicated against the new baseline.
    let third = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(third["cached"], json!(true));
    assert_eq!(third["context"], json!(""));
    assert_eq!(third["snapshot_id"], json!(second_id));
}

/// F. Deduplication is per session: a Worker hand-off in a distinct session is
/// never suppressed by the Lead session's cached snapshot.
#[test]
fn worker_handoff_is_not_suppressed_by_lead_snapshot_deduplication() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    // Cache the Lead session's snapshot.
    let first = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));
    let cached = dispatch_chat(&bridge, "lead-1", TASK);
    assert_eq!(cached["cached"], json!(true));
    assert_eq!(cached["context"], json!(""));

    // A Worker subagent delegation runs in its own session and still receives
    // its typed hand-off context even though the Lead snapshot is cached.
    let worker = bridge.dispatch(
        "tool.execute.before",
        &json!({
            "session_id": "worker-1",
            "args": {"subagent_type": "ocg-explore", "prompt": "explore the parser"}
        }),
    );
    assert_eq!(worker["ok"], json!(true));
    assert_eq!(worker["destination"], json!("explore"));
    assert!(!worker["context"].as_str().unwrap().is_empty());

    // A distinct Lead session also still gets its own initial snapshot.
    let other_lead = dispatch_chat(&bridge, "lead-2", TASK);
    assert_eq!(other_lead["cached"], json!(false));
    assert!(!other_lead["context"].as_str().unwrap().is_empty());
}

/// A differently phrased follow-up is a new task id but the same effective
/// repository snapshot, so the per-session identity is preserved across the
/// task reset and the snapshot is not re-appended.
#[test]
fn lead_context_snapshot_dedup_survives_a_new_task_message() {
    let dir = tempfile::tempdir().unwrap();
    // An empty repository keeps the rendered snapshot body stable across
    // messages, isolating the per-session identity from task-aware ranking.
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let first = dispatch_chat(&bridge, "lead-1", "first task");
    assert_eq!(first["cached"], json!(false));
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();

    let second = dispatch_chat(&bridge, "lead-1", "a differently phrased follow-up task");
    assert_ne!(second["task_id"], first["task_id"]);
    assert_eq!(second["cached"], json!(true));
    assert_eq!(second["context"], json!(""));
    assert_eq!(second["snapshot_id"], json!(first_id));
}

/// A materially different task in the same session must not append another full
/// repository baseline. Task-aware ranking may differ, but the session baseline
/// is scoped to the indexed repository generation, not the task wording.
#[test]
fn lead_context_snapshot_suppresses_different_task() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let first = dispatch_chat(&bridge, "lead-1", "update module_1 parser");
    assert_eq!(first["cached"], json!(false));
    assert!(!first["context"].as_str().unwrap().is_empty());
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();

    let second = dispatch_chat(&bridge, "lead-1", "refactor module_2 helper");
    assert_ne!(second["task_id"], first["task_id"]);
    assert_eq!(second["cached"], json!(true));
    assert_eq!(second["context"], json!(""));
    assert_eq!(second["snapshot_id"], json!(first_id));
}

/// Returning to a previously seen task/context still does not append another
/// full repository baseline (A -> B -> A).
#[test]
fn lead_context_snapshot_suppresses_a_b_a() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let a = dispatch_chat(&bridge, "lead-1", "update module_1 parser");
    assert_eq!(a["cached"], json!(false));
    let baseline = a["snapshot_id"].as_str().unwrap().to_string();

    let b = dispatch_chat(&bridge, "lead-1", "refactor module_2 helper");
    assert_eq!(b["cached"], json!(true));
    assert_eq!(b["context"], json!(""));

    let a2 = dispatch_chat(&bridge, "lead-1", "update module_1 parser");
    assert_eq!(a2["cached"], json!(true));
    assert_eq!(a2["context"], json!(""));
    assert_eq!(a2["snapshot_id"], json!(baseline));
}

/// Four different task-aware projections must produce exactly one persistent
/// full repository baseline (A -> B -> C -> D).
#[test]
fn lead_context_snapshot_suppresses_a_b_c_d() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let tasks = [
        "update module_1 parser",
        "refactor module_2 helper",
        "document module_3 logic",
        "test module_4 behavior",
    ];
    let mut baseline: Option<String> = None;
    for (index, task) in tasks.iter().enumerate() {
        let result = dispatch_chat(&bridge, "lead-1", task);
        if index == 0 {
            assert_eq!(result["cached"], json!(false));
            assert!(!result["context"].as_str().unwrap().is_empty());
            baseline = Some(result["snapshot_id"].as_str().unwrap().to_string());
        } else {
            assert_eq!(
                result["cached"],
                json!(true),
                "task {task} should reuse baseline"
            );
            assert_eq!(result["context"], json!(""));
            assert_eq!(result["snapshot_id"], json!(baseline.as_ref().unwrap()));
        }
    }
}

/// A trivial third-turn message must not append another full repository
/// baseline. This is the exact live failure shape that motivated the fix.
#[test]
fn lead_context_snapshot_suppresses_trivial_third_turn() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    let first = dispatch_chat(
        &bridge,
        "lead-1",
        "Use the ocg-explore subagent exactly once.",
    );
    assert_eq!(first["cached"], json!(false));
    assert!(!first["context"].as_str().unwrap().is_empty());
    let baseline = first["snapshot_id"].as_str().unwrap().to_string();

    let second = dispatch_chat(
        &bridge,
        "lead-1",
        "After the subagent returns, reply with PROJECT=fixture",
    );
    assert_eq!(second["cached"], json!(true));
    assert_eq!(second["context"], json!(""));

    let third = dispatch_chat(&bridge, "lead-1", "Reply with exactly: TURN3_OK");
    assert_eq!(third["cached"], json!(true));
    assert_eq!(third["context"], json!(""));
    assert_eq!(third["snapshot_id"], json!(baseline));
}

// ---- V2 model-dispatch baseline (session.context) regressions -------------

/// A `session.context` model dispatch. The payload's `text` models whatever
/// conversation content the adapter could see (a real user turn, a tool
/// continuation, or a runtime-generated synthetic user-role message); the
/// bridge must ignore it — dispatch-time text is never a task signal.
fn dispatch_context(bridge: &BridgeContext<'_>, session: &str, text: &str) -> serde_json::Value {
    bridge.dispatch(
        "session.context",
        &json!({"session_id": session, "text": text}),
    )
}

/// A genuine user-prompt admission. This is the only surface that establishes
/// or resets the session's task identity.
fn admit_task(bridge: &BridgeContext<'_>, session: &str, text: &str) -> serde_json::Value {
    bridge.dispatch(
        "session.prompt",
        &json!({"session_id": session, "text": text}),
    )
}

/// The exact continuation text OpenCode v2.0.11 admits as a synthetic
/// user-role message when a response was interrupted
/// (`packages/core/src/session/runner/llm.ts`). It bypasses prompt admission
/// and reaches the model — and the `session.context` hook — as `role: "user"`.
const SYNTHETIC_CONTINUATION: &str =
    "The previous response was interrupted. Continue from where you left off without repeating completed content.";

/// The V2 model-dispatch invariant: nothing is persisted into the conversation
/// history, so every outgoing root-Lead dispatch receives exactly one full
/// baseline — the first turn, a tool-driven continuation and a later user turn
/// alike. `cached` reports reused computation, never suppressed inclusion.
///
/// Regression coverage for the v0.3.2 blocker: a trailing synthetic
/// user-role continuation must leave the admitted task untouched.
#[test]
fn model_context_supplies_the_full_baseline_on_every_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    // The task enters through prompt admission, never through dispatch
    // content.
    let admitted = admit_task(&bridge, "lead-1", TASK);
    assert_eq!(admitted["ok"], json!(true));
    assert_eq!(admitted["event"], json!("session.prompt"));
    assert_eq!(admitted["changed"], json!(true));
    assert_eq!(admitted["task_id"], json!(Controller::task_id(TASK)));

    // First dispatch: fresh render, full baseline.
    let first = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(first["ok"], json!(true));
    assert_eq!(first["cached"], json!(false));
    let body = first["context"].as_str().unwrap().to_string();
    assert!(body.contains(TASK));
    let snapshot_id = first["snapshot_id"].as_str().unwrap().to_string();
    assert!(snapshot_id.starts_with("sha256:"), "{snapshot_id}");
    let bytes = first["bytes"].as_u64().unwrap();
    assert!(bytes > 0);
    assert_eq!(bytes as usize, body.len());

    // Repeated dispatch (tool-driven continuation, same user message): the
    // computation is reused, but the full body is still supplied.
    let second = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(second["ok"], json!(true));
    assert_eq!(second["cached"], json!(true));
    assert_eq!(second["context"].as_str().unwrap(), body);
    assert_eq!(second["snapshot_id"], json!(snapshot_id));
    assert_eq!(second["bytes"], json!(bytes));

    // A synthetic interruption/resume continuation reaches the dispatch as a
    // trailing user-role message with *different* text. It is not an
    // admission: the baseline is served unchanged and the running task is not
    // reset.
    let third = dispatch_context(&bridge, "lead-1", SYNTHETIC_CONTINUATION);
    assert_eq!(third["cached"], json!(true));
    assert_eq!(third["context"].as_str().unwrap(), body);
    assert_eq!(third["snapshot_id"], json!(snapshot_id));
    assert_eq!(third["task_id"], json!(Controller::task_id(TASK)));
    let session = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(session.task_id, Controller::task_id(TASK));
    assert_eq!(session.task.as_deref(), Some(TASK));
}

/// A tool-continuation dispatch must not clobber the worker orchestration
/// state mid-turn, and a later genuine admission resets the task while
/// carrying the repository baseline over.
#[test]
fn model_context_never_clobbers_worker_state_mid_turn() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    // Turn start: admission establishes the task, the first dispatch renders
    // the baseline.
    admit_task(&bridge, "lead-1", TASK);
    let first = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));
    let body = first["context"].as_str().unwrap().to_string();

    // The Lead delegates to Explore: the session is now owned by the worker.
    controller
        .prepare_handoff("lead-1", Role::Explore, "explore the parser call paths")
        .unwrap();
    let owned = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(owned.source, Role::Explore);
    assert_eq!(owned.phase, OrchestrationPhase::Explore);

    // A tool-driven continuation dispatch (same user message) supplies the
    // baseline again without resetting the worker-owned session state.
    let continuation = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(continuation["cached"], json!(true));
    assert_eq!(continuation["context"].as_str().unwrap(), body);
    let after = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(
        after.source,
        Role::Explore,
        "a context dispatch must not clobber the worker source"
    );
    assert_eq!(after.phase, OrchestrationPhase::Explore);
    assert_eq!(after.destination, Some(Role::Explore));
    assert_eq!(after.task_id, Controller::task_id(TASK));

    // The next user turn is a genuine admission: it resets task-scoped state
    // but reuses the repository baseline body verbatim, because the baseline
    // is session/repository scoped, not task scoped.
    let next = admit_task(&bridge, "lead-1", "a different task entirely");
    assert_eq!(next["changed"], json!(true));
    let reset = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(
        reset.task_id,
        Controller::task_id("a different task entirely")
    );
    assert_eq!(reset.source, Role::Lead);
    assert!(reset.findings.is_empty());
    assert_eq!(reset.attempts, Attempts::default());

    // The first dispatch of the new task serves the carried-over baseline.
    let dispatch = dispatch_context(&bridge, "lead-1", "a different task entirely");
    assert_eq!(dispatch["cached"], json!(true));
    assert_eq!(dispatch["context"].as_str().unwrap(), body);
}

/// Regression for the v0.3.2 release blocker: dispatch-time conversation
/// content — including OpenCode's synthetic interruption/resume user-role
/// message — must leave every piece of task-scoped orchestration state
/// untouched. Only a genuine prompt admission may reset it.
#[test]
fn session_context_dispatch_never_resets_task_state() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = failing_runner();
    let bridge = bridge_with_runtime(&controller, &runner);

    // Admit the task and build up non-trivial task-scoped state: findings, a
    // checkpoint, a consumed build attempt, phase and destination.
    admit_task(&bridge, "lead-1", TASK);
    dispatch_context(&bridge, "lead-1", TASK);
    controller
        .prepare_handoff("lead-1", Role::Explore, "explore the parser")
        .unwrap();
    controller
        .consume_explore_result("lead-1", &explore_json())
        .unwrap();
    controller
        .prepare_handoff("lead-1", Role::Build, "implement the fix")
        .unwrap();
    controller.after_build("lead-1", &runner, None).unwrap();

    let before = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert!(!before.findings.is_empty());
    assert!(!before.checkpoints.is_empty());
    assert_eq!(before.attempts.build, 1);
    assert_eq!(before.task_id, Controller::task_id(TASK));

    // A dispatch carrying the synthetic continuation text (and one carrying a
    // differently phrased would-be task) changes nothing at all.
    let synthetic = dispatch_context(&bridge, "lead-1", SYNTHETIC_CONTINUATION);
    assert_eq!(synthetic["ok"], json!(true));
    let different = dispatch_context(&bridge, "lead-1", "a completely different task");
    assert_eq!(different["ok"], json!(true));

    let after = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(
        after, before,
        "dispatch content must never reset task state"
    );
}

/// The mirror image of the dispatch invariant: a genuinely admitted new
/// prompt *is* a task boundary. It resets findings, attempts, checkpoints,
/// phase and destination — but keeps the repository-scoped baseline.
#[test]
fn session_prompt_admission_resets_task_state() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = failing_runner();
    let bridge = bridge_with_runtime(&controller, &runner);

    // Admit the first task and accumulate task-scoped state plus a retained
    // repository baseline.
    admit_task(&bridge, "lead-1", "first task");
    let baseline = dispatch_context(&bridge, "lead-1", "first task");
    assert_eq!(baseline["cached"], json!(false));
    let body = baseline["context"].as_str().unwrap().to_string();
    controller
        .prepare_handoff("lead-1", Role::Explore, "explore")
        .unwrap();
    controller
        .consume_explore_result("lead-1", &explore_json())
        .unwrap();
    controller
        .prepare_handoff("lead-1", Role::Build, "build it")
        .unwrap();
    controller.after_build("lead-1", &runner, None).unwrap();
    let before = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert!(!before.findings.is_empty());
    assert!(!before.checkpoints.is_empty());
    assert_eq!(before.attempts.build, 1);

    // Re-admitting the same prompt is not a reset.
    let repeat = admit_task(&bridge, "lead-1", "first task");
    assert_eq!(repeat["changed"], json!(false));
    let unchanged = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(unchanged, before);

    // A genuinely new admitted prompt resets task-scoped state.
    let next = admit_task(&bridge, "lead-1", "second task");
    assert_eq!(next["ok"], json!(true));
    assert_eq!(next["changed"], json!(true));
    assert_eq!(next["task_id"], json!(Controller::task_id("second task")));
    let reset = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(reset.task_id, Controller::task_id("second task"));
    assert_eq!(reset.task.as_deref(), Some("second task"));
    assert!(reset.findings.is_empty());
    assert!(reset.checkpoints.is_empty());
    assert_eq!(reset.attempts, Attempts::default());
    assert_eq!(reset.phase, OrchestrationPhase::Idle);
    assert_eq!(reset.source, Role::Lead);
    assert_eq!(reset.destination, Some(Role::Lead));

    // The repository baseline identity and body carry over the reset: the
    // next dispatch reuses the retained rendering for the unchanged
    // repository generation.
    assert_eq!(
        reset.repository_generation_id, before.repository_generation_id,
        "the repository baseline must survive a task reset"
    );
    let dispatch = dispatch_context(&bridge, "lead-1", "second task");
    assert_eq!(dispatch["cached"], json!(true));
    assert_eq!(dispatch["context"].as_str().unwrap(), body);
}

/// A material repository change refreshes the baseline exactly once; the
/// following dispatch reuses the refreshed body.
#[test]
fn model_context_refreshes_once_after_a_repository_change() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    admit_task(&bridge, "lead-1", TASK);
    let first = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));
    let first_id = first["snapshot_id"].as_str().unwrap().to_string();
    let first_body = first["context"].as_str().unwrap().to_string();
    assert!(first_body.contains(TASK));

    // Edit the selected file so the repository generation materially changes.
    fs::write(
        dir.path().join("src/module_1.rs"),
        "pub fn parse_1(value: u32) -> u32 {\n    value + 2\n}\n// changed\n",
    )
    .unwrap();

    // The next dispatch re-renders exactly once.
    let second = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(second["cached"], json!(false));
    let second_id = second["snapshot_id"].as_str().unwrap().to_string();
    assert_ne!(second_id, first_id, "changed repository must refresh");
    let second_body = second["context"].as_str().unwrap().to_string();
    assert!(!second_body.is_empty());
    assert_ne!(second_body, first_body);

    // The dispatch after that reuses the refreshed baseline, and the running
    // task survived the refresh.
    let third = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(third["cached"], json!(true));
    assert_eq!(third["context"].as_str().unwrap(), second_body);
    assert_eq!(third["snapshot_id"], json!(second_id));
    assert_eq!(third["task_id"], json!(Controller::task_id(TASK)));
}

/// The baseline is session scoped: a distinct root Lead session receives its
/// own baseline even when another session already holds the same generation.
#[test]
fn model_context_is_session_scoped() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    admit_task(&bridge, "lead-1", TASK);
    let first = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));

    // A distinct session gets its own baseline (freshly rendered for that
    // session; the identity is legitimately the same repository generation).
    admit_task(&bridge, "lead-2", TASK);
    let other = dispatch_context(&bridge, "lead-2", TASK);
    assert_eq!(other["ok"], json!(true));
    assert_eq!(other["cached"], json!(false));
    assert!(!other["context"].as_str().unwrap().is_empty());
    assert_eq!(other["snapshot_id"], first["snapshot_id"]);

    // Each session then reuses its own retained body.
    let again = dispatch_context(&bridge, "lead-2", TASK);
    assert_eq!(again["cached"], json!(true));
    assert_eq!(again["context"], other["context"]);
}

/// A dispatch without an extractable user message carries no task signal: it
/// must never reset the running task, and it still receives the baseline.
#[test]
fn model_context_without_a_user_message_keeps_the_running_task() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = lead_controller(dir.path(), &git, &clock);
    let runner = FakeCaptureRunner::new();
    let bridge = bridge_with_runtime(&controller, &runner);

    // The running task exists only because it was genuinely admitted; the
    // first dispatch renders the baseline for it.
    admit_task(&bridge, "lead-1", TASK);
    let first = dispatch_context(&bridge, "lead-1", TASK);
    assert_eq!(first["cached"], json!(false));
    let body = first["context"].as_str().unwrap().to_string();
    assert!(body.contains(TASK));

    // A dispatch with no text at all (a bare continuation) keeps the running
    // task and reuses the retained baseline.
    let empty = dispatch_context(&bridge, "lead-1", "");
    assert_eq!(empty["ok"], json!(true));
    assert_eq!(empty["cached"], json!(true));
    assert_eq!(empty["context"].as_str().unwrap(), body);
    let session = controller
        .load_state()
        .state
        .session("lead-1")
        .cloned()
        .unwrap();
    assert_eq!(session.task_id, Controller::task_id(TASK));
    assert_eq!(session.task.as_deref(), Some(TASK));
}
