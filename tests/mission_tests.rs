//! Focused tests for the durable Mission foundation.
//!
//! The invariant under test: **session is disposable execution state; Mission
//! is durable product state.** A Mission must survive the death, replacement
//! or rollover of any individual OpenCode/model session, keep its identity
//! and committed progress, and never replay a committed side effect on
//! recovery.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::checkpoint;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::{BuildDecision, Controller};
use ocg::orchestration::handoff::Role;
use ocg::orchestration::mission::{
    self, Mission, MissionEventKind, MissionStatus, NextAction, MISSION_SCHEMA_VERSION,
};
use ocg::orchestration::state::OrchestrationPhase;
use ocg::process::{FakeCaptureRunner, FakeGitHost};
use ocg::verification::config::VerificationConfig;
use serde_json::json;
use std::fs;
use std::path::Path;

const TASK: &str = "make the parser tolerate unicode escapes";
const MAX_DEBUG_RETRIES: usize = 1;

fn fixture(root: &Path) {
    fs::create_dir_all(root.join("src")).unwrap();
    for index in 1..12 {
        fs::write(
            root.join(format!("src/module_{index}.rs")),
            format!("pub fn parse_{index}(value: u32) -> u32 {{\n    value + 1\n}}\n"),
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

/// A controller over `root`, as a fresh process would build it.
fn new_controller<'a>(root: &Path, git: &'a FakeGitHost, clock: &'a FixedClock) -> Controller<'a> {
    Controller::new(
        root,
        OrchestrationConfig {
            max_build_retries: 2,
            max_debug_retries: MAX_DEBUG_RETRIES,
            ..OrchestrationConfig::default()
        },
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification(),
        git,
        clock,
    )
}

fn passing_runner() -> FakeCaptureRunner {
    FakeCaptureRunner::new().with_success("cargo", &["test"], "test result: ok\n")
}

fn failing_runner() -> FakeCaptureRunner {
    FakeCaptureRunner::new().with_failure("cargo", &["test"], 1, "error: mismatched types\n")
}

fn explore_json() -> String {
    json!({
        "goal": "make unicode escapes parse",
        "constraints": ["keep the public API"],
        "files": ["src/module_1.rs"],
        "symbols": [{"name": "parse_1"}],
        "findings": [{"summary": "escape handling lives in parse_1", "severity": "warning"}]
    })
    .to_string()
}

fn count_events(mission: &Mission, kind: MissionEventKind) -> usize {
    mission
        .history
        .iter()
        .filter(|event| event.kind == kind)
        .count()
}

/// The recovery requirement, mechanically demonstrated: admit -> persist
/// progress -> restart -> reload the same identity and state -> replace the
/// session binding -> bind a fresh session -> continue without losing
/// progress or replaying committed work.
#[test]
fn mission_survives_session_rollover_and_resumes_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let mission_id = Controller::task_id(TASK);

    // 1. Create/admit a Mission in the original session.
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let before_restart = new_controller(dir.path(), &git, &clock);
    let admission = before_restart.admit_user_task("session-old", TASK).unwrap();
    assert!(admission.changed);
    assert_eq!(admission.task_id, mission_id);
    assert!(mission::mission_path(dir.path(), &mission_id)
        .unwrap()
        .is_file());

    // 2. Persist meaningful progress: explore findings, a checkpoint, and a
    //    failed first verification (which schedules a bounded build retry).
    before_restart
        .prepare_handoff("session-old", Role::Explore, "explore the parser")
        .unwrap();
    let explored = before_restart
        .consume_explore_result("session-old", &explore_json())
        .unwrap();
    let explore_checkpoint = explored.checkpoint_id.clone().expect("checkpoint");
    before_restart
        .prepare_handoff("session-old", Role::Build, "implement the fix")
        .unwrap();
    let first = before_restart
        .after_build("session-old", &failing_runner(), None)
        .unwrap();
    assert!(
        matches!(first.decision, BuildDecision::RetryBuild { attempt: 1, .. }),
        "{:?}",
        first.decision
    );

    let before = before_restart
        .load_mission(&mission_id)
        .unwrap()
        .expect("durable mission");
    assert_eq!(before.session_id.as_deref(), Some("session-old"));
    assert_eq!(before.status, MissionStatus::Active);
    assert_eq!(before.phase, OrchestrationPhase::Build);
    assert_eq!(before.attempts.build, 1);
    assert_eq!(before.attempts.verify, 1);
    assert!(before
        .findings
        .iter()
        .any(|finding| finding.summary.contains("escape handling")));
    assert!(before.checkpoints.contains(&explore_checkpoint));
    assert_eq!(before.next_action(MAX_DEBUG_RETRIES), NextAction::Build);
    assert_eq!(before.next_action(MAX_DEBUG_RETRIES).as_str(), "build");

    // 3./4. Restart: a brand-new controller, over brand-new fakes, reloads the
    //        same Mission identity and state without any conversation history.
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let restarted = new_controller(dir.path(), &git, &clock);
    let reloaded = restarted.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(reloaded.mission_id, before.mission_id);
    assert_eq!(reloaded.generation, before.generation);
    assert_eq!(reloaded.status, before.status);
    assert_eq!(reloaded.phase, before.phase);
    assert_eq!(reloaded.attempts, before.attempts);
    assert_eq!(reloaded.findings, before.findings);
    assert_eq!(reloaded.checkpoints, before.checkpoints);
    assert_eq!(reloaded.next_action(MAX_DEBUG_RETRIES), NextAction::Build);

    // 5./6. Replace the execution binding: a fresh session binds to the same
    //        Mission and is seeded from it.
    let resumed = restarted.admit_user_task("session-new", TASK).unwrap();
    assert!(resumed.changed, "a new session admits the same task");
    assert_eq!(
        resumed.task_id, mission_id,
        "identity is session-independent"
    );

    let rebound = restarted.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(rebound.mission_id, mission_id);
    assert_eq!(rebound.generation, before.generation);
    assert_eq!(rebound.session_id.as_deref(), Some("session-new"));
    assert_eq!(rebound.attempts.build, 1, "progress survived the rollover");
    assert_eq!(rebound.findings, before.findings);
    assert_eq!(rebound.checkpoints, before.checkpoints);
    assert!(rebound.history.iter().any(|event| {
        event.kind == MissionEventKind::SessionBound
            && event.session_id.as_deref() == Some("session-new")
    }));

    let seeded = restarted
        .load_state()
        .state
        .session("session-new")
        .cloned()
        .expect("fresh session seeded from the Mission");
    assert_eq!(seeded.task_id, mission_id);
    assert_eq!(seeded.attempts.build, 1);
    assert!(!seeded.findings.is_empty());
    assert!(seeded.checkpoints.contains(&explore_checkpoint));
    assert_eq!(seeded.phase, OrchestrationPhase::Build);

    // 7. Continue: the retry budget was not reset and the committed first
    //    attempt was not replayed (attempts advance 1 -> 2, verify 1 -> 2).
    restarted
        .prepare_handoff("session-new", Role::Build, "retry the fix")
        .unwrap();
    let second = restarted
        .after_build("session-new", &passing_runner(), None)
        .unwrap();
    assert!(
        matches!(second.decision, BuildDecision::Passed { .. }),
        "{:?}",
        second.decision
    );

    let completed = restarted.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(completed.status, MissionStatus::Completed);
    assert_eq!(completed.phase, OrchestrationPhase::Done);
    assert_eq!(completed.attempts.build, 2, "no replay, no reset");
    assert_eq!(completed.attempts.verify, 2);
    assert_eq!(
        completed.next_action(MAX_DEBUG_RETRIES),
        NextAction::Complete
    );
    assert_eq!(count_events(&completed, MissionEventKind::Completed), 1);

    // The old session is still readable and unchanged: a session is
    // disposable execution state, not Mission identity.
    assert_eq!(
        restarted
            .load_state()
            .state
            .session("session-old")
            .unwrap()
            .attempts
            .build,
        1
    );
}

#[test]
fn admission_creates_a_versioned_durable_record_bound_to_the_session() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = new_controller(dir.path(), &git, &clock);
    controller.admit_user_task("s", TASK).unwrap();

    let mission_id = Controller::task_id(TASK);
    let path = mission::mission_path(dir.path(), &mission_id).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(value["schema_version"], json!(MISSION_SCHEMA_VERSION));
    assert_eq!(value["mission_id"], json!(mission_id));
    assert_eq!(value["status"], json!("active"));
    assert_eq!(value["generation"], json!(1));
    assert_eq!(value["session_id"], json!("s"));
    assert_eq!(value["task"], json!(TASK));
    assert_eq!(
        value["history"][0]["kind"],
        json!("admitted"),
        "admission itself is a recorded transition"
    );

    // Both admission entry points resolve the same durable Mission identity.
    let lead = controller.prepare_lead_context("v1", TASK).unwrap();
    assert_eq!(lead.task_id, mission_id);
    let mission = controller.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("v1"));
    assert_eq!(mission.generation, 1);
}

#[test]
fn terminal_states_are_durable_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let mission_id = Controller::task_id(TASK);
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let ocg = new_controller(dir.path(), &git, &clock);

    // Durable completion: verification passed.
    ocg.admit_user_task("complete", TASK).unwrap();
    ocg.prepare_handoff("complete", Role::Build, "implement")
        .unwrap();
    ocg.after_build("complete", &passing_runner(), None)
        .unwrap();
    let completed = ocg.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(completed.status, MissionStatus::Completed);
    assert_eq!(
        completed.next_action(MAX_DEBUG_RETRIES),
        NextAction::Complete
    );

    // Durable failure with a persisted reason, stable across a restart.
    let failed_task = "a different task that will be failed";
    let failed_id = Controller::task_id(failed_task);
    ocg.admit_user_task("fail", failed_task).unwrap();
    let failed = ocg
        .fail_mission("fail", "the user abandoned this work")
        .unwrap();
    assert_eq!(failed.status, MissionStatus::Failed);

    // Durable cancellation, likewise.
    let cancelled_task = "a third task that will be cancelled";
    let cancelled_id = Controller::task_id(cancelled_task);
    ocg.admit_user_task("cancel", cancelled_task).unwrap();
    let cancelled = ocg
        .cancel_mission("cancel", "superseded by a newer request")
        .unwrap();
    assert_eq!(cancelled.status, MissionStatus::Cancelled);

    // Everything above is read back by a brand-new process-equivalent
    // controller, from durable state alone.
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let restarted = new_controller(dir.path(), &git, &clock);
    let reloaded = restarted.load_mission(&failed_id).unwrap().unwrap();
    assert_eq!(reloaded.status, MissionStatus::Failed);
    let event = reloaded
        .history
        .iter()
        .find(|event| event.kind == MissionEventKind::Failed)
        .expect("failure event");
    assert_eq!(event.note.as_deref(), Some("the user abandoned this work"));
    assert_eq!(event.session_id.as_deref(), Some("fail"));
    let reloaded = restarted.load_mission(&cancelled_id).unwrap().unwrap();
    assert_eq!(reloaded.status, MissionStatus::Cancelled);
    assert_eq!(
        reloaded.next_action(MAX_DEBUG_RETRIES),
        NextAction::Cancelled
    );
    assert_eq!(
        restarted.load_mission(&mission_id).unwrap().unwrap().status,
        MissionStatus::Completed
    );

    // A terminal Mission is never overwritten by another terminal state.
    assert!(restarted.fail_mission("cancel", "too late").is_err());
    assert!(restarted.cancel_mission("fail", "too late").is_err());
    assert_eq!(
        restarted
            .load_mission(&cancelled_id)
            .unwrap()
            .unwrap()
            .status,
        MissionStatus::Cancelled
    );
}

#[test]
fn readmission_after_a_terminal_state_starts_a_new_generation() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = new_controller(dir.path(), &git, &clock);
    let mission_id = Controller::task_id(TASK);

    controller.admit_user_task("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore the parser")
        .unwrap();
    controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    controller
        .prepare_handoff("s", Role::Build, "implement")
        .unwrap();
    controller
        .after_build("s", &passing_runner(), None)
        .unwrap();
    let terminal = controller.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(terminal.status, MissionStatus::Completed);
    assert!(!terminal.checkpoints.is_empty());
    let terminal_history = terminal.history.len();

    // Re-admitting the same task in a different session resumes the same
    // identity as the next generation: progress resets, history is kept.
    let admission = controller.admit_user_task("s2", TASK).unwrap();
    assert_eq!(admission.task_id, mission_id, "identity never changes");
    let next = controller.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(next.generation, 2);
    assert_eq!(next.status, MissionStatus::Active);
    assert_eq!(next.phase, OrchestrationPhase::Idle);
    assert_eq!(next.attempts.build, 0);
    assert!(next.findings.is_empty());
    assert!(next.checkpoints.is_empty());
    assert!(next.history.len() > terminal_history);
    assert!(next
        .history
        .iter()
        .any(|event| { event.kind == MissionEventKind::Completed && event.generation == 1 }));
    assert!(next
        .history
        .iter()
        .any(|event| { event.kind == MissionEventKind::Admitted && event.generation == 2 }));

    let seeded = controller
        .load_state()
        .state
        .session("s2")
        .cloned()
        .unwrap();
    assert_eq!(seeded.task_id, mission_id);
    assert_eq!(seeded.attempts.build, 0);
}

#[test]
fn a_terminal_mission_is_not_reopened_by_a_re_delivered_prompt() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let ocg = new_controller(dir.path(), &git, &clock);
    let mission_id = Controller::task_id(TASK);

    ocg.admit_user_task("s", TASK).unwrap();
    ocg.prepare_handoff("s", Role::Build, "implement").unwrap();
    ocg.after_build("s", &passing_runner(), None).unwrap();
    let completed = ocg.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(completed.status, MissionStatus::Completed);

    // A re-delivered prompt in the live session is the same admission, not a
    // new attempt: the terminal record stays terminal and unrewritten.
    let readmitted = ocg.admit_user_task("s", TASK).unwrap();
    assert!(!readmitted.changed);
    assert_eq!(
        ocg.load_mission(&mission_id).unwrap().unwrap(),
        completed,
        "a terminal generation is never silently reopened or rewritten"
    );

    // A fresh session admitting the same task starts the next generation
    // instead, with the previous completion retained in history.
    ocg.admit_user_task("s2", TASK).unwrap();
    let next = ocg.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(next.mission_id, mission_id);
    assert_eq!(next.generation, 2);
    assert_eq!(next.status, MissionStatus::Active);
    assert!(next
        .history
        .iter()
        .any(|event| event.kind == MissionEventKind::Completed && event.generation == 1));
}

#[test]
fn replaying_the_same_transition_identity_has_no_duplicate_effect() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = new_controller(dir.path(), &git, &clock);
    let mission_id = Controller::task_id(TASK);

    controller.admit_user_task("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore the parser")
        .unwrap();
    let first = controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    let after_first = controller.load_mission(&mission_id).unwrap().unwrap();

    // The same committed explore result is re-delivered after a crash/replay.
    let replay = controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    let after_replay = controller.load_mission(&mission_id).unwrap().unwrap();

    assert_eq!(replay.checkpoint_id, first.checkpoint_id);
    assert_eq!(
        after_replay.checkpoints, after_first.checkpoints,
        "checkpoint references are not duplicated"
    );
    assert_eq!(
        after_replay.findings, after_first.findings,
        "findings are not duplicated"
    );
    assert_eq!(after_replay.attempts, after_first.attempts);
    assert_eq!(
        count_events(&after_replay, MissionEventKind::ExploreToBuild),
        1
    );
    assert_eq!(count_events(&after_replay, MissionEventKind::Admitted), 1);
    assert_eq!(after_replay.history, after_first.history);

    // Re-admitting the same running task is idempotent at the record level:
    // no new admission event, no re-binding, no progress change.
    controller.admit_user_task("s", TASK).unwrap();
    let after_readmit = controller.load_mission(&mission_id).unwrap().unwrap();
    assert_eq!(after_readmit.generation, after_first.generation);
    assert_eq!(after_readmit.history, after_first.history);
    assert_eq!(after_readmit.session_id.as_deref(), Some("s"));
}

#[test]
fn a_corrupt_mission_fails_explicitly_and_preserves_the_record() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = new_controller(dir.path(), &git, &clock);
    let mission_id = Controller::task_id(TASK);

    // A legacy projection that predates the replay authority is corrupt. This
    // exercises the migration-bootstrap read path, where the projection is the
    // authority until replay is first initialized.
    let path = mission::mission_path(dir.path(), &mission_id).unwrap();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "{ truncated").unwrap();

    // A fresh session admitting the task must NOT silently restart.
    let error = controller.admit_user_task("s2", TASK).unwrap_err();
    let text = error.to_string();
    assert!(text.contains("corrupt"), "{text}");
    assert!(text.contains("quarantined"), "{text}");
    assert!(!path.exists(), "the corrupt record was moved aside");
    assert_eq!(
        fs::read_to_string(path.with_extension("corrupt.json")).unwrap(),
        "{ truncated",
        "recoverable bytes are preserved for inspection"
    );
    assert!(
        controller.load_state().state.session("s2").is_none(),
        "no session was admitted against a corrupt Mission"
    );

    // The quarantined artifact is unresolved corruption: it blocks an
    // automatic replay bootstrap rather than being silently incorporated.
    let retry = controller.admit_user_task("s2", TASK).unwrap_err();
    assert!(retry.to_string().contains("unresolved corrupt"), "{retry}");
    assert!(path.with_extension("corrupt.json").is_file());
}

#[test]
fn missions_and_the_existing_session_state_coexist() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let controller = new_controller(dir.path(), &git, &clock);

    controller.admit_user_task("s", TASK).unwrap();
    controller
        .prepare_handoff("s", Role::Explore, "explore the parser")
        .unwrap();
    controller
        .consume_explore_result("s", &explore_json())
        .unwrap();
    controller
        .prepare_handoff("s", Role::Build, "implement")
        .unwrap();
    controller
        .after_build("s", &failing_runner(), None)
        .unwrap();

    // Session state keeps its existing shape and remains disposable.
    let state_path = dir.path().join(".ocg/orchestration/state.json");
    let value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap()).unwrap();
    assert_eq!(
        value["sessions"]["s"]["task_id"],
        json!(Controller::task_id(TASK))
    );
    assert_eq!(value["sessions"]["s"]["phase"], json!("build"));

    // Checkpoints remain the shared, referenced evidence store: the Mission
    // stores references, not a copy of the evidence.
    let (checkpoints, corrupt) = checkpoint::list(dir.path());
    assert_eq!(corrupt, 0);
    assert!(checkpoints
        .iter()
        .any(|summary| summary.phase.as_str() == "explore_to_build"));
    let mission = controller
        .load_mission(&Controller::task_id(TASK))
        .unwrap()
        .unwrap();
    assert!(!mission.checkpoints.is_empty());
    assert!(mission
        .checkpoints
        .iter()
        .all(|id| checkpoint::load(dir.path(), id, &git).is_ok()));
}

#[test]
fn mission_listing_is_read_only_and_does_not_create_state() {
    let dir = tempfile::tempdir().unwrap();
    let (summaries, corrupt) = mission::list(dir.path());
    assert!(summaries.is_empty());
    assert_eq!(corrupt, 0);
    assert!(!dir.path().join(".ocg").exists());
}

#[test]
fn an_invalid_revision_is_quarantined_instead_of_being_adopted() {
    let dir = tempfile::tempdir().unwrap();
    let mission_id = Controller::task_id(TASK);
    // Write the legacy projection directly so the replay authority is not
    // initialized and the projection validation is exercised.
    let path = mission::mission_path(dir.path(), &mission_id).unwrap();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut value = serde_json::to_value(Mission::admit(&mission_id, TASK, "s", 1)).unwrap();
    value["revision"] = json!(0);
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(mission::load(dir.path(), &mission_id).is_err());
    assert!(path
        .with_file_name(format!("{mission_id}.corrupt.json"))
        .is_file());
}

#[test]
fn unsafe_mission_identity_cannot_escape_the_mission_directory() {
    let dir = tempfile::tempdir().unwrap();
    assert!(mission::mission_path(dir.path(), "../../etc/passwd").is_err());
    assert!(mission::mission_path(dir.path(), "task/../other").is_err());
    assert!(mission::save(dir.path(), &Mission::default()).is_err());
    assert!(mission::load(dir.path(), "..").is_err());
}
