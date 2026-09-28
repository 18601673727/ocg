//! Focused tests for the single-node durable Mission reconciler.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::Controller;
use ocg::orchestration::mission::{
    self, Mission, MissionReconcileStatus, MissionRolloverStatus, MissionStatus,
};
use ocg::orchestration::reconcile::{
    plan, ObservationStatus, ReconcileAction, ReconcileInput, ReconcileOutcome, RuntimeObservation,
};
use ocg::process::FakeGitHost;
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContextEvent, RuntimeContextObservation,
    RuntimeContinuation, RuntimeError, RuntimeErrorKind, RuntimeExecution, RuntimeExecutionId,
    RuntimeIdentity, RuntimeProfile, RuntimeRecoveryKey, RuntimeResult,
};
use ocg::verification::config::VerificationConfig;
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashSet;
use std::path::Path;
use std::process::Command;
use std::rc::Rc;

const TASK: &str = "reconcile a durable mission";
const MISSION_SESSION: &str = "ses_old";
const TARGET_SESSION: &str = "ses_recovered";

#[derive(Default)]
struct FakeState {
    executions: HashSet<String>,
    inspect_calls: usize,
    created: Vec<String>,
    prepared: Vec<String>,
    staged: Vec<String>,
    resumed: Vec<String>,
    recover_calls: usize,
    fail_create: bool,
    fail_prepare: bool,
    fail_stage: bool,
    fail_resume: bool,
    inspect_failure: Option<RuntimeErrorKind>,
    /// Optional override for the next create, so a test can model a second,
    /// distinct replacement execution.
    next_create_id: Option<String>,
}

#[derive(Clone, Default)]
struct FakeRuntime {
    state: Rc<RefCell<FakeState>>,
}

impl FakeRuntime {
    fn with_missing_current() -> Self {
        let runtime = Self::default();
        runtime
            .state
            .borrow_mut()
            .executions
            .extend([TARGET_SESSION.to_string()]);
        runtime
    }

    fn with_current() -> Self {
        let runtime = Self::default();
        runtime
            .state
            .borrow_mut()
            .executions
            .extend([MISSION_SESSION.to_string(), TARGET_SESSION.to_string()]);
        runtime
    }
}

impl RuntimeAdapter for FakeRuntime {
    fn identity(&self) -> RuntimeIdentity {
        RuntimeIdentity::new("fake", "reconcile-test", "unit")
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::OPENCODE_V2
    }

    fn create_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        let mut state = self.state.borrow_mut();
        let id = state
            .next_create_id
            .take()
            .unwrap_or_else(|| TARGET_SESSION.to_string());
        state.created.push(id.clone());
        if state.fail_create {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Unavailable,
                "fake create failure",
            ));
        }
        state.executions.insert(id.clone());
        Ok(RuntimeExecutionId::new(id))
    }

    fn recover_execution(
        &mut self,
        key: &RuntimeRecoveryKey,
    ) -> RuntimeResult<Option<RuntimeExecution>> {
        let mut state = self.state.borrow_mut();
        state.recover_calls += 1;
        if key.mission_id.is_empty() || key.operation_id.is_empty() {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "fake recovery key is incomplete",
            ));
        }
        if state.executions.contains(TARGET_SESSION) {
            Ok(Some(RuntimeExecution {
                id: RuntimeExecutionId::new(TARGET_SESSION),
                profile: None,
            }))
        } else {
            Ok(None)
        }
    }

    fn inspect_execution(
        &self,
        execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<RuntimeExecution> {
        self.state.borrow_mut().inspect_calls += 1;
        if let Some(kind) = self.state.borrow().inspect_failure {
            return Err(RuntimeError::new(kind, "fake inspect failure"));
        }
        if self
            .state
            .borrow()
            .executions
            .contains(execution_id.as_str())
        {
            Ok(RuntimeExecution {
                id: execution_id.clone(),
                profile: None,
            })
        } else {
            Err(RuntimeError::new(
                RuntimeErrorKind::ExecutionMissing,
                "fake execution is missing",
            ))
        }
    }

    fn prepare_execution(
        &mut self,
        execution_id: &RuntimeExecutionId,
        profile: &RuntimeProfile,
    ) -> RuntimeResult<RuntimeExecution> {
        let mut state = self.state.borrow_mut();
        state.prepared.push(execution_id.as_str().to_string());
        if state.fail_prepare {
            return Err(RuntimeError::new(
                RuntimeErrorKind::ProfileSelection,
                "fake profile failure",
            ));
        }
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: Some(profile.clone()),
        })
    }

    fn observe_context(
        &self,
        event: &RuntimeContextEvent,
    ) -> RuntimeResult<RuntimeContextObservation> {
        Ok(RuntimeContextObservation::unknown(event, "fake context"))
    }

    fn stage_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        let mut state = self.state.borrow_mut();
        state.staged.push(continuation.id.clone());
        if state.fail_stage {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Transport,
                "fake stage failure",
            ));
        }
        let _ = execution_id;
        Ok(())
    }

    fn resume_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        let mut state = self.state.borrow_mut();
        if !state.resumed.contains(&continuation.id) {
            state.resumed.push(continuation.id.clone());
        }
        if state.fail_resume {
            return Err(RuntimeError::new(
                RuntimeErrorKind::ProviderCompletion,
                "fake resume failure",
            ));
        }
        let _ = execution_id;
        Ok(())
    }
}

fn controller<'a>(root: &'a Path, git: &'a FakeGitHost, clock: &'a FixedClock) -> Controller<'a> {
    Controller::new(
        root,
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        VerificationConfig::default(),
        git,
        clock,
    )
}

fn profile() -> RuntimeProfile {
    RuntimeProfile::new("lead-high", "test/model", None)
}

fn admitted<'a>(
    root: &'a Path,
    git: &'a FakeGitHost,
    clock: &'a FixedClock,
) -> (Controller<'a>, String) {
    let controller = controller(root, git, clock);
    let id = controller
        .admit_user_task(MISSION_SESSION, TASK)
        .expect("admit Mission")
        .task_id;
    (controller, id)
}

fn planner(
    mission: &Mission,
    observation: RuntimeObservation,
) -> ocg::orchestration::reconcile::ReconcileDecision {
    plan(ReconcileInput {
        mission,
        observation,
        rollover: None,
        capabilities: RuntimeCapabilities::OPENCODE_V2,
        max_debug_retries: 1,
        now: 100,
    })
}

#[test]
fn terminal_mission_is_a_noop_even_when_observation_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    mission.complete(MISSION_SESSION, None, 2).unwrap();
    let decision = planner(&mission, RuntimeObservation::Missing);
    assert_eq!(decision.action, ReconcileAction::Noop);
    assert_eq!(mission.status, MissionStatus::Completed);
}

#[test]
fn healthy_current_execution_does_not_churn_or_use_agent_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mission = controller.load_mission(&id).unwrap().unwrap();
    let execution = RuntimeExecution {
        id: RuntimeExecutionId::new(MISSION_SESSION),
        // Agent/model provenance is deliberately absent from the observation.
        profile: None,
    };
    let decision = planner(&mission, RuntimeObservation::Exists { execution });
    assert_eq!(decision.action, ReconcileAction::Noop);
    assert_eq!(decision.observation, ObservationStatus::Exists);

    let build_execution = RuntimeExecution {
        id: RuntimeExecutionId::new(MISSION_SESSION),
        profile: Some(RuntimeProfile::new("build", "test/model", None)),
    };
    let changed_metadata = planner(
        &mission,
        RuntimeObservation::Exists {
            execution: build_execution,
        },
    );
    assert_eq!(changed_metadata, decision);
}

#[test]
fn missing_is_deterministic_but_observation_failure_is_not_missing() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mission = controller.load_mission(&id).unwrap().unwrap();
    let missing = planner(&mission, RuntimeObservation::Missing);
    let failed = planner(
        &mission,
        RuntimeObservation::TransientTransportFailure(RuntimeError::new(
            RuntimeErrorKind::Transport,
            "fake transport failure",
        )),
    );
    assert_eq!(missing.action, ReconcileAction::RecoverExecution);
    assert_eq!(failed.action, ReconcileAction::Wait);
    assert_ne!(failed.observation, ObservationStatus::Missing);
    assert_eq!(planner(&mission, RuntimeObservation::Missing), missing);
}

#[test]
fn incomplete_rollover_wins_over_generic_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    mission.rollover.status = MissionRolloverStatus::Pending;
    let decision = planner(&mission, RuntimeObservation::Missing);
    assert_eq!(decision.action, ReconcileAction::RecoverRollover);
}

#[test]
fn unsupported_observation_is_explicitly_blocked() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mission = controller.load_mission(&id).unwrap().unwrap();
    let decision = planner(
        &mission,
        RuntimeObservation::Unsupported(RuntimeError::new(
            RuntimeErrorKind::Unsupported,
            "fake unsupported",
        )),
    );
    assert_eq!(decision.action, ReconcileAction::Blocked);
}

#[test]
fn recovery_reuses_same_mission_generation_and_converges_without_duplicate_actions() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut runtime = FakeRuntime::default();
    let mut reconciler =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());

    let mut outcomes = Vec::new();
    for _ in 0..7 {
        outcomes.push(reconciler.reconcile_mission(&id));
    }
    assert!(outcomes
        .iter()
        .any(|result| result.result == ReconcileOutcome::Applied));
    assert_eq!(outcomes.last().unwrap().result, ReconcileOutcome::Noop);
    let mission = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(mission.mission_id, id);
    assert_eq!(mission.generation, 1);
    assert_eq!(mission.session_id.as_deref(), Some(TARGET_SESSION));
    assert_eq!(
        mission.reconcile.status,
        ocg::orchestration::mission::MissionReconcileStatus::Applied
    );
    drop(reconciler);
    assert_eq!(runtime.state.borrow().created.len(), 1);
    assert_eq!(runtime.state.borrow().staged.len(), 1);
    assert_eq!(runtime.state.borrow().resumed.len(), 1);
    let mut after_restart =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
    let repeated = after_restart.reconcile_mission(&id);
    assert_eq!(repeated.result, ReconcileOutcome::Noop);
    drop(after_restart);
    assert_eq!(runtime.state.borrow().created.len(), 1);
    assert_eq!(runtime.state.borrow().staged.len(), 1);
    assert_eq!(runtime.state.borrow().resumed.len(), 1);
}

#[test]
fn observation_failure_does_not_trigger_creation() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut runtime = FakeRuntime::with_missing_current();
    runtime.state.borrow_mut().inspect_failure = Some(RuntimeErrorKind::Transport);
    let mut reconciler =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
    let result = reconciler.reconcile_mission(&id);
    assert_eq!(
        result.observed,
        ObservationStatus::TransientTransportFailure
    );
    assert_eq!(result.result, ReconcileOutcome::Deferred);
    let second = reconciler.reconcile_mission(&id);
    assert_eq!(second.result, ReconcileOutcome::Deferred);
    drop(reconciler);
    assert_eq!(runtime.state.borrow().inspect_calls, 1);
    assert!(runtime.state.borrow().created.is_empty());
    let mission = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(
        mission.reconcile.observation_status.as_deref(),
        Some("transient_transport_failure")
    );
    assert!(mission.reconcile.observation_retry_after.is_some());
}

#[test]
fn restart_recovers_a_created_target_before_local_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut runtime = FakeRuntime::default();
    let mut first =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
    let first_result = first.reconcile_mission(&id);
    assert_eq!(first_result.result, ReconcileOutcome::Applied);
    let operation_id = mission::load(dir.path(), &id)
        .unwrap()
        .unwrap()
        .reconcile
        .operation_id
        .unwrap();
    let mut artifact = ocg::orchestration::reconcile::load_artifact(dir.path(), &operation_id)
        .unwrap()
        .unwrap();
    let mut mission = mission::load(dir.path(), &id).unwrap().unwrap();
    // Simulate the process dying after the runtime create and before the
    // Mission/artifact outcome was persisted.
    artifact.target_execution_id = None;
    artifact.phase = ocg::orchestration::mission::MissionReconcileStatus::Creating;
    ocg::orchestration::reconcile::save_artifact(dir.path(), &artifact).unwrap();
    mission.reconcile.status = ocg::orchestration::mission::MissionReconcileStatus::Creating;
    mission.reconcile.target_execution_id = None;
    mission.reconcile.create_attempt = 1;
    mission.revision = mission.revision.saturating_add(1);
    mission::save(dir.path(), &mission).unwrap();

    let mut restarted =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
    let recovered = restarted.reconcile_mission(&id);
    assert_eq!(recovered.result, ReconcileOutcome::Applied);
    let bound = restarted.reconcile_mission(&id);
    assert_eq!(bound.result, ReconcileOutcome::Applied);
    drop(restarted);
    assert_eq!(runtime.state.borrow().created.len(), 1);
    assert_eq!(runtime.state.borrow().recover_calls, 1);
    let mission = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some(TARGET_SESSION));
}

#[test]
fn restart_before_a_runtime_side_effect_can_claim_and_create_once() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    let operation_id = "rec-before-side-effect".to_string();
    assert!(mission.begin_reconcile(
        &operation_id,
        Some(&RuntimeExecutionId::new(MISSION_SESSION)),
        100
    ));
    mission.reconcile.create_attempt = 1;
    mission.revision = mission.revision.saturating_add(1);
    mission::save(dir.path(), &mission).unwrap();
    let artifact = ocg::orchestration::reconcile::ReconcileArtifact::new(
        &mission,
        &operation_id,
        profile(),
        100,
        1,
        16_384,
    )
    .unwrap();
    ocg::orchestration::reconcile::save_artifact(dir.path(), &artifact).unwrap();

    let mut runtime = FakeRuntime::default();
    let mut reconciler =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
    let result = reconciler.reconcile_mission(&id);
    assert_eq!(result.result, ReconcileOutcome::Applied);
    assert_eq!(runtime.state.borrow().created.len(), 1);
    assert_eq!(runtime.state.borrow().recover_calls, 1);
}

#[test]
fn bridge_persistence_cannot_erase_reconcile_control_state() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    assert!(mission.begin_reconcile(
        "rec-bridge-preserve",
        Some(&RuntimeExecutionId::new(MISSION_SESSION)),
        2
    ));
    mission::save(dir.path(), &mission).unwrap();

    controller
        .prepare_lead_context(MISSION_SESSION, TASK)
        .unwrap();
    let after = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(
        after.reconcile.status,
        ocg::orchestration::mission::MissionReconcileStatus::Creating
    );
    assert_eq!(
        after.reconcile.operation_id.as_deref(),
        Some("rec-bridge-preserve")
    );
}

#[test]
fn current_execution_authority_does_not_promote_a_child_or_stale_execution() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mission = controller.load_mission(&id).unwrap().unwrap();
    assert!(
        controller.is_current_execution_for(&mission, &RuntimeExecutionId::new(MISSION_SESSION))
    );
    assert!(!controller.is_current_execution_for(&mission, &RuntimeExecutionId::new("ses_child")));
    assert_ne!(id, MISSION_SESSION);
}

#[test]
fn runtime_adapter_fake_keeps_planner_transport_independent() {
    // The fake implements only the neutral lifecycle contract. No OpenCode
    // request/response type is needed to plan or execute this test.
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut runtime = FakeRuntime::with_current();
    let result =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile())
            .reconcile_mission(&id);
    assert_eq!(result.result, ReconcileOutcome::Noop);
    let _ = json!({ "agent": "build" });
}

#[test]
fn cli_reconcile_once_reports_unavailable_runtime_without_creating() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    // `reconcile` is a non-interactive surface: it requires an OCG Profile.
    std::fs::write(
        project.join(".ocg.yaml"),
        serde_yaml_ng::to_string(&json!({
            "profile": {"origin": "new", "defaultModel": "local"},
            "models": {"providers": {"local": {"label": "Local", "placeholder": false}},
                "models": {"local": {"provider": "local", "id": "local", "placeholder": false}}}
        }))
        .unwrap(),
    )
    .unwrap();
    let mission = Mission::admit("task-cli-0123456789abcdef", TASK, MISSION_SESSION, 1);
    mission::save(&project, &mission).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .current_dir(&project)
        .args([
            "reconcile",
            "--once",
            "--project",
            project.to_str().unwrap(),
        ])
        .env("OCG_USER_CONFIG", dir.path().join("no-user.yaml"))
        .env("OCG_PROJECT_CONFIG", project.join(".ocg.yaml"))
        .env("XDG_STATE_HOME", dir.path().join("no-opencode-state"))
        .env_remove("OCG_V2_SERVER_URL")
        .env_remove("OCG_V2_SERVER_PASSWORD")
        .env_remove("OCG_LEAD_CONTRACT")
        .env_remove("OCG_ORCHESTRATION")
        .output()
        .expect("run ocg reconcile");
    assert!(
        !output.status.success(),
        "runtime-unavailable pass is not converged"
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("reconcile JSON");
    assert_eq!(
        value["results"][0]["observed"],
        json!("runtime_unavailable")
    );
    assert_eq!(value["results"][0]["decision"], json!("wait"));
    assert_eq!(value["results"][0]["result"], json!("deferred"));
    assert_eq!(
        value["results"][0]["current_execution_id"],
        json!(MISSION_SESSION)
    );
    assert!(!project.join(".ocg/orchestration/reconcile").exists());
}

#[test]
fn a_later_loss_of_the_recovery_target_starts_a_fresh_recovery_operation() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut runtime = FakeRuntime::default();
    {
        let mut reconciler =
            ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
        for _ in 0..6 {
            if reconciler.reconcile_mission(&id).result == ReconcileOutcome::Noop {
                break;
            }
        }
    }
    let first = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(first.session_id.as_deref(), Some(TARGET_SESSION));
    assert_eq!(first.reconcile.operation_count, 1);

    // The converged replacement is lost after it became the Mission's current
    // execution. Convergence must be able to start a brand-new operation
    // instead of being trapped on the already-applied one.
    runtime.state.borrow_mut().executions.remove(TARGET_SESSION);
    runtime.state.borrow_mut().next_create_id = Some("ses_recovered_2".to_string());
    {
        let mut reconciler =
            ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile());
        for _ in 0..8 {
            if reconciler.reconcile_mission(&id).result == ReconcileOutcome::Noop {
                break;
            }
        }
    }
    let second = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(second.session_id.as_deref(), Some("ses_recovered_2"));
    assert_eq!(second.reconcile.operation_count, 2);
    assert_eq!(second.reconcile.status, MissionReconcileStatus::Applied);
    assert_eq!(runtime.state.borrow().created.len(), 2);
}

#[test]
fn explicit_admission_supersedes_and_freezes_an_in_flight_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    assert!(mission.begin_reconcile(
        "rec-supersede",
        Some(&RuntimeExecutionId::new(MISSION_SESSION)),
        2
    ));
    // An interrupted rollover makes the explicit admission take the supersede
    // path, which rebinds the session directly and must not bypass the
    // in-flight reconcile guard.
    mission.rollover.status = MissionRolloverStatus::Pending;
    mission.rollover.artifact_id = Some("roll-supersede".to_string());
    mission.rollover.source_session_id = Some(MISSION_SESSION.to_string());
    mission.rollover.generation = mission.generation;
    mission::save(dir.path(), &mission).unwrap();

    controller.admit_user_task("ses_explicit", TASK).unwrap();
    let after = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(after.session_id.as_deref(), Some("ses_explicit"));
    assert_eq!(after.reconcile.status, MissionReconcileStatus::Conflict);
    assert_eq!(
        after.reconcile.operation_id.as_deref(),
        Some("rec-supersede")
    );
}

#[test]
fn incomplete_rollover_is_not_preempted_by_a_corrupt_generic_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let mut mission = controller.load_mission(&id).unwrap().unwrap();
    assert!(mission.begin_reconcile(
        "rec-corrupt",
        Some(&RuntimeExecutionId::new(MISSION_SESSION)),
        2
    ));
    mission.rollover.status = MissionRolloverStatus::Pending;
    mission.rollover.artifact_id = Some("roll-corrupt".to_string());
    mission.rollover.source_session_id = Some(MISSION_SESSION.to_string());
    mission.rollover.generation = mission.generation;
    mission::save(dir.path(), &mission).unwrap();
    let path = ocg::orchestration::reconcile::artifact_path(dir.path(), "rec-corrupt").unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "{ this is not valid json").unwrap();

    // No runtime: the rollover waits, but the corrupt generic artifact must not
    // turn the pass into a hard failure.
    let mut reconciler =
        ocg::orchestration::reconcile::Reconciler::without_runtime(&controller, profile());
    let run = reconciler.reconcile_once();
    assert!(
        run.results
            .iter()
            .all(|result| result.result != ReconcileOutcome::Failed),
        "a corrupt generic artifact preempted rollover recovery: {:?}",
        run.results
    );
    assert!(run
        .issues
        .iter()
        .any(|issue| issue.classification == "corrupt_artifact"));
}

#[test]
fn a_corrupt_artifact_for_another_mission_does_not_block_a_healthy_one() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let (controller, id) = admitted(dir.path(), &git, &clock);
    let path = ocg::orchestration::reconcile::artifact_path(dir.path(), "rec-other").unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        r#"{"schema_version":1,"mission_id":"task-unrelated"}"#,
    )
    .unwrap();

    let mut runtime = FakeRuntime::with_current();
    let result =
        ocg::orchestration::reconcile::Reconciler::new(&controller, &mut runtime, profile())
            .reconcile_mission(&id);
    assert_eq!(result.result, ReconcileOutcome::Noop);
}
