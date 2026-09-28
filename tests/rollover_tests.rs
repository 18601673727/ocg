//! Contract tests for artifact-backed same-Mission session replacement.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::bridge::BridgeContext;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::context_governor::{ContextObservation, TelemetryProvenance};
use ocg::orchestration::controller::Controller;
use ocg::orchestration::mission::{self, Mission, MissionRolloverStatus};
use ocg::orchestration::state::SessionState;
use ocg::process::{FakeCaptureRunner, FakeGitHost};
use ocg::runtime::compat::{EffectiveLead, LeadSelection, SessionClient, SessionLifecycleClient};
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContextEvent, RuntimeContextObservation,
    RuntimeContinuation, RuntimeError, RuntimeErrorKind, RuntimeExecution, RuntimeExecutionId,
    RuntimeIdentity, RuntimeModelMetadata, RuntimeProfile, RuntimeResult,
};
use ocg::telemetry::TelemetryConfig;
use ocg::verification::config::VerificationConfig;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;

const TASK: &str = "make rollover durable";

fn profile() -> RuntimeProfile {
    lead().runtime_profile()
}

fn lead() -> LeadSelection {
    LeadSelection {
        level: "high".to_string(),
        agent: "lead-high".to_string(),
        provider_id: "test-provider".to_string(),
        model_id: "test-model".to_string(),
        variant: None,
    }
}

#[derive(Default)]
struct RuntimeState {
    agent: Option<String>,
    model: Option<(String, String, Option<String>)>,
    calls: Vec<String>,
    staged: Vec<String>,
    resumed: Vec<String>,
    fail_create: bool,
    fail_stage: bool,
    fail_resume: bool,
    fail_context: bool,
    target: String,
    conflict_on_stage: Option<(PathBuf, String)>,
}

/// A lifecycle-only fake: the controller never constructs an OpenCode HTTP
/// client, service registration, or transport object for these contracts.
#[derive(Clone)]
struct FakeRolloverRuntime {
    state: Rc<RefCell<RuntimeState>>,
}

impl Default for FakeRolloverRuntime {
    fn default() -> Self {
        Self {
            state: Rc::new(RefCell::new(RuntimeState {
                target: "ses_target".to_string(),
                ..RuntimeState::default()
            })),
        }
    }
}

impl FakeRolloverRuntime {
    fn failing_create() -> Self {
        let runtime = Self::default();
        runtime.state.borrow_mut().fail_create = true;
        runtime
    }

    fn failing_stage() -> Self {
        let runtime = Self::default();
        runtime.state.borrow_mut().fail_stage = true;
        runtime
    }

    fn failing_resume() -> Self {
        let runtime = Self::default();
        runtime.state.borrow_mut().fail_resume = true;
        runtime
    }

    fn failing_context() -> Self {
        let runtime = Self::default();
        runtime.state.borrow_mut().fail_context = true;
        runtime
    }

    fn conflicting_on_stage(root: &Path, mission_id: &str) -> Self {
        let runtime = Self::default();
        runtime.state.borrow_mut().conflict_on_stage =
            Some((root.to_path_buf(), mission_id.to_string()));
        runtime
    }
}

impl SessionClient for FakeRolloverRuntime {
    fn resolve_session(&mut self) -> ocg::error::Result<String> {
        self.state
            .borrow_mut()
            .calls
            .push("resolve_session".to_string());
        Ok("ses_source".to_string())
    }

    fn select_agent(&mut self, session: &str, agent: &str) -> ocg::error::Result<()> {
        assert!(session.starts_with("ses_target"));
        self.state.borrow_mut().agent = Some(agent.to_string());
        self.state
            .borrow_mut()
            .calls
            .push(format!("select_agent:{agent}"));
        Ok(())
    }

    fn select_model(
        &mut self,
        session: &str,
        provider_id: &str,
        model_id: &str,
        variant: Option<&str>,
    ) -> ocg::error::Result<()> {
        assert!(session.starts_with("ses_target"));
        self.state.borrow_mut().model = Some((
            provider_id.to_string(),
            model_id.to_string(),
            variant.map(str::to_string),
        ));
        self.state
            .borrow_mut()
            .calls
            .push(format!("select_model:{provider_id}/{model_id}"));
        Ok(())
    }

    fn effective_lead(&self, _session: &str) -> ocg::error::Result<EffectiveLead> {
        let state = self.state.borrow();
        let (provider, model, variant) = state
            .model
            .clone()
            .unwrap_or_else(|| ("test-provider".into(), "test-model".into(), None));
        Ok(EffectiveLead {
            agent: state.agent.clone(),
            provider_id: Some(provider),
            model_id: Some(model),
            variant,
        })
    }
}

impl SessionLifecycleClient for FakeRolloverRuntime {
    fn create_fresh_session(&self) -> ocg::error::Result<String> {
        let mut state = self.state.borrow_mut();
        state.calls.push("create_fresh_session".to_string());
        if state.fail_create {
            return Err(ocg::error::OcgError::config(
                "synthetic target creation failure",
            ));
        }
        Ok(state.target.clone())
    }

    fn context_messages(&self, _session: &str) -> ocg::error::Result<Vec<Value>> {
        if self.state.borrow().fail_context {
            return Err(ocg::error::OcgError::config(
                "synthetic context query failure",
            ));
        }
        Ok(vec![json!({"id": "msg", "type": "assistant"})])
    }

    fn session_info(&self, session: &str) -> ocg::error::Result<Value> {
        Ok(json!({"id": session, "model": {"providerID": "test-provider", "id": "test-model"}}))
    }

    fn model_metadata(
        &self,
        _provider_id: Option<&str>,
        _model_id: Option<&str>,
    ) -> ocg::error::Result<RuntimeModelMetadata> {
        Ok(RuntimeModelMetadata {
            provider_id: Some("test-provider".into()),
            model_id: Some("test-model".into()),
            context_limit: Some(100),
            input_limit: Some(100),
            output_limit: Some(20),
            effective_limit: Some(100),
            source: Some("fake".into()),
        })
    }

    fn inject_continuation(
        &self,
        session: &str,
        message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &Value,
    ) -> ocg::error::Result<()> {
        self.state
            .borrow_mut()
            .calls
            .push(format!("inject:{session}:{message_id}"));
        Ok(())
    }

    fn stage_continuation(
        &self,
        session: &str,
        message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &Value,
    ) -> ocg::error::Result<()> {
        let mut state = self.state.borrow_mut();
        state.calls.push(format!("stage:{session}:{message_id}"));
        if state.fail_stage {
            return Err(ocg::error::OcgError::config("synthetic staging failure"));
        }
        state.staged.push(message_id.to_string());
        let conflict = state.conflict_on_stage.clone();
        drop(state);
        if let Some((root, mission_id)) = conflict {
            let mut mission = mission::load(&root, &mission_id).unwrap().unwrap();
            mission
                .findings
                .push(ocg::orchestration::handoff::HandoffFinding {
                    summary: "concurrent progress".to_string(),
                    detail: None,
                    source: None,
                    severity: ocg::orchestration::handoff::Severity::Info,
                });
            // Simulate the other writer's durable transition witness, not a
            // mere in-memory field mutation.
            mission.revision = mission.revision.saturating_add(1);
            mission::save(&root, &mission).unwrap();
        }
        Ok(())
    }

    fn resume_continuation(
        &self,
        session: &str,
        message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &Value,
    ) -> ocg::error::Result<()> {
        self.state
            .borrow_mut()
            .calls
            .push(format!("resume:{session}:{message_id}"));
        if self.state.borrow().fail_resume {
            return Err(ocg::error::OcgError::config("synthetic resume failure"));
        }
        self.state.borrow_mut().resumed.push(message_id.to_string());
        Ok(())
    }
}

impl RuntimeAdapter for FakeRolloverRuntime {
    fn identity(&self) -> RuntimeIdentity {
        RuntimeIdentity::new("fake", "rollover-test", "in-memory")
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::OPENCODE_V2
    }

    fn resolve_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        <Self as SessionClient>::resolve_session(self)
            .map(RuntimeExecutionId::new)
            .map_err(|error| RuntimeError::new(RuntimeErrorKind::Unavailable, error.to_string()))
    }

    fn create_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        <Self as SessionLifecycleClient>::create_fresh_session(self)
            .map(RuntimeExecutionId::new)
            .map_err(|error| RuntimeError::new(RuntimeErrorKind::Unavailable, error.to_string()))
    }

    fn inspect_execution(
        &self,
        execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<RuntimeExecution> {
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: None,
        })
    }

    fn prepare_execution(
        &mut self,
        execution_id: &RuntimeExecutionId,
        profile: &RuntimeProfile,
    ) -> RuntimeResult<RuntimeExecution> {
        <Self as SessionClient>::select_agent(self, execution_id.as_str(), &profile.profile_id)
            .map_err(|error| {
                RuntimeError::new(RuntimeErrorKind::ProfileSelection, error.to_string())
            })?;
        let (provider, model) = profile.model_selector.split_once('/').ok_or_else(|| {
            RuntimeError::new(RuntimeErrorKind::ProfileSelection, "profile selector")
        })?;
        <Self as SessionClient>::select_model(
            self,
            execution_id.as_str(),
            provider,
            model,
            profile.variant.as_deref(),
        )
        .map_err(|error| {
            RuntimeError::new(RuntimeErrorKind::ProfileSelection, error.to_string())
        })?;
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: Some(profile.clone()),
        })
    }

    fn observe_context(
        &self,
        event: &RuntimeContextEvent,
    ) -> RuntimeResult<RuntimeContextObservation> {
        if self.state.borrow().fail_context {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Unavailable,
                "synthetic context query failure",
            ));
        }
        Ok(RuntimeContextObservation::unknown(event, "fake context"))
    }

    fn stage_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        <Self as SessionLifecycleClient>::stage_continuation(
            self,
            execution_id.as_str(),
            &continuation.id,
            &continuation.text,
            &continuation.description,
            &continuation.metadata,
        )
        .map_err(|error| RuntimeError::new(RuntimeErrorKind::Transport, error.to_string()))
    }

    fn resume_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        <Self as SessionLifecycleClient>::resume_continuation(
            self,
            execution_id.as_str(),
            &continuation.id,
            &continuation.text,
            &continuation.description,
            &continuation.metadata,
        )
        .map_err(|error| RuntimeError::new(RuntimeErrorKind::ProviderCompletion, error.to_string()))
    }
}

fn controller<'a>(root: &Path, git: &'a FakeGitHost, clock: &'a FixedClock) -> Controller<'a> {
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

fn observation(safe_boundary: bool, event_id: &str) -> ContextObservation {
    ContextObservation {
        session_id: "ses_source".to_string(),
        event_id: event_id.to_string(),
        observed_at: 10,
        safe_boundary,
        used_tokens: Some(90),
        limit_tokens: Some(100),
        usage_provenance: TelemetryProvenance::Exact,
        context_provenance: TelemetryProvenance::Exact,
        ..ContextObservation::default()
    }
}

#[test]
fn safe_rollover_preserves_mission_progress_and_targets_the_verified_session() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    ocg.prepare_handoff(
        "ses_source",
        ocg::orchestration::handoff::Role::Explore,
        "inspect",
    )
    .unwrap();
    let before = ocg.load_mission(&mission_id).unwrap().unwrap();

    let mut runtime = FakeRolloverRuntime::default();
    let result = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-safe"),
            &mut runtime,
            &profile(),
        )
        .unwrap();

    assert_eq!(result.rollover_status, Some(MissionRolloverStatus::Applied));
    assert_eq!(result.artifact_status.map(|s| s.as_str()), Some("applied"));
    assert_eq!(result.target_session_id.as_deref(), Some("ses_target"));
    let calls = runtime.state.borrow().calls.clone();
    assert!(!calls.iter().any(|call| call == "resolve_session"));
    assert!(calls.iter().any(|call| call == "create_fresh_session"));
    assert!(calls.iter().any(|call| call.starts_with("stage:")));
    assert!(calls.iter().any(|call| call.starts_with("resume:")));

    let after = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.status, before.status);
    assert_eq!(after.findings, before.findings);
    assert_eq!(after.checkpoints, before.checkpoints);
    assert_eq!(after.session_id.as_deref(), Some("ses_target"));
    assert_eq!(after.rollover.status, MissionRolloverStatus::Applied);
    assert!(after.history.iter().any(|event| {
        event.kind == ocg::orchestration::mission::MissionEventKind::RolloverBound
    }));
    let target_state = ocg
        .load_state()
        .state
        .session("ses_target")
        .cloned()
        .expect("target session seeded from Mission");
    assert_eq!(target_state.task_id, mission_id);
}

#[test]
fn failed_target_creation_keeps_the_old_binding_and_exposes_a_retryable_failure() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::failing_create();

    let result = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-fail"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert!(!result.decision.rollover_allowed);
    assert_eq!(result.rollover_status, Some(MissionRolloverStatus::Failed));
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
    assert_eq!(mission.rollover.status, MissionRolloverStatus::Failed);
    assert!(mission.rollover.retry_after.is_some());
    assert!(runtime.state.borrow().staged.is_empty());
}

#[test]
fn mission_revision_cas_never_overwrites_a_newer_record() {
    let dir = tempfile::tempdir().unwrap();
    let mission = Mission::admit("task-cas-0123456789abcdef", "durable", "ses_source", 1);
    mission::save(dir.path(), &mission).unwrap();
    let expected = mission.revision;
    let mut first = mission.clone();
    first
        .findings
        .push(ocg::orchestration::handoff::HandoffFinding {
            summary: "first".to_string(),
            detail: None,
            source: None,
            severity: ocg::orchestration::handoff::Severity::Info,
        });
    first.revision = first.revision.saturating_add(1);
    assert!(mission::save_if_revision(dir.path(), &first, expected, Some("ses_source")).unwrap());
    let mut stale = mission.clone();
    stale
        .findings
        .push(ocg::orchestration::handoff::HandoffFinding {
            summary: "stale".to_string(),
            detail: None,
            source: None,
            severity: ocg::orchestration::handoff::Severity::Info,
        });
    stale.revision = stale.revision.saturating_add(1);
    assert!(!mission::save_if_revision(dir.path(), &stale, expected, Some("ses_source")).unwrap());
    let loaded = mission::load(dir.path(), "task-cas-0123456789abcdef")
        .unwrap()
        .unwrap();
    assert!(loaded
        .findings
        .iter()
        .any(|finding| finding.summary == "first"));
    assert!(!loaded
        .findings
        .iter()
        .any(|finding| finding.summary == "stale"));
}

#[test]
fn an_artifact_left_before_the_mission_intent_can_be_adopted_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::default();
    let pending = ocg
        .observe_context(
            "ses_source",
            observation(false, "evt-orphan"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert!(pending.decision.deferred_for_boundary);
    let mut mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    mission.rollover = Default::default();
    mission.revision = mission.revision.saturating_add(1);
    mission::save(dir.path(), &mission).unwrap();
    let applied = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-orphan-safe"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        applied.rollover_status,
        Some(MissionRolloverStatus::Applied)
    );
    assert_eq!(
        mission::load(dir.path(), &mission_id)
            .unwrap()
            .unwrap()
            .session_id
            .as_deref(),
        Some("ses_target")
    );
}

#[test]
fn a_concurrent_mission_update_forces_conflict_instead_of_overwriting_progress() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::conflicting_on_stage(dir.path(), &mission_id);
    let result = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-conflict"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        result.rollover_status,
        Some(MissionRolloverStatus::Conflict)
    );
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
    assert_eq!(mission.rollover.status, MissionRolloverStatus::Conflict);
    assert!(mission
        .findings
        .iter()
        .any(|finding| finding.summary == "concurrent progress"));
    let artifact = ocg::orchestration::rollover::latest_for_mission(dir.path(), &mission_id)
        .unwrap()
        .unwrap();
    assert_eq!(artifact.status.as_str(), "conflict");
}

#[test]
fn a_second_pressure_cycle_creates_a_new_artifact_without_changing_generation() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::default();
    let first = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-first"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(first.rollover_status, Some(MissionRolloverStatus::Applied));

    // Simulate a TUI/client restart losing only the disposable session view.
    // The durable Mission must be enough to recover the current owner.
    std::fs::remove_file(ocg::orchestration::state::state_path(dir.path())).unwrap();
    let mut second_observation = observation(true, "evt-second");
    second_observation.session_id = "ses_target".to_string();
    runtime.state.borrow_mut().target = "ses_target_2".to_string();
    let second = ocg
        .observe_context("ses_target", second_observation, &mut runtime, &profile())
        .unwrap();
    assert_eq!(second.rollover_status, Some(MissionRolloverStatus::Applied));
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.generation, 1);
    assert_eq!(mission.session_id.as_deref(), Some("ses_target_2"));
    assert!(
        runtime
            .state
            .borrow()
            .calls
            .iter()
            .filter(|call| *call == "create_fresh_session")
            .count()
            >= 1
    );
}

#[test]
fn an_unsafe_boundary_is_recorded_and_replayed_at_the_next_safe_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::default();

    let pending = ocg
        .observe_context(
            "ses_source",
            observation(false, "evt-pending"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert!(pending.decision.deferred_for_boundary);
    assert!(
        pending.decision.rollover_allowed,
        "policy requests rollover; boundary defers execution"
    );
    assert!(runtime.state.borrow().calls.is_empty());
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
    assert_eq!(mission.rollover.status, MissionRolloverStatus::Preparing);

    let applied = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-safe-after-pending"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        applied.rollover_status,
        Some(MissionRolloverStatus::Applied)
    );
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_target"));
}

#[test]
fn a_failed_rollover_can_retry_after_the_cooldown_without_changing_identity() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let mut clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::failing_stage();
    let failed = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-stage-fail"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(failed.rollover_status, Some(MissionRolloverStatus::Failed));
    let failed_mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(failed_mission.session_id.as_deref(), Some("ses_source"));
    assert!(failed_mission.rollover.retry_after.is_some());

    // A fixed clock before the cooldown cannot retry. Move the deterministic
    // clock to the boundary and make the next attempt succeed.
    drop(ocg);
    clock.set(161);
    runtime.state.borrow_mut().fail_stage = false;
    let ocg = controller(dir.path(), &git, &clock);
    let retried = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-stage-retry"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        retried.rollover_status,
        Some(MissionRolloverStatus::Applied)
    );
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_target"));
    assert_eq!(mission.generation, 1);
}

#[test]
fn a_resume_failure_after_cutover_is_recoverable_without_rebinding_the_old_session() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let mut clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::failing_resume();
    let failed = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-resume-fail"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(failed.rollover_status, Some(MissionRolloverStatus::Active));
    let after_failure = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(after_failure.session_id.as_deref(), Some("ses_target"));
    assert_eq!(after_failure.rollover.status, MissionRolloverStatus::Active);

    drop(ocg);
    clock.set(161);
    runtime.state.borrow_mut().fail_resume = false;
    let ocg = controller(dir.path(), &git, &clock);
    let recovered = ocg
        .observe_context(
            "ses_target",
            {
                let mut value = observation(true, "evt-resume-retry");
                value.session_id = "ses_target".to_string();
                value.used_tokens = Some(10);
                value
            },
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        recovered.rollover_status,
        Some(MissionRolloverStatus::Applied)
    );
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_target"));
    assert_eq!(mission.generation, 1);
    assert_eq!(runtime.state.borrow().resumed.len(), 1);
    assert_eq!(
        runtime.state.borrow().staged.len(),
        1,
        "recovery resumes the staged message without duplicating it"
    );
}

#[test]
fn a_failed_v2_context_query_is_unknown_and_cannot_change_the_mission_owner() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let runner = FakeCaptureRunner::new();
    let bridge = BridgeContext::new(&ocg, &runner, TelemetryConfig::disabled())
        .with_rollover_runtime(FakeRolloverRuntime::failing_context(), profile());
    let value = bridge.dispatch(
        "context.observe",
        &serde_json::json!({
            "session_id": "ses_source",
            "event_id": "evt-query-failure",
            "agent": "lead-high",
            "finish": "stop",
            "tokens": {"input": 90, "cache": {"read": 0}},
            "safe_boundary": true,
            "output_persisted": true
        }),
    );
    assert_eq!(value["ok"], serde_json::json!(true));
    assert_eq!(value["decision"]["state"], serde_json::json!("unknown"));
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
}

#[test]
fn explicit_readmission_can_supersede_an_unacknowledged_active_rollover() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::failing_resume();
    let failed = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-readmission-failure"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(failed.rollover_status, Some(MissionRolloverStatus::Active));

    let readmitted = ocg.admit_user_task("ses_replacement", TASK).unwrap();
    assert_eq!(readmitted.task_id, mission_id);
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_replacement"));
    assert_eq!(mission.generation, 1);
    assert_eq!(mission.rollover.status, MissionRolloverStatus::Idle);
    let artifact = ocg::orchestration::rollover::latest_for_mission(dir.path(), &mission_id)
        .unwrap()
        .unwrap();
    assert_eq!(artifact.status.as_str(), "conflict");
}

#[test]
fn an_unavailable_runtime_client_never_changes_the_mission_owner() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut unknown = observation(true, "evt-unknown");
    unknown.used_tokens = None;
    unknown.context_provenance = TelemetryProvenance::Unknown;
    let mut runtime = FakeRolloverRuntime::default();
    let result = ocg
        .observe_context("ses_source", unknown, &mut runtime, &profile())
        .unwrap();
    assert!(!result.decision.rollover_allowed);
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
    assert!(runtime.state.borrow().calls.is_empty());
}

#[test]
fn a_disabled_governor_is_inert_and_does_not_write_telemetry_or_rollover() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let mut config = OrchestrationConfig::default();
    config.context_governor.enabled = false;
    let ocg = Controller::new(
        dir.path(),
        config,
        ContextConfig::default(),
        CapabilityConfig::default(),
        VerificationConfig::default(),
        &git,
        &clock,
    );
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    let mut runtime = FakeRolloverRuntime::default();
    let result = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-disabled"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert_eq!(
        result.decision.state,
        ocg::orchestration::GovernorState::Disabled
    );
    assert!(runtime.state.borrow().calls.is_empty());
    assert!(!dir.path().join(".ocg/orchestration/context").exists());
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
}

#[test]
fn a_terminal_mission_is_never_resurrected_by_observation() {
    let dir = tempfile::tempdir().unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(dir.path(), &git, &clock);
    let mission_id = ocg.admit_user_task("ses_source", TASK).unwrap().task_id;
    ocg.fail_mission("ses_source", "operator stopped the mission")
        .unwrap();
    let mut runtime = FakeRolloverRuntime::default();
    let result = ocg
        .observe_context(
            "ses_source",
            observation(true, "evt-terminal"),
            &mut runtime,
            &profile(),
        )
        .unwrap();
    assert!(!result.decision.rollover_allowed);
    let mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some("ses_source"));
    assert!(runtime.state.borrow().calls.is_empty());
}

// Keep the test fixture's session type referenced so changes to the public
// session seed contract do not silently make this test compile against a
// different shape.
#[test]
fn seed_session_is_still_disposable_state() {
    let mut session = SessionState::new("s", "task", 1);
    session.phase = ocg::orchestration::state::OrchestrationPhase::Build;
    assert_eq!(session.session_id, "s");
}
