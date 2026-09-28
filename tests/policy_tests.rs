//! Focused tests for the Policy / Admission Engine.
//!
//! These tests cover: the typed decision model and its precedence, the
//! `Allow/Deny/Defer/RequireApproval` semantics staying distinguishable,
//! first-class Unknown handling (including `ResourceHealth::Available` not being
//! a capacity claim and a stale fact not being current), the direct (never
//! ranked) association of a resource, the generation-bound approval primitive,
//! Reconciler integration with zero side effects for a non-Allow decision, and
//! the read-only / resolving CLI surface. They never assert that Policy selects
//! or ranks a resource — that is future Placement work.

mod common;

use common::{project_profile, write_yaml, TestDir};
use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::Controller;
use ocg::orchestration::mission::{self, MissionStatus};
use ocg::orchestration::policy::{
    self, approval_id, approval_path, ensure_pending, evaluate, list_approvals, load_approval,
    resolve_approval, ApprovalRequest, ApprovalStatus, ApprovalView, FactStatus, PolicyAction,
    PolicyConfig, PolicyContext, PolicyDecision, ResourceFacts,
};
use ocg::orchestration::reconcile::{ObservationStatus, ReconcileOutcome, Reconciler};
use ocg::process::FakeGitHost;
use ocg::resources::{
    HealthFacts, ResourceHealth, ResourceIdentity, ResourceProvenance, ResourceRegistry,
};
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContinuation, RuntimeError, RuntimeErrorKind,
    RuntimeExecution, RuntimeExecutionId, RuntimeIdentity, RuntimeProfile, RuntimeRecoveryKey,
    RuntimeResult,
};
use ocg::verification::config::VerificationConfig;
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::rc::Rc;

const MISSION_SESSION: &str = "ses_old";
const TARGET_SESSION: &str = "ses_recovered";
const TASK: &str = "policy admission boundary";

/// A realistic API-key shape, split so the repository hygiene scan does not
/// flag this very test file while the runtime value stays secret-shaped.
const SK_LIKE: &str = concat!("sk-", "abcdefghijklmnopqrstuvwxyz012345");

// -- evaluation helpers -------------------------------------------------------

fn model_identity(provider: &str, model: &str) -> ResourceIdentity {
    ResourceIdentity::for_model(provider, model)
}

/// The identity the reconciler associates with `profile()` + `FakeRuntime`.
fn associated_identity() -> ResourceIdentity {
    model_identity("test", "model").with_runtime_family("fake", "reconcile-test")
}

fn context(action: PolicyAction) -> PolicyContext {
    PolicyContext {
        mission_id: "task-1".to_string(),
        generation: 1,
        mission_status: MissionStatus::Active,
        action,
        current_execution_id: Some(RuntimeExecutionId::new(MISSION_SESSION)),
        observation: ObservationStatus::Missing,
        reconcile_status: mission::MissionReconcileStatus::Idle,
        capabilities: RuntimeCapabilities::OPENCODE_V2,
        resource: ResourceFacts::unknown(associated_identity()),
        approval: ApprovalView::not_required(),
        evaluated_at: 1_000,
    }
}

// -- decision vocabulary and precedence ---------------------------------------

#[test]
fn precedence_is_strict_and_order_independent() {
    assert!(PolicyDecision::Deny.precedence() > PolicyDecision::RequireApproval.precedence());
    assert!(PolicyDecision::RequireApproval.precedence() > PolicyDecision::Defer.precedence());
    assert!(PolicyDecision::Defer.precedence() > PolicyDecision::Allow.precedence());

    // A terminal Mission (Deny) that also lacks a capability (Defer) still
    // resolves to the higher-precedence Deny, regardless of rule order.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.mission_status = MissionStatus::Completed;
    ctx.capabilities = RuntimeCapabilities::NONE;
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Deny);
    assert_eq!(assessment.rule, "mission.terminal");

    // A missing capability (Defer) that also needs an approval resolves to
    // RequireApproval: approval outranks defer.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.capabilities = RuntimeCapabilities::NONE;
    ctx.approval = ApprovalView {
        required: true,
        status: None,
        corrupt: false,
    };
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::RequireApproval);
    assert_eq!(assessment.rule, "approval");
}

#[test]
fn co_firing_blocking_rules_are_all_retained_not_reduced_to_the_winner() {
    // A terminal Mission (Deny) that also lacks a required capability (Defer)
    // co-fires two blocking rules. Both must be retained (bounded) so the
    // receipt never silently hides one behind the other.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.mission_status = MissionStatus::Completed;
    ctx.capabilities = RuntimeCapabilities::NONE;
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Deny);
    assert_eq!(assessment.rule, "mission.terminal");
    assert!(
        assessment
            .blocking
            .iter()
            .any(|block| block.starts_with("mission.terminal=")),
        "{:?}",
        assessment.blocking
    );
    assert!(
        assessment
            .blocking
            .iter()
            .any(|block| block.starts_with("runtime.capability=")),
        "{:?}",
        assessment.blocking
    );
    assert!(assessment.blocking.len() <= 8, "receipts stay bounded");

    // The durable summary carries the same bounded blocking set.
    let summary = assessment.summary();
    assert_eq!(summary.blocking, assessment.blocking);
    assert_eq!(summary.decision, "deny");
}

#[test]
fn never_allowed_differs_from_not_safe_and_needs_approval() {
    // "never allowed": a terminal Mission.
    let mut denied = context(PolicyAction::RecoverExecution);
    denied.mission_status = MissionStatus::Failed;
    let denied = evaluate(&denied);
    assert_eq!(denied.decision, PolicyDecision::Deny);
    assert_eq!(denied.reason_code, "terminal_mission");

    // "not currently safe": an unsupported required capability.
    let mut deferred = context(PolicyAction::RecoverExecution);
    deferred.capabilities = RuntimeCapabilities::NONE;
    let deferred = evaluate(&deferred);
    assert_eq!(deferred.decision, PolicyDecision::Defer);
    assert_eq!(deferred.reason_code, "capability_unsupported");

    // "needs user approval".
    let mut approval = context(PolicyAction::EnsureExecution);
    approval.observation = ObservationStatus::Unbound;
    approval.approval = ApprovalView {
        required: true,
        status: Some(ApprovalStatus::Pending),
        corrupt: false,
    };
    let approval = evaluate(&approval);
    assert_eq!(approval.decision, PolicyDecision::RequireApproval);
    assert!(approval.approval.is_some());

    // The three are distinct and each carries an actionable reason code.
    assert_ne!(denied.decision, deferred.decision);
    assert_ne!(deferred.decision, approval.decision);
}

#[test]
fn assessment_is_explainable_and_identity_bound() {
    let ctx = context(PolicyAction::RecoverExecution);
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Allow);
    assert_eq!(assessment.rule, "policy.default_allow");
    assert_eq!(assessment.reason_code, "default_allow");
    assert_eq!(assessment.mission_id, "task-1");
    assert_eq!(assessment.generation, 1);
    assert_eq!(assessment.action, PolicyAction::RecoverExecution);
    assert_eq!(
        assessment.current_execution_id.as_deref(),
        Some(MISSION_SESSION)
    );
    assert_eq!(assessment.evaluated_at, 1_000);
    assert!(!assessment.reason.is_empty());
    assert!(assessment.reason.len() <= policy::MAX_REASON_BYTES);
}

// -- Unknown is first class ---------------------------------------------------

#[test]
fn unknown_capacity_quota_and_cost_are_never_optimistic() {
    let facts = ResourceFacts::unknown(associated_identity());
    assert_eq!(facts.capacity_status(), FactStatus::Unknown);
    assert_eq!(facts.quota_status(), FactStatus::Unknown);
    assert_eq!(facts.cost_status(), FactStatus::Unknown);
    assert_eq!(
        facts.health_status(1_000, policy::RESOURCE_FACT_MAX_AGE_SECONDS),
        FactStatus::Unknown
    );

    // Unknown capacity against a *known* availability still allows: a rule is
    // only blocked by the facts it actually requires.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.resource = ResourceFacts {
        found: true,
        health: ResourceHealth::Available,
        health_observed_at: Some(1_000),
        health_provenance: ResourceProvenance::RuntimeObserved,
        ..ResourceFacts::unknown(associated_identity())
    };
    assert_eq!(ctx.resource.capacity_status(), FactStatus::Unknown);
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Allow);
}

#[test]
fn unknown_health_is_recorded_and_does_not_block_or_claim_health() {
    let ctx = context(PolicyAction::RecoverExecution);
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Allow);
    let fact = assessment
        .required_facts
        .iter()
        .find(|fact| fact.kind == "resource.health")
        .expect("the availability rule surfaces the health fact");
    assert_eq!(fact.status, FactStatus::Unknown);
}

#[test]
fn a_stale_unavailable_fact_is_not_current_and_a_fresh_one_defers() {
    let mut stale = context(PolicyAction::RecoverExecution);
    stale.resource = ResourceFacts {
        found: true,
        health: ResourceHealth::Unavailable,
        health_observed_at: Some(1),
        health_provenance: ResourceProvenance::RuntimeObserved,
        ..ResourceFacts::unknown(associated_identity())
    };
    stale.evaluated_at = policy::RESOURCE_FACT_MAX_AGE_SECONDS + 2;
    assert_eq!(evaluate(&stale).decision, PolicyDecision::Allow);

    let mut fresh = stale.clone();
    fresh.evaluated_at = 2;
    let fresh = evaluate(&fresh);
    assert_eq!(fresh.decision, PolicyDecision::Defer);
    assert_eq!(fresh.reason_code, "resource_unavailable");
}

// -- Registry semantics -------------------------------------------------------

#[test]
fn availability_is_not_a_capacity_fact() {
    // `Available` is reachability, never a claim that the resource can accept
    // new work.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.resource = ResourceFacts {
        found: true,
        health: ResourceHealth::Available,
        health_observed_at: Some(1_000),
        health_provenance: ResourceProvenance::RuntimeObserved,
        ..ResourceFacts::unknown(associated_identity())
    };
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Allow);
    assert_eq!(ctx.resource.capacity_status(), FactStatus::Unknown);
    // No rule claims a capacity value.
    let detail = assessment
        .required_facts
        .iter()
        .find(|fact| fact.kind == "resource.health")
        .map(|fact| fact.detail.clone())
        .unwrap_or_default();
    assert!(!detail.to_ascii_lowercase().contains("capacity"));
}

#[test]
fn an_unregistered_resource_stays_unknown() {
    let facts = ResourceFacts::unknown(associated_identity());
    assert!(!facts.found);
    assert_eq!(facts.health, ResourceHealth::Unknown);
    assert_eq!(facts.health_provenance, ResourceProvenance::Unknown);
    // A missing record is not a substitute candidate.
    assert_eq!(facts.identity, associated_identity());
}

// -- approval identity --------------------------------------------------------

#[test]
fn approval_identity_is_bound_to_generation_and_action() {
    let execution = RuntimeExecutionId::new(MISSION_SESSION);
    let base = approval_id(
        "task-1",
        1,
        PolicyAction::RecoverExecution,
        Some(&execution),
    );
    assert_eq!(
        base,
        approval_id(
            "task-1",
            1,
            PolicyAction::RecoverExecution,
            Some(&execution)
        ),
        "the same exact action is idempotent"
    );
    assert_ne!(
        base,
        approval_id(
            "task-1",
            2,
            PolicyAction::RecoverExecution,
            Some(&execution)
        ),
        "a later generation cannot inherit an approval"
    );
    assert_ne!(
        base,
        approval_id("task-1", 1, PolicyAction::EnsureExecution, Some(&execution)),
        "a different action cannot inherit an approval"
    );
    assert!(base.starts_with("apr-"));
}

// -- Reconciler fakes ---------------------------------------------------------

#[derive(Default)]
struct FakeState {
    executions: HashSet<String>,
    created: Vec<String>,
    recover_calls: usize,
}

#[derive(Clone, Default)]
struct FakeRuntime {
    state: Rc<RefCell<FakeState>>,
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
        state.created.push(TARGET_SESSION.to_string());
        state.executions.insert(TARGET_SESSION.to_string());
        Ok(RuntimeExecutionId::new(TARGET_SESSION))
    }

    fn recover_execution(
        &mut self,
        _key: &RuntimeRecoveryKey,
    ) -> RuntimeResult<Option<RuntimeExecution>> {
        let mut state = self.state.borrow_mut();
        state.recover_calls += 1;
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
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: Some(profile.clone()),
        })
    }

    fn stage_runtime_continuation(
        &self,
        _execution_id: &RuntimeExecutionId,
        _continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn resume_runtime_continuation(
        &self,
        _execution_id: &RuntimeExecutionId,
        _continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        Ok(())
    }
}

fn controller<'a>(
    root: &'a Path,
    git: &'a FakeGitHost,
    clock: &'a FixedClock,
    config: PolicyConfig,
) -> Controller<'a> {
    Controller::new(
        root,
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        VerificationConfig::default(),
        git,
        clock,
    )
    .with_policy(config)
}

fn profile() -> RuntimeProfile {
    RuntimeProfile::new("lead-high", "test/model", None)
}

fn approval_required_for_recovery() -> PolicyConfig {
    PolicyConfig {
        enabled: true,
        require_approval_for: vec!["recover_execution".to_string()],
    }
}

fn admit(controller: &Controller<'_>) -> String {
    controller
        .admit_user_task(MISSION_SESSION, TASK)
        .expect("admit Mission")
        .task_id
}

// -- Reconciler integration ---------------------------------------------------

#[test]
fn allow_executes_exactly_one_consequential_side_effect() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(dir.path(), &git, &clock, PolicyConfig::default());
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());

    let mut saw_allow = false;
    let mut applied = false;
    for _ in 0..7 {
        let result = reconciler.reconcile_mission(&id);
        if let Some(summary) = &result.policy {
            if summary.decision == "allow" {
                saw_allow = true;
            }
        }
        if result.result == ReconcileOutcome::Applied {
            applied = true;
        }
    }
    assert!(applied, "the recovery must converge");
    assert!(
        saw_allow,
        "the executed action must record an Allow decision"
    );
    drop(reconciler);
    assert_eq!(
        runtime.state.borrow().created.len(),
        1,
        "exactly one create side effect"
    );
}

#[test]
fn require_approval_executes_zero_side_effects_and_is_durable() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(dir.path(), &git, &clock, approval_required_for_recovery());
    let id = admit(&controller);
    let execution = controller
        .load_mission(&id)
        .unwrap()
        .unwrap()
        .runtime_execution_id()
        .unwrap();
    let expected_id = approval_id(&id, 1, PolicyAction::RecoverExecution, Some(&execution));
    let mut runtime = FakeRuntime::default();
    let state = runtime.state.clone();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());

    let first = reconciler.reconcile_mission(&id);
    assert_eq!(first.result, ReconcileOutcome::AwaitingApproval);
    let summary = first.policy.as_ref().expect("policy summary");
    assert_eq!(summary.decision, "require_approval");
    assert_eq!(summary.rule, "approval");
    assert_eq!(summary.reason_code, "approval_required");
    assert_eq!(summary.action, "recover_execution");
    assert_eq!(summary.approval_id.as_deref(), Some(expected_id.as_str()));
    assert!(!summary.required_facts.is_empty());
    assert_eq!(state.borrow().created.len(), 0);

    // The pending approval is durable and idempotent.
    let record = load_approval(dir.path(), &expected_id)
        .unwrap()
        .expect("pending approval persisted");
    assert_eq!(record.status, ApprovalStatus::Pending);
    assert_eq!(record.mission_id, id);
    assert_eq!(record.generation, 1);
    assert_eq!(record.action, "recover_execution");

    // The Mission receipt records the admission decision.
    let mission = mission::load(dir.path(), &id).unwrap().unwrap();
    let receipt = mission.reconcile.last_receipt.as_ref().unwrap();
    let policy = receipt.policy.as_ref().unwrap();
    assert_eq!(policy.decision, "require_approval");
    assert_eq!(policy.approval_id.as_deref(), Some(expected_id.as_str()));

    // A second tick re-observes the same pending approval and still does no work.
    let second = reconciler.reconcile_mission(&id);
    assert_eq!(second.result, ReconcileOutcome::AwaitingApproval);
    assert_eq!(
        second.policy.as_ref().unwrap().approval_id.as_deref(),
        Some(expected_id.as_str())
    );
    assert_eq!(state.borrow().created.len(), 0);
    let listed = list_approvals(dir.path());
    assert_eq!(
        listed
            .approvals
            .iter()
            .filter(|record| record.approval_id == expected_id)
            .count(),
        1,
        "repeated ticks never duplicate an approval"
    );

    // Granting it admits the action; the same durable state now converges.
    resolve_approval(
        dir.path(),
        &expected_id,
        ApprovalStatus::Approved,
        None,
        101,
    )
    .unwrap();
    let mut applied = false;
    for _ in 0..7 {
        let result = reconciler.reconcile_mission(&id);
        assert_ne!(result.result, ReconcileOutcome::AwaitingApproval);
        if result.result == ReconcileOutcome::Applied {
            applied = true;
        }
    }
    assert!(applied, "an approved action proceeds");
    drop(reconciler);
    assert_eq!(runtime.state.borrow().created.len(), 1);
}

#[test]
fn rejected_approval_denies_with_zero_side_effects() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(dir.path(), &git, &clock, approval_required_for_recovery());
    let id = admit(&controller);
    let execution = controller
        .load_mission(&id)
        .unwrap()
        .unwrap()
        .runtime_execution_id()
        .unwrap();
    let aid = approval_id(&id, 1, PolicyAction::RecoverExecution, Some(&execution));
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());

    assert_eq!(
        reconciler.reconcile_mission(&id).result,
        ReconcileOutcome::AwaitingApproval
    );
    resolve_approval(dir.path(), &aid, ApprovalStatus::Rejected, None, 101).unwrap();

    let denied = reconciler.reconcile_mission(&id);
    assert_eq!(denied.result, ReconcileOutcome::Denied);
    let summary = denied.policy.as_ref().expect("policy summary");
    assert_eq!(summary.decision, "deny");
    assert_eq!(summary.reason_code, "approval_rejected");
    drop(reconciler);
    assert_eq!(runtime.state.borrow().created.len(), 0);
}

#[test]
fn a_fresh_unavailable_resource_defers_with_zero_side_effects() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(dir.path(), &git, &clock, PolicyConfig::default());
    let id = admit(&controller);

    let identity = associated_identity();
    let mut registry = ResourceRegistry::new(100);
    registry.observe_health(
        &identity,
        HealthFacts {
            state: ResourceHealth::Unavailable,
            reason: Some("seeded runtime outage".to_string()),
            provenance: ResourceProvenance::RuntimeObserved,
            observed_at: Some(100),
        },
        100,
    );
    ocg::resources::save(dir.path(), &registry).unwrap();

    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let result = reconciler.reconcile_mission(&id);
    assert_eq!(result.result, ReconcileOutcome::Deferred);
    let summary = result.policy.as_ref().expect("policy summary");
    assert_eq!(summary.decision, "defer");
    assert_eq!(summary.rule, "resource.availability");
    assert_eq!(summary.reason_code, "resource_unavailable");
    drop(reconciler);
    assert_eq!(runtime.state.borrow().created.len(), 0);
}

#[test]
fn a_durable_create_intent_retry_is_not_deferred_by_observation() {
    // A same-generation retry with a durable create intent must not be converted
    // into a policy defer just because the fresh observation is not an
    // authoritative absence. This preserves the existing convergence path.
    let mut ctx = context(PolicyAction::RecoverExecution);
    ctx.observation = ObservationStatus::ObservationFailed;
    ctx.reconcile_status = mission::MissionReconcileStatus::Failed;
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Allow);
    assert_eq!(assessment.rule, "policy.default_allow");

    // Without a durable create intent the same non-authoritative observation
    // must defer.
    ctx.reconcile_status = mission::MissionReconcileStatus::Idle;
    let assessment = evaluate(&ctx);
    assert_eq!(assessment.decision, PolicyDecision::Defer);
    assert_eq!(assessment.rule, "observation.authoritative_absence");
}

#[test]
fn policy_disabled_preserves_existing_behavior() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        PolicyConfig {
            enabled: false,
            require_approval_for: vec!["recover_execution".to_string()],
        },
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let mut applied = false;
    for _ in 0..7 {
        let result = reconciler.reconcile_mission(&id);
        assert!(
            result.policy.is_none(),
            "a disabled Policy records no admission decision"
        );
        if result.result == ReconcileOutcome::Applied {
            applied = true;
        }
    }
    assert!(applied);
    drop(reconciler);
    assert_eq!(runtime.state.borrow().created.len(), 1);
}

// -- approval durability, staleness and secrets -------------------------------

fn pending_request(mission_id: &str) -> ApprovalRequest {
    let execution = RuntimeExecutionId::new(MISSION_SESSION);
    ApprovalRequest {
        approval_id: approval_id(
            mission_id,
            1,
            PolicyAction::RecoverExecution,
            Some(&execution),
        ),
        mission_id: mission_id.to_string(),
        generation: 1,
        action: PolicyAction::RecoverExecution,
        current_execution_id: Some(MISSION_SESSION.to_string()),
        requested_at: 5,
    }
}

#[test]
fn approval_persistence_round_trips_and_rejects_corruption() {
    let dir = TestDir::new();
    let request = pending_request("task-1");
    let record = ensure_pending(dir.path(), &request).unwrap();
    assert_eq!(record.status, ApprovalStatus::Pending);

    // Idempotent: an existing record (any status) is returned unchanged.
    resolve_approval(
        dir.path(),
        &request.approval_id,
        ApprovalStatus::Approved,
        None,
        6,
    )
    .unwrap();
    let again = ensure_pending(dir.path(), &request).unwrap();
    assert_eq!(again.status, ApprovalStatus::Approved);

    // Resolving a different generation must not silently inherit this approval.
    assert!(again.authorizes("task-1", 1, PolicyAction::RecoverExecution));
    assert!(!again.authorizes("task-1", 2, PolicyAction::RecoverExecution));
    assert!(!again.authorizes("task-1", 1, PolicyAction::EnsureExecution));

    // Corruption of the durable authority is explicit, never a silent reset.
    fs::write(ocg::orchestration::state_path(dir.path()), "{ not json").unwrap();
    assert!(load_approval(dir.path(), &request.approval_id).is_err());
    let listed = list_approvals(dir.path());
    assert!(listed.approvals.is_empty());
    assert!(!listed.issues.is_empty(), "corruption is surfaced");
}

#[test]
fn approval_notes_are_bounded_and_secret_redacted() {
    let dir = TestDir::new();
    let request = pending_request("task-1");
    ensure_pending(dir.path(), &request).unwrap();
    let secret = format!("api_key = {SK_LIKE}");
    resolve_approval(
        dir.path(),
        &request.approval_id,
        ApprovalStatus::Approved,
        Some(secret.clone()),
        7,
    )
    .unwrap();

    let path = approval_path(dir.path(), &request.approval_id).unwrap();
    let text = fs::read_to_string(&path).unwrap();
    assert!(!text.contains(SK_LIKE));
    let record = load_approval(dir.path(), &request.approval_id)
        .unwrap()
        .unwrap();
    let note = record.note.as_deref().unwrap_or_default();
    assert!(!note.contains(SK_LIKE));
    assert!(!note.is_empty());
}

// -- CLI surface --------------------------------------------------------------

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ocg"))
        .current_dir(cwd)
        .env("OCG_USER_CONFIG", work.join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE_BIN")
        .args(args)
        .output()
        .expect("run ocg")
}

fn init_project(dir: &TestDir) -> PathBuf {
    let project = dir.project();
    // Policy/approvals are non-interactive surfaces: they need a Profile and
    // never bootstrap one.
    write_yaml(&project.join(".ocg.yaml"), &project_profile());
    project
}

#[test]
fn policy_cli_reports_the_effective_default_without_writing() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let output = run(&project, dir.path(), &["policy", "--json"]);
    assert!(output.status.success(), "ocg policy --json failed");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["enabled"], json!(true));
    assert_eq!(value["require_approval_for"], json!([]));
    assert!(value["missions"].as_array().unwrap().is_empty());
    assert!(value["fingerprint"].as_str().unwrap().len() >= 16);

    // Read-only: no approvals or missions were created.
    assert!(!ocg::orchestration::policy::approval_dir(&project).exists());
}

#[test]
fn approvals_cli_lists_and_resolves_durably() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let request = pending_request("task-cli");
    ensure_pending(&project, &request).unwrap();

    let listed = run(&project, dir.path(), &["approvals", "--json"]);
    assert!(listed.status.success());
    let value: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let records = value["approvals"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["approval_id"], json!(request.approval_id));
    assert_eq!(records[0]["status"], json!("pending"));

    let secret = format!("api_key = {SK_LIKE}");
    let approved = run(
        &project,
        dir.path(),
        &[
            "approve",
            request.approval_id.as_str(),
            "--note",
            secret.as_str(),
        ],
    );
    assert!(approved.status.success(), "ocg approve failed");
    let stored = load_approval(&project, &request.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, ApprovalStatus::Approved);
    assert!(!stored.note.as_deref().unwrap_or_default().contains("sk-"));

    let json = run(&project, dir.path(), &["approvals", "--json"]);
    assert!(json.status.success());
    let text = String::from_utf8_lossy(&json.stdout);
    assert!(!text.contains(SK_LIKE));

    let rejected = run(
        &project,
        dir.path(),
        &[
            "reject",
            request.approval_id.as_str(),
            "--note",
            "operator decided",
        ],
    );
    assert!(rejected.status.success());
    let stored = load_approval(&project, &request.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, ApprovalStatus::Rejected);
}

#[test]
fn approval_cli_rejects_unknown_options_and_missing_ids() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    for args in [
        vec!["approvals", "--nope"],
        vec!["policy", "--nope"],
        vec!["approve"],
        vec!["approve", "apr-does-not-exist"],
        vec!["reject", "apr-does-not-exist"],
    ] {
        let output = run(&project, dir.path(), &args);
        assert!(!output.status.success(), "expected failure for {args:?}");
    }
}
