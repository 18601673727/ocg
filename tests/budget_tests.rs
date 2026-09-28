//! Focused tests for the mandatory Mission monetary budget and quota admission.
//!
//! These tests prove the economic safety boundary end to end:
//!
//! - a configured hard Mission budget survives restart and settles exactly once;
//! - an unknown cost or quota is never treated as free/unlimited;
//! - `policy.enabled = false` cannot bypass a configured hard cap;
//! - an ordinary approval cannot authorize spending past a hard cap;
//! - `ResourceHealth::Available` is not a quota fact;
//! - the only way past a cap is to change the hard budget itself.
//!
//! They deliberately never assert resource ranking, selection or failover —
//! that is future Placement work.

mod common;

use common::{project_profile, write_yaml, TestDir};
use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::orchestration::budget::{
    BudgetConfig, BudgetOrigin, BudgetStatus, CostBasis, Money, QuotaFacts, SpendAction,
    SpendDecision, REASON_CURRENCY,
};
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::context_governor::{ContextObservation, TelemetryProvenance};
use ocg::orchestration::controller::Controller;
use ocg::orchestration::mission::{self, Mission, MissionRolloverStatus};
use ocg::orchestration::policy::{resolve_approval, ApprovalStatus, PolicyConfig};
use ocg::orchestration::reconcile::{ReconcileOutcome, Reconciler};
use ocg::process::FakeGitHost;
use ocg::resources::{ResourceIdentity, ResourceRegistry};
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContextEvent, RuntimeContextObservation,
    RuntimeContinuation, RuntimeError, RuntimeErrorKind, RuntimeExecution, RuntimeExecutionId,
    RuntimeIdentity, RuntimeProfile, RuntimeRecoveryKey, RuntimeResult,
};
use ocg::verification::config::VerificationConfig;
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::rc::Rc;

const TASK: &str = "budget the mission";
const SOURCE_SESSION: &str = "ses_source";
const TARGET_SESSION: &str = "ses_target";

// -- fakes --------------------------------------------------------------------

#[derive(Default)]
struct FakeState {
    executions: HashSet<String>,
    created: Vec<String>,
    prepared: Vec<String>,
    staged: Vec<String>,
    resumed: Vec<String>,
    fail_resume: bool,
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

    fn resolve_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        Ok(RuntimeExecutionId::new(SOURCE_SESSION))
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
        Ok(None)
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
        self.state
            .borrow_mut()
            .prepared
            .push(execution_id.as_str().to_string());
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
        _execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        self.state.borrow_mut().staged.push(continuation.id.clone());
        Ok(())
    }

    fn resume_runtime_continuation(
        &self,
        _execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        let mut state = self.state.borrow_mut();
        if state.fail_resume {
            return Err(RuntimeError::new(
                RuntimeErrorKind::Transport,
                "fake resume failure",
            ));
        }
        if !state.resumed.contains(&continuation.id) {
            state.resumed.push(continuation.id.clone());
        }
        Ok(())
    }
}

fn controller<'a>(
    root: &'a Path,
    git: &'a FakeGitHost,
    clock: &'a FixedClock,
    budget: BudgetConfig,
    policy: PolicyConfig,
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
    .with_policy(policy)
    .with_budget(budget)
}

fn profile() -> RuntimeProfile {
    RuntimeProfile::new("lead-high", "test/model", None)
}

fn configured_budget(limit: i64, estimate: Option<i64>) -> BudgetConfig {
    BudgetConfig {
        currency: Some("USD".to_string()),
        hard_limit_micros: Some(limit),
        estimated_operation_cost_micros: estimate,
        require_quota: false,
    }
}

fn admit(controller: &Controller<'_>) -> String {
    controller
        .admit_user_task(SOURCE_SESSION, TASK)
        .expect("admit Mission")
        .task_id
}

fn observation(safe_boundary: bool, event_id: &str) -> ContextObservation {
    ContextObservation {
        session_id: SOURCE_SESSION.to_string(),
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

fn associated_identity() -> ResourceIdentity {
    ResourceIdentity::for_model("test", "model").with_runtime_family("fake", "reconcile-test")
}

// -- durable accounting -------------------------------------------------------

#[test]
fn a_configured_budget_materializes_once_and_settles_exactly_once() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(10);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(1_000_000, Some(100_000)),
        PolicyConfig::default(),
    );
    let id = admit(&controller);

    let (assessment, _) = controller
        .admit_mandatory_spend(
            &id,
            SpendAction::ResumeContinuation,
            "op-1",
            QuotaFacts::unknown(),
            11,
        )
        .expect("admit spend");
    assert!(assessment.is_allowed());
    let reservation_id = assessment.reservation_id.clone().expect("reservation id");

    // A reload (a restart) sees the durable reservation.
    let reloaded = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(
        reloaded.budget.hard_limit,
        Some(Money::new(1_000_000, "USD"))
    );
    assert_eq!(reloaded.budget.origin, BudgetOrigin::SystemDefault);
    assert_eq!(reloaded.budget.reserved.micros, 100_000);
    assert_eq!(reloaded.budget.status, BudgetStatus::Active);

    assert!(controller
        .settle_mandatory_spend(&id, &reservation_id, None, 12)
        .unwrap());
    // Settling twice is a no-op, never a double count.
    assert!(!controller
        .settle_mandatory_spend(&id, &reservation_id, None, 13)
        .unwrap());
    let settled = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(settled.budget.settled.micros, 100_000);
    assert_eq!(settled.budget.reserved.micros, 0);
    assert_eq!(settled.budget.status, BudgetStatus::Active);

    // Re-admitting the exact same operation reuses the settled reservation and
    // never double-counts.
    let (replay, _) = controller
        .admit_mandatory_spend(
            &id,
            SpendAction::ResumeContinuation,
            "op-1",
            QuotaFacts::unknown(),
            14,
        )
        .expect("re-admit spend");
    assert!(replay.is_allowed());
    assert_eq!(
        replay.reservation_id.as_deref(),
        Some(reservation_id.as_str())
    );
    let after = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(after.budget.settled.micros, 100_000);
    assert_eq!(after.budget.reserved.micros, 0);
}

#[test]
fn a_second_reservation_past_the_cap_is_denied_without_double_counting() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(10);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(150_000, Some(100_000)),
        PolicyConfig::default(),
    );
    let id = admit(&controller);

    let (first, _) = controller
        .admit_mandatory_spend(
            &id,
            SpendAction::ResumeContinuation,
            "op-1",
            QuotaFacts::unknown(),
            11,
        )
        .unwrap();
    assert!(first.is_allowed());

    // A distinct, separately identified provider-costly operation cannot fit.
    let (second, _) = controller
        .admit_mandatory_spend(
            &id,
            SpendAction::ResumeContinuation,
            "op-2",
            QuotaFacts::unknown(),
            12,
        )
        .unwrap();
    assert_eq!(second.decision, SpendDecision::Deny);
    assert_eq!(second.reason_code, "mission_hard_budget_exceeded");
    assert!(second.reservation_id.is_none());

    let reloaded = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(
        reloaded.budget.reservations.len(),
        1,
        "a denied spend records no reservation"
    );
    assert_eq!(reloaded.budget.reserved.micros, 100_000);
}

#[test]
fn an_uncertain_dispatch_keeps_its_reservation() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(10);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(1_000_000, Some(100_000)),
        PolicyConfig::default(),
    );
    let id = admit(&controller);
    let (assessment, _) = controller
        .admit_mandatory_spend(
            &id,
            SpendAction::ResumeContinuation,
            "op-1",
            QuotaFacts::unknown(),
            11,
        )
        .unwrap();
    let reservation_id = assessment.reservation_id.unwrap();
    assert!(controller
        .mark_mandatory_spend_unresolved(&id, &reservation_id, 12)
        .unwrap());

    let reloaded = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(reloaded.budget.reserved.micros, 100_000);
    assert_eq!(reloaded.budget.unresolved.micros, 100_000);
    assert_eq!(reloaded.budget.status, BudgetStatus::Active);
}

// -- reconcile boundary -------------------------------------------------------

fn drive(reconciler: &mut Reconciler<'_, '_>, id: &str, ticks: usize) -> ReconcileOutcome {
    let mut last = ReconcileOutcome::Noop;
    for _ in 0..ticks {
        last = reconciler.reconcile_mission(id).result;
    }
    last
}

#[test]
fn an_unconfigured_budget_never_blocks_a_resume() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        BudgetConfig::default(),
        PolicyConfig::default(),
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let last = drive(&mut reconciler, &id, 8);
    assert_eq!(last, ReconcileOutcome::Noop);
    drop(reconciler);
    assert_eq!(runtime.state.borrow().resumed.len(), 1);
}

#[test]
fn unknown_cost_defers_the_resume_with_zero_provider_calls() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(1_000_000, None),
        PolicyConfig::default(),
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let mut last = None;
    for _ in 0..8 {
        last = Some(reconciler.reconcile_mission(&id));
    }
    let last = last.unwrap();
    assert_eq!(last.result, ReconcileOutcome::Deferred);
    assert_eq!(
        last.budget
            .as_ref()
            .and_then(|budget| budget.reason.clone()),
        Some("mission_cost_unknown".to_string()),
        "an unknown cost must defer, never default to free"
    );
    drop(reconciler);
    assert!(
        runtime.state.borrow().resumed.is_empty(),
        "an unknown cost must perform no provider-costly resume"
    );
}

#[test]
fn a_hard_cap_blocks_the_resume_even_when_policy_is_disabled() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(50, Some(100)),
        PolicyConfig {
            enabled: false,
            require_approval_for: vec![],
        },
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let mut last = None;
    for _ in 0..8 {
        last = Some(reconciler.reconcile_mission(&id));
    }
    let last = last.unwrap();
    assert_eq!(last.result, ReconcileOutcome::Deferred);
    assert!(
        last.policy.is_none(),
        "a disabled Policy records no admission decision"
    );
    assert_eq!(
        last.budget
            .as_ref()
            .and_then(|budget| budget.reason.clone()),
        Some("mission_hard_budget_exceeded".to_string()),
        "the mandatory economic receipt must still record the hard-cap denial"
    );
    drop(reconciler);
    assert!(
        runtime.state.borrow().resumed.is_empty(),
        "policy.enabled=false must not bypass a hard cap"
    );

    // The durable reconcile receipt carries the mandatory budget projection.
    let receipt = mission::load(dir.path(), &id)
        .unwrap()
        .unwrap()
        .reconcile
        .last_receipt
        .expect("durable receipt");
    assert!(receipt.policy.is_none());
    let budget = receipt.budget.expect("durable economic receipt");
    assert_eq!(budget.hard_limit_micros, Some(50));
    assert_eq!(budget.origin, "system_default");
}

#[test]
fn an_approved_approval_cannot_authorize_spending_past_a_hard_cap() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(50, Some(100)),
        PolicyConfig {
            enabled: true,
            require_approval_for: vec!["continue_execution".to_string()],
        },
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());

    // The continuation needs an approval bound to the exact current execution.
    // The first request predates the target binding, so the test grants every
    // approval the control plane asks for until the action reaches the budget
    // gate.
    let mut approved = 0usize;
    let mut saw_allow = false;
    let mut last = None;
    for _ in 0..12 {
        let result = reconciler.reconcile_mission(&id);
        if let Some(policy) = &result.policy {
            if policy.decision == "allow" {
                saw_allow = true;
            }
            if policy.decision == "require_approval" {
                if let Some(approval_id) = &policy.approval_id {
                    resolve_approval(dir.path(), approval_id, ApprovalStatus::Approved, None, 101)
                        .unwrap();
                    approved += 1;
                    continue;
                }
            }
        }
        last = Some(result);
    }
    let last = last.unwrap();
    assert!(approved >= 1, "the continuation asked for an approval");
    assert!(saw_allow, "an approved action passes Policy");
    assert_eq!(last.result, ReconcileOutcome::Deferred);
    assert_eq!(
        last.budget
            .as_ref()
            .and_then(|budget| budget.reason.clone()),
        Some("mission_hard_budget_exceeded".to_string())
    );
    drop(reconciler);
    assert!(
        runtime.state.borrow().resumed.is_empty(),
        "an approval must never authorize exceeding a hard cap"
    );
}

#[test]
fn raising_the_hard_budget_is_the_only_way_past_a_cap() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(50, Some(100)),
        PolicyConfig::default(),
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();

    {
        let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
        for _ in 0..8 {
            reconciler.reconcile_mission(&id);
        }
    }
    assert!(runtime.state.borrow().resumed.is_empty());

    // The only supported way past the cap: explicitly change the hard budget.
    let mut mission = mission::load(dir.path(), &id).unwrap().unwrap();
    let expected_revision = mission.revision;
    let expected_owner = mission.session_id.clone();
    assert!(mission
        .set_hard_budget(Money::new(1_000_000, "USD"), 200)
        .unwrap());
    assert_eq!(mission.budget.origin, BudgetOrigin::ExplicitUserLimit);
    assert!(mission::save_if_revision(
        dir.path(),
        &mission,
        expected_revision,
        expected_owner.as_deref(),
    )
    .unwrap());

    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let last = drive(&mut reconciler, &id, 4);
    assert_eq!(last, ReconcileOutcome::Noop);
    drop(reconciler);
    assert_eq!(
        runtime.state.borrow().resumed.len(),
        1,
        "the explicitly raised budget admits the continuation"
    );
    let after = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(after.budget.settled.micros, 100);
}

#[test]
fn a_configured_budget_in_another_currency_is_denied_not_silently_uncapped() {
    // A Mission that already accounts in one currency must never fall back to
    // an uncapped admission when the configured budget is expressed in another
    // currency. OCG performs no FX conversion, so the action is denied.
    let mut mission = Mission::admit("task-currency-conflict-01", TASK, SOURCE_SESSION, 1);
    // Simulate durable accounting in USD with no enforceable limit (the state a
    // currency conflict leaves behind).
    mission.budget.currency = "USD".to_string();
    assert!(mission.budget.hard_limit.is_none());

    let eur = BudgetConfig {
        currency: Some("EUR".to_string()),
        hard_limit_micros: Some(1_000_000),
        estimated_operation_cost_micros: Some(1_000),
        require_quota: false,
    };
    let assessment = mission.admit_spend(
        &eur,
        SpendAction::ResumeContinuation,
        "op-1",
        CostBasis::Unknown,
        QuotaFacts::unknown(),
        10,
    );
    assert_eq!(assessment.decision, SpendDecision::Deny);
    assert_eq!(assessment.reason_code, REASON_CURRENCY);
    assert!(assessment.amount.is_none());
    assert!(mission.budget.reservations.is_empty());
}

#[test]
fn an_unknown_quota_defers_even_when_health_is_available() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        BudgetConfig {
            require_quota: true,
            ..configured_budget(1_000_000, Some(100_000))
        },
        PolicyConfig::default(),
    );
    let id = admit(&controller);

    // Reachability is not a quota fact. `ResourceHealth::Available` must not
    // satisfy the required quota check.
    let mut registry = ResourceRegistry::new(100);
    registry.observe_available(&associated_identity(), "reachable", 100);
    ocg::resources::save(dir.path(), &registry).unwrap();

    let mut runtime = FakeRuntime::default();
    let mut reconciler = Reconciler::new(&controller, &mut runtime, profile());
    let mut last = None;
    for _ in 0..8 {
        last = Some(reconciler.reconcile_mission(&id));
    }
    let last = last.unwrap();
    assert_eq!(last.result, ReconcileOutcome::Deferred);
    assert_eq!(
        last.budget
            .as_ref()
            .and_then(|budget| budget.reason.clone()),
        Some("mission_quota_unknown".to_string())
    );
    drop(reconciler);
    assert!(
        runtime.state.borrow().resumed.is_empty(),
        "an unknown quota must defer, never assume unlimited capacity"
    );
}

// -- rollover boundary --------------------------------------------------------

#[test]
fn the_rollover_resume_is_gated_by_the_hard_cap_and_stays_recoverable() {
    let dir = TestDir::new();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let controller = controller(
        dir.path(),
        &git,
        &clock,
        configured_budget(50, Some(100)),
        PolicyConfig::default(),
    );
    let id = admit(&controller);
    let mut runtime = FakeRuntime::default();

    let result = controller
        .observe_context(
            SOURCE_SESSION,
            observation(true, "evt-hard-cap"),
            &mut runtime,
            &profile(),
        )
        .expect("observe context");
    assert!(
        !result.decision.rollover_allowed,
        "the economic admission refuses the provider-costly resume"
    );
    assert!(
        result
            .note
            .as_deref()
            .unwrap_or_default()
            .contains("hard Mission budget"),
        "the refusal names the hard budget: {:?}",
        result.note
    );
    assert_eq!(
        result.rollover_status,
        Some(MissionRolloverStatus::Active),
        "the cutover leaves a recoverable rollover, not a silent loss"
    );
    assert!(
        runtime.state.borrow().resumed.is_empty(),
        "no provider-costly resume runs past the cap"
    );

    // The target was created and staged (local/transport work), but the resume
    // never happened, and the Mission remains recoverable once the budget is
    // explicitly raised.
    assert_eq!(runtime.state.borrow().created.len(), 1);
    assert_eq!(runtime.state.borrow().staged.len(), 1);
    let mission = mission::load(dir.path(), &id).unwrap().unwrap();
    assert_eq!(mission.session_id.as_deref(), Some(TARGET_SESSION));
    assert_ne!(mission.rollover.status, MissionRolloverStatus::Applied);
}

// -- CLI ----------------------------------------------------------------------

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    // A budget CLI surface still needs a structurally valid OCG Profile; the
    // test project is created explicitly by `init_project`.
    let profile = cwd.join(".ocg.yaml");
    if !profile.is_file() {
        write_yaml(&profile, &project_profile());
    }
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

fn init_project(dir: &TestDir, yaml: &str) -> PathBuf {
    let project = dir.project();
    // A project always needs a structurally valid OCG Profile; the caller's
    // YAML carries only the section under test.
    let mut config = project_profile();
    if let Ok(extra) = serde_yaml_ng::from_str::<serde_json::Value>(yaml) {
        if let Some(object) = extra.as_object() {
            for (key, value) in object {
                config[key] = value.clone();
            }
        }
    }
    write_yaml(&project.join(".ocg.yaml"), &config);
    project
}

#[test]
fn budget_cli_reports_the_effective_default_without_writing() {
    let dir = TestDir::new();
    let project = init_project(&dir, "---\n{}\n");
    let output = run(&project, dir.path(), &["budget", "--json"]);
    assert!(output.status.success(), "ocg budget --json failed");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["configured"], json!(false));
    assert_eq!(value["hard_limit_micros"], serde_json::Value::Null);
    assert!(value["missions"].as_array().unwrap().is_empty());
    assert!(value["fingerprint"].as_str().unwrap().len() >= 16);

    // Read-only: no mission store was created.
    assert!(!project.join(".ocg/orchestration/missions").exists());
}

#[test]
fn budget_cli_set_is_the_only_supported_way_to_change_a_hard_cap() {
    let dir = TestDir::new();
    let project = init_project(&dir, "---\n{}\n");
    let mission = Mission::admit("task-cli-budget-0001", TASK, SOURCE_SESSION, 1);
    let mission_id = mission.mission_id.clone();
    mission::save(&project, &mission).unwrap();

    let set = run(
        &project,
        dir.path(),
        &[
            "budget",
            "set",
            "--mission",
            mission_id.as_str(),
            "--limit",
            "500000",
            "--currency",
            "USD",
            "--json",
        ],
    );
    assert!(
        set.status.success(),
        "ocg budget set failed: {}",
        String::from_utf8_lossy(&set.stderr)
    );
    let set_value: serde_json::Value = serde_json::from_slice(&set.stdout).unwrap();
    assert_eq!(set_value["hard_limit_micros"], json!(500000));
    assert_eq!(set_value["origin"], json!("explicit_user_limit"));

    // The change is durable and inspectable.
    let read = run(&project, dir.path(), &["budget", "--json"]);
    assert!(read.status.success());
    let read_value: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    let missions = read_value["missions"].as_array().unwrap();
    assert_eq!(missions.len(), 1);
    assert_eq!(missions[0]["mission_id"], json!(mission_id));
    assert_eq!(missions[0]["hard_limit_micros"], json!(500000));
    assert_eq!(missions[0]["currency"], json!("USD"));

    // A contradictory currency is refused (no FX conversion).
    let mismatch = run(
        &project,
        dir.path(),
        &[
            "budget",
            "set",
            "--mission",
            mission_id.as_str(),
            "--limit",
            "600000",
            "--currency",
            "EUR",
        ],
    );
    assert!(!mismatch.status.success(), "currency mismatch must fail");

    // Unknown options are refused.
    assert!(!run(&project, dir.path(), &["budget", "--nope"])
        .status
        .success());
    assert!(!run(
        &project,
        dir.path(),
        &[
            "budget",
            "set",
            "--mission",
            mission_id.as_str(),
            "--limit",
            "600000",
        ],
    )
    .status
    .success());
    assert!(!run(
        &project,
        dir.path(),
        &[
            "budget",
            "set",
            "--mission",
            "task-does-not-exist",
            "--limit",
            "600000",
            "--currency",
            "USD",
        ],
    )
    .status
    .success());
}

#[test]
fn budget_cli_surfaces_a_configured_default_cap() {
    let dir = TestDir::new();
    let project = init_project(
        &dir,
        "---\nbudget:\n  currency: USD\n  hardLimitMicros: 1000000\n  estimatedOperationCostMicros: 100000\n",
    );
    let output = run(&project, dir.path(), &["budget", "--json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["configured"], json!(true));
    assert_eq!(value["currency"], json!("USD"));
    assert_eq!(value["hard_limit_micros"], json!(1000000));
    assert_eq!(value["estimated_operation_cost_micros"], json!(100000));
}

#[test]
fn budget_config_requires_a_currency_and_rejects_unknown_values() {
    let dir = TestDir::new();
    // A limit without a currency must be rejected by whole-config validation.
    let project = init_project(&dir, "---\nbudget:\n  hardLimitMicros: 1000000\n");
    let output = run(&project, dir.path(), &["budget", "--json"]);
    assert!(!output.status.success());

    let project = init_project(
        &dir,
        "---\nbudget:\n  currency: \"us$\"\n  hardLimitMicros: 1000000\n",
    );
    let output = run(&project, dir.path(), &["budget", "--json"]);
    assert!(!output.status.success());
}
