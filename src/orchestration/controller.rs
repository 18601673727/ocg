//! The Rust orchestration controller.
//!
//! This is where every orchestration decision is made. The generated JavaScript
//! adapter is intentionally inert: it only carries bytes between OpenCode and
//! this controller through `ocg __bridge`. Context ranking, projection, policy,
//! freshness, retry budgets, checkpointing and telemetry all live here.
//!
//! ```text
//! chat.message      -> prepare_lead_context   (V1 persisted dynamic context suffix)
//! session.prompt    -> admit_user_task        (V2 genuine prompt admission: task identity)
//! session.context   -> prepare_model_context  (V2 model-dispatch repository baseline)
//! task before       -> prepare_handoff        (typed role projection)
//! task after        -> consume_explore_result (bounded findings + checkpoint)
//!                   -> after_build            (verification + retry/debug policy)
//! ```
//!
//! Invariants:
//!
//! - No arbitrary shell. Verification uses the existing trusted, structured
//!   `verification` configuration and the shared [`CaptureRunner`].
//! - Fail-soft state. A corrupt state, checkpoint or cache is skipped and
//!   reported, never fatal, and never silently reused.
//! - No learned router. The only inputs are explicit config and deterministic
//!   local evidence.

use crate::capabilities::{CapabilityConfig, CapabilityEvidence, CapabilityPlan};
use crate::clock::Clock;
use crate::context::capsule::{CapsuleFile, Finding as CapsuleFinding, TaskCapsule};
use crate::context::config::ContextConfig;
use crate::context::engine::{ContextEngine, PlanOutcome};
use crate::context::freshness::{Provenance, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::gitdiff::{snapshot_fingerprint, GitSnapshot};
use crate::error::{OcgError, Result};
use crate::orchestration::budget::BudgetConfig;
use crate::orchestration::checkpoint::{self, Checkpoint, Phase};
use crate::orchestration::config::OrchestrationConfig;
use crate::orchestration::context_governor::{
    self, ContextObservation, GovernorAction, GovernorDecision, GovernorState,
};
use crate::orchestration::domain::{CanonicalAdmission, DomainRepository};
use crate::orchestration::handoff::{
    HandoffFinding, HandoffVerification, ModelHandoffCapsule, ProjectionInput, Role, Severity,
};
use crate::orchestration::policy::PolicyConfig;
use crate::orchestration::projection::{self, ProjectionLimits};
use crate::orchestration::state::{self, Attempts, OrchestrationPhase, SessionState};
use crate::process::{CaptureRunner, GitHost};
use crate::runtime::lifecycle::{RuntimeAdapter, RuntimeExecutionId, RuntimeProfile};
use crate::telemetry::OrchestrationMetrics;
use crate::verification::config::VerificationConfig;
use crate::verification::distill;
use crate::verification::result::VerificationReport;
use crate::verification::runner::{execute, VerifyRequest};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// A compact, machine-followable response contract appended to Explore
/// hand-offs. It is advisory: [`parse_explore_output`] still falls back to a
/// deterministic line parser when the model ignores it.
pub const EXPLORE_RESPONSE_CONTRACT: &str = "\nExplore response contract (advisory): end your reply with one JSON object and no prose after it:\n{\"goal\":\"...\",\"constraints\":[\"...\"],\"findings\":[{\"summary\":\"...\",\"source\":\"path\",\"severity\":\"info|warning|critical\"}],\"files\":[\"path\"],\"symbols\":[\"name\"]}\n";

/// The dynamic context prepared for an ordinary user message.
///
/// `snapshot_id` is the deterministic identity of the *session repository
/// baseline*. It is derived from indexed repository content (repo root,
/// engine/schema version and file fingerprints), not from the current task or
/// ranked projection. The bridge uses it to decide whether the full baseline
/// has already been injected in this session. The metadata fields are
/// conservative estimates for presentation only, never provider billing.
#[derive(Debug, Clone)]
pub struct LeadContext {
    pub session_id: String,
    pub task_id: String,
    pub dynamic_context: String,
    pub snapshot_id: String,
    /// Estimated tokens for the injected dynamic context
    /// (`dynamic_context.len() / 4`). Clearly an estimate; never presented as
    /// exact provider tokens.
    pub estimated_tokens: usize,
    /// Byte length of the injected dynamic context.
    pub bytes: usize,
    /// Number of relevant files in the prepared input.
    pub file_count: usize,
    /// Number of relevant symbols in the prepared input.
    pub symbol_count: usize,
    pub goal: Option<String>,
    pub metrics: OrchestrationMetrics,
    /// Whether the baseline body was reused from the session store rather than
    /// freshly rendered. The V1 persisted-prompt path suppresses re-injection
    /// when this is set (the baseline already lives in the persisted history);
    /// the V2 model-dispatch path ignores it for inclusion and still returns
    /// the full body, because nothing is persisted there.
    pub cached: bool,
}

/// The rendered session repository baseline for one turn.
///
/// `identity` is what the session deduplicates on: the task-independent
/// repository generation when a reliable signal exists, otherwise the
/// projection identity of the rendered body (a safe degrade that still
/// deduplicates identical renderings).
struct RenderedBaseline {
    dynamic_context: String,
    identity: String,
    bytes: usize,
    file_count: usize,
    symbol_count: usize,
    goal: Option<String>,
    rich_bytes: usize,
    metrics: OrchestrationMetrics,
}

impl RenderedBaseline {
    /// The baseline is suppressed because its identity was already injected in
    /// this session: no context, no presentation metadata, no accounting.
    fn cached(identity: String) -> Self {
        Self {
            dynamic_context: String::new(),
            identity,
            bytes: 0,
            file_count: 0,
            symbol_count: 0,
            goal: None,
            rich_bytes: 0,
            metrics: OrchestrationMetrics {
                phase: Some(OrchestrationPhase::Idle.as_str().to_string()),
                source: Some(Role::Lead.as_str().to_string()),
                destination: Some(Role::Lead.as_str().to_string()),
                model_dynamic_context_bytes: 0,
                ..OrchestrationMetrics::default()
            },
        }
    }
}

/// The result of admitting a genuinely submitted user prompt.
#[derive(Debug, Clone)]
pub struct TaskAdmission {
    pub session_id: String,
    pub task_id: String,
    /// Whether this admission changed the session's task identity. `false`
    /// means the running task was re-admitted (or merely backfilled) and all
    /// task-scoped state was preserved.
    pub changed: bool,
}

/// A role hand-off projected for a delegation.
#[derive(Debug, Clone)]
pub struct HandoffOutcome {
    pub session_id: String,
    pub task_id: String,
    pub source: Role,
    pub destination: Role,
    pub agent: Option<String>,
    pub capsule: ModelHandoffCapsule,
    pub dynamic_context: String,
    /// A conservative, explicitly advisory capability view. This is never
    /// applied to OpenCode's agent permissions in this version.
    pub advisory_permissions: Value,
    pub stale: bool,
    pub stale_reasons: Vec<String>,
    pub metrics: OrchestrationMetrics,
}

/// The bounded result of consuming an Explore output.
#[derive(Debug, Clone)]
pub struct ExploreDigest {
    pub session_id: String,
    pub task_id: String,
    pub structured: bool,
    pub findings: Vec<HandoffFinding>,
    pub locations: Vec<crate::verification::distill::SourceLocation>,
    pub checkpoint_id: Option<String>,
    pub metrics: OrchestrationMetrics,
}

/// What the controller decided after Build verification.
#[derive(Debug, Clone)]
pub enum BuildDecision {
    /// Verification passed. No Debug is recommended.
    Passed {
        report: VerificationReport,
        verification: HandoffVerification,
    },
    /// Verification failed and a bounded Build retry is allowed.
    RetryBuild {
        attempt: usize,
        report: VerificationReport,
        verification: HandoffVerification,
    },
    /// The Build retry budget is exhausted: Debug is recommended.
    Debug {
        reason: String,
        report: VerificationReport,
        handoff: Box<HandoffOutcome>,
    },
    /// No trusted verification command was configured; nothing was run.
    NotConfigured { note: String },
}

/// The full Build outcome, including the checkpoint, the Build→Verify hand-off
/// capsule and telemetry.
#[derive(Debug, Clone)]
pub struct BuildOutcome {
    pub session_id: String,
    pub task_id: String,
    pub stage: String,
    pub decision: BuildDecision,
    pub checkpoint_id: Option<String>,
    /// The typed Build→Verify projection. `None` only when no verification ran.
    pub verify_handoff: Option<ModelHandoffCapsule>,
    pub metrics: OrchestrationMetrics,
}

/// The result of one context-pressure observation and any same-Mission
/// rollover that was durably attempted.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContextGovernanceResult {
    pub observation: ContextObservation,
    pub decision: GovernorDecision,
    /// The Mission-side lifecycle state, when a rollover artifact exists.
    pub rollover_status: Option<String>,
    /// The artifact state, useful to diagnostics and recovery callers.
    pub artifact_status: Option<String>,
    pub artifact_id: Option<String>,
    pub source_session_id: Option<String>,
    pub target_session_id: Option<String>,
    /// A bounded, non-sensitive explanation of a deferred/failed/conflict
    /// outcome. It is never a provider transcript.
    pub note: Option<String>,
}

/// The controller for one project root.
pub struct Controller<'a> {
    root: PathBuf,
    config: OrchestrationConfig,
    context: ContextConfig,
    capabilities: CapabilityConfig,
    verification: VerificationConfig,
    policy: PolicyConfig,
    /// The mandatory economic configuration. It is not gated by
    /// `policy.enabled`: a configured hard Mission budget is always enforced.
    budget: BudgetConfig,
    git: &'a dyn GitHost,
    clock: &'a dyn Clock,
}

impl<'a> Controller<'a> {
    /// Admit the session prompt into the SQLite canonical execution domain.
    /// The legacy Mission snapshot remains a compatibility/context projection;
    /// it is not consulted for Job, Attempt, Executor, or Call authority.
    pub fn admit_canonical_job(
        &self,
        binding_key: &str,
        payload: &str,
        executor_kind: &str,
    ) -> Result<CanonicalAdmission> {
        let mut repository = DomainRepository::open(&self.root)?;
        let project = repository.ensure_project(&self.root)?;
        let admission = repository.admit_job(project, binding_key, payload, executor_kind)?;
        Ok(admission)
    }

    pub fn canonical_authority(
        &self,
        binding_key: &str,
    ) -> Result<Option<crate::orchestration::domain::AttemptAuthority>> {
        let repository = DomainRepository::open(&self.root)?;
        let project = repository.ensure_project(&self.root)?;
        repository.authority_for_binding(&project.id, binding_key)
    }

    pub fn new(
        root: impl Into<PathBuf>,
        config: OrchestrationConfig,
        context: ContextConfig,
        capabilities: CapabilityConfig,
        verification: VerificationConfig,
        git: &'a dyn GitHost,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            root: root.into(),
            config,
            context,
            capabilities,
            verification,
            policy: PolicyConfig::default(),
            budget: BudgetConfig::default(),
            git,
            clock,
        }
    }

    /// Attach an explicit Policy configuration. The default preserves current
    /// behavior exactly (Policy enabled, no action requiring approval).
    pub fn with_policy(mut self, policy: PolicyConfig) -> Self {
        self.policy = policy;
        self
    }

    /// Attach the mandatory economic configuration. There is no `enabled`
    /// switch: an unconfigured budget simply has no cap, and a configured one
    /// is always enforced independently of Policy.
    pub fn with_budget(mut self, budget: BudgetConfig) -> Self {
        self.budget = budget;
        self
    }

    pub fn policy(&self) -> &PolicyConfig {
        &self.policy
    }

    pub fn budget(&self) -> &BudgetConfig {
        &self.budget
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_current_execution(&self, execution_id: &RuntimeExecutionId) -> Result<bool> {
        Ok(self.canonical_authority(execution_id.as_str())?.is_some())
    }

    pub fn config(&self) -> &OrchestrationConfig {
        &self.config
    }

    pub fn verification(&self) -> &VerificationConfig {
        &self.verification
    }

    /// The injected clock, so trusted verification inside the canonical lane
    /// uses the same deterministic time source as the rest of the controller.
    pub fn clock(&self) -> &dyn Clock {
        self.clock
    }

    pub fn load_state(&self) -> state::LoadedState {
        state::load(&self.root)
    }

    fn persist(
        &self,
        loaded: &mut state::LoadedState,
        session: SessionState,
        now: i64,
    ) -> Result<()> {
        loaded.state.upsert(session, now);
        state::save(&self.root, &loaded.state)?;
        Ok(())
    }

    pub fn observe_context(
        &self,
        session_id: &str,
        observation: ContextObservation,
        _runtime: &mut dyn RuntimeAdapter,
        _profile: &RuntimeProfile,
    ) -> Result<ContextGovernanceResult> {
        let mut decision = self.evaluate_context(session_id, observation.clone())?;
        decision.rollover_allowed = false;
        Ok(ContextGovernanceResult {
            observation,
            decision,
            rollover_status: None,
            artifact_status: None,
            artifact_id: None,
            source_session_id: Some(session_id.to_string()),
            target_session_id: None,
            note: Some("execution replacement requires a canonical Attempt command".into()),
        })
    }

    /// Convenience policy-only observation for callers that do not have a
    /// runtime lifecycle client. It records telemetry and returns the decision
    /// without creating a session.
    pub fn evaluate_context(
        &self,
        session_id: &str,
        observation: ContextObservation,
    ) -> Result<GovernorDecision> {
        if !self.config.context_governor.enabled {
            return Ok(GovernorDecision {
                state: GovernorState::Disabled,
                action: GovernorAction::Continue,
                utilization_percent: None,
                rollover_allowed: false,
                deferred_for_boundary: false,
                reason: "context governor is disabled".to_string(),
            });
        }
        let now = self.now();
        let mut observation = observation;
        let session_id = state::safe_id(session_id);
        if observation.session_id.is_empty() {
            observation.session_id = session_id.clone();
        }
        if observation.event_id.is_empty() {
            observation.event_id = context_governor::event_identity(
                &session_id,
                observation.assistant_message_id.as_deref(),
                observation.finish.as_deref(),
                Some(now),
            );
        }
        context_governor::save_observation(&self.root, &observation)?;
        Ok(context_governor::decide(
            &self.config.context_governor,
            &observation,
        ))
    }

    /// The controller clock's current instant. Exposed so the bridge records a
    /// deterministic timestamp under a fixed clock in tests.
    pub fn now_unix(&self) -> i64 {
        self.clock.now_unix()
    }

    fn now(&self) -> i64 {
        self.clock.now_unix()
    }

    fn engine(&self) -> ContextEngine<'a> {
        ContextEngine::new(
            self.root.clone(),
            self.context.clone(),
            self.git,
            self.clock,
        )
        .with_capabilities(self.capabilities.clone())
        .with_verification(self.verification.clone())
    }

    fn context_enabled(&self) -> bool {
        self.config.enabled && self.context.enabled
    }

    /// The deterministic identity of the current indexed repository content.
    ///
    /// It is independent of the current task, ranked files or symbols. It
    /// changes only when the repo root, engine/schema version, truncation state
    /// or indexed file contents change. Returns `None` when context is disabled
    /// or when the generation signal cannot be produced, in which case the
    /// caller should fall back to the projection-identity path.
    fn repository_generation_id(&self) -> Result<Option<String>> {
        if !self.context_enabled() {
            return Ok(None);
        }
        match self.engine().prepare() {
            Ok(prepared) => Ok(Some(prepared.index_report.generation_id)),
            Err(_) => Ok(None),
        }
    }

    /// A stable, non-reversible task id. Raw task text is never part of it.
    pub fn task_id(task: &str) -> String {
        crate::telemetry::Event::hashed_task_id(&format!("orchestration|{task}"))
    }

    /// The task representation persisted in state and checkpoints. It is bounded
    /// and passed through the existing secret detector: a secret-shaped task is
    /// redacted entirely, an ordinary task is stored verbatim (bounded). The
    /// in-memory prompt is still used for ranking; only the persisted copy is
    /// sanitized.
    pub fn stored_task_text(task: &str) -> String {
        let bounded: String = task.chars().take(4096).collect();
        crate::telemetry::task::redact(&bounded)
    }

    fn plan(&self, task: &str, role: Option<&str>) -> (Option<PlanOutcome>, Vec<String>) {
        if !self.context_enabled() {
            return (None, Vec::new());
        }
        match self.engine().plan(task, role) {
            Ok(outcome) => {
                let warnings = outcome.warnings.clone();
                (Some(outcome), warnings)
            }
            Err(error) => (
                None,
                vec![format!("context preparation was skipped: {error}")],
            ),
        }
    }

    /// Build the rich projection input from a plan plus bounded session state.
    fn build_input(
        &self,
        session: &SessionState,
        plan: Option<&PlanOutcome>,
        task: &str,
    ) -> ProjectionInput {
        let mut input = ProjectionInput {
            task: task.to_string(),
            ..ProjectionInput::default()
        };
        if let Some(outcome) = plan {
            let context_plan = &outcome.plan;
            if let Some(capsule) = &context_plan.capsule {
                input.goal = capsule.goal.clone();
                input.constraints = capsule.constraints.clone();
                input.findings = capsule
                    .findings
                    .iter()
                    .map(|finding| HandoffFinding {
                        summary: finding.summary.clone(),
                        detail: finding.detail.clone(),
                        source: finding.source.clone(),
                        severity: Severity::Info,
                    })
                    .collect();
                input.files = capsule.files.clone();
                input.symbols = capsule.symbols.clone();
                input.decisions = capsule
                    .decisions
                    .iter()
                    .map(|decision| decision.decision.clone())
                    .collect();
            }
            input.slices = context_plan.slices.clone();
            input.diff_context = render_diff_context(context_plan);
            input.diff_ref = diff_reference(&input.diff_context);
            input.git = context_plan.git.clone();
            input.provenance = context_plan.provenance.clone();
        }
        // Explicit session state always wins over derived plan state.
        if session.goal.is_some() {
            input.goal = session.goal.clone();
        }
        if !session.constraints.is_empty() {
            input.constraints = session.constraints.clone();
        }
        input.findings.extend(session.findings.clone());
        for path in &session.files {
            if !input.files.iter().any(|file| &file.path == path) {
                input.files.push(CapsuleFile {
                    path: path.clone(),
                    reason: Some("explore".to_string()),
                    changed: true,
                });
            }
        }
        for name in &session.symbols {
            if !input.symbols.iter().any(|symbol| &symbol.name == name) {
                input.symbols.push(crate::context::symbols::SymbolRef {
                    path: session
                        .files
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string()),
                    name: name.clone(),
                    kind: crate::context::symbols::SymbolKind::Reference,
                    start_line: 1,
                    end_line: 1,
                });
            }
        }
        input.failures = session.failures.clone();
        input.evidence = session.evidence.clone();
        input.verification = session.last_verification.clone();
        input.raw_log_refs = session
            .last_verification
            .as_ref()
            .map(|verification| verification.raw_log_refs.clone())
            .unwrap_or_default();
        input
    }

    /// `chat.message`: prepare bounded, deterministic dynamic context for the
    /// Lead on an ordinary user message (the V1 persisted-prompt path).
    ///
    /// With [`Self::admit_user_task`] (the V2 prompt-admission path) this is
    /// one of the two places the overall task is defined or reset. Task
    /// identity is resolved through the durable Mission: a repeated message
    /// keeps the running session, a genuinely new message binds the session
    /// to a different Mission (a prior Mission is never destroyed by it), and
    /// a message whose Mission already exists — for example after a session
    /// rollover — seeds the fresh session from the Mission instead of
    /// restarting committed work. Delegated subagent prompts never reset it.
    pub fn prepare_lead_context(&self, session_id: &str, message: &str) -> Result<LeadContext> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let admission = self.admit_canonical_job(session_id, message, "lead")?;
        let task_id = admission.job.id;
        let mut loaded = state::load(&self.root);
        let mut session = loaded
            .state
            .session(&session_key)
            .cloned()
            .filter(|session| session.task_id == task_id)
            .unwrap_or_else(|| SessionState::new(&session_key, &task_id, now));
        session.session_id = session_key.clone();
        session.task_id = task_id.clone();
        session.phase = OrchestrationPhase::Idle;
        session.source = Role::Lead;
        session.destination = Some(Role::Lead);
        if session.task.is_none() {
            session.task = Some(Self::stored_task_text(message));
        }

        // Ensure the OCG gitignore exists before computing the repository
        // generation. `.gitignore` is an indexed file: a later append by the
        // index or cache writers would otherwise look like a repository change
        // and trigger an unnecessary baseline refresh.
        crate::runtime::install::ensure_gitignore(&self.root)?;

        // The session repository baseline is keyed by the repository
        // generation, a task-independent identity of the indexed content. A
        // task or ranking change therefore never re-appends the baseline;
        // only a real repository generation change does.
        let generation = self.repository_generation_id()?;
        let (baseline, cached) = match generation.as_deref() {
            Some(id) if session.repository_generation_id.as_deref() == Some(id) => {
                (RenderedBaseline::cached(id.to_string()), true)
            }
            _ => (
                self.render_session_baseline(&session, message, generation.as_deref()),
                false,
            ),
        };

        if !cached {
            session.repository_generation_id = Some(baseline.identity.clone());
            session.last_rich_bytes = baseline.rich_bytes;
        }
        session.updated_at = now;
        self.persist(&mut loaded, session, now)?;

        Ok(LeadContext {
            session_id: session_key,
            task_id,
            estimated_tokens: baseline.bytes / 4,
            bytes: baseline.bytes,
            dynamic_context: baseline.dynamic_context,
            snapshot_id: baseline.identity,
            file_count: baseline.file_count,
            symbol_count: baseline.symbol_count,
            goal: baseline.goal,
            metrics: baseline.metrics,
            cached,
        })
    }

    /// `session.prompt` (OpenCode V2 prompt admission): register a genuinely
    /// admitted user prompt as the session's current task.
    ///
    /// OpenCode runs prompt admission (`SessionPrompt.prepare`, the source of
    /// the `session.prompt` hook) only for a real user prompt. Synthetic
    /// user-role messages the runtime generates itself — interruption/resume
    /// continuations and similar — are admitted through `Session.synthetic`
    /// and never pass through prompt admission. Admission is therefore the
    /// *only* authoritative task boundary; model dispatch
    /// ([`Self::prepare_model_context`]) never performs this bookkeeping.
    ///
    /// A re-admitted prompt whose task id matches the running session leaves
    /// every piece of task-scoped state (findings, retry budgets, checkpoint
    /// references, phase, destination) exactly as the worker orchestration
    /// path left it. A re-delivered prompt therefore also never reopens a
    /// terminal Mission: a completed/failed/cancelled generation stays the
    /// durable record of how it ended, and new work belongs to a new
    /// generation. A genuinely new prompt resolves the durable Mission for
    /// its task id and seeds the session from it: a Mission admitted earlier
    /// — including by a session that has since died or rolled over — resumes
    /// with its committed progress; a terminal Mission starts a new
    /// generation. The repository baseline identity and retained body carry
    /// over: the baseline is keyed by the task-independent repository
    /// generation, never by task wording.
    pub fn admit_user_task(&self, session_id: &str, message: &str) -> Result<TaskAdmission> {
        let admission = self.admit_canonical_job(session_id, message, "lead")?;
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let task_id = admission.job.id;
        let mut loaded = state::load(&self.root);
        let previous = loaded.state.session(&session_key).cloned();
        let changed = previous
            .as_ref()
            .is_none_or(|session| session.task_id != task_id);
        let mut session = if changed {
            let mut session = SessionState::new(&session_key, &task_id, now);
            if let Some(previous) = previous {
                session.repository_generation_id = previous.repository_generation_id;
                session.repository_baseline = previous.repository_baseline;
            }
            session
        } else {
            previous.ok_or_else(|| OcgError::config("session projection disappeared"))?
        };
        session.task = Some(Self::stored_task_text(&admission.job.payload));
        self.persist(&mut loaded, session, now)?;
        Ok(TaskAdmission {
            session_id: session_key,
            task_id,
            changed,
        })
    }

    /// `session.context` (OpenCode V2): provide the current session repository
    /// baseline for one root-Lead model dispatch.
    ///
    /// The V2 adapter injects the baseline into the outgoing request's system
    /// context, which is never persisted, so — unlike the V1 persisted-prompt
    /// path — the baseline is *always* returned: the first dispatch, a repeated
    /// dispatch, a tool-driven continuation and the next user turn each receive
    /// exactly one copy. Baseline *computation* stays session/repository scoped
    /// and task-independent: when the repository generation is unchanged the
    /// retained body is reused verbatim and never re-derived from new task
    /// wording; only a material generation change re-renders it.
    ///
    /// Task identity is *not* decided here; it is owned by prompt admission
    /// ([`Self::admit_user_task`]). A dispatch-time user-role message may be a
    /// tool-loop continuation or a runtime-generated synthetic (an
    /// interruption/resume continuation reaches the model as `role: "user"`),
    /// so it is never a trustworthy task signal: this path serves the baseline
    /// from whatever session state admission established and never resets
    /// findings, retry budgets, checkpoint references, phase or destination.
    /// A dispatch ahead of any admission (no task yet) is served from a
    /// task-less session, fail-soft.
    pub fn prepare_model_context(&self, session_id: &str) -> Result<LeadContext> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let mut loaded = state::load(&self.root);
        let existing = loaded.state.session(&session_key).cloned();
        // A dispatch without a prior admission has no task; serve it from a
        // task-less session so the baseline is still delivered.
        let mut session = existing.unwrap_or_else(|| SessionState::new(&session_key, "", now));
        // The task text used for rendering comes from admission state, never
        // from dispatch-time conversation content.
        let task = session.task.clone().unwrap_or_default();

        // Ensure the OCG gitignore exists before computing the repository
        // generation. `.gitignore` is an indexed file: a later append by the
        // index or cache writers would otherwise look like a repository change
        // and trigger an unnecessary baseline refresh.
        crate::runtime::install::ensure_gitignore(&self.root)?;

        // The session repository baseline is keyed by the repository
        // generation, a task-independent identity of the indexed content.
        // Baseline *reuse* is separate from baseline *inclusion*: an unchanged
        // generation reuses the retained body (no re-render), but the body is
        // still returned for this dispatch, because the V2 adapter injects it
        // into an ephemeral per-request system context rather than persisted
        // history.
        let generation = self.repository_generation_id()?;
        if let (Some(identity), Some(stored)) =
            (generation.as_deref(), session.repository_baseline.clone())
        {
            if session.repository_generation_id.as_deref() == Some(identity)
                && !stored.body.is_empty()
            {
                let bytes = stored.body.len();
                return Ok(LeadContext {
                    session_id: session_key,
                    task_id: session.task_id.clone(),
                    dynamic_context: stored.body,
                    snapshot_id: identity.to_string(),
                    estimated_tokens: bytes / 4,
                    bytes,
                    file_count: stored.file_count,
                    symbol_count: stored.symbol_count,
                    goal: session.goal.clone(),
                    metrics: OrchestrationMetrics {
                        phase: Some(OrchestrationPhase::Idle.as_str().to_string()),
                        source: Some(Role::Lead.as_str().to_string()),
                        destination: Some(Role::Lead.as_str().to_string()),
                        // The reused body is still supplied to the model on
                        // this dispatch, so the bytes are counted, not zeroed.
                        model_dynamic_context_bytes: bytes as u64,
                        ..OrchestrationMetrics::default()
                    },
                    cached: true,
                });
            }
        }

        // No retained body for the current generation (first dispatch, a
        // material repository change, or no reliable generation signal):
        // render the baseline exactly as the V1 path would and retain it.
        let baseline = self.render_session_baseline(&session, &task, generation.as_deref());
        session.repository_generation_id = Some(baseline.identity.clone());
        session.repository_baseline = Some(state::RepositoryBaseline {
            body: baseline.dynamic_context.clone(),
            file_count: baseline.file_count,
            symbol_count: baseline.symbol_count,
        });
        session.last_rich_bytes = baseline.rich_bytes;
        session.updated_at = now;
        loaded.state.upsert(session.clone(), now);
        let _ = state::save(&self.root, &loaded.state);

        Ok(LeadContext {
            session_id: session_key,
            task_id: session.task_id.clone(),
            dynamic_context: baseline.dynamic_context,
            snapshot_id: baseline.identity,
            estimated_tokens: baseline.bytes / 4,
            bytes: baseline.bytes,
            file_count: baseline.file_count,
            symbol_count: baseline.symbol_count,
            goal: baseline.goal,
            metrics: baseline.metrics,
            cached: false,
        })
    }

    /// Render the full session repository baseline for `message`.
    ///
    /// `generation` is the task-independent repository identity when a reliable
    /// signal exists. Without one (context disabled or preparation failed) the
    /// identity falls back to the projection identity of the rendered body:
    /// identical renderings still deduplicate, while a different task may
    /// re-inject, which is the safe degrade when no generation signal exists.
    fn render_session_baseline(
        &self,
        session: &SessionState,
        message: &str,
        generation: Option<&str>,
    ) -> RenderedBaseline {
        let (plan, warnings) = self.plan(message, Some("lead"));
        let input = self.build_input(session, plan.as_ref(), message);
        let (input, omitted) = projection::sanitize(&input);
        let file_count = input.files.len();
        let symbol_count = input.symbols.len();
        let rich_bytes = input.rich_bytes();
        // The repository snapshot excludes the current user message so the
        // same effective repository context yields the same projection body;
        // the baseline identity is the repository generation, which is
        // independent of the task.
        let mut snapshot = self.render_lead_snapshot(&input);
        snapshot.push_str(&render_warnings(&warnings));
        snapshot.push_str(&render_warnings(&omitted));
        snapshot.push_str(&self.render_source_slices(&input.slices));
        let identity = generation
            .map(str::to_string)
            .unwrap_or_else(|| snapshot_identity(&snapshot));
        let mut dynamic = self.render_lead_header(&input);
        dynamic.push_str(&snapshot);
        let bytes = dynamic.len();

        let metrics = OrchestrationMetrics {
            phase: Some(OrchestrationPhase::Idle.as_str().to_string()),
            source: Some(Role::Lead.as_str().to_string()),
            destination: Some(Role::Lead.as_str().to_string()),
            rich_capsule_bytes: rich_bytes as u64,
            handoff_capsule_bytes: 0,
            selected_source_bytes: input.selected_source_bytes() as u64,
            diff_context_bytes: input.diff_context_bytes() as u64,
            verification_context_bytes: input.verification_context_bytes() as u64,
            model_dynamic_context_bytes: bytes as u64,
            cache_hits: usize::from(
                plan.as_ref()
                    .map(|outcome| outcome.from_cache)
                    .unwrap_or(false),
            ),
            index_hits: plan
                .as_ref()
                .map(|outcome| outcome.index_report.metrics.reused)
                .unwrap_or(0),
            ..OrchestrationMetrics::default()
        };

        RenderedBaseline {
            dynamic_context: dynamic,
            identity,
            bytes,
            file_count,
            symbol_count,
            goal: input.goal,
            rich_bytes,
            metrics,
        }
    }

    /// `tool.execute.before` for `task`: project the typed role hand-off.
    ///
    /// The overall task identity comes from the session that `chat.message`
    /// established; the delegated prompt is used only for the context plan and
    /// the capsule's `task` field. A different delegated prompt must **not**
    /// reset findings or retry counters. A `Debug` delegation uses the narrow
    /// debug projection and never appends selected source.
    pub fn prepare_handoff(
        &self,
        session_id: &str,
        role: Role,
        task: &str,
    ) -> Result<HandoffOutcome> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let mut loaded = state::load(&self.root);
        let delegated_task_id = Self::task_id(task);
        let mut session = match loaded.state.session(&session_key).cloned() {
            Some(session) => session,
            None => {
                let mut fresh = SessionState::new(&session_key, &delegated_task_id, now);
                fresh.task = Some(Self::stored_task_text(task));
                fresh
            }
        };
        if session.task_id.is_empty() {
            session.task_id = delegated_task_id;
        }
        if session.task.is_none() {
            session.task = Some(Self::stored_task_text(task));
        }
        let overall_task_id = session.task_id.clone();

        let previous = session.source;
        session.phase = phase_for(role);
        session.destination = Some(role);
        session.last_transition = Some(projection::transition_for(previous, role));

        let limits = ProjectionLimits::from_config(&self.config);
        let (stale, stale_reasons, _checkpoint_id) = self.checkpoint_freshness(&session);

        let rich_bytes;
        let verification_context_bytes;
        let mut selected_source_bytes = 0usize;
        let mut diff_context_bytes = 0usize;
        let mut cache_hits = 0usize;
        let mut index_hits = 0usize;
        let mut debug_attempt: Option<usize> = None;
        let mut capsule;
        let mut dynamic;
        let advisory;

        if role == Role::Debug {
            // Debug is a real budgeted transition. The first Debug delegation
            // consumes attempt 1; every delegation is counted, and once the
            // budget is exhausted the hand-off carries an explicit user
            // escalation instead of silently continuing.
            session.attempts.debug += 1;
            debug_attempt = Some(session.attempts.debug);
            // Narrow debug delegation: failures, evidence, verification and
            // raw-log references only. No repository plan, no source slices.
            let verification = session.last_verification.clone();
            let raw_log_refs = verification
                .as_ref()
                .map(|verification| verification.raw_log_refs.clone())
                .unwrap_or_default();
            let input = ProjectionInput {
                task: task.to_string(),
                failures: session.failures.clone(),
                evidence: session.evidence.clone(),
                diff_context: session.last_diff_context.clone(),
                diff_ref: diff_reference(&session.last_diff_context),
                raw_log_refs,
                verification,
                ..ProjectionInput::default()
            };
            let (clean, omitted) = projection::sanitize(&input);
            let rich_reference = session.last_rich_bytes.max(clean.rich_bytes());
            rich_bytes = rich_reference;
            verification_context_bytes = clean.verification_context_bytes();
            capsule = projection::project_with_rich(
                &clean,
                previous,
                role,
                &overall_task_id,
                &session_key,
                limits,
                rich_reference,
            );
            capsule.omitted.extend(omitted);
            if stale {
                capsule.omitted.push(format!(
                    "previous checkpoint is stale: {}",
                    stale_reasons.join("; ")
                ));
            }
            capsule.omitted.sort();
            capsule.omitted.dedup();
            advisory = json!({
                "advisory": true,
                "note": "debug delegation uses the narrow projection; OpenCode permissions remain authoritative",
            });
            dynamic = self.render_handoff_context(&capsule);
            dynamic.push_str(&render_advisory(&advisory));
            if let Some(escalation) = self.debug_escalation(session.attempts.debug) {
                capsule.omitted.push(escalation.clone());
                dynamic.push_str(&format!("\nescalation: {escalation}\n"));
            }
        } else {
            let (plan, warnings) = self.plan(task, Some(role.routing_role()));
            let input = self.build_input(&session, plan.as_ref(), task);
            let (input, omitted) = projection::sanitize(&input);
            rich_bytes = input.rich_bytes();
            selected_source_bytes = input.selected_source_bytes();
            diff_context_bytes = input.diff_context_bytes();
            verification_context_bytes = input.verification_context_bytes();
            cache_hits = usize::from(
                plan.as_ref()
                    .map(|outcome| outcome.from_cache)
                    .unwrap_or(false),
            );
            index_hits = plan
                .as_ref()
                .map(|outcome| outcome.index_report.metrics.reused)
                .unwrap_or(0);
            capsule = projection::project(
                &input,
                previous,
                role,
                &overall_task_id,
                &session_key,
                limits,
            );
            capsule.omitted.extend(omitted);
            if stale {
                capsule.omitted.push(format!(
                    "previous checkpoint is stale: {}",
                    stale_reasons.join("; ")
                ));
            }
            capsule.omitted.sort();
            capsule.omitted.dedup();

            advisory = self.advisory_permissions(task, &input);

            if session.goal.is_none() {
                session.goal = input.goal.clone();
            }
            if session.constraints.is_empty() {
                session.constraints = input.constraints.clone();
            }
            session.files = input
                .files
                .iter()
                .take(32)
                .map(|file| file.path.clone())
                .collect();
            session.symbols = input
                .symbols
                .iter()
                .take(32)
                .map(|symbol| symbol.name.clone())
                .collect();
            session.last_rich_bytes = input.rich_bytes();
            session.last_diff_context = input.diff_context.clone();

            dynamic = self.render_handoff_context(&capsule);
            dynamic.push_str(&render_warnings(&warnings));
            dynamic.push_str(&self.render_source_slices(&input.slices));
            if matches!(role, Role::Explore | Role::ExploreDeep) {
                dynamic.push_str(EXPLORE_RESPONSE_CONTRACT);
            }
            dynamic.push_str(&render_advisory(&advisory));
        }

        // Debug → Build is a real transition: when the previous owner was Debug
        // and the work returns to Build, checkpoint the hand-back with the
        // current session capsule and the last verification.
        if previous == Role::Debug && role == Role::Build {
            let checkpoint_id = self.save_checkpoint(
                Phase::DebugToBuild,
                self.session_capsule(&session),
                session.last_report.clone(),
            );
            if let Some(id) = &checkpoint_id {
                session.push_checkpoint(id);
            }
        }

        session.source = role;
        self.persist(&mut loaded, session, now)?;

        let mut metrics = OrchestrationMetrics {
            phase: Some(phase_for(role).as_str().to_string()),
            source: Some(previous.as_str().to_string()),
            destination: Some(role.as_str().to_string()),
            rich_capsule_bytes: rich_bytes as u64,
            handoff_capsule_bytes: capsule.measured_bytes() as u64,
            selected_source_bytes: selected_source_bytes as u64,
            diff_context_bytes: diff_context_bytes as u64,
            verification_context_bytes: verification_context_bytes as u64,
            model_dynamic_context_bytes: dynamic.len() as u64,
            cache_hits,
            index_hits,
            ..OrchestrationMetrics::default()
        };
        if let Some(attempt) = debug_attempt {
            metrics.attempt = Some(attempt);
            metrics.retry = Some(attempt.saturating_sub(1));
        }
        Ok(HandoffOutcome {
            session_id: session_key,
            task_id: overall_task_id,
            source: previous,
            destination: role,
            agent: role.agent(),
            capsule,
            dynamic_context: dynamic,
            advisory_permissions: advisory,
            stale,
            stale_reasons,
            metrics,
        })
    }

    /// `tool.execute.after` for an Explore task: consume the result into bounded
    /// typed findings and checkpoint Explore→Build.
    pub fn consume_explore_result(&self, session_id: &str, output: &str) -> Result<ExploreDigest> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let mut loaded = state::load(&self.root);
        let mut session = loaded
            .state
            .session(&session_key)
            .cloned()
            .unwrap_or_else(|| SessionState::new(&session_key, "", now));
        let parsed = parse_explore_output(output);
        for finding in &parsed.findings {
            session.push_finding(finding.clone());
        }
        if let Some(goal) = &parsed.goal {
            session.goal = Some(goal.clone());
        }
        for constraint in &parsed.constraints {
            if !session.constraints.contains(constraint) {
                session.constraints.push(constraint.clone());
            }
        }
        for path in &parsed.files {
            if !session.files.contains(path) {
                session.files.push(path.clone());
            }
        }
        for symbol in &parsed.symbols {
            if !session.symbols.contains(symbol) {
                session.symbols.push(symbol.clone());
            }
        }
        if !parsed.locations.is_empty() {
            let text = parsed
                .locations
                .iter()
                .map(distill::SourceLocation::display)
                .collect::<Vec<_>>()
                .join(", ");
            if !session.evidence.contains(&text) {
                session.evidence.push(text);
            }
        }
        session.phase = OrchestrationPhase::Build;

        let capsule = self.session_capsule(&session);
        let checkpoint_id = self.save_checkpoint(Phase::ExploreToBuild, capsule, None);
        if let Some(id) = &checkpoint_id {
            session.push_checkpoint(id);
        }

        let task_id = session.task_id.clone();
        self.persist(&mut loaded, session, now)?;

        let metrics = OrchestrationMetrics {
            phase: Some(OrchestrationPhase::Build.as_str().to_string()),
            source: Some(Role::Explore.as_str().to_string()),
            destination: Some(Role::Build.as_str().to_string()),
            handoff_capsule_bytes: 0,
            verification_outcome: None,
            ..OrchestrationMetrics::default()
        };
        Ok(ExploreDigest {
            session_id: session_key,
            task_id,
            structured: parsed.structured,
            findings: parsed.findings,
            locations: parsed.locations,
            checkpoint_id,
            metrics,
        })
    }

    /// Run trusted configured verification after Build and apply the retry /
    /// Debug policy.
    pub fn after_build(
        &self,
        session_id: &str,
        runner: &dyn CaptureRunner,
        stage: Option<&str>,
    ) -> Result<BuildOutcome> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let mut loaded = state::load(&self.root);
        let mut session = loaded
            .state
            .session(&session_key)
            .cloned()
            .unwrap_or_else(|| SessionState::new(&session_key, "", now));
        session.attempts.build += 1;
        let stage = stage
            .map(str::to_string)
            .unwrap_or_else(|| self.verification.default_stage.clone());

        if !self.verification.enabled || self.verification.command_count(&stage) == 0 {
            session.phase = OrchestrationPhase::Verify;
            let task_id = session.task_id.clone();
            let note = format!(
                "no trusted verification command is configured for stage '{stage}'; nothing was run"
            );
            self.persist(&mut loaded, session.clone(), now)?;
            return Ok(BuildOutcome {
                session_id: session_key,
                task_id,
                stage,
                decision: BuildDecision::NotConfigured { note },
                checkpoint_id: None,
                verify_handoff: None,
                metrics: OrchestrationMetrics {
                    phase: Some(OrchestrationPhase::Verify.as_str().to_string()),
                    source: Some(Role::Build.as_str().to_string()),
                    destination: Some(Role::Verify.as_str().to_string()),
                    attempt: Some(session.attempts.build),
                    ..OrchestrationMetrics::default()
                },
            });
        }

        let request = VerifyRequest {
            root: &self.root,
            config: &self.verification,
            stage: stage.clone(),
            runner,
            clock: self.clock,
            test_proposal: None,
        };
        let report = match execute(&request) {
            Ok(report) => report,
            Err(error) => {
                session.phase = OrchestrationPhase::Verify;
                let task_id = session.task_id.clone();
                let note = format!("verification could not run: {error}");
                self.persist(&mut loaded, session.clone(), now)?;
                return Ok(BuildOutcome {
                    session_id: session_key,
                    task_id,
                    stage,
                    decision: BuildDecision::NotConfigured { note },
                    checkpoint_id: None,
                    verify_handoff: None,
                    metrics: OrchestrationMetrics::default(),
                });
            }
        };

        let verification = handoff_verification(&report);
        session.last_verification = Some(verification.clone());
        session.last_report = Some(report.clone());
        // Refresh the context plan at the post-Build boundary so the Verify
        // hand-off reflects the current worktree (changed files, symbols, real
        // diff, freshness evidence) rather than the delegation-time plan.
        let session_task = session.task.clone().unwrap_or_default();
        let (fresh_plan, _fresh_warnings) = self.plan(&session_task, Some("verify"));
        if let Some(plan) = &fresh_plan {
            let fresh_diff = render_diff_context(&plan.plan);
            if !fresh_diff.is_empty() {
                session.last_diff_context = fresh_diff;
            }
        }
        let (verify_handoff, verify_rich_bytes) =
            self.verify_handoff_capsule(&session, &verification, fresh_plan.as_ref());
        record_verification_evidence(&mut session, &report);
        let capsule = self.session_capsule(&session);
        let checkpoint_id =
            self.save_checkpoint(Phase::BuildToVerify, capsule, Some(report.clone()));
        if let Some(id) = &checkpoint_id {
            session.push_checkpoint(id);
        }
        let task_id = session.task_id.clone();

        if report.passed() {
            session.attempts.verify += 1;
            session.phase = OrchestrationPhase::Done;
            self.persist(&mut loaded, session.clone(), now)?;
            return Ok(BuildOutcome {
                session_id: session_key,
                task_id,
                stage,
                decision: BuildDecision::Passed {
                    report,
                    verification,
                },
                checkpoint_id,
                verify_handoff: Some(verify_handoff.clone()),
                metrics: {
                    let mut metrics = orchestration_metrics(
                        OrchestrationPhase::Done,
                        Role::Build,
                        Role::Verify,
                        session.attempts,
                    );
                    metrics.rich_capsule_bytes = verify_rich_bytes as u64;
                    metrics.handoff_capsule_bytes = verify_handoff.measured_bytes() as u64;
                    metrics
                },
            });
        }

        session.attempts.verify += 1;
        if session.attempts.build <= self.config.max_build_retries {
            session.phase = OrchestrationPhase::Build;
            let attempt = session.attempts.build;
            self.persist(&mut loaded, session.clone(), now)?;
            return Ok(BuildOutcome {
                session_id: session_key,
                task_id,
                stage,
                decision: BuildDecision::RetryBuild {
                    attempt,
                    report,
                    verification,
                },
                checkpoint_id,
                verify_handoff: Some(verify_handoff.clone()),
                metrics: {
                    let mut metrics = orchestration_metrics(
                        OrchestrationPhase::Build,
                        Role::Build,
                        Role::Verify,
                        session.attempts,
                    );
                    metrics.rich_capsule_bytes = verify_rich_bytes as u64;
                    metrics.handoff_capsule_bytes = verify_handoff.measured_bytes() as u64;
                    metrics
                },
            });
        }

        // Budget exhausted: recommend Debug with an explainable reason.
        let reason = debug_reason(&stage, &session.attempts, &self.config, &report);
        session.debug_reason = Some(reason.clone());
        session.phase = OrchestrationPhase::Debug;
        let handoff = self.debug_handoff_from_session(&session, &session_key);
        let debug_checkpoint = self.save_checkpoint(
            Phase::VerifyToDebug,
            self.session_capsule(&session),
            Some(report.clone()),
        );
        if let Some(id) = &debug_checkpoint {
            session.push_checkpoint(id);
        }
        self.persist(&mut loaded, session.clone(), now)?;
        let mut metrics = orchestration_metrics(
            OrchestrationPhase::Debug,
            Role::Verify,
            Role::Debug,
            session.attempts,
        );
        metrics.rich_capsule_bytes = verify_rich_bytes as u64;
        metrics.handoff_capsule_bytes = verify_handoff.measured_bytes() as u64;
        metrics.debug_reason = Some(reason.clone());
        Ok(BuildOutcome {
            session_id: session_key,
            task_id,
            stage,
            decision: BuildDecision::Debug {
                reason,
                report,
                handoff: Box::new(handoff),
            },
            checkpoint_id: debug_checkpoint.or(checkpoint_id),
            verify_handoff: Some(verify_handoff.clone()),
            metrics,
        })
    }

    /// The user-escalation instruction once the Debug retry budget is exceeded.
    /// Shared by the public `debug_handoff` and the `prepare_handoff(Debug)`
    /// path so the policy cannot diverge.
    fn debug_escalation(&self, attempts: usize) -> Option<String> {
        if attempts > self.config.max_debug_retries {
            Some(format!(
                "user escalation required: debug retry budget exhausted ({attempts} attempt(s), max {}); ask the user for an explicit decision instead of continuing automatically",
                self.config.max_debug_retries
            ))
        } else {
            None
        }
    }

    /// Produce a Debug hand-off from the current session state. Delegates to the
    /// same builder and budget policy as `prepare_handoff(Role::Debug)`; kept as
    /// a public entry point for callers and tests.
    pub fn debug_handoff(&self, session_id: &str) -> Result<HandoffOutcome> {
        let now = self.now();
        let session_key = state::safe_id(session_id);
        let mut loaded = state::load(&self.root);
        let mut session = loaded
            .state
            .session(&session_key)
            .cloned()
            .unwrap_or_else(|| SessionState::new(&session_key, "", now));
        session.attempts.debug += 1;
        session.phase = OrchestrationPhase::Debug;
        let mut handoff = self.debug_handoff_from_session(&session, &session_key);
        if let Some(escalation) = self.debug_escalation(session.attempts.debug) {
            handoff.capsule.omitted.push(escalation.clone());
            handoff.stale_reasons.push(escalation.clone());
            handoff
                .dynamic_context
                .push_str(&format!("\nescalation: {escalation}\n"));
        }
        handoff.metrics.attempt = Some(session.attempts.debug);
        handoff.metrics.retry = Some(session.attempts.debug.saturating_sub(1));
        self.persist(&mut loaded, session, now)?;
        Ok(handoff)
    }

    fn debug_handoff_from_session(
        &self,
        session: &SessionState,
        session_key: &str,
    ) -> HandoffOutcome {
        let task = session.task.clone().unwrap_or_default();
        let limits = ProjectionLimits::from_config(&self.config);
        let verification = session.last_verification.clone();
        let raw_log_refs = verification
            .as_ref()
            .map(|verification| verification.raw_log_refs.clone())
            .unwrap_or_default();
        let input = ProjectionInput {
            task: task.clone(),
            failures: session.failures.clone(),
            evidence: session.evidence.clone(),
            diff_context: session.last_diff_context.clone(),
            diff_ref: diff_reference(&session.last_diff_context),
            raw_log_refs,
            verification,
            git: crate::context::gitdiff::GitState::default(),
            ..ProjectionInput::default()
        };
        let rich_reference = session.last_rich_bytes.max(input.rich_bytes());
        let capsule = projection::project_with_rich(
            &input,
            Role::Verify,
            Role::Debug,
            &session.task_id,
            session_key,
            limits,
            rich_reference,
        );
        let dynamic = self.render_handoff_context(&capsule);
        let metrics = OrchestrationMetrics {
            phase: Some(OrchestrationPhase::Debug.as_str().to_string()),
            source: Some(Role::Verify.as_str().to_string()),
            destination: Some(Role::Debug.as_str().to_string()),
            rich_capsule_bytes: rich_reference as u64,
            handoff_capsule_bytes: capsule.measured_bytes() as u64,
            model_dynamic_context_bytes: dynamic.len() as u64,
            verification_context_bytes: capsule
                .verification
                .as_ref()
                .and_then(|verification| serde_json::to_vec(verification).ok())
                .map(|bytes| bytes.len() as u64)
                .unwrap_or(0),
            debug_reason: session.debug_reason.clone(),
            ..OrchestrationMetrics::default()
        };
        HandoffOutcome {
            session_id: session_key.to_string(),
            task_id: session.task_id.clone(),
            source: Role::Verify,
            destination: Role::Debug,
            agent: Role::Debug.agent(),
            capsule,
            dynamic_context: dynamic,
            advisory_permissions: json!({"advisory": true}),
            stale: false,
            stale_reasons: Vec::new(),
            metrics,
        }
    }

    /// Project the typed Build→Verify hand-off for a completed build. It carries
    /// the goal, changed files, symbols, the real bounded diff and the distilled
    /// verification block (including failing locations).
    ///
    /// `fresh_plan` is a context plan recomputed at the post-Build boundary, so
    /// the hand-off reflects the current worktree rather than the state at
    /// delegation time.
    fn verify_handoff_capsule(
        &self,
        session: &SessionState,
        verification: &HandoffVerification,
        fresh_plan: Option<&PlanOutcome>,
    ) -> (ModelHandoffCapsule, usize) {
        let fallback_path = session
            .files
            .first()
            .cloned()
            .unwrap_or_else(|| "worktree".to_string());

        // Prefer the fresh plan's actual changed files and symbols.
        let changed_paths: Vec<String> = fresh_plan
            .map(|plan| plan.plan.changed_paths.clone())
            .filter(|paths| !paths.is_empty())
            .unwrap_or_else(|| session.files.clone());
        let symbols: Vec<crate::context::symbols::SymbolRef> = fresh_plan
            .and_then(|plan| plan.plan.capsule.as_ref())
            .map(|capsule| capsule.symbols.clone())
            .filter(|symbols| !symbols.is_empty())
            .unwrap_or_else(|| {
                session
                    .symbols
                    .iter()
                    .map(|name| crate::context::symbols::SymbolRef {
                        path: fallback_path.clone(),
                        name: name.clone(),
                        kind: crate::context::symbols::SymbolKind::Reference,
                        start_line: 1,
                        end_line: 1,
                    })
                    .collect()
            });
        let diff_context = fresh_plan
            .map(|plan| render_diff_context(&plan.plan))
            .unwrap_or_else(|| session.last_diff_context.clone());
        let git = fresh_plan
            .map(|plan| plan.plan.git.clone())
            .unwrap_or_default();

        let input = ProjectionInput {
            task: session.task.clone().unwrap_or_default(),
            goal: session.goal.clone(),
            constraints: session.constraints.clone(),
            files: changed_paths
                .iter()
                .map(|path| CapsuleFile {
                    path: path.clone(),
                    reason: Some("changed".to_string()),
                    changed: true,
                })
                .collect(),
            symbols,
            verification: Some(verification.clone()),
            evidence: session.evidence.clone(),
            diff_context: diff_context.clone(),
            diff_ref: diff_reference(&diff_context),
            git,
            ..ProjectionInput::default()
        };
        let rich_reference = session.last_rich_bytes.max(input.rich_bytes());
        let limits = ProjectionLimits::from_config(&self.config);
        let capsule = projection::project_with_rich(
            &input,
            Role::Build,
            Role::Verify,
            &session.task_id,
            &session.session_id,
            limits,
            rich_reference,
        );
        (capsule, rich_reference)
    }

    fn save_checkpoint(
        &self,
        phase: Phase,
        capsule: TaskCapsule,
        verification: Option<VerificationReport>,
    ) -> Option<String> {
        let snapshot = GitSnapshot::collect(&self.root, self.git);
        let fingerprint = snapshot_fingerprint(&snapshot);
        let provenance = Provenance {
            engine_version: ENGINE_VERSION.to_string(),
            schema_version: SCHEMA_VERSION,
            repo_id: capsule.provenance.repo_id.clone(),
            git_head: snapshot.state.head.clone(),
            git_dirty: snapshot.state.dirty,
            generated_at: self.now(),
            sources: capsule.provenance.sources.clone(),
            stale: false,
            validated: true,
            notes: Vec::new(),
        };
        let checkpoint = Checkpoint::build(
            phase,
            capsule,
            snapshot.state.clone(),
            fingerprint,
            verification,
            provenance,
            Vec::new(),
            self.now(),
        );
        let id = checkpoint.id.clone();
        match checkpoint.save(&self.root) {
            Ok(_) => Some(id),
            Err(error) => {
                eprintln!("ocg: warning: checkpoint was not saved: {error}");
                None
            }
        }
    }

    fn checkpoint_freshness(&self, session: &SessionState) -> (bool, Vec<String>, Option<String>) {
        let Some(id) = session.checkpoints.last() else {
            return (false, Vec::new(), None);
        };
        match checkpoint::load(&self.root, id, self.git) {
            Ok(loaded) if loaded.staleness.stale => {
                (true, loaded.staleness.reasons, Some(id.clone()))
            }
            Ok(_) => (false, Vec::new(), Some(id.clone())),
            Err(_) => (
                false,
                vec![format!("checkpoint {id} could not be read; it was ignored")],
                None,
            ),
        }
    }

    fn advisory_permissions(&self, task: &str, input: &ProjectionInput) -> Value {
        let evidence = CapabilityEvidence {
            changed_paths: input
                .files
                .iter()
                .filter(|file| file.changed)
                .map(|file| file.path.clone())
                .collect(),
            explicit: Vec::new(),
        };
        let plan = CapabilityPlan::plan_config(
            task,
            &evidence,
            &self.capabilities.custom,
            self.capabilities.enabled,
        );
        let allowed: Vec<String> = plan
            .capabilities
            .iter()
            .map(|entry| entry.capability.name())
            .collect();
        let denied: Vec<String> = plan
            .denied
            .iter()
            .map(|capability| capability.name())
            .collect();
        json!({
            "advisory": true,
            "note": "capability narrowing is advisory in this version; OpenCode agent permissions remain authoritative",
            "allowed": allowed,
            "denied": denied,
        })
    }

    /// The volatile header of the injected Lead context: the engine banner and
    /// the current user message. These are deliberately **excluded** from
    /// [`Self::render_lead_snapshot`] so the repository snapshot identity does
    /// not depend on the current message.
    fn render_lead_header(&self, input: &ProjectionInput) -> String {
        format!(
            "ocg orchestration context ({}):\ntask: {}\n",
            ENGINE_VERSION, input.task
        )
    }

    /// The repository-derived body of the Lead context. The current user
    /// message is not rendered here; it lives in [`Self::render_lead_header`].
    fn render_lead_snapshot(&self, input: &ProjectionInput) -> String {
        let mut out = String::new();
        if let Some(goal) = &input.goal {
            out.push_str(&format!("goal: {goal}\n"));
        }
        if !input.constraints.is_empty() {
            out.push_str("hard constraints:\n");
            for constraint in &input.constraints {
                out.push_str(&format!("- {constraint}\n"));
            }
        }
        if !input.files.is_empty() {
            out.push_str("relevant files:\n");
            for file in &input.files {
                let reason = file.reason.as_deref().unwrap_or("");
                out.push_str(&format!(
                    "- {}{}\n",
                    file.path,
                    if reason.is_empty() {
                        String::new()
                    } else {
                        format!(" ({reason})")
                    }
                ));
            }
        }
        if !input.symbols.is_empty() {
            out.push_str("relevant symbols:\n");
            for symbol in &input.symbols {
                out.push_str(&format!(
                    "- {} ({}:{})\n",
                    symbol.name, symbol.path, symbol.start_line
                ));
            }
        }
        if !input.findings.is_empty() {
            out.push_str("findings:\n");
            for finding in &input.findings {
                let severity = severity_label(finding.severity);
                out.push_str(&format!("- [{severity}] {}", finding.summary));
                if let Some(source) = &finding.source {
                    out.push_str(&format!(" ({source})"));
                }
                out.push('\n');
            }
        }
        out.push_str(&render_verification(input.verification.as_ref()));
        out
    }

    fn render_handoff_context(&self, capsule: &ModelHandoffCapsule) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "ocg role handoff: {} -> {} ({})\n",
            capsule.source.as_str(),
            capsule.destination.as_str(),
            capsule.transition.as_str()
        ));
        out.push_str(&format!("task: {}\n", capsule.task));
        if let Some(goal) = &capsule.goal {
            out.push_str(&format!("goal: {goal}\n"));
        }
        if !capsule.hard_constraints.is_empty() {
            out.push_str("hard constraints:\n");
            for constraint in &capsule.hard_constraints {
                out.push_str(&format!("- {constraint}\n"));
            }
        }
        if !capsule.files.is_empty() {
            out.push_str("files:\n");
            for file in &capsule.files {
                out.push_str(&format!(
                    "- {}{}\n",
                    file.path,
                    if file.changed { " (changed)" } else { "" }
                ));
            }
        }
        if !capsule.symbols.is_empty() {
            out.push_str("symbols:\n");
            for symbol in &capsule.symbols {
                out.push_str(&format!(
                    "- {} ({}:{})\n",
                    symbol.name, symbol.path, symbol.start_line
                ));
            }
        }
        if !capsule.findings.is_empty() {
            out.push_str("findings:\n");
            for finding in &capsule.findings {
                out.push_str(&format!(
                    "- [{}] {}",
                    severity_label(finding.severity),
                    finding.summary
                ));
                if let Some(source) = &finding.source {
                    out.push_str(&format!(" ({source})"));
                }
                out.push('\n');
            }
        }
        if !capsule.failures.is_empty() {
            out.push_str("failures:\n");
            for failure in &capsule.failures {
                out.push_str(&format!("- {failure}\n"));
            }
        }
        if !capsule.evidence.is_empty() {
            out.push_str("evidence:\n");
            for evidence in &capsule.evidence {
                out.push_str(&format!("- {evidence}\n"));
            }
        }
        out.push_str(&render_verification(capsule.verification.as_ref()));
        if let Some(diff_ref) = &capsule.diff_ref {
            out.push_str(&format!("diff: {diff_ref}\n"));
        }
        if let Some(diff_context) = &capsule.diff_context {
            out.push_str("diff context:\n");
            out.push_str(diff_context);
        }
        if !capsule.raw_log_refs.is_empty() {
            out.push_str("raw logs:\n");
            for reference in &capsule.raw_log_refs {
                out.push_str(&format!("- {reference}\n"));
            }
        }
        if !capsule.omitted.is_empty() {
            out.push_str("projection notes:\n");
            for note in &capsule.omitted {
                out.push_str(&format!("- {note}\n"));
            }
        }
        out
    }

    fn render_source_slices(&self, slices: &[crate::context::ranking::ContextSlice]) -> String {
        if slices.is_empty() {
            return String::new();
        }
        let mut out = String::new();
        let total: usize = slices.iter().map(|slice| slice.bytes).sum();
        out.push_str(&format!(
            "selected source ({} slice(s), {total} bytes):\n",
            slices.len()
        ));
        for slice in slices {
            out.push_str(&format!(
                "--- {}:{}-{}\n{}\n",
                slice.path, slice.start_line, slice.end_line, slice.content
            ));
        }
        out
    }

    /// Build the checkpoint capsule for a session and fingerprint every
    /// non-sensitive source it names, so a later `checkpoint::staleness` check is
    /// meaningful rather than vacuous.
    fn session_capsule(&self, session: &SessionState) -> TaskCapsule {
        let mut capsule = TaskCapsule::new(session.task.clone().unwrap_or_default());
        capsule.goal = session.goal.clone();
        capsule.constraints = session.constraints.clone();
        capsule.findings = session
            .findings
            .iter()
            .map(|finding| CapsuleFinding {
                summary: finding.summary.clone(),
                detail: finding.detail.clone(),
                source: finding.source.clone(),
            })
            .collect();
        capsule.files = session
            .files
            .iter()
            .map(|path| CapsuleFile {
                path: path.clone(),
                reason: Some("explore".to_string()),
                changed: true,
            })
            .collect();
        capsule.failures = session.failures.clone();
        let mut sources: Vec<crate::context::freshness::SourceFingerprint> = session
            .files
            .iter()
            .filter(|path| !crate::context::classify::classify(Path::new(path)).sensitive)
            .filter_map(|path| {
                let bytes = std::fs::read(self.root.join(path)).ok()?;
                Some(crate::context::freshness::SourceFingerprint {
                    path: path.clone(),
                    fingerprint: format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes)),
                    size: bytes.len() as u64,
                })
            })
            .collect();
        sources.sort_by(|a, b| a.path.cmp(&b.path));
        sources.dedup_by(|a, b| a.path == b.path);
        capsule.provenance.sources = sources;
        capsule.provenance.repo_id = String::new();
        capsule.recompute_size();
        capsule
    }
}

/// A deterministic identity for a rendered repository snapshot.
///
/// It hashes exactly the bytes of the message-independent snapshot text, so no
/// timestamp, secret or current user message can enter it. The same effective
/// repository context always yields the same identity; a materially different
/// snapshot yields a different one.
fn snapshot_identity(snapshot: &str) -> String {
    format!(
        "sha256:{}",
        crate::runtime::hash::sha256_hex(snapshot.as_bytes())
    )
}

fn phase_for(role: Role) -> OrchestrationPhase {
    match role {
        Role::Lead => OrchestrationPhase::Idle,
        Role::Explore | Role::ExploreDeep => OrchestrationPhase::Explore,
        Role::Build => OrchestrationPhase::Build,
        Role::Verify => OrchestrationPhase::Verify,
        Role::Debug => OrchestrationPhase::Debug,
        Role::Docs => OrchestrationPhase::Done,
    }
}

fn severity_label(severity: Severity) -> &'static str {
    match severity {
        Severity::Info => "info",
        Severity::Warning => "warning",
        Severity::Critical => "critical",
    }
}

fn render_verification(verification: Option<&HandoffVerification>) -> String {
    let Some(verification) = verification else {
        return String::new();
    };
    let mut out = format!(
        "verification: stage '{}' {}\n",
        verification.stage, verification.outcome
    );
    if !verification.failed_commands.is_empty() {
        out.push_str("failed commands:\n");
        for command in &verification.failed_commands {
            out.push_str(&format!("- {command}\n"));
        }
    }
    if !verification.failed_tests.is_empty() {
        out.push_str("failed tests:\n");
        for test in &verification.failed_tests {
            out.push_str(&format!("- {test}\n"));
        }
    }
    if !verification.locations.is_empty() {
        out.push_str("failing locations:\n");
        for location in &verification.locations {
            out.push_str(&format!("- {}\n", location.display()));
        }
    }
    if !verification.raw_log_refs.is_empty() {
        out.push_str("raw logs:\n");
        for reference in &verification.raw_log_refs {
            out.push_str(&format!("- {reference}\n"));
        }
    }
    out
}

fn render_warnings(warnings: &[String]) -> String {
    if warnings.is_empty() {
        return String::new();
    }
    let mut out = String::from("warnings:\n");
    for warning in warnings {
        out.push_str(&format!("- {warning}\n"));
    }
    out
}

fn render_advisory(advisory: &Value) -> String {
    let allowed = advisory
        .get("allowed")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let denied = advisory
        .get("denied")
        .and_then(Value::as_array)
        .map(|values| values.len())
        .unwrap_or(0);
    format!(
        "capabilities (advisory; OpenCode permissions remain authoritative): allowed=[{allowed}] denied={denied}\n"
    )
}

fn orchestration_metrics(
    phase: OrchestrationPhase,
    source: Role,
    destination: Role,
    attempts: Attempts,
) -> OrchestrationMetrics {
    OrchestrationMetrics {
        phase: Some(phase.as_str().to_string()),
        source: Some(source.as_str().to_string()),
        destination: Some(destination.as_str().to_string()),
        attempt: Some(attempts.build.max(attempts.debug).max(1)),
        retry: Some(
            attempts
                .build
                .saturating_sub(1)
                .max(attempts.debug.saturating_sub(1)),
        ),
        ..OrchestrationMetrics::default()
    }
}

/// Build the compact verification block from a report. Public so the bridge can
/// render Debug feedback without duplicating the mapping.
pub fn handoff_verification(report: &VerificationReport) -> HandoffVerification {
    let mut failed_commands = Vec::new();
    let mut failed_tests = Vec::new();
    let mut locations = Vec::new();
    let mut raw_log_refs = Vec::new();
    let mut distilled = Vec::new();
    for result in &report.results {
        if !result.success {
            failed_commands.push(result.display());
        }
        for test in &result.failed_tests {
            if !failed_tests.contains(test) {
                failed_tests.push(test.clone());
            }
        }
        for location in &result.source_locations {
            if !locations.contains(location) {
                locations.push(location.clone());
            }
        }
        if let Some(reference) = &result.raw_log {
            if !raw_log_refs.contains(reference) {
                raw_log_refs.push(reference.clone());
            }
        }
        for line in result.output.summary.iter().take(4) {
            if !distilled.contains(line) {
                distilled.push(line.clone());
            }
        }
    }
    HandoffVerification {
        stage: report.stage.clone(),
        outcome: report.overall().as_str().to_string(),
        failed_commands,
        failed_tests,
        locations,
        raw_log_refs,
        distilled,
    }
}

fn record_verification_evidence(session: &mut SessionState, report: &VerificationReport) {
    let summary = format!(
        "verification stage '{}': {} ({} attempt(s))",
        report.stage,
        report.overall().as_str(),
        report.results.len()
    );
    if !session.evidence.contains(&summary) {
        session.evidence.push(summary);
    }
    for result in &report.results {
        if result.success {
            continue;
        }
        let failure = format!("{} ({})", result.display(), result.exit.label());
        if !session.failures.contains(&failure) {
            session.failures.push(failure);
        }
        for test in &result.failed_tests {
            let failure = format!("test failed: {test}");
            if !session.failures.contains(&failure) {
                session.failures.push(failure);
            }
        }
        for location in &result.source_locations {
            let failure = format!("failing at {}", location.display());
            if !session.failures.contains(&failure) {
                session.failures.push(failure);
            }
        }
    }
    if session.failures.len() > 64 {
        let excess = session.failures.len() - 64;
        session.failures.drain(0..excess);
    }
    if session.evidence.len() > 64 {
        let excess = session.evidence.len() - 64;
        session.evidence.drain(0..excess);
    }
}

/// An explainable, privacy-safe Debug reason. It deliberately contains **no**
/// configured command string and no raw output: only the stage, the outcome,
/// attempt/retry counts, the number of failing commands and the first distilled
/// source location.
fn debug_reason(
    stage: &str,
    attempts: &Attempts,
    config: &OrchestrationConfig,
    report: &VerificationReport,
) -> String {
    let failed = report
        .results
        .iter()
        .filter(|result| !result.success)
        .count();
    let location = report
        .results
        .iter()
        .flat_map(|result| result.source_locations.iter())
        .next()
        .map(distill::SourceLocation::display)
        .unwrap_or_else(|| "no location in the distilled output".to_string());
    format!(
        "verification stage '{stage}' {} after {n} build attempt(s) (retry budget {}); {failed} failing command(s); first location: {location}",
        report.overall().as_str(),
        config.max_build_retries,
        n = attempts.build,
    )
}

/// The deterministic result of parsing an Explore output.
#[derive(Debug, Clone, Default)]
struct ParsedExplore {
    structured: bool,
    goal: Option<String>,
    constraints: Vec<String>,
    findings: Vec<HandoffFinding>,
    files: Vec<String>,
    symbols: Vec<String>,
    locations: Vec<distill::SourceLocation>,
}

/// Parse an Explore output. A structured JSON object is honored when present;
/// otherwise a safe, deterministic line-based fallback is used. Never invents a
/// conclusion and never keeps secret-shaped content.
fn parse_explore_output(output: &str) -> ParsedExplore {
    if let Some(value) = extract_json(output) {
        if let Some(parsed) = parse_structured(&value) {
            return parsed;
        }
    }
    parse_fallback(output)
}

fn extract_json(output: &str) -> Option<Value> {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str::<Value>(&trimmed[start..=end]).ok()
}

fn parse_structured(value: &Value) -> Option<ParsedExplore> {
    let object = value.as_object()?;
    let has_shape = object.contains_key("findings")
        || object.contains_key("goal")
        || object.contains_key("constraints")
        || object.contains_key("files")
        || object.contains_key("symbols");
    if !has_shape {
        return None;
    }
    let mut parsed = ParsedExplore {
        structured: true,
        ..ParsedExplore::default()
    };
    if let Some(goal) = object.get("goal").and_then(Value::as_str) {
        let goal = goal.trim();
        if !goal.is_empty() && !crate::telemetry::task::is_secret_like(goal) {
            parsed.goal = Some(goal.to_string());
        }
    }
    for constraint in object
        .get("constraints")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(text) = constraint.as_str() {
            let text = text.trim();
            if !text.is_empty() && !crate::telemetry::task::is_secret_like(text) {
                parsed.constraints.push(text.to_string());
            }
        }
    }
    for finding in object
        .get("findings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let summary = finding
            .get("summary")
            .and_then(Value::as_str)
            .or_else(|| finding.as_str())
            .unwrap_or("")
            .trim();
        if summary.is_empty() || crate::telemetry::task::is_secret_like(summary) {
            continue;
        }
        let detail = finding
            .get("detail")
            .and_then(Value::as_str)
            .map(str::to_string);
        let source = finding
            .get("source")
            .and_then(Value::as_str)
            .map(str::to_string);
        if source
            .as_deref()
            .map(|source| crate::context::classify::classify(Path::new(source)).sensitive)
            .unwrap_or(false)
        {
            continue;
        }
        let severity = finding
            .get("severity")
            .and_then(Value::as_str)
            .map(parse_severity)
            .unwrap_or(Severity::Info);
        parsed.findings.push(HandoffFinding {
            summary: truncate_text(summary, 300),
            detail,
            source,
            severity,
        });
    }
    for path in object
        .get("files")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(path) = path.as_str() {
            if !crate::context::classify::classify(Path::new(path)).sensitive {
                parsed.files.push(path.to_string());
            }
        }
    }
    for symbol in object
        .get("symbols")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = match symbol {
            Value::String(name) => Some(name.as_str()),
            Value::Object(map) => map.get("name").and_then(Value::as_str),
            _ => None,
        };
        if let Some(name) = name {
            if !name.trim().is_empty() && !crate::telemetry::task::is_secret_like(name) {
                parsed.symbols.push(name.to_string());
            }
        }
    }
    if parsed.findings.is_empty() && parsed.goal.is_none() && parsed.files.is_empty() {
        // A structured object with only symbols is still useful, so keep it.
        parsed.structured = !parsed.symbols.is_empty();
    }
    Some(parsed)
}

fn parse_severity(text: &str) -> Severity {
    match text.trim().to_ascii_lowercase().as_str() {
        "critical" | "error" | "fatal" => Severity::Critical,
        "warning" | "warn" => Severity::Warning,
        _ => Severity::Info,
    }
}

fn parse_fallback(output: &str) -> ParsedExplore {
    let mut parsed = ParsedExplore::default();
    for line in output.lines() {
        if parsed.findings.len() >= 12 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || crate::telemetry::task::is_secret_like(trimmed) {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        let severity = if lower.contains("error")
            || lower.contains("failed")
            || lower.contains("panic")
            || lower.contains("fatal")
        {
            Severity::Critical
        } else if lower.contains("warn") {
            Severity::Warning
        } else {
            Severity::Info
        };
        if let Some(location) = distill::parse_location(trimmed) {
            if !crate::context::classify::classify(Path::new(&location.path)).sensitive
                && !parsed.locations.contains(&location)
            {
                parsed.locations.push(location);
            }
        }
        parsed.findings.push(HandoffFinding {
            summary: truncate_text(trimmed, 200),
            detail: None,
            source: None,
            severity,
        });
    }
    parsed
}

fn truncate_text(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max).collect();
    out.push('…');
    out
}

/// The maximum bytes of real diff text carried in a hand-off. Bounded and
/// deterministic; the full diff stays in the context plan's own artifact.
pub const MAX_DIFF_CONTEXT_BYTES: usize = 8192;

/// Render a bounded, real diff block from the plan's [`DiffSummary`]. It names
/// the changed paths, keeps the actual hunks the context engine retained, drops
/// secret-shaped lines and sensitive paths, and states structural truncation.
pub fn render_diff_context(plan: &crate::context::ranking::ContextPlan) -> String {
    let mut out = String::new();
    let safe_paths: Vec<&String> = plan
        .changed_paths
        .iter()
        .filter(|path| !crate::context::classify::classify(Path::new(path)).sensitive)
        .collect();
    out.push_str(&format!("changed paths ({}):\n", safe_paths.len()));
    for path in safe_paths {
        out.push_str(&format!("- {path}\n"));
    }
    if let Some(diff) = &plan.diff {
        let block = render_bounded_diff(diff);
        if !block.is_empty() {
            out.push_str(&block);
        }
    }
    if plan.git.dirty {
        out.push_str("git: dirty\n");
    }
    if let Some(head) = &plan.git.head {
        out.push_str(&format!("git head: {head}\n"));
    }
    out
}

/// A bounded, deterministic rendering of the retained diff. Only files and
/// hunks the context engine actually kept are shown; nothing is invented.
pub fn render_bounded_diff(diff: &crate::context::gitdiff::DiffSummary) -> String {
    let mut out = String::new();
    let mut truncated = false;
    for entry in &diff.entries {
        if crate::context::classify::classify(Path::new(&entry.path)).sensitive {
            continue;
        }
        if out.len() >= MAX_DIFF_CONTEXT_BYTES {
            truncated = true;
            break;
        }
        out.push_str(&format!(
            "diff {} ({})\n",
            entry.path,
            entry.status.as_str()
        ));
        for hunk in diff.hunks.iter().filter(|hunk| hunk.path == entry.path) {
            if out.len() >= MAX_DIFF_CONTEXT_BYTES {
                truncated = true;
                break;
            }
            out.push_str(&format!("  {}\n", hunk.header));
            for line in &hunk.body {
                if crate::telemetry::task::is_secret_like(line) {
                    continue;
                }
                if out.len() >= MAX_DIFF_CONTEXT_BYTES {
                    truncated = true;
                    break;
                }
                out.push_str(&format!("  {line}\n"));
            }
        }
    }
    if let Some(structural) = &diff.structural {
        out.push_str(&format!(
            "diff structural: {} file(s), {} retained hunk(s), truncated={}\n",
            structural.files_changed, structural.hunks, structural.truncated
        ));
    }
    if truncated || diff.truncated || diff.capture_truncated {
        out.push_str("diff context truncated by the configured limits\n");
    }
    out
}

/// A content fingerprint of a rendered diff block, used as a real (not
/// fabricated) hand-off diff reference. `None` for an empty block.
pub fn diff_reference(block: &str) -> Option<String> {
    if block.trim().is_empty() {
        return None;
    }
    Some(format!(
        "sha256:{}",
        crate::runtime::hash::sha256_hex(block.as_bytes())
    ))
}

/// A public summary of an existing hand-off for diagnostics.
pub fn capsule_summary(capsule: &ModelHandoffCapsule) -> String {
    format!(
        "{} {} -> {} ({} bytes)",
        capsule.transition.as_str(),
        capsule.source.as_str(),
        capsule.destination.as_str(),
        capsule.measured_bytes()
    )
}
