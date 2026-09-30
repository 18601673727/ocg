//! The hidden `ocg __bridge` surface.
//!
//! The generated JavaScript adapter spawns `ocg __bridge <event>` with a direct
//! argv and a JSON payload on stdin. This module is the *only* thing the
//! adapter can ask for. It is deliberately a thin, typed translation layer:
//!
//! - all decisions are delegated to [`crate::orchestration::controller`];
//! - errors are converted into `{ "ok": false }` and never panic or write raw
//!   content;
//! - telemetry is recorded here, with no prompt, source or log content.
//!
//! No model or network is involved in this translation layer.

use crate::orchestration::context_governor::{ContextObservation, GovernorState};
use crate::orchestration::controller::{ContextGovernanceResult, Controller};
use crate::orchestration::domain::ExecutionWitness;
use crate::orchestration::domain::{AttemptAuthority, DomainRepository};
use crate::orchestration::handoff::Role;
use crate::process::CaptureRunner;
use crate::reports::ReportsConfig;
use crate::runtime::compat::{BridgeRuntimeClient, LeadSelection};
use crate::runtime::lifecycle::{
    RuntimeAdapter, RuntimeContextEvent, RuntimeContextUsage, RuntimeExecutionId, RuntimeProfile,
};
use crate::telemetry::{self, Event, OrchestrationMetrics, Outcome, TelemetryConfig};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

/// One bridge request's result plus its telemetry accounting.
struct BridgeOutcome {
    value: Value,
    metrics: OrchestrationMetrics,
    outcome: Outcome,
    role: Option<String>,
    session_id: Option<String>,
    task_type: String,
}

impl BridgeOutcome {
    fn ok(mut value: Value, role: Role, session_id: Option<String>) -> Self {
        value["ok"] = json!(true);
        Self {
            value,
            metrics: OrchestrationMetrics::default(),
            outcome: Outcome::Success,
            role: Some(role.as_str().to_string()),
            session_id,
            task_type: "orchestration".to_string(),
        }
    }

    fn err(msg: String, role: Role, session_id: Option<String>) -> Self {
        Self {
            value: json!({"ok": false, "error": msg}),
            metrics: OrchestrationMetrics::default(),
            outcome: Outcome::Unknown,
            role: Some(role.as_str().to_string()),
            session_id,
            task_type: "orchestration".to_string(),
        }
    }
}

/// The bridge context: the controller, the capture runner and the telemetry
/// policy. `runner` is the trusted verification runner; tests inject a fake.
pub struct BridgeContext<'a> {
    pub controller: &'a Controller<'a>,
    pub runner: &'a dyn CaptureRunner,
    pub telemetry: TelemetryConfig,
    pub reports: ReportsConfig,
    /// Invocation-scoped runtime client. It is absent for ordinary bridge
    /// calls and for tests that only exercise policy projection.
    /// Used by `context.observe` for `observe_context` and by
    /// `session.prompt` for Lead authority enforcement.
    ///
    /// Stored as one shared concrete-client trait object so `context.observe`
    /// and `session.prompt` cannot accidentally use different transports.
    pub rollover_runtime: Option<Rc<RefCell<Box<dyn BridgeRuntimeClient>>>>,
    /// Compatibility seam for lifecycle-only callers (including rollover
    /// tests). It is intentionally never used by `session.prompt`, which must
    /// have the full shared client surface above.
    pub lifecycle_runtime: Option<Rc<RefCell<Box<dyn RuntimeAdapter>>>>,
    pub rollover_profile: Option<RuntimeProfile>,
    /// Canonical ResolvedLeadContract for this invocation. Used by the prompt
    /// authority gate to enforce Lead before task admission.
    pub lead_contract: Option<LeadSelection>,
    /// The worker routing table OCG wrote into the generated agent config:
    /// `role -> provider/model`. A canonical child Attempt's frozen contract must
    /// record the model that agent will actually use, not the Lead's.
    pub routing: Option<WorkerRouting>,
}

/// The durable worker routing table, keyed by the routing role name.
#[derive(Debug, Clone, Default)]
pub struct WorkerRouting {
    models: std::collections::BTreeMap<String, String>,
}

impl WorkerRouting {
    /// Read the routing table from an effective configuration. A role without a
    /// configured model is simply absent; the caller then falls back visibly
    /// rather than inventing a contract.
    pub fn from_config(data: &Value) -> Self {
        let mut models = std::collections::BTreeMap::new();
        if let Some(roles) = crate::model::role_specs(data) {
            for (role, spec) in roles {
                if let Some(model) = spec.get("model").and_then(Value::as_str) {
                    if let Ok((_provider, full)) = crate::model::model_full_id(data, model) {
                        models.insert(role.clone(), full);
                    }
                }
            }
        }
        Self { models }
    }

    /// The provider/model OCG configures for one routing role.
    pub fn model_for(&self, routing_role: &str) -> Option<&str> {
        self.models.get(routing_role).map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

impl<'a> BridgeContext<'a> {
    pub fn new(
        controller: &'a Controller<'a>,
        runner: &'a dyn CaptureRunner,
        telemetry: TelemetryConfig,
    ) -> Self {
        Self {
            controller,
            runner,
            telemetry,
            reports: ReportsConfig::default(),
            rollover_runtime: None,
            lifecycle_runtime: None,
            rollover_profile: None,
            lead_contract: None,
            routing: None,
        }
    }

    /// Attach the invocation-owned client used by `context.observe`
    /// and `session.prompt`. The client is held inside this short-lived
    /// bridge value: a dropped OpenCode/UI connection cannot turn into a
    /// Mission failure because the Mission record is never stored in this
    /// object.
    ///
    /// The client must implement the three runtime/session interfaces. Both
    /// `V2SessionClient` and `MemorySessionClient` implement all three.
    pub fn with_bridge_runtime(
        mut self,
        runtime: Rc<RefCell<Box<dyn BridgeRuntimeClient>>>,
        profile: RuntimeProfile,
    ) -> Self {
        self.rollover_runtime = Some(runtime);
        self.rollover_profile = Some(profile);
        self
    }

    /// Attach a lifecycle-only runtime for context observation and rollover.
    /// Prompt authority enforcement remains unavailable unless callers use
    /// [`Self::with_bridge_runtime`].
    pub fn with_rollover_runtime<R: RuntimeAdapter + 'static>(
        mut self,
        runtime: R,
        profile: RuntimeProfile,
    ) -> Self {
        self.lifecycle_runtime = Some(Rc::new(RefCell::new(Box::new(runtime))));
        self.rollover_profile = Some(profile);
        self
    }

    /// Attach the canonical ResolvedLeadContract for prompt authority enforcement.
    pub fn with_lead_contract(mut self, lead: LeadSelection) -> Self {
        self.lead_contract = Some(lead);
        self
    }

    /// Attach the resolved worker routing table so a canonical child Attempt can
    /// freeze the model its agent will actually use.
    pub fn with_routing(mut self, routing: WorkerRouting) -> Self {
        self.routing = Some(routing);
        self
    }

    /// Apply the report policy for this bridge.
    pub fn with_reports(mut self, reports: ReportsConfig) -> Self {
        self.reports = reports;
        self
    }

    /// Dispatch one event. Never fails: a bad payload or a controller error is
    /// reported as `{ "ok": false }`.
    pub fn dispatch(&self, event: &str, payload: &Value) -> Value {
        // A disabled policy is inert here too: no state, no telemetry, no
        // context. The CLI short-circuits earlier, but the bridge stays honest
        // when driven directly.
        if !self.controller.config().enabled {
            return json!({"ok": false, "disabled": true, "context": ""});
        }
        let started = Instant::now();
        let outcome = match event {
            "work.mission.create"
            | "work.child.create"
            | "work.ready"
            | "work.dispatch"
            | "work.finish"
            | "work.replace"
            | "work.inspect" => self.canonical_domain_work(event, payload),
            "chat.message" | "chat-message" => self.chat_message(payload),
            "session.prompt" => self.session_prompt(payload),
            "session.context" => self.session_context(payload),
            "context.observe" | "session.context.observe" | "context-observation" => {
                self.context_observe(payload)
            }
            "tool.execute.before" | "tool.execute.before/task" | "task-before" => {
                self.tool_before(payload)
            }
            "tool.execute.after" | "task-after" => self.tool_after(payload),
            "lead.output" | "lead-output" => self.lead_output(payload),
            other => BridgeOutcome {
                value: json!({"ok": false, "error": format!("unknown bridge event: {other}")}),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: None,
                session_id: None,
                task_type: "orchestration".to_string(),
            },
        };
        self.record(&outcome, started.elapsed().as_millis() as u64);
        outcome.value
    }

    fn canonical_domain_work(&self, event: &str, payload: &Value) -> BridgeOutcome {
        let operation = || -> crate::error::Result<Value> {
            let mut repository = DomainRepository::open(self.controller.root())?;
            match event {
                "work.mission.create" => {
                    let session = payload
                        .get("runtime_execution_id")
                        .or_else(|| payload.get("session_id"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| crate::error::OcgError::config("missing session_id"))?;
                    let body = payload.get("payload").and_then(Value::as_str).unwrap_or("");
                    let executor = payload
                        .get("contract")
                        .and_then(|value| value.get("executor"))
                        .and_then(Value::as_str)
                        .unwrap_or("lead");
                    let admission = self
                        .controller
                        .admit_canonical_job(session, body, executor)?;
                    Ok(json!({
                        "project_id": admission.project.id,
                        "job_id": admission.job.id,
                        "attempt_id": admission.attempt.id,
                        "executor_id": admission.executor.id,
                        "generation": admission.attempt.generation,
                    }))
                }
                "work.child.create" => {
                    let parent = self
                        .canonical_authority(
                            payload
                                .get("session_id")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                        )
                        .ok_or_else(|| {
                            crate::error::OcgError::config("missing canonical parent authority")
                        })?;
                    let dependencies = payload
                        .get("dependencies")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .map(|value| {
                                    value.as_str().map(str::to_string).ok_or_else(|| {
                                        crate::error::OcgError::config(
                                            "dependencies must be Job identifiers",
                                        )
                                    })
                                })
                                .collect::<crate::error::Result<Vec<_>>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let refs = dependencies.iter().map(String::as_str).collect::<Vec<_>>();
                    let body = payload.get("payload").and_then(Value::as_str).unwrap_or("");
                    let job = repository.create_child_job(&parent.authority, body, &refs)?;
                    Ok(json!({
                        "project_id": job.project_id,
                        "job_id": job.id,
                        "job": job,
                    }))
                }
                "work.dispatch" => {
                    let job_id = payload
                        .get("job_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| crate::error::OcgError::config("missing job_id"))?;
                    let kind = payload
                        .get("executor")
                        .and_then(Value::as_str)
                        .unwrap_or("worker");
                    let (attempt, executor) = repository.dispatch_job(job_id, kind)?;
                    Ok(
                        json!({"job_id":job_id,"attempt_id":attempt.id,"executor_id":executor.id,"generation":attempt.generation}),
                    )
                }
                "work.finish" => {
                    let attempt_id = payload
                        .get("attempt_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| crate::error::OcgError::config("missing attempt_id"))?;
                    let state = payload
                        .get("outcome")
                        .and_then(Value::as_str)
                        .unwrap_or("failed");
                    if !matches!(state, "completed" | "failed") {
                        return Err(crate::error::OcgError::config(
                            "outcome must be completed or failed",
                        ));
                    }
                    if let Some(call_id) = payload.get("call_id").and_then(Value::as_str) {
                        let generation = payload
                            .get("generation")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| crate::error::OcgError::config("missing generation"))?;
                        let result = payload.get("result").cloned().unwrap_or(Value::Null);
                        let witness =
                            repository.witness_for_call(call_id, attempt_id, generation)?;
                        let response = serde_json::to_string(&result)
                            .map_err(|error| crate::error::OcgError::config(error.to_string()))?;
                        let disposition =
                            repository.deliver_result(&witness, &response, state == "completed")?;
                        return Ok(
                            json!({"attempt_id":attempt_id,"call_id":call_id,"disposition":disposition,"applied":disposition == "authoritative"}),
                        );
                    }
                    if !matches!(state, "completed" | "failed") {
                        return Err(crate::error::OcgError::config("outcome must be completed or failed; cancellation requires confirmed stop"));
                    }
                    repository.finish_attempt(attempt_id, state == "completed")?;
                    Ok(json!({"attempt_id":attempt_id,"outcome":state,"applied":true}))
                }
                "work.replace" => {
                    let job_id = payload
                        .get("job_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| crate::error::OcgError::config("missing job_id"))?;
                    let kind = payload
                        .get("executor")
                        .and_then(Value::as_str)
                        .unwrap_or("worker");
                    let expected = payload
                        .get("attempt_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            crate::error::OcgError::config("missing expected attempt_id")
                        })?;
                    let admission =
                        repository.replace_attempt_checked(job_id, kind, Some(expected))?;
                    Ok(
                        json!({"job_id":admission.job.id,"attempt_id":admission.attempt.id,"executor_id":admission.executor.id,"generation":admission.attempt.generation}),
                    )
                }
                "work.inspect" => {
                    let job_id = payload
                        .get("job_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| crate::error::OcgError::config("missing job_id"))?;
                    repository.inspect_job(job_id)
                }
                "work.ready" => {
                    let project = repository.ensure_project(self.controller.root())?;
                    Ok(json!({"project_id":project.id,"ready":repository.ready_jobs(&project.id)?}))
                }
                _ => Err(crate::error::OcgError::config(
                    "unknown canonical work event",
                )),
            }
        };
        match operation() {
            Ok(mut value) => {
                value["event"] = json!(event);
                BridgeOutcome::ok(value, Role::Lead, None)
            }
            Err(error) => BridgeOutcome::err(safe_error(&error.to_string()), Role::Lead, None),
        }
    }

    fn chat_message(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        let text = payload
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if text.trim().is_empty() {
            return BridgeOutcome {
                value: json!({"ok": false, "error": "empty chat.message payload"}),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(session_id),
                task_type: "orchestration".to_string(),
            };
        }
        match self.controller.prepare_lead_context(&session_id, &text) {
            Ok(context) => BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "chat.message",
                    "session_id": context.session_id,
                    "task_id": context.task_id,
                    // V1 persisted-prompt path: an unchanged repository
                    // baseline already lives in the persisted history, so it
                    // is not appended again. The plugin treats an empty
                    // context as a no-op. (The V2 `session.context` path above
                    // always returns the full baseline instead.)
                    "context": if context.cached { String::new() } else { context.dynamic_context },
                    "snapshot_id": context.snapshot_id,
                    "cached": context.cached,
                    "estimated_tokens": context.estimated_tokens,
                    "bytes": context.bytes,
                    "file_count": context.file_count,
                    "symbol_count": context.symbol_count,
                }),
                metrics: context.metrics,
                outcome: Outcome::Success,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(context.session_id),
                task_type: "orchestration".to_string(),
            },
            Err(error) => self.error_outcome(error.to_string(), Some(Role::Lead), Some(session_id)),
        }
    }

    /// `session.prompt` (OpenCode V2 prompt admission): a genuinely admitted
    /// user prompt. This is the *only* bridge event that may establish or
    /// reset the session's task identity. Runtime-generated synthetic
    /// user-role messages (interruption/resume continuations and similar)
    /// never pass through OpenCode's prompt admission, so they never reach
    /// this handler and can never reset task-scoped state.
    ///
    /// Lead authority gate (fail-closed):
    ///   1. Validate payload
    ///   2. Classify root session via durable Mission binding (not mutable
    ///      runtime agent name — see P0-J)
    ///   3. Inspect effective Lead via SessionClient
    ///   4. Enforce via ensure_existing_session_lead (idempotent)
    ///   5. ONLY THEN admit_user_task
    ///   6. Return success
    fn session_prompt(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        let text = payload
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if text.trim().is_empty() {
            return BridgeOutcome::err(
                "empty session.prompt payload".into(),
                Role::Lead,
                Some(session_id),
            );
        }

        // 1. CLASSIFY: Require runtime client + canonical Lead contract
        let Some(lead) = self.lead_contract.as_ref() else {
            return BridgeOutcome::err(
                "Lead enforcement unavailable: missing runtime client or Lead contract".into(),
                Role::Lead,
                Some(session_id),
            );
        };
        let Some(runtime_cell) = self.rollover_runtime.as_ref() else {
            return BridgeOutcome::err(
                "Lead enforcement unavailable: missing runtime client or Lead contract".into(),
                Role::Lead,
                Some(session_id),
            );
        };

        // 2. Determine root ownership from runtime lineage. The canonical
        //    SQLite binding is established below; mutable agent labels are
        //    never authority.
        let mut runtime = runtime_cell.borrow_mut();
        let is_root_lead =
            match is_root_lead_execution(self.controller, &mut **runtime, &session_id) {
                Ok(root) => root,
                Err(error) => {
                    return BridgeOutcome::err(
                        format!("runtime ownership unavailable: {error}"),
                        Role::Lead,
                        Some(session_id),
                    )
                }
            };
        drop(runtime);

        if is_root_lead {
            // 3. INSPECT + 4. ENFORCE: idempotent ensure_existing_session_lead
            //    Uses canonical verify_effective_lead; no-op if already correct
            let mut session_client = runtime_cell.borrow_mut();
            if let Err(e) = crate::runtime::compat::ensure_existing_session_lead(
                session_client.as_session(),
                &session_id,
                lead,
            ) {
                return BridgeOutcome::err(
                    format!("Lead enforcement failed: {}", e),
                    Role::Lead,
                    Some(session_id),
                );
            }
        }
        // else: worker/subagent (ocg-*) or non-current execution → skip, preserve worker routing

        // 4b. A worker session presents the OCG-owned envelope its parent
        // dispatch attached. Binding it here is what lets the worker create
        // children for its own subtree: the identity is the durable witness,
        // never the agent name or the prompt.
        {
            if let Some(present) = witness_from_prompt(&text) {
                let job_id = &present.job_id;
                if job_id.is_empty() {
                    return BridgeOutcome::err(
                        "canonical binding refused: witness has no Job identity".into(),
                        Role::Lead,
                        Some(session_id),
                    );
                };
                let mut repository = match DomainRepository::open(self.controller.root()) {
                    Ok(repository) => repository,
                    Err(error) => {
                        return BridgeOutcome::err(
                            format!("canonical binding refused: {error}"),
                            Role::Lead,
                            Some(session_id),
                        )
                    }
                };
                let binding = repository
                    .validate_execution_witness(&present)
                    .and_then(|call| {
                        if call.state != "running"
                            || repository.authority(&present.attempt_id)?.is_none()
                        {
                            return Err(crate::error::OcgError::config("stale execution witness"));
                        }
                        repository.bind_attempt(
                            &session_id,
                            &AttemptAuthority {
                                job_id: job_id.clone(),
                                attempt_id: present.attempt_id.clone(),
                                generation: present.generation,
                            },
                        )
                    });
                if let Err(error) = binding {
                    return BridgeOutcome::err(
                        format!("canonical binding refused: {error}"),
                        Role::Lead,
                        Some(session_id),
                    );
                }
            }
        }

        // 5b. Production cutover: a genuinely admitted root Lead prompt also
        // establishes the SQLite canonical Project/Job/Attempt/Executor chain.
        // The legacy task record remains a context compatibility projection and
        // cannot make a canonical execution decision.
        let canonical = if is_root_lead {
            match self.admit_canonical_job(&session_id, &text) {
                Ok(value) => value,
                Err(error) => {
                    return BridgeOutcome::err(
                        format!("canonical admission refused: {error}"),
                        Role::Lead,
                        Some(session_id),
                    )
                }
            }
        } else {
            if self.canonical_authority(&session_id).is_none() {
                return BridgeOutcome::err(
                    "worker prompt requires a current canonical Attempt binding".into(),
                    Role::Lead,
                    Some(session_id),
                );
            }
            None
        };

        // 5. ONLY NOW: admit user task (OCG task state advances)
        let admission = match self.controller.admit_user_task(&session_id, &text) {
            Ok(a) => a,
            Err(e) => return BridgeOutcome::err(e.to_string(), Role::Lead, Some(session_id)),
        };

        // 6. SUCCESS
        let mut value = json!({
            "event": "session.prompt",
            "session_id": admission.session_id,
            "task_id": admission.task_id,
            "changed": admission.changed,
        });
        if let Some(canonical) = canonical {
            value["canonical"] = canonical;
        }
        BridgeOutcome::ok(value, Role::Lead, Some(admission.session_id))
    }

    /// Establish (or recover) the canonical Job for an admitted root Lead
    /// session. The session binding is the idempotency key; all authority is
    /// committed by the SQLite domain repository in one immediate transaction.
    fn admit_canonical_job(
        &self,
        session_id: &str,
        text: &str,
    ) -> crate::error::Result<Option<Value>> {
        let lead = self
            .lead_contract
            .as_ref()
            .ok_or_else(|| crate::error::OcgError::config("canonical Lead contract missing"))?;
        let objective: String = text.chars().take(4096).collect();
        let admission = self
            .controller
            .admit_canonical_job(session_id, &objective, &lead.agent)?;
        Ok(Some(json!({
            "project_id": admission.project.id,
            "job_id": admission.job.id,
            "attempt_id": admission.attempt.id,
            "generation": admission.attempt.generation,
            "executor_id": admission.executor.id,
            "reused": admission.job.generation > 1,
        })))
    }

    /// `session.context` (OpenCode V2 model dispatch): supply the current
    /// session repository baseline for one root-Lead request. Unlike the V1
    /// `chat.message` path, the full baseline is returned on *every* dispatch —
    /// the adapter injects it into the outgoing request's system context,
    /// which is never persisted, so nothing accumulates in the conversation
    /// history. `cached` only reports that baseline computation was reused; it
    /// never suppresses inclusion.
    ///
    /// Dispatch-time conversation content is never a task signal here: task
    /// identity is owned by `session.prompt` admission. A tool-driven
    /// continuation or a synthetic user-role message must not reset
    /// task-scoped state, so a `text` field in the payload is ignored.
    fn session_context(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        match self.controller.prepare_model_context(&session_id) {
            Ok(context) => BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "session.context",
                    "session_id": context.session_id,
                    "task_id": context.task_id,
                    "context": context.dynamic_context,
                    "snapshot_id": context.snapshot_id,
                    "cached": context.cached,
                    "estimated_tokens": context.estimated_tokens,
                    "bytes": context.bytes,
                    "file_count": context.file_count,
                    "symbol_count": context.symbol_count,
                }),
                metrics: context.metrics,
                outcome: Outcome::Success,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(context.session_id),
                task_type: "orchestration".to_string(),
            },
            Err(error) => self.error_outcome(error.to_string(), Some(Role::Lead), Some(session_id)),
        }
    }

    fn context_observe(&self, payload: &Value) -> BridgeOutcome {
        let session = session_id(payload);
        if session.is_empty() {
            return BridgeOutcome {
                value: json!({"ok": false, "error": "context.observe requires session_id"}),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: None,
                task_type: "context".to_string(),
            };
        }
        let Some(agent) = payload.get("agent").and_then(Value::as_str) else {
            return BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "context.observe",
                    "ignored": true,
                    "reason": "context pressure is observed only for a root Lead session",
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(session),
                task_type: "context".to_string(),
            };
        };
        let execution_id = RuntimeExecutionId::new(session.clone());
        match self.controller.is_current_execution(&execution_id) {
            Ok(true) => {}
            Ok(false) => {
                return BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "context.observe",
                        "ignored": true,
                        "reason": "context pressure is observed only for the current Mission execution",
                    }),
                    metrics: OrchestrationMetrics::default(),
                    outcome: Outcome::Unknown,
                    role: Some(agent.to_string()),
                    session_id: Some(session),
                    task_type: "context".to_string(),
                }
            }
            Err(_) => {
                return BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "context.observe",
                        "ignored": true,
                        "reason": "current Mission execution identity is unavailable",
                    }),
                    metrics: OrchestrationMetrics::default(),
                    outcome: Outcome::Unknown,
                    role: Some(agent.to_string()),
                    session_id: Some(session),
                    task_type: "context".to_string(),
                }
            }
        }
        if !self.controller.config().context_governor.enabled {
            let observation = ContextObservation {
                session_id: session.clone(),
                ..ContextObservation::default()
            };
            let decision = crate::orchestration::context_governor::GovernorDecision {
                state: GovernorState::Disabled,
                action: crate::orchestration::context_governor::GovernorAction::Continue,
                utilization_percent: None,
                rollover_allowed: false,
                deferred_for_boundary: false,
                reason: "context governor is disabled".to_string(),
            };
            return BridgeOutcome {
                value: context_governance_value(&observation, &decision, None),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Success,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(session),
                task_type: "context".to_string(),
            };
        }
        let now = self.controller.now_unix();
        let event_id = payload
            .get("event_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                crate::orchestration::context_governor::event_identity(
                    &session,
                    payload.get("assistant_message_id").and_then(Value::as_str),
                    payload.get("finish").and_then(Value::as_str),
                    payload.get("observed_at").and_then(Value::as_i64),
                )
            });
        let finish = payload
            .get("finish")
            .and_then(Value::as_str)
            .map(str::to_string);
        let assistant_message_id = payload
            .get("assistant_message_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        // A step is a rollover boundary only after the event adapter has
        // durably handed its completed output to the bridge. A caller cannot
        // simply set a boolean in an arbitrary payload and skip that ordering.
        let safe_boundary = payload
            .get("safe_boundary")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && payload
                .get("output_persisted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            && finish.as_deref() == Some("stop");
        let step_tokens = payload
            .get("tokens")
            .or_else(|| payload.get("data").and_then(|data| data.get("tokens")))
            .cloned();

        let (observation, decision, rollover) =
            if let Some(runtime_cell) = self.rollover_runtime.as_ref() {
                let mut runtime_cell = runtime_cell.borrow_mut();
                let reported_profile = payload
                    .get("provider_id")
                    .and_then(Value::as_str)
                    .zip(payload.get("model_id").and_then(Value::as_str))
                    .map(|(provider, model)| {
                        RuntimeProfile::new(
                            payload
                                .get("agent")
                                .and_then(Value::as_str)
                                .unwrap_or("runtime"),
                            format!("{provider}/{model}"),
                            payload
                                .get("variant")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        )
                    });
                let event = RuntimeContextEvent {
                    execution_id: session.clone().into(),
                    event_id: event_id.clone(),
                    observed_at: now,
                    assistant_message_id: assistant_message_id.clone(),
                    finish: finish.clone(),
                    safe_boundary,
                    reported_usage: Some(RuntimeContextUsage::from_value(step_tokens.as_ref())),
                    reported_profile,
                };
                let runtime: &mut dyn RuntimeAdapter = runtime_cell.as_lifecycle();
                match runtime.observe_context(&event) {
                    Ok(runtime_observation) => {
                        let observation = ContextObservation::from_runtime(runtime_observation);
                        match self.controller.observe_context(
                            &session,
                            observation.clone(),
                            runtime,
                            self.rollover_profile
                                .as_ref()
                                .expect("runtime implies a profile"),
                        ) {
                            Ok(result) => {
                                let decision = result.decision.clone();
                                (observation, decision, Some(result))
                            }
                            Err(error) => {
                                return self.error_outcome(
                                    safe_error(&error.to_string()),
                                    Some(Role::Lead),
                                    Some(session),
                                )
                            }
                        }
                    }
                    Err(error) => {
                        let observation = ContextObservation::unknown(
                            &session,
                            &event_id,
                            now,
                            format!(
                                "runtime context observation unavailable: {}",
                                safe_error(&error.to_string())
                            ),
                        );
                        let decision =
                            self.controller
                                .evaluate_context(&session, observation.clone())
                                .unwrap_or_else(|error| {
                                    crate::orchestration::context_governor::GovernorDecision {
                                state: GovernorState::Unknown,
                                action:
                                    crate::orchestration::context_governor::GovernorAction::Warn,
                                utilization_percent: None,
                                rollover_allowed: false,
                                deferred_for_boundary: false,
                                reason: safe_error(&error.to_string()),
                            }
                                });
                        (observation, decision, None)
                    }
                }
            } else if let Some(runtime_cell) = self.lifecycle_runtime.as_ref() {
                let mut runtime_cell = runtime_cell.borrow_mut();
                let reported_profile = payload
                    .get("provider_id")
                    .and_then(Value::as_str)
                    .zip(payload.get("model_id").and_then(Value::as_str))
                    .map(|(provider, model)| {
                        RuntimeProfile::new(
                            payload
                                .get("agent")
                                .and_then(Value::as_str)
                                .unwrap_or("runtime"),
                            format!("{provider}/{model}"),
                            payload
                                .get("variant")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        )
                    });
                let event = RuntimeContextEvent {
                    execution_id: session.clone().into(),
                    event_id: event_id.clone(),
                    observed_at: now,
                    assistant_message_id: assistant_message_id.clone(),
                    finish: finish.clone(),
                    safe_boundary,
                    reported_usage: Some(RuntimeContextUsage::from_value(step_tokens.as_ref())),
                    reported_profile,
                };
                match runtime_cell.observe_context(&event) {
                    Ok(runtime_observation) => {
                        let observation = ContextObservation::from_runtime(runtime_observation);
                        match self.controller.observe_context(
                            &session,
                            observation.clone(),
                            &mut **runtime_cell,
                            self.rollover_profile
                                .as_ref()
                                .expect("runtime implies a profile"),
                        ) {
                            Ok(result) => {
                                let decision = result.decision.clone();
                                (observation, decision, Some(result))
                            }
                            Err(error) => {
                                return self.error_outcome(
                                    safe_error(&error.to_string()),
                                    Some(Role::Lead),
                                    Some(session),
                                )
                            }
                        }
                    }
                    Err(error) => {
                        let observation = ContextObservation {
                            session_id: session.clone(),
                            ..ContextObservation::default()
                        };
                        let decision = crate::orchestration::context_governor::GovernorDecision {
                            state: GovernorState::Unknown,
                            action: self.controller.config().context_governor.unknown,
                            utilization_percent: None,
                            rollover_allowed: false,
                            deferred_for_boundary: false,
                            reason: error.to_string(),
                        };
                        (observation, decision, None)
                    }
                }
            } else {
                let observation = ContextObservation::unknown(
                &session,
                &event_id,
                now,
                "no invocation-scoped runtime adapter is available; context telemetry is unknown",
            );
                let decision = self
                    .controller
                    .evaluate_context(&session, observation.clone())
                    .unwrap_or_else(|error| {
                        crate::orchestration::context_governor::GovernorDecision {
                            state: GovernorState::Unknown,
                            action: crate::orchestration::context_governor::GovernorAction::Warn,
                            utilization_percent: None,
                            rollover_allowed: false,
                            deferred_for_boundary: false,
                            reason: safe_error(&error.to_string()),
                        }
                    });
                (observation, decision, None)
            };

        let value = context_governance_value(&observation, &decision, rollover.as_ref());
        BridgeOutcome {
            value,
            metrics: OrchestrationMetrics::default(),
            outcome: if decision.state == GovernorState::Unknown {
                Outcome::Unknown
            } else {
                Outcome::Success
            },
            role: Some(Role::Lead.as_str().to_string()),
            session_id: Some(session),
            task_type: "context".to_string(),
        }
    }

    fn tool_before(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        let args = payload.get("args").cloned().unwrap_or(Value::Null);
        let subagent = subagent(&args).unwrap_or_default();
        let Some(role) = Role::parse(&subagent) else {
            return BridgeOutcome {
                value: json!({
                    "ok": false,
                    "error": format!("unknown subagent role: {subagent}"),
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: None,
                session_id: Some(session_id),
                task_type: "orchestration".to_string(),
            };
        };
        let task = task_text(&args);
        // Production cutover: a delegated execution that belongs to a live
        // canonical Mission is dispatched as a real child Job + Attempt and
        // returns its durable witness. The legacy hand-off capsule is still
        // projected for the model, but it never decides which Attempt a result
        // belongs to.
        if let Some(canonical) = self.canonical_dispatch(&session_id, role, &task, &args) {
            return canonical;
        }
        self.error_outcome(
            "delegation requires current canonical Attempt authority".into(),
            Some(role),
            Some(session_id),
        )
    }

    fn tool_after(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        let args = payload.get("args").cloned().unwrap_or(Value::Null);
        let subagent = subagent(&args).unwrap_or_default();
        let Some(role) = Role::parse(&subagent) else {
            return BridgeOutcome {
                value: json!({
                    "ok": false,
                    "error": format!("unknown subagent role: {subagent}"),
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: None,
                session_id: Some(session_id),
                task_type: "orchestration".to_string(),
            };
        };
        // The witness travels with the exact subagent invocation. When it is
        // present the canonical Attempt owns the result: the legacy
        // session/phase correlation path is not consulted at all.
        if let Some(witness) = witness_from(&args) {
            return self.canonical_result(&session_id, role, witness, payload);
        }
        self.error_outcome(
            "result delivery requires a canonical execution witness".into(),
            Some(role),
            Some(session_id),
        )
    }

    /// The canonical Attempt this caller session is bound to. SQLite owns the
    /// lookup; no Mission, Job or ready-order projection participates.
    fn canonical_authority(&self, session_id: &str) -> Option<CanonicalAuthority> {
        if session_id.is_empty() {
            return None;
        }
        let repository = DomainRepository::open(self.controller.root()).ok()?;
        let project = repository.ensure_project(self.controller.root()).ok()?;
        let authority = repository
            .authority_for_binding(&project.id, session_id)
            .ok()??;
        Some(CanonicalAuthority { authority })
    }

    /// Canonical child dispatch. The returned witness is the only identity the
    /// completion path accepts for this invocation.
    fn canonical_dispatch(
        &self,
        session_id: &str,
        role: Role,
        task: &str,
        args: &Value,
    ) -> Option<BridgeOutcome> {
        let authority = self.canonical_authority(session_id)?;
        // Only an authoritative caller may create work. A stale or fenced
        // session is absent from this path.
        let objective: String = task.chars().take(4096).collect();
        let dependencies = canonical_dependencies_from(args);
        let executor = subagent(args).unwrap_or_else(|| role.agent().unwrap_or_default());
        let mut repository = match DomainRepository::open(self.controller.root()) {
            Ok(repository) => repository,
            Err(error) => {
                return Some(self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                ))
            }
        };
        let child = match repository.admit_child(
            &authority.authority,
            &objective,
            &dependencies.iter().map(String::as_str).collect::<Vec<_>>(),
            &executor,
        ) {
            Ok(child) => child,
            Err(error) => {
                return Some(self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                ))
            }
        };
        let call_request = serde_json::to_string(&json!({ "arguments": args }))
            .unwrap_or_else(|_| "{\"arguments\":{}}".to_string());
        let call = match repository.create_call(
            &child.attempt.id,
            Some(&child.executor.id),
            child.attempt.generation,
            true,
            &call_request,
        ) {
            Ok(call) => call,
            Err(error) => {
                return Some(self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                ))
            }
        };
        if let Err(error) =
            repository.start_call(&call.id, &child.attempt.id, child.attempt.generation)
        {
            let _ = repository.fail_call(
                &call.id,
                &child.attempt.id,
                child.attempt.generation,
                "call_start_rejected",
            );
            return Some(self.error_outcome(
                error.to_string(),
                Some(role),
                Some(session_id.to_string()),
            ));
        }
        let witness = canonical_witness(&authority.authority, &child, Some(&call.id));
        let handoff = self.controller.prepare_handoff(session_id, role, task).ok();
        // The OCG-owned envelope travels with the delegated prompt. It is how
        // a worker session recovers *its own* durable Attempt identity on its first
        // prompt admission, so it can create children for its own subtree. It
        // is a structured, delimited block rather than prose the model is
        // asked to reason about, and the bridge always re-validates the Attempt it
        // names before anything changes.
        let envelope = format!(
            "{WITNESS_START}\n{}\n{WITNESS_END}",
            serde_json::to_string(&witness).unwrap_or_default()
        );
        let context = match handoff
            .as_ref()
            .map(|handoff| handoff.dynamic_context.clone())
        {
            Some(body) if !body.is_empty() => format!("{body}\n{envelope}"),
            _ => envelope,
        };
        let metrics = handoff
            .as_ref()
            .map(|handoff| handoff.metrics.clone())
            .unwrap_or_default();
        let mut value = json!({
            "ok": true,
            "event": "tool.execute.before",
            "canonical": true,
            "job_id": child.job.id,
            "attempt_id": child.attempt.id,
            "generation": child.attempt.generation,
            "executor_id": child.executor.id,
            "call_id": call.id,
            "parent_attempt_id": authority.authority.attempt_id,
            "witness": witness.to_json(),
            "context": context,
        });
        if let Some(handoff) = &handoff {
            value["session_id"] = json!(handoff.session_id);
            value["task_id"] = json!(handoff.task_id);
            value["source"] = json!(handoff.source.as_str());
            value["destination"] = json!(handoff.destination.as_str());
        }
        Some(BridgeOutcome {
            value,
            metrics,
            outcome: Outcome::Success,
            role: Some(role.as_str().to_string()),
            session_id: Some(session_id.to_string()),
            task_type: "orchestration".to_string(),
        })
    }

    /// Canonical result handling: the witness decides which Attempt changes state.
    /// A late or duplicate delivery is retained as evidence and reported
    /// honestly; it never mutates the Job.
    fn canonical_result(
        &self,
        session_id: &str,
        role: Role,
        witness: ExecutionWitness,
        payload: &Value,
    ) -> BridgeOutcome {
        let output = result_text(payload);
        if !witness.attempt_id.is_empty() {
            let attempt_id = &witness.attempt_id;
            return self.canonical_call_result(session_id, role, &witness, attempt_id, &output);
        }
        self.error_outcome(
            "missing canonical Attempt identity".into(),
            Some(role),
            Some(session_id.to_string()),
        )
    }

    fn canonical_call_result(
        &self,
        session_id: &str,
        role: Role,
        witness: &ExecutionWitness,
        _attempt_id: &str,
        output: &str,
    ) -> BridgeOutcome {
        let operation = || -> crate::error::Result<Value> {
            let mut repository = DomainRepository::open(self.controller.root())?;
            let call = repository.validate_execution_witness(witness)?;
            let response = serde_json::to_string(
                &serde_json::from_str::<Value>(output)
                    .unwrap_or_else(|_| Value::String(output.to_string())),
            )
            .map_err(|error| crate::error::OcgError::config(error.to_string()))?;
            let current =
                repository.authority(&witness.attempt_id)?.is_some() && call.state == "running";
            let mut verified = repository
                .verification(&witness.call_id)?
                .map(|(passed, _)| passed);
            if current && verified.is_none() {
                let executor = repository
                    .executor(&witness.executor_id)?
                    .ok_or_else(|| crate::error::OcgError::config("missing canonical Executor"))?;
                let execution_role = Role::parse(&executor.kind).ok_or_else(|| {
                    crate::error::OcgError::config("Executor has no verification role")
                })?;
                let evidence = match self.canonical_verification(execution_role) {
                    Some(VerificationOutcome::Passed(report)) => Some((true, report)),
                    Some(VerificationOutcome::Failed(report)) => Some((false, report)),
                    Some(VerificationOutcome::NotRun) | None => None,
                };
                let Some((passed, report)) = evidence else {
                    return Ok(
                        json!({"applied":false,"disposition":"unverified","call_id":witness.call_id,
                        "note":"no trusted verification ran; Attempt remains active"}),
                    );
                };
                repository.record_verification(witness, passed, &report)?;
                verified = repository
                    .verification(&witness.call_id)?
                    .map(|(passed, _)| passed);
            }
            let disposition =
                repository.deliver_result(witness, &response, verified.unwrap_or(false))?;
            if disposition == "authoritative" && matches!(role, Role::Explore | Role::ExploreDeep) {
                self.controller.consume_explore_result(session_id, output)?;
            }
            Ok(json!({"event":"tool.execute.after","canonical":true,
                "call_id":witness.call_id,"attempt_id":witness.attempt_id,"generation":witness.generation,
                "applied":disposition == "authoritative","disposition":disposition,"verified":verified,"context":""}))
        };
        match operation() {
            Ok(value) => BridgeOutcome::ok(value, role, Some(session_id.to_string())),
            Err(error) => {
                self.error_outcome(error.to_string(), Some(role), Some(session_id.to_string()))
            }
        }
    }

    /// Run the configured trusted verification stage for one canonical Attempt.
    /// Returns `(passed, report)`. The report is the durable evidence record;
    /// model output is never used as evidence.
    fn canonical_verification(&self, role: Role) -> Option<VerificationOutcome> {
        let stage = match role {
            Role::Explore | Role::ExploreDeep | Role::Docs => "fast",
            _ => "normal",
        };
        let root = self.controller.root();
        let report =
            crate::verification::runner::execute(&crate::verification::runner::VerifyRequest {
                root,
                config: self.controller.verification(),
                stage: stage.to_string(),
                runner: self.runner,
                clock: self.controller.clock(),
                test_proposal: None,
            })
            .ok()?;
        let value = serde_json::to_value(&report).ok()?;
        Some(match report.overall() {
            crate::verification::result::Overall::Passed => VerificationOutcome::Passed(value),
            crate::verification::result::Overall::Failed => VerificationOutcome::Failed(value),
            // Nothing ran: this is an absence of evidence, not a failure.
            crate::verification::result::Overall::NotRun => VerificationOutcome::NotRun,
        })
    }

    fn lead_output(&self, payload: &Value) -> BridgeOutcome {
        let session_id = session_id(payload);
        let text = payload.get("text").and_then(Value::as_str).unwrap_or("");
        let reject = |message: &str, outcome: Outcome| BridgeOutcome {
            value: json!({"ok": false, "error": message}),
            metrics: OrchestrationMetrics::default(),
            outcome,
            role: None,
            session_id: Some(session_id.clone()),
            task_type: "report".to_string(),
        };
        if !self.reports.latest_lead_output.enabled {
            return BridgeOutcome {
                value: json!({"ok": false, "disabled": true, "context": ""}),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: None,
                session_id: Some(session_id),
                task_type: "report".to_string(),
            };
        }
        if text.trim().is_empty() {
            return reject("empty lead.output payload", Outcome::Unknown);
        }
        let execution_id = RuntimeExecutionId::new(session_id.clone());
        match self.controller.is_current_execution(&execution_id) {
            Ok(true) => {}
            Ok(false) => {
                return reject(
                    "lead.output is not associated with the current Mission execution",
                    Outcome::Unknown,
                )
            }
            Err(error) => return reject(&safe_error(&error.to_string()), Outcome::Failure),
        }
        match crate::reports::write_latest_lead_output(self.controller.root(), text) {
            Ok(path) => BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "lead.output",
                    "session_id": session_id,
                    "bytes": text.len(),
                    "path": path.to_string_lossy(),
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Success,
                role: Some(Role::Lead.as_str().to_string()),
                session_id: Some(session_id),
                task_type: "report".to_string(),
            },
            Err(error) => reject(&safe_error(&error.to_string()), Outcome::Failure),
        }
    }

    fn error_outcome(
        &self,
        message: String,
        role: Option<Role>,
        session_id: Option<String>,
    ) -> BridgeOutcome {
        BridgeOutcome {
            value: json!({"ok": false, "error": safe_error(&message)}),
            metrics: OrchestrationMetrics::default(),
            outcome: Outcome::Unknown,
            role: role.map(|role| role.as_str().to_string()),
            session_id,
            task_type: "orchestration".to_string(),
        }
    }

    fn record(&self, outcome: &BridgeOutcome, duration_ms: u64) {
        let timestamp = self.controller.now_unix();
        let mut event = Event::new(
            crate::telemetry::Event::hashed_task_id(&format!(
                "orchestration|{}|{}",
                outcome.session_id.as_deref().unwrap_or(""),
                outcome.role.as_deref().unwrap_or("")
            )),
            timestamp,
        );
        event.session_id = outcome.session_id.clone();
        event.task_type = Some(outcome.task_type.clone());
        event.role = outcome.role.clone();
        event.duration_ms = duration_ms;
        event.orchestration = outcome.metrics.clone();
        event.outcome = outcome.outcome;
        for warning in telemetry::record(self.controller.root(), &self.telemetry, event) {
            eprintln!("ocg: warning: {warning}");
        }
    }
}

fn context_governance_value(
    observation: &ContextObservation,
    decision: &crate::orchestration::context_governor::GovernorDecision,
    result: Option<&ContextGovernanceResult>,
) -> Value {
    let mut value = json!({
        "ok": true,
        "event": "context.observe",
        "session_id": observation.session_id,
        "event_id": observation.event_id,
        "observation": observation,
        "decision": decision,
    });
    if let Some(result) = result {
        value["rollover_status"] = result
            .rollover_status
            .as_ref()
            .map(|status| json!(status.as_str()))
            .unwrap_or(Value::Null);
        value["artifact_status"] = result
            .artifact_status
            .as_ref()
            .map(|status| json!(status.as_str()))
            .unwrap_or(Value::Null);
        value["artifact_id"] = result
            .artifact_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
        value["source_session_id"] = result
            .source_session_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
        value["target_session_id"] = result
            .target_session_id
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
        value["note"] = result
            .note
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null);
    } else {
        value["rollover_status"] = Value::Null;
        value["artifact_status"] = Value::Null;
        value["artifact_id"] = Value::Null;
        value["source_session_id"] = json!(observation.session_id);
        value["target_session_id"] = Value::Null;
        value["note"] = if decision.state == GovernorState::Disabled {
            json!("context governor is disabled; no telemetry or rollover was attempted")
        } else {
            json!("runtime context was unknown; no rollover was attempted")
        };
    }
    value
}

fn session_id(payload: &Value) -> String {
    for key in ["session_id", "sessionID", "sessionId"] {
        if let Some(value) = payload.get(key).and_then(Value::as_str) {
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    "default".to_string()
}

/// Classify prompt authority from durable Mission ownership only.
///
/// A Mission lookup is not itself authority: a returned Mission is accepted
/// only when its current durable execution binding equals this session. This
/// protects the first-prompt fallback from promoting a worker/session view
/// that merely belongs to the same Mission. No runtime agent name is read.
fn is_root_lead_execution(
    _controller: &Controller<'_>,
    runtime: &mut dyn BridgeRuntimeClient,
    session_id: &str,
) -> crate::error::Result<bool> {
    let execution_id = RuntimeExecutionId::new(session_id.to_string());
    // A worker can be a runtime execution descended from the Mission owner.
    // Its session may be visible to the runtime while no Mission is directly
    // bound to that child. A verified lineage prevents the first-prompt
    // fallback from treating that child as the root Lead.
    let current = execution_id.clone();
    match runtime.as_lifecycle().execution_parent(&current) {
        Ok(Some(_parent)) => return Ok(false),
        Ok(None) => {}
        Err(error) => {
            return Err(crate::error::OcgError::config(format!(
                "runtime lineage lookup failed: {error}"
            )))
        }
    }

    Ok(true)
}

fn subagent(args: &Value) -> Option<String> {
    for key in ["subagent_type", "subagentType", "agent", "subagent"] {
        if let Some(value) = args.get(key).and_then(Value::as_str) {
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn task_text(args: &Value) -> String {
    for key in ["prompt", "description"] {
        if let Some(value) = args.get(key).and_then(Value::as_str) {
            if !value.trim().is_empty() {
                return value.to_string();
            }
        }
    }
    String::new()
}

fn result_text(payload: &Value) -> String {
    if let Some(text) = payload.get("result").and_then(Value::as_str) {
        return text.to_string();
    }
    if let Some(result) = payload.get("result") {
        if let Some(text) = result.get("output").and_then(Value::as_str) {
            return text.to_string();
        }
    }
    if let Some(text) = payload.get("output").and_then(Value::as_str) {
        return text.to_string();
    }
    String::new()
}

enum VerificationOutcome {
    Passed(Value),
    Failed(Value),
    NotRun,
}

/// The durable canonical authority of a caller session.
struct CanonicalAuthority {
    authority: AttemptAuthority,
}

/// Delimiters of the OCG-owned dispatch-witness envelope.
///
/// The envelope is a structured, OCG-owned block appended to a delegated
/// prompt. It is the only channel by which a worker session recovers the
/// durable identity of the Attempt that created it. It is never parsed for
/// authority: the bridge re-validates the Attempt the witness names.
pub const WITNESS_START: &str = "<<<OCG:ATTEMPT_WITNESS v1>>>";
pub const WITNESS_END: &str = "<<<OCG:ATTEMPT_WITNESS:END>>>";

/// Read the dispatch witness a delegated prompt carries, if any.
pub fn witness_from_prompt(text: &str) -> Option<ExecutionWitness> {
    let start = text.find(WITNESS_START)? + WITNESS_START.len();
    let end = text[start..].find(WITNESS_END)? + start;
    let value: Value = serde_json::from_str(text[start..end].trim()).ok()?;
    ExecutionWitness::from_json(&value).ok()
}

/// Read the dispatch witness an adapter attached to this exact invocation.
///
/// The adapter puts the witness in a structured field the host returns
/// unchanged. Prompt text is never parsed for identity; a witness that cannot
/// be read as a complete durable witness is simply absent, and the caller then
/// falls through to the compatibility path rather than guessing.
fn witness_from(args: &Value) -> Option<ExecutionWitness> {
    let value = args.get("ocg_witness")?;
    ExecutionWitness::from_json(value).ok()
}

fn canonical_dependencies_from(args: &Value) -> Vec<String> {
    args.get("ocg_dependencies")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn canonical_witness(
    _parent: &AttemptAuthority,
    admission: &crate::orchestration::domain::CanonicalAdmission,
    call_id: Option<&str>,
) -> ExecutionWitness {
    ExecutionWitness {
        job_id: admission.job.id.clone(),
        attempt_id: admission.attempt.id.clone(),
        executor_id: admission.executor.id.clone(),
        call_id: call_id.unwrap_or_default().to_string(),
        generation: admission.attempt.generation,
    }
}

fn safe_error(message: &str) -> String {
    if crate::telemetry::task::is_secret_like(message) {
        "orchestration bridge error (details withheld)".to_string()
    } else {
        message.to_string()
    }
}
