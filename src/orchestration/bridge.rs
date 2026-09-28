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
//! It is safe for tests to drive with fake payloads and a fake capture runner;
//! no model or network is involved.

use crate::orchestration::context_governor::{ContextObservation, GovernorState};
use crate::orchestration::controller::{
    BuildDecision, ContextGovernanceResult, Controller, HandoffOutcome,
};
use crate::orchestration::handoff::Role;
use crate::orchestration::mission;
use crate::orchestration::substrate::{
    DispatchWitness, RunContract, RunId, RunState, SubstrateRepository, WitnessDisposition,
    WorkNodeId,
};
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
    /// `role -> provider/model`. A canonical child Run's frozen contract must
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

    /// Attach the resolved worker routing table so a canonical child Run can
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
            | "work.result.late"
            | "work.inspect" => self.canonical_work(event, payload),
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

    /// Explicit canonical lifecycle boundary. Callers carry the Run witness;
    /// this path never consults legacy replay, session state or task phases.
    fn canonical_work(&self, event: &str, payload: &Value) -> BridgeOutcome {
        let operation = || -> crate::error::Result<Value> {
            let mission = payload
                .get("mission_id")
                .and_then(Value::as_str)
                .ok_or_else(|| crate::error::OcgError::config("missing mission_id"))?;
            let node = || -> crate::error::Result<WorkNodeId> {
                Ok(WorkNodeId(
                    payload
                        .get("node_id")
                        .and_then(Value::as_u64)
                        .and_then(|id| usize::try_from(id).ok())
                        .ok_or_else(|| crate::error::OcgError::config("missing node_id"))?,
                ))
            };
            let run = || -> crate::error::Result<RunId> {
                Ok(RunId(
                    payload
                        .get("run_id")
                        .and_then(Value::as_u64)
                        .and_then(|id| usize::try_from(id).ok())
                        .ok_or_else(|| crate::error::OcgError::config("missing run_id"))?,
                ))
            };
            let binding = || -> crate::error::Result<RuntimeExecutionId> {
                Ok(RuntimeExecutionId::new(
                    payload
                        .get("runtime_execution_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| {
                            crate::error::OcgError::config("missing runtime_execution_id")
                        })?,
                ))
            };
            let contract = || -> crate::error::Result<RunContract> {
                let value = payload
                    .get("contract")
                    .ok_or_else(|| crate::error::OcgError::config("missing RunContract"))?;
                serde_json::from_value(value.clone())
                    .map_err(|_| crate::error::OcgError::config("invalid RunContract"))
            };
            match event {
                "work.mission.create" => {
                    let lead = self.lead_contract.as_ref().ok_or_else(|| {
                        crate::error::OcgError::config("resolved Lead contract unavailable")
                    })?;
                    let runtime_cell = self.rollover_runtime.as_ref().ok_or_else(|| {
                        crate::error::OcgError::config("Lead runtime unavailable")
                    })?;
                    let runtime_id = binding()?;
                    let mut client = runtime_cell.borrow_mut();
                    match client.as_lifecycle().execution_parent(&runtime_id) {
                        Ok(None) => {}
                        _ => {
                            return Err(crate::error::OcgError::config(
                                "root runtime ancestry unavailable",
                            ))
                        }
                    }
                    crate::runtime::compat::ensure_existing_session_lead(
                        client.as_session(),
                        runtime_id.as_str(),
                        lead,
                    )
                    .map_err(|_| crate::error::OcgError::config("Lead enforcement failed"))?;
                    drop(client);
                    let payload_text = payload.get("payload").and_then(Value::as_str).unwrap_or("");
                    let run = self.controller.create_work_mission(
                        mission,
                        payload_text,
                        RunContract {
                            executor: lead.agent.clone(),
                            model: lead.full_model_id(),
                            role: Role::Lead.as_str().into(),
                        },
                        &runtime_id,
                    )?;
                    Ok(witness_json(json!({"root_node_id":0}), &run))
                }
                "work.child.create" => {
                    let deps = payload
                        .get("dependencies")
                        .and_then(Value::as_array)
                        .map(|deps| {
                            deps.iter()
                                .map(|id| {
                                    id.as_u64()
                                        .and_then(|id| usize::try_from(id).ok())
                                        .map(WorkNodeId)
                                        .ok_or_else(|| {
                                            crate::error::OcgError::config("invalid Dependency")
                                        })
                                })
                                .collect::<crate::error::Result<Vec<_>>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let child = self.controller.create_child_work(
                        mission,
                        node()?,
                        run()?,
                        payload.get("payload").and_then(Value::as_str).unwrap_or(""),
                        &deps,
                    )?;
                    Ok(json!({"node_id":child.0}))
                }
                "work.dispatch" => {
                    let run = self.controller.dispatch_work(
                        mission,
                        node()?,
                        run()?,
                        contract()?,
                        &binding()?,
                    )?;
                    Ok(witness_json(json!({"node_id":node()?.0}), &run))
                }
                "work.finish" => {
                    let outcome = match payload.get("outcome").and_then(Value::as_str) {
                        Some("completed") => RunState::Completed,
                        Some("failed") => RunState::Failed,
                        Some("cancelled") => RunState::Cancelled,
                        Some("superseded") => RunState::Superseded,
                        _ => {
                            return Err(crate::error::OcgError::config(
                                "invalid terminal Run outcome",
                            ))
                        }
                    };
                    self.controller.finish_work(
                        mission,
                        node()?,
                        run()?,
                        outcome,
                        payload.get("result").and_then(Value::as_str),
                    )?;
                    Ok(
                        json!({"node_id":node()?.0,"run_id":run()?.0,"outcome":outcome_name(outcome)}),
                    )
                }
                "work.replace" => {
                    let replacement = self.controller.replace_work_run(
                        mission,
                        node()?,
                        run()?,
                        contract()?,
                        &binding()?,
                    )?;
                    Ok(witness_json(json!({"node_id":node()?.0}), &replacement))
                }
                "work.result.late" => {
                    self.controller.record_late_work_result(
                        mission,
                        run()?,
                        payload.get("result").and_then(Value::as_str).unwrap_or(""),
                    )?;
                    Ok(json!({"run_id":run()?.0,"reconciled":true}))
                }
                "work.inspect" => {
                    // Read-only canonical inspection: the full ownership tree,
                    // dependency DAG, every Run generation, frozen contract,
                    // witness, verification evidence and event sequence.
                    Ok(self.controller.inspect_work(mission)?.ok_or_else(|| {
                        crate::error::OcgError::config("unknown canonical Mission")
                    })?)
                }
                "work.ready" => {
                    let state = self.controller.load_work_mission(mission)?.ok_or_else(|| {
                        crate::error::OcgError::config("unknown canonical Mission")
                    })?;
                    let ready: Vec<_> = state
                        .ready(self.controller.now_unix())
                        .iter()
                        .map(|id| id.0)
                        .collect();
                    Ok(json!({
                        "root_node_id":state.root().0,
                        "ready":ready,
                        "work_nodes":state.work_nodes.iter_enumerated().map(|(id,n)| json!({"node_id":id.0,"parent_node_id":n.parent_node_id.map(|p|p.0),"spawned_by_run_id":n.spawned_by_run_id.map(|r|r.0),"state":format!("{:?}",n.state).to_lowercase(),"generation":n.generation,"active_run_id":n.active_run_id.map(|r|r.0),"payload":n.payload})).collect::<Vec<_>>(),
                        "runs":state.runs.iter_enumerated().map(|(id,r)| json!({"run_id":id.0,"node_id":r.node_id.0,"generation":r.generation,"state":outcome_name(r.state),"runtime_execution_id":r.runtime_execution_id,"host_session_id":r.host_session_id,"contract":r.contract()})).collect::<Vec<_>>(),
                        "dependencies":state.dependencies.iter().map(|d|json!({"node_id":d.node_id.0,"depends_on_node_id":d.depends_on_node_id.0})).collect::<Vec<_>>()
                    }))
                }
                _ => unreachable!(),
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

        // 2. Determine Root Lead ownership via durable Mission binding.
        //    The runtime's observed agent name is MUTABLE runtime state
        //    and MUST NOT be used as the authority test (P0-J).
        //    A session is an OCG Root Lead session if and only if it
        //    is the current execution binding for a durable Mission.
        //    If no mission exists yet (first prompt), we enforce
        //    conservatively since the bridge carries a canonical
        //    Lead contract.
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
        if self.controller.config().canonical_execution {
            if let Some(present) = witness_from_prompt(&text) {
                if let Err(error) = self.controller.bind_work_runtime(&present, &session_id) {
                    return BridgeOutcome::err(
                        format!("canonical binding refused: {error}"),
                        Role::Lead,
                        Some(session_id),
                    );
                }
            }
        }

        // 5. ONLY NOW: admit user task (OCG task state advances)
        let admission = match self.controller.admit_user_task(&session_id, &text) {
            Ok(a) => a,
            Err(e) => return BridgeOutcome::err(e.to_string(), Role::Lead, Some(session_id)),
        };

        // 5b. Production cutover: a genuinely admitted root Lead prompt also
        // establishes the canonical Mission — exactly one root WorkNode and one
        // Lead Run, bound atomically to this session, with its dispatch witness
        // committed before any delegated execution starts. The legacy task
        // record stays a compatibility projection; it cannot make an execution
        // decision the substrate owns.
        let canonical = if is_root_lead {
            match self.admit_canonical_mission(&session_id, &text) {
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
            None
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

    /// Establish (or recover) the canonical Mission for an admitted root Lead
    /// session. The Mission id is derived from the session binding, so a
    /// restart re-attaches to the same durable WorkNode tree instead of
    /// inventing a new one.
    fn admit_canonical_mission(
        &self,
        session_id: &str,
        text: &str,
    ) -> crate::error::Result<Option<Value>> {
        if !self.controller.config().canonical_execution {
            return Ok(None);
        }
        let mission_id = canonical_mission_id(session_id)
            .ok_or_else(|| crate::error::OcgError::config("invalid canonical Mission identity"))?;
        let mut repo = SubstrateRepository::open(self.controller.root())?;
        if repo.load(&mission_id)?.is_some() {
            // Already established: report the existing root authority.
            let witness = repo
                .witness_for_run(&mission_id, RunId(0))?
                .ok_or_else(|| {
                    crate::error::OcgError::config("canonical root Run witness missing")
                })?;
            if witness.runtime_execution_id != session_id
                || witness.work_node_id != 0
                || repo.validate_witness(&witness)? != WitnessDisposition::Authoritative
            {
                return Err(crate::error::OcgError::config(
                    "canonical root Run is not authoritative",
                ));
            }
            return Ok(Some(json!({
                "mission_id": mission_id.as_str(),
                "root_node_id": 0,
                "run_id": 0,
                "reused": true,
                "witness": witness.to_json(),
            })));
        }
        let lead = self
            .lead_contract
            .as_ref()
            .ok_or_else(|| crate::error::OcgError::config("canonical Lead contract missing"))?;
        let contract = RunContract {
            executor: lead.agent.clone(),
            model: lead.full_model_id(),
            role: Role::Lead.as_str().to_string(),
        };
        let objective: String = text.chars().take(4096).collect();
        let witness = repo.create_live_mission(
            &mission_id,
            &objective,
            contract,
            session_id,
            self.controller.now_unix(),
        )?;
        if witness.runtime_execution_id != session_id
            || repo.validate_witness(&witness)? != WitnessDisposition::Authoritative
        {
            return Err(crate::error::OcgError::config(
                "canonical root Run is not authoritative",
            ));
        }
        Ok(Some(json!({
            "mission_id": mission_id.as_str(),
            "root_node_id": witness.work_node_id,
            "run_id": witness.run_id,
            "reused": false,
            "witness": witness.to_json(),
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
        // canonical Mission is dispatched as a real child WorkNode + Run and
        // returns its durable witness. The legacy hand-off capsule is still
        // projected for the model, but it never decides which Run a result
        // belongs to.
        if let Some(canonical) = self.canonical_dispatch(&session_id, role, &task, &args) {
            return canonical;
        }
        match self.controller.prepare_handoff(&session_id, role, &task) {
            Ok(handoff) => BridgeOutcome {
                value: handoff_value("tool.execute.before", &handoff),
                metrics: handoff.metrics.clone(),
                outcome: Outcome::Success,
                role: Some(role.as_str().to_string()),
                session_id: Some(handoff.session_id.clone()),
                task_type: "orchestration".to_string(),
            },
            Err(error) => self.error_outcome(error.to_string(), Some(role), Some(session_id)),
        }
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
        // present the canonical Run owns the result: the legacy
        // session/phase correlation path is not consulted at all.
        if let Some(witness) = witness_from(&args) {
            return self.canonical_result(&session_id, role, witness, payload);
        }
        match role {
            Role::Explore | Role::ExploreDeep => self.after_explore(&session_id, payload),
            Role::Build => self.after_build(&session_id),
            _ => BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "tool.execute.after",
                    "context": "",
                    "note": format!("no orchestration action for {} completion", role.as_str()),
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: Some(role.as_str().to_string()),
                session_id: Some(session_id),
                task_type: "orchestration".to_string(),
            },
        }
    }

    /// The canonical Mission, WorkNode and Run this caller session is bound
    /// to. It is a durable binding lookup, never a prompt, role or ready-order
    /// guess.
    fn canonical_authority(&self, session_id: &str) -> Option<CanonicalAuthority> {
        if !self.controller.config().canonical_execution || session_id.is_empty() {
            return None;
        }
        let mut repo = SubstrateRepository::open(self.controller.root()).ok()?;
        for mission in repo.missions_by_binding(session_id).ok()? {
            if let Some((node, run)) = repo
                .authoritative_run_by_binding(&mission, session_id)
                .ok()
                .flatten()
            {
                return Some(CanonicalAuthority {
                    mission_id: mission.as_str().to_string(),
                    node_id: node.0,
                    run_id: run.0,
                });
            }
        }
        None
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
        // session never reaches this path at all.
        let node = WorkNodeId(authority.node_id);
        let run = RunId(authority.run_id);
        // The caller must still name a live WorkNode. `canonical_authority`
        // already proved the Run is authoritative, so this is a cheap shape
        // check rather than a second authority decision.
        let parent = self
            .controller
            .load_work_mission(&authority.mission_id)
            .ok()
            .flatten()?;
        parent.work_nodes.get(node)?;
        // A delegated objective is bounded and never the identity.
        let objective: String = task.chars().take(4096).collect();
        let dependencies = dependencies_from(args);
        let child = match self.controller.create_child_work(
            &authority.mission_id,
            node,
            run,
            &objective,
            &dependencies,
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
        // The frozen contract records the model the delegated agent will
        // actually use. OCG writes the worker routing table into the generated
        // agent config, so the routed model is the real one; the Lead model is
        // only a visible fallback when the role has no configured model.
        let executor = subagent(args).unwrap_or_else(|| role.agent().unwrap_or_default());
        let model = self
            .routing
            .as_ref()
            .and_then(|routing| routing.model_for(role.routing_role()))
            .map(str::to_string)
            .or_else(|| self.lead_contract.as_ref().map(|lead| lead.full_model_id()))
            .unwrap_or_else(|| role.as_str().to_string());
        let contract = RunContract {
            executor,
            model,
            role: role.as_str().to_string(),
        };
        // The child Run's runtime binding is provisional until the host
        // reports the concrete subagent session; the witness is durable now.
        let planned = format!(
            "ocg-pending-{}-{}-{}",
            authority.mission_id,
            child.0,
            role.as_str()
        );
        let witness = match self.controller.dispatch_work(
            &authority.mission_id,
            child,
            run,
            contract,
            &RuntimeExecutionId::new(planned),
        ) {
            Ok(witness) => witness,
            Err(error) => {
                return Some(self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                ))
            }
        };
        let handoff = self.controller.prepare_handoff(session_id, role, task).ok();
        // The OCG-owned envelope travels with the delegated prompt. It is how
        // a worker session recovers *its own* durable Run identity on its first
        // prompt admission, so it can create children for its own subtree. It
        // is a structured, delimited block rather than prose the model is
        // asked to reason about, and the bridge always re-validates the Run it
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
            "mission_id": authority.mission_id,
            "node_id": child.0,
            "parent_node_id": authority.node_id,
            "parent_run_id": authority.run_id,
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

    /// Canonical result handling: the witness decides which Run changes state.
    /// A late or duplicate delivery is retained as evidence and reported
    /// honestly; it never mutates the WorkNode.
    fn canonical_result(
        &self,
        session_id: &str,
        role: Role,
        witness: DispatchWitness,
        payload: &Value,
    ) -> BridgeOutcome {
        let output = result_text(payload);
        let host_session = payload
            .get("host_session_id")
            .and_then(Value::as_str)
            .or_else(|| {
                payload
                    .get("result")
                    .and_then(|result| result.get("sessionID"))
                    .and_then(Value::as_str)
            })
            .unwrap_or("");
        if !host_session.is_empty() {
            if let Err(error) = self.controller.bind_work_runtime(&witness, host_session) {
                return self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                );
            }
        }
        let disposition = match self.controller.validate_work_witness(&witness) {
            Ok(disposition) => disposition,
            Err(error) => {
                return self.error_outcome(
                    error.to_string(),
                    Some(role),
                    Some(session_id.to_string()),
                )
            }
        };
        if !disposition.is_authoritative() {
            // Retain the evidence and report that authority was refused. The
            // canonical substrate decides whether it is kept as late evidence.
            let recorded = self
                .controller
                .finish_work_witness(&witness, RunState::Failed, &output);
            return BridgeOutcome {
                value: json!({
                    "ok": true,
                    "event": "tool.execute.after",
                    "canonical": true,
                    "witness": witness.to_json(),
                    "disposition": disposition.text(),
                    "applied": false,
                    "note": "stale delivery retained as non-authoritative evidence",
                    "outcome": recorded.as_ref().map(|outcome| outcome.disposition.clone()).unwrap_or_else(|_| disposition.text().to_string()),
                    "context": "",
                }),
                metrics: OrchestrationMetrics::default(),
                outcome: Outcome::Unknown,
                role: Some(role.as_str().to_string()),
                session_id: Some(session_id.to_string()),
                task_type: "orchestration".to_string(),
            };
        }
        // Independent trusted verification, separate from model output. The
        // three outcomes stay distinct: passing completes the Run, failing fails
        // it, and *not running* neither completes nor fails anything. A project
        // with no configured stage must not silently fail real work, nor let it
        // pass unverified.
        // `None` becomes the honest "unverified" outcome below, so every path
        // either returns early or sets `verified` before it is read.
        let verified = match self.canonical_verification(role) {
            Some(VerificationOutcome::Passed(report)) => {
                let commands = verification_commands(&report);
                if let Err(error) = self
                    .controller
                    .record_work_verification(&witness, true, &report, &commands)
                {
                    return self.error_outcome(
                        error.to_string(),
                        Some(role),
                        Some(session_id.to_string()),
                    );
                }
                true
            }
            Some(VerificationOutcome::Failed(report)) => {
                let commands = verification_commands(&report);
                if let Err(error) = self
                    .controller
                    .record_work_verification(&witness, false, &report, &commands)
                {
                    return self.error_outcome(
                        error.to_string(),
                        Some(role),
                        Some(session_id.to_string()),
                    );
                }
                false
            }
            Some(VerificationOutcome::NotRun) | None => {
                // No trusted command ran, so there is no evidence either way.
                // Record that explicitly and leave the Run active: the caller
                // can configure verification and redeliver, and nothing is
                // completed on an unchecked result.
                if let Err(error) = self.controller.record_work_verification(
                    &witness,
                    false,
                    &json!({"evidence": "not-run", "note": "no trusted verification stage is configured for this role"}),
                    &[],
                ) {
                    return self.error_outcome(
                        error.to_string(),
                        Some(role),
                        Some(session_id.to_string()),
                    );
                }
                return BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "tool.execute.after",
                        "canonical": true,
                        "witness": witness.to_json(),
                        "disposition": "authoritative",
                        "applied": false,
                        "verified": false,
                        "note": "no trusted verification stage is configured; the Run stays active until the result is verifiable",
                        "context": "ocg: this result was not verified. Configure a verification stage before completing it.",
                    }),
                    metrics: OrchestrationMetrics::default(),
                    outcome: Outcome::Unknown,
                    role: Some(role.as_str().to_string()),
                    session_id: Some(session_id.to_string()),
                    task_type: "orchestration".to_string(),
                };
            }
        };
        let outcome_state = if verified {
            RunState::Completed
        } else {
            RunState::Failed
        };
        match self
            .controller
            .finish_work_witness(&witness, outcome_state, &output)
        {
            Ok(outcome) => {
                let note = if verified {
                    format!(
                        "ocg verification: canonical Run {} completed with passing evidence",
                        witness.run_id
                    )
                } else {
                    format!(
                        "ocg verification: canonical Run {} did not complete; trusted verification evidence is required for success",
                        witness.run_id
                    )
                };
                BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "tool.execute.after",
                        "canonical": true,
                        "witness": witness.to_json(),
                        "disposition": outcome.disposition,
                        "applied": outcome.disposition == "authoritative",
                        "node_state": outcome.node_state,
                        "run_state": outcome.run_state,
                        "verification": verified,
                        "context": note,
                    }),
                    metrics: OrchestrationMetrics::default(),
                    outcome: if verified {
                        Outcome::Success
                    } else {
                        Outcome::Failure
                    },
                    role: Some(role.as_str().to_string()),
                    session_id: Some(session_id.to_string()),
                    task_type: "orchestration".to_string(),
                }
            }
            Err(error) => {
                self.error_outcome(error.to_string(), Some(role), Some(session_id.to_string()))
            }
        }
    }

    /// Run the configured trusted verification stage for one canonical Run.
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

    fn after_explore(&self, session_id: &str, payload: &Value) -> BridgeOutcome {
        let output = result_text(payload);
        match self.controller.consume_explore_result(session_id, &output) {
            Ok(digest) => {
                let context = format!(
                    "explore result captured ({}): {} finding(s), {} location(s){}",
                    if digest.structured {
                        "structured JSON"
                    } else {
                        "deterministic fallback"
                    },
                    digest.findings.len(),
                    digest.locations.len(),
                    digest
                        .checkpoint_id
                        .as_deref()
                        .map(|id| format!(", checkpoint {id}"))
                        .unwrap_or_default()
                );
                BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "tool.execute.after",
                        "session_id": digest.session_id,
                        "task_id": digest.task_id,
                        "context": context,
                        "findings": digest.findings.len(),
                        "checkpoint": digest.checkpoint_id,
                    }),
                    metrics: digest.metrics,
                    outcome: Outcome::Success,
                    role: Some(Role::Explore.as_str().to_string()),
                    session_id: Some(digest.session_id.clone()),
                    task_type: "orchestration".to_string(),
                }
            }
            Err(error) => self.error_outcome(
                error.to_string(),
                Some(Role::Explore),
                Some(session_id.to_string()),
            ),
        }
    }

    fn after_build(&self, session_id: &str) -> BridgeOutcome {
        match self.controller.after_build(session_id, self.runner, None) {
            Ok(outcome) => {
                let (context, telemetry_outcome) = build_feedback(&outcome.decision);
                BridgeOutcome {
                    value: json!({
                        "ok": true,
                        "event": "tool.execute.after",
                        "session_id": outcome.session_id,
                        "task_id": outcome.task_id,
                        "stage": outcome.stage,
                        "context": context,
                        "checkpoint": outcome.checkpoint_id,
                    }),
                    metrics: outcome.metrics,
                    outcome: telemetry_outcome,
                    role: Some(Role::Build.as_str().to_string()),
                    session_id: Some(outcome.session_id.clone()),
                    task_type: "orchestration".to_string(),
                }
            }
            Err(error) => self.error_outcome(
                error.to_string(),
                Some(Role::Build),
                Some(session_id.to_string()),
            ),
        }
    }

    /// `lead.output` (OpenCode V2 event stream): persist the raw user-visible
    /// text of one completed assistant step from the Mission's current root
    /// execution. The raw OpenCode agent name is diagnostic metadata, not the
    /// authority for root-ness; the durable Mission binding decides that.
    /// Worker sessions and stale pre-cutover executions are therefore rejected
    /// without confusing an OpenCode agent name with an OCG role.
    ///
    /// The write itself is atomic and every failure is soft — a broken report
    /// must never break a session.
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
            .map(|status| json!(status.as_str()))
            .unwrap_or(Value::Null);
        value["artifact_status"] = result
            .artifact_status
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

fn handoff_value(event: &str, handoff: &HandoffOutcome) -> Value {
    json!({
        "ok": true,
        "event": event,
        "session_id": handoff.session_id,
        "task_id": handoff.task_id,
        "source": handoff.source.as_str(),
        "destination": handoff.destination.as_str(),
        "agent": handoff.agent,
        "context": handoff.dynamic_context,
        "handoff_bytes": handoff.capsule.measured_bytes(),
        "stale": handoff.stale,
        "stale_reasons": handoff.stale_reasons,
        "advisory_permissions": handoff.advisory_permissions,
    })
}

fn build_feedback(decision: &BuildDecision) -> (String, Outcome) {
    let mut context = String::new();
    let outcome = match decision {
        BuildDecision::Passed { verification, .. } => {
            context.push_str(&format!(
                "ocg verification: stage '{}' passed; no Debug hand-off is recommended.\n",
                verification.stage
            ));
            Outcome::Success
        }
        BuildDecision::RetryBuild {
            attempt,
            verification,
            ..
        } => {
            context.push_str(&format!(
                "ocg verification: stage '{}' failed; bounded Build retry {attempt} is allowed.\n",
                verification.stage
            ));
            append_verification(&mut context, verification);
            Outcome::Failure
        }
        BuildDecision::Debug {
            reason,
            handoff,
            report,
        } => {
            context.push_str(&format!("ocg verification failed: {reason}\n"));
            context.push_str(&format!(
                "ocg recommends the Debug role (agent {}).\n",
                handoff.agent.as_deref().unwrap_or("ocg-debug")
            ));
            let verification = crate::orchestration::controller::handoff_verification(report);
            append_verification(&mut context, &verification);
            Outcome::Failure
        }
        BuildDecision::NotConfigured { note } => {
            context.push_str(&format!("ocg verification: {note}\n"));
            Outcome::Unknown
        }
    };
    (context, outcome)
}

fn append_verification(
    context: &mut String,
    verification: &crate::orchestration::handoff::HandoffVerification,
) {
    if !verification.failed_commands.is_empty() {
        context.push_str("failed commands:\n");
        for command in &verification.failed_commands {
            context.push_str(&format!("- {command}\n"));
        }
    }
    if !verification.failed_tests.is_empty() {
        context.push_str("failed tests:\n");
        for test in &verification.failed_tests {
            context.push_str(&format!("- {test}\n"));
        }
    }
    if !verification.locations.is_empty() {
        context.push_str("failing locations:\n");
        for location in &verification.locations {
            context.push_str(&format!("- {}\n", location.display()));
        }
    }
    if !verification.raw_log_refs.is_empty() {
        context.push_str("raw logs:\n");
        for reference in &verification.raw_log_refs {
            context.push_str(&format!("- {reference}\n"));
        }
    }
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
    controller: &Controller<'_>,
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

    // Fast path for the controller's canonical durable ownership predicate.
    // The lookup below remains explicit because first-prompt admission may
    // race the creation of the Mission record.
    if controller.is_current_execution(&execution_id)? {
        return Ok(true);
    }

    // This is deliberately an ownership check, not a membership check. The
    // Mission returned by `find_by_session` must itself carry this exact
    // durable execution binding. A worker view that can find the Mission but
    // is not its authoritative binding is rejected.
    match mission::find_by_session(controller.root(), session_id) {
        Ok(Some(mission)) => Ok(mission
            .runtime_execution_id()
            .is_some_and(|bound| bound == execution_id)),
        Ok(None) => {
            // No Mission exists yet for a verified root execution: this is the
            // only first-prompt case, so enforce conservatively before
            // admission creates the durable binding.
            Ok(true)
        }
        Err(error) => Err(error),
    }
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

/// The canonical Mission id for an admitted Lead session.
///
/// It is derived from the durable session binding, so the same session always
/// re-attaches to the same WorkNode tree across restart. The id is not derived
/// from prompt text, agent name or ready-queue order.
fn verification_commands(report: &Value) -> Vec<String> {
    report
        .get("results")
        .and_then(Value::as_array)
        .map(|results| {
            results
                .iter()
                .filter_map(|result| {
                    let program = result.get("command").and_then(Value::as_str)?;
                    let args = result
                        .get("args")
                        .and_then(Value::as_array)
                        .map(|args| {
                            args.iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(" ")
                        })
                        .unwrap_or_default();
                    Some(if args.is_empty() {
                        program.to_string()
                    } else {
                        format!("{program} {args}")
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn canonical_mission_id(session_id: &str) -> Option<crate::orchestration::substrate::MissionId> {
    use crate::runtime::hash::sha256_hex;
    let digest = sha256_hex(format!("ocg-canonical|{session_id}").as_bytes());
    let short = digest.get(..24)?;
    crate::orchestration::substrate::MissionId::new(format!("wn-{short}")).ok()
}

/// The three distinct outcomes of trusted verification for one canonical Run.
///
/// Keeping `NotRun` separate from `Failed` is what stops an unconfigured project
/// from failing real work, and stops unchecked work from counting as complete.
enum VerificationOutcome {
    Passed(Value),
    Failed(Value),
    NotRun,
}

/// The durable canonical authority of a caller session.
struct CanonicalAuthority {
    mission_id: String,
    node_id: usize,
    run_id: usize,
}

/// Delimiters of the OCG-owned dispatch-witness envelope.
///
/// The envelope is a structured, OCG-owned block appended to a delegated
/// prompt. It is the only channel by which a worker session recovers the
/// durable identity of the Run that created it. It is never parsed for
/// authority: the bridge re-validates the Run the witness names.
pub const WITNESS_START: &str = "<<<OCG:RUN_WITNESS v1>>>";
pub const WITNESS_END: &str = "<<<OCG:RUN_WITNESS:END>>>";

/// Read the dispatch witness a delegated prompt carries, if any.
pub fn witness_from_prompt(text: &str) -> Option<DispatchWitness> {
    let start = text.find(WITNESS_START)? + WITNESS_START.len();
    let end = text[start..].find(WITNESS_END)? + start;
    let value: Value = serde_json::from_str(text[start..end].trim()).ok()?;
    DispatchWitness::from_json(&value).ok()
}

/// Read the dispatch witness an adapter attached to this exact invocation.
///
/// The adapter puts the witness in a structured field the host returns
/// unchanged. Prompt text is never parsed for identity; a witness that cannot
/// be read as a complete durable witness is simply absent, and the caller then
/// falls through to the compatibility path rather than guessing.
fn witness_from(args: &Value) -> Option<DispatchWitness> {
    let value = args.get("ocg_witness")?;
    DispatchWitness::from_json(value).ok()
}

/// Optional explicit dependency edges a caller may attach to a delegation.
fn dependencies_from(args: &Value) -> Vec<WorkNodeId> {
    args.get("ocg_dependencies")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|item| item.as_u64())
                .filter_map(|raw| usize::try_from(raw).ok())
                .map(WorkNodeId)
                .collect()
        })
        .unwrap_or_default()
}

fn witness_json(mut value: Value, witness: &DispatchWitness) -> Value {
    value["run_id"] = json!(witness.run_id);
    value["run_generation"] = json!(witness.run_generation);
    value["runtime_execution_id"] = json!(witness.runtime_execution_id);
    value["dispatch_id"] = json!(witness.dispatch_id);
    value["witness"] = witness.to_json();
    value
}

fn outcome_name(outcome: RunState) -> &'static str {
    match outcome {
        RunState::Active => "active",
        RunState::Completed => "completed",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
        RunState::Superseded => "superseded",
        RunState::Fenced => "fenced",
    }
}

fn safe_error(message: &str) -> String {
    if crate::telemetry::task::is_secret_like(message) {
        "orchestration bridge error (details withheld)".to_string()
    } else {
        message.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::CapabilityConfig;
    use crate::clock::FixedClock;
    use crate::context::ContextConfig;
    use crate::orchestration::config::OrchestrationConfig;
    use crate::orchestration::mission;
    use crate::process::{FakeCaptureRunner, FakeGitHost};
    use crate::runtime::compat::MemorySessionClient;
    use crate::runtime::compat::{BridgeRuntimeClient, LeadSelection};
    use crate::runtime::lifecycle::RuntimeProfile;
    use crate::verification::config::VerificationConfig;
    use std::cell::RefCell;
    use std::rc::Rc;

    fn controller<'a>(
        root: &'a std::path::Path,
        git: &'a FakeGitHost,
        clock: &'a FixedClock,
        verification: VerificationConfig,
    ) -> Controller<'a> {
        Controller::new(
            root,
            OrchestrationConfig::default(),
            ContextConfig::default(),
            CapabilityConfig::default(),
            verification,
            git,
            clock,
        )
    }

    fn lead_selection() -> LeadSelection {
        LeadSelection {
            level: "high".to_string(),
            agent: "lead-high".to_string(),
            provider_id: "openai".to_string(),
            model_id: "gpt-6-astra".to_string(),
            variant: Some("low".to_string()),
        }
    }

    /// Create a bridge with a `MemorySessionClient` as the invocation-scoped
    /// runtime and a canonical Lead contract. Required for `session.prompt`
    /// authority-gate tests (P0-J).
    fn bridge_with_runtime<'a>(
        controller: &'a Controller<'a>,
        runner: &'a FakeCaptureRunner,
    ) -> BridgeContext<'a> {
        let client = MemorySessionClient::new();
        let runtime: Rc<RefCell<Box<dyn BridgeRuntimeClient>>> =
            Rc::new(RefCell::new(Box::new(client)));
        let profile = RuntimeProfile::new(
            lead_selection().agent.clone(),
            lead_selection().full_model_id(),
            lead_selection().variant.clone(),
        );
        BridgeContext::new(controller, runner, TelemetryConfig::disabled())
            .with_bridge_runtime(runtime, profile)
            .with_lead_contract(lead_selection())
    }

    #[test]
    fn canonical_controller_bridge_lifecycle_recovers_without_legacy_replay() {
        use crate::orchestration::substrate::{RunState, WorkState};
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(10);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_runtime(&controller, &runner);
        let mission = "wn-production";
        let call = |event: &str, data: Value| {
            let mut data = data;
            data["mission_id"] = json!(mission);
            let value = bridge.dispatch(event, &data);
            assert_eq!(value["ok"], true, "{event}: {value}");
            value
        };
        let contract = json!({"executor":"worker","model":"provider/model","role":"worker"});
        assert_eq!(
            call(
                "work.mission.create",
                json!({"runtime_execution_id":"session-lead","payload":"goal"})
            )["run_id"],
            0
        );
        let first = call(
            "work.child.create",
            json!({"node_id":0,"run_id":0,"payload":"A"}),
        )["node_id"]
            .as_u64()
            .unwrap();
        let second = call(
            "work.child.create",
            json!({"node_id":0,"run_id":0,"payload":"B"}),
        )["node_id"]
            .as_u64()
            .unwrap();
        let dependent = call(
            "work.child.create",
            json!({"node_id":0,"run_id":0,"payload":"C","dependencies":[first,second]}),
        )["node_id"]
            .as_u64()
            .unwrap();
        assert_eq!(
            call("work.ready", json!({}))["ready"],
            json!([first, second])
        );
        let run_a = call("work.dispatch",json!({"node_id":first,"run_id":0,"contract":contract,"runtime_execution_id":"session-worker-a"}))["run_id"].as_u64().unwrap();
        assert_ne!(run_a.to_string(), "session-worker-a");
        let run_b = call("work.dispatch",json!({"node_id":second,"run_id":0,"contract":contract,"runtime_execution_id":"session-worker-b"}))["run_id"].as_u64().unwrap();
        assert_eq!(call("work.ready", json!({}))["ready"], json!([]));
        call(
            "work.finish",
            json!({"node_id":first,"run_id":run_a,"outcome":"completed","result":"A done"}),
        );
        assert_eq!(call("work.ready", json!({}))["ready"], json!([]));
        call(
            "work.finish",
            json!({"node_id":second,"run_id":run_b,"outcome":"completed"}),
        );
        assert_eq!(call("work.ready", json!({}))["ready"], json!([dependent]));
        let run_c = call("work.dispatch",json!({"node_id":dependent,"run_id":0,"contract":contract,"runtime_execution_id":"session-worker-c"}))["run_id"].as_u64().unwrap();
        call(
            "work.finish",
            json!({"node_id":dependent,"run_id":run_c,"outcome":"failed","result":"failed"}),
        );
        let retry = call("work.replace",json!({"node_id":dependent,"run_id":run_c,"contract":contract,"runtime_execution_id":"session-worker-c2"}))["run_id"].as_u64().unwrap();
        let lead2 = call("work.replace",json!({"node_id":0,"run_id":0,"contract":{"executor":"lead-high","model":"openai/gpt-6-astra","role":"lead"},"runtime_execution_id":"session-lead-2"}))["run_id"].as_u64().unwrap();
        assert_eq!(
            bridge.dispatch(
                "work.child.create",
                &json!({"mission_id":mission,"node_id":0,"run_id":0})
            )["ok"],
            false
        );
        assert_eq!(bridge.dispatch("work.dispatch",&json!({"mission_id":mission,"node_id":dependent,"run_id":0,"contract":contract,"runtime_execution_id":"stale"}))["ok"],false);
        assert_eq!(
            bridge.dispatch(
                "work.finish",
                &json!({"mission_id":mission,"node_id":0,"run_id":0,"outcome":"completed"})
            )["ok"],
            false
        );
        call("work.result.late", json!({"run_id":0,"result":"late lead"}));
        let child = call(
            "work.child.create",
            json!({"node_id":0,"run_id":lead2,"payload":"new work"}),
        )["node_id"]
            .as_u64()
            .unwrap();
        let state = controller.load_work_mission(mission).unwrap().unwrap();
        assert_eq!(state.work_nodes.len(), 5);
        assert_eq!(
            state.work_nodes[WorkNodeId(dependent as usize)].active_run_id,
            Some(RunId(retry as usize))
        );
        assert_eq!(
            state.work_nodes[WorkNodeId(dependent as usize)].spawned_by_run_id,
            Some(RunId(0))
        );
        assert_eq!(
            state.work_nodes[WorkNodeId(first as usize)].state,
            WorkState::Completed
        );
        assert_eq!(state.runs[RunId(run_c as usize)].state, RunState::Fenced);
        assert_eq!(state.runs[RunId(0)].result.as_deref(), Some("late lead"));
        assert_eq!(
            state.work_nodes[WorkNodeId(child as usize)].parent_node_id,
            Some(WorkNodeId(0))
        );
        assert_eq!(
            state.runs[RunId(retry as usize)]
                .runtime_execution_id
                .as_deref(),
            Some("session-worker-c2")
        );
        assert!(state.events.iter().any(|e| e.kind == "worknode_ready"));
        drop(bridge);
        drop(controller);
        // No replay JSON is written by the canonical execution lane.
        assert!(!dir
            .path()
            .join(".ocg/orchestration/replay/state.json")
            .exists());
        let restarted = self::controller(dir.path(), &git, &clock, VerificationConfig::default());
        let recovered = restarted.load_work_mission(mission).unwrap().unwrap();
        assert_eq!(recovered.ready(10), vec![WorkNodeId(child as usize)]);
        assert!(recovered.authoritative(WorkNodeId(0), RunId(lead2 as usize)));
        assert!(recovered.authoritative(WorkNodeId(dependent as usize), RunId(retry as usize)));
    }

    #[test]
    fn canonical_bridge_fails_closed_when_initialized_database_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(10);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_runtime(&controller, &runner);
        let created = bridge.dispatch(
            "work.mission.create",
            &json!({
                "mission_id":"wn-missing", "runtime_execution_id":"lead-session"
            }),
        );
        assert_eq!(created["ok"], true, "{created}");
        let path = crate::orchestration::state::state_dir(dir.path()).join("substrate.sqlite3");
        let retained = path.with_extension("retained");
        std::fs::rename(&path, &retained).unwrap();
        let inspected = bridge.dispatch("work.inspect", &json!({"mission_id":"wn-missing"}));
        assert_eq!(inspected["ok"], false);
        assert!(inspected["error"]
            .as_str()
            .unwrap()
            .contains("missing after initialization"));
        assert!(!path.exists());
        assert!(controller.load_work_mission("wn-missing").is_err());
    }

    #[test]
    fn unknown_event_is_rejected_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::disabled());
        let value = bridge.dispatch("nope", &json!({}));
        assert_eq!(value["ok"], json!(false));
    }

    #[test]
    fn unknown_subagent_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::disabled());
        let value = bridge.dispatch(
            "tool.execute.before",
            &json!({"session_id": "s", "args": {"subagent_type": "nonsense"}}),
        );
        assert_eq!(value["ok"], json!(false));
    }

    #[test]
    fn chat_message_returns_dynamic_context() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("parser.rs"), "pub fn parse() {}\n").unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::disabled());
        let value = bridge.dispatch(
            "chat.message",
            &json!({"session_id": "s", "text": "fix the parser"}),
        );
        assert_eq!(value["ok"], json!(true));
        assert!(value["context"]
            .as_str()
            .unwrap()
            .contains("fix the parser"));
    }

    fn bridge_with_reports<'a>(
        controller: &'a Controller<'a>,
        runner: &'a FakeCaptureRunner,
        reports: ReportsConfig,
    ) -> BridgeContext<'a> {
        BridgeContext::new(controller, runner, TelemetryConfig::disabled()).with_reports(reports)
    }

    fn admit_report_mission<'a>(controller: &Controller<'a>, session: &str) -> String {
        controller
            .admit_user_task(session, "latest output identity regression")
            .expect("admit report Mission")
            .task_id
    }

    #[test]
    fn context_observation_without_a_runtime_is_unknown_and_fail_soft() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_runtime(&controller, &runner);
        let admitted = bridge.dispatch(
            "session.prompt",
            &json!({"session_id": "ses_source", "text": "keep this Mission durable"}),
        );
        assert_eq!(admitted["ok"], json!(true));
        let value = bridge.dispatch(
            "context.observe",
            &json!({
                "session_id": "ses_source",
                "event_id": "event-unknown",
                "agent": "lead-high",
                "finish": "stop",
                "safe_boundary": true,
                "output_persisted": true,
                "tokens": {"input": 90, "cache": {"read": 0}},
            }),
        );
        assert_eq!(value["ok"], json!(true));
        assert_eq!(value["decision"]["state"], json!("unknown"));
        assert_eq!(value["decision"]["rollover_allowed"], json!(false));
        assert_eq!(value["artifact_id"], json!(null));
    }

    #[test]
    fn context_observation_uses_the_current_mission_binding_for_a_build_agent() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::disabled());
        admit_report_mission(&controller, "root");
        let value = bridge.dispatch(
            "context.observe",
            &json!({
                "session_id": "root",
                "event_id": "build-context",
                "agent": "build",
                "finish": "stop",
                "safe_boundary": true,
                "output_persisted": true,
                "tokens": {"input": 10, "cache": {"read": 0}},
            }),
        );
        assert_eq!(value["ok"], json!(true), "{value}");
        assert_eq!(value["ignored"], json!(null));
    }

    #[test]
    fn mission_bound_worker_execution_is_not_root_lead_authority() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();

        // Deliberately bind the Mission lookup to the worker execution. The
        // lookup succeeds, but runtime lineage proves that this execution is
        // a child of the durable root execution.
        let worker = "worker-execution";
        let task = "worker prompt must preserve worker routing";
        controller.admit_user_task(worker, task).unwrap();
        assert!(mission::find_by_session(dir.path(), worker)
            .unwrap()
            .is_some());

        let client = MemorySessionClient::new()
            .with_session_id(worker)
            .with_parent_session("root-execution");
        let runtime: Rc<RefCell<Box<dyn BridgeRuntimeClient>>> =
            Rc::new(RefCell::new(Box::new(client)));
        let lead = lead_selection();
        let profile = lead.runtime_profile();
        let bridge = BridgeContext::new(&controller, &runner, TelemetryConfig::disabled())
            .with_bridge_runtime(runtime, profile)
            .with_lead_contract(lead);

        let result = bridge.dispatch(
            "session.prompt",
            &json!({"session_id": worker, "text": "continue worker work"}),
        );
        assert_eq!(result["ok"], json!(true), "{result}");
        // The worker path admits bookkeeping but skips Root Lead selection.
        // The durable parent check is what makes this independent of any
        // mutable runtime agent name.
        let mut runtime = bridge.rollover_runtime.as_ref().unwrap().borrow_mut();
        assert_eq!(
            runtime.as_session().effective_lead(worker).unwrap().agent,
            None
        );
    }

    #[test]
    fn lead_output_persists_the_raw_text_byte_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        admit_report_mission(&controller, "s");
        let text = "# Final answer\n\n- one\n- two\n\n```rust\nfn done() {}\n```\n";
        let value = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "s", "message_id": "m", "agent": "lead-high", "text": text}),
        );
        assert_eq!(value["ok"], json!(true), "{value}");
        let path = crate::reports::latest_lead_output_path(dir.path());
        assert_eq!(value["path"], json!(path.to_string_lossy()));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
    }

    #[test]
    fn lead_output_accepts_the_current_root_even_when_its_agent_is_build() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        admit_report_mission(&controller, "root");
        let value = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "root", "agent": "build", "text": "root build output\n"}),
        );
        assert_eq!(value["ok"], json!(true), "{value}");
        assert_eq!(
            std::fs::read_to_string(crate::reports::latest_lead_output_path(dir.path())).unwrap(),
            "root build output\n"
        );
    }

    #[test]
    fn lead_output_follows_the_current_mission_binding_across_rollover_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        let mission_id = admit_report_mission(&controller, "execution-a");

        let first = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "execution-a", "agent": "lead-mid", "text": "A good\n"}),
        );
        assert_eq!(first["ok"], json!(true), "{first}");

        let mut mission = mission::load(dir.path(), &mission_id).unwrap().unwrap();
        mission
            .request_rollover("execution-a", "report-rollover", "context", 2)
            .unwrap();
        mission
            .mark_rollover_prepared("report-rollover", 3)
            .unwrap();
        mission
            .mark_rollover_target_ready("report-rollover", "execution-b", 4)
            .unwrap();
        // Before cutover, A is still the authoritative current execution.
        mission::save(dir.path(), &mission).unwrap();
        let before_cutover = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "execution-a", "agent": "build", "text": "A still current\n"}),
        );
        assert_eq!(before_cutover["ok"], json!(true), "{before_cutover}");
        assert_eq!(
            std::fs::read_to_string(crate::reports::latest_lead_output_path(dir.path())).unwrap(),
            "A still current\n"
        );

        let mut mission = mission::load(dir.path(), &mission.mission_id)
            .unwrap()
            .unwrap();
        mission
            .bind_rollover_session("report-rollover", "execution-a", "execution-b", 5)
            .unwrap();
        mission::save(dir.path(), &mission).unwrap();

        let stale = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "execution-a", "agent": "lead-mid", "text": "stale A\n"}),
        );
        assert_eq!(stale["ok"], json!(false), "{stale}");
        assert_eq!(
            std::fs::read_to_string(crate::reports::latest_lead_output_path(dir.path())).unwrap(),
            "A still current\n"
        );

        let current = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "execution-b", "agent": "build", "text": "B current\n"}),
        );
        assert_eq!(current["ok"], json!(true), "{current}");
        assert_eq!(
            std::fs::read_to_string(crate::reports::latest_lead_output_path(dir.path())).unwrap(),
            "B current\n"
        );

        // A fresh bridge/controller instance reads the durable Mission binding;
        // no disposable state or raw agent name is needed for recovery.
        let clock_after_restart = FixedClock::new(6);
        let controller_after_restart = Controller::new(
            dir.path(),
            OrchestrationConfig::default(),
            ContextConfig::default(),
            CapabilityConfig::default(),
            VerificationConfig::default(),
            &git,
            &clock_after_restart,
        );
        let bridge_after_restart =
            bridge_with_reports(&controller_after_restart, &runner, ReportsConfig::default());
        let recovered = bridge_after_restart.dispatch(
            "lead.output",
            &json!({"session_id": "execution-b", "agent": "build", "text": "B recovered\n"}),
        );
        assert_eq!(recovered["ok"], json!(true), "{recovered}");
        assert_eq!(
            std::fs::read_to_string(crate::reports::latest_lead_output_path(dir.path())).unwrap(),
            "B recovered\n"
        );
    }

    #[test]
    fn lead_output_refuses_non_current_worker_sessions_even_when_agent_is_build() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        admit_report_mission(&controller, "root");
        for (session, agent) in [("worker", "build"), ("worker", "ocg-build"), ("other", "")] {
            let value = bridge.dispatch(
                "lead.output",
                &json!({"session_id": session, "agent": agent, "text": "worker text\n"}),
            );
            assert_eq!(
                value["ok"],
                json!(false),
                "session {session}, agent {agent}"
            );
        }
        assert!(!crate::reports::latest_lead_output_path(dir.path()).exists());
    }

    #[test]
    fn lead_output_refuses_an_empty_payload() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        let value = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "s", "agent": "lead-low", "text": "   \n"}),
        );
        assert_eq!(value["ok"], json!(false), "{value}");
        assert!(!crate::reports::latest_lead_output_path(dir.path()).exists());
    }

    #[test]
    fn lead_output_is_inert_when_the_switch_is_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let reports = ReportsConfig::from_config(&json!({
            "reports": {"latestLeadOutput": {"enabled": false}}
        }))
        .unwrap();
        let bridge = bridge_with_reports(&controller, &runner, reports);
        let value = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "s", "agent": "lead-high", "text": "hidden\n"}),
        );
        assert_eq!(value["ok"], json!(false));
        assert_eq!(value["disabled"], json!(true));
        assert!(!crate::reports::latest_lead_output_path(dir.path()).exists());
    }

    #[test]
    fn lead_output_reports_a_write_failure_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        // A file where the reports directory must be makes the write fail.
        std::fs::create_dir_all(dir.path().join(".ocg")).unwrap();
        std::fs::write(dir.path().join(".ocg/reports"), "blocked").unwrap();
        let git = FakeGitHost::new();
        let clock = FixedClock::new(1);
        let controller = controller(dir.path(), &git, &clock, VerificationConfig::default());
        let runner = FakeCaptureRunner::new();
        let bridge = bridge_with_reports(&controller, &runner, ReportsConfig::default());
        admit_report_mission(&controller, "s");
        let value = bridge.dispatch(
            "lead.output",
            &json!({"session_id": "s", "agent": "lead-high", "text": "cannot land\n"}),
        );
        assert_eq!(value["ok"], json!(false), "{value}");
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".ocg/reports")).unwrap(),
            "blocked"
        );
    }
}
