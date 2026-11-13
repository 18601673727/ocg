use super::*;
use crate::db::entities::{
    AllocationToken, ConfigBody, ConfigKind, DispatchSelection, ExecutionConfiguration,
    JobDependency, JobToolCall, MutableJob, ToolAdmission, ToolCallRef, ToolDecision, ToolEffect,
    ToolEvidenceRecord, ToolOutcome, ToolPayload, Violation,
};
use serde_json::{Value, json};

pub(crate) struct JobToolApplied {
    pub(crate) value: Value,
    pub(crate) running: Vec<AllocationToken>,
    pub(crate) ended: Vec<JobId>,
}

impl JobToolApplied {
    fn of(value: Value) -> Self {
        Self {
            value,
            running: Vec::new(),
            ended: Vec::new(),
        }
    }
}

const MAX_PLAN_STEPS: usize = 6;
const MAX_STEP_TITLE_CHARS: usize = 120;
const MAX_STEP_OBJECTIVE_CHARS: usize = 2000;
const MAX_SELECTED_PROJECTS: usize = 16;
const MAX_INSTRUCTION_SUMMARY_CHARS: usize = 256;

impl Batch {
    fn job_tool_id(&mut self, text: &str) -> Result<u128, BatchError> {
        match crate::db::entities::parse_entity_id(text) {
            Some(id) => Ok(id),
            None => self.reject(Violation::InvalidValue(
                "invalid_target",
                "expected a canonical UUID",
            )),
        }
    }

    fn job_tool_page(&mut self, limit: Option<u32>) -> Result<usize, BatchError> {
        match limit.unwrap_or(10) {
            limit @ 1..=20 => Ok(limit as usize),
            _ => self.reject(Violation::InvalidValue("limit", "must be between 1 and 20")),
        }
    }

    pub(crate) fn apply_job_tool(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        call: ToolCallRef,
        tool: JobToolCall,
    ) -> Result<JobToolApplied, BatchError> {
        self.require_development_call(proof, call)?;
        let Some(evidence) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if evidence.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        let Some(admission) = evidence.admission() else {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        };
        if admission.handler_version != tool.handler_version() || evidence.name != tool.name() {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        let effect = admission.effect;
        let Some(job) = self.mutable_job(call.job)? else {
            return self.corrupted("a job tool was called by a Job that is not native".into());
        };
        if !self.frozen_agent_manages_jobs(&job)? {
            return self.reject(Violation::InvalidValue(
                "capabilities",
                "Job management requires Chat capability",
            ));
        }
        let caller = job.parent_id;
        let applied = match tool {
            JobToolCall::JobPlan { steps } => self.job_plan(&job, call, steps)?,
            JobToolCall::JobLimits {
                conversation,
                max_depth,
                max_auto_turns,
                max_iterations,
            } => JobToolApplied::of(self.job_limits_tool(
                caller,
                conversation,
                max_depth,
                max_auto_turns,
                max_iterations,
            )?),
            JobToolCall::JobAgents { after, limit } => {
                JobToolApplied::of(self.job_agent_catalog(after, limit)?)
            }
            JobToolCall::JobWorkers { after, limit } => {
                JobToolApplied::of(self.job_worker_catalog(caller, after, limit)?)
            }
            JobToolCall::JobMessage { conversation, text } => {
                let target = JobId::new(self.job_tool_id(&conversation)?);
                self.tool_rights(caller, target)?;
                if text.trim().is_empty() {
                    return self.reject(Violation::InvalidValue("text", "must not be empty"));
                }
                self.require_open_conversation(target)?;
                let turn = self.agent_turn(target, text, call, b"message")?;
                JobToolApplied::of(
                    json!({"conversation": target.to_string(), "turn": turn.to_string()}),
                )
            }
            JobToolCall::JobRetry { turn } => {
                let turn = JobId::new(self.job_tool_id(&turn)?);
                let Some(original) = self.mutable_job(turn)? else {
                    return self.reject(Violation::InvalidValue("turn", "expected a stored turn"));
                };
                let conversation = original.parent_id;
                self.tool_rights(caller, conversation)?;
                let appended = self.retry(
                    conversation,
                    turn,
                    Actor::Agent {
                        conversation: caller,
                    },
                )?;
                JobToolApplied::of(
                    json!({"conversation": conversation.to_string(), "turn": appended.turn.to_string()}),
                )
            }
            JobToolCall::JobStop { conversation } => {
                self.manage(caller, &conversation, ConversationAction::Stop)?
            }
            JobToolCall::JobClose { conversation } => {
                self.manage(caller, &conversation, ConversationAction::Close)?
            }
            JobToolCall::JobHold { conversation } => {
                self.manage(caller, &conversation, ConversationAction::Hold)?
            }
            JobToolCall::JobUnhold { conversation } => {
                self.manage(caller, &conversation, ConversationAction::Unhold)?
            }
            JobToolCall::JobAssign { .. } | JobToolCall::JobDispatch { .. } => {
                return self.reject(Violation::InvalidValue(
                    "tool",
                    "this call is decided by the user, not applied by the host",
                ));
            }
        };
        let text = applied.value.to_string();
        let outcome = if effect == ToolEffect::ReadOnly {
            ToolOutcome::CompletedElided {
                bytes: text.len() as u64,
                digest: ContentDigest::of(text.as_bytes()),
            }
        } else {
            ToolOutcome::Completed {
                result: ToolPayload::new(text),
            }
        };
        self.tool_outcome(proof, call, outcome)?;
        Ok(applied)
    }

    fn require_open_conversation(&mut self, conversation: JobId) -> Result<(), BatchError> {
        self.require_mutable_conversation(conversation)?;
        if self.job_control(conversation)?.closing.is_some() {
            return self.reject(Violation::InvalidValue(
                "conversation_closing",
                "the conversation no longer accepts messages",
            ));
        }
        if self.require_job_state(conversation)?.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "conversation_closed",
                "the conversation no longer accepts messages",
            ));
        }
        Ok(())
    }

    fn manage(
        &mut self,
        caller: JobId,
        conversation: &str,
        action: ConversationAction,
    ) -> Result<JobToolApplied, BatchError> {
        let conversation = JobId::new(self.job_tool_id(conversation)?);
        self.tool_rights(caller, conversation)?;
        let by = Actor::Agent {
            conversation: caller,
        };
        let effect = match action {
            ConversationAction::Stop => self.stop_turn(conversation, by)?,
            ConversationAction::Close => self.close(conversation, by)?,
            ConversationAction::Hold => self.hold(conversation, by, false)?,
            ConversationAction::Unhold => self.hold(conversation, by, true)?,
        };
        let status = if effect.already {
            if self.require_job_state(conversation)?.outcome.is_some() {
                "already_settled"
            } else {
                "already_closing"
            }
        } else {
            "applied"
        };
        Ok(JobToolApplied {
            value: json!({
                "conversation": conversation.to_string(),
                "status": status,
                "running": effect.running.iter().map(|token| token.job.to_string()).collect::<Vec<_>>(),
            }),
            running: effect.running,
            ended: effect.ended,
        })
    }

    fn job_limits_tool(
        &mut self,
        caller: JobId,
        conversation: Option<String>,
        max_depth: Option<u8>,
        max_auto_turns: Option<u8>,
        max_iterations: Option<u32>,
    ) -> Result<Value, BatchError> {
        let target = match conversation {
            Some(id) => JobId::new(self.job_tool_id(&id)?),
            None => caller,
        };
        if target != caller {
            self.tool_rights(caller, target)?;
        }
        let (current, depth) = self.group_limits(target)?;
        if max_depth.is_some() || max_auto_turns.is_some() || max_iterations.is_some() {
            self.set_job_limits(
                target,
                JobLimits {
                    max_depth: max_depth.unwrap_or(current.max_depth),
                    max_auto_turns: max_auto_turns.unwrap_or(current.max_auto_turns),
                    max_iterations: max_iterations.unwrap_or(current.max_iterations),
                },
                Actor::Agent {
                    conversation: caller,
                },
            )?;
        }
        let (limits, _) = self.group_limits(target)?;
        Ok(json!({
            "conversation": target.to_string(),
            "depth": depth,
            "remaining_depth": limits.max_depth.saturating_sub(depth),
            "max_depth": limits.max_depth,
            "max_auto_turns": limits.max_auto_turns,
            "max_iterations": limits.max_iterations,
        }))
    }

    fn job_plan(
        &mut self,
        job: &MutableJob,
        call: ToolCallRef,
        steps: Vec<crate::db::entities::JobPlanStep>,
    ) -> Result<JobToolApplied, BatchError> {
        if !(1..=MAX_PLAN_STEPS).contains(&steps.len())
            || steps.iter().any(|step| {
                step.title.trim().is_empty()
                    || step.title.chars().count() > MAX_STEP_TITLE_CHARS
                    || step.objective.trim().is_empty()
                    || step.objective.chars().count() > MAX_STEP_OBJECTIVE_CHARS
            })
        {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "expected one to six bounded steps",
            ));
        }
        if steps.iter().enumerate().any(|(index, step)| {
            step.depends_on
                .iter()
                .any(|dependency| *dependency >= index)
                || step.depends_on.iter().collect::<BTreeSet<_>>().len() != step.depends_on.len()
        }) {
            return self.reject(Violation::InvalidValue(
                "task_dependencies",
                "dependencies must be unique indices of earlier steps",
            ));
        }
        let conversation = job.parent_id;
        if self.group_head(conversation)? != Some(job.id) {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "a later message is already queued; finish this turn before processing it",
            ));
        }
        let work_started = self.previous_tool_evidence(call)?.iter().any(|previous| {
            let refused_plan = previous.name == "job_plan"
                && previous.outcome.as_ref().is_some_and(|outcome| {
                    matches!(
                        outcome.fact,
                        ToolOutcome::NotApplied { .. } | ToolOutcome::NotInvoked { .. }
                    )
                });
            !matches!(previous.decision.fact, ToolDecision::Refused(_)) && !refused_plan
        });
        if work_started {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "a task plan must precede execution",
            ));
        }
        let spec = self.require_mutable_conversation(conversation)?;
        if let JobKind::Group {
            origin: Origin::Agent { call: opened },
            ..
        } = spec.kind
            && self
                .tool_evidence(opened)?
                .is_some_and(|opening| opening.name == "job_plan")
        {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "the steps of a plan cannot create another plan",
            ));
        }
        if steps.len() == 1 {
            return Ok(JobToolApplied::of(json!({
                "steps": [],
                "continuation": job.id.to_string(),
                "next": "one local step: continue directly in this Job",
            })));
        }
        let (configuration, priority) = self.conversation_configuration(conversation)?;
        let (agent, project) = match spec.kind {
            JobKind::Group { agent, project, .. } => (agent, project),
            JobKind::Execution { .. } => {
                return self.corrupted("a conversation is not a group".into());
            }
        };
        let agent = agent.or_else(|| {
            configuration
                .agent_profile
                .map(|id| ConfigId::new(id.get()))
        });
        let mut opened: Vec<JobId> = Vec::new();
        let mut receipts = Vec::new();
        for (index, step) in steps.into_iter().enumerate() {
            let after = step
                .depends_on
                .iter()
                .filter_map(|dependency| opened.get(*dependency).copied())
                .map(|id| (id, DependencyCondition::Settled))
                .collect::<Vec<_>>();
            if after.len() != step.depends_on.len() {
                return self.reject(Violation::InvalidValue(
                    "task_dependencies",
                    "dependency has not been allocated",
                ));
            }
            let child =
                self.open_child_conversation(conversation, call, agent, project, false, after)?;
            let mut control = self.job_control(child)?;
            control.title = Some(step.title);
            self.put_job_control(child, &control)?;
            let n = u32::try_from(index).unwrap_or(u32::MAX);
            let turn = self.host_turn(
                child,
                step.objective,
                configuration,
                priority,
                job_tree::tool_submission(call, b"plan create", n),
                job_tree::tool_submission(call, b"plan submit", n),
            )?;
            opened.push(child);
            receipts.push(json!({"conversation": child.to_string(), "turn": turn.to_string()}));
        }
        Ok(JobToolApplied::of(json!({
            "steps": receipts,
            "next": "the host adds a message to this conversation with every step's result once all have settled; end your turn now",
        })))
    }

    fn awaiting_job_call(
        &mut self,
        evidence: &ToolEvidenceRecord,
    ) -> Result<Option<JobToolCall>, BatchError> {
        let admission: &ToolAdmission = match &evidence.decision.fact {
            ToolDecision::AwaitingApproval(admission) | ToolDecision::AwaitingAnswer(admission) => {
                admission
            }
            ToolDecision::Admitted(_) | ToolDecision::Refused(_) => return Ok(None),
        };
        if !JobToolCall::is_job_tool(&evidence.name) {
            return Ok(None);
        }
        match JobToolCall::parse(&evidence.name, admission.arguments.as_str()) {
            Some(call) => Ok(Some(call)),
            None => self.corrupted("an awaiting job call no longer parses".into()),
        }
    }

    pub(crate) fn perform_decided_job_call(
        &mut self,
        approval: ToolCallRef,
        conversation: JobId,
        evidence: &ToolEvidenceRecord,
        decision: &ApprovalDecision,
    ) -> Result<Option<String>, BatchError> {
        let Some(call) = self.awaiting_job_call(evidence)? else {
            return Ok(None);
        };
        match (call, decision) {
            (
                JobToolCall::JobDispatch {
                    objective,
                    keep_open,
                    every_seconds,
                    depends_on,
                    ..
                },
                ApprovalDecision::Answered { text },
            ) => {
                let Ok(selection) = serde_json::from_str::<DispatchSelection>(text) else {
                    return self.reject(Violation::InvalidValue(
                        "dispatch_selection",
                        "the answer is not a target selection",
                    ));
                };
                let dispatched = self.perform_dispatch(
                    approval,
                    conversation,
                    &selection,
                    objective,
                    keep_open,
                    every_seconds,
                    depends_on,
                )?;
                Ok(Some(dispatched))
            }
            (
                JobToolCall::JobAssign {
                    conversation: worker,
                    objective,
                },
                ApprovalDecision::Approved,
            ) => {
                let worker = JobId::new(self.job_tool_id(&worker)?);
                self.tool_rights(conversation, worker)?;
                if !self.idle_worker(conversation, worker)? {
                    return self.reject(Violation::InvalidValue(
                        "worker_not_idle",
                        "the target is not an idle worker owned by this parent",
                    ));
                }
                if objective.trim().is_empty() {
                    return self.reject(Violation::InvalidValue("objective", "must not be empty"));
                }
                let turn = self.agent_turn(worker, objective, approval, b"assign")?;
                Ok(Some(format!(
                    "I approve the assignment. The worker conversation {worker} was given the task as turn {turn}. Its result returns here when it settles. Please continue."
                )))
            }
            (_, _) => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn perform_dispatch(
        &mut self,
        approval: ToolCallRef,
        conversation: JobId,
        selection: &DispatchSelection,
        objective: String,
        keep_open: bool,
        every_seconds: Option<u32>,
        depends_on: Vec<JobDependency>,
    ) -> Result<String, BatchError> {
        self.validate_dispatch_selection(selection)?;
        if objective.trim().is_empty() {
            return self.reject(Violation::InvalidValue("objective", "must not be empty"));
        }
        if every_seconds.is_some() {
            return self.reject(Violation::InvalidValue(
                "every_seconds",
                "scheduled recurrence is not available for native conversations",
            ));
        }
        let agent = selection
            .agent
            .as_deref()
            .map(|id| self.job_tool_id(id).map(ConfigId::new))
            .transpose()?;
        let model = ConfigId::new(self.job_tool_id(&selection.model)?);
        let projects = if selection.projects.is_empty() {
            vec![None]
        } else {
            selection
                .projects
                .iter()
                .map(|project| self.job_tool_id(project).map(|id| Some(ProjectId::new(id))))
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut after = Vec::new();
        for dependency in depends_on {
            let id = JobId::new(self.job_tool_id(&dependency.conversation)?);
            let row = self.require_mutable_conversation(id)?;
            if !matches!(
                row.kind,
                JobKind::Group {
                    keep_open: false,
                    ..
                }
            ) || row.parent_id != Some(conversation)
                || self.job_control(id)?.user_input
            {
                return self.reject(Violation::InvalidValue(
                    "depends_on",
                    "dependencies must be sibling conversations with keep_open false and no user message",
                ));
            }
            let condition = match dependency.condition.as_str() {
                "settled" => DependencyCondition::Settled,
                "success" => DependencyCondition::Success,
                _ => {
                    return self.reject(Violation::InvalidValue(
                        "depends_on",
                        "condition must be settled or success",
                    ));
                }
            };
            after.push((id, condition));
        }
        after.sort_by_key(|(id, _)| *id);
        if after.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return self.reject(Violation::InvalidValue(
                "depends_on",
                "dependencies must be unique",
            ));
        }
        let Some(asked) = self.mutable_job(approval.job)? else {
            return self.corrupted("the Job that asked is not native".into());
        };
        let configuration = ExecutionConfiguration {
            reasoning_effort: None,
            agent_profile: agent.map(Into::into),
            llm_profile: model.into(),
        };
        let mut lines = Vec::new();
        for (index, project) in projects.into_iter().enumerate() {
            let child = self.open_child_conversation(
                conversation,
                approval,
                agent,
                project,
                keep_open,
                after.clone(),
            )?;
            let n = u32::try_from(index).unwrap_or(u32::MAX);
            let turn = self.host_turn(
                child,
                objective.clone(),
                configuration,
                asked.input.priority,
                job_tree::tool_submission(approval, b"dispatch create", n),
                job_tree::tool_submission(approval, b"dispatch submit", n),
            )?;
            lines.push(format!("conversation {child}, turn {turn}"));
        }
        Ok(format!(
            "I selected the target. The host dispatched your objective as: {}. Their results return here when they settle. Please continue.",
            lines.join("; ")
        ))
    }

    pub(crate) fn validate_dispatch_selection(
        &mut self,
        selection: &DispatchSelection,
    ) -> Result<(), BatchError> {
        if selection.projects.len() > MAX_SELECTED_PROJECTS {
            return self.reject(Violation::InvalidValue(
                "projects",
                "select at most 16 projects",
            ));
        }
        let model = ConfigId::new(self.job_tool_id(&selection.model)?);
        let record = self.require_config_kind(model, ConfigKind::Model)?;
        if !record.model_enabled {
            return self.reject(Violation::InvalidValue(
                "dispatch_selection",
                "the selected model is disabled",
            ));
        }
        let Some(agent) = &selection.agent else {
            return self.reject(Violation::InvalidValue(
                "dispatch_selection",
                "select an Agent that lists this Model",
            ));
        };
        let agent = ConfigId::new(self.job_tool_id(agent)?);
        let config = self.require_config_kind(agent, ConfigKind::Agent)?;
        let ConfigBody::Agent(agent) = config.body else {
            return self.corrupted("invalid dispatch Agent".into());
        };
        if !agent.models.contains(&model)
            || !agent.supports_model(&record.body)
            || !agent.runs_generative()
        {
            return self.reject(Violation::InvalidValue(
                "dispatch_selection",
                "the selected Agent cannot run this model",
            ));
        }
        if agent.workspace_grant().is_some() && selection.projects.is_empty() {
            return self.reject(Violation::ProjectRequired);
        }
        let mut projects = BTreeSet::new();
        for project in &selection.projects {
            let project = ProjectId::new(self.job_tool_id(project)?);
            if !projects.insert(project) {
                return self.reject(Violation::InvalidValue(
                    "projects",
                    "projects must be unique",
                ));
            }
            self.require_project(project)?;
        }
        Ok(())
    }

    fn job_agent_catalog(
        &mut self,
        after: Option<String>,
        limit: Option<u32>,
    ) -> Result<Value, BatchError> {
        let limit = self.job_tool_page(limit)?;
        let after = after
            .map(|id| self.job_tool_id(&id))
            .transpose()?
            .unwrap_or(0);
        let (targets, next_after) = self.agent_profile_ids(after, limit)?;
        let has_more = next_after.is_some();
        let mut agents = Vec::new();
        for target in targets.into_iter().take(limit) {
            let record = self.require_config_kind(target, ConfigKind::Agent)?;
            let ConfigBody::Agent(agent) = record.body else {
                return self.corrupted("Agent kind mismatch".into());
            };
            let instructions = agent.instructions.as_deref().unwrap_or("");
            let summary: String = instructions
                .chars()
                .take(MAX_INSTRUCTION_SUMMARY_CHARS)
                .collect();
            let model = match agent.default_model() {
                Some(model) => {
                    let record = self.require_config(model)?;
                    match record.body {
                        ConfigBody::Model(body) => json!({
                            "profile": ProfileId::new(model.get()).to_string(),
                            "name": record.name,
                            "model": body.model,
                            "declared_capabilities": body.declared_capabilities.iter().map(|capability| match capability {
                                ModelCapability::Chat => "chat",
                                ModelCapability::Tools => "tools",
                                ModelCapability::Vision => "vision",
                                ModelCapability::Reasoning => "reasoning",
                            }).collect::<Vec<_>>(),
                            "enabled": record.model_enabled,
                            "archived": record.archived_at.is_some(),
                        }),
                        ConfigBody::DecisionModel(body) => json!({
                            "profile": ProfileId::new(model.get()).to_string(),
                            "name": record.name,
                            "model": body.model,
                            "kind": "decision_model",
                            "declared_capabilities": body.declared_capabilities,
                            "archived": record.archived_at.is_some(),
                        }),
                        ConfigBody::Agent(_)
                        | ConfigBody::DecisionAgent(_)
                        | ConfigBody::Connection(_)
                        | ConfigBody::Pool(_) => {
                            return self.corrupted("invalid Agent model target".into());
                        }
                    }
                }
                None => Value::Null,
            };
            agents.push(json!({
                "agent": ProfileId::new(target.get()).to_string(),
                "name": record.name,
                "archived": record.archived_at.is_some(),
                "instructions_summary": summary,
                "instructions_truncated": summary.len() < instructions.len(),
                "capabilities": agent.capabilities,
                "default_model": model,
            }));
        }
        Ok(json!({
            "agents": agents,
            "has_more": has_more,
            "next_after": next_after.map(|id| ProfileId::from(id).to_string()),
        }))
    }

    fn job_worker_catalog(
        &mut self,
        caller: JobId,
        after: Option<String>,
        limit: Option<u32>,
    ) -> Result<Value, BatchError> {
        let limit = self.job_tool_page(limit)?;
        let after = after
            .map(|id| self.job_tool_id(&id))
            .transpose()?
            .unwrap_or(0);
        if after == u128::MAX {
            return Ok(json!({"workers": [], "has_more": false, "next_after": null}));
        }
        let ids = self.child_conversations(caller, after, limit + 1)?;
        let has_more = ids.len() > limit;
        let mut workers = Vec::new();
        for worker in ids.into_iter().take(limit) {
            let row = self.require_mutable_conversation(worker)?;
            let JobKind::Group { agent, project, .. } = row.kind else {
                return self.corrupted("child is not a conversation".into());
            };
            workers.push(json!({
                "conversation": worker.to_string(),
                "agent": agent.map(|id| ProfileId::new(id.get()).to_string()),
                "project": project.map(|id| id.to_string()),
                "title": self.job_control(worker)?.title,
                "idle": self.idle_worker(caller, worker)?,
            }));
        }
        let next_after = if has_more {
            workers.last().map(|worker| worker["conversation"].clone())
        } else {
            None
        };
        Ok(json!({"workers": workers, "has_more": has_more, "next_after": next_after}))
    }
}

#[derive(Clone, Copy)]
enum ConversationAction {
    Stop,
    Close,
    Hold,
    Unhold,
}
