use crate::db::entities::*;
use crate::db::entities::{
    ConfigBody, ConfigKind, ExecutionConfiguration, JobDispatchResult, JobPlanReceipt, JobPlanStep,
    JobToolCall, ToolCallRef, ToolOutcome, ToolPayload, Violation,
};
use crate::db::{Batch, BatchError, Failure, StorageError};
use serde_json::{Value, json};
use std::collections::BTreeSet;

impl Batch {
    pub(crate) fn validate_dispatch_selection(
        &mut self,
        selection: &DispatchSelection,
    ) -> Result<(), BatchError> {
        if selection.projects.len() > 16 {
            return self.reject(Violation::InvalidValue(
                "projects",
                "select at most 16 projects",
            ));
        }
        let model = ConfigId::new(self.job_tool_id(&selection.model, "profile:")?);
        let record = self.require_config_kind(model, ConfigKind::Model)?;
        if !record.model_enabled {
            return self.reject(Violation::InvalidValue(
                "dispatch_selection",
                "the selected model is disabled",
            ));
        }
        let ConfigBody::Model(body) = &record.body else {
            return self.corrupted("invalid dispatch model".into());
        };
        if let Some(agent) = &selection.agent {
            let agent = ConfigId::new(self.job_tool_id(agent, "profile:")?);
            let config = self.require_config_kind(agent, ConfigKind::Agent)?;
            let ConfigBody::Agent(agent) = config.body else {
                return self.corrupted("invalid dispatch Agent".into());
            };
            if !agent.models.contains(&model)
                || !agent.supports_model(&record.body)
                || (!agent.manages_jobs() && agent.workspace_grant().is_none())
            {
                return self.reject(Violation::InvalidValue(
                    "dispatch_selection",
                    "the selected Agent cannot run this model",
                ));
            }
            if agent.workspace_grant().is_some() && selection.projects.is_empty() {
                return self.reject(Violation::ProjectRequired);
            }
        } else if !body.declared_capabilities.contains(&ModelCapability::Chat)
            || !body.declared_capabilities.contains(&ModelCapability::Tools)
            || selection.projects.is_empty()
        {
            return self.reject(Violation::InvalidValue(
                "dispatch_selection",
                "model-only workspace jobs require chat, tools and a project",
            ));
        }
        let mut projects = BTreeSet::new();
        for project in &selection.projects {
            let project = ProjectId::new(self.job_tool_id(project, "project:")?);
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

    fn job_tool_page(&mut self, limit: Option<u32>) -> Result<usize, BatchError> {
        match limit.unwrap_or(10) {
            limit @ 1..=20 => Ok(limit as usize),
            _ => self.reject(Violation::InvalidValue("limit", "must be between 1 and 20")),
        }
    }

    fn idle_worker(&mut self, caller: JobId, worker: JobId) -> Result<bool, BatchError> {
        let row = self.require_agent_group(worker)?;
        if row.parent_id != Some(caller)
            || !matches!(
                row.kind,
                JobKind::Group {
                    origin: Origin::Agent { .. },
                    keep_open: true,
                    ..
                }
            )
        {
            return Ok(false);
        }
        let metadata = self.job_control(worker)?;
        if metadata.closing.is_some()
            || metadata.on_hold.is_some()
            || self.require_job_state(worker)?.outcome.is_some()
            || self.active_job(worker)?.is_some()
            || self.recurrence(worker)?.is_some()
            || self.has_user_input(worker)?
        {
            return Ok(false);
        }
        match self.group_head(worker)? {
            Some(tip) => Ok(self.require_job_state(tip)?.outcome.is_some()),
            None => Ok(false),
        }
    }

    fn job_agent_catalog(
        &mut self,
        _turn: &JobSpec,
        after: Option<String>,
        limit: Option<u32>,
    ) -> Result<Value, BatchError> {
        let limit = self.job_tool_page(limit)?;
        let after = after
            .map(|id| self.job_tool_id(&id, "profile:"))
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
            let summary: String = instructions.chars().take(256).collect();
            let model = match agent.default_model() {
                Some(model) => {
                    let record = self.require_config(model)?;
                    match record.body {
                        ConfigBody::Model(body) => {
                            json!({"profile":ProfileId::new(model.get()).to_string(), "name":record.name, "model":body.model, "declared_capabilities":body.declared_capabilities.iter().map(|cap| match cap { ModelCapability::Chat => "chat", ModelCapability::Tools => "tools", ModelCapability::Vision => "vision", ModelCapability::Reasoning => "reasoning" }).collect::<Vec<_>>(), "enabled":record.model_enabled, "archived":record.archived_at.is_some()})
                        }
                        ConfigBody::DecisionModel(body) => {
                            json!({"profile":ProfileId::new(model.get()).to_string(), "name":record.name, "model":body.model, "kind":"decision_model", "declared_capabilities":body.declared_capabilities, "archived":record.archived_at.is_some()})
                        }
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
            agents.push(json!({"agent":ProfileId::new(target.get()).to_string(), "name":record.name, "archived":record.archived_at.is_some(), "instructions_summary":summary, "instructions_truncated":summary.len() < instructions.len(), "capabilities":agent.capabilities, "default_model":model}));
        }
        Ok(
            json!({"agents":agents,"has_more":has_more,"next_after":next_after.map(|id| ProfileId::from(id).to_string())}),
        )
    }

    fn job_worker_catalog(
        &mut self,
        caller: JobId,
        after: Option<String>,
        limit: Option<u32>,
    ) -> Result<Value, BatchError> {
        let limit = self.job_tool_page(limit)?;
        let after = after
            .map(|id| self.job_tool_id(&id, "job:"))
            .transpose()?
            .unwrap_or(0);
        if after == u128::MAX {
            return Ok(json!({"workers":[], "has_more":false, "next_after":null}));
        }
        let ids = self.child_groups(caller, after, limit + 1)?;
        let has_more = ids.len() > limit;
        let mut workers = Vec::new();
        for worker in ids.into_iter().take(limit) {
            let row = self.require_agent_group(worker)?;
            let JobKind::Group { agent, project, .. } = row.kind else {
                return self.corrupted("child is not a conversation".into());
            };
            workers.push(json!({"conversation":worker.to_string(), "agent":agent.map(|id|ProfileId::new(id.get()).to_string()), "project":project.map(|id|id.to_string()), "title":self.job_control(worker)?.title, "idle":self.idle_worker(caller,worker)?}));
        }
        let next_after = if has_more {
            workers.last().map(|worker| worker["conversation"].clone())
        } else {
            None
        };
        Ok(json!({"workers":workers, "has_more":has_more, "next_after":next_after}))
    }

    fn job_tool_id(&mut self, text: &str, _prefix: &'static str) -> Result<u128, BatchError> {
        match crate::db::entities::parse_entity_id(text) {
            Some(id) => Ok(id),
            None => self.reject(Violation::InvalidValue(
                "invalid_target",
                "expected a canonical UUID",
            )),
        }
    }
    pub(crate) fn job_tool_rights(
        &mut self,
        caller: JobId,
        target: JobId,
    ) -> Result<(), BatchError> {
        let target_row = self.require_agent_group(target)?;
        let mut parent = target_row.parent_id;
        let mut below = false;
        for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
            match parent {
                Some(id) if id == caller => {
                    below = true;
                    break;
                }
                Some(id) => {
                    self.require_agent_group(id)?;
                    parent = self.require_job(id)?.parent_id;
                }
                None => break,
            }
        }
        if !below {
            return self.reject(Violation::InvalidValue(
                "rights",
                "the target must be in the caller's strict subtree",
            ));
        }
        for conversation in self.live_subtree(target)? {
            self.require_agent_group(conversation)?;
            let mut turns = BTreeSet::new();
            if let Some(tip) = self.group_head(conversation)? {
                let state = self.require_job_state(tip)?;
                if state.claim.is_none() && state.outcome.is_none() {
                    turns.insert(tip);
                }
            }
            if let Some(active) = self.active_job(conversation)? {
                turns.insert(active);
            }
            for turn in turns {
                if self.has_user_message(turn)? {
                    return self.reject(Violation::InvalidValue("user_turn", "a queued or running user turn in the target subtree prevents agent management"));
                }
            }
        }
        Ok(())
    }
    fn dispatch_plan(
        &mut self,
        caller_turn: &JobSpec,
        call: ToolCallRef,
        steps: Vec<JobPlanStep>,
    ) -> Result<Value, BatchError> {
        if !(1..=6).contains(&steps.len())
            || steps.iter().any(|step| {
                step.title.trim().is_empty()
                    || step.title.chars().count() > 120
                    || step.objective.trim().is_empty()
                    || step.objective.chars().count() > 2000
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
        let Some(caller) = caller_turn.parent_id else {
            return self.corrupted("task planner has no conversation".into());
        };
        if self.group_head(caller)? != Some(caller_turn.id) {
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
        if self.group_allocation(caller)?.is_some() {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "allocated jobs cannot create another plan",
            ));
        }
        let (frozen, project, agent) = self.job_execution(caller_turn)?;
        if frozen.agent.is_none() {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "task planning requires a frozen Agent",
            ));
        }
        if steps.len() == 1 {
            return serde_json::to_value(JobPlanReceipt {
                steps: Vec::new(),
                continuation: caller_turn.id.to_string(),
            })
            .map_err(|_| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "task receipt serialization",
                )))
            });
        }
        let mut allocated = Vec::new();
        let mut conversations = Vec::new();
        for step in steps {
            let execution = frozen.clone();
            let model = execution.llm.model.id;
            let dependencies = step
                .depends_on
                .iter()
                .map(|index| {
                    conversations
                        .get(*index)
                        .copied()
                        .map(|id| (id, DependencyCondition::Settled))
                        .ok_or(BatchError(Failure::Rejected(Violation::InvalidValue(
                            "task_dependencies",
                            "dependency has not been allocated",
                        ))))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (conversation, appended) = self.open_group_with_execution(
                Some(caller),
                JobGroup {
                    name: Some(step.title),
                    agent,
                    project,
                    origin: Origin::Agent { call },
                    keep_open: false,
                },
                Entry::AgentMessage {
                    text: step.objective,
                    call,
                },
                ExecutionConfiguration {
                    reasoning_effort: None,
                    agent_profile: agent.map(Into::into),
                    llm_profile: model.into(),
                },
                None,
                dependencies,
                None,
                Some(JobExecution::Frozen(execution)),
                Some(crate::db::entities::JobAllocation {
                    owner: call.job,
                    synthesis: false,
                }),
            )?;
            conversations.push(conversation);
            allocated.push(JobDispatchResult {
                conversation: conversation.to_string(),
                turn: appended.turn.to_string(),
            });
        }
        let dependencies = allocated
            .iter()
            .map(|step| {
                step.conversation
                    .parse::<JobId>()
                    .map(|id| (id, DependencyCondition::Settled))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "task allocation identity",
                )))
            })?;
        let continuation = self.append_named(
            caller,
            Entry::AgentMessage { text: "Synthesize the completed task phases. Use the recorded child results already provided in context. Report verified findings and any failed or incomplete scopes; do not restart exploration or claim that a failed phase completed.".into(), call },
            None, None, false, Some("Task synthesis".into()), dependencies, None, None,
         Some(crate::db::entities::JobAllocation { owner: call.job, synthesis: true }),)?;
        let receipt = JobPlanReceipt {
            steps: allocated,
            continuation: continuation.turn.to_string(),
        };
        serde_json::to_value(receipt).map_err(|_| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "task receipt serialization",
            )))
        })
    }

    pub(crate) fn apply_job_tool(
        &mut self,
        call: ToolCallRef,
        tool: JobToolCall,
    ) -> Result<Value, BatchError> {
        self.require_executing_here(call.job)?;
        let Some(evidence) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if evidence.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        if evidence
            .admission()
            .is_none_or(|admission| admission.handler_version != tool.handler_version())
            || evidence.name != tool.name()
        {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        let caller_turn = self.require_job(call.job)?;
        if self
            .job_execution(&caller_turn)?
            .0
            .agent
            .as_ref()
            .is_none_or(|agent| !agent.manages_jobs())
        {
            return self.reject(Violation::InvalidValue(
                "capabilities",
                "Job management requires Chat capability",
            ));
        }
        if !caller_turn.kind.is_persistent() {
            return self.reject(Violation::InvalidValue(
                "stored_turn_required",
                "job tools require a stored turn",
            ));
        }
        let Some(caller) = caller_turn.parent_id else {
            return self.corrupted("stored caller has no conversation".into());
        };
        let result = match tool {
            JobToolCall::JobPlan { steps } => self.dispatch_plan(&caller_turn, call, steps)?,
            JobToolCall::JobLimits {
                conversation,
                max_depth,
                max_auto_turns,
                max_iterations,
            } => {
                let target = match conversation {
                    Some(id) => JobId::new(self.job_tool_id(&id, "job:")?),
                    None => caller,
                };
                if target != caller {
                    self.job_tool_rights(caller, target)?;
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
                json!({"conversation":target.to_string(), "depth":depth, "remaining_depth":limits.max_depth.saturating_sub(depth), "max_depth":limits.max_depth,"max_auto_turns":limits.max_auto_turns,"max_iterations":limits.max_iterations})
            }
            JobToolCall::JobAgents { after, limit } => {
                self.job_agent_catalog(&caller_turn, after, limit)?
            }
            JobToolCall::JobWorkers { after, limit } => {
                self.job_worker_catalog(caller, after, limit)?
            }
            JobToolCall::JobAssign {
                conversation,
                objective,
            } => {
                if evidence.approval.is_none() {
                    return self.reject(Violation::InvalidValue(
                        "dispatch_selection",
                        "worker assignment requires user confirmation",
                    ));
                }
                let worker = JobId::new(self.job_tool_id(&conversation, "job:")?);
                self.job_tool_rights(caller, worker)?;
                if !self.idle_worker(caller, worker)? {
                    return self.reject(Violation::InvalidValue(
                        "worker_not_idle",
                        "the target is not an idle worker owned by this parent",
                    ));
                }
                if objective.trim().is_empty() {
                    return self.reject(Violation::InvalidValue("objective", "must not be empty"));
                }
                let appended = self.append(
                    worker,
                    Entry::AgentMessage {
                        text: objective,
                        call,
                    },
                    None,
                    None,
                    false,
                )?;
                json!({"conversation":worker.to_string(), "turn":appended.turn.to_string()})
            }
            JobToolCall::JobDispatch {
                projects: _,
                project: _,
                objective,
                keep_open,
                every_seconds,
                depends_on,
            } => {
                let approval = evidence.approval.ok_or(BatchError(Failure::Rejected(
                    Violation::InvalidValue(
                        "dispatch_selection",
                        "dispatch requires the user's target selection",
                    ),
                )))?;
                let selection = self.dispatch_selection(approval)?;
                self.validate_dispatch_selection(&selection)?;
                let agent = selection
                    .agent
                    .as_deref()
                    .map(|id| self.job_tool_id(id, "profile:").map(ConfigId::new))
                    .transpose()?;
                let model = ConfigId::new(self.job_tool_id(&selection.model, "profile:")?);
                if objective.trim().is_empty() {
                    return self.reject(Violation::InvalidValue("objective", "must not be empty"));
                }
                if every_seconds.is_some_and(|interval| !(60..=2_592_000).contains(&interval)) {
                    return self.reject(Violation::InvalidValue(
                        "every_seconds",
                        "must be between 60 and 2592000",
                    ));
                }
                if every_seconds.is_some() && self.group_limits(caller)?.0.max_iterations == 0 {
                    return self.reject(Violation::InvalidValue(
                        "max_iterations",
                        "scheduled recurrence is disabled by the conversation limits",
                    ));
                }
                let selected_projects = if selection.projects.is_empty() {
                    vec![None]
                } else {
                    selection
                        .projects
                        .iter()
                        .map(|project| {
                            self.job_tool_id(project, "project:")
                                .map(|id| Some(ProjectId::new(id)))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                let mut dependencies = Vec::new();
                for dependency in depends_on {
                    let id = JobId::new(self.job_tool_id(&dependency.conversation, "job:")?);
                    let row = self.require_job(id)?;
                    if !matches!(
                        row.kind,
                        JobKind::Group {
                            keep_open: false,
                            ..
                        }
                    ) || row.parent_id != Some(caller)
                        || self.has_user_input(id)?
                    {
                        return self.reject(Violation::InvalidValue("depends_on", "dependencies must be sibling conversations with keep_open false and no user message"));
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
                    dependencies.push((id, condition));
                }
                dependencies.sort_by_key(|(id, _)| *id);
                if dependencies.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                    return self.reject(Violation::InvalidValue(
                        "depends_on",
                        "dependencies must be unique",
                    ));
                }
                let mut dispatched = Vec::new();
                for project in selected_projects {
                    let (conversation, appended) = self.open_group(
                        Some(caller),
                        JobGroup {
                            name: None,
                            agent,
                            project,
                            origin: Origin::Agent { call },
                            keep_open: keep_open || every_seconds.is_some(),
                        },
                        Entry::AgentMessage {
                            text: objective.clone(),
                            call,
                        },
                        ExecutionConfiguration {
                            reasoning_effort: None,
                            agent_profile: agent.map(Into::into),
                            llm_profile: model.into(),
                        },
                        None,
                        dependencies.clone(),
                    )?;
                    if let Some(interval_s) = every_seconds {
                        self.put_recurrence(
                            conversation,
                            &Recurrence {
                                phase: RecurrencePhase::Active,
                                n: 1,
                                current_turn: appended.turn,
                                previous_turn: None,
                                interval_s,
                            },
                            None,
                        )?;
                    }
                    dispatched.push(json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()}));
                }
                json!({"jobs":dispatched})
            }
            JobToolCall::JobRetry { turn } => {
                let turn = JobId::new(self.job_tool_id(&turn, "job:")?);
                let row = self.require_job(turn)?;
                let Some(conversation) = row.parent_id.filter(|_| row.kind.is_persistent()) else {
                    return self.reject(Violation::InvalidValue("turn", "expected a stored turn"));
                };
                self.job_tool_rights(caller, conversation)?;
                let appended = self.retry(
                    conversation,
                    turn,
                    Actor::Agent {
                        conversation: caller,
                    },
                )?;
                json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()})
            }
            JobToolCall::JobMessage { conversation, text } => {
                let conversation = JobId::new(self.job_tool_id(&conversation, "job:")?);
                self.job_tool_rights(caller, conversation)?;
                let appended = self.append(
                    conversation,
                    Entry::AgentMessage { text, call },
                    None,
                    None,
                    false,
                )?;
                json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()})
            }
            tool => {
                let (conversation, action) = match tool {
                    JobToolCall::JobStop { conversation } => (conversation, "stop"),
                    JobToolCall::JobClose { conversation } => (conversation, "close"),
                    JobToolCall::JobHold { conversation } => (conversation, "hold"),
                    JobToolCall::JobUnhold { conversation } => (conversation, "unhold"),
                    _ => return self.corrupted("unexpected management tool".into()),
                };
                let conversation = JobId::new(self.job_tool_id(&conversation, "job:")?);
                self.job_tool_rights(caller, conversation)?;
                let by = Actor::Agent {
                    conversation: caller,
                };
                let effect = match action {
                    "stop" => self.stop_turn(conversation, by)?,
                    "close" => self.close(conversation, by)?,
                    "hold" => self.hold(conversation, by, false)?,
                    _ => self.hold(conversation, by, true)?,
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
                json!({"conversation":conversation.to_string(), "status":status, "record_seq":effect.record_seq, "running":effect.running.iter().map(ToString::to_string).collect::<Vec<_>>()})
            }
        };
        let text = result.to_string();
        let outcome = ToolOutcome::Completed {
            result: ToolPayload::new(text),
        };
        self.tool_outcome(call, outcome)?;
        Ok(result)
    }
}
