use crate::db::entities::{
    Actor, AgentCapability, ApprovalDecision, ConfigId, ContentDigest, DependencyCondition,
    DependencyState, ExecutionOperation, JobId, JobLimits, LlmTransport, Origin, ProfileId,
    ProjectId,
};
use crate::db::entities::{
    AllocationToken, ConfigBody, ConfigKind, DispatchSelection, ExecutionConfiguration,
    FrozenConfiguration, JobDependency, JobPlanRoute, JobPlanStep, JobSpec, JobToolCall,
    ModelCapability, MutableJob, MutableJobContent, ReasoningEffort, ToolAdmission, ToolCallRef,
    ToolDecision, ToolEffect, ToolEvidenceRecord, ToolOutcome, ToolPayload, Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::rows::{ConfigRow, decode_project};
use crate::db::redb::schema::{CONFIGS, EXECUTORS, POOLS, PROJECTS, RECURRENCES};
use crate::db::redb::{StorageError, TxRead, allocations, configs, job_tree};
use ::redb::ReadableTable;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

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

pub(super) struct RoutedPlan {
    pub(super) arguments: String,
    pub(super) confirmation: String,
    targets: Vec<(ExecutionConfiguration, Option<ProjectId>)>,
}

fn selected_model_matches(selector: Option<&str>, id: ConfigId, name: &str) -> bool {
    selector.is_none_or(|value| {
        value.eq_ignore_ascii_case(name)
            || crate::db::entities::parse_entity_id(value) == Some(id.get())
    })
}

fn enabled_llm_transports(tx: &impl TxRead) -> Result<BTreeSet<LlmTransport>, StorageError> {
    let pools = tx.table(POOLS)?;
    let executors = tx.table(EXECUTORS)?;
    let mut transports = BTreeSet::new();
    for entry in executors.iter()? {
        let (key, value) = entry?;
        let executor = allocations::decode_allocation_executor(key.value(), value.value())?;
        if !executor.enabled || executor.archived {
            continue;
        }
        let pool = pools
            .get(executor.pool.get())?
            .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
        let pool = allocations::decode_allocation_pool(executor.pool.get(), pool.value())?;
        if pool.enabled && !pool.archived {
            transports.extend(executor.capabilities.iter().filter_map(|&(op, transport)| {
                (op == ExecutionOperation::LlmRequest)
                    .then_some(transport)
                    .flatten()
            }));
        }
    }
    Ok(transports)
}

fn choose_plan_target(
    tx: &impl TxRead,
    configs: &BTreeMap<ConfigId, ConfigRow>,
    transports: &BTreeSet<LlmTransport>,
    current: ExecutionConfiguration,
    route: &JobPlanRoute,
) -> Result<ExecutionConfiguration, BatchError> {
    let model_capabilities = route
        .model_capabilities
        .iter()
        .map(|value| ModelCapability::parse(value))
        .collect::<Option<BTreeSet<_>>>()
        .ok_or(Violation::InvalidValue("route", "unknown model capability"))?;
    let effort = route
        .reasoning_effort
        .as_deref()
        .map(|value| {
            ReasoningEffort::parse(value).ok_or(Violation::InvalidValue(
                "reasoning_effort",
                "invalid plan effort",
            ))
        })
        .transpose()?;
    let mut best = None;
    for row in configs.values() {
        let ConfigBody::Agent(agent) = &row.body else {
            continue;
        };
        if !agent.capabilities.contains(&route.agent_capability)
            || agent.workspace_grant().is_some()
                != (route.agent_capability == AgentCapability::Coding)
        {
            continue;
        }
        for (index, id) in agent.models.iter().enumerate() {
            let Some(model) = configs.get(id).filter(|row| row.model_enabled) else {
                continue;
            };
            let ConfigBody::Model(model_body) = &model.body else {
                continue;
            };
            let Some(provider) = configs.get(&model_body.connection_id) else {
                continue;
            };
            let ConfigBody::Connection(connection) = &provider.body else {
                continue;
            };
            if !transports.contains(&connection.protocol)
                || !agent.supports_model(&model.body)
                || !model_capabilities.is_subset(&model_body.declared_capabilities)
                || !selected_model_matches(route.model.as_deref(), model.id, &model_body.model)
                || !selected_model_matches(route.provider.as_deref(), provider.id, &provider.name)
                || effort.is_some_and(|effort| {
                    connection.protocol == LlmTransport::AnthropicMessages
                        || !model_body
                            .declared_capabilities
                            .contains(&ModelCapability::Reasoning)
                        || model_body.reasoning_efforts.as_ref().is_some_and(|values| {
                            !values.iter().any(|value| value == effort.as_str())
                        })
                })
            {
                continue;
            }
            let candidate = ExecutionConfiguration {
                agent_profile: Some(row.id.into()),
                llm_profile: model.id.into(),
                reasoning_effort: effort,
            };
            let rank = (
                effort.is_some() && model_body.reasoning_efforts.is_none(),
                candidate.llm_profile != current.llm_profile,
                candidate.agent_profile != current.agent_profile,
                index,
                row.id,
                *id,
            );
            if best.as_ref().is_some_and(|(previous, _)| *previous <= rank) {
                continue;
            }
            match configs::freeze_configuration(tx, candidate) {
                Ok(FrozenConfiguration::Generative(config))
                    if config.native_execution_refusal().is_none() =>
                {
                    best = Some((rank, candidate));
                }
                Ok(_) | Err(BatchError(Failure::Rejected(_))) => continue,
                Err(error) => return Err(error),
            }
        }
    }
    best.map(|(_, configuration)| configuration).ok_or_else(|| {
        Violation::InvalidValue("route", "no enabled Agent and Model satisfy the plan step").into()
    })
}

pub(super) fn route_plan(
    tx: &impl TxRead,
    job: &MutableJob,
    spec: &JobSpec,
    steps: &[JobPlanStep],
) -> Result<RoutedPlan, BatchError> {
    let MutableJobContent::Generative {
        configuration: Some(current),
        ..
    } = job.input.content
    else {
        return Err(Violation::InvalidValue("route", "the parent has no generative target").into());
    };
    let mut configs = BTreeMap::new();
    for row in tx
        .table(CONFIGS)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (id, value) = row.map_err(StorageError::from)?;
        let row = ConfigRow::decode_at(id.value(), value.value())?;
        if row.archived_at.is_none() {
            configs.insert(row.id, row);
        }
    }
    let transports = enabled_llm_transports(tx)?;
    let mut targets = Vec::with_capacity(steps.len());
    let mut proposed = Vec::with_capacity(steps.len());
    let mut revisions = Vec::with_capacity(steps.len());
    for step in steps {
        let (configuration, project) = match &step.route {
            Some(route) => {
                let project = match route.agent_capability {
                    AgentCapability::Chat => None,
                    AgentCapability::Coding => {
                        Some(spec.project.ok_or(Violation::ProjectRequired)?)
                    }
                    AgentCapability::Decision => {
                        return Err(
                            Violation::InvalidValue("route", "expected chat or coding").into()
                        );
                    }
                };
                (
                    choose_plan_target(tx, &configs, &transports, current, route)?,
                    project,
                )
            }
            None => (current, spec.project),
        };
        let FrozenConfiguration::Generative(frozen) =
            configs::freeze_configuration(tx, configuration)?
        else {
            return Err(
                Violation::InvalidValue("route", "the plan target is not generative").into(),
            );
        };
        let Some(agent) = frozen.agent_ref.and_then(|id| configs.get(&id.id)) else {
            return Err(Violation::InvalidValue("route", "the plan target has no Agent").into());
        };
        let Some(model) = configs.get(&frozen.llm.model.id) else {
            return Err(Violation::InvalidValue("route", "the plan target has no Model").into());
        };
        let project = project
            .map(|id| {
                let table = tx.table(PROJECTS).map_err(StorageError::from)?;
                let bytes = table
                    .get(id.get())
                    .map_err(StorageError::from)?
                    .ok_or_else(|| StorageError::corrupted("plan Project is missing"))?;
                decode_project(id.get(), bytes.value())
            })
            .transpose()?;
        proposed.push(json!({
            "title": step.title,
            "objective": step.objective,
            "depends_on": step.depends_on,
            "provider": frozen.llm.provider.name,
            "model": frozen.llm.config.model,
            "agent": agent.name,
            "agent_capabilities": frozen.agent.as_ref().map(|agent| &agent.capabilities),
            "model_capabilities": frozen.llm.config.declared_capabilities.iter().map(|capability| capability.as_str()).collect::<Vec<_>>(),
            "reasoning_effort": frozen.llm.config.parameters.reasoning_effort,
            "project": project.as_ref().map(|p| &p.name),
        }));
        revisions.push(json!([
            [agent.id.to_string(), agent.edit_seq],
            [model.id.to_string(), model.edit_seq],
            [
                frozen.llm.provider.id.to_string(),
                frozen.llm.provider.edit_seq
            ],
            project.as_ref().map(|p| (
                p.id.to_string(),
                p.updated_at.as_unix_micros(),
                p.decision_revision
            )),
        ]));
        targets.push((configuration, project.map(|p| p.id)));
    }
    let arguments = json!({"steps": proposed}).to_string();
    let confirmation = ContentDigest::of(
        json!([job.id.to_string(), arguments, revisions])
            .to_string()
            .as_bytes(),
    )
    .to_string();
    Ok(RoutedPlan {
        targets,
        arguments,
        confirmation,
    })
}

impl Batch {
    fn job_tool_id(&mut self, text: &str) -> Result<u128, BatchError> {
        match crate::db::entities::parse_entity_id(text) {
            Some(id) => Ok(id),
            None => self.reject(Violation::InvalidValue(
                "invalid_target",
                "expected a lowercase hyphenated UUID",
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
        proof: &crate::db::handoff::DevelopmentWriteProof,
        call: ToolCallRef,
        tool: JobToolCall,
        retain_read_only_result: bool,
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
        if admission.handler_version != tool.handler_version()
            || evidence.name != tool.name()
            || (admission.effect == ToolEffect::ReadOnly
                && matches!(&tool, JobToolCall::JobLimits { .. })
                && !tool.reads_limits())
        {
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
            JobToolCall::JobPlan { steps } => {
                let spec = self.check_job_plan(&job, call, &steps)?;
                if steps.iter().any(|step| step.route.is_some()) {
                    return self.reject(Violation::InvalidValue(
                        "task_plan",
                        "a routed plan requires user confirmation",
                    ));
                }
                if steps.len() == 1 {
                    JobToolApplied::of(json!({
                        "steps": [],
                        "continuation": job.id.to_string(),
                        "next": "one local step: continue directly in this Job",
                    }))
                } else {
                    let (configuration, _) = self.conversation_configuration(spec.id)?;
                    let targets = vec![(configuration, spec.project); steps.len()];
                    JobToolApplied::of(self.launch_job_plan(&job, call, steps, targets)?)
                }
            }
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
                let turn = self.agent_turn(target, text, call, b"message", caller)?;
                JobToolApplied::of(
                    json!({"conversation": target.to_string(), "turn": turn.to_string()}),
                )
            }
            JobToolCall::JobReportProgress { text } => {
                let (parent, turn) = self.post_child_notice(caller, call, &text)?;
                JobToolApplied::of(match turn {
                    Some(turn) => json!({"parent": parent.to_string(), "turn": turn.to_string()}),
                    None => json!({"parent": parent.to_string(), "deferred": true}),
                })
            }
            JobToolCall::JobAnswerChild {
                turn,
                request,
                index,
                text,
            } => {
                let child_turn = JobId::new(self.job_tool_id(&turn)?);
                let appended = self.answer_child_help(
                    caller,
                    ToolCallRef {
                        job: child_turn,
                        request,
                        index,
                    },
                    &text,
                )?;
                JobToolApplied::of(json!({"turn": appended.turn.to_string()}))
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
            JobToolCall::JobAssign { .. }
            | JobToolCall::JobDispatch { .. }
            | JobToolCall::JobAskParent { .. } => {
                return self.reject(Violation::InvalidValue(
                    "tool",
                    "this call is decided by the user, not applied by the host",
                ));
            }
        };
        let text = applied.value.to_string();
        let outcome = if effect == ToolEffect::ReadOnly && !retain_read_only_result {
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
        if self.job_lifecycle(conversation)?.closing.is_some() {
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

    fn check_job_plan(
        &mut self,
        job: &MutableJob,
        call: ToolCallRef,
        steps: &[JobPlanStep],
    ) -> Result<JobSpec, BatchError> {
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
            let work = match &previous.decision.fact {
                ToolDecision::Admitted(admission) => {
                    admission.effect != crate::db::entities::ToolEffect::ReadOnly
                }
                ToolDecision::Refused(_) => false,
                ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_) => true,
            };
            work && !refused_plan
        });
        if work_started {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "a task plan must precede execution",
            ));
        }
        let spec = self.require_mutable_conversation(conversation)?;
        if let Origin::Agent { call: opened } = spec.origin
            && self
                .tool_evidence(opened)?
                .is_some_and(|opening| opening.name == "job_plan")
        {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "the steps of a plan cannot create another plan",
            ));
        }
        Ok(spec)
    }

    fn launch_job_plan(
        &mut self,
        job: &MutableJob,
        call: ToolCallRef,
        steps: Vec<JobPlanStep>,
        targets: Vec<(ExecutionConfiguration, Option<ProjectId>)>,
    ) -> Result<Value, BatchError> {
        let conversation = job.parent_id;
        let priority = job.input.priority;
        let mut opened: Vec<JobId> = Vec::new();
        let mut receipts = Vec::new();
        for (index, (step, (configuration, project))) in steps.into_iter().zip(targets).enumerate()
        {
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
            let child = self.open_child_conversation(
                conversation,
                call,
                configuration.agent_profile.map(Into::into),
                project,
                false,
                after,
            )?;
            let mut lifecycle = self.job_lifecycle(child)?;
            lifecycle.title = Some(step.title);
            self.put_job_lifecycle(child, &lifecycle)?;
            let n = u32::try_from(index).unwrap_or(u32::MAX);
            let turn = self.host_turn(
                child,
                job_tree::host_input(step.objective, configuration, priority),
                job_tree::tool_submission(call, b"plan create", n),
                job_tree::tool_submission(call, b"plan submit", n),
                Actor::Agent { conversation },
            )?;
            opened.push(child);
            receipts.push(json!({"conversation": child.to_string(), "turn": turn.to_string()}));
        }
        Ok(json!({
            "steps": receipts,
            "next": "the host adds a message to this conversation with every step's result once all have settled; end your turn now",
        }))
    }

    pub(super) fn check_routed_plan(
        &mut self,
        call: ToolCallRef,
        arguments: &str,
    ) -> Result<(), BatchError> {
        let Some(JobToolCall::JobPlan { steps }) = JobToolCall::parse("job_plan", arguments) else {
            return self.reject(Violation::InvalidValue(
                "task_plan",
                "invalid plan arguments",
            ));
        };
        let Some(job) = self.mutable_job(call.job)? else {
            return self.corrupted("a routed plan was proposed by a non-native Job".into());
        };
        if !steps.iter().any(|step| step.route.is_some())
            || !self.frozen_agent_manages_jobs(&job)?
        {
            return self.reject(Violation::InvalidValue("task_plan", "invalid routed plan"));
        }
        let spec = self.check_job_plan(&job, call, &steps)?;
        self.inspect(|tx| route_plan(tx, &job, &spec, &steps))?;
        Ok(())
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
            (JobToolCall::JobPlan { steps }, ApprovalDecision::Answered { text }) => {
                let Some(job) = self.mutable_job(approval.job)? else {
                    return self.corrupted("a routed plan was proposed by a non-native Job".into());
                };
                let spec = self.require_mutable_conversation(conversation)?;
                let plan = self.inspect(|tx| route_plan(tx, &job, &spec, &steps))?;
                if plan.confirmation != *text {
                    return self.reject(Violation::InvalidValue(
                        "plan_routes_changed",
                        "the routing proposal changed; review the current targets",
                    ));
                }
                let dispatched = self.launch_job_plan(&job, approval, steps, plan.targets)?;
                Ok(Some(format!(
                    "I confirmed the routed plan. The host started the child tasks: {dispatched}. Wait for their automatic results before announcing completion."
                )))
            }
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
                let turn = self.agent_turn(worker, objective, approval, b"assign", conversation)?;
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
        let configuration = self.dispatch_configuration(selection)?;
        if objective.trim().is_empty() {
            return self.reject(Violation::InvalidValue("objective", "must not be empty"));
        }
        if let Some(interval_s) = every_seconds
            && !(60..=2_592_000).contains(&interval_s)
        {
            return self.reject(Violation::InvalidValue(
                "every_seconds",
                "interval must be between 60 and 2592000 seconds",
            ));
        }
        let agent = configuration.agent_profile.map(Into::into);
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
            if row.keep_open
                || row.is_chunk_group()
                || row.parent_id != Some(conversation)
                || self.step(|tx| job_tree::has_user_turn(tx, id, false))?
            {
                return self.reject(Violation::InvalidValue(
                    "depends_on",
                    "dependencies must be sibling non-system conversations with keep_open false and no user message",
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
            if self.prerequisite_state(id, condition)? == DependencyState::Impossible {
                return self.reject(Violation::InvalidValue(
                    "depends_on",
                    "a prerequisite already ended unsuccessfully and cannot meet the success condition",
                ));
            }
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
        let mut lines = Vec::new();
        for (index, project) in projects.into_iter().enumerate() {
            let child = self.open_child_conversation(
                conversation,
                approval,
                agent,
                project,
                keep_open || every_seconds.is_some(),
                after.clone(),
            )?;
            let n = u32::try_from(index).unwrap_or(u32::MAX);
            let turn = self.host_turn(
                child,
                job_tree::host_input(objective.clone(), configuration, asked.input.priority),
                job_tree::tool_submission(approval, b"dispatch create", n),
                job_tree::tool_submission(approval, b"dispatch submit", n),
                Actor::Agent {
                    conversation: asked.parent_id,
                },
            )?;
            if let Some(interval_s) = every_seconds {
                let rec = crate::db::entities::Recurrence {
                    phase: crate::db::entities::RecurrencePhase::Active,
                    n: 1,
                    current_turn: turn,
                    previous_turn: None,
                    interval_s,
                };
                self.insert(RECURRENCES, child.get(), rec.encode().as_slice())?;
            }
            lines.push(format!("conversation {child}, turn {turn}"));
        }
        Ok(format!(
            "I selected the target. The host dispatched your objective as: {}. Their results return here when they settle. Please continue.",
            lines.join("; ")
        ))
    }

    fn dispatch_configuration(
        &mut self,
        selection: &DispatchSelection,
    ) -> Result<ExecutionConfiguration, BatchError> {
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
        let agent_id = ConfigId::new(self.job_tool_id(agent)?);
        let config = self.require_config_kind(agent_id, ConfigKind::Agent)?;
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
        let reasoning_effort = if let Some(value) = &selection.reasoning_effort {
            let ConfigBody::Model(body) = &record.body else {
                return self.corrupted("invalid dispatch model".into());
            };
            let Some(effort) = ReasoningEffort::parse(value) else {
                return self.reject(Violation::InvalidValue(
                    "reasoning_effort",
                    "invalid effort",
                ));
            };
            if !body
                .declared_capabilities
                .contains(&ModelCapability::Reasoning)
            {
                return self.reject(Violation::InvalidValue(
                    "reasoning_effort",
                    "the selected model has no reasoning capability",
                ));
            }
            Some(effort)
        } else {
            None
        };
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
        Ok(ExecutionConfiguration {
            reasoning_effort,
            agent_profile: Some(agent_id.into()),
            llm_profile: model.into(),
        })
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
                        ConfigBody::Model(body) => {
                            let provider = self.require_config(body.connection_id)?;
                            json!({
                                "profile": ProfileId::new(model.get()).to_string(),
                                "name": record.name,
                                "model": body.model,
                                "provider": provider.name,
                                "provider_id": provider.id.to_string(),
                                "reasoning_efforts": body.reasoning_efforts,
                                "declared_capabilities": body.declared_capabilities.iter().map(|capability| capability.as_str()).collect::<Vec<_>>(),
                                "enabled": record.model_enabled,
                                "archived": record.archived_at.is_some(),
                            })
                        }
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
                        | ConfigBody::Connection(_) => {
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
            workers.push(json!({
                "conversation": worker.to_string(),
                "agent": row.agent.map(|id| ProfileId::new(id.get()).to_string()),
                "project": row.project.map(|id| id.to_string()),
                "title": self.job_lifecycle(worker)?.title,
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
