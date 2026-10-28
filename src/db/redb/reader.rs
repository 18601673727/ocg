use std::ops::Bound;

use ::redb::ReadableTableMetadata;

use super::*;
use crate::db::derived::ActivityProjection;
use crate::db::entities::{
    ConfigBody, ConfigId, ConfigKind, ConfigRecord, ConnectionConfig, DependencyState,
    ExecutionConfiguration, ExecutionOperation, ExecutionPoolConfig, FrozenPoolBinding,
    JobInputRecord, OutcomeKind, SubmittedConfiguration, ToolCallRef,
};

// ---- typed reads ---------------------------------------------------------------

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

fn read_job_row(rx: &ReadTransaction, id: JobId) -> Result<Option<JobSpec>, StorageError> {
    rx.open_table(JOBS)?
        .get(id.get())?
        .map(|bytes| JobSpec::decode_at(id.get(), bytes.value()))
        .transpose()
}

fn read_job_control(rx: &ReadTransaction, id: JobId) -> Result<JobControl, StorageError> {
    rx.open_table(JOB_CONTROLS)?
        .get(id.get())?
        .map(|bytes| JobControl::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no conversation row")))
}

fn read_state(rx: &ReadTransaction, id: JobId) -> Result<JobState, StorageError> {
    rx.open_table(JOB_STATES)?
        .get(id.get())?
        .map(|bytes| JobState::decode_at(id.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no job_state")))
}

pub(super) fn job_execution(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let JobKind::Execution {
        execution,
        plan_source,
        ..
    } = &row.kind
    else {
        return Err(StorageError::corrupted("expected a turn"));
    };
    let execution = match execution {
        JobExecution::Frozen(execution) => execution.clone(),
        JobExecution::Summarization(work) => work.frozen.clone(),
        JobExecution::Decision(_) => {
            return Err(StorageError::corrupted(
                "decision work cannot use the generative context reader",
            ));
        }
        JobExecution::Ref(owner) => {
            let bytes = jobs
                .get(owner.get())?
                .ok_or_else(|| StorageError::corrupted("missing execution owner"))?;
            let owner = JobSpec::decode_at(owner.get(), bytes.value())?;
            match owner.kind {
                JobKind::Execution {
                    execution: JobExecution::Frozen(execution),
                    ..
                } if owner.parent_id == row.parent_id => execution,
                _ => {
                    return Err(StorageError::corrupted(
                        "execution reference must name a Frozen turn in this conversation",
                    ));
                }
            }
        }
    };
    let (project, agent) = match plan_source {
        PlanSource::Process { project, .. } => {
            (*project, execution.agent_ref.map(|agent| agent.id))
        }
        PlanSource::Stored { .. } => {
            let conversation = row
                .parent_id
                .ok_or_else(|| StorageError::corrupted("stored turn has no conversation"))?;
            let bytes = jobs
                .get(conversation.get())?
                .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
            match JobSpec::decode_at(conversation.get(), bytes.value())?.kind {
                JobKind::Group { project, agent, .. } => (project, agent),
                JobKind::Execution { .. } => {
                    return Err(StorageError::corrupted(
                        "stored turn parent must be a conversation",
                    ));
                }
            }
        }
    };
    Ok((execution, project, agent))
}

/// A Job assembled from its spec, its state and the derived tables.
pub(super) fn job_record(rx: &ReadTransaction, row: JobSpec) -> Result<JobRecord, StorageError> {
    let id = row.id;
    let state = read_state(rx, id)?;
    let at = |kind: OutcomeKind| {
        state
            .outcome
            .as_ref()
            .filter(|outcome| outcome.kind == kind)
            .map(|outcome| outcome.at)
    };
    let subtree_settled = rx.open_table(UNSETTLED)?.get(id.get())?.is_none();
    let execution = turn_config(rx, &row)?;
    let configuration = execution.as_ref().map(|execution| ExecutionConfiguration {
        reasoning_effort: execution
            .llm
            .config
            .parameters
            .reasoning_effort
            .as_deref()
            .and_then(crate::db::entities::ReasoningEffort::parse),
        agent_profile: execution.agent_ref.map(|agent| agent.id.into()),
        llm_profile: execution.llm.model.id.into(),
    });
    let updated_at = [
        Some(row.created_at),
        state.claim.as_ref().map(|claim| claim.started_at),
        state.outcome.as_ref().map(|outcome| outcome.at),
        state.stop_confirmed_at,
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(row.created_at);
    let children_closed_at = if row.kind.is_group() {
        read_job_control(rx, id)?.closing.map(|closing| closing.at)
    } else {
        state.outcome.as_ref().map(|outcome| outcome.at)
    };
    Ok(JobRecord {
        id,
        name: row.kind.name().map(str::to_owned),
        started_at: state.claim.as_ref().map(|claim| claim.started_at),
        completed_at: at(OutcomeKind::Completed),
        failed_at: at(OutcomeKind::Failed),
        cancelled_at: at(OutcomeKind::Cancelled),
        unknown_at: at(OutcomeKind::Unknown),
        stop_confirmed_at: state.stop_confirmed_at,
        stop_confirmed_by: state.stop_confirmed_by,
        created_at: row.created_at,
        updated_at,
        input: JobInputId::new(id.get()),
        parent: row.parent_id.unwrap_or(JobId::new(0)),
        parent_id: row.parent_id,
        group: row.kind.is_persistent().then_some(row.parent_id).flatten(),
        objective: row.kind.objective().map(str::to_owned),
        configuration,
        assigned_executor: state.claim.as_ref().map(|claim| claim.pool_id.into()),
        previous: row.kind.previous(),
        claim: state.claim,
        outcome: state.outcome,
        children_closed_at,
        kind: row.kind,
        subtree_settled,
    })
}

pub(crate) fn read_job(database: &Database, id: JobId) -> Result<Option<JobRecord>, StorageError> {
    let rx = read(database)?;
    read_job_row(&rx, id)?
        .map(|row| job_record(&rx, row))
        .transpose()
}

#[derive(Clone)]
pub(crate) struct JobScope {
    pub owner: JobId,
    pub synthesis: bool,
    pub results: Vec<JobId>,
    pub incomplete: bool,
}

struct JobPlanSnapshot {
    steps: Vec<JobAssignedStep>,
    continuation: JobId,
}

struct JobAssignedStep {
    group: JobId,
    job: JobId,
}

fn job_plan_snapshot(
    rx: &ReadTransaction,
    row: &JobSpec,
) -> Result<Option<(JobId, JobPlanSnapshot)>, StorageError> {
    let mut source = row.clone();
    if source.kind.is_group()
        && let Some(tip) = rx.open_table(GROUP_HEADS)?.get(source.id.get())?
    {
        source = read_job_row(rx, JobId::new(tip.value()))?
            .ok_or_else(|| StorageError::corrupted("missing group head"))?;
    }
    let owner = if let Some(allocation) = &source.allocation {
        allocation.owner
    } else if let Some(parent) = source.parent_id {
        match read_job_row(rx, parent)?.map(|row| row.kind) {
            Some(JobKind::Group {
                origin: Origin::Agent { call },
                ..
            }) => call.job,
            Some(
                JobKind::Group {
                    origin: Origin::User | Origin::System,
                    ..
                }
                | JobKind::Execution { .. },
            )
            | None => source.id,
        }
    } else {
        source.id
    };
    let mut steps = Vec::new();
    let mut continuation = None;
    for entry in rx
        .open_table(JOB_ALLOCATIONS)?
        .range((owner.get(), 0)..=(owner.get(), u128::MAX))?
    {
        let (key, _) = entry?;
        let job = JobId::new(key.value().1);
        let row = read_job_row(rx, job)?
            .ok_or_else(|| StorageError::corrupted("allocated job missing"))?;
        let allocation = row
            .allocation
            .ok_or_else(|| StorageError::corrupted("job allocation index has no source"))?;
        if allocation.owner != owner {
            return Err(StorageError::corrupted("job allocation owner mismatch"));
        }
        if allocation.synthesis {
            if continuation.replace(job).is_some() {
                return Err(StorageError::corrupted("multiple job plan continuations"));
            }
        } else {
            let group = row
                .parent_id
                .ok_or_else(|| StorageError::corrupted("allocated job has no group"))?;
            steps.push(JobAssignedStep { group, job });
        }
        if steps.len() > 6 {
            return Err(StorageError::corrupted("job plan exceeds step limit"));
        }
    }
    if steps.is_empty() && continuation.is_none() {
        return Ok(None);
    }
    let continuation =
        continuation.ok_or_else(|| StorageError::corrupted("job plan continuation missing"))?;
    Ok(Some((
        owner,
        JobPlanSnapshot {
            steps,
            continuation,
        },
    )))
}

pub(crate) fn read_job_scope(
    database: &Database,
    job: JobId,
) -> Result<Option<JobScope>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    let Some((owner, receipt)) = job_plan_snapshot(&rx, &row)? else {
        return Ok(None);
    };
    let synthesis = receipt.continuation == job;
    let selected = receipt
        .steps
        .iter()
        .position(|step| Some(step.group) == row.parent_id);
    if !synthesis && selected.is_none() {
        return Ok(None);
    }
    let mut results = Vec::new();
    let mut incomplete = false;
    let mut prerequisites = BTreeSet::new();
    if let Some(selected) = selected {
        for (index, step) in receipt.steps.iter().enumerate().take(selected + 1).rev() {
            let conversation = step.group;
            if index == selected || prerequisites.contains(&conversation) {
                let turn = step.job;
                let original = read_job_row(&rx, turn)?
                    .ok_or_else(|| StorageError::corrupted("missing task turn"))?;
                prerequisites.extend(original.kind.dependencies().iter().map(|(id, _)| *id));
            }
        }
    }
    for step in &receipt.steps {
        let conversation = step.group;
        let tip = rx
            .open_table(GROUP_HEADS)?
            .get(conversation.get())?
            .map(|id| JobId::new(id.value()));
        if let Some(tip) = tip
            && (synthesis || prerequisites.contains(&conversation))
        {
            results.push(tip);
            incomplete |= read_state(&rx, tip)?
                .outcome
                .is_none_or(|outcome| outcome.kind != OutcomeKind::Completed);
        }
    }
    Ok(Some(JobScope {
        owner,
        synthesis,
        results,
        incomplete,
    }))
}

pub(crate) struct JobDetails {
    pub job: JobRecord,
    pub result: Option<JobOutputRecord>,
    pub usage: Option<JobUsage>,
    pub unknown_reason: Option<String>,
    pub activity: JobActivity,
    pub task_plan: Option<JobPlanView>,
}

fn job_plan_view(rx: &ReadTransaction, row: &JobSpec) -> Result<Option<JobPlanView>, StorageError> {
    let Some((owner, receipt)) = job_plan_snapshot(rx, row)? else {
        return Ok(None);
    };
    let continuation = receipt.continuation;
    let Some(synthesis) = read_job_row(rx, continuation)? else {
        return Err(StorageError::corrupted("missing task continuation"));
    };
    let synthesis = job_record(rx, synthesis)?;
    let state = if synthesis.outcome.is_some() {
        JobActivityPhase::for_plan(&synthesis)
    } else if synthesis.started_at.is_some() {
        JobActivityPhase::Synthesizing
    } else {
        JobActivityPhase::RunningTasks
    };
    let mut steps = Vec::new();
    for step in receipt.steps.iter().take(6) {
        let conversation = step.group;
        let tip = rx
            .open_table(GROUP_HEADS)?
            .get(conversation.get())?
            .map(|id| JobId::new(id.value()))
            .ok_or_else(|| StorageError::corrupted("missing task tip"))?;
        let row =
            read_job_row(rx, tip)?.ok_or_else(|| StorageError::corrupted("missing task turn"))?;
        let (configuration, _, _) = job_execution(&rx.open_table(JOBS)?, &row)?;
        let job = job_record(rx, row)?;
        let agent = configuration
            .agent_ref
            .map(|reference| reference.id.to_string());
        let model = Some(configuration.llm.model.id.to_string());
        let original = read_job_row(rx, step.job)?
            .ok_or_else(|| StorageError::corrupted("missing task turn"))?;
        steps.push(JobPlanStepView {
            conversation: step.group.to_string(),
            turn: tip.to_string(),
            title: original
                .kind
                .name()
                .map(str::to_owned)
                .unwrap_or_else(|| "Task phase".into()),
            agent,
            model,
            state: JobActivityPhase::for_plan(&job),
            depends_on: original
                .kind
                .dependencies()
                .iter()
                .map(|(id, _)| id.to_string())
                .collect(),
        });
    }
    Ok(Some(JobPlanView {
        owner: owner.to_string(),
        continuation: receipt.continuation.to_string(),
        state,
        steps,
    }))
}

/// Cuts `message` to its first 2048 bytes at a character boundary, ending
/// with "…" when cut, so the Job route stays bounded.
pub(crate) fn read_job_details(
    database: &Database,
    id: JobId,
) -> Result<Option<JobDetails>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, id)? else {
        return Ok(None);
    };
    let conversation = row.kind.is_group();
    let task_plan = job_plan_view(&rx, &row)?;
    let job = job_record(&rx, row)?;
    let latest_output = if conversation {
        conversation_result(&rx, id)?
    } else {
        rx.open_table(JOB_OUTPUTS)?
            .range((id.get(), 0)..=(id.get(), u32::MAX))?
            .next_back()
            .transpose()?
            .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
            .transpose()?
    };
    let result = latest_output
        .clone()
        .filter(|output| output.kind == OutputKind::Result);
    let mut activity = ActivityProjection::new(&job, latest_output.as_ref());
    let usage = if conversation {
        None
    } else {
        let target = observations::frozen_llm_target(&rx, id)?;
        let mut usage = JobUsage::default();
        let mut events = Vec::new();
        let mut scanned = 0;
        for entry in rx
            .open_table(REQUEST_EVENTS)?
            .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
            .rev()
            .take(MAX_OBSERVATION_SCAN + 1)
        {
            if scanned == MAX_OBSERVATION_SCAN {
                usage.truncated = true;
                break;
            }
            let (key, bytes) = entry?;
            let (_, request, seq) = key.value();
            let request = RequestNumber::new(request)
                .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?;
            scanned += 1;
            let event = super::rows::decode_request_event(id, request, seq, bytes.value())?;
            activity.request_event(&event);
            let seq = event.seq;
            events.push(event);
            if seq == 1 {
                events.reverse();
                if let Some(sample) = observations::usage_sample_of(id, request, &target, &events) {
                    usage.add_sample(&sample);
                }
                events.clear();
            }
        }
        (scanned != 0).then_some(usage)
    };
    for entry in rx
        .open_table(TOOL_CALLS)?
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .rev()
        .take(256)
    {
        let (key, bytes) = entry?;
        let call = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
        let awaiting_user = match &call.decision.fact {
            crate::db::entities::ToolDecision::AwaitingAnswer(_)
            | crate::db::entities::ToolDecision::AwaitingApproval(_) => {
                if let Some(conversation) = job.group {
                    rx.open_table(OPEN_APPROVALS)?
                        .get((
                            conversation.get(),
                            call.call.job.get(),
                            call.call.request.get(),
                            call.call.index,
                        ))?
                        .is_some()
                } else {
                    false
                }
            }
            crate::db::entities::ToolDecision::Admitted(_)
            | crate::db::entities::ToolDecision::Refused(_) => false,
        };
        activity.tool_call(call, awaiting_user);
    }
    let (activity, unknown_reason) = activity.finish(&job);

    Ok(Some(JobDetails {
        job,
        result,
        usage,
        unknown_reason,
        activity,
        task_plan,
    }))
}

pub(crate) fn read_jobs(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobRecord>, StorageError> {
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u128::MAX, JobId::get);
    let mut records = Vec::new();
    for entry in jobs.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(job_record(
            &rx,
            JobSpec::decode_at(key.value(), bytes.value())?,
        )?);
    }
    Ok(records)
}

pub(crate) struct JobSearchPage {
    pub records: Vec<JobRecord>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

pub(crate) fn read_job_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<JobSearchPage, StorageError> {
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u128::MAX, JobId::get);
    let limit = limit.clamp(1, 100);
    let mut records = Vec::new();
    let mut last = None;
    let mut has_more = false;
    for entry in jobs.range(..end)?.rev() {
        if records.len() == limit {
            has_more = true;
            break;
        }
        let (key, bytes) = entry?;
        let id = JobId::new(key.value());
        last = Some(id);
        let record = job_record(&rx, JobSpec::decode_at(key.value(), bytes.value())?)?;
        let identity = record.id.to_string().to_lowercase();
        let name = record.name.as_deref().unwrap_or("").to_lowercase();
        let objective = record.objective.as_deref().unwrap_or("").to_lowercase();
        if terms
            .iter()
            .all(|term| identity.contains(term) || name.contains(term) || objective.contains(term))
        {
            records.push(record);
        }
    }
    Ok(JobSearchPage {
        records,
        next_before: last.filter(|_| has_more),
        has_more,
    })
}

pub(crate) fn read_children(
    database: &Database,
    parent: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CHILDREN)?;
    let start = after.map_or(0, |after| after.get().saturating_add(1));
    let mut children = Vec::new();
    for entry in table
        .range((parent.get(), start)..=(parent.get(), u128::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        children.push(JobId::new(entry?.0.value().1));
    }
    Ok(children)
}

// ---- the tree of the work ---------------------------------------------------------

/// The most children the root's own page lists.
pub(crate) const TREE_PAGE_MAX: usize = 100;
/// The most children a node deeper than the root lists.
pub(crate) const TREE_NESTED: usize = 20;
/// The most nodes one answer holds, the root included.
pub(crate) const TREE_NODES_MAX: usize = 300;
/// The deepest level the tree expands.
pub(crate) const TREE_DEPTH_MAX: u8 = 4;
/// The most ancestors a root names.
const TREE_ANCESTORS_MAX: usize = 8;
/// The most dependencies one turn waits on.
const TREE_DEPENDENCIES_MAX: usize = 16;
/// The most characters of a title.
const TITLE_CHARS: usize = 80;

/// What one node of the tree is. Every fact is read from the row that owns
/// it: nothing here is a copy of another row.
#[derive(Clone, Debug, PartialEq)]
pub enum TreeFacts {
    Conversation {
        title: String,
        /// `open`, `closing` or `settled`, as the conversation itself computes.
        state: &'static str,
        on_hold: Option<Actor>,
        /// `user`, `agent` or `system`.
        origin: &'static str,
        keep_open: bool,
        /// The turn the conversation is running, with the model it runs with.
        active_turn: Option<(JobId, ConfigId, String)>,
        recurrence: Option<Recurrence>,
    },
    Turn {
        title: String,
        /// `completed`, `failed`, `cancelled`, `unknown`, `running`,
        /// `waiting` or `queued`.
        state: &'static str,
        /// The conversation the turn belongs to.
        conversation: Option<JobId>,
        previous: Option<JobId>,
        /// The LLM Profile the turn runs with, and the model it names.
        model: (ConfigId, String),
        depends_on: Vec<(JobId, DependencyCondition, DependencyState)>,
    },
}

/// One node of the tree: the Job, what it is, and the children it lists.
#[derive(Clone, Debug, PartialEq)]
pub struct TreeNode {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub created_at: Timestamp,
    pub facts: TreeFacts,
    /// The Agent of the node, with its name when its config row is there.
    pub agent: Option<(ConfigId, Option<String>)>,
    /// The children this node lists, as indexes into the tree's nodes.
    pub children: Option<Vec<usize>>,
    /// Whether the node has children this answer does not list.
    pub has_more_children: bool,
    /// The cursor of the node's next page of children.
    pub next_after: Option<JobId>,
}

/// The tree of the work below one Job: its ancestors, and the nodes of the
/// tree itself, the root first.
#[derive(Clone, Debug, PartialEq)]
pub struct JobTree {
    pub ancestors: Vec<JobId>,
    pub nodes: Vec<TreeNode>,
}

/// One page of a node's children.
struct ChildPage {
    listed: Vec<JobId>,
    has_more: bool,
    next_after: Option<JobId>,
}

/// The children of `parent` in creation order, strictly after `after` when
/// given. The parent is never its own child.
fn child_page(
    rx: &ReadTransaction,
    parent: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<ChildPage, StorageError> {
    let start = after.map_or(0, |after| after.get().saturating_add(1));
    let mut listed = Vec::new();
    let mut has_more = false;
    for entry in rx
        .open_table(CHILDREN)?
        .range((parent.get(), start)..=(parent.get(), u128::MAX))?
    {
        let child = JobId::new(entry?.0.value().1);
        if child == parent {
            continue;
        }
        if listed.len() == limit {
            has_more = true;
            break;
        }
        listed.push(child);
    }
    let next_after = has_more.then(|| listed.last().copied()).flatten();
    Ok(ChildPage {
        listed,
        has_more,
        next_after,
    })
}

/// Whether a node has any child at all.
fn has_children(rx: &ReadTransaction, parent: JobId) -> Result<bool, StorageError> {
    Ok(rx
        .open_table(CHILDREN)?
        .range((parent.get(), 0)..=(parent.get(), u128::MAX))?
        .next()
        .transpose()?
        .is_some())
}

/// The ancestors of `root`, the topmost first. The Genesis Job is the root of
/// every tree and is never named.
fn read_ancestors(rx: &ReadTransaction, root: JobId) -> Result<Vec<JobId>, StorageError> {
    let mut ancestors = Vec::new();
    let mut current = read_job_row(rx, root)?.and_then(|row| row.parent_id);
    while let Some(id) = current {
        if id.get() == 0 {
            break;
        }
        ancestors.push(id);
        if ancestors.len() > TREE_ANCESTORS_MAX {
            return Err(StorageError::corrupted("too many ancestors"));
        }
        current = match read_job_row(rx, id)? {
            Some(row) => row.parent_id,
            None => return Err(StorageError::corrupted(format!("{id} has no job row"))),
        };
    }
    ancestors.reverse();
    Ok(ancestors)
}

/// The name of an Agent, read once per request.
fn agent_name(
    rx: &ReadTransaction,
    agent: ConfigId,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<Option<String>, StorageError> {
    if let Some(name) = names.get(&agent) {
        return Ok(name.clone());
    }
    let name = rx
        .open_table(CONFIGS)?
        .get(agent.get())?
        .map(|bytes| ConfigRow::decode_at(agent.get(), bytes.value()))
        .transpose()?
        .map(|row| row.name);
    names.insert(agent, name.clone());
    Ok(name)
}

/// The turn a conversation is running, with the model it runs with. It is the
/// active turn only while that turn is claimed and has no outcome.
fn active_job(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<(JobId, ConfigId, String)>, StorageError> {
    let Some(turn) = rx
        .open_table(ACTIVE_JOBS)?
        .get(conversation.get())?
        .map(|id| JobId::new(id.value()))
    else {
        return Ok(None);
    };
    let state = read_state(rx, turn)?;
    if state.claim.is_none() || state.outcome.is_some() {
        return Ok(None);
    }
    let row = read_job_row(rx, turn)?
        .ok_or_else(|| StorageError::corrupted(format!("{turn} has no job row")))?;
    let (model, name) = turn_identity(rx, &row)?;
    Ok(Some((turn, model, name)))
}

/// How a turn stands: what its outcome says, else whether it runs, else
/// whether it waits on a dependency, else that it is queued.
fn turn_state(rx: &ReadTransaction, id: JobId) -> Result<&'static str, StorageError> {
    let state = read_state(rx, id)?;
    if let Some(outcome) = &state.outcome {
        return Ok(match outcome.kind {
            OutcomeKind::Completed => "completed",
            OutcomeKind::Failed => "failed",
            OutcomeKind::Cancelled => "cancelled",
            OutcomeKind::Unknown => "unknown",
        });
    }
    if state.claim.is_some() {
        if let Some(row) = read_job_row(rx, id)?
            && row.kind.is_persistent()
            && let Some(group) = row.parent_id
        {
            for approval in rx.open_table(OPEN_APPROVALS)?.range(
                (group.get(), id.get(), 0, 0)..=(group.get(), id.get(), u32::MAX, u32::MAX),
            )? {
                if approval?.1.value() == 0 {
                    return Ok("waiting_for_user");
                }
            }
        }
        return Ok("running");
    }
    let open = rx
        .open_table(UNSETTLED)?
        .get(id.get())?
        .map(|counts| counts.value().0)
        .unwrap_or(0);
    Ok(if open > 0 { "waiting" } else { "queued" })
}

/// How a dependency stands: it waits while the prerequisite has not settled,
/// and it can never be met once the prerequisite has settled without the
/// outcome the condition asks for.
fn dependency_state(
    rx: &ReadTransaction,
    dependency: JobId,
    condition: DependencyCondition,
) -> Result<DependencyState, StorageError> {
    if rx.open_table(UNSETTLED)?.get(dependency.get())?.is_some() {
        return Ok(DependencyState::Waiting);
    }
    let state = rx
        .open_table(JOB_STATES)?
        .get(dependency.get())?
        .map(|bytes| JobState::decode_at(dependency.get(), bytes.value()))
        .transpose()?;
    let completed = state
        .as_ref()
        .and_then(|state| state.outcome.as_ref())
        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
    Ok(if completed || condition == DependencyCondition::Settled {
        DependencyState::Satisfied
    } else {
        DependencyState::Impossible
    })
}

/// A title, as long as the tree shows it.
fn cap_title(title: &str) -> String {
    title.chars().take(TITLE_CHARS).collect()
}

/// The text of a turn's first entry: what it was asked, or nothing when its
/// first entry is not a message.
fn first_entry_text(rx: &ReadTransaction, turn: JobId) -> Result<String, StorageError> {
    Ok(turn_entries(rx, turn)?
        .into_iter()
        .next()
        .map(|(_, entry)| match entry.entry {
            Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } => {
                text.chars().take(TITLE_CHARS).collect()
            }
            Entry::UserMessageEdit { .. }
            | Entry::UserMessageWithdraw { .. }
            | Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::RetryOf { .. }
            | Entry::Iteration { .. }
            | Entry::ResponseFormat { .. }
            | Entry::ContextPreparation { .. }
            | Entry::ContextUse { .. } => String::new(),
        })
        .unwrap_or_default())
}

/// One node of the tree: the Job's own row, its state, and what it owns.
fn tree_node(
    rx: &ReadTransaction,
    row: &JobSpec,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<TreeNode, StorageError> {
    let (facts, agent) = match &row.kind {
        JobKind::Group {
            origin, keep_open, ..
        } => {
            let tip = rx
                .open_table(GROUP_HEADS)?
                .get(row.id.get())?
                .map(|id| JobId::new(id.value()))
                .ok_or_else(|| StorageError::corrupted(format!("{} has no tip", row.id)))?;
            let summary = conversation_summary(rx, row.id, tip)?;
            let state = match summary.state.as_str() {
                "open" => "open",
                "closing" => "closing",
                "settled" => "settled",
                _ => return Err(StorageError::corrupted("unknown conversation state")),
            };
            let origin = match origin {
                Origin::User => "user",
                Origin::Agent { .. } => "agent",
                Origin::System => "system",
            };
            let recurrence = rx
                .open_table(RECURRENCES)?
                .get(row.id.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?;
            (
                TreeFacts::Conversation {
                    title: cap_title(&summary.title),
                    state,
                    on_hold: summary.on_hold,
                    origin,
                    keep_open: *keep_open,
                    active_turn: active_job(rx, row.id)?,
                    recurrence,
                },
                summary.agent,
            )
        }
        JobKind::Execution { name, .. } => {
            let (model_id, model_name) = turn_identity(rx, row)?;
            let agent = match &row.kind {
                JobKind::Execution {
                    execution: JobExecution::Decision(work),
                    ..
                } => work.frozen.agent_ref().map(|reference| reference.id),
                JobKind::Execution { .. } => job_execution(&rx.open_table(JOBS)?, row)?.2,
                _ => None,
            };
            let title = match name {
                Some(name) => name.clone(),
                None => match row.kind.objective() {
                    Some(objective) => objective.to_owned(),
                    None => first_entry_text(rx, row.id)?,
                },
            };
            let mut depends_on = Vec::new();
            for (dependency, condition) in
                row.kind.dependencies().iter().take(TREE_DEPENDENCIES_MAX)
            {
                depends_on.push((
                    *dependency,
                    *condition,
                    dependency_state(rx, *dependency, *condition)?,
                ));
            }
            (
                TreeFacts::Turn {
                    title: cap_title(&title),
                    state: turn_state(rx, row.id)?,
                    conversation: row.kind.is_persistent().then_some(row.parent_id).flatten(),
                    previous: row.kind.previous(),
                    model: (model_id, model_name),
                    depends_on,
                },
                agent,
            )
        }
    };
    let agent = match agent {
        Some(agent) => Some((agent, agent_name(rx, agent, names)?)),
        None => None,
    };
    Ok(TreeNode {
        id: row.id,
        parent_id: row.parent_id,
        created_at: row.created_at,
        facts,
        agent,
        children: None,
        has_more_children: false,
        next_after: None,
    })
}

/// The tree of the work below `root`: the ancestors above it, and the nodes
/// below it, breadth first and in creation order. The root's own children
/// are one page; every node deeper than that lists only as many children as
/// the bounds allow.
pub(crate) fn read_job_tree(
    database: &Database,
    root: JobId,
    after: Option<JobId>,
    limit: usize,
    depth: u8,
) -> Result<Option<JobTree>, StorageError> {
    let rx = read(database)?;
    let Some(root_row) = read_job_row(&rx, root)? else {
        return Ok(None);
    };
    let ancestors = read_ancestors(&rx, root)?;
    let mut names: BTreeMap<ConfigId, Option<String>> = BTreeMap::new();
    let mut nodes = vec![tree_node(&rx, &root_row, &mut names)?];
    let page = child_page(&rx, root, after, limit.min(TREE_PAGE_MAX))?;
    let mut level = Vec::new();
    for child in page.listed {
        let row = read_job_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted(format!("{child} has no job row")))?;
        nodes.push(tree_node(&rx, &row, &mut names)?);
        level.push(nodes.len() - 1);
    }
    nodes[0].children = Some(level.clone());
    nodes[0].has_more_children = page.has_more;
    nodes[0].next_after = page.next_after;
    for _ in 2..=depth.min(TREE_DEPTH_MAX) {
        let mut next = Vec::new();
        for index in level {
            if nodes.len() + TREE_NESTED > TREE_NODES_MAX {
                nodes[index].has_more_children = has_children(&rx, nodes[index].id)?;
                continue;
            }
            let page = child_page(&rx, nodes[index].id, None, TREE_NESTED)?;
            let mut children = Vec::new();
            for child in page.listed {
                let row = read_job_row(&rx, child)?
                    .ok_or_else(|| StorageError::corrupted(format!("{child} has no job row")))?;
                nodes.push(tree_node(&rx, &row, &mut names)?);
                children.push(nodes.len() - 1);
            }
            nodes[index].children = Some(children.clone());
            nodes[index].has_more_children = page.has_more;
            nodes[index].next_after = page.next_after;
            next.extend(children);
        }
        level = next;
    }
    for index in level {
        nodes[index].has_more_children = has_children(&rx, nodes[index].id)?;
    }
    Ok(Some(JobTree { ancestors, nodes }))
}

#[derive(Clone, Debug, PartialEq)]
pub struct CursorPage<T, C> {
    pub records: Vec<T>,
    pub next: Option<C>,
}

pub(crate) fn read_retries(
    database: &Database,
    original: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<CursorPage<JobId, JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(RETRIES)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut more = false;
    for entry in table.range((
        std::ops::Bound::Excluded((original.get(), after.map_or(0, JobId::get))),
        std::ops::Bound::Included((original.get(), u128::MAX)),
    ))? {
        if records.len() == limit {
            more = true;
            break;
        }
        records.push(JobId::new(entry?.0.value().1));
    }
    let next = records.last().copied().filter(|_| more);
    Ok(CursorPage { records, next })
}

pub(crate) fn read_outputs(
    database: &Database,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    if read_job_row(&rx, job)?.is_some_and(|row| row.kind.is_group()) {
        if after_seq.is_some_and(|seq| seq >= 1) || limit == 0 {
            return Ok(Vec::new());
        }
        return Ok(conversation_result(&rx, job)?.into_iter().collect());
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = match after_seq.map(u32::try_from) {
        None => 0,
        // A sequence at the end of the domain has nothing after it. A
        // negative cursor is not a sequence; neither replays the page from
        // the beginning.
        Some(Err(_)) => return Ok(Vec::new()),
        Some(Ok(u32::MAX)) => return Ok(Vec::new()),
        Some(Ok(seq)) => seq + 1,
    };
    let mut records = Vec::new();
    for entry in outputs
        .range((job.get(), start)..=(job.get(), u32::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        let (key, bytes) = entry?;
        records.push(decode_output(key.value(), bytes.value())?);
    }
    Ok(records)
}

/// The Job's newest output: a backward seek on `job_output`.
pub(crate) fn read_latest_output(
    database: &Database,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    if read_job_row(&rx, job)?.is_some_and(|row| row.kind.is_group()) {
        return conversation_result(&rx, job);
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    outputs
        .range((job.get(), 0)..=(job.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()
}

fn conversation_result(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    let state = read_state(rx, conversation)?;
    if state.outcome.is_none() {
        return Ok(None);
    }
    let mut summary = String::new();
    let mut value = None;
    let answered = rx
        .open_table(GROUP_RESULTS)?
        .get(conversation.get())?
        .map(|tips| tips.value().1)
        .filter(|turn| *turn != 0);
    if let Some(turn) = answered
        && let Some((key, bytes)) = rx
            .open_table(JOB_OUTPUTS)?
            .range((turn, 0)..=(turn, u32::MAX))?
            .next_back()
            .transpose()?
    {
        let output = decode_output(key.value(), bytes.value())?;
        summary = output.summary.unwrap_or_default();
        value = output.value;
    }
    let after = rx
        .open_table(NOTICE_READ)?
        .get(conversation.get())?
        .map_or(0, |seq| seq.value());
    if after < u32::MAX {
        for entry in rx
            .open_table(NOTICES)?
            .range((conversation.get(), after + 1)..=(conversation.get(), u32::MAX))?
        {
            let notice = Notice::decode(entry?.1.value())?;
            if matches!(notice.class, NoticeClass::Attention | NoticeClass::Progress) {
                summary.push_str(&format!("\nUnread notice: {:?}", notice.body));
            }
        }
    }
    for call in approved_calls(rx, conversation)? {
        summary.push_str(&format!(
            "\nApproved, never ran: {} {}",
            call.name,
            call.decision
                .fact
                .stored_admission()
                .map_or("{}", |a| a.arguments.as_str())
        ));
    }
    Ok(Some(JobOutputRecord {
        id: JobOutputId::new(1),
        job: conversation,
        seq: 1,
        kind: OutputKind::Result,
        summary: Some(summary),
        code: None,
        goal_reached: None,
        value,
        created_at: state
            .outcome
            .map_or(Timestamp::from_unix_micros(0), |outcome| outcome.at),
    }))
}

pub(crate) fn read_tool_evidence(
    database: &Database,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_CALLS)?;
    let start = match after {
        None => (job.get(), 0, 0),
        Some((request, u32::MAX)) => match request.get().checked_add(1) {
            Some(next) => (job.get(), next, 0),
            None => return Ok(Vec::new()),
        },
        Some((request, index)) => (job.get(), request.get(), index + 1),
    };
    let mut records = Vec::new();
    for entry in table
        .range(start..=(job.get(), u32::MAX, u32::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        let (key, bytes) = entry?;
        records.push(decode_tool_evidence(
            tool_call_ref(key.value())?,
            bytes.value(),
        )?);
    }
    Ok(records)
}

// ---- a Job's input ---------------------------------------------------------------

/// The input of a Job is part of its spec, so it is read from the Job. The
/// identity of the input is the Job's.
fn input_of(rx: &ReadTransaction, row: JobSpec) -> Result<JobInputRecord, StorageError> {
    let execution = turn_config(rx, &row)?;
    let mut objective = row.kind.objective().map(str::to_owned);
    let mut attachments = row.kind.attachments().to_vec();
    if row.kind.is_persistent() {
        let mut messages = Vec::new();
        for (_, entry) in turn_entries(rx, row.id)? {
            match entry.entry {
                Entry::UserMessage {
                    text,
                    attachments: images,
                } => {
                    messages.push(text);
                    attachments.extend(images);
                }
                Entry::AgentMessage { text, .. } => messages.push(text),
                Entry::UserMessageEdit { .. }
                | Entry::UserMessageWithdraw { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::RetryOf { .. }
                | Entry::Iteration { .. }
                | Entry::ResponseFormat { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. } => {}
            }
        }
        if !messages.is_empty() {
            objective = Some(messages.join("\n"));
        }
    }
    Ok(JobInputRecord {
        id: JobInputId::new(row.id.get()),
        objective,
        submitted_at: Some(row.created_at),
        created_at: row.created_at,
        updated_at: row.created_at,
        configuration: execution,
        attachments,
    })
}

pub(crate) fn read_job_input(
    database: &Database,
    id: JobInputId,
) -> Result<Option<JobInputRecord>, StorageError> {
    read_job_input_for_job(database, JobId::new(id.get()))
}

pub(crate) fn read_job_input_for_job(
    database: &Database,
    job: JobId,
) -> Result<Option<JobInputRecord>, StorageError> {
    let rx = read(database)?;
    read_job_row(&rx, job)?
        .map(|row| input_of(&rx, row))
        .transpose()
}

/// The Profile and model identity a turn runs with, for projections that do
/// not need its whole frozen configuration. A decision turn names its
/// Decision Model; a generative turn its Model.
fn turn_identity(rx: &ReadTransaction, row: &JobSpec) -> Result<(ConfigId, String), StorageError> {
    if let JobKind::Execution {
        execution: JobExecution::Decision(work),
        ..
    } = &row.kind
    {
        return Ok((
            work.frozen.model_ref().id,
            work.frozen.model_name().to_owned(),
        ));
    }
    let (execution, _, _) = job_execution(&rx.open_table(JOBS)?, row)?;
    Ok((execution.llm.model.id, execution.llm.config.model.clone()))
}

/// The frozen generative configuration of a turn. A decision turn has none:
/// its configuration is typed decision work, not an LLM Profile.
fn turn_config(
    rx: &ReadTransaction,
    row: &JobSpec,
) -> Result<Option<FrozenExecutionConfig>, StorageError> {
    let jobs = rx.open_table(JOBS)?;
    turn_config_of(&jobs, row)
}

/// See [`turn_config`]; it takes the table so a write batch can call it.
pub(super) fn turn_config_of(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<Option<FrozenExecutionConfig>, StorageError> {
    if matches!(
        &row.kind,
        JobKind::Execution {
            execution: JobExecution::Decision(_),
            ..
        }
    ) || row.kind.is_group()
    {
        return Ok(None);
    }
    Ok(Some(job_execution(jobs, row)?.0))
}

#[derive(Clone)]
pub(super) struct UsageIdentity {
    pub(super) provider: u128,
    pub(super) model: String,
}

/// The connection and model identity used to group token usage. Decision
/// turns use their frozen decision connection and model; generative turns use
/// their frozen provider and model.
pub(super) fn turn_usage_identity_of(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<Option<UsageIdentity>, StorageError> {
    match &row.kind {
        JobKind::Group { .. } => Ok(None),
        JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } => Ok(Some(UsageIdentity {
            provider: work.frozen.connection_ref().id.get(),
            model: work.frozen.model_name().to_owned(),
        })),
        JobKind::Execution { .. } => {
            let (execution, _, _) = job_execution(jobs, row)?;
            Ok(Some(UsageIdentity {
                provider: execution.llm.provider.id.get(),
                model: execution.llm.config.model,
            }))
        }
    }
}

/// The generative accounting target of a turn. A decision turn has no LLM
/// Profile or generative Provider, so it has none.
pub(super) fn turn_target(
    rx: &ReadTransaction,
    row: &JobSpec,
) -> Result<Option<crate::db::entities::LlmTarget>, StorageError> {
    let jobs = rx.open_table(JOBS)?;
    turn_target_of(&jobs, row)
}

/// See [`turn_target`]; it takes the table so a write batch can call it.
pub(super) fn turn_target_of(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<Option<crate::db::entities::LlmTarget>, StorageError> {
    if matches!(
        &row.kind,
        JobKind::Execution {
            execution: JobExecution::Decision(_),
            ..
        }
    ) || row.kind.is_group()
    {
        return Ok(None);
    }
    let (execution, _, _) = job_execution(jobs, row)?;
    Ok(Some(crate::db::entities::LlmTarget {
        llm_profile: execution.llm.model.id.into(),
        frozen: execution.llm,
    }))
}

pub(crate) fn read_job_configuration(
    database: &Database,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    if row.kind.is_group() {
        return Ok(None);
    }
    let Some(execution) = turn_config(&rx, &row)? else {
        return Ok(None);
    };
    Ok(Some(SubmittedConfiguration {
        agent_profile: execution.agent_ref.map(|agent| agent.id.into()),
        llm_profile: execution.llm.model.id.into(),
        frozen: execution,
    }))
}

/// The typed work of a decision turn, read from its immutable spec. It is
/// `None` for every other Job, so a caller must branch on execution class
/// rather than pass a generative snapshot to the decision runtime.
pub(crate) fn read_decision_work(
    database: &Database,
    job: JobId,
) -> Result<Option<crate::db::entities::DecisionWork>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    match row.kind {
        JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } => Ok(Some(work)),
        _ => Ok(None),
    }
}

// ---- submissions -------------------------------------------------------------------

pub(crate) fn read_submission(
    database: &Database,
    key: SubmissionKey,
) -> Result<Option<SubmissionRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(SUBMISSIONS)?
        .get(key.as_bytes().as_slice())?
        .map(|bytes| decode_submission(key.as_bytes(), bytes.value()))
        .transpose()
}

// ---- attachments -------------------------------------------------------------------

pub(crate) fn read_attachment(
    database: &Database,
    id: AttachmentId,
) -> Result<Option<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(ATTACHMENTS)?
        .get(id.get())?
        .map(|bytes| decode_attachment(id.get(), bytes.value()))
        .transpose()
}

pub(crate) fn read_attachments(
    database: &Database,
    before: Option<AttachmentId>,
    limit: usize,
) -> Result<Vec<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(ATTACHMENTS)?;
    let end = before.map_or(u128::MAX, AttachmentId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(decode_attachment(key.value(), bytes.value())?);
    }
    Ok(records)
}

pub(crate) fn content_available(
    database: &Database,
    digest: ContentDigest,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    Ok(rx
        .open_table(CONTENT_REFERENCES)?
        .get(digest.as_bytes().as_slice())?
        .is_some_and(|value| value.value().1 > 0))
}

// ---- projects ----------------------------------------------------------------------

pub(crate) fn read_project(
    database: &Database,
    id: ProjectId,
) -> Result<Option<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(PROJECTS)?
        .get(id.get())?
        .map(|bytes| decode_project(id.get(), bytes.value()))
        .transpose()
}

pub(crate) fn read_projects(
    database: &Database,
    before: Option<ProjectId>,
    limit: usize,
) -> Result<Vec<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(PROJECTS)?;
    let end = before.map_or(u128::MAX, ProjectId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(decode_project(key.value(), bytes.value())?);
    }
    Ok(records)
}

// ---- configs -----------------------------------------------------------------------

pub(super) fn config_record(row: ConfigRow) -> ConfigRecord {
    ConfigRecord {
        id: row.id,
        kind: row.kind,
        name: row.name,
        edit_seq: row.edit_seq,
        created_at: row.created_at,
        updated_at: row.updated_at,
        archived_at: row.archived_at,
        body: row.body,
    }
}

pub(crate) fn read_config(
    database: &Database,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()).map(config_record))
        .transpose()
}

/// Configs of `kind` (any kind when `None`), newest first, strictly before
/// `before` when given. Archived configs are included.
pub(crate) fn read_configs(
    database: &Database,
    kind: Option<ConfigKind>,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<CursorPage<ConfigRecord, ConfigId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CONFIGS)?;
    let end = before.map_or(u128::MAX, ConfigId::get);
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut last = None;
    let mut more = false;
    for (scanned, entry) in table.range(..end)?.rev().enumerate() {
        if records.len() == limit || scanned == MAX_PAGE {
            more = true;
            break;
        }
        let (key, bytes) = entry?;
        last = Some(ConfigId::new(key.value()));
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if kind.is_none_or(|kind| kind == row.kind) {
            records.push(config_record(row));
        }
    }
    Ok(CursorPage {
        records,
        next: last.filter(|_| more),
    })
}

/// One bounded page of active Model, Agent and Pool configs before `before`,
/// plus point reads of the Connections those Models name. Pool protocol offers
/// travel in the same catalog scan; compatibility is joined after paging.
/// `next_before` advances even when no visible config is found.
pub(crate) struct ProfilePageRecords {
    pub records: Vec<ConfigRecord>,
    pub connections: BTreeMap<ConfigId, (String, ConnectionConfig)>,
    pub next_before: Option<ConfigId>,
}

pub(crate) fn read_profile_page(
    database: &Database,
    before: Option<ConfigId>,
    limit: usize,
    scan: usize,
) -> Result<ProfilePageRecords, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let end = before.map_or(u128::MAX, ConfigId::get);
    let limit = limit.max(1);
    let scan = scan.max(limit);
    let mut records = Vec::new();
    let mut last = None;
    let mut more = false;
    for (examined, entry) in configs.range(..end)?.rev().enumerate() {
        if examined >= scan {
            more = true;
            break;
        }
        let (key, bytes) = entry?;
        let id = key.value();
        let row = ConfigRow::decode_at(id, bytes.value())?;
        last = Some(ConfigId::new(id));
        if matches!(
            row.body,
            ConfigBody::Model(_)
                | ConfigBody::Agent(_)
                | ConfigBody::Pool(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::DecisionAgent(_)
        ) && row.archived_at.is_none()
        {
            records.push(config_record(row));
        }
        if records.len() >= limit {
            more = true;
            break;
        }
    }
    if more
        && let Some(last) = last
        && configs.range(..last.get())?.next().is_none()
    {
        more = false;
    }
    let next_before = more.then_some(last).flatten();

    let mut connections = BTreeMap::new();
    for record in &records {
        let connection_id = match &record.body {
            ConfigBody::Model(model) => model.connection_id,
            ConfigBody::DecisionModel(model) => model.connection_id,
            ConfigBody::Agent(_)
            | ConfigBody::DecisionAgent(_)
            | ConfigBody::Pool(_)
            | ConfigBody::Connection(_) => continue,
        };
        if connections.contains_key(&connection_id) {
            continue;
        }
        let Some(bytes) = configs.get(connection_id.get())? else {
            continue;
        };
        let row = ConfigRow::decode_at(connection_id.get(), bytes.value())?;
        if let ConfigBody::Connection(connection) = row.body {
            connections.insert(connection_id, (row.name, connection));
        }
    }
    Ok(ProfilePageRecords {
        records,
        connections,
        next_before,
    })
}

fn pool_view(row: ConfigRow) -> Option<ExecutionPoolRecord> {
    let ConfigBody::Pool(body) = row.body else {
        return None;
    };
    Some(ExecutionPoolRecord {
        id: row.id.into(),
        name: row.name,
        config: ExecutionPoolConfig {
            concurrency_limit: std::num::NonZeroU32::new(body.concurrency_limit)?,
            capabilities: body.capability_set(),
        },
        enabled: body.enabled,
        edit_seq: row.edit_seq,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn read_pool_view<T>(
    database: &Database,
    id: u128,
    view: impl FnOnce(ConfigRow) -> Option<T>,
) -> Result<Option<T>, StorageError> {
    let rx = read(database)?;
    Ok(rx
        .open_table(CONFIGS)?
        .get(id)?
        .map(|bytes| ConfigRow::decode_at(id, bytes.value()))
        .transpose()?
        .and_then(view))
}

fn read_pool_views<T>(
    database: &Database,
    before: u128,
    limit: usize,
    view: impl Fn(ConfigRow) -> Option<T>,
) -> Result<Vec<T>, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let mut records = Vec::new();
    for entry in configs.range(..before)?.rev() {
        let (key, bytes) = entry?;
        if let Some(record) = view(ConfigRow::decode_at(key.value(), bytes.value())?) {
            records.push(record);
            if records.len() >= limit.min(MAX_PAGE) {
                break;
            }
        }
    }
    Ok(records)
}

pub(crate) fn read_execution_pool(
    database: &Database,
    id: ExecutionPoolId,
) -> Result<Option<ExecutionPoolRecord>, StorageError> {
    read_pool_view(database, id.get(), pool_view)
}

pub(crate) fn read_execution_pools(
    database: &Database,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<Vec<ExecutionPoolRecord>, StorageError> {
    read_pool_views(
        database,
        before.map_or(u128::MAX, ExecutionPoolId::get),
        limit,
        pool_view,
    )
}

fn executor_view(row: ConfigRow) -> Option<ExecutorRecord> {
    let ConfigBody::Pool(body) = row.body else {
        return None;
    };
    Some(ExecutorRecord {
        id: row.id.into(),
        name: row.name,
        concurrency_limit: body.concurrency_limit,
        llm_transports: body.capability_set().llm_protocols,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub(crate) fn read_executor(
    database: &Database,
    id: ExecutorId,
) -> Result<Option<ExecutorRecord>, StorageError> {
    read_pool_view(database, id.get(), executor_view)
}

pub(crate) fn read_executors(
    database: &Database,
    before: Option<ExecutorId>,
    limit: usize,
) -> Result<Vec<ExecutorRecord>, StorageError> {
    read_pool_views(
        database,
        before.map_or(u128::MAX, ExecutorId::get),
        limit,
        executor_view,
    )
}

/// The enabled Pools that offer an LLM request over `protocol`, oldest first.
fn pools_offering(
    rx: &ReadTransaction,
    protocol: LlmTransport,
) -> Result<Vec<ExecutorId>, StorageError> {
    pools_offering_capability(rx, ExecutionOperation::LlmRequest, Some(protocol))
}

fn pools_offering_operation(
    rx: &ReadTransaction,
    operation: ExecutionOperation,
) -> Result<Vec<ExecutorId>, StorageError> {
    pools_offering_capability(rx, operation, None)
}

fn pools_offering_capability(
    rx: &ReadTransaction,
    operation: ExecutionOperation,
    protocol: Option<LlmTransport>,
) -> Result<Vec<ExecutorId>, StorageError> {
    let mut pools = Vec::new();
    for entry in rx.open_table(CONFIGS)?.iter()? {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if row.archived_at.is_none()
            && let ConfigBody::Pool(body) = &row.body
            && body.enabled
            && body.offers(operation, protocol)
        {
            pools.push(row.id.into());
            if pools.len() >= MAX_PAGE {
                break;
            }
        }
    }
    Ok(pools)
}

/// The Pools that offer what a Model config's Connection speaks.
pub(crate) fn read_compatible_executors(
    database: &Database,
    llm_profile: ProfileId,
) -> Result<Vec<ExecutorId>, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let Some(model) = configs
        .get(llm_profile.get())?
        .map(|bytes| ConfigRow::decode_at(llm_profile.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(Vec::new());
    };
    let ConfigBody::Model(model) = model.body else {
        return Ok(Vec::new());
    };
    let Some(connection) = configs
        .get(model.connection_id.get())?
        .map(|bytes| ConfigRow::decode_at(model.connection_id.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(Vec::new());
    };
    let ConfigBody::Connection(connection) = connection.body else {
        return Ok(Vec::new());
    };
    pools_offering(&rx, connection.protocol)
}

/// The Pools that offer typed decision work, oldest first.
pub(crate) fn read_decision_executors(
    database: &Database,
) -> Result<Vec<ExecutorId>, StorageError> {
    let rx = read(database)?;
    pools_offering_operation(&rx, ExecutionOperation::Decision)
}

// ---- claims and held slots ------------------------------------------------------------

pub(crate) fn read_pool_binding(
    database: &Database,
    job: JobId,
) -> Result<Option<FrozenPoolBinding>, StorageError> {
    let rx = read(database)?;
    let Some(bytes) = rx.open_table(JOB_STATES)?.get(job.get())? else {
        return Ok(None);
    };
    let state = JobState::decode_at(job.get(), bytes.value())?;
    Ok(state.claim.map(|claim| FrozenPoolBinding {
        pool: claim.pool_id.into(),
        edit_seq: claim.pool_edit_seq,
        capabilities: claim.capabilities,
    }))
}

pub(crate) fn read_occupancy(
    database: &Database,
) -> Result<Vec<(ExecutionPoolId, JobId)>, StorageError> {
    let rx = read(database)?;
    let mut held = Vec::new();
    for entry in rx.open_table(HELD_SLOTS)?.iter()? {
        let (pool, job) = entry?.0.value();
        held.push((ExecutionPoolId::new(pool), JobId::new(job)));
    }
    Ok(held)
}

/// A held slot whose Job's stop was never proven: quarantined capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnresolvedExecution {
    pub epoch: u64,
    pub unresolved_at: Timestamp,
    pub pool: Option<ExecutionPoolId>,
}

pub(crate) fn read_unresolved_executions(
    database: &Database,
) -> Result<Vec<(JobId, UnresolvedExecution)>, StorageError> {
    let rx = read(database)?;
    let mut unresolved = Vec::new();
    for entry in rx.open_table(HELD_SLOTS)?.iter()? {
        let (key, value) = entry?;
        let ((pool, job), (epoch, quarantined)) = (key.value(), value.value());
        if quarantined {
            let state = read_state(&rx, JobId::new(job))?;
            unresolved.push((
                JobId::new(job),
                UnresolvedExecution {
                    epoch,
                    unresolved_at: state
                        .outcome
                        .map_or(Timestamp::from_unix_micros(0), |outcome| outcome.at),
                    pool: Some(ExecutionPoolId::new(pool)),
                },
            ));
        }
    }
    Ok(unresolved)
}

/// Whether the natural key of a command is present in the database.
pub(crate) fn probe_committed(database: &Database, probe: Probe) -> Result<bool, StorageError> {
    let rx = read(database)?;
    Ok(match probe {
        Probe::Job(job) => rx.open_table(JOBS)?.get(job.get())?.is_some(),
        Probe::Submission(key) => rx
            .open_table(SUBMISSIONS)?
            .get(key.as_bytes().as_slice())?
            .is_some(),
        Probe::Claim(job) => rx
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|state| state.claim.is_some()),
        Probe::ToolCall(call) => rx
            .open_table(TOOL_CALLS)?
            .get(tool_call_key(call))?
            .is_some(),
        Probe::ToolOutcome(call) => rx
            .open_table(TOOL_CALLS)?
            .get(tool_call_key(call))?
            .map(|bytes| decode_tool_evidence(call, bytes.value()))
            .transpose()?
            .is_some_and(|record| record.outcome.is_some()),
        Probe::ProviderAvailability(job, id, digest) => {
            let index = rx.open_table(JOB_OBSERVATIONS)?;
            let events = rx.open_table(REQUEST_EVENTS)?;
            if let Some(key) = index.get((job.get(), 2, id.get()))? {
                let (request, seq) = key.value();
                events
                    .get((job.get(), request, seq))?
                    .is_some_and(|bytes| ContentDigest::of(bytes.value()) == digest)
            } else {
                false
            }
        }
        Probe::ProviderOutcome(job) => rx
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|state| {
                state.outcome.is_some_and(|outcome| {
                    outcome.cause != Some(crate::db::entities::OutcomeCause::Abandoned)
                })
            }),
        Probe::Outcome(job) => rx
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|state| state.outcome.is_some()),
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationEntry {
    pub root_job: JobId,
    pub tip_job: JobId,
    pub agent: Option<ConfigId>,
    pub title: String,
    pub project: Option<ProjectId>,
    pub state: String,
    pub on_hold: Option<Actor>,
    pub tip_settlement_state: String,
    pub tip_created_at: Timestamp,
    pub job_state: String,
    pub has_children: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ConversationsPage {
    pub conversations: Vec<ConversationEntry>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

fn conversation_job_state(
    rx: &ReadTransaction,
    conversation: JobId,
    tip: JobId,
) -> Result<String, StorageError> {
    let mut awaiting_user = false;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        let (_, state) = entry?;
        if state.value() == 0 {
            awaiting_user = true;
            break;
        }
    }
    Ok(if read_state(rx, conversation)?.outcome.is_some() {
        turn_state(rx, conversation)?
    } else if awaiting_user {
        "waiting_for_user"
    } else if let Some((active, _, _)) = active_job(rx, conversation)? {
        turn_state(rx, active)?
    } else {
        turn_state(rx, tip)?
    }
    .to_owned())
}

fn has_visible_child(rx: &ReadTransaction, conversation: JobId) -> Result<bool, StorageError> {
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
    {
        let child = JobId::new(entry?.0.value().1);
        if let Some(row) = read_job_row(rx, child)?
            && let JobKind::Group {
                origin: Origin::User | Origin::Agent { .. },
                ..
            } = row.kind
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn conversation_summary(
    rx: &ReadTransaction,
    conversation: JobId,
    tip: JobId,
) -> Result<ConversationEntry, StorageError> {
    let row = read_job_row(rx, conversation)?
        .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
    let JobKind::Group {
        agent,
        project,
        origin,
        ..
    } = row.kind
    else {
        return Err(StorageError::corrupted(
            "recent entry is not a conversation",
        ));
    };
    let metadata = rx
        .open_table(JOB_CONTROLS)?
        .get(conversation.get())?
        .map(|bytes| JobControl::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("missing conversation state"))?;
    let title = match metadata.title {
        Some(title) => title,
        None => {
            let first = rx
                .open_table(CHILDREN)?
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .next()
                .transpose()?
                .map(|entry| entry.0.value().1);
            match first {
                Some(first) if matches!(origin, Origin::System) => {
                    let first = JobId::new(first);
                    match read_job_row(rx, first)?
                        .and_then(|row| row.kind.name().map(str::to_owned))
                    {
                        Some(name) => name,
                        None => first_entry_text(rx, first)?,
                    }
                }
                Some(first) => first_entry_text(rx, JobId::new(first))?,
                None => String::new(),
            }
        }
    };
    let state = read_state(rx, conversation)?;
    let on_hold = metadata
        .on_hold
        .map(|seq| {
            rx.open_table(NOTICES)?
                .get((conversation.get(), seq))?
                .map(|bytes| Notice::decode(bytes.value()))
                .transpose()
        })
        .transpose()?
        .flatten()
        .and_then(|notice| match notice.body {
            NoticeBody::Record { by, .. } => Some(by),
            _ => None,
        });
    let tip_state = read_state(rx, tip)?;
    let tip_row = read_job_row(rx, tip)?.ok_or_else(|| StorageError::corrupted("missing tip"))?;
    Ok(ConversationEntry {
        root_job: conversation,
        tip_job: tip,
        agent,
        project,
        title,
        state: if state.outcome.is_some() {
            "settled"
        } else if metadata.closing.is_some() {
            "closing"
        } else {
            "open"
        }
        .to_owned(),
        on_hold,
        tip_settlement_state: tip_state
            .outcome
            .map_or("unsettled", |outcome| match outcome.kind {
                OutcomeKind::Completed => "completed",
                OutcomeKind::Failed => "failed",
                OutcomeKind::Cancelled => "cancelled",
                OutcomeKind::Unknown => "unknown",
            })
            .to_owned(),
        tip_created_at: tip_row.created_at,
        job_state: conversation_job_state(rx, conversation, tip)?,
        has_children: has_visible_child(rx, conversation)?,
    })
}
#[derive(Clone, Debug, PartialEq)]
pub struct ChildConversationEntry {
    pub conversation: ConversationEntry,
    pub origin: Origin,
    pub keep_open: bool,
    pub recurrence: Option<Recurrence>,
    pub job_state: String,
}
pub(crate) fn read_conversation_children(
    database: &Database,
    parent: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<ChildConversationEntry>, StorageError> {
    let rx = read(database)?;
    let mut children = Vec::new();
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((parent.get(), 0)..(parent.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .take(limit.min(MAX_PAGE))
    {
        let child = JobId::new(entry?.0.value().1);
        let row = read_job_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted("child conversation missing"))?;
        let JobKind::Group {
            origin, keep_open, ..
        } = row.kind
        else {
            return Err(StorageError::corrupted("child is not a conversation"));
        };
        let tip = rx
            .open_table(GROUP_HEADS)?
            .get(child.get())?
            .map(|id| JobId::new(id.value()))
            .ok_or_else(|| StorageError::corrupted("child tip missing"))?;
        let conversation = conversation_summary(&rx, child, tip)?;
        children.push(ChildConversationEntry {
            job_state: conversation.job_state.clone(),
            conversation,
            origin,
            keep_open,
            recurrence: rx
                .open_table(RECURRENCES)?
                .get(child.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?,
        });
    }
    Ok(children)
}

pub(crate) fn read_conversations(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, &[])
}

pub(crate) fn read_conversation_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, terms)
}

fn conversation_page(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
    terms: &[String],
) -> Result<ConversationsPage, StorageError> {
    let rx = read(database)?;
    let groups = rx.open_table(CONVERSATIONS)?;
    let limit = limit.clamp(1, 100);
    let mut conversations = Vec::new();
    let mut last = None;
    let mut has_more = false;
    for item in groups.range(..before.map_or(u128::MAX, JobId::get))?.rev() {
        if conversations.len() == limit {
            has_more = true;
            break;
        }
        let conversation = JobId::new(item?.0.value());
        last = Some(conversation);
        let row = read_job_row(&rx, conversation)?
            .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
        if !indexes::searchable_conversation(&row)
            || read_job_control(&rx, conversation)?.hidden_at.is_some()
        {
            continue;
        }
        let Some(tip) = rx
            .open_table(GROUP_HEADS)?
            .get(conversation.get())?
            .map(|value| JobId::new(value.value()))
        else {
            continue;
        };
        let summary = conversation_summary(&rx, conversation, tip)?;
        if terms.is_empty() || conversation_matches(&rx, conversation, &summary.title, terms)? {
            conversations.push(summary);
        }
    }
    Ok(ConversationsPage {
        conversations,
        next_before: last.filter(|_| has_more),
        has_more,
    })
}

/// Gram postings locate candidate source rows across the whole history.
/// Source verification preserves literal substring matching, including Unicode
/// and punctuation, without treating the index as authoritative text.
fn conversation_matches(
    rx: &ReadTransaction,
    conversation: JobId,
    title: &str,
    terms: &[String],
) -> Result<bool, StorageError> {
    let title = title.to_lowercase();
    let index = rx.open_table(CONVERSATION_SEARCH)?;
    for term in terms {
        if title.contains(term) {
            continue;
        }
        let chars = term.chars().take(3).collect::<Vec<_>>();
        let gram = indexes::search_gram(&chars);
        let mut matched = false;
        for candidate in index.range(
            (conversation.get(), gram, 0, 0, 0)
                ..=(conversation.get(), gram, u128::MAX, u8::MAX, u32::MAX),
        )? {
            let (_, _, turn, source, seq) = candidate?.0.value();
            let row = read_job_row(rx, JobId::new(turn))?
                .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
            if !indexes::searchable_turn(&row) {
                continue;
            }
            let text = match source {
                0 => {
                    let entries = turn_entries(rx, JobId::new(turn))?;
                    let Some((_, entry)) = entries.iter().find(|(current, _)| *current == seq)
                    else {
                        continue;
                    };
                    let Some(text) =
                        indexes::search_input_text(&rx.open_table(ATTACHMENTS)?, &entry.entry)?
                    else {
                        continue;
                    };
                    text
                }
                1 => {
                    let table = rx.open_table(JOB_OUTPUTS)?;
                    let bytes = table
                        .get((turn, seq))?
                        .ok_or_else(|| StorageError::corrupted("search output missing"))?;
                    let output = decode_output((turn, seq), bytes.value())?;
                    let completed = rx
                        .open_table(JOB_STATES)?
                        .get(turn)?
                        .map(|bytes| JobState::decode_at(turn, bytes.value()))
                        .transpose()?
                        .and_then(|state| state.outcome)
                        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                    let Some(text) = indexes::search_output_text(&output, completed) else {
                        continue;
                    };
                    text.to_owned()
                }
                _ => return Err(StorageError::corrupted("invalid search source")),
            };
            if text.to_lowercase().contains(term) {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(crate) fn queue_counts(database: &Database) -> Result<(u64, u64, u64), StorageError> {
    let rx = read(database)?;
    let pending = rx.open_table(PENDING)?.len()?;
    let ready = rx.open_table(READY)?.len()?;
    let delayed = rx.open_table(DELAYED)?.len()?;
    Ok((pending, ready, delayed))
}

pub(crate) fn read_tool_stats(
    database: &Database,
) -> Result<Vec<crate::db::entities::ToolStatRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_STATS)?;
    let mut stats = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max) =
            value.value();
        stats.push(crate::db::entities::ToolStatRecord {
            name: key.value().to_owned(),
            admitted,
            refused_arguments,
            completed,
            failed,
            duration_us_total,
            duration_us_max,
        });
    }
    Ok(stats)
}

pub const MAX_USAGE_SCAN: usize = 100_000;
/// The most groups one page holds. A row is at most a 256-byte model name
/// plus identifiers and counters, so this also keeps the serialized page
/// below the server's response budget.
const MAX_USAGE_PAGE_GROUPS: usize = 512;

fn resolve_model_name(
    rx: &ReadTransaction,
    provider: u128,
    model_hash: [u8; 32],
) -> Result<Option<String>, StorageError> {
    let scopes = rx.open_table(REQUEST_SCOPES)?;
    let start = (provider, model_hash, i64::MIN, 0, 0, 0);
    let end = (
        provider,
        model_hash,
        i64::MAX,
        u128::MAX,
        u32::MAX,
        u32::MAX,
    );
    for entry in scopes.range(start..=end)? {
        let (key, _) = entry?;
        let (_provider, _hash, _obs, job, _req, _seq) = key.value();
        if let Some(job_row) = read_job_row(rx, JobId::new(job))? {
            let jobs = rx.open_table(JOBS)?;
            let Some(identity) = turn_usage_identity_of(&jobs, &job_row)? else {
                continue;
            };
            return Ok(Some(identity.model));
        }
    }
    Ok(None)
}

type UsageGroupKey = (i64, u128, u128, [u8; 32]);
type UsageCounters = (u64, u64, u64, u64, u64, u64, u64);

pub(crate) fn read_usage_summary(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageSummaryResponse, StorageError> {
    read_usage_summary_scanned(
        database,
        filter,
        after,
        MAX_USAGE_SCAN,
        MAX_USAGE_PAGE_GROUPS,
    )
}

pub(crate) fn read_usage_summary_scanned(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    after: Option<crate::db::entities::UsageCursor>,
    row_budget: usize,
    group_budget: usize,
) -> Result<crate::db::entities::UsageSummaryResponse, StorageError> {
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageSummaryResponse};

    // `from_us` is inclusive and rounds down to its bucket; `until_us` is
    // exclusive and rounds up, so the buckets scanned are exactly those an
    // instant in `[from_us, until_us)` falls in. The last bucket is
    // `ceil(until_us / HOUR) - 1`.
    let from_hour = filter
        .from_us
        .map(|us| us.div_euclid(HOUR))
        .unwrap_or(i64::MIN);
    let until_end = match filter.until_us {
        None => None,
        Some(us) => {
            let hour = us.div_euclid(HOUR);
            Some(if us.rem_euclid(HOUR) == 0 {
                hour
            } else {
                hour.checked_add(1).ok_or_else(|| {
                    StorageError::corrupted("usage summary until_us overflows its hour bucket")
                })?
            })
        }
    };
    let until_end_us = match until_end {
        Some(hour) => Some(hour.checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary until_us overflows its hour bucket")
        })?),
        None => None,
    };
    let until_hour = match until_end {
        Some(hour) => hour - 1,
        None => i64::MAX,
    };

    let requested_from_us = match filter.from_us {
        Some(us) => Some(us.div_euclid(HOUR).checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary from_us underflows its hour bucket")
        })?),
        None => None,
    };

    if from_hour > until_hour {
        return Ok(UsageSummaryResponse {
            from_us: requested_from_us.unwrap_or(0),
            until_us: until_end_us.unwrap_or(0),
            complete: true,
            resume_after: None,
            rows: Vec::new(),
        });
    }

    let UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    } = scan_usage_totals(
        database,
        filter,
        from_hour,
        until_hour,
        None,
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    let rows = rows.into_iter().map(|bucket| bucket.row).collect();

    let effective_from_us = requested_from_us
        .or_else(|| earliest_hour.map(|hour| hour.saturating_mul(HOUR)))
        .unwrap_or(0);

    let effective_until_us = match until_end_us {
        Some(until) => until,
        None => latest_hour
            .and_then(|hour| hour.checked_add(1))
            .map_or(0, |hour| hour.saturating_mul(HOUR)),
    };

    Ok(UsageSummaryResponse {
        from_us: effective_from_us,
        until_us: effective_until_us,
        // Complete says the scan finished, not that every request's usage is
        // final. `resume_after` is set exactly when it did not.
        complete: next.is_none(),
        resume_after: next,
        rows,
    })
}

pub(crate) fn read_usage_buckets(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: &crate::db::entities::UsageBucketing,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageBucketResponse, StorageError> {
    read_usage_buckets_scanned(
        database,
        filter,
        bucketing,
        after,
        MAX_USAGE_SCAN,
        MAX_USAGE_PAGE_GROUPS,
    )
}

pub(crate) fn read_usage_buckets_scanned(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: &crate::db::entities::UsageBucketing,
    after: Option<crate::db::entities::UsageCursor>,
    row_budget: usize,
    group_budget: usize,
) -> Result<crate::db::entities::UsageBucketResponse, StorageError> {
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketResponse};

    if bucketing.offset_minutes % 60 != 0 || !(-840..=840).contains(&bucketing.offset_minutes) {
        return Err(StorageError::corrupted("invalid usage bucket offset"));
    }
    let from_us = filter
        .from_us
        .and_then(|us| bucketing.start_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable from_us"))?;
    let until_us = filter
        .until_us
        .and_then(|us| us.checked_sub(1))
        .and_then(|us| bucketing.end_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable until_us"))?;
    let scan = scan_usage_totals(
        database,
        filter,
        from_us.div_euclid(HOUR),
        until_us.div_euclid(HOUR) - 1,
        Some(bucketing),
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    Ok(UsageBucketResponse {
        from_us,
        until_us,
        complete: scan.next.is_none(),
        resume_after: scan.next,
        rows: scan.rows,
    })
}

#[derive(Clone, Copy)]
struct UsageBudget {
    rows: usize,
    groups: usize,
}

#[derive(Default)]
struct UsageScan {
    rows: Vec<crate::db::entities::UsageBucketRow>,
    earliest_hour: Option<i64>,
    latest_hour: Option<i64>,
    next: Option<crate::db::entities::UsageCursor>,
}

/// Reads one bounded window of `usage_total` rows, newest bucket last. Every
/// examined row counts against `row_budget`, including rows the agent filter
/// excludes, and every group counts against `group_budget`. A window may end
/// inside a group, so a page's rows can be partial aggregates; `next` is the
/// last row examined and continues strictly after it. `earliest_hour` and
/// `latest_hour` describe the whole requested range, not just this window, so
/// every page of one traversal reports the same effective bounds.
fn scan_usage_totals(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    from_hour: i64,
    until_hour: i64,
    bucketing: Option<&crate::db::entities::UsageBucketing>,
    after: Option<crate::db::entities::UsageCursor>,
    budget: UsageBudget,
) -> Result<UsageScan, StorageError> {
    use crate::db::entities::{
        USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketRow, UsageCursor, UsageSummaryRow,
    };

    let rx = read(database)?;
    let table = rx.open_table(USAGE_TOTALS)?;
    if from_hour > until_hour || budget.rows == 0 || budget.groups == 0 {
        return Ok(UsageScan::default());
    }
    let range_start = (from_hour, 0, 0, [0u8; 32]);
    let end = (until_hour, u128::MAX, u128::MAX, [0xffu8; 32]);

    let configs = rx.open_table(CONFIGS)?;
    let mut agents = std::collections::HashMap::new();
    let mut matches_agent = |agent: u128| -> Result<bool, StorageError> {
        if filter.agent.is_some_and(|target| agent != target) {
            return Ok(false);
        }
        if !filter.generative_only || agent == 0 {
            return Ok(true);
        }
        if let Some(generative) = agents.get(&agent) {
            return Ok(*generative);
        }
        let bytes = configs
            .get(agent)?
            .ok_or_else(|| StorageError::corrupted("usage Agent missing"))?;
        let generative = match ConfigRow::decode_at(agent, bytes.value())?.kind {
            ConfigKind::Agent => true,
            ConfigKind::DecisionAgent => false,
            ConfigKind::Connection
            | ConfigKind::Model
            | ConfigKind::Pool
            | ConfigKind::DecisionModel => {
                return Err(StorageError::corrupted(
                    "usage Agent has an incompatible kind",
                ));
            }
        };
        agents.insert(agent, generative);
        Ok(generative)
    };
    let mut earliest_hour = None;
    if filter.from_us.is_none() {
        for entry in table.range(range_start..=end)? {
            let key = entry?.0.value();
            if matches_agent(key.1)? {
                earliest_hour = Some(key.0);
                break;
            }
        }
    }
    let mut latest_hour = None;
    if filter.until_us.is_none() {
        for entry in table.range(range_start..=end)?.rev() {
            let key = entry?.0.value();
            if matches_agent(key.1)? {
                latest_hour = Some(key.0);
                break;
            }
        }
    }

    let start = match after {
        Some(cursor) => {
            let key = (
                cursor.hour,
                cursor.agent,
                cursor.provider,
                cursor.model_hash,
            );
            if key < range_start {
                Bound::Included(range_start)
            } else {
                Bound::Excluded(key)
            }
        }
        None => Bound::Included(range_start),
    };

    let mut grouped: std::collections::BTreeMap<UsageGroupKey, UsageCounters> =
        std::collections::BTreeMap::new();
    let mut last = None;
    let mut stopped = false;

    for (examined, entry) in table.range((start, Bound::Included(end)))?.enumerate() {
        if examined >= budget.rows {
            stopped = true;
            break;
        }
        let (key, value) = entry?;
        let (hour, agent, provider, model_hash) = key.value();
        let cursor = UsageCursor {
            hour,
            agent,
            provider,
            model_hash,
        };
        if !matches_agent(agent)? {
            last = Some(cursor);
            continue;
        }

        let bucket_us = match bucketing {
            Some(bucketing) => hour
                .checked_mul(HOUR)
                .and_then(|us| bucketing.start_of(us))
                .ok_or_else(|| StorageError::corrupted("usage bucket overflows"))?,
            None => hour,
        };
        let group = (
            if bucketing.is_some() { bucket_us } else { 0 },
            agent,
            provider,
            model_hash,
        );
        if !grouped.contains_key(&group) && grouped.len() >= budget.groups {
            stopped = true;
            break;
        }

        last = Some(cursor);

        let (reqs, comp, inp, out, cr, cw, reas) = value.value();
        let slot = grouped.entry(group).or_insert((0, 0, 0, 0, 0, 0, 0));
        slot.0 = slot.0.saturating_add(reqs);
        slot.1 = slot.1.saturating_add(comp);
        slot.2 = slot.2.saturating_add(inp);
        slot.3 = slot.3.saturating_add(out);
        slot.4 = slot.4.saturating_add(cr);
        slot.5 = slot.5.saturating_add(cw);
        slot.6 = slot.6.saturating_add(reas);
    }

    let next = if stopped {
        match last {
            Some(cursor) => {
                let key = (
                    cursor.hour,
                    cursor.agent,
                    cursor.provider,
                    cursor.model_hash,
                );
                table
                    .range((Bound::Excluded(key), Bound::Included(end)))?
                    .next()
                    .transpose()?
                    .map(|_| cursor)
            }
            None => None,
        }
    } else {
        None
    };

    let mut model_cache: std::collections::HashMap<(u128, [u8; 32]), Option<String>> =
        std::collections::HashMap::new();
    let mut rows = Vec::with_capacity(grouped.len());

    for ((bucket_us, agent, provider, model_hash), (reqs, comp, inp, out, cr, cw, reas)) in grouped
    {
        let model = match model_cache.entry((provider, model_hash)) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resolved = resolve_model_name(&rx, provider, model_hash)?;
                e.insert(resolved.clone());
                resolved
            }
        };
        rows.push(UsageBucketRow {
            bucket_us,
            row: UsageSummaryRow {
                agent,
                provider,
                model,
                requests: reqs,
                complete_requests: comp,
                input_tokens: inp,
                output_tokens: out,
                cache_read_tokens: cr,
                cache_write_tokens: cw,
                reasoning_tokens: reas,
            },
        });
    }

    Ok(UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    })
}

pub(crate) fn read_turn_execution(
    database: &Database,
    turn: JobId,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let rx = read(database)?;
    let row = read_job_row(&rx, turn)?.ok_or_else(|| StorageError::corrupted("turn missing"))?;
    job_execution(&rx.open_table(JOBS)?, &row)
}

pub(crate) fn read_turn_compaction(
    database: &Database,
    turn: JobId,
) -> Result<Option<(JobId, &'static str, Timestamp)>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, turn)? else {
        return Ok(None);
    };
    if read_state(&rx, turn)?.claim.is_none() {
        return Ok(None);
    }
    let selected = turn_entries(&rx, turn)?
        .into_iter()
        .rev()
        .find_map(|(_, entry)| match entry.entry {
            Entry::ContextUse { artifact } => Some((artifact, entry.at)),
            _ => None,
        });
    let Some((Some(artifact), at)) = selected else {
        return Ok(None);
    };
    let mut previous = row.kind.previous();
    let mut seen = BTreeSet::new();
    while let Some(id) = previous {
        if !seen.insert(id) {
            return Err(StorageError::corrupted("context ancestry cycle"));
        }
        let used = turn_entries(&rx, id)?
            .into_iter()
            .rev()
            .find_map(|(_, entry)| match entry.entry {
                Entry::ContextUse { artifact } => Some(artifact),
                _ => None,
            });
        if let Some(used) = used {
            if used == Some(artifact) {
                return Ok(None);
            }
            break;
        }
        previous = read_job_row(&rx, id)?.and_then(|row| row.kind.previous());
    }
    let Some(producer) = read_job_row(&rx, artifact)? else {
        return Err(StorageError::corrupted("adopted context artifact missing"));
    };
    let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = producer.kind
    else {
        return Err(StorageError::corrupted(
            "adopted context has no summary producer",
        ));
    };
    let root = summary_root(&rx.open_table(JOBS)?, &work)?;
    let kind = if root.prediction.is_some() {
        "predictive"
    } else if root.policy_revision.is_some() {
        "rule_based"
    } else {
        "manual"
    };
    Ok(Some((artifact, kind, at)))
}

pub(crate) fn read_turn_entries(
    database: &Database,
    turn: JobId,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    let rx = read(database)?;
    job_entries_of(&rx.open_table(JOB_INPUTS)?, turn)
}
fn turn_entries(
    rx: &ReadTransaction,
    turn: JobId,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    effective_entries(job_entries_of(&rx.open_table(JOB_INPUTS)?, turn)?)
}

pub(super) fn job_entries_of(
    inputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    turn: JobId,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    inputs
        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().1, JobInputEntry::decode(value.value())?))
        })
        .collect()
}

pub(super) fn effective_entries(
    entries: Vec<(u32, JobInputEntry)>,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    crate::db::entities::effective_entries(entries).map_err(StorageError::corrupted)
}

pub(crate) fn read_notices(
    database: &Database,
    conversation: JobId,
    after: u32,
    limit: usize,
) -> Result<Vec<(u32, Notice)>, StorageError> {
    let rx = read(database)?;
    if after == u32::MAX {
        return Ok(Vec::new());
    }
    rx.open_table(NOTICES)?
        .range((conversation.get(), after + 1)..=(conversation.get(), u32::MAX))?
        .take(limit.min(MAX_PAGE))
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().1, Notice::decode(value.value())?))
        })
        .collect()
}
pub(crate) fn read_conversation_turns(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobRecord>, StorageError> {
    let rx = read(database)?;
    if rx
        .open_table(CONVERSATIONS)?
        .get(conversation.get())?
        .is_none()
    {
        return Ok(Vec::new());
    }
    if let Some(before) = before
        && !read_job_row(&rx, before)?
            .is_some_and(|row| row.parent_id == Some(conversation) && row.kind.is_persistent())
    {
        return Ok(Vec::new());
    }
    let bounds = (
        Bound::Included((conversation.get(), 0)),
        before.map_or(Bound::Included((conversation.get(), u128::MAX)), |before| {
            Bound::Excluded((conversation.get(), before.get()))
        }),
    );
    let mut result = Vec::new();
    for entry in rx
        .open_table(TURNS)?
        .range(bounds)?
        .rev()
        .take(limit.min(101))
    {
        let (key, _) = entry?;
        let row = read_job_row(&rx, JobId::new(key.value().1))?
            .ok_or_else(|| StorageError::corrupted("turn projection has no source job"))?;
        result.push(job_record(&rx, row)?);
    }
    Ok(result)
}

pub(crate) fn read_conversation_limits(
    database: &Database,
    conversation: JobId,
) -> Result<Option<(JobLimits, u8)>, StorageError> {
    let rx = read(database)?;
    let mut current = conversation;
    let mut limits = JobLimits::CEILING;
    for depth in 0..=crate::db::entities::MAX_JOB_DEPTH {
        let Some(row) = read_job_row(&rx, current)? else {
            return Ok(None);
        };
        if !row.kind.is_group() {
            return Ok(None);
        }
        let own = read_job_control(&rx, current)?.limits;
        if let Some(own) = own {
            limits = limits.intersect(own);
        }
        match row.parent_id {
            Some(parent) => current = parent,
            None => return Ok(Some((limits.intersect(own.unwrap_or_default()), depth))),
        }
    }
    Err(StorageError::corrupted(
        "conversation ancestry exceeds the hard depth limit",
    ))
}

#[derive(Clone, Debug)]
pub struct ContextTurn {
    pub turn: JobId,
    pub text: String,
    pub attachments: Vec<AttachmentId>,
    pub attachment_bytes: Vec<u64>,
    pub answer: Option<String>,
}
#[derive(Clone, Debug)]
pub struct TurnContext {
    pub turns: Vec<ContextTurn>,
    pub omitted: bool,
    pub attention: String,
    pub tree_status: String,
    pub response_format: Option<ResponseFormat>,
    /// A validated summary that replaces the covered prefix. The original
    /// records remain stored; this is only the prompt view.
    pub adopted_summary: Option<String>,
    /// Turns the adopted summary covers. The suffix after them stays raw.
    pub covered_turns: Vec<JobId>,
}

fn entry_text(rx: &ReadTransaction, entry: &Entry) -> Result<String, StorageError> {
    Ok(match entry {
        Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } => text.clone(),
        Entry::Notice {
            child,
            event,
            reference,
        } => {
            let answer = rx
                .open_table(JOB_OUTPUTS)?
                .range((reference.get(), 0)..=(reference.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(key, value)| decode_output(key.value(), value.value()))
                .transpose()?
                .filter(|output| output.kind == OutputKind::Result)
                .and_then(|output| output.display_text())
                .unwrap_or_default();
            format!("Child conversation {child}: {event:?}; result of turn {reference}: {answer}")
        }
        Entry::RetryOf { turn, .. } => {
            let mut text = format!(
                "Retry {turn}. Its started tool calls below are uncertain; do not replay them automatically."
            );
            // A retry of a retry does not repeat the original prompt, so the
            // chain of retries is walked to the root turn that has none, and
            // that root's entries and uncertain calls come first.
            let mut chain = Vec::new();
            let mut visited = BTreeSet::new();
            let mut next = Some(*turn);
            while let Some(current) = next {
                if !visited.insert(current) {
                    break;
                }
                chain.push(current);
                next = turn_entries(rx, current)?
                    .into_iter()
                    .find_map(|(_, entry)| match entry.entry {
                        Entry::RetryOf { turn: original, .. } => Some(original),
                        _ => None,
                    });
            }
            chain.reverse();
            for current in &chain {
                for (_, original) in turn_entries(rx, *current)? {
                    if !matches!(
                        original.entry,
                        Entry::RetryOf { .. }
                            | Entry::ResponseFormat { .. }
                            | Entry::UserMessageEdit { .. }
                            | Entry::UserMessageWithdraw { .. }
                            | Entry::ContextPreparation { .. }
                            | Entry::ContextUse { .. }
                    ) {
                        text.push('\n');
                        text.push_str(&entry_text(rx, &original.entry)?);
                    }
                }
            }
            for current in &chain {
                for call in job_tool_evidence(&rx.open_table(TOOL_CALLS)?, *current)? {
                    if call.admission().is_some()
                        && call.outcome.as_ref().is_none_or(|outcome| {
                            matches!(
                                outcome.fact,
                                crate::db::entities::ToolOutcome::Uncertain { .. }
                            )
                        })
                    {
                        text.push_str(&format!(
                            "\nUncertain: request {}, index {}, tool {}, arguments {}",
                            call.call.request.get(),
                            call.call.index,
                            call.name,
                            call.admission()
                                .map(|admission| admission.arguments.as_str())
                                .unwrap_or("{}")
                        ));
                    }
                }
            }
            text
        }
        Entry::Decision { approval } => {
            let call = tool_call_ref((approval.0.get(), approval.1, approval.2))?;
            let bytes = rx
                .open_table(TOOL_CALLS)?
                .get(tool_call_key(call))?
                .ok_or_else(|| StorageError::corrupted("approval call missing"))?
                .value()
                .to_vec();
            let call = decode_tool_evidence(call, &bytes)?;
            let bytes = rx
                .open_table(APPROVALS)?
                .get((approval.0.get(), approval.1, approval.2))?
                .ok_or_else(|| StorageError::corrupted("approval decision missing"))?
                .value()
                .to_vec();
            let decision = ApprovalRecord::decode(&bytes)?;
            let arguments = call
                .decision
                .fact
                .stored_admission()
                .map_or("{}", |a| a.arguments.as_str());
            match decision.decision {
                ApprovalDecision::Approved => format!("Approved: {} {}", call.name, arguments),
                ApprovalDecision::Rejected => format!("Rejected: {} {}", call.name, arguments),
                ApprovalDecision::Answered { text } => {
                    format!("Answer to {}: {}", question(arguments), text)
                }
                ApprovalDecision::Voided => format!("Voided: {} {}", call.name, arguments),
            }
        }
        Entry::Iteration { n } => format!("Iteration {n}"),
        Entry::ResponseFormat { .. }
        | Entry::UserMessageEdit { .. }
        | Entry::UserMessageWithdraw { .. }
        | Entry::ContextPreparation { .. }
        | Entry::ContextUse { .. } => String::new(),
    })
}

/// One immutable source turn, rendered as data for summarization. The hash
/// covers framing, identity, frozen configuration, inputs, results and tools.
pub(super) struct SealedTurn {
    pub boundary: crate::db::entities::SourceBoundary,
    pub text: String,
    pub reducible_tokens: usize,
    pub context_tokens: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn source_turn(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    states: &impl ReadableTable<u128, &'static [u8]>,
    inputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    outputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    tools: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    approvals: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    turn: JobId,
    bounds: Option<(u32, u32)>,
) -> Result<Option<SealedTurn>, StorageError> {
    let Some(job) = jobs.get(turn.get())? else {
        return Ok(None);
    };
    let row = JobSpec::decode_at(turn.get(), job.value())?;
    if matches!(
        row.kind,
        JobKind::Execution {
            execution: JobExecution::Decision(_),
            ..
        }
    ) {
        return Ok(None);
    }
    let Some(bytes) = states.get(turn.get())? else {
        return Ok(None);
    };
    let state = JobState::decode_at(turn.get(), bytes.value())?;
    if !state.locally_stopped() {
        return Ok(None);
    }
    let mut canonical = b"ocg-context-source-v2".to_vec();
    let mut frame = |bytes: &[u8]| {
        canonical.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        canonical.extend_from_slice(bytes);
    };
    frame(job.value());
    frame(format!("{:?}", state.outcome).as_bytes());
    let mut text = format!("Turn {turn}, outcome {:?}\n", state.outcome);
    let mut input_bytes = 0usize;
    let mut answer_bytes = 0usize;
    let frozen = job_execution(jobs, &row)?.0;
    text.push_str(&format!(
        "Frozen model {:?}; Agent {:?}. Active instructions are supplied separately.\n",
        frozen.llm.model, frozen.agent_ref
    ));
    let mut input_through = 0u32;
    let mut journal = Vec::new();
    for entry in inputs.range((turn.get(), 1)..=(turn.get(), bounds.map_or(u32::MAX, |b| b.0)))? {
        let (key, bytes) = entry?;
        if key.value().1 != input_through.saturating_add(1) {
            return Ok(None);
        }
        input_through = key.value().1;
        frame(bytes.value());
        journal.push((key.value().1, JobInputEntry::decode(bytes.value())?));
    }
    for (_, entry) in effective_entries(journal)? {
        match entry.entry {
            Entry::UserMessage { text: message, attachments } => {
                let message = format!("User ({turn}): {message}\nAttachments: {attachments:?}\n");
                text.push_str(&message);
                input_bytes = input_bytes.saturating_add(message.len());
            }
            Entry::AgentMessage { text: message, call } => {
                let message = format!("Agent ({call:?}): {message}\n");
                text.push_str(&message);
                input_bytes = input_bytes.saturating_add(message.len());
            }
            Entry::Decision { approval } => {
                if let Some(bytes) = approvals.get((approval.0.get(), approval.1, approval.2))? {
                    frame(bytes.value());
                    text.push_str(&format!("Approval: {:?}\n", ApprovalRecord::decode(bytes.value())?));
                } else { return Ok(None); }
            }
            Entry::Notice { child, event, reference } => {
                text.push_str(&format!("Notice from {child}: {event:?}, reference {reference}\n"));
                for output in outputs.range((reference.get(), 0)..=(reference.get(), u32::MAX))? {
                    let (key, bytes) = output?;
                    let output = decode_output(key.value(), bytes.value())?;
                    if output.kind == OutputKind::Result {
                        frame(bytes.value());
                        text.push_str(&format!("Referenced result: {:?} {:?}\n", output.summary, output.value));
                    }
                }
            }
            Entry::RetryOf { turn: original, by } => text.push_str(&format!("Retry of {original}, requested by {by:?}; previous tool effects must not be repeated automatically.\n")),
            Entry::Iteration { n } => text.push_str(&format!("Iteration {n}\n")),
            Entry::UserMessageEdit { .. } | Entry::UserMessageWithdraw { .. } | Entry::ResponseFormat { .. } | Entry::ContextPreparation { .. } | Entry::ContextUse { .. } => {}
        }
    }
    let mut output_through = 0u32;
    if bounds.is_none_or(|b| b.1 > 0) {
        for entry in
            outputs.range((turn.get(), 0)..=(turn.get(), bounds.map_or(u32::MAX, |b| b.1)))?
        {
            let (key, bytes) = entry?;
            let output = decode_output(key.value(), bytes.value())?;
            if output.kind == OutputKind::Result {
                output_through = key.value().1;
                frame(bytes.value());
                answer_bytes = answer_bytes
                    .saturating_add(output.display_text().as_ref().map_or(0, String::len));
                text.push_str(&format!(
                    "Result: {:?} {:?}\n",
                    output.summary, output.value
                ));
            }
        }
    }
    for entry in tools.range((turn.get(), 0, 0)..=(turn.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        frame(bytes.value());
        let call = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
        text.push_str(&format!(
            "Tool {} at {:?}, arguments {:?}, outcome {:?}, reconciliation {:?}\n",
            call.name,
            call.call,
            call.admission()
                .map(|admission| admission.arguments.as_str()),
            call.outcome,
            call.reconciliation
        ));
    }
    if input_through == 0 || bounds.is_some_and(|b| b != (input_through, output_through)) {
        return Ok(None);
    }
    Ok(Some(SealedTurn {
        boundary: crate::db::entities::SourceBoundary {
            turn,
            input_through,
            output_through,
            content: ContentDigest::of(&canonical),
        },
        text,
        reducible_tokens: answer_bytes.div_ceil(3),
        context_tokens: input_bytes
            .saturating_add(answer_bytes)
            .saturating_add(16)
            .div_ceil(3),
    }))
}

pub(super) fn load_context_manifest(
    messages: &impl ReadableTable<&'static [u8], &'static [u8]>,
    digest: ContentDigest,
) -> Result<crate::db::entities::ContextSourceManifest, StorageError> {
    let bytes = messages
        .get(digest.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("sealed source manifest missing"))?;
    if ContentDigest::of(bytes.value()) != digest {
        return Err(StorageError::corrupted("source manifest digest mismatch"));
    }
    let manifest: crate::db::entities::ContextSourceManifest =
        serde_json::from_slice(bytes.value())
            .map_err(|_| StorageError::corrupted("invalid source manifest"))?;
    manifest.validate().map_err(StorageError::corrupted)?;
    Ok(manifest)
}

pub(super) fn source_is_prefix(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    consumer: &JobSpec,
    manifest: &crate::db::entities::ContextSourceManifest,
) -> Result<bool, StorageError> {
    if consumer.parent_id != Some(manifest.conversation) {
        return Ok(false);
    }
    source_is_prefix_from(
        jobs,
        manifest.conversation,
        consumer.kind.previous(),
        manifest,
    )
}

fn source_is_prefix_from(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    conversation: JobId,
    mut cursor: Option<JobId>,
    manifest: &crate::db::entities::ContextSourceManifest,
) -> Result<bool, StorageError> {
    let mut reverse = Vec::new();
    let mut seen = BTreeSet::new();
    while let Some(turn) = cursor {
        if !seen.insert(turn) || reverse.len() >= 4096 {
            return Ok(false);
        }
        let Some(bytes) = jobs.get(turn.get())? else {
            return Ok(false);
        };
        let row = JobSpec::decode_at(turn.get(), bytes.value())?;
        if row.parent_id != Some(conversation) {
            return Ok(false);
        }
        reverse.push(turn);
        cursor = row.kind.previous();
    }
    reverse.reverse();
    Ok(reverse.len() >= manifest.turns.len()
        && reverse
            .iter()
            .zip(&manifest.turns)
            .all(|(turn, boundary)| *turn == boundary.turn))
}

pub(crate) struct AdoptedContext {
    pub source: crate::db::entities::ContextSourceManifest,
    pub summary: String,
}

pub(super) fn global_compaction_settings(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<crate::db::entities::GlobalCompactionSettings, StorageError> {
    let bytes = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("global compaction settings missing"))?;
    let mut d = Dec::new(bytes.value(), TAG_GLOBAL_COMPACTION)?;
    let settings = crate::db::entities::GlobalCompactionSettings {
        revision: d.u64()?,
        prediction_enabled: d.bool()?,
        policy: d.compaction_policy()?,
    };
    d.end()?;
    Ok(settings)
}

pub(crate) fn read_global_compaction_settings(
    database: &Database,
) -> Result<crate::db::entities::GlobalCompactionSettings, StorageError> {
    global_compaction_settings(&read(database)?.open_table(META)?)
}

pub(super) fn effective_context_policy(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<
    (
        crate::db::entities::CompactionPolicy,
        u64,
        Option<ConfigRef>,
    ),
    StorageError,
> {
    let mut settings = global_compaction_settings(meta)?;
    if !settings.prediction_enabled
        || matches!(
            settings.policy.mode,
            crate::db::entities::CompactionMode::Off
        )
    {
        settings.policy.predictive = None;
    }
    Ok((settings.policy, settings.revision, None))
}

pub(super) fn raw_context_usage(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    states: &impl ReadableTable<u128, &'static [u8]>,
    inputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    outputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    start: JobId,
) -> Result<(usize, u32, u32), StorageError> {
    let mut cursor = Some(start);
    let mut seen = BTreeSet::new();
    let mut total = 0usize;
    let mut growth = 0usize;
    let mut settled = 0u32;
    while let Some(turn) = cursor {
        if !seen.insert(turn) {
            return Err(StorageError::corrupted("context ancestry cycle"));
        }
        if seen.len() > 4096 {
            return Ok((usize::MAX, u32::try_from(growth).unwrap_or(u32::MAX), 4096));
        }
        let mut bytes = 16usize;
        for (_, entry) in effective_entries(job_entries_of(inputs, turn)?)? {
            match entry.entry {
                Entry::UserMessage { text, attachments } => {
                    bytes = bytes
                        .saturating_add(text.len())
                        .saturating_add(attachments.len().saturating_mul(4800));
                }
                Entry::AgentMessage { text, .. } => bytes = bytes.saturating_add(text.len()),
                Entry::RetryOf { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::Iteration { .. } => bytes = bytes.saturating_add(256),
                Entry::ResponseFormat { .. }
                | Entry::UserMessageEdit { .. }
                | Entry::UserMessageWithdraw { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. } => {}
            }
        }
        for output in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
            let (key, bytes_row) = output?;
            let output = decode_output(key.value(), bytes_row.value())?;
            if output.kind == OutputKind::Result {
                bytes = bytes.saturating_add(output.display_text().as_ref().map_or(0, String::len));
            }
        }
        let tokens = bytes.div_ceil(3);
        total = total.saturating_add(tokens);
        let state = states
            .get(turn.get())?
            .ok_or_else(|| StorageError::corrupted("context turn state missing"))?;
        if JobState::decode_at(turn.get(), state.value())?.locally_stopped() {
            settled = settled.saturating_add(1);
            if settled <= 8 {
                growth = growth.max(tokens);
            }
        }
        let row = jobs
            .get(turn.get())?
            .ok_or_else(|| StorageError::corrupted("context turn missing"))?;
        cursor = JobSpec::decode_at(turn.get(), row.value())?.kind.previous();
    }
    Ok((total, u32::try_from(growth).unwrap_or(u32::MAX), settled))
}

pub(crate) fn read_effective_context_policy(
    database: &Database,
    conversation: JobId,
) -> Result<
    Option<(
        crate::db::entities::CompactionPolicy,
        u64,
        Option<ConfigRef>,
    )>,
    StorageError,
> {
    let rx = read(database)?;
    if read_job_row(&rx, conversation)?.is_none_or(|row| !row.kind.is_group()) {
        return Ok(None);
    }
    effective_context_policy(&rx.open_table(META)?).map(Some)
}

pub(crate) struct ContextWorkState {
    pub job: JobId,
    pub conversation: JobId,
    pub state: &'static str,
    pub details: ContextWorkDetails,
}
pub(crate) enum ContextWorkDetails {
    Summary {
        summarizer: ConfigId,
        failure: Option<String>,
        automatic: bool,
        stage: &'static str,
        chain: Option<JobId>,
        prediction: Option<JobId>,
        reserved_tokens: u32,
        reserved_requests: u32,
        source: ContentDigest,
        first: crate::db::entities::SourceBoundary,
        through: crate::db::entities::SourceBoundary,
        turn_count: usize,
    },
    Prediction {
        predictor: ConfigId,
        model: ConfigId,
        binding: crate::db::entities::PredictionBinding,
        forecast_tokens: Option<u64>,
        result: Option<Box<crate::db::entities::DecisionResult>>,
    },
}

pub(super) fn steer_blocker(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    states: &impl ReadableTable<u128, &'static [u8]>,
    children: &impl ReadableTable<(u128, u128), ()>,
    tips: &impl ReadableTable<u128, u128>,
    conversation: JobId,
) -> Result<Option<&'static str>, StorageError> {
    for child in children.range((conversation.get(), 0)..=(conversation.get(), u128::MAX))? {
        let child = child?.0.value().1;
        let Some(tip) = tips.get(child)?.map(|value| value.value()) else {
            continue;
        };
        let row = jobs
            .get(tip)?
            .ok_or_else(|| StorageError::corrupted("live context job missing"))?;
        let row = JobSpec::decode_at(tip, row.value())?;
        if let JobKind::Execution {
            execution: JobExecution::Summarization(work),
            ..
        } = row.kind
        {
            let state = states
                .get(tip)?
                .ok_or_else(|| StorageError::corrupted("live context state missing"))?;
            if JobState::decode_at(tip, state.value())?.locally_stopped() {
                continue;
            }
            let root = summary_root(jobs, &work)?;
            if root.prediction.is_none() {
                return Ok(Some(if root.policy_revision.is_some() {
                    "rule_based"
                } else {
                    "manual"
                }));
            }
        }
    }
    Ok(None)
}

pub(crate) struct ContextState {
    pub policy: crate::db::entities::CompactionPolicy,
    pub stored_override: Option<crate::db::entities::CompactionPolicy>,
    pub policy_source: &'static str,
    pub revision: u64,
    pub model: Option<FrozenLlmConfig>,
    pub estimated_tokens: Option<u64>,
    pub growth: u32,
    pub predictor: Option<ConfigId>,
    pub predictor_model: Option<ConfigId>,
    pub prediction_unavailable: Option<String>,
    pub last_use: Option<(JobId, u32, Option<JobId>)>,
    pub steer_blocked_by: Option<&'static str>,
    pub work: Vec<ContextWorkState>,
    pub next_before: Option<JobId>,
}

pub(crate) fn read_context_state(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<ContextState>, StorageError> {
    let rx = read(database)?;
    let Some(owner) = read_job_row(&rx, conversation)? else {
        return Ok(None);
    };
    if !owner.kind.is_group() {
        return Ok(None);
    }
    let (mut policy, revision, profile) = effective_context_policy(&rx.open_table(META)?)?;
    let stored_override = None;
    let tip = rx
        .open_table(GROUP_HEADS)?
        .get(conversation.get())?
        .map(|value| JobId::new(value.value()));
    let active = tip
        .map(|tip| read_job_row(&rx, tip))
        .transpose()?
        .flatten()
        .map(|row| turn_config(&rx, &row))
        .transpose()?
        .flatten();
    let model = active.as_ref().map(|frozen| frozen.llm.clone());
    let (raw, growth, _) = tip
        .map(|tip| {
            raw_context_usage(
                &rx.open_table(JOBS)?,
                &rx.open_table(JOB_STATES)?,
                &rx.open_table(JOB_INPUTS)?,
                &rx.open_table(JOB_OUTPUTS)?,
                tip,
            )
        })
        .transpose()?
        .unwrap_or_default();
    let mut tokens = raw;
    let mut last_use = None;
    let mut cursor = tip;
    for _ in 0..32 {
        let Some(turn) = cursor else {
            break;
        };
        for entry in rx
            .open_table(JOB_INPUTS)?
            .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
            .rev()
            .take(128)
        {
            let (key, bytes) = entry?;
            if let Entry::ContextUse { artifact } = JobInputEntry::decode(bytes.value())?.entry {
                last_use = Some((turn, key.value().1, artifact));
                break;
            }
        }
        if last_use.is_some() {
            break;
        }
        cursor = read_job_row(&rx, turn)?.and_then(|row| row.kind.previous());
    }
    if let Some(tip) = tip
        && let Some(adopted) = adopted_context(&rx, tip)?
    {
        for boundary in &adopted.source.turns {
            if let Some(turn) = source_turn(
                &rx.open_table(JOBS)?,
                &rx.open_table(JOB_STATES)?,
                &rx.open_table(JOB_INPUTS)?,
                &rx.open_table(JOB_OUTPUTS)?,
                &rx.open_table(TOOL_CALLS)?,
                &rx.open_table(APPROVALS)?,
                boundary.turn,
                Some((boundary.input_through, boundary.output_through)),
            )? {
                tokens = tokens.saturating_sub(turn.reducible_tokens);
            }
        }
        tokens = tokens.saturating_add(adopted.summary.len().saturating_add(128).div_ceil(3));
    }
    let settings = global_compaction_settings(&rx.open_table(META)?)?;
    let project = match owner.kind {
        JobKind::Group { project, .. } => project,
        JobKind::Execution { .. } => None,
    };
    let decision = project
        .map(|id| {
            rx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|bytes| decode_project(id.get(), bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("context project missing"))
        })
        .transpose()?
        .map(|project| project.decision_policy())
        .unwrap_or_default();
    let mut predictor = None;
    let mut predictor_model = None;
    let previous_binding = policy.predictive.take();
    let prediction_unavailable = if !settings.prediction_enabled {
        Some("prediction is disabled in global settings".to_owned())
    } else if matches!(policy.mode, crate::db::entities::CompactionMode::Off) {
        Some("automatic compaction is disabled".to_owned())
    } else if !decision.enabled {
        Some("prediction is disabled by the project decision configuration".to_owned())
    } else if let Some(active) = &active {
        let resolved = crate::service::jobs::ConfigurationResolver::new(|id| {
            read_config_record(&rx, id).map_err(|error| BatchError(Failure::Storage(error)))
        })
        .project_decision(&decision, active);
        match resolved {
            Ok(Some(frozen)) => {
                predictor = Some(frozen.profile_ref().id);
                predictor_model = Some(frozen.model_ref().id);
                policy.predictive = previous_binding;
                crate::service::jobs::bind_prediction(&mut policy, &frozen);
                if !decision_executor_available(&rx.open_table(CONFIGS)?, &frozen)? {
                    Some("no enabled Executor offers the project decision model".to_owned())
                } else if active.llm.config.context_window.is_none() {
                    Some("foreground model capacity is unknown".to_owned())
                } else {
                    None
                }
            }
            Ok(None) => {
                Some("prediction is disabled by the project decision configuration".to_owned())
            }
            Err(BatchError(Failure::Rejected(violation))) => Some(violation.to_string()),
            Err(BatchError(Failure::Storage(error))) => return Err(error),
        }
    } else {
        Some("foreground model is unavailable".to_owned())
    };
    let limit = limit.clamp(1, 100);
    let mut work = Vec::new();
    let mut next_before = None;
    let mut verify_remaining = 4096usize;
    for (scanned, entry) in rx
        .open_table(CHILD_GROUPS)?
        .range((conversation.get(), 0)..(conversation.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .enumerate()
    {
        let child = JobId::new(entry?.0.value().1);
        if scanned >= limit.saturating_mul(4) || work.len() >= limit {
            break;
        }
        next_before = Some(child);
        let Some(job) = rx
            .open_table(GROUP_HEADS)?
            .get(child.get())?
            .map(|value| JobId::new(value.value()))
        else {
            continue;
        };
        let Some(row) = read_job_row(&rx, job)? else {
            continue;
        };
        if let JobKind::Execution {
            execution: JobExecution::Decision(decision),
            ..
        } = &row.kind
            && let Some(binding) = &decision.prediction
        {
            let state = read_state(&rx, job)?;
            let current = prediction_current(
                &rx.open_table(META)?,
                &rx.open_table(JOBS)?,
                &rx.open_table(CONFIGS)?,
                &rx.open_table(PROJECTS)?,
                &rx.open_table(JOB_CONTROLS)?,
                &rx.open_table(GROUP_HEADS)?,
                &row,
            )?;
            let label = match state.outcome {
                None if !current => "stale",
                None if state.claim.is_some() => "predicting",
                None => "queued",
                Some(outcome) => match outcome.kind {
                    OutcomeKind::Completed => "recorded",
                    OutcomeKind::Failed => "failed",
                    OutcomeKind::Cancelled => "cancelled",
                    OutcomeKind::Unknown => "unknown",
                },
            };
            let result = rx
                .open_table(JOB_OUTPUTS)?
                .range((job.get(), 0)..=(job.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
                .transpose()?
                .and_then(|output| output.value)
                .and_then(|value| value.get("decision").cloned())
                .and_then(|value| serde_json::from_value(value).ok());
            work.push(ContextWorkState {
                job,
                conversation: child,
                state: label,
                details: ContextWorkDetails::Prediction {
                    predictor: decision.frozen.profile_ref().id,
                    model: decision.frozen.model_ref().id,
                    binding: binding.clone(),
                    forecast_tokens: decision
                        .request
                        .state
                        .get("deterministic_forecast_tokens")
                        .and_then(serde_json::Value::as_u64),
                    result,
                },
            });
            continue;
        }
        let JobKind::Execution {
            execution: JobExecution::Summarization(summary_work),
            ..
        } = row.kind
        else {
            continue;
        };
        let manifest = load_context_manifest(&rx.open_table(MESSAGES)?, summary_work.source)?;
        let first = manifest
            .turns
            .first()
            .cloned()
            .ok_or_else(|| StorageError::corrupted("empty context source"))?;
        let through = manifest
            .turns
            .last()
            .cloned()
            .ok_or_else(|| StorageError::corrupted("empty context source"))?;
        let state = read_state(&rx, job)?;
        let binding = summary_root(&rx.open_table(JOBS)?, &summary_work)?;
        let failed = state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Failed);
        let label = if context_work_stopped(&rx.open_table(NOTICES)?, child)?
            || !preparation_current(
                &rx.open_table(META)?,
                &rx.open_table(JOBS)?,
                &rx.open_table(CONFIGS)?,
                &rx.open_table(PROJECTS)?,
                &rx.open_table(JOB_CONTROLS)?,
                &rx.open_table(NOTICES)?,
                &read_job_row(&rx, job)?
                    .ok_or_else(|| StorageError::corrupted("summary missing"))?,
            )?
            || binding
                .policy_revision
                .is_some_and(|bound| bound != revision || binding.policy_profile != profile)
        {
            "discarded"
        } else {
            match state.outcome {
                None if state.claim.is_some() => "preparing",
                None => "queued",
                Some(outcome) => match outcome.kind {
                    OutcomeKind::Failed => "failed",
                    OutcomeKind::Cancelled => "cancelled",
                    OutcomeKind::Unknown => "unknown",
                    OutcomeKind::Completed if !summary_work.is_artifact() => "part_ready",
                    OutcomeKind::Completed => {
                        if last_use.is_some_and(|(_, _, artifact)| artifact == Some(job)) {
                            "adopted"
                        } else {
                            "prepared"
                        }
                    }
                },
            }
        };
        let mut label = label;
        if matches!(label, "prepared" | "adopted") {
            let prefix_valid =
                source_is_prefix_from(&rx.open_table(JOBS)?, conversation, tip, &manifest)?;
            if !prefix_valid {
                label = "incompatible";
            } else if manifest.turns.len() > verify_remaining {
                label = "unverified";
            } else {
                verify_remaining -= manifest.turns.len();
                let output = rx
                    .open_table(JOB_OUTPUTS)?
                    .range((job.get(), 0)..=(job.get(), u32::MAX))?
                    .next_back()
                    .transpose()?
                    .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
                    .transpose()?;
                let summary_tokens = output
                    .filter(|output| output.kind == OutputKind::Result)
                    .and_then(|output| output.value)
                    .and_then(|value| {
                        serde_json::from_value::<crate::db::entities::SummarizationResult>(value)
                            .ok()
                    })
                    .filter(|result| {
                        result.validate().is_ok() && result.source == summary_work.source
                    })
                    .map(|result| result.context_text().len().saturating_add(128).div_ceil(3));
                if summary_tokens.is_none_or(|tokens| tokens > summary_work.target_tokens as usize)
                {
                    label = "incompatible";
                }
                let mut reducible = 0usize;
                for boundary in &manifest.turns {
                    if label == "incompatible" {
                        break;
                    }
                    let source = source_turn(
                        &rx.open_table(JOBS)?,
                        &rx.open_table(JOB_STATES)?,
                        &rx.open_table(JOB_INPUTS)?,
                        &rx.open_table(JOB_OUTPUTS)?,
                        &rx.open_table(TOOL_CALLS)?,
                        &rx.open_table(APPROVALS)?,
                        boundary.turn,
                        Some((boundary.input_through, boundary.output_through)),
                    )?;
                    match source {
                        Some(source) if source.boundary == *boundary => {
                            reducible = reducible.saturating_add(source.reducible_tokens)
                        }
                        Some(_) | None => label = "incompatible",
                    }
                }
                if summary_tokens.is_some_and(|tokens| {
                    reducible.saturating_sub(tokens) < summary_work.minimum_savings_tokens as usize
                }) {
                    label = "incompatible";
                }
            }
        }
        work.push(ContextWorkState {
            job,
            conversation: child,
            state: label,
            details: ContextWorkDetails::Summary {
                summarizer: summary_work.frozen.llm.model.id,
                failure: if failed {
                    rx.open_table(JOB_OUTPUTS)?
                        .range((job.get(), 0)..=(job.get(), u32::MAX))?
                        .next_back()
                        .transpose()?
                        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
                        .transpose()?
                        .filter(|output| output.kind == OutputKind::Result)
                        .and_then(|output| output.summary)
                } else {
                    None
                },
                automatic: binding.policy_revision.is_some(),
                stage: match summary_work.stage {
                    crate::db::entities::SummarizationStage::Direct => "direct",
                    crate::db::entities::SummarizationStage::Chunk { .. } => "chunk",
                    crate::db::entities::SummarizationStage::Reduce => "reduce",
                },
                chain: summary_work.chain,
                prediction: binding.prediction,
                reserved_tokens: binding.reserved_tokens,
                reserved_requests: binding.reserved_requests,
                source: summary_work.source,
                first,
                through,
                turn_count: manifest.turns.len(),
            },
        });
    }
    if let Some(cursor) = next_before
        && rx
            .open_table(CHILD_GROUPS)?
            .range((conversation.get(), 0)..(conversation.get(), cursor.get()))?
            .next()
            .is_none()
    {
        next_before = None;
    }
    let steer_blocked_by = steer_blocker(
        &rx.open_table(JOBS)?,
        &rx.open_table(JOB_STATES)?,
        &rx.open_table(LIVE_CHILDREN)?,
        &rx.open_table(GROUP_HEADS)?,
        conversation,
    )?;
    Ok(Some(ContextState {
        steer_blocked_by,
        policy,
        stored_override: stored_override.clone(),
        policy_source: "global",
        revision,
        model,
        estimated_tokens: (raw != usize::MAX).then_some(u64::try_from(tokens).unwrap_or(u64::MAX)),
        growth,
        predictor,
        predictor_model,
        prediction_unavailable,
        last_use,
        work,
        next_before,
    }))
}

fn read_config_record(
    rx: &ReadTransaction,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()).map(config_record))
        .transpose()
}

pub(super) fn decision_executor_available(
    configs: &impl ReadableTable<u128, &'static [u8]>,
    frozen: &crate::db::entities::FrozenDecisionConfig,
) -> Result<bool, StorageError> {
    let (operation, protocol) = match frozen {
        crate::db::entities::FrozenDecisionConfig::Native(_) => {
            (ExecutionOperation::Decision, None)
        }
        crate::db::entities::FrozenDecisionConfig::Generative(active, _) => (
            ExecutionOperation::LlmRequest,
            Some(active.llm.provider.transport),
        ),
    };
    for entry in configs.iter()? {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if row.archived_at.is_none()
            && let ConfigBody::Pool(pool) = row.body
            && pool.enabled
            && pool.offers(operation, protocol)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn same_compaction_policy(
    left: &crate::db::entities::CompactionPolicy,
    right: &crate::db::entities::CompactionPolicy,
) -> bool {
    left.mode == right.mode && left.summarization_model == right.summarization_model
}

fn decision_assignment_current(
    projects: &impl ReadableTable<u128, &'static [u8]>,
    configs: &impl ReadableTable<u128, &'static [u8]>,
    work: &crate::db::entities::DecisionWork,
) -> Result<bool, StorageError> {
    if let Some(assignment) = &work.assignment {
        let Some(bytes) = projects.get(assignment.project.get())? else {
            return Ok(false);
        };
        let project = decode_project(assignment.project.get(), bytes.value())?;
        if !project.decision_policy().enabled || project.decision_revision != assignment.revision {
            return Ok(false);
        }
    }
    for reference in work.frozen.references() {
        let Some(bytes) = configs.get(reference.id.get())? else {
            return Ok(false);
        };
        let config = ConfigRow::decode_at(reference.id.get(), bytes.value())?;
        if config.archived_at.is_some() || config.edit_seq != reference.edit_seq {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn prediction_current(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    configs: &impl ReadableTable<u128, &'static [u8]>,
    projects: &impl ReadableTable<u128, &'static [u8]>,
    conversations: &impl ReadableTable<u128, &'static [u8]>,
    tips: &impl ReadableTable<u128, u128>,
    row: &JobSpec,
) -> Result<bool, StorageError> {
    let JobKind::Execution {
        execution: JobExecution::Decision(work),
        ..
    } = &row.kind
    else {
        return Ok(true);
    };
    if !decision_assignment_current(projects, configs, work)? {
        return Ok(false);
    }
    let Some(prediction) = &work.prediction else {
        return Ok(true);
    };
    let Some(parent) = row.parent_id else {
        return Ok(false);
    };
    let Some(bytes) = jobs.get(parent.get())? else {
        return Ok(false);
    };
    let parent = JobSpec::decode_at(parent.get(), bytes.value())?;
    if !matches!(
        parent.kind,
        JobKind::Group {
            origin: Origin::System,
            ..
        }
    ) {
        return Ok(false);
    }
    let Some(conversation) = parent.parent_id else {
        return Ok(false);
    };
    if prediction.conversation != conversation {
        return Ok(false);
    }
    let (policy, revision, profile) = effective_context_policy(meta)?;
    if !global_compaction_settings(meta)?.prediction_enabled
        || !same_compaction_policy(&policy, &prediction.policy)
    {
        return Ok(false);
    }
    let Some(binding) = &prediction.policy.predictive else {
        return Ok(false);
    };
    if binding.decision_agent != work.frozen.profile_ref().id
        || prediction.policy_revision != revision
        || prediction.policy_profile != profile
        || prediction.max_tokens != binding.max_prediction_tokens
    {
        return Ok(false);
    }
    if tips
        .get(conversation.get())?
        .map(|value| JobId::new(value.value()))
        != Some(prediction.activity_turn)
    {
        return Ok(false);
    }

    let metadata = conversations
        .get(conversation.get())?
        .ok_or_else(|| StorageError::corrupted("prediction owner missing"))?;
    Ok(JobControl::decode(metadata.value())?.closing.is_none())
}

pub(crate) fn read_prediction_current(
    database: &Database,
    job: JobId,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(false);
    };
    prediction_current(
        &rx.open_table(META)?,
        &rx.open_table(JOBS)?,
        &rx.open_table(CONFIGS)?,
        &rx.open_table(PROJECTS)?,
        &rx.open_table(JOB_CONTROLS)?,
        &rx.open_table(GROUP_HEADS)?,
        &row,
    )
}

pub(super) fn summary_root(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    work: &crate::db::entities::SummarizationWork,
) -> Result<crate::db::entities::SummarizationWork, StorageError> {
    let Some(anchor) = work.chain else {
        return Ok(work.clone());
    };
    let bytes = jobs
        .get(anchor.get())?
        .ok_or_else(|| StorageError::corrupted("summary chain anchor missing"))?;
    let row = JobSpec::decode_at(anchor.get(), bytes.value())?;
    let JobKind::Execution {
        execution: JobExecution::Summarization(root),
        ..
    } = row.kind
    else {
        return Err(StorageError::corrupted("invalid summary chain anchor"));
    };
    if root.chain.is_some()
        || root.source != work.source
        || !matches!(
            root.stage,
            crate::db::entities::SummarizationStage::Chunk { start: 0, .. }
        )
    {
        return Err(StorageError::corrupted("inconsistent summary chain"));
    }
    Ok(root)
}

pub(super) fn context_work_stopped(
    notices: &impl ReadableTable<(u128, u32), &'static [u8]>,
    conversation: JobId,
) -> Result<bool, StorageError> {
    for (scanned, entry) in notices
        .range((conversation.get(), 0)..=(conversation.get(), u32::MAX))?
        .rev()
        .enumerate()
        .take(129)
    {
        if scanned == 128 {
            return Ok(true);
        }
        if matches!(
            Notice::decode(entry?.1.value())?.body,
            NoticeBody::Record {
                event: RecordEvent::Stop,
                ..
            }
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Known usage only increases the reservation charge. Missing or unfinished
/// usage never refunds a reservation, including after a restart.
pub(super) fn preparation_charge(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    children: &impl ReadableTable<(u128, u128), ()>,
    tips: &impl ReadableTable<u128, u128>,
    events: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    row: &JobSpec,
) -> Result<(u64, Timestamp), StorageError> {
    let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = &row.kind
    else {
        return Ok((0, row.created_at));
    };
    let root = summary_root(jobs, work)?;
    if root.prediction.is_none() {
        return Ok((0, row.created_at));
    }
    let anchor = work.chain.unwrap_or(row.id);
    let Some(parent) = row.parent_id else {
        return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
    };
    let Some(bytes) = jobs.get(parent.get())? else {
        return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
    };
    let Some(owner) = JobSpec::decode_at(parent.get(), bytes.value())?.parent_id else {
        return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
    };
    let mut total = 0u64;
    let mut charged_at = row.created_at;
    for (scanned, entry) in children
        .range((owner.get(), 0)..=(owner.get(), u128::MAX))?
        .rev()
        .enumerate()
        .take(257)
    {
        if scanned == 256 {
            return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
        }
        let child = entry?.0.value().1;
        let Some(tip) = tips.get(child)?.map(|tip| tip.value()) else {
            continue;
        };
        if tip < anchor.get() {
            break;
        }
        let Some(bytes) = jobs.get(tip)? else {
            return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
        };
        let member = JobSpec::decode_at(tip, bytes.value())?;
        let JobKind::Execution {
            execution: JobExecution::Summarization(part),
            ..
        } = &member.kind
        else {
            continue;
        };
        if part.source != root.source || part.chain.unwrap_or(member.id) != anchor {
            continue;
        }
        let mut evidence = Vec::new();
        for (scanned, entry) in events
            .range((tip, 1, 0)..=(tip, u32::MAX, u32::MAX))?
            .enumerate()
            .take(129)
        {
            if scanned == 128 {
                return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
            }
            let (key, bytes) = entry?;
            let (_, request, seq) = key.value();
            if request != 1 {
                return Ok((u64::MAX, Timestamp::from_unix_micros(i64::MAX)));
            }
            evidence.push(rows::decode_request_event(
                member.id,
                RequestNumber::FIRST,
                seq,
                bytes.value(),
            )?);
        }
        for event in &evidence {
            charged_at = charged_at.max(event.recorded_at);
        }
        if let Some(sample) = observations::usage_sample_of(
            member.id,
            RequestNumber::FIRST,
            &turn_target_of(jobs, &member)?,
            &evidence,
        ) {
            total = total
                .saturating_add(u64::from(sample.input.value().unwrap_or(0)))
                .saturating_add(u64::from(sample.output.value().unwrap_or(0)));
        }
    }
    Ok((total.max(u64::from(root.reserved_tokens)), charged_at))
}

pub(crate) fn read_preparation_current(
    database: &Database,
    job: JobId,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(false);
    };
    if read_state(&rx, job)?.outcome.is_some() {
        return Ok(true);
    }
    if !preparation_current(
        &rx.open_table(META)?,
        &rx.open_table(JOBS)?,
        &rx.open_table(CONFIGS)?,
        &rx.open_table(PROJECTS)?,
        &rx.open_table(JOB_CONTROLS)?,
        &rx.open_table(NOTICES)?,
        &row,
    )? {
        return Ok(false);
    }
    if let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = &row.kind
    {
        let root = summary_root(&rx.open_table(JOBS)?, work)?;
        if root.prediction.is_some() {
            let jobs = rx.open_table(JOBS)?;
            for (scanned, entry) in rx.open_table(PENDING)?.iter()?.enumerate().take(129) {
                if scanned == 128 {
                    return Ok(false);
                }
                let id = entry?.0.value();
                let Some(bytes) = jobs.get(id)? else {
                    return Ok(false);
                };
                if work_priority(&jobs, &JobSpec::decode_at(id, bytes.value())?)? == 0 {
                    return Ok(false);
                }
            }
        }
        if root.prediction.is_some()
            && preparation_charge(
                &rx.open_table(JOBS)?,
                &rx.open_table(CHILD_GROUPS)?,
                &rx.open_table(GROUP_HEADS)?,
                &rx.open_table(REQUEST_EVENTS)?,
                &row,
            )?
            .0 > u64::from(root.reserved_tokens)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn preparation_current(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    configs: &impl ReadableTable<u128, &'static [u8]>,
    projects: &impl ReadableTable<u128, &'static [u8]>,
    conversations: &impl ReadableTable<u128, &'static [u8]>,
    notices: &impl ReadableTable<(u128, u32), &'static [u8]>,
    row: &JobSpec,
) -> Result<bool, StorageError> {
    let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = &row.kind
    else {
        return Ok(true);
    };
    let Some(parent) = row.parent_id else {
        return Ok(false);
    };
    if context_work_stopped(notices, parent)? {
        return Ok(false);
    }
    if let Some(anchor) = work.chain {
        let Some(bytes) = jobs.get(anchor.get())? else {
            return Ok(false);
        };
        let Some(anchor_parent) = JobSpec::decode_at(anchor.get(), bytes.value())?.parent_id else {
            return Ok(false);
        };
        if context_work_stopped(notices, anchor_parent)? {
            return Ok(false);
        }
    }
    let Some(bytes) = jobs.get(parent.get())? else {
        return Ok(false);
    };
    let owner = JobSpec::decode_at(parent.get(), bytes.value())?;
    let Some(conversation) = owner.parent_id else {
        return Ok(false);
    };
    let Some(metadata) = conversations.get(conversation.get())? else {
        return Ok(false);
    };
    if JobControl::decode(metadata.value())?.closing.is_some() {
        return Ok(false);
    }
    summary_current(meta, jobs, configs, projects, work, conversation)
}

pub(super) fn summary_current(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    configs: &impl ReadableTable<u128, &'static [u8]>,
    projects: &impl ReadableTable<u128, &'static [u8]>,
    work: &crate::db::entities::SummarizationWork,
    conversation: JobId,
) -> Result<bool, StorageError> {
    let root = summary_root(jobs, work)?;
    let (policy, revision, profile) = effective_context_policy(meta)?;
    if root.policy_revision.is_some_and(|bound| {
        bound != revision
            || root.policy_profile != profile
            || matches!(policy.mode, crate::db::entities::CompactionMode::Off)
    }) {
        return Ok(false);
    }
    let Some(prediction) = root.prediction else {
        return Ok(true);
    };
    let Some(bytes) = jobs.get(prediction.get())? else {
        return Ok(false);
    };
    let row = JobSpec::decode_at(prediction.get(), bytes.value())?;
    let JobKind::Execution {
        execution: JobExecution::Decision(decision),
        ..
    } = row.kind
    else {
        return Ok(false);
    };
    let Some(snapshot) = &decision.prediction else {
        return Ok(false);
    };
    if snapshot.conversation != conversation
        || !global_compaction_settings(meta)?.prediction_enabled
        || !same_compaction_policy(&snapshot.policy, &policy)
        || !decision_assignment_current(projects, configs, &decision)?
    {
        return Ok(false);
    }
    let Some(binding) = &snapshot.policy.predictive else {
        return Ok(false);
    };
    if binding.mode != crate::db::entities::PredictionMode::Prepare
        || binding.decision_agent != decision.frozen.profile_ref().id
        || binding
            .acceptance
            .as_ref()
            .is_none_or(|acceptance| acceptance.model != decision.frozen.model_ref())
    {
        return Ok(false);
    }

    Ok(true)
}

pub(super) fn work_priority(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<u8, StorageError> {
    Ok(match &row.kind {
        JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } if work.prediction.is_some() => 2,
        JobKind::Execution {
            execution: JobExecution::Summarization(work),
            ..
        } => {
            let root = summary_root(jobs, work)?;
            if root.prediction.is_some() {
                2
            } else if root.policy_revision.is_some() {
                1
            } else {
                0
            }
        }
        JobKind::Group { .. } | JobKind::Execution { .. } => 0,
    })
}

pub(crate) struct SummarizationInput {
    pub work: crate::db::entities::SummarizationWork,
    pub source: crate::db::entities::ContextSourceManifest,
    pub text: String,
    pub source_tokens: usize,
    pub current: bool,
    pub speculative: bool,
    pub automatic: bool,
}

pub(crate) fn read_summarization_input(
    database: &Database,
    job: JobId,
) -> Result<Option<SummarizationInput>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    let eligible = preparation_current(
        &rx.open_table(META)?,
        &rx.open_table(JOBS)?,
        &rx.open_table(CONFIGS)?,
        &rx.open_table(PROJECTS)?,
        &rx.open_table(JOB_CONTROLS)?,
        &rx.open_table(NOTICES)?,
        &row,
    )?;
    let charge = preparation_charge(
        &rx.open_table(JOBS)?,
        &rx.open_table(CHILD_GROUPS)?,
        &rx.open_table(GROUP_HEADS)?,
        &rx.open_table(REQUEST_EVENTS)?,
        &row,
    )?;
    let dependencies = row.kind.dependencies().to_vec();
    let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = row.kind
    else {
        return Ok(None);
    };
    let source = load_context_manifest(&rx.open_table(MESSAGES)?, work.source)?;
    let root = summary_root(&rx.open_table(JOBS)?, &work)?;
    let mut current =
        eligible && (root.prediction.is_none() || charge.0 <= u64::from(root.reserved_tokens));
    let mut text = String::new();
    let mut reducible = 0usize;
    for boundary in &source.turns {
        let Some(sealed) = source_turn(
            &rx.open_table(JOBS)?,
            &rx.open_table(JOB_STATES)?,
            &rx.open_table(JOB_INPUTS)?,
            &rx.open_table(JOB_OUTPUTS)?,
            &rx.open_table(TOOL_CALLS)?,
            &rx.open_table(APPROVALS)?,
            boundary.turn,
            Some((boundary.input_through, boundary.output_through)),
        )?
        else {
            current = false;
            break;
        };
        if sealed.boundary != *boundary {
            current = false;
            break;
        }
        reducible = reducible.saturating_add(sealed.reducible_tokens);
        text.push_str(&sealed.text);
    }
    match work.stage {
        crate::db::entities::SummarizationStage::Direct => {}
        crate::db::entities::SummarizationStage::Chunk {
            start,
            end,
            content,
        } => {
            let range = usize::try_from(start).ok().zip(usize::try_from(end).ok());
            match range.and_then(|(start, end)| text.get(start..end)) {
                Some(part) if ContentDigest::of(part.as_bytes()) == content => {
                    text = part.to_owned()
                }
                Some(_) | None => current = false,
            }
        }
        crate::db::entities::SummarizationStage::Reduce => {
            let mut pieces = Vec::new();
            for (child, condition) in dependencies {
                if condition != DependencyCondition::Success {
                    current = false;
                    break;
                }
                let Some(part_job) = rx
                    .open_table(GROUP_HEADS)?
                    .get(child.get())?
                    .map(|value| JobId::new(value.value()))
                else {
                    current = false;
                    break;
                };
                let Some(row) = read_job_row(&rx, part_job)? else {
                    current = false;
                    break;
                };
                let JobKind::Execution {
                    execution: JobExecution::Summarization(part_work),
                    ..
                } = row.kind
                else {
                    current = false;
                    break;
                };
                let crate::db::entities::SummarizationStage::Chunk {
                    start,
                    end,
                    content,
                } = part_work.stage
                else {
                    current = false;
                    break;
                };
                if part_work.chain.unwrap_or(part_job) != work.chain.unwrap_or(job)
                    || part_work.source != work.source
                    || !read_state(&rx, part_job)?
                        .outcome
                        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
                {
                    current = false;
                    break;
                }
                let output = rx
                    .open_table(JOB_OUTPUTS)?
                    .range((part_job.get(), 0)..=(part_job.get(), u32::MAX))?
                    .next_back()
                    .transpose()?
                    .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
                    .transpose()?;
                let fragment = output
                    .filter(|output| output.kind == OutputKind::Result)
                    .and_then(|output| output.value)
                    .and_then(|value| value.get("summary_part").cloned())
                    .and_then(|value| {
                        serde_json::from_value::<crate::db::entities::SummaryFragment>(value).ok()
                    });
                let Some(fragment) = fragment.filter(|fragment| {
                    fragment.validate().is_ok()
                        && fragment
                            .context_text()
                            .len()
                            .saturating_add(128)
                            .div_ceil(3)
                            <= part_work.target_tokens as usize
                }) else {
                    current = false;
                    break;
                };
                if !usize::try_from(start)
                    .ok()
                    .zip(usize::try_from(end).ok())
                    .and_then(|(start, end)| text.get(start..end))
                    .is_some_and(|source| ContentDigest::of(source.as_bytes()) == content)
                {
                    current = false;
                    break;
                }
                pieces.push((start, end, fragment));
            }
            pieces.sort_by_key(|piece| piece.0);
            let mut covered = 0u64;
            let mut reduced = String::new();
            if pieces.is_empty() || pieces.len() > 7 {
                current = false;
            }
            for (start, end, fragment) in pieces {
                if start != covered || end <= start {
                    current = false;
                    break;
                }
                covered = end;
                reduced.push_str(&format!(
                    "Source bytes {start}..{end}:\n{}\n",
                    fragment.context_text()
                ));
            }
            if covered != text.len() as u64 {
                current = false;
            }
            text = reduced;
        }
    }
    let source_tokens = reducible;
    Ok(Some(SummarizationInput {
        work,
        source,
        text,
        source_tokens,
        current,
        speculative: root.prediction.is_some(),
        automatic: root.policy_revision.is_some(),
    }))
}

/// The latest candidate whose sealed prefix is still the stored bytes.
/// A failed check returns no summary; it does not fail the prompt read.
fn adopted_context(
    rx: &ReadTransaction,
    turn: JobId,
) -> Result<Option<AdoptedContext>, StorageError> {
    let row = read_job_row(rx, turn)?.ok_or_else(|| StorageError::corrupted("turn missing"))?;
    if !row.kind.is_persistent() {
        return Ok(None);
    }
    let mut selected = None;
    for (_, entry) in turn_entries(rx, turn)? {
        if let Entry::ContextUse { artifact } = entry.entry {
            selected = Some(artifact);
        }
    }
    let Some(artifact) = selected.flatten() else {
        return Ok(None);
    };
    let Some(producer) = read_job_row(rx, artifact)? else {
        return Ok(None);
    };
    let JobKind::Execution {
        execution: JobExecution::Summarization(work),
        ..
    } = &producer.kind
    else {
        return Ok(None);
    };
    if !work.is_artifact() {
        return Ok(None);
    }
    let Some(parent) = producer.parent_id else {
        return Ok(None);
    };
    let Some(owner) = read_job_row(rx, parent)? else {
        return Ok(None);
    };
    if owner.parent_id != row.parent_id {
        return Ok(None);
    }
    let state = read_state(rx, artifact)?;
    if !state
        .outcome
        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
    {
        return Ok(None);
    }
    let result = rx
        .open_table(JOB_OUTPUTS)?
        .range((artifact.get(), 0)..=(artifact.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()?
        .filter(|output| output.kind == OutputKind::Result)
        .and_then(|output| output.value)
        .and_then(|value| serde_json::from_value(value).ok());
    let Some(result) = result else {
        return Ok(None);
    };
    let result: crate::db::entities::SummarizationResult = result;
    if result.source != work.source {
        return Ok(None);
    }
    let manifest = load_context_manifest(&rx.open_table(MESSAGES)?, result.source)?;
    if result.validate().is_err() || !source_is_prefix(&rx.open_table(JOBS)?, &row, &manifest)? {
        return Ok(None);
    }
    let mut reducible = 0usize;
    for boundary in &manifest.turns {
        let Some(source) = source_turn(
            &rx.open_table(JOBS)?,
            &rx.open_table(JOB_STATES)?,
            &rx.open_table(JOB_INPUTS)?,
            &rx.open_table(JOB_OUTPUTS)?,
            &rx.open_table(TOOL_CALLS)?,
            &rx.open_table(APPROVALS)?,
            boundary.turn,
            Some((boundary.input_through, boundary.output_through)),
        )?
        else {
            return Ok(None);
        };
        if source.boundary != *boundary {
            return Ok(None);
        }
        reducible = reducible.saturating_add(source.reducible_tokens);
    }
    let tokens = result.context_text().len().saturating_add(128).div_ceil(3);
    if tokens > work.target_tokens as usize
        || reducible.saturating_sub(tokens) < work.minimum_savings_tokens as usize
    {
        return Ok(None);
    }
    Ok(Some(AdoptedContext {
        source: manifest,
        summary: result.context_text(),
    }))
}
pub(crate) fn read_turn_context(
    database: &Database,
    turn: JobId,
    budget_tokens: usize,
) -> Result<TurnContext, StorageError> {
    let rx = read(database)?;
    let row = read_job_row(&rx, turn)?.ok_or_else(|| StorageError::corrupted("turn missing"))?;
    let mut context = TurnContext {
        turns: Vec::new(),
        omitted: false,
        attention: String::new(),
        tree_status: String::new(),
        response_format: None,
        adopted_summary: None,
        covered_turns: Vec::new(),
    };
    if let Some(adopted) = adopted_context(&rx, turn)? {
        context.adopted_summary = Some(adopted.summary);
        context.covered_turns = adopted
            .source
            .turns
            .into_iter()
            .map(|source| source.turn)
            .collect();
    }
    let own_entries = turn_entries(&rx, turn)?;
    context.response_format = own_entries
        .iter()
        .rev()
        .find_map(|(_, entry)| match &entry.entry {
            Entry::ResponseFormat { format } => Some(format.clone()),
            _ => None,
        });
    if context.response_format.is_none() {
        let mut root = None;
        let mut next = own_entries.iter().find_map(|(_, entry)| match entry.entry {
            Entry::RetryOf { turn, .. } => Some(turn),
            _ => None,
        });
        let mut visited = BTreeSet::from([turn]);
        while let Some(current) = next {
            if !visited.insert(current) {
                return Err(StorageError::corrupted("retry ancestry cycle"));
            }
            let entries = turn_entries(&rx, current)?;
            next = entries.iter().find_map(|(_, entry)| match entry.entry {
                Entry::RetryOf { turn, .. } => Some(turn),
                _ => None,
            });
            root = Some(entries);
        }
        context.response_format =
            root.into_iter()
                .flatten()
                .rev()
                .find_map(|(_, entry)| match entry.entry {
                    Entry::ResponseFormat { format } => Some(format),
                    _ => None,
                });
    }
    let iteration_only = own_entries
        .iter()
        .any(|(_, entry)| matches!(entry.entry, Entry::Iteration { .. }))
        && !own_entries
            .iter()
            .any(|(_, entry)| matches!(entry.entry, Entry::UserMessage { .. }));
    let iteration_context = if iteration_only && let Some(conversation) = row.parent_id {
        let recurrence = rx
            .open_table(RECURRENCES)?
            .get(conversation.get())?
            .map(|bytes| Recurrence::decode(bytes.value()))
            .transpose()?;
        let first = rx
            .open_table(CHILDREN)?
            .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
            .next()
            .transpose()?
            .map(|(key, _)| JobId::new(key.value().1));
        recurrence
            .zip(first)
            .map(|(recurrence, first)| (first, recurrence.previous_turn))
    } else {
        None
    };
    let mut cursor = Some(turn);
    let mut used = context
        .adopted_summary
        .as_ref()
        .map_or(0, |summary| summary.len().saturating_add(128).div_ceil(3));
    let mut visited = BTreeSet::new();
    while let Some(id) = cursor {
        if !visited.insert(id) {
            return Err(StorageError::corrupted("context ancestry cycle"));
        }
        let row = read_job_row(&rx, id)?
            .ok_or_else(|| StorageError::corrupted("previous turn missing"))?;
        let covered = id != turn && context.covered_turns.contains(&id);
        let mut text = String::new();
        let mut attachments = Vec::new();
        for (_, entry) in turn_entries(&rx, id)?.into_iter().filter(|_| {
            iteration_context
                .is_none_or(|(first, previous)| Some(id) != previous || id == first || id == turn)
        }) {
            if matches!(
                entry.entry,
                Entry::ResponseFormat { .. }
                    | Entry::UserMessageEdit { .. }
                    | Entry::UserMessageWithdraw { .. }
                    | Entry::ContextPreparation { .. }
                    | Entry::ContextUse { .. }
            ) {
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&entry_text(&rx, &entry.entry)?);
            if let Entry::UserMessage {
                attachments: images,
                ..
            } = entry.entry
            {
                attachments.extend(images);
            }
        }
        let state = read_state(&rx, id)?;
        let answer = if covered
            || iteration_context
                .is_some_and(|(first, previous)| id == first && previous != Some(id))
            || state
                .outcome
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Cancelled)
        {
            None
        } else {
            rx.open_table(JOB_OUTPUTS)?
                .range((id.get(), 0)..=(id.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(key, value)| decode_output(key.value(), value.value()))
                .transpose()?
                .filter(|output| output.kind == OutputKind::Result)
                .and_then(|output| output.display_text())
        };
        cursor = match iteration_context {
            Some((first, previous)) if id == turn => previous
                .filter(|previous| *previous != turn)
                .or((first != turn).then_some(first)),
            Some((first, _)) if id != first => Some(first),
            Some(_) => None,
            None => row.kind.previous(),
        };
        if id != turn && text.is_empty() && attachments.is_empty() && answer.is_none() {
            continue;
        }
        let weight = (text.len() + answer.as_ref().map_or(0, String::len) + 16).div_ceil(3)
            + attachments.len() * 1600;
        if used.saturating_add(weight) > budget_tokens && id != turn {
            context.omitted = true;
            break;
        }
        used = used.saturating_add(weight);
        let attachment_table = rx.open_table(ATTACHMENTS)?;
        let attachment_bytes = attachments
            .iter()
            .map(|id| {
                attachment_table
                    .get(id.get())?
                    .map(|bytes| {
                        decode_attachment(id.get(), bytes.value())
                            .map(|attachment| attachment.byte_length)
                    })
                    .transpose()?
                    .ok_or_else(|| StorageError::corrupted("turn attachment missing"))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        {
            context.turns.push(ContextTurn {
                turn: id,
                text,
                attachments,
                attachment_bytes,
                answer,
            });
        }
    }
    context.turns.reverse();
    if let Some(conversation) = row.parent_id.filter(|_| row.kind.is_persistent()) {
        let state = read_state(&rx, turn)?;
        let through = state.claim.map_or(0, |claim| claim.notice_seq);
        let mut previous = row.kind.previous();
        let mut after = 0;
        while let Some(id) = previous {
            if let Some(claim) = read_state(&rx, id)?.claim {
                after = claim.notice_seq;
                break;
            }
            previous = read_job_row(&rx, id)?
                .ok_or_else(|| StorageError::corrupted("previous turn missing"))?
                .kind
                .previous();
        }
        let mut status_rows = Vec::new();
        let mut live = vec![conversation];
        let mut offset = 0;
        while offset < live.len() {
            let parent = live[offset];
            offset += 1;
            for entry in rx
                .open_table(LIVE_CHILDREN)?
                .range((parent.get(), 0)..=(parent.get(), u128::MAX))?
            {
                let child = JobId::new(entry?.0.value().1);
                let child_row = read_job_row(&rx, child)?
                    .ok_or_else(|| StorageError::corrupted("live child missing"))?;
                if matches!(
                    child_row.kind,
                    JobKind::Group {
                        origin: Origin::System,
                        ..
                    }
                ) {
                    continue;
                }

                let closing = read_job_control(&rx, child)?.closing;
                status_rows.push(format!(
                    "{child}: {}",
                    if closing.is_some() { "closing" } else { "open" }
                ));
                live.push(child);
            }
        }
        if after < through {
            for entry in rx
                .open_table(NOTICES)?
                .range((conversation.get(), after + 1)..=(conversation.get(), through))?
            {
                let notice = Notice::decode(entry?.1.value())?;
                if notice.class == NoticeClass::Attention {
                    if let NoticeBody::Event {
                        child,
                        event,
                        reference,
                    } = notice.body
                    {
                        context.attention.push_str(&entry_text(
                            &rx,
                            &Entry::Notice {
                                child,
                                event,
                                reference,
                            },
                        )?);
                        context.attention.push('\n');
                    }
                } else {
                    status_rows.push(format!("{:?}", notice.body));
                }
            }
        }
        let mut remaining = 0;
        for line in status_rows {
            if context.tree_status.len() + line.len() + 30 <= 3072 {
                context.tree_status.push_str(&line);
                context.tree_status.push('\n');
            } else {
                remaining += 1;
            }
        }
        if remaining > 0 {
            context
                .tree_status
                .push_str(&format!("and {remaining} more"));
        }
    }
    Ok(context)
}

pub(super) fn has_decider(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    conversation: Option<JobId>,
) -> Result<bool, StorageError> {
    let mut current = conversation;
    for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
        let Some(id) = current else {
            return Ok(false);
        };
        let Some(bytes) = jobs.get(id.get())? else {
            return Ok(false);
        };
        let row = JobSpec::decode_at(id.get(), bytes.value())?;
        match row.kind {
            JobKind::Group {
                origin: Origin::User,
                ..
            } => return Ok(true),
            JobKind::Group { .. } => current = row.parent_id,
            JobKind::Execution { .. } => return Ok(false),
        }
    }
    Ok(false)
}
pub(crate) fn read_has_decider(
    database: &Database,
    conversation: Option<JobId>,
) -> Result<bool, StorageError> {
    has_decider(&read(database)?.open_table(JOBS)?, conversation)
}
fn approved_calls(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let mut calls = Vec::new();
    let table = rx.open_table(TOOL_CALLS)?;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        let (key, state) = entry?;
        if state.value() != 1 {
            continue;
        }
        let (_, turn, request, index) = key.value();
        let call = tool_call_ref((turn, request, index))?;
        let bytes = table
            .get(tool_call_key(call))?
            .ok_or_else(|| StorageError::corrupted("approved call missing"))?;
        calls.push(decode_tool_evidence(call, bytes.value())?);
    }
    Ok(calls)
}
pub(crate) fn read_delegated_workspace(
    database: &Database,
    job: JobId,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(false);
    };
    let Some(parent) = row.parent_id else {
        return Ok(false);
    };
    let Some(group) = read_job_row(&rx, parent)? else {
        return Ok(false);
    };
    let JobKind::Group {
        agent: None,
        project: Some(project),
        origin: Origin::Agent { call },
        ..
    } = group.kind
    else {
        return Ok(false);
    };
    let evidence = rx
        .open_table(TOOL_CALLS)?
        .get(tool_call_key(call))?
        .map(|bytes| decode_tool_evidence(call, bytes.value()))
        .transpose()?;
    let Some(approval) = evidence
        .filter(|call| call.name == "job_dispatch")
        .and_then(|call| call.approval)
    else {
        return Ok(false);
    };
    let decision = rx
        .open_table(APPROVALS)?
        .get(tool_call_key(approval))?
        .map(|bytes| ApprovalRecord::decode(bytes.value()))
        .transpose()?;
    let Some(ApprovalRecord {
        decision: ApprovalDecision::Answered { text },
        by: Actor::User,
        ..
    }) = decision
    else {
        return Ok(false);
    };
    let selection: crate::db::entities::DispatchSelection = serde_json::from_str(&text)
        .map_err(|_| StorageError::corrupted("invalid dispatch selection"))?;
    let frozen = job_execution(&rx.open_table(JOBS)?, &row)?.0;
    Ok(selection.agent.is_none()
        && crate::db::entities::parse_entity_id(&selection.model)
            == Some(frozen.llm.model.id.get())
        && selection
            .projects
            .iter()
            .any(|id| crate::db::entities::parse_entity_id(id) == Some(project.get())))
}

pub(crate) fn read_has_pending_approvals(
    database: &Database,
    conversation: JobId,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        if entry?.1.value() == 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn read_approved_calls(
    database: &Database,
    conversation: JobId,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    approved_calls(&read(database)?, conversation)
}
fn question(arguments: &str) -> String {
    serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|v| {
            v.get("question")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}
fn approval_assessment(
    rx: &ReadTransaction,
    call: ToolCallRef,
) -> Result<Option<serde_json::Value>, StorageError> {
    use crate::db::entities::{DecisionAnswer, DecisionOutcome, DecisionResult};
    let row = read_job_row(rx, call.job)?
        .ok_or_else(|| StorageError::corrupted("approval job missing"))?;
    let (_, project, _) = job_execution(&rx.open_table(JOBS)?, &row)?;
    let Some(project) = project else {
        return Ok(None);
    };
    let policy = rx
        .open_table(PROJECTS)?
        .get(project.get())?
        .map(|bytes| decode_project(project.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("approval project missing"))?
        .decision_policy();
    if !policy.enabled {
        return Ok(None);
    }
    let mut assessment = serde_json::json!({
        "state": "unavailable", "confidence": null,
        "minimum_confidence": f64::from(policy.minimum_confidence_millis) / 1000.0
    });
    let identity = approvals::command_assessment_identity(call)
        .map_err(|_| StorageError::corrupted("invalid command assessment identity"))?;
    let digest = identity.digest();
    let Some(submission) = rx
        .open_table(SUBMISSIONS)?
        .get(digest.as_bytes().as_slice())?
        .map(|bytes| decode_submission(digest.as_bytes(), bytes.value()))
        .transpose()?
    else {
        return Ok(Some(assessment));
    };
    let state = read_state(rx, submission.job)?;
    assessment["state"] = serde_json::json!(match state.outcome {
        None =>
            if state.claim.is_some() {
                "running"
            } else {
                "pending"
            },
        Some(ref outcome) if outcome.kind == OutcomeKind::Completed => "completed",
        Some(_) => "failed",
    });
    if let Some((key, bytes)) = rx
        .open_table(JOB_OUTPUTS)?
        .range((submission.job.get(), 0)..=(submission.job.get(), u32::MAX))?
        .next_back()
        .transpose()?
    {
        let output = decode_output(key.value(), bytes.value())?;
        if let Some(result) = output
            .value
            .as_ref()
            .and_then(|value| value.get("decision"))
        {
            let result: DecisionResult = serde_json::from_value(result.clone())
                .map_err(|_| StorageError::corrupted("invalid command assessment result"))?;
            if let DecisionOutcome::Answer {
                answer: DecisionAnswer::Choice { confidence, .. },
            } = result.outcome
            {
                assessment["confidence"] = confidence
                    .map_or(serde_json::Value::Null, |confidence| {
                        serde_json::json!(confidence.value)
                    });
            }
        }
    }
    Ok(Some(assessment))
}

pub(crate) fn read_cards(
    database: &Database,
    root: JobId,
) -> Result<Vec<serde_json::Value>, StorageError> {
    use serde_json::json;
    let rx = read(database)?;
    let mut cards = Vec::new();
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(conversation) = pending.pop() {
        if !seen.insert(conversation) || seen.len() > 65 {
            return Err(StorageError::corrupted(
                "live conversation tree exceeds its bound",
            ));
        }
        let row = read_job_row(&rx, conversation)?
            .ok_or_else(|| StorageError::corrupted("conversation missing"))?;
        let JobKind::Group { origin, .. } = row.kind else {
            return Err(StorageError::corrupted("expected a conversation"));
        };
        let parent = row.parent_id.map(|id| id.to_string());
        let metadata = read_job_control(&rx, conversation)?;
        if origin != Origin::System {
            for entry in rx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
            )? {
                let (key, state) = entry?;
                if state.value() != 0 {
                    continue;
                }
                let (_, turn, request, index) = key.value();
                let call = tool_call_ref((turn, request, index))?;
                let table = rx.open_table(TOOL_CALLS)?;
                let bytes = table
                    .get(tool_call_key(call))?
                    .ok_or_else(|| StorageError::corrupted("awaiting call missing"))?;
                let call = decode_tool_evidence(call, bytes.value())?;
                let arguments = call
                    .decision
                    .fact
                    .stored_admission()
                    .ok_or_else(|| StorageError::corrupted("awaiting admission missing"))?
                    .arguments
                    .as_str();
                let mut card = json!({"kind": if matches!(call.decision.fact, crate::db::entities::ToolDecision::AwaitingAnswer(_)) { "question" } else { "approval" }, "conversation": conversation.to_string(), "parent":parent, "approval":[JobId::new(turn).to_string(),request,index], "tool":call.name});
                if call.name == "job_dispatch" {
                    card["kind"] = json!("dispatch");
                    card["arguments"] = json!(arguments);
                } else if matches!(
                    call.decision.fact,
                    crate::db::entities::ToolDecision::AwaitingAnswer(_)
                ) {
                    card["question"] = json!(question(arguments));
                } else {
                    card["arguments"] = json!(arguments);
                    if matches!(
                        call.name.as_str(),
                        "workspace_request_command" | "workspace_run_command"
                    ) && let Some(assessment) = approval_assessment(&rx, call.call)?
                    {
                        card["assessment"] = assessment;
                    }
                }
                cards.push(card);
            }
            if let Some(seq) = metadata.on_hold {
                let notice = rx
                    .open_table(NOTICES)?
                    .get((conversation.get(), seq))?
                    .map(|v| Notice::decode(v.value()))
                    .transpose()?;
                if matches!(
                    notice.map(|n| n.body),
                    Some(NoticeBody::Record {
                        by: Actor::Loop,
                        ..
                    })
                ) {
                    cards.push(json!({"kind":"loop_hold", "conversation":conversation.to_string(), "parent":parent}));
                }
            }
        }
        if let Some(active) = rx.open_table(ACTIVE_JOBS)?.get(conversation.get())? {
            let turn = JobId::new(active.value());
            let state = read_state(&rx, turn)?;
            if state
                .outcome
                .is_some_and(|o| o.kind == OutcomeKind::Unknown)
                && state.stop_confirmed_at.is_none()
            {
                cards.push(json!({"kind":"quarantine", "conversation":conversation.to_string(), "parent":parent, "turn":turn.to_string()}));
            }
        }
        for entry in rx
            .open_table(LIVE_CHILDREN)?
            .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
        {
            pending.push(JobId::new(entry?.0.value().1));
        }
    }
    Ok(cards)
}

pub(crate) struct UsageRetriesPage {
    /// Partial groups: the same `(bucket_us, agent, provider, model)` key may
    /// appear again on the next page, and its `retries` must be summed.
    pub rows: Vec<crate::db::entities::UsageRetryRow>,
    /// The last raw retry key scanned. It is set exactly when more keys
    /// remain, so `rows.is_empty()` with `next` is a valid continuation, not
    /// exhaustion.
    pub next: Option<(u128, u128)>,
}

/// Scans at most `limit` retry keys, including those outside the time and
/// agent filters, and groups the matches. A group can span pages because the
/// scan window ends at a raw key, not at a group boundary; consumers merge
/// partial rows by their stable key. The cursor advances over scanned keys
/// even when the page has no matches, and is `None` only at real exhaustion.
pub(crate) fn read_usage_retries(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: crate::db::entities::UsageBucketing,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<UsageRetriesPage, StorageError> {
    let rx = read(database)?;
    let retries = rx.open_table(RETRIES)?;
    let start = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    let mut entries = retries.range((start, std::ops::Bound::Unbounded))?;
    let mut grouped: BTreeMap<UsageGroupKey, (String, u64)> = BTreeMap::new();
    let mut last = None;
    for _ in 0..limit.clamp(1, 100) {
        let Some(entry) = entries.next() else { break };
        let key = entry?.0.value();
        last = Some(key);
        let retry = read_job_row(&rx, JobId::new(key.1))?
            .ok_or_else(|| StorageError::corrupted("retry has no job row"))?;
        let at = retry.created_at.as_unix_micros();
        if filter.from_us.is_some_and(|from| at < from)
            || filter.until_us.is_some_and(|until| at >= until)
        {
            continue;
        }
        let original = read_job_row(&rx, JobId::new(key.0))?
            .ok_or_else(|| StorageError::corrupted("retry names a missing turn"))?;
        let parent = original
            .parent_id
            .map(|id| read_job_row(&rx, id))
            .transpose()?
            .flatten();
        let agent = usage_agent(&original, parent.as_ref());
        if filter.agent.is_some_and(|target| agent != target) {
            continue;
        }
        let Some(target) = turn_target(&rx, &original)? else {
            continue;
        };
        let config = target.frozen;
        let bucket = bucketing
            .start_of(at)
            .ok_or_else(|| StorageError::corrupted("retry bucket overflows"))?;
        let model = config.config.model;
        let hash = *ContentDigest::of(model.as_bytes()).as_bytes();
        let slot = grouped
            .entry((bucket, agent, config.provider.id.get(), hash))
            .or_insert((model, 0));
        slot.1 += 1;
    }
    let more = entries.next().transpose()?.is_some();
    Ok(UsageRetriesPage {
        rows: grouped
            .into_iter()
            .map(|((bucket_us, agent, provider, _), (model, retries))| {
                crate::db::entities::UsageRetryRow {
                    bucket_us,
                    agent,
                    provider,
                    model: Some(model),
                    retries,
                }
            })
            .collect(),
        next: if more { last } else { None },
    })
}
