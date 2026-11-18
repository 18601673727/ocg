use crate::db::derived::ActivityProjection;
use crate::db::entities::{ConfigBody, ConfigId, DependencyState, OutcomeKind, ToolCallRef};
use crate::db::entities::{
    DependencyCondition, ExecutionEventKind, JobId, JobLifecycle, JobOutputId, JobOutputRecord,
    JobSearchPage, JobSpec, JobState, JobTree, JobUsage, MutableConfiguration, MutableJob,
    MutableJobContent, MutableJobDetails, NativeJobItem, Origin, OutputKind, Recurrence,
    RequestNumber, ToolEvidenceRecord, TreeFacts, TreeNode,
};
use crate::db::redb::approvals::approved_calls;
use crate::db::redb::batch::Batch;
use crate::db::redb::reads::conversations::{conversation_summary, conversation_tip};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{ConfigRow, decode_output, decode_tool_evidence, tool_call_ref};
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, APPROVALS, CHILDREN, CONFIGS, CONVERSATIONS, JOB_EVENTS, JOB_OBSERVATIONS,
    JOB_OUTPUTS, JOB_RUNTIME, JOBS, MAX_OBSERVATION_SCAN, MAX_PAGE, OPEN_APPROVALS,
    POOL_ALLOCATIONS, RECURRENCES, REQUEST_EVENTS, TOOL_CALLS, TURNS,
};
use crate::db::redb::{StorageError, TxRead, allocations, rows, state};
use crate::db::redb::{execution, observations};
use ::redb::{Database, ReadTransaction, ReadableTable};
use std::collections::BTreeMap;

pub(crate) fn read_conversation_row(
    rx: &ReadTransaction,
    id: JobId,
) -> Result<Option<JobSpec>, StorageError> {
    rx.open_table(CONVERSATIONS)?
        .get(id.get())?
        .map(|bytes| JobSpec::decode_at(id.get(), bytes.value()))
        .transpose()
}

pub(crate) fn read_job_lifecycle(
    rx: &ReadTransaction,
    id: JobId,
) -> Result<JobLifecycle, StorageError> {
    Ok(state::require(rx, id)?.lifecycle)
}

pub(crate) fn read_state(rx: &ReadTransaction, id: JobId) -> Result<JobState, StorageError> {
    Ok(state::require(rx, id)?.state)
}

pub(crate) fn turn_head(tx: &impl TxRead, group: JobId) -> Result<Option<JobId>, StorageError> {
    let turns = tx.table(TURNS)?;
    let jobs = tx.table(JOBS)?;
    for entry in turns
        .range((group.get(), 0)..=(group.get(), u128::MAX))?
        .rev()
    {
        let id = entry?.0.value().1;
        let row = jobs
            .get(id)?
            .ok_or_else(|| StorageError::corrupted("turn has no source Job"))?;
        let job = crate::db::redb::jobs::decode_mutable_job(id, row.value())?;
        if project_planning_target(tx, &job)?.is_none() {
            return Ok(Some(job.id));
        }
    }
    Ok(None)
}

pub(super) fn project_planning_target(
    tx: &impl TxRead,
    job: &MutableJob,
) -> Result<Option<JobId>, StorageError> {
    let MutableJobContent::Decision { request, .. } = &job.input.content else {
        return Ok(None);
    };
    if request.state.get("kind").and_then(|value| value.as_str())
        != Some("project_planning_decision")
    {
        return Ok(None);
    }
    let Some(target) = request
        .state
        .get("target_job")
        .and_then(|value| value.as_str())
        .and_then(|id| id.parse::<u128>().ok())
    else {
        return Ok(None);
    };
    // The planning dependency is recorded in the transaction that creates the Decision.
    let events = tx.table(JOB_EVENTS)?;
    let Some((key, bytes)) = events
        .range((job.id.get(), 0)..=(job.id.get(), u64::MAX))?
        .next()
        .transpose()?
    else {
        return Ok(None);
    };
    let transaction = execution::decode_event(key.value(), bytes.value())?.transaction;
    for entry in events.range((target, transaction)..=(target, u64::MAX))? {
        let (key, bytes) = entry?;
        let event = execution::decode_event(key.value(), bytes.value())?;
        if event.transaction != transaction {
            break;
        }
        if matches!(
            event.kind,
            ExecutionEventKind::DependencyAdded { prerequisite, condition: DependencyCondition::Success }
                if prerequisite == job.id
        ) {
            return Ok(Some(JobId::new(target)));
        }
    }
    Ok(None)
}

pub(crate) fn turn_first(
    turns: &impl ReadableTable<(u128, u128), ()>,
    group: JobId,
) -> Result<Option<JobId>, StorageError> {
    Ok(turns
        .range((group.get(), 0)..=(group.get(), u128::MAX))?
        .next()
        .transpose()?
        .map(|(key, _)| JobId::new(key.value().1)))
}

pub(crate) fn mutable_job_record_in(
    rx: &ReadTransaction,
    id: JobId,
) -> Result<Option<crate::db::entities::MutableJobRecord>, StorageError> {
    let jobs = rx.open_table(JOBS)?;
    let Some(bytes) = jobs.get(id.get())? else {
        return Ok(None);
    };
    let job = crate::db::redb::jobs::decode_mutable_job(id.get(), bytes.value())?;
    let states = rx.open_table(JOB_RUNTIME)?;
    let state = states
        .get(id.get())?
        .ok_or_else(|| StorageError::corrupted("mutable Job state is missing"))?;
    let state = state::decode_state(id.get(), state.value())?;
    let parent = read_conversation_row(rx, job.parent_id)?
        .ok_or_else(|| StorageError::corrupted("mutable Job conversation missing"))?;
    let record = crate::db::entities::MutableJobRecord {
        job,
        outcome: state.outcome,
        project: parent.project,
        event_seq: execution::latest_seq(rx, id)?,
    };
    Ok(Some(record))
}

fn native_job_item(
    rx: &ReadTransaction,
    record: crate::db::entities::MutableJobRecord,
) -> Result<NativeJobItem, StorageError> {
    let id = record.job.id;
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(id.get())?
        .map(|bytes| allocations::decode_pool_allocation(id.get(), bytes.value()))
        .transpose()?;
    if allocation
        .as_ref()
        .is_some_and(|allocation| allocation.job_revision != record.job.revision)
    {
        return Err(StorageError::corrupted(
            "allocation refers to another Job revision",
        ));
    }
    let updated_at = [
        Some(record.job.created_at),
        record.job.submitted_at,
        record.outcome.as_ref().map(|outcome| outcome.at),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(record.job.created_at);
    Ok(NativeJobItem {
        record,
        allocation,
        updated_at_us: updated_at.as_unix_micros(),
    })
}

pub(crate) fn read_native_job(
    database: &Database,
    id: JobId,
) -> Result<Option<NativeJobItem>, StorageError> {
    let rx = read(database)?;
    let Some(record) = mutable_job_record_in(&rx, id)? else {
        return Ok(None);
    };
    if record.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
        return Ok(None);
    }
    native_job_item(&rx, record).map(Some)
}

const JOB_LIST_SCAN: usize = 5_000;

pub(crate) fn read_job_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<JobSearchPage, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u128::MAX, JobId::get);
    let limit = limit.clamp(1, 100);
    let mut items: Vec<NativeJobItem> = Vec::new();
    let mut last_examined = None;
    let mut has_more = false;
    let mut next_before = None;
    for (examined, entry) in jobs.range(..end)?.rev().enumerate() {
        if examined == JOB_LIST_SCAN {
            has_more = true;
            next_before = last_examined;
            break;
        }
        let (key, _) = entry?;
        let id = JobId::new(key.value());
        last_examined = Some(id);
        let Some(record) = mutable_job_record_in(&rx, id)? else {
            continue;
        };
        if record.job.phase == MutableJobPhase::Unsubmitted {
            continue;
        }
        if !terms.is_empty() {
            let identity = id.to_string().to_lowercase();
            let text = record.job.input.text().to_lowercase();
            if !terms
                .iter()
                .all(|term| identity.contains(term.as_str()) || text.contains(term.as_str()))
            {
                continue;
            }
        }
        if items.len() == limit {
            has_more = true;
            next_before = items.last().map(|item| item.record.job.id);
            break;
        }
        items.push(native_job_item(&rx, record)?);
    }
    Ok(JobSearchPage {
        items,
        next_before,
        has_more,
    })
}

pub(crate) const TREE_PAGE_MAX: usize = 100;

pub(crate) const TREE_NESTED: usize = 20;

pub(crate) const TREE_NODES_MAX: usize = 300;

pub(crate) const TREE_DEPTH_MAX: u8 = 4;

const TREE_ANCESTORS_MAX: usize = 8;

const TREE_DEPENDENCIES_MAX: usize = 16;

const TITLE_CHARS: usize = 80;

struct ChildPage {
    listed: Vec<JobId>,
    has_more: bool,
    next_after: Option<JobId>,
}

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

fn has_children(rx: &ReadTransaction, parent: JobId) -> Result<bool, StorageError> {
    Ok(rx
        .open_table(CHILDREN)?
        .range((parent.get(), 0)..=(parent.get(), u128::MAX))?
        .next()
        .transpose()?
        .is_some())
}

fn read_ancestors(rx: &ReadTransaction, root: JobId) -> Result<Vec<JobId>, StorageError> {
    let mut ancestors = Vec::new();
    let mut current = read_conversation_row(rx, root)?.and_then(|row| row.parent_id);
    while let Some(id) = current {
        if id.get() == 0 {
            break;
        }
        ancestors.push(id);
        if ancestors.len() > TREE_ANCESTORS_MAX {
            return Err(StorageError::corrupted("too many ancestors"));
        }
        current = match read_conversation_row(rx, id)? {
            Some(row) => row.parent_id,
            None => return Err(StorageError::corrupted(format!("{id} has no job row"))),
        };
    }
    ancestors.reverse();
    Ok(ancestors)
}

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

pub(crate) fn active_job_in(
    rx: &impl TxRead,
    conversation: JobId,
) -> Result<Option<crate::db::entities::MutableJob>, StorageError> {
    let jobs = rx.table(JOBS)?;
    for entry in rx.table(ACTIVE_ALLOCATIONS)?.iter()? {
        let (_, _, id) = entry?.0.value();
        let row = jobs
            .get(id)?
            .ok_or_else(|| StorageError::corrupted("active Allocation references a missing Job"))?;
        let job = crate::db::redb::jobs::decode_mutable_job(id, row.value())?;
        if job.parent_id == conversation && job.summary_source.is_none() {
            return Ok(Some(job));
        }
    }
    Ok(None)
}

pub(crate) fn active_job(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<(JobId, ConfigId, String)>, StorageError> {
    let Some(job) = active_job_in(rx, conversation)? else {
        return Ok(None);
    };
    let (model, name) = match job.frozen {
        Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
            (frozen.llm.model.id, frozen.llm.config.model)
        }
        Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) => {
            (frozen.model_ref.id, frozen.model.model)
        }
        None => {
            return Err(StorageError::corrupted(
                "active Job has no frozen configuration",
            ));
        }
    };
    Ok(Some((job.id, model, name)))
}

fn dependency_state(
    rx: &ReadTransaction,
    dependency: JobId,
    condition: DependencyCondition,
) -> Result<DependencyState, StorageError> {
    let target = match read_conversation_row(rx, dependency)? {
        Some(row) if !row.is_chunk_group() => match turn_head(rx, dependency)? {
            Some(head) => head,
            None => return Ok(DependencyState::Waiting),
        },
        Some(_) | None => dependency,
    };
    let state = rx
        .open_table(JOB_RUNTIME)?
        .get(target.get())?
        .map(|bytes| state::decode_state(target.get(), bytes.value()))
        .transpose()?;
    Ok(Batch::dependency_outcome_state(
        state.as_ref().and_then(|state| state.outcome.as_ref()),
        condition,
    ))
}

fn cap_title(title: &str) -> String {
    title.chars().take(TITLE_CHARS).collect()
}

pub(crate) fn first_entry_text(rx: &ReadTransaction, turn: JobId) -> Result<String, StorageError> {
    let job = mutable_job_record_in(rx, turn)?
        .ok_or_else(|| StorageError::corrupted("conversation turn missing"))?;
    Ok(job.job.input.text().chars().take(TITLE_CHARS).collect())
}

enum TreeRow {
    Conversation(Box<JobSpec>),
    Turn(Box<crate::db::entities::MutableJobRecord>),
}

impl TreeRow {
    fn id(&self) -> JobId {
        match self {
            Self::Conversation(row) => row.id,
            Self::Turn(record) => record.job.id,
        }
    }
}

fn tree_row(rx: &ReadTransaction, id: JobId) -> Result<Option<TreeRow>, StorageError> {
    if let Some(row) = read_conversation_row(rx, id)? {
        return Ok(Some(TreeRow::Conversation(Box::new(row))));
    }
    Ok(mutable_job_record_in(rx, id)?.map(|record| TreeRow::Turn(Box::new(record))))
}

fn tree_node(
    rx: &ReadTransaction,
    row: &TreeRow,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<TreeNode, StorageError> {
    let (parent_id, created_at, facts, agent) = match row {
        TreeRow::Conversation(row) => {
            let summary = conversation_summary(rx, row, conversation_tip(rx, row)?)?;
            let state = match summary.state.as_str() {
                "open" => "open",
                "closing" => "closing",
                "settled" => "settled",
                _ => return Err(StorageError::corrupted("unknown conversation state")),
            };
            let origin = match row.origin {
                Origin::User => "user",
                Origin::Agent { .. } => "agent",
                Origin::System => "system",
            };
            let recurrence = rx
                .open_table(RECURRENCES)?
                .get(row.id.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?;
            let mut depends_on = Vec::new();
            for (dependency, condition) in read_job_lifecycle(rx, row.id)?
                .after
                .iter()
                .take(TREE_DEPENDENCIES_MAX)
            {
                depends_on.push((
                    *dependency,
                    *condition,
                    dependency_state(rx, *dependency, *condition)?,
                ));
            }
            (
                row.parent_id,
                row.created_at,
                TreeFacts::Conversation {
                    title: cap_title(&summary.title),
                    state,
                    on_hold: summary.on_hold,
                    origin,
                    keep_open: row.keep_open,
                    active_turn: active_job(rx, row.id)?,
                    recurrence,
                    depends_on,
                },
                summary.agent,
            )
        }
        TreeRow::Turn(record) => {
            use crate::db::entities::{
                MutableFrozenConfiguration, MutableJobContent, MutableJobPhase,
            };
            let job = &record.job;
            let (model, agent) = match (&job.frozen, &job.input.content) {
                (Some(MutableFrozenConfiguration::Generative(frozen)), _) => (
                    (frozen.llm.model.id, frozen.llm.config.model.clone()),
                    frozen.agent_ref.as_ref().map(|reference| reference.id),
                ),
                (
                    _,
                    MutableJobContent::Generative {
                        configuration: Some(configuration),
                        ..
                    },
                ) => (
                    (configuration.llm_profile.into(), String::new()),
                    configuration.agent_profile.map(Into::into),
                ),
                (_, MutableJobContent::Decision { profile, .. }) => {
                    ((*profile, String::new()), Some(*profile))
                }
                (_, MutableJobContent::Generative { .. }) => {
                    return Err(StorageError::corrupted("turn has no configuration"));
                }
            };
            let state = match record.outcome.as_ref().map(|outcome| outcome.kind) {
                Some(OutcomeKind::Completed) => "completed",
                Some(OutcomeKind::Failed) => "failed",
                Some(OutcomeKind::Cancelled) => "cancelled",
                Some(OutcomeKind::Unknown) => "unknown",
                None => match job.phase {
                    MutableJobPhase::Executing => "running",
                    MutableJobPhase::Unsubmitted | MutableJobPhase::Queued => "queued",
                },
            };
            (
                Some(job.parent_id),
                job.created_at,
                TreeFacts::Turn {
                    title: cap_title(job.input.text()),
                    state,
                    conversation: Some(job.parent_id),
                    previous: job.previous,
                    model,
                    depends_on: Vec::new(),
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
        id: row.id(),
        parent_id,
        created_at,
        facts,
        agent,
        children: None,
        has_more_children: false,
        next_after: None,
    })
}

pub(crate) fn read_job_tree(
    database: &Database,
    root: JobId,
    after: Option<JobId>,
    limit: usize,
    depth: u8,
) -> Result<Option<JobTree>, StorageError> {
    let rx = read(database)?;
    let Some(root_row) = tree_row(&rx, root)? else {
        return Ok(None);
    };
    let ancestors = read_ancestors(&rx, root)?;
    let mut names: BTreeMap<ConfigId, Option<String>> = BTreeMap::new();
    let mut nodes = vec![tree_node(&rx, &root_row, &mut names)?];
    let page = child_page(&rx, root, after, limit.min(TREE_PAGE_MAX))?;
    let mut level = Vec::new();
    for child in page.listed {
        let row = tree_row(&rx, child)?
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
                let row = tree_row(&rx, child)?
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

pub(crate) fn read_output_job_is_decision(
    database: &Database,
    id: JobId,
) -> Result<Option<bool>, StorageError> {
    use crate::db::entities::{MutableJobContent, MutableJobPhase};

    let rx = read(database)?;
    if let Some(row) = rx.open_table(JOBS)?.get(id.get())? {
        let job = crate::db::redb::jobs::decode_mutable_job(id.get(), row.value())?;
        return Ok(
            (job.phase != MutableJobPhase::Unsubmitted).then_some(matches!(
                job.input.content,
                MutableJobContent::Decision { .. }
            )),
        );
    }
    Ok(read_conversation_row(&rx, id)?.map(|_| false))
}

pub(crate) fn read_outputs(
    database: &Database,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    outputs(&read(database)?, job, after_seq, limit)
}

fn outputs(
    rx: &ReadTransaction,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    if let Some(conversation) = read_conversation_row(rx, job)? {
        if after_seq.is_some_and(|seq| seq >= 1) || limit == 0 {
            return Ok(Vec::new());
        }
        return Ok(conversation_result(rx, &conversation)?
            .into_iter()
            .collect());
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = match after_seq.map(u32::try_from) {
        None => 0,
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

pub(crate) fn read_latest_output(
    database: &Database,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    latest_output(&read(database)?, job)
}

fn latest_output(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    if let Some(conversation) = read_conversation_row(rx, job)? {
        return conversation_result(rx, &conversation);
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
    row: &JobSpec,
) -> Result<Option<JobOutputRecord>, StorageError> {
    let runtime = state::require(rx, row.id)?;
    let Some(outcome) = runtime.state.outcome else {
        return Ok(None);
    };
    let mut summary = String::new();
    let mut value = None;
    if row.is_chunk_group() {
        summary = match outcome.kind {
            OutcomeKind::Completed => "All summary chunks completed.",
            OutcomeKind::Failed => "A summary chunk failed.",
            OutcomeKind::Cancelled => "A summary chunk was cancelled.",
            OutcomeKind::Unknown => "The summary chunks have an unknown outcome.",
        }
        .to_owned();
    } else if let Some(turn) = runtime.last_result.map(JobId::get)
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
    for call in approved_calls(rx, row.id)? {
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
        job: row.id,
        seq: 1,
        kind: OutputKind::Result,
        summary: Some(summary),
        code: None,
        goal_reached: None,
        value,
        created_at: outcome.at,
    }))
}

pub(crate) fn read_tool_evidence(
    database: &Database,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    tool_evidence(&read(database)?, job, after, limit)
}

pub(crate) fn tool_evidence(
    rx: &ReadTransaction,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
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

pub(crate) fn tool_call_name(
    rx: &ReadTransaction,
    call: ToolCallRef,
) -> Result<Option<String>, StorageError> {
    rx.open_table(TOOL_CALLS)?
        .get(rows::tool_call_key(call))?
        .map(|bytes| decode_tool_evidence(call, bytes.value()).map(|record| record.name))
        .transpose()
}

#[derive(Clone)]
pub(crate) struct UsageIdentity {
    pub(crate) provider: u128,
    pub(crate) model: String,
}

pub(crate) fn read_mutable_job_details(
    database: &Database,
    id: JobId,
    context: Option<crate::db::entities::ComposerContextId>,
) -> Result<Option<MutableJobDetails>, StorageError> {
    mutable_job_details_in(&read(database)?, id, context, true)
}

fn mutable_configuration_in(
    rx: &ReadTransaction,
    job: &crate::db::entities::MutableJob,
) -> Result<Option<MutableConfiguration>, StorageError> {
    let Some(selection) = job.input.configuration() else {
        return Ok(None);
    };
    if let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) = &job.frozen {
        if frozen.llm.model.id.get() != selection.llm_profile.get()
            || frozen.agent_ref.map(|agent| agent.id.get())
                != selection.agent_profile.map(|agent| agent.get())
        {
            return Err(StorageError::corrupted(
                "frozen Job configuration disagrees with input",
            ));
        }
        return Ok(Some(MutableConfiguration {
            selection,
            model: frozen.llm.config.model.clone(),
            context_window: frozen.llm.config.context_window,
        }));
    }
    let row = rx
        .open_table(CONFIGS)?
        .get(selection.llm_profile.get())?
        .map(|bytes| ConfigRow::decode_at(selection.llm_profile.get(), bytes.value()))
        .transpose()?;
    let Some(row) = row else {
        if job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
            return Ok(None);
        }
        return Err(StorageError::corrupted("queued Job model missing"));
    };
    let ConfigBody::Model(model) = row.body else {
        if job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
            return Ok(None);
        }
        return Err(StorageError::corrupted(
            "queued Job model has the wrong kind",
        ));
    };
    Ok(Some(MutableConfiguration {
        selection,
        model: model.model,
        context_window: model.context_window,
    }))
}

pub(super) fn unstarted_settlement_in(
    rx: &ReadTransaction,
    record: &crate::db::entities::MutableJobRecord,
    allocation: Option<&crate::db::entities::PoolAllocation>,
) -> Result<bool, StorageError> {
    use crate::db::entities::MutableJobPhase;
    if record.job.phase != MutableJobPhase::Queued
        || allocation.is_some_and(|allocation| allocation.holds_capacity())
    {
        return Ok(false);
    }
    let invalid = || {
        StorageError::corrupted(format!(
            "{} has inconsistent unstarted settlement evidence",
            record.job.id
        ))
    };
    let Some(outcome) = &record.outcome else {
        return if allocation.is_none() {
            Ok(false)
        } else {
            Err(invalid())
        };
    };
    match outcome.kind {
        OutcomeKind::Unknown => return Ok(false),
        OutcomeKind::Completed => return Err(invalid()),
        OutcomeKind::Failed | OutcomeKind::Cancelled => {}
    }
    if allocation.is_none() && outcome.kind != OutcomeKind::Cancelled {
        return Err(invalid());
    }
    if allocation.is_some_and(|allocation| {
        allocation.token.job != record.job.id
            || allocation.job_revision != record.job.revision
            || allocation.started_at.is_some()
    }) {
        return Err(invalid());
    }

    let id = record.job.id.get();
    for table in [REQUEST_EVENTS, TOOL_CALLS, APPROVALS] {
        if rx
            .open_table(table)?
            .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))?
            .next()
            .transpose()?
            .is_some()
        {
            return Err(invalid());
        }
    }
    if rx
        .open_table(JOB_OBSERVATIONS)?
        .range((id, 0, 0)..=(id, u8::MAX, u128::MAX))?
        .next()
        .transpose()?
        .is_some()
    {
        return Err(invalid());
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let mut rows = outputs.range((id, 0)..=(id, u32::MAX))?;
    let output = rows
        .next()
        .transpose()?
        .map(|(key, value)| decode_output(key.value(), value.value()))
        .transpose()?;
    if rows.next().transpose()?.is_some() {
        return Err(invalid());
    }
    match (allocation, output) {
        (None, None) => Ok(true),
        (Some(_), Some(output))
            if output.seq == 1
                && output.kind == OutputKind::Result
                && output.created_at == outcome.at
                && output.goal_reached == Some(false)
                && output.value.is_none() =>
        {
            Ok(true)
        }
        _ => Err(invalid()),
    }
}

pub(crate) fn mutable_job_details_in(
    rx: &ReadTransaction,
    id: JobId,
    context: Option<crate::db::entities::ComposerContextId>,
    authorize: bool,
) -> Result<Option<MutableJobDetails>, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let Some(record) = mutable_job_record_in(rx, id)? else {
        return Ok(None);
    };
    if authorize
        && (context.is_some_and(|context| context != record.job.composer_context)
            || (record.job.phase == MutableJobPhase::Unsubmitted && context.is_none()))
    {
        return Ok(None);
    }
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(id.get())?
        .map(|bytes| allocations::decode_pool_allocation(id.get(), bytes.value()))
        .transpose()?;
    let latest_output = rx
        .open_table(JOB_OUTPUTS)?
        .range((id.get(), 0)..=(id.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()?;
    let mut activity =
        ActivityProjection::new_mutable(&record, allocation.as_ref(), latest_output.as_ref());
    let result = latest_output.filter(|output| output.kind == OutputKind::Result);
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
            if let Some(sample) = observations::usage_sample_of(id, request, None, &events) {
                usage.add_sample(&sample);
            }
            events.clear();
        }
    }
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
            | crate::db::entities::ToolDecision::AwaitingApproval(_) => rx
                .open_table(OPEN_APPROVALS)?
                .get((
                    record.job.parent_id.get(),
                    call.call.job.get(),
                    call.call.request.get(),
                    call.call.index,
                ))?
                .is_some(),
            crate::db::entities::ToolDecision::Admitted(_)
            | crate::db::entities::ToolDecision::Refused(_) => false,
        };
        activity.tool_call(call, awaiting_user);
    }
    let (activity, unknown_reason) = activity.finish_mutable(&record);
    let unknown_reason = unknown_reason.or_else(|| {
        (record
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            && allocation
                .as_ref()
                .is_some_and(crate::db::entities::PoolAllocation::holds_capacity))
        .then(|| {
            let detail = match record.outcome.as_ref().and_then(|outcome| outcome.cause.as_ref()) {
                Some(crate::db::entities::OutcomeCause::Uncertain(reason)) => {
                    format!(" {}", reason.description())
                }
                Some(crate::db::entities::OutcomeCause::Text(detail)) => format!(" {detail}"),
                Some(_) | None => String::new(),
            };
            format!(
                "Execution outcome is uncertain. The Pool Allocation is retained and its capacity is not released until termination is confirmed.{detail}"
            )
        })
    });
    let configuration = mutable_configuration_in(rx, &record.job)?;
    let unsupported_decision_history = if context.is_some() {
        let job = &record.job;
        let queued_generative = job.phase == MutableJobPhase::Queued
            && record.outcome.is_none()
            && matches!(
                job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            );
        Some(if queued_generative {
            super::super::jobs::decision_predecessors(&rx.open_table(JOBS)?, job)?
        } else {
            Ok(false)
        })
    } else {
        None
    };
    Ok(Some(MutableJobDetails {
        record,
        allocation,
        result,
        usage: (scanned != 0).then_some(usage),
        activity,
        unknown_reason,
        configuration,
        unsupported_decision_history,
    }))
}
