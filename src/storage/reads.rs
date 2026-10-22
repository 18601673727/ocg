use std::ops::Bound;

use redb::ReadableTableMetadata;

use super::*;
use crate::persistence::{
    ConfigBody, ConfigId, ConfigKind, ConfigRecord, DependencyState, ExecutionConfiguration,
    ExecutionOperation, ExecutionPoolConfig, FrozenPoolBinding, JobInputRecord, OutcomeKind,
    SubmittedConfiguration,
};

// ---- typed reads ---------------------------------------------------------------

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

fn read_job_row(rx: &ReadTransaction, id: JobId) -> Result<Option<JobRow>, StorageError> {
    rx.open_table(JOBS)?
        .get(id.get())?
        .map(|bytes| JobRow::decode_at(id.get(), bytes.value()))
        .transpose()
}

fn read_conversation(rx: &ReadTransaction, id: JobId) -> Result<ConversationRow, StorageError> {
    rx.open_table(CONVERSATIONS)?
        .get(id.get())?
        .map(|bytes| ConversationRow::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no conversation row")))
}

fn read_state(rx: &ReadTransaction, id: JobId) -> Result<JobStateRow, StorageError> {
    rx.open_table(JOB_STATES)?
        .get(id.get())?
        .map(|bytes| JobStateRow::decode_at(id.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no job_state")))
}

pub(super) fn turn_execution(
    jobs: &impl ReadableTable<u64, &'static [u8]>,
    row: &JobRow,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let JobKind::Turn {
        execution,
        plan_source,
        ..
    } = &row.kind
    else {
        return Err(StorageError::corrupted("expected a turn"));
    };
    let execution = match execution {
        TurnExecution::Frozen(execution) => execution.clone(),
        TurnExecution::Ref(owner) => {
            let bytes = jobs
                .get(owner.get())?
                .ok_or_else(|| StorageError::corrupted("missing execution owner"))?;
            let owner = JobRow::decode_at(owner.get(), bytes.value())?;
            match owner.kind {
                JobKind::Turn {
                    execution: TurnExecution::Frozen(execution),
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
            match JobRow::decode_at(conversation.get(), bytes.value())?.kind {
                JobKind::Conversation { project, agent, .. } => (project, agent),
                JobKind::Turn { .. } => {
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
pub(super) fn job_record(rx: &ReadTransaction, row: JobRow) -> Result<JobRecord, StorageError> {
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
    let execution = if row.kind.is_conversation() {
        None
    } else {
        Some(turn_execution(&rx.open_table(JOBS)?, &row)?.0)
    };
    let configuration = execution.as_ref().map(|execution| ExecutionConfiguration {
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
    let children_closed_at = if row.kind.is_conversation() {
        read_conversation(rx, id)?.closing.map(|closing| closing.at)
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
        conversation: row.kind.is_stored().then_some(row.parent_id).flatten(),
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

#[derive(serde::Serialize, ts_rs::TS, Default)]
#[ts(rename = "WireJobUsage")]
pub(crate) struct JobUsage {
    requests: u64,
    complete_requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
    truncated: bool,
}

pub(crate) struct JobDetails {
    pub job: JobRecord,
    pub result: Option<JobOutputRecord>,
    pub usage: Option<JobUsage>,
    pub unknown_reason: Option<String>,
}

/// Cuts `message` to its first 2048 bytes at a character boundary, ending
/// with "…" when cut, so the Job route stays bounded.
fn cut_unknown_reason(message: &str) -> String {
    const LIMIT: usize = 2048;
    if message.len() <= LIMIT {
        return message.to_owned();
    }
    let mut boundary = LIMIT;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &message[..boundary])
}

pub(crate) fn read_job_details(
    database: &Database,
    id: JobId,
) -> Result<Option<JobDetails>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, id)? else {
        return Ok(None);
    };
    let conversation = row.kind.is_conversation();
    let job = job_record(&rx, row)?;
    let result = if conversation {
        conversation_result(&rx, id)?
    } else {
        let mut result = None;
        for entry in rx
            .open_table(JOB_OUTPUTS)?
            .range((id.get(), 0)..=(id.get(), u32::MAX))?
            .rev()
            .take(MAX_OBSERVATION_SCAN)
        {
            let (key, bytes) = entry?;
            let output = decode_output(key.value(), bytes.value())?;
            if output.kind == OutputKind::Result {
                result = Some(output);
                break;
            }
        }
        result
    };
    let (usage, unknown_reason) = if conversation {
        (None, None)
    } else {
        let target = observations::frozen_llm_target(&rx, id)?;
        let mut usage = JobUsage::default();
        let mut events = Vec::new();
        let mut scanned = 0;
        // The Job's latest request, named by the first event decoded in
        // reverse; only its first Availability counts, so a retried attempt
        // never reports an earlier attempt's failure.
        let want_unknown = job.unknown_at.is_some();
        let mut latest_request: Option<RequestNumber> = None;
        let mut unknown_reason: Option<String> = None;
        let mut unknown_done = !want_unknown;
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
            if !unknown_done {
                if latest_request.is_none() {
                    latest_request = Some(event.request);
                }
                if event.request != latest_request.expect("latest request named") {
                    unknown_done = true;
                } else if let crate::persistence::RequestPayload::Availability { failure, .. } =
                    &event.payload
                {
                    if let Some(failure) = failure {
                        unknown_reason = Some(cut_unknown_reason(&failure.message));
                    }
                    unknown_done = true;
                }
            }
            let seq = event.seq;
            events.push(event);
            if seq == 1 {
                events.reverse();
                if let Some(sample) = observations::usage_sample_of(id, request, &target, &events) {
                    usage.requests = usage.requests.saturating_add(1);
                    usage.complete_requests = usage.complete_requests.saturating_add(u64::from(
                        sample.usage == crate::token_usage::TokenUsageState::Complete,
                    ));
                    usage.input_tokens = usage
                        .input_tokens
                        .saturating_add(u64::from(sample.input.value().unwrap_or(0)));
                    usage.output_tokens = usage
                        .output_tokens
                        .saturating_add(u64::from(sample.output.value().unwrap_or(0)));
                    usage.cache_read_tokens = usage
                        .cache_read_tokens
                        .saturating_add(u64::from(sample.cache_read.value().unwrap_or(0)));
                    usage.cache_write_tokens = usage
                        .cache_write_tokens
                        .saturating_add(u64::from(sample.cache_write.value().unwrap_or(0)));
                    usage.reasoning_tokens = usage
                        .reasoning_tokens
                        .saturating_add(u64::from(sample.reasoning.value().unwrap_or(0)));
                }
                events.clear();
            }
        }
        ((scanned != 0).then_some(usage), unknown_reason)
    };
    Ok(Some(JobDetails {
        job,
        result,
        usage,
        unknown_reason,
    }))
}

pub(crate) fn read_jobs(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobRecord>, StorageError> {
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u64::MAX, JobId::get);
    let mut records = Vec::new();
    for entry in jobs.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(job_record(
            &rx,
            JobRow::decode_at(key.value(), bytes.value())?,
        )?);
    }
    Ok(records)
}

pub(crate) fn read_pending_jobs(
    database: &Database,
    after: Option<JobId>,
    limit: usize,
) -> Result<PendingJobsPage, StorageError> {
    let rx = read(database)?;
    let index = rx.open_table(PENDING)?;
    let start = after.map_or(Bound::Unbounded, |job| Bound::Excluded(job.get()));
    let limit = limit.min(MAX_PAGE);
    let mut entries = index.range((start, Bound::Unbounded))?;
    let mut visited = Vec::new();
    let mut exhausted = false;
    while visited.len() < limit {
        let Some(entry) = entries.next() else {
            exhausted = true;
            break;
        };
        visited.push(PendingJobKey {
            job: JobId::new(entry?.0.value()),
        });
    }
    let last_visited = visited.last().map(|key| key.job);
    Ok(PendingJobsPage {
        visited,
        last_visited,
        exhausted,
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
        .range((parent.get(), start)..=(parent.get(), u64::MAX))?
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
        .range((parent.get(), start)..=(parent.get(), u64::MAX))?
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
        .range((parent.get(), 0)..=(parent.get(), u64::MAX))?
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
fn active_turn(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<(JobId, ConfigId, String)>, StorageError> {
    let Some(turn) = rx
        .open_table(ACTIVE_TURNS)?
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
    let (execution, _, _) = turn_execution(&rx.open_table(JOBS)?, &row)?;
    Ok(Some((
        turn,
        execution.llm.model.id,
        execution.llm.config.model.clone(),
    )))
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
        .map(|bytes| JobStateRow::decode_at(dependency.get(), bytes.value()))
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
    Ok(rx
        .open_table(TURN_INPUTS)?
        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
        .next()
        .transpose()?
        .map(|entry| TurnEntry::decode(entry.1.value()))
        .transpose()?
        .map(|entry| match entry.entry {
            Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } => {
                text.chars().take(TITLE_CHARS).collect()
            }
            _ => String::new(),
        })
        .unwrap_or_default())
}

/// One node of the tree: the Job's own row, its state, and what it owns.
fn tree_node(
    rx: &ReadTransaction,
    row: &JobRow,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<TreeNode, StorageError> {
    let (facts, agent) = match &row.kind {
        JobKind::Conversation {
            origin, keep_open, ..
        } => {
            let tip = rx
                .open_table(TIPS)?
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
                    active_turn: active_turn(rx, row.id)?,
                    recurrence,
                },
                summary.agent,
            )
        }
        JobKind::Turn { name, .. } => {
            let (execution, _, agent) = turn_execution(&rx.open_table(JOBS)?, row)?;
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
                    conversation: row.kind.is_stored().then_some(row.parent_id).flatten(),
                    previous: row.kind.previous(),
                    model: (execution.llm.model.id, execution.llm.config.model.clone()),
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

pub(crate) fn read_retries(
    database: &Database,
    original: JobId,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(RETRIES)?;
    let mut retries = Vec::new();
    for entry in table
        .range((original.get(), 0)..=(original.get(), u64::MAX))?
        .take(MAX_PAGE)
    {
        retries.push(JobId::new(entry?.0.value().1));
    }
    Ok(retries)
}

pub(crate) fn read_outputs(
    database: &Database,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    if read_job_row(&rx, job)?.is_some_and(|row| row.kind.is_conversation()) {
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
    if read_job_row(&rx, job)?.is_some_and(|row| row.kind.is_conversation()) {
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
        .open_table(ANSWER_TIPS)?
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
fn input_of(rx: &ReadTransaction, row: JobRow) -> Result<JobInputRecord, StorageError> {
    let execution = if row.kind.is_conversation() {
        None
    } else {
        Some(turn_execution(&rx.open_table(JOBS)?, &row)?.0)
    };
    Ok(JobInputRecord {
        id: JobInputId::new(row.id.get()),
        objective: row.kind.objective().map(str::to_owned),
        submitted_at: Some(row.created_at),
        created_at: row.created_at,
        updated_at: row.created_at,
        configuration: execution,
        attachments: row.kind.attachments().to_vec(),
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

pub(crate) fn read_job_configuration(
    database: &Database,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    if row.kind.is_conversation() {
        return Ok(None);
    }
    let execution = turn_execution(&rx.open_table(JOBS)?, &row)?.0;
    Ok(Some(SubmittedConfiguration {
        agent_profile: execution.agent_ref.map(|agent| agent.id.into()),
        llm_profile: execution.llm.model.id.into(),
        frozen: execution,
    }))
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
    let end = before.map_or(u64::MAX, AttachmentId::get);
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
    for entry in rx.open_table(ATTACHMENTS)?.iter()? {
        let (key, bytes) = entry?;
        let record = decode_attachment(key.value(), bytes.value())?;
        if record.sha256 == digest && record.is_available() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn unneeded_content(
    database: &Database,
) -> Result<Vec<(ContentDigest, u64)>, StorageError> {
    let rx = read(database)?;
    let mut contents = BTreeMap::new();
    for entry in rx.open_table(ATTACHMENTS)?.iter()? {
        let (key, bytes) = entry?;
        let record = decode_attachment(key.value(), bytes.value())?;
        let needed = contents
            .entry(record.sha256)
            .or_insert((record.byte_length, false));
        needed.1 |= record.is_available();
    }
    Ok(contents
        .into_iter()
        .filter(|(_, (_, needed))| !needed)
        .map(|(digest, (length, _))| (digest, length))
        .collect())
}

#[cfg(test)]
pub(crate) fn record_attachment_cleanup(
    database: &Database,
    id: AttachmentId,
    cleanup: crate::persistence::AttachmentCleanup,
) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    {
        let mut table = tx.open_table(ATTACHMENTS)?;
        let mut record = match table.get(id.get())? {
            Some(bytes) => decode_attachment(id.get(), bytes.value())?,
            None => return Err(StorageError::corrupted(format!("{id} is missing"))),
        };
        record.cleanup = Some(cleanup);
        table.insert(id.get(), encode_attachment(&record).as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// The Jobs that carry an Attachment, whose inputs name it.
pub(crate) fn read_attachment_inputs(
    database: &Database,
    attachment: AttachmentId,
) -> Result<Vec<JobInputId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(ATTACHMENT_REFS)?;
    let mut inputs = Vec::new();
    for entry in table
        .range((attachment.get(), 0)..=(attachment.get(), u64::MAX))?
        .take(MAX_PAGE)
    {
        inputs.push(JobInputId::new(entry?.0.value().1));
    }
    Ok(inputs)
}

// ---- projects ----------------------------------------------------------------------

pub(crate) fn read_project(
    database: &Database,
    id: ProjectId,
) -> Result<Option<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(PROJECTS)?
        .get(id.get())?
        .map(|bytes| decode_project(bytes.value()))
        .transpose()
}

pub(crate) fn read_projects(
    database: &Database,
    before: Option<ProjectId>,
    limit: usize,
) -> Result<Vec<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(PROJECTS)?;
    let end = before.map_or(u64::MAX, ProjectId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        records.push(decode_project(entry?.1.value())?);
    }
    Ok(records)
}

// ---- configs -----------------------------------------------------------------------

fn config_record(row: ConfigRow) -> ConfigRecord {
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
) -> Result<Vec<ConfigRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CONFIGS)?;
    let end = before.map_or(u64::MAX, ConfigId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev() {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if kind.is_none_or(|kind| kind == row.kind) {
            records.push(config_record(row));
            if records.len() >= limit.min(MAX_PAGE) {
                break;
            }
        }
    }
    Ok(records)
}

fn pool_view(row: ConfigRow) -> Option<ExecutionPool> {
    let ConfigBody::Pool(body) = row.body else {
        return None;
    };
    Some(ExecutionPool {
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

pub(crate) fn read_execution_pool(
    database: &Database,
    id: ExecutionPoolId,
) -> Result<Option<ExecutionPool>, StorageError> {
    let rx = read(database)?;
    Ok(rx
        .open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
        .transpose()?
        .and_then(pool_view))
}

pub(crate) fn read_execution_pools(
    database: &Database,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<Vec<ExecutionPool>, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let end = before.map_or(u64::MAX, ExecutionPoolId::get);
    let mut pools = Vec::new();
    for entry in configs.range(..end)?.rev() {
        let (key, bytes) = entry?;
        if let Some(pool) = pool_view(ConfigRow::decode_at(key.value(), bytes.value())?) {
            pools.push(pool);
            if pools.len() >= limit.min(MAX_PAGE) {
                break;
            }
        }
    }
    Ok(pools)
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
    let rx = read(database)?;
    Ok(rx
        .open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
        .transpose()?
        .and_then(executor_view))
}

pub(crate) fn read_executors(
    database: &Database,
    before: Option<ExecutorId>,
    limit: usize,
) -> Result<Vec<ExecutorRecord>, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let end = before.map_or(u64::MAX, ExecutorId::get);
    let mut records = Vec::new();
    for entry in configs.range(..end)?.rev() {
        let (key, bytes) = entry?;
        if let Some(record) = executor_view(ConfigRow::decode_at(key.value(), bytes.value())?) {
            records.push(record);
            if records.len() >= limit.min(MAX_PAGE) {
                break;
            }
        }
    }
    Ok(records)
}

/// The enabled Pools that offer an LLM request over `protocol`, oldest first.
fn pools_offering(
    rx: &ReadTransaction,
    protocol: LlmTransport,
) -> Result<Vec<ExecutorId>, StorageError> {
    let mut pools = Vec::new();
    for entry in rx.open_table(CONFIGS)?.iter()? {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if row.archived_at.is_none()
            && let ConfigBody::Pool(body) = &row.body
            && body.enabled
            && body.offers(ExecutionOperation::LlmRequest, Some(protocol))
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

/// The Pools that offer what the Connection a Job froze speaks.
pub(crate) fn read_job_compatible_executors(
    database: &Database,
    job: JobId,
) -> Result<Vec<ExecutorId>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(Vec::new());
    };
    if row.kind.is_conversation() {
        return Ok(Vec::new());
    }
    let execution = turn_execution(&rx.open_table(JOBS)?, &row)?.0;
    pools_offering(&rx, execution.llm.provider.transport)
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
    let state = JobStateRow::decode_at(job.get(), bytes.value())?;
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
            .map(|bytes| JobStateRow::decode_at(job.get(), bytes.value()))
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
        Probe::Outcome(job) => rx
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobStateRow::decode_at(job.get(), bytes.value()))
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
}
#[derive(Clone, Debug, PartialEq)]
pub struct ConversationsPage {
    pub conversations: Vec<ConversationEntry>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

fn conversation_summary(
    rx: &ReadTransaction,
    conversation: JobId,
    tip: JobId,
) -> Result<ConversationEntry, StorageError> {
    let row = read_job_row(rx, conversation)?
        .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
    let JobKind::Conversation { agent, project, .. } = row.kind else {
        return Err(StorageError::corrupted(
            "recent entry is not a conversation",
        ));
    };
    let metadata = rx
        .open_table(CONVERSATIONS)?
        .get(conversation.get())?
        .map(|bytes| ConversationRow::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("missing conversation state"))?;
    let title = match metadata.title {
        Some(title) => title,
        None => {
            let first = rx
                .open_table(CHILDREN)?
                .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
                .next()
                .transpose()?
                .map(|entry| entry.0.value().1);
            match first {
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
    })
}
#[derive(Clone, Debug, PartialEq)]
pub struct ChildConversationEntry {
    pub conversation: ConversationEntry,
    pub keep_open: bool,
    pub recurrence: Option<Recurrence>,
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
        .open_table(CHILD_CONVERSATIONS)?
        .range((parent.get(), 0)..(parent.get(), before.map_or(u64::MAX, JobId::get)))?
        .rev()
        .take(limit.min(MAX_PAGE))
    {
        let child = JobId::new(entry?.0.value().1);
        let row = read_job_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted("child conversation missing"))?;
        let JobKind::Conversation { keep_open, .. } = row.kind else {
            return Err(StorageError::corrupted("child is not a conversation"));
        };
        let tip = rx
            .open_table(TIPS)?
            .get(child.get())?
            .map(|id| JobId::new(id.value()))
            .ok_or_else(|| StorageError::corrupted("child tip missing"))?;
        children.push(ChildConversationEntry {
            conversation: conversation_summary(&rx, child, tip)?,
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
    let rx = read(database)?;
    let recent = rx.open_table(RECENT)?;
    let mut conversations = Vec::new();
    let limit = limit.clamp(1, 100);
    for item in recent
        .range(..(before.map_or(u64::MAX, JobId::get), 0))?
        .rev()
        .take(limit + 1)
    {
        let (tip, conversation) = item?.0.value();
        let conversation = JobId::new(conversation);
        let tip = JobId::new(tip);
        conversations.push(conversation_summary(&rx, conversation, tip)?);
    }
    let has_more = conversations.len() > limit;
    conversations.truncate(limit);
    Ok(ConversationsPage {
        next_before: has_more
            .then(|| conversations.last().map(|entry| entry.tip_job))
            .flatten(),
        conversations,
        has_more,
    })
}

/// The number of Jobs in each readiness queue. A failed read is an error, so
/// a caller never reports a queue as empty when it could not be read.
pub(crate) fn queue_counts(database: &Database) -> Result<(u64, u64, u64), StorageError> {
    let rx = read(database)?;
    let pending = rx.open_table(PENDING)?.len()?;
    let ready = rx.open_table(READY)?.len()?;
    let delayed = rx.open_table(DELAYED)?.len()?;
    Ok((pending, ready, delayed))
}

pub(crate) fn read_tool_stats(
    database: &Database,
) -> Result<Vec<crate::persistence::ToolStatRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_STATS)?;
    let mut stats = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max) =
            value.value();
        stats.push(crate::persistence::ToolStatRecord {
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
const MAX_USAGE_BUCKET_ROWS: usize = 1_000;

fn resolve_model_name(
    rx: &ReadTransaction,
    provider: u64,
    model_hash: [u8; 32],
) -> Result<Option<String>, StorageError> {
    let scopes = rx.open_table(REQUEST_SCOPES)?;
    let start = (provider, model_hash, i64::MIN, 0, 0, 0);
    let end = (provider, model_hash, i64::MAX, u64::MAX, u32::MAX, u32::MAX);
    for entry in scopes.range(start..=end)? {
        let (key, _) = entry?;
        let (_provider, _hash, _obs, job, _req, _seq) = key.value();
        if let Some(job_row) = read_job_row(rx, JobId::new(job))? {
            let execution = turn_execution(&rx.open_table(JOBS)?, &job_row)?.0;
            return Ok(Some(execution.llm.config.model));
        }
    }
    Ok(None)
}

type UsageGroupKey = (i64, u64, u64, [u8; 32]);
type UsageCounters = (u64, u64, u64, u64, u64, u64, u64);

pub(crate) fn read_usage_summary(
    database: &Database,
    filter: &crate::persistence::UsageSummaryFilter,
) -> Result<crate::persistence::UsageSummaryResponse, StorageError> {
    use crate::persistence::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageSummaryResponse};

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
            resume_from_us: None,
            rows: Vec::new(),
        });
    }

    let UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        resume_from_us,
    } = scan_usage_totals(database, filter, from_hour, until_hour, None)?;
    let rows = rows.into_iter().map(|bucket| bucket.row).collect();

    let effective_from_us = requested_from_us
        .or_else(|| earliest_hour.map(|hour| hour.saturating_mul(HOUR)))
        .unwrap_or(0);

    let effective_until_us = match resume_from_us {
        Some(resume) => resume,
        None => match until_end_us {
            Some(until) => until,
            None => latest_hour
                .and_then(|hour| hour.checked_add(1))
                .map_or(0, |hour| hour.saturating_mul(HOUR)),
        },
    };

    Ok(UsageSummaryResponse {
        from_us: effective_from_us,
        until_us: effective_until_us,
        // Complete says the scan finished, not that every request's usage is
        // final. `resume_from_us` is set exactly when it did not finish.
        complete: resume_from_us.is_none(),
        resume_from_us,
        rows,
    })
}

pub(crate) fn read_usage_buckets(
    database: &Database,
    filter: &crate::persistence::UsageSummaryFilter,
    bucketing: &crate::persistence::UsageBucketing,
) -> Result<crate::persistence::UsageBucketResponse, StorageError> {
    use crate::persistence::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketResponse};

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
    )?;
    Ok(UsageBucketResponse {
        from_us,
        until_us: scan.resume_from_us.unwrap_or(until_us),
        complete: scan.resume_from_us.is_none(),
        resume_from_us: scan.resume_from_us,
        rows: scan.rows,
    })
}

#[derive(Default)]
struct UsageScan {
    rows: Vec<crate::persistence::UsageBucketRow>,
    earliest_hour: Option<i64>,
    latest_hour: Option<i64>,
    resume_from_us: Option<i64>,
}

fn scan_usage_totals(
    database: &Database,
    filter: &crate::persistence::UsageSummaryFilter,
    from_hour: i64,
    until_hour: i64,
    bucketing: Option<&crate::persistence::UsageBucketing>,
) -> Result<UsageScan, StorageError> {
    use crate::persistence::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketRow, UsageSummaryRow};

    let rx = read(database)?;
    let table = rx.open_table(USAGE_TOTALS)?;
    if from_hour > until_hour {
        return Ok(UsageScan::default());
    }
    let start = (from_hour, 0, 0, [0u8; 32]);
    let end = (until_hour, u64::MAX, u64::MAX, [0xffu8; 32]);

    let mut current_bucket: Option<i64> = None;
    let mut resume_from_us = None;
    let mut earliest_hour: Option<i64> = None;
    let mut latest_hour: Option<i64> = None;

    let mut grouped: std::collections::BTreeMap<UsageGroupKey, UsageCounters> =
        std::collections::BTreeMap::new();

    for (scanned_rows, entry) in table.range(start..=end)?.enumerate() {
        let (key, value) = entry?;
        let (hour, agent, provider, model_hash) = key.value();

        let bucket_us = match bucketing {
            Some(bucketing) => hour
                .checked_mul(HOUR)
                .and_then(|us| bucketing.start_of(us))
                .ok_or_else(|| StorageError::corrupted("usage bucket overflows"))?,
            None => hour,
        };
        if let Some(cur) = current_bucket {
            if bucket_us != cur {
                if scanned_rows >= MAX_USAGE_SCAN
                    || (bucketing.is_some() && grouped.len() >= MAX_USAGE_BUCKET_ROWS)
                {
                    resume_from_us = Some(match bucketing {
                        Some(_) => bucket_us,
                        None => hour.checked_mul(HOUR).ok_or_else(|| {
                            StorageError::corrupted("usage summary resume point overflows")
                        })?,
                    });
                    break;
                }
                current_bucket = Some(bucket_us);
            }
        } else {
            current_bucket = Some(bucket_us);
        }

        if earliest_hour.is_none() {
            earliest_hour = Some(hour);
        }
        latest_hour = Some(hour);

        if let Some(target_agent) = filter.agent
            && agent != target_agent
        {
            continue;
        }

        let (reqs, comp, inp, out, cr, cw, reas) = value.value();
        let slot = grouped
            .entry((
                if bucketing.is_some() { bucket_us } else { 0 },
                agent,
                provider,
                model_hash,
            ))
            .or_insert((0, 0, 0, 0, 0, 0, 0));
        slot.0 = slot.0.saturating_add(reqs);
        slot.1 = slot.1.saturating_add(comp);
        slot.2 = slot.2.saturating_add(inp);
        slot.3 = slot.3.saturating_add(out);
        slot.4 = slot.4.saturating_add(cr);
        slot.5 = slot.5.saturating_add(cw);
        slot.6 = slot.6.saturating_add(reas);
    }

    let mut model_cache: std::collections::HashMap<(u64, [u8; 32]), Option<String>> =
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
        resume_from_us,
    })
}

pub(crate) fn read_turn_execution(
    database: &Database,
    turn: JobId,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let rx = read(database)?;
    let row = read_job_row(&rx, turn)?.ok_or_else(|| StorageError::corrupted("turn missing"))?;
    turn_execution(&rx.open_table(JOBS)?, &row)
}

pub(crate) fn read_turn_entries(
    database: &Database,
    turn: JobId,
) -> Result<Vec<(u32, TurnEntry)>, StorageError> {
    let rx = read(database)?;
    turn_entries(&rx, turn)
}
fn turn_entries(rx: &ReadTransaction, turn: JobId) -> Result<Vec<(u32, TurnEntry)>, StorageError> {
    rx.open_table(TURN_INPUTS)?
        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().1, TurnEntry::decode(value.value())?))
        })
        .collect()
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
    let Some(row) = read_job_row(&rx, conversation)? else {
        return Ok(Vec::new());
    };
    if !row.kind.is_conversation() {
        return Ok(Vec::new());
    }
    let mut cursor = match before {
        Some(before) => read_job_row(&rx, before)?
            .filter(|row| row.parent_id == Some(conversation) && row.kind.is_stored())
            .and_then(|row| row.kind.previous()),
        None => rx
            .open_table(TIPS)?
            .get(conversation.get())?
            .map(|tip| JobId::new(tip.value())),
    };
    let mut turns = Vec::new();
    while let Some(turn) = cursor {
        if turns.len() >= limit.min(101) {
            break;
        }
        let row =
            read_job_row(&rx, turn)?.ok_or_else(|| StorageError::corrupted("missing turn"))?;
        cursor = row.kind.previous();
        turns.push(job_record(&rx, row)?);
    }
    Ok(turns)
}

pub(crate) fn read_conversation_limits(
    database: &Database,
    conversation: JobId,
) -> Result<Option<(ConversationLimits, u8)>, StorageError> {
    let rx = read(database)?;
    let mut current = conversation;
    let mut limits = ConversationLimits::CEILING;
    for depth in 0..=crate::persistence::MAX_CONVERSATION_DEPTH {
        let Some(row) = read_job_row(&rx, current)? else {
            return Ok(None);
        };
        if !row.kind.is_conversation() {
            return Ok(None);
        }
        let own = read_conversation(&rx, current)?.limits;
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
                .and_then(|output| output.summary)
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
                        Entry::RetryOf { .. } | Entry::ResponseFormat { .. }
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
                                crate::persistence::ToolOutcome::Uncertain { .. }
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
        Entry::ResponseFormat { .. } => String::new(),
    })
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
    };
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
            .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
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
    let mut used = 0usize;
    while let Some(id) = cursor {
        let row = read_job_row(&rx, id)?
            .ok_or_else(|| StorageError::corrupted("previous turn missing"))?;
        let mut text = String::new();
        let mut attachments = Vec::new();
        for (_, entry) in turn_entries(&rx, id)?.into_iter().filter(|_| {
            iteration_context
                .is_none_or(|(first, previous)| Some(id) != previous || id == first || id == turn)
        }) {
            if matches!(entry.entry, Entry::ResponseFormat { .. }) {
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
        let answer = if iteration_context
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
                .and_then(|output| output.summary)
        };
        let weight = (text.len() + answer.as_ref().map_or(0, String::len) + 16).div_ceil(3)
            + attachments.len() * 1600;
        if used.saturating_add(weight) > budget_tokens && id != turn {
            context.omitted = true;
            break;
        }
        used = used.saturating_add(weight);
        cursor = match iteration_context {
            Some((first, previous)) if id == turn => previous
                .filter(|previous| *previous != turn)
                .or((first != turn).then_some(first)),
            Some((first, _)) if id != first => Some(first),
            Some(_) => None,
            None => row.kind.previous(),
        };
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
        context.turns.push(ContextTurn {
            turn: id,
            text,
            attachments,
            attachment_bytes,
            answer,
        });
    }
    context.turns.reverse();
    if let Some(conversation) = row.parent_id.filter(|_| row.kind.is_stored()) {
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
                .range((parent.get(), 0)..=(parent.get(), u64::MAX))?
            {
                let child = JobId::new(entry?.0.value().1);
                let closing = read_conversation(&rx, child)?.closing;
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
    jobs: &impl ReadableTable<u64, &'static [u8]>,
    conversation: Option<JobId>,
) -> Result<bool, StorageError> {
    let mut current = conversation;
    for _ in 0..=3 {
        let Some(id) = current else {
            return Ok(false);
        };
        let Some(bytes) = jobs.get(id.get())? else {
            return Ok(false);
        };
        let row = JobRow::decode_at(id.get(), bytes.value())?;
        match row.kind {
            JobKind::Conversation {
                origin: Origin::User,
                ..
            } => return Ok(true),
            JobKind::Conversation { .. } => current = row.parent_id,
            JobKind::Turn { .. } => return Ok(false),
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
    for entry in rx
        .open_table(OPEN_APPROVALS)?
        .range((conversation.get(), 0, 0, 0)..=(conversation.get(), u64::MAX, u32::MAX, u32::MAX))?
    {
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
        let JobKind::Conversation { origin, .. } = row.kind else {
            return Err(StorageError::corrupted("expected a conversation"));
        };
        let parent = row.parent_id.map(|id| id.to_string());
        let metadata = read_conversation(&rx, conversation)?;
        if origin != Origin::System {
            for entry in rx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u64::MAX, u32::MAX, u32::MAX),
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
                let mut card = json!({"kind": if matches!(call.decision.fact, crate::persistence::ToolDecision::AwaitingAnswer(_)) { "question" } else { "approval" }, "conversation": conversation.to_string(), "parent":parent, "approval":[turn,request,index], "tool":call.name});
                if matches!(
                    call.decision.fact,
                    crate::persistence::ToolDecision::AwaitingAnswer(_)
                ) {
                    card["question"] = json!(question(arguments));
                } else {
                    card["arguments"] = json!(arguments);
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
        if let Some(active) = rx.open_table(ACTIVE_TURNS)?.get(conversation.get())? {
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
            .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
        {
            pending.push(JobId::new(entry?.0.value().1));
        }
    }
    Ok(cards)
}

pub(crate) struct UsageRetriesPage {
    pub rows: Vec<crate::persistence::UsageRetryRow>,
    pub next: Option<(u64, u64)>,
}

/// Scans at most `limit` retry keys, including those outside the time filter.
/// The cursor advances over scanned keys even when the page has no matches.
pub(crate) fn read_usage_retries(
    database: &Database,
    filter: &crate::persistence::UsageSummaryFilter,
    bucketing: crate::persistence::UsageBucketing,
    after: Option<(u64, u64)>,
    limit: usize,
) -> Result<UsageRetriesPage, StorageError> {
    let rx = read(database)?;
    let retries = rx.open_table(RETRIES)?;
    let jobs = rx.open_table(JOBS)?;
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
        let execution = turn_execution(&jobs, &original)?.0;
        let bucket = bucketing
            .start_of(at)
            .ok_or_else(|| StorageError::corrupted("retry bucket overflows"))?;
        let model = execution.llm.config.model;
        let hash = *ContentDigest::of(model.as_bytes()).as_bytes();
        let slot = grouped
            .entry((bucket, agent, execution.llm.provider.id.get(), hash))
            .or_insert((model, 0));
        slot.1 += 1;
    }
    let more = entries.next().transpose()?.is_some();
    Ok(UsageRetriesPage {
        rows: grouped
            .into_iter()
            .map(|((bucket_us, agent, provider, _), (model, retries))| {
                crate::persistence::UsageRetryRow {
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
