use std::ops::Bound;

use super::*;
use crate::persistence::{
    ConfigBody, ConfigId, ConfigKind, ConfigRecord, ExecutionConfiguration, ExecutionOperation,
    ExecutionPoolConfig, FrozenPoolBinding, JobInputRecord, OutcomeKind, SubmittedConfiguration,
};

// ---- typed reads ---------------------------------------------------------------

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

fn read_job_row(rx: &ReadTransaction, id: JobId) -> Result<Option<JobRow>, StorageError> {
    rx.open_table(JOBS)?
        .get(id.get())?
        .map(|bytes| JobRow::decode(bytes.value()))
        .transpose()
}

fn read_state(rx: &ReadTransaction, id: JobId) -> Result<JobStateRow, StorageError> {
    rx.open_table(JOB_STATES)?
        .get(id.get())?
        .map(|bytes| JobStateRow::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no job_state")))
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
    let mut projects = Vec::new();
    for entry in rx
        .open_table(JOB_PROJECTS)?
        .range((id.get(), 0)..=(id.get(), u64::MAX))?
    {
        projects.push(ProjectId::new(entry?.0.value().1));
    }
    let subtree_settled = rx.open_table(UNSETTLED)?.get(id.get())?.is_none();
    let configuration = row
        .execution
        .as_ref()
        .map(|execution| ExecutionConfiguration {
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
    Ok(JobRecord {
        id,
        name: row.name,
        eligible_at: row.eligible_at,
        started_at: state.claim.as_ref().map(|claim| claim.started_at),
        completed_at: at(OutcomeKind::Completed),
        failed_at: at(OutcomeKind::Failed),
        cancelled_at: at(OutcomeKind::Cancelled),
        unknown_at: at(OutcomeKind::Unknown),
        stop_confirmed_at: state.stop_confirmed_at,
        created_at: row.created_at,
        updated_at,
        input: JobInputId::new(id.get()),
        parent: row.parent_id.unwrap_or(JobId::new(0)),
        parent_id: row.parent_id,
        agent_id: row.agent_id,
        execution: row.execution,
        objective: row.objective,
        attachments: row.attachments,
        dependencies: row.dependencies,
        max_depth: row.max_depth,
        max_children: row.max_children,
        max_descendants: row.max_descendants,
        retry_of: row.retry_of,
        continues: row.continues,
        submission: row.submission,
        projects,
        configuration,
        assigned_executor: state.claim.as_ref().map(|claim| claim.pool_id.into()),
        previous: row.continues,
        claim: state.claim,
        suspension: state.suspension,
        outcome: state.outcome,
        children_closed_at: state.children_closed_at,
        subtree_settled,
    })
}

pub(crate) fn read_job(database: &Database, id: JobId) -> Result<Option<JobRecord>, StorageError> {
    let rx = read(database)?;
    read_job_row(&rx, id)?
        .map(|row| job_record(&rx, row))
        .transpose()
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
        records.push(job_record(&rx, JobRow::decode(entry?.1.value())?)?);
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
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = after_seq.map_or(0, |seq| {
        u32::try_from(seq).map_or(0, |seq| seq.saturating_add(1))
    });
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
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    outputs
        .range((job.get(), 0)..=(job.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()
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
fn input_of(row: JobRow) -> JobInputRecord {
    JobInputRecord {
        id: JobInputId::new(row.id.get()),
        objective: row.objective,
        max_depth: row.max_depth,
        max_children_per_job: row.max_children,
        max_total_descendants: row.max_descendants,
        submitted_at: Some(row.created_at),
        created_at: row.created_at,
        updated_at: row.created_at,
        configuration: row.execution,
        attachments: row.attachments,
    }
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
    Ok(read_job_row(&rx, job)?.map(input_of))
}

pub(crate) fn read_job_configuration(
    database: &Database,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    let rx = read(database)?;
    let Some(execution) = read_job_row(&rx, job)?.and_then(|row| row.execution) else {
        return Ok(None);
    };
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
        .map(|bytes| decode_attachment(bytes.value()))
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
        records.push(decode_attachment(entry?.1.value())?);
    }
    Ok(records)
}

pub(crate) fn content_available(
    database: &Database,
    digest: ContentDigest,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    for entry in rx.open_table(ATTACHMENTS)?.iter()? {
        let record = decode_attachment(entry?.1.value())?;
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
        let record = decode_attachment(entry?.1.value())?;
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
            Some(bytes) => decode_attachment(bytes.value())?,
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

pub(crate) fn read_project_jobs(
    database: &Database,
    project: ProjectId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(PROJECT_MEMBERS)?;
    let start = after.map_or(0, |after| after.get().saturating_add(1));
    let mut jobs = Vec::new();
    for entry in table
        .range((project.get(), start)..=(project.get(), u64::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        jobs.push(JobId::new(entry?.0.value().1));
    }
    Ok(jobs)
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
        .map(|bytes| ConfigRow::decode(bytes.value()).map(config_record))
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
        let row = ConfigRow::decode(entry?.1.value())?;
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
        .map(|bytes| ConfigRow::decode(bytes.value()))
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
        if let Some(pool) = pool_view(ConfigRow::decode(entry?.1.value())?) {
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
        .map(|bytes| ConfigRow::decode(bytes.value()))
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
        if let Some(record) = executor_view(ConfigRow::decode(entry?.1.value())?) {
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
        let row = ConfigRow::decode(entry?.1.value())?;
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
        .map(|bytes| ConfigRow::decode(bytes.value()))
        .transpose()?
    else {
        return Ok(Vec::new());
    };
    let ConfigBody::Model(model) = model.body else {
        return Ok(Vec::new());
    };
    let Some(connection) = configs
        .get(model.connection_id.get())?
        .map(|bytes| ConfigRow::decode(bytes.value()))
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
    let Some(execution) = read_job_row(&rx, job)?.and_then(|row| row.execution) else {
        return Ok(Vec::new());
    };
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
    let state = JobStateRow::decode(bytes.value())?;
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
            .map(|bytes| JobStateRow::decode(bytes.value()))
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
            .map(|bytes| JobStateRow::decode(bytes.value()))
            .transpose()?
            .is_some_and(|state| state.outcome.is_some()),
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationEntry {
    pub root_job: JobId,
    pub tip_job: JobId,
    pub agent: ConfigId,
    pub title: String,
    pub turn_count: u32,
    pub tip_settlement_state: String,
    pub tip_created_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationsPage {
    pub conversations: Vec<ConversationEntry>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

pub(crate) fn read_conversations(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    let rx = read(database)?;
    let threads_table = rx.open_table(CHAT_THREADS)?;
    let successors_table = rx.open_table(SUCCESSORS)?;

    struct ThreadInfo {
        root_job: JobId,
        tip_job: JobId,
        agent: ConfigId,
        turn_count: u32,
    }

    let mut threads = Vec::new();
    for entry in threads_table.iter()? {
        let (key, _) = entry?;
        let (agent_id, root_id) = key.value();
        let root_job = JobId::new(root_id);
        let agent = ConfigId::new(agent_id);
        let mut tip = root_id;
        let mut turn_count = 1u32;
        while let Some(succ) = successors_table.get(tip)? {
            tip = succ.value();
            turn_count += 1;
        }
        let tip_job = JobId::new(tip);
        if let Some(before_id) = before
            && tip_job >= before_id
        {
            continue;
        }
        threads.push(ThreadInfo {
            root_job,
            tip_job,
            agent,
            turn_count,
        });
    }

    // Newest activity first: sort descending by tip_job
    threads.sort_by_key(|thread| std::cmp::Reverse(thread.tip_job));

    let limit = limit.clamp(1, 100);
    let has_more = threads.len() > limit;
    let slice = if has_more {
        &threads[..limit]
    } else {
        &threads[..]
    };
    let next_before = if has_more {
        slice.last().map(|t| t.tip_job)
    } else {
        None
    };

    let mut conversations = Vec::with_capacity(slice.len());
    for t in slice {
        let root_row = read_job_row(&rx, t.root_job)?;
        let title = match root_row.as_ref().and_then(|r| r.objective.as_deref()) {
            Some(obj) => match obj.char_indices().nth(80) {
                Some((idx, _)) => obj[..idx].to_string(),
                None => obj.to_string(),
            },
            None => String::new(),
        };

        let tip_row = read_job_row(&rx, t.tip_job)?;
        let tip_created_at = tip_row
            .as_ref()
            .map(|r| r.created_at)
            .unwrap_or_else(|| Timestamp::from_unix_micros(0));

        let tip_state = read_state(&rx, t.tip_job)?;
        let tip_settlement_state = match tip_state.outcome.as_ref() {
            None => "unsettled".to_string(),
            Some(outcome) => match outcome.kind {
                OutcomeKind::Completed => "completed".to_string(),
                OutcomeKind::Failed => "failed".to_string(),
                OutcomeKind::Cancelled => "cancelled".to_string(),
                OutcomeKind::Unknown => "unknown".to_string(),
            },
        };

        conversations.push(ConversationEntry {
            root_job: t.root_job,
            tip_job: t.tip_job,
            agent: t.agent,
            title,
            turn_count: t.turn_count,
            tip_settlement_state,
            tip_created_at,
        });
    }

    Ok(ConversationsPage {
        conversations,
        next_before,
        has_more,
    })
}
