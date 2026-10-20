use std::ops::Bound;

use redb::ReadableTableMetadata;

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

pub(super) fn turn_execution(
    jobs: &impl ReadableTable<u64, &'static [u8]>,
    row: &JobRow,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let JobKind::Turn {
        execution,
        plan_source,
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
            let owner = JobRow::decode(bytes.value())?;
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
        PlanSource::Stored => {
            let conversation = row
                .parent_id
                .ok_or_else(|| StorageError::corrupted("stored turn has no conversation"))?;
            let bytes = jobs
                .get(conversation.get())?
                .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
            match JobRow::decode(bytes.value())?.kind {
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
        stop_confirmed_by: state.stop_confirmed_by,
        created_at: row.created_at,
        updated_at,
        input: JobInputId::new(id.get()),
        parent: row.parent_id.unwrap_or(JobId::new(0)),
        parent_id: row.parent_id,
        conversation: row.kind.is_stored().then_some(row.parent_id).flatten(),
        kind: row.kind,
        objective: row.objective,
        attachments: row.attachments,
        dependencies: row.dependencies,
        submission: row.submission,
        configuration,
        assigned_executor: state.claim.as_ref().map(|claim| claim.pool_id.into()),
        previous: row.previous,
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
    if read_job_row(&rx, job)?.is_some_and(|row| row.kind.is_conversation()) {
        if after_seq.is_some_and(|seq| seq >= 1) || limit == 0 {
            return Ok(Vec::new());
        }
        return Ok(conversation_result(&rx, job)?.into_iter().collect());
    }
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
        objective: row.objective,
        submitted_at: Some(row.created_at),
        created_at: row.created_at,
        updated_at: row.created_at,
        configuration: execution,
        attachments: row.attachments,
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
        let row = read_job_row(&rx, conversation)?
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
                    Some(first) => rx
                        .open_table(TURN_INPUTS)?
                        .range((first, 0)..=(first, u32::MAX))?
                        .next()
                        .transpose()?
                        .map(|entry| TurnEntry::decode(entry.1.value()))
                        .transpose()?
                        .map(|entry| match entry.entry {
                            Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } => {
                                text.chars().take(80).collect()
                            }
                            _ => String::new(),
                        })
                        .unwrap_or_default(),
                    None => String::new(),
                }
            }
        };
        let state = read_state(&rx, conversation)?;
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
        let tip_state = read_state(&rx, tip)?;
        let tip_row =
            read_job_row(&rx, tip)?.ok_or_else(|| StorageError::corrupted("missing tip"))?;
        conversations.push(ConversationEntry {
            root_job: conversation,
            tip_job: tip,
            agent,
            project,
            title,
            state: if state.outcome.is_some() {
                "settled"
            } else if state.children_closed_at.is_some() {
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
        });
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

type UsageGroupKey = (u64, u64, [u8; 32]);
type UsageCounters = (u64, u64, u64, u64, u64, u64, u64);

pub(crate) fn read_usage_summary(
    database: &Database,
    filter: &crate::persistence::UsageSummaryFilter,
) -> Result<crate::persistence::UsageSummaryResponse, StorageError> {
    use crate::persistence::{
        USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageSummaryResponse, UsageSummaryRow,
    };

    let rx = read(database)?;
    let table = rx.open_table(USAGE_TOTALS)?;
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

    let start = (from_hour, 0, 0, [0u8; 32]);
    let end = (until_hour, u64::MAX, u64::MAX, [0xffu8; 32]);

    let mut current_hour: Option<i64> = None;
    let mut resume_from_us = None;
    let mut earliest_hour: Option<i64> = None;
    let mut latest_hour: Option<i64> = None;

    let mut grouped: std::collections::BTreeMap<UsageGroupKey, UsageCounters> =
        std::collections::BTreeMap::new();

    for (scanned_rows, entry) in table.range(start..=end)?.enumerate() {
        let (key, value) = entry?;
        let (hour, agent, provider, model_hash) = key.value();

        if let Some(cur) = current_hour {
            if hour != cur {
                if scanned_rows >= MAX_USAGE_SCAN {
                    resume_from_us = Some(hour.checked_mul(HOUR).ok_or_else(|| {
                        StorageError::corrupted("usage summary resume point overflows")
                    })?);
                    break;
                }
                current_hour = Some(hour);
            }
        } else {
            current_hour = Some(hour);
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
            .entry((agent, provider, model_hash))
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

    for ((agent, provider, model_hash), (reqs, comp, inp, out, cr, cw, reas)) in grouped {
        let model = match model_cache.entry((provider, model_hash)) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resolved = resolve_model_name(&rx, provider, model_hash)?;
                e.insert(resolved.clone());
                resolved
            }
        };
        rows.push(UsageSummaryRow {
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
        });
    }

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
            .and_then(|row| row.previous),
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
        cursor = row.previous;
        turns.push(job_record(&rx, row)?);
    }
    Ok(turns)
}

#[derive(Clone, Debug)]
pub struct ContextTurn {
    pub turn: JobId,
    pub text: String,
    pub attachments: Vec<AttachmentId>,
    pub answer: Option<String>,
}
#[derive(Clone, Debug)]
pub struct TurnContext {
    pub turns: Vec<ContextTurn>,
    pub omitted: bool,
    pub attention: String,
    pub tree_status: String,
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
            format!("{child}: {event:?}; answer from {reference}: {answer}")
        }
        Entry::RetryOf { turn, .. } => {
            let mut text = format!(
                "Retry {turn}. Its started tool calls below are uncertain; do not replay them automatically."
            );
            for (_, original) in turn_entries(rx, *turn)? {
                if !matches!(original.entry, Entry::RetryOf { .. }) {
                    text.push('\n');
                    text.push_str(&entry_text(rx, &original.entry)?);
                }
            }
            for call in job_tool_evidence(&rx.open_table(TOOL_CALLS)?, *turn)? {
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
            text
        }
        Entry::Decision { approval } => {
            format!("Decision for {}:{}/{}", approval.0, approval.1, approval.2)
        }
        Entry::Iteration { n } => format!("Iteration {n}"),
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
    };
    let mut cursor = Some(turn);
    let mut used = 0usize;
    while let Some(id) = cursor {
        let row = read_job_row(&rx, id)?
            .ok_or_else(|| StorageError::corrupted("previous turn missing"))?;
        let mut text = String::new();
        let mut attachments = Vec::new();
        for (_, entry) in turn_entries(&rx, id)? {
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
        let answer = if state
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
        cursor = row.previous;
        context.turns.push(ContextTurn {
            turn: id,
            text,
            attachments,
            answer,
        });
    }
    context.turns.reverse();
    if let Some(conversation) = row.parent_id.filter(|_| row.kind.is_stored()) {
        let state = read_state(&rx, turn)?;
        let through = state.claim.map_or(0, |claim| claim.notice_seq);
        let mut previous = row.previous;
        let mut after = 0;
        while let Some(id) = previous {
            if let Some(claim) = read_state(&rx, id)?.claim {
                after = claim.notice_seq;
                break;
            }
            previous = read_job_row(&rx, id)?
                .ok_or_else(|| StorageError::corrupted("previous turn missing"))?
                .previous;
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
                let state = read_state(&rx, child)?;
                status_rows.push(format!(
                    "{child}: {}",
                    if state.children_closed_at.is_some() {
                        "closing"
                    } else {
                        "open"
                    }
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
