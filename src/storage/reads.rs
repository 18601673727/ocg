use std::ops::Bound;

use super::*;

// ---- typed reads ---------------------------------------------------------------

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

pub(super) fn read_link(
    rx: &ReadTransaction,
    table: Link,
    from: u64,
) -> Result<Option<u64>, StorageError> {
    Ok(rx.open_table(table)?.get(from)?.map(|v| v.value()))
}

pub(super) fn read_pairs(
    rx: &ReadTransaction,
    table: PairSet,
    from: u64,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<u64>, StorageError> {
    pair_targets(&rx.open_table(table)?, from, after, limit)
}

pub(super) fn job_record(rx: &ReadTransaction, row: JobRow) -> Result<JobRecord, StorageError> {
    let id = row.id.get();
    let input = read_link(rx, HAS_INPUT, id)?
        .map(JobInputId::new)
        .ok_or_else(|| StorageError::corrupted(format!("{} has no input", row.id)))?;
    let parent = read_link(rx, CHILD_OF, id)?
        .map(JobId::new)
        .ok_or_else(|| StorageError::corrupted(format!("{} has no parent", row.id)))?;
    let projects = read_pairs(rx, IN_PROJECT, id, None, usize::MAX)?
        .into_iter()
        .map(ProjectId::new)
        .collect();
    let configuration = match (
        read_link(rx, APPLIES_PROFILE, id)?,
        read_link(rx, RUNS_LLM_PROFILE, id)?,
    ) {
        (agent_profile, Some(llm_profile)) => Some(ExecutionConfiguration {
            agent_profile: agent_profile.map(ProfileId::new),
            llm_profile: ProfileId::new(llm_profile),
        }),
        (None, None) => None,
        (Some(_), None) => {
            return Err(StorageError::corrupted(format!(
                "{} applies an Agent Profile without an LLM Profile",
                row.id
            )));
        }
    };
    Ok(JobRecord {
        id: row.id,
        name: row.name,
        execution_mode: row.execution_mode,
        eligible_at: row.eligible_at,
        started_at: row.started_at,
        paused_at: row.paused_at,
        sterilized_at: row.sterilized_at,
        completed_at: row.completed_at,
        failed_at: row.failed_at,
        cancelled_at: row.cancelled_at,
        unknown_at: row.unknown_at,
        stop_confirmed_at: row.stop_confirmed_at,
        next_due_at: row.next_due_at,
        next_iteration_no: row.next_iteration_no,
        iteration_no: row.iteration_no,
        scheduled_at: row.scheduled_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        input,
        parent,
        projects,
        configuration,
        assigned_executor: read_link(rx, ASSIGNED_TO, id)?.map(ExecutorId::new),
        previous: read_link(rx, PREVIOUS, id)?.map(JobId::new),
        replaces: read_link(rx, REPLACES, id)?.map(JobId::new),
        retry_of: read_link(rx, RETRY_OF, id)?.map(JobId::new),
    })
}

pub(crate) fn read_job(database: &Database, id: JobId) -> Result<Option<JobRecord>, StorageError> {
    let rx = read(database)?;
    let row = rx
        .open_table(JOBS)?
        .get(id.get())?
        .map(|v| JobRow::decode(v.value()))
        .transpose()?;
    row.map(|row| job_record(&rx, row)).transpose()
}

/// Newest Jobs first; IDs are allocated in creation order.
pub(crate) fn read_jobs(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobRecord>, StorageError> {
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u64::MAX, |id| id.get());
    let mut records = Vec::new();
    for entry in jobs.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let row = JobRow::decode(entry?.1.value())?;
        records.push(job_record(&rx, row)?);
    }
    Ok(records)
}

/// Up to `limit` entries of [`PENDING_JOBS`] in key order, strictly after
/// `after` when given, never more than [`MAX_PAGE`]. Every entry examined
/// counts against the limit, so the cost of a call is bounded by it however
/// many Jobs the table holds. An entry must reference a Job row that is
/// still pending and carries that very key; anything else is corruption and
/// fails the read rather than being skipped.
pub(crate) fn read_pending_jobs(
    database: &Database,
    after: Option<PendingJobKey>,
    limit: usize,
) -> Result<PendingJobsPage, StorageError> {
    let rx = read(database)?;
    let genesis = genesis_id(&rx.open_table(META)?)?;
    let index = rx.open_table(PENDING_JOBS)?;
    let jobs = rx.open_table(JOBS)?;
    let start = after.map_or(Bound::Unbounded, |key| {
        Bound::Excluded((key.created_at.as_unix_micros(), key.job.get()))
    });
    let limit = limit.min(MAX_PAGE);
    let mut entries = index.range((start, Bound::Unbounded))?;
    let mut visited = Vec::new();
    let mut exhausted = false;
    while visited.len() < limit {
        let Some(entry) = entries.next() else {
            exhausted = true;
            break;
        };
        let key = entry?.0.value();
        let (created_at, job) = (Timestamp::from_unix_micros(key.0), JobId::new(key.1));
        let row = jobs
            .get(key.1)?
            .map(|v| JobRow::decode(v.value()))
            .transpose()?
            .ok_or_else(|| {
                StorageError::corrupted(format!("PENDING_JOBS names the missing Job {job}"))
            })?;
        if row.pending_key() != key || row.id == genesis || !row.awaits_start() {
            return Err(StorageError::corrupted(format!(
                "PENDING_JOBS names {job}, which is not a pending Job created at {created_at:?}"
            )));
        }
        visited.push(PendingJobKey { created_at, job });
    }
    let last_visited = visited.last().copied();
    Ok(PendingJobsPage {
        visited,
        last_visited,
        exhausted,
    })
}

/// A Job's ordinary children in ID order, strictly after `after` when given.
/// The Genesis Job is never its own child.
pub(crate) fn read_children(
    database: &Database,
    parent: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let children = read_pairs(
        &rx,
        CHILDREN,
        parent.get(),
        after.map(JobId::get),
        limit.min(MAX_PAGE),
    )?;
    Ok(children.into_iter().map(JobId::new).collect())
}

/// The Jobs submitted with an input, in ID order: the Job that froze it and
/// every retry that reused it.
pub(crate) fn read_input_jobs(
    database: &Database,
    input: JobInputId,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    Ok(read_pairs(&rx, INPUT_JOBS, input.get(), None, MAX_PAGE)?
        .into_iter()
        .map(JobId::new)
        .collect())
}

/// The Jobs that explicitly retry `original`, in creation order.
pub(crate) fn read_retries(
    database: &Database,
    original: JobId,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    Ok(read_pairs(&rx, RETRIES, original.get(), None, MAX_PAGE)?
        .into_iter()
        .map(JobId::new)
        .collect())
}

/// A Job's outputs in seq order, after `after_seq` when given.
pub(crate) fn read_outputs(
    database: &Database,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    let index = rx.open_table(HAS_OUTPUT)?;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = after_seq.map_or(0, |seq| {
        u64::try_from(seq).map_or(0, |seq| seq.saturating_add(1))
    });
    let mut records = Vec::new();
    for entry in index
        .range((job.get(), start)..=(job.get(), u64::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        let output = entry?.1.value();
        let bytes = outputs.get(output)?.ok_or_else(|| {
            StorageError::corrupted(format!("HAS_OUTPUT names missing output {output}"))
        })?;
        records.push(decode_output(bytes.value())?);
    }
    Ok(records)
}

/// A Job's tool evidence in call order, after the call `after` names by its
/// request and index when given.
pub(crate) fn read_tool_evidence(
    database: &Database,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_EVIDENCE)?;
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

pub(crate) fn read_latest_output(
    database: &Database,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    let Some(output) = read_link(&rx, LATEST_OUTPUT, job.get())? else {
        return Ok(None);
    };
    let bytes = rx.open_table(JOB_OUTPUTS)?.get(output)?.ok_or_else(|| {
        StorageError::corrupted(format!("LATEST_OUTPUT names missing output {output}"))
    })?;
    decode_output(bytes.value()).map(Some)
}

pub(crate) fn read_marker(
    database: &Database,
    operation: OperationId,
) -> Result<Option<OperationMarker>, StorageError> {
    let rx = read(database)?;
    let t = rx.open_table(OPERATIONS)?;
    let bytes = t.get((operation.epoch, operation.seq))?;
    bytes
        .map(|b| decode_marker(operation, b.value()))
        .transpose()
}

/// Every unsettled local execution claimed by an opening before `epoch`,
/// with the epoch that claimed it, in Job order. Settled Jobs leave no
/// claim, so this reads only work that an earlier opening left unsettled.
pub(crate) fn abandoned_local_executions(
    database: &Database,
    epoch: u64,
) -> Result<Vec<(JobId, u64)>, StorageError> {
    let rx = read(database)?;
    let t = rx.open_table(LOCAL_EXECUTIONS)?;
    let mut abandoned = Vec::new();
    for entry in t.iter()? {
        let (job, claimed) = entry?;
        if claimed.value() < epoch {
            abandoned.push((JobId::new(job.value()), claimed.value()));
        }
    }
    Ok(abandoned)
}

/// Markers in commit-identifier order, after `after` when given.
pub(crate) fn read_markers(
    database: &Database,
    after: Option<OperationId>,
    limit: usize,
) -> Result<Vec<OperationMarker>, StorageError> {
    let rx = read(database)?;
    let t = rx.open_table(OPERATIONS)?;
    let start = match after {
        Some(op) if op.seq == u64::MAX => (op.epoch.saturating_add(1), 0),
        Some(op) => (op.epoch, op.seq + 1),
        None => (0, 0),
    };
    let mut markers = Vec::new();
    for entry in t.range(start..)?.take(limit.min(MAX_PAGE)) {
        let (key, value) = entry?;
        let (epoch, seq) = key.value();
        markers.push(decode_marker(OperationId::new(epoch, seq), value.value())?);
    }
    Ok(markers)
}

pub(crate) fn read_job_input(
    database: &Database,
    id: JobInputId,
) -> Result<Option<JobInputRecord>, StorageError> {
    let rx = read(database)?;
    let Some(row) = rx
        .open_table(JOB_INPUTS)?
        .get(id.get())?
        .map(|v| JobInputRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    job_input_record(&rx, row).map(Some)
}

/// The record of an input, with its Attachments in order. Each must exist.
pub(super) fn job_input_record(
    rx: &ReadTransaction,
    row: JobInputRow,
) -> Result<JobInputRecord, StorageError> {
    let input = row.id.get();
    let attachment_table = rx.open_table(ATTACHMENTS)?;
    let mut attachments = Vec::new();
    for entry in rx
        .open_table(HAS_ATTACHMENT)?
        .range((input, 0)..=(input, u32::MAX))?
    {
        let (key, attachment) = entry?;
        let (position, attachment) = (key.value().1, attachment.value());
        if usize::try_from(position).ok() != Some(attachments.len()) {
            return Err(StorageError::corrupted(format!(
                "the Attachments of {} are not numbered from 0 without gaps",
                row.id
            )));
        }
        if attachment_table.get(attachment)?.is_none() {
            return Err(StorageError::corrupted(format!(
                "{} names missing attachment {attachment}",
                row.id
            )));
        }
        attachments.push(AttachmentId::new(attachment));
    }
    Ok(JobInputRecord {
        id: row.id,
        submission_id: row.submission_id,
        objective: row.objective,
        max_depth: row.max_depth,
        max_children_per_job: row.max_children_per_job,
        max_total_descendants: row.max_total_descendants,
        submitted_at: row.submitted_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        configuration: row.configuration,
        attachments,
    })
}

/// A Job's input.
pub(crate) fn read_job_input_for_job(
    database: &Database,
    job: JobId,
) -> Result<Option<JobInputRecord>, StorageError> {
    let rx = read(database)?;
    let Some(input) = read_link(&rx, HAS_INPUT, job.get())? else {
        return Ok(None);
    };
    let bytes = rx
        .open_table(JOB_INPUTS)?
        .get(input)?
        .ok_or_else(|| StorageError::corrupted(format!("HAS_INPUT names missing input {input}")))?;
    let row = JobInputRow::decode(bytes.value())?;
    job_input_record(&rx, row).map(Some)
}

/// The submission recorded under `key`, if a Job was ever created with it.
pub(crate) fn read_submission(
    database: &Database,
    key: SubmissionKey,
) -> Result<Option<SubmissionRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(SUBMISSIONS)?
        .get(key.as_bytes().as_slice())?
        .map(|v| decode_submission(key.as_bytes(), v.value()))
        .transpose()
}

pub(crate) fn read_attachment(
    database: &Database,
    id: AttachmentId,
) -> Result<Option<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(ATTACHMENTS)?
        .get(id.get())?
        .map(|v| decode_attachment(v.value()))
        .transpose()
}

/// Attachments in reverse ID order; newest first. Strictly before `before`
/// when given; at most 1,000 per call.
pub(crate) fn read_attachments(
    database: &Database,
    before: Option<AttachmentId>,
    limit: usize,
) -> Result<Vec<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    newest(
        &rx,
        ATTACHMENTS,
        before.map(AttachmentId::get),
        limit,
        decode_attachment,
    )
}

/// Whether an available Attachment names the content `digest`. Every
/// Attachment record is read: they have no index by content.
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

/// The content that cleaned-up Attachments name and no available one does:
/// each digest once, with its recorded length, in digest order. Every
/// Attachment record is read.
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

/// Records `cleanup` on an Attachment directly, so a test can choose its
/// kind and time, without the cleanup operation's marker or reclamation:
/// it touches nothing else, its content file included.
#[cfg(test)]
pub(crate) fn record_attachment_cleanup(
    database: &Database,
    id: AttachmentId,
    cleanup: AttachmentCleanup,
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

/// The inputs that carry an Attachment, oldest first, at most 1,000.
pub(crate) fn read_attachment_inputs(
    database: &Database,
    attachment: AttachmentId,
) -> Result<Vec<JobInputId>, StorageError> {
    let rx = read(database)?;
    Ok(
        read_pairs(&rx, ATTACHMENT_INPUTS, attachment.get(), None, MAX_PAGE)?
            .into_iter()
            .map(JobInputId::new)
            .collect(),
    )
}

pub(super) fn executor_record(row: ExecutorRow) -> ExecutorRecord {
    ExecutorRecord {
        id: row.id,
        name: row.name,
        executor_type: row.executor_type,
        concurrency_limit: row.concurrency_limit,
        latest_enabled_at: row.latest_enabled_at,
        latest_disabled_at: row.latest_disabled_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        llm_transports: row.llm_transports,
    }
}

/// A single executor.
pub(crate) fn read_executor(
    database: &Database,
    id: ExecutorId,
) -> Result<Option<ExecutorRecord>, StorageError> {
    let rx = read(database)?;
    let row = rx
        .open_table(EXECUTORS)?
        .get(id.get())?
        .map(|v| ExecutorRow::decode(v.value()))
        .transpose()?;
    Ok(row.map(executor_record))
}

/// Records of `table` in reverse ID order, newest first, strictly before
/// `before` when given.
pub(super) fn newest<T>(
    rx: &ReadTransaction,
    table: Records,
    before: Option<u64>,
    limit: usize,
    decode: impl Fn(&[u8]) -> DecodeResult<T>,
) -> Result<Vec<T>, StorageError> {
    let records = rx.open_table(table)?;
    let end = before.unwrap_or(u64::MAX);
    let mut decoded = Vec::new();
    for entry in records.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        decoded.push(decode(entry?.1.value())?);
    }
    Ok(decoded)
}

/// Executors in reverse ID order; newest first. Strictly before `before` when given;
/// at most 1,000 per call.
pub(crate) fn read_executors(
    database: &Database,
    before: Option<ExecutorId>,
    limit: usize,
) -> Result<Vec<ExecutorRecord>, StorageError> {
    let rx = read(database)?;
    let rows = newest(
        &rx,
        EXECUTORS,
        before.map(ExecutorId::get),
        limit,
        ExecutorRow::decode,
    )?;
    Ok(rows.into_iter().map(executor_record).collect())
}

/// The Executors whose adapter supports `transport`, in ID order, at most
/// [`MAX_PAGE`]. Every one is an Llm Executor.
pub(super) fn executors_supporting(
    rx: &ReadTransaction,
    transport: LlmTransport,
) -> Result<Vec<ExecutorId>, StorageError> {
    let code = transport.code();
    let mut executors = Vec::new();
    for entry in rx
        .open_table(TRANSPORT_EXECUTORS)?
        .range((code, 0)..=(code, u64::MAX))?
        .take(MAX_PAGE)
    {
        executors.push(ExecutorId::new(entry?.0.value().1));
    }
    Ok(executors)
}

/// The transport of an LLM Profile's Provider as configured now; `None` for
/// a missing Profile or an Agent Profile.
pub(super) fn current_transport(
    rx: &ReadTransaction,
    profile: ProfileId,
) -> Result<Option<LlmTransport>, StorageError> {
    let Some(row) = rx
        .open_table(PROFILES)?
        .get(profile.get())?
        .map(|v| ProfileRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    if row.config.kind() != ProfileKind::Llm {
        return Ok(None);
    }
    let provider = read_link(rx, USES_PROVIDER, profile.get())?
        .ok_or_else(|| StorageError::corrupted(format!("{profile} has no Provider")))?;
    let provider = rx
        .open_table(LLM_PROVIDERS)?
        .get(provider)?
        .map(|v| decode_llm_provider(v.value()))
        .transpose()?
        .ok_or_else(|| {
            StorageError::corrupted(format!("{profile} names missing Provider {provider}"))
        })?;
    Ok(Some(provider.transport))
}

/// The Executors that can run an LLM Profile as it is configured now, in ID
/// order: those supporting its Provider's transport.
pub(crate) fn read_compatible_executors(
    database: &Database,
    llm_profile: ProfileId,
) -> Result<Vec<ExecutorId>, StorageError> {
    let rx = read(database)?;
    match current_transport(&rx, llm_profile)? {
        Some(transport) => executors_supporting(&rx, transport),
        None => Ok(Vec::new()),
    }
}

/// The Executors that can run the LLM configuration a submitted Job froze,
/// in ID order; empty for a Job without one.
pub(crate) fn read_job_compatible_executors(
    database: &Database,
    job: JobId,
) -> Result<Vec<ExecutorId>, StorageError> {
    let rx = read(database)?;
    let Some(submitted) = submitted_configuration(&rx, job)? else {
        return Ok(Vec::new());
    };
    executors_supporting(&rx, submitted.frozen.llm.provider.transport)
}

pub(crate) fn read_project(
    database: &Database,
    id: ProjectId,
) -> Result<Option<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(PROJECTS)?
        .get(id.get())?
        .map(|v| decode_project(v.value()))
        .transpose()
}

pub(crate) fn read_projects(
    database: &Database,
    before: Option<ProjectId>,
    limit: usize,
) -> Result<Vec<ProjectRecord>, StorageError> {
    newest(
        &read(database)?,
        PROJECTS,
        before.map(ProjectId::get),
        limit,
        decode_project,
    )
}

/// The Jobs associated with a Project in ID order, strictly after `after`.
pub(crate) fn read_project_jobs(
    database: &Database,
    project: ProjectId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let jobs = read_pairs(
        &rx,
        PROJECT_JOBS,
        project.get(),
        after.map(JobId::get),
        limit.min(MAX_PAGE),
    )?;
    Ok(jobs.into_iter().map(JobId::new).collect())
}

pub(crate) fn read_llm_provider(
    database: &Database,
    id: LlmProviderId,
) -> Result<Option<LlmProviderRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(LLM_PROVIDERS)?
        .get(id.get())?
        .map(|v| decode_llm_provider(v.value()))
        .transpose()
}

pub(crate) fn read_llm_providers(
    database: &Database,
    before: Option<LlmProviderId>,
    limit: usize,
) -> Result<Vec<LlmProviderRecord>, StorageError> {
    newest(
        &read(database)?,
        LLM_PROVIDERS,
        before.map(LlmProviderId::get),
        limit,
        decode_llm_provider,
    )
}

pub(super) fn profile_record(
    rx: &ReadTransaction,
    row: ProfileRow,
) -> Result<ProfileRecord, StorageError> {
    let llm_provider = read_link(rx, USES_PROVIDER, row.id.get())?.map(LlmProviderId::new);
    if (row.config.kind() == ProfileKind::Llm) != llm_provider.is_some() {
        return Err(StorageError::corrupted(format!(
            "{} and its USES_PROVIDER edge disagree",
            row.id
        )));
    }
    let llm_targets: Vec<ProfileId> = read_pairs(rx, TARGETS_LLM, row.id.get(), None, usize::MAX)?
        .into_iter()
        .map(ProfileId::new)
        .collect();
    if row.config.kind() == ProfileKind::Llm && !llm_targets.is_empty() {
        return Err(StorageError::corrupted(format!(
            "the LLM Profile {} has LLM targets",
            row.id
        )));
    }
    Ok(ProfileRecord {
        id: row.id,
        name: row.name,
        config: row.config,
        llm_provider,
        llm_targets,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub(super) fn submitted_configuration(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    let Some(row) = rx
        .open_table(JOBS)?
        .get(job.get())?
        .map(|v| JobRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    let record = job_record(rx, row)?;
    let Some(configuration) = record.configuration else {
        return Ok(None);
    };
    let input = rx
        .open_table(JOB_INPUTS)?
        .get(record.input.get())?
        .map(|v| JobInputRow::decode(v.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{job} names missing {}", record.input)))?;
    let frozen = input.configuration.ok_or_else(|| {
        StorageError::corrupted(format!(
            "the configured {job} has no frozen configuration in {}",
            record.input
        ))
    })?;
    Ok(Some(SubmittedConfiguration {
        agent_profile: configuration.agent_profile,
        llm_profile: configuration.llm_profile,
        frozen,
    }))
}

/// What a submitted Job was configured to execute, as frozen in its input,
/// with the Profiles it was submitted with as provenance. Current Profile
/// contents are not read. `None` for a Job submitted without one.
pub(crate) fn read_job_configuration(
    database: &Database,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    submitted_configuration(&read(database)?, job)
}

/// The Jobs submitted with a Profile, in ID order: those that applied it
/// for an Agent Profile, those that chose it for an LLM Profile.
pub(crate) fn read_profile_jobs(
    database: &Database,
    profile: ProfileId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let Some(row) = rx
        .open_table(PROFILES)?
        .get(profile.get())?
        .map(|v| ProfileRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(Vec::new());
    };
    let index = profile_jobs_index(row.config.kind());
    Ok(read_pairs(
        &rx,
        index,
        profile.get(),
        after.map(JobId::get),
        limit.min(MAX_PAGE),
    )?
    .into_iter()
    .map(JobId::new)
    .collect())
}

/// Whether a Profile can be edited now, and which unfinished Jobs hold it.
pub(crate) fn read_profile_editability(
    database: &Database,
    profile: ProfileId,
) -> Result<Option<ProfileEditability>, StorageError> {
    let rx = read(database)?;
    let Some(row) = rx
        .open_table(PROFILES)?
        .get(profile.get())?
        .map(|v| ProfileRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    let index = rx.open_table(profile_jobs_index(row.config.kind()))?;
    let (blocking_jobs, more_blocking_jobs) =
        profile_holders(&index, &rx.open_table(JOBS)?, profile, MAX_PAGE)?;
    Ok(Some(ProfileEditability {
        blocking_jobs,
        more_blocking_jobs,
    }))
}

pub(crate) fn read_profile(
    database: &Database,
    id: ProfileId,
) -> Result<Option<ProfileRecord>, StorageError> {
    let rx = read(database)?;
    let row = rx
        .open_table(PROFILES)?
        .get(id.get())?
        .map(|v| ProfileRow::decode(v.value()))
        .transpose()?;
    row.map(|row| profile_record(&rx, row)).transpose()
}

/// Profiles of both kinds, newest first.
pub(crate) fn read_profiles(
    database: &Database,
    before: Option<ProfileId>,
    limit: usize,
) -> Result<Vec<ProfileRecord>, StorageError> {
    let rx = read(database)?;
    let rows = newest(
        &rx,
        PROFILES,
        before.map(ProfileId::get),
        limit,
        ProfileRow::decode,
    )?;
    rows.into_iter()
        .map(|row| profile_record(&rx, row))
        .collect()
}

/// The LLM Profiles that use a Provider, in ID order.
pub(crate) fn read_provider_profiles(
    database: &Database,
    provider: LlmProviderId,
) -> Result<Vec<ProfileId>, StorageError> {
    let rx = read(database)?;
    Ok(
        read_pairs(&rx, PROVIDER_PROFILES, provider.get(), None, MAX_PAGE)?
            .into_iter()
            .map(ProfileId::new)
            .collect(),
    )
}

pub(crate) fn read_agent(
    database: &Database,
    id: AgentId,
) -> Result<Option<AgentRecord>, StorageError> {
    let rx = read(database)?;
    let Some(row) = rx
        .open_table(AGENTS)?
        .get(id.get())?
        .map(|v| AgentRow::decode(v.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    let profiles = read_pairs(&rx, USES_PROFILE, id.get(), None, usize::MAX)?
        .into_iter()
        .map(ProfileId::new)
        .collect();
    Ok(Some(AgentRecord {
        id: row.id,
        name: row.name,
        profiles,
        created_at: row.created_at,
    }))
}
