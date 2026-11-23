use super::catalog::global_compaction_settings;
use crate::db::entities::{
    AllocationPhase, ExecutionOperation, JobSpec, JobState, MutableJob, MutableJobPhase,
    OutcomeKind,
};
use crate::db::entities::{
    AttachmentId, AttachmentRecord, ContentDigest, DevelopmentCandidate, DevelopmentContextTurn,
    DevelopmentDecisionContext, DevelopmentExecutionContext, DevelopmentRetainedInput, JobId,
    ModelCapability, Origin, OutputKind, VerifiedNativeSummary,
};
use crate::db::redb::reads::jobs::{
    mutable_job_record_in, read_conversation_row, read_job_lifecycle, tool_call_name,
    unstarted_settlement_in,
};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{
    decode_attachment, decode_output, decode_project, decode_tool_evidence, tool_call_ref,
};
use crate::db::redb::schema::{
    ATTACHMENTS, CHILD_GROUPS, CHILDREN, DEPENDENCY_WAITERS, EXECUTORS, JOB_OUTPUTS, JOB_RUNTIME,
    JOBS, MESSAGES, META, POOL_ALLOCATIONS, POOLS, PROJECTS, TOOL_CALLS,
};
use crate::db::redb::{StorageError, allocations, jobs, state};
use ::redb::{Database, ReadTransaction, ReadableTable};
use std::collections::BTreeSet;

fn context_unavailable(detail: &str) -> StorageError {
    StorageError {
        kind: super::StorageErrorKind::Other,
        os_error: None,
        detail: detail.to_owned(),
    }
}

fn read_awaiting_batch(
    rx: &ReadTransaction,
    job: JobId,
    output: Option<&crate::db::entities::JobOutputRecord>,
    selected_bytes: &mut usize,
) -> Result<Vec<crate::db::entities::ToolEvidenceRecord>, StorageError> {
    use crate::db::entities::{AWAITING_TOOL_DECISION, ToolDecision};
    let Some(output) = output else {
        return Ok(Vec::new());
    };
    let paused = output.code.as_deref() == Some(AWAITING_TOOL_DECISION);
    let legacy = output.code.is_none()
        && output
            .value
            .as_ref()
            .and_then(|value| value.get("finish_reason"))
            .and_then(serde_json::Value::as_str)
            == Some("tool_calls");
    if !paused && !legacy {
        return Ok(Vec::new());
    }

    let table = rx.open_table(TOOL_CALLS)?;
    let id = job.get();
    let Some((last, _)) = table
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))?
        .next_back()
        .transpose()?
    else {
        if paused {
            return Err(StorageError::corrupted(format!(
                "paused Job {job} has no tool calls"
            )));
        }
        return Ok(Vec::new());
    };
    let (_, request, _) = last.value();
    let mut batch = Vec::new();
    for entry in table.range((id, request, 0)..=(id, request, u32::MAX))? {
        let (key, value) = entry?;
        *selected_bytes = selected_bytes.saturating_add(value.value().len());
        if *selected_bytes > 8 * 1024 * 1024 || batch.len() == 128 {
            return Err(context_unavailable(
                "paused tool batch exceeds execution context limit",
            ));
        }
        batch.push(decode_tool_evidence(
            tool_call_ref(key.value())?,
            value.value(),
        )?);
    }
    if batch.iter().any(|call| {
        matches!(
            call.decision.fact,
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_)
        )
    }) {
        Ok(batch)
    } else if paused {
        Err(StorageError::corrupted(format!(
            "paused Job {job} has no awaiting tool call"
        )))
    } else {
        Ok(Vec::new())
    }
}

fn context_attachments(
    rx: &ReadTransaction,
    ids: &[AttachmentId],
) -> Result<Vec<AttachmentRecord>, StorageError> {
    let table = rx.open_table(ATTACHMENTS)?;
    ids.iter()
        .map(|id| {
            let bytes = table
                .get(id.get())?
                .ok_or_else(|| StorageError::corrupted("context Attachment missing"))?;
            let attachment = decode_attachment(id.get(), bytes.value())?;
            if !attachment.is_available() {
                return Err(context_unavailable("context Attachment is unavailable"));
            }
            Ok(attachment)
        })
        .collect()
}

const SUMMARY_MAX_OUTPUTS: usize = 100_000;

const SUMMARY_MAX_BYTES: usize = 128 * 1024 * 1024;

const SUMMARY_MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;

pub(crate) enum NativeSourceError {
    Unavailable(&'static str),
    Corrupt { job: JobId, detail: String },
    Storage(StorageError),
}

impl NativeSourceError {
    pub(crate) fn malformed(job: JobId, detail: impl Into<String>) -> Self {
        Self::Corrupt {
            job,
            detail: detail.into(),
        }
    }

    // Use only for payload decoders of Job-owned rows. Index and cross-record
    // checks, shared rows and redb errors go through the Storage conversion.
    pub(crate) fn payload_error(job: JobId, error: StorageError) -> Self {
        if error.kind == super::StorageErrorKind::Corrupted {
            Self::malformed(job, error.detail)
        } else {
            Self::Storage(error)
        }
    }

    pub(crate) fn decoded<T>(job: JobId, decoded: Result<T, StorageError>) -> Result<T, Self> {
        decoded.map_err(|error| Self::payload_error(job, error))
    }
}

impl From<StorageError> for NativeSourceError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<::redb::StorageError> for NativeSourceError {
    fn from(error: ::redb::StorageError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<::redb::TableError> for NativeSourceError {
    fn from(error: ::redb::TableError) -> Self {
        Self::Storage(error.into())
    }
}

pub(crate) struct SummaryBudget {
    pub(crate) bytes: usize,
    pub(crate) outputs: usize,
}

pub(in crate::db::redb) fn summary_turn_head(
    tx: &impl crate::db::redb::TxRead,
    conversation: JobId,
) -> Result<Option<JobId>, NativeSourceError> {
    super::jobs::turn_head_with(tx, conversation, NativeSourceError::payload_error)
}

fn check_summary_history_job(job: &MutableJob, conversation: JobId) -> Result<(), StorageError> {
    if job.parent_id != conversation || job.phase == MutableJobPhase::Unsubmitted {
        return Err(StorageError::corrupted(format!(
            "{} is outside the submitted history of {conversation}",
            job.id
        )));
    }
    Ok(())
}

pub(in crate::db::redb) fn summary_history_job(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    job: JobId,
    conversation: JobId,
) -> Result<MutableJob, NativeSourceError> {
    let row = jobs.get(job.get())?.ok_or_else(|| {
        StorageError::corrupted(format!(
            "summary history of {conversation} references missing {job}"
        ))
    })?;
    let job = NativeSourceError::decoded(job, jobs::decode_mutable_job(job.get(), row.value()))?;
    check_summary_history_job(&job, conversation)?;
    Ok(job)
}

pub(in crate::db::redb) fn summary_child_job(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    groups: &impl ReadableTable<u128, &'static [u8]>,
    conversation: JobId,
    child: JobId,
) -> Result<Option<MutableJob>, NativeSourceError> {
    if let Some(row) = jobs.get(child.get())? {
        let job =
            NativeSourceError::decoded(child, jobs::decode_mutable_job(child.get(), row.value()))?;
        check_summary_history_job(&job, conversation)?;
        return Ok(Some(job));
    }
    let group = groups.get(child.get())?.ok_or_else(|| {
        StorageError::corrupted(format!(
            "child index of {conversation} references missing {child}"
        ))
    })?;
    if JobSpec::decode_at(child.get(), group.value())?.parent_id != Some(conversation) {
        return Err(StorageError::corrupted(format!(
            "child index of {conversation} disagrees with Group {child}"
        ))
        .into());
    }
    Ok(None)
}

pub(in crate::db::redb) fn summary_job_state(
    states: &impl ReadableTable<u128, &'static [u8]>,
    job: JobId,
) -> Result<JobState, NativeSourceError> {
    let row = states.get(job.get())?.ok_or_else(|| {
        StorageError::corrupted(format!("summary source {job} runtime is missing"))
    })?;
    NativeSourceError::decoded(job, state::decode_state(job.get(), row.value()))
}

impl SummaryBudget {
    fn bytes(&mut self, n: usize) -> Result<(), NativeSourceError> {
        if n > SUMMARY_MAX_RECORD_BYTES {
            return Err(NativeSourceError::Unavailable(
                "summary verification record exceeds byte limit",
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(n)
            .ok_or(NativeSourceError::Unavailable(
                "summary verification byte budget exceeded",
            ))?;
        if self.bytes > SUMMARY_MAX_BYTES {
            return Err(NativeSourceError::Unavailable(
                "summary verification byte budget exceeded",
            ));
        }
        Ok(())
    }

    fn output(&mut self) -> Result<(), NativeSourceError> {
        self.outputs += 1;
        if self.outputs > SUMMARY_MAX_OUTPUTS {
            return Err(NativeSourceError::Unavailable(
                "summary verification output budget exceeded",
            ));
        }
        Ok(())
    }
}

fn native_summary_frame(
    hash: &mut aws_lc_rs::digest::Context,
    budget: &mut SummaryBudget,
    bytes: &[u8],
) -> Result<(), NativeSourceError> {
    budget.bytes(bytes.len())?;
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    Ok(())
}

fn native_summary_owner(
    pools: &impl ReadableTable<u128, &'static [u8]>,
    executors: &impl ReadableTable<u128, &'static [u8]>,
    allocation: &crate::db::entities::PoolAllocation,
    budget: &mut SummaryBudget,
) -> Result<(), NativeSourceError> {
    let job = allocation.token.job;
    let pool = pools.get(allocation.token.pool.get())?.ok_or_else(|| {
        StorageError::corrupted(format!(
            "{job} Allocation references missing {}",
            allocation.token.pool
        ))
    })?;
    let executor = executors
        .get(allocation.token.executor.get())?
        .ok_or_else(|| {
            StorageError::corrupted(format!(
                "{job} Allocation references missing {}",
                allocation.token.executor
            ))
        })?;
    let pool_row = allocations::decode_allocation_pool(allocation.token.pool.get(), pool.value())?;
    let executor_row =
        allocations::decode_allocation_executor(allocation.token.executor.get(), executor.value())?;
    let supports_operation = executor_row
        .capabilities
        .contains(&(allocation.operation, allocation.protocol));
    if executor_row.pool != pool_row.id
        || pool_row.revision < allocation.pool_revision
        || executor_row.revision < allocation.executor_revision
        || (executor_row.revision == allocation.executor_revision && !supports_operation)
    {
        return Err(StorageError::corrupted(format!(
            "{job} Allocation ownership disagrees with {} and {}",
            pool_row.id, executor_row.id
        ))
        .into());
    }
    budget.bytes(pool.value().len())?;
    budget.bytes(executor.value().len())?;
    if !supports_operation {
        return Err(NativeSourceError::Unavailable(
            "summary Executor no longer supports the source operation",
        ));
    }
    Ok(())
}

pub(crate) struct NativeSummaryTables<J, S, A, O, AT, T, P, E, C, R, M> {
    pub conversations: C,
    pub request_events: R,
    pub messages: M,
    pub jobs: J,
    pub states: S,
    pub allocations: A,
    pub outputs: O,
    pub attachments: AT,
    pub tools: T,
    pub pools: P,
    pub executors: E,
}

pub(crate) fn native_source_boundary<J, S, A, O, AT, T, P, E, C, R, M>(
    tables: &NativeSummaryTables<J, S, A, O, AT, T, P, E, C, R, M>,
    id: JobId,
    conversation: JobId,
    budget: &mut SummaryBudget,
) -> Result<(crate::db::entities::NativeSummaryBoundary, Option<JobId>), NativeSourceError>
where
    J: ReadableTable<u128, &'static [u8]>,
    S: ReadableTable<u128, &'static [u8]>,
    A: ReadableTable<u128, &'static [u8]>,
    O: ReadableTable<(u128, u32), &'static [u8]>,
    AT: ReadableTable<u128, &'static [u8]>,
    T: ReadableTable<(u128, u32, u32), &'static [u8]>,
    P: ReadableTable<u128, &'static [u8]>,
    E: ReadableTable<u128, &'static [u8]>,
{
    use aws_lc_rs::digest::{Context, SHA256};
    let jobs = &tables.jobs;
    let states = &tables.states;
    let allocations = &tables.allocations;
    let attachments = &tables.attachments;
    let outputs = &tables.outputs;
    let tools = &tables.tools;
    let job_bytes = jobs.get(id.get())?.ok_or_else(|| {
        StorageError::corrupted(format!(
            "summary history of {conversation} references missing {id}"
        ))
    })?;
    let state_bytes = states.get(id.get())?.ok_or_else(|| {
        StorageError::corrupted(format!("summary source {id} runtime is missing"))
    })?;
    budget.bytes(job_bytes.value().len())?;
    budget.bytes(state_bytes.value().len())?;
    let job =
        NativeSourceError::decoded(id, jobs::decode_mutable_job(id.get(), job_bytes.value()))?;
    check_summary_history_job(&job, conversation)?;
    if matches!(
        job.input.content,
        crate::db::entities::MutableJobContent::Decision { .. }
    ) {
        return Err(NativeSourceError::Unavailable(
            "Decision Jobs cannot be summary sources",
        ));
    }
    let state = NativeSourceError::decoded(id, state::decode_state(id.get(), state_bytes.value()))?;
    if job.phase != MutableJobPhase::Executing || !state.confirmed_terminal() {
        return Err(NativeSourceError::Unavailable(
            "summary source Job has no settled execution",
        ));
    }
    let allocation_bytes = allocations.get(id.get())?.ok_or_else(|| {
        StorageError::corrupted(format!("settled summary source {id} has no Allocation"))
    })?;
    let allocation = allocations::decode_pool_allocation(id.get(), allocation_bytes.value())?;
    budget.bytes(allocation_bytes.value().len())?;
    if allocation.phase != AllocationPhase::Released || allocation.job_revision != job.revision {
        return Err(StorageError::corrupted(format!(
            "{id} Allocation contradicts its settled Job"
        ))
        .into());
    }
    if job.summary_source.is_some()
        || job.context_selection != crate::db::entities::MutableContextSelection::FullHistory
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
        || allocation.operation != ExecutionOperation::LlmRequest
        || !job
            .frozen
            .as_ref()
            .is_some_and(|frozen| matches!(frozen, crate::db::entities::MutableFrozenConfiguration::Generative(frozen) if allocation.protocol == Some(frozen.llm.provider.transport)))
        || !job
            .submitted_at
            .is_some_and(|at| at <= allocation.claimed_at)
        || !state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.at >= allocation.claimed_at)
    {
        return Err(NativeSourceError::Unavailable(
            "summary source is not a settled conversation Job",
        ));
    }
    native_summary_owner(&tables.pools, &tables.executors, &allocation, budget)?;
    if tools
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .next()
        .transpose()?
        .is_some()
    {
        return Err(NativeSourceError::Unavailable(
            "summary source tool evidence is not supported",
        ));
    }
    let mut hash = Context::new(&SHA256);
    hash.update(b"ocg-native-summary-source-v1");
    native_summary_frame(&mut hash, budget, job_bytes.value())?;
    native_summary_frame(&mut hash, budget, state_bytes.value())?;
    native_summary_frame(&mut hash, budget, allocation_bytes.value())?;
    for attachment_id in job.input.attachments() {
        let bytes = attachments.get(attachment_id.get())?.ok_or_else(|| {
            StorageError::corrupted(format!(
                "summary source {id} references missing {attachment_id}"
            ))
        })?;
        let attachment = decode_attachment(attachment_id.get(), bytes.value())?;
        native_summary_frame(&mut hash, budget, bytes.value())?;
        if !attachment.is_available() {
            return Err(NativeSourceError::Unavailable(
                "summary source attachment unavailable",
            ));
        }
    }
    let mut through = 0u32;
    let mut final_result = false;
    for entry in outputs.range((id.get(), 0)..=(id.get(), u32::MAX))? {
        let (key, bytes) = entry?;
        budget.output()?;
        let seq = key.value().1;
        if seq
            != through
                .checked_add(1)
                .ok_or(NativeSourceError::Unavailable(
                    "summary source output overflow",
                ))?
        {
            return Err(NativeSourceError::malformed(
                id,
                "source output sequence has a gap",
            ));
        }
        native_summary_frame(&mut hash, budget, bytes.value())?;
        let output = NativeSourceError::decoded(id, decode_output(key.value(), bytes.value()))?;
        if output.created_at < allocation.claimed_at
            || state
                .outcome
                .as_ref()
                .is_none_or(|outcome| output.created_at > outcome.at)
        {
            return Err(NativeSourceError::Unavailable(
                "summary source output timing is inconsistent",
            ));
        }
        if final_result {
            return Err(NativeSourceError::malformed(
                id,
                "source output follows its final result",
            ));
        }
        final_result = output.kind == OutputKind::Result;
        through = seq;
    }
    if through == 0 || !final_result {
        return Err(NativeSourceError::malformed(
            id,
            "source final output is missing",
        ));
    }
    let digest: [u8; 32] = hash
        .finish()
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid source digest"))?;
    Ok((
        crate::db::entities::NativeSummaryBoundary {
            job: id,
            revision: job.revision,
            output_through: through,
            content: ContentDigest::from_bytes(digest),
        },
        job.previous,
    ))
}

/// Why a published summary could not be verified.
///
/// `Corrupt` is confined to rows the producer Job owns: its payload no longer
/// decodes, so that one summary is unusable. Batch callers isolate it, record
/// that storage verification is due, and continue with other work. Read-only
/// projections cannot record that, so they surface it as `Storage`.
///
/// `Storage` covers everything else: a missing referenced row, an index or
/// cross-record disagreement, and damage to shared `MESSAGES`, `POOLS` or
/// `EXECUTORS`. Those stay fail-closed because they are not confined to the
/// summary being verified.
pub(crate) enum SummarySourceError {
    Corrupt { producer: JobId, detail: String },
    Storage(StorageError),
}

impl SummarySourceError {
    // Only payload decoders of Job-owned rows qualify; see the variant docs.
    fn payload(producer: JobId, error: StorageError) -> Self {
        if error.kind == super::StorageErrorKind::Corrupted {
            Self::Corrupt {
                producer,
                detail: error.detail,
            }
        } else {
            Self::Storage(error)
        }
    }

    pub(crate) fn into_storage(self) -> StorageError {
        match self {
            Self::Corrupt { producer, detail } => {
                StorageError::corrupted(format!("summary source {producer} is corrupt: {detail}"))
            }
            Self::Storage(error) => error,
        }
    }
}

impl From<StorageError> for SummarySourceError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<::redb::StorageError> for SummarySourceError {
    fn from(error: ::redb::StorageError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<::redb::TableError> for SummarySourceError {
    fn from(error: ::redb::TableError) -> Self {
        Self::Storage(error.into())
    }
}

pub(crate) fn verify_native_summary_in(
    tx: &impl crate::db::redb::TxRead,
    conversation: JobId,
    consumer: JobId,
    producer: JobId,
    source: ContentDigest,
) -> Result<Option<VerifiedNativeSummary>, SummarySourceError> {
    if consumer == producer {
        return Ok(None);
    }
    let jobs = tx.table(JOBS)?;
    let producer_bytes = jobs
        .get(producer.get())?
        .ok_or_else(|| StorageError::corrupted("summary producer missing"))?;
    let producer_job =
        crate::db::redb::jobs::decode_mutable_job(producer.get(), producer_bytes.value())
            .map_err(|error| SummarySourceError::payload(producer, error))?;
    let states = tx.table(JOB_RUNTIME)?;
    let state_bytes = states
        .get(producer.get())?
        .ok_or_else(|| StorageError::corrupted("summary producer state missing"))?;
    let state = state::decode_state(producer.get(), state_bytes.value())
        .map_err(|error| SummarySourceError::payload(producer, error))?;
    if producer_job.parent_id != conversation
        || producer_job.summary_source != Some(source)
        || !state
            .outcome
            .as_ref()
            .is_some_and(|o| o.kind == OutcomeKind::Completed)
    {
        return Ok(None);
    }
    let manifest = {
        let messages = tx.table(MESSAGES)?;
        let manifest_bytes = messages
            .get(source.as_bytes().as_slice())?
            .ok_or_else(|| StorageError::corrupted("summary manifest missing"))?;
        serde_json::from_slice::<crate::db::entities::NativeSummaryManifest>(manifest_bytes.value())
            .map_err(|_| StorageError::corrupted("malformed summary manifest"))?
    };
    manifest.validate().map_err(StorageError::corrupted)?;
    if manifest.conversation != conversation {
        return Err(StorageError::corrupted("summary manifest names another conversation").into());
    }
    let outputs = tx.table(JOB_OUTPUTS)?;
    let output = outputs
        .range((producer.get(), 0)..=(producer.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(k, b)| decode_output(k.value(), b.value()))
        .transpose()
        .map_err(|error| SummarySourceError::payload(producer, error))?
        .filter(|o| o.kind == OutputKind::Result)
        .ok_or_else(|| StorageError::corrupted("summary producer result missing"))?;
    let link: crate::db::entities::NativeSummaryResultLink = serde_json::from_value(
        output
            .value
            .ok_or_else(|| StorageError::corrupted("summary result link missing"))?,
    )
    .map_err(|_| SummarySourceError::Corrupt {
        producer,
        detail: "malformed summary result link".to_owned(),
    })?;
    if link.source() != source {
        return Err(StorageError::corrupted("summary result names another source").into());
    }
    let summary = crate::db::redb::observations::linked_summary(tx, producer, link)?;
    Ok(Some(VerifiedNativeSummary { manifest, summary }))
}

pub(crate) fn read_trial_job(
    database: &Database,
    job: JobId,
) -> Result<
    Option<(
        crate::db::entities::MutableJobRecord,
        Option<crate::db::entities::PoolAllocation>,
    )>,
    StorageError,
> {
    let rx = read(database)?;
    let Some(record) = mutable_job_record_in(&rx, job)? else {
        return Ok(None);
    };
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let allocation = match allocations.get(job.get())? {
        Some(row) => Some(allocations::decode_pool_allocation(job.get(), row.value())?),
        None => None,
    };
    Ok(Some((record, allocation)))
}

fn native_epoch(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<u64, StorageError> {
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("database epoch missing"))?;
    let bytes: [u8; 8] = epoch
        .value()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid database epoch"))?;
    Ok(u64::from_le_bytes(bytes))
}

pub(crate) fn read_development_execution_context(
    database: &Database,
    token: crate::db::entities::AllocationToken,
    requirements: &dyn crate::db::entities::DevelopmentContextRequirements,
) -> Result<DevelopmentCandidate, StorageError> {
    use crate::db::entities::AllocationPhase;
    let rx = read(database)?;
    let meta = rx.open_table(META)?;
    if native_epoch(&meta)? != token.epoch {
        return Err(context_unavailable(
            "Allocation belongs to another database epoch",
        ));
    }
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(token.job.get())?
        .map(|v| allocations::decode_pool_allocation(token.job.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Allocation is missing"))?;
    if allocation.token != token
        || allocation.phase != AllocationPhase::Reserved
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
    {
        return Err(context_unavailable(
            "Allocation ownership is not a current reservation",
        ));
    }
    development_candidate_in(&rx, &meta, allocation, requirements)
}

fn development_candidate_in(
    rx: &ReadTransaction,
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    allocation: crate::db::entities::PoolAllocation,
    requirements: &dyn crate::db::entities::DevelopmentContextRequirements,
) -> Result<DevelopmentCandidate, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let token = allocation.token;
    let record = mutable_job_record_in(rx, token.job)?
        .ok_or_else(|| context_unavailable("Job is missing"))?;
    if record.job.phase != MutableJobPhase::Queued
        || record.outcome.is_some()
        || record.job.revision != allocation.job_revision
        || record.job.frozen.is_none()
    {
        return Err(context_unavailable("Job is not the reserved revision"));
    }
    let adopted_summary = match record.job.context_selection {
        crate::db::entities::MutableContextSelection::FullHistory => None,
        crate::db::entities::MutableContextSelection::AdoptedSummary { producer, source } => Some(
            verify_native_summary_in(rx, record.job.parent_id, record.job.id, producer, source)
                .map_err(SummarySourceError::into_storage)?
                .ok_or_else(|| context_unavailable("adopted summary is unavailable"))?,
        ),
    };
    let pools = rx.open_table(POOLS)?;
    let executors = rx.open_table(EXECUTORS)?;
    let pool = pools
        .get(token.pool.get())?
        .map(|v| allocations::decode_allocation_pool(token.pool.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Pool is missing"))?;
    let executor = executors
        .get(token.executor.get())?
        .map(|v| allocations::decode_allocation_executor(token.executor.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Executor is missing"))?;
    if !pool.enabled
        || pool.archived
        || !executor.enabled
        || executor.archived
        || executor.pool != pool.id
        || !executor
            .capabilities
            .contains(&(allocation.operation, allocation.protocol))
    {
        return Err(context_unavailable(
            "Pool or Executor configuration changed",
        ));
    }
    let conversation = read_conversation_row(rx, record.job.parent_id)?
        .ok_or_else(|| StorageError::corrupted("conversation missing"))?;
    let conversation_lifecycle = read_job_lifecycle(rx, conversation.id)?;
    if conversation_lifecycle.on_hold.is_some() || conversation_lifecycle.closing.is_some() {
        return Err(context_unavailable("conversation is not eligible"));
    }
    let project = record
        .project
        .map(|id| {
            rx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|v| decode_project(id.get(), v.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("conversation Project missing"))
        })
        .transpose()?;
    if matches!(
        &record.job.input.content,
        crate::db::entities::MutableJobContent::Decision { .. }
    ) {
        let configuration = match record.job.frozen.clone() {
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(configuration)) => {
                crate::db::entities::FrozenDecisionConfig::Native(configuration)
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) => {
                crate::db::entities::FrozenDecisionConfig::Generative(configuration, None)
            }
            None => return Err(context_unavailable("Decision configuration is missing")),
        };
        return Ok(DevelopmentCandidate::Decision(DevelopmentDecisionContext {
            allocation,
            job: record.job,
            pool,
            executor,
            configuration,
            conversation,
            conversation_lifecycle,
            project,
        }));
    }
    let Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) =
        record.job.frozen.clone()
    else {
        return Err(context_unavailable("Job operation is unsupported"));
    };
    let plan_step = match conversation.origin {
        Origin::Agent { call } => tool_call_name(rx, call)?.as_deref() == Some("job_plan"),
        Origin::User | Origin::System => false,
    };
    let attachments = context_attachments(rx, record.job.input.attachments())?;
    let compaction = global_compaction_settings(meta)?;
    let covered: BTreeSet<_> = adopted_summary
        .as_ref()
        .map_or_else(BTreeSet::new, |summary| {
            summary
                .manifest
                .sources
                .iter()
                .map(|source| source.job)
                .collect()
        });
    let mut covered_inputs = Vec::with_capacity(covered.len());
    let is_merge = record.job.input.text().starts_with("Merge ");
    if let Some(source_digest) = record.job.summary_source {
        let messages = rx.open_table(MESSAGES)?;
        let manifest_bytes = messages
            .get(source_digest.as_bytes().as_slice())?
            .ok_or_else(|| StorageError::corrupted("summary manifest missing"))?;
        let manifest: crate::db::entities::NativeSummaryManifest =
            serde_json::from_slice(manifest_bytes.value())
                .map_err(|_| StorageError::corrupted("malformed summary manifest"))?;
        manifest.validate().map_err(StorageError::corrupted)?;
        let jobs_table = rx.open_table(JOBS)?;
        let outputs_table = rx.open_table(JOB_OUTPUTS)?;
        if !is_merge {
            for boundary in &manifest.sources {
                let job_bytes = jobs_table
                    .get(boundary.job.get())?
                    .ok_or_else(|| StorageError::corrupted("summary source Job missing"))?;
                let src_job = crate::db::redb::jobs::decode_mutable_job(
                    boundary.job.get(),
                    job_bytes.value(),
                )?;
                let mut turn_text = format!("User: {}\n", src_job.input.text());
                for entry in
                    outputs_table.range((boundary.job.get(), 0)..=(boundary.job.get(), u32::MAX))?
                {
                    let (key, val) = entry?;
                    let out = decode_output(key.value(), val.value())?;
                    if out.kind != OutputKind::Result {
                        continue;
                    }
                    let answer = super::super::observations::answer_text(rx, boundary.job, &out)?;
                    if let Some(t) = answer
                        .or_else(|| out.display_text())
                        .filter(|text| !text.is_empty())
                    {
                        turn_text.push_str("Assistant: ");
                        turn_text.push_str(&t);
                        turn_text.push('\n');
                    }
                }
                covered_inputs.push(DevelopmentRetainedInput {
                    job: boundary.job,
                    text: turn_text,
                    attachments: Vec::new(),
                });
            }
        }
        if is_merge {
            let child_groups = rx.open_table(CHILD_GROUPS)?;
            let children = rx.open_table(CHILDREN)?;
            let waiters = rx.open_table(DEPENDENCY_WAITERS)?;
            for cg in child_groups
                .range((conversation.id.get(), 0)..=(conversation.id.get(), u128::MAX))?
            {
                let (cg_key, _) = cg?;
                let (_, cg_id) = cg_key.value();
                for ch in children.range((cg_id, 0)..=(cg_id, u128::MAX))? {
                    let (ch_key, _) = ch?;
                    let (_, chunk_id) = ch_key.value();
                    if waiters.get((chunk_id, record.job.id.get()))?.is_some() {
                        let mut chunk_text = String::new();
                        for entry in outputs_table.range((chunk_id, 0)..=(chunk_id, u32::MAX))? {
                            let (key, val) = entry?;
                            let out = decode_output(key.value(), val.value())?;
                            if out.kind == OutputKind::Result
                                && let Some(t) = out.display_text()
                            {
                                chunk_text.push_str(&t);
                            }
                        }
                        if !chunk_text.is_empty() {
                            covered_inputs.push(DevelopmentRetainedInput {
                                job: JobId::new(chunk_id),
                                text: chunk_text,
                                attachments: Vec::new(),
                            });
                        }
                    }
                }
            }
            if covered_inputs.is_empty() {
                return Err(context_unavailable("chunk summaries missing for merge"));
            }
        }
    }
    let mut history = Vec::new();
    let mut previous = record.job.previous;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let mut output_count = 0usize;
    let mut output_bytes = 0usize;
    let mut selected_bytes = record.job.input.text().len().saturating_add(
        requirements
            .prompt_bytes(&configuration, project.as_ref(), adopted_summary.as_ref())
            .map_err(|_| StorageError::corrupted("invalid project context"))?,
    );
    let mut image_count = attachments.len();
    while let Some(id) = previous {
        let is_covered = covered.contains(&id);
        if !is_covered && history.len() == 128 {
            return Err(context_unavailable(
                "conversation history exceeds context limit",
            ));
        }
        let prior = mutable_job_record_in(rx, id)?
            .ok_or_else(|| StorageError::corrupted("predecessor missing"))?;
        if matches!(
            prior.job.input.content,
            crate::db::entities::MutableJobContent::Decision { .. }
        ) {
            return Err(context_unavailable(
                "Decision predecessors cannot be projected as generative messages",
            ));
        }
        let ended_unstarted = prior.job.parent_id == record.job.parent_id
            && prior.job.phase == MutableJobPhase::Queued
            && unstarted_settlement_in(
                rx,
                &prior,
                rx.open_table(POOL_ALLOCATIONS)?
                    .get(id.get())?
                    .map(|row| allocations::decode_pool_allocation(id.get(), row.value()))
                    .transpose()?
                    .as_ref(),
            )?;
        let unknown = prior
            .outcome
            .as_ref()
            .is_some_and(|o| o.kind == OutcomeKind::Unknown);
        if unknown
            && !rx
                .open_table(POOL_ALLOCATIONS)?
                .get(id.get())?
                .map(|row| allocations::decode_pool_allocation(id.get(), row.value()))
                .transpose()?
                .is_some_and(|a| !a.holds_capacity())
        {
            return Err(context_unavailable(
                "unknown predecessor still holds capacity",
            ));
        }
        if !ended_unstarted
            && (prior.job.parent_id != record.job.parent_id
                || prior.job.phase != MutableJobPhase::Executing
                || prior.outcome.is_none())
        {
            return Err(context_unavailable(
                "predecessor is not settled in this conversation",
            ));
        }
        selected_bytes = selected_bytes.saturating_add(prior.job.input.text().len());
        if selected_bytes > 8 * 1024 * 1024 {
            return Err(context_unavailable(
                "selected execution context exceeds byte limit",
            ));
        }
        image_count = image_count.saturating_add(prior.job.input.attachments().len());
        if image_count > requirements.max_images() {
            return Err(context_unavailable(
                "selected execution context has too many images",
            ));
        }
        let prior_attachments = context_attachments(rx, prior.job.input.attachments())?;
        if is_covered {
            previous = prior.job.previous;
            covered_inputs.push(DevelopmentRetainedInput {
                job: id,
                text: prior.job.input.text().to_owned(),
                attachments: prior_attachments,
            });
            continue;
        }
        let mut prior_outputs = Vec::new();
        let mut counted = BTreeSet::new();
        for (expected_sequence, entry) in
            (1u32..).zip(outputs.range((id.get(), 0)..=(id.get(), u32::MAX))?)
        {
            if output_count == 1000 {
                return Err(context_unavailable(
                    "conversation output exceeds context limit",
                ));
            }
            let (key, value) = entry?;
            if key.value().1 != expected_sequence {
                return Err(StorageError::corrupted(
                    "predecessor output sequence has a gap",
                ));
            }
            let row = decode_output(key.value(), value.value())?;
            let cost = super::super::observations::context_output_bytes(
                rx,
                &row,
                value.value().len(),
                &mut counted,
            )?;
            output_bytes = output_bytes.saturating_add(cost);
            selected_bytes = selected_bytes.saturating_add(cost);
            if selected_bytes > 8 * 1024 * 1024 {
                return Err(context_unavailable(
                    "selected execution context exceeds byte limit",
                ));
            }
            if output_bytes > 8 * 1024 * 1024 {
                return Err(context_unavailable(
                    "conversation output exceeds byte limit",
                ));
            }
            prior_outputs.push(row);
            output_count += 1;
        }
        if !ended_unstarted
            && !unknown
            && prior_outputs
                .last()
                .is_none_or(|output| output.kind != OutputKind::Result)
        {
            return Err(StorageError::corrupted(
                "settled predecessor has no final output",
            ));
        }
        let awaiting_batch = if unknown {
            Vec::new()
        } else {
            read_awaiting_batch(rx, id, prior_outputs.last(), &mut selected_bytes)?
        };
        let response_replay = if matches!(
            configuration.llm.provider.transport,
            crate::db::entities::LlmTransport::OpenAiResponses
                | crate::db::entities::LlmTransport::AnthropicMessages
        ) && matches!(&prior.job.frozen,
                Some(crate::db::entities::MutableFrozenConfiguration::Generative(previous))
                    if previous.llm.provider == configuration.llm.provider
                        && previous.llm.config.model == configuration.llm.config.model
                        && previous.agent.as_ref().and_then(|agent| agent.workspace_grant())
                            == configuration.agent.as_ref().and_then(|agent| agent.workspace_grant()))
            && prior
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
        {
            super::super::observations::read_responses_replay(
                rx,
                id,
                awaiting_batch.is_empty(),
                &mut selected_bytes,
            )?
        } else {
            Vec::new()
        };
        let answer_text = projected_answer(rx, &prior, prior_outputs.last())?;
        if unknown {
            selected_bytes =
                selected_bytes.saturating_add(answer_text.as_ref().map_or(0, String::len));
            if selected_bytes > 8 * 1024 * 1024 {
                return Err(context_unavailable(
                    "selected execution context exceeds byte limit",
                ));
            }
        }
        previous = prior.job.previous;
        history.push(DevelopmentContextTurn {
            record: prior,
            attachments: prior_attachments,
            outputs: prior_outputs,
            answer_text,
            awaiting_batch,
            response_replay,
        });
    }
    if record.job.summary_source.is_none() {
        covered_inputs.reverse();
    }
    history.reverse();
    if record.job.summary_source.is_none() && covered_inputs.len() != covered.len() {
        return Err(StorageError::corrupted(
            "verified summary coverage is incomplete",
        ));
    }
    let model = &configuration.llm.config;
    if image_count > 0
        && !model
            .declared_capabilities
            .contains(&ModelCapability::Vision)
    {
        return Err(context_unavailable(
            "selected Model does not support images",
        ));
    }
    if !requirements.admits_tokens(model, selected_bytes, image_count) {
        return Err(context_unavailable(
            "selected execution context exceeds estimated model token budget",
        ));
    }
    Ok(DevelopmentCandidate::Generative(
        DevelopmentExecutionContext {
            allocation,
            job: record.job,
            pool,
            executor,
            configuration,
            conversation,
            conversation_lifecycle,
            plan_step,
            project,
            compaction,
            attachments,
            adopted_summary,
            covered_inputs,
            history,
        },
    ))
}

pub(in crate::db::redb) fn projected_answer(
    tx: &impl crate::db::redb::TxRead,
    record: &crate::db::entities::MutableJobRecord,
    output: Option<&crate::db::entities::JobOutputRecord>,
) -> Result<Option<String>, StorageError> {
    if record
        .outcome
        .as_ref()
        .is_some_and(|o| o.kind == OutcomeKind::Unknown)
    {
        return super::jobs::unknown_execution_note(tx, record.job.id).map(Some);
    }
    match output {
        Some(output) => super::super::observations::answer_text(tx, record.job.id, output),
        None => Ok(None),
    }
}
