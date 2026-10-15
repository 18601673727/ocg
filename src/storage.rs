//! The only module that touches redb.
//!
//! redb is the durable source of truth. Every record is stored under its
//! stable business ID with an explicit, versioned encoding and holds only its
//! own attributes; every relationship has its own table, keyed so that its
//! cardinality is structural, plus an incoming index where a rule or query
//! needs one. An incoming index is written only by the [`Batch`] operation
//! that writes its relationship, in the same transaction, so it is never an
//! authority of its own. The database, its transactions and its tables never
//! leave this module: mutations go through [`Batch`], whose typed operations
//! are the only write path, and every relationship write checks that its
//! endpoints exist and have the required type.
//!
//! A batch is one redb write transaction. Its first failure, a rule
//! violation as much as a storage error, poisons it: every later operation
//! returns that failure again and [`run_batch`] aborts instead of committing,
//! even when the operation's caller ignored the error. Records,
//! relationships, indexes, sequences and the operation marker therefore
//! commit or abort together.
//!
//! The instance has exactly one Genesis Job, created with the database file
//! and named by the `genesis` meta entry. It is the only Job that is its own
//! parent; that self-parent edge is a root marker kept in CHILD_OF only, so
//! child enumeration, the waiting graph and descendant traversal never see it.
//!
//! The waiting graph is never cached. [`Batch::check_waiting`] builds a
//! bounded petgraph view from records read inside the current write
//! transaction, adds the proposed edges and rejects any cycle.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use petgraph::graph::{DiGraph, NodeIndex};
use redb::{
    CommitError, Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};

use crate::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};
use crate::persistence::{
    AgentConfig, AgentId, AgentRecord, AttachmentCleanup, AttachmentId, AttachmentOrigin,
    AttachmentRecord, AuthorizationBasis, AvailabilityObservation, AvailabilityStatId,
    AvailabilityStatRecord, CleanupKind, ContentDigest, DependencyCondition, DependencyState,
    EffectConclusion, ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorRecord,
    ExecutorType, FrozenExecutionConfig, FrozenLlmConfig, JobId, JobInputId, JobInputRecord,
    JobOutputId, JobOutputRecord, JobRecord, LlmConfig, LlmParameters, LlmProviderId,
    LlmProviderRecord, LlmTarget, LlmTransport, Measurements, MetricsId, MetricsRecord,
    ModelCapability, NotInvokedReason, ObservationCursor, ObservationFilter, ObservationPage,
    ObservedModel, ObservedModelChoice, ObservedModelFilter, ObservedModelPage, OperationId,
    OperationKind, OperationMarker, OutputKind, ProfileConfig, ProfileEditability, ProfileId,
    ProfileKind, ProfileRecord, ProjectId, ProjectRecord, ProviderSnapshot, RecordRef, Recorded,
    RequestFingerprint, RequestNumber, RequestRef, SubmissionKey, SubmissionRecord,
    SubmittedConfiguration, Timestamp, ToolAdmission, ToolCallRef, ToolDecision, ToolEffect,
    ToolEvidenceRecord, ToolOutcome, ToolPayload, ToolReconciliation, UncertainCause, Violation,
    WithheldReason, WrongProfileKind,
};
use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

mod availability_aggregation;
mod batch;
mod batch_links;
mod codec;
mod open;
mod rows;
mod schema;
mod sequences;
mod token_aggregation;
pub(crate) use availability_aggregation::{
    AvailabilityPlan, MAX_AGGREGATION_ATTEMPTS, commit_availability_aggregation,
    prepare_availability_aggregation, read_availability_minute, read_availability_progress,
    read_request_sample,
};
#[cfg(test)]
pub(crate) use availability_aggregation::{WriteProbe, replace_minute};
use batch::Failure;
#[cfg(test)]
pub(crate) use batch::run_batch;
pub(crate) use batch::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, MarkerSubjects,
    allocated_bytes, compact, persist_staged, run_batch_declinable, run_batch_with,
};
use batch_links::{last_seq, pair_targets, profile_holders, profile_jobs_index};
use codec::{
    Code, Dec, DecodeResult, Enc, TAG_AGENT, TAG_AGGREGATION_PROGRESS, TAG_ATTACHMENT,
    TAG_AVAILABILITY_STAT, TAG_EXECUTOR, TAG_INPUT, TAG_JOB, TAG_LLM_PROVIDER, TAG_METRICS,
    TAG_MINUTE_COUNTS, TAG_OPERATION, TAG_OUTPUT, TAG_PROFILE, TAG_PROJECT, TAG_REQUEST_SAMPLE,
    TAG_SUBMISSION, TAG_TOKEN_MINUTE, TAG_TOKEN_REQUEST, TAG_TOOL_EVIDENCE,
};
pub(crate) use open::{OpenError, OpenedFile, database_file, open};
use open::{allocate, genesis_id};
pub(crate) use rows::{
    AgentRow, AvailabilityStatRow, ExecutorRow, JobInputRow, JobRow, MetricsRow, ProfileLinks,
    ProfileRow,
};
use rows::{
    decode_attachment, decode_llm_provider, decode_marker, decode_output, decode_project,
    decode_submission, decode_tool_evidence, encode_attachment, encode_llm_provider, encode_marker,
    encode_output, encode_project, encode_submission, encode_tool_evidence, job_tool_evidence,
    provider_snapshot, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, DATABASE_FILE_NAME, FORMAT_NAME, FORMAT_VERSION, MAX_OBSERVATION_SCAN,
    MAX_OBSERVED_MODEL_SCAN, MAX_PAGE, MAX_WAITING_JOBS, MAX_WAITING_LAYERS,
};
pub(crate) use sequences::Sequence;
use sequences::{OPEN_EPOCH, Scope, filter_matches};
pub(crate) use token_aggregation::{
    TokenPlan, commit_token_aggregation, prepare_token_aggregation, read_token_minute_window,
    read_token_progress, read_token_request,
};
#[cfg(test)]
pub(crate) use token_aggregation::{replace_token_minute, replace_token_request};

// ---- storage errors -------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// An operating-system I/O error, such as `StorageFull`.
    Io(std::io::ErrorKind),
    /// An earlier I/O error was latched; the open database refuses further work.
    PreviousIo,
    Corrupted,
    AlreadyOpen,
    Closed,
    ValueTooLarge,
    Other,
}

/// A storage-engine failure. Its kind is structured; the detail is for humans.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {detail}")]
pub struct StorageError {
    pub kind: StorageErrorKind,
    /// The raw OS error number for I/O failures (28 is ENOSPC on macOS and Linux).
    pub os_error: Option<i32>,
    pub detail: String,
}

impl StorageError {
    pub(crate) fn corrupted(detail: impl Into<String>) -> Self {
        Self {
            kind: StorageErrorKind::Corrupted,
            os_error: None,
            detail: detail.into(),
        }
    }
}

impl From<redb::Error> for StorageError {
    fn from(error: redb::Error) -> Self {
        let (kind, os_error) = match &error {
            redb::Error::Io(io) => (StorageErrorKind::Io(io.kind()), io.raw_os_error()),
            redb::Error::PreviousIo => (StorageErrorKind::PreviousIo, None),
            redb::Error::Corrupted(_) => (StorageErrorKind::Corrupted, None),
            redb::Error::DatabaseAlreadyOpen => (StorageErrorKind::AlreadyOpen, None),
            redb::Error::DatabaseClosed => (StorageErrorKind::Closed, None),
            redb::Error::ValueTooLarge(_) => (StorageErrorKind::ValueTooLarge, None),
            _ => (StorageErrorKind::Other, None),
        };
        Self {
            kind,
            os_error,
            detail: error.to_string(),
        }
    }
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(error: $t) -> Self { redb::Error::from(error).into() }
        }
    )*};
}
storage_from!(
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
    redb::DatabaseError,
    redb::SetDurabilityError,
    CommitError
);

// ---- waiting graph -------------------------------------------------------------

pub(crate) fn dependency_state(
    condition: DependencyCondition,
    prerequisite: &JobRow,
) -> DependencyState {
    if prerequisite.completed_at.is_some() {
        DependencyState::Satisfied
    } else if prerequisite.settled() {
        match condition {
            DependencyCondition::Settled => DependencyState::Satisfied,
            DependencyCondition::Success => DependencyState::Impossible,
        }
    } else {
        DependencyState::Waiting
    }
}

enum WaitingVerdict {
    Acyclic,
    Cycle(Vec<JobId>),
    Limit,
}

struct WaitingTables<J, D, C> {
    jobs: J,
    depends_on: D,
    children: C,
}

impl<J, D, C> WaitingTables<J, D, C>
where
    J: ReadableTable<u64, &'static [u8]>,
    D: ReadableTable<(u64, u64), u8>,
    C: ReadableTable<(u64, u64), ()>,
{
    fn job(&self, id: u64) -> Result<Option<JobRow>, StorageError> {
        self.jobs
            .get(id)?
            .map(|v| JobRow::decode(v.value()))
            .transpose()
    }

    /// Active waiting edges out of `id`: dependencies still waiting on an
    /// unsettled prerequisite and unsettled children. A settled Job waits
    /// for nothing. The Genesis root marker is not in the children index, so
    /// the Genesis Job never waits for itself.
    fn waits_for(&self, id: u64) -> Result<Vec<u64>, StorageError> {
        let Some(job) = self.job(id)? else {
            return Ok(Vec::new());
        };
        if job.settled() {
            return Ok(Vec::new());
        }
        let mut targets = Vec::new();
        for entry in self.depends_on.range((id, 0)..=(id, u64::MAX))? {
            let (key, condition) = entry?;
            let prerequisite = key.value().1;
            let condition = DependencyCondition::from_code(condition.value())
                .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
            if let Some(row) = self.job(prerequisite)?
                && dependency_state(condition, &row) == DependencyState::Waiting
            {
                targets.push(prerequisite);
            }
        }
        for entry in self.children.range((id, 0)..=(id, u64::MAX))? {
            let child = entry?.0.value().1;
            if let Some(row) = self.job(child)?
                && !row.settled()
            {
                targets.push(child);
            }
        }
        Ok(targets)
    }
}

fn waiting_verdict(
    tx: &WriteTransaction,
    proposed: &[(JobId, JobId)],
) -> Result<WaitingVerdict, StorageError> {
    let tables = WaitingTables {
        jobs: tx.open_table(JOBS)?,
        depends_on: tx.open_table(DEPENDS_ON)?,
        children: tx.open_table(CHILDREN)?,
    };
    let mut graph: DiGraph<u64, ()> = DiGraph::new();
    let mut nodes: HashMap<u64, NodeIndex> = HashMap::new();
    let mut node = |graph: &mut DiGraph<u64, ()>, id: u64| -> Option<NodeIndex> {
        if let Some(index) = nodes.get(&id) {
            return Some(*index);
        }
        if nodes.len() >= MAX_WAITING_JOBS {
            return None;
        }
        let index = graph.add_node(id);
        nodes.insert(id, index);
        Some(index)
    };
    let mut extra: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut edges = HashSet::new();
    let mut expanded = HashSet::new();
    let mut frontier = Vec::new();
    for (from, to) in proposed {
        extra.entry(from.get()).or_default().push(to.get());
        let (Some(f), Some(t)) = (node(&mut graph, from.get()), node(&mut graph, to.get())) else {
            return Ok(WaitingVerdict::Limit);
        };
        if edges.insert((f, t)) {
            graph.add_edge(f, t, ());
        }
        if !frontier.contains(&t) {
            frontier.push(t);
        }
    }
    let mut layers = 0;
    while !frontier.is_empty() {
        if layers == MAX_WAITING_LAYERS {
            return Ok(WaitingVerdict::Limit);
        }
        layers += 1;
        let mut next = Vec::new();
        for index in frontier {
            if !expanded.insert(index) {
                continue;
            }
            let id = graph[index];
            let mut targets = tables.waits_for(id)?;
            if let Some(more) = extra.get(&id) {
                targets.extend(more);
            }
            for target in targets {
                let Some(t) = node(&mut graph, target) else {
                    return Ok(WaitingVerdict::Limit);
                };
                if edges.insert((index, t)) {
                    graph.add_edge(index, t, ());
                }
                if !expanded.contains(&t) {
                    next.push(t);
                }
            }
        }
        frontier = next;
    }
    if !petgraph::algo::is_cyclic_directed(&graph) {
        return Ok(WaitingVerdict::Acyclic);
    }
    let cycle = petgraph::algo::tarjan_scc(&graph)
        .into_iter()
        .find(|component| component.len() > 1 || graph.contains_edge(component[0], component[0]))
        .map(|component| {
            component
                .into_iter()
                .map(|index| JobId::new(graph[index]))
                .collect()
        })
        .unwrap_or_default();
    Ok(WaitingVerdict::Cycle(cycle))
}

// ---- typed reads ---------------------------------------------------------------

fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

fn read_link(rx: &ReadTransaction, table: Link, from: u64) -> Result<Option<u64>, StorageError> {
    Ok(rx.open_table(table)?.get(from)?.map(|v| v.value()))
}

fn read_pairs(
    rx: &ReadTransaction,
    table: PairSet,
    from: u64,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<u64>, StorageError> {
    pair_targets(&rx.open_table(table)?, from, after, limit)
}

fn job_record(rx: &ReadTransaction, row: JobRow) -> Result<JobRecord, StorageError> {
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
fn job_input_record(
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

fn executor_record(row: ExecutorRow) -> ExecutorRecord {
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
fn newest<T>(
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
fn executors_supporting(
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
fn current_transport(
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

fn profile_record(rx: &ReadTransaction, row: ProfileRow) -> Result<ProfileRecord, StorageError> {
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

fn submitted_configuration(
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

/// The LLM target a stored Job froze, read from only the links and the input
/// that define it: RUNS_LLM_PROFILE, APPLIES_PROFILE for its consistency,
/// HAS_INPUT and the frozen input. `None` for a Job submitted without an
/// LLM configuration. Unlike [`submitted_configuration`], it assembles no
/// [`JobRecord`], so the Job's Projects, parent, assignment and lineage are
/// never read.
fn frozen_llm_target(rx: &ReadTransaction, job: JobId) -> Result<Option<LlmTarget>, StorageError> {
    if rx
        .open_table(JOBS)?
        .get(job.get())?
        .map(|v| JobRow::decode(v.value()))
        .transpose()?
        .is_none()
    {
        return Err(StorageError::corrupted(format!(
            "an observation names missing {job}"
        )));
    }
    let Some(llm_profile) = read_link(rx, RUNS_LLM_PROFILE, job.get())? else {
        if read_link(rx, APPLIES_PROFILE, job.get())?.is_some() {
            return Err(StorageError::corrupted(format!(
                "{job} applies an Agent Profile without an LLM Profile"
            )));
        }
        return Ok(None);
    };
    let input = read_link(rx, HAS_INPUT, job.get())?
        .map(JobInputId::new)
        .ok_or_else(|| StorageError::corrupted(format!("{job} has no input")))?;
    let row = rx
        .open_table(JOB_INPUTS)?
        .get(input.get())?
        .map(|v| JobInputRow::decode(v.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{job} names missing {input}")))?;
    let frozen = row.configuration.ok_or_else(|| {
        StorageError::corrupted(format!(
            "the configured {job} has no frozen configuration in {input}"
        ))
    })?;
    Ok(Some(LlmTarget {
        llm_profile: ProfileId::new(llm_profile),
        frozen: frozen.llm,
    }))
}

/// The LLM a Job ran, as its input froze it, cached per Job for one read.
fn job_target(
    rx: &ReadTransaction,
    cache: &mut HashMap<JobId, Option<LlmTarget>>,
    job: JobId,
) -> Result<Option<LlmTarget>, StorageError> {
    if let Some(target) = cache.get(&job) {
        return Ok(target.clone());
    }
    let target = frozen_llm_target(rx, job)?;
    cache.insert(job, target.clone());
    Ok(target)
}

/// A Metrics record whose Job and target the caller has read.
fn metrics_from(
    rx: &ReadTransaction,
    id: u64,
    job: JobId,
    target: Option<LlmTarget>,
) -> Result<MetricsRecord, StorageError> {
    let bytes = rx
        .open_table(METRICS)?
        .get(id)?
        .ok_or_else(|| StorageError::corrupted(format!("an index names missing metrics {id}")))?;
    let row = MetricsRow::decode(bytes.value())?;
    Ok(MetricsRecord {
        id: row.id,
        job,
        request: row.request,
        target,
        observed_at: row.observed_at,
        measurements: row.measurements,
        token: row.token,
        recorded_at: row.recorded_at,
    })
}

/// An AvailabilityStat record whose Job and target the caller has read.
fn availability_from(
    rx: &ReadTransaction,
    id: u64,
    job: JobId,
    target: Option<LlmTarget>,
) -> Result<AvailabilityStatRecord, StorageError> {
    let bytes = rx.open_table(AVAILABILITY_STATS)?.get(id)?.ok_or_else(|| {
        StorageError::corrupted(format!("an index names missing availability_stat {id}"))
    })?;
    let row = AvailabilityStatRow::decode(bytes.value())?;
    let target = target.ok_or_else(|| {
        StorageError::corrupted(format!(
            "availability_stat {id} observes {job}, which ran no LLM"
        ))
    })?;
    Ok(AvailabilityStatRecord {
        id: row.id,
        job,
        request: row.request,
        target,
        observed_at: row.observed_at,
        observation: row.observation,
        recorded_at: row.recorded_at,
    })
}

type BuildObservation<T> =
    fn(&ReadTransaction, u64, JobId, Option<LlmTarget>) -> Result<T, StorageError>;

fn observation_job(rx: &ReadTransaction, link: Link, id: u64) -> Result<JobId, StorageError> {
    read_link(rx, link, id)?.map(JobId::new).ok_or_else(|| {
        StorageError::corrupted(format!("observation record {id} is linked to no Job"))
    })
}

/// Records a Job links to through `incoming`, in recording order. The page of
/// linked IDs is read first, so a Job with no observations needs no frozen
/// target: only a page with records resolves the Job's target, and a dangling
/// link there is corrupted.
fn job_observations<T>(
    rx: &ReadTransaction,
    incoming: PairSet,
    build: BuildObservation<T>,
    job: JobId,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<T>, StorageError> {
    let ids = read_pairs(rx, incoming, job.get(), after, limit.min(MAX_PAGE))?;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let target = frozen_llm_target(rx, job)?;
    ids.into_iter()
        .map(|id| build(rx, id, job, target.clone()))
        .collect()
}

/// One page of an observation query. It range-reads the filter's scope from
/// `index` within the observation time range, strictly after `after`, in
/// (observed_at, record) order, and checks every entry against the whole
/// filter. It reads at most `scan_limit` entries, matching or not, and
/// returns at most `limit` records, each clamped to at least 1 and `limit`
/// to [`MAX_PAGE`]. The cursor it returns is the last entry read, so a scan
/// that found nothing still advances.
#[allow(clippy::too_many_arguments)]
fn scan_observations<T>(
    rx: &ReadTransaction,
    index: ScopeIndex,
    link: Link,
    build: BuildObservation<T>,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<u64>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<T, u64>, StorageError> {
    let done = |records| {
        Ok(ObservationPage {
            records,
            next: None,
        })
    };
    let (limit, scan_limit) = (limit.clamp(1, MAX_PAGE), scan_limit.max(1));
    let until = filter.observed.until.as_unix_micros();
    let mut start = (filter.observed.from.as_unix_micros(), 0);
    if let Some(ObservationCursor {
        observed_at,
        record,
    }) = after
    {
        let at = observed_at.as_unix_micros();
        let next = match (record.checked_add(1), at.checked_add(1)) {
            (Some(record), _) => (at, record),
            (None, Some(later)) => (later, 0),
            (None, None) => return done(Vec::new()),
        };
        start = start.max(next);
    }
    if start.0 >= until {
        return done(Vec::new());
    }
    let scope = Scope::for_filter(filter).key();
    let table = rx.open_table(index)?;
    // Record IDs start at 1, so (time, 0) sorts before every entry at that time.
    let mut entries =
        table.range((scope.as_slice(), start.0, start.1)..(scope.as_slice(), until, 0))?;
    let mut cache = HashMap::new();
    let (mut records, mut scanned, mut last) = (Vec::new(), 0, None);
    while records.len() < limit && scanned < scan_limit {
        let Some(entry) = entries.next() else {
            return done(records);
        };
        let key = entry?.0;
        let (_, at, id) = key.value();
        scanned += 1;
        last = Some(ObservationCursor {
            observed_at: Timestamp::from_unix_micros(at),
            record: id,
        });
        let job = observation_job(rx, link, id)?;
        let target = job_target(rx, &mut cache, job)?;
        if filter_matches(filter, target.as_ref()) {
            records.push(build(rx, id, job, target)?);
        }
    }
    let more = entries.next().transpose()?.is_some();
    Ok(ObservationPage {
        records,
        next: if more { last } else { None },
    })
}

/// A Job's Metrics in recording order, strictly after `after` when given.
pub(crate) fn read_job_metrics(
    database: &Database,
    job: JobId,
    after: Option<MetricsId>,
    limit: usize,
) -> Result<Vec<MetricsRecord>, StorageError> {
    job_observations(
        &read(database)?,
        JOB_METRICS,
        metrics_from,
        job,
        after.map(MetricsId::get),
        limit,
    )
}

/// A Job's AvailabilityStats in recording order, strictly after `after` when given.
pub(crate) fn read_job_availability(
    database: &Database,
    job: JobId,
    after: Option<AvailabilityStatId>,
    limit: usize,
) -> Result<Vec<AvailabilityStatRecord>, StorageError> {
    job_observations(
        &read(database)?,
        JOB_AVAILABILITY,
        availability_from,
        job,
        after.map(AvailabilityStatId::get),
        limit,
    )
}

/// The tag of [`Scope::ProviderModel`] keys.
const PROVIDER_MODEL_TAG: u8 = 4;

/// The (Provider, model) pair a [`Scope::ProviderModel`] key names.
fn observed_model(scope: &[u8]) -> Result<ObservedModel, StorageError> {
    match scope {
        [PROVIDER_MODEL_TAG, rest @ ..] if rest.len() >= 8 => {
            let (provider, model) = rest.split_at(8);
            let provider =
                LlmProviderId::new(u64::from_be_bytes(provider.try_into().expect("8 bytes")));
            let model = String::from_utf8(model.to_vec()).map_err(|_| {
                StorageError::corrupted("a scope key names a model that is not UTF-8")
            })?;
            Ok(ObservedModel { provider, model })
        }
        _ => Err(StorageError::corrupted(
            "expected a Provider and model scope key",
        )),
    }
}

/// The first [`Scope::ProviderModel`] key in `index` strictly after the
/// `after` key, or from `lower` without one, and before `upper`. One seek:
/// the entries of a skipped key are never read.
fn next_provider_model(
    table: &impl ReadableTable<(&'static [u8], i64, u64), ()>,
    after: Option<&[u8]>,
    lower: &[u8],
    upper: &[u8],
) -> Result<Option<Vec<u8>>, StorageError> {
    use std::ops::Bound;
    let start = match after {
        Some(after) if after >= upper => return Ok(None),
        Some(after) if after >= lower => Bound::Excluded((after, i64::MAX, u64::MAX)),
        _ => Bound::Included((lower, i64::MIN, 0)),
    };
    let mut entries =
        table.range::<(&[u8], i64, u64)>((start, Bound::Excluded((upper, i64::MIN, 0))))?;
    Ok(match entries.next() {
        Some(entry) => Some(entry?.0.value().0.to_vec()),
        None => None,
    })
}

/// Whether `index` has an entry of `scope` observed within [from, until).
fn observed_within(
    table: &impl ReadableTable<(&'static [u8], i64, u64), ()>,
    scope: &[u8],
    from: i64,
    until: i64,
) -> Result<bool, StorageError> {
    // An iteration error is a read failure, not evidence that an entry exists.
    Ok(table
        .range((scope, from, 0)..(scope, until, 0))?
        .next()
        .transpose()?
        .is_some())
}

/// One page of distinct historical (Provider, model) pairs from the
/// Provider-and-model scope entries of both observation indexes, merged in
/// key order, which is Provider ID and then model bytes. Each examined pair
/// costs at most two seeks and two range checks, whatever its record count;
/// at most `scan_limit` pairs are examined and `limit` returned, each
/// clamped to at least 1 and `limit` to [`MAX_PAGE`]. The cursor is the
/// last pair examined, so a page whose pairs all fell outside the time range
/// still advances. One call reads one snapshot.
pub(crate) fn read_observed_models(
    database: &Database,
    filter: &ObservedModelFilter,
    after: Option<&ObservedModel>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservedModelPage, StorageError> {
    let (limit, scan_limit) = (limit.clamp(1, MAX_PAGE), scan_limit.max(1));
    let (from, until) = (
        filter.observed.from.as_unix_micros(),
        filter.observed.until.as_unix_micros(),
    );
    let done = |choices| {
        Ok(ObservedModelPage {
            choices,
            next: None,
        })
    };
    if from >= until {
        return done(Vec::new());
    }
    let (lower, upper) = match filter.provider {
        Some(provider) => {
            let lower = Scope::ProviderModel(provider, "").key();
            let upper = match provider.get().checked_add(1) {
                Some(following) => Scope::ProviderModel(LlmProviderId::new(following), "").key(),
                None => vec![PROVIDER_MODEL_TAG + 1],
            };
            (lower, upper)
        }
        None => (vec![PROVIDER_MODEL_TAG], vec![PROVIDER_MODEL_TAG + 1]),
    };
    let after = after.map(|model| Scope::ProviderModel(model.provider, &model.model).key());
    let rx = read(database)?;
    let tables = [
        rx.open_table(METRICS_BY_SCOPE)?,
        rx.open_table(AVAILABILITY_BY_SCOPE)?,
    ];
    let mut heads = [None, None];
    for (head, table) in heads.iter_mut().zip(&tables) {
        *head = next_provider_model(table, after.as_deref(), &lower, &upper)?;
    }
    let (mut choices, mut examined, mut last) = (Vec::new(), 0, None);
    while choices.len() < limit && examined < scan_limit {
        let Some(key) = heads.iter().flatten().min().cloned() else {
            return done(choices);
        };
        examined += 1;
        let mut present = [false, false];
        for ((head, table), present) in heads.iter_mut().zip(&tables).zip(&mut present) {
            if head.as_deref() == Some(key.as_slice()) {
                *present = observed_within(table, &key, from, until)?;
                *head = next_provider_model(table, Some(&key), &lower, &upper)?;
            }
        }
        let model = observed_model(&key)?;
        if present.contains(&true) {
            choices.push(ObservedModelChoice {
                model: model.clone(),
                has_metrics: present[0],
                has_availability_stats: present[1],
            });
        }
        last = Some(model);
    }
    let more = heads.iter().any(Option::is_some);
    Ok(ObservedModelPage {
        choices,
        next: if more { last } else { None },
    })
}

/// One page of the Metrics `filter` selects, see [`scan_observations`].
pub(crate) fn read_metrics(
    database: &Database,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<MetricsId>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<MetricsRecord, MetricsId>, StorageError> {
    let after = after.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: cursor.record.get(),
    });
    let page = scan_observations(
        &read(database)?,
        METRICS_BY_SCOPE,
        MEASURES,
        metrics_from,
        filter,
        after,
        limit,
        scan_limit,
    )?;
    let next = page.next.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: MetricsId::new(cursor.record),
    });
    Ok(ObservationPage {
        records: page.records,
        next,
    })
}

/// One page of the AvailabilityStats `filter` selects, see [`scan_observations`].
pub(crate) fn read_availability_stats(
    database: &Database,
    filter: &ObservationFilter,
    after: Option<ObservationCursor<AvailabilityStatId>>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservationPage<AvailabilityStatRecord, AvailabilityStatId>, StorageError> {
    let after = after.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: cursor.record.get(),
    });
    let page = scan_observations(
        &read(database)?,
        AVAILABILITY_BY_SCOPE,
        OBSERVES,
        availability_from,
        filter,
        after,
        limit,
        scan_limit,
    )?;
    let next = page.next.map(|cursor| ObservationCursor {
        observed_at: cursor.observed_at,
        record: AvailabilityStatId::new(cursor.record),
    });
    Ok(ObservationPage {
        records: page.records,
        next,
    })
}

/// Test fault probe: replaces IN_PROJECT with a table of another key type,
/// so every later read that traverses a Job's Projects fails instead of
/// succeeding unnoticed.
#[cfg(test)]
pub(crate) fn break_project_links(database: &Database) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.delete_table(IN_PROJECT)?;
    tx.open_table(TableDefinition::<&str, u64>::new("in_project"))?;
    tx.commit()?;
    Ok(())
}

/// Test fault: removes a Job's HAS_INPUT edge, leaving its incoming index.
#[cfg(test)]
pub(crate) fn remove_input_link(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(HAS_INPUT)?.remove(job.get())?;
    tx.commit()?;
    Ok(())
}

/// Test fault: removes a Job's row while its observations still link to it,
/// a dangling observation reference.
#[cfg(test)]
pub(crate) fn remove_job_row(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(JOBS)?.remove(job.get())?;
    tx.commit()?;
    Ok(())
}

/// Checks every relationship table against its incoming index and its
/// endpoint tables, and the Genesis root against the parent tree. Returns
/// one line per inconsistency.
#[cfg(test)]
pub(crate) fn relationship_inconsistencies(
    database: &Database,
) -> Result<Vec<String>, StorageError> {
    use std::collections::BTreeMap;

    fn ids(rx: &ReadTransaction, table: Records) -> Result<BTreeSet<u64>, StorageError> {
        let mut ids = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            ids.insert(entry?.0.value());
        }
        Ok(ids)
    }
    fn link_pairs(rx: &ReadTransaction, table: Link) -> Result<BTreeSet<(u64, u64)>, StorageError> {
        let mut pairs = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            let (from, to) = entry?;
            pairs.insert((from.value(), to.value()));
        }
        Ok(pairs)
    }
    fn set_pairs(
        rx: &ReadTransaction,
        table: PairSet,
    ) -> Result<BTreeSet<(u64, u64)>, StorageError> {
        let mut pairs = BTreeSet::new();
        for entry in rx.open_table(table)?.iter()? {
            pairs.insert(entry?.0.value());
        }
        Ok(pairs)
    }
    fn reversed(pairs: &BTreeSet<(u64, u64)>) -> BTreeSet<(u64, u64)> {
        pairs.iter().map(|(from, to)| (*to, *from)).collect()
    }

    let rx = read(database)?;
    let genesis = genesis_id(&rx.open_table(META)?)?.get();
    let jobs = ids(&rx, JOBS)?;
    let inputs = ids(&rx, JOB_INPUTS)?;
    let executors = ids(&rx, EXECUTORS)?;
    let projects = ids(&rx, PROJECTS)?;
    let agents = ids(&rx, AGENTS)?;
    let providers = ids(&rx, LLM_PROVIDERS)?;
    let metrics = ids(&rx, METRICS)?;
    let availability_stats = ids(&rx, AVAILABILITY_STATS)?;
    let attachments = ids(&rx, ATTACHMENTS)?;
    let mut profile_kinds = BTreeMap::new();
    for entry in rx.open_table(PROFILES)?.iter()? {
        let (id, bytes) = entry?;
        profile_kinds.insert(id.value(), ProfileRow::decode(bytes.value())?.config.kind());
    }
    let agent_profiles: BTreeSet<u64> = profile_kinds
        .iter()
        .filter(|(_, kind)| **kind == ProfileKind::Agent)
        .map(|(id, _)| *id)
        .collect();
    let llm_profiles: BTreeSet<u64> = profile_kinds
        .iter()
        .filter(|(_, kind)| **kind == ProfileKind::Llm)
        .map(|(id, _)| *id)
        .collect();

    let mut problems = Vec::new();
    let mut has_attachment = BTreeSet::new();
    let mut positions: BTreeMap<u64, u32> = BTreeMap::new();
    for entry in rx.open_table(HAS_ATTACHMENT)?.iter()? {
        let (key, attachment) = entry?;
        let (input, position) = key.value();
        let next = positions.entry(input).or_default();
        if position != *next {
            problems.push(format!(
                "the Attachments of input {input} are not numbered from 0 without gaps"
            ));
        }
        *next = position.saturating_add(1);
        if !has_attachment.insert((input, attachment.value())) {
            problems.push(format!(
                "input {input} names attachment {} more than once",
                attachment.value()
            ));
        }
    }
    let mut check_endpoints =
        |name: &str, pairs: &BTreeSet<(u64, u64)>, from: &BTreeSet<u64>, to: &BTreeSet<u64>| {
            for (source, target) in pairs {
                if !from.contains(source) || !to.contains(target) {
                    problems.push(format!(
                        "{name} ({source}, {target}) names a missing or mistyped endpoint"
                    ));
                }
            }
        };

    let has_input = link_pairs(&rx, HAS_INPUT)?;
    let child_of = link_pairs(&rx, CHILD_OF)?;
    let retry_of = link_pairs(&rx, RETRY_OF)?;
    let in_project = set_pairs(&rx, IN_PROJECT)?;
    let uses_profile = set_pairs(&rx, USES_PROFILE)?;
    let uses_provider = link_pairs(&rx, USES_PROVIDER)?;
    let measures = link_pairs(&rx, MEASURES)?;
    let observes = link_pairs(&rx, OBSERVES)?;
    check_endpoints("HAS_INPUT", &has_input, &jobs, &inputs);
    check_endpoints("CHILD_OF", &child_of, &jobs, &jobs);
    check_endpoints(
        "ASSIGNED_TO",
        &link_pairs(&rx, ASSIGNED_TO)?,
        &jobs,
        &executors,
    );
    check_endpoints("PREVIOUS", &link_pairs(&rx, PREVIOUS)?, &jobs, &jobs);
    check_endpoints("REPLACES", &link_pairs(&rx, REPLACES)?, &jobs, &jobs);
    check_endpoints("RETRY_OF", &retry_of, &jobs, &jobs);
    check_endpoints("IN_PROJECT", &in_project, &jobs, &projects);
    check_endpoints("USES_PROFILE", &uses_profile, &agents, &agent_profiles);
    check_endpoints("USES_PROVIDER", &uses_provider, &llm_profiles, &providers);
    check_endpoints("MEASURES", &measures, &metrics, &jobs);
    check_endpoints("OBSERVES", &observes, &availability_stats, &jobs);
    let targets_llm = set_pairs(&rx, TARGETS_LLM)?;
    let applies_profile = link_pairs(&rx, APPLIES_PROFILE)?;
    let runs_llm_profile = link_pairs(&rx, RUNS_LLM_PROFILE)?;
    check_endpoints("TARGETS_LLM", &targets_llm, &agent_profiles, &llm_profiles);
    check_endpoints("APPLIES_PROFILE", &applies_profile, &jobs, &agent_profiles);
    check_endpoints("RUNS_LLM_PROFILE", &runs_llm_profile, &jobs, &llm_profiles);
    check_endpoints("HAS_ATTACHMENT", &has_attachment, &inputs, &attachments);

    let mut executor_rows = BTreeMap::new();
    let mut expected_transports = BTreeSet::new();
    for entry in rx.open_table(EXECUTORS)?.iter()? {
        let (id, bytes) = entry?;
        let row = ExecutorRow::decode(bytes.value())?;
        if !row.llm_transports.is_empty() && row.executor_type != ExecutorType::Llm {
            problems.push(format!(
                "executor {} is not an Llm Executor but names LLM transports",
                id.value()
            ));
        }
        expected_transports.extend(
            row.llm_transports
                .iter()
                .map(|transport| (transport.code(), id.value())),
        );
        executor_rows.insert(id.value(), row);
    }
    let mut indexed_transports = BTreeSet::new();
    for entry in rx.open_table(TRANSPORT_EXECUTORS)?.iter()? {
        indexed_transports.insert(entry?.0.value());
    }
    if indexed_transports != expected_transports {
        problems.push("the LLM transport index and the Executor records disagree".into());
    }

    let ordinary_children: BTreeSet<(u64, u64)> = child_of
        .iter()
        .copied()
        .filter(|(child, parent)| child != parent)
        .collect();
    for (name, forward, incoming) in [
        ("HAS_INPUT", &has_input, set_pairs(&rx, INPUT_JOBS)?),
        ("CHILD_OF", &ordinary_children, set_pairs(&rx, CHILDREN)?),
        ("RETRY_OF", &retry_of, set_pairs(&rx, RETRIES)?),
        ("IN_PROJECT", &in_project, set_pairs(&rx, PROJECT_JOBS)?),
        (
            "USES_PROFILE",
            &uses_profile,
            set_pairs(&rx, PROFILE_AGENTS)?,
        ),
        (
            "USES_PROVIDER",
            &uses_provider,
            set_pairs(&rx, PROVIDER_PROFILES)?,
        ),
        ("MEASURES", &measures, set_pairs(&rx, JOB_METRICS)?),
        ("OBSERVES", &observes, set_pairs(&rx, JOB_AVAILABILITY)?),
        (
            "APPLIES_PROFILE",
            &applies_profile,
            set_pairs(&rx, AGENT_PROFILE_JOBS)?,
        ),
        (
            "RUNS_LLM_PROFILE",
            &runs_llm_profile,
            set_pairs(&rx, LLM_PROFILE_JOBS)?,
        ),
        (
            "HAS_ATTACHMENT",
            &has_attachment,
            set_pairs(&rx, ATTACHMENT_INPUTS)?,
        ),
    ] {
        if reversed(forward) != incoming {
            problems.push(format!("{name} and its incoming index disagree"));
        }
    }

    let roots: Vec<u64> = child_of
        .iter()
        .filter(|(child, parent)| child == parent)
        .map(|(child, _)| *child)
        .collect();
    if roots != [genesis] {
        problems.push(format!(
            "expected only the Genesis Job {genesis} to be its own parent, found {roots:?}"
        ));
    }
    let parents: BTreeMap<u64, u64> = child_of.iter().copied().collect();
    let job_inputs: BTreeSet<u64> = has_input.iter().map(|(job, _)| *job).collect();
    for job in &jobs {
        if !job_inputs.contains(job) {
            problems.push(format!("job {job} has no input"));
        }
        let mut ancestor = *job;
        let mut steps = 0;
        while ancestor != genesis {
            match parents.get(&ancestor) {
                Some(parent) if steps <= jobs.len() => ancestor = *parent,
                _ => {
                    problems.push(format!("job {job} does not reach the Genesis Job"));
                    break;
                }
            }
            steps += 1;
        }
    }
    let agent_profile_of: BTreeMap<u64, u64> = applies_profile.iter().copied().collect();
    let llm_profile_of: BTreeMap<u64, u64> = runs_llm_profile.iter().copied().collect();
    let mut targets = HashMap::new();
    for (name, records, links, index, decode_at) in [
        (
            "metrics",
            METRICS,
            &measures,
            METRICS_BY_SCOPE,
            (|bytes| MetricsRow::decode(bytes).map(|row| row.observed_at))
                as fn(&[u8]) -> DecodeResult<Timestamp>,
        ),
        (
            "availability_stat",
            AVAILABILITY_STATS,
            &observes,
            AVAILABILITY_BY_SCOPE,
            |bytes| AvailabilityStatRow::decode(bytes).map(|row| row.observed_at),
        ),
    ] {
        let job_of: BTreeMap<u64, u64> = links.iter().copied().collect();
        let mut expected = BTreeSet::new();
        for entry in rx.open_table(records)?.iter()? {
            let (id, bytes) = entry?;
            let observed_at = decode_at(bytes.value())?.as_unix_micros();
            let Some(job) = job_of.get(&id.value()) else {
                problems.push(format!("{name} {} is linked to no Job", id.value()));
                continue;
            };
            let target = job_target(&rx, &mut targets, JobId::new(*job))?;
            if target.is_none() && name == "availability_stat" {
                problems.push(format!(
                    "{name} {} observes a Job that ran no LLM",
                    id.value()
                ));
            }
            // Stated here rather than taken from `Scope::of`: a record without
            // an LLM belongs to `All` only.
            let mut scopes = vec![Scope::All];
            if let Some(target) = &target {
                let (provider, model) = (
                    target.frozen.provider.id,
                    target.frozen.config.model.as_str(),
                );
                scopes.extend([
                    Scope::LlmProfile(target.llm_profile),
                    Scope::Provider(provider),
                    Scope::Model(model),
                    Scope::ProviderModel(provider, model),
                ]);
            }
            for scope in scopes {
                expected.insert((scope.key(), observed_at, id.value()));
            }
        }
        let mut indexed = BTreeSet::new();
        for entry in rx.open_table(index)?.iter()? {
            let key = entry?.0;
            let (scope, at, id) = key.value();
            indexed.insert((scope.to_vec(), at, id));
        }
        if indexed != expected {
            problems.push(format!(
                "the {name} scope index disagrees with its records and their Jobs' frozen targets"
            ));
        }
    }
    let assigned: BTreeMap<u64, u64> = link_pairs(&rx, ASSIGNED_TO)?.into_iter().collect();
    let input_of: BTreeMap<u64, u64> = has_input.iter().copied().collect();
    let job_inputs_table = rx.open_table(JOB_INPUTS)?;
    let jobs_table = rx.open_table(JOBS)?;
    for job in &jobs {
        let frozen = match input_of.get(job) {
            Some(input) => job_inputs_table
                .get(*input)?
                .map(|v| JobInputRow::decode(v.value()))
                .transpose()?
                .and_then(|row| row.configuration),
            None => None,
        };
        let row = jobs_table
            .get(*job)?
            .map(|v| JobRow::decode(v.value()))
            .transpose()?;
        if row
            .as_ref()
            .is_some_and(|row| row.stop_confirmed_at.is_some() && row.unknown_at.is_none())
        {
            problems.push(format!(
                "job {job} has a confirmed stop without an unknown outcome"
            ));
        }
        let holds = row.is_some_and(|row| row.holds_profiles());
        match (agent_profile_of.get(job), llm_profile_of.get(job), frozen) {
            (None, None, None) => {}
            (agent_profile, Some(llm_profile), Some(frozen))
                if agent_profile.is_some() == frozen.agent.is_some() =>
            {
                // A holding Job holds both Profiles, so its submission-time
                // target membership still holds; released work keeps only provenance.
                if let Some(agent_profile) = agent_profile
                    && holds
                    && !targets_llm.contains(&(*agent_profile, *llm_profile))
                {
                    problems.push(format!(
                        "holding job {job} runs an LLM Profile its Agent Profile no longer targets"
                    ));
                }
                if let Some(executor) = assigned.get(job)
                    && !executor_rows.get(executor).is_some_and(|row| {
                        row.executor_type == ExecutorType::Llm
                            && row.llm_transports.contains(&frozen.llm.provider.transport)
                    })
                {
                    problems.push(format!("job {job} is assigned an Executor that does not support its frozen transport"));
                }
            }
            _ => problems.push(format!(
                "job {job} has an incomplete configuration or snapshot"
            )),
        }
    }
    let mut indexed_tool_call_ids = BTreeSet::new();
    for entry in rx.open_table(TOOL_CALL_IDS)?.iter()? {
        let (key, value) = entry?;
        let (job, id) = key.value();
        let (request, index) = value.value();
        indexed_tool_call_ids.insert((job, id.to_owned(), request, index));
    }
    let mut expected_tool_call_ids = BTreeSet::new();
    for entry in rx.open_table(TOOL_EVIDENCE)?.iter()? {
        let (key, bytes) = entry?;
        let call = tool_call_ref(key.value())?;
        let record = decode_tool_evidence(call, bytes.value())?;
        let job = jobs_table
            .get(call.job.get())?
            .map(|v| JobRow::decode(v.value()))
            .transpose()?;
        match job {
            None => problems.push(format!("{call} belongs to a missing Job")),
            Some(job) if record.is_unresolved() && job.confirmed_terminal() => {
                problems.push(format!(
                    "{call} is unresolved but its Job has a confirmed outcome"
                ));
            }
            Some(_) => {}
        }
        expected_tool_call_ids.insert((
            call.job.get(),
            record.tool_call_id,
            call.request.get(),
            call.index,
        ));
    }
    if indexed_tool_call_ids != expected_tool_call_ids {
        problems.push("the tool call id index and the tool evidence disagree".into());
    }
    let mut submitted = BTreeSet::new();
    for entry in rx.open_table(SUBMISSIONS)?.iter()? {
        let (key, bytes) = entry?;
        let record = decode_submission(key.value(), bytes.value())?;
        if !jobs.contains(&record.job.get()) {
            problems.push(format!("{:?} names the missing {}", record.key, record.job));
        }
        if !submitted.insert(record.job) {
            problems.push(format!("{} was created by two submissions", record.job));
        }
    }
    for (profile, kind) in &profile_kinds {
        let has_provider = uses_provider.iter().any(|(source, _)| source == profile);
        if (*kind == ProfileKind::Llm) != has_provider {
            problems.push(format!(
                "profile {profile} and its USES_PROVIDER edge disagree"
            ));
        }
    }
    Ok(problems)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{self, JobInputDraft, NewJob};
    use redb::ReadableTableMetadata;
    use std::fs;

    /// Removes its directory when dropped.
    struct TestRoot(PathBuf);

    impl Drop for TestRoot {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).ok();
        }
    }

    struct TestDatabase {
        database: Database,
        epoch: u64,
        genesis: JobId,
        // Declared last so the database closes before its directory goes.
        root: TestRoot,
    }

    impl TestDatabase {
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("ocg-storage-{}-{name}", std::process::id()));
            fs::remove_dir_all(&root).ok();
            fs::create_dir_all(&root).expect("create storage test directory");
            Self::open(TestRoot(root))
        }

        fn open(root: TestRoot) -> Self {
            let opened = open(&root.0).unwrap_or_else(|_| panic!("open storage test database"));
            Self {
                database: opened.database,
                epoch: opened.epoch,
                genesis: opened.genesis,
                root,
            }
        }

        /// Closes and reopens the same file.
        fn reopen(self) -> Self {
            let Self { database, root, .. } = self;
            drop(database);
            Self::open(root)
        }

        fn batch<T>(
            &self,
            sequence: u64,
            body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
        ) -> Result<T, BatchFailure> {
            run_batch(
                &self.database,
                OperationId::new(self.epoch, sequence),
                OperationKind::CreateJobWithInput,
                body,
            )
            .result
        }

        fn assert_consistent(&self) {
            let problems =
                relationship_inconsistencies(&self.database).expect("scan relationships");
            assert!(
                problems.is_empty(),
                "relationship inconsistencies: {problems:#?}"
            );
        }
    }

    fn new_job(
        name: &str,
        parent: JobId,
        dependencies: Vec<(JobId, DependencyCondition)>,
    ) -> NewJob {
        NewJob {
            name: Some(name.to_owned()),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent,
            projects: Vec::new(),
            dependencies,
            configuration: None,
            assigned_executor: None,
            previous: None,
            replaces: None,
        }
    }

    fn create_job(test: &TestDatabase, sequence: u64, job: NewJob) -> Result<JobId, BatchFailure> {
        test.batch(sequence, |batch| {
            let (input, _) = persistence::save_job_input(
                batch,
                None,
                JobInputDraft {
                    objective: job.name.clone(),
                    ..JobInputDraft::default()
                },
            )?;
            persistence::create_job_with_input(batch, job, input)
        })
    }

    #[test]
    fn poisoned_batch_rolls_back_records_sequences_and_marker() {
        let test = TestDatabase::new("poison");
        let outcome = run_batch(
            &test.database,
            OperationId::new(test.epoch, 1),
            OperationKind::SaveJobInput,
            |batch| {
                let (input, _) = persistence::save_job_input(
                    batch,
                    None,
                    JobInputDraft {
                        objective: Some("must roll back".into()),
                        ..JobInputDraft::default()
                    },
                )?;
                let _ = batch.reject::<()>(Violation::ValueOutOfRange("poisoned test"));
                Ok((
                    input,
                    MarkerSubjects::created(RecordRef::JobInput(input), RecordRef::JobInput(input)),
                ))
            },
        );

        assert!(matches!(
            outcome.result,
            Err(BatchFailure::Rejected(Violation::ValueOutOfRange(
                "poisoned test"
            )))
        ));
        // The Genesis Job's input took the first input ID.
        let rolled_back = JobInputId::new(2);
        assert!(
            read_job_input(&test.database, rolled_back)
                .expect("read rolled-back input")
                .is_none()
        );
        assert!(
            read_marker(&test.database, OperationId::new(test.epoch, 1))
                .expect("read rolled-back marker")
                .is_none()
        );

        let committed = run_batch(
            &test.database,
            OperationId::new(test.epoch, 2),
            OperationKind::SaveJobInput,
            |batch| {
                let (input, subjects) =
                    persistence::save_job_input(batch, None, JobInputDraft::default())?;
                Ok((input, subjects))
            },
        )
        .result
        .expect("unpoisoned batch commits");
        assert_eq!(
            committed, rolled_back,
            "the rolled-back sequence allocation must be reusable"
        );
        assert!(
            read_marker(&test.database, OperationId::new(test.epoch, 2))
                .expect("read committed marker")
                .is_some()
        );
    }

    #[test]
    fn waiting_cycle_is_rejected_atomically() {
        let test = TestDatabase::new("cycle");
        let first = create_job(&test, 1, new_job("first", test.genesis, Vec::new()))
            .expect("create first job");
        let second =
            create_job(&test, 2, new_job("second", first, Vec::new())).expect("create second job");

        let cycle = create_job(
            &test,
            3,
            new_job("cycle", second, vec![(first, DependencyCondition::Success)]),
        );
        match cycle {
            Err(BatchFailure::Rejected(Violation::WaitingCycle(jobs))) => assert!(!jobs.is_empty()),
            other => panic!("expected waiting-cycle rejection, got {other:?}"),
        }

        let jobs = read_jobs(&test.database, None, 100).expect("read jobs after rejection");
        assert_eq!(
            jobs.len(),
            3,
            "the cyclic job and its input must not commit beside Genesis and the two jobs"
        );
        let third = create_job(&test, 4, new_job("third", test.genesis, Vec::new()))
            .expect("create after rejection");
        assert_eq!(
            third.get(),
            second.get() + 1,
            "the rejected transaction must roll back its job sequence"
        );
        test.assert_consistent();
    }

    #[test]
    fn genesis_is_created_once_and_reused_after_reopening() {
        let test = TestDatabase::new("genesis");
        let genesis = test.genesis;
        let record = read_job(&test.database, genesis)
            .expect("read Genesis")
            .expect("Genesis exists");
        assert_eq!(record.parent, genesis, "Genesis is its own parent");
        assert!(
            read_job_input(&test.database, record.input)
                .expect("read Genesis input")
                .is_some_and(|input| input.submitted_at.is_some())
        );
        assert!(
            read_children(&test.database, genesis, None, 100)
                .expect("read children")
                .is_empty(),
            "the root marker is not a child"
        );
        let child = create_job(&test, 1, new_job("child", genesis, Vec::new()))
            .expect("create child of Genesis");
        test.assert_consistent();

        let test = test.reopen();
        assert_eq!(
            test.genesis, genesis,
            "reopening reuses the recorded Genesis Job"
        );
        let roots: Vec<_> = read_jobs(&test.database, None, 100)
            .expect("read jobs")
            .into_iter()
            .filter(|job| job.parent == job.id)
            .map(|job| job.id)
            .collect();
        assert_eq!(roots, [genesis], "no second root is created on reopen");
        assert_eq!(
            read_children(&test.database, genesis, None, 100).expect("read children"),
            [child]
        );
        test.assert_consistent();
    }

    #[test]
    fn parent_edges_are_validated_fixed_and_acyclic_without_partial_writes() {
        let test = TestDatabase::new("parents");
        let parent = create_job(&test, 1, new_job("parent", test.genesis, Vec::new()))
            .expect("create parent");
        let child =
            create_job(&test, 2, new_job("child", parent, Vec::new())).expect("create child");
        let other =
            create_job(&test, 3, new_job("other", test.genesis, Vec::new())).expect("create other");

        let missing = JobId::new(999);
        match create_job(&test, 4, new_job("orphan", missing, Vec::new())) {
            Err(BatchFailure::Rejected(Violation::NotFound(RecordRef::Job(id)))) => {
                assert_eq!(id, missing)
            }
            other => panic!("expected a missing parent to be refused, got {other:?}"),
        }
        type Expected = fn(&Violation) -> bool;
        let attempts: [(JobId, JobId, Expected); 4] = [
            (other, other, |violation| {
                matches!(violation, Violation::SelfParent(_))
            }),
            (child, other, |violation| {
                matches!(violation, Violation::ParentFixed(_))
            }),
            (parent, child, |violation| {
                matches!(violation, Violation::ParentCycle { .. })
            }),
            (test.genesis, other, |violation| {
                matches!(violation, Violation::ParentCycle { .. })
            }),
        ];
        for (sequence, (from, to, expected)) in (5..).zip(attempts) {
            let outcome = test.batch(sequence, |batch| {
                batch.link_child(from, to)?;
                Ok(((), MarkerSubjects::addressed(RecordRef::Job(from))))
            });
            match outcome {
                Err(BatchFailure::Rejected(violation)) => assert!(
                    expected(&violation),
                    "{from} -> {to}: unexpected {violation:?}"
                ),
                other => panic!("{from} -> {to} must be refused, got {other:?}"),
            }
        }

        for (job, parent) in [
            (parent, test.genesis),
            (child, parent),
            (other, test.genesis),
            (test.genesis, test.genesis),
        ] {
            assert_eq!(
                read_job(&test.database, job)
                    .expect("read job")
                    .expect("job exists")
                    .parent,
                parent
            );
        }
        assert_eq!(
            read_jobs(&test.database, None, 100)
                .expect("read jobs")
                .len(),
            4,
            "no orphan was committed"
        );
        assert_eq!(
            read_children(&test.database, other, None, 100).expect("read children"),
            Vec::<JobId>::new()
        );
        test.assert_consistent();
    }

    #[test]
    fn insert_paths_cannot_rebind_a_job_profile_or_executor() {
        use crate::persistence::{
            AgentConfig, ExecutionConfiguration, LlmConfig, LlmParameters, LlmTransport,
            NewExecutor, NewLlmProvider, NewProfile, NewProfileConfig,
        };
        let test = TestDatabase::new("configuration");
        let (first, second, agent, other_agent, job, executor) = test
            .batch(1, |batch| {
                let provider = NewLlmProvider {
                    name: "p".into(),
                    transport: LlmTransport::OpenAiChatCompletions,
                    endpoint: None,
                    credential_ref: None,
                };
                let (provider, _) = persistence::create_llm_provider(batch, provider)?;
                let mut llm = |model: &str| {
                    let config = LlmConfig {
                        model: model.into(),
                        parameters: LlmParameters::default(),
                        declared_capabilities: BTreeSet::new(),
                    };
                    persistence::create_profile(
                        batch,
                        NewProfile {
                            name: model.into(),
                            config: NewProfileConfig::Llm { provider, config },
                        },
                    )
                    .map(|(id, _)| id)
                };
                let (first, second) = (llm("first")?, llm("second")?);
                let mut agent = |llm_targets| {
                    let config = NewProfileConfig::Agent {
                        config: AgentConfig::default(),
                        llm_targets,
                    };
                    persistence::create_profile(
                        batch,
                        NewProfile {
                            name: "agent".into(),
                            config,
                        },
                    )
                    .map(|(id, _)| id)
                };
                let (agent, other_agent) = (agent(vec![first, second])?, agent(vec![second])?);
                let (executor, _) = persistence::create_executor(
                    batch,
                    NewExecutor {
                        name: "llm".into(),
                        executor_type: ExecutorType::Llm,
                        concurrency_limit: std::num::NonZeroU32::MIN,
                        llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
                    },
                )?;
                let (input, _) =
                    persistence::save_job_input(batch, None, JobInputDraft::default())?;
                let configuration = Some(ExecutionConfiguration {
                    agent_profile: Some(agent),
                    llm_profile: first,
                });
                let (job, _) = persistence::create_job_with_input(
                    batch,
                    NewJob {
                        configuration,
                        ..new_job("child", test.genesis, Vec::new())
                    },
                    input,
                )?;
                Ok((
                    (first, second, agent, other_agent, job, executor),
                    MarkerSubjects::addressed(RecordRef::Job(job)),
                ))
            })
            .expect("create configuration");
        let before = (
            read_job(&test.database, job).expect("read job"),
            read_profile(&test.database, agent).expect("read Agent Profile"),
            read_profile(&test.database, first).expect("read LLM Profile"),
            read_executor(&test.database, executor).expect("read Executor"),
        );

        // Every rebinding names Profiles that exist and would be valid on a new Job.
        for (sequence, rebinding) in (2..).zip([
            ExecutionConfiguration {
                agent_profile: Some(agent),
                llm_profile: second,
            },
            ExecutionConfiguration {
                agent_profile: Some(other_agent),
                llm_profile: second,
            },
            ExecutionConfiguration {
                agent_profile: None,
                llm_profile: second,
            },
        ]) {
            let outcome = test.batch(sequence, |batch| {
                batch.link_configuration(job, rebinding)?;
                Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
            });
            assert!(
                matches!(
                    outcome,
                    Err(BatchFailure::Rejected(Violation::ConfigurationFixed(_)))
                ),
                "got {outcome:?}"
            );
        }
        let readd = test.batch(5, |batch| {
            let row = ProfileRow {
                id: agent,
                name: "agent".into(),
                config: ProfileConfig::Agent(AgentConfig::default()),
                created_at: batch.now(),
                updated_at: batch.now(),
            };
            batch.insert_profile(
                &row,
                &ProfileLinks::Agent {
                    llm_targets: vec![second],
                },
            )?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Profile(agent))))
        });
        assert!(
            matches!(
                readd,
                Err(BatchFailure::Rejected(Violation::DuplicateId(
                    RecordRef::Profile(_)
                )))
            ),
            "got {readd:?}"
        );
        let reregister = test.batch(6, |batch| {
            let row = batch.require_executor_row(executor)?;
            batch.insert_executor(&ExecutorRow {
                llm_transports: BTreeSet::new(),
                ..row
            })?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Executor(executor))))
        });
        assert!(
            matches!(
                reregister,
                Err(BatchFailure::Rejected(Violation::DuplicateId(
                    RecordRef::Executor(_)
                )))
            ),
            "got {reregister:?}"
        );

        let after = (
            read_job(&test.database, job).expect("read job"),
            read_profile(&test.database, agent).expect("read Agent Profile"),
            read_profile(&test.database, first).expect("read LLM Profile"),
            read_executor(&test.database, executor).expect("read Executor"),
        );
        assert_eq!(after, before, "refused writes changed nothing");
        test.assert_consistent();
    }

    #[test]
    fn a_file_of_another_format_version_is_refused_before_any_write() {
        let test = TestDatabase::new("format");
        let path = database_file(&test.root.0);
        let write_meta = |database: &Database, version: u32| {
            let tx = database.begin_write().expect("begin write");
            tx.open_table(META)
                .expect("open meta")
                .insert("format_version", version.to_le_bytes().as_slice())
                .expect("write version");
            tx.commit().expect("commit version");
        };
        // The version before this build's, which is refused like any other.
        let previous = FORMAT_VERSION - 1;
        write_meta(&test.database, previous);
        let state = |database: &Database| {
            let rx = database.begin_read().expect("begin read");
            let epoch = rx
                .open_table(SEQUENCES)
                .expect("open sequences")
                .get(OPEN_EPOCH)
                .expect("read epoch")
                .map(|v| v.value());
            let version = rx
                .open_table(META)
                .expect("open meta")
                .get("format_version")
                .expect("read version")
                .map(|v| v.value().to_vec());
            let jobs = rx
                .open_table(JOBS)
                .expect("open jobs")
                .len()
                .expect("count jobs");
            (epoch, version, jobs)
        };
        let before = state(&test.database);
        let TestDatabase { root, database, .. } = test;
        drop(database);
        let bytes = fs::read(&path).expect("read the file");

        match open(&root.0) {
            Err(OpenError::UnsupportedFormat { found }) => assert_eq!(found, previous),
            Err(_) => panic!("expected an unsupported-format refusal"),
            Ok(_) => panic!("a file of the previous format must not open"),
        }
        assert!(
            fs::read(&path).expect("reread the file") == bytes,
            "the refused file is byte-for-byte unchanged"
        );
        let database = Database::create(&path).expect("inspect the refused file");
        assert_eq!(
            state(&database),
            before,
            "the refused open must not change the file"
        );
        write_meta(&database, FORMAT_VERSION);
        drop(database);
        assert!(
            open(&root.0).is_ok(),
            "the untouched file still opens at the supported version"
        );
    }

    fn retry(original: JobId, acknowledge_repeated_effects: bool) -> persistence::JobRetry {
        persistence::JobRetry {
            original,
            input: persistence::RetryInput::Unchanged,
            name: None,
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration: None,
            assigned_executor: None,
            acknowledge_repeated_effects,
        }
    }

    fn fail(test: &TestDatabase, sequence: u64, job: JobId) {
        test.batch(sequence, |batch| {
            persistence::finalize_job(
                batch,
                job,
                persistence::JobFinalization::Failed(persistence::FinalResult {
                    summary: None,
                    code: None,
                    goal_reached: Some(false),
                    value: None,
                }),
            )
        })
        .expect("fail the Job");
    }

    /// Three Jobs created as they would be and linked by `edges` as
    /// (retry, original) directly.
    fn retry_chain(test: &TestDatabase, edges: &[(usize, usize)]) -> [JobId; 3] {
        let jobs = [1, 2, 3].map(|sequence| {
            create_job(test, sequence, new_job("job", test.genesis, Vec::new())).expect("create")
        });
        test.batch(4, |batch| {
            for (retry, original) in edges {
                batch.link_retry(jobs[*retry], jobs[*original])?;
            }
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(jobs[0]))))
        })
        .expect("link the retries");
        jobs
    }

    #[test]
    fn a_retry_is_refused_over_an_unresolved_effect_anywhere_in_its_lineage() {
        let test = TestDatabase::new("retry-lineage-unresolved");
        // The first Job's effect is unresolved; the two retries after it
        // ran no tool and failed.
        let [first, second, third] = retry_chain(&test, &[(1, 0), (2, 1)]);
        let call = ToolCallRef {
            job: first,
            request: RequestNumber::FIRST,
            index: 0,
        };
        test.batch(8, |batch| {
            batch.insert_tool_evidence(&ToolEvidenceRecord {
                call,
                tool_call_id: "c".into(),
                name: "add".into(),
                decision: Recorded {
                    fact: ToolDecision::Admitted(ToolAdmission {
                        handler_version: "test-1".into(),
                        effect: ToolEffect::NonIdempotent,
                        parameters: "{}".into(),
                        arguments: ToolPayload::new("{}"),
                        authorization: AuthorizationBasis::RegistryGrant,
                    }),
                    at: batch.now(),
                },
                outcome: None,
                reconciliation: None,
            })?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(first))))
        })
        .expect("record the unresolved admission");
        test.batch(9, |batch| {
            persistence::finalize_job(batch, first, persistence::JobFinalization::Unknown)?;
            persistence::confirm_job_stopped(batch, first)
        })
        .expect("settle the first Job unknown");
        fail(&test, 10, second);
        fail(&test, 11, third);
        for acknowledge in [false, true] {
            assert!(
                matches!(
                    test.batch(12, |batch| persistence::retry_job(batch, retry(third, acknowledge))),
                    Err(BatchFailure::Rejected(Violation::UnresolvedToolEffect(found))) if found == call
                ),
                "acknowledged: {acknowledge}"
            );
        }
        test.assert_consistent();
    }

    #[test]
    fn a_malformed_retry_lineage_refuses_the_retry() {
        // A cycle of retries.
        let test = TestDatabase::new("retry-lineage-cycle");
        let [first, second, _] = retry_chain(&test, &[(0, 1), (1, 0)]);
        fail(&test, 5, first);
        fail(&test, 6, second);
        assert!(matches!(
            test.batch(9, |batch| persistence::retry_job(batch, retry(first, true))),
            Err(BatchFailure::NotCommitted(error)) if error.kind == StorageErrorKind::Corrupted
        ));
        assert!(
            test.batch(10, |batch| batch.retries(first).map(|retries| (
                retries,
                MarkerSubjects::addressed(RecordRef::Job(first))
            )))
            .expect("read the retries")
                == [second],
            "no retry was added"
        );

        // A retry of a Job that is gone.
        let test = TestDatabase::new("retry-lineage-missing");
        let [first, second, _] = retry_chain(&test, &[(1, 0)]);
        fail(&test, 5, second);
        remove_job_row(&test.database, first).expect("remove the original's row");
        assert!(matches!(
            test.batch(9, |batch| persistence::retry_job(batch, retry(second, true))),
            Err(BatchFailure::NotCommitted(error)) if error.kind == StorageErrorKind::Corrupted
        ));
    }
}
