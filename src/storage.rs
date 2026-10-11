//! The only module that touches redb.
//!
//! redb is the durable source of truth. Every record is stored under its
//! stable business ID with an explicit, versioned encoding; every relationship
//! has its own table, keyed so that its cardinality is structural, plus an
//! incoming index where a rule or query needs one. The database, its
//! transactions and its tables never leave this module: mutations go through
//! [`Batch`], whose typed operations are the only write path.
//!
//! A batch is one redb write transaction. Its first failure, a rule
//! violation as much as a storage error, poisons it: every later operation
//! returns that failure again and [`run_batch`] aborts instead of committing,
//! even when the operation's caller ignored the error. Records,
//! relationships, indexes, sequences and the operation marker therefore
//! commit or abort together.
//!
//! The waiting graph is never cached. [`Batch::check_waiting`] builds a
//! bounded petgraph view from records read inside the current write
//! transaction, adds the proposed edges and rejects any cycle.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use petgraph::graph::{DiGraph, NodeIndex};
use redb::{
    CommitError, Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};

use crate::persistence::{
    DependencyCondition, DependencyState, EffectKind, ExecutionMode, ExecutorId, ExecutorType,
    JobId, JobInputId, JobOutputId, JobOutputRecord, JobRecord, JobStepId, JobType,
    OperationId, OperationKind, OperationMarker, OutputKind, RecordRef, Timestamp, Violation,
};

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const FORMAT_NAME: &[u8] = b"ocg-foundation";
pub(crate) const FORMAT_VERSION: u32 = 1;
pub(crate) const MAX_WAITING_LAYERS: usize = 64;
pub(crate) const MAX_WAITING_JOBS: usize = 10_000;
pub(crate) const MAX_PAGE: usize = 1_000;

type Records = TableDefinition<'static, u64, &'static [u8]>;
type Link = TableDefinition<'static, u64, u64>;
type PairSet = TableDefinition<'static, (u64, u64), ()>;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("sequence");
const JOBS: Records = TableDefinition::new("job");
const JOB_INPUTS: Records = TableDefinition::new("job_input");
const JOB_OUTPUTS: Records = TableDefinition::new("job_output");
const JOB_STEPS: Records = TableDefinition::new("job_step");
const EXECUTORS: Records = TableDefinition::new("executor");
/// job -> input, and its incoming index input -> job.
const HAS_INPUT: Link = TableDefinition::new("has_input");
const INPUT_JOB: Link = TableDefinition::new("has_input_in");
/// (job, seq) -> output; ordered by seq within a Job.
const HAS_OUTPUT: TableDefinition<(u64, u64), u64> = TableDefinition::new("has_output");
const LATEST_OUTPUT: Link = TableDefinition::new("latest_output");
/// (job, seq) -> step; ordered by seq within a Job.
const HAS_STEP: TableDefinition<(u64, u64), u64> = TableDefinition::new("has_step");
/// child -> parent, and its incoming index (parent, child).
const CHILD_OF: Link = TableDefinition::new("child_of");
const CHILDREN: PairSet = TableDefinition::new("child_of_in");
/// (job, prerequisite) -> condition code.
const DEPENDS_ON: TableDefinition<(u64, u64), u8> = TableDefinition::new("depends_on");
const ASSIGNED_TO: Link = TableDefinition::new("assigned_to");
const EXECUTED_BY: Link = TableDefinition::new("executed_by");
const PREVIOUS: Link = TableDefinition::new("previous");
const REPLACES: Link = TableDefinition::new("replaces");
const RETRY_OF: Link = TableDefinition::new("retry_of");
/// (epoch, seq) -> operation marker.
const OPERATIONS: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("operation");

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
    fn corrupted(detail: impl Into<String>) -> Self {
        Self { kind: StorageErrorKind::Corrupted, os_error: None, detail: detail.into() }
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
        Self { kind, os_error, detail: error.to_string() }
    }
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(error: $t) -> Self { redb::Error::from(error).into() }
        }
    )*};
}
storage_from!(redb::StorageError, redb::TableError, redb::TransactionError, redb::DatabaseError, redb::SetDurabilityError, CommitError);

// ---- explicit versioned encoding ----------------------------------------------

const RECORD_VERSION: u8 = 1;
const TAG_JOB: u8 = 1;
const TAG_INPUT: u8 = 2;
const TAG_OUTPUT: u8 = 3;
const TAG_STEP: u8 = 4;
const TAG_EXECUTOR: u8 = 5;
const TAG_OPERATION: u8 = 6;

struct Enc(Vec<u8>);

impl Enc {
    fn new(tag: u8) -> Self {
        Enc(vec![tag, RECORD_VERSION])
    }
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn bool(&mut self, v: bool) {
        self.u8(u8::from(v));
    }
    fn ts(&mut self, v: Timestamp) {
        self.i64(v.as_unix_micros());
    }
    fn str(&mut self, v: &str) {
        self.u32(u32::try_from(v.len()).expect("strings above 4 GiB are refused by redb first"));
        self.0.extend_from_slice(v.as_bytes());
    }
    fn opt<T>(&mut self, v: &Option<T>, mut put: impl FnMut(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                put(self, v);
            }
        }
    }
    fn ots(&mut self, v: Option<Timestamp>) {
        self.opt(&v, |e, v| e.ts(*v));
    }
    fn ostr(&mut self, v: &Option<String>) {
        self.opt(v, |e, v| e.str(v));
    }
    fn oi64(&mut self, v: Option<i64>) {
        self.opt(&v, |e, v| e.i64(*v));
    }
    fn ojson(&mut self, v: &Option<serde_json::Map<String, serde_json::Value>>) {
        self.opt(v, |e, v| e.str(&serde_json::Value::Object(v.clone()).to_string()));
    }
    fn record_ref(&mut self, r: RecordRef) {
        let (tag, id) = match r {
            RecordRef::Job(id) => (1, id.get()),
            RecordRef::JobInput(id) => (2, id.get()),
            RecordRef::JobOutput(id) => (3, id.get()),
            RecordRef::JobStep(id) => (4, id.get()),
            RecordRef::Executor(id) => (5, id.get()),
        };
        self.u8(tag);
        self.u64(id);
    }
}

struct Dec<'a> {
    bytes: &'a [u8],
    at: usize,
}

type DecodeResult<T> = Result<T, StorageError>;

impl<'a> Dec<'a> {
    /// Refuses a record of another type or of an unsupported encoding version.
    fn new(bytes: &'a [u8], tag: u8) -> DecodeResult<Self> {
        match bytes {
            [found, RECORD_VERSION, ..] if *found == tag => Ok(Dec { bytes, at: 2 }),
            [found, version, ..] if *found == tag => Err(StorageError::corrupted(format!(
                "record type {tag} uses unsupported encoding version {version}"
            ))),
            _ => Err(StorageError::corrupted(format!("expected a record of type {tag}"))),
        }
    }
    fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        let slice = self
            .bytes
            .get(self.at..self.at + n)
            .ok_or_else(|| StorageError::corrupted("truncated record"))?;
        self.at += n;
        Ok(slice)
    }
    fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }
    fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }
    fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }
    fn bool(&mut self) -> DecodeResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageError::corrupted("invalid boolean")),
        }
    }
    fn ts(&mut self) -> DecodeResult<Timestamp> {
        Ok(Timestamp::from_unix_micros(self.i64()?))
    }
    fn str(&mut self) -> DecodeResult<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| StorageError::corrupted("invalid UTF-8"))
    }
    fn opt<T>(&mut self, get: impl FnOnce(&mut Self) -> DecodeResult<T>) -> DecodeResult<Option<T>> {
        match self.u8()? {
            0 => Ok(None),
            1 => get(self).map(Some),
            _ => Err(StorageError::corrupted("invalid option flag")),
        }
    }
    fn ots(&mut self) -> DecodeResult<Option<Timestamp>> {
        self.opt(Self::ts)
    }
    fn ostr(&mut self) -> DecodeResult<Option<String>> {
        self.opt(Self::str)
    }
    fn oi64(&mut self) -> DecodeResult<Option<i64>> {
        self.opt(Self::i64)
    }
    fn ojson(&mut self) -> DecodeResult<Option<serde_json::Map<String, serde_json::Value>>> {
        self.opt(|d| match serde_json::from_str(&d.str()?) {
            Ok(serde_json::Value::Object(map)) => Ok(map),
            _ => Err(StorageError::corrupted("invalid JSON object")),
        })
    }
    fn code<T>(&mut self, decode: impl FnOnce(u8) -> Option<T>) -> DecodeResult<T> {
        let code = self.u8()?;
        decode(code).ok_or_else(|| StorageError::corrupted(format!("invalid enum code {code}")))
    }
    fn record_ref(&mut self) -> DecodeResult<RecordRef> {
        let tag = self.u8()?;
        let id = self.u64()?;
        Ok(match tag {
            1 => RecordRef::Job(JobId::new(id)),
            2 => RecordRef::JobInput(JobInputId::new(id)),
            3 => RecordRef::JobOutput(JobOutputId::new(id)),
            4 => RecordRef::JobStep(JobStepId::new(id)),
            5 => RecordRef::Executor(ExecutorId::new(id)),
            _ => return Err(StorageError::corrupted("invalid record reference")),
        })
    }
    fn end(self) -> DecodeResult<()> {
        if self.at == self.bytes.len() { Ok(()) } else { Err(StorageError::corrupted("trailing record bytes")) }
    }
}

macro_rules! codes {
    ($t:ty { $($variant:path = $code:literal),* $(,)? }) => {
        impl Code for $t {
            fn code(self) -> u8 { match self { $($variant => $code),* } }
            fn from_code(code: u8) -> Option<Self> { match code { $($code => Some($variant),)* _ => None } }
        }
    };
}

trait Code: Sized + Copy {
    fn code(self) -> u8;
    fn from_code(code: u8) -> Option<Self>;
}

codes!(JobType { JobType::Task = 1, JobType::System = 2 });
codes!(ExecutionMode { ExecutionMode::Once = 1, ExecutionMode::Continuous = 2, ExecutionMode::Periodic = 3, ExecutionMode::Container = 4 });
codes!(OutputKind { OutputKind::Progress = 1, OutputKind::Evaluation = 2, OutputKind::Result = 3 });
codes!(EffectKind { EffectKind::Idempotent = 1, EffectKind::StrictFenced = 2, EffectKind::Reconcilable = 3, EffectKind::NonRetryable = 4 });
codes!(ExecutorType { ExecutorType::System = 1, ExecutorType::Llm = 2, ExecutorType::Agent = 3, ExecutorType::Tool = 4, ExecutorType::Database = 5, ExecutorType::Frontend = 6 });
codes!(DependencyCondition { DependencyCondition::Success = 1, DependencyCondition::Settled = 2 });
codes!(OperationKind {
    OperationKind::SaveJobInput = 1, OperationKind::CreateJobWithInput = 2, OperationKind::AssignJobExecutor = 3,
    OperationKind::AppendJobStep = 4, OperationKind::StartJobStep = 5, OperationKind::SettleJobStep = 6,
    OperationKind::AppendJobOutput = 7, OperationKind::FinalizeJob = 8, OperationKind::CreateExecutor = 9,
    OperationKind::BeginCoreLogCleanup = 10,
});

// ---- rows ------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JobRow {
    pub(crate) id: JobId,
    pub(crate) name: Option<String>,
    pub(crate) job_type: JobType,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) eligible_at: Option<Timestamp>,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) paused_at: Option<Timestamp>,
    pub(crate) sterilized_at: Option<Timestamp>,
    pub(crate) completed_at: Option<Timestamp>,
    pub(crate) failed_at: Option<Timestamp>,
    pub(crate) cancelled_at: Option<Timestamp>,
    pub(crate) unknown_at: Option<Timestamp>,
    pub(crate) next_due_at: Option<Timestamp>,
    pub(crate) next_iteration_no: Option<i64>,
    pub(crate) iteration_no: Option<i64>,
    pub(crate) scheduled_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl JobRow {
    pub(crate) fn confirmed_terminal(&self) -> bool {
        self.completed_at.is_some() || self.failed_at.is_some() || self.cancelled_at.is_some()
    }

    pub(crate) fn any_terminal(&self) -> bool {
        self.confirmed_terminal() || self.unknown_at.is_some()
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB);
        e.u64(self.id.get());
        e.ostr(&self.name);
        e.u8(self.job_type.code());
        e.u8(self.execution_mode.code());
        for at in [
            self.eligible_at,
            self.started_at,
            self.paused_at,
            self.sterilized_at,
            self.completed_at,
            self.failed_at,
            self.cancelled_at,
            self.unknown_at,
            self.next_due_at,
        ] {
            e.ots(at);
        }
        e.oi64(self.next_iteration_no);
        e.oi64(self.iteration_no);
        e.ots(self.scheduled_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB)?;
        let row = JobRow {
            id: JobId::new(d.u64()?),
            name: d.ostr()?,
            job_type: d.code(JobType::from_code)?,
            execution_mode: d.code(ExecutionMode::from_code)?,
            eligible_at: d.ots()?,
            started_at: d.ots()?,
            paused_at: d.ots()?,
            sterilized_at: d.ots()?,
            completed_at: d.ots()?,
            failed_at: d.ots()?,
            cancelled_at: d.ots()?,
            unknown_at: d.ots()?,
            next_due_at: d.ots()?,
            next_iteration_no: d.oi64()?,
            iteration_no: d.oi64()?,
            scheduled_at: d.ots()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct JobInputRow {
    pub(crate) id: JobInputId,
    pub(crate) submission_id: Option<String>,
    pub(crate) objective: Option<String>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) max_children_per_job: Option<u32>,
    pub(crate) max_total_descendants: Option<u32>,
    pub(crate) submitted_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl JobInputRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_INPUT);
        e.u64(self.id.get());
        e.ostr(&self.submission_id);
        e.ostr(&self.objective);
        for limit in [self.max_depth, self.max_children_per_job, self.max_total_descendants] {
            e.opt(&limit, |e, v| e.u32(*v));
        }
        e.ots(self.submitted_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_INPUT)?;
        let row = JobInputRow {
            id: JobInputId::new(d.u64()?),
            submission_id: d.ostr()?,
            objective: d.ostr()?,
            max_depth: d.opt(Dec::u32)?,
            max_children_per_job: d.opt(Dec::u32)?,
            max_total_descendants: d.opt(Dec::u32)?,
            submitted_at: d.ots()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

fn encode_output(o: &JobOutputRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_OUTPUT);
    e.u64(o.id.get());
    e.u64(o.job.get());
    e.i64(o.seq);
    e.u8(o.kind.code());
    e.ostr(&o.summary);
    e.ostr(&o.code);
    e.opt(&o.goal_reached, |e, v| e.bool(*v));
    e.ts(o.created_at);
    e.0
}

fn decode_output(bytes: &[u8]) -> DecodeResult<JobOutputRecord> {
    let mut d = Dec::new(bytes, TAG_OUTPUT)?;
    let o = JobOutputRecord {
        id: JobOutputId::new(d.u64()?),
        job: JobId::new(d.u64()?),
        seq: d.i64()?,
        kind: d.code(OutputKind::from_code)?,
        summary: d.ostr()?,
        code: d.ostr()?,
        goal_reached: d.opt(Dec::bool)?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(o)
}

#[derive(Clone, Debug)]
pub(crate) struct JobStepRow {
    pub(crate) id: JobStepId,
    pub(crate) job: JobId,
    pub(crate) seq: u64,
    pub(crate) operation: String,
    pub(crate) protocol_call_id: Option<String>,
    pub(crate) effect_kind: EffectKind,
    pub(crate) confirmation_requested_at: Option<Timestamp>,
    pub(crate) approved_at: Option<Timestamp>,
    pub(crate) rejected_at: Option<Timestamp>,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) completed_at: Option<Timestamp>,
    pub(crate) failed_at: Option<Timestamp>,
    pub(crate) cancelled_at: Option<Timestamp>,
    pub(crate) unknown_at: Option<Timestamp>,
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) http_status: Option<i64>,
    pub(crate) exit_code: Option<i64>,
    pub(crate) stdout: Option<String>,
    pub(crate) stderr: Option<String>,
    pub(crate) truncated: bool,
    pub(crate) structured_result: Option<serde_json::Map<String, serde_json::Value>>,
    pub(crate) diagnostic_details: Option<serde_json::Map<String, serde_json::Value>>,
    pub(crate) external_task_id: Option<String>,
    pub(crate) external_session_id: Option<String>,
    pub(crate) ttfb_ms: Option<i64>,
    pub(crate) reasoning_ttft_ms: Option<i64>,
    pub(crate) output_ttft_ms: Option<i64>,
    pub(crate) request_finished_in_ms: Option<i64>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl JobStepRow {
    pub(crate) fn confirmed_terminal(&self) -> bool {
        self.completed_at.is_some() || self.failed_at.is_some() || self.cancelled_at.is_some()
    }

    pub(crate) fn any_terminal(&self) -> bool {
        self.confirmed_terminal() || self.unknown_at.is_some()
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_STEP);
        e.u64(self.id.get());
        e.u64(self.job.get());
        e.u64(self.seq);
        e.str(&self.operation);
        e.ostr(&self.protocol_call_id);
        e.u8(self.effect_kind.code());
        for at in [
            self.confirmation_requested_at,
            self.approved_at,
            self.rejected_at,
            self.started_at,
            self.completed_at,
            self.failed_at,
            self.cancelled_at,
            self.unknown_at,
        ] {
            e.ots(at);
        }
        e.ostr(&self.code);
        e.ostr(&self.message);
        e.oi64(self.http_status);
        e.oi64(self.exit_code);
        e.ostr(&self.stdout);
        e.ostr(&self.stderr);
        e.bool(self.truncated);
        e.ojson(&self.structured_result);
        e.ojson(&self.diagnostic_details);
        e.ostr(&self.external_task_id);
        e.ostr(&self.external_session_id);
        for ms in [self.ttfb_ms, self.reasoning_ttft_ms, self.output_ttft_ms, self.request_finished_in_ms] {
            e.oi64(ms);
        }
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_STEP)?;
        let row = JobStepRow {
            id: JobStepId::new(d.u64()?),
            job: JobId::new(d.u64()?),
            seq: d.u64()?,
            operation: d.str()?,
            protocol_call_id: d.ostr()?,
            effect_kind: d.code(EffectKind::from_code)?,
            confirmation_requested_at: d.ots()?,
            approved_at: d.ots()?,
            rejected_at: d.ots()?,
            started_at: d.ots()?,
            completed_at: d.ots()?,
            failed_at: d.ots()?,
            cancelled_at: d.ots()?,
            unknown_at: d.ots()?,
            code: d.ostr()?,
            message: d.ostr()?,
            http_status: d.oi64()?,
            exit_code: d.oi64()?,
            stdout: d.ostr()?,
            stderr: d.ostr()?,
            truncated: d.bool()?,
            structured_result: d.ojson()?,
            diagnostic_details: d.ojson()?,
            external_task_id: d.ostr()?,
            external_session_id: d.ostr()?,
            ttfb_ms: d.oi64()?,
            reasoning_ttft_ms: d.oi64()?,
            output_ttft_ms: d.oi64()?,
            request_finished_in_ms: d.oi64()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutorRow {
    pub(crate) id: ExecutorId,
    pub(crate) name: String,
    pub(crate) executor_type: ExecutorType,
    pub(crate) concurrency_limit: u32,
    pub(crate) latest_enabled_at: Option<Timestamp>,
    pub(crate) latest_disabled_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl ExecutorRow {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut decoder = Dec::new(bytes, TAG_EXECUTOR)?;
        let row = Self {
            id: ExecutorId::new(decoder.u64()?),
            name: decoder.str()?,
            executor_type: decoder.code(ExecutorType::from_code)?,
            concurrency_limit: decoder.u32()?,
            latest_enabled_at: decoder.ots()?,
            latest_disabled_at: decoder.ots()?,
            created_at: decoder.ts()?,
            updated_at: decoder.ts()?,
        };
        decoder.end()?;
        Ok(row)
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_EXECUTOR);
        e.u64(self.id.get());
        e.str(&self.name);
        e.u8(self.executor_type.code());
        e.u32(self.concurrency_limit);
        e.ots(self.latest_enabled_at);
        e.ots(self.latest_disabled_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }
}

fn encode_marker(m: &OperationMarker) -> Vec<u8> {
    let mut e = Enc::new(TAG_OPERATION);
    e.u8(m.kind.code());
    e.record_ref(m.subject);
    e.opt(&m.created, |e, r| e.record_ref(*r));
    e.ts(m.committed_at);
    e.0
}

fn decode_marker(operation: OperationId, bytes: &[u8]) -> DecodeResult<OperationMarker> {
    let mut d = Dec::new(bytes, TAG_OPERATION)?;
    let m = OperationMarker {
        operation,
        kind: d.code(OperationKind::from_code)?,
        subject: d.record_ref()?,
        created: d.opt(Dec::record_ref)?,
        committed_at: d.ts()?,
    };
    d.end()?;
    Ok(m)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Sequence {
    Job,
    JobInput,
    JobOutput,
    JobStep,
    Executor,
}

impl Sequence {
    fn key(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::JobInput => "job_input",
            Self::JobOutput => "job_output",
            Self::JobStep => "job_step",
            Self::Executor => "executor",
        }
    }
}

const OPEN_EPOCH: &str = "open_epoch";

// ---- open ----------------------------------------------------------------------

pub(crate) enum OpenError {
    Storage(StorageError),
    /// The file is a redb database that this application did not create.
    ForeignFile,
    /// The file uses a storage format this build does not support.
    UnsupportedFormat { found: u32 },
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub(crate) fn database_file(directory: &Path) -> PathBuf {
    directory.join(DATABASE_FILE_NAME)
}

/// Opens or creates the database file, checks its storage format and
/// durably allocates this opening's operation epoch.
pub(crate) fn open(directory: &Path) -> Result<(Database, u64), OpenError> {
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    let database = builder.create(database_file(directory)).map_err(StorageError::from)?;
    let epoch = initialize(&database)?;
    Ok((database, epoch))
}

fn initialize(database: &Database) -> Result<u64, OpenError> {
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate).map_err(StorageError::from)?;
    let existing_tables = tx.list_tables().map_err(StorageError::from)?.count();
    let epoch = {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        let format = meta.get("format").map_err(StorageError::from)?.map(|v| v.value().to_vec());
        let version = meta.get("format_version").map_err(StorageError::from)?.map(|v| v.value().to_vec());
        match (format, version) {
            (None, None) if existing_tables == 0 => {
                meta.insert("format", FORMAT_NAME).map_err(StorageError::from)?;
                meta.insert("format_version", FORMAT_VERSION.to_le_bytes().as_slice()).map_err(StorageError::from)?;
            }
            (Some(format), Some(version)) if format == FORMAT_NAME => {
                let found = <[u8; 4]>::try_from(version.as_slice()).map(u32::from_le_bytes).unwrap_or(u32::MAX);
                if found != FORMAT_VERSION {
                    drop(meta);
                    tx.abort().map_err(StorageError::from)?;
                    return Err(OpenError::UnsupportedFormat { found });
                }
            }
            _ => {
                drop(meta);
                tx.abort().map_err(StorageError::from)?;
                return Err(OpenError::ForeignFile);
            }
        }
        drop(meta);
        create_tables(&tx)?;
        let mut sequences = tx.open_table(SEQUENCES).map_err(StorageError::from)?;
        let epoch = sequences.get(OPEN_EPOCH).map_err(StorageError::from)?.map_or(0, |v| v.value()) + 1;
        sequences.insert(OPEN_EPOCH, epoch).map_err(StorageError::from)?;
        epoch
    };
    tx.commit().map_err(StorageError::from)?;
    Ok(epoch)
}

fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
    for table in [JOBS, JOB_INPUTS, JOB_OUTPUTS, JOB_STEPS, EXECUTORS] {
        tx.open_table(table)?;
    }
    for table in [HAS_INPUT, INPUT_JOB, LATEST_OUTPUT, CHILD_OF, ASSIGNED_TO, EXECUTED_BY, PREVIOUS, REPLACES, RETRY_OF] {
        tx.open_table(table)?;
    }
    tx.open_table(HAS_OUTPUT)?;
    tx.open_table(HAS_STEP)?;
    tx.open_table(CHILDREN)?;
    tx.open_table(DEPENDS_ON)?;
    tx.open_table(OPERATIONS)?;
    tx.open_table(SEQUENCES)?;
    Ok(())
}

// ---- batches -------------------------------------------------------------------

/// A failure inside a batch. Only [`Batch`] creates one, and creating it
/// poisons that batch.
#[derive(Clone, Debug)]
pub(crate) struct BatchError(Failure);

#[derive(Clone, Debug)]
enum Failure {
    Rejected(Violation),
    Storage(StorageError),
}

/// Who the operation marker names.
pub(crate) struct MarkerSubjects {
    subject: RecordRef,
    created: Option<RecordRef>,
}

impl MarkerSubjects {
    pub(crate) fn addressed(subject: RecordRef) -> Self {
        Self { subject, created: None }
    }

    pub(crate) fn created(subject: RecordRef, created: RecordRef) -> Self {
        Self { subject, created: Some(created) }
    }
}

/// How a batch ended when it did not commit.
#[derive(Debug)]
pub(crate) enum BatchFailure {
    /// A rule refused the batch; nothing was committed.
    Rejected(Violation),
    /// Storage failed before the commit point; nothing was committed.
    NotCommitted(StorageError),
    /// The commit itself failed; its changes may or may not be durable.
    OutcomeUnknown(StorageError),
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    /// Set when the storage engine failed, so the open database must stop
    /// admitting work and be released before it can be reopened.
    pub(crate) storage_failure: Option<StorageError>,
}

pub(crate) struct Batch {
    tx: WriteTransaction,
    failed: Option<Failure>,
    now: Timestamp,
}

/// Runs one command as one batch and commits it with its operation marker
/// only if no operation in it failed.
pub(crate) fn run_batch<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
) -> BatchOutcome<T> {
    let not_committed = |error: StorageError| BatchOutcome {
        result: Err(BatchFailure::NotCommitted(error.clone())),
        storage_failure: Some(error),
    };
    let mut tx = match database.begin_write() {
        Ok(tx) => tx,
        Err(error) => return not_committed(error.into()),
    };
    if let Err(error) = tx.set_durability(Durability::Immediate) {
        return finish_abort(tx, Failure::Storage(error.into()));
    }
    let mut batch = Batch { tx, failed: None, now: Timestamp::now() };
    let body_result = body(&mut batch);
    // A failure the body swallowed still prevents the commit.
    let value = match (body_result, batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => return finish_abort(batch.tx, failure),
        (Ok(value), None) => value,
    };
    let (value, subjects) = value;
    let marker = OperationMarker {
        operation,
        kind,
        subject: subjects.subject,
        created: subjects.created,
        committed_at: batch.now,
    };
    if let Err(BatchError(failure)) = batch.write_marker(&marker) {
        return finish_abort(batch.tx, failure);
    }
    match batch.tx.commit() {
        Ok(()) => BatchOutcome { result: Ok(value), storage_failure: None },
        // redb documents a poisoned commit as rolled back.
        Err(CommitError::TransactionPoisoned) => {
            let error = StorageError::from(CommitError::TransactionPoisoned);
            not_committed(error)
        }
        Err(error) => {
            let error = StorageError::from(error);
            BatchOutcome {
                result: Err(BatchFailure::OutcomeUnknown(error.clone())),
                storage_failure: Some(error),
            }
        }
    }
}

fn finish_abort<T>(tx: WriteTransaction, failure: Failure) -> BatchOutcome<T> {
    let abort_failure = tx.abort().err().map(StorageError::from);
    match failure {
        Failure::Rejected(violation) => BatchOutcome {
            result: Err(BatchFailure::Rejected(violation)),
            storage_failure: abort_failure,
        },
        Failure::Storage(error) => BatchOutcome {
            result: Err(BatchFailure::NotCommitted(error.clone())),
            storage_failure: Some(error),
        },
    }
}

impl Batch {
    pub(crate) fn now(&self) -> Timestamp {
        self.now
    }

    /// Refuses the batch for a business rule. The batch stays poisoned even
    /// if the returned error is ignored.
    pub(crate) fn reject<T>(&mut self, violation: Violation) -> Result<T, BatchError> {
        self.fail(Failure::Rejected(violation))
    }

    fn fail<T>(&mut self, failure: Failure) -> Result<T, BatchError> {
        let first = self.failed.get_or_insert(failure).clone();
        Err(BatchError(first))
    }

    /// Runs one storage step; a failed batch refuses further steps.
    fn step<T>(&mut self, f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>) -> Result<T, BatchError> {
        if let Some(failure) = &self.failed {
            return Err(BatchError(failure.clone()));
        }
        match f(&self.tx) {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
    }

    fn get_bytes(&mut self, table: Records, id: u64) -> Result<Option<Vec<u8>>, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get(id)?.map(|v| v.value().to_vec())))
    }

    fn insert_new(&mut self, table: Records, id: u64, bytes: Vec<u8>, record: RecordRef) -> Result<(), BatchError> {
        let existed = self.step(|tx| {
            let mut t = tx.open_table(table)?;
            if t.get(id)?.is_some() {
                return Ok(true);
            }
            t.insert(id, bytes.as_slice())?;
            Ok(false)
        })?;
        if existed { self.reject(Violation::DuplicateId(record)) } else { Ok(()) }
    }

    fn overwrite(&mut self, table: Records, id: u64, bytes: Vec<u8>) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(table)?.insert(id, bytes.as_slice())?;
            Ok(())
        })
    }

    fn decoded<T>(&mut self, decoded: DecodeResult<T>) -> Result<T, BatchError> {
        match decoded {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
    }

    pub(crate) fn next_id(&mut self, sequence: Sequence) -> Result<u64, BatchError> {
        self.step(|tx| {
            let mut t = tx.open_table(SEQUENCES)?;
            let next = t.get(sequence.key())?.map_or(0, |v| v.value()) + 1;
            t.insert(sequence.key(), next)?;
            Ok(next)
        })
    }

    // -- records --

    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobRow>, BatchError> {
        let bytes = self.get_bytes(JOBS, id.get())?;
        let row = bytes.map(|b| JobRow::decode(&b)).transpose();
        self.decoded(row)
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobRow, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn insert_job(&mut self, row: &JobRow) -> Result<(), BatchError> {
        self.insert_new(JOBS, row.id.get(), row.encode(), RecordRef::Job(row.id))
    }

    pub(crate) fn put_job(&mut self, row: &JobRow) -> Result<(), BatchError> {
        self.overwrite(JOBS, row.id.get(), row.encode())
    }

    pub(crate) fn require_input(&mut self, id: JobInputId) -> Result<JobInputRow, BatchError> {
        match self.get_bytes(JOB_INPUTS, id.get())? {
            Some(bytes) => {
                let row = JobInputRow::decode(&bytes);
                self.decoded(row)
            }
            None => self.reject(Violation::NotFound(RecordRef::JobInput(id))),
        }
    }

    pub(crate) fn insert_input(&mut self, row: &JobInputRow) -> Result<(), BatchError> {
        self.insert_new(JOB_INPUTS, row.id.get(), row.encode(), RecordRef::JobInput(row.id))
    }

    pub(crate) fn put_input(&mut self, row: &JobInputRow) -> Result<(), BatchError> {
        self.overwrite(JOB_INPUTS, row.id.get(), row.encode())
    }

    pub(crate) fn require_step(&mut self, id: JobStepId) -> Result<JobStepRow, BatchError> {
        match self.get_bytes(JOB_STEPS, id.get())? {
            Some(bytes) => {
                let row = JobStepRow::decode(&bytes);
                self.decoded(row)
            }
            None => self.reject(Violation::NotFound(RecordRef::JobStep(id))),
        }
    }

    /// Inserts the step and its HAS_STEP entry at the step's seq.
    pub(crate) fn insert_step(&mut self, row: &JobStepRow) -> Result<(), BatchError> {
        self.insert_new(JOB_STEPS, row.id.get(), row.encode(), RecordRef::JobStep(row.id))?;
        let (job, seq, step) = (row.job.get(), row.seq, row.id.get());
        let taken = self.step(|tx| {
            let mut t = tx.open_table(HAS_STEP)?;
            if t.get((job, seq))?.is_some() {
                return Ok(true);
            }
            t.insert((job, seq), step)?;
            Ok(false)
        })?;
        if taken { self.reject(Violation::DuplicateId(RecordRef::JobStep(row.id))) } else { Ok(()) }
    }

    pub(crate) fn put_step(&mut self, row: &JobStepRow) -> Result<(), BatchError> {
        self.overwrite(JOB_STEPS, row.id.get(), row.encode())
    }

    pub(crate) fn last_step_seq(&mut self, job: JobId) -> Result<u64, BatchError> {
        self.step(|tx| last_seq(&tx.open_table(HAS_STEP)?, job.get()))
    }

    pub(crate) fn job_steps(&mut self, job: JobId) -> Result<Vec<JobStepRow>, BatchError> {
        let rows = self.step(|tx| {
            let index = tx.open_table(HAS_STEP)?;
            let steps = tx.open_table(JOB_STEPS)?;
            let mut rows = Vec::new();
            for entry in index.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                let step = entry?.1.value();
                let bytes = steps.get(step)?.ok_or_else(|| StorageError::corrupted(format!("HAS_STEP names missing step {step}")))?;
                rows.push(JobStepRow::decode(bytes.value())?);
            }
            Ok(rows)
        })?;
        Ok(rows)
    }

    /// Inserts the output, its HAS_OUTPUT entry and moves LATEST_OUTPUT to it.
    pub(crate) fn insert_output(&mut self, record: &JobOutputRecord) -> Result<(), BatchError> {
        self.insert_new(JOB_OUTPUTS, record.id.get(), encode_output(record), RecordRef::JobOutput(record.id))?;
        let (job, output) = (record.job.get(), record.id.get());
        let Ok(seq) = u64::try_from(record.seq) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let taken = self.step(|tx| {
            let mut t = tx.open_table(HAS_OUTPUT)?;
            if t.get((job, seq))?.is_some() {
                return Ok(true);
            }
            t.insert((job, seq), output)?;
            tx.open_table(LATEST_OUTPUT)?.insert(job, output)?;
            Ok(false)
        })?;
        if taken { self.reject(Violation::DuplicateId(RecordRef::JobOutput(record.id))) } else { Ok(()) }
    }

    pub(crate) fn last_output_seq(&mut self, job: JobId) -> Result<u64, BatchError> {
        self.step(|tx| last_seq(&tx.open_table(HAS_OUTPUT)?, job.get()))
    }

    pub(crate) fn require_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        match self.get_bytes(EXECUTORS, id.get())? {
            Some(_) => Ok(()),
            None => self.reject(Violation::NotFound(RecordRef::Executor(id))),
        }
    }

    pub(crate) fn require_core_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        let Some(bytes) = self.get_bytes(EXECUTORS, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::Executor(id)));
        };
        let decoded = ExecutorRow::decode(&bytes);
        let row = self.decoded(decoded)?;
        if row.name != "__CORE__" || row.executor_type != ExecutorType::System || row.concurrency_limit == 0 {
            return self.reject(Violation::NotCoreExecutor(id));
        }
        Ok(())
    }

    pub(crate) fn insert_executor(&mut self, row: &ExecutorRow) -> Result<(), BatchError> {
        self.insert_new(EXECUTORS, row.id.get(), row.encode(), RecordRef::Executor(row.id))
    }

    // -- relationships --

    pub(crate) fn input_job(&mut self, input: JobInputId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| Ok(tx.open_table(INPUT_JOB)?.get(input.get())?.map(|v| JobId::new(v.value()))))
    }

    /// HAS_INPUT: one input per Job and one Job per input.
    pub(crate) fn link_input(&mut self, job: JobId, input: JobInputId) -> Result<(), BatchError> {
        let taken = self.step(|tx| {
            let mut out = tx.open_table(HAS_INPUT)?;
            let mut inn = tx.open_table(INPUT_JOB)?;
            if out.get(job.get())?.is_some() || inn.get(input.get())?.is_some() {
                return Ok(true);
            }
            out.insert(job.get(), input.get())?;
            inn.insert(input.get(), job.get())?;
            Ok(false)
        })?;
        if taken { self.reject(Violation::InputAlreadySubmitted(input)) } else { Ok(()) }
    }

    /// CHILD_OF: one parent per child, with the parent's incoming index.
    pub(crate) fn link_child(&mut self, child: JobId, parent: JobId) -> Result<(), BatchError> {
        let taken = self.step(|tx| {
            let mut out = tx.open_table(CHILD_OF)?;
            if out.get(child.get())?.is_some() {
                return Ok(true);
            }
            out.insert(child.get(), parent.get())?;
            tx.open_table(CHILDREN)?.insert((parent.get(), child.get()), ())?;
            Ok(false)
        })?;
        if taken { self.reject(Violation::DuplicateId(RecordRef::Job(child))) } else { Ok(()) }
    }

    pub(crate) fn children(&mut self, parent: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| children_of(&tx.open_table(CHILDREN)?, parent.get()))
    }

    pub(crate) fn link_dependency(&mut self, job: JobId, prerequisite: JobId, condition: DependencyCondition) -> Result<(), BatchError> {
        let taken = self.step(|tx| {
            let mut t = tx.open_table(DEPENDS_ON)?;
            if t.get((job.get(), prerequisite.get()))?.is_some() {
                return Ok(true);
            }
            t.insert((job.get(), prerequisite.get()), condition.code())?;
            Ok(false)
        })?;
        if taken { self.reject(Violation::DuplicateDependency(prerequisite)) } else { Ok(()) }
    }

    /// ASSIGNED_TO: one per Job; replacing it is the caller's decision.
    pub(crate) fn set_assigned_executor(&mut self, job: JobId, executor: ExecutorId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(ASSIGNED_TO)?.insert(job.get(), executor.get())?;
            Ok(())
        })
    }

    /// EXECUTED_BY: one per step; replacing it is the caller's decision.
    pub(crate) fn set_executed_by(&mut self, step: JobStepId, executor: ExecutorId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(EXECUTED_BY)?.insert(step.get(), executor.get())?;
            Ok(())
        })
    }

    /// PREVIOUS, REPLACES and RETRY_OF are fixed at creation and are not
    /// waiting edges.
    pub(crate) fn link_lineage(&mut self, job: JobId, previous: Option<JobId>, replaces: Option<JobId>, retry_of: Option<JobId>) -> Result<(), BatchError> {
        self.step(|tx| {
            for (table, target) in [(PREVIOUS, previous), (REPLACES, replaces), (RETRY_OF, retry_of)] {
                if let Some(target) = target {
                    tx.open_table(table)?.insert(job.get(), target.get())?;
                }
            }
            Ok(())
        })
    }

    fn write_marker(&mut self, marker: &OperationMarker) -> Result<(), BatchError> {
        let key = (marker.operation.epoch, marker.operation.seq);
        let bytes = encode_marker(marker);
        let existed = self.step(|tx| {
            let mut t = tx.open_table(OPERATIONS)?;
            if t.get(key)?.is_some() {
                return Ok(true);
            }
            t.insert(key, bytes.as_slice())?;
            Ok(false)
        })?;
        if existed {
            return self.fail(Failure::Storage(StorageError::corrupted(format!("{} already has a marker", marker.operation))));
        }
        Ok(())
    }

    /// Materializes the waiting graph reachable from the proposed edges'
    /// targets inside this transaction, adds the proposed edges and rejects
    /// any cycle. Every new cycle passes through a proposed edge and therefore
    /// through its target. Reaching a bound rejects instead of skipping the check.
    pub(crate) fn check_waiting(&mut self, proposed: &[(JobId, JobId)]) -> Result<(), BatchError> {
        if proposed.is_empty() {
            return Ok(());
        }
        let verdict = self.step(|tx| waiting_verdict(tx, proposed))?;
        match verdict {
            WaitingVerdict::Acyclic => Ok(()),
            WaitingVerdict::Cycle(jobs) => self.reject(Violation::WaitingCycle(jobs)),
            WaitingVerdict::Limit => self.reject(Violation::WaitingGraphLimit {
                max_layers: MAX_WAITING_LAYERS,
                max_jobs: MAX_WAITING_JOBS,
            }),
        }
    }
}

fn last_seq(table: &impl ReadableTable<(u64, u64), u64>, job: u64) -> Result<u64, StorageError> {
    Ok(match table.range((job, 0)..=(job, u64::MAX))?.next_back() {
        Some(entry) => entry?.0.value().1,
        None => 0,
    })
}

fn children_of(table: &impl ReadableTable<(u64, u64), ()>, parent: u64) -> Result<Vec<JobId>, StorageError> {
    let mut children = Vec::new();
    for entry in table.range((parent, 0)..=(parent, u64::MAX))? {
        children.push(JobId::new(entry?.0.value().1));
    }
    Ok(children)
}

// ---- waiting graph -------------------------------------------------------------

pub(crate) fn dependency_state(condition: DependencyCondition, prerequisite: &JobRow) -> DependencyState {
    if prerequisite.completed_at.is_some() {
        DependencyState::Satisfied
    } else if prerequisite.failed_at.is_some() || prerequisite.cancelled_at.is_some() {
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
        self.jobs.get(id)?.map(|v| JobRow::decode(v.value())).transpose()
    }

    /// Active waiting edges out of `id`: dependencies still waiting on an
    /// unsettled prerequisite and children without a confirmed outcome. A Job
    /// with a confirmed outcome waits for nothing.
    fn waits_for(&self, id: u64) -> Result<Vec<u64>, StorageError> {
        let Some(job) = self.job(id)? else { return Ok(Vec::new()) };
        if job.confirmed_terminal() {
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
                && !row.confirmed_terminal()
            {
                targets.push(child);
            }
        }
        Ok(targets)
    }
}

fn waiting_verdict(tx: &WriteTransaction, proposed: &[(JobId, JobId)]) -> Result<WaitingVerdict, StorageError> {
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
        .map(|component| component.into_iter().map(|index| JobId::new(graph[index])).collect())
        .unwrap_or_default();
    Ok(WaitingVerdict::Cycle(cycle))
}

// ---- typed reads ---------------------------------------------------------------

fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

fn job_record(rx: &ReadTransaction, row: JobRow) -> Result<JobRecord, StorageError> {
    let id = row.id.get();
    let input = rx
        .open_table(HAS_INPUT)?
        .get(id)?
        .map(|v| JobInputId::new(v.value()))
        .ok_or_else(|| StorageError::corrupted(format!("{} has no input", row.id)))?;
    let parent = rx.open_table(CHILD_OF)?.get(id)?.map(|v| JobId::new(v.value()));
    let assigned_executor = rx.open_table(ASSIGNED_TO)?.get(id)?.map(|v| ExecutorId::new(v.value()));
    Ok(JobRecord {
        id: row.id,
        name: row.name,
        job_type: row.job_type,
        execution_mode: row.execution_mode,
        eligible_at: row.eligible_at,
        started_at: row.started_at,
        paused_at: row.paused_at,
        sterilized_at: row.sterilized_at,
        completed_at: row.completed_at,
        failed_at: row.failed_at,
        cancelled_at: row.cancelled_at,
        unknown_at: row.unknown_at,
        next_due_at: row.next_due_at,
        next_iteration_no: row.next_iteration_no,
        iteration_no: row.iteration_no,
        scheduled_at: row.scheduled_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        input,
        parent,
        assigned_executor,
    })
}

pub(crate) fn read_job(database: &Database, id: JobId) -> Result<Option<JobRecord>, StorageError> {
    let rx = read(database)?;
    let row = rx.open_table(JOBS)?.get(id.get())?.map(|v| JobRow::decode(v.value())).transpose()?;
    row.map(|row| job_record(&rx, row)).transpose()
}

/// Newest Jobs first; IDs are allocated in creation order.
pub(crate) fn read_jobs(database: &Database, before: Option<JobId>, limit: usize) -> Result<Vec<JobRecord>, StorageError> {
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

/// A Job's outputs in seq order, after `after_seq` when given.
pub(crate) fn read_outputs(database: &Database, job: JobId, after_seq: Option<i64>, limit: usize) -> Result<Vec<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    let index = rx.open_table(HAS_OUTPUT)?;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = after_seq.map_or(0, |seq| u64::try_from(seq).map_or(0, |seq| seq.saturating_add(1)));
    let mut records = Vec::new();
    for entry in index.range((job.get(), start)..=(job.get(), u64::MAX))?.take(limit.min(MAX_PAGE)) {
        let output = entry?.1.value();
        let bytes = outputs.get(output)?.ok_or_else(|| StorageError::corrupted(format!("HAS_OUTPUT names missing output {output}")))?;
        records.push(decode_output(bytes.value())?);
    }
    Ok(records)
}

pub(crate) fn read_latest_output(database: &Database, job: JobId) -> Result<Option<JobOutputRecord>, StorageError> {
    let rx = read(database)?;
    let Some(output) = rx.open_table(LATEST_OUTPUT)?.get(job.get())?.map(|v| v.value()) else {
        return Ok(None);
    };
    let bytes = rx
        .open_table(JOB_OUTPUTS)?
        .get(output)?
        .ok_or_else(|| StorageError::corrupted(format!("LATEST_OUTPUT names missing output {output}")))?;
    decode_output(bytes.value()).map(Some)
}

pub(crate) fn read_marker(database: &Database, operation: OperationId) -> Result<Option<OperationMarker>, StorageError> {
    let rx = read(database)?;
    let t = rx.open_table(OPERATIONS)?;
    let bytes = t.get((operation.epoch, operation.seq))?;
    bytes.map(|b| decode_marker(operation, b.value())).transpose()
}

/// Markers in commit-identifier order, after `after` when given.
pub(crate) fn read_markers(database: &Database, after: Option<OperationId>, limit: usize) -> Result<Vec<OperationMarker>, StorageError> {
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
