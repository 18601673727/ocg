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

use std::collections::{BTreeSet, HashMap, HashSet};
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
    RequestNumber, RequestRef, SubmittedConfiguration, Timestamp, ToolAdmission, ToolCallRef,
    ToolDecision, ToolEffect, ToolEvidenceRecord, ToolOutcome, ToolPayload, ToolReconciliation,
    UncertainCause, Violation, WithheldReason, WrongProfileKind,
};
use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const FORMAT_NAME: &[u8] = b"ocg-foundation";
/// Version 2 retired JobStep and JobType and added Genesis, Projects,
/// Profiles, Agents, LLM Providers and HealthChecks. Version 3 added the
/// configuration links TARGETS_LLM, APPLIES_PROFILE, RUNS_LLM_PROFILE and
/// SERVES; a version 2 build would ignore them and write past their rules.
/// Version 4 made Profiles editable: JobInputs freeze their Job's execution
/// configuration, Profiles carry `updated_at`, HealthChecks record the
/// configuration they checked and SERVES entries the configuration they
/// were registered for. Version 5 retired HealthChecks and SERVES: Providers
/// and Executors name LLM transports, configured Jobs may run an LLM without
/// an Agent Profile, Jobs record a confirmed stop, outputs carry a JSON
/// value, and Metrics and AvailabilityStats are recorded per Job. Version 6
/// replaced the per-LLM-Profile observation indexes with indexes by
/// observation scope; a version 5 file lacks their entries. Version 7 added
/// the AvailabilityStat aggregation state: request samples, sparse minute
/// counts and consumption progress. A version 6 file lacks those tables, so
/// it is refused rather than initialized in place. Version 8 added the
/// optional Token annotation to Metrics records; a version 7 build would
/// not decode it, and no annotation is invented for an older file. Version 9
/// added the Token aggregation state: per-Job request evidence, sparse minute
/// totals and the Token stream's progress. A version 8 file lacks those
/// tables, so it is refused rather than initialized in place. Version 10
/// added the local execution claims of Jobs started by this process's own
/// runtime; a version 9 build would leave them behind when it settled such
/// a Job. Version 11 numbers the Provider requests of a Job: every Metrics
/// and AvailabilityStat record names its request, and the aggregation state
/// is kept per request rather than per Job. Version 12 adds the evidence of
/// each tool call a Provider request asked for; an older build would settle
/// Jobs past their unresolved tool effects. Version 13 adds Attachments and
/// the ordered Attachments of each JobInput; an older build would decode
/// such an input without them. Files of any other version are refused on
/// open, before anything is written to them.
pub(crate) const FORMAT_VERSION: u32 = 13;
pub(crate) const MAX_WAITING_LAYERS: usize = 64;
pub(crate) const MAX_WAITING_JOBS: usize = 10_000;
pub(crate) const MAX_PAGE: usize = 1_000;
/// The most index entries one observation query reads, matching or not.
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;
/// The most distinct (Provider, model) pairs one choice query examines,
/// matching or not.
pub(crate) const MAX_OBSERVED_MODEL_SCAN: usize = 2_000;

type Records = TableDefinition<'static, u64, &'static [u8]>;
type Link = TableDefinition<'static, u64, u64>;
type PairSet = TableDefinition<'static, (u64, u64), ()>;
type ScopeIndex = TableDefinition<'static, (&'static [u8], i64, u64), ()>;
/// (job, request number) -> a request's encoded state.
type RequestRecords = TableDefinition<'static, (u64, u32), &'static [u8]>;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const GENESIS_KEY: &str = "genesis";
const SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("sequence");
const JOBS: Records = TableDefinition::new("job");
const JOB_INPUTS: Records = TableDefinition::new("job_input");
const JOB_OUTPUTS: Records = TableDefinition::new("job_output");
const EXECUTORS: Records = TableDefinition::new("executor");
const PROJECTS: Records = TableDefinition::new("project");
const PROFILES: Records = TableDefinition::new("profile");
const AGENTS: Records = TableDefinition::new("agent");
const LLM_PROVIDERS: Records = TableDefinition::new("llm_provider");
const METRICS: Records = TableDefinition::new("metrics");
const AVAILABILITY_STATS: Records = TableDefinition::new("availability_stat");
const ATTACHMENTS: Records = TableDefinition::new("attachment");
/// job -> input, and its incoming index (input, job). A retry may run with
/// its original's frozen input, so one input can serve several Jobs.
const HAS_INPUT: Link = TableDefinition::new("has_input");
const INPUT_JOBS: PairSet = TableDefinition::new("has_input_in");
/// HAS_ATTACHMENT: an input's Attachments by their position in it, from 0,
/// and the inputs that carry each Attachment.
const HAS_ATTACHMENT: TableDefinition<(u64, u32), u64> = TableDefinition::new("has_attachment");
const ATTACHMENT_INPUTS: PairSet = TableDefinition::new("has_attachment_in");
/// (job, seq) -> output; ordered by seq within a Job.
const HAS_OUTPUT: TableDefinition<(u64, u64), u64> = TableDefinition::new("has_output");
/// job -> its highest-seq output, moved by every output insert.
const LATEST_OUTPUT: Link = TableDefinition::new("latest_output");
/// child -> parent, and its incoming index (parent, child). The Genesis
/// root marker (genesis -> genesis) has no incoming entry.
const CHILD_OF: Link = TableDefinition::new("child_of");
const CHILDREN: PairSet = TableDefinition::new("child_of_in");
/// (job, prerequisite) -> condition code.
const DEPENDS_ON: TableDefinition<(u64, u64), u8> = TableDefinition::new("depends_on");
const ASSIGNED_TO: Link = TableDefinition::new("assigned_to");
const PREVIOUS: Link = TableDefinition::new("previous");
const REPLACES: Link = TableDefinition::new("replaces");
/// retry -> original, and its incoming index (original, retry).
const RETRY_OF: Link = TableDefinition::new("retry_of");
const RETRIES: PairSet = TableDefinition::new("retry_of_in");
/// (job, project), and its incoming index (project, job).
const IN_PROJECT: PairSet = TableDefinition::new("in_project");
const PROJECT_JOBS: PairSet = TableDefinition::new("in_project_in");
/// (agent, Agent Profile), and its incoming index (profile, agent).
const USES_PROFILE: PairSet = TableDefinition::new("uses_profile");
const PROFILE_AGENTS: PairSet = TableDefinition::new("uses_profile_in");
/// LLM Profile -> LLM Provider, and its incoming index (provider, profile).
const USES_PROVIDER: Link = TableDefinition::new("uses_provider");
const PROVIDER_PROFILES: PairSet = TableDefinition::new("uses_provider_in");
/// (Agent Profile, LLM Profile): the LLM Profiles an Agent Profile may run
/// with, fixed when the Agent Profile is created.
const TARGETS_LLM: PairSet = TableDefinition::new("targets_llm");
/// Job -> the Agent Profile its submission applied, and its incoming index.
const APPLIES_PROFILE: Link = TableDefinition::new("applies_profile");
const AGENT_PROFILE_JOBS: PairSet = TableDefinition::new("applies_profile_in");
/// Job -> the LLM Profile its submission chose, and its incoming index. A
/// Job with an APPLIES_PROFILE edge has this one too; a Job that runs an LLM
/// without an Agent Profile has only this one.
const RUNS_LLM_PROFILE: Link = TableDefinition::new("runs_llm_profile");
const LLM_PROFILE_JOBS: PairSet = TableDefinition::new("runs_llm_profile_in");
/// (transport code, Executor): an index of the transports in each Executor
/// record, written with the record at registration.
const TRANSPORT_EXECUTORS: TableDefinition<(u8, u64), ()> =
    TableDefinition::new("llm_transport_executors");
/// Metrics -> the Job it measured, and its incoming index (job, metrics).
const MEASURES: Link = TableDefinition::new("measures");
const JOB_METRICS: PairSet = TableDefinition::new("measures_in");
/// (scope, observed_at, metrics) for every [`Scope`] a Metrics record is
/// in, written with the record from its MEASURES Job and that Job's frozen
/// input, all fixed.
const METRICS_BY_SCOPE: ScopeIndex = TableDefinition::new("metrics_by_scope");
/// AvailabilityStat -> the Job whose request it observed, and its incoming
/// index (job, stat).
const OBSERVES: Link = TableDefinition::new("observes");
const JOB_AVAILABILITY: PairSet = TableDefinition::new("observes_in");
/// (scope, observed_at, stat), written like METRICS_BY_SCOPE.
const AVAILABILITY_BY_SCOPE: ScopeIndex = TableDefinition::new("availability_stat_by_scope");
/// Job -> the epoch of the opening whose own runtime started it. Written
/// with the start and removed when the Job settles: with a confirmed outcome,
/// or with a confirmed stop after an unknown one. An entry of an earlier
/// opening is work that no running process can still execute or settle.
const LOCAL_EXECUTIONS: Link = TableDefinition::new("local_execution");
/// (epoch, seq) -> operation marker.
const OPERATIONS: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("operation");
/// (job, request) -> its request sample as materialized. Rebuildable from
/// the raw AvailabilityStat records; the sample is fixed by its lowest record.
const AVAILABILITY_SAMPLES: RequestRecords = TableDefinition::new("availability_request_sample");
/// (provider, minute, model) -> requests per classification, for the
/// buckets that hold at least one request. Rebuildable like the samples.
const AVAILABILITY_MINUTES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("availability_minute");
/// The aggregation stream's progress: its last consumed AvailabilityStat ID
/// and the wall-clock time of its last batch. It is written only with the
/// batch it describes.
const AGGREGATION_PROGRESS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("aggregation_progress");

/// (job, request) -> its Token request state: the frozen target and the
/// finite evidence the request has so far. Rebuildable from the raw
/// annotated Metrics.
const TOKEN_REQUESTS: RequestRecords = TableDefinition::new("token_request");
/// (provider, minute, model) -> Token totals of the buckets that hold at
/// least one eligible request. Rebuildable like the requests.
const TOKEN_MINUTES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("token_minute");

/// (job, request, index) -> the evidence of the tool call at that wire index
/// of the Job's Provider request. The key is the call's identity, so a call
/// has one record. Not rebuildable: it is the only record of these facts.
const TOOL_EVIDENCE: TableDefinition<(u64, u32, u32), &[u8]> =
    TableDefinition::new("tool_evidence");
/// (job, Provider tool call id) -> (request, index) of the one evidence
/// record of the Job that uses the id, written with that record.
const TOOL_CALL_IDS: TableDefinition<(u64, &str), (u32, u32)> =
    TableDefinition::new("tool_call_id");

mod availability_aggregation;
mod token_aggregation;
pub(crate) use availability_aggregation::{
    AvailabilityPlan, MAX_AGGREGATION_ATTEMPTS, commit_availability_aggregation,
    prepare_availability_aggregation, read_availability_minute, read_availability_progress,
    read_request_sample,
};
#[cfg(test)]
pub(crate) use availability_aggregation::{WriteProbe, replace_minute};
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

// ---- explicit versioned encoding ----------------------------------------------

const RECORD_VERSION: u8 = 1;
const TAG_JOB: u8 = 1;
const TAG_INPUT: u8 = 2;
const TAG_OUTPUT: u8 = 3;
// Tag 4 was the retired JobStep record.
const TAG_EXECUTOR: u8 = 5;
const TAG_OPERATION: u8 = 6;
const TAG_PROJECT: u8 = 7;
const TAG_PROFILE: u8 = 8;
const TAG_AGENT: u8 = 9;
const TAG_LLM_PROVIDER: u8 = 10;
// Tags 11 and 12 were the retired HealthCheck and SERVES records.
const TAG_METRICS: u8 = 13;
const TAG_AVAILABILITY_STAT: u8 = 14;
const TAG_REQUEST_SAMPLE: u8 = 15;
const TAG_MINUTE_COUNTS: u8 = 16;
const TAG_AGGREGATION_PROGRESS: u8 = 17;
const TAG_TOKEN_REQUEST: u8 = 18;
const TAG_TOKEN_MINUTE: u8 = 19;
const TAG_TOOL_EVIDENCE: u8 = 20;
const TAG_ATTACHMENT: u8 = 21;

struct Enc(Vec<u8>);

impl Enc {
    fn new(tag: u8) -> Self {
        Enc(vec![tag, RECORD_VERSION])
    }
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
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
    fn f64(&mut self, v: f64) {
        self.u64(v.to_bits());
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
    fn ou32(&mut self, v: Option<u32>) {
        self.opt(&v, |e, v| e.u32(*v));
    }
    fn ou64(&mut self, v: Option<u64>) {
        self.opt(&v, |e, v| e.u64(*v));
    }
    fn of64(&mut self, v: Option<f64>) {
        self.opt(&v, |e, v| e.f64(*v));
    }
    fn ojson(&mut self, v: &Option<serde_json::Value>) {
        self.opt(v, |e, v| e.str(&v.to_string()));
    }
    fn record_ref(&mut self, r: RecordRef) {
        let (tag, id) = match r {
            RecordRef::Job(id) => (1, id.get()),
            RecordRef::JobInput(id) => (2, id.get()),
            RecordRef::JobOutput(id) => (3, id.get()),
            // Tag 4 was the retired JobStep reference.
            RecordRef::Executor(id) => (5, id.get()),
            RecordRef::Project(id) => (6, id.get()),
            RecordRef::Profile(id) => (7, id.get()),
            RecordRef::Agent(id) => (8, id.get()),
            RecordRef::LlmProvider(id) => (9, id.get()),
            // Tag 10 was the retired HealthCheck reference.
            RecordRef::Metrics(id) => (11, id.get()),
            RecordRef::AvailabilityStat(id) => (12, id.get()),
            RecordRef::Attachment(id) => (13, id.get()),
        };
        self.u8(tag);
        self.u64(id);
    }
    fn profile_config(&mut self, config: &ProfileConfig) {
        match config {
            ProfileConfig::Agent(agent) => {
                self.u8(1);
                self.agent_config(agent);
            }
            ProfileConfig::Llm(llm) => {
                self.u8(2);
                self.llm_config(llm);
            }
        }
    }
    fn agent_config(&mut self, agent: &AgentConfig) {
        self.ostr(&agent.instructions);
    }
    fn llm_config(&mut self, llm: &LlmConfig) {
        self.str(&llm.model);
        let parameters = &llm.parameters;
        self.of64(parameters.temperature);
        self.of64(parameters.top_p);
        self.ou32(parameters.max_tokens);
        self.of64(parameters.presence_penalty);
        self.of64(parameters.frequency_penalty);
        self.u32(u32::try_from(llm.declared_capabilities.len()).unwrap_or(u32::MAX));
        for capability in &llm.declared_capabilities {
            self.u8(capability.code());
        }
    }
    fn frozen_llm(&mut self, frozen: &FrozenLlmConfig) {
        self.llm_config(&frozen.config);
        let provider = &frozen.provider;
        self.u64(provider.id.get());
        self.str(&provider.name);
        self.u8(provider.transport.code());
        self.ostr(&provider.endpoint);
        self.ostr(&provider.credential_ref);
    }
    fn frozen_execution(&mut self, frozen: &FrozenExecutionConfig) {
        self.opt(&frozen.agent, Enc::agent_config);
        self.frozen_llm(&frozen.llm);
    }
    fn transports(&mut self, transports: &BTreeSet<LlmTransport>) {
        self.u32(u32::try_from(transports.len()).unwrap_or(u32::MAX));
        for transport in transports {
            self.u8(transport.code());
        }
    }
    fn provider_failure(&mut self, failure: &ProviderFailure) {
        self.str(&failure.message);
        self.opt(&failure.status_code, |e, v| e.u16(*v));
        self.ostr(&failure.provider_code);
        self.ostr(&failure.response_body);
        self.ostr(&failure.request_id);
        self.ou64(failure.retry_after_ms);
        self.bool(failure.retryable);
    }
    fn usage(&mut self, usage: &NormalizedUsage) {
        for count in [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage.reasoning_tokens,
        ] {
            self.ou32(count);
        }
        self.ojson(&usage.raw);
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
            _ => Err(StorageError::corrupted(format!(
                "expected a record of type {tag}"
            ))),
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
    fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }
    fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }
    fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
    fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
    fn f64(&mut self) -> DecodeResult<f64> {
        Ok(f64::from_bits(self.u64()?))
    }
    fn bool(&mut self) -> DecodeResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageError::corrupted("invalid boolean")),
        }
    }
    /// A stored request number, which is never 0.
    fn request(&mut self) -> DecodeResult<RequestNumber> {
        let number = self.u32()?;
        RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))
    }

    fn ts(&mut self) -> DecodeResult<Timestamp> {
        Ok(Timestamp::from_unix_micros(self.i64()?))
    }
    fn str(&mut self) -> DecodeResult<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| StorageError::corrupted("invalid UTF-8"))
    }
    fn opt<T>(
        &mut self,
        get: impl FnOnce(&mut Self) -> DecodeResult<T>,
    ) -> DecodeResult<Option<T>> {
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
    fn ou32(&mut self) -> DecodeResult<Option<u32>> {
        self.opt(Self::u32)
    }
    fn ou64(&mut self) -> DecodeResult<Option<u64>> {
        self.opt(Self::u64)
    }
    fn of64(&mut self) -> DecodeResult<Option<f64>> {
        self.opt(Self::f64)
    }
    fn ojson(&mut self) -> DecodeResult<Option<serde_json::Value>> {
        self.opt(|d| {
            serde_json::from_str(&d.str()?)
                .map_err(|_| StorageError::corrupted("invalid JSON value"))
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
            5 => RecordRef::Executor(ExecutorId::new(id)),
            6 => RecordRef::Project(ProjectId::new(id)),
            7 => RecordRef::Profile(ProfileId::new(id)),
            8 => RecordRef::Agent(AgentId::new(id)),
            9 => RecordRef::LlmProvider(LlmProviderId::new(id)),
            11 => RecordRef::Metrics(MetricsId::new(id)),
            12 => RecordRef::AvailabilityStat(AvailabilityStatId::new(id)),
            13 => RecordRef::Attachment(AttachmentId::new(id)),
            _ => return Err(StorageError::corrupted("invalid record reference")),
        })
    }
    fn profile_config(&mut self) -> DecodeResult<ProfileConfig> {
        Ok(match self.u8()? {
            1 => ProfileConfig::Agent(self.agent_config()?),
            2 => ProfileConfig::Llm(self.llm_config()?),
            _ => {
                return Err(StorageError::corrupted(
                    "invalid Profile configuration kind",
                ));
            }
        })
    }
    fn agent_config(&mut self) -> DecodeResult<AgentConfig> {
        Ok(AgentConfig {
            instructions: self.ostr()?,
        })
    }
    fn llm_config(&mut self) -> DecodeResult<LlmConfig> {
        let model = self.str()?;
        let parameters = LlmParameters {
            temperature: self.of64()?,
            top_p: self.of64()?,
            max_tokens: self.ou32()?,
            presence_penalty: self.of64()?,
            frequency_penalty: self.of64()?,
        };
        let mut declared_capabilities = BTreeSet::new();
        for _ in 0..self.u32()? {
            if !declared_capabilities.insert(self.code(ModelCapability::from_code)?) {
                return Err(StorageError::corrupted("repeated declared capability"));
            }
        }
        Ok(LlmConfig {
            model,
            parameters,
            declared_capabilities,
        })
    }
    fn frozen_llm(&mut self) -> DecodeResult<FrozenLlmConfig> {
        let config = self.llm_config()?;
        let provider = ProviderSnapshot {
            id: LlmProviderId::new(self.u64()?),
            name: self.str()?,
            transport: self.code(LlmTransport::from_code)?,
            endpoint: self.ostr()?,
            credential_ref: self.ostr()?,
        };
        Ok(FrozenLlmConfig { config, provider })
    }
    fn frozen_execution(&mut self) -> DecodeResult<FrozenExecutionConfig> {
        Ok(FrozenExecutionConfig {
            agent: self.opt(Self::agent_config)?,
            llm: self.frozen_llm()?,
        })
    }
    fn transports(&mut self) -> DecodeResult<BTreeSet<LlmTransport>> {
        let mut transports = BTreeSet::new();
        for _ in 0..self.u32()? {
            if !transports.insert(self.code(LlmTransport::from_code)?) {
                return Err(StorageError::corrupted("repeated LLM transport"));
            }
        }
        Ok(transports)
    }
    fn provider_failure(&mut self) -> DecodeResult<ProviderFailure> {
        Ok(ProviderFailure {
            message: self.str()?,
            status_code: self.opt(Self::u16)?,
            provider_code: self.ostr()?,
            response_body: self.ostr()?,
            request_id: self.ostr()?,
            retry_after_ms: self.ou64()?,
            retryable: self.bool()?,
        })
    }
    fn usage(&mut self) -> DecodeResult<NormalizedUsage> {
        Ok(NormalizedUsage {
            input_tokens: self.ou32()?,
            output_tokens: self.ou32()?,
            cache_read_tokens: self.ou32()?,
            cache_write_tokens: self.ou32()?,
            reasoning_tokens: self.ou32()?,
            raw: self.ojson()?,
        })
    }
    fn end(self) -> DecodeResult<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageError::corrupted("trailing record bytes"))
        }
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

codes!(ExecutionMode { ExecutionMode::Once = 1, ExecutionMode::Continuous = 2, ExecutionMode::Periodic = 3, ExecutionMode::Container = 4 });
codes!(OutputKind { OutputKind::Progress = 1, OutputKind::Evaluation = 2, OutputKind::Result = 3 });
codes!(ExecutorType { ExecutorType::System = 1, ExecutorType::Llm = 2, ExecutorType::Agent = 3, ExecutorType::Tool = 4, ExecutorType::Database = 5, ExecutorType::Frontend = 6 });
codes!(DependencyCondition { DependencyCondition::Success = 1, DependencyCondition::Settled = 2 });
codes!(ModelCapability { ModelCapability::Chat = 1, ModelCapability::Tools = 2, ModelCapability::Vision = 3, ModelCapability::Reasoning = 4 });
codes!(LlmTransport { LlmTransport::OpenAiChatCompletions = 1 });
codes!(ChatFinishReason {
    ChatFinishReason::Stop = 1, ChatFinishReason::Length = 2, ChatFinishReason::ToolCalls = 3,
    ChatFinishReason::ContentFilter = 4, ChatFinishReason::Error = 5, ChatFinishReason::Other = 6,
});
// Codes 4, 5 and 6 were the retired JobStep commands; 18 recorded HealthChecks.
codes!(OperationKind {
    OperationKind::SaveJobInput = 1, OperationKind::CreateJobWithInput = 2, OperationKind::AssignJobExecutor = 3,
    OperationKind::AppendJobOutput = 7, OperationKind::FinalizeJob = 8, OperationKind::CreateExecutor = 9,
    OperationKind::BeginCoreLogCleanup = 10, OperationKind::StartJob = 11, OperationKind::RetryJob = 12,
    OperationKind::AssociateJobProject = 13, OperationKind::CreateProject = 14, OperationKind::CreateLlmProvider = 15,
    OperationKind::CreateProfile = 16, OperationKind::CreateAgent = 17, OperationKind::UpdateProfile = 19,
    OperationKind::ConfirmJobStopped = 20, OperationKind::RecordMetrics = 21, OperationKind::RecordAvailabilityStat = 22,
    OperationKind::ConsumeAvailability = 23, OperationKind::ConsumeTokenUsage = 24,
    OperationKind::StartLocalJob = 25, OperationKind::ReconcileAbandonedJob = 26,
    OperationKind::RecordToolDecision = 27, OperationKind::RecordToolOutcome = 28,
    OperationKind::ReconcileToolEffect = 29, OperationKind::CreateAttachment = 30,
});
codes!(ToolCallRefusal {
    ToolCallRefusal::Malformed = 1, ToolCallRefusal::ConflictingIdentity = 2, ToolCallRefusal::AmbiguousIdentity = 3,
    ToolCallRefusal::LimitExceeded = 4, ToolCallRefusal::Incomplete = 5, ToolCallRefusal::Unauthorized = 6,
    ToolCallRefusal::MissingSchema = 7, ToolCallRefusal::UnsupportedSchema = 8, ToolCallRefusal::InvalidArguments = 9,
});
codes!(ToolEffect { ToolEffect::ReadOnly = 1, ToolEffect::Idempotent = 2, ToolEffect::NonIdempotent = 3 });
codes!(AuthorizationBasis { AuthorizationBasis::RegistryGrant = 1 });
codes!(WithheldReason { WithheldReason::TooLarge = 1, WithheldReason::Unserializable = 2 });
codes!(UncertainCause { UncertainCause::TimedOut = 1, UncertainCause::Cancelled = 2, UncertainCause::HandlerReported = 3 });
codes!(EffectConclusion { EffectConclusion::Applied = 1, EffectConclusion::NotApplied = 2 });
codes!(AttachmentOrigin { AttachmentOrigin::UserProvided = 1, AttachmentOrigin::ModelProduced = 2 });
codes!(CleanupKind { CleanupKind::Manual = 1, CleanupKind::Automatic = 2 });

// ---- rows ------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JobRow {
    pub(crate) id: JobId,
    pub(crate) name: Option<String>,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) eligible_at: Option<Timestamp>,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) paused_at: Option<Timestamp>,
    pub(crate) sterilized_at: Option<Timestamp>,
    pub(crate) completed_at: Option<Timestamp>,
    pub(crate) failed_at: Option<Timestamp>,
    pub(crate) cancelled_at: Option<Timestamp>,
    pub(crate) unknown_at: Option<Timestamp>,
    pub(crate) stop_confirmed_at: Option<Timestamp>,
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

    /// Whether the Job is settled: it has a confirmed outcome, or an unknown
    /// outcome whose execution the trusted path confirmed stopped. An unknown
    /// outcome without that confirmation may still be executing.
    pub(crate) fn settled(&self) -> bool {
        self.confirmed_terminal() || (self.unknown_at.is_some() && self.stop_confirmed_at.is_some())
    }

    /// Whether this Job holds the Profiles it was submitted with: until a
    /// confirmed outcome, or, for an unknown outcome, until its execution is
    /// confirmed stopped.
    pub(crate) fn holds_profiles(&self) -> bool {
        !self.confirmed_terminal() && self.stop_confirmed_at.is_none()
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB);
        e.u64(self.id.get());
        e.ostr(&self.name);
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
            self.stop_confirmed_at,
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
            execution_mode: d.code(ExecutionMode::from_code)?,
            eligible_at: d.ots()?,
            started_at: d.ots()?,
            paused_at: d.ots()?,
            sterilized_at: d.ots()?,
            completed_at: d.ots()?,
            failed_at: d.ots()?,
            cancelled_at: d.ots()?,
            unknown_at: d.ots()?,
            stop_confirmed_at: d.ots()?,
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
    pub(crate) configuration: Option<FrozenExecutionConfig>,
}

impl JobInputRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_INPUT);
        e.u64(self.id.get());
        e.ostr(&self.submission_id);
        e.ostr(&self.objective);
        for limit in [
            self.max_depth,
            self.max_children_per_job,
            self.max_total_descendants,
        ] {
            e.ou32(limit);
        }
        e.ots(self.submitted_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.opt(&self.configuration, Enc::frozen_execution);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_INPUT)?;
        let row = JobInputRow {
            id: JobInputId::new(d.u64()?),
            submission_id: d.ostr()?,
            objective: d.ostr()?,
            max_depth: d.ou32()?,
            max_children_per_job: d.ou32()?,
            max_total_descendants: d.ou32()?,
            submitted_at: d.ots()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
            configuration: d.opt(Dec::frozen_execution)?,
        };
        d.end()?;
        Ok(row)
    }
}

fn encode_attachment(a: &AttachmentRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_ATTACHMENT);
    e.u64(a.id.get());
    e.0.extend_from_slice(a.sha256.as_bytes());
    e.u64(a.byte_length);
    e.str(&a.mime);
    e.ostr(&a.display_name);
    e.u8(a.origin.code());
    e.ts(a.created_at);
    // Kind and time, together or not at all.
    e.opt(&a.cleanup, |e, cleanup| {
        e.u8(cleanup.kind.code());
        e.ts(cleanup.cleaned_at);
    });
    e.0
}

fn decode_attachment(bytes: &[u8]) -> DecodeResult<AttachmentRecord> {
    let mut d = Dec::new(bytes, TAG_ATTACHMENT)?;
    let a = AttachmentRecord {
        id: AttachmentId::new(d.u64()?),
        sha256: ContentDigest::from_bytes(d.take(32)?.try_into().expect("32 bytes")),
        byte_length: d.u64()?,
        mime: d.str()?,
        display_name: d.ostr()?,
        origin: d.code(AttachmentOrigin::from_code)?,
        created_at: d.ts()?,
        cleanup: d.opt(|d| {
            Ok(AttachmentCleanup {
                kind: d.code(CleanupKind::from_code)?,
                cleaned_at: d.ts()?,
            })
        })?,
    };
    d.end()?;
    Ok(a)
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
    e.ojson(&o.value);
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
        value: d.ojson()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(o)
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
    pub(crate) llm_transports: BTreeSet<LlmTransport>,
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
            llm_transports: decoder.transports()?,
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
        e.transports(&self.llm_transports);
        e.0
    }
}

fn encode_project(p: &ProjectRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_PROJECT);
    e.u64(p.id.get());
    e.str(&p.name);
    e.ostr(&p.description);
    e.ts(p.created_at);
    e.0
}

fn decode_project(bytes: &[u8]) -> DecodeResult<ProjectRecord> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let p = ProjectRecord {
        id: ProjectId::new(d.u64()?),
        name: d.str()?,
        description: d.ostr()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(p)
}

fn encode_llm_provider(p: &LlmProviderRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_LLM_PROVIDER);
    e.u64(p.id.get());
    e.str(&p.name);
    e.u8(p.transport.code());
    e.ostr(&p.endpoint);
    e.ostr(&p.credential_ref);
    e.ts(p.created_at);
    e.0
}

fn provider_snapshot(provider: LlmProviderRecord) -> ProviderSnapshot {
    ProviderSnapshot {
        id: provider.id,
        name: provider.name,
        transport: provider.transport,
        endpoint: provider.endpoint,
        credential_ref: provider.credential_ref,
    }
}

fn decode_llm_provider(bytes: &[u8]) -> DecodeResult<LlmProviderRecord> {
    let mut d = Dec::new(bytes, TAG_LLM_PROVIDER)?;
    let p = LlmProviderRecord {
        id: LlmProviderId::new(d.u64()?),
        name: d.str()?,
        transport: d.code(LlmTransport::from_code)?,
        endpoint: d.ostr()?,
        credential_ref: d.ostr()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(p)
}

/// A Profile's own attributes; its Provider lives in USES_PROVIDER.
#[derive(Clone, Debug)]
pub(crate) struct ProfileRow {
    pub(crate) id: ProfileId,
    pub(crate) name: String,
    pub(crate) config: ProfileConfig,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl ProfileRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_PROFILE);
        e.u64(self.id.get());
        e.str(&self.name);
        e.profile_config(&self.config);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_PROFILE)?;
        let row = Self {
            id: ProfileId::new(d.u64()?),
            name: d.str()?,
            config: d.profile_config()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// The references that define a Profile, written with it and replaced only
/// together with its payload.
pub(crate) enum ProfileLinks {
    Agent { llm_targets: Vec<ProfileId> },
    Llm { provider: LlmProviderId },
}

/// An Agent's own attributes; its Profiles live in USES_PROFILE.
#[derive(Clone, Debug)]
pub(crate) struct AgentRow {
    pub(crate) id: AgentId,
    pub(crate) name: String,
    pub(crate) created_at: Timestamp,
}

impl AgentRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_AGENT);
        e.u64(self.id.get());
        e.str(&self.name);
        e.ts(self.created_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_AGENT)?;
        let row = Self {
            id: AgentId::new(d.u64()?),
            name: d.str()?,
            created_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// A Metrics record's own attributes; its Job lives in MEASURES.
#[derive(Clone, Debug)]
pub(crate) struct MetricsRow {
    pub(crate) id: MetricsId,
    pub(crate) request: RequestNumber,
    pub(crate) observed_at: Timestamp,
    pub(crate) measurements: Measurements,
    pub(crate) token: Option<TokenAnnotation>,
    pub(crate) recorded_at: Timestamp,
}

impl MetricsRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_METRICS);
        e.u64(self.id.get());
        e.u32(self.request.get());
        e.ts(self.observed_at);
        let m = &self.measurements;
        for ms in [
            m.ttfb_ms,
            m.reasoning_ttft_ms,
            m.output_ttft_ms,
            m.request_finished_in_ms,
        ] {
            e.ou64(ms);
        }
        e.usage(&m.usage);
        e.opt(&self.token, |e, token| {
            e.u8(match token.eligibility {
                RequestEligibility::Eligible => 1,
                RequestEligibility::Excluded => 2,
            });
            e.u8(match token.finality {
                UsageFinality::Cumulative => 1,
                UsageFinality::Final => 2,
            });
        });
        e.ts(self.recorded_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_METRICS)?;
        let row = Self {
            id: MetricsId::new(d.u64()?),
            request: d.request()?,
            observed_at: d.ts()?,
            measurements: Measurements {
                ttfb_ms: d.ou64()?,
                reasoning_ttft_ms: d.ou64()?,
                output_ttft_ms: d.ou64()?,
                request_finished_in_ms: d.ou64()?,
                usage: d.usage()?,
            },
            token: d.opt(|d| {
                let eligibility = match d.u8()? {
                    1 => RequestEligibility::Eligible,
                    2 => RequestEligibility::Excluded,
                    other => {
                        return Err(StorageError::corrupted(format!(
                            "unknown Token eligibility code {other}"
                        )));
                    }
                };
                let finality = match d.u8()? {
                    1 => UsageFinality::Cumulative,
                    2 => UsageFinality::Final,
                    other => {
                        return Err(StorageError::corrupted(format!(
                            "unknown Token finality code {other}"
                        )));
                    }
                };
                Ok(TokenAnnotation {
                    eligibility,
                    finality,
                })
            })?,
            recorded_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// An AvailabilityStat's own attributes; its Job lives in OBSERVES.
#[derive(Clone, Debug)]
pub(crate) struct AvailabilityStatRow {
    pub(crate) id: AvailabilityStatId,
    pub(crate) request: RequestNumber,
    pub(crate) observed_at: Timestamp,
    pub(crate) observation: AvailabilityObservation,
    pub(crate) recorded_at: Timestamp,
}

impl AvailabilityStatRow {
    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_AVAILABILITY_STAT);
        e.u64(self.id.get());
        e.u32(self.request.get());
        e.ts(self.observed_at);
        let o = &self.observation;
        e.opt(&o.status_code, |e, v| e.u16(*v));
        e.opt(&o.finish_reason, |e, v| e.u8(v.code()));
        e.ostr(&o.raw_finish_reason);
        e.opt(&o.failure, Enc::provider_failure);
        e.ts(self.recorded_at);
        e.0
    }

    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_AVAILABILITY_STAT)?;
        let row = Self {
            id: AvailabilityStatId::new(d.u64()?),
            request: d.request()?,
            observed_at: d.ts()?,
            observation: AvailabilityObservation {
                status_code: d.opt(Dec::u16)?,
                finish_reason: d.opt(|d| d.code(ChatFinishReason::from_code))?,
                raw_finish_reason: d.ostr()?,
                failure: d.opt(Dec::provider_failure)?,
            },
            recorded_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
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

/// The record without its call, which is its key.
fn encode_tool_evidence(record: &ToolEvidenceRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_TOOL_EVIDENCE);
    e.str(&record.tool_call_id);
    e.str(&record.name);
    e.ts(record.decision.at);
    match &record.decision.fact {
        ToolDecision::Refused(refusal) => {
            e.u8(1);
            e.u8(refusal.code());
        }
        ToolDecision::Admitted(admission) => {
            e.u8(2);
            e.str(&admission.handler_version);
            e.u8(admission.effect.code());
            e.str(&admission.parameters);
            e.str(admission.arguments.as_str());
            e.u8(admission.authorization.code());
        }
    }
    e.opt(&record.outcome, |e, outcome| {
        e.ts(outcome.at);
        match &outcome.fact {
            ToolOutcome::Completed { result } => {
                e.u8(1);
                e.str(result.as_str());
            }
            ToolOutcome::CompletedWithheld { reason } => {
                e.u8(2);
                e.u8(reason.code());
            }
            ToolOutcome::NotApplied { message } => {
                e.u8(3);
                e.str(message);
            }
            ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Refused(refusal),
            } => {
                e.u8(4);
                e.u8(refusal.code());
            }
            ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Cancelled,
            } => e.u8(6),
            ToolOutcome::Uncertain { cause, message } => {
                e.u8(5);
                e.u8(cause.code());
                e.ostr(message);
            }
        }
    });
    e.opt(&record.reconciliation, |e, reconciliation| {
        e.ts(reconciliation.at);
        let fact = &reconciliation.fact;
        e.u8(fact.conclusion.code());
        e.str(&fact.reconciler);
        e.str(&fact.evidence);
        e.str(&fact.explanation);
    });
    e.0
}

fn decode_tool_evidence(call: ToolCallRef, bytes: &[u8]) -> DecodeResult<ToolEvidenceRecord> {
    let mut d = Dec::new(bytes, TAG_TOOL_EVIDENCE)?;
    let tool_call_id = d.str()?;
    let name = d.str()?;
    let decided_at = d.ts()?;
    let decision = match d.u8()? {
        1 => ToolDecision::Refused(d.code(ToolCallRefusal::from_code)?),
        2 => ToolDecision::Admitted(ToolAdmission {
            handler_version: d.str()?,
            effect: d.code(ToolEffect::from_code)?,
            parameters: d.str()?,
            arguments: ToolPayload::new(d.str()?),
            authorization: d.code(AuthorizationBasis::from_code)?,
        }),
        _ => return Err(StorageError::corrupted("invalid tool decision")),
    };
    let outcome = d.opt(|d| {
        let at = d.ts()?;
        let fact = match d.u8()? {
            1 => ToolOutcome::Completed {
                result: ToolPayload::new(d.str()?),
            },
            2 => ToolOutcome::CompletedWithheld {
                reason: d.code(WithheldReason::from_code)?,
            },
            3 => ToolOutcome::NotApplied { message: d.str()? },
            4 => ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Refused(d.code(ToolCallRefusal::from_code)?),
            },
            5 => ToolOutcome::Uncertain {
                cause: d.code(UncertainCause::from_code)?,
                message: d.ostr()?,
            },
            6 => ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Cancelled,
            },
            _ => return Err(StorageError::corrupted("invalid tool outcome")),
        };
        Ok(Recorded { fact, at })
    })?;
    let reconciliation = d.opt(|d| {
        let at = d.ts()?;
        let fact = ToolReconciliation {
            conclusion: d.code(EffectConclusion::from_code)?,
            reconciler: d.str()?,
            evidence: d.str()?,
            explanation: d.str()?,
        };
        Ok(Recorded { fact, at })
    })?;
    d.end()?;
    Ok(ToolEvidenceRecord {
        call,
        tool_call_id,
        name,
        decision: Recorded {
            fact: decision,
            at: decided_at,
        },
        outcome,
        reconciliation,
    })
}

/// Every tool evidence record of `job` in `table`, in call order.
fn job_tool_evidence(
    table: &impl ReadableTable<(u64, u32, u32), &'static [u8]>,
    job: JobId,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let mut records = Vec::new();
    for entry in table.range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        records.push(decode_tool_evidence(
            tool_call_ref(key.value())?,
            bytes.value(),
        )?);
    }
    Ok(records)
}

fn tool_call_ref((job, request, index): (u64, u32, u32)) -> DecodeResult<ToolCallRef> {
    Ok(ToolCallRef {
        job: JobId::new(job),
        request: RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?,
        index,
    })
}

fn tool_call_key(call: ToolCallRef) -> (u64, u32, u32) {
    (call.job.get(), call.request.get(), call.index)
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
    Executor,
    Project,
    Profile,
    Agent,
    LlmProvider,
    Metrics,
    AvailabilityStat,
    Attachment,
}

impl Sequence {
    fn key(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::JobInput => "job_input",
            Self::JobOutput => "job_output",
            Self::Executor => "executor",
            Self::Project => "project",
            Self::Profile => "profile",
            Self::Agent => "agent",
            Self::LlmProvider => "llm_provider",
            Self::Metrics => "metrics",
            Self::AvailabilityStat => "availability_stat",
            Self::Attachment => "attachment",
        }
    }
}

const OPEN_EPOCH: &str = "open_epoch";

/// A set of observation records with an index range of its own. Every
/// record is in `All`; one whose Job ran an LLM is also in the four scopes
/// of the target that Job froze.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Scope<'a> {
    All,
    LlmProfile(ProfileId),
    Provider(LlmProviderId),
    Model(&'a str),
    ProviderModel(LlmProviderId, &'a str),
}

impl Scope<'_> {
    /// A tag byte, then the scope's identity: IDs as 8 big-endian bytes and
    /// the model name as its UTF-8 bytes. Each tag's encoding is injective.
    fn key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::All => key.push(0),
            Self::LlmProfile(profile) => {
                key.push(1);
                key.extend_from_slice(&profile.get().to_be_bytes());
            }
            Self::Provider(provider) => {
                key.push(2);
                key.extend_from_slice(&provider.get().to_be_bytes());
            }
            Self::Model(model) => {
                key.push(3);
                key.extend_from_slice(model.as_bytes());
            }
            Self::ProviderModel(provider, model) => {
                key.push(4);
                key.extend_from_slice(&provider.get().to_be_bytes());
                key.extend_from_slice(model.as_bytes());
            }
        }
        key
    }

    fn of(target: Option<&LlmTarget>) -> Vec<Scope<'_>> {
        let mut scopes = vec![Scope::All];
        if let Some(target) = target {
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
        scopes
    }

    /// The scope a query scans: the LLM Profile when one is given, since a
    /// Profile's history may span several targets, and otherwise the
    /// narrowest scope the filter names. The filter is still checked
    /// against every record read.
    fn for_filter(filter: &ObservationFilter) -> Scope<'_> {
        match (filter.llm_profile, filter.provider, filter.model.as_deref()) {
            (Some(profile), _, _) => Scope::LlmProfile(profile),
            (None, Some(provider), Some(model)) => Scope::ProviderModel(provider, model),
            (None, Some(provider), None) => Scope::Provider(provider),
            (None, None, Some(model)) => Scope::Model(model),
            (None, None, None) => Scope::All,
        }
    }
}

fn filter_matches(filter: &ObservationFilter, target: Option<&LlmTarget>) -> bool {
    match target {
        Some(target) => {
            filter
                .llm_profile
                .is_none_or(|profile| profile == target.llm_profile)
                && filter
                    .provider
                    .is_none_or(|provider| provider == target.frozen.provider.id)
                && filter
                    .model
                    .as_deref()
                    .is_none_or(|model| model == target.frozen.config.model)
        }
        None => filter.llm_profile.is_none() && filter.provider.is_none() && filter.model.is_none(),
    }
}

// ---- open ----------------------------------------------------------------------

pub(crate) enum OpenError {
    Storage(StorageError),
    /// The file is a redb database that this application did not create.
    ForeignFile,
    /// The file uses a storage format this build does not support.
    UnsupportedFormat {
        found: u32,
    },
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub(crate) fn database_file(directory: &Path) -> PathBuf {
    directory.join(DATABASE_FILE_NAME)
}

/// An opened database file with this opening's operation epoch and the
/// instance's Genesis Job.
pub(crate) struct OpenedFile {
    pub(crate) database: Database,
    pub(crate) epoch: u64,
    pub(crate) genesis: JobId,
}

/// Opens or creates the database file, checks its storage format, creates
/// the Genesis Job with a new file or verifies the recorded one, and durably
/// allocates this opening's operation epoch, all in one transaction.
pub(crate) fn open(directory: &Path) -> Result<OpenedFile, OpenError> {
    let path = database_file(directory);
    prepare_private_file(&path)?;
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    // A writable opening lets redb rewrite its own bookkeeping in the file,
    // so an existing file is first checked through a read-only opening and a
    // file of another format is refused without one. A file that cannot be
    // opened read-only, such as one that needs repair, is checked again by
    // `initialize` before anything is written to it.
    if path.metadata().is_ok_and(|metadata| metadata.len() > 0)
        && let Ok(existing) = builder.open_read_only(&path)
    {
        check_existing_format(&existing)?;
    }
    let database = builder.create(&path).map_err(StorageError::from)?;
    let (epoch, genesis) = initialize(&database)?;
    Ok(OpenedFile {
        database,
        epoch,
        genesis,
    })
}

/// Creates the database file empty and private to its owner (0600 on Unix)
/// when it is missing, so redb initializes a file nobody else can read, and
/// makes an existing regular file private. A symbolic link is not followed.
fn prepare_private_file(path: &Path) -> Result<(), StorageError> {
    let io_error = |error: std::io::Error| StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: format!("cannot make {} private: {error}", path.display()),
    };
    match crate::data_directory::private_file(&mut std::fs::OpenOptions::new())
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            crate::data_directory::restrict_file(path).map_err(io_error)
        }
        Err(error) => Err(io_error(error)),
    }
}

/// How a file's recorded format compares with this build's.
enum FormatVerdict {
    /// A file with no tables, which this opening will initialize.
    New,
    Supported,
}

fn format_verdict(
    format: Option<Vec<u8>>,
    version: Option<Vec<u8>>,
    existing_tables: usize,
) -> Result<FormatVerdict, OpenError> {
    match (format, version) {
        (None, None) if existing_tables == 0 => Ok(FormatVerdict::New),
        (Some(format), Some(version)) if format == FORMAT_NAME => {
            let found = <[u8; 4]>::try_from(version.as_slice())
                .map(u32::from_le_bytes)
                .unwrap_or(u32::MAX);
            match found {
                FORMAT_VERSION => Ok(FormatVerdict::Supported),
                _ => Err(OpenError::UnsupportedFormat { found }),
            }
        }
        _ => Err(OpenError::ForeignFile),
    }
}

fn check_existing_format(database: &redb::ReadOnlyDatabase) -> Result<(), OpenError> {
    let rx = database.begin_read().map_err(StorageError::from)?;
    let existing_tables = rx.list_tables().map_err(StorageError::from)?.count();
    let (format, version) = match rx.open_table(META) {
        Ok(meta) => (
            meta.get("format")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
            meta.get("format_version")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
        ),
        Err(redb::TableError::TableDoesNotExist(_)) => (None, None),
        Err(error) => return Err(StorageError::from(error).into()),
    };
    format_verdict(format, version, existing_tables).map(|_| ())
}

/// Refuses a foreign or other-format file before writing anything to it.
fn initialize(database: &Database) -> Result<(u64, JobId), OpenError> {
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    let existing_tables = tx.list_tables().map_err(StorageError::from)?.count();
    let opened = {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        let format = meta
            .get("format")
            .map_err(StorageError::from)?
            .map(|v| v.value().to_vec());
        let version = meta
            .get("format_version")
            .map_err(StorageError::from)?
            .map(|v| v.value().to_vec());
        let verdict = match format_verdict(format, version, existing_tables) {
            Ok(verdict) => verdict,
            Err(refusal) => {
                drop(meta);
                tx.abort().map_err(StorageError::from)?;
                return Err(refusal);
            }
        };
        let created = matches!(verdict, FormatVerdict::New);
        if created {
            meta.insert("format", FORMAT_NAME)
                .map_err(StorageError::from)?;
            meta.insert("format_version", FORMAT_VERSION.to_le_bytes().as_slice())
                .map_err(StorageError::from)?;
        }
        drop(meta);
        create_tables(&tx)?;
        let genesis = if created {
            create_genesis(&tx)?
        } else {
            verify_genesis(&tx)?
        };
        let mut sequences = tx.open_table(SEQUENCES).map_err(StorageError::from)?;
        let epoch = sequences
            .get(OPEN_EPOCH)
            .map_err(StorageError::from)?
            .map_or(0, |v| v.value())
            + 1;
        sequences
            .insert(OPEN_EPOCH, epoch)
            .map_err(StorageError::from)?;
        (epoch, genesis)
    };
    tx.commit().map_err(StorageError::from)?;
    Ok(opened)
}

fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
    for table in [
        JOBS,
        JOB_INPUTS,
        JOB_OUTPUTS,
        EXECUTORS,
        PROJECTS,
        PROFILES,
        AGENTS,
        LLM_PROVIDERS,
        METRICS,
        AVAILABILITY_STATS,
        ATTACHMENTS,
    ] {
        tx.open_table(table)?;
    }
    for table in [
        HAS_INPUT,
        LATEST_OUTPUT,
        CHILD_OF,
        ASSIGNED_TO,
        PREVIOUS,
        REPLACES,
        RETRY_OF,
        USES_PROVIDER,
        APPLIES_PROFILE,
        RUNS_LLM_PROFILE,
        MEASURES,
        OBSERVES,
        LOCAL_EXECUTIONS,
    ] {
        tx.open_table(table)?;
    }
    for table in [
        INPUT_JOBS,
        CHILDREN,
        RETRIES,
        IN_PROJECT,
        PROJECT_JOBS,
        USES_PROFILE,
        PROFILE_AGENTS,
        PROVIDER_PROFILES,
        TARGETS_LLM,
        AGENT_PROFILE_JOBS,
        LLM_PROFILE_JOBS,
        JOB_METRICS,
        JOB_AVAILABILITY,
        ATTACHMENT_INPUTS,
    ] {
        tx.open_table(table)?;
    }
    for table in [METRICS_BY_SCOPE, AVAILABILITY_BY_SCOPE] {
        tx.open_table(table)?;
    }
    tx.open_table(AVAILABILITY_SAMPLES)?;
    tx.open_table(AVAILABILITY_MINUTES)?;
    tx.open_table(AGGREGATION_PROGRESS)?;
    tx.open_table(TOKEN_REQUESTS)?;
    tx.open_table(TOKEN_MINUTES)?;
    tx.open_table(TOOL_EVIDENCE)?;
    tx.open_table(TOOL_CALL_IDS)?;
    tx.open_table(TRANSPORT_EXECUTORS)?;
    tx.open_table(HAS_OUTPUT)?;
    tx.open_table(HAS_ATTACHMENT)?;
    tx.open_table(DEPENDS_ON)?;
    tx.open_table(OPERATIONS)?;
    tx.open_table(SEQUENCES)?;
    Ok(())
}

fn allocate(tx: &WriteTransaction, sequence: Sequence) -> Result<u64, StorageError> {
    let mut table = tx.open_table(SEQUENCES)?;
    let next = table.get(sequence.key())?.map_or(0, |v| v.value()) + 1;
    table.insert(sequence.key(), next)?;
    Ok(next)
}

/// Creates the Genesis Job with its frozen input and its self-parent root
/// marker, and records its identity. Runs only for a new file.
fn create_genesis(tx: &WriteTransaction) -> Result<JobId, StorageError> {
    let now = Timestamp::now();
    let input = JobInputId::new(allocate(tx, Sequence::JobInput)?);
    let genesis = JobId::new(allocate(tx, Sequence::Job)?);
    let input_row = JobInputRow {
        id: input,
        submission_id: None,
        objective: None,
        max_depth: None,
        max_children_per_job: None,
        max_total_descendants: None,
        submitted_at: Some(now),
        created_at: now,
        updated_at: now,
        configuration: None,
    };
    let job_row = JobRow {
        id: genesis,
        name: Some("Genesis".into()),
        execution_mode: ExecutionMode::Continuous,
        eligible_at: None,
        started_at: None,
        paused_at: None,
        sterilized_at: None,
        completed_at: None,
        failed_at: None,
        cancelled_at: None,
        unknown_at: None,
        stop_confirmed_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        scheduled_at: None,
        created_at: now,
        updated_at: now,
    };
    tx.open_table(JOB_INPUTS)?
        .insert(input.get(), input_row.encode().as_slice())?;
    tx.open_table(JOBS)?
        .insert(genesis.get(), job_row.encode().as_slice())?;
    tx.open_table(HAS_INPUT)?
        .insert(genesis.get(), input.get())?;
    tx.open_table(INPUT_JOBS)?
        .insert((input.get(), genesis.get()), ())?;
    tx.open_table(CHILD_OF)?
        .insert(genesis.get(), genesis.get())?;
    tx.open_table(META)?
        .insert(GENESIS_KEY, genesis.get().to_le_bytes().as_slice())?;
    Ok(genesis)
}

fn genesis_id(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<JobId, StorageError> {
    let bytes = meta
        .get(GENESIS_KEY)?
        .ok_or_else(|| StorageError::corrupted("the database records no Genesis Job"))?;
    <[u8; 8]>::try_from(bytes.value())
        .map(|id| JobId::new(u64::from_le_bytes(id)))
        .map_err(|_| StorageError::corrupted("the recorded Genesis Job identity is malformed"))
}

/// The recorded Genesis Job must exist and carry its root marker.
fn verify_genesis(tx: &WriteTransaction) -> Result<JobId, StorageError> {
    let genesis = genesis_id(&tx.open_table(META)?)?;
    if tx.open_table(JOBS)?.get(genesis.get())?.is_none() {
        return Err(StorageError::corrupted(format!(
            "the Genesis Job {genesis} is missing"
        )));
    }
    if tx
        .open_table(CHILD_OF)?
        .get(genesis.get())?
        .map(|v| v.value())
        != Some(genesis.get())
    {
        return Err(StorageError::corrupted(format!(
            "the Genesis Job {genesis} lost its root marker"
        )));
    }
    Ok(genesis)
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
        Self {
            subject,
            created: None,
        }
    }

    pub(crate) fn created(subject: RecordRef, created: RecordRef) -> Self {
        Self {
            subject,
            created: Some(created),
        }
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

/// How a batch's commit becomes durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Commit {
    /// The commit is durable when it returns.
    Durable,
    /// The commit is visible to new read transactions at once but is durable
    /// only after a later [`persist_staged`] succeeds. Until then a crash or
    /// a failed [`persist_staged`] can lose it.
    Staged,
}

/// Runs one command as one batch and commits it durably with its operation
/// marker only if no operation in it failed.
#[cfg(test)]
pub(crate) fn run_batch<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
) -> BatchOutcome<T> {
    run_batch_with(database, operation, kind, Commit::Durable, body)
}

/// Like [`run_batch`], with the commit's durability chosen. A
/// [`Commit::Staged`] success is not durable yet, and the caller must keep
/// readers away from it until [`persist_staged`] has succeeded.
pub(crate) fn run_batch_with<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    commit: Commit,
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
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        return finish_abort(tx, Failure::Storage(error.into()));
    }
    let mut batch = Batch {
        tx,
        failed: None,
        now: Timestamp::now(),
    };
    let body_result = body(&mut batch);
    // A failure the body swallowed still prevents the commit.
    let value = match (body_result, batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
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
    commit_transaction(batch.tx, value)
}

/// Like [`run_batch_with`], but the body may decline: `Ok(None)` aborts the
/// transaction, so nothing is committed and no operation marker is written.
/// Work uses it when it finds, after reading, that it must not apply.
pub(crate) fn run_batch_declinable<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<Option<(T, MarkerSubjects)>, BatchError>,
) -> BatchOutcome<Option<T>> {
    let mut tx = match database.begin_write() {
        Ok(tx) => tx,
        Err(error) => {
            let error = StorageError::from(error);
            return BatchOutcome {
                result: Err(BatchFailure::NotCommitted(error.clone())),
                storage_failure: Some(error),
            };
        }
    };
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        return finish_abort(tx, Failure::Storage(error.into()));
    }
    let mut batch = Batch {
        tx,
        failed: None,
        now: Timestamp::now(),
    };
    let body_result = body(&mut batch);
    let (value, subjects) = match (body_result, batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
        (Ok(None), None) => {
            return match batch.tx.abort() {
                Ok(()) => BatchOutcome {
                    result: Ok(None),
                    storage_failure: None,
                },
                Err(error) => {
                    let error = StorageError::from(error);
                    BatchOutcome {
                        result: Err(BatchFailure::NotCommitted(error.clone())),
                        storage_failure: Some(error),
                    }
                }
            };
        }
        (Ok(Some(value)), None) => value,
    };
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
    let outcome = commit_transaction(batch.tx, value);
    BatchOutcome {
        result: outcome.result.map(Some),
        storage_failure: outcome.storage_failure,
    }
}

/// Commits a batch's transaction and reports how the commit ended.
fn commit_transaction<T>(tx: WriteTransaction, value: T) -> BatchOutcome<T> {
    match tx.commit() {
        Ok(()) => BatchOutcome {
            result: Ok(value),
            storage_failure: None,
        },
        // redb documents a poisoned commit as rolled back.
        Err(CommitError::TransactionPoisoned) => {
            let error = StorageError::from(CommitError::TransactionPoisoned);
            BatchOutcome {
                result: Err(BatchFailure::NotCommitted(error.clone())),
                storage_failure: Some(error),
            }
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

/// Makes every earlier [`Commit::Staged`] commit durable with an empty
/// `Durability::Immediate` transaction: redb writes the unpersisted pages of
/// earlier non-durable commits and publishes their roots when a durable
/// commit follows. An error leaves those commits' durability unknown, as for
/// any failed commit.
pub(crate) fn persist_staged(database: &Database) -> Result<(), StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    tx.commit()?;
    Ok(())
}

// ---- maintenance ---------------------------------------------------------------

/// The bytes of the file that redb's live pages occupy, read in a write
/// transaction that changes nothing. The file's length minus this is an upper
/// bound on what compaction can return, not a promise: it counts free pages
/// anywhere in the file, and `fragmented_bytes` is deliberately not part of
/// it because compaction does not recover space inside pages.
pub(crate) fn allocated_bytes(database: &Database) -> Result<u64, StorageError> {
    let tx = database.begin_write()?;
    let stats = tx.stats()?;
    tx.abort()?;
    let page_size = u64::try_from(stats.page_size()).unwrap_or(u64::MAX);
    Ok(stats.allocated_pages().saturating_mul(page_size))
}

/// Why compaction did not run to completion.
#[derive(Debug)]
pub(crate) enum CompactFailure {
    /// A transaction was still open on the handle. Nothing was changed.
    TransactionInProgress,
    /// A savepoint exists. Nothing was changed.
    SavepointExists,
    /// The storage engine failed part way. Every compaction step is its own
    /// atomic commit, so the file is consistent, but the handle is no longer
    /// usable and must be reopened.
    Storage(StorageError),
}

/// Compacts the file. This is redb's iterative, uninterruptible
/// `Database::compact`: it needs exclusive access to the handle and runs for
/// as long as it finds pages to move. Every step commits durably. The result
/// is `true` when redb moved pages and `false` when none could be moved; the
/// file may still have shrunk on `false`, because redb frees pending pages
/// before it checks for progress.
pub(crate) fn compact(database: &mut Database) -> Result<bool, CompactFailure> {
    match database.compact() {
        Ok(moved) => Ok(moved),
        Err(redb::CompactionError::TransactionInProgress) => {
            Err(CompactFailure::TransactionInProgress)
        }
        Err(
            redb::CompactionError::PersistentSavepointExists
            | redb::CompactionError::EphemeralSavepointExists,
        ) => Err(CompactFailure::SavepointExists),
        Err(redb::CompactionError::Storage(error)) => Err(CompactFailure::Storage(error.into())),
        Err(error) => Err(CompactFailure::Storage(redb::Error::from(error).into())),
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
    /// The wall-clock time read once when this batch's write transaction
    /// began. Every timestamp the batch writes uses it.
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

    fn corrupted<T>(&mut self, detail: String) -> Result<T, BatchError> {
        self.fail(Failure::Storage(StorageError::corrupted(detail)))
    }

    /// Runs one storage step; a failed batch refuses further steps.
    fn step<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>,
    ) -> Result<T, BatchError> {
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

    /// Refuses the batch unless `record` exists in `table`.
    fn require_exists(
        &mut self,
        table: Records,
        record: RecordRef,
        id: u64,
    ) -> Result<(), BatchError> {
        match self.get_bytes(table, id)? {
            Some(_) => Ok(()),
            None => self.reject(Violation::NotFound(record)),
        }
    }

    fn insert_new(
        &mut self,
        table: Records,
        id: u64,
        bytes: Vec<u8>,
        record: RecordRef,
    ) -> Result<(), BatchError> {
        let existed = self.step(|tx| {
            let mut t = tx.open_table(table)?;
            if t.get(id)?.is_some() {
                return Ok(true);
            }
            t.insert(id, bytes.as_slice())?;
            Ok(false)
        })?;
        if existed {
            self.reject(Violation::DuplicateId(record))
        } else {
            Ok(())
        }
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
        self.step(|tx| allocate(tx, sequence))
    }

    /// The instance's Genesis Job, as recorded in this transaction.
    pub(crate) fn genesis(&mut self) -> Result<JobId, BatchError> {
        self.step(|tx| genesis_id(&tx.open_table(META)?))
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
        self.insert_new(
            JOB_INPUTS,
            row.id.get(),
            row.encode(),
            RecordRef::JobInput(row.id),
        )
    }

    pub(crate) fn put_input(&mut self, row: &JobInputRow) -> Result<(), BatchError> {
        self.overwrite(JOB_INPUTS, row.id.get(), row.encode())
    }

    pub(crate) fn require_attachment(
        &mut self,
        id: AttachmentId,
    ) -> Result<AttachmentRecord, BatchError> {
        match self.get_bytes(ATTACHMENTS, id.get())? {
            Some(bytes) => {
                let record = decode_attachment(&bytes);
                self.decoded(record)
            }
            None => self.reject(Violation::NotFound(RecordRef::Attachment(id))),
        }
    }

    pub(crate) fn insert_attachment(
        &mut self,
        record: &AttachmentRecord,
    ) -> Result<(), BatchError> {
        self.insert_new(
            ATTACHMENTS,
            record.id.get(),
            encode_attachment(record),
            RecordRef::Attachment(record.id),
        )
    }

    /// Replaces the input's HAS_ATTACHMENT entries, and their incoming
    /// index, with `attachments` in order. Each must exist.
    pub(crate) fn set_input_attachments(
        &mut self,
        input: JobInputId,
        attachments: &[AttachmentId],
    ) -> Result<(), BatchError> {
        self.require_input(input)?;
        for attachment in attachments {
            self.require_attachment(*attachment)?;
        }
        let Ok(count) = u32::try_from(attachments.len()) else {
            return self.reject(Violation::ValueOutOfRange("attachments"));
        };
        let duplicate = self.step(|tx| {
            let input = input.get();
            let mut forward = tx.open_table(HAS_ATTACHMENT)?;
            let mut incoming = tx.open_table(ATTACHMENT_INPUTS)?;
            let mut previous = Vec::new();
            for entry in forward.range((input, 0)..=(input, u32::MAX))? {
                let (key, attachment) = entry?;
                previous.push((key.value().1, attachment.value()));
            }
            for (position, attachment) in previous {
                forward.remove((input, position))?;
                incoming.remove((attachment, input))?;
            }
            for (position, attachment) in (0..count).zip(attachments) {
                forward.insert((input, position), attachment.get())?;
                if incoming.insert((attachment.get(), input), ())?.is_some() {
                    return Ok(Some(*attachment));
                }
            }
            Ok(None)
        })?;
        match duplicate {
            Some(attachment) => self.reject(Violation::DuplicateReference(RecordRef::Attachment(
                attachment,
            ))),
            None => Ok(()),
        }
    }

    /// Inserts the output, its HAS_OUTPUT entry and moves LATEST_OUTPUT to it.
    pub(crate) fn insert_output(&mut self, record: &JobOutputRecord) -> Result<(), BatchError> {
        self.insert_new(
            JOB_OUTPUTS,
            record.id.get(),
            encode_output(record),
            RecordRef::JobOutput(record.id),
        )?;
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
        if taken {
            self.reject(Violation::DuplicateId(RecordRef::JobOutput(record.id)))
        } else {
            Ok(())
        }
    }

    pub(crate) fn last_output_seq(&mut self, job: JobId) -> Result<u64, BatchError> {
        self.step(|tx| last_seq(&tx.open_table(HAS_OUTPUT)?, job.get()))
    }

    fn require_executor_row(&mut self, id: ExecutorId) -> Result<ExecutorRow, BatchError> {
        let Some(bytes) = self.get_bytes(EXECUTORS, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::Executor(id)));
        };
        let decoded = ExecutorRow::decode(&bytes);
        self.decoded(decoded)
    }

    pub(crate) fn require_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        self.require_exists(EXECUTORS, RecordRef::Executor(id), id.get())
    }

    /// Refuses the batch unless the Executor exists and, when the Job runs
    /// an LLM configuration, is an Llm Executor whose adapter supports the
    /// transport that configuration froze. The Executor never changes which
    /// Provider, model or parameters the Job runs.
    pub(crate) fn require_executor_for(
        &mut self,
        executor: ExecutorId,
        frozen: Option<&FrozenLlmConfig>,
    ) -> Result<(), BatchError> {
        let row = self.require_executor_row(executor)?;
        let Some(frozen) = frozen else {
            return Ok(());
        };
        if row.executor_type != ExecutorType::Llm {
            return self.reject(Violation::IncompatibleExecutorKind {
                executor,
                found: row.executor_type,
            });
        }
        let transport = frozen.provider.transport;
        if !row.llm_transports.contains(&transport) {
            return self.reject(Violation::ExecutorLacksTransport {
                executor,
                transport,
            });
        }
        Ok(())
    }

    pub(crate) fn require_core_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        let row = self.require_executor_row(id)?;
        if row.name != "__CORE__"
            || row.executor_type != ExecutorType::System
            || row.concurrency_limit == 0
        {
            return self.reject(Violation::NotCoreExecutor(id));
        }
        Ok(())
    }

    /// Inserts the Executor and indexes its LLM transports. Inserting refuses
    /// an existing Executor, so its transports never change after registration.
    pub(crate) fn insert_executor(&mut self, row: &ExecutorRow) -> Result<(), BatchError> {
        if !row.llm_transports.is_empty() && row.executor_type != ExecutorType::Llm {
            return self.reject(Violation::IncompatibleExecutorKind {
                executor: row.id,
                found: row.executor_type,
            });
        }
        self.insert_new(
            EXECUTORS,
            row.id.get(),
            row.encode(),
            RecordRef::Executor(row.id),
        )?;
        let executor = row.id.get();
        let transports: Vec<u8> = row
            .llm_transports
            .iter()
            .map(|transport| transport.code())
            .collect();
        self.step(|tx| {
            let mut index = tx.open_table(TRANSPORT_EXECUTORS)?;
            for transport in transports {
                index.insert((transport, executor), ())?;
            }
            Ok(())
        })
    }

    pub(crate) fn require_project(&mut self, id: ProjectId) -> Result<(), BatchError> {
        self.require_exists(PROJECTS, RecordRef::Project(id), id.get())
    }

    pub(crate) fn insert_project(&mut self, record: &ProjectRecord) -> Result<(), BatchError> {
        self.insert_new(
            PROJECTS,
            record.id.get(),
            encode_project(record),
            RecordRef::Project(record.id),
        )
    }

    pub(crate) fn require_llm_provider(&mut self, id: LlmProviderId) -> Result<(), BatchError> {
        self.require_exists(LLM_PROVIDERS, RecordRef::LlmProvider(id), id.get())
    }

    pub(crate) fn insert_llm_provider(
        &mut self,
        record: &LlmProviderRecord,
    ) -> Result<(), BatchError> {
        self.insert_new(
            LLM_PROVIDERS,
            record.id.get(),
            encode_llm_provider(record),
            RecordRef::LlmProvider(record.id),
        )
    }

    /// Refuses the batch unless the Profile exists and is of `kind`.
    pub(crate) fn require_profile_row(&mut self, id: ProfileId) -> Result<ProfileRow, BatchError> {
        let Some(bytes) = self.get_bytes(PROFILES, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::Profile(id)));
        };
        let decoded = ProfileRow::decode(&bytes);
        self.decoded(decoded)
    }

    pub(crate) fn require_profile_kind(
        &mut self,
        id: ProfileId,
        kind: ProfileKind,
    ) -> Result<(), BatchError> {
        let found = self.require_profile_row(id)?.config.kind();
        if found != kind {
            return self.reject(
                WrongProfileKind {
                    profile: id,
                    expected: kind,
                    found,
                }
                .into(),
            );
        }
        Ok(())
    }

    /// Inserts the Profile with the references that define it. Inserting
    /// refuses an existing Profile; only [`Self::replace_profile`] changes
    /// one, payload and references together.
    pub(crate) fn insert_profile(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        self.insert_new(
            PROFILES,
            row.id.get(),
            row.encode(),
            RecordRef::Profile(row.id),
        )?;
        self.write_profile_links(row, links)
    }

    /// Replaces a stored Profile's payload and the references that define it,
    /// with their incoming indexes. The caller has checked that no unfinished
    /// Job holds the Profile.
    pub(crate) fn replace_profile(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        let existing = self.require_profile_row(row.id)?;
        if existing.config.kind() != row.config.kind() {
            return self.reject(
                WrongProfileKind {
                    profile: row.id,
                    expected: existing.config.kind(),
                    found: row.config.kind(),
                }
                .into(),
            );
        }
        let profile = row.id.get();
        self.step(|tx| {
            let mut targets = tx.open_table(TARGETS_LLM)?;
            let old_targets = pair_targets(&targets, profile, None, usize::MAX)?;
            for target in old_targets {
                targets.remove((profile, target))?;
            }
            drop(targets);
            let old_provider = tx
                .open_table(USES_PROVIDER)?
                .remove(profile)?
                .map(|v| v.value());
            if let Some(provider) = old_provider {
                tx.open_table(PROVIDER_PROFILES)?
                    .remove((provider, profile))?;
            }
            Ok(())
        })?;
        self.overwrite(PROFILES, profile, row.encode())?;
        self.write_profile_links(row, links)
    }

    fn write_profile_links(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        match (links, row.config.kind()) {
            (ProfileLinks::Llm { provider }, ProfileKind::Llm) => {
                self.link_profile_provider(row.id, *provider)
            }
            (ProfileLinks::Agent { llm_targets }, ProfileKind::Agent) => {
                for target in llm_targets {
                    self.require_profile_kind(*target, ProfileKind::Llm)?;
                    if !self.insert_pair(TARGETS_LLM, row.id.get(), target.get())? {
                        return self
                            .reject(Violation::DuplicateReference(RecordRef::Profile(*target)));
                    }
                }
                Ok(())
            }
            (_, kind) => self.corrupted(format!(
                "{} is an {kind} Profile given the other kind's references",
                row.id
            )),
        }
    }

    /// Refuses the batch unless the Profiles have the right kinds and, when
    /// an Agent Profile is named, the LLM Profile is one of its configured
    /// targets.
    pub(crate) fn require_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<(), BatchError> {
        let ExecutionConfiguration {
            agent_profile,
            llm_profile,
        } = configuration;
        if let Some(agent_profile) = agent_profile {
            self.require_profile_kind(agent_profile, ProfileKind::Agent)?;
        }
        self.require_profile_kind(llm_profile, ProfileKind::Llm)?;
        if let Some(agent_profile) = agent_profile
            && !self.has_pair(TARGETS_LLM, agent_profile.get(), llm_profile.get())?
        {
            return self.reject(Violation::NotAConfiguredTarget {
                agent_profile,
                llm_profile,
            });
        }
        Ok(())
    }

    pub(crate) fn insert_agent(&mut self, row: &AgentRow) -> Result<(), BatchError> {
        self.insert_new(AGENTS, row.id.get(), row.encode(), RecordRef::Agent(row.id))
    }

    /// Inserts the Metrics with its MEASURES edge, the edge's incoming index
    /// and its scope index entries.
    pub(crate) fn insert_metrics(
        &mut self,
        row: &MetricsRow,
        job: JobId,
    ) -> Result<(), BatchError> {
        self.insert_new(
            METRICS,
            row.id.get(),
            row.encode(),
            RecordRef::Metrics(row.id),
        )?;
        self.insert_observation(
            MEASURES,
            JOB_METRICS,
            METRICS_BY_SCOPE,
            row.id.get(),
            job,
            row.observed_at,
        )?;
        Ok(())
    }

    /// The Token eligibility the committed Metrics of one request of the
    /// Job state: that of its first annotated record, which every later
    /// annotated record of the request shares. Reads only the Job's own
    /// Metrics, in ID order, up to that record.
    pub(crate) fn request_token_eligibility(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<RequestEligibility>, BatchError> {
        self.step(|tx| {
            let index = tx.open_table(JOB_METRICS)?;
            let records = tx.open_table(METRICS)?;
            for entry in index.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                let metrics = entry?.0.value().1;
                let bytes = records.get(metrics)?.ok_or_else(|| {
                    StorageError::corrupted(format!(
                        "{job} is measured by missing metrics {metrics}"
                    ))
                })?;
                let row = MetricsRow::decode(bytes.value())?;
                if row.request == request
                    && let Some(token) = row.token
                {
                    return Ok(Some(token.eligibility));
                }
            }
            Ok(None)
        })
    }

    /// Whether the Job has a Metrics or AvailabilityStat record of the
    /// request: the observation that makes the request a recorded one.
    pub(crate) fn request_observed(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            for (index, records, decode) in [
                (
                    JOB_AVAILABILITY,
                    AVAILABILITY_STATS,
                    (|bytes| AvailabilityStatRow::decode(bytes).map(|row| row.request))
                        as fn(&[u8]) -> DecodeResult<RequestNumber>,
                ),
                (JOB_METRICS, METRICS, |bytes| {
                    MetricsRow::decode(bytes).map(|row| row.request)
                }),
            ] {
                let index = tx.open_table(index)?;
                let records = tx.open_table(records)?;
                for entry in index.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                    let record = entry?.0.value().1;
                    let bytes = records.get(record)?.ok_or_else(|| {
                        StorageError::corrupted(format!(
                            "{job} is linked to missing observation {record}"
                        ))
                    })?;
                    if decode(bytes.value())? == request {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
    }

    pub(crate) fn tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_EVIDENCE)?
                .get(tool_call_key(call))?
                .map(|bytes| decode_tool_evidence(call, bytes.value()))
                .transpose()
        })
    }

    /// Every tool evidence record of the Job, in call order.
    pub(crate) fn job_tool_evidence(
        &mut self,
        job: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| job_tool_evidence(&tx.open_table(TOOL_EVIDENCE)?, job))
    }

    /// Inserts a new record with its Provider id. Refuses a call that
    /// already has a record, and an id another record of the Job uses.
    pub(crate) fn insert_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        let call = record.call;
        let key = tool_call_key(call);
        let job = call.job.get();
        let id = record.tool_call_id.as_str();
        let taken = self.step(|tx| {
            if tx.open_table(TOOL_EVIDENCE)?.get(key)?.is_some() {
                return Ok(Some(None));
            }
            let mut ids = tx.open_table(TOOL_CALL_IDS)?;
            if let Some(existing) = ids.get((job, id))? {
                return Ok(Some(Some(existing.value())));
            }
            ids.insert((job, id), (key.1, key.2))?;
            tx.open_table(TOOL_EVIDENCE)?
                .insert(key, encode_tool_evidence(record).as_slice())?;
            Ok(None)
        })?;
        match taken {
            None => Ok(()),
            Some(None) => self.reject(Violation::DuplicateToolCall(call)),
            Some(Some((request, index))) => {
                let Some(request) = RequestNumber::new(request) else {
                    return self.corrupted(format!("{call}: an indexed request number is 0"));
                };
                self.reject(Violation::DuplicateToolCallId {
                    call,
                    existing: ToolCallRef {
                        job: call.job,
                        request,
                        index,
                    },
                })
            }
        }
    }

    /// Replaces an existing record, which keeps its call and Provider id.
    pub(crate) fn put_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        let key = tool_call_key(record.call);
        let replaced = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_EVIDENCE)?
                .insert(key, encode_tool_evidence(record).as_slice())?
                .is_some())
        })?;
        if replaced {
            Ok(())
        } else {
            self.corrupted(format!("{} was replaced without a record", record.call))
        }
    }

    /// Inserts the AvailabilityStat with its OBSERVES edge, the edge's
    /// incoming index and its scope index entries. The caller has checked
    /// that the Job ran an LLM.
    pub(crate) fn insert_availability_stat(
        &mut self,
        row: &AvailabilityStatRow,
        job: JobId,
    ) -> Result<(), BatchError> {
        self.insert_new(
            AVAILABILITY_STATS,
            row.id.get(),
            row.encode(),
            RecordRef::AvailabilityStat(row.id),
        )?;
        if !self.insert_observation(
            OBSERVES,
            JOB_AVAILABILITY,
            AVAILABILITY_BY_SCOPE,
            row.id.get(),
            job,
            row.observed_at,
        )? {
            return self.corrupted(format!(
                "availability_stat {} observes {job}, which ran no LLM",
                row.id.get()
            ));
        }
        Ok(())
    }

    /// Links a just-inserted observation record to its Job and writes its
    /// scope index entries from the target the Job froze. The target is
    /// never taken from the caller. Returns whether the Job ran an LLM.
    fn insert_observation(
        &mut self,
        forward: Link,
        incoming: PairSet,
        by_scope: ScopeIndex,
        record: u64,
        job: JobId,
        observed_at: Timestamp,
    ) -> Result<bool, BatchError> {
        self.require_job(job)?;
        let target = match (self.job_llm_profile(job)?, self.job_frozen_llm(job)?) {
            (Some(llm_profile), Some(frozen)) => Some(LlmTarget {
                llm_profile,
                frozen,
            }),
            _ => None,
        };
        if !self.insert_indexed_link(forward, incoming, record, job.get())? {
            return self.corrupted(format!("observation record {record} is already linked"));
        }
        let keys: Vec<Vec<u8>> = Scope::of(target.as_ref()).iter().map(Scope::key).collect();
        let at = observed_at.as_unix_micros();
        self.step(|tx| {
            let mut index = tx.open_table(by_scope)?;
            for key in &keys {
                index.insert((key.as_slice(), at, record), ())?;
            }
            Ok(())
        })?;
        Ok(target.is_some())
    }

    // -- relationships --

    fn has_pair(&mut self, table: PairSet, from: u64, to: u64) -> Result<bool, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get((from, to))?.is_some()))
    }

    /// Inserts `(from, to)` into a pair set without an incoming index.
    /// Returns false, writing nothing, when the pair exists.
    fn insert_pair(&mut self, table: PairSet, from: u64, to: u64) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(table)?;
            if table.get((from, to))?.is_some() {
                return Ok(false);
            }
            table.insert((from, to), ())?;
            Ok(true)
        })
    }

    /// Inserts `(from, to)` into a pair set and `(to, from)` into its
    /// incoming index. Returns false, writing nothing, when the pair exists.
    fn insert_indexed_pair(
        &mut self,
        forward: PairSet,
        incoming: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(forward)?;
            if table.get((from, to))?.is_some() {
                return Ok(false);
            }
            table.insert((from, to), ())?;
            drop(table);
            tx.open_table(incoming)?.insert((to, from), ())?;
            Ok(true)
        })
    }

    /// Sets `from -> to` in a single-valued link and `(to, from)` in its
    /// incoming index. Returns false, writing nothing, when `from` is linked.
    fn insert_indexed_link(
        &mut self,
        forward: Link,
        incoming: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(forward)?;
            if table.get(from)?.is_some() {
                return Ok(false);
            }
            table.insert(from, to)?;
            drop(table);
            tx.open_table(incoming)?.insert((to, from), ())?;
            Ok(true)
        })
    }

    fn link_target(&mut self, table: Link, from: u64) -> Result<Option<u64>, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get(from)?.map(|v| v.value())))
    }

    /// Whether any Job has been submitted with the input.
    pub(crate) fn input_has_jobs(&mut self, input: JobInputId) -> Result<bool, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(INPUT_JOBS)?
                .range((input.get(), 0)..=(input.get(), u64::MAX))?
                .next()
                .is_some())
        })
    }

    /// The input a stored Job was submitted with.
    pub(crate) fn input_of(&mut self, job: JobId) -> Result<JobInputId, BatchError> {
        match self.link_target(HAS_INPUT, job.get())? {
            Some(input) => Ok(JobInputId::new(input)),
            None => self.corrupted(format!("{job} has no input")),
        }
    }

    /// HAS_INPUT: one input per Job; an input may serve several Jobs.
    pub(crate) fn link_input(&mut self, job: JobId, input: JobInputId) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_input(input)?;
        if self.insert_indexed_link(HAS_INPUT, INPUT_JOBS, job.get(), input.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Job(job)))
        }
    }

    /// The parent of a stored Job; the Genesis Job is its own.
    pub(crate) fn parent_of(&mut self, job: JobId) -> Result<JobId, BatchError> {
        match self.link_target(CHILD_OF, job.get())? {
            Some(parent) => Ok(JobId::new(parent)),
            None => self.corrupted(format!("{job} has no parent")),
        }
    }

    /// CHILD_OF: exactly one existing parent per Job, fixed once written,
    /// never the child itself, and never a descendant of the child. The
    /// parent's ancestor chain must reach the Genesis Job.
    pub(crate) fn link_child(&mut self, child: JobId, parent: JobId) -> Result<(), BatchError> {
        if child == parent {
            return self.reject(Violation::SelfParent(child));
        }
        self.require_job(child)?;
        self.require_job(parent)?;
        let genesis = self.genesis()?;
        let mut ancestor = parent;
        let mut visited = HashSet::new();
        loop {
            if ancestor == child {
                return self.reject(Violation::ParentCycle { child, parent });
            }
            if ancestor == genesis {
                break;
            }
            if !visited.insert(ancestor) {
                return self.corrupted(format!(
                    "the ancestors of {parent} loop without reaching the Genesis Job"
                ));
            }
            ancestor = self.parent_of(ancestor)?;
        }
        if self.insert_indexed_link(CHILD_OF, CHILDREN, child.get(), parent.get())? {
            Ok(())
        } else {
            self.reject(Violation::ParentFixed(child))
        }
    }

    /// Ordinary children only: the Genesis root marker is never listed.
    pub(crate) fn children(&mut self, parent: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            Ok(
                pair_targets(&tx.open_table(CHILDREN)?, parent.get(), None, usize::MAX)?
                    .into_iter()
                    .map(JobId::new)
                    .collect(),
            )
        })
    }

    /// The Job's explicit prerequisites with their conditions, in Job order.
    pub(crate) fn dependencies(
        &mut self,
        job: JobId,
    ) -> Result<Vec<(JobId, DependencyCondition)>, BatchError> {
        self.step(|tx| {
            let mut dependencies = Vec::new();
            for entry in tx
                .open_table(DEPENDS_ON)?
                .range((job.get(), 0)..=(job.get(), u64::MAX))?
            {
                let (key, condition) = entry?;
                let condition = DependencyCondition::from_code(condition.value())
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
                dependencies.push((JobId::new(key.value().1), condition));
            }
            Ok(dependencies)
        })
    }

    /// The Jobs that directly retry `original`, in creation order.
    pub(crate) fn retries(&mut self, original: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            Ok(
                pair_targets(&tx.open_table(RETRIES)?, original.get(), None, usize::MAX)?
                    .into_iter()
                    .map(JobId::new)
                    .collect(),
            )
        })
    }

    pub(crate) fn link_dependency(
        &mut self,
        job: JobId,
        prerequisite: JobId,
        condition: DependencyCondition,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_job(prerequisite)?;
        let taken = self.step(|tx| {
            let mut t = tx.open_table(DEPENDS_ON)?;
            if t.get((job.get(), prerequisite.get()))?.is_some() {
                return Ok(true);
            }
            t.insert((job.get(), prerequisite.get()), condition.code())?;
            Ok(false)
        })?;
        if taken {
            self.reject(Violation::DuplicateDependency(prerequisite))
        } else {
            Ok(())
        }
    }

    /// LOCAL_EXECUTIONS: records that the opening of `epoch` started `job`
    /// as its own local execution.
    pub(crate) fn claim_local_execution(
        &mut self,
        job: JobId,
        epoch: u64,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(LOCAL_EXECUTIONS)?.insert(job.get(), epoch)?;
            Ok(())
        })
    }

    /// The epoch of the opening that started `job` as its own local
    /// execution, while the Job is unsettled.
    pub(crate) fn local_execution(&mut self, job: JobId) -> Result<Option<u64>, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(LOCAL_EXECUTIONS)?
                .get(job.get())?
                .map(|v| v.value()))
        })
    }

    /// Removes the Job's local execution claim, if it has one.
    pub(crate) fn release_local_execution(&mut self, job: JobId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(LOCAL_EXECUTIONS)?.remove(job.get())?;
            Ok(())
        })
    }

    /// ASSIGNED_TO: one per Job; replacing it is the caller's decision.
    pub(crate) fn set_assigned_executor(
        &mut self,
        job: JobId,
        executor: ExecutorId,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_executor(executor)?;
        self.step(|tx| {
            tx.open_table(ASSIGNED_TO)?
                .insert(job.get(), executor.get())?;
            Ok(())
        })
    }

    /// PREVIOUS and REPLACES are fixed at creation and are not waiting edges.
    pub(crate) fn link_lineage(
        &mut self,
        job: JobId,
        previous: Option<JobId>,
        replaces: Option<JobId>,
    ) -> Result<(), BatchError> {
        for (table, target) in [(PREVIOUS, previous), (REPLACES, replaces)] {
            let Some(target) = target else { continue };
            self.require_job(target)?;
            let written = self.step(|tx| {
                let mut t = tx.open_table(table)?;
                if t.get(job.get())?.is_some() {
                    return Ok(false);
                }
                t.insert(job.get(), target.get())?;
                Ok(true)
            })?;
            if !written {
                return self.reject(Violation::DuplicateId(RecordRef::Job(job)));
            }
        }
        Ok(())
    }

    /// RETRY_OF: one original per retry Job, fixed at creation.
    /// `job` and every Job it retries through RETRY_OF, nearest first. A
    /// RETRY_OF edge to a missing Job, or one that leads back into the chain,
    /// is corruption: the chain is never cut short.
    pub(crate) fn retry_lineage(&mut self, job: JobId) -> Result<Vec<JobId>, BatchError> {
        let mut lineage = vec![job];
        let mut current = job;
        while let Some(original) = self.link_target(RETRY_OF, current.get())? {
            let original = JobId::new(original);
            if lineage.contains(&original) {
                return self.corrupted(format!("the retry lineage of {job} returns to {original}"));
            }
            if self.job(original)?.is_none() {
                return self.corrupted(format!("{current} retries the missing {original}"));
            }
            lineage.push(original);
            current = original;
        }
        Ok(lineage)
    }

    pub(crate) fn link_retry(&mut self, retry: JobId, original: JobId) -> Result<(), BatchError> {
        self.require_job(retry)?;
        self.require_job(original)?;
        if self.insert_indexed_link(RETRY_OF, RETRIES, retry.get(), original.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Job(retry)))
        }
    }

    /// IN_PROJECT: any number of Projects per Job and Jobs per Project.
    pub(crate) fn link_project(
        &mut self,
        job: JobId,
        project: ProjectId,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_project(project)?;
        if self.insert_indexed_pair(IN_PROJECT, PROJECT_JOBS, job.get(), project.get())? {
            Ok(())
        } else {
            self.reject(Violation::AlreadyInProject { job, project })
        }
    }

    /// USES_PROFILE: an Agent and one of its Agent Profiles.
    pub(crate) fn link_agent_profile(
        &mut self,
        agent: AgentId,
        profile: ProfileId,
    ) -> Result<(), BatchError> {
        self.require_exists(AGENTS, RecordRef::Agent(agent), agent.get())?;
        self.require_profile_kind(profile, ProfileKind::Agent)?;
        if self.insert_indexed_pair(USES_PROFILE, PROFILE_AGENTS, agent.get(), profile.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateReference(RecordRef::Profile(profile)))
        }
    }

    /// An LLM Profile's configuration and Provider as they are now.
    pub(crate) fn freeze_llm(&mut self, profile: ProfileId) -> Result<FrozenLlmConfig, BatchError> {
        let row = self.require_profile_row(profile)?;
        let ProfileConfig::Llm(config) = row.config else {
            return self.reject(
                WrongProfileKind {
                    profile,
                    expected: ProfileKind::Llm,
                    found: ProfileKind::Agent,
                }
                .into(),
            );
        };
        let Some(provider) = self.link_target(USES_PROVIDER, profile.get())? else {
            return self.corrupted(format!("{profile} has no Provider"));
        };
        let Some(bytes) = self.get_bytes(LLM_PROVIDERS, provider)? else {
            return self.corrupted(format!("{profile} names missing Provider {provider}"));
        };
        let decoded = decode_llm_provider(&bytes);
        let provider = self.decoded(decoded)?;
        Ok(FrozenLlmConfig {
            config,
            provider: provider_snapshot(provider),
        })
    }

    /// Validates a submission's configuration and reads the values it will
    /// execute with from its Profiles as they are now.
    /// No Agent configuration is read, or made up, for work submitted
    /// without an Agent Profile.
    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenExecutionConfig, BatchError> {
        self.require_configuration(configuration)?;
        let agent = match configuration.agent_profile {
            Some(profile) => {
                let ProfileConfig::Agent(agent) = self.require_profile_row(profile)?.config else {
                    return self.corrupted(format!("{profile} changed kind"));
                };
                Some(agent)
            }
            None => None,
        };
        let llm = self.freeze_llm(configuration.llm_profile)?;
        Ok(FrozenExecutionConfig { agent, llm })
    }

    /// The LLM Profile a stored Job was submitted with, if any.
    pub(crate) fn job_llm_profile(&mut self, job: JobId) -> Result<Option<ProfileId>, BatchError> {
        Ok(self
            .link_target(RUNS_LLM_PROFILE, job.get())?
            .map(ProfileId::new))
    }

    /// The LLM configuration a Job's input froze for it, if the Job is configured.
    pub(crate) fn job_frozen_llm(
        &mut self,
        job: JobId,
    ) -> Result<Option<FrozenLlmConfig>, BatchError> {
        if self.job_llm_profile(job)?.is_none() {
            return Ok(None);
        }
        let input = self.input_of(job)?;
        match self.require_input(input)?.configuration {
            Some(frozen) => Ok(Some(frozen.llm)),
            None => self.corrupted(format!(
                "the configured {job} has no frozen configuration in {input}"
            )),
        }
    }

    /// The Jobs holding a Profile, see [`JobRow::holds_profiles`]. Returns at
    /// most `limit` in ID order, and whether there are more.
    pub(crate) fn profile_holders(
        &mut self,
        profile: ProfileId,
        limit: usize,
    ) -> Result<(Vec<JobId>, bool), BatchError> {
        let kind = self.require_profile_row(profile)?.config.kind();
        self.step(|tx| {
            profile_holders(
                &tx.open_table(profile_jobs_index(kind))?,
                &tx.open_table(JOBS)?,
                profile,
                limit,
            )
        })
    }

    /// RUNS_LLM_PROFILE and, when an Agent Profile is named, APPLIES_PROFILE:
    /// the configuration an execution child Job is submitted with. Refused
    /// for a Job that already has one, whatever the new Profiles are.
    pub(crate) fn link_configuration(
        &mut self,
        job: JobId,
        configuration: ExecutionConfiguration,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_configuration(configuration)?;
        let bound = self.link_target(APPLIES_PROFILE, job.get())?.is_some()
            || self.link_target(RUNS_LLM_PROFILE, job.get())?.is_some();
        if bound {
            return self.reject(Violation::ConfigurationFixed(job));
        }
        if let Some(agent_profile) = configuration.agent_profile {
            self.insert_indexed_link(
                APPLIES_PROFILE,
                AGENT_PROFILE_JOBS,
                job.get(),
                agent_profile.get(),
            )?;
        }
        self.insert_indexed_link(
            RUNS_LLM_PROFILE,
            LLM_PROFILE_JOBS,
            job.get(),
            configuration.llm_profile.get(),
        )?;
        Ok(())
    }

    /// USES_PROVIDER: one LLM Provider per LLM Profile, fixed at creation.
    fn link_profile_provider(
        &mut self,
        profile: ProfileId,
        provider: LlmProviderId,
    ) -> Result<(), BatchError> {
        self.require_profile_kind(profile, ProfileKind::Llm)?;
        self.require_llm_provider(provider)?;
        if self.insert_indexed_link(
            USES_PROVIDER,
            PROVIDER_PROFILES,
            profile.get(),
            provider.get(),
        )? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Profile(profile)))
        }
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
            return self.fail(Failure::Storage(StorageError::corrupted(format!(
                "{} already has a marker",
                marker.operation
            ))));
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

/// The index of the Jobs submitted with a Profile of `kind`.
fn profile_jobs_index(kind: ProfileKind) -> PairSet {
    match kind {
        ProfileKind::Agent => AGENT_PROFILE_JOBS,
        ProfileKind::Llm => LLM_PROFILE_JOBS,
    }
}

/// The lock on a Profile, derived from canonical Job facts on every call: a
/// Job submitted with it holds it until the Job is completed, failed or
/// cancelled. Queued, started and paused Jobs hold it, and so does an
/// unknown outcome, which may still be executing, until its stop is
/// confirmed.
fn profile_holders(
    index: &impl ReadableTable<(u64, u64), ()>,
    jobs: &impl ReadableTable<u64, &'static [u8]>,
    profile: ProfileId,
    limit: usize,
) -> Result<(Vec<JobId>, bool), StorageError> {
    let mut holders = Vec::new();
    for entry in index.range((profile.get(), 0)..=(profile.get(), u64::MAX))? {
        let job = entry?.0.value().1;
        let bytes = jobs.get(job)?.ok_or_else(|| {
            StorageError::corrupted(format!("{profile} is held by missing job {job}"))
        })?;
        if !JobRow::decode(bytes.value())?.holds_profiles() {
            continue;
        }
        if holders.len() == limit {
            return Ok((holders, true));
        }
        holders.push(JobId::new(job));
    }
    Ok((holders, false))
}

fn last_seq(table: &impl ReadableTable<(u64, u64), u64>, job: u64) -> Result<u64, StorageError> {
    Ok(match table.range((job, 0)..=(job, u64::MAX))?.next_back() {
        Some(entry) => entry?.0.value().1,
        None => 0,
    })
}

/// The second components of the pairs starting with `from`, in order,
/// strictly after `after` when given.
fn pair_targets(
    table: &impl ReadableTable<(u64, u64), ()>,
    from: u64,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<u64>, StorageError> {
    let start = match after {
        Some(u64::MAX) => return Ok(Vec::new()),
        Some(after) => after + 1,
        None => 0,
    };
    let mut targets = Vec::new();
    for entry in table.range((from, start)..=(from, u64::MAX))?.take(limit) {
        targets.push(entry?.0.value().1);
    }
    Ok(targets)
}

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

/// Test stand-in for the cleanup a later milestone adds: records `cleanup`
/// on an Attachment in one transition, as that operation will, and touches
/// nothing else, its content file included.
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
