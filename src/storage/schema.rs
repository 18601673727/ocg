use super::*;

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
/// such an input without them. Version 14 requires PENDING_JOBS, the ordered
/// derived index of pending Jobs; a version 13 build neither knew nor
/// maintained it, so a version 13 file is refused rather than initialized in
/// place. Files of any other version are refused on open, before anything is
/// written to them.
///
/// Format 13 later gained [`SUBMISSIONS`], the index of Control Plane
/// submissions, without a new version: it is created empty when a format 13
/// file without it is opened, which is exact, since no earlier build ever
/// created a Job with a submission key. An earlier build that opens a file
/// holding it leaves it alone and cannot contradict it: it never creates a
/// Job with a key, and a committed Job identity is never reused.
///
/// It then gained [`PENDING_JOBS`], again without a new version. An earlier
/// build neither knows nor maintains it and can start or finalize a Job
/// behind its back, so its entries are never trusted: every opening discards
/// the table and rebuilds it from the Job table. Format 14 requires this
/// table; a version 13 file is refused before that rebuild could run for it.
pub(crate) const FORMAT_VERSION: u32 = 15;
pub(crate) const MAX_WAITING_LAYERS: usize = 64;
pub(crate) const MAX_WAITING_JOBS: usize = 10_000;
pub(crate) const MAX_PENDING_JOBS: u64 = crate::persistence::MAX_PENDING_JOBS;
pub(crate) const MAX_PAGE: usize = 1_000;
/// The most index entries one observation query reads, matching or not.
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;
/// The most distinct (Provider, model) pairs one choice query examines,
/// matching or not.
pub(crate) const MAX_OBSERVED_MODEL_SCAN: usize = 2_000;

pub(super) type Records = TableDefinition<'static, u64, &'static [u8]>;
pub(super) type Link = TableDefinition<'static, u64, u64>;
pub(super) type PairSet = TableDefinition<'static, (u64, u64), ()>;
pub(super) type ScopeIndex = TableDefinition<'static, (&'static [u8], i64, u64), ()>;
/// (job, request number) -> a request's encoded state.
pub(super) type RequestRecords = TableDefinition<'static, (u64, u32), &'static [u8]>;

pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub(super) const GENESIS_KEY: &str = "genesis";
pub(super) const SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("sequence");
pub(super) const JOBS: Records = TableDefinition::new("job");
/// Job ID for every Job other than Genesis
/// that has not started and has no terminal outcome. Derived from JOBS and
/// never an authority of its own: JOBS decides, and [`Batch::insert_job`] and
/// [`Batch::put_job`] keep the two in step in the transaction that writes the
/// Job. IDs are allocated by SEQUENCES in the single write transaction, so
/// their order is commit order even when the wall clock moves backwards.
pub(super) const PENDING_JOBS: TableDefinition<u64, ()> = TableDefinition::new("pending_job");
pub(super) const JOB_INPUTS: Records = TableDefinition::new("job_input");
pub(super) const JOB_OUTPUTS: Records = TableDefinition::new("job_output");
pub(super) const EXECUTORS: Records = TableDefinition::new("executor");
pub(super) const EXECUTION_POOLS: Records = TableDefinition::new("execution_pool");
pub(super) const PROJECTS: Records = TableDefinition::new("project");
pub(super) const PROFILES: Records = TableDefinition::new("profile");
pub(super) const AGENTS: Records = TableDefinition::new("agent");
pub(super) const LLM_PROVIDERS: Records = TableDefinition::new("llm_provider");
pub(super) const METRICS: Records = TableDefinition::new("metrics");
pub(super) const AVAILABILITY_STATS: Records = TableDefinition::new("availability_stat");
pub(super) const ATTACHMENTS: Records = TableDefinition::new("attachment");
/// job -> input, and its incoming index (input, job). A retry may run with
/// its original's frozen input, so one input can serve several Jobs.
pub(super) const HAS_INPUT: Link = TableDefinition::new("has_input");
pub(super) const INPUT_JOBS: PairSet = TableDefinition::new("has_input_in");
/// HAS_ATTACHMENT: an input's Attachments by their position in it, from 0,
/// and the inputs that carry each Attachment.
pub(super) const HAS_ATTACHMENT: TableDefinition<(u64, u32), u64> =
    TableDefinition::new("has_attachment");
pub(super) const ATTACHMENT_INPUTS: PairSet = TableDefinition::new("has_attachment_in");
/// (job, seq) -> output; ordered by seq within a Job.
pub(super) const HAS_OUTPUT: TableDefinition<(u64, u64), u64> = TableDefinition::new("has_output");
/// job -> its highest-seq output, moved by every output insert.
pub(super) const LATEST_OUTPUT: Link = TableDefinition::new("latest_output");
/// child -> parent, and its incoming index (parent, child). The Genesis
/// root marker (genesis -> genesis) has no incoming entry.
pub(super) const CHILD_OF: Link = TableDefinition::new("child_of");
pub(super) const CHILDREN: PairSet = TableDefinition::new("child_of_in");
/// (job, prerequisite) -> condition code.
pub(super) const DEPENDS_ON: TableDefinition<(u64, u64), u8> = TableDefinition::new("depends_on");
pub(super) const ASSIGNED_TO: Link = TableDefinition::new("assigned_to");
pub(super) const PREVIOUS: Link = TableDefinition::new("previous");
pub(super) const REPLACES: Link = TableDefinition::new("replaces");
/// retry -> original, and its incoming index (original, retry).
pub(super) const RETRY_OF: Link = TableDefinition::new("retry_of");
pub(super) const RETRIES: PairSet = TableDefinition::new("retry_of_in");
/// (job, project), and its incoming index (project, job).
pub(super) const IN_PROJECT: PairSet = TableDefinition::new("in_project");
pub(super) const PROJECT_JOBS: PairSet = TableDefinition::new("in_project_in");
/// (agent, Agent Profile), and its incoming index (profile, agent).
pub(super) const USES_PROFILE: PairSet = TableDefinition::new("uses_profile");
pub(super) const PROFILE_AGENTS: PairSet = TableDefinition::new("uses_profile_in");
/// LLM Profile -> LLM Provider, and its incoming index (provider, profile).
pub(super) const USES_PROVIDER: Link = TableDefinition::new("uses_provider");
pub(super) const PROVIDER_PROFILES: PairSet = TableDefinition::new("uses_provider_in");
/// (Agent Profile, LLM Profile): the LLM Profiles an Agent Profile may run
/// with, fixed when the Agent Profile is created.
pub(super) const TARGETS_LLM: PairSet = TableDefinition::new("targets_llm");
/// Job -> the Agent Profile its submission applied, and its incoming index.
pub(super) const APPLIES_PROFILE: Link = TableDefinition::new("applies_profile");
pub(super) const AGENT_PROFILE_JOBS: PairSet = TableDefinition::new("applies_profile_in");
/// Job -> the LLM Profile its submission chose, and its incoming index. A
/// Job with an APPLIES_PROFILE edge has this one too; a Job that runs an LLM
/// without an Agent Profile has only this one.
pub(super) const RUNS_LLM_PROFILE: Link = TableDefinition::new("runs_llm_profile");
pub(super) const LLM_PROFILE_JOBS: PairSet = TableDefinition::new("runs_llm_profile_in");
pub(super) const POOL_PROFILE_JOBS: PairSet = TableDefinition::new("pool_profile_jobs");
/// (transport code, Executor): an index of the transports in each Executor
/// record, written with the record at registration.
pub(super) const TRANSPORT_EXECUTORS: TableDefinition<(u8, u64), ()> =
    TableDefinition::new("llm_transport_executors");
/// Metrics -> the Job it measured, and its incoming index (job, metrics).
pub(super) const MEASURES: Link = TableDefinition::new("measures");
pub(super) const JOB_METRICS: PairSet = TableDefinition::new("measures_in");
/// (scope, observed_at, metrics) for every [`Scope`] a Metrics record is
/// in, written with the record from its MEASURES Job and that Job's frozen
/// input, all fixed.
pub(super) const METRICS_BY_SCOPE: ScopeIndex = TableDefinition::new("metrics_by_scope");
/// AvailabilityStat -> the Job whose request it observed, and its incoming
/// index (job, stat).
pub(super) const OBSERVES: Link = TableDefinition::new("observes");
pub(super) const JOB_AVAILABILITY: PairSet = TableDefinition::new("observes_in");
/// (scope, observed_at, stat), written like METRICS_BY_SCOPE.
pub(super) const AVAILABILITY_BY_SCOPE: ScopeIndex =
    TableDefinition::new("availability_stat_by_scope");
/// Job -> the epoch of the opening whose own runtime started it. Written
/// with the start and removed when the Job settles: with a confirmed outcome,
/// or with a confirmed stop after an unknown one. An entry of an earlier
/// opening is work that no running process in this opening can still execute
/// or settle, but its external work is not thereby known to have stopped; see
/// [`UNRESOLVED_EXECUTIONS`].
pub(super) const LOCAL_EXECUTIONS: Link = TableDefinition::new("local_execution");
/// Job -> why the external execution it started is unresolved, and the
/// capacity its unresolved work may still occupy. Written by the opening that
/// found the Job abandoned, and removed only by a stop confirmation backed by
/// evidence of termination. It is authority, not a projection: nothing rebuilds
/// it, and losing an entry would silently reallocate capacity whose prior
/// occupant was never credibly shown to have stopped.
///
/// It exists because the death of an OCG supervisor is not the termination of
/// its descendants. [`LOCAL_EXECUTIONS`] naming a dead epoch proves only that
/// no opening can still *drive* the Job; it says nothing about processes that
/// epoch left running. An entry here keeps that distinction durable, so an
/// unknown outcome and an unconfirmed physical stop stay distinguishable across
/// any number of restarts.
pub(super) const UNRESOLVED_EXECUTIONS: Records = TableDefinition::new("unresolved_execution");
/// (epoch, seq) -> operation marker.
pub(super) const OPERATIONS: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("operation");
/// (job, request) -> its request sample as materialized. Rebuildable from
/// the raw AvailabilityStat records; the sample is fixed by its lowest record.
pub(super) const AVAILABILITY_SAMPLES: RequestRecords =
    TableDefinition::new("availability_request_sample");
/// (provider, minute, model) -> requests per classification, for the
/// buckets that hold at least one request. Rebuildable like the samples.
pub(super) const AVAILABILITY_MINUTES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("availability_minute");
/// The aggregation stream's progress: its last consumed AvailabilityStat ID
/// and the wall-clock time of its last batch. It is written only with the
/// batch it describes.
pub(super) const AGGREGATION_PROGRESS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("aggregation_progress");

/// (job, request) -> its Token request state: the frozen target and the
/// finite evidence the request has so far. Rebuildable from the raw
/// annotated Metrics.
pub(super) const TOKEN_REQUESTS: RequestRecords = TableDefinition::new("token_request");
/// (provider, minute, model) -> Token totals of the buckets that hold at
/// least one eligible request. Rebuildable like the requests.
pub(super) const TOKEN_MINUTES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("token_minute");

/// (job, request, index) -> the evidence of the tool call at that wire index
/// of the Job's Provider request. The key is the call's identity, so a call
/// has one record. Not rebuildable: it is the only record of these facts.
pub(super) const TOOL_EVIDENCE: TableDefinition<(u64, u32, u32), &[u8]> =
    TableDefinition::new("tool_evidence");
/// (job, Provider tool call id) -> (request, index) of the one evidence
/// record of the Job that uses the id, written with that record.
pub(super) const TOOL_CALL_IDS: TableDefinition<(u64, &str), (u32, u32)> =
    TableDefinition::new("tool_call_id");

/// The SHA-256 of a Control Plane submission's idempotency key -> the one
/// Job it created, with the fingerprint of the request that created it.
/// Written only by the transaction that creates that Job, and never changed
/// or removed, so a key names at most one Job and only a committed one. Not
/// rebuildable: it is the only record of which key created which Job.
pub(super) const SUBMISSIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("control_submission");

// Binding is authoritative; these projections are always rebuilt on opening.
pub(super) const FROZEN_POOL_BINDINGS: Records = TableDefinition::new("frozen_pool_binding");
pub(super) const ASSIGNED_POOL: Link = TableDefinition::new("assigned_pool");
pub(super) const OCCUPANCY: PairSet = TableDefinition::new("occupancy");
pub(super) const COMPATIBLE_POOLS: TableDefinition<(u8, u8), &[u8]> =
    TableDefinition::new("compatible_pool_projection");
