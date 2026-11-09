use super::*;

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CREDENTIAL_KEY_FILE_NAME: &str = "ocg.key";
// Database format is independent of the per-record encoding version.
pub(super) const DATABASE_FORMAT_KEY: &str = "database_format";
/// Names the exact table set and record encodings of the format. A database
/// whose fingerprint differs is incompatible even when its format number matches.
pub(super) const SCHEMA_FINGERPRINT_KEY: &str = "schema_fingerprint";
pub(super) const SCHEMA_FINGERPRINT: &[u8] = b"ocg-native-genesis-v3";
pub(super) const DATABASE_FORMAT_VERSION: u64 = 22;
pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const P_MAX: u64 = 1_048_576;
pub(crate) const MAX_PAGE: usize = 1_000;
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;

// ---- Authoritative tables ---------------------------------------------------------

pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub(super) const SEQUENCES: TableDefinition<&str, u128> = TableDefinition::new("sequence");
pub(super) const CONFIGS: TableDefinition<u128, &[u8]> = TableDefinition::new("config");
pub(super) const CREDENTIALS: TableDefinition<&str, &[u8]> = TableDefinition::new("credential");
pub(super) const JOBS: TableDefinition<u128, &[u8]> = TableDefinition::new("job");
pub(super) const JOB_STATES: TableDefinition<u128, &[u8]> = TableDefinition::new("job_state");
/// One row per Job, keyed by Job: the Allocation of a Pool and an Executor to it.
pub(super) const POOL_ALLOCATIONS: TableDefinition<u128, &[u8]> =
    TableDefinition::new("pool_allocation");
pub(super) const JOB_OUTPUTS: TableDefinition<(u128, u32), &[u8]> =
    TableDefinition::new("job_output");
pub(super) const SUBMISSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("submission");
pub(super) const ATTACHMENTS: TableDefinition<u128, &[u8]> = TableDefinition::new("attachment");
pub(super) const TOOL_CALLS: TableDefinition<(u128, u32, u32), &[u8]> =
    TableDefinition::new("tool_call");
pub(super) const MESSAGES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("message");
pub(super) const REQUEST_EVENTS: TableDefinition<(u128, u32, u32), &[u8]> =
    TableDefinition::new("request_event");
pub(super) const PROJECTS: TableDefinition<u128, &[u8]> = TableDefinition::new("project");
pub(super) const JOB_CONTROLS: TableDefinition<u128, &[u8]> = TableDefinition::new("job_control");
pub(super) const JOB_INPUTS: TableDefinition<(u128, u32), &[u8]> =
    TableDefinition::new("job_input");
pub(super) const NOTICES: TableDefinition<(u128, u32), &[u8]> = TableDefinition::new("notice");
pub(super) const APPROVALS: TableDefinition<(u128, u32, u32), &[u8]> =
    TableDefinition::new("approval");
pub(super) const RECURRENCES: TableDefinition<u128, &[u8]> = TableDefinition::new("recurrence");

// ---- Derived tables ----------------------------------------------------------------

pub(super) const JOB_OBSERVATIONS: TableDefinition<(u128, u8, u128), (u32, u32)> =
    TableDefinition::new("job_observation");
pub(super) const CONTENT_REFERENCES: TableDefinition<&[u8], (u64, u64)> =
    TableDefinition::new("content_reference");
pub(super) const CONTENT_RECLAIM: TableDefinition<&[u8], u64> =
    TableDefinition::new("content_reclaim");

/// (pool, executor, job) for every Allocation that still holds capacity, that
/// is, every `POOL_ALLOCATIONS` row whose phase is not Released. It is written
/// in the same transaction as the phase change that adds or removes the row,
/// so Pool and Executor occupancy is a bounded range read rather than a scan
/// of every Allocation ever made.
pub(super) const ACTIVE_ALLOCATIONS: TableDefinition<(u128, u128, u128), ()> =
    TableDefinition::new("active_allocation");
/// Reservation request identity -> the Job whose single Allocation it made.
/// Source: `POOL_ALLOCATIONS`. A request identity is never reused, so a row
/// is added with its Allocation and never removed.
pub(super) const ALLOCATION_REQUESTS: TableDefinition<u128, u128> =
    TableDefinition::new("allocation_request");
/// Composer context -> the one unsubmitted, unsettled Job drafted there.
/// Source: `JOBS` rows in the Unsubmitted phase whose state has no outcome.
pub(super) const DRAFT_CONTEXTS: TableDefinition<u128, u128> =
    TableDefinition::new("draft_context");

pub(super) const JOB_ALLOCATIONS: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("job_allocation");
pub(super) const CONVERSATIONS: TableDefinition<u128, ()> = TableDefinition::new("conversation");
pub(super) const TURNS: TableDefinition<(u128, u128), ()> = TableDefinition::new("turn");

pub(super) const PENDING: TableDefinition<u128, ()> = TableDefinition::new("pending");
pub(super) const READY: TableDefinition<(u16, u128), ()> = TableDefinition::new("ready");
/// The ready queue orders high priority first, then submission time and Job ID.
pub(super) const MUTABLE_READY: TableDefinition<(u8, i64, u128, u16), ()> =
    TableDefinition::new("ready");
pub(super) const DELAYED: TableDefinition<(i64, u128), ()> = TableDefinition::new("delayed");
pub(super) const DEPENDENCY_WAITERS: TableDefinition<(u128, u128), u8> =
    TableDefinition::new("dependency_waiter");
pub(super) const UNSETTLED: TableDefinition<u128, (u32, u32)> = TableDefinition::new("unsettled");
pub(super) const HELD_SLOTS: TableDefinition<(u128, u128), (u64, bool)> =
    TableDefinition::new("held_slot");
pub(super) const CHILDREN: TableDefinition<(u128, u128), ()> = TableDefinition::new("children");
pub(super) const SUCCESSORS: TableDefinition<u128, ()> = TableDefinition::new("successor");
/// (conversation, Unicode gram, turn)
pub(crate) type ConversationSearchKey = (u128, u64, u128);
pub(super) const CONVERSATION_SEARCH: TableDefinition<ConversationSearchKey, ()> =
    TableDefinition::new("conversation_search");
pub(super) const CHILD_GROUPS: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("child_group");
pub(super) const LIVE_CHILDREN: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("live_child");
pub(super) const USER_INPUT_AT: TableDefinition<u128, ()> = TableDefinition::new("user_input_at");
pub(super) const ACTIVE_JOBS: TableDefinition<u128, u128> = TableDefinition::new("active_job");
pub(super) const GROUP_RESULTS: TableDefinition<u128, (u128, u128)> =
    TableDefinition::new("group_result");
pub(super) const NOTICE_READ: TableDefinition<u128, u32> = TableDefinition::new("notice_read");
pub(super) const TOOL_CALL_IDS: TableDefinition<(u128, &str), (u32, u32)> =
    TableDefinition::new("tool_call_id");
/// (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max)
/// Duration semantics: admission commit -> outcome commit (includes the
/// admission's durable commit), never spanning a restart.
pub(crate) type ToolStatValue = (u64, u64, u64, u64, u64, u64);
pub(super) const TOOL_STATS: TableDefinition<&str, ToolStatValue> =
    TableDefinition::new("tool_stat");
/// (hour: i64, agent: u64, provider: u64, model_hash: [u8; 32])
pub(crate) type UsageTotalKey = (i64, u128, u128, [u8; 32]);
/// (requests, complete_requests, input, output, cache_read, cache_write, reasoning)
pub(crate) type UsageTotalValue = (u64, u64, u64, u64, u64, u64, u64);
pub(super) const USAGE_TOTALS: TableDefinition<UsageTotalKey, UsageTotalValue> =
    TableDefinition::new("usage_total");
pub(super) const RETRIES: TableDefinition<(u128, u128), bool> = TableDefinition::new("retry");
/// (provider, model_hash) -> the smallest Job with a qualifying request event.
pub(super) const USAGE_MODEL: TableDefinition<(u128, [u8; 32]), u128> =
    TableDefinition::new("usage_model");
pub(super) type RequestTimelineKey = (i64, u128, u32, u32);
pub(super) const REQUEST_TIMELINE: TableDefinition<RequestTimelineKey, ()> =
    TableDefinition::new("request_timeline");

pub(super) const OPEN_APPROVALS: TableDefinition<(u128, u128, u32, u32), u8> =
    TableDefinition::new("open_approval");
pub(super) const APPROVAL_CALLS: TableDefinition<(u128, u32, u32), (u128, u32, u32)> =
    TableDefinition::new("approval_call");

pub(super) const RECURRENCE_DUE: TableDefinition<(i64, u128), ()> =
    TableDefinition::new("recurrence_due");
