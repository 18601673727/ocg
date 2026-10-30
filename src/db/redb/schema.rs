use super::*;

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const P_MAX: u64 = 1_048_576;
pub(crate) const MAX_PAGE: usize = 1_000;
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;

// ---- Authoritative tables ---------------------------------------------------------

pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub(super) const SEQUENCES: TableDefinition<&str, u128> = TableDefinition::new("sequence");
pub(super) const CONFIGS: TableDefinition<u128, &[u8]> = TableDefinition::new("config");
pub(super) const JOBS: TableDefinition<u128, &[u8]> = TableDefinition::new("job");
pub(super) const JOB_STATES: TableDefinition<u128, &[u8]> = TableDefinition::new("job_state");
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

pub(super) const JOB_ALLOCATIONS: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("job_allocation");
pub(super) const CONVERSATIONS: TableDefinition<u128, ()> = TableDefinition::new("conversation");
pub(super) const TURNS: TableDefinition<(u128, u128), ()> = TableDefinition::new("turn");

pub(super) const PENDING: TableDefinition<u128, ()> = TableDefinition::new("pending");
pub(super) const READY: TableDefinition<(u16, u128), ()> = TableDefinition::new("ready");
pub(super) const DELAYED: TableDefinition<(i64, u128), ()> = TableDefinition::new("delayed");
pub(super) const DEPENDENCY_WAITERS: TableDefinition<(u128, u128), u8> =
    TableDefinition::new("dependency_waiter");
pub(super) const UNSETTLED: TableDefinition<u128, (u32, u32)> = TableDefinition::new("unsettled");
pub(super) const HELD_SLOTS: TableDefinition<(u128, u128), (u64, bool)> =
    TableDefinition::new("held_slot");
pub(super) const CHILDREN: TableDefinition<(u128, u128), ()> = TableDefinition::new("children");
pub(super) const SUCCESSORS: TableDefinition<u128, u128> = TableDefinition::new("successor");
pub(super) const GROUP_HEADS: TableDefinition<u128, u128> = TableDefinition::new("job_group_head");
/// (conversation, Unicode gram, turn)
pub(crate) type ConversationSearchKey = (u128, u64, u128);
pub(super) const CONVERSATION_SEARCH: TableDefinition<ConversationSearchKey, ()> =
    TableDefinition::new("conversation_search");
pub(super) const CHILD_GROUPS: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("child_group");
pub(super) const LIVE_CHILDREN: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("live_child");
pub(super) const USER_INPUT_AT: TableDefinition<u128, i64> = TableDefinition::new("user_input_at");
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
pub(super) const ATTACHMENT_REFS: TableDefinition<(u128, u128), ()> =
    TableDefinition::new("attachment_ref");
pub(super) type RequestScopeKey = (u128, [u8; 32], i64, u128, u32, u32);
pub(super) const REQUEST_SCOPES: TableDefinition<RequestScopeKey, ()> =
    TableDefinition::new("request_scope");
pub(super) type RequestTimelineKey = (i64, u128, u32, u32);
pub(super) const REQUEST_TIMELINE: TableDefinition<RequestTimelineKey, ()> =
    TableDefinition::new("request_timeline");

pub(super) const OPEN_APPROVALS: TableDefinition<(u128, u128, u32, u32), u8> =
    TableDefinition::new("open_approval");
pub(super) const APPROVAL_CALLS: TableDefinition<(u128, u32, u32), (u128, u32, u32)> =
    TableDefinition::new("approval_call");

pub(super) const RECURRENCE_DUE: TableDefinition<(i64, u128), ()> =
    TableDefinition::new("recurrence_due");
