use super::*;

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const FORMAT_NAME: &[u8] = b"ocg-foundation";
pub(crate) const FORMAT_VERSION: u32 = 23;
pub(crate) const P_MAX: u64 = 1_048_576;
pub(crate) const MAX_PAGE: usize = 1_000;
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;
pub(crate) const MAX_OBSERVED_MODEL_SCAN: usize = 2_000;
pub(crate) const MAX_PENDING_JOBS: u64 = P_MAX;

// ---- Authoritative tables ---------------------------------------------------------

pub(super) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub(super) const SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("sequence");
pub(super) const CONFIGS: TableDefinition<u64, &[u8]> = TableDefinition::new("config");
pub(super) const JOBS: TableDefinition<u64, &[u8]> = TableDefinition::new("job");
pub(super) const JOB_STATES: TableDefinition<u64, &[u8]> = TableDefinition::new("job_state");
pub(super) const JOB_OUTPUTS: TableDefinition<(u64, u32), &[u8]> =
    TableDefinition::new("job_output");
pub(super) const SUBMISSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("submission");
pub(super) const ATTACHMENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("attachment");
pub(super) const TOOL_CALLS: TableDefinition<(u64, u32, u32), &[u8]> =
    TableDefinition::new("tool_call");
pub(super) const MESSAGES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("message");
pub(super) const REQUEST_EVENTS: TableDefinition<(u64, u32, u32), &[u8]> =
    TableDefinition::new("request_event");
pub(super) const PROJECTS: TableDefinition<u64, &[u8]> = TableDefinition::new("project");
pub(super) const CONVERSATIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("conversation");

// ---- Derived tables ----------------------------------------------------------------

pub(super) const PENDING: TableDefinition<u64, ()> = TableDefinition::new("pending");
pub(super) const READY: TableDefinition<(u16, u64), ()> = TableDefinition::new("ready");
pub(super) const DELAYED: TableDefinition<(i64, u64), ()> = TableDefinition::new("delayed");
pub(super) const DEPENDENCY_WAITERS: TableDefinition<(u64, u64), u8> =
    TableDefinition::new("dependency_waiter");
pub(super) const UNSETTLED: TableDefinition<u64, (u32, u32)> = TableDefinition::new("unsettled");
pub(super) const HELD_SLOTS: TableDefinition<(u64, u64), (u64, bool)> =
    TableDefinition::new("held_slot");
pub(super) const CHILDREN: TableDefinition<(u64, u64), ()> = TableDefinition::new("children");
pub(super) const SUCCESSORS: TableDefinition<u64, u64> = TableDefinition::new("successor");
pub(super) const TURN_INPUTS: TableDefinition<(u64, u32), &[u8]> =
    TableDefinition::new("turn_input");
pub(super) const NOTICES: TableDefinition<(u64, u32), &[u8]> = TableDefinition::new("notice");
pub(super) const TIPS: TableDefinition<u64, u64> = TableDefinition::new("tip");
pub(super) const RECENT: TableDefinition<(u64, u64), ()> = TableDefinition::new("recent");
pub(super) const CHILD_CONVERSATIONS: TableDefinition<(u64, u64), ()> =
    TableDefinition::new("child_conversation");
pub(super) const LIVE_CHILDREN: TableDefinition<(u64, u64), ()> =
    TableDefinition::new("live_child");
pub(super) const USER_ENTRY_AT: TableDefinition<u64, i64> = TableDefinition::new("user_entry_at");
pub(super) const ACTIVE_TURNS: TableDefinition<u64, u64> = TableDefinition::new("active_turn");
pub(super) const ANSWER_TIPS: TableDefinition<u64, (u64, u64)> = TableDefinition::new("answer_tip");
pub(super) const NOTICE_READ: TableDefinition<u64, u32> = TableDefinition::new("notice_read");
pub(super) const TOOL_CALL_IDS: TableDefinition<(u64, &str), (u32, u32)> =
    TableDefinition::new("tool_call_id");
pub(super) const TOOL_TOTALS: TableDefinition<u64, (u64, u64)> = TableDefinition::new("tool_total");
/// (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max)
/// Duration semantics: admission commit -> outcome commit (includes the
/// admission's durable commit), never spanning a restart.
pub(crate) type ToolStatValue = (u64, u64, u64, u64, u64, u64);
pub(super) const TOOL_STATS: TableDefinition<&str, ToolStatValue> =
    TableDefinition::new("tool_stat");
/// (hour: i64, agent: u64, provider: u64, model_hash: [u8; 32])
pub(crate) type UsageTotalKey = (i64, u64, u64, [u8; 32]);
/// (requests, complete_requests, input, output, cache_read, cache_write, reasoning)
pub(crate) type UsageTotalValue = (u64, u64, u64, u64, u64, u64, u64);
pub(super) const USAGE_TOTALS: TableDefinition<UsageTotalKey, UsageTotalValue> =
    TableDefinition::new("usage_total");
pub(super) const RETRIES: TableDefinition<(u64, u64), bool> = TableDefinition::new("retry");
pub(super) const ATTACHMENT_REFS: TableDefinition<(u64, u64), ()> =
    TableDefinition::new("attachment_ref");
type RequestScopeKey = (u64, [u8; 32], i64, u64, u32, u32);
pub(super) const REQUEST_SCOPES: TableDefinition<RequestScopeKey, ()> =
    TableDefinition::new("request_scope");

pub(super) const APPROVALS: TableDefinition<(u64, u32, u32), &[u8]> =
    TableDefinition::new("approval");
pub(super) const OPEN_APPROVALS: TableDefinition<(u64, u64, u32, u32), u8> =
    TableDefinition::new("open_approval");
pub(super) const APPROVAL_CALLS: TableDefinition<(u64, u32, u32), (u64, u32, u32)> =
    TableDefinition::new("approval_call");

pub(super) const RECURRENCES: TableDefinition<u64, &[u8]> = TableDefinition::new("recurrence");
pub(super) const RECURRENCE_DUE: TableDefinition<(i64, u64), ()> =
    TableDefinition::new("recurrence_due");
