use super::StorageError;
use crate::db::redb::recovery;
use ::redb::{ReadTransaction, ReadableTable, TableDefinition, WriteTransaction};
use std::collections::BTreeSet;

pub(crate) const DATABASE_FILE_NAME: &str = "ocg.redb";
pub(crate) const CREDENTIAL_KEY_FILE_NAME: &str = "ocg.key";
pub(super) const DATABASE_FORMAT_KEY: &str = "database_format";
pub(super) const FORMAT_VERSION: u64 = 4; // Shared by database metadata and record headers.
pub(super) const SCHEMA_FINGERPRINT_KEY: &str = "schema_fingerprint";
pub(super) const SCHEMA_FINGERPRINT: &[u8] = b"ocg-native-format-4";

pub(super) const fn record_version_byte() -> u8 {
    const { assert!(FORMAT_VERSION <= u8::MAX as u64) };
    FORMAT_VERSION as u8
}

pub(crate) const CACHE_BYTES: usize = 32 * 1024 * 1024;
pub(crate) const P_MAX: u64 = 1_048_576;
pub(crate) const MAX_PAGE: usize = 1_000;
pub(crate) const MAX_OBSERVATION_SCAN: usize = 5_000;
pub(crate) type ConversationSearchKey = (u128, u64, u128);
pub(crate) type ToolStatValue = (u64, u64, u64, u64, u64, u64);
pub(crate) type UsageTotalKey = (i64, u128, u128, [u8; 32]);
pub(crate) type UsageTotalValue = (u64, u64, u64, u64, u64, u64, u64);
pub(super) type RequestTimelineKey = (i64, u128, u32, u32);

// A single inventory defines, creates and verifies the tables.
// Startup verifies the layout. Writes maintain indexes alongside their source rows.
macro_rules! tables {
    ($($group:ident {
        $($name:ident: $key:ty => $value:ty = $table:literal $(tags [$($tag:literal),+])?;)*
    })*) => {
        $($(pub(super) const $name: TableDefinition<$key, $value> =
            TableDefinition::new($table);)*)*

        pub(super) fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
            $($(tx.open_table($name)?;)*)*
            Ok(())
        }

        pub(super) fn check_required_tables(tx: &ReadTransaction) -> Result<(), StorageError> {
            check_layout(tx, &[$($($table,)*)*])?;
            $($(tx.open_table($name)?;)*)*
            Ok(())
        }

        pub(super) fn check_record_headers(tx: &ReadTransaction) -> Result<(), StorageError> {
            $($($(
                for entry in tx.open_table($name)?.iter()? {
                    recovery::check_preflight_header(entry?.1.value(), &[$($tag),+])?;
                }
            )?)*)*
            Ok(())
        }

        #[cfg(unix)]
        pub(super) fn rewrite_record_headers(versions: recovery::migration::Versions, source: &ReadTransaction, tx: &WriteTransaction) -> Result<(), StorageError> {
            $($($(
                recovery::migration::rewrite_tagged(versions, source, tx, $name, &[$($tag),+])?;
            )?)*)*
            Ok(())
        }

        #[cfg(unix)]
        pub(super) fn verify_record_headers(versions: recovery::migration::Versions, source: &ReadTransaction, result: &ReadTransaction) -> Result<(), recovery::reserved::ReservedRefusal> {
            macro_rules! verify {
                (META) => { recovery::migration::verify_metadata(versions, source, result)? };
                ($definition:ident, $tags:expr) => {
                    recovery::migration::verify_tagged(versions, source, result, $definition, $tags)?
                };
                ($definition:ident) => {
                    recovery::audit::verify_identical_table(source, result, $definition)?
                };
            }
            $($(verify!($name $(, &[$($tag),+])?);)*)*
            Ok(())
        }

        pub(super) fn verify_identical_database(expected: &ReadTransaction, published: &ReadTransaction) -> Result<(), recovery::reserved::ReservedRefusal> {
            $($(recovery::audit::verify_identical_table(expected, published, $name)?;)*)*
            Ok(())
        }

        #[cfg(unix)]
        pub(super) fn verify_compaction_rebuild_preservation(
            source: &ReadTransaction,
            published: &ReadTransaction,
        ) -> Result<(), recovery::reserved::ReservedRefusal> {
            use ::redb::TableHandle;

            $($(if $name.name() != CONVERSATION_CONFIG_REFS.name() {
                recovery::audit::verify_identical_table(source, published, $name)?;
            })*)*
            Ok(())
        }
    };
}

fn check_layout(tx: &ReadTransaction, expected: &[&str]) -> Result<(), StorageError> {
    use ::redb::TableHandle;

    let present = tx
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if present.len() != expected.len()
        || expected.iter().any(|name| !present.contains(*name))
        || tx.list_multimap_tables()?.next().is_some()
    {
        return Err(StorageError::corrupted(
            "database tables do not match the declared storage format",
        ));
    }
    Ok(())
}

tables! {
    facts {
        CONFIGS: u128 => &[u8] = "config" tags [1];
        POOLS: u128 => &[u8] = "pool" tags [40];
        EXECUTORS: u128 => &[u8] = "executor" tags [41];
        CREDENTIALS: &str => &[u8] = "credential" tags [16];
        PROJECTS: u128 => &[u8] = "project" tags [9];

        // Containers and executable work have distinct, non-polymorphic rows.
        CONVERSATIONS: u128 => &[u8] = "conversation" tags [53];
        JOBS: u128 => &[u8] = "job" tags [2];
        JOB_RUNTIME: u128 => &[u8] = "job_runtime" tags [3];
        JOB_INPUTS: (u128, u32) => &[u8] = "job_input" tags [52];
        JOB_OUTPUTS: (u128, u32) => &[u8] = "job_output" tags [4];
        POOL_ALLOCATIONS: u128 => &[u8] = "pool_allocation" tags [42];
        SUBMISSIONS: &[u8] => &[u8] = "submission" tags [5];
        ATTACHMENTS: u128 => &[u8] = "attachment" tags [6];
        MESSAGES: &[u8] => &[u8] = "message";
        RECURRENCES: u128 => &[u8] = "recurrence" tags [13];
        APPROVALS: (u128, u32, u32) => &[u8] = "approval" tags [15];
        SCHEDULED_EXTERNAL_POLICY: u128 => &[u8] = "scheduled_external_policy" tags [18];
        SCHEDULED_EXTERNAL_BINDING: u128 => &[u8] = "scheduled_external_binding" tags [19];

        // Append-only execution evidence belongs to its Job.
        JOB_EVENTS: (u128, u64) => &[u8] = "job_event" tags [55];
        REQUEST_EVENTS: (u128, u32, u32) => &[u8] = "request_event" tags [8];
        TOOL_CALLS: (u128, u32, u32) => &[u8] = "tool_call" tags [7];
    }
    metadata {
        META: &str => &[u8] = "meta";
        SEQUENCES: &str => u128 = "sequence";
        EXECUTION_COMMITS: u64 => u64 = "execution_commit";
    }
    indexes {
        // Admission and idempotence.
        POOL_EXECUTORS: (u128, u128) => () = "pool_executor";
        ACTIVE_ALLOCATIONS: (u128, u128, u128) => () = "active_allocation";
        ALLOCATION_REQUESTS: u128 => u128 = "allocation_request";
        DRAFT_CONTEXTS: u128 => u128 = "draft_context";
        PENDING: u128 => () = "pending";
        MUTABLE_READY: (u8, i64, u128, u16) => () = "ready";
        DEPENDENCY_WAITERS: (u128, u128) => u8 = "dependency_waiter";
        RECURRENCE_DUE: (i64, u128) => () = "recurrence_due";

        // Bounded hierarchy, history and search queries.
        TURNS: (u128, u128) => () = "turn";
        CHILDREN: (u128, u128) => () = "children";
        CHILD_GROUPS: (u128, u128) => () = "child_group";
        LIVE_CHILDREN: (u128, u128) => () = "live_child";
        SUCCESSORS: u128 => () = "successor";
        RETRY_JOBS: (u128, u128) => () = "job_retry";
        CONVERSATION_CONFIG_REFS: (u8, u128, u128) => () = "conversation_config_ref";
        CONVERSATION_SEARCH: ConversationSearchKey => () = "conversation_search";
        TOOL_CALL_IDS: (u128, &str) => (u32, u32) = "tool_call_id";
        OPEN_APPROVALS: (u128, u128, u32, u32) => u8 = "open_approval";

        // Observation projections and content ownership.
        JOB_OBSERVATIONS: (u128, u8, u128) => (u32, u32) = "job_observation";
        TOOL_STATS: &str => ToolStatValue = "tool_stat";
        USAGE_TOTALS: UsageTotalKey => UsageTotalValue = "usage_total";
        USAGE_MODEL: (u128, [u8; 32]) => u128 = "usage_model";
        REQUEST_TIMELINE: RequestTimelineKey => () = "request_timeline";
        CONTENT_REFERENCES: &[u8] => (u64, u64) = "content_reference";
        CONTENT_RECLAIM: &[u8] => u64 = "content_reclaim";
    }
}
