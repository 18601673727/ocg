//! The one offline migration this build supports: database format 22
//! (`ocg-native-genesis-v3`) to format 23 (`ocg-native-genesis-v4`).
//!
//! The change is additive: two new empty tables (the durable scheduled
//! external policy and its Allocation binding) and the two identity markers. No
//! authoritative row is rewritten, no derived table is rebuilt and no
//! credential is touched. The source is opened read-only; the migration applies
//! to a private copy and is published only after every logical row and every
//! copied file has been verified against the source.
//!
//! An older or unmarked format is refused rather than reinterpreted. A database
//! marked format 22 that already carries a non-empty new table, or one marked
//! format 23 that lacks either new table, is a partial migration and is
//! refused. The source is never modified: publication is an atomic no-replace
//! rename of the verified staging directory.

use std::collections::BTreeSet;
use std::path::Path;

use ::redb::TableHandle;
use aws_lc_rs::digest::{Context, SHA256};

use super::*;

/// The format this build can migrate from. The current format needs no
/// migration.
pub(super) const MIGRATION_SOURCE_FORMAT: u64 = 22;
pub(super) const MIGRATION_SOURCE_FINGERPRINT: &[u8] = b"ocg-native-genesis-v3";

/// The logical identity of every authoritative row in a database. Two
/// databases with equal digests hold exactly the same rows; table pages, free
/// space and file layout are deliberately excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MigrationDigest {
    tables: [u8; 32],
    meta: [u8; 32],
    pub(super) rows: u64,
    pub(super) tables_count: u64,
}

/// What `--check` found.
pub(crate) enum MigrationInspection {
    /// Already this build's format; nothing to do.
    Current,
    /// The immediately preceding format, with a verified logical snapshot.
    Upgrade(MigrationPlan),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MigrationPlan {
    pub(crate) from: u64,
    pub(crate) to: u64,
    digest: MigrationDigest,
}

impl MigrationPlan {
    /// Logical tables and rows that must survive unchanged.
    pub(crate) fn tables(&self) -> u64 {
        self.digest.tables_count
    }

    pub(crate) fn rows(&self) -> u64 {
        self.digest.rows
    }
}

/// Inspects `path` read-only. A running host holds the write lock, so this
/// reports redb's `Database already open` instead of reading under it.
pub(crate) fn inspect(path: &Path) -> Result<MigrationInspection, Box<dyn std::error::Error>> {
    if std::fs::metadata(path)?.len() == 0 {
        return Err("database is empty/uninitialized; start OCG to initialize it".into());
    }
    let database = ::redb::ReadOnlyDatabase::open(path)?;
    inspect_database(&database)
}

fn inspect_database(
    database: &impl ::redb::ReadableDatabase,
) -> Result<MigrationInspection, Box<dyn std::error::Error>> {
    let tx = database.begin_read()?;
    let names = table_names(&tx)?;
    let meta = tx.open_table(META)?;
    let format = read_format(&meta)?;
    let fingerprint = read_fingerprint(&meta)?;
    check_metadata(&meta)?;
    match format {
        DATABASE_FORMAT_VERSION => {
            if fingerprint.as_slice() != SCHEMA_FINGERPRINT {
                return Err(format!(
                    "database format {} has an unexpected schema fingerprint; this build opens only `{}`",
                    format,
                    String::from_utf8_lossy(SCHEMA_FINGERPRINT)
                )
                .into());
            }
            for required in [
                SCHEDULED_EXTERNAL_POLICY.name(),
                SCHEDULED_EXTERNAL_BINDING.name(),
            ] {
                if !names.iter().any(|name| name == required) {
                    return Err(format!(
                        "database is marked format {DATABASE_FORMAT_VERSION} but lacks the {required} table; it is a partial migration, not a supported database"
                    )
                    .into());
                }
            }
            Ok(MigrationInspection::Current)
        }
        MIGRATION_SOURCE_FORMAT => {
            if fingerprint.as_slice() != MIGRATION_SOURCE_FINGERPRINT {
                return Err(format!(
                    "database format {format} has an unexpected schema fingerprint; this build migrates only the format {MIGRATION_SOURCE_FORMAT} database it wrote itself"
                )
                .into());
            }
            for present in [SCHEDULED_EXTERNAL_POLICY.name(), SCHEDULED_EXTERNAL_BINDING.name()] {
                if names.iter().any(|name| name == present) {
                    let len = open_table_len(&tx, present)?;
                    if len != 0 {
                        return Err(format!(
                            "database is marked format {format} but the {present} table already has rows; it is a partial migration, not a supported source"
                        )
                        .into());
                    }
                }
            }
            require_source_tables(&names)?;
            recovery::migration_check_headers(&tx)?;
            let digest = digest_tables(&tx, &names)?;
            Ok(MigrationInspection::Upgrade(MigrationPlan {
                from: format,
                to: DATABASE_FORMAT_VERSION,
                digest,
            }))
        }
        other => Err(format!(
            "database format {other} is not supported; this build opens only format {DATABASE_FORMAT_VERSION} and migrates only format {MIGRATION_SOURCE_FORMAT}"
        )
        .into()),
    }
}

/// Applies the plan to the private copy at `path` in one immediate write
/// transaction, then verifies that every logical row, every metadata value
/// other than the two identity markers, and the table set are unchanged apart
/// from the two empty new tables.
pub(crate) fn apply(path: &Path, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    let database = ::redb::Database::open(path)?;
    {
        let tx = database.begin_read()?;
        let names = table_names(&tx)?;
        require_source_tables(&names)?;
        let digest = digest_tables(&tx, &names)?;
        if digest != plan.digest {
            return Err("copied database does not match the source".into());
        }
    }
    let mut tx = database.begin_write()?;
    tx.set_durability(::redb::Durability::Immediate)?;
    tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
    {
        let mut table = tx.open_table(META)?;
        table.insert(
            DATABASE_FORMAT_KEY,
            DATABASE_FORMAT_VERSION.to_le_bytes().as_slice(),
        )?;
        table.insert(SCHEMA_FINGERPRINT_KEY, SCHEMA_FINGERPRINT)?;
    }
    tx.commit()?;
    {
        let tx = database.begin_read()?;
        let names = table_names(&tx)?;
        for required in [
            SCHEDULED_EXTERNAL_POLICY.name(),
            SCHEDULED_EXTERNAL_BINDING.name(),
        ] {
            if !names.iter().any(|name| name == required) {
                return Err(format!("the migrated database lacks {required}").into());
            }
        }
        let digest = digest_tables(&tx, &names)?;
        if digest != plan.digest {
            return Err("migrated authoritative data differs from the source".into());
        }
        let meta = tx.open_table(META)?;
        recovery::check_identity(&meta)?;
        check_metadata(&meta)?;
        recovery::migration_check_headers(&tx)?;
    }
    Ok(())
}

fn table_names(tx: &ReadTransaction) -> Result<BTreeSet<String>, Box<dyn std::error::Error>> {
    use ::redb::TableHandle;
    let names = tx
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect();
    if tx.list_multimap_tables()?.next().is_some() {
        return Err("multimap tables are not part of this database format".into());
    }
    Ok(names)
}

fn read_format(
    meta: &impl ::redb::ReadableTable<&'static str, &'static [u8]>,
) -> Result<u64, Box<dyn std::error::Error>> {
    match meta.get(DATABASE_FORMAT_KEY)? {
        Some(value) => <[u8; 8]>::try_from(value.value())
            .map(u64::from_le_bytes)
            .map_err(|_| "database format marker is malformed".into()),
        None => Err(
            "unmarked database; this build opens only format 23 and migrates only format 22".into(),
        ),
    }
}

fn read_fingerprint(
    meta: &impl ::redb::ReadableTable<&'static str, &'static [u8]>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match meta.get(SCHEMA_FINGERPRINT_KEY)? {
        Some(value) => Ok(value.value().to_vec()),
        None => Err("database has no schema fingerprint".into()),
    }
}

fn check_metadata(
    meta: &impl ::redb::ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), Box<dyn std::error::Error>> {
    for key in ["open_epoch", "clean_shutdown", "compaction_settings"] {
        if meta.get(key)?.is_none() {
            return Err(format!("database metadata is missing {key}").into());
        }
    }
    let epoch = meta
        .get("open_epoch")?
        .ok_or("database metadata is missing open_epoch")?;
    if epoch.value().len() != 8 {
        return Err("database open epoch is malformed".into());
    }
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or("database metadata is missing clean_shutdown")?;
    if !matches!(shutdown.value(), [0] | [1]) {
        return Err("database shutdown marker is malformed".into());
    }
    let settings = meta
        .get("compaction_settings")?
        .ok_or("database metadata is missing compaction_settings")?;
    recovery::check_preflight_header(settings.value(), &[TAG_GLOBAL_COMPACTION])?;
    Ok(())
}

fn open_table_len(tx: &ReadTransaction, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    use ::redb::ReadableTableMetadata;
    let definition = match name {
        "scheduled_external_policy" => tx.open_table(SCHEDULED_EXTERNAL_POLICY)?.len()?,
        "scheduled_external_binding" => tx.open_table(SCHEDULED_EXTERNAL_BINDING)?.len()?,
        _ => return Err(format!("unexpected optional table {name}").into()),
    };
    Ok(definition)
}

/// Every table the preceding format always had. `held_slot` is created lazily
/// by a first Allocation and is allowed to be absent.
fn require_source_tables(names: &BTreeSet<String>) -> Result<(), Box<dyn std::error::Error>> {
    for required in [
        "meta",
        "sequence",
        "config",
        "credential",
        "job",
        "job_state",
        "pool_allocation",
        "job_output",
        "submission",
        "attachment",
        "tool_call",
        "message",
        "request_event",
        "project",
        "job_control",
        "job_input",
        "notice",
        "approval",
        "recurrence",
        "job_observation",
        "content_reference",
        "content_reclaim",
        "active_allocation",
        "allocation_request",
        "draft_context",
        "job_allocation",
        "conversation",
        "turn",
        "pending",
        "ready",
        "delayed",
        "dependency_waiter",
        "unsettled",
        "children",
        "successor",
        "conversation_search",
        "child_group",
        "live_child",
        "user_input_at",
        "active_job",
        "group_result",
        "notice_read",
        "tool_call_id",
        "tool_stat",
        "usage_total",
        "retry",
        "usage_model",
        "request_timeline",
        "open_approval",
        "approval_call",
        "recurrence_due",
    ] {
        if !names.iter().any(|name| name == required) {
            return Err(format!(
                "database lacks the {required} table this format defines; it is not a complete format {MIGRATION_SOURCE_FORMAT} database"
            )
            .into());
        }
    }
    Ok(())
}

fn digest_tables(
    tx: &ReadTransaction,
    names: &BTreeSet<String>,
) -> Result<MigrationDigest, Box<dyn std::error::Error>> {
    let mut tables = Context::new(&SHA256);
    let mut meta = Context::new(&SHA256);
    let mut rows = 0u64;
    let mut tables_count = 0u64;
    for name in names {
        rows = rows.saturating_add(match name.as_str() {
            "meta" => {
                tables_count += 1;
                hash_meta(tx, &mut meta)?
            }
            "scheduled_external_policy" => {
                let len = open_table_len(tx, name)?;
                if len != 0 {
                    return Err("the scheduled external policy table is not empty".into());
                }
                0
            }
            "scheduled_external_binding" => {
                let len = open_table_len(tx, name)?;
                if len != 0 {
                    return Err("the scheduled external binding table is not empty".into());
                }
                0
            }
            "sequence" => {
                tables_count += 1;
                hash_table(tx, SEQUENCES, &mut tables)?
            }
            "config" => {
                tables_count += 1;
                hash_table(tx, CONFIGS, &mut tables)?
            }
            "credential" => {
                tables_count += 1;
                hash_table(tx, CREDENTIALS, &mut tables)?
            }
            "job" => {
                tables_count += 1;
                hash_table(tx, JOBS, &mut tables)?
            }
            "job_state" => {
                tables_count += 1;
                hash_table(tx, JOB_STATES, &mut tables)?
            }
            "pool_allocation" => {
                tables_count += 1;
                hash_table(tx, POOL_ALLOCATIONS, &mut tables)?
            }
            "job_output" => {
                tables_count += 1;
                hash_table(tx, JOB_OUTPUTS, &mut tables)?
            }
            "submission" => {
                tables_count += 1;
                hash_table(tx, SUBMISSIONS, &mut tables)?
            }
            "attachment" => {
                tables_count += 1;
                hash_table(tx, ATTACHMENTS, &mut tables)?
            }
            "tool_call" => {
                tables_count += 1;
                hash_table(tx, TOOL_CALLS, &mut tables)?
            }
            "message" => {
                tables_count += 1;
                hash_table(tx, MESSAGES, &mut tables)?
            }
            "request_event" => {
                tables_count += 1;
                hash_table(tx, REQUEST_EVENTS, &mut tables)?
            }
            "project" => {
                tables_count += 1;
                hash_table(tx, PROJECTS, &mut tables)?
            }
            "job_control" => {
                tables_count += 1;
                hash_table(tx, JOB_CONTROLS, &mut tables)?
            }
            "job_input" => {
                tables_count += 1;
                hash_table(tx, JOB_INPUTS, &mut tables)?
            }
            "notice" => {
                tables_count += 1;
                hash_table(tx, NOTICES, &mut tables)?
            }
            "approval" => {
                tables_count += 1;
                hash_table(tx, APPROVALS, &mut tables)?
            }
            "recurrence" => {
                tables_count += 1;
                hash_table(tx, RECURRENCES, &mut tables)?
            }
            "job_observation" => {
                tables_count += 1;
                hash_table(tx, JOB_OBSERVATIONS, &mut tables)?
            }
            "content_reference" => {
                tables_count += 1;
                hash_table(tx, CONTENT_REFERENCES, &mut tables)?
            }
            "content_reclaim" => {
                tables_count += 1;
                hash_table(tx, CONTENT_RECLAIM, &mut tables)?
            }
            "active_allocation" => {
                tables_count += 1;
                hash_table(tx, ACTIVE_ALLOCATIONS, &mut tables)?
            }
            "allocation_request" => {
                tables_count += 1;
                hash_table(tx, ALLOCATION_REQUESTS, &mut tables)?
            }
            "draft_context" => {
                tables_count += 1;
                hash_table(tx, DRAFT_CONTEXTS, &mut tables)?
            }
            "job_allocation" => {
                tables_count += 1;
                hash_table(tx, JOB_ALLOCATIONS, &mut tables)?
            }
            "conversation" => {
                tables_count += 1;
                hash_table(tx, CONVERSATIONS, &mut tables)?
            }
            "turn" => {
                tables_count += 1;
                hash_table(tx, TURNS, &mut tables)?
            }
            "pending" => {
                tables_count += 1;
                hash_table(tx, PENDING, &mut tables)?
            }
            "ready" => {
                tables_count += 1;
                hash_table(tx, MUTABLE_READY, &mut tables)?
            }
            "delayed" => {
                tables_count += 1;
                hash_table(tx, DELAYED, &mut tables)?
            }
            "dependency_waiter" => {
                tables_count += 1;
                hash_table(tx, DEPENDENCY_WAITERS, &mut tables)?
            }
            "unsettled" => {
                tables_count += 1;
                hash_table(tx, UNSETTLED, &mut tables)?
            }
            "held_slot" => {
                tables_count += 1;
                hash_table(tx, HELD_SLOTS, &mut tables)?
            }
            "children" => {
                tables_count += 1;
                hash_table(tx, CHILDREN, &mut tables)?
            }
            "successor" => {
                tables_count += 1;
                hash_table(tx, SUCCESSORS, &mut tables)?
            }
            "conversation_search" => {
                tables_count += 1;
                hash_table(tx, CONVERSATION_SEARCH, &mut tables)?
            }
            "child_group" => {
                tables_count += 1;
                hash_table(tx, CHILD_GROUPS, &mut tables)?
            }
            "live_child" => {
                tables_count += 1;
                hash_table(tx, LIVE_CHILDREN, &mut tables)?
            }
            "user_input_at" => {
                tables_count += 1;
                hash_table(tx, USER_INPUT_AT, &mut tables)?
            }
            "active_job" => {
                tables_count += 1;
                hash_table(tx, ACTIVE_JOBS, &mut tables)?
            }
            "group_result" => {
                tables_count += 1;
                hash_table(tx, GROUP_RESULTS, &mut tables)?
            }
            "notice_read" => {
                tables_count += 1;
                hash_table(tx, NOTICE_READ, &mut tables)?
            }
            "tool_call_id" => {
                tables_count += 1;
                hash_table(tx, TOOL_CALL_IDS, &mut tables)?
            }
            "tool_stat" => {
                tables_count += 1;
                hash_table(tx, TOOL_STATS, &mut tables)?
            }
            "usage_total" => {
                tables_count += 1;
                hash_table(tx, USAGE_TOTALS, &mut tables)?
            }
            "retry" => {
                tables_count += 1;
                hash_table(tx, RETRIES, &mut tables)?
            }
            "usage_model" => {
                tables_count += 1;
                hash_table(tx, USAGE_MODEL, &mut tables)?
            }
            "request_timeline" => {
                tables_count += 1;
                hash_table(tx, REQUEST_TIMELINE, &mut tables)?
            }
            "open_approval" => {
                tables_count += 1;
                hash_table(tx, OPEN_APPROVALS, &mut tables)?
            }
            "approval_call" => {
                tables_count += 1;
                hash_table(tx, APPROVAL_CALLS, &mut tables)?
            }
            "recurrence_due" => {
                tables_count += 1;
                hash_table(tx, RECURRENCE_DUE, &mut tables)?
            }
            other => {
                return Err(format!(
                    "unexpected table `{other}` in a format {MIGRATION_SOURCE_FORMAT} database"
                )
                .into());
            }
        });
    }
    Ok(MigrationDigest {
        tables: <[u8; 32]>::try_from(tables.finish().as_ref())
            .map_err(|_| "table digest is malformed")?,
        meta: <[u8; 32]>::try_from(meta.finish().as_ref())
            .map_err(|_| "metadata digest is malformed")?,
        rows,
        tables_count,
    })
}

fn hash_table<K, V>(
    tx: &ReadTransaction,
    definition: TableDefinition<'_, K, V>,
    hash: &mut Context,
) -> Result<u64, Box<dyn std::error::Error>>
where
    K: ::redb::Key + 'static,
    V: ::redb::Value + 'static,
{
    use ::redb::{ReadableTable, TableHandle};
    let name = definition.name().to_owned();
    let table = tx.open_table(definition)?;
    hash.update(name.as_bytes());
    hash.update(b"\0");
    let mut rows = 0u64;
    for entry in table.iter()? {
        let (key, value) = entry?;
        hash.update(K::as_bytes(&key.value()).as_ref());
        hash.update(b"\0");
        hash.update(V::as_bytes(&value.value()).as_ref());
        hash.update(b"\0");
        rows = rows.saturating_add(1);
    }
    Ok(rows)
}

fn hash_meta(tx: &ReadTransaction, hash: &mut Context) -> Result<u64, Box<dyn std::error::Error>> {
    use ::redb::ReadableTable;
    let table = tx.open_table(META)?;
    let mut rows = 0u64;
    for entry in table.iter()? {
        let (key, value) = entry?;
        let key = key.value();
        if key == DATABASE_FORMAT_KEY || key == SCHEMA_FINGERPRINT_KEY {
            continue;
        }
        hash.update(key.as_bytes());
        hash.update(b"\0");
        hash.update(value.value());
        hash.update(b"\0");
        rows = rows.saturating_add(1);
    }
    Ok(rows)
}
