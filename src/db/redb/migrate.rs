//! The offline migration framework. Format 1 is the first database format, so
//! no migration step exists yet: [`STEPS`] is empty, and the framework refuses
//! any format but the current one with that reason.
//!
//! The framework is what a future format change plugs into. `ocg migrate`
//! inspects a stopped database read-only, copies the data directory to a
//! private staging directory, applies the one [`Step`] that reads the source's
//! format to the copy in a single write transaction, verifies the result
//! against the source, and publishes it with an atomic no-replace rename. The
//! source is opened read-only and never modified.
//!
//! A step is verified by logical identity: [`digest_tables`] hashes every row of
//! every table, and the migrated copy must hash exactly like the source. That
//! suits an additive step. A step that adds tables or rewrites rows must extend
//! [`digest_tables`] and the comparison in [`apply`] to say what changed.
//!
//! Any other format is refused rather than reinterpreted.

use std::collections::BTreeSet;
use std::path::Path;

use aws_lc_rs::digest::{Context, SHA256};

use super::*;

type StepResult = Result<(), Box<dyn std::error::Error>>;

/// One migration step: it reads format [`Step::from`] and writes the current
/// format. The framework runs exactly one step; a chain of formats is a
/// chain of `ocg migrate` runs.
pub(super) struct Step {
    /// The format this step reads.
    pub(super) from: u64,
    /// The schema fingerprint the source must carry.
    pub(super) fingerprint: &'static [u8],
    /// Read-only checks of the source, including its own record encoding: the
    /// source's records are not the current format's, so the framework reads
    /// none of them.
    pub(super) check: fn(&ReadTransaction, &BTreeSet<String>) -> StepResult,
    /// Applies the change to the private copy, inside the one write transaction
    /// that also writes the new format marker and fingerprint.
    pub(super) apply: fn(&::redb::WriteTransaction) -> StepResult,
}

/// Every migration this build can run. Empty: format 1 is the first format.
pub(super) const STEPS: &[Step] = &[];

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
    /// A format a [`Step`] reads, with a verified logical snapshot.
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
/// reports redb's `Database already open` instead of reading under it. The
/// handle is released on return; use [`HeldSource`] for a source that must stay
/// excluded from writers.
pub(crate) fn inspect(path: &Path) -> Result<MigrationInspection, Box<dyn std::error::Error>> {
    HeldSource::open(path)?.inspect()
}

/// A source database opened read-only and kept open. redb's file lock is
/// shared while the handle lives, so no writer can open the file between
/// inspection, copy, verification and publication. The caller also holds the
/// data directory's host lock for the same span.
pub(crate) struct HeldSource(::redb::ReadOnlyDatabase);

impl HeldSource {
    pub(crate) fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if std::fs::metadata(path)?.len() == 0 {
            return Err("database is empty/uninitialized; start OCG to initialize it".into());
        }
        Ok(Self(::redb::ReadOnlyDatabase::open(path)?))
    }

    /// Reads the held source again, in a new read transaction.
    pub(crate) fn inspect(&self) -> Result<MigrationInspection, Box<dyn std::error::Error>> {
        inspect_database(&self.0)
    }
}

fn inspect_database(
    database: &impl ::redb::ReadableDatabase,
) -> Result<MigrationInspection, Box<dyn std::error::Error>> {
    let tx = database.begin_read()?;
    let names = table_names(&tx)?;
    let meta = tx.open_table(META)?;
    let format = read_format(&meta)?;
    let fingerprint = read_fingerprint(&meta)?;
    if format == DATABASE_FORMAT_VERSION {
        if fingerprint.as_slice() != SCHEMA_FINGERPRINT {
            return Err(format!(
                "database format {} has an unexpected schema fingerprint; this build opens only `{}`",
                format,
                String::from_utf8_lossy(SCHEMA_FINGERPRINT)
            )
            .into());
        }
        check_metadata(&meta)?;
        require_tables(&names)?;
        return Ok(MigrationInspection::Current);
    }
    let Some(step) = STEPS.iter().find(|step| step.from == format) else {
        return Err(format!(
            "database format {format} is not supported; this build opens only format {DATABASE_FORMAT_VERSION} and has no migration from it"
        )
        .into());
    };
    if fingerprint.as_slice() != step.fingerprint {
        return Err(format!(
            "database format {format} has an unexpected schema fingerprint; this build migrates only the format {format} database it wrote itself"
        )
        .into());
    }
    (step.check)(&tx, &names)?;
    let digest = digest_tables(&tx, &names)?;
    Ok(MigrationInspection::Upgrade(MigrationPlan {
        from: format,
        to: DATABASE_FORMAT_VERSION,
        digest,
    }))
}

/// Applies the plan to the private copy at `path` in one immediate write
/// transaction, then verifies that every logical row, every metadata value
/// other than the two identity markers, and the table set are unchanged.
pub(crate) fn apply(path: &Path, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    let step = STEPS
        .iter()
        .find(|step| step.from == plan.from)
        .ok_or("no migration step reads this format")?;
    let database = ::redb::Database::open(path)?;
    {
        let tx = database.begin_read()?;
        let names = table_names(&tx)?;
        let digest = digest_tables(&tx, &names)?;
        if digest != plan.digest {
            return Err("copied database does not match the source".into());
        }
    }
    let mut tx = database.begin_write()?;
    tx.set_durability(::redb::Durability::Immediate)?;
    (step.apply)(&tx)?;
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
        require_tables(&names)?;
        let digest = digest_tables(&tx, &names)?;
        if digest != plan.digest {
            return Err("migrated authoritative data differs from the source".into());
        }
        let meta = tx.open_table(META)?;
        check_metadata(&meta)?;
    }
    // Not merely the marker: the checks a host's startup preflight applies,
    // plus every record header and native invariant. The copy is read, never
    // opened by a host, so no epoch or shutdown marker changes and no ownership
    // is released, adopted or cleared.
    recovery::verify_migrated(&database)?;
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
        None => Err(format!(
            "unmarked database; this build opens only format {DATABASE_FORMAT_VERSION}"
        )
        .into()),
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

/// Every table the current format defines.
fn require_tables(names: &BTreeSet<String>) -> Result<(), Box<dyn std::error::Error>> {
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
        "scheduled_external_policy",
        "scheduled_external_binding",
    ] {
        if !names.iter().any(|name| name == required) {
            return Err(format!(
                "database lacks the {required} table this format defines; it is not a complete format {DATABASE_FORMAT_VERSION} database"
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
                tables_count += 1;
                hash_table(tx, SCHEDULED_EXTERNAL_POLICY, &mut tables)?
            }
            "scheduled_external_binding" => {
                tables_count += 1;
                hash_table(tx, SCHEDULED_EXTERNAL_BINDING, &mut tables)?
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
                    "unexpected table `{other}` in a format {DATABASE_FORMAT_VERSION} database"
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

/// One length-prefixed, domain-tagged field of the logical digest. Raw keys and
/// values are arbitrary bytes, so a separator byte could not delimit them: the
/// explicit length makes every framing unambiguous. The digest is internal and
/// never stored.
fn frame(hash: &mut Context, domain: u8, bytes: &[u8]) {
    hash.update(&[domain]);
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

const TABLE_NAME: u8 = b'N';
const TABLE_KEY: u8 = b'K';
const TABLE_VALUE: u8 = b'V';
const META_KEY: u8 = b'm';
const META_VALUE: u8 = b'v';

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
    frame(hash, TABLE_NAME, name.as_bytes());
    let mut rows = 0u64;
    for entry in table.iter()? {
        let (key, value) = entry?;
        frame(hash, TABLE_KEY, K::as_bytes(&key.value()).as_ref());
        frame(hash, TABLE_VALUE, V::as_bytes(&value.value()).as_ref());
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
        frame(hash, META_KEY, key.as_bytes());
        frame(hash, META_VALUE, value.value());
        rows = rows.saturating_add(1);
    }
    Ok(rows)
}
