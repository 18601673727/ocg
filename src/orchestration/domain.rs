//! SQLite-backed canonical Project, Job, Attempt, Executor and Call records.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{self, BudgetConfig, QuotaFacts, SpendAction, SpendAssessment};
use crate::orchestration::journal::{
    self, EventDelta, EventDraft, EventKind, ExecutionEvent, ExecutionSnapshot, JournalBoundary,
    JournalPrune,
};
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::{DiGraph, NodeIndex};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use strum::{Display, EnumString};

/// Call states that are still unsettled. The fencing cascades and the journal
/// use exactly the same live set, so a record is emitted for every row the SQL
/// actually changed.
const LIVE_CALL_STATES: &[&str] = &["created", "running"];
/// DispatchIntent states that are not terminally settled.
const LIVE_INTENT_STATES: &[&str] = &["pending", "queued", "running"];

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

fn sql(error: rusqlite::Error) -> OcgError {
    OcgError::config(format!("canonical domain SQLite: {error}"))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7())
}

fn validate_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 160 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(invalid("invalid canonical domain identifier"));
    }
    Ok(())
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name=?2)",
            params![table, column],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if !exists {
        connection
            .execute(
                &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
                [],
            )
            .map_err(sql)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub root: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub project_id: String,
    pub state: JobState,
    pub generation: u64,
    pub authoritative_attempt_id: Option<String>,
    pub payload: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum JobState {
    Pending,
    Eligible,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Unknown,
    Orphaned,
}

impl JobState {
    fn parse(value: &str) -> Result<Self> {
        value
            .parse()
            .map_err(|_| invalid("invalid canonical Job state"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub id: String,
    pub job_id: String,
    pub generation: u64,
    pub state: AttemptState,
    pub authoritative: bool,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AttemptState {
    Queued,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Unknown,
    Orphaned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Executor {
    pub id: String,
    pub attempt_id: String,
    pub kind: String,
    pub state: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Call {
    pub id: String,
    pub attempt_id: String,
    pub executor_id: Option<String>,
    pub generation: u64,
    pub side_effect: bool,
    pub effect_kind: EffectIntentKind,
    pub state: String,
    pub request: String,
    pub response: Option<String>,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EffectIntentKind {
    Idempotent,
    StrictFenced,
    Reconcilable,
    NonRetryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EffectIntentState {
    NotStarted,
    Started,
    Settled,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchIntent {
    pub id: String,
    pub call_id: String,
    pub job_id: String,
    pub attempt_id: String,
    pub executor_id: Option<String>,
    pub generation: u64,
    pub state: String,
    pub effect_kind: EffectIntentKind,
    pub effect_state: EffectIntentState,
    pub request: String,
    pub reservation_id: Option<String>,
    pub budget_admitted: bool,
    pub failure: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptAuthority {
    pub attempt_id: String,
    pub job_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
pub struct ExecutionWitness {
    pub job_id: String,
    pub attempt_id: String,
    pub executor_id: String,
    pub call_id: String,
    pub generation: u64,
}

impl ExecutionWitness {
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(value.clone())
            .map_err(|_| invalid("invalid canonical execution witness"))
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdmission {
    pub project: Project,
    pub job: Job,
    pub attempt: Attempt,
    pub executor: Executor,
}

const DOMAIN_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS domain_projects (
    id TEXT PRIMARY KEY,
    root TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_jobs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    state TEXT NOT NULL CHECK(state IN ('pending','eligible','running','cancelling','completed','failed','cancelled','unknown','orphaned')),
    generation INTEGER NOT NULL CHECK(generation >= 0),
    authoritative_attempt_id TEXT,
    payload TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    FOREIGN KEY(authoritative_attempt_id) REFERENCES domain_attempts(id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE IF NOT EXISTS domain_job_bindings (
    binding_key TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    job_id TEXT NOT NULL UNIQUE REFERENCES domain_jobs(id),
    created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_attempts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES domain_jobs(id),
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('queued','running','cancelling','completed','failed','cancelled','unknown','orphaned')),
    authoritative INTEGER NOT NULL CHECK(authoritative IN (0,1)),
    created_at INTEGER NOT NULL,
    finished_at INTEGER,
    UNIQUE(job_id,generation)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS domain_one_authority_per_job
    ON domain_attempts(job_id) WHERE authoritative=1;
CREATE TABLE IF NOT EXISTS domain_executors (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_calls (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    executor_id TEXT REFERENCES domain_executors(id),
    generation INTEGER NOT NULL CHECK(generation > 0),
    side_effect INTEGER NOT NULL CHECK(side_effect IN (0,1)),
    state TEXT NOT NULL,
    request TEXT NOT NULL,
    response TEXT,
    created_at INTEGER NOT NULL,
    finished_at INTEGER
) STRICT;
CREATE TABLE IF NOT EXISTS domain_dependency_revisions (
    project_id TEXT PRIMARY KEY REFERENCES domain_projects(id),
    revision INTEGER NOT NULL CHECK(revision >= 0)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_job_dependencies (
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    job_id TEXT NOT NULL REFERENCES domain_jobs(id),
    prerequisite_job_id TEXT NOT NULL REFERENCES domain_jobs(id),
    CHECK(job_id != prerequisite_job_id),
    PRIMARY KEY(project_id,job_id,prerequisite_job_id)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_job_origins (
    job_id TEXT PRIMARY KEY REFERENCES domain_jobs(id),
    parent_job_id TEXT NOT NULL REFERENCES domain_jobs(id),
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    generation INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_dispatch_intents (
    id TEXT PRIMARY KEY,
    call_id TEXT NOT NULL UNIQUE REFERENCES domain_calls(id),
    job_id TEXT NOT NULL REFERENCES domain_jobs(id),
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    executor_id TEXT REFERENCES domain_executors(id),
    generation INTEGER NOT NULL CHECK(generation > 0),
    state TEXT NOT NULL CHECK(state IN ('pending','queued','running','completed','failed','fenced')),
    effect_kind TEXT NOT NULL CHECK(effect_kind IN ('idempotent','strict_fenced','reconcilable','non_retryable')),
    effect_state TEXT NOT NULL CHECK(effect_state IN ('not_started','started','settled','unknown')),
    request TEXT NOT NULL,
    reservation_id TEXT,
    budget_admitted INTEGER NOT NULL DEFAULT 0 CHECK(budget_admitted IN (0,1)),
    failure TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_budgets (
    project_id TEXT PRIMARY KEY REFERENCES domain_projects(id),
    budget TEXT NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_result_evidence (
    call_id TEXT NOT NULL REFERENCES domain_calls(id),
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    generation INTEGER NOT NULL,
    response TEXT NOT NULL,
    disposition TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(call_id,attempt_id,generation,response,disposition)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_verifications (
    call_id TEXT PRIMARY KEY REFERENCES domain_calls(id),
    passed INTEGER NOT NULL,
    report TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS domain_dispatch_recovery
    ON domain_dispatch_intents(state,created_at);
CREATE TABLE IF NOT EXISTS domain_job_configs (
    job_id TEXT PRIMARY KEY REFERENCES domain_jobs(id),
    configuration TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 0),
    updated_at INTEGER NOT NULL
) STRICT;
"#;

/// In-memory dependency view. The SQLite revision determines whether it is current.
#[derive(Debug)]
pub struct DependencyProjection {
    pub project_id: String,
    pub revision: u64,
    graph: DiGraph<String, ()>,
    nodes: HashMap<String, NodeIndex>,
}

impl DependencyProjection {
    pub fn is_acyclic(&self) -> bool {
        !is_cyclic_directed(&self.graph)
    }

    pub fn prerequisites(&self, job_id: &str) -> Vec<String> {
        let Some(&node) = self.nodes.get(job_id) else {
            return Vec::new();
        };
        self.graph
            .neighbors_directed(node, petgraph::Direction::Incoming)
            .filter_map(|neighbor| self.graph.node_weight(neighbor).cloned())
            .collect()
    }
}

/// Repository for the canonical domain tables in the project SQLite database.
pub struct DomainRepository {
    connection: Connection,
    path: PathBuf,
}

impl DomainRepository {
    pub fn open(root: &Path) -> Result<Self> {
        crate::runtime::install::ensure_gitignore(root)?;
        let path = crate::orchestration::state::state_dir(root).join("substrate.sqlite3");
        let parent = path
            .parent()
            .ok_or_else(|| invalid("canonical database has no parent directory"))?;
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io("create canonical database directory", error))?;
        let connection = Connection::open(&path).map_err(sql)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sql)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(sql)?;
        connection.execute_batch(DOMAIN_SCHEMA).map_err(sql)?;
        // The durable execution event journal shares this database so a state
        // change and its event commit in one transaction. It is evidence, never
        // a decision input: nothing in this file reads it back to choose an
        // execution outcome.
        journal::ensure_schema(&connection)?;
        ensure_column(
            &connection,
            "domain_dispatch_intents",
            "reservation_id",
            "TEXT",
        )?;
        ensure_column(
            &connection,
            "domain_dispatch_intents",
            "budget_admitted",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_column(&connection, "domain_job_bindings", "attempt_id", "TEXT")?;
        connection.execute(
            "UPDATE domain_job_bindings SET attempt_id=(SELECT a.id FROM domain_attempts a WHERE a.job_id=domain_job_bindings.job_id ORDER BY a.generation DESC LIMIT 1) WHERE attempt_id IS NULL",
            [],
        ).map_err(sql)?;
        connection
            .execute(
                "INSERT INTO domain_dispatch_intents(id,call_id,job_id,attempt_id,executor_id,generation,state,effect_kind,effect_state,request,reservation_id,budget_admitted,failure,created_at,updated_at) SELECT 'intent-' || c.id,c.id,a.job_id,c.attempt_id,c.executor_id,c.generation,CASE WHEN c.state='created' THEN 'pending' WHEN c.state='running' THEN 'running' ELSE 'completed' END,CASE WHEN c.side_effect=1 THEN 'strict_fenced' ELSE 'idempotent' END,CASE WHEN c.state='running' THEN 'unknown' ELSE 'settled' END,c.request,NULL,1,NULL,c.created_at,COALESCE(c.finished_at,c.created_at) FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id WHERE NOT EXISTS(SELECT 1 FROM domain_dispatch_intents i WHERE i.call_id=c.id)",
                [],
            )
            .map_err(sql)?;
        let repository = Self { connection, path };
        repository.ensure_project(root)?;
        Ok(repository)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Begin the canonical writer transaction.
    ///
    /// Every lifecycle mutation runs inside one such transaction together with
    /// its journal appends, so a canonical change and its event are atomic.
    /// It is taken on `&self` so an immutable mutation method can still commit
    /// state and evidence together.
    fn begin(&self) -> Result<rusqlite::Transaction<'_>> {
        journal::begin(&self.connection)
    }

    // ---------------------------------------------------------------------
    // Durable execution event journal (read side)
    // ---------------------------------------------------------------------

    /// The journal head: the cursor of the most recent committed event, or
    /// [`journal::INITIAL_CURSOR`] when nothing has been journalized.
    pub fn journal_head(&self) -> Result<u64> {
        journal::head(&self.connection)
    }

    /// The durable retention boundary: head, floor and anchor.
    pub fn journal_boundary(&self) -> Result<JournalBoundary> {
        journal::boundary(&self.connection)
    }

    /// The lowest cursor still retained. `0` means nothing has been pruned.
    pub fn journal_floor(&self) -> Result<u64> {
        Ok(journal::boundary(&self.connection)?.floor_cursor)
    }

    /// Read the committed events strictly after `after`, in cursor order.
    pub fn events_after(&self, after: u64, limit: usize) -> Result<Vec<ExecutionEvent>> {
        journal::read_after(&self.connection, after, limit)
    }

    /// Read one Job's event stream strictly after `after`, in cursor order.
    pub fn events_for_job(
        &self,
        job_id: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ExecutionEvent>> {
        journal::read_for_job(&self.connection, job_id, after, limit)
    }

    /// Ask for the complete delta after `cursor`.
    ///
    /// The outcome is always explicit: the complete delta, an empty delta at
    /// the head, [`EventDelta::ResyncRequired`] when the cursor fell below the
    /// retained floor, or [`EventDelta::AheadOfHead`] when the cursor cannot
    /// name a real position. A partial suffix is never returned.
    pub fn journal_delta(&self, cursor: u64, limit: usize) -> Result<EventDelta> {
        journal::delta_after(&self.connection, cursor, limit)
    }

    /// The same boundary-checked delta, filtered to one Job.
    pub fn journal_delta_for_job(
        &self,
        job_id: &str,
        cursor: u64,
        limit: usize,
    ) -> Result<EventDelta> {
        journal::delta_for_job(&self.connection, job_id, cursor, limit)
    }

    /// Explicitly drop retained events below `keep_from` and move the floor.
    ///
    /// This is the only way the journal loses an event. It changes no canonical
    /// state, never moves the head, and never re-issues a cursor: the numbering
    /// comes from the durable boundary counter, which pruning does not touch.
    pub fn prune_journal(&mut self, keep_from: u64) -> Result<JournalPrune> {
        journal::prune(&self.connection, keep_from)
    }

    /// Capture the canonical rows and the journal head in one read transaction.
    ///
    /// This is the base a projection rebuild starts from: the cursor, the floor
    /// and the rows are read together, so a consumer can never pair pre-commit
    /// rows with a post-commit cursor.
    pub fn execution_snapshot(&self) -> Result<ExecutionSnapshot> {
        let transaction = journal::begin_read(&self.connection)?;
        let view: &Connection = &transaction;
        let boundary = journal::boundary(view)?;
        let snapshot = ExecutionSnapshot {
            cursor: boundary.head_cursor,
            floor_cursor: boundary.floor_cursor,
            anchor_digest: boundary.anchor_digest,
            projects: all_projects(view)?,
            jobs: all_jobs(view)?,
            attempts: all_attempts(view)?,
            executors: all_executors(view)?,
            calls: all_calls(view)?,
            dispatch_intents: all_dispatch_intents(view)?,
            dependencies: all_dependencies(view)?,
            bindings: all_bindings(view)?,
            job_configurations: all_job_configurations(view)?,
            result_evidence: all_result_evidence(view)?,
            verifications: all_verifications(view)?,
        };
        transaction.commit().map_err(sql)?;
        Ok(snapshot)
    }

    pub fn project_budget(&self, project_id: &str) -> Result<budget::MissionBudgetReceipt> {
        let raw: Option<String> = self
            .connection
            .query_row(
                "SELECT budget FROM domain_budgets WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        let ledger = raw
            .map(|raw| {
                serde_json::from_str::<budget::MissionBudget>(&raw)
                    .map_err(|error| invalid(&format!("invalid Project budget: {error}")))
            })
            .transpose()?
            .unwrap_or_default();
        Ok(ledger.receipt())
    }

    pub fn set_project_budget(&mut self, project_id: &str, amount: budget::Money) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let raw: Option<String> = transaction
            .query_row(
                "SELECT budget FROM domain_budgets WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        let mut ledger = raw
            .map(|raw| {
                serde_json::from_str::<budget::MissionBudget>(&raw)
                    .map_err(|error| invalid(&format!("invalid Project budget: {error}")))
            })
            .transpose()?
            .unwrap_or_default();
        let changed = ledger.set_hard_limit(amount, now())?;
        let serialized =
            serde_json::to_string(&ledger).map_err(|error| invalid(&error.to_string()))?;
        transaction.execute(
            "INSERT INTO domain_budgets(project_id,budget,updated_at) VALUES(?1,?2,?3) ON CONFLICT(project_id) DO UPDATE SET budget=excluded.budget,updated_at=excluded.updated_at",
            params![project_id,serialized,now()],
        ).map_err(sql)?;
        transaction.commit().map_err(sql)?;
        Ok(changed)
    }

    pub fn validate_execution_witness(&self, witness: &ExecutionWitness) -> Result<Call> {
        let call = self.call(&witness.call_id)?;
        let attempt = self
            .attempt(&witness.attempt_id)?
            .ok_or_else(|| invalid("unknown Attempt"))?;
        if call.attempt_id != witness.attempt_id
            || call.executor_id.as_deref() != Some(&witness.executor_id)
            || call.generation != witness.generation
            || attempt.job_id != witness.job_id
            || attempt.generation != witness.generation
        {
            return Err(invalid(
                "execution witness does not match canonical identity",
            ));
        }
        Ok(call)
    }

    pub fn witness_for_call(
        &self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
    ) -> Result<ExecutionWitness> {
        let call = self.call(call_id)?;
        let attempt = self
            .attempt(attempt_id)?
            .ok_or_else(|| invalid("unknown Attempt"))?;
        let witness = ExecutionWitness {
            job_id: attempt.job_id,
            attempt_id: attempt_id.to_string(),
            generation,
            executor_id: call
                .executor_id
                .ok_or_else(|| invalid("Call has no Executor"))?,
            call_id: call_id.to_string(),
        };
        self.validate_execution_witness(&witness)?;
        Ok(witness)
    }

    pub fn deliver_result(
        &mut self,
        witness: &ExecutionWitness,
        response: &str,
        succeeded: bool,
    ) -> Result<&'static str> {
        let generation = i64::try_from(witness.generation)
            .map_err(|_| invalid("generation exceeds SQLite range"))?;
        let call = self.validate_execution_witness(witness)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let (state, stored_response, current): (String, Option<String>, bool) = transaction.query_row(
            "SELECT c.state,c.response,a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.generation=c.generation AND a.state IN ('queued','running') FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id JOIN domain_jobs j ON j.id=a.job_id WHERE c.id=?1",
            [&call.id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))
        ).map_err(sql)?;
        let duplicate = state == "completed" && stored_response.as_deref() == Some(response);
        if !duplicate && (!current || state != "running") {
            let recorded = transaction.execute(
                "INSERT OR IGNORE INTO domain_result_evidence(call_id,attempt_id,generation,response,disposition,created_at) VALUES(?1,?2,?3,?4,'late',?5)",
                params![witness.call_id,witness.attempt_id,generation,response,now()],
            ).map_err(sql)?;
            // A late result is durable evidence, not canonical state. It is
            // journalized as evidence so the rejection itself is auditable,
            // naming the fenced authority it was refused under. Redelivering
            // the same result retains the existing evidence and records nothing.
            if recorded == 1 {
                let stored: i64 = transaction
                    .query_row(
                        "SELECT created_at FROM domain_result_evidence WHERE call_id=?1 AND attempt_id=?2 AND generation=?3 AND response=?4 AND disposition='late'",
                        params![witness.call_id, witness.attempt_id, generation, response],
                        |row| row.get(0),
                    )
                    .map_err(sql)?;
                emit_result_evidence(
                    &transaction,
                    &journal::ResultEvidence {
                        call_id: witness.call_id.clone(),
                        attempt_id: witness.attempt_id.clone(),
                        generation: witness.generation,
                        disposition: "late".to_string(),
                        created_at: stored,
                    },
                    &witness.job_id,
                    &witness.executor_id,
                )?;
            }
            transaction.commit().map_err(sql)?;
            return Ok("late_evidence");
        }
        finish_call_in(
            &transaction,
            &witness.call_id,
            &witness.attempt_id,
            witness.generation,
            response,
        )?;
        if current {
            finish_attempt_in(
                &transaction,
                &witness.attempt_id,
                if succeeded { "completed" } else { "failed" },
                false,
            )?;
        }
        transaction.commit().map_err(sql)?;
        Ok(if duplicate {
            "duplicate"
        } else {
            "authoritative"
        })
    }

    pub fn record_verification(
        &mut self,
        witness: &ExecutionWitness,
        passed: bool,
        report: &serde_json::Value,
    ) -> Result<()> {
        self.validate_execution_witness(witness)?;
        let report = serde_json::to_string(report).map_err(|error| invalid(&error.to_string()))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction.execute(
            "INSERT INTO domain_verifications(call_id,passed,report,created_at) SELECT ?1,?2,?3,?4 WHERE EXISTS(SELECT 1 FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id JOIN domain_jobs j ON j.id=a.job_id WHERE c.id=?1 AND c.state='running' AND a.authoritative=1 AND j.authoritative_attempt_id=a.id) ON CONFLICT(call_id) DO NOTHING",
            params![witness.call_id,passed,report,now()],
        ).map_err(sql)?;
        if changed == 0 {
            if read_verification(&transaction, &witness.call_id)?.is_none() {
                return Err(invalid("verification rejected: Attempt authority is stale"));
            }
            // Already recorded: the fact is unchanged, so it records nothing.
            transaction.commit().map_err(sql)?;
            return Ok(());
        }
        let verification = read_verification(&transaction, &witness.call_id)?
            .ok_or_else(|| invalid("recorded verification disappeared"))?;
        emit_verification(
            &transaction,
            &verification,
            &witness.job_id,
            &witness.attempt_id,
            witness.generation,
            &witness.executor_id,
        )?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    pub fn verification(&self, call_id: &str) -> Result<Option<(bool, serde_json::Value)>> {
        let row: Option<(bool, String)> = self
            .connection
            .query_row(
                "SELECT passed,report FROM domain_verifications WHERE call_id=?1",
                [call_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        row.map(|(passed, report)| {
            Ok((
                passed,
                serde_json::from_str(&report).map_err(|error| invalid(&error.to_string()))?,
            ))
        })
        .transpose()
    }

    pub fn ready_jobs(&self, project_id: &str) -> Result<Vec<Job>> {
        self.jobs(project_id)?
            .into_iter()
            .filter(|job| matches!(job.state, JobState::Pending | JobState::Eligible))
            .filter_map(
                |job| match self.dependencies_satisfied(project_id, &job.id) {
                    Ok(true) => Some(Ok(job)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect()
    }

    pub fn inspect_job(&self, job_id: &str) -> Result<serde_json::Value> {
        let job = self.job(job_id)?.ok_or_else(|| invalid("unknown Job"))?;
        let attempts = self.attempts_for_job(job_id)?;
        let mut executors = Vec::new();
        let mut calls = Vec::new();
        for attempt in &attempts {
            executors.extend(self.executor_for_attempt(&attempt.id)?);
            calls.extend(self.calls_for_attempt(&attempt.id)?);
        }
        let pending = self
            .pending_dispatch_intents()?
            .into_iter()
            .filter(|intent| intent.job_id == job_id)
            .collect::<Vec<_>>();
        let mut statement = self.connection.prepare("SELECT e.call_id,e.attempt_id,e.generation,e.disposition,e.created_at FROM domain_result_evidence e JOIN domain_attempts a ON a.id=e.attempt_id WHERE a.job_id=?1 ORDER BY e.created_at").map_err(sql)?;
        let evidence = statement.query_map([job_id], |row| Ok(serde_json::json!({
            "call_id":row.get::<_, String>(0)?,"attempt_id":row.get::<_, String>(1)?,
            "generation":row.get::<_, i64>(2)?,"disposition":row.get::<_, String>(3)?,"created_at":row.get::<_, i64>(4)?
        }))).map_err(sql)?.collect::<std::result::Result<Vec<_>, _>>().map_err(sql)?;
        Ok(
            serde_json::json!({"job":job,"attempts":attempts,"executors":executors,"calls":calls,"pending_dispatch_intents":pending,"result_evidence":evidence}),
        )
    }

    pub fn reconcile_dispatches(&mut self) -> Result<serde_json::Value> {
        let mut fenced = Vec::new();
        for intent in self.pending_dispatch_intents()? {
            if self.authority(&intent.attempt_id)?.is_none_or(|authority| {
                authority.generation != intent.generation || authority.job_id != intent.job_id
            }) {
                self.fence_dispatch_intent(&intent.call_id, "stale_attempt_authority")?;
                fenced.push(intent.call_id);
            }
        }
        Ok(
            serde_json::json!({"fenced_calls":fenced,"pending_dispatch_intents":self.pending_dispatch_intents()?,"dispatch_owner":"executor"}),
        )
    }

    /// Evaluate and persist the canonical provider-spend admission. The
    /// budget is stored beside the Project/Job/Attempt authority so replay's
    /// Mission ledger cannot authorize a provider Call.
    pub fn admit_dispatch(
        &mut self,
        project_id: &str,
        generation: u64,
        operation_id: &str,
        config: &BudgetConfig,
        quota: QuotaFacts,
    ) -> Result<SpendAssessment> {
        validate_id(project_id)?;
        validate_id(operation_id)?;
        let generation_i32 = u32::try_from(generation)
            .map_err(|_| invalid("provider generation exceeds budget range"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let mut budget = transaction
            .query_row(
                "SELECT budget FROM domain_budgets WHERE project_id=?1",
                [project_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql)?
            .map(|value| {
                serde_json::from_str::<budget::MissionBudget>(&value).map_err(|error| {
                    OcgError::config(format!("invalid canonical Project budget: {error}"))
                })
            })
            .transpose()?
            .unwrap_or_default();
        let materialized = budget.materialize_config(config);
        let existing = budget
            .reservation_for(
                SpendAction::ProviderDispatch,
                project_id,
                generation_i32,
                operation_id,
            )
            .filter(|reservation| reservation.state != budget::ReservationState::Released)
            .map(|reservation| reservation.reservation_id.clone());
        let request = budget::SpendRequest {
            action: SpendAction::ProviderDispatch,
            operation_id,
            estimate: config.estimated_cost(),
            quota,
            already_reserved: existing.is_some(),
        };
        let mut assessment = budget::admit(&budget, config.require_quota, &request);
        let mut changed = materialized;
        if let Some(reservation_id) = existing {
            assessment.reservation_id = Some(reservation_id);
        }
        if assessment.is_allowed() {
            if let Some(amount) = assessment.amount.clone() {
                let reservation_id = budget::reservation_id(
                    SpendAction::ProviderDispatch,
                    project_id,
                    generation_i32,
                    operation_id,
                );
                changed |= budget.reserve(
                    SpendAction::ProviderDispatch,
                    project_id,
                    generation_i32,
                    operation_id,
                    amount,
                    now(),
                );
                assessment.reservation_id = Some(reservation_id);
            }
        }
        let reason_changed = budget.reason.as_deref() != Some(assessment.reason_code.as_str());
        budget.reason = Some(assessment.reason_code.clone());
        changed |= reason_changed;
        if changed {
            transaction
                .execute(
                    "INSERT INTO domain_budgets(project_id,budget,updated_at) VALUES(?1,?2,?3) ON CONFLICT(project_id) DO UPDATE SET budget=excluded.budget,updated_at=excluded.updated_at",
                    params![project_id, serde_json::to_string(&budget).map_err(|error| OcgError::config(format!("serialize canonical Project budget: {error}")))?, now()],
                )
                .map_err(sql)?;
        }
        if assessment.is_allowed() {
            let attached = transaction
                .execute(
                    "UPDATE domain_dispatch_intents SET reservation_id=?2,budget_admitted=1,updated_at=?3 WHERE call_id=?1 AND state='pending'",
                    params![operation_id, assessment.reservation_id.as_deref(), now()],
                )
                .map_err(sql)?;
            if attached != 1 {
                return Err(invalid(
                    "canonical dispatch intent disappeared during budget admission",
                ));
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(assessment)
    }

    /// Associate a budget reservation with its durable dispatch intent. This
    /// is idempotent so a retry cannot create a second economic obligation.
    pub fn attach_dispatch_reservation(
        &mut self,
        call_id: &str,
        reservation_id: Option<&str>,
    ) -> Result<()> {
        let changed = self
            .connection
            .execute(
                "UPDATE domain_dispatch_intents SET reservation_id=?2,budget_admitted=1,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued')",
                params![call_id, reservation_id, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is no longer attachable"));
        }
        Ok(())
    }

    pub fn mark_budget_admitted(&mut self, call_id: &str) -> Result<()> {
        let changed = self
            .connection
            .execute(
                "UPDATE domain_dispatch_intents SET budget_admitted=1,updated_at=?2 WHERE call_id=?1 AND state IN ('pending','queued')",
                params![call_id, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is no longer budget-admittable"));
        }
        Ok(())
    }

    /// Apply the terminal economic disposition exactly once. Unknown or
    /// provider-started work remains reserved as unresolved; only work proven
    /// not dispatched is released.
    pub fn settle_dispatch_budget(&mut self, call_id: &str, outcome: &str) -> Result<()> {
        if !matches!(
            outcome,
            "completed" | "failed" | "fenced" | "not_dispatched"
        ) {
            return Err(invalid("invalid dispatch budget outcome"));
        }
        let (project_id, generation, reservation_id): (String, i64, Option<String>) = self
            .connection
            .query_row(
                "SELECT j.project_id,i.generation,i.reservation_id FROM domain_dispatch_intents i JOIN domain_jobs j ON j.id=i.job_id WHERE i.call_id=?1",
                [call_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(sql)?;
        let Some(reservation_id) = reservation_id else {
            return Ok(());
        };
        let mut budget = self
            .connection
            .query_row(
                "SELECT budget FROM domain_budgets WHERE project_id=?1",
                [&project_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql)?
            .map(|value| {
                serde_json::from_str::<budget::MissionBudget>(&value).map_err(|error| {
                    OcgError::config(format!("invalid canonical Project budget: {error}"))
                })
            })
            .transpose()?
            .ok_or_else(|| invalid("dispatch reservation budget disappeared"))?;
        let changed = match outcome {
            "completed" => budget.settle(&reservation_id, None, now())?,
            "not_dispatched" => budget.release(&reservation_id, now()),
            "failed" | "fenced" => budget.mark_unresolved(&reservation_id, now()),
            _ => unreachable!(),
        };
        if changed {
            self.connection
                .execute(
                    "UPDATE domain_budgets SET budget=?2,updated_at=?3 WHERE project_id=?1",
                    params![
                        project_id,
                        serde_json::to_string(&budget).map_err(|error| OcgError::config(
                            format!("serialize canonical Project budget: {error}")
                        ))?,
                        now()
                    ],
                )
                .map_err(sql)?;
        }
        let _ = generation;
        Ok(())
    }

    pub fn jobs(&self, project_id: &str) -> Result<Vec<Job>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM domain_jobs WHERE project_id=?1 ORDER BY created_at,id")
            .map_err(sql)?;
        let rows = statement
            .query_map([project_id], |row| row.get::<_, String>(0))
            .map_err(sql)?;
        rows.map(|row| {
            let id = row.map_err(sql)?;
            self.job(&id)?
                .ok_or_else(|| invalid("Job disappeared while listing"))
        })
        .collect()
    }

    pub fn attempts_for_job(&self, job_id: &str) -> Result<Vec<Attempt>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM domain_attempts WHERE job_id=?1 ORDER BY generation,id")
            .map_err(sql)?;
        let rows = statement
            .query_map([job_id], |row| row.get::<_, String>(0))
            .map_err(sql)?;
        rows.map(|row| {
            let id = row.map_err(sql)?;
            self.attempt(&id)?
                .ok_or_else(|| invalid("Attempt disappeared while listing"))
        })
        .collect()
    }

    pub fn set_job_configuration(
        &mut self,
        job_id: &str,
        configuration: &serde_json::Value,
    ) -> Result<u64> {
        validate_id(job_id)?;
        if !configuration.is_object() {
            return Err(invalid("Job configuration must be an object"));
        }
        self.job(job_id)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let state: String = transaction
            .query_row(
                "SELECT state FROM domain_jobs WHERE id=?1",
                [job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !matches!(state.as_str(), "pending" | "eligible") {
            return Err(invalid(
                "Job configuration is frozen after execution admission",
            ));
        }
        let revision: i64 = transaction
            .query_row(
                "INSERT INTO domain_job_configs(job_id,configuration,revision,updated_at) VALUES(?1,?2,1,?3) ON CONFLICT(job_id) DO UPDATE SET configuration=excluded.configuration,revision=domain_job_configs.revision+1,updated_at=excluded.updated_at RETURNING revision",
                params![job_id, configuration.to_string(), now()],
                |row| row.get(0),
            )
            .map_err(sql)?;
        // The frozen configuration a future Attempt will be admitted against is
        // canonical state, so each revision is reconstructable from the journal.
        emit_job_configuration(
            &transaction,
            &journal::StoredJobConfiguration {
                job_id: job_id.to_string(),
                configuration: configuration.clone(),
                revision: u64::try_from(revision)
                    .map_err(|_| invalid("negative Job configuration revision"))?,
            },
        )?;
        transaction.commit().map_err(sql)?;
        u64::try_from(revision).map_err(|_| invalid("negative Job configuration revision"))
    }

    pub fn job_configuration(&self, job_id: &str) -> Result<Option<(serde_json::Value, u64)>> {
        self.connection
            .query_row(
                "SELECT configuration,revision FROM domain_job_configs WHERE job_id=?1",
                [job_id],
                |row| {
                    let configuration: String = row.get(0)?;
                    let revision: i64 = row.get(1)?;
                    Ok((configuration, revision))
                },
            )
            .optional()
            .map_err(sql)?
            .map(|(configuration, revision)| {
                let value = serde_json::from_str(&configuration)
                    .map_err(|error| invalid(&format!("invalid Job configuration: {error}")))?;
                Ok((
                    value,
                    u64::try_from(revision)
                        .map_err(|_| invalid("negative Job configuration revision"))?,
                ))
            })
            .transpose()
    }

    pub fn set_dependency(
        &mut self,
        project_id: &str,
        job_id: &str,
        prerequisite_job_id: &str,
    ) -> Result<u64> {
        validate_id(project_id)?;
        validate_id(job_id)?;
        validate_id(prerequisite_job_id)?;
        if job_id == prerequisite_job_id {
            return Err(invalid("a Job cannot depend on itself"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let job_is_unstarted: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_jobs j WHERE j.id=?1 AND j.project_id=?2 AND j.state IN ('pending','eligible') AND j.authoritative_attempt_id IS NULL)",
                params![job_id, project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !job_is_unstarted {
            return Err(invalid(
                "dependencies cannot change after Job execution begins",
            ));
        }
        let same_project: bool = transaction
            .query_row(
                "SELECT (SELECT project_id FROM domain_jobs WHERE id=?1) = ?3 AND (SELECT project_id FROM domain_jobs WHERE id=?2) = ?3",
                params![job_id, prerequisite_job_id, project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !same_project {
            return Err(invalid("dependency Jobs must belong to the same Project"));
        }
        let would_cycle: bool = transaction
            .query_row(
                "WITH RECURSIVE dependents(id) AS (SELECT ?2 UNION SELECT dependencies.prerequisite_job_id FROM domain_job_dependencies dependencies JOIN dependents ON dependencies.job_id=dependents.id WHERE dependencies.project_id=?3) SELECT EXISTS(SELECT 1 FROM dependents WHERE id=?1)",
                params![job_id, prerequisite_job_id, project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if would_cycle {
            return Err(invalid("dependency edge would create a cycle"));
        }
        transaction
            .execute(
                "INSERT INTO domain_dependency_revisions(project_id,revision) VALUES(?1,0) ON CONFLICT(project_id) DO NOTHING",
                [project_id],
            )
            .map_err(sql)?;
        let inserted = transaction
            .execute(
                "INSERT INTO domain_job_dependencies(project_id,job_id,prerequisite_job_id) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
                params![project_id, job_id, prerequisite_job_id],
            )
            .map_err(sql)?;
        if inserted == 0 {
            // The edge already exists, so no canonical change occurred and no
            // event is written for it.
            let revision: i64 = transaction
                .query_row(
                    "SELECT revision FROM domain_dependency_revisions WHERE project_id=?1",
                    [project_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            transaction.commit().map_err(sql)?;
            return u64::try_from(revision).map_err(|_| invalid("negative dependency revision"));
        }
        emit_dependency(
            &transaction,
            EventKind::DependencyAdded,
            &journal::DependencyEdge {
                project_id: project_id.to_string(),
                job_id: job_id.to_string(),
                prerequisite_job_id: prerequisite_job_id.to_string(),
            },
            None,
        )?;
        let revision: i64 = transaction
            .query_row(
                "UPDATE domain_dependency_revisions SET revision=revision+1 WHERE project_id=?1 RETURNING revision",
                [project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        transaction.commit().map_err(sql)?;
        u64::try_from(revision).map_err(|_| invalid("negative dependency revision"))
    }

    pub fn remove_dependency(
        &mut self,
        project_id: &str,
        job_id: &str,
        prerequisite_job_id: &str,
    ) -> Result<u64> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let deleted = transaction
            .execute(
                "DELETE FROM domain_job_dependencies WHERE project_id=?1 AND job_id=?2 AND prerequisite_job_id=?3",
                params![project_id, job_id, prerequisite_job_id],
            )
            .map_err(sql)?;
        if deleted == 0 {
            transaction.commit().map_err(sql)?;
            return self.dependency_revision(project_id);
        }
        emit_dependency(
            &transaction,
            EventKind::DependencyRemoved,
            &journal::DependencyEdge {
                project_id: project_id.to_string(),
                job_id: job_id.to_string(),
                prerequisite_job_id: prerequisite_job_id.to_string(),
            },
            None,
        )?;
        let revision: i64 = transaction
            .query_row(
                "UPDATE domain_dependency_revisions SET revision=revision+1 WHERE project_id=?1 RETURNING revision",
                [project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        transaction.commit().map_err(sql)?;
        u64::try_from(revision).map_err(|_| invalid("negative dependency revision"))
    }

    pub fn dependency_revision(&self, project_id: &str) -> Result<u64> {
        let revision: Option<i64> = self
            .connection
            .query_row(
                "SELECT revision FROM domain_dependency_revisions WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        u64::try_from(revision.unwrap_or(0)).map_err(|_| invalid("negative dependency revision"))
    }

    /// Load nodes and edges from one read transaction to form a consistent projection.
    pub fn rebuild_dependency_projection(
        &mut self,
        project_id: &str,
    ) -> Result<DependencyProjection> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(sql)?;
        let revision: i64 = transaction
            .query_row(
                "SELECT revision FROM domain_dependency_revisions WHERE project_id=?1",
                [project_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?
            .unwrap_or(0);
        let mut graph = DiGraph::new();
        let mut nodes = HashMap::new();
        {
            let mut statement = transaction
                .prepare("SELECT id FROM domain_jobs WHERE project_id=?1 ORDER BY id")
                .map_err(sql)?;
            let rows = statement
                .query_map([project_id], |row| row.get::<_, String>(0))
                .map_err(sql)?;
            for row in rows {
                let id = row.map_err(sql)?;
                let index = graph.add_node(id.clone());
                nodes.insert(id, index);
            }
        }
        {
            let mut statement = transaction
                .prepare("SELECT job_id,prerequisite_job_id FROM domain_job_dependencies WHERE project_id=?1 ORDER BY job_id,prerequisite_job_id")
                .map_err(sql)?;
            let rows = statement
                .query_map([project_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(sql)?;
            for row in rows {
                let (job, prerequisite) = row.map_err(sql)?;
                let from = *nodes
                    .get(&prerequisite)
                    .ok_or_else(|| invalid("dependency references missing Job"))?;
                let to = *nodes
                    .get(&job)
                    .ok_or_else(|| invalid("dependency references missing Job"))?;
                graph.add_edge(from, to, ());
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(DependencyProjection {
            project_id: project_id.to_string(),
            revision: u64::try_from(revision)
                .map_err(|_| invalid("negative dependency revision"))?,
            graph,
            nodes,
        })
    }

    pub fn projection_is_current(
        &self,
        projection: &DependencyProjection,
        project_id: &str,
    ) -> Result<bool> {
        Ok(projection.project_id == project_id
            && projection.revision == self.dependency_revision(project_id)?)
    }

    pub fn ensure_project(&self, root: &Path) -> Result<Project> {
        let root = crate::project::canonicalize(root)
            .to_string_lossy()
            .to_string();
        let id = new_id("prj");
        self.connection
            .execute(
                "INSERT INTO domain_projects(id,root,created_at) VALUES(?1,?2,?3) ON CONFLICT(root) DO NOTHING",
                params![id, root, now()],
            )
            .map_err(sql)?;
        self.connection
            .query_row(
                "SELECT id,root,created_at FROM domain_projects WHERE root=?1",
                [root],
                |row| {
                    Ok(Project {
                        id: row.get(0)?,
                        root: row.get(1)?,
                        created_at: row.get(2)?,
                    })
                },
            )
            .map_err(sql)
    }

    pub fn create_job(&self, project_id: &str, payload: &str) -> Result<Job> {
        validate_id(project_id)?;
        let id = new_id("job");
        let timestamp = now();
        let transaction = self.begin()?;
        transaction
            .execute(
                "INSERT INTO domain_jobs(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at) VALUES(?1,?2,'pending',0,NULL,?3,?4,?4)",
                params![id, project_id, payload, timestamp],
            )
            .map_err(sql)?;
        let job = Job {
            id: id.clone(),
            project_id: project_id.to_string(),
            state: JobState::Pending,
            generation: 0,
            authoritative_attempt_id: None,
            payload: payload.to_string(),
            created_at: timestamp,
            updated_at: timestamp,
        };
        emit_job(&transaction, EventKind::JobCreated, &job, None)?;
        transaction.commit().map_err(sql)?;
        Ok(job)
    }

    /// Create a child Job while requiring the current parent Attempt authority.
    /// Dependency edges are committed with the Job in the same immediate
    /// transaction, so an executor cannot enqueue work from a stale parent.
    pub fn create_child_job(
        &mut self,
        parent_authority: &AttemptAuthority,
        payload: &str,
        prerequisite_job_ids: &[&str],
    ) -> Result<Job> {
        validate_id(&parent_authority.attempt_id)?;
        validate_id(&parent_authority.job_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let parent_generation = i64::try_from(parent_authority.generation)
            .map_err(|_| invalid("parent Attempt generation exceeds SQLite range"))?;
        let parent_valid: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?1 AND a.job_id=?2 AND a.generation=?3 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running'))",
                params![parent_authority.attempt_id, parent_authority.job_id, parent_generation],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !parent_valid {
            return Err(invalid("parent Attempt is no longer authoritative"));
        }
        let project_id: String = transaction
            .query_row(
                "SELECT project_id FROM domain_jobs WHERE id=?1",
                [&parent_authority.job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        let job_id = new_id("job");
        let timestamp = now();
        transaction
            .execute(
                "INSERT INTO domain_jobs(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at) VALUES(?1,?2,'pending',0,NULL,?3,?4,?4)",
                params![job_id, project_id, payload, timestamp],
            )
            .map_err(sql)?;
        for prerequisite in prerequisite_job_ids {
            validate_id(prerequisite)?;
            let same_project: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_jobs WHERE id=?1 AND project_id=?2)",
                    params![prerequisite, project_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if !same_project {
                return Err(invalid("child dependency is not in the parent Project"));
            }
            let would_cycle: bool = transaction
                .query_row(
                    "WITH RECURSIVE dependents(id) AS (SELECT ?2 UNION SELECT dependencies.prerequisite_job_id FROM domain_job_dependencies dependencies JOIN dependents ON dependencies.job_id=dependents.id WHERE dependencies.project_id=?3) SELECT EXISTS(SELECT 1 FROM dependents WHERE id=?1)",
                    params![job_id, prerequisite, project_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if would_cycle {
                return Err(invalid("child dependency would create a cycle"));
            }
            transaction
                .execute(
                    "INSERT INTO domain_job_dependencies(project_id,job_id,prerequisite_job_id) VALUES(?1,?2,?3)",
                    params![project_id, job_id, prerequisite],
                )
                .map_err(sql)?;
        }
        transaction.execute(
            "INSERT INTO domain_job_origins(job_id,parent_job_id,attempt_id,generation) VALUES(?1,?2,?3,?4)",
            params![job_id,parent_authority.job_id,parent_authority.attempt_id,parent_generation],
        ).map_err(sql)?;
        if !prerequisite_job_ids.is_empty() {
            transaction
                .execute(
                    "INSERT INTO domain_dependency_revisions(project_id,revision) VALUES(?1,0) ON CONFLICT(project_id) DO NOTHING",
                    [&project_id],
                )
                .map_err(sql)?;
            transaction
                .execute(
                    "UPDATE domain_dependency_revisions SET revision=revision+1 WHERE project_id=?1",
                    [&project_id],
                )
                .map_err(sql)?;
        }
        let job = Job {
            id: job_id.clone(),
            project_id: project_id.clone(),
            state: JobState::Pending,
            generation: 0,
            authoritative_attempt_id: None,
            payload: payload.to_string(),
            created_at: timestamp,
            updated_at: timestamp,
        };
        // The spawned Job is the root fact; its prerequisite edges are caused
        // by the same spawn, and the parent Attempt that authorized it is the
        // event's authority identity.
        let root = journal::append(
            &transaction,
            EventDraft::new(EventKind::JobCreated, "job", &job.id, journal::value(&job)?)
                .project(&project_id)
                .job(&job.id)
                .authority(&parent_authority.attempt_id, parent_authority.generation)
                .causation_key(&parent_authority.attempt_id),
        )?;
        for prerequisite in prerequisite_job_ids {
            emit_dependency(
                &transaction,
                EventKind::DependencyAdded,
                &journal::DependencyEdge {
                    project_id: project_id.clone(),
                    job_id: job_id.clone(),
                    prerequisite_job_id: (*prerequisite).to_string(),
                },
                Some(root),
            )?;
        }
        transaction.commit().map_err(sql)?;
        Ok(job)
    }

    /// Admit a child execution in one canonical transaction. The parent
    /// Attempt is checked while the child Job, dependency edges, Attempt and
    /// Executor are published, so a stale worker cannot create an orphaned
    /// execution branch.
    pub fn admit_child(
        &mut self,
        parent_authority: &AttemptAuthority,
        payload: &str,
        prerequisite_job_ids: &[&str],
        executor_kind: &str,
    ) -> Result<CanonicalAdmission> {
        validate_id(&parent_authority.attempt_id)?;
        validate_id(&parent_authority.job_id)?;
        validate_id(executor_kind)?;
        let parent_generation = i64::try_from(parent_authority.generation)
            .map_err(|_| invalid("parent Attempt generation exceeds SQLite range"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let project_id: String = transaction
            .query_row(
                "SELECT j.project_id FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?1 AND a.job_id=?2 AND a.generation=?3 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running')",
                params![parent_authority.attempt_id, parent_authority.job_id, parent_generation],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("parent Attempt is no longer authoritative"))?;

        // A child is only executable when every prerequisite has already
        // completed. Check this while holding the admission transaction so a
        // caller cannot publish an authoritative Attempt for blocked work.
        for prerequisite in prerequisite_job_ids {
            validate_id(prerequisite)?;
            let status: Option<String> = transaction
                .query_row(
                    "SELECT state FROM domain_jobs WHERE id=?1 AND project_id=?2",
                    params![prerequisite, project_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql)?;
            match status.as_deref() {
                Some("completed") => {}
                Some(_) => {
                    return Err(invalid("child dependencies are not satisfied"));
                }
                None => return Err(invalid("child dependency is not in the parent Project")),
            }
        }

        let job_id = new_id("job");
        let attempt_id = new_id("att");
        let executor_id = new_id("exec");
        let timestamp = now();
        transaction
            .execute(
                "INSERT INTO domain_jobs(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at) VALUES(?1,?2,'pending',0,NULL,?3,?4,?4)",
                params![job_id, project_id, payload, timestamp],
            )
            .map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_job_origins(job_id,parent_job_id,attempt_id,generation) VALUES(?1,?2,?3,?4)",
            params![job_id,parent_authority.job_id,parent_authority.attempt_id,parent_generation],
        ).map_err(sql)?;
        for prerequisite in prerequisite_job_ids {
            validate_id(prerequisite)?;
            let same_project: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_jobs WHERE id=?1 AND project_id=?2)",
                    params![prerequisite, project_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if !same_project {
                return Err(invalid("child dependency is not in the parent Project"));
            }
            let would_cycle: bool = transaction
                .query_row(
                    "WITH RECURSIVE dependents(id) AS (SELECT ?2 UNION SELECT dependencies.prerequisite_job_id FROM domain_job_dependencies dependencies JOIN dependents ON dependencies.job_id=dependents.id WHERE dependencies.project_id=?3) SELECT EXISTS(SELECT 1 FROM dependents WHERE id=?1)",
                    params![job_id, prerequisite, project_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if would_cycle {
                return Err(invalid("child dependency would create a cycle"));
            }
            transaction
                .execute(
                    "INSERT INTO domain_job_dependencies(project_id,job_id,prerequisite_job_id) VALUES(?1,?2,?3)",
                    params![project_id, job_id, prerequisite],
                )
                .map_err(sql)?;
        }
        if !prerequisite_job_ids.is_empty() {
            transaction
                .execute(
                    "INSERT INTO domain_dependency_revisions(project_id,revision) VALUES(?1,0) ON CONFLICT(project_id) DO NOTHING",
                    [&project_id],
                )
                .map_err(sql)?;
            transaction
                .execute(
                    "UPDATE domain_dependency_revisions SET revision=revision+1 WHERE project_id=?1",
                    [&project_id],
                )
                .map_err(sql)?;
        }
        transaction
            .execute(
                "INSERT INTO domain_attempts(id,job_id,generation,state,authoritative,created_at,finished_at) VALUES(?1,?2,1,'queued',1,?3,NULL)",
                params![attempt_id, job_id, timestamp],
            )
            .map_err(sql)?;
        let changed = transaction
            .execute(
                "UPDATE domain_jobs SET state='running',generation=1,authoritative_attempt_id=?2,updated_at=?3 WHERE id=?1 AND state='pending'",
                params![job_id, attempt_id, timestamp],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("child Job changed while creating Attempt"));
        }
        transaction
            .execute(
                "INSERT INTO domain_executors(id,attempt_id,kind,state,created_at) VALUES(?1,?2,?3,'ready',?4)",
                params![executor_id, attempt_id, executor_kind, timestamp],
            )
            .map_err(sql)?;

        let job = Job {
            id: job_id.clone(),
            project_id: project_id.clone(),
            state: JobState::Running,
            generation: 1,
            authoritative_attempt_id: Some(attempt_id.clone()),
            payload: payload.to_string(),
            created_at: timestamp,
            updated_at: timestamp,
        };
        let attempt = Attempt {
            id: attempt_id.clone(),
            job_id: job_id.clone(),
            generation: 1,
            state: AttemptState::Queued,
            authoritative: true,
            created_at: timestamp,
            finished_at: None,
        };
        let executor = Executor {
            id: executor_id.clone(),
            attempt_id: attempt_id.clone(),
            kind: executor_kind.to_string(),
            state: "ready".to_string(),
            created_at: timestamp,
        };
        // A spawn is one causal fact committed under the parent Attempt's
        // authority: the child Job, its prerequisite edges, its first Attempt
        // and its Executor all become canonical together.
        let root = journal::append(
            &transaction,
            EventDraft::new(EventKind::JobCreated, "job", &job.id, journal::value(&job)?)
                .project(&project_id)
                .job(&job.id)
                .authority(&parent_authority.attempt_id, parent_authority.generation)
                .causation_key(&parent_authority.attempt_id),
        )?;
        for prerequisite in prerequisite_job_ids {
            emit_dependency(
                &transaction,
                EventKind::DependencyAdded,
                &journal::DependencyEdge {
                    project_id: project_id.clone(),
                    job_id: job_id.clone(),
                    prerequisite_job_id: (*prerequisite).to_string(),
                },
                Some(root),
            )?;
        }
        emit_attempt(
            &transaction,
            EventKind::AttemptCreated,
            &attempt,
            Some(root),
        )?;
        emit_executor(
            &transaction,
            EventKind::ExecutorCreated,
            &executor,
            Some(root),
        )?;
        transaction.commit().map_err(sql)?;

        Ok(CanonicalAdmission {
            project: self
                .connection
                .query_row(
                    "SELECT id,root,created_at FROM domain_projects WHERE id=?1",
                    [&project_id],
                    |row| {
                        Ok(Project {
                            id: row.get(0)?,
                            root: row.get(1)?,
                            created_at: row.get(2)?,
                        })
                    },
                )
                .map_err(sql)?,
            job,
            attempt,
            executor,
        })
    }

    /// Admit one externally bound execution into the canonical domain.
    ///
    /// The binding key is the stable session/runtime identity supplied by the
    /// caller. Re-admission is idempotent and returns the existing authority;
    /// no legacy Mission, WorkNode, Run, or JSON recovery record participates.
    pub fn admit_job(
        &mut self,
        project: Project,
        binding_key: &str,
        payload: &str,
        executor_kind: &str,
    ) -> Result<CanonicalAdmission> {
        validate_id(&project.id)?;
        validate_id(binding_key)?;
        validate_id(executor_kind)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;

        if let Some(existing) = transaction
            .query_row(
                "SELECT j.id,j.project_id,j.state,j.generation,j.authoritative_attempt_id,j.payload,j.created_at,j.updated_at,a.id,a.job_id,a.generation,a.state,a.authoritative,a.created_at,a.finished_at,e.id,e.attempt_id,e.kind,e.state,e.created_at FROM domain_job_bindings b JOIN domain_jobs j ON j.id=b.job_id JOIN domain_attempts a ON a.id=b.attempt_id LEFT JOIN domain_executors e ON e.attempt_id=a.id WHERE b.binding_key=?1 AND b.project_id=?2",
                params![binding_key, project.id],
                |row| {
                    Ok((
                        Job {
                            id: row.get(0)?, project_id: row.get(1)?,
                            state: row.get::<_, String>(2)?.parse().map_err(|_| rusqlite::Error::InvalidQuery)?,
                            generation: u64::try_from(row.get::<_, i64>(3)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                            authoritative_attempt_id: row.get(4)?, payload: row.get(5)?,
                            created_at: row.get(6)?, updated_at: row.get(7)?,
                        },
                        Attempt {
                            id: row.get(8)?, job_id: row.get(9)?,
                            generation: u64::try_from(row.get::<_, i64>(10)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                            state: row.get::<_, String>(11)?.parse().map_err(|_| rusqlite::Error::InvalidQuery)?,
                            authoritative: row.get(12)?, created_at: row.get(13)?, finished_at: row.get(14)?,
                        },
                        Executor { id: row.get(15)?, attempt_id: row.get(16)?, kind: row.get(17)?, state: row.get(18)?, created_at: row.get(19)? },
                    ))
                },
            )
            .optional()
            .map_err(sql)?
        {
            transaction.commit().map_err(sql)?;
            return Ok(CanonicalAdmission { project, job: existing.0, attempt: existing.1, executor: existing.2 });
        }

        let timestamp = now();
        let job_id = new_id("job");
        let attempt_id = new_id("att");
        let executor_id = new_id("exec");
        // Keep the scheduling transition explicit inside the same immediate
        // transaction: admission publishes an eligible Job, then claims its
        // authoritative Attempt before exposing the Executor.
        transaction.execute(
            "INSERT INTO domain_jobs(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at) VALUES(?1,?2,'eligible',0,NULL,?3,?4,?4)",
            params![job_id, project.id, payload, timestamp],
        ).map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_attempts(id,job_id,generation,state,authoritative,created_at,finished_at) VALUES(?1,?2,1,'queued',1,?3,NULL)",
            params![attempt_id, job_id, timestamp],
        ).map_err(sql)?;
        transaction.execute(
            "UPDATE domain_jobs SET state='running',generation=1,authoritative_attempt_id=?2,updated_at=?3 WHERE id=?1 AND state='eligible' AND authoritative_attempt_id IS NULL",
            params![job_id, attempt_id, timestamp],
        ).map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_executors(id,attempt_id,kind,state,created_at) VALUES(?1,?2,?3,'ready',?4)",
            params![executor_id, attempt_id, executor_kind, timestamp],
        ).map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_job_bindings(binding_key,project_id,job_id,created_at,attempt_id) VALUES(?1,?2,?3,?4,?5)",
            params![binding_key, project.id, job_id, timestamp, attempt_id],
        ).map_err(sql)?;

        // One admission is one causal fact. The Job record is the root; the
        // Attempt, the Executor and the session binding are recorded as caused
        // by it inside the same commit, so a reader can never see a bound
        // session whose Attempt is not journalized yet.
        let job = Job {
            id: job_id.clone(),
            project_id: project.id.clone(),
            state: JobState::Running,
            generation: 1,
            authoritative_attempt_id: Some(attempt_id.clone()),
            payload: payload.to_string(),
            created_at: timestamp,
            updated_at: timestamp,
        };
        let attempt = Attempt {
            id: attempt_id.clone(),
            job_id: job_id.clone(),
            generation: 1,
            state: AttemptState::Queued,
            authoritative: true,
            created_at: timestamp,
            finished_at: None,
        };
        let executor = Executor {
            id: executor_id.clone(),
            attempt_id: attempt_id.clone(),
            kind: executor_kind.to_string(),
            state: "ready".to_string(),
            created_at: timestamp,
        };
        let root = journal::append(
            &transaction,
            EventDraft::new(EventKind::JobCreated, "job", &job.id, journal::value(&job)?)
                .project(&project.id)
                .job(&job.id)
                .causation_key(binding_key),
        )?;
        emit_attempt(
            &transaction,
            EventKind::AttemptCreated,
            &attempt,
            Some(root),
        )?;
        emit_executor(
            &transaction,
            EventKind::ExecutorCreated,
            &executor,
            Some(root),
        )?;
        emit_binding(
            &transaction,
            &journal::JobBinding {
                binding_key: binding_key.to_string(),
                project_id: project.id.clone(),
                job_id: job_id.clone(),
                attempt_id: Some(attempt_id.clone()),
            },
            Some(root),
        )?;
        transaction.commit().map_err(sql)?;

        Ok(CanonicalAdmission {
            project: project.clone(),
            job,
            attempt,
            executor,
        })
    }

    pub fn authority_for_binding(
        &self,
        project_id: &str,
        binding_key: &str,
    ) -> Result<Option<AttemptAuthority>> {
        self.connection
            .query_row(
                "SELECT a.id,a.job_id,a.generation FROM domain_job_bindings b JOIN domain_attempts a ON a.id=b.attempt_id JOIN domain_jobs j ON j.id=b.job_id WHERE b.project_id=?1 AND b.binding_key=?2 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running')",
                params![project_id, binding_key],
                |row| Ok(AttemptAuthority { attempt_id: row.get(0)?, job_id: row.get(1)?, generation: u64::try_from(row.get::<_, i64>(2)?).map_err(|_| rusqlite::Error::InvalidQuery)? }),
            )
            .optional()
            .map_err(sql)
    }

    pub fn bind_job(&mut self, binding_key: &str, job_id: &str) -> Result<()> {
        let job = self.job(job_id)?.ok_or_else(|| invalid("unknown Job"))?;
        let attempt_id = job
            .authoritative_attempt_id
            .ok_or_else(|| invalid("Job has no current Attempt"))?;
        let authority = self
            .authority(&attempt_id)?
            .ok_or_else(|| invalid("Attempt is not authoritative"))?;
        self.bind_attempt(binding_key, &authority)
    }

    pub fn bind_attempt(&mut self, binding_key: &str, authority: &AttemptAuthority) -> Result<()> {
        let job_id = &authority.job_id;
        validate_id(binding_key)?;
        validate_id(job_id)?;
        let project_id: String = self
            .connection
            .query_row(
                "SELECT project_id FROM domain_jobs WHERE id=?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let current: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_jobs j JOIN domain_attempts a ON a.id=j.authoritative_attempt_id WHERE j.id=?1 AND a.id=?2 AND a.generation=?3 AND a.authoritative=1 AND a.state IN ('queued','running'))",
            params![job_id,authority.attempt_id,i64::try_from(authority.generation).map_err(|_| invalid("invalid Attempt generation"))?], |row| row.get(0)
        ).map_err(sql)?;
        if !current {
            return Err(invalid("cannot bind a stale Attempt"));
        }
        let existing: Option<(String, String, Option<String>)> = transaction
            .query_row(
                "SELECT project_id,job_id,attempt_id FROM domain_job_bindings WHERE binding_key=?1",
                [binding_key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        if let Some((existing_project, existing_job, existing_attempt)) = existing {
            if existing_project != project_id
                || &existing_job != job_id
                || existing_attempt.as_deref() != Some(&authority.attempt_id)
            {
                return Err(invalid("binding key is already assigned to another Job"));
            }
            transaction.commit().map_err(sql)?;
            return Ok(());
        }
        transaction
            .execute(
                "INSERT INTO domain_job_bindings(binding_key,project_id,job_id,created_at,attempt_id) VALUES(?1,?2,?3,?4,?5)",
                params![binding_key, project_id, job_id, now(), authority.attempt_id],
            )
            .map_err(sql)?;
        emit_binding(
            &transaction,
            &journal::JobBinding {
                binding_key: binding_key.to_string(),
                project_id: project_id.clone(),
                job_id: job_id.clone(),
                attempt_id: Some(authority.attempt_id.clone()),
            },
            None,
        )?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    pub fn authority_for_root(&self, project_id: &str) -> Result<Option<AttemptAuthority>> {
        self.connection
            .query_row(
                "SELECT a.id,a.job_id,a.generation FROM domain_jobs j JOIN domain_attempts a ON a.id=j.authoritative_attempt_id WHERE j.project_id=?1 AND a.authoritative=1 AND a.state IN ('queued','running') AND NOT EXISTS(SELECT 1 FROM domain_job_origins o WHERE o.job_id=j.id) ORDER BY j.created_at,j.id LIMIT 1",
                [project_id],
                |row| {
                    Ok(AttemptAuthority {
                        attempt_id: row.get(0)?,
                        job_id: row.get(1)?,
                        generation: u64::try_from(row.get::<_, i64>(2)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    })
                },
            )
            .optional()
            .map_err(sql)
    }

    pub fn binding_for_job(&self, job_id: &str) -> Result<Option<String>> {
        self.connection
            .query_row(
                "SELECT binding_key FROM domain_job_bindings WHERE job_id=?1",
                [job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)
    }

    pub fn set_job_eligible(&self, job_id: &str) -> Result<()> {
        validate_id(job_id)?;
        let timestamp = now();
        let transaction = self.begin()?;
        let changed = transaction.execute(
            "UPDATE domain_jobs SET state='eligible',updated_at=?2 WHERE id=?1 AND state='pending' AND authoritative_attempt_id IS NULL",
            params![job_id, timestamp],
        ).map_err(sql)?;
        if changed != 1 {
            return Err(invalid(
                "Job is not pending or already has an authoritative Attempt",
            ));
        }
        let job = self
            .job(job_id)?
            .ok_or_else(|| invalid("Job disappeared while becoming eligible"))?;
        emit_job(&transaction, EventKind::JobUpdated, &job, None)?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    pub fn dependencies_satisfied(&self, project_id: &str, job_id: &str) -> Result<bool> {
        let blocked: i64 = self
            .connection
            .query_row(
                "SELECT COUNT(*) FROM domain_job_dependencies d JOIN domain_jobs prerequisite ON prerequisite.id=d.prerequisite_job_id WHERE d.project_id=?1 AND d.job_id=?2 AND prerequisite.state NOT IN ('completed')",
                params![project_id, job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        Ok(blocked == 0)
    }

    /// Claim an eligible Job and establish its authoritative Attempt atomically.
    pub fn create_attempt(&mut self, job_id: &str) -> Result<Attempt> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let (attempt, _root) = create_attempt_in(&transaction, job_id)?;
        transaction.commit().map_err(sql)?;
        Ok(attempt)
    }

    pub fn dispatch_job(
        &mut self,
        job_id: &str,
        executor_kind: &str,
    ) -> Result<(Attempt, Executor)> {
        validate_id(executor_kind)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        transaction.execute("UPDATE domain_jobs SET state='eligible',updated_at=?2 WHERE id=?1 AND state='pending' AND authoritative_attempt_id IS NULL", params![job_id,now()]).map_err(sql)?;
        let (attempt, root) = create_attempt_in(&transaction, job_id)?;
        let executor = Executor {
            id: new_id("exec"),
            attempt_id: attempt.id.clone(),
            kind: executor_kind.to_string(),
            state: "ready".into(),
            created_at: now(),
        };
        transaction.execute("INSERT INTO domain_executors(id,attempt_id,kind,state,created_at) VALUES(?1,?2,?3,?4,?5)", params![executor.id,executor.attempt_id,executor.kind,executor.state,executor.created_at]).map_err(sql)?;
        emit_executor(
            &transaction,
            EventKind::ExecutorCreated,
            &executor,
            Some(root),
        )?;
        transaction.commit().map_err(sql)?;
        Ok((attempt, executor))
    }

    /// Fence the current Attempt and publish a new generation for the same
    /// Job. The old authority is revoked in the same transaction as the new
    /// Attempt and Executor publication.
    pub fn replace_attempt(
        &mut self,
        job_id: &str,
        executor_kind: &str,
    ) -> Result<CanonicalAdmission> {
        self.replace_attempt_checked(job_id, executor_kind, None)
    }

    pub fn replace_attempt_checked(
        &mut self,
        job_id: &str,
        executor_kind: &str,
        expected_attempt: Option<&str>,
    ) -> Result<CanonicalAdmission> {
        validate_id(job_id)?;
        validate_id(executor_kind)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let (project_id, _payload, generation, old_attempt): (String, String, i64, Option<String>) = transaction
            .query_row(
                "SELECT project_id,payload,generation,authoritative_attempt_id FROM domain_jobs WHERE id=?1",
                [job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql)?
            .ok_or_else(|| invalid("unknown Job"))?;
        if expected_attempt.is_some_and(|expected| old_attempt.as_deref() != Some(expected)) {
            return Err(invalid("replacement rejected: Attempt authority changed"));
        }
        let state: String = transaction
            .query_row(
                "SELECT state FROM domain_jobs WHERE id=?1",
                [job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if matches!(state.as_str(), "completed" | "cancelled") {
            return Err(invalid("terminal Job cannot be replaced"));
        }
        let blocked: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_job_dependencies d JOIN domain_jobs prerequisite ON prerequisite.id=d.prerequisite_job_id WHERE d.job_id=?1 AND prerequisite.state!='completed')",
                [job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if blocked {
            return Err(invalid("Job dependencies are not satisfied"));
        }
        let next_generation = generation
            .checked_add(1)
            .ok_or_else(|| invalid("Job generation overflow"))?;
        if let Some(old_attempt) = &old_attempt {
            // Capture exactly the rows the fence will change, so the journal
            // records each one that actually moved rather than the whole
            // Attempt's history.
            let fenced_executors = attempt_executors(&transaction, old_attempt)?;
            let fenced_intents = attempt_intents(&transaction, old_attempt)?;
            let fenced_calls = attempt_calls(&transaction, old_attempt)?;
            transaction
                .execute(
                    "UPDATE domain_executors SET state='fenced' WHERE attempt_id=?1",
                    [old_attempt],
                )
                .map_err(sql)?;
            transaction.execute(
                "UPDATE domain_dispatch_intents SET state='fenced',failure='attempt_replaced',effect_state=CASE WHEN effect_state='started' THEN 'unknown' ELSE effect_state END,updated_at=?2 WHERE attempt_id=?1 AND state IN ('pending','queued','running')",
                params![old_attempt, now()],
            ).map_err(sql)?;
            transaction.execute(
                "UPDATE domain_calls SET state='unknown',finished_at=?2 WHERE attempt_id=?1 AND state IN ('created','running')",
                params![old_attempt, now()],
            ).map_err(sql)?;
            transaction
                .execute(
                    "UPDATE domain_attempts SET authoritative=0,state='failed',finished_at=?2 WHERE id=?1 AND authoritative=1",
                    params![old_attempt, now()],
                )
                .map_err(sql)?;
            // The revocation is the causal root: the replaced Attempt stops
            // being authoritative first, and every cascade it forces is
            // recorded as caused by it in this same commit.
            let root = emit_attempt(
                &transaction,
                EventKind::AttemptUpdated,
                &read_attempt(&transaction, old_attempt)?,
                None,
            )?;
            emit_changed_executors(&transaction, &fenced_executors, Some(root))?;
            emit_changed_intents(&transaction, &fenced_intents, Some(root))?;
            emit_changed_calls(&transaction, &fenced_calls, Some(root))?;
        }
        let attempt_id = new_id("att");
        let executor_id = new_id("exec");
        let timestamp = now();
        transaction
            .execute(
                "INSERT INTO domain_attempts(id,job_id,generation,state,authoritative,created_at,finished_at) VALUES(?1,?2,?3,'queued',1,?4,NULL)",
                params![attempt_id, job_id, next_generation, timestamp],
            )
            .map_err(sql)?;
        transaction
            .execute(
                "UPDATE domain_jobs SET state='running',generation=?2,authoritative_attempt_id=?3,updated_at=?4 WHERE id=?1",
                params![job_id, next_generation, attempt_id, timestamp],
            )
            .map_err(sql)?;
        transaction
            .execute(
                "INSERT INTO domain_executors(id,attempt_id,kind,state,created_at) VALUES(?1,?2,?3,'ready',?4)",
                params![executor_id, attempt_id, executor_kind, timestamp],
            )
            .map_err(sql)?;
        // The replacement generation becomes canonical here, still inside the
        // commit that revoked the previous one: there is no observable instant
        // in which two Attempts share authority.
        let attempt = read_attempt(&transaction, &attempt_id)?;
        let executor = read_executor(&transaction, &executor_id)?
            .ok_or_else(|| invalid("replacement Executor disappeared"))?;
        let job =
            read_job(&transaction, job_id)?.ok_or_else(|| invalid("replaced Job disappeared"))?;
        let root = emit_attempt(&transaction, EventKind::AttemptCreated, &attempt, None)?;
        emit_executor(
            &transaction,
            EventKind::ExecutorCreated,
            &executor,
            Some(root),
        )?;
        emit_job(&transaction, EventKind::JobUpdated, &job, Some(root))?;
        transaction.commit().map_err(sql)?;
        let project = self
            .connection
            .query_row(
                "SELECT id,root,created_at FROM domain_projects WHERE id=?1",
                [&project_id],
                |row| {
                    Ok(Project {
                        id: row.get(0)?,
                        root: row.get(1)?,
                        created_at: row.get(2)?,
                    })
                },
            )
            .map_err(sql)?;
        Ok(CanonicalAdmission {
            project,
            job,
            attempt,
            executor,
        })
    }

    pub fn create_executor(&self, attempt_id: &str, kind: &str) -> Result<Executor> {
        validate_id(attempt_id)?;
        if kind.is_empty() || kind.len() > 120 {
            return Err(invalid("invalid Executor kind"));
        }
        let id = new_id("exe");
        let timestamp = now();
        let transaction = self.begin()?;
        if read_attempt(&transaction, attempt_id).is_err() {
            return Err(invalid("unknown Attempt"));
        }
        transaction
            .execute(
                "INSERT INTO domain_executors(id,attempt_id,kind,state,created_at) VALUES(?1,?2,?3,'created',?4)",
                params![id, attempt_id, kind, timestamp],
            )
            .map_err(sql)?;
        let executor = Executor {
            id,
            attempt_id: attempt_id.to_string(),
            kind: kind.to_string(),
            state: "created".to_string(),
            created_at: timestamp,
        };
        emit_executor(&transaction, EventKind::ExecutorCreated, &executor, None)?;
        transaction.commit().map_err(sql)?;
        Ok(executor)
    }

    pub fn executor(&self, executor_id: &str) -> Result<Option<Executor>> {
        self.connection
            .query_row(
                "SELECT id,attempt_id,kind,state,created_at FROM domain_executors WHERE id=?1",
                [executor_id],
                |row| {
                    Ok(Executor {
                        id: row.get(0)?,
                        attempt_id: row.get(1)?,
                        kind: row.get(2)?,
                        state: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(sql)
    }

    pub fn executor_for_attempt(&self, attempt_id: &str) -> Result<Option<Executor>> {
        self.connection
            .query_row(
                "SELECT id,attempt_id,kind,state,created_at FROM domain_executors WHERE attempt_id=?1 ORDER BY created_at,id LIMIT 1",
                [attempt_id],
                |row| {
                    Ok(Executor {
                        id: row.get(0)?,
                        attempt_id: row.get(1)?,
                        kind: row.get(2)?,
                        state: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(sql)
    }

    pub fn authority(&self, attempt_id: &str) -> Result<Option<AttemptAuthority>> {
        let row: Option<(String, String, i64)> = self
            .connection
            .query_row(
                "SELECT a.id,a.job_id,a.generation FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?1 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running')",
                [attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        row.map(|(attempt_id, job_id, generation)| {
            Ok(AttemptAuthority {
                attempt_id,
                job_id,
                generation: u64::try_from(generation)
                    .map_err(|_| invalid("negative Attempt generation"))?,
            })
        })
        .transpose()
    }

    pub fn mark_attempt_running(&self, authority: &AttemptAuthority) -> Result<()> {
        let generation = i64::try_from(authority.generation)
            .map_err(|_| invalid("Attempt generation exceeds SQLite range"))?;
        let transaction = self.begin()?;
        let changed = transaction
            .execute(
                "UPDATE domain_attempts SET state='running' WHERE id=?1 AND generation=?2 AND authoritative=1 AND state IN ('queued','running')",
                params![authority.attempt_id, generation],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("Attempt is no longer authoritative"));
        }
        let attempt = read_attempt(&transaction, &authority.attempt_id)?;
        // Already running is still a canonical claim of execution, and the
        // state it asserts has to be reconstructable from the journal alone.
        emit_attempt(&transaction, EventKind::AttemptUpdated, &attempt, None)?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    /// Record a Call. Side-effecting Calls must name the current authority and generation.
    pub fn create_call(
        &mut self,
        attempt_id: &str,
        executor_id: Option<&str>,
        generation: u64,
        side_effect: bool,
        request: &str,
    ) -> Result<Call> {
        let effect_kind = if side_effect {
            EffectIntentKind::StrictFenced
        } else {
            EffectIntentKind::Idempotent
        };
        self.create_call_with_effect(attempt_id, executor_id, generation, effect_kind, request)
    }

    /// Create the Call and its durable dispatch intent in one transaction.
    /// The intent is the recovery fact; the in-memory dispatcher is only a
    /// bounded delivery mechanism.
    pub fn create_call_with_effect(
        &mut self,
        attempt_id: &str,
        executor_id: Option<&str>,
        generation: u64,
        effect_kind: EffectIntentKind,
        request: &str,
    ) -> Result<Call> {
        validate_id(attempt_id)?;
        let side_effect = !matches!(effect_kind, EffectIntentKind::Idempotent);
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let authoritative: Option<i64> = transaction.query_row(
            "SELECT a.generation FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?1 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id",
            [attempt_id], |row| row.get(0),
        ).optional().map_err(sql)?;
        if side_effect
            && authoritative.and_then(|value| u64::try_from(value).ok()) != Some(generation)
        {
            return Err(invalid(
                "side-effect Call rejected: Attempt authority or generation is stale",
            ));
        }
        if side_effect {
            let running: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_attempts WHERE id=?1 AND state IN ('queued','running'))",
                    [attempt_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if !running {
                return Err(invalid(
                    "side-effect Call rejected: Attempt is not executable",
                ));
            }
        }
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_attempts WHERE id=?1)",
                [attempt_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !exists {
            return Err(invalid("unknown Attempt"));
        }
        if let Some(executor_id) = executor_id {
            let belongs: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_executors WHERE id=?1 AND attempt_id=?2 AND state IN ('ready','running'))",
                    params![executor_id, attempt_id],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if !belongs {
                return Err(invalid("Executor does not belong to the named Attempt"));
            }
        }
        let generation_i64 = i64::try_from(generation)
            .map_err(|_| invalid("Call generation exceeds SQLite range"))?;
        let id = new_id("call");
        let intent_id = new_id("intent");
        let timestamp = now();
        transaction.execute(
            "INSERT INTO domain_calls(id,attempt_id,executor_id,generation,side_effect,state,request,response,created_at,finished_at) VALUES(?1,?2,?3,?4,?5,'created',?6,NULL,?7,NULL)",
            params![id, attempt_id, executor_id, generation_i64, side_effect, request, timestamp],
        ).map_err(sql)?;
        let job_id: String = transaction
            .query_row(
                "SELECT job_id FROM domain_attempts WHERE id=?1",
                [attempt_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_dispatch_intents(id,call_id,job_id,attempt_id,executor_id,generation,state,effect_kind,effect_state,request,reservation_id,failure,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,'pending',?7,'not_started',?8,NULL,NULL,?9,?9)",
            params![intent_id, id, job_id, attempt_id, executor_id, generation_i64, effect_kind.to_string(), request, timestamp],
        ).map_err(sql)?;
        let call = Call {
            id: id.clone(),
            attempt_id: attempt_id.to_string(),
            executor_id: executor_id.map(str::to_string),
            generation,
            side_effect,
            effect_kind,
            state: "created".to_string(),
            request: request.to_string(),
            response: None,
            created_at: timestamp,
            finished_at: None,
        };
        let intent = read_dispatch_intent(&transaction, &intent_id)?
            .ok_or_else(|| invalid("created dispatch intent disappeared"))?;
        // The Call and its durable dispatch intent are one admission fact. The
        // intent is the recovery fact, so it is recorded as caused by the Call
        // in the same commit and can never exist without one.
        let root = emit_call(&transaction, EventKind::CallCreated, &call, None)?;
        emit_dispatch_intent(
            &transaction,
            EventKind::DispatchIntentCreated,
            &intent,
            Some(root),
        )?;
        transaction.commit().map_err(sql)?;
        Ok(call)
    }

    /// Claim an admitted Call immediately before its side effect starts. The
    /// claim repeats the Attempt and generation fence so a queued Call cannot
    /// cross a replacement or cancellation boundary.
    pub fn start_call(&mut self, call_id: &str, attempt_id: &str, generation: u64) -> Result<()> {
        if self.claim_call(call_id, attempt_id, generation)? {
            Ok(())
        } else {
            Err(invalid("Call has already been delivered"))
        }
    }

    pub fn claim_call(&mut self, call_id: &str, attempt_id: &str, generation: u64) -> Result<bool> {
        validate_id(call_id)?;
        validate_id(attempt_id)?;
        let generation_i64 = i64::try_from(generation)
            .map_err(|_| invalid("Call generation exceeds SQLite range"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction.execute(
            "UPDATE domain_calls SET state='running' WHERE id=?1 AND attempt_id=?2 AND generation=?3 AND state='created' AND EXISTS(SELECT 1 FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?2 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.generation=?3 AND a.state IN ('queued','running'))",
            params![call_id, attempt_id, generation_i64],
        ).map_err(sql)?;
        if changed != 1 {
            let duplicate: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_calls WHERE id=?1 AND attempt_id=?2 AND generation=?3 AND state IN ('running','completed','failed','unknown'))",
                params![call_id,attempt_id,generation_i64], |row| row.get(0)
            ).map_err(sql)?;
            if duplicate {
                // A duplicate claim changed nothing, so it records nothing.
                transaction.commit().map_err(sql)?;
                return Ok(false);
            }
            return Err(invalid("Call start rejected: Attempt authority is stale"));
        }
        let intent_moved = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='running',effect_state=CASE WHEN effect_kind='idempotent' THEN 'started' ELSE 'started' END,updated_at=?2 WHERE call_id=?1 AND state IN ('queued','pending')",
            params![call_id, now()],
        ).map_err(sql)?;
        // Claiming the Call is the fact that the external effect may now start.
        // The intent reaching `running` is caused by it, not an independent fact.
        let root = emit_call(
            &transaction,
            EventKind::CallUpdated,
            &read_call(&transaction, call_id)?
                .ok_or_else(|| invalid("claimed Call disappeared"))?,
            None,
        )?;
        if intent_moved == 1 {
            if let Some(intent) = read_dispatch_intent_by_call(&transaction, call_id)? {
                emit_dispatch_intent(
                    &transaction,
                    EventKind::DispatchIntentUpdated,
                    &intent,
                    Some(root),
                )?;
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(true)
    }

    pub fn recover_provider_dispatches(&mut self) -> Result<Vec<DispatchIntent>> {
        self.reconcile_dispatches()?;
        let mut ready = Vec::new();
        for intent in self.pending_dispatch_intents()? {
            let input: serde_json::Value = serde_json::from_str(&intent.request)
                .map_err(|error| invalid(&format!("invalid durable Call input: {error}")))?;
            let provider = input
                .get("executor_transport")
                .and_then(serde_json::Value::as_str)
                == Some("provider")
                || input
                    .get("arguments")
                    .and_then(|arguments| arguments.get("messages"))
                    .is_some_and(serde_json::Value::is_array);
            if !provider {
                continue;
            }
            let failure = if !intent.budget_admitted {
                Some("restart_incomplete_budget_admission")
            } else if intent.state == "running"
                || intent.effect_state != EffectIntentState::NotStarted
            {
                Some("restart_external_effect_unknown")
            } else if intent.executor_id.is_none() {
                Some("restart_missing_executor")
            } else {
                None
            };
            if let Some(failure) = failure {
                self.fence_dispatch_intent(&intent.call_id, failure)?;
                if failure == "restart_external_effect_unknown"
                    && self.authority(&intent.attempt_id)?.is_some()
                {
                    self.set_attempt_terminal_or_cancelling(&intent.attempt_id, "unknown", false)?;
                }
            } else {
                ready.push(intent);
            }
        }
        Ok(ready)
    }

    /// Record that a pending intent was handed to the bounded queue. This is
    /// deliberately separate from `start_call`: queue presence never grants
    /// execution authority.
    pub fn mark_dispatch_queued(&mut self, call_id: &str) -> Result<()> {
        validate_id(call_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='queued',updated_at=?2 WHERE call_id=?1 AND state IN ('pending','queued')",
            params![call_id, now()],
        ).map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is no longer pending"));
        }
        let intent = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("queued dispatch intent disappeared"))?;
        emit_dispatch_intent(
            &transaction,
            EventKind::DispatchIntentUpdated,
            &intent,
            None,
        )?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    /// Return durable work that was not terminally settled, for restart
    /// recovery. Call and Attempt authority are rechecked by the caller before
    /// handing each row back to the bounded dispatcher.
    pub fn pending_dispatch_intents(&self) -> Result<Vec<DispatchIntent>> {
        let mut statement = self.connection.prepare(
            "SELECT id,call_id,job_id,attempt_id,executor_id,generation,state,effect_kind,effect_state,request,reservation_id,budget_admitted,failure,created_at,updated_at FROM domain_dispatch_intents WHERE state IN ('pending','queued','running') ORDER BY created_at,id"
        ).map_err(sql)?;
        let rows = statement
            .query_map([], |row| {
                let effect_kind: String = row.get(7)?;
                let effect_state: String = row.get(8)?;
                Ok(DispatchIntent {
                    id: row.get(0)?,
                    call_id: row.get(1)?,
                    job_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    executor_id: row.get(4)?,
                    generation: u64::try_from(row.get::<_, i64>(5)?)
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    state: row.get(6)?,
                    effect_kind: effect_kind
                        .parse()
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    effect_state: effect_state
                        .parse()
                        .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    request: row.get(9)?,
                    reservation_id: row.get(10)?,
                    budget_admitted: row.get(11)?,
                    failure: row.get(12)?,
                    created_at: row.get(13)?,
                    updated_at: row.get(14)?,
                })
            })
            .map_err(sql)?;
        rows.map(|row| row.map_err(sql)).collect()
    }

    pub fn finish_dispatch_intent(
        &mut self,
        call_id: &str,
        state: &str,
        effect_state: EffectIntentState,
        failure: Option<&str>,
    ) -> Result<()> {
        if !matches!(state, "completed" | "failed" | "fenced") {
            return Err(invalid("invalid canonical dispatch intent state"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction.execute(
            "UPDATE domain_dispatch_intents SET state=?2,effect_state=?3,failure=?4,updated_at=?5 WHERE call_id=?1 AND state IN ('pending','queued','running')",
            params![call_id, state, effect_state.to_string(), failure, now()],
        ).map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is already terminal"));
        }
        let intent = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("finished dispatch intent disappeared"))?;
        emit_dispatch_intent(
            &transaction,
            EventKind::DispatchIntentUpdated,
            &intent,
            None,
        )?;
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    /// Fence an intent after restart when its external effect may already have
    /// happened. A late provider result must not revive this Call.
    pub fn fence_dispatch_intent(&mut self, call_id: &str, failure: &str) -> Result<()> {
        validate_id(call_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let intent_moved = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='fenced',effect_state='unknown',failure=?2,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued','running')",
            params![call_id, failure, now()],
        ).map_err(sql)?;
        let call_moved = transaction.execute(
            "UPDATE domain_calls SET state='unknown',response=?2,finished_at=?3 WHERE id=?1 AND state IN ('created','running')",
            params![call_id, failure, now()],
        ).map_err(sql)?;
        // Fencing the intent is the root fact: the Call it belongs to becomes
        // indeterminate because that intent is gone, not the other way around.
        if intent_moved == 1 {
            let intent = read_dispatch_intent_by_call(&transaction, call_id)?
                .ok_or_else(|| invalid("fenced dispatch intent disappeared"))?;
            let root = emit_dispatch_intent(
                &transaction,
                EventKind::DispatchIntentUpdated,
                &intent,
                None,
            )?;
            if call_moved == 1 {
                emit_call(
                    &transaction,
                    EventKind::CallUpdated,
                    &read_call(&transaction, call_id)?
                        .ok_or_else(|| invalid("fenced Call disappeared"))?,
                    Some(root),
                )?;
            }
        }
        transaction.commit().map_err(sql)
    }

    /// Complete a Call only when its Attempt and generation are still
    /// authoritative. Output validation happens before the durable mutation.
    pub fn finish_call(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        response: &str,
    ) -> Result<Call> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        finish_call_in(&transaction, call_id, attempt_id, generation, response)?;
        transaction.commit().map_err(sql)?;
        self.call(call_id)
    }

    /// Persist a terminal Call failure only while the Attempt authority and
    /// generation still match. This keeps provider failures from mutating a
    /// replacement Attempt's state.
    pub fn fail_call(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        failure: &str,
    ) -> Result<Call> {
        validate_id(call_id)?;
        validate_id(attempt_id)?;
        if failure.is_empty() || failure.len() > 4096 {
            return Err(invalid("invalid Call failure"));
        }
        let generation_i64 = i64::try_from(generation)
            .map_err(|_| invalid("Call generation exceeds SQLite range"))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let current: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id JOIN domain_jobs j ON j.id=a.job_id WHERE c.id=?1 AND c.attempt_id=?2 AND c.generation=?3 AND c.state IN ('created','running') AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running'))",
                params![call_id, attempt_id, generation_i64],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !current {
            return Err(invalid("Call failure rejected: Attempt authority is stale"));
        }
        let changed = transaction
            .execute(
                "UPDATE domain_calls SET state='failed',response=?2,finished_at=?3 WHERE id=?1 AND state IN ('created','running')",
                params![call_id, failure, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("Call is already terminal"));
        }
        let intent_moved = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='failed',effect_state=CASE WHEN effect_state='started' THEN 'unknown' ELSE 'not_started' END,failure=?2,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued','running')",
            params![call_id, failure, now()],
        ).map_err(sql)?;
        let call =
            read_call(&transaction, call_id)?.ok_or_else(|| invalid("failed Call disappeared"))?;
        let root = emit_call(&transaction, EventKind::CallUpdated, &call, None)?;
        if intent_moved == 1 {
            if let Some(intent) = read_dispatch_intent_by_call(&transaction, call_id)? {
                emit_dispatch_intent(
                    &transaction,
                    EventKind::DispatchIntentUpdated,
                    &intent,
                    Some(root),
                )?;
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(call)
    }

    /// Revoke authority before asking any Executor to stop.
    pub fn request_cancel(&mut self, attempt_id: &str) -> Result<AttemptAuthority> {
        let authority = self
            .authority(attempt_id)?
            .ok_or_else(|| invalid("Attempt is not currently authoritative"))?;
        self.set_attempt_terminal_or_cancelling(attempt_id, "cancelling", false)?;
        Ok(authority)
    }

    pub fn finish_attempt(&mut self, attempt_id: &str, succeeded: bool) -> Result<()> {
        let state = if succeeded { "completed" } else { "failed" };
        self.set_attempt_terminal_or_cancelling(attempt_id, state, false)
    }

    pub fn confirm_cancel(&mut self, attempt_id: &str, stopped: bool) -> Result<()> {
        let terminal = if stopped { "cancelled" } else { "unknown" };
        self.set_attempt_terminal_or_cancelling(attempt_id, terminal, true)
    }

    pub fn mark_orphaned(&mut self, attempt_id: &str) -> Result<()> {
        self.set_attempt_terminal_or_cancelling(attempt_id, "orphaned", true)
    }

    fn set_attempt_terminal_or_cancelling(
        &mut self,
        attempt_id: &str,
        state: &str,
        require_cancelling: bool,
    ) -> Result<()> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        finish_attempt_in(&transaction, attempt_id, state, require_cancelling)?;
        transaction.commit().map_err(sql)
    }

    pub fn job(&self, job_id: &str) -> Result<Option<Job>> {
        self.connection.query_row(
            "SELECT id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at FROM domain_jobs WHERE id=?1",
            [job_id], |row| {
                let state: String = row.get(2)?;
                let generation: i64 = row.get(3)?;
                Ok((row.get(0)?, row.get(1)?, state, generation, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?))
            },
        ).optional().map_err(sql)?.map(|(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at)| {
            Ok(Job { id, project_id, state: JobState::parse(&state)?, generation: u64::try_from(generation).map_err(|_| invalid("negative Job generation"))?, authoritative_attempt_id, payload, created_at, updated_at })
        }).transpose()
    }

    pub fn attempt(&self, attempt_id: &str) -> Result<Option<Attempt>> {
        self.connection.query_row(
            "SELECT id,job_id,generation,state,authoritative,created_at,finished_at FROM domain_attempts WHERE id=?1",
            [attempt_id], |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?,row.get::<_,String>(3)?,row.get::<_,bool>(4)?,row.get::<_,i64>(5)?,row.get::<_,Option<i64>>(6)?)),
        ).optional().map_err(sql)?.map(|(id,job_id,generation,state,authoritative,created_at,finished_at)| {
            let state = match state.as_str() { "queued"=>AttemptState::Queued,"running"=>AttemptState::Running,"cancelling"=>AttemptState::Cancelling,"completed"=>AttemptState::Completed,"failed"=>AttemptState::Failed,"cancelled"=>AttemptState::Cancelled,"unknown"=>AttemptState::Unknown,"orphaned"=>AttemptState::Orphaned,_=>return Err(invalid("invalid canonical Attempt state")) };
            Ok(Attempt { id, job_id, generation: u64::try_from(generation).map_err(|_| invalid("invalid Attempt generation"))?, state, authoritative, created_at, finished_at })
        }).transpose()
    }

    pub fn call(&self, call_id: &str) -> Result<Call> {
        self.connection
            .query_row(
                "SELECT c.id,c.attempt_id,c.executor_id,c.generation,c.side_effect,c.state,c.request,c.response,c.created_at,c.finished_at,i.effect_kind FROM domain_calls c JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.id=?1",
                [call_id],
                |row| {
                    Ok(Call {
                        id: row.get(0)?,
                        attempt_id: row.get(1)?,
                        executor_id: row.get(2)?,
                        generation: u64::try_from(row.get::<_, i64>(3)?).map_err(|_| rusqlite::Error::InvalidQuery)?,
                        side_effect: row.get(4)?,
                        state: row.get(5)?,
                        request: row.get(6)?,
                        response: row.get(7)?,
                        created_at: row.get(8)?,
                        finished_at: row.get(9)?,
                        effect_kind: row.get::<_, String>(10)?.parse().map_err(|_| rusqlite::Error::InvalidQuery)?,
                    })
                },
            )
            .map_err(sql)
    }

    pub fn calls_for_attempt(&self, attempt_id: &str) -> Result<Vec<Call>> {
        let mut statement = self
            .connection
            .prepare("SELECT id FROM domain_calls WHERE attempt_id=?1 ORDER BY created_at,id")
            .map_err(sql)?;
        let rows = statement
            .query_map([attempt_id], |row| row.get::<_, String>(0))
            .map_err(sql)?;
        rows.map(|row| {
            let id = row.map_err(sql)?;
            self.call(&id)
        })
        .collect()
    }
}

// -------------------------------------------------------------------------
// Journal emitters
//
// Each helper records one entity's committed post-state. They are called only
// from inside the same transaction that produced that post-state, so an event
// can never describe a fact that was rolled back, and a committed fact always
// has its event. The `caused_by` cursor links cascade records to the single
// causal fact in the same commit.
// -------------------------------------------------------------------------

fn emit_job(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    job: &Job,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(kind, "job", &job.id, journal::value(job)?)
            .project(&job.project_id)
            .job(&job.id)
            .generation(job.generation)
            .caused_by_opt(caused_by),
    )
}

/// A Job event that must keep naming the authority it completed or revoked.
/// After a terminal transition the Job no longer points at an Attempt, so
/// resolving authority from state alone would silently drop the fence value.
fn emit_job_under(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    job: &Job,
    attempt_id: &str,
    generation: u64,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(kind, "job", &job.id, journal::value(job)?)
            .project(&job.project_id)
            .job(&job.id)
            .generation(job.generation)
            .authority(attempt_id, generation)
            .caused_by_opt(caused_by),
    )
}

fn emit_attempt(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    attempt: &Attempt,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(kind, "attempt", &attempt.id, journal::value(attempt)?)
            .job(&attempt.job_id)
            .attempt_scope(&attempt.id, &attempt.job_id, attempt.generation)
            .caused_by_opt(caused_by),
    )
}

fn emit_executor(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    executor: &Executor,
    caused_by: Option<u64>,
) -> Result<u64> {
    let (job_id, generation) = journal::attempt_scope(transaction, &executor.attempt_id)?
        .ok_or_else(|| invalid("Executor refers to an Attempt that does not exist"))?;
    journal::append(
        transaction,
        EventDraft::new(kind, "executor", &executor.id, journal::value(executor)?)
            .job(&job_id)
            .attempt_scope(&executor.attempt_id, &job_id, generation)
            .executor(Some(&executor.id))
            .caused_by_opt(caused_by),
    )
}

fn emit_call(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    call: &Call,
    caused_by: Option<u64>,
) -> Result<u64> {
    let (job_id, generation) = journal::attempt_scope(transaction, &call.attempt_id)?
        .ok_or_else(|| invalid("Call refers to an Attempt that does not exist"))?;
    journal::append(
        transaction,
        EventDraft::new(kind, "call", &call.id, journal::value(call)?)
            .job(&job_id)
            .attempt_scope(&call.attempt_id, &job_id, generation)
            .executor(call.executor_id.as_deref())
            .call(Some(&call.id))
            .caused_by_opt(caused_by),
    )
}

fn emit_dispatch_intent(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    intent: &DispatchIntent,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(kind, "dispatch_intent", &intent.id, journal::value(intent)?)
            .job(&intent.job_id)
            .attempt_scope(&intent.attempt_id, &intent.job_id, intent.generation)
            .executor(intent.executor_id.as_deref())
            .call(Some(&intent.call_id))
            .dispatch_intent(Some(&intent.id))
            .caused_by_opt(caused_by),
    )
}

fn emit_dependency(
    transaction: &rusqlite::Transaction<'_>,
    kind: EventKind,
    edge: &journal::DependencyEdge,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(kind, "dependency", &edge.job_id, journal::value(edge)?)
            .project(&edge.project_id)
            .job(&edge.job_id)
            .caused_by_opt(caused_by),
    )
}

fn emit_binding(
    transaction: &rusqlite::Transaction<'_>,
    binding: &journal::JobBinding,
    caused_by: Option<u64>,
) -> Result<u64> {
    let mut draft = EventDraft::new(
        EventKind::BindingSet,
        "binding",
        &binding.binding_key,
        journal::value(binding)?,
    )
    .project(&binding.project_id)
    .job(&binding.job_id)
    .causation_key(&binding.binding_key)
    .caused_by_opt(caused_by);
    if let Some(attempt_id) = &binding.attempt_id {
        // The binding pins the exact Attempt, so the event names the authority
        // a returning session may still act under.
        if let Ok(Some((_, generation))) = journal::attempt_scope(transaction, attempt_id) {
            draft = draft.attempt_scope(attempt_id, &binding.job_id, generation);
        }
    }
    journal::append(transaction, draft)
}

fn emit_job_configuration(
    transaction: &rusqlite::Transaction<'_>,
    configuration: &journal::StoredJobConfiguration,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(
            EventKind::JobConfigurationSet,
            "job_configuration",
            &configuration.job_id,
            journal::value(configuration)?,
        )
        .job(&configuration.job_id)
        .causation_key(&configuration.job_id),
    )
}

fn emit_result_evidence(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &journal::ResultEvidence,
    job_id: &str,
    actor: &str,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(
            EventKind::ResultEvidenceRecorded,
            "result_evidence",
            &evidence.call_id,
            journal::value(evidence)?,
        )
        .job(job_id)
        .attempt_scope(&evidence.attempt_id, job_id, evidence.generation)
        .call(Some(&evidence.call_id))
        .causation_key(&evidence.call_id)
        .actor(actor),
    )
}

fn emit_verification(
    transaction: &rusqlite::Transaction<'_>,
    verification: &journal::StoredVerification,
    job_id: &str,
    attempt_id: &str,
    generation: u64,
    actor: &str,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(
            EventKind::VerificationRecorded,
            "verification",
            &verification.call_id,
            journal::value(verification)?,
        )
        .job(job_id)
        .attempt_scope(attempt_id, job_id, generation)
        .call(Some(&verification.call_id))
        .causation_key(&verification.call_id)
        .actor(actor),
    )
}

// -------------------------------------------------------------------------
// Row mapping and bulk readers
//
// These take `&Connection` so the same definitions serve both the public read
// API and the writer transaction (which derefs to a connection), and so the
// journal always records exactly what a reader would observe.
// -------------------------------------------------------------------------

fn executor_from_row(row: &Row<'_>) -> rusqlite::Result<Executor> {
    Ok(Executor {
        id: row.get(0)?,
        attempt_id: row.get(1)?,
        kind: row.get(2)?,
        state: row.get(3)?,
        created_at: row.get(4)?,
    })
}

const EXECUTOR_COLUMNS: &str = "id,attempt_id,kind,state,created_at";

fn call_from_row(row: &Row<'_>) -> rusqlite::Result<Call> {
    Ok(Call {
        id: row.get(0)?,
        attempt_id: row.get(1)?,
        executor_id: row.get(2)?,
        generation: u64::try_from(row.get::<_, i64>(3)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        side_effect: row.get(4)?,
        state: row.get(5)?,
        request: row.get(6)?,
        response: row.get(7)?,
        created_at: row.get(8)?,
        finished_at: row.get(9)?,
        effect_kind: row
            .get::<_, String>(10)?
            .parse()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

/// `Call` needs its `effect_kind`, which lives on the durable DispatchIntent.
const CALL_COLUMNS: &str = "c.id,c.attempt_id,c.executor_id,c.generation,c.side_effect,c.state,\
c.request,c.response,c.created_at,c.finished_at,i.effect_kind";

fn dispatch_intent_from_row(row: &Row<'_>) -> rusqlite::Result<DispatchIntent> {
    Ok(DispatchIntent {
        id: row.get(0)?,
        call_id: row.get(1)?,
        job_id: row.get(2)?,
        attempt_id: row.get(3)?,
        executor_id: row.get(4)?,
        generation: u64::try_from(row.get::<_, i64>(5)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        state: row.get(6)?,
        effect_kind: row
            .get::<_, String>(7)?
            .parse()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        effect_state: row
            .get::<_, String>(8)?
            .parse()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        request: row.get(9)?,
        reservation_id: row.get(10)?,
        budget_admitted: row.get(11)?,
        failure: row.get(12)?,
        created_at: row.get(13)?,
        updated_at: row.get(14)?,
    })
}

const DISPATCH_INTENT_COLUMNS: &str = "id,call_id,job_id,attempt_id,executor_id,generation,state,\
effect_kind,effect_state,request,reservation_id,budget_admitted,failure,created_at,updated_at";

fn attempt_from_row(row: &Row<'_>) -> rusqlite::Result<Attempt> {
    let state: String = row.get(3)?;
    let state = match state.as_str() {
        "queued" => AttemptState::Queued,
        "running" => AttemptState::Running,
        "cancelling" => AttemptState::Cancelling,
        "completed" => AttemptState::Completed,
        "failed" => AttemptState::Failed,
        "cancelled" => AttemptState::Cancelled,
        "unknown" => AttemptState::Unknown,
        "orphaned" => AttemptState::Orphaned,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(Attempt {
        id: row.get(0)?,
        job_id: row.get(1)?,
        generation: u64::try_from(row.get::<_, i64>(2)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        state,
        authoritative: row.get(4)?,
        created_at: row.get(5)?,
        finished_at: row.get(6)?,
    })
}

fn job_from_row(row: &Row<'_>) -> rusqlite::Result<Job> {
    let state: String = row.get(2)?;
    let state: JobState = state.parse().map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(Job {
        id: row.get(0)?,
        project_id: row.get(1)?,
        state,
        generation: u64::try_from(row.get::<_, i64>(3)?)
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        authoritative_attempt_id: row.get(4)?,
        payload: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

const JOB_COLUMNS: &str =
    "id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at";

fn query_all<T>(
    connection: &Connection,
    statement: &str,
    bind: &[&dyn rusqlite::ToSql],
    map: fn(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut prepared = connection.prepare(statement).map_err(sql)?;
    let rows = prepared.query_map(bind, map).map_err(sql)?;
    rows.map(|row| row.map_err(sql)).collect()
}

/// Record every Executor whose state actually moved.
///
/// The fencing `UPDATE` is not restricted by state, so comparing the before and
/// after rows is what keeps the journal to real changes instead of restating
/// rows that were already in their final state.
fn emit_changed_executors(
    transaction: &rusqlite::Transaction<'_>,
    before: &[Executor],
    caused_by: Option<u64>,
) -> Result<()> {
    for previous in before {
        let Some(current) = read_executor(transaction, &previous.id)? else {
            continue;
        };
        if current.state != previous.state {
            emit_executor(transaction, EventKind::ExecutorUpdated, &current, caused_by)?;
        }
    }
    Ok(())
}

fn emit_changed_calls(
    transaction: &rusqlite::Transaction<'_>,
    before: &[Call],
    caused_by: Option<u64>,
) -> Result<()> {
    for previous in before {
        let Some(current) = read_call(transaction, &previous.id)? else {
            continue;
        };
        if current.state != previous.state {
            emit_call(transaction, EventKind::CallUpdated, &current, caused_by)?;
        }
    }
    Ok(())
}

fn emit_changed_intents(
    transaction: &rusqlite::Transaction<'_>,
    before: &[DispatchIntent],
    caused_by: Option<u64>,
) -> Result<()> {
    for previous in before {
        let Some(current) = read_dispatch_intent(transaction, &previous.id)? else {
            continue;
        };
        if current.state != previous.state || current.effect_state != previous.effect_state {
            emit_dispatch_intent(
                transaction,
                EventKind::DispatchIntentUpdated,
                &current,
                caused_by,
            )?;
        }
    }
    Ok(())
}

fn read_job(connection: &Connection, job_id: &str) -> Result<Option<Job>> {
    connection
        .query_row(
            &format!("SELECT {JOB_COLUMNS} FROM domain_jobs WHERE id=?1"),
            [job_id],
            job_from_row,
        )
        .optional()
        .map_err(sql)
}

fn read_attempt(connection: &Connection, attempt_id: &str) -> Result<Attempt> {
    connection
        .query_row(
            "SELECT id,job_id,generation,state,authoritative,created_at,finished_at FROM domain_attempts WHERE id=?1",
            [attempt_id],
            attempt_from_row,
        )
        .optional()
        .map_err(sql)?
        .ok_or_else(|| invalid("Attempt disappeared while journaling its transition"))
}

fn read_executor(connection: &Connection, executor_id: &str) -> Result<Option<Executor>> {
    connection
        .query_row(
            &format!("SELECT {EXECUTOR_COLUMNS} FROM domain_executors WHERE id=?1"),
            [executor_id],
            executor_from_row,
        )
        .optional()
        .map_err(sql)
}

fn read_call(connection: &Connection, call_id: &str) -> Result<Option<Call>> {
    connection
        .query_row(
            &format!(
                "SELECT {CALL_COLUMNS} FROM domain_calls c JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.id=?1"
            ),
            [call_id],
            call_from_row,
        )
        .optional()
        .map_err(sql)
}

fn read_dispatch_intent(
    connection: &Connection,
    intent_id: &str,
) -> Result<Option<DispatchIntent>> {
    connection
        .query_row(
            &format!("SELECT {DISPATCH_INTENT_COLUMNS} FROM domain_dispatch_intents WHERE id=?1"),
            [intent_id],
            dispatch_intent_from_row,
        )
        .optional()
        .map_err(sql)
}

/// A Call owns at most one durable dispatch intent, so the intent is reachable
/// from the Call id that caused it.
fn read_dispatch_intent_by_call(
    connection: &Connection,
    call_id: &str,
) -> Result<Option<DispatchIntent>> {
    connection
        .query_row(
            &format!(
                "SELECT {DISPATCH_INTENT_COLUMNS} FROM domain_dispatch_intents WHERE call_id=?1"
            ),
            [call_id],
            dispatch_intent_from_row,
        )
        .optional()
        .map_err(sql)
}

fn read_verification(
    connection: &Connection,
    call_id: &str,
) -> Result<Option<journal::StoredVerification>> {
    let row: Option<(bool, String, i64)> = connection
        .query_row(
            "SELECT passed,report,created_at FROM domain_verifications WHERE call_id=?1",
            [call_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql)?;
    row.map(|(passed, report, created_at)| {
        Ok(journal::StoredVerification {
            call_id: call_id.to_string(),
            passed,
            report: serde_json::from_str(&report)
                .map_err(|error| invalid(&format!("invalid verification report: {error}")))?,
            created_at,
        })
    })
    .transpose()
}

fn all_projects(connection: &Connection) -> Result<Vec<Project>> {
    query_all(
        connection,
        "SELECT id,root,created_at FROM domain_projects ORDER BY created_at,id",
        &[],
        |row| {
            Ok(Project {
                id: row.get(0)?,
                root: row.get(1)?,
                created_at: row.get(2)?,
            })
        },
    )
}

fn all_jobs(connection: &Connection) -> Result<Vec<Job>> {
    query_all(
        connection,
        &format!("SELECT {JOB_COLUMNS} FROM domain_jobs ORDER BY created_at,id"),
        &[],
        job_from_row,
    )
}

fn all_attempts(connection: &Connection) -> Result<Vec<Attempt>> {
    query_all(
        connection,
        "SELECT id,job_id,generation,state,authoritative,created_at,finished_at FROM domain_attempts ORDER BY created_at,id",
        &[],
        attempt_from_row,
    )
}

fn all_executors(connection: &Connection) -> Result<Vec<Executor>> {
    query_all(
        connection,
        &format!("SELECT {EXECUTOR_COLUMNS} FROM domain_executors ORDER BY created_at,id"),
        &[],
        executor_from_row,
    )
}

/// Every Executor row owned by an Attempt, unfiltered: callers compare the
/// before/after state to decide what actually changed.
fn attempt_executors(connection: &Connection, attempt_id: &str) -> Result<Vec<Executor>> {
    query_all(
        connection,
        &format!(
            "SELECT {EXECUTOR_COLUMNS} FROM domain_executors WHERE attempt_id=?1 ORDER BY created_at,id"
        ),
        &[&attempt_id],
        executor_from_row,
    )
}

fn all_calls(connection: &Connection) -> Result<Vec<Call>> {
    query_all(
        connection,
        &format!(
            "SELECT {CALL_COLUMNS} FROM domain_calls c JOIN domain_dispatch_intents i ON i.call_id=c.id ORDER BY c.created_at,c.id"
        ),
        &[],
        call_from_row,
    )
}

fn attempt_calls(connection: &Connection, attempt_id: &str) -> Result<Vec<Call>> {
    query_all(
        connection,
        &format!(
            "SELECT {CALL_COLUMNS} FROM domain_calls c JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.attempt_id=?1 ORDER BY c.created_at,c.id"
        ),
        &[&attempt_id],
        call_from_row,
    )
    .map(|mut calls| {
        calls.retain(|call| LIVE_CALL_STATES.contains(&call.state.as_str()));
        calls
    })
}

fn all_dispatch_intents(connection: &Connection) -> Result<Vec<DispatchIntent>> {
    query_all(
        connection,
        &format!(
            "SELECT {DISPATCH_INTENT_COLUMNS} FROM domain_dispatch_intents ORDER BY created_at,id"
        ),
        &[],
        dispatch_intent_from_row,
    )
}

fn attempt_intents(connection: &Connection, attempt_id: &str) -> Result<Vec<DispatchIntent>> {
    query_all(
        connection,
        &format!(
            "SELECT {DISPATCH_INTENT_COLUMNS} FROM domain_dispatch_intents WHERE attempt_id=?1 ORDER BY created_at,id"
        ),
        &[&attempt_id],
        dispatch_intent_from_row,
    )
    .map(|mut intents| {
        intents.retain(|intent| LIVE_INTENT_STATES.contains(&intent.state.as_str()));
        intents
    })
}

fn all_dependencies(connection: &Connection) -> Result<Vec<journal::DependencyEdge>> {
    query_all(
        connection,
        "SELECT project_id,job_id,prerequisite_job_id FROM domain_job_dependencies ORDER BY project_id,job_id,prerequisite_job_id",
        &[],
        |row| {
            Ok(journal::DependencyEdge {
                project_id: row.get(0)?,
                job_id: row.get(1)?,
                prerequisite_job_id: row.get(2)?,
            })
        },
    )
}

fn all_bindings(connection: &Connection) -> Result<Vec<journal::JobBinding>> {
    query_all(
        connection,
        "SELECT binding_key,project_id,job_id,attempt_id FROM domain_job_bindings ORDER BY binding_key",
        &[],
        |row| {
            Ok(journal::JobBinding {
                binding_key: row.get(0)?,
                project_id: row.get(1)?,
                job_id: row.get(2)?,
                attempt_id: row.get(3)?,
            })
        },
    )
}

fn all_job_configurations(connection: &Connection) -> Result<Vec<journal::StoredJobConfiguration>> {
    query_all(
        connection,
        "SELECT job_id,configuration,revision FROM domain_job_configs ORDER BY job_id",
        &[],
        |row| {
            let configuration: String = row.get(1)?;
            let configuration: serde_json::Value =
                serde_json::from_str(&configuration).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        configuration.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            let revision =
                u64::try_from(row.get::<_, i64>(2)?).map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok(journal::StoredJobConfiguration {
                job_id: row.get(0)?,
                configuration,
                revision,
            })
        },
    )
}

fn all_result_evidence(connection: &Connection) -> Result<Vec<journal::ResultEvidence>> {
    query_all(
        connection,
        "SELECT call_id,attempt_id,generation,disposition,created_at FROM domain_result_evidence ORDER BY created_at,call_id,attempt_id,generation",
        &[],
        |row| {
            Ok(journal::ResultEvidence {
                call_id: row.get(0)?,
                attempt_id: row.get(1)?,
                generation: u64::try_from(row.get::<_, i64>(2)?)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
                disposition: row.get(3)?,
                created_at: row.get(4)?,
            })
        },
    )
}

fn all_verifications(connection: &Connection) -> Result<Vec<journal::StoredVerification>> {
    query_all(
        connection,
        "SELECT call_id,passed,report,created_at FROM domain_verifications ORDER BY created_at,call_id",
        &[],
        |row| {
            let report: String = row.get(2)?;
            let report: serde_json::Value = serde_json::from_str(&report).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    report.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(journal::StoredVerification {
                call_id: row.get(0)?,
                passed: row.get(1)?,
                report,
                created_at: row.get(3)?,
            })
        },
    )
}

fn finish_call_in(
    transaction: &rusqlite::Transaction<'_>,
    call_id: &str,
    attempt_id: &str,
    generation: u64,
    response: &str,
) -> Result<()> {
    validate_id(call_id)?;
    validate_id(attempt_id)?;
    let value: serde_json::Value = serde_json::from_str(response)
        .map_err(|error| OcgError::config(format!("invalid Call output JSON: {error}")))?;
    crate::orchestration::call_schema::validate_output(&serde_json::json!({
        "result": value
    }))?;
    let generation_i64 =
        i64::try_from(generation).map_err(|_| invalid("Call generation exceeds SQLite range"))?;
    let duplicate: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_calls WHERE id=?1 AND attempt_id=?2 AND generation=?3 AND state='completed' AND response=?4)",
            params![call_id, attempt_id, generation_i64, response], |row| row.get(0)
        ).map_err(sql)?;
    if duplicate {
        return Ok(());
    }
    let current: Option<(String, i64)> = transaction
            .query_row(
                "SELECT c.attempt_id,c.generation FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id JOIN domain_jobs j ON j.id=a.job_id WHERE c.id=?1 AND c.attempt_id=?2 AND c.generation=?3 AND c.state='running' AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running')",
                params![call_id, attempt_id, generation_i64],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
    if current.is_none() {
        return Err(invalid(
            "Call completion rejected: Attempt authority is stale",
        ));
    }
    let changed = transaction
            .execute(
                "UPDATE domain_calls SET state='completed',response=?2,finished_at=?3 WHERE id=?1 AND state='running'",
                params![call_id, response, now()],
            )
            .map_err(sql)?;
    if changed != 1 {
        return Err(invalid("Call is already terminal"));
    }
    let intent_moved = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='completed',effect_state='settled',updated_at=?2 WHERE call_id=?1 AND state IN ('pending','queued','running')",
            params![call_id, now()],
        ).map_err(sql)?;
    // The Call completing is the root fact; settling the external effect it was
    // admitted for is caused by it, and is recorded only when it really moved.
    let root = emit_call(
        transaction,
        EventKind::CallUpdated,
        &read_call(transaction, call_id)?.ok_or_else(|| invalid("completed Call disappeared"))?,
        None,
    )?;
    if intent_moved == 1 {
        if let Some(intent) = read_dispatch_intent_by_call(transaction, call_id)? {
            emit_dispatch_intent(
                transaction,
                EventKind::DispatchIntentUpdated,
                &intent,
                Some(root),
            )?;
        }
    }
    Ok(())
}

fn finish_attempt_in(
    transaction: &rusqlite::Transaction<'_>,
    attempt_id: &str,
    state: &str,
    require_cancelling: bool,
) -> Result<()> {
    validate_id(attempt_id)?;
    let row: Option<(String, String, bool, i64)> = transaction
            .query_row(
                "SELECT a.job_id,a.state,a.authoritative=1 AND EXISTS(SELECT 1 FROM domain_jobs j WHERE j.id=a.job_id AND j.authoritative_attempt_id=a.id),a.generation FROM domain_attempts a WHERE a.id=?1",
                [attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sql)?;
    let (job_id, current, is_authoritative, generation) =
        row.ok_or_else(|| invalid("unknown Attempt"))?;
    let generation = u64::try_from(generation)
        .map_err(|_| invalid("Attempt generation exceeds SQLite range"))?;
    if current == state && !matches!(state, "queued" | "running") {
        // The Attempt already holds exactly this terminal state, so the fact is
        // unchanged and nothing is journalized.
        return Ok(());
    }
    if !matches!(current.as_str(), "queued" | "running" | "cancelling") {
        return Err(invalid("Attempt is already terminal"));
    }
    if require_cancelling {
        if current != "cancelling" {
            return Err(invalid("Attempt cancellation has not been requested"));
        }
    } else if !is_authoritative {
        return Err(invalid("Attempt is no longer authoritative"));
    }
    let timestamp = now();
    if state == "cancelling" {
        let attempt_moved = transaction.execute("UPDATE domain_attempts SET authoritative=0,state='cancelling' WHERE id=?1 AND authoritative=1", [attempt_id]).map_err(sql)?;
        let job_moved = transaction.execute("UPDATE domain_jobs SET authoritative_attempt_id=NULL,state='cancelling',updated_at=?2 WHERE id=?1 AND authoritative_attempt_id=?3", params![job_id,timestamp,attempt_id]).map_err(sql)?;
        // Authority is revoked here, so every record that moved must keep
        // naming the authority it is revoking; the Job would otherwise lose it.
        if attempt_moved == 1 {
            emit_attempt(
                transaction,
                EventKind::AttemptUpdated,
                &read_attempt(transaction, attempt_id)?,
                None,
            )?;
        }
        if job_moved == 1 {
            let job = read_job(transaction, &job_id)?
                .ok_or_else(|| invalid("cancelling Job disappeared"))?;
            emit_job_under(
                transaction,
                EventKind::JobUpdated,
                &job,
                attempt_id,
                generation,
                None,
            )?;
        }
    } else {
        if state == "completed" {
            let unfinished: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_calls WHERE attempt_id=?1 AND state!='completed')",
                    [attempt_id], |row| row.get(0)
                ).map_err(sql)?;
            if unfinished {
                return Err(invalid("Attempt still has unsettled or unsuccessful Calls"));
            }
        }
        // Capture the rows this terminal transition settles before it moves
        // them, so each one that actually changed gets exactly one record.
        let fenced_executors = attempt_executors(transaction, attempt_id)?;
        let fenced_intents = attempt_intents(transaction, attempt_id)?;
        let fenced_calls = attempt_calls(transaction, attempt_id)?;
        transaction.execute(
                "UPDATE domain_dispatch_intents SET state='fenced',failure='attempt_terminal',effect_state=CASE WHEN effect_state='started' THEN 'unknown' ELSE effect_state END,updated_at=?2 WHERE attempt_id=?1 AND state IN ('pending','queued','running')",
                params![attempt_id, timestamp],
            ).map_err(sql)?;
        transaction.execute(
                "UPDATE domain_calls SET state='unknown',finished_at=?2 WHERE attempt_id=?1 AND state IN ('created','running')",
                params![attempt_id, timestamp],
            ).map_err(sql)?;
        transaction
            .execute(
                "UPDATE domain_executors SET state=?2 WHERE attempt_id=?1",
                params![attempt_id, state],
            )
            .map_err(sql)?;
        transaction
            .execute(
                "UPDATE domain_attempts SET authoritative=0,state=?2,finished_at=?3 WHERE id=?1",
                params![attempt_id, state, timestamp],
            )
            .map_err(sql)?;
        transaction.execute("UPDATE domain_jobs SET authoritative_attempt_id=NULL,state=?2,updated_at=?3 WHERE id=?1 AND authoritative_attempt_id=?4", params![job_id,state,timestamp,attempt_id]).map_err(sql)?;
        // The Attempt leaving authority is the single causal fact for this
        // transition; everything it settles hangs off it in the same commit.
        let root = emit_attempt(
            transaction,
            EventKind::AttemptUpdated,
            &read_attempt(transaction, attempt_id)?,
            None,
        )?;
        emit_changed_intents(transaction, &fenced_intents, Some(root))?;
        emit_changed_calls(transaction, &fenced_calls, Some(root))?;
        emit_changed_executors(transaction, &fenced_executors, Some(root))?;
        let job =
            read_job(transaction, &job_id)?.ok_or_else(|| invalid("terminal Job disappeared"))?;
        emit_job_under(
            transaction,
            EventKind::JobUpdated,
            &job,
            attempt_id,
            generation,
            Some(root),
        )?;
    }
    Ok(())
}

/// Claim an eligible Job and establish its authoritative Attempt atomically.
/// Returns the Attempt and the cursor of its `AttemptCreated` event, so a
/// caller that also publishes an Executor can record it as caused by the same
/// fact.
fn create_attempt_in(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &str,
) -> Result<(Attempt, u64)> {
    validate_id(job_id)?;
    let (state, generation): (String, i64) = transaction
        .query_row(
            "SELECT state,generation FROM domain_jobs WHERE id=?1",
            [job_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql)?
        .ok_or_else(|| invalid("unknown Job"))?;
    if state != "eligible" {
        return Err(invalid("Job is not eligible for an Attempt"));
    }
    let blocked: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_job_dependencies d JOIN domain_jobs prerequisite ON prerequisite.id=d.prerequisite_job_id WHERE d.job_id=?1 AND prerequisite.state!='completed')",
                [job_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
    if blocked {
        return Err(invalid("Job dependencies are not satisfied"));
    }
    let generation = generation
        .checked_add(1)
        .ok_or_else(|| invalid("Job generation overflow"))?;
    let attempt_id = new_id("att");
    let timestamp = now();
    transaction.execute(
            "INSERT INTO domain_attempts(id,job_id,generation,state,authoritative,created_at,finished_at) VALUES(?1,?2,?3,'queued',1,?4,NULL)",
            params![attempt_id, job_id, generation, timestamp],
        ).map_err(sql)?;
    let changed = transaction.execute(
            "UPDATE domain_jobs SET state='running',generation=?2,authoritative_attempt_id=?3,updated_at=?4 WHERE id=?1 AND state='eligible' AND authoritative_attempt_id IS NULL",
            params![job_id, generation, attempt_id, timestamp],
        ).map_err(sql)?;
    if changed != 1 {
        return Err(invalid("Job eligibility changed while creating Attempt"));
    }
    let attempt = read_attempt(transaction, &attempt_id)?;
    let job =
        read_job(transaction, job_id)?.ok_or_else(|| invalid("dispatched Job disappeared"))?;
    // Establishing the Attempt is the root fact; the Job acquiring that
    // authority is caused by it in the same commit, so no reader can observe a
    // running Job without a journaled Attempt behind it.
    let root = emit_attempt(transaction, EventKind::AttemptCreated, &attempt, None)?;
    emit_job(transaction, EventKind::JobUpdated, &job, Some(root))?;
    Ok((attempt, root))
}
