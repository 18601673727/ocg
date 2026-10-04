//! SQLite-backed canonical Project, Job, Attempt, Executor and Call records.

use crate::core_contract::{
    Actor, Conversation, EntityId, EntityKind, EntityRef, Message, MessageBlock, MessageBlockKind,
    MessageLifecycle, ProjectScope,
};
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

mod usage;

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

/// Durable Job identity and Attempt identity use SQLite text at the storage
/// boundary but remain separately named throughout the domain API.
pub type JobId = String;
pub type AttemptId = String;
pub type JobSpec = String;

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
    pub id: JobId,
    pub project_id: String,
    pub state: JobState,
    pub generation: u64,
    pub authoritative_attempt_id: Option<AttemptId>,
    #[serde(rename = "payload")]
    pub spec: JobSpec,
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
    pub id: AttemptId,
    pub job_id: JobId,
    pub generation: u64,
    pub state: AttemptState,
    pub authoritative: bool,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct RecentProjectExecution {
    pub job_id: JobId,
    pub objective: Option<String>,
    pub objective_truncated: bool,
    pub job_state: JobState,
    pub generation: u64,
    pub created_at: i64,
    pub updated_at: i64,
    pub attempt_id: Option<AttemptId>,
    pub attempt_state: Option<AttemptState>,
    pub attempt_authoritative: bool,
    pub attempt_finished_at: Option<i64>,
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
    pub attempt_id: AttemptId,
    pub kind: String,
    pub state: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Call {
    pub id: String,
    pub attempt_id: AttemptId,
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
    pub job_id: JobId,
    pub attempt_id: AttemptId,
    pub executor_id: Option<String>,
    pub generation: u64,
    pub state: String,
    pub effect_kind: EffectIntentKind,
    pub effect_state: EffectIntentState,
    pub request: String,
    pub reservation_id: Option<String>,
    pub budget_admitted: bool,
    /// The pricing basis frozen for this dispatch before the provider ran, and
    /// the only pricing a settlement of this Call may consult.
    pub pricing_basis: Option<budget::PricingBasis>,
    /// Frozen provider execution configuration for provider Calls.
    /// None for native tool Calls.
    pub provider_key: Option<String>,
    pub model: Option<String>,
    /// The provider-facing model id sent on the wire, frozen at admission.
    #[serde(default)]
    pub upstream_model_id: Option<String>,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
    pub failure: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptAuthority {
    pub attempt_id: AttemptId,
    pub job_id: JobId,
    pub generation: u64,
}

/// The identity a settlement caller *claims*. It is verified against canonical
/// state inside the accounting transaction, never trusted: the ledger decides
/// whether this Attempt may still assert an amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountingAuthority {
    pub attempt_id: AttemptId,
    pub generation: u64,
}

/// What one provider run established about a dispatched Call.
///
/// The three terminal shapes map onto the three accounting dispositions:
///
/// - `Completed` carries the provider-reported usage, if any. It becomes a
///   `Settled` actual only when a canonical price values it; otherwise the
///   reservation stays `Unresolved` and the usage is retained as evidence.
/// - `NotDispatched` is a proof, not a guess: the request never left OCG, so the
///   reservation is released in full.
/// - `Failed` and `Fenced` are both "the effect may or may not have happened".
///   Neither is ever optimistically released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchAccounting {
    Completed(ProviderSettlement),
    NotDispatched,
    Failed,
    Fenced,
}

impl DispatchAccounting {
    /// The reason code this disposition books when nothing more specific applies.
    fn default_reason(&self) -> &'static str {
        match self {
            Self::Completed { .. } => budget::REASON_ACTUAL_REPORTED,
            Self::NotDispatched => budget::REASON_NOT_DISPATCHED,
            Self::Failed | Self::Fenced => budget::REASON_EFFECT_UNKNOWN,
        }
    }
}

/// The result of applying one economic disposition to one reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountingOutcome {
    /// True when a new accounting fact was committed. False means the
    /// disposition was a duplicate and changed nothing at all.
    pub recorded: bool,
    pub disposition: budget::SettlementDisposition,
    pub reason_code: String,
    pub settlement_id: String,
    /// The money movement, present exactly when `recorded` is true.
    pub effect: Option<budget::SettlementEffect>,
    /// False when a usage record was observed but not booked because the
    /// reporting Attempt no longer held authority.
    pub usage_authoritative: bool,
}

/// What a provider run reported when it completed a Call.
///
/// The route is deliberately absent: the pricing basis was frozen against the
/// effective dispatched route before the provider ran, and a settlement values
/// usage against that frozen basis rather than against whatever route the
/// provider happened to name in its response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSettlement {
    /// The provider-reported usage, in the provider's own idiom. `None` means
    /// the provider stated nothing, which is an unknown actual and never a zero
    /// one.
    pub usage: Option<budget::UsageRecord>,
    /// The same usage normalized into non-overlapping billable quantities by
    /// the provider adapter, which is what the frozen price is applied to.
    pub billable: Option<budget::BillableUsage>,
}

/// The canonical dispatch intent a settlement operates on, read inside the
/// accounting transaction.
struct AccountingTarget {
    intent: DispatchIntent,
    project_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
pub struct ExecutionWitness {
    pub job_id: JobId,
    pub attempt_id: AttemptId,
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
    -- The pricing basis frozen for this dispatch before the provider ran. It is
    -- the only pricing a settlement of this Call may consult: the current
    -- configuration is never re-resolved at completion time.
    pricing_basis TEXT,
    -- Frozen provider execution configuration for provider Calls.
    -- NULL for native tool Calls.
    provider_key TEXT,
    model TEXT,
    upstream_model_id TEXT,
    endpoint TEXT,
    credential_ref TEXT,
    failure TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_budgets (
    project_id TEXT PRIMARY KEY REFERENCES domain_projects(id),
    budget TEXT NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS domain_settlements (
    settlement_id TEXT PRIMARY KEY,
    reservation_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    call_id TEXT NOT NULL REFERENCES domain_calls(id),
    dispatch_intent_id TEXT REFERENCES domain_dispatch_intents(id),
    attempt_id TEXT NOT NULL REFERENCES domain_attempts(id),
    generation INTEGER NOT NULL CHECK(generation > 0),
    disposition TEXT NOT NULL CHECK(disposition IN ('settled','released','unresolved')),
    reason_code TEXT NOT NULL,
    usage_authoritative INTEGER NOT NULL CHECK(usage_authoritative IN (0,1)),
    -- The content digest of the settlement payload, so a re-delivery of the same
    -- identity with different content is a conflict rather than a duplicate.
    payload_digest TEXT NOT NULL DEFAULT '',
    settlement TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS domain_settlements_by_call
    ON domain_settlements(project_id,call_id,created_at,settlement_id);
CREATE INDEX IF NOT EXISTS domain_settlements_by_reservation
    ON domain_settlements(reservation_id,created_at);
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
CREATE TABLE IF NOT EXISTS domain_launch_commands (
    command_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    request_hash TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('accepted','rejected','failed')),
    job_id TEXT REFERENCES domain_jobs(id),
    message TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(command_id, project_id)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_conversations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    session_id TEXT NOT NULL,
    record TEXT NOT NULL,
    UNIQUE(project_id,session_id)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_chat_turns (
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    command_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    conversation_id TEXT NOT NULL REFERENCES domain_conversations(id),
    turn_order INTEGER NOT NULL CHECK(turn_order > 0),
    job_id TEXT NOT NULL UNIQUE REFERENCES domain_jobs(id),
    attempt_id TEXT NOT NULL UNIQUE REFERENCES domain_attempts(id),
    PRIMARY KEY(project_id,command_id),
    UNIQUE(conversation_id,turn_order)
) STRICT;
CREATE TABLE IF NOT EXISTS domain_messages (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES domain_chat_turns(attempt_id),
    role TEXT NOT NULL CHECK(role IN ('user','assistant')),
    record TEXT NOT NULL,
    UNIQUE(attempt_id,role)
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

pub struct ChatMessageOrigin {
    pub command_id: String,
    pub job_id: String,
    pub attempt_state: String,
    pub failure_reason: Option<String>,
}

pub struct ChatHistory {
    pub conversation: Conversation,
    pub messages: Vec<Message>,
    pub origins: std::collections::HashMap<String, ChatMessageOrigin>,
}

impl DomainRepository {
    /// Open the durable store at a boundary without deciding what Project owns
    /// it.
    ///
    /// Opening a store and creating a Project identity are two separate facts.
    /// A whole Project — its `.ocg` store included — can move to a new
    /// directory, and the store it carries already holds the durable Project
    /// that must survive the move. Creating a Project row here would mint a
    /// second identity for the same durable store and make that new row look
    /// like the authoritative one. Callers that only need to read or reconcile
    /// what a store already holds use this entry point; callers that require a
    /// Project to exist use [`DomainRepository::open`].
    pub fn open_existing(root: &Path) -> Result<Self> {
        crate::install::ensure_gitignore(root)?;
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
        ensure_column(
            &connection,
            "domain_dispatch_intents",
            "pricing_basis",
            "TEXT",
        )?;
        ensure_column(
            &connection,
            "domain_settlements",
            "payload_digest",
            "TEXT NOT NULL DEFAULT ''",
        )?;
        ensure_column(
            &connection,
            "domain_dispatch_intents",
            "upstream_model_id",
            "TEXT",
        )?;
        ensure_column(&connection, "domain_job_bindings", "attempt_id", "TEXT")?;
        connection.execute(
            "UPDATE domain_job_bindings SET attempt_id=(SELECT a.id FROM domain_attempts a WHERE a.job_id=domain_job_bindings.job_id ORDER BY a.generation DESC LIMIT 1) WHERE attempt_id IS NULL",
            [],
        ).map_err(sql)?;
        Ok(Self { connection, path })
    }

    /// Open the durable store and require a Project identity at this boundary.
    pub fn open(root: &Path) -> Result<Self> {
        let repository = Self::open_existing(root)?;
        repository.ensure_project(root)?;
        Ok(repository)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn conversations(&self, project_id: &str) -> Result<Vec<(String, Conversation)>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT session_id,record FROM domain_conversations WHERE project_id=?1
             ORDER BY CAST(json_extract(record,'$.updated_at') AS INTEGER) DESC,id DESC",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map([project_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql)?;
        rows.map(|row| {
            let (session_id, record) = row.map_err(sql)?;
            Ok((session_id, decode_chat_record(&record)?))
        })
        .collect()
    }

    pub fn delete_conversation(&self, project_id: &str, session_id: &str) -> Result<()> {
        validate_id(project_id)?;
        validate_id(session_id)?;
        let transaction = self.begin()?;
        let record: Option<String> = transaction
            .query_row(
                "SELECT record FROM domain_conversations WHERE project_id=?1 AND session_id=?2",
                params![project_id, session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        if let Some(record) = record {
            let mut conversation: Conversation = decode_chat_record(&record)?;
            let active: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM domain_chat_turns t JOIN domain_attempts a ON a.id=t.attempt_id
                     WHERE t.conversation_id=?1 AND a.state IN ('queued','running','cancelling'))",
                    [conversation.id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql)?;
            if active {
                return Err(invalid("Conversation is running. Stop it before deleting."));
            }
            if conversation.archived_at.is_none() {
                // Keep execution and accounting references intact while making the chat inaccessible.
                let timestamp = now().to_string();
                conversation.archived_at = Some(timestamp.clone());
                conversation.updated_at = timestamp;
                conversation.revision += 1;
                transaction
                    .execute(
                        "UPDATE domain_conversations SET record=?1 WHERE project_id=?2 AND session_id=?3",
                        params![encode_chat_record(&conversation)?, project_id, session_id],
                    )
                    .map_err(sql)?;
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    pub fn conversation_history(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Option<(Conversation, Vec<Message>)>> {
        Ok(self
            .conversation_history_with_origins(project_id, session_id)?
            .map(|history| (history.conversation, history.messages)))
    }

    pub fn conversation_history_with_origins(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<Option<ChatHistory>> {
        let transaction = self.connection.unchecked_transaction().map_err(sql)?;
        let record: Option<String> = transaction
            .query_row(
                "SELECT record FROM domain_conversations WHERE project_id=?1 AND session_id=?2",
                params![project_id, session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        let Some(record) = record else {
            return Ok(None);
        };
        let conversation: Conversation = decode_chat_record(&record)?;
        if conversation.archived_at.is_some() {
            return Ok(None);
        }
        let messages = read_conversation_messages(&transaction, conversation.id.as_str())?;
        let mut statement = transaction
            .prepare(
                "SELECT m.id,t.command_id,t.job_id,a.state,
                 (SELECT i.failure FROM domain_dispatch_intents i
                  WHERE i.attempt_id=t.attempt_id AND i.failure IS NOT NULL
                    AND i.state IN ('failed','fenced')
                  ORDER BY CASE i.state WHEN 'failed' THEN 0 ELSE 1 END,
                    i.updated_at DESC,i.created_at DESC,i.id DESC LIMIT 1)
                 FROM domain_messages m
             JOIN domain_chat_turns t ON t.attempt_id=m.attempt_id
             JOIN domain_conversations c ON c.id=t.conversation_id
             JOIN domain_attempts a ON a.id=t.attempt_id
             WHERE c.project_id=?1 AND c.session_id=?2",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map(params![project_id, session_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    ChatMessageOrigin {
                        command_id: row.get(1)?,
                        job_id: row.get(2)?,
                        attempt_state: row.get(3)?,
                        failure_reason: row.get(4)?,
                    },
                ))
            })
            .map_err(sql)?;
        let origins = rows.map(|row| row.map_err(sql)).collect::<Result<_>>()?;
        drop(statement);
        transaction.commit().map_err(sql)?;
        Ok(Some(ChatHistory {
            conversation,
            messages,
            origins,
        }))
    }

    pub fn prepare_chat_turn(
        &self,
        request: &crate::contracts::JobLaunchRequest,
        request_hash: &str,
        attempt: &Attempt,
        content: &str,
    ) -> Result<Vec<serde_json::Value>> {
        self.prepare_chat_turn_with_images(request, request_hash, attempt, content, &[])
    }

    pub fn prepare_chat_turn_with_images(
        &self,
        request: &crate::contracts::JobLaunchRequest,
        request_hash: &str,
        attempt: &Attempt,
        content: &str,
        images: &[crate::contracts::ChatImage],
    ) -> Result<Vec<serde_json::Value>> {
        validate_id(&request.session_id)?;
        let transaction = self.begin()?;
        let owns_project: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_jobs WHERE id=?1 AND project_id=?2)",
                params![attempt.job_id, request.project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !owns_project {
            return Err(invalid("chat Attempt belongs to another Project"));
        }
        let timestamp = now().to_string();
        let candidate = Conversation {
            id: EntityId::new(uuid::Uuid::now_v7().to_string())?,
            project_scope: ProjectScope::new(&request.project_id)?,
            title: None,
            revision: 0,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
            archived_at: None,
        };
        transaction.execute(
            "INSERT INTO domain_conversations(id,project_id,session_id,record) VALUES(?1,?2,?3,?4) ON CONFLICT(project_id,session_id) DO NOTHING",
            params![candidate.id.as_str(), request.project_id, request.session_id, encode_chat_record(&candidate)?],
        ).map_err(sql)?;
        let record: String = transaction
            .query_row(
                "SELECT record FROM domain_conversations WHERE project_id=?1 AND session_id=?2",
                params![request.project_id, request.session_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        let conversation: Conversation = decode_chat_record(&record)?;
        if conversation.archived_at.is_some() {
            return Err(invalid("Conversation has been deleted."));
        }
        let turn_order: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(turn_order),0)+1 FROM domain_chat_turns WHERE conversation_id=?1",
            [conversation.id.as_str()], |row| row.get(0),
        ).map_err(sql)?;
        transaction.execute(
            "INSERT INTO domain_chat_turns(project_id,command_id,request_hash,conversation_id,turn_order,job_id,attempt_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![request.project_id, request.command_id, request_hash, conversation.id.as_str(), turn_order, attempt.job_id, attempt.id],
        ).map_err(sql)?;
        let attempt_ref = EntityRef {
            kind: EntityKind::Attempt,
            id: EntityId::new(attempt.id.strip_prefix("att-").unwrap_or(&attempt.id))?,
        };
        let user = Message {
            id: EntityId::new(uuid::Uuid::now_v7().to_string())?,
            project_scope: conversation.project_scope.clone(),
            conversation_ref: EntityRef {
                kind: EntityKind::Conversation,
                id: conversation.id.clone(),
            },
            author: Actor::User {
                user_id: "local".to_string(),
            },
            produced_by_attempt_ref: None,
            blocks: std::iter::once(chat_text_block(content))
                .chain(images.iter().map(chat_image_block))
                .collect(),
            state: MessageLifecycle::Pending,
            revision: 0,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            deleted_at: None,
        };
        let assistant = Message {
            id: EntityId::new(uuid::Uuid::now_v7().to_string())?,
            author: Actor::Attempt {
                attempt_ref: attempt_ref.clone(),
            },
            produced_by_attempt_ref: Some(attempt_ref),
            blocks: Vec::new(),
            ..user.clone()
        };
        for (role, message) in [("user", &user), ("assistant", &assistant)] {
            transaction
                .execute(
                    "INSERT INTO domain_messages(id,attempt_id,role,record) VALUES(?1,?2,?3,?4)",
                    params![
                        message.id.as_str(),
                        attempt.id,
                        role,
                        encode_chat_record(message)?
                    ],
                )
                .map_err(sql)?;
        }
        // Only complete canonical messages enter history. The current staged
        // user is included explicitly, before the immutable Call is admitted.
        let root: String = transaction
            .query_row(
                "SELECT root FROM domain_projects WHERE id=?1",
                [&request.project_id],
                |row| row.get(0),
            )
            .map_err(sql)?;
        let mut history = Vec::new();
        for message in read_conversation_messages(&transaction, conversation.id.as_str())? {
            if message.state != MessageLifecycle::Complete && message.id != user.id {
                continue;
            }
            let role = if matches!(message.author, Actor::User { .. }) {
                "user"
            } else {
                "assistant"
            };
            let content = message
                .blocks
                .iter()
                .filter(|block| block.kind == MessageBlockKind::Markdown)
                .filter_map(|block| block.content.as_deref())
                .collect::<Vec<_>>()
                .join("\n\n");
            let mut parts = vec![serde_json::json!({"type": "text", "text": content})];
            for block in message
                .blocks
                .iter()
                .filter(|block| block.kind == MessageBlockKind::Image)
            {
                let image: crate::contracts::ChatImage = serde_json::from_value(
                    block
                        .raw
                        .clone()
                        .ok_or_else(|| invalid("image block has no image"))?,
                )
                .map_err(|error| invalid(&error.to_string()))?;
                let url = crate::chat_images::upstream_url(
                    std::path::Path::new(&root),
                    &request.project_id,
                    &image,
                )?;
                parts.push(serde_json::json!({"type": "image_url", "image_url": {"url": url}}));
            }
            let content = if parts.len() == 1 {
                serde_json::Value::String(content)
            } else {
                serde_json::Value::Array(parts)
            };
            history.push(serde_json::json!({"role": role, "content": content}));
        }
        bump_conversation(&transaction, conversation.id.as_str())?;
        transaction.commit().map_err(sql)?;
        Ok(history)
    }

    pub(crate) fn first_chat_project(&self, attempt_id: &str) -> Result<Option<Project>> {
        let transaction = self.begin()?;
        // A replacement Attempt may own the original turn's Job. Later user
        // turns remain ineligible even when an earlier turn failed.
        let turn: Option<(String, i64, i64, Project)> = transaction.query_row(
            "SELECT t.conversation_id,t.turn_order,(SELECT MIN(turn_order) FROM domain_chat_turns WHERE conversation_id=t.conversation_id),p.id,p.root,p.created_at
             FROM domain_attempts a JOIN domain_chat_turns t ON t.job_id=a.job_id
             JOIN domain_jobs j ON j.id=a.job_id JOIN domain_projects p ON p.id=j.project_id
             JOIN domain_conversations c ON c.id=t.conversation_id AND c.project_id=p.id
             WHERE a.id=?1 AND t.project_id=p.id ORDER BY t.turn_order LIMIT 1",
            [attempt_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,Project {id:row.get(3)?,root:row.get(4)?,created_at:row.get(5)?})),
        ).optional().map_err(sql)?;
        let Some((conversation, order, first, project)) = turn else {
            return Ok(None);
        };
        if order != first {
            return Ok(None);
        }
        let completed_assistant = read_conversation_messages(&transaction, &conversation)?
            .iter()
            .any(|message| {
                message.state == MessageLifecycle::Complete
                    && matches!(message.author, Actor::Attempt { .. })
            });
        transaction.commit().map_err(sql)?;
        Ok((!completed_assistant).then_some(project))
    }

    pub fn accept_chat_turn(&self, attempt_id: &str) -> Result<()> {
        let transaction = self.begin()?;
        let turn: Option<(String, String, String, String)> = transaction.query_row(
            "SELECT project_id,command_id,request_hash,job_id FROM domain_chat_turns WHERE attempt_id=?1",
            [attempt_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
        ).optional().map_err(sql)?;
        if let Some((project_id, command_id, request_hash, job_id)) = turn {
            update_chat_message(
                &transaction,
                attempt_id,
                "user",
                MessageLifecycle::Complete,
                None,
            )?;
            transaction.execute(
                "INSERT INTO domain_launch_commands(command_id,project_id,request_hash,outcome,job_id,message,created_at) VALUES(?1,?2,?3,'accepted',?4,?5,?6) ON CONFLICT(command_id,project_id) DO NOTHING",
                params![command_id, project_id, request_hash, job_id, format!("job launched: {job_id}"), now()],
            ).map_err(sql)?;
        }
        transaction.commit().map_err(sql)
    }

    pub fn discard_unaccepted_chat_turn(&self, attempt_id: &str) -> Result<()> {
        let transaction = self.begin()?;
        let record: Option<String> = transaction
            .query_row(
                "SELECT record FROM domain_messages WHERE attempt_id=?1 AND role='user'",
                [attempt_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)?;
        if let Some(record) = record {
            let message: Message = decode_chat_record(&record)?;
            if message.state == MessageLifecycle::Complete {
                update_chat_message(
                    &transaction,
                    attempt_id,
                    "user",
                    MessageLifecycle::Deleted,
                    None,
                )?;
            }
        }
        transaction.commit().map_err(sql)
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
            reservations: all_reservations(view)?,
            settlements: all_settlements(view)?,
            budget_limits: all_budget_limits(view)?,
            usage_evidence: all_usage_evidence(view)?,
        };
        transaction.commit().map_err(sql)?;
        Ok(snapshot)
    }

    pub fn recent_project_execution(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<(Vec<RecentProjectExecution>, bool)> {
        if !(1..=10).contains(&limit) {
            return Err(invalid("recent execution limit must be 1..10"));
        }
        // Terminalization clears the authority pointer. Its last generation is
        // still the producing Attempt, never an arbitrary older/orphaned row.
        // Project only objective text, not the potentially large Job payload.
        let mut statement = self.connection.prepare(
            "SELECT j.id,CASE WHEN json_valid(j.payload) THEN CASE WHEN json_type(j.payload,'$.objective')='text' THEN substr(json_extract(j.payload,'$.objective'),1,513) END END,j.state,j.generation,j.created_at,j.updated_at,a.id,a.state,coalesce(a.authoritative,0),a.finished_at,j.authoritative_attempt_id FROM domain_jobs j LEFT JOIN domain_attempts a ON a.job_id=j.id AND a.generation=j.generation AND ((j.authoritative_attempt_id=a.id AND (a.authoritative=1 OR (j.state='cancelling' AND a.state='cancelling'))) OR (j.authoritative_attempt_id IS NULL AND j.state IN ('completed','failed','cancelled','unknown','orphaned') AND a.state=j.state AND a.authoritative=0)) WHERE j.project_id=?1 ORDER BY j.updated_at DESC,j.id DESC LIMIT ?2"
        ).map_err(sql)?;
        let rows = statement
            .query_map(params![project_id, (limit + 1) as i64], |row| {
                let mut objective: Option<String> = row.get(1)?;
                let objective_truncated = objective.as_ref().is_some_and(|text| text.len() > 512);
                if let Some(text) = &mut objective {
                    let mut boundary = text.len().min(512);
                    while !text.is_char_boundary(boundary) {
                        boundary -= 1;
                    }
                    text.truncate(boundary);
                }
                let job_state: String = row.get(2)?;
                let attempt_state: Option<String> = row.get(7)?;
                let pointer: Option<String> = row.get(10)?;
                let attempt_id: Option<String> = row.get(6)?;
                let generation = u64::try_from(row.get::<_, i64>(3)?)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                if pointer.is_some() && attempt_id.is_none() {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let state: JobState = job_state
                    .parse()
                    .map_err(|_| rusqlite::Error::InvalidQuery)?;
                if generation > 0
                    && matches!(
                        state,
                        JobState::Completed
                            | JobState::Failed
                            | JobState::Cancelled
                            | JobState::Unknown
                            | JobState::Orphaned
                    )
                    && attempt_id.is_none()
                {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                let attempt_state: Option<AttemptState> = attempt_state
                    .map(|state| state.parse().map_err(|_| rusqlite::Error::InvalidQuery))
                    .transpose()?;
                let attempt_authoritative: bool = row.get(8)?;
                if matches!(state, JobState::Running | JobState::Cancelling) && pointer.is_none() {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                if pointer.is_some()
                    && !((state == JobState::Running
                        && attempt_authoritative
                        && matches!(
                            attempt_state,
                            Some(AttemptState::Queued | AttemptState::Running)
                        ))
                        || (state == JobState::Cancelling
                            && !attempt_authoritative
                            && attempt_state == Some(AttemptState::Cancelling)))
                {
                    return Err(rusqlite::Error::InvalidQuery);
                }
                Ok(RecentProjectExecution {
                    job_id: row.get(0)?,
                    objective,
                    objective_truncated,
                    job_state: state,
                    generation,
                    created_at: row.get(4)?,
                    updated_at: row.get(5)?,
                    attempt_id,
                    attempt_state,
                    attempt_authoritative,
                    attempt_finished_at: row.get(9)?,
                })
            })
            .map_err(sql)?;
        let mut entries = rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)?;
        let truncated =
            entries.len() > limit || entries.iter().any(|entry| entry.objective_truncated);
        entries.truncate(limit);
        Ok((entries, truncated))
    }

    pub fn project_budget(&self, project_id: &str) -> Result<budget::ProjectBudgetReceipt> {
        Ok(read_budget(&self.connection, project_id)?.receipt())
    }

    /// The durable accounting facts recorded for one Project, newest last.
    /// They are a read model of the ledger; the journal keeps the durable
    /// evidence even after a bounded ledger entry is pruned.
    pub fn project_settlements(&self, project_id: &str) -> Result<Vec<budget::Settlement>> {
        query_all(
            &self.connection,
            "SELECT settlement FROM domain_settlements WHERE project_id=?1 ORDER BY created_at,settlement_id",
            &[&project_id],
            |row| {
                let raw: String = row.get(0)?;
                serde_json::from_str(&raw).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        raw.len(),
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })
            },
        )
    }

    /// Set or replace the Project's hard budget: the only supported way past a
    /// cap, and an explicit operator change rather than an approval.
    ///
    /// It is a Money mutation, so the limit that results is journalized in the
    /// same commit that stored it.
    pub fn set_project_budget(&mut self, project_id: &str, amount: budget::Money) -> Result<bool> {
        validate_id(project_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let mut ledger = read_budget(&transaction, project_id)?;
        let changed = ledger.set_hard_limit(amount, now())?;
        store_budget(&transaction, project_id, &mut ledger)?;
        if changed {
            emit_budget_limit(&transaction, project_id, &ledger)?;
        }
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
    /// budget is stored beside the Project/Job/Attempt authority so a replayed
    /// ledger cannot authorize a provider Call.
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
        let before = read_dispatch_intent_by_call(&transaction, operation_id)?;
        let mut ledger = read_budget(&transaction, project_id)?;
        let materialized = ledger.materialize_config(config);
        let existing = ledger
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
        let mut assessment = budget::admit(&ledger, config.require_quota, &request);
        let mut reserved_id = existing;
        let mut recorded_reservation = false;
        if assessment.is_allowed() {
            if let Some(amount) = assessment.amount.clone() {
                let reservation_id = budget::reservation_id(
                    SpendAction::ProviderDispatch,
                    project_id,
                    generation_i32,
                    operation_id,
                );
                recorded_reservation = ledger.reserve(
                    SpendAction::ProviderDispatch,
                    project_id,
                    generation_i32,
                    operation_id,
                    amount,
                    now(),
                );
                assessment.reservation_id = Some(reservation_id.clone());
                reserved_id = Some(reservation_id);
            }
        }
        let reason_changed = ledger.reason.as_deref() != Some(assessment.reason_code.as_str());
        ledger.reason = Some(assessment.reason_code.clone());
        let mut changed = materialized || reason_changed || recorded_reservation;
        if assessment.is_allowed() {
            let attached = transaction
                .execute(
                    "UPDATE domain_dispatch_intents SET reservation_id=?2,budget_admitted=1,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued')",
                    params![operation_id, assessment.reservation_id.as_deref(), now()],
                )
                .map_err(sql)?;
            if attached == 0 {
                // A replay of an already-admitted dispatch carries the very same
                // reservation. Anything else means the intent has already moved
                // past admission, and the reservation must not be left dangling.
                let current = read_dispatch_intent_by_call(&transaction, operation_id)?
                    .ok_or_else(|| {
                        invalid("canonical dispatch intent disappeared during budget admission")
                    })?;
                if current.reservation_id.as_deref() != assessment.reservation_id.as_deref() {
                    return Err(invalid(
                        "canonical dispatch intent is no longer budget-admittable",
                    ));
                }
            } else {
                changed = true;
            }
        }
        if changed {
            store_budget(&transaction, project_id, &mut ledger)?;
            // Every admission that moved the durable budget leaves one fact
            // behind: a cap that was materialized, a reservation that was taken,
            // or a reason that changed. A denied admission is recorded too, so
            // "the budget refused this" is reconstructable and not just absent.
            emit_budget_limit(&transaction, project_id, &ledger)?;
        }
        let current =
            read_dispatch_intent_by_call(&transaction, operation_id)?.ok_or_else(|| {
                invalid("canonical dispatch intent disappeared during budget admission")
            })?;
        // The intent acquiring the reservation is the root fact; the money it now
        // holds is caused by it in the same commit, so no reader can observe a
        // held reservation whose operation is unrecorded.
        let mut root = None;
        if before.as_ref() != Some(&current) {
            root = Some(emit_dispatch_intent(
                &transaction,
                EventKind::DispatchIntentUpdated,
                &current,
                None,
            )?);
        }
        if recorded_reservation {
            if let Some(reservation) = ledger
                .reservations
                .iter()
                .find(|reservation| Some(&reservation.reservation_id) == reserved_id.as_ref())
            {
                emit_reservation(
                    &transaction,
                    &journal::ReservationFact {
                        project_id: project_id.to_string(),
                        call_id: operation_id.to_string(),
                        reservation: reservation.clone(),
                    },
                    &current,
                    root,
                )?;
            }
        }
        transaction.commit().map_err(sql)?;
        Ok(assessment)
    }

    /// Associate a budget reservation with its durable dispatch intent. This
    /// is idempotent so a retry cannot create a second economic obligation.
    ///
    /// It is a change to the intent's economic identity, so it is journalized
    /// with the intent in one commit.
    pub fn attach_dispatch_reservation(
        &mut self,
        call_id: &str,
        reservation_id: Option<&str>,
    ) -> Result<()> {
        let transaction = self.begin()?;
        let before = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("dispatch intent is no longer attachable"))?;
        let changed = transaction
            .execute(
                "UPDATE domain_dispatch_intents SET reservation_id=?2,budget_admitted=1,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued')",
                params![call_id, reservation_id, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is no longer attachable"));
        }
        let after = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("dispatch intent is no longer attachable"))?;
        if after != before {
            emit_dispatch_intent(&transaction, EventKind::DispatchIntentUpdated, &after, None)?;
        }
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    pub fn dispatch_intent(&self, call_id: &str) -> Result<Option<DispatchIntent>> {
        read_dispatch_intent_by_call(&self.connection, call_id)
    }

    pub fn mark_budget_admitted(&mut self, call_id: &str) -> Result<()> {
        let transaction = self.begin()?;
        let before = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("dispatch intent is no longer budget-admittable"))?;
        let changed = transaction
            .execute(
                "UPDATE domain_dispatch_intents SET budget_admitted=1,updated_at=?2 WHERE call_id=?1 AND state IN ('pending','queued')",
                params![call_id, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent is no longer budget-admittable"));
        }
        let after = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("dispatch intent is no longer budget-admittable"))?;
        if after != before {
            emit_dispatch_intent(&transaction, EventKind::DispatchIntentUpdated, &after, None)?;
        }
        transaction.commit().map_err(sql)?;
        Ok(())
    }

    /// Freeze the pricing basis for one dispatch, durably, before the provider
    /// is asked to do anything.
    ///
    /// This is the only moment pricing is resolved for a Call. The basis names
    /// the route that was actually dispatched — which is what the price is
    /// resolved against — and records the price and the revision it came from.
    /// From here on a settlement of this Call values usage against the frozen
    /// basis and nothing else: the current configuration is never consulted at
    /// completion time, so a later pricing revision cannot reprice a dispatch
    /// that already happened.
    ///
    /// It is idempotent per Call: the first freeze wins, and a second call for
    /// the same Call is a no-op rather than a re-pricing. A Call that was
    /// admitted before this existed simply has no basis, and its settlement
    /// reports that instead of inventing one.
    ///
    /// Returns `false` when the Call has no dispatch intent that can still be
    /// frozen, which the caller must treat as "do not dispatch".
    pub fn freeze_dispatch_pricing(
        &mut self,
        call_id: &str,
        basis: &budget::PricingBasis,
    ) -> Result<bool> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        // The first freeze wins. A retry of the same dispatch finds the basis
        // already there and proceeds with it rather than re-pricing the Call.
        let existing: Option<Option<String>> = transaction
            .query_row(
                "SELECT pricing_basis FROM domain_dispatch_intents WHERE call_id=?1",
                [call_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(sql)?;
        if existing.flatten().is_some() {
            transaction.commit().map_err(sql)?;
            return Ok(true);
        }
        let changed = transaction
            .execute(
                "UPDATE domain_dispatch_intents SET pricing_basis=?2,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued') AND pricing_basis IS NULL",
                params![
                    call_id,
                    serde_json::to_string(basis)
                        .map_err(|error| invalid(&format!("serialize pricing basis: {error}")))?,
                    now()
                ],
            )
            .map_err(sql)?;
        if changed != 1 {
            // No dispatch intent can still be frozen: it has already moved past
            // admission, so the caller must not dispatch it.
            transaction.commit().map_err(sql)?;
            return Ok(false);
        }
        let after = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("dispatch intent disappeared during pricing freeze"))?;
        emit_dispatch_intent(&transaction, EventKind::DispatchIntentUpdated, &after, None)?;
        transaction.commit().map_err(sql)?;
        Ok(true)
    }

    /// Read the pricing basis frozen for one Call, if one was frozen.
    ///
    /// This is the only pricing a settlement of the Call may consult. It is
    /// exposed so the provider adapter can normalize the provider's reported
    /// counters against the same price the settlement will apply, which keeps
    /// the quantities it bills and the price that bills them from ever coming
    /// from different revisions.
    pub fn dispatch_pricing_basis(&self, call_id: &str) -> Result<Option<budget::PricingBasis>> {
        let found: Option<Option<String>> = self
            .connection
            .query_row(
                "SELECT pricing_basis FROM domain_dispatch_intents WHERE call_id=?1",
                [call_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(sql)?;
        let Some(raw) = found.flatten() else {
            return Ok(None);
        };
        let basis = serde_json::from_str(&raw)
            .map_err(|error| invalid(&format!("invalid frozen pricing basis: {error}")))?;
        Ok(Some(basis))
    }

    /// Apply exactly one economic disposition to one dispatched Call.
    ///
    /// This is the single accounting entry point. Every path that ends a
    /// provider-costly dispatch — completion with usage, a failure, a
    /// disconnect, a fence, a late result, an attempt terminal transition —
    /// reaches the ledger through it, so they cannot drift into different
    /// meanings for `reserved`, `actual`, `released` and `unresolved`.
    ///
    /// The Money mutation, the durable settlement row and the journal accounting
    /// facts all commit in the caller's single immediate transaction, so money
    /// can never move without exactly one durable record of what moved.
    ///
    /// Pricing is never resolved here. The usage is valued against the basis
    /// frozen for the dispatch before the provider ran, so the canonical Money
    /// for a Call cannot depend on the pricing configuration that happens to
    /// exist when the provider reports back.
    pub fn settle_dispatch_accounting(
        &mut self,
        call_id: &str,
        claim: &AccountingAuthority,
        accounting: &DispatchAccounting,
    ) -> Result<AccountingOutcome> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let target = read_accounting_target(&transaction, call_id)?
            .ok_or_else(|| invalid("no durable dispatch intent exists for this Call"))?;
        let outcome = apply_accounting_in(&transaction, &target, claim, accounting, None)?;
        transaction.commit().map_err(sql)?;
        Ok(outcome)
    }

    /// Complete a provider Call and settle its reservation in one transaction.
    ///
    /// The Call's canonical completion, the dispatch intent's terminal state and
    /// the Money settlement are one commit: there is no window in which a Call
    /// is durably complete but its spend is neither reserved nor settled. A
    /// stale Attempt fails the completion, so it can never book an actual.
    pub fn complete_provider_dispatch(
        &mut self,
        call_id: &str,
        claim: &AccountingAuthority,
        response: &str,
        reported: &ProviderSettlement,
    ) -> Result<AccountingOutcome> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let root = finish_call_in(
            &transaction,
            call_id,
            &claim.attempt_id,
            claim.generation,
            response,
        )?;
        let target = read_accounting_target(&transaction, call_id)?
            .ok_or_else(|| invalid("no durable dispatch intent exists for this Call"))?;
        let outcome = apply_accounting_in(
            &transaction,
            &target,
            claim,
            &DispatchAccounting::Completed(reported.clone()),
            root,
        )?;
        transaction.commit().map_err(sql)?;
        Ok(outcome)
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

    /// Resolve the durable Project for a canonical root, adopting the existing
    /// durable identity when the whole Project store (its `.ocg` directory)
    /// moved with the directory. Never mints a second identity for the same
    /// durable store: when no row matches the new root but the store holds
    /// exactly one Project row, that row's recorded root is repaired in place.
    /// With zero rows a new Project is created; with several and no root match
    /// the durable identity is ambiguous and must not be guessed.
    /// Read the durable Project recorded at a canonical root, if any. This
    /// never creates or changes identity.
    pub fn project_at_root(&self, root: &Path) -> Result<Option<Project>> {
        let root = crate::project::canonicalize(root)
            .to_string_lossy()
            .to_string();
        self.connection
            .query_row(
                "SELECT id,root,created_at FROM domain_projects WHERE root=?1",
                [&root],
                |row| {
                    Ok(Project {
                        id: row.get(0)?,
                        root: row.get(1)?,
                        created_at: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(sql)
    }

    /// Resolve the durable Project for a canonical root, adopting the existing
    /// durable identity when the whole Project store (its `.ocg` directory)
    /// moved with the directory. Never mints a second identity for the same
    /// durable store: when no row matches the new root but the store holds
    /// exactly one Project row, that row's recorded root is repaired in place.
    /// With zero rows a new Project is created; with several and no root match
    /// the durable identity is ambiguous and must not be guessed.
    pub fn reconcile_project(&self, root: &Path) -> Result<Project> {
        let root = crate::project::canonicalize(root)
            .to_string_lossy()
            .to_string();
        if let Some(project) = self.project_at_root(Path::new(&root))? {
            return Ok(project);
        }
        let projects = all_projects(&self.connection)?;
        match projects.len() {
            0 => self.ensure_project(Path::new(&root)),
            1 => {
                self.connection
                    .execute(
                        "UPDATE domain_projects SET root=?1 WHERE id=?2",
                        params![&root, &projects[0].id],
                    )
                    .map_err(sql)?;
                Ok(Project {
                    id: projects[0].id.clone(),
                    root,
                    created_at: projects[0].created_at,
                })
            }
            _ => Err(invalid(
                "ambiguous durable Project identity at this boundary",
            )),
        }
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
            spec: payload.to_string(),
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
            spec: payload.to_string(),
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
            spec: payload.to_string(),
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
    /// no legacy Job, Attempt, or JSON recovery record participates.
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
                            authoritative_attempt_id: row.get(4)?, spec: row.get(5)?,
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
            spec: payload.to_string(),
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
            "INSERT INTO domain_dispatch_intents(id,call_id,job_id,attempt_id,executor_id,generation,state,effect_kind,effect_state,request,reservation_id,provider_key,model,endpoint,credential_ref,failure,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,'pending',?7,'not_started',?8,NULL,NULL,NULL,NULL,NULL,NULL,?9,?9)",
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
            "UPDATE domain_dispatch_intents SET state='running',effect_state='started',updated_at=?2 WHERE call_id=?1 AND state IN ('queued','pending')",
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

    /// Freeze provider execution configuration for a specific Call.
    /// This must be called after create_call_with_effect and before mark_dispatch_queued.
    pub fn set_provider_config(
        &mut self,
        call_id: &str,
        provider_key: &str,
        model: &str,
        upstream_model_id: &str,
        endpoint: &str,
        credential_ref: Option<&str>,
    ) -> Result<()> {
        validate_id(call_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction
            .execute(
                "UPDATE domain_dispatch_intents SET provider_key=?2,model=?3,upstream_model_id=?4,endpoint=?5,credential_ref=?6,updated_at=?7 WHERE call_id=?1 AND state='pending' AND provider_key IS NULL",
                params![call_id, provider_key, model, upstream_model_id, endpoint, credential_ref, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("dispatch intent not found for provider config"));
        }
        let intent = read_dispatch_intent_by_call(&transaction, call_id)?
            .ok_or_else(|| invalid("configured dispatch intent disappeared"))?;
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
        let mut statement = self.connection.prepare(&format!(
            "SELECT {DISPATCH_INTENT_COLUMNS} FROM domain_dispatch_intents WHERE state IN ('pending','queued','running') ORDER BY created_at,id"
        )).map_err(sql)?;
        let rows = statement
            .query_map([], dispatch_intent_from_row)
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
            // Fencing is also the moment the reservation stops being a live
            // expectation: the money may or may not have been spent, so it is
            // retained and marked unresolved in the same commit. Nothing here
            // can release it.
            apply_fenced_accounting_in(&transaction, call_id, Some(root))?;
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

    pub(crate) fn record_provider_context_costs(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        costs: &crate::provider_loop::context_cost::ContextCosts,
    ) -> Result<()> {
        let metadata = serde_json::to_value(costs)
            .map_err(|error| invalid(&format!("invalid provider accounting: {error}")))?;
        if serde_json::to_vec(&metadata)
            .map_err(|error| invalid(&error.to_string()))?
            .len()
            > 256 * 1024
        {
            return Err(invalid("provider accounting exceeds metadata bound"));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let call = read_call(&transaction, call_id)?.ok_or_else(|| invalid("unknown Call"))?;
        let request: serde_json::Value = serde_json::from_str(&call.request)
            .map_err(|_| invalid("invalid provider Call request"))?;
        if call.attempt_id != attempt_id
            || call.generation != generation
            || request["executor_transport"] != "provider"
        {
            return Err(invalid("provider accounting identity mismatch"));
        }
        // Observability can be appended to this producing Call after fencing or
        // cancellation. It never changes authority, lifecycle or settlement.
        let mut response = call
            .response
            .as_deref()
            .and_then(|response| serde_json::from_str::<serde_json::Value>(response).ok())
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| match &call.response {
                Some(failure) => serde_json::json!({"failure": failure}),
                None => serde_json::json!({}),
            });
        response["context_costs"] = metadata;
        let response =
            serde_json::to_string(&response).map_err(|error| invalid(&error.to_string()))?;
        if call.response.as_deref() != Some(&response) {
            transaction
                .execute(
                    "UPDATE domain_calls SET response=?2 WHERE id=?1",
                    params![call_id, response],
                )
                .map_err(sql)?;
            let updated =
                read_call(&transaction, call_id)?.ok_or_else(|| invalid("unknown Call"))?;
            emit_call(&transaction, EventKind::CallUpdated, &updated, None)?;
        }
        transaction.commit().map_err(sql)
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
        self.fail_call_checked(call_id, attempt_id, generation, failure, false, None)?
            .ok_or_else(|| invalid("Call failure rejected: Attempt authority is stale"))
    }

    pub fn fail_unclaimed_call(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        failure: &str,
    ) -> Result<bool> {
        Ok(self
            .fail_call_checked(call_id, attempt_id, generation, failure, true, None)?
            .is_some())
    }

    pub(crate) fn fail_native_tool_call(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        response: &str,
    ) -> Result<Call> {
        let result = crate::native_tools::ToolResult::validate_response(response, false)?;
        let failure = result
            .error
            .as_ref()
            .map(|error| error.kind.as_str())
            .ok_or_else(|| invalid("native tool failure has no error"))?;
        self.fail_call_checked(
            call_id,
            attempt_id,
            generation,
            failure,
            false,
            Some(response),
        )?
        .ok_or_else(|| invalid("native tool failure rejected: Attempt authority is stale"))
    }

    fn fail_call_checked(
        &mut self,
        call_id: &str,
        attempt_id: &str,
        generation: u64,
        failure: &str,
        unclaimed: bool,
        tool_response: Option<&str>,
    ) -> Result<Option<Call>> {
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
        if let Some(response) = tool_response {
            let call = read_call(&transaction, call_id)?
                .ok_or_else(|| invalid("unknown native tool Call"))?;
            let request: serde_json::Value = serde_json::from_str(&call.request)
                .map_err(|_| invalid("invalid native tool Call request"))?;
            if request.get("kind").and_then(serde_json::Value::as_str) != Some("native_tool") {
                return Err(invalid(
                    "structured tool failure requires a native tool Call",
                ));
            }
            let duplicate = call.attempt_id == attempt_id
                && call.generation == generation
                && call.state == "failed"
                && call.response.as_deref() == Some(response);
            if duplicate {
                transaction.commit().map_err(sql)?;
                return Ok(Some(call));
            }
            if call.state != "running"
                || read_dispatch_intent_by_call(&transaction, call_id)?.is_none_or(|intent| {
                    intent.state != "running" || intent.effect_state != EffectIntentState::Started
                })
            {
                return Err(invalid("native tool failure requires the current claim"));
            }
        }
        let current: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM domain_calls c JOIN domain_attempts a ON a.id=c.attempt_id JOIN domain_jobs j ON j.id=a.job_id WHERE c.id=?1 AND c.attempt_id=?2 AND c.generation=?3 AND c.state IN ('created','running') AND (?4=0 OR c.state='created') AND a.generation=?3 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id AND a.state IN ('queued','running'))",
                params![call_id, attempt_id, generation_i64, unclaimed],
                |row| row.get(0),
            )
            .map_err(sql)?;
        if !current {
            if unclaimed {
                return Ok(None);
            }
            return Err(invalid("Call failure rejected: Attempt authority is stale"));
        }
        let mut provider_response = None;
        if tool_response.is_none() {
            let call = read_call(&transaction, call_id)?.ok_or_else(|| invalid("unknown Call"))?;
            if serde_json::from_str::<serde_json::Value>(&call.request)
                .is_ok_and(|request| request["executor_transport"] == "provider")
            {
                if let Some(costs) = call
                    .response
                    .as_deref()
                    .and_then(|response| serde_json::from_str::<serde_json::Value>(response).ok())
                    .and_then(|response| response.get("context_costs").cloned())
                {
                    provider_response = Some(
                        serde_json::to_string(&serde_json::json!({
                            "failure": failure,
                            "context_costs": costs,
                        }))
                        .map_err(|error| invalid(&error.to_string()))?,
                    );
                }
            }
        }
        let failure_response = tool_response.or(provider_response.as_deref()).unwrap_or(failure);
        let changed = transaction
            .execute(
                "UPDATE domain_calls SET state='failed',response=?2,finished_at=?3 WHERE id=?1 AND state IN ('created','running')",
                params![call_id, failure_response, now()],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("Call is already terminal"));
        }
        let intent_moved = transaction.execute(
            "UPDATE domain_dispatch_intents SET state='failed',effect_state=CASE WHEN ?4 THEN 'settled' WHEN effect_state='started' THEN 'unknown' ELSE 'not_started' END,failure=?2,updated_at=?3 WHERE call_id=?1 AND state IN ('pending','queued','running')",
            params![call_id, failure, now(), tool_response.is_some()],
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
        Ok(Some(call))
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
            Ok(Job { id, project_id, state: JobState::parse(&state)?, generation: u64::try_from(generation).map_err(|_| invalid("negative Job generation"))?, authoritative_attempt_id, spec: payload, created_at, updated_at })
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
                &format!("SELECT {CALL_COLUMNS} FROM domain_calls c LEFT JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.id=?1"),
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

    pub(crate) fn first_provider_call_input(
        &self,
        attempt_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let input: Option<String> = self.connection.query_row(
            "SELECT request FROM domain_calls WHERE attempt_id=?1 AND CASE WHEN json_valid(request) THEN json_extract(request,'$.executor_transport')='provider' ELSE 1 END ORDER BY created_at,id LIMIT 1",
            [attempt_id], |row| row.get(0),
        ).optional().map_err(sql)?;
        input
            .map(|input| {
                serde_json::from_str(&input)
                    .map_err(|error| invalid(&format!("invalid frozen Provider input: {error}")))
            })
            .transpose()
    }

    /// Check for an existing launch command and return its outcome if found.
    pub fn check_launch_command(
        &self,
        command_id: &str,
        project_id: &str,
        request_hash: &str,
    ) -> Result<Option<(String, Option<String>, String)>> {
        self.connection
            .query_row(
                "SELECT outcome,job_id,message FROM domain_launch_commands WHERE command_id=?1 AND project_id=?2",
                params![command_id, project_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)?
            .map(|(outcome, job_id, message)| {
                // Verify request hash matches to detect conflicting retries
                let stored_hash: String = self.connection
                    .query_row(
                        "SELECT request_hash FROM domain_launch_commands WHERE command_id=?1 AND project_id=?2",
                        params![command_id, project_id],
                        |row| row.get(0),
                    )
                    .map_err(sql)?;
                if stored_hash != request_hash {
                    return Err(invalid("command_id reused with different request content"));
                }
                Ok((outcome, job_id, message))
            })
            .transpose()
    }

    /// Record a launch command outcome for idempotency.
    pub fn lookup_launch_command(
        &self,
        command_id: &str,
        project_id: &str,
    ) -> Result<Option<(String, Option<String>, String, String)>> {
        let mut statement = self
            .connection
            .prepare("SELECT request_hash, job_id, outcome, message FROM domain_launch_commands WHERE command_id = ?1 AND project_id = ?2")
            .map_err(sql)?;
        let result = statement
            .query_row(params![command_id, project_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .optional()
            .map_err(sql)?;
        if result.is_some() {
            return Ok(result);
        }
        // A crash during staging must not let the same command create another
        // turn. Staged messages remain non-complete and cannot enter context.
        self.connection.query_row(
            "SELECT request_hash,job_id,'failed','chat admission was interrupted' FROM domain_chat_turns WHERE command_id=?1 AND project_id=?2",
            params![command_id, project_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional().map_err(sql)
    }

    pub fn record_launch_command(
        &mut self,
        command_id: &str,
        project_id: &str,
        request_hash: &str,
        outcome: &str,
        job_id: Option<&str>,
        message: &str,
    ) -> Result<()> {
        validate_id(command_id)?;
        validate_id(project_id)?;
        let timestamp = now();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let changed = transaction
            .execute(
                "INSERT INTO domain_launch_commands(command_id,project_id,request_hash,outcome,job_id,message,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(command_id,project_id) DO UPDATE SET outcome=excluded.outcome,message=excluded.message WHERE domain_launch_commands.request_hash=excluded.request_hash AND domain_launch_commands.job_id IS excluded.job_id",
                params![command_id, project_id, request_hash, outcome, job_id, message, timestamp],
            )
            .map_err(sql)?;
        if changed != 1 {
            return Err(invalid("launch command conflicts with recorded admission"));
        }
        transaction.commit().map_err(sql)
    }

    /// Atomically claim a command_id and create a Job, ensuring exactly one Job
    /// per (command_id, project_id) even under concurrent requests.
    ///
    /// Returns:
    /// - Ok(Some(job_id)) if this call created the Job
    /// - Ok(None) if command_id already exists (duplicate/conflict detected)
    /// - Err if validation or database error
    pub fn try_claim_command_and_create_job(
        &mut self,
        command_id: &str,
        project_id: &str,
        request_hash: &str,
        payload: &str,
    ) -> Result<Option<Job>> {
        validate_id(command_id)?;
        validate_id(project_id)?;

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;

        // Try to claim the command_id
        let claim_result = transaction.execute(
            "INSERT INTO domain_launch_commands(command_id,project_id,request_hash,outcome,job_id,message,created_at) VALUES(?1,?2,?3,'pending',NULL,'job creation in progress',?4)",
            params![command_id, project_id, request_hash, now()],
        );

        match claim_result {
            Ok(_) => {
                // Successfully claimed; now create the Job
                let job_id = new_id("job");
                let timestamp = now();

                transaction
                    .execute(
                        "INSERT INTO domain_jobs(id,project_id,state,generation,authoritative_attempt_id,payload,created_at,updated_at) VALUES(?1,?2,'pending',0,NULL,?3,?4,?4)",
                        params![job_id, project_id, payload, timestamp],
                    )
                    .map_err(sql)?;

                let job = Job {
                    id: job_id.clone(),
                    project_id: project_id.to_string(),
                    state: JobState::Pending,
                    generation: 0,
                    authoritative_attempt_id: None,
                    spec: payload.to_string(),
                    created_at: timestamp,
                    updated_at: timestamp,
                };

                emit_job(&transaction, EventKind::JobCreated, &job, None)?;

                transaction.commit().map_err(sql)?;
                Ok(Some(job))
            }
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                // Command_id already exists; this is a duplicate or conflict
                Ok(None)
            }
            Err(e) => Err(sql(e)),
        }
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

/// The Project budget's durable accounting state, journalized whenever it moves:
/// a cap that was materialized or explicitly set, a reservation taken, or an
/// admission that changed the reason. A reader can reconstruct what the cap and
/// the rollups were at any cursor, and can never treat the event as the cap
/// itself.
fn emit_budget_limit(
    transaction: &rusqlite::Transaction<'_>,
    project_id: &str,
    ledger: &budget::ProjectBudget,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(
            EventKind::BudgetLimitSet,
            "budget_limit",
            project_id,
            journal::value(&journal::BudgetLimitFact {
                project_id: project_id.to_string(),
                hard_limit: ledger.hard_limit.clone(),
                origin: ledger.origin,
                status: ledger.status,
                currency: ledger.currency.clone(),
                settled_micros: ledger.settled.micros,
                reserved_micros: ledger.reserved.micros,
                unresolved_micros: ledger.unresolved.micros,
                reason: ledger.reason.clone(),
                updated_at: ledger.updated_at,
            })?,
        )
        .project(project_id)
        .causation_key(project_id),
    )
}

/// A reservation's post-state, scoped to the Attempt that holds the authority
/// over the operation it was taken for.
fn emit_reservation(
    transaction: &rusqlite::Transaction<'_>,
    fact: &journal::ReservationFact,
    intent: &DispatchIntent,
    caused_by: Option<u64>,
) -> Result<u64> {
    let kind = match fact.reservation.state {
        budget::ReservationState::Reserved => EventKind::BudgetReservationRecorded,
        budget::ReservationState::Settled => EventKind::BudgetSettled,
        budget::ReservationState::Released => EventKind::BudgetReservationReleased,
    };
    journal::append(
        transaction,
        EventDraft::new(
            kind,
            "reservation",
            &fact.reservation.reservation_id,
            journal::value(fact)?,
        )
        .project(&fact.project_id)
        .job(&intent.job_id)
        .attempt_scope(&intent.attempt_id, &intent.job_id, intent.generation)
        .executor(intent.executor_id.as_deref())
        .call(Some(&intent.call_id))
        .dispatch_intent(Some(&intent.id))
        .causation_key(&fact.reservation.reservation_id)
        .caused_by_opt(caused_by),
    )
}

/// One accounting fact: how a reservation was discharged, from which usage, at
/// which price, under which authority.
fn emit_settlement(
    transaction: &rusqlite::Transaction<'_>,
    settlement: &budget::Settlement,
    intent: &DispatchIntent,
    caused_by: Option<u64>,
) -> Result<u64> {
    let kind = match settlement.disposition {
        budget::SettlementDisposition::Settled => EventKind::BudgetSettled,
        budget::SettlementDisposition::Released => EventKind::BudgetReservationReleased,
        budget::SettlementDisposition::Unresolved => EventKind::BudgetUnresolved,
    };
    journal::append(
        transaction,
        EventDraft::new(
            kind,
            "settlement",
            &settlement.settlement_id,
            journal::value(settlement)?,
        )
        .project(&settlement.project_id)
        .job(&intent.job_id)
        .attempt_scope(&intent.attempt_id, &intent.job_id, intent.generation)
        .executor(intent.executor_id.as_deref())
        .call(Some(&settlement.call_id))
        .dispatch_intent(settlement.dispatch_intent_id.as_deref())
        .causation_key(&settlement.settlement_id)
        .caused_by_opt(caused_by),
    )
}

/// A usage record that was observed but not booked. It is evidence that a spend
/// happened, and it deliberately asserts no Money.
fn emit_usage_evidence(
    transaction: &rusqlite::Transaction<'_>,
    evidence: &journal::UsageEvidence,
    intent: &DispatchIntent,
    caused_by: Option<u64>,
) -> Result<u64> {
    journal::append(
        transaction,
        EventDraft::new(
            EventKind::BudgetUsageRetained,
            "usage_evidence",
            &evidence.call_id,
            journal::value(evidence)?,
        )
        .project(&evidence.project_id)
        .job(&intent.job_id)
        .attempt_scope(&evidence.attempt_id, &intent.job_id, evidence.generation)
        .executor(intent.executor_id.as_deref())
        .call(Some(&evidence.call_id))
        .dispatch_intent(evidence.dispatch_intent_id.as_deref())
        .causation_key(&evidence.call_id)
        .caused_by_opt(caused_by),
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

// A missing intent must not hide its Call from failure settlement or its journal.
// Without the frozen kind, a side-effecting Call is conservatively strict-fenced.
const CALL_COLUMNS: &str = "c.id,c.attempt_id,c.executor_id,c.generation,c.side_effect,c.state,\
c.request,c.response,c.created_at,c.finished_at,COALESCE(i.effect_kind,CASE WHEN c.side_effect=1 THEN 'strict_fenced' ELSE 'idempotent' END)";

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
        pricing_basis: match row.get::<_, Option<String>>(12)? {
            Some(raw) => Some(serde_json::from_str(&raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    raw.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?),
            None => None,
        },
        provider_key: row.get(13)?,
        model: row.get(14)?,
        endpoint: row.get(15)?,
        credential_ref: row.get(16)?,
        failure: row.get(17)?,
        created_at: row.get(18)?,
        updated_at: row.get(19)?,
        upstream_model_id: row.get(20)?,
    })
}

const DISPATCH_INTENT_COLUMNS: &str = "id,call_id,job_id,attempt_id,executor_id,generation,state,\
effect_kind,effect_state,request,reservation_id,budget_admitted,pricing_basis,provider_key,model,endpoint,credential_ref,failure,created_at,updated_at,upstream_model_id";

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
        spec: row.get(5)?,
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
                "SELECT {CALL_COLUMNS} FROM domain_calls c LEFT JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.id=?1"
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

// -------------------------------------------------------------------------
// Canonical Money accounting
//
// The Project budget row is the only budget authority. Every movement of money
// goes through `apply_accounting_in`, which is called from inside the writer
// transaction that made the corresponding execution change, so the Money
// mutation, its durable settlement row and its journal facts are one commit.
// -------------------------------------------------------------------------

fn read_budget(connection: &Connection, project_id: &str) -> Result<budget::ProjectBudget> {
    let raw: Option<String> = connection
        .query_row(
            "SELECT budget FROM domain_budgets WHERE project_id=?1",
            [project_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql)?;
    raw.map(|raw| {
        serde_json::from_str::<budget::ProjectBudget>(&raw)
            .map_err(|error| invalid(&format!("invalid canonical Project budget: {error}")))
    })
    .transpose()
    .map(|ledger| ledger.unwrap_or_default())
}

fn store_budget(
    connection: &Connection,
    project_id: &str,
    ledger: &mut budget::ProjectBudget,
) -> Result<()> {
    connection.execute(
        "INSERT INTO domain_budgets(project_id,budget,updated_at) VALUES(?1,?2,?3) ON CONFLICT(project_id) DO UPDATE SET budget=excluded.budget,updated_at=excluded.updated_at",
        params![
            project_id,
            serde_json::to_string(ledger)
                .map_err(|error| invalid(&format!("serialize canonical Project budget: {error}")))?,
            now()
        ],
    ).map_err(sql)?;
    Ok(())
}

/// Resolve the intent a settlement operates on, together with the Project that
/// owns its budget. Project is the budget authority scope: there is no separate
/// Job budget that could authorize or account for a provider Call.
fn read_accounting_target(
    connection: &Connection,
    call_id: &str,
) -> Result<Option<AccountingTarget>> {
    let Some(intent) = read_dispatch_intent_by_call(connection, call_id)? else {
        return Ok(None);
    };
    let project_id: String = connection
        .query_row(
            "SELECT project_id FROM domain_jobs WHERE id=?1",
            [&intent.job_id],
            |row| row.get(0),
        )
        .map_err(sql)?;
    Ok(Some(AccountingTarget { intent, project_id }))
}

/// Whether the Attempt an intent was admitted under is still the Job's single
/// authority. This is read from canonical state, never from the caller's claim.
fn attempt_holds_authority(connection: &Connection, intent: &DispatchIntent) -> Result<bool> {
    let generation = i64::try_from(intent.generation)
        .map_err(|_| invalid("Attempt generation exceeds SQLite range"))?;
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_attempts a JOIN domain_jobs j ON j.id=a.job_id WHERE a.id=?1 AND a.generation=?2 AND a.authoritative=1 AND j.authoritative_attempt_id=a.id)",
            params![intent.attempt_id, generation],
            |row| row.get(0),
        )
        .map_err(sql)
}

/// Whether the reporting caller is the Attempt the reservation belongs to, and
/// that Attempt is still the Job's authority.
///
/// A caller that fails either test is stale. It may still *retain* a
/// reservation — that only makes the budget stricter — but it may never assert
/// an actual or hand money back, because both assert a fact about the world only
/// the current authority is allowed to state.
fn settlement_may_assert(
    connection: &Connection,
    target: &AccountingTarget,
    claim: &AccountingAuthority,
) -> Result<bool> {
    Ok(claim.attempt_id == target.intent.attempt_id
        && claim.generation == target.intent.generation
        && attempt_holds_authority(connection, &target.intent)?)
}

/// Apply one economic disposition to one reservation, and journal everything it
/// produces, inside the caller's writer transaction.
///
/// Three independent guards keep the ledger honest:
///
/// 1. The reservation's own state. A terminal reservation discharges nothing and
///    can never be charged again.
/// 2. The durable settlement row. Its id is deterministic over the reservation,
///    the authority, the disposition and the reason, so the same fact delivered
///    twice — a duplicated provider result, a retried settlement, a replayed
///    commit — is dropped even after the bounded reservation ledger has pruned
///    the entry.
/// 3. The payload digest of that row. Identity is not equality: the same id can
///    arrive carrying different content, and that is a conflict that must never
///    be booked, replaced, or silently dropped as a duplicate.
///
/// Pricing is never resolved here. The usage is valued against the basis frozen
/// for the dispatch before the provider ran, so the canonical Money for a Call
/// cannot depend on the pricing configuration that happens to exist when the
/// provider reports back.
#[allow(clippy::too_many_lines)]
fn apply_accounting_in(
    transaction: &rusqlite::Transaction<'_>,
    target: &AccountingTarget,
    claim: &AccountingAuthority,
    accounting: &DispatchAccounting,
    caused_by: Option<u64>,
) -> Result<AccountingOutcome> {
    let timestamp = now();
    let mut ledger = read_budget(transaction, &target.project_id)?;
    let may_assert = settlement_may_assert(transaction, target, claim)?;
    let mut usage: Option<budget::UsageRecord> = None;
    let mut billable: Option<budget::BillableUsage> = None;
    let mut price_fact: Option<budget::TokenPrice> = None;
    let mut actual: Option<budget::Money> = None;
    let mut disposition = budget::SettlementDisposition::Unresolved;
    let mut reason_code = accounting.default_reason().to_string();
    match accounting {
        DispatchAccounting::Completed(reported) => match &reported.usage {
            Some(record) => {
                usage = Some(record.clone());
                billable = reported.billable.clone();
                // The frozen basis is the only pricing this Call may be valued
                // against. It was resolved against the effective dispatched
                // route before the provider ran, so neither a later pricing
                // revision nor a route the provider happened to name in its
                // response can change what this usage is worth.
                let basis = target.intent.pricing_basis.as_ref();
                let price = basis.and_then(|basis| basis.price.as_ref());
                let outcome = match basis {
                    // No basis was frozen for this dispatch at all, which is a
                    // fact about the dispatch: a price configured later does not
                    // value this Call.
                    None => budget::PriceOutcome::Unpriced(budget::PriceRefusal {
                        reason_code: budget::REASON_USAGE_UNPRICED,
                        reason: "no pricing basis was frozen for this dispatch, so the reservation is retained rather than valued from a guess".to_string(),
                    }),
                    Some(basis) => match basis.price.as_ref() {
                        None => budget::PriceOutcome::Unpriced(budget::PriceRefusal {
                            reason_code: budget::REASON_USAGE_UNPRICED,
                            reason: format!(
                                "no price for {}/{} was frozen for this dispatch, so the reservation is retained rather than valued from a guess",
                                basis.effective_provider, basis.effective_model
                            ),
                        }),
                        Some(price) => match billable.as_ref() {
                            Some(billable) => price.price_usage(billable, &ledger.currency),
                            // The provider stated usage but the adapter could not
                            // normalize it into billable quantities. That is an
                            // unknown component, not a zero one.
                            None => budget::PriceOutcome::Unpriced(budget::PriceRefusal {
                                reason_code: budget::REASON_USAGE_INCOMPLETE,
                                reason: format!(
                                    "the usage reported for {}/{} could not be normalized into billable quantities, so the actual would be a guess",
                                    basis.effective_provider, basis.effective_model
                                ),
                            }),
                        },
                    },
                };
                match outcome {
                    budget::PriceOutcome::Priced(money) => {
                        actual = Some(money);
                        disposition = budget::SettlementDisposition::Settled;
                        price_fact = price.cloned();
                    }
                    budget::PriceOutcome::Unpriced(refusal) => {
                        reason_code = refusal.reason_code.to_string();
                    }
                }
            }
            // The provider finished the call but stated no usage. The spend is
            // real and its amount is unknown, so the reservation is retained in
            // full rather than settled at a fabricated number.
            None => reason_code = budget::REASON_USAGE_ABSENT.to_string(),
        },
        DispatchAccounting::NotDispatched => {
            disposition = budget::SettlementDisposition::Released;
        }
        DispatchAccounting::Failed | DispatchAccounting::Fenced => {}
    }
    if !may_assert && disposition.asserts_actual() {
        // Downgrade, never upgrade: a losing authority can retain money but
        // never assert or return it.
        if actual.is_some() {
            reason_code = budget::REASON_STALE_AUTHORITY.to_string();
        } else {
            reason_code = budget::REASON_RELEASE_UNAUTHORIZED.to_string();
        }
        disposition = budget::SettlementDisposition::Unresolved;
        actual = None;
        price_fact = None;
    }
    let usage_authoritative = may_assert && actual.is_some();
    let Some(reservation_id) = target.intent.reservation_id.clone() else {
        return Ok(AccountingOutcome {
            recorded: false,
            disposition,
            reason_code: budget::REASON_NO_RESERVATION.to_string(),
            settlement_id: String::new(),
            effect: None,
            usage_authoritative: false,
        });
    };
    let settlement_id = budget::settlement_id(
        &reservation_id,
        &target.intent.attempt_id,
        target.intent.generation,
        disposition,
        &reason_code,
    );
    // Guard 2 and 3: the durable settlement row decides whether this fact is new,
    // a duplicate, or a conflict. The writer holds the single-writer lock for
    // this whole immediate transaction, so reading the row and then acting on it
    // is atomic against any other accounting writer. This runs before the
    // reservation-state guard below, because a re-delivered fact for an
    // already-discharged reservation is exactly where a conflicting payload
    // would otherwise be mistaken for an ordinary duplicate.
    let existing: Option<(String, String)> = transaction
        .query_row(
            "SELECT payload_digest,settlement FROM domain_settlements WHERE settlement_id=?1",
            [&settlement_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql)?;
    if let Some((stored_digest, stored_raw)) = existing {
        // The candidate carries the effect this fact *would* have booked, so a
        // re-delivery compares equal to the settlement it repeats even after the
        // reservation has gone terminal and can no longer be re-derived from
        // the ledger. The reserved amount comes from the ledger when the entry
        // is still there and from the recorded settlement when it is not.
        let stored: budget::Settlement = serde_json::from_str(&stored_raw)
            .map_err(|error| invalid(&format!("invalid recorded settlement: {error}")))?;
        let reserved = ledger
            .reservations
            .iter()
            .find(|reservation| reservation.reservation_id == reservation_id)
            .map(|reservation| reservation.amount.clone())
            .unwrap_or_else(|| stored.effect.reserved.clone());
        let candidate = budget::Settlement {
            settlement_id: settlement_id.clone(),
            reservation_id: reservation_id.clone(),
            project_id: target.project_id.clone(),
            call_id: target.intent.call_id.clone(),
            dispatch_intent_id: Some(target.intent.id.clone()),
            attempt_id: target.intent.attempt_id.clone(),
            generation: target.intent.generation,
            disposition,
            pricing: price_fact.clone(),
            usage: usage.clone(),
            billable: billable.clone(),
            payload_digest: String::new(),
            effect: budget::SettlementEffect::for_discharge(
                disposition,
                actual.as_ref(),
                &reserved,
            ),
            reason_code: reason_code.clone(),
            usage_authoritative,
            created_at: timestamp,
        };
        let candidate_digest = budget::settlement_payload_digest(&candidate);
        if stored_digest == candidate_digest {
            // The same fact, delivered twice. No money moves, nothing is
            // journalized.
            return Ok(AccountingOutcome {
                recorded: false,
                disposition,
                reason_code,
                settlement_id,
                effect: None,
                usage_authoritative,
            });
        }
        // Same identity, different content. This is not a duplicate: the two
        // payloads disagree about what actually happened, and neither may
        // replace the other. The conflicting payload is retained as evidence
        // under its own idempotent id and asserts no Money, so the canonical
        // settlement and the reservation it discharged are untouched.
        return record_settlement_conflict(
            transaction,
            target,
            &candidate,
            &candidate_digest,
            caused_by,
        );
    }
    // A terminal reservation can never be charged again. A live one is
    // discharged, and whether the same fact has already been recorded is decided
    // by the durable settlement id above rather than by the unresolved flag, so
    // a later fact carrying new information still gets its own record.
    let live_reservation = ledger
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
        .filter(|reservation| reservation.state == budget::ReservationState::Reserved)
        .cloned();
    let Some(reserved_entry) = live_reservation else {
        // Already settled or released: a duplicate changes nothing at all. No
        // money moves and nothing is journalized.
        return Ok(AccountingOutcome {
            recorded: false,
            disposition,
            reason_code,
            settlement_id: String::new(),
            effect: None,
            usage_authoritative,
        });
    };
    let effect = match disposition {
        budget::SettlementDisposition::Settled => ledger
            .settle_actual(
                &reservation_id,
                actual
                    .clone()
                    .ok_or_else(|| invalid("a settled disposition requires a known actual"))?,
                timestamp,
            )?
            .ok_or_else(|| invalid("a live reservation could not be settled"))?,
        budget::SettlementDisposition::Released => ledger
            .release(&reservation_id, timestamp)
            .ok_or_else(|| invalid("a live reservation could not be released"))?,
        budget::SettlementDisposition::Unresolved => {
            // Retaining an already-unresolved reservation moves no money, but the
            // fact is still new when it carries a reason or usage the earlier
            // record did not, so the record is kept either way.
            ledger
                .mark_unresolved(&reservation_id, timestamp)
                .unwrap_or_else(|| budget::SettlementEffect {
                    actual: budget::Money::zero(reserved_entry.amount.currency.clone()),
                    released: budget::Money::zero(reserved_entry.amount.currency.clone()),
                    overage: budget::Money::zero(reserved_entry.amount.currency.clone()),
                    reserved: reserved_entry.amount.clone(),
                    variance: None,
                    exceeded_hard_limit: false,
                })
        }
    };
    let settlement = budget::Settlement {
        settlement_id: settlement_id.clone(),
        reservation_id: reservation_id.clone(),
        project_id: target.project_id.clone(),
        call_id: target.intent.call_id.clone(),
        dispatch_intent_id: Some(target.intent.id.clone()),
        attempt_id: target.intent.attempt_id.clone(),
        generation: target.intent.generation,
        disposition,
        pricing: price_fact,
        usage: usage.clone(),
        billable: billable.clone(),
        payload_digest: String::new(),
        effect: effect.clone(),
        reason_code: reason_code.clone(),
        usage_authoritative,
        created_at: timestamp,
    };
    let payload_digest = budget::settlement_payload_digest(&settlement);
    // The record carries its own digest, so a reader can tell what content was
    // accepted without re-deriving the normalization that produced it.
    let settlement = budget::Settlement {
        payload_digest: payload_digest.clone(),
        ..settlement
    };
    transaction
        .execute(
            "INSERT INTO domain_settlements(settlement_id,reservation_id,project_id,call_id,dispatch_intent_id,attempt_id,generation,disposition,reason_code,usage_authoritative,payload_digest,settlement,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                settlement_id,
                reservation_id,
                target.project_id,
                target.intent.call_id,
                target.intent.id,
                target.intent.attempt_id,
                i64::try_from(target.intent.generation)
                    .map_err(|_| invalid("Attempt generation exceeds SQLite range"))?,
                disposition.as_str(),
                reason_code,
                usage_authoritative,
                payload_digest,
                serde_json::to_string(&settlement)
                    .map_err(|error| invalid(&format!("serialize settlement: {error}")))?,
                timestamp,
            ],
        )
        .map_err(sql)?;
    ledger.record_settlement(settlement.clone());
    // An overage is the most urgent thing an operator can be told, so it wins the
    // durable reason slot over the disposition that produced it.
    ledger.set_reason(
        if effect.overage.micros > 0 {
            budget::REASON_OVERAGE
        } else {
            reason_code.as_str()
        },
        timestamp,
    );
    store_budget(transaction, &target.project_id, &mut ledger)?;
    // The money moved, so the accounting state it left behind is journaled in the
    // same commit. Without this the journal's copy of the cap, the rollups and
    // the status would lag the authoritative row, and a projection rebuilt from
    // the journal would disagree with one rebuilt from a snapshot.
    emit_budget_limit(transaction, &target.project_id, &ledger)?;
    let root = emit_settlement(transaction, &settlement, &target.intent, caused_by)?;
    if let Some(reservation) = ledger
        .reservations
        .iter()
        .find(|reservation| reservation.reservation_id == reservation_id)
    {
        emit_reservation(
            transaction,
            &journal::ReservationFact {
                project_id: target.project_id.clone(),
                call_id: target.intent.call_id.clone(),
                reservation: reservation.clone(),
            },
            &target.intent,
            Some(root),
        )?;
    }
    if let Some(record) = usage {
        if !usage_authoritative {
            // The spend happened; the reporting Attempt simply may not state it.
            emit_usage_evidence(
                transaction,
                &journal::UsageEvidence {
                    project_id: target.project_id.clone(),
                    call_id: target.intent.call_id.clone(),
                    attempt_id: target.intent.attempt_id.clone(),
                    generation: target.intent.generation,
                    dispatch_intent_id: Some(target.intent.id.clone()),
                    usage: record,
                    reason_code: budget::REASON_STALE_AUTHORITY.to_string(),
                    created_at: timestamp,
                },
                &target.intent,
                Some(root),
            )?;
        }
    }
    Ok(AccountingOutcome {
        recorded: true,
        disposition,
        reason_code,
        settlement_id,
        effect: Some(effect),
        usage_authoritative,
    })
}

/// Retain one settlement payload that conflicts with the settlement already
/// recorded under the same identity.
///
/// The two payloads disagree about what actually happened — different usage, a
/// different actual — so neither may replace the other and neither may be
/// dropped as an ordinary duplicate. The canonical settlement is left exactly as
/// it is: no Money moves, the reservation keeps the state it was discharged to,
/// and the conflicting payload is recorded under its own id, which is derived
/// from the business id *and* the payload digest. That makes the retention
/// itself idempotent: the same conflicting payload delivered again is a
/// duplicate of the record that already retained it.
///
/// The retained fact asserts nothing. Its disposition is `Unresolved` and its
/// effect is zero, so it can never be read as a second charge, a release, or an
/// overage, while its usage stays visible as evidence of what was claimed.
#[allow(clippy::too_many_lines)]
fn record_settlement_conflict(
    transaction: &rusqlite::Transaction<'_>,
    target: &AccountingTarget,
    candidate: &budget::Settlement,
    candidate_digest: &str,
    caused_by: Option<u64>,
) -> Result<AccountingOutcome> {
    let conflict_id = budget::conflict_settlement_id(&candidate.settlement_id, candidate_digest);
    // The id is derived from the payload digest, so a row existing under it can
    // only be the record of this same conflicting payload.
    let retained: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_settlements WHERE settlement_id=?1)",
            [&conflict_id],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if retained {
        return Ok(AccountingOutcome {
            recorded: false,
            disposition: budget::SettlementDisposition::Unresolved,
            reason_code: budget::REASON_SETTLEMENT_CONFLICT.to_string(),
            settlement_id: candidate.settlement_id.clone(),
            effect: None,
            usage_authoritative: false,
        });
    }
    let timestamp = candidate.created_at;
    let retained_settlement = budget::Settlement {
        settlement_id: conflict_id.clone(),
        reservation_id: candidate.reservation_id.clone(),
        project_id: candidate.project_id.clone(),
        call_id: candidate.call_id.clone(),
        dispatch_intent_id: candidate.dispatch_intent_id.clone(),
        attempt_id: candidate.attempt_id.clone(),
        generation: candidate.generation,
        disposition: budget::SettlementDisposition::Unresolved,
        pricing: candidate.pricing.clone(),
        usage: candidate.usage.clone(),
        billable: candidate.billable.clone(),
        payload_digest: candidate_digest.to_string(),
        effect: budget::SettlementEffect::default(),
        reason_code: budget::REASON_SETTLEMENT_CONFLICT.to_string(),
        usage_authoritative: false,
        created_at: timestamp,
    };
    transaction
        .execute(
            "INSERT INTO domain_settlements(settlement_id,reservation_id,project_id,call_id,dispatch_intent_id,attempt_id,generation,disposition,reason_code,usage_authoritative,payload_digest,settlement,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                conflict_id,
                retained_settlement.reservation_id,
                retained_settlement.project_id,
                retained_settlement.call_id,
                retained_settlement.dispatch_intent_id,
                retained_settlement.attempt_id,
                i64::try_from(retained_settlement.generation)
                    .map_err(|_| invalid("Attempt generation exceeds SQLite range"))?,
                retained_settlement.disposition.as_str(),
                retained_settlement.reason_code,
                false,
                candidate_digest,
                serde_json::to_string(&retained_settlement)
                    .map_err(|error| invalid(&format!("serialize settlement: {error}")))?,
                timestamp,
            ],
        )
        .map_err(sql)?;
    let mut ledger = read_budget(transaction, &target.project_id)?;
    ledger.record_settlement(retained_settlement.clone());
    ledger.set_reason(budget::REASON_SETTLEMENT_CONFLICT, timestamp);
    store_budget(transaction, &target.project_id, &mut ledger)?;
    let root = emit_settlement(transaction, &retained_settlement, &target.intent, caused_by)?;
    // The conflicting usage was observed and deliberately not booked, so it is
    // retained as evidence too — exactly as an unbookable usage from a stale
    // Attempt is — rather than being visible only inside the settlement record.
    if let Some(record) = &candidate.usage {
        emit_usage_evidence(
            transaction,
            &journal::UsageEvidence {
                project_id: target.project_id.clone(),
                call_id: target.intent.call_id.clone(),
                attempt_id: target.intent.attempt_id.clone(),
                generation: target.intent.generation,
                dispatch_intent_id: Some(target.intent.id.clone()),
                usage: record.clone(),
                reason_code: budget::REASON_SETTLEMENT_CONFLICT.to_string(),
                created_at: timestamp,
            },
            &target.intent,
            Some(root),
        )?;
    }
    Ok(AccountingOutcome {
        recorded: false,
        disposition: budget::SettlementDisposition::Unresolved,
        reason_code: budget::REASON_SETTLEMENT_CONFLICT.to_string(),
        settlement_id: candidate.settlement_id.clone(),
        effect: None,
        usage_authoritative: false,
    })
}

/// Retain a fenced dispatch's reservation as unresolved.
///
/// Every fencing path converges here — a stale Attempt at claim time, a late
/// result, a restart with an unknown external effect, an Attempt going terminal
/// — so no caller can forget it, and none of them can optimistically release
/// money that a provider may already have been paid for.
fn apply_fenced_accounting_in(
    transaction: &rusqlite::Transaction<'_>,
    call_id: &str,
    caused_by: Option<u64>,
) -> Result<AccountingOutcome> {
    let Some(target) = read_accounting_target(transaction, call_id)? else {
        return Ok(AccountingOutcome {
            recorded: false,
            disposition: budget::SettlementDisposition::Unresolved,
            reason_code: budget::REASON_NO_RESERVATION.to_string(),
            settlement_id: String::new(),
            effect: None,
            usage_authoritative: false,
        });
    };
    let claim = AccountingAuthority {
        attempt_id: target.intent.attempt_id.clone(),
        generation: target.intent.generation,
    };
    apply_accounting_in(
        transaction,
        &target,
        &claim,
        &DispatchAccounting::Fenced,
        caused_by,
    )
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
            "SELECT {CALL_COLUMNS} FROM domain_calls c LEFT JOIN domain_dispatch_intents i ON i.call_id=c.id ORDER BY c.created_at,c.id"
        ),
        &[],
        call_from_row,
    )
}

fn attempt_calls(connection: &Connection, attempt_id: &str) -> Result<Vec<Call>> {
    query_all(
        connection,
        &format!(
            "SELECT {CALL_COLUMNS} FROM domain_calls c LEFT JOIN domain_dispatch_intents i ON i.call_id=c.id WHERE c.attempt_id=?1 ORDER BY c.created_at,c.id"
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

/// Every live reservation, with the Project that owns the money and the Call it
/// was taken for. A `Released` or `Settled` entry is retained in the ledger but
/// is no longer live, so it is not part of the "still holds money" view.
fn all_reservations(connection: &Connection) -> Result<Vec<journal::ReservationFact>> {
    let mut facts = Vec::new();
    for row in all_budgets(connection)? {
        for reservation in row.ledger.reservations {
            if reservation.state != budget::ReservationState::Reserved {
                continue;
            }
            facts.push(journal::ReservationFact {
                project_id: row.project_id.clone(),
                call_id: reservation.operation_id.clone(),
                reservation,
            });
        }
    }
    Ok(facts)
}

/// Every accounting fact ever recorded for a Project, in the order it was
/// applied. This is the durable per-operation record; the bounded in-ledger list
/// may prune the oldest, this table never does.
fn all_settlements(connection: &Connection) -> Result<Vec<budget::Settlement>> {
    query_all(
        connection,
        "SELECT settlement FROM domain_settlements ORDER BY created_at,settlement_id",
        &[],
        |row| {
            let raw: String = row.get(0)?;
            serde_json::from_str(&raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    raw.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })
        },
    )
}

/// Usage that was observed but deliberately not booked. It is reconstructed from
/// the settlements that carry it, so it can never disagree with the accounting
/// record it came from.
fn all_usage_evidence(connection: &Connection) -> Result<Vec<journal::UsageEvidence>> {
    Ok(all_settlements(connection)?
        .into_iter()
        .filter(|settlement| !settlement.usage_authoritative)
        .filter_map(|settlement| {
            settlement.usage.map(|usage| journal::UsageEvidence {
                project_id: settlement.project_id,
                call_id: settlement.call_id,
                attempt_id: settlement.attempt_id,
                generation: settlement.generation,
                dispatch_intent_id: settlement.dispatch_intent_id,
                usage,
                reason_code: settlement.reason_code,
                created_at: settlement.created_at,
            })
        })
        .collect())
}

fn all_budgets(connection: &Connection) -> Result<Vec<BudgetLedgerRow>> {
    query_all(
        connection,
        "SELECT project_id,budget FROM domain_budgets ORDER BY project_id",
        &[],
        |row| {
            let project_id: String = row.get(0)?;
            let raw: String = row.get(1)?;
            let ledger = serde_json::from_str::<budget::ProjectBudget>(&raw).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    raw.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            Ok(BudgetLedgerRow { project_id, ledger })
        },
    )
}

struct BudgetLedgerRow {
    project_id: String,
    ledger: budget::ProjectBudget,
}

fn all_budget_limits(connection: &Connection) -> Result<Vec<journal::BudgetLimitFact>> {
    Ok(all_budgets(connection)?
        .into_iter()
        .map(|row| journal::BudgetLimitFact {
            project_id: row.project_id,
            hard_limit: row.ledger.hard_limit.clone(),
            origin: row.ledger.origin,
            status: row.ledger.status,
            currency: row.ledger.currency.clone(),
            settled_micros: row.ledger.settled.micros,
            reserved_micros: row.ledger.reserved.micros,
            unresolved_micros: row.ledger.unresolved.micros,
            reason: row.ledger.reason.clone(),
            updated_at: row.ledger.updated_at,
        })
        .collect())
}

/// Complete a Call inside an existing writer transaction.
///
/// Returns the cursor of the `CallUpdated` event, which is the root fact of the
/// transition. A caller that settles the dispatch's money in the same commit
/// records that settlement as caused by it, so a reader can see that the
/// completion and the spend are one decision rather than two.
#[allow(clippy::too_many_lines)]
fn finish_call_in(
    transaction: &rusqlite::Transaction<'_>,
    call_id: &str,
    attempt_id: &str,
    generation: u64,
    response: &str,
) -> Result<Option<u64>> {
    validate_id(call_id)?;
    validate_id(attempt_id)?;
    let value: serde_json::Value = serde_json::from_str(response)
        .map_err(|error| OcgError::config(format!("invalid Call output JSON: {error}")))?;
    crate::orchestration::call_schema::validate_output(&serde_json::json!({
        "result": value
    }))?;
    let call = read_call(transaction, call_id)?.ok_or_else(|| invalid("unknown Call"))?;
    if serde_json::from_str::<serde_json::Value>(&call.request).is_ok_and(|request| {
        request.get("kind").and_then(serde_json::Value::as_str) == Some("native_tool")
    }) {
        crate::native_tools::ToolResult::validate_response(response, true)?;
    }
    let generation_i64 =
        i64::try_from(generation).map_err(|_| invalid("Call generation exceeds SQLite range"))?;
    let duplicate: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_calls WHERE id=?1 AND attempt_id=?2 AND generation=?3 AND state='completed' AND response=?4)",
            params![call_id, attempt_id, generation_i64, response], |row| row.get(0)
        ).map_err(sql)?;
    if duplicate {
        // A duplicate completion changed nothing, so it records nothing. The
        // caller still settles accounting, which is idempotent on its own.
        return Ok(None);
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
    Ok(Some(root))
}

fn encode_chat_record(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| OcgError::config(format!("serialize canonical chat record: {error}")))
}

fn decode_chat_record<T: serde::de::DeserializeOwned>(record: &str) -> Result<T> {
    serde_json::from_str(record)
        .map_err(|error| OcgError::config(format!("decode canonical chat record: {error}")))
}

fn chat_text_block(content: &str) -> MessageBlock {
    MessageBlock {
        kind: MessageBlockKind::Markdown,
        content: Some(content.to_string()),
        entity_ref: None,
        artifact_ref: None,
        changeset_ref: None,
        projection_kind: None,
        raw: None,
    }
}

fn chat_image_block(image: &crate::contracts::ChatImage) -> MessageBlock {
    MessageBlock {
        kind: MessageBlockKind::Image,
        content: None,
        raw: Some(serde_json::json!(image)),
        ..chat_text_block("")
    }
}

fn read_conversation_messages(
    connection: &Connection,
    conversation_id: &str,
) -> Result<Vec<Message>> {
    let mut statement = connection.prepare(
        "SELECT m.record FROM domain_chat_turns t JOIN domain_messages m ON m.attempt_id=t.attempt_id WHERE t.conversation_id=?1 ORDER BY t.turn_order,CASE m.role WHEN 'user' THEN 0 ELSE 1 END",
    ).map_err(sql)?;
    let records = statement
        .query_map([conversation_id], |row| row.get::<_, String>(0))
        .map_err(sql)?;
    records
        .map(|record| decode_chat_record(&record.map_err(sql)?))
        .collect()
}

fn bump_conversation(transaction: &rusqlite::Transaction<'_>, conversation_id: &str) -> Result<()> {
    let record: String = transaction
        .query_row(
            "SELECT record FROM domain_conversations WHERE id=?1",
            [conversation_id],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let mut conversation: Conversation = decode_chat_record(&record)?;
    conversation.revision = conversation
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid("Conversation revision overflow"))?;
    conversation.updated_at = now().to_string();
    transaction
        .execute(
            "UPDATE domain_conversations SET record=?2 WHERE id=?1",
            params![conversation_id, encode_chat_record(&conversation)?],
        )
        .map_err(sql)?;
    Ok(())
}

fn update_chat_message(
    transaction: &rusqlite::Transaction<'_>,
    attempt_id: &str,
    role: &str,
    state: MessageLifecycle,
    content: Option<&str>,
) -> Result<()> {
    let record: Option<String> = transaction
        .query_row(
            "SELECT record FROM domain_messages WHERE attempt_id=?1 AND role=?2",
            params![attempt_id, role],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql)?;
    let Some(record) = record else {
        return Ok(());
    };
    let mut message: Message = decode_chat_record(&record)?;
    if message.state == state {
        return Ok(());
    }
    // An accepted user remains a fact even when its Attempt fails. Assistant
    // placeholders never retain streaming text or hidden reasoning.
    if role == "user"
        && state == MessageLifecycle::Failed
        && matches!(
            message.state,
            MessageLifecycle::Complete | MessageLifecycle::Deleted
        )
    {
        return Ok(());
    }
    message.state = message.state.transition(state)?;
    if let Some(content) = content {
        message.blocks = vec![chat_text_block(content)];
    }
    message.revision = message
        .revision
        .checked_add(1)
        .ok_or_else(|| invalid("Message revision overflow"))?;
    message.updated_at = now().to_string();
    if state == MessageLifecycle::Deleted {
        message.deleted_at = Some(message.updated_at.clone());
    }
    transaction
        .execute(
            "UPDATE domain_messages SET record=?2 WHERE id=?1",
            params![message.id.as_str(), encode_chat_record(&message)?],
        )
        .map_err(sql)?;
    bump_conversation(transaction, message.conversation_ref.id.as_str())
}

fn settle_chat_turn(
    transaction: &rusqlite::Transaction<'_>,
    attempt_id: &str,
    state: &str,
) -> Result<()> {
    let is_chat: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM domain_chat_turns WHERE attempt_id=?1)",
            [attempt_id],
            |row| row.get(0),
        )
        .map_err(sql)?;
    if !is_chat {
        return Ok(());
    }
    if state == "completed" {
        let response: String = transaction.query_row(
            "SELECT response FROM domain_calls WHERE attempt_id=?1 AND state='completed' AND json_extract(request,'$.executor_transport')='provider' ORDER BY created_at DESC,id DESC LIMIT 1",
            [attempt_id], |row| row.get(0),
        ).map_err(sql)?;
        let response: serde_json::Value = decode_chat_record(&response)?;
        let content = response
            .get("content")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid("completed chat Call has no final content"))?;
        update_chat_message(
            transaction,
            attempt_id,
            "assistant",
            MessageLifecycle::Complete,
            Some(content),
        )?;
        if let Some(images) = response.get("images").and_then(serde_json::Value::as_array) {
            let (root, project_id, record): (String, String, String) = transaction.query_row(
                "SELECT p.root,t.project_id,m.record FROM domain_chat_turns t JOIN domain_projects p ON p.id=t.project_id JOIN domain_messages m ON m.attempt_id=t.attempt_id AND m.role='assistant' WHERE t.attempt_id=?1",
                [attempt_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            ).map_err(sql)?;
            let mut message: Message = decode_chat_record(&record)?;
            for url in images.iter().take(crate::chat_images::MAX_IMAGES) {
                let url = url.as_str().ok_or_else(|| invalid("invalid final image"))?;
                let image =
                    crate::chat_images::received(std::path::Path::new(&root), &project_id, url)?;
                message.blocks.push(chat_image_block(&image));
            }
            transaction
                .execute(
                    "UPDATE domain_messages SET record=?2 WHERE id=?1",
                    params![message.id.as_str(), encode_chat_record(&message)?],
                )
                .map_err(sql)?;
        }
    } else {
        update_chat_message(
            transaction,
            attempt_id,
            "user",
            MessageLifecycle::Failed,
            None,
        )?;
        update_chat_message(
            transaction,
            attempt_id,
            "assistant",
            MessageLifecycle::Failed,
            None,
        )?;
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
        // Cancellation is two-phase: this step revokes the Attempt's authority
        // to act, and a later `confirm_cancel` / `mark_orphaned` settles the
        // outcome. The Job keeps naming the Attempt that is being cancelled, so
        // that settlement still matches this Job and can carry it to its terminal
        // state. Revoking action authority does not need that link cleared —
        // `authority()` and `claim_call` already exclude an Attempt that is no
        // longer `queued`/`running` and no longer `authoritative` — and clearing
        // it here would strand the Job in `cancelling` forever.
        let job_moved = transaction.execute("UPDATE domain_jobs SET state='cancelling',updated_at=?2 WHERE id=?1 AND authoritative_attempt_id=?3", params![job_id,timestamp,attempt_id]).map_err(sql)?;
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
            let calls = attempt_calls(transaction, attempt_id)?;
            let provider_completed = calls.iter().any(|call| {
                call.state == "completed"
                    && serde_json::from_str::<serde_json::Value>(&call.request).is_ok_and(
                        |request| {
                            request
                                .get("executor_transport")
                                .and_then(serde_json::Value::as_str)
                                == Some("provider")
                        },
                    )
            });
            for call in calls {
                if call.state == "completed" {
                    continue;
                }
                // A known native tool failure is an observation the provider
                // can correct. Framework failures and unknown effects cannot
                // qualify a successful Attempt.
                let request: serde_json::Value = serde_json::from_str(&call.request)
                    .map_err(|_| invalid("invalid durable Call request"))?;
                let observed_failure = provider_completed
                    && call.state == "failed"
                    && request.get("kind").and_then(serde_json::Value::as_str)
                        == Some("native_tool")
                    && call.response.as_deref().is_some_and(|response| {
                        crate::native_tools::ToolResult::validate_response(response, false).is_ok()
                    })
                    && read_dispatch_intent_by_call(transaction, &call.id)?.is_some_and(|intent| {
                        intent.state == "failed"
                            && intent.effect_state == EffectIntentState::Settled
                    });
                if !observed_failure {
                    return Err(invalid("Attempt still has unsettled or unsuccessful Calls"));
                }
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
        // An Attempt going terminal fences every dispatch it still owned. Each
        // one's reservation is retained as unresolved in the same commit, so
        // abandoning an Attempt can neither leak a held reservation nor hand
        // money back that a provider may already have been paid for.
        for intent in &fenced_intents {
            apply_fenced_accounting_in(transaction, &intent.call_id, Some(root))?;
        }
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
    settle_chat_turn(transaction, attempt_id, state)?;
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
