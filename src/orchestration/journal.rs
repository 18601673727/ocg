//! The canonical durable execution event journal.
//!
//! This module is the **evidence** half of the single canonical execution
//! authority that lives in `orchestration::domain`. Its purpose is narrow and
//! deliberate: every canonical lifecycle change of `Job`, `Attempt`,
//! `Executor`, `Call` and `DispatchIntent` must leave exactly one durable
//! record of the fact that already became canonical.
//!
//! # Authority model
//!
//! - The canonical tables in `substrate.sqlite3` remain the **only** execution
//!   authority. A lifecycle decision is never read from, derived from, or
//!   arbitrated by this journal. The journal is append-only, derived and
//!   read-only to every execution path; deleting it would reduce observability
//!   but would not change a single scheduling, fencing, budget or completion
//!   decision. It is therefore *not* a second execution authority.
//! - An event only ever describes a change that **already became canonical
//!   fact**. Events are appended after the corresponding `UPDATE`/`INSERT`
//!   succeeds and before the transaction commits, so an event can never be
//!   written for a change that was rolled back, and a change can never commit
//!   without its event. A no-op mutation records nothing.
//! - Writing is fail-closed: an event append failure propagates as `Err` and
//!   rolls the enclosing transaction back, so canonical state and its event can
//!   never diverge into a partially visible pair.
//!
//! This journal is **not** a replacement for `orchestration::replay`. That
//! module is a separate hash-chained file journal for the non-execution domains
//! (Approvals and Resources) and is untouched here. The two cover disjoint
//! entity sets, so there is exactly one journal per truth class and exactly one
//! authority per entity: no entity is described by two journals, and no
//! execution fact lives in either legacy journal.
//!
//! # Authority identity, generation, causation and provenance
//!
//! Every event preserves:
//!
//! - **authority identity** — `(authority_attempt_id, authority_generation)`,
//!   the Attempt that held authority over the affected Job at the moment the
//!   fact became canonical. Terminal/fencing events carry the identity of the
//!   authority they revoke, so a post-hoc reader can always tell *which*
//!   authority made (or lost) the fact.
//! - **generation** — the subject's own generation, so a replacement Attempt's
//!   facts are never confused with its predecessor's.
//! - **causation** — `caused_by_seq` links a cascaded record (the Calls and
//!   DispatchIntents fenced by an Attempt terminal transition) to the single
//!   causal event in the same transaction, and `causation_key` carries the
//!   stable command identity (session binding key, Call id) when one exists.
//! - **provenance** — `actor` names who caused the fact, and the payload is the
//!   committed post-state, so an event is self-describing evidence.
//!
//! No timestamp in this journal carries authority semantics: `recorded_at` is
//! local telemetry only. Ordering authority is the cursor, never wall time.
//!
//! # Cursor, ordering and the retention boundary
//!
//! One local OCG node owns one canonical database, so the journal is a single
//! **per-authority stream** with a gap-free, strictly increasing `seq`. The
//! cursor is allocated from a durable counter (`domain_journal_meta.head_seq`),
//! never from `MAX(seq)`, so:
//!
//! - there is no fabricated global order and no cross-node sequence;
//! - a reader that has applied `seq = N` asks for `seq > N` and continues;
//! - because the counter is durable, a restarted process resumes from exactly
//!   the position it had reached, with no prompt reconstruction;
//! - deleting old events can never make a cursor reappear or restart, because
//!   the counter is independent of which rows survive;
//! - a gap can only mean corruption or truncation, never a lost commit.
//!
//! Retention is explicit. [`prune`] moves a floor and records an anchor; it
//! never runs on its own, never on a timer, and never touches canonical state.
//! After a prune, a consumer whose cursor is below `floor - 1` is told
//! [`EventDelta::ResyncRequired`] — it is never handed a truncated suffix, and
//! a pruned prefix is never reported as a [`ApplyOutcome::Gap`].
//!
//! # Snapshot and replay
//!
//! The canonical tables are themselves the snapshot. A consumer reads
//! [`ExecutionSnapshot`] (canonical rows plus the head cursor and the boundary
//! anchor) in one read transaction and then applies [`ExecutionEvent`]s after
//! that cursor. Applying obeys the contiguous rule: `seq == last + 1` applies,
//! `seq <= last` is an idempotent duplicate and is ignored, and a forward gap
//! stops application and reports [`ReplayStatus::Gap`] so the consumer resyncs
//! from a snapshot rather than silently inventing an order.
//!
//! # Deliberate non-goals
//!
//! This round adds accounting fact events for Money: a reservation that was
//! recorded, the actual a provider run was settled for, the release of a
//! reservation that never reached the provider, the retention of one whose
//! actual is still unknown, and a hard limit an operator changed. Those are
//! facts about the same single-writer commit that moved the money, so they are
//! written in the same transaction and can never describe a change that was
//! rolled back. They remain evidence: nothing reads them back to decide an
//! admission, a settlement or a cap, and the canonical `domain_budgets` row is
//! still the only budget authority.
//!
//! This round does **not** add replication, clustering or a second writer, and
//! does not restore any legacy Job/Attempt execution semantics. It is a
//! single-node, single-authority journal.
//!
//! The stream also has no automatic retention: nothing is pruned on a timer or
//! on wall-clock expiry, because a correctness boundary must never depend on
//! the clock. Pruning is an explicit [`prune`] call that moves the floor and
//! records an anchor, after which older consumers are told
//! [`EventDelta::ResyncRequired`] instead of being served a partial delta.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{
    BudgetOrigin, BudgetStatus, Money, Reservation, Settlement, UsageRecord,
};
use crate::orchestration::domain::{Attempt, Call, DispatchIntent, Executor, Job, Project};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use strum::{Display, EnumString};

fn invalid(message: &str) -> OcgError {
    OcgError::config(message)
}

fn sql(error: rusqlite::Error) -> OcgError {
    OcgError::config(format!("execution journal SQLite: {error}"))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid("execution journal value exceeds SQLite range"))
}

fn to_u64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| invalid("execution journal stored a negative counter"))
}

/// The cursor before any event exists.
pub const INITIAL_CURSOR: u64 = 0;
/// Upper bound on one bounded read, so a caller cannot materialize the whole
/// journal in a single request.
pub const MAX_EVENT_READ: usize = 4096;
/// The actor recorded when the local canonical authority itself caused a fact.
pub const AUTHORITY_ACTOR: &str = "authority";
/// Schema version of the durable journal boundary row.
pub const JOURNAL_SCHEMA_VERSION: u32 = 1;

/// The durable journal table.
///
/// `seq` is the stable cursor. It is **not** derived from the rows that happen
/// to be present: it is allocated from the durable boundary counter below, so
/// deleting old events can never make a cursor reappear or restart.
pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS domain_events (
    seq INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    project_id TEXT,
    job_id TEXT,
    attempt_id TEXT,
    executor_id TEXT,
    call_id TEXT,
    dispatch_intent_id TEXT,
    generation INTEGER,
    authority_attempt_id TEXT,
    authority_generation INTEGER,
    caused_by_seq INTEGER,
    causation_key TEXT,
    actor TEXT NOT NULL,
    payload TEXT NOT NULL,
    recorded_at INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS domain_events_by_job ON domain_events(job_id, seq);
CREATE INDEX IF NOT EXISTS domain_events_by_entity
    ON domain_events(entity_type, entity_id, seq);
CREATE INDEX IF NOT EXISTS domain_events_by_kind ON domain_events(kind, seq);
CREATE TABLE IF NOT EXISTS domain_journal_meta (
    id INTEGER PRIMARY KEY CHECK(id=1),
    schema_version INTEGER NOT NULL,
    head_seq INTEGER NOT NULL CHECK(head_seq >= 0),
    floor_seq INTEGER NOT NULL CHECK(floor_seq >= 0),
    anchor_event_id TEXT,
    anchor_digest TEXT NOT NULL
) STRICT;
"#;

const SELECT_COLUMNS: &str = "SELECT seq,event_id,kind,entity_type,entity_id,project_id,job_id,\
attempt_id,executor_id,call_id,dispatch_intent_id,generation,authority_attempt_id,\
authority_generation,caused_by_seq,causation_key,actor,payload,recorded_at FROM domain_events";

/// The explicit retention boundary of the canonical execution journal.
///
/// - `head_cursor` is the cursor of the newest committed event, and is also the
///   durable allocator: the next event takes `head + 1` even when every earlier
///   row has been pruned.
/// - `floor_cursor` is the lowest cursor still retained. `0` means **nothing has
///   ever been pruned**, so the whole stream from `1` is available. After a prune
///   to `F`, every `seq < F` is gone and `F` itself is retained.
/// - `anchor_event_id` is the identity of the last pruned event, and
///   `anchor_digest` binds `(head, floor, anchor)` so a truncated or rewritten
///   boundary is detectable rather than silently accepted.
///
/// A consumer that has applied up to `C` can be served a **complete** delta
/// exactly when `C <= head_cursor` and `C + 1 >= floor_cursor`. Otherwise the
/// authority says [`EventDelta::ResyncRequired`] instead of returning a partial
/// suffix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalBoundary {
    pub head_cursor: u64,
    pub floor_cursor: u64,
    pub anchor_event_id: Option<String>,
    pub anchor_digest: String,
}

impl JournalBoundary {
    /// Whether a consumer sitting at `cursor` can still be served every event
    /// after it, without a resync.
    pub fn serves(&self, cursor: u64) -> bool {
        cursor <= self.head_cursor && cursor + 1 >= self.floor_cursor
    }
}

/// The result of asking for the events after a cursor.
///
/// A complete delta is all-or-nothing. The journal never returns a truncated
/// suffix and never dresses one up as a gap.
#[derive(Debug, Clone, PartialEq)]
pub enum EventDelta {
    /// Every event after the cursor, in cursor order.
    Available {
        events: Vec<ExecutionEvent>,
        head_cursor: u64,
        floor_cursor: u64,
    },
    /// The cursor is already at the head: there is nothing to apply.
    Empty { head_cursor: u64 },
    /// The cursor is older than the retained floor, so the delta it asks for can
    /// no longer be produced. Refetch `execution_snapshot()` and continue from
    /// the cursor that snapshot reports.
    ResyncRequired {
        requested: u64,
        floor_cursor: u64,
        head_cursor: u64,
    },
    /// The cursor is ahead of the durable head, so it cannot name a real
    /// position. It is rejected instead of being treated as "up to date".
    AheadOfHead { requested: u64, head_cursor: u64 },
}

impl EventDelta {
    /// Whether the caller must resynchronize from a fresh snapshot.
    pub fn is_resync_required(&self) -> bool {
        matches!(self, Self::ResyncRequired { .. })
    }

    /// The events of a usable delta, empty for the terminal outcomes.
    pub fn events(&self) -> &[ExecutionEvent] {
        match self {
            Self::Available { events, .. } => events,
            _ => &[],
        }
    }
}

/// What one explicit prune did. Pruning is never silent and never touches
/// canonical state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalPrune {
    /// The new floor. Every `seq < floor_cursor` was removed.
    pub floor_cursor: u64,
    /// Unchanged by pruning: a prune must never move the head.
    pub head_cursor: u64,
    /// How many events were removed.
    pub removed: u64,
    /// The anchor now naming the last pruned event.
    pub anchor_event_id: Option<String>,
    pub anchor_digest: String,
}

/// Install the journal and its retention boundary, then reconcile the boundary
/// with whatever is already durable. Safe to call on every open.
pub(crate) fn ensure_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(SCHEMA).map_err(sql)?;
    let transaction = begin(connection)?;
    let stored: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(seq),0) FROM domain_events",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let stored = to_u64(stored)?;
    let existing: Option<(u64, u64, Option<String>)> = transaction
        .query_row(
            "SELECT head_seq,floor_seq,anchor_event_id FROM domain_journal_meta WHERE id=1",
            [],
            |row| {
                Ok((
                    to_u64(row.get::<_, i64>(0)?).unwrap_or(0),
                    to_u64(row.get::<_, i64>(1)?).unwrap_or(0),
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql)?;
    let lowest: i64 = transaction
        .query_row(
            "SELECT COALESCE(MIN(seq),0) FROM domain_events",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let lowest = to_u64(lowest)?;
    match existing {
        // No boundary yet: adopt the stream that already exists. This is the
        // one-time migration from a journal that derived its cursor from
        // `MAX(seq) + 1`; nothing has been pruned yet, so the floor is 0.
        None => {
            let digest = boundary_digest(stored, INITIAL_CURSOR, None)?;
            transaction
                .execute(
                    "INSERT INTO domain_journal_meta(id,schema_version,head_seq,floor_seq,anchor_event_id,anchor_digest) VALUES(1,?1,?2,?3,NULL,?4)",
                    params![
                        to_i64(JOURNAL_SCHEMA_VERSION as u64)?,
                        to_i64(stored)?,
                        to_i64(INITIAL_CURSOR)?,
                        digest
                    ],
                )
                .map_err(sql)?;
        }
        Some((head, floor, anchor)) => {
            // The counter only ever moves forward, so an out-of-band deletion of
            // rows can lower the observed maximum but never the head. Keeping
            // the higher value is what stops a cursor from being reissued.
            let head = head.max(stored);
            // If history below the lowest surviving event is gone without a
            // recorded prune, that prefix was lost outside this module. Admit
            // it as a floor — including the fully-emptied case, where the
            // boundary becomes `head + 1` — so consumers are told to resync
            // instead of being handed a hole or a silent empty delta.
            let (floor, anchor) = match (lowest, floor) {
                // Every row is gone but the counter remembers them.
                (0, current) if head > 0 => {
                    let floor = head.saturating_add(1).max(current);
                    (floor, anchor_from(&transaction, floor.saturating_sub(1))?)
                }
                // A prefix vanished without a recorded prune.
                (lowest, INITIAL_CURSOR) if lowest > 1 => {
                    (lowest, anchor_from(&transaction, lowest.saturating_sub(1))?)
                }
                // The boundary already records the prune. Its anchor names an
                // event that was itself removed, so it must be preserved rather
                // than re-derived from a row that no longer exists.
                (_, current) => (current, anchor),
            };
            let digest = boundary_digest(head, floor, anchor.as_deref())?;
            transaction
                .execute(
                    "UPDATE domain_journal_meta SET head_seq=?1,floor_seq=?2,anchor_event_id=?3,anchor_digest=?4 WHERE id=1",
                    params![to_i64(head)?, to_i64(floor)?, anchor, digest],
                )
                .map_err(sql)?;
        }
    }
    transaction.commit().map_err(sql)
}

/// The identity of the last event below `seq`, used as the boundary anchor.
fn anchor_from(connection: &Connection, seq: u64) -> Result<Option<String>> {
    if seq == 0 {
        return Ok(None);
    }
    connection
        .query_row(
            "SELECT event_id FROM domain_events WHERE seq=?1",
            [to_i64(seq)?],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql)
}

/// A floor that claims a retained range it does not have is corruption, not a
/// boundary: fail closed instead of ever serving a partial delta.
fn validate_floor(connection: &Connection, floor: u64) -> Result<()> {
    if floor == INITIAL_CURSOR {
        return Ok(());
    }
    let head: i64 = connection
        .query_row(
            "SELECT head_seq FROM domain_journal_meta WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let head = to_u64(head)?;
    let retained: i64 = connection
        .query_row(
            "SELECT COALESCE(MIN(seq),0) FROM domain_events WHERE seq>=?1",
            [to_i64(floor)?],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let retained = to_u64(retained)?;
    if retained == floor {
        return Ok(());
    }
    // A floor past the head is the fully-emptied journal: nothing is retained and
    // every consumer must resync. That is a boundary, not a hole.
    if retained == 0 && floor > head {
        return Ok(());
    }
    Err(invalid(
        "execution journal floor does not match the retained range; refusing to serve a partial \
         delta",
    ))
}

/// Bind the boundary so a truncated or rewritten floor/head/anchor is visible.
fn boundary_digest(head: u64, floor: u64, anchor_event_id: Option<&str>) -> Result<String> {
    let mut bytes = Vec::with_capacity(96);
    bytes.extend_from_slice(b"ocg-execution-journal-v1|");
    bytes.extend_from_slice(head.to_string().as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(floor.to_string().as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(anchor_event_id.unwrap_or("").as_bytes());
    Ok(crate::hash::sha256_hex(&bytes))
}

fn read_boundary(connection: &Connection) -> Result<JournalBoundary> {
    let row: Option<(i64, i64, Option<String>, String, i64)> = connection
        .query_row(
            "SELECT head_seq,floor_seq,anchor_event_id,anchor_digest,schema_version FROM domain_journal_meta WHERE id=1",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql)?;
    let (head, floor, anchor_event_id, anchor_digest, schema_version) =
        row.ok_or_else(|| invalid("execution journal boundary row is missing"))?;
    if schema_version != JOURNAL_SCHEMA_VERSION as i64 {
        return Err(invalid(
            "execution journal has an unsupported boundary schema_version",
        ));
    }
    let boundary = JournalBoundary {
        head_cursor: to_u64(head)?,
        floor_cursor: to_u64(floor)?,
        anchor_event_id,
        anchor_digest,
    };
    let expected = boundary_digest(
        boundary.head_cursor,
        boundary.floor_cursor,
        boundary.anchor_event_id.as_deref(),
    )?;
    if boundary.anchor_digest != expected {
        return Err(invalid(
            "execution journal boundary digest does not match its head/floor/anchor",
        ));
    }
    validate_floor(connection, boundary.floor_cursor)?;
    Ok(boundary)
}

/// The durable retention boundary of the journal.
pub(crate) fn boundary(connection: &Connection) -> Result<JournalBoundary> {
    read_boundary(connection)
}

/// The lifecycle transition one event records.
///
/// The variants are deliberately the *canonical* lifecycle edges of the five
/// journalized entities. They are not a general-purpose activity log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum EventKind {
    JobCreated,
    JobUpdated,
    JobConfigurationSet,
    DependencyAdded,
    DependencyRemoved,
    BindingSet,
    AttemptCreated,
    AttemptUpdated,
    ExecutorCreated,
    ExecutorUpdated,
    CallCreated,
    CallUpdated,
    DispatchIntentCreated,
    DispatchIntentUpdated,
    ResultEvidenceRecorded,
    VerificationRecorded,
    /// An operator explicitly changed the Project's hard limit, or an admission
    /// moved the Project's durable accounting state. The event records the state
    /// the budget was left in — cap, rollups, status and reason — not a decision
    /// that any reader may replay as authority.
    BudgetLimitSet,
    /// A bounded spend was reserved before a provider-costly side effect.
    BudgetReservationRecorded,
    /// A reservation was returned to the Project because the request provably
    /// never reached the provider.
    BudgetReservationReleased,
    /// A reservation was discharged against a provider-reported usage record
    /// valued in canonical Money.
    BudgetSettled,
    /// A reservation is retained because no reliable actual exists. It still
    /// counts against the hard cap.
    BudgetUnresolved,
    /// A usage record was observed but not booked, because the reporting Attempt
    /// no longer held authority. Evidence, never an actual.
    BudgetUsageRetained,
}

/// The authority identity an event was committed under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventAuthority {
    /// The Attempt that held authority over the affected Job.
    pub attempt_id: String,
    /// That Attempt's generation. It is the fence value for the same work.
    pub generation: u64,
}

/// One durable journal entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvent {
    /// The stable cursor. Strictly increasing and gap-free per authority.
    pub seq: u64,
    /// Unique event identity, for dedup and audit references.
    pub event_id: String,
    pub kind: EventKind,
    /// The journalized entity this event describes.
    pub entity_type: String,
    pub entity_id: String,
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub attempt_id: Option<String>,
    pub executor_id: Option<String>,
    pub call_id: Option<String>,
    pub dispatch_intent_id: Option<String>,
    /// The subject's own generation.
    pub generation: Option<u64>,
    /// The authority identity in effect when the fact became canonical.
    pub authority: Option<EventAuthority>,
    /// The cursor of the event that caused this one, within the same commit.
    pub caused_by_seq: Option<u64>,
    /// A stable command identity (session binding, Call id) when one exists.
    pub causation_key: Option<String>,
    /// Who caused the fact.
    pub actor: String,
    /// The committed post-state of the subject.
    pub payload: Value,
    /// Local telemetry only. Never an ordering or authority input.
    pub recorded_at: i64,
}

impl ExecutionEvent {
    /// Whether the event still carries the authority identity, so a consumer
    /// can detect a record that cannot be attributed.
    pub fn has_authority(&self) -> bool {
        self.authority.is_some()
    }
}

/// Builder for one journal append. Only `kind`, the entity identity and the
/// payload are required; everything else is provenance that the canonical
/// commit path can supply.
pub(crate) struct EventDraft {
    kind: EventKind,
    entity_type: String,
    entity_id: String,
    project_id: Option<String>,
    job_id: Option<String>,
    attempt_id: Option<String>,
    executor_id: Option<String>,
    call_id: Option<String>,
    dispatch_intent_id: Option<String>,
    generation: Option<u64>,
    authority: Option<EventAuthority>,
    caused_by_seq: Option<u64>,
    causation_key: Option<String>,
    actor: Option<String>,
    payload: Value,
}

impl EventDraft {
    pub(crate) fn new(kind: EventKind, entity_type: &str, entity_id: &str, payload: Value) -> Self {
        Self {
            kind,
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
            project_id: None,
            job_id: None,
            attempt_id: None,
            executor_id: None,
            call_id: None,
            dispatch_intent_id: None,
            generation: None,
            authority: None,
            caused_by_seq: None,
            causation_key: None,
            actor: None,
            payload,
        }
    }

    pub(crate) fn project(mut self, project_id: &str) -> Self {
        self.project_id = Some(project_id.to_string());
        self
    }

    pub(crate) fn job(mut self, job_id: &str) -> Self {
        self.job_id = Some(job_id.to_string());
        self
    }

    pub(crate) fn generation(mut self, generation: u64) -> Self {
        self.generation = Some(generation);
        self
    }

    /// Attach the Attempt scope. Attempt-scoped subjects carry their own
    /// generation and, by construction, their own authority identity.
    pub(crate) fn attempt_scope(mut self, attempt_id: &str, job_id: &str, generation: u64) -> Self {
        self.attempt_id = Some(attempt_id.to_string());
        self.job_id = Some(job_id.to_string());
        self.generation = Some(generation);
        self.authority = Some(EventAuthority {
            attempt_id: attempt_id.to_string(),
            generation,
        });
        self
    }

    pub(crate) fn executor(mut self, executor_id: Option<&str>) -> Self {
        self.executor_id = executor_id.map(str::to_string);
        self
    }

    pub(crate) fn call(mut self, call_id: Option<&str>) -> Self {
        self.call_id = call_id.map(str::to_string);
        self
    }

    pub(crate) fn dispatch_intent(mut self, intent_id: Option<&str>) -> Self {
        self.dispatch_intent_id = intent_id.map(str::to_string);
        self
    }

    /// Pin the authority identity explicitly. Required for transitions that
    /// revoke authority, because after the update the Job no longer points at
    /// an authoritative Attempt and the journal would otherwise lose it.
    pub(crate) fn authority(mut self, attempt_id: &str, generation: u64) -> Self {
        self.authority = Some(EventAuthority {
            attempt_id: attempt_id.to_string(),
            generation,
        });
        self
    }

    pub(crate) fn caused_by_opt(mut self, seq: Option<u64>) -> Self {
        self.caused_by_seq = seq;
        self
    }

    pub(crate) fn causation_key(mut self, key: &str) -> Self {
        self.causation_key = Some(key.to_string());
        self
    }

    pub(crate) fn actor(mut self, actor: &str) -> Self {
        self.actor = Some(actor.to_string());
        self
    }
}

/// Serialize a committed post-state into an event payload.
pub(crate) fn value<T: Serialize>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| {
        invalid(&format!(
            "cannot serialize execution event payload: {error}"
        ))
    })
}

/// The authority identity currently recorded on a Job, read inside the writer
/// transaction so an event never describes a state that did not yet exist.
pub(crate) fn job_authority(
    transaction: &Transaction<'_>,
    job_id: &str,
) -> Result<(Option<String>, Option<EventAuthority>)> {
    let row: Option<(String, Option<String>, Option<i64>)> = transaction
        .query_row(
            "SELECT j.project_id,j.authoritative_attempt_id,a.generation FROM domain_jobs j \
             LEFT JOIN domain_attempts a ON a.id=j.authoritative_attempt_id WHERE j.id=?1",
            [job_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql)?;
    let Some((project_id, attempt_id, generation)) = row else {
        return Ok((None, None));
    };
    let authority = match (attempt_id, generation) {
        (Some(attempt_id), Some(generation)) => Some(EventAuthority {
            attempt_id,
            generation: to_u64(generation)?,
        }),
        _ => None,
    };
    Ok((Some(project_id), authority))
}

/// The Job and generation that own an Attempt, read inside the writer
/// transaction. Executor events need it because an Executor row carries only
/// its Attempt.
pub(crate) fn attempt_scope(
    connection: &Connection,
    attempt_id: &str,
) -> Result<Option<(String, u64)>> {
    let row: Option<(String, i64)> = connection
        .query_row(
            "SELECT job_id,generation FROM domain_attempts WHERE id=?1",
            [attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql)?;
    row.map(|(job_id, generation)| Ok((job_id, to_u64(generation)?)))
        .transpose()
}

/// Append one event inside the canonical writer transaction and return its
/// cursor. The caller must already have made the mutation succeed; if this
/// fails the whole transaction rolls back, so state and evidence stay atomic.
pub(crate) fn append(transaction: &Transaction<'_>, draft: EventDraft) -> Result<u64> {
    let EventDraft {
        kind,
        entity_type,
        entity_id,
        project_id,
        job_id,
        attempt_id,
        executor_id,
        call_id,
        dispatch_intent_id,
        generation,
        authority,
        caused_by_seq,
        causation_key,
        actor,
        payload,
    } = draft;
    // When the caller did not pin the authority identity, read the Job's current
    // authority. When it did, keep it: a fencing/terminal event must keep
    // naming the authority it is revoking.
    let (project_id, authority) = match (&authority, &job_id) {
        (Some(authority), _) => (project_id, Some(authority.clone())),
        (None, Some(job_id)) => {
            let (resolved_project, resolved) = job_authority(transaction, job_id)?;
            (project_id.or(resolved_project), resolved)
        }
        (None, None) => (project_id, None),
    };
    // The cursor comes from the durable boundary counter, not from
    // `MAX(seq) + 1`. Pruning removes rows, so a row-derived cursor would
    // restart at the surviving minimum and re-issue cursors that consumers have
    // already applied. This counter only ever moves forward, inside the same
    // transaction that writes the event.
    let next: i64 = transaction
        .query_row(
            "UPDATE domain_journal_meta SET head_seq=head_seq+1 WHERE id=1 RETURNING head_seq",
            [],
            |row| row.get(0),
        )
        .map_err(sql)?;
    let seq = to_u64(next)?;
    let event_id = format!("evt-{}", uuid::Uuid::now_v7());
    let payload = serde_json::to_string(&payload).map_err(|error| {
        invalid(&format!(
            "cannot serialize execution event payload: {error}"
        ))
    })?;
    // Re-bind the digest to the advanced head so the boundary stays verifiable
    // after every append.
    let (floor, anchor): (i64, Option<String>) = transaction
        .query_row(
            "SELECT floor_seq,anchor_event_id FROM domain_journal_meta WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql)?;
    transaction
        .execute(
            "UPDATE domain_journal_meta SET anchor_digest=?1 WHERE id=1",
            params![boundary_digest(seq, to_u64(floor)?, anchor.as_deref())?],
        )
        .map_err(sql)?;
    transaction
        .execute(
            "INSERT INTO domain_events(seq,event_id,kind,entity_type,entity_id,project_id,job_id,\
             attempt_id,executor_id,call_id,dispatch_intent_id,generation,authority_attempt_id,\
             authority_generation,caused_by_seq,causation_key,actor,payload,recorded_at) \
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
            params![
                to_i64(seq)?,
                event_id,
                kind.to_string(),
                entity_type,
                entity_id,
                project_id,
                job_id,
                attempt_id,
                executor_id,
                call_id,
                dispatch_intent_id,
                generation.map(to_i64).transpose()?,
                authority.as_ref().map(|value| value.attempt_id.clone()),
                authority
                    .as_ref()
                    .map(|value| to_i64(value.generation))
                    .transpose()?,
                caused_by_seq.map(to_i64).transpose()?,
                causation_key,
                actor.unwrap_or_else(|| AUTHORITY_ACTOR.to_string()),
                payload,
                now(),
            ],
        )
        .map_err(sql)?;
    Ok(seq)
}

/// The current journal head: the cursor of the newest committed event.
///
/// This is the durable counter, not `MAX(seq)`, so it stays correct after
/// pruning and never regresses when old events are removed.
pub(crate) fn head(connection: &Connection) -> Result<u64> {
    Ok(read_boundary(connection)?.head_cursor)
}

/// Classify a delta request against the durable boundary.
///
/// A consumer is served the complete delta or an explicit outcome. It is never
/// served a truncated suffix, and "your prefix was pruned" is never dressed up
/// as a gap.
pub(crate) fn delta_after(connection: &Connection, after: u64, limit: usize) -> Result<EventDelta> {
    let boundary = read_boundary(connection)?;
    if after > boundary.head_cursor {
        return Ok(EventDelta::AheadOfHead {
            requested: after,
            head_cursor: boundary.head_cursor,
        });
    }
    // The next event this consumer still needs is `after + 1`. If that is below
    // the retained floor the delta can no longer be produced from the journal.
    if after + 1 < boundary.floor_cursor {
        return Ok(EventDelta::ResyncRequired {
            requested: after,
            floor_cursor: boundary.floor_cursor,
            head_cursor: boundary.head_cursor,
        });
    }
    if after == boundary.head_cursor {
        return Ok(EventDelta::Empty {
            head_cursor: boundary.head_cursor,
        });
    }
    let events = read_after(connection, after, limit)?;
    if events.is_empty() {
        // The consumer is below the head, so events must exist. Their absence
        // means the retained range lost rows without the floor moving, so the
        // only correct answer is to resync rather than to claim an empty delta.
        return Ok(EventDelta::ResyncRequired {
            requested: after,
            floor_cursor: boundary.floor_cursor,
            head_cursor: boundary.head_cursor,
        });
    }
    // The delta is only complete if it starts exactly where the consumer left
    // off. Anything else is a real hole inside the retained range, so it is
    // reported rather than served as a partial suffix.
    let expected = after + 1;
    if events[0].seq != expected {
        return Err(invalid(&format!(
            "execution journal retained range has a hole: expected seq {expected}, found {}",
            events[0].seq
        )));
    }
    Ok(EventDelta::Available {
        events,
        head_cursor: boundary.head_cursor,
        floor_cursor: boundary.floor_cursor,
    })
}

/// The same boundary-checked delta for one Job's filtered view of the stream.
pub(crate) fn delta_for_job(
    connection: &Connection,
    job_id: &str,
    after: u64,
    limit: usize,
) -> Result<EventDelta> {
    let boundary = read_boundary(connection)?;
    if after > boundary.head_cursor {
        return Ok(EventDelta::AheadOfHead {
            requested: after,
            head_cursor: boundary.head_cursor,
        });
    }
    if after + 1 < boundary.floor_cursor {
        return Ok(EventDelta::ResyncRequired {
            requested: after,
            floor_cursor: boundary.floor_cursor,
            head_cursor: boundary.head_cursor,
        });
    }
    if after == boundary.head_cursor {
        return Ok(EventDelta::Empty {
            head_cursor: boundary.head_cursor,
        });
    }
    Ok(EventDelta::Available {
        events: read_for_job(connection, job_id, after, limit)?,
        head_cursor: boundary.head_cursor,
        floor_cursor: boundary.floor_cursor,
    })
}

/// Drop retained events below `keep_from` and move the floor there.
///
/// This is the only way the journal ever loses an event, it is always explicit,
/// and it touches nothing but the journal tables: canonical state, the head
/// cursor and the cursor numbering are unaffected, so a consumer's existing
/// cursor can never be re-issued to a different event.
pub(crate) fn prune(connection: &Connection, keep_from: u64) -> Result<JournalPrune> {
    if keep_from == INITIAL_CURSOR {
        return Err(invalid(
            "a journal prune must keep at least the first cursor of the retained range",
        ));
    }
    let transaction = begin(connection)?;
    let boundary = read_boundary(&transaction)?;
    if keep_from > boundary.head_cursor {
        return Err(invalid(
            "a journal prune cannot move the floor beyond the current head",
        ));
    }
    if keep_from <= boundary.floor_cursor {
        return Err(invalid("a journal prune must move the floor forward"));
    }
    // The anchor names the last event this prune removes, so the boundary keeps
    // a verifiable pointer into the history it discarded.
    let anchor: Option<(String, i64)> = transaction
        .query_row(
            "SELECT event_id,seq FROM domain_events WHERE seq=?1",
            [to_i64(keep_from - 1)?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql)?;
    let removed = transaction
        .execute(
            "DELETE FROM domain_events WHERE seq<?1",
            [to_i64(keep_from)?],
        )
        .map_err(sql)?;
    let anchor_event_id = anchor.map(|(event_id, _)| event_id);
    let digest = boundary_digest(boundary.head_cursor, keep_from, anchor_event_id.as_deref())?;
    transaction
        .execute(
            "UPDATE domain_journal_meta SET floor_seq=?1,anchor_event_id=?2,anchor_digest=?3 WHERE id=1",
            params![to_i64(keep_from)?, anchor_event_id, digest],
        )
        .map_err(sql)?;
    validate_floor(&transaction, keep_from)?;
    transaction.commit().map_err(sql)?;
    Ok(JournalPrune {
        floor_cursor: keep_from,
        head_cursor: boundary.head_cursor,
        removed: removed as u64,
        anchor_event_id,
        anchor_digest: digest,
    })
}

fn bound(limit: usize) -> usize {
    limit.clamp(1, MAX_EVENT_READ)
}

/// Read the events strictly after `after`, in cursor order.
pub(crate) fn read_after(
    connection: &Connection,
    after: u64,
    limit: usize,
) -> Result<Vec<ExecutionEvent>> {
    let mut statement = connection
        .prepare(&format!(
            "{SELECT_COLUMNS} WHERE seq>?1 ORDER BY seq ASC LIMIT ?2"
        ))
        .map_err(sql)?;
    let rows = statement
        .query_map(
            params![to_i64(after)?, to_i64(bound(limit) as u64)?],
            RawEvent::from_row,
        )
        .map_err(sql)?;
    collect(rows)
}

/// Read the events of one Job stream strictly after `after`, in cursor order.
pub(crate) fn read_for_job(
    connection: &Connection,
    job_id: &str,
    after: u64,
    limit: usize,
) -> Result<Vec<ExecutionEvent>> {
    let mut statement = connection
        .prepare(&format!(
            "{SELECT_COLUMNS} WHERE job_id=?1 AND seq>?2 ORDER BY seq ASC LIMIT ?3"
        ))
        .map_err(sql)?;
    let rows = statement
        .query_map(
            params![job_id, to_i64(after)?, to_i64(bound(limit) as u64)?],
            RawEvent::from_row,
        )
        .map_err(sql)?;
    collect(rows)
}

fn collect<I>(rows: I) -> Result<Vec<ExecutionEvent>>
where
    I: Iterator<Item = rusqlite::Result<RawEvent>>,
{
    let mut events = Vec::new();
    for row in rows {
        events.push(row.map_err(sql)?.into_event()?);
    }
    Ok(events)
}

struct RawEvent {
    seq: i64,
    event_id: String,
    kind: String,
    entity_type: String,
    entity_id: String,
    project_id: Option<String>,
    job_id: Option<String>,
    attempt_id: Option<String>,
    executor_id: Option<String>,
    call_id: Option<String>,
    dispatch_intent_id: Option<String>,
    generation: Option<i64>,
    authority_attempt_id: Option<String>,
    authority_generation: Option<i64>,
    caused_by_seq: Option<i64>,
    causation_key: Option<String>,
    actor: String,
    payload: String,
    recorded_at: i64,
}

impl RawEvent {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            seq: row.get(0)?,
            event_id: row.get(1)?,
            kind: row.get(2)?,
            entity_type: row.get(3)?,
            entity_id: row.get(4)?,
            project_id: row.get(5)?,
            job_id: row.get(6)?,
            attempt_id: row.get(7)?,
            executor_id: row.get(8)?,
            call_id: row.get(9)?,
            dispatch_intent_id: row.get(10)?,
            generation: row.get(11)?,
            authority_attempt_id: row.get(12)?,
            authority_generation: row.get(13)?,
            caused_by_seq: row.get(14)?,
            causation_key: row.get(15)?,
            actor: row.get(16)?,
            payload: row.get(17)?,
            recorded_at: row.get(18)?,
        })
    }

    fn into_event(self) -> Result<ExecutionEvent> {
        let kind: EventKind = self
            .kind
            .parse()
            .map_err(|_| invalid("execution journal contains an unknown event kind"))?;
        let authority = match (self.authority_attempt_id, self.authority_generation) {
            (Some(attempt_id), Some(generation)) => Some(EventAuthority {
                attempt_id,
                generation: to_u64(generation)?,
            }),
            (None, None) => None,
            _ => {
                return Err(invalid(
                    "execution journal has a partially recorded authority identity",
                ))
            }
        };
        let payload: Value = serde_json::from_str(&self.payload).map_err(|error| {
            invalid(&format!(
                "execution journal event {} has an unreadable payload: {error}",
                self.event_id
            ))
        })?;
        Ok(ExecutionEvent {
            seq: to_u64(self.seq)?,
            event_id: self.event_id,
            kind,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            project_id: self.project_id,
            job_id: self.job_id,
            attempt_id: self.attempt_id,
            executor_id: self.executor_id,
            call_id: self.call_id,
            dispatch_intent_id: self.dispatch_intent_id,
            generation: self.generation.map(to_u64).transpose()?,
            authority,
            caused_by_seq: self.caused_by_seq.map(to_u64).transpose()?,
            causation_key: self.causation_key,
            actor: self.actor,
            payload,
            recorded_at: self.recorded_at,
        })
    }
}

/// A Job dependency edge, as a projection-visible fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyEdge {
    pub project_id: String,
    pub job_id: String,
    pub prerequisite_job_id: String,
}

/// A stable session/runtime binding to the exact Job and Attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobBinding {
    pub binding_key: String,
    pub project_id: String,
    pub job_id: String,
    pub attempt_id: Option<String>,
}

/// A Job's frozen execution configuration at a revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredJobConfiguration {
    pub job_id: String,
    pub configuration: Value,
    pub revision: u64,
}

/// A result that was retained as evidence but is not canonical state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultEvidence {
    pub call_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub disposition: String,
    pub created_at: i64,
}

/// A recorded Call verification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredVerification {
    pub call_id: String,
    pub passed: bool,
    pub report: Value,
    pub created_at: i64,
}

/// One bounded spend reservation as a projection-visible accounting fact.
///
/// It names the Call the reservation was taken for, so a projection can answer
/// "which operation still holds money" without re-deriving anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationFact {
    pub project_id: String,
    pub call_id: String,
    pub reservation: Reservation,
}

/// A Project budget's durable accounting state as a projection-visible fact.
///
/// It is the evidence of what the cap and the rollups were at a given cursor —
/// including a denied admission, which moves no money and would otherwise leave
/// no trace. It is not the cap: the authority is the canonical
/// `domain_budgets` row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BudgetLimitFact {
    pub project_id: String,
    pub hard_limit: Option<Money>,
    pub origin: BudgetOrigin,
    pub status: BudgetStatus,
    pub currency: String,
    pub settled_micros: i64,
    pub reserved_micros: i64,
    pub unresolved_micros: i64,
    /// The reason code of the decision that produced this state.
    pub reason: Option<String>,
    pub updated_at: i64,
}

/// A provider-reported usage record that was observed but deliberately *not*
/// booked as an actual.
///
/// The only reason a usage record is retained without an actual is that the
/// Attempt reporting it no longer holds authority. The spend still happened, so
/// the fact is kept; the budget keeps holding the reservation instead of
/// adopting an amount a fenced authority may not assert.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct UsageEvidence {
    pub project_id: String,
    pub call_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub dispatch_intent_id: Option<String>,
    pub usage: UsageRecord,
    pub reason_code: String,
    pub created_at: i64,
}

/// The canonical rows plus the journal head, read in one transaction.
///
/// This is the base a projection starts from. The cursor and the rows are read
/// together, so a snapshot can never pair rows from before a commit with a
/// cursor from after it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExecutionSnapshot {
    pub cursor: u64,
    /// The retention floor at the moment this snapshot was read. A consumer that
    /// later falls below it must resync rather than expect a partial delta.
    pub floor_cursor: u64,
    /// The boundary anchor matching `floor_cursor`, so a consumer can tell a
    /// boundary it trusts from a rewritten or truncated one.
    pub anchor_digest: String,
    pub projects: Vec<Project>,
    pub jobs: Vec<Job>,
    pub attempts: Vec<Attempt>,
    pub executors: Vec<Executor>,
    pub calls: Vec<Call>,
    pub dispatch_intents: Vec<DispatchIntent>,
    pub dependencies: Vec<DependencyEdge>,
    pub bindings: Vec<JobBinding>,
    pub job_configurations: Vec<StoredJobConfiguration>,
    pub result_evidence: Vec<ResultEvidence>,
    pub verifications: Vec<StoredVerification>,
    /// The durable accounting facts. They are part of the snapshot because a
    /// projection that started after the money moved must still be able to
    /// rebuild the money view from the same boundary.
    pub reservations: Vec<ReservationFact>,
    pub settlements: Vec<Settlement>,
    pub budget_limits: Vec<BudgetLimitFact>,
    pub usage_evidence: Vec<UsageEvidence>,
}

/// The outcome of applying one event to a projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Applied and the projection cursor advanced.
    Applied,
    /// Already applied; ignored idempotently.
    Duplicate,
    /// A forward gap: application must stop and the consumer must resync.
    Gap { expected: u64, got: u64 },
}

/// How far a replay got, so a consumer can state its own completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayStatus {
    /// Every supplied event was applied contiguously.
    Complete,
    /// At least one already-applied event was ignored; the rest applied.
    CompleteWithDuplicates { ignored: usize },
    /// A forward gap stopped application before the end of the batch.
    Gap { expected: u64, got: u64 },
}

impl ReplayStatus {
    /// Whether the projection is complete for the last applied cursor.
    pub fn is_complete(&self) -> bool {
        !matches!(self, Self::Gap { .. })
    }
}

/// A rebuildable read model of the canonical execution domain.
///
/// It is a projection: it may be dropped at any time and rebuilt from a
/// snapshot plus subsequent events. It never feeds an execution decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExecutionProjection {
    pub cursor: u64,
    pub projects: BTreeMap<String, Project>,
    pub jobs: BTreeMap<String, Job>,
    pub attempts: BTreeMap<String, Attempt>,
    pub executors: BTreeMap<String, Executor>,
    pub calls: BTreeMap<String, Call>,
    pub dispatch_intents: BTreeMap<String, DispatchIntent>,
    pub dependencies: BTreeSet<(String, String, String)>,
    pub bindings: BTreeMap<String, JobBinding>,
    pub job_configurations: BTreeMap<String, StoredJobConfiguration>,
    pub result_evidence: Vec<ResultEvidence>,
    pub verifications: BTreeMap<String, StoredVerification>,
    /// Reservations keyed by `reservation_id`.
    pub reservations: BTreeMap<String, ReservationFact>,
    /// Settlements keyed by `settlement_id`. The key is deterministic over the
    /// reservation, the authority and the disposition, so re-applying a
    /// duplicate overwrites the identical record instead of double-booking it.
    pub settlements: BTreeMap<String, Settlement>,
    /// Hard limits keyed by `project_id`.
    pub budget_limits: BTreeMap<String, BudgetLimitFact>,
    /// Usage that was observed but never booked, in observation order.
    pub usage_evidence: Vec<UsageEvidence>,
}

impl From<ExecutionSnapshot> for ExecutionProjection {
    fn from(snapshot: ExecutionSnapshot) -> Self {
        let mut projection = Self {
            cursor: snapshot.cursor,
            ..Self::default()
        };
        for project in snapshot.projects {
            projection.projects.insert(project.id.clone(), project);
        }
        for job in snapshot.jobs {
            projection.jobs.insert(job.id.clone(), job);
        }
        for attempt in snapshot.attempts {
            projection.attempts.insert(attempt.id.clone(), attempt);
        }
        for executor in snapshot.executors {
            projection.executors.insert(executor.id.clone(), executor);
        }
        for call in snapshot.calls {
            projection.calls.insert(call.id.clone(), call);
        }
        for intent in snapshot.dispatch_intents {
            projection
                .dispatch_intents
                .insert(intent.id.clone(), intent);
        }
        for edge in snapshot.dependencies {
            projection.dependencies.insert((
                edge.project_id,
                edge.job_id,
                edge.prerequisite_job_id,
            ));
        }
        for binding in snapshot.bindings {
            projection
                .bindings
                .insert(binding.binding_key.clone(), binding);
        }
        for configuration in snapshot.job_configurations {
            projection
                .job_configurations
                .insert(configuration.job_id.clone(), configuration);
        }
        projection.result_evidence = snapshot.result_evidence;
        for verification in snapshot.verifications {
            projection
                .verifications
                .insert(verification.call_id.clone(), verification);
        }
        for reservation in snapshot.reservations {
            let id = reservation.reservation.reservation_id.clone();
            projection.reservations.insert(id, reservation);
        }
        for settlement in snapshot.settlements {
            let id = settlement.settlement_id.clone();
            projection.settlements.insert(id, settlement);
        }
        for limit in snapshot.budget_limits {
            projection
                .budget_limits
                .insert(limit.project_id.clone(), limit);
        }
        projection.usage_evidence = snapshot.usage_evidence;
        projection
    }
}

impl ExecutionProjection {
    /// Apply one event under the contiguous rule.
    pub fn apply(&mut self, event: &ExecutionEvent) -> Result<ApplyOutcome> {
        if event.seq <= self.cursor {
            return Ok(ApplyOutcome::Duplicate);
        }
        if event.seq != self.cursor + 1 {
            return Ok(ApplyOutcome::Gap {
                expected: self.cursor + 1,
                got: event.seq,
            });
        }
        self.reduce(event)?;
        self.cursor = event.seq;
        Ok(ApplyOutcome::Applied)
    }

    fn reduce(&mut self, event: &ExecutionEvent) -> Result<()> {
        let payload = event.payload.clone();
        match event.entity_type.as_str() {
            "job" => {
                let job: Job = decode(&payload, &event.event_id)?;
                self.jobs.insert(job.id.clone(), job);
            }
            "attempt" => {
                let attempt: Attempt = decode(&payload, &event.event_id)?;
                self.attempts.insert(attempt.id.clone(), attempt);
            }
            "executor" => {
                let executor: Executor = decode(&payload, &event.event_id)?;
                self.executors.insert(executor.id.clone(), executor);
            }
            "call" => {
                let call: Call = decode(&payload, &event.event_id)?;
                self.calls.insert(call.id.clone(), call);
            }
            "dispatch_intent" => {
                let intent: DispatchIntent = decode(&payload, &event.event_id)?;
                self.dispatch_intents.insert(intent.id.clone(), intent);
            }
            "dependency" => {
                let edge: DependencyEdge = decode(&payload, &event.event_id)?;
                let key = (edge.project_id, edge.job_id, edge.prerequisite_job_id);
                match event.kind {
                    EventKind::DependencyAdded => {
                        self.dependencies.insert(key);
                    }
                    EventKind::DependencyRemoved => {
                        self.dependencies.remove(&key);
                    }
                    _ => return Err(unexpected(&event.event_id, event.kind)),
                }
            }
            "binding" => {
                let binding: JobBinding = decode(&payload, &event.event_id)?;
                self.bindings.insert(binding.binding_key.clone(), binding);
            }
            "job_configuration" => {
                let configuration: StoredJobConfiguration = decode(&payload, &event.event_id)?;
                self.job_configurations
                    .insert(configuration.job_id.clone(), configuration);
            }
            "result_evidence" => {
                let evidence: ResultEvidence = decode(&payload, &event.event_id)?;
                self.result_evidence.push(evidence);
            }
            "verification" => {
                let verification: StoredVerification = decode(&payload, &event.event_id)?;
                self.verifications
                    .insert(verification.call_id.clone(), verification);
            }
            "reservation" => {
                let fact: ReservationFact = decode(&payload, &event.event_id)?;
                self.reservations
                    .insert(fact.reservation.reservation_id.clone(), fact);
            }
            "settlement" => {
                let settlement: Settlement = decode(&payload, &event.event_id)?;
                self.settlements
                    .insert(settlement.settlement_id.clone(), settlement);
            }
            "budget_limit" => {
                let limit: BudgetLimitFact = decode(&payload, &event.event_id)?;
                self.budget_limits.insert(limit.project_id.clone(), limit);
            }
            "usage_evidence" => {
                let evidence: UsageEvidence = decode(&payload, &event.event_id)?;
                self.usage_evidence.push(evidence);
            }
            other => {
                return Err(invalid(&format!(
                    "execution journal event {} names an unknown entity type {other}",
                    event.event_id
                )))
            }
        }
        Ok(())
    }

    /// Rebuild one Project's money view from the accounting facts.
    ///
    /// The view is derived per identity and then aggregated, never by summing
    /// the journal:
    ///
    /// - `reserved` and `unresolved` come from the *current* state of each
    ///   reservation. The map is keyed by reservation id and holds the latest
    ///   post-state journaled for it, so a reservation that has since been
    ///   settled or released contributes nothing: a refinement moves the same
    ///   money between states exactly once instead of leaving the earlier state
    ///   counting as well.
    /// - `settled`, `released` and `overage` come from the settlements that
    ///   actually asserted Money. An uncertain dispatch, a retention and a
    ///   conflicting payload that was refused are all `Unresolved` assertions
    ///   and assert nothing by definition, so they contribute nothing; the fact
    ///   that discharged the reservation contributes its effect exactly once.
    ///   The map is keyed by settlement id, so re-applying the same fact — a
    ///   duplicated provider result, a replayed commit — overwrites it rather
    ///   than adding to it.
    ///
    /// The result is the same whether the projection is rebuilt from the full
    /// journal or from a snapshot plus the delta after it: both apply the same
    /// facts in the same order, and the snapshot carries the same current
    /// reservation and settlement states the delta would have produced.
    pub fn accounting(&self, project_id: &str) -> ProjectAccounting {
        let mut accounting = ProjectAccounting {
            project_id: project_id.to_string(),
            ..ProjectAccounting::default()
        };
        if let Some(limit) = self.budget_limits.get(project_id) {
            accounting.hard_limit = limit.hard_limit.clone();
            accounting.origin = limit.origin;
            accounting.status = limit.status;
            accounting.currency = limit.currency.clone();
            accounting.updated_at = limit.updated_at;
        }
        for fact in self.reservations.values() {
            if fact.project_id != project_id {
                continue;
            }
            if fact.reservation.state == crate::orchestration::budget::ReservationState::Reserved {
                accounting.reserved_micros = accounting
                    .reserved_micros
                    .saturating_add(fact.reservation.amount.micros);
                if fact.reservation.unresolved {
                    accounting.unresolved_micros = accounting
                        .unresolved_micros
                        .saturating_add(fact.reservation.amount.micros);
                }
            }
        }
        for settlement in self.settlements.values() {
            if settlement.project_id != project_id {
                continue;
            }
            // Only a fact that asserted Money moves these totals. A refinement
            // fact carries no Money of its own — it is the same reservation's
            // money being restated — so an `Unresolved` assertion can never be
            // read as a second charge, a second release or a second overage.
            if !settlement.disposition.asserts_actual() {
                continue;
            }
            accounting.settled_micros = accounting
                .settled_micros
                .saturating_add(settlement.effect.actual.micros);
            accounting.released_micros = accounting
                .released_micros
                .saturating_add(settlement.effect.released.micros);
            accounting.overage_micros = accounting
                .overage_micros
                .saturating_add(settlement.effect.overage.micros);
        }
        accounting
    }
}

/// One Project's money, rebuilt from journaled accounting facts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProjectAccounting {
    pub project_id: String,
    pub currency: String,
    pub hard_limit: Option<Money>,
    pub origin: BudgetOrigin,
    pub status: BudgetStatus,
    pub settled_micros: i64,
    pub reserved_micros: i64,
    pub unresolved_micros: i64,
    pub released_micros: i64,
    pub overage_micros: i64,
    pub updated_at: i64,
}

fn decode<T: serde::de::DeserializeOwned>(payload: &Value, event_id: &str) -> Result<T> {
    serde_json::from_value(payload.clone()).map_err(|error| {
        invalid(&format!(
            "execution journal event {event_id} does not decode into its projection entity: {error}"
        ))
    })
}

fn unexpected(event_id: &str, kind: EventKind) -> OcgError {
    invalid(&format!(
        "execution journal event {event_id} is a {kind} transition that this entity cannot accept"
    ))
}

/// Rebuild a projection from a snapshot plus the events strictly after its
/// cursor.
///
/// A forward gap is not an error: it stops application and is reported as
/// [`ReplayStatus::Gap`] so the consumer resyncs from a fresh snapshot instead
/// of inventing an order. An unreadable event *is* an error.
pub fn replay(
    snapshot: ExecutionSnapshot,
    events: &[ExecutionEvent],
) -> Result<(ExecutionProjection, ReplayStatus)> {
    let mut projection = ExecutionProjection::from(snapshot);
    let mut duplicates = 0usize;
    for event in events {
        match projection.apply(event)? {
            ApplyOutcome::Applied => {}
            ApplyOutcome::Duplicate => duplicates += 1,
            ApplyOutcome::Gap { expected, got } => {
                return Ok((projection, ReplayStatus::Gap { expected, got }));
            }
        }
    }
    let status = if duplicates == 0 {
        ReplayStatus::Complete
    } else {
        ReplayStatus::CompleteWithDuplicates {
            ignored: duplicates,
        }
    };
    Ok((projection, status))
}

/// Start an immediate writer transaction without requiring a mutable borrow of
/// the repository, so a `&self` mutation method can still make its event and
/// its state change atomic.
pub(crate) fn begin(connection: &Connection) -> Result<Transaction<'_>> {
    Transaction::new_unchecked(connection, TransactionBehavior::Immediate).map_err(sql)
}

/// Start a read transaction the same way, so a snapshot's rows and its cursor
/// are read from one consistent view without a mutable borrow.
pub(crate) fn begin_read(connection: &Connection) -> Result<Transaction<'_>> {
    Transaction::new_unchecked(connection, TransactionBehavior::Deferred).map_err(sql)
}
