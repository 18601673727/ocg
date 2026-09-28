//! Durable replay authority: one atomic document that owns the current
//! authoritative Mission/Approval/Resource domains plus a bounded, hash-chained
//! journal of normalized domain events.
//!
//! ## Authority and invariant
//!
//! - The replay document under
//!   `.ocg/orchestration/replay/state.json` is the **authority** for
//!   durable orchestration state. Existing per-domain files
//!   (`missions/<id>.json`, `approvals/<id>.json`, `resources/registry.json`)
//!   remain compatibility projections that are derived from it. Once the
//!   authority is initialized the public domain readers are served by it; only
//!   the first bootstrap reads projections (read-only and strictly).
//! - A cursor is `{ epoch, seq }`. `seq` is assigned while holding a
//!   project-wide cross-process file lock and strictly increments within an
//!   epoch. The head cursor and the snapshot are always read together under the
//!   same lock, so a caller can never observe a state/cursor pair from two
//!   different writes.
//! - The journal is an append-only hash chain. Each envelope hashes its
//!   `(epoch, seq, prev_hash, event)`; the snapshot carries a state digest.
//!   Loading validates schema, epoch reason, journal length, cursor/head/floor
//!   consistency, contiguity, the chained hashes, the state digest, marker
//!   epoch agreement and every identity (reusing the domain validators). Hash
//!   serialization is fallible. Anything corrupt or ambiguous fails closed and
//!   is never silently reset.
//! - Mission upserts are monotonic: within a generation the revision must
//!   strictly advance, a generation may only advance, and a content change at
//!   an unchanged generation/revision is rejected. An exact no-op records
//!   nothing.
//! - Resource saves commit the complete normalized observation set as one
//!   atomic `ResourceReplace` (so removals and an emptied registry are
//!   representable), and the resource/approval domains keep their own
//!   `MAX_RESOURCES`/`MAX_APPROVALS` bounds.
//! - Retention is bounded. Pruning advances a floor and keeps the hash of the
//!   last pruned envelope as an anchor, so the retained suffix is still
//!   chained. A replay request whose prefix was pruned reports `Expired` and
//!   never returns a partial suffix.
//! - A new epoch is only ever started by the explicit
//!   [`SnapshotService::begin_new_epoch_after_continuity_loss`] assertion with a
//!   nonempty bounded reason, and the increment is checked. Ordinary corruption
//!   never resets the epoch.
//!
//! Only durable domains enter the snapshot: full `Mission` values (which carry
//! lifecycle, policy receipts and budget), `ApprovalRecord`s and normalized
//! durable `ResourceObservation`s. Sessions, UI, messages, tokens, runtime-only
//! facts and HTTP/SSE traffic are explicitly **not** durable authority and are
//! never stored here.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{
    BudgetConfig, CostBasis, QuotaFacts, SpendAction, SpendAssessment,
};
use crate::orchestration::dispatch::{
    DispatchId, DispatchRecord, DispatchState, DispatchUsage, MAX_DISPATCHES,
};
use crate::orchestration::mission::{self, Mission};
use crate::orchestration::policy::{self, ApprovalRecord, MAX_APPROVALS};
use crate::resources::{self, ResourceObservation, MAX_RESOURCES};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Schema version of the replay document. Unknown versions fail closed.
pub const REPLAY_SCHEMA_VERSION: u32 = 1;
/// Directory (under the orchestration state dir) that owns the replay store.
pub const REPLAY_DIR: &str = "replay";
/// The authoritative document file name.
pub const REPLAY_FILE: &str = "state.json";
/// The stable lock file name. It is never deleted.
pub const REPLAY_LOCK_FILE: &str = "state.lock";
/// The initialization marker. Its presence plus a missing document is a
/// persistence failure, never a bootstrap.
pub const REPLAY_MARKER_FILE: &str = "replay.initialized";
/// Default bounded journal retention.
pub const DEFAULT_RETENTION: usize = 256;
/// Largest accepted journal retention.
pub const MAX_RETENTION: usize = 65_536;
/// Upper bound on an explicit continuity-loss reason.
pub const MAX_EPOCH_REASON_BYTES: usize = 240;
/// The hash that precedes the first envelope of an epoch.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// A durable position in the replay journal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Cursor {
    pub epoch: u64,
    pub seq: u64,
}

/// The authoritative durable domains. Each map is keyed by the stable domain
/// identity, so an upsert is a deterministic replace.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AuthoritativeSnapshot {
    pub missions: BTreeMap<String, Mission>,
    pub approvals: BTreeMap<String, ApprovalRecord>,
    pub resources: BTreeMap<String, ResourceObservation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub dispatches: BTreeMap<String, DispatchRecord>,
}

/// A normalized upsert of one durable domain value.
///
/// Variant sizes differ because a full durable domain value is carried
/// verbatim. Boxing would not change the serialized form; the plain payload
/// keeps the public API ergonomic and the journal is bounded anyway.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DomainEvent {
    MissionUpsert {
        mission: Mission,
    },
    ApprovalUpsert {
        approval: ApprovalRecord,
    },
    ResourceUpsert {
        observation: ResourceObservation,
    },
    /// Replace the complete durable resource observation set in one transition.
    ///
    /// This is the low-level `resources::save` transition: it represents
    /// removals and an empty registry atomically, so a batch of observations
    /// can never be observed half-applied. Entries are keyed by resource id.
    ResourceReplace {
        resources: BTreeMap<String, ResourceObservation>,
    },
    DispatchUpsert {
        dispatch: DispatchRecord,
    },
}

impl DomainEvent {
    /// A short, stable kind label for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::MissionUpsert { .. } => "mission_upsert",
            Self::ApprovalUpsert { .. } => "approval_upsert",
            Self::ResourceUpsert { .. } => "resource_upsert",
            Self::ResourceReplace { .. } => "resource_replace",
            Self::DispatchUpsert { dispatch } => match dispatch.state {
                DispatchState::Reserved => "dispatch_reserved",
                DispatchState::DispatchStarted => "dispatch_started",
                DispatchState::Completed => "dispatch_completed",
                DispatchState::Unresolved => "dispatch_unresolved",
                DispatchState::KnownNotDispatched | DispatchState::Failed => "dispatch_blocked",
                DispatchState::Settled => "dispatch_settled",
            },
        }
    }
}

/// One journal entry: a cursor, the previous hash and its own chained hash.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub cursor: Cursor,
    pub prev_hash: String,
    pub hash: String,
    pub event: DomainEvent,
}

/// The explicit result of a replay request.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplayAfter {
    /// The events strictly after the requested cursor, in order.
    Success { events: Vec<EventEnvelope> },
    /// The cursor is already at the head; there is nothing to replay.
    Empty,
    /// The cursor belongs to a different epoch.
    WrongEpoch { expected: u64, got: u64 },
    /// The retained prefix no longer covers the requested cursor. No partial
    /// suffix is ever returned.
    Expired { floor_seq: u64, requested_seq: u64 },
    /// The cursor is ahead of the current head.
    Future { head_seq: u64, requested_seq: u64 },
    /// The store could not be read or validated, or was never initialized.
    PersistenceFailure { detail: String },
}

impl ReplayAfter {
    /// Whether this is a usable replay outcome (`Success` or `Empty`).
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Success { .. } | Self::Empty)
    }
}

/// Configuration for a [`SnapshotService`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// Maximum number of retained journal envelopes.
    pub retention: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            retention: DEFAULT_RETENTION,
        }
    }
}

impl SnapshotConfig {
    pub fn new(retention: usize) -> Self {
        Self { retention }
    }

    fn validate(&self) -> Result<()> {
        if self.retention == 0 || self.retention > MAX_RETENTION {
            return Err(OcgError::config(format!(
                "replay retention must be between 1 and {MAX_RETENTION}"
            )));
        }
        Ok(())
    }
}

/// A handle to the durable replay authority.
///
/// The handle itself is cheap and stateless: every operation re-reads the
/// document while holding the project-wide lock, so a long-lived handle still
/// observes cross-process writes and a snapshot/cursor pair is always atomic.
#[derive(Debug, Clone)]
pub struct SnapshotService {
    root: PathBuf,
    retention: usize,
}

impl SnapshotService {
    /// Compare current ownership and atomically reserve one distinct network
    /// attempt with its Dispatch record. A failure cannot authorize a send.
    pub fn reserve_dispatch(
        &self,
        mut dispatch: DispatchRecord,
        config: &BudgetConfig,
        quota: QuotaFacts,
    ) -> Result<(SpendAssessment, DispatchRecord)> {
        dispatch.validate()?;
        if dispatch.state != DispatchState::Reserved || dispatch.reservation_id.is_some() {
            return Err(OcgError::config(
                "dispatch must enter as a fresh reservation",
            ));
        }
        let _lock = ReplayLock::acquire(&self.root)?;
        let mut document = read_document(&self.root)?;
        if document
            .snapshot
            .dispatches
            .contains_key(dispatch.id.as_str())
        {
            return Err(OcgError::config("DispatchId was already admitted"));
        }
        let mission = document
            .snapshot
            .missions
            .get(&dispatch.mission_id)
            .ok_or_else(|| OcgError::config("no current Mission for provider dispatch"))?;
        if mission.is_terminal()
            || mission.generation != dispatch.generation
            || mission.session_id.as_deref() != Some(&dispatch.root_id)
        {
            return Err(OcgError::config("provider dispatch owner is stale"));
        }
        let mut mission = mission.clone();
        // The configurable operation estimate is NOT a provider-enforced upper
        // bound. An estimate cannot authorize a hard-capped network attempt.
        let assessment = mission.admit_spend(
            config,
            SpendAction::ProviderDispatch,
            dispatch.id.as_str(),
            CostBasis::Unknown,
            quota,
            dispatch.created_at,
        );
        if !assessment.is_allowed() {
            return Ok((assessment, dispatch));
        }
        dispatch.reservation_id = assessment.reservation_id.clone();
        if mission != document.snapshot.missions[&dispatch.mission_id] {
            append_to_document(
                &mut document,
                DomainEvent::MissionUpsert { mission },
                self.retention,
            )?;
        }
        append_to_document(
            &mut document,
            DomainEvent::DispatchUpsert {
                dispatch: dispatch.clone(),
            },
            self.retention,
        )?;
        write_document(&self.root, &document)?;
        Ok((assessment, dispatch))
    }

    /// Claim the network send in durable state; it can succeed only once.
    pub fn start_dispatch(&self, id: &DispatchId, now: i64) -> Result<DispatchRecord> {
        let _lock = ReplayLock::acquire(&self.root)?;
        let mut document = read_document(&self.root)?;
        let mut dispatch = document
            .snapshot
            .dispatches
            .get(id.as_str())
            .ok_or_else(|| OcgError::config("unknown DispatchId"))?
            .clone();
        if dispatch.state != DispatchState::Reserved {
            return Err(OcgError::config("DispatchId cannot send twice"));
        }
        let mission = document
            .snapshot
            .missions
            .get(&dispatch.mission_id)
            .ok_or_else(|| OcgError::config("dispatch Mission missing"))?;
        if mission.is_terminal()
            || mission.generation != dispatch.generation
            || mission.session_id.as_deref() != Some(&dispatch.root_id)
        {
            return Err(OcgError::config(
                "dispatch Mission root is no longer current",
            ));
        }
        dispatch.state = DispatchState::DispatchStarted;
        dispatch.updated_at = now;
        append_to_document(
            &mut document,
            DomainEvent::DispatchUpsert {
                dispatch: dispatch.clone(),
            },
            self.retention,
        )?;
        write_document(&self.root, &document)?;
        Ok(dispatch)
    }

    /// Record a terminal attempt. An uncertain attempt retains its reservation.
    pub fn finish_dispatch(
        &self,
        id: &DispatchId,
        state: DispatchState,
        usage: Option<DispatchUsage>,
        failure: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        if !matches!(
            state,
            DispatchState::Settled
                | DispatchState::Unresolved
                | DispatchState::KnownNotDispatched
                | DispatchState::Failed
        ) {
            return Err(OcgError::config("invalid dispatch completion state"));
        }
        let _lock = ReplayLock::acquire(&self.root)?;
        let mut document = read_document(&self.root)?;
        let mut dispatch = document
            .snapshot
            .dispatches
            .get(id.as_str())
            .ok_or_else(|| OcgError::config("unknown DispatchId"))?
            .clone();
        if dispatch.state == state {
            return Ok(false);
        }
        if dispatch.state != DispatchState::DispatchStarted {
            return Err(OcgError::config("dispatch not started or already finished"));
        }
        let mut mission = document
            .snapshot
            .missions
            .get(&dispatch.mission_id)
            .ok_or_else(|| OcgError::config("dispatch Mission missing"))?
            .clone();
        if mission.generation != dispatch.generation {
            return Err(OcgError::config("dispatch Mission generation changed"));
        }
        if let Some(reservation) = &dispatch.reservation_id {
            match state {
                DispatchState::Settled => {
                    mission.settle_spend(reservation, None, now)?;
                }
                DispatchState::Unresolved | DispatchState::Failed => {
                    mission.mark_spend_unresolved(reservation, now);
                }
                DispatchState::KnownNotDispatched => {
                    mission.release_spend(reservation, now);
                }
                _ => unreachable!(),
            }
        }
        dispatch.state = state;
        dispatch.updated_at = now;
        dispatch.usage = usage;
        dispatch.failure_class = failure.map(str::to_string);
        dispatch.validate()?;
        if mission != document.snapshot.missions[&dispatch.mission_id] {
            append_to_document(
                &mut document,
                DomainEvent::MissionUpsert { mission },
                self.retention,
            )?;
        }
        append_to_document(
            &mut document,
            DomainEvent::DispatchUpsert { dispatch },
            self.retention,
        )?;
        write_document(&self.root, &document)?;
        Ok(true)
    }
    /// Open the replay authority, bootstrapping from existing durable domains
    /// on first use. A previously initialized store whose document is missing
    /// fails closed.
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_config(root, SnapshotConfig::default())
    }

    /// Open with an explicit (test-tunable) retention bound.
    pub fn open_with_config(root: &Path, config: SnapshotConfig) -> Result<Self> {
        config.validate()?;
        {
            let _lock = ReplayLock::acquire(root)?;
            ensure_initialized_locked(root)?;
        }
        Ok(Self {
            root: root.to_path_buf(),
            retention: config.retention,
        })
    }

    /// Atomically read the current snapshot and its head cursor under the same
    /// project-wide lock used by [`SnapshotService::append`].
    pub fn snapshot_with_cursor(&self) -> Result<(AuthoritativeSnapshot, Cursor)> {
        let _lock = ReplayLock::acquire(&self.root)?;
        let document = read_document(&self.root)?;
        let cursor = document.head_cursor();
        Ok((document.snapshot, cursor))
    }

    /// The current authoritative snapshot.
    pub fn snapshot(&self) -> Result<AuthoritativeSnapshot> {
        self.snapshot_with_cursor().map(|(snapshot, _)| snapshot)
    }

    /// The current head cursor.
    pub fn head(&self) -> Result<Cursor> {
        self.snapshot_with_cursor().map(|(_, cursor)| cursor)
    }

    /// Apply one typed upsert and advance the cursor in a single atomic durable
    /// write. Returns `Ok(None)` when the event does not change the
    /// authoritative state (a no-op never creates an event).
    pub fn append(&self, event: DomainEvent) -> Result<Option<Cursor>> {
        let _lock = ReplayLock::acquire(&self.root)?;
        ensure_initialized_locked(&self.root)?;
        let mut document = read_document(&self.root)?;
        let Some(cursor) = append_to_document(&mut document, event, self.retention)? else {
            return Ok(None);
        };
        write_document(&self.root, &document)?;
        Ok(Some(cursor))
    }

    /// Atomically compare and commit one Mission mutation under the replay
    /// lock. Returns `false` when the expected revision/generation/owner no
    /// longer matches. Repeating an already-committed candidate is idempotent.
    pub(crate) fn compare_and_append_mission(
        root: &Path,
        mission: &Mission,
        expected_revision: u64,
        expected_owner: Option<&str>,
    ) -> Result<bool> {
        let _lock = ReplayLock::acquire(root)?;
        ensure_initialized_locked(root)?;
        let mut document = read_document(root)?;
        let Some(current) = document.snapshot.missions.get(&mission.mission_id) else {
            return Ok(false);
        };
        if current == mission {
            return Ok(true);
        }
        if current.revision != expected_revision
            || current.generation != mission.generation
            || expected_owner.is_some_and(|owner| current.session_id.as_deref() != Some(owner))
        {
            return Ok(false);
        }
        let event = DomainEvent::MissionUpsert {
            mission: mission.clone(),
        };
        let Some(_) = append_to_document(&mut document, event, DEFAULT_RETENTION)? else {
            return Ok(true);
        };
        write_document(root, &document)?;
        Ok(true)
    }

    /// Replay the events strictly after `cursor`.
    ///
    /// Never returns a partial suffix: when the retained window no longer
    /// covers the request the result is [`ReplayAfter::Expired`].
    pub fn replay_after(&self, cursor: Cursor) -> ReplayAfter {
        let _lock = match ReplayLock::acquire(&self.root) {
            Ok(lock) => lock,
            Err(error) => {
                return ReplayAfter::PersistenceFailure {
                    detail: error.to_string(),
                }
            }
        };
        let document = match read_document(&self.root) {
            Ok(document) => document,
            Err(error) => {
                return ReplayAfter::PersistenceFailure {
                    detail: error.to_string(),
                }
            }
        };
        if cursor.epoch != document.epoch {
            return ReplayAfter::WrongEpoch {
                expected: document.epoch,
                got: cursor.epoch,
            };
        }
        if cursor.seq > document.head_seq {
            return ReplayAfter::Future {
                head_seq: document.head_seq,
                requested_seq: cursor.seq,
            };
        }
        let anchor_seq = document.anchor_seq();
        if cursor.seq < anchor_seq {
            return ReplayAfter::Expired {
                floor_seq: document.floor_seq,
                requested_seq: cursor.seq,
            };
        }
        let events: Vec<EventEnvelope> = document
            .journal
            .iter()
            .filter(|envelope| envelope.cursor.seq > cursor.seq)
            .cloned()
            .collect();
        if events.is_empty() {
            ReplayAfter::Empty
        } else {
            ReplayAfter::Success { events }
        }
    }

    /// Explicitly start a new epoch after an asserted continuity loss.
    ///
    /// This is the **only** way an epoch changes. It requires a nonempty,
    /// bounded reason and the caller-supplied replacement snapshot. Ordinary
    /// corruption never reaches this path automatically.
    pub fn begin_new_epoch_after_continuity_loss(
        root: &Path,
        snapshot: AuthoritativeSnapshot,
        reason: &str,
    ) -> Result<Self> {
        let reason = bounded_reason(reason)?;
        validate_snapshot(&snapshot)?;
        let _lock = ReplayLock::acquire(root)?;
        let next_epoch = match read_epoch_for_reset(root) {
            Some(epoch) if epoch >= 1 => epoch
                .checked_add(1)
                .ok_or_else(|| OcgError::config("replay epoch is exhausted"))?,
            _ => 1,
        };
        let document = ReplayDocument::genesis(next_epoch, snapshot, Some(reason))?;
        write_document(root, &document)?;
        write_marker(root, next_epoch)?;
        Ok(Self {
            root: root.to_path_buf(),
            retention: SnapshotConfig::default().retention,
        })
    }
}

/// On-disk replay document. Private: the only durable shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ReplayDocument {
    schema_version: u32,
    epoch: u64,
    head_seq: u64,
    /// First retained sequence. `0` means the journal is empty.
    floor_seq: u64,
    /// Hash of the last pruned envelope, or [`GENESIS_HASH`].
    anchor_hash: String,
    /// Explicit audit reason for a continuity-loss epoch change.
    #[serde(default)]
    epoch_reason: Option<String>,
    state_digest: String,
    snapshot: AuthoritativeSnapshot,
    journal: Vec<EventEnvelope>,
}

impl ReplayDocument {
    fn genesis(
        epoch: u64,
        snapshot: AuthoritativeSnapshot,
        epoch_reason: Option<String>,
    ) -> Result<Self> {
        let state_digest = snapshot_digest(&snapshot)?;
        Ok(Self {
            schema_version: REPLAY_SCHEMA_VERSION,
            epoch,
            head_seq: 0,
            floor_seq: 0,
            anchor_hash: GENESIS_HASH.to_string(),
            epoch_reason,
            state_digest,
            snapshot,
            journal: Vec::new(),
        })
    }

    fn head_cursor(&self) -> Cursor {
        Cursor {
            epoch: self.epoch,
            seq: self.head_seq,
        }
    }

    /// The last pruned sequence, or `0` when nothing was pruned.
    fn anchor_seq(&self) -> u64 {
        if self.floor_seq == 0 {
            0
        } else {
            self.floor_seq - 1
        }
    }
}

/// Record one normalized Mission update through the authority, before the
/// compatibility projection is written. Bootstrap-safe: it never calls the
/// projection's own save path, so it cannot recurse.
pub(crate) fn record_mission_update(root: &Path, mission: &Mission) -> Result<()> {
    let service = SnapshotService::open(root)?;
    service.append(DomainEvent::MissionUpsert {
        mission: mission.clone(),
    })?;
    Ok(())
}

/// Record one normalized Approval update through the authority.
pub(crate) fn record_approval_update(root: &Path, approval: &ApprovalRecord) -> Result<()> {
    let service = SnapshotService::open(root)?;
    service.append(DomainEvent::ApprovalUpsert {
        approval: approval.clone(),
    })?;
    Ok(())
}

/// Record the normalized durable Resource observations through the authority
/// as one atomic transition. The complete observation set replaces the
/// authoritative resource map, so removals and an emptied registry are
/// represented in the same durable write as additions.
pub(crate) fn record_resource_updates(
    root: &Path,
    registry: &resources::ResourceRegistry,
) -> Result<()> {
    let service = SnapshotService::open(root)?;
    service.append(DomainEvent::ResourceReplace {
        resources: registry.observations().clone(),
    })?;
    Ok(())
}

/// Whether the replay store has ever been initialized (its document or marker
/// exists). This is a pure path check: it never creates the store.
pub(crate) fn authority_initialized(root: &Path) -> Result<bool> {
    Ok(path_exists_strict(&state_path(root))? || path_exists_strict(&marker_path(root))?)
}

/// Read the authoritative snapshot if the replay store is initialized.
///
/// - `Ok(None)`: the authority was never initialized; callers may fall back to
///   the legacy projection for migration bootstrap.
/// - `Ok(Some(_))`: the validated authoritative snapshot.
/// - `Err(_)`: the authority is initialized but missing or corrupt. Callers
///   must fail closed; this never bootstraps and never mutates the store.
pub(crate) fn read_authoritative_snapshot(root: &Path) -> Result<Option<AuthoritativeSnapshot>> {
    if !authority_initialized(root)? {
        return Ok(None);
    }
    let _lock = ReplayLock::acquire(root)?;
    let document = read_document(root)?;
    Ok(Some(document.snapshot))
}

/// The replay store directory.
pub fn replay_dir(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join(REPLAY_DIR)
}

/// The authoritative document path.
pub fn state_path(root: &Path) -> PathBuf {
    replay_dir(root).join(REPLAY_FILE)
}

fn lock_path(root: &Path) -> PathBuf {
    replay_dir(root).join(REPLAY_LOCK_FILE)
}

fn marker_path(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join(REPLAY_MARKER_FILE)
}

/// The project-wide cross-process lock. The lock file is created once and
/// retained forever; only the advisory lock is released on drop.
struct ReplayLock {
    file: fs::File,
}

impl ReplayLock {
    fn acquire(root: &Path) -> Result<Self> {
        let path = lock_path(root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                OcgError::io(
                    format!("cannot create replay directory {}", parent.display()),
                    error,
                )
            })?;
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|error| {
                OcgError::io(format!("cannot open replay lock {}", path.display()), error)
            })?;
        FileExt::lock_exclusive(&file).map_err(|error| {
            OcgError::io(
                format!("cannot lock replay store {}", path.display()),
                error,
            )
        })?;
        Ok(Self { file })
    }
}

impl Drop for ReplayLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn ensure_initialized_locked(root: &Path) -> Result<()> {
    let state = state_path(root);
    if path_exists_strict(&state)? {
        // Validate eagerly so a corrupt store fails closed before any write.
        let _ = read_document(root)?;
        return Ok(());
    }
    if path_exists_strict(&marker_path(root))? {
        return Err(OcgError::config(format!(
            "replay authority {} is missing after its initialization marker; refusing to \
             bootstrap over lost durable state (use the explicit continuity-loss epoch change)",
            state.display()
        )));
    }
    let snapshot = bootstrap_snapshot(root)?;
    let document = ReplayDocument::genesis(1, snapshot, None)?;
    write_document(root, &document)?;
    write_marker(root, 1)?;
    Ok(())
}

/// Build the first snapshot strictly from existing durable domains. Any corrupt
/// or ambiguous input aborts the bootstrap: it never silently drops data.
fn bootstrap_snapshot(root: &Path) -> Result<AuthoritativeSnapshot> {
    let mut missions = BTreeMap::new();
    let dir = mission::missions_dir(root);
    match fs::read_dir(&dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(|error| {
                    OcgError::io(
                        format!("cannot read a mission directory entry in {}", dir.display()),
                        error,
                    )
                })?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".corrupt.json") {
                    return Err(OcgError::config(format!(
                        "cannot bootstrap replay authority: unresolved corrupt mission artifact {}",
                        entry.path().display()
                    )));
                }
                if !name.ends_with(".json") {
                    continue;
                }
                let Some(id) = name.strip_suffix(".json") else {
                    continue;
                };
                if let Some(value) = mission::load_raw(root, id)? {
                    missions.insert(value.mission_id.clone(), value);
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(OcgError::io(
                format!("cannot read the mission directory {}", dir.display()),
                error,
            ))
        }
    }

    let loaded_approvals = policy::list_approvals_raw(root)?;
    if !loaded_approvals.issues.is_empty() {
        return Err(OcgError::config(format!(
            "cannot bootstrap replay authority: {} approval record(s) are corrupt or ambiguous",
            loaded_approvals.issues.len()
        )));
    }
    let mut approvals = BTreeMap::new();
    for record in loaded_approvals.approvals {
        approvals.insert(record.approval_id.clone(), record);
    }
    enforce_approval_bound(&mut approvals);

    let loaded_resources = resources::load_raw(root)?;
    if loaded_resources.corrupt || !loaded_resources.issues.is_empty() {
        return Err(OcgError::config(format!(
            "cannot bootstrap replay authority: the resource registry is {} with {} issue(s)",
            if loaded_resources.corrupt {
                "corrupt"
            } else {
                "ambiguous"
            },
            loaded_resources.issues.len()
        )));
    }
    let resource_map = loaded_resources.registry.observations().clone();

    let snapshot = AuthoritativeSnapshot {
        missions,
        approvals,
        resources: resource_map,
        dispatches: BTreeMap::new(),
    };
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn append_to_document(
    document: &mut ReplayDocument,
    event: DomainEvent,
    retention: usize,
) -> Result<Option<Cursor>> {
    validate_event(&event)?;
    let mut candidate = document.snapshot.clone();
    if !candidate.apply(&event)? {
        return Ok(None);
    }
    let seq = document
        .head_seq
        .checked_add(1)
        .ok_or_else(|| OcgError::config("replay sequence is exhausted"))?;
    let cursor = Cursor {
        epoch: document.epoch,
        seq,
    };
    let prev_hash = match document.journal.last() {
        Some(previous) => previous.hash.clone(),
        None => document.anchor_hash.clone(),
    };
    let hash = envelope_hash(&cursor, &prev_hash, &event)?;
    document.journal.push(EventEnvelope {
        cursor,
        prev_hash,
        hash,
        event,
    });
    document.head_seq = seq;
    if document.journal.len() > retention {
        let excess = document.journal.len() - retention;
        document.journal.drain(0..excess);
    }
    document.floor_seq = document
        .journal
        .first()
        .map(|envelope| envelope.cursor.seq)
        .unwrap_or(0);
    document.anchor_hash = match document.journal.first() {
        Some(envelope) => envelope.prev_hash.clone(),
        None => GENESIS_HASH.to_string(),
    };
    document.snapshot = candidate;
    document.state_digest = snapshot_digest(&document.snapshot)?;
    validate_document(document)?;
    Ok(Some(cursor))
}

impl AuthoritativeSnapshot {
    /// Upsert one event into the snapshot. Returns whether anything changed,
    /// or an error when the event would roll an authority backward.
    fn apply(&mut self, event: &DomainEvent) -> Result<bool> {
        match event {
            DomainEvent::MissionUpsert { mission } => {
                if let Some(existing) = self.missions.get(&mission.mission_id) {
                    if existing == mission {
                        return Ok(false);
                    }
                    // A stale projection or caller must never roll authoritative
                    // progress backward: within a generation the revision must
                    // strictly advance, and a generation may only advance.
                    let rollback = mission.generation < existing.generation
                        || (mission.generation == existing.generation
                            && mission.revision <= existing.revision);
                    if rollback {
                        return Err(OcgError::config(format!(
                            "replay authority refuses a Mission upsert that rolls back {} \
                             (generation {} revision {} -> generation {} revision {})",
                            mission.mission_id,
                            existing.generation,
                            existing.revision,
                            mission.generation,
                            mission.revision
                        )));
                    }
                }
                self.missions
                    .insert(mission.mission_id.clone(), mission.clone());
                Ok(true)
            }
            DomainEvent::ApprovalUpsert { approval } => {
                if self.approvals.get(&approval.approval_id) == Some(approval) {
                    return Ok(false);
                }
                let before = self.approvals.clone();
                self.approvals
                    .insert(approval.approval_id.clone(), approval.clone());
                enforce_approval_bound(&mut self.approvals);
                Ok(self.approvals != before)
            }
            DomainEvent::ResourceUpsert { observation } => {
                let key = observation.resource_id.as_str().to_string();
                if self.resources.get(&key) == Some(observation) {
                    return Ok(false);
                }
                let before = self.resources.clone();
                self.resources.insert(key, observation.clone());
                enforce_resource_bound(&mut self.resources);
                Ok(self.resources != before)
            }
            DomainEvent::ResourceReplace { resources } => {
                if &self.resources == resources {
                    return Ok(false);
                }
                self.resources = resources.clone();
                enforce_resource_bound(&mut self.resources);
                Ok(true)
            }
            DomainEvent::DispatchUpsert { dispatch } => {
                if self.dispatches.get(dispatch.id.as_str()) == Some(dispatch) {
                    return Ok(false);
                }
                let mut candidate = self.dispatches.clone();
                candidate.insert(dispatch.id.as_str().to_string(), dispatch.clone());
                if candidate.len() > MAX_DISPATCHES {
                    let excess = candidate.len() - MAX_DISPATCHES;
                    let mut terminal: Vec<_> = candidate
                        .values()
                        .filter(|item| !item.state.is_live())
                        .map(|item| (item.updated_at, item.id.as_str().to_string()))
                        .collect();
                    terminal.sort();
                    if terminal.len() < excess {
                        return Err(OcgError::config("live dispatch retention bound reached"));
                    }
                    for (_, id) in terminal.into_iter().take(excess) {
                        candidate.remove(&id);
                    }
                }
                self.dispatches = candidate;
                Ok(true)
            }
        }
    }
}

/// Keep authoritative approvals bounded exactly like the domain projection:
/// only resolved records are pruned, oldest request first, and a pending record
/// is never dropped while a resolved one could be removed instead.
fn enforce_approval_bound(approvals: &mut BTreeMap<String, ApprovalRecord>) {
    if approvals.len() <= MAX_APPROVALS {
        return;
    }
    let mut resolved: Vec<(i64, String)> = approvals
        .values()
        .filter(|record| !record.status.is_pending())
        .map(|record| (record.requested_at, record.approval_id.clone()))
        .collect();
    resolved.sort();
    let mut excess = approvals.len() - MAX_APPROVALS;
    for (_, approval_id) in resolved {
        if excess == 0 {
            break;
        }
        approvals.remove(&approval_id);
        excess -= 1;
    }
}

/// Keep the authoritative resource map bounded exactly like the registry:
/// newest observation wins and older ids are dropped first.
fn enforce_resource_bound(resources: &mut BTreeMap<String, ResourceObservation>) {
    if resources.len() <= MAX_RESOURCES {
        return;
    }
    let mut entries: Vec<(i64, String)> = resources
        .values()
        .map(|observation| {
            (
                observation.updated_at,
                observation.resource_id.as_str().to_string(),
            )
        })
        .collect();
    entries.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    for (_, key) in entries.into_iter().skip(MAX_RESOURCES) {
        resources.remove(&key);
    }
}

fn read_document(root: &Path) -> Result<ReplayDocument> {
    let path = state_path(root);
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let document: ReplayDocument = serde_json::from_str(&text).map_err(|error| {
        OcgError::config(format!(
            "replay authority {} is not a valid document: {error}",
            path.display()
        ))
    })?;
    validate_document(&document)?;
    if let Some(marker_epoch) = read_marker_epoch(root)? {
        if marker_epoch != document.epoch {
            return Err(OcgError::config(format!(
                "replay authority document epoch {} disagrees with initialization marker epoch {}",
                document.epoch, marker_epoch
            )));
        }
    }
    Ok(document)
}

/// Parse the initialization marker's epoch. A missing marker is `Ok(None)`; a
/// present but malformed marker is corruption.
fn read_marker_epoch(root: &Path) -> Result<Option<u64>> {
    let path = marker_path(root);
    if !path_exists_strict(&path)? {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let lines: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    if lines.len() != 1 {
        return Err(OcgError::config(format!(
            "replay initialization marker {} is ambiguous",
            path.display()
        )));
    }
    let epoch = lines[0]
        .trim()
        .strip_prefix("epoch=")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|epoch| *epoch > 0)
        .ok_or_else(|| {
            OcgError::config(format!(
                "replay initialization marker {} does not carry one valid epoch",
                path.display()
            ))
        })?;
    Ok(Some(epoch))
}

fn path_exists_strict(path: &Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(OcgError::read(path, error)),
    }
}

fn validate_document(document: &ReplayDocument) -> Result<()> {
    if document.schema_version != REPLAY_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "replay authority has unsupported schema_version {} (expected {REPLAY_SCHEMA_VERSION})",
            document.schema_version
        )));
    }
    if document.epoch == 0 {
        return Err(OcgError::config(
            "replay authority has an invalid zero epoch",
        ));
    }
    if document.epoch > 1 {
        match document.epoch_reason.as_deref() {
            Some(reason) if !reason.trim().is_empty() && reason.len() <= MAX_EPOCH_REASON_BYTES => {
            }
            _ => {
                return Err(OcgError::config(
                    "replay authority changed epoch without a bounded continuity-loss reason",
                ))
            }
        }
    }
    if document.journal.len() > MAX_RETENTION {
        return Err(OcgError::config(format!(
            "replay authority retains {} journal entries, above the hard maximum {MAX_RETENTION}",
            document.journal.len()
        )));
    }
    if document.journal.is_empty() {
        if document.head_seq != 0 || document.floor_seq != 0 || document.anchor_hash != GENESIS_HASH
        {
            return Err(OcgError::config(
                "replay authority journal is empty but head/floor/anchor are inconsistent",
            ));
        }
    } else {
        if document.floor_seq == 0 {
            return Err(OcgError::config(
                "replay authority has events but a zero retention floor",
            ));
        }
        if document.floor_seq == 1 && document.anchor_hash != GENESIS_HASH {
            return Err(OcgError::config(
                "replay authority anchor does not match an unpruned genesis prefix",
            ));
        }
        if document.floor_seq > 1 && document.anchor_hash == GENESIS_HASH {
            return Err(OcgError::config(
                "replay authority lost its pruned-prefix hash anchor",
            ));
        }
        let mut expected_prev = document.anchor_hash.as_str();
        let mut expected_seq = document.floor_seq;
        for envelope in &document.journal {
            if envelope.cursor.epoch != document.epoch {
                return Err(OcgError::config(
                    "replay authority contains an event from a different epoch",
                ));
            }
            if envelope.cursor.seq != expected_seq || envelope.cursor.seq == 0 {
                return Err(OcgError::config(
                    "replay authority journal is not contiguous from its floor",
                ));
            }
            if envelope.prev_hash != expected_prev {
                return Err(OcgError::config("replay authority hash chain is broken"));
            }
            let recomputed = envelope_hash(&envelope.cursor, &envelope.prev_hash, &envelope.event)?;
            if recomputed != envelope.hash {
                return Err(OcgError::config(
                    "replay authority event hash does not match its contents",
                ));
            }
            validate_event(&envelope.event)?;
            expected_prev = envelope.hash.as_str();
            expected_seq = expected_seq
                .checked_add(1)
                .ok_or_else(|| OcgError::config("replay authority sequence overflowed"))?;
        }
        let expected_head = document
            .floor_seq
            .checked_add(document.journal.len() as u64)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| OcgError::config("replay authority head is inconsistent"))?;
        if document.head_seq != expected_head {
            return Err(OcgError::config(
                "replay authority head does not match its retained journal",
            ));
        }
    }
    if document.state_digest != snapshot_digest(&document.snapshot)? {
        return Err(OcgError::config(
            "replay authority state digest does not match its snapshot",
        ));
    }
    validate_snapshot(&document.snapshot)
}

fn validate_snapshot(snapshot: &AuthoritativeSnapshot) -> Result<()> {
    if snapshot.dispatches.len() > MAX_DISPATCHES {
        return Err(OcgError::config("dispatch retention bound exceeded"));
    }
    for (id, dispatch) in &snapshot.dispatches {
        dispatch.validate()?;
        if id != dispatch.id.as_str() {
            return Err(OcgError::config("dispatch key mismatch"));
        }
    }
    for (key, mission) in &snapshot.missions {
        if key != &mission.mission_id {
            return Err(OcgError::config(
                "replay authority mission key does not match its identity",
            ));
        }
        validate_mission_value(mission)?;
    }
    for (key, approval) in &snapshot.approvals {
        if key != &approval.approval_id {
            return Err(OcgError::config(
                "replay authority approval key does not match its identity",
            ));
        }
        validate_approval_value(approval)?;
    }
    // The authoritative resource map obeys the registry's own hard bound; a
    // larger map is corruption, not a silently truncated registry.
    if snapshot.resources.len() > MAX_RESOURCES {
        return Err(OcgError::config(format!(
            "replay authority retains {} resources, above the maximum {MAX_RESOURCES}",
            snapshot.resources.len()
        )));
    }
    for (key, observation) in &snapshot.resources {
        if key != observation.resource_id.as_str() {
            return Err(OcgError::config(
                "replay authority resource key does not match its identity",
            ));
        }
        validate_resource_value(observation)?;
    }
    Ok(())
}

fn validate_event(event: &DomainEvent) -> Result<()> {
    match event {
        DomainEvent::MissionUpsert { mission } => validate_mission_value(mission),
        DomainEvent::ApprovalUpsert { approval } => validate_approval_value(approval),
        DomainEvent::ResourceUpsert { observation } => validate_resource_value(observation),
        DomainEvent::ResourceReplace { resources } => {
            if resources.len() > MAX_RESOURCES {
                return Err(OcgError::config(format!(
                    "replay resource batch carries {} observations, above the maximum {MAX_RESOURCES}",
                    resources.len()
                )));
            }
            for (key, observation) in resources {
                if key != observation.resource_id.as_str() {
                    return Err(OcgError::config(
                        "replay resource batch key does not match its identity",
                    ));
                }
                validate_resource_value(observation)?;
            }
            Ok(())
        }
        DomainEvent::DispatchUpsert { dispatch } => dispatch.validate(),
    }
}

/// Reuse the domain's own validator instead of duplicating weaker checks.
fn validate_mission_value(mission: &Mission) -> Result<()> {
    mission::validate_mission(mission)
}

fn validate_approval_value(approval: &ApprovalRecord) -> Result<()> {
    policy::validate_approval(&approval.approval_id, approval)
}

fn validate_resource_value(observation: &ResourceObservation) -> Result<()> {
    let id = observation.resource_id.as_str();
    resources::validate_observation(id, observation)
        .map_err(|detail| OcgError::config(format!("replay resource is invalid: {detail}")))
}

fn bounded_reason(reason: &str) -> Result<String> {
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(OcgError::config(
            "a continuity-loss epoch change requires a nonempty reason",
        ));
    }
    if reason.len() > MAX_EPOCH_REASON_BYTES {
        return Err(OcgError::config(format!(
            "the continuity-loss reason must be at most {MAX_EPOCH_REASON_BYTES} bytes"
        )));
    }
    Ok(crate::telemetry::task::redact(reason))
}

fn snapshot_digest(snapshot: &AuthoritativeSnapshot) -> Result<String> {
    let bytes = serde_json::to_vec(snapshot).map_err(|error| {
        OcgError::config(format!(
            "cannot serialize the replay snapshot for its integrity digest: {error}"
        ))
    })?;
    Ok(crate::runtime::hash::sha256_hex(&bytes))
}

fn envelope_hash(cursor: &Cursor, prev_hash: &str, event: &DomainEvent) -> Result<String> {
    let event_bytes = serde_json::to_vec(event).map_err(|error| {
        OcgError::config(format!(
            "cannot serialize a replay event for its integrity hash: {error}"
        ))
    })?;
    let mut bytes = Vec::with_capacity(event_bytes.len() + 96);
    bytes.extend_from_slice(b"ocg-replay-v1|");
    bytes.extend_from_slice(cursor.epoch.to_string().as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(cursor.seq.to_string().as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(prev_hash.as_bytes());
    bytes.push(b'|');
    bytes.extend_from_slice(&event_bytes);
    Ok(crate::runtime::hash::sha256_hex(&bytes))
}

/// Read a best-effort prior epoch for an explicit continuity-loss reset. The
/// document may itself be corrupt, so this tolerates parse failures and falls
/// back to the marker; the caller applies a checked increment.
fn read_epoch_for_reset(root: &Path) -> Option<u64> {
    if let Ok(text) = fs::read_to_string(state_path(root)) {
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            if let Some(epoch) = value.get("epoch").and_then(Value::as_u64) {
                return Some(epoch);
            }
        }
    }
    read_marker_epoch(root).ok().flatten()
}

fn write_document(root: &Path, document: &ReplayDocument) -> Result<()> {
    crate::runtime::install::ensure_gitignore(root)?;
    let bytes = serde_json::to_vec(document).map_err(|error| {
        OcgError::config(format!("cannot serialize the replay authority: {error}"))
    })?;
    write_bytes_durable(&state_path(root), &bytes)
}

fn write_marker(root: &Path, epoch: u64) -> Result<()> {
    let path = marker_path(root);
    write_bytes_durable(&path, format!("epoch={epoch}\n").as_bytes())
}

/// Durable atomic write: a temp sibling is fsynced, renamed into place and the
/// parent directory is fsynced afterwards.
fn write_bytes_durable(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().ok_or_else(|| {
        OcgError::config(format!(
            "cannot determine a parent for {}",
            target.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        OcgError::io(
            format!("cannot create replay directory {}", parent.display()),
            error,
        )
    })?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".ocg-replay-")
        .tempfile_in(parent)
        .map_err(|error| {
            OcgError::io(
                format!(
                    "cannot create a temporary replay document in {}",
                    parent.display()
                ),
                error,
            )
        })?;
    temporary
        .write_all(bytes)
        .map_err(|error| OcgError::write(target, error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| OcgError::io(format!("cannot fsync {}", target.display()), error))?;
    temporary
        .persist(target)
        .map_err(|error| OcgError::write(target, error.error))?;
    sync_parent(parent)
}

fn sync_parent(parent: &Path) -> Result<()> {
    match fs::File::open(parent) {
        Ok(directory) => directory
            .sync_all()
            .map_err(|error| OcgError::io(format!("cannot fsync {}", parent.display()), error)),
        // A directory cannot be opened on every platform; a missing parent is
        // impossible immediately after a successful rename.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(OcgError::io(
            format!("cannot open {} for fsync", parent.display()),
            error,
        )),
    }
}

/// A test-only observer of the durable journal length, used to assert the
/// retention bound without exposing the document.
#[cfg(test)]
fn retained_len(root: &Path) -> usize {
    read_document(root)
        .map(|document| document.journal.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt(id: &str, execution: &str) -> DispatchRecord {
        DispatchRecord {
            id: DispatchId::new(id.to_string()).unwrap(),
            mission_id: "task-0000000000000001".to_string(),
            generation: 1,
            logical_operation: "turn-1".to_string(),
            execution_id: execution.to_string(),
            root_id: "session-1".to_string(),
            provider: "fixture".to_string(),
            model: "fixture-model".to_string(),
            reservation_id: None,
            state: DispatchState::Reserved,
            created_at: 2,
            updated_at: 2,
            usage: None,
            cost_provenance: "unknown".to_string(),
            failure_class: None,
        }
    }

    #[test]
    fn uncapped_dispatch_attempts_share_one_durable_mission() {
        let dir = tempfile::tempdir().unwrap();
        let service = SnapshotService::open(dir.path()).unwrap();
        service
            .append(DomainEvent::MissionUpsert {
                mission: mission(1, 1),
            })
            .unwrap();
        let config = BudgetConfig::default();
        let (allowed, root) = service
            .reserve_dispatch(
                attempt("dsp-root-1", "session-1"),
                &config,
                QuotaFacts::unknown(),
            )
            .unwrap();
        assert!(allowed.is_allowed());
        let root = service.start_dispatch(&root.id, 3).unwrap();
        assert!(
            service.start_dispatch(&root.id, 3).is_err(),
            "one id cannot send twice"
        );
        let (allowed_worker, _) = service
            .reserve_dispatch(
                attempt("dsp-worker-1", "worker-1"),
                &config,
                QuotaFacts::unknown(),
            )
            .unwrap();
        assert!(allowed_worker.is_allowed());
        assert_eq!(service.snapshot().unwrap().dispatches.len(), 2);
        assert!(service
            .finish_dispatch(
                &root.id,
                DispatchState::Unresolved,
                None,
                Some("midstream_disconnect"),
                4
            )
            .unwrap());
        assert!(!service
            .finish_dispatch(&root.id, DispatchState::Unresolved, None, None, 4)
            .unwrap());
    }

    #[test]
    fn unknown_required_cost_blocks_before_dispatch_record() {
        let dir = tempfile::tempdir().unwrap();
        let service = SnapshotService::open(dir.path()).unwrap();
        service
            .append(DomainEvent::MissionUpsert {
                mission: mission(1, 1),
            })
            .unwrap();
        let config = BudgetConfig {
            currency: Some("USD".into()),
            hard_limit_micros: Some(100),
            ..BudgetConfig::default()
        };
        let (blocked, _) = service
            .reserve_dispatch(
                attempt("dsp-unknown-1", "worker-1"),
                &config,
                QuotaFacts::unknown(),
            )
            .unwrap();
        assert!(!blocked.is_allowed());
        assert!(service.snapshot().unwrap().dispatches.is_empty());
    }

    fn mission(index: u64, now: i64) -> Mission {
        Mission::admit(&format!("task-{index:016}"), "task", "session-1", now)
    }

    #[test]
    fn append_advances_cursor_and_noop_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let service = SnapshotService::open(dir.path()).unwrap();
        let first = service
            .append(DomainEvent::MissionUpsert {
                mission: mission(1, 1),
            })
            .unwrap()
            .unwrap();
        assert_eq!(first, Cursor { epoch: 1, seq: 1 });
        let again = service
            .append(DomainEvent::MissionUpsert {
                mission: mission(1, 1),
            })
            .unwrap();
        assert_eq!(again, None, "a same-state upsert must not create an event");
        let (snapshot, cursor) = service.snapshot_with_cursor().unwrap();
        assert_eq!(cursor, first);
        assert_eq!(snapshot.missions.len(), 1);
    }

    #[test]
    fn corrupt_hash_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let service = SnapshotService::open(dir.path()).unwrap();
        service
            .append(DomainEvent::MissionUpsert {
                mission: mission(1, 1),
            })
            .unwrap();
        let path = state_path(dir.path());
        let mut value: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        value["journal"][0]["hash"] = Value::String("deadbeef".to_string());
        fs::write(&path, value.to_string()).unwrap();
        assert!(SnapshotService::open(dir.path()).is_err());
        assert!(matches!(
            service.replay_after(Cursor { epoch: 1, seq: 0 }),
            ReplayAfter::PersistenceFailure { .. }
        ));
    }

    #[test]
    fn bounded_retention_prunes_the_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let service =
            SnapshotService::open_with_config(dir.path(), SnapshotConfig::new(3)).unwrap();
        for index in 1..=6 {
            service
                .append(DomainEvent::MissionUpsert {
                    mission: mission(index, index as i64),
                })
                .unwrap();
        }
        assert_eq!(retained_len(dir.path()), 3);
        let head = service.head().unwrap();
        assert_eq!(head, Cursor { epoch: 1, seq: 6 });
        assert!(matches!(
            service.replay_after(Cursor { epoch: 1, seq: 0 }),
            ReplayAfter::Expired { .. }
        ));
        let ReplayAfter::Success { events } = service.replay_after(Cursor { epoch: 1, seq: 3 })
        else {
            panic!("expected the retained suffix");
        };
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].cursor.seq, 4);
        assert_eq!(events[2].cursor.seq, 6);
    }
}
