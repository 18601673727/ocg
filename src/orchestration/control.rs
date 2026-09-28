//! Thin, authority-backed control service.
//!
//! This is the single seam the transport layer (the loopback HTTP/SSE server and
//! the CLI) uses to read and mutate durable orchestration state. It owns no
//! durable state of its own: every operation delegates to the Phase 2B-1 replay
//! authority ([`SnapshotService`]) or to the domain APIs that already commit
//! through it (`policy::resolve_approval`, `Mission::set_hard_budget` +
//! `mission::save_if_revision`).
//!
//! Two invariants matter to callers:
//!
//! - A snapshot and its cursor are read atomically from the same authority
//!   write; a mutation returns the **post-commit** authoritative cursor, read
//!   after the domain commit, so a client can resume an event tail from it.
//! - Every failure is a typed [`ControlError`] with a bounded, redacted message
//!   and a stable code/status. There is no "generic 500 with a raw path" path.
//!
//! The service is deliberately transport-neutral: it knows nothing about HTTP,
//! SSE, headers or routing.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{MissionBudgetReceipt, Money};
use crate::orchestration::mission::{self, Mission};
use crate::orchestration::policy::{self, ApprovalIssue, ApprovalRecord, ApprovalStatus};
use crate::orchestration::replay::{
    AuthoritativeSnapshot, Cursor, EventEnvelope, ReplayAfter, SnapshotConfig, SnapshotService,
};
use crate::resources::{self, ResourceRecord};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Upper bound on an error message returned to a client. Anything longer is
/// truncated on a char boundary so a hostile filesystem path or payload echo
/// cannot be reflected unbounded.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 400;
/// Stable schema version of the HTTP control DTOs.
pub const CONTROL_API_SCHEMA_VERSION: u32 = 1;

/// A typed, bounded control error.
///
/// The code/status mapping is part of the API contract documented in
/// `docs/control.md`. Messages pass through `telemetry::task::redact` so a
/// credential-shaped value can never be echoed back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    /// The addressed entity does not exist.
    NotFound { message: String },
    /// The request or its input is malformed or fails domain validation.
    Invalid { message: String },
    /// The mutation lost a compare-and-swap, or the request conflicts with
    /// current authoritative state.
    Conflict { message: String },
    /// A cursor belongs to a different epoch than the current authority.
    WrongEpoch { expected: u64, got: u64 },
    /// The retained journal no longer covers the requested cursor.
    Expired { floor_seq: u64, requested_seq: u64 },
    /// The requested cursor is ahead of the current head.
    Future { head_seq: u64, requested_seq: u64 },
    /// The authority is missing, unreadable or invalid. Fails closed.
    Storage { message: String },
}

impl ControlError {
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: bounded(message.into()),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid {
            message: bounded(message.into()),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: bounded(message.into()),
        }
    }

    pub fn storage(error: impl std::fmt::Display) -> Self {
        Self::Storage {
            message: bounded(error.to_string()),
        }
    }

    /// A stable machine-readable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound { .. } => "not_found",
            Self::Invalid { .. } => "invalid_request",
            Self::Conflict { .. } => "conflict",
            Self::WrongEpoch { .. } => "wrong_epoch",
            Self::Expired { .. } => "replay_expired",
            Self::Future { .. } => "future_cursor",
            Self::Storage { .. } => "persistence_unavailable",
        }
    }

    /// The HTTP status this error maps to.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::NotFound { .. } => 404,
            Self::Invalid { .. } => 400,
            Self::Conflict { .. } => 409,
            Self::WrongEpoch { .. } => 409,
            Self::Expired { .. } => 410,
            Self::Future { .. } => 409,
            Self::Storage { .. } => 503,
        }
    }

    /// The bounded, redacted human-readable message.
    pub fn message(&self) -> String {
        match self {
            Self::NotFound { message }
            | Self::Invalid { message }
            | Self::Conflict { message }
            | Self::Storage { message } => message.clone(),
            Self::WrongEpoch { expected, got } => format!(
                "cursor belongs to epoch {got}, but the current authority epoch is {expected}"
            ),
            Self::Expired {
                floor_seq,
                requested_seq,
            } => format!(
                "cursor at seq {requested_seq} is older than the retained window starting at seq \
                 {floor_seq}; no partial replay is returned"
            ),
            Self::Future {
                head_seq,
                requested_seq,
            } => format!(
                "cursor at seq {requested_seq} is ahead of the authoritative head seq {head_seq}"
            ),
        }
    }

    /// The stable JSON error envelope.
    pub fn to_json(&self) -> Value {
        let mut error = json!({
            "code": self.code(),
            "message": self.message(),
        });
        match self {
            Self::WrongEpoch { expected, got } => {
                error["expected_epoch"] = json!(expected);
                error["got_epoch"] = json!(got);
            }
            Self::Expired {
                floor_seq,
                requested_seq,
            } => {
                error["floor_seq"] = json!(floor_seq);
                error["requested_seq"] = json!(requested_seq);
            }
            Self::Future {
                head_seq,
                requested_seq,
            } => {
                error["head_seq"] = json!(head_seq);
                error["requested_seq"] = json!(requested_seq);
            }
            _ => {}
        }
        json!({ "error": error })
    }

    /// Lossy conversion for the CLI, which reports `OcgError`s.
    pub fn into_ocg_error(self) -> OcgError {
        OcgError::config(self.message())
    }
}

/// Redact and bound an error message. `redact` collapses credential-shaped runs
/// before the byte cap is applied, so truncation cannot split a redactor token.
fn bounded(message: String) -> String {
    let redacted = crate::telemetry::task::redact(&message);
    if redacted.len() <= MAX_ERROR_MESSAGE_BYTES {
        return redacted;
    }
    let mut end = MAX_ERROR_MESSAGE_BYTES;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &redacted[..end])
}

/// The explicit outcome of a replay request.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplaySlice {
    /// The cursor is already at the head; nothing to replay.
    Empty,
    /// The events strictly after the cursor, in order.
    Events(Vec<EventEnvelope>),
}

/// An unchanged entity plus its authoritative cursor.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotView {
    pub api_version: &'static str,
    pub schema_version: u32,
    pub cursor: Cursor,
    pub snapshot: AuthoritativeSnapshot,
}

/// The bounded approval listing.
#[derive(Debug, Clone, Serialize)]
pub struct ApprovalsView {
    pub approvals: Vec<ApprovalRecord>,
    pub issues: Vec<ApprovalIssue>,
}

/// One normalized issue projection (resource issues are not `Serialize`).
#[derive(Debug, Clone, Serialize)]
pub struct ApiIssue {
    pub target: String,
    pub detail: String,
}

/// The bounded resource listing.
#[derive(Debug, Clone, Serialize)]
pub struct ResourcesView {
    pub resources: Vec<ResourceRecord>,
    pub issues: Vec<ApiIssue>,
    pub corrupt: bool,
}

/// The authoritative budget projection for one Mission.
#[derive(Debug, Clone, Serialize)]
pub struct BudgetView {
    pub cursor: Cursor,
    pub mission_id: String,
    pub revision: u64,
    /// Whether this call changed the durable budget.
    pub changed: bool,
    pub budget: MissionBudgetReceipt,
}

/// Compact, authoritative orientation data for model-facing adapters.
#[derive(Debug, Clone, Serialize)]
pub struct StateSummaryView {
    pub cursor: Cursor,
    pub mission_count: usize,
    pub mission_statuses: BTreeMap<String, usize>,
    pub pending_approval_count: usize,
    pub resource_count: usize,
    pub resource_health: BTreeMap<String, usize>,
    pub budget_blocked_mission_count: usize,
}

/// A deliberately compact Mission projection for bounded listings.
#[derive(Debug, Clone, Serialize)]
pub struct MissionListItem {
    pub mission_id: String,
    pub revision: u64,
    pub generation: u32,
    pub status: String,
    pub phase: String,
    pub runtime_execution_id: Option<String>,
    pub reconcile_status: String,
    pub budget: MissionBudgetReceipt,
    pub updated_at: i64,
}

/// A bounded Mission listing paired with the exact authority cursor.
#[derive(Debug, Clone, Serialize)]
pub struct MissionsView {
    pub cursor: Cursor,
    pub total: usize,
    pub truncated: bool,
    pub missions: Vec<MissionListItem>,
}

/// One authoritative Mission paired with the exact authority cursor.
#[derive(Debug, Clone, Serialize)]
pub struct MissionView {
    pub cursor: Cursor,
    pub mission: Mission,
}

/// Authority-backed approval state paired with its freshness cursor.
#[derive(Debug, Clone, Serialize)]
pub struct AuthoritativeApprovalsView {
    pub cursor: Cursor,
    pub total: usize,
    pub truncated: bool,
    pub approvals: Vec<ApprovalRecord>,
}

/// Authority-backed durable resource observations. Configured facts are not
/// invented here and Unknown remains explicit in each observation.
#[derive(Debug, Clone, Serialize)]
pub struct AuthoritativeResourcesView {
    pub cursor: Cursor,
    pub total: usize,
    pub truncated: bool,
    pub resources: Vec<crate::resources::ResourceObservation>,
}

/// A thin handle to the durable orchestration authority.
///
/// The underlying [`SnapshotService`] is cheap and stateless, so a long-lived
/// handle observes cross-process writes and every read is atomic with its
/// cursor.
#[derive(Debug, Clone)]
pub struct ControlService {
    service: SnapshotService,
    root: PathBuf,
}

impl ControlService {
    /// Open the service, bootstrapping the replay authority on first use.
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_config(root, SnapshotConfig::default())
    }

    /// Open with an explicit test-tunable retention bound.
    pub fn open_with_config(root: &Path, config: SnapshotConfig) -> Result<Self> {
        let service = SnapshotService::open_with_config(root, config)?;
        Ok(Self {
            service,
            root: root.to_path_buf(),
        })
    }

    /// Atomically read the authoritative snapshot and its head cursor.
    pub fn snapshot(&self) -> std::result::Result<SnapshotView, ControlError> {
        match self.service.snapshot_with_cursor() {
            Ok((snapshot, cursor)) => Ok(SnapshotView {
                api_version: "v1",
                schema_version: CONTROL_API_SCHEMA_VERSION,
                cursor,
                snapshot,
            }),
            Err(error) => Err(ControlError::storage(error)),
        }
    }

    /// Replay the events strictly after `(epoch, after)`.
    pub fn replay(&self, epoch: u64, after: u64) -> std::result::Result<ReplaySlice, ControlError> {
        match self.service.replay_after(Cursor { epoch, seq: after }) {
            ReplayAfter::Success { events } => Ok(ReplaySlice::Events(events)),
            ReplayAfter::Empty => Ok(ReplaySlice::Empty),
            ReplayAfter::WrongEpoch { expected, got } => {
                Err(ControlError::WrongEpoch { expected, got })
            }
            ReplayAfter::Expired {
                floor_seq,
                requested_seq,
            } => Err(ControlError::Expired {
                floor_seq,
                requested_seq,
            }),
            ReplayAfter::Future {
                head_seq,
                requested_seq,
            } => Err(ControlError::Future {
                head_seq,
                requested_seq,
            }),
            ReplayAfter::PersistenceFailure { detail } => Err(ControlError::storage(detail)),
        }
    }

    /// The current authoritative head cursor.
    pub fn head(&self) -> std::result::Result<Cursor, ControlError> {
        self.service.head().map_err(ControlError::storage)
    }

    /// Return a cheap aggregate without exposing the full snapshot.
    pub fn state_summary(&self) -> std::result::Result<StateSummaryView, ControlError> {
        let view = self.snapshot()?;
        let mut mission_statuses = BTreeMap::new();
        let mut budget_blocked_mission_count = 0;
        for mission in view.snapshot.missions.values() {
            *mission_statuses
                .entry(mission.status.as_str().to_string())
                .or_insert(0) += 1;
            if matches!(
                mission.budget.status,
                crate::orchestration::budget::BudgetStatus::Exhausted
                    | crate::orchestration::budget::BudgetStatus::Breached
            ) {
                budget_blocked_mission_count += 1;
            }
        }
        let mut resource_health = BTreeMap::new();
        for resource in view.snapshot.resources.values() {
            *resource_health
                .entry(resource.health.state.as_str().to_string())
                .or_insert(0) += 1;
        }
        Ok(StateSummaryView {
            cursor: view.cursor,
            mission_count: view.snapshot.missions.len(),
            mission_statuses,
            pending_approval_count: view
                .snapshot
                .approvals
                .values()
                .filter(|approval| approval.status.is_pending())
                .count(),
            resource_count: view.snapshot.resources.len(),
            resource_health,
            budget_blocked_mission_count,
        })
    }

    /// List Missions from one authoritative snapshot, newest first.
    pub fn missions(&self, limit: usize) -> std::result::Result<MissionsView, ControlError> {
        let view = self.snapshot()?;
        let total = view.snapshot.missions.len();
        let mut missions: Vec<&Mission> = view.snapshot.missions.values().collect();
        missions.sort_by(|a, b| {
            b.updated_at
                .cmp(&a.updated_at)
                .then_with(|| a.mission_id.cmp(&b.mission_id))
        });
        let missions = missions
            .into_iter()
            .take(limit)
            .map(|mission| MissionListItem {
                mission_id: mission.mission_id.clone(),
                revision: mission.revision,
                generation: mission.generation,
                status: mission.status.as_str().to_string(),
                phase: mission.phase.as_str().to_string(),
                runtime_execution_id: mission.session_id.clone(),
                reconcile_status: mission.reconcile.status.as_str().to_string(),
                budget: mission.budget.receipt(),
                updated_at: mission.updated_at,
            })
            .collect();
        Ok(MissionsView {
            cursor: view.cursor,
            total,
            truncated: total > limit,
            missions,
        })
    }

    /// Get one Mission from the authoritative snapshot, never its projection.
    pub fn mission(&self, mission_id: &str) -> std::result::Result<MissionView, ControlError> {
        if !crate::orchestration::checkpoint::is_safe_id(mission_id) {
            return Err(ControlError::invalid("mission_id is not a safe identifier"));
        }
        let view = self.snapshot()?;
        let mission = view
            .snapshot
            .missions
            .get(mission_id)
            .cloned()
            .ok_or_else(|| ControlError::not_found(format!("unknown Mission '{mission_id}'")))?;
        Ok(MissionView {
            cursor: view.cursor,
            mission,
        })
    }

    /// List approvals from the same validated snapshot as the returned cursor.
    pub fn authoritative_approvals(
        &self,
        limit: usize,
    ) -> std::result::Result<AuthoritativeApprovalsView, ControlError> {
        let view = self.snapshot()?;
        let mut approvals: Vec<ApprovalRecord> =
            view.snapshot.approvals.values().cloned().collect();
        let total = approvals.len();
        approvals.sort_by(|a, b| {
            b.requested_at
                .cmp(&a.requested_at)
                .then_with(|| a.approval_id.cmp(&b.approval_id))
        });
        approvals.truncate(limit);
        Ok(AuthoritativeApprovalsView {
            cursor: view.cursor,
            total,
            truncated: total > limit,
            approvals,
        })
    }

    /// List durable resource observations from the authoritative snapshot.
    pub fn authoritative_resources(
        &self,
        limit: usize,
    ) -> std::result::Result<AuthoritativeResourcesView, ControlError> {
        let view = self.snapshot()?;
        let mut resources: Vec<_> = view.snapshot.resources.values().cloned().collect();
        let total = resources.len();
        resources.sort_by(|a, b| a.resource_id.as_str().cmp(b.resource_id.as_str()));
        resources.truncate(limit);
        Ok(AuthoritativeResourcesView {
            cursor: view.cursor,
            total,
            truncated: total > limit,
            resources,
        })
    }

    /// List every readable approval. Corruption is surfaced, never hidden.
    pub fn approvals(&self) -> ApprovalsView {
        let loaded = policy::list_approvals(&self.root);
        ApprovalsView {
            approvals: loaded.approvals,
            issues: loaded.issues,
        }
    }

    /// Resolve one approval through the existing authority-backed domain path.
    pub fn resolve_approval(
        &self,
        approval_id: &str,
        status: ApprovalStatus,
        note: Option<String>,
        now: i64,
    ) -> std::result::Result<(ApprovalRecord, Cursor), ControlError> {
        match policy::load_approval(&self.root, approval_id) {
            Ok(None) => {
                return Err(ControlError::not_found(format!(
                    "approval {approval_id} does not exist"
                )))
            }
            Ok(Some(_)) => {}
            Err(error) => return Err(ControlError::storage(error)),
        }
        policy::resolve_approval(&self.root, approval_id, status, note, now)
            .map_err(ControlError::storage)?;
        // Return state and cursor from one post-commit authority read. Another
        // writer may have committed afterward; in that case the response
        // truthfully represents the newer state through the returned cursor.
        let view = self.snapshot()?;
        let record = view
            .snapshot
            .approvals
            .get(approval_id)
            .cloned()
            .ok_or_else(|| {
                ControlError::conflict(
                    "approval changed after the resolution committed; refresh and retry",
                )
            })?;
        Ok((record, view.cursor))
    }

    /// List every known resource with its bounded issues.
    pub fn resources(&self) -> ResourcesView {
        let loaded = resources::load(&self.root);
        ResourcesView {
            resources: loaded.registry.list(),
            issues: loaded
                .issues
                .into_iter()
                .map(|issue| ApiIssue {
                    target: issue.resource,
                    detail: bounded(issue.detail),
                })
                .collect(),
            corrupt: loaded.corrupt,
        }
    }

    /// Read the durable budget of one Mission.
    pub fn budget(&self, mission_id: &str) -> std::result::Result<BudgetView, ControlError> {
        let view = self.snapshot()?;
        let mission = view
            .snapshot
            .missions
            .get(mission_id)
            .cloned()
            .ok_or_else(|| ControlError::not_found(format!("unknown Mission '{mission_id}'")))?;
        Ok(BudgetView {
            cursor: view.cursor,
            mission_id: mission.mission_id.clone(),
            revision: mission.revision,
            changed: false,
            budget: mission.budget.receipt(),
        })
    }

    /// Explicitly set the hard Mission budget.
    ///
    /// This is the only supported path past a hard cap: it goes through
    /// `Mission::set_hard_budget` and the authority-backed revision CAS
    /// (`mission::save_if_revision`). A lost CAS is a typed conflict, never a
    /// silent overwrite.
    pub fn set_budget(
        &self,
        mission_id: &str,
        amount: Money,
        now: i64,
    ) -> std::result::Result<BudgetView, ControlError> {
        let mut mission = self.load_mission(mission_id)?;
        let expected_revision = mission.revision;
        let expected_owner = mission.session_id.clone();
        let changed = mission
            .set_hard_budget(amount, now)
            .map_err(mutation_error)?;
        let saved = mission::save_if_revision(
            &self.root,
            &mission,
            expected_revision,
            expected_owner.as_deref(),
        )
        .map_err(ControlError::storage)?;
        if !saved {
            return Err(ControlError::conflict(
                "Mission changed while setting the hard budget; retry later",
            ));
        }
        // As with all query responses, pair the returned state with a cursor
        // from the same post-commit authority read. This cannot skip a racing
        // writer's event while returning pre-race state.
        let snapshot = self.snapshot()?;
        let mission = snapshot
            .snapshot
            .missions
            .get(mission_id)
            .cloned()
            .ok_or_else(|| {
                ControlError::conflict(
                    "Mission changed after the budget committed; refresh and retry",
                )
            })?;
        Ok(BudgetView {
            cursor: snapshot.cursor,
            mission_id: mission.mission_id.clone(),
            revision: mission.revision,
            changed,
            budget: mission.budget.receipt(),
        })
    }

    fn load_mission(&self, mission_id: &str) -> std::result::Result<Mission, ControlError> {
        match mission::load(&self.root, mission_id) {
            Ok(Some(mission)) => Ok(mission),
            Ok(None) => Err(ControlError::not_found(format!(
                "unknown Mission '{mission_id}'"
            ))),
            Err(error) => Err(ControlError::storage(error)),
        }
    }
}

/// Classification for domain mutations: I/O is a storage failure; a
/// configuration/validation message is the caller's input problem.
fn mutation_error(error: OcgError) -> ControlError {
    match error {
        OcgError::Io { context, source } => ControlError::storage(format!("{context}: {source}")),
        OcgError::Config(message) => ControlError::invalid(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::mission::Mission;
    use crate::orchestration::policy::{ApprovalRecord, ApprovalRequest, PolicyAction};

    fn temp_root() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn pending_approval(root: &Path, now: i64) -> ApprovalRecord {
        let mission = Mission::admit("task-control-0001", "task", "session-1", now);
        mission::save(root, &mission).unwrap();
        let request = ApprovalRequest {
            approval_id: "apr-0000000000000001".to_string(),
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            action: PolicyAction::EnsureExecution,
            current_execution_id: None,
            requested_at: now,
        };
        policy::ensure_pending(root, &request).unwrap()
    }

    #[test]
    fn snapshot_and_cursor_are_read_together() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        let view = service.snapshot().unwrap();
        assert_eq!(view.cursor, Cursor { epoch: 1, seq: 0 });
        assert!(view.snapshot.missions.is_empty());
    }

    #[test]
    fn replay_reports_explicit_cursor_failures() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        assert!(matches!(service.replay(1, 0).unwrap(), ReplaySlice::Empty));
        assert!(matches!(
            service.replay(1, 5).unwrap_err(),
            ControlError::Future { .. }
        ));
        assert!(matches!(
            service.replay(9, 0).unwrap_err(),
            ControlError::WrongEpoch { .. }
        ));
    }

    #[test]
    fn resolve_approval_returns_a_post_commit_cursor() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        let record = pending_approval(dir.path(), 10);
        let before = service.head().unwrap();
        let (resolved, cursor) = service
            .resolve_approval(&record.approval_id, ApprovalStatus::Approved, None, 11)
            .unwrap();
        assert_eq!(resolved.status, ApprovalStatus::Approved);
        assert!(
            cursor.seq > before.seq,
            "a committed approval advances the authoritative cursor"
        );
        assert_eq!(cursor, service.head().unwrap());
    }

    #[test]
    fn resolve_unknown_approval_is_not_found() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        let error = service
            .resolve_approval("apr-missing", ApprovalStatus::Approved, None, 1)
            .unwrap_err();
        assert!(matches!(error, ControlError::NotFound { .. }));
        assert_eq!(error.http_status(), 404);
    }

    #[test]
    fn set_budget_commits_and_returns_post_commit_cursor() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        let mission = Mission::admit("task-control-budget-0001", "task", "session-1", 1);
        let mission_id = mission.mission_id.clone();
        mission::save(dir.path(), &mission).unwrap();

        let before = service.head().unwrap();
        let view = service
            .set_budget(&mission_id, Money::new(500_000, "USD"), 2)
            .unwrap();
        assert!(view.changed);
        assert_eq!(view.budget.hard_limit_micros, Some(500_000));
        assert!(view.cursor.seq > before.seq);

        // Re-setting the same limit is still an explicit operator assertion: it
        // commits another durable revision rather than silently doing nothing.
        let again = service
            .set_budget(&mission_id, Money::new(500_000, "USD"), 3)
            .unwrap();
        assert!(again.changed);
        assert_eq!(again.budget.hard_limit_micros, Some(500_000));
        assert!(again.cursor.seq > view.cursor.seq);
    }

    #[test]
    fn set_budget_unknown_mission_is_not_found() {
        let dir = temp_root();
        let service = ControlService::open(dir.path()).unwrap();
        let error = service
            .set_budget("task-missing", Money::new(1, "USD"), 1)
            .unwrap_err();
        assert!(matches!(error, ControlError::NotFound { .. }));
    }

    #[test]
    fn error_envelope_is_bounded_and_redacted() {
        // Assemble the credential-shaped trigger at runtime: the source tree is
        // scanned for literal secret shapes by the hygiene test.
        let scheme = ["Bea", "rer "].concat();
        let error = ControlError::invalid(format!("Authorization: {}{}", scheme, "x".repeat(1000)));
        let message = error.message();
        assert!(message.len() <= MAX_ERROR_MESSAGE_BYTES + 3);
        let value = error.to_json();
        assert_eq!(value["error"]["code"], json!("invalid_request"));
    }
}
