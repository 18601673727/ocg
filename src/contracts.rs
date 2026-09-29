//! The wire contract for the loopback control surface — the single source of
//! truth shared by the Rust server and the TypeScript PWA.
//!
//! Every type here is a real, serialized struct. Nothing on the HTTP boundary
//! is built from an anonymous `json!` literal, so the TypeScript generated from
//! these definitions describes the actual bytes rather than a hand-maintained
//! guess.
//!
//! The generated TypeScript lives in
//! `frontend/components/ocg/contracts/generated.ts` and is produced by
//! `cargo run --bin ocg-rs-ts`. It is committed, reviewed like any other
//! artifact, and `make contracts-check` fails if it drifts from these
//! definitions.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use ts_rs::{Config, TS};

pub use crate::fingerprint::{
    ChangeSetFingerprintInputV1, CommandFingerprintInputV1, SpawnChildSpecV1,
    SpawnFingerprintInputV1,
};

pub use crate::core_contract::{
    event_registry, ActivityCursor, Actor, Approval, ApprovalDecision, ApprovalState, BlockerKind,
    BudgetScope, BudgetSnapshot, CallLifecycle, CanonicalEntityProjection, CanonicalSyncProjection,
    CapabilityConstraints, CapabilityGrant, CapabilityRef, CapabilityRevocation,
    CapabilityRevocationState, ChildCancellationPolicy, ChildFailurePolicy, ChildJoinPolicy,
    ChildPolicy, ClientKind, Command, CommandState, Consistency, DerivedMetric, EffectIntent,
    EffectIntentState, EntityId, EntityKind, EntityRef, EventEnvelope, EventPayload,
    EventRegistryEntry, EventType, ExecutionGraphEdge, ExecutionGraphNode,
    ExecutionGraphProjection, ExecutionLeaseRecord, ExecutionLimits, ExecutionMode,
    ExecutionPolicy, ExecutionRetryPolicy, ExecutorContract, ExternalSourceKind, Fact, Failure,
    FailureClass, LeaseState, MeasurementQuality, MeasurementView, Message, MessageBlock,
    MessageBlockKind, MessageLifecycle, ProjectScope, ProjectionEffect, ProjectionEffectPolicy,
    ResumeCursor, RunLifecycle, SideEffectMode, Snapshot, SpawnChildRequest, SyncWindowPolicy,
    TransientMessageDelta, WorkNodeBlocker, WorkNodeLifecycle, CORE_CONTRACT_VERSION,
    CURSOR_SCHEMA_VERSION, SNAPSHOT_SCHEMA_VERSION,
};
pub use crate::orchestration::canonical_control::{
    CanonicalConfigurationResponse, CanonicalDashboardResponse, CanonicalMissionResponse,
    CanonicalProjectResponse, CanonicalWorkEvent, CanonicalWorkSnapshot, GlobalConfiguration,
    ProjectConfiguration, ProjectConfigurationView, ProjectRecord, ResourceBudget,
    CANONICAL_CONTROL_API_VERSION,
};
/// The authority record. It travels inside a canonical event payload, so the
/// PWA reads it out of an opaque JSON value and must not be able to drift from
/// the definition that actually confers authority.
pub use crate::orchestration::domain::ExecutionWitness;
pub use crate::profile::{
    Candidate, Model, Origin, Profile, Provider, PROVIDER_PROFILE_API_VERSION,
};

/// The Profile control API version. Shared with the generated TypeScript, so a
/// version bump is a compile-time mismatch rather than a runtime surprise.
pub const PROFILE_API_VERSION: &str = PROVIDER_PROFILE_API_VERSION;

/// The error envelope every failing control route returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ApiErrorEnvelope {
    pub error: ApiErrorBody,
}

/// `GET /api/v1/canonical/projects`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalProjectsResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub projects: Vec<ProjectRecord>,
}

/// The `configuration` envelope shared by the three configuration read routes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalConfigurationEnvelope {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub configuration: ProjectConfigurationView,
}

/// `GET /api/v1/canonical/missions/{mission}/configuration`
///
/// The substrate stores a Mission's configuration as a `(value, revision)`
/// pair. That pair used to be interpolated into the response whole, which
/// serialised as `{"0":…,"1":…}`; the two halves are named fields now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalMissionConfigEnvelope {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub mission_id: String,
    /// `null` when the Mission has no stored pre-run configuration yet.
    pub configuration: Value,
    /// The substrate revision the configuration was read at; `0` when unset.
    pub revision: u64,
}

/// `GET /api/v1/canonical/work/events`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalEventsEnvelope {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub mission_id: String,
    pub events: Vec<CanonicalWorkEvent>,
}

/// `GET|POST|PUT /api/v1/profile` and `/api/v1/profile/bootstrap`.
///
/// This response used to be an anonymous `json!` literal, which is exactly the
/// kind of shape a hand-written TypeScript mirror cannot be checked against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ProfileView {
    #[ts(type = "ProfileApiVersion")]
    pub api_version: String,
    pub profile: Option<Profile>,
    /// SHA-256 of the project YAML the response was derived from. A write must
    /// present the same value; a mismatch is rejected instead of overwriting a
    /// concurrent edit.
    pub revision: Option<String>,
    pub candidates: Vec<Candidate>,
}

/// The request bodies the PWA sends. Typed so a malformed body is a compile
/// error in the client rather than a runtime surprise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProfileBootstrapRequest {
    /// `"new"` or `"import"`.
    pub choice: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProfileReplaceRequest {
    pub revision: String,
    pub profile: Profile,
}

// -- generation ---------------------------------------------------------------

/// One generation config for the whole file.
///
/// `number` is deliberate and load-bearing: `serde_json` renders `u64`/`i64` as
/// JSON numbers, so `bigint` would be a *lie* that forces the client to wrap
/// every id in `BigInt` and breaks `JSON.parse` round-trips.
fn config(out_dir: &Path) -> Config {
    Config::default()
        .with_large_int("number")
        .with_out_dir(out_dir)
}

/// The contract roots the frontend consumes. `export_all` walks each root's
/// transitive dependencies, so a new field of a new type can never produce a
/// file that references a type it does not declare.
fn export_roots(cfg: &Config) -> Result<(), ts_rs::ExportError> {
    ApiErrorEnvelope::export_all(cfg)?;
    CanonicalProjectsResponse::export_all(cfg)?;
    CanonicalConfigurationEnvelope::export_all(cfg)?;
    CanonicalConfigurationResponse::export_all(cfg)?;
    CanonicalMissionConfigEnvelope::export_all(cfg)?;
    CanonicalMissionResponse::export_all(cfg)?;
    CanonicalEventsEnvelope::export_all(cfg)?;
    CanonicalDashboardResponse::export_all(cfg)?;
    CanonicalProjectResponse::export_all(cfg)?;
    ResourceBudget::export_all(cfg)?;
    ExecutionWitness::export_all(cfg)?;
    CanonicalWorkSnapshot::export_all(cfg)?;
    CanonicalWorkEvent::export_all(cfg)?;
    ProfileView::export_all(cfg)?;
    ProfileBootstrapRequest::export_all(cfg)?;
    ProfileReplaceRequest::export_all(cfg)?;
    ActivityCursor::export_all(cfg)?;
    ChangeSetFingerprintInputV1::export_all(cfg)?;
    CommandFingerprintInputV1::export_all(cfg)?;
    Actor::export_all(cfg)?;
    Approval::export_all(cfg)?;
    ApprovalDecision::export_all(cfg)?;
    ApprovalState::export_all(cfg)?;
    BlockerKind::export_all(cfg)?;
    CapabilityConstraints::export_all(cfg)?;
    CapabilityGrant::export_all(cfg)?;
    CapabilityRef::export_all(cfg)?;
    CapabilityRevocation::export_all(cfg)?;
    CapabilityRevocationState::export_all(cfg)?;
    BudgetScope::export_all(cfg)?;
    BudgetSnapshot::export_all(cfg)?;
    CallLifecycle::export_all(cfg)?;
    CanonicalEntityProjection::export_all(cfg)?;
    CanonicalSyncProjection::export_all(cfg)?;
    ChildCancellationPolicy::export_all(cfg)?;
    ChildFailurePolicy::export_all(cfg)?;
    ChildJoinPolicy::export_all(cfg)?;
    ChildPolicy::export_all(cfg)?;
    ClientKind::export_all(cfg)?;
    Command::export_all(cfg)?;
    CommandState::export_all(cfg)?;
    Consistency::export_all(cfg)?;
    DerivedMetric::export_all(cfg)?;
    EntityId::export_all(cfg)?;
    EntityKind::export_all(cfg)?;
    EntityRef::export_all(cfg)?;
    EventEnvelope::export_all(cfg)?;
    EventPayload::export_all(cfg)?;
    EventRegistryEntry::export_all(cfg)?;
    EventType::export_all(cfg)?;
    EffectIntent::export_all(cfg)?;
    EffectIntentState::export_all(cfg)?;
    ExecutionGraphEdge::export_all(cfg)?;
    ExecutionGraphNode::export_all(cfg)?;
    ExecutionGraphProjection::export_all(cfg)?;
    ExecutionLeaseRecord::export_all(cfg)?;
    ExecutionLimits::export_all(cfg)?;
    ExecutionMode::export_all(cfg)?;
    ExecutionPolicy::export_all(cfg)?;
    ExecutionRetryPolicy::export_all(cfg)?;
    ExecutorContract::export_all(cfg)?;
    ExternalSourceKind::export_all(cfg)?;
    Fact::export_all(cfg)?;
    Failure::export_all(cfg)?;
    FailureClass::export_all(cfg)?;
    LeaseState::export_all(cfg)?;
    MeasurementQuality::export_all(cfg)?;
    MeasurementView::export_all(cfg)?;
    Message::export_all(cfg)?;
    MessageBlock::export_all(cfg)?;
    MessageBlockKind::export_all(cfg)?;
    MessageLifecycle::export_all(cfg)?;
    ProjectScope::export_all(cfg)?;
    ProjectionEffect::export_all(cfg)?;
    ProjectionEffectPolicy::export_all(cfg)?;
    ResumeCursor::export_all(cfg)?;
    RunLifecycle::export_all(cfg)?;
    Snapshot::export_all(cfg)?;
    SpawnChildSpecV1::export_all(cfg)?;
    SpawnFingerprintInputV1::export_all(cfg)?;
    SideEffectMode::export_all(cfg)?;
    SpawnChildRequest::export_all(cfg)?;
    SyncWindowPolicy::export_all(cfg)?;
    TransientMessageDelta::export_all(cfg)?;
    WorkNodeBlocker::export_all(cfg)?;
    WorkNodeLifecycle::export_all(cfg)?;
    Ok(())
}

/// Render the generated TypeScript module as a single self-contained file.
///
/// ts-rs emits one file per type with relative imports, which is the right
/// shape for a published package and the wrong shape for an in-repo module. The
/// per-type files are therefore bundled here: imports are dropped, because every
/// symbol is declared in the same file, and the result is emitted in a stable
/// order.
///
/// Deterministic: the same Rust definitions always produce byte-identical
/// output, so regenerating without a contract change is a no-op in git.
///
/// Staging lands in the system temporary directory. Callers that write project
/// artifacts should use [`render_at`] and stage inside the project, which is
/// where OCG keeps its ignored intermediate state.
pub fn render() -> Result<String, String> {
    render_at(std::env::temp_dir().as_path())
}

/// [`render`], staging inside a caller-chosen directory.
///
/// ts-rs only offers filesystem export, so generation needs a scratch
/// directory. Placing it under the project's ignored `.ocg/` keeps
/// OCG intermediate state on persistent disk instead of a small tmpfs.
pub fn render_at(staging_root: &Path) -> Result<String, String> {
    let staging = staging_dir(staging_root);
    // A stale staging directory would leak yesterday's types into today's file.
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging)
        .map_err(|error| format!("create staging directory: {error}"))?;

    let result = bundle(&staging);
    let _ = std::fs::remove_dir_all(&staging);
    result
}

/// A private staging directory per call.
///
/// `render` is called by the generator binary, by the drift test, and by every
/// other contract assertion, several of which run in parallel on different
/// threads of one process. Keying the directory on the process id alone would
/// let concurrent calls read each other's half-written output, so the counter
/// keeps each call isolated.
fn staging_dir(root: &Path) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    root.join(format!("rs-ts-{}-{sequence}", std::process::id()))
}

fn bundle(staging: &Path) -> Result<String, String> {
    let cfg = config(staging);
    export_roots(&cfg).map_err(|error| format!("export contract: {error}"))?;

    let mut emitted: BTreeMap<String, String> = BTreeMap::new();
    collect(staging, &mut emitted)?;

    // `serde_json::Value` is not an OCG contract; it is emitted as a local shim.
    emitted.remove("JsonValue");

    let mut out = String::new();
    out.push_str(header());
    out.push_str("\n\n");
    out.push_str(shims());
    out.push('\n');
    for body in emitted.values() {
        out.push_str(body);
        out.push('\n');
    }
    out.push_str(footer());
    Ok(out)
}

fn collect(dir: &Path, out: &mut BTreeMap<String, String>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|error| format!("read {}: {error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read dir entry: {error}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out)?;
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("ts") {
            continue;
        }
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("unnamed contract file: {}", path.display()))?
            .to_string();
        let body = std::fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        out.insert(name, clean(&body));
    }
    Ok(())
}

fn clean(body: &str) -> String {
    let mut out = String::new();
    for line in body.lines() {
        let trimmed = line.trim();
        // Per-type `import type {...} from "./X"` statements assume one file per
        // type. This module is a single file, so they are dropped: every symbol
        // it references is declared here.
        if trimmed.starts_with("import type ") || trimmed.starts_with("import {") {
            continue;
        }
        // ts-rs's own banner and the redundant `export type { X };` echo add
        // noise without adding information.
        if trimmed.starts_with("// This file was generated by [ts-rs]") {
            continue;
        }
        if trimmed.starts_with("export type { ") && trimmed.ends_with(" };") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    while out.ends_with("\n\n") {
        out.pop();
    }
    out
}

fn header() -> &'static str {
    r#"/**
 * GENERATED FILE - DO NOT EDIT.
 *
 * Projected from the Rust wire contract in `src/contracts.rs` by
 * `cargo run --bin ocg-rs-ts`. Rust is the single source of truth for every
 * type below; this file is a projection of it, committed so the frontend needs
 * no build step and reviewers see contract changes in the diff.
 *
 * If you change a Rust contract, run `make contracts` and commit the result.
 * `make contracts-check` fails when this file drifts.
 */"#
}

/// Types the contract references that are not themselves OCG contracts.
fn shims() -> &'static str {
    r#"/**
 * An opaque JSON value, mirroring `serde_json::Value`. The backend does not
 * constrain its shape, so the client must not invent one.
 */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/** The canonical control protocol version, from the Rust constant. */
export type CanonicalApiVersion = "ocg.canonical.v1";

/** The Profile control protocol version, from the Rust constant. */
export type ProfileApiVersion = "ocg.profile.v1";
"#
}

fn footer() -> &'static str {
    r#"/**
 * The protocol versions this file was generated from. They are asserted equal to
 * the Rust constants by `make contracts-check`, so a version bump cannot silently
 * desynchronise the two sides.
 */
export const CANONICAL_API_VERSION: CanonicalApiVersion = "ocg.canonical.v1";
export const PROFILE_API_VERSION: ProfileApiVersion = "ocg.profile.v1";
"#
}
