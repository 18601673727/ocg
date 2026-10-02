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
    event_registry, ActivityCursor, Actor, Approval, ApprovalDecision, ApprovalState,
    AttemptLifecycle, BlockerKind, BudgetScope, BudgetSnapshot, CallLifecycle,
    CanonicalEntityProjection, CanonicalSyncProjection, CapabilityConstraints, CapabilityGrant,
    CapabilityRef, CapabilityRevocation, CapabilityRevocationState, ChildCancellationPolicy,
    ChildFailurePolicy, ChildJoinPolicy, ChildPolicy, ClientKind, Command, CommandState,
    Consistency, DerivedMetric, EffectIntent, EffectIntentState, EntityId, EntityKind, EntityRef,
    EventEnvelope, EventPayload, EventRegistryEntry, EventType, ExecutionGraphEdge,
    ExecutionGraphNode, ExecutionGraphProjection, ExecutionLeaseRecord, ExecutionLimits,
    ExecutionMode, ExecutionPolicy, ExecutionRetryPolicy, ExecutorContract, ExternalSourceKind,
    Fact, Failure, FailureClass, JobBlocker, JobLifecycle, LeaseState, MeasurementQuality,
    MeasurementView, Message, MessageBlock, MessageBlockKind, MessageLifecycle, ProjectScope,
    ProjectionEffect, ProjectionEffectPolicy, ResumeCursor, SideEffectMode, Snapshot,
    SpawnChildRequest, SyncWindowPolicy, TransientMessageDelta, CORE_CONTRACT_VERSION,
    CURSOR_SCHEMA_VERSION, SNAPSHOT_SCHEMA_VERSION,
};
pub use crate::orchestration::canonical_control::{
    CanonicalConfigurationResponse, CanonicalDashboardResponse, CanonicalJobConfigResponse,
    CanonicalJobEvent, CanonicalJobSnapshot, CanonicalProjectResponse, GlobalConfiguration,
    ProjectConfiguration, ProjectConfigurationView, ProjectRecord, ResourceBudget,
    CANONICAL_CONTROL_API_VERSION,
};
/// The authority record. It travels inside a canonical event payload, so the
/// PWA reads it out of an opaque JSON value and must not be able to drift from
/// the definition that actually confers authority.
pub use crate::orchestration::domain::ExecutionWitness;
pub use crate::profile::{Model, Origin, Profile, Provider, PROVIDER_PROFILE_API_VERSION};
pub use crate::provider_protocol::ProviderProtocol;

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

/// `GET /api/v1/canonical/jobs/{job}/configuration`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalJobConfigEnvelope {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub job_id: String,
    /// `null` when the Job has no stored pre-attempt configuration yet.
    pub configuration: Value,
    /// The substrate revision the configuration was read at; `0` when unset.
    pub revision: u64,
}

/// `GET /api/v1/canonical/jobs/events`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalEventsEnvelope {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub job_id: String,
    pub events: Vec<CanonicalJobEvent>,
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
    /// SHA-256 of the Profile document. A write must present the same value;
    /// a mismatch is rejected instead of overwriting a concurrent edit.
    pub revision: Option<String>,
    /// Backend-computed execution readiness: model keys that satisfy the
    /// same provider/model/endpoint/credential rules as canonical launch.
    /// The PWA decides onboarding vs workspace from this alone. Secrets are
    /// never included.
    pub runnable_choices: Vec<String>,
}

/// The request bodies the PWA sends. Typed so a malformed body is a compile
/// error in the client rather than a runtime surprise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProfileBootstrapRequest {
    /// The explicit bootstrap action, currently `"new"`.
    pub choice: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProfileReplaceRequest {
    pub revision: String,
    pub profile: Profile,
}

/// `POST /api/v1/profile/credentials`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProfileCredentialRequest {
    /// Vault credential name. Validated by the Vault, never logged.
    pub name: String,
    /// The secret value. Written to the Vault only; never returned.
    pub value: String,
}

/// `POST /api/v1/canonical/jobs/launch`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct JobLaunchRequest {
    pub command_id: String,
    pub draft_id: String,
    pub project_id: String,
    pub session_id: String,
    pub objective: String,
    pub success_criteria: Option<String>,
    pub constraints: Option<String>,
    pub hard_budget_micros: i64,
    pub resource_commitment: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChatConversationView {
    pub conversation_id: String,
    pub session_id: String,
    pub title: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChatConversationsResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub conversations: Vec<ChatConversationView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ChatMessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChatMessageView {
    pub message_id: String,
    pub command_id: String,
    pub role: ChatMessageRole,
    pub state: MessageLifecycle,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
    pub attempt_state: String,
    pub replay_job_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChatMessagesResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub conversation: ChatConversationView,
    pub messages: Vec<ChatMessageView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct JobLaunchResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    /// "accepted" | "rejected" | "failed"
    pub outcome: String,
    pub command_id: String,
    pub draft_id: String,
    pub project_id: String,
    pub session_id: String,
    pub job_id: Option<String>,
    pub message: String,
    pub duplicate: bool,
}

// -- setup / first-run ---------------------------------------------------------

/// Request to connect a new provider and discover models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupConnectRequest {
    /// Human-readable provider name.
    pub name: String,
    /// Provider endpoint URL (base URL or full chat/completions URL).
    pub endpoint: String,
    /// API key for the provider.
    pub api_key: String,
}

/// Response after connecting a provider and discovering models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupConnectResponse {
    #[ts(type = "ProfileApiVersion")]
    pub api_version: String,
    /// Provider key assigned by the backend.
    pub provider_key: String,
    /// Normalized models from the provider.
    pub models: Vec<SetupModel>,
    /// Profile revision after the provider was persisted, for the model save.
    pub revision: String,
}

/// A model discovered from a provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupModel {
    /// OCG model key (provider_key/model_id normalized).
    pub key: String,
    /// Upstream provider model id.
    pub id: String,
    /// Display label.
    pub label: String,
    /// Known metadata (may be empty).
    pub metadata: crate::profile::ModelMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupRefreshRequest {
    pub provider_key: String,
    pub revision: String,
}

/// Request to save selected models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupModelsRequest {
    /// Provider key.
    pub provider_key: String,
    /// Models to enable, each carrying the OCG key and the upstream provider model id.
    pub models: Vec<SetupModelSelection>,
    /// Default model key.
    pub default_model: String,
    /// Profile revision for optimistic locking.
    pub revision: String,
}

/// A model selected by the user during setup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupModelSelection {
    /// OCG model key.
    pub key: String,
    /// Upstream provider model id.
    pub id: String,
}

/// Response after saving models.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupModelsResponse {
    #[ts(type = "ProfileApiVersion")]
    pub api_version: String,
    pub selected_models: Vec<String>,
    pub default_model: String,
    pub runnable_choices: Vec<String>,
    pub revision: String,
}

/// Request to browse filesystem directories.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupBrowseRequest {
    /// Path to browse (empty = home directory).
    pub path: Option<String>,
}

/// A directory entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupDirectoryEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
}

/// Response for directory listing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupBrowseResponse {
    pub current: String,
    pub parent: Option<String>,
    pub entries: Vec<SetupDirectoryEntry>,
}

/// Request to initialize and import a project.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupProjectRequest {
    pub command_id: String,
    /// Root path of the project.
    pub root: String,
}

/// Response after project initialization.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SetupProjectResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub name: String,
    pub root: String,
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
    CanonicalJobConfigEnvelope::export_all(cfg)?;
    CanonicalJobConfigResponse::export_all(cfg)?;
    CanonicalEventsEnvelope::export_all(cfg)?;
    CanonicalDashboardResponse::export_all(cfg)?;
    CanonicalProjectResponse::export_all(cfg)?;
    ResourceBudget::export_all(cfg)?;
    ExecutionWitness::export_all(cfg)?;
    CanonicalJobSnapshot::export_all(cfg)?;
    CanonicalJobEvent::export_all(cfg)?;
    JobLaunchRequest::export_all(cfg)?;
    ChatConversationsResponse::export_all(cfg)?;
    ChatMessagesResponse::export_all(cfg)?;
    JobLaunchResponse::export_all(cfg)?;
    ProfileView::export_all(cfg)?;
    ProviderProtocol::export_all(cfg)?;
    ProfileBootstrapRequest::export_all(cfg)?;
    ProfileReplaceRequest::export_all(cfg)?;
    ProfileCredentialRequest::export_all(cfg)?;
    SetupConnectRequest::export_all(cfg)?;
    SetupRefreshRequest::export_all(cfg)?;
    SetupConnectResponse::export_all(cfg)?;
    SetupModel::export_all(cfg)?;
    SetupModelSelection::export_all(cfg)?;
    SetupModelsRequest::export_all(cfg)?;
    SetupModelsResponse::export_all(cfg)?;
    SetupBrowseRequest::export_all(cfg)?;
    SetupDirectoryEntry::export_all(cfg)?;
    SetupBrowseResponse::export_all(cfg)?;
    SetupProjectRequest::export_all(cfg)?;
    SetupProjectResponse::export_all(cfg)?;
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
    AttemptLifecycle::export_all(cfg)?;
    Snapshot::export_all(cfg)?;
    SpawnChildSpecV1::export_all(cfg)?;
    SpawnFingerprintInputV1::export_all(cfg)?;
    SideEffectMode::export_all(cfg)?;
    SpawnChildRequest::export_all(cfg)?;
    SyncWindowPolicy::export_all(cfg)?;
    TransientMessageDelta::export_all(cfg)?;
    JobBlocker::export_all(cfg)?;
    JobLifecycle::export_all(cfg)?;
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
        out.push_str(line.trim_end());
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
