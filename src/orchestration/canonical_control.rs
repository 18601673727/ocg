//! Backend-backed control plane for the canonical Job/Attempt lane.
//!
//! This module is deliberately transport-neutral. The PWA and loopback server
//! consume these DTOs; neither owns execution state. The SQLite substrate is
//! the authority, and every response includes a protocol version and the
//! canonical Job snapshot needed to reconcile a reconnect.

use crate::core_contract::{Failure, FailureClass};
use crate::core_store::{ConfigChange, ConfigDocument, CoreStore};
use crate::error::{OcgError, Result};
use crate::orchestration::admission::{
    publish_call, reserve, AdmissionContext, AdmissionTarget, PayloadMode, PreparedExecution,
};
use crate::orchestration::disk_guard::{DiskGuard, DiskGuardConfig, DiskGuardStatus};
use crate::orchestration::domain::{
    Attempt, AttemptAuthority, Call, ChildPolicy, DispatchIntent, DomainRepository, Executor, Job,
    JobAttachment, JobOrigin, JobPrincipal, JobScope, JobSpec,
};
use crate::orchestration::execution_dispatch::{CallCancellation, ExecutionEvent};
use crate::orchestration::health_probe::HealthProbeObservation;
use crate::orchestration::journal::{EventDelta, ExecutionProjection, MAX_EVENT_READ};
use crate::project::{self, ProjectBoundary};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use ts_rs::TS;

pub const CANONICAL_CONTROL_API_VERSION: &str = "ocg.canonical.v1";
/// A terminal chat tail remains replayable for this bounded period after the
/// provider terminal event is recorded. This is transport retention only, not
/// durable chat history.
const CHAT_REPLAY_LIFETIME: Duration = Duration::from_secs(300);
/// How long a Chat-root Job's Attempt may execute before the Core cancels it.
const CHAT_EXECUTION_TIMEOUT: Duration = Duration::from_secs(900);

/// Why a Chat request with these inputs cannot run on this model, if it
/// cannot. Launch and the replay of a frozen Chat request apply the same rule.
fn chat_capability_refusal(
    provider: &crate::profile::Provider,
    model: &crate::profile::Model,
    effort: Option<&str>,
    images: bool,
) -> Option<&'static str> {
    if images
        && model.metadata.as_ref().is_some_and(|metadata| {
            metadata.images == Some(false)
                || (metadata.images.is_none() && metadata.multimodal == Some(false))
        })
    {
        return Some("selected model does not support image input");
    }
    if let Some(effort) = effort {
        if !super::placement::supports_effort(model, effort)
            || !provider.wire_protocol().is_openai_chat_completions()
            || !matches!(
                effort,
                "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
            )
        {
            return Some("selected reasoning effort is unsupported for this model/protocol");
        }
    }
    None
}

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
}

fn chat_conversation_view(
    session_id: String,
    conversation: crate::core_contract::Conversation,
) -> crate::contracts::ChatConversationView {
    crate::contracts::ChatConversationView {
        conversation_id: conversation.id.as_str().to_string(),
        session_id,
        title: conversation.title,
        created_at: conversation.created_at,
        updated_at: conversation.updated_at,
    }
}

/// The deterministic first model input: the launch intent and nothing else.
/// No persona and no system prompt is ever synthesized here.
fn initial_user_message(request: &crate::contracts::JobLaunchRequest) -> String {
    let mut message = request.objective.clone();
    for (label, content) in [
        ("Success criteria", request.success_criteria.as_deref()),
        ("Constraints", request.constraints.as_deref()),
    ] {
        if let Some(content) = content.filter(|content| !content.trim().is_empty()) {
            message.push_str("\n\n");
            message.push_str(label);
            message.push_str(":\n");
            message.push_str(content);
        }
    }
    message
}

/// The content digest a Job-management command is recorded with, so a replay
/// of the same command identity with different content is a conflict.
fn command_hash(request: &impl Serialize) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(request).map_err(|error| invalid(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

/// Backend-owned Project identity. Import is a boundary validation operation,
/// not a general filesystem manager.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProjectRecord {
    pub project_id: String,
    pub root: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The unit reported when a budget does not name one.
pub const DEFAULT_BUDGET_UNIT: &str = "USD";

/// A resource budget attached to the global configuration.
///
/// This is a real struct rather than an opaque JSON value. It used to be
/// `Option<Value>`, which forced the PWA to guess a shape and cast its way
/// through an untyped value; typing it here means the budget the control
/// surface sends is validated at the boundary and the shape the PWA reads is
/// the one the backend actually stores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(default)]
pub struct ResourceBudget {
    /// Zero records "no explicit hard limit", not "a budget of nothing".
    pub hard_limit: f64,
    pub unit: String,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            hard_limit: 0.0,
            unit: DEFAULT_BUDGET_UNIT.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, TS)]
pub struct GlobalConfiguration {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub profile: Option<String>,
    pub routing: Option<String>,
    pub runtime: Option<String>,
    pub resource_budget: Option<ResourceBudget>,
    /// Execution disk-space protection. `None` selects the conservative
    /// built-in reserve; existing stored configurations keep loading unchanged.
    pub storage_guard: Option<DiskGuardConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, TS)]
pub struct ProjectConfiguration {
    pub defaults: Value,
}

/// Why an Attempt whose admission stopped before its provider Call was
/// published settled as failed: it can no longer be published as committed.
fn unpublished_admission_failure(why: &str) -> Failure {
    super::domain::job_failure(
        "admission_interrupted",
        FailureClass::Internal,
        &format!("admission was interrupted before its provider Call was published; nothing was sent to the provider; {why}"),
        true,
    )
}

/// Settle a resumed reservation that admission refused. It is refused only
/// when its committed target can no longer run, so an Attempt that never
/// published a Call settles now instead of being refused on every pass. A
/// Job that holds no such Attempt is left as it is.
fn settle_unpublished_reservation(
    domain: &mut DomainRepository,
    project_id: &str,
    job: &Job,
    why: &str,
) -> Result<()> {
    let Some(attempt_id) = job
        .authoritative_attempt_id
        .as_deref()
        .filter(|_| job.state == super::domain::JobState::Running)
    else {
        return Ok(());
    };
    if domain
        .unpublished_admissions(project_id)?
        .iter()
        .any(|(_, unpublished, _)| unpublished == attempt_id)
    {
        domain.fail_attempt(attempt_id, &unpublished_admission_failure(why))?;
    }
    Ok(())
}

fn provider_concurrency(defaults: &Value) -> Result<usize> {
    match defaults.get("provider_concurrency") {
        None => Ok(1),
        Some(value) => value
            .as_u64()
            .filter(|limit| (1..=64).contains(limit))
            .map(|limit| limit as usize)
            .ok_or_else(|| invalid("Project provider_concurrency must be between 1 and 64")),
    }
}

/// The effective disk-guard thresholds: the configured reserve, or the
/// conservative built-in policy when the Project predates disk safety.
fn disk_guard_config(global: &GlobalConfiguration) -> DiskGuardConfig {
    global.storage_guard.unwrap_or_default()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ProjectConfigurationView {
    pub project: ProjectRecord,
    pub global: GlobalConfiguration,
    /// The revision of the global configuration; a change must name it.
    pub global_revision: u64,
    pub project_defaults: ProjectConfiguration,
    /// The revision of this Project's configuration; a change must name it.
    pub project_revision: u64,
}

/// The configuration scope of the control plane's own settings.
pub const CONTROL_SCOPE: &str = "control";
/// The section of [`CONTROL_SCOPE`] holding the [`GlobalConfiguration`].
const GLOBAL_SECTION: &str = "global";
/// The section of a Project's scope holding its defaults.
const PROJECT_DEFAULTS_SECTION: &str = "defaults";

fn global_from_document(document: &ConfigDocument) -> Result<GlobalConfiguration> {
    document
        .section(GLOBAL_SECTION)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| invalid(format!("invalid global configuration: {error}")))
        .map(Option::unwrap_or_default)
}

/// The committed global configuration and the revision of its scope.
pub(crate) fn global_configuration(store: &CoreStore) -> Result<(GlobalConfiguration, u64)> {
    let document = store.document(CONTROL_SCOPE)?;
    Ok((global_from_document(&document)?, document.revision))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalProjectResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub command_id: String,
    pub accepted: bool,
    pub project: ProjectRecord,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalConfigurationResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub command_id: String,
    pub accepted: bool,
    pub project_id: String,
    pub revision: u64,
    pub configuration: ProjectConfigurationView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobConfigResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub command_id: String,
    pub accepted: bool,
    pub job_id: String,
    pub revision: u64,
    pub configuration: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobSnapshot {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub job: Value,
    pub cursor: u64,
}

/// The parentage facts a relations view is built from: every child's parent,
/// and the provenance of the authority that created it.
#[derive(Debug, Default)]
struct ChildOrigins {
    parents: BTreeMap<String, String>,
    spawns: BTreeMap<String, JobOrigin>,
    attachments: BTreeMap<String, JobAttachment>,
}

impl ChildOrigins {
    fn take(snapshot: &mut super::journal::ExecutionSnapshot) -> Self {
        Self {
            parents: std::mem::take(&mut snapshot.job_origins),
            spawns: std::mem::take(&mut snapshot.job_origin_details),
            attachments: std::mem::take(&mut snapshot.job_attachments),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobRelations {
    pub parent_job_id: Option<String>,
    /// Set when an Executor spawned this Job under its parent Attempt.
    #[serde(default)]
    pub origin: Option<JobOrigin>,
    /// Set when an authorized principal attached this Job to an open
    /// orchestration Job. A child has exactly one of `origin` and
    /// `attachment`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment: Option<JobAttachment>,
    /// Set when this Job is an orchestration scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<JobScope>,
    pub child_job_ids: Vec<String>,
    pub depends_on: Vec<String>,
    pub blocks: Vec<String>,
    pub blocked_by: Vec<String>,
    pub blocked: bool,
    pub root_job_id: String,
    pub depth: u64,
    pub descendant_job_ids: Vec<String>,
    pub descendant_summary: BTreeMap<String, usize>,
}

impl CanonicalJobRelations {
    fn from_projection(
        job: &Job,
        projection: &ExecutionProjection,
        origins: &ChildOrigins,
    ) -> Self {
        let ChildOrigins {
            parents: origins,
            spawns: origin_details,
            attachments,
        } = origins;
        let depends_on: Vec<String> = projection
            .dependencies
            .iter()
            .filter(|(project_id, job_id, _)| project_id == &job.project_id && job_id == &job.id)
            .map(|(_, _, prerequisite)| prerequisite.clone())
            .collect();
        let blocked_by: Vec<String> = depends_on
            .iter()
            .filter(|id| {
                !projection
                    .jobs
                    .get(*id)
                    .is_some_and(|prerequisite| prerequisite.state.satisfies_dependency())
            })
            .cloned()
            .collect();
        let mut descendant_job_ids = origins
            .iter()
            .filter(|(_, parent)| *parent == &job.id)
            .map(|(child, _)| child.clone())
            .collect::<Vec<_>>();
        let mut cursor = 0;
        while cursor < descendant_job_ids.len() {
            let parent = &descendant_job_ids[cursor];
            let children = origins
                .iter()
                .filter(|(_, origin_parent)| origin_parent == &parent)
                .map(|(child, _)| child.clone())
                .filter(|child| !descendant_job_ids.contains(child))
                .collect::<Vec<_>>();
            descendant_job_ids.extend(children);
            cursor += 1;
        }
        let mut descendant_summary = BTreeMap::new();
        for descendant_id in &descendant_job_ids {
            if let Some(descendant) = projection.jobs.get(descendant_id) {
                *descendant_summary
                    .entry(descendant.state.to_string())
                    .or_insert(0) += 1;
            }
        }
        Self {
            parent_job_id: origins.get(&job.id).cloned(),
            origin: origin_details.get(&job.id).cloned(),
            attachment: attachments.get(&job.id).cloned(),
            scope: (job.scope != JobScope::None).then_some(job.scope),
            child_job_ids: origins
                .iter()
                .filter(|(_, parent)| *parent == &job.id)
                .map(|(child, _)| child.clone())
                .collect(),
            depends_on,
            blocks: projection
                .dependencies
                .iter()
                .filter(|(project_id, _, prerequisite)| {
                    project_id == &job.project_id && prerequisite == &job.id
                })
                .map(|(_, dependent, _)| dependent.clone())
                .collect(),
            blocked: !blocked_by.is_empty(),
            blocked_by,
            root_job_id: job.root_job_id.clone(),
            depth: job.depth,
            descendant_job_ids,
            descendant_summary,
        }
    }
}

#[derive(Serialize)]
struct CanonicalJobView<'a> {
    #[serde(flatten)]
    job: &'a Job,
    #[serde(flatten)]
    relations: CanonicalJobRelations,
    #[serde(flatten)]
    operations: CanonicalJobOperations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobOperations {
    pub can_cancel: bool,
    pub can_retry: bool,
}

impl CanonicalJobOperations {
    fn for_job(job: &Job, blocked: bool) -> Self {
        Self {
            can_cancel: job.state.can_cancel(),
            can_retry: job.state.can_retry()
                && job.authoritative_attempt_id.is_none()
                && job.generation < i64::MAX as u64
                && !blocked,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct CanonicalJobOperationRequest {
    pub expected_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobOperationResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub job_id: String,
    pub accepted: bool,
    pub snapshot: CanonicalJobSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct CanonicalJobSpawnRequest {
    pub parent_attempt_id: String,
    pub expected_generation: u64,
    #[serde(default)]
    pub call_id: Option<String>,
    pub spawn_key: String,
    pub spec: JobSpec,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub executor_kind: String,
    #[serde(default)]
    pub policy: ChildPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobSpawnResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub child_job_id: String,
    pub duplicate: bool,
    pub snapshot: CanonicalJobSnapshot,
}

/// Create an orchestration root Job in a Project.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalOrchestrationRequest {
    pub command_id: String,
    pub project_id: String,
    pub spec: JobSpec,
}

/// Attach child work to an open orchestration Job through Job management.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalJobAttachRequest {
    pub command_id: String,
    pub spec: JobSpec,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub policy: ChildPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobManagementResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub job_id: String,
    /// True when the command identity was already recorded and this response
    /// names the Job it created.
    pub duplicate: bool,
    pub snapshot: CanonicalJobSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobSummary {
    pub job_id: String,
    pub created_at: i64,
    pub state: String,
    pub updated_at: i64,
    pub termination_reason: Option<Failure>,
    /// Set when this Job is a Health Probe. The dashboard already carries the
    /// Job, so the declared Provider × Model × Effort target rides with it
    /// rather than requiring a second read of every probe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_probe: Option<super::health_probe::HealthProbeIntent>,
    #[serde(flatten)]
    pub relations: CanonicalJobRelations,
    #[serde(flatten)]
    pub operations: CanonicalJobOperations,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobEvent {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub job_id: String,
    /// The canonical execution journal cursor this event occupies.
    pub sequence: u64,
    pub event_id: String,
    pub kind: String,
    pub payload: Value,
}

/// The outcome of asking for the canonical event tail after a cursor.
///
/// This is transport-side control state, not a declared wire type: the three
/// outcomes differ in *what the consumer must do next*, which is exactly what
/// must not be flattened into a list of events.
#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalEventTail {
    /// The complete delta after the requested cursor, in cursor order. May be
    /// empty when the cursor is already at the head.
    Events(Vec<CanonicalJobEvent>),
    /// The requested cursor is below the journal's retained floor, so its delta
    /// can no longer be produced. The consumer must refetch
    /// [`CanonicalControlService::canonical_snapshot`] and resume from the
    /// cursor that snapshot reports. No partial suffix is returned.
    ResyncRequired {
        requested: u64,
        floor_cursor: u64,
        head_cursor: u64,
    },
    /// The requested cursor is ahead of the durable head and cannot name a real
    /// position.
    InvalidCursor { requested: u64, head_cursor: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalDashboardResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub project_id: String,
    pub jobs: Vec<CanonicalJobSummary>,
    pub selected_job: Option<CanonicalJobSnapshot>,
    /// Cached execution disk-space state for this Project, if its runtime has
    /// performed an observation. Absent before the first observation.
    pub disk_guard: Option<DiskGuardStatus>,
}

/// Acknowledgement for cancelling one active chat turn. Transport state only;
///
/// execution authority stays with the durable Attempt that was revoked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChatCancelResponse {
    #[ts(type = "CanonicalApiVersion")]
    pub api_version: String,
    pub session_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Clone)]
pub struct CanonicalControlService {
    initial_root: PathBuf,
    store: CoreStore,
    runtime_handle: Option<crate::orchestration::execution_runtime::ExecutionRuntimeHandle>,
    remote_execution: Option<crate::remote_execution::RemoteExecution>,
    runtime_registry: Option<Arc<crate::orchestration::execution_runtime::ProjectRuntimeRegistry>>,
    profile_service: crate::profile::ProfileService,
    chat_registrations: Arc<Mutex<std::collections::HashMap<String, ChatRegistration>>>,
    active_chats: Arc<Mutex<std::collections::HashMap<(String, String), ActiveChat>>>,
    launch_lock: Arc<Mutex<()>>,
    registry_lock: Arc<Mutex<()>>,
    /// Projects whose automatic admission pass is running in this process.
    admitting: Arc<Mutex<std::collections::BTreeSet<String>>>,
    chat_forwarder: Arc<ChatForwarder>,
    placement_deferrals: Arc<Mutex<BTreeMap<String, PlacementDeferral>>>,
}

/// One Project's automatic admission pass in this process; released on drop.
struct AdmissionGuard {
    admitting: Arc<Mutex<std::collections::BTreeSet<String>>>,
    project_id: String,
}

impl AdmissionGuard {
    /// `None` when a pass for this Project is already running.
    fn try_acquire(
        admitting: &Arc<Mutex<std::collections::BTreeSet<String>>>,
        project_id: &str,
    ) -> Result<Option<Self>> {
        let inserted = admitting
            .lock()
            .map_err(|_| invalid("admission registry poisoned"))?
            .insert(project_id.to_string());
        Ok(inserted.then(|| Self {
            admitting: admitting.clone(),
            project_id: project_id.to_string(),
        }))
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Ok(mut admitting) = self.admitting.lock() {
            admitting.remove(&self.project_id);
        }
    }
}

// Only Profile/spec incompatibility is memoized. Runtime availability is not
// cached, and restart discards this optimization before authoritative recovery.
#[derive(Debug)]
struct PlacementDeferral {
    root: PathBuf,
    profile_revision: String,
    configuration_revision: u64,
    job_configuration_revision: Option<u64>,
    generation: u64,
    spec: JobSpec,
}

pub(crate) struct JobAdmissionWorker {
    stopped: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    runtimes: Option<Arc<crate::orchestration::execution_runtime::ProjectRuntimeRegistry>>,
}

impl Drop for JobAdmissionWorker {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Closing the bounded queues also releases an admission blocked on send.
        if let Some(registry) = &self.runtimes {
            if let Err(error) = registry.shutdown() {
                tracing::error!(%error, "automatic admission runtime shutdown failed");
            }
        }
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                tracing::error!("automatic Job admission worker panicked");
            }
        }
    }
}

/// One registered Project considered as a candidate owner of a Job lookup.
///
/// Only the boundary itself can be missing. Once the boundary exists, every
/// other failure — an unreadable store, a failed read, an identity this
/// boundary no longer owns — stays an error so corruption is never reported as
/// an absent Job.
enum CandidateRepository {
    Ready(DomainRepository),
    Unavailable(&'static str),
}

#[derive(Debug)]
struct ChatRegistration {
    images: Vec<crate::contracts::ChatImage>,
    selection: Option<crate::contracts::ChatModelSelection>,
    sender: flume::Sender<ExecutionEvent>,
    cancelled: CallCancellation,
}

#[derive(Debug)]
struct ChatForwarder {
    registrations: flume::Sender<ChatForwardRegistration>,
}

struct ChatForwardRegistration {
    receiver: flume::Receiver<ExecutionEvent>,
    buffer: Arc<ChatEventBuffer>,
}

impl std::fmt::Debug for ChatForwardRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ChatForwardRegistration").finish()
    }
}

impl ChatForwarder {
    fn new() -> Arc<Self> {
        let (registrations, receiver) = flume::unbounded();
        std::thread::spawn(move || Self::run(receiver));
        Arc::new(Self { registrations })
    }

    fn register(
        &self,
        receiver: flume::Receiver<ExecutionEvent>,
        buffer: Arc<ChatEventBuffer>,
    ) -> Result<()> {
        self.registrations
            .send(ChatForwardRegistration { receiver, buffer })
            .map_err(|_| invalid("shared chat forwarder is unavailable"))
    }

    fn run(registrations: flume::Receiver<ChatForwardRegistration>) {
        let mut active = Vec::new();
        loop {
            while let Ok(registration) = registrations.try_recv() {
                active.push(registration);
            }
            if active.is_empty() {
                match registrations.recv_timeout(std::time::Duration::from_millis(100)) {
                    Ok(registration) => active.push(registration),
                    Err(flume::RecvTimeoutError::Disconnected) => return,
                    Err(flume::RecvTimeoutError::Timeout) => continue,
                }
            }
            let mut index = 0;
            while index < active.len() {
                let registration = &active[index];
                let mut terminal = false;
                while let Ok(event) = registration.receiver.try_recv() {
                    terminal =
                        matches!(&event, ExecutionEvent::Finished | ExecutionEvent::Failed(_));
                    if let Ok(mut state) = registration.buffer.state.lock() {
                        state.events.push(event);
                        if terminal {
                            state.terminal = true;
                            state.terminal_at = Some(Instant::now());
                        }
                    }
                    registration.buffer.cvar.notify_all();
                    if terminal {
                        break;
                    }
                }
                if terminal || registration.receiver.is_disconnected() {
                    active.swap_remove(index);
                } else {
                    index += 1;
                }
            }
            if registrations.is_disconnected() && active.is_empty() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// Retained provider tail for one chat turn. Events are appended in order
/// by a forwarder the moment the provider produces them, so an EventSource
/// that attaches after `POST /chat/send` replays the prefix before going
/// live. Terminal `Finished`/`Failed` ends the tail; the entry lives until
/// the stream consumes it or cancellation removes it.
#[derive(Debug, Default)]
pub(crate) struct ChatEventBuffer {
    pub(crate) state: Mutex<ChatBufferState>,
    pub(crate) cvar: std::sync::Condvar,
}

#[derive(Debug, Default)]
pub(crate) struct ChatBufferState {
    pub(crate) events: Vec<ExecutionEvent>,
    pub(crate) terminal: bool,
    pub(crate) terminal_at: Option<Instant>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct ActiveChat {
    project_id: String,
    session_id: String,
    job_id: String,
    attempt_id: String,
    cancelled: CallCancellation,
    sender: flume::Sender<ExecutionEvent>,
    buffer: std::sync::Arc<ChatEventBuffer>,
}

/// The live transport a Chat retry installed for its replacement Attempt.
struct ChatRetryTransport {
    key: (String, String),
    sender: flume::Sender<ExecutionEvent>,
    cancelled: CallCancellation,
    /// The settled tail this replacement displaced from its session.
    superseded: Option<ActiveChat>,
}

impl CanonicalControlService {
    /// The control service for the Project at `root`, over this
    /// installation's database.
    pub fn open(root: &Path) -> Result<Self> {
        let boundary = project::resolve(root);
        Ok(Self::with_store(boundary.root(), CoreStore::locate()?))
    }

    /// The control service a long-lived process serves.
    pub fn open_process(root: &Path, store: &CoreStore) -> Result<Self> {
        let boundary = project::resolve(root);
        Ok(Self::with_store(boundary.root(), store.clone()))
    }

    fn with_store(root: &Path, store: CoreStore) -> Self {
        Self {
            initial_root: root.to_path_buf(),
            profile_service: crate::profile::ProfileService::new(store.clone()),
            store,
            runtime_handle: None,
            remote_execution: None,
            runtime_registry: None,
            chat_registrations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_chats: Arc::new(Mutex::new(std::collections::HashMap::new())),
            launch_lock: Arc::new(Mutex::new(())),
            registry_lock: Arc::new(Mutex::new(())),
            admitting: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
            chat_forwarder: ChatForwarder::new(),
            placement_deferrals: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The pre-database constructor the unchanged unit tests still call. The
    /// paths are not read: all state lives in the OCG database.
    #[cfg(test)]
    fn new(root: &Path, _profile_path: &Path, _control_state: PathBuf) -> Self {
        Self::with_store(root, CoreStore)
    }

    pub(crate) fn with_remote_execution(
        mut self,
        confinement: crate::remote_execution::RemoteExecution,
    ) -> Self {
        self.remote_execution = Some(confinement);
        self
    }

    pub fn with_runtime_handle(
        mut self,
        handle: crate::orchestration::execution_runtime::ExecutionRuntimeHandle,
    ) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    pub fn with_profile_service(mut self, profile_service: crate::profile::ProfileService) -> Self {
        self.profile_service = profile_service;
        self
    }

    pub fn with_runtime_registry(
        mut self,
        registry: Arc<crate::orchestration::execution_runtime::ProjectRuntimeRegistry>,
    ) -> Self {
        self.runtime_registry = Some(registry);
        self
    }

    pub fn root(&self) -> &Path {
        &self.initial_root
    }

    pub(crate) fn security_store(&self) -> Result<crate::control_security::OwnershipStore> {
        crate::control_security::OwnershipStore::open()
    }

    /// This installation's configuration, credentials and Project registry.
    pub fn store(&self) -> &CoreStore {
        &self.store
    }

    pub(crate) fn chat_project_for(&self, session_id: &str, job_id: &str) -> Option<String> {
        self.active_chats
            .lock()
            .ok()?
            .values()
            .find(|chat| chat.session_id == session_id && chat.job_id == job_id)
            .map(|chat| chat.project_id.clone())
    }

    fn read_projects(&self) -> Result<Vec<ProjectRecord>> {
        self.store.projects()
    }

    /// The committed global configuration and its revision.
    fn read_configuration(&self) -> Result<(GlobalConfiguration, u64)> {
        global_configuration(&self.store)
    }

    /// One Project's committed defaults and the revision of its scope.
    fn project_defaults(&self, project_id: &str) -> Result<(ProjectConfiguration, u64)> {
        let document = self.store.document(project_id)?;
        let defaults = document
            .section(PROJECT_DEFAULTS_SECTION)
            .cloned()
            .map(|defaults| ProjectConfiguration { defaults })
            .unwrap_or_default();
        Ok((defaults, document.revision))
    }

    /// Register an existing directory as a Project, or return the
    /// registered Project that already contains it.
    ///
    /// Registration is the declaration: nothing is written into the
    /// directory. Identity belongs to the canonical root, so a client can
    /// never claim an existing identity for another directory, and a moved
    /// directory registers as a new Project.
    pub fn register_project(
        &self,
        command_id: &str,
        requested_root: &Path,
        now: i64,
    ) -> Result<CanonicalProjectResponse> {
        if !safe_id(command_id) {
            return Err(invalid("invalid command_id"));
        }
        let _registry = self
            .registry_lock
            .lock()
            .map_err(|_| invalid("Project registry poisoned"))?;
        let boundary: ProjectBoundary = project::resolve(requested_root);
        let root = project::canonicalize(boundary.root());
        if !root.is_dir() {
            return Err(invalid("project root is not a directory"));
        }
        let installation = crate::database::installation_dir()?;
        if installation.starts_with(&root) {
            return Err(invalid(
                "a Project must not contain the OCG installation directory",
            ));
        }
        let repository = DomainRepository::open_existing(&root)?;
        repository.ensure_project(&root)?;
        let project = repository.touch_project(&root, now)?;
        let project = ProjectRecord {
            project_id: project.id,
            root: project.root,
            created_at: project.created_at,
            updated_at: now,
        };
        Ok(CanonicalProjectResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project,
        })
    }

    pub fn projects(&self) -> Result<Vec<ProjectRecord>> {
        let mut projects = self.read_projects()?;
        if let Some(confinement) = &self.remote_execution {
            let ownership = self.security_store()?;
            let mut allowed = Vec::new();
            for project in projects {
                if ownership.owns(&confinement.operator_id, &project.project_id)?
                    && confinement.accepts_root(Path::new(&project.root)).is_ok()
                {
                    allowed.push(project);
                }
            }
            projects = allowed;
        }
        Ok(projects)
    }

    pub fn configuration(&self, project_id: &str) -> Result<ProjectConfigurationView> {
        let project = self
            .store
            .project(project_id)?
            .ok_or_else(|| invalid("unknown Project identity"))?;
        let (global, global_revision) = self.read_configuration()?;
        let (project_defaults, project_revision) = self.project_defaults(project_id)?;
        Ok(ProjectConfigurationView {
            project,
            global,
            global_revision,
            project_defaults,
            project_revision,
        })
    }

    /// Replace the global configuration, provided it is still at
    /// `expected_revision`.
    pub fn set_global_configuration(
        &self,
        command_id: &str,
        config: GlobalConfiguration,
        expected_revision: u64,
    ) -> Result<CanonicalConfigurationResponse> {
        if !safe_id(command_id) {
            return Err(invalid("invalid command_id"));
        }
        let value = serde_json::to_value(&config).map_err(|error| invalid(error.to_string()))?;
        let committed = self.store.commit(
            ConfigChange {
                scope: CONTROL_SCOPE,
                command_id,
                principal: "operator",
                expected_revision,
                sections: vec![(GLOBAL_SECTION, Some(value))],
            },
            |candidate| {
                let config = global_from_document(candidate)?;
                if let Some(storage_guard) = config.storage_guard.as_ref() {
                    storage_guard.validate()?;
                }
                Ok(())
            },
        )?;
        let global = global_from_document(&committed.document)?;
        let project = self
            .read_projects()?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("register a Project before configuring OCG"))?;
        let (project_defaults, project_revision) = self.project_defaults(&project.project_id)?;
        Ok(CanonicalConfigurationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project_id: project.project_id.clone(),
            revision: committed.revision,
            configuration: ProjectConfigurationView {
                project,
                global,
                global_revision: committed.document.revision,
                project_defaults,
                project_revision,
            },
        })
    }

    /// Replace one Project's defaults, provided its scope is still at
    /// `expected_revision`. Another Project's scope and the global scope are
    /// never touched.
    pub fn set_project_defaults(
        &self,
        command_id: &str,
        project_id: &str,
        defaults: Value,
        expected_revision: u64,
    ) -> Result<CanonicalConfigurationResponse> {
        if !safe_id(command_id) || !defaults.is_object() {
            return Err(invalid("invalid project configuration command"));
        }
        let project = self
            .store
            .project(project_id)?
            .ok_or_else(|| invalid("unknown Project identity"))?;
        let committed = self.store.commit(
            ConfigChange {
                scope: project_id,
                command_id,
                principal: "operator",
                expected_revision,
                sections: vec![(PROJECT_DEFAULTS_SECTION, Some(defaults))],
            },
            |candidate| {
                provider_concurrency(
                    candidate
                        .section(PROJECT_DEFAULTS_SECTION)
                        .unwrap_or(&Value::Null),
                )
                .map(|_| ())
            },
        )?;
        let (global, global_revision) = self.read_configuration()?;
        let project_defaults = ProjectConfiguration {
            defaults: committed
                .document
                .section(PROJECT_DEFAULTS_SECTION)
                .cloned()
                .unwrap_or(Value::Null),
        };
        Ok(CanonicalConfigurationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project_id: project.project_id.clone(),
            revision: committed.revision,
            configuration: ProjectConfigurationView {
                project,
                global,
                global_revision,
                project_defaults,
                project_revision: committed.document.revision,
            },
        })
    }

    pub fn set_job_configuration(
        &self,
        command_id: &str,
        job_id: &str,
        config: Value,
        _now: i64,
    ) -> Result<CanonicalJobConfigResponse> {
        if !safe_id(command_id) {
            return Err(invalid("invalid command_id"));
        }
        let mut repository = self.repository_for_job(job_id)?;
        let revision = repository.set_job_configuration(job_id, &config)?;
        Ok(CanonicalJobConfigResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            job_id: job_id.to_string(),
            revision,
            configuration: config,
        })
    }

    pub fn job_configuration(&self, job_id: &str) -> Result<Option<(Value, u64)>> {
        self.repository_for_job(job_id)?.job_configuration(job_id)
    }

    fn project_repository(&self, project_id: &str) -> Result<(ProjectRecord, DomainRepository)> {
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        match self.candidate_repository(&project)? {
            CandidateRepository::Unavailable(reason) => Err(invalid(reason)),
            CandidateRepository::Ready(repository) => Ok((project, repository)),
        }
    }

    /// Resolve the execution disk-space guard for one Project. A live runtime
    /// owns the shared cached guard; operator paths without a runtime get a
    /// one-shot guard over the same root and the same configured thresholds.
    /// Either way the configured policy is applied before the guard is used,
    /// so threshold updates take effect without restart.
    fn disk_guard_for_project(
        &self,
        project_id: &str,
        root: &Path,
        global: &GlobalConfiguration,
    ) -> Result<DiskGuard> {
        let config = disk_guard_config(global);
        if let Some(registry) = &self.runtime_registry {
            if let Some(handle) = registry.handle(project_id)? {
                let guard = handle.disk_guard().clone();
                guard.apply_config(&config)?;
                return Ok(guard);
            }
        }
        if let Some(handle) = &self.runtime_handle {
            if handle.project_root() == root && !handle.is_cancelled() {
                let guard = handle.disk_guard().clone();
                guard.apply_config(&config)?;
                return Ok(guard);
            }
        }
        DiskGuard::for_root(root, config)
    }

    /// The cached disk-guard projection for one Project. Performs no
    /// filesystem I/O: a missing runtime simply means no observation yet.
    fn disk_guard_status(&self, project_id: &str, root: &Path) -> Option<DiskGuardStatus> {
        if let Some(registry) = &self.runtime_registry {
            if let Ok(Some(handle)) = registry.handle(project_id) {
                return Some(handle.disk_guard().status());
            }
        }
        self.runtime_handle
            .as_ref()
            .filter(|handle| handle.project_root() == root && !handle.is_cancelled())
            .map(|handle| handle.disk_guard().status())
    }

    /// Open one registered Project as a candidate owner of a Job lookup.
    ///
    /// Only a registry entry whose boundary itself is gone is reported as
    /// unavailable: a deleted root, an unreachable path, or a marker that no
    /// longer proves this boundary. Everything after that boundary exists is a
    /// real failure — a corrupt store, a failed read or an identity this
    /// boundary no longer owns — and must never be downgraded into "not the
    /// owner", because that would report corruption as an unknown Job.
    fn candidate_repository(&self, project: &ProjectRecord) -> Result<CandidateRepository> {
        let root = Path::new(&project.root);
        if let Some(confinement) = &self.remote_execution {
            if !self
                .security_store()?
                .owns(&confinement.operator_id, &project.project_id)?
            {
                return Ok(CandidateRepository::Unavailable(
                    "Project is not owned by the remote operator",
                ));
            }
            confinement.accepts_root(root)?;
        }
        if !root.is_dir() {
            return Ok(CandidateRepository::Unavailable(
                "registered Project root is unavailable",
            ));
        }
        let canonical_root = match root.canonicalize() {
            Ok(canonical_root) => canonical_root,
            Err(_) => {
                return Ok(CandidateRepository::Unavailable(
                    "registered Project root is unavailable",
                ))
            }
        };
        if canonical_root != root {
            return Ok(CandidateRepository::Unavailable(
                "registered Project root is no longer its canonical path",
            ));
        }
        let repository = DomainRepository::open_existing(root)?;
        if repository.project_at_root(root)?.map(|project| project.id)
            != Some(project.project_id.clone())
        {
            return Err(invalid("Project identity is not registered at this root"));
        }
        Ok(CandidateRepository::Ready(repository))
    }

    pub(crate) fn project_for_job(&self, job_id: &str) -> Result<String> {
        self.repository_for_job(job_id)?
            .job(job_id)?
            .map(|job| job.project_id)
            .ok_or_else(|| invalid("unknown canonical Job"))
    }

    fn repository_for_job(&self, job_id: &str) -> Result<DomainRepository> {
        let project_id = DomainRepository::open_existing(&self.initial_root)?
            .job(job_id)?
            .map(|job| job.project_id)
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        let project = self
            .projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        match self.candidate_repository(&project)? {
            CandidateRepository::Ready(repository) => Ok(repository),
            CandidateRepository::Unavailable(reason) => Err(invalid(reason)),
        }
    }

    pub fn project_usage(
        &self,
        project_id: &str,
        window: crate::contracts::UsageWindow,
    ) -> Result<crate::contracts::ProjectUsageResponse> {
        let (_, repository) = self.project_repository(project_id)?;
        repository.project_usage(project_id, window)
    }

    pub fn conversation_usage(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<crate::contracts::ConversationUsageResponse> {
        let (_, repository) = self.project_repository(project_id)?;
        repository.conversation_usage(project_id, session_id)
    }

    pub fn job_usage(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> Result<crate::contracts::JobUsageResponse> {
        let (_, repository) = self.project_repository(project_id)?;
        repository.job_usage(project_id, job_id)
    }

    pub fn chat_conversations(
        &self,
        project_id: &str,
    ) -> Result<crate::contracts::ChatConversationsResponse> {
        let (_, repository) = self.project_repository(project_id)?;
        let conversations = repository
            .conversations(project_id)?
            .into_iter()
            .filter(|(_, conversation)| conversation.archived_at.is_none())
            .map(|(session_id, conversation)| chat_conversation_view(session_id, conversation))
            .collect();
        Ok(crate::contracts::ChatConversationsResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: project_id.to_string(),
            conversations,
        })
    }

    pub fn delete_chat_conversation(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<crate::contracts::ChatConversationsResponse> {
        let (_, repository) = self.project_repository(project_id)?;
        repository.delete_conversation(project_id, session_id)?;
        self.active_chats
            .lock()
            .map_err(|_| invalid("active chats poisoned"))?
            .remove(&(project_id.to_string(), session_id.to_string()));
        self.chat_conversations(project_id)
    }

    pub fn chat_messages(
        &self,
        project_id: &str,
        session_id: &str,
    ) -> Result<crate::contracts::ChatMessagesResponse> {
        use crate::contracts::{ChatMessageRole, ChatMessageView, ChatMessagesResponse};
        use crate::core_contract::{Actor, MessageBlockKind, MessageLifecycle};
        let (_, repository) = self.project_repository(project_id)?;
        let history = repository
            .conversation_history_with_origins(project_id, session_id)?
            .ok_or_else(|| invalid("unknown conversation in this Project"))?;
        let messages = history
            .messages
            .into_iter()
            .filter(|message| message.state != MessageLifecycle::Deleted)
            .map(|message| {
                let role = match message.author {
                    Actor::User { .. } => ChatMessageRole::User,
                    Actor::Attempt { .. } => ChatMessageRole::Assistant,
                    _ => return Err(invalid("unsupported chat message author")),
                };
                let origin = history
                    .origins
                    .get(message.id.as_str())
                    .ok_or_else(|| invalid("chat message has no canonical turn"))?;
                let replay_job_id = if role == ChatMessageRole::Assistant
                    && matches!(
                        message.state,
                        MessageLifecycle::Pending | MessageLifecycle::Streaming
                    )
                    && self.chat_buffer_for(session_id, &origin.job_id).is_some()
                {
                    Some(origin.job_id.clone())
                } else {
                    None
                };
                Ok(ChatMessageView {
                    message_id: message.id.as_str().to_string(),
                    command_id: origin.command_id.clone(),
                    job_id: Some(origin.job_id.clone()),
                    role: role.clone(),
                    state: message.state,
                    // Dispatch failures are already redacted at the provider boundary.
                    // Diagnostics belong to the view, never to Conversation blocks.
                    // A stop the Core made for a recorded cause names it, even
                    // when the stop was confirmed, so it never reads as an
                    // operator's cancellation.
                    failure_reason: if role == ChatMessageRole::Assistant
                        && message.state == MessageLifecycle::Failed
                    {
                        origin
                            .cause_reason
                            .as_ref()
                            .or(origin
                                .failure_reason
                                .as_ref()
                                .filter(|_| origin.attempt_state != "cancelled"))
                            .map(|reason| reason.chars().take(1024).collect::<String>())
                    } else {
                        None
                    },
                    images: message
                        .blocks
                        .iter()
                        .filter(|block| block.kind == MessageBlockKind::Image)
                        .filter_map(|block| block.raw.clone())
                        .map(serde_json::from_value)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|error| invalid(error.to_string()))?,
                    content: message
                        .blocks
                        .iter()
                        .filter(|block| block.kind == MessageBlockKind::Markdown)
                        .filter_map(|block| block.content.clone())
                        .collect::<Vec<_>>()
                        .join("\n\n"),
                    reasoning: {
                        let reasoning = message
                            .blocks
                            .iter()
                            .filter(|block| block.kind == MessageBlockKind::Reasoning)
                            .filter_map(|block| block.content.as_deref())
                            .collect::<Vec<_>>()
                            .join("\n\n");
                        (!reasoning.is_empty()).then_some(reasoning)
                    },
                    created_at: message.created_at,
                    updated_at: message.updated_at,
                    attempt_state: origin.attempt_state.clone(),
                    replay_job_id,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ChatMessagesResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: project_id.to_string(),
            conversation: chat_conversation_view(session_id.to_string(), history.conversation),
            messages,
        })
    }

    /// The canonical snapshot of one Job, taken at the real journal cursor.
    ///
    /// The read model is the `ExecutionProjection` built from
    /// [`DomainRepository::execution_snapshot`], so the rows and the cursor come
    /// from one read transaction. The cursor is the journal head — a real
    /// `domain_events.seq` position — never a count of entities.
    pub fn canonical_snapshot(
        &self,
        project_id: &str,
        job_id: &str,
    ) -> Result<CanonicalJobSnapshot> {
        let (_, repository) = self.project_repository(project_id)?;
        let mut snapshot = repository.execution_snapshot()?;
        let origins = ChildOrigins::take(&mut snapshot);
        let projection = ExecutionProjection::from(snapshot);
        Self::snapshot_from_projection(project_id, job_id, &projection, &origins, &repository)
    }

    fn snapshot_from_projection(
        project_id: &str,
        job_id: &str,
        projection: &ExecutionProjection,
        origins: &ChildOrigins,
        repository: &DomainRepository,
    ) -> Result<CanonicalJobSnapshot> {
        let job = projection
            .jobs
            .get(job_id)
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        if job.project_id != project_id {
            return Err(invalid("Job does not belong to the requested Project"));
        }
        let attempts: Vec<Attempt> = projection
            .attempts
            .values()
            .filter(|attempt| attempt.job_id == job.id)
            .cloned()
            .collect();
        let calls: Vec<Call> = projection
            .calls
            .values()
            .filter(|call| attempts.iter().any(|attempt| attempt.id == call.attempt_id))
            .cloned()
            .collect();
        // Executors and DispatchIntents are backend-owned canonical entities,
        // not a frontend re-derivation from Calls. An Executor belongs to one
        // Attempt; a DispatchIntent belongs to one Job.
        let executors: Vec<Executor> = projection
            .executors
            .values()
            .filter(|executor| {
                attempts
                    .iter()
                    .any(|attempt| attempt.id == executor.attempt_id)
            })
            .cloned()
            .collect();
        let dispatch_intents: Vec<DispatchIntent> = projection
            .dispatch_intents
            .values()
            .filter(|intent| intent.job_id == job.id)
            .cloned()
            .collect();
        let relations = CanonicalJobRelations::from_projection(job, projection, origins);
        let operations = CanonicalJobOperations::for_job(job, relations.blocked);
        let value = json!({
            "job":CanonicalJobView {
                job,
                relations,
                operations,
            },
            "attempts":attempts,
            "executors":executors,
            "calls":calls,
            "dispatch_intents":dispatch_intents,
            "placement": repository.placement_projection(job_id)?,
            "watchdog": repository.watchdog_actions(job_id)?,
            "execution_graph":"canonical state projection"
        });
        Ok(CanonicalJobSnapshot {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: project_id.to_string(),
            job: value,
            cursor: projection.cursor,
        })
    }

    /// The incremental event tail for one Job, taken from the real journal.
    ///
    /// Every event carries its own `domain_events.seq` as `sequence`, so the
    /// consumer's resume cursor is a position in the canonical execution stream
    /// and nothing else. The outcome is explicit: a complete delta, or a
    /// `ResyncRequired` telling the consumer to refetch
    /// [`CanonicalControlService::canonical_snapshot`] and continue from the
    /// cursor that snapshot reports. A pruned prefix is never served as a
    /// partial suffix and never disguised as a gap.
    pub fn canonical_event_tail(
        &self,
        project_id: &str,
        job_id: &str,
        after: u64,
    ) -> Result<CanonicalEventTail> {
        let (project, repository) = self.project_repository(project_id)?;
        if !repository
            .job(job_id)?
            .is_some_and(|job| job.project_id == project_id)
        {
            return Err(invalid("Job does not belong to the requested Project"));
        }
        match repository.journal_delta_for_job(job_id, after, MAX_EVENT_READ)? {
            EventDelta::Available { events, .. } => Ok(CanonicalEventTail::Events(
                events
                    .into_iter()
                    .map(|event| CanonicalJobEvent {
                        api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                        project_id: project.project_id.clone(),
                        job_id: job_id.to_string(),
                        sequence: event.seq,
                        event_id: event.event_id,
                        kind: event.kind.to_string(),
                        payload: event.payload,
                    })
                    .collect(),
            )),
            // At the head there is nothing to apply; an empty tail is a
            // complete, honest delta rather than a gap.
            EventDelta::Empty { head_cursor: _ } => Ok(CanonicalEventTail::Events(Vec::new())),
            EventDelta::ResyncRequired {
                requested,
                floor_cursor,
                head_cursor,
            } => Ok(CanonicalEventTail::ResyncRequired {
                requested,
                floor_cursor,
                head_cursor,
            }),
            EventDelta::AheadOfHead {
                requested,
                head_cursor,
            } => Ok(CanonicalEventTail::InvalidCursor {
                requested,
                head_cursor,
            }),
        }
    }

    pub fn dashboard(
        &self,
        project_id: &str,
        job_id: Option<&str>,
    ) -> Result<CanonicalDashboardResponse> {
        let (project, repository) = self.project_repository(project_id)?;
        let mut snapshot = repository.execution_snapshot()?;
        let origins = ChildOrigins::take(&mut snapshot);
        let projection = ExecutionProjection::from(snapshot);
        let mut jobs: Vec<CanonicalJobSummary> = projection
            .jobs
            .values()
            .filter(|job| job.project_id == project_id)
            .map(|job| {
                let relations = CanonicalJobRelations::from_projection(job, &projection, &origins);
                CanonicalJobSummary {
                    job_id: job.id.clone(),
                    created_at: job.created_at,
                    state: job.state.to_string(),
                    updated_at: job.updated_at,
                    termination_reason: job.termination_reason.clone(),
                    health_probe: job.spec.health_probe.clone(),
                    operations: CanonicalJobOperations::for_job(job, relations.blocked),
                    relations,
                }
            })
            .collect();
        jobs.sort_by(|left, right| {
            (left.created_at, &left.job_id).cmp(&(right.created_at, &right.job_id))
        });
        let selected_job = job_id
            .map(|id| {
                Self::snapshot_from_projection(project_id, id, &projection, &origins, &repository)
            })
            .transpose()?;
        Ok(CanonicalDashboardResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: project.project_id.clone(),
            jobs,
            selected_job,
            disk_guard: self.disk_guard_status(&project.project_id, Path::new(&project.root)),
        })
    }

    pub(crate) fn start_job_admission_worker(&self) -> Result<JobAdmissionWorker> {
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let service = self.clone();
        let stop = stopped.clone();
        let thread = std::thread::Builder::new()
            .name("job-admission".to_string())
            .spawn(move || {
                let mut last_error = None;
                let mut last_watchdog = Instant::now();
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    match service.admit_eligible_jobs() {
                        Ok(()) => last_error = None,
                        Err(error) => {
                            let message = error.to_string();
                            if last_error.as_ref() != Some(&message) {
                                tracing::error!(%error, "automatic Job admission deferred");
                                last_error = Some(message);
                            }
                        }
                    }
                    // Watchdog shares this worker's lifetime and the Project
                    // runtimes it already starts. It does not get its own loop
                    // at the admission cadence: one pass every few seconds is a
                    // reconciliation, not a second scheduler.
                    if last_watchdog.elapsed() >= super::watchdog::WATCHDOG_INTERVAL {
                        last_watchdog = Instant::now();
                        if let Err(error) = service.reconcile_watchdog() {
                            tracing::error!(%error, "fleet watchdog reconciliation deferred");
                        }
                        if let Err(error) = service.enforce_chat_execution_deadlines() {
                            tracing::error!(%error, "chat execution deadline enforcement deferred");
                        }
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
            .map_err(|error| invalid(format!("start Job admission worker: {error}")))?;
        Ok(JobAdmissionWorker {
            stopped,
            thread: Some(thread),
            runtimes: self.runtime_registry.clone(),
        })
    }

    /// One Watchdog pass over every registered Project.
    ///
    /// The pass only opens stores that already exist and only signals a runtime
    /// that admission has already started. It never starts a provider worker
    /// merely to ask whether one owns a Call.
    /// This process's runtime for a Project, only while it owns the Project's
    /// execution. It never acquires ownership, so a background pass acts only
    /// on Projects this process already owns and leaves another process's
    /// live Attempts untouched.
    fn owned_runtime(
        &self,
        project_id: &str,
        root: &Path,
    ) -> Result<Option<crate::orchestration::execution_runtime::ExecutionRuntimeHandle>> {
        let handle = if let Some(registry) = &self.runtime_registry {
            registry.handle(project_id)?
        } else {
            self.runtime_handle
                .clone()
                .filter(|handle| handle.project_root() == root)
        };
        Ok(handle.filter(|handle| handle.owns_execution()))
    }

    /// This process's runtime for a Project, acquiring the Project's execution
    /// ownership if no runtime holds it yet. Starting a runtime runs startup
    /// recovery once ownership is held; another live owner is refused with
    /// nothing recovered. Every explicit execution operation goes through here.
    fn execution_owner(
        &self,
        project_id: &str,
        root: &Path,
    ) -> Result<crate::orchestration::execution_runtime::ExecutionRuntimeHandle> {
        if let Some(registry) = &self.runtime_registry {
            let (defaults, _) = self.project_defaults(project_id)?;
            let concurrency = provider_concurrency(&defaults.defaults)?;
            return registry.get_or_start(project_id, root, concurrency);
        }
        self.runtime_handle
            .clone()
            .filter(|handle| handle.project_root() == root && handle.owns_execution())
            .ok_or_else(|| invalid("execution runtime is not available for this Project"))
    }

    pub(crate) fn reconcile_watchdog(&self) -> Result<()> {
        let mut failure = None;
        let global = self
            .read_configuration()
            .map(|(global, _)| global)
            .unwrap_or_default();
        // An unreadable Project registry skips this pass's reconciliation, but
        // never the notification of settlements already committed below.
        let projects = self.projects().unwrap_or_else(|error| {
            failure.get_or_insert(error);
            Vec::new()
        });
        for project in projects {
            // Watchdog reconciliation doubles as a bounded disk reassessment
            // point. Fencing itself never consults the Guard — reconciliation
            // is essential settlement — but replacement execution always flows
            // back through Admission, where the Guard blocks it until recovery.
            if let Ok(guard) =
                self.disk_guard_for_project(&project.project_id, Path::new(&project.root), &global)
            {
                guard.refresh_if_stale();
            }
            // Watchdog classifies an Attempt whose token this process cannot
            // see as disappeared. That is only true for a Project this process
            // owns: another owner's live Attempts are invisible here, not lost.
            let runtime = match self.owned_runtime(&project.project_id, Path::new(&project.root)) {
                Ok(Some(runtime)) => runtime,
                Ok(None) => continue,
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
            };
            let mut domain = match DomainRepository::open_existing(Path::new(&project.root)) {
                Ok(domain) => domain,
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
            };
            if let Err(error) = super::watchdog::reconcile_repository(&mut domain, Some(&runtime)) {
                failure.get_or_insert(error);
            }
        }
        // Recovery commits per Job, and a later failure in the same pass
        // discards the results already committed. Chat tails are therefore
        // ended from committed state, after every pass, whatever it returned.
        self.publish_watchdog_settlements();
        failure.map_or(Ok(()), Err)
    }

    /// End every live Chat tail whose Attempt Watchdog has settled because its
    /// executor disappeared.
    ///
    /// This reads committed state, not the outcome of any one pass, so a
    /// settlement whose pass later failed is still announced on the next pass.
    /// A tail is ended only when its own Attempt is terminal, its Job still
    /// names that generation and holds no authority, and the Job records
    /// Watchdog's reason. An Attempt another actor settled publishes its own
    /// terminal event and is left alone. A tail receives at most one terminal:
    /// one already terminal is skipped, and the forwarder stops at the first.
    fn publish_watchdog_settlements(&self) {
        let live: Vec<(String, String, String, std::sync::Arc<ChatEventBuffer>)> =
            match self.active_chats.lock() {
                Ok(chats) => chats
                    .values()
                    .filter(|chat| !chat.attempt_id.is_empty())
                    .filter(|chat| chat.buffer.state.lock().is_ok_and(|state| !state.terminal))
                    .map(|chat| {
                        (
                            chat.project_id.clone(),
                            chat.job_id.clone(),
                            chat.attempt_id.clone(),
                            chat.buffer.clone(),
                        )
                    })
                    .collect(),
                Err(_) => return,
            };
        for (project_id, job_id, attempt_id, buffer) in live {
            let committed = (|| -> Result<Option<String>> {
                // A live tail's turn runs on this process's own runtime, so its
                // store is reached through that runtime's root rather than the
                // Project registry, which may be what failed this pass. Attempt
                // identities are unique, so a store that does not hold this
                // Attempt yields nothing.
                let runtime = if let Some(registry) = &self.runtime_registry {
                    registry.handle(&project_id)?
                } else {
                    self.runtime_handle.clone()
                };
                let Some(runtime) = runtime else {
                    return Ok(None);
                };
                let domain = DomainRepository::open_existing(runtime.project_root())?;
                let Some(attempt) = domain.attempt(&attempt_id)? else {
                    return Ok(None);
                };
                if attempt.job_id.as_str() != job_id
                    || matches!(
                        attempt.state,
                        super::domain::AttemptState::Queued
                            | super::domain::AttemptState::Running
                            | super::domain::AttemptState::Cancelling
                    )
                {
                    return Ok(None);
                }
                Ok(domain
                    .job(&job_id)?
                    .filter(|job| {
                        job.generation == attempt.generation
                            && job.authoritative_attempt_id.is_none()
                    })
                    .and_then(|job| job.termination_reason)
                    .filter(|reason| reason.code == "watchdog_executor_disappeared")
                    .map(|reason| reason.message))
            })();
            let message = match committed {
                Ok(Some(message)) => message,
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(%error, attempt_id, "watchdog settlement could not be read back");
                    continue;
                }
            };
            if let Ok(chats) = self.active_chats.lock() {
                for chat in chats.values().filter(|chat| {
                    chat.attempt_id == attempt_id && std::sync::Arc::ptr_eq(&chat.buffer, &buffer)
                }) {
                    chat.cancelled.cancel();
                    if let Err(error) = chat.sender.send(ExecutionEvent::Failed(message.clone())) {
                        tracing::debug!(%error, "watchdog-settled Job chat receiver closed");
                    }
                }
            }
        }
    }

    /// Cancel every Chat-root Job whose current Attempt outlived the Chat
    /// execution deadline.
    ///
    /// The deadline is a Chat policy enforced here, independently of any
    /// stream: each stop goes through [`Self::cancel_job`]'s lifecycle and
    /// records the timeout as its cause. A cancellation that is rejected
    /// (the Job settled or moved on) records and publishes nothing.
    pub(crate) fn enforce_chat_execution_deadlines(&self) -> Result<()> {
        let cutoff = crate::clock::Clock::now_unix(&crate::clock::SystemClock)
            - CHAT_EXECUTION_TIMEOUT.as_secs() as i64;
        let mut failure = None;
        for project in self.projects()? {
            // Only the owning process enforces the deadline; its runtime is the
            // one that can signal the execution it cancels.
            match self.owned_runtime(&project.project_id, Path::new(&project.root)) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
            }
            let due = match DomainRepository::open_existing(Path::new(&project.root))
                .and_then(|domain| domain.chat_execution_deadline_due(cutoff))
            {
                Ok(due) => due,
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
            };
            for (job_id, generation) in due {
                if let Err(error) = self.cancel_job_for_cause(
                    &job_id,
                    generation,
                    Some(super::domain::CancelCause::ChatExecutionTimeout),
                ) {
                    tracing::debug!(%error, job_id, "chat execution deadline cancellation rejected");
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(crate) fn admit_eligible_jobs(&self) -> Result<()> {
        let _launch = self
            .launch_lock
            .lock()
            .map_err(|_| invalid("launch lock poisoned"))?;
        // Thresholds are re-read every pass so configuration updates take
        // effect without restart; observation refresh is cadence-gated inside
        // the Guard, so most passes are a cached read.
        let (global, _) = self.read_configuration().unwrap_or_default();
        let configuration_revision = self.store.active_revision().unwrap_or_default();
        let mut failure = None;
        let mut selected_jobs = std::collections::BTreeSet::new();
        let mut profile_revision = None;
        for project in self.projects()? {
            // Periodic runtime reassessment while OCG is active. Deferred Jobs
            // stay eligible in the substrate, so recovery past the resume
            // reserve lets this same worker admit them again with no new
            // scheduler and no operator action.
            if let Ok(guard) =
                self.disk_guard_for_project(&project.project_id, Path::new(&project.root), &global)
            {
                guard.refresh_if_stale();
            }
            let result = (|| -> Result<()> {
                let (_, mut domain) = self.project_repository(&project.project_id)?;
                // Serialize the complete Attempt-to-Call handoff among this
                // process's consumers of one Project. Other processes are
                // excluded by Project ownership, taken below before anything
                // is admitted, and SQLite still guards every authoritative
                // claim independently. Other Projects are never blocked.
                let Some(_admitting) =
                    AdmissionGuard::try_acquire(&self.admitting, &project.project_id)?
                else {
                    return Ok(());
                };
                let jobs = domain.automatic_admission_jobs(&project.project_id)?;
                let unpublished = domain.unpublished_admissions(&project.project_id)?;
                if jobs.is_empty()
                    && unpublished.is_empty()
                    && domain.pending_dispatch_intents()?.is_empty()
                {
                    return Ok(());
                }
                // Admission acts only for a Project this process owns. With
                // work waiting it takes that ownership, which first runs
                // startup recovery; a Project another process owns is refused
                // here, before any of its Jobs is touched.
                self.execution_owner(&project.project_id, Path::new(&project.root))?;
                // Recovery disposition of an Attempt committed without a
                // published Call is decided by its frozen input alone. Only a
                // worker-owned reservation whose input is re-derivable resumes,
                // and it resumes by publishing exactly that input. Everything
                // else settles as failed: nothing reached a provider, and an
                // explicit retry replays the frozen input if one exists.
                let mut interrupted = std::collections::BTreeSet::new();
                for (job_id, attempt_id, automatic) in unpublished {
                    let generation = domain
                        .attempt(&attempt_id)?
                        .ok_or_else(|| invalid("unpublished Attempt disappeared"))?
                        .generation;
                    let resumable = automatic
                        && domain
                            .job_input(&job_id, generation)?
                            .is_some_and(|frozen| {
                                frozen.input.is_ok()
                                    && frozen.replay == super::domain::InputReplay::Replaceable
                            });
                    if resumable {
                        continue;
                    }
                    domain.fail_attempt(
                        &attempt_id,
                        &super::domain::job_failure(
                            "admission_interrupted",
                            FailureClass::Internal,
                            "admission was interrupted before its provider Call was published; nothing was sent to the provider",
                            true,
                        ),
                    )?;
                    interrupted.insert(job_id);
                }
                for job in jobs {
                    if interrupted.contains(&job.id) {
                        continue;
                    }
                    selected_jobs.insert(job.id.clone());
                    if job.state == super::domain::JobState::Eligible {
                        let mut deferrals = self
                            .placement_deferrals
                            .lock()
                            .map_err(|_| invalid("placement deferrals poisoned"))?;
                        if let Some(deferred) = deferrals.get(&job.id) {
                            let revision = match &profile_revision {
                                Some(revision) => revision,
                                None => profile_revision
                                    .insert(self.profile_service.content_revision()?),
                            };
                            if deferred.root == Path::new(&project.root)
                                && revision.as_ref() == Some(&deferred.profile_revision)
                                && deferred.configuration_revision == configuration_revision
                                && deferred.generation == job.generation
                                && deferred.spec == job.spec
                                && deferred.job_configuration_revision
                                    == domain
                                        .job_configuration(&job.id)?
                                        .map(|(_, revision)| revision)
                            {
                                continue;
                            }
                        }
                        deferrals.remove(&job.id);
                    }
                    let request = crate::contracts::JobLaunchRequest {
                        command_id: format!("auto-{}", job.id),
                        draft_id: job.id.clone(),
                        project_id: job.project_id.clone(),
                        session_id: String::new(),
                        objective: job.spec.objective.clone().unwrap_or_default(),
                        success_criteria: job.spec.success_criteria.clone(),
                        constraints: job.spec.constraints.clone(),
                        hard_budget_micros: job.spec.hard_budget_micros.unwrap_or(0),
                        resource_commitment: job.spec.resource_commitment,
                    };
                    match self.launch_job_inner_for_existing(request, Some(job)) {
                        Ok(response) if response.outcome == "accepted" => {}
                        Ok(response) => {
                            failure.get_or_insert_with(|| invalid(response.message));
                        }
                        Err(error) => {
                            failure.get_or_insert(error);
                        }
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        self.placement_deferrals
            .lock()
            .map_err(|_| invalid("placement deferrals poisoned"))?
            .retain(|job_id, _| selected_jobs.contains(job_id));
        failure.map_or(Ok(()), Err)
    }

    pub fn launch_job(
        &self,
        request: crate::contracts::JobLaunchRequest,
        _now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        let _launch = self
            .launch_lock
            .lock()
            .map_err(|_| invalid("launch lock poisoned"))?;
        self.launch_job_inner(request)
    }

    pub fn spawn_job(
        &self,
        parent_job_id: &str,
        request: CanonicalJobSpawnRequest,
    ) -> Result<CanonicalJobSpawnResponse> {
        let mut domain = self.repository_for_job(parent_job_id)?;
        // Recursive fan-out multiplies future durable writes, so the Guard is
        // consulted before the spawn transaction commits anything. The refusal
        // happens before any lineage, dependency, counter or idempotency-key
        // write: a blocked spawn key stays unbound and the same logical spawn
        // can succeed after recovery.
        {
            let parent = domain
                .job(parent_job_id)?
                .ok_or_else(|| invalid("unknown parent Job"))?;
            let root = self
                .read_projects()?
                .into_iter()
                .find(|project| project.project_id == parent.project_id)
                .map(|project| project.root)
                .ok_or_else(|| invalid("unknown Project identity"))?;
            let (global, _) = self.read_configuration()?;
            let guard =
                self.disk_guard_for_project(&parent.project_id, Path::new(&root), &global)?;
            if let Some(deferral) = guard.check_amplifying_expansion().deferral() {
                return Err(OcgError::spawn_refused(
                    crate::error::SpawnRefusalReason::StorageProtection,
                    deferral.message(),
                ));
            }
        }
        let authority = AttemptAuthority {
            job_id: parent_job_id.to_string(),
            attempt_id: request.parent_attempt_id,
            generation: request.expected_generation,
        };
        let prerequisites = request
            .depends_on
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let (child, duplicate) =
            domain.spawn_child(crate::orchestration::domain::SpawnChildRequest {
                parent_authority: &authority,
                spawn_key: &request.spawn_key,
                spec: request.spec,
                prerequisite_job_ids: &prerequisites,
                executor_kind: &request.executor_kind,
                policy: &request.policy,
                call_id: request.call_id.as_deref(),
            })?;
        Ok(CanonicalJobSpawnResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            snapshot: self.canonical_snapshot(&child.project_id, &child.id)?,
            child_job_id: child.id,
            duplicate,
        })
    }

    /// Create an orchestration root Job on behalf of an authorized principal.
    pub fn create_orchestration(
        &self,
        principal: &JobPrincipal,
        request: CanonicalOrchestrationRequest,
    ) -> Result<CanonicalJobManagementResponse> {
        if !safe_id(&request.command_id) || !safe_id(&request.project_id) {
            return Err(invalid("invalid command_id or project_id"));
        }
        let (project, mut domain) = self.project_repository(&request.project_id)?;
        let (global, _) = self.read_configuration()?;
        let guard =
            self.disk_guard_for_project(&project.project_id, Path::new(&project.root), &global)?;
        if let Some(deferral) = guard.check_execution_admission().deferral() {
            return Err(invalid(deferral.message()));
        }
        let canonical_project = domain
            .project_at_root(Path::new(&project.root))?
            .ok_or_else(|| invalid("registered Project identity is not durable at this root"))?;
        let request_hash = command_hash(&("orchestration", &request))?;
        let (job, duplicate) = domain.create_orchestration_job(
            &canonical_project.id,
            request.spec,
            principal,
            &request.command_id,
            &request_hash,
        )?;
        Ok(CanonicalJobManagementResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            snapshot: self.canonical_snapshot(&job.project_id, &job.id)?,
            job_id: job.id,
            duplicate,
        })
    }

    /// Attach child work to an open orchestration Job on behalf of an
    /// authorized principal. No parent Attempt is involved.
    pub fn attach_job(
        &self,
        principal: &JobPrincipal,
        parent_job_id: &str,
        request: CanonicalJobAttachRequest,
    ) -> Result<CanonicalJobManagementResponse> {
        if !safe_id(&request.command_id) {
            return Err(invalid("invalid command_id"));
        }
        let mut domain = self.repository_for_job(parent_job_id)?;
        let parent = domain
            .job(parent_job_id)?
            .ok_or_else(|| invalid("unknown parent Job"))?;
        // Attachment fans durable work out exactly as a spawn does, so the
        // same storage gate refuses it before anything is written.
        {
            let (project, _) = self.project_repository(&parent.project_id)?;
            let (global, _) = self.read_configuration()?;
            let guard =
                self.disk_guard_for_project(&parent.project_id, Path::new(&project.root), &global)?;
            if let Some(deferral) = guard.check_amplifying_expansion().deferral() {
                return Err(OcgError::spawn_refused(
                    crate::error::SpawnRefusalReason::StorageProtection,
                    deferral.message(),
                ));
            }
        }
        let request_hash = command_hash(&("attach", parent_job_id, &request))?;
        let prerequisites = request
            .depends_on
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let (child, duplicate) =
            domain.attach_child(crate::orchestration::domain::AttachChildRequest {
                parent_job_id,
                principal,
                command_id: &request.command_id,
                request_hash: &request_hash,
                spec: request.spec,
                prerequisite_job_ids: &prerequisites,
                policy: &request.policy,
            })?;
        Ok(CanonicalJobManagementResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            snapshot: self.canonical_snapshot(&child.project_id, &child.id)?,
            job_id: child.id,
            duplicate,
        })
    }

    /// Close an orchestration Job to further attachment. Its completion then
    /// follows its required children.
    pub fn close_job(&self, job_id: &str) -> Result<CanonicalJobOperationResponse> {
        let mut domain = self.repository_for_job(job_id)?;
        let job = domain.close_orchestration_job(job_id)?;
        Ok(CanonicalJobOperationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            accepted: true,
            snapshot: self.canonical_snapshot(&job.project_id, job_id)?,
            job_id: job.id,
        })
    }

    pub fn cancel_job(
        &self,
        job_id: &str,
        expected_generation: u64,
    ) -> Result<CanonicalJobOperationResponse> {
        self.cancel_job_for_cause(job_id, expected_generation, None)
    }

    /// Cancel through the one canonical cancellation lifecycle, recording the
    /// Core's `cause` when it, not an operator, initiated the stop.
    fn cancel_job_for_cause(
        &self,
        job_id: &str,
        expected_generation: u64,
        cause: Option<super::domain::CancelCause>,
    ) -> Result<CanonicalJobOperationResponse> {
        let mut domain = self.repository_for_job(job_id)?;
        let job = domain
            .job(job_id)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        let mut accepted = job.state.can_cancel();
        // Cancellation revokes and settles execution authority, so only the
        // Project's owner performs it, through the runtime that can signal the
        // execution. A Project another process owns is refused before any
        // cancellation is requested.
        let (project, _) = self.project_repository(&job.project_id)?;
        let runtime = self.execution_owner(&job.project_id, Path::new(&project.root))?;
        // The cause is recorded with the request, in one transaction; a
        // Core-initiated stop finds nothing to request once anyone else has.
        let requested = domain.request_job_cancel_cascade_for(
            job_id,
            expected_generation,
            cause.unwrap_or(super::domain::CancelCause::Operator),
        )?;
        if cause.is_some() && requested.is_empty() {
            accepted = false;
        }
        for (authority, never_started) in requested {
            let signalled = runtime
                .provider_dispatcher()
                .cancel_attempt(&authority.attempt_id)?;
            domain.confirm_cancel(&authority.attempt_id, signalled || never_started)?;
            // The stream reports the committed outcome: an operator's stop
            // stays "chat cancelled", and a Core-initiated stop names its
            // cause and whether the stop was confirmed.
            let outcome = match cause {
                None => "chat cancelled".to_string(),
                Some(cause) => domain
                    .job(&authority.job_id)?
                    .and_then(|job| job.termination_reason)
                    .map_or_else(|| cause.summary().to_string(), |reason| reason.message),
            };
            if let Ok(chats) = self.active_chats.lock() {
                for chat in chats
                    .values()
                    .filter(|chat| chat.attempt_id == authority.attempt_id)
                {
                    chat.cancelled.cancel();
                    if let Err(error) = chat.sender.send(ExecutionEvent::Failed(outcome.clone())) {
                        tracing::debug!(%error, "cancelled Job chat receiver closed");
                    }
                }
            }
        }
        Ok(CanonicalJobOperationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            job_id: job.id,
            accepted,
            snapshot: self.canonical_snapshot(&job.project_id, job_id)?,
        })
    }

    /// The request, protocol and execution target that replay a Job
    /// generation's frozen execution input exactly, or a refusal.
    ///
    /// Nothing is substituted: the frozen provider, model, upstream model,
    /// protocol, endpoint and credential reference must all still be what the
    /// Profile configures, the Effort and images must still be supported, and
    /// every image must still resolve. Only the credential value behind the
    /// same Vault reference may have changed.
    fn frozen_input_replay(
        &self,
        domain: &DomainRepository,
        project_id: &str,
        frozen: &super::domain::FrozenJobInput,
    ) -> Result<(
        Value,
        crate::provider_protocol::ProviderProtocol,
        crate::orchestration::execution_dispatch::ProviderExecutionConfig,
    )> {
        let refuse = |why: String| {
            invalid(format!(
                "replay refused: {why}; the original request is never replayed against a different target"
            ))
        };
        let launch = frozen.input.as_ref().map_err(|why| refuse(why.clone()))?;
        let target = &launch.target;
        let (profile, _) = self
            .profile_service
            .current()?
            .ok_or_else(|| invalid("profile not configured"))?;
        let provider = profile.providers.get(&target.provider_key).ok_or_else(|| {
            refuse(format!(
                "provider {} is no longer configured",
                target.provider_key
            ))
        })?;
        let model = profile
            .models
            .get(&target.model_key)
            .filter(|model| model.provider == target.provider_key)
            .ok_or_else(|| {
                refuse(format!(
                    "model {} is no longer configured for provider {}",
                    target.model_key, target.provider_key
                ))
            })?;
        if model.id != target.upstream_model_id {
            return Err(refuse(format!(
                "model {} now names upstream model {} instead of {}",
                target.model_key, model.id, target.upstream_model_id
            )));
        }
        if provider.wire_protocol() != target.protocol {
            return Err(refuse(format!(
                "provider {} now speaks {} instead of {}",
                target.provider_key,
                provider.wire_protocol(),
                target.protocol
            )));
        }
        if provider.endpoint.as_deref() != Some(target.endpoint.as_str()) {
            return Err(refuse(format!(
                "the endpoint of provider {} changed",
                target.provider_key
            )));
        }
        if provider.credential_ref != target.credential_ref {
            return Err(refuse(format!(
                "the credential reference of provider {} changed",
                target.provider_key
            )));
        }
        if let Some(reference) = target.credential_ref.as_deref() {
            if !self.store.credential_names()?.contains(reference) {
                return Err(refuse(format!("credential {reference} is not stored")));
            }
        }
        if let Some(refusal) = chat_capability_refusal(
            provider,
            model,
            launch.effort.as_deref(),
            launch.has_images(),
        ) {
            return Err(refuse(refusal.to_string()));
        }
        let messages = domain
            .resolve_job_input_messages(project_id, launch)
            .map_err(|error| refuse(format!("an original image cannot be read ({error})")))?;
        let mut request = json!({
            "model": target.model_key,
            "messages": messages,
            "stream": true,
        });
        if let Some(effort) = &launch.effort {
            request["reasoning_effort"] = json!(effort);
        }
        Ok((
            request,
            target.protocol,
            crate::orchestration::execution_dispatch::ProviderExecutionConfig {
                provider_key: target.provider_key.clone(),
                model: target.model_key.clone(),
                upstream_model_id: target.upstream_model_id.clone(),
                endpoint: target.endpoint.clone(),
                credential_ref: target.credential_ref.clone(),
            },
        ))
    }

    pub fn retry_job(
        &self,
        job_id: &str,
        expected_generation: u64,
    ) -> Result<CanonicalJobOperationResponse> {
        let _launch = self
            .launch_lock
            .lock()
            .map_err(|_| invalid("launch lock poisoned"))?;
        let mut domain = self.repository_for_job(job_id)?;
        let job = domain
            .job(job_id)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        if job.generation != expected_generation
            || !job.state.can_retry()
            || job.authoritative_attempt_id.is_some()
        {
            return Err(invalid("retry rejected: Job state or generation changed"));
        }
        if !domain.dependencies_satisfied(&job.project_id, job_id)? {
            return Err(invalid("Job dependencies are not satisfied"));
        }
        // Disk safety precedes the replacement Attempt: an explicit operator
        // retry is not permission to exhaust the host disk. The Job stays
        // retryable and deferred; no execution is created here.
        let retry_guard = {
            let (project, _) = self.project_repository(&job.project_id)?;
            let (global, _) = self.read_configuration()?;
            self.disk_guard_for_project(&job.project_id, Path::new(&project.root), &global)?
        };
        if let Some(deferral) = retry_guard.check_execution_admission().deferral() {
            return Err(invalid(deferral.message()));
        }
        // A retry mints a replacement Attempt, so only the Project's owner may
        // perform it. Taking ownership before the replacement is read or
        // written runs startup recovery first when this process did not yet
        // own the Project.
        let owner = {
            let (project, _) = self.project_repository(&job.project_id)?;
            self.execution_owner(&job.project_id, Path::new(&project.root))?
        };
        let snapshot = domain.execution_snapshot()?;
        // Chat identity is durable: the turn's live transport may already have
        // been consumed by `finish_chat`, while the turn itself stays retryable.
        let chat_session = domain.chat_session_for_job(job_id)?;
        if let Some((_, session_id)) = &chat_session {
            // Retry re-enters this turn; it never displaces another live turn
            // of the same Conversation.
            let other = self
                .active_chats
                .lock()
                .map_err(|_| invalid("active chats poisoned"))?
                .get(&(job.project_id.clone(), session_id.clone()))
                .map(|chat| chat.job_id.clone())
                .filter(|other| other != job_id);
            if let Some(other) = other {
                if other.is_empty()
                    || domain
                        .job(&other)?
                        .is_some_and(|other| other.authoritative_attempt_id.is_some())
                {
                    return Err(invalid(
                        "retry rejected: another turn in this Conversation is running",
                    ));
                }
            }
        }
        let template = snapshot
            .dispatch_intents
            .iter()
            .filter(|intent| intent.job_id == job_id && intent.provider_key.is_some())
            .min_by_key(|intent| (intent.generation, intent.created_at, &intent.id));
        let frozen = domain.latest_job_input(job_id)?;
        let runtime = (template.is_some() || frozen.is_some()).then_some(owner);
        let replay = template
            .map(|intent| -> Result<_> {
                let input: Value = serde_json::from_str(&intent.request)
                    .map_err(|error| invalid(error.to_string()))?;
                let request = input
                    .get("arguments")
                    .filter(|value| value.is_object())
                    .cloned()
                    .ok_or_else(|| invalid("durable provider input is missing"))?;
                let protocol = crate::provider_protocol::ProviderProtocol::from_wire(
                    input.get("provider_protocol").and_then(Value::as_str),
                );
                let config = crate::orchestration::execution_dispatch::ProviderExecutionConfig {
                    provider_key: intent
                        .provider_key
                        .clone()
                        .ok_or_else(|| invalid("durable provider is missing"))?,
                    model: intent
                        .model
                        .clone()
                        .ok_or_else(|| invalid("durable model is missing"))?,
                    upstream_model_id: intent
                        .upstream_model_id
                        .clone()
                        .ok_or_else(|| invalid("durable upstream model is missing"))?,
                    endpoint: intent
                        .endpoint
                        .clone()
                        .ok_or_else(|| invalid("durable endpoint is missing"))?,
                    credential_ref: intent.credential_ref.clone(),
                };
                Ok((request, protocol, config))
            })
            .transpose()?;
        // Replay precedence: a provider DispatchIntent's frozen request is what
        // actually ran, so it stays authoritative once it exists. Before any
        // Call, the Job's frozen execution input is what was asked. A provider
        // Job with neither is refused below rather than given an invented
        // request, whatever origin created it.
        let replay = match replay {
            Some(replay) => Some(replay),
            None => frozen
                .as_ref()
                .map(|frozen| self.frozen_input_replay(&domain, &job.project_id, frozen))
                .transpose()?,
        };
        let previous_kind = snapshot
            .executors
            .iter()
            .find(|executor| {
                snapshot.attempts.iter().any(|attempt| {
                    attempt.id == executor.attempt_id
                        && attempt.job_id == job_id
                        && attempt.generation == job.generation
                })
            })
            .map(|executor| executor.kind.as_str());
        if replay.is_none() && (chat_session.is_some() || previous_kind == Some("provider")) {
            return Err(invalid(
                "retry refused: this Job has no frozen execution input to replay; launch it again",
            ));
        }
        let executor_kind = if replay.is_some() {
            "provider"
        } else {
            previous_kind.unwrap_or("worker")
        };
        // Resolve everything the replacement needs before it is admitted, so a
        // refusal here leaves the Job exactly as it was.
        let replay = replay
            .map(|(request, protocol, provider_config)| -> Result<_> {
                let runtime = runtime
                    .clone()
                    .ok_or_else(|| invalid("retry runtime disappeared"))?;
                // Retry does not choose a new candidate. It republishes the Call
                // against the target the previous Attempt already froze.
                let (profile, _) = self
                    .profile_service
                    .current()?
                    .ok_or_else(|| invalid("profile not configured"))?;
                let (project, _) = self.project_repository(&job.project_id)?;
                let resolved = super::admission::ResolvedTarget {
                    protocol,
                    config: provider_config,
                    effort: request
                        .get("reasoning_effort")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    reservation: super::admission::AdmissionReservation {
                        choice: super::placement::ProviderChoice {
                            provider: job.spec.provider.clone().unwrap_or_default(),
                            model: job.spec.model.clone().unwrap_or_default(),
                        },
                        pickup: if job.spec.health_probe.is_some() {
                            super::admission::AdmissionPickup::Explicit
                        } else {
                            super::admission::AdmissionPickup::Automatic
                        },
                        exact: job.spec.health_probe.as_ref().map(|intent| {
                            super::admission::ExactTarget {
                                provider: intent.provider.clone(),
                                model: intent.model.clone(),
                                effort: intent.effort.clone(),
                            }
                        }),
                    },
                };
                Ok((request, runtime, profile, project, resolved))
            })
            .transpose()?;
        let (global, _) = self.read_configuration()?;
        let budget_config = self.provider_budget_config(&global)?;
        // The replacement Attempt and the reactivated assistant Message commit
        // together; the turn and its Message keep their identity.
        let admission = domain.retry_job(job_id, executor_kind, expected_generation)?;
        // A Chat replacement executes only through a retained transport: it is
        // installed and consumed before its Call can be published, and a
        // replacement that cannot get one never runs.
        let chat = match chat_session {
            Some((project_id, session_id)) => {
                match self.install_chat_retry_transport(
                    project_id,
                    session_id,
                    job_id,
                    &admission.attempt.id,
                ) {
                    Ok(transport) => Some(transport),
                    Err(error) => {
                        Self::fail_unpublished_retry(&mut domain, &admission.attempt.id, &error)?;
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        if let Some((request, runtime, profile, project, resolved)) = replay {
            let published = publish_call(
                &mut AdmissionContext {
                    domain: &mut domain,
                    profile: &profile,
                    root: Path::new(&project.root),
                    project_id: &project.project_id,
                    budget: &budget_config,
                    concurrency: None,
                    governor: None,
                    disk_guard: Some(retry_guard.clone()),
                    now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
                },
                &admission.job,
                &resolved,
                &super::admission::ReservedExecution {
                    attempt: admission.attempt.clone(),
                    executor: admission.executor.clone(),
                    existing_call: false,
                    messages: None,
                },
                PreparedExecution {
                    request,
                    payload: if job.spec.health_probe.is_some() {
                        PayloadMode::Probe
                    } else {
                        PayloadMode::Agent
                    },
                    event_sender: chat.as_ref().map(|transport| transport.sender.clone()),
                    cancelled: chat
                        .as_ref()
                        .map(|transport| transport.cancelled.clone())
                        .unwrap_or_default(),
                },
                &runtime,
            )
            .and_then(|published| published.map_err(|refusal| invalid(refusal.message)));
            if let Err(error) = published {
                if let Some(transport) = chat {
                    self.abandon_chat_retry(&mut domain, &admission.attempt.id, transport, &error)?;
                }
                return Err(error);
            }
        }
        if let Some(superseded) = chat.and_then(|transport| transport.superseded) {
            // The previous tail belongs to a settled generation; its stream
            // has nothing left to deliver.
            superseded.cancelled.cancel();
        }
        Ok(CanonicalJobOperationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            job_id: job.id,
            accepted: true,
            snapshot: self.canonical_snapshot(&job.project_id, job_id)?,
        })
    }

    /// Install the replacement turn's transport for a Chat retry: a fresh
    /// ActiveChat for the session and a retained consumer of its events.
    ///
    /// Nothing is published yet. On failure the session holds exactly the
    /// tail it had before, and the caller must settle the replacement.
    fn install_chat_retry_transport(
        &self,
        project_id: String,
        session_id: String,
        job_id: &str,
        attempt_id: &str,
    ) -> Result<ChatRetryTransport> {
        let (sender, receiver) = flume::unbounded();
        let cancelled = CallCancellation::new();
        let buffer = Arc::new(ChatEventBuffer::default());
        let key = (project_id.clone(), session_id.clone());
        let superseded = self
            .active_chats
            .lock()
            .map_err(|_| invalid("active chats poisoned"))?
            .insert(
                key.clone(),
                ActiveChat {
                    project_id,
                    session_id,
                    job_id: job_id.to_string(),
                    attempt_id: attempt_id.to_string(),
                    cancelled: cancelled.clone(),
                    sender: sender.clone(),
                    buffer: buffer.clone(),
                },
            );
        let transport = ChatRetryTransport {
            key,
            sender,
            cancelled,
            superseded,
        };
        if let Err(error) = self.chat_forwarder.register(receiver, buffer) {
            transport.cancelled.cancel();
            self.release_chat_retry_transport(attempt_id, transport)?;
            return Err(error);
        }
        Ok(transport)
    }

    /// Remove a replacement's ActiveChat and give the session back the tail
    /// it superseded. Dropping the last sender disconnects the forwarder's
    /// registration, which then drains on its own.
    fn release_chat_retry_transport(
        &self,
        attempt_id: &str,
        transport: ChatRetryTransport,
    ) -> Result<()> {
        let mut chats = self
            .active_chats
            .lock()
            .map_err(|_| invalid("active chats poisoned"))?;
        if chats
            .get(&transport.key)
            .is_some_and(|chat| chat.attempt_id == attempt_id)
        {
            chats.remove(&transport.key);
        }
        if let Some(superseded) = transport.superseded {
            chats.entry(transport.key).or_insert(superseded);
        }
        Ok(())
    }

    /// A replacement Attempt whose Call was never published must not stay
    /// authoritative; failing it settles its Chat Message through the same
    /// path as any failed Attempt.
    fn fail_unpublished_retry(
        domain: &mut DomainRepository,
        attempt_id: &str,
        error: &OcgError,
    ) -> Result<()> {
        if domain.authority(attempt_id)?.is_some() {
            domain.fail_attempt(
                attempt_id,
                &super::domain::job_failure(
                    "retry_admission_failed",
                    FailureClass::Unknown,
                    &error.to_string(),
                    true,
                ),
            )?;
        }
        Ok(())
    }

    /// Undo a Chat retry whose Call was never published: the replacement
    /// Attempt must not stay authoritative without execution, and the session
    /// returns to the tail it had.
    fn abandon_chat_retry(
        &self,
        domain: &mut DomainRepository,
        attempt_id: &str,
        transport: ChatRetryTransport,
        error: &OcgError,
    ) -> Result<()> {
        transport.cancelled.cancel();
        Self::fail_unpublished_retry(domain, attempt_id, error)?;
        self.release_chat_retry_transport(attempt_id, transport)
    }

    /// The idempotency digest for a Health Probe command.
    ///
    /// The command ledger is shared with canonical launch, which hashes a whole
    /// request. A probe hashes its command identity and target only: which Job a
    /// retry refers to is decided by the tuple, not by the caller's transport
    /// details.
    fn health_probe_command_hash(request: &crate::contracts::HealthProbeRequest) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(request.command_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(request.project_id.as_bytes());
        hasher.update(b"\0");
        hasher.update(request.target.provider.as_bytes());
        hasher.update(b"\0");
        hasher.update(request.target.model.as_bytes());
        hasher.update(b"\0");
        hasher.update(request.target.effort.as_deref().unwrap_or("").as_bytes());
        format!("{:x}", hasher.finalize())
    }

    fn provider_budget_config(
        &self,
        global: &GlobalConfiguration,
    ) -> Result<super::budget::BudgetConfig> {
        let mut config = self.profile_service.budget_config()?;
        if let Some(budget) = global
            .resource_budget
            .as_ref()
            .filter(|budget| budget.hard_limit > 0.0)
        {
            config.currency = Some(super::budget::normalize_currency(&budget.unit)?);
            config.hard_limit_micros = Some((budget.hard_limit * 1_000_000.0) as i64);
        }
        Ok(config)
    }

    fn launch_job_inner(
        &self,
        request: crate::contracts::JobLaunchRequest,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        self.launch_job_inner_for_existing(request, None)
    }

    fn launch_job_inner_for_existing(
        &self,
        request: crate::contracts::JobLaunchRequest,
        existing_job: Option<Job>,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        use sha2::{Digest, Sha256};

        if !safe_id(&request.command_id) {
            return Ok(crate::contracts::JobLaunchResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "rejected".to_string(),
                command_id: request.command_id.clone(),
                draft_id: request.draft_id.clone(),
                project_id: request.project_id.clone(),
                session_id: request.session_id.clone(),
                job_id: None,
                message: "invalid command_id".to_string(),
                duplicate: false,
            });
        }
        if !safe_id(&request.project_id) {
            return Ok(crate::contracts::JobLaunchResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "rejected".to_string(),
                command_id: request.command_id.clone(),
                draft_id: request.draft_id.clone(),
                project_id: request.project_id.clone(),
                session_id: request.session_id.clone(),
                job_id: None,
                message: "invalid project_id".to_string(),
                duplicate: false,
            });
        }

        let (project, mut domain) = match self.project_repository(&request.project_id) {
            Ok(project) => project,
            Err(error) => {
                return Ok(crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: request.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: error.to_string(),
                    duplicate: false,
                });
            }
        };

        // Compute request hash for idempotency conflict detection
        let request_canonical = serde_json::to_string(&request)
            .map_err(|e| invalid(format!("cannot serialize request: {e}")))?;
        let mut hasher = Sha256::new();
        hasher.update(request_canonical.as_bytes());
        let selection = self
            .chat_registrations
            .lock()
            .map_err(|_| invalid("chat registrations poisoned"))?
            .get(&request.command_id)
            .and_then(|entry| entry.selection.clone());
        if let Some(selection) = &selection {
            hasher
                .update(serde_json::to_vec(selection).map_err(|error| invalid(error.to_string()))?);
        }
        let images = self
            .chat_registrations
            .lock()
            .map_err(|_| invalid("chat registrations poisoned"))?
            .get(&request.command_id)
            .map(|entry| entry.images.clone())
            .unwrap_or_default();
        if !images.is_empty() {
            hasher.update(serde_json::to_vec(&images).map_err(|error| invalid(error.to_string()))?);
        }
        let request_hash = format!("{:x}", hasher.finalize());

        // Check for existing command
        if let Some((stored_hash, job_id, outcome, message)) = if existing_job.is_none() {
            domain.lookup_launch_command(&request.command_id, &request.project_id)?
        } else {
            None
        } {
            if stored_hash != request_hash {
                return Ok(crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: request.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: "command_id already used with different request content".to_string(),
                    duplicate: false,
                });
            }
            return Ok(crate::contracts::JobLaunchResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome,
                command_id: request.command_id.clone(),
                draft_id: request.draft_id.clone(),
                project_id: request.project_id.clone(),
                session_id: request.session_id.clone(),
                job_id,
                message,
                duplicate: true,
            });
        }

        // Resolve configuration
        let (global_config, _) = self.read_configuration()?;
        let configuration_revision = self.store.active_revision()?;
        let (project_config, _) = self.project_defaults(&project.project_id)?;
        let provider_concurrency = provider_concurrency(&project_config.defaults)?;

        let runtime = if let Some(registry) = &self.runtime_registry {
            registry.get_or_start(
                &project.project_id,
                Path::new(&project.root),
                provider_concurrency,
            )
        } else {
            self.runtime_handle
                .as_ref()
                .filter(|handle| {
                    handle.project_root() == Path::new(&project.root) && handle.owns_execution()
                })
                .cloned()
                .ok_or_else(|| invalid("execution runtime is not available for this Project"))
        };
        let runtime_handle = match runtime {
            Ok(handle) => handle,
            Err(error) => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "failed".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: format!("{error}; Job was not created"),
                    duplicate: false,
                };
                // Without execution ownership this process has no authority to
                // record anything about the command, not even a failure in the
                // command ledger: the outcome is decided by whichever process
                // can own the Project, and a retry of the same command stays
                // free to succeed.
                return Ok(response);
            }
        };

        // Disk safety precedes every other admission decision: resolve the
        // guard before profile, Placement or payload work begins.
        let disk_guard = self.disk_guard_for_project(
            &project.project_id,
            Path::new(&project.root),
            &global_config,
        )?;

        // Resolve profile from the user-global profile service
        let (profile, profile_revision) = match self.profile_service.current()? {
            Some(p) => p,
            None => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: "profile not configured".to_string(),
                    duplicate: false,
                };
                if let Some(job) = &existing_job {
                    settle_unpublished_reservation(
                        &mut domain,
                        &project.project_id,
                        job,
                        &response.message,
                    )?;
                    domain.record_admission_failure(
                        &job.id,
                        &super::domain::job_failure(
                            "placement_incompatible",
                            FailureClass::Validation,
                            "Profile has no configured execution candidates",
                            false,
                        ),
                    )?;
                }
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    existing_job.as_ref().map(|job| job.id.as_str()),
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        // Chat uses the Profile selection saved by setup unless this Project
        // explicitly overrides it. Ordinary Job launch keeps its configured defaults.
        let is_chat = self
            .chat_registrations
            .lock()
            .map_err(|_| invalid("chat registrations poisoned"))?
            .contains_key(&request.command_id);
        let chat_selection = if is_chat {
            Some(
                profile.select(
                    selection
                        .as_ref()
                        .map(|selection| selection.model.as_str())
                        .or_else(|| project_config.defaults.get("model").and_then(Value::as_str)),
                )?,
            )
        } else {
            None
        };

        let budget_config = self.provider_budget_config(&global_config)?;
        let spec = existing_job
            .as_ref()
            .map(|job| job.spec.clone())
            .unwrap_or_else(|| JobSpec {
                provider: chat_selection.map(|(_, model)| model.provider.clone()),
                model: chat_selection.map(|(key, _)| key.to_string()),
                objective: Some(request.objective.clone()),
                hard_budget_micros: Some(request.hard_budget_micros),
                ..JobSpec::default()
            });
        // Chat and generic launch both require candidate selection. An exact
        // target, such as a Health Probe, never enters this function.
        let preferred_provider = project_config
            .defaults
            .get("provider")
            .and_then(Value::as_str)
            .map(str::to_string);
        let preferred_model = project_config
            .defaults
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| profile.default_model.clone());
        let effort = selection
            .as_ref()
            .and_then(|selection| selection.effort.clone());
        let resolved = match super::admission::resolve_target(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                // Chat's accepted-turn queue predates placement. Its worker
                // still acquires the existing execution slot before I/O.
                concurrency: (!is_chat).then_some(provider_concurrency),
                // Placement evaluation must observe the same Governor that
                // execution-time acquisition enforces, so a recorded upstream
                // cooldown is visible before a candidate is selected.
                governor: Some(runtime_handle.governor()),
                disk_guard: Some(disk_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            existing_job.as_ref(),
            &spec,
            &AdmissionTarget::SelectCandidate,
            super::placement::Requirements {
                images: !images.is_empty(),
                effort: effort.as_deref(),
            },
            preferred_provider.as_deref(),
            preferred_model.as_deref(),
        )? {
            Ok(resolved) => resolved,
            Err(refusal) => {
                if let Some(job) = &existing_job {
                    settle_unpublished_reservation(
                        &mut domain,
                        &project.project_id,
                        job,
                        &refusal.message,
                    )?;
                    domain.record_admission_failure(&job.id, &refusal.failure)?;
                    if refusal.profile_incompatible
                        && job.state == super::domain::JobState::Eligible
                    {
                        self.placement_deferrals
                            .lock()
                            .map_err(|_| invalid("placement deferrals poisoned"))?
                            .insert(
                                job.id.clone(),
                                PlacementDeferral {
                                    root: PathBuf::from(&project.root),
                                    profile_revision,
                                    configuration_revision,
                                    job_configuration_revision: domain
                                        .job_configuration(&job.id)?
                                        .map(|(_, revision)| revision),
                                    generation: job.generation,
                                    spec: job.spec.clone(),
                                },
                            );
                    }
                }
                return Ok(crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: existing_job.as_ref().map(|job| job.id.clone()),
                    message: refusal.message,
                    duplicate: false,
                });
            }
        };
        let provider_key = resolved.reservation.choice.provider.clone();
        let provider_key = provider_key.as_str();
        let model = resolved.reservation.choice.model.clone();
        let model = model.as_str();

        let provider_entry = profile
            .providers
            .get(provider_key)
            .ok_or_else(|| invalid("resolved provider disappeared"))?;

        let effort = selection
            .as_ref()
            .and_then(|selection| selection.effort.as_deref());
        if let Some(entry) = profile.models.get(model) {
            if let Some(refusal) =
                chat_capability_refusal(provider_entry, entry, effort, !images.is_empty())
            {
                return Err(invalid(refusal));
            }
        } else if effort.is_some() {
            return Err(invalid("selected model missing"));
        }

        // Create the Job, then hand it to canonical admission. The Job spec is
        // the durable launch intent. Frozen provider transport identity belongs
        // to the Call's DispatchIntent, which admission publishes. The boundary
        // already proved this Project identity; read it back rather than
        // creating one.
        let canonical_project = domain
            .project_at_root(Path::new(&project.root))?
            .ok_or_else(|| invalid("registered Project identity is not durable at this root"))?;
        let job_spec = super::domain::JobSpec {
            provider: Some(provider_key.to_string()),
            model: Some(model.to_string()),
            objective: Some(request.objective.clone()),
            success_criteria: request.success_criteria.clone(),
            constraints: request.constraints.clone(),
            hard_budget_micros: Some(request.hard_budget_micros),
            resource_commitment: request.resource_commitment,
            recursive_limits: super::domain::RecursiveLimits::default(),
            health_probe: None,
        };

        let job = if let Some(job) = existing_job.as_ref() {
            if !domain.mark_automatic_admission(&job.id)? {
                return Err(invalid("automatic admission Job is no longer ready"));
            }
            domain
                .job(&job.id)?
                .ok_or_else(|| invalid("admission Job disappeared"))?
        } else {
            domain.create_job(&canonical_project.id, job_spec)?
        };
        // Reservation is the canonical admission transition. Every origin
        // freezes its generation's execution input in the same transaction as
        // the Attempt. A new Chat turn also stages its Conversation turn and
        // Messages there, and its input is the Conversation history at that
        // turn boundary. Any other Job's input is derived from its
        // specification alone, so recovery may re-place it.
        let user_content = initial_user_message(&request);
        let input = super::domain::JobInputDraft {
            target: super::domain::ExecutionTarget {
                provider_key: resolved.config.provider_key.clone(),
                model_key: resolved.config.model.clone(),
                upstream_model_id: resolved.config.upstream_model_id.clone(),
                protocol: resolved.protocol,
                endpoint: resolved.config.endpoint.clone(),
                credential_ref: resolved.config.credential_ref.clone(),
            },
            effort: effort.map(str::to_string),
            payload: super::domain::InputPayload::Agent,
            replay: if is_chat {
                super::domain::InputReplay::Exact
            } else {
                super::domain::InputReplay::Replaceable
            },
            content: if is_chat && existing_job.is_none() {
                super::domain::InputContent::ChatTurn(super::domain::ChatTurnDraft {
                    request: &request,
                    request_hash: &request_hash,
                    content: &user_content,
                    images: &images,
                })
            } else {
                super::domain::InputContent::Messages(vec![
                    json!({"role": "user", "content": user_content}),
                ])
            },
        };
        let reserved = match reserve(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                concurrency: (!is_chat).then_some(provider_concurrency),
                governor: None,
                disk_guard: Some(disk_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            &job,
            &resolved,
            &input,
        )? {
            Ok(reserved) => reserved,
            Err(refusal) => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "failed".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: Some(job.id.clone()),
                    message: refusal.message,
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "failed",
                    Some(&job.id),
                    &response.message,
                )?;
                return Ok(response);
            }
        };
        let mut resolved = resolved;
        let provider_request = match (&reserved.messages, reserved.existing_call) {
            // The Call already exists; publication resumes it unchanged.
            (_, true) => json!({"model": model, "messages": [], "stream": true}),
            (Some(messages), false) => {
                let mut request = json!({
                    "model": model,
                    "messages": messages,
                    "stream": true,
                });
                if let Some(effort) = effort {
                    request["reasoning_effort"] = json!(effort);
                }
                request
            }
            // A resumed reservation publishes exactly the input its generation
            // froze, against that frozen target. Recovery already refused
            // every reservation that has no such input.
            (None, false) => {
                let frozen = domain
                    .job_input(&job.id, reserved.attempt.generation)?
                    .ok_or_else(|| invalid("resumed Attempt has no frozen execution input"))?;
                let (request, protocol, config) =
                    match self.frozen_input_replay(&domain, &job.project_id, &frozen) {
                        Ok(replay) => replay,
                        Err(error) => {
                            // Nothing was sent, and the frozen input can no
                            // longer run as committed: settle it, never
                            // substitute a target or a rebuilt request.
                            domain.fail_attempt(
                                &reserved.attempt.id,
                                &unpublished_admission_failure(&error.to_string()),
                            )?;
                            return Err(error);
                        }
                    };
                resolved.protocol = protocol;
                resolved.config = config;
                resolved.effort = request
                    .get("reasoning_effort")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                request
            }
        };
        let chat = self
            .chat_registrations
            .lock()
            .ok()
            .and_then(|registrations| {
                registrations.get(&request.command_id).map(|registration| {
                    (registration.sender.clone(), registration.cancelled.clone())
                })
            });
        let prepared = PreparedExecution {
            request: provider_request,
            payload: PayloadMode::Agent,
            event_sender: chat.as_ref().map(|(sender, _)| sender.clone()),
            cancelled: chat
                .as_ref()
                .map(|(_, cancelled)| cancelled.clone())
                .unwrap_or_default(),
        };
        let admitted = publish_call(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                concurrency: (!is_chat).then_some(provider_concurrency),
                governor: None,
                disk_guard: Some(disk_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            &job,
            &resolved,
            &reserved,
            prepared,
            &runtime_handle,
        )?;
        let _admitted = match admitted {
            Ok(admitted) => admitted,
            Err(refusal) => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "failed".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: Some(job.id.clone()),
                    message: refusal.message,
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "failed",
                    Some(&job.id),
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        let response = crate::contracts::JobLaunchResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            outcome: "accepted".to_string(),
            command_id: request.command_id.clone(),
            draft_id: request.draft_id.clone(),
            project_id: project.project_id.clone(),
            session_id: request.session_id.clone(),
            job_id: Some(job.id.clone()),
            message: format!("job launched: {}", job.id),
            duplicate: false,
        };
        domain.record_launch_command(
            &request.command_id,
            &request.project_id,
            &request_hash,
            "accepted",
            Some(&job.id),
            &response.message,
        )?;

        Ok(response)
    }

    /// Launch a Health Probe Job for one Provider x Model x Effort tuple.
    ///
    /// The probe is a real canonical Job: it is created with the target on its
    /// specification, dispatched through the ordinary Job -> Attempt -> Executor
    /// transition, and its Call is admitted and enqueued on the same bounded
    /// provider dispatcher every other Job uses. There is no second execution
    /// path and nothing here schedules anything — a probe runs when it is asked
    /// for, and only then.
    ///
    /// A probe deliberately bypasses Placement. Placement ranks candidates for
    /// work that is allowed to go anywhere; a probe names the exact tuple it
    /// must prove, so choosing one for it would destroy the only fact the probe
    /// exists to produce. For the same reason it does not take a per-Project
    /// capacity reservation: it queues behind real work on the bounded
    /// dispatcher instead of displacing it.
    pub fn launch_health_probe(
        &self,
        request: crate::contracts::HealthProbeRequest,
        _now: i64,
    ) -> Result<crate::contracts::HealthProbeResponse> {
        let _launch = self
            .launch_lock
            .lock()
            .map_err(|_| invalid("launch lock poisoned"))?;
        let rejected =
            |message: String, job_id: Option<String>| crate::contracts::HealthProbeResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "rejected".to_string(),
                command_id: request.command_id.clone(),
                project_id: request.project_id.clone(),
                target: request.target.clone(),
                job_id,
                message,
                duplicate: false,
            };
        if !safe_id(&request.command_id) || !safe_id(&request.project_id) {
            return Ok(rejected(
                "invalid command_id or project_id".to_string(),
                None,
            ));
        }
        let (project, mut domain) = match self.project_repository(&request.project_id) {
            Ok(project) => project,
            Err(error) => return Ok(rejected(error.to_string(), None)),
        };
        let intent = request.target.intent();
        // Idempotency is the same command ledger canonical launch uses, so a
        // retried probe never spends a second round.
        if let Some((_, job_id, outcome, message)) =
            domain.lookup_launch_command(&request.command_id, &request.project_id)?
        {
            return Ok(crate::contracts::HealthProbeResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome,
                command_id: request.command_id.clone(),
                project_id: request.project_id.clone(),
                target: request.target.clone(),
                job_id,
                message,
                duplicate: true,
            });
        }
        let (profile, _) = match self.profile_service.current()? {
            Some(profile) => profile,
            None => return Ok(rejected("profile not configured".to_string(), None)),
        };

        // A probe Job is created before the target is validated, and a rejection
        // is recorded on it. The verdict "OCG cannot execute this tuple" is
        // itself health evidence, and evidence that only lives in an HTTP
        // response body would be gone by the time Placement asked the question.
        // The rejection path therefore produces the same canonical shape as a
        // probe that ran: Job -> Attempt -> Executor -> terminal reason.
        let spec = super::domain::JobSpec {
            provider: Some(intent.provider.clone()),
            model: Some(intent.model.clone()),
            objective: Some(super::health_probe::PROBE_OBJECTIVE.to_string()),
            health_probe: Some(intent.clone()),
            ..JobSpec::default()
        };
        // Execution ownership is settled before the probe Job exists. Without
        // it this process never had the right to run the tuple, so a failure
        // to acquire it is refused with nothing written: it is not health
        // evidence about the tuple.
        let runtime = if let Some(registry) = &self.runtime_registry {
            registry.get_or_start(&project.project_id, Path::new(&project.root), 1)
        } else {
            self.runtime_handle
                .as_ref()
                .filter(|handle| {
                    handle.project_root() == Path::new(&project.root) && handle.owns_execution()
                })
                .cloned()
                .ok_or_else(|| invalid("execution runtime is not available for this Project"))
        };
        let runtime_handle = match runtime {
            Ok(handle) => handle,
            Err(error) => return Ok(rejected(error.to_string(), None)),
        };
        let job = domain.create_job(&project.project_id, spec.clone())?;
        // A rejected probe is recorded on the Job before any Attempt is
        // claimed. The refusal is the same admission decision every other
        // origin records: the Job becomes eligible evidence and is marked
        // explicit, so the automatic worker cannot later place a different
        // candidate for it.
        let record = |domain: &mut DomainRepository, failure: &Failure| {
            domain.set_job_eligible(&job.id)?;
            domain.freeze_explicit_admission(
                &job.id,
                &super::admission::AdmissionReservation {
                    choice: super::placement::ProviderChoice {
                        provider: intent.provider.clone(),
                        model: intent.model.clone(),
                    },
                    pickup: super::admission::AdmissionPickup::Explicit,
                    exact: Some(super::admission::ExactTarget {
                        provider: intent.provider.clone(),
                        model: intent.model.clone(),
                        effort: intent.effort.clone(),
                    }),
                },
            )?;
            domain.record_admission_failure(&job.id, failure)
        };
        let budget_config = self.provider_budget_config(&self.read_configuration()?.0)?;
        // Probes are Exact diagnostic targets: they fan out durable state
        // without producing user work, so disk safety gates them before the
        // target is even validated.
        let (probe_global, _) = self.read_configuration()?;
        let probe_guard = self.disk_guard_for_project(
            &project.project_id,
            Path::new(&project.root),
            &probe_global,
        )?;
        let resolved = match super::admission::resolve_target(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                concurrency: None,
                governor: None,
                disk_guard: Some(probe_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            Some(&job),
            &spec,
            &AdmissionTarget::Exact(super::admission::ExactTarget {
                provider: intent.provider.clone(),
                model: intent.model.clone(),
                effort: intent.effort.clone(),
            }),
            super::placement::Requirements {
                images: false,
                effort: intent.effort.as_deref(),
            },
            None,
            None,
        )? {
            Ok(resolved) => resolved,
            Err(refusal) => {
                record(&mut domain, &refusal.failure)?;
                let response = rejected(refusal.message, Some(job.id.clone()));
                domain.record_launch_command(
                    &request.command_id,
                    &project.project_id,
                    &Self::health_probe_command_hash(&request),
                    "rejected",
                    Some(&job.id),
                    &response.message,
                )?;
                return Ok(response);
            }
        };
        // The exact target is frozen. Reservation claims the Attempt without
        // asking Placement to choose, and the probe payload is built only after
        // that reservation exists.
        let reserved = match reserve(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                concurrency: None,
                governor: None,
                disk_guard: Some(probe_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            &job,
            &resolved,
            &super::domain::JobInputDraft {
                target: super::domain::ExecutionTarget {
                    provider_key: resolved.config.provider_key.clone(),
                    model_key: resolved.config.model.clone(),
                    upstream_model_id: resolved.config.upstream_model_id.clone(),
                    protocol: resolved.protocol,
                    endpoint: resolved.config.endpoint.clone(),
                    credential_ref: resolved.config.credential_ref.clone(),
                },
                effort: intent.effort.clone(),
                payload: super::domain::InputPayload::Probe,
                replay: super::domain::InputReplay::Exact,
                content: super::domain::InputContent::Messages(
                    super::health_probe::probe_request(&intent.model, intent.effort.as_deref())
                        .get("messages")
                        .and_then(Value::as_array)
                        .cloned()
                        .ok_or_else(|| invalid("probe request has no messages"))?,
                ),
            },
        )? {
            Ok(reserved) => reserved,
            Err(refusal) => {
                let response = rejected(refusal.message, Some(job.id.clone()));
                domain.record_launch_command(
                    &request.command_id,
                    &project.project_id,
                    &Self::health_probe_command_hash(&request),
                    "failed",
                    Some(&job.id),
                    &response.message,
                )?;
                return Ok(response);
            }
        };
        let prepared = PreparedExecution {
            request: super::health_probe::probe_request(&intent.model, intent.effort.as_deref()),
            payload: PayloadMode::Probe,
            event_sender: None,
            cancelled: CallCancellation::new(),
        };
        let outcome = match publish_call(
            &mut AdmissionContext {
                domain: &mut domain,
                profile: &profile,
                root: Path::new(&project.root),
                project_id: &project.project_id,
                budget: &budget_config,
                concurrency: None,
                governor: None,
                disk_guard: Some(probe_guard.clone()),
                now: crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            },
            &job,
            &resolved,
            &reserved,
            prepared,
            &runtime_handle,
        )? {
            Ok(admitted) => crate::contracts::HealthProbeResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "accepted".to_string(),
                command_id: request.command_id.clone(),
                project_id: project.project_id.clone(),
                target: request.target.clone(),
                job_id: Some(job.id.clone()),
                message: format!("health probe launched: {}", admitted.call_id),
                duplicate: false,
            },
            Err(refusal) => crate::contracts::HealthProbeResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "failed".to_string(),
                command_id: request.command_id.clone(),
                project_id: project.project_id.clone(),
                target: request.target.clone(),
                job_id: Some(job.id.clone()),
                message: refusal.message,
                duplicate: false,
            },
        };
        domain.record_launch_command(
            &request.command_id,
            &project.project_id,
            &Self::health_probe_command_hash(&request),
            &outcome.outcome,
            outcome.job_id.as_deref(),
            &outcome.message,
        )?;
        Ok(outcome)
    }

    /// Read the latest usable health evidence for one candidate.
    ///
    /// The answer is projected from the probe Jobs' canonical rows on every
    /// read, so it cannot lag or contradict execution history, and it costs
    /// nothing when no probe has run.
    pub fn health_probe(
        &self,
        request: crate::contracts::HealthProbeQuery,
    ) -> Result<crate::contracts::HealthProbeQueryResponse> {
        let domain = self.project_repository(&request.project_id)?.1;
        let intent = request.target.intent();
        let observation = domain
            .latest_health_probe(&request.project_id, &intent)?
            .map(HealthProbeObservation::from);
        Ok(crate::contracts::HealthProbeQueryResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: request.project_id,
            target: request.target,
            observation,
        })
    }

    /// Start a plain chat turn on the canonical Job/Attempt/Call lane.
    /// Chat state is transport state; execution remains owned by the durable
    /// canonical entities. The live provider receiver is retained for the
    /// SSE tail; use `take_chat_receiver` to stream it once.
    pub fn launch_chat(
        &self,
        request: crate::contracts::JobLaunchRequest,
        _now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        self.launch_chat_selected(request, None, _now)
    }

    pub fn launch_chat_selected(
        &self,
        request: crate::contracts::JobLaunchRequest,
        selection: Option<crate::contracts::ChatModelSelection>,
        _now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        self.launch_chat_with_images(request, selection, &[], _now)
    }

    pub fn upload_chat_image(
        &self,
        request: &crate::contracts::ChatImageUploadRequest,
    ) -> Result<crate::contracts::ChatImage> {
        self.project_repository(&request.project_id)?;
        crate::chat_images::upload(request)
    }

    pub fn read_chat_image(&self, project_id: &str, image_id: &str) -> Result<(String, Vec<u8>)> {
        self.project_repository(project_id)?;
        crate::chat_images::read(project_id, image_id)
    }

    pub fn receive_chat_image_for_turn(
        &self,
        session_id: &str,
        job_id: &str,
        call_id: &str,
        url: &str,
    ) -> Result<crate::contracts::ChatImage> {
        let (project_id, attempt_id) = self
            .active_chats
            .lock()
            .map_err(|_| invalid("active chats poisoned"))?
            .values()
            .find(|chat| chat.session_id == session_id && chat.job_id == job_id)
            .map(|chat| (chat.project_id.clone(), chat.attempt_id.clone()))
            .ok_or_else(|| invalid("unknown chat turn"))?;
        self.project_repository(&project_id)?;
        crate::chat_images::received(
            &project_id,
            crate::attachments::Producer {
                job_id,
                attempt_id: Some(&attempt_id),
                call_id: Some(call_id),
            },
            url,
        )
    }

    pub fn launch_chat_with_images(
        &self,
        request: crate::contracts::JobLaunchRequest,
        selection: Option<crate::contracts::ChatModelSelection>,
        image_ids: &[String],
        _now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        self.project_repository(&request.project_id)?;
        let images = crate::chat_images::selected(&request.project_id, image_ids)?;
        let _launch = self
            .launch_lock
            .lock()
            .map_err(|_| invalid("launch lock poisoned"))?;
        let session_key = (request.project_id.clone(), request.session_id.clone());
        self.reap_expired_chats();
        let (sender, receiver) = flume::unbounded();
        let cancelled = CallCancellation::new();
        if let Ok(mut registrations) = self.chat_registrations.lock() {
            registrations.insert(
                request.command_id.clone(),
                ChatRegistration {
                    images,
                    selection,
                    sender: sender.clone(),
                    cancelled: cancelled.clone(),
                },
            );
        }
        let buffer = std::sync::Arc::new(ChatEventBuffer::default());
        // One active turn per session. Detach the previous turn so this one can
        // take the session's single registration slot, and register before
        // launching: `launch_job` is what enqueues the provider envelope, so
        // from here on an envelope can exist that only this registration can
        // cancel. Registering after the launch would leave a window in which a
        // queued turn has no reachable token.
        //
        // The detached turn is only ended once this launch is accepted, so a
        // launch that fails leaves the previous turn exactly as it was.
        let superseded = self
            .active_chats
            .lock()
            .ok()
            .and_then(|mut active| active.remove(&session_key));
        let registered = self
            .active_chats
            .lock()
            .map(|mut active| {
                active.insert(
                    session_key.clone(),
                    ActiveChat {
                        project_id: request.project_id.clone(),
                        session_id: request.session_id.clone(),
                        // Published once the launch resolves them.
                        job_id: String::new(),
                        attempt_id: String::new(),
                        cancelled: cancelled.clone(),
                        sender: sender.clone(),
                        buffer: buffer.clone(),
                    },
                );
            })
            .is_ok();

        let launch = self.launch_job_inner(request.clone());
        // Registration is transient: launch_job has already cloned the sender
        // into the dispatched provider envelope.
        if let Ok(mut registrations) = self.chat_registrations.lock() {
            registrations.remove(&request.command_id);
        }
        let response = match launch {
            Ok(response) => response,
            Err(error) => {
                // This turn never became addressable work: drop its registration
                // rather than leave one claiming the session holds a turn that
                // does not exist, and hand the session back to the turn it had.
                if registered {
                    self.discard_active_turn(&session_key);
                }
                if let Some(superseded) = superseded {
                    self.restore_active_turn(superseded);
                }
                return Err(error);
            }
        };
        if response.outcome != "accepted" || response.duplicate {
            if registered {
                self.discard_active_turn(&session_key);
            }
            if let Some(superseded) = superseded {
                self.restore_active_turn(superseded);
            }
            return Ok(response);
        }
        // This turn is accepted, so the previous one in this session is
        // superseded: revoke its authority and stop its transport.
        if let Some(superseded) = superseded {
            let _ = self.end_active_chat(superseded);
        }
        let job_id = match response.job_id.clone() {
            Some(id) => id,
            None => {
                if registered {
                    self.discard_active_turn(&session_key);
                }
                return Ok(response);
            }
        };
        // The images this turn's frozen input references now belong to its
        // Job, unless an earlier Job's input already referenced them.
        crate::attachments::adopt(&request.project_id, image_ids, &job_id)?;
        // Resolve the Attempt that launch_job created for this Job. There is
        // exactly one Attempt right after launch.
        let attempt_id = {
            let (_, domain) = self.project_repository(&request.project_id)?;
            let attempts = domain.attempts_for_job(&job_id)?;
            attempts
                .last()
                .map(|attempt| attempt.id.clone())
                .unwrap_or_default()
        };
        if attempt_id.is_empty() {
            if registered {
                self.discard_active_turn(&session_key);
            }
            return Ok(response);
        }
        // The turn was cancelled while it was still launching, so this
        // registration is already gone. Settle the Attempt the launch did
        // create — `cancel_chat` could not, because it had no Attempt identity
        // then — and let the queued envelope be fenced by the worker.
        if cancelled.is_cancelled() {
            let (_, domain) = self.project_repository(&request.project_id)?;
            if let Some(job) = domain.job(&job_id)? {
                if job.authoritative_attempt_id.as_deref() == Some(attempt_id.as_str()) {
                    self.cancel_job(&job.id, job.generation)?;
                }
            }
            return Ok(response);
        }
        // Retain every provider event from this point on through the shared
        // forwarder. Registration happens after launch so failed launches do
        // not leave a live receiver in the shared event loop.
        self.chat_forwarder.register(receiver, buffer.clone())?;
        // Publish the real identity onto the registration made before the
        // launch, so cancellation can now settle the Attempt as well as the
        // transport.
        let published = self
            .active_chats
            .lock()
            .map(|mut active| match active.get_mut(&session_key) {
                Some(entry) => {
                    entry.job_id = job_id.clone();
                    entry.attempt_id = attempt_id.clone();
                    true
                }
                None => false,
            })
            .unwrap_or(false);
        if !published {
            // The turn was cancelled between launch and publication. Settle the
            // Attempt it created rather than leaving it live and unreachable.
            let (_, domain) = self.project_repository(&request.project_id)?;
            if let Some(job) = domain.job(&job_id)? {
                if job.authoritative_attempt_id.as_deref() == Some(attempt_id.as_str()) {
                    self.cancel_job(&job.id, job.generation)?;
                }
            }
        }
        Ok(response)
    }

    /// Drop this session's registration without settling anything: the turn it
    /// described never became addressable work.
    fn discard_active_turn(&self, session_key: &(String, String)) {
        if let Ok(mut active) = self.active_chats.lock() {
            if let Some(entry) = active.get(session_key) {
                if entry.job_id.is_empty() {
                    active.remove(session_key);
                }
            }
        }
    }

    /// Put a detached turn back under its session, because the launch that was
    /// going to supersede it did not happen. A turn that has meanwhile taken the
    /// session over owns it again and is left alone.
    fn restore_active_turn(&self, superseded: ActiveChat) {
        if let Ok(mut active) = self.active_chats.lock() {
            let key = (superseded.project_id.clone(), superseded.session_id.clone());
            active.entry(key).or_insert(superseded);
        }
    }

    /// Clone the retained tail for one SSE attach. The entry stays for
    /// cancellation until `finish_chat` removes it. Late attach replays from
    /// index zero; live events follow once the prefix is drained.
    pub(crate) fn chat_buffer_for(
        &self,
        session_id: &str,
        job_id: &str,
    ) -> Option<std::sync::Arc<ChatEventBuffer>> {
        self.reap_expired_chats();
        let mut guard = self.active_chats.lock().ok()?;
        let key = guard
            .iter()
            .find(|(_, entry)| entry.session_id == session_id && entry.job_id == job_id)
            .map(|(key, _)| key.clone())?;
        let entry = guard.get(&key)?;
        if entry
            .buffer
            .state
            .lock()
            .ok()
            .and_then(|state| state.terminal_at)
            .is_some_and(|terminal_at| terminal_at.elapsed() >= CHAT_REPLAY_LIFETIME)
        {
            guard.remove(&key);
            return None;
        }
        Some(entry.buffer.clone())
    }

    /// Opportunistically reap terminal transport buffers. Running chats are
    /// deliberately never touched; only completed tails past their replay
    /// lifetime are eligible.
    fn reap_expired_chats(&self) {
        if let Ok(mut active) = self.active_chats.lock() {
            let expired = active
                .iter()
                .filter_map(|(session_id, entry)| {
                    let expired = entry
                        .buffer
                        .state
                        .lock()
                        .ok()
                        .and_then(|state| state.terminal_at)
                        .is_some_and(|terminal_at| terminal_at.elapsed() >= CHAT_REPLAY_LIFETIME);
                    expired.then(|| session_id.clone())
                })
                .collect::<Vec<_>>();
            for session_id in expired {
                active.remove(&session_id);
            }
        }
    }

    /// Remove a finished stream entry. Only removes when the job matches so a
    /// newer turn cannot be dropped by a stale tail.
    ///
    /// A retried Job reuses its session and Job identity with a new tail, so
    /// the consumed tail must also match: a stale stream of the previous
    /// generation cannot drop its replacement.
    pub(crate) fn finish_chat(
        &self,
        session_id: &str,
        job_id: &str,
        buffer: &std::sync::Arc<ChatEventBuffer>,
    ) {
        if let Ok(mut active) = self.active_chats.lock() {
            active.retain(|_, entry| {
                entry.session_id != session_id
                    || entry.job_id != job_id
                    || !std::sync::Arc::ptr_eq(&entry.buffer, buffer)
            });
        }
    }

    pub fn cancel_chat(&self, session_id: &str) -> Result<bool> {
        self.cancel_chat_in_project(None, session_id)
    }

    pub fn cancel_chat_in_project(
        &self,
        project_id: Option<&str>,
        session_id: &str,
    ) -> Result<bool> {
        let active = {
            let mut chats = self
                .active_chats
                .lock()
                .map_err(|_| invalid("active chats poisoned"))?;
            let keys = chats
                .iter()
                .filter(|(_, entry)| {
                    entry.session_id == session_id
                        && project_id.is_none_or(|project| entry.project_id == project)
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            if keys.len() > 1 {
                return Err(invalid(
                    "project_id is required to cancel an ambiguous session",
                ));
            }
            keys.first().and_then(|key| chats.remove(key))
        };
        let Some(active) = active else {
            return Ok(false);
        };
        self.end_active_chat(active)
    }

    /// End one active turn: revoke its Attempt authority, signal its
    /// cancellation token and stop the provider transport.
    ///
    /// The Attempt is settled by the canonical cancellation lifecycle, never by
    /// the transport, so a turn that has not published its Attempt identity yet
    /// only has its token signalled — its queued envelope is then fenced by the
    /// worker's own cancellation gate before any side effect.
    fn end_active_chat(&self, active: ActiveChat) -> Result<bool> {
        // If authority is already gone (completed/failed), or the turn has not
        // resolved its Attempt yet, there is nothing to revoke; still stop the
        // transport so a queued or running read cannot continue.
        if active.job_id.is_empty() || active.attempt_id.is_empty() {
            active.cancelled.cancel();
            return Ok(false);
        }
        let domain = self.repository_for_job(&active.job_id)?;
        let job = domain
            .job(&active.job_id)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        if job.authoritative_attempt_id.as_deref() != Some(active.attempt_id.as_str()) {
            active.cancelled.cancel();
            return Ok(false);
        }
        let response = self.cancel_job(&job.id, job.generation)?;
        active.cancelled.cancel();
        if let Err(error) = active
            .sender
            .send(ExecutionEvent::Failed("chat cancelled".to_string()))
        {
            tracing::debug!(%error, "cancelled Job chat receiver closed");
        }
        Ok(response.accepted)
    }
}

#[cfg(test)]
mod chat_retry_transport_tests {
    use super::*;
    use crate::core_contract::{Actor, MessageLifecycle};

    struct Retried {
        _directory: tempfile::TempDir,
        service: CanonicalControlService,
        domain: DomainRepository,
        project_id: String,
        job_id: String,
        first: String,
        replacement: String,
    }

    /// A failed Chat turn whose Job was retried in the domain: the replacement
    /// Attempt is authoritative and its Call has not been published.
    fn retried() -> Retried {
        let directory = tempfile::tempdir().expect("temp");
        let root = directory.path().to_path_buf();
        let service = CanonicalControlService::new(
            &root,
            &root.join("profile.json"),
            root.join("control.json"),
        );
        let mut domain = DomainRepository::open(&root).expect("domain");
        let project = domain.ensure_project(&root).expect("project");
        let job = domain
            .create_job(
                &project.id,
                JobSpec {
                    objective: Some("chat".into()),
                    ..JobSpec::default()
                },
            )
            .expect("job");
        domain.set_job_eligible(&job.id).expect("eligible");
        let first = domain.create_attempt(&job.id).expect("attempt");
        let request = crate::contracts::JobLaunchRequest {
            command_id: "command-1".into(),
            draft_id: "draft-1".into(),
            project_id: project.id.clone(),
            session_id: "session-1".into(),
            objective: "hello".into(),
            success_criteria: None,
            constraints: None,
            hard_budget_micros: 0,
            resource_commitment: None,
        };
        domain
            .prepare_chat_turn(&request, "hash", &first, "hello")
            .expect("turn");
        domain.accept_chat_turn(&first.id).expect("accept");
        domain
            .fail_attempt(
                &first.id,
                &super::super::domain::job_failure("test", FailureClass::Unknown, "boom", true),
            )
            .expect("fail first");
        let generation = domain.job(&job.id).expect("job").expect("job").generation;
        let replacement = domain
            .retry_job(&job.id, "provider", generation)
            .expect("retry")
            .attempt
            .id;
        Retried {
            _directory: directory,
            service,
            domain,
            project_id: project.id,
            job_id: job.id,
            first: first.id,
            replacement,
        }
    }

    fn key(retried: &Retried) -> (String, String) {
        (retried.project_id.clone(), "session-1".to_string())
    }

    /// The settled tail of the first Attempt, as `finish_chat` had not yet consumed it.
    fn previous_tail(retried: &Retried) -> Arc<ChatEventBuffer> {
        let buffer = Arc::new(ChatEventBuffer::default());
        let (sender, _) = flume::unbounded();
        retried.service.active_chats.lock().expect("chats").insert(
            key(retried),
            ActiveChat {
                project_id: retried.project_id.clone(),
                session_id: "session-1".into(),
                job_id: retried.job_id.clone(),
                attempt_id: retried.first.clone(),
                cancelled: CallCancellation::new(),
                sender,
                buffer: buffer.clone(),
            },
        );
        buffer
    }

    fn install(retried: &Retried) -> Result<ChatRetryTransport> {
        retried.service.install_chat_retry_transport(
            retried.project_id.clone(),
            "session-1".into(),
            &retried.job_id,
            &retried.replacement,
        )
    }

    fn assert_replacement_settled(retried: &mut Retried, error: &OcgError) {
        CanonicalControlService::fail_unpublished_retry(
            &mut retried.domain,
            &retried.replacement,
            error,
        )
        .expect("settle");
        assert_settled(retried);
    }

    fn assert_settled(retried: &Retried) {
        assert!(retried
            .domain
            .authority(&retried.replacement)
            .expect("authority")
            .is_none());
        let job = retried
            .domain
            .job(&retried.job_id)
            .expect("job")
            .expect("job");
        assert!(job.authoritative_attempt_id.is_none());
        let history = retried
            .domain
            .conversation_history(&retried.project_id, "session-1")
            .expect("history")
            .expect("conversation")
            .1;
        assert_eq!(history.len(), 2);
        let assistant = history
            .iter()
            .find(|message| matches!(message.author, Actor::Attempt { .. }))
            .expect("assistant");
        assert_eq!(assistant.state, MessageLifecycle::Failed);
    }

    fn assert_previous_tail(retried: &Retried, buffer: &Arc<ChatEventBuffer>) {
        let chats = retried
            .service
            .active_chats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = chats.get(&key(retried)).expect("previous tail restored");
        assert_eq!(entry.attempt_id, retried.first);
        assert!(Arc::ptr_eq(&entry.buffer, buffer));
    }

    #[test]
    fn poisoned_active_chats_refuse_the_retry_transport() {
        let mut retried = retried();
        let chats = retried.service.active_chats.clone();
        let _ = std::thread::spawn(move || {
            let _guard = chats.lock().expect("chats");
            panic!("poison active chats");
        })
        .join();
        let error = install(&retried)
            .err()
            .expect("a poisoned registry installs no transport");
        assert!(retried
            .service
            .active_chats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
        assert_replacement_settled(&mut retried, &error);
    }

    #[test]
    fn forwarder_failure_removes_the_replacement_and_restores_the_tail() {
        let mut retried = retried();
        let tail = previous_tail(&retried);
        let (registrations, closed) = flume::unbounded();
        drop(closed);
        retried.service.chat_forwarder = Arc::new(ChatForwarder { registrations });
        let error = install(&retried)
            .err()
            .expect("an unconsumed transport is refused");
        assert_previous_tail(&retried, &tail);
        assert_replacement_settled(&mut retried, &error);
    }

    #[test]
    fn publish_failure_abandons_the_replacement_and_restores_the_tail() {
        let mut retried = retried();
        let tail = previous_tail(&retried);
        let transport = install(&retried).expect("transport");
        let cancelled = transport.cancelled.clone();
        let replacement = retried.replacement.clone();
        retried
            .service
            .abandon_chat_retry(
                &mut retried.domain,
                &replacement,
                transport,
                &invalid("economic admission failed"),
            )
            .expect("abandon");
        assert!(cancelled.is_cancelled());
        assert_previous_tail(&retried, &tail);
        assert_settled(&retried);
    }

    #[test]
    fn installed_transport_is_consumed_before_publication() {
        let retried = retried();
        previous_tail(&retried);
        let transport = install(&retried).expect("transport");
        let buffer = {
            let chats = retried.service.active_chats.lock().expect("chats");
            let entry = chats.get(&key(&retried)).expect("replacement");
            assert_eq!(entry.job_id, retried.job_id);
            assert_eq!(entry.attempt_id, retried.replacement);
            entry.buffer.clone()
        };
        assert_eq!(
            transport
                .superseded
                .as_ref()
                .map(|tail| tail.attempt_id.as_str()),
            Some(retried.first.as_str())
        );
        // An event sent the moment the provider could start is already retained.
        transport
            .sender
            .send(ExecutionEvent::Finished)
            .expect("send");
        let state = buffer.state.lock().expect("buffer");
        let (state, _) = buffer
            .cvar
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.terminal)
            .expect("wait");
        assert!(state.terminal);
        assert_eq!(state.events.len(), 1);
    }
}
