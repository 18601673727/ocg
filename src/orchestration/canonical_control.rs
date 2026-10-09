//! Backend-backed control plane for the canonical Job/Attempt lane.
//!
//! This module is deliberately transport-neutral. The PWA and loopback server
//! consume these DTOs; neither owns execution state. The SQLite substrate is
//! the authority, and every response includes a protocol version and the
//! canonical Job snapshot needed to reconcile a reconnect.

use crate::core_contract::{Failure, FailureClass};
use crate::error::{OcgError, Result};
use crate::orchestration::admission::{
    publish_call, reserve, AdmissionContext, AdmissionTarget, PayloadMode, PreparedExecution,
};
use crate::orchestration::disk_guard::{DiskGuard, DiskGuardConfig, DiskGuardStatus};
use crate::orchestration::domain::{
    Attempt, AttemptAuthority, Call, ChildPolicy, DispatchIntent, DomainRepository, Executor, Job,
    JobOrigin, JobSpec,
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
const PROJECTS_FILE: &str = "projects.json";
const CONFIG_FILE: &str = "configuration.json";
/// A terminal chat tail remains replayable for this bounded period after the
/// provider terminal event is recorded. This is transport retention only, not
/// durable chat history.
const CHAT_REPLAY_LIFETIME: Duration = Duration::from_secs(300);
pub(crate) const CHAT_EXECUTION_TIMEOUT: Duration = Duration::from_secs(900);

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
}

static TEMPORARY_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A temp path that can never collide with another writer in this process:
/// the serialization locks already order committed writers, and the unique
/// suffix removes the fixed-name temp race entirely.
fn unique_temporary_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "state".to_string());
    let sequence = TEMPORARY_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    path.with_file_name(format!("{name}.tmp-{}-{sequence}", std::process::id()))
}

/// Write a temp file on the same filesystem, flush it, then atomically rename
/// it over the target. The authoritative file is only replaced by a complete
/// temp; a failure removes the temp and leaves the previous revision intact.
fn atomic_write(path: &Path, bytes: &[u8], context: &'static str) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| OcgError::io("create control state", e))?;
    }
    let temporary = unique_temporary_path(path);
    let result = (|| -> Result<()> {
        let mut file = std::fs::File::create(&temporary).map_err(|e| OcgError::io(context, e))?;
        file.write_all(bytes)
            .map_err(|e| OcgError::io(context, e))?;
        file.sync_all().map_err(|e| OcgError::io(context, e))?;
        drop(file);
        std::fs::rename(&temporary, path).map_err(|e| OcgError::io(context, e))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Hold an exclusive OS lock beside one control-state file for a complete
/// read-modify-write: the read happens under it, so a concurrent process can
/// never commit over an update it did not see. The kernel releases the lock
/// when its holder exits, and readers never take it because every commit is
/// an atomic rename. Holders do only local file work.
fn lock_control_file(path: &Path) -> Result<std::fs::File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| OcgError::io("create control state", e))?;
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path.with_extension("lock"))
        .map_err(|e| OcgError::io("open control state lock", e))?;
    fs2::FileExt::lock_exclusive(&lock).map_err(|e| OcgError::io("lock control state", e))?;
    Ok(lock)
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
    pub boundary: String,
    pub marker: bool,
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
    pub project_defaults: ProjectConfiguration,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CanonicalJobRelations {
    pub parent_job_id: Option<String>,
    #[serde(default)]
    pub origin: Option<JobOrigin>,
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
        origins: &BTreeMap<String, String>,
        origin_details: &BTreeMap<String, JobOrigin>,
    ) -> Self {
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
    control_state: PathBuf,
    runtime_handle: Option<crate::orchestration::execution_runtime::ExecutionRuntimeHandle>,
    runtime_registry: Option<Arc<crate::orchestration::execution_runtime::ProjectRuntimeRegistry>>,
    profile_service: crate::profile::ProfileService,
    chat_registrations: Arc<Mutex<std::collections::HashMap<String, ChatRegistration>>>,
    active_chats: Arc<Mutex<std::collections::HashMap<(String, String), ActiveChat>>>,
    launch_lock: Arc<Mutex<()>>,
    registry_lock: Arc<Mutex<()>>,
    configuration_lock: Arc<Mutex<()>>,
    chat_forwarder: Arc<ChatForwarder>,
    placement_deferrals: Arc<Mutex<BTreeMap<String, PlacementDeferral>>>,
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
    /// When the turn started. The execution deadline is anchored here,
    /// so every SSE attach/reconnect of the same turn shares one deadline.
    started_at: Instant,
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
    pub fn open(root: &Path) -> Result<Self> {
        let boundary = project::resolve(root);
        boundary.require(root)?;

        // Use user-global profile path
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from);
        let profile_path = crate::config::user_config_path(
            None,
            std::env::var_os("OCG_USER_CONFIG")
                .as_deref()
                .map(Path::new),
            std::env::var_os("XDG_CONFIG_HOME")
                .as_deref()
                .map(Path::new),
            home.as_deref(),
        );

        Ok(Self::new(
            boundary.root(),
            &profile_path,
            crate::orchestration::state::state_dir(boundary.root()),
        ))
    }

    pub fn open_process(root: &Path, profile_path: &Path) -> Result<Self> {
        let boundary = project::resolve(root);
        let service = Self::new(
            boundary.root(),
            profile_path,
            project::canonicalize(profile_path)
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("state")
                .join("control"),
        );
        // Adopt the existing control metadata once; Project substrates stay put.
        let legacy = Self::new(
            boundary.root(),
            profile_path,
            crate::orchestration::state::state_dir(boundary.root()),
        );
        // Re-checked under the file lock: another process may have adopted
        // its own legacy state or committed a registration meanwhile.
        if !service.project_file().exists() && legacy.project_file().exists() {
            let _file = lock_control_file(&service.project_file())?;
            if !service.project_file().exists() {
                service.write_projects(&legacy.read_projects()?)?;
            }
        }
        if !service.config_file().exists() && legacy.config_file().exists() {
            let _file = lock_control_file(&service.config_file())?;
            if !service.config_file().exists() {
                let (global, defaults, revision) = legacy.read_configuration()?;
                service.write_configuration(&global, &defaults, revision)?;
            }
        }
        Ok(service)
    }

    fn new(root: &Path, profile_path: &Path, control_state: PathBuf) -> Self {
        Self {
            initial_root: root.to_path_buf(),
            control_state,
            runtime_handle: None,
            runtime_registry: None,
            profile_service: crate::profile::ProfileService::with_workspace(profile_path, root),
            chat_registrations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_chats: Arc::new(Mutex::new(std::collections::HashMap::new())),
            launch_lock: Arc::new(Mutex::new(())),
            registry_lock: Arc::new(Mutex::new(())),
            configuration_lock: Arc::new(Mutex::new(())),
            chat_forwarder: ChatForwarder::new(),
            placement_deferrals: Arc::new(Mutex::new(BTreeMap::new())),
        }
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
        crate::control_security::OwnershipStore::open(&self.control_state.join("security.sqlite"))
    }

    pub(crate) fn chat_project_for(&self, session_id: &str, job_id: &str) -> Option<String> {
        self.active_chats
            .lock()
            .ok()?
            .values()
            .find(|chat| chat.session_id == session_id && chat.job_id == job_id)
            .map(|chat| chat.project_id.clone())
    }

    fn project_file(&self) -> PathBuf {
        self.control_state.join(PROJECTS_FILE)
    }

    fn config_file(&self) -> PathBuf {
        self.control_state.join(CONFIG_FILE)
    }

    fn read_projects(&self) -> Result<Vec<ProjectRecord>> {
        let path = self.project_file();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = std::fs::read(&path).map_err(|e| OcgError::io("read project registry", e))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| invalid(format!("invalid project registry: {e}")))
    }

    fn write_projects(&self, projects: &[ProjectRecord]) -> Result<()> {
        let path = self.project_file();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| OcgError::io("create control state", e))?;
        }
        let bytes = serde_json::to_vec_pretty(projects).map_err(|e| invalid(e.to_string()))?;
        atomic_write(&path, &bytes, "commit project registry")
    }

    fn read_configuration(
        &self,
    ) -> Result<(
        GlobalConfiguration,
        std::collections::BTreeMap<String, ProjectConfiguration>,
        u64,
    )> {
        let path = self.config_file();
        if !path.exists() {
            return Ok((
                GlobalConfiguration::default(),
                std::collections::BTreeMap::new(),
                0,
            ));
        }
        let bytes =
            std::fs::read(&path).map_err(|e| OcgError::io("read canonical configuration", e))?;
        #[derive(Deserialize, Default)]
        struct Stored {
            #[serde(default)]
            global: GlobalConfiguration,
            #[serde(default)]
            project_defaults: Value,
            #[serde(default)]
            revision: u64,
        }
        let stored: Stored = serde_json::from_slice(&bytes)
            .map_err(|e| invalid(format!("invalid canonical configuration: {e}")))?;
        // Compatibility reader for the first draft: it stored one defaults
        // object in a list, before Project identity was part of the scope.
        // Adopt that single entry only for the sole registered Project; if
        // there are multiple Projects, refuse to guess which one owned it.
        let project_defaults = if stored.project_defaults.is_array() {
            let list = stored.project_defaults.as_array().unwrap();
            let projects = self.read_projects()?;
            if list.len() != 1 || projects.len() != 1 {
                return Err(invalid(
                    "legacy project defaults have ambiguous Project scope",
                ));
            }
            let defaults: ProjectConfiguration = serde_json::from_value(list[0].clone())
                .map_err(|e| invalid(format!("invalid legacy project defaults: {e}")))?;
            let mut scoped = std::collections::BTreeMap::new();
            scoped.insert(projects[0].project_id.clone(), defaults);
            scoped
        } else if stored.project_defaults.is_null() {
            std::collections::BTreeMap::new()
        } else {
            serde_json::from_value(stored.project_defaults)
                .map_err(|e| invalid(format!("invalid scoped project defaults: {e}")))?
        };
        Ok((stored.global, project_defaults, stored.revision))
    }

    fn write_configuration(
        &self,
        global: &GlobalConfiguration,
        project_defaults: &std::collections::BTreeMap<String, ProjectConfiguration>,
        revision: u64,
    ) -> Result<()> {
        let path = self.config_file();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| OcgError::io("create control state", e))?;
        }
        let value =
            json!({"global":global,"project_defaults":project_defaults,"revision":revision});
        let bytes = serde_json::to_vec_pretty(&value).map_err(|e| invalid(e.to_string()))?;
        atomic_write(&path, &bytes, "commit canonical configuration")
    }

    /// Register/import an existing repository only after nearest-boundary
    /// resolution and canonical path validation.
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
        boundary.require(requested_root)?;
        let root = project::canonicalize(boundary.root());
        if !root.is_dir() {
            return Err(invalid("project root is not a directory"));
        }
        let marker = root.join(project::MARKER);
        let canonical_marker = marker
            .canonicalize()
            .map_err(|error| OcgError::io("resolve Project marker", error))?;
        if !canonical_marker.starts_with(&root)
            || project::resolve(&canonical_marker).root() != root
        {
            return Err(invalid("Project marker must stay inside its boundary"));
        }
        let boundary_root = project::canonicalize(boundary.root());
        // The durable Project identity is proven by the new root's own
        // `.ocg` store, never by a project_id a client may claim. The store is
        // opened without creating identity so a moved Project is read first:
        // `open` would mint a second row for the same durable store and that
        // new row would then look authoritative.
        let project_id = DomainRepository::open_existing(&root)?
            .reconcile_project(&root)?
            .id;
        let previous = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id);
        if previous.is_some_and(|existing| existing.root != root.to_string_lossy()) {
            // Same durable identity at a new location: this is a repair of
            // registry location authority, not a new Project. A live runtime
            // still pinned to the old root is stopped and joined before the
            // registry may name the new one, so one project_id can never hold
            // two authoritative roots. A failed shutdown aborts registration
            // with the registry untouched. This happens before the registry
            // file lock: joining workers is not bounded local work.
            if let Some(registry) = &self.runtime_registry {
                registry.invalidate(&project_id)?;
            }
        }
        // The commit merges this one record into the registry as committed
        // now, keyed by project_id, so concurrent registrations all survive.
        let project = {
            let _file = lock_control_file(&self.project_file())?;
            let mut projects = self.read_projects()?;
            let record = projects
                .iter_mut()
                .find(|project| project.project_id == project_id);
            let project = if let Some(existing) = record {
                // Identity and created_at stay; the observed location metadata
                // is replaced.
                existing.root = root.to_string_lossy().to_string();
                existing.boundary = boundary_root.to_string_lossy().to_string();
                existing.marker = boundary.has_marker();
                existing.updated_at = now;
                existing.clone()
            } else {
                let project = ProjectRecord {
                    project_id,
                    root: root.to_string_lossy().to_string(),
                    boundary: boundary_root.to_string_lossy().to_string(),
                    marker: boundary.has_marker(),
                    created_at: now,
                    updated_at: now,
                };
                projects.push(project.clone());
                project
            };
            self.write_projects(&projects)?;
            project
        };
        Ok(CanonicalProjectResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project,
        })
    }

    pub fn projects(&self) -> Result<Vec<ProjectRecord>> {
        self.read_projects()
    }

    pub fn configuration(&self, project_id: &str) -> Result<ProjectConfigurationView> {
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        let (global, project_defaults, _) = self.read_configuration()?;
        let defaults = project_defaults
            .get(project_id)
            .cloned()
            .unwrap_or_default();
        Ok(ProjectConfigurationView {
            project,
            global,
            project_defaults: defaults,
        })
    }

    pub fn set_global_configuration(
        &self,
        command_id: &str,
        config: GlobalConfiguration,
        _now: i64,
    ) -> Result<CanonicalConfigurationResponse> {
        if !safe_id(command_id) {
            return Err(invalid("invalid command_id"));
        }
        if let Some(storage_guard) = config.storage_guard.as_ref() {
            storage_guard.validate()?;
        }
        // One serialization boundary covers the whole read-modify-write: the
        // latest authoritative revision is read, mutated and committed while
        // no other configuration writer can interleave.
        let revision = {
            let _configuration = self
                .configuration_lock
                .lock()
                .map_err(|_| invalid("configuration authority poisoned"))?;
            let _file = lock_control_file(&self.config_file())?;
            let (_old, project_defaults, revision) = self.read_configuration()?;
            let revision = revision.saturating_add(1);
            // Global configuration and project defaults are separate scopes:
            // this never rewrites a project's own defaults.
            self.write_configuration(&config, &project_defaults, revision)?;
            revision
        };
        let project = self
            .read_projects()?
            .into_iter()
            .next()
            .ok_or_else(|| invalid("register a Project before configuring OCG"))?;
        let view = ProjectConfigurationView {
            project: project.clone(),
            global: config,
            project_defaults: ProjectConfiguration::default(),
        };
        Ok(CanonicalConfigurationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project_id: project.project_id,
            revision,
            configuration: view,
        })
    }

    pub fn set_project_defaults(
        &self,
        command_id: &str,
        project_id: &str,
        defaults: Value,
        _now: i64,
    ) -> Result<CanonicalConfigurationResponse> {
        if !safe_id(command_id) || !defaults.is_object() {
            return Err(invalid("invalid project configuration command"));
        }
        provider_concurrency(&defaults)?;
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        let (global, revision) = {
            let _configuration = self
                .configuration_lock
                .lock()
                .map_err(|_| invalid("configuration authority poisoned"))?;
            let _file = lock_control_file(&self.config_file())?;
            let (global, mut project_defaults, revision) = self.read_configuration()?;
            let revision = revision.saturating_add(1);
            // Scoped per Project: editing one project's defaults never touches
            // another project's persisted defaults or the global scope.
            project_defaults.insert(
                project_id.to_string(),
                ProjectConfiguration {
                    defaults: defaults.clone(),
                },
            );
            self.write_configuration(&global, &project_defaults, revision)?;
            (global, revision)
        };
        let view = ProjectConfigurationView {
            project: project.clone(),
            global,
            project_defaults: ProjectConfiguration { defaults },
        };
        Ok(CanonicalConfigurationResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            command_id: command_id.to_string(),
            accepted: true,
            project_id: project.project_id,
            revision,
            configuration: view,
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
        let boundary = project::resolve(&canonical_root);
        if canonical_root != root || !boundary.has_marker() || boundary.root() != root {
            return Ok(CandidateRepository::Unavailable(
                "registered Project boundary is no longer valid",
            ));
        }
        let marker = root.join(project::MARKER);
        let canonical_marker = match marker.canonicalize() {
            Ok(canonical_marker) => canonical_marker,
            Err(_) => {
                return Ok(CandidateRepository::Unavailable(
                    "registered Project boundary is no longer valid",
                ))
            }
        };
        if !canonical_marker.starts_with(root) || project::resolve(&canonical_marker).root() != root
        {
            return Ok(CandidateRepository::Unavailable(
                "registered Project marker must stay inside its boundary",
            ));
        }
        // Read the identity this store already holds. Opening it never mints a
        // new Project, so a Job lookup can never create one.
        let repository = DomainRepository::open_existing(root)?;
        if repository.project_at_root(root)?.map(|project| project.id)
            != Some(project.project_id.clone())
        {
            return Err(invalid(
                "Project identity does not own this durable boundary",
            ));
        }
        Ok(CandidateRepository::Ready(repository))
    }

    fn repository_for_job(&self, job_id: &str) -> Result<DomainRepository> {
        let mut owner = None;
        let mut skipped = 0usize;
        for project in self.read_projects()? {
            // Search every registered Project as a candidate owner, but only
            // skip a candidate whose boundary is gone. A candidate whose store
            // cannot be opened, read, or proven to own its own identity fails
            // the lookup here instead of being silently passed over.
            let repository = match self.candidate_repository(&project)? {
                CandidateRepository::Ready(repository) => repository,
                CandidateRepository::Unavailable(_) => {
                    skipped += 1;
                    continue;
                }
            };
            let job = repository.job(job_id)?;
            if job.is_some_and(|job| job.project_id == project.project_id) {
                if owner.is_some() {
                    return Err(invalid("ambiguous canonical Job identity"));
                }
                owner = Some(repository);
            }
        }
        owner.ok_or_else(|| {
            if skipped > 0 {
                invalid(format!(
                    "unknown canonical Job ({skipped} unavailable Project candidate{} skipped)",
                    if skipped == 1 { "" } else { "s" }
                ))
            } else {
                invalid("unknown canonical Job")
            }
        })
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
                    failure_reason: if role == ChatMessageRole::Assistant
                        && message.state == MessageLifecycle::Failed
                        && origin.attempt_state != "cancelled"
                    {
                        origin
                            .failure_reason
                            .as_ref()
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
        let origins = std::mem::take(&mut snapshot.job_origins);
        let origin_details = std::mem::take(&mut snapshot.job_origin_details);
        let projection = ExecutionProjection::from(snapshot);
        Self::snapshot_from_projection(
            project_id,
            job_id,
            &projection,
            &origins,
            &origin_details,
            &repository,
        )
    }

    fn snapshot_from_projection(
        project_id: &str,
        job_id: &str,
        projection: &ExecutionProjection,
        origins: &BTreeMap<String, String>,
        origin_details: &BTreeMap<String, JobOrigin>,
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
        let relations =
            CanonicalJobRelations::from_projection(job, projection, origins, origin_details);
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
        let origins = std::mem::take(&mut snapshot.job_origins);
        let origin_details = std::mem::take(&mut snapshot.job_origin_details);
        let projection = ExecutionProjection::from(snapshot);
        let mut jobs: Vec<CanonicalJobSummary> = projection
            .jobs
            .values()
            .filter(|job| job.project_id == project_id)
            .map(|job| {
                let relations = CanonicalJobRelations::from_projection(
                    job,
                    &projection,
                    &origins,
                    &origin_details,
                );
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
                Self::snapshot_from_projection(
                    project_id,
                    id,
                    &projection,
                    &origins,
                    &origin_details,
                    &repository,
                )
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
    pub(crate) fn reconcile_watchdog(&self) -> Result<()> {
        let mut failure = None;
        let global = self
            .read_configuration()
            .map(|(global, _, _)| global)
            .unwrap_or_default();
        for project in self.projects()? {
            // Watchdog reconciliation doubles as a bounded disk reassessment
            // point. Fencing itself never consults the Guard — reconciliation
            // is essential settlement — but replacement execution always flows
            // back through Admission, where the Guard blocks it until recovery.
            if let Ok(guard) =
                self.disk_guard_for_project(&project.project_id, Path::new(&project.root), &global)
            {
                guard.refresh_if_stale();
            }
            let mut domain = match DomainRepository::open_existing(Path::new(&project.root)) {
                Ok(domain) => domain,
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
            };
            let runtime = if let Some(registry) = &self.runtime_registry {
                registry.handle(&project.project_id)?
            } else {
                self.runtime_handle.clone().filter(|handle| {
                    handle.project_root() == Path::new(&project.root) && !handle.is_cancelled()
                })
            };
            if let Err(error) = super::watchdog::reconcile_repository(&mut domain, runtime.as_ref())
            {
                failure.get_or_insert(error);
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
        let (global, _, configuration_revision) = self.read_configuration().unwrap_or_default();
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
                let (_, domain) = self.project_repository(&project.project_id)?;
                // Serialize the complete Attempt-to-Call handoff across consumers;
                // SQLite still guards every authoritative claim independently.
                let lock = std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(domain.path().with_extension("admission.lock"))
                    .map_err(|error| OcgError::io("open Job admission lock", error))?;
                match fs2::FileExt::try_lock_exclusive(&lock) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) => return Err(OcgError::io("lock Job admission", error)),
                }
                let jobs = domain.automatic_admission_jobs(&project.project_id)?;
                if !domain.pending_dispatch_intents()?.is_empty() {
                    if let Some(registry) = &self.runtime_registry {
                        if registry.handle(&project.project_id)?.is_none() {
                            let (_, defaults, _) = self.read_configuration()?;
                            let concurrency = provider_concurrency(
                                &defaults
                                    .get(&project.project_id)
                                    .cloned()
                                    .unwrap_or_default()
                                    .defaults,
                            )?;
                            registry.get_or_start(
                                &project.project_id,
                                Path::new(&project.root),
                                concurrency,
                            )?;
                        }
                    }
                }
                for job in jobs {
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
            let (global, _, _) = self.read_configuration()?;
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

    pub fn cancel_job(
        &self,
        job_id: &str,
        expected_generation: u64,
    ) -> Result<CanonicalJobOperationResponse> {
        let mut domain = self.repository_for_job(job_id)?;
        let job = domain
            .job(job_id)?
            .ok_or_else(|| invalid("unknown canonical Job"))?;
        let accepted = job.state.can_cancel();
        for (authority, never_started) in
            domain.request_job_cancel_cascade(job_id, expected_generation)?
        {
            let runtime = if let Some(registry) = &self.runtime_registry {
                registry.handle(&job.project_id)?
            } else {
                match &self.runtime_handle {
                    Some(handle)
                        if domain
                            .project_at_root(handle.project_root())?
                            .is_some_and(|project| project.id == job.project_id) =>
                    {
                        Some(handle.clone())
                    }
                    _ => None,
                }
            };
            let signalled = match runtime {
                Some(runtime) => runtime
                    .provider_dispatcher()
                    .cancel_attempt(&authority.attempt_id)?,
                None => false,
            };
            domain.confirm_cancel(&authority.attempt_id, signalled || never_started)?;
            if let Ok(chats) = self.active_chats.lock() {
                for chat in chats
                    .values()
                    .filter(|chat| chat.attempt_id == authority.attempt_id)
                {
                    chat.cancelled.cancel();
                    if let Err(error) = chat
                        .sender
                        .send(ExecutionEvent::Failed("chat cancelled".to_string()))
                    {
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
            let (global, _, _) = self.read_configuration()?;
            self.disk_guard_for_project(&job.project_id, Path::new(&project.root), &global)?
        };
        if let Some(deferral) = retry_guard.check_execution_admission().deferral() {
            return Err(invalid(deferral.message()));
        }
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
        let runtime = if template.is_some() {
            let (project, _) = self.project_repository(&job.project_id)?;
            let (_, defaults, _) = self.read_configuration()?;
            let concurrency = provider_concurrency(
                &defaults
                    .get(&job.project_id)
                    .cloned()
                    .unwrap_or_default()
                    .defaults,
            )?;
            Some(if let Some(registry) = &self.runtime_registry {
                registry.get_or_start(&job.project_id, Path::new(&project.root), concurrency)?
            } else {
                self.runtime_handle
                    .clone()
                    .filter(|handle| {
                        handle.project_root() == Path::new(&project.root) && !handle.is_cancelled()
                    })
                    .ok_or_else(|| invalid("execution runtime is not available for this Project"))?
            })
        } else {
            None
        };
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
        let executor_kind = if replay.is_some() {
            "provider"
        } else {
            snapshot
                .executors
                .iter()
                .find(|executor| {
                    snapshot.attempts.iter().any(|attempt| {
                        attempt.id == executor.attempt_id
                            && attempt.job_id == job_id
                            && attempt.generation == job.generation
                    })
                })
                .map(|executor| executor.kind.as_str())
                .unwrap_or("worker")
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
        if chat_session.is_some() && replay.is_none() {
            return Err(invalid("Chat retry has no frozen provider target"));
        }
        let (global, _, _) = self.read_configuration()?;
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
                    started_at: Instant::now(),
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
        let (global_config, project_configs, configuration_revision) = self.read_configuration()?;
        let project_config = project_configs
            .get(&project.project_id)
            .cloned()
            .unwrap_or_default();
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
                    handle.project_root() == Path::new(&project.root) && !handle.is_cancelled()
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
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "failed",
                    existing_job.as_ref().map(|job| job.id.as_str()),
                    &response.message,
                )?;
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
        let provider_key = resolved.reservation.choice.provider.as_str();
        let model = resolved.reservation.choice.model.as_str();

        let provider_entry = profile
            .providers
            .get(provider_key)
            .ok_or_else(|| invalid("resolved provider disappeared"))?;

        if !images.is_empty()
            && profile
                .models
                .get(model)
                .and_then(|entry| entry.metadata.as_ref())
                .is_some_and(|metadata| {
                    metadata.images == Some(false)
                        || (metadata.images.is_none() && metadata.multimodal == Some(false))
                })
        {
            return Err(invalid("selected model does not support image input"));
        }

        let effort = selection
            .as_ref()
            .and_then(|selection| selection.effort.as_deref());
        if let Some(effort) = effort {
            let entry = profile
                .models
                .get(model)
                .ok_or_else(|| invalid("selected model missing"))?;
            let supported = super::placement::supports_effort(entry, effort);
            if !supported
                || !provider_entry.wire_protocol().is_openai_chat_completions()
                || !matches!(
                    effort,
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
                )
            {
                return Err(invalid(
                    "selected reasoning effort is unsupported for this model/protocol",
                ));
            }
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
        // Reservation is the canonical admission transition. Chat and generic
        // launch both pass through it; only the payload built afterwards differs.
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
        let user_content = initial_user_message(&request);
        let messages = if reserved.existing_call {
            Vec::new()
        } else if is_chat {
            match domain.prepare_chat_turn_with_images(
                &request,
                &request_hash,
                &reserved.attempt,
                &user_content,
                &images,
            ) {
                Ok(messages) => messages,
                Err(error) => {
                    domain.finish_attempt(&reserved.attempt.id, false)?;
                    return Err(error);
                }
            }
        } else {
            vec![json!({"role": "user", "content": user_content})]
        };
        let mut provider_request = serde_json::json!({
            "model": model,
            "messages": messages,
            "stream": true,
        });
        if let Some(effort) = effort {
            provider_request["reasoning_effort"] = json!(effort);
        }
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
        let (probe_global, _, _) = self.read_configuration()?;
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
        let runtime = if let Some(registry) = &self.runtime_registry {
            registry.get_or_start(&project.project_id, Path::new(&project.root), 1)
        } else {
            self.runtime_handle
                .as_ref()
                .filter(|handle| {
                    handle.project_root() == Path::new(&project.root) && !handle.is_cancelled()
                })
                .cloned()
                .ok_or_else(|| invalid("execution runtime is not available for this Project"))
        };
        let runtime_handle = match runtime {
            Ok(handle) => handle,
            Err(error) => {
                record(
                    &mut domain,
                    &super::health_probe::unsupported_target(
                        "health_probe_no_runtime",
                        error.to_string(),
                    ),
                )?;
                return Ok(rejected(error.to_string(), Some(job.id.clone())));
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
        let (project, _) = self.project_repository(&request.project_id)?;
        crate::chat_images::upload(Path::new(&project.root), request)
    }

    pub fn read_chat_image(&self, project_id: &str, image_id: &str) -> Result<(String, Vec<u8>)> {
        let (project, _) = self.project_repository(project_id)?;
        crate::chat_images::read(Path::new(&project.root), image_id)
    }

    pub fn receive_chat_image_for_turn(
        &self,
        session_id: &str,
        job_id: &str,
        url: &str,
    ) -> Result<crate::contracts::ChatImage> {
        let project_id = self
            .active_chats
            .lock()
            .map_err(|_| invalid("active chats poisoned"))?
            .values()
            .find(|chat| chat.session_id == session_id && chat.job_id == job_id)
            .map(|chat| chat.project_id.clone())
            .ok_or_else(|| invalid("unknown chat turn"))?;
        self.receive_chat_image(&project_id, url)
    }

    pub fn receive_chat_image(
        &self,
        project_id: &str,
        url: &str,
    ) -> Result<crate::contracts::ChatImage> {
        let (project, _) = self.project_repository(project_id)?;
        crate::chat_images::received(Path::new(&project.root), project_id, url)
    }

    pub fn launch_chat_with_images(
        &self,
        request: crate::contracts::JobLaunchRequest,
        selection: Option<crate::contracts::ChatModelSelection>,
        image_ids: &[String],
        _now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        let (project, _) = self.project_repository(&request.project_id)?;
        let images =
            crate::chat_images::selected(Path::new(&project.root), &request.project_id, image_ids)?;
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
                        started_at: Instant::now(),
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

    /// Clone the retained tail for one SSE attach, together with the turn's
    /// start time. The entry stays for cancellation until `finish_chat`
    /// removes it. Late attach replays from index zero; live events follow
    /// once the prefix is drained.
    pub(crate) fn chat_buffer_for(
        &self,
        session_id: &str,
        job_id: &str,
    ) -> Option<(std::sync::Arc<ChatEventBuffer>, Instant)> {
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
        Some((entry.buffer.clone(), entry.started_at))
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

    pub(crate) fn cancel_chat_turn(&self, session_id: &str, job_id: &str) -> Result<bool> {
        let active = {
            let mut chats = self
                .active_chats
                .lock()
                .map_err(|_| invalid("active chats poisoned"))?;
            let key = chats
                .iter()
                .find(|(_, entry)| entry.session_id == session_id && entry.job_id == job_id)
                .map(|(key, _)| key.clone());
            key.and_then(|key| chats.remove(&key))
        };
        match active {
            Some(active) => self.end_active_chat(active),
            None => Ok(false),
        }
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
                started_at: Instant::now(),
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
