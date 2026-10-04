//! Backend-backed control plane for the canonical Job/Attempt lane.
//!
//! This module is deliberately transport-neutral. The PWA and loopback server
//! consume these DTOs; neither owns execution state. The SQLite substrate is
//! the authority, and every response includes a protocol version and the
//! canonical Job snapshot needed to reconcile a reconnect.

use crate::error::{OcgError, Result};
use crate::orchestration::domain::{Attempt, Call, DispatchIntent, DomainRepository, Executor};
use crate::orchestration::journal::{EventDelta, ExecutionProjection, MAX_EVENT_READ};
use crate::project::{self, ProjectBoundary};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use crate::orchestration::execution_dispatch::{CallCancellation, ExecutionEvent};
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
        let mut file =
            std::fs::File::create(&temporary).map_err(|e| OcgError::io(context, e))?;
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

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    endpoint.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['/', '?', '#']).next().unwrap_or("").contains('@')
    })
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, TS)]
pub struct ProjectConfiguration {
    pub defaults: Value,
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
    pub jobs: Vec<Value>,
    pub selected_job: Option<CanonicalJobSnapshot>,
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
        if !service.project_file().exists() && legacy.project_file().exists() {
            service.write_projects(&legacy.read_projects()?)?;
        }
        if !service.config_file().exists() && legacy.config_file().exists() {
            let (global, defaults, revision) = legacy.read_configuration()?;
            service.write_configuration(&global, &defaults, revision)?;
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
        }
    }

    pub fn with_runtime_handle(
        mut self,
        handle: crate::orchestration::execution_runtime::ExecutionRuntimeHandle,
    ) -> Self {
        self.runtime_handle = Some(handle);
        self
    }

    pub fn with_profile_service(
        mut self,
        profile_service: crate::profile::ProfileService,
    ) -> Self {
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
        let bytes = serde_json::to_vec_pretty(&value)
            .map_err(|e| invalid(e.to_string()))?;
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
        let _registry = self.registry_lock.lock()
            .map_err(|_| invalid("Project registry poisoned"))?;
        let boundary: ProjectBoundary = project::resolve(requested_root);
        boundary.require(requested_root)?;
        let root = project::canonicalize(boundary.root());
        if !root.is_dir() {
            return Err(invalid("project root is not a directory"));
        }
        let marker = root.join(project::MARKER);
        let canonical_marker = marker.canonicalize()
            .map_err(|error| OcgError::io("resolve Project marker", error))?;
        if !canonical_marker.starts_with(&root) || project::resolve(&canonical_marker).root() != root {
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
        let mut projects = self.read_projects()?;
        let record = projects
            .iter_mut()
            .find(|project| project.project_id == project_id);
        let project = if let Some(existing) = record {
            // Same durable identity at a new location: this is a repair of
            // registry location authority, not a new Project. Identity and
            // created_at stay; the observed location metadata is replaced.
            let root_moved = existing.root != root.to_string_lossy();
            if root_moved {
                // A live runtime still pinned to the old root is stopped and
                // joined before the registry may name the new one, so one
                // project_id can never hold two authoritative roots. A failed
                // shutdown aborts registration with the registry untouched.
                if let Some(registry) = &self.runtime_registry {
                    registry.invalidate(&project_id)?;
                }
            }
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
        // One serialization boundary covers the whole read-modify-write: the
        // latest authoritative revision is read, mutated and committed while
        // no other configuration writer can interleave.
        let _configuration = self
            .configuration_lock
            .lock()
            .map_err(|_| invalid("configuration authority poisoned"))?;
        let (_old, project_defaults, revision) = self.read_configuration()?;
        let revision = revision.saturating_add(1);
        // Global configuration and project defaults are separate scopes: this
        // never rewrites a project's own defaults.
        self.write_configuration(&config, &project_defaults, revision)?;
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
        if !canonical_marker.starts_with(root) || project::resolve(&canonical_marker).root() != root {
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
                        .into_iter()
                        .filter(|block| block.kind == MessageBlockKind::Markdown)
                        .filter_map(|block| block.content)
                        .collect::<Vec<_>>()
                        .join("\n\n"),
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
        let snapshot = repository.execution_snapshot()?;
        let projection = ExecutionProjection::from(snapshot);
        let job = projection
            .jobs
            .get(job_id)
            .cloned()
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
            .filter(|executor| attempts.iter().any(|attempt| attempt.id == executor.attempt_id))
            .cloned()
            .collect();
        let dispatch_intents: Vec<DispatchIntent> = projection
            .dispatch_intents
            .values()
            .filter(|intent| intent.job_id == job.id)
            .cloned()
            .collect();
        let value = json!({
            "job":job,
            "attempts":attempts,
            "executors":executors,
            "calls":calls,
            "dispatch_intents":dispatch_intents,
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
        if !repository.job(job_id)?
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
        let jobs = repository.jobs(project_id)?.into_iter().map(|job| json!({"job_id":job.id,"created_at":job.created_at,"state":job.state,"updated_at":job.updated_at})).collect();
        let selected_job = job_id
            .map(|id| self.canonical_snapshot(&project.project_id, id))
            .transpose()?;
        Ok(CanonicalDashboardResponse {
            api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
            project_id: project.project_id,
            jobs,
            selected_job,
        })
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

    fn launch_job_inner(
        &self,
        request: crate::contracts::JobLaunchRequest,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        use sha2::{Sha256, Digest};

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
            hasher.update(
                serde_json::to_vec(selection).map_err(|error| invalid(error.to_string()))?,
            );
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
        if let Some((stored_hash, job_id, outcome, message)) = domain.lookup_launch_command(
            &request.command_id,
            &request.project_id,
        )? {
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

        let runtime = if let Some(registry) = &self.runtime_registry {
            registry.get_or_start(&project.project_id, Path::new(&project.root))
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
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        // Resolve configuration
        let (global_config, project_configs, _revision) = self.read_configuration()?;
        let project_config = project_configs
            .get(&project.project_id)
            .cloned()
            .unwrap_or_default();

        // Resolve profile from the user-global profile service
        let (profile, _) = match self.profile_service.current()? {
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
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
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
            Some(profile.select(
                selection.as_ref().map(|selection| selection.model.as_str())
                    .or_else(|| project_config.defaults.get("model").and_then(Value::as_str)),
            )?)
        } else {
            None
        };

        // Resolve provider and model from project configuration defaults
        let provider_key = match selection.as_ref()
            .and_then(|_| chat_selection.map(|(_, model)| model.provider.as_str()))
            .or_else(|| project_config.defaults.get("provider").and_then(Value::as_str))
            .or_else(|| chat_selection.map(|(_, model)| model.provider.as_str()))
        {
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
                    message: "provider not configured".to_string(),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        let model = match selection.as_ref().map(|selection| selection.model.as_str())
            .or_else(|| project_config.defaults.get("model").and_then(Value::as_str))
            .or_else(|| chat_selection.map(|(key, _)| key))
        {
            Some(m) => m,
            None => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: "model not configured".to_string(),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        let provider_entry = match profile.providers.get(provider_key) {
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
                    message: format!("provider not found: {}", provider_key),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        let upstream_model_id = match profile.models.get(model) {
            Some(entry) if entry.provider == provider_key && !entry.id.is_empty() => {
                entry.id.clone()
            }
            _ => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: format!("model {model} is not runnable for provider {provider_key}"),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id, &request.project_id, &request_hash,
                    "rejected", None, &response.message,
                )?;
                return Ok(response);
            }
        };

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
            let metadata = entry.metadata.as_ref();
            let supported = metadata.and_then(|metadata| metadata.efforts.as_ref())
                .is_some_and(|efforts| efforts.iter().any(|candidate| candidate == effort))
                || entry.variants.iter().any(|candidate| candidate == effort)
                || metadata.and_then(|metadata| metadata.variants.as_ref())
                    .is_some_and(|variants| variants.iter().any(|candidate| candidate == effort));
            if !supported
                || !provider_entry.wire_protocol().is_openai_chat_completions()
                || !matches!(effort, "none" | "minimal" | "low" | "medium" | "high" | "xhigh")
            {
                return Err(invalid(
                    "selected reasoning effort is unsupported for this model/protocol",
                ));
            }
        }

        // Validate endpoint
        let endpoint = match &provider_entry.endpoint {
            Some(e) if !e.is_empty() => e.clone(),
            _ => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: format!("provider {} has no endpoint configured", provider_key),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        };

        // Credentials are optional: an endpoint that needs no bearer token is a
        // legitimate provider, so only a declared reference has to resolve.
        if endpoint_has_userinfo(&endpoint) {
            let response = crate::contracts::JobLaunchResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "rejected".to_string(),
                command_id: request.command_id.clone(),
                draft_id: request.draft_id.clone(),
                project_id: project.project_id.clone(),
                session_id: request.session_id.clone(),
                job_id: None,
                message: format!("provider {provider_key} endpoint must not contain userinfo"),
                duplicate: false,
            };
            domain.record_launch_command(
                &request.command_id,
                &request.project_id,
                &request_hash,
                "rejected",
                None,
                &response.message,
            )?;
            return Ok(response);
        }

        let credential_ref = provider_entry.credential_ref.clone();
        if let Some(reference) = credential_ref.as_deref() {
            // Verify a declared credential exists without keeping it in memory;
            // the raw value is only read again at execution time.
            let vault = crate::vault::Vault::user_global()?;
            if vault.get(reference)?.is_none() {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: format!("credential not found: {reference}"),
                    duplicate: false,
                };
                domain.record_launch_command(
                    &request.command_id,
                    &request.project_id,
                    &request_hash,
                    "rejected",
                    None,
                    &response.message,
                )?;
                return Ok(response);
            }
        }

        // Build budget config from resource_budget if present
        let budget_config = if let Some(resource_budget) = &global_config.resource_budget {
            let budget_data = serde_json::json!({
                "budget": {
                    "currency": &resource_budget.unit,
                    "hardLimitMicros": (resource_budget.hard_limit * 1_000_000.0) as i64,
                }
            });
            crate::orchestration::budget::BudgetConfig::from_config(&budget_data)?
        } else {
            crate::orchestration::budget::BudgetConfig::default()
        };

        // Create canonical Job/Attempt/Executor. The Job spec is the durable
        // launch intent: what was asked, what success looks like, the
        // constraints, and the declared budget commitment. Frozen provider
        // transport identity (endpoint, upstream model id, credential
        // reference) belongs to the Call's DispatchIntent, not to the Job.
        // The boundary already proved this Project identity; read it back
        // rather than creating one, so a launch can never mint identity for a
        // boundary whose durable row is missing.
        let canonical_project = domain
            .project_at_root(Path::new(&project.root))?
            .ok_or_else(|| invalid("registered Project identity is not durable at this root"))?;
        let job_payload = serde_json::to_string(&serde_json::json!({
            "provider": provider_key,
            "model": model,
            "objective": request.objective,
            "success_criteria": request.success_criteria,
            "constraints": request.constraints,
            "hard_budget_micros": request.hard_budget_micros,
            "resource_commitment": request.resource_commitment,
        }))
        .map_err(|e| invalid(format!("cannot serialize job payload: {e}")))?;

        let job = domain.create_job(&canonical_project.id, &job_payload)?;
        // Admission is one canonical step, not three. `dispatch_job` is the
        // domain operation that makes a newly created Job dispatchable: inside
        // a single immediate transaction it performs the `pending -> eligible`
        // transition, claims the authoritative Attempt (which carries the Job
        // to `running` with its authoritative Attempt) and publishes the
        // provider Executor. An Attempt may only be claimed out of `eligible`,
        // so a launch cannot create the Attempt directly off `create_job`;
        // reusing the transition keeps this lane on the same scheduler
        // semantics `ocg work dispatch` and admission already use.
        let (attempt, executor) = domain.dispatch_job(&job.id, "provider")?;

        let user_content = initial_user_message(&request);
        let messages = if is_chat {
            match domain.prepare_chat_turn_with_images(
                &request,
                &request_hash,
                &attempt,
                &user_content,
                &images,
            ) {
                Ok(messages) => messages,
                Err(error) => {
                    domain.finish_attempt(&attempt.id, false)?;
                    return Err(error);
                }
            }
        } else {
            vec![json!({"role": "user", "content": user_content})]
        };
        // This snapshot becomes the immutable Call/DispatchIntent input. The
        // worker compacts these messages without reading the Conversation again.
        let mut provider_request = serde_json::json!({
            "model": model,
            "messages": messages,
            "stream": true,
        });

        if let Some(effort) = effort {
            provider_request["reasoning_effort"] = json!(effort);
        }

        // Resolve quota facts for economic admission
        let quota_facts = crate::orchestration::budget::QuotaFacts::unknown();

        // Admit the provider call
        let authority = domain.authority(&attempt.id)?.ok_or_else(|| invalid("attempt authority disappeared"))?;

        // Freeze provider execution configuration for this Call
        let provider_config = crate::orchestration::execution_dispatch::ProviderExecutionConfig {
            provider_key: provider_key.to_string(),
            model: model.to_string(),
            upstream_model_id,
            endpoint: endpoint.clone(),
            credential_ref,
        };

        // The protocol is declared by the provider the Profile names, not
        // inferred from its key, label or host, and it is frozen with the rest
        // of the dispatch.
        let protocol = provider_entry.wire_protocol();

        let chat = self.chat_registrations.lock().ok().and_then(|registrations| {
            registrations.get(&request.command_id).map(|registration| {
                (registration.sender.clone(), registration.cancelled.clone())
            })
        });
        let _call = match crate::provider_loop::admit_provider_call_with_events(
            crate::provider_loop::ProviderCallAdmission {
                domain: &mut domain,
                authority: &authority,
                executor_id: &executor.id,
                request: provider_request,
                config: &budget_config,
                quota: quota_facts,
                dispatcher: runtime_handle.provider_dispatcher(),
                provider_config,
                protocol,
            },
            chat.as_ref().map(|(sender, _)| sender.clone()),
            chat.as_ref()
                .map(|(_, cancelled)| cancelled.clone())
                .unwrap_or_default(),
        ) {
            Ok((call, _)) => call,
            Err(e) => {
                // Economic admission failed; finish the attempt as failed
                domain.discard_unaccepted_chat_turn(&attempt.id)?;
                domain.finish_attempt(&attempt.id, false)?;
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "failed".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: Some(job.id.clone()),
                    message: format!("economic admission failed: {}", e),
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

        // Record successful launch
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
            attempts.last().map(|attempt| attempt.id.clone()).unwrap_or_default()
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
            let (_, mut domain) = self.project_repository(&request.project_id)?;
            if domain.request_cancel(&attempt_id).is_ok() {
                let _ = domain.confirm_cancel(&attempt_id, true);
            }
            return Ok(response);
        }
        // Retain every provider event from this point on. The forwarder
        // appends in order the moment the provider produces an event, so a
        // later EventSource attach replays the prefix instead of losing it.
        {
            let forward_buffer = buffer.clone();
            std::thread::Builder::new()
                .name("chat-forward".to_string())
                .spawn(move || {
                    for event in receiver.iter() {
                        let terminal = matches!(
                            &event,
                            ExecutionEvent::Finished | ExecutionEvent::Failed(_)
                        );
                        if let Ok(state) = forward_buffer.state.lock() {
                            // The mutex is only poisoned on panic; retain
                            // what arrived rather than dropping the tail.
                            let mut guard = state;
                            guard.events.push(event);
                            if terminal {
                                guard.terminal = true;
                                guard.terminal_at = Some(Instant::now());
                            }
                        }
                        forward_buffer.cvar.notify_all();
                        if terminal {
                            break;
                        }
                    }
                })
                .ok();
        }
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
            let (_, mut domain) = self.project_repository(&request.project_id)?;
            if domain.request_cancel(&attempt_id).is_ok() {
                let _ = domain.confirm_cancel(&attempt_id, true);
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
    pub fn finish_chat(&self, session_id: &str, job_id: &str) {
        if let Ok(mut active) = self.active_chats.lock() {
            active.retain(|_, entry| entry.session_id != session_id || entry.job_id != job_id);
        }
    }

    pub fn cancel_chat(&self, session_id: &str) -> Result<bool> {
        self.cancel_chat_in_project(None, session_id)
    }

    pub(crate) fn cancel_chat_turn(&self, session_id: &str, job_id: &str) -> Result<bool> {
        let active = {
            let mut chats = self.active_chats.lock()
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
        let mut domain = self.project_repository(&active.project_id)
            .map(|(_, repository)| repository);
        let revocable = !active.attempt_id.is_empty()
            && domain.as_mut()
                .is_ok_and(|domain| domain.request_cancel(&active.attempt_id).is_ok());
        active.cancelled.cancel();
        let _ = active.sender.send(ExecutionEvent::Failed("chat cancelled".to_string()));
        let mut domain = domain?;
        if revocable {
            let _ = domain.confirm_cancel(&active.attempt_id, true);
            return Ok(true);
        }
        Ok(false)
    }
}
