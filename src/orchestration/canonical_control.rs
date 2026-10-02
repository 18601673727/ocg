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

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
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
    root: PathBuf,
    runtime_handle: Option<crate::orchestration::execution_runtime::ExecutionRuntimeHandle>,
    profile_service: crate::profile::ProfileService,
    chat_registrations: Arc<Mutex<std::collections::HashMap<String, ChatRegistration>>>,
    active_chats: Arc<Mutex<std::collections::HashMap<String, ActiveChat>>>,
}

#[derive(Debug)]
struct ChatRegistration {
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
    /// When the turn started. The 300s execution deadline is anchored here,
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

        Ok(Self {
            root: boundary.root().to_path_buf(),
            runtime_handle: None,
            profile_service: crate::profile::ProfileService::with_workspace(&profile_path, root),
            chat_registrations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            active_chats: Arc::new(Mutex::new(std::collections::HashMap::new())),
        })
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

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn project_file(&self) -> PathBuf {
        crate::orchestration::state::state_dir(&self.root).join(PROJECTS_FILE)
    }

    fn config_file(&self) -> PathBuf {
        crate::orchestration::state::state_dir(&self.root).join(CONFIG_FILE)
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
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(|e| OcgError::io("write project registry", e))?;
        std::fs::rename(&temporary, &path).map_err(|e| OcgError::io("commit project registry", e))
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
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, serde_json::to_vec_pretty(&value).unwrap())
            .map_err(|e| OcgError::io("write canonical configuration", e))?;
        std::fs::rename(&temporary, &path)
            .map_err(|e| OcgError::io("commit canonical configuration", e))
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
        let boundary: ProjectBoundary = project::resolve(requested_root);
        boundary.require(requested_root)?;
        let root = project::canonicalize(boundary.root());
        if !root.is_dir() {
            return Err(invalid("project root is not a directory"));
        }
        let boundary_root = project::canonicalize(boundary.root());
        let project_id = DomainRepository::open(&root)?.ensure_project(&root)?.id;
        let mut projects = self.read_projects()?;
        let record = projects
            .iter_mut()
            .find(|project| project.project_id == project_id);
        let project = if let Some(existing) = record {
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
        let (global, mut project_defaults, revision) = self.read_configuration()?;
        let revision = revision.saturating_add(1);
        // Scoped per Project: editing one project's defaults never touches
        // another project's persisted defaults or the global scope.
        let entry = ProjectConfiguration { defaults };
        project_defaults.insert(project_id.to_string(), entry.clone());
        self.write_configuration(&global, &project_defaults, revision)?;
        let view = ProjectConfigurationView {
            project: project.clone(),
            global,
            project_defaults: entry,
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
        let mut repository = DomainRepository::open(&self.root)?;
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
        DomainRepository::open(&self.root)?.job_configuration(job_id)
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
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        if project.root != self.root.to_string_lossy() {
            return Err(invalid("Project identity does not own this boundary"));
        }
        let repository = DomainRepository::open(&self.root)?;
        let snapshot = repository.execution_snapshot()?;
        let projection = ExecutionProjection::from(snapshot);
        let job = projection
            .jobs
            .get(job_id)
            .cloned()
            .ok_or_else(|| invalid("unknown canonical Job"))?;
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
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        if project.root != self.root.to_string_lossy() {
            return Err(invalid("Project identity does not own this boundary"));
        }
        let repository = DomainRepository::open(&self.root)?;
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
        let project = self
            .read_projects()?
            .into_iter()
            .find(|project| project.project_id == project_id)
            .ok_or_else(|| invalid("unknown Project identity"))?;
        let repository = DomainRepository::open(&self.root)?;
        let canonical_project = repository.ensure_project(&self.root)?;
        let jobs = repository.jobs(&canonical_project.id)?.into_iter().map(|job| json!({"job_id":job.id,"created_at":job.created_at,"state":job.state,"updated_at":job.updated_at})).collect();
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

        // Compute request hash for idempotency conflict detection
        let request_canonical = serde_json::to_string(&request)
            .map_err(|e| invalid(format!("cannot serialize request: {e}")))?;
        let mut hasher = Sha256::new();
        hasher.update(request_canonical.as_bytes());
        let request_hash = format!("{:x}", hasher.finalize());

        // Check for existing command
        let mut domain = crate::orchestration::domain::DomainRepository::open(&self.root)?;
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

        // Early validation: Verify project is registered with this control service
        // This check does not record outcomes to avoid claiming command_id on invalid input
        let project = match self
            .read_projects()?
            .into_iter()
            .find(|p| p.project_id == request.project_id)
        {
            Some(p) => p,
            None => {
                return Ok(crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "rejected".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: request.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: format!("unknown project: {}", request.project_id),
                    duplicate: false,
                });
            }
        };

        // Early validation: Verify execution runtime is available
        let runtime_handle = match &self.runtime_handle {
            Some(h) => h,
            None => {
                let response = crate::contracts::JobLaunchResponse {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    outcome: "failed".to_string(),
                    command_id: request.command_id.clone(),
                    draft_id: request.draft_id.clone(),
                    project_id: project.project_id.clone(),
                    session_id: request.session_id.clone(),
                    job_id: None,
                    message: "execution runtime is not available; Job was not created".to_string(),
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

        // Resolve provider and model from project configuration defaults
        let provider_key = match project_config.defaults.get("provider").and_then(|v| v.as_str()) {
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

        let model = match project_config.defaults.get("model").and_then(|v| v.as_str()) {
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

        if provider_entry.placeholder {
            let response = crate::contracts::JobLaunchResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                outcome: "rejected".to_string(),
                command_id: request.command_id.clone(),
                draft_id: request.draft_id.clone(),
                project_id: project.project_id.clone(),
                session_id: request.session_id.clone(),
                job_id: None,
                message: format!("provider is placeholder: {}", provider_key),
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

        let upstream_model_id = match profile.models.get(model) {
            Some(entry) if !entry.placeholder && entry.provider == provider_key && !entry.id.is_empty() => entry.id.clone(),
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
        let canonical_project = domain.ensure_project(Path::new(&project.root))?;
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

        // Construct initial provider request
        let provider_request = serde_json::json!({
            "model": model,
            "messages": [{"role": "user", "content": initial_user_message(&request)}],
            "stream": true,
        });

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
                .unwrap_or_else(CallCancellation::new),
        ) {
            Ok((call, _)) => call,
            Err(e) => {
                // Economic admission failed; finish the attempt as failed
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
        now: i64,
    ) -> Result<crate::contracts::JobLaunchResponse> {
        self.reap_expired_chats();
        let (sender, receiver) = flume::unbounded();
        let cancelled = CallCancellation::new();
        if let Ok(mut registrations) = self.chat_registrations.lock() {
            registrations.insert(request.command_id.clone(), ChatRegistration {
                sender: sender.clone(),
                cancelled: cancelled.clone(),
            });
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
            .and_then(|mut active| active.remove(&request.session_id));
        let registered = self
            .active_chats
            .lock()
            .map(|mut active| {
                active.insert(
                    request.session_id.clone(),
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

        let launch = self.launch_job(request.clone(), now);
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
                    self.discard_active_turn(&request.session_id);
                }
                if let Some(superseded) = superseded {
                    self.restore_active_turn(superseded);
                }
                return Err(error);
            }
        };
        if response.outcome != "accepted" {
            if registered {
                self.discard_active_turn(&request.session_id);
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
                    self.discard_active_turn(&request.session_id);
                }
                return Ok(response);
            }
        };
        // Resolve the Attempt that launch_job created for this Job. There is
        // exactly one Attempt right after launch.
        let attempt_id = {
            let domain = DomainRepository::open(&self.root)?;
            let attempts = domain.attempts_for_job(&job_id)?;
            attempts.last().map(|attempt| attempt.id.clone()).unwrap_or_default()
        };
        if attempt_id.is_empty() {
            if registered {
                self.discard_active_turn(&request.session_id);
            }
            return Ok(response);
        }
        // The turn was cancelled while it was still launching, so this
        // registration is already gone. Settle the Attempt the launch did
        // create — `cancel_chat` could not, because it had no Attempt identity
        // then — and let the queued envelope be fenced by the worker.
        if cancelled.is_cancelled() {
            let mut domain = DomainRepository::open(&self.root)?;
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
            .map(|mut active| match active.get_mut(&request.session_id) {
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
            let mut domain = DomainRepository::open(&self.root)?;
            if domain.request_cancel(&attempt_id).is_ok() {
                let _ = domain.confirm_cancel(&attempt_id, true);
            }
        }
        Ok(response)
    }

    /// Drop this session's registration without settling anything: the turn it
    /// described never became addressable work.
    fn discard_active_turn(&self, session_id: &str) {
        if let Ok(mut active) = self.active_chats.lock() {
            if let Some(entry) = active.get(session_id) {
                if entry.job_id.is_empty() {
                    active.remove(session_id);
                }
            }
        }
    }

    /// Put a detached turn back under its session, because the launch that was
    /// going to supersede it did not happen. A turn that has meanwhile taken the
    /// session over owns it again and is left alone.
    fn restore_active_turn(&self, superseded: ActiveChat) {
        if let Ok(mut active) = self.active_chats.lock() {
            if !active.contains_key(&superseded.session_id) {
                active.insert(superseded.session_id.clone(), superseded);
            }
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
        let entry = guard.get(session_id)?;
        if entry.job_id != job_id {
            return None;
        }
        if entry
            .buffer
            .state
            .lock()
            .ok()
            .and_then(|state| state.terminal_at)
            .is_some_and(|terminal_at| terminal_at.elapsed() >= CHAT_REPLAY_LIFETIME)
        {
            guard.remove(session_id);
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
            if let Some(entry) = active.get(session_id) {
                if entry.job_id == job_id {
                    active.remove(session_id);
                }
            }
        }
    }

    /// Revoke the Attempt authority first, then stop the provider transport.
    /// A turn that already finished is a no-op returning false.
    pub fn cancel_chat(&self, session_id: &str) -> Result<bool> {
        let active = self
            .active_chats
            .lock()
            .ok()
            .and_then(|mut active| active.remove(session_id));
        let Some(active) = active else { return Ok(false); };
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
        let mut domain = DomainRepository::open(&self.root)?;
        // If authority is already gone (completed/failed), or the turn has not
        // resolved its Attempt yet, there is nothing to revoke; still stop the
        // transport so a queued or running read cannot continue.
        let revocable = !active.attempt_id.is_empty()
            && domain.request_cancel(&active.attempt_id).is_ok();
        active.cancelled.cancel();
        let _ = active.sender.send(ExecutionEvent::Failed("chat cancelled".to_string()));
        if revocable {
            let _ = domain.confirm_cancel(&active.attempt_id, true);
            return Ok(true);
        }
        Ok(false)
    }
}
