//! Backend-backed control plane for the canonical Job/Attempt lane.
//!
//! This module is deliberately transport-neutral. The PWA and loopback server
//! consume these DTOs; neither owns execution state. The SQLite substrate is
//! the authority, and every response includes a protocol version and the
//! canonical Mission snapshot needed to reconcile a reconnect.

use crate::error::{OcgError, Result};
use crate::orchestration::domain::{Attempt, Call, DomainRepository};
use crate::orchestration::journal::{EventDelta, ExecutionProjection, MAX_EVENT_READ};
use crate::project::{self, ProjectBoundary};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use ts_rs::TS;

pub const CANONICAL_CONTROL_API_VERSION: &str = "ocg.canonical.v1";
const PROJECTS_FILE: &str = "projects.json";
const CONFIG_FILE: &str = "configuration.json";

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
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

#[derive(Debug, Clone)]
pub struct CanonicalControlService {
    root: PathBuf,
}

impl CanonicalControlService {
    pub fn open(root: &Path) -> Result<Self> {
        let boundary = project::resolve(root);
        boundary.require(root)?;
        Ok(Self {
            root: boundary.root().to_path_buf(),
        })
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
        let value = json!({
            "job":job,
            "attempts":attempts,
            "calls":calls,
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
}
