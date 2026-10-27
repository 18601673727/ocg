//! Project routes:
//! - `GET /api/v1/projects?before=project:N&limit=M`
//! - `POST /api/v1/projects`
//! - `POST /api/v1/projects/{project}`
//!
//! A Project names a directory, its root, and the commands its Agents may
//! run there by name. The root is resolved here, once, when the Project is
//! created, and never changes; the name, description and commands are
//! replaced as a whole. The commands' argument vectors are sent to the
//! Provider with every request, so they must hold no secret. A Project is
//! never deleted.

use super::contracts::*;
use ntex::http::StatusCode;
use serde::Deserialize;

use super::{INTERNAL, INVALID_REQUEST, Refusal, Reply, Shared, catalog, profiles, submissions};
use crate::db::entities::{
    Project, ProjectCommand, ProjectId, ProjectRecord, RecordRef, Violation,
};
use crate::db::{DatabaseError, DatabaseHandle};

const MAX_DESCRIPTION_BYTES: usize = 1024;

pub(super) const PROJECT_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::NOT_FOUND,
    "project_not_found",
    "no such Project",
);
const PROJECT_ROOT_INVALID: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "project_root_invalid",
    "the Project root cannot be used",
);
const PROJECT_ROOT_OVERLAPS: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "project_root_overlaps",
    "the Project root equals, contains or lies inside the root of another Project",
);
const INVALID_COMMANDS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_commands",
    "the commands are not valid: at most 32, each with a unique name matching [a-z][a-z0-9_]{0,63} other than cargo_check and cargo_test, 1 to 64 argv entries of 1 to 4096 bytes without NUL whose first is a bare program name or an absolute path, within 16 KiB",
);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandInput {
    name: String,
    argv: Vec<String>,
    #[serde(default)]
    owner: crate::db::entities::CommandOwner,
    #[serde(default)]
    status: crate::db::entities::CommandStatus,
}

impl From<CommandInput> for ProjectCommand {
    fn from(wire: CommandInput) -> Self {
        Self {
            name: wire.name,
            argv: wire.argv,
            owner: wire.owner,
            status: wire.status,
            proposal: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCreateInput {
    name: String,
    root: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    commands: Vec<CommandInput>,
}

/// The fields an update replaces, all required. The root is not among them,
/// so a body that names it is refused.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectUpdateInput {
    name: String,
    #[serde(deserialize_with = "present")]
    description: Option<String>,
    commands: Vec<CommandInput>,
}

/// A field that must be in the body but may be `null`: `deserialize_with`
/// turns off serde's default for a missing `Option`.
fn present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Option::deserialize(deserializer)
}

pub(super) fn project_id(text: &str) -> Result<ProjectId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(ProjectId::new)
        .ok_or(INVALID_REQUEST)
}

pub(super) fn projects_query(query: &str) -> Result<(Option<ProjectId>, usize), Refusal> {
    catalog::page_query(query, project_id)
}

fn project_json(record: &ProjectRecord) -> serde_json::Value {
    serde_json::json!({
        "id": record.id.to_string(),
        "name": record.name.clone(),
        "description": record.description.clone(),
        "root": record.root.clone(),
        "commands": record
                    .commands
                    .iter()
                    .map(|command| serde_json::json!({
                        "name": command.name.clone(),
                        "argv": command.argv.clone(),
                        "owner": match command.owner {
                            crate::db::entities::CommandOwner::User => "user",
                            crate::db::entities::CommandOwner::Decision => "decision",
                        }
                        .to_owned(),
                        "status": match command.status {
                            crate::db::entities::CommandStatus::Allowed => "allowed",
                            crate::db::entities::CommandStatus::Review => "review",
                            crate::db::entities::CommandStatus::Blocked => "blocked",
                        }
                        .to_owned(),
                        "proposal_job": command.proposal.map(|job| job.to_string()),
                    }))
                    .collect::<Vec<_>>(),
        "decision": record.decision.as_ref().map(|policy| {
                    serde_json::json!({
                        "enabled": policy.enabled,
                        "decision_profile": policy.decision_profile.map(|id| id.to_string()),
                        "minimum_confidence": f64::from(policy.minimum_confidence_millis) / 1000.0,
                    })
                }),
        "created_at_us": record.created_at.as_unix_micros(),
        "updated_at_us": record.updated_at.as_unix_micros(),
    })
}

pub(super) async fn list(
    database: &DatabaseHandle,
    before: Option<ProjectId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let mut records = database.projects(before, limit + 1).await?;
    let has_more = records.len() > limit;
    records.truncate(limit);
    Ok(Reply::ok(envelope(serde_json::json!({
        "projects": records.iter().map(project_json).collect::<Vec<_>>(),
        "has_more": has_more,
        "next_before": has_more
        .then(|| records.last().map(|record| record.id.to_string()))
        .flatten(),
    }))))
}

fn description(description: Option<String>) -> Result<Option<String>, Refusal> {
    match description {
        Some(text) if !submissions::text(&text, MAX_DESCRIPTION_BYTES) => Err(INVALID_REQUEST),
        other => Ok(other),
    }
}

/// The root the owner named, resolved and checked off the writer: the
/// canonical UTF-8 path every Workspace of the Project will be authorized
/// from.
#[cfg(unix)]
async fn resolve_root(shared: &Shared, root: String) -> Result<String, Refusal> {
    use crate::system::executor::blocking::{self, Kind};
    use crate::system::executor::workspace_read::{Workspace, WorkspaceError};

    const NOT_ABSOLUTE: Refusal = root_refusal("the Project root must be an absolute path");
    const UNRESOLVED: Refusal = root_refusal("the Project root cannot be resolved");
    const NOT_DIRECTORY: Refusal = root_refusal("the Project root is not a directory");
    const TOO_BROAD: Refusal = root_refusal(
        "the Project root may not be the filesystem root or contain the home directory",
    );
    const HOME_UNKNOWN: Refusal = root_refusal(
        "the home directory of the effective user cannot be determined, so no Project root can be authorized",
    );
    const OVERLAPS_DATA: Refusal = root_refusal("the Project root overlaps the OCG data directory");
    const NOT_UTF8: Refusal = root_refusal("the Project root is not valid UTF-8");

    let data_directory = shared.workspace_tools.data_directory().clone();
    let authorized = blocking::run(Kind::File, move || {
        Workspace::authorize(std::path::Path::new(&root), &data_directory)
            .map(|workspace| workspace.root().to_path_buf())
    })
    .await
    .map_err(|_| INTERNAL)?;
    match authorized {
        Ok(root) => root.into_os_string().into_string().map_err(|_| NOT_UTF8),
        Err(WorkspaceError::NotAbsolute) => Err(NOT_ABSOLUTE),
        Err(WorkspaceError::Unresolved(_)) => Err(UNRESOLVED),
        Err(WorkspaceError::NotDirectory) => Err(NOT_DIRECTORY),
        Err(WorkspaceError::TooBroad) => Err(TOO_BROAD),
        Err(WorkspaceError::HomeUnknown(_)) => Err(HOME_UNKNOWN),
        Err(WorkspaceError::OverlapsDataDirectory) => Err(OVERLAPS_DATA),
    }
}

#[cfg(not(unix))]
async fn resolve_root(_shared: &Shared, _root: String) -> Result<String, Refusal> {
    Err(root_refusal("this host cannot authorize a Project root"))
}

const fn root_refusal(message: &'static str) -> Refusal {
    PROJECT_ROOT_INVALID.with_message(message)
}

fn refusal(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::ProjectRootOverlap { .. }) => PROJECT_ROOT_OVERLAPS,
        DatabaseError::Rejected(Violation::InvalidValue("commands", _)) => INVALID_COMMANDS,
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Project(_))) => PROJECT_NOT_FOUND,
        other => profiles::map_mutation_error(other),
    }
}

pub(super) async fn create(shared: &Shared, body: &[u8]) -> Result<Reply, Refusal> {
    let wire: ProjectCreateInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    if !submissions::text(&wire.name, submissions::MAX_NAME_BYTES) {
        return Err(INVALID_REQUEST);
    }
    let description = description(wire.description)?;
    let root = resolve_root(shared, wire.root).await?;
    let id = shared
        .database
        .create_project(Project {
            name: wire.name,
            description,
            root,
            commands: wire.commands.into_iter().map(Into::into).collect(),
        })
        .await
        .map_err(refusal)?;
    let record = shared.database.project(id).await?.ok_or(INTERNAL)?;
    Ok(Reply::new(
        StatusCode::CREATED,
        envelope(serde_json::json!({
            "project": project_json(&record),
        })),
    ))
}

pub(super) async fn update(
    database: &DatabaseHandle,
    id: ProjectId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let wire: ProjectUpdateInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    if !submissions::text(&wire.name, submissions::MAX_NAME_BYTES) {
        return Err(INVALID_REQUEST);
    }
    let description = description(wire.description)?;
    database
        .update_project(
            id,
            wire.name,
            description,
            wire.commands.into_iter().map(Into::into).collect(),
        )
        .await
        .map_err(refusal)?;
    let record = database.project(id).await?.ok_or(PROJECT_NOT_FOUND)?;
    Ok(Reply::ok(envelope(serde_json::json!({
        "project": project_json(&record),
    }))))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectDecisionInput {
    enabled: bool,
    decision_profile: Option<String>,
    minimum_confidence: f64,
}

pub(super) async fn configure_decision(
    shared: &Shared,
    id: ProjectId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ProjectDecisionInput =
        serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    if !request.minimum_confidence.is_finite()
        || !(0.0..=1.0).contains(&request.minimum_confidence)
        || (request.minimum_confidence * 1000.0 - (request.minimum_confidence * 1000.0).round())
            .abs()
            > 1e-9
    {
        return Err(INVALID_REQUEST);
    }
    let record = shared
        .database
        .project(id)
        .await?
        .ok_or(PROJECT_NOT_FOUND)?;
    let profile = request
        .decision_profile
        .as_deref()
        .map(|profile| {
            crate::db::entities::parse_entity_id(profile)
                .map(crate::db::entities::ConfigId::new)
                .ok_or(INVALID_REQUEST)
        })
        .transpose()?;
    let assignment = Some(crate::db::entities::ProjectDecisionConfig {
        enabled: request.enabled,
        decision_profile: profile,
        minimum_confidence_millis: (request.minimum_confidence * 1000.0).round() as u16,
    });
    let record = shared
        .database
        .configure_project_decision(id, record.updated_at, assignment)
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(serde_json::json!({
        "project": project_json(&record),
    }))))
}
