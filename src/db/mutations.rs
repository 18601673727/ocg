use crate::db::entities::*;
use crate::db::redb::{Batch, BatchError};
use crate::service::ai::providers::openai_compatible::{
    MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_DEFINITION_BYTES, is_function_name,
};

pub(crate) type OpResult<T> = Result<T, BatchError>;

pub(crate) enum ProfileCredentialRef {
    Preserve,
    Clear,
    Explicit(String),
    PreserveOrGenerate(String),
}

pub(crate) struct ProfileSaveGuard {
    pub(crate) profile_edit_seq: u32,
    pub(crate) connection_id: ConfigId,
    pub(crate) connection_edit_seq: u32,
}

pub(crate) enum ModelProfileDefinition {
    Llm {
        protocol: Option<LlmTransport>,
        model: String,
        context_window: Option<u32>,
        parameters: LlmParameters,
        declared_capabilities: std::collections::BTreeSet<ModelCapability>,
    },
    Decision {
        protocol: DecisionProtocol,
        model: String,
        limits: DecisionLimits,
        declared_capabilities: std::collections::BTreeSet<DecisionCapability>,
    },
}

#[derive(Clone)]
pub(crate) struct CredentialPayload {
    pub(crate) secret: crate::db::redb::CredentialSecret,
    pub(crate) key: std::sync::Arc<crate::db::redb::CredentialKey>,
}

pub(crate) enum HeaderEditValue {
    Plain(String),
    Secret(Option<CredentialPayload>),
}

pub(crate) struct HeaderEdit {
    pub(crate) name: String,
    pub(crate) value: HeaderEditValue,
}

pub(crate) enum HeaderChange {
    Preserve,
    Replace(Vec<HeaderEdit>),
}

pub(crate) struct ModelProfileSave {
    pub(crate) headers: HeaderChange,
    pub(crate) existing_id: Option<ProfileId>,
    pub(crate) name: String,
    pub(crate) connection_name: String,
    pub(crate) endpoint: Option<String>,
    pub(crate) credential_ref: ProfileCredentialRef,
    pub(crate) secret: Option<crate::db::redb::CredentialSecret>,
    pub(crate) credential: Option<CredentialPayload>,
    pub(crate) guard: Option<ProfileSaveGuard>,
    pub(crate) definition: ModelProfileDefinition,
}

pub(crate) struct ImportModelsSave {
    pub(crate) headers: HeaderChange,
    pub(crate) connection: Config,
    pub(crate) credential_ref: ProfileCredentialRef,
    pub(crate) secret: Option<crate::db::redb::CredentialSecret>,
    pub(crate) credential: Option<CredentialPayload>,
    pub(crate) models: Vec<crate::db::entities::DiscoveredModel>,
    pub(crate) capabilities: std::collections::BTreeSet<ModelCapability>,
}

pub(crate) fn save_model_profile(
    batch: &mut Batch,
    save: ModelProfileSave,
) -> OpResult<(ConfigId, Option<String>)> {
    batch.save_model_profile(save)
}

pub(crate) fn record_tool_decision(
    batch: &mut Batch,
    proof: &crate::db::handle::DevelopmentWriteProof,
    evidence: ToolEvidence,
) -> OpResult<()> {
    let call = evidence.call;
    if evidence.tool_call_id.is_empty() || evidence.tool_call_id.len() > MAX_TOOL_CALL_ID_BYTES {
        return batch.reject(Violation::InvalidValue(
            "tool_call_id",
            "must be 1 to 256 bytes",
        ));
    }
    if !is_function_name(&evidence.name) {
        return batch.reject(Violation::InvalidValue("name", "is not a function name"));
    }
    if let Some(admission) = evidence.decision.stored_admission() {
        if !is_handler_version(&admission.handler_version) {
            return batch.reject(Violation::InvalidValue(
                "handler_version",
                "is not a handler version",
            ));
        }
        if serde_json::from_str::<serde_json::Value>(&admission.parameters).is_err() {
            return batch.reject(Violation::InvalidValue("parameters", "is not JSON"));
        }
        if !is_json_object(admission.arguments.as_str()) {
            return batch.reject(Violation::InvalidValue("arguments", "is not a JSON object"));
        }
        if admission.parameters.len() > MAX_TOOL_DEFINITION_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the schema of one call",
            });
        }
        if admission.arguments.len() > MAX_STORED_TOOL_ARGUMENTS_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the arguments of one call",
            });
        }
    }
    batch.tool_decide(proof, evidence)
}

pub(crate) fn record_tool_outcome(
    batch: &mut Batch,
    proof: &crate::db::handle::DevelopmentWriteProof,
    call: ToolCallRef,
    outcome: ToolOutcome,
) -> OpResult<()> {
    batch.tool_outcome(proof, call, outcome)
}

fn is_json_object(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok_and(|value| value.is_object())
}

pub(crate) fn create_config(batch: &mut Batch, config: Config) -> OpResult<ConfigId> {
    batch.create_config(config.name, config.body)
}

pub(crate) fn import_models(
    batch: &mut Batch,
    save: ImportModelsSave,
) -> OpResult<(Vec<ConfigId>, usize)> {
    batch.import_models(save)
}

pub(crate) fn archive_model(batch: &mut Batch, id: ConfigId) -> OpResult<()> {
    batch.archive_model(id)
}

pub(crate) fn update_config(
    batch: &mut Batch,
    id: ConfigId,
    config: Config,
    expected_edit_seq: u32,
) -> OpResult<()> {
    batch.update_config_from(id, config.name, config.body, Some(expected_edit_seq))
}

fn is_mime_type(mime: &str) -> bool {
    let name = |part: &str| {
        (1..=127).contains(&part.len())
            && part.starts_with(|first: char| first.is_ascii_lowercase() || first.is_ascii_digit())
            && part.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$&-^_.+".contains(&byte)
            })
    };
    mime.len() <= MAX_ATTACHMENT_MIME_BYTES
        && mime
            .split_once('/')
            .is_some_and(|(kind, subtype)| name(kind) && name(subtype))
}

pub(crate) fn check_new_attachment(attachment: &Attachment) -> Result<(), Violation> {
    if attachment.content.is_empty() {
        return Err(Violation::InvalidValue("content", "cannot be empty"));
    }
    if attachment.content.len() > MAX_ATTACHMENT_BYTES {
        return Err(Violation::ValueOutOfRange("content"));
    }
    if !is_mime_type(&attachment.mime) {
        return Err(Violation::InvalidValue(
            "mime",
            "must be a lowercase type/subtype without parameters",
        ));
    }
    Ok(())
}

pub(crate) struct PublishedAttachment {
    pub(crate) sha256: ContentDigest,
    pub(crate) byte_length: u64,
    pub(crate) mime: String,
    pub(crate) display_name: Option<String>,
    pub(crate) origin: AttachmentOrigin,
}

pub(crate) fn create_attachment(
    batch: &mut Batch,
    attachment: PublishedAttachment,
) -> OpResult<AttachmentId> {
    if !is_mime_type(&attachment.mime) {
        return batch.reject(Violation::InvalidValue(
            "mime",
            "must be a lowercase type/subtype without parameters",
        ));
    }
    if attachment.byte_length == 0 {
        return batch.reject(Violation::InvalidValue("content", "cannot be empty"));
    }
    let id = AttachmentId::new(batch.next_id()?);
    batch.insert_attachment(&AttachmentRecord {
        id,
        sha256: attachment.sha256,
        byte_length: attachment.byte_length,
        mime: attachment.mime,
        display_name: attachment
            .display_name
            .as_deref()
            .and_then(sanitize_display_name),
        origin: attachment.origin,
        created_at: batch.now(),
        cleanup: None,
    })?;
    Ok(id)
}

pub(crate) fn create_project(batch: &mut Batch, mut project: Project) -> OpResult<ProjectId> {
    if let Err(reason) = validate_project_commands(&project.commands) {
        return batch.reject(Violation::InvalidValue("commands", reason));
    }
    for command in &mut project.commands {
        command.owner = CommandOwner::User;
        command.proposal = None;
    }
    let root = std::path::Path::new(&project.root);
    for (existing, other) in batch.project_roots()? {
        let other = std::path::Path::new(&other);
        if other.starts_with(root) || root.starts_with(other) {
            return batch.reject(Violation::ProjectRootOverlap { project: existing });
        }
    }
    let id = ProjectId::new(batch.next_id()?);
    let now = batch.now();
    batch.insert_project(&ProjectRecord {
        id,
        name: project.name,
        description: project.description,
        root: project.root,
        decision: None,
        decision_revision: 0,
        commands: project.commands,
        created_at: now,
        updated_at: now,
    })?;
    Ok(id)
}

pub(crate) fn update_project(
    batch: &mut Batch,
    id: ProjectId,
    name: String,
    description: Option<String>,
    commands: Vec<ProjectCommand>,
) -> OpResult<()> {
    if let Err(reason) = validate_project_commands(&commands) {
        return batch.reject(Violation::InvalidValue("commands", reason));
    }
    let mut record = batch.require_project_record(id)?;
    record.name = name;
    record.description = description;
    let mut commands = commands;
    for command in &mut commands {
        let prior = record
            .commands
            .iter()
            .find(|prior| prior.name == command.name);
        let unchanged = prior.is_some_and(|prior| {
            prior.argv == command.argv
                && prior.status == command.status
                && prior.owner == command.owner
        });
        command.proposal = prior.and_then(|prior| prior.proposal);
        if !unchanged {
            command.owner = CommandOwner::User;
        }
    }
    if record
        .commands
        .iter()
        .any(|prior| !commands.iter().any(|command| command.name == prior.name))
    {
        record.decision_revision = match record.decision_revision.checked_add(1) {
            Some(revision) => revision,
            None => {
                return batch.reject(Violation::InvalidValue(
                    "commands",
                    "command policy revision exhausted",
                ));
            }
        };
    }
    if let Err(reason) = validate_project_commands(&commands) {
        return batch.reject(Violation::InvalidValue("commands", reason));
    }
    record.commands = commands;
    record.updated_at = batch.now();
    batch.insert_project(&record)?;
    batch.resume_user_command_grants(&record)
}

pub(crate) fn configure_project_decision(
    batch: &mut Batch,
    id: ProjectId,
    expected: Timestamp,
    assignment: Option<ProjectDecisionConfig>,
) -> OpResult<ProjectRecord> {
    let mut project = batch.require_project_record(id)?;
    if project.updated_at != expected {
        return batch.reject(Violation::InvalidValue(
            "decision",
            "project changed; refresh and retry",
        ));
    }
    if let Some(policy) = &assignment {
        if policy.minimum_confidence_millis > 1000 {
            return batch.reject(Violation::InvalidValue(
                "decision",
                "confidence must be between zero and one",
            ));
        }
        if let Some(profile) = policy.decision_profile {
            batch.freeze_decision_profile(profile)?;
        }
    }
    project.decision_revision = match project.decision_revision.checked_add(1) {
        Some(revision) => revision,
        None => {
            return batch.reject(Violation::InvalidValue(
                "decision",
                "project decision revision exhausted",
            ));
        }
    };
    project.decision = assignment;
    project.updated_at = batch.now();
    batch.insert_project(&project)?;
    Ok(project)
}

fn require_conversation(batch: &mut Batch, id: JobId) -> OpResult<()> {
    let row = match batch.job(id)? {
        Some(row) => row,
        None => return batch.reject(Violation::NotFound(RecordRef::Job(id))),
    };
    if row.kind.is_group() {
        Ok(())
    } else {
        batch.reject(Violation::NotFound(RecordRef::Job(id)))
    }
}

pub(crate) fn set_conversation(batch: &mut Batch, root: JobId, edit: JobEdit) -> OpResult<()> {
    require_conversation(batch, root)?;
    let mut row = batch.job_control(root)?;
    match edit {
        JobEdit::Limits(limits) => {
            return batch.set_job_limits(root, limits, Actor::User);
        }
        JobEdit::CompactionPolicy(policy) => {
            if let Some(policy) = &policy {
                batch.check_compaction_policy(policy)?;
            }
            if row.compaction_policy == policy {
                return Ok(());
            }
            row.compaction_policy = policy;
            row.compaction_revision = match row.compaction_revision.checked_add(1) {
                Some(revision) => revision,
                None => {
                    return batch.reject(Violation::InvalidValue(
                        "compaction_policy",
                        "policy revision exhausted",
                    ));
                }
            };
        }
        JobEdit::Title(title) => row.title = Some(title),
        JobEdit::SuggestedTitle(_) if row.title.is_some() => return Ok(()),
        JobEdit::SuggestedTitle(title) => row.title = Some(title),
        JobEdit::Hide if row.hidden_at.is_some() => return Ok(()),
        JobEdit::Hide => row.hidden_at = Some(batch.now()),
    }
    batch.put_job_control(root, &row)
}
