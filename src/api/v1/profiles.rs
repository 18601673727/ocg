use std::collections::{BTreeMap, BTreeSet};

use super::contracts::*;
use ntex::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;

use super::wire::{
    DATABASE_BUSY, DATABASE_UNAVAILABLE, INCONSISTENT_RECORD, INTERNAL, INVALID_REQUEST, Refusal,
    Reply, SHUTTING_DOWN,
};
use crate::db::entities::{
    AgentCapability, AgentConfig, Config, ConfigBody, ConfigId, ConfigKind, ConfigRecord,
    ConnectionConfig, DecisionAgentConfig, DecisionCapability, DecisionLimits, DecisionProtocol,
    ExecutionOperation, LlmParameters, LlmTransport, ModelCapability, ProfileId, RecordRef,
    Violation,
};
use crate::db::entities::{
    FrozenHeaders, HeaderError, HeaderScope, OutboundHeader, OutboundHeaderValue, canonical_name,
    merge, validate_fields, validate_value,
};
use crate::db::mutations::{
    HeaderChange, HeaderEdit, HeaderEditValue, ImportModelsSave, ModelProfileDefinition,
    ModelProfileSave, ProfileCredentialRef,
};
use crate::db::redb::CredentialSecret;
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::providers::CredentialResolver;
use crate::system::http::{RequestHeaders, resolve_request_headers};
use std::sync::Arc;

const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_MODEL_BYTES: usize = 256;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_CREDENTIAL_REF_BYTES: usize = 128;
const MAX_PROVIDER_MESSAGE_BYTES: usize = 1024;
const MAX_INSTRUCTIONS_BYTES: usize = 65_536;

const INVALID_PROFILE_BODY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the request body is not a valid Profile configuration payload",
);
const PROFILE_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::NOT_FOUND,
    "profile_not_found",
    "the named Profile does not exist",
);
const INVALID_PROFILE_NAME: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_profile_name",
    "Profile name must be 1..=128 non-control characters",
);
const INVALID_MODEL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_model",
    "model identifier must be 1..=256 non-control characters",
);
const UNSUPPORTED_PROTOCOL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "unsupported_protocol",
    "protocol must be openai_chat_completions, openai_responses or anthropic_messages",
);
const INVALID_ENDPOINT: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_endpoint",
    "endpoint must use https:// (or http:// on localhost, 127.0.0.1 or ::1), at most 512 characters without whitespace",
);
const INVALID_CREDENTIAL_REF: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_credential_ref",
    "credential_ref must be 1..=128 ASCII letters, digits, '_', '-' or '.'",
);
const INVALID_CREDENTIAL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_credential",
    "credential secret must be 1..=4096 bytes and a valid HTTP Bearer token",
);
const INVALID_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_capabilities",
    "declared_capabilities must include at least one of chat, tools, vision, reasoning",
);
const INVALID_PARAMETERS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_parameters",
    "LLM parameters must be finite numbers within valid bounds (temperature 0..=2, top_p 0..=1, one token limit >= 1, reasoning_effort supported by the wire, penalties -2..=2)",
);
const INVALID_INSTRUCTIONS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_instructions",
    "Agent instructions must be at most 65536 bytes without control characters",
);
const MISSING_LLM_TARGETS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "missing_llm_targets",
    "an Agent Profile must select at least one target LLM Profile",
);
const INVALID_LLM_TARGET: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_llm_target",
    "every llm_target must be an existing LLM Profile without duplicates",
);
const INVALID_AGENT_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_agent_capabilities",
    "select chat, coding or decision capabilities",
);
const UNSUPPORTED_DECISION_PROTOCOL: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "unsupported_decision_protocol",
    "decision protocol must be typesafe_systemone",
);
const INVALID_DECISION_CAPABILITIES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_capabilities",
    "declared_capabilities must include at least one of choice, ordinal_score, binary_probability",
);
const INVALID_DECISION_LIMITS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_limits",
    "decision limits must be between 1 and 1000000, and a per-question limit cannot exceed its per-request limit",
);
const INVALID_DECISION_MODELS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_models",
    "every decision target must be an existing model from Models without duplicates",
);
const MISSING_DECISION_MODELS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "missing_decision_models",
    "a decision configuration must select at least one model from Models",
);

const WRONG_PROFILE_KIND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "wrong_profile_kind",
    "a Profile's kind cannot be changed after creation",
);
const PROVIDER_DISCOVERY_DISABLED: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "provider_discovery_disabled",
    "model discovery against this endpoint is disabled in this build; only a literal loopback endpoint is permitted",
);
const PROFILE_CHANGED: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "profile_changed",
    "the Profile changed while it was being validated; reload and retry",
);
const PROFILE_REVISION_REQUIRED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "profile_revision_required",
    "an update must name the revision it was made from (expected_edit_seq)",
);
const PROFILE_REVISION_CHANGED: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "profile_revision_changed",
    "the Profile was changed elsewhere; reload it and apply the edit again",
);
const STORAGE_REFUSED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "storage_refused",
    "storage refused to commit the Profile change",
);

pub(crate) fn profile_id(text: &str) -> Result<ProfileId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(ProfileId::new)
        .ok_or(INVALID_REQUEST)
}

fn capability_str(capability: ModelCapability) -> &'static str {
    match capability {
        ModelCapability::Chat => "chat",
        ModelCapability::Tools => "tools",
        ModelCapability::Vision => "vision",
        ModelCapability::Reasoning => "reasoning",
    }
}

fn parse_capability(value: &str) -> Option<ModelCapability> {
    match value {
        "chat" => Some(ModelCapability::Chat),
        "tools" => Some(ModelCapability::Tools),
        "vision" => Some(ModelCapability::Vision),
        "reasoning" => Some(ModelCapability::Reasoning),
        _ => None,
    }
}

fn decision_capability_str(capability: DecisionCapability) -> &'static str {
    match capability {
        DecisionCapability::Choice => "choice",
        DecisionCapability::OrdinalScore => "ordinal_score",
        DecisionCapability::BinaryProbability => "binary_probability",
    }
}

fn parse_decision_capability(value: &str) -> Option<DecisionCapability> {
    match value {
        "choice" => Some(DecisionCapability::Choice),
        "ordinal_score" => Some(DecisionCapability::OrdinalScore),
        "binary_probability" => Some(DecisionCapability::BinaryProbability),
        _ => None,
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParametersInput {
    #[serde(default)]
    temperature: Option<f64>,
    #[serde(default)]
    top_p: Option<f64>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    reasoning_effort: Option<String>,
    #[serde(default)]
    max_completion_tokens: Option<u32>,
    #[serde(default)]
    presence_penalty: Option<f64>,
    #[serde(default)]
    frequency_penalty: Option<f64>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ProfileRequest {
    Llm {
        name: String,
        model: String,
        #[serde(default)]
        context_window: Option<u32>,
        #[serde(default)]
        protocol: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        credential_ref: Option<String>,
        #[serde(default)]
        credential: Option<String>,
        #[serde(default)]
        clear_credential: bool,
        #[serde(default)]
        declared_capabilities: Vec<String>,
        #[serde(default)]
        parameters: ParametersInput,
        #[serde(default)]
        headers: Option<Vec<OutboundHeaderInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    Agent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default)]
        llm_targets: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        compaction_policy: Option<Option<CompactionPolicyInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    DecisionModel {
        name: String,
        model: String,
        #[serde(default)]
        protocol: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        credential_ref: Option<String>,
        #[serde(default)]
        credential: Option<String>,
        #[serde(default)]
        clear_credential: bool,
        #[serde(default)]
        declared_capabilities: Vec<String>,
        #[serde(default)]
        limits: DecisionLimitInput,
        #[serde(default)]
        headers: Option<Vec<OutboundHeaderInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    DecisionAgent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        models: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        default_model: Option<Option<String>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DecisionLimitInput {
    #[serde(default)]
    max_input_tokens_per_request: Option<u32>,
    #[serde(default)]
    max_output_tokens_per_request: Option<u32>,
    #[serde(default)]
    max_input_tokens_per_question: Option<u32>,
    #[serde(default)]
    max_output_tokens_per_question: Option<u32>,
}

pub(crate) fn optional_value<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn valid_single_line(text: &str, max_bytes: usize) -> bool {
    let trimmed = text.trim();
    !trimmed.is_empty() && trimmed.len() <= max_bytes && !trimmed.chars().any(char::is_control)
}

fn valid_multiline(text: &str, max_bytes: usize) -> bool {
    text.len() <= max_bytes
        && !text
            .chars()
            .any(|ch| ch.is_control() && !matches!(ch, '\t' | '\n' | '\r'))
}

fn normalize_endpoint(endpoint: Option<&str>) -> Result<Option<String>, Refusal> {
    match endpoint.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(url) => {
            if url.len() > MAX_ENDPOINT_BYTES
                || crate::service::ai::providers::model_endpoint(url).is_err()
                || url.chars().any(|ch| ch.is_whitespace() || ch.is_control())
            {
                return Err(INVALID_ENDPOINT);
            }
            Ok(Some(url.to_owned()))
        }
    }
}

fn normalize_instructions(instructions: Option<&str>) -> Result<Option<String>, Refusal> {
    match instructions
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        None => Ok(None),
        Some(text) if valid_multiline(text, MAX_INSTRUCTIONS_BYTES) => Ok(Some(text.to_owned())),
        Some(_) => Err(INVALID_INSTRUCTIONS),
    }
}

fn valid_credential_ref(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= MAX_CREDENTIAL_REF_BYTES
        && reference
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn bounded_provider_message(mut message: String) -> String {
    if message.len() > MAX_PROVIDER_MESSAGE_BYTES {
        let mut end = MAX_PROVIDER_MESSAGE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

async fn saved_connection_credential(
    database: &DatabaseHandle,
    reference: &str,
) -> Result<bool, Refusal> {
    Ok(database.credential_ref_in_use(reference).await?)
}

fn default_credential_ref(name: &str) -> String {
    let mut slug = String::from("OCG_PROFILE_");
    for ch in name.chars() {
        if slug.len() >= 48 {
            break;
        }
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_uppercase());
        } else if !slug.ends_with('_') {
            slug.push('_');
        }
    }
    while slug.ends_with('_') {
        slug.pop();
    }
    if slug == "OCG_PROFILE" {
        slug.push_str("_DEFAULT");
    }
    slug.push_str("_API_KEY");
    slug
}

pub(crate) fn map_mutation_error(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::InvalidValue(
            "executor",
            "no enabled native Executor supports this operation and protocol",
        )) => Refusal::new(
            StatusCode::CONFLICT,
            "executor_unavailable",
            "no enabled Executor supports this operation and protocol",
        ),
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Profile(_))) => PROFILE_NOT_FOUND,
        DatabaseError::Rejected(Violation::WrongConfigKind { .. }) => WRONG_PROFILE_KIND,
        DatabaseError::Rejected(Violation::ProfileChanged) => PROFILE_CHANGED,
        DatabaseError::Rejected(Violation::DuplicateReference(_)) => INVALID_LLM_TARGET,
        DatabaseError::QueueFull | DatabaseError::ReadQueueFull => DATABASE_BUSY,
        DatabaseError::ShuttingDown | DatabaseError::ReadsClosed => SHUTTING_DOWN,
        DatabaseError::DiskProtected(_)
        | DatabaseError::RefusedByDiskProtection { .. }
        | DatabaseError::InsufficientSpace { .. }
        | DatabaseError::RolledBack(_)
        | DatabaseError::NeverExecuted { .. } => STORAGE_REFUSED,
        DatabaseError::ReadsStorageFailed(_)
        | DatabaseError::ReadInterrupted
        | DatabaseError::ReadPoolStopped
        | DatabaseError::Read(_) => DATABASE_UNAVAILABLE,
        _ => INTERNAL,
    }
}

async fn previous_model_connection(
    database: &DatabaseHandle,
    existing_id: Option<ProfileId>,
    kind: ConfigKind,
) -> Result<
    Option<(
        ConfigBody,
        ConnectionConfig,
        crate::db::mutations::ProfileSaveGuard,
    )>,
    Refusal,
> {
    let Some(id) = existing_id else {
        return Ok(None);
    };
    let record = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
    if record.archived_at.is_some() {
        return Err(PROFILE_NOT_FOUND);
    }
    if record.body.kind() != kind {
        return Err(WRONG_PROFILE_KIND);
    }
    let connection_id = match &record.body {
        ConfigBody::Model(model) => model.connection_id,
        ConfigBody::DecisionModel(model) => model.connection_id,
        ConfigBody::Connection(_)
        | ConfigBody::Agent(_)
        | ConfigBody::DecisionAgent(_)
        | ConfigBody::Pool(_) => return Err(WRONG_PROFILE_KIND),
    };
    let connection_record = database
        .config(connection_id)
        .await?
        .ok_or(INCONSISTENT_RECORD)?;
    let ConfigBody::Connection(connection) = connection_record.body else {
        return Err(INCONSISTENT_RECORD);
    };
    Ok(Some((
        record.body,
        connection,
        crate::db::mutations::ProfileSaveGuard {
            profile_edit_seq: record.edit_seq,
            connection_id,
            connection_edit_seq: connection_record.edit_seq,
        },
    )))
}

fn config_revision(current: u32, expected_edit_seq: Option<u32>) -> Result<Option<u32>, Refusal> {
    match expected_edit_seq {
        None => Err(PROFILE_REVISION_REQUIRED),
        Some(expected) if expected != current => Err(PROFILE_REVISION_CHANGED),
        Some(expected) => Ok(Some(expected)),
    }
}

fn revision_guard(
    previous: Option<crate::db::mutations::ProfileSaveGuard>,
    expected_edit_seq: Option<u32>,
) -> Result<Option<crate::db::mutations::ProfileSaveGuard>, Refusal> {
    let Some(guard) = previous else {
        return Ok(None);
    };
    match expected_edit_seq {
        None => Err(PROFILE_REVISION_REQUIRED),
        Some(expected) if expected != guard.profile_edit_seq => Err(PROFILE_REVISION_CHANGED),
        Some(_) => Ok(Some(guard)),
    }
}

fn resolve_credential_input(
    profile_name: &str,
    credential_ref: Option<&str>,
    credential: Option<&str>,
    clear_credential: bool,
) -> Result<(ProfileCredentialRef, Option<CredentialSecret>), Refusal> {
    let explicit_ref = credential_ref
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(reference) = explicit_ref.as_deref()
        && !valid_credential_ref(reference)
    {
        return Err(INVALID_CREDENTIAL_REF);
    }
    let secret = credential.map(str::trim).filter(|value| !value.is_empty());
    if let Some(secret_value) = secret
        && (secret_value.len() > 4096
            || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret_value}")).is_err())
    {
        return Err(INVALID_CREDENTIAL);
    }
    if clear_credential {
        return Ok((ProfileCredentialRef::Clear, None));
    }
    match secret {
        Some(secret_value) => {
            let credential_ref = match explicit_ref {
                Some(reference) => ProfileCredentialRef::Explicit(reference),
                None => {
                    ProfileCredentialRef::PreserveOrGenerate(default_credential_ref(profile_name))
                }
            };
            Ok((
                credential_ref,
                Some(
                    CredentialSecret::new(secret_value.to_owned())
                        .map_err(|_| INVALID_CREDENTIAL)?,
                ),
            ))
        }
        None => Ok((
            explicit_ref.map_or(
                ProfileCredentialRef::Preserve,
                ProfileCredentialRef::Explicit,
            ),
            None,
        )),
    }
}

const INVALID_HEADERS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_headers",
    "invalid custom request headers",
);

fn invalid_headers(error: &HeaderError) -> Refusal {
    INVALID_HEADERS.with_reason(error.to_string())
}

struct ProbeResolver {
    stored: Arc<dyn CredentialResolver>,
    supplied: BTreeMap<String, String>,
}

impl CredentialResolver for ProbeResolver {
    fn resolve<'a>(
        &'a self,
        reference: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<String>,
                        crate::service::ai::providers::ProviderExecutionError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        match reference.and_then(|reference| self.supplied.get(reference)) {
            Some(value) => {
                let value = value.clone();
                Box::pin(async move { Ok(Some(value)) })
            }
            None => self.stored.resolve(reference),
        }
    }
}

struct HeaderPlan {
    change: HeaderChange,
    probe: RequestHeaders,
}

async fn plan_headers(
    database: &DatabaseHandle,
    inputs: Option<Vec<OutboundHeaderInput>>,
    previous: &[OutboundHeader],
) -> Result<HeaderPlan, Refusal> {
    let global = database.global_outbound_headers().await?;
    let mut supplied = BTreeMap::new();
    let (change, connection) = match inputs {
        None => (HeaderChange::Preserve, previous.to_vec()),
        Some(inputs) => {
            let mut edits = Vec::with_capacity(inputs.len());
            let mut fields = Vec::with_capacity(inputs.len());
            for (index, input) in inputs.into_iter().enumerate() {
                let name = canonical_name(input.name.trim()).map_err(|e| invalid_headers(&e))?;
                let value = input.value.filter(|value| !value.is_empty());
                if let Some(value) = &value {
                    validate_value(value).map_err(|e| invalid_headers(&e))?;
                }
                let (edit, field) = match (input.secret, value) {
                    (false, Some(value)) => (
                        HeaderEditValue::Plain(value.clone()),
                        OutboundHeaderValue::Plain(value),
                    ),
                    (false, None) => return Err(invalid_headers(&HeaderError::InvalidValue)),
                    (true, None) => {
                        let kept = previous.iter().find_map(|field| match &field.value {
                            OutboundHeaderValue::Secret(reference) if field.name == name => {
                                Some(reference.clone())
                            }
                            OutboundHeaderValue::Secret(_) | OutboundHeaderValue::Plain(_) => None,
                        });
                        let Some(reference) = kept else {
                            return Err(INVALID_HEADERS.with_reason(format!(
                                "header `{name}` has no stored secret; supply its value"
                            )));
                        };
                        (
                            HeaderEditValue::Secret(None),
                            OutboundHeaderValue::Secret(reference),
                        )
                    }
                    (true, Some(value)) => {
                        let placeholder = format!("probe-{index}");
                        supplied.insert(placeholder.clone(), value.clone());
                        let secret =
                            CredentialSecret::new(value).map_err(|_| INVALID_CREDENTIAL)?;
                        let payload = database.credential_payload(secret).await?;
                        (
                            HeaderEditValue::Secret(Some(payload)),
                            OutboundHeaderValue::Secret(placeholder),
                        )
                    }
                };
                edits.push(HeaderEdit {
                    name: name.clone(),
                    value: edit,
                });
                fields.push(OutboundHeader { name, value: field });
            }
            fields.sort_by(|left, right| left.name.cmp(&right.name));
            validate_fields(&fields, HeaderScope::Connection).map_err(|e| invalid_headers(&e))?;
            (HeaderChange::Replace(edits), fields)
        }
    };
    let probe = resolve_request_headers(
        &FrozenHeaders {
            global_revision: global.revision,
            fields: merge(&global.fields, &connection),
        },
        &ProbeResolver {
            stored: database.credential_resolver(),
            supplied,
        },
    )
    .await
    .map_err(|_| INVALID_HEADERS)?;
    Ok(HeaderPlan { change, probe })
}

fn header_views(
    fields: &[OutboundHeader],
    credentials: &BTreeSet<String>,
) -> Vec<OutboundHeaderView> {
    fields
        .iter()
        .map(|field| match &field.value {
            OutboundHeaderValue::Plain(value) => OutboundHeaderView {
                name: field.name.clone(),
                secret: false,
                value: Some(value.clone()),
                has_secret: false,
            },
            OutboundHeaderValue::Secret(reference) => OutboundHeaderView {
                name: field.name.clone(),
                secret: true,
                value: None,
                has_secret: credentials.contains(reference.trim()),
            },
        })
        .collect()
}

const DEFAULT_PROFILE_PAGE: usize = 50;
const PROFILE_SCAN_FACTOR: usize = 4;
const MAX_PROFILE_PAGE_BYTES: usize = 512 * 1024;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "LlmProfile")]
pub(crate) struct LlmProfileView {
    edit_seq: u32,
    enabled: bool,
    reasoning_efforts: Option<Vec<String>>,
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    profile_type: Option<String>,
    name: String,
    model: String,
    context_window: Option<u32>,
    protocol: String,
    endpoint: Option<String>,
    credential_ref: Option<String>,
    has_credential: bool,
    headers: Vec<OutboundHeaderView>,
    declared_capabilities: Vec<String>,
    parameters: LlmParameters,
    editable: bool,
    blocking_jobs: Vec<String>,
    more_blocking_jobs: bool,
    created_at_us: i64,
    updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ManagedAgent")]
pub(crate) struct ManagedAgentView {
    id: String,
    edit_seq: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    profile_type: Option<String>,
    name: String,
    instructions: Option<String>,
    capabilities: BTreeSet<AgentCapability>,
    llm_targets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    compaction_policy: Option<CompactionPolicyInput>,
    editable: bool,
    blocking_jobs: Vec<String>,
    more_blocking_jobs: bool,
    created_at_us: i64,
    updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionLimits")]
pub(crate) struct DecisionLimitsView {
    max_input_tokens_per_request: Option<u32>,
    max_output_tokens_per_request: Option<u32>,
    max_input_tokens_per_question: Option<u32>,
    max_output_tokens_per_question: Option<u32>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionProfile")]
pub(crate) struct DecisionProfileView {
    id: String,
    edit_seq: u32,
    name: String,
    model: String,
    protocol: String,
    endpoint: Option<String>,
    credential_ref: Option<String>,
    has_credential: bool,
    headers: Vec<OutboundHeaderView>,
    declared_capabilities: Vec<String>,
    limits: DecisionLimitsView,
    editable: bool,
    created_at_us: i64,
    updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionAgentProfile")]
pub(crate) struct DecisionAgentProfileView {
    id: String,
    edit_seq: u32,
    name: String,
    instructions: Option<String>,
    models: Vec<String>,
    default_model: Option<String>,
    editable: bool,
    created_at_us: i64,
    updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProfileExecutor")]
pub(crate) struct ProfileExecutorView {
    id: String,
    protocol: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Provider")]
pub(crate) struct ProviderView {
    id: String,
    name: String,
    protocol: String,
    endpoint: Option<String>,
    credential_ref: Option<String>,
    has_credential: bool,
    headers: Vec<OutboundHeaderView>,
    profile_ids: Vec<String>,
    models: Vec<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Profiles")]
pub(crate) struct ProfilesView {
    llm_profiles: Vec<LlmProfileView>,
    agent_profiles: Vec<ManagedAgentView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    decision_profiles: Option<Vec<DecisionProfileView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    decision_agent_profiles: Option<Vec<DecisionAgentProfileView>>,
    executor_protocols: Vec<ProfileExecutorView>,
    providers: Vec<ProviderView>,
    has_more: bool,
    next_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    profile_id: Option<String>,
}

pub(crate) async fn list_profiles(
    database: &DatabaseHandle,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .profile_page(before, limit, limit.saturating_mul(PROFILE_SCAN_FACTOR))
        .await?;
    Ok(Reply::ok(build_profiles_page(page, None)?))
}

async fn build_profiles_snapshot(
    database: &DatabaseHandle,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let page = database
        .profile_page(
            None,
            DEFAULT_PROFILE_PAGE,
            DEFAULT_PROFILE_PAGE.saturating_mul(PROFILE_SCAN_FACTOR),
        )
        .await?;
    build_profiles_page(page, saved_profile)
}

fn has_profile_credential(credentials: &BTreeSet<String>, reference: &str) -> bool {
    let reference = reference.trim();
    !reference.is_empty()
        && (credentials.contains(reference)
            || std::env::var_os(reference).is_some_and(|value| !value.is_empty()))
}

fn parse_capability_set<T: Ord>(
    items: &[String],
    parse: fn(&str) -> Option<T>,
    err: Refusal,
) -> Result<BTreeSet<T>, Refusal> {
    let mut set = BTreeSet::new();
    for item in items {
        set.insert(parse(item.trim()).ok_or_else(|| err.clone())?);
    }
    if set.is_empty() {
        return Err(err);
    }
    Ok(set)
}

fn valid_secret_header(secret: &str) -> bool {
    !secret.is_empty()
        && secret.len() <= 4096
        && ntex::http::header::HeaderValue::try_from(format!("Bearer {secret}")).is_ok()
}

fn connection_for_model<'a>(
    connections: &'a BTreeMap<ConfigId, (String, ConnectionConfig, bool)>,
    credentials: &BTreeSet<String>,
    connection_id: ConfigId,
) -> Result<(&'a str, &'a ConnectionConfig, bool, bool), Refusal> {
    let Some((provider_name, connection, discovery_visible)) = connections.get(&connection_id)
    else {
        return Err(INCONSISTENT_RECORD);
    };
    let has_credential = connection
        .credential_ref
        .as_deref()
        .is_some_and(|reference| has_profile_credential(credentials, reference));
    Ok((
        provider_name,
        connection,
        *discovery_visible,
        has_credential,
    ))
}

fn push_page_item<T: serde::Serialize>(
    list: &mut Vec<T>,
    item: T,
    id: ConfigId,
    page_bytes: &mut usize,
    last_emitted: &mut Option<ConfigId>,
    next_before: &mut Option<ConfigId>,
) -> Result<bool, Refusal> {
    if !fits_profile_page(&item, page_bytes)? {
        *next_before = *last_emitted;
        return Ok(false);
    }
    list.push(item);
    *last_emitted = Some(id);
    Ok(true)
}

fn build_profiles_page(
    page: crate::db::redb::ProfilePageRecords,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let crate::db::redb::ProfilePageRecords {
        records,
        connections,
        credentials,
        next_before,
    } = page;
    let mut llm_profiles = Vec::new();
    let mut agent_profiles = Vec::new();
    let mut decision_profiles = Vec::new();
    let mut decision_agent_profiles = Vec::new();
    let mut executor_protocols = Vec::new();

    struct ProviderGroup {
        name: String,
        protocol: &'static str,
        endpoint: Option<String>,
        credential_ref: Option<String>,
        has_credential: bool,
        headers: Vec<OutboundHeaderView>,
        profile_ids: Vec<String>,
        models: BTreeSet<String>,
    }
    let mut provider_groups: BTreeMap<ConfigId, ProviderGroup> = BTreeMap::new();
    let mut last_emitted = None;
    let mut page_bytes = 0;
    let mut next_before = next_before;

    for record in records {
        let record = match record {
            crate::db::redb::ProfilePageRecord::Config(record) => record,
            crate::db::redb::ProfilePageRecord::Executor(executor) => {
                let offered: Vec<_> = executor
                    .capabilities
                    .iter()
                    .filter_map(|(operation, protocol)| match (operation, protocol) {
                        (ExecutionOperation::LlmRequest, Some(protocol)) => {
                            Some(ProfileExecutorView {
                                id: executor.id.to_string(),
                                protocol: protocol.as_str().to_owned(),
                            })
                        }
                        _ => None,
                    })
                    .collect();
                if !offered.is_empty() {
                    if !fits_profile_page(&offered, &mut page_bytes)? {
                        next_before = last_emitted;
                        break;
                    }
                    executor_protocols.extend(offered);
                    last_emitted = Some(ConfigId::new(executor.id.get()));
                }
                continue;
            }
        };
        let id = ProfileId::from(record.id);
        match record.body {
            ConfigBody::Model(model) => {
                let (provider_name, connection, discovery_visible, has_credential) =
                    connection_for_model(&connections, &credentials, model.connection_id)?;
                let protocol = connection.protocol.as_str();
                let id_str = id.to_string();
                let profile = LlmProfileView {
                    edit_seq: record.edit_seq,
                    enabled: record.model_enabled,
                    reasoning_efforts: model.reasoning_efforts,
                    id: id_str.clone(),
                    profile_type: Some("llm".to_owned()),
                    name: record.name,
                    model: model.model.clone(),
                    context_window: model.context_window,
                    protocol: protocol.to_owned(),
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                    has_credential,
                    headers: header_views(&connection.headers, &credentials),
                    declared_capabilities: model
                        .declared_capabilities
                        .iter()
                        .map(|cap| capability_str(*cap).to_owned())
                        .collect(),
                    parameters: model.parameters,
                    editable: true,
                    blocking_jobs: Vec::new(),
                    more_blocking_jobs: false,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut llm_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
                if discovery_visible {
                    let group = provider_groups
                        .entry(model.connection_id)
                        .or_insert_with(|| ProviderGroup {
                            name: provider_name.to_owned(),
                            protocol,
                            endpoint: connection.endpoint.clone(),
                            credential_ref: connection.credential_ref.clone(),
                            has_credential,
                            headers: header_views(&connection.headers, &credentials),
                            profile_ids: Vec::new(),
                            models: BTreeSet::new(),
                        });
                    group.profile_ids.push(id_str);
                    group.models.insert(model.model);
                }
            }
            ConfigBody::Agent(agent) => {
                let profile = ManagedAgentView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    profile_type: Some("agent".to_owned()),
                    name: record.name,
                    instructions: agent.instructions,
                    capabilities: agent.capabilities,
                    llm_targets: agent
                        .models
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    compaction_policy: agent.compaction_policy.map(compaction_policy_view),
                    editable: true,
                    blocking_jobs: Vec::new(),
                    more_blocking_jobs: false,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut agent_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
            ConfigBody::Pool(_) => return Err(INCONSISTENT_RECORD),
            ConfigBody::Connection(_) => {}
            ConfigBody::DecisionModel(model) => {
                let (_, connection, _, has_credential) =
                    connection_for_model(&connections, &credentials, model.connection_id)?;
                let profile = DecisionProfileView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    name: record.name,
                    model: model.model.clone(),
                    protocol: model.protocol.as_str().to_owned(),
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                    has_credential,
                    headers: header_views(&connection.headers, &credentials),
                    declared_capabilities: model
                        .declared_capabilities
                        .iter()
                        .map(|capability| decision_capability_str(*capability).to_owned())
                        .collect(),
                    limits: DecisionLimitsView {
                        max_input_tokens_per_request: model.limits.max_input_tokens_per_request,
                        max_output_tokens_per_request: model.limits.max_output_tokens_per_request,
                        max_input_tokens_per_question: model.limits.max_input_tokens_per_question,
                        max_output_tokens_per_question: model.limits.max_output_tokens_per_question,
                    },
                    editable: true,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut decision_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
            ConfigBody::DecisionAgent(agent) => {
                let profile = DecisionAgentProfileView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    name: record.name,
                    instructions: agent.instructions,
                    models: agent
                        .models
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    default_model: agent
                        .default_model
                        .map(|id| ProfileId::from(id).to_string()),
                    editable: true,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !push_page_item(
                    &mut decision_agent_profiles,
                    profile,
                    record.id,
                    &mut page_bytes,
                    &mut last_emitted,
                    &mut next_before,
                )? {
                    break;
                }
            }
        }
    }
    llm_profiles.sort_by_key(|profile| {
        std::cmp::Reverse(profile_id(&profile.id).ok().map_or(0, ProfileId::get))
    });
    agent_profiles.sort_by_key(|profile| {
        std::cmp::Reverse(profile_id(&profile.id).ok().map_or(0, ProfileId::get))
    });

    let providers = provider_groups
        .into_iter()
        .map(|(key, group)| ProviderView {
            id: ProfileId::from(key).to_string(),
            name: group.name,
            protocol: group.protocol.to_owned(),
            endpoint: group.endpoint,
            credential_ref: group.credential_ref,
            has_credential: group.has_credential,
            headers: group.headers,
            profile_ids: group.profile_ids,
            models: group.models.into_iter().collect(),
        })
        .collect();

    let result = envelope(serde_json::json!(ProfilesView {
        llm_profiles,
        agent_profiles,
        decision_profiles: (!decision_profiles.is_empty()).then_some(decision_profiles),
        decision_agent_profiles: (!decision_agent_profiles.is_empty())
            .then_some(decision_agent_profiles),
        executor_protocols,
        providers,
        has_more: next_before.is_some(),
        next_before: next_before.map(|id| ProfileId::from(id).to_string()),
        profile_id: saved_profile.map(|id| id.to_string()),
    }));
    let result = {
        let mut result = result;
        if let Some(object) = result.as_object_mut() {
            object.insert("resource_model".into(), "independent".into());
        }
        result
    };
    Ok(result)
}

fn fits_profile_page(entry: &impl serde::Serialize, bytes: &mut usize) -> Result<bool, Refusal> {
    let size = serde_json::to_vec(entry).map_err(|_| INCONSISTENT_RECORD)?;
    if *bytes > 0 && *bytes + size.len() > MAX_PROFILE_PAGE_BYTES {
        return Ok(false);
    }
    *bytes += size.len();
    Ok(true)
}

enum PreparedProfileSave {
    Config(Config, Option<u32>),
    Model(ModelProfileSave),
}

struct CommonModelInput {
    name: String,
    model: String,
    endpoint: Option<String>,
    credential_ref: Option<String>,
    credential: Option<String>,
    clear_credential: bool,
    headers: Option<Vec<OutboundHeaderInput>>,
    expected_edit_seq: Option<u32>,
}

async fn prepare_model_save(
    database: &DatabaseHandle,
    existing_id: Option<ProfileId>,
    kind: ConfigKind,
    input: CommonModelInput,
    connection_suffix: &str,
    build_definition: impl FnOnce(String) -> ModelProfileDefinition,
) -> Result<ModelProfileSave, Refusal> {
    if !valid_single_line(&input.name, MAX_PROFILE_NAME_BYTES) {
        return Err(INVALID_PROFILE_NAME);
    }
    if !valid_single_line(&input.model, MAX_MODEL_BYTES) {
        return Err(INVALID_MODEL);
    }
    let name = input.name.trim().to_owned();
    let model = input.model.trim().to_owned();
    let endpoint = normalize_endpoint(input.endpoint.as_deref())?;
    let (credential_ref, secret) = resolve_credential_input(
        &name,
        input.credential_ref.as_deref(),
        input.credential.as_deref(),
        input.clear_credential,
    )?;
    let previous = previous_model_connection(database, existing_id, kind).await?;
    let previous_headers = previous
        .as_ref()
        .map(|(_, connection, _)| connection.headers.clone())
        .unwrap_or_default();
    let guard = revision_guard(previous.map(|(_, _, guard)| guard), input.expected_edit_seq)?;
    let plan = plan_headers(database, input.headers, &previous_headers).await?;
    Ok(ModelProfileSave {
        headers: plan.change,
        existing_id,
        connection_name: format!("{name} {connection_suffix}"),
        name,
        endpoint,
        credential_ref,
        secret,
        credential: None,
        guard,
        definition: build_definition(model),
    })
}

pub(crate) async fn save_profile(
    database: &DatabaseHandle,
    existing_id: Option<ProfileId>,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ProfileRequest = serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    let prepared = match request {
        ProfileRequest::Llm {
            name,
            model,
            context_window,
            protocol,
            endpoint,
            credential_ref,
            credential,
            clear_credential,
            declared_capabilities,
            parameters,
            headers,
            expected_edit_seq,
        } => {
            let protocol = protocol
                .as_deref()
                .map(str::trim)
                .map(|value| LlmTransport::parse(value).ok_or(UNSUPPORTED_PROTOCOL))
                .transpose()?;
            let capabilities = parse_capability_set(
                &declared_capabilities,
                parse_capability,
                INVALID_CAPABILITIES,
            )?;
            if parameters
                .temperature
                .is_some_and(|v| !v.is_finite() || !(0.0..=2.0).contains(&v))
                || parameters
                    .top_p
                    .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
                || parameters.max_tokens.is_some_and(|v| v == 0)
                || parameters.max_completion_tokens.is_some_and(|v| v == 0)
                || (parameters.max_tokens.is_some() && parameters.max_completion_tokens.is_some())
                || parameters
                    .reasoning_effort
                    .as_deref()
                    .is_some_and(|effort| {
                        crate::service::ai::providers::openai_compatible::request::parse_reasoning_effort(effort)
                            .is_err()
                    })
                || parameters
                    .presence_penalty
                    .is_some_and(|v| !v.is_finite() || !(-2.0..=2.0).contains(&v))
                || parameters
                    .frequency_penalty
                    .is_some_and(|v| !v.is_finite() || !(-2.0..=2.0).contains(&v))
            {
                return Err(INVALID_PARAMETERS);
            }
            let save = prepare_model_save(
                database,
                existing_id,
                ConfigKind::Model,
                CommonModelInput {
                    name,
                    model,
                    endpoint,
                    credential_ref,
                    credential,
                    clear_credential,
                    headers,
                    expected_edit_seq,
                },
                "provider",
                |model| ModelProfileDefinition::Llm {
                    protocol,
                    model,
                    context_window,
                    parameters: LlmParameters {
                        temperature: parameters.temperature,
                        top_p: parameters.top_p,
                        max_tokens: parameters.max_tokens,
                        reasoning_effort: parameters.reasoning_effort,
                        max_completion_tokens: parameters.max_completion_tokens,
                        presence_penalty: parameters.presence_penalty,
                        frequency_penalty: parameters.frequency_penalty,
                    },
                    declared_capabilities: capabilities,
                },
            )
            .await?;
            PreparedProfileSave::Model(save)
        }
        ProfileRequest::Agent {
            name,
            instructions,
            capabilities,
            llm_targets,
            compaction_policy,
            expected_edit_seq,
        } => {
            if compaction_policy.is_some() {
                return Err(Refusal::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "global_compaction_policy",
                    "change compaction through /api/v1/settings/compaction",
                ));
            }
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let normalized_instructions = normalize_instructions(instructions.as_deref())?;
            let mut selected_capabilities = BTreeSet::new();
            for capability in capabilities {
                selected_capabilities
                    .insert(AgentCapability::parse(&capability).ok_or(INVALID_AGENT_CAPABILITIES)?);
            }
            if selected_capabilities.is_empty() {
                return Err(INVALID_AGENT_CAPABILITIES);
            }
            let selection = AgentConfig {
                capabilities: selected_capabilities.clone(),
                ..AgentConfig::default()
            };
            if llm_targets.is_empty() {
                return Err(MISSING_LLM_TARGETS);
            }
            let parsed_targets =
                parse_profile_refs(database, &llm_targets, INVALID_LLM_TARGET, |record| {
                    record.archived_at.is_none() && selection.supports_model(&record.body)
                })
                .await?;
            let (prior, expected) = if let Some(id) = existing_id {
                let record = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
                let expected = config_revision(record.edit_seq, expected_edit_seq)?;
                let ConfigBody::Agent(agent) = record.body else {
                    return Err(WRONG_PROFILE_KIND);
                };
                (agent, expected)
            } else {
                (AgentConfig::default(), None)
            };
            PreparedProfileSave::Config(
                Config {
                    name: name.trim().to_owned(),
                    body: ConfigBody::Agent(AgentConfig {
                        instructions: normalized_instructions,
                        capabilities: selected_capabilities,
                        models: parsed_targets,
                        compaction_policy: prior.compaction_policy,
                    }),
                },
                expected,
            )
        }
        ProfileRequest::DecisionModel {
            name,
            model,
            protocol,
            endpoint,
            credential_ref,
            credential,
            clear_credential,
            declared_capabilities,
            limits,
            headers,
            expected_edit_seq,
        } => {
            let decision_protocol = protocol
                .as_deref()
                .map(str::trim)
                .map(DecisionProtocol::parse)
                .unwrap_or(Some(DecisionProtocol::TypeSafeSystemOne))
                .ok_or(UNSUPPORTED_DECISION_PROTOCOL)?;
            let capabilities = parse_capability_set(
                &declared_capabilities,
                parse_decision_capability,
                INVALID_DECISION_CAPABILITIES,
            )?;
            let decision_limits = DecisionLimits {
                max_input_tokens_per_request: limits.max_input_tokens_per_request,
                max_output_tokens_per_request: limits.max_output_tokens_per_request,
                max_input_tokens_per_question: limits.max_input_tokens_per_question,
                max_output_tokens_per_question: limits.max_output_tokens_per_question,
            };
            for value in [
                decision_limits.max_input_tokens_per_request,
                decision_limits.max_output_tokens_per_request,
                decision_limits.max_input_tokens_per_question,
                decision_limits.max_output_tokens_per_question,
            ] {
                if value.is_some_and(|value| value == 0 || value > 1_000_000) {
                    return Err(INVALID_DECISION_LIMITS);
                }
            }
            for (question, request) in [
                (
                    decision_limits.max_input_tokens_per_question,
                    decision_limits.max_input_tokens_per_request,
                ),
                (
                    decision_limits.max_output_tokens_per_question,
                    decision_limits.max_output_tokens_per_request,
                ),
            ] {
                if question
                    .zip(request)
                    .is_some_and(|(question, request)| question > request)
                {
                    return Err(INVALID_DECISION_LIMITS);
                }
            }
            let save = prepare_model_save(
                database,
                existing_id,
                ConfigKind::DecisionModel,
                CommonModelInput {
                    name,
                    model,
                    endpoint,
                    credential_ref,
                    credential,
                    clear_credential,
                    headers,
                    expected_edit_seq,
                },
                "decision provider",
                |model| ModelProfileDefinition::Decision {
                    protocol: decision_protocol,
                    model,
                    limits: decision_limits,
                    declared_capabilities: capabilities,
                },
            )
            .await?;
            PreparedProfileSave::Model(save)
        }
        ProfileRequest::DecisionAgent {
            name,
            instructions,
            models,
            default_model,
            expected_edit_seq,
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let normalized_instructions = normalize_instructions(instructions.as_deref())?;
            if models.is_empty() {
                return Err(MISSING_DECISION_MODELS);
            }
            let mut parsed_models =
                parse_profile_refs(database, &models, INVALID_DECISION_MODELS, |record| {
                    record.archived_at.is_none()
                        && matches!(
                            record.body,
                            ConfigBody::DecisionModel(_) | ConfigBody::Model(_)
                        )
                })
                .await?;
            parsed_models.sort();
            let invalid_default = Refusal::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_default_decision_model",
                "default_model must be one of models",
            );
            let default_model = match default_model {
                None => None,
                Some(value) => value
                    .map(|raw| {
                        profile_id(&raw)
                            .map(ConfigId::from)
                            .map_err(|_| invalid_default.clone())
                    })
                    .transpose()?,
            };
            if default_model.is_some_and(|id| !parsed_models.contains(&id)) {
                return Err(invalid_default);
            }
            let expected = match existing_id {
                Some(id) => {
                    let record = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
                    if record.kind != ConfigKind::DecisionAgent {
                        return Err(WRONG_PROFILE_KIND);
                    }
                    config_revision(record.edit_seq, expected_edit_seq)?
                }
                None => None,
            };
            PreparedProfileSave::Config(
                Config {
                    name: name.trim().to_owned(),
                    body: ConfigBody::DecisionAgent(DecisionAgentConfig {
                        instructions: normalized_instructions,
                        models: parsed_models,
                        default_model,
                    }),
                },
                expected,
            )
        }
    };

    let status = if existing_id.is_some() {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let profile_id = match prepared {
        PreparedProfileSave::Config(new_config, expected) => match existing_id {
            None => ProfileId::from(
                database
                    .create_config(new_config)
                    .await
                    .map_err(map_mutation_error)?,
            ),
            Some(id) => {
                database
                    .update_config(
                        id.into(),
                        new_config,
                        expected.ok_or(PROFILE_REVISION_REQUIRED)?,
                    )
                    .await
                    .map_err(map_mutation_error)?;
                id
            }
        },
        PreparedProfileSave::Model(save) => {
            let (saved_id, _) = database
                .save_model_profile(save)
                .await
                .map_err(map_mutation_error)?;
            ProfileId::from(saved_id)
        }
    };
    let snapshot = build_profiles_snapshot(database, Some(profile_id)).await?;
    Ok(Reply::new(status, snapshot))
}

async fn parse_profile_refs(
    database: &crate::db::DatabaseHandle,
    raw_ids: &[String],
    invalid: Refusal,
    accept: impl Fn(&ConfigRecord) -> bool,
) -> Result<Vec<ConfigId>, Refusal> {
    let mut parsed = Vec::with_capacity(raw_ids.len());
    for raw in raw_ids {
        let id = crate::db::entities::parse_entity_id(raw.trim())
            .map(ConfigId::new)
            .ok_or_else(|| invalid.clone())?;
        if parsed.contains(&id) {
            return Err(invalid);
        }
        match database.config(id).await? {
            Some(record) if accept(&record) => parsed.push(id),
            _ => return Err(invalid),
        }
    }
    Ok(parsed)
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ImportedModels")]
pub(crate) struct ImportedModelsView {
    profile_ids: Vec<String>,
    created: usize,
    existing: usize,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProviderModels")]
pub(crate) struct ProviderModelsView {
    models: Vec<crate::db::entities::DiscoveredModel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportModelsRequest {
    source_profile_id: Option<String>,
    provider: Option<ImportProvider>,
    models: Vec<crate::db::entities::DiscoveredModel>,
    declared_capabilities: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportProvider {
    protocol: Option<String>,
    name: String,
    endpoint: String,
    credential_ref: Option<String>,
    credential: Option<String>,
    #[serde(default)]
    headers: Option<Vec<OutboundHeaderInput>>,
}

pub(crate) async fn import_models(
    database: &DatabaseHandle,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ImportModelsRequest =
        serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    if request.models.is_empty() {
        return Err(INVALID_MODEL);
    }
    let mut models = BTreeMap::new();
    for model in request.models {
        if !valid_single_line(&model.id, MAX_MODEL_BYTES) || model.context_window == Some(0) {
            return Err(INVALID_MODEL);
        }
        models.entry(model.id.trim().to_owned()).or_insert(model);
    }
    let capabilities = parse_capability_set(
        &request.declared_capabilities,
        parse_capability,
        INVALID_CAPABILITIES,
    )?;
    let headers;
    let (connection, credential_ref, secret) = match (request.source_profile_id, request.provider) {
        (Some(id), None) => {
            headers = HeaderChange::Preserve;
            let id = profile_id(&id)?;
            let source = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
            if source.archived_at.is_some() {
                return Err(PROFILE_NOT_FOUND);
            }
            let ConfigBody::Model(model) = source.body else {
                return Err(WRONG_PROFILE_KIND);
            };
            let connection = database
                .config(model.connection_id)
                .await?
                .ok_or(INCONSISTENT_RECORD)?;
            if connection.archived_at.is_some() {
                return Err(PROFILE_NOT_FOUND);
            }
            let ConfigBody::Connection(_) = &connection.body else {
                return Err(INCONSISTENT_RECORD);
            };
            (
                Config {
                    name: connection.name,
                    body: connection.body,
                },
                ProfileCredentialRef::Preserve,
                None,
            )
        }
        (None, Some(provider)) => {
            if !valid_single_line(&provider.name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let endpoint = provider.endpoint.trim();
            if endpoint.len() > MAX_ENDPOINT_BYTES
                || endpoint.chars().any(char::is_whitespace)
                || crate::service::ai::providers::model_endpoint(endpoint).is_err()
            {
                return Err(INVALID_ENDPOINT);
            }
            let reference = provider.credential_ref.filter(|value| !value.is_empty());
            if reference
                .as_deref()
                .is_some_and(|value| !valid_credential_ref(value))
            {
                return Err(INVALID_CREDENTIAL_REF);
            }
            let credential = provider.credential.filter(|value| !value.is_empty());
            if credential
                .as_deref()
                .is_some_and(|secret| !valid_secret_header(secret))
            {
                return Err(INVALID_CREDENTIAL);
            }
            headers = plan_headers(database, provider.headers, &[]).await?.change;
            let (credential_ref, secret) = match credential {
                Some(secret) => {
                    let credential_ref = match reference {
                        Some(reference) => ProfileCredentialRef::Explicit(reference),
                        None => ProfileCredentialRef::PreserveOrGenerate(default_credential_ref(
                            provider.name.trim(),
                        )),
                    };
                    (
                        credential_ref,
                        Some(CredentialSecret::new(secret).map_err(|_| INVALID_CREDENTIAL)?),
                    )
                }
                None => (
                    reference.map_or(ProfileCredentialRef::Clear, ProfileCredentialRef::Explicit),
                    None,
                ),
            };
            (
                Config {
                    name: format!("{} provider", provider.name.trim()),
                    body: ConfigBody::Connection(ConnectionConfig {
                        protocol: provider
                            .protocol
                            .as_deref()
                            .map(LlmTransport::parse)
                            .unwrap_or(Some(LlmTransport::OpenAiChatCompletions))
                            .ok_or(UNSUPPORTED_PROTOCOL)?,
                        endpoint: Some(endpoint.to_owned()),
                        credential_ref: None,
                        headers: Vec::new(),
                    }),
                },
                credential_ref,
                secret,
            )
        }
        (None, None) | (Some(_), Some(_)) => return Err(INVALID_PROFILE_BODY),
    };
    if !matches!(&connection.body, ConfigBody::Connection(_)) {
        return Err(WRONG_PROFILE_KIND);
    }
    let (ids, created) = database
        .import_models(ImportModelsSave {
            headers,
            connection,
            credential_ref,
            secret,
            credential: None,
            models: models.into_values().collect(),
            capabilities,
        })
        .await
        .map_err(map_mutation_error)?;
    let existing = ids.len().saturating_sub(created);
    Ok(Reply::new(
        StatusCode::CREATED,
        envelope(serde_json::json!(ImportedModelsView {
            profile_ids: ids
                .into_iter()
                .map(|id| ProfileId::from(id).to_string())
                .collect(),
            created,
            existing,
        })),
    ))
}

pub(crate) async fn delete_model(
    database: &DatabaseHandle,
    id: ProfileId,
) -> Result<Reply, Refusal> {
    database
        .archive_model(id.into())
        .await
        .map_err(map_mutation_error)?;
    Ok(Reply::ok(
        build_profiles_snapshot(database, Some(id)).await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelAvailability {
    enabled: bool,
}

pub(crate) async fn set_model_availability(
    database: &DatabaseHandle,
    id: ProfileId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ModelAvailability =
        serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    database
        .set_config_enabled(id.into(), ConfigKind::Model, request.enabled)
        .await
        .map_err(map_mutation_error)?;
    Ok(Reply::ok(
        build_profiles_snapshot(database, Some(id)).await?,
    ))
}

pub(crate) async fn remove_provider(
    database: &DatabaseHandle,
    id: ProfileId,
) -> Result<Reply, Refusal> {
    database
        .remove_provider(id.into())
        .await
        .map_err(map_mutation_error)?;
    Ok(Reply::ok(build_profiles_snapshot(database, None).await?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderModelsRequest {
    endpoint: String,
    protocol: Option<String>,
    credential: Option<String>,
    credential_ref: Option<String>,
    #[serde(default)]
    profile_id: Option<String>,
    #[serde(default)]
    headers: Option<Vec<OutboundHeaderInput>>,
}

#[derive(Clone, Copy)]
enum DiscoveryProtocol {
    Generative(LlmTransport),
    Decision(DecisionProtocol),
}

pub(crate) async fn provider_models(
    database: &DatabaseHandle,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ProviderModelsRequest =
        serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    let protocol = match request.protocol.as_deref().map(str::trim) {
        None => DiscoveryProtocol::Generative(LlmTransport::OpenAiChatCompletions),
        Some(name) => match DecisionProtocol::parse(name) {
            Some(protocol) => DiscoveryProtocol::Decision(protocol),
            None => DiscoveryProtocol::Generative(
                LlmTransport::parse(name).ok_or(UNSUPPORTED_PROTOCOL)?,
            ),
        },
    };
    if request.endpoint.len() > MAX_ENDPOINT_BYTES
        || crate::service::ai::providers::model_endpoint(&request.endpoint).is_err()
    {
        return Err(INVALID_ENDPOINT);
    }
    if request.credential.is_some() && request.credential_ref.is_some() {
        return Err(INVALID_PROFILE_BODY);
    }
    if !crate::system::http::discovery_permitted(&request.endpoint) {
        return Err(PROVIDER_DISCOVERY_DISABLED);
    }
    let saved_connection = request.credential.is_none();
    let credential = match request.credential {
        Some(credential) => Some(credential),
        None => match request.credential_ref.as_deref() {
            Some(reference) => {
                if !valid_credential_ref(reference)
                    || !saved_connection_credential(database, reference).await?
                {
                    return Err(INVALID_CREDENTIAL_REF);
                }
                database
                    .credential_resolver()
                    .resolve(Some(reference))
                    .await
                    .map_err(|_| INVALID_CREDENTIAL_REF)?
            }
            None => None,
        },
    };
    if credential
        .as_deref()
        .is_some_and(|secret| !valid_secret_header(secret))
    {
        return Err(INVALID_CREDENTIAL);
    }
    use crate::service::ai::providers::ModelListError;
    let saved_headers = match request.profile_id.as_deref() {
        None => Vec::new(),
        Some(id) => {
            let kind = match protocol {
                DiscoveryProtocol::Generative(_) => ConfigKind::Model,
                DiscoveryProtocol::Decision(_) => ConfigKind::DecisionModel,
            };
            match previous_model_connection(database, Some(profile_id(id)?), kind).await? {
                Some((_, connection, _)) => connection.headers,
                None => Vec::new(),
            }
        }
    };
    let plan = plan_headers(database, request.headers, &saved_headers).await?;
    let listed = crate::service::ai::providers::list_models(
        &request.endpoint,
        credential.as_deref(),
        match protocol {
            DiscoveryProtocol::Generative(protocol) => Some(protocol),
            DiscoveryProtocol::Decision(protocol) => {
                debug_assert_eq!(protocol, DecisionProtocol::TypeSafeSystemOne);
                None
            }
        },
        &plan.probe,
    )
    .await;
    let (code, message, status_code) = match listed {
        Ok(models) => {
            if saved_connection && let DiscoveryProtocol::Generative(protocol) = protocol {
                database
                    .refresh_model_metadata(
                        ConnectionConfig {
                            protocol,
                            endpoint: Some(request.endpoint.clone()),
                            credential_ref: request.credential_ref.clone(),
                            headers: Vec::new(),
                        },
                        models.clone(),
                    )
                    .await?;
            }
            return Ok(Reply::ok(envelope(serde_json::json!(ProviderModelsView {
                models
            }))));
        }
        Err(ModelListError::NotAuthorized) => return Err(PROVIDER_DISCOVERY_DISABLED),
        Err(ModelListError::Unreachable) => (
            "provider_unreachable",
            "could not connect to the provider".to_owned(),
            None,
        ),
        Err(ModelListError::NotListed) => (
            "models_not_listed",
            "the provider does not list models".to_owned(),
            None,
        ),
        Err(ModelListError::Rejected {
            status_code,
            message,
        }) => (
            "provider_rejected",
            bounded_provider_message(message),
            Some(status_code),
        ),
    };
    Ok(Reply::new(
        StatusCode::BAD_GATEWAY,
        envelope(serde_json::json!({
            "error": ({ let mut object = serde_json::json!({
                "code": code.to_owned(),
                "message": message,
            }); if let Some(value) = status_code { object["status_code"] = serde_json::json!(value); } object }),
        })),
    ))
}

pub(super) async fn global_outbound_headers(
    database: &DatabaseHandle,
    body: Option<&[u8]>,
) -> Result<Reply, Refusal> {
    if let Some(body) = body {
        let wire: GlobalOutboundHeadersInput =
            serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
        let mut fields = Vec::with_capacity(wire.headers.len());
        for input in wire.headers {
            if input.secret {
                return Err(invalid_headers(&HeaderError::GlobalSecret));
            }
            let name = canonical_name(input.name.trim()).map_err(|e| invalid_headers(&e))?;
            let value = input
                .value
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid_headers(&HeaderError::InvalidValue))?;
            validate_value(&value).map_err(|e| invalid_headers(&e))?;
            fields.push(OutboundHeader {
                name,
                value: OutboundHeaderValue::Plain(value),
            });
        }
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        validate_fields(&fields, HeaderScope::Global).map_err(|e| invalid_headers(&e))?;
        database
            .set_global_outbound_headers(wire.revision, fields)
            .await
            .map_err(|error| match error {
                DatabaseError::Rejected(Violation::InvalidValue("revision", _)) => Refusal::new(
                    StatusCode::CONFLICT,
                    "headers_revision_changed",
                    "global headers changed; reload before saving",
                ),
                error => map_mutation_error(error),
            })?;
    }
    let headers = database.global_outbound_headers().await?;
    Ok(Reply::ok(envelope(serde_json::json!(
        GlobalOutboundHeadersView {
            revision: headers.revision,
            headers: header_views(&headers.fields, &BTreeSet::new()),
            default_user_agent: concat!("ocg/", env!("CARGO_PKG_VERSION")).to_owned(),
        }
    ))))
}

pub(super) async fn global_compaction_settings(
    database: &DatabaseHandle,
    body: Option<&[u8]>,
) -> Result<Reply, Refusal> {
    if let Some(body) = body {
        let wire: GlobalCompactionSettingsInput =
            serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
        database
            .set_global_compaction_settings(crate::db::entities::GlobalCompactionSettings {
                revision: wire.revision,
                prediction_enabled: wire.prediction_enabled,
                policy: wire.policy.into_policy()?,
            })
            .await
            .map_err(|error| match error {
                DatabaseError::Rejected(Violation::InvalidValue("revision", _)) => Refusal::new(
                    StatusCode::CONFLICT,
                    "compaction_revision_changed",
                    "global compaction settings changed; reload before saving",
                ),
                DatabaseError::Rejected(Violation::InvalidValue(_, _)) => Refusal::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "invalid_compaction_policy",
                    "invalid compaction policy, predictor or budgets",
                ),
                error => map_mutation_error(error),
            })?;
    }
    let settings = database.global_compaction_settings().await?;
    Ok(Reply::ok(envelope(serde_json::json!(
        GlobalCompactionSettingsInput {
            revision: settings.revision,
            prediction_enabled: settings.prediction_enabled,
            policy: compaction_policy_view(settings.policy),
        }
    ))))
}
