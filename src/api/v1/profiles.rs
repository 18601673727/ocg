//! Profile management and computed Provider projection routes:
//! - `GET /api/v1/profiles`
//! - `POST /api/v1/profiles`
//! - `POST /api/v1/profiles/{profile}`
//!
//! A Profile is a config: an LLM Profile is a Model config and an Agent
//! Profile an Agent config. Providers are computed projections derived from
//! the LLM Profiles; the Connection configs they name are resolved or created
//! automatically from Profile-owned configuration. Edits are always allowed
//! and change no Job that froze the old values. Raw credentials are
//! write-only and never returned in any read response.

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
use crate::db::mutations::{ModelProfileDefinition, ModelProfileSave, ProfileCredentialRef};
use crate::db::{DatabaseError, DatabaseHandle};

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
const CREDENTIAL_NOT_SAVED: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "credential_not_saved",
    "the profile was saved but its API key was not; save again",
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
const PROFILE_CHANGED: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "profile_changed",
    "the Profile changed while it was being validated; reload and retry",
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
    },
    /// A Decision Model: a typed model reached over a decision protocol.
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
    },
    /// A Decision Profile: an Agent whose models are Decision Models.
    DecisionAgent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        models: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        default_model: Option<Option<String>>,
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

/// Whether some saved Connection names `reference`, so `/provider-models`
/// cannot turn a `credential_ref` into an arbitrary environment lookup.
async fn saved_connection_credential(
    database: &DatabaseHandle,
    reference: &str,
) -> Result<bool, Refusal> {
    Ok(database
        .find_config(ConfigKind::Connection, |record| {
            matches!(&record.body, ConfigBody::Connection(connection)
                if connection.credential_ref.as_deref() == Some(reference))
        })
        .await?
        .is_some())
}

async fn unused_credential_ref(database: &DatabaseHandle, name: &str) -> Result<String, Refusal> {
    let base = default_credential_ref(name);
    let mut candidate = base.clone();
    let mut suffix = 2u64;
    while saved_connection_credential(database, &candidate).await? {
        candidate = format!("{base}_{suffix}");
        suffix = suffix.checked_add(1).ok_or(INTERNAL)?;
    }
    Ok(candidate)
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

/// The credential reference a Profile names, and the secret to write, if
/// any. An update keeps the reference it had when none is supplied, and an
/// explicit clear removes it.
fn resolve_credential_input(
    profile_name: &str,
    credential_ref: Option<&str>,
    credential: Option<&str>,
    clear_credential: bool,
) -> Result<(ProfileCredentialRef, Option<String>), Refusal> {
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
            Ok((credential_ref, Some(secret_value.to_owned())))
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

async fn validate_decision_model(
    database: &DatabaseHandle,
    protocol: DecisionProtocol,
    endpoint: Option<&str>,
    model: &str,
    credential_ref: Option<&str>,
    credential: Option<&str>,
) -> Result<(), Refusal> {
    let resolved_credential;
    let credential = match credential {
        Some(secret) => Some(secret),
        None => {
            resolved_credential = database
                .credential_resolver()
                .resolve(credential_ref)
                .map_err(|error| INVALID_CREDENTIAL_REF.with_reason(error.to_string()))?;
            resolved_credential.as_deref()
        }
    };
    use crate::service::ai::providers::ModelListError;
    let listed = match protocol {
        DecisionProtocol::TypeSafeSystemOne => {
            crate::service::ai::decisive_model::list_models(
                endpoint.unwrap_or(crate::service::ai::decisive_model::DEFAULT_ENDPOINT),
                credential,
            )
            .await
        }
    }
    .map_err(|error| {
        let (code, message) = match error {
            ModelListError::Unreachable => (
                "provider_unreachable",
                "could not connect to the provider".to_owned(),
            ),
            ModelListError::NotListed => (
                "models_not_listed",
                "the provider does not list models".to_owned(),
            ),
            ModelListError::Rejected { message, .. } => {
                ("provider_rejected", bounded_provider_message(message))
            }
        };
        Refusal::new(
            StatusCode::BAD_GATEWAY,
            code,
            "decision model validation failed",
        )
        .with_reason(message)
    })?;
    if listed.iter().any(|listed| listed.id == model) {
        Ok(())
    } else {
        Err(Refusal::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "decision_model_unavailable",
            "the provider does not list the configured decision model",
        )
        .with_reason(format!(
            "the provider does not list decision model `{model}`"
        )))
    }
}

/// The default Profile page size.
const DEFAULT_PROFILE_PAGE: usize = 50;
/// How many configs one page examines per Profile it may return.
const PROFILE_SCAN_FACTOR: usize = 4;
/// The serialized budget one Profile page may exceed only for a single entry.
const MAX_PROFILE_PAGE_BYTES: usize = 512 * 1024;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "LlmProfile")]
pub(crate) struct LlmProfileView {
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

/// Reads one page of LLM and Agent Profiles, newest first, and the Provider
/// projection of the LLM Profiles on that page. `next_before` continues from
/// the last Profile returned, or from the last config examined when the scan
/// or byte budget ended the page first; `None` means the configs were
/// exhausted.
pub(crate) async fn list_profiles(
    database: &DatabaseHandle,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .profile_page(before, limit, limit.saturating_mul(PROFILE_SCAN_FACTOR))
        .await?;
    Ok(Reply::ok(build_profiles_page(database, page, None)?))
}

/// The first page after a mutation, which also names the Profile it saved.
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
    build_profiles_page(database, page, saved_profile)
}

fn build_profiles_page(
    database: &DatabaseHandle,
    page: crate::db::redb::ProfilePageRecords,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let crate::db::redb::ProfilePageRecords {
        records,
        connections,
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
        profile_ids: Vec<String>,
        models: BTreeSet<String>,
    }
    let mut provider_groups: BTreeMap<ConfigId, ProviderGroup> = BTreeMap::new();
    let mut last_emitted = None;
    let mut page_bytes = 0;
    let mut next_before = next_before;

    for record in records {
        let id = ProfileId::from(record.id);
        match record.body {
            ConfigBody::Model(model) => {
                let Some((provider_name, connection, discovery_visible)) =
                    connections.get(&model.connection_id).cloned()
                else {
                    return Err(INCONSISTENT_RECORD);
                };
                let protocol = connection.protocol.as_str();
                let has_credential = connection
                    .credential_ref
                    .as_deref()
                    .is_some_and(|reference| database.has_credential(reference));
                let capabilities = model
                    .declared_capabilities
                    .iter()
                    .map(|cap| capability_str(*cap))
                    .collect::<Vec<_>>();
                let id_str = id.to_string();
                let profile = LlmProfileView {
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
                    declared_capabilities: capabilities.into_iter().map(str::to_owned).collect(),
                    parameters: model.parameters,
                    editable: true,
                    blocking_jobs: Vec::new(),
                    more_blocking_jobs: false,
                    created_at_us: record.created_at.as_unix_micros(),
                    updated_at_us: record.updated_at.as_unix_micros(),
                };
                if !fits_profile_page(&profile, &mut page_bytes)? {
                    next_before = last_emitted;
                    break;
                }
                if discovery_visible {
                    let group = provider_groups
                        .entry(model.connection_id)
                        .or_insert_with(|| ProviderGroup {
                            name: provider_name,
                            protocol,
                            endpoint: connection.endpoint,
                            credential_ref: connection.credential_ref,
                            has_credential,
                            profile_ids: Vec::new(),
                            models: BTreeSet::new(),
                        });
                    group.profile_ids.push(id_str);
                    group.models.insert(model.model);
                }
                llm_profiles.push(profile);
                last_emitted = Some(record.id);
            }
            ConfigBody::Agent(agent) => {
                let profile = ManagedAgentView {
                    id: id.to_string(),
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
                if !fits_profile_page(&profile, &mut page_bytes)? {
                    next_before = last_emitted;
                    break;
                }
                agent_profiles.push(profile);
                last_emitted = Some(record.id);
            }
            ConfigBody::Pool(pool) => {
                let offered: Vec<_> = [
                    LlmTransport::OpenAiChatCompletions,
                    LlmTransport::OpenAiResponses,
                    LlmTransport::AnthropicMessages,
                ]
                .into_iter()
                .filter(|protocol| {
                    pool.enabled && pool.offers(ExecutionOperation::LlmRequest, Some(*protocol))
                })
                .map(|protocol| ProfileExecutorView {
                    id: record.id.to_string(),
                    protocol: protocol.as_str().to_owned(),
                })
                .collect();
                if !offered.is_empty() {
                    if !fits_profile_page(&offered, &mut page_bytes)? {
                        next_before = last_emitted;
                        break;
                    }
                    executor_protocols.extend(offered);
                    last_emitted = Some(record.id);
                }
            }
            ConfigBody::Connection(_) => {}
            ConfigBody::DecisionModel(model) => {
                let Some((_, connection, _)) = connections.get(&model.connection_id).cloned()
                else {
                    return Err(INCONSISTENT_RECORD);
                };
                let has_credential = connection
                    .credential_ref
                    .as_deref()
                    .is_some_and(|reference| database.has_credential(reference));
                let profile = DecisionProfileView {
                    id: id.to_string(),
                    edit_seq: record.edit_seq,
                    name: record.name,
                    model: model.model.clone(),
                    protocol: model.protocol.as_str().to_owned(),
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                    has_credential,
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
                if !fits_profile_page(&profile, &mut page_bytes)? {
                    next_before = last_emitted;
                    break;
                }
                decision_profiles.push(profile);
                last_emitted = Some(record.id);
            }
            ConfigBody::DecisionAgent(agent) => {
                let profile = DecisionAgentProfileView {
                    id: id.to_string(),
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
                if !fits_profile_page(&profile, &mut page_bytes)? {
                    next_before = last_emitted;
                    break;
                }
                decision_agent_profiles.push(profile);
                last_emitted = Some(record.id);
            }
        }
    }
    // Newest first, as Profiles have always been listed.
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
            profile_ids: group.profile_ids,
            models: group.models.into_iter().collect(),
        })
        .collect();

    Ok(envelope(serde_json::json!(ProfilesView {
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
    })))
}

/// Whether `entry` fits the page budget. The first entry always does, so a
/// page that starts on an oversized Profile still advances.
fn fits_profile_page(entry: &impl serde::Serialize, bytes: &mut usize) -> Result<bool, Refusal> {
    let size = serde_json::to_vec(entry).map_err(|_| INCONSISTENT_RECORD)?;
    if *bytes > 0 && *bytes + size.len() > MAX_PROFILE_PAGE_BYTES {
        return Ok(false);
    }
    *bytes += size.len();
    Ok(true)
}

enum PreparedProfileSave {
    Config(Config),
    Model(ModelProfileSave, Option<String>),
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
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            if !valid_single_line(&model, MAX_MODEL_BYTES) {
                return Err(INVALID_MODEL);
            }
            let protocol = protocol
                .as_deref()
                .map(str::trim)
                .map(|value| LlmTransport::parse(value).ok_or(UNSUPPORTED_PROTOCOL))
                .transpose()?;
            let normalized_endpoint = normalize_endpoint(endpoint.as_deref())?;

            let mut capabilities = BTreeSet::new();
            for item in &declared_capabilities {
                let Some(parsed) = parse_capability(item.trim()) else {
                    return Err(INVALID_CAPABILITIES);
                };
                capabilities.insert(parsed);
            }
            if capabilities.is_empty() {
                return Err(INVALID_CAPABILITIES);
            }

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

            let (credential_ref, secret) = resolve_credential_input(
                name.trim(),
                credential_ref.as_deref(),
                credential.as_deref(),
                clear_credential,
            )?;
            PreparedProfileSave::Model(
                ModelProfileSave {
                    existing_id,
                    name: name.trim().to_owned(),
                    connection_name: format!("{} provider", name.trim()),
                    endpoint: normalized_endpoint,
                    credential_ref,
                    guard: None,
                    definition: ModelProfileDefinition::Llm {
                        protocol,
                        model: model.trim().to_owned(),
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
                },
                secret,
            )
        }
        ProfileRequest::Agent {
            name,
            instructions,
            capabilities,
            llm_targets,
            compaction_policy,
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
            let mut parsed_targets: Vec<ConfigId> = Vec::with_capacity(llm_targets.len());
            for raw_target in &llm_targets {
                let target_id = crate::db::entities::parse_entity_id(raw_target.trim())
                    .map(ConfigId::new)
                    .ok_or(INVALID_LLM_TARGET)?;
                if parsed_targets.contains(&target_id) {
                    return Err(INVALID_LLM_TARGET);
                }
                match database.config(target_id).await? {
                    Some(record)
                        if record.archived_at.is_none()
                            && selection.supports_model(&record.body) =>
                    {
                        parsed_targets.push(target_id)
                    }
                    _ => return Err(INVALID_LLM_TARGET),
                }
            }
            let prior = if let Some(id) = existing_id {
                let record = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
                let ConfigBody::Agent(agent) = record.body else {
                    return Err(WRONG_PROFILE_KIND);
                };
                agent
            } else {
                AgentConfig::default()
            };
            PreparedProfileSave::Config(Config {
                name: name.trim().to_owned(),
                body: ConfigBody::Agent(AgentConfig {
                    instructions: normalized_instructions,
                    capabilities: selected_capabilities,
                    models: parsed_targets,
                    compaction_policy: match compaction_policy {
                        None => prior.compaction_policy,
                        Some(None) => None,
                        Some(Some(policy)) => Some(policy.into_policy()?),
                    },
                }),
            })
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
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            if !valid_single_line(&model, MAX_MODEL_BYTES) {
                return Err(INVALID_MODEL);
            }
            let decision_protocol = protocol
                .as_deref()
                .map(str::trim)
                .map(DecisionProtocol::parse)
                .unwrap_or(Some(DecisionProtocol::TypeSafeSystemOne))
                .ok_or(UNSUPPORTED_DECISION_PROTOCOL)?;
            let normalized_endpoint = normalize_endpoint(endpoint.as_deref())?;
            let mut capabilities = BTreeSet::new();
            for item in &declared_capabilities {
                let Some(parsed) = parse_decision_capability(item.trim()) else {
                    return Err(INVALID_DECISION_CAPABILITIES);
                };
                capabilities.insert(parsed);
            }
            if capabilities.is_empty() {
                return Err(INVALID_DECISION_CAPABILITIES);
            }
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
            // The existing Profile's Connection, when it is edited, keeps its
            // credential reference unless another one is named.
            let previous =
                previous_model_connection(database, existing_id, ConfigKind::DecisionModel).await?;
            let prior_credential_ref = previous
                .as_ref()
                .and_then(|(_, connection, _)| connection.credential_ref.clone());
            let guard = previous.map(|(_, _, guard)| guard);
            let (credential_ref, secret) = resolve_credential_input(
                name.trim(),
                credential_ref.as_deref(),
                credential.as_deref(),
                clear_credential,
            )?;
            let validation_ref = match &credential_ref {
                ProfileCredentialRef::Preserve | ProfileCredentialRef::PreserveOrGenerate(_) => {
                    prior_credential_ref
                }
                ProfileCredentialRef::Clear => None,
                ProfileCredentialRef::Explicit(reference) => Some(reference.clone()),
            };
            validate_decision_model(
                database,
                decision_protocol,
                normalized_endpoint.as_deref(),
                model.trim(),
                validation_ref.as_deref(),
                secret.as_deref(),
            )
            .await?;
            PreparedProfileSave::Model(
                ModelProfileSave {
                    existing_id,
                    name: name.trim().to_owned(),
                    connection_name: format!("{} decision provider provider", name.trim()),
                    endpoint: normalized_endpoint,
                    credential_ref,
                    guard,
                    definition: ModelProfileDefinition::Decision {
                        protocol: decision_protocol,
                        model: model.trim().to_owned(),
                        limits: decision_limits,
                        declared_capabilities: capabilities,
                    },
                },
                secret,
            )
        }
        ProfileRequest::DecisionAgent {
            name,
            instructions,
            models,
            default_model,
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let normalized_instructions = normalize_instructions(instructions.as_deref())?;
            if models.is_empty() {
                return Err(MISSING_DECISION_MODELS);
            }
            let mut parsed_models: Vec<ConfigId> = Vec::with_capacity(models.len());
            for raw_model in &models {
                let model_id = crate::db::entities::parse_entity_id(raw_model.trim())
                    .map(ConfigId::new)
                    .ok_or(INVALID_DECISION_MODELS)?;
                if parsed_models.contains(&model_id) {
                    return Err(INVALID_DECISION_MODELS);
                }
                match database.config(model_id).await? {
                    Some(ConfigRecord {
                        body: ConfigBody::DecisionModel(_) | ConfigBody::Model(_),
                        archived_at: None,
                        ..
                    }) => parsed_models.push(model_id),
                    _ => return Err(INVALID_DECISION_MODELS),
                }
            }
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
            // Workspace reads are not implemented. A Decision Profile that
            // names metadata or full access would advertise a permission it
            // cannot use, so only none is accepted.
            PreparedProfileSave::Config(Config {
                name: name.trim().to_owned(),
                body: ConfigBody::DecisionAgent(DecisionAgentConfig {
                    instructions: normalized_instructions,
                    models: parsed_models,
                    default_model,
                }),
            })
        }
    };

    let (status, profile_id, credential_to_save) = match prepared {
        PreparedProfileSave::Config(new_config) => {
            let profile_id = match existing_id {
                None => ProfileId::from(
                    database
                        .create_config(new_config)
                        .await
                        .map_err(map_mutation_error)?,
                ),
                Some(id) => {
                    database
                        .update_config(id.into(), new_config)
                        .await
                        .map_err(map_mutation_error)?;
                    id
                }
            };
            let status = if existing_id.is_some() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            (status, profile_id, None)
        }
        PreparedProfileSave::Model(save, secret) => {
            let (saved_id, credential_ref) = database
                .save_model_profile(save)
                .await
                .map_err(map_mutation_error)?;
            let status = if existing_id.is_some() {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            };
            (
                status,
                ProfileId::from(saved_id),
                secret.zip(credential_ref),
            )
        }
    };
    if let Some((secret_value, target_ref)) = credential_to_save {
        // The Profile change is already committed. A key that cannot be
        // written leaves it with the old key, or none, so never answer as if
        // the whole save succeeded.
        database
            .set_credential(&target_ref, &secret_value)
            .await
            .map_err(|_| CREDENTIAL_NOT_SAVED)?;
    }
    let snapshot = build_profiles_snapshot(database, Some(profile_id)).await?;
    Ok(Reply::new(status, snapshot))
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
    let mut capabilities = BTreeSet::new();
    for value in request.declared_capabilities {
        capabilities.insert(parse_capability(&value).ok_or(INVALID_CAPABILITIES)?);
    }
    if capabilities.is_empty() {
        return Err(INVALID_CAPABILITIES);
    }
    let mut credential_to_save = None;
    let connection = match (request.source_profile_id, request.provider) {
        (Some(id), None) => {
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
            Config {
                name: connection.name,
                body: connection.body,
            }
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
            if credential.as_ref().is_some_and(|secret| {
                secret.len() > 4096
                    || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret}"))
                        .is_err()
            }) {
                return Err(INVALID_CREDENTIAL);
            }
            let reference = match credential {
                Some(secret) => {
                    let reference = match reference {
                        Some(reference) => reference,
                        None => unused_credential_ref(database, provider.name.trim()).await?,
                    };
                    credential_to_save = Some((reference.clone(), secret));
                    Some(reference)
                }
                None => reference,
            };
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
                    credential_ref: reference,
                }),
            }
        }
        (None, None) | (Some(_), Some(_)) => return Err(INVALID_PROFILE_BODY),
    };
    if !matches!(&connection.body, ConfigBody::Connection(_)) {
        return Err(WRONG_PROFILE_KIND);
    }
    let (ids, created) = database
        .import_models(connection, models.into_values().collect(), capabilities)
        .await
        .map_err(map_mutation_error)?;
    if let Some((reference, credential)) = credential_to_save {
        database
            .set_credential(&reference, &credential)
            .await
            .map_err(|_| CREDENTIAL_NOT_SAVED)?;
    }
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
}

/// Which protocol a discovery request names: a generative transport, or a
/// decision protocol. The two are discovered by different routes, so the
/// protocol decides which one is used.
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
                    .map_err(|_| INVALID_CREDENTIAL_REF)?
            }
            None => None,
        },
    };
    if credential.as_ref().is_some_and(|secret| {
        secret.is_empty()
            || secret.len() > 4096
            || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret}")).is_err()
    }) {
        return Err(INVALID_CREDENTIAL);
    }
    use crate::service::ai::providers::ModelListError;
    let listed = match protocol {
        DiscoveryProtocol::Generative(protocol) => {
            crate::service::ai::providers::list_models(
                &request.endpoint,
                credential.as_deref(),
                protocol,
            )
            .await
        }
        DiscoveryProtocol::Decision(protocol) => {
            debug_assert_eq!(protocol, DecisionProtocol::TypeSafeSystemOne);
            crate::service::ai::decisive_model::list_models(
                &request.endpoint,
                credential.as_deref(),
            )
            .await
        }
    };
    let (code, message, status_code) = match listed {
        Ok(models) => {
            if saved_connection && let DiscoveryProtocol::Generative(protocol) = protocol {
                database
                    .refresh_model_metadata(
                        ConnectionConfig {
                            protocol,
                            endpoint: Some(request.endpoint.clone()),
                            credential_ref: request.credential_ref.clone(),
                        },
                        models.clone(),
                    )
                    .await?;
            }
            return Ok(Reply::ok(envelope(serde_json::json!(ProviderModelsView {
                models
            }))));
        }
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
