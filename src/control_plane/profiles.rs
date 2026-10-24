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
use std::num::NonZeroU32;

use super::contracts::*;
use ntex::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

use super::{
    DATABASE_BUSY, DATABASE_UNAVAILABLE, INCONSISTENT_RECORD, INTERNAL, INVALID_REQUEST, Refusal,
    Reply, SHUTTING_DOWN,
};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    AgentBehavior, AgentConfig, ConfigBody, ConfigId, ConfigKind, ConfigRecord, ConnectionConfig,
    DecisionAgentConfig, DecisionCapability, DecisionLimits, DecisionModelConfig, DecisionProtocol,
    ExecutionOperation, LlmParameters, LlmTransport, ModelCapability, ModelConfig, NewConfig,
    NewExecutor, ProfileId, RecordRef, Violation, WorkspaceAccess,
};

const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_MODEL_BYTES: usize = 256;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_CREDENTIAL_REF_BYTES: usize = 128;
const MAX_PROVIDER_MESSAGE_BYTES: usize = 1024;
const MAX_INSTRUCTIONS_BYTES: usize = 65_536;
const PROFILE_SCAN_LIMIT: usize = 1_000;

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
const INVALID_BEHAVIOR: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_behavior",
    "behavior must be chat or general",
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
    "every decision model must be an existing Decision Model Profile without duplicates",
);
const MISSING_DECISION_MODELS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "missing_decision_models",
    "a Decision Profile must select at least one Decision Model Profile",
);
const INVALID_WORKSPACE_ACCESS: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_workspace_access",
    "workspace_access must be none",
);
const INVALID_DECISION_ASSISTANCE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_assistance",
    "decision_assistance must name an existing Decision Profile",
);
const INVALID_TOOL_GRANT: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_tool_grant",
    "tool_grant must be null, read, edit or code",
);
const WRONG_PROFILE_KIND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "wrong_profile_kind",
    "a Profile's kind cannot be changed after creation",
);
const STORAGE_REFUSED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "storage_refused",
    "storage refused to commit the Profile change",
);

pub(super) fn profile_id(text: &str) -> Result<ProfileId, Refusal> {
    crate::persistence::parse_entity_id(text)
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

fn workspace_access_str(access: WorkspaceAccess) -> &'static str {
    match access {
        WorkspaceAccess::None => "none",
        WorkspaceAccess::Metadata => "metadata",
        WorkspaceAccess::Full => "full",
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireParameters {
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
enum WireProfileRequest {
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
        parameters: WireParameters,
    },
    Agent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        behavior: Option<String>,
        #[serde(default)]
        tool_grant: Option<String>,
        #[serde(default)]
        llm_targets: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        dispatch_to: Option<Option<Vec<String>>>,
        #[serde(default, deserialize_with = "optional_value")]
        approve: Option<Option<Vec<String>>>,
        #[serde(default, deserialize_with = "optional_value")]
        default_model: Option<Option<String>>,
        /// An existing Decision Profile this Agent may ask for typed advice.
        /// `null` clears an assignment; absent leaves the previous one.
        #[serde(default, deserialize_with = "optional_value")]
        decision_assistance: Option<Option<String>>,
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
        limits: WireDecisionLimitInput,
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
        #[serde(default)]
        workspace_access: Option<String>,
    },
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct WireDecisionLimitInput {
    #[serde(default)]
    max_input_tokens_per_request: Option<u32>,
    #[serde(default)]
    max_output_tokens_per_request: Option<u32>,
    #[serde(default)]
    max_input_tokens_per_question: Option<u32>,
    #[serde(default)]
    max_output_tokens_per_question: Option<u32>,
}

fn optional_value<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
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
        .all_configs(Some(ConfigKind::Connection))
        .await?
        .iter()
        .any(|record| {
            matches!(
                &record.body,
                ConfigBody::Connection(connection)
                    if connection.credential_ref.as_deref() == Some(reference)
            )
        }))
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

pub(super) fn map_mutation_error(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Profile(_))) => PROFILE_NOT_FOUND,
        DatabaseError::Rejected(Violation::WrongConfigKind { .. }) => WRONG_PROFILE_KIND,
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

/// Ensures at least one Pool offers the requested protocol, so newly created
/// LLM Profiles can be selected and executed immediately.
async fn ensure_compatible_executor(
    database: &DatabaseHandle,
    protocol: LlmTransport,
) -> Result<(), Refusal> {
    let mut before = None;
    let has_compatible = loop {
        let page = database.executors(before, PROFILE_SCAN_LIMIT).await?;
        let found = page
            .iter()
            .any(|executor| executor.llm_transports.contains(&protocol));
        if found || page.len() < PROFILE_SCAN_LIMIT {
            break found;
        }
        before = page.last().map(|executor| executor.id);
    };
    if !has_compatible {
        let concurrency_limit = NonZeroU32::new(4).ok_or(INTERNAL)?;
        database
            .create_executor(NewExecutor {
                name: format!("local-{}-executor", protocol.as_str()),
                concurrency_limit,
                llm_transports: BTreeSet::from([protocol]),
            })
            .await
            .map_err(map_mutation_error)?;
    }
    Ok(())
}

/// Resolves or creates the Connection config matching the Profile-owned
/// protocol, endpoint and credential reference.
async fn resolve_or_create_connection(
    database: &DatabaseHandle,
    profile_name: &str,
    protocol: LlmTransport,
    endpoint: Option<String>,
    credential_ref: Option<String>,
) -> Result<ConfigId, Refusal> {
    let wanted = ConnectionConfig {
        protocol,
        endpoint,
        credential_ref,
    };
    let existing = database.all_configs(Some(ConfigKind::Connection)).await?;
    if let Some(found) = existing.into_iter().find(
        |connection| matches!(&connection.body, ConfigBody::Connection(body) if *body == wanted),
    ) {
        return Ok(found.id);
    }
    database
        .create_config(NewConfig {
            name: format!("{profile_name} provider"),
            body: ConfigBody::Connection(wanted),
        })
        .await
        .map_err(map_mutation_error)
}

/// The credential reference a Profile names, and the secret to write, if
/// any. An update keeps the reference it had when none is supplied, and an
/// explicit clear removes it.
async fn resolve_credential(
    database: &DatabaseHandle,
    profile_name: &str,
    credential_ref: Option<&str>,
    credential: Option<&str>,
    clear_credential: bool,
    prior_credential_ref: Option<String>,
) -> Result<(Option<String>, Option<(String, String)>), Refusal> {
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
        return Ok((None, None));
    }
    match secret {
        Some(secret_value) => {
            let target_ref = match explicit_ref.or(prior_credential_ref) {
                Some(reference) => reference,
                None => {
                    let base = default_credential_ref(profile_name);
                    let existing_connections =
                        database.all_configs(Some(ConfigKind::Connection)).await?;
                    let is_in_use = |reference: &str| {
                        existing_connections.iter().any(|record| {
                            if let ConfigBody::Connection(connection) = &record.body {
                                connection.credential_ref.as_deref() == Some(reference)
                            } else {
                                false
                            }
                        })
                    };
                    let mut candidate = base.clone();
                    if is_in_use(&candidate) {
                        let mut suffix = 2;
                        loop {
                            let suffixed = format!("{base}_{suffix}");
                            if !is_in_use(&suffixed) {
                                candidate = suffixed;
                                break;
                            }
                            suffix += 1;
                        }
                    }
                    candidate
                }
            };
            Ok((
                Some(target_ref.clone()),
                Some((target_ref, secret_value.to_owned())),
            ))
        }
        None => Ok((explicit_ref.or(prior_credential_ref), None)),
    }
}

/// The default Profile page size.
const DEFAULT_PROFILE_PAGE: usize = 50;
/// How many configs one page examines per Profile it may return.
const PROFILE_SCAN_FACTOR: usize = 4;
/// The serialized budget one Profile page may exceed only for a single entry.
const MAX_PROFILE_PAGE_BYTES: usize = 512 * 1024;

/// Reads one page of LLM and Agent Profiles, newest first, and the Provider
/// projection of the LLM Profiles on that page. `next_before` continues from
/// the last Profile returned, or from the last config examined when the scan
/// or byte budget ended the page first; `None` means the configs were
/// exhausted.
pub(super) async fn list_profiles(
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
    page: crate::storage::ProfilePageRecords,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let crate::storage::ProfilePageRecords {
        records,
        connections,
        next_before,
    } = page;
    let mut llm_profiles = Vec::new();
    let mut agent_profiles = Vec::new();
    let mut decision_profiles = Vec::new();
    let mut decision_agent_profiles = Vec::new();
    let mut executor_protocols = Vec::new();

    #[derive(Ord, PartialOrd, Eq, PartialEq)]
    struct ProviderKey {
        protocol: &'static str,
        endpoint: Option<String>,
        credential_ref: Option<String>,
    }
    struct ProviderGroup {
        has_credential: bool,
        profile_ids: Vec<String>,
        models: BTreeSet<String>,
    }
    let mut provider_groups: BTreeMap<ProviderKey, ProviderGroup> = BTreeMap::new();
    let mut last_emitted = None;
    let mut page_bytes = 0;
    let mut next_before = next_before;

    for record in records {
        let id = ProfileId::from(record.id);
        match record.body {
            ConfigBody::Model(model) => {
                let Some(connection) = connections.get(&model.connection_id).cloned() else {
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
                let profile = WireLlmProfile {
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
                    parameters: WireLlmParameters::from_parameters(&model.parameters),
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
                let key = ProviderKey {
                    protocol,
                    endpoint: connection.endpoint,
                    credential_ref: connection.credential_ref,
                };
                let group = provider_groups.entry(key).or_insert_with(|| ProviderGroup {
                    has_credential,
                    profile_ids: Vec::new(),
                    models: BTreeSet::new(),
                });
                group.profile_ids.push(id_str);
                group.models.insert(model.model);
                llm_profiles.push(profile);
                last_emitted = Some(record.id);
            }
            ConfigBody::Agent(agent) => {
                let profile = WireManagedAgent {
                    id: id.to_string(),
                    profile_type: Some("agent".to_owned()),
                    name: record.name,
                    instructions: agent.instructions,
                    tool_grant: agent.tool_grant,
                    approve: agent
                        .approve
                        .iter()
                        .map(|class| class.as_str().to_owned())
                        .collect(),
                    dispatch_to: agent
                        .dispatch_to
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    default_model: agent
                        .default_model
                        .map(|id| ProfileId::from(id).to_string()),
                    behavior: match agent.behavior {
                        AgentBehavior::Chat => "chat",
                        AgentBehavior::General => "general",
                    }
                    .to_owned(),
                    llm_targets: agent
                        .models
                        .iter()
                        .map(|id| ProfileId::from(*id).to_string())
                        .collect(),
                    decision_assistance: agent
                        .decision_assistance
                        .map(|id| ProfileId::from(id).to_string()),
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
                .map(|protocol| WireProfileExecutor {
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
                let Some(connection) = connections.get(&model.connection_id).cloned() else {
                    return Err(INCONSISTENT_RECORD);
                };
                let has_credential = connection
                    .credential_ref
                    .as_deref()
                    .is_some_and(|reference| database.has_credential(reference));
                let profile = WireDecisionProfile {
                    id: id.to_string(),
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
                    limits: WireDecisionLimits {
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
                let profile = WireDecisionAgentProfile {
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
                    workspace_access: workspace_access_str(agent.workspace_access).to_owned(),
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
        std::cmp::Reverse(profile_id(&profile.id).map_or(0, ProfileId::get))
    });
    agent_profiles.sort_by_key(|profile| {
        std::cmp::Reverse(profile_id(&profile.id).map_or(0, ProfileId::get))
    });

    let providers = provider_groups
        .into_iter()
        .map(|(key, group)| WireProvider {
            protocol: key.protocol.to_owned(),
            endpoint: key.endpoint,
            credential_ref: key.credential_ref,
            has_credential: group.has_credential,
            profile_ids: group.profile_ids,
            models: group.models.into_iter().collect(),
        })
        .collect::<Vec<_>>();

    Ok(envelope(WireProfiles {
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
    }))
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

pub(super) async fn save_profile(
    database: &DatabaseHandle,
    existing_id: Option<ProfileId>,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: WireProfileRequest =
        serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    let mut credential_to_save: Option<(String, String)> = None;
    let new_config = match request {
        WireProfileRequest::Llm {
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
            let keep_protocol = protocol.is_none();
            let mut protocol = protocol
                .as_deref()
                .map(str::trim)
                .map(LlmTransport::parse)
                .unwrap_or(Some(LlmTransport::OpenAiChatCompletions))
                .ok_or(UNSUPPORTED_PROTOCOL)?;
            let normalized_endpoint = match endpoint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => None,
                Some(url) => {
                    if url.len() > MAX_ENDPOINT_BYTES
                        || crate::provider_execution::model_endpoint(url).is_err()
                        || url.chars().any(|ch| ch.is_whitespace() || ch.is_control())
                    {
                        return Err(INVALID_ENDPOINT);
                    }
                    Some(url.to_owned())
                }
            };

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
                        crate::openai_compatible::request::parse_reasoning_effort(effort).is_err()
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

            // If updating an existing LLM profile, inspect its current
            // Connection so we can preserve its credential_ref when none is
            // supplied.
            let prior_credential_ref = if let Some(id) = existing_id {
                let Some(existing_record) = database.config(id.into()).await? else {
                    return Err(PROFILE_NOT_FOUND);
                };
                if existing_record.archived_at.is_some() {
                    return Err(PROFILE_NOT_FOUND);
                }
                let ConfigBody::Model(existing_model) = existing_record.body else {
                    return Err(WRONG_PROFILE_KIND);
                };
                let existing_connection = database
                    .config(existing_model.connection_id)
                    .await?
                    .ok_or(INCONSISTENT_RECORD)?;
                match existing_connection.body {
                    ConfigBody::Connection(connection) => {
                        if keep_protocol {
                            protocol = connection.protocol;
                        }
                        connection.credential_ref
                    }
                    _ => return Err(INCONSISTENT_RECORD),
                }
            } else {
                None
            };

            let explicit_ref = credential_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if let Some(ref reference) = explicit_ref
                && !valid_credential_ref(reference)
            {
                return Err(INVALID_CREDENTIAL_REF);
            }

            let secret = credential
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());

            if let Some(secret_value) = secret
                && (secret_value.len() > 4096
                    || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret_value}"))
                        .is_err())
            {
                return Err(INVALID_CREDENTIAL);
            }

            let resolved_credential_ref = if clear_credential {
                None
            } else if let Some(secret_value) = secret {
                let target_ref = match explicit_ref.or(prior_credential_ref) {
                    Some(reference) => reference,
                    None => {
                        let base = default_credential_ref(name.trim());
                        let existing_connections =
                            database.all_configs(Some(ConfigKind::Connection)).await?;
                        let is_in_use = |r: &str| {
                            existing_connections.iter().any(|record| {
                                if let ConfigBody::Connection(conn) = &record.body {
                                    conn.credential_ref.as_deref() == Some(r)
                                } else {
                                    false
                                }
                            })
                        };
                        let mut candidate = base.clone();
                        if is_in_use(&candidate) {
                            let mut suffix = 2;
                            loop {
                                let suffixed = format!("{base}_{suffix}");
                                if !is_in_use(&suffixed) {
                                    candidate = suffixed;
                                    break;
                                }
                                suffix += 1;
                            }
                        }
                        candidate
                    }
                };
                credential_to_save = Some((target_ref.clone(), secret_value.to_owned()));
                Some(target_ref)
            } else {
                explicit_ref.or(prior_credential_ref)
            };

            ensure_compatible_executor(database, protocol).await?;
            let connection = resolve_or_create_connection(
                database,
                name.trim(),
                protocol,
                normalized_endpoint,
                resolved_credential_ref,
            )
            .await?;

            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::Model(ModelConfig {
                    connection_id: connection,
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
                }),
            }
        }
        WireProfileRequest::Agent {
            name,
            instructions,
            behavior,
            tool_grant,
            llm_targets,
            dispatch_to,
            approve,
            default_model,
            decision_assistance,
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let normalized_instructions = match instructions
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => None,
                Some(text) => {
                    if !valid_multiline(text, MAX_INSTRUCTIONS_BYTES) {
                        return Err(INVALID_INSTRUCTIONS);
                    }
                    Some(text.to_owned())
                }
            };
            // An Agent that does not say otherwise can hold a conversation.
            let behavior = match behavior.as_deref().map(str::trim) {
                None | Some("chat") => AgentBehavior::Chat,
                Some("general") => AgentBehavior::General,
                Some(_) => return Err(INVALID_BEHAVIOR),
            };
            if !matches!(tool_grant.as_deref(), None | Some("read" | "edit" | "code")) {
                return Err(INVALID_TOOL_GRANT);
            }
            if llm_targets.is_empty() {
                return Err(MISSING_LLM_TARGETS);
            }
            let mut parsed_targets: Vec<ConfigId> = Vec::with_capacity(llm_targets.len());
            for raw_target in &llm_targets {
                let target_id = crate::persistence::parse_entity_id(raw_target.trim())
                    .map(ConfigId::new)
                    .ok_or(INVALID_LLM_TARGET)?;
                if parsed_targets.contains(&target_id) {
                    return Err(INVALID_LLM_TARGET);
                }
                match database.config(target_id).await? {
                    Some(ConfigRecord {
                        body: ConfigBody::Model(_),
                        ..
                    }) => parsed_targets.push(target_id),
                    _ => return Err(INVALID_LLM_TARGET),
                }
            }
            parsed_targets.sort();
            let prior = if let Some(id) = existing_id {
                let record = database.config(id.into()).await?.ok_or(PROFILE_NOT_FOUND)?;
                let ConfigBody::Agent(agent) = record.body else {
                    return Err(WRONG_PROFILE_KIND);
                };
                agent
            } else {
                AgentConfig::default()
            };
            let mut parsed_approve = match approve {
                None => prior.approve,
                Some(values) => {
                    let invalid = Refusal::new(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "invalid_approve",
                        "approve must contain unique commands, writes, dispatch or close",
                    );
                    let mut classes = Vec::new();
                    for value in values.unwrap_or_default() {
                        let class = match value.as_str() {
                            "commands" => crate::persistence::ApproveClass::Commands,
                            "writes" => crate::persistence::ApproveClass::Writes,
                            "dispatch" => crate::persistence::ApproveClass::Dispatch,
                            "close" => crate::persistence::ApproveClass::Close,
                            _ => return Err(invalid),
                        };
                        if classes.contains(&class) {
                            return Err(invalid);
                        }
                        classes.push(class);
                    }
                    classes
                }
            };
            parsed_approve.sort();
            let invalid_dispatch = Refusal::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_dispatch_to",
                "dispatch_to must name unique Agent Profiles",
            );
            let mut parsed_dispatch = match dispatch_to {
                None => prior.dispatch_to,
                Some(values) => {
                    let mut parsed = Vec::new();
                    for raw in values.unwrap_or_default() {
                        let id = profile_id(&raw)
                            .map(ConfigId::from)
                            .map_err(|_| invalid_dispatch)?;
                        if parsed.contains(&id)
                            || !matches!(
                                database.config(id).await?,
                                Some(ConfigRecord {
                                    body: ConfigBody::Agent(_),
                                    ..
                                })
                            )
                        {
                            return Err(invalid_dispatch);
                        }
                        parsed.push(id);
                    }
                    parsed
                }
            };
            parsed_dispatch.sort();
            let invalid_default = Refusal::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_default_model",
                "default_model must be one of llm_targets",
            );
            let default_model = match default_model {
                None => prior.default_model.filter(|id| parsed_targets.contains(id)),
                Some(value) => value
                    .map(|raw| {
                        profile_id(&raw)
                            .map(ConfigId::from)
                            .map_err(|_| invalid_default)
                    })
                    .transpose()?,
            };
            if default_model.is_some_and(|id| !parsed_targets.contains(&id)) {
                return Err(invalid_default);
            }
            let decision_assistance = match decision_assistance {
                None => prior.decision_assistance,
                Some(None) => None,
                Some(Some(raw)) => {
                    let id = profile_id(&raw)
                        .map(ConfigId::from)
                        .map_err(|_| INVALID_DECISION_ASSISTANCE)?;
                    match database.config(id).await? {
                        Some(ConfigRecord {
                            body: ConfigBody::DecisionAgent(_),
                            archived_at: None,
                            ..
                        }) => Some(id),
                        _ => return Err(INVALID_DECISION_ASSISTANCE),
                    }
                }
            };
            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::Agent(AgentConfig {
                    approve: parsed_approve,
                    instructions: normalized_instructions,
                    behavior,
                    models: parsed_targets,
                    default_model,
                    dispatch_to: parsed_dispatch,
                    tool_grant,
                    decision_assistance,
                    compaction_policy: prior.compaction_policy,
                }),
            }
        }
        WireProfileRequest::DecisionModel {
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
            let normalized_endpoint = match endpoint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => None,
                Some(url) => {
                    if url.len() > MAX_ENDPOINT_BYTES
                        || crate::provider_execution::model_endpoint(url).is_err()
                        || url.chars().any(|ch| ch.is_whitespace() || ch.is_control())
                    {
                        return Err(INVALID_ENDPOINT);
                    }
                    Some(url.to_owned())
                }
            };
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
            let prior_credential_ref = if let Some(id) = existing_id {
                let Some(existing_record) = database.config(id.into()).await? else {
                    return Err(PROFILE_NOT_FOUND);
                };
                if existing_record.archived_at.is_some() {
                    return Err(PROFILE_NOT_FOUND);
                }
                let ConfigBody::DecisionModel(existing_model) = existing_record.body else {
                    return Err(WRONG_PROFILE_KIND);
                };
                let existing_connection = database
                    .config(existing_model.connection_id)
                    .await?
                    .ok_or(INCONSISTENT_RECORD)?;
                match existing_connection.body {
                    ConfigBody::Connection(connection) => connection.credential_ref,
                    _ => return Err(INCONSISTENT_RECORD),
                }
            } else {
                None
            };
            let (resolved_credential_ref, secret) = resolve_credential(
                database,
                name.trim(),
                credential_ref.as_deref(),
                credential.as_deref(),
                clear_credential,
                prior_credential_ref,
            )
            .await?;
            credential_to_save = secret;
            // The Connection carries the endpoint and the credential. Its
            // transport records how the endpoint would be spoken generatively
            // and is never used for decision work: the decision protocol
            // belongs to this Profile.
            let connection = resolve_or_create_connection(
                database,
                &format!("{} decision provider", name.trim()),
                LlmTransport::OpenAiChatCompletions,
                normalized_endpoint,
                resolved_credential_ref,
            )
            .await?;
            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::DecisionModel(DecisionModelConfig {
                    connection_id: connection,
                    model: model.trim().to_owned(),
                    protocol: decision_protocol,
                    limits: decision_limits,
                    declared_capabilities: capabilities,
                }),
            }
        }
        WireProfileRequest::DecisionAgent {
            name,
            instructions,
            models,
            default_model,
            workspace_access,
        } => {
            if !valid_single_line(&name, MAX_PROFILE_NAME_BYTES) {
                return Err(INVALID_PROFILE_NAME);
            }
            let normalized_instructions = match instructions
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => None,
                Some(text) => {
                    if !valid_multiline(text, MAX_INSTRUCTIONS_BYTES) {
                        return Err(INVALID_INSTRUCTIONS);
                    }
                    Some(text.to_owned())
                }
            };
            if models.is_empty() {
                return Err(MISSING_DECISION_MODELS);
            }
            let mut parsed_models: Vec<ConfigId> = Vec::with_capacity(models.len());
            for raw_model in &models {
                let model_id = crate::persistence::parse_entity_id(raw_model.trim())
                    .map(ConfigId::new)
                    .ok_or(INVALID_DECISION_MODELS)?;
                if parsed_models.contains(&model_id) {
                    return Err(INVALID_DECISION_MODELS);
                }
                match database.config(model_id).await? {
                    Some(ConfigRecord {
                        body: ConfigBody::DecisionModel(_),
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
                            .map_err(|_| invalid_default)
                    })
                    .transpose()?,
            };
            if default_model.is_some_and(|id| !parsed_models.contains(&id)) {
                return Err(invalid_default);
            }
            // Workspace reads are not implemented. A Decision Profile that
            // names metadata or full access would advertise a permission it
            // cannot use, so only none is accepted.
            let access = match workspace_access.as_deref().map(str::trim) {
                None | Some("none") => WorkspaceAccess::None,
                Some(_) => return Err(INVALID_WORKSPACE_ACCESS),
            };
            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::DecisionAgent(DecisionAgentConfig {
                    instructions: normalized_instructions,
                    models: parsed_models,
                    default_model,
                    workspace_access: access,
                }),
            }
        }
    };

    let (status, profile_id) = match existing_id {
        None => {
            let created = database
                .create_config(new_config)
                .await
                .map_err(map_mutation_error)?;
            (StatusCode::CREATED, ProfileId::from(created))
        }
        Some(id) => {
            database
                .update_config(id.into(), new_config)
                .await
                .map_err(map_mutation_error)?;
            (StatusCode::OK, id)
        }
    };
    if let Some((target_ref, secret_value)) = credential_to_save {
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportModelsRequest {
    source_profile_id: Option<String>,
    provider: Option<ImportProvider>,
    models: Vec<ImportModel>,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportModel {
    id: String,
    context_window: Option<u32>,
}

pub(super) async fn import_models(
    database: &DatabaseHandle,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let request: ImportModelsRequest =
        serde_json::from_slice(body).map_err(|_| INVALID_PROFILE_BODY)?;
    if request.models.is_empty() || request.models.len() > 200 {
        return Err(INVALID_MODEL);
    }
    let mut models = BTreeMap::new();
    for model in request.models {
        if !valid_single_line(&model.id, MAX_MODEL_BYTES) || model.context_window == Some(0) {
            return Err(INVALID_MODEL);
        }
        models
            .entry(model.id.trim().to_owned())
            .or_insert(model.context_window);
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
            NewConfig {
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
                || crate::provider_execution::model_endpoint(endpoint).is_err()
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
                        None => {
                            let base = default_credential_ref(provider.name.trim());
                            let connections =
                                database.all_configs(Some(ConfigKind::Connection)).await?;
                            let mut candidate = base.clone();
                            let mut suffix = 2;
                            while connections.iter().any(|record| matches!(&record.body, ConfigBody::Connection(body) if body.credential_ref.as_deref() == Some(candidate.as_str()))) {
                                candidate = format!("{base}_{suffix}"); suffix += 1;
                            }
                            candidate
                        }
                    };
                    credential_to_save = Some((reference.clone(), secret));
                    Some(reference)
                }
                None => reference,
            };
            NewConfig {
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
    let ConfigBody::Connection(ref connection_body) = connection.body else {
        return Err(WRONG_PROFILE_KIND);
    };
    ensure_compatible_executor(database, connection_body.protocol).await?;
    let (ids, created) = database
        .import_models(connection, models.into_iter().collect(), capabilities)
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
        envelope(WireImportedModels {
            profile_ids: ids
                .into_iter()
                .map(|id| ProfileId::from(id).to_string())
                .collect(),
            created,
            existing,
        }),
    ))
}

pub(super) async fn delete_model(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{ExecutionConfiguration, JobInputDraft, NewJob};
    use crate::test_support::data_directory;

    #[test]
    fn model_import_is_atomic_and_repeated_import_preserves_edits() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = data_directory("profiles", "model-import");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let database = worker.handle();
            let connection = NewConfig {
                name: "provider".into(),
                body: ConfigBody::Connection(ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: Some("https://example.com/v1".into()),
                    credential_ref: None,
                }),
            };
            let capabilities = BTreeSet::from([ModelCapability::Chat]);
            assert!(
                database
                    .import_models(
                        connection.clone(),
                        vec![("valid".into(), None), ("invalid".into(), Some(0))],
                        capabilities.clone()
                    )
                    .await
                    .is_err()
            );
            assert!(database.all_configs(None).await.unwrap().is_empty());
            let (ids, created) = database
                .import_models(
                    connection.clone(),
                    vec![("a".into(), Some(128000)), ("b".into(), None)],
                    capabilities.clone(),
                )
                .await
                .unwrap();
            assert_eq!(created, 2);
            let original = database.config(ids[0]).await.unwrap().unwrap();
            let mut edited = original.body;
            if let ConfigBody::Model(model) = &mut edited {
                model.parameters.temperature = Some(0.3);
            }
            database
                .update_config(
                    ids[0],
                    NewConfig {
                        name: "My model".into(),
                        body: edited,
                    },
                )
                .await
                .unwrap();
            let (again, created) = database
                .import_models(
                    connection,
                    vec![("a".into(), Some(42)), ("b".into(), None)],
                    BTreeSet::from([ModelCapability::Tools]),
                )
                .await
                .unwrap();
            assert_eq!(again, ids);
            assert_eq!(created, 0);
            assert_eq!(
                database
                    .all_configs(Some(ConfigKind::Connection))
                    .await
                    .unwrap()
                    .len(),
                1
            );
            let preserved = database.config(ids[0]).await.unwrap().unwrap();
            assert_eq!(preserved.name, "My model");
            let ConfigBody::Model(model) = preserved.body else {
                panic!()
            };
            assert_eq!(model.context_window, Some(128000));
            assert_eq!(model.parameters.temperature, Some(0.3));
            assert_eq!(model.declared_capabilities, capabilities);
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn deleting_a_model_removes_agent_choices_but_keeps_frozen_history() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = data_directory("profiles", "delete-model");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let saved = save_profile(&db, None, br#"{"kind":"llm","name":"old","model":"original","declared_capabilities":["chat"]}"#).await.unwrap();
            let model = profile_id(saved.body["profile_id"].as_str().unwrap()).unwrap();
            let agent = save_profile(&db, None, &serde_json::to_vec(&json!({"kind":"agent","name":"agent","llm_targets":[model.to_string()],"default_model":model.to_string()})).unwrap()).await.unwrap();
            let agent = profile_id(agent.body["profile_id"].as_str().unwrap()).unwrap();
            let job = db
                .create_job(NewJob {
                    process_grant: None,
                    name: None,
                    eligible_at: None,
                    parent: db.genesis(),
                    project: None,
                    input: JobInputDraft {
                        objective: Some("history".into()),
                        ..Default::default()
                    },
                    configuration: Some(ExecutionConfiguration {
                        agent_profile: Some(agent),
                        llm_profile: model,
                    }),
                    previous: None,
                })
                .await
                .unwrap();
            delete_model(&db, model).await.unwrap();
            delete_model(&db, model).await.unwrap();
            assert!(
                db.config(model.into())
                    .await
                    .unwrap()
                    .unwrap()
                    .archived_at
                    .is_some()
            );
            let ConfigBody::Agent(body) = db.config(agent.into()).await.unwrap().unwrap().body
            else {
                panic!()
            };
            assert!(body.models.is_empty());
            assert_eq!(body.default_model, None);
            assert_eq!(
                db.job_configuration(job)
                    .await
                    .unwrap()
                    .unwrap()
                    .frozen
                    .llm
                    .config
                    .model,
                "original"
            );
            let response = list_profiles(&db, None, 50).await.unwrap();
            assert!(response.body["llm_profiles"].as_array().unwrap().is_empty());
            let invalid = save_profile(
                &db,
                None,
                &serde_json::to_vec(
                    &json!({"kind":"agent","name":"new","llm_targets":[model.to_string()]}),
                )
                .unwrap(),
            )
            .await;
            assert!(invalid.is_err());
            assert!(delete_model(&db, agent).await.is_err());
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn new_provider_import_saves_credentials_without_returning_them() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = data_directory("profiles", "import-new-provider");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let body = json!({"provider":{"name":"New","endpoint":"https://example.com/v1","credential":"test-only-secret","credential_ref":"IMPORT_TEST_KEY"},"models":[{"id":"a"},{"id":"b"}],"declared_capabilities":["chat"]});
            let reply = import_models(&db, &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            assert_eq!(reply.body["created"], 2);
            assert!(!reply.body.to_string().contains("test-only-secret"));
            assert_eq!(
                db.credential_resolver()
                    .resolve(Some("IMPORT_TEST_KEY"))
                    .unwrap()
                    .as_deref(),
                Some("test-only-secret")
            );
            assert_eq!(
                db.all_configs(Some(ConfigKind::Connection))
                    .await
                    .unwrap()
                    .len(),
                1
            );
            let again = import_models(&db, &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            assert_eq!(again.body["created"], 0);
            assert_eq!(again.body["existing"], 2);
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn import_models_validates_and_deduplicates_the_selected_models() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = data_directory("profiles", "import-api");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let saved = save_profile(
                &db,
                None,
                br#"{"kind":"llm","name":"provider","model":"a","declared_capabilities":["chat"]}"#,
            )
            .await
            .unwrap();
            let source = saved.body["profile_id"].as_str().unwrap();
            let mut body = json!({"source_profile_id":source,"models":[{"id":"a"},{"id":"b","context_window":65536},{"id":"b"}],"declared_capabilities":["chat"]});
            let reply = import_models(&db, &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            assert_eq!(reply.body["api_version"], super::super::API_VERSION);
            assert_eq!(reply.body["created"], 1);
            assert_eq!(reply.body["existing"], 1);
            body["models"] = json!([{"id":"c","context_window":0}]);
            assert!(
                import_models(&db, &serde_json::to_vec(&body).unwrap())
                    .await
                    .is_err()
            );
            assert_eq!(
                db.all_configs(Some(ConfigKind::Model)).await.unwrap().len(),
                2
            );
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn agent_dispatch_and_default_model_round_trip_and_reject_a_model_target() {
        ntex::rt::System::new("dispatch-profiles", ntex::rt::DefaultRuntime).block_on(async {
            let (_root, directory) = data_directory("profiles", "dispatch");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let database = worker.handle();
            let model = save_profile(&database, None, &serde_json::to_vec(&json!({"kind":"llm","name":"model","model":"m","declared_capabilities":["chat","tools"]})).unwrap()).await.unwrap();
            let model = model.body["profile_id"].as_str().unwrap();
            let target = save_profile(&database, None, &serde_json::to_vec(&json!({"kind":"agent","name":"target","llm_targets":[model],"default_model":model})).unwrap()).await.unwrap();
            let target = target.body["profile_id"].as_str().unwrap();
            let mut body = json!({"kind":"agent","name":"dispatcher","llm_targets":[model],"dispatch_to":[target],"default_model":model});
            let reply = save_profile(&database, None, &serde_json::to_vec(&body).unwrap()).await.unwrap();
            let agent = reply.body["agent_profiles"].as_array().unwrap().iter().find(|agent| agent["id"] == reply.body["profile_id"]).unwrap();
            assert_eq!(agent["dispatch_to"], json!([target]));
            assert_eq!(agent["default_model"], model);
            let id = profile_id(reply.body["profile_id"].as_str().unwrap()).unwrap();
            let renamed = json!({"kind":"agent","name":"renamed","llm_targets":[model]});
            let updated = save_profile(&database, Some(id), &serde_json::to_vec(&renamed).unwrap()).await.unwrap();
            let kept = updated.body["agent_profiles"].as_array().unwrap().iter().find(|agent| agent["id"] == reply.body["profile_id"]).unwrap();
            assert_eq!(kept["dispatch_to"], json!([target]));
            assert_eq!(kept["default_model"], model);
            let approved = json!({"kind":"agent","name":"renamed","llm_targets":[model],"approve":["commands"]});
            let updated = save_profile(&database, Some(id), &serde_json::to_vec(&approved).unwrap()).await.unwrap();
            let stored = updated.body["agent_profiles"].as_array().unwrap().iter().find(|agent| agent["id"] == reply.body["profile_id"]).unwrap();
            assert_eq!(stored["approve"], json!(["commands"]));
            let cleared = json!({"kind":"agent","name":"renamed","llm_targets":[model],"dispatch_to":null,"approve":null});
            let updated = save_profile(&database, Some(id), &serde_json::to_vec(&cleared).unwrap()).await.unwrap();
            let cleared = updated.body["agent_profiles"].as_array().unwrap().iter().find(|agent| agent["id"] == reply.body["profile_id"]).unwrap();
            assert_eq!(cleared["dispatch_to"], json!([]));
            assert_eq!(cleared["approve"], json!([]));
            body["dispatch_to"] = json!([model]);
            assert!(matches!(save_profile(&database, None, &serde_json::to_vec(&body).unwrap()).await, Err(refusal) if refusal.code == "invalid_dispatch_to"));
            body["dispatch_to"] = json!([target]);
            body["default_model"] = json!(target);
            assert!(matches!(save_profile(&database, None, &serde_json::to_vec(&body).unwrap()).await, Err(refusal) if refusal.code == "invalid_default_model"));
            let model_id: ConfigId = profile_id(model).unwrap().into();
            assert!(matches!(database.create_config(NewConfig { name: "invalid dispatcher".into(), body: ConfigBody::Agent(AgentConfig { dispatch_to: vec![model_id], ..Default::default() }) }).await, Err(DatabaseError::Rejected(Violation::WrongConfigKind { expected: ConfigKind::Agent, .. }))));
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn profile_crud_computes_providers_redacts_secrets_and_edits_leave_frozen_jobs_alone() {
        ntex::rt::System::new("profiles-test", ntex::rt::DefaultRuntime).block_on(async {
            let (_root, directory) = data_directory("profiles", "crud");
            let worker = DatabaseWorker::start(&directory).expect("start database");
            let handle = worker.handle();

            let secret = "sk-test-super-secret-value-987654321";
            let create_llm = json!({
                "kind": "llm",
                "name": "Local GPT-4o",
                "model": "gpt-4o",
                "protocol": "openai_chat_completions",
                "endpoint": "https://api.example.com/v1/chat/completions",
                "credential_ref": "LOCAL_OPENAI_KEY",
                "credential": secret,
                "declared_capabilities": ["chat", "vision"],
                "parameters": {
                    "temperature": 0.5,
                    "max_tokens": 2048
                }
            });
            let reply = save_profile(&handle, None, &serde_json::to_vec(&create_llm).unwrap())
                .await
                .expect("create llm profile");
            assert_eq!(reply.status, StatusCode::CREATED);
            let serialized = reply.body.to_string();
            assert!(
                !serialized.contains(secret),
                "raw secret must never be returned"
            );

            let llm_id_str = reply.body["profile_id"]
                .as_str()
                .expect("profile_id")
                .to_owned();
            let llm_id = profile_id(&llm_id_str).expect("valid profile id");
            assert_eq!(reply.body["llm_profiles"][0]["has_credential"], true);
            assert_eq!(reply.body["llm_profiles"][0]["editable"], true);
            assert_eq!(reply.body["providers"].as_array().unwrap().len(), 1);
            assert_eq!(
                reply.body["providers"][0]["endpoint"],
                "https://api.example.com/v1/chat/completions"
            );
            assert_eq!(reply.body["providers"][0]["models"], json!(["gpt-4o"]));

            // Verify credential resolver resolves the saved secret.
            let resolved = handle
                .credential_resolver()
                .resolve(Some("LOCAL_OPENAI_KEY"))
                .expect("resolve credential");
            assert_eq!(resolved.as_deref(), Some(secret));

            // Create Agent Profile targeting the LLM Profile.
            let create_agent = json!({
                "kind": "agent",
                "name": "Vision Assistant",
                "instructions": "Describe images accurately.",
                "llm_targets": [llm_id_str]
            });
            let agent_reply =
                save_profile(&handle, None, &serde_json::to_vec(&create_agent).unwrap())
                    .await
                    .expect("create agent profile");
            assert_eq!(agent_reply.status, StatusCode::CREATED);
            let agent_id_str = agent_reply.body["profile_id"]
                .as_str()
                .expect("agent profile_id")
                .to_owned();
            let agent_id = profile_id(&agent_id_str).expect("valid agent id");

            // A Job submitted with both Profiles freezes what they hold.
            let job = handle
                .create_job(NewJob {
                    process_grant: None,
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    project: None,
                    input: JobInputDraft {
                        objective: Some("Hold the old model".to_owned()),
                        ..JobInputDraft::default()
                    },
                    configuration: Some(ExecutionConfiguration {
                        agent_profile: Some(agent_id),
                        llm_profile: llm_id,
                    }),
                    previous: None,
                })
                .await
                .expect("create the Job");

            // Nothing locks a Profile: the edit is made in place.
            let mut edited = create_llm.clone();
            edited["model"] = json!("gpt-4o-mini");
            let edited_reply =
                save_profile(&handle, Some(llm_id), &serde_json::to_vec(&edited).unwrap())
                    .await
                    .expect("an edit is always allowed");
            assert_eq!(edited_reply.status, StatusCode::OK);
            assert_eq!(edited_reply.body["llm_profiles"][0]["model"], "gpt-4o-mini");
            assert_eq!(edited_reply.body["llm_profiles"][0]["editable"], true);
            let config = handle.config(llm_id.into()).await.unwrap().unwrap();
            assert_eq!(config.edit_seq, 2);

            // The Job keeps the values it froze.
            let frozen = handle.job_configuration(job).await.unwrap().unwrap();
            assert_eq!(frozen.frozen.llm.config.model, "gpt-4o");
            assert_eq!(frozen.frozen.llm.model.edit_seq, 1);

            worker.shutdown().expect("shutdown");
        });
    }
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
enum DiscoveryProtocol {
    Generative(LlmTransport),
    Decision(DecisionProtocol),
}

pub(super) async fn provider_models(
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
        || crate::provider_execution::model_endpoint(&request.endpoint).is_err()
    {
        return Err(INVALID_ENDPOINT);
    }
    if request.credential.is_some() && request.credential_ref.is_some() {
        return Err(INVALID_PROFILE_BODY);
    }
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
    use crate::provider_execution::ModelListError;
    let listed = match protocol {
        DiscoveryProtocol::Generative(protocol) => {
            crate::provider_execution::list_models(
                &request.endpoint,
                credential.as_deref(),
                protocol,
            )
            .await
        }
        DiscoveryProtocol::Decision(protocol) => {
            debug_assert_eq!(protocol, DecisionProtocol::TypeSafeSystemOne);
            crate::provider_execution::list_decision_models(
                &request.endpoint,
                credential.as_deref(),
            )
            .await
        }
    };
    let (code, message, status_code) = match listed {
        Ok(models) => {
            return Ok(Reply::ok(envelope(WireProviderModels {
                models: models
                    .into_iter()
                    .map(|model| WireProviderModel {
                        id: model.id,
                        context_window: model.context_window,
                    })
                    .collect(),
            })));
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
        envelope(WireProviderErrorResponse {
            error: WireProviderError {
                code: code.to_owned(),
                message,
                status_code,
            },
        }),
    ))
}

#[cfg(test)]
mod provider_network_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::provider_execution::scripted;
    use crate::test_support::{data_directory, on_system, wait};

    #[test]
    fn provider_network_http_is_local_only_at_save_and_send_without_credentials() {
        let (_root, directory) = data_directory("provider-network", "local");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        on_system(move || async move {
            for (host, allowed) in [
                ("localhost", true),
                ("127.0.0.1", true),
                ("[::1]", true),
                ("example.com", false),
                ("localhost.example.com", false),
                ("127.0.0.2", false),
                ("[::2]", false),
                ("localhost@example.com", false),
            ] {
                let endpoint = format!("http://{host}:1234/v1");
                assert_eq!(
                    crate::provider_execution::model_endpoint(&endpoint).is_ok(),
                    allowed,
                    "{endpoint}"
                );
                let body = serde_json::to_vec(&json!({
                    "kind":"llm", "name":format!("local {host}"), "model":"local-model",
                    "endpoint":endpoint, "declared_capabilities":["chat"], "parameters":{},
                }))
                .expect("JSON");
                let saved = save_profile(&database, None, &body).await;
                if allowed {
                    saved.expect("save local profile");
                    let models_endpoint = crate::provider_execution::model_endpoint(&endpoint)
                        .expect("local endpoint");
                    scripted::install_http(&models_endpoint, |authorization| {
                        assert!(authorization.is_none());
                        Ok((200, br#"{"data":[{"id":"local-model"}]}"#.to_vec()))
                    });
                    let reply = provider_models(
                        &database,
                        &serde_json::to_vec(&json!({"endpoint":endpoint})).expect("JSON"),
                    )
                    .await
                    .expect("local models");
                    assert_eq!(reply.body["models"][0]["id"], "local-model");
                } else {
                    assert!(saved.is_err(), "remote HTTP refused at save");
                }
            }
            assert!(crate::provider_execution::model_endpoint("https://example.com/v1").is_ok());
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn provider_network_models_saved_credentials_refusals_and_redaction() {
        let (_root, directory) = data_directory("provider-network", "models");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        wait(database.set_credential("MODEL_TEST_KEY", "saved-secret")).expect("save credential");
        on_system(move || async move {
            database
                .create_config(NewConfig {
                    name: "models provider".to_owned(),
                    body: ConfigBody::Connection(ConnectionConfig {
                        protocol: LlmTransport::OpenAiChatCompletions,
                        endpoint: None,
                        credential_ref: Some("MODEL_TEST_KEY".to_owned()),
                    }),
                })
                .await
                .expect("saved Connection");
            scripted::install_http("https://models.example/v1/models", |authorization| {
                assert_eq!(
                    authorization.expect("authorization").as_bytes(),
                    b"Bearer saved-secret"
                );
                Ok((200, br#"{"data":[{"id":"z"},{"id":"a","context_length":8192},{"id":"a","context_window":16384},{"id":"b","max_context_length":4096}]}"#.to_vec()))
            });
            let request = serde_json::to_vec(&json!({"endpoint":"https://models.example/v1/chat/completions/", "credential_ref":"MODEL_TEST_KEY"})).expect("JSON");
            let reply = provider_models(&database, &request).await.expect("models");
            assert_eq!(
                reply.body,
                json!({"api_version": super::super::API_VERSION, "models":[{"id":"a","context_window":16384},{"id":"b","context_window":4096},{"id":"z","context_window":null}]})
            );
            for (status, body, code) in [
                (
                    401,
                    r#"{"error":{"message":"bad key saved-secret"}}"#,
                    "provider_rejected",
                ),
                (404, "missing", "models_not_listed"),
                (200, r#"{"message":"not a list"}"#, "models_not_listed"),
            ] {
                scripted::install_http("https://models.example/v1/models", move |_| {
                    Ok((status, body.as_bytes().to_vec()))
                });
                let reply = provider_models(&database, &request).await.expect("refusal");
                assert_eq!(reply.body["error"]["code"], code);
                if status == 401 {
                    assert_eq!(reply.body["error"]["status_code"], 401);
                    assert!(
                        reply.body["error"]["message"]
                            .as_str()
                            .expect("message")
                            .contains("bad key")
                    );
                }
                assert!(!reply.body.to_string().contains("saved-secret"));
            }
            scripted::install_http("https://models.example/v1/models", |_| Err(()));
            assert_eq!(
                provider_models(&database, &request)
                    .await
                    .expect("unreachable")
                    .body["error"]["code"],
                "provider_unreachable"
            );
            scripted::install_http("https://models.example/v1/models", |_| {
                Ok((200, vec![b' '; 1024 * 1024 + 1]))
            });
            assert_eq!(
                provider_models(&database, &request)
                    .await
                    .expect("bounded")
                    .body["error"]["code"],
                "models_not_listed"
            );
            scripted::install_http("https://models.example/v1/models", |authorization| {
                assert_eq!(
                    authorization.expect("raw credential").as_bytes(),
                    b"Bearer raw-secret"
                );
                Ok((
                    200,
                    br#"{"data":[{"id":"model"},{"id":"raw-secret"}]}"#.to_vec(),
                ))
            });
            let raw = provider_models(
                &database,
                br#"{"endpoint":"https://models.example/v1", "credential":"raw-secret"}"#,
            )
            .await
            .expect("raw credential");
            assert!(!raw.body.to_string().contains("raw-secret"));
            assert!(!database.has_credential("raw-secret"));
            assert!(
                provider_models(&database, br#"{"endpoint":"http://models.example"}"#)
                    .await
                    .is_err()
            );
            assert!(provider_models(&database, br#"{"endpoint":"https://models.example","credential":"raw-secret","credential_ref":"MODEL_TEST_KEY"}"#).await.is_err());
            assert_eq!(
                database
                    .credential_resolver()
                    .resolve(Some("MODEL_TEST_KEY"))
                    .expect("saved credential")
                    .as_deref(),
                Some("saved-secret")
            );
            assert!(
                super::super::routing::authorize(
                    &super::super::security::Caller::Anonymous,
                    super::super::routing::Route::ProviderModels
                )
                .is_err()
            );
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn provider_network_models_credential_ref_must_name_a_saved_connection() {
        let (_root, directory) = data_directory("provider-network", "models-credential-ref");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        wait(database.set_credential("SAVED_REF", "saved-secret")).expect("save credential");
        on_system(move || async move {
            scripted::install_http("https://credential-ref.example/v1/models", |_| {
                panic!("a credential_ref that names no saved Connection is never sent")
            });
            for reference in ["not valid", "UNSAVED_REF"] {
                let request = serde_json::to_vec(&json!({
                    "endpoint": "https://credential-ref.example/v1",
                    "credential_ref": reference,
                }))
                .expect("JSON");
                let refusal = match provider_models(&database, &request).await {
                    Err(refusal) => refusal,
                    Ok(_) => panic!("refused before any resolution"),
                };
                assert_eq!(refusal.code, "invalid_credential_ref", "{reference}");
            }
            database
                .create_config(NewConfig {
                    name: "saved provider".to_owned(),
                    body: ConfigBody::Connection(ConnectionConfig {
                        protocol: LlmTransport::OpenAiChatCompletions,
                        endpoint: None,
                        credential_ref: Some("SAVED_REF".to_owned()),
                    }),
                })
                .await
                .expect("saved Connection");
            scripted::install_http(
                "https://credential-ref.example/v1/models",
                |authorization| {
                    assert_eq!(
                        authorization.expect("authorization").as_bytes(),
                        b"Bearer saved-secret"
                    );
                    Ok((200, br#"{"data":[{"id":"m"}]}"#.to_vec()))
                },
            );
            let request = serde_json::to_vec(&json!({
                "endpoint": "https://credential-ref.example/v1",
                "credential_ref": "SAVED_REF",
            }))
            .expect("JSON");
            let reply = provider_models(&database, &request).await.expect("models");
            assert_eq!(reply.body["models"][0]["id"], "m");
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn provider_network_models_caps_the_provider_rejection_message() {
        let (_root, directory) = data_directory("provider-network", "models-message-cap");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        on_system(move || async move {
            let body = serde_json::to_vec(&json!({"error": {"message": "x".repeat(4 * 1024)}}))
                .expect("JSON");
            scripted::install_http("https://cap.example/v1/models", move |_| {
                Ok((401, body.clone()))
            });
            let reply = provider_models(&database, br#"{"endpoint":"https://cap.example/v1"}"#)
                .await
                .expect("refusal");
            assert_eq!(reply.body["error"]["code"], "provider_rejected");
            assert_eq!(reply.body["error"]["status_code"], 401);
            let echoed = reply.body["error"]["message"].as_str().expect("message");
            assert_eq!(echoed.len(), 1024);
            assert!(echoed.bytes().all(|byte| byte == b'x'));
        });
        worker.shutdown().expect("shutdown");
    }
}

#[cfg(test)]
mod reasoning_parameter_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{ExecutionConfiguration, JobInputDraft, NewJob};

    #[test]
    fn reasoning_parameters_validate_freeze_and_round_trip() {
        crate::test_support::wait(async {
            let (_root, directory) =
                crate::test_support::data_directory("profiles", "reasoning-parameters");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let mut body = json!({"kind":"llm", "name":"reasoner", "model":"reasoner",
                "declared_capabilities":["chat","reasoning"],
                "parameters":{"reasoning_effort":"high", "max_completion_tokens":8192}});
            let saved = save_profile(&handle, None, &serde_json::to_vec(&body).expect("JSON"))
                .await
                .expect("save");
            let profile =
                profile_id(saved.body["profile_id"].as_str().expect("id")).expect("profile");
            let job = handle
                .create_job(NewJob {
                    process_grant: None,
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    project: None,
                    input: JobInputDraft::default(),
                    previous: None,
                    configuration: Some(ExecutionConfiguration {
                        agent_profile: None,
                        llm_profile: profile,
                    }),
                })
                .await
                .expect("freeze");
            for invalid in [
                json!({"reasoning_effort":"HIGH"}),
                json!({"reasoning_effort":"unknown"}),
                json!({"max_tokens":1,"max_completion_tokens":2}),
                json!({"max_completion_tokens":0}),
            ] {
                body["parameters"] = invalid;
                let refused = save_profile(
                    &handle,
                    Some(profile),
                    &serde_json::to_vec(&body).expect("JSON"),
                )
                .await;
                assert!(matches!(refused, Err(refusal) if refusal.code == "invalid_parameters"));
            }
            for effort in ["none", "minimal", "low", "medium", "high", "xhigh"] {
                body["parameters"] = json!({"reasoning_effort":effort,"max_tokens":128});
                save_profile(
                    &handle,
                    Some(profile),
                    &serde_json::to_vec(&body).expect("JSON"),
                )
                .await
                .expect("wire accepts effort");
            }
            worker.shutdown().expect("close");
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let handle = worker.handle();
            let frozen = handle
                .job_configuration(job)
                .await
                .expect("read")
                .expect("frozen")
                .frozen;
            assert_eq!(
                frozen.llm.config.parameters.reasoning_effort.as_deref(),
                Some("high")
            );
            assert_eq!(
                frozen.llm.config.parameters.max_completion_tokens,
                Some(8192)
            );
            let conversation =
                crate::provider_execution::objective_conversation(&frozen, Some("hi"));
            let request = crate::provider_execution::request_body(
                &frozen.llm.config.model,
                &frozen.llm.config.parameters,
                &conversation,
            )
            .expect("request");
            assert_eq!(request["reasoning_effort"], "high");
            assert_eq!(request["max_completion_tokens"], 8192);
            assert!(request.get("max_tokens").is_none());
            use crate::orchestration::execution_runtime::{ContextLimits, fit_conversation};
            let mut limits = ContextLimits::for_profile(&frozen.llm.config);
            assert_eq!(limits.max_tokens, Some(8192));
            limits.context_window = Some(8193);
            assert!(
                fit_conversation(
                    &frozen,
                    vec![],
                    Some("hello"),
                    &[],
                    None,
                    None,
                    None,
                    vec![],
                    limits
                )
                .is_none()
            );
            limits.context_window = Some(8194);
            assert!(
                fit_conversation(
                    &frozen,
                    vec![],
                    Some("hello"),
                    &[],
                    None,
                    None,
                    None,
                    vec![],
                    limits
                )
                .is_some()
            );
            let listed = list_profiles(&handle, None, 50)
                .await
                .expect("GET profiles");
            assert_eq!(
                listed.body["llm_profiles"][0]["parameters"]["reasoning_effort"],
                "xhigh"
            );
            assert_eq!(
                listed.body["llm_profiles"][0]["parameters"]["max_tokens"],
                128
            );
            worker.shutdown().expect("close");
        });
    }

    #[test]
    fn profiles_page_old_connections_and_empty_scan_windows() {
        let (_root, directory) = crate::test_support::data_directory("profiles", "paging");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        crate::test_support::on_system(move || async move {
            let connection = database
                .create_config(NewConfig {
                    name: "old connection".into(),
                    body: ConfigBody::Connection(ConnectionConfig {
                        protocol: LlmTransport::OpenAiChatCompletions,
                        endpoint: Some("https://old.example".into()),
                        credential_ref: None,
                    }),
                })
                .await
                .expect("connection");
            let executor = database
                .create_executor(NewExecutor {
                    name: "pool".into(),
                    concurrency_limit: NonZeroU32::new(2).expect("limit"),
                    llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
                })
                .await
                .expect("executor");
            let mut expected = BTreeSet::new();
            for chunk in (0..1_005).collect::<Vec<_>>().chunks(128) {
                let mut queued = Vec::new();
                for &index in chunk {
                    let database = database.clone();
                    queued.push(ntex::rt::spawn(async move {
                        database
                            .create_config(NewConfig {
                                name: format!("profile {index}"),
                                body: if index % 3 == 0 {
                                    ConfigBody::Agent(AgentConfig::default())
                                } else {
                                    ConfigBody::Model(ModelConfig {
                                        connection_id: connection,
                                        model: format!("model-{index}"),
                                        context_window: None,
                                        parameters: LlmParameters::default(),
                                        declared_capabilities: BTreeSet::from([
                                            ModelCapability::Chat,
                                        ]),
                                    })
                                },
                            })
                            .await
                            .expect("profile")
                    }));
                }
                for task in queued {
                    expected.insert(task.await.expect("config task"));
                }
            }
            // Newer Connections fill the first scan window without a Profile,
            // so its page advances through an empty page.
            for index in 0..30 {
                database
                    .create_config(NewConfig {
                        name: format!("noise {index}"),
                        body: ConfigBody::Connection(ConnectionConfig {
                            protocol: LlmTransport::OpenAiChatCompletions,
                            endpoint: Some("https://noise.example".into()),
                            credential_ref: None,
                        }),
                    })
                    .await
                    .expect("connection");
            }
            let mut before = None;
            let mut seen = BTreeSet::new();
            let mut pages = 0;
            let mut seen_executors = BTreeSet::new();
            loop {
                let reply = list_profiles(&database, before, 7).await.expect("page");
                pages += 1;
                assert!(pages <= 1_005);
                assert!(
                    serde_json::to_vec(&reply.body).expect("body").len()
                        <= MAX_PROFILE_PAGE_BYTES + 4_096
                );
                let llm = reply.body["llm_profiles"].as_array().expect("llm");
                let agents = reply.body["agent_profiles"].as_array().expect("agents");
                let executors = reply.body["executor_protocols"]
                    .as_array()
                    .expect("executors");
                assert!(llm.len() + agents.len() + executors.len() <= 7);
                for offered in executors {
                    assert_eq!(offered["protocol"], "openai_chat_completions");
                    assert!(
                        seen_executors
                            .insert(offered["id"].as_str().expect("executor id").to_owned())
                    );
                }
                for profile in llm {
                    let id = ConfigId::from(
                        profile_id(profile["id"].as_str().expect("id")).expect("identity"),
                    );
                    assert!(seen.insert(id));
                    assert_eq!(profile["endpoint"], "https://old.example");
                }
                for profile in agents {
                    let id = ConfigId::from(
                        profile_id(profile["id"].as_str().expect("id")).expect("identity"),
                    );
                    assert!(seen.insert(id));
                }
                let next = reply.body["next_before"]
                    .as_str()
                    .map(|raw| ConfigId::from(profile_id(raw).expect("continuation identity")));
                assert_eq!(reply.body["has_more"], json!(next.is_some()));
                match next {
                    Some(next) => {
                        assert!(before.is_none_or(|previous| next < previous));
                        before = Some(next);
                    }
                    None => break,
                }
            }
            assert_eq!(seen, expected);
            assert_eq!(seen_executors, BTreeSet::from([executor.to_string()]));
        });
        worker.shutdown().expect("shutdown");
    }
}

#[cfg(test)]
mod approval_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{ConversationSpec, Entry, ExecutionConfiguration, Origin};
    #[test]
    fn an_agent_with_unsorted_or_repeated_models_is_refused() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = crate::test_support::data_directory("profiles", "models");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let first = save_profile(&db, None, br#"{"kind":"llm","name":"first","model":"a","declared_capabilities":["chat","tools"]}"#).await.unwrap().body["profile_id"].as_str().unwrap().to_owned();
            let second = save_profile(&db, None, br#"{"kind":"llm","name":"second","model":"b","declared_capabilities":["chat","tools"]}"#).await.unwrap().body["profile_id"].as_str().unwrap().to_owned();
            let first: ConfigId = profile_id(&first).unwrap().into();
            let second: ConfigId = profile_id(&second).unwrap().into();
            let models = vec![second, first];
            assert!(matches!(
                db.create_config(crate::persistence::NewConfig {
                    name: "descending agent".into(),
                    body: crate::persistence::ConfigBody::Agent(AgentConfig {
                        models,
                        ..Default::default()
                    })
                })
                .await,
                Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "models",
                    "must be sorted and unique"
                )))
            ));
            worker.shutdown().unwrap();
        });
    }

    #[test]
    fn approve_profile_round_trips_keeps_omitted_clears_empty_and_requires_a_decider_at_t1() {
        crate::test_support::on_system(|| async {
            let (_root, directory) = crate::test_support::data_directory("profiles", "approve");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let model = save_profile(&db, None, br#"{"kind":"llm","name":"model","model":"m","declared_capabilities":["chat","tools"]}"#).await.unwrap().body["profile_id"].as_str().unwrap().to_owned();
            let mut body = json!({"kind":"agent","name":"approval agent","llm_targets":[model],"default_model":model,"approve":["close","commands"]});
            let saved = save_profile(&db, None, &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            let id = profile_id(saved.body["profile_id"].as_str().unwrap()).unwrap();
            assert_eq!(
                saved.body["agent_profiles"][0]["approve"],
                json!(["commands", "close"])
            );
            body.as_object_mut().unwrap().remove("approve");
            let saved = save_profile(&db, Some(id), &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            assert_eq!(
                saved.body["agent_profiles"][0]["approve"],
                json!(["commands", "close"])
            );
            assert!(matches!(
                db.open_conversation(
                    None,
                    ConversationSpec {
                        name: None,
                        agent: Some(id.into()),
                        project: None,
                        origin: Origin::System,
                        keep_open: true
                    },
                    Entry::UserMessage {
                        text: "system".into(),
                        attachments: vec![]
                    },
                    ExecutionConfiguration {
                        agent_profile: Some(id),
                        llm_profile: profile_id(&model).unwrap()
                    },
                    None
                )
                .await,
                Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "approve",
                    "no deciding user"
                )))
            ));
            body["approve"] = json!([]);
            let saved = save_profile(&db, Some(id), &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            assert_eq!(saved.body["agent_profiles"][0]["approve"], json!([]));
            body["approve"] = json!(["unknown"]);
            assert!(
                matches!(save_profile(&db, Some(id), &serde_json::to_vec(&body).unwrap()).await, Err(refusal) if refusal.code == "invalid_approve")
            );
            worker.shutdown().unwrap();
        });
    }
}

#[cfg(test)]
mod responses_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::test_support::{data_directory, on_system};

    #[test]
    fn responses_profiles_import_and_executor_survive_reopening_existing_format() {
        on_system(|| async {
            let (_root, directory) = data_directory("responses", "profiles");
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let old = save_profile(
                &db,
                None,
                br#"{"kind":"llm","name":"old","model":"old","declared_capabilities":["chat"]}"#,
            )
            .await
            .unwrap();
            let old_id = old.body["profile_id"].as_str().unwrap().to_owned();
            let body = json!({"kind":"llm","name":"responses","model":"muse","protocol":"openai_responses","endpoint":"https://opencode.ai/inference/go/openai/v1/responses","declared_capabilities":["chat","tools"]});
            let saved = save_profile(&db, None, &serde_json::to_vec(&body).unwrap())
                .await
                .unwrap();
            let id = saved.body["profile_id"].as_str().unwrap().to_owned();
            let mut edit = body.clone();
            edit.as_object_mut().unwrap().remove("protocol");
            save_profile(
                &db,
                Some(profile_id(&id).unwrap()),
                &serde_json::to_vec(&edit).unwrap(),
            )
            .await
            .unwrap();
            let import = json!({"source_profile_id":id,"models":[{"id":"second"}],"declared_capabilities":["chat"]});
            import_models(&db, &serde_json::to_vec(&import).unwrap())
                .await
                .unwrap();
            drop(db);
            worker.shutdown().unwrap();
            let worker = DatabaseWorker::start(&directory).unwrap();
            let db = worker.handle();
            let profiles = list_profiles(&db, None, 50).await.unwrap();
            let models = profiles.body["llm_profiles"].as_array().unwrap();
            assert_eq!(
                models.iter().find(|m| m["id"] == id).unwrap()["protocol"],
                "openai_responses"
            );
            assert_eq!(
                models.iter().find(|m| m["id"] == old_id).unwrap()["protocol"],
                "openai_chat_completions"
            );
            assert_eq!(
                models.iter().find(|m| m["model"] == "second").unwrap()["protocol"],
                "openai_responses"
            );
            assert!(
                profiles.body["executor_protocols"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| e["protocol"] == "openai_responses")
            );
            worker.shutdown().unwrap();
        });
    }
}
