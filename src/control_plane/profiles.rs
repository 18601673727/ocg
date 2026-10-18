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

use ntex::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    API_VERSION, DATABASE_BUSY, DATABASE_UNAVAILABLE, INCONSISTENT_RECORD, INTERNAL,
    INVALID_REQUEST, Refusal, Reply, SHUTTING_DOWN, decimal, micros,
};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    AgentBehavior, AgentConfig, ConfigBody, ConfigId, ConfigKind, ConfigRecord, ConnectionConfig,
    LlmParameters, LlmTransport, ModelCapability, ModelConfig, NewConfig, NewExecutor, ProfileId,
    RecordRef, Violation,
};

const MAX_PROFILE_NAME_BYTES: usize = 128;
const MAX_MODEL_BYTES: usize = 256;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_CREDENTIAL_REF_BYTES: usize = 128;
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
    "protocol must be openai_chat_completions",
);
const INVALID_ENDPOINT: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_endpoint",
    "endpoint must be an https:// URL of at most 512 characters without whitespace",
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
    "LLM parameters must be finite numbers within valid bounds (temperature 0..=2, top_p 0..=1, max_tokens >= 1, penalties -2..=2)",
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
    text.strip_prefix("profile:")
        .and_then(decimal::<u64>)
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
        llm_targets: Vec<String>,
    },
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

fn map_mutation_error(error: DatabaseError) -> Refusal {
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

/// Ensures at least one Pool offers OpenAI Chat Completions, so newly created
/// LLM Profiles can be selected and executed immediately.
async fn ensure_compatible_executor(database: &DatabaseHandle) -> Result<(), Refusal> {
    let executors = database.executors(None, PROFILE_SCAN_LIMIT).await?;
    let has_compatible = executors.iter().any(|executor| {
        executor
            .llm_transports
            .contains(&LlmTransport::OpenAiChatCompletions)
    });
    if !has_compatible {
        let concurrency_limit = NonZeroU32::new(4).ok_or(INTERNAL)?;
        database
            .create_executor(NewExecutor {
                name: "local-openai-executor".to_owned(),
                concurrency_limit,
                llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
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
    endpoint: Option<String>,
    credential_ref: Option<String>,
) -> Result<ConfigId, Refusal> {
    let wanted = ConnectionConfig {
        protocol: LlmTransport::OpenAiChatCompletions,
        endpoint,
        credential_ref,
    };
    let existing = database
        .configs(Some(ConfigKind::Connection), None, PROFILE_SCAN_LIMIT)
        .await?;
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

/// Reads all LLM and Agent Profiles and computes the derived Provider
/// projection from the LLM Profiles.
pub(super) async fn list_profiles(database: &DatabaseHandle) -> Result<Reply, Refusal> {
    let snapshot = build_profiles_snapshot(database, None).await?;
    Ok(Reply::ok(snapshot))
}

async fn build_profiles_snapshot(
    database: &DatabaseHandle,
    saved_profile: Option<ProfileId>,
) -> Result<Value, Refusal> {
    let records = database.configs(None, None, PROFILE_SCAN_LIMIT).await?;
    let mut llm_profiles = Vec::new();
    let mut agent_profiles = Vec::new();

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
    let connections: BTreeMap<ConfigId, ConnectionConfig> = records
        .iter()
        .filter_map(|record| match &record.body {
            ConfigBody::Connection(body) => Some((record.id, body.clone())),
            _ => None,
        })
        .collect();

    for record in records {
        if record.archived_at.is_some() {
            continue;
        }
        let id = ProfileId::from(record.id);
        match record.body {
            ConfigBody::Model(model) => {
                let connection = connections
                    .get(&model.connection_id)
                    .ok_or(INCONSISTENT_RECORD)?;
                let executors = database
                    .compatible_executors(id)
                    .await?
                    .into_iter()
                    .map(|executor| executor.to_string())
                    .collect::<Vec<_>>();
                let protocol = match connection.protocol {
                    LlmTransport::OpenAiChatCompletions => "openai_chat_completions",
                };
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

                let key = ProviderKey {
                    protocol,
                    endpoint: connection.endpoint.clone(),
                    credential_ref: connection.credential_ref.clone(),
                };
                let group = provider_groups.entry(key).or_insert_with(|| ProviderGroup {
                    has_credential,
                    profile_ids: Vec::new(),
                    models: BTreeSet::new(),
                });
                group.profile_ids.push(id_str.clone());
                group.models.insert(model.model.clone());

                llm_profiles.push(json!({
                    "id": id_str,
                    "name": record.name,
                    "model": model.model,
                    "protocol": protocol,
                    "endpoint": connection.endpoint,
                    "credential_ref": connection.credential_ref,
                    "has_credential": has_credential,
                    "declared_capabilities": capabilities,
                    "parameters": {
                        "temperature": model.parameters.temperature,
                        "top_p": model.parameters.top_p,
                        "max_tokens": model.parameters.max_tokens,
                        "presence_penalty": model.parameters.presence_penalty,
                        "frequency_penalty": model.parameters.frequency_penalty,
                    },
                    "executors": executors,
                    // Edits are always allowed and change no Job that froze the
                    // old values, so nothing locks a Profile.
                    "editable": true,
                    "blocking_jobs": Vec::<String>::new(),
                    "more_blocking_jobs": false,
                    "created_at_us": micros(Some(record.created_at)),
                    "updated_at_us": micros(Some(record.updated_at)),
                }));
            }
            ConfigBody::Agent(agent) => {
                agent_profiles.push(json!({
                    "id": id.to_string(),
                    "name": record.name,
                    "instructions": agent.instructions,
                    "behavior": match agent.behavior {
                        AgentBehavior::Chat => "chat",
                        AgentBehavior::General => "general",
                    },
                    "llm_targets": agent
                        .models
                        .iter()
                        .map(|model| ProfileId::from(*model).to_string())
                        .collect::<Vec<_>>(),
                    "editable": true,
                    "blocking_jobs": Vec::<String>::new(),
                    "more_blocking_jobs": false,
                    "created_at_us": micros(Some(record.created_at)),
                    "updated_at_us": micros(Some(record.updated_at)),
                }));
            }
            ConfigBody::Connection(_) | ConfigBody::Pool(_) => {}
        }
    }
    // Newest first, as Profiles have always been listed.
    llm_profiles.sort_by_key(|profile| std::cmp::Reverse(profile_number(profile)));
    agent_profiles.sort_by_key(|profile| std::cmp::Reverse(profile_number(profile)));

    let providers = provider_groups
        .into_iter()
        .map(|(key, group)| {
            json!({
                "protocol": key.protocol,
                "endpoint": key.endpoint,
                "credential_ref": key.credential_ref,
                "has_credential": group.has_credential,
                "profile_ids": group.profile_ids,
                "models": group.models.into_iter().collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    let mut body = json!({
        "api_version": API_VERSION,
        "llm_profiles": llm_profiles,
        "agent_profiles": agent_profiles,
        "providers": providers,
    });
    if let Some(saved) = saved_profile {
        body["profile_id"] = json!(saved.to_string());
    }
    Ok(body)
}

fn profile_number(profile: &Value) -> u64 {
    profile["id"]
        .as_str()
        .and_then(|id| profile_id(id).ok())
        .map_or(0, ProfileId::get)
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
            if protocol
                .as_deref()
                .is_some_and(|proto| proto.trim() != "openai_chat_completions")
            {
                return Err(UNSUPPORTED_PROTOCOL);
            }
            let normalized_endpoint = match endpoint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                None => None,
                Some(url) => {
                    if url.len() > MAX_ENDPOINT_BYTES
                        || !url.starts_with("https://")
                        || url["https://".len()..].is_empty()
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
                let ConfigBody::Model(existing_model) = existing_record.body else {
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

            if let Some(secret_value) = secret {
                if secret_value.len() > 4096
                    || ntex::http::header::HeaderValue::try_from(format!("Bearer {secret_value}"))
                        .is_err()
                {
                    return Err(INVALID_CREDENTIAL);
                }
            }

            let resolved_credential_ref = if clear_credential {
                None
            } else if let Some(secret_value) = secret {
                let target_ref = match explicit_ref.or(prior_credential_ref) {
                    Some(reference) => reference,
                    None => {
                        let base = default_credential_ref(name.trim());
                        let existing_connections = database
                            .configs(Some(ConfigKind::Connection), None, PROFILE_SCAN_LIMIT)
                            .await?;
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

            ensure_compatible_executor(database).await?;
            let connection = resolve_or_create_connection(
                database,
                name.trim(),
                normalized_endpoint,
                resolved_credential_ref,
            )
            .await?;

            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::Model(ModelConfig {
                    connection_id: connection,
                    model: model.trim().to_owned(),
                    parameters: LlmParameters {
                        temperature: parameters.temperature,
                        top_p: parameters.top_p,
                        max_tokens: parameters.max_tokens,
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
            llm_targets,
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
            if llm_targets.is_empty() {
                return Err(MISSING_LLM_TARGETS);
            }
            let mut parsed_targets: Vec<ConfigId> = Vec::with_capacity(llm_targets.len());
            for raw_target in &llm_targets {
                let target_id = raw_target
                    .trim()
                    .strip_prefix("profile:")
                    .and_then(decimal::<u64>)
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
            NewConfig {
                name: name.trim().to_owned(),
                body: ConfigBody::Agent(AgentConfig {
                    instructions: normalized_instructions,
                    behavior,
                    models: parsed_targets,
                    default_model: None,
                    tool_grant: None,
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
        database
            .set_credential(&target_ref, &secret_value)
            .map_err(|_| INVALID_CREDENTIAL)?;
    }
    let snapshot = build_profiles_snapshot(database, Some(profile_id)).await?;
    Ok(Reply::new(status, snapshot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{ExecutionConfiguration, JobInputDraft, NewJob};
    use crate::test_support::data_directory;

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
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    projects: Vec::new(),
                    dependencies: Vec::new(),
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
