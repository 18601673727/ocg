use super::super::contracts::{
    GlobalCompactionSettingsInput, OutboundHeaderInput, compaction_policy_view, envelope,
};
use super::super::wire::{INCONSISTENT_RECORD, Refusal, Reply};
use super::headers::plan_headers;
use super::listing::build_profiles_snapshot;
use super::validation::{
    INVALID_AGENT_CAPABILITIES, INVALID_CAPABILITIES, INVALID_DECISION_CAPABILITIES,
    INVALID_DECISION_LIMITS, INVALID_DECISION_MODELS, INVALID_LLM_TARGET, INVALID_MODEL,
    INVALID_PARAMETERS, INVALID_PROFILE_BODY, INVALID_PROFILE_NAME, MAX_MODEL_BYTES,
    MAX_PROFILE_NAME_BYTES, MISSING_DECISION_MODELS, MISSING_LLM_TARGETS, PROFILE_NOT_FOUND,
    PROFILE_REVISION_REQUIRED, UNSUPPORTED_DECISION_PROTOCOL, UNSUPPORTED_PROTOCOL,
    WRONG_PROFILE_KIND, config_revision, map_mutation_error, normalize_endpoint,
    normalize_instructions, parse_capability_set, parse_profile_refs, profile_id,
    resolve_credential_input, revision_guard, valid_single_line, validate_credential_origin,
};
use super::wire::ProfileRequest;
use crate::db::entities::{
    AgentCapability, AgentConfig, Config, ConfigBody, ConfigId, ConfigKind, ConfigRecord,
    DecisionAgentConfig, DecisionCapability, DecisionLimits, DecisionProtocol, LlmParameters,
    LlmTransport, ModelCapability, ProfileId, Violation,
};
use crate::db::mutations::{ModelProfileDefinition, ModelProfileSave, ProfileCredentialRef};
use crate::db::{DatabaseError, DatabaseHandle};
use ntex::http::StatusCode;
use serde::Deserialize;
use std::collections::BTreeSet;

pub(super) async fn previous_model_connection(
    database: &DatabaseHandle,
    existing_id: Option<ProfileId>,
    kind: ConfigKind,
) -> Result<Option<(ConfigRecord, crate::db::mutations::ProfileSaveGuard)>, Refusal> {
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
        ConfigBody::Connection(_) | ConfigBody::Agent(_) | ConfigBody::DecisionAgent(_) => {
            return Err(WRONG_PROFILE_KIND);
        }
    };
    let connection_record = database
        .config(connection_id)
        .await?
        .ok_or(INCONSISTENT_RECORD)?;
    let guard = crate::db::mutations::ProfileSaveGuard {
        profile_edit_seq: record.edit_seq,
        connection_id,
        connection_edit_seq: connection_record.edit_seq,
    };
    Ok(Some((connection_record, guard)))
}

enum PreparedProfileSave {
    Config(Config, Option<u32>),
    Model(ModelProfileSave),
}

struct CommonModelInput {
    name: String,
    model: String,
    connection_protocol: Option<LlmTransport>,
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
    let previous = previous_model_connection(database, existing_id, kind).await?;
    let connection_protocol = input
        .connection_protocol
        .or_else(|| match &previous {
            Some((record, _)) => match &record.body {
                ConfigBody::Connection(connection) => Some(connection.protocol),
                ConfigBody::Model(_)
                | ConfigBody::Agent(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::DecisionAgent(_) => None,
            },
            None => None,
        })
        .unwrap_or(LlmTransport::OpenAiChatCompletions);
    let (previous_headers, previous_credential_ref, allow_stored_secrets) = match &previous {
        Some((record, _)) => {
            let ConfigBody::Connection(connection) = &record.body else {
                return Err(INCONSISTENT_RECORD);
            };
            (
                connection.headers.clone(),
                connection.credential_ref.clone(),
                if kind == ConfigKind::DecisionModel {
                    match (connection.endpoint.as_deref(), endpoint.as_deref()) {
                        (_, None) => true,
                        (Some(previous), Some(next)) => {
                            crate::ai::providers::provider_origins_match(
                                Some(previous),
                                connection.protocol,
                                Some(next),
                                connection_protocol,
                            )
                        }
                        (None, Some(_)) => false,
                    }
                } else {
                    crate::ai::providers::provider_origins_match(
                        connection.endpoint.as_deref(),
                        connection.protocol,
                        endpoint.as_deref(),
                        connection_protocol,
                    )
                },
            )
        }
        None => (Vec::new(), None, true),
    };
    let (credential_ref, secret) = resolve_credential_input(
        &name,
        input.credential_ref.as_deref(),
        input.credential.as_deref(),
        input.clear_credential,
    )?;
    if secret.is_none() {
        let reused_reference = match &credential_ref {
            ProfileCredentialRef::Preserve => previous_credential_ref.as_deref(),
            ProfileCredentialRef::Explicit(reference) => Some(reference.as_str()),
            ProfileCredentialRef::Clear | ProfileCredentialRef::PreserveOrGenerate(_) => None,
        };
        if let Some(reference) = reused_reference {
            let target_endpoint = if kind == ConfigKind::DecisionModel {
                endpoint.as_deref()
            } else {
                Some(crate::ai::providers::effective_provider_endpoint(
                    endpoint.as_deref(),
                    connection_protocol,
                ))
            };
            if let Some(target_endpoint) = target_endpoint {
                validate_credential_origin(database, reference, target_endpoint, false).await?;
            }
        }
    }
    let guard = revision_guard(previous.map(|(_, guard)| guard), input.expected_edit_seq)?;
    let plan = plan_headers(
        database,
        input.headers,
        &previous_headers,
        allow_stored_secrets,
    )
    .await?;
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
                ModelCapability::parse,
                INVALID_CAPABILITIES,
            )?;
            let max_temperature = match protocol {
                Some(LlmTransport::AnthropicMessages) => {
                    crate::ai::providers::anthropic::MAX_TEMPERATURE
                }
                Some(LlmTransport::OpenAiChatCompletions | LlmTransport::OpenAiResponses)
                | None => 2.0,
            };
            if parameters
                .temperature
                .is_some_and(|v| !(0.0..=max_temperature).contains(&v))
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
                        crate::ai::providers::openai_compatible::request::parse_reasoning_effort(
                            effort,
                        )
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
                    connection_protocol: protocol,
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
                DecisionCapability::parse,
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
                    connection_protocol: Some(LlmTransport::OpenAiChatCompletions),
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

pub(in crate::api::v1) async fn global_compaction_settings(
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
