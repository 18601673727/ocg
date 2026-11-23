use super::super::contracts::{OutboundHeaderInput, envelope};
use super::super::wire::{INCONSISTENT_RECORD, Refusal, Reply};
use super::handlers::previous_model_connection;
use super::headers::plan_headers;
use super::validation::{
    INVALID_CAPABILITIES, INVALID_CREDENTIAL, INVALID_CREDENTIAL_REF, INVALID_ENDPOINT,
    INVALID_MODEL, INVALID_PROFILE_BODY, INVALID_PROFILE_NAME, MAX_ENDPOINT_BYTES, MAX_MODEL_BYTES,
    MAX_PROFILE_NAME_BYTES, MAX_PROVIDER_MESSAGE_BYTES, PROFILE_NOT_FOUND, UNSUPPORTED_PROTOCOL,
    WRONG_PROFILE_KIND, default_credential_ref, map_mutation_error, parse_capability_set,
    profile_id, valid_credential_ref, valid_secret_header, valid_single_line,
};
use super::wire::{ImportedModelsView, ProviderModelsView};
use crate::ai::providers::CredentialResolver;
use crate::db::DatabaseHandle;
use crate::db::entities::{
    Config, ConfigBody, ConfigKind, ConnectionConfig, CredentialSecret, DecisionProtocol,
    LlmTransport, ModelCapability, ProfileId,
};
use crate::db::mutations::{HeaderChange, ImportModelsSave, ProfileCredentialRef};
use ntex::http::StatusCode;
use serde::Deserialize;
use std::collections::BTreeMap;

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
        ModelCapability::parse,
        INVALID_CAPABILITIES,
    )?;
    let headers;
    let (connection, credential_ref, secret) = match (request.source_profile_id, request.provider) {
        (Some(id), None) => {
            headers = HeaderChange::Preserve;
            let id = profile_id(&id)?;
            let (connection, _) = previous_model_connection(database, Some(id), ConfigKind::Model)
                .await?
                .ok_or(PROFILE_NOT_FOUND)?;
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
                || crate::ai::providers::model_endpoint(endpoint).is_err()
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
        || crate::ai::providers::model_endpoint(&request.endpoint).is_err()
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
    use crate::ai::providers::ModelListError;
    let saved_headers = match request.profile_id.as_deref() {
        None => Vec::new(),
        Some(id) => {
            let kind = match protocol {
                DiscoveryProtocol::Generative(_) => ConfigKind::Model,
                DiscoveryProtocol::Decision(_) => ConfigKind::DecisionModel,
            };
            match previous_model_connection(database, Some(profile_id(id)?), kind).await? {
                Some((record, _)) => {
                    let ConfigBody::Connection(connection) = record.body else {
                        return Err(INCONSISTENT_RECORD);
                    };
                    connection.headers
                }
                None => Vec::new(),
            }
        }
    };
    let plan = plan_headers(database, request.headers, &saved_headers).await?;
    let listed = crate::ai::providers::list_models(
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
