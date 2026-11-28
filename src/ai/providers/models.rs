use std::collections::BTreeMap;

use super::headers::{RequestHeaders, credential_header};
use super::transport::provider_client_for;
use super::{ACCEPT, DestinationPolicy, Duration, Value, anthropic};

pub(crate) use crate::db::entities::DiscoveredModel as ProviderModel;

pub(crate) enum ModelListError {
    Unreachable,
    Rejected { status_code: u16, message: String },
    NotListed,
}

pub(crate) async fn list_models(
    endpoint: &str,
    credential: Option<&str>,
    protocol: Option<crate::db::entities::LlmTransport>,
    headers: &RequestHeaders,
) -> Result<Vec<ProviderModel>, ModelListError> {
    let base = super::model_endpoint(endpoint).map_err(|_| ModelListError::Unreachable)?;
    let anthropic = protocol == Some(crate::db::entities::LlmTransport::AnthropicMessages);
    let decision = protocol.is_none();
    let authorization =
        credential_header(credential, anthropic).map_err(|_| ModelListError::Unreachable)?;
    ntex::time::timeout(Duration::from_secs(30), async {
        let client = provider_client_for(&DestinationPolicy::Standard, &base)
            .await
            .map_err(|_| ModelListError::Unreachable)?;
        let mut endpoint = if anthropic {
            format!("{base}?limit=1000")
        } else {
            base.to_string()
        };
        let mut models = BTreeMap::new();
        let mut cursors = std::collections::BTreeSet::new();
        for _ in 0..100 {
            let (status, body) = {
                let request = headers
                    .apply(client.get(&endpoint))
                    .set_header(ACCEPT, "application/json")
                    .if_some(authorization.clone(), |(name, value), request| {
                        request.set_header(name, value)
                    });
                let request = if anthropic {
                    request.set_header("anthropic-version", anthropic::API_VERSION)
                } else {
                    request
                };
                let response = request
                    .send()
                    .await
                    .map_err(|_| ModelListError::Unreachable)?;
                let status = response.status().as_u16();
                if status == 404 {
                    return Err(ModelListError::NotListed);
                }
                let body = match response.body().limit(1024 * 1024).await {
                    Ok(body) => body,
                    Err(error)
                        if matches!(**error, ntex::http::error::PayloadError::Overflow)
                            && (200..300).contains(&status) =>
                    {
                        return Err(ModelListError::NotListed);
                    }
                    Err(_) if !(200..300).contains(&status) => Default::default(),
                    Err(_) => return Err(ModelListError::Unreachable),
                };
                (status, body)
            };
            for model in parse_models(status, &body, credential, headers, decision)? {
                models.insert(model.id.clone(), model);
            }
            if !anthropic {
                return Ok(models.into_values().collect());
            }
            let value =
                serde_json::from_slice::<Value>(&body).map_err(|_| ModelListError::NotListed)?;
            if value["has_more"] == false {
                return Ok(models.into_values().collect());
            }
            if value["has_more"] != true {
                return Err(ModelListError::NotListed);
            }
            let cursor = value["last_id"]
                .as_str()
                .filter(|cursor| !cursor.is_empty() && cursor.len() <= 1024)
                .ok_or(ModelListError::NotListed)?;
            if !cursors.insert(cursor.to_owned()) {
                return Err(ModelListError::NotListed);
            }
            let cursor = cursor
                .bytes()
                .map(|byte| {
                    if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                        char::from(byte).to_string()
                    } else {
                        format!("%{byte:02X}")
                    }
                })
                .collect::<String>();
            endpoint = format!("{base}?limit=1000&after_id={cursor}");
        }
        Err(ModelListError::NotListed)
    })
    .await
    .map_err(|_| ModelListError::Unreachable)?
}

fn parse_models(
    status: u16,
    body: &[u8],
    credential: Option<&str>,
    headers: &RequestHeaders,
    decision: bool,
) -> Result<Vec<ProviderModel>, ModelListError> {
    if status == 404 {
        return Err(ModelListError::NotListed);
    }
    let value = (body.len() <= 1024 * 1024)
        .then(|| serde_json::from_slice::<Value>(body).ok())
        .flatten();
    if !(200..300).contains(&status) {
        let message = value
            .as_ref()
            .and_then(|value| {
                if decision {
                    value.get("detail").and_then(Value::as_str)
                } else {
                    value.get("error").and_then(|error| {
                        error
                            .get("message")
                            .and_then(Value::as_str)
                            .or_else(|| error.as_str())
                    })
                }
            })
            .or_else(|| value.as_ref()?.get("message")?.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("provider returned HTTP {status}"));
        let message = credential.filter(|secret| !secret.is_empty()).map_or_else(
            || message.clone(),
            |secret| message.replace(secret, "[redacted]"),
        );
        let message = headers
            .secrets()
            .iter()
            .filter(|secret| !secret.is_empty())
            .fold(message, |message, secret| {
                message.replace(secret.as_str(), "[redacted]")
            });
        return Err(ModelListError::Rejected {
            status_code: status,
            message,
        });
    }
    let entries = value
        .as_ref()
        .and_then(|value| value.get(if decision { "models" } else { "data" }))
        .and_then(Value::as_array)
        .ok_or(ModelListError::NotListed)?;
    let mut models = BTreeMap::<String, ProviderModel>::new();
    for entry in entries {
        let Some(id) = entry
            .get(if decision { "name" } else { "id" })
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && (!decision || id.len() <= 512))
        else {
            continue;
        };
        if credential.is_some_and(|secret| !secret.is_empty() && id.contains(secret)) {
            continue;
        }
        let context = [
            "context_length",
            "context_window",
            "max_context_length",
            "max_input_tokens",
        ]
        .iter()
        .find_map(|key| {
            entry
                .get(key)
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0)
        });
        let efforts = [
            entry.get("reasoning_efforts"),
            entry.get("supported_reasoning_efforts"),
            entry.get("reasoning_effort"),
            entry.pointer("/metadata/efforts"),
            entry.pointer("/metadata/reasoning_efforts"),
            entry.pointer("/capabilities/reasoning_efforts"),
            entry.pointer("/reasoning/efforts"),
            entry.pointer("/reasoning/effort/enum"),
            entry.get("efforts"),
        ]
        .into_iter()
        .flatten()
        .find_map(Value::as_array)
        .map(|values| {
            let mut values = values
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| crate::db::entities::ReasoningEffort::parse(value).is_some())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            values.sort_by_key(|value| crate::db::entities::ReasoningEffort::parse(value));
            values.dedup();
            values
        });
        models.insert(
            id.to_owned(),
            ProviderModel {
                id: id.to_owned(),
                context_window: context,
                reasoning_efforts: efforts,
            },
        );
    }
    Ok(models.into_values().collect())
}
