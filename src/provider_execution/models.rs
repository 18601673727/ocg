use std::collections::BTreeMap;

use serde::Serialize;

use super::*;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderModel {
    pub id: String,
    pub context_window: Option<u64>,
}

pub(crate) enum ModelListError {
    Unreachable,
    Rejected { status_code: u16, message: String },
    NotListed,
}

pub(crate) async fn list_models(
    endpoint: &str,
    credential: Option<&str>,
    protocol: crate::persistence::LlmTransport,
) -> Result<Vec<ProviderModel>, ModelListError> {
    let base = exchange::endpoint(endpoint, "models").map_err(|_| ModelListError::Unreachable)?;
    let anthropic = protocol == crate::persistence::LlmTransport::AnthropicMessages;
    let authorization = credential
        .map(if anthropic {
            anthropic::api_key
        } else {
            exchange::authorization
        })
        .transpose()
        .map_err(|_| ModelListError::Unreachable)?;
    ntex::time::timeout(Duration::from_secs(30), async {
        let client = exchange::provider_client(Duration::from_secs(30))
            .await
            .map_err(|_| ModelListError::Unreachable)?;
        let mut endpoint = if anthropic {
            format!("{base}?limit=1000")
        } else {
            base.clone()
        };
        let mut models = BTreeMap::new();
        let mut cursors = std::collections::BTreeSet::new();
        for _ in 0..100 {
            #[cfg(test)]
            let scripted = scripted::http_response(&endpoint, authorization.as_ref());
            #[cfg(not(test))]
            let scripted: Option<Result<(u16, Vec<u8>), ()>> = None;
            let (status, body) = if let Some(response) = scripted {
                response.map_err(|()| ModelListError::Unreachable)?
            } else {
                let request = client
                    .get(&endpoint)
                    .set_header(ACCEPT, "application/json")
                    .if_some(authorization.clone(), |authorization, request| {
                        request.set_header(
                            if anthropic {
                                "x-api-key"
                            } else {
                                "authorization"
                            },
                            authorization,
                        )
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
                    Ok(body) => body.to_vec(),
                    Err(ntex::http::error::PayloadError::Overflow)
                        if (200..300).contains(&status) =>
                    {
                        return Err(ModelListError::NotListed);
                    }
                    Err(_) if !(200..300).contains(&status) => Vec::new(),
                    Err(_) => return Err(ModelListError::Unreachable),
                };
                (status, body)
            };
            for model in parse_models(status, &body, credential)? {
                models.insert(model.id, model.context_window);
            }
            let value =
                serde_json::from_slice::<Value>(&body).map_err(|_| ModelListError::NotListed)?;
            if !anthropic || value["has_more"] == false {
                return Ok(models
                    .into_iter()
                    .map(|(id, context_window)| ProviderModel { id, context_window })
                    .collect());
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
            .and_then(|value| value.get("error"))
            .and_then(|error| {
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .or_else(|| error.as_str())
            })
            .or_else(|| value.as_ref()?.get("message")?.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("provider returned HTTP {status}"));
        let message = credential.filter(|secret| !secret.is_empty()).map_or_else(
            || message.clone(),
            |secret| message.replace(secret, "[redacted]"),
        );
        return Err(ModelListError::Rejected {
            status_code: status,
            message,
        });
    }
    let entries = value
        .as_ref()
        .and_then(|value| value.get("data"))
        .and_then(Value::as_array)
        .ok_or(ModelListError::NotListed)?;
    let mut models = BTreeMap::<String, Option<u64>>::new();
    for entry in entries {
        let Some(id) = entry
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
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
        .find_map(|key| entry.get(key).and_then(Value::as_u64));
        models
            .entry(id.to_owned())
            .and_modify(|existing| *existing = (*existing).max(context))
            .or_insert(context);
    }
    Ok(models
        .into_iter()
        .map(|(id, context_window)| ProviderModel { id, context_window })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_network_model_entries_without_an_id_are_skipped() {
        let models = match parse_models(
            200,
            br#"{"data":[{"id":""},{"no_id":true},{"id":"kept","context_length":7},{"id":3}]}"#,
            None,
        ) {
            Ok(models) => models,
            Err(_) => panic!("the list is still usable"),
        };
        assert_eq!(
            models,
            [ProviderModel {
                id: "kept".to_owned(),
                context_window: Some(7),
            }]
        );
    }
}
