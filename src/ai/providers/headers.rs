use ntex::client::ClientRequest;
use ntex::http::header::{HeaderName, HeaderValue};

use super::{CredentialResolver, ProviderExecutionError};
use crate::db::entities::FrozenHeaders;

pub(crate) fn authorization(credential: &str) -> Result<HeaderValue, ProviderExecutionError> {
    Ok(crate::network::http::authorization(credential)?)
}

#[derive(Default)]
pub(crate) struct RequestHeaders {
    fields: Vec<(HeaderName, HeaderValue)>,
    secrets: Vec<String>,
}

impl std::fmt::Debug for RequestHeaders {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_list()
            .entries(self.fields.iter().map(|(name, _)| name.as_str()))
            .finish()
    }
}

impl Drop for RequestHeaders {
    fn drop(&mut self) {
        self.secrets
            .iter_mut()
            .for_each(crate::support::secrets::wipe_string);
    }
}

impl RequestHeaders {
    pub(crate) fn apply(&self, mut request: ClientRequest) -> ClientRequest {
        for (name, value) in &self.fields {
            request = request.set_header(name.clone(), value.clone());
        }
        request
    }

    pub(crate) fn secrets(&self) -> &[String] {
        &self.secrets
    }
}

pub(crate) async fn resolve_request_headers(
    frozen: &FrozenHeaders,
    resolver: &dyn CredentialResolver,
) -> Result<RequestHeaders, ProviderExecutionError> {
    use crate::db::entities::{HeaderScope, OutboundHeaderValue, validate_fields};
    let invalid = || ProviderExecutionError::Build {
        message: "the custom request headers are invalid".to_owned(),
    };
    validate_fields(&frozen.fields, HeaderScope::Connection).map_err(|_| invalid())?;
    let mut resolved = RequestHeaders::default();
    for field in &frozen.fields {
        let name = HeaderName::try_from(field.name.as_str()).map_err(|_| invalid())?;
        let (text, secret) = match &field.value {
            OutboundHeaderValue::Plain(value) => (value.clone(), false),
            OutboundHeaderValue::Secret(reference) => {
                let value = resolver
                    .resolve(Some(reference.as_str()))
                    .await?
                    .ok_or_else(|| ProviderExecutionError::Build {
                        message: "a secret request header cannot be resolved".to_owned(),
                    })?;
                (value, true)
            }
        };
        let value = HeaderValue::try_from(text.as_str()).map(|mut value| {
            value.set_sensitive(secret);
            value
        });
        if secret {
            resolved.secrets.push(text);
        }
        resolved.fields.push((name, value.map_err(|_| invalid())?));
    }
    Ok(resolved)
}
