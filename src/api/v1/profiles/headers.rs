use super::super::contracts::{
    GlobalOutboundHeadersInput, GlobalOutboundHeadersView, OutboundHeaderInput, OutboundHeaderView,
    envelope,
};
use super::super::wire::{Refusal, Reply};
use super::validation::{INVALID_CREDENTIAL, INVALID_PROFILE_BODY, map_mutation_error};
use crate::ai::providers::CredentialResolver;
use crate::ai::providers::headers::{RequestHeaders, resolve_request_headers};
use crate::db::entities::{
    CredentialSecret, FrozenHeaders, HeaderError, HeaderScope, OutboundHeader, OutboundHeaderValue,
    Violation, merge, normalize_header_name, validate_fields, validate_value,
};
use crate::db::mutations::{HeaderChange, HeaderEdit, HeaderEditValue};
use crate::db::{DatabaseError, DatabaseHandle};
use ntex::http::StatusCode;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(in crate::api::v1) const INVALID_HEADERS: Refusal = Refusal::new(
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
                    Output = Result<Option<String>, crate::ai::providers::ProviderExecutionError>,
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

pub(super) struct HeaderPlan {
    pub(super) change: HeaderChange,
    pub(super) probe: RequestHeaders,
}

pub(super) async fn plan_headers(
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
                let name =
                    normalize_header_name(input.name.trim()).map_err(|e| invalid_headers(&e))?;
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
            stored: Arc::new(database.clone()),
            supplied,
        },
    )
    .await
    .map_err(|_| INVALID_HEADERS)?;
    Ok(HeaderPlan { change, probe })
}

pub(super) fn header_views(
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

pub(in crate::api::v1) async fn global_outbound_headers(
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
            let name = normalize_header_name(input.name.trim()).map_err(|e| invalid_headers(&e))?;
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
