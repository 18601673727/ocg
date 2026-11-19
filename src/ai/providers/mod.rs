use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::client::{ClientResponse, error::ClientError};

use ntex::http::header::{ACCEPT, CONTENT_TYPE};
use ntex::util::{Either, select};
use serde_json::{Value, json};

use crate::db::entities::{
    FrozenExecutionConfig, FrozenHeaders, IntermediateOutputKind, Timestamp, UsageFinality,
};
use crate::db::entities::{NormalizedUsage, ProviderFailure};
use openai_compatible::{CompletedToolCall, InvalidToolCall};

pub(crate) const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

mod anthropic;
mod credentials;
pub(crate) mod headers;
mod models;
pub(crate) mod observe;
pub mod openai_compatible;
mod record;
pub(crate) mod responses;
pub use record::{RequestRecorder, SentRequestState};
mod transport;
mod types;
pub(crate) mod usage;

pub use crate::support::cancellation::CancellationToken;
pub(crate) use observe::announce;
use observe::announce_at;
pub(crate) use observe::unannounced_fact;
pub(crate) fn model_endpoint(endpoint: &str) -> Result<String, ProviderExecutionError> {
    self::endpoint(endpoint, "models")
}
pub(crate) use models::{ModelListError, list_models};
pub use observe::{error_observations, metrics, result_value, success_observation};
pub use types::{
    CredentialResolver, DestinationPolicy, EnvironmentCredentialResolver, OutputSinkError,
    ProviderExecutionConfig, ProviderExecutionError, ProviderExecutionResult, ProviderFact,
    ProviderObservation, ProviderOutputSink, ResponseFacts,
};

pub(crate) use types::incomplete_tool_calls;
pub(crate) use usage::UsageCapture;

pub(crate) fn endpoint(endpoint: &str, resource: &str) -> Result<String, ProviderExecutionError> {
    let endpoint = endpoint.trim_end_matches('/');
    let base = endpoint
        .strip_suffix("/chat/completions")
        .or_else(|| endpoint.strip_suffix("/responses"))
        .or_else(|| endpoint.strip_suffix("/messages"))
        .or_else(|| endpoint.strip_suffix("/systemone"))
        .or_else(|| endpoint.strip_suffix("/models"))
        .unwrap_or(endpoint);
    let url = format!("{base}/{resource}");
    if provider_uri(&url).is_none() {
        return Err(ProviderExecutionError::Build {
            message:
                "provider endpoint must use https (or http on localhost, 127.0.0.1 or ::1) without credentials, query or fragment"
                    .to_owned(),
        });
    }
    Ok(url)
}

pub(crate) fn provider_uri(url: &str) -> Option<ntex::http::Uri> {
    let uri = url.parse::<ntex::http::Uri>().ok()?;
    let supported_scheme = uri.scheme_str() == Some("https")
        || (uri.scheme_str() == Some("http")
            && matches!(
                uri.host(),
                Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
            ));
    (supported_scheme
        && uri.host().is_some_and(|host| !host.is_empty())
        && uri
            .authority()
            .is_some_and(|authority| !authority.as_str().contains('@'))
        && uri.query().is_none()
        && !url.contains('#'))
    .then_some(uri)
}

pub(super) async fn send(
    endpoint: String,
    (credential_ref, frozen_headers): (Option<&str>, &FrozenHeaders),
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    body: &Value,
    sink: Option<&mut dyn ProviderOutputSink>,
    stream: bool,
) -> Result<(ClientResponse, Instant, observe::Redactor), ProviderExecutionError> {
    use self::headers::{authorization, resolve_request_headers};
    use self::transport::provider_client_for;

    let operation = if stream { "provider" } else { "decision" };
    let credential = resolver.resolve(credential_ref).await?;
    let headers = resolve_request_headers(frozen_headers, resolver).await?;
    let redactor = observe::Redactor::new(credential.as_deref()).with_secrets(headers.secrets());
    let authorization = credential.as_deref().map(authorization).transpose()?;
    let cancelled = || ProviderExecutionError::NotDelivered {
        message: format!("{operation} execution was cancelled before sending"),
    };
    if config.cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let timeout = if stream {
        config.first_delta_timeout.unwrap_or(config.timeout)
    } else {
        config.timeout
    };
    let client = provider_client_for(timeout, &config.destination, &endpoint).await?;
    let session = stream
        .then(|| endpoint.parse::<ntex::http::Uri>().ok())
        .flatten()
        .filter(|uri| {
            uri.host()
                .is_some_and(|host| host.eq_ignore_ascii_case("opencode.ai"))
        })
        .and(config.session_id)
        .map(|id| format!("ocg-{}", id.get()));
    let accept = if stream {
        "text/event-stream"
    } else {
        "application/json"
    };
    let request = headers
        .apply(client.post(endpoint))
        .set_header(CONTENT_TYPE, "application/json")
        .set_header(ACCEPT, accept)
        .if_some(stream.then_some("no-cache"), |value, request| {
            request.set_header("Cache-Control", value)
        })
        .if_some(session, |session, request| {
            request.set_header("x-opencode-session", session)
        })
        .if_some(authorization, |authorization, request| {
            request.set_header("authorization", authorization)
        });
    if config.cancellation.is_cancelled() {
        return Err(cancelled());
    }
    if let Some(sink) = sink {
        sink.sending(body)
            .await
            .map_err(|error| ProviderExecutionError::NotDelivered {
                message: error.message,
            })?;
    }
    // Cancellation during the send-record write can leave evidence even before network I/O.
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::Interrupted {
            message: format!("{operation} execution was cancelled at the send boundary"),
        });
    }
    let started = Instant::now();
    let sent = select(
        config.cancellation.cancelled(),
        ntex::time::timeout(timeout, request.send_json(body)),
    )
    .await;
    let response = match sent {
        Either::Left(()) => {
            return Err(ProviderExecutionError::Interrupted {
                message: format!("{operation} execution was cancelled before a response arrived"),
            });
        }
        Either::Right(sent) => sent,
    }
    .map_err(|_| ProviderExecutionError::Unknown {
        message: format!("{operation} request timed out after it may have been delivered"),
        facts: None,
        finished: false,
    })?
    .map_err(observe::classify_client_error)?;
    Ok((response, started, redactor))
}
