//! OpenAI-compatible provider execution for an admitted Job.

use std::sync::{
    Arc, Mutex, PoisonError, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Poll, Waker};
use std::time::Duration;
use std::{future::Future, pin::Pin};

use ntex::client::{ClientResponse, error::ClientError};

use ntex::http::header::{ACCEPT, HeaderValue};
use serde_json::{Value, json};

use crate::db::entities::{
    FrozenExecutionConfig, IntermediateOutputKind, Timestamp, UsageFinality,
};
use openai_compatible::{
    ChatConversation, ChatFinishReason, ChatMessage, CompletedToolCall, InvalidToolCall,
    NormalizedUsage, ProviderFailure,
};

pub(crate) const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
pub(crate) const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

pub mod anthropic;
mod cancellation;
mod models;
pub(crate) mod observe;
pub mod openai_compatible;
mod record;
pub use record::{RequestRecorder, SentRequestState};
pub(crate) mod responses;
mod types;
pub(crate) mod usage;

pub use cancellation::CancellationToken;
use observe::announce_at;
pub(crate) use observe::unannounced_fact;
pub(crate) use observe::{announce, rejected_failure};
pub(crate) fn model_endpoint(endpoint: &str) -> Result<String, ProviderExecutionError> {
    self::endpoint(endpoint, "models")
}
pub(crate) use models::{ModelListError, ProviderModel, list_models};
pub use observe::{error_observations, metrics, result_value, success_observation};
pub use types::{
    CredentialResolver, EnvironmentCredentialResolver, OutputSinkError, ProviderExecutionConfig,
    ProviderExecutionError, ProviderExecutionResult, ProviderFact, ProviderObservation,
    ProviderOutputSink, ResponseFacts,
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
    let uri = url.parse::<ntex::http::Uri>().ok();
    if uri.as_ref().is_none_or(|uri| {
        !(uri.scheme_str() == Some("https")
            || (uri.scheme_str() == Some("http")
                && matches!(
                    uri.host(),
                    Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
                )))
            || uri.host().is_none_or(str::is_empty)
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || uri.query().is_some()
    }) || url.contains('#')
    {
        return Err(ProviderExecutionError::Build {
            message:
                "provider endpoint must use https (or http on localhost, 127.0.0.1 or ::1) without credentials, query or fragment"
                    .to_owned(),
        });
    }
    Ok(url)
}
