//! Anthropic native provider.
//!
//! OCG-owned protocol boundary for the Anthropic Messages API. Anthropic is not
//! reached through an OpenAI-compatible translation: OCG encodes the Messages
//! request directly and decodes the Messages SSE stream directly, and the
//! adapter ends at OCG's existing canonical provider structures —
//! [`crate::openai_compatible::stream::ChatStreamEvent`],
//! [`crate::openai_compatible::stream::ChatStreamSummary`] and
//! [`crate::openai_compatible::stream::NormalizedUsage`]. Nothing Anthropic-
//! shaped reaches the provider loop, the Call lifecycle or the Chat surface.
//!
//! ```text
//! Anthropic Messages endpoint
//!         ↓
//! OCG-owned ntex HTTP surface (incremental chunk delivery)
//!         ↓
//! Anthropic SSE event  →  canonical ChatStreamEvent / ChatStreamSummary
//!         ↓
//! canonical provider round → Call / DispatchIntent → accounting
//! ```
//!
//! ## Endpoint
//!
//! The Profile endpoint is the full Messages URL (`…/v1/messages`), the same
//! convention the OpenAI providers already use: OCG never composes a path onto
//! a configured endpoint, so no host-relative guessing is introduced here.
//!
//! ## Credentials
//!
//! The Vault value is Anthropic's `x-api-key` verbatim. It is read from the
//! user-global Vault at execution time and never persisted, logged or copied
//! into provider, model or Call data. The canonical wire version travels in the
//! `anthropic-version` header.

pub mod error;
pub mod request;
pub mod stream;

pub use error::{anthropic_failure, anthropic_failure_from_body, describe};
pub use request::{build_request, DEFAULT_MAX_TOKENS};
pub use stream::{parse_usage, validate_tool_arguments, AnthropicStreamState};

/// The `anthropic-version` header value this protocol speaks.
///
/// Pinned rather than negotiated: the Messages request/response shapes decoded
/// here are one specific version of that contract, and sending an unrecognised
/// version would let the provider answer in a shape this module cannot decode.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";
