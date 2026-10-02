//! Provider protocol identification.
//!
//! A *protocol* is the wire contract OCG speaks to an endpoint. It is a
//! property of the provider the Profile names, never something inferred from
//! the provider's key, label or endpoint host: the same host can front either
//! contract, and a provider key is a user-chosen label with no semantics.
//!
//! OCG's Provider Plane is protocol-agnostic above this boundary. Each protocol
//! owns only its own request encoding and its own stream decoding, and both
//! terminate at OCG's canonical provider request/response structures:
//!
//! ```text
//! canonical execution
//!        ↓
//! Provider Plane
//!    ├── OpenAI native        (OpenAi)
//!    ├── Anthropic native     (Anthropic)
//!    └── OpenAI-compatible    (OpenAiCompatible)
//! ```
//!
//! The protocol is frozen into the provider Call's durable payload at admission,
//! so recovery and re-execution replay the same protocol without re-reading
//! configuration.

use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// The protocol OCG speaks to a provider endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, TS)]
pub enum ProviderProtocol {
    /// The Anthropic Messages API.
    #[serde(rename = "anthropic")]
    #[ts(rename = "anthropic")]
    Anthropic,
    /// OpenAI Chat Completions, spoken natively by OCG.
    #[serde(rename = "openai")]
    #[ts(rename = "openai")]
    OpenAi,
    /// OpenAI Chat Completions served by a third party.
    #[serde(rename = "openai_compatible")]
    #[ts(rename = "openai_compatible")]
    #[default]
    OpenAiCompatible,
}

impl ProviderProtocol {
    /// The protocol used when a provider declares none. Chat Completions is the
    /// shape every existing Profile already speaks, so an absent declaration is
    /// the OpenAI-compatible contract and not a guess about the endpoint.
    #[must_use]
    pub fn default_protocol() -> Self {
        Self::default()
    }

    /// The stable wire spelling used in the frozen provider payload.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::OpenAi => "openai",
            Self::OpenAiCompatible => "openai_compatible",
        }
    }

    /// Read the protocol from its frozen wire spelling.
    ///
    /// An absent or unrecognized value is [`ProviderProtocol::default_protocol`]
    /// rather than an error: an intent written before the protocol was frozen is
    /// replayed as the contract it was admitted under, not rejected.
    #[must_use]
    pub fn from_wire(value: Option<&str>) -> Self {
        match value {
            Some("anthropic") => Self::Anthropic,
            Some("openai") => Self::OpenAi,
            _ => Self::default_protocol(),
        }
    }

    /// Whether this protocol's wire shape is OpenAI Chat Completions.
    ///
    /// Both native OpenAI and OpenAI-compatible endpoints speak it, so the tool
    /// projection and its strict argument contract apply to either.
    #[must_use]
    pub fn is_openai_chat_completions(self) -> bool {
        matches!(self, Self::OpenAi | Self::OpenAiCompatible)
    }
}

impl std::fmt::Display for ProviderProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
