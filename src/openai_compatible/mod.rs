//! OpenAI-compatible Chat Completions wire protocol.
//!
//! OCG-owned protocol boundary for OpenAI-compatible HTTP/streaming surfaces.
//! This module owns request parsing, response/stream encoding, and error
//! normalization for the OpenAI Chat Completions wire format.
//!
//! ```text
//! OpenAI-compatible client
//!         ↓
//! OCG-owned ntex HTTP surface
//!         ↓
//! OCG request validation / normalization
//!         ↓
//! Canonical Project / Job / Attempt / Call
//!         ↓
//! OCG provider execution plane
//!         ↓
//! OpenAI-compatible endpoint
//! ```

pub mod request;
pub mod response;
pub mod stream;
pub mod error;
pub mod normalize;

pub use request::{
    ChatRequest, ChatMessage, ChatRole, MessageContentWire, ContentPartWire,
    ToolWire, FunctionDefinitionWire, ChatToolCallWire, ChatFunctionCallWire,
    StreamOptions, StopSequences, ToolChoiceWire, NamedToolChoiceWire,
    NamedFunctionWire, ResponseFormatWire, JsonSchemaWire,
};
pub use response::ChatCompletionChunk;
pub use stream::{ChatStreamEvent, ChatFinishReason, NormalizedUsage, CompletedToolCall, ChatStreamSummary};
pub use error::{TransportError, ProviderFailure};
pub use normalize::ToolCallNormalizer;
