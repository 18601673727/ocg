//! OCG-native context observation types.
//!
//! Provider-neutral vocabulary for context observation and model metadata.
//! These types are OCG-owned and do not depend
//! on any external agent runtime.

use serde::{Deserialize, Serialize};

/// Token usage for a context observation.
///
/// Cache reads and writes remain separate fields. The active-context
/// projection is one message's input plus cache-read count, never a sum over a
/// transcript.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextUsage {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl ContextUsage {
    pub fn is_empty(&self) -> bool {
        self.input.is_none()
            && self.output.is_none()
            && self.reasoning.is_none()
            && self.cache_read.is_none()
            && self.cache_write.is_none()
    }

    pub fn active_context_tokens(&self) -> Option<u64> {
        match (self.input, self.cache_read) {
            (None, None) => None,
            (Some(input), None) => Some(input),
            (None, Some(cache_read)) => Some(cache_read),
            (Some(input), Some(cache_read)) => input.checked_add(cache_read),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationProvenance {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

/// Model limits and identity for a normalized context observation.
/// The source is a diagnostic label, never a raw API response.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ObservedModelMetadata {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub context_limit: Option<u64>,
    pub input_limit: Option<u64>,
    pub output_limit: Option<u64>,
    pub effective_limit: Option<u64>,
    pub source: Option<String>,
}

/// OCG-owned context observation consumed by the context governor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextObservation {
    pub execution_id: String,
    pub event_id: String,
    pub observed_at: i64,
    pub assistant_message_id: Option<String>,
    pub finish: Option<String>,
    pub safe_boundary: bool,
    pub usage: ContextUsage,
    pub used_tokens: Option<u64>,
    pub limit_tokens: Option<u64>,
    pub model: ObservedModelMetadata,
    pub message_count: usize,
    pub compaction_count: usize,
    pub usage_provenance: ObservationProvenance,
    pub context_provenance: ObservationProvenance,
    pub note: Option<String>,
}
