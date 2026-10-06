//! Token counts and where they came from.
//!
//! OCG never invents a token number. A count is either reported by a
//! provider-reported, explicitly estimated (for example bytes / 4)
//! or unknown. [`TokenSource`] is part of the persisted schema so a later budget
//! or routing feature can tell a measured number from a guess, and the
//! `ocg stats` renderer can label estimates as estimates.

use serde::{Deserialize, Serialize};

/// Where a token count came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenSource {
    /// A real number reported by the model provider.
    ProviderReported,
    /// A deterministic estimate (for example context bytes / 4).
    Estimated,
    /// No number is available. The count is explicit `null`, never zero.
    Unknown,
}

impl TokenSource {
    /// A stable, human-readable label.
    pub fn label(self) -> &'static str {
        match self {
            TokenSource::ProviderReported => "provider_reported",
            TokenSource::Estimated => "estimated",
            TokenSource::Unknown => "unknown",
        }
    }

    /// Whether the number is an exact measurement rather than an estimate.
    pub fn is_exact(self) -> bool {
        matches!(self, TokenSource::ProviderReported)
    }
}

/// One token count with its provenance. `Unknown` always carries `None` and a
/// reported/estimated count always carries `Some`, so a missing number is never
/// silently read as zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCount {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    pub source: TokenSource,
}

impl Default for TokenCount {
    fn default() -> Self {
        Self::unknown()
    }
}

impl TokenCount {
    /// No number is available.
    pub fn unknown() -> Self {
        Self {
            total: None,
            source: TokenSource::Unknown,
        }
    }

    /// An explicit estimate.
    pub fn estimated(total: u64) -> Self {
        Self {
            total: Some(total),
            source: TokenSource::Estimated,
        }
    }

    /// A provider-reported measurement.
    pub fn provider_reported(total: u64) -> Self {
        Self {
            total: Some(total),
            source: TokenSource::ProviderReported,
        }
    }

    /// The `selected_bytes / 4` estimate this repository already uses for
    /// context, kept in one place so it stays clearly an estimate.
    pub fn estimate_bytes(bytes: u64) -> Self {
        Self::estimated(bytes.div_ceil(4))
    }

    /// Whether the count is a real, reported measurement.
    pub fn is_exact(self) -> bool {
        self.source.is_exact() && self.total.is_some()
    }

    /// Whether the count is an explicit estimate.
    pub fn is_estimated(self) -> bool {
        self.source == TokenSource::Estimated && self.total.is_some()
    }
}
