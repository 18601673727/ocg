//! Bounded, credential-free provider attempt facts in the replay authority.
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};

pub const MAX_DISPATCHES: usize = 1024;

/// Unique identity for one potentially chargeable network attempt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DispatchId(String);

impl DispatchId {
    pub fn new(value: String) -> Result<Self> {
        if value.len() < 8
            || value.len() > 80
            || !value.starts_with("dsp-")
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(OcgError::config("invalid dispatch identity"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchState {
    Reserved,
    DispatchStarted,
    Completed,
    KnownNotDispatched,
    Failed,
    Unresolved,
    Settled,
}

impl DispatchState {
    pub fn is_live(self) -> bool {
        matches!(
            self,
            Self::Reserved | Self::DispatchStarted | Self::Unresolved | Self::Completed
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub provenance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchRecord {
    pub id: DispatchId,
    pub mission_id: String,
    pub generation: u32,
    pub logical_operation: String,
    pub execution_id: String,
    pub root_id: String,
    pub provider: String,
    pub model: String,
    pub reservation_id: Option<String>,
    pub state: DispatchState,
    pub created_at: i64,
    pub updated_at: i64,
    pub usage: Option<DispatchUsage>,
    pub cost_provenance: String,
    pub failure_class: Option<String>,
}

impl DispatchRecord {
    pub fn validate(&self) -> Result<()> {
        DispatchId::new(self.id.0.clone())?;
        for field in [
            &self.mission_id,
            &self.logical_operation,
            &self.execution_id,
            &self.root_id,
            &self.provider,
            &self.model,
            &self.cost_provenance,
        ] {
            if field.is_empty() || field.len() > 240 || field.bytes().any(|b| b.is_ascii_control())
            {
                return Err(OcgError::config("invalid bounded dispatch field"));
            }
        }
        if self.failure_class.as_ref().is_some_and(|v| v.len() > 80)
            || self.logical_operation.len() > 80
            || self.reservation_id.as_ref().is_some_and(|v| v.len() > 80)
            || self.usage.as_ref().is_some_and(|v| v.provenance.len() > 80)
        {
            return Err(OcgError::config("dispatch metadata exceeds bound"));
        }
        Ok(())
    }
}
