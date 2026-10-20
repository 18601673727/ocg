use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use super::execution_runtime::{ToolExecution, ToolLoopLimits};
use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use crate::persistence::ToolEffect;

#[derive(Clone, Debug, Deserialize)]
#[serde(
    tag = "name",
    content = "arguments",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum JobToolCall {
    JobDispatch {
        agent: String,
        project: Option<String>,
        objective: String,
        #[serde(default)]
        keep_open: bool,
        every_seconds: Option<u32>,
        #[serde(default)]
        depends_on: Vec<JobDependency>,
    },
    JobMessage {
        conversation: String,
        text: String,
    },
    JobStop {
        conversation: String,
    },
    JobClose {
        conversation: String,
    },
    JobHold {
        conversation: String,
    },
    JobUnhold {
        conversation: String,
    },
    JobRetry {
        turn: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobDependency {
    pub conversation: String,
    pub condition: String,
}

pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) const NAMES: [&str; 7] = [
    "job_dispatch",
    "job_message",
    "job_stop",
    "job_close",
    "job_hold",
    "job_unhold",
    "job_retry",
];

struct HostOnly {
    version: String,
}
impl ToolHandler for HostOnly {
    fn version(&self) -> &str {
        &self.version
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }
    fn call(&self, _arguments: Value) -> ToolFuture<'_> {
        Box::pin(async {
            Err(ToolFailure::not_applied(
                "host effects require a database transaction",
            ))
        })
    }
}

pub(crate) fn register(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    let conversation = json!({"type":"string", "pattern":"^job:[0-9]+$", "description":"A child conversation id (conversation from job_dispatch, or child from a notice), never an answered turn id."});
    let profile = json!({"type":"string", "pattern":"^profile:[0-9]+$"});
    for name in NAMES {
        let (properties, required, description) = match name {
            "job_dispatch" => (
                json!({
                    "agent": profile,
                    "project": {"type":"string", "pattern":"^project:[0-9]+$"},
                    "objective": {"type":"string", "minLength":1},
                    "keep_open": {"type":"boolean"},
                    "every_seconds": {"type":"integer", "minimum":60, "maximum":2592000},
                    "depends_on": {"type":"array", "maxItems":16, "items": {
                        "type":"object", "additionalProperties":false,
                        "properties": {"conversation":conversation, "condition":{"type":"string", "enum":["settled","success"]}},
                        "required":["conversation","condition"]
                    }}
                }),
                vec!["agent", "objective"],
                "Dispatch a child conversation to an allowed Agent. Optional recurrence repeats the objective; dependencies name earlier siblings.",
            ),
            "job_message" => (
                json!({"conversation":conversation, "text":{"type":"string", "minLength":1}}),
                vec!["conversation", "text"],
                "Message a conversation in your strict subtree.",
            ),
            "job_retry" => (
                json!({"turn":{"type":"string","pattern":"^job:[0-9]+$","description":"The original turn id to retry, not its conversation id."}}),
                vec!["turn"],
                "Retry a descendant turn, at most twice per original turn.",
            ),
            _ => (
                json!({"conversation":conversation}),
                vec!["conversation"],
                "Manage a conversation in your strict subtree. Queued or running user messages prevent agent management.",
            ),
        };
        registry.register(name, Some(description), json!({"type":"object", "additionalProperties":false, "properties":properties, "required":required}), Arc::new(HostOnly { version: format!("{name}-1") }))?;
        registry.authorize(name)?;
    }
    Ok(())
}

pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    ToolLoopLimits {
        handler_timeout: timeout.saturating_add(super::blocking::STOP_GRACE),
        ..ToolLoopLimits::default()
    }
}

pub(crate) fn add(tools: Option<ToolExecution>) -> Result<ToolExecution, ToolRegistryError> {
    let mut tools = match tools {
        Some(tools) => tools,
        None => ToolExecution {
            offer: ToolRegistry::default().offer(&[])?,
            limits: coding_limits(COMMAND_TIMEOUT),
        },
    };
    tools.offer = tools.offer.with_job_tools()?;
    Ok(tools)
}

impl JobToolCall {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::JobDispatch { .. } => "job_dispatch",
            Self::JobMessage { .. } => "job_message",
            Self::JobStop { .. } => "job_stop",
            Self::JobClose { .. } => "job_close",
            Self::JobHold { .. } => "job_hold",
            Self::JobUnhold { .. } => "job_unhold",
            Self::JobRetry { .. } => "job_retry",
        }
    }
}
