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
    JobLimits {
        conversation: Option<String>,
        max_depth: Option<u8>,
        max_auto_turns: Option<u8>,
        max_iterations: Option<u32>,
    },
    JobAgents {
        after: Option<String>,
        limit: Option<u32>,
    },
    JobWorkers {
        after: Option<String>,
        limit: Option<u32>,
    },
    JobAssign {
        conversation: String,
        objective: String,
    },
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

pub(crate) const NAMES: [&str; 11] = [
    "job_limits",
    "job_agents",
    "job_workers",
    "job_assign",
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
    effect: ToolEffect,
}
impl ToolHandler for HostOnly {
    fn version(&self) -> &str {
        &self.version
    }
    fn effect(&self) -> ToolEffect {
        self.effect
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
    let conversation = json!({"type":"string", "format":"uuid", "pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", "description":"A child conversation id (conversation from job_dispatch, or child from a notice), never an answered turn id."});
    let profile = json!({"type":"string", "format":"uuid", "pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"});
    for name in NAMES {
        let (properties, required, description) = match name {
            "job_limits" => (
                json!({"conversation":conversation, "max_depth":{"type":"integer","minimum":0,"maximum":16}, "max_auto_turns":{"type":"integer","minimum":0,"maximum":255}, "max_iterations":{"type":"integer","minimum":0,"maximum":10000}}),
                vec![],
                "Read your conversation limits or tighten limits on your own conversation or descendants. max_depth is the absolute tree depth (root=0); defaults are 3 child levels, 8 automatic follow-up turns and 100 scheduled iterations. Agents cannot raise effective limits; the user controls the ceiling. Omit settings to read only. A limit of 0 disables that kind of new work.",
            ),
            "job_agents" | "job_workers" => (
                json!({
                    "after": if name == "job_agents" { profile.clone() } else { conversation.clone() },
                    "limit": {"type":"integer", "minimum":1, "maximum":20}
                }),
                vec![],
                if name == "job_agents" {
                    "List allowed Agent profiles with their instructions summary, tool grant and model capabilities. Choose a suitable profile by its declared strengths; this does not grant new authority. Follow next_after when has_more is true."
                } else {
                    "List your direct child workers with idle eligibility, Agent profile and Project. Use job_assign only for an idle worker matching the task; otherwise job_dispatch creates a child from an allowed Agent. Follow next_after when has_more is true."
                },
            ),
            "job_assign" => (
                json!({"conversation":conversation, "objective":{"type":"string", "minLength":1}}),
                vec!["conversation", "objective"],
                "Assign a task to your own idle, open worker. Eligibility and dispatch authority are checked atomically; a busy, held, recurring or user-managed worker is refused. Results return through the existing child notices. Does not take over unrelated conversations.",
            ),
            "job_dispatch" => (
                json!({
                    "agent": profile,
                    "project": {"type":"string", "format":"uuid", "pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"},
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
                json!({"turn":{"type":"string","pattern":"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$","description":"The original turn id to retry, not its conversation id."}}),
                vec!["turn"],
                "Retry a descendant turn, at most twice per original turn.",
            ),
            _ => (
                json!({"conversation":conversation}),
                vec!["conversation"],
                "Manage a conversation in your strict subtree. Queued or running user messages prevent agent management.",
            ),
        };
        registry.register(name, Some(description), json!({"type":"object", "additionalProperties":false, "properties":properties, "required":required}), Arc::new(HostOnly { version: format!("{name}-1"), effect: if matches!(name, "job_agents" | "job_workers") { ToolEffect::ReadOnly } else { ToolEffect::NonIdempotent } }))?;
        registry.authorize(name)?;
    }
    Ok(())
}

pub(crate) fn register_ask_user(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    registry.register("ask_user", Some("Ask the deciding user a question and wait for their answer."), json!({"type":"object", "additionalProperties":false, "properties":{"question":{"type":"string", "minLength":1}}, "required":["question"]}), Arc::new(HostOnly { version:"ask_user-1".into(), effect: ToolEffect::NonIdempotent }))?;
    registry.authorize("ask_user")
}

pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    ToolLoopLimits {
        handler_timeout: timeout.saturating_add(super::blocking::STOP_GRACE),
        // Twice the read cap and 8 KiB more, so a 32 KiB file whose every
        // byte escapes to two (quotes, backslashes, line ends) still fits
        // with its keys and a path of up to 1 KiB, even escaped.
        max_tool_result_bytes: 72 * 1024,
        ..ToolLoopLimits::default()
    }
}

pub(crate) fn add(
    tools: Option<ToolExecution>,
    dispatch: bool,
    ask_user: bool,
) -> Result<ToolExecution, ToolRegistryError> {
    let mut tools = match tools {
        Some(tools) => tools,
        None => ToolExecution {
            approve: Vec::new(),
            decider: false,
            offer: ToolRegistry::default().offer(&[])?,
            limits: coding_limits(COMMAND_TIMEOUT),
        },
    };
    if dispatch {
        tools.offer = tools.offer.with_job_tools()?;
    }
    if ask_user {
        tools.offer = tools.offer.with_ask_user()?;
    }
    Ok(tools)
}

impl JobToolCall {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::JobLimits { .. } => "job_limits",
            Self::JobAgents { .. } => "job_agents",
            Self::JobWorkers { .. } => "job_workers",
            Self::JobAssign { .. } => "job_assign",
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
