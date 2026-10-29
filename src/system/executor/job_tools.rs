use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use super::execution_runtime::{ToolExecution, ToolLoopLimits};
use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
pub(crate) use crate::db::entities::JobPlanReceipt;
use crate::db::entities::ToolEffect;
pub use crate::db::entities::{JobDependency, JobPlanStep, JobToolCall};

pub(crate) fn register_plan(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    registry.register("job_plan", Some("Submit a bounded execution plan. Each step names its prerequisites in depends_on as zero-based indices of earlier steps. Use [] for independent work, which may run concurrently when capacity permits. A dependent step waits for its prerequisites to settle and receives their recorded results. One local step continues directly in the current Job without creating a child or synthesis turn. For multiple steps, the host uses this Agent's frozen permissions, schedules the steps and one synthesis turn. Use one precise scope per step; the host does not grant new tools or targets."), json!({"type":"object","additionalProperties":false,"properties":{"steps":{"type":"array","minItems":1,"maxItems":6,"items":{"type":"object","additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":120},"objective":{"type":"string","minLength":1,"maxLength":2000},"depends_on":{"type":"array","maxItems":5,"uniqueItems":true,"items":{"type":"integer","minimum":0,"maximum":5}}},"required":["title","objective","depends_on"]}}},"required":["steps"]}), Arc::new(HostOnly { version:"job_plan-6".into(), effect:ToolEffect::NonIdempotent }))?;
    registry.authorize("job_plan")
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
                    "List Agent profiles with their instructions summary and model capabilities. Dispatch targets are selected by the user when a dispatch request is made. Follow next_after when has_more is true."
                } else {
                    "List your direct child workers with idle eligibility, Agent profile and Project. Use job_assign only for an idle worker matching the task; otherwise job_dispatch asks the user to select a new execution target. Follow next_after when has_more is true."
                },
            ),
            "job_assign" => (
                json!({"conversation":conversation, "objective":{"type":"string", "minLength":1}}),
                vec!["conversation", "objective"],
                "Assign a task to your own idle, open worker. Eligibility and dispatch authority are checked atomically; a busy, held, recurring or user-managed worker is refused. Results return through the existing child notices. Does not take over unrelated conversations.",
            ),
            "job_dispatch" => (
                json!({
                    "projects": {"type":"array","maxItems":16,"uniqueItems":true,"items":{"type":"string","format":"uuid"}},
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
                vec!["objective"],
                "Request dispatch and pause for the user to choose an Agent or model and projects in the transcript. Each selected project creates one Job. Optional recurrence repeats the objective; dependencies name earlier siblings.",
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
        registry.register(name, Some(description), json!({"type":"object", "additionalProperties":false, "properties":properties, "required":required}), Arc::new(HostOnly { version: format!("{name}-{}", if matches!(name, "job_dispatch" | "job_assign") { 2 } else { 1 }), effect: if matches!(name, "job_agents" | "job_workers") { ToolEffect::ReadOnly } else { ToolEffect::NonIdempotent } }))?;
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
