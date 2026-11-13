//! The job tools of an Agent that manages Jobs: what the model is offered,
//! and how each call is told apart from a tool a handler runs.
//!
//! A job tool has no handler of its own. What it does is a change to the
//! conversation tree, so the database applies it in one transaction that
//! also records the call's outcome (see [`crate::db::DatabaseHandle::apply_development_job_tool`]),
//! and a call that spends the user's choice waits for it. The registration
//! here states the schema the model is held to and what the call may change.

use std::sync::Arc;

use serde_json::{Value, json};

use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use crate::db::entities::ToolEffect;

const UUID: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

/// The tools that change the tree and the ones that only read it, which
/// store just the size and digest of their result.
const READS: [&str; 2] = ["job_agents", "job_workers"];

/// The tools whose call waits for the user.
pub(crate) const USER_DECIDED: [&str; 2] = ["job_assign", "job_dispatch"];

/// A job tool's registration: it states the schema and the effect, and never
/// runs.
struct Applied {
    version: String,
    effect: ToolEffect,
}

impl ToolHandler for Applied {
    fn version(&self) -> &str {
        &self.version
    }
    fn effect(&self) -> ToolEffect {
        self.effect
    }
    fn call(&self, _arguments: Value) -> ToolFuture<'_> {
        Box::pin(async {
            Err(ToolFailure::not_applied(
                "a job tool is applied by the database, not run by a handler",
            ))
        })
    }
}

/// Registers the job tools, and returns the names offered. The tools the user
/// decides are offered only when a user decides for the execution.
pub(crate) fn register(
    registry: &ToolRegistry,
    decider: bool,
) -> Result<Vec<&'static str>, ToolRegistryError> {
    let conversation = json!({"type":"string","format":"uuid","pattern":UUID,"description":"A child conversation id (the conversation from job_dispatch or job_plan, or a worker from job_workers), never an answered turn id."});
    let profile = json!({"type":"string","format":"uuid","pattern":UUID});
    let mut names = Vec::new();
    let plan = (
        "job_plan",
        json!({"steps":{"type":"array","minItems":1,"maxItems":6,"items":{"type":"object","additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":120},"objective":{"type":"string","minLength":1,"maxLength":2000},"depends_on":{"type":"array","maxItems":5,"uniqueItems":true,"items":{"type":"integer","minimum":0,"maximum":5}}},"required":["title","objective","depends_on"]}}}),
        vec!["steps"],
        "Submit a bounded execution plan. Each step names its prerequisites in depends_on as zero-based indices of earlier steps. Use [] for independent work, which may run concurrently when capacity permits. A dependent step waits for its prerequisites to settle. One local step continues directly in the current Job without creating a child conversation. For several steps, the host opens one child conversation per step with this Agent's own permissions and, once every step has settled, adds their results to this conversation as a message. Use one precise scope per step; a plan must come before any other work and cannot be made by one of its own steps.",
    );
    let mut specs = vec![plan];
    specs.push((
        "job_limits",
        json!({"conversation":conversation,"max_depth":{"type":"integer","minimum":0,"maximum":16},"max_auto_turns":{"type":"integer","minimum":0,"maximum":255},"max_iterations":{"type":"integer","minimum":0,"maximum":10000}}),
        vec![],
        "Read your conversation limits or tighten limits on your own conversation or its descendants. max_depth is the absolute tree depth (root=0); defaults are 3 child levels, 8 automatic follow-up turns and 100 scheduled iterations. Agents cannot raise effective limits; the user controls the ceiling. Omit settings to read only. A limit of 0 disables that kind of new work.",
    ));
    specs.push((
        "job_agents",
        json!({"after":profile,"limit":{"type":"integer","minimum":1,"maximum":20}}),
        vec![],
        "List Agent profiles with their instructions summary and model capabilities. Dispatch targets are selected by the user when a dispatch request is made. Follow next_after when has_more is true.",
    ));
    specs.push((
        "job_workers",
        json!({"after":conversation,"limit":{"type":"integer","minimum":1,"maximum":20}}),
        vec![],
        "List your direct child workers with idle eligibility, Agent profile and Project. Use job_assign only for an idle worker matching the task; otherwise job_dispatch asks the user to select a new execution target. Follow next_after when has_more is true.",
    ));
    if decider {
        specs.push((
            "job_assign",
            json!({"conversation":conversation,"objective":{"type":"string","minLength":1}}),
            vec!["conversation", "objective"],
            "Assign a task to your own idle, open worker, after the user confirms. Eligibility is checked when the user confirms; a busy, held or user-managed worker is refused. The worker's result returns to this conversation as a message.",
        ));
        specs.push((
            "job_dispatch",
            json!({
                "projects":{"type":"array","maxItems":16,"uniqueItems":true,"items":{"type":"string","format":"uuid"}},
                "project":{"type":"string","format":"uuid","pattern":UUID},
                "objective":{"type":"string","minLength":1},
                "keep_open":{"type":"boolean"},
                "every_seconds":{"type":"integer","minimum":60,"maximum":2592000},
                "depends_on":{"type":"array","maxItems":16,"items":{"type":"object","additionalProperties":false,"properties":{"conversation":conversation,"condition":{"type":"string","enum":["settled","success"]}},"required":["conversation","condition"]}}
            }),
            vec!["objective"],
            "Request dispatch and pause for the user to choose an Agent or model and projects in the transcript. Each selected project creates one child conversation. Dependencies name earlier sibling conversations. The results return to this conversation as a message once the children settle.",
        ));
    }
    specs.push((
        "job_message",
        json!({"conversation":conversation,"text":{"type":"string","minLength":1}}),
        vec!["conversation", "text"],
        "Message a conversation in your strict subtree.",
    ));
    specs.push((
        "job_retry",
        json!({"turn":{"type":"string","pattern":UUID,"description":"The original turn id to retry, not its conversation id."}}),
        vec!["turn"],
        "Retry a descendant turn, at most twice per original turn.",
    ));
    for (name, description) in [
        (
            "job_stop",
            "Stop the queued or running turns of a conversation in your strict subtree. Queued or running user messages prevent agent management.",
        ),
        (
            "job_close",
            "Close a conversation in your strict subtree and everything below it. Queued or running user messages prevent agent management.",
        ),
        (
            "job_hold",
            "Hold a conversation in your strict subtree: its queued turns wait until it is unheld. Queued or running user messages prevent agent management.",
        ),
        (
            "job_unhold",
            "Release a hold you placed on a conversation in your strict subtree. Queued or running user messages prevent agent management.",
        ),
    ] {
        specs.push((
            name,
            json!({"conversation":conversation}),
            vec!["conversation"],
            description,
        ));
    }
    for (name, properties, required, description) in specs {
        let version = if USER_DECIDED.contains(&name) {
            2
        } else if name == "job_plan" {
            6
        } else {
            1
        };
        registry.register(
            name,
            Some(description),
            json!({"type":"object","additionalProperties":false,"properties":properties,"required":required}),
            Arc::new(Applied {
                version: format!("{name}-{version}"),
                effect: if READS.contains(&name) {
                    ToolEffect::ReadOnly
                } else {
                    ToolEffect::NonIdempotent
                },
            }),
        )?;
        registry.authorize(name)?;
        names.push(name);
    }
    Ok(names)
}
