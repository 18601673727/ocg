use std::sync::{Arc, LazyLock};

use serde_json::{Value, json};

use super::registry::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, ToolSpec,
};
use crate::db::entities::ToolEffect;

const UUID: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

const READS: [&str; 2] = ["job_agents", "job_workers"];

pub(crate) const USER_DECIDED: [&str; 2] = ["job_assign", "job_dispatch"];

#[derive(Clone)]
struct Applied {
    version: Arc<str>,
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

struct JobTool {
    name: &'static str,
    spec: Arc<ToolSpec>,
    handler: Applied,
}

fn job_tools() -> Result<Vec<JobTool>, ToolRegistryError> {
    let conversation = json!({"type":"string","format":"uuid","pattern":UUID,"description":"A child conversation id (the conversation from job_dispatch or job_plan, or a worker from job_workers), never an answered turn id."});
    let profile = json!({"type":"string","format":"uuid","pattern":UUID});
    let mut specs = vec![
        (
            "job_plan",
            json!({"steps":{"type":"array","minItems":1,"maxItems":6,"items":{"type":"object","additionalProperties":false,"properties":{"title":{"type":"string","minLength":1,"maxLength":120},"objective":{"type":"string","minLength":1,"maxLength":2000},"depends_on":{"type":"array","maxItems":5,"uniqueItems":true,"items":{"type":"integer","minimum":0,"maximum":5}}},"required":["title","objective","depends_on"]}}}),
            vec!["steps"],
            "Submit a bounded execution plan. Each step names its prerequisites in depends_on as zero-based indices of earlier steps. Use [] for independent work, which may run concurrently when capacity permits. A dependent step waits for its prerequisites to settle. One local step continues directly in the current Job without creating a child conversation. For several steps, the host opens one child conversation per step with this Agent's own permissions and, once every step has settled, adds their results to this conversation as a message. Use one precise scope per step; a plan must come before any other work and cannot be made by one of its own steps.",
        ),
        (
            "job_limits",
            json!({"conversation":conversation,"max_depth":{"type":"integer","minimum":0,"maximum":16},"max_auto_turns":{"type":"integer","minimum":0,"maximum":255},"max_iterations":{"type":"integer","minimum":0,"maximum":10000}}),
            vec![],
            "Read your conversation limits or tighten limits on your own conversation or its descendants. max_depth is the absolute tree depth (root=0); defaults are 3 child levels, 8 automatic follow-up turns and 100 scheduled iterations. Agents cannot raise effective limits; the user sets the ceiling. Omit settings to read only. A limit of 0 disables that kind of new work.",
        ),
        (
            "job_agents",
            json!({"after":profile,"limit":{"type":"integer","minimum":1,"maximum":20}}),
            vec![],
            "List Agent profiles with their instructions summary and model capabilities. Dispatch targets are selected by the user when a dispatch request is made. Follow next_after when has_more is true.",
        ),
        (
            "job_workers",
            json!({"after":conversation,"limit":{"type":"integer","minimum":1,"maximum":20}}),
            vec![],
            "List your direct child workers with idle eligibility, Agent profile and Project. Use job_assign only for an idle worker matching the task; otherwise job_dispatch asks the user to select a new execution target. Follow next_after when has_more is true.",
        ),
        (
            "job_assign",
            json!({"conversation":conversation,"objective":{"type":"string","minLength":1}}),
            vec!["conversation", "objective"],
            "Assign a task to your own idle, open worker, after the user confirms. Eligibility is checked when the user confirms; a busy, held or user-managed worker is refused. The worker's result returns to this conversation as a message.",
        ),
        (
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
        ),
        (
            "job_message",
            json!({"conversation":conversation,"text":{"type":"string","minLength":1}}),
            vec!["conversation", "text"],
            "Message a conversation in your strict subtree.",
        ),
        (
            "job_retry",
            json!({"turn":{"type":"string","pattern":UUID,"description":"The original turn id to retry, not its conversation id."}}),
            vec!["turn"],
            "Retry a descendant turn, at most twice per original turn.",
        ),
    ];
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
    let mut tools = Vec::with_capacity(specs.len());
    for (name, properties, required, description) in specs {
        let version = if USER_DECIDED.contains(&name) {
            2
        } else if name == "job_plan" {
            6
        } else {
            1
        };
        tools.push(JobTool {
            name,
            spec: ToolSpec::compile(
                name,
                Some(description),
                json!({"type":"object","additionalProperties":false,"properties":properties,"required":required}),
            )?,
            handler: Applied {
                version: format!("{name}-{version}").into(),
                effect: if READS.contains(&name) {
                    ToolEffect::ReadOnly
                } else {
                    ToolEffect::NonIdempotent
                },
            },
        });
    }
    Ok(tools)
}

pub(crate) fn register(
    registry: &ToolRegistry,
    decider: bool,
    offer_plan: bool,
) -> Result<Vec<&'static str>, ToolRegistryError> {
    static TOOLS: LazyLock<Result<Vec<JobTool>, ToolRegistryError>> = LazyLock::new(job_tools);
    let mut names = Vec::new();
    for tool in TOOLS.as_ref().map_err(Clone::clone)? {
        if (!offer_plan && tool.name == "job_plan")
            || (!decider && USER_DECIDED.contains(&tool.name))
        {
            continue;
        }
        registry.register_spec(tool.spec.clone(), Arc::new(tool.handler.clone()))?;
        registry.authorize(tool.name)?;
        names.push(tool.name);
    }
    Ok(names)
}
