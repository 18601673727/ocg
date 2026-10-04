use super::{openai_projection::OpenAiToolProjection, NativeToolRegistry};
use crate::error::{OcgError, Result};
use crate::provider_protocol::ProviderProtocol;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolProjectionProfile {
    #[default]
    Full,
    Context,
    Coding,
    NoTools,
}

impl ToolProjectionProfile {
    fn includes(self, canonical_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Context => matches!(
                canonical_name,
                "context.search" | "context.read" | "context.snapshot" | "context.validation"
            ),
            Self::Coding => matches!(
                canonical_name,
                "context.search"
                    | "context.read"
                    | "filesystem.list"
                    | "filesystem.read"
                    | "filesystem.edit"
                    | "filesystem.search"
                    | "process.exec"
            ),
            Self::NoTools => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolProjectionFacts {
    pub profile: ToolProjectionProfile,
    pub visible_tool_names: Vec<String>,
    pub projected_tool_count: usize,
    pub projected_schema_bytes: usize,
    pub full_schema_baseline_bytes: usize,
    pub schema_bytes_saved: usize,
    pub projection_us: u64,
}

pub(crate) fn project(
    protocol: ProviderProtocol,
    profile: ToolProjectionProfile,
) -> Result<(Vec<Value>, ToolProjectionFacts)> {
    let started = Instant::now();
    let all = if protocol.is_openai_chat_completions() {
        OpenAiToolProjection::from_registry()?.tools()
    } else {
        NativeToolRegistry::definitions()
            .into_iter()
            .map(|definition| {
                json!({
                    "type": "function",
                    "function": {
                        "name": definition.name.replace('.', "_"),
                        "description": definition.description,
                        "parameters": definition.parameters,
                    },
                })
            })
            .collect()
    };
    let full_schema_baseline_bytes = schema_bytes(&all)?;
    // Filtering preserves the registry's existing order and each schema's exact
    // serialization; profiles never rewrite definitions or permissions.
    let allowed_names = NativeToolRegistry::definitions()
        .into_iter()
        .filter(|definition| profile.includes(definition.name))
        .map(|definition| definition.name.replace('.', "_"))
        .collect::<Vec<_>>();
    let selected = all
        .into_iter()
        .filter(|tool| {
            tool["function"]["name"]
                .as_str()
                .is_some_and(|name| allowed_names.iter().any(|allowed| allowed == name))
        })
        .collect::<Vec<_>>();
    let projected_schema_bytes = schema_bytes(&selected)?;
    let facts = ToolProjectionFacts {
        profile,
        visible_tool_names: names(&selected)?,
        projected_tool_count: selected.len(),
        projected_schema_bytes,
        full_schema_baseline_bytes,
        schema_bytes_saved: full_schema_baseline_bytes.saturating_sub(projected_schema_bytes),
        projection_us: started.elapsed().as_micros().min(u64::MAX as u128) as u64,
    };
    Ok((selected, facts))
}

fn names(tools: &[Value]) -> Result<Vec<String>> {
    tools
        .iter()
        .map(|tool| {
            tool["function"]["name"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| OcgError::config("invalid frozen tool schema name"))
        })
        .collect()
}

fn schema_bytes(tools: &[Value]) -> Result<usize> {
    if tools.is_empty() {
        return Ok(0);
    }
    serde_json::to_vec(tools)
        .map(|bytes| bytes.len())
        .map_err(|error| OcgError::config(format!("serialize tool projection: {error}")))
}

pub(crate) fn frozen(input: &Value) -> Result<Option<ToolProjectionFacts>> {
    let Some(value) = input.get("tool_projection") else {
        return Ok(None);
    };
    let facts: ToolProjectionFacts = serde_json::from_value(value.clone())
        .map_err(|error| OcgError::config(format!("invalid frozen tool projection: {error}")))?;
    let tools = input["arguments"]["tools"]
        .as_array()
        .ok_or_else(|| OcgError::config("frozen tool projection has no schemas"))?;
    let expected_names = NativeToolRegistry::definitions()
        .into_iter()
        .filter(|definition| facts.profile.includes(definition.name))
        .map(|definition| definition.name.replace('.', "_"))
        .collect::<Vec<_>>();
    if tools.len() != facts.projected_tool_count
        || facts.visible_tool_names != expected_names
        || names(tools)? != facts.visible_tool_names
        || schema_bytes(tools)? != facts.projected_schema_bytes
        || facts.projected_schema_bytes > facts.full_schema_baseline_bytes
        || facts.schema_bytes_saved
            != facts.full_schema_baseline_bytes - facts.projected_schema_bytes
    {
        return Err(OcgError::config(
            "frozen tool projection facts differ from schemas",
        ));
    }
    Ok(Some(facts))
}
