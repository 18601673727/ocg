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
    /// The canonical tool names this profile can select.
    ///
    /// This list is part of the projection contract itself, not of the current
    /// registry, so validating a frozen projection against it stays stable across
    /// binary upgrades that change which Native Tools exist.
    fn canonical_names(self) -> &'static [&'static str] {
        match self {
            Self::Full => &["*"],
            Self::Context => &[
                "context.search",
                "context.read",
                "context.snapshot",
                "context.validation",
            ],
            Self::Coding => &[
                "context.search",
                "context.read",
                "filesystem.list",
                "filesystem.read",
                "filesystem.read_many",
                "filesystem.edit",
                "filesystem.search",
                "process.exec",
            ],
            Self::NoTools => &[],
        }
    }

    fn includes(self, canonical_name: &str) -> bool {
        let names = self.canonical_names();
        names.contains(&"*") || names.contains(&canonical_name)
    }

    /// Whether every wire-format name in `wire_names` is selectable by this
    /// profile, judged against the profile contract rather than the registry.
    ///
    /// This deliberately does not consult [`NativeToolRegistry`]: the profile's
    /// own name list is the stable part of the contract, so a frozen projection
    /// stays valid when an upgrade changes which Native Tools exist.
    fn includes_name(self, wire_names: &[String]) -> bool {
        if self == Self::Full {
            // `Full` projects the whole registry, so the only decidable
            // property is that each name is a well-formed tool name.
            return wire_names.iter().all(|name| is_wellformed_tool_name(name));
        }
        // A wire name is its canonical name with `.` replaced by `_`, so the
        // profile's canonical names project to exactly the names it may freeze.
        let allowed = self
            .canonical_names()
            .iter()
            .map(|name| name.replace('.', "_"))
            .collect::<Vec<_>>();
        wire_names
            .iter()
            .all(|name| allowed.iter().any(|allowed| allowed == name))
    }
}

/// A wire tool name must be a non-empty, bounded, `[a-z0-9_]` identifier.
fn is_wellformed_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
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

/// Validates an already-admitted Call's frozen tool projection.
///
/// Recovery trusts the frozen admission facts and validates them *internally*:
/// the frozen schemas are the only authority for what this Call may carry. The
/// current binary's [`NativeToolRegistry`] is deliberately not consulted here.
/// An upgrade that adds or drops a Native Tool changes the registry for NEW
/// admissions, but it must not retroactively invalidate a Call that was already
/// admitted against a different registry, because that would make a valid frozen
/// Call unreplayable after an ordinary binary upgrade.
///
/// Malformed or tampered frozen data still fails closed: the schemas must be
/// well-formed tool schemas, and every recorded count, name, and byte total must
/// agree with the frozen schemas themselves.
pub(crate) fn frozen(input: &Value) -> Result<Option<ToolProjectionFacts>> {
    let Some(value) = input.get("tool_projection") else {
        return Ok(None);
    };
    let facts: ToolProjectionFacts = serde_json::from_value(value.clone())
        .map_err(|error| OcgError::config(format!("invalid frozen tool projection: {error}")))?;
    let tools = input["arguments"]["tools"]
        .as_array()
        .ok_or_else(|| OcgError::config("frozen tool projection has no schemas"))?;
    let frozen_names = names(tools)?;
    // Self-consistency of the frozen record: the recorded facts must describe
    // exactly the schemas this Call froze.
    let unique = {
        let mut unique = frozen_names.clone();
        unique.sort();
        unique.dedup();
        unique.len() == frozen_names.len()
    };
    let consistent = unique
        && frozen_names.len() == facts.projected_tool_count
        && facts.projected_tool_count == tools.len()
        && facts.visible_tool_names == frozen_names
        && schema_bytes(tools)? == facts.projected_schema_bytes
        && facts.projected_schema_bytes <= facts.full_schema_baseline_bytes
        && facts.schema_bytes_saved
            == facts
                .full_schema_baseline_bytes
                .saturating_sub(facts.projected_schema_bytes);
    // Every frozen name must be one this profile is allowed to project, judged
    // against the profile contract rather than the current registry.
    let profile_consistent = facts.profile.includes_name(&frozen_names);
    if !consistent || !profile_consistent {
        return Err(OcgError::config(
            "frozen tool projection facts differ from schemas",
        ));
    }
    Ok(Some(facts))
}
