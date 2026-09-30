//! Runtime provider/model availability checks.
//!
//! Static OCG validation proves that configured model keys and variants are
//! internally coherent. This module performs the separate runtime check: the
//! selected OpenCode executable must currently expose every configured Lead
//! and worker provider/model ID. It uses OpenCode's supported `models` CLI
//! output and never reads provider credential stores.

use crate::error::Result;
use crate::model::{self, ModelRequirement, ModelRequirementKind};
use crate::process::ProcessHost;
use crate::proxy::ChildProxyEnv;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Availability {
    Available,
    MissingProvider,
    MissingModel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelCheck {
    pub requirement: ModelRequirement,
    pub availability: Availability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelPreflight {
    Complete { checks: Vec<ModelCheck> },
    Unavailable { reason: String },
}

impl ModelPreflight {
    /// A missing active Lead is fatal because OpenCode would otherwise be free
    /// to run the OCG Lead identity through an unrelated fallback model.
    pub fn active_lead_failure(&self, active_level: &str) -> Option<String> {
        let agent = model::lead_agent_id(active_level);
        let Self::Complete { checks } = self else {
            return None;
        };
        let check = checks.iter().find(|check| {
            check.requirement.kind == ModelRequirementKind::Lead && check.requirement.agent == agent
        })?;
        match check.availability {
            Availability::Available => None,
            Availability::MissingProvider | Availability::MissingModel => Some(format!(
                "required Lead model {} is not currently exposed by OpenCode; authenticate/configure the provider or adjust OCG configuration",
                check.requirement.full_model_id
            )),
        }
    }

    pub fn missing_non_active_count(&self, active_level: &str) -> usize {
        let active = model::lead_agent_id(active_level);
        match self {
            Self::Complete { checks } => checks
                .iter()
                .filter(|check| {
                    check.availability != Availability::Available
                        && check.requirement.agent != active
                })
                .count(),
            Self::Unavailable { .. } => 0,
        }
    }
}

/// Probe one resolved OpenCode executable with the generated config.
/// A process failure is a non-fatal `Unavailable` result; callers decide
/// whether to warn (launch) or display an informational diagnostic (doctor).
pub fn probe(
    data: &Value,
    selected: Option<&str>,
    process: &dyn ProcessHost,
    program: &Path,
    cwd: &Path,
    config_content: &str,
    proxy: &ChildProxyEnv,
) -> Result<ModelPreflight> {
    let requirements = model::runtime_model_requirements(data, selected)?;
    let output = match process.models(program, cwd, config_content, proxy) {
        Ok(output) => output,
        Err(_) => {
            return Ok(ModelPreflight::Unavailable {
                reason: "runtime model check could not be completed (`opencode models` failed)"
                    .to_string(),
            })
        }
    };
    Ok(check_output(requirements, &output))
}

pub fn check_output(requirements: Vec<ModelRequirement>, output: &str) -> ModelPreflight {
    let available: BTreeSet<String> = model_tokens(output);
    let providers: BTreeSet<&str> = available
        .iter()
        .filter_map(|model| model.split_once('/').map(|(provider, _)| provider))
        .collect();
    let checks = requirements
        .into_iter()
        .map(|requirement| {
            let availability = if available.contains(&requirement.full_model_id) {
                Availability::Available
            } else {
                let provider = requirement
                    .full_model_id
                    .split_once('/')
                    .map(|(provider, _)| provider)
                    .unwrap_or("");
                if providers.contains(provider) {
                    Availability::MissingModel
                } else {
                    Availability::MissingProvider
                }
            };
            ModelCheck {
                requirement,
                availability,
            }
        })
        .collect();
    ModelPreflight::Complete { checks }
}

/// Every `provider/model` token in the catalogue output.
///
/// `opencode models` prints exactly `provider/model`, one per line. Matching on
/// the token instead of the whole line keeps the check strict (a requirement is
/// available only when that exact id is present) while tolerating decoration a
/// future runtime or terminal wrapper might add: ANSI colours, table borders,
/// trailing punctuation or a trailing description column. A parsed token can
/// only ever make a required id *available*, never silently satisfy a different
/// one, so this cannot produce a false "missing".
fn model_tokens(output: &str) -> BTreeSet<String> {
    strip_ansi(output)
        .split_whitespace()
        .map(|raw| raw.trim_matches(|c: char| !is_model_char(c)))
        .filter(|token| is_model_id(token))
        .map(str::to_string)
        .collect()
}

fn is_model_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '-' | '_')
}

fn is_model_id(token: &str) -> bool {
    match token.split_once('/') {
        Some((provider, model)) => {
            !provider.is_empty() && !model.is_empty() && !model.contains('/')
        }
        None => false,
    }
}

/// Remove ANSI escape sequences (CSI `ESC [ ... final-byte`) so a coloured
/// catalogue still yields clean tokens. No other interpretation is attempted.
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(current) = chars.next() {
        if current != '\u{1b}' {
            out.push(current);
            continue;
        }
        if chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if ('@'..='~').contains(&next) {
                    break;
                }
            }
        }
    }
    out
}
