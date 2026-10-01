//! Skill activation: deciding which bodies a request carries.
//!
//! ## Activation is a projection decision, not an authority decision
//!
//! Activation means "this skill's prose is in the request". It does not mean the
//! skill ran, and it grants nothing. A skill whose declared tools are missing is
//! still activated — with the gap reported — because silently withholding a
//! skill would leave the model with no explanation for why a capability it was
//! told about is unavailable.
//!
//! ## The order matters, and it is not alphabetical
//!
//! When the active-body budget binds, skills are dropped **most-recently
//! activated first**. The skill the model just asked for is the one it is
//! working from; an older activation is more likely to be a precondition that has
//! already been satisfied. Alphabetical order would drop the same skill on every
//! request regardless of what the model is doing.
//!
//! ## Re-materialization after compaction
//!
//! An activated skill is identified by name and revision, both recorded outside
//! message history. After a compaction the caller replays the activation list and
//! the bodies are re-read from disk — no skill content is carried in the summary,
//! and no skill content needs to be. See [`crate::derived`].

use crate::derived::{DerivedBindings, DerivedId, DerivedKind, DerivedRevision};
use crate::skills::config::SkillsConfig;
use crate::skills::definition::{unsatisfied_dependencies, SkillDefinition};
use serde::{Deserialize, Serialize};

/// One requested activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationRequest {
    /// The skill name, as registered.
    pub name: String,
    /// Why it was activated. Kept because "the model asked" and "the user named
    /// it" have different authority, and a reader of the log should not have to
    /// guess which happened.
    pub origin: ActivationOrigin,
}

/// Why a skill was activated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActivationOrigin {
    /// The caller named it explicitly.
    Explicit,
    /// The model selected it from the catalog.
    ModelSelected,
    /// Re-materialized after a compaction.
    Rematerialized,
}

/// An activated skill as it appears in a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveSkill {
    pub name: String,
    pub revision: DerivedRevision,
    pub origin: ActivationOrigin,
    /// Declared dependencies the request does not carry. A diagnostic, never an
    /// authorization decision.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_dependencies: Vec<String>,
    /// Whether the body was cut at the per-body cap.
    #[serde(default)]
    pub truncated: bool,
    /// Rendered size of the body, for budget accounting.
    #[serde(default)]
    pub chars: usize,
}

/// A skill that was requested but not activated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationRefusal {
    pub name: String,
    pub reason: String,
}

/// The activation set for one request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivationSet {
    /// Activated skills, oldest activation first. The order is the drop order,
    /// reversed.
    pub active: Vec<ActiveSkill>,
    /// Requests that could not be honoured.
    pub refused: Vec<ActivationRefusal>,
    /// Activations dropped because a budget bound.
    pub dropped: Vec<ActivationRefusal>,
}

impl ActivationSet {
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    pub fn names(&self) -> Vec<&str> {
        self.active.iter().map(|entry| entry.name.as_str()).collect()
    }

    /// Whether a skill is active.
    pub fn contains(&self, name: &str) -> bool {
        self.active.iter().any(|entry| entry.name == name)
    }

    /// Total characters across activated bodies.
    pub fn chars(&self) -> usize {
        self.active.iter().map(|entry| entry.chars).sum()
    }

    /// Durable bindings for the activated bodies.
    pub fn bindings(&self) -> DerivedBindings {
        let mut bindings = DerivedBindings::new();
        for entry in &self.active {
            bindings.bind(crate::derived::DerivedBinding::new(
                DerivedId::skill(&entry.name),
                DerivedKind::Skill,
                entry.revision.clone(),
                entry.name.clone(),
                entry.chars,
            ));
        }
        bindings
    }

    /// Fold a catalog binding in, so one binding set covers both halves of the
    /// skill projection.
    pub fn with_catalog(&self, catalog: &DerivedBindings) -> DerivedBindings {
        let mut merged = self.bindings();
        merged.merge(catalog);
        merged
    }
}

/// A private per-entry char count, kept out of the serialized shape.
#[derive(Debug, Clone)]
struct Sized {
    skill: SkillDefinition,
    origin: ActivationOrigin,
    chars: usize,
}

/// Resolve a list of activation requests against the registry.
///
/// `available_tools` and `available_capabilities` are what the *request* carries,
/// supplied by the caller. They are compared, never modified: a skill cannot
/// widen them, and this function cannot either.
///
/// Requests are honoured in the order given, so the caller controls precedence by
/// ordering. Duplicates collapse to the first occurrence.
pub fn resolve(
    registry: &crate::skills::registry::SkillRegistry,
    requests: &[ActivationRequest],
    policy: &SkillsConfig,
    available_tools: &[String],
    available_capabilities: &[String],
) -> ActivationSet {
    let mut set = ActivationSet::default();
    let mut seen: Vec<String> = Vec::new();
    let mut sized: Vec<Sized> = Vec::new();
    let mut budget = policy.max_active_body_chars;

    for request in requests {
        if seen.iter().any(|name| name == &request.name) {
            continue;
        }
        seen.push(request.name.clone());
        if sized.len() >= policy.max_active_skills {
            set.dropped.push(ActivationRefusal {
                name: request.name.clone(),
                reason: format!(
                    "{} skills are already active, the configured maximum",
                    sized.len()
                ),
            });
            continue;
        }
        let Some(skill) = registry.get(&request.name) else {
            set.refused.push(ActivationRefusal {
                name: request.name.clone(),
                reason: "no skill is registered under that name".to_string(),
            });
            continue;
        };
        let chars = skill.instructions.chars().count();
        if chars > budget {
            // The newest activation does not fit. An older one is not dropped to
            // make room: the model asked for this one, and evicting something it
            // is still relying on to satisfy a request it just made would be a
            // worse outcome than refusing.
            set.dropped.push(ActivationRefusal {
                name: request.name.clone(),
                reason: format!(
                    "the activated-body budget of {} characters cannot fit this skill's {chars}",
                    policy.max_active_body_chars
                ),
            });
            continue;
        }
        budget = budget.saturating_sub(chars);
        sized.push(Sized {
            skill: skill.clone(),
            origin: request.origin,
            chars,
        });
    }

    // Oldest first, so the head of the vector is the first thing dropped if a
    // later pass needs to make room.
    for entry in sized {
        let missing = unsatisfied_dependencies(&entry.skill, available_tools, available_capabilities);
        set.active.push(ActiveSkill {
            name: entry.skill.metadata.name.clone(),
            revision: entry.skill.revision.clone(),
            origin: entry.origin,
            missing_dependencies: missing,
            truncated: entry.skill.truncated,
            chars: entry.chars,
        });
    }
    set
}

/// Drop the least recently activated skills until `budget` characters remain.
///
/// Used when a request assembles several sections and the skill bodies must give
/// ground. Activation order is preserved among survivors, so the skill the model
/// just asked for is never the one dropped.
pub fn enforce_budget(
    set: &mut ActivationSet,
    registry: &crate::skills::registry::SkillRegistry,
    budget: usize,
) {
    // `active` is oldest-first, so index 0 is always the least recently
    // activated. Removing it promotes the next-oldest into that slot, so the
    // index deliberately does not advance.
    let mut total = set.chars();
    while total > budget && !set.active.is_empty() {
        let victim = set.active.remove(0);
        let size = registry
            .get(&victim.name)
            .map(|skill| skill.instructions.chars().count())
            .unwrap_or(victim.chars);
        total = total.saturating_sub(size);
        set.dropped.push(ActivationRefusal {
            name: victim.name,
            reason: format!(
                "the activated-body budget of {budget} characters was exceeded; the least recently activated skill was dropped"
            ),
        });
    }
}

/// Build the re-materialization list for a post-compaction request.
///
/// The input is whatever durable state the caller kept; the output is the
/// activation list to replay. No body content is involved on either side, which
/// is the point: a summary never has to carry a skill.
pub fn rematerialize(
    previous: &ActivationSet,
    registry: &crate::skills::registry::SkillRegistry,
) -> ActivationSet {
    let requests: Vec<ActivationRequest> = previous
        .active
        .iter()
        .filter(|entry| registry.get(&entry.name).is_some())
        .map(|entry| ActivationRequest {
            name: entry.name.clone(),
            origin: ActivationOrigin::Rematerialized,
        })
        .collect();
    let mut refusals: Vec<ActivationRefusal> = previous
        .active
        .iter()
        .filter(|entry| registry.get(&entry.name).is_none())
        .map(|entry| ActivationRefusal {
            name: entry.name.clone(),
            reason: "the skill is no longer registered; its instructions were re-read from disk and could not be restored"
                .to_string(),
        })
        .collect();
    let mut rebuilt = ActivationSet {
        active: Vec::new(),
        refused: Vec::new(),
        dropped: Vec::new(),
    };
    for request in &requests {
        let Some(skill) = registry.get(&request.name) else {
            continue;
        };
        rebuilt.active.push(ActiveSkill {
            name: skill.metadata.name.clone(),
            revision: skill.revision.clone(),
            origin: ActivationOrigin::Rematerialized,
            missing_dependencies: Vec::new(),
            truncated: skill.truncated,
            chars: skill.instructions.chars().count(),
        });
    }
    refusals.append(&mut rebuilt.refused);
    rebuilt.refused = refusals;
    rebuilt
}
