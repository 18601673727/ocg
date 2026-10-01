//! Bounded context projection for the skill catalog and activated bodies.
//!
//! ## Two sections, because they are paid for separately
//!
//! ```text
//! <available_skills>   name + description, every request, revision-gated
//! <active_skill>       one body per activated skill, revision-gated
//! ```
//!
//! They are separate sections rather than one block because they have different
//! lifetimes. The catalog changes when the registry changes; bodies change when
//! a skill is edited. Merging them would make an ordinary skill edit invalidate
//! the catalog and re-send every description, which is the cost the three-tier
//! disclosure model exists to avoid.
//!
//! ## The catalog is *marked* when it is incomplete
//!
//! When the catalog budget drops entries, the projection says so in band and
//! names how many were left out. A silently shortened catalog reads as complete,
//! and a model that cannot tell a complete list from a truncated one will not ask
//! for the skill it did not see. This is a real failure mode in the surveyed
//! implementations and it is cheap to avoid.
//!
//! ## Bodies are references, not transcripts
//!
//! An activated body is projected with its revision and its resource listing.
//! When the revision has not moved since the last request, nothing is emitted —
//! which is what keeps an unchanged body out of the repeated payload. After a
//! compaction the caller replays [`crate::skills::resolve::rematerialize`] and
//! the body is re-read from disk.

use crate::derived::{DerivedBindings, DerivedTransition};
use crate::skills::config::SkillsConfig;
use crate::skills::definition::SkillDefinition;
use crate::skills::registry::SkillRegistry;
use crate::skills::resolve::ActivationSet;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt::Write as _;

/// Header for the catalog section.
pub const CATALOG_HEADER: &str = "## Available skills";

/// Header for one activated skill's body.
pub const SKILL_HEADER: &str = "## Active skill";

/// The line that marks a catalog which lost entries to its budget.
pub const CATALOG_TRUNCATED_NOTICE: &str =
    "// this list is incomplete: the catalog budget dropped the skills with the lowest precedence.";

/// The line that marks a replacement of a previously provided section.
pub const REPLACEMENT_NOTICE: &str =
    "This section replaces all previously provided content of the same section.";

/// The role a skill section is projected into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SkillRole {
    System,
    User,
}

impl SkillRole {
    pub fn as_str(self) -> &'static str {
        match self {
            SkillRole::System => "system",
            SkillRole::User => "user",
        }
    }
}

/// What a caller should do about a projected section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SectionAction {
    /// Emit nothing; the revision has not moved.
    Unchanged,
    /// Project the section.
    Add,
    /// Project it with a replacement notice.
    Replace,
    /// Emit a notice that the section no longer applies.
    Remove,
}

/// The comparison result for one section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionDiff {
    pub transition: DerivedTransition,
    pub action: SectionAction,
}

impl SectionDiff {
    pub fn should_emit(&self) -> bool {
        !matches!(self.action, SectionAction::Unchanged)
    }
}

/// Render the catalog.
///
/// Entries are ordered by name so the catalog is a function of registry content
/// alone. When the budget binds, the *lowest-precedence* skills are dropped
/// first: a project-local skill is more likely to be the one the model needs
/// than a user-global default.
pub fn render_catalog(registry: &SkillRegistry, policy: &SkillsConfig) -> SkillProjection {
    let mut entries: Vec<&SkillDefinition> = registry.list();
    let mut dropped = 0usize;
    let mut text = String::new();

    loop {
        // The budget is per render, not per registry: each pass re-decides how
        // many entries fit from scratch, so the choice of which entry to drop
        // cannot depend on how much earlier passes happened to spend.
        let mut budget = policy.max_catalog_chars;
        text.clear();
        let _ = writeln!(text, "{CATALOG_HEADER}");
        let _ = writeln!(
            text,
            "// Load a skill with the skill tool by name. Only the body of a skill you load costs context."
        );
        for skill in ordered_for_budget(&entries) {
            let line = catalog_line(skill);
            if line.chars().count() + 1 > budget {
                continue;
            }
            budget = budget.saturating_sub(line.chars().count() + 1);
            let _ = writeln!(text, "{line}");
        }
        let kept = count_lines(&text);
        if kept >= entries.len() || entries.is_empty() {
            break;
        }
        // Drop the lowest-precedence remaining entry and re-render.
        match entries
            .iter()
            .enumerate()
            .min_by_key(|(_, skill)| skill.source.clone())
            .map(|(index, _)| index)
        {
            Some(index) => {
                entries.remove(index);
                dropped += 1;
            }
            None => break,
        }
    }

    if dropped > 0 {
        let _ = writeln!(text, "{CATALOG_TRUNCATED_NOTICE}");
        let _ = writeln!(text, "// {dropped} skill(s) with the lowest precedence were not listed.");
    }

    SkillProjection {
        text,
        bindings: registry.catalog_binding(),
        dropped,
    }
}

/// Catalog order: name ascending, which is stable and readable.
///
/// Within the budget-drop loop the *lowest precedence* is chosen for removal, so
/// the order here is presentation only.
fn ordered_for_budget<'a>(entries: &[&'a SkillDefinition]) -> Vec<&'a SkillDefinition> {
    let mut ordered = entries.to_vec();
    ordered.sort_by(|left, right| left.metadata.name.cmp(&right.metadata.name));
    ordered
}

fn catalog_line(skill: &SkillDefinition) -> String {
    format!(
        "- **{}**: {} (source: {})",
        skill.metadata.name,
        skill.metadata.description,
        skill.source.as_str()
    )
}

fn count_lines(text: &str) -> usize {
    text.lines()
        .filter(|line| line.starts_with("- **"))
        .count()
}

/// A rendered section plus what it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillProjection {
    text: String,
    bindings: DerivedBindings,
    /// How many entries the budget removed. Zero for a body projection.
    pub dropped: usize,
}

impl SkillProjection {
    /// Build a projection from already-rendered text and bindings.
    pub fn new(text: String, bindings: DerivedBindings) -> Self {
        Self {
            text,
            bindings,
            dropped: 0,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn chars(&self) -> usize {
        self.text.chars().count()
    }

    pub fn bindings(&self) -> &DerivedBindings {
        &self.bindings
    }

    /// Compare against a previously recorded binding set.
    ///
    /// The diff is scoped to this section's own kind, because the recorded set
    /// normally also covers the catalog and the *other* skill bodies. Diffing
    /// all of it would report those as removed and make every section look
    /// changed on every request.
    pub fn diff(&self, previous: Option<&DerivedBindings>) -> SectionDiff {
        let kind = self
            .bindings
            .iter()
            .next()
            .map(|entry| entry.kind)
            .unwrap_or(crate::derived::DerivedKind::Skill);
        let transition = self.bindings.transition_from_kind(kind, previous);
        let recorded_before = previous.is_some_and(|set| set.has_kind(kind));
        let action = if self.text.is_empty() {
            // Nothing to project, so there is nothing to announce — including
            // when a previously projected section has gone away. A removal is
            // reported by the caller's own bookkeeping, not by emitting an empty
            // section that would only invite the model to look for content.
            SectionAction::Unchanged
        } else if !recorded_before {
            SectionAction::Add
        } else if transition.is_noop() {
            SectionAction::Unchanged
        } else {
            SectionAction::Replace
        };
        SectionDiff {
            transition,
            action,
        }
    }

    /// Project into a request message.
    pub fn to_message(&self, role: SkillRole, replacement: bool) -> Option<Value> {
        if self.text.is_empty() {
            return None;
        }
        let content = if replacement {
            format!("{REPLACEMENT_NOTICE}\n\n{}", self.text)
        } else {
            self.text.clone()
        };
        Some(json!({ "role": role.as_str(), "content": content }))
    }
}

/// Render the activated bodies.
///
/// One section per skill, each naming its revision, because a revision is what
/// makes the section re-materializable rather than transcript content. Missing
/// declared dependencies are stated in band: a model told to use a tool that is
/// not on the request should learn that from the skill section, not from a failed
/// call.
pub fn render_active(
    registry: &SkillRegistry,
    activations: &ActivationSet,
    policy: &SkillsConfig,
) -> Vec<SkillProjection> {
    activations
        .active
        .iter()
        .filter_map(|entry| {
            let skill = registry.get(&entry.name)?;
            Some(render_one(skill, entry, policy))
        })
        .collect()
}

fn render_one(
    skill: &SkillDefinition,
    entry: &crate::skills::resolve::ActiveSkill,
    policy: &SkillsConfig,
) -> SkillProjection {
    // The *registry's* revision, not the one recorded on the activation. An
    // activation set can outlive an edit to the skill file — that is the normal
    // case, because it is what makes an edited skill show up on the next request
    // without anything having to remember the edit. Using the recorded revision
    // here would project new text under an old revision, and the next diff would
    // see no change and never re-send it.
    let revision = skill.revision.clone();
    let mut text = String::new();
    let _ = writeln!(text, "{SKILL_HEADER}: {}", skill.metadata.name);
    let _ = writeln!(text, "// revision: {revision}");
    let _ = writeln!(text, "// source: {}", skill.source.describe());
    if let Some(base) = &skill.base_dir {
        let _ = writeln!(
            text,
            "// base directory: {}\n// paths in this skill are relative to that directory.",
            base.display()
        );
    }
    if entry.truncated {
        let _ = writeln!(
            text,
            "// note: this body is longer than maxBodyChars of {} and was cut; instructions later in the file were NOT read",
            policy.max_body_chars
        );
    }
    for missing in &entry.missing_dependencies {
        let _ = writeln!(
            text,
            "// note: this skill declares {missing}, which this request does not carry"
        );
    }
    if !skill.resources.is_empty() {
        let _ = writeln!(
            text,
            "// supporting files (read one by name when you need it; they are not loaded yet):"
        );
        for resource in skill.resources.iter().take(policy.max_resource_files) {
            let _ = writeln!(text, "//   {}", resource.path);
        }
    }
    let _ = writeln!(text);
    let _ = writeln!(text, "{}", skill.instructions.trim_end());
    let _ = writeln!(
        text,
        "// This body is re-read from the skill package on demand. It is not carried by conversation history."
    );

    let mut bindings = DerivedBindings::new();
    bindings.bind(crate::derived::DerivedBinding::new(
        skill.id(),
        crate::derived::DerivedKind::Skill,
        revision,
        skill.source.describe(),
        text.chars().count(),
    ));
    SkillProjection::new(text, bindings)
}

/// Project the catalog and every activated body into request messages.
///
/// Sections whose revision has not moved produce nothing, which is what keeps an
/// unchanged catalog and unchanged bodies out of the repeated payload. Order is
/// catalog first, then bodies in activation order: the catalog is what the model
/// reads to decide, the bodies are what it reads once it has decided.
pub fn project(
    registry: &SkillRegistry,
    activations: &ActivationSet,
    policy: &SkillsConfig,
    previous: Option<&DerivedBindings>,
    role: SkillRole,
) -> Vec<Value> {
    let mut messages = Vec::new();
    let catalog = render_catalog(registry, policy);
    let diff = catalog.diff(previous);
    if diff.should_emit() {
        if let Some(message) =
            catalog.to_message(role, diff.action == SectionAction::Replace)
        {
            messages.push(message);
        }
    }
    for projection in render_active(registry, activations, policy) {
        let diff = projection.diff(previous);
        if diff.should_emit() {
            if let Some(message) =
                projection.to_message(role, diff.action == SectionAction::Replace)
            {
                messages.push(message);
            }
        }
    }
    messages
}

/// A human-readable summary of what a request would pay for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillCostReport {
    pub catalog_chars: usize,
    pub catalog_dropped: usize,
    pub active_chars: usize,
    pub active_count: usize,
}

impl SkillCostReport {
    /// Total characters across both sections.
    pub fn total(&self) -> usize {
        self.catalog_chars + self.active_chars
    }
}

/// Measure what a projection costs, without rendering it into messages.
///
/// Kept separate from [`project`] because a caller usually wants the number for a
/// budget decision *before* deciding whether to include the sections at all.
pub fn measure(
    registry: &SkillRegistry,
    activations: &ActivationSet,
    policy: &SkillsConfig,
) -> SkillCostReport {
    let catalog = render_catalog(registry, policy);
    let active: usize = activations.chars();
    SkillCostReport {
        catalog_chars: catalog.chars(),
        catalog_dropped: catalog.dropped,
        active_chars: active,
        active_count: activations.active.len(),
    }
}
