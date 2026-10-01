//! Bounded context projection for a resolved instruction chain.
//!
//! The projection is where the discovery result becomes something a model can
//! read. Three decisions live here, and each of them is a position rather than a
//! default.
//!
//! ## One section, not one message per file
//!
//! Files are rendered into a single delimited block with a provenance line per
//! file. Per-file messages would make the chain's *order* the only thing
//! expressing precedence, which is exactly the property a model is worst at
//! inferring. Naming the scope and the path per file makes precedence readable
//! instead of merely positional.
//!
//! ## Replacement, never mutation
//!
//! The projection is derived from disk on every request and diffed against the
//! revision recorded outside message history. When the revision has not moved,
//! [`InstructionProjection::diff`] reports nothing and the caller emits nothing
//! — which is what keeps the block inside the provider's cacheable static
//! prefix. When it has moved, the projection carries an explicit replacement
//! notice rather than hoping the model notices that the text differs.
//!
//! ## Truncation is always visible
//!
//! A truncated or skipped instruction file is named in the block. An
//! instruction that is silently shortened is worse than one that is absent,
//! because the model cannot tell the difference between "this rule has no more
//! text" and "the rest was cut".
//!
//! ## Placement
//!
//! [`InstructionProjection::to_message`] accepts a role rather than choosing
//! one. OCG does not own the request shape, and a static instruction block is
//! the kind of content whose placement relative to the cache boundary is a
//! provider-level decision.

use crate::derived::{DerivedId, DerivedTransition};
use crate::error::{OcgError, Result};
use crate::instructions::config::InstructionsConfig;
use crate::instructions::discovery::{InstructionFile, InstructionScope, InstructionSet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt::Write as _;

/// The header that opens the block. Also used to recognize the block in a
/// request, so a caller can replace it rather than append a second copy.
pub const INSTRUCTIONS_HEADER: &str = "## Project instructions";

/// The line that marks a replacement of a previously provided block.
pub const REPLACEMENT_NOTICE: &str =
    "These instructions replace all previously provided project instructions.";

/// The line that marks instructions which no longer apply.
pub const REMOVAL_NOTICE: &str = "The previously provided project instructions no longer apply.";

/// The role an instruction block is projected into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstructionRole {
    /// Request framing. Sent on every request and normally inside the cached
    /// prefix.
    System,
    /// Conversation framing. Some providers cache a `user`-role block more
    /// reliably than a `system` one.
    User,
}

impl InstructionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            InstructionRole::System => "system",
            InstructionRole::User => "user",
        }
    }
}

/// A rendered instruction block plus the bindings it accounts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionProjection {
    text: String,
    bindings: crate::derived::DerivedBindings,
}

impl InstructionProjection {
    /// Render a resolved chain.
    ///
    /// An empty chain renders to the empty string rather than to a header with
    /// nothing under it: a header alone reads as "there are instructions" and
    /// invites the model to look for them.
    pub fn render(set: &InstructionSet) -> Self {
        Self {
            text: render_text(set),
            bindings: set.bindings(),
        }
    }

    /// The rendered block.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether there is anything to project.
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn chars(&self) -> usize {
        self.text.chars().count()
    }

    /// The durable bindings for the sources this block carries.
    pub fn bindings(&self) -> &crate::derived::DerivedBindings {
        &self.bindings
    }

    /// Compare against a previously recorded binding set.
    ///
    /// The returned [`InstructionDiff`] is data, not messages. A caller decides
    /// whether a changed block becomes a fresh message, a replacement notice, or
    /// a log line; the projection only knows *that* it changed.
    pub fn diff(&self, previous: Option<&crate::derived::DerivedBindings>) -> InstructionDiff {
        let kind = crate::derived::DerivedKind::InstructionFile;
        // Scoped to this kind on purpose: the recorded set may also cover skill
        // sections, and diffing those in would report them as removed and make
        // an unchanged instruction block look changed on every request.
        let transition = self.bindings.transition_from_kind(kind, previous);
        let quiet = transition.is_noop();
        let current = !self.text.is_empty();
        let was_current = previous.is_some_and(|bindings| bindings.has_kind(kind));
        InstructionDiff {
            transition,
            action: match (current, was_current) {
                (false, false) => InstructionAction::Unchanged,
                (false, true) => InstructionAction::Remove,
                (true, false) => InstructionAction::Add,
                (true, true) if quiet => InstructionAction::Unchanged,
                (true, true) => InstructionAction::Replace,
            },
        }
    }

    /// Project the block into a request message.
    pub fn to_message(&self, role: InstructionRole) -> Value {
        json!({ "role": role.as_str(), "content": self.text })
    }

    /// The message that tells the model its previous instructions no longer
    /// apply.
    pub fn removal_message(role: InstructionRole) -> Value {
        json!({ "role": role.as_str(), "content": REMOVAL_NOTICE })
    }
}

/// What a caller should do about a diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstructionAction {
    /// Emit nothing. The block is unchanged and already in the request.
    Unchanged,
    /// Project the block for the first time.
    Add,
    /// Project the block with [`REPLACEMENT_NOTICE`] in front of it.
    Replace,
    /// Emit [`REMOVAL_NOTICE`]; the chain no longer applies.
    Remove,
}

/// The comparison result for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionDiff {
    pub transition: DerivedTransition,
    pub action: InstructionAction,
}

impl InstructionDiff {
    /// Whether the caller should emit a message at all.
    pub fn should_emit(&self) -> bool {
        !matches!(self.action, InstructionAction::Unchanged)
    }

    /// The message to append, if any.
    pub fn to_message(&self, projection: &InstructionProjection, role: InstructionRole) -> Option<Value> {
        match self.action {
            InstructionAction::Unchanged => None,
            InstructionAction::Remove => Some(InstructionProjection::removal_message(role)),
            InstructionAction::Add => Some(projection.to_message(role)),
            InstructionAction::Replace => Some(json!({
                "role": role.as_str(),
                "content": format!("{REPLACEMENT_NOTICE}\n\n{}", projection.text()),
            })),
        }
    }
}

/// Render one file's contribution, with its provenance and truncation state.
fn render_file(file: &InstructionFile, out: &mut String) {
    let _ = writeln!(
        out,
        "### Instructions from {} ({})",
        file.source,
        file.scope.as_str()
    );
    if let InstructionScope::Nested { depth } = file.scope {
        let _ = writeln!(
            out,
            "// scope: this directory and everything below it ({depth} director{} below the project root)",
            if depth == 1 { "y" } else { "ies" }
        );
    }
    if file.truncated {
        let bytes = file.bytes;
        let _ = writeln!(
            out,
            "// note: this file is {bytes} bytes; only the leading part is shown, so instructions later in the file were NOT read"
        );
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "{}", file.content.trim_end());
    let _ = writeln!(out);
}

/// Render the chain.
fn render_text(set: &InstructionSet) -> String {
    if set.files.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    let _ = writeln!(out, "{INSTRUCTIONS_HEADER}");
    let _ = writeln!(
        out,
        "// {} file(s), listed from least to most specific. Later instructions refine earlier ones.",
        set.files.len()
    );
    if !set.skipped.is_empty() {
        let _ = writeln!(
            out,
            "// {} file(s) were discovered but NOT applied:",
            set.skipped.len()
        );
        for note in &set.skipped {
            let _ = writeln!(out, "//   - {}: {}", note.source, note.reason);
        }
    }
    let _ = writeln!(out);
    for file in &set.files {
        render_file(file, &mut out);
    }
    let _ = writeln!(
        out,
        "// These instructions are re-read from disk on every request. A conversation summary may refer to them by path but never replaces them."
    );
    out
}

/// The identity of the block as a whole, for a caller that wants to bind the
/// section rather than each file.
///
/// Per-file bindings are the primary mechanism; this exists for a caller that
/// treats the chain as one opaque section and wants a single revision to compare.
pub fn section_id() -> DerivedId {
    DerivedId::new(crate::derived::DerivedKind::InstructionFile, "section")
}

/// Reject a caller-supplied instruction path that escapes the project root.
///
/// Thin wrapper over [`crate::instructions::discovery::require_inside_root`],
/// kept here so a request-assembly caller has one obvious entry point and does
/// not have to know which sibling module owns the rule.
pub fn require_project_path(root: &std::path::Path, path: &std::path::Path) -> Result<()> {
    crate::instructions::discovery::require_inside_root(root, path)
}

/// Whether a value looks like a previously projected instruction block.
///
/// Lets a caller assembling a request from mixed sources recognize a block it
/// already sent, instead of appending a second copy under a different header.
pub fn is_instruction_block(content: &str) -> bool {
    content.trim_start().starts_with(INSTRUCTIONS_HEADER)
}

/// Validate a projected block against the policy that produced it.
///
/// The projection is bounded by construction, so this only reports the one thing
/// a caller cannot see from the text: whether the chain exceeded a configured
/// limit and was therefore reduced.
pub fn audit(set: &InstructionSet, config: &InstructionsConfig) -> Result<InstructionAudit> {
    let total: usize = set.files.iter().map(|file| file.content.len()).sum();
    if total > config.max_total_bytes {
        return Err(OcgError::config(format!(
            "the projected instruction chain is {total} bytes, above maxTotalBytes of {}",
            config.max_total_bytes
        )));
    }
    for file in &set.files {
        if file.content.len() > config.max_file_bytes {
            return Err(OcgError::config(format!(
                "instruction file {} retained {} bytes, above maxFileBytes of {}",
                file.source,
                file.content.len(),
                config.max_file_bytes
            )));
        }
    }
    Ok(InstructionAudit {
        files: set.files.len(),
        bytes: total,
        truncated: set.files.iter().filter(|file| file.truncated).count(),
        skipped: set.skipped.len(),
    })
}

/// What a projection actually contains, for logs and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionAudit {
    pub files: usize,
    pub bytes: usize,
    pub truncated: usize,
    pub skipped: usize,
}
