//! Derived context: content OCG re-materializes by identity instead of
//! remembering inside the transcript.
//!
//! `AGENTS.md` instruction chains and Skill packages are both *derived*: the
//! filesystem is their source of truth and OCG holds no authority over their
//! content. That single fact decides how they behave under compaction, and it
//! is worth stating separately from either feature because both need the same
//! thing and neither should invent its own version of it.
//!
//! ```text
//! durable history ── compaction may replace this with a summary
//!        │
//! derived sources ── compaction never touches this; it is re-derived per
//!        │           request and diffed against a recorded revision
//!        ▼
//! active request
//! ```
//!
//! Three rules follow, and they are the whole content of this module:
//!
//! 1. **Never carried in a summary.** A summary may *reference* a derived
//!    source by identity; it may never restate its content. If a summary held
//!    the instructions, compaction would become a lossy copy of the filesystem
//!    and the next request would act on a paraphrase of a file nobody edited.
//!
//! 2. **Re-derived, then diffed.** Every derived source has a stable
//!    [`DerivedId`] and a content [`DerivedRevision`]. When the revision moves,
//!    the projection emits a *replacement* ([`DerivedTransition`]) instead of
//!    mutating history. When it does not move, nothing is emitted at all — which
//!    is what keeps unchanged instructions and catalogs inside the cacheable
//!    static prefix instead of re-sent on every turn.
//!
//! 3. **Compaction is a non-event.** Because the source of truth is the
//!    filesystem and the previous revision is recorded outside message history,
//!    a compaction cannot lose a derived source. There is nothing in the
//!    transcript to lose.
//!
//! This is the durable/re-derived pattern rather than the durable/scoped one
//! (which injects instructions through tool output and therefore loses them
//! when that output is pruned). The scoped pattern is cheaper per turn but
//! makes a standing instruction depend on a tool call surviving, which is
//! exactly the coupling compaction breaks.
//!
//! ## What this module deliberately does not decide
//!
//! *When* to re-derive, *where* in a request the rendered block goes, and
//! *whether* a given change is acceptable to the operator all belong to the
//! provider loop and to execution authority. Only the identity, revision and
//! diff contract lives here, so that `instructions` and `skills` can both use
//! it without either owning it.

use crate::hash::sha256_hex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// Prefix on every [`DerivedRevision`], so a revision is never mistaken for an
/// arbitrary operator-supplied string.
pub const REVISION_PREFIX: &str = "sha256:";

/// What kind of derived source a binding describes.
///
/// The kind is part of the identity namespace, so a skill called `agents` and
/// an instruction file called `agents` cannot collide in one binding set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DerivedKind {
    /// One file in a discovered `AGENTS.md` chain.
    InstructionFile,
    /// The skill catalog: every discoverable skill's name and description.
    SkillCatalog,
    /// One activated skill's instruction body.
    Skill,
}

impl DerivedKind {
    /// A stable lowercase label, used in ids and in rendered provenance.
    pub fn as_str(self) -> &'static str {
        match self {
            DerivedKind::InstructionFile => "instruction",
            DerivedKind::SkillCatalog => "skillCatalog",
            DerivedKind::Skill => "skill",
        }
    }
}

impl fmt::Display for DerivedKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A stable identity for one derived source.
///
/// Identity is derived from *what the source is*, not from where it happened to
/// be found or when. An instruction file is identified by its project-relative
/// path so that moving the project does not change its identity, and a skill is
/// identified by its name so that a skill loaded from a different source
/// directory is recognisably the same skill.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedId(String);

impl DerivedId {
    /// Build an id from an explicit namespaced string.
    pub fn new(kind: DerivedKind, name: &str) -> Self {
        Self(format!("{}:{name}", kind.as_str()))
    }

    /// Identity of one `AGENTS.md`-chain file, named by its project-relative
    /// path.
    pub fn instruction_file(relative_path: &str) -> Self {
        Self::new(DerivedKind::InstructionFile, relative_path)
    }

    /// Identity of the skill catalog. The catalog is one source, not one per
    /// skill, because it is re-derived as a unit and diffed as a unit.
    pub fn skill_catalog() -> Self {
        Self::new(DerivedKind::SkillCatalog, "catalog")
    }

    /// Identity of one activated skill.
    pub fn skill(name: &str) -> Self {
        Self::new(DerivedKind::Skill, name)
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The namespaced name, with the kind prefix removed.
    pub fn name(&self) -> &str {
        match self.0.split_once(':') {
            Some((_, name)) => name,
            None => &self.0,
        }
    }

    /// Derive an instruction-file id from an absolute path and a project root.
    ///
    /// A path outside `root` cannot be named relatively, so it falls back to its
    /// canonical spelling. Discovery rejects such paths before they reach an
    /// id; this fallback exists so the function is total rather than panicking
    /// on a caller that skipped that check.
    pub fn instruction_file_in(root: &Path, path: &Path) -> Self {
        let name = path
            .strip_prefix(root)
            .ok()
            .map(|relative| relative.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| path.to_string_lossy().replace('\\', "/"));
        Self::instruction_file(&name)
    }
}

impl fmt::Display for DerivedId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The content revision of a derived source.
///
/// It is a digest of the *bytes that would be rendered*, not of the file's
/// metadata, so a change that does not alter the rendered text does not
/// invalidate a cache and a change that does alter it always does.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedRevision(String);

impl DerivedRevision {
    /// Digest a rendered payload.
    pub fn of(payload: &[u8]) -> Self {
        Self(format!("{REVISION_PREFIX}{}", sha256_hex(payload)))
    }

    /// Digest a sequence of parts unambiguously.
    ///
    /// Length-prefixing each part means `["ab", "c"]` and `["a", "bc"]` cannot
    /// collide, which a plain concatenation would allow. Concatenation is used
    /// for projections built from several files, so this is the constructor
    /// they use.
    pub fn of_parts<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut buffer = Vec::new();
        for part in parts {
            buffer.extend_from_slice(&(part.len() as u64).to_be_bytes());
            buffer.extend_from_slice(part);
        }
        Self::of(&buffer)
    }

    /// The digest as a string slice, prefix included.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this revision is a well-formed digest.
    pub fn is_valid(&self) -> bool {
        self.0
            .strip_prefix(REVISION_PREFIX)
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
    }
}

impl fmt::Display for DerivedRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One derived source, recorded at the revision that was last projected.
///
/// The binding is the durable half of the contract: it is stored outside message
/// history, so it survives compaction untouched and remains a valid comparison
/// baseline afterwards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedBinding {
    pub id: DerivedId,
    pub kind: DerivedKind,
    pub revision: DerivedRevision,
    /// Human-readable provenance: the project-relative path, or the source
    /// label a skill was loaded from. Never a placeholder, so a projection can
    /// always say where its content came from.
    pub source: String,
    /// Rendered size in characters, for budget accounting.
    pub chars: usize,
}

impl DerivedBinding {
    pub fn new(
        id: DerivedId,
        kind: DerivedKind,
        revision: DerivedRevision,
        source: impl Into<String>,
        chars: usize,
    ) -> Self {
        Self {
            id,
            kind,
            revision,
            source: source.into(),
            chars,
        }
    }
}

/// The diff between a previously recorded binding set and the current one.
///
/// This is the shape of "what changed about the world since the last request",
/// and it is deliberately *not* a message list. Turning it into messages is the
/// caller's decision, because only the caller knows the request shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedTransition {
    /// Sources present now that were not present before.
    pub added: Vec<DerivedId>,
    /// Sources whose revision moved.
    pub updated: Vec<DerivedId>,
    /// Sources that were present before and are gone now.
    pub removed: Vec<DerivedId>,
}

impl DerivedTransition {
    /// Whether nothing changed. A caller that sees this should emit nothing,
    /// which is what preserves a static cacheable prefix.
    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }

    /// Whether any source is new or changed, i.e. whether a replacement has to
    /// be emitted.
    pub fn has_current(&self) -> bool {
        !self.added.is_empty() || !self.updated.is_empty()
    }

    /// Total number of changes.
    pub fn len(&self) -> usize {
        self.added.len() + self.updated.len() + self.removed.len()
    }

    /// Whether there were no changes at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A recorded set of derived sources, keyed by identity.
///
/// Ordering is by identity, not by discovery order, so the set has exactly one
/// serialization for a given content. Two projections that resolved the same
/// sources agree on their fingerprint regardless of the order they found them
/// in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedBindings {
    entries: BTreeMap<DerivedId, DerivedBinding>,
}

impl DerivedBindings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a source, returning the binding it displaced.
    ///
    /// Last write wins for one identity. Discovery resolves precedence before it
    /// binds, so a displacement here means two projections of the same identity
    /// were merged rather than two sources colliding.
    pub fn bind(&mut self, binding: DerivedBinding) -> Option<DerivedBinding> {
        self.entries.insert(binding.id.clone(), binding)
    }

    /// Look up one recorded source.
    pub fn get(&self, id: &DerivedId) -> Option<&DerivedBinding> {
        self.entries.get(id)
    }

    /// The revision recorded for one identity.
    pub fn revision(&self, id: &DerivedId) -> Option<&DerivedRevision> {
        self.entries.get(id).map(|binding| &binding.revision)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every binding, ordered by identity.
    pub fn iter(&self) -> impl Iterator<Item = &DerivedBinding> {
        self.entries.values()
    }

    /// Only the bindings of one kind.
    pub fn of_kind(&self, kind: DerivedKind) -> impl Iterator<Item = &DerivedBinding> {
        self.entries
            .values()
            .filter(move |entry| entry.kind == kind)
    }

    /// A stable fingerprint of the whole set.
    pub fn fingerprint(&self) -> DerivedRevision {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            let mut part = Vec::new();
            part.extend_from_slice(entry.id.as_str().as_bytes());
            part.push(0);
            part.extend_from_slice(entry.revision.as_str().as_bytes());
            parts.push(part);
        }
        let borrowed: Vec<&[u8]> = parts.iter().map(|part| part.as_slice()).collect();
        DerivedRevision::of_parts(borrowed)
    }

    /// Fold another set into this one. Later bindings win.
    pub fn merge(&mut self, other: &DerivedBindings) {
        for entry in other.entries.values() {
            self.entries.insert(entry.id.clone(), entry.clone());
        }
    }

    /// Diff this set against a previously recorded one.
    ///
    /// `previous` of `None` means nothing was recorded, which is reported as
    /// every current source being added rather than as a no-op: a first request
    /// genuinely has to carry the content.
    pub fn transition_from(&self, previous: Option<&DerivedBindings>) -> DerivedTransition {
        let mut transition = DerivedTransition::default();
        for entry in self.entries.values() {
            match previous.and_then(|set| set.entries.get(&entry.id)) {
                None => transition.added.push(entry.id.clone()),
                Some(before) if before.revision != entry.revision => {
                    transition.updated.push(entry.id.clone())
                }
                Some(_) => {}
            }
        }
        if let Some(previous) = previous {
            for entry in previous.entries.values() {
                if !self.entries.contains_key(&entry.id) {
                    transition.removed.push(entry.id.clone());
                }
            }
        }
        transition
    }

    /// Diff only the entries of one kind against a previously recorded set,
    /// reporting additions and updates but not removals.
    ///
    /// A projection covers one section — the catalog, or one skill body — while
    /// the recorded set it is compared against normally covers every section at
    /// once. Two things follow, and both are why this method exists rather than
    /// [`Self::transition_from`]:
    ///
    /// * **Other sections must not read as removed.** A full diff would report
    ///   every other body as gone, so an unchanged body would look changed on
    ///   every request and be re-sent forever.
    /// * **A section cannot observe its own removal.** If a skill is deleted
    ///   from disk, its section simply stops rendering. The caller already knows,
    ///   because it compares the activation set; a section that rendered nothing
    ///   has no previous self to diff against.
    ///
    /// Removal is therefore a property of the whole set, and is reported once by
    /// the caller via [`Self::transition_from`] rather than by every section.
    pub fn transition_from_kind(
        &self,
        kind: DerivedKind,
        previous: Option<&DerivedBindings>,
    ) -> DerivedTransition {
        let mut transition = DerivedTransition::default();
        for entry in self.entries.values().filter(|entry| entry.kind == kind) {
            match previous.and_then(|set| set.entries.get(&entry.id)) {
                None => transition.added.push(entry.id.clone()),
                Some(before) if before.revision != entry.revision => {
                    transition.updated.push(entry.id.clone())
                }
                Some(_) => {}
            }
        }
        transition
    }

    /// Whether a recorded set holds any entry of one kind.
    pub fn has_kind(&self, kind: DerivedKind) -> bool {
        self.entries.values().any(|entry| entry.kind == kind)
    }

    /// Whether any recorded revision is malformed.
    ///
    /// A malformed revision is a corrupt record rather than a mismatch: it is
    /// reported so the caller can discard the baseline instead of treating a
    /// broken value as "unchanged" and never re-projecting.
    pub fn has_malformed_revision(&self) -> bool {
        self.entries
            .values()
            .any(|entry| !entry.revision.is_valid())
    }
}
