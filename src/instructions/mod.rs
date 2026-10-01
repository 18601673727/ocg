//! Repository instruction discovery: the `AGENTS.md` capability, owned by OCG.
//!
//! ## What this is
//!
//! An `AGENTS.md` file is prose a project author writes for a coding agent. It
//! is **derived context**: the filesystem is its source of truth and OCG holds
//! no authority over its content. That is the same class of thing as a Skill
//! package and the same class of thing as an activated tool schema, and all
//! three are handled by the shared identity/revision contract in
//! [`crate::derived`] rather than by three private mechanisms.
//!
//! ```text
//! Project                       OCG's existing ProjectBoundary — not re-derived here
//!      │
//!      ▼
//! Instruction discovery          root-to-cwd walk, bounded by the boundary
//!      │
//!      ▼
//! Scoped chain                  one file per directory, root-first
//!      │
//!      ▼
//! Resolved instruction set       precedence-ordered, provenance-carrying, fingerprinted
//!      │
//!      ▼
//! Context projection             one bounded, labelled block
//! ```
//!
//! ## Where the caller plugs in
//!
//! Nothing here is wired to a request. A caller assembles a request like this:
//!
//! ```text
//! boundary  = project::resolve(cwd)                      // OCG's authority
//! set       = instructions::discover(&boundary, cwd, &policy, user_path)
//! projected = instructions::InstructionProjection::render(&set)
//! diff      = projected.diff(previous_bindings)          // durable, not in history
//! messages += diff.to_message(&projected, role).into_iter()
//! ```
//!
//! `previous_bindings` is the [`crate::derived::DerivedBindings`] recorded when
//! the last request was built. It belongs in session state beside the
//! [`crate::compaction::CompactionPoint`], never inside the messages, which is
//! what makes the diff survive a compaction unchanged.
//!
//! ## What this is not
//!
//! It is not a Project authority. The project root comes from
//! [`crate::project`]; this module cannot decide where a project begins. It is
//! not a policy engine: it does not judge whether an instruction is reasonable,
//! and an `AGENTS.md` cannot grant a capability, widen a permission, or redirect
//! an execution decision. It is not durable session state: an instruction is
//! re-read from disk, so editing the file changes the next request without any
//! event being recorded.
//!
//! ## The property that matters most
//!
//! **Compaction cannot lose an instruction.** The content is not in the
//! transcript, so there is nothing for compaction to summarize away. A summary
//! may name a path; it may never restate the rules. See [`crate::derived`] for
//! why that is stated once there rather than in each feature.
//!
//! ## Known trade-offs, stated rather than hidden
//!
//! * **Nested files concatenate; they do not override.** Every discovered file
//!   contributes. A file that needs to cancel an inherited rule must say so in
//!   prose. This follows the `agents.md` convention ("the closest file wins") in
//!   the only way that is observable to a model: order, with each file's scope
//!   named.
//! * **The chain is re-read per request.** That costs a directory walk and a few
//!   reads, and it is what buys the compaction guarantee. The alternative —
//!   attaching nested instructions through tool output, as OpenCode does — is
//!   cheaper per turn and loses them when the carrying tool call is pruned.
//! * **Budget exhaustion drops the least specific file first.** A root file is
//!   inherited everywhere, so it is the cheapest to lose. Every dropped file is
//!   named in the projection.

pub mod config;
pub mod discovery;
pub mod projection;

pub use config::{InstructionsConfig, CANONICAL_FILENAME, DEFAULT_FILENAMES, OVERRIDE_FILENAME};
pub use discovery::{
    discover, require_inside_root, DiscoveryNote, InstructionFile, InstructionScope, InstructionSet,
};
pub use projection::{
    audit, is_instruction_block, require_project_path, section_id, InstructionAction,
    InstructionAudit, InstructionDiff, InstructionProjection, InstructionRole, INSTRUCTIONS_HEADER,
    REMOVAL_NOTICE, REPLACEMENT_NOTICE,
};
