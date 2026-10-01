//! Skills: a discoverable, selectable capability description plus its resources.
//!
//! ## What a Skill is
//!
//! A directory containing a `SKILL.md` whose frontmatter names it and describes
//! when to use it, and whose body is prose telling a model how to approach a kind
//! of work. Optional sibling files — scripts, references, templates — travel with
//! it and are read on demand rather than loaded with it.
//!
//! ```text
//! <project>/.ocg/skills/<name>/SKILL.md
//! <config>/skills/<name>/SKILL.md
//! ```
//!
//! ## The three kinds of thing, and why they are three
//!
//! ```text
//! AGENTS.md   project- and directory-scoped standing instructions
//! Skill       a discoverable, selectable capability description plus resources
//! Native Tool something OCG actually executes
//! ```
//!
//! The line is authority. A Native Tool performs an effect under OCG's execution
//! authority and a permission class. A Skill is prose: it cannot call a tool,
//! grant a capability, or widen a permission. It can *declare* that it expects
//! certain tools, and that declaration is compared against what the request
//! actually carries and reported as a diagnostic — see
//! [`definition::unsatisfied_dependencies`].
//!
//! ## Three-tier disclosure is the whole cost model
//!
//! ```text
//! name + description   every skill, every request      ← the only unconditional cost
//! body                 one skill, once activated
//! resources            one file, once the model asks
//! ```
//!
//! A project with forty skills pays for forty names and descriptions. That is why
//! [`projection::render_catalog`] is budgeted separately from bodies, and why a
//! body is projected only for a skill that was actually activated.
//!
//! ## Identity is a re-materialization key
//!
//! A skill is identified by its `name`, and its body carries a content
//! [`crate::derived::DerivedRevision`]. Both are recorded outside message history.
//! After a compaction the caller replays
//! [`resolve::rematerialize`] and the bodies are re-read from disk: no skill
//! content is in the summary, and none needs to be. A skill edited between two
//! requests is therefore picked up automatically, which is the correct behaviour
//! for a file a human is still writing.
//!
//! ## What is deliberately not here
//!
//! No execution, no permission grant, no nested skills, no remote fetching, and
//! no signature verification. Each of those is a real feature in some surveyed
//! implementation and each is a separate authority question; this module answers
//! none of them. See [`registry`] for the boundary checks it does apply.
//!
//! ## Where the caller plugs in
//!
//! Nothing here is wired to a request. A caller assembles a request like this:
//!
//! ```text
//! registry  = skills::discover(project_root, config_home, builtins, &policy)
//! catalog   = skills::render_catalog(&registry, &policy)
//! active    = skills::resolve(&registry, requests, &policy, tools, capabilities)
//! bindings  = previous_bindings    // durable, beside the CompactionPoint
//! messages += skills::project(&registry, &active, &policy, Some(&bindings), role)
//! ```
//!
//! After a compaction the caller replays
//! [`resolve::rematerialize`] instead of [`resolve::resolve`]: the activation
//! list is rebuilt from names, the bodies are re-read from disk, and the
//! resulting bindings replace the old ones. Nothing is read out of the summary.

pub mod config;
pub mod definition;
pub mod projection;
pub mod registry;
pub mod resolve;

pub use config::{SkillsConfig, SKILL_FILENAME};
pub use definition::{
    load, parse_metadata, resolve_resource, split_frontmatter, unsatisfied_dependencies,
    SkillDefinition, SkillMetadata, SkillRejection, SkillResource, SkillSource,
};
pub use projection::{
    measure, project, render_active, render_catalog, SectionAction, SectionDiff, SkillCostReport,
    SkillProjection, SkillRole, CATALOG_HEADER, CATALOG_TRUNCATED_NOTICE, REPLACEMENT_NOTICE,
    SKILL_HEADER,
};
pub use registry::{discover, resolve_named, SkillCollision, SkillRegistry};
pub use resolve::{
    enforce_budget, rematerialize, resolve, ActivationOrigin, ActivationRefusal,
    ActivationRequest, ActivationSet, ActiveSkill,
};
