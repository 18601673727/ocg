//! Typed tool capabilities and a deterministic capability planner.
//!
//! This is **context/config planning and diagnostics, not a security sandbox**.
//! The planner decides which capability groups a task plausibly needs so the
//! context planner and the CLI can describe an explicit boundary. Nothing here
//! activates a runtime tool schema, grants a provider permission or blocks an
//! actual call; enforcement remains OpenCode's job. The honesty of that
//! boundary is part of the contract.
//!
//! The planning rules are conservative:
//!
//! - `filesystem` is the baseline capability for every task;
//! - only keyword evidence selects another capability;
//! - a generic coding task adds `git` only when there is evidence (changed
//!   paths or an explicit request);
//! - an unknown task exposes `filesystem` only and never cloud/browser/db.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A capability group. `Custom` carries a configured, user-defined name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Filesystem,
    Git,
    Github,
    Web,
    Documentation,
    Browser,
    Database,
    Cloud,
    Custom(String),
}

impl Capability {
    /// A stable display name.
    pub fn name(&self) -> String {
        match self {
            Capability::Filesystem => "filesystem".to_string(),
            Capability::Git => "git".to_string(),
            Capability::Github => "github".to_string(),
            Capability::Web => "web".to_string(),
            Capability::Documentation => "documentation".to_string(),
            Capability::Browser => "browser".to_string(),
            Capability::Database => "database".to_string(),
            Capability::Cloud => "cloud".to_string(),
            Capability::Custom(name) => format!("custom:{name}"),
        }
    }

    /// A deterministic rank used for plan ordering.
    fn rank(&self) -> u8 {
        match self {
            Capability::Filesystem => 0,
            Capability::Git => 1,
            Capability::Github => 2,
            Capability::Web => 3,
            Capability::Documentation => 4,
            Capability::Browser => 5,
            Capability::Database => 6,
            Capability::Cloud => 7,
            Capability::Custom(_) => 8,
        }
    }

    /// Every built-in capability, in plan order.
    pub fn builtins() -> Vec<Capability> {
        vec![
            Capability::Filesystem,
            Capability::Git,
            Capability::Github,
            Capability::Web,
            Capability::Documentation,
            Capability::Browser,
            Capability::Database,
            Capability::Cloud,
        ]
    }
}

/// Optional `capabilities` policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityConfig {
    pub enabled: bool,
    /// Extra capability names a project may select by keyword.
    pub custom: Vec<String>,
}

impl Default for CapabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            custom: Vec::new(),
        }
    }
}

impl CapabilityConfig {
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(capabilities) = data.get("capabilities") else {
            return Ok(Self::default());
        };
        if capabilities.is_null() {
            return Ok(Self::default());
        }
        let object = capabilities
            .as_object()
            .ok_or_else(|| OcgError::config("capabilities must be a JSON object"))?;
        let mut config = Self::default();
        if let Some(value) = object.get("enabled") {
            config.enabled = value
                .as_bool()
                .ok_or_else(|| OcgError::config("capabilities.enabled must be a boolean"))?;
        }
        if let Some(value) = object.get("custom") {
            let list = value
                .as_array()
                .ok_or_else(|| OcgError::config("capabilities.custom must be a list"))?;
            for entry in list {
                let name = entry
                    .as_str()
                    .ok_or_else(|| {
                        OcgError::config("capabilities.custom must contain only strings")
                    })?
                    .trim()
                    .to_string();
                if name.is_empty() {
                    return Err(OcgError::config(
                        "capabilities.custom names must not be empty",
                    ));
                }
                if !name
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
                {
                    return Err(OcgError::config(format!(
                        "capabilities.custom name '{name}' must use letters, digits, '-' or '_'"
                    )));
                }
                if !config.custom.contains(&name.to_ascii_lowercase()) {
                    config.custom.push(name.to_ascii_lowercase());
                }
            }
        }
        Ok(config)
    }

    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    /// A stable fingerprint of the policy, usable in cache keys.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
    }
}

/// One allowed capability with the evidence that selected it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEntry {
    pub capability: Capability,
    pub source: String,
    pub reason: String,
}

/// Extra, deterministic evidence a planner may use.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CapabilityEvidence {
    /// Changed paths (from Git, for example). Presence is evidence for `git`.
    #[serde(default)]
    pub changed_paths: Vec<String>,
    /// Capabilities a caller explicitly requested.
    #[serde(default)]
    pub explicit: Vec<Capability>,
}

/// The plan/Tool Context Firewall description. Advisory only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityPlan {
    pub task: String,
    pub enabled: bool,
    #[serde(default)]
    pub capabilities: Vec<CapabilityEntry>,
    /// Capabilities deliberately not exposed to this task.
    #[serde(default)]
    pub denied: Vec<Capability>,
    #[serde(default)]
    pub notes: Vec<String>,
}

/// The firewall view derived from a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FirewallPlan {
    pub allowed: Vec<Capability>,
    pub denied: Vec<Capability>,
    pub note: String,
}

impl CapabilityPlan {
    /// Plan for a task with no extra evidence.
    pub fn plan(task: &str) -> Self {
        Self::plan_with(task, &CapabilityEvidence::default(), &[])
    }

    /// Plan with evidence and configured custom capability names. The policy is
    /// treated as enabled; use [`CapabilityPlan::plan_config`] to honor the
    /// `capabilities.enabled` flag.
    pub fn plan_with(task: &str, evidence: &CapabilityEvidence, custom: &[String]) -> Self {
        Self::plan_config(task, evidence, custom, true)
    }

    /// Plan with the project capability policy. A disabled policy plans
    /// nothing: no capability is allowed or exposed, and the disabled state is
    /// carried into the plan instead of being silently ignored.
    pub fn plan_config(
        task: &str,
        evidence: &CapabilityEvidence,
        custom: &[String],
        enabled: bool,
    ) -> Self {
        let custom: Vec<Capability> = custom.iter().cloned().map(Capability::Custom).collect();
        if !enabled {
            return Self::disabled(task, &custom);
        }
        Self::plan_inner(task, evidence, &custom)
    }

    /// A plan for a project that disabled capability planning.
    fn disabled(task: &str, custom: &[Capability]) -> Self {
        let mut denied = Capability::builtins();
        denied.extend(custom.iter().cloned());
        denied.sort_by(|a, b| {
            a.rank()
                .cmp(&b.rank())
                .then_with(|| a.name().cmp(&b.name()))
        });
        denied.dedup();
        Self {
            task: task.to_string(),
            enabled: false,
            capabilities: Vec::new(),
            denied,
            notes: vec![
                "capabilities are disabled (capabilities.enabled=false); no capability is planned or exposed"
                    .to_string(),
            ],
        }
    }

    fn plan_inner(task: &str, evidence: &CapabilityEvidence, custom: &[Capability]) -> Self {
        let lower = task.to_ascii_lowercase();
        let terms = task_terms(&lower);
        let mut entries: Vec<CapabilityEntry> = Vec::new();

        let mut allow = |capability: Capability, source: &str, reason: String| {
            if let Some(existing) = entries
                .iter_mut()
                .find(|entry| entry.capability == capability)
            {
                if !existing.reason.contains(&reason) {
                    existing.reason.push_str("; ");
                    existing.reason.push_str(&reason);
                }
            } else {
                entries.push(CapabilityEntry {
                    capability,
                    source: source.to_string(),
                    reason,
                });
            }
        };

        // The baseline capability is always present.
        allow(
            Capability::Filesystem,
            "baseline",
            "repository files and local state".to_string(),
        );

        let mut matched_any = false;
        for (capability, keywords) in keyword_groups() {
            let hits: Vec<&str> = keywords
                .iter()
                .copied()
                .filter(|keyword| keyword_hit(&lower, &terms, keyword))
                .collect();
            if !hits.is_empty() {
                matched_any = true;
                allow(
                    capability,
                    "keyword",
                    format!("task mentions {}", hits.join(", ")),
                );
            }
        }
        for capability in custom {
            let name = match capability {
                Capability::Custom(name) => name.clone(),
                _ => continue,
            };
            if terms.contains(&name) || lower.contains(&name) {
                matched_any = true;
                allow(
                    capability.clone(),
                    "config",
                    format!("task mentions configured capability {name}"),
                );
            }
        }

        let mut notes = Vec::new();
        // A generic coding task may add git only when there is concrete
        // evidence (changed paths) — never merely because code exists.
        if !matched_any && has_coding_terms(&terms) {
            if !evidence.changed_paths.is_empty() {
                allow(
                    Capability::Git,
                    "evidence",
                    format!(
                        "{} changed path(s) provided as evidence",
                        evidence.changed_paths.len()
                    ),
                );
                notes.push(
                    "generic coding task: git was exposed because changed paths were supplied"
                        .to_string(),
                );
            } else {
                notes.push(
                    "generic coding task: only filesystem is exposed (no git evidence)".to_string(),
                );
            }
        } else if !matched_any {
            notes.push(
                "unknown or ambiguous task: only the filesystem baseline is exposed".to_string(),
            );
        }

        for capability in &evidence.explicit {
            allow(
                capability.clone(),
                "explicit",
                "explicitly requested".to_string(),
            );
        }

        entries.sort_by(|a, b| {
            a.capability
                .rank()
                .cmp(&b.capability.rank())
                .then_with(|| a.capability.name().cmp(&b.capability.name()))
        });
        entries.dedup_by(|a, b| a.capability == b.capability);

        let allowed: Vec<Capability> = entries
            .iter()
            .map(|entry| entry.capability.clone())
            .collect();
        let mut universe = Capability::builtins();
        universe.extend(custom.iter().cloned());
        let denied: Vec<Capability> = universe
            .into_iter()
            .filter(|capability| !allowed.contains(capability))
            .collect();

        Self {
            task: task.to_string(),
            enabled: true,
            capabilities: entries,
            denied,
            notes,
        }
    }

    /// The allowed/denied view. Advisory, not a sandbox.
    pub fn firewall(&self) -> FirewallPlan {
        FirewallPlan {
            allowed: self
                .capabilities
                .iter()
                .map(|entry| entry.capability.clone())
                .collect(),
            denied: self.denied.clone(),
            note: "context/config plan only; this does not enforce runtime tool permissions"
                .to_string(),
        }
    }

    pub fn allows(&self, capability: &Capability) -> bool {
        self.capabilities
            .iter()
            .any(|entry| entry.capability == *capability)
    }

    /// A stable, human-readable rendering for `ocg tools`.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("capability plan for: {}\n", self.task));
        if !self.enabled {
            out.push_str(
                "capability planning is disabled (capabilities.enabled=false); no capability is planned or exposed\n",
            );
        } else {
            out.push_str("allowed:\n");
            for entry in &self.capabilities {
                out.push_str(&format!(
                    "  {:<16} [{}] {}\n",
                    entry.capability.name(),
                    entry.source,
                    entry.reason
                ));
            }
        }
        if !self.denied.is_empty() {
            out.push_str("denied:\n");
            let names: Vec<String> = self.denied.iter().map(Capability::name).collect();
            out.push_str(&format!("  {}\n", names.join(", ")));
        }
        for note in &self.notes {
            out.push_str(&format!("note: {note}\n"));
        }
        out.push_str(
            "boundary: this is a context/config plan and diagnostic only; it does not activate or\n\
             enforce runtime tool schemas. OpenCode owns execution, conversation, provider and tool\n\
             semantics.\n",
        );
        out
    }
}

fn keyword_groups() -> Vec<(Capability, Vec<&'static str>)> {
    vec![
        (
            Capability::Git,
            vec![
                "git",
                "commit",
                "commits",
                "branch",
                "branches",
                "diff",
                "diffs",
                "merge",
                "rebase",
                "cherry-pick",
                "stash",
                "tag",
                "tags",
                "checkout",
                "worktree",
            ],
        ),
        (
            Capability::Github,
            vec![
                "github",
                "pull request",
                "pull-request",
                "issue",
                "issues",
                "workflow",
                "workflows",
                "pr",
            ],
        ),
        (
            Capability::Web,
            vec![
                "web", "search", "online", "url", "urls", "http", "https", "latest", "browse",
            ],
        ),
        (
            Capability::Documentation,
            vec![
                "doc",
                "docs",
                "documentation",
                "readme",
                "guide",
                "manual",
                "reference",
                "changelog",
                "api",
            ],
        ),
        (
            Capability::Browser,
            vec![
                "browser",
                "playwright",
                "puppeteer",
                "selenium",
                "screenshot",
                "screenshots",
                "dom",
            ],
        ),
        (
            Capability::Database,
            vec![
                "database",
                "databases",
                "db",
                "sql",
                "query",
                "queries",
                "migration",
                "migrations",
                "schema",
                "postgres",
                "postgresql",
                "sqlite",
                "mysql",
                "d1",
            ],
        ),
        (
            Capability::Cloud,
            vec![
                "cloud",
                "aws",
                "gcp",
                "azure",
                "deploy",
                "deployment",
                "kubernetes",
                "k8s",
                "lambda",
                "s3",
                "terraform",
            ],
        ),
    ]
}

fn has_coding_terms(terms: &[String]) -> bool {
    const CODING: [&str; 16] = [
        "implement",
        "build",
        "fix",
        "bug",
        "refactor",
        "code",
        "coding",
        "feature",
        "test",
        "tests",
        "compile",
        "debug",
        "function",
        "module",
        "add",
        "update",
    ];
    terms.iter().any(|term| CODING.contains(&term.as_str()))
}

fn task_terms(lower: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in lower.split(|ch: char| !ch.is_alphanumeric() && ch != '-' && ch != '_') {
        if raw.len() < 2 {
            continue;
        }
        if !out.iter().any(|existing| existing == raw) {
            out.push(raw.to_string());
        }
    }
    out
}

fn keyword_hit(lower: &str, terms: &[String], keyword: &str) -> bool {
    if keyword.contains(' ') || keyword.contains('-') {
        lower.contains(keyword)
    } else {
        terms.iter().any(|term| term == keyword)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names(plan: &CapabilityPlan) -> Vec<String> {
        plan.capabilities
            .iter()
            .map(|entry| entry.capability.name())
            .collect()
    }

    #[test]
    fn git_only_task_exposes_filesystem_and_git_only() {
        let plan = CapabilityPlan::plan("commit the changes to the git branch");
        assert_eq!(names(&plan), vec!["filesystem", "git"]);
        assert!(plan.allows(&Capability::Filesystem));
        assert!(plan.allows(&Capability::Git));
        for denied in [
            Capability::Cloud,
            Capability::Browser,
            Capability::Database,
            Capability::Github,
            Capability::Web,
        ] {
            assert!(!plan.allows(&denied), "{denied:?} must not be exposed");
        }
    }

    #[test]
    fn docs_task_exposes_documentation() {
        let plan = CapabilityPlan::plan("look up the readme and documentation");
        assert!(plan.allows(&Capability::Filesystem));
        assert!(plan.allows(&Capability::Documentation));
        assert!(!plan.allows(&Capability::Cloud));
        assert!(!plan.allows(&Capability::Browser));
        assert!(!plan.allows(&Capability::Database));
    }

    #[test]
    fn docs_web_task_exposes_web_when_online() {
        let plan = CapabilityPlan::plan("search online docs for the latest api");
        assert!(plan.allows(&Capability::Documentation));
        assert!(plan.allows(&Capability::Web));
        assert!(!plan.allows(&Capability::Database));
    }

    #[test]
    fn database_task_exposes_database() {
        let plan = CapabilityPlan::plan("write a SQL migration for the database schema");
        assert!(plan.allows(&Capability::Filesystem));
        assert!(plan.allows(&Capability::Database));
        assert!(!plan.allows(&Capability::Cloud));
        assert!(!plan.allows(&Capability::Browser));
    }

    #[test]
    fn generic_coding_exposes_filesystem_and_git_only_with_evidence() {
        let without = CapabilityPlan::plan("implement the feature and add tests");
        assert_eq!(names(&without), vec!["filesystem"]);
        assert!(!without.allows(&Capability::Git));

        let evidence = CapabilityEvidence {
            changed_paths: vec!["src/lib.rs".to_string()],
            explicit: Vec::new(),
        };
        let with = CapabilityPlan::plan_with("implement the feature and add tests", &evidence, &[]);
        assert_eq!(names(&with), vec!["filesystem", "git"]);
    }

    #[test]
    fn unknown_task_never_exposes_cloud_browser_or_db() {
        let plan = CapabilityPlan::plan("xyzzy plugh frobnicate");
        assert_eq!(names(&plan), vec!["filesystem"]);
        assert!(!plan.allows(&Capability::Cloud));
        assert!(!plan.allows(&Capability::Browser));
        assert!(!plan.allows(&Capability::Database));
        assert!(plan.firewall().note.contains("does not enforce"));
    }

    #[test]
    fn disabled_policy_exposes_nothing() {
        let plan = CapabilityPlan::plan_config(
            "commit the changes to the git branch",
            &CapabilityEvidence::default(),
            &[],
            false,
        );
        assert!(!plan.enabled);
        assert!(plan.capabilities.is_empty());
        assert!(!plan.allows(&Capability::Filesystem));
        assert!(!plan.allows(&Capability::Git));
        assert!(plan.denied.contains(&Capability::Git));
        assert!(plan.denied.contains(&Capability::Filesystem));
        assert!(plan.render().contains("disabled"));
        assert!(plan.firewall().allowed.is_empty());
        assert!(plan
            .notes
            .iter()
            .any(|note| note.contains("capabilities.enabled=false")));
    }

    #[test]
    fn enabled_policy_still_plans() {
        let plan = CapabilityPlan::plan_config(
            "commit the changes to the git branch",
            &CapabilityEvidence::default(),
            &[],
            true,
        );
        assert!(plan.enabled);
        assert!(plan.allows(&Capability::Git));
    }

    #[test]
    fn explicit_and_custom_capabilities_are_honored() {
        let evidence = CapabilityEvidence {
            changed_paths: Vec::new(),
            explicit: vec![Capability::Browser],
        };
        let plan = CapabilityPlan::plan_with("do the thing", &evidence, &["warehouse".to_string()]);
        assert!(plan.allows(&Capability::Browser));
        let custom = Capability::Custom("warehouse".to_string());
        // Custom names are registered as config capabilities.
        assert!(plan
            .capabilities
            .iter()
            .any(|entry| entry.capability == custom || entry.capability == Capability::Browser));
    }

    #[test]
    fn custom_capability_is_selected_by_keyword() {
        let plan = CapabilityPlan::plan_with(
            "query the warehouse ledger",
            &CapabilityEvidence::default(),
            &["warehouse".to_string()],
        );
        assert!(plan.allows(&Capability::Custom("warehouse".to_string())));
        assert!(plan.denied.contains(&Capability::Cloud));
    }

    #[test]
    fn config_validation() {
        assert_eq!(
            CapabilityConfig::from_config(&json!({})).unwrap(),
            CapabilityConfig::default()
        );
        assert!(CapabilityConfig::from_config(&json!({"capabilities": "on"})).is_err());
        assert!(CapabilityConfig::from_config(&json!({"capabilities": {"custom": [1]}})).is_err());
        assert!(
            CapabilityConfig::from_config(&json!({"capabilities": {"custom": ["bad name"]}}))
                .is_err()
        );
        let config = CapabilityConfig::from_config(
            &json!({"capabilities": {"enabled": false, "custom": ["Warehouse"]}}),
        )
        .unwrap();
        assert!(!config.enabled);
        assert_eq!(config.custom, vec!["warehouse".to_string()]);
        assert!(!CapabilityConfig::validate(&json!({"capabilities": {"custom": [1]}})).is_empty());
    }
}
