//! The context engine: one object that ties the repo map, index, diff, ranking
//! and cache together. It is intentionally synchronous, local and read-only
//! apart from `.ocg/index/` and `.ocg/cache/`.

use crate::clock::Clock;
use crate::context::cache::{self, CacheKey, CacheStats, CleanReport, ContextCache};
use crate::context::config::ContextConfig;
use crate::context::freshness::{Provenance, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::gitdiff::{self, DiffSummary, GitSnapshot};
use crate::context::index::{self, ContextIndex, IndexReport};
use crate::context::ranking::{self, ContextPlan, PlanEnrichment};
use crate::context::repomap::{self, FileKind, RepoMap};
use crate::context::symbols::SymbolRef;
use crate::error::Result;
use crate::process::GitHost;
use crate::verification::select::{propose, SourceChange, TestIndexEntry};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One prepared context snapshot: the map, diff and index report.
#[derive(Debug, Clone)]
pub struct Prepared {
    pub map: RepoMap,
    pub diff: DiffSummary,
    pub index_report: IndexReport,
}

/// The outcome of a plan request.
#[derive(Debug, Clone)]
pub struct PlanOutcome {
    pub plan: ContextPlan,
    pub from_cache: bool,
    /// Non-fatal warnings (for example a failed cache write).
    pub warnings: Vec<String>,
    pub index_report: IndexReport,
}

/// The engine for one project root.
pub struct ContextEngine<'a> {
    root: PathBuf,
    config: ContextConfig,
    capabilities: crate::capabilities::CapabilityConfig,
    verification: crate::verification::Config,
    git: &'a dyn GitHost,
    clock: &'a dyn Clock,
}

impl<'a> ContextEngine<'a> {
    pub fn new(
        root: impl Into<PathBuf>,
        config: ContextConfig,
        git: &'a dyn GitHost,
        clock: &'a dyn Clock,
    ) -> Self {
        Self {
            root: root.into(),
            config,
            capabilities: crate::capabilities::CapabilityConfig::default(),
            verification: crate::verification::Config::default(),
            git,
            clock,
        }
    }

    /// Attach the planned capability policy.
    pub fn with_capabilities(
        mut self,
        capabilities: crate::capabilities::CapabilityConfig,
    ) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Attach the verification policy used for the state section and fallback.
    pub fn with_verification(mut self, verification: crate::verification::Config) -> Self {
        self.verification = verification;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &ContextConfig {
        &self.config
    }

    /// The combined configuration fingerprint used in cache keys. It covers the
    /// context, capability and verification policies because all three shape a
    /// plan.
    pub fn cache_config_fingerprint(&self) -> String {
        format!(
            "{}|{}|{}",
            self.config.fingerprint(),
            self.capabilities.fingerprint(),
            self.verification.fingerprint()
        )
    }

    /// Build the repo map together with the git snapshot it depends on.
    pub fn repo_map(&self) -> Result<(RepoMap, GitSnapshot)> {
        let snapshot = GitSnapshot::collect(&self.root, self.git);
        let map = repomap::build(&self.root, &self.config, &snapshot)?;
        Ok((map, snapshot))
    }

    /// Load the persisted index, if any.
    pub fn load_index(&self) -> Option<ContextIndex> {
        index::load(&self.root)
    }

    /// Incrementally update the index for a map/snapshot.
    pub fn update_index(
        &self,
        map: &RepoMap,
        snapshot: &GitSnapshot,
        previous: Option<&ContextIndex>,
    ) -> Result<(ContextIndex, IndexReport)> {
        index::update(
            &self.root,
            map,
            snapshot,
            &self.config,
            previous,
            self.clock.now_unix(),
        )
    }

    /// Compute the current diff summary against the index.
    pub fn diff(&self, snapshot: &GitSnapshot, index: &ContextIndex) -> Result<DiffSummary> {
        gitdiff::compute(&self.root, self.git, snapshot, index, &self.config)
    }

    /// Prepared map + diff + index without a task. Used on launch.
    pub fn prepare(&self) -> Result<Prepared> {
        let (map, snapshot) = self.repo_map()?;
        let previous = self.load_index();
        let (index, index_report) = self.update_index(&map, &snapshot, previous.as_ref())?;
        let diff = self.diff(&snapshot, &index)?;
        Ok(Prepared {
            map,
            diff,
            index_report,
        })
    }

    /// Build a deterministic plan for `task`, using the cache when fresh.
    pub fn plan(&self, task: &str, role: Option<&str>) -> Result<PlanOutcome> {
        let (map, snapshot) = self.repo_map()?;
        let previous = self.load_index();
        let (index, index_report) = self.update_index(&map, &snapshot, previous.as_ref())?;
        let now = self.clock.now_unix();
        let query = format!("{task}|{}", role.unwrap_or(""));
        let config_fingerprint = self.cache_config_fingerprint();
        let key = CacheKey::new(
            &index.repo_id,
            &config_fingerprint,
            &ranking::fingerprint_text(&query),
            &gitdiff::snapshot_fingerprint(&snapshot),
        );

        // A cache hit skips ranking, slicing and file reads entirely. The entry
        // is only returned when its git identity, dependency fingerprints and
        // slice contents are still valid.
        if self.config.cache {
            let context_cache = ContextCache::new(&self.root);
            if let Some(cached) = context_cache.get(&self.root, &key) {
                return Ok(PlanOutcome {
                    plan: cached,
                    from_cache: true,
                    warnings: Vec::new(),
                    index_report,
                });
            }
        }

        let diff = self.diff(&snapshot, &index)?;
        let test_proposal = if self.verification.include_test_proposal {
            Some(self.test_proposal(&map, &index, &diff)?)
        } else {
            None
        };
        let enrichment = PlanEnrichment {
            capabilities: crate::capabilities::CapabilityPlan::plan_config(
                task,
                &crate::capabilities::CapabilityEvidence {
                    changed_paths: diff.changed_paths(),
                    explicit: Vec::new(),
                },
                &self.capabilities.custom,
                self.capabilities.enabled,
            ),
            verification: crate::verification::VerificationState::from_config(&self.verification),
            test_proposal,
        };
        let ranked = ranking::rank(&map, &index, &diff, task, &self.config);
        let mut plan = ranking::assemble(
            &self.root,
            task,
            role,
            &map,
            &index,
            &diff,
            ranked,
            &self.config,
            enrichment,
            now,
        )?;
        let mut warnings = Vec::new();
        if self.config.cache {
            let dependencies = cache::dependencies(&plan);
            if let Err(error) = ContextCache::new(&self.root).put(&key, &dependencies, &plan, now) {
                // A cache write failure must never discard a computed plan.
                let warning = format!("context cache was not written: {error}");
                plan.notes.push(warning.clone());
                warnings.push(warning);
            }
        }

        Ok(PlanOutcome {
            plan,
            from_cache: false,
            warnings,
            index_report,
        })
    }

    /// Build the conservative targeted-test proposal for the current diff.
    ///
    /// This is a deterministic heuristic over changed files, their declared
    /// symbols and the test files whose indexed symbols have matching names. It
    /// never runs anything and always reports `complete = false`.
    pub fn test_proposal(
        &self,
        map: &RepoMap,
        index: &ContextIndex,
        diff: &DiffSummary,
    ) -> Result<crate::verification::TestProposal> {
        let mut source_changes = Vec::new();
        let mut changed_symbols: Vec<String> = Vec::new();
        for path in diff.changed_paths() {
            let Some(entry) = map.file(&path) else {
                continue;
            };
            if entry.sensitive || entry.binary {
                continue;
            }
            let symbols: Vec<String> = index
                .file(&path)
                .map(|file| {
                    file.symbols
                        .iter()
                        .filter(|symbol| symbol.kind != crate::context::symbols::SymbolKind::Import)
                        .map(|symbol| symbol.name.clone())
                        .collect()
                })
                .unwrap_or_default();
            for name in &symbols {
                if !changed_symbols.contains(name) {
                    changed_symbols.push(name.clone());
                }
            }
            source_changes.push(SourceChange {
                path,
                language: entry.language.clone(),
                symbols,
            });
        }
        changed_symbols.truncate(200);

        // Index name matches come from the index, so no test file is read here.
        let mut index_names: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for name in &changed_symbols {
            for reference in index.probable_references(name, 200) {
                if gitdiff::is_test_path(&reference.path) {
                    index_names
                        .entry(reference.path)
                        .or_default()
                        .push(name.clone());
                }
            }
        }
        let tests: Vec<TestIndexEntry> = map
            .files
            .iter()
            .filter(|file| {
                file.kind == FileKind::Test && !file.sensitive && !file.binary && !file.huge
            })
            .map(|file| TestIndexEntry {
                path: file.path.clone(),
                language: file.language.clone(),
                index_names: index_names.get(&file.path).cloned().unwrap_or_default(),
            })
            .collect();
        Ok(propose(
            &source_changes,
            &tests,
            Some(self.verification.default_stage.as_str()),
        ))
    }

    /// Convenience: build the proposal for the current working tree without a
    /// task plan. Used by `ocg verify` to attach an advisory proposal.
    pub fn targeted_tests(&self) -> Result<crate::verification::TestProposal> {
        let (map, snapshot) = self.repo_map()?;
        let previous = self.load_index();
        let (index, _) = self.update_index(&map, &snapshot, previous.as_ref())?;
        let diff = self.diff(&snapshot, &index)?;
        self.test_proposal(&map, &index, &diff)
    }

    /// Search symbols by case-insensitive substring.
    pub fn search_symbols(&self, query: &str, limit: usize) -> Result<Vec<SymbolRef>> {
        let index = self.current_index()?;
        Ok(index.search(query, limit))
    }

    /// The first non-import declaration named `name`.
    pub fn definition(&self, name: &str) -> Result<Option<SymbolRef>> {
        let index = self.current_index()?;
        Ok(index.definition(name))
    }

    /// A source slice for a symbol reference.
    pub fn source_slice(&self, reference: &SymbolRef) -> Result<Option<String>> {
        let index = self.current_index()?;
        index.source_slice(&self.root, reference)
    }

    /// Imports declared in a file.
    pub fn imports(&self, path: &str) -> Result<Vec<SymbolRef>> {
        let index = self.current_index()?;
        Ok(index.imports(path))
    }

    /// Probable references to a name across indexed files. This reads indexed
    /// (non-sensitive) file contents and matches on word boundaries; it is a
    /// deterministic heuristic, not a type checker.
    pub fn probable_references(&self, name: &str, limit: usize) -> Result<Vec<SymbolRef>> {
        let index = self.current_index()?;
        let mut out = Vec::new();
        for file in &index.files {
            if !file.supported || file.excluded || file.binary || file.huge {
                continue;
            }
            let path = self.root.join(&file.path);
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            for (offset, line) in content.lines().enumerate() {
                if !crate::context::symbols::contains_word(line, name) {
                    continue;
                }
                let line_number = (offset + 1) as u32;
                let enclosing = file.symbols.iter().find(|symbol| {
                    symbol.start_line <= line_number && symbol.end_line >= line_number
                });
                out.push(SymbolRef {
                    path: file.path.clone(),
                    name: name.to_string(),
                    kind: enclosing
                        .map(|symbol| symbol.kind)
                        .unwrap_or(crate::context::symbols::SymbolKind::Reference),
                    start_line: line_number,
                    end_line: line_number,
                });
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    /// Symbols related to `reference`: same-file declarations plus probable
    /// references to its name in other files.
    pub fn related(&self, reference: &SymbolRef) -> Result<Vec<SymbolRef>> {
        let index = self.current_index()?;
        let mut related: Vec<SymbolRef> = index
            .symbols_in(&reference.path)
            .into_iter()
            .filter(|symbol| {
                symbol.name != reference.name || symbol.start_line != reference.start_line
            })
            .collect();
        for candidate in self.probable_references(&reference.name, 50)? {
            if candidate.path != reference.path {
                related.push(candidate);
            }
        }
        related.sort_by(|a, b| {
            (&a.path, a.start_line, &a.name).cmp(&(&b.path, b.start_line, &b.name))
        });
        related.dedup();
        Ok(related)
    }

    /// Load the index, rebuilding it when missing or stale.
    fn current_index(&self) -> Result<ContextIndex> {
        let (map, snapshot) = self.repo_map()?;
        let previous = self.load_index();
        let (index, _) = self.update_index(&map, &snapshot, previous.as_ref())?;
        Ok(index)
    }

    pub fn cache_stats(&self) -> CacheStats {
        ContextCache::new(&self.root).stats()
    }

    pub fn cache_clean(&self) -> Result<CleanReport> {
        ContextCache::new(&self.root).clean()
    }

    /// Provenance for a fresh engine identity.
    pub fn provenance(&self) -> Provenance {
        Provenance {
            engine_version: ENGINE_VERSION.to_string(),
            schema_version: SCHEMA_VERSION,
            repo_id: index::repo_id(&self.root, &crate::context::gitdiff::GitState::default()),
            ..Provenance::default()
        }
    }
}
